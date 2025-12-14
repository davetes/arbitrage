from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from binance.spot import Spot as BinanceClient
try:
    from binance.websocket.websocket_client import BinanceWebsocketClient
except Exception:  # Dependency may be missing in some deployments
    BinanceWebsocketClient = None
from arbbot import settings as S
from .symbol_loader import get_symbol_loader
import time
import logging
import random
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# Major coins that must be included in the middle leg of routes
MAJOR_COINS = {
    "BTC", "ETH", "BNB", "SOL", "ADA", "XRP", "DOGE", "MATIC",
    "AVAX", "DOT", "LINK", "UNI", "ATOM", "LTC", "ETC", "XLM",
    "ALGO", "VET", "FIL", "TRX", "EOS", "AAVE", "SXP", "CHZ",
    "BICO", "LISTA", "APT", "ARB", "OP", "SUI", "SEI", "TIA"
}


class DepthCache:
    """Cache for order book depth snapshots with TTL"""
    def __init__(self, ttl_seconds: float = 2.0):
        self.ttl = ttl_seconds
        self._cache: Dict[str, Tuple[dict, float]] = {}
    
    def get(self, symbol: str) -> Optional[dict]:
        """Get cached depth if still valid"""
        if symbol in self._cache:
            data, timestamp = self._cache[symbol]
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self._cache[symbol]
        return None
    
    def set(self, symbol: str, data: dict):
        """Cache depth data"""
        self._cache[symbol] = (data, time.time())
    
    def clear(self):
        """Clear all cached data"""
        self._cache.clear()


# Global depth cache instance (2 second TTL)
_depth_cache = DepthCache(ttl_seconds=2.0)


class _BookTickerStream:
    """
    WebSocket-based best bid/ask cache using BinanceWebsocketClient (binance-connector 3.6.0).
    Implements combined streams following official Binance WebSocket API.
    Based on: https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams
    """

    def __init__(self, ttl_seconds: float = 5.0):
        self.ttl = ttl_seconds
        self.client: Optional[BinanceWebsocketClient] = None
        self.lock = threading.Lock()
        self.cache: Dict[str, Tuple[dict, float]] = {}
        self.running = False
        self.subscribed_symbols: set = set()
        self.last_reconnect_time = 0.0
        self.reconnect_interval = 82800.0  # Reconnect before 24 hour limit (23 hours)
        self._base_url = "wss://stream.binance.com:9443"  # Official base endpoint

    def _on_message(self, _, message: str):
        """
        Handle WebSocket messages according to Binance API:
        - Combined stream format: {"stream": "btcusdt@bookTicker", "data": {...}}
        - Single stream format: {"s": "BTCUSDT", "b": "...", ...}
        - Control messages: {"result": ..., "id": ...}
        """
        try:
            data = json.loads(message)
            if not isinstance(data, dict):
                return
            
            # Handle control messages (subscribe/unsubscribe responses)
            if "result" in data and "id" in data:
                # This is a control message response, ignore
                return
            
            # Handle combined stream format: {"stream": "btcusdt@bookTicker", "data": {...}}
            if "stream" in data and "data" in data:
                stream_name = data.get("stream", "")
                if "@bookTicker" in stream_name:
                    ticker_data = data["data"]
                    symbol = ticker_data.get("s") or ticker_data.get("symbol")
                    self._process_book_ticker(symbol, ticker_data)
                return
            
            # Handle single stream format (raw stream)
            # Check if it's a bookTicker message
            if "s" in data and ("b" in data or "a" in data):
                symbol = data.get("s") or data.get("symbol")
                self._process_book_ticker(symbol, data)
                return
                
        except Exception as e:
            logger.debug(f"Error processing WebSocket message: {e}")
            return
    
    def _process_book_ticker(self, symbol: str, ticker_data: dict):
        """Process bookTicker data according to Binance API format"""
        if not symbol:
            return
        
        # Binance bookTicker format:
        # {"u":400900217, "s":"BNBUSDT", "b":"25.35190000", "B":"31.21000000", 
        #  "a":"25.36520000", "A":"40.66000000"}
        try:
            bid_price = float(ticker_data.get("b", 0))
            bid_qty = float(ticker_data.get("B", 0))
            ask_price = float(ticker_data.get("a", 0))
            ask_qty = float(ticker_data.get("A", 0))
            
            if bid_price <= 0 or ask_price <= 0:
                return
                
            snapshot = {
                "ask_price": ask_price,
                "ask_qty": ask_qty,
                "bid_price": bid_price,
                "bid_qty": bid_qty,
                "total_ask_qty": ask_qty,
                "total_bid_qty": bid_qty,
                "cap_ask_quote": ask_price * ask_qty,
                "cap_bid_quote": bid_price * bid_qty,
            }
            with self.lock:
                self.cache[symbol.upper()] = (snapshot, time.time())
        except (ValueError, TypeError) as e:
            logger.debug(f"Error parsing bookTicker data for {symbol}: {e}")
            return

    def _ensure_connection(self, symbols: List[str], **kwargs):
        """
        Ensure WebSocket connection is active and subscribed to required symbols.
        Uses BinanceWebsocketClient which starts automatically upon instantiation.
        """
        if BinanceWebsocketClient is None:
            logger.warning("BinanceWebsocketClient unavailable; skipping websocket bookTicker startup.")
            return False
        
        symbols_set = {s.upper() for s in symbols}
        needs_update = not self.running or symbols_set != self.subscribed_symbols
        
        # Check if reconnection is needed (prevent stale connections)
        now = time.time()
        needs_reconnect = (now - self.last_reconnect_time) > self.reconnect_interval
        
        if needs_reconnect and self.running:
            logger.info("Reconnecting WebSocket to prevent stale connection...")
            self.stop()
            needs_update = True
        
        if not needs_update:
            return True
        
        try:
            # Stop existing connection if running
            if self.running:
                self.stop()
                time.sleep(0.2)  # Brief pause for cleanup
            
            # Use combined stream format for efficiency (up to 1024 streams per connection per Binance API)
            # Binance format: wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/...
            stream_symbols = [s.lower() for s in symbols]
            
            # Binance allows up to 1024 streams per combined stream (per official API)
            max_streams_per_connection = 1024
            if len(stream_symbols) <= max_streams_per_connection:
                # Single connection for all symbols using combined stream
                streams = "/".join([f"{sym}@bookTicker" for sym in stream_symbols])
                # Use combined stream endpoint
                combined_url = f"{self._base_url}/stream?streams={streams}"
                
                # BinanceWebsocketClient starts automatically upon instantiation
                # on_message must be first parameter, stream_url second
                self.client = BinanceWebsocketClient(
                    on_message=self._on_message,
                    stream_url=combined_url,
                    **kwargs
                )
                # No need to call start() - client starts automatically
                
                self.subscribed_symbols = symbols_set
                self.running = True
                self.last_reconnect_time = now
                logger.info(f"WebSocket connected: {len(stream_symbols)} symbols via combined stream (max 1024)")
            else:
                # Multiple connections for large symbol sets - use first 1024 (highest priority)
                logger.info(f"Large symbol set ({len(stream_symbols)}), using top {max_streams_per_connection} symbols")
                limited_symbols = stream_symbols[:max_streams_per_connection]
                streams = "/".join([f"{sym}@bookTicker" for sym in limited_symbols])
                combined_url = f"{self._base_url}/stream?streams={streams}"
                
                # BinanceWebsocketClient starts automatically upon instantiation
                # on_message must be first parameter, stream_url second
                self.client = BinanceWebsocketClient(
                    on_message=self._on_message,
                    stream_url=combined_url,
                    **kwargs
                )
                # No need to call start() - client starts automatically
                
                self.subscribed_symbols = {s.upper() for s in limited_symbols}
                self.running = True
                self.last_reconnect_time = now
                logger.info(f"WebSocket connected: {len(limited_symbols)} symbols (limited to {max_streams_per_connection})")
            
            return True
        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}", exc_info=True)
            self.running = False
            return False

    def start(self, symbols: List[str], **kwargs):
        """Start or update bookTicker websocket subscriptions (keeps connection alive)."""
        if not symbols:
            return
        
        self._ensure_connection(symbols, **kwargs)

    def stop(self):
        """Stop websocket connection"""
        if self.client:
            try:
                self.client.stop()
            except Exception:
                pass
        self.running = False
        self.client = None

    def get(self, symbol: str) -> Optional[dict]:
        """Get cached ticker data if available and fresh"""
        now = time.time()
        with self.lock:
            symbol_upper = symbol.upper()
            if symbol_upper in self.cache:
                data, ts = self.cache[symbol_upper]
                if now - ts < self.ttl:
                    return data
                else:
                    # Data expired, remove from cache
                    del self.cache[symbol_upper]
        return None
    
    def get_coverage(self, symbols: List[str]) -> Tuple[int, int]:
        """Get WebSocket coverage stats"""
        now = time.time()
        with self.lock:
            valid_count = 0
            for sym in symbols:
                sym_upper = sym.upper()
                if sym_upper in self.cache:
                    _, ts = self.cache[sym_upper]
                    if now - ts < self.ttl:
                        valid_count += 1
            return valid_count, len(symbols)


# Global WebSocket stream instance with persistent connection (created after class definition)
_book_ticker_stream = _BookTickerStream(ttl_seconds=5.0)


@dataclass
class CandidateRoute:
    a: str  # leg1 label e.g., "BTC/USDT buy"
    b: str  # leg2 label e.g., "ETH/BTC buy"
    c: str  # leg3 label e.g., "ETH/USDT sell"
    profit_pct: float
    volume_usd: float


def _client(timeout: int = None) -> BinanceClient:
    if timeout is None:
        timeout = 30
    kwargs = {
        "timeout": timeout,
        "base_url": getattr(S, "BINANCE_BASE_URL", "https://api.binance.com"),
    }
    proxy_url = getattr(S, "PROXY_URL", "")
    if proxy_url:
        kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
    if S.BINANCE_API_KEY and S.BINANCE_API_SECRET:
        return BinanceClient(api_key=S.BINANCE_API_KEY, api_secret=S.BINANCE_API_SECRET, **kwargs)
    return BinanceClient(**kwargs)


def _load_symbols(client: BinanceClient) -> Dict[str, dict]:
    """Load all active spot markets with target quotes (USDT, BTC, ETH, BNB, FDUSD, USDC)."""
    TARGET_QUOTES = {"USDT", "BTC", "ETH", "BNB", "FDUSD", "USDC"}
    
    try:
        exchange_info = client.exchange_info()
        symbols = {}
        seen_symbols = set()
        invalid_count = 0
        filtered_count = 0
        
        for symbol_info in exchange_info.get("symbols", []):
            symbol = symbol_info.get("symbol", "").strip()
            if not symbol:
                continue
            
            if symbol_info.get("status") != "TRADING":
                continue
            
            base_asset = symbol_info.get("baseAsset", "").upper().strip()
            quote_asset = symbol_info.get("quoteAsset", "").upper().strip()
            
            if not base_asset or not quote_asset or base_asset == quote_asset:
                invalid_count += 1
                continue
            
            if quote_asset not in TARGET_QUOTES:
                filtered_count += 1
                continue
            
            if symbol in seen_symbols:
                continue
            
            seen_symbols.add(symbol)
            
            symbols[symbol] = {
                "symbol": symbol,
                "baseAsset": base_asset,
                "quoteAsset": quote_asset,
                "status": "TRADING"
            }
        
        logger.info(
            f"Loaded {len(symbols)} active spot markets with quotes {sorted(TARGET_QUOTES)} "
            f"(filtered {filtered_count} non-target quotes, {invalid_count} invalid pairs)"
        )
        
        # Log breakdown by quote
        quote_counts = {}
        for sym_info in symbols.values():
            quote = sym_info.get("quoteAsset", "UNKNOWN")
            quote_counts[quote] = quote_counts.get(quote, 0) + 1
        
        logger.info(f"Symbol breakdown by quote: {dict(sorted(quote_counts.items()))}")
        
        return symbols
        
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}", exc_info=True)
        logger.info("Falling back to FastSymbolLoader")
        loader = get_symbol_loader(cache_ttl_seconds=300)
        return loader.load_symbols(
            client, 
            base_asset=S.BASE_ASSET, 
            use_cache=True, 
            include_all_pairs=False
        )


def _depth_snapshot(client: BinanceClient, symbol: str, depth: int = 20, use_cache: bool = True) -> Optional[dict]:
    """Get depth snapshot from websocket cache first, then REST with caching and retries."""
    # Try websocket bookTicker cache first
    ws_data = _book_ticker_stream.get(symbol)
    if ws_data:
        return ws_data

    if use_cache:
        cached = _depth_cache.get(symbol)
        if cached is not None:
            return cached
    
    backoff = 0.25
    for attempt in range(3):
        try:
            d = client.depth(symbol, limit=depth)
            if not d.get("asks") or not d.get("bids"):
                return None
            ask_p, ask_q = float(d["asks"][0][0]), float(d["asks"][0][1])
            bid_p, bid_q = float(d["bids"][0][0]), float(d["bids"][0][1])
            total_ask_qty = sum(float(q) for _, q in d["asks"])
            total_bid_qty = sum(float(q) for _, q in d["bids"])
            cap_ask_quote = sum(float(p) * float(q) for p, q in d["asks"])
            cap_bid_quote = sum(float(p) * float(q) for p, q in d["bids"])
            result = {
                "ask_price": ask_p,
                "ask_qty": ask_q,
                "bid_price": bid_p,
                "bid_qty": bid_q,
                "total_ask_qty": total_ask_qty,
                "total_bid_qty": total_bid_qty,
                "cap_ask_quote": cap_ask_quote,
                "cap_bid_quote": cap_bid_quote,
            }
            if use_cache:
                _depth_cache.set(symbol, result)
            return result
        except Exception as e:
            error_str = str(e).lower()
            if "too much request weight" in error_str or "banned" in error_str or "429" in error_str:
                if attempt == 0:
                    logger.warning(f"Rate limit detected while fetching {symbol}, waiting longer...")
                time.sleep(min(backoff * 4, 5.0))
            else:
                time.sleep(backoff)
            backoff *= 2
    return None


def _try_random_triangle(
    client: BinanceClient, 
    symbols: Dict[str, dict], 
    base: str, 
    x: str, 
    y: str,
    min_profit_pct: float,
    max_profit_pct: float,
    depth_cache: Dict[str, Optional[dict]] = None,
    stats: dict = None
) -> Optional[CandidateRoute]:
    """
    Simple triangle check for random exploration.
    Returns route if profit is within target range.
    Middle leg must include at least one major coin.
    """
    def get_depth(symbol: str) -> Optional[dict]:
        if depth_cache is not None:
            return depth_cache.get(symbol)
        return _depth_snapshot(client, symbol, use_cache=True)
    
    # Ensure middle leg includes at least one major coin
    # The middle leg is the cross pair between x and y, so either x or y must be a major coin
    x_upper = x.upper()
    y_upper = y.upper()
    if x_upper not in MAJOR_COINS and y_upper not in MAJOR_COINS:
        if stats is not None:
            stats["filtered_no_major_coin"] = stats.get("filtered_no_major_coin", 0) + 1
        return None
    
    # Required symbols
    symbol1 = f"{x}{base}"  # X/USDT
    symbol3 = f"{y}{base}"  # Y/USDT
    
    # Check base pairs exist
    if symbol1 not in symbols or symbol3 not in symbols:
        return None
    
    # Get depths
    d1 = get_depth(symbol1)
    d3 = get_depth(symbol3)
    
    if not d1 or not d3:
        return None
    
    # Check for cross pair (Y/X or X/Y)
    cross_pair = None
    inverted = False
    
    # Try Y/X first (preferred)
    symbol2 = f"{y}{x}"
    if symbol2 in symbols:
        cross_pair = symbol2
        d2 = get_depth(symbol2)
    else:
        # Try X/Y (inverted)
        symbol2_alt = f"{x}{y}"
        if symbol2_alt in symbols:
            orig = get_depth(symbol2_alt)
            if orig:
                try:
                    inv_ask = 1.0 / orig["bid_price"] if orig["bid_price"] else None
                    inv_bid = 1.0 / orig["ask_price"] if orig["ask_price"] else None
                except Exception:
                    inv_ask = inv_bid = None
                if inv_ask and inv_bid:
                    d2 = {
                        "ask_price": inv_ask,
                        "bid_price": inv_bid,
                        "ask_qty": orig.get("bid_qty", 0),
                        "bid_qty": orig.get("ask_qty", 0),
                        "total_ask_qty": orig.get("total_bid_qty", 0),
                        "total_bid_qty": orig.get("total_ask_qty", 0),
                        "cap_ask_quote": 0,
                        "cap_bid_quote": 0,
                    }
                    cross_pair = symbol2_alt
                    inverted = True
        else:
            # No cross pair, use synthetic
            try:
                synthetic_ask = d3["ask_price"] / d1["bid_price"] if d1["bid_price"] > 0 else None
                synthetic_bid = d3["bid_price"] / d1["ask_price"] if d1["ask_price"] > 0 else None
                
                if synthetic_ask and synthetic_bid and synthetic_ask > 0 and synthetic_bid > 0:
                    d2 = {
                        "ask_price": synthetic_ask,
                        "bid_price": synthetic_bid,
                        "ask_qty": min(d1["bid_qty"], d3["ask_qty"]),
                        "bid_qty": min(d1["ask_qty"], d3["bid_qty"]),
                        "total_ask_qty": min(d1["total_bid_qty"], d3["total_ask_qty"]),
                        "total_bid_qty": min(d1["total_ask_qty"], d3["total_bid_qty"]),
                        "cap_ask_quote": 0,
                        "cap_bid_quote": 0,
                    }
                    cross_pair = f"{y}{x}"  # Synthetic
                else:
                    return None
            except Exception:
                return None
    
    if not cross_pair:
        return None
    
    # Calculate profit for both patterns
    # Pattern 1: BUY-BUY-SELL
    amount = 1.0
    amount = amount / d1["ask_price"]  # Buy X with USDT
    amount = amount / d2["ask_price"]  # Buy Y with X
    amount = amount * d3["bid_price"]  # Sell Y for USDT
    profit1 = (amount - 1.0) * 100.0
    
    # Pattern 2: BUY-SELL-SELL
    amount = 1.0
    amount = amount / d1["ask_price"]  # Buy X with USDT
    amount = amount * d2["bid_price"]  # Sell X for Y
    amount = amount * d3["bid_price"]  # Sell Y for USDT
    profit2 = (amount - 1.0) * 100.0
    
    # Use best profit
    gross_profit_pct = max(profit1, profit2)
    
    # Validate profit is reasonable
    if gross_profit_pct < -50 or gross_profit_pct > 50:
        return None
    
    # Check if profit is in target range
    if gross_profit_pct < min_profit_pct or gross_profit_pct > max_profit_pct:
        if stats is not None:
            stats["filtered_profit"] = stats.get("filtered_profit", 0) + 1
        return None
    
    # Determine which pattern gave the best profit
    best_pattern = "BUY-BUY-SELL" if profit1 >= profit2 else "BUY-SELL-SELL"
    
    # Calculate fees
    fee_rate = (S.FEE_RATE_BPS + S.EXTRA_FEE_BPS) / 10000.0
    total_fee = 3 * fee_rate
    gross_multiplier = 1.0 + (gross_profit_pct / 100.0)
    net_multiplier = gross_multiplier * (1.0 - total_fee)
    net_profit_pct = (net_multiplier - 1.0) * 100.0
    
    # Create route
    if best_pattern == "BUY-BUY-SELL":
        route = CandidateRoute(
            a=f"{x}/{base} buy",
            b=f"{y}/{x} buy",
            c=f"{y}/{base} sell",
            profit_pct=round(gross_profit_pct, 4),
            volume_usd=round(S.MAX_NOTIONAL_USD, 2),
        )
    else:
        route = CandidateRoute(
            a=f"{x}/{base} buy",
            b=f"{y}/{x} sell",
            c=f"{y}/{base} sell",
            profit_pct=round(gross_profit_pct, 4),
            volume_usd=round(S.MAX_NOTIONAL_USD, 2),
        )
    
    if stats is not None:
        stats["routes_found"] = stats.get("routes_found", 0) + 1
    
    logger.info(f"RANDOM FOUND: {route.a} → {route.b} → {route.c}, profit: {route.profit_pct}%")
    
    return route


def find_random_routes(
    *, 
    min_profit_pct: float, 
    max_profit_pct: float,
    available_balance: float = None,
    max_attempts: int = 2000,
    max_routes: int = 10,
    use_parallel: bool = True
) -> Tuple[List[CandidateRoute], dict]:
    """
    Pure random exploration for triangular arbitrage opportunities.
    No scoring, no prioritization - just random sampling.
    """
    start_time = time.time()
    
    stats = {
        "attempts": 0,
        "random_checks": 0,
        "routes_found": 0,
        "filtered_profit": 0,
        "filtered_missing_pairs": 0,
        "filtered_no_major_coin": 0,
        "unique_assets_tried": set(),
        "unique_pairs_tried": set(),
        "start_time": start_time,
    }
    
    try:
        client = _client()
        symbols = _load_symbols(client)
        base = S.BASE_ASSET.upper()
        
        logger.info(f"=== RANDOM EXPLORATION STARTED ===")
        logger.info(f"Target profit: {min_profit_pct}% - {max_profit_pct}%")
        logger.info(f"Max attempts: {max_attempts}")
        logger.info(f"Max routes to find: {max_routes}")
        
        # Get ALL assets that trade with USDT
        usdt_assets = []
        major_assets = []
        for sym, info in symbols.items():
            if info.get("status") != "TRADING":
                continue
            quote = info.get("quoteAsset", "").upper()
            base_asset = info.get("baseAsset", "").upper()
            
            if quote == base and base_asset != base:
                usdt_assets.append(base_asset)
                if base_asset in MAJOR_COINS:
                    major_assets.append(base_asset)
        
        logger.info(f"Found {len(usdt_assets)} assets trading with {base}")
        logger.info(f"Found {len(major_assets)} major coins trading with {base}: {major_assets[:10]}...")
        
        if len(usdt_assets) < 10:
            logger.warning(f"Need at least 10 assets trading with {base}")
            return [], stats

        top_symbols = [f"{asset}{base}" for asset in usdt_assets[:200]]
        _book_ticker_stream.start(top_symbols)
        logger.info(f"WebSocket started for {len(top_symbols)} symbols") 
        # Build cross pairs map for faster checking
        cross_pairs = set()
        for sym, info in symbols.items():
            if info.get("status") != "TRADING":
                continue
            quote = info.get("quoteAsset", "").upper()
            base_asset = info.get("baseAsset", "").upper()
            
            if base_asset in usdt_assets and quote in usdt_assets:
                cross_pairs.add((base_asset, quote))
                cross_pairs.add((quote, base_asset))  # Both directions
        
        logger.info(f"Found {len(cross_pairs)} cross pairs")
        
        # Random exploration
        cand: List[CandidateRoute] = []
        seen_pairs = set()
        
        # Pre-fetch some depths to speed up checks
        # Fetch depths for most common symbols
        common_symbols = []
        for asset in random.sample(usdt_assets, min(100, len(usdt_assets))):
            common_symbols.append(f"{asset}{base}")
        
        depth_cache = {}
        if common_symbols:
            logger.info(f"Pre-fetching depths for {len(common_symbols)} common symbols...")
            for symbol in common_symbols:
                depth = _depth_snapshot(client, symbol, use_cache=True)
                if depth:
                    depth_cache[symbol] = depth
        
        attempts = 0
        batch_size = 50
        
        while attempts < max_attempts and len(cand) < max_routes:
            batch_attempts = min(batch_size, max_attempts - attempts)
            attempts += batch_attempts
            
            # Generate random pairs for this batch
            # Ensure at least one asset in each pair is a major coin (for middle leg requirement)
            random_pairs = []
            for _ in range(batch_attempts):
                if len(usdt_assets) < 2:
                    break
                
                # Prefer pairs with major coins: 70% chance to include a major coin
                if major_assets and random.random() < 0.7:
                    # Pick one major coin and one random asset
                    x = random.choice(major_assets)
                    y = random.choice(usdt_assets)
                    # Ensure they're different
                    while y == x:
                        y = random.choice(usdt_assets)
                else:
                    # Random pair, but still check if at least one is major
                    x, y = random.sample(usdt_assets, 2)
                    # If neither is major, skip this pair (will be filtered later anyway)
                    if x not in MAJOR_COINS and y not in MAJOR_COINS:
                        continue
                
                pair_key = tuple(sorted([x, y]))
                
                if pair_key in seen_pairs:
                    continue
                
                seen_pairs.add(pair_key)
                stats["unique_assets_tried"].add(x)
                stats["unique_assets_tried"].add(y)
                stats["unique_pairs_tried"].add(pair_key)
                
                random_pairs.append((x, y))
            
            if not random_pairs:
                continue
            
            # Check triangles in parallel or sequentially
            if use_parallel and len(random_pairs) > 10:
                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = []
                    for x, y in random_pairs:
                        future = executor.submit(
                            _try_random_triangle,
                            client=client,
                            symbols=symbols,
                            base=base,
                            x=x,
                            y=y,
                            min_profit_pct=min_profit_pct,
                            max_profit_pct=max_profit_pct,
                            depth_cache=depth_cache,
                            stats=stats
                        )
                        futures.append(future)
                    
                    for future in as_completed(futures):
                        try:
                            route = future.result()
                            if route:
                                # Check for duplicates
                                route_key = (route.a, route.b, route.c)
                                if not any(r.a == route.a and r.b == route.b and r.c == route.c for r in cand):
                                    cand.append(route)
                                    logger.info(f"✅ Found route #{len(cand)}: {route.profit_pct}% profit")
                                    
                                    if len(cand) >= max_routes:
                                        break
                        except Exception as e:
                            logger.debug(f"Triangle check failed: {e}")
            else:
                # Sequential checking
                for x, y in random_pairs:
                    route = _try_random_triangle(
                        client=client,
                        symbols=symbols,
                        base=base,
                        x=x,
                        y=y,
                        min_profit_pct=min_profit_pct,
                        max_profit_pct=max_profit_pct,
                        depth_cache=depth_cache,
                        stats=stats
                    )
                    
                    if route:
                        # Check for duplicates
                        route_key = (route.a, route.b, route.c)
                        if not any(r.a == route.a and r.b == route.b and r.c == route.c for r in cand):
                            cand.append(route)
                            logger.info(f"✅ Found route #{len(cand)}: {route.profit_pct}% profit")
                            
                            if len(cand) >= max_routes:
                                break
            
            stats["attempts"] = attempts
            stats["random_checks"] = len(seen_pairs)
            
            # Log progress every 100 attempts
            if attempts % 100 == 0:
                elapsed = time.time() - start_time
                logger.info(f"Progress: {attempts}/{max_attempts} attempts, {len(cand)}/{max_routes} routes found, {elapsed:.1f}s elapsed")
                logger.info(f"Unique assets tried: {len(stats['unique_assets_tried'])}/{len(usdt_assets)}")
                logger.info(f"Unique pairs tried: {len(stats['unique_pairs_tried'])}")
        
        # Calculate final stats
        total_time = time.time() - start_time
        stats["total_time"] = total_time
        stats["routes_found"] = len(cand)
        
        if stats["attempts"] > 0:
            success_rate = (len(cand) / stats["attempts"]) * 100
            stats["success_rate"] = round(success_rate, 4)
        
        # Log results
        logger.info(f"=== RANDOM EXPLORATION COMPLETE ===")
        logger.info(f"Total attempts: {stats['attempts']}")
        logger.info(f"Routes found: {len(cand)}")
        logger.info(f"Success rate: {stats.get('success_rate', 0):.4f}%")
        logger.info(f"Total time: {total_time:.2f}s")
        logger.info(f"Unique assets explored: {len(stats['unique_assets_tried'])}")
        logger.info(f"Unique pairs explored: {len(stats['unique_pairs_tried'])}")
        if stats.get("filtered_no_major_coin", 0) > 0:
            logger.info(f"Filtered (no major coin in middle leg): {stats['filtered_no_major_coin']}")
        
        if cand:
            profits = [r.profit_pct for r in cand]
            logger.info(f"Profit range: {min(profits):.4f}% - {max(profits):.4f}%")
            logger.info(f"Average profit: {sum(profits)/len(profits):.4f}%")
            
            # Log all found routes
            for i, route in enumerate(cand, 1):
                logger.info(f"Route #{i}: {route.profit_pct}% - {route.a} → {route.b} → {route.c}")
        else:
            logger.warning("No profitable routes found in target range")
        
        # Sort by profit (highest first)
        cand.sort(key=lambda x: x.profit_pct, reverse=True)
        
        return cand[:max_routes], stats
        
    except Exception as e:
        logger.error(f"Error in random exploration: {e}", exc_info=True)
        return [], stats


# Keep your existing helper functions for backward compatibility
def find_candidate_routes(
    *, 
    min_profit_pct: float, 
    max_profit_pct: float,
    available_balance: float = None
) -> Tuple[List[CandidateRoute], dict]:
    """
    Wrapper to use random exploration by default.
    """
    return find_random_routes(
        min_profit_pct=min_profit_pct,
        max_profit_pct=max_profit_pct,
        available_balance=available_balance,
        max_attempts=getattr(S, "MAX_RANDOM_ATTEMPTS", 2000),
        max_routes=getattr(S, "MAX_ROUTES_TO_RETURN", 10),
        use_parallel=True
    )


def _parse_leg(label: str) -> Tuple[str, str, str]:
    """Parse leg label like "BTC/USDT buy" into (base, quote, side)"""
    try:
        label = label.strip()
        if ":" in label:
            pair, side = label.split(":", 1)
            pair = pair.strip()
            side = side.strip()
        else:
            parts = label.split()
            if len(parts) < 2:
                raise ValueError(f"Invalid leg format: '{label}'")
            pair = parts[0]
            side = parts[1]
        
        if "/" not in pair:
            raise ValueError(f"Invalid pair format: '{pair}'")
        
        base, quote = pair.split("/")
        return base.upper(), quote.upper(), side.lower()
    except Exception as e:
        logger.error(f"Error parsing leg '{label}': {e}")
        raise


def revalidate_route(route: CandidateRoute) -> Optional[CandidateRoute]:
    """Revalidate a route with fresh market data"""
    try:
        a_base, a_quote, _ = _parse_leg(route.a)
        c_base, c_quote, _ = _parse_leg(route.c)
        base = S.BASE_ASSET.upper()
        
        x = a_base
        y = c_base
        
        client = _client()
        symbols = _load_symbols(client)
        
        # Use original profit as reference
        original_profit = route.profit_pct
        min_prof = max(0.01, original_profit * 0.5)
        max_prof = 50.0
        
        # Re-run triangle check
        new_route = _try_random_triangle(
            client, 
            symbols, 
            base, 
            x, 
            y,
            min_profit_pct=min_prof,
            max_profit_pct=max_prof
        )
        
        if new_route:
            logger.info(f"Route revalidated: {x}/{base}->{y}/{x}->{y}/{base} profit={new_route.profit_pct:.4f}%")
        else:
            logger.warning(f"Route validation failed: {x}/{base}->{y}/{x}->{y}/{base}")
        
        return new_route
    except Exception as e:
        logger.error(f"Error revalidating route: {e}", exc_info=True)
        return None


def debug_random_exploration():
    """Debug function to test random exploration"""
    print("\n=== TESTING RANDOM EXPLORATION ===")
    
    # Test with wide profit range to see what's available
    routes, stats = find_random_routes(
        min_profit_pct=0.1,
        max_profit_pct=10.0,
        max_attempts=500,
        max_routes=5,
        use_parallel=False
    )
    
    print(f"\nResults: {len(routes)} routes found")
    for i, route in enumerate(routes, 1):
        print(f"{i}. {route.profit_pct:.4f}% - {route.a} → {route.b} → {route.c}")
    
    print(f"\nStats:")
    print(f"  Attempts: {stats.get('attempts', 0)}")
    print(f"  Unique assets: {len(stats.get('unique_assets_tried', set()))}")
    print(f"  Unique pairs: {len(stats.get('unique_pairs_tried', set()))}")
    print(f"  Success rate: {stats.get('success_rate', 0):.4f}%")
    
    return routes, stats


# Add to settings for configuration:
# MAX_RANDOM_ATTEMPTS = 2000  # How many random pairs to try
# USE_RANDOM_EXPLORATION = True  # Set to True to use random instead of priority-based