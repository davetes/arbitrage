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

# Only these coins are allowed as bridge (middle-leg) assets
# Patterns:
#   1) MAJOR/USDT -> X/MAJOR -> X/USDT  (Example: BTC/USDT -> RAD/BTC -> RAD/USDT)
#   2) X/USDT -> MAJOR/X -> MAJOR/USDT  (Example: RUNE/USDT -> RUNE/BNB -> BNB/USDT)
MAJOR_COINS = {"BTC", "ETH", "BNB"}


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


def _try_triangle_pattern(
    client: BinanceClient, 
    symbols: Dict[str, dict], 
    base: str, 
    major: str, 
    alt: str,
    min_profit_pct: float,
    max_profit_pct: float,
    depth_cache: Dict[str, Optional[dict]] = None,
    stats: dict = None
) -> Optional[CandidateRoute]:
    """
    Check triangle patterns exactly as shown in screenshots:
    Pattern 1: MAJOR/USDT -> ALT/MAJOR -> ALT/USDT  (Buy-Buy-Sell)
    Pattern 2: ALT/USDT -> ALT/MAJOR -> MAJOR/USDT  (Buy-Sell-Sell)
    
    Returns route if profit is within target range.
    """
    def get_depth(symbol: str) -> Optional[dict]:
        if depth_cache is not None:
            return depth_cache.get(symbol)
        return _depth_snapshot(client, symbol, use_cache=True)
    
    # Validate inputs
    if major not in MAJOR_COINS:
        return None
    
    # PATTERN 1: MAJOR/BASE -> ALT/MAJOR -> ALT/BASE
    pattern1_symbols = [
        f"{major}{base}",    # MAJOR/BASE (e.g., BTC/USDT)
        f"{alt}{major}",     # ALT/MAJOR  (e.g., RAD/BTC)
        f"{alt}{base}"       # ALT/BASE   (e.g., RAD/USDT)
    ]
    
    # PATTERN 2: ALT/BASE -> ALT/MAJOR -> MAJOR/BASE
    pattern2_symbols = [
        f"{alt}{base}",      # ALT/BASE   (e.g., RUNE/USDT)
        f"{alt}{major}",     # ALT/MAJOR  (e.g., RUNE/BNB)
        f"{major}{base}"     # MAJOR/BASE (e.g., BNB/USDT)
    ]
    
    # Check if Pattern 1 symbols exist
    pattern1_valid = all(sym in symbols for sym in pattern1_symbols)
    pattern2_valid = all(sym in symbols for sym in pattern2_symbols)
    
    if not pattern1_valid and not pattern2_valid:
        # Check for inverted cross pair (MAJOR/ALT instead of ALT/MAJOR)
        alt_major_inverted = f"{major}{alt}"
        if alt_major_inverted in symbols:
            # We have MAJOR/ALT instead of ALT/MAJOR, but we can still try patterns
            pattern1_symbols[1] = alt_major_inverted  # Use MAJOR/ALT for pattern 1
            pattern2_symbols[1] = alt_major_inverted  # Use MAJOR/ALT for pattern 2
            pattern1_valid = all(sym in symbols for sym in pattern1_symbols)
            pattern2_valid = all(sym in symbols for sym in pattern2_symbols)
    
    if not pattern1_valid and not pattern2_valid:
        return None
    
    best_route = None
    best_profit = -999
    
    # Try Pattern 1: MAJOR/BASE -> ALT/MAJOR -> ALT/BASE
    if pattern1_valid:
        d1 = get_depth(pattern1_symbols[0])  # MAJOR/BASE
        d2 = get_depth(pattern1_symbols[1])  # ALT/MAJOR or MAJOR/ALT
        d3 = get_depth(pattern1_symbols[2])  # ALT/BASE
        
        if d1 and d2 and d3:
            # Check if we have ALT/MAJOR or MAJOR/ALT
            is_inverted = pattern1_symbols[1].startswith(major)
            
            if is_inverted:
                # We have MAJOR/ALT, need to invert for calculation
                # For Pattern 1 with MAJOR/ALT: Buy MAJOR/BASE -> Sell MAJOR/ALT -> Buy ALT/BASE? Wait, that's wrong.
                # Actually, Pattern 1 needs: Buy MAJOR, Buy ALT with MAJOR
                # If we have MAJOR/ALT, then selling MAJOR/ALT means buying ALT with MAJOR
                # So: 1. Buy MAJOR with BASE, 2. Sell MAJOR for ALT, 3. Sell ALT for BASE
                amount = 1.0
                amount = amount / d1["ask_price"]        # Buy MAJOR with BASE
                amount = amount * d2["bid_price"]        # Sell MAJOR for ALT (using MAJOR/ALT bid)
                amount = amount * d3["bid_price"]        # Sell ALT for BASE
                profit1 = (amount - 1.0) * 100.0
            else:
                # Normal ALT/MAJOR pair
                amount = 1.0
                amount = amount / d1["ask_price"]        # Buy MAJOR with BASE
                amount = amount / d2["ask_price"]        # Buy ALT with MAJOR
                amount = amount * d3["bid_price"]        # Sell ALT for BASE
                profit1 = (amount - 1.0) * 100.0
            
            if -50 < profit1 < 50 and profit1 > best_profit:
                best_profit = profit1
                if is_inverted:
                    best_route = CandidateRoute(
                        a=f"{major}/{base} buy",
                        b=f"{major}/{alt} sell",  # Selling MAJOR/ALT means buying ALT with MAJOR
                        c=f"{alt}/{base} sell",
                        profit_pct=round(profit1, 4),
                        volume_usd=round(S.MAX_NOTIONAL_USD, 2),
                    )
                else:
                    best_route = CandidateRoute(
                        a=f"{major}/{base} buy",
                        b=f"{alt}/{major} buy",
                        c=f"{alt}/{base} sell",
                        profit_pct=round(profit1, 4),
                        volume_usd=round(S.MAX_NOTIONAL_USD, 2),
                    )
    
    # Try Pattern 2: ALT/BASE -> ALT/MAJOR -> MAJOR/BASE
    if pattern2_valid:
        d1 = get_depth(pattern2_symbols[0])  # ALT/BASE
        d2 = get_depth(pattern2_symbols[1])  # ALT/MAJOR or MAJOR/ALT
        d3 = get_depth(pattern2_symbols[2])  # MAJOR/BASE
        
        if d1 and d2 and d3:
            # Check if we have ALT/MAJOR or MAJOR/ALT
            is_inverted = pattern2_symbols[1].startswith(major)
            
            if is_inverted:
                # We have MAJOR/ALT, need to invert for calculation
                # For Pattern 2 with MAJOR/ALT: Buy ALT/BASE -> Buy MAJOR/ALT -> Sell MAJOR/BASE
                amount = 1.0
                amount = amount / d1["ask_price"]        # Buy ALT with BASE
                amount = amount / d2["ask_price"]        # Buy MAJOR with ALT (using MAJOR/ALT ask)
                amount = amount * d3["bid_price"]        # Sell MAJOR for BASE
                profit2 = (amount - 1.0) * 100.0
            else:
                # Normal ALT/MAJOR pair
                amount = 1.0
                amount = amount / d1["ask_price"]        # Buy ALT with BASE
                amount = amount * d2["bid_price"]        # Sell ALT for MAJOR
                amount = amount * d3["bid_price"]        # Sell MAJOR for BASE
                profit2 = (amount - 1.0) * 100.0
            
            if -50 < profit2 < 50 and profit2 > best_profit:
                best_profit = profit2
                if is_inverted:
                    best_route = CandidateRoute(
                        a=f"{alt}/{base} buy",
                        b=f"{major}/{alt} buy",  # Buying MAJOR/ALT means selling ALT for MAJOR
                        c=f"{major}/{base} sell",
                        profit_pct=round(profit2, 4),
                        volume_usd=round(S.MAX_NOTIONAL_USD, 2),
                    )
                else:
                    best_route = CandidateRoute(
                        a=f"{alt}/{base} buy",
                        b=f"{alt}/{major} sell",
                        c=f"{major}/{base} sell",
                        profit_pct=round(profit2, 4),
                        volume_usd=round(S.MAX_NOTIONAL_USD, 2),
                    )
    
    # Check if best profit is in target range
    if best_route and min_profit_pct <= best_route.profit_pct <= max_profit_pct:
        if stats is not None:
            stats["routes_found"] = stats.get("routes_found", 0) + 1
        logger.info(f"FOUND: {best_route.a} → {best_route.b} → {best_route.c}, profit: {best_route.profit_pct}%")
        return best_route
    elif best_route:
        if stats is not None:
            stats["filtered_profit"] = stats.get("filtered_profit", 0) + 1
    
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
    Wrapper for backward compatibility - calls the new pattern-based function.
    Determines which coin is major and which is alt, then calls _try_triangle_pattern.
    """
    # Determine which is major and which is alt
    if x in MAJOR_COINS and y not in MAJOR_COINS:
        # x is major, y is alt
        return _try_triangle_pattern(
            client, symbols, base, x, y, 
            min_profit_pct, max_profit_pct, depth_cache, stats
        )
    elif y in MAJOR_COINS and x not in MAJOR_COINS:
        # y is major, x is alt
        return _try_triangle_pattern(
            client, symbols, base, y, x, 
            min_profit_pct, max_profit_pct, depth_cache, stats
        )
    else:
        # Both are majors or both are alts - invalid for our patterns
        if stats is not None:
            stats["filtered_major_pattern"] = stats.get("filtered_major_pattern", 0) + 1
        return None


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
    Exhaustive exploration for triangular arbitrage opportunities.
    Tries all valid (major, alt) combinations instead of random sampling.
    """
    start_time = time.time()
    
    stats = {
        "attempts": 0,
        "random_checks": 0,
        "routes_found": 0,
        "filtered_profit": 0,
        "filtered_missing_pairs": 0,
        "filtered_major_pattern": 0,
        "unique_assets_tried": set(),
        "unique_pairs_tried": set(),
        "start_time": start_time,
    }
    
    try:
        client = _client()
        symbols = _load_symbols(client)
        base = S.BASE_ASSET.upper()
        
        logger.info(f"=== EXHAUSTIVE EXPLORATION STARTED ===")
        logger.info(f"Target profit: {min_profit_pct}% - {max_profit_pct}%")
        logger.info(f"Max routes to return: {max_routes}")
        
        # Get ALL assets that trade with BASE (typically USDT)
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
        if not major_assets:
            logger.warning("No major coins (BTC/ETH/BNB) trading with base asset; cannot build required routes")
            return [], stats
        
        if len(usdt_assets) < 10:
            logger.warning(f"Need at least 10 assets trading with {base}")
            return [], stats

        top_symbols = [f"{asset}{base}" for asset in usdt_assets[:200]]
        _book_ticker_stream.start(top_symbols)
        logger.info(f"WebSocket started for {len(top_symbols)} symbols") 
        
        # Also include major/alt cross pairs in WebSocket
        cross_symbols = []
        for major in major_assets:
            for asset in usdt_assets:
                if asset == major or asset in MAJOR_COINS:
                    continue
                # Check both directions
                if f"{asset}{major}" in symbols:
                    cross_symbols.append(f"{asset}{major}")
                elif f"{major}{asset}" in symbols:
                    cross_symbols.append(f"{major}{asset}")
        
        if cross_symbols:
            _book_ticker_stream.start(list(set(top_symbols + cross_symbols[:500])))
            logger.info(f"WebSocket updated with {len(cross_symbols[:500])} cross pairs")
        
        # Exhaustive exploration setup
        cand: List[CandidateRoute] = []
        
        # Pre-fetch some depths to speed up checks
        # Fetch depths for most common symbols
        common_symbols = []
        for asset in random.sample(usdt_assets, min(100, len(usdt_assets))):
            common_symbols.append(f"{asset}{base}")
        
        # Add major/base pairs
        for major in major_assets:
            common_symbols.append(f"{major}{base}")
        
        depth_cache = {}
        if common_symbols:
            logger.info(f"Pre-fetching depths for {len(common_symbols)} common symbols...")
            for symbol in common_symbols:
                depth = _depth_snapshot(client, symbol, use_cache=True)
                if depth:
                    depth_cache[symbol] = depth

        # Build full list of (major, alt) pairs to test
        all_pairs: List[Tuple[str, str]] = []
        seen_pairs: set = set()
        
        # Generate all valid (major, alt) combinations
        for major in set(major_assets):
            for alt in set(usdt_assets):
                if alt == major or alt in MAJOR_COINS:
                    continue
                
                pair_key = (major, alt)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                all_pairs.append((major, alt))
                stats["unique_assets_tried"].add(major)
                stats["unique_assets_tried"].add(alt)
                stats["unique_pairs_tried"].add(tuple(sorted([major, alt])))
        
        stats["attempts"] = len(all_pairs)
        stats["random_checks"] = len(all_pairs)
        logger.info(f"Planned exhaustive checks for {len(all_pairs)} (major, alt) combinations")

        # Check triangles in parallel or sequentially over all pairs
        if use_parallel and len(all_pairs) > 10:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for major, alt in all_pairs:
                    futures.append(
                        executor.submit(
                            _try_triangle_pattern,
                            client=client,
                            symbols=symbols,
                            base=base,
                            major=major,
                            alt=alt,
                            min_profit_pct=min_profit_pct,
                            max_profit_pct=max_profit_pct,
                            depth_cache=depth_cache,
                            stats=stats,
                        )
                    )

                for i, future in enumerate(as_completed(futures), start=1):
                    try:
                        route = future.result()
                        if route:
                            route_key = (route.a, route.b, route.c)
                            if not any(
                                r.a == route.a and r.b == route.b and r.c == route.c
                                for r in cand
                            ):
                                cand.append(route)
                                logger.info(
                                    f"✅ Found route #{len(cand)}: {route.profit_pct}% profit"
                                )
                    except Exception as e:
                        logger.debug(f"Triangle check failed: {e}")

                    # Periodic progress logging
                    if i % 100 == 0:
                        elapsed = time.time() - start_time
                        logger.info(
                            f"Progress: {i}/{len(all_pairs)} combinations checked, "
                            f"{len(cand)} routes found, {elapsed:.1f}s elapsed"
                        )
        else:
            # Sequential exhaustive checking
            for i, (major, alt) in enumerate(all_pairs, start=1):
                route = _try_triangle_pattern(
                    client=client,
                    symbols=symbols,
                    base=base,
                    major=major,
                    alt=alt,
                    min_profit_pct=min_profit_pct,
                    max_profit_pct=max_profit_pct,
                    depth_cache=depth_cache,
                    stats=stats,
                )

                if route:
                    route_key = (route.a, route.b, route.c)
                    if not any(
                        r.a == route.a and r.b == route.b and r.c == route.c
                        for r in cand
                    ):
                        cand.append(route)
                        logger.info(
                            f"✅ Found route #{len(cand)}: {route.profit_pct}% profit"
                        )

                if i % 100 == 0:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Progress: {i}/{len(all_pairs)} combinations checked, "
                        f"{len(cand)} routes found, {elapsed:.1f}s elapsed"
                    )
        
        # Calculate final stats
        total_time = time.time() - start_time
        stats["total_time"] = total_time
        stats["routes_found"] = len(cand)
        
        if stats["attempts"] > 0:
            success_rate = (len(cand) / stats["attempts"]) * 100
            stats["success_rate"] = round(success_rate, 4)
        
        # Log results
        logger.info(f"=== EXPLORATION COMPLETE ===")
        logger.info(f"Total checks: {stats['attempts']}")
        logger.info(f"Routes found: {len(cand)}")
        logger.info(f"Success rate: {stats.get('success_rate', 0):.4f}%")
        logger.info(f"Total time: {total_time:.2f}s")
        logger.info(f"Unique assets explored: {len(stats['unique_assets_tried'])}")
        logger.info(f"Unique pairs explored: {len(stats['unique_pairs_tried'])}")
        if stats.get("filtered_major_pattern", 0) > 0:
            logger.info(f"Filtered (invalid major/non-major pattern): {stats['filtered_major_pattern']}")
        
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
        
        # Extract major and alt from the route
        # Pattern 1: MAJOR/BASE buy -> ALT/MAJOR buy -> ALT/BASE sell
        # Pattern 2: ALT/BASE buy -> ALT/MAJOR sell -> MAJOR/BASE sell
        
        if "buy" in route.a and "buy" in route.b and "sell" in route.c:
            # Pattern 1
            major = a_base  # First leg base is major
            alt = c_base    # Third leg base is alt
        elif "buy" in route.a and "sell" in route.b and "sell" in route.c:
            # Pattern 2
            major = c_base  # Third leg base is major
            alt = a_base    # First leg base is alt
        else:
            logger.warning(f"Cannot determine pattern for route: {route}")
            return None
        
        client = _client()
        symbols = _load_symbols(client)
        
        # Use original profit as reference
        original_profit = route.profit_pct
        min_prof = max(0.01, original_profit * 0.5)
        max_prof = 50.0
        
        # Re-run triangle check
        new_route = _try_triangle_pattern(
            client, 
            symbols, 
            base, 
            major, 
            alt,
            min_profit_pct=min_prof,
            max_profit_pct=max_prof
        )
        
        if new_route:
            logger.info(f"Route revalidated: {major}/{base}->{alt}/{major}->{alt}/{base} profit={new_route.profit_pct:.4f}%")
        else:
            logger.warning(f"Route validation failed: {major}/{base}->{alt}/{major}->{alt}/{base}")
        
        return new_route
    except Exception as e:
        logger.error(f"Error revalidating route: {e}", exc_info=True)
        return None


def debug_random_exploration():
    """Debug function to test random exploration"""
    print("\n=== TESTING EXPLORATION ===")
    
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