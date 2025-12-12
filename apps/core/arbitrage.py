from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from binance.spot import Spot as BinanceClient
try:
    from binance.websocket.spot.websocket_client import SpotWebsocketClient
except Exception:  # Dependency may be missing in some deployments
    SpotWebsocketClient = None
from arbbot import settings as S
from .symbol_loader import get_symbol_loader
import time
import logging
import random
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


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
    """Websocket-based best bid/ask cache with persistent connection and combined streams."""

    def __init__(self, ttl_seconds: float = 5.0):
        self.ttl = ttl_seconds
        self.client: Optional[SpotWebsocketClient] = None
        self.lock = threading.Lock()
        self.cache: Dict[str, Tuple[dict, float]] = {}
        self.running = False
        self.subscribed_symbols: set = set()
        self.last_reconnect_time = 0.0
        self.reconnect_interval = 300.0  # Reconnect every 5 minutes to prevent stale connections
        self._reconnect_thread: Optional[threading.Thread] = None
        self._stop_reconnect = False

    def _on_message(self, _, message: str):
        try:
            data = json.loads(message)
            if not isinstance(data, dict):
                return
            
            # Handle both single stream and combined stream formats
            # Combined stream format: {"stream": "btcusdt@bookTicker", "data": {...}}
            # Single stream format: {"s": "BTCUSDT", "b": "...", ...}
            
            if "data" in data:
                # Combined stream format
                ticker_data = data["data"]
                symbol = ticker_data.get("s") or ticker_data.get("symbol")
            else:
                # Single stream format
                symbol = data.get("s") or data.get("symbol")
                ticker_data = data
            
            if not symbol:
                return
                
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
        except Exception as e:
            logger.debug(f"Error processing WebSocket message: {e}")
            return

    def _ensure_connection(self, symbols: List[str], **kwargs):
        """Ensure WebSocket connection is active and subscribed to required symbols"""
        if SpotWebsocketClient is None:
            logger.warning("SpotWebsocketClient unavailable; skipping websocket bookTicker startup.")
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
            
            # Use combined stream format for efficiency (up to 200 streams per connection)
            # Binance format: wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/...
            stream_symbols = [s.lower() for s in symbols]
            
            # Binance allows up to 200 streams per combined stream
            # Split into chunks if needed
            max_streams_per_connection = 200
            if len(stream_symbols) <= max_streams_per_connection:
                # Single connection for all symbols using combined stream
                streams = "/".join([f"{sym}@bookTicker" for sym in stream_symbols])
                # Use combined stream endpoint
                combined_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
                kwargs_with_url = {**kwargs, "stream_url": combined_url}
                self.client = SpotWebsocketClient(on_message=self._on_message, **kwargs_with_url)
                self.client.start()
                
                self.subscribed_symbols = symbols_set
                self.running = True
                self.last_reconnect_time = now
                logger.info(f"WebSocket connected: {len(stream_symbols)} symbols via combined stream")
            else:
                # Multiple connections for large symbol sets - use first 200 (highest priority)
                logger.info(f"Large symbol set ({len(stream_symbols)}), using top {max_streams_per_connection} symbols")
                limited_symbols = stream_symbols[:max_streams_per_connection]
                streams = "/".join([f"{sym}@bookTicker" for sym in limited_symbols])
                combined_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
                kwargs_with_url = {**kwargs, "stream_url": combined_url}
                self.client = SpotWebsocketClient(on_message=self._on_message, **kwargs_with_url)
                self.client.start()
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
_book_ticker_stream = _BookTickerStream(ttl_seconds=5.0)  # Increased TTL for better cache hit rate


@dataclass
class CandidateRoute:
    a: str  # leg1 label e.g., "BTC/USDT buy"
    b: str  # leg2 label e.g., "ETH/BTC buy"
    c: str  # leg3 label e.g., "ETH/USDT sell"
    profit_pct: float
    volume_usd: float


@dataclass
class PerformanceMetrics:
    """Track arbitrage scanner performance"""
    scan_count: int = 0
    routes_found: int = 0
    avg_profit: float = 0.0
    max_profit: float = 0.0
    success_rate: float = 0.0
    ws_coverage: float = 0.0
    scan_time_ms: float = 0.0
    symbols_loaded: int = 0
    triangles_checked: int = 0
    
    def update(self, routes: List[CandidateRoute], scan_time: float, ws_hits: int, total_symbols: int, triangles: int):
        self.scan_count += 1
        self.routes_found += len(routes)
        self.scan_time_ms = scan_time * 1000
        self.ws_coverage = (ws_hits / total_symbols * 100) if total_symbols > 0 else 0
        self.triangles_checked = triangles
        
        if routes:
            profits = [r.profit_pct for r in routes]
            self.avg_profit = sum(profits) / len(profits)
            self.max_profit = max(profits)
        
        self.success_rate = (self.routes_found / self.scan_count) if self.scan_count > 0 else 0


# Global performance tracker
_performance = PerformanceMetrics()


@dataclass
class TriangularArbResult:
    forward_profit_pct: float
    backward_profit_pct: float
    best_path: str
    best_profit_pct: float
    pattern: str
    forward_steps: List[str]
    backward_steps: List[str]
    warnings: List[str]


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
    """
    Load all active spot markets with target quotes (USDT, BTC, ETH, BNB, FDUSD, USDC).
    Filters out invalid pairs (e.g., USDT:USDT) and ensures comprehensive coverage.
    Target: ~2,200+ symbols total.
    """
    # Target quotes for arbitrage opportunities
    TARGET_QUOTES = {"USDT", "BTC", "ETH", "BNB", "FDUSD", "USDC"}
    
    try:
        exchange_info = client.exchange_info()
        symbols = {}
        seen_symbols = set()  # Track to prevent duplicates
        invalid_count = 0
        filtered_count = 0
        
        for symbol_info in exchange_info.get("symbols", []):
            symbol = symbol_info.get("symbol", "").strip()
            if not symbol:
                continue
            
            # Only include TRADING status (exclude BREAK, PRE_TRADING, etc.)
            if symbol_info.get("status") != "TRADING":
                continue
            
            # Extract base and quote assets
            base_asset = symbol_info.get("baseAsset", "").upper().strip()
            quote_asset = symbol_info.get("quoteAsset", "").upper().strip()
            
            # Filter out invalid pairs where base == quote (e.g., USDT:USDT)
            if not base_asset or not quote_asset or base_asset == quote_asset:
                invalid_count += 1
                continue
            
            # Only include pairs with target quotes
            if quote_asset not in TARGET_QUOTES:
                filtered_count += 1
                continue
            
            # Prevent duplicates (shouldn't happen, but safety check)
            if symbol in seen_symbols:
                logger.warning(f"Duplicate symbol detected: {symbol}")
                continue
            
            seen_symbols.add(symbol)
            
            # Store minimal symbol info for efficiency
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
        # Fallback to symbol loader
        logger.info("Falling back to FastSymbolLoader")
        loader = get_symbol_loader(cache_ttl_seconds=300)
        return loader.load_symbols(
            client, 
            base_asset=S.BASE_ASSET, 
            use_cache=True, 
            include_all_pairs=False  # Use filtered mode to get target quotes only
        )


def _top_assets_by_quote_volume(
    client: BinanceClient,
    symbols: Dict[str, dict],
    base_quote: str,
    top_n: int = 100,
) -> List[str]:
    """Return a list of asset tickers ranked by 24h quoteVolume against the given base quote."""
    try:
        stats = client.ticker_24hr()
        vol_map: Dict[str, float] = {}
        for s in stats:
            sym = s.get("symbol")
            if not sym or sym not in symbols:
                continue
            info = symbols[sym]
            if info.get("quoteAsset") != base_quote or info.get("status") != "TRADING":
                continue
            try:
                vol_map[sym] = float(s.get("quoteVolume", 0))
            except Exception:
                continue

        ranked: List[Tuple[str, float]] = []
        for sym, vol in vol_map.items():
            asset = symbols[sym].get("baseAsset")
            if asset and asset != base_quote:
                ranked.append((asset, vol))

        by_asset: Dict[str, float] = {}
        for asset, vol in ranked:
            by_asset[asset] = max(vol, by_asset.get(asset, 0.0))

        top = sorted(by_asset.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [a for a, _ in top]
    except Exception:
        return []


def _select_all_assets(
    client: BinanceClient,
    symbols: Dict[str, dict],
    quotes: List[str],
) -> Tuple[List[str], Dict[str, float]]:
    """
    Load ALL assets that trade with major quotes (2000+ symbols).
    First extracts all tradable assets from symbols dict, then uses ticker data only for volume ranking.
    """
    quotes_upper = {q.upper() for q in quotes}
    assets_set = set()
    volumes = {}
    
    # STEP 1: Extract ALL assets from symbols dict first (primary source)
    for sym, info in symbols.items():
        if info.get("status") != "TRADING":
            continue
        quote = info.get("quoteAsset", "").upper()
        if quote not in quotes_upper:
            continue
        base_asset = info.get("baseAsset", "").upper()
        if base_asset and base_asset != quote:
            assets_set.add(base_asset)
            # Initialize volume to 0, will be updated from ticker data if available
            volumes[base_asset] = volumes.get(base_asset, 0.0)
    
    logger.info(f"Extracted {len(assets_set)} assets from symbols dict")
    
    # STEP 2: Use ticker data ONLY for volume ranking (not for asset discovery)
    try:
        stats = client.ticker_24hr()
        ticker_symbol_map = {}
        for s in stats:
            sym = s.get("symbol", "").upper()
            if sym:
                ticker_symbol_map[sym] = s
        
        # Update volumes from ticker data for assets we already found
        for sym, info in symbols.items():
            if info.get("status") != "TRADING":
                continue
            quote = info.get("quoteAsset", "").upper()
            if quote not in quotes_upper:
                continue
            base_asset = info.get("baseAsset", "").upper()
            if base_asset and base_asset != quote and base_asset in assets_set:
                # Get volume from ticker data if available
                ticker_data = ticker_symbol_map.get(sym.upper())
                if ticker_data:
                    try:
                        vol = float(ticker_data.get("quoteVolume", 0))
                        # Keep maximum volume across all quote pairs for this asset
                        volumes[base_asset] = max(volumes.get(base_asset, 0.0), vol)
                    except Exception:
                        pass
        
        logger.info(f"Updated volumes for {len([v for v in volumes.values() if v > 0])} assets from ticker data")
    except Exception as e:
        logger.warning(f"Failed to fetch ticker data for volume ranking: {e}. Using symbols-only data.")
    
    # Return all assets (2000+) with volumes
    return list(assets_set), volumes


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
            # Check for rate limit errors
            if "too much request weight" in error_str or "banned" in error_str or "429" in error_str:
                if attempt == 0:  # Only log once
                    logger.warning(f"Rate limit detected while fetching {symbol}, waiting longer...")
                time.sleep(min(backoff * 4, 5.0))  # Longer wait for rate limits
            else:
                time.sleep(backoff)
            backoff *= 2
    return None


def _fetch_depth_parallel(client: BinanceClient, symbols: List[str], max_workers: int = 8) -> Dict[str, Optional[dict]]:
    """Fetch depths in parallel with WebSocket priority and rate limiting"""
    results = {}
    
    # First try WebSocket cache for all symbols
    ws_hits = 0
    rest_needed = []
    for symbol in symbols:
        ws_data = _book_ticker_stream.get(symbol)
        if ws_data:
            results[symbol] = ws_data
            ws_hits += 1
        else:
            rest_needed.append(symbol)
    
    if ws_hits > 0:
        logger.info(f"WebSocket cache hits: {ws_hits}/{len(symbols)} ({(ws_hits/len(symbols)*100):.1f}%)")
    
    if not rest_needed:
        return results
    
    # Fetch remaining via REST with optimized rate limiting
    # Binance rate limits: 1200 requests per minute (20 req/sec)
    # Use conservative 15 req/sec to avoid hitting limits
    request_delay = 1.0 / 15.0  # ~0.067 seconds between requests
    last_request_time = [0.0]
    rate_limit_errors = [0]
    
    def fetch_one(symbol: str):
        # Rate limiting
        elapsed = time.time() - last_request_time[0]
        if elapsed < request_delay:
            time.sleep(request_delay - elapsed)
        last_request_time[0] = time.time()
        
        try:
            return symbol, _depth_snapshot(client, symbol, use_cache=True)
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "rate limit" in error_str or "too many" in error_str:
                rate_limit_errors[0] += 1
                # Exponential backoff on rate limit
                time.sleep(min(2.0 ** rate_limit_errors[0], 10.0))
            raise
    
    # Process REST requests with controlled concurrency
    with ThreadPoolExecutor(max_workers=min(max_workers, 4)) as executor:  # Limit workers to avoid rate limits
        future_to_symbol = {executor.submit(fetch_one, sym): sym for sym in rest_needed}
        for future in as_completed(future_to_symbol):
            try:
                symbol, depth = future.result()
                results[symbol] = depth
            except Exception as e:
                logger.debug(f"Failed to fetch depth for {future_to_symbol[future]}: {e}")
    
    if rate_limit_errors[0] > 0:
        logger.warning(f"Encountered {rate_limit_errors[0]} rate limit errors during REST fetch")
    
    return results


def _try_triangle(
    client: BinanceClient, 
    symbols: Dict[str, dict], 
    base: str, 
    x: str, 
    y: str,
    min_profit_pct: float = None,
    max_profit_pct: float = None,
    depth_cache: Dict[str, Optional[dict]] = None,
    available_balance: float = None,
    stats: dict = None,
    require_intermediate: bool = True
) -> Optional[CandidateRoute]:
    """
    Triangle detection for Binance's actual pair formats.
    Route: base -> x -> y -> base
    Format required: XBASE (e.g., BTCUSDT), YX (e.g., ETHBTC), YBASE (e.g., ETHUSDT)
    
    If require_intermediate=False, allows routes without cross pair (direct only for non-major assets).
    """
    def get_depth(symbol: str) -> Optional[dict]:
        if depth_cache is not None:
            return depth_cache.get(symbol)
        return _depth_snapshot(client, symbol, use_cache=True)
    
    # Major assets that require intermediate pairs
    MAJOR_ASSETS = {"BTC", "ETH", "BNB", "USDT", "USDC", "FDUSD"}
    
    # We need these 3 symbols:
    # 1. X/base (e.g., BTCUSDT) - buy X with base
    # 2. Y/X (e.g., ETHBTC) - buy Y with X (or inverse if only XY exists) - OPTIONAL for non-major
    # 3. Y/base (e.g., ETHUSDT) - sell Y for base
    
    symbol1 = f"{x}{base}"  # Xbase (e.g., BTCUSDT)
    symbol2 = f"{y}{x}"     # YX preferred (e.g., ETHBTC)
    symbol2_alt = f"{x}{y}" # XY fallback (e.g., BTCETH)
    symbol3 = f"{y}{base}"  # Ybase (e.g., ETHUSDT)
    
    # Check base pairs exist
    if symbol1 not in symbols or symbol3 not in symbols:
        return None
    
    # Get depths for base pairs
    d1 = get_depth(symbol1)
    d3 = get_depth(symbol3)
    
    if not d1 or not d3:
        return None
    
    # For non-major assets, intermediate pair is optional
    # If both x and y are non-major, we can skip intermediate pair check
    is_major_x = x in MAJOR_ASSETS
    is_major_y = y in MAJOR_ASSETS
    
    # Only require intermediate pair if:
    # 1. require_intermediate is True (explicit requirement)
    # 2. At least one asset is major (for quality routes)
    need_intermediate = require_intermediate or is_major_x or is_major_y
    
    d2 = None
    inverted_cross = False
    
    if need_intermediate:
        # Try to get intermediate pair
        if symbol2 in symbols:
            d2 = get_depth(symbol2)
        elif symbol2_alt in symbols:
            # Use inverted depth from XY
            orig = get_depth(symbol2_alt)
            if orig:
                # Invert bids/asks: price flips, bids become asks
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
                    inverted_cross = True
        
        # If intermediate required but not found, skip
        if not d2:
            if stats is not None:
                stats["filtered_missing_pairs"] = stats.get("filtered_missing_pairs", 0) + 1
            return None
    else:
        # For non-major assets without intermediate pair, use synthetic cross rate
        # Calculate cross rate from base pairs: Y/X = (Y/base) / (X/base)
        try:
            x_base_ask = d1["ask_price"]  # Price to buy X with base
            x_base_bid = d1["bid_price"]   # Price to sell X for base
            y_base_ask = d3["ask_price"]  # Price to buy Y with base
            y_base_bid = d3["bid_price"]   # Price to sell Y for base
            
            # Synthetic Y/X calculation:
            # To buy Y with X: sell X for base (X/base bid), then buy Y with base (Y/base ask)
            # Y/X ask = (Y/base ask) / (X/base bid)
            # To sell Y for X: sell Y for base (Y/base bid), then buy X with base (X/base ask)
            # Y/X bid = (Y/base bid) / (X/base ask)
            synthetic_ask = y_base_ask / x_base_bid if x_base_bid > 0 else None
            synthetic_bid = y_base_bid / x_base_ask if x_base_ask > 0 else None
            
            # Validate synthetic prices are reasonable
            if (synthetic_ask and synthetic_bid and 
                synthetic_ask > 0 and synthetic_bid > 0 and
                synthetic_ask >= synthetic_bid):  # Ask should be >= bid
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
            else:
                return None
        except Exception:
            return None
    
    if not d2:
        return None
    
    # Pattern evaluation: Only valid patterns starting with BUY (we start with base asset)
    # Invalid patterns starting with SELL are removed - we don't have X to sell initially
    patterns = {
        ("BUY", "BUY", "SELL"): [
            ("ask", symbol1, d1, "BUY"),   # Buy X with base (USDT)
            ("ask", symbol2 if not inverted_cross else symbol2_alt, d2, "BUY"),   # Buy Y with X
            ("bid", symbol3, d3, "SELL"),  # Sell Y for base (USDT)
        ],
        ("BUY", "SELL", "SELL"): [
            ("ask", symbol1, d1, "BUY"),   # Buy X with base (USDT)
            ("bid", symbol2 if not inverted_cross else symbol2_alt, d2, "SELL"),  # Sell X for Y
            ("bid", symbol3, d3, "SELL"),  # Sell Y for base (USDT)
        ],
    }

    def evaluate_pattern(pat: Tuple[str, str, str]):
        """Evaluate pattern using step-by-step method: start with 1.0, apply each trade."""
        legs = patterns[pat]
        executed_sides = []
        steps = []
        
        # Start with 1.0 in base asset
        amount = 1.0
        
        # Process each trade step-by-step
        for (side_price, sym, depth, logical_side) in legs:
            # Get correct price: ask for BUY, bid for SELL
            price = depth["ask_price"] if side_price == "ask" else depth["bid_price"]
            
            # Validate price is reasonable
            if price <= 0 or not isinstance(price, (int, float)) or price > 1e8:
                return None  # Invalid price
            
            # Check bid/ask spread is reasonable (< 10% for most pairs)
            ask_price = depth["ask_price"]
            bid_price = depth["bid_price"]
            if ask_price > 0 and bid_price > 0:
                spread_pct = ((ask_price - bid_price) / bid_price) * 100
                if spread_pct > 20:  # Reject pairs with >20% spread (likely stale data)
                    return None
            
            # Apply trade: BUY divides amount by price, SELL multiplies amount by price
            amount_before = amount
            if logical_side == "BUY":
                amount = amount / price  # Spending current asset to buy next asset
            else:  # SELL
                amount = amount * price  # Selling current asset for next asset
            
            # Validate amount is reasonable
            if amount <= 0 or amount > 1e20:
                return None
            
            # Build step description
            steps.append(f"{logical_side} {sym} @{price:.10f} | {amount_before:.10f}->{amount:.10f}")
            executed_sides.append(logical_side.lower())
        
        # Calculate gross profit percentage: (final_amount - 1.0) * 100.0
        gross_profit_pct = (amount - 1.0) * 100.0
        
        # Validate profit is reasonable (between -50% and 50%)
        if gross_profit_pct < -50 or gross_profit_pct > 50:
            return None
        
        return {
            "gross_pct": gross_profit_pct,
            "steps": steps,
            "sides": executed_sides,
        }

    best = None
    best_pattern = None
    for pat in patterns.keys():
        res = evaluate_pattern(pat)
        if res is None:
            continue
        if best is None or res["gross_pct"] > best["gross_pct"]:
            best = res
            best_pattern = pat

    if best is None:
        return None

    gross_profit_pct = best["gross_pct"]
    # Calculate net profit by applying fees (3 trades, fee per trade)
    fee_rate = (S.FEE_RATE_BPS + S.EXTRA_FEE_BPS) / 10000.0
    total_fee = 3 * fee_rate  # 3 trades in triangular arbitrage
    gross_multiplier = 1.0 + (gross_profit_pct / 100.0)
    net_multiplier = gross_multiplier * (1.0 - total_fee)
    net_profit_pct = (net_multiplier - 1.0) * 100.0
    gross_net = (1 + gross_profit_pct / 100.0)
    steps_trace = best["steps"]
    executed_sides = best["sides"]
    
    # Track statistics (store gross samples)
    if stats is not None:
        if len(stats.get("sample_profits", [])) < 100:
            stats["sample_profits"].append(gross_profit_pct)
    
    # Check profit thresholds using GROSS profit
    min_prof = min_profit_pct if min_profit_pct is not None else getattr(S, "MIN_PROFIT_PCT", 1.0)
    max_prof = max_profit_pct if max_profit_pct is not None else S.MAX_PROFIT_PCT
    
    if gross_profit_pct < min_prof or gross_profit_pct > max_prof:
        if stats is not None:
            stats["filtered_profit"] = stats.get("filtered_profit", 0) + 1
            # Track routes close to target range for debugging
            if 0.5 <= gross_profit_pct <= 6.0:  # Close to 1-5% range
                close_routes = stats.get("close_routes", [])
                if len(close_routes) < 10:
                    close_routes.append({
                        "route": f"{base}->{x}->{y}->{base}",
                        "gross": gross_profit_pct,
                        "net": net_profit_pct,
                        "pattern": "-".join(best_pattern)
                    })
                    stats["close_routes"] = close_routes
        # Log routes close to target range more frequently
        if 0.5 <= gross_profit_pct <= 6.0 and random.random() < 0.1:
            logger.info(f"Route near target range: {base}->{x}->{y}->{base} gross={gross_profit_pct:.4f}% (target: {min_prof:.1f}-{max_prof:.1f}%)")
        elif random.random() < 0.001:  # Very rare logging for other routes
            logger.debug(f"Route filtered: {base}->{x}->{y}->{base} gross={gross_profit_pct:.4f}%")
        return None
    
    # In theoretical mode, skip capacity and notional checks
    theoretical = getattr(S, "THEORETICAL_MODE", True)
    if theoretical:
        max_base = S.MAX_NOTIONAL_USD
    else:
        # Baseline conversions for capacity approximation
        base_to_x = 1.0 / d1["ask_price"] if d1["ask_price"] else 0
        x_to_y = 1.0 / d2["ask_price"] if d2["ask_price"] else 0
        y_to_base = d3["bid_price"]
        
        # Capacity calculation
        cap1_base = d1["cap_ask_quote"]
        cap2_base = d2["cap_ask_quote"] / base_to_x if base_to_x > 0 else 0
        cap3_base = d3["total_bid_qty"] * y_to_base
        max_base = min(cap1_base, cap2_base, cap3_base)
        
        if available_balance is not None and available_balance > 0:
            max_base = min(max_base, available_balance)
        
        max_base = min(max_base, S.MAX_NOTIONAL_USD)
        
        if max_base < S.MIN_NOTIONAL_USD:
            if stats is not None:
                stats["filtered_volume"] = stats.get("filtered_volume", 0) + 1
            if random.random() < 0.01:
                logger.debug(f"Route filtered by volume: {base}->{x}->{y}->{base} volume=${max_base:.2f}")
            return None
    
    def _side_label(s: str) -> str:
        return "sell" if s.upper().startswith("SELL") else "buy"

    route = CandidateRoute(
        a=f"{x}/{base} {_side_label(executed_sides[0])}",
        b=f"{y}/{x} {_side_label(executed_sides[1])}",
        c=f"{y}/{base} {_side_label(executed_sides[2])}",
        profit_pct=round(gross_profit_pct, 4),
        volume_usd=round(max_base, 2),
    )

    # Detailed trace for correctness verification (toggle via S.LOG_ARBITRAGE_STEPS)
    if getattr(S, "LOG_ARBITRAGE_STEPS", False):
        logger.info(
            "arb-trace pattern=%s base=%s path=%s/%s/%s gross=%.6f%% net=%.6f%% vol=$%.2f steps=%s inverted_cross=%s",
            "-".join(best_pattern),
            base,
            x,
            y,
            base,
            gross_profit_pct,
            net_profit_pct,
            max_base,
            " || ".join(steps_trace),
            inverted_cross,
        )
    else:
        logger.debug(
            "triangle %s->%s->%s->%s pattern=%s gross=%.6f%% net=%.6f%% vol=$%.2f inverted_cross=%s",
            base,
            x,
            y,
            base,
            "-".join(best_pattern),
            gross_profit_pct,
            net_profit_pct,
            max_base,
            inverted_cross,
        )

    return route


def _score_triangle(x: str, y: str, base: str, asset_volumes: Dict[str, float], asset_quote_map: Dict[str, set]) -> float:
    """Score triangle by profitability likelihood (higher = better)"""
    score = 0.0
    
    # Major-to-major triangles (highest priority)
    majors = {"BTC", "ETH", "BNB"}
    if x in majors and y in majors:
        score += 1000.0
    
    # High-priority assets from screenshot examples
    priority_assets = {"ONT", "SSV", "KITE", "SOL", "ADA", "DOT", "LINK", "MATIC"}
    if x in priority_assets or y in priority_assets:
        score += 500.0
    
    # Major quote routing (medium-high priority)
    elif x in majors or y in majors:
        score += 300.0
    
    # Volume-based scoring (normalized)
    vol_x = asset_volumes.get(x, 0.0)
    vol_y = asset_volumes.get(y, 0.0)
    avg_vol = (vol_x + vol_y) / 2.0
    if avg_vol > 0:
        score += min(avg_vol / 1000000.0, 200.0)  # Cap at 200 points
    
    # Multi-quote bonus (more arbitrage paths)
    quotes_x = len(asset_quote_map.get(x, set()))
    quotes_y = len(asset_quote_map.get(y, set()))
    score += (quotes_x + quotes_y) * 10.0
    
    return score


def _get_triangle_symbols(x: str, y: str, base: str) -> List[str]:
    """Get required symbols for a triangle"""
    return [f"{x}{base}", f"{y}{x}", f"{x}{y}", f"{y}{base}"]


def _optimize_symbol_selection(scored_triangles: List[Tuple[float, str, str]], 
                              base: str, symbols: Dict[str, dict], 
                              max_symbols: int = 500) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Select triangles to maximize coverage within symbol limit - optimized for 2000+ symbols"""
    selected_triangles = []
    required_symbols = set()
    
    # Always include major quote pairs
    majors = ["BTC", "ETH", "BNB", "USDC", "FDUSD"]
    for major in majors:
        if major != base:
            major_pair = f"{major}{base}"
            if major_pair in symbols:
                required_symbols.add(major_pair)
    
    # Track symbol usage count for better optimization
    symbol_usage = {}
    
    # Add triangles in score order until symbol limit
    for score, x, y in scored_triangles:
        triangle_symbols = _get_triangle_symbols(x, y, base)
        # Filter to existing symbols
        valid_symbols = [s for s in triangle_symbols if s in symbols and symbols[s].get("status") == "TRADING"]
        
        # Check if we can fit this triangle
        new_symbols = set(valid_symbols) - required_symbols
        if len(required_symbols) + len(new_symbols) <= max_symbols:
            selected_triangles.append((x, y))
            required_symbols.update(new_symbols)
            
            # Track symbol usage
            for sym in valid_symbols:
                symbol_usage[sym] = symbol_usage.get(sym, 0) + 1
            
            # Early exit if we have enough triangles (increased limit for larger sets)
            if len(selected_triangles) >= 2000:
                break
    
    logger.debug(f"Symbol optimization: {len(required_symbols)} symbols for {len(selected_triangles)} triangles")
    
    return selected_triangles, list(required_symbols)


def find_candidate_routes(
    *, 
    min_profit_pct: float, 
    max_profit_pct: float,
    available_balance: float = None
) -> Tuple[List[CandidateRoute], dict]:
    """
    Find candidate triangular arbitrage routes with optimized triangle selection.
    """
    logger = logging.getLogger(__name__)
    
    stats = {
        "symbols_loaded": 0,
        "symbols_fetched": 0,
        "triangles_checked": 0,
        "routes_found": 0,
        "fetch_time": 0.0,
        "triangle_time": 0.0,
        "filtered_profit": 0,
        "filtered_volume": 0,
        "filtered_missing_pairs": 0,
        "sample_profits": [],
        "triangles_generated": 0,
        "triangles_selected": 0,
    }
    
    try:
        client = _client()
        symbols = _load_symbols(client)
        stats["symbols_loaded"] = len(symbols)
        
        base = S.BASE_ASSET.upper()
        allowed_quotes = getattr(S, "ALLOWED_QUOTES", ["USDT", "BTC", "ETH", "BNB", "FDUSD", "USDC"])
        if base not in allowed_quotes:
            allowed_quotes.append(base)
        logger.info(
            f"Loaded {len(symbols)} symbols, base={base}, profit range: {min_profit_pct}% - {max_profit_pct}%, "
            f"quotes={allowed_quotes}"
        )
        
        # Define major quotes for routing
        MAJOR_QUOTES = ["USDT", "BTC", "ETH", "BNB", "USDC", "FDUSD"]
        MAJOR_ASSETS = {"BTC", "ETH", "BNB", "USDT", "USDC", "FDUSD"}
        
        # Load ALL assets (2000+) that trade with major quotes
        universe, asset_volumes = _select_all_assets(client, symbols, MAJOR_QUOTES)
        logger.info(f"Loaded {len(universe)} assets from all trading pairs (2000+ symbols)")
        
        _performance.symbols_loaded = len(symbols)
        
        # Build asset quote map for all assets
        asset_quote_map = {}
        for symbol_name, symbol_info in symbols.items():
            if symbol_info.get("status") != "TRADING":
                continue
            quote_asset = symbol_info.get("quoteAsset")
            base_asset = symbol_info.get("baseAsset")
            
            if quote_asset in MAJOR_QUOTES and base_asset in universe:
                if base_asset not in asset_quote_map:
                    asset_quote_map[base_asset] = set()
                asset_quote_map[base_asset].add(quote_asset)
        
        logger.info(f"Using {len(universe)} assets with {sum(len(v) for v in asset_quote_map.values())} quote pairs")
        
        # Generate and score all triangles efficiently with strict deduplication
        all_triangles = []
        triangle_set = set()  # Track unique triangles using sorted tuple keys (x, y)
        duplicates_found = 0
        
        def add_triangle(x: str, y: str, score: float, require_int: bool, pattern: str = ""):
            """Helper to add triangle with strict deduplication"""
            nonlocal duplicates_found  # Allow modification of outer scope variable
            # Use sorted tuple for strict deduplication (order-independent)
            key = tuple(sorted([x.upper(), y.upper()]))
            if key in triangle_set:
                duplicates_found += 1
                return False
            triangle_set.add(key)
            all_triangles.append((score, x, y, require_int))
            return True
        
        # Pattern 1: Major-to-major (highest priority) - require intermediate
        major_assets_list = ["BTC", "ETH", "BNB"]
        for i, major1 in enumerate(major_assets_list):
            if major1 == base:
                continue
            for major2 in major_assets_list[i+1:]:
                if major2 == base or major2 == major1:
                    continue
                
                # Check if both have base pairs
                if (f"{major1}{base}" in symbols and f"{major2}{base}" in symbols):
                    # Check if cross pair exists (preferred but not strictly required for scoring)
                    cross1 = f"{major2}{major1}"
                    cross2 = f"{major1}{major2}"
                    has_cross = ((cross1 in symbols and symbols[cross1].get("status") == "TRADING") or
                                 (cross2 in symbols and symbols[cross2].get("status") == "TRADING"))
                    
                    score = _score_triangle(major1, major2, base, asset_volumes, asset_quote_map)
                    if has_cross:
                        score += 100.0  # Bonus for having cross pair
                    add_triangle(major1, major2, score, True, "major-to-major")
        
        # Pattern 2: Major routing (require intermediate for major assets)
        for asset in universe:
            if asset == base:
                continue
            asset_quotes = asset_quote_map.get(asset, set())
            
            if base not in asset_quotes:
                continue
            
            # Major routing triangles
            for major in major_assets_list:
                if major == base or major == asset:
                    continue
                if major in asset_quotes:
                    # Major assets require intermediate pair
                    require_int = major in MAJOR_ASSETS or asset in MAJOR_ASSETS
                    score = _score_triangle(major, asset, base, asset_volumes, asset_quote_map)
                    add_triangle(major, asset, score, require_int, "major-routing")
        
        # Pattern 3: Direct triangles (intermediate optional for non-major)
        # Only check pairs that both trade with base
        base_trading_assets = [a for a in universe if base in asset_quote_map.get(a, set())]
        
        # Optimize: check in batches, prioritize high-volume pairs
        base_trading_assets.sort(key=lambda a: asset_volumes.get(a, 0.0), reverse=True)
        
        # Limit direct triangle checks to top pairs to avoid explosion
        max_direct_checks = getattr(S, "MAX_DIRECT_TRIANGLES", 10000)  # Increased limit
        checked_pairs = 0
        
        for i, asset in enumerate(base_trading_assets):
            if checked_pairs >= max_direct_checks:
                break
            for other_asset in base_trading_assets[i+1:]:
                if checked_pairs >= max_direct_checks:
                    break
                checked_pairs += 1
                
                # For non-major assets, intermediate is optional
                require_int = asset in MAJOR_ASSETS or other_asset in MAJOR_ASSETS
                score = _score_triangle(asset, other_asset, base, asset_volumes, asset_quote_map)
                # Lower score for non-major pairs without intermediate
                if not require_int:
                    score *= 0.5
                add_triangle(asset, other_asset, score, require_int, "direct")
        
        if duplicates_found > 0:
            logger.info(f"Filtered {duplicates_found} duplicate triangles during generation")
        
        # Sort by score (highest first)
        all_triangles.sort(reverse=True)
        
        stats["triangles_generated"] = len(all_triangles)
        
        # Optimize symbol selection for rate limits
        # Increase limit for larger symbol sets
        max_symbols_to_fetch = getattr(S, "MAX_DEPTH_FETCH", 500)  # Increased from 200
        
        # Extract triangles without require_intermediate flag for optimization function
        triangles_for_opt = [(score, x, y) for score, x, y, _ in all_triangles]
        selected_triangles_opt, required_symbols = _optimize_symbol_selection(
            triangles_for_opt, base, symbols, max_symbols_to_fetch
        )
        
        # Rebuild selected triangles with require_intermediate flag
        selected_triangles = []
        selected_set = {(x, y) for x, y in selected_triangles_opt}
        for score, x, y, require_int in all_triangles:
            if (x, y) in selected_set:
                selected_triangles.append((x, y, require_int))
                if len(selected_triangles) >= len(selected_triangles_opt):
                    break
        
        stats["triangles_selected"] = len(selected_triangles)
        
        # Analyze selected triangle types
        direct_count = major_quote_count = major_to_major_count = 0
        for triangle_data in selected_triangles:
            if len(triangle_data) == 3:
                x, y, _ = triangle_data
            else:
                x, y = triangle_data
            if x in MAJOR_ASSETS and y in MAJOR_ASSETS:
                major_to_major_count += 1
            elif x in MAJOR_ASSETS or y in MAJOR_ASSETS:
                major_quote_count += 1
            else:
                direct_count += 1
        
        logger.info(f"Optimized selection: {len(selected_triangles)} triangles from {len(all_triangles)} candidates")
        logger.info(f"Selected triangles: {direct_count} direct, {major_quote_count} via majors, {major_to_major_count} major-to-major")
        logger.info(f"Symbol optimization: {len(required_symbols)} symbols needed (limit: {max_symbols_to_fetch})")

        # Start/update WebSocket connection (persistent, only updates subscriptions if needed)
        use_ws = getattr(S, "USE_BOOK_TICKER_WS", True) and SpotWebsocketClient is not None
        if use_ws and required_symbols:
            try:
                stream_url = getattr(S, "BINANCE_WS_URL", None)
                kwargs = {"stream_url": stream_url} if stream_url else {}
                
                # Start or update WebSocket (keeps connection alive, only updates if symbols changed)
                _book_ticker_stream.start(required_symbols, **kwargs)
                
                # Wait for initial data to populate (shorter wait since connection may already exist)
                wait_time = 1.0 if _book_ticker_stream.running else 2.0
                time.sleep(wait_time)
                
                # Check WebSocket coverage
                ws_valid, ws_total = _book_ticker_stream.get_coverage(required_symbols)
                logger.info(f"WebSocket: {ws_valid}/{ws_total} symbols have fresh data ({ws_valid/ws_total*100:.1f}% coverage)")
                
            except Exception as e:
                logger.warning(f"WebSocket startup failed, using REST only: {e}")
                use_ws = False
        elif getattr(S, "USE_BOOK_TICKER_WS", True) and SpotWebsocketClient is None:
            logger.warning("WebSocket client not available, install binance-connector-python[websocket]")
        
        # Fetch market data with WebSocket priority (minimal REST calls)
        start_fetch = time.time()
        depth_cache = _fetch_depth_parallel(client, required_symbols, max_workers=8)
        fetch_time = time.time() - start_fetch
        
        fetched_count = sum(1 for v in depth_cache.values() if v is not None)
        ws_valid, ws_total = _book_ticker_stream.get_coverage(required_symbols)
        ws_hits = ws_valid
        
        stats["symbols_fetched"] = fetched_count
        stats["fetch_time"] = fetch_time
        stats["ws_hits"] = ws_hits
        
        coverage_pct = (fetched_count / len(required_symbols)) * 100 if required_symbols else 0
        ws_pct = (ws_hits / len(required_symbols)) * 100 if required_symbols else 0
        logger.info(f"Market data: {fetched_count}/{len(required_symbols)} symbols ({coverage_pct:.1f}% coverage, {ws_pct:.1f}% WebSocket) in {fetch_time:.2f}s")
        
        # Concurrent triangle scanning for faster detection
        cand: List[CandidateRoute] = []
        start_triangles = time.time()
        route_keys = set()
        
        def check_triangle(triangle_data):
            if len(triangle_data) == 3:
                x, y, require_int = triangle_data
            else:
                x, y = triangle_data
                require_int = True  # Default to requiring intermediate
            return _try_triangle(
                client, symbols, base, x, y,
                min_profit_pct, max_profit_pct,
                depth_cache=depth_cache,
                available_balance=available_balance,
                stats=stats,
                require_intermediate=require_int
            )
        
        # Process triangles in parallel batches with optimized filtering
        batch_size = 64  # Increased batch size for efficiency
        checked = 0
        max_routes = getattr(S, "MAX_ROUTES_TO_RETURN", 10)
        
        with ThreadPoolExecutor(max_workers=8) as executor:  # Increased workers
            for i in range(0, len(selected_triangles), batch_size):
                batch = selected_triangles[i:i + batch_size]
                futures = {executor.submit(check_triangle, triangle): triangle for triangle in batch}
                
                for future in as_completed(futures):
                    checked += 1
                    try:
                        r = future.result()
                        if r:
                            route_key = (r.a, r.b, r.c)
                            if route_key not in route_keys:
                                route_keys.add(route_key)
                                logger.info(f"PROFITABLE: {r.a} → {r.b} → {r.c}, profit: {r.profit_pct}%, volume: ${r.volume_usd}")
                                cand.append(r)
                                stats["routes_found"] += 1
                                if len(cand) >= max_routes:
                                    break
                    except Exception as e:
                        logger.debug(f"Triangle check failed: {e}")
                
                if len(cand) >= max_routes:
                    break
        
        triangle_time = time.time() - start_triangles
        stats["triangles_checked"] = checked
        stats["triangle_time"] = triangle_time
        
        logger.info(f"Checked {checked} triangles in {triangle_time:.2f}s")
        logger.info(f"Found {len(cand)} routes")
        logger.info(f"Filter stats: missing_pairs={stats['filtered_missing_pairs']}, profit={stats['filtered_profit']}, volume={stats['filtered_volume']}")
        
        # Performance metrics and efficiency tracking
        total_scan_time = time.time() - start_fetch
        _performance.update(cand, total_scan_time, ws_hits, len(required_symbols), checked)
        
        if checked > 0:
            missing_pct = (stats['filtered_missing_pairs'] / checked) * 100.0
            profit_pct = (stats['filtered_profit'] / checked) * 100.0
            logger.info(f"Efficiency: {missing_pct:.1f}% missing pairs, {profit_pct:.1f}% filtered by profit")
            
            # Performance metrics
            selection_ratio = (stats['triangles_selected'] / stats['triangles_generated']) * 100 if stats['triangles_generated'] > 0 else 0
            symbol_efficiency = checked / len(required_symbols) if required_symbols else 0
            
            logger.info(f"Performance: {_performance.scan_time_ms:.0f}ms scan, {_performance.success_rate:.1f}% success rate, {_performance.ws_coverage:.1f}% WebSocket")
            logger.info(f"Optimization: {selection_ratio:.1f}% triangles selected, {symbol_efficiency:.2f} triangles per symbol")
            
            if _performance.routes_found > 0:
                logger.info(f"P&L Metrics: {_performance.avg_profit:.4f}% avg profit, {_performance.max_profit:.4f}% max profit")
        
        if stats.get("sample_profits"):
            profits = stats["sample_profits"]
            if profits:
                logger.info(f"Profit sample (n={len(profits)}): min={min(profits):.6f}%, max={max(profits):.6f}%, avg={sum(profits)/len(profits):.6f}%")
                # Show profit distribution
                in_range = [p for p in profits if min_profit_pct <= p <= max_profit_pct]
                if in_range:
                    logger.info(f"  Routes in target range ({min_profit_pct:.1f}-{max_profit_pct:.1f}%): {len(in_range)}/{len(profits)}")
                else:
                    logger.warning(f"  No routes found in target range ({min_profit_pct:.1f}-{max_profit_pct:.1f}%)")
        
        # Log routes that were close to target range
        if stats.get("close_routes"):
            close_routes = stats["close_routes"]
            logger.info(f"Found {len(close_routes)} routes close to target range (0.5-6.0%):")
            for cr in close_routes[:5]:  # Show first 5
                logger.info(f"  {cr['route']}: {cr['gross']:.4f}% gross ({cr['net']:.4f}% net) pattern={cr['pattern']}")
        
        # Sort by profit desc and filter routes
        cand.sort(key=lambda z: z.profit_pct, reverse=True)
        
        # Filter routes: remove duplicates and low-quality routes
        filtered_cand = []
        seen_profit = set()
        for route in cand:
            # Round profit to avoid near-duplicates
            profit_key = round(route.profit_pct, 2)
            if profit_key not in seen_profit:
                seen_profit.add(profit_key)
                filtered_cand.append(route)
        
        max_return = getattr(S, "MAX_ROUTES_TO_RETURN", 10)
        
        # Enhanced stats with performance data
        stats.update({
            "performance": {
                "scan_count": _performance.scan_count,
                "success_rate": _performance.success_rate,
                "avg_profit": _performance.avg_profit,
                "max_profit": _performance.max_profit,
                "ws_coverage": _performance.ws_coverage,
                "scan_time_ms": _performance.scan_time_ms,
            },
            "routes_filtered": len(cand) - len(filtered_cand)
        })
        
        return filtered_cand[:max_return], stats
        
    except Exception as e:
        logger.error(f"Error in find_candidate_routes: {e}", exc_info=True)
        return [], stats


def _parse_leg(label: str) -> Tuple[str, str, str]:
    """Parse leg label like "BTC/USDT buy" into (base, quote, side)"""
    pair, side = label.split()
    base, quote = pair.split("/")
    return base, quote, side.lower()


def revalidate_route(route: CandidateRoute) -> Optional[CandidateRoute]:
    """Revalidate a route with fresh market data"""
    try:
        a_base, a_quote, _ = _parse_leg(route.a)
        c_base, c_quote, _ = _parse_leg(route.c)
        base = S.BASE_ASSET.upper()
        
        # Extract x and y from route labels
        # Route format: x/base buy -> y/x buy -> y/base sell
        x = a_base  # From "x/base buy"
        y = c_base  # From "y/base sell"
        
        client = _client()
        symbols = _load_symbols(client)
        
        # Re-run triangle check
        new_route = _try_triangle(client, symbols, base, x, y)
        return new_route
    except Exception as e:
        logger.error(f"Error revalidating route: {e}")
        return None


def get_performance_metrics() -> dict:
    """Get current performance metrics for monitoring"""
    return {
        "scan_count": _performance.scan_count,
        "routes_found": _performance.routes_found,
        "success_rate": round(_performance.success_rate, 2),
        "avg_profit": round(_performance.avg_profit, 4),
        "max_profit": round(_performance.max_profit, 4),
        "ws_coverage": round(_performance.ws_coverage, 1),
        "scan_time_ms": round(_performance.scan_time_ms, 0),
        "symbols_loaded": _performance.symbols_loaded,
        "triangles_checked": _performance.triangles_checked,
    }


def reset_performance_metrics():
    """Reset performance tracking"""
    global _performance
    _performance = PerformanceMetrics()


# Optional: Debug function to test specific triangles
def debug_specific_triangles():
    """Debug function to test specific triangle combinations"""
    client = _client()
    symbols = _load_symbols(client)
    base = S.BASE_ASSET.upper()
    
    test_triangles = [
        ("BTC", "ETH"),
        ("BNB", "SOL"),
        ("ADA", "XRP"),
        ("DOGE", "MATIC"),
    ]
    
    print(f"\n=== Testing specific triangles (base={base}) ===")
    
    for x, y in test_triangles:
        print(f"\nTriangle: {base}->{x}->{y}->{base}")
        
        # Check required symbols
        symbol1 = f"{x}{base}"
        symbol2 = f"{y}{x}"
        symbol3 = f"{y}{base}"
        
        exists1 = symbol1 in symbols
        exists2 = symbol2 in symbols
        exists3 = symbol3 in symbols
        
        print(f"  {symbol1}: {'✓' if exists1 else '✗'}")
        print(f"  {symbol2}: {'✓' if exists2 else '✗'}")
        print(f"  {symbol3}: {'✓' if exists3 else '✗'}")
        
        if exists1 and exists2 and exists3:
            # Calculate profit
            route = _try_triangle(client, symbols, base, x, y, min_profit_pct=-10, max_profit_pct=10)
            if route:
                print(f"  Result: {route.profit_pct:.6f}% profit, ${route.volume_usd:.2f}")
            else:
                print(f"  Result: No route found (check depth data)")
        else:
            print(f"  Result: Missing pairs")


def _apply_leg(amount: float, base: str, quote: str, price: float, side: str) -> Tuple[float, str]:
    """
    Apply one trade leg using BUY/SELL semantics.
    BUY: spend quote to receive base (amount / price)
    SELL: spend base to receive quote (amount * price)
    """
    side_upper = side.upper()
    next_asset = base if side_upper == "BUY" else quote
    new_amount = amount / price if side_upper == "BUY" else amount * price
    return new_amount, next_asset


def _parse_pair(raw: str) -> Tuple[str, str, float, str]:
    """Parse input like 'BNB/USDT: 921.69 BUY'"""
    parts = raw.replace(",", " ").split()
    if len(parts) < 3:
        raise ValueError(f"Cannot parse pair input: '{raw}'")
    pair = parts[0].rstrip(":")
    price = float(parts[1])
    side = parts[2].upper()
    if "/" not in pair:
        raise ValueError(f"Pair must contain '/': '{pair}'")
    base, quote = pair.split("/")
    return base.upper(), quote.upper(), price, side


def _invert_pair(base: str, quote: str, price: float, side: str) -> Tuple[str, str, float, str]:
    """Invert a pair: flip assets, invert price, flip side."""
    inv_price = 1.0 / price
    inv_side = "BUY" if side.upper() == "SELL" else "SELL"
    return quote, base, inv_price, inv_side


def _describe_step(src_asset: str, dst_asset: str, price: float, side: str, amount_before: float, amount_after: float) -> str:
    """Human-friendly step description."""
    return f"{src_asset} -> {dst_asset}: {side.upper()} @ {price:.10f} | {amount_before:.10f} -> {amount_after:.10f}"


def calculate_triangular_arbitrage(pair1: str, pair2: str, pair3: str) -> TriangularArbResult:
    """
    Calculate forward and backward triangular arbitrage profit for three pairs.
    Input format per leg: 'ASSETX/ASSETY: price SIDE'

    Forward path: pair1 -> pair2 -> pair3
    Backward path: pair3 -> inverse(pair2) -> inverse(pair1)
    """
    warnings: List[str] = []

    b1, q1, p1, s1 = _parse_pair(pair1)
    b2, q2, p2, s2 = _parse_pair(pair2)
    b3, q3, p3, s3 = _parse_pair(pair3)

    asset1 = q1  # start asset (quote of first pair, typically USDT)
    asset2 = b1
    asset3 = b2

    # Chain validation for forward path
    if q2 != asset2:
        warnings.append(f"Chain mismatch: pair2 quote {q2} should be {asset2}")
    if b3 != asset3 or q3 != asset1:
        warnings.append(f"Chain mismatch: pair3 expected {asset3}/{asset1}, got {b3}/{q3}")

    pattern = f"{s1.upper()}-{s2.upper()}-{s3.upper()}"

    def run_path(legs: List[Tuple[str, str, float, str]]) -> Tuple[float, List[str]]:
        amt = 1.0
        current_asset = asset1
        steps: List[str] = []
        for base, quote, price, side in legs:
            amt_before = amt
            side_upper = side.upper()
            if side_upper == "BUY" and current_asset != quote:
                warnings.append(f"Expected to spend {quote} but have {current_asset}; using leg as-is.")
            if side_upper == "SELL" and current_asset != base:
                warnings.append(f"Expected to spend {base} but have {current_asset}; using leg as-is.")
            amt, new_asset = _apply_leg(amt, base, quote, price, side)
            steps.append(_describe_step(current_asset, new_asset, price, side, amt_before, amt))
            current_asset = new_asset
        return amt, steps

    # Forward: given order
    forward_amount, forward_steps = run_path([
        (b1, q1, p1, s1),
        (b2, q2, p2, s2),
        (b3, q3, p3, s3),
    ])
    forward_profit = (forward_amount - 1.0) * 100.0

    # Backward: pair3 -> inverse(pair2) -> inverse(pair1)
    inv_b2, inv_q2, inv_p2, inv_s2 = _invert_pair(b2, q2, p2, s2)
    inv_b1, inv_q1, inv_p1, inv_s1 = _invert_pair(b1, q1, p1, s1)
    backward_amount, backward_steps = run_path([
        (b3, q3, p3, s3),
        (inv_b2, inv_q2, inv_p2, inv_s2),
        (inv_b1, inv_q1, inv_p1, inv_s1),
    ])
    backward_profit = (backward_amount - 1.0) * 100.0

    best_path = "Forward" if forward_profit >= backward_profit else "Backward"
    best_profit = forward_profit if best_path == "Forward" else backward_profit

    if getattr(S, "LOG_ARBITRAGE_STEPS", False):
        logger.info(
            "tri-calc pattern=%s forward=%.6f%% backward=%.6f%% best=%s(%.6f%%) warnings=%s",
            pattern,
            forward_profit,
            backward_profit,
            best_path.lower(),
            best_profit,
            "; ".join(warnings) if warnings else "none",
        )
        logger.info("tri-calc forward steps: %s", " || ".join(forward_steps))
        logger.info("tri-calc backward steps: %s", " || ".join(backward_steps))

    return TriangularArbResult(
        forward_profit_pct=round(forward_profit, 4),
        backward_profit_pct=round(backward_profit, 4),
        best_path=best_path,
        best_profit_pct=round(best_profit, 4),
        pattern=pattern,
        forward_steps=forward_steps,
        backward_steps=backward_steps,
        warnings=warnings,
    )