"""
Triangular Arbitrage Detection Module
Features:
- WebSocket real-time book ticker for fast price updates
- Fee-adjusted profit calculation
- Real volume calculation from order book depth
- Thread-safe parallel execution
"""
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from binance.spot import Spot as BinanceClient
try:
    from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
except ImportError:
    SpotWebsocketStreamClient = None
from arbbot import settings as S
from .symbol_loader import get_symbol_loader
import time
import logging
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# Only these coins are allowed as bridge (middle-leg) assets
MAJOR_COINS = {"BTC", "ETH", "BNB"}


class DepthCache:
    """Thread-safe cache for order book depth snapshots with TTL"""
    def __init__(self, ttl_seconds: float = 10.0):
        self.ttl = ttl_seconds
        self._cache: Dict[str, Tuple[dict, float]] = {}
        self._lock = threading.Lock()
    
    def get(self, symbol: str) -> Optional[dict]:
        """Get cached depth if still valid"""
        with self._lock:
            if symbol in self._cache:
                data, timestamp = self._cache[symbol]
                if time.time() - timestamp < self.ttl:
                    return data
                else:
                    del self._cache[symbol]
        return None
    
    def set(self, symbol: str, data: dict):
        """Cache depth data"""
        with self._lock:
            self._cache[symbol] = (data, time.time())
    
    def clear(self):
        """Clear all cached data"""
        with self._lock:
            self._cache.clear()


# Global depth cache instance
_depth_cache = DepthCache(ttl_seconds=10.0)


class BookTickerStream:
    """
    WebSocket-based best bid/ask cache for real-time price updates.
    Uses Binance's bookTicker stream for fastest price data.
    """
    
    def __init__(self, ttl_seconds: float = 5.0):
        self.ttl = ttl_seconds
        self._cache: Dict[str, Tuple[dict, float]] = {}
        self._lock = threading.Lock()
        self._client = None
        self._running = False
        self._subscribed_symbols: set = set()
        self._last_connect_time = 0.0
    
    def _on_message(self, _, message):
        """Handle WebSocket messages"""
        try:
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            if not isinstance(data, dict):
                return
            
            # Handle combined stream format: {"stream": "btcusdt@bookTicker", "data": {...}}
            if "stream" in data and "data" in data:
                ticker_data = data["data"]
            elif "s" in data:
                # Direct stream format
                ticker_data = data
            else:
                return
            
            symbol = ticker_data.get("s", "").upper()
            if not symbol:
                return
            
            # Parse bookTicker data
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
                "source": "websocket"
            }
            
            with self._lock:
                self._cache[symbol] = (snapshot, time.time())
                
        except Exception as e:
            logger.debug(f"WebSocket message error: {e}")
    
    def start(self, symbols: List[str]):
        """Start WebSocket connection for given symbols"""
        if SpotWebsocketStreamClient is None:
            logger.warning("WebSocket client not available - using REST API only")
            return False
        
        symbols_set = {s.upper() for s in symbols}
        
        # Check if we need to reconnect
        if self._running and symbols_set == self._subscribed_symbols:
            return True
        
        try:
            # Stop existing connection
            self.stop()
            
            # Create new connection
            self._client = SpotWebsocketStreamClient(
                on_message=self._on_message,
                is_combined=True
            )
            
            # Subscribe to bookTicker streams (up to 200 symbols)
            limited_symbols = list(symbols_set)[:200]
            for symbol in limited_symbols:
                self._client.book_ticker(symbol=symbol.lower())
            
            self._subscribed_symbols = set(s.upper() for s in limited_symbols)
            self._running = True
            self._last_connect_time = time.time()
            
            logger.info(f"WebSocket started: {len(limited_symbols)} symbols")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}")
            self._running = False
            return False
    
    def stop(self):
        """Stop WebSocket connection"""
        if self._client:
            try:
                self._client.stop()
            except Exception:
                pass
        self._client = None
        self._running = False
    
    def get(self, symbol: str) -> Optional[dict]:
        """Get cached ticker data if fresh"""
        with self._lock:
            symbol_upper = symbol.upper()
            if symbol_upper in self._cache:
                data, ts = self._cache[symbol_upper]
                if time.time() - ts < self.ttl:
                    return data
                else:
                    del self._cache[symbol_upper]
        return None
    
    def is_running(self) -> bool:
        """Check if WebSocket is active"""
        return self._running
    
    def get_stats(self) -> dict:
        """Get WebSocket stats"""
        with self._lock:
            valid_count = sum(
                1 for _, (_, ts) in self._cache.items()
                if time.time() - ts < self.ttl
            )
            return {
                "running": self._running,
                "subscribed": len(self._subscribed_symbols),
                "cached": valid_count,
            }


# Global WebSocket stream instance
_book_ticker_stream = BookTickerStream(ttl_seconds=5.0)

@dataclass
class CandidateRoute:
    a: str  # leg1 label e.g., "BTC/USDT buy"
    b: str  # leg2 label e.g., "ETH/BTC buy"
    c: str  # leg3 label e.g., "ETH/USDT sell"
    profit_pct: float  # Net profit after fees
    volume_usd: float  # Calculated from order book depth
    gross_profit_pct: float = 0.0  # Gross profit before fees


def _client(timeout: int = None) -> BinanceClient:
    """Create Binance client with configured settings"""
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
    """Load all active spot markets using FastSymbolLoader"""
    loader = get_symbol_loader(cache_ttl_seconds=300)
    return loader.load_symbols(
        client, 
        base_asset=S.BASE_ASSET, 
        use_cache=True, 
        include_all_pairs=False
    )


def _depth_snapshot(client: BinanceClient, symbol: str, depth: int = 5, use_cache: bool = True) -> Optional[dict]:
    """Get depth snapshot - tries WebSocket cache first, then REST cache, then API"""
    # 1. Try WebSocket cache first (fastest, real-time data)
    ws_data = _book_ticker_stream.get(symbol)
    if ws_data:
        return ws_data
    
    # 2. Try REST cache
    if use_cache:
        cached = _depth_cache.get(symbol)
        if cached is not None:
            return cached
    
    # Simple retry with backoff
    for attempt in range(2):
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
            if "too much request" in error_str or "429" in error_str:
                logger.warning(f"Rate limit hit for {symbol}, waiting...")
                time.sleep(2.0)
            else:
                time.sleep(0.5)
    return None


def _calculate_fee_pct() -> float:
    """Calculate total fee percentage for 3 legs"""
    fee_per_leg = (S.FEE_RATE_BPS + S.EXTRA_FEE_BPS) / 100.0  # BPS to percentage
    return fee_per_leg * 3  # 3 legs


def _calculate_volume_usd(depths: List[Optional[dict]], base_asset: str = "USDT") -> float:
    """
    Calculate executable volume from order book depth.
    Returns minimum capacity across all legs in USD.
    """
    if not all(depths):
        return 0.0
    
    capacities = []
    for d in depths:
        if d:
            # Use the smaller of ask and bid capacity
            ask_cap = d.get("cap_ask_quote", 0)
            bid_cap = d.get("cap_bid_quote", 0)
            capacities.append(min(ask_cap, bid_cap) if ask_cap and bid_cap else max(ask_cap, bid_cap))
    
    if not capacities:
        return 0.0
    
    # Volume is limited by the smallest leg
    min_volume = min(capacities)
    
    # Cap at MAX_NOTIONAL_USD
    return min(min_volume, S.MAX_NOTIONAL_USD)


def _try_triangle_pattern(
    client: BinanceClient, 
    symbols: Dict[str, dict], 
    base: str, 
    major: str, 
    alt: str,
    min_profit_pct: float,
    max_profit_pct: float,
    depth_cache: Dict[str, Optional[dict]] = None,
    stats: dict = None,
    stats_lock: threading.Lock = None
) -> Optional[CandidateRoute]:
    """
    Check triangle patterns with FEE-ADJUSTED profit calculation:
    Pattern 1: MAJOR/USDT -> ALT/MAJOR -> ALT/USDT  (Buy-Buy-Sell)
    Pattern 2: ALT/USDT -> ALT/MAJOR -> MAJOR/USDT  (Buy-Sell-Sell)
    
    Returns route if NET profit (after fees) is within target range.
    """
    def get_depth(symbol: str) -> Optional[dict]:
        """Get depth with local cache support"""
        if depth_cache is not None:
            cached = depth_cache.get(symbol)
            if cached is not None:
                return cached
            depth = _depth_snapshot(client, symbol, use_cache=True)
            if depth:
                depth_cache[symbol] = depth
            return depth
        return _depth_snapshot(client, symbol, use_cache=True)
    
    def update_stats(key: str, increment: int = 1):
        """Thread-safe stats update"""
        if stats is None:
            return
        if stats_lock:
            with stats_lock:
                stats[key] = stats.get(key, 0) + increment
        else:
            stats[key] = stats.get(key, 0) + increment
    
    # Validate inputs
    if major not in MAJOR_COINS:
        return None
    
    # Calculate total fee for 3 legs
    total_fee_pct = _calculate_fee_pct()
    
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
    
    # Check if symbols exist
    pattern1_valid = all(sym in symbols for sym in pattern1_symbols)
    pattern2_valid = all(sym in symbols for sym in pattern2_symbols)
    
    # Check for inverted cross pair
    if not pattern1_valid and not pattern2_valid:
        alt_major_inverted = f"{major}{alt}"
        if alt_major_inverted in symbols:
            pattern1_symbols[1] = alt_major_inverted
            pattern2_symbols[1] = alt_major_inverted
            pattern1_valid = all(sym in symbols for sym in pattern1_symbols)
            pattern2_valid = all(sym in symbols for sym in pattern2_symbols)
    
    if not pattern1_valid and not pattern2_valid:
        return None
    
    best_route = None
    best_profit = -999
    
    # Try Pattern 1: MAJOR/BASE -> ALT/MAJOR -> ALT/BASE
    if pattern1_valid:
        d1 = get_depth(pattern1_symbols[0])
        d2 = get_depth(pattern1_symbols[1])
        d3 = get_depth(pattern1_symbols[2])
        
        if d1 and d2 and d3:
            is_inverted = pattern1_symbols[1].startswith(major)
            
            if is_inverted:
                amount = 1.0
                amount = amount / d1["ask_price"]
                amount = amount * d2["bid_price"]
                amount = amount * d3["bid_price"]
            else:
                amount = 1.0
                amount = amount / d1["ask_price"]
                amount = amount / d2["ask_price"]
                amount = amount * d3["bid_price"]
            
            gross_profit = (amount - 1.0) * 100.0
            net_profit = gross_profit - total_fee_pct  # APPLY FEES
            
            # Calculate real volume from order book
            volume_usd = _calculate_volume_usd([d1, d2, d3], base)
            
            if -50 < net_profit < 50 and net_profit > best_profit and volume_usd >= S.MIN_NOTIONAL_USD:
                best_profit = net_profit
                if is_inverted:
                    best_route = CandidateRoute(
                        a=f"{major}/{base} buy",
                        b=f"{major}/{alt} sell",
                        c=f"{alt}/{base} sell",
                        profit_pct=round(net_profit, 4),
                        gross_profit_pct=round(gross_profit, 4),
                        volume_usd=round(volume_usd, 2),
                    )
                else:
                    best_route = CandidateRoute(
                        a=f"{major}/{base} buy",
                        b=f"{alt}/{major} buy",
                        c=f"{alt}/{base} sell",
                        profit_pct=round(net_profit, 4),
                        gross_profit_pct=round(gross_profit, 4),
                        volume_usd=round(volume_usd, 2),
                    )
    
    # Try Pattern 2: ALT/BASE -> ALT/MAJOR -> MAJOR/BASE
    if pattern2_valid:
        d1 = get_depth(pattern2_symbols[0])
        d2 = get_depth(pattern2_symbols[1])
        d3 = get_depth(pattern2_symbols[2])
        
        if d1 and d2 and d3:
            is_inverted = pattern2_symbols[1].startswith(major)
            
            if is_inverted:
                amount = 1.0
                amount = amount / d1["ask_price"]
                amount = amount / d2["ask_price"]
                amount = amount * d3["bid_price"]
            else:
                amount = 1.0
                amount = amount / d1["ask_price"]
                amount = amount * d2["bid_price"]
                amount = amount * d3["bid_price"]
            
            gross_profit = (amount - 1.0) * 100.0
            net_profit = gross_profit - total_fee_pct  # APPLY FEES
            
            # Calculate real volume from order book
            volume_usd = _calculate_volume_usd([d1, d2, d3], base)
            
            if -50 < net_profit < 50 and net_profit > best_profit and volume_usd >= S.MIN_NOTIONAL_USD:
                best_profit = net_profit
                if is_inverted:
                    best_route = CandidateRoute(
                        a=f"{alt}/{base} buy",
                        b=f"{major}/{alt} buy",
                        c=f"{major}/{base} sell",
                        profit_pct=round(net_profit, 4),
                        gross_profit_pct=round(gross_profit, 4),
                        volume_usd=round(volume_usd, 2),
                    )
                else:
                    best_route = CandidateRoute(
                        a=f"{alt}/{base} buy",
                        b=f"{alt}/{major} sell",
                        c=f"{major}/{base} sell",
                        profit_pct=round(net_profit, 4),
                        gross_profit_pct=round(gross_profit, 4),
                        volume_usd=round(volume_usd, 2),
                    )
    
    # Check if best profit is in target range
    if best_route and min_profit_pct <= best_route.profit_pct <= max_profit_pct:
        update_stats("routes_found")
        logger.info(
            f"FOUND: {best_route.a} → {best_route.b} → {best_route.c}, "
            f"net: {best_route.profit_pct}%, gross: {best_route.gross_profit_pct}%, "
            f"volume: ${best_route.volume_usd}"
        )
        return best_route
    elif best_route:
        update_stats("filtered_profit")
    
    return None


def find_candidate_routes(
    *, 
    min_profit_pct: float, 
    max_profit_pct: float,
    available_balance: float = None,
    max_routes: int = 10,
    use_parallel: bool = True
) -> Tuple[List[CandidateRoute], dict]:
    """
    Find triangular arbitrage opportunities with fee-adjusted profits.
    
    Returns:
        Tuple of (list of CandidateRoute, stats dict)
    """
    start_time = time.time()
    stats_lock = threading.Lock()
    
    stats = {
        "attempts": 0,
        "routes_found": 0,
        "filtered_profit": 0,
        "unique_assets_tried": set(),
        "start_time": start_time,
        "fee_pct_applied": _calculate_fee_pct(),
    }
    
    try:
        client = _client()
        symbols = _load_symbols(client)
        base = S.BASE_ASSET.upper()
        
        logger.info(f"=== ROUTE SCAN STARTED ===")
        logger.info(f"Target net profit: {min_profit_pct}% - {max_profit_pct}% (after {stats['fee_pct_applied']:.2f}% fees)")
        logger.info(f"Min volume: ${S.MIN_NOTIONAL_USD}, Max: ${S.MAX_NOTIONAL_USD}")
        
        # Get assets that trade with BASE
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
        
        logger.info(f"Found {len(usdt_assets)} assets with {base}, {len(major_assets)} major coins")
        
        if not major_assets or len(usdt_assets) < 10:
            logger.warning("Insufficient assets for triangular arbitrage")
            return [], stats
        
        # Start WebSocket for real-time price updates
        ws_symbols = [f"{asset}{base}" for asset in usdt_assets[:200]]
        # Also include cross pairs for triangular routes
        for major in major_assets:
            for alt in usdt_assets[:100]:
                if alt != major and alt not in MAJOR_COINS:
                    ws_symbols.append(f"{alt}{major}")
                    ws_symbols.append(f"{major}{alt}")
        
        if _book_ticker_stream.start(ws_symbols):
            # Give WebSocket time to receive initial data
            time.sleep(0.5)
            ws_stats = _book_ticker_stream.get_stats()
            logger.info(f"WebSocket: {ws_stats['cached']} prices cached from {ws_stats['subscribed']} subscriptions")
            stats["websocket_enabled"] = True
        else:
            logger.info("Using REST API for price data (WebSocket unavailable)")
            stats["websocket_enabled"] = False
        
        # Build (major, alt) pairs
        all_pairs: List[Tuple[str, str]] = []
        seen_pairs: set = set()
        
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
        
        stats["attempts"] = len(all_pairs)
        logger.info(f"Checking {len(all_pairs)} (major, alt) combinations")
        
        # Thread-safe depth cache for parallel execution
        depth_cache = {}
        depth_cache_lock = threading.Lock()
        
        def get_cached_depth(symbol: str) -> Optional[dict]:
            with depth_cache_lock:
                return depth_cache.get(symbol)
        
        def set_cached_depth(symbol: str, data: dict):
            with depth_cache_lock:
                depth_cache[symbol] = data
        
        cand: List[CandidateRoute] = []
        
        # Parallel execution
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
                            stats_lock=stats_lock,
                        )
                    )
                
                for i, future in enumerate(as_completed(futures), start=1):
                    try:
                        route = future.result()
                        if route:
                            if not any(r.a == route.a and r.b == route.b and r.c == route.c for r in cand):
                                cand.append(route)
                                logger.info(f"✅ Route #{len(cand)}: {route.profit_pct}% net profit")
                    except Exception as e:
                        logger.debug(f"Triangle check failed: {e}")
                    
                    if i % 100 == 0:
                        elapsed = time.time() - start_time
                        logger.info(f"Progress: {i}/{len(all_pairs)}, {len(cand)} routes, {elapsed:.1f}s")
        else:
            # Sequential execution
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
                    if not any(r.a == route.a and r.b == route.b and r.c == route.c for r in cand):
                        cand.append(route)
                
                if i % 100 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Progress: {i}/{len(all_pairs)}, {len(cand)} routes, {elapsed:.1f}s")
        
        # Final stats
        total_time = time.time() - start_time
        stats["total_time"] = total_time
        stats["routes_found"] = len(cand)
        
        logger.info(f"=== SCAN COMPLETE ===")
        logger.info(f"Total checks: {stats['attempts']}, Routes found: {len(cand)}")
        logger.info(f"Time: {total_time:.2f}s, Fee applied: {stats['fee_pct_applied']:.2f}%")
        
        if cand:
            profits = [r.profit_pct for r in cand]
            logger.info(f"Profit range: {min(profits):.4f}% - {max(profits):.4f}%")
        
        # Sort by profit (highest first)
        cand.sort(key=lambda x: x.profit_pct, reverse=True)
        
        return cand[:max_routes], stats
        
    except Exception as e:
        logger.error(f"Error in route scan: {e}", exc_info=True)
        return [], stats


def _parse_leg(label: str) -> Tuple[str, str, str]:
    """Parse leg label like 'BTC/USDT buy' into (base, quote, side)"""
    try:
        label = label.strip()
        if ":" in label:
            pair, side = label.split(":", 1)
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
        
        # Determine major and alt from route pattern
        if "buy" in route.a and "buy" in route.b and "sell" in route.c:
            major = a_base
            alt = c_base
        elif "buy" in route.a and "sell" in route.b and "sell" in route.c:
            major = c_base
            alt = a_base
        else:
            logger.warning(f"Cannot determine pattern for route: {route}")
            return None
        
        client = _client()
        symbols = _load_symbols(client)
        
        # Use wide range for revalidation
        new_route = _try_triangle_pattern(
            client, 
            symbols, 
            base, 
            major, 
            alt,
            min_profit_pct=-50.0,
            max_profit_pct=50.0
        )
        
        if new_route:
            logger.info(f"Route revalidated: {new_route.profit_pct:.4f}% net profit")
        else:
            logger.warning(f"Route validation failed")
        
        return new_route
    except Exception as e:
        logger.error(f"Error revalidating route: {e}", exc_info=True)
        return None


# Backward compatibility alias
find_random_routes = find_candidate_routes