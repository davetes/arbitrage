from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from binance.spot import Spot as BinanceClient
from arbbot import settings as S
from .symbol_loader import get_symbol_loader
import time
import logging
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


@dataclass
class CandidateRoute:
    a: str  # leg1 label e.g., "BTC/USDT buy"
    b: str  # leg2 label e.g., "SXP/BTC buy"
    c: str  # leg3 label e.g., "SXP/USDT sell"
    profit_pct: float
    volume_usd: float


def _client(timeout: int = None) -> BinanceClient:
    # Public endpoints are sufficient for order books; keys optional
    # Add timeout to prevent hanging on network issues
    # Default 30s for exchangeInfo (large download), can be overridden
    if timeout is None:
        timeout = 30  # Increased for exchangeInfo downloads (15.5MB)
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
    """Load symbols using FastSymbolLoader (cached, USDT-filtered)"""
    loader = get_symbol_loader(cache_ttl_seconds=300)  # 5 minute cache
    return loader.load_symbols(client, base_asset=S.BASE_ASSET, use_cache=True)


def _depth_snapshot(client: BinanceClient, symbol: str, depth: int = 20, use_cache: bool = True) -> Optional[dict]:
    """Get depth snapshot with caching and retries"""
    # Check cache first
    if use_cache:
        cached = _depth_cache.get(symbol)
        if cached is not None:
            return cached
    
    # Fetch from API with retries
    backoff = 0.25
    for _ in range(3):
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
            # Cache the result
            if use_cache:
                _depth_cache.set(symbol, result)
            return result
        except Exception:
            time.sleep(backoff)
            backoff *= 2
    return None


def _fetch_depth_parallel(client: BinanceClient, symbols: List[str], max_workers: int = 10) -> Dict[str, Optional[dict]]:
    """Fetch multiple depth snapshots in parallel"""
    results = {}
    
    def fetch_one(symbol: str):
        return symbol, _depth_snapshot(client, symbol, use_cache=True)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_one, sym): sym for sym in symbols}
        for future in as_completed(future_to_symbol):
            symbol, depth = future.result()
            results[symbol] = depth
    
    return results


def _try_triangle(
    client: BinanceClient, 
    symbols: Dict[str, dict], 
    base: str, 
    x: str, 
    y: str,
    min_profit_pct: float = None,
    max_profit_pct: float = None,
    depth_cache: Dict[str, Optional[dict]] = None
) -> Optional[CandidateRoute]:
    # Route: base -> x -> y -> base using available direction per symbol existence
    # Pairs considered: XBASE, XY or YX, YBASE
    pair1 = f"{x}{base}"
    pair1_inv = f"{base}{x}"
    pair2_xy = f"{x}{y}"
    pair2_yx = f"{y}{x}"
    pair3 = f"{y}{base}"
    pair3_inv = f"{base}{y}"

    # Helper to get depth from cache or fetch
    def get_depth(symbol: str) -> Optional[dict]:
        if depth_cache is not None:
            return depth_cache.get(symbol)
        return _depth_snapshot(client, symbol, use_cache=True)
    
    # Leg 1: buy X with BASE (prefer XBASE ask); if only BASEX exists, sell BASE for X via BASEX bid
    leg1_label = None
    if pair1 in symbols:
        d1 = get_depth(pair1)
        if not d1:
            return None
        p1_ask, q1_ask = d1["ask_price"], d1["ask_qty"]
        # 1 unit BASE buys BASE/p1_ask units of X
        base_to_x = 1.0 / p1_ask
        cap1_usd = d1["cap_ask_quote"]  # quote notional depth
        leg1_label = f"{x}/{base} buy"
    elif pair1_inv in symbols:
        d1 = get_depth(pair1_inv)
        if not d1:
            return None
        p1_bid, q1_bid = d1["bid_price"], d1["bid_qty"]
        # Sell BASE to receive X at bid: 1 BASE sells for 1/p1_bid X
        base_to_x = 1.0 / p1_bid
        cap1_usd = d1["cap_bid_quote"]
        leg1_label = f"{base}/{x} sell"
    else:
        return None

    # Leg 2: convert X to Y using XY or YX
    if pair2_xy in symbols:
        d2 = get_depth(pair2_xy)
        if not d2:
            return None
        p2_bid, q2_bid = d2["bid_price"], d2["bid_qty"]
        # Sell X to get Y at bid
        x_to_y = p2_bid
        # capacity in X is total bid qty
        cap2_x = d2["total_bid_qty"]
        leg2_label = f"{y}/{x} buy"  # equivalent of selling X for Y on XY
    elif pair2_yx in symbols:
        d2 = get_depth(pair2_yx)
        if not d2:
            return None
        p2_ask, q2_ask = d2["ask_price"], d2["ask_qty"]
        # Buy X->Y using YX ask: X amount buys X/p2_ask of Y
        x_to_y = 1.0 / p2_ask
        cap2_x = d2["total_ask_qty"] * p2_ask  # convert Y qty to X spend roughly
        leg2_label = f"{x}/{y} buy"
    else:
        return None

    # Leg 3: convert Y to BASE
    if pair3 in symbols:
        d3 = get_depth(pair3)
        if not d3:
            return None
        p3_bid, q3_bid = d3["bid_price"], d3["bid_qty"]
        y_to_base = p3_bid
        cap3_y = d3["total_bid_qty"]
        leg3_label = f"{y}/{base} sell"
    elif pair3_inv in symbols:
        d3 = get_depth(pair3_inv)
        if not d3:
            return None
        p3_ask, q3_ask = d3["ask_price"], d3["ask_qty"]
        y_to_base = 1.0 / p3_ask
        cap3_y = d3["total_ask_qty"] * p3_ask
        leg3_label = f"{base}/{y} buy"
    else:
        return None

    # Apply fees (bps to fraction)
    fee = (S.FEE_RATE_BPS + S.EXTRA_FEE_BPS) / 10000.0
    eff1 = base_to_x * (1 - fee)
    eff2 = x_to_y * (1 - fee)
    eff3 = y_to_base * (1 - fee)

    # Net multiplier for 1 unit of BASE
    net = eff1 * eff2 * eff3
    profit_pct = (net - 1.0) * 100.0

    # Use passed parameters or fallback to settings
    min_prof = min_profit_pct if min_profit_pct is not None else S.MIN_PROFIT_PCT
    max_prof = max_profit_pct if max_profit_pct is not None else S.MAX_PROFIT_PCT
    
    if profit_pct < min_prof or profit_pct > max_prof:
        return None

    # Capacity estimation using shallow depth snapshot
    # Determine max BASE notional respecting leg capacities
    # Leg1 cap in BASE: cap1_usd
    # Leg2 cap in BASE: cap2_x converted to BASE via inverse of leg1 rate (approx)
    # Leg3 cap in BASE: cap3_y converted to BASE via y_to_base
    cap2_base = (cap2_x * x_to_y) * y_to_base  # X->Y at mid leg 2 then to BASE at leg3
    cap3_base = cap3_y * y_to_base
    max_base = max(0.0, min(cap1_usd, cap2_base, cap3_base, S.MAX_NOTIONAL_USD))
    if max_base < S.MIN_NOTIONAL_USD:
        return None

    return CandidateRoute(
        a=leg1_label,
        b=leg2_label,
        c=leg3_label,
        profit_pct=round(profit_pct, 4),
        volume_usd=round(max_base, 2),
    )


def find_candidate_routes(*, min_profit_pct: float, max_profit_pct: float) -> List[CandidateRoute]:
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        client = _client()
        symbols = _load_symbols(client)
        logger.info(f"Loaded {len(symbols)} symbols, filtering for profit: {min_profit_pct}% - {max_profit_pct}%")
        
        base = S.BASE_ASSET.upper()
        # Expanded universe of popular trading pairs with good liquidity
        universe = [
            "BTC", "ETH", "BNB", "SOL", "ADA", "XRP", "DOGE", "MATIC", 
            "AVAX", "DOT", "LINK", "UNI", "ATOM", "LTC", "ETC", "XLM",
            "ALGO", "VET", "FIL", "TRX", "EOS", "AAVE", "SXP", "CHZ",
            "BICO", "LISTA", "APT", "ARB", "OP", "SUI", "SEI", "TIA"
        ]
        
        # Step 1: Collect all unique symbols needed for triangles
        needed_symbols = set()
        for i, x in enumerate(universe):
            if x == base:
                continue
            for y in universe[i + 1:]:
                if y == base or y == x:
                    continue
                # Add all possible pair combinations
                needed_symbols.add(f"{x}{base}")
                needed_symbols.add(f"{base}{x}")
                needed_symbols.add(f"{x}{y}")
                needed_symbols.add(f"{y}{x}")
                needed_symbols.add(f"{y}{base}")
                needed_symbols.add(f"{base}{y}")
        
        # Filter to only symbols that exist
        existing_symbols = [s for s in needed_symbols if s in symbols]
        logger.info(f"Pre-fetching depths for {len(existing_symbols)} unique symbols...")
        
        # Step 2: Fetch all depths in parallel
        start_fetch = time.time()
        depth_cache = _fetch_depth_parallel(client, existing_symbols, max_workers=20)
        fetch_time = time.time() - start_fetch
        fetched_count = sum(1 for v in depth_cache.values() if v is not None)
        logger.info(f"Fetched {fetched_count}/{len(existing_symbols)} depths in {fetch_time:.2f}s (parallel)")
        
        # Step 3: Check triangles using cached depths
        cand: List[CandidateRoute] = []
        checked = 0
        start_triangles = time.time()
        for i, x in enumerate(universe):
            if x == base:
                continue
            for y in universe[i + 1:]:
                if y == base or y == x:
                    continue
                checked += 1
                r = _try_triangle(client, symbols, base, x, y, min_profit_pct, max_profit_pct, depth_cache=depth_cache)
                if r:
                    logger.debug(f"Found route: {r.a} → {r.b} → {r.c}, profit: {r.profit_pct}%, volume: ${r.volume_usd}")
                    cand.append(r)
        
        triangle_time = time.time() - start_triangles
        logger.info(f"Checked {checked} triangles in {triangle_time:.2f}s ({triangle_time/checked*1000:.1f}ms per triangle)")
        
        logger.info(f"Checked {checked} triangle combinations, found {len(cand)} profitable routes")
        # Sort by profit desc and return a few
        cand.sort(key=lambda z: z.profit_pct, reverse=True)
        return cand[:5]
    except Exception as e:
        logger.error(f"Error in find_candidate_routes: {e}", exc_info=True)
        return []


def _parse_leg(label: str) -> Tuple[str, str, str]:
    # returns (base, quote, side) from label like "BTC/USDT buy"
    pair, side = label.split()
    base, quote = pair.split("/")
    return base, quote, side.lower()


def revalidate_route(route: CandidateRoute) -> Optional[CandidateRoute]:
    try:
        # Infer x and y from legs relative to BASE
        a_base, a_quote, _ = _parse_leg(route.a)
        c_base, c_quote, _ = _parse_leg(route.c)
        base = S.BASE_ASSET.upper()
        # x is the non-base asset in leg a
        x = a_base if a_quote == base else a_quote
        # y is the non-base asset in leg c
        y = c_base if c_quote == base else c_quote
        client = _client()
        symbols = _load_symbols(client)
        new = _try_triangle(client, symbols, base, x, y)
        return new
    except Exception:
        return None
