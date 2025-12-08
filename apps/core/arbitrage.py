from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from binance.spot import Spot as BinanceClient
from arbbot import settings as S
from .symbol_loader import get_symbol_loader
import time
import logging
import random
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


def _top_assets_by_quote_volume(
    client: BinanceClient,
    symbols: Dict[str, dict],
    base_quote: str,
    top_n: int = 100,
) -> List[str]:
    """Return a list of asset tickers (e.g., ["BTC","ETH",...]) ranked by 24h quoteVolume against the given base quote (e.g., USDT).

    We consider symbols where quoteAsset == base_quote and status == TRADING, then sort by 24h quoteVolume (descending).
    Falls back to an empty list if the endpoint fails.
    """
    try:
        # Request 24h stats for all symbols once (heavy but single call)
        stats = client.ticker_24hr()
        # Map symbol -> quoteVolume
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

        # Build (asset, volume) using the baseAsset of each symbol (e.g., BTCUSDT -> BTC)
        ranked: List[Tuple[str, float]] = []
        for sym, vol in vol_map.items():
            asset = symbols[sym].get("baseAsset")
            if asset and asset != base_quote:
                ranked.append((asset, vol))

        # Deduplicate by taking max volume per asset
        by_asset: Dict[str, float] = {}
        for asset, vol in ranked:
            by_asset[asset] = max(vol, by_asset.get(asset, 0.0))

        # Sort by volume desc and return top_n assets
        top = sorted(by_asset.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [a for a, _ in top]
    except Exception:
        # Fall back to empty to trigger static universe fallback
        return []


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


def _try_triangle_generic(
    client: BinanceClient,
    symbols: Dict[str, dict],
    x: str,
    y: str,
    z: str,
    min_profit_pct: float = None,
    max_profit_pct: float = None,
    depth_cache: Dict[str, Optional[dict]] = None,
    available_balance: float = None,
    stats: dict = None
) -> Optional[CandidateRoute]:
    """
    Try to find a triangular arbitrage route: x -> y -> z -> x
    Works with any three assets, not just base asset.
    """
    # Pairs for leg 1: x -> y (can be XY or YX)
    pair1_xy = f"{x}{y}"
    pair1_yx = f"{y}{x}"
    # Pairs for leg 2: y -> z (can be YZ or ZY)
    pair2_yz = f"{y}{z}"
    pair2_zy = f"{z}{y}"
    # Pairs for leg 3: z -> x (can be ZX or XZ)
    pair3_zx = f"{z}{x}"
    pair3_xz = f"{x}{z}"

    # Helper to get depth from cache or fetch
    def get_depth(symbol: str) -> Optional[dict]:
        if depth_cache is not None:
            return depth_cache.get(symbol)
        return _depth_snapshot(client, symbol, use_cache=True)
    
    # Leg 1: Convert X to Y
    leg1_label = None
    x_to_y = None
    cap1_x = None  # Capacity in X units
    
    if pair1_xy in symbols:
        d1 = get_depth(pair1_xy)
        if not d1:
            return None
        p1_ask, q1_ask = d1["ask_price"], d1["ask_qty"]
        # Buy Y with X: 1 unit X buys 1/p1_ask units of Y
        x_to_y = 1.0 / p1_ask
        # cap_ask_quote is in X (quote currency) terms
        cap1_x = d1["cap_ask_quote"]
        leg1_label = f"{y}/{x} buy"
    elif pair1_yx in symbols:
        d1 = get_depth(pair1_yx)
        if not d1:
            return None
        p1_bid, q1_bid = d1["bid_price"], d1["bid_qty"]
        # Sell X to get Y: 1 unit X sells for p1_bid units of Y
        x_to_y = p1_bid
        # cap_bid_quote is in X (quote currency) terms
        cap1_x = d1["cap_bid_quote"]
        leg1_label = f"{x}/{y} sell"
    else:
        return None

    # Leg 2: Convert Y to Z
    leg2_label = None
    y_to_z = None
    cap2_y = None  # Capacity in Y units
    
    if pair2_yz in symbols:
        d2 = get_depth(pair2_yz)
        if not d2:
            return None
        p2_ask, q2_ask = d2["ask_price"], d2["ask_qty"]
        # Buy Z with Y: 1 unit Y buys 1/p2_ask units of Z
        y_to_z = 1.0 / p2_ask
        # cap_ask_quote is in Y (quote currency) terms
        cap2_y = d2["cap_ask_quote"]
        leg2_label = f"{z}/{y} buy"
    elif pair2_zy in symbols:
        d2 = get_depth(pair2_zy)
        if not d2:
            return None
        p2_bid, q2_bid = d2["bid_price"], d2["bid_qty"]
        # Sell Y to get Z: 1 unit Y sells for p2_bid units of Z
        y_to_z = p2_bid
        # cap_bid_quote is in Y (quote currency) terms
        cap2_y = d2["cap_bid_quote"]
        leg2_label = f"{y}/{z} sell"
    else:
        return None

    # Leg 3: Convert Z back to X
    leg3_label = None
    z_to_x = None
    cap3_z = None  # Capacity in Z units
    
    if pair3_zx in symbols:
        d3 = get_depth(pair3_zx)
        if not d3:
            return None
        p3_ask, q3_ask = d3["ask_price"], d3["ask_qty"]
        # Buy X with Z: 1 unit Z buys 1/p3_ask units of X
        z_to_x = 1.0 / p3_ask
        # cap_ask_quote is in Z (quote currency) terms
        cap3_z = d3["cap_ask_quote"]
        leg3_label = f"{x}/{z} buy"
    elif pair3_xz in symbols:
        d3 = get_depth(pair3_xz)
        if not d3:
            return None
        p3_bid, q3_bid = d3["bid_price"], d3["bid_qty"]
        # Sell Z to get X: 1 unit Z sells for p3_bid units of X
        z_to_x = p3_bid
        # cap_bid_quote is in Z (quote currency) terms
        cap3_z = d3["cap_bid_quote"]
        leg3_label = f"{z}/{x} sell"
    else:
        return None

    # Apply fees (bps to fraction)
    fee = (S.FEE_RATE_BPS + S.EXTRA_FEE_BPS) / 10000.0
    eff1 = x_to_y * (1 - fee)
    eff2 = y_to_z * (1 - fee)
    eff3 = z_to_x * (1 - fee)

    # Net multiplier for 1 unit of X
    net = eff1 * eff2 * eff3
    profit_pct = (net - 1.0) * 100.0

    # Use passed parameters or fallback to settings
    min_prof = min_profit_pct if min_profit_pct is not None else S.MIN_PROFIT_PCT
    max_prof = max_profit_pct if max_profit_pct is not None else S.MAX_PROFIT_PCT
    
    # Track profit statistics
    if stats is not None:
        if len(stats.get("sample_profits", [])) < 100:
            stats["sample_profits"].append(profit_pct)
    
    # Log filtered routes for debugging (only log a sample to avoid spam)
    if profit_pct < min_prof or profit_pct > max_prof:
        if stats is not None:
            stats["filtered_profit"] = stats.get("filtered_profit", 0) + 1
        # Log occasionally to help diagnose threshold issues
        if random.random() < 0.01:  # Log 1% of filtered routes
            logger.debug(f"Route filtered by profit: {x}-{y}-{z} profit={profit_pct:.4f}% (range: {min_prof}%-{max_prof}%)")
        return None

    # Capacity estimation - convert all to X terms
    # cap1_x is already in X (or needs conversion if it was in quote terms)
    # For XY pair: cap1_x is in X terms (quote currency)
    # For YX pair: cap1_x is in X terms (base currency)
    
    # cap2_y needs to be converted to X: cap2_y / x_to_y
    cap2_x = cap2_y / x_to_y if x_to_y > 0 else 0
    # cap3_z needs to be converted to X: cap3_z / (x_to_y * y_to_z)
    if x_to_y > 0 and y_to_z > 0:
        cap3_x = cap3_z / (x_to_y * y_to_z)
    else:
        cap3_x = 0
    
    # Determine max X notional (in X units)
    max_x_units = max(0.0, min(cap1_x, cap2_x, cap3_x))
    
    # Convert X units to USD using base asset pair (with fallback to BTC/ETH)
    base = S.BASE_ASSET.upper()
    max_volume_usd = None
    
    def try_conversion(x_asset: str, intermediate: str, target: str) -> Optional[float]:
        """Try to convert x_asset to target via intermediate asset"""
        # Try X/INTERMEDIATE → INTERMEDIATE/TARGET
        pair1a = f"{x_asset}{intermediate}"
        pair1b = f"{intermediate}{x_asset}"
        pair2a = f"{intermediate}{target}"
        pair2b = f"{target}{intermediate}"
        
        # Path 1: X/INT → INT/TARGET
        if pair1a in symbols and pair2a in symbols:
            d1 = get_depth(pair1a)
            d2 = get_depth(pair2a)
            if d1 and d2:
                # X/INT: price is INT per X (bid to sell X)
                price1 = d1["bid_price"]
                # INT/TARGET: price is TARGET per INT (bid to sell INT)
                price2 = d2["bid_price"]
                return max_x_units * price1 * price2
        
        # Path 2: INT/X → INT/TARGET
        if pair1b in symbols and pair2a in symbols:
            d1 = get_depth(pair1b)
            d2 = get_depth(pair2a)
            if d1 and d2:
                # INT/X: price is X per INT (ask to buy X)
                price1 = d1["ask_price"]
                if price1 > 0:
                    # INT/TARGET: price is TARGET per INT (bid to sell INT)
                    price2 = d2["bid_price"]
                    return max_x_units / price1 * price2
        
        # Path 3: X/INT → TARGET/INT
        if pair1a in symbols and pair2b in symbols:
            d1 = get_depth(pair1a)
            d2 = get_depth(pair2b)
            if d1 and d2:
                # X/INT: price is INT per X (bid to sell X)
                price1 = d1["bid_price"]
                # TARGET/INT: price is INT per TARGET (ask to buy INT)
                price2 = d2["ask_price"]
                if price2 > 0:
                    return max_x_units * price1 / price2
        
        return None
    
    # Try direct conversion first
    x_base_pair = f"{x}{base}"
    base_x_pair = f"{base}{x}"
    
    if x_base_pair in symbols:
        d_x = get_depth(x_base_pair)
        if d_x:
            price_x = d_x["bid_price"]
            max_volume_usd = max_x_units * price_x
    elif base_x_pair in symbols:
        d_x = get_depth(base_x_pair)
        if d_x:
            price_x = d_x["ask_price"]
            if price_x > 0:
                max_volume_usd = max_x_units / price_x
    
    # Try via BTC if direct conversion failed
    if (max_volume_usd is None or max_volume_usd <= 0) and base != "BTC":
        max_volume_usd = try_conversion(x, "BTC", base)
    
    # Try via ETH if still failed
    if (max_volume_usd is None or max_volume_usd <= 0) and base != "ETH":
        max_volume_usd = try_conversion(x, "ETH", base)
    
    # If we still can't convert, use fallback estimate
    if max_volume_usd is None or max_volume_usd <= 0:
        if stats is not None:
            stats["filtered_usd_conversion"] = stats.get("filtered_usd_conversion", 0) + 1
        # Fallback: Use conservative estimate ($1 per unit)
        # This allows routes to be saved even without direct conversion
        estimated_price = 1.0
        max_volume_usd = max_x_units * estimated_price
        
        # Still validate minimum
        if max_volume_usd < S.MIN_NOTIONAL_USD:
            if random.random() < 0.01:
                logger.debug(f"Route filtered: {x}-{y}-{z} (estimated volume ${max_volume_usd:.2f} < ${S.MIN_NOTIONAL_USD})")
            return None
        
        # Log estimated conversion (occasionally)
        if random.random() < 0.05:
            logger.debug(f"Using estimated USD for {x}-{y}-{z}: ${max_volume_usd:.2f} (no conversion path)")
    
    # Apply limits
    if available_balance is not None and available_balance > 0:
        max_volume_usd = min(available_balance, max_volume_usd, S.MAX_NOTIONAL_USD)
    else:
        max_volume_usd = min(max_volume_usd, S.MAX_NOTIONAL_USD)
    
    if max_volume_usd < S.MIN_NOTIONAL_USD:
        if stats is not None:
            stats["filtered_volume"] = stats.get("filtered_volume", 0) + 1
        if random.random() < 0.01:  # Log 1% of filtered routes
            logger.debug(f"Route filtered by volume: {x}-{y}-{z} volume=${max_volume_usd:.2f} < ${S.MIN_NOTIONAL_USD}")
        return None

    return CandidateRoute(
        a=leg1_label,
        b=leg2_label,
        c=leg3_label,
        profit_pct=round(profit_pct, 4),
        volume_usd=round(max_volume_usd, 2),
    )


def _try_triangle(
    client: BinanceClient, 
    symbols: Dict[str, dict], 
    base: str, 
    x: str, 
    y: str,
    min_profit_pct: float = None,
    max_profit_pct: float = None,
    depth_cache: Dict[str, Optional[dict]] = None,
    available_balance: float = None
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
    
    # Log filtered routes for debugging (only log a sample to avoid spam)
    if profit_pct < min_prof or profit_pct > max_prof:
        # Log occasionally to help diagnose threshold issues
        if random.random() < 0.01:  # Log 1% of filtered routes
            logger.debug(f"Route filtered: {x}-{y} profit={profit_pct:.4f}% (range: {min_prof}%-{max_prof}%)")
        return None

    # Capacity estimation using shallow depth snapshot
    # Determine max BASE notional respecting leg capacities
    # Leg1 cap in BASE: cap1_usd
    # Leg2 cap in BASE: cap2_x converted to BASE via inverse of leg1 rate (approx)
    # Leg3 cap in BASE: cap3_y converted to BASE via y_to_base
    cap2_base = (cap2_x * x_to_y) * y_to_base  # X->Y at mid leg 2 then to BASE at leg3
    cap3_base = cap3_y * y_to_base
    
    # If available_balance is provided, use it as the target (but still respect market capacity)
    if available_balance is not None and available_balance > 0:
        # Use available balance, but cap at market capacity and MAX_NOTIONAL_USD
        max_base = min(available_balance, cap1_usd, cap2_base, cap3_base, S.MAX_NOTIONAL_USD)
    else:
        # Original logic: use market capacity
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


def _try_triangle_fixed(
    client: BinanceClient,
    symbols: Dict[str, dict],
    base: str,
    x: str,
    y: str,
    min_profit_pct: float = None,
    max_profit_pct: float = None,
    depth_cache: Dict[str, Optional[dict]] = None,
    available_balance: float = None,
) -> Optional[CandidateRoute]:
    """
    Fixed triangle detection that works with Binance's actual pair listings.
    Tries several common pair direction patterns.
    """

    def get_depth(symbol: str) -> Optional[dict]:
        if depth_cache is not None:
            return depth_cache.get(symbol)
        return _depth_snapshot(client, symbol, use_cache=True)

    routes = []

    # Pattern A: X/BASE → Y/X → BASE/Y
    if f"{x}{base}" in symbols and f"{y}{x}" in symbols and f"{base}{y}" in symbols:
        routes.append({
            "pairs": [f"{x}{base}", f"{y}{x}", f"{base}{y}"],
            "labels": [f"{x}/{base} buy", f"{y}/{x} buy", f"{base}/{y} sell"],
            "conversions": [
                lambda d: 1.0 / d["ask_price"],  # BASE→X
                lambda d: 1.0 / d["ask_price"],  # X→Y
                lambda d: d["bid_price"],        # Y→BASE
            ],
            "qty_refs": ["ask_qty", "ask_qty", "bid_qty"],
        })

    # Pattern B: X/BASE → X/Y → Y/BASE
    if f"{x}{base}" in symbols and f"{x}{y}" in symbols and f"{y}{base}" in symbols:
        routes.append({
            "pairs": [f"{x}{base}", f"{x}{y}", f"{y}{base}"],
            "labels": [f"{x}/{base} buy", f"{x}/{y} sell", f"{y}/{base} sell"],
            "conversions": [
                lambda d: 1.0 / d["ask_price"],  # BASE→X
                lambda d: d["bid_price"],        # X→Y
                lambda d: d["bid_price"],        # Y→BASE
            ],
            "qty_refs": ["ask_qty", "bid_qty", "bid_qty"],
        })

    # Pattern C: BASE/X → X/Y → Y/BASE
    if f"{base}{x}" in symbols and f"{x}{y}" in symbols and f"{y}{base}" in symbols:
        routes.append({
            "pairs": [f"{base}{x}", f"{x}{y}", f"{y}{base}"],
            "labels": [f"{base}/{x} sell", f"{x}/{y} sell", f"{y}/{base} sell"],
            "conversions": [
                lambda d: d["bid_price"],        # BASE→X (sell BASE for X)
                lambda d: d["bid_price"],        # X→Y
                lambda d: d["bid_price"],        # Y→BASE
            ],
            "qty_refs": ["bid_qty", "bid_qty", "bid_qty"],
        })

    best_route: Optional[CandidateRoute] = None
    best_profit = -1e9

    for route in routes:
        depths: List[dict] = []
        for pair in route["pairs"]:
            d = get_depth(pair)
            if not d:
                break
            depths.append(d)
        else:
            # Compute conversion rates
            conv_rates: List[float] = []
            for i, fn in enumerate(route["conversions"]):
                conv_rates.append(fn(depths[i]))

            # Apply fees from settings (set to 0 in settings for no-fee testing)
            fee = (S.FEE_RATE_BPS + S.EXTRA_FEE_BPS) / 10000.0
            eff1 = conv_rates[0] * (1 - fee)
            eff2 = conv_rates[1] * (1 - fee)
            eff3 = conv_rates[2] * (1 - fee)
            net = eff1 * eff2 * eff3
            profit_pct = (net - 1.0) * 100.0

            # Profit bounds
            min_prof = min_profit_pct if min_profit_pct is not None else S.MIN_PROFIT_PCT
            max_prof = max_profit_pct if max_profit_pct is not None else S.MAX_PROFIT_PCT
            if not (min_prof <= profit_pct <= max_prof):
                continue

            # Rough volume calc in BASE terms using available depths
            try:
                vol1 = depths[0][route["qty_refs"][0]] * depths[0]["ask_price"] if route["qty_refs"][0] == "ask_qty" else depths[0][route["qty_refs"][0]] * depths[0]["bid_price"]
            except KeyError:
                vol1 = 0.0
            try:
                # Convert leg2 qty to BASE using previous conversion
                price_leg2 = depths[1]["ask_price"] if route["qty_refs"][1] == "ask_qty" else depths[1]["bid_price"]
                qty_leg2 = depths[1][route["qty_refs"][1]]
                # Approx in BASE by chaining with first conversion rate
                vol2 = qty_leg2 * price_leg2
            except KeyError:
                vol2 = 0.0
            try:
                # Leg3 already quoted against BASE in common patterns
                price_leg3 = depths[2]["bid_price"]
                qty_leg3 = depths[2][route["qty_refs"][2]]
                vol3 = qty_leg3 * price_leg3
            except KeyError:
                vol3 = 0.0

            max_volume_usd = max(0.0, min(vol1, vol2, vol3))
            if available_balance is not None and available_balance > 0:
                max_volume_usd = min(available_balance, max_volume_usd, S.MAX_NOTIONAL_USD)
            else:
                max_volume_usd = min(max_volume_usd, S.MAX_NOTIONAL_USD)

            if max_volume_usd < S.MIN_NOTIONAL_USD:
                continue

            candidate = CandidateRoute(
                a=route["labels"][0],
                b=route["labels"][1],
                c=route["labels"][2],
                profit_pct=round(profit_pct, 4),
                volume_usd=round(max_volume_usd, 2),
            )
            if profit_pct > best_profit:
                best_profit = profit_pct
                best_route = candidate

    return best_route

def find_candidate_routes(
    *, 
    min_profit_pct: float, 
    max_profit_pct: float,
    available_balance: float = None
) -> Tuple[List[CandidateRoute], dict]:
    """
    Find candidate triangular arbitrage routes.
    Returns: (routes_list, stats_dict)
    stats_dict contains: symbols_loaded, symbols_fetched, triangles_checked, routes_found
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
        "filtered_usd_conversion": 0,
        "filtered_volume": 0,
        "filtered_missing_pairs": 0,
        "sample_profits": [],  # Store sample profit values for analysis
    }
    
    try:
        client = _client()
        symbols = _load_symbols(client)
        stats["symbols_loaded"] = len(symbols)
        logger.info(f"Loaded {len(symbols)} symbols, filtering for profit: {min_profit_pct}% - {max_profit_pct}%")
        
        base = S.BASE_ASSET.upper()
        # Dynamic universe: top assets by 24h quoteVolume against BASE
        universe = _top_assets_by_quote_volume(client, symbols, base_quote=base, top_n=100)
        
        # Safe fallback to previous static universe if dynamic detection fails
        if not universe:
            universe = [
                "BTC", "ETH", "BNB", "SOL", "ADA", "XRP", "DOGE", "MATIC", 
                "AVAX", "DOT", "LINK", "UNI", "ATOM", "LTC", "ETC", "XLM",
                "ALGO", "VET", "FIL", "TRX", "EOS", "AAVE", "SXP", "CHZ",
                "BICO", "LISTA", "APT", "ARB", "OP", "SUI", "SEI", "TIA"
            ]
        
        # Ensure BASE itself is not in the universe
        universe = [asset for asset in universe if asset != base]
        
        # Step 1: Collect all unique symbols needed for BASE-asset triangles only
        # Routes: BASE → y → z → BASE (where y and z are from universe)
        needed_symbols = set()
        for i, y in enumerate(universe):
            for z in universe[i + 1:]:
                if y == z:
                    continue
                # Add all possible pair combinations for triangle BASE->y->z->BASE
                # Leg 1: BASE -> y (need BASE/Y or Y/BASE)
                needed_symbols.add(f"{base}{y}")
                needed_symbols.add(f"{y}{base}")
                # Leg 2: y -> z (need Y/Z or Z/Y)
                needed_symbols.add(f"{y}{z}")
                needed_symbols.add(f"{z}{y}")
                # Leg 3: z -> BASE (need Z/BASE or BASE/Z)
                needed_symbols.add(f"{z}{base}")
                needed_symbols.add(f"{base}{z}")
        
        # Filter to only symbols that exist
        existing_symbols = [s for s in needed_symbols if s in symbols]
        logger.info(f"Pre-fetching depths for {len(existing_symbols)} unique symbols (BASE-asset triangles only)...")
        
        # Step 2: Fetch all depths in parallel
        start_fetch = time.time()
        depth_cache = _fetch_depth_parallel(client, existing_symbols, max_workers=20)
        fetch_time = time.time() - start_fetch
        fetched_count = sum(1 for v in depth_cache.values() if v is not None)
        stats["symbols_fetched"] = fetched_count
        stats["fetch_time"] = fetch_time
        logger.info(f"Fetched {fetched_count}/{len(existing_symbols)} depths in {fetch_time:.2f}s (parallel)")
        
        # Step 3: Check BASE-asset triangles only: BASE -> y -> z -> BASE
        cand: List[CandidateRoute] = []
        checked = 0
        start_triangles = time.time()
        for i, y in enumerate(universe):
            for z in universe[i + 1:]:
                if y == z:
                    continue
                checked += 1
                # Use the fixed triangle detector matching Binance pair formats
                r = _try_triangle_fixed(
                    client, symbols, base, y, z,
                    min_profit_pct, max_profit_pct,
                    depth_cache=depth_cache,
                    available_balance=available_balance
                )
                if r:
                    logger.debug(f"Found route: {r.a} → {r.b} → {r.c}, profit: {r.profit_pct}%, volume: ${r.volume_usd}")
                    cand.append(r)
                else:
                    # Check if pairs exist (for missing pairs tracking)
                    pair1_y = f"{y}{base}"
                    pair1_y_inv = f"{base}{y}"
                    pair2_yz = f"{y}{z}"
                    pair2_zy = f"{z}{y}"
                    pair3_z = f"{z}{base}"
                    pair3_z_inv = f"{base}{z}"
                    pairs_exist = (
                        (pair1_y in symbols or pair1_y_inv in symbols) and
                        (pair2_yz in symbols or pair2_zy in symbols) and
                        (pair3_z in symbols or pair3_z_inv in symbols)
                    )
                    if not pairs_exist and stats is not None:
                        stats["filtered_missing_pairs"] = stats.get("filtered_missing_pairs", 0) + 1
        
        triangle_time = time.time() - start_triangles
        stats["triangles_checked"] = checked
        stats["routes_found"] = len(cand)
        stats["triangle_time"] = triangle_time
        logger.info(f"Checked {checked} triangles in {triangle_time:.2f}s ({triangle_time/checked*1000:.1f}ms per triangle)")
        
        # Log diagnostic information
        filtered_profit = stats.get("filtered_profit", 0)
        filtered_usd = stats.get("filtered_usd_conversion", 0)
        filtered_volume = stats.get("filtered_volume", 0)
        filtered_missing = stats.get("filtered_missing_pairs", 0)
        sample_profits = stats.get("sample_profits", [])
        
        logger.info(f"Checked {checked} triangle combinations, found {len(cand)} profitable routes")
        logger.info(f"Filter statistics: profit={filtered_profit}, usd_conversion={filtered_usd}, volume={filtered_volume}, missing_pairs={filtered_missing}")
        
        if sample_profits:
            sample_profits.sort()
            logger.info(f"Sample profit range: min={min(sample_profits):.4f}%, max={max(sample_profits):.4f}%, "
                       f"median={sample_profits[len(sample_profits)//2]:.4f}%, "
                       f"avg={sum(sample_profits)/len(sample_profits):.4f}%")
        
        # Sort by profit desc and return a few
        cand.sort(key=lambda z: z.profit_pct, reverse=True)
        return cand[:5], stats
    except Exception as e:
        logger.error(f"Error in find_candidate_routes: {e}", exc_info=True)
        return [], stats


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
