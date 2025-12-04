from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from binance.spot import Spot as BinanceClient
from arbbot import settings as S


@dataclass
class CandidateRoute:
    a: str  # leg1 label e.g., "BTC/USDT buy"
    b: str  # leg2 label e.g., "SXP/BTC buy"
    c: str  # leg3 label e.g., "SXP/USDT sell"
    profit_pct: float
    volume_usd: float


def _client() -> BinanceClient:
    # Public endpoints are sufficient for order books; keys optional
    if S.BINANCE_API_KEY and S.BINANCE_API_SECRET:
        return BinanceClient(api_key=S.BINANCE_API_KEY, api_secret=S.BINANCE_API_SECRET)
    return BinanceClient()


def _load_symbols(client: BinanceClient) -> Dict[str, dict]:
    info = client.exchange_info()
    return {s["symbol"]: s for s in info.get("symbols", []) if s.get("status") == "TRADING"}


def _depth_best(client: BinanceClient, symbol: str) -> Optional[Tuple[float, float, float, float]]:
    # returns (best_ask_price, best_ask_qty, best_bid_price, best_bid_qty)
    try:
        d = client.depth(symbol, limit=5)
        if not d.get("asks") or not d.get("bids"):
            return None
        ask_p, ask_q = float(d["asks"][0][0]), float(d["asks"][0][1])
        bid_p, bid_q = float(d["bids"][0][0]), float(d["bids"][0][1])
        return ask_p, ask_q, bid_p, bid_q
    except Exception:
        return None


def _try_triangle(client: BinanceClient, symbols: Dict[str, dict], base: str, x: str, y: str) -> Optional[CandidateRoute]:
    # Route: base -> x -> y -> base using available direction per symbol existence
    # Pairs considered: XBASE, XY or YX, YBASE
    pair1 = f"{x}{base}"
    pair1_inv = f"{base}{x}"
    pair2_xy = f"{x}{y}"
    pair2_yx = f"{y}{x}"
    pair3 = f"{y}{base}"
    pair3_inv = f"{base}{y}"

    # Leg 1: buy X with BASE (prefer XBASE ask); if only BASEX exists, sell BASE for X via BASEX bid
    leg1_label = None
    if pair1 in symbols:
        d1 = _depth_best(client, pair1)
        if not d1:
            return None
        p1_ask, q1_ask, _, _ = d1
        # 1 unit BASE buys BASE/p1_ask units of X
        base_to_x = 1.0 / p1_ask
        cap1_usd = p1_ask * q1_ask  # in BASE (assuming BASE is USDT)
        leg1_label = f"{x}/{base} buy"
    elif pair1_inv in symbols:
        d1 = _depth_best(client, pair1_inv)
        if not d1:
            return None
        _, _, p1_bid, q1_bid = d1
        # Sell BASE to receive X at bid: 1 BASE sells for 1/p1_bid X
        base_to_x = 1.0 / p1_bid
        cap1_usd = p1_bid * q1_bid
        leg1_label = f"{base}/{x} sell"
    else:
        return None

    # Leg 2: convert X to Y using XY or YX
    if pair2_xy in symbols:
        d2 = _depth_best(client, pair2_xy)
        if not d2:
            return None
        _, _, p2_bid, q2_bid = d2
        # Sell X to get Y at bid
        x_to_y = p2_bid
        # capacity in X is q2_bid
        cap2_x = q2_bid
        leg2_label = f"{y}/{x} buy"  # equivalent of selling X for Y on XY
    elif pair2_yx in symbols:
        d2 = _depth_best(client, pair2_yx)
        if not d2:
            return None
        p2_ask, q2_ask, _, _ = d2
        # Buy X->Y using YX ask: X amount buys X/p2_ask of Y
        x_to_y = 1.0 / p2_ask
        cap2_x = q2_ask * p2_ask  # Y qty ask -> max X you can spend ~ ask_qty*ask_price
        leg2_label = f"{x}/{y} buy"
    else:
        return None

    # Leg 3: convert Y to BASE
    if pair3 in symbols:
        d3 = _depth_best(client, pair3)
        if not d3:
            return None
        _, _, p3_bid, q3_bid = d3
        y_to_base = p3_bid
        cap3_y = q3_bid
        leg3_label = f"{y}/{base} sell"
    elif pair3_inv in symbols:
        d3 = _depth_best(client, pair3_inv)
        if not d3:
            return None
        p3_ask, q3_ask, _, _ = d3
        y_to_base = 1.0 / p3_ask
        cap3_y = q3_ask * p3_ask
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

    if profit_pct < S.MIN_PROFIT_PCT or profit_pct > S.MAX_PROFIT_PCT:
        return None

    # Rough capacity estimation using top-of-book only
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
    try:
        client = _client()
        symbols = _load_symbols(client)
        base = S.BASE_ASSET.upper()
        # Limit universe for now; can expand later or fetch top volumes
        universe = ["BTC", "ETH", "BNB", "SXP", "CHZ", "BICO", "LISTA"]
        cand: List[CandidateRoute] = []
        for i, x in enumerate(universe):
            if x == base:
                continue
            for y in universe[i + 1 :]:
                if y == base or y == x:
                    continue
                r = _try_triangle(client, symbols, base, x, y)
                if r:
                    # Clamp to provided thresholds at call-site too
                    if r.profit_pct >= min_profit_pct and r.profit_pct <= max_profit_pct:
                        cand.append(r)
        # Sort by profit desc and return a few
        cand.sort(key=lambda z: z.profit_pct, reverse=True)
        return cand[:5]
    except Exception:
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
