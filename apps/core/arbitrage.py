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
    """Websocket-based best bid/ask cache."""

    def __init__(self, ttl_seconds: float = 2.0):
        self.ttl = ttl_seconds
        self.client: Optional[SpotWebsocketClient] = None
        self.lock = threading.Lock()
        self.cache: Dict[str, Tuple[dict, float]] = {}
        self.running = False

    def _on_message(self, _, message: str):
        try:
            data = json.loads(message)
            if not isinstance(data, dict):
                return
            symbol = data.get("s") or data.get("symbol")
            bid_price = float(data.get("b"))
            bid_qty = float(data.get("B"))
            ask_price = float(data.get("a"))
            ask_qty = float(data.get("A"))
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
        except Exception:
            return

    def start(self, symbols: List[str], **kwargs):
        """Start bookTicker websocket for given symbols (idempotent)."""
        if self.running:
            return
        if SpotWebsocketClient is None:
            logger.warning("SpotWebsocketClient unavailable; skipping websocket bookTicker startup.")
            return
        # Binance expects lowercase symbols for stream names.
        stream_symbols = [s.lower() for s in symbols]
        self.client = SpotWebsocketClient(on_message=self._on_message, **kwargs)
        self.client.start()
        for sym in stream_symbols:
            self.client.book_ticker(symbol=sym)
        self.running = True

    def stop(self):
        if self.client:
            try:
                self.client.stop()
            except Exception:
                pass
        self.running = False

    def get(self, symbol: str) -> Optional[dict]:
        now = time.time()
        with self.lock:
            if symbol.upper() in self.cache:
                data, ts = self.cache[symbol.upper()]
                if now - ts < self.ttl:
                    return data
                else:
                    del self.cache[symbol.upper()]
        return None


_book_ticker_stream = _BookTickerStream(ttl_seconds=2.0)


@dataclass
class CandidateRoute:
    a: str  # leg1 label e.g., "BTC/USDT buy"
    b: str  # leg2 label e.g., "ETH/BTC buy"
    c: str  # leg3 label e.g., "ETH/USDT sell"
    profit_pct: float
    volume_usd: float


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
    """Load symbols using FastSymbolLoader (cached, optionally full universe)"""
    loader = get_symbol_loader(cache_ttl_seconds=300)
    include_all_pairs = getattr(S, "LOAD_ALL_SYMBOLS", True)
    return loader.load_symbols(
        client,
        base_asset=S.BASE_ASSET,
        use_cache=True,
        include_all_pairs=include_all_pairs,
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


def _top_assets_multi_quote(
    client: BinanceClient,
    symbols: Dict[str, dict],
    quotes: List[str],
    max_assets: int = 120,
) -> List[str]:
    """Rank assets by max 24h quoteVolume across multiple quote currencies."""
    try:
        stats = client.ticker_24hr()
    except Exception:
        return []

    quotes_upper = {q.upper() for q in quotes}
    vol_by_asset: Dict[str, float] = {}

    for s in stats:
        sym = s.get("symbol")
        if not sym or sym not in symbols:
            continue
        info = symbols[sym]
        if info.get("status") != "TRADING":
            continue
        quote = info.get("quoteAsset", "").upper()
        if quote not in quotes_upper:
            continue
        base_asset = info.get("baseAsset", "").upper()
        if not base_asset or base_asset == quote:
            continue
        try:
            vol = float(s.get("quoteVolume", 0))
        except Exception:
            continue
        # Keep max volume seen across quotes
        vol_by_asset[base_asset] = max(vol_by_asset.get(base_asset, 0.0), vol)

    top = sorted(vol_by_asset.items(), key=lambda x: x[1], reverse=True)[:max_assets]
    return [a for a, _ in top]


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


def _fetch_depth_parallel(client: BinanceClient, symbols: List[str], max_workers: int = 10) -> Dict[str, Optional[dict]]:
    """Fetch multiple depth snapshots in parallel with rate limiting"""
    results = {}
    
    # Rate limiting: Binance allows ~1200 requests per minute
    # We'll be conservative: max 10 requests per second = 600/min
    request_delay = 0.1  # 100ms between requests = 10 req/sec
    last_request_time = [0.0]  # Use list to allow modification in nested function
    
    def fetch_one(symbol: str):
        # Rate limiting: ensure minimum delay between requests
        elapsed = time.time() - last_request_time[0]
        if elapsed < request_delay:
            time.sleep(request_delay - elapsed)
        last_request_time[0] = time.time()
        return symbol, _depth_snapshot(client, symbol, use_cache=True)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_one, sym): sym for sym in symbols}
        for future in as_completed(future_to_symbol):
            try:
                symbol, depth = future.result()
                results[symbol] = depth
            except Exception as e:
                logger.debug(f"Failed to fetch depth for {symbol}: {e}")
    
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
    stats: dict = None
) -> Optional[CandidateRoute]:
    """
    Triangle detection for Binance's actual pair formats.
    Route: base -> x -> y -> base
    Format required: XBASE (e.g., BTCUSDT), YX (e.g., ETHBTC), YBASE (e.g., ETHUSDT)
    """
    def get_depth(symbol: str) -> Optional[dict]:
        if depth_cache is not None:
            return depth_cache.get(symbol)
        return _depth_snapshot(client, symbol, use_cache=True)
    
    # We need these 3 symbols:
    # 1. X/base (e.g., BTCUSDT) - buy X with base
    # 2. Y/X (e.g., ETHBTC) - buy Y with X (or inverse if only XY exists)
    # 3. Y/base (e.g., ETHUSDT) - sell Y for base
    
    symbol1 = f"{x}{base}"  # Xbase (e.g., BTCUSDT)
    symbol2 = f"{y}{x}"     # YX preferred (e.g., ETHBTC)
    symbol2_alt = f"{x}{y}" # XY fallback (e.g., BTCETH)
    symbol3 = f"{y}{base}"  # Ybase (e.g., ETHUSDT)
    
    # Check all symbols exist
    if symbol1 not in symbols or symbol3 not in symbols:
        return None
    
    # Get depths
    d1 = get_depth(symbol1)
    d2 = None
    inverted_cross = False
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
                    # quantities not used in theoretical mode; keep placeholders
                    "ask_qty": orig.get("bid_qty", 0),
                    "bid_qty": orig.get("ask_qty", 0),
                    "total_ask_qty": orig.get("total_bid_qty", 0),
                    "total_bid_qty": orig.get("total_ask_qty", 0),
                    "cap_ask_quote": 0,
                    "cap_bid_quote": 0,
                }
                inverted_cross = True
    d3 = get_depth(symbol3)
    
    if not all([d1, d2, d3]):
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
        """Evaluate pattern using exact formulas from user specification."""
        legs = patterns[pat]
        executed_sides = []
        prices = []
        steps = []
        
        # Extract prices and build step descriptions
        for (side_price, sym, depth, logical_side) in legs:
            price = depth["ask_price"] if side_price == "ask" else depth["bid_price"]
            if price <= 0 or not isinstance(price, (int, float)) or price > 1e10:
                return None  # Invalid price
            prices.append(price)
            steps.append(f"{logical_side} {sym} @{price:.10f}")
            executed_sides.append(logical_side.lower())
        
        # Calculate gross profit using exact formulas
        # Pattern 1: BUY-BUY-SELL -> (1 ÷ Price₁) × (1 ÷ Price₂) × Price₃ - 1
        # Pattern 2: BUY-SELL-SELL -> (1 ÷ Price₁) × Price₂ × Price₃ - 1
        if pat == ("BUY", "BUY", "SELL"):
            # Pattern 1: BUY-BUY-SELL
            # Price₁ = Ask price of X/base (BUY X with base)
            # Price₂ = Ask price of Y/X (BUY Y with X)
            # Price₃ = Bid price of Y/base (SELL Y for base)
            price1, price2, price3 = prices[0], prices[1], prices[2]
            gross_final_amount = (1.0 / price1) * (1.0 / price2) * price3
        elif pat == ("BUY", "SELL", "SELL"):
            # Pattern 2: BUY-SELL-SELL
            # Price₁ = Ask price of X/base (BUY X with base)
            # Price₂ = Bid price of X/Y (SELL X for Y) - note: might be inverted
            # Price₃ = Bid price of Y/base (SELL Y for base)
            price1, price2, price3 = prices[0], prices[1], prices[2]
            gross_final_amount = (1.0 / price1) * price2 * price3
        else:
            return None
        
        # Calculate gross profit percentage
        gross_profit_pct = (gross_final_amount - 1.0) * 100.0
        
        # Validate profit is reasonable (between -100% and 1000%)
        if gross_profit_pct < -100 or gross_profit_pct > 1000:
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
        if random.random() < 0.01:
            logger.debug(f"Route filtered by gross: {base}->{x}->{y}->{base} gross={gross_profit_pct:.4f}% (net={net_profit_pct:.4f}%)")
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


def find_candidate_routes(
    *, 
    min_profit_pct: float, 
    max_profit_pct: float,
    available_balance: float = None
) -> Tuple[List[CandidateRoute], dict]:
    """
    Find candidate triangular arbitrage routes.
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
    }
    
    try:
        client = _client()
        symbols = _load_symbols(client)
        stats["symbols_loaded"] = len(symbols)
        
        base = S.BASE_ASSET.upper()
        allowed_quotes = getattr(S, "ALLOWED_QUOTES", ["USDT", "BTC", "ETH", "BNB", "FDUSD", "USDC"])
        if base not in allowed_quotes:
            allowed_quotes.append(base)
        max_assets = getattr(S, "MAX_ASSETS", 150)
        logger.info(
            f"Loaded {len(symbols)} symbols, base={base}, profit range: {min_profit_pct}% - {max_profit_pct}%, "
            f"quotes={allowed_quotes}, max_assets={max_assets}"
        )
        
        # Dynamic universe across multiple quotes
        universe = _top_assets_multi_quote(client, symbols, quotes=allowed_quotes, max_assets=max_assets)
        
        # Add curated small/mid caps
        curated = [
            "EDEN", "ONT", "BB", "CYBER", "SHELL", "OPEN", "XPL", "BANANA",
            "SHIB", "PEPE", "WIF", "ORDI", "INJ", "FTM", "RUNE",
        ]
        universe = list(dict.fromkeys(universe + curated))
        
        # Fallback if dynamic fails
        if not universe:
            universe = [
                "BTC", "ETH", "BNB", "SOL", "ADA", "XRP", "DOGE", "MATIC",
                "AVAX", "DOT", "LINK", "UNI", "ATOM", "LTC", "ETC", "XLM",
                "ALGO", "VET", "FIL", "TRX", "EOS", "AAVE", "SXP", "CHZ",
                "BICO", "LISTA", "APT", "ARB", "OP", "SUI", "SEI", "TIA",
                "SHIB", "PEPE", "WIF", "ORDI", "INJ", "FTM", "RUNE",
                "EDEN", "ONT", "BB", "CYBER", "SHELL", "OPEN", "XPL", "BANANA",
            ]
        
        universe = [asset for asset in universe if asset != base][:max_assets]
        logger.info(f"Using {len(universe)} assets: {universe[:15]}...")
        
        # Step 1: Collect symbols needed for triangles
        # For triangle base -> X -> Y -> base, we need:
        # 1. Xbase (e.g., BTCUSDT)
        # 2. YX (e.g., ETHBTC)
        # 3. Ybase (e.g., ETHUSDT)
        
        needed_symbols = set()
        
        # Add base pairs for each asset
        for asset in universe:
            needed_symbols.add(f"{asset}{base}")  # Xbase
        
        # Add cross pairs in YX format
        for i, x in enumerate(universe):
            for y in universe[i + 1:]:
                needed_symbols.add(f"{y}{x}")  # YX format (e.g., ETHBTC, not BTCETH)
        
        # Filter to existing symbols
        existing_symbols = [s for s in needed_symbols if s in symbols]
        logger.info(f"Pre-fetching depths for {len(existing_symbols)} unique symbols...")

        # Optional: start websocket bookTicker stream to reduce REST polling (only if dependency available)
        use_ws = getattr(S, "USE_BOOK_TICKER_WS", True) and SpotWebsocketClient is not None
        if use_ws and existing_symbols:
            try:
                stream_url = getattr(S, "BINANCE_WS_URL", None)
                kwargs = {"stream_url": stream_url} if stream_url else {}
                _book_ticker_stream.start(existing_symbols, **kwargs)
                logger.info(f"Started bookTicker websocket for {len(existing_symbols)} symbols")
            except Exception as e:
                logger.warning(f"Failed to start bookTicker websocket, falling back to REST: {e}")
                use_ws = False
        elif getattr(S, "USE_BOOK_TICKER_WS", True) and SpotWebsocketClient is None:
            logger.warning("USE_BOOK_TICKER_WS is True but SpotWebsocketClient is not installed; using REST depth only.")
        
        # Step 2: Fetch depths in parallel with rate limiting (reduced workers to avoid bans)
        start_fetch = time.time()
        # Limit symbols to fetch to avoid rate limits (max 200 symbols)
        max_symbols_to_fetch = getattr(S, "MAX_DEPTH_FETCH", 200)
        symbols_to_fetch = existing_symbols[:max_symbols_to_fetch]
        if len(existing_symbols) > max_symbols_to_fetch:
            logger.warning(f"Limiting depth fetch to {max_symbols_to_fetch} symbols to avoid rate limits (requested {len(existing_symbols)})")
        depth_cache = _fetch_depth_parallel(client, symbols_to_fetch, max_workers=5)  # Reduced to 5 workers
        fetch_time = time.time() - start_fetch
        fetched_count = sum(1 for v in depth_cache.values() if v is not None)
        stats["symbols_fetched"] = fetched_count
        stats["fetch_time"] = fetch_time
        logger.info(f"Fetched {fetched_count}/{len(symbols_to_fetch)} depths in {fetch_time:.2f}s")
        
        # Step 3: Check triangles in BOTH directions
        cand: List[CandidateRoute] = []
        checked = 0
        start_triangles = time.time()
        route_keys = set()  # Track unique routes
        
        # Define testnet tokens that can't pair directly
        testnet_tokens = {"0G", "PNUT", "WAL", "S", "ASTER", "LUNA", "EUR"}
        
        # Check all unique asset pairs (both directions)
        for i in range(len(universe)):
            for j in range(len(universe)):
                if i == j:
                    continue  # Skip same asset
                checked += 1
                
                x = universe[i]
                y = universe[j]
                
                # SKIP IMPOSSIBLE: Both are testnet tokens (no direct pair exists)
                if x in testnet_tokens and y in testnet_tokens:
                    stats["filtered_missing_pairs"] += 1
                    continue
                
                # Check if required pairs could exist
                symbol1 = f"{x}{base}"  # Xbase (e.g., BTCUSDT)
                symbol3 = f"{y}{base}"  # Ybase (e.g., ETHUSDT)
                
                if not (symbol1 in symbols and symbol3 in symbols):
                    stats["filtered_missing_pairs"] += 1
                    continue
                
                # Check intermediate pair in either direction
                symbol2_yx = f"{y}{x}"  # YX (e.g., ETHBTC)
                symbol2_xy = f"{x}{y}"   # XY (e.g., BTCETH)
                
                if not (symbol2_yx in symbols or symbol2_xy in symbols):
                    stats["filtered_missing_pairs"] += 1
                    continue
                
                # Try the triangle
                r = _try_triangle(
                    client, symbols, base, x, y,
                    min_profit_pct, max_profit_pct,
                    depth_cache=depth_cache,
                    available_balance=available_balance,
                    stats=stats
                )
                
                if r:
                    # Check for duplicates
                    route_key = (r.a, r.b, r.c)
                    if route_key not in route_keys:
                        route_keys.add(route_key)
                        logger.debug(f"Found route: {r.a} → {r.b} → {r.c}, profit: {r.profit_pct}%, volume: ${r.volume_usd}")
                        cand.append(r)
                        stats["routes_found"] += 1
                        # Early exit if we found enough routes
                        if len(cand) >= 5:
                            break
            if len(cand) >= 5:
                break
        
        triangle_time = time.time() - start_triangles
        stats["triangles_checked"] = checked
        stats["triangle_time"] = triangle_time
        
        logger.info(f"Checked {checked} triangles in {triangle_time:.2f}s")
        logger.info(f"Found {len(cand)} routes")
        logger.info(f"Filter stats: missing_pairs={stats['filtered_missing_pairs']}, profit={stats['filtered_profit']}, volume={stats['filtered_volume']}")
        
        if stats.get("sample_profits"):
            profits = stats["sample_profits"]
            if profits:
                logger.info(f"Profit sample (n={len(profits)}): min={min(profits):.6f}%, max={max(profits):.6f}%, avg={sum(profits)/len(profits):.6f}%")
        
        # Sort by profit desc and return top 5
        cand.sort(key=lambda z: z.profit_pct, reverse=True)
        return cand[:5], stats
        
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