import math
from typing import Dict, Tuple, List, Any

from binance.spot import Spot as BinanceClient

from arbbot import settings as S
from .arbitrage import _parse_leg, CandidateRoute, _load_symbols


def _client_auth() -> BinanceClient:
    if not (S.BINANCE_API_KEY and S.BINANCE_API_SECRET):
        raise RuntimeError("Binance API keys are required for execution")
    return BinanceClient(api_key=S.BINANCE_API_KEY, api_secret=S.BINANCE_API_SECRET)


def get_account_balance(asset: str = None) -> Dict[str, float]:
    """
    Get account balances. If asset is specified, returns balance for that asset only.
    Returns dict of {asset: free_balance}
    """
    client = _client_auth()
    account = client.account()
    balances = {}
    for bal in account.get("balances", []):
        free = float(bal.get("free", 0))
        if free > 0:
            balances[bal["asset"]] = free
    if asset:
        return {asset: balances.get(asset.upper(), 0.0)}
    return balances


def _symbol_filters(symbol_info: dict) -> Tuple[float, float]:
    price_filter = next((f for f in symbol_info.get("filters", []) if f["filterType"] == "PRICE_FILTER"), None)
    lot_filter = next((f for f in symbol_info.get("filters", []) if f["filterType"] == "LOT_SIZE"), None)
    notional_filter = next((f for f in symbol_info.get("filters", []) if f["filterType"] == "NOTIONAL"), None)

    step_size = float(lot_filter["stepSize"]) if lot_filter else 0.000001
    min_notional = float(notional_filter["minNotional"]) if notional_filter else 0.0
    return step_size, min_notional


def _round_step(qty: float, step: float) -> float:
    if step == 0:
        return qty
    precision = max(0, int(round(-math.log10(step))))
    factor = 10 ** precision
    return math.floor(qty * factor) / factor


def execute_cycle(route: CandidateRoute, notional_usd: float) -> Tuple[float, List[Dict[str, Any]]]:
    """
    Executes the three-leg cycle using market orders.
    Returns (final_base_amount, orders).
    """
    if not S.TRADING_ENABLED:
        raise RuntimeError("Trading disabled. Set TRADING_ENABLED=true to allow live orders.")

    client = _client_auth()
    symbols = _load_symbols(client)

    # Safety factor to avoid hitting exact balance limits on subsequent legs.
    # For example, 0.98 means we always use 98% of the theoretical amount
    # when placing orders, leaving a small buffer for fees and rounding.
    safety_factor = getattr(S, "EXEC_SAFETY_FACTOR", 0.98)

    orders: List[Dict[str, Any]] = []
    current_asset = S.BASE_ASSET.upper()
    current_amount = notional_usd

    for leg in [route.a, route.b, route.c]:
        base, quote, side = _parse_leg(leg)
        symbol = f"{base}{quote}"
        info = symbols.get(symbol)
        if not info:
            raise RuntimeError(f"Symbol {symbol} not tradable")
        step_size, min_notional = _symbol_filters(info)

        # Determine input/output assets
        if side == "buy":
            # Spend quote to receive base
            if current_asset != quote:
                raise RuntimeError(f"Asset mismatch for leg {leg}. Have {current_asset}, need {quote}")
            effective_amount = current_amount * safety_factor
            if effective_amount < min_notional:
                raise RuntimeError(f"Amount {effective_amount} below min notional {min_notional} for {symbol}")
            # Round quoteOrderQty to 8 decimal places to avoid precision errors
            rounded_quote_qty = round(effective_amount, 8)
            order = client.new_order(symbol=symbol, side="BUY", type="MARKET", quoteOrderQty=rounded_quote_qty)
            qty_out = sum(float(f["qty"]) for f in order.get("fills", [])) if order.get("fills") else float(order.get("executedQty", 0))
            current_asset = base
            current_amount = qty_out
        else:
            # Sell base to receive quote
            if current_asset != base:
                raise RuntimeError(f"Asset mismatch for leg {leg}. Have {current_asset}, need {base}")
            sell_qty = _round_step(current_amount * safety_factor, step_size)
            if sell_qty <= 0:
                raise RuntimeError(f"Sell quantity too small for {symbol}")
            order = client.new_order(symbol=symbol, side="SELL", type="MARKET", quantity=sell_qty)
            quote_got = float(order.get("cummulativeQuoteQty", 0))
            current_asset = quote
            current_amount = quote_got

        orders.append(order)

    return current_amount, orders


