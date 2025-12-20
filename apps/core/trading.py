import math
import time
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple, List, Any, Optional

try:
    from binance.spot import Spot as BinanceClient
except Exception:  # optional dependency for non-trading environments/tests
    BinanceClient = None  # type: ignore

from arbbot import settings as S
from .arbitrage import _parse_leg, CandidateRoute, _load_symbols


def _client_auth() -> "BinanceClient":
    if BinanceClient is None:
        raise RuntimeError("binance-connector is not installed; cannot execute live trades")
    if not (S.BINANCE_API_KEY and S.BINANCE_API_SECRET):
        raise RuntimeError("Binance API keys are required for execution")
    return BinanceClient(api_key=S.BINANCE_API_KEY, api_secret=S.BINANCE_API_SECRET)


def get_account_balance(asset: str = None) -> Dict[str, float]:
    """Get account balances.

    If asset is specified, returns balance for that asset only.
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


def _get_filter(symbol_info: dict, filter_type: str) -> Optional[dict]:
    return next((f for f in symbol_info.get("filters", []) if f.get("filterType") == filter_type), None)


def _symbol_filters(symbol_info: dict) -> Tuple[Decimal, Decimal, Decimal]:
    """Return (step_size, min_qty, min_notional) as Decimal."""

    lot_filter = _get_filter(symbol_info, "LOT_SIZE")
    notional_filter = _get_filter(symbol_info, "NOTIONAL") or _get_filter(symbol_info, "MIN_NOTIONAL")

    step_size = Decimal(lot_filter["stepSize"]) if lot_filter else Decimal("0.000001")
    min_qty = Decimal(lot_filter["minQty"]) if lot_filter else Decimal("0")
    min_notional = Decimal(str(notional_filter.get("minNotional", "0"))) if notional_filter else Decimal("0")

    return step_size, min_qty, min_notional


def _floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
    """Floor `qty` down to the nearest multiple of `step`.

    Uses Decimal math to avoid float rounding issues that commonly trigger LOT_SIZE errors.
    """

    if step <= 0:
        return qty
    if qty <= 0:
        return Decimal("0")
    steps = (qty / step).to_integral_value(rounding=ROUND_DOWN)
    return (steps * step).normalize()


def _as_decimal(x: float | Decimal) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def _get_order(client: BinanceClient, symbol: str, order_id: int) -> dict:
    """Compat wrapper for different Binance client method names."""

    if hasattr(client, "get_order"):
        return client.get_order(symbol=symbol, orderId=order_id)
    if hasattr(client, "query_order"):
        return client.query_order(symbol=symbol, orderId=order_id)
    raise RuntimeError("Binance client does not support get_order/query_order; cannot poll for FILLED status")


def _wait_for_filled(client: BinanceClient, symbol: str, order_id: int) -> dict:
    """Poll order status until FILLED (or fail/timeout)."""

    timeout_s = float(getattr(S, "EXEC_ORDER_TIMEOUT_SECONDS", 30))
    poll_s = float(getattr(S, "EXEC_ORDER_POLL_INTERVAL_SECONDS", 0.25))

    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        last = _get_order(client, symbol=symbol, order_id=order_id)
        status = (last or {}).get("status")

        if status == "FILLED":
            return last
        if status in {"CANCELED", "REJECTED", "EXPIRED"}:
            raise RuntimeError(f"Order {order_id} on {symbol} failed with status={status}: {last}")

        time.sleep(poll_s)

    raise RuntimeError(f"Timeout waiting for order {order_id} on {symbol} to fill. Last={last}")


def _filled_base_qty(order: dict) -> float:
    """Base-asset quantity filled (executedQty or sum of fills qty)."""

    if "executedQty" in order:
        try:
            return float(order.get("executedQty") or 0)
        except Exception:
            pass

    fills = order.get("fills") or []
    if fills:
        return float(sum(Decimal(str(f.get("qty", 0))) for f in fills))

    return 0.0


def _filled_quote_qty(order: dict) -> float:
    """Quote-asset amount filled (cummulativeQuoteQty or derived from fills)."""

    if "cummulativeQuoteQty" in order:
        try:
            return float(order.get("cummulativeQuoteQty") or 0)
        except Exception:
            pass

    fills = order.get("fills") or []
    if fills:
        total = Decimal("0")
        for f in fills:
            qty = Decimal(str(f.get("qty", 0)))
            price = Decimal(str(f.get("price", 0)))
            total += qty * price
        return float(total)

    # last resort: executedQty * avgPrice
    try:
        executed = Decimal(str(order.get("executedQty", 0) or 0))
        avg_price = Decimal(str(order.get("avgPrice", 0) or 0))
        return float(executed * avg_price)
    except Exception:
        return 0.0


def execute_cycle(route: CandidateRoute, notional_usd: float) -> Tuple[float, List[Dict[str, Any]]]:
    """Execute the 3-leg cycle using market orders.

    Logic enforced:
      buy coin1 -> wait until FULLY filled
      buy coin2 -> wait until FULLY filled
      sell/buy coin3 -> wait until FULLY filled

    Returns (final_base_amount, orders).
    """

    if not S.TRADING_ENABLED:
        raise RuntimeError("Trading disabled. Set TRADING_ENABLED=true to allow live orders.")

    client = _client_auth()
    symbols = _load_symbols(client)

    safety_factor = Decimal(str(getattr(S, "EXEC_SAFETY_FACTOR", 0.98)))

    orders: List[Dict[str, Any]] = []
    current_asset = S.BASE_ASSET.upper()
    current_amount = Decimal(str(notional_usd))

    for leg in [route.a, route.b, route.c]:
        base, quote, side = _parse_leg(leg)
        symbol = f"{base}{quote}"
        info = symbols.get(symbol)
        if not info:
            raise RuntimeError(f"Symbol {symbol} not tradable")

        step_size, min_qty, min_notional = _symbol_filters(info)

        if side == "buy":
            # Spend quote to receive base
            if current_asset != quote:
                raise RuntimeError(f"Asset mismatch for leg {leg}. Have {current_asset}, need {quote}")

            effective_quote = (current_amount * safety_factor).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
            if effective_quote < min_notional:
                raise RuntimeError(f"Amount {effective_quote} below min notional {min_notional} for {symbol}")

            order = client.new_order(
                symbol=symbol,
                side="BUY",
                type="MARKET",
                quoteOrderQty=float(effective_quote),
            )

            # Ensure FULL fill before continuing
            order_id = int(order.get("orderId"))
            order = _wait_for_filled(client, symbol=symbol, order_id=order_id)

            qty_out = Decimal(str(_filled_base_qty(order)))

            # Use actual free balance (some exchanges report executedQty, but fees/rounding can affect free)
            actual_balance = Decimal(str(get_account_balance(base).get(base, 0.0)))
            current_asset = base
            current_amount = min(qty_out, actual_balance)

        else:
            # Sell base to receive quote
            if current_asset != base:
                raise RuntimeError(f"Asset mismatch for leg {leg}. Have {current_asset}, need {base}")

            actual_balance = Decimal(str(get_account_balance(base).get(base, 0.0)))
            available_qty = min(current_amount, actual_balance)

            sell_qty = _floor_to_step(available_qty * safety_factor, step_size)
            if min_qty > 0 and sell_qty < min_qty:
                # Try to sell the max available floored to step (still must be >= minQty)
                sell_qty = _floor_to_step(available_qty, step_size)

            if sell_qty <= 0 or (min_qty > 0 and sell_qty < min_qty):
                raise RuntimeError(
                    f"Insufficient balance for {symbol}: have {actual_balance} {base}, "
                    f"available={available_qty}, step={step_size}, minQty={min_qty}"
                )

            order = client.new_order(
                symbol=symbol,
                side="SELL",
                type="MARKET",
                quantity=float(sell_qty),
            )

            order_id = int(order.get("orderId"))
            order = _wait_for_filled(client, symbol=symbol, order_id=order_id)

            quote_got = Decimal(str(_filled_quote_qty(order)))
            current_asset = quote
            current_amount = quote_got

        orders.append(order)

    return float(current_amount), orders
