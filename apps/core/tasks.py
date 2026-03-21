import os
from celery import shared_task
from django.utils import timezone
from arbbot import settings as S
import json
import requests
import logging
import redis
from .models import BotSettings, Route, Execution
from .arbitrage import find_candidate_routes, _parse_leg, _client, CandidateRoute, revalidate_route
from .symbol_loader import get_symbol_status
from .trading import execute_cycle, get_account_balance

logger = logging.getLogger(__name__)

_SYMBOL_ALERT_KEY = "arbbot:symbols:last_alert_ts"


def _get_redis():
    url = getattr(S, "REDIS_URL", "")
    if not url:
        return None
    try:
        return redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _should_alert_symbols(status_ts: float) -> bool:
    client = _get_redis()
    if not client:
        return True
    try:
        last_ts_raw = client.get(_SYMBOL_ALERT_KEY)
        last_ts = float(last_ts_raw) if last_ts_raw else 0.0
        if last_ts >= status_ts:
            return False
        client.set(_SYMBOL_ALERT_KEY, str(status_ts), ex=3600)
        return True
    except Exception:
        return True


def _t(key: str, lang: str = None) -> str:
    """Translation helper for tasks (matches bot.py t() function)"""
    if lang is None:
        lang = "en"

    en = {
        "check": "Check Validity",
        "exec": "Execute Trade",
        "prices": "Live Prices",
    }
    return en.get(key)


def _format_price_value(price: float) -> str:
    if price >= 1:
        return f"{price:.2f}"
    if price >= 0.01:
        return f"{price:.4f}"
    return f"{price:.8f}"


def _route_price_lines(legs):
    client = _client(timeout=10)
    lines = []
    for leg in legs:
        try:
            base, quote, side = _parse_leg(leg)
            symbol = f"{base}{quote}"
            depth = client.depth(symbol, limit=5)
            if not depth or not depth.get("asks") or not depth.get("bids"):
                lines.append(f"{base}/{quote}: n/a {side}")
                continue
            ask_price = float(depth["asks"][0][0])
            bid_price = float(depth["bids"][0][0])
            price = ask_price if side == "buy" else bid_price
            lines.append(f"{base}/{quote}: {_format_price_value(price)} {side}")
        except Exception:
            lines.append(f"{leg}: n/a")
    return lines




def _send_telegram_message(text: str, reply_markup: dict = None) -> None:
    if not S.TELEGRAM_BOT_TOKEN or not S.ADMIN_TELEGRAM_ID:
        logger.warning("Telegram not configured: missing TELEGRAM_BOT_TOKEN or ADMIN_TELEGRAM_ID")
        return
    payload = {
        "chat_id": S.ADMIN_TELEGRAM_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    url = f"https://api.telegram.org/bot{S.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data=payload, timeout=10)
        if not resp.ok:
            logger.error(
                "Telegram sendMessage failed: status=%s body=%s",
                resp.status_code,
                resp.text[:200],
            )
    except Exception as exc:
        logger.error("Telegram sendMessage error: %s", exc, exc_info=True)


@shared_task
def scan_triangular_routes():
    try:
        cfg, _ = BotSettings.objects.get_or_create(id=1, defaults={
            "scanning_enabled": True,
            "auto_trade_enabled": False,
            "min_profit_pct": S.MIN_PROFIT_PCT,
            "max_profit_pct": S.MAX_PROFIT_PCT,
            "fee_bps": S.FEE_RATE_BPS,
            "extra_fee_bps": S.EXTRA_FEE_BPS,
            "slippage_bps": S.SLIPPAGE_BPS,
            "min_notional_usd": S.MIN_NOTIONAL_USD,
            "max_notional_usd": S.MAX_NOTIONAL_USD,
            "base_asset": S.BASE_ASSET,
        })
        if not cfg.scanning_enabled:
            logger.info("Scanning is disabled, skipping route search")
            return "disabled"

        logger.info(f"Starting route scan: profit_range={cfg.min_profit_pct}%-{cfg.max_profit_pct}%, "
                   f"notional_range=${cfg.min_notional_usd}-${cfg.max_notional_usd}")

        # Get available balance if use_entire_balance is enabled
        available_balance = None
        if cfg.use_entire_balance:
            try:
                balances = get_account_balance(cfg.base_asset.upper())
                available_balance = balances.get(cfg.base_asset.upper(), 0.0)
                if available_balance > 0:
                    # Use 95% of balance to leave some buffer
                    available_balance = available_balance * 0.95
                    logger.info(f"Using entire balance mode: ${available_balance:,.2f} available")
            except Exception as e:
                logger.warning(f"Balance check failed: {e}, falling back to normal behavior")
                available_balance = None

        created = 0
        lang = "en"

        def _handle_candidate(c: CandidateRoute):
            nonlocal created
            r = Route.objects.create(
                leg_a=c.a,
                leg_b=c.b,
                leg_c=c.c,
                profit_pct=c.profit_pct,
                volume_usd=c.volume_usd,
            )
            created += 1
            logger.info(
                f"Created route {r.id}: {r.label()} (profit: {r.profit_pct:.4f}%, volume: ${r.volume_usd:,.2f})"
            )
            try:
                kb = {
                    "inline_keyboard": [
                        [{"text": _t("prices", lang), "callback_data": f"prices:{r.id}"}],
                        [{"text": _t("check", lang), "callback_data": f"check:{r.id}"}],
                        [{"text": _t("exec", lang), "callback_data": f"exec:{r.id}"}],
                    ]
                }
                text = (
                    f"Route: {r.leg_a} → {r.leg_b} → {r.leg_c}\n"
                    f"Profit: {r.profit_pct:.2f}%\n"
                    f"Volume: ${r.volume_usd:,.0f}\n"
                    f"Auto-trade: {'enabled' if cfg.auto_trade_enabled else 'disabled'}"
                )
                _send_telegram_message(text, reply_markup=kb)
                logger.info(f"Telegram notification sent for route {r.id}")
            except Exception as e:
                logger.error(f"Failed to send Telegram notification for route {r.id}: {e}", exc_info=True)

            if cfg.auto_trade_enabled:
                auto_trade_route.delay(r.id)
        
        candidates, stats = find_candidate_routes(
            min_profit_pct=cfg.min_profit_pct,
            max_profit_pct=cfg.max_profit_pct,
            available_balance=available_balance,
            on_route_found=_handle_candidate,
        )

        symbols_status = get_symbol_status()
        if symbols_status and not symbols_status.get("ok", True):
            status_ts = float(symbols_status.get("ts", 0.0))
            if _should_alert_symbols(status_ts) and S.TELEGRAM_BOT_TOKEN and S.ADMIN_TELEGRAM_ID:
                try:
                    text = "Symbols API failed"
                    url = f"https://api.telegram.org/bot{S.TELEGRAM_BOT_TOKEN}/sendMessage"
                    payload = {
                        "chat_id": S.ADMIN_TELEGRAM_ID,
                        "text": text,
                        "disable_web_page_preview": True,
                    }
                    requests.post(url, data=payload, timeout=10)
                except Exception as e:
                    logger.error(f"Failed to send symbols API alert: {e}", exc_info=True)

        logger.info(f"Found {len(candidates)} candidate routes")

        # Send summary message to Telegram only when at least one route was created
        try:
            if created > 0 and S.TELEGRAM_BOT_TOKEN and S.ADMIN_TELEGRAM_ID:
                # Get translations
                def _get_translation(key: str) -> str:
                    ru_translations = {
                        "scan_summary": "📊 Сводка сканирования",
                        "symbols_loaded": "📈 Символов загружено",
                        "depths_fetched": "🔍 Глубин получено",
                        "triangles_checked": "🔺 Треугольников проверено",
                        "routes_found": "✅ Маршрутов найдено",
                        "routes_created": "💾 Маршрутов создано",
                        "no_routes_found": "⚠️ Прибыльных маршрутов не найдено в этом сканировании.",
                        "routes_saved": "🎉 {count} маршрут(ов) сохранено в базу данных!",
                    }
                    en_translations = {
                        "scan_summary": "📊 Scan Summary",
                        "symbols_loaded": "📈 Symbols loaded",
                        "depths_fetched": "🔍 Depths fetched",
                        "triangles_checked": "🔺 Triangles checked",
                        "routes_found": "✅ Routes found",
                        "routes_created": "💾 Routes created",
                        "no_routes_found": "⚠️ No profitable routes found in this scan.",
                        "routes_saved": "🎉 {count} route(s) saved to database!",
                    }
                    translations = ru_translations if lang and lang.lower().startswith("ru") else en_translations
                    return translations.get(key, key)
                
                summary_text = (
                    f"{_get_translation('scan_summary')}\n\n"
                    f"{_get_translation('symbols_loaded')}: {stats['symbols_loaded']}\n"
                    f"{_get_translation('depths_fetched')}: {stats['symbols_fetched']} ({stats['fetch_time']:.2f}s)\n"
                    f"{_get_translation('triangles_checked')}: {stats['triangles_checked']}\n"
                    f"{_get_translation('routes_found')}: {stats['routes_found']}\n"
                    f"{_get_translation('routes_created')}: {created}"
                )
                
                # Since created > 0, add saved routes line
                summary_text += f"\n\n{_get_translation('routes_saved').format(count=created)}"
                
                _send_telegram_message(summary_text)
                logger.info("Summary message sent to Telegram")
        except Exception as e:
            logger.error(f"Failed to send summary message: {e}", exc_info=True)
        
        result_msg = f"routes_created={created} at {timezone.now()}"
        logger.info(result_msg)
        return result_msg
    except Exception as e:
        error_msg = f"error: {e}"
        logger.error(f"Error in scan_triangular_routes: {e}", exc_info=True)
        return error_msg


@shared_task
def auto_trade_route(route_id: int) -> str:
    cfg = BotSettings.objects.filter(id=1).first()
    if not cfg or not cfg.auto_trade_enabled:
        return "disabled"

    r = Route.objects.filter(id=route_id).first()
    if not r:
        return "missing"

    try:
        cand = CandidateRoute(
            a=r.leg_a,
            b=r.leg_b,
            c=r.leg_c,
            profit_pct=r.profit_pct,
            volume_usd=r.volume_usd,
        )
        new_cand = revalidate_route(cand)
        if not new_cand:
            logger.info(f"Auto-trade skipped (route invalid) for route {r.id}")
            _send_telegram_message(
                f"⚠️ Auto-trade skipped (invalid route)\n"
                f"Route: {r.leg_a} → {r.leg_b} → {r.leg_c}"
            )
            return "invalid"

        balances = get_account_balance(S.BASE_ASSET.upper())
        available = balances.get(S.BASE_ASSET.upper(), 0.0)
        if cfg.use_entire_balance:
            notional = available * 0.95
            notional = min(notional, new_cand.volume_usd, S.MAX_NOTIONAL_USD)
        else:
            notional = min(new_cand.volume_usd, S.MAX_NOTIONAL_USD, available)

        if notional < S.MIN_NOTIONAL_USD:
            logger.info(
                f"Auto-trade skipped (insufficient balance) for route {r.id}: ${notional:.2f}"
            )
            _send_telegram_message(
                f"⚠️ Auto-trade skipped (insufficient balance)\n"
                f"Route: {r.leg_a} → {r.leg_b} → {r.leg_c}\n"
                f"Needed: ${S.MIN_NOTIONAL_USD:,.2f}, available: ${available:,.2f}"
            )
            return "insufficient"

        ex = Execution.objects.create(route=r, status="running", notional_usd=notional)
        _send_telegram_message(
            f"⏳ Auto-trade started\n"
            f"Route: {r.leg_a} → {r.leg_b} → {r.leg_c}\n"
            f"Planned notional: ${notional:,.2f}"
        )
        try:
            final_base, orders = execute_cycle(new_cand, notional)
            pnl = final_base - notional
            ex.status = "completed"
            ex.pnl_usd = pnl
            ex.finished_at = timezone.now()
            ex.details = {"orders": orders, "final_base": final_base}
            _send_telegram_message(
                f"✅ Auto-trade executed\nRoute: {r.leg_a} → {r.leg_b} → {r.leg_c}\n"
                f"Notional: ${notional:,.2f}\nP&L: ${pnl:,.2f}"
            )
        except Exception as e:
            ex.status = "failed"
            ex.details = {"error": str(e)}
            ex.finished_at = timezone.now()
            _send_telegram_message(
                f"❌ Auto-trade failed\nRoute: {r.leg_a} → {r.leg_b} → {r.leg_c}\n"
                f"Error: {e}"
            )
        finally:
            ex.save()
        return "ok"
    except Exception as e:
        logger.error(f"Auto-trade failed for route {route_id}: {e}", exc_info=True)
        return "error"
