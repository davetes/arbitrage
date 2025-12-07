import os
from celery import shared_task
from django.utils import timezone
from django.db import transaction
from arbbot import settings as S
import json
import requests
import logging
from .models import BotSettings, Route
from .arbitrage import find_candidate_routes
from .trading import get_account_balance

logger = logging.getLogger(__name__)


def _t(key: str, lang: str = None) -> str:
    """Translation helper for tasks (matches bot.py t() function)"""
    if lang is None:
        # Try to get from database
        try:
            cfg = BotSettings.objects.filter(id=1).first()
            if cfg and cfg.bot_language:
                lang = cfg.bot_language
            else:
                lang = S.BOT_LANGUAGE
        except Exception:
            lang = S.BOT_LANGUAGE
    
    ru = {
        "check": "Проверить актуальность",
        "exec": "Исполнить сделку",
    }
    en = {
        "check": "Check Validity",
        "exec": "Execute Trade",
    }
    return ru.get(key) if lang and lang.lower().startswith("ru") else en.get(key)


@shared_task
def scan_triangular_routes():
    try:
        cfg, _ = BotSettings.objects.get_or_create(id=1, defaults={
            "scanning_enabled": True,
            "min_profit_pct": S.MIN_PROFIT_PCT,
            "max_profit_pct": S.MAX_PROFIT_PCT,
            "fee_bps": S.FEE_RATE_BPS,
            "extra_fee_bps": S.EXTRA_FEE_BPS,
            "slippage_bps": S.SLIPPAGE_BPS,
            "min_notional_usd": S.MIN_NOTIONAL_USD,
            "max_notional_usd": S.MAX_NOTIONAL_USD,
            "base_asset": S.BASE_ASSET,
            "bot_language": S.BOT_LANGUAGE,
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

        candidates, stats = find_candidate_routes(
            min_profit_pct=cfg.min_profit_pct,
            max_profit_pct=cfg.max_profit_pct,
            available_balance=available_balance,
        )
        
        logger.info(f"Found {len(candidates)} candidate routes")
        
        created = 0
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
        with transaction.atomic():
            for c in candidates:
                r = Route.objects.create(
                    leg_a=c.a,
                    leg_b=c.b,
                    leg_c=c.c,
                    profit_pct=c.profit_pct,
                    volume_usd=c.volume_usd,
                )
                created += 1
                logger.info(f"Created route {r.id}: {r.label()} (profit: {r.profit_pct:.4f}%, volume: ${r.volume_usd:,.2f})")
                # Notify Telegram immediately with inline buttons
                try:
                    kb = {
                        "inline_keyboard": [
                            [{"text": _t("check", lang), "callback_data": f"check:{r.id}"}],
                            [{"text": _t("exec", lang), "callback_data": f"exec:{r.id}"}],
                        ]
                    }
                    text = f"Route: {r.leg_a} → {r.leg_b} → {r.leg_c}\nProfit: {r.profit_pct:.2f}%\nVolume: ${r.volume_usd:,.0f}"
                    url = f"https://api.telegram.org/bot{S.TELEGRAM_BOT_TOKEN}/sendMessage"
                    payload = {
                        "chat_id": S.ADMIN_TELEGRAM_ID,
                        "text": text,
                        "reply_markup": json.dumps(kb),
                        "disable_web_page_preview": True,
                    }
                    if S.TELEGRAM_BOT_TOKEN and S.ADMIN_TELEGRAM_ID:
                        response = requests.post(url, data=payload, timeout=10)
                        if response.status_code == 200:
                            logger.info(f"Telegram notification sent for route {r.id}")
                        else:
                            logger.warning(f"Telegram notification failed for route {r.id}: {response.status_code} - {response.text}")
                except Exception as e:
                    logger.error(f"Failed to send Telegram notification for route {r.id}: {e}", exc_info=True)
        
        # Send summary message to Telegram
        try:
            if S.TELEGRAM_BOT_TOKEN and S.ADMIN_TELEGRAM_ID:
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
                
                # Add diagnostic information if no routes found
                if created == 0:
                    summary_text += f"\n\n{_get_translation('no_routes_found')}"
                    filtered_profit = stats.get('filtered_profit', 0)
                    filtered_usd = stats.get('filtered_usd_conversion', 0)
                    filtered_volume = stats.get('filtered_volume', 0)
                    filtered_missing = stats.get('filtered_missing_pairs', 0)
                    
                    reasons = []
                    if filtered_profit > 0:
                        reasons.append(f"Profit threshold: {filtered_profit}")
                    if filtered_usd > 0:
                        reasons.append(f"USD conversion: {filtered_usd}")
                    if filtered_volume > 0:
                        reasons.append(f"Volume too small: {filtered_volume}")
                    if filtered_missing > 0:
                        reasons.append(f"Missing pairs: {filtered_missing}")
                    
                    if reasons:
                        summary_text += f"\n\n🔍 Filtered out:\n" + "\n".join(f"  • {r}" for r in reasons)
                    
                    # Show sample profit statistics
                    sample_profits = stats.get('sample_profits', [])
                    if sample_profits:
                        sample_profits.sort()
                        min_prof = min(sample_profits)
                        max_prof = max(sample_profits)
                        avg_prof = sum(sample_profits) / len(sample_profits)
                        summary_text += f"\n\n📊 Sample profits: {min_prof:.4f}% to {max_prof:.4f}% (avg: {avg_prof:.4f}%)"
                else:
                    summary_text += f"\n\n{_get_translation('routes_saved').format(count=created)}"
                
                url = f"https://api.telegram.org/bot{S.TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": S.ADMIN_TELEGRAM_ID,
                    "text": summary_text,
                    "disable_web_page_preview": True,
                }
                response = requests.post(url, data=payload, timeout=10)
                if response.status_code == 200:
                    logger.info("Summary message sent to Telegram")
                else:
                    logger.warning(f"Failed to send summary message: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Failed to send summary message: {e}", exc_info=True)
        
        result_msg = f"routes_created={created} at {timezone.now()}"
        logger.info(result_msg)
        return result_msg
    except Exception as e:
        error_msg = f"error: {e}"
        logger.error(f"Error in scan_triangular_routes: {e}", exc_info=True)
        return error_msg
