import os
from celery import shared_task
from django.utils import timezone
from django.db import transaction
from arbbot import settings as S
import json
import requests
from .models import BotSettings, Route
from .arbitrage import find_candidate_routes


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
            return "disabled"

        candidates = find_candidate_routes(
            min_profit_pct=cfg.min_profit_pct,
            max_profit_pct=cfg.max_profit_pct,
        )
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
                        requests.post(url, data=payload, timeout=10)
                except Exception:
                    pass
        return f"routes_created={created} at {timezone.now()}"
    except Exception as e:
        return f"error: {e}"
