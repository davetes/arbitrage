#!/usr/bin/env python
"""Quick script to check and enable scanning"""
import os
import sys
import django
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "arbbot.settings")
django.setup()

from apps.core.models import BotSettings
from arbbot import settings as S

cfg, created = BotSettings.objects.get_or_create(id=1)
print(f"Current scanning_enabled: {cfg.scanning_enabled}")
print(f"Created new: {created}")

# Sync settings from .env
updated = False
if not cfg.scanning_enabled:
    cfg.scanning_enabled = True
    updated = True
    print("✅ Enabled scanning!")

if cfg.min_profit_pct != S.MIN_PROFIT_PCT:
    cfg.min_profit_pct = S.MIN_PROFIT_PCT
    updated = True
    print(f"✅ Updated min_profit_pct to {S.MIN_PROFIT_PCT}")

if cfg.max_profit_pct != S.MAX_PROFIT_PCT:
    cfg.max_profit_pct = S.MAX_PROFIT_PCT
    updated = True
    print(f"✅ Updated max_profit_pct to {S.MAX_PROFIT_PCT}")

if cfg.base_asset != S.BASE_ASSET:
    cfg.base_asset = S.BASE_ASSET
    updated = True
    print(f"✅ Updated base_asset to {S.BASE_ASSET}")

if cfg.fee_bps != S.FEE_RATE_BPS:
    cfg.fee_bps = S.FEE_RATE_BPS
    updated = True

if cfg.extra_fee_bps != S.EXTRA_FEE_BPS:
    cfg.extra_fee_bps = S.EXTRA_FEE_BPS
    updated = True

if cfg.min_notional_usd != S.MIN_NOTIONAL_USD:
    cfg.min_notional_usd = S.MIN_NOTIONAL_USD
    updated = True

if cfg.max_notional_usd != S.MAX_NOTIONAL_USD:
    cfg.max_notional_usd = S.MAX_NOTIONAL_USD
    updated = True

if not cfg.bot_language or cfg.bot_language != S.BOT_LANGUAGE:
    cfg.bot_language = S.BOT_LANGUAGE
    updated = True
    print(f"✅ Updated bot_language to {S.BOT_LANGUAGE}")

if updated:
    cfg.save()
    print("\n✅ Settings synced from .env!")
else:
    print("\n✅ All settings already match .env")

print(f"\nCurrent database settings:")
print(f"  scanning_enabled: {cfg.scanning_enabled}")
print(f"  min_profit_pct: {cfg.min_profit_pct}")
print(f"  max_profit_pct: {cfg.max_profit_pct}")
print(f"  base_asset: {cfg.base_asset}")
print(f"  min_notional_usd: {cfg.min_notional_usd}")
print(f"  max_notional_usd: {cfg.max_notional_usd}")
print(f"  use_entire_balance: {cfg.use_entire_balance}")
print(f"  bot_language: {cfg.bot_language}")


