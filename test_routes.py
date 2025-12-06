#!/usr/bin/env python
"""Debug script to test route finding"""
import os
import sys
import django
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "arbbot.settings")
django.setup()

from apps.core.arbitrage import _client, _load_symbols, _try_triangle
from arbbot import settings as S

print(f"MIN_PROFIT_PCT: {S.MIN_PROFIT_PCT}")
print(f"MAX_PROFIT_PCT: {S.MAX_PROFIT_PCT}")
print(f"MIN_NOTIONAL_USD: {S.MIN_NOTIONAL_USD}")
print(f"MAX_NOTIONAL_USD: {S.MAX_NOTIONAL_USD}")
print(f"BASE_ASSET: {S.BASE_ASSET}")
print()

client = _client()
symbols = _load_symbols(client)
base = S.BASE_ASSET.upper()

# Test a few specific pairs
test_pairs = [
    ("BTC", "ETH"),
    ("BNB", "SOL"),
    ("ADA", "XRP"),
]

print("Testing specific pairs (bypassing MIN_NOTIONAL_USD check):")
for x, y in test_pairs:
    try:
        # Temporarily override MIN_NOTIONAL_USD
        original_min = S.MIN_NOTIONAL_USD
        S.MIN_NOTIONAL_USD = 0.01  # Very low for testing
        r = _try_triangle(client, symbols, base, x, y)
        S.MIN_NOTIONAL_USD = original_min
        if r:
            print(f"✅ {x}/{y}: {r.profit_pct:.4f}% profit, ${r.volume_usd:.2f} volume")
            print(f"   Route: {r.a} -> {r.b} -> {r.c}")
        else:
            print(f"❌ {x}/{y}: No route found")
    except Exception as e:
        print(f"❌ {x}/{y}: Error - {e}")

print("\nTesting with current settings:")
from apps.core.arbitrage import find_candidate_routes
routes = find_candidate_routes(min_profit_pct=0.001, max_profit_pct=5)
print(f"Found {len(routes)} routes")
for r in routes[:5]:
    print(f"  {r.a} -> {r.b} -> {r.c}: {r.profit_pct:.4f}%, ${r.volume_usd:.2f}")


