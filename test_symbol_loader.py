#!/usr/bin/env python
"""Test script for FastSymbolLoader optimization"""
import os
import sys
import django
from pathlib import Path
import time

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "arbbot.settings")
django.setup()

from apps.core.symbol_loader import get_symbol_loader
from apps.core.arbitrage import _client, find_candidate_routes
from apps.core.models import BotSettings

print("=" * 60)
print("Testing FastSymbolLoader Optimization")
print("=" * 60)

# Test 1: Symbol Loading
print("\n1. Testing Symbol Loading...")
loader = get_symbol_loader(cache_ttl_seconds=300)
client = _client()

start = time.time()
symbols = loader.load_symbols(client, base_asset="USDT", use_cache=False)
first_load_time = time.time() - start

if len(symbols) <= 32:
    print(f"   First load: {len(symbols)} symbols in {first_load_time:.2f}s (using fallback - API timed out)")
    print("   Note: If API keeps timing out, consider setting PROXY_URL in .env")
else:
    print(f"   First load: {len(symbols)} symbols in {first_load_time:.2f}s")

# Test 2: Cached Load
start = time.time()
symbols_cached = loader.load_symbols(client, base_asset="USDT", use_cache=True)
cached_load_time = time.time() - start

print(f"   Cached load: {len(symbols_cached)} symbols in {cached_load_time:.3f}s")
if cached_load_time > 0:
    print(f"   Speed improvement: {first_load_time/cached_load_time:.1f}x faster")
else:
    print(f"   Speed improvement: Instant (cached)")

# Test 3: Route Finding
print("\n2. Testing Route Finding...")
try:
    cfg, _ = BotSettings.objects.get_or_create(id=1)
    min_profit = cfg.min_profit_pct if cfg.min_profit_pct else 0.01
    max_profit = cfg.max_profit_pct if cfg.max_profit_pct else 5.0
    min_notional = cfg.min_notional_usd
    max_notional = cfg.max_notional_usd
except Exception as e:
    print(f"   ⚠️  Database connection failed: {e}")
    print("   Using default settings from .env")
    from arbbot import settings as S
    min_profit = S.MIN_PROFIT_PCT
    max_profit = S.MAX_PROFIT_PCT
    min_notional = S.MIN_NOTIONAL_USD
    max_notional = S.MAX_NOTIONAL_USD

print(f"   Profit range: {min_profit}% - {max_profit}%")
print(f"   Min notional: ${min_notional}")
print(f"   Max notional: ${max_notional}")

try:
    start = time.time()
    routes = find_candidate_routes(min_profit_pct=min_profit, max_profit_pct=max_profit)
    route_time = time.time() - start

    print(f"   Found {len(routes)} routes in {route_time:.2f}s")
    if routes:
        print("\n   Top routes:")
        for i, r in enumerate(routes[:3], 1):
            print(f"   {i}. {r.a} → {r.b} → {r.c}")
            print(f"      Profit: {r.profit_pct:.4f}%, Volume: ${r.volume_usd:,.2f}")
    else:
        print("   No profitable routes found (this is normal in efficient markets)")
except Exception as e:
    print(f"   ⚠️  Route finding failed: {e}")
    print("   This may be due to network issues or insufficient symbols")

print("\n" + "=" * 60)
print("Test Summary")
print("=" * 60)
if len(symbols) <= 32:
    print("⚠️  Using fallback symbols (API connection failed)")
    print("   - Bot will work with 32 major pairs only")
    print("   - Consider setting PROXY_URL in .env for better connectivity")
    print("   - Or check network/firewall settings")
else:
    print("✅ Symbol loading working correctly")
    print(f"   - Loaded {len(symbols)} symbols")
    print(f"   - Cache working: {cached_load_time:.3f}s for cached load")

if 'routes' in locals() and len(routes) > 0:
    print("✅ Route finding working correctly")
    print(f"   - Found {len(routes)} profitable routes")
else:
    print("ℹ️  No routes found (normal in efficient markets)")
    print("   - Try lowering MIN_PROFIT_PCT for testing")

print("=" * 60)
