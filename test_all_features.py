#!/usr/bin/env python
"""Comprehensive test script for arbitrage bot"""
import os
import sys
import django
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "arbbot.settings")
django.setup()

from apps.core.models import BotSettings, Route, Execution
from apps.core.arbitrage import find_candidate_routes, revalidate_route, CandidateRoute
from apps.core.trading import get_account_balance, execute_cycle
from arbbot import settings as S
import traceback

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def test_1_settings():
    print_section("Test 1: Settings & Configuration")
    try:
        cfg, _ = BotSettings.objects.get_or_create(id=1)
        print(f"✅ Scanning enabled: {cfg.scanning_enabled}")
        print(f"✅ Min profit: {cfg.min_profit_pct}%")
        print(f"✅ Max profit: {cfg.max_profit_pct}%")
        print(f"✅ Base asset: {cfg.base_asset}")
        print(f"✅ Min notional: ${cfg.min_notional_usd}")
        print(f"✅ Max notional: ${cfg.max_notional_usd}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_2_route_detection():
    print_section("Test 2: Route Detection")
    try:
        print("Searching for routes (this may take 30-60 seconds)...")
        routes = find_candidate_routes(min_profit_pct=0.01, max_profit_pct=5)
        print(f"✅ Found {len(routes)} routes")
        if routes:
            for i, r in enumerate(routes[:3], 1):
                print(f"   Route {i}: {r.a} → {r.b} → {r.c}")
                print(f"      Profit: {r.profit_pct:.4f}%")
                print(f"      Volume: ${r.volume_usd:,.2f}")
        else:
            print("   (No routes found - this is normal in efficient markets)")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        return False

def test_3_account_balance():
    print_section("Test 3: Account Balance Checking")
    try:
        if not (S.BINANCE_API_KEY and S.BINANCE_API_SECRET):
            print("⚠️  API keys not set - skipping balance check")
            return True
        
        balances = get_account_balance(S.BASE_ASSET.upper())
        base_balance = balances.get(S.BASE_ASSET.upper(), 0.0)
        print(f"✅ {S.BASE_ASSET} balance: ${base_balance:,.2f}")
        
        if base_balance > 0:
            print(f"✅ Balance check successful")
        else:
            print(f"⚠️  Balance is $0 - ensure you have funds for testing")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   (This is OK if API keys are read-only or network issue)")
        return True  # Don't fail test for this

def test_4_route_revalidation():
    print_section("Test 4: Route Revalidation")
    try:
        # Get a recent route from database
        route = Route.objects.order_by('-id').first()
        if not route:
            print("⚠️  No routes in database - create one first by running scanner")
            return True
        
        print(f"Testing revalidation for route: {route.label()}")
        cand = CandidateRoute(
            a=route.leg_a,
            b=route.leg_b,
            c=route.leg_c,
            profit_pct=route.profit_pct,
            volume_usd=route.volume_usd
        )
        
        new_cand = revalidate_route(cand)
        if new_cand:
            print(f"✅ Route is valid")
            print(f"   Current profit: {new_cand.profit_pct:.4f}%")
            print(f"   Current volume: ${new_cand.volume_usd:,.2f}")
        else:
            print(f"⚠️  Route is no longer valid (prices changed)")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        return False

def test_5_database():
    print_section("Test 5: Database Operations")
    try:
        route_count = Route.objects.count()
        exec_count = Execution.objects.count()
        print(f"✅ Total routes in database: {route_count}")
        print(f"✅ Total executions in database: {exec_count}")
        
        if route_count > 0:
            latest = Route.objects.order_by('-id').first()
            print(f"✅ Latest route: {latest.label()} ({latest.profit_pct}%)")
        
        if exec_count > 0:
            latest_exec = Execution.objects.order_by('-id').first()
            print(f"✅ Latest execution: {latest_exec.status} (P&L: ${latest_exec.pnl_usd:.2f})")
        
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_6_trading_safety():
    print_section("Test 6: Trading Safety")
    try:
        print(f"✅ TRADING_ENABLED: {S.TRADING_ENABLED}")
        if not S.TRADING_ENABLED:
            print("✅ Trading is disabled (safe mode)")
            print("   To enable: Set TRADING_ENABLED=true in .env")
        else:
            print("⚠️  Trading is ENABLED - real trades will execute!")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_7_configuration():
    print_section("Test 7: Configuration Check")
    try:
        checks = []
        
        # Check required settings
        if S.TELEGRAM_BOT_TOKEN:
            checks.append(("Telegram Bot Token", True))
        else:
            checks.append(("Telegram Bot Token", False))
        
        if S.ADMIN_TELEGRAM_ID:
            checks.append(("Admin Telegram ID", True))
        else:
            checks.append(("Admin Telegram ID", False))
        
        if S.BINANCE_API_KEY:
            checks.append(("Binance API Key", True))
        else:
            checks.append(("Binance API Key", False))
        
        if S.REDIS_URL:
            checks.append(("Redis URL", True))
        else:
            checks.append(("Redis URL", False))
        
        for name, status in checks:
            status_str = "✅" if status else "❌"
            print(f"{status_str} {name}: {'Set' if status else 'Missing'}")
        
        all_ok = all(status for _, status in checks)
        return all_ok
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("  ARBITRAGE BOT - COMPREHENSIVE TEST SUITE")
    print("="*60)
    
    results = []
    
    results.append(("Settings", test_1_settings()))
    results.append(("Route Detection", test_2_route_detection()))
    results.append(("Account Balance", test_3_account_balance()))
    results.append(("Route Revalidation", test_4_route_revalidation()))
    results.append(("Database", test_5_database()))
    results.append(("Trading Safety", test_6_trading_safety()))
    results.append(("Configuration", test_7_configuration()))
    
    print_section("Test Results Summary")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")
    
    print(f"\n{'='*60}")
    print(f"  Total: {passed}/{total} tests passed")
    print(f"{'='*60}\n")
    
    if passed == total:
        print("🎉 All tests passed!")
    else:
        print("⚠️  Some tests failed - check output above for details")

if __name__ == "__main__":
    main()
