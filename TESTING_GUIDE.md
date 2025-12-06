# Testing Guide for Arbitrage Bot

## Prerequisites

1. **Environment Setup:**
   ```powershell
   # Activate virtual environment
   .venv\Scripts\activate
   
   # Ensure all services are running:
   # - PostgreSQL
   # - Redis
   # - Django server (optional, for admin)
   ```

2. **Required Services Running:**
   - Redis: `redis-server` (or Windows service)
   - PostgreSQL: Running and accessible
   - Celery Worker: `python -m celery -A arbbot worker -l info -P solo -c 1`
   - Celery Beat: `python -m celery -A arbbot beat -l info`
   - Telegram Bot: `python apps/telegram_bot/bot.py`

3. **Configuration Check:**
   ```powershell
   python check_scanning.py
   ```
   Should show `scanning_enabled: True` and correct settings.

---

## Test 1: Route Detection & Scanning

### Step 1: Enable Scanning
```powershell
python check_scanning.py
```
Verify: `scanning_enabled: True`

### Step 2: Lower Profit Thresholds (for testing)
Edit `.env`:
```
MIN_PROFIT_PCT=0.01
MAX_PROFIT_PCT=5
MIN_NOTIONAL_USD=1
```

### Step 3: Sync Settings
```powershell
python check_scanning.py
```

### Step 4: Monitor Worker Logs
Watch the Celery worker terminal. You should see:
```
Task apps.core.tasks.scan_triangular_routes[...] succeeded in Xs: 'routes_created=X at ...'
```

**Expected Result:** 
- Tasks complete successfully
- If `routes_created=0`, no profitable routes found (normal in efficient markets)
- If `routes_created>0`, routes found and Telegram notifications sent

---

## Test 2: Telegram Notifications

### Step 1: Start Telegram Bot
```powershell
python apps/telegram_bot/bot.py
```

### Step 2: Send /start Command
In Telegram, send `/start` to your bot.

**Expected Result:**
- Bot responds with welcome message
- Shows "Stop Search" or "Start Search" button

### Step 3: Check for Route Notifications
If routes are found, you should receive messages like:
```
Route: BTC/USDT buy → ETH/BTC buy → ETH/USDT sell
Profit: 1.23%
Volume: $1,000

[Check Validity] [Execute Trade]
```

**Expected Result:**
- Message received with route details
- Two buttons visible: "Check Validity" and "Execute Trade"

---

## Test 3: Check Route Validity Button

### Step 1: Find a Route
Wait for a route notification in Telegram, or check database:
```powershell
python manage.py shell -c "from apps.core.models import Route; r=Route.objects.order_by('-id').first(); print(f'Route {r.id}: {r.label()}')"
```

### Step 2: Click "Check Validity" Button
Click the "Check Validity" button on any route notification.

**Expected Result:**
- Bot responds with "Revalidated" or "Not valid now"
- If valid: Message updates with current profit % and volume
- If invalid: Shows alert "Not valid now"

---

## Test 4: Execute Trade Button (DRY RUN - No Real Trading)

### ⚠️ IMPORTANT: Safety First
Before testing execution:
1. Set `TRADING_ENABLED=false` in `.env` (default)
2. Or use testnet API keys if available

### Step 1: Verify Trading is Disabled
```powershell
python manage.py shell -c "from arbbot import settings; print(f'TRADING_ENABLED: {settings.TRADING_ENABLED}')"
```
Should show: `TRADING_ENABLED: False`

### Step 2: Click "Execute Trade" Button
Click "Execute Trade" on a route notification.

**Expected Result:**
- Shows confirmation dialog:
  ```
  Confirm execution:
  Route: BTC/USDT buy → ETH/BTC buy → ETH/USDT sell
  Profit: 1.23%
  Available balance: $1.10
  Planned notional: $1.00
  [Confirm] [Cancel]
  ```

### Step 3: Try to Confirm (Should Fail Safely)
Click "Confirm" button.

**Expected Result:**
- Shows alert: "Trading disabled (set TRADING_ENABLED=true)"
- No trades executed (safe!)

### Step 4: Test with Trading Enabled (ONLY IF YOU WANT REAL TRADES)
⚠️ **WARNING: This will execute real trades!**

1. Set in `.env`: `TRADING_ENABLED=true`
2. Ensure you have sufficient balance
3. Click "Execute Trade" → "Confirm"
4. Bot executes 3-leg cycle
5. Shows P&L result

---

## Test 5: Account Balance Checking

### Step 1: Check Balance Function
```powershell
python manage.py shell -c "from apps.core.trading import get_account_balance; from arbbot import settings; bal = get_account_balance(settings.BASE_ASSET); print(f'{settings.BASE_ASSET} balance: {bal}')"
```

**Expected Result:**
- Returns your actual account balance
- Example: `{'USDT': 1.1}`

### Step 2: Test Balance Display in Confirmation
When clicking "Execute Trade", the confirmation should show:
```
Available balance: $X.XX
```

**Expected Result:**
- Shows your actual balance
- Planned notional ≤ available balance

---

## Test 6: Settings & Configuration

### Step 1: Test Settings Sync
```powershell
python check_scanning.py
```

**Expected Result:**
- Shows current database settings
- Syncs from `.env` if different
- Updates `scanning_enabled` if needed

### Step 2: Test Start/Stop Search Toggle
In Telegram:
1. Click "Start Search" or "Stop Search"
2. Button text changes
3. Check worker logs - should see `'disabled'` when stopped

**Expected Result:**
- Button toggles correctly
- Scanning stops/starts as expected

---

## Test 7: Manual Route Testing

### Test Route Finding Directly
```powershell
python manage.py shell -c "from apps.core.arbitrage import find_candidate_routes; routes = find_candidate_routes(min_profit_pct=0.01, max_profit_pct=5); print(f'Found {len(routes)} routes'); [print(f'{r.a} -> {r.b} -> {r.c}: {r.profit_pct:.4f}%, ${r.volume_usd:.2f}') for r in routes[:3]]"
```

**Expected Result:**
- Returns list of routes (may be empty if none found)
- Shows profit % and volume for each

---

## Test 8: Network Resilience

### Test Retry Logic
The bot should handle network issues automatically:
- If Binance API times out, it retries 3 times with backoff
- Check worker logs for retry attempts

**Expected Result:**
- Network errors don't crash the bot
- Retries happen automatically
- Eventually succeeds or logs error

---

## Test 9: Russian Language Support

### Step 1: Set Language
In `.env`: `BOT_LANGUAGE=ru`

### Step 2: Restart Bot
Restart Telegram bot to pick up new language.

### Step 3: Send /start
**Expected Result:**
- All messages in Russian
- Buttons in Russian

---

## Test 10: Database Verification

### Check Routes in Database
```powershell
python manage.py shell -c "from apps.core.models import Route; routes = Route.objects.order_by('-id')[:5]; print(f'Total routes: {Route.objects.count()}'); [print(f'{r.id}: {r.label()} - {r.profit_pct}%') for r in routes]"
```

### Check Executions
```powershell
python manage.py shell -c "from apps.core.models import Execution; execs = Execution.objects.order_by('-id')[:5]; print(f'Total executions: {Execution.objects.count()}'); [print(f'{e.id}: {e.status} - P&L: ${e.pnl_usd:.2f}') for e in execs]"
```

---

## Troubleshooting

### No Routes Found
- **Cause:** Market is efficient, no arbitrage opportunities
- **Solution:** Lower `MIN_PROFIT_PCT` to 0.01 or 0.001 for testing
- **Note:** This is normal - real opportunities are rare

### Telegram Notifications Not Received
- Check `TELEGRAM_BOT_TOKEN` and `ADMIN_TELEGRAM_ID` in `.env`
- Verify bot is running: `python apps/telegram_bot/bot.py`
- Check bot logs for errors

### Worker Shows 'disabled'
- Run: `python check_scanning.py`
- Ensure `scanning_enabled: True`

### Balance Check Fails
- Verify Binance API keys have "Read" permission
- Check network connectivity
- Ensure API keys are correct in `.env`

---

## Quick Test Script

Run this to test everything at once:
```powershell
python test_all_features.py
```
