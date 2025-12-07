# Troubleshooting: routes_created = 0 Issue

## Problem
When clicking "Start Search", the button toggles to "Stop Search" but:
- No routes are found (`routes_created=0` in Celery worker)
- No Telegram messages with "Check Validity" and "Execute Trade" buttons appear

## Root Causes

### 1. **Profit Thresholds Too High** (Most Likely)
The default profit range is **1.0% - 2.5%**, which is very high for real arbitrage opportunities.

**In efficient markets:**
- Real arbitrage opportunities are usually **0.01% - 0.5%**
- Opportunities above 1% are extremely rare and disappear quickly
- Your current settings filter out 99%+ of real opportunities

**Solution:**
Lower your profit thresholds via Telegram bot:
1. Click `/config`
2. Set "Min Profit %" to **0.01** or **0.1**
3. Set "Max Profit %" to **0.5** or **1.0**

Or set in `.env`:
```env
MIN_PROFIT_PCT=0.01
MAX_PROFIT_PCT=0.5
```

### 2. **Celery Worker Not Running**
The scanning task runs via Celery Beat scheduler. If the worker isn't running, tasks won't execute.

**Check:**
```bash
# Check if Celery worker is running
celery -A arbbot worker --loglevel=info

# Check if Celery Beat is running
celery -A arbbot beat --loglevel=info
```

### 3. **Network/API Issues**
If Binance API is unreachable or rate-limited, route discovery will fail.

**Check Celery worker logs** for errors like:
- Connection timeouts
- Rate limit errors (429)
- API key issues

### 4. **Insufficient Market Depth**
Routes are filtered if market capacity is below `MIN_NOTIONAL_USD` (default $10).

**Solution:**
Lower `MIN_NOTIONAL_USD` if you want to catch smaller opportunities:
```env
MIN_NOTIONAL_USD=1
```

## Debugging Steps

### 1. Check Celery Worker Logs
Look for these log messages (now added):
```
Starting route scan: profit_range=1.0%-2.5%, notional_range=$10-$10000
Loaded X symbols, filtering for profit: 1.0% - 2.5%
Fetched X/Y depths in X.XXs
Checked X triangles in X.XXs
Checked X triangle combinations, found 0 profitable routes
routes_created=0 at ...
```

### 2. Test Route Discovery Manually
Run the test script:
```bash
python test_all_features.py
```

This will show if routes can be found with current settings.

### 3. Check Database Settings
Verify your BotSettings in the database match your expectations:
```python
from apps.core.models import BotSettings
cfg = BotSettings.objects.get(id=1)
print(f"Min profit: {cfg.min_profit_pct}%")
print(f"Max profit: {cfg.max_profit_pct}%")
print(f"Scanning enabled: {cfg.scanning_enabled}")
```

### 4. Lower Profit Thresholds for Testing
To verify the system works, temporarily set very low thresholds:
- Min: 0.001% (0.00001)
- Max: 10% (10.0)

This will catch almost any opportunity and help verify the system is working.

## Expected Behavior After Fix

1. **Celery worker logs** will show:
   ```
   Found X candidate routes
   Created route Y: ... (profit: X.XXXX%, volume: $X.XX)
   Telegram notification sent for route Y
   routes_created=X at ...
   ```

2. **Telegram bot** will send messages like:
   ```
   Route: BTC/USDT buy → ETH/BTC buy → ETH/USDT sell
   Profit: 0.15%
   Volume: $100
   [Check Validity] [Execute Trade]
   ```

## Quick Fix

**Immediate solution:** Lower profit thresholds to realistic values:

Via Telegram:
1. `/config`
2. Set Min Profit: **0.01**
3. Set Max Profit: **0.5**

Or restart with new `.env`:
```env
MIN_PROFIT_PCT=0.01
MAX_PROFIT_PCT=0.5
```

Then restart Celery worker and beat.

## Additional Notes

- **Real arbitrage opportunities are small** - don't expect 1%+ profits regularly
- **Routes appear and disappear quickly** - market efficiency eliminates them fast
- **Network latency matters** - routes may be invalid by the time you execute
- **Fees reduce profits** - ensure your fee settings match your account (default 0.1% = 10 bps)


