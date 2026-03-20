import asyncio
import os
import sys
import django
from pathlib import Path
import logging
import httpx
import json
import redis

BASE_DIR = Path(__file__).resolve().parents[2]
# Ensure project root is importable so 'arbbot' package can be found
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "arbbot.settings")
django.setup()

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from aiogram.exceptions import TelegramBadRequest
from django.utils import timezone
from arbbot import settings as S
from arbbot.celery import app as celery_app
from apps.core.models import BotSettings, Route, Execution
from apps.core.arbitrage import CandidateRoute, revalidate_route, _parse_leg, _client, _depth_snapshot, ensure_ws_symbols, get_ws_stats
from apps.core.trading import execute_cycle, get_account_balance
from asgiref.sync import sync_to_async
from apps.core.tasks import scan_triangular_routes


def t(key: str, lang: str = None) -> str:
    """Translation function. Always returns English."""
    # Force English regardless of input
    lang = "en"
    en = {
        "ready": "Arbitrage bot ready. Use buttons to control scanning.",
        "config": "Bot Settings",
        "config_menu": "⚙️ Bot Settings\n\nSelect parameter to change:",
        "tri": "You will receive triangular arbitrage route messages here when found.",
        "direct": "Direct arbitrage alerts are not implemented yet.",
        "history": "Execution history will be available soon.",
        "toggle": "Scanning toggled",
        "route_missing": "Route no longer exists",
        "not_valid": "Not valid now",
        "revalidated": "Revalidated",
        "trade_disabled": "Trading disabled (set TRADING_ENABLED=true)",
        "exec_started": "Execution pending confirmation",
        "exec_done": "Cycle executed",
        "exec_failed": "Execution failed",
        "confirm_title": "Confirm execution:",
        "confirm_btn": "Confirm",
        "cancel_btn": "Cancel",
        "cancelled": "Cancelled",
        "start_search": "Start Search",
        "stop_search": "Stop Search",
        "check": "Check Validity",
        "exec": "Execute Trade",
        "prices": "Live Prices",
        "settings_saved": "Settings saved",
        "min_profit": "Min Profit %",
        "max_profit": "Max Profit %",
        "min_notional": "Min Notional $",
        "max_notional": "Max Notional $",
        "back": "Back",
        "current_value": "Current value",
        "enter_new_value": "Enter new value",
        "language": "Language",
        "scanning": "Scanning",
        "base_asset": "Base Asset",
        "select_language": "Select language:",
        "select_preset": "Select a preset value:",
        "enabled": "Enabled",
        "disabled": "Disabled",
        "toggle_scanning": "Toggle Scanning",
        "auto_trade": "Auto Trade",
        "toggle_auto_trade": "Toggle Auto Trade",
        "use_entire_balance": "Use Entire Balance",
        "entire_balance": "Entire Balance",
        "fixed_amount": "Fixed Amount",
        "scan_started": "Scanning started",
        "scan_summary": "Scan Summary",
        "symbols_loaded": "Symbols loaded",
        "depths_fetched": "Depths fetched",
        "triangles_checked": "Triangles checked",
        "routes_found": "Routes found",
        "routes_created": "Routes created",
        "no_routes_found": "No profitable routes found in this scan",
        "routes_saved": "route(s) saved to database",
    }
    return en.get(key, key)


async def get_binance_status():
    base_url = S.BINANCE_BASE_URL.rstrip("/")
    url = f"{base_url}/api/v3/ping"
    try:
        async with httpx.AsyncClient(timeout=2.5) as client:
            resp = await client.get(url)
        if resp.status_code == 200:
            return True, "OK"
        return False, f"HTTP {resp.status_code}"
    except Exception as exc:
        return False, f"Error: {exc.__class__.__name__}"


def get_symbols_api_status():
    url = getattr(S, "REDIS_URL", "")
    if not url:
        return None
    try:
        client = redis.from_url(url, decode_responses=True)
        raw = client.get("arbbot:symbols:status")
        if not raw:
            return None
        data = json.loads(raw)
        ok = bool(data.get("ok", False))
        message = data.get("message", "")
        return ok, message
    except Exception:
        return None


async def _fetch_external_status():
    binance_task = asyncio.create_task(get_binance_status())
    symbols_task = asyncio.create_task(sync_to_async(get_symbols_api_status, thread_sensitive=True)())
    binance_ok, binance_msg = await binance_task
    symbols_status = await symbols_task
    return binance_ok, binance_msg, symbols_status


def _format_price_value(price: float) -> str:
    if price >= 1:
        return f"{price:.2f}"
    if price >= 0.01:
        return f"{price:.4f}"
    return f"{price:.8f}"


def _format_balance_amount(amount: float) -> str:
    if amount >= 1:
        return f"{amount:,.6f}".rstrip("0").rstrip(".")
    return f"{amount:.8f}".rstrip("0").rstrip(".")


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


def _get_celery_status():
    broker_url = getattr(S, "CELERY_BROKER_URL", "") or getattr(S, "REDIS_URL", "")
    redis_ok = None
    redis_error = None
    inspect_error = None

    if broker_url:
        try:
            client = redis.from_url(
                broker_url,
                decode_responses=True,
                socket_connect_timeout=1.5,
                socket_timeout=1.5,
            )
            redis_ok = bool(client.ping())
        except Exception as exc:
            redis_ok = False
            redis_error = str(exc)

    workers = []
    active_count = 0
    scheduled_count = 0

    try:
        replies = celery_app.control.ping(timeout=2.0) or []
        for item in replies:
            if isinstance(item, dict):
                workers.extend(item.keys())
        workers = sorted(set(workers))
    except Exception as exc:
        inspect_error = str(exc)

    if workers:
        try:
            insp = celery_app.control.inspect(timeout=2.0)
            active = insp.active() or {}
            scheduled = insp.scheduled() or {}
            active_count = sum(len(v or []) for v in active.values())
            scheduled_count = sum(len(v or []) for v in scheduled.values())
        except Exception as exc:
            inspect_error = str(exc)

    return {
        "broker_url": broker_url,
        "redis_ok": redis_ok,
        "redis_error": redis_error,
        "workers": workers,
        "active_count": active_count,
        "scheduled_count": scheduled_count,
        "inspect_error": inspect_error,
    }


async def kb_global():
    cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
    lang = "en"
    if cfg.scanning_enabled:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t("stop_search", lang), callback_data="toggle_scan")]])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t("start_search", lang), callback_data="toggle_scan")]])


def kb_route(route_id: int, lang: str = None):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("prices", lang), callback_data=f"prices:{route_id}")],
        [InlineKeyboardButton(text=t("check", lang), callback_data=f"check:{route_id}")],
        [InlineKeyboardButton(text=t("exec", lang), callback_data=f"exec:{route_id}")],
    ])


def kb_confirm(route_id: int, lang: str = None):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("confirm_btn", lang), callback_data=f"execok:{route_id}"),
            InlineKeyboardButton(text=t("cancel_btn", lang), callback_data=f"execcancel:{route_id}"),
        ]
    ])


def kb_settings_menu(lang: str = None):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("min_profit", lang), callback_data="config:min_profit")],
        [InlineKeyboardButton(text=t("max_profit", lang), callback_data="config:max_profit")],
        [InlineKeyboardButton(text=t("min_notional", lang), callback_data="config:min_notional")],
        [InlineKeyboardButton(text=t("max_notional", lang), callback_data="config:max_notional")],
        [InlineKeyboardButton(text="💰 " + t("use_entire_balance", lang), callback_data="config:use_entire_balance")],
        [InlineKeyboardButton(text="🤖 " + t("auto_trade", lang), callback_data="config:auto_trade")],
        # Language toggle removed
        [InlineKeyboardButton(text="🔄 " + t("toggle_scanning", lang), callback_data="config:toggle_scan")],
        [InlineKeyboardButton(text="🔙 " + t("back", lang), callback_data="config:back")],
    ])


def kb_setting_presets(setting: str, current_val: float, unit: str, lang: str = None):
    """Create preset buttons for a setting"""
    if "%" in unit:
        # Profit percentage presets
        presets = [-2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 5.0, 10.0]
    else:
        # Notional USD presets
        presets = [1, 10, 50, 100, 500, 1000, 5000, 10000]
    
    buttons = []
    row = []
    for preset in presets:
        if abs(preset - current_val) < 0.01:
            label = f"✓ {preset}{unit}"
        else:
            label = f"{preset}{unit}"
        row.append(InlineKeyboardButton(
            text=label,
            callback_data=f"set:{setting}:{preset}"
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="🔙 " + t("back", lang), callback_data="config:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# kb_language_presets removed


async def main():
    logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    # Increase verbosity for aiogram and HTTP stacks
    logging.getLogger("aiogram").setLevel(logging.DEBUG)
    logging.getLogger("aiohttp.client").setLevel(logging.DEBUG)
    logging.getLogger("aiohttp.client.pool").setLevel(logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.DEBUG)
    logging.info("Starting Telegram bot...")
    bot = Bot(token=S.TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()

    # Set bot menu commands
    commands = [
        BotCommand(command="start", description="Start the arbitrage bot"),
        BotCommand(command="config", description="Configure your bot settings"),
        BotCommand(command="autotrade", description="Toggle auto trade"),
        BotCommand(command="balance", description="Show base asset balance"),
        BotCommand(command="balances", description="Show all balances"),
        BotCommand(command="celery_status", description="Check Celery worker/beat"),
        BotCommand(command="top_routes", description="Show top 10 routes"),
        BotCommand(command="triangular_alerts", description="Get triangular arbitrage alerts"),
        BotCommand(command="direct_alerts", description="Get direct arbitrage alerts"),
        BotCommand(command="transaction_history", description="Get transaction history"),
    ]
    await bot.set_my_commands(commands)

    @dp.message(CommandStart())
    async def on_start(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        reply_kb = await kb_global()
        logging.info(f"/start from chat_id={msg.chat.id} username={getattr(msg.from_user, 'username', '')}")
        
        # Add status info
        status_emoji = "✅" if cfg.scanning_enabled else "❌"
        status_text = t("enabled", lang) if cfg.scanning_enabled else t("disabled", lang)
        welcome_text = (
            f"{t('ready', lang)}\n\n"
            f"{status_emoji} {t('scanning', lang)}: {status_text}\n"
            f"⏳ Checking Binance and Symbols..."
        )

        sent = await msg.answer(welcome_text, reply_markup=reply_kb)

        async def update_start_message():
            try:
                binance_ok, binance_msg, symbols_status = await _fetch_external_status()
                binance_emoji = "✅" if binance_ok else "❌"
                symbols_line = ""
                if symbols_status:
                    symbols_ok, symbols_msg = symbols_status
                    symbols_emoji = "✅" if symbols_ok else "❌"
                    symbols_line = f"\n{symbols_emoji} Symbols API: {symbols_msg}"
                api_status = "Connected" if binance_ok else "Disconnected"
                ws_stats = get_ws_stats()
                ws_status = "Connected" if ws_stats.get("running") else "Disconnected"
                ws_details = f"{ws_stats.get('cached', 0)}/{ws_stats.get('subscribed', 0)}"
                updated_text = (
                    f"{t('ready', lang)}\n\n"
                    f"{status_emoji} {t('scanning', lang)}: {status_text}\n"
                    f"{binance_emoji} Binance: {binance_msg}\n"
                    f"🔌 API: {api_status}"
                    f"\n📡 WS: {ws_status} ({ws_details})"
                    f"{symbols_line}"
                )
                await sent.edit_text(updated_text, reply_markup=reply_kb)
            except TelegramBadRequest:
                pass
            except Exception as exc:
                logging.error(f"/start status update failed: {exc}", exc_info=True)

        asyncio.create_task(update_start_message())

    @dp.message(F.text == "/config")
    async def on_config(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        # lang_display removed
        scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
        balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
        auto_trade_status = f"✅ {t('enabled', lang)}" if cfg.auto_trade_enabled else f"❌ {t('disabled', lang)}"
        text = (
            f"{t('config_menu', lang)}\n\n"
            f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
            f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
            f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
            f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
            f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
            f"🤖 {t('auto_trade', lang)}: {auto_trade_status}\n"
            # Language display line removed
            f"🔄 {t('scanning', lang)}: {scanning_status}\n"
            f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
        )
        await msg.answer(text, reply_markup=kb_settings_menu(lang))

    @dp.message(F.text == "/autotrade")
    async def on_autotrade(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        cfg.auto_trade_enabled = not cfg.auto_trade_enabled
        await sync_to_async(cfg.save)()
        status_text = t('enabled', lang) if cfg.auto_trade_enabled else t('disabled', lang)
        status_emoji = "✅" if cfg.auto_trade_enabled else "❌"
        await msg.answer(f"{status_emoji} {t('auto_trade', lang)}: {status_text}")

    @dp.message(F.text == "/balance")
    async def on_balance(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        base_asset = (cfg.base_asset or S.BASE_ASSET or "").upper()
        if not base_asset:
            await msg.answer("Base asset is not configured.")
            return
        try:
            balances = await asyncio.wait_for(
                sync_to_async(get_account_balance)(base_asset),
                timeout=float(getattr(S, "BINANCE_TIMEOUT_SECONDS", 10)),
            )
            amount = balances.get(base_asset, 0.0)
            await msg.answer(f"Balance {base_asset}: {_format_balance_amount(amount)}")
        except asyncio.TimeoutError:
            await msg.answer("Balance check timed out. Please try again.")
        except Exception as exc:
            await msg.answer(f"Balance check failed: {exc}")

    @dp.message(F.text == "/balances")
    async def on_balances(msg: Message):
        try:
            balances = await asyncio.wait_for(
                sync_to_async(get_account_balance)(),
                timeout=float(getattr(S, "BINANCE_TIMEOUT_SECONDS", 10)),
            )
        except asyncio.TimeoutError:
            await msg.answer("Balance check timed out. Please try again.")
            return
        except Exception as exc:
            await msg.answer(f"Balance check failed: {exc}")
            return

        if not balances:
            await msg.answer("No balances found.")
            return

        rows = sorted(balances.items(), key=lambda item: item[1], reverse=True)
        lines = ["Balances"]
        for asset, amount in rows[:15]:
            lines.append(f"{asset}: {_format_balance_amount(amount)}")
        if len(rows) > 15:
            lines.append(f"... and {len(rows) - 15} more")
        await msg.answer("\n".join(lines))

    @dp.message(F.text == "/celery_status")
    async def on_celery_status(msg: Message):
        sent = await msg.answer("Checking Celery status...")
        try:
            status = await asyncio.wait_for(
                sync_to_async(_get_celery_status)(),
                timeout=6.0,
            )
        except asyncio.TimeoutError:
            await sent.edit_text("Celery status check timed out.")
            return

        broker_url = status.get("broker_url") or "(not set)"
        redis_ok = status.get("redis_ok")
        redis_error = status.get("redis_error")
        workers = status.get("workers", [])
        active_count = status.get("active_count", 0)
        scheduled_count = status.get("scheduled_count", 0)

        lines = ["Celery Status", f"Broker: {broker_url}"]

        if redis_ok is True:
            lines.append("Redis: OK")
        elif redis_ok is False:
            lines.append(f"Redis: ERROR ({redis_error})")

        if status.get("inspect_error"):
            lines.append(f"Inspect error: {status['inspect_error']}")

        if workers:
            lines.extend([
                f"Workers online: {len(workers)}",
                f"Active tasks: {active_count}",
                f"Scheduled tasks: {scheduled_count}",
                "Beat: not directly detectable; scheduled tasks > 0 suggests beat is running",
            ])
        else:
            lines.append("Workers online: 0")

        await sent.edit_text("\n".join(lines))

    @dp.message(F.text == "/triangular_alerts")
    async def on_tri_alerts(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        await msg.answer(t("tri", lang))

    @dp.message(F.text == "/direct_alerts")
    async def on_direct_alerts(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        await msg.answer(t("direct", lang))

    @dp.message(F.text == "/transaction_history")
    async def on_tx_history(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        await msg.answer(t("history", lang))

    @dp.message(F.text == "/top_routes")
    async def on_top_routes(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        routes = await sync_to_async(lambda: list(Route.objects.order_by('-profit_pct')[:10]))()
        if not routes:
            await msg.answer("No routes found yet.")
            return

        lines = ["🏆 Top 10 Routes (by profit)"]
        for idx, r in enumerate(routes, start=1):
            lines.append(
                f"{idx}. {r.leg_a} → {r.leg_b} → {r.leg_c} | "
                f"Profit: {r.profit_pct:.2f}% | Volume: ${r.volume_usd:,.0f}"
            )
        await msg.answer("\n".join(lines))

    @dp.message(F.text == "/status")
    async def on_status(msg: Message):
        """Check bot status and configuration"""
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        status_emoji = "✅" if cfg.scanning_enabled else "❌"
        base_text = (
            f"🤖 Bot Status\n\n"
            f"{status_emoji} {t('scanning', lang)}: {t('enabled', lang) if cfg.scanning_enabled else t('disabled', lang)}\n\n"
            f"🤖 {t('auto_trade', lang)}: {t('enabled', lang) if cfg.auto_trade_enabled else t('disabled', lang)}\n\n"
            f"⏳ Checking Binance, Symbols, and Routes...\n"
        )
        sent = await msg.answer(base_text, reply_markup=await kb_global())

        async def update_status_message():
            try:
                binance_ok, binance_msg, symbols_status = await _fetch_external_status()
                binance_emoji = "✅" if binance_ok else "❌"
                symbols_line = ""
                if symbols_status:
                    symbols_ok, symbols_msg = symbols_status
                    symbols_emoji = "✅" if symbols_ok else "❌"
                    symbols_line = f"{symbols_emoji} Symbols API: {symbols_msg}\n\n"

                from apps.core.models import Route
                recent_routes = await sync_to_async(lambda: Route.objects.order_by('-id')[:5].count())()
                total_routes = await sync_to_async(lambda: Route.objects.count())()

                api_status = "Connected" if binance_ok else "Disconnected"
                ws_stats = get_ws_stats()
                ws_status = "Connected" if ws_stats.get("running") else "Disconnected"
                ws_details = f"{ws_stats.get('cached', 0)}/{ws_stats.get('subscribed', 0)}"
                status_text = (
                    f"🤖 Bot Status\n\n"
                    f"{status_emoji} {t('scanning', lang)}: {t('enabled', lang) if cfg.scanning_enabled else t('disabled', lang)}\n\n"
                    f"🤖 {t('auto_trade', lang)}: {t('enabled', lang) if cfg.auto_trade_enabled else t('disabled', lang)}\n\n"
                    f"{binance_emoji} Binance: {binance_msg}\n"
                    f"🔌 API: {api_status}\n\n"
                    f"📡 WS: {ws_status} ({ws_details})\n\n"
                    f"{symbols_line}"
                    f"📊 Settings:\n"
                    f"   Profit: {cfg.min_profit_pct}% - {cfg.max_profit_pct}%\n"
                    f"   Volume: ${cfg.min_notional_usd:,.0f} - ${cfg.max_notional_usd:,.0f}\n"
                    f"   Base: {cfg.base_asset}\n\n"
                    f"📈 Routes:\n"
                    f"   Total found: {total_routes}\n"
                    f"   Recent (last 5): {recent_routes}\n\n"
                )

                if cfg.scanning_enabled:
                    status_text += (
                        f"⚠️  Make sure:\n"
                        f"   1. Celery worker is running\n"
                        f"   2. Celery beat is running\n"
                        f"   3. Redis is running\n"
                    )
                else:
                    status_text += f"💡 Click 'Start Search' to begin scanning"

                await sent.edit_text(status_text, reply_markup=await kb_global())
            except TelegramBadRequest:
                pass
            except Exception as exc:
                logging.error(f"/status update failed: {exc}", exc_info=True)

        asyncio.create_task(update_status_message())


    @dp.callback_query(F.data == "toggle_scan")
    async def toggle_scan(cb: CallbackQuery):
        try:
            cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
            lang = "en"
            old_status = cfg.scanning_enabled
            cfg.scanning_enabled = not cfg.scanning_enabled
            await sync_to_async(cfg.save)()
            
            # Get updated keyboard
            reply_kb = await kb_global()
            
            # Create status message
            status_text = t("enabled", lang) if cfg.scanning_enabled else t("disabled", lang)
            status_emoji = "✅" if cfg.scanning_enabled else "❌"
            message_text = (
                f"{status_emoji} {t('scanning', lang)}: {status_text}\n\n"
            )
            if cfg.scanning_enabled:
                message_text += (
                    f"🔍 {t('scan_started', lang)}\n\n"
                    f"📊 Profit range: {cfg.min_profit_pct}% - {cfg.max_profit_pct}%\n"
                    f"💰 Volume: ${cfg.min_notional_usd:,.0f} - ${cfg.max_notional_usd:,.0f}\n\n"
                    f"📈 You will receive scan summaries after each scan cycle."
                )
            else:
                message_text += "⏸️ Route scanning stopped."
            
            # Always answer callback first (shows popup)
            await cb.answer(f"{t('scanning', lang)} {status_text.lower()}")
            
            # Try to update the button on the original message
            try:
                await cb.message.edit_reply_markup(reply_markup=reply_kb)
            except TelegramBadRequest:
                # If edit fails, that's okay - button might be on a different message
                pass
            
            # Send a new message with status (always visible)
            await cb.message.answer(message_text, reply_markup=reply_kb)
            
            logging.info(f"Scanning toggled from {old_status} to {cfg.scanning_enabled} by chat_id={cb.from_user.id}")
        except Exception as e:
            logging.error(f"Error in toggle_scan: {e}", exc_info=True)
            try:
                await cb.answer(f"Error: {str(e)}", show_alert=True)
            except:
                pass

    @dp.callback_query(F.data.startswith("check:"))
    async def check_route(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer(t("route_missing", lang), show_alert=True)
            return
        
        try:
            cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
            new_cand = revalidate_route(cand)
            if not new_cand:
                # Provide more informative error message
                error_msg = t("not_valid", lang)
                # Try to get more details about why validation failed
                try:
                    from apps.core.arbitrage import _parse_leg
                    a_base, a_quote, _ = _parse_leg(r.leg_a)
                    c_base, c_quote, _ = _parse_leg(r.leg_c)
                    error_msg = f"{error_msg}\nRoute: {a_base}/{a_quote} → {c_base}/{a_base} → {c_base}/{c_quote}\nOriginal profit: {r.profit_pct:.2f}%"
                except Exception:
                    pass
                await cb.answer(error_msg, show_alert=True)
            else:
                logging.info(f"Route {route_id} revalidated: profit={new_cand.profit_pct}% volume=${new_cand.volume_usd}")
                price_lines = await sync_to_async(_route_price_lines)([new_cand.a, new_cand.b, new_cand.c])
                price_block = "\n".join(price_lines)
                text = (
                    f"{price_block}\n\n"
                    f"Route: {new_cand.a} → {new_cand.b} → {new_cand.c}\n"
                    f"Profit: {new_cand.profit_pct:.2f}%\n"
                    f"Volume: ${new_cand.volume_usd:,.0f}"
                )
                await cb.message.edit_text(text, reply_markup=kb_route(route_id, lang))
                await cb.answer(t("revalidated", lang))
        except Exception as e:
            logging.error(f"Error checking route {route_id}: {e}", exc_info=True)
            await cb.answer(f"Error: {str(e)[:100]}", show_alert=True)

    @dp.callback_query(F.data.startswith("prices:"))
    async def show_prices(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer(t("route_missing", lang), show_alert=True)
            return

        try:
            price_lines = await sync_to_async(_route_price_lines)([r.leg_a, r.leg_b, r.leg_c])
            price_block = "\n".join(price_lines)
            text = (
                f"{price_block}\n\n"
                f"Route: {r.leg_a} → {r.leg_b} → {r.leg_c}\n"
                f"Profit: {r.profit_pct:.2f}%\n"
                f"Volume: ${r.volume_usd:,.0f}"
            )
            await cb.message.edit_text(text, reply_markup=kb_route(route_id, lang))
            await cb.answer()
        except Exception as e:
            logging.error(f"Error fetching live prices for route {route_id}: {e}", exc_info=True)
            await cb.answer(f"Error: {str(e)[:100]}", show_alert=True)

    @dp.callback_query(F.data.startswith("exec:"))
    async def exec_route(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer(t("route_missing", lang), show_alert=True)
            return
        # Revalidate before execution
        try:
            cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
            new_cand = revalidate_route(cand)
            if not new_cand:
                # Provide more informative error message
                error_msg = t("not_valid", lang)
                try:
                    from apps.core.arbitrage import _parse_leg
                    a_base, a_quote, _ = _parse_leg(r.leg_a)
                    c_base, c_quote, _ = _parse_leg(r.leg_c)
                    error_msg = f"{error_msg}\nRoute: {a_base}/{a_quote} → {c_base}/{a_base} → {c_base}/{c_quote}\nOriginal profit: {r.profit_pct:.2f}%\nMarket may have moved or route no longer profitable."
                except Exception:
                    pass
                await cb.answer(error_msg, show_alert=True)
                return
        except Exception as e:
            logging.error(f"Error revalidating route {route_id} for execution: {e}", exc_info=True)
            await cb.answer(f"Validation error: {str(e)[:100]}", show_alert=True)
            return
        
        # Calculate executable amount
        try:
            balances = await sync_to_async(get_account_balance)(S.BASE_ASSET.upper())
            available_balance = balances.get(S.BASE_ASSET.upper(), 0.0)
        except Exception:
            available_balance = S.MAX_NOTIONAL_USD  # Fallback if balance check fails
        
        # Use entire balance if enabled, otherwise use normal logic
        if cfg.use_entire_balance:
            # Use 95% of balance to leave buffer
            notional = available_balance * 0.95
            # Still respect route capacity and max notional
            notional = min(notional, new_cand.volume_usd, S.MAX_NOTIONAL_USD)
            balance_mode = t("entire_balance", lang)
        else:
            # Normal logic: min(route_volume, max_notional, account_balance)
            notional = min(new_cand.volume_usd, S.MAX_NOTIONAL_USD, available_balance)
            balance_mode = t("fixed_amount", lang)
        
        price_lines = await sync_to_async(_route_price_lines)([new_cand.a, new_cand.b, new_cand.c])
        price_block = "\n".join(price_lines)
        text = (
            f"{t('confirm_title', lang)}\n"
            f"{price_block}\n\n"
            f"Route: {new_cand.a} → {new_cand.b} → {new_cand.c}\n"
            f"Profit: {new_cand.profit_pct:.2f}%\n"
            f"Available balance: ${available_balance:,.2f}\n"
            f"Mode: {balance_mode}\n"
            f"Planned notional: ${notional:,.2f}"
        )
        await cb.message.edit_text(text, reply_markup=kb_confirm(route_id, lang))
        await cb.answer(t("exec_started", lang))

    @dp.callback_query(F.data.startswith("execcancel:"))
    async def exec_cancel(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        await cb.answer(t("cancelled", lang))
        await cb.message.edit_text(t("cancelled", lang))

    @dp.callback_query(F.data == "config:back")
    async def config_back(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        # lang_display removed
        scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
        balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
        auto_trade_status = f"✅ {t('enabled', lang)}" if cfg.auto_trade_enabled else f"❌ {t('disabled', lang)}"
        text = (
            f"{t('config_menu', lang)}\n\n"
            f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
            f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
            f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
            f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
            f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
            f"🤖 {t('auto_trade', lang)}: {auto_trade_status}\n"
            # Language display line removed
            f"🔄 {t('scanning', lang)}: {scanning_status}\n"
            f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
        )
        await cb.message.edit_text(text, reply_markup=kb_settings_menu(lang))
        await cb.answer()

    @dp.callback_query(F.data.startswith("config:"))
    async def config_setting(cb: CallbackQuery):
        setting = cb.data.split(":")[1]
        if setting == "back":
            return  # Handled separately
        
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        
        if setting == "toggle_scan":
            cfg.scanning_enabled = not cfg.scanning_enabled
            await sync_to_async(cfg.save)()
            scanning_status_text = t('enabled', lang) if cfg.scanning_enabled else t('disabled', lang)
            await cb.answer(f"{t('scanning', lang)} {scanning_status_text}")
            
            # Return to config menu
            # lang_display removed
            scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
            balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
            auto_trade_status = f"✅ {t('enabled', lang)}" if cfg.auto_trade_enabled else f"❌ {t('disabled', lang)}"
            text = (
                f"{t('config_menu', lang)}\n\n"
                f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
                f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
                f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
                f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
                f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
                f"🤖 {t('auto_trade', lang)}: {auto_trade_status}\n"
                # Language display line removed
                f"🔄 {t('scanning', lang)}: {scanning_status}\n"
                f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
            )
            await cb.message.edit_text(text, reply_markup=kb_settings_menu(lang))
            return
        
        if setting == "use_entire_balance":
            cfg.use_entire_balance = not cfg.use_entire_balance
            await sync_to_async(cfg.save)()
            balance_status_text = t('enabled', lang) if cfg.use_entire_balance else t('disabled', lang)
            await cb.answer(f"{t('use_entire_balance', lang)} {balance_status_text}")
            
            # Return to config menu
            # lang_display removed
            scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
            balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
            auto_trade_status = f"✅ {t('enabled', lang)}" if cfg.auto_trade_enabled else f"❌ {t('disabled', lang)}"
            text = (
                f"{t('config_menu', lang)}\n\n"
                f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
                f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
                f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
                f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
                f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
                f"🤖 {t('auto_trade', lang)}: {auto_trade_status}\n"
                # Language display line removed
                f"🔄 {t('scanning', lang)}: {scanning_status}\n"
                f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
            )
            await cb.message.edit_text(text, reply_markup=kb_settings_menu(lang))
            return

        if setting == "auto_trade":
            cfg.auto_trade_enabled = not cfg.auto_trade_enabled
            await sync_to_async(cfg.save)()
            auto_trade_status_text = t('enabled', lang) if cfg.auto_trade_enabled else t('disabled', lang)
            await cb.answer(f"{t('auto_trade', lang)} {auto_trade_status_text}")

            scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
            balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
            auto_trade_status = f"✅ {t('enabled', lang)}" if cfg.auto_trade_enabled else f"❌ {t('disabled', lang)}"
            text = (
                f"{t('config_menu', lang)}\n\n"
                f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
                f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
                f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
                f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
                f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
                f"🤖 {t('auto_trade', lang)}: {auto_trade_status}\n"
                f"🔄 {t('scanning', lang)}: {scanning_status}\n"
                f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
            )
            await cb.message.edit_text(text, reply_markup=kb_settings_menu(lang))
            return
        
        if setting == "language":
            # Language callback removed
            await cb.answer("Language selection disabled", show_alert=True)
            return
        
        setting_info = {
            "min_profit": (cfg.min_profit_pct, "min_profit_pct", "%", t("min_profit", lang)),
            "max_profit": (cfg.max_profit_pct, "max_profit_pct", "%", t("max_profit", lang)),
            "min_notional": (cfg.min_notional_usd, "min_notional_usd", "$", t("min_notional", lang)),
            "max_notional": (cfg.max_notional_usd, "max_notional_usd", "$", t("max_notional", lang)),
        }
        
        if setting not in setting_info:
            await cb.answer("Unknown setting", show_alert=True)
            return
        
        current_val, field_name, unit, setting_label = setting_info[setting]
        text = (
            f"⚙️ {setting_label}\n\n"
            f"{t('current_value', lang)}: {current_val}{unit}\n\n"
            f"{t('select_preset', lang)}"
        )
        
        await cb.message.edit_text(text, reply_markup=kb_setting_presets(setting, current_val, unit, lang))
        await cb.answer()

    @dp.callback_query(F.data.startswith("set:"))
    async def set_value(cb: CallbackQuery):
        parts = cb.data.split(":")
        if len(parts) != 3:
            await cb.answer("Invalid format", show_alert=True)
            return
        
        setting = parts[1]
        new_value_str = parts[2]
        
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        
        # Handle language setting (disabled)
        if setting == "language":
            await cb.answer("Language selection disabled", show_alert=True)
            return
        else:
            # Handle numeric settings
            try:
                new_value = float(new_value_str)
            except ValueError:
                await cb.answer("Invalid value", show_alert=True)
                return
            
            setting_map = {
                "min_profit": ("min_profit_pct", "%"),
                "max_profit": ("max_profit_pct", "%"),
                "min_notional": ("min_notional_usd", "$"),
                "max_notional": ("max_notional_usd", "$"),
            }
            
            if setting not in setting_map:
                await cb.answer("Unknown setting", show_alert=True)
                return
            
            field_name, unit = setting_map[setting]

            if field_name in ("min_profit_pct", "max_profit_pct"):
                new_value = max(-2.0, min(10.0, new_value))
                if field_name == "min_profit_pct" and new_value > cfg.max_profit_pct:
                    cfg.max_profit_pct = new_value
                if field_name == "max_profit_pct" and new_value < cfg.min_profit_pct:
                    cfg.min_profit_pct = new_value

            setattr(cfg, field_name, new_value)
            await sync_to_async(cfg.save)()
            await cb.answer(f"{t('settings_saved', lang)}: {new_value}{unit}")
        
        # Return to config menu with updated language
        # lang_display removed
        scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
        balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
        auto_trade_status = f"✅ {t('enabled', lang)}" if cfg.auto_trade_enabled else f"❌ {t('disabled', lang)}"
        text = (
            f"{t('config_menu', lang)}\n\n"
            f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
            f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
            f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
            f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
            f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
            f"🤖 {t('auto_trade', lang)}: {auto_trade_status}\n"
            # Language display line removed
            f"🔄 {t('scanning', lang)}: {scanning_status}\n"
            f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
        )
        await cb.message.edit_text(text, reply_markup=kb_settings_menu(lang))

    @dp.callback_query(F.data.startswith("execok:"))
    async def exec_ok(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = "en"
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer(t("route_missing", lang), show_alert=True)
            return
        cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
        new_cand = revalidate_route(cand)
        if not new_cand:
            await cb.answer(t("not_valid", lang), show_alert=True)
            return
        # Calculate executable amount
        # Re-check balance right before execution (balance may have changed)
        try:
            balances = await sync_to_async(get_account_balance)(S.BASE_ASSET.upper())
            available_balance = balances.get(S.BASE_ASSET.upper(), 0.0)
        except Exception as e:
            await cb.answer(f"Balance check failed: {e}", show_alert=True)
            return
        
        # Use entire balance if enabled, otherwise use normal logic
        if cfg.use_entire_balance:
            # Use 95% of balance to leave buffer
            notional = available_balance * 0.95
            # Still respect route capacity and max notional
            notional = min(notional, new_cand.volume_usd, S.MAX_NOTIONAL_USD)
        else:
            # Normal logic: min(route_volume, max_notional, account_balance)
            notional = min(new_cand.volume_usd, S.MAX_NOTIONAL_USD, available_balance)
        
        if notional < S.MIN_NOTIONAL_USD:
            await cb.answer(f"Insufficient balance. Need ${S.MIN_NOTIONAL_USD:.2f}, have ${available_balance:.2f}", show_alert=True)
            return
        
        ex = await sync_to_async(Execution.objects.create)(route=r, status="running", notional_usd=notional)
        try:
            final_base, orders = await sync_to_async(execute_cycle)(new_cand, notional)
            pnl = final_base - notional
            ex.status = "completed"
            ex.pnl_usd = pnl
            ex.finished_at = timezone.now()
            ex.details = {"orders": orders, "final_base": final_base}
            await cb.answer(t("exec_done", lang))
            await cb.message.edit_text(
                f"{t('exec_done', lang)}\nP&L: ${pnl:,.2f}\nFinal base: {final_base:.4f}",
                reply_markup=kb_route(route_id, lang),
            )
        except Exception as e:
            ex.status = "failed"
            ex.details = {"error": str(e)}
            ex.finished_at = timezone.now()
            await cb.answer(t("exec_failed", lang), show_alert=True)
            await cb.message.edit_text(f"{t('exec_failed', lang)}: {e}", reply_markup=kb_route(route_id, lang))
        finally:
            await sync_to_async(ex.save)()

    logging.info("Bot is running. Press Ctrl+C to stop.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
