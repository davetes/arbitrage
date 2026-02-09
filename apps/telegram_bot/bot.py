import asyncio
import os
import sys
import django
from pathlib import Path
import logging

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
from apps.core.models import BotSettings, Route, Execution
from apps.core.arbitrage import CandidateRoute, revalidate_route
from apps.core.trading import execute_cycle, get_account_balance
from asgiref.sync import sync_to_async
from apps.core.tasks import scan_triangular_routes


def t(key: str, lang: str = None) -> str:
    """Translation function. Always returns English."""
    # Force English
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
    # Directly return English text
    return en.get(key, key)


async def kb_global():
    cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
    lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
    if cfg.scanning_enabled:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t("stop_search", lang), callback_data="toggle_scan")]])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t("start_search", lang), callback_data="toggle_scan")]])


def kb_route(route_id: int, lang: str = None):
    return InlineKeyboardMarkup(inline_keyboard=[
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
        # Language toggle removed
        [InlineKeyboardButton(text="🔄 " + t("toggle_scanning", lang), callback_data="config:toggle_scan")],
        [InlineKeyboardButton(text="🔙 " + t("back", lang), callback_data="config:back")],
    ])


def kb_setting_presets(setting: str, current_val: float, unit: str, lang: str = None):
    """Create preset buttons for a setting"""
    if "%" in unit:
        # Profit percentage presets
        presets = [0.01, 0.1, 0.5, 1.0, 2.0, 2.5, 5.0]
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
        BotCommand(command="triangular_alerts", description="Get triangular arbitrage alerts"),
        BotCommand(command="direct_alerts", description="Get direct arbitrage alerts"),
        BotCommand(command="transaction_history", description="Get transaction history"),
    ]
    await bot.set_my_commands(commands)

    @dp.message(CommandStart())
    async def on_start(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        if not cfg.bot_language:
            cfg.bot_language = S.BOT_LANGUAGE
            await sync_to_async(cfg.save)()
        lang = cfg.bot_language
        reply_kb = await kb_global()
        logging.info(f"/start from chat_id={msg.chat.id} username={getattr(msg.from_user, 'username', '')}")
        
        # Add status info
        status_emoji = "✅" if cfg.scanning_enabled else "❌"
        status_text = t("enabled", lang) if cfg.scanning_enabled else t("disabled", lang)
        welcome_text = (
            f"{t('ready', lang)}\n\n"
            f"{status_emoji} {t('scanning', lang)}: {status_text}"
        )
        
        await msg.answer(
            welcome_text,
            reply_markup=reply_kb,
        )

    @dp.message(F.text == "/config")
    async def on_config(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        if not cfg.bot_language:
            cfg.bot_language = S.BOT_LANGUAGE
            await sync_to_async(cfg.save)()
        lang = cfg.bot_language
        # lang_display removed
        scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
        balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
        text = (
            f"{t('config_menu', lang)}\n\n"
            f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
            f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
            f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
            f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
            f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
            # Language display line removed
            f"🔄 {t('scanning', lang)}: {scanning_status}\n"
            f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
        )
        await msg.answer(text, reply_markup=kb_settings_menu(lang))

    @dp.message(F.text == "/triangular_alerts")
    async def on_tri_alerts(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
        await msg.answer(t("tri", lang))

    @dp.message(F.text == "/direct_alerts")
    async def on_direct_alerts(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
        await msg.answer(t("direct", lang))

    @dp.message(F.text == "/transaction_history")
    async def on_tx_history(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
        await msg.answer(t("history", lang))

    @dp.message(F.text == "/status")
    async def on_status(msg: Message):
        """Check bot status and configuration"""
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
        
        # Check recent routes
        from apps.core.models import Route
        recent_routes = await sync_to_async(lambda: Route.objects.order_by('-id')[:5].count())()
        total_routes = await sync_to_async(lambda: Route.objects.count())()
        
        status_emoji = "✅" if cfg.scanning_enabled else "❌"
        status_text = (
            f"🤖 Bot Status\n\n"
            f"{status_emoji} {t('scanning', lang)}: {t('enabled', lang) if cfg.scanning_enabled else t('disabled', lang)}\n\n"
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
        
        await msg.answer(status_text, reply_markup=await kb_global())


    @dp.callback_query(F.data == "toggle_scan")
    async def toggle_scan(cb: CallbackQuery):
        try:
            cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
            lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
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
                    f"💡 Make sure Celery worker and beat are running!\n\n"
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
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
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
                text = f"Route: {new_cand.a} → {new_cand.b} → {new_cand.c}\nProfit: {new_cand.profit_pct:.2f}%\nVolume: ${new_cand.volume_usd:,.0f}"
                await cb.message.edit_text(text, reply_markup=kb_route(route_id, lang))
                await cb.answer(t("revalidated", lang))
        except Exception as e:
            logging.error(f"Error checking route {route_id}: {e}", exc_info=True)
            await cb.answer(f"Error: {str(e)[:100]}", show_alert=True)

    @dp.callback_query(F.data.startswith("exec:"))
    async def exec_route(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
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
        
        text = (
            f"{t('confirm_title', lang)}\n"
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
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
        await cb.answer(t("cancelled", lang))
        await cb.message.edit_text(t("cancelled", lang))

    @dp.callback_query(F.data == "config:back")
    async def config_back(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        if not cfg.bot_language:
            cfg.bot_language = S.BOT_LANGUAGE
            await sync_to_async(cfg.save)()
        lang = cfg.bot_language
        # lang_display removed
        scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
        balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
        text = (
            f"{t('config_menu', lang)}\n\n"
            f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
            f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
            f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
            f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
            f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
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
        if not cfg.bot_language:
            cfg.bot_language = S.BOT_LANGUAGE
            await sync_to_async(cfg.save)()
        lang = cfg.bot_language
        
        if setting == "toggle_scan":
            cfg.scanning_enabled = not cfg.scanning_enabled
            await sync_to_async(cfg.save)()
            scanning_status_text = t('enabled', lang) if cfg.scanning_enabled else t('disabled', lang)
            await cb.answer(f"{t('scanning', lang)} {scanning_status_text}")
            
            # Return to config menu
            # lang_display removed
            scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
            balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
            text = (
                f"{t('config_menu', lang)}\n\n"
                f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
                f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
                f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
                f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
                f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
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
            text = (
                f"{t('config_menu', lang)}\n\n"
                f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
                f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
                f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
                f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
                f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
                # Language display line removed
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
        if not cfg.bot_language:
            cfg.bot_language = S.BOT_LANGUAGE
            await sync_to_async(cfg.save)()
        lang = cfg.bot_language
        
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
            setattr(cfg, field_name, new_value)
            await sync_to_async(cfg.save)()
            await cb.answer(f"{t('settings_saved', lang)}: {new_value}{unit}")
        
        # Return to config menu with updated language
        # lang_display removed
        scanning_status = f"✅ {t('enabled', lang)}" if cfg.scanning_enabled else f"❌ {t('disabled', lang)}"
        balance_mode_status = f"✅ {t('enabled', lang)}" if cfg.use_entire_balance else f"❌ {t('disabled', lang)}"
        text = (
            f"{t('config_menu', lang)}\n\n"
            f"📊 {t('min_profit', lang)}: {cfg.min_profit_pct}%\n"
            f"📊 {t('max_profit', lang)}: {cfg.max_profit_pct}%\n"
            f"💰 {t('min_notional', lang)}: ${cfg.min_notional_usd:,.0f}\n"
            f"💰 {t('max_notional', lang)}: ${cfg.max_notional_usd:,.0f}\n"
            f"💵 {t('use_entire_balance', lang)}: {balance_mode_status}\n"
            # Language display line removed
            f"🔄 {t('scanning', lang)}: {scanning_status}\n"
            f"💱 {t('base_asset', lang)}: {cfg.base_asset}"
        )
        await cb.message.edit_text(text, reply_markup=kb_settings_menu(lang))

    @dp.callback_query(F.data.startswith("execok:"))
    async def exec_ok(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        lang = cfg.bot_language if cfg.bot_language else S.BOT_LANGUAGE
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
        if not S.TRADING_ENABLED:
            await cb.answer(t("trade_disabled", lang), show_alert=True)
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
