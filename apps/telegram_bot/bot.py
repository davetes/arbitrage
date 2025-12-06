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


def t(key: str) -> str:
    ru = {
        "ready": "Бот готов. Используйте кнопки, чтобы управлять сканированием.",
        "config": "Откройте Django admin (/admin) или используйте кнопки. Встроенные настройки будут позже.",
        "tri": "Здесь вы будете получать маршруты треугольного арбитража.",
        "direct": "Прямой арбитраж пока не реализован.",
        "history": "История сделок будет доступна позже.",
        "toggle": "Поиск переключен",
        "route_missing": "Маршрут больше не существует",
        "not_valid": "Маршрут сейчас невалиден",
        "revalidated": "Маршрут обновлён",
        "trade_disabled": "Трейдинг выключен (TRADING_ENABLED=false)",
        "exec_started": "Исполнение ожидает подтверждения",
        "exec_done": "Цикл выполнен",
        "exec_failed": "Ошибка исполнения",
        "confirm_title": "Подтвердите исполнение:",
        "confirm_btn": "Подтвердить",
        "cancel_btn": "Отмена",
        "cancelled": "Отменено",
        "start_search": "Запустить поиск",
        "stop_search": "Остановить поиск",
        "check": "Проверить актуальность",
        "exec": "Исполнить сделку",
    }
    en = {
        "ready": "Arbitrage bot ready. Use buttons to control scanning.",
        "config": "Open Django admin at /admin or use buttons. In-bot settings coming soon.",
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
    }
    return ru.get(key) if S.BOT_LANGUAGE.lower().startswith("ru") else en.get(key)


async def kb_global():
    cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
    if cfg.scanning_enabled:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t("stop_search"), callback_data="toggle_scan")]])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t("start_search"), callback_data="toggle_scan")]])


def kb_route(route_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("check"), callback_data=f"check:{route_id}")],
        [InlineKeyboardButton(text=t("exec"), callback_data=f"exec:{route_id}")],
    ])


def kb_confirm(route_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("confirm_btn"), callback_data=f"execok:{route_id}"),
            InlineKeyboardButton(text=t("cancel_btn"), callback_data=f"execcancel:{route_id}"),
        ]
    ])


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
        reply_kb = await kb_global()
        logging.info(f"/start from chat_id={msg.chat.id} username={getattr(msg.from_user, 'username', '')}")
        await msg.answer(
            t("ready"),
            reply_markup=reply_kb,
        )

    @dp.message(F.text == "/config")
    async def on_config(msg: Message):
        await msg.answer(t("config"))

    @dp.message(F.text == "/triangular_alerts")
    async def on_tri_alerts(msg: Message):
        await msg.answer(t("tri"))

    @dp.message(F.text == "/direct_alerts")
    async def on_direct_alerts(msg: Message):
        await msg.answer(t("direct"))

    @dp.message(F.text == "/transaction_history")
    async def on_tx_history(msg: Message):
        await msg.answer(t("history"))

    @dp.callback_query(F.data == "toggle_scan")
    async def toggle_scan(cb: CallbackQuery):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        cfg.scanning_enabled = not cfg.scanning_enabled
        await sync_to_async(cfg.save)()
        reply_kb = await kb_global()
        try:
            await cb.message.edit_reply_markup(reply_markup=reply_kb)
        except TelegramBadRequest:
            # If markup/content are the same, ignore the error
            pass
        logging.info(f"Scanning toggled to {cfg.scanning_enabled} by chat_id={cb.from_user.id}")
        await cb.answer(t("toggle"))

    @dp.callback_query(F.data.startswith("check:"))
    async def check_route(cb: CallbackQuery):
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer(t("route_missing"), show_alert=True)
            return
        cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
        new_cand = revalidate_route(cand)
        if not new_cand:
            await cb.answer(t("not_valid"), show_alert=True)
        else:
            logging.info(f"Route {route_id} revalidated: profit={new_cand.profit_pct}% volume=${new_cand.volume_usd}")
            text = f"Route: {new_cand.a} → {new_cand.b} → {new_cand.c}\nProfit: {new_cand.profit_pct:.2f}%\nVolume: ${new_cand.volume_usd:,.0f}"
            await cb.message.edit_text(text, reply_markup=kb_route(route_id))
            await cb.answer(t("revalidated"))

    @dp.callback_query(F.data.startswith("exec:"))
    async def exec_route(cb: CallbackQuery):
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer(t("route_missing"), show_alert=True)
            return
        # Revalidate before execution
        cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
        new_cand = revalidate_route(cand)
        if not new_cand:
            await cb.answer(t("not_valid"), show_alert=True)
            return
        
        # Calculate executable amount: min(route_volume, max_notional, account_balance)
        try:
            balances = await sync_to_async(get_account_balance)(S.BASE_ASSET.upper())
            available_balance = balances.get(S.BASE_ASSET.upper(), 0.0)
        except Exception:
            available_balance = S.MAX_NOTIONAL_USD  # Fallback if balance check fails
        
        notional = min(new_cand.volume_usd, S.MAX_NOTIONAL_USD, available_balance)
        
        text = (
            f"{t('confirm_title')}\n"
            f"Route: {new_cand.a} → {new_cand.b} → {new_cand.c}\n"
            f"Profit: {new_cand.profit_pct:.2f}%\n"
            f"Available balance: ${available_balance:,.2f}\n"
            f"Planned notional: ${notional:,.2f}"
        )
        await cb.message.edit_text(text, reply_markup=kb_confirm(route_id))
        await cb.answer(t("exec_started"))

    @dp.callback_query(F.data.startswith("execcancel:"))
    async def exec_cancel(cb: CallbackQuery):
        await cb.answer(t("cancelled"))
        await cb.message.edit_text(t("cancelled"))

    @dp.callback_query(F.data.startswith("execok:"))
    async def exec_ok(cb: CallbackQuery):
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer(t("route_missing"), show_alert=True)
            return
        cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
        new_cand = revalidate_route(cand)
        if not new_cand:
            await cb.answer(t("not_valid"), show_alert=True)
            return
        if not S.TRADING_ENABLED:
            await cb.answer(t("trade_disabled"), show_alert=True)
            return
        
        # Calculate executable amount: min(route_volume, max_notional, account_balance)
        # Re-check balance right before execution (balance may have changed)
        try:
            balances = await sync_to_async(get_account_balance)(S.BASE_ASSET.upper())
            available_balance = balances.get(S.BASE_ASSET.upper(), 0.0)
        except Exception as e:
            await cb.answer(f"Balance check failed: {e}", show_alert=True)
            return
        
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
            await cb.answer(t("exec_done"))
            await cb.message.edit_text(
                f"{t('exec_done')}\nP&L: ${pnl:,.2f}\nFinal base: {final_base:.4f}",
                reply_markup=kb_route(route_id),
            )
        except Exception as e:
            ex.status = "failed"
            ex.details = {"error": str(e)}
            ex.finished_at = timezone.now()
            await cb.answer(t("exec_failed"), show_alert=True)
            await cb.message.edit_text(f"{t('exec_failed')}: {e}", reply_markup=kb_route(route_id))
        finally:
            await sync_to_async(ex.save)()

    logging.info("Bot is running. Press Ctrl+C to stop.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
