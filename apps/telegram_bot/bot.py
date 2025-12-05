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
from asgiref.sync import sync_to_async
from apps.core.tasks import scan_triangular_routes


async def kb_global():
    cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
    if cfg.scanning_enabled:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Stop Search", callback_data="toggle_scan")]])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Start Search", callback_data="toggle_scan")]])


def kb_route(route_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Check Validity", callback_data=f"check:{route_id}")],
        [InlineKeyboardButton(text="Execute Trade", callback_data=f"exec:{route_id}")],
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
            "Arbitrage bot ready. Use buttons to control scanning.",
            reply_markup=reply_kb,
        )

    @dp.message(F.text == "/config")
    async def on_config(msg: Message):
        await msg.answer("Open Django admin at /admin or use buttons to toggle scanning. In-bot settings will be added soon.")

    @dp.message(F.text == "/triangular_alerts")
    async def on_tri_alerts(msg: Message):
        await msg.answer("You will receive triangular arbitrage route messages here when found.")

    @dp.message(F.text == "/direct_alerts")
    async def on_direct_alerts(msg: Message):
        await msg.answer("Direct arbitrage alerts are not implemented yet. We'll add them after triangular flow.")

    @dp.message(F.text == "/transaction_history")
    async def on_tx_history(msg: Message):
        await msg.answer("Execution history will be available in Django admin and here soon.")

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
        await cb.answer("Scanning toggled")

    @dp.callback_query(F.data.startswith("check:"))
    async def check_route(cb: CallbackQuery):
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer("Route no longer exists", show_alert=True)
            return
        cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
        new_cand = revalidate_route(cand)
        if not new_cand:
            await cb.answer("Not valid now", show_alert=True)
        else:
            logging.info(f"Route {route_id} revalidated: profit={new_cand.profit_pct}% volume=${new_cand.volume_usd}")
            text = f"Route: {new_cand.a} → {new_cand.b} → {new_cand.c}\nProfit: {new_cand.profit_pct:.2f}%\nVolume: ${new_cand.volume_usd:,.0f}"
            await cb.message.edit_text(text, reply_markup=kb_route(route_id))
            await cb.answer("Revalidated")

    @dp.callback_query(F.data.startswith("exec:"))
    async def exec_route(cb: CallbackQuery):
        route_id = int(cb.data.split(":")[1])
        r = await sync_to_async(lambda: Route.objects.filter(id=route_id).first())()
        if not r:
            await cb.answer("Route no longer exists", show_alert=True)
            return
        # Revalidate before execution
        cand = CandidateRoute(a=r.leg_a, b=r.leg_b, c=r.leg_c, profit_pct=r.profit_pct, volume_usd=r.volume_usd)
        new_cand = revalidate_route(cand)
        if not new_cand:
            await cb.answer("Route invalid now", show_alert=True)
            return
        ex = await sync_to_async(Execution.objects.create)(route=r, status="running", notional_usd=min(new_cand.volume_usd, 1000))
        # Placeholder: call actual trading executor here
        ex.status = "completed"
        ex.pnl_usd = 0.0
        ex.finished_at = timezone.now()
        await sync_to_async(ex.save)()
        logging.info(f"Executed placeholder for route {route_id} notional=${ex.notional_usd}")
        await cb.answer("Executed (placeholder)")

    logging.info("Bot is running. Press Ctrl+C to stop.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
