import asyncio
import os
import sys
import django
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
# Ensure project root is importable so 'arbbot' package can be found
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "arbbot.settings")
django.setup()

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
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
    bot = Bot(token=S.TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def on_start(msg: Message):
        cfg, _ = await sync_to_async(BotSettings.objects.get_or_create)(id=1)
        reply_kb = await kb_global()
        await msg.answer(
            "Arbitrage bot ready. Use buttons to control scanning.",
            reply_markup=reply_kb,
        )

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
        await cb.answer("Executed (placeholder)")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
