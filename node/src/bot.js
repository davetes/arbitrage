import { Telegraf } from 'telegraf';
import { config } from './config.js';
import { logger } from './logger.js';
import { initDb, models } from './db/index.js';

await initDb();

if (!config.telegramBotToken) {
  throw new Error('TELEGRAM_BOT_TOKEN is required');
}

const bot = new Telegraf(config.telegramBotToken);

async function getOrCreateSettings() {
  const [cfg] = await models.BotSettings.findOrCreate({
    where: { id: 1 },
    defaults: { id: 1 },
  });
  return cfg;
}

function kbGlobal(scanningEnabled, lang) {
  return {
    inline_keyboard: [
      [{ text: scanningEnabled ? 'Stop Search' : 'Start Search', callback_data: 'toggle_scan' }],
      [{ text: 'Status', callback_data: 'status' }],
    ],
  };
}

function kbRoute(routeId, lang) {
  return {
    inline_keyboard: [
      [{ text: 'Check Validity', callback_data: `check:${routeId}` }],
      [{ text: 'Execute Trade', callback_data: `exec:${routeId}` }],
    ],
  };
}

function kbConfirm(routeId) {
  return {
    inline_keyboard: [
      [{ text: 'Confirm Execute', callback_data: `exec_ok:${routeId}` }],
      [{ text: 'Cancel', callback_data: `exec_cancel:${routeId}` }],
    ],
  };
}

bot.start(async (ctx) => {
  const cfg = await getOrCreateSettings();
  await ctx.reply(
    `ArbBot ready. Scanning is ${cfg.scanning_enabled ? 'ON' : 'OFF'}.`,
    { reply_markup: kbGlobal(cfg.scanning_enabled, cfg.bot_language) }
  );
});

bot.command('status', async (ctx) => {
  const cfg = await getOrCreateSettings();
  const count = await models.Route.count();
  await ctx.reply(`Scanning: ${cfg.scanning_enabled}\nRoutes in DB: ${count}`);
});

bot.command('toggle', async (ctx) => {
  const cfg = await getOrCreateSettings();
  await cfg.update({ scanning_enabled: !cfg.scanning_enabled });
  await ctx.reply(`Scanning now: ${cfg.scanning_enabled ? 'ON' : 'OFF'}`);
});

bot.on('callback_query', async (ctx) => {
  try {
    const data = String(ctx.callbackQuery?.data || '');

    const cfg = await getOrCreateSettings();

    if (data === 'status') {
      const count = await models.Route.count();
      await ctx.editMessageText(
        `Scanning: ${cfg.scanning_enabled ? 'ON' : 'OFF'}\nRoutes in DB: ${count}`,
        { reply_markup: kbGlobal(cfg.scanning_enabled, cfg.bot_language) }
      );
      return ctx.answerCbQuery();
    }

    if (data === 'toggle_scan') {
      await cfg.update({ scanning_enabled: !cfg.scanning_enabled });
      await ctx.editMessageText(
        `Scanning now: ${cfg.scanning_enabled ? 'ON' : 'OFF'}`,
        { reply_markup: kbGlobal(cfg.scanning_enabled, cfg.bot_language) }
      );
      return ctx.answerCbQuery();
    }

    if (data.startsWith('check:')) {
      const id = Number(data.split(':')[1]);
      const route = await models.Route.findByPk(id);
      if (!route) {
        await ctx.answerCbQuery('Route not found');
        return;
      }

      const { createBinanceHttp } = await import('./core/binanceClient.js');
      const { revalidateRoute } = await import('./core/arbitrage.js');
      const binanceHttp = createBinanceHttp();

      const fresh = await revalidateRoute({
        binanceHttp,
        route: { a: route.leg_a, b: route.leg_b, c: route.leg_c },
      });

      if (!fresh) {
        await ctx.reply(`❌ Not valid anymore: ${route.leg_a} → ${route.leg_b} → ${route.leg_c}`);
      } else {
        await ctx.reply(
          `✅ Revalidated\nRoute: ${fresh.a} → ${fresh.b} → ${fresh.c}\nProfit: ${fresh.profit_pct.toFixed(2)}%`,
          { reply_markup: kbRoute(route.id, cfg.bot_language) }
        );
      }

      await ctx.answerCbQuery();
      return;
    }

    if (data.startsWith('exec:')) {
      const id = Number(data.split(':')[1]);
      const route = await models.Route.findByPk(id);
      if (!route) {
        await ctx.answerCbQuery('Route not found');
        return;
      }
      await ctx.reply(
        `Execute trade for route #${route.id}?\n${route.leg_a} → ${route.leg_b} → ${route.leg_c}`,
        { reply_markup: kbConfirm(route.id) }
      );
      await ctx.answerCbQuery();
      return;
    }

    if (data.startsWith('exec_cancel:')) {
      await ctx.answerCbQuery('Cancelled');
      return;
    }

    if (data.startsWith('exec_ok:')) {
      const id = Number(data.split(':')[1]);
      const route = await models.Route.findByPk(id);
      if (!route) {
        await ctx.answerCbQuery('Route not found');
        return;
      }

      // Create execution record
      const execRow = await models.Execution.create({
        route_id: route.id,
        status: 'running',
        notional_usd: cfg.max_notional_usd,
        details: {},
      });

      try {
        const { createBinanceHttp } = await import('./core/binanceClient.js');
        const { revalidateRoute } = await import('./core/arbitrage.js');
        const { executeCycle } = await import('./core/trading.js');

        // Revalidate before execution (same as python bot)
        const binanceHttp = createBinanceHttp();
        const fresh = await revalidateRoute({
          binanceHttp,
          route: { a: route.leg_a, b: route.leg_b, c: route.leg_c },
        });

        if (!fresh) {
          await execRow.update({ status: 'failed', details: { error: 'Route not valid on revalidation' } });
          await ctx.reply(`❌ Route not valid anymore. Execution cancelled.`);
          await ctx.answerCbQuery();
          return;
        }

        const { finalBase, orders } = await executeCycle({ route: fresh, notionalUsd: cfg.max_notional_usd });
        // finalBase is a decimal string
        const pnl = Number(finalBase) - Number(cfg.max_notional_usd);

        await execRow.update({
          status: 'completed',
          pnl_usd: pnl,
          finished_at: new Date(),
          details: { orders, final_base: finalBase, revalidated: fresh },
        });

        await ctx.reply(
          `✅ Execution done\nP&L: $${pnl.toFixed(2)}\nFinal base: ${Number(finalBase).toFixed(6)}`,
          { reply_markup: kbRoute(route.id, cfg.bot_language) }
        );
      } catch (e) {
        await execRow.update({
          status: 'failed',
          finished_at: new Date(),
          details: { error: String(e?.message || e) },
        });
        await ctx.reply(`⚠️ Execution failed: ${String(e?.message || e)}`);
      }

      await ctx.answerCbQuery();
      return;
    }

    await ctx.answerCbQuery('Unknown action');
  } catch (e) {
    await ctx.answerCbQuery('Error');
    logger.error({ err: e }, 'Bot callback error');
  }
});

await bot.launch();
logger.info('Telegram bot started.');
