import { Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';
import { config } from './config.js';
import { logger } from './logger.js';
import { initDb, models } from './db/index.js';

import { createBinanceHttp } from './core/binanceClient.js';
import { findCandidateRoutes } from './core/arbitrage.js';
import { sendTelegramMessage } from './core/telegramNotify.js';
import { t } from './core/translations.js';

const connection = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });
const queueName = 'scan-triangular';

await initDb();

const queue = new Queue(queueName, { connection });

// repeat job like Celery beat
await queue.upsertJobScheduler('scan', {
  every: config.scanIntervalSeconds * 1000,
});

await queue.add('scan', {}, { jobId: 'scan' });

const binanceHttp = createBinanceHttp();

const worker = new Worker(queueName, async () => {
  const [cfg] = await models.BotSettings.findOrCreate({ where: { id: 1 }, defaults: { id: 1 } });
  if (cfg.scanning_enabled === false) {
    logger.info('Scanning disabled; skipping tick');
    return { skipped: true };
  }

  const minProfitPct = cfg.min_profit_pct;
  const maxProfitPct = cfg.max_profit_pct;

  const { routes, stats } = await findCandidateRoutes({
    binanceHttp,
    minProfitPct,
    maxProfitPct,
    maxRoutes: 10,
  });

  let created = 0;
  const lang = cfg.bot_language || config.botLanguage;

  for (const r of routes) {
    const createdRoute = await models.Route.create({
      leg_a: r.a,
      leg_b: r.b,
      leg_c: r.c,
      profit_pct: r.profit_pct,
      volume_usd: r.volume_usd,
    });
    created++;

    // Notify per-route with inline buttons
    await sendTelegramMessage({
      text: `Route: ${createdRoute.leg_a} → ${createdRoute.leg_b} → ${createdRoute.leg_c}\nProfit: ${createdRoute.profit_pct.toFixed(2)}%\nVolume: $${Math.round(createdRoute.volume_usd).toLocaleString()}`,
      replyMarkup: {
        inline_keyboard: [
          [{ text: t('check', lang), callback_data: `check:${createdRoute.id}` }],
          [{ text: t('exec', lang), callback_data: `exec:${createdRoute.id}` }],
        ],
      },
    });
  }

  if (created > 0) {
    await sendTelegramMessage({
      text:
        `Scan Summary\n\n` +
        `Symbols loaded: ${stats.symbols_loaded}\n` +
        `Triangles checked: ${stats.triangles_checked}\n` +
        `Routes found: ${stats.routes_found}\n` +
        `Routes created: ${created}`,
    });
  }

  return { created, stats };
}, { connection });

worker.on('completed', (job) => logger.info({ jobId: job.id }, 'completed'));
worker.on('failed', (job, err) => logger.error({ jobId: job?.id, err }, 'failed'));

logger.info('Worker started.');
