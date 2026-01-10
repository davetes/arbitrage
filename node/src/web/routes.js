import { Router } from 'express';
import { models } from '../db/index.js';

export const routesRouter = Router();

routesRouter.get('/', async (_req, res, next) => {
  try {
    const routes = await models.Route.findAll({ order: [['created_at', 'DESC']], limit: 50 });
    res.json(routes.map(r => r.toJSON()));
  } catch (e) { next(e); }
});

routesRouter.get('/:id', async (req, res, next) => {
  try {
    const r = await models.Route.findByPk(req.params.id);
    if (!r) return res.status(404).json({ error: 'not_found' });
    res.json(r.toJSON());
  } catch (e) { next(e); }
});

// manual revalidation endpoint (useful for debugging without Telegram)
routesRouter.post('/:id/revalidate', async (req, res, next) => {
  try {
    const r = await models.Route.findByPk(req.params.id);
    if (!r) return res.status(404).json({ error: 'not_found' });

    const { createBinanceHttp } = await import('../core/binanceClient.js');
    const { revalidateRoute } = await import('../core/arbitrage.js');

    const binanceHttp = createBinanceHttp();
    const fresh = await revalidateRoute({
      binanceHttp,
      route: { a: r.leg_a, b: r.leg_b, c: r.leg_c },
    });

    res.json({ ok: true, fresh });
  } catch (e) { next(e); }
});
