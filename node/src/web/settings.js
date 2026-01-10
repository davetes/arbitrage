import { Router } from 'express';
import { models } from '../db/index.js';

export const settingsRouter = Router();

async function getOrCreateSettings() {
  const [cfg] = await models.BotSettings.findOrCreate({
    where: { id: 1 },
    defaults: { id: 1 },
  });
  return cfg;
}

settingsRouter.get('/', async (_req, res, next) => {
  try {
    const cfg = await getOrCreateSettings();
    res.json(cfg.toJSON());
  } catch (e) { next(e); }
});

settingsRouter.patch('/', async (req, res, next) => {
  try {
    const cfg = await getOrCreateSettings();
    await cfg.update(req.body);
    res.json(cfg.toJSON());
  } catch (e) { next(e); }
});
