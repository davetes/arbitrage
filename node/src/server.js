import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import { config } from './config.js';
import { logger } from './logger.js';
import { initDb } from './db/index.js';
import { routesRouter } from './web/routes.js';
import { settingsRouter } from './web/settings.js';

const app = express();
app.use(helmet());
app.use(express.json({ limit: '1mb' }));
app.use(morgan('dev'));

app.get('/health', (_req, res) => res.json({ ok: true }));
app.use('/api/routes', routesRouter);
app.use('/api/settings', settingsRouter);

app.use((err, _req, res, _next) => {
  logger.error({ err }, 'Unhandled error');
  res.status(500).json({ error: 'internal_error' });
});

await initDb();

app.listen(config.port, () => {
  logger.info(`HTTP server listening on http://127.0.0.1:${config.port}`);
});
