import dotenv from 'dotenv';

dotenv.config();

function env(name, fallback = undefined) {
  const v = process.env[name];
  return (v === undefined || v === '') ? fallback : v;
}

function envFloat(name, fallback) {
  const v = env(name);
  if (v === undefined) return fallback;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Invalid number for ${name}: ${v}`);
  return n;
}

function envInt(name, fallback) {
  const v = env(name);
  if (v === undefined) return fallback;
  const n = Number.parseInt(v, 10);
  if (!Number.isFinite(n)) throw new Error(`Invalid int for ${name}: ${v}`);
  return n;
}

function envBool(name, fallback = false) {
  const v = env(name);
  if (v === undefined) return fallback;
  return ['1', 'true', 'yes', 'y', 'on'].includes(String(v).toLowerCase());
}

export const config = {
  port: envInt('PORT', 3000),
  databaseUrl: env('DATABASE_URL', null),
  redisUrl: env('REDIS_URL', 'redis://127.0.0.1:6379/0'),

  binanceBaseUrl: env('BINANCE_BASE_URL', 'https://api.binance.com'),
  proxyUrl: env('PROXY_URL', ''),
  binanceApiKey: env('BINANCE_API_KEY', ''),
  binanceApiSecret: env('BINANCE_API_SECRET', ''),

  botLanguage: env('BOT_LANGUAGE', 'en'),
  baseAsset: env('BASE_ASSET', 'USDT'),
  feeRateBps: envFloat('FEE_RATE_BPS', 10),
  extraFeeBps: envFloat('EXTRA_FEE_BPS', 0),
  minProfitPct: envFloat('MIN_PROFIT_PCT', 1.0),
  maxProfitPct: envFloat('MAX_PROFIT_PCT', 2.5),
  slippageBps: envFloat('SLIPPAGE_BPS', 10),
  minNotionalUsd: envFloat('MIN_NOTIONAL_USD', 10),
  maxNotionalUsd: envFloat('MAX_NOTIONAL_USD', 10000),
  scanIntervalSeconds: envInt('SCAN_INTERVAL_SECONDS', 3),
  useMidPrices: envBool('USE_MID_PRICES', false),
  execSafetyFactor: envFloat('EXEC_SAFETY_FACTOR', 0.98),
  tradingEnabled: envBool('TRADING_ENABLED', false),

  telegramBotToken: env('TELEGRAM_BOT_TOKEN', ''),
  adminTelegramId: env('ADMIN_TELEGRAM_ID', ''),
};

// Allow running with a default local Postgres URL if DATABASE_URL is not provided.
// This matches the Django sample defaults (postgres/postgres@127.0.0.1:5432/arbbot).
if (!config.databaseUrl) {
  config.databaseUrl = 'postgres://postgres:postgres@127.0.0.1:5432/arbbot';
}
