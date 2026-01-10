import { logger } from '../logger.js';

// Same as python TARGET_QUOTES
const TARGET_QUOTES = new Set(['USDT', 'BTC', 'ETH', 'BNB', 'FDUSD', 'USDC']);

let cache = null;
let cacheTs = 0;
const CACHE_TTL_MS = 5 * 60 * 1000;

export async function loadSymbols(binanceHttp) {
  const now = Date.now();
  if (cache && (now - cacheTs) < CACHE_TTL_MS) return cache;

  const { data } = await binanceHttp.get('/api/v3/exchangeInfo');
  const symbols = {};
  let invalidCount = 0;
  let filteredCount = 0;
  for (const info of (data.symbols || [])) {
    const symbol = String(info.symbol || '').trim();
    if (!symbol) continue;
    if (info.status !== 'TRADING') continue;

    const baseAsset = String(info.baseAsset || '').toUpperCase().trim();
    const quoteAsset = String(info.quoteAsset || '').toUpperCase().trim();
    if (!baseAsset || !quoteAsset || baseAsset === quoteAsset) {
      invalidCount++;
      continue;
    }
    if (!TARGET_QUOTES.has(quoteAsset)) {
      filteredCount++;
      continue;
    }

    symbols[symbol] = { symbol, baseAsset, quoteAsset, status: 'TRADING' };
  }

  logger.info({ count: Object.keys(symbols).length, filteredCount, invalidCount }, 'Loaded symbols');
  cache = symbols;
  cacheTs = now;
  return symbols;
}

export function clearSymbolsCache() {
  cache = null;
  cacheTs = 0;
}
