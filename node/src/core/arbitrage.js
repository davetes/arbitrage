import { config } from '../config.js';
import { logger } from '../logger.js';
import { loadSymbols } from './symbolLoader.js';
import { depthSnapshot } from './depth.js';

export const MAJOR_COINS = new Set(['BTC', 'ETH', 'BNB']);

export function parseLeg(label) {
  const s = String(label || '').trim();
  let pair, side;
  if (s.includes(':')) {
    const parts = s.split(':');
    pair = parts[0].trim();
    side = (parts[1] || '').trim();
  } else {
    const parts = s.split(/\s+/);
    if (parts.length < 2) throw new Error(`Invalid leg format: '${label}'`);
    pair = parts[0];
    side = parts[1];
  }
  if (!pair.includes('/')) throw new Error(`Invalid pair format: '${pair}'`);
  const [base, quote] = pair.split('/');
  return { base: base.toUpperCase(), quote: quote.toUpperCase(), side: side.toLowerCase() };
}

export function applyMidPriceIfEnabled(depth) {
  if (!depth || !config.useMidPrices) return depth;
  const ask = Number(depth.ask_price || 0);
  const bid = Number(depth.bid_price || 0);
  if (!(ask > 0 && bid > 0)) return depth;
  const mid = (ask + bid) / 2;
  return { ...depth, ask_price: mid, bid_price: mid };
}

export async function tryTrianglePattern({ binanceHttp, symbols, base, major, alt, minProfitPct, maxProfitPct, localDepthCache }) {
  if (!MAJOR_COINS.has(major)) return null;

  const pattern1 = [`${major}${base}`, `${alt}${major}`, `${alt}${base}`];
  const pattern2 = [`${alt}${base}`, `${alt}${major}`, `${major}${base}`];

  let pattern1Valid = pattern1.every(s => symbols[s]);
  let pattern2Valid = pattern2.every(s => symbols[s]);

  if (!pattern1Valid && !pattern2Valid) {
    const inverted = `${major}${alt}`;
    if (symbols[inverted]) {
      pattern1[1] = inverted;
      pattern2[1] = inverted;
      pattern1Valid = pattern1.every(s => symbols[s]);
      pattern2Valid = pattern2.every(s => symbols[s]);
    }
  }
  if (!pattern1Valid && !pattern2Valid) return null;

  const getDepth = async (symbol) => {
    if (localDepthCache) {
      if (Object.prototype.hasOwnProperty.call(localDepthCache, symbol)) return localDepthCache[symbol];
      const d = await depthSnapshot(binanceHttp, symbol, true);
      if (d) localDepthCache[symbol] = d;
      return d;
    }
    return depthSnapshot(binanceHttp, symbol, true);
  };

  let best = null;
  let bestProfit = -999;

  if (pattern1Valid) {
    const d1 = applyMidPriceIfEnabled(await getDepth(pattern1[0]));
    const d2 = applyMidPriceIfEnabled(await getDepth(pattern1[1]));
    const d3 = applyMidPriceIfEnabled(await getDepth(pattern1[2]));
    if (d1 && d2 && d3) {
      const isInverted = pattern1[1].startsWith(major);
      let amount = 1.0;
      let profit;
      if (isInverted) {
        amount = amount / d1.ask_price;
        amount = amount * d2.bid_price;
        amount = amount * d3.bid_price;
        profit = (amount - 1.0) * 100.0;
      } else {
        amount = amount / d1.ask_price;
        amount = amount / d2.ask_price;
        amount = amount * d3.bid_price;
        profit = (amount - 1.0) * 100.0;
      }
      if (profit > bestProfit && profit > -50 && profit < 50) {
        bestProfit = profit;
        best = {
          a: `${major}/${base} buy`,
          b: isInverted ? `${major}/${alt} sell` : `${alt}/${major} buy`,
          c: `${alt}/${base} sell`,
          profit_pct: Math.round(profit * 10000) / 10000,
          volume_usd: Math.round(config.maxNotionalUsd * 100) / 100,
        };
      }
    }
  }

  if (pattern2Valid) {
    const d1 = applyMidPriceIfEnabled(await getDepth(pattern2[0]));
    const d2 = applyMidPriceIfEnabled(await getDepth(pattern2[1]));
    const d3 = applyMidPriceIfEnabled(await getDepth(pattern2[2]));
    if (d1 && d2 && d3) {
      const isInverted = pattern2[1].startsWith(major);
      let amount = 1.0;
      let profit;
      if (isInverted) {
        amount = amount / d1.ask_price;
        amount = amount / d2.ask_price;
        amount = amount * d3.bid_price;
        profit = (amount - 1.0) * 100.0;
      } else {
        amount = amount / d1.ask_price;
        amount = amount * d2.bid_price;
        amount = amount * d3.bid_price;
        profit = (amount - 1.0) * 100.0;
      }
      if (profit > bestProfit && profit > -50 && profit < 50) {
        bestProfit = profit;
        best = {
          a: `${alt}/${base} buy`,
          b: isInverted ? `${major}/${alt} buy` : `${alt}/${major} sell`,
          c: `${major}/${base} sell`,
          profit_pct: Math.round(profit * 10000) / 10000,
          volume_usd: Math.round(config.maxNotionalUsd * 100) / 100,
        };
      }
    }
  }

  if (best && best.profit_pct >= minProfitPct && best.profit_pct <= maxProfitPct) {
    return best;
  }
  return null;
}

export async function findCandidateRoutes({ binanceHttp, minProfitPct, maxProfitPct, maxRoutes = 10 }) {
  const start = Date.now();
  const symbols = await loadSymbols(binanceHttp);
  const base = config.baseAsset.toUpperCase();

  // collect assets that trade with base + list major assets
  const baseAssets = [];
  const majorAssets = [];
  for (const info of Object.values(symbols)) {
    const quote = info.quoteAsset;
    const b = info.baseAsset;
    if (quote === base && b !== base) {
      baseAssets.push(b);
      if (MAJOR_COINS.has(b)) majorAssets.push(b);
    }
  }

  if (majorAssets.length === 0) return { routes: [], stats: { symbols_loaded: Object.keys(symbols).length, routes_found: 0 } };

  const uniqMajor = [...new Set(majorAssets)];
  const uniqBaseAssets = [...new Set(baseAssets)];

  const allPairs = [];
  for (const major of uniqMajor) {
    for (const alt of uniqBaseAssets) {
      if (alt === major) continue;
      if (MAJOR_COINS.has(alt)) continue;
      allPairs.push([major, alt]);
    }
  }

  const depthCache = {};
  const routes = [];

  // sequential to be safe with rate limits (python used 2 threads)
  for (let i = 0; i < allPairs.length; i++) {
    const [major, alt] = allPairs[i];
    const r = await tryTrianglePattern({
      binanceHttp,
      symbols,
      base,
      major,
      alt,
      minProfitPct,
      maxProfitPct,
      localDepthCache: depthCache,
    });
    if (r) {
      if (!routes.some(x => x.a === r.a && x.b === r.b && x.c === r.c)) routes.push(r);
      if (routes.length >= maxRoutes) break;
    }
    if (i > 0 && i % 200 === 0) {
      logger.info({ progress: `${i}/${allPairs.length}`, found: routes.length }, 'Scanning progress');
    }
  }

  routes.sort((x, y) => y.profit_pct - x.profit_pct);

  const stats = {
    symbols_loaded: Object.keys(symbols).length,
    triangles_checked: allPairs.length,
    routes_found: routes.length,
    fetch_time: (Date.now() - start) / 1000,
  };

  return { routes, stats };
}

export async function revalidateRoute({ binanceHttp, route }) {
  // mimic python: infer major/alt by sides
  const base = config.baseAsset.toUpperCase();

  let major, alt;
  if (route.a.includes('buy') && route.b.includes('buy') && route.c.includes('sell')) {
    major = parseLeg(route.a).base;
    alt = parseLeg(route.c).base;
  } else if (route.a.includes('buy') && route.b.includes('sell') && route.c.includes('sell')) {
    major = parseLeg(route.c).base;
    alt = parseLeg(route.a).base;
  } else {
    return null;
  }

  const symbols = await loadSymbols(binanceHttp);
  return tryTrianglePattern({
    binanceHttp,
    symbols,
    base,
    major,
    alt,
    minProfitPct: -50,
    maxProfitPct: 50,
    localDepthCache: null,
  });
}
