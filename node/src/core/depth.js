import { logger } from '../logger.js';

const depthCache = new Map(); // symbol -> { data, ts }
const TTL_MS = 10_000;

export function clearDepthCache() {
  depthCache.clear();
}

export async function depthSnapshot(binanceHttp, symbol, useCache = true) {
  const sym = symbol.toUpperCase();
  const now = Date.now();

  if (useCache) {
    const cached = depthCache.get(sym);
    if (cached && (now - cached.ts) < TTL_MS) return cached.data;
  }

  // Similar to python: 2 attempts with backoff, depth limit 5
  let backoff = 500;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const { data } = await binanceHttp.get('/api/v3/depth', {
        params: { symbol: sym, limit: 5 },
      });

      if (!data.asks?.length || !data.bids?.length) return null;

      const [askP, askQ] = data.asks[0].map(Number);
      const [bidP, bidQ] = data.bids[0].map(Number);
      const totalAskQty = data.asks.reduce((s, [, q]) => s + Number(q), 0);
      const totalBidQty = data.bids.reduce((s, [, q]) => s + Number(q), 0);
      const capAskQuote = data.asks.reduce((s, [p, q]) => s + Number(p) * Number(q), 0);
      const capBidQuote = data.bids.reduce((s, [p, q]) => s + Number(p) * Number(q), 0);

      const result = {
        ask_price: askP,
        ask_qty: askQ,
        bid_price: bidP,
        bid_qty: bidQ,
        total_ask_qty: totalAskQty,
        total_bid_qty: totalBidQty,
        cap_ask_quote: capAskQuote,
        cap_bid_quote: capBidQuote,
      };

      if (useCache) depthCache.set(sym, { data: result, ts: now });
      return result;
    } catch (e) {
      const msg = String(e?.message || e);
      const lowered = msg.toLowerCase();
      if (lowered.includes('429') || lowered.includes('too much request weight') || lowered.includes('banned')) {
        const waitMs = backoff * 6;
        logger.warn({ symbol: sym, waitMs }, 'Rate limit hit, backing off');
        await new Promise(r => setTimeout(r, waitMs));
      } else {
        await new Promise(r => setTimeout(r, backoff));
      }
      backoff *= 3;
    }
  }

  return null;
}
