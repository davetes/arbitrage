import { config } from '../config.js';
import { createBinanceHttp } from './binanceClient.js';
import { createBinanceSignedHttp, getAccount, getOrder, newMarketOrder } from './binanceSigned.js';
import { parseLeg } from './arbitrage.js';
import { Decimal, d, floorToStepDecimal, quantize8 } from './decimal.js';

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function getFilter(symbolInfo, filterType) {
  return (symbolInfo.filters || []).find((f) => f.filterType === filterType) || null;
}

function symbolFilters(symbolInfo) {
  const lot = getFilter(symbolInfo, 'LOT_SIZE');
  const notional = getFilter(symbolInfo, 'NOTIONAL') || getFilter(symbolInfo, 'MIN_NOTIONAL');

  const stepSize = d(lot?.stepSize ?? '0.000001');
  const minQty = d(lot?.minQty ?? '0');
  const minNotional = d(notional?.minNotional ?? '0');
  return { stepSize, minQty, minNotional };
}

function filledBaseQty(order) {
  if (order?.executedQty !== undefined) return Number(order.executedQty || 0);
  const fills = order?.fills || [];
  if (fills.length) return fills.reduce((s, f) => s + Number(f.qty || 0), 0);
  return 0;
}

function filledQuoteQty(order) {
  if (order?.cummulativeQuoteQty !== undefined) return Number(order.cummulativeQuoteQty || 0);
  const fills = order?.fills || [];
  if (fills.length) return fills.reduce((s, f) => s + Number(f.qty || 0) * Number(f.price || 0), 0);
  const executed = Number(order?.executedQty || 0);
  const avgPrice = Number(order?.avgPrice || 0);
  return executed * avgPrice;
}

async function waitForFilled(signedHttp, symbol, orderId) {
  const timeoutS = Number(process.env.EXEC_ORDER_TIMEOUT_SECONDS || 30);
  const pollS = Number(process.env.EXEC_ORDER_POLL_INTERVAL_SECONDS || 0.25);
  const deadline = Date.now() + timeoutS * 1000;
  let last = null;

  while (Date.now() < deadline) {
    last = await getOrder(signedHttp, { symbol, orderId });
    const status = last?.status;
    if (status === 'FILLED') return last;
    if (['CANCELED', 'REJECTED', 'EXPIRED'].includes(status)) {
      throw new Error(`Order ${orderId} on ${symbol} failed with status=${status}: ${JSON.stringify(last)}`);
    }
    await sleep(pollS * 1000);
  }

  throw new Error(`Timeout waiting for order ${orderId} on ${symbol} to fill. Last=${JSON.stringify(last)}`);
}

export async function getAccountBalance(asset = null) {
  const signedHttp = createBinanceSignedHttp();
  const account = await getAccount(signedHttp);
  const balances = {};
  for (const bal of account?.balances || []) {
    const free = Number(bal.free || 0);
    if (free > 0) balances[String(bal.asset).toUpperCase()] = free;
  }
  if (asset) return { [String(asset).toUpperCase()]: balances[String(asset).toUpperCase()] || 0 };
  return balances;
}

// Port of Python execute_cycle()
export async function executeCycle({ route, notionalUsd }) {
  if (!config.tradingEnabled) {
    throw new Error('Trading disabled. Set TRADING_ENABLED=true to allow live orders.');
  }

  const publicHttp = createBinanceHttp();
  const signedHttp = createBinanceSignedHttp();

  // Need full exchangeInfo filters -> use unsigned exchangeInfo from public http (already in symbolLoader)
  // but symbolLoader strips filters. So fetch full exchangeInfo here.
  const { data: exInfo } = await publicHttp.get('/api/v3/exchangeInfo');
  const symbolMap = new Map();
  for (const s of exInfo.symbols || []) {
    if (s.status === 'TRADING') symbolMap.set(String(s.symbol).toUpperCase(), s);
  }

  const safety = d(config.execSafetyFactor || 0.98);

  const orders = [];
  let currentAsset = String(config.baseAsset).toUpperCase();
  let currentAmount = d(notionalUsd);

  for (const leg of [route.a, route.b, route.c]) {
    const { base, quote, side } = parseLeg(leg);
    const symbol = `${base}${quote}`.toUpperCase();
    const info = symbolMap.get(symbol);
    if (!info) throw new Error(`Symbol ${symbol} not tradable`);

    const { stepSize, minQty, minNotional } = symbolFilters(info);

    if (side === 'buy') {
      if (currentAsset !== quote) {
        throw new Error(`Asset mismatch for leg ${leg}. Have ${currentAsset}, need ${quote}`);
      }

      const effectiveQuote = quantize8(currentAmount.mul(safety));
      if (effectiveQuote.lt(minNotional)) {
        throw new Error(`Amount ${effectiveQuote.toString()} below min notional ${minNotional.toString()} for ${symbol}`);
      }

      let order = await newMarketOrder(signedHttp, {
        symbol,
        side: 'BUY',
        // send as string to avoid float rounding
        quoteOrderQty: effectiveQuote.toString(),
      });

      const orderId = Number(order.orderId);
      order = await waitForFilled(signedHttp, symbol, orderId);

      const qtyOut = d(filledBaseQty(order));
      const bal = await getAccountBalance(base);
      const actualBalance = d(bal[base] || 0);

      currentAsset = base;
      currentAmount = Decimal.min(qtyOut, actualBalance);
      orders.push(order);
    } else {
      if (currentAsset !== base) {
        throw new Error(`Asset mismatch for leg ${leg}. Have ${currentAsset}, need ${base}`);
      }

      const bal = await getAccountBalance(base);
      const actualBalance = d(bal[base] || 0);
      const availableQty = Decimal.min(currentAmount, actualBalance);

      let sellQty = floorToStepDecimal(availableQty.mul(safety), stepSize);
      if (minQty.gt(0) && sellQty.lt(minQty)) {
        sellQty = floorToStepDecimal(availableQty, stepSize);
      }
      if (sellQty.lte(0) || (minQty.gt(0) && sellQty.lt(minQty))) {
        throw new Error(
          `Insufficient balance for ${symbol}: have ${actualBalance.toString()} ${base}, ` +
          `available=${availableQty.toString()}, step=${stepSize.toString()}, minQty=${minQty.toString()}`
        );
      }

      let order = await newMarketOrder(signedHttp, {
        symbol,
        side: 'SELL',
        quantity: sellQty.toString(),
      });
      const orderId = Number(order.orderId);
      order = await waitForFilled(signedHttp, symbol, orderId);

      const quoteGot = d(filledQuoteQty(order));
      currentAsset = quote;
      currentAmount = quoteGot;
      orders.push(order);
    }
  }

  // Return as string to preserve precision
  return { finalBase: currentAmount.toString(), orders };
}
