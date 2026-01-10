import crypto from 'crypto';
import axios from 'axios';
import { config } from '../config.js';

function assertTradingConfigured() {
  if (!config.binanceApiKey || !config.binanceApiSecret) {
    throw new Error('BINANCE_API_KEY and BINANCE_API_SECRET are required for signed endpoints');
  }
}

function toQuery(params) {
  const usp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null) continue;
    usp.append(k, String(v));
  }
  return usp.toString();
}

function sign(queryString) {
  return crypto.createHmac('sha256', config.binanceApiSecret).update(queryString).digest('hex');
}

export function createBinanceSignedHttp() {
  assertTradingConfigured();
  return axios.create({
    baseURL: config.binanceBaseUrl,
    timeout: 30000,
    headers: {
      'X-MBX-APIKEY': config.binanceApiKey,
    },
    proxy: false,
  });
}

export async function signedGet(http, path, params = {}) {
  assertTradingConfigured();
  const timestamp = Date.now();
  const recvWindow = 10_000;
  const qs = toQuery({ ...params, timestamp, recvWindow });
  const signature = sign(qs);
  const url = `${path}?${qs}&signature=${signature}`;
  const { data } = await http.get(url);
  return data;
}

export async function signedPost(http, path, params = {}) {
  assertTradingConfigured();
  const timestamp = Date.now();
  const recvWindow = 10_000;
  const qs = toQuery({ ...params, timestamp, recvWindow });
  const signature = sign(qs);
  // Binance expects signed params in querystring for application/x-www-form-urlencoded
  const url = `${path}?${qs}&signature=${signature}`;
  const { data } = await http.post(url);
  return data;
}

export async function signedDelete(http, path, params = {}) {
  assertTradingConfigured();
  const timestamp = Date.now();
  const recvWindow = 10_000;
  const qs = toQuery({ ...params, timestamp, recvWindow });
  const signature = sign(qs);
  const url = `${path}?${qs}&signature=${signature}`;
  const { data } = await http.delete(url);
  return data;
}

export async function getAccount(http) {
  return signedGet(http, '/api/v3/account');
}

export async function getOrder(http, { symbol, orderId }) {
  return signedGet(http, '/api/v3/order', { symbol, orderId });
}

export async function newMarketOrder(http, { symbol, side, quantity, quoteOrderQty }) {
  const params = {
    symbol,
    side,
    type: 'MARKET',
    // One of these must be provided depending on side
    quantity,
    quoteOrderQty,
  };
  return signedPost(http, '/api/v3/order', params);
}
