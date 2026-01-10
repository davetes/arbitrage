import axios from 'axios';
import { config } from '../config.js';

export function createBinanceHttp() {
  const instance = axios.create({
    baseURL: config.binanceBaseUrl,
    timeout: 30000,
    proxy: false,
  });

  // Note: axios proxy option doesn't support socks; to keep parity with python
  // we support HTTP(S) proxy via env by letting Node's global agent be configured externally.

  // Binance public endpoints do not require auth for exchangeInfo/depth.
  return instance;
}
