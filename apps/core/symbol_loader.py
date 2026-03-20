"""
FastSymbolLoader: Optimized symbol loading with caching and filtering
Previously filtered to USDT pairs only; now supports bridge quotes (USDT, BTC, ETH, BNB, FDUSD, USDC)
to enable triangular routes that require cross pairs like CFXBTC, ETHBTC, etc.
"""
from typing import Dict, Optional
from binance.spot import Spot as BinanceClient
from arbbot import settings as S
import time
import logging
import json
import redis

logger = logging.getLogger(__name__)

_SYMBOL_STATUS_KEY = "arbbot:symbols:status"


def _get_redis():
    url = getattr(S, "REDIS_URL", "")
    if not url:
        return None
    try:
        return redis.from_url(url, decode_responses=True)
    except Exception:
        return None


def _set_symbol_status(ok: bool, message: str):
    client = _get_redis()
    if not client:
        return
    payload = {"ok": ok, "message": message, "ts": time.time()}
    try:
        client.set(_SYMBOL_STATUS_KEY, json.dumps(payload), ex=3600)
    except Exception:
        pass


def get_symbol_status():
    client = _get_redis()
    if not client:
        return None
    try:
        raw = client.get(_SYMBOL_STATUS_KEY)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


class FastSymbolLoader:
    """Optimized symbol loader with caching and bridge-quote filtering"""
    
    def __init__(self, cache_ttl_seconds: int = 300):
        """
        Initialize FastSymbolLoader
        
        Args:
            cache_ttl_seconds: Cache time-to-live in seconds (default: 300 = 5 minutes)
        """
        self.cache_ttl = cache_ttl_seconds
        self._cache: Optional[Dict[str, dict]] = None
        self._cache_timestamp: float = 0.0
    
    def _filter_and_extract(
        self, 
        exchange_info: dict, 
        base_asset: str = "USDT",
        include_all_pairs: bool = False
    ) -> Dict[str, dict]:
        """
        Filter symbols and extract only needed fields.
        Filters out invalid pairs (e.g., USDT:USDT) and ensures comprehensive coverage.
        
        Args:
            exchange_info: Full exchange_info response from Binance
            base_asset: Base asset to filter for (default: USDT)
            include_all_pairs: If True, include all trading pairs, not just base_asset pairs
            
        Returns:
            Dict mapping symbol -> {symbol, baseAsset, quoteAsset, status}
        """
        filtered = {}
        base_upper = base_asset.upper()
        # Bridge quotes we want to keep to build triangles: include common quotes
        bridge_quotes = {base_upper, "BTC", "ETH", "BNB", "FDUSD", "USDC"}
        invalid_count = 0
        
        for symbol_data in exchange_info.get("symbols", []):
            # Only include TRADING symbols
            if symbol_data.get("status") != "TRADING":
                continue
            
            symbol = symbol_data.get("symbol", "").strip()
            if not symbol:
                continue
            
            base_asset_symbol = symbol_data.get("baseAsset", "").upper().strip()
            quote_asset_symbol = symbol_data.get("quoteAsset", "").upper().strip()
            
            # Filter out invalid pairs where base == quote (e.g., USDT:USDT)
            if not base_asset_symbol or not quote_asset_symbol or base_asset_symbol == quote_asset_symbol:
                invalid_count += 1
                continue
            
            if include_all_pairs:
                # Include all trading pairs (status already checked, invalid pairs filtered)
                filtered[symbol] = {
                    "symbol": symbol,
                    "baseAsset": base_asset_symbol,
                    "quoteAsset": quote_asset_symbol,
                    "status": symbol_data.get("status", "TRADING")
                }
            else:
                # Keep any pair whose quote is one of our bridge quotes
                # This includes USDT pairs and cross pairs quoted in BTC/ETH/BNB/FDUSD/USDC
                if quote_asset_symbol in bridge_quotes:
                    filtered[symbol] = {
                        "symbol": symbol,
                        "baseAsset": base_asset_symbol,
                        "quoteAsset": quote_asset_symbol,
                        "status": symbol_data.get("status", "TRADING")
                    }
        
        if invalid_count > 0:
            logger.debug(f"Filtered out {invalid_count} invalid pairs (baseAsset == quoteAsset)")
        
        return filtered
    
    def load_symbols(
        self, 
        client: BinanceClient, 
        base_asset: str = "USDT",
        use_cache: bool = True,
        include_all_pairs: bool = False,
    ) -> Dict[str, dict]:
        """
        Load symbols with caching and USDT filtering
        
        Args:
            client: Binance client instance
            base_asset: Base asset to filter for (default: USDT)
            use_cache: Whether to use cache (default: True)
            
        Returns:
            Dict mapping symbol -> {symbol, baseAsset, quoteAsset, status}
        """
        # Check cache first
        if use_cache and self._cache is not None:
            age = time.time() - self._cache_timestamp
            if age < self.cache_ttl:
                logger.debug(f"Using cached symbols (age: {age:.1f}s, count: {len(self._cache)})")
                return self._cache.copy()
        
        # Try to fetch from API with retries
        backoff = 0.5
        last_err: Optional[Exception] = None
        
        for attempt in range(3):
            try:
                logger.debug(f"Fetching exchange_info (attempt {attempt + 1}/3)")
                start_time = time.time()
                exchange_info = client.exchange_info()
                fetch_time = time.time() - start_time
                
                # Filter and extract USDT pairs + cross pairs (e.g., ETHBTC)
                filtered = self._filter_and_extract(exchange_info, base_asset, include_all_pairs=include_all_pairs)
                filter_time = time.time() - start_time - fetch_time
                
                # Update cache
                self._cache = filtered
                self._cache_timestamp = time.time()
                
                logger.info(
                    f"Loaded {len(filtered)} symbols with quotes in { {'USDT','BTC','ETH','BNB','FDUSD','USDC'} } "
                    f"in {fetch_time:.2f}s (filtered in {filter_time:.3f}s, cached for {self.cache_ttl}s)"
                )
                _set_symbol_status(True, "OK")
                return filtered.copy()
                
            except Exception as e:
                last_err = e
                error_msg = str(e).lower()
                if "timeout" in error_msg or "timed out" in error_msg:
                    logger.warning(
                        f"Exchange info fetch timed out (attempt {attempt + 1}/3). "
                        f"Consider using PROXY_URL in .env if network is slow."
                    )
                elif "connection" in error_msg and ("reset" in error_msg or "aborted" in error_msg):
                    logger.warning(
                        f"Connection reset by remote host (attempt {attempt + 1}/3). "
                        f"This may be due to network restrictions. Consider using PROXY_URL in .env."
                    )
                else:
                    logger.warning(f"Exchange info fetch failed (attempt {attempt + 1}/3): {e}")
                if attempt < 2:  # Don't sleep on last attempt
                    time.sleep(backoff)
                    backoff *= 2
        
        # If all attempts failed, do not use fallback
        logger.error(
            "All API attempts failed; no symbols loaded. Last error: %s",
            last_err,
        )
        _set_symbol_status(False, "Symbols API failed; no fallback in use")
        self._cache = None
        self._cache_timestamp = 0.0
        return {}
    
    def clear_cache(self):
        """Clear the symbol cache"""
        self._cache = None
        self._cache_timestamp = 0.0
        logger.debug("Symbol cache cleared")


# Global instance for reuse across scans
_loader_instance: Optional[FastSymbolLoader] = None


def get_symbol_loader(cache_ttl_seconds: int = 300) -> FastSymbolLoader:
    """Get or create global FastSymbolLoader instance"""
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = FastSymbolLoader(cache_ttl_seconds=cache_ttl_seconds)
    return _loader_instance
