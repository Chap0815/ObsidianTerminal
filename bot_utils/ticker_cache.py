"""
bot_utils/ticker_cache.py  Bounded, thread-pooled ticker fetch with cache.

Uses an OrderedDict with move_to_end()/popitem(last=False) for O(1) LRU
eviction, so an eviction (cache_max=500) holds the lock only for a dict
mutation instead of an O(N log N) sort under load.

Public API:
  TickerCache(pool_size=8, in_flight_max=32, fresh_ttl=3.0, stale_max=8.0,
              cache_max=500)
    .get(ex, symbol_full, timeout=5.0)  dict
    .shutdown()  cancel pending work
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutTimeout

from bot_utils.safe_numeric import safe_positive_float


class TickerOverloaded(Exception):
    """Raised when the ticker pool is saturated and a fetch is dropped.
    Callers should treat this like a timeout: try cached, else skip."""


def _normalize_ticker_price(ticker: dict | None) -> dict | None:
    if not isinstance(ticker, dict):
        return None
    price = safe_positive_float(ticker.get("last"), 0.0)
    if price <= 0:
        price = safe_positive_float(ticker.get("close"), 0.0)
    if price <= 0:
        return None
    out = dict(ticker)
    out["last"] = price
    return out


class TickerCache:
    def __init__(self,
                 pool_size: int = 8,
                 in_flight_max: int = 32,
                 fresh_ttl: float = 3.0,
                 stale_max: float = 8.0,
                 rate_limit_stale_max: float = 30.0,
                 rate_limit_backoff: float = 12.0,
                 cache_max: int = 500,
                 thread_name_prefix: str = "ticker"):
        self.fresh_ttl = fresh_ttl
        self.stale_max = stale_max
        self.rate_limit_stale_max = rate_limit_stale_max
        self.rate_limit_backoff = rate_limit_backoff
        self.cache_max = cache_max
        # OrderedDict gives O(1) LRU operations (move_to_end on access,
        # popitem(last=False) on evict).
        self._cache: "OrderedDict[str, tuple]" = OrderedDict()
        self._cache_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(
            max_workers=pool_size,
            thread_name_prefix=thread_name_prefix,
        )
        self._inflight_sem = threading.Semaphore(in_flight_max)
        self._in_flight_max = in_flight_max
        self._rate_limited_until = 0.0

    def _stale(self, symbol_full: str, now: float, max_age: float) -> dict | None:
        with self._cache_lock:
            stale = self._cache.get(symbol_full)
            if stale and (now - stale[0]) < max_age:
                self._cache.move_to_end(symbol_full)
                return stale[1]
        return None

    def _is_rate_limited(self, exc: BaseException) -> bool:
        try:
            from bot_utils.network_retry import is_rate_limited
            return is_rate_limited(exc)
        except Exception:
            s = str(exc).lower()
            return ("429" in s or "too frequent" in s
                    or "too many requests" in s or "rate limit" in s)

    def get(self, ex, symbol_full: str, timeout: float = 5.0,
            critical: bool = False) -> dict:
        """Fetch ticker with caching, timeout, and backpressure.

        ``critical=True`` marks an exit-critical fetch (price for an
        OPEN position): the cross-process API-budget gate is told NOT to
        starve it, so a DB-contention spike can't silently stop stop-loss
        monitoring. Backpressure (pool saturation) and timeout still apply.

        Raises:
          TimeoutError  fetch timed out AND no fresh-enough stale cache
          TickerOverloaded  pool saturated; stale cache also missing
        """
        pre_now = time.monotonic()

        # Fast path: fresh cache hit. Touch LRU position on read.
        with self._cache_lock:
            cached = self._cache.get(symbol_full)
            if cached and (pre_now - cached[0]) < self.fresh_ttl:
                # mark as recently used to keep popular symbols in cache
                # during eviction storms.
                self._cache.move_to_end(symbol_full)
                return cached[1]

        if pre_now < self._rate_limited_until:
            max_age = self.stale_max if critical else self.rate_limit_stale_max
            stale = self._stale(symbol_full, pre_now, max_age)
            if stale is not None:
                return stale
            raise TickerOverloaded(
                f"ticker fetch in rate-limit backoff for {symbol_full}"
            )

        # Acquire backpressure permit BEFORE submitting
        if not self._inflight_sem.acquire(timeout=0.2):
            with self._cache_lock:
                stale = self._cache.get(symbol_full)
                if stale and (pre_now - stale[0]) < self.stale_max:
                    self._cache.move_to_end(symbol_full)
                    return stale[1]
            raise TickerOverloaded(
                f"ticker pool saturated ({self._in_flight_max} in flight)  "
                f"dropping fetch for {symbol_full}"
            )

        try:
            # Atomic cross-process budget gate BEFORE the fetch (not just
            # record_api_call() after) so bot subprocesses can't collectively
            # overshoot the limit  429/IP-ban. On exhausted budget: serve the
            # freshest stale cache (exits still get a price < stale_max), else skip.
            try:
                from bot_utils.api_budget import try_consume_api_call
                _allowed = try_consume_api_call("fetch_ticker", critical=critical)
            except Exception:
                _allowed = True  # Budget module unavailable; do not block.
            if not _allowed:
                with self._cache_lock:
                    stale = self._cache.get(symbol_full)
                    if stale and (pre_now - stale[0]) < self.stale_max:
                        self._cache.move_to_end(symbol_full)
                        return stale[1]
                raise TickerOverloaded(
                    f"API budget exhausted (cross-process)  dropping "
                    f"fetch for {symbol_full}, no fresh cache available"
                )

            future = self._pool.submit(ex.fetch_ticker, symbol_full)
            try:
                ticker = future.result(timeout=timeout) or {}
            except _FutTimeout:
                try:
                    future.cancel()
                except Exception:
                    pass
                with self._cache_lock:
                    stale = self._cache.get(symbol_full)
                    if stale and (pre_now - stale[0]) < self.stale_max:
                        self._cache.move_to_end(symbol_full)
                        return stale[1]
                raise TimeoutError(
                    f"fetch_ticker({symbol_full}) timed out after {timeout}s "
                    f"and no fresh cache (<{self.stale_max}s) available"
                )
            except Exception as e:
                if self._is_rate_limited(e):
                    self._rate_limited_until = (
                        time.monotonic() + self.rate_limit_backoff
                    )
                    max_age = self.stale_max if critical else self.rate_limit_stale_max
                    stale = self._stale(symbol_full, pre_now, max_age)
                    if stale is not None:
                        return stale
                raise

            # Timestamp AFTER fetch completes (so cache TTL reflects
            # actual data freshness, not when we submitted the job).
            post_now = time.monotonic()
            ticker = _normalize_ticker_price(ticker)
            if ticker is None:
                stale = self._stale(symbol_full, post_now, self.stale_max)
                if stale is not None:
                    return stale
                raise ValueError(f"invalid ticker price for {symbol_full}")
            with self._cache_lock:
                # insert/overwrite + LRU bookkeeping in O(1).
                if symbol_full in self._cache:
                    # Re-insert to bump LRU position. OrderedDict
                    # documents that __setitem__ on an existing key
                    # does NOT change order, so we must delete first.
                    del self._cache[symbol_full]
                self._cache[symbol_full] = (post_now, ticker)
                # Evict oldest while over cap  O(1) each.
                while len(self._cache) > self.cache_max:
                    self._cache.popitem(last=False)
            return ticker
        finally:
            self._inflight_sem.release()

    def shutdown(self) -> None:
        """Tear down the pool. Cancels queued work, doesn't wait for running.

        Workers are NOT daemon threads, so without this a stuck DNS/TLS
        operation keeps Python alive after the main thread exits (zombie
        process surviving SIGTERM).
        """
        try:
            self._pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
