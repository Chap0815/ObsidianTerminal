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
        self._shutdown_lock = threading.Lock()
        self._shutdown_complete = False
        self._futures_lock = threading.Lock()
        self._futures = set()
        self._inflight_sem = threading.Semaphore(in_flight_max)
        self._in_flight_max = in_flight_max
        self._symbol_locks_lock = threading.Lock()
        # Values are ``[lock, registered_callers]``.  Entries exist only while
        # a caller owns or waits for the coalescing lock; idle symbols must not
        # accumulate forever as a rotating market universe is scanned.
        self._symbol_locks: dict[tuple[int, str], list] = {}
        self._rate_limited_until = 0.0
        self._stats_lock = threading.Lock()
        self._stats = {
            "requests": 0,
            "fresh_hits": 0,
            "stale_hits": 0,
            "fetch_attempts": 0,
            "fetch_success": 0,
            "fetch_errors": 0,
            "invalid_payloads": 0,
            "budget_denied": 0,
            "overloaded": 0,
            "fetch_latency_ms_total": 0.0,
            "fetch_latency_ms_max": 0.0,
            "late_fetch_success": 0,
            "late_invalid_payloads": 0,
            "late_fetch_superseded": 0,
            "consecutive_fetch_errors": 0,
            "first_consecutive_error_wall_ts": None,
            "last_fetch_success_wall_ts": None,
            "last_fetch_error_wall_ts": None,
            "consecutive_unavailable_requests": 0,
            "first_unavailable_request_wall_ts": None,
            "last_ticker_success_wall_ts": None,
            "last_ticker_unavailable_wall_ts": None,
        }
        self._last_fetch_success_monotonic: float | None = None
        self._consecutive_error_started_monotonic: float | None = None
        self._last_ticker_success_monotonic: float | None = None
        self._unavailable_request_started_monotonic: float | None = None

    def _note(self, key: str, value: float = 1.0) -> None:
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + value

    def stats(self) -> dict:
        """Return a thread-safe aggregate snapshot without resetting it."""
        now = time.monotonic()
        with self._stats_lock:
            data = dict(self._stats)
            last_success = self._last_fetch_success_monotonic
            error_started = self._consecutive_error_started_monotonic
            last_ticker_success = self._last_ticker_success_monotonic
            unavailable_started = self._unavailable_request_started_monotonic
        attempts = int(data.get("fetch_attempts") or 0)
        total_ms = float(data.pop("fetch_latency_ms_total", 0.0) or 0.0)
        data["fetch_latency_ms_avg"] = (
            total_ms / attempts if attempts else 0.0)
        data["cache_hit_rate"] = (
            (float(data.get("fresh_hits") or 0)
             + float(data.get("stale_hits") or 0))
            / float(data.get("requests") or 1)
        )
        data["fetch_success_age_seconds"] = (
            max(0.0, now - last_success) if last_success is not None else None
        )
        data["consecutive_error_age_seconds"] = (
            max(0.0, now - error_started) if error_started is not None else None
        )
        data["ticker_success_age_seconds"] = (
            max(0.0, now - last_ticker_success)
            if last_ticker_success is not None
            else None
        )
        data["unavailable_request_age_seconds"] = (
            max(0.0, now - unavailable_started)
            if unavailable_started is not None
            else None
        )
        return data

    def health(
        self,
        *,
        failure_threshold: int = 8,
        success_stale_after: float = 30.0,
    ) -> dict:
        """Return client-visible ticker health without penalizing idle caches.

        A cache is unhealthy only when both a sufficiently long consecutive
        unavailable-request streak and a sufficiently old last delivered
        ticker are present. Aggregate historical fetch errors alone never
        degrade readiness when callers still receive valid cached data.
        """
        threshold = max(1, int(failure_threshold))
        stale_after = max(0.0, float(success_stale_after))
        snapshot = self.stats()
        requests = int(snapshot.get("requests") or 0)
        attempts = int(snapshot.get("fetch_attempts") or 0)
        unavailable = int(
            snapshot.get("consecutive_unavailable_requests") or 0
        )
        fetch_errors = int(snapshot.get("consecutive_fetch_errors") or 0)
        ticker_success_age = snapshot.get("ticker_success_age_seconds")
        unavailable_age = snapshot.get("unavailable_request_age_seconds")
        fetch_success_age = snapshot.get("fetch_success_age_seconds")
        fetch_error_age = snapshot.get("consecutive_error_age_seconds")
        delivery_outage_age = (
            ticker_success_age
            if ticker_success_age is not None
            else unavailable_age
        )
        fetch_outage_age = (
            fetch_success_age
            if fetch_success_age is not None
            else fetch_error_age
        )
        delivery_outage = bool(
            requests > 0
            and unavailable >= threshold
            and delivery_outage_age is not None
            and float(delivery_outage_age) >= stale_after
        )
        fetch_outage = bool(
            attempts > 0
            and fetch_errors >= threshold
            and fetch_outage_age is not None
            and float(fetch_outage_age) >= stale_after
        )
        unhealthy = delivery_outage or fetch_outage
        if requests <= 0 and attempts <= 0:
            state = "idle"
        elif unhealthy:
            state = "outage"
        elif unavailable:
            state = "unavailable_streak"
        elif fetch_errors:
            state = "fetch_error_streak"
        else:
            state = "healthy"
        if delivery_outage and fetch_outage:
            reason = "fetch_and_delivery_outage"
        elif delivery_outage:
            reason = "ticker_delivery_outage"
        elif fetch_outage:
            reason = "fetch_source_outage"
        else:
            reason = ""
        return {
            "ok": not unhealthy,
            "component": "ticker_cache",
            "state": state,
            "reason": reason,
            "consecutive_unavailable_requests": unavailable,
            "consecutive_fetch_errors": fetch_errors,
            "failure_threshold": threshold,
            "success_stale_after_seconds": stale_after,
            "ticker_success_age_seconds": ticker_success_age,
            "unavailable_request_age_seconds": unavailable_age,
            "fetch_success_age_seconds": fetch_success_age,
            "consecutive_error_age_seconds": fetch_error_age,
            "last_ticker_success_wall_ts": snapshot.get(
                "last_ticker_success_wall_ts"
            ),
            "last_ticker_unavailable_wall_ts": snapshot.get(
                "last_ticker_unavailable_wall_ts"
            ),
        }

    def _note_stale_hit(self) -> None:
        self._note("stale_hits")

    def _note_request_result(self, *, ok: bool) -> None:
        with self._stats_lock:
            completed_at = time.monotonic()
            completed_wall_ts = time.time()
            if ok:
                self._stats["consecutive_unavailable_requests"] = 0
                self._stats["first_unavailable_request_wall_ts"] = None
                self._stats["last_ticker_success_wall_ts"] = completed_wall_ts
                self._last_ticker_success_monotonic = completed_at
                self._unavailable_request_started_monotonic = None
            else:
                if not self._stats["consecutive_unavailable_requests"]:
                    self._stats[
                        "first_unavailable_request_wall_ts"
                    ] = completed_wall_ts
                    self._unavailable_request_started_monotonic = completed_at
                self._stats["consecutive_unavailable_requests"] += 1
                self._stats[
                    "last_ticker_unavailable_wall_ts"
                ] = completed_wall_ts

    def _note_fetch_result(self, started_at: float, *, ok: bool) -> None:
        with self._stats_lock:
            completed_at = time.monotonic()
            completed_wall_ts = time.time()
            elapsed_ms = max(0.0, (completed_at - started_at) * 1000.0)
            key = "fetch_success" if ok else "fetch_errors"
            self._stats[key] += 1
            self._stats["fetch_latency_ms_total"] += elapsed_ms
            self._stats["fetch_latency_ms_max"] = max(
                self._stats["fetch_latency_ms_max"], elapsed_ms)
            if ok:
                self._stats["consecutive_fetch_errors"] = 0
                self._stats["first_consecutive_error_wall_ts"] = None
                self._stats["last_fetch_success_wall_ts"] = completed_wall_ts
                self._last_fetch_success_monotonic = completed_at
                self._consecutive_error_started_monotonic = None
            else:
                if not self._stats["consecutive_fetch_errors"]:
                    self._stats[
                        "first_consecutive_error_wall_ts"
                    ] = completed_wall_ts
                    self._consecutive_error_started_monotonic = completed_at
                self._stats["consecutive_fetch_errors"] += 1
                self._stats["last_fetch_error_wall_ts"] = completed_wall_ts

    def _note_late_fetch_success(self) -> None:
        """Reset current source-outage state without double-counting an attempt."""
        with self._stats_lock:
            completed_at = time.monotonic()
            completed_wall_ts = time.time()
            self._stats["late_fetch_success"] += 1
            self._stats["consecutive_fetch_errors"] = 0
            self._stats["first_consecutive_error_wall_ts"] = None
            self._stats["last_fetch_success_wall_ts"] = completed_wall_ts
            self._last_fetch_success_monotonic = completed_at
            self._consecutive_error_started_monotonic = None

    def _stale(self, symbol_full: str, now: float, max_age: float) -> dict | None:
        with self._cache_lock:
            stale = self._cache.get(symbol_full)
            if stale and (now - stale[0]) < max_age:
                self._cache.move_to_end(symbol_full)
                return stale[1]
        return None

    def _store(
        self,
        symbol_full: str,
        ticker: dict,
        stored_at: float,
        *,
        superseded_after: float | None = None,
    ) -> bool:
        with self._cache_lock:
            current = self._cache.get(symbol_full)
            if (
                superseded_after is not None
                and current is not None
                and current[0] >= superseded_after
            ):
                return False
            if symbol_full in self._cache:
                del self._cache[symbol_full]
            self._cache[symbol_full] = (stored_at, ticker)
            while len(self._cache) > self.cache_max:
                self._cache.popitem(last=False)
        return True

    def _cache_late_ticker(
        self,
        symbol_full: str,
        future,
        fetch_started_at: float,
    ) -> None:
        """Retain a valid result that arrived after the caller timed out."""
        try:
            ticker = _normalize_ticker_price(future.result() or {})
        except Exception:
            return
        if ticker is None:
            self._note("late_invalid_payloads")
            return
        # Record source recovery before touching the cache. This preserves
        # completion ordering if another request fails while the late result
        # is competing with a newer cache write.
        self._note_late_fetch_success()
        stored = self._store(
            symbol_full,
            ticker,
            time.monotonic(),
            superseded_after=fetch_started_at,
        )
        if not stored:
            self._note("late_fetch_superseded")

    def _is_rate_limited(self, exc: BaseException) -> bool:
        try:
            from bot_utils.network_retry import is_rate_limited
            return is_rate_limited(exc)
        except Exception:
            s = str(exc).lower()
            return ("429" in s or "too frequent" in s
                    or "too many requests" in s or "rate limit" in s)

    def get(self, ex, symbol_full: str, timeout: float = 5.0,
            critical: bool = False,
            allow_extended_rate_limit_stale: bool = True) -> dict:
        """Fetch ticker with caching, timeout, and backpressure.

        ``critical=True`` marks an exit-critical fetch (price for an
        OPEN position): the cross-process API-budget gate is told NOT to
        starve it, so a DB-contention spike can't silently stop stop-loss
        monitoring. Backpressure (pool saturation) and timeout still apply.

        Raises:
          TimeoutError  fetch timed out AND no fresh-enough stale cache
          TickerOverloaded  pool saturated; stale cache also missing
        """
        self._note("requests")
        fetch_key = (id(ex), symbol_full)
        with self._symbol_locks_lock:
            lock_entry = self._symbol_locks.get(fetch_key)
            if lock_entry is None:
                lock_entry = [threading.Lock(), 0]
                self._symbol_locks[fetch_key] = lock_entry
            lock_entry[1] += 1
            symbol_lock = lock_entry[0]
        try:
            # Concurrent monitor/reconcile callers for one symbol share the
            # first completed fetch via the cache instead of multiplying the
            # same exchange request and its rate-limit pressure.
            with symbol_lock:
                ticker = self._get(
                    ex,
                    symbol_full,
                    timeout=timeout,
                    critical=critical,
                    allow_extended_rate_limit_stale=(
                        allow_extended_rate_limit_stale
                    ),
                )
        except Exception:
            self._note_request_result(ok=False)
            raise
        else:
            self._note_request_result(ok=True)
            return ticker
        finally:
            with self._symbol_locks_lock:
                current = self._symbol_locks.get(fetch_key)
                if current is lock_entry:
                    lock_entry[1] -= 1
                    if lock_entry[1] <= 0:
                        self._symbol_locks.pop(fetch_key, None)

    def _get(self, ex, symbol_full: str, timeout: float = 5.0,
             critical: bool = False,
             allow_extended_rate_limit_stale: bool = True) -> dict:
        if not isinstance(allow_extended_rate_limit_stale, bool):
            raise ValueError(
                "allow_extended_rate_limit_stale must be boolean"
            )
        pre_now = time.monotonic()

        # Fast path: fresh cache hit. Touch LRU position on read.
        with self._cache_lock:
            cached = self._cache.get(symbol_full)
            if cached and (pre_now - cached[0]) < self.fresh_ttl:
                # mark as recently used to keep popular symbols in cache
                # during eviction storms.
                self._cache.move_to_end(symbol_full)
                self._note("fresh_hits")
                return cached[1]

        if pre_now < self._rate_limited_until:
            max_age = (
                self.rate_limit_stale_max
                if not critical and allow_extended_rate_limit_stale
                else self.stale_max
            )
            stale = self._stale(symbol_full, time.monotonic(), max_age)
            if stale is not None:
                self._note_stale_hit()
                return stale
            raise TickerOverloaded(
                f"ticker fetch in rate-limit backoff for {symbol_full}"
            )

        # Acquire backpressure permit BEFORE submitting
        if not self._inflight_sem.acquire(timeout=0.2):
            stale = self._stale(
                symbol_full, time.monotonic(), self.stale_max
            )
            if stale is not None:
                self._note_stale_hit()
                return stale
            self._note("overloaded")
            raise TickerOverloaded(
                f"ticker pool saturated ({self._in_flight_max} in flight)  "
                f"dropping fetch for {symbol_full}"
            )

        permit_owned_by_caller = True
        api_reservation = None

        def _record_fetch_api_error() -> None:
            try:
                from bot_utils.api_budget import (
                    ApiCallReservation,
                    record_api_error,
                )

                if isinstance(api_reservation, ApiCallReservation):
                    record_api_error("fetch_ticker", api_reservation)
            except Exception:
                # Health accounting must never replace the causal ticker
                # exception or disturb price-protection fallback behavior.
                pass

        try:
            # Atomic cross-process budget gate BEFORE the fetch (not just
            # record_api_call() after) so bot subprocesses can't collectively
            # overshoot the limit  429/IP-ban. On exhausted budget: serve the
            # freshest stale cache (exits still get a price < stale_max), else skip.
            try:
                from bot_utils.api_budget import try_consume_api_call
                api_reservation = try_consume_api_call(
                    "fetch_ticker",
                    critical=critical,
                    return_reservation=True,
                )
            except Exception:
                api_reservation = None
            if not api_reservation:
                self._note("budget_denied")
                stale = self._stale(
                    symbol_full, time.monotonic(), self.stale_max
                )
                if stale is not None:
                    self._note_stale_hit()
                    return stale
                raise TickerOverloaded(
                    f"API budget exhausted (cross-process)  dropping "
                    f"fetch for {symbol_full}, no fresh cache available"
                )

            self._note("fetch_attempts")
            fetch_started_at = time.monotonic()
            with self._shutdown_lock:
                future = self._pool.submit(ex.fetch_ticker, symbol_full)
                with self._futures_lock:
                    self._futures.add(future)

                def _release_future(completed) -> None:
                    with self._futures_lock:
                        self._futures.discard(completed)
                    self._inflight_sem.release()

            # A timed-out Future may already be running, in which case
            # cancel() cannot stop it. Keep the backpressure permit attached
            # to the actual work item until it really finishes.
            try:
                future.add_done_callback(_release_future)
            except Exception:
                with self._futures_lock:
                    self._futures.discard(future)
                try:
                    future.cancel()
                except Exception:
                    pass
                raise
            permit_owned_by_caller = False
            try:
                ticker = future.result(timeout=timeout) or {}
            except _FutTimeout:
                _record_fetch_api_error()
                self._note_fetch_result(fetch_started_at, ok=False)
                cancelled = False
                try:
                    cancelled = future.cancel()
                except Exception:
                    pass
                if not cancelled:
                    future.add_done_callback(
                        lambda completed, symbol=symbol_full,
                        started=fetch_started_at:
                        self._cache_late_ticker(symbol, completed, started)
                    )
                stale = self._stale(
                    symbol_full, time.monotonic(), self.stale_max
                )
                if stale is not None:
                    self._note_stale_hit()
                    return stale
                raise TimeoutError(
                    f"fetch_ticker({symbol_full}) timed out after {timeout}s "
                    f"and no fresh cache (<{self.stale_max}s) available"
                )
            except Exception as e:
                _record_fetch_api_error()
                self._note_fetch_result(fetch_started_at, ok=False)
                if self._is_rate_limited(e):
                    self._rate_limited_until = (
                        time.monotonic() + self.rate_limit_backoff
                    )
                    max_age = (
                        self.rate_limit_stale_max
                        if not critical and allow_extended_rate_limit_stale
                        else self.stale_max
                    )
                    stale = self._stale(
                        symbol_full, time.monotonic(), max_age
                    )
                    if stale is not None:
                        self._note_stale_hit()
                        return stale
                raise

            # Timestamp AFTER fetch completes (so cache TTL reflects
            # actual data freshness, not when we submitted the job).
            post_now = time.monotonic()
            ticker = _normalize_ticker_price(ticker)
            if ticker is None:
                _record_fetch_api_error()
                self._note("invalid_payloads")
                self._note_fetch_result(fetch_started_at, ok=False)
                stale = self._stale(symbol_full, post_now, self.stale_max)
                if stale is not None:
                    self._note_stale_hit()
                    return stale
                raise ValueError(f"invalid ticker price for {symbol_full}")
            self._note_fetch_result(fetch_started_at, ok=True)
            self._store(
                symbol_full,
                ticker,
                post_now,
                superseded_after=fetch_started_at,
            )
            return ticker
        finally:
            if permit_owned_by_caller:
                self._inflight_sem.release()

    def shutdown(self) -> bool:
        """Tear down the pool. Cancels queued work, doesn't wait for running.

        Workers are NOT daemon threads, so without this a stuck DNS/TLS
        operation keeps Python alive after the main thread exits. Return
        ``False`` while such work is still running so lifecycle owners can
        retry after their core-thread join.
        """
        with self._shutdown_lock:
            if self._shutdown_complete:
                return True
            try:
                self._pool.shutdown(wait=False, cancel_futures=True)
            except Exception as exc:
                try:
                    from bot_utils.silent_log import silent_log
                    silent_log("ticker cache shutdown", exc)
                except Exception:
                    pass
                return False
            with self._futures_lock:
                pending = [future for future in self._futures if not future.done()]
            if pending:
                try:
                    from bot_utils.silent_log import silent_log
                    silent_log(
                        "ticker cache shutdown",
                        RuntimeError(
                            f"{len(pending)} ticker fetch(es) still running"
                        ),
                    )
                except Exception:
                    pass
                return False
            self._shutdown_complete = True
            return True
