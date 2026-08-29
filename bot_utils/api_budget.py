"""
bot_utils/api_budget.py  Cross-process API call budget guard.

Bots share the same API key/IP. Without a shared budget guard each bot
subprocess would count only its own calls and collectively blow past the
exchange rate limit  429 / IP ban. This delegates to
``core.database.check_and_consume_global_api`` (a shared SQLite
``api_rate_global`` table) for true cross-process counting; a process-local
counter is the fallback only when SQLite is unreachable.

The limit (MAX_API_CALLS_PER_MINUTE = 300) is shared by all five bot processes
and the launcher. It is configurable via the ``API_BUDGET_PER_MINUTE`` env var
(read once at import).

``try_consume_api_call(endpoint)`` does an atomic ``BEGIN IMMEDIATE``
check-and-consume  use it instead of the two-step ``if not budget_exhausted():
record_api_call()`` pattern, which races between the SELECT and the INSERT
(two bots both see a free slot and both consume):

    if not try_consume_api_call("fetch_ticker"):
        skip_this_tick()
    else:
        ex.fetch_ticker(...)

The older ``budget_remaining`` / ``budget_exhausted`` / ``record_api_call``
helpers are kept for diagnostics / back-compat.
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

from shared_limits import API_RATE_HARD_MAX_PER_MINUTE


def _read_limit_from_env(default: int = 300) -> int:
    raw = os.getenv("API_BUDGET_PER_MINUTE", "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        if not 10 <= v <= API_RATE_HARD_MAX_PER_MINUTE:
            return default
        return v
    except (TypeError, ValueError):
        return default


_DEFAULT_API_CONSUMER_COUNT = 6  # five bot subprocesses plus the launcher


def _read_expected_bot_count(default: int = _DEFAULT_API_CONSUMER_COUNT) -> int:
    try:
        count = int(os.environ.get("API_EXPECTED_BOT_COUNT", str(default)))
    except (TypeError, ValueError, OverflowError):
        return default
    # The deployment always has the five bot processes plus the launcher as
    # potential API consumers.  An understated override would multiply the
    # effective cluster allowance during a SQLite outage (each process applies
    # MAX/count locally), defeating the conservative fallback contract.
    return max(default, count) if count >= 1 else default


MAX_API_CALLS_PER_MINUTE = _read_limit_from_env()


def _validated_endpoint(value) -> str:
    if not isinstance(value, str):
        raise ValueError("endpoint must be text")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("endpoint contains control characters")
    endpoint = value.strip()
    if len(endpoint) > 256:
        raise ValueError("endpoint exceeds 256 characters")
    return endpoint


def _validated_ok(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value not in (0, 1):
        raise ValueError("ok must be 0 or 1")
    return value


# Process-local fallback counter  used only if SQLite is unreachable.
_call_log_fallback: list = []
_lock = threading.Lock()

# Cached bot name to attribute calls in the DB.
_bot_name_cache: Optional[str] = None

# DB-failure throttling: if SQLite repeatedly errors, don't hammer it.
_db_failed_until: float = 0.0
_DB_FAILED_BACKOFF_SEC = 30.0


@dataclass(frozen=True)
class ApiCallReservation:
    """One admission decision plus the optional durable ledger row id."""

    allowed: bool
    row_id: Optional[int] = None

    def __bool__(self) -> bool:
        return self.allowed


def _resolve_bot_name() -> str:
    """Read BOT_NAME from env once (set by the launcher per subprocess)."""
    global _bot_name_cache
    if _bot_name_cache is None:
        raw = os.getenv("BOT_NAME", "")
        candidate = raw.strip().upper()
        if (
            not candidate
            or len(candidate) > 64
            or any(ord(char) < 32 or ord(char) == 127 for char in raw)
        ):
            candidate = "UNKNOWN"
        _bot_name_cache = candidate
    return _bot_name_cache


def _fallback_record(now: float) -> None:
    with _lock:
        _call_log_fallback.append(now)
        cutoff = now - 60.0
        _call_log_fallback[:] = [t for t in _call_log_fallback if t >= cutoff]


def _fallback_count(now: float) -> int:
    with _lock:
        cutoff = now - 60.0
        return sum(1 for t in _call_log_fallback if t >= cutoff)


def _fallback_remaining(now: float) -> int:
    """Conservative local share while the global ledger is unavailable."""
    expected_consumers = _read_expected_bot_count()
    per_process_cap = max(
        1, MAX_API_CALLS_PER_MINUTE // expected_consumers
    )
    return max(0, per_process_cap - _fallback_count(now))


def _db_available() -> bool:
    return time.monotonic() >= _db_failed_until


def _mark_db_failed() -> None:
    global _db_failed_until
    _db_failed_until = time.monotonic() + _DB_FAILED_BACKOFF_SEC


def record_api_call(endpoint: str = "") -> None:
    """Call this whenever the bot issues an exchange API request.

    Writes to the shared SQLite ``api_rate_global`` table so all bots and the
    launcher see a unified count. Falls back to a process-local counter only if
    SQLite is unreachable for >30s.
    """
    endpoint = _validated_endpoint(endpoint)
    now_mono = time.monotonic()
    _fallback_record(now_mono)

    if not _db_available():
        return

    try:
        from core.database import check_and_consume_global_api
        # Used as record-only: ``max_per_minute`` is set very high so
        # the consume always succeeds. The real gate is ``budget_exhausted()``
        # which queries the count via a SELECT  this avoids races where
        # two bots both pass the gate and both consume.
        result = check_and_consume_global_api(
            _resolve_bot_name(),
            endpoint=endpoint,
            max_per_minute=10_000_000,
            ok=1,
        )
        if result is None:
            _mark_db_failed()
    except Exception:
        _mark_db_failed()


def record_api_error(
    endpoint: str = "",
    reservation: Optional[ApiCallReservation] = None,
) -> None:
    """Like ``record_api_call`` but ``ok=0`` for error-rate tracking."""
    endpoint = _validated_endpoint(endpoint)
    if reservation is not None and not isinstance(
        reservation, ApiCallReservation
    ):
        raise ValueError("reservation must be an ApiCallReservation or None")
    now_mono = time.monotonic()
    already_counted = bool(reservation and reservation.allowed)

    # A successful admission already consumed both the durable and fallback
    # slot. Reclassify that row instead of recording the same network request
    # a second time.
    if reservation is not None and reservation.row_id is not None:
        if not _db_available():
            return
        try:
            from core.database import mark_global_api_call_error
            marked = mark_global_api_call_error(
                reservation.row_id,
                _resolve_bot_name(),
                endpoint=endpoint,
            )
            if marked is None:
                _mark_db_failed()
                return
            if marked:
                return
            # Preserve one-call/one-row accounting even if the reserved row
            # disappeared or SQLite was briefly locked. Under-reporting this
            # outcome is safer than consuming the budget twice.
            return
        except Exception:
            _mark_db_failed()
            return

    if not already_counted:
        _fallback_record(now_mono)

    if not _db_available():
        return

    try:
        from core.database import check_and_consume_global_api
        result = check_and_consume_global_api(
            _resolve_bot_name(),
            endpoint=endpoint,
            max_per_minute=10_000_000,
            ok=0,
        )
        if result is None:
            _mark_db_failed()
    except Exception:
        _mark_db_failed()


def budget_remaining() -> int:
    """Approximate API calls remaining in the current 60s window.

    Queries the shared SQLite counter when available. The launcher poller AND
    all bots contribute, so this number reflects the true global budget  not a
    per-process illusion. During a DB outage it reports this process's
    conservative share, matching ``try_consume_api_call``.
    """
    now_mono = time.monotonic()

    if not _db_available():
        return _fallback_remaining(now_mono)

    try:
        from core.database import _tight_connection
        from datetime import datetime, timezone, timedelta
        from core.constants import (
            API_LEDGER_CLOCK_ROLLBACK_TOLERANCE_SECONDS,
        )
        # Match the exchange-anchored clock the rows were written with.
        try:
            from core.clock import now_utc as _now_utc
            _win_now = _now_utc()
        except Exception:
            _win_now = datetime.now(timezone.utc)
        cutoff = (_win_now - timedelta(seconds=60)
                   ).strftime("%Y-%m-%d %H:%M:%S")
        latest_recent = (
            _win_now
            + timedelta(
                seconds=API_LEDGER_CLOCK_ROLLBACK_TOLERANCE_SECONDS
            )
        ).strftime("%Y-%m-%d %H:%M:%S")
        conn = _tight_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM api_rate_global "
            "WHERE called_at >= ? AND called_at <= ?",
            (cutoff, latest_recent),
        ).fetchone()
        count = row[0] if row else 0
        return max(0, MAX_API_CALLS_PER_MINUTE - count)
    except Exception:
        _mark_db_failed()
        return _fallback_remaining(now_mono)


def budget_exhausted() -> bool:
    """True if we're at/over the per-minute budget (cross-process)."""
    return budget_remaining() == 0


#  atomic check-and-consume 

def try_consume_api_call(endpoint: str = "", ok: int = 1,
                         critical: bool = False,
                         return_reservation: bool = False):
    """Atomically check the global budget AND record the call in one
    SQLite transaction (BEGIN IMMEDIATE).

    Returns True if the call may proceed (the slot has been consumed),
    False if the cluster-wide budget is exhausted.

    Use this INSTEAD of the two-step ``if not budget_exhausted(): ...
    record_api_call()`` pattern  the two-step version has a race
    between SELECT and INSERT where two bots can both pass the gate
    and then both consume.

    Falls back to a process-local counter on DB outage. The fallback
    is best-effort and CAN over-count across processes during the
    outage; this is intentional  we'd rather refuse some calls than
    over-fire and trigger an IP ban.
    """
    endpoint = _validated_endpoint(endpoint)
    ok = _validated_ok(ok)
    if not isinstance(critical, bool):
        raise ValueError("critical must be boolean")
    if not isinstance(return_reservation, bool):
        raise ValueError("return_reservation must be boolean")

    def _result(allowed: bool, row_id: Optional[int] = None):
        if return_reservation:
            return ApiCallReservation(bool(allowed), row_id)
        return bool(allowed)

    now_mono = time.monotonic()

    # Per-process fallback divisor: consumer count from
    # API_EXPECTED_BOT_COUNT. The default includes all five bot subprocesses
    # plus the launcher, which also performs budgeted exchange calls.
    expected_bot_count = _read_expected_bot_count()

    if not _db_available():
        # Process-local fallback. Conservative: each process enforces
        # its OWN budget so N bots at MAX/N each  MAX global.
        used = _fallback_count(now_mono)
        per_proc_cap = max(1, MAX_API_CALLS_PER_MINUTE // expected_bot_count)
        if used >= per_proc_cap:
            # exit-critical calls (price for an OPEN position, close/verify)
            # must NOT be starved by the budget  a missed stop-loss is far
            # worse than a marginal over-budget. Record it (keep the count
            # honest) but allow it through.
            if critical:
                _fallback_record(now_mono)
                return _result(True)
            return _result(False)
        _fallback_record(now_mono)
        return _result(True)

    try:
        from core.database import check_and_consume_global_api
        ok_call = check_and_consume_global_api(
            _resolve_bot_name(),
            endpoint=endpoint or "",
            max_per_minute=MAX_API_CALLS_PER_MINUTE,
            ok=ok,
            return_reservation=True,
            critical=critical,
        )
        if ok_call is None:
            raise RuntimeError("global API budget gate unavailable")
        if ok_call:
            # Mirror to fallback so a sudden DB outage still has recent
            # data to estimate from.
            _fallback_record(now_mono)
            row_id = (
                int(ok_call)
                if not isinstance(ok_call, bool) and int(ok_call) > 0
                else None
            )
            return _result(True, row_id)
        # The durable gate records critical bypasses even above the normal cap,
        # so False here is a genuine non-critical budget denial.
        return _result(False)
    except Exception:
        _mark_db_failed()
        # On unexpected exception, prefer fail-open with a per-proc
        # cap so trading continues during transient DB issues.
        used = _fallback_count(now_mono)
        per_proc_cap = max(1, MAX_API_CALLS_PER_MINUTE // expected_bot_count)
        if used >= per_proc_cap:
            if critical:   # never starve exit-critical calls
                _fallback_record(now_mono)
                return _result(True)
            return _result(False)
        _fallback_record(now_mono)
        return _result(True)
