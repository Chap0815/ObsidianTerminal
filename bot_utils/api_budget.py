"""
bot_utils/api_budget.py  Cross-process API call budget guard.

Bots share the same API key/IP. Without a shared budget guard each bot
subprocess would count only its own calls and collectively blow past the
exchange rate limit  429 / IP ban. This delegates to
``core.database.check_and_consume_global_api`` (a shared SQLite
``api_rate_global`` table) for true cross-process counting; a process-local
counter is the fallback only when SQLite is unreachable.

The limit (MAX_API_CALLS_PER_MINUTE = 300) leaves headroom for ~150/min per
bot. Configurable via the ``API_BUDGET_PER_MINUTE`` env var (read once at
import).

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
from typing import Optional


def _read_limit_from_env(default: int = 300) -> int:
    raw = os.getenv("API_BUDGET_PER_MINUTE", "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        if v < 10:
            return default
        return v
    except (TypeError, ValueError):
        return default


MAX_API_CALLS_PER_MINUTE = _read_limit_from_env()

# Process-local fallback counter  used only if SQLite is unreachable.
_call_log_fallback: list = []
_lock = threading.Lock()

# Cached bot name to attribute calls in the DB.
_bot_name_cache: Optional[str] = None

# DB-failure throttling: if SQLite repeatedly errors, don't hammer it.
_db_failed_until: float = 0.0
_DB_FAILED_BACKOFF_SEC = 30.0


def _resolve_bot_name() -> str:
    """Read BOT_NAME from env once (set by the launcher per subprocess)."""
    global _bot_name_cache
    if _bot_name_cache is None:
        _bot_name_cache = os.getenv("BOT_NAME", "unknown")
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
        check_and_consume_global_api(
            _resolve_bot_name(),
            endpoint=endpoint or "",
            max_per_minute=10_000_000,
            ok=1,
        )
    except Exception:
        _mark_db_failed()


def record_api_error(endpoint: str = "") -> None:
    """Like ``record_api_call`` but ``ok=0`` for error-rate tracking."""
    now_mono = time.monotonic()
    _fallback_record(now_mono)

    if not _db_available():
        return

    try:
        from core.database import check_and_consume_global_api
        check_and_consume_global_api(
            _resolve_bot_name(),
            endpoint=endpoint or "",
            max_per_minute=10_000_000,
            ok=0,
        )
    except Exception:
        _mark_db_failed()


def budget_remaining() -> int:
    """Approximate API calls remaining in the current 60s window.

    Queries the shared SQLite counter when available. The launcher poller AND
    all bots contribute, so this number reflects the true global budget  not a
    per-process illusion.
    """
    now_mono = time.monotonic()

    if not _db_available():
        used = _fallback_count(now_mono)
        return max(0, MAX_API_CALLS_PER_MINUTE - used)

    try:
        from core.database import _tight_connection
        from datetime import datetime, timezone, timedelta
        # Match the exchange-anchored clock the rows were written with.
        try:
            from core.clock import now_utc as _now_utc
            _win_now = _now_utc()
        except Exception:
            _win_now = datetime.now(timezone.utc)
        cutoff = (_win_now - timedelta(seconds=60)
                   ).strftime("%Y-%m-%d %H:%M:%S")
        conn = _tight_connection()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM api_rate_global WHERE called_at >= ?",
                (cutoff,)
            ).fetchone()
            count = row[0] if row else 0
            return max(0, MAX_API_CALLS_PER_MINUTE - count)
        finally:
            conn.close()
    except Exception:
        _mark_db_failed()
        used = _fallback_count(now_mono)
        return max(0, MAX_API_CALLS_PER_MINUTE - used)


def budget_exhausted() -> bool:
    """True if we're at/over the per-minute budget (cross-process)."""
    return budget_remaining() == 0


#  atomic check-and-consume 

def try_consume_api_call(endpoint: str = "", ok: int = 1,
                         critical: bool = False) -> bool:
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
    now_mono = time.monotonic()

    # Per-process fallback divisor: bot count from API_EXPECTED_BOT_COUNT env
    # (default 3) so the per-process cap matches how many bots actually run.
    try:
        _N_BOTS = max(1, int(os.environ.get("API_EXPECTED_BOT_COUNT", "3")))
    except (TypeError, ValueError):
        _N_BOTS = 3

    if not _db_available():
        # Process-local fallback. Conservative: each process enforces
        # its OWN budget so N bots at MAX/N each  MAX global.
        used = _fallback_count(now_mono)
        per_proc_cap = max(1, MAX_API_CALLS_PER_MINUTE // _N_BOTS)
        if used >= per_proc_cap:
            # exit-critical calls (price for an OPEN position, close/verify)
            # must NOT be starved by the budget  a missed stop-loss is far
            # worse than a marginal over-budget. Record it (keep the count
            # honest) but allow it through.
            if critical:
                _fallback_record(now_mono)
                return True
            return False
        _fallback_record(now_mono)
        return True

    try:
        from core.database import check_and_consume_global_api
        ok_call = check_and_consume_global_api(
            _resolve_bot_name(),
            endpoint=endpoint or "",
            max_per_minute=MAX_API_CALLS_PER_MINUTE,
            ok=ok,
        )
        if ok_call:
            # Mirror to fallback so a sudden DB outage still has recent
            # data to estimate from.
            _fallback_record(now_mono)
            return True
        # budget exhausted (or lock-timeout fail-closed in the DB gate).
        # Let exit-critical calls proceed regardless.
        if critical:
            _fallback_record(now_mono)
            return True
        return False
    except Exception:
        _mark_db_failed()
        # On unexpected exception, prefer fail-open with a per-proc
        # cap so trading continues during transient DB issues.
        used = _fallback_count(now_mono)
        per_proc_cap = max(1, MAX_API_CALLS_PER_MINUTE // _N_BOTS)
        if used >= per_proc_cap:
            if critical:   # never starve exit-critical calls
                _fallback_record(now_mono)
                return True
            return False
        _fallback_record(now_mono)
        return True

