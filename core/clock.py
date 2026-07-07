"""
core/clock.py  Exchange-anchored wall clock (single source of truth for "now").

The bot runs worldwide and must NOT trust the local OS clock for absolute time:
a drifting local clock caused a live close-failure incident. The exchange layer
already syncs the localserver offset for request signing; it also publishes
that offset here via ``set_exchange_offset_ms`` so the bot's OWN timestamps and
date logic use exchange-anchored time. Until an offset is known (or if the
exchange is unreachable) every getter falls back to the local clock, so this is
always safe to call  including from sub-tools and tests with no connection.

Only ABSOLUTE wall-clock readings need this. Relative durations (cooldowns,
throttles, intervals) keep using ``time.time()`` directly  a constant offset
cancels out, so drift can't affect them.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

_lock = threading.Lock()
_offset_ms: float = 0.0  # exchange_time  local_time, in milliseconds
_have_offset: bool = False


def set_exchange_offset_ms(offset_ms: float) -> None:
    """Publish the localexchange offset (exchange  local, in ms).

    Called by the exchange layer after each (re)sync. Ignores non-numeric input
    so a bad value can never poison the clock."""
    global _offset_ms, _have_offset
    try:
        off = float(offset_ms)
    except (TypeError, ValueError):
        return
    with _lock:
        _offset_ms = off
        _have_offset = True


def get_offset_ms() -> float:
    """Current exchange offset in ms (0.0 if none known yet)."""
    with _lock:
        return _offset_ms if _have_offset else 0.0


def have_offset() -> bool:
    """True once the exchange layer has published an offset."""
    with _lock:
        return _have_offset


def now_ms() -> float:
    """Exchange-anchored epoch milliseconds (local + published offset)."""
    return time.time() * 1000.0 + get_offset_ms()


def now_utc() -> datetime:
    """Exchange-anchored timezone-aware UTC datetime."""
    return datetime.fromtimestamp(now_ms() / 1000.0, tz=timezone.utc)


def utc_now_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Exchange-anchored UTC timestamp string (default '%Y-%m-%d %H:%M:%S')."""
    return now_utc().strftime(fmt)


def backtest_asof_ms():
    """Optional backtest data cutoff in epoch ms, or None when unset.

    Set the env var ``BACKTEST_ASOF=YYYY-MM-DD`` to make every backtest data
    fetch behave as if "now" were the END of that UTC day and drop any later
    candle. This enforces the In-Sample/Out-of-Sample wall structurally: the
    optimizer runs WITH it set so it can never see the locked OOS window; the
    risk/red-team runs WITHOUT it (full history). Empty/invalid  None (no cut).
    Affects only the backtest fetchers that consult it  never live trading."""
    raw = (os.getenv("BACKTEST_ASOF") or "").strip()
    if not raw:
        return None
    try:
        d = datetime.strptime(raw, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc)
        return int(d.timestamp() * 1000)
    except ValueError:
        return None
