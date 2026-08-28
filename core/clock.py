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

import math
import os
import threading
import time
from datetime import datetime, timezone

_lock = threading.Lock()
_offset_ms: float = 0.0  # exchange_time  local_time, in milliseconds
_have_offset: bool = False
_offset_set_monotonic: float | None = None
_anchor_epoch_ms: float | None = None
_anchor_monotonic: float | None = None
def set_exchange_offset_ms(offset_ms: float) -> None:
    """Publish the localexchange offset (exchange  local, in ms).

    Called by the exchange layer after each (re)sync. Ignores non-numeric input
    so a bad value can never poison the clock."""
    global _offset_ms, _have_offset, _offset_set_monotonic
    global _anchor_epoch_ms, _anchor_monotonic
    if isinstance(offset_ms, bool):
        return
    try:
        off = float(offset_ms)
    except (TypeError, ValueError, OverflowError):
        return
    if not math.isfinite(off):
        return
    try:
        anchor_epoch_ms = time.time() * 1000.0 + off
        # ``datetime.fromtimestamp(0)`` is representable, but an exchange-time
        # sentinel at/before the Unix epoch must never replace a last-good
        # live clock anchor.  Keep synthetic positive epochs valid for tests.
        if anchor_epoch_ms <= 0.0:
            return
        datetime.fromtimestamp(
            anchor_epoch_ms / 1000.0,
            tz=timezone.utc,
        )
    except (OSError, OverflowError, ValueError):
        return
    try:
        observed_monotonic = float(time.monotonic())
        if not math.isfinite(observed_monotonic):
            observed_monotonic = None
    except (TypeError, ValueError, OverflowError):
        observed_monotonic = None
    with _lock:
        _offset_ms = off
        _have_offset = True
        _offset_set_monotonic = observed_monotonic
        _anchor_epoch_ms = anchor_epoch_ms
        _anchor_monotonic = observed_monotonic


def get_offset_ms() -> float:
    """Current exchange offset in ms (0.0 if none known yet)."""
    with _lock:
        return _offset_ms if _have_offset else 0.0


def have_offset() -> bool:
    """True once the exchange layer has published an offset."""
    with _lock:
        return _have_offset


def get_offset_age_seconds() -> float | None:
    """Monotonic age of the current exchange offset, or ``None`` if unknown."""
    with _lock:
        observed = _offset_set_monotonic if _have_offset else None
    if observed is None:
        return None
    try:
        age = float(time.monotonic()) - observed
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(age):
        return None
    return max(0.0, age)


def now_ms() -> float:
    """Exchange-anchored epoch milliseconds immune to later wall-clock jumps."""
    with _lock:
        anchored = _have_offset
        offset_ms = _offset_ms if anchored else 0.0
        anchor_epoch_ms = _anchor_epoch_ms
        anchor_monotonic = _anchor_monotonic
    if (
        anchored
        and anchor_epoch_ms is not None
        and anchor_monotonic is not None
    ):
        try:
            elapsed = float(time.monotonic()) - anchor_monotonic
            value = anchor_epoch_ms + elapsed * 1000.0
            if elapsed >= 0.0 and math.isfinite(value):
                return value
        except (TypeError, ValueError, OverflowError):
            pass
    return time.time() * 1000.0 + offset_ms


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
