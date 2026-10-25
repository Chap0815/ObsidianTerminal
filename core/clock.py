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

# Independent research-availability authority. Offset publication and signing
# neither create nor clear this measured monotonic upper bound.
_availability_upper_epoch_ms: float | None = None
_availability_lower_epoch_ms: float | None = None
_availability_upper_monotonic: float | None = None
_availability_last_monotonic: float | None = None


def _clear_availability_bound_locked() -> None:
    global _availability_upper_epoch_ms, _availability_upper_monotonic
    global _availability_last_monotonic, _availability_lower_epoch_ms
    _availability_upper_epoch_ms = None
    _availability_lower_epoch_ms = None
    _availability_upper_monotonic = None
    _availability_last_monotonic = None


def _availability_bound_at_locked(monotonic: float) -> tuple[float | None, bool]:
    global _availability_last_monotonic
    if (not math.isfinite(monotonic)
            or (_availability_last_monotonic is not None
                and monotonic < _availability_last_monotonic)):
        _clear_availability_bound_locked()
        return None, False
    if _availability_upper_epoch_ms is None or _availability_upper_monotonic is None:
        return None, True
    elapsed = monotonic - _availability_upper_monotonic
    value = _availability_upper_epoch_ms + elapsed * 1000.0
    if elapsed < 0 or not math.isfinite(value):
        _clear_availability_bound_locked()
        return None, False
    _availability_last_monotonic = monotonic
    return value, True


def publish_availability_upper_bound(server_ms, request_started_monotonic,
                                     received_monotonic) -> bool:
    """Bound receipt UTC by raw server time plus measured full RTT and 1ms.

    The sample may have been taken anywhere during the existing request.
    Publication delay advances monotonically; no request or offset is changed.
    """
    global _availability_upper_epoch_ms, _availability_upper_monotonic
    global _availability_last_monotonic, _availability_lower_epoch_ms
    with _lock:
        try:
            observed = float(time.monotonic())
        except Exception:
            _clear_availability_bound_locked()
            return False
        previous, continuous = _availability_bound_at_locked(observed)
        if not continuous:
            return False
        previous_lower = None
        if (_availability_lower_epoch_ms is not None
                and _availability_upper_monotonic is not None):
            previous_lower = (_availability_lower_epoch_ms
                              + (observed - _availability_upper_monotonic) * 1000.0)
        values = (server_ms, request_started_monotonic, received_monotonic)
        try:
            valid = all(type(value) in (int, float) and math.isfinite(value) for value in values)
        except (OverflowError, TypeError, ValueError):
            valid = False
        if not valid:
            return False
        if received_monotonic < request_started_monotonic or observed < received_monotonic:
            _clear_availability_bound_locked()
            return False
        upper = server_ms + (received_monotonic - request_started_monotonic) * 1000.0 + 1.0
        upper += (observed - received_monotonic) * 1000.0
        # The server sample precedes receipt. Unlike the upper bound, its
        # conservative lower bound must never add the request RTT.
        lower = server_ms - 1.0 + (observed - received_monotonic) * 1000.0
        try:
            if (server_ms <= 0 or not math.isfinite(upper) or not math.isfinite(lower)
                    or (previous_lower is not None and not math.isfinite(previous_lower))):
                return False
            datetime.fromtimestamp(server_ms / 1000.0, tz=timezone.utc)
            datetime.fromtimestamp(upper / 1000.0, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return False
        _availability_upper_epoch_ms = math.ceil(max(upper, previous) if previous is not None else upper)
        _availability_lower_epoch_ms = math.floor(max(lower, previous_lower)
                                                 if previous_lower is not None else lower)
        _availability_upper_monotonic = observed
        _availability_last_monotonic = observed
        return True


def availability_upper_bound_ms() -> int | None:
    """Availability UTC or unknown until an independently measured sample.

    Rollback discards the old authority permanently; the counter catching up
    cannot revive it. A fresh measured request is needed after invalidation.
    """
    with _lock:
        try:
            observed = float(time.monotonic())
        except Exception:
            _clear_availability_bound_locked()
            return None
        value, _continuous = _availability_bound_at_locked(observed)
        return math.ceil(value) if value is not None else None


def availability_time_interval_ms() -> tuple[int, int] | None:
    """Conservative exchange UTC interval for a predecision causal witness.

    A lower bound may limit a historical fitting cutoff; the upper bound may
    attest data availability. Neither is a replacement for the trading clock.
    Both originate in the same measured request and share rollback invalidation.
    """
    with _lock:
        try:
            observed = float(time.monotonic())
        except Exception:
            _clear_availability_bound_locked()
            return None
        upper, _continuous = _availability_bound_at_locked(observed)
        if (upper is None or _availability_lower_epoch_ms is None
                or _availability_upper_monotonic is None):
            return None
        lower = (_availability_lower_epoch_ms
                 + (observed - _availability_upper_monotonic) * 1000.0)
        if not math.isfinite(lower) or lower > upper:
            _clear_availability_bound_locked()
            return None
        return math.floor(lower), math.ceil(upper)


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
    except Exception:
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
    except Exception:
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
    except Exception:
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
        except Exception:
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
    risk/red-team runs WITHOUT it (full history). Empty means no cutoff;
    malformed non-empty values fail closed so a typo cannot expose the locked
    OOS window.
    Affects only the backtest fetchers that consult it  never live trading."""
    raw = (os.getenv("BACKTEST_ASOF") or "").strip()
    if not raw:
        return None
    try:
        d = datetime.strptime(raw, "%Y-%m-%d")
        if d.strftime("%Y-%m-%d") != raw:
            raise ValueError("date is not canonical YYYY-MM-DD")
        d = d.replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
        return int(d.timestamp() * 1000)
    except (ValueError, OSError, OverflowError) as exc:
        raise ValueError(
            "BACKTEST_ASOF must be an exact UTC date in YYYY-MM-DD format"
        ) from exc
