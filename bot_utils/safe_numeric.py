"""
bot_utils/safe_numeric.py  Defensive numeric coercion.

There are many ``float(d.get(...))`` calls across the codebase. ``float(None)``
and ``float("")`` raise TypeError/ValueError; while outer try/except blocks
catch them, a single corrupt entry can silently drop a whole monitor tick or
scan cycle. These helpers normalize the "extract a number or get a sensible
default" pattern (NaN/Inf also map to the default).
"""
from __future__ import annotations

import math
from typing import Any


def safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce ``value`` to a finite float, or return ``default``.

    Handles ``None``, empty string, ``NaN``, ``Infinity`` and non-numeric
    types without raising.
    """
    if value is None or isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not math.isfinite(f):
        return default
    return f


def safe_positive_float(value: Any, default: float = 0.0) -> float:
    """Coerce ``value`` to a finite positive float, or return ``default``.

    Intended for prices, amounts and notionals where zero/negative/boolean
    values must not be treated as valid market data.
    """
    if value is None or isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not math.isfinite(f) or f <= 0:
        return default
    return f


def parse_ohlcv_closes(
    bars: Any,
    *,
    expected_interval_ms: float,
    now_ms: float,
) -> list[float] | None:
    """Return validated closed-candle closes, or reject the OHLCV snapshot.

    Signal histories must remain contiguous: skipping one malformed candle
    silently changes moving-average and lookback windows.  Accept only ccxt's
    list/tuple OHLCV shape, a finite strictly increasing and evenly spaced
    timeline aligned to the requested cadence, and positive finite closes.
    The final row is omitted only when its open time is the current bucket.
    """
    if not isinstance(bars, (list, tuple)):
        return None
    if isinstance(expected_interval_ms, bool):
        return None
    try:
        expected_interval = float(expected_interval_ms)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(expected_interval) or expected_interval <= 0:
        return None
    if isinstance(now_ms, bool):
        return None
    try:
        current_time = float(now_ms)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(current_time) or current_time < 0:
        return None
    current_bucket_start = (
        current_time - (current_time % expected_interval)
    )
    closes: list[float] = []
    previous_timestamp: float | None = None
    candle_interval: float | None = None
    last_index = len(bars) - 1
    for index, bar in enumerate(bars):
        if not isinstance(bar, (list, tuple)) or len(bar) <= 4:
            return None
        raw_timestamp = bar[0]
        if isinstance(raw_timestamp, bool):
            return None
        try:
            timestamp = float(raw_timestamp)
        except (TypeError, ValueError, OverflowError):
            return None
        if (
            not math.isfinite(timestamp)
            or timestamp < 0
            or timestamp % expected_interval != 0
            or timestamp > current_bucket_start
            or (
                previous_timestamp is not None
                and timestamp <= previous_timestamp
            )
        ):
            return None
        if previous_timestamp is not None:
            interval = timestamp - previous_timestamp
            if interval != expected_interval:
                return None
            if candle_interval is None:
                candle_interval = interval
            elif interval != candle_interval:
                return None
        is_forming = index == last_index and timestamp == current_bucket_start
        if not is_forming:
            close = safe_positive_float(bar[4], 0.0)
            if close <= 0:
                return None
            closes.append(close)
        previous_timestamp = timestamp
    return closes


def safe_int(value: Any, default: int = 0) -> int:
    """Coerce ``value`` to an int, or return ``default``.

    Truncates floats. Returns ``default`` on TypeError/ValueError/OverflowError
    and on non-finite inputs (NaN, Inf).
    """
    if value is None or isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    # Pre-filter non-finite values  int() on Inf raises OverflowError,
    # int() on NaN raises ValueError.
    if not math.isfinite(f):
        return default
    try:
        return int(f)
    except (TypeError, ValueError, OverflowError):
        return default


def safe_dict_float(d: dict, key: str, default: float = 0.0) -> float:
    """Read ``d[key]`` as a finite float, with default on missing/bad.

    Equivalent to ``safe_float(d.get(key), default)``  kept as its own
    name because it's THE most common pattern in the bot code.
    """
    if not isinstance(d, dict):
        return default
    return safe_float(d.get(key), default)
