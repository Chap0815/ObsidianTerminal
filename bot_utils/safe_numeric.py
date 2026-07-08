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
