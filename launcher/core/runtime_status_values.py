"""Strict parsing helpers for launcher runtime-status JSON values."""

from __future__ import annotations

import math


def finite_float_or_none(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def positive_int_or_zero(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value if 0 < value <= 0xFFFFFFFF else 0


def nonnegative_int_or_zero(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value if value >= 0 else 0


def strict_bool_or_none(value) -> bool | None:
    return value if isinstance(value, bool) else None
