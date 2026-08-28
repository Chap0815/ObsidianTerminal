"""Validated age-gated MFE fallback for regular FUTURES positions."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


MFE_FALLBACK_EXIT_REASON = "Aged MFE Fallback Stop"
DEFAULT_MIN_AGE_MINUTES = 45.0
DEFAULT_MIN_MFE_PCT = 0.8
DEFAULT_EXIT_MOVE_PCT = -1.5
MIN_AGE_MINUTES = 5.0
MAX_AGE_MINUTES = 1440.0
MIN_MFE_PCT = 0.1
MAX_MFE_PCT = 5.0
MIN_EXIT_MOVE_PCT = -10.0
MAX_EXIT_MOVE_PCT = -0.25


@dataclass(frozen=True)
class MfeFallbackConfig:
    enabled: bool
    min_age_minutes: float
    min_mfe_pct: float
    exit_move_pct: float


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _boolean(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
    return None


def _utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validate_mfe_fallback_config(
    *,
    enabled: Any,
    min_age_minutes: Any,
    min_mfe_pct: Any,
    exit_move_pct: Any,
    initial_stop_loss_pct: Any,
) -> tuple[MfeFallbackConfig | None, str]:
    """Return normalized settings or a stable fail-closed error."""
    parsed_enabled = _boolean(enabled)
    if parsed_enabled is None:
        return None, "enabled flag must be boolean"
    if not parsed_enabled:
        return MfeFallbackConfig(False, 0.0, 0.0, 0.0), ""

    age = _finite(min_age_minutes)
    mfe = _finite(min_mfe_pct)
    exit_move = _finite(exit_move_pct)
    initial_stop = _finite(initial_stop_loss_pct)
    if None in {age, mfe, exit_move, initial_stop}:
        return None, "age, MFE, fallback and initial stop must be finite numbers"
    if not MIN_AGE_MINUTES <= age <= MAX_AGE_MINUTES:
        return None, "minimum age must be within 5-1440 minutes"
    if not MIN_MFE_PCT <= mfe <= MAX_MFE_PCT:
        return None, "minimum MFE must be within 0.1-5%"
    if not MIN_EXIT_MOVE_PCT <= exit_move <= MAX_EXIT_MOVE_PCT:
        return None, "fallback move must be within -10--0.25%"
    if initial_stop >= 0.0:
        return None, "initial stop must be negative"
    return MfeFallbackConfig(True, age, mfe, exit_move), ""


def mfe_fallback_hit(
    *,
    buy_time: Any,
    move_pct: Any,
    mfe_pct: Any,
    config: MfeFallbackConfig,
    now: datetime | None = None,
) -> bool:
    """Return whether an owned pre-partial position reached the fallback."""
    if not config.enabled:
        return False
    opened = _utc_datetime(buy_time)
    move = _finite(move_pct)
    mfe = _finite(mfe_pct)
    if now is None:
        try:
            from core.clock import now_utc

            current = now_utc()
        except Exception:
            return False
    else:
        current = now
    if opened is None or move is None or mfe is None:
        return False
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    age_minutes = (current - opened).total_seconds() / 60.0
    if not math.isfinite(age_minutes) or age_minutes < config.min_age_minutes:
        return False
    return mfe >= config.min_mfe_pct and move <= config.exit_move_pct
