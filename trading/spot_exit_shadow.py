"""Passive SPOT exit counterfactuals.

The rules in this module only produce telemetry.  They never place orders or
change the real SPOT exit decision.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any


SHADOW_VERSION = "spot_exit_shadow_v1"


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _utc_datetime(value: Any) -> datetime | None:
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


def _trigger(
    rule: str,
    family: str,
    age_minutes: float,
    move_pct: float,
    mfe_pct: float,
    mae_pct: float,
    **thresholds: float,
) -> dict[str, Any]:
    return {
        "rule": rule,
        "family": family,
        "version": SHADOW_VERSION,
        "analysis_only": True,
        "age_minutes": round(age_minutes, 4),
        "trigger_move_pct": round(move_pct, 6),
        "mfe_pct": round(mfe_pct, 6),
        "mae_pct": round(mae_pct, 6),
        "giveback_pct": round(max(0.0, mfe_pct - move_pct), 6),
        "thresholds": thresholds,
    }


def evaluate_spot_exit_shadow_rules(
    *,
    buy_time: Any,
    move_pct: Any,
    mfe_pct: Any,
    mae_pct: Any,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return every hypothetical rule reached by the observed price path."""
    opened = _utc_datetime(buy_time)
    move = _finite(move_pct)
    mfe = _finite(mfe_pct)
    mae = _finite(mae_pct)
    if now is None:
        try:
            from core.clock import now_utc

            current = now_utc()
        except Exception:
            current = datetime.now(timezone.utc)
    else:
        current = now
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    if opened is None or move is None or mfe is None or mae is None:
        return []
    age_minutes = (current - opened).total_seconds() / 60.0
    if not math.isfinite(age_minutes) or age_minutes < 0.0:
        return []

    triggered: list[dict[str, Any]] = []
    if age_minutes >= 60.0 and mfe < 0.50 and move <= -2.00:
        triggered.append(_trigger(
            "spot_early_failure_60m",
            "early_failure",
            age_minutes,
            move,
            mfe,
            mae,
            min_age_minutes=60.0,
            max_mfe_pct=0.50,
            exit_move_pct=-2.00,
        ))
    if age_minutes >= 180.0 and mfe < 1.00 and move <= -3.00:
        triggered.append(_trigger(
            "spot_early_failure_180m",
            "early_failure",
            age_minutes,
            move,
            mfe,
            mae,
            min_age_minutes=180.0,
            max_mfe_pct=1.00,
            exit_move_pct=-3.00,
        ))

    giveback = max(0.0, mfe - move)
    if mfe >= 1.50 and giveback >= 1.50:
        triggered.append(_trigger(
            "spot_peak_1p5_trail_1p5",
            "pre_activation_peak_trail",
            age_minutes,
            move,
            mfe,
            mae,
            activation_mfe_pct=1.50,
            trail_distance_pct=1.50,
        ))
    if mfe >= 3.00 and giveback >= 2.00:
        triggered.append(_trigger(
            "spot_peak_3p0_trail_2p0",
            "pre_activation_peak_trail",
            age_minutes,
            move,
            mfe,
            mae,
            activation_mfe_pct=3.00,
            trail_distance_pct=2.00,
        ))
    return triggered
