"""Pure FUTURES peak-trail validation and execution metrics."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from core.constants import DEFAULT_TAKER_FEE


PEAK_TRAIL_EXIT_REASON = "Pre-Activation Giveback Stop"
DEFAULT_ACTIVATION_MFE_PCT = 1.5
DEFAULT_GIVEBACK_PCT = 0.75
EXECUTION_SLIPPAGE_BUFFER_PCT = 0.10
MIN_LOCKED_MOVE_PCT = round(
    2.0 * DEFAULT_TAKER_FEE * 100.0 + EXECUTION_SLIPPAGE_BUFFER_PCT, 8)


@dataclass(frozen=True)
class PeakTrailConfig:
    enabled: bool
    activation_mfe_pct: float
    giveback_pct: float
    locked_move_pct: float


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


def validate_peak_trail_config(
    *,
    enabled: Any,
    activation_mfe_pct: Any,
    giveback_pct: Any,
) -> tuple[PeakTrailConfig | None, str]:
    """Return a normalized config or a stable rejection reason."""
    parsed_enabled = _boolean(enabled)
    if parsed_enabled is None:
        return None, "enabled flag must be boolean"
    if not parsed_enabled:
        return PeakTrailConfig(False, 0.0, 0.0, 0.0), ""

    activation = _finite(activation_mfe_pct)
    giveback = _finite(giveback_pct)
    if activation is None or giveback is None:
        return None, "activation and giveback must be finite numbers"
    if activation <= 0.0 or giveback <= 0.0 or giveback >= activation:
        return None, "requires activation > giveback > 0"

    locked = activation - giveback
    if locked + 1e-12 < MIN_LOCKED_MOVE_PCT:
        return None, (
            f"minimum locked move is {MIN_LOCKED_MOVE_PCT:.2f}% after "
            "round-trip fees and slippage buffer"
        )
    return PeakTrailConfig(True, activation, giveback, locked), ""


def peak_trail_hit(
    *, move_pct: Any, mfe_pct: Any, config: PeakTrailConfig,
) -> bool:
    if not config.enabled:
        return False
    move = _finite(move_pct)
    mfe = _finite(mfe_pct)
    if move is None or mfe is None:
        return False
    return (
        mfe >= config.activation_mfe_pct
        and (mfe - move) >= config.giveback_pct
    )


def build_execution_metrics(
    *,
    position_type: Any,
    decision_price: Any,
    fill_price: Any,
    decision_move_pct: Any,
    fill_move_pct: Any,
    mfe_pct: Any,
    latency_ms: Any,
) -> dict[str, float]:
    """Build direction-aware decision-to-fill metrics for a verified close."""
    direction = str(position_type or "").upper()
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("position_type must be LONG or SHORT")
    values = {
        "decision_price": _finite(decision_price),
        "fill_price": _finite(fill_price),
        "decision_move_pct": _finite(decision_move_pct),
        "fill_move_pct": _finite(fill_move_pct),
        "mfe_pct": _finite(mfe_pct),
        "execution_latency_ms": _finite(latency_ms),
    }
    if any(value is None for value in values.values()):
        raise ValueError("execution metrics require finite numeric values")
    if values["decision_price"] <= 0.0 or values["fill_price"] <= 0.0:
        raise ValueError("execution prices must be positive")
    if values["execution_latency_ms"] < 0.0:
        raise ValueError("execution latency must be non-negative")

    decision = values["decision_price"]
    fill = values["fill_price"]
    signed_delta = decision - fill if direction == "LONG" else fill - decision
    values["adverse_slippage_bps"] = signed_delta / decision * 10_000.0
    values["decision_giveback_pct"] = max(
        0.0, values["mfe_pct"] - values["decision_move_pct"])
    values["fill_giveback_pct"] = max(
        0.0, values["mfe_pct"] - values["fill_move_pct"])
    return {key: round(value, 6) for key, value in values.items()}
