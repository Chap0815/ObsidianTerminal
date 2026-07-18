"""Fail-closed policies for calibrated entries and time-decay exits."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping


def _mode(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {"disabled", "shadow", "enforce"}:
        raise ValueError(f"invalid experiment mode: {value!r}")
    return normalized


@dataclass(frozen=True)
class LinearExpectancyModel:
    feature_order: tuple[str, ...]
    coefficients: tuple[float, ...]
    intercept: float
    probability_scale: float = 1.0
    probability_intercept: float = 0.0
    version: str = ""
    feature_means: tuple[float, ...] = ()
    feature_scales: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        if not self.version:
            raise ValueError("model version is required")
        if len(self.feature_order) != len(self.coefficients):
            raise ValueError("feature and coefficient lengths differ")
        if self.feature_means and len(self.feature_means) != len(self.feature_order):
            raise ValueError("feature mean length differs")
        if self.feature_scales and len(self.feature_scales) != len(self.feature_order):
            raise ValueError("feature scale length differs")


@dataclass(frozen=True)
class ExpectancyDecision:
    allowed: bool
    shadow_allowed: bool
    expected_net_bps: float | None
    probability_positive: float | None
    model_version: str
    reason: str


def decide_net_expectancy(
    model: LinearExpectancyModel,
    features: Mapping[str, float],
    *,
    mode: str = "shadow",
    min_expected_net_bps: float = 0.0,
    min_probability_positive: float = 0.5,
) -> ExpectancyDecision:
    normalized_mode = _mode(mode)
    try:
        values = [float(features[name]) for name in model.feature_order]
        if not all(math.isfinite(value) for value in values):
            raise ValueError("non-finite feature")
    except (KeyError, TypeError, ValueError):
        allowed = normalized_mode != "enforce"
        return ExpectancyDecision(
            allowed, False, None, None, model.version, "feature coverage incomplete"
        )
    if model.feature_means and model.feature_scales:
        values = [
            (value - mean) / scale
            for value, mean, scale in zip(
                values, model.feature_means, model.feature_scales, strict=True
            )
        ]
    expected = float(model.intercept) + sum(
        coefficient * value
        for coefficient, value in zip(model.coefficients, values, strict=True)
    )
    logit = model.probability_intercept + model.probability_scale * expected
    probability = 1.0 / (1.0 + math.exp(-max(-700.0, min(700.0, logit))))
    shadow_allowed = bool(
        expected > float(min_expected_net_bps)
        and probability >= float(min_probability_positive)
    )
    allowed = shadow_allowed if normalized_mode == "enforce" else True
    reason = "expected net gate passed" if shadow_allowed else "expected net gate failed"
    return ExpectancyDecision(
        allowed, shadow_allowed, expected, probability, model.version, reason
    )


@dataclass(frozen=True)
class TimeDecayDecision:
    should_exit: bool
    shadow_should_exit: bool
    reason: str


def time_decay_decision(
    *,
    age_minutes: float,
    max_age_minutes: float,
    mfe_pct: float,
    min_mfe_pct: float,
    mode: str = "shadow",
) -> TimeDecayDecision:
    normalized_mode = _mode(mode)
    candidate = bool(
        float(max_age_minutes) > 0.0
        and float(age_minutes) >= float(max_age_minutes)
        and float(mfe_pct) < float(min_mfe_pct)
    )
    should_exit = candidate and normalized_mode == "enforce"
    return TimeDecayDecision(
        should_exit,
        candidate,
        "stale low-MFE position" if candidate else "time-decay gate not met",
    )


def position_age_minutes(opened_at, now: datetime | None = None) -> float | None:
    try:
        opened = datetime.fromisoformat(str(opened_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if opened.tzinfo is None:
        opened = opened.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0.0, (current.astimezone(timezone.utc) - opened.astimezone(timezone.utc)).total_seconds() / 60.0)
