"""Fail-closed policies for calibrated entries and time-decay exits."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

from shared_limits import normalize_gate_mode


def _mode(value: str) -> str:
    return normalize_gate_mode(value)


def _finite_number(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


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
        if not isinstance(self.version, str) or not self.version.strip():
            raise ValueError("model version is required")
        feature_order = tuple(self.feature_order)
        if (
            not feature_order
            or any(
                not isinstance(name, str) or not name.strip()
                for name in feature_order
            )
            or len(set(feature_order)) != len(feature_order)
        ):
            raise ValueError("feature names must be non-empty and unique")

        def _finite(value, label: str) -> float:
            if isinstance(value, bool):
                raise ValueError(f"{label} must be finite")
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"{label} must be finite") from exc
            if not math.isfinite(parsed):
                raise ValueError(f"{label} must be finite")
            return parsed

        coefficients = tuple(
            _finite(value, "coefficient") for value in self.coefficients
        )
        if len(feature_order) != len(coefficients):
            raise ValueError("feature and coefficient lengths differ")
        means = tuple(_finite(value, "feature mean") for value in self.feature_means)
        scales = tuple(
            _finite(value, "feature scale") for value in self.feature_scales
        )
        if bool(means) != bool(scales):
            raise ValueError("feature means and scales must be provided together")
        if means and len(means) != len(feature_order):
            raise ValueError("feature mean length differs")
        if scales and len(scales) != len(feature_order):
            raise ValueError("feature scale length differs")
        if any(scale <= 0 for scale in scales):
            raise ValueError("feature scales must be positive")

        object.__setattr__(self, "feature_order", feature_order)
        object.__setattr__(self, "coefficients", coefficients)
        object.__setattr__(self, "intercept", _finite(self.intercept, "intercept"))
        object.__setattr__(
            self,
            "probability_scale",
            _finite(self.probability_scale, "probability scale"),
        )
        object.__setattr__(
            self,
            "probability_intercept",
            _finite(self.probability_intercept, "probability intercept"),
        )
        object.__setattr__(self, "version", self.version.strip())
        object.__setattr__(self, "feature_means", means)
        object.__setattr__(self, "feature_scales", scales)


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

    def _invalid(reason: str) -> ExpectancyDecision:
        return ExpectancyDecision(
            normalized_mode != "enforce",
            False,
            None,
            None,
            model.version,
            reason,
        )

    try:
        values = [float(features[name]) for name in model.feature_order]
        if not all(math.isfinite(value) for value in values):
            raise ValueError("non-finite feature")
    except (KeyError, TypeError, ValueError, OverflowError):
        return _invalid("feature coverage incomplete")
    if model.feature_means and model.feature_scales:
        values = [
            (value - mean) / scale
            for value, mean, scale in zip(
                values, model.feature_means, model.feature_scales, strict=True
            )
        ]
        if not all(math.isfinite(value) for value in values):
            return _invalid("expectancy arithmetic invalid")
    expected = float(model.intercept) + sum(
        coefficient * value
        for coefficient, value in zip(model.coefficients, values, strict=True)
    )
    if not math.isfinite(expected):
        return _invalid("expectancy arithmetic invalid")
    logit = model.probability_intercept + model.probability_scale * expected
    if not math.isfinite(logit):
        return _invalid("expectancy arithmetic invalid")
    probability = 1.0 / (1.0 + math.exp(-max(-700.0, min(700.0, logit))))
    try:
        min_expected = float(min_expected_net_bps)
        min_probability = float(min_probability_positive)
    except (TypeError, ValueError, OverflowError):
        return _invalid("expectancy threshold invalid")
    if (
        not math.isfinite(min_expected)
        or not math.isfinite(min_probability)
        or not 0.0 <= min_probability <= 1.0
    ):
        return _invalid("expectancy threshold invalid")
    shadow_allowed = bool(
        expected > min_expected
        and probability >= min_probability
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
    age = _finite_number(age_minutes)
    max_age = _finite_number(max_age_minutes)
    mfe = _finite_number(mfe_pct)
    min_mfe = _finite_number(min_mfe_pct)
    if (
        age is None
        or max_age is None
        or mfe is None
        or min_mfe is None
        or age < 0.0
        or max_age < 0.0
    ):
        return TimeDecayDecision(False, False, "time-decay inputs invalid")
    candidate = bool(
        max_age > 0.0
        and age >= max_age
        and mfe < min_mfe
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
