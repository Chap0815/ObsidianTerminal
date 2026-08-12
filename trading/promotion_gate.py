"""Shared fail-closed promotion gates for profit experiments."""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class PromotionEvidence:
    causality_passed: bool
    coverage: float
    oos_net: float
    profit_factor: float
    confidence_lower_bound: float
    dsr: float
    pbo: float
    max_symbol_profit_share: float
    cost_stress_passed: bool
    forward_shadow_days: int
    sample_count: int
    calibration_brier: float | None = None
    calibration_ece: float | None = None
    calibration_slope: float | None = None
    tail_risk_passed: bool = False
    capacity_passed: bool = False
    regime_stability_passed: bool = False
    parameter_stability_passed: bool = False
    multiple_testing_adjusted: bool = False
    experiment_trials: int = 0


@dataclass(frozen=True)
class PromotionDecision:
    research_passed: bool
    live_allowed: bool
    reasons: tuple[str, ...]


def _finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _control_float(value, name: str) -> float:
    number = _finite(value)
    if number is None:
        raise ValueError(f"{name} must be finite")
    return number


def _positive_integer(value, name: str) -> int:
    number = _control_float(value, name)
    if number <= 0.0 or not number.is_integer():
        raise ValueError(f"{name} must be a positive integer")
    return int(number)


def _unit_interval(value, name: str) -> float:
    number = _control_float(value, name)
    if not 0.0 <= number <= 1.0:
        raise ValueError(f"{name} must be between zero and one")
    return number


def _evidence_nonnegative_integer(value) -> int | None:
    number = _finite(value)
    if number is None or number < 0.0 or not number.is_integer():
        return None
    return int(number)


def _evidence_unit_interval(value) -> float | None:
    number = _finite(value)
    if number is None or not 0.0 <= number <= 1.0:
        return None
    return number


def evaluate_promotion(
    evidence: PromotionEvidence,
    *,
    minimum_samples: int,
    minimum_shadow_days: int = 30,
    explicit_manual_live_approval: bool = False,
    maximum_brier: float = 0.25,
    maximum_ece: float = 0.10,
    minimum_calibration_slope: float = 0.80,
    maximum_calibration_slope: float = 1.20,
) -> PromotionDecision:
    minimum_samples = _positive_integer(minimum_samples, "minimum_samples")
    minimum_shadow_days = _positive_integer(
        minimum_shadow_days, "minimum_shadow_days"
    )
    maximum_brier = _unit_interval(maximum_brier, "maximum_brier")
    maximum_ece = _unit_interval(maximum_ece, "maximum_ece")
    minimum_calibration_slope = _control_float(
        minimum_calibration_slope, "minimum_calibration_slope"
    )
    maximum_calibration_slope = _control_float(
        maximum_calibration_slope, "maximum_calibration_slope"
    )
    if minimum_calibration_slope <= 0.0:
        raise ValueError("minimum_calibration_slope must be positive")
    if maximum_calibration_slope <= 0.0:
        raise ValueError("maximum_calibration_slope must be positive")
    if minimum_calibration_slope > maximum_calibration_slope:
        raise ValueError("calibration_slope range must be ordered")
    if not isinstance(explicit_manual_live_approval, bool):
        raise ValueError("explicit_manual_live_approval must be boolean")
    reasons = []
    if evidence.causality_passed is not True:
        reasons.append("causality gate failed")
    coverage = _evidence_unit_interval(evidence.coverage)
    if coverage is None or coverage < 0.95:
        reasons.append("coverage below 95%")
    oos_net = _finite(evidence.oos_net)
    if oos_net is None or oos_net <= 0.0:
        reasons.append("OOS net is not positive")
    profit_factor = _finite(evidence.profit_factor)
    if profit_factor is None or profit_factor < 1.20:
        reasons.append("profit factor below 1.20")
    confidence_lower_bound = _finite(evidence.confidence_lower_bound)
    if confidence_lower_bound is None or confidence_lower_bound <= 0.0:
        reasons.append("clustered confidence lower bound is not positive")
    dsr = _evidence_unit_interval(evidence.dsr)
    if dsr is None or dsr < 0.95:
        reasons.append("DSR below 0.95")
    pbo = _evidence_unit_interval(evidence.pbo)
    if pbo is None or pbo > 0.25:
        reasons.append("PBO above 0.25")
    concentration = _evidence_unit_interval(evidence.max_symbol_profit_share)
    if concentration is None or concentration > 0.25:
        reasons.append("symbol concentration above 25%")
    if evidence.cost_stress_passed is not True:
        reasons.append("cost stress failed")
    shadow_days = _evidence_nonnegative_integer(evidence.forward_shadow_days)
    if shadow_days is None or shadow_days < minimum_shadow_days:
        reasons.append("forward shadow is too short")
    sample_count = _evidence_nonnegative_integer(evidence.sample_count)
    if sample_count is None or sample_count < minimum_samples:
        reasons.append("sample count below minimum")
    brier = _evidence_unit_interval(evidence.calibration_brier)
    if brier is None:
        reasons.append("calibration Brier score unavailable")
    elif brier > maximum_brier:
        reasons.append("calibration Brier score too high")
    ece = _evidence_unit_interval(evidence.calibration_ece)
    if ece is None:
        reasons.append("calibration ECE unavailable")
    elif ece > maximum_ece:
        reasons.append("calibration ECE too high")
    slope = _finite(evidence.calibration_slope)
    if slope is None:
        reasons.append("calibration slope unavailable")
    elif not (
        minimum_calibration_slope
        <= slope
        <= maximum_calibration_slope
    ):
        reasons.append("calibration slope outside allowed range")
    if evidence.tail_risk_passed is not True:
        reasons.append("tail-risk evidence failed")
    if evidence.capacity_passed is not True:
        reasons.append("capacity evidence failed")
    if evidence.regime_stability_passed is not True:
        reasons.append("regime stability failed")
    if evidence.parameter_stability_passed is not True:
        reasons.append("parameter stability failed")
    if evidence.multiple_testing_adjusted is not True:
        reasons.append("multiple-testing adjustment missing")
    experiment_trials = _evidence_nonnegative_integer(
        evidence.experiment_trials
    )
    if experiment_trials is None or experiment_trials < 1:
        reasons.append("experiment trial count unavailable")
    research_passed = not reasons
    return PromotionDecision(
        research_passed=research_passed,
        live_allowed=research_passed and explicit_manual_live_approval,
        reasons=tuple(reasons),
    )
