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
    reasons = []
    if not evidence.causality_passed:
        reasons.append("causality gate failed")
    coverage = _finite(evidence.coverage)
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
    dsr = _finite(evidence.dsr)
    if dsr is None or dsr < 0.95:
        reasons.append("DSR below 0.95")
    pbo = _finite(evidence.pbo)
    if pbo is None or pbo > 0.25:
        reasons.append("PBO above 0.25")
    concentration = _finite(evidence.max_symbol_profit_share)
    if concentration is None or concentration > 0.25:
        reasons.append("symbol concentration above 25%")
    if not evidence.cost_stress_passed:
        reasons.append("cost stress failed")
    try:
        shadow_days = int(evidence.forward_shadow_days)
    except (TypeError, ValueError, OverflowError):
        shadow_days = -1
    if shadow_days < minimum_shadow_days:
        reasons.append("forward shadow is too short")
    try:
        sample_count = int(evidence.sample_count)
    except (TypeError, ValueError, OverflowError):
        sample_count = -1
    if sample_count < minimum_samples:
        reasons.append("sample count below minimum")
    brier = _finite(evidence.calibration_brier)
    if brier is None:
        reasons.append("calibration Brier score unavailable")
    elif brier > maximum_brier:
        reasons.append("calibration Brier score too high")
    ece = _finite(evidence.calibration_ece)
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
    if not evidence.tail_risk_passed:
        reasons.append("tail-risk evidence failed")
    if not evidence.capacity_passed:
        reasons.append("capacity evidence failed")
    if not evidence.regime_stability_passed:
        reasons.append("regime stability failed")
    if not evidence.parameter_stability_passed:
        reasons.append("parameter stability failed")
    if not evidence.multiple_testing_adjusted:
        reasons.append("multiple-testing adjustment missing")
    try:
        experiment_trials = int(evidence.experiment_trials)
    except (TypeError, ValueError, OverflowError):
        experiment_trials = 0
    if experiment_trials < 1:
        reasons.append("experiment trial count unavailable")
    research_passed = not reasons
    return PromotionDecision(
        research_passed=research_passed,
        live_allowed=research_passed and explicit_manual_live_approval,
        reasons=tuple(reasons),
    )
