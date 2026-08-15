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
    monte_carlo_passed: bool = False
    monte_carlo_runs: int = 0
    monte_carlo_positive_share: float | None = None
    forward_shadow_net_after_cost: float | None = None
    forward_shadow_position_count: int = 0
    forward_shadow_cost_stress_passed: bool = False
    walk_forward_passed: bool = False
    walk_forward_positive_folds: int = 0
    stressed_walk_forward_positive_folds: int = 0
    walk_forward_total_folds: int = 0
    final_holdout_net_after_cost: float | None = None
    stressed_final_holdout_net_after_cost: float | None = None
    oos_max_drawdown_pct: float | None = None
    stressed_oos_max_drawdown_pct: float | None = None
    independent_position_count: int = 0
    observed_regime_count: int = 0
    max_regime_profit_share: float | None = None
    parameter_perturbation_count: int = 0
    parameter_sensitivity_avg_change_pct: float | None = None
    parameter_sensitivity_max_change_pct: float | None = None
    capacity_sample_count: int = 0
    maximum_capacity_participation_rate: float | None = None
    oos_outlier_position_count: int | None = None
    oos_net_after_best_position_removed: float | None = None
    stressed_oos_net_after_best_position_removed: float | None = None
    oos_liquidation_count: int | None = None
    stressed_oos_liquidation_count: int | None = None
    promotion_evidence_schema: int = 0
    optimizer_strategy: str | None = None
    optimizer_run_id: str | None = None
    dataset_fingerprint: str | None = None
    optimizer_candidate_fingerprint: str | None = None
    promotion_source_bundle_fingerprint: str | None = None


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


def _canonical_sha256(value) -> str | None:
    if not isinstance(value, str) or len(value) != 64:
        return None
    return value if all(char in "0123456789abcdef" for char in value) else None


def _canonical_strategy(value) -> str | None:
    if not isinstance(value, str) or not 1 <= len(value) <= 32:
        return None
    allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
    if value[0] not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        return None
    return value if all(char in allowed for char in value) else None


def evaluate_promotion(
    evidence: PromotionEvidence,
    *,
    minimum_samples: int,
    minimum_shadow_days: int = 30,
    minimum_shadow_positions: int = 30,
    minimum_monte_carlo_runs: int = 100,
    minimum_monte_carlo_positive_share: float = 0.90,
    minimum_walk_forward_folds: int = 4,
    maximum_oos_drawdown_pct: float = 10.0,
    minimum_independent_positions: int = 300,
    minimum_observed_regimes: int = 2,
    maximum_regime_profit_share: float = 0.80,
    minimum_parameter_perturbations: int = 2,
    maximum_parameter_avg_change_pct: float = 15.0,
    maximum_parameter_change_pct: float = 30.0,
    minimum_capacity_samples: int = 50,
    maximum_capacity_participation_rate: float = 0.10,
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
    minimum_shadow_positions = _positive_integer(
        minimum_shadow_positions, "minimum_shadow_positions"
    )
    minimum_monte_carlo_runs = _positive_integer(
        minimum_monte_carlo_runs, "minimum_monte_carlo_runs"
    )
    minimum_monte_carlo_positive_share = _unit_interval(
        minimum_monte_carlo_positive_share,
        "minimum_monte_carlo_positive_share",
    )
    minimum_walk_forward_folds = _positive_integer(
        minimum_walk_forward_folds, "minimum_walk_forward_folds"
    )
    maximum_oos_drawdown_pct = _control_float(
        maximum_oos_drawdown_pct, "maximum_oos_drawdown_pct"
    )
    if not 0.0 < maximum_oos_drawdown_pct <= 100.0:
        raise ValueError("maximum_oos_drawdown_pct must be between zero and 100")
    minimum_independent_positions = _positive_integer(
        minimum_independent_positions, "minimum_independent_positions"
    )
    minimum_observed_regimes = _positive_integer(
        minimum_observed_regimes, "minimum_observed_regimes"
    )
    maximum_regime_profit_share = _unit_interval(
        maximum_regime_profit_share, "maximum_regime_profit_share"
    )
    minimum_parameter_perturbations = _positive_integer(
        minimum_parameter_perturbations, "minimum_parameter_perturbations"
    )
    maximum_parameter_avg_change_pct = _control_float(
        maximum_parameter_avg_change_pct,
        "maximum_parameter_avg_change_pct",
    )
    if maximum_parameter_avg_change_pct <= 0.0:
        raise ValueError("maximum_parameter_avg_change_pct must be positive")
    maximum_parameter_change_pct = _control_float(
        maximum_parameter_change_pct, "maximum_parameter_change_pct"
    )
    if maximum_parameter_change_pct <= 0.0:
        raise ValueError("maximum_parameter_change_pct must be positive")
    if maximum_parameter_avg_change_pct > maximum_parameter_change_pct:
        raise ValueError("parameter sensitivity limits must be ordered")
    minimum_capacity_samples = _positive_integer(
        minimum_capacity_samples, "minimum_capacity_samples"
    )
    maximum_capacity_participation_rate = _unit_interval(
        maximum_capacity_participation_rate,
        "maximum_capacity_participation_rate",
    )
    if maximum_capacity_participation_rate <= 0.0:
        raise ValueError("maximum_capacity_participation_rate must be positive")
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
    evidence_schema = _evidence_nonnegative_integer(
        evidence.promotion_evidence_schema
    )
    if evidence_schema != 2:
        reasons.append("promotion evidence schema is invalid")
    if _canonical_strategy(evidence.optimizer_strategy) is None:
        reasons.append("optimizer strategy is invalid")
    if _canonical_sha256(evidence.optimizer_run_id) is None:
        reasons.append("optimizer run id is invalid")
    if _canonical_sha256(evidence.dataset_fingerprint) is None:
        reasons.append("dataset fingerprint is invalid")
    if _canonical_sha256(evidence.optimizer_candidate_fingerprint) is None:
        reasons.append("optimizer candidate fingerprint is invalid")
    if _canonical_sha256(evidence.promotion_source_bundle_fingerprint) is None:
        reasons.append("promotion source bundle fingerprint is invalid")
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
    if evidence.walk_forward_passed is not True:
        reasons.append("walk-forward evidence failed")
    walk_forward_folds = _evidence_nonnegative_integer(
        evidence.walk_forward_total_folds
    )
    positive_folds = _evidence_nonnegative_integer(
        evidence.walk_forward_positive_folds
    )
    if (
        walk_forward_folds is None
        or walk_forward_folds < minimum_walk_forward_folds
    ):
        reasons.append("walk-forward fold count below minimum")
    if (
        positive_folds is None
        or walk_forward_folds is None
        or positive_folds > walk_forward_folds
    ):
        reasons.append("walk-forward positive fold count is invalid")
    elif positive_folds * 2 <= walk_forward_folds:
        reasons.append("walk-forward folds are not predominantly positive")
    stressed_positive_folds = _evidence_nonnegative_integer(
        evidence.stressed_walk_forward_positive_folds
    )
    if (
        stressed_positive_folds is None
        or walk_forward_folds is None
        or stressed_positive_folds > walk_forward_folds
    ):
        reasons.append("stressed walk-forward positive fold count is invalid")
    elif stressed_positive_folds * 2 <= walk_forward_folds:
        reasons.append("stressed walk-forward folds are not predominantly positive")
    holdout_net = _finite(evidence.final_holdout_net_after_cost)
    if holdout_net is None or holdout_net <= 0.0:
        reasons.append("final holdout net after cost is not positive")
    stressed_holdout_net = _finite(evidence.stressed_final_holdout_net_after_cost)
    if stressed_holdout_net is None or stressed_holdout_net <= 0.0:
        reasons.append("stressed final holdout net after cost is not positive")
    drawdown = _finite(evidence.oos_max_drawdown_pct)
    if drawdown is None or drawdown < 0.0:
        reasons.append("OOS max drawdown is invalid")
    elif drawdown > maximum_oos_drawdown_pct:
        reasons.append("OOS max drawdown exceeds limit")
    stressed_drawdown = _finite(evidence.stressed_oos_max_drawdown_pct)
    if stressed_drawdown is None or stressed_drawdown < 0.0:
        reasons.append("stressed OOS max drawdown is invalid")
    elif stressed_drawdown > maximum_oos_drawdown_pct:
        reasons.append("stressed OOS max drawdown exceeds limit")
    if evidence.monte_carlo_passed is not True:
        reasons.append("monte-carlo robustness failed")
    monte_carlo_runs = _evidence_nonnegative_integer(evidence.monte_carlo_runs)
    if (
        monte_carlo_runs is None
        or monte_carlo_runs < minimum_monte_carlo_runs
    ):
        reasons.append("monte-carlo run count below minimum")
    monte_carlo_positive_share = _evidence_unit_interval(
        evidence.monte_carlo_positive_share
    )
    if (
        monte_carlo_positive_share is None
        or monte_carlo_positive_share < minimum_monte_carlo_positive_share
    ):
        reasons.append("monte-carlo positive share below minimum")
    shadow_days = _evidence_nonnegative_integer(evidence.forward_shadow_days)
    if shadow_days is None or shadow_days < minimum_shadow_days:
        reasons.append("forward shadow is too short")
    shadow_net = _finite(evidence.forward_shadow_net_after_cost)
    if shadow_net is None or shadow_net <= 0.0:
        reasons.append("forward shadow net after cost is not positive")
    shadow_positions = _evidence_nonnegative_integer(
        evidence.forward_shadow_position_count
    )
    if shadow_positions is None or shadow_positions < minimum_shadow_positions:
        reasons.append("forward shadow position count below minimum")
    if evidence.forward_shadow_cost_stress_passed is not True:
        reasons.append("forward shadow cost stress failed")
    sample_count = _evidence_nonnegative_integer(evidence.sample_count)
    if sample_count is None or sample_count < minimum_samples:
        reasons.append("sample count below minimum")
    independent_positions = _evidence_nonnegative_integer(
        evidence.independent_position_count
    )
    if (
        independent_positions is None
        or independent_positions < minimum_independent_positions
    ):
        reasons.append("independent position count below minimum")
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
    outlier_positions = _evidence_nonnegative_integer(
        evidence.oos_outlier_position_count
    )
    if outlier_positions is None:
        reasons.append("outlier position count is invalid")
    elif (
        independent_positions is not None
        and outlier_positions != independent_positions
    ):
        reasons.append("outlier position count does not match independent positions")
    outlier_net = _finite(evidence.oos_net_after_best_position_removed)
    if outlier_net is None or outlier_net <= 0.0:
        reasons.append("OOS net after best position removal is not positive")
    stressed_outlier_net = _finite(
        evidence.stressed_oos_net_after_best_position_removed
    )
    if stressed_outlier_net is None or stressed_outlier_net <= 0.0:
        reasons.append(
            "stressed OOS net after best position removal is not positive"
        )
    liquidation_count = _evidence_nonnegative_integer(
        evidence.oos_liquidation_count
    )
    if liquidation_count is None:
        reasons.append("OOS liquidation count is invalid")
    elif liquidation_count:
        reasons.append("OOS liquidations detected")
    stressed_liquidation_count = _evidence_nonnegative_integer(
        evidence.stressed_oos_liquidation_count
    )
    if stressed_liquidation_count is None:
        reasons.append("stressed OOS liquidation count is invalid")
    elif stressed_liquidation_count:
        reasons.append("stressed OOS liquidations detected")
    if evidence.capacity_passed is not True:
        reasons.append("capacity evidence failed")
    capacity_samples = _evidence_nonnegative_integer(evidence.capacity_sample_count)
    if capacity_samples is None or capacity_samples < minimum_capacity_samples:
        reasons.append("capacity sample count below minimum")
    capacity_participation = _finite(
        evidence.maximum_capacity_participation_rate
    )
    if capacity_participation is None or not 0.0 < capacity_participation <= 1.0:
        reasons.append("capacity participation evidence is invalid")
    elif capacity_participation > maximum_capacity_participation_rate:
        reasons.append("capacity participation exceeds maximum")
    if evidence.regime_stability_passed is not True:
        reasons.append("regime stability failed")
    regime_count = _evidence_nonnegative_integer(evidence.observed_regime_count)
    if regime_count is None or regime_count < minimum_observed_regimes:
        reasons.append("observed regime count below minimum")
    regime_share = _evidence_unit_interval(evidence.max_regime_profit_share)
    if regime_share is None:
        reasons.append("regime concentration evidence is invalid")
    elif regime_share > maximum_regime_profit_share:
        reasons.append("regime concentration above maximum")
    if evidence.parameter_stability_passed is not True:
        reasons.append("parameter stability failed")
    perturbation_count = _evidence_nonnegative_integer(
        evidence.parameter_perturbation_count
    )
    if (
        perturbation_count is None
        or perturbation_count < minimum_parameter_perturbations
    ):
        reasons.append("parameter perturbation count below minimum")
    sensitivity_average = _finite(evidence.parameter_sensitivity_avg_change_pct)
    if sensitivity_average is None or sensitivity_average < 0.0:
        reasons.append("parameter sensitivity average is invalid")
    elif sensitivity_average >= maximum_parameter_avg_change_pct:
        reasons.append("parameter sensitivity average is not below maximum")
    sensitivity_maximum = _finite(evidence.parameter_sensitivity_max_change_pct)
    if sensitivity_maximum is None or sensitivity_maximum < 0.0:
        reasons.append("parameter sensitivity maximum is invalid")
    elif sensitivity_maximum >= maximum_parameter_change_pct:
        reasons.append("parameter sensitivity maximum is not below maximum")
    if (
        sensitivity_average is not None
        and sensitivity_average >= 0.0
        and sensitivity_maximum is not None
        and sensitivity_maximum >= 0.0
        and sensitivity_average > sensitivity_maximum
    ):
        reasons.append("parameter sensitivity summary is inconsistent")
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
