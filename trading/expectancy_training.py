"""True expanding walk-forward training for calibrated net expectancy."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Mapping

import numpy as np

from trading.profit_experiments import LinearExpectancyModel


def _is_boolean(value) -> bool:
    return isinstance(value, (bool, np.bool_))


def _positive_integer(value, name: str) -> int:
    if _is_boolean(value):
        raise ValueError(f"{name} must be a positive integer")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if not math.isfinite(number) or number <= 0.0 or not number.is_integer():
        raise ValueError(f"{name} must be a positive integer")
    return int(number)


def normalize_walk_forward_sizes(min_train, test_size) -> tuple[int, int]:
    normalized_min_train = _positive_integer(min_train, "min_train")
    normalized_test_size = _positive_integer(test_size, "test_size")
    if normalized_min_train < 30:
        raise ValueError("min_train must be at least 30")
    return normalized_min_train, normalized_test_size


def normalize_calibration_controls(
    calibration_fraction, min_calibration
) -> tuple[float, int]:
    if _is_boolean(calibration_fraction):
        raise ValueError(
            "calibration_fraction must be between 0.05 and 0.50"
        )
    try:
        normalized_fraction = float(calibration_fraction)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "calibration_fraction must be between 0.05 and 0.50"
        ) from exc
    if (
        not math.isfinite(normalized_fraction)
        or not 0.05 <= normalized_fraction <= 0.50
    ):
        raise ValueError(
            "calibration_fraction must be between 0.05 and 0.50"
        )
    normalized_minimum = _positive_integer(
        min_calibration, "min_calibration"
    )
    if normalized_minimum < 20:
        raise ValueError("min_calibration must be at least 20")
    return normalized_fraction, normalized_minimum


def normalize_feature_order(feature_order) -> tuple[str, ...]:
    if isinstance(feature_order, (str, bytes)):
        raise ValueError("feature_order must contain unique non-empty strings")
    try:
        normalized = tuple(feature_order)
    except TypeError as exc:
        raise ValueError(
            "feature_order must contain unique non-empty strings"
        ) from exc
    if (
        not normalized
        or any(
            not isinstance(name, str)
            or not name.strip()
            or name != name.strip()
            for name in normalized
        )
        or len(set(normalized)) != len(normalized)
    ):
        raise ValueError("feature_order must contain unique non-empty strings")
    return normalized


class CalibrationDataUnavailableError(ValueError):
    """A temporal split lacks enough disjoint fitting/calibration rows."""


@dataclass(frozen=True)
class CandidateLabel:
    candidate_time: datetime
    label_closed_time: datetime | None
    features: Mapping[str, float]
    net_return_bps: float | None
    label_available_time: datetime | None = None
    decision_lower_time: datetime | None = None


GROUPED_UTC_DAY_POLICY = "entry_utc_day_v1"


@dataclass(frozen=True, kw_only=True)
class GroupedCandidateLabel(CandidateLabel):
    entry_id: str
    cluster_id: str


@dataclass(frozen=True)
class WalkForwardFold:
    test_start: datetime
    test_end: datetime
    training_rows: int
    test_rows: int
    max_training_label_time: datetime
    fitting_rows: int
    calibration_rows: int
    calibration_start: datetime
    max_fitting_label_time: datetime


@dataclass(frozen=True)
class ExpectancyPrediction:
    candidate_time: datetime
    actual_net_bps: float
    predicted_net_bps: float
    probability_positive: float


@dataclass(frozen=True, kw_only=True)
class GroupedExpectancyPrediction(ExpectancyPrediction):
    entry_id: str
    cluster_id: str


@dataclass(frozen=True)
class CalibrationMetrics:
    samples: int
    brier_score: float
    log_loss: float
    ece: float
    slope: float
    intercept: float


@dataclass(frozen=True)
class WalkForwardResult:
    folds: tuple[WalkForwardFold, ...]
    predictions: tuple[ExpectancyPrediction, ...]
    final_model: LinearExpectancyModel
    calibration: CalibrationMetrics


@dataclass(frozen=True)
class SkippedWalkForwardFold:
    test_start: datetime
    test_end: datetime
    test_rows: int
    training_rows: int
    training_cutoff: datetime
    reason: str


@dataclass(frozen=True)
class ForwardWalkForwardResult(WalkForwardResult):
    skipped_unready_folds: tuple[SkippedWalkForwardFold, ...]
    cold_start_policy: str = "skip_unready_calibration_v1"


@dataclass(frozen=True)
class GroupedWalkForwardResult(ForwardWalkForwardResult):
    grouping_policy: str = GROUPED_UTC_DAY_POLICY
    grouping_audit: tuple[dict[str, object], ...] = ()
    final_grouping: dict[str, object] | None = None


def _training_number(value) -> float:
    if _is_boolean(value):
        raise ValueError("training evidence must be finite numeric data")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "training evidence must be finite numeric data"
        ) from exc
    if not math.isfinite(number):
        raise ValueError("training evidence must be finite numeric data")
    return number


def _training_timestamp(value) -> datetime:
    try:
        if (not isinstance(value, datetime) or value.tzinfo is None
                or value.utcoffset() is None):
            raise ValueError("training requires timezone-aware timestamps")
        normalized = value.astimezone(timezone.utc)
        if not math.isfinite(normalized.timestamp()):
            raise ValueError("training requires timezone-aware timestamps")
        return normalized
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise ValueError("training requires timezone-aware timestamps") from exc


def _label_ready_time(row: CandidateLabel) -> datetime:
    available = getattr(row, "label_available_time", None)
    if available is None:
        return row.label_closed_time
    return max(row.label_closed_time.astimezone(timezone.utc),
               available.astimezone(timezone.utc))


def _decision_cutoff_time(row: CandidateLabel) -> datetime:
    lower = getattr(row, "decision_lower_time", None)
    if lower is None:
        return row.candidate_time
    return min(row.candidate_time.astimezone(timezone.utc),
               lower.astimezone(timezone.utc))


def _validate_rows(
    rows: list[CandidateLabel], feature_order: tuple[str, ...],
    *, final_as_of: datetime | None = None,
) -> list[CandidateLabel]:
    if not feature_order:
        raise ValueError("feature_order cannot be empty")
    as_of = _training_timestamp(final_as_of) if final_as_of is not None else None
    validated = []
    for row in rows:
        if row.label_closed_time is None or (row.net_return_bps is None and as_of is None):
            raise ValueError("training requires complete closed labels")
        candidate_utc = _training_timestamp(row.candidate_time)
        closed_utc = _training_timestamp(row.label_closed_time)
        available = getattr(row, "label_available_time", None)
        lower = getattr(row, "decision_lower_time", None)
        if available is not None:
            available = _training_timestamp(available)
        if lower is not None:
            lower = _training_timestamp(lower)
        # Timestamp authority is validated even for a future row. Its future
        # features/outcome never enter validation, counts, folds or final fit.
        ready = max(closed_utc, available) if available is not None else closed_utc
        if as_of is not None and ready > as_of:
            continue
        if row.net_return_bps is None:
            raise ValueError("training requires complete closed labels")
        try:
            raw_values = [row.features[name] for name in feature_order]
        except (KeyError, TypeError) as exc:
            raise ValueError("training requires complete closed labels") from exc
        values = [_training_number(value) for value in raw_values]
        outcome = _training_number(row.net_return_bps)
        closes_before_candidate = (
            closed_utc < candidate_utc if available is not None or lower is not None
            else row.label_closed_time < row.candidate_time
        )
        if closes_before_candidate:
            raise ValueError("label closes before candidate")
        row_type = GroupedCandidateLabel if isinstance(row, GroupedCandidateLabel) else CandidateLabel
        identity = ({"entry_id": row.entry_id, "cluster_id": row.cluster_id}
                    if row_type is GroupedCandidateLabel else {})
        validated.append(
            row_type(
                candidate_time=candidate_utc if row_type is GroupedCandidateLabel else row.candidate_time,
                label_closed_time=closed_utc if row_type is GroupedCandidateLabel else row.label_closed_time,
                features=MappingProxyType(dict(zip(feature_order, values))),
                net_return_bps=outcome,
                label_available_time=available,
                decision_lower_time=lower,
                **identity,
            )
        )
    return sorted(validated, key=lambda row: row.candidate_time)


def _split_fit_calibration(
    rows: list[CandidateLabel],
    *,
    calibration_fraction: float,
    min_calibration: int,
) -> tuple[list[CandidateLabel], list[CandidateLabel]]:
    count = max(int(min_calibration), int(math.ceil(len(rows) * calibration_fraction)))
    count = min(count, len(rows) - 30)
    if count < min_calibration or len(rows) - count < 30:
        raise CalibrationDataUnavailableError("insufficient rows for disjoint calibration")
    calibration = rows[-count:]
    cutoff = min(_decision_cutoff_time(row) for row in calibration)
    fitting = [
        row
        for row in rows[:-count]
        if row.label_closed_time is not None and _label_ready_time(row) < cutoff
    ]
    if len(fitting) < 30:
        raise CalibrationDataUnavailableError("calibration purge leaves too few fitting rows")
    return fitting, calibration


def _validate_grouped_identities(rows):
    seen = set()
    for row in rows:
        if not isinstance(row, GroupedCandidateLabel):
            raise ValueError("grouped training requires original entry and cluster identities")
        if row.label_available_time is None or row.decision_lower_time is None:
            raise ValueError("grouped training requires explicit label availability and pre-entry decision evidence")
        identity = row.entry_id
        if (type(identity) is not str or not 1 <= len(identity) <= 64
                or not identity.isascii() or any(not 33 <= ord(char) <= 126 for char in identity)):
            raise ValueError("invalid grouped training entry identity")
        if identity in seen:
            raise ValueError("duplicate grouped training entry identity")
        seen.add(identity)
        expected = "utc-day:" + row.candidate_time.astimezone(timezone.utc).strftime("%Y-%m-%d")
        if type(row.cluster_id) is not str or row.cluster_id != expected:
            raise ValueError("grouped training requires the fixed original UTC candidate-day cluster")


def _grouped_units(rows):
    units = []
    for row in rows:
        if not units or units[-1][0].cluster_id != row.cluster_id:
            units.append([])
        units[-1].append(row)
    return units


def _grouped_disjoint(left, right):
    if ({row.entry_id for row in left} & {row.entry_id for row in right}
            or {row.cluster_id for row in left} & {row.cluster_id for row in right}):
        raise ValueError("grouped training partitions share an entry or cluster")


def _split_grouped_fit_calibration(rows, *, calibration_fraction, min_calibration, audit):
    requested = max(int(min_calibration), int(math.ceil(len(rows) * calibration_fraction)))
    count = min(requested, len(rows) - 30)
    audit["requested_calibration_rows"] = requested
    if count < min_calibration:
        raise CalibrationDataUnavailableError("insufficient rows for grouped disjoint calibration")
    first_cluster = rows[-count].cluster_id
    boundary = next(index for index, row in enumerate(rows) if row.cluster_id == first_cluster)
    calibration = rows[boundary:]
    prior = rows[:boundary]
    cutoff = min(_decision_cutoff_time(row) for row in calibration)
    prior_units = _grouped_units(prior)
    fitting_units = [unit for unit in prior_units
                     if max(_label_ready_time(row) for row in unit) < cutoff]
    fitting = [row for unit in fitting_units for row in unit]
    _grouped_disjoint(fitting, calibration)
    audit.update(calibration_cutoff=cutoff, fitting_rows=len(fitting),
        fitting_clusters=len(fitting_units), calibration_rows=len(calibration),
        calibration_clusters=len(_grouped_units(calibration)),
        purged_fitting_rows=len(prior) - len(fitting),
        purged_fitting_clusters=len(prior_units) - len(fitting_units))
    if len(fitting) < 30:
        raise CalibrationDataUnavailableError("grouped calibration purge leaves too few fitting rows")
    return fitting, calibration


def _grouped_walk_forward_fit(rows, *, feature_order, min_train, test_size, purge,
                              ridge, calibration_fraction, min_calibration,
                              final_as_of, skip_unready_folds):
    _validate_grouped_identities(rows)
    rows = sorted(rows, key=lambda row: (row.candidate_time, row.entry_id))
    units = _grouped_units(rows)
    start, prefix = 0, 0
    while start < len(units) and prefix < min_train:
        prefix += len(units[start])
        start += 1
    folds, predictions, skipped, audits = [], [], [], []
    while start < len(units):
        end, count = start, 0
        while end < len(units) and count < test_size:
            count += len(units[end])
            end += 1
        test_rows = [row for unit in units[start:end] for row in unit]
        try:
            cutoff = min(_decision_cutoff_time(row) for row in test_rows) - purge
        except OverflowError as exc:
            raise ValueError("purge exceeds candidate timestamp range") from exc
        prior = units[:start]
        eligible = [unit for unit in prior
                    if max(_label_ready_time(row) for row in unit) < cutoff]
        training = [row for unit in eligible for row in unit]
        _grouped_disjoint(training, test_rows)
        audit = dict(attempt=len(audits) + 1, test_start=test_rows[0].candidate_time,
            test_end=test_rows[-1].candidate_time, training_cutoff=cutoff,
            test_decision_cutoff=min(_decision_cutoff_time(row) for row in test_rows),
            purge_seconds=purge.total_seconds(), calibration_fraction=calibration_fraction,
            requested_min_calibration_rows=min_calibration, required_min_fitting_rows=30,
            requested_min_training_rows=min_train, requested_test_rows=test_size,
            effective_test_rows=len(test_rows), test_clusters=end - start,
            training_rows_before_purge=sum(len(unit) for unit in prior),
            training_clusters_before_purge=len(prior), training_rows=len(training),
            training_clusters=len(eligible),
            purged_training_rows=sum(len(unit) for unit in prior) - len(training),
            purged_training_clusters=len(prior) - len(eligible),
            fitting_rows=0, fitting_clusters=0, calibration_rows=0, calibration_clusters=0,
            purged_fitting_rows=0, purged_fitting_clusters=0, calibration_cutoff=None,
            requested_calibration_rows=0, status="skipped", reason=None)
        audits.append(audit)
        reason = None
        if len(training) < min_train:
            reason = "insufficient available grouped training rows"
        else:
            try:
                fitting, calibration = _split_grouped_fit_calibration(training,
                    calibration_fraction=calibration_fraction, min_calibration=min_calibration, audit=audit)
            except CalibrationDataUnavailableError as exc:
                if not skip_unready_folds:
                    raise
                reason = str(exc)
        if reason is not None:
            audit["reason"] = reason
            if skip_unready_folds:
                skipped.append(SkippedWalkForwardFold(test_rows[0].candidate_time,
                    test_rows[-1].candidate_time, len(test_rows), len(training), cutoff, reason))
            start = end
            continue
        model = _fit_model(fitting, calibration, feature_order, ridge)
        for row in test_rows:
            expected, probability = _predict(model, row)
            predictions.append(GroupedExpectancyPrediction(row.candidate_time,
                float(row.net_return_bps), expected, probability,
                entry_id=row.entry_id, cluster_id=row.cluster_id))
        folds.append(WalkForwardFold(test_rows[0].candidate_time, test_rows[-1].candidate_time,
            len(training), len(test_rows), max(row.label_closed_time for row in training),
            len(fitting), len(calibration), calibration[0].candidate_time,
            max(row.label_closed_time for row in fitting)))
        audit["status"] = "fitted"
        start = end
    if not folds:
        raise ValueError("purge leaves no valid grouped walk-forward folds")
    final_audit = dict(training_rows=len(rows), training_clusters=len(units),
        training_cutoff=final_as_of, calibration_fraction=calibration_fraction,
        requested_min_calibration_rows=min_calibration, required_min_fitting_rows=30,
        status="fitted", reason=None)
    final_fitting, final_calibration = _split_grouped_fit_calibration(rows,
        calibration_fraction=calibration_fraction, min_calibration=min_calibration, audit=final_audit)
    final_model = _fit_model(final_fitting, final_calibration, feature_order, ridge)
    return GroupedWalkForwardResult(tuple(folds), tuple(predictions), final_model,
        _calibration_metrics(predictions), tuple(skipped),
        cold_start_policy=("skip_unready_calibration_v1" if skip_unready_folds else "require_ready_calibration_v1"),
        grouping_audit=tuple(audits), final_grouping=final_audit)


def _fit_model(
    fitting_rows: list[CandidateLabel],
    calibration_rows: list[CandidateLabel],
    feature_order: tuple[str, ...],
    ridge: float,
) -> LinearExpectancyModel:
    matrix = np.asarray(
        [
            [float(row.features[name]) for name in feature_order]
            for row in fitting_rows
        ],
        dtype=float,
    )
    targets = np.asarray(
        [float(row.net_return_bps) for row in fitting_rows], dtype=float
    )
    magnitudes = np.max(np.abs(matrix), axis=0)
    divisors = np.where(magnitudes > 0.0, magnitudes, 1.0)
    scaled = matrix / divisors
    scaled_means = scaled.mean(axis=0)
    scaled_scales = scaled.std(axis=0)
    scaled_scales[scaled_scales < 1e-12] = 1.0
    means = scaled_means * divisors
    scales = scaled_scales * divisors
    normalized = (scaled - scaled_means) / scaled_scales
    if not all(
        np.all(np.isfinite(values))
        for values in (means, scales, normalized)
    ):
        raise ValueError("feature normalization must remain finite")
    design = np.column_stack([np.ones(len(fitting_rows)), normalized])
    target_magnitude = float(np.max(np.abs(targets)))
    target_divisor = target_magnitude if target_magnitude > 0.0 else 1.0
    scaled_targets = targets / target_divisor
    regularizer = np.eye(design.shape[1]) * math.sqrt(ridge)
    regularizer[0, 0] = 0.0
    augmented_design = np.vstack([design, regularizer])
    augmented_targets = np.concatenate(
        [scaled_targets, np.zeros(design.shape[1])]
    )
    try:
        scaled_coefficients = np.linalg.lstsq(
            augmented_design, augmented_targets, rcond=None
        )[0]
    except np.linalg.LinAlgError as exc:
        raise ValueError("ridge least-squares fit failed") from exc
    coefficients = scaled_coefficients * target_divisor
    if not np.all(np.isfinite(coefficients)):
        raise ValueError("ridge coefficients must remain finite")
    calibration_matrix = np.asarray(
        [
            [float(row.features[name]) for name in feature_order]
            for row in calibration_rows
        ],
        dtype=float,
    )
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        calibration_normalized = (
            calibration_matrix / divisors - scaled_means
        ) / scaled_scales
    if not np.all(np.isfinite(calibration_normalized)):
        raise ValueError("feature normalization must remain finite")
    calibration_design = np.column_stack(
        [np.ones(len(calibration_rows)), calibration_normalized]
    )
    raw_scores_scaled = calibration_design @ scaled_coefficients
    if not np.all(np.isfinite(raw_scores_scaled)):
        raise ValueError("calibration scores must remain finite")
    labels = np.asarray(
        [float(row.net_return_bps) > 0.0 for row in calibration_rows], dtype=float
    )
    platt_intercept, scaled_platt_scale = _fit_platt(
        raw_scores_scaled, labels
    )
    platt_scale = scaled_platt_scale / target_divisor
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "features": feature_order,
                "fit_n": len(fitting_rows),
                "cal_n": len(calibration_rows),
                "last": calibration_rows[-1].label_closed_time.isoformat(),
                "coef": coefficients.tolist(),
                "feature_means": means.tolist(),
                "feature_scales": scales.tolist(),
                "probability_intercept": platt_intercept,
                "probability_scale": platt_scale,
                "ridge": max(0.0, float(ridge)),
                "fingerprint_schema": 2,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    return LinearExpectancyModel(
        feature_order=feature_order,
        coefficients=tuple(float(value) for value in coefficients[1:]),
        intercept=float(coefficients[0]),
        probability_scale=platt_scale,
        probability_intercept=platt_intercept,
        version=f"ridge-platt-cal-{fingerprint}",
        feature_means=tuple(float(value) for value in means),
        feature_scales=tuple(float(value) for value in scales),
    )


def _fit_platt(scores: np.ndarray, labels: np.ndarray) -> tuple[float, float]:
    if (
        scores.ndim != 1
        or labels.ndim != 1
        or len(scores) != len(labels)
        or not len(scores)
        or not np.all(np.isfinite(scores))
        or not np.all(np.isfinite(labels))
    ):
        raise ValueError("probability calibration inputs must be finite")
    if not np.any(labels == 0.0) or not np.any(labels == 1.0):
        raise ValueError("probability calibration requires both outcome classes")
    score_magnitude = float(np.max(np.abs(scores)))
    score_divisor = score_magnitude if score_magnitude > 0.0 else 1.0
    normalized_scores = scores / score_divisor
    intercept = math.log((labels.sum() + 1.0) / ((1.0 - labels).sum() + 1.0))
    scale = 0.0
    learning_rate = 0.01 / max(1.0, float(np.std(normalized_scores)))
    for _ in range(1_000):
        logits = np.clip(intercept + scale * normalized_scores, -35.0, 35.0)
        probabilities = 1.0 / (1.0 + np.exp(-logits))
        errors = probabilities - labels
        intercept -= learning_rate * float(errors.mean())
        scale -= learning_rate * float((errors * normalized_scores).mean())
    return float(intercept), float(scale / score_divisor)


def _predict(model: LinearExpectancyModel, row: CandidateLabel) -> tuple[float, float]:
    values = np.asarray([float(row.features[name]) for name in model.feature_order])
    if model.feature_means:
        means = np.asarray(model.feature_means)
        scales = np.asarray(model.feature_scales)
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            normalized = (values - means) / scales
            alternate = values / scales - means / scales
        values = np.where(np.isfinite(normalized), normalized, alternate)
        if not np.all(np.isfinite(values)):
            raise ValueError("prediction normalization must remain finite")
    with np.errstate(over="ignore", invalid="ignore"):
        expected = model.intercept + float(
            np.dot(values, model.coefficients)
        )
        raw_logit = (
            model.probability_intercept
            + model.probability_scale * expected
        )
    if not math.isfinite(expected) or not math.isfinite(raw_logit):
        raise ValueError("prediction arithmetic must remain finite")
    logit = np.clip(raw_logit, -35.0, 35.0)
    probability = float(1.0 / (1.0 + np.exp(-logit)))
    if not math.isfinite(probability):
        raise ValueError("prediction arithmetic must remain finite")
    return expected, probability


def _calibration_metrics(
    predictions: list[ExpectancyPrediction], *, bins: int = 10
) -> CalibrationMetrics:
    if not predictions:
        raise ValueError("calibration metrics require OOS predictions")
    probabilities = np.asarray(
        [prediction.probability_positive for prediction in predictions], dtype=float
    )
    labels = np.asarray(
        [prediction.actual_net_bps > 0.0 for prediction in predictions], dtype=float
    )
    probabilities = np.clip(probabilities, 1e-12, 1.0 - 1e-12)
    brier = float(np.mean((probabilities - labels) ** 2))
    log_loss = float(
        -np.mean(
            labels * np.log(probabilities)
            + (1.0 - labels) * np.log(1.0 - probabilities)
        )
    )
    edges = np.linspace(0.0, 1.0, max(2, int(bins)) + 1)
    ece = 0.0
    for index in range(len(edges) - 1):
        if index == len(edges) - 2:
            mask = (probabilities >= edges[index]) & (
                probabilities <= edges[index + 1]
            )
        else:
            mask = (probabilities >= edges[index]) & (
                probabilities < edges[index + 1]
            )
        if np.any(mask):
            ece += float(np.mean(mask)) * abs(
                float(np.mean(probabilities[mask])) - float(np.mean(labels[mask]))
            )
    logits = np.log(probabilities / (1.0 - probabilities))
    intercept, slope = _fit_platt(logits, labels)
    return CalibrationMetrics(
        len(predictions), brier, log_loss, ece, slope, intercept
    )


def expanding_walk_forward_fit(
    rows: list[CandidateLabel],
    *,
    feature_order: tuple[str, ...],
    min_train: int = 1_000,
    test_size: int = 200,
    purge: timedelta = timedelta(days=8),
    ridge: float = 1.0,
    calibration_fraction: float = 0.20,
    min_calibration: int = 30,
    final_as_of: datetime | None = None,
    skip_unready_folds: bool = False,
    grouping_policy: str | None = None,
) -> WalkForwardResult:
    if type(skip_unready_folds) is not bool:
        raise ValueError("skip_unready_folds must be boolean")
    if grouping_policy is not None and (type(grouping_policy) is not str or grouping_policy != GROUPED_UTC_DAY_POLICY):
        raise ValueError("unsupported training grouping_policy")
    min_train, test_size = normalize_walk_forward_sizes(min_train, test_size)
    calibration_fraction, min_calibration = normalize_calibration_controls(
        calibration_fraction, min_calibration
    )
    feature_order = normalize_feature_order(feature_order)
    if not isinstance(purge, timedelta) or purge < timedelta(0):
        raise ValueError("purge must be non-negative")
    try:
        ridge_value = float(ridge)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("ridge must be finite and non-negative") from exc
    if _is_boolean(ridge) or not math.isfinite(ridge_value) or ridge_value < 0.0:
        raise ValueError("ridge must be finite and non-negative")
    validated = (
        _validate_rows(rows, feature_order)
        if final_as_of is None
        else _validate_rows(rows, feature_order, final_as_of=final_as_of)
    )
    if len(validated) < min_train + test_size:
        raise ValueError("insufficient labeled candidates for walk-forward")
    if grouping_policy is not None:
        return _grouped_walk_forward_fit(validated, feature_order=feature_order,
            min_train=min_train, test_size=test_size, purge=purge, ridge=ridge_value,
            calibration_fraction=calibration_fraction, min_calibration=min_calibration,
            final_as_of=final_as_of, skip_unready_folds=skip_unready_folds)
    folds = []
    predictions = []
    skipped = []
    test_start_index = min_train
    while test_start_index < len(validated):
        test_rows = validated[test_start_index : test_start_index + test_size]
        if not test_rows:
            break
        try:
            cutoff = min(_decision_cutoff_time(row) for row in test_rows) - purge
        except OverflowError as exc:
            raise ValueError("purge exceeds candidate timestamp range") from exc
        training = [
            row
            for row in validated[:test_start_index]
            if row.label_closed_time is not None and _label_ready_time(row) < cutoff
        ]
        if len(training) < min_train:
            if skip_unready_folds:
                skipped.append(SkippedWalkForwardFold(
                    test_rows[0].candidate_time, test_rows[-1].candidate_time,
                    len(test_rows), len(training), cutoff,
                    "insufficient available training rows",
                ))
            test_start_index += test_size
            continue
        try:
            fitting, calibration = _split_fit_calibration(
                training,
                calibration_fraction=calibration_fraction,
                min_calibration=min_calibration,
            )
        except CalibrationDataUnavailableError as exc:
            if not skip_unready_folds:
                raise
            skipped.append(SkippedWalkForwardFold(
                test_rows[0].candidate_time, test_rows[-1].candidate_time,
                len(test_rows), len(training), cutoff, str(exc),
            ))
            test_start_index += test_size
            continue
        model = _fit_model(fitting, calibration, feature_order, ridge_value)
        for row in test_rows:
            expected, probability = _predict(model, row)
            predictions.append(
                ExpectancyPrediction(
                    row.candidate_time,
                    float(row.net_return_bps),
                    expected,
                    probability,
                )
            )
        folds.append(
            WalkForwardFold(
                test_start=test_rows[0].candidate_time,
                test_end=test_rows[-1].candidate_time,
                training_rows=len(training),
                test_rows=len(test_rows),
                max_training_label_time=max(row.label_closed_time for row in training),
                fitting_rows=len(fitting),
                calibration_rows=len(calibration),
                calibration_start=calibration[0].candidate_time,
                max_fitting_label_time=max(
                    row.label_closed_time for row in fitting
                ),
            )
        )
        test_start_index += test_size
    if not folds:
        raise ValueError("purge leaves no valid walk-forward folds")
    final_fitting, final_calibration = _split_fit_calibration(
        validated,
        calibration_fraction=calibration_fraction,
        min_calibration=min_calibration,
    )
    final_model = _fit_model(
        final_fitting, final_calibration, feature_order, ridge_value
    )
    metrics = _calibration_metrics(predictions)
    if skip_unready_folds:
        return ForwardWalkForwardResult(
            tuple(folds), tuple(predictions), final_model, metrics, tuple(skipped),
        )
    return WalkForwardResult(
        tuple(folds), tuple(predictions), final_model, metrics
    )
