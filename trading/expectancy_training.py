"""True expanding walk-forward training for calibrated net expectancy."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Mapping

import numpy as np

from trading.profit_experiments import LinearExpectancyModel


def _positive_integer(value, name: str) -> int:
    if isinstance(value, bool):
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
    if isinstance(calibration_fraction, bool):
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


@dataclass(frozen=True)
class CandidateLabel:
    candidate_time: datetime
    label_closed_time: datetime | None
    features: Mapping[str, float]
    net_return_bps: float | None


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


def _validate_rows(
    rows: list[CandidateLabel], feature_order: tuple[str, ...]
) -> list[CandidateLabel]:
    if not feature_order:
        raise ValueError("feature_order cannot be empty")
    validated = []
    for row in rows:
        if row.label_closed_time is None or row.net_return_bps is None:
            raise ValueError("training requires complete closed labels")
        if (
            not isinstance(row.candidate_time, datetime)
            or row.candidate_time.tzinfo is None
            or row.candidate_time.utcoffset() is None
            or not isinstance(row.label_closed_time, datetime)
            or row.label_closed_time.tzinfo is None
            or row.label_closed_time.utcoffset() is None
        ):
            raise ValueError("training requires timezone-aware timestamps")
        try:
            values = [float(row.features[name]) for name in feature_order]
            outcome = float(row.net_return_bps)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("training requires complete closed labels") from exc
        if not all(math.isfinite(value) for value in values + [outcome]):
            raise ValueError("training requires finite features and labels")
        if row.label_closed_time < row.candidate_time:
            raise ValueError("label closes before candidate")
        validated.append(row)
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
        raise ValueError("insufficient rows for disjoint calibration")
    calibration = rows[-count:]
    cutoff = calibration[0].candidate_time
    fitting = [
        row
        for row in rows[:-count]
        if row.label_closed_time is not None and row.label_closed_time < cutoff
    ]
    if len(fitting) < 30:
        raise ValueError("calibration purge leaves too few fitting rows")
    return fitting, calibration


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
) -> WalkForwardResult:
    min_train, test_size = normalize_walk_forward_sizes(min_train, test_size)
    calibration_fraction, min_calibration = normalize_calibration_controls(
        calibration_fraction, min_calibration
    )
    feature_order = normalize_feature_order(feature_order)
    if not isinstance(purge, timedelta) or purge < timedelta(0):
        raise ValueError("purge must be non-negative")
    try:
        ridge_value = float(ridge)
    except (TypeError, ValueError) as exc:
        raise ValueError("ridge must be finite and non-negative") from exc
    if isinstance(ridge, bool) or not math.isfinite(ridge_value) or ridge_value < 0.0:
        raise ValueError("ridge must be finite and non-negative")
    validated = _validate_rows(rows, feature_order)
    if len(validated) < min_train + test_size:
        raise ValueError("insufficient labeled candidates for walk-forward")
    folds = []
    predictions = []
    test_start_index = min_train
    while test_start_index < len(validated):
        test_rows = validated[test_start_index : test_start_index + test_size]
        if not test_rows:
            break
        try:
            cutoff = test_rows[0].candidate_time - purge
        except OverflowError as exc:
            raise ValueError("purge exceeds candidate timestamp range") from exc
        training = [
            row
            for row in validated[:test_start_index]
            if row.label_closed_time is not None and row.label_closed_time < cutoff
        ]
        if len(training) < min_train:
            test_start_index += test_size
            continue
        fitting, calibration = _split_fit_calibration(
            training,
            calibration_fraction=calibration_fraction,
            min_calibration=min_calibration,
        )
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
    return WalkForwardResult(
        tuple(folds), tuple(predictions), final_model, metrics
    )
