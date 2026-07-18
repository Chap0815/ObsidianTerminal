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
    means = matrix.mean(axis=0)
    scales = matrix.std(axis=0)
    scales[scales < 1e-12] = 1.0
    normalized = (matrix - means) / scales
    design = np.column_stack([np.ones(len(fitting_rows)), normalized])
    penalty = np.eye(design.shape[1]) * max(0.0, float(ridge))
    penalty[0, 0] = 0.0
    coefficients = np.linalg.solve(
        design.T @ design + penalty,
        design.T @ targets,
    )
    calibration_matrix = np.asarray(
        [
            [float(row.features[name]) for name in feature_order]
            for row in calibration_rows
        ],
        dtype=float,
    )
    calibration_design = np.column_stack(
        [np.ones(len(calibration_rows)), (calibration_matrix - means) / scales]
    )
    raw_scores = calibration_design @ coefficients
    labels = np.asarray(
        [float(row.net_return_bps) > 0.0 for row in calibration_rows], dtype=float
    )
    platt_intercept, platt_scale = _fit_platt(raw_scores, labels)
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "features": feature_order,
                "fit_n": len(fitting_rows),
                "cal_n": len(calibration_rows),
                "last": calibration_rows[-1].label_closed_time.isoformat(),
                "coef": coefficients.tolist(),
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
    intercept = math.log((labels.sum() + 1.0) / ((1.0 - labels).sum() + 1.0))
    scale = 0.0
    learning_rate = 0.01 / max(1.0, float(np.std(scores)))
    for _ in range(1_000):
        logits = np.clip(intercept + scale * scores, -35.0, 35.0)
        probabilities = 1.0 / (1.0 + np.exp(-logits))
        errors = probabilities - labels
        intercept -= learning_rate * float(errors.mean())
        scale -= learning_rate * float((errors * scores).mean())
    return float(intercept), float(scale)


def _predict(model: LinearExpectancyModel, row: CandidateLabel) -> tuple[float, float]:
    values = np.asarray([float(row.features[name]) for name in model.feature_order])
    if model.feature_means:
        values = (values - np.asarray(model.feature_means)) / np.asarray(
            model.feature_scales
        )
    expected = model.intercept + float(np.dot(values, model.coefficients))
    logit = np.clip(
        model.probability_intercept + model.probability_scale * expected,
        -35.0,
        35.0,
    )
    return expected, float(1.0 / (1.0 + np.exp(-logit)))


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
    validated = _validate_rows(rows, feature_order)
    if min_train < 30 or test_size < 1:
        raise ValueError("invalid walk-forward sizes")
    if not 0.05 <= float(calibration_fraction) <= 0.50:
        raise ValueError("calibration_fraction must be between 0.05 and 0.50")
    if min_calibration < 20:
        raise ValueError("min_calibration must be at least 20")
    if len(validated) < min_train + test_size:
        raise ValueError("insufficient labeled candidates for walk-forward")
    folds = []
    predictions = []
    test_start_index = min_train
    while test_start_index < len(validated):
        test_rows = validated[test_start_index : test_start_index + test_size]
        if not test_rows:
            break
        cutoff = test_rows[0].candidate_time - purge
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
        model = _fit_model(fitting, calibration, feature_order, ridge)
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
        final_fitting, final_calibration, feature_order, ridge
    )
    metrics = _calibration_metrics(predictions)
    return WalkForwardResult(
        tuple(folds), tuple(predictions), final_model, metrics
    )
