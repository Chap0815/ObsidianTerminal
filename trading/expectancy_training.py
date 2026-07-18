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


@dataclass(frozen=True)
class ExpectancyPrediction:
    candidate_time: datetime
    actual_net_bps: float
    predicted_net_bps: float
    probability_positive: float


@dataclass(frozen=True)
class WalkForwardResult:
    folds: tuple[WalkForwardFold, ...]
    predictions: tuple[ExpectancyPrediction, ...]
    final_model: LinearExpectancyModel


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


def _fit_model(
    rows: list[CandidateLabel], feature_order: tuple[str, ...], ridge: float
) -> LinearExpectancyModel:
    matrix = np.asarray(
        [[float(row.features[name]) for name in feature_order] for row in rows],
        dtype=float,
    )
    targets = np.asarray([float(row.net_return_bps) for row in rows], dtype=float)
    means = matrix.mean(axis=0)
    scales = matrix.std(axis=0)
    scales[scales < 1e-12] = 1.0
    normalized = (matrix - means) / scales
    design = np.column_stack([np.ones(len(rows)), normalized])
    penalty = np.eye(design.shape[1]) * max(0.0, float(ridge))
    penalty[0, 0] = 0.0
    coefficients = np.linalg.solve(
        design.T @ design + penalty,
        design.T @ targets,
    )
    raw_scores = design @ coefficients
    labels = (targets > 0.0).astype(float)
    platt_intercept, platt_scale = _fit_platt(raw_scores, labels)
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "features": feature_order,
                "n": len(rows),
                "last": rows[-1].label_closed_time.isoformat(),
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
        version=f"ridge-platt-{fingerprint}",
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


def expanding_walk_forward_fit(
    rows: list[CandidateLabel],
    *,
    feature_order: tuple[str, ...],
    min_train: int = 1_000,
    test_size: int = 200,
    purge: timedelta = timedelta(days=8),
    ridge: float = 1.0,
) -> WalkForwardResult:
    validated = _validate_rows(rows, feature_order)
    if min_train < 30 or test_size < 1:
        raise ValueError("invalid walk-forward sizes")
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
        model = _fit_model(training, feature_order, ridge)
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
            )
        )
        test_start_index += test_size
    if not folds:
        raise ValueError("purge leaves no valid walk-forward folds")
    final_model = _fit_model(validated, feature_order, ridge)
    return WalkForwardResult(tuple(folds), tuple(predictions), final_model)
