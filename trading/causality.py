"""Generic prefix-invariance checks for strategy feature pipelines."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Sequence


@dataclass(frozen=True)
class CausalityReport:
    passed: bool
    compared: int
    first_mismatch_index: int | None
    max_absolute_error: float
    reason: str


def scan_prefix_invariance(
    rows: Sequence,
    feature_fn: Callable[[Sequence], Sequence],
    *,
    min_compare: int = 10,
    absolute_tolerance: float = 1e-12,
) -> CausalityReport:
    if len(rows) < min_compare:
        return CausalityReport(False, 0, None, 0.0, "insufficient coverage")
    full = list(feature_fn(rows))
    if len(full) != len(rows):
        return CausalityReport(False, 0, None, 0.0, "feature length mismatch")
    compared = 0
    max_error = 0.0
    for end in range(min_compare, len(rows) + 1):
        prefix = list(feature_fn(rows[:end]))
        if len(prefix) != end:
            return CausalityReport(False, compared, end - 1, max_error, "prefix length mismatch")
        try:
            left = float(prefix[-1])
            right = float(full[end - 1])
            error = abs(left - right)
        except (TypeError, ValueError):
            equal = prefix[-1] == full[end - 1]
            error = 0.0 if equal else math.inf
        compared += 1
        max_error = max(max_error, error)
        if error > absolute_tolerance:
            return CausalityReport(
                False, compared, end - 1, max_error, "future-tail dependency detected"
            )
    return CausalityReport(True, compared, None, max_error, "prefix invariant")


def scan_future_tail_perturbation(
    rows: Sequence,
    feature_fn: Callable[[Sequence], Sequence],
    *,
    cutoff: int,
    perturb: Callable[[object], object],
    absolute_tolerance: float = 1e-12,
) -> CausalityReport:
    if cutoff <= 0 or cutoff >= len(rows):
        return CausalityReport(False, 0, None, 0.0, "invalid perturbation cutoff")
    baseline = list(feature_fn(rows))
    changed_rows = list(rows[:cutoff]) + [perturb(row) for row in rows[cutoff:]]
    changed = list(feature_fn(changed_rows))
    if len(baseline) != len(rows) or len(changed) != len(rows):
        return CausalityReport(False, 0, None, 0.0, "feature length mismatch")
    max_error = 0.0
    for index in range(cutoff):
        try:
            error = abs(float(baseline[index]) - float(changed[index]))
        except (TypeError, ValueError):
            error = 0.0 if baseline[index] == changed[index] else math.inf
        max_error = max(max_error, error)
        if error > absolute_tolerance:
            return CausalityReport(
                False,
                index + 1,
                index,
                max_error,
                "future-tail perturbation changed past output",
            )
    return CausalityReport(True, cutoff, None, max_error, "future tail invariant")


def report_to_dict(report: CausalityReport) -> dict:
    return {
        "passed": report.passed,
        "compared": report.compared,
        "first_mismatch_index": report.first_mismatch_index,
        "max_absolute_error": report.max_absolute_error,
        "reason": report.reason,
    }
