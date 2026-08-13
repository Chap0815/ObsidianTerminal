"""Generic prefix-invariance checks for strategy feature pipelines."""
from __future__ import annotations

from copy import deepcopy
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


def _integer(value, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f"{name} must be an integer")
    return int(number)


def _positive_integer(value, name: str) -> int:
    number = _integer(value, name)
    if number <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return number


def _absolute_tolerance(value) -> float:
    if isinstance(value, bool):
        raise ValueError("absolute_tolerance must be finite and non-negative")
    try:
        tolerance = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "absolute_tolerance must be finite and non-negative"
        ) from exc
    if not math.isfinite(tolerance) or tolerance < 0.0:
        raise ValueError("absolute_tolerance must be finite and non-negative")
    return tolerance


def _comparison_error(left, right) -> tuple[float, bool]:
    """Return absolute error and whether numeric evidence was invalid."""
    try:
        left_number = float(left)
        right_number = float(right)
    except OverflowError:
        return math.inf, True
    except (TypeError, ValueError):
        try:
            equal = left == right
            if not isinstance(equal, bool):
                equal = bool(equal)
        except (TypeError, ValueError):
            equal = False
        return (0.0 if equal else math.inf), False
    if not math.isfinite(left_number) or not math.isfinite(right_number):
        return math.inf, True
    error = abs(left_number - right_number)
    if not math.isfinite(error):
        return math.inf, True
    return error, False


def scan_prefix_invariance(
    rows: Sequence,
    feature_fn: Callable[[Sequence], Sequence],
    *,
    min_compare: int = 10,
    absolute_tolerance: float = 1e-12,
) -> CausalityReport:
    min_compare = _positive_integer(min_compare, "min_compare")
    absolute_tolerance = _absolute_tolerance(absolute_tolerance)
    if len(rows) <= min_compare:
        return CausalityReport(False, 0, None, 0.0, "insufficient coverage")
    full = list(feature_fn(rows))
    if len(full) != len(rows):
        return CausalityReport(False, 0, None, 0.0, "feature length mismatch")
    compared = 0
    max_error = 0.0
    for end in range(min_compare, len(rows)):
        prefix = list(feature_fn(rows[:end]))
        if len(prefix) != end:
            return CausalityReport(False, compared, end - 1, max_error, "prefix length mismatch")
        error, invalid_numeric = _comparison_error(
            prefix[-1], full[end - 1]
        )
        compared += 1
        max_error = max(max_error, error)
        if invalid_numeric:
            return CausalityReport(
                False,
                compared,
                end - 1,
                max_error,
                "non-finite feature output",
            )
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
    cutoff = _integer(cutoff, "cutoff")
    absolute_tolerance = _absolute_tolerance(absolute_tolerance)
    if cutoff <= 0 or cutoff >= len(rows):
        return CausalityReport(False, 0, None, 0.0, "invalid perturbation cutoff")
    changed_rows = list(rows[:cutoff]) + [
        perturb(deepcopy(row)) for row in rows[cutoff:]
    ]
    intervention_effective = False
    for original, changed_row in zip(
        rows[cutoff:], changed_rows[cutoff:], strict=True
    ):
        error, invalid_numeric = _comparison_error(original, changed_row)
        if not invalid_numeric and error > 0.0:
            intervention_effective = True
            break
    if not intervention_effective:
        return CausalityReport(
            False,
            0,
            None,
            0.0,
            "future-tail perturbation had no effect",
        )
    baseline = list(feature_fn(rows))
    changed = list(feature_fn(changed_rows))
    if len(baseline) != len(rows) or len(changed) != len(rows):
        return CausalityReport(False, 0, None, 0.0, "feature length mismatch")
    max_error = 0.0
    for index in range(cutoff):
        error, invalid_numeric = _comparison_error(
            baseline[index], changed[index]
        )
        max_error = max(max_error, error)
        if invalid_numeric:
            return CausalityReport(
                False,
                index + 1,
                index,
                max_error,
                "non-finite feature output",
            )
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
