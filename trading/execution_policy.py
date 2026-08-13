"""Pure research-only maker/taker/abstention evaluation.

The module never submits, cancels, or modifies an order.  A maker fill is only
labelled with the conservative cross-through proxy; queue position is unknown
and is deliberately never reconstructed from public order-book snapshots.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping


def _finite(value, name: str) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{name} must be finite")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _positive_integer(value, name: str) -> int:
    number = _finite(value, name)
    if number <= 0.0 or not number.is_integer():
        raise ValueError(f"{name} must be a positive integer")
    return int(number)


def _nonnegative(value, name: str) -> float:
    number = _finite(value, name)
    if number < 0.0:
        raise ValueError(f"{name} must be non-negative")
    return number


@dataclass(frozen=True)
class ExecutionPolicyEvidence:
    gross_edge_bps: float | None
    taker_cost_bps: float | None
    maker_fee_bps: float | None
    maker_adverse_selection_bps: float | None
    maker_fill_probability: float | None
    missed_fill_cost_bps: float | None
    samples: int
    sequence_valid: bool


@dataclass(frozen=True)
class ExecutionPolicyDecision:
    action: str
    expected_taker_cost_bps: float | None
    expected_maker_cost_bps: float | None
    expected_net_edge_bps: float | None
    samples: int
    reasons: tuple[str, ...]
    changes_orders: bool = False


@dataclass(frozen=True)
class MakerCrossThroughOutcome:
    crossed_through: bool
    queue_fill_claimed: bool
    observed_books: int
    final_touch: float | None
    missed_fill_cost_bps: float | None
    proxy: str = "opposite_touch_cross_through"


def evaluate_shadow_execution_policy(
    evidence: ExecutionPolicyEvidence,
    *,
    minimum_samples: int = 50,
    minimum_maker_savings_bps: float = 0.5,
    edge_safety_buffer_bps: float = 0.0,
) -> ExecutionPolicyDecision:
    """Choose a research recommendation from fully observed cost evidence."""
    minimum = _positive_integer(minimum_samples, "minimum_samples")
    maker_savings = _nonnegative(
        minimum_maker_savings_bps, "minimum_maker_savings_bps"
    )
    edge_buffer = _nonnegative(
        edge_safety_buffer_bps, "edge_safety_buffer_bps"
    )
    reasons: list[str] = []
    try:
        samples = _positive_integer(evidence.samples, "samples")
    except ValueError:
        samples = 0
        reasons.append("sample_count_invalid")
    if samples and samples < minimum:
        reasons.append("sample_count_below_minimum")
    if evidence.sequence_valid is not True:
        reasons.append("sequence_valid_evidence_missing")

    parsed: dict[str, float] = {}
    for name in (
        "gross_edge_bps",
        "taker_cost_bps",
        "maker_fee_bps",
        "maker_adverse_selection_bps",
        "maker_fill_probability",
        "missed_fill_cost_bps",
    ):
        try:
            parsed[name] = _finite(getattr(evidence, name), name)
        except ValueError:
            reasons.append(f"{name}_missing")
    probability = parsed.get("maker_fill_probability")
    if probability is not None and not 0.0 <= probability <= 1.0:
        reasons.append("maker_fill_probability_out_of_range")
    for name in (
        "taker_cost_bps",
        "maker_fee_bps",
        "maker_adverse_selection_bps",
        "missed_fill_cost_bps",
    ):
        if name in parsed and parsed[name] < 0.0:
            reasons.append(f"{name}_negative")
    if reasons:
        return ExecutionPolicyDecision(
            action="INSUFFICIENT_DATA",
            expected_taker_cost_bps=parsed.get("taker_cost_bps"),
            expected_maker_cost_bps=None,
            expected_net_edge_bps=None,
            samples=max(0, samples),
            reasons=tuple(dict.fromkeys(reasons)),
        )

    taker_cost = parsed["taker_cost_bps"]
    maker_cost_if_filled = (
        parsed["maker_fee_bps"] + parsed["maker_adverse_selection_bps"]
    )
    missed_then_taker = parsed["missed_fill_cost_bps"] + taker_cost
    maker_cost = (
        probability * maker_cost_if_filled
        + (1.0 - probability)
        * missed_then_taker
    )
    best_cost = min(taker_cost, maker_cost)
    best_net_edge = parsed["gross_edge_bps"] - best_cost
    maker_threshold = maker_cost + maker_savings
    if not all(
        math.isfinite(value)
        for value in (
            maker_cost_if_filled,
            missed_then_taker,
            maker_cost,
            best_cost,
            best_net_edge,
            maker_threshold,
        )
    ):
        return ExecutionPolicyDecision(
            action="INSUFFICIENT_DATA",
            expected_taker_cost_bps=taker_cost,
            expected_maker_cost_bps=None,
            expected_net_edge_bps=None,
            samples=samples,
            reasons=("derived_cost_nonfinite",),
        )
    if best_net_edge <= edge_buffer:
        action = "ABSTAIN"
        net_edge = best_net_edge
        reasons = ["net_edge_not_positive_after_execution"]
    elif maker_threshold <= taker_cost:
        action = "MAKER"
        net_edge = parsed["gross_edge_bps"] - maker_cost
        reasons = ["maker_expected_cost_lower"]
    else:
        action = "TAKER"
        net_edge = parsed["gross_edge_bps"] - taker_cost
        reasons = ["taker_expected_cost_lower_or_equal"]
    return ExecutionPolicyDecision(
        action=action,
        expected_taker_cost_bps=taker_cost,
        expected_maker_cost_bps=maker_cost,
        expected_net_edge_bps=net_edge,
        samples=samples,
        reasons=tuple(reasons),
    )


def evaluate_maker_cross_through(
    *,
    side: str,
    maker_price: float,
    arrival_touch: float,
    subsequent_books: Iterable[Mapping],
) -> MakerCrossThroughOutcome:
    """Conservatively label whether price traded through a hypothetical quote."""
    normalized_side = str(side).strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("side must be buy or sell")
    quote = _finite(maker_price, "maker_price")
    arrival = _finite(arrival_touch, "arrival_touch")
    if quote <= 0.0 or arrival <= 0.0:
        raise ValueError("maker price and arrival touch must be positive")
    if (
        normalized_side == "buy" and quote >= arrival
    ) or (
        normalized_side == "sell" and quote <= arrival
    ):
        raise ValueError("maker quote must be passive at arrival")

    crossed = False
    final_touch = None
    observed = 0
    for book in subsequent_books:
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            continue
        bid = _finite(bids[0][0], "bid")
        ask = _finite(asks[0][0], "ask")
        if not 0.0 < bid <= ask:
            raise ValueError("invalid subsequent order book")
        observed += 1
        final_touch = ask if normalized_side == "buy" else bid
        if (
            normalized_side == "buy" and ask <= quote
        ) or (
            normalized_side == "sell" and bid >= quote
        ):
            crossed = True
            break
    missed_cost = None
    if final_touch is not None:
        if crossed:
            missed_cost = 0.0
        else:
            sign = 1.0 if normalized_side == "buy" else -1.0
            derived_missed_cost = (
                sign * (final_touch - arrival) / arrival * 10_000.0
            )
            if not math.isfinite(derived_missed_cost):
                raise ValueError("derived missed fill cost must be finite")
            missed_cost = max(0.0, derived_missed_cost)
    return MakerCrossThroughOutcome(
        crossed_through=crossed,
        queue_fill_claimed=False,
        observed_books=observed,
        final_touch=final_touch,
        missed_fill_cost_bps=missed_cost,
    )
