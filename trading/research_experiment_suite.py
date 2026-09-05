"""Offline, fail-closed experiment suite for profit research.

The suite consumes existing telemetry and cached market data.  It never places
orders, changes runtime models, or promotes a result automatically.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import re
import sqlite3
import statistics
import time
import uuid
import zipfile
from collections.abc import Iterable
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import portalocker

from trading.expectancy_training import (
    CandidateLabel,
    ExpectancyPrediction,
    expanding_walk_forward_fit,
)

RESEARCH_VENUE_PAYLOAD_MAX_BYTES = 4 * 1024 * 1024
RESEARCH_REPORT_MAX_BYTES = 64 * 1024 * 1024
RESEARCH_OHLCV_ARCHIVE_MAX_BYTES = 5_000_000_000
RESEARCH_ARCHIVE_MAX_REPORTS = 10_000
RESEARCH_ARCHIVE_MAX_BLOBS = 100_000
RESEARCH_ARCHIVE_BLOB_MAX_BYTES = 50_000_000
RESEARCH_ARCHIVE_INVENTORY_MAX_BYTES = 50_000_000_000
RESEARCH_ARCHIVE_REPORT_INVENTORY_MAX_BYTES = 1_000_000_000
RESEARCH_EXPERIMENT_BUNDLE_MAX_BYTES = 5_200_000_000
RESEARCH_EXPERIMENT_BUNDLE_SCHEMA = 1
RESEARCH_BUNDLE_STAGING_MAX_BATCHES = 10_000
RESEARCH_BUNDLE_STAGING_MAX_ROOT_ENTRIES = 20_002
# One sequential publisher can be interrupted after linking its target but before
# removing its private temp, so the sealed maximum needs one recovery slot.
RESEARCH_BUNDLE_STAGING_MAX_FILES = RESEARCH_ARCHIVE_MAX_BLOBS + 4
RESEARCH_BUNDLE_STAGING_MAX_BYTES = RESEARCH_ARCHIVE_INVENTORY_MAX_BYTES


@dataclass(frozen=True)
class ExperimentDefinition:
    experiment_id: str
    title: str
    hypothesis: str
    primary_gate: str
    automatic_promotion: bool = False
    changes_orders: bool = False


EXPERIMENT_CATALOG: tuple[ExperimentDefinition, ...] = (
    ExperimentDefinition(
        "net_expectancy",
        "Calibrated net expectancy",
        "Causal entry features predict net basis points after costs.",
        "purged walk-forward with at least 1,200 closed labels",
    ),
    ExperimentDefinition(
        "entry_selectivity",
        "Entry threshold and hysteresis precursor",
        "A stricter entry hurdle improves net expectancy and reduces turnover.",
        "at least 200 closed labels per tested threshold",
    ),
    ExperimentDefinition(
        "execution_cost",
        "Empirical execution-cost stress",
        "Observed p75/p95 costs identify entries with insufficient gross edge.",
        "at least 50 paired arrival/fill observations",
    ),
    ExperimentDefinition(
        "execution_policy",
        "Dynamic maker, taker, or abstention policy",
        "Sequence-valid shadow outcomes identify the lowest expected execution cost.",
        "at least 50 complete cross-through shadow samples",
    ),
    ExperimentDefinition(
        "order_flow",
        "Continuous L2 order-flow imbalance",
        "Sequence-valid depth-normalized OFI adds short-horizon net edge.",
        "at least 1,000 promotable continuous-book windows",
    ),
    ExperimentDefinition(
        "cross_sectional_momentum",
        "Cross-sectional crypto momentum",
        "Trailing relative strength predicts the next holding window net of costs.",
        "at least 30 causal rebalance windows across 20 symbols",
    ),
    ExperimentDefinition(
        "time_series_momentum_crash",
        "Time-series momentum with crash overlay",
        "Past-only volatility and loss overlays improve TSMOM tail behaviour.",
        "at least 30 causal rebalance windows across 20 symbols",
    ),
    ExperimentDefinition(
        "funding_carry",
        "Delta-neutral perpetual funding carry",
        "Verified spot/perpetual pairs retain positive carry after stressed costs.",
        "verified spot hedge and at least 90 overview observations",
    ),
    ExperimentDefinition(
        "regime_uncertainty",
        "Regime shift and uncertainty abstention",
        "Past-only shift and calibrated uncertainty identify no-trade conditions.",
        "adequate OHLC history plus purged OOS expectancy predictions",
    ),
    ExperimentDefinition(
        "bounded_regime_grid",
        "Bounded range-regime grid",
        "A non-levered range grid earns net returns only inside a past-only range regime.",
        "at least 500 next-bar marked-to-market observations",
    ),
)


def _finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _positive_sample_count(value) -> int:
    number = _finite(value)
    if number is None or number < 1.0 or not number.is_integer():
        raise ValueError("minimum_samples must be a positive integer")
    return int(number)


def _utc_datetime(value) -> datetime | None:
    try:
        if isinstance(value, (int, float)) or str(value).strip().isdigit():
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _performance(returns_bps: Iterable[float]) -> dict:
    values = [float(value) for value in returns_bps if _finite(value) is not None]
    if not values:
        return {
            "samples": 0,
            "mean_net_bps": None,
            "median_net_bps": None,
            "win_rate": None,
            "mean_t_stat": None,
            "compounded_return_pct": None,
            "max_drawdown_pct": None,
        }
    log_equity = 0.0
    log_peak = 0.0
    maximum_drawdown = 0.0
    for value in values:
        multiplier = 1.0 + value / 10_000.0
        if multiplier <= 0.0:
            log_equity = -math.inf
            maximum_drawdown = 1.0
            continue
        if math.isfinite(log_equity):
            log_equity += math.log(multiplier)
            log_peak = max(log_peak, log_equity)
            maximum_drawdown = max(
                maximum_drawdown,
                1.0 - math.exp(max(-700.0, log_equity - log_peak)),
            )
    deviation = statistics.stdev(values) if len(values) > 1 else 0.0
    mean_t_stat = (
        statistics.mean(values) / deviation * math.sqrt(len(values))
        if deviation > 0.0
        else None
    )
    if _finite(mean_t_stat) is None:
        mean_t_stat = None
    if log_equity == -math.inf:
        compounded = -100.0
    elif log_equity > 700.0:
        compounded = None
    else:
        compounded = math.expm1(log_equity) * 100.0
    return {
        "samples": len(values),
        "mean_net_bps": statistics.mean(values),
        "median_net_bps": statistics.median(values),
        "win_rate": sum(value > 0.0 for value in values) / len(values),
        "mean_t_stat": mean_t_stat,
        "compounded_return_pct": compounded,
        "max_drawdown_pct": maximum_drawdown * 100.0,
    }


def evaluate_score_thresholds(
    labels: Iterable[CandidateLabel],
    *,
    score_feature: str,
    thresholds: Iterable[float] = (50.0, 60.0, 70.0, 80.0),
    minimum_samples: int = 200,
) -> dict:
    """Evaluate entry selectivity only; this does not claim exit hysteresis."""
    required_samples = _positive_sample_count(minimum_samples)
    prepared = []
    for label in labels:
        score = _finite(label.features.get(score_feature))
        outcome = _finite(label.net_return_bps)
        if score is not None and outcome is not None:
            prepared.append((score, outcome))
    rows = []
    for threshold in sorted({_finite(value) for value in thresholds} - {None}):
        selected = [outcome for score, outcome in prepared if score >= threshold]
        metrics = _performance(selected)
        data_ready = len(selected) >= required_samples
        rows.append(
            {
                "threshold": threshold,
                **metrics,
                "coverage": len(selected) / len(prepared) if prepared else 0.0,
                "data_ready": data_ready,
                "ready": False,
            }
        )
    return {
        "score_feature": score_feature,
        "available_labels": len(prepared),
        "minimum_samples": required_samples,
        "rows": rows,
        "research_only": True,
        "exit_hysteresis_claimed": False,
        "selection_protocol": "exploratory_in_sample_threshold_grid",
        "frozen_holdout_required": True,
    }


def evaluate_abstention_grid(
    predictions: Iterable[ExpectancyPrediction],
    *,
    probability_thresholds: Iterable[float] = (0.50, 0.55, 0.60, 0.65),
    minimum_samples: int = 200,
) -> dict:
    required_samples = _positive_sample_count(minimum_samples)
    prepared = []
    for prediction in predictions:
        probability = _finite(prediction.probability_positive)
        predicted = _finite(prediction.predicted_net_bps)
        actual = _finite(prediction.actual_net_bps)
        if (
            probability is not None
            and 0.0 <= probability <= 1.0
            and predicted is not None
            and actual is not None
        ):
            prepared.append((probability, predicted, actual))
    rows = []
    valid_thresholds = []
    for value in probability_thresholds:
        threshold = _finite(value)
        if threshold is not None and 0.0 <= threshold <= 1.0:
            valid_thresholds.append(threshold)
    for threshold in sorted(set(valid_thresholds)):
        selected = [
            actual
            for probability, predicted, actual in prepared
            if probability >= threshold and predicted > 0.0
        ]
        metrics = _performance(selected)
        rows.append(
            {
                "probability_threshold": threshold,
                **{key: value for key, value in metrics.items() if key != "mean_net_bps"},
                "mean_actual_net_bps": metrics["mean_net_bps"],
                "coverage": len(selected) / len(prepared) if prepared else 0.0,
                "ready": len(selected) >= required_samples,
            }
        )
    return {
        "oos_predictions": len(prepared),
        "minimum_samples": required_samples,
        "rows": rows,
        "research_only": True,
    }


def _series_map(
    rows: Iterable[tuple[int, float, float, float]],
) -> dict[int, tuple[float, float, float]]:
    mapped = {}
    conflicted_timestamps = set()
    for row in rows:
        try:
            timestamp, open_price, close, volume = row
        except (TypeError, ValueError):
            continue
        opening = _finite(open_price)
        price = _finite(close)
        amount = _finite(volume)
        if isinstance(timestamp, bool):
            continue
        try:
            timestamp_value = int(timestamp)
            timestamp_number = float(timestamp)
        except (TypeError, ValueError, OverflowError):
            continue
        if (
            not math.isfinite(timestamp_number)
            or timestamp_number <= 0.0
            or timestamp_number != timestamp_value
            or opening is None
            or opening <= 0.0
            or price is None
            or price <= 0.0
            or amount is None
            or amount < 0.0
        ):
            continue
        candidate = (opening, price, amount)
        if timestamp_value in conflicted_timestamps:
            continue
        existing = mapped.get(timestamp_value)
        if existing is None:
            mapped[timestamp_value] = candidate
        elif existing != candidate:
            mapped.pop(timestamp_value)
            conflicted_timestamps.add(timestamp_value)
    return mapped


def _weight_turnover(previous: dict[str, float], current: dict[str, float]) -> float:
    return sum(
        abs(current.get(symbol, 0.0) - previous.get(symbol, 0.0))
        for symbol in previous.keys() | current.keys()
    )


def _crash_exposure(values: list[float], *, window: int = 4) -> float:
    if len(values) < window:
        return 1.0
    trailing = values[-window:]
    if statistics.mean(trailing) < 0.0:
        return 0.0
    deviation = statistics.stdev(trailing) if len(trailing) > 1 else 0.0
    return min(1.0, 100.0 / deviation) if deviation > 0.0 else 1.0


def evaluate_momentum_panel(
    panel: dict[str, list[tuple[int, float, float, float]]],
    *,
    lookback_hours: int = 72,
    tsmom_lookback_hours: int = 168,
    holding_hours: int = 24,
    basket_size: int = 5,
    universe_size: int = 40,
    one_way_cost_bps: float = 12.0,
    funding_drag_bps_per_day: float = 6.0,
    minimum_windows: int = 30,
    point_in_time_universe_complete: bool = False,
    hold_rank_buffer: int = 3,
) -> dict:
    universe_proven = point_in_time_universe_complete is True
    hour_ms = 3_600_000
    lookback_ms = max(1, int(lookback_hours)) * hour_ms
    tsmom_lookback_ms = max(1, int(tsmom_lookback_hours)) * hour_ms
    holding_ms = max(1, int(holding_hours)) * hour_ms
    basket = max(1, int(basket_size))
    universe_limit = max(2 * basket + 2, int(universe_size))
    series = {}
    for symbol, rows in panel.items():
        mapped = _series_map(rows)
        if mapped:
            series[str(symbol)] = mapped
    timestamps = sorted({timestamp for rows in series.values() for timestamp in rows})
    if not timestamps:
        timestamps = []
    origin = timestamps[0] if timestamps else 0
    cross_sectional = []
    cross_hysteresis = []
    winner_long_only = []
    winner_market_hedged = []
    liquid_short = []
    dispersion_scaled = []
    time_series = []
    guarded_time_series = []
    previous_cross_weights: dict[str, float] = {}
    previous_hysteresis_weights: dict[str, float] = {}
    previous_long_only_weights: dict[str, float] = {}
    previous_market_hedged_weights: dict[str, float] = {}
    previous_liquid_short_weights: dict[str, float] = {}
    previous_dispersion_weights: dict[str, float] = {}
    previous_time_weights: dict[str, float] = {}
    previous_guarded_weights: dict[str, float] = {}
    cross_turnovers = []
    hysteresis_turnovers = []
    dispersion_exposures = []
    liquidity_pool_sizes = []
    market_hedge_benchmarks = []
    skipped_invested_windows = 0
    last_rebalance_timestamp = None
    for timestamp in timestamps:
        if (timestamp - origin) % holding_ms:
            continue
        invested = any((
            previous_cross_weights,
            previous_hysteresis_weights,
            previous_long_only_weights,
            previous_market_hedged_weights,
            previous_liquid_short_weights,
            previous_dispersion_weights,
            previous_time_weights,
            previous_guarded_weights,
        ))
        if (
            invested
            and last_rebalance_timestamp is not None
            and timestamp - last_rebalance_timestamp > holding_ms
        ):
            skipped_invested_windows += (
                timestamp - last_rebalance_timestamp
            ) // holding_ms - 1
        last_rebalance_timestamp = timestamp
        previous = timestamp - lookback_ms
        tsmom_previous = timestamp - tsmom_lookback_ms
        entry_timestamp = timestamp + hour_ms
        exit_timestamp = entry_timestamp + holding_ms
        ranked = []
        for symbol, rows in series.items():
            if (
                timestamp not in rows
                or previous not in rows
                or tsmom_previous not in rows
                or entry_timestamp not in rows
                or exit_timestamp not in rows
            ):
                continue
            current_price = rows[timestamp][1]
            past_price = rows[previous][1]
            tsmom_past_price = rows[tsmom_previous][1]
            entry_price = rows[entry_timestamp][0]
            future_price = rows[exit_timestamp][0]
            cross_momentum = _finite(current_price / past_price - 1.0)
            time_momentum = _finite(current_price / tsmom_past_price - 1.0)
            forward_return = _finite(future_price / entry_price - 1.0)
            if None in {cross_momentum, time_momentum, forward_return}:
                continue
            trailing_notional = 0.0
            for offset in range(24):
                point = rows.get(timestamp - offset * hour_ms)
                if point is not None:
                    trailing_notional += point[1] * point[2]
            if trailing_notional <= 0.0:
                continue
            ranked.append(
                (
                    symbol,
                    trailing_notional,
                    cross_momentum,
                    time_momentum,
                    forward_return,
                )
            )
        ranked.sort(key=lambda row: row[1], reverse=True)
        eligible = ranked[:universe_limit]
        if len(eligible) < 2 * basket + 2:
            if invested:
                skipped_invested_windows += 1
            continue
        by_momentum = sorted(eligible, key=lambda row: row[2])
        shorts = by_momentum[:basket]
        longs = by_momentum[-basket:]
        long_return = statistics.mean(row[4] for row in longs)
        short_return = statistics.mean(row[4] for row in shorts)
        cross_weights = {
            **{row[0]: 0.5 / basket for row in longs},
            **{row[0]: -0.5 / basket for row in shorts},
        }
        cross_turnover = _weight_turnover(previous_cross_weights, cross_weights)
        cross_turnovers.append(cross_turnover)
        cross_cost = max(0.0, float(one_way_cost_bps)) * cross_turnover
        funding_drag = max(0.0, float(funding_drag_bps_per_day)) * (
            max(1, int(holding_hours)) / 24.0
        )
        cross_sectional.append(
            0.5 * (long_return - short_return) * 10_000.0
            - cross_cost
            - funding_drag
        )
        previous_cross_weights = cross_weights

        long_only_weights = {row[0]: 1.0 / basket for row in longs}
        long_only_cost = max(0.0, float(one_way_cost_bps)) * _weight_turnover(
            previous_long_only_weights, long_only_weights
        )
        winner_long_only.append(
            long_return * 10_000.0 - long_only_cost - funding_drag
        )
        previous_long_only_weights = long_only_weights

        btc_rows = [
            row
            for row in eligible
            if str(row[0]).upper().split("/")[0].split("_")[0].startswith("BTC")
        ]
        # Keep every reported variant on one unit of gross capital. The hedge
        # therefore allocates 0.5 gross long and 0.5 gross short instead of
        # silently comparing a 2x-gross book with the 1x baselines.
        market_hedged_weights = {
            symbol: 0.5 * weight for symbol, weight in long_only_weights.items()
        }
        if btc_rows:
            benchmark_return = btc_rows[0][4]
            benchmark_symbol = btc_rows[0][0]
            market_hedged_weights[benchmark_symbol] = (
                market_hedged_weights.get(benchmark_symbol, 0.0) - 0.5
            )
            market_hedge_benchmarks.append("BTC")
        else:
            benchmark_return = statistics.mean(row[4] for row in eligible)
            for row in eligible:
                market_hedged_weights[row[0]] = (
                    market_hedged_weights.get(row[0], 0.0) - 0.5 / len(eligible)
                )
            market_hedge_benchmarks.append("eligible_equal_weight")
        market_hedged_cost = max(
            0.0, float(one_way_cost_bps)
        ) * _weight_turnover(previous_market_hedged_weights, market_hedged_weights)
        winner_market_hedged.append(
            0.5 * (long_return - benchmark_return) * 10_000.0
            - market_hedged_cost
            - funding_drag
        )
        previous_market_hedged_weights = market_hedged_weights

        liquid_pool_size = min(
            len(eligible), max(2 * basket + 2, universe_limit // 2)
        )
        liquid_pool = eligible[:liquid_pool_size]
        long_symbols = {row[0] for row in longs}
        liquid_short_rows = [
            row
            for row in sorted(liquid_pool, key=lambda candidate: candidate[2])
            if row[0] not in long_symbols
        ][:basket]
        if len(liquid_short_rows) == basket:
            liquid_short_weights = {
                **{row[0]: 0.5 / basket for row in longs},
                **{row[0]: -0.5 / basket for row in liquid_short_rows},
            }
            liquid_short_cost = max(
                0.0, float(one_way_cost_bps)
            ) * _weight_turnover(
                previous_liquid_short_weights, liquid_short_weights
            )
            liquid_short.append(
                0.5
                * (
                    long_return
                    - statistics.mean(row[4] for row in liquid_short_rows)
                )
                * 10_000.0
                - liquid_short_cost
                - funding_drag
            )
            previous_liquid_short_weights = liquid_short_weights
            liquidity_pool_sizes.append(liquid_pool_size)

        momentum_values = [row[2] for row in eligible]
        dispersion = (
            statistics.pstdev(momentum_values)
            if len(momentum_values) > 1
            else 0.0
        )
        momentum_separation = statistics.mean(row[2] for row in longs) - statistics.mean(
            row[2] for row in shorts
        )
        dispersion_exposure = (
            min(1.0, max(0.0, momentum_separation / dispersion))
            if dispersion > 0.0
            else 0.0
        )
        dispersion_weights = {
            symbol: dispersion_exposure * weight
            for symbol, weight in cross_weights.items()
        }
        dispersion_cost = max(
            0.0, float(one_way_cost_bps)
        ) * _weight_turnover(previous_dispersion_weights, dispersion_weights)
        dispersion_scaled.append(
            dispersion_exposure
            * 0.5
            * (long_return - short_return)
            * 10_000.0
            - dispersion_cost
            - dispersion_exposure * funding_drag
        )
        dispersion_exposures.append(dispersion_exposure)
        previous_dispersion_weights = dispersion_weights

        rank_by_symbol = {
            row[0]: rank for rank, row in enumerate(by_momentum)
        }
        buffer = max(0, int(hold_rank_buffer))
        held_longs = [
            row
            for row in reversed(by_momentum)
            if previous_hysteresis_weights.get(row[0], 0.0) > 0.0
            and rank_by_symbol[row[0]] >= len(by_momentum) - basket - buffer
        ][:basket]
        held_long_symbols = {row[0] for row in held_longs}
        hysteresis_longs = list(held_longs)
        for row in reversed(by_momentum):
            if len(hysteresis_longs) >= basket:
                break
            if row[0] not in held_long_symbols:
                hysteresis_longs.append(row)
                held_long_symbols.add(row[0])
        held_shorts = [
            row
            for row in by_momentum
            if previous_hysteresis_weights.get(row[0], 0.0) < 0.0
            and rank_by_symbol[row[0]] < basket + buffer
            and row[0] not in held_long_symbols
        ][:basket]
        held_short_symbols = {row[0] for row in held_shorts}
        hysteresis_shorts = list(held_shorts)
        for row in by_momentum:
            if len(hysteresis_shorts) >= basket:
                break
            if (
                row[0] not in held_short_symbols
                and row[0] not in held_long_symbols
            ):
                hysteresis_shorts.append(row)
                held_short_symbols.add(row[0])
        if len(hysteresis_longs) == basket and len(hysteresis_shorts) == basket:
            hysteresis_weights = {
                **{row[0]: 0.5 / basket for row in hysteresis_longs},
                **{row[0]: -0.5 / basket for row in hysteresis_shorts},
            }
            hysteresis_turnover = _weight_turnover(
                previous_hysteresis_weights, hysteresis_weights
            )
            hysteresis_turnovers.append(hysteresis_turnover)
            hysteresis_gross = 0.5 * (
                statistics.mean(row[4] for row in hysteresis_longs)
                - statistics.mean(row[4] for row in hysteresis_shorts)
            ) * 10_000.0
            cross_hysteresis.append(
                hysteresis_gross
                - max(0.0, float(one_way_cost_bps)) * hysteresis_turnover
                - funding_drag
            )
            previous_hysteresis_weights = hysteresis_weights

        time_weights = {
            row[0]: (1.0 if row[3] >= 0.0 else -1.0) / len(eligible)
            for row in eligible
        }
        gross_time_bps = sum(
            time_weights[row[0]] * row[4] for row in eligible
        ) * 10_000.0
        time_cost = max(0.0, float(one_way_cost_bps)) * _weight_turnover(
            previous_time_weights, time_weights
        )
        time_net = gross_time_bps - time_cost - funding_drag
        time_series.append(time_net)
        previous_time_weights = time_weights

        exposure = _crash_exposure(time_series[:-1])
        guarded_weights = {
            symbol: exposure * weight for symbol, weight in time_weights.items()
        }
        guarded_cost = max(0.0, float(one_way_cost_bps)) * _weight_turnover(
            previous_guarded_weights, guarded_weights
        )
        guarded_time_series.append(
            exposure * gross_time_bps - guarded_cost - exposure * funding_drag
        )
        previous_guarded_weights = guarded_weights
    if cross_sectional and previous_cross_weights:
        cross_sectional[-1] -= max(0.0, float(one_way_cost_bps)) * sum(
            abs(weight) for weight in previous_cross_weights.values()
        )
    if time_series and previous_time_weights:
        time_series[-1] -= max(0.0, float(one_way_cost_bps)) * sum(
            abs(weight) for weight in previous_time_weights.values()
        )
    if guarded_time_series and previous_guarded_weights:
        guarded_time_series[-1] -= max(0.0, float(one_way_cost_bps)) * sum(
            abs(weight) for weight in previous_guarded_weights.values()
        )
    if cross_hysteresis and previous_hysteresis_weights:
        cross_hysteresis[-1] -= max(0.0, float(one_way_cost_bps)) * sum(
            abs(weight) for weight in previous_hysteresis_weights.values()
        )
    for values, weights in (
        (winner_long_only, previous_long_only_weights),
        (winner_market_hedged, previous_market_hedged_weights),
        (liquid_short, previous_liquid_short_weights),
        (dispersion_scaled, previous_dispersion_weights),
    ):
        if values and weights:
            values[-1] -= max(0.0, float(one_way_cost_bps)) * sum(
                abs(weight) for weight in weights.values()
            )
    cross_metrics = _performance(cross_sectional)
    hysteresis_metrics = _performance(cross_hysteresis)
    time_metrics = _performance(time_series)
    guarded = _performance(guarded_time_series)
    data_sufficient = (
        len(series) >= 20
        and cross_metrics["samples"] >= max(1, int(minimum_windows))
        and time_metrics["samples"] >= max(1, int(minimum_windows))
        and skipped_invested_windows == 0
    )
    ready = bool(data_sufficient and universe_proven)
    return {
        "symbols": len(series),
        "lookback_hours": max(1, int(lookback_hours)),
        "tsmom_lookback_hours": max(1, int(tsmom_lookback_hours)),
        "holding_hours": max(1, int(holding_hours)),
        "basket_size": basket,
        "universe_size": universe_limit,
        "one_way_cost_bps": max(0.0, float(one_way_cost_bps)),
        "funding_drag_bps_per_day": max(
            0.0, float(funding_drag_bps_per_day)
        ),
        "cost_model": "weight_turnover_plus_final_liquidation",
        "minimum_windows": max(1, int(minimum_windows)),
        "skipped_invested_windows": skipped_invested_windows,
        "position_continuity_valid": skipped_invested_windows == 0,
        "cross_sectional": cross_metrics,
        "cross_sectional_hysteresis": {
            **hysteresis_metrics,
            "hold_rank_buffer": max(0, int(hold_rank_buffer)),
            "mean_gross_turnover": (
                statistics.mean(hysteresis_turnovers)
                if hysteresis_turnovers
                else None
            ),
            "baseline_mean_gross_turnover": (
                statistics.mean(cross_turnovers) if cross_turnovers else None
            ),
        },
        "cross_sectional_variants": {
            "winner_long_only": _performance(winner_long_only),
            "winner_market_hedged": {
                **_performance(winner_market_hedged),
                "target_gross_exposure": 1.0,
                "long_gross_exposure": 0.5,
                "short_gross_exposure": 0.5,
                "funding_drag_included": True,
                "benchmark": (
                    "BTC"
                    if market_hedge_benchmarks
                    and all(value == "BTC" for value in market_hedge_benchmarks)
                    else "eligible_equal_weight"
                ),
            },
            "liquid_short": {
                **_performance(liquid_short),
                "mean_liquidity_pool_size": (
                    statistics.mean(liquidity_pool_sizes)
                    if liquidity_pool_sizes
                    else None
                ),
                "liquidity_proxy": "trailing_24h_notional",
            },
            "dispersion_scaled": {
                **_performance(dispersion_scaled),
                "mean_exposure": (
                    statistics.mean(dispersion_exposures)
                    if dispersion_exposures
                    else 0.0
                ),
                "uses_future_dispersion": False,
                "dispersion_source": "cross_momentum_at_feature_cutoff",
            },
        },
        "time_series": time_metrics,
        "crash_guard": guarded,
        "data_sufficient": data_sufficient,
        "ready": ready,
        "causality": {
            "universe_uses_future_volume": False,
            "universe_selector": "trailing_24h_notional_at_feature_cutoff",
            "signals_use_future_returns": False,
            "same_bar_close_execution": False,
            "signal_cutoff": "completed_bar_close",
            "execution_price": "next_bar_open",
            "exit_price": "holding_window_end_open",
            "point_in_time_universe_complete": universe_proven,
        },
        "research_only": True,
    }


def _ohlcv_source_record(
    *,
    path: Path,
    folder: Path,
    market_id: str,
    raw: bytes,
    source_rows: int,
    selected_rows: list[tuple[int, float, float, float]],
) -> dict:
    return {
        "path": path.relative_to(folder).as_posix(),
        "market_id": market_id,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "source_rows": source_rows,
        "selected_rows": len(selected_rows),
        "first_timestamp": selected_rows[0][0],
        "last_timestamp": selected_rows[-1][0],
    }


def _selected_ohlcv_rows(
    source_rows: list,
    *,
    as_of_ms: int,
    bar_limit: int,
) -> list[tuple[int, float, float, float]]:
    row_candidates = []
    for bar in source_rows:
        timestamp = int(bar[0])
        timestamp_number = float(bar[0])
        open_price = _finite(bar[1])
        close = _finite(bar[4])
        volume = _finite(bar[5])
        if (
            not math.isfinite(timestamp_number)
            or timestamp <= 0
            or timestamp_number != timestamp
            or timestamp + 3_600_000 > as_of_ms
            or open_price is None
            or open_price <= 0.0
            or close is None
            or close <= 0.0
            or volume is None
            or volume < 0.0
        ):
            continue
        row_candidates.append((timestamp, open_price, close, volume))
    return [
        (timestamp, opening, close, volume)
        for timestamp, (opening, close, volume) in sorted(
            _series_map(row_candidates).items()
        )[-bar_limit:]
    ]


def _finalize_ohlcv_source(
    *,
    candidate_files: int,
    artifacts: list[dict],
    rejection_counts: dict[str, int],
    omitted_due_to_symbol_limit: int,
    max_symbols: int,
    max_bars: int,
    maximum_file_bytes: int,
    as_of_ms: int,
) -> dict:
    rejected_files = sum(rejection_counts.values())
    source = {
        "schema_version": 2,
        "as_of_ms": as_of_ms,
        "candidate_files": candidate_files,
        "accepted_files": len(artifacts),
        "rejected_files": rejected_files,
        "omitted_due_to_symbol_limit": omitted_due_to_symbol_limit,
        "rejection_counts": dict(sorted(rejection_counts.items())),
        "complete": bool(
            candidate_files > 0
            and rejected_files == 0
            and omitted_due_to_symbol_limit == 0
            and len(artifacts) == candidate_files
            and len(artifacts) + rejected_files + omitted_due_to_symbol_limit
            == candidate_files
        ),
        "limits": {
            "max_symbols": max_symbols,
            "max_bars": max_bars,
            "maximum_file_bytes": maximum_file_bytes,
        },
        "artifacts": sorted(artifacts, key=lambda item: item["path"]),
    }
    encoded = json.dumps(
        source,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    source["snapshot_sha256"] = hashlib.sha256(encoded).hexdigest()
    return source


def _load_cached_ohlcv_snapshot_unlocked(
    root: str | Path,
    *,
    max_symbols: int = 100,
    max_bars: int = 8_760,
    maximum_file_bytes: int = 50_000_000,
    as_of_ms: int,
) -> dict:
    from tools.ohlcv_cache import (
        decode_ohlcv_cache_payload,
        ohlcv_cache_filename,
    )

    symbol_limit = max(1, int(max_symbols))
    bar_limit = max(1, int(max_bars))
    file_limit = max(1, int(maximum_file_bytes))

    def empty_source(*, rejection_counts=None) -> dict:
        return _finalize_ohlcv_source(
            candidate_files=0,
            artifacts=[],
            rejection_counts=rejection_counts or {},
            omitted_due_to_symbol_limit=0,
            max_symbols=symbol_limit,
            max_bars=bar_limit,
            maximum_file_bytes=file_limit,
            as_of_ms=as_of_ms,
        )

    try:
        folder = _absolute_without_links(
            Path(root) / "data" / "ohlcv_cache", label="OHLCV cache directory"
        )
    except ValueError:
        return {
            "panel": {},
            "source": empty_source(
                rejection_counts={"invalid_cache_root": 1}
            ),
        }
    if not folder.is_dir():
        return {"panel": {}, "source": empty_source()}
    try:
        candidates = sorted(folder.glob("*/*__1h.json"))
    except OSError:
        return {
            "panel": {},
            "source": empty_source(rejection_counts={"discovery_error": 1}),
        }
    panel = {}
    conflicted = set()
    artifact_by_market = {}
    rejection_counts = {}
    omitted = 0

    def reject(reason: str, count: int = 1) -> None:
        rejection_counts[reason] = rejection_counts.get(reason, 0) + count

    for index, path in enumerate(candidates):
        if len(panel) >= symbol_limit:
            omitted = len(candidates) - index
            break
        try:
            path = _absolute_without_links(path, label="OHLCV cache artifact")
        except ValueError:
            reject("linked_or_invalid_path")
            continue
        try:
            with path.open("rb") as handle:
                before = os.fstat(handle.fileno())
                raw = handle.read(file_limit + 1)
                after = os.fstat(handle.fileno())
        except OSError:
            reject("unreadable_artifact")
            continue
        if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
            reject("changed_during_read")
            continue
        if before.st_size > file_limit or len(raw) > file_limit:
            reject("oversized_artifact")
            continue
        if len(raw) != before.st_size:
            reject("changed_during_read")
            continue
        try:
            path = _absolute_without_links(path, label="OHLCV cache artifact")
        except ValueError:
            reject("linked_or_invalid_path")
            continue
        try:
            payload = decode_ohlcv_cache_payload(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            reject("invalid_payload")
            continue
        exchange = payload["exchange"]
        symbol = payload["symbol"]
        if (
            exchange is None
            or path.parent.name != exchange
            or path.name != ohlcv_cache_filename(symbol, payload["timeframe"])
            or payload["timeframe"] != "1h"
        ):
            reject("identity_mismatch")
            continue
        rows = _selected_ohlcv_rows(
            payload["rows"], as_of_ms=as_of_ms, bar_limit=bar_limit
        )
        if not rows:
            reject("no_completed_rows")
            continue
        market_id = f"{exchange}:{symbol}"
        if market_id in conflicted:
            reject("duplicate_market_identity")
            continue
        if market_id in panel:
            panel.pop(market_id, None)
            artifact_by_market.pop(market_id, None)
            conflicted.add(market_id)
            reject("duplicate_market_identity", 2)
            continue
        panel[market_id] = rows
        artifact_by_market[market_id] = _ohlcv_source_record(
            path=path,
            folder=folder,
            market_id=market_id,
            raw=raw,
            source_rows=len(payload["rows"]),
            selected_rows=rows,
        )
    source = _finalize_ohlcv_source(
        candidate_files=len(candidates),
        artifacts=list(artifact_by_market.values()),
        rejection_counts=rejection_counts,
        omitted_due_to_symbol_limit=omitted,
        max_symbols=symbol_limit,
        max_bars=bar_limit,
        maximum_file_bytes=file_limit,
        as_of_ms=as_of_ms,
    )
    return {"panel": panel, "source": source}


@contextmanager
def _locked_ohlcv_cache_snapshot(folder: Path):
    from tools.ohlcv_cache import OHLCV_CACHE_NAMESPACE_LOCK

    with ExitStack() as stack:
        namespace_lock_path = _absolute_without_links(
            folder / OHLCV_CACHE_NAMESPACE_LOCK,
            label="OHLCV cache namespace lock",
        )
        stack.enter_context(
            portalocker.Lock(
                str(namespace_lock_path),
                mode="a",
                timeout=30,
                check_interval=0.05,
                fail_when_locked=False,
            )
        )
        try:
            candidates = sorted(folder.glob("*/*__1h.json"))
        except OSError as exc:
            raise ValueError("OHLCV cache discovery is unavailable") from exc
        for candidate in candidates:
            try:
                path = _absolute_without_links(
                    candidate, label="OHLCV cache artifact"
                )
            except ValueError:
                continue
            if not path.is_file():
                continue
            lock_path = _absolute_without_links(
                f"{path}.lock", label="OHLCV source lock"
            )
            stack.enter_context(
                portalocker.Lock(
                    str(lock_path),
                    mode="a",
                    timeout=30,
                    check_interval=0.05,
                    fail_when_locked=False,
                )
            )
        try:
            stable_candidates = sorted(folder.glob("*/*__1h.json"))
        except OSError as exc:
            raise ValueError("OHLCV cache discovery is unavailable") from exc
        if stable_candidates != candidates:
            raise ValueError("OHLCV cache candidate set changed during snapshot lock")
        yield


def load_cached_ohlcv_snapshot(
    root: str | Path,
    *,
    max_symbols: int = 100,
    max_bars: int = 8_760,
    maximum_file_bytes: int = 50_000_000,
    as_of_ms: int | None = None,
) -> dict:
    if as_of_ms is None:
        wallclock_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        cutoff_ms = wallclock_ms // 3_600_000 * 3_600_000
    else:
        if isinstance(as_of_ms, bool) or not isinstance(as_of_ms, int):
            raise TypeError("OHLCV as_of_ms must be an integer")
        cutoff_ms = as_of_ms
    if cutoff_ms <= 0 or cutoff_ms % 3_600_000 != 0:
        raise ValueError("OHLCV as_of_ms must be positive and aligned to the 1h grid")
    try:
        folder = _absolute_without_links(
            Path(root) / "data" / "ohlcv_cache", label="OHLCV cache directory"
        )
    except ValueError:
        return _load_cached_ohlcv_snapshot_unlocked(
            root,
            max_symbols=max_symbols,
            max_bars=max_bars,
            maximum_file_bytes=maximum_file_bytes,
            as_of_ms=cutoff_ms,
        )
    if not folder.is_dir():
        return _load_cached_ohlcv_snapshot_unlocked(
            root,
            max_symbols=max_symbols,
            max_bars=max_bars,
            maximum_file_bytes=maximum_file_bytes,
            as_of_ms=cutoff_ms,
        )
    with _locked_ohlcv_cache_snapshot(folder):
        return _load_cached_ohlcv_snapshot_unlocked(
            root,
            max_symbols=max_symbols,
            max_bars=max_bars,
            maximum_file_bytes=maximum_file_bytes,
            as_of_ms=cutoff_ms,
        )


def load_cached_ohlcv_panel(
    root: str | Path,
    *,
    max_symbols: int = 100,
    max_bars: int = 8_760,
    maximum_file_bytes: int = 50_000_000,
    as_of_ms: int | None = None,
) -> dict[str, list[tuple[int, float, float, float]]]:
    return load_cached_ohlcv_snapshot(
        root,
        max_symbols=max_symbols,
        max_bars=max_bars,
        maximum_file_bytes=maximum_file_bytes,
        as_of_ms=as_of_ms,
    )["panel"]


_OHLCV_SOURCE_V1_KEYS = {
    "schema_version",
    "candidate_files",
    "accepted_files",
    "rejected_files",
    "omitted_due_to_symbol_limit",
    "rejection_counts",
    "complete",
    "limits",
    "artifacts",
    "snapshot_sha256",
}
_OHLCV_SOURCE_KEYS = _OHLCV_SOURCE_V1_KEYS | {"as_of_ms"}
_OHLCV_ARTIFACT_KEYS = {
    "path",
    "market_id",
    "sha256",
    "bytes",
    "source_rows",
    "selected_rows",
    "first_timestamp",
    "last_timestamp",
}


def _strict_nonnegative_int(value, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a nonnegative integer")
    return value


def _validated_ohlcv_source_contract(source: dict) -> list[dict]:
    if not isinstance(source, dict):
        raise TypeError("OHLCV panel source contract is invalid")
    schema_version = source.get("schema_version")
    if type(schema_version) is not int or schema_version not in {1, 2}:
        raise ValueError("OHLCV panel source schema is invalid")
    expected_keys = (
        _OHLCV_SOURCE_KEYS if schema_version == 2 else _OHLCV_SOURCE_V1_KEYS
    )
    if set(source) != expected_keys:
        raise ValueError("OHLCV panel source contract is invalid")
    if schema_version == 2:
        as_of_ms = _strict_nonnegative_int(
            source["as_of_ms"], label="OHLCV as_of_ms"
        )
        if as_of_ms <= 0 or as_of_ms % 3_600_000 != 0:
            raise ValueError("OHLCV as_of_ms must be aligned to the 1h grid")
    candidate_files = _strict_nonnegative_int(
        source["candidate_files"], label="OHLCV candidate_files"
    )
    accepted_files = _strict_nonnegative_int(
        source["accepted_files"], label="OHLCV accepted_files"
    )
    rejected_files = _strict_nonnegative_int(
        source["rejected_files"], label="OHLCV rejected_files"
    )
    omitted = _strict_nonnegative_int(
        source["omitted_due_to_symbol_limit"],
        label="OHLCV omitted_due_to_symbol_limit",
    )
    if not isinstance(source["complete"], bool):
        raise TypeError("OHLCV panel source complete must be boolean")
    rejection_counts = source["rejection_counts"]
    if not isinstance(rejection_counts, dict) or any(
        not isinstance(reason, str)
        or not reason
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count <= 0
        for reason, count in rejection_counts.items()
    ):
        raise ValueError("OHLCV rejection counts are invalid")
    if sum(rejection_counts.values()) != rejected_files:
        raise ValueError("OHLCV rejected file count is inconsistent")
    limits = source["limits"]
    if not isinstance(limits, dict) or set(limits) != {
        "max_symbols",
        "max_bars",
        "maximum_file_bytes",
    }:
        raise ValueError("OHLCV panel source limits are invalid")
    for label, value in limits.items():
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"OHLCV {label} must be a positive integer")
    artifacts = source["artifacts"]
    if not isinstance(artifacts, list) or len(artifacts) != accepted_files:
        raise ValueError("OHLCV accepted artifact count is inconsistent")
    paths = set()
    market_ids = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict) or set(artifact) != _OHLCV_ARTIFACT_KEYS:
            raise ValueError("OHLCV source artifact contract is invalid")
        relative = artifact["path"]
        market_id = artifact["market_id"]
        digest = artifact["sha256"]
        if (
            not isinstance(relative, str)
            or "\\" in relative
            or len(relative.split("/")) != 2
            or any(part in {"", ".", ".."} for part in relative.split("/"))
            or relative in paths
        ):
            raise ValueError("OHLCV source artifact path is invalid")
        if not isinstance(market_id, str) or not market_id or market_id in market_ids:
            raise ValueError("OHLCV source market identity is invalid")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or digest != digest.lower()
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError("OHLCV source artifact digest is invalid")
        byte_count = _strict_nonnegative_int(
            artifact["bytes"], label="OHLCV artifact bytes"
        )
        source_rows = _strict_nonnegative_int(
            artifact["source_rows"], label="OHLCV source rows"
        )
        selected_rows = _strict_nonnegative_int(
            artifact["selected_rows"], label="OHLCV selected rows"
        )
        first_timestamp = _strict_nonnegative_int(
            artifact["first_timestamp"], label="OHLCV first timestamp"
        )
        last_timestamp = _strict_nonnegative_int(
            artifact["last_timestamp"], label="OHLCV last timestamp"
        )
        if (
            byte_count == 0
            or byte_count > limits["maximum_file_bytes"]
            or source_rows == 0
            or selected_rows == 0
            or selected_rows > source_rows
            or first_timestamp > last_timestamp
        ):
            raise ValueError("OHLCV source artifact bounds are invalid")
        paths.add(relative)
        market_ids.add(market_id)
    expected_complete = bool(
        candidate_files > 0
        and rejected_files == 0
        and omitted == 0
        and accepted_files == candidate_files
        and accepted_files + rejected_files + omitted == candidate_files
    )
    if source["complete"] is not expected_complete:
        raise ValueError("OHLCV panel source completeness is inconsistent")
    if accepted_files + rejected_files + omitted != candidate_files:
        raise ValueError("OHLCV panel source accounting is inconsistent")
    snapshot_digest = source["snapshot_sha256"]
    if (
        not isinstance(snapshot_digest, str)
        or len(snapshot_digest) != 64
        or snapshot_digest != snapshot_digest.lower()
        or any(
            character not in "0123456789abcdef"
            for character in snapshot_digest
        )
    ):
        raise ValueError("OHLCV panel source digest is invalid")
    fingerprint_input = dict(source)
    fingerprint_input.pop("snapshot_sha256")
    encoded = json.dumps(
        fingerprint_input,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if hashlib.sha256(encoded).hexdigest() != snapshot_digest:
        raise ValueError("OHLCV panel source digest mismatch")
    return artifacts


def _verify_ohlcv_artifact_bytes(raw: bytes, artifact: dict, source: dict) -> None:
    from tools.ohlcv_cache import (
        decode_ohlcv_cache_payload,
        ohlcv_cache_filename,
    )

    if (
        len(raw) != artifact["bytes"]
        or hashlib.sha256(raw).hexdigest() != artifact["sha256"]
    ):
        raise ValueError("OHLCV source artifact digest mismatch")
    try:
        payload = decode_ohlcv_cache_payload(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("OHLCV source artifact payload is invalid") from exc
    parts = artifact["path"].split("/")
    exchange = payload["exchange"]
    symbol = payload["symbol"]
    if (
        exchange is None
        or artifact["market_id"] != f"{exchange}:{symbol}"
        or parts[0] != exchange
        or parts[1] != ohlcv_cache_filename(symbol, payload["timeframe"])
        or payload["timeframe"] != "1h"
        or len(payload["rows"]) != artifact["source_rows"]
    ):
        raise ValueError("OHLCV source artifact identity mismatch")
    timestamps = [int(row[0]) for row in payload["rows"]]
    try:
        first_index = timestamps.index(artifact["first_timestamp"])
        last_index = timestamps.index(artifact["last_timestamp"])
    except ValueError as exc:
        raise ValueError("OHLCV selected row bounds are unavailable") from exc
    if last_index - first_index + 1 != artifact["selected_rows"]:
        raise ValueError("OHLCV selected row bounds are inconsistent")
    if source["schema_version"] == 2:
        selected_rows = _selected_ohlcv_rows(
            payload["rows"],
            as_of_ms=source["as_of_ms"],
            bar_limit=source["limits"]["max_bars"],
        )
        if (
            not selected_rows
            or len(selected_rows) != artifact["selected_rows"]
            or selected_rows[0][0] != artifact["first_timestamp"]
            or selected_rows[-1][0] != artifact["last_timestamp"]
        ):
            raise ValueError("OHLCV exact selected rows are inconsistent")


def verify_ohlcv_panel_source(root: str | Path, source: dict) -> dict:
    artifacts = _validated_ohlcv_source_contract(source)
    project = _absolute_without_links(root, label="OHLCV source root")
    if not project.is_dir():
        raise ValueError("OHLCV source root must be a real directory")
    folder = _absolute_without_links(
        project / "data" / "ohlcv_cache", label="OHLCV cache directory"
    )
    if artifacts and not folder.is_dir():
        raise ValueError("OHLCV cache directory is unavailable")
    artifact_paths = set()
    for artifact in artifacts:
        parts = artifact["path"].split("/")
        path = _absolute_without_links(
            folder.joinpath(*parts), label="OHLCV source artifact"
        )
        if not path.is_file():
            raise ValueError("OHLCV source artifact is unavailable")
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            raw = handle.read(artifact["bytes"] + 1)
            after = os.fstat(handle.fileno())
        path = _absolute_without_links(path, label="OHLCV source artifact")
        if (
            before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or len(raw) != artifact["bytes"]
            or before.st_size != artifact["bytes"]
        ):
            raise ValueError("OHLCV source artifact size or read stability mismatch")
        _verify_ohlcv_artifact_bytes(raw, artifact, source)
        artifact_paths.add(artifact["path"])
    if source["complete"]:
        try:
            current_paths = {
                _absolute_without_links(
                    path, label="OHLCV source artifact"
                ).relative_to(folder).as_posix()
                for path in folder.glob("*/*__1h.json")
            }
        except OSError as exc:
            raise ValueError("OHLCV cache discovery is unavailable") from exc
        if current_paths != artifact_paths:
            raise ValueError("OHLCV source candidate set mismatch")
    return _ohlcv_source_verification_result(source)


def _ohlcv_source_verification_result(source: dict) -> dict:
    complete = source["complete"] is True
    result = {
        "state": "VERIFIED_COMPLETE" if complete else "VERIFIED_ACCEPTED_ONLY",
        "snapshot_sha256": source["snapshot_sha256"],
        "accepted_files": source["accepted_files"],
        "complete": complete,
        "accepted_source_bytes_match": True,
        "complete_candidate_set_match": complete,
        "source_bytes_reproducible": complete,
    }
    if source["schema_version"] == 2:
        result.update(
            {
                "source_schema_version": 2,
                "as_of_ms": source["as_of_ms"],
            }
        )
    return result


@contextmanager
def _locked_ohlcv_panel_sources(root: str | Path, source: dict):
    from tools.ohlcv_cache import OHLCV_CACHE_NAMESPACE_LOCK

    artifacts = _validated_ohlcv_source_contract(source)
    project = _absolute_without_links(root, label="OHLCV source root")
    folder = _absolute_without_links(
        project / "data" / "ohlcv_cache", label="OHLCV cache directory"
    )
    with ExitStack() as stack:
        if folder.is_dir():
            namespace_lock_path = _absolute_without_links(
                folder / OHLCV_CACHE_NAMESPACE_LOCK,
                label="OHLCV cache namespace lock",
            )
            stack.enter_context(
                portalocker.Lock(
                    str(namespace_lock_path),
                    mode="a",
                    timeout=30,
                    check_interval=0.05,
                    fail_when_locked=False,
                )
            )
        for artifact in sorted(artifacts, key=lambda item: item["path"]):
            path = _absolute_without_links(
                folder.joinpath(*artifact["path"].split("/")),
                label="OHLCV source artifact",
            )
            if not path.is_file():
                raise ValueError("OHLCV source artifact is unavailable")
            lock_path = _absolute_without_links(
                f"{path}.lock", label="OHLCV source lock"
            )
            stack.enter_context(
                portalocker.Lock(
                    str(lock_path),
                    mode="a",
                    timeout=30,
                    check_interval=0.05,
                    fail_when_locked=False,
                )
            )
        yield


def evaluate_regime_shift(
    panel: dict[str, list[tuple[int, float, float, float]]],
    *,
    point_in_time_universe_complete: bool = False,
) -> dict:
    universe_proven = point_in_time_universe_complete is True
    series = {symbol: _series_map(rows) for symbol, rows in panel.items()}
    by_time: dict[int, list[float]] = {}
    hour_ms = 3_600_000
    for rows in series.values():
        for timestamp, (_opening, price, _volume) in rows.items():
            previous = rows.get(timestamp - hour_ms)
            if previous is not None:
                by_time.setdefault(timestamp, []).append(price / previous[1] - 1.0)
    market_points = [
        (timestamp, statistics.mean(by_time[timestamp]))
        for timestamp in sorted(by_time)
        if len(by_time[timestamp]) >= 5
    ]
    market_returns = [value for _timestamp, value in market_points]
    contiguous_returns = []
    previous_timestamp = None
    for timestamp, value in market_points:
        if previous_timestamp is None or timestamp == previous_timestamp + hour_ms:
            contiguous_returns.append(value)
        else:
            contiguous_returns = [value]
        previous_timestamp = timestamp
    recent_count = 7 * 24
    prior_count = 28 * 24
    required_count = recent_count + prior_count
    if len(contiguous_returns) < required_count:
        return {
            "ready": False,
            "data_sufficient": False,
            "samples": len(market_returns),
            "contiguous_samples": len(contiguous_returns),
            "minimum_samples": required_count,
            "shadow_alert": False,
            "method": "distribution_shift_shadow_v1",
            "causality": {
                "point_in_time_universe_complete": universe_proven
            },
        }
    recent = contiguous_returns[-recent_count:]
    prior = contiguous_returns[-required_count:-recent_count]
    prior_vol = statistics.stdev(prior) if len(prior) > 1 else 0.0
    recent_vol = statistics.stdev(recent) if len(recent) > 1 else 0.0
    volatility_ratio = recent_vol / prior_vol if prior_vol > 0.0 else None
    mean_shift_z = (
        abs(statistics.mean(recent) - statistics.mean(prior)) / prior_vol
        if prior_vol > 0.0
        else None
    )
    alert = bool(
        (volatility_ratio is not None and volatility_ratio >= 2.0)
        or (mean_shift_z is not None and mean_shift_z >= 0.5)
    )
    return {
        "ready": universe_proven,
        "data_sufficient": True,
        "samples": len(market_returns),
        "contiguous_samples": len(contiguous_returns),
        "minimum_samples": required_count,
        "recent_hours": recent_count,
        "prior_hours": prior_count,
        "volatility_ratio": volatility_ratio,
        "mean_shift_in_prior_vols": mean_shift_z,
        "shadow_alert": alert,
        "method": "distribution_shift_shadow_v1",
        "changes_orders": False,
        "causality": {
            "point_in_time_universe_complete": universe_proven
        },
    }


def _projected_funding_periods_24h(median_interval_hours: float | None) -> float:
    """Expected settlement count in 24h, bounded without rounding upward."""
    interval = _finite(median_interval_hours)
    if interval is None or interval <= 0.0:
        return 1.0
    return min(3.0, 24.0 / interval)


def _verified_carry_capture_scope(venue_root: Path) -> dict | None:
    """Bind production carry evidence to immutable closed-day capture seals."""
    integrity_root = venue_root / "integrity"
    if not integrity_root.is_dir():
        return None
    from trading.capture_integrity import (
        _verify_sealed_report,
        capture_continuity_health,
    )

    eligible: dict[str, tuple[tuple[datetime, datetime], ...]] = {}
    verified_reports = []
    invalid_days = 0
    seal_errors = 0
    for report_path in sorted(integrity_root.glob("*.json")):
        try:
            if report_path.stat().st_size > RESEARCH_VENUE_PAYLOAD_MAX_BYTES:
                raise ValueError("capture seal is oversized")
            report = json.loads(
                report_path.read_text(encoding="utf-8"),
                parse_constant=_reject_research_json_constant,
                object_pairs_hook=_strict_research_json_object,
            )
            _verify_sealed_report(
                venue_root, report, expected_day=report_path.stem
            )
            day = str(report.get("day") or "")
            if day != report_path.stem:
                raise ValueError("capture seal day does not match its path")
            status = report.get("status")
            if status == "invalid":
                invalid_days += 1
                verified_reports.append(report)
                continue
            if status not in {"valid", "usable_with_gaps"}:
                raise ValueError("capture seal status is unsupported")
            expected_partition = f"overview/{day}.sqlite3"
            manifest_paths = {
                item.get("path")
                for item in report.get("manifest") or ()
                if isinstance(item, dict)
            }
            if expected_partition not in manifest_paths:
                raise ValueError("capture seal does not bind overview partition")
            availability = report.get("availability")
            if (
                not isinstance(availability, dict)
                or availability.get("contract")
                != "bounded_outage_exclusion_v1"
                or availability.get("within_daily_budget") is not True
                or not isinstance(
                    availability.get("exclusion_intervals"), list
                )
            ):
                raise ValueError("capture seal availability is invalid")
            exclusions = []
            for interval in availability["exclusion_intervals"]:
                if not isinstance(interval, dict):
                    raise ValueError("capture exclusion interval is invalid")
                started = _utc_datetime(interval.get("start"))
                ended = _utc_datetime(interval.get("end"))
                if started is None or ended is None or ended <= started:
                    raise ValueError("capture exclusion interval is invalid")
                exclusions.append((started, ended))
            if status == "valid" and exclusions:
                raise ValueError("valid capture seal contains exclusions")
            eligible[day] = tuple(sorted(exclusions))
            verified_reports.append(report)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            seal_errors += 1
    continuity = capture_continuity_health(verified_reports)
    if continuity.get("ok") is True:
        start_day = str(continuity.get("start_day") or "")
        end_day = str(continuity.get("end_day") or "")
        eligible = {
            day: intervals
            for day, intervals in eligible.items()
            if start_day <= day <= end_day
        }
    return {
        "eligible": eligible,
        "invalid_days": invalid_days,
        "seal_errors": seal_errors,
        "continuity": continuity,
    }


def _carry_history_report(
    root: Path,
    *,
    minimum_samples: int = 90,
    require_verified_seals: bool = True,
) -> dict:
    from trading.carry_sim import CarryEngine, CarryState, CarryTerms

    grouped: dict[str, dict[str, dict]] = {}
    overview_counts: dict[str, int] = {}
    missing_settlement = 0
    noncausal_settlement_observations = 0
    unsettled_funding_periods: set[tuple[str, str]] = set()
    analysis_time = datetime.now(timezone.utc)
    folder = root / "data" / "venue_native" / "overview"
    capture_scope = _verified_carry_capture_scope(folder.parent)
    capture_contract = (
        "verified_closed_day_seals"
        if capture_scope is not None
        else "verified_closed_day_seals_missing"
        if require_verified_seals
        else "legacy_unsealed_test_fixture"
    )
    capture_window_ready = bool(
        not require_verified_seals
        or (
            capture_scope is not None
            and capture_scope["continuity"].get("ok") is True
        )
    )
    total_events = 0
    excluded_overview_events = 0
    skipped_unusable_partitions = 0
    for path in folder.glob("*.sqlite3"):
        exclusions: tuple[tuple[datetime, datetime], ...] = ()
        if capture_scope is None and require_verified_seals:
            skipped_unusable_partitions += 1
            continue
        if capture_scope is not None:
            scoped = capture_scope["eligible"].get(path.stem)
            if scoped is None:
                skipped_unusable_partitions += 1
                continue
            exclusions = scoped
        conn = None
        try:
            conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
            rows = conn.execute(
                "SELECT event_id, exchange_time, received_time, payload_json "
                "FROM venue_events "
                "WHERE length(CAST(payload_json AS BLOB)) <= ? "
                "ORDER BY exchange_time, received_time, event_id",
                (RESEARCH_VENUE_PAYLOAD_MAX_BYTES,),
            )
            for event_id, exchange_time, received_time, encoded in rows:
                snapshot_received_time = _utc_datetime(received_time)
                if snapshot_received_time is None:
                    continue
                if any(
                    started <= snapshot_received_time <= ended
                    for started, ended in exclusions
                ):
                    excluded_overview_events += 1
                    continue
                total_events += 1
                try:
                    payload = json.loads(
                        encoded,
                        parse_constant=_reject_research_json_constant,
                        object_pairs_hook=_strict_research_json_object,
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                markets = payload.get("markets") if isinstance(payload, dict) else None
                if not isinstance(markets, dict):
                    continue
                for market_id, market in markets.items():
                    if not isinstance(market, dict):
                        continue
                    funding = _finite(market.get("funding_rate"))
                    if funding is None:
                        continue
                    key = str(market_id)
                    overview_counts[key] = overview_counts.get(key, 0) + 1
                    settlement = market.get("next_settle_time")
                    if settlement is None or isinstance(settlement, bool):
                        missing_settlement += 1
                        continue
                    settlement_number = _finite(settlement)
                    if isinstance(settlement, (int, float)) and settlement_number is None:
                        missing_settlement += 1
                        continue
                    if settlement_number is not None:
                        if settlement_number <= 0.0:
                            missing_settlement += 1
                            continue
                        settlement_time = _utc_datetime(settlement_number)
                        if settlement_time is None:
                            missing_settlement += 1
                            continue
                        period_key = settlement_time.isoformat()
                    else:
                        period_key = str(settlement).strip()
                        if not period_key:
                            missing_settlement += 1
                            continue
                        settlement_time = _utc_datetime(period_key)
                        if settlement_time is None:
                            missing_settlement += 1
                            continue
                        period_key = settlement_time.isoformat()
                    if settlement_time > analysis_time:
                        unsettled_funding_periods.add((key, period_key))
                        continue
                    index_price = _finite(market.get("index_price"))
                    fair_price = _finite(market.get("fair_price"))
                    basis_bps = (
                        abs(fair_price / index_price - 1.0) * 10_000.0
                        if index_price is not None and index_price > 0.0
                        and fair_price is not None and fair_price > 0.0
                        else None
                    )
                    snapshot_time = _utc_datetime(exchange_time)
                    if (
                        snapshot_time is None
                        or snapshot_time > settlement_time
                        or snapshot_received_time > settlement_time
                    ):
                        noncausal_settlement_observations += 1
                        continue
                    snapshot_order = (
                        snapshot_time is not None,
                        snapshot_time or datetime.min.replace(tzinfo=timezone.utc),
                        snapshot_received_time is not None,
                        snapshot_received_time
                        or datetime.min.replace(tzinfo=timezone.utc),
                        str(exchange_time),
                        str(received_time),
                        str(event_id),
                    )
                    candidate = {
                        "exchange_time": str(exchange_time),
                        "settlement_time": settlement_time,
                        "funding": funding,
                        "spot_verified": market.get("spot_available") is True,
                        "basis_bps": basis_bps,
                        "quote_volume": _finite(market.get("quote_volume")),
                        "_snapshot_order": snapshot_order,
                    }
                    periods = grouped.setdefault(key, {})
                    previous = periods.get(period_key)
                    if (
                        previous is None
                        or snapshot_order > previous["_snapshot_order"]
                    ):
                        periods[period_key] = candidate
        except (OSError, sqlite3.Error):
            continue
        finally:
            if conn is not None:
                conn.close()
    summaries = []
    qualified = []
    required = max(1, int(minimum_samples))
    for market_id, periods in grouped.items():
        ordered_periods = sorted(
            periods.values(), key=lambda row: row["exchange_time"]
        )
        values = [row["funding"] for row in ordered_periods]
        positive_fraction = sum(value > 0.0 for value in values) / len(values)
        sign_flips = sum(
            (left > 0.0) != (right > 0.0)
            for left, right in itertools.pairwise(values)
        )
        sign_flip_rate = sign_flips / max(1, len(values) - 1)
        verified = sum(row["spot_verified"] for row in ordered_periods)
        settlement_times = sorted(
            {
                row["settlement_time"]
                for row in ordered_periods
                if row["settlement_time"] is not None
            }
        )
        interval_hours = [
            (right - left).total_seconds() / 3600.0
            for left, right in itertools.pairwise(settlement_times)
            if right > left
        ]
        median_interval = statistics.median(interval_hours) if interval_hours else None
        interval_deviations = (
            [abs(value - median_interval) for value in interval_hours]
            if median_interval is not None else []
        )
        max_interval_deviation = max(interval_deviations, default=None)
        interval_stable = bool(
            median_interval is not None
            and median_interval > 0.0
            and len(interval_hours) >= max(1, required - 1)
            and max_interval_deviation is not None
            and max_interval_deviation <= median_interval * 0.25
        )
        basis_values = [
            row["basis_bps"] for row in ordered_periods
            if row["basis_bps"] is not None
        ]
        basis_values.sort()
        basis_p95 = (
            basis_values[min(len(basis_values) - 1, int(0.95 * len(basis_values)))]
            if basis_values else None
        )
        volumes = [
            row["quote_volume"] for row in ordered_periods
            if row["quote_volume"] is not None and row["quote_volume"] > 0.0
        ]
        minimum_turnover = min(volumes) if volumes else None
        # One basis point of observed quote turnover is a conservative capacity
        # proxy only; it is not claimed as executable market capacity.
        capacity_proxy = (
            minimum_turnover / 10_000.0 if minimum_turnover is not None else None
        )
        lower_funding = sorted(values)[max(0, int(0.20 * len(values)) - 1)]
        projected_periods = _projected_funding_periods_24h(median_interval)
        stressed = CarryEngine().start(
            f"history-{market_id}",
            CarryTerms(
                notional_usdt=100.0,
                expected_funding_rate=lower_funding,
                taker_fee_rate=0.001,
                maker_fee_rate=0.0002,
                expected_funding_periods=projected_periods,
                entry_slippage_bps_per_leg=2.0,
                exit_slippage_bps_per_leg=2.0,
                max_basis_adverse_bps=basis_p95 or 0.0,
                adl_stress_bps=2.0,
                capacity_usdt=capacity_proxy,
                liquidation_buffer_pct=25.0,
                minimum_liquidation_buffer_pct=10.0,
            ),
        )
        stress_evidence_complete = bool(
            len(basis_values) >= math.ceil(len(values) * 0.80)
            and len(volumes) >= math.ceil(len(values) * 0.80)
        )
        ready = (
            capture_window_ready
            and len(periods) >= required
            and verified == len(values)
            and positive_fraction >= 0.80
            and sign_flip_rate <= 0.20
            and statistics.mean(values) > 0.0
            and interval_stable
            and stress_evidence_complete
            and stressed.state == CarryState.CAPITAL_RESERVED
        )
        summaries.append(
            {
                "market_id": market_id,
                "samples": len(periods),
                "overview_samples": overview_counts.get(market_id, 0),
                "independent_funding_periods": len(periods),
                "verified_spot_periods": verified,
                "positive_fraction": positive_fraction,
                "sign_flips": sign_flips,
                "sign_flip_rate": sign_flip_rate,
                "mean_funding_rate": statistics.mean(values),
                "median_funding_rate": statistics.median(values),
                "stressed_funding_rate_p20": lower_funding,
                "median_funding_interval_hours": median_interval,
                "maximum_interval_deviation_hours": max_interval_deviation,
                "funding_interval_stable": interval_stable,
                "projected_funding_periods_24h": projected_periods,
                "basis_observations": len(basis_values),
                "basis_adverse_p95_bps": basis_p95,
                "turnover_observations": len(volumes),
                "minimum_quote_turnover_usdt": minimum_turnover,
                "capacity_proxy_usdt_at_1bp_participation": capacity_proxy,
                "capacity_is_observable_proxy_only": True,
                "modeled_liquidation_buffer_pct": 25.0,
                "stress_evidence_complete": stress_evidence_complete,
                "stressed_state": stressed.state.value,
                "stressed_projected_net_pnl": stressed.projected_net_pnl,
                "ready": ready,
            }
        )
        if ready:
            qualified.append(market_id)
    summaries.sort(
        key=lambda row: (
            not row["ready"],
            -row["samples"],
            row["market_id"],
        )
    )
    qualified.sort()
    return {
        "capture_evidence_contract": capture_contract,
        "eligible_sealed_days": (
            None if capture_scope is None else len(capture_scope["eligible"])
        ),
        "invalid_sealed_days": (
            None if capture_scope is None else capture_scope["invalid_days"]
        ),
        "seal_validation_errors": (
            None if capture_scope is None else capture_scope["seal_errors"]
        ),
        "capture_continuity": (
            None if capture_scope is None else capture_scope["continuity"]
        ),
        "skipped_unusable_partitions": skipped_unusable_partitions,
        "excluded_overview_events": excluded_overview_events,
        "overview_events": total_events,
        "minimum_market_samples": required,
        "minimum_independent_funding_periods": required,
        "observations_without_settlement_id": missing_settlement,
        "noncausal_settlement_observations": noncausal_settlement_observations,
        "unsettled_funding_periods": len(unsettled_funding_periods),
        "qualified_markets": qualified,
        "markets": summaries[:100],
    }


def _result(ready: bool, evidence: dict, reason: str) -> dict:
    return {
        "state": "DATA_READY" if ready else "BLOCKED",
        "ready": bool(ready),
        "promotion_eligible": False,
        "reason": reason,
        "evidence": evidence,
        "changes_orders": False,
        "automatic_promotion": False,
    }


def run_research_experiments(
    root: str | Path,
    *,
    bot: str,
    mode: str,
    minimum_expectancy_rows: int = 1_200,
    minimum_entry_rows: int = 200,
    minimum_cost_samples: int = 50,
    minimum_momentum_windows: int = 30,
    as_of_ms: int | None = None,
) -> dict:
    """Run every currently measurable experiment without mutating the project."""
    from trading.expectancy_telemetry import EXPECTANCY_FEATURES
    from trading.profit_research_runner import (
        build_carry_preview,
        build_execution_cost_report,
        build_execution_policy_report,
        build_observation_readiness,
        build_ofi_report,
        select_expectancy_schema,
    )

    project = Path(root)
    normalized_bot = str(bot).strip().upper()
    normalized_mode = str(mode).strip().upper()
    if EXPECTANCY_FEATURES.get(normalized_bot) is None or normalized_mode not in {"LIVE", "SIM"}:
        raise ValueError("unsupported bot or mode")
    selected_schema, features, labels, schema_counts = select_expectancy_schema(
        project,
        bot=normalized_bot,
        mode=normalized_mode,
        minimum_rows=max(30, int(minimum_expectancy_rows)),
    )
    primary_score = "score" if "score" in features else features[0]
    score_thresholds = (
        (1.0, 2.0, 3.0, 4.0)
        if primary_score == "trend_votes"
        else (50.0, 60.0, 70.0, 80.0)
    )
    selectivity = evaluate_score_thresholds(
        labels,
        score_feature=primary_score,
        thresholds=score_thresholds,
        minimum_samples=minimum_entry_rows,
    )
    selectivity_ready = any(row["ready"] for row in selectivity["rows"])

    expectancy_evidence = {
        "closed_labels": len(labels),
        "schema_version": selected_schema,
        "closed_labels_by_schema": {
            str(schema): int(count) for schema, count in schema_counts.items()
        },
        "minimum_rows": max(30, int(minimum_expectancy_rows)),
    }
    observation_readiness = build_observation_readiness(
        project,
        bot=normalized_bot,
        mode=normalized_mode,
    )
    expectancy_evidence["observation_readiness"] = observation_readiness
    abstention = None
    expectancy_ready = False
    if (
        observation_readiness["ready"] is True
        and len(labels) >= max(30, int(minimum_expectancy_rows))
    ):
        try:
            min_train = max(
                100,
                min(1_000, len(labels) - 400),
            )
            fitted = expanding_walk_forward_fit(
                labels,
                feature_order=features,
                min_train=min_train,
                test_size=min(200, max(1, len(labels) - min_train)),
            )
            expectancy_evidence.update(
                {
                    "folds": len(fitted.folds),
                    "oos_predictions": len(fitted.predictions),
                    "calibration": asdict(fitted.calibration),
                }
            )
            abstention = evaluate_abstention_grid(
                fitted.predictions, minimum_samples=minimum_entry_rows
            )
            expectancy_evidence["abstention"] = abstention
            expectancy_ready = bool(fitted.predictions)
        except ValueError as exc:
            expectancy_evidence["error"] = str(exc)

    cost = build_execution_cost_report(
        project,
        bot=normalized_bot,
        mode=normalized_mode,
        minimum_samples=minimum_cost_samples,
    )
    execution_policy = build_execution_policy_report(
        project, minimum_samples=minimum_cost_samples
    )
    ofi = build_ofi_report(project, max_windows=5_000)
    carry_history = _carry_history_report(project)
    carry_periods = {
        str(row["market_id"]): float(row["projected_funding_periods_24h"])
        for row in carry_history.get("markets", [])
        if row.get("projected_funding_periods_24h") is not None
    }
    carry = build_carry_preview(
        project,
        expected_funding_periods_by_market=carry_periods,
    )
    accepted_markets = {
        str(row.get("market_id"))
        for row in carry.get("rows", [])
        if row.get("state") == "CAPITAL_RESERVED"
    }
    carry_ready = bool(
        accepted_markets.intersection(carry_history["qualified_markets"])
    )

    panel_snapshot = load_cached_ohlcv_snapshot(project, as_of_ms=as_of_ms)
    panel = panel_snapshot["panel"]
    panel_source = panel_snapshot["source"]
    panel_source_complete = panel_source["complete"] is True
    panel_source_ref = {
        "snapshot_sha256": panel_source["snapshot_sha256"],
        "complete": panel_source_complete,
        "candidate_files": panel_source["candidate_files"],
        "accepted_files": panel_source["accepted_files"],
        "rejected_files": panel_source["rejected_files"],
        "omitted_due_to_symbol_limit": panel_source[
            "omitted_due_to_symbol_limit"
        ],
    }
    momentum = evaluate_momentum_panel(
        panel, minimum_windows=minimum_momentum_windows
    )
    regime = evaluate_regime_shift(panel)
    from trading.range_grid_research import evaluate_bounded_range_grid

    grid = evaluate_bounded_range_grid(panel)
    momentum = {**momentum, "ohlcv_panel_source": panel_source_ref}
    regime = {**regime, "ohlcv_panel_source": panel_source_ref}
    grid = {**grid, "ohlcv_panel_source": panel_source_ref}
    abstention_ready = bool(
        abstention
        and any(row.get("ready") is True for row in abstention.get("rows", []))
    )
    momentum_evidence_ready = bool(momentum["ready"])
    momentum_ready = momentum_evidence_ready and panel_source_complete
    uncertainty_evidence_ready = bool(regime.get("ready") and abstention_ready)
    uncertainty_ready = uncertainty_evidence_ready and panel_source_complete
    grid_evidence_ready = bool(grid["ready"])
    grid_ready = grid_evidence_ready and panel_source_complete

    results = {
        "net_expectancy": _result(
            expectancy_ready,
            expectancy_evidence,
            "purged walk-forward evidence available"
            if expectancy_ready
            else (
                "observation window is not ready"
                if observation_readiness["ready"] is not True
                else "insufficient closed causal labels"
            ),
        ),
        "entry_selectivity": _result(
            selectivity_ready,
            selectivity,
            "entry threshold grid has adequate samples"
            if selectivity_ready
            else "insufficient labels per threshold; exit hysteresis not yet claimed",
        ),
        "execution_cost": _result(
            bool(cost["ready"]),
            cost,
            "empirical execution costs are sample-ready"
            if cost["ready"]
            else "insufficient paired arrival/fill observations",
        ),
        "execution_policy": _result(
            bool(execution_policy["ready"]),
            execution_policy,
            "sequence-valid execution policy evidence is sample-ready"
            if execution_policy["ready"]
            else "sequence-valid maker cross-through evidence is unavailable",
        ),
        "order_flow": _result(
            ofi.get("promotable_windows", 0) >= 1_000,
            ofi,
            "continuous L2 OFI sample is ready"
            if ofi.get("promotable_windows", 0) >= 1_000
            else "continuous sequence-valid L2 windows are unavailable",
        ),
        "cross_sectional_momentum": _result(
            momentum_ready,
            momentum,
            "causal momentum panel is sample-ready"
            if momentum_ready
            else (
                "OHLCV cache snapshot integrity is incomplete"
                if momentum_evidence_ready
                else (
                    "exploratory metrics computed, but point-in-time universe is incomplete"
                    if momentum["data_sufficient"]
                    else "insufficient symbols or rebalance windows"
                )
            ),
        ),
        "time_series_momentum_crash": _result(
            momentum_ready,
            {
                "time_series": momentum["time_series"],
                "crash_guard": momentum["crash_guard"],
                "causality": momentum["causality"],
                "minimum_windows": momentum["minimum_windows"],
                "ohlcv_panel_source": panel_source_ref,
            },
            "past-only TSMOM and crash overlay are sample-ready"
            if momentum_ready
            else (
                "OHLCV cache snapshot integrity is incomplete"
                if momentum_evidence_ready
                else (
                    "exploratory metrics computed, but point-in-time universe is incomplete"
                    if momentum["data_sufficient"]
                    else "insufficient causal momentum windows"
                )
            ),
        ),
        "funding_carry": _result(
            carry_ready,
            {"current_preview": carry, "history": carry_history},
            "verified carry history is sample-ready"
            if carry_ready
            else "insufficient verified spot/perpetual funding history",
        ),
        "regime_uncertainty": _result(
            uncertainty_ready,
            {"regime": regime, "abstention": abstention},
            "regime and calibrated abstention evidence are available"
            if uncertainty_ready
            else (
                "OHLCV cache snapshot integrity is incomplete"
                if uncertainty_evidence_ready
                else "regime history or calibrated OOS predictions are unavailable"
            ),
        ),
        "bounded_regime_grid": _result(
            grid_ready,
            grid,
            "bounded causal grid simulation is sample-ready"
            if grid_ready
            else (
                "OHLCV cache snapshot integrity is incomplete"
                if grid_evidence_ready
                else (
                    "invalid numeric grid evidence was excluded; "
                    "insufficient valid observations remain"
                    if grid.get("invalid_numeric_symbols")
                    or grid.get("portfolio_numeric_valid") is False
                    else "insufficient contiguous OHLC observations for grid research"
                )
            ),
        ),
    }
    ready_count = sum(result["ready"] for result in results.values())
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root": str(project.resolve()),
        "bot": normalized_bot,
        "mode": normalized_mode,
        "ohlcv_panel_source": panel_source,
        "catalog": [asdict(item) for item in EXPERIMENT_CATALOG],
        "results": results,
        "summary": {"ready": ready_count, "blocked": len(results) - ready_count},
        "safety": {
            "changes_orders": False,
            "writes_live_model": False,
            "automatic_promotion": False,
        },
    }


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: str | os.PathLike, *, label: str) -> Path:
    requested = Path(path).expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} must be a real path without links")
    return requested


def _require_json_native(value, *, seen: set[int] | None = None) -> None:
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("research report must contain finite JSON values")
        return
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("research report must contain JSON-native string keys")
        seen = set() if seen is None else seen
        identity = id(value)
        if identity in seen:
            raise ValueError("research report must not contain cycles")
        seen.add(identity)
        try:
            for item in value.values():
                _require_json_native(item, seen=seen)
        finally:
            seen.remove(identity)
        return
    if isinstance(value, list):
        seen = set() if seen is None else seen
        identity = id(value)
        if identity in seen:
            raise ValueError("research report must not contain cycles")
        seen.add(identity)
        try:
            for item in value:
                _require_json_native(item, seen=seen)
        finally:
            seen.remove(identity)
        return
    raise TypeError(
        f"research report must contain JSON-native values, got {type(value).__name__}"
    )


def _encode_research_report(payload: dict) -> bytes:
    if not isinstance(payload, dict):
        raise TypeError("research report must be a JSON-native object")
    _require_json_native(payload)
    encoded = json.dumps(
        payload, sort_keys=True, indent=2, allow_nan=False
    ).encode("utf-8")
    if len(encoded) > RESEARCH_REPORT_MAX_BYTES:
        raise ValueError("research report exceeds the 64 MiB size limit")
    return encoded


def _sync_directory(path: Path) -> None:
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(str(path), flags)
    except AttributeError:
        return
    except OSError as exc:
        if os.name == "nt":
            if isinstance(exc, PermissionError):
                return
            if (
                isinstance(exc, FileNotFoundError)
                and path == Path(path.anchor)
                and path.is_dir()
            ):
                return
        raise
    primary_error: BaseException | None = None
    try:
        os.fsync(directory_fd)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            os.close(directory_fd)
        except BaseException as close_error:
            if primary_error is None:
                raise
            try:
                primary_error.add_note(
                    "research directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _mkdir_with_parent_fsync(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    parent = path.parent
    while True:
        _sync_directory(parent)
        if parent == parent.parent:
            break
        parent = parent.parent


def _publish_immutable_research_report(path: Path, encoded: bytes) -> Path:
    if not isinstance(encoded, bytes) or len(encoded) > RESEARCH_REPORT_MAX_BYTES:
        raise ValueError("research report exceeds the 64 MiB size limit")
    path = _absolute_without_links(path, label="research report path")
    if not path.parent.is_dir():
        raise ValueError("research report parent must be a real directory")
    def existing_matches() -> bool:
        if _is_linklike(path) or not path.is_file():
            return False
        try:
            if path.stat().st_size != len(encoded):
                return False
            with path.open("rb") as existing:
                return existing.read(len(encoded) + 1) == encoded
        except OSError:
            return False

    if path.exists() or _is_linklike(path):
        if existing_matches():
            _sync_directory(path.parent)
            return path
        raise FileExistsError("immutable research report conflict")
    temporary: Path | None = None
    temporary_owned = False
    primary_error: BaseException | None = None
    handle = None
    try:
        for attempt in range(3):
            candidate = path.with_name(
                f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            )
            _absolute_without_links(candidate, label="research report path")
            try:
                handle = candidate.open("xb")
            except FileExistsError:
                if attempt == 2:
                    raise
                continue
            temporary = candidate
            temporary_owned = True
            break
        if handle is None or temporary is None:
            raise RuntimeError("research report temporary allocation failed")
        try:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if primary_error is None:
                    primary_error = close_error
                    raise
                try:
                    primary_error.add_note(
                        "research report temporary close failed: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        path = _absolute_without_links(path, label="research report path")
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            if not existing_matches():
                raise FileExistsError(
                    "immutable research report conflict"
                ) from exc
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        else:
            temporary_owned = False
        _sync_directory(path.parent)
    except BaseException as exc:
        if primary_error is None:
            primary_error = exc
        raise
    finally:
        if temporary_owned and temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            except BaseException as cleanup_error:
                if primary_error is None:
                    raise
                try:
                    primary_error.add_note(
                        "research report temporary cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
    return path


def write_immutable_research_report(
    root: str | Path, payload: dict, *, report_kind: str
) -> Path:
    encoded = _encode_research_report(payload)
    kinds = {
        "status": ("reports", "profit_research_status", False),
        "experiment": ("experiments", "profit_experiments", True),
    }
    try:
        subdirectory, prefix, include_digest = kinds[report_kind]
    except KeyError as exc:
        raise ValueError("unsupported research report kind") from exc
    project = _absolute_without_links(root, label="research report root")
    if not project.is_dir():
        raise ValueError("research report root must be a real directory")
    folder = _absolute_without_links(
        project / "data" / "research" / subdirectory,
        label="research report directory",
    )
    _mkdir_with_parent_fsync(folder)
    folder = _absolute_without_links(folder, label="research report directory")
    if not folder.is_dir():
        raise ValueError("research report directory must be a real directory")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    unique = uuid.uuid4().hex[:8]
    digest_part = (
        f"_{hashlib.sha256(encoded).hexdigest()[:12]}" if include_digest else ""
    )
    path = folder / f"{prefix}_{stamp}{digest_part}_{unique}.json"
    return _publish_immutable_research_report(path, encoded)


def _ohlcv_archive_contract(source: dict) -> dict:
    artifacts = _validated_ohlcv_source_contract(source)
    total_bytes = sum(artifact["bytes"] for artifact in artifacts)
    if total_bytes > RESEARCH_OHLCV_ARCHIVE_MAX_BYTES:
        raise ValueError("OHLCV archive exceeds the total byte limit")
    archive = {
        "schema_version": 1,
        "snapshot_sha256": source["snapshot_sha256"],
        "accepted_files": source["accepted_files"],
        "total_bytes": total_bytes,
        "blobs": [
            {
                "source_path": artifact["path"],
                "sha256": artifact["sha256"],
                "bytes": artifact["bytes"],
                "blob_path": (
                    "data/research/ohlcv_blobs/"
                    f"{artifact['sha256']}.json"
                ),
            }
            for artifact in artifacts
        ],
    }
    encoded = json.dumps(
        archive,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    archive["archive_sha256"] = hashlib.sha256(encoded).hexdigest()
    return archive


def _validated_ohlcv_archive_contract(source: dict, archive: dict) -> dict:
    if not isinstance(archive, dict):
        raise TypeError("OHLCV source archive contract is invalid")
    expected = _ohlcv_archive_contract(source)
    if archive != expected:
        raise ValueError("OHLCV source archive contract is inconsistent")
    return expected


def _read_ohlcv_archive_blob(path: Path, *, byte_count: int, digest: str) -> bytes:
    path = _absolute_without_links(path, label="OHLCV archive blob")
    if not path.is_file():
        raise ValueError("OHLCV archive blob is unavailable")
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            raw = handle.read(byte_count + 1)
            after = os.fstat(handle.fileno())
        path = _absolute_without_links(path, label="OHLCV archive blob")
        current = path.stat()
    except OSError as exc:
        raise ValueError("OHLCV archive blob is unavailable") from exc
    if (
        len(raw) != byte_count
        or before.st_size != byte_count
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or current.st_size != after.st_size
        or current.st_mtime_ns != after.st_mtime_ns
        or (before.st_ino and current.st_ino and before.st_ino != current.st_ino)
        or (before.st_dev and current.st_dev and before.st_dev != current.st_dev)
    ):
        raise ValueError("OHLCV archive blob size or read stability mismatch")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("OHLCV archive blob digest mismatch")
    return raw


def _archive_ohlcv_panel_source(root: str | Path, source: dict) -> dict:
    artifacts = _validated_ohlcv_source_contract(source)
    archive = _ohlcv_archive_contract(source)
    if not artifacts:
        return archive
    project = _absolute_without_links(root, label="OHLCV archive root")
    cache_folder = _absolute_without_links(
        project / "data" / "ohlcv_cache", label="OHLCV cache directory"
    )
    blob_folder = _absolute_without_links(
        project / "data" / "research" / "ohlcv_blobs",
        label="OHLCV archive directory",
    )
    blob_folder.mkdir(parents=True, exist_ok=True)
    blob_folder = _absolute_without_links(
        blob_folder, label="OHLCV archive directory"
    )
    if not blob_folder.is_dir():
        raise ValueError("OHLCV archive directory is unavailable")
    for artifact, blob in zip(artifacts, archive["blobs"], strict=True):
        source_path = _absolute_without_links(
            cache_folder.joinpath(*artifact["path"].split("/")),
            label="OHLCV source artifact",
        )
        if not source_path.is_file():
            raise ValueError("OHLCV source artifact is unavailable")
        try:
            with source_path.open("rb") as handle:
                raw = handle.read(artifact["bytes"] + 1)
        except OSError as exc:
            raise ValueError("OHLCV source artifact is unavailable") from exc
        _verify_ohlcv_artifact_bytes(raw, artifact, source)
        blob_path = _absolute_without_links(
            project.joinpath(*blob["blob_path"].split("/")),
            label="OHLCV archive blob",
        )
        try:
            _publish_immutable_research_report(blob_path, raw)
        except FileExistsError:
            pass
        archived = _read_ohlcv_archive_blob(
            blob_path,
            byte_count=blob["bytes"],
            digest=blob["sha256"],
        )
        _verify_ohlcv_artifact_bytes(archived, artifact, source)
    return archive


def verify_ohlcv_panel_archive(
    root: str | Path, source: dict, archive: dict
) -> dict:
    project = _absolute_without_links(root, label="OHLCV archive root")
    expected = _validated_ohlcv_archive_contract(source, archive)
    artifacts = _validated_ohlcv_source_contract(source)
    for artifact, blob in zip(artifacts, expected["blobs"], strict=True):
        blob_path = _absolute_without_links(
            project.joinpath(*blob["blob_path"].split("/")),
            label="OHLCV archive blob",
        )
        raw = _read_ohlcv_archive_blob(
            blob_path,
            byte_count=blob["bytes"],
            digest=blob["sha256"],
        )
        _verify_ohlcv_artifact_bytes(raw, artifact, source)
    complete = source["complete"] is True
    result = {
        "state": (
            "VERIFIED_ARCHIVED_COMPLETE"
            if complete
            else "VERIFIED_ARCHIVED_ACCEPTED_ONLY"
        ),
        "snapshot_sha256": source["snapshot_sha256"],
        "archive_sha256": expected["archive_sha256"],
        "accepted_files": source["accepted_files"],
        "total_bytes": expected["total_bytes"],
        "accepted_source_bytes_reproducible": True,
        "complete_source_reproducible": complete,
    }
    if source["schema_version"] == 2:
        result.update(
            {
                "source_schema_version": 2,
                "as_of_ms": source["as_of_ms"],
            }
        )
    return result


@contextmanager
def _locked_ohlcv_archive_lifecycle(root: str | Path):
    project = _absolute_without_links(root, label="OHLCV archive root")
    research_folder = _absolute_without_links(
        project / "data" / "research", label="OHLCV archive lifecycle directory"
    )
    research_folder.mkdir(parents=True, exist_ok=True)
    research_folder = _absolute_without_links(
        research_folder, label="OHLCV archive lifecycle directory"
    )
    if not research_folder.is_dir():
        raise ValueError("OHLCV archive lifecycle directory is unavailable")
    lock_path = _absolute_without_links(
        research_folder / ".ohlcv_archive_lifecycle.lock",
        label="OHLCV archive lifecycle lock",
    )
    with portalocker.Lock(
        str(lock_path),
        mode="a",
        timeout=30,
        check_interval=0.05,
        fail_when_locked=False,
    ):
        yield


def write_immutable_experiment_report(root: str | Path, payload: dict) -> Path:
    """Write a uniquely named research artifact; never overwrite an old report."""
    encoded = _encode_research_report(payload)
    payload_snapshot = json.loads(encoded.decode("utf-8"))
    is_suite_report = "results" in payload_snapshot or "catalog" in payload_snapshot
    if not is_suite_report:
        return write_immutable_research_report(
            root, payload_snapshot, report_kind="experiment"
        )
    if (
        "ohlcv_panel_source_verification" in payload_snapshot
        or "ohlcv_panel_source_archive" in payload_snapshot
    ):
        raise ValueError("OHLCV source verification fields are reserved")
    source = payload_snapshot.get("ohlcv_panel_source")
    if not isinstance(source, dict):
        raise TypeError("experiment report requires OHLCV panel source")
    _ohlcv_archive_contract(source)
    with (
        _locked_ohlcv_panel_sources(root, source),
        _locked_ohlcv_archive_lifecycle(root),
    ):
        verification = verify_ohlcv_panel_source(root, source)
        archive = _archive_ohlcv_panel_source(root, source)
        bound_payload = {
            **payload_snapshot,
            "ohlcv_panel_source_verification": verification,
            "ohlcv_panel_source_archive": archive,
        }
        return write_immutable_research_report(
            root, bound_payload, report_kind="experiment"
        )


_EXPERIMENT_REPORT_NAME_RE = re.compile(
    r"^profit_experiments_\d{8}T\d{12}Z_([0-9a-f]{12})_[0-9a-f]{8}\.json$"
)


def _reject_research_json_constant(value: str):
    raise ValueError(f"non-finite JSON constant is not allowed: {value}")


def _strict_research_json_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate research report key is not allowed")
        result[key] = value
    return result


def _load_immutable_experiment_report(
    root: str | Path, report: str | Path
) -> tuple[Path, Path, str, dict, bytes]:
    project = _absolute_without_links(root, label="research report root")
    if not project.is_dir():
        raise ValueError("research report root must be a real directory")
    folder = _absolute_without_links(
        project / "data" / "research" / "experiments",
        label="experiment report directory",
    )
    if not folder.is_dir():
        raise ValueError("experiment report directory is unavailable")
    path = _absolute_without_links(report, label="experiment report")
    if path.parent != folder:
        raise ValueError("experiment report is outside the experiment report directory")
    match = _EXPERIMENT_REPORT_NAME_RE.fullmatch(path.name)
    if match is None or not path.is_file():
        raise ValueError("experiment report identity is invalid")
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            raw = handle.read(RESEARCH_REPORT_MAX_BYTES + 1)
            after = os.fstat(handle.fileno())
        path = _absolute_without_links(path, label="experiment report")
        current = path.stat()
    except OSError as exc:
        raise ValueError("experiment report is unavailable") from exc
    if len(raw) > RESEARCH_REPORT_MAX_BYTES:
        raise ValueError(
            "experiment report exceeds the "
            f"{RESEARCH_REPORT_MAX_BYTES} byte size limit"
        )
    if (
        before.st_size != len(raw)
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or current.st_size != after.st_size
        or current.st_mtime_ns != after.st_mtime_ns
        or (before.st_ino and current.st_ino and before.st_ino != current.st_ino)
        or (before.st_dev and current.st_dev and before.st_dev != current.st_dev)
    ):
        raise ValueError("experiment report changed during verification")
    report_sha256 = hashlib.sha256(raw).hexdigest()
    if report_sha256[:12] != match.group(1):
        raise ValueError("experiment report digest does not match its filename")
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            parse_constant=_reject_research_json_constant,
            object_pairs_hook=_strict_research_json_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("experiment report JSON is invalid") from exc
    if not isinstance(payload, dict):
        raise TypeError("experiment report must be a JSON object")
    _require_json_native(payload)
    return project, path, report_sha256, payload, raw


def verify_immutable_experiment_report(
    root: str | Path, report: str | Path
) -> dict:
    """Verify immutable report bytes and their current OHLCV source read-only."""
    project, path, report_sha256, payload, _raw = _load_immutable_experiment_report(
        root, report
    )
    source = payload.get("ohlcv_panel_source")
    historical = payload.get("ohlcv_panel_source_verification")
    if not isinstance(source, dict) or not isinstance(historical, dict):
        raise TypeError("experiment report source evidence is missing")
    _validated_ohlcv_source_contract(source)
    if historical != _ohlcv_source_verification_result(source):
        raise ValueError("experiment report source verification is inconsistent")
    archive = payload.get("ohlcv_panel_source_archive")
    if archive is None:
        with _locked_ohlcv_panel_sources(project, source):
            current_verification = verify_ohlcv_panel_source(project, source)
        return {
            "ok": True,
            "report": str(path),
            "report_sha256": report_sha256,
            "published_source_verification": historical,
            "source_verification": current_verification,
            "changes_orders": False,
            "automatic_promotion": False,
        }
    archive_verification = verify_ohlcv_panel_archive(project, source, archive)
    try:
        with _locked_ohlcv_panel_sources(project, source):
            live_verification = verify_ohlcv_panel_source(project, source)
    except (OSError, ValueError, portalocker.exceptions.LockException) as exc:
        live_verification = {
            "state": "DRIFTED" if isinstance(exc, ValueError) else "UNAVAILABLE",
            "error_type": type(exc).__name__,
            "reason": str(exc)[:512],
        }
    source_verification = (
        archive_verification
        if live_verification["state"] in {"DRIFTED", "UNAVAILABLE"}
        else live_verification
    )
    return {
        "ok": True,
        "report": str(path),
        "report_sha256": report_sha256,
        "published_source_verification": historical,
        "source_verification": source_verification,
        "archive_verification": archive_verification,
        "live_source_verification": live_verification,
        "changes_orders": False,
        "automatic_promotion": False,
    }


def _validated_archived_experiment_payload(payload: dict) -> tuple[dict, dict, list]:
    source = payload.get("ohlcv_panel_source")
    historical = payload.get("ohlcv_panel_source_verification")
    archive = payload.get("ohlcv_panel_source_archive")
    if not isinstance(source, dict) or not isinstance(historical, dict):
        raise TypeError("experiment report source evidence is missing")
    artifacts = _validated_ohlcv_source_contract(source)
    if historical != _ohlcv_source_verification_result(source):
        raise ValueError("experiment report source verification is inconsistent")
    if archive is None:
        raise ValueError("experiment report has no portable OHLCV archive")
    expected_archive = _validated_ohlcv_archive_contract(source, archive)
    return source, expected_archive, artifacts


def _experiment_bundle_manifest(
    *, report_name: str, report_sha256: str, report_bytes: int,
    source: dict, archive: dict,
) -> dict:
    blobs_by_digest = {}
    for blob in archive["blobs"]:
        record = {
            "path": f"blobs/{blob['sha256']}.json",
            "sha256": blob["sha256"],
            "bytes": blob["bytes"],
        }
        previous = blobs_by_digest.setdefault(blob["sha256"], record)
        if previous != record:
            raise ValueError("OHLCV archive blob identity conflicts")
    core = {
        "schema_version": RESEARCH_EXPERIMENT_BUNDLE_SCHEMA,
        "artifact_kind": "ohlcv_experiment_bundle",
        "report": {
            "path": f"report/{report_name}",
            "sha256": report_sha256,
            "bytes": report_bytes,
        },
        "source_snapshot_sha256": source["snapshot_sha256"],
        "archive_sha256": archive["archive_sha256"],
        "blobs": [blobs_by_digest[key] for key in sorted(blobs_by_digest)],
    }
    sealed = json.dumps(
        core, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return {**core, "manifest_sha256": hashlib.sha256(sealed).hexdigest()}


def _encode_experiment_bundle_manifest(manifest: dict) -> bytes:
    encoded = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    if len(encoded) > RESEARCH_REPORT_MAX_BYTES:
        raise ValueError("experiment bundle manifest exceeds the size limit")
    return encoded


def _experiment_bundle_zip_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
    info.create_system = 3
    info.external_attr = 0o100600 << 16
    info.compress_type = zipfile.ZIP_STORED
    return info


def _stable_file_sha256(
    path: str | Path, *, maximum_bytes: int, label: str
) -> tuple[str, int]:
    candidate = _absolute_without_links(path, label=label)
    try:
        with candidate.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if before.st_size > maximum_bytes:
                raise ValueError(f"{label} exceeds the total byte limit")
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > maximum_bytes:
                    raise ValueError(f"{label} exceeds the total byte limit")
                digest.update(chunk)
            after = os.fstat(handle.fileno())
        candidate = _absolute_without_links(candidate, label=label)
        current = candidate.stat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable") from exc
    if (
        not candidate.is_file()
        or total != before.st_size
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or current.st_size != after.st_size
        or current.st_mtime_ns != after.st_mtime_ns
        or (before.st_ino and current.st_ino and before.st_ino != current.st_ino)
        or (before.st_dev and current.st_dev and before.st_dev != current.st_dev)
    ):
        raise ValueError(f"{label} changed during verification")
    return digest.hexdigest(), total


def _publish_experiment_bundle(path: Path, temporary: Path) -> bool:
    expected_digest, expected_bytes = _stable_file_sha256(
        temporary,
        maximum_bytes=RESEARCH_EXPERIMENT_BUNDLE_MAX_BYTES,
        label="temporary experiment bundle",
    )
    path = _absolute_without_links(path, label="experiment bundle destination")
    published = False
    if path.exists() or _is_linklike(path):
        actual_digest, actual_bytes = _stable_file_sha256(
            path,
            maximum_bytes=RESEARCH_EXPERIMENT_BUNDLE_MAX_BYTES,
            label="experiment bundle destination",
        )
        if (actual_digest, actual_bytes) != (expected_digest, expected_bytes):
            raise ValueError(
                "experiment bundle destination conflicts with existing evidence"
            )
    else:
        try:
            os.link(temporary, path)
        except FileExistsError:
            actual_digest, actual_bytes = _stable_file_sha256(
                path,
                maximum_bytes=RESEARCH_EXPERIMENT_BUNDLE_MAX_BYTES,
                label="experiment bundle destination",
            )
            if (actual_digest, actual_bytes) != (expected_digest, expected_bytes):
                raise ValueError(
                    "experiment bundle destination conflicts with concurrently written evidence"
                )
        else:
            published = True
    _sync_directory(path.parent)
    return published


def export_immutable_experiment_bundle(
    root: str | Path, report: str | Path, destination: str | Path
) -> dict:
    """Create one deterministic, immutable report-plus-OHLCV transport bundle."""
    output = _absolute_without_links(
        destination, label="experiment bundle destination"
    )
    project, report_path, report_sha256, payload, report_raw = (
        _load_immutable_experiment_report(root, report)
    )
    protected_folders = {
        _absolute_without_links(
            project / "data" / "research" / name,
            label="protected research directory",
        )
        for name in ("experiments", "ohlcv_blobs")
    }
    if any(folder == output or folder in output.parents for folder in protected_folders):
        raise ValueError("experiment bundle destination is inside protected evidence")
    source, archive, artifacts = _validated_archived_experiment_payload(payload)
    manifest = _experiment_bundle_manifest(
        report_name=report_path.name,
        report_sha256=report_sha256,
        report_bytes=len(report_raw),
        source=source,
        archive=archive,
    )
    manifest_raw = _encode_experiment_bundle_manifest(manifest)
    references_by_digest = {}
    for artifact, blob in zip(artifacts, archive["blobs"], strict=True):
        references_by_digest.setdefault(blob["sha256"], []).append((artifact, blob))
    _mkdir_with_parent_fsync(output.parent)
    output = _absolute_without_links(
        output, label="experiment bundle destination"
    )
    if not output.parent.is_dir():
        raise ValueError("experiment bundle parent must be a real directory")
    temporary: Path | None = None
    published = False
    temporary_owned = False
    primary_error: BaseException | None = None
    raw_handle = None
    try:
        for attempt in range(3):
            candidate = output.with_name(
                f".{output.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            )
            _absolute_without_links(candidate, label="temporary experiment bundle")
            try:
                raw_handle = candidate.open("xb")
            except FileExistsError:
                if attempt == 2:
                    raise
                continue
            temporary = candidate
            temporary_owned = True
            break
        if raw_handle is None or temporary is None:
            raise RuntimeError("experiment bundle temporary allocation failed")
        try:
            with zipfile.ZipFile(
                raw_handle, "w", compression=zipfile.ZIP_STORED, allowZip64=True
            ) as bundle:
                bundle.writestr(
                    _experiment_bundle_zip_info("manifest.json"), manifest_raw
                )
                bundle.writestr(
                    _experiment_bundle_zip_info(f"report/{report_path.name}"),
                    report_raw,
                )
                for digest in sorted(references_by_digest):
                    references = references_by_digest[digest]
                    first_blob = references[0][1]
                    blob_path = _absolute_without_links(
                        project / first_blob["blob_path"],
                        label="OHLCV archive blob",
                    )
                    blob_raw = _read_ohlcv_archive_blob(
                        blob_path,
                        byte_count=first_blob["bytes"],
                        digest=digest,
                    )
                    for artifact, blob in references:
                        if (
                            blob["sha256"] != first_blob["sha256"]
                            or blob["bytes"] != first_blob["bytes"]
                            or blob["blob_path"] != first_blob["blob_path"]
                        ):
                            raise ValueError("OHLCV archive blob identity conflicts")
                        _verify_ohlcv_artifact_bytes(blob_raw, artifact, source)
                    bundle.writestr(
                        _experiment_bundle_zip_info(f"blobs/{digest}.json"),
                        blob_raw,
                    )
            raw_handle.flush()
            os.fsync(raw_handle.fileno())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                raw_handle.close()
            except BaseException as close_error:
                if primary_error is None:
                    primary_error = close_error
                    raise
                try:
                    primary_error.add_note(
                        "experiment bundle temporary close failed: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        published = _publish_experiment_bundle(output, temporary)
        temporary.unlink()
        temporary_owned = False
        _sync_directory(output.parent)
    except BaseException as exc:
        if primary_error is None:
            primary_error = exc
        raise
    finally:
        if temporary_owned and temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            except BaseException as cleanup_error:
                if primary_error is None:
                    raise
                try:
                    primary_error.add_note(
                        "experiment bundle temporary cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
    verified = verify_immutable_experiment_bundle(output)
    return {**verified, "changes_files": published}


def _decode_bundle_json(raw: bytes, *, label: str) -> dict:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            parse_constant=_reject_research_json_constant,
            object_pairs_hook=_strict_research_json_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} JSON is invalid") from exc
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be a JSON object")
    _require_json_native(value)
    return value


def _decode_bundled_experiment_report(name: str, raw: bytes) -> tuple[str, dict]:
    if not isinstance(name, str):
        raise TypeError("bundled experiment report path must be a string")
    basename = name.removeprefix("report/")
    match = _EXPERIMENT_REPORT_NAME_RE.fullmatch(basename)
    if name != f"report/{basename}" or match is None:
        raise ValueError("bundled experiment report identity is invalid")
    if len(raw) > RESEARCH_REPORT_MAX_BYTES:
        raise ValueError("bundled experiment report exceeds the size limit")
    digest = hashlib.sha256(raw).hexdigest()
    if digest[:12] != match.group(1):
        raise ValueError("bundled experiment report digest does not match its filename")
    return digest, _decode_bundle_json(raw, label="bundled experiment report")


def _validated_bundle_manifest(manifest: dict, *, report_raw: bytes) -> tuple:
    if set(manifest) != {
        "schema_version",
        "artifact_kind",
        "report",
        "source_snapshot_sha256",
        "archive_sha256",
        "blobs",
        "manifest_sha256",
    }:
        raise ValueError("experiment bundle manifest structure is invalid")
    if (
        type(manifest["schema_version"]) is not int
        or manifest["schema_version"] != RESEARCH_EXPERIMENT_BUNDLE_SCHEMA
        or manifest["artifact_kind"] != "ohlcv_experiment_bundle"
        or not isinstance(manifest["report"], dict)
        or not isinstance(manifest["blobs"], list)
        or len(manifest["blobs"]) > RESEARCH_ARCHIVE_MAX_BLOBS
    ):
        raise ValueError("experiment bundle manifest contract is invalid")
    report = manifest["report"]
    if set(report) != {"path", "sha256", "bytes"}:
        raise ValueError("experiment bundle report manifest is invalid")
    if not isinstance(report.get("path"), str):
        raise TypeError("experiment bundle report path must be a string")
    report_sha256, payload = _decode_bundled_experiment_report(
        report.get("path", ""), report_raw
    )
    if (
        report.get("sha256") != report_sha256
        or type(report.get("bytes")) is not int
        or report["bytes"] != len(report_raw)
    ):
        raise ValueError("experiment bundle report evidence is inconsistent")
    source, archive, artifacts = _validated_archived_experiment_payload(payload)
    expected = _experiment_bundle_manifest(
        report_name=report["path"].removeprefix("report/"),
        report_sha256=report_sha256,
        report_bytes=len(report_raw),
        source=source,
        archive=archive,
    )
    if manifest != expected:
        raise ValueError("experiment bundle manifest does not match its report")
    return report_sha256, source, archive, artifacts


class _ExperimentBundleConsumerError(Exception):
    def __init__(self, error: Exception):
        super().__init__(str(error))
        self.error = error


def _verify_immutable_experiment_bundle(
    bundle: str | Path, *, verified_consumer=None
) -> tuple[dict, object]:
    path = _absolute_without_links(bundle, label="experiment bundle")
    consumer_result = None
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if before.st_size > RESEARCH_EXPERIMENT_BUNDLE_MAX_BYTES:
                raise ValueError("experiment bundle exceeds the total byte limit")
            digest = hashlib.sha256()
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            handle.seek(0)
            try:
                with zipfile.ZipFile(handle, "r") as archive_file:
                    infos = archive_file.infolist()
                    if len(infos) > RESEARCH_ARCHIVE_MAX_BLOBS + 2:
                        raise ValueError("experiment bundle exceeds the entry limit")
                    names = [info.filename for info in infos]
                    if len(names) != len(set(names)):
                        raise ValueError("experiment bundle contains duplicate entries")
                    for info in infos:
                        if (
                            info.is_dir()
                            or info.compress_type != zipfile.ZIP_STORED
                            or info.compress_size != info.file_size
                            or info.date_time != (1980, 1, 1, 0, 0, 0)
                            or info.create_system != 3
                            or info.external_attr >> 16 != 0o100600
                        ):
                            raise ValueError("experiment bundle entry metadata is invalid")
                    if "manifest.json" not in names:
                        raise ValueError("experiment bundle manifest is missing")
                    manifest_info = archive_file.getinfo("manifest.json")
                    if manifest_info.file_size > RESEARCH_REPORT_MAX_BYTES:
                        raise ValueError("experiment bundle manifest exceeds the size limit")
                    manifest = _decode_bundle_json(
                        archive_file.read(manifest_info), label="experiment bundle manifest"
                    )
                    report_value = manifest.get("report")
                    report_name = (
                        report_value.get("path")
                        if isinstance(report_value, dict)
                        else None
                    )
                    if not isinstance(report_name, str) or report_name not in names:
                        raise ValueError("bundled experiment report is missing")
                    report_info = archive_file.getinfo(report_name)
                    if report_info.file_size > RESEARCH_REPORT_MAX_BYTES:
                        raise ValueError("bundled experiment report exceeds the size limit")
                    report_raw = archive_file.read(report_info)
                    report_sha256, source, source_archive, artifacts = (
                        _validated_bundle_manifest(manifest, report_raw=report_raw)
                    )
                    expected_names = {"manifest.json", report_name}
                    references_by_digest = {}
                    for artifact, blob in zip(
                        artifacts, source_archive["blobs"], strict=True
                    ):
                        references_by_digest.setdefault(blob["sha256"], []).append(
                            artifact
                        )
                    for blob in manifest["blobs"]:
                        blob_name = blob["path"]
                        expected_names.add(blob_name)
                        if blob_name not in names:
                            raise ValueError("experiment bundle archive blob is missing")
                        info = archive_file.getinfo(blob_name)
                        if (
                            type(blob.get("bytes")) is not int
                            or blob["bytes"] <= 0
                            or blob["bytes"] > RESEARCH_ARCHIVE_BLOB_MAX_BYTES
                            or info.file_size != blob["bytes"]
                        ):
                            raise ValueError("experiment bundle archive blob size is invalid")
                        raw = archive_file.read(info)
                        if hashlib.sha256(raw).hexdigest() != blob.get("sha256"):
                            raise ValueError("experiment bundle archive blob digest mismatch")
                        references = references_by_digest.get(blob["sha256"])
                        if not references:
                            raise ValueError("experiment bundle archive blob is unreferenced")
                        for artifact in references:
                            _verify_ohlcv_artifact_bytes(raw, artifact, source)
                        del raw
                    if set(names) != expected_names:
                        raise ValueError("experiment bundle contains unbound entries")
                    if set(references_by_digest) != {
                        blob["sha256"] for blob in manifest["blobs"]
                    }:
                        raise ValueError("experiment bundle archive blob is missing")
                    if verified_consumer is not None:
                        try:
                            consumer_result = verified_consumer(
                                archive_file,
                                manifest,
                                report_raw,
                                report_sha256,
                            source,
                            source_archive,
                            digest.hexdigest(),
                        )
                        except Exception as exc:
                            raise _ExperimentBundleConsumerError(exc) from exc
            except (KeyError, RuntimeError, zipfile.BadZipFile) as exc:
                raise ValueError("experiment bundle ZIP is invalid") from exc
            after = os.fstat(handle.fileno())
        path = _absolute_without_links(path, label="experiment bundle")
        current = path.stat()
    except _ExperimentBundleConsumerError as exc:
        raise exc.error
    except OSError as exc:
        raise ValueError("experiment bundle is unavailable") from exc
    if (
        not path.is_file()
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or current.st_size != after.st_size
        or current.st_mtime_ns != after.st_mtime_ns
        or (before.st_ino and current.st_ino and before.st_ino != current.st_ino)
        or (before.st_dev and current.st_dev and before.st_dev != current.st_dev)
    ):
        raise ValueError("experiment bundle changed during verification")
    return {
        "ok": True,
        "state": "VERIFIED_PORTABLE_ARCHIVE",
        "bundle": str(path),
        "bundle_sha256": digest.hexdigest(),
        "bundle_bytes": after.st_size,
        "report_sha256": report_sha256,
        "source_snapshot_sha256": source["snapshot_sha256"],
        "archive_sha256": source_archive["archive_sha256"],
        "blob_files": len(manifest["blobs"]),
        "blob_bytes": sum(item["bytes"] for item in manifest["blobs"]),
        "changes_files": False,
        "changes_orders": False,
        "automatic_promotion": False,
    }, consumer_result


def verify_immutable_experiment_bundle(bundle: str | Path) -> dict:
    """Verify a closed portable experiment bundle without extracting any file."""
    verified, _consumer_result = _verify_immutable_experiment_bundle(bundle)
    return verified


def _bundle_import_paths(project: Path, manifest: dict) -> tuple[Path, dict]:
    report_name = manifest["report"]["path"].removeprefix("report/")
    report_path = _absolute_without_links(
        project / "data" / "research" / "experiments" / report_name,
        label="imported experiment report",
    )
    blob_paths = {}
    for blob in manifest["blobs"]:
        path = _absolute_without_links(
            project
            / "data"
            / "research"
            / "ohlcv_blobs"
            / f"{blob['sha256']}.json",
            label="imported OHLCV archive blob",
        )
        blob_paths[blob["sha256"]] = path
    return report_path, blob_paths


def _import_file_identity(path: Path) -> tuple[int, int, int, int]:
    try:
        current = path.stat()
    except OSError as exc:
        raise ValueError(
            "bundle import destination conflicts with existing evidence"
        ) from exc
    if not path.is_file():
        raise ValueError("bundle import destination conflicts with existing evidence")
    return (
        current.st_size,
        current.st_mtime_ns,
        current.st_ino,
        current.st_dev,
    )


def _import_destination_state(
    project: Path, manifest: dict, report_raw: bytes
) -> dict:
    for folder in (
        project / "data",
        project / "data" / "research",
        project / "data" / "research" / "experiments",
        project / "data" / "research" / "ohlcv_blobs",
    ):
        folder = _absolute_without_links(
            folder, label="experiment bundle import directory"
        )
        if folder.exists() and not folder.is_dir():
            raise ValueError(
                "bundle import destination conflicts with existing evidence"
            )
    report_path, blob_paths = _bundle_import_paths(project, manifest)
    reused_blobs = set()
    observations = {}
    for blob in manifest["blobs"]:
        path = blob_paths[blob["sha256"]]
        if not path.exists() and not _is_linklike(path):
            observations[path] = None
            continue
        try:
            _read_ohlcv_archive_blob(
                path, byte_count=blob["bytes"], digest=blob["sha256"]
            )
        except (OSError, TypeError, ValueError) as exc:
            raise ValueError(
                "bundle import destination conflicts with existing evidence"
            ) from exc
        reused_blobs.add(blob["sha256"])
        observations[path] = _import_file_identity(path)
    report_reused = False
    if report_path.exists() or _is_linklike(report_path):
        try:
            _, _, existing_sha256, _payload, existing_raw = (
                _load_immutable_experiment_report(project, report_path)
            )
        except (OSError, TypeError, ValueError) as exc:
            raise ValueError(
                "bundle import destination conflicts with existing evidence"
            ) from exc
        if (
            existing_sha256 != manifest["report"]["sha256"]
            or existing_raw != report_raw
        ):
            raise ValueError(
                "bundle import destination conflicts with existing evidence"
            )
        report_reused = True
        observations[report_path] = _import_file_identity(report_path)
    else:
        observations[report_path] = None
    return {
        "report_path": report_path,
        "blob_paths": blob_paths,
        "report_reused": report_reused,
        "reused_blobs": reused_blobs,
        "observations": observations,
    }


def _import_observations_unchanged(observations: dict) -> bool:
    for path, expected in observations.items():
        path = _absolute_without_links(path, label="bundle import destination")
        if expected is None:
            if path.exists() or _is_linklike(path):
                return False
            continue
        try:
            current = _import_file_identity(path)
        except ValueError:
            return False
        if current != expected:
            return False
    return True


def _ensure_import_folder(path: Path, *, label: str) -> Path:
    existed = path.exists()
    path.mkdir(parents=True, exist_ok=True)
    path = _absolute_without_links(path, label=label)
    if not path.is_dir():
        raise ValueError(f"{label} is unavailable")
    if not existed:
        _sync_directory(path.parent)
    return path


_BUNDLE_STAGING_KEY_RE = re.compile(r"^[0-9a-f]{32}$")
_BUNDLE_STAGING_LOCK_RE = re.compile(r"^\.([0-9a-f]{32})\.lock$")
_BUNDLE_STAGING_TEMP_RE = re.compile(r"^\.s\.[0-9a-f]{32}\.tmp$")
_BUNDLE_STAGING_REGISTRY_LOCK = ".registry.lock"


def _bundle_staging_lock(
    lock_path: Path, *, timeout: float, fail_when_locked: bool
):
    return portalocker.Lock(
        str(lock_path),
        mode="a",
        timeout=timeout,
        check_interval=0.05,
        fail_when_locked=fail_when_locked,
    )


@contextmanager
def _locked_bundle_staging_registry(staging_root: Path):
    lock_path = _absolute_without_links(
        staging_root / _BUNDLE_STAGING_REGISTRY_LOCK,
        label="experiment bundle staging registry lock",
    )
    with _bundle_staging_lock(
        lock_path, timeout=300, fail_when_locked=False
    ):
        yield lock_path


def _bounded_staging_entries(folder: Path, *, limit: int) -> list[Path]:
    try:
        entries = list(itertools.islice(folder.iterdir(), limit + 1))
    except OSError as exc:
        raise ValueError("experiment bundle staging is unavailable") from exc
    if len(entries) > limit:
        raise ValueError("experiment bundle staging exceeds the file limit")
    return entries


def _inspect_bundle_staging_batch_scope(
    batch: Path, *, maximum_files: int = RESEARCH_BUNDLE_STAGING_MAX_FILES
) -> dict:
    if type(maximum_files) is not int or maximum_files < 0:
        raise ValueError("experiment bundle staging file budget is invalid")
    batch = _absolute_without_links(batch, label="experiment bundle staging batch")
    if _BUNDLE_STAGING_KEY_RE.fullmatch(batch.name) is None or not batch.is_dir():
        raise ValueError("bundle staging conflicts with existing evidence")
    directories = [batch]
    files = []
    temporary_files = []
    fixed_files = set()
    latest_mtime_ns = batch.stat().st_mtime_ns

    def record_file(path: Path, *, temporary: bool = False) -> None:
        nonlocal latest_mtime_ns
        candidate = _absolute_without_links(
            path, label="experiment bundle staging entry"
        )
        if not candidate.is_file():
            raise ValueError("bundle staging conflicts with existing evidence")
        stat_result = candidate.stat()
        if stat_result.st_size > RESEARCH_REPORT_MAX_BYTES:
            raise ValueError("bundle staging conflicts with existing evidence")
        latest_mtime_ns = max(latest_mtime_ns, stat_result.st_mtime_ns)
        item = {
            "path": candidate,
            "bytes": stat_result.st_size,
            "mtime_ns": stat_result.st_mtime_ns,
        }
        files.append(item)
        if temporary:
            temporary_files.append(candidate)

    top = _bounded_staging_entries(
        batch, limit=maximum_files + 2
    )
    child_folders = {}
    for entry in top:
        if entry.name in {"manifest.json", "ready.json"}:
            record_file(entry)
            fixed_files.add(entry.name)
        elif entry.name in {"report", "blobs"}:
            candidate = _absolute_without_links(
                entry, label="experiment bundle staging entry"
            )
            if not candidate.is_dir():
                raise ValueError("bundle staging conflicts with existing evidence")
            child_folders[entry.name] = candidate
            directories.append(candidate)
            latest_mtime_ns = max(latest_mtime_ns, candidate.stat().st_mtime_ns)
        elif _BUNDLE_STAGING_TEMP_RE.fullmatch(entry.name):
            record_file(entry, temporary=True)
        else:
            raise ValueError("bundle staging conflicts with existing evidence")

    for name, folder in child_folders.items():
        children = _bounded_staging_entries(
            folder, limit=maximum_files + 1
        )
        for child in children:
            is_payload = (
                name == "report" and child.name == "report.json"
            ) or (
                name == "blobs"
                and re.fullmatch(r"[0-9a-f]{32}\.json", child.name) is not None
            )
            if is_payload:
                record_file(child)
                fixed_files.add(f"{name}/{child.name}")
            elif _BUNDLE_STAGING_TEMP_RE.fullmatch(child.name):
                record_file(child, temporary=True)
            else:
                raise ValueError("bundle staging conflicts with existing evidence")
    if len(files) > maximum_files:
        raise ValueError("experiment bundle staging exceeds the file limit")
    byte_count = sum(item["bytes"] for item in files)
    if byte_count > RESEARCH_BUNDLE_STAGING_MAX_BYTES:
        raise ValueError("experiment bundle staging exceeds the total byte limit")
    ready_marked = "ready.json" in fixed_files
    structurally_complete = bool(
        ready_marked
        and "manifest.json" in fixed_files
        and "report/report.json" in fixed_files
    )
    return {
        "path": batch,
        "key": batch.name,
        "files": files,
        "directories": directories,
        "temporary_files": temporary_files,
        "file_count": len(files),
        "bytes": byte_count,
        "latest_mtime_ns": latest_mtime_ns,
        "ready_marked": ready_marked,
        "structurally_complete": structurally_complete,
    }


def _recover_abandoned_bundle_staging_temps(batch: Path) -> int:
    if not batch.exists() and not _is_linklike(batch):
        return 0
    details = _inspect_bundle_staging_batch_scope(batch)
    for temporary in details["temporary_files"]:
        temporary.unlink()
    for folder in sorted(
        details["directories"],
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        _sync_directory(folder)
    return len(details["temporary_files"])


@contextmanager
def _locked_experiment_bundle_staging(project: Path, bundle_sha256: str):
    if (
        not isinstance(bundle_sha256, str)
        or len(bundle_sha256) != 64
        or any(character not in "0123456789abcdef" for character in bundle_sha256)
    ):
        raise ValueError("experiment bundle staging identity is invalid")
    staging_root = _ensure_import_folder(
        project / "data" / "research" / ".bi",
        label="experiment bundle staging directory",
    )
    lock_path = _absolute_without_links(
        staging_root / f".{bundle_sha256[:32]}.lock",
        label="experiment bundle staging lock",
    )
    deadline = time.monotonic() + 300.0
    bundle_lock = None
    last_error = None
    while bundle_lock is None:
        with _locked_bundle_staging_registry(staging_root):
            candidate = _bundle_staging_lock(
                lock_path, timeout=0, fail_when_locked=True
            )
            try:
                candidate.acquire()
            except portalocker.exceptions.LockException as exc:
                last_error = exc
            else:
                bundle_lock = candidate
        if bundle_lock is not None:
            break
        if time.monotonic() >= deadline:
            raise last_error
        time.sleep(0.05)
    batch = staging_root / bundle_sha256[:32]
    try:
        recovered = _recover_abandoned_bundle_staging_temps(batch)
        yield batch, staging_root, recovered
    finally:
        bundle_lock.release()


def _publish_bundle_staging_file(path: Path, raw: bytes) -> bool:
    if not isinstance(raw, bytes) or len(raw) > RESEARCH_REPORT_MAX_BYTES:
        raise ValueError("bundle staging file exceeds the size limit")
    path = _absolute_without_links(path, label="experiment bundle staging file")
    expected = hashlib.sha256(raw).hexdigest()

    def validate_existing() -> None:
        try:
            actual, byte_count = _stable_file_sha256(
                path,
                maximum_bytes=RESEARCH_REPORT_MAX_BYTES,
                label="experiment bundle staging file",
            )
        except ValueError as exc:
            raise ValueError(
                "bundle staging conflicts with existing evidence"
            ) from exc
        if actual != expected or byte_count != len(raw):
            raise ValueError("bundle staging conflicts with existing evidence")

    if path.exists() or _is_linklike(path):
        validate_existing()
        _sync_directory(path.parent)
        return False
    if not path.parent.is_dir():
        raise ValueError("experiment bundle staging directory is unavailable")
    temporary: Path | None = None
    temporary_owned = False
    primary_error: BaseException | None = None
    result: bool | None = None
    handle = None
    try:
        for attempt in range(3):
            candidate = path.parent / f".s.{uuid.uuid4().hex}.tmp"
            candidate = _absolute_without_links(
                candidate, label="temporary experiment bundle staging file"
            )
            try:
                handle = candidate.open("xb")
            except FileExistsError:
                if attempt == 2:
                    raise
                continue
            temporary = candidate
            temporary_owned = True
            break
        if handle is None or temporary is None:
            raise RuntimeError("experiment bundle staging allocation failed")
        try:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if primary_error is None:
                    primary_error = close_error
                    raise
                try:
                    primary_error.add_note(
                        "experiment bundle staging close failed: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        try:
            os.link(temporary, path)
        except FileExistsError:
            validate_existing()
            result = False
        else:
            result = True
    except BaseException as exc:
        if primary_error is None:
            primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        if temporary_owned and temporary is not None:
            for _attempt in range(2):
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    temporary_owned = False
                    break
                except OSError as exc:
                    cleanup_error = exc
                except BaseException as exc:
                    cleanup_error = exc
                    break
                else:
                    temporary_owned = False
                    break
            if not temporary_owned:
                cleanup_error = None
        sync_error: BaseException | None = None
        if temporary is not None:
            try:
                _sync_directory(path.parent)
            except BaseException as exc:
                sync_error = exc
        if primary_error is not None:
            for label, secondary_error in (
                ("temporary cleanup", cleanup_error),
                ("directory sync", sync_error),
            ):
                if secondary_error is None:
                    continue
                try:
                    primary_error.add_note(
                        f"experiment bundle staging {label} failed: "
                        f"{type(secondary_error).__name__}: {secondary_error}"
                    )
                except BaseException:
                    pass
        elif sync_error is not None:
            if cleanup_error is not None:
                try:
                    sync_error.add_note(
                        "experiment bundle staging temporary cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
            raise sync_error
        elif cleanup_error is not None:
            raise cleanup_error
    if result is None:
        raise RuntimeError("experiment bundle staging publish did not complete")
    return result


def _validate_bundle_staging_scope(batch: Path, manifest: dict) -> int:
    if not batch.exists():
        return 0
    batch = _absolute_without_links(batch, label="experiment bundle staging batch")
    if not batch.is_dir():
        raise ValueError("bundle staging conflicts with existing evidence")
    allowed_top = {"manifest.json", "ready.json", "report", "blobs"}
    try:
        top = list(itertools.islice(batch.iterdir(), len(allowed_top) + 1))
    except OSError as exc:
        raise ValueError("experiment bundle staging is unavailable") from exc
    if len(top) > 4 or any(entry.name not in allowed_top for entry in top):
        raise ValueError("bundle staging conflicts with existing evidence")
    existing_files = 0
    for entry in top:
        entry = _absolute_without_links(entry, label="experiment bundle staging entry")
        if entry.name in {"manifest.json", "ready.json"}:
            if not entry.is_file():
                raise ValueError("bundle staging conflicts with existing evidence")
            existing_files += 1
            continue
        if not entry.is_dir():
            raise ValueError("bundle staging conflicts with existing evidence")
        allowed = (
            {"report.json"}
            if entry.name == "report"
            else {f"{blob['sha256'][:32]}.json" for blob in manifest["blobs"]}
        )
        children = list(itertools.islice(entry.iterdir(), len(allowed) + 1))
        if len(children) > len(allowed) or any(
            child.name not in allowed for child in children
        ):
            raise ValueError("bundle staging conflicts with existing evidence")
        for child in children:
            child = _absolute_without_links(
                child, label="experiment bundle staging entry"
            )
            if not child.is_file():
                raise ValueError("bundle staging conflicts with existing evidence")
            existing_files += 1
    return existing_files


def _bundle_staging_ready_payload(
    manifest: dict, *, bundle_sha256: str, report_sha256: str
) -> dict:
    return {
        "schema_version": 1,
        "artifact_kind": "ohlcv_experiment_bundle_staging",
        "bundle_sha256": bundle_sha256,
        "manifest_sha256": manifest["manifest_sha256"],
        "report_sha256": report_sha256,
        "blob_files": len(manifest["blobs"]),
        "blob_bytes": sum(blob["bytes"] for blob in manifest["blobs"]),
    }


def _prepare_bundle_staging(
    archive_file,
    manifest: dict,
    report_raw: bytes,
    report_sha256: str,
    bundle_sha256: str,
    batch: Path,
) -> dict:
    existing_before = _validate_bundle_staging_scope(batch, manifest)
    batch = _ensure_import_folder(
        batch, label="experiment bundle staging batch"
    )
    report_folder = _ensure_import_folder(
        batch / "report", label="experiment bundle staging report directory"
    )
    blob_folder = _ensure_import_folder(
        batch / "blobs", label="experiment bundle staging blob directory"
    )
    created = 0
    reused = 0

    def publish(path: Path, raw: bytes) -> None:
        nonlocal created, reused
        if _publish_bundle_staging_file(path, raw):
            created += 1
        else:
            reused += 1

    publish(batch / "manifest.json", _encode_experiment_bundle_manifest(manifest))
    publish(report_folder / "report.json", report_raw)
    for blob in manifest["blobs"]:
        raw = archive_file.read(blob["path"])
        if (
            len(raw) != blob["bytes"]
            or hashlib.sha256(raw).hexdigest() != blob["sha256"]
        ):
            raise ValueError("experiment bundle changed before staging")
        publish(blob_folder / f"{blob['sha256'][:32]}.json", raw)
        del raw
    ready_raw = _encode_research_report(
        _bundle_staging_ready_payload(
            manifest,
            bundle_sha256=bundle_sha256,
            report_sha256=report_sha256,
        )
    )
    publish(batch / "ready.json", ready_raw)
    _validate_bundle_staging_scope(batch, manifest)
    return {
        "batch": batch,
        "report": report_folder / "report.json",
        "blobs": {
            blob["sha256"]: blob_folder / f"{blob['sha256'][:32]}.json"
            for blob in manifest["blobs"]
        },
        "staging_resumed": existing_before > 0,
        "staging_created_files": created,
        "staging_reused_files": reused,
    }


def _link_staged_import_file(source: Path, target: Path) -> None:
    source = _absolute_without_links(source, label="staged bundle evidence")
    target = _absolute_without_links(target, label="imported bundle evidence")
    if not source.is_file() or not target.parent.is_dir():
        raise ValueError("staged bundle evidence is unavailable")
    try:
        os.link(source, target)
    except FileExistsError as exc:
        raise ValueError(
            "bundle import destination changed during commit"
        ) from exc
    _sync_directory(target.parent)


def _commit_staged_bundle(
    project: Path, manifest: dict, report_raw: bytes, staged: dict | None
) -> dict:
    for _attempt in range(3):
        state = _import_destination_state(project, manifest, report_raw)
        with _locked_ohlcv_archive_lifecycle(project):
            if not _import_observations_unchanged(state["observations"]):
                continue
            missing = [
                blob
                for blob in manifest["blobs"]
                if blob["sha256"] not in state["reused_blobs"]
            ]
            if (missing or not state["report_reused"]) and staged is None:
                raise ValueError("experiment bundle staging is unavailable")
            if missing:
                _ensure_import_folder(
                    project / "data" / "research" / "ohlcv_blobs",
                    label="OHLCV archive import directory",
                )
            for blob in missing:
                _link_staged_import_file(
                    staged["blobs"][blob["sha256"]],
                    state["blob_paths"][blob["sha256"]],
                )
            report_created = False
            if not state["report_reused"]:
                report_folder = project / "data" / "research" / "experiments"
                report_folder_preexisting = report_folder.exists()
                _ensure_import_folder(
                    report_folder,
                    label="experiment report import directory",
                )
                try:
                    _link_staged_import_file(
                        staged["report"], state["report_path"]
                    )
                except (OSError, TypeError, ValueError):
                    if not report_folder_preexisting:
                        try:
                            report_folder.rmdir()
                        except OSError:
                            pass
                        else:
                            _sync_directory(report_folder.parent)
                    raise
                report_created = True
            return {
                "state": "IMPORTED",
                "applied": True,
                "report": str(state["report_path"]),
                "report_to_create": False,
                "report_created": report_created,
                "blobs_to_create": len(missing),
                "created_blobs": len(missing),
                "reused_blobs": len(manifest["blobs"]) - len(missing),
                "changes_files": bool(missing or report_created),
            }
    raise ValueError("bundle import destination changed repeatedly before commit")


def _cleanup_bundle_staging(batch: Path, staging_root: Path, manifest: dict) -> bool:
    try:
        _validate_bundle_staging_scope(batch, manifest)
        files = [
            batch / "ready.json",
            batch / "manifest.json",
            batch / "report" / "report.json",
            *(
                batch / "blobs" / f"{blob['sha256'][:32]}.json"
                for blob in manifest["blobs"]
            ),
        ]
        for path in files:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        for folder in (batch / "report", batch / "blobs", batch):
            try:
                folder.rmdir()
            except FileNotFoundError:
                pass
        _sync_directory(staging_root)
        return not batch.exists()
    except (OSError, TypeError, ValueError):
        return False


def import_immutable_experiment_bundle(
    root: str | Path, bundle: str | Path, *, apply: bool = False
) -> dict:
    """Plan or create-once import one verified report and its archived OHLCV blobs."""
    if not isinstance(apply, bool):
        raise TypeError("experiment bundle import apply must be boolean")
    project = _absolute_without_links(root, label="experiment bundle import root")
    if not project.is_dir():
        raise ValueError("experiment bundle import root must be a real directory")

    def consume_verified(
        archive_file,
        manifest,
        report_raw,
        _report_sha256,
        _source,
        _source_archive,
        bundle_sha256,
    ):
        if not apply:
            state = _import_destination_state(project, manifest, report_raw)
            return {
                "state": "IMPORT_PLAN",
                "applied": False,
                "report": str(state["report_path"]),
                "report_to_create": not state["report_reused"],
                "report_created": False,
                "blobs_to_create": len(manifest["blobs"])
                - len(state["reused_blobs"]),
                "created_blobs": 0,
                "reused_blobs": len(state["reused_blobs"]),
                "staging_resumed": False,
                "staging_created_files": 0,
                "staging_reused_files": 0,
                "staging_recovered_temp_files": 0,
                "staging_cleanup_pending": False,
                "changes_files": False,
            }
        initial = _import_destination_state(project, manifest, report_raw)
        if initial["report_reused"] and len(initial["reused_blobs"]) == len(
            manifest["blobs"]
        ):
            existing_staging_root = _absolute_without_links(
                project / "data" / "research" / ".bi",
                label="experiment bundle staging directory",
            )
            if existing_staging_root.exists() and not existing_staging_root.is_dir():
                raise ValueError(
                    "bundle staging conflicts with existing evidence"
                )
            existing_batch = existing_staging_root / bundle_sha256[:32]
            if not existing_batch.exists() and not _is_linklike(existing_batch):
                committed = _commit_staged_bundle(
                    project, manifest, report_raw, None
                )
                return {
                    **committed,
                    "staging_resumed": False,
                    "staging_created_files": 0,
                    "staging_reused_files": 0,
                    "staging_recovered_temp_files": 0,
                    "staging_cleanup_pending": False,
                }
            with _locked_experiment_bundle_staging(project, bundle_sha256) as (
                batch,
                staging_root,
                recovered_temps,
            ):
                committed = _commit_staged_bundle(
                    project, manifest, report_raw, None
                )
                stage_was_present = batch.exists()
                cleanup_ok = _cleanup_bundle_staging(
                    batch, staging_root, manifest
                )
                return {
                    **committed,
                    "staging_resumed": stage_was_present,
                    "staging_created_files": 0,
                    "staging_reused_files": 0,
                    "staging_recovered_temp_files": recovered_temps,
                    "staging_cleanup_pending": not cleanup_ok,
                }
        with _locked_experiment_bundle_staging(project, bundle_sha256) as (
            batch,
            staging_root,
            recovered_temps,
        ):
            refreshed = _import_destination_state(project, manifest, report_raw)
            if refreshed["report_reused"] and len(refreshed["reused_blobs"]) == len(
                manifest["blobs"]
            ):
                committed = _commit_staged_bundle(
                    project, manifest, report_raw, None
                )
                stage_was_present = batch.exists()
                cleanup_ok = _cleanup_bundle_staging(
                    batch, staging_root, manifest
                )
                return {
                    **committed,
                    "staging_resumed": stage_was_present,
                    "staging_created_files": 0,
                    "staging_reused_files": 0,
                    "staging_recovered_temp_files": recovered_temps,
                    "staging_cleanup_pending": not cleanup_ok,
                }
            staged = _prepare_bundle_staging(
                archive_file,
                manifest,
                report_raw,
                _report_sha256,
                bundle_sha256,
                batch,
            )
            committed = _commit_staged_bundle(
                project, manifest, report_raw, staged
            )
            cleanup_ok = _cleanup_bundle_staging(batch, staging_root, manifest)
            return {
                **committed,
                "staging_resumed": staged["staging_resumed"],
                "staging_created_files": staged["staging_created_files"],
                "staging_reused_files": staged["staging_reused_files"],
                "staging_recovered_temp_files": recovered_temps,
                "staging_cleanup_pending": not cleanup_ok,
            }

    verified, imported = _verify_immutable_experiment_bundle(
        bundle, verified_consumer=consume_verified
    )
    return {**verified, **imported}


def _bundle_staging_inventory_details(root: str | Path) -> dict:
    project = _absolute_without_links(root, label="bundle staging inventory root")
    if not project.is_dir():
        raise ValueError("bundle staging inventory root must be a real directory")
    staging_root = _absolute_without_links(
        project / "data" / "research" / ".bi",
        label="experiment bundle staging directory",
    )
    empty = {
        "state": "READY",
        "staging_batches": 0,
        "ready_marked_batches": 0,
        "partial_batches": 0,
        "invalid_batches": 0,
        "invalid_root_entries": 0,
        "invalid_reasons": {},
        "staging_files": 0,
        "staging_bytes": 0,
        "lock_files": 0,
        "orphan_lock_files": 0,
        "temporary_files": 0,
        "safe_to_prune": True,
        "changes_files": False,
        "_staging_root": staging_root,
        "_batch_details": [],
        "_lock_details": {},
    }
    if not staging_root.exists() and not _is_linklike(staging_root):
        return empty
    staging_root = _absolute_without_links(
        staging_root, label="experiment bundle staging directory"
    )
    if not staging_root.is_dir():
        raise ValueError("experiment bundle staging directory is invalid")
    try:
        entries = list(
            itertools.islice(
                staging_root.iterdir(),
                RESEARCH_BUNDLE_STAGING_MAX_ROOT_ENTRIES + 1,
            )
        )
    except OSError as exc:
        raise ValueError("experiment bundle staging inventory is unavailable") from exc
    if len(entries) > RESEARCH_BUNDLE_STAGING_MAX_ROOT_ENTRIES:
        raise ValueError("experiment bundle staging root entry limit exceeded")

    invalid_root_entries = 0
    invalid_batches = 0
    invalid_reasons = {}
    batches = []
    locks = {}
    staged_files_seen = 0
    staged_bytes_seen = 0
    batch_scan_blocked = False

    def reject(reason: str, *, batch: bool = False) -> None:
        nonlocal invalid_root_entries, invalid_batches
        if batch:
            invalid_batches += 1
        else:
            invalid_root_entries += 1
        invalid_reasons[reason] = invalid_reasons.get(reason, 0) + 1

    for entry in sorted(entries, key=lambda item: item.name):
        if entry.name == _BUNDLE_STAGING_REGISTRY_LOCK:
            try:
                candidate = _absolute_without_links(
                    entry, label="experiment bundle staging registry lock"
                )
                if not candidate.is_file() or candidate.stat().st_size != 0:
                    raise ValueError
            except (OSError, ValueError):
                reject("invalid_registry_lock")
            continue
        lock_match = _BUNDLE_STAGING_LOCK_RE.fullmatch(entry.name)
        if lock_match is not None:
            try:
                candidate = _absolute_without_links(
                    entry, label="experiment bundle staging lock"
                )
                stat_result = candidate.stat()
                if not candidate.is_file() or stat_result.st_size != 0:
                    raise ValueError
                locks[lock_match.group(1)] = {
                    "path": candidate,
                    "mtime_ns": stat_result.st_mtime_ns,
                }
            except (OSError, ValueError):
                reject("invalid_bundle_lock")
            continue
        if _BUNDLE_STAGING_KEY_RE.fullmatch(entry.name) is None:
            reject("unknown_root_entry")
            continue
        if len(batches) + invalid_batches >= RESEARCH_BUNDLE_STAGING_MAX_BATCHES:
            raise ValueError("experiment bundle staging batch limit exceeded")
        if batch_scan_blocked:
            try:
                candidate = _absolute_without_links(
                    entry, label="experiment bundle staging batch"
                )
                if not candidate.is_dir():
                    raise ValueError
            except (OSError, ValueError):
                pass
            reject("invalid_batch_scope", batch=True)
            continue
        try:
            details = _inspect_bundle_staging_batch_scope(
                entry,
                maximum_files=RESEARCH_BUNDLE_STAGING_MAX_FILES
                - staged_files_seen,
            )
            staged_files_seen += details["file_count"]
            staged_bytes_seen += details["bytes"]
            if staged_bytes_seen > RESEARCH_BUNDLE_STAGING_MAX_BYTES:
                raise ValueError(
                    "experiment bundle staging exceeds the total byte limit"
                )
            batches.append(details)
        except (OSError, TypeError, ValueError) as exc:
            if "file limit" in str(exc) or "total byte limit" in str(exc):
                raise
            reject("invalid_batch_scope", batch=True)
            batch_scan_blocked = True

    staging_files = staged_files_seen
    staging_bytes = staged_bytes_seen
    if staging_files > RESEARCH_BUNDLE_STAGING_MAX_FILES:
        raise ValueError("experiment bundle staging exceeds the file limit")
    if staging_bytes > RESEARCH_BUNDLE_STAGING_MAX_BYTES:
        raise ValueError("experiment bundle staging exceeds the total byte limit")
    batch_keys = {item["key"] for item in batches}
    for item in batches:
        lock = locks.get(item["key"])
        if lock is not None:
            item["latest_mtime_ns"] = max(
                item["latest_mtime_ns"], lock["mtime_ns"]
            )
            item["lock_path"] = lock["path"]
        else:
            item["lock_path"] = staging_root / f".{item['key']}.lock"
    safe_to_prune = invalid_root_entries == 0 and invalid_batches == 0
    return {
        "state": "READY" if safe_to_prune else "BLOCKED",
        "staging_batches": len(batches) + invalid_batches,
        "ready_marked_batches": sum(
            item["structurally_complete"] for item in batches
        ),
        "partial_batches": sum(
            not item["structurally_complete"] for item in batches
        ),
        "invalid_batches": invalid_batches,
        "invalid_root_entries": invalid_root_entries,
        "invalid_reasons": dict(sorted(invalid_reasons.items())),
        "staging_files": staging_files,
        "staging_bytes": staging_bytes,
        "lock_files": len(locks),
        "orphan_lock_files": len(set(locks) - batch_keys),
        "temporary_files": sum(
            len(item["temporary_files"]) for item in batches
        ),
        "safe_to_prune": safe_to_prune,
        "changes_files": False,
        "_staging_root": staging_root,
        "_batch_details": batches,
        "_lock_details": locks,
    }


def _public_bundle_staging_inventory(details: dict) -> dict:
    return {key: value for key, value in details.items() if not key.startswith("_")}


def _remove_bundle_staging_batch(details: dict, staging_root: Path) -> tuple[int, int]:
    ordered_files = sorted(
        details["files"],
        key=lambda item: (
            item["path"].name != "ready.json",
            str(item["path"]),
        ),
    )
    for item in ordered_files:
        item["path"].unlink()
    for folder in sorted(
        details["directories"], key=lambda item: len(item.parts), reverse=True
    ):
        folder.rmdir()
    _sync_directory(staging_root)
    return len(ordered_files), sum(item["bytes"] for item in ordered_files)


def prune_experiment_bundle_staging(
    root: str | Path,
    *,
    apply: bool = False,
    minimum_age_seconds: float = 7 * 24 * 60 * 60,
    now_epoch_seconds: float | None = None,
) -> dict:
    """Plan or remove only old, inactive, private bundle-staging evidence."""
    if not isinstance(apply, bool):
        raise TypeError("bundle staging GC apply must be boolean")
    minimum_age = _archive_gc_time(
        minimum_age_seconds, label="bundle staging minimum age"
    )
    if minimum_age < 24.0 * 60.0 * 60.0:
        raise ValueError("bundle staging minimum age must be at least 24 hours")
    now_epoch = _archive_gc_time(
        (
            datetime.now(timezone.utc).timestamp()
            if now_epoch_seconds is None
            else now_epoch_seconds
        ),
        label="bundle staging current time",
    )
    cutoff_ns = int((now_epoch - minimum_age) * 1_000_000_000)

    def classify(details: dict) -> tuple[list[dict], list[dict]]:
        batches = [
            item
            for item in details["_batch_details"]
            if item["latest_mtime_ns"] <= cutoff_ns
        ]
        batch_keys = {item["key"] for item in details["_batch_details"]}
        locks = [
            {"key": key, **item}
            for key, item in details["_lock_details"].items()
            if key not in batch_keys and item["mtime_ns"] <= cutoff_ns
        ]
        return batches, locks

    initial = _bundle_staging_inventory_details(root)
    eligible_batches, eligible_locks = classify(initial)
    public = _public_bundle_staging_inventory(initial)
    planned = {
        **public,
        "applied": apply,
        "minimum_age_seconds": minimum_age,
        "eligible_batches": len(eligible_batches),
        "eligible_batch_files": sum(item["file_count"] for item in eligible_batches),
        "eligible_batch_bytes": sum(item["bytes"] for item in eligible_batches),
        "eligible_orphan_lock_files": len(eligible_locks),
        "deleted_batches": 0,
        "deleted_files": 0,
        "deleted_bytes": 0,
        "deleted_lock_files": 0,
        "busy_batches": 0,
        "busy_orphan_lock_files": 0,
    }
    if not apply:
        return planned
    if not initial["safe_to_prune"]:
        raise ValueError("bundle staging inventory is not safe to prune")
    if not eligible_batches and not eligible_locks:
        return planned

    staging_root = initial["_staging_root"]
    deleted_batches = 0
    deleted_files = 0
    deleted_bytes = 0
    deleted_lock_files = 0
    busy_batches = 0
    busy_orphan_locks = 0
    with _locked_bundle_staging_registry(staging_root):
        current = _bundle_staging_inventory_details(root)
        if not current["safe_to_prune"]:
            raise ValueError("bundle staging inventory is not safe to prune")
        eligible_batches, eligible_locks = classify(current)
        planned.update(
            {
                **_public_bundle_staging_inventory(current),
                "eligible_batches": len(eligible_batches),
                "eligible_batch_files": sum(
                    item["file_count"] for item in eligible_batches
                ),
                "eligible_batch_bytes": sum(
                    item["bytes"] for item in eligible_batches
                ),
                "eligible_orphan_lock_files": len(eligible_locks),
            }
        )
        for item in eligible_batches:
            lock_preexisting = item["lock_path"].exists()
            lock = _bundle_staging_lock(
                item["lock_path"], timeout=0, fail_when_locked=True
            )
            try:
                lock.acquire()
            except portalocker.exceptions.LockException:
                busy_batches += 1
                continue
            batch_deleted = False
            try:
                refreshed = _inspect_bundle_staging_batch_scope(item["path"])
                lock_path = item["lock_path"]
                if lock_preexisting:
                    refreshed["latest_mtime_ns"] = max(
                        refreshed["latest_mtime_ns"], lock_path.stat().st_mtime_ns
                    )
                if refreshed["latest_mtime_ns"] <= cutoff_ns:
                    count, byte_count = _remove_bundle_staging_batch(
                        refreshed, staging_root
                    )
                    deleted_batches += 1
                    deleted_files += count
                    deleted_bytes += byte_count
                    batch_deleted = True
            finally:
                lock.release()
            if batch_deleted or not lock_preexisting:
                lock_path = _absolute_without_links(
                    item["lock_path"], label="experiment bundle staging lock"
                )
                if lock_path.exists():
                    if not lock_path.is_file() or lock_path.stat().st_size != 0:
                        raise ValueError("bundle staging lock changed before prune")
                    lock_path.unlink()
                    if lock_preexisting:
                        deleted_lock_files += 1
                    _sync_directory(staging_root)
        for item in eligible_locks:
            lock = _bundle_staging_lock(
                item["path"], timeout=0, fail_when_locked=True
            )
            try:
                lock.acquire()
            except portalocker.exceptions.LockException:
                busy_orphan_locks += 1
                continue
            try:
                current_lock = _absolute_without_links(
                    item["path"], label="experiment bundle staging lock"
                )
                stat_result = current_lock.stat()
                if (
                    not current_lock.is_file()
                    or stat_result.st_size != 0
                    or stat_result.st_mtime_ns > cutoff_ns
                ):
                    continue
            finally:
                lock.release()
            current_lock.unlink()
            deleted_lock_files += 1
            _sync_directory(staging_root)
    return {
        **planned,
        "changes_files": bool(deleted_batches or deleted_lock_files),
        "deleted_batches": deleted_batches,
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "deleted_lock_files": deleted_lock_files,
        "busy_batches": busy_batches,
        "busy_orphan_lock_files": busy_orphan_locks,
    }


_OHLCV_BLOB_NAME_RE = re.compile(r"^([0-9a-f]{64})\.json$")


def _build_ohlcv_archive_inventory_details(root: str | Path) -> dict:
    project = _absolute_without_links(root, label="OHLCV archive root")
    if not project.is_dir():
        raise ValueError("OHLCV archive root must be a real directory")
    report_folder = _absolute_without_links(
        project / "data" / "research" / "experiments",
        label="experiment report directory",
    )
    blob_folder = _absolute_without_links(
        project / "data" / "research" / "ohlcv_blobs",
        label="OHLCV archive directory",
    )
    report_paths = []
    report_file_count = 0
    invalid_reports = 0
    invalid_report_reasons = {}

    def reject_report(reason: str) -> None:
        nonlocal invalid_reports
        invalid_reports += 1
        invalid_report_reasons[reason] = invalid_report_reasons.get(reason, 0) + 1

    if report_folder.exists():
        if not report_folder.is_dir():
            raise ValueError("experiment report directory is invalid")
        try:
            entries = sorted(report_folder.iterdir())
        except OSError as exc:
            raise ValueError("experiment report inventory is unavailable") from exc
        if len(entries) > RESEARCH_ARCHIVE_MAX_REPORTS:
            raise ValueError("experiment report inventory exceeds the file limit")
        report_file_count = len(entries)
        for entry in entries:
            try:
                candidate = _absolute_without_links(
                    entry, label="experiment report"
                )
            except ValueError:
                reject_report("linked_or_invalid_path")
                continue
            if (
                not candidate.is_file()
                or _EXPERIMENT_REPORT_NAME_RE.fullmatch(candidate.name) is None
            ):
                reject_report("invalid_file_or_name")
                continue
            report_paths.append(candidate)
    try:
        report_inventory_bytes = sum(path.stat().st_size for path in report_paths)
    except OSError as exc:
        raise ValueError("experiment report inventory metadata is unavailable") from exc
    if report_inventory_bytes > RESEARCH_ARCHIVE_REPORT_INVENTORY_MAX_BYTES:
        raise ValueError("experiment report inventory exceeds the total byte limit")
    references = {}
    archived_reports = 0
    legacy_reports = 0
    for report_path in report_paths:
        try:
            _, _, _, payload, _raw = _load_immutable_experiment_report(
                project, report_path
            )
            source = payload.get("ohlcv_panel_source")
            archive = payload.get("ohlcv_panel_source_archive")
            if archive is None:
                legacy_reports += 1
                continue
            if not isinstance(source, dict):
                raise TypeError("archived experiment source is missing")
            expected = _validated_ohlcv_archive_contract(source, archive)
            artifacts = _validated_ohlcv_source_contract(source)
            for artifact, blob in zip(
                artifacts, expected["blobs"], strict=True
            ):
                prior = references.get(blob["blob_path"], [])
                reference = {
                    "artifact": artifact,
                    "source": source,
                    "blob": blob,
                }
                if prior and prior[0]["blob"] != blob:
                    raise ValueError("OHLCV archive reference conflict")
                references.setdefault(blob["blob_path"], []).append(reference)
            archived_reports += 1
        except (OSError, TypeError, ValueError):
            reject_report("invalid_report_contract")
    blob_paths = []
    blob_file_count = 0
    invalid_blobs = 0
    invalid_blob_reasons = {}

    def reject_blob(reason: str) -> None:
        nonlocal invalid_blobs
        invalid_blobs += 1
        invalid_blob_reasons[reason] = invalid_blob_reasons.get(reason, 0) + 1

    if blob_folder.exists():
        if not blob_folder.is_dir():
            raise ValueError("OHLCV archive directory is invalid")
        try:
            entries = sorted(blob_folder.iterdir())
        except OSError as exc:
            raise ValueError("OHLCV blob inventory is unavailable") from exc
        if len(entries) > RESEARCH_ARCHIVE_MAX_BLOBS:
            raise ValueError("OHLCV blob inventory exceeds the file limit")
        blob_file_count = len(entries)
        for entry in entries:
            try:
                candidate = _absolute_without_links(entry, label="OHLCV archive blob")
            except ValueError:
                reject_blob("linked_or_invalid_path")
                continue
            if (
                not candidate.is_file()
                or _OHLCV_BLOB_NAME_RE.fullmatch(candidate.name) is None
            ):
                reject_blob("invalid_file_or_name")
                continue
            blob_paths.append(candidate)
    try:
        inventory_bytes = sum(path.stat().st_size for path in blob_paths)
    except OSError as exc:
        raise ValueError("OHLCV blob inventory metadata is unavailable") from exc
    if inventory_bytes > RESEARCH_ARCHIVE_INVENTORY_MAX_BYTES:
        raise ValueError("OHLCV blob inventory exceeds the total byte limit")
    blob_details = []
    present_relative_paths = set()
    referenced_bytes = 0
    orphan_bytes = 0
    for blob_path in blob_paths:
        relative = blob_path.relative_to(project).as_posix()
        present_relative_paths.add(relative)
        match = _OHLCV_BLOB_NAME_RE.fullmatch(blob_path.name)
        digest = match.group(1)
        try:
            stat_result = blob_path.stat()
            byte_count = stat_result.st_size
            if byte_count <= 0 or byte_count > RESEARCH_ARCHIVE_BLOB_MAX_BYTES:
                raise ValueError("OHLCV archive blob size is invalid")
            blob_references = references.get(relative, [])
            if blob_references and (
                blob_references[0]["blob"]["sha256"] != digest
                or blob_references[0]["blob"]["bytes"] != byte_count
            ):
                raise ValueError("OHLCV archive reference metadata mismatch")
            raw = _read_ohlcv_archive_blob(
                blob_path, byte_count=byte_count, digest=digest
            )
            if blob_references:
                for reference in blob_references:
                    _verify_ohlcv_artifact_bytes(
                        raw, reference["artifact"], reference["source"]
                    )
                referenced_bytes += byte_count
            else:
                orphan_bytes += byte_count
            blob_details.append(
                {
                    "path": blob_path,
                    "relative": relative,
                    "digest": digest,
                    "bytes": byte_count,
                    "mtime_ns": stat_result.st_mtime_ns,
                    "referenced": bool(blob_references),
                }
            )
        except (OSError, TypeError, ValueError):
            reject_blob("invalid_blob_content")
    missing_references = sorted(set(references) - present_relative_paths)
    referenced = [item for item in blob_details if item["referenced"]]
    orphans = [item for item in blob_details if not item["referenced"]]
    safe_to_prune = bool(
        invalid_reports == 0
        and invalid_blobs == 0
        and not missing_references
    )
    return {
        "state": "READY" if safe_to_prune else "BLOCKED",
        "report_files": report_file_count,
        "report_inventory_bytes": report_inventory_bytes,
        "archived_reports": archived_reports,
        "legacy_reports": legacy_reports,
        "invalid_reports": invalid_reports,
        "invalid_report_reasons": dict(sorted(invalid_report_reasons.items())),
        "blob_files": blob_file_count,
        "referenced_blobs": len(referenced),
        "orphan_blobs": len(orphans),
        "missing_referenced_blobs": len(missing_references),
        "invalid_blobs": invalid_blobs,
        "invalid_blob_reasons": dict(sorted(invalid_blob_reasons.items())),
        "inventory_bytes": inventory_bytes,
        "referenced_bytes": referenced_bytes,
        "orphan_bytes": orphan_bytes,
        "referenced_digests": sorted(item["digest"] for item in referenced),
        "orphan_digests": sorted(item["digest"] for item in orphans),
        "safe_to_prune": safe_to_prune,
        "changes_files": False,
        "_blob_details": blob_details,
    }


def build_ohlcv_archive_inventory(root: str | Path) -> dict:
    inventory = _build_ohlcv_archive_inventory_details(root)
    return {key: value for key, value in inventory.items() if not key.startswith("_")}


def _archive_gc_time(value, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a finite nonnegative number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.0:
        raise ValueError(f"{label} must be a finite nonnegative number")
    return parsed


def prune_ohlcv_archive_blobs(
    root: str | Path,
    *,
    apply: bool = False,
    minimum_age_seconds: float = 7 * 24 * 60 * 60,
    now_epoch_seconds: float | None = None,
) -> dict:
    if not isinstance(apply, bool):
        raise TypeError("OHLCV archive GC apply must be boolean")
    minimum_age = _archive_gc_time(
        minimum_age_seconds, label="OHLCV archive minimum age"
    )
    if minimum_age < 24.0 * 60.0 * 60.0:
        raise ValueError("OHLCV archive minimum age must be at least 24 hours")
    now_epoch = _archive_gc_time(
        (
            datetime.now(timezone.utc).timestamp()
            if now_epoch_seconds is None
            else now_epoch_seconds
        ),
        label="OHLCV archive current time",
    )
    cutoff_ns = int((now_epoch - minimum_age) * 1_000_000_000)

    def inspect() -> tuple[dict, list[dict]]:
        details = _build_ohlcv_archive_inventory_details(root)
        eligible = [
            item
            for item in details["_blob_details"]
            if not item["referenced"] and item["mtime_ns"] <= cutoff_ns
        ]
        return details, eligible

    if not apply:
        inventory, eligible = inspect()
        public = {
            key: value for key, value in inventory.items() if not key.startswith("_")
        }
        return {
            **public,
            "applied": False,
            "minimum_age_seconds": minimum_age,
            "eligible_orphan_blobs": len(eligible),
            "eligible_orphan_bytes": sum(item["bytes"] for item in eligible),
            "deleted_blobs": 0,
            "deleted_bytes": 0,
        }
    with _locked_ohlcv_archive_lifecycle(root):
        inventory, eligible = inspect()
        if not inventory["safe_to_prune"]:
            raise ValueError("OHLCV archive inventory is not safe to prune")
        for item in eligible:
            path = _absolute_without_links(item["path"], label="OHLCV archive blob")
            current = path.stat()
            if (
                not path.is_file()
                or current.st_size != item["bytes"]
                or current.st_mtime_ns != item["mtime_ns"]
            ):
                raise ValueError("OHLCV archive blob changed before prune")
            _read_ohlcv_archive_blob(
                path, byte_count=item["bytes"], digest=item["digest"]
            )
        for item in eligible:
            item["path"].unlink()
        if eligible:
            _sync_directory(eligible[0]["path"].parent)
        public = {
            key: value for key, value in inventory.items() if not key.startswith("_")
        }
        return {
            **public,
            "applied": True,
            "changes_files": bool(eligible),
            "minimum_age_seconds": minimum_age,
            "eligible_orphan_blobs": len(eligible),
            "eligible_orphan_bytes": sum(item["bytes"] for item in eligible),
            "deleted_blobs": len(eligible),
            "deleted_bytes": sum(item["bytes"] for item in eligible),
        }
