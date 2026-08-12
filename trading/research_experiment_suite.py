"""Offline, fail-closed experiment suite for profit research.

The suite consumes existing telemetry and cached market data.  It never places
orders, changes runtime models, or promotes a result automatically.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import statistics
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from trading.expectancy_training import (
    CandidateLabel,
    ExpectancyPrediction,
    expanding_walk_forward_fit,
)

RESEARCH_VENUE_PAYLOAD_MAX_BYTES = 4 * 1024 * 1024


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
    for timestamp in timestamps:
        if (timestamp - origin) % holding_ms:
            continue
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


def load_cached_ohlcv_panel(
    root: str | Path,
    *,
    max_symbols: int = 100,
    max_bars: int = 8_760,
    maximum_file_bytes: int = 50_000_000,
) -> dict[str, list[tuple[int, float, float, float]]]:
    folder = Path(root) / "data" / "ohlcv_cache"
    panel = {}
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    symbol_limit = max(1, int(max_symbols))
    bar_limit = max(1, int(max_bars))
    file_limit = max(1, int(maximum_file_bytes))
    for path in sorted(folder.glob("*__1h.json")):
        if len(panel) >= symbol_limit:
            break
        try:
            with path.open("rb") as handle:
                raw = handle.read(file_limit + 1)
            if len(raw) > file_limit:
                continue
            payload = json.loads(raw.decode("utf-8-sig"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, list):
            continue
        candidates = []
        for bar in payload:
            if not isinstance(bar, list) or len(bar) < 6:
                continue
            if isinstance(bar[0], bool):
                continue
            try:
                timestamp = int(bar[0])
                timestamp_number = float(bar[0])
            except (TypeError, ValueError, OverflowError):
                continue
            open_price = _finite(bar[1])
            close = _finite(bar[4])
            volume = _finite(bar[5])
            if (
                not math.isfinite(timestamp_number)
                or timestamp <= 0
                or timestamp_number != timestamp
                or timestamp + 3_600_000 > now_ms
                or open_price is None
                or open_price <= 0.0
                or close is None
                or close <= 0.0
                or volume is None
                or volume < 0.0
            ):
                continue
            candidates.append((timestamp, open_price, close, volume))
        rows = [
            (timestamp, opening, close, volume)
            for timestamp, (opening, close, volume) in sorted(
                _series_map(candidates).items()
            )[-bar_limit:]
        ]
        if rows:
            symbol = path.name.removesuffix("__1h.json")
            if not symbol:
                continue
            panel[symbol] = rows
    return panel


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


def _carry_history_report(root: Path, *, minimum_samples: int = 90) -> dict:
    from trading.carry_sim import CarryEngine, CarryState, CarryTerms

    grouped: dict[str, dict[str, dict]] = {}
    overview_counts: dict[str, int] = {}
    missing_settlement = 0
    noncausal_settlement_observations = 0
    unsettled_funding_periods: set[tuple[str, str]] = set()
    analysis_time = datetime.now(timezone.utc)
    folder = root / "data" / "venue_native" / "overview"
    total_events = 0
    for path in folder.glob("*.sqlite3"):
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
                total_events += 1
                try:
                    payload = json.loads(encoded)
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
                    snapshot_received_time = _utc_datetime(received_time)
                    if (
                        snapshot_time is None
                        or snapshot_received_time is None
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
            for left, right in zip(values, values[1:])
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
            for left, right in zip(settlement_times, settlement_times[1:])
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
            len(periods) >= required
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
) -> dict:
    """Run every currently measurable experiment without mutating the project."""
    from trading.expectancy_telemetry import EXPECTANCY_FEATURES
    from trading.profit_research_runner import (
        build_carry_preview,
        build_execution_cost_report,
        build_execution_policy_report,
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
        "closed_labels_by_schema": schema_counts,
        "minimum_rows": max(30, int(minimum_expectancy_rows)),
    }
    abstention = None
    expectancy_ready = False
    if len(labels) >= max(30, int(minimum_expectancy_rows)):
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

    panel = load_cached_ohlcv_panel(project)
    momentum = evaluate_momentum_panel(
        panel, minimum_windows=minimum_momentum_windows
    )
    regime = evaluate_regime_shift(panel)
    from trading.range_grid_research import evaluate_bounded_range_grid

    grid = evaluate_bounded_range_grid(panel)
    abstention_ready = bool(
        abstention
        and any(row.get("ready") is True for row in abstention.get("rows", []))
    )
    uncertainty_ready = bool(regime.get("ready") and abstention_ready)

    results = {
        "net_expectancy": _result(
            expectancy_ready,
            expectancy_evidence,
            "purged walk-forward evidence available"
            if expectancy_ready
            else "insufficient closed causal labels",
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
            bool(momentum["ready"]),
            momentum,
            "causal momentum panel is sample-ready"
            if momentum["ready"]
            else (
                "exploratory metrics computed, but point-in-time universe is incomplete"
                if momentum["data_sufficient"]
                else "insufficient symbols or rebalance windows"
            ),
        ),
        "time_series_momentum_crash": _result(
            bool(momentum["ready"]),
            {
                "time_series": momentum["time_series"],
                "crash_guard": momentum["crash_guard"],
                "causality": momentum["causality"],
                "minimum_windows": momentum["minimum_windows"],
            },
            "past-only TSMOM and crash overlay are sample-ready"
            if momentum["ready"]
            else (
                "exploratory metrics computed, but point-in-time universe is incomplete"
                if momentum["data_sufficient"]
                else "insufficient causal momentum windows"
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
            else "regime history or calibrated OOS predictions are unavailable",
        ),
        "bounded_regime_grid": _result(
            bool(grid["ready"]),
            grid,
            "bounded causal grid simulation is sample-ready"
            if grid["ready"]
            else "insufficient contiguous OHLC observations for grid research",
        ),
    }
    ready_count = sum(result["ready"] for result in results.values())
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root": str(project.resolve()),
        "bot": normalized_bot,
        "mode": normalized_mode,
        "catalog": [asdict(item) for item in EXPERIMENT_CATALOG],
        "results": results,
        "summary": {"ready": ready_count, "blocked": len(results) - ready_count},
        "safety": {
            "changes_orders": False,
            "writes_live_model": False,
            "automatic_promotion": False,
        },
    }


def write_immutable_experiment_report(root: str | Path, payload: dict) -> Path:
    """Write a uniquely named research artifact; never overwrite an old report."""
    encoded = json.dumps(
        payload, default=str, sort_keys=True, indent=2, allow_nan=False
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()[:12]
    folder = Path(root) / "data" / "research" / "experiments"
    folder.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    unique = uuid.uuid4().hex[:8]
    path = folder / f"profit_experiments_{stamp}_{digest}_{unique}.json"
    temp = path.with_suffix(".tmp")
    with temp.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)
    return path
