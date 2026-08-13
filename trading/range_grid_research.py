"""Bounded long-only range-grid research with causal next-bar execution."""
from __future__ import annotations

import math
import statistics
from typing import Iterable


class _InvalidNumericEvidence(ValueError):
    """A finite input produced unusable numeric research evidence."""


def _finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _finite_parameter(name: str, value) -> float:
    parsed = _finite(value)
    if parsed is None:
        raise ValueError(f"{name} must be a finite number")
    return parsed


def _integer_parameter(name: str, value, *, minimum: int) -> int:
    parsed = _finite_parameter(name, value)
    if not parsed.is_integer():
        raise ValueError(f"{name} must be an integer")
    return max(minimum, int(parsed))


def _metrics(values: list[float]) -> dict:
    if not values:
        return {
            "samples": 0,
            "mean_net_bps": None,
            "median_net_bps": None,
            "win_rate": None,
            "compounded_return_pct": None,
            "max_drawdown_pct": None,
        }
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in values:
        if not math.isfinite(value):
            raise _InvalidNumericEvidence("return observation is not finite")
        equity *= max(0.0, 1.0 + value / 10_000.0)
        if not math.isfinite(equity):
            raise _InvalidNumericEvidence("compounded return overflowed")
        peak = max(peak, equity)
        if peak > 0.0:
            drawdown = max(drawdown, 1.0 - equity / peak)
    result = {
        "samples": len(values),
        "mean_net_bps": statistics.mean(values),
        "median_net_bps": statistics.median(values),
        "win_rate": sum(value > 0.0 for value in values) / len(values),
        "compounded_return_pct": (equity - 1.0) * 100.0,
        "max_drawdown_pct": drawdown * 100.0,
    }
    if any(
        not math.isfinite(value)
        for key, value in result.items()
        if key != "samples"
    ):
        raise _InvalidNumericEvidence("summary metric is not finite")
    return result


def _series(rows: Iterable[tuple[int, float, float, float]]) -> dict[int, tuple[float, float]]:
    result = {}
    conflicted_timestamps = set()
    for row in rows:
        try:
            timestamp, raw_open, raw_close, _volume = row
        except (TypeError, ValueError, OverflowError):
            continue
        if isinstance(timestamp, bool):
            continue
        try:
            timestamp_value = int(timestamp)
            timestamp_number = float(timestamp)
        except (TypeError, ValueError, OverflowError):
            continue
        opening, close = _finite(raw_open), _finite(raw_close)
        if (
            not math.isfinite(timestamp_number)
            or timestamp_number <= 0.0
            or timestamp_number != timestamp_value
            or opening is None
            or close is None
            or opening <= 0.0
            or close <= 0.0
        ):
            continue
        candidate = (opening, close)
        if timestamp_value in conflicted_timestamps:
            continue
        existing = result.get(timestamp_value)
        if existing is None:
            result[timestamp_value] = candidate
        elif existing != candidate:
            result.pop(timestamp_value)
            conflicted_timestamps.add(timestamp_value)
    return result


def _gap_exit_bps(
    *,
    position: float,
    last_mark: float,
    next_open: float,
    one_way_cost_bps: float,
) -> float:
    """Mark carried inventory across a data gap and exit when price returns."""
    exposure = max(0.0, min(1.0, _finite_parameter("position", position)))
    last_mark_value = _finite_parameter("last_mark", last_mark)
    next_open_value = _finite_parameter("next_open", next_open)
    if last_mark_value <= 0.0 or next_open_value <= 0.0:
        raise ValueError("gap marks must be positive")
    costs = max(
        0.0, _finite_parameter("one_way_cost_bps", one_way_cost_bps)
    )
    result = exposure * (next_open_value / last_mark_value - 1.0) * 10_000.0
    result -= exposure * costs
    if not math.isfinite(result):
        raise _InvalidNumericEvidence("gap return overflowed")
    return result


def evaluate_bounded_range_grid(
    panel: dict[str, list[tuple[int, float, float, float]]],
    *,
    lookback_hours: int = 168,
    grid_levels: int = 5,
    minimum_range_width_pct: float = 1.0,
    maximum_range_width_pct: float = 15.0,
    maximum_trend_fraction: float = 0.35,
    one_way_cost_bps: float = 12.0,
    minimum_samples: int = 500,
) -> dict:
    """Mark all inventory to market; never shorts, leverages, or martingales."""
    hour_ms = 3_600_000
    lookback = _integer_parameter("lookback_hours", lookback_hours, minimum=24)
    levels = _integer_parameter("grid_levels", grid_levels, minimum=2)
    minimum_required = _integer_parameter(
        "minimum_samples", minimum_samples, minimum=1
    )
    minimum_width = _finite_parameter(
        "minimum_range_width_pct", minimum_range_width_pct
    )
    maximum_width = _finite_parameter(
        "maximum_range_width_pct", maximum_range_width_pct
    )
    if minimum_width > maximum_width:
        raise ValueError(
            "minimum_range_width_pct must not exceed maximum_range_width_pct"
        )
    maximum_trend = max(
        0.0, _finite_parameter("maximum_trend_fraction", maximum_trend_fraction)
    )
    costs = max(0.0, _finite_parameter("one_way_cost_bps", one_way_cost_bps))
    portfolio_by_time: dict[int, list[float]] = {}
    pooled_observations = 0
    per_symbol = []
    invalid_numeric_symbols = []
    for symbol, raw_rows in sorted(panel.items()):
        rows = _series(raw_rows)
        timestamps = sorted(rows)
        if len(timestamps) < lookback + 2:
            continue
        values = []
        position = 0.0
        turnover = 0.0
        active = 0
        breaches = 0
        gap_exits = 0
        invalid_numeric_evidence = False
        symbol_portfolio_by_time: dict[int, list[float]] = {}
        last_portfolio_slot: tuple[int, int] | None = None
        for index in range(lookback - 1, len(timestamps) - 2):
            cutoff = timestamps[index]
            entry_time = timestamps[index + 1]
            exit_time = timestamps[index + 2]
            if entry_time - cutoff != hour_ms or exit_time - entry_time != hour_ms:
                if position > 0.0:
                    try:
                        gap_value = _gap_exit_bps(
                            position=position,
                            last_mark=rows[entry_time][0],
                            next_open=rows[exit_time][0],
                            one_way_cost_bps=costs,
                        )
                    except _InvalidNumericEvidence:
                        invalid_numeric_evidence = True
                        break
                    values.append(gap_value)
                    bucket = symbol_portfolio_by_time.setdefault(exit_time, [])
                    bucket.append(gap_value)
                    last_portfolio_slot = (exit_time, len(bucket) - 1)
                    turnover += position
                    gap_exits += 1
                    position = 0.0
                continue
            history_times = timestamps[index - lookback + 1 : index + 1]
            if any(
                right - left != hour_ms
                for left, right in zip(history_times, history_times[1:])
            ):
                if position > 0.0:
                    try:
                        gap_value = _gap_exit_bps(
                            position=position,
                            last_mark=rows[entry_time][0],
                            next_open=rows[entry_time][0],
                            one_way_cost_bps=costs,
                        )
                    except _InvalidNumericEvidence:
                        invalid_numeric_evidence = True
                        break
                    values.append(gap_value)
                    bucket = symbol_portfolio_by_time.setdefault(entry_time, [])
                    bucket.append(gap_value)
                    last_portfolio_slot = (entry_time, len(bucket) - 1)
                    turnover += position
                    gap_exits += 1
                    position = 0.0
                continue
            closes = [rows[timestamp][1] for timestamp in history_times]
            lower, upper = min(closes), max(closes)
            midpoint = lower / 2.0 + upper / 2.0
            width_pct = (upper - lower) / midpoint * 100.0 if midpoint > 0.0 else 0.0
            trend_fraction = (
                abs(closes[-1] - closes[0]) / (upper - lower)
                if upper > lower else math.inf
            )
            if not math.isfinite(width_pct) or (
                upper > lower and not math.isfinite(trend_fraction)
            ):
                invalid_numeric_evidence = True
                break
            regime_ok = (
                minimum_width <= width_pct <= maximum_width
                and trend_fraction <= maximum_trend
            )
            entry_open = rows[entry_time][0]
            next_open = rows[exit_time][0]
            breached = not lower <= entry_open <= upper
            if regime_ok and not breached:
                normalized = (entry_open - lower) / (upper - lower)
                raw_target = 1.0 - normalized
                target = round(max(0.0, min(1.0, raw_target)) * levels) / levels
                if not all(
                    math.isfinite(value)
                    for value in (normalized, raw_target, target)
                ):
                    invalid_numeric_evidence = True
                    break
                active += 1
            else:
                target = 0.0
                breaches += int(regime_ok and breached)
            trade = abs(target - position)
            turnover += trade
            net_bps = target * (next_open / entry_open - 1.0) * 10_000.0
            net_bps -= trade * costs
            if not math.isfinite(net_bps):
                invalid_numeric_evidence = True
                break
            values.append(net_bps)
            bucket = symbol_portfolio_by_time.setdefault(exit_time, [])
            bucket.append(net_bps)
            last_portfolio_slot = (exit_time, len(bucket) - 1)
            position = target
        if invalid_numeric_evidence:
            invalid_numeric_symbols.append(str(symbol))
            continue
        if values and position > 0.0:
            liquidation_cost = position * costs
            values[-1] -= liquidation_cost
            if last_portfolio_slot is not None:
                bucket_time, bucket_index = last_portfolio_slot
                symbol_portfolio_by_time[bucket_time][bucket_index] -= liquidation_cost
            turnover += position
            position = 0.0
        if not values:
            continue
        try:
            symbol_metrics = _metrics(values)
        except _InvalidNumericEvidence:
            invalid_numeric_symbols.append(str(symbol))
            continue
        for timestamp, symbol_values in symbol_portfolio_by_time.items():
            portfolio_by_time.setdefault(timestamp, []).extend(symbol_values)
        pooled_observations += len(values)
        per_symbol.append(
            {
                "symbol": str(symbol),
                **symbol_metrics,
                "active_windows": active,
                "range_breaches": breaches,
                "gap_exits": gap_exits,
                "gross_turnover": turnover,
                "final_inventory": position,
            }
        )
    portfolio_returns = [
        statistics.mean(portfolio_by_time[timestamp])
        for timestamp in sorted(portfolio_by_time)
    ]
    try:
        portfolio_metrics = _metrics(portfolio_returns)
        portfolio_numeric_valid = True
    except _InvalidNumericEvidence:
        portfolio_metrics = _metrics([])
        portfolio_numeric_valid = False
    data_sufficient = (
        portfolio_numeric_valid and len(portfolio_returns) >= minimum_required
    )
    return {
        **portfolio_metrics,
        "symbols": len(per_symbol),
        "pooled_symbol_observations": pooled_observations,
        "invalid_numeric_symbols": invalid_numeric_symbols,
        "portfolio_numeric_valid": portfolio_numeric_valid,
        "portfolio_aggregation": "equal_weight_mean_by_exit_timestamp",
        "minimum_samples": minimum_required,
        "data_sufficient": data_sufficient,
        "ready": data_sufficient,
        "lookback_hours": lookback,
        "grid_levels": levels,
        "one_way_cost_bps": costs,
        "inventory_model": "long_only_bounded_zero_to_one_mark_to_market",
        "leverage": False,
        "shorting": False,
        "martingale": False,
        "same_bar_execution": False,
        "signal_cutoff": "completed_bar_close",
        "execution_price": "next_bar_open",
        "final_liquidation_cost_included": True,
        "gap_mark_to_market_included": True,
        "changes_orders": False,
        "simulation_only": True,
        "per_symbol": per_symbol[:100],
    }
