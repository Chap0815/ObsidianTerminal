"""Bounded long-only range-grid research with causal next-bar execution."""
from __future__ import annotations

import math
import statistics
from typing import Iterable


def _finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


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
        equity *= max(0.0, 1.0 + value / 10_000.0)
        peak = max(peak, equity)
        if peak > 0.0:
            drawdown = max(drawdown, 1.0 - equity / peak)
    return {
        "samples": len(values),
        "mean_net_bps": statistics.mean(values),
        "median_net_bps": statistics.median(values),
        "win_rate": sum(value > 0.0 for value in values) / len(values),
        "compounded_return_pct": (equity - 1.0) * 100.0,
        "max_drawdown_pct": drawdown * 100.0,
    }


def _series(rows: Iterable[tuple[int, float, float, float]]) -> dict[int, tuple[float, float]]:
    result = {}
    for row in rows:
        try:
            timestamp, raw_open, raw_close, _volume = row
            timestamp = int(timestamp)
        except (TypeError, ValueError, OverflowError):
            continue
        opening, close = _finite(raw_open), _finite(raw_close)
        if opening is not None and close is not None and opening > 0.0 and close > 0.0:
            result[timestamp] = (opening, close)
    return result


def _gap_exit_bps(
    *,
    position: float,
    last_mark: float,
    next_open: float,
    one_way_cost_bps: float,
) -> float:
    """Mark carried inventory across a data gap and exit when price returns."""
    exposure = max(0.0, min(1.0, float(position)))
    if not math.isfinite(last_mark) or not math.isfinite(next_open):
        raise ValueError("gap marks must be finite")
    if last_mark <= 0.0 or next_open <= 0.0:
        raise ValueError("gap marks must be positive")
    costs = max(0.0, float(one_way_cost_bps))
    return exposure * (next_open / last_mark - 1.0) * 10_000.0 - exposure * costs


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
    lookback = max(24, int(lookback_hours))
    levels = max(2, int(grid_levels))
    costs = max(0.0, float(one_way_cost_bps))
    portfolio_by_time: dict[int, list[float]] = {}
    pooled_observations = 0
    per_symbol = []
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
        last_portfolio_slot: tuple[int, int] | None = None
        for index in range(lookback - 1, len(timestamps) - 2):
            cutoff = timestamps[index]
            entry_time = timestamps[index + 1]
            exit_time = timestamps[index + 2]
            if entry_time - cutoff != hour_ms or exit_time - entry_time != hour_ms:
                if position > 0.0:
                    gap_value = _gap_exit_bps(
                        position=position,
                        last_mark=rows[cutoff][1],
                        next_open=rows[entry_time][0],
                        one_way_cost_bps=costs,
                    )
                    values.append(gap_value)
                    bucket = portfolio_by_time.setdefault(entry_time, [])
                    bucket.append(gap_value)
                    last_portfolio_slot = (entry_time, len(bucket) - 1)
                    pooled_observations += 1
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
                    gap_value = _gap_exit_bps(
                        position=position,
                        last_mark=rows[cutoff][1],
                        next_open=rows[entry_time][0],
                        one_way_cost_bps=costs,
                    )
                    values.append(gap_value)
                    bucket = portfolio_by_time.setdefault(entry_time, [])
                    bucket.append(gap_value)
                    last_portfolio_slot = (entry_time, len(bucket) - 1)
                    pooled_observations += 1
                    turnover += position
                    gap_exits += 1
                    position = 0.0
                continue
            closes = [rows[timestamp][1] for timestamp in history_times]
            lower, upper = min(closes), max(closes)
            midpoint = (lower + upper) / 2.0
            width_pct = (upper - lower) / midpoint * 100.0 if midpoint > 0.0 else 0.0
            trend_fraction = (
                abs(closes[-1] - closes[0]) / (upper - lower)
                if upper > lower else math.inf
            )
            regime_ok = (
                float(minimum_range_width_pct) <= width_pct
                <= float(maximum_range_width_pct)
                and trend_fraction <= max(0.0, float(maximum_trend_fraction))
            )
            entry_open = rows[entry_time][0]
            next_open = rows[exit_time][0]
            breached = not lower <= entry_open <= upper
            if regime_ok and not breached:
                normalized = (entry_open - lower) / (upper - lower)
                raw_target = 1.0 - normalized
                target = round(max(0.0, min(1.0, raw_target)) * levels) / levels
                active += 1
            else:
                target = 0.0
                breaches += int(regime_ok and breached)
            trade = abs(target - position)
            turnover += trade
            net_bps = target * (next_open / entry_open - 1.0) * 10_000.0
            net_bps -= trade * costs
            values.append(net_bps)
            bucket = portfolio_by_time.setdefault(exit_time, [])
            bucket.append(net_bps)
            last_portfolio_slot = (exit_time, len(bucket) - 1)
            pooled_observations += 1
            position = target
        if values and position > 0.0:
            liquidation_cost = position * costs
            values[-1] -= liquidation_cost
            if last_portfolio_slot is not None:
                bucket_time, bucket_index = last_portfolio_slot
                portfolio_by_time[bucket_time][bucket_index] -= liquidation_cost
            turnover += position
            position = 0.0
        if not values:
            continue
        per_symbol.append(
            {
                "symbol": str(symbol),
                **_metrics(values),
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
    data_sufficient = len(portfolio_returns) >= max(1, int(minimum_samples))
    return {
        **_metrics(portfolio_returns),
        "symbols": len(per_symbol),
        "pooled_symbol_observations": pooled_observations,
        "portfolio_aggregation": "equal_weight_mean_by_exit_timestamp",
        "minimum_samples": max(1, int(minimum_samples)),
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
