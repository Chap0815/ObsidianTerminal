"""Causal quarter-hour order-flow feature construction."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class ClockWindow:
    phase_minute: int
    feature_cutoff: datetime
    earliest_entry: datetime
    buy_amount: float
    sell_amount: float
    imbalance: float


def build_clock_window(
    *,
    exchange_time: datetime,
    observations: list[dict],
    observation_seconds: int = 30,
    sequence_valid: bool = True,
) -> ClockWindow:
    if not sequence_valid:
        raise ValueError("order-flow window contains a sequence gap")
    if exchange_time.tzinfo is None:
        raise ValueError("exchange event time must be timezone-aware")
    start = exchange_time.astimezone(timezone.utc)
    if start.minute % 15 != 0 or start.second or start.microsecond:
        raise ValueError("window must start exactly on an exchange UTC quarter-hour")
    cutoff = start + timedelta(seconds=max(1, int(observation_seconds)))
    buy = 0.0
    sell = 0.0
    for observation in observations:
        event_time = observation.get("event_time")
        if not isinstance(event_time, datetime) or event_time.tzinfo is None:
            raise ValueError("observation event time must be timezone-aware")
        event_time = event_time.astimezone(timezone.utc)
        if event_time < start or event_time >= cutoff:
            continue
        amount = max(0.0, float(observation.get("amount", 0.0)))
        if str(observation.get("side", "")).lower() == "buy":
            buy += amount
        elif str(observation.get("side", "")).lower() == "sell":
            sell += amount
    total = buy + sell
    imbalance = (buy - sell) / total if total else 0.0
    return ClockWindow(
        phase_minute=start.minute,
        feature_cutoff=cutoff,
        earliest_entry=cutoff + timedelta(microseconds=1),
        buy_amount=buy,
        sell_amount=sell,
        imbalance=imbalance,
    )


def label_fixed_horizon(
    *,
    window: ClockWindow,
    side: str,
    entry_time: datetime,
    entry_price: float,
    exit_time: datetime,
    exit_price: float,
    total_cost_bps: float,
) -> float:
    """Return a causal virtual-entry net label in basis points."""
    if entry_time <= window.feature_cutoff or entry_time < window.earliest_entry:
        raise ValueError("entry must be strictly after the feature cutoff")
    if exit_time <= entry_time:
        raise ValueError("label horizon must end after entry")
    entry = float(entry_price)
    exit_value = float(exit_price)
    if entry <= 0.0 or exit_value <= 0.0:
        raise ValueError("entry and exit prices must be positive")
    sign = -1.0 if str(side).lower() == "short" else 1.0
    gross_bps = sign * (exit_value - entry) / entry * 10_000.0
    return gross_bps - max(0.0, float(total_cost_bps))
