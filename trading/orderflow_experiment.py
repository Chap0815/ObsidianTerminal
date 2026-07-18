"""Causal quarter-hour order-flow feature construction."""
from __future__ import annotations

import math
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


@dataclass(frozen=True)
class OrderFlowWindow:
    feature_cutoff: datetime
    earliest_entry: datetime
    book_ofi: float
    trade_imbalance: float
    normalized_ofi: float
    mean_top_depth: float
    book_events: int
    trade_events: int
    continuous_book: bool
    promotable: bool
    quality_flags: tuple[str, ...]


def _aware_event_time(observation: dict) -> datetime:
    event_time = observation.get("event_time")
    if not isinstance(event_time, datetime) or event_time.tzinfo is None:
        raise ValueError("order-flow event time must be timezone-aware")
    return event_time.astimezone(timezone.utc)


def _positive(value, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be positive")
    number = float(value)
    if not math.isfinite(number) or number <= 0.0:
        raise ValueError(f"{name} must be positive")
    return number


def build_ofi_window(
    *,
    exchange_time: datetime,
    book_observations: list[dict],
    trade_observations: list[dict],
    observation_seconds: int = 30,
    sequence_valid: bool = True,
    continuous_book: bool = False,
    max_staleness_seconds: float = 5.0,
) -> OrderFlowWindow:
    """Build depth-normalized OFI; sampled REST books remain research-only."""
    if not sequence_valid:
        raise ValueError("order-flow window contains a sequence gap")
    if exchange_time.tzinfo is None:
        raise ValueError("exchange event time must be timezone-aware")
    start = exchange_time.astimezone(timezone.utc)
    cutoff = start + timedelta(seconds=max(1, int(observation_seconds)))
    books = sorted(
        (
            observation
            for observation in book_observations
            if start <= _aware_event_time(observation) < cutoff
        ),
        key=_aware_event_time,
    )
    if len(books) < 2:
        raise ValueError("OFI requires at least two book observations")
    if (cutoff - _aware_event_time(books[-1])).total_seconds() > float(
        max_staleness_seconds
    ):
        raise ValueError("order-flow window is stale at feature cutoff")
    book_ofi = 0.0
    depths = []
    previous = None
    for observation in books:
        bid_price = _positive(observation.get("bid_price"), "bid price")
        ask_price = _positive(observation.get("ask_price"), "ask price")
        bid_size = _positive(observation.get("bid_size"), "bid size")
        ask_size = _positive(observation.get("ask_size"), "ask size")
        if bid_price > ask_price:
            raise ValueError("crossed order book in OFI window")
        depths.append(bid_size + ask_size)
        if previous is not None:
            previous_bid, previous_bid_size, previous_ask, previous_ask_size = previous
            if bid_price >= previous_bid:
                book_ofi += bid_size
            if bid_price <= previous_bid:
                book_ofi -= previous_bid_size
            if ask_price <= previous_ask:
                book_ofi -= ask_size
            if ask_price >= previous_ask:
                book_ofi += previous_ask_size
        previous = (bid_price, bid_size, ask_price, ask_size)
    buy = 0.0
    sell = 0.0
    trade_events = 0
    for observation in trade_observations:
        event_time = _aware_event_time(observation)
        if event_time < start or event_time >= cutoff:
            continue
        amount = max(0.0, float(observation.get("amount", 0.0)))
        side = str(observation.get("side", "")).lower()
        if side == "buy":
            buy += amount
            trade_events += 1
        elif side == "sell":
            sell += amount
            trade_events += 1
    trade_total = buy + sell
    trade_imbalance = (buy - sell) / trade_total if trade_total else 0.0
    mean_depth = sum(depths) / len(depths)
    normalized_ofi = book_ofi / mean_depth if mean_depth > 0.0 else 0.0
    flags = () if continuous_book else ("snapshot_approximation",)
    return OrderFlowWindow(
        cutoff,
        cutoff + timedelta(microseconds=1),
        book_ofi,
        trade_imbalance,
        normalized_ofi,
        mean_depth,
        len(books),
        trade_events,
        bool(continuous_book),
        bool(continuous_book and sequence_valid),
        flags,
    )


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
