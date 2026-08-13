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
    return _aware_datetime(event_time, "order-flow event time")


def _aware_datetime(value, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    try:
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{name} must be timezone-aware") from exc


def _positive(value, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be positive")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be positive") from exc
    if not math.isfinite(number) or number <= 0.0:
        raise ValueError(f"{name} must be positive")
    return number


def _positive_integral(value, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if (
        not math.isfinite(number)
        or number <= 0.0
        or not number.is_integer()
    ):
        raise ValueError(f"{name} must be a positive integer")
    return int(number)


def _cutoff(start: datetime, observation_seconds) -> datetime:
    seconds = _positive_integral(
        observation_seconds, "observation seconds"
    )
    try:
        return start + timedelta(seconds=seconds)
    except OverflowError as exc:
        raise ValueError(
            "observation seconds exceed the datetime range"
        ) from exc


def _require_finite_derived(*values: float) -> None:
    if not all(math.isfinite(value) for value in values):
        raise ValueError("order-flow derived values must be finite")


def _trade_side(value) -> str:
    if not isinstance(value, str):
        raise ValueError("trade side must be buy or sell")
    side = value.strip().lower()
    if side not in {"buy", "sell"}:
        raise ValueError("trade side must be buy or sell")
    return side


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
    if sequence_valid is not True:
        raise ValueError("order-flow window contains a sequence gap")
    if not isinstance(continuous_book, bool):
        raise ValueError("continuous book provenance must be boolean")
    if exchange_time.tzinfo is None:
        raise ValueError("exchange event time must be timezone-aware")
    start = exchange_time.astimezone(timezone.utc)
    cutoff = _cutoff(start, observation_seconds)
    staleness_limit = _positive(
        max_staleness_seconds, "max staleness seconds"
    )
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
    if (
        cutoff - _aware_event_time(books[-1])
    ).total_seconds() > staleness_limit:
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
        depth = bid_size + ask_size
        _require_finite_derived(depth)
        depths.append(depth)
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
            _require_finite_derived(book_ofi)
        previous = (bid_price, bid_size, ask_price, ask_size)
    buy = 0.0
    sell = 0.0
    trade_events = 0
    for observation in trade_observations:
        event_time = _aware_event_time(observation)
        if event_time < start or event_time >= cutoff:
            continue
        side = _trade_side(observation.get("side"))
        amount = _positive(observation.get("amount"), "trade amount")
        if side == "buy":
            buy += amount
            _require_finite_derived(buy)
            trade_events += 1
        elif side == "sell":
            sell += amount
            _require_finite_derived(sell)
            trade_events += 1
    trade_total = buy + sell
    _require_finite_derived(book_ofi, buy, sell, trade_total)
    trade_imbalance = (buy - sell) / trade_total if trade_total else 0.0
    try:
        mean_depth = math.fsum(depths) / len(depths)
    except OverflowError as exc:
        raise ValueError("order-flow derived values must be finite") from exc
    normalized_ofi = book_ofi / mean_depth if mean_depth > 0.0 else 0.0
    _require_finite_derived(trade_imbalance, mean_depth, normalized_ofi)
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
        continuous_book,
        continuous_book,
        flags,
    )


def build_clock_window(
    *,
    exchange_time: datetime,
    observations: list[dict],
    observation_seconds: int = 30,
    sequence_valid: bool = True,
) -> ClockWindow:
    if sequence_valid is not True:
        raise ValueError("order-flow window contains a sequence gap")
    if exchange_time.tzinfo is None:
        raise ValueError("exchange event time must be timezone-aware")
    start = exchange_time.astimezone(timezone.utc)
    if start.minute % 15 != 0 or start.second or start.microsecond:
        raise ValueError("window must start exactly on an exchange UTC quarter-hour")
    cutoff = _cutoff(start, observation_seconds)
    buy = 0.0
    sell = 0.0
    for observation in observations:
        event_time = observation.get("event_time")
        if not isinstance(event_time, datetime) or event_time.tzinfo is None:
            raise ValueError("observation event time must be timezone-aware")
        event_time = event_time.astimezone(timezone.utc)
        if event_time < start or event_time >= cutoff:
            continue
        side = _trade_side(observation.get("side"))
        amount = _positive(observation.get("amount"), "trade amount")
        if side == "buy":
            buy += amount
            _require_finite_derived(buy)
        else:
            sell += amount
            _require_finite_derived(sell)
    total = buy + sell
    _require_finite_derived(buy, sell, total)
    imbalance = (buy - sell) / total if total else 0.0
    _require_finite_derived(imbalance)
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
    if not isinstance(window, ClockWindow):
        raise ValueError("label window must be a ClockWindow")
    feature_cutoff = _aware_datetime(
        window.feature_cutoff, "feature cutoff"
    )
    earliest_entry = _aware_datetime(
        window.earliest_entry, "earliest entry"
    )
    entry_timestamp = _aware_datetime(entry_time, "entry time")
    exit_timestamp = _aware_datetime(exit_time, "exit time")
    if earliest_entry <= feature_cutoff:
        raise ValueError("earliest entry must follow the feature cutoff")
    if (
        entry_timestamp <= feature_cutoff
        or entry_timestamp < earliest_entry
    ):
        raise ValueError("entry must be strictly after the feature cutoff")
    if exit_timestamp <= entry_timestamp:
        raise ValueError("label horizon must end after entry")
    if any(
        isinstance(value, bool)
        for value in (entry_price, exit_price, total_cost_bps)
    ):
        raise ValueError("label prices and costs must be finite numbers")
    try:
        entry = float(entry_price)
        exit_value = float(exit_price)
        cost_bps = float(total_cost_bps)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "label prices and costs must be finite numbers"
        ) from exc
    if not all(math.isfinite(value) for value in (entry, exit_value, cost_bps)):
        raise ValueError("label prices and costs must be finite numbers")
    if entry <= 0.0 or exit_value <= 0.0:
        raise ValueError("entry and exit prices must be positive")
    normalized_side = str(side).strip().lower()
    if normalized_side in {"short", "sell"}:
        sign = -1.0
    elif normalized_side in {"long", "buy"}:
        sign = 1.0
    else:
        raise ValueError("label side must be long/buy or short/sell")
    gross_bps = sign * (exit_value - entry) / entry * 10_000.0
    net_bps = gross_bps - max(0.0, cost_bps)
    _require_finite_derived(gross_bps, net_bps)
    return net_bps
