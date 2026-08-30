"""Causal, immutable MEXC overview and funding evidence for research replay.

The live venue recorder stores one all-market overview per observation.  This
module turns copied, immutable partitions into deterministic replay snapshots
and exact per-settlement funding charges.  It never opens a writable database
connection and never reads an observation before its ``received_time``.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping


MAX_OVERVIEW_PAYLOAD_BYTES = 8 * 1024 * 1024
MAX_FUNDING_ENDPOINT_AGE_SECONDS = 300
MAX_FUNDING_RATE_AGE_SECONDS = 300


def _utc(value) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number) or number <= 0.0:
            return None
        # MEXC nextSettleTime is epoch milliseconds.
        if number > 10_000_000_000:
            number /= 1000.0
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _finite(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _canonical_symbol(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    base = text.split("/", 1)[0] if "/" in text else text.split("_", 1)[0]
    return f"{base}/USDT:USDT" if base else ""


@dataclass(frozen=True)
class HistoricalMarket:
    market_id: str
    symbol: str
    last: float
    bid: float | None
    ask: float | None
    quote_volume: float
    funding_rate: float | None
    next_settlement: datetime | None


@dataclass(frozen=True)
class OverviewSnapshot:
    event_id: str
    exchange_time: datetime
    received_time: datetime
    markets: Mapping[str, HistoricalMarket]
    quality_flags: tuple[str, ...]


@dataclass(frozen=True)
class FundingPeriod:
    symbol: str
    settlement_time: datetime
    rate: float
    observed_time: datetime
    event_id: str
    mark_price: float | None = None


@dataclass(frozen=True)
class FundingCharge:
    cost_usdt: float
    periods: int
    rates: tuple[float, ...]


class IncompleteFundingEvidence(ValueError):
    """Raised when a held position crosses a period absent from evidence."""


class HistoricalFundingTimeline:
    """Newest causal funding observation for each symbol/settlement pair."""

    def __init__(
        self,
        periods: Iterable[FundingPeriod],
        *,
        observation_times: Mapping[str, Iterable[datetime]] | None = None,
        settled_history: bool = False,
    ) -> None:
        grouped: dict[str, dict[datetime, FundingPeriod]] = {}
        for period in periods:
            symbol = _canonical_symbol(period.symbol)
            if not symbol:
                continue
            by_settlement = grouped.setdefault(symbol, {})
            previous = by_settlement.get(period.settlement_time)
            ordering = (period.observed_time, period.event_id)
            if previous is None or ordering > (
                previous.observed_time,
                previous.event_id,
            ):
                by_settlement[period.settlement_time] = period
        self._periods = {
            symbol: tuple(sorted(rows.values(), key=lambda row: row.settlement_time))
            for symbol, rows in grouped.items()
        }
        self._observations = {
            _canonical_symbol(symbol): tuple(sorted(set(times)))
            for symbol, times in (observation_times or {}).items()
            if _canonical_symbol(symbol)
        }
        self._settled_history = settled_history is True

    @classmethod
    def from_snapshots(
        cls,
        snapshots: Iterable[OverviewSnapshot],
        *,
        completed_as_of: datetime | None = None,
    ) -> "HistoricalFundingTimeline":
        cutoff = completed_as_of
        if cutoff is not None:
            cutoff = _require_aware(cutoff, "completed_as_of")
        periods = []
        observations: dict[str, list[datetime]] = {}
        for snapshot in snapshots:
            for market in snapshot.markets.values():
                observations.setdefault(market.symbol, []).append(
                    snapshot.received_time
                )
                settlement = market.next_settlement
                rate = market.funding_rate
                if settlement is None or rate is None:
                    continue
                if snapshot.received_time > settlement:
                    continue
                if cutoff is not None and settlement > cutoff:
                    continue
                periods.append(FundingPeriod(
                    symbol=market.symbol,
                    settlement_time=settlement,
                    rate=rate,
                    observed_time=snapshot.received_time,
                    event_id=snapshot.event_id,
                ))
        return cls(periods, observation_times=observations)

    @classmethod
    def from_settled_history(
        cls,
        history: Mapping[
            str,
            Iterable[
                tuple[datetime, float] | tuple[datetime, float, float]
            ],
        ],
    ) -> "HistoricalFundingTimeline":
        periods = []
        observations = {}
        settled_values: dict[
            tuple[str, datetime], tuple[float, float | None]
        ] = {}
        for symbol, rows in history.items():
            canonical = _canonical_symbol(symbol)
            if not canonical:
                continue
            stamps = []
            for index, raw_row in enumerate(rows):
                try:
                    timestamp, raw_rate, *optional_mark = raw_row
                except (TypeError, ValueError) as exc:
                    raise ValueError("settled funding row is invalid") from exc
                if len(optional_mark) > 1:
                    raise ValueError("settled funding row is invalid")
                settlement = _as_utc_datetime(timestamp, "funding settlement")
                rate = _finite(raw_rate)
                if rate is None:
                    raise ValueError("settled funding rate must be finite")
                mark_price = (
                    _finite(optional_mark[0]) if optional_mark else None
                )
                if mark_price is not None and mark_price <= 0.0:
                    raise ValueError("settled funding mark must be positive")
                key = (canonical, settlement)
                previous_value = settled_values.get(key)
                if previous_value is not None and previous_value != (
                    rate,
                    mark_price,
                ):
                    raise ValueError(
                        "conflicting settled funding rates or marks for "
                        f"{canonical} at {settlement.isoformat()}"
                    )
                settled_values[key] = (rate, mark_price)
                stamps.append(settlement)
                periods.append(FundingPeriod(
                    symbol=canonical,
                    settlement_time=settlement,
                    rate=rate,
                    observed_time=settlement,
                    event_id=f"settled:{canonical}:{index}:{settlement.isoformat()}",
                    mark_price=mark_price,
                ))
            observations[canonical] = stamps
        return cls(
            periods,
            observation_times=observations,
            settled_history=True,
        )

    def periods(self, symbol: str) -> tuple[FundingPeriod, ...]:
        return self._periods.get(_canonical_symbol(symbol), ())

    def has_complete_coverage(
        self,
        symbol: str,
        entry_time,
        exit_time,
    ) -> bool:
        entry = _as_utc_datetime(entry_time, "entry_time")
        exit_ = _as_utc_datetime(exit_time, "exit_time")
        observations = self._observations.get(_canonical_symbol(symbol), ())
        if self._settled_history:
            return _settled_coverage_is_complete(observations, entry, exit_)
        return _coverage_is_complete(observations, entry, exit_)

    def summary(self, start_time, end_time) -> dict:
        start = _as_utc_datetime(start_time, "start_time")
        end = _as_utc_datetime(end_time, "end_time")
        rows = [
            row
            for periods in self._periods.values()
            for row in periods
            if start < row.settlement_time <= end
        ]
        rates = [row.rate for row in rows]
        return {
            "symbols": len({row.symbol for row in rows}),
            "settlement_periods": len(rows),
            "positive_periods": sum(rate > 0.0 for rate in rates),
            "negative_periods": sum(rate < 0.0 for rate in rates),
            "zero_periods": sum(rate == 0.0 for rate in rates),
            "mean_rate": math.fsum(rates) / len(rates) if rates else None,
            "minimum_rate": min(rates) if rates else None,
            "maximum_rate": max(rates) if rates else None,
            "first_settlement": min(
                (row.settlement_time for row in rows), default=None
            ),
            "last_settlement": max(
                (row.settlement_time for row in rows), default=None
            ),
        }

    def charge(
        self,
        symbol: str,
        side: str,
        notional_usdt: float,
        entry_time,
        exit_time,
        *,
        require_complete: bool = True,
        base_amount: float | None = None,
        mark_price_resolver: Callable[[datetime], float | None] | None = None,
    ) -> FundingCharge:
        entry = _as_utc_datetime(entry_time, "entry_time")
        exit_ = _as_utc_datetime(exit_time, "exit_time")
        notional = _finite(notional_usdt)
        if notional is None or notional < 0.0:
            raise ValueError("notional_usdt must be finite and non-negative")
        direction = str(side).upper()
        if direction not in {"LONG", "SHORT"}:
            raise ValueError("side must be LONG or SHORT")
        if exit_ <= entry or notional == 0.0:
            return FundingCharge(0.0, 0, ())

        observations = self._observations.get(_canonical_symbol(symbol), ())
        coverage_complete = (
            _settled_coverage_is_complete(observations, entry, exit_)
            if self._settled_history
            else _coverage_is_complete(observations, entry, exit_)
        )
        if require_complete and not coverage_complete:
            raise IncompleteFundingEvidence(
                f"incomplete funding capture coverage for "
                f"{_canonical_symbol(symbol)} in ({entry.isoformat()}, "
                f"{exit_.isoformat()}]"
            )
        rows = [
            row for row in self.periods(symbol)
            if entry < row.settlement_time <= exit_
        ]
        stale = [
            row for row in rows
            if (row.settlement_time - row.observed_time).total_seconds()
            > MAX_FUNDING_RATE_AGE_SECONDS
        ]
        if require_complete and stale and not self._settled_history:
            raise IncompleteFundingEvidence(
                f"stale settlement funding for {_canonical_symbol(symbol)} at "
                f"{stale[0].settlement_time.isoformat()}"
            )
        rates = tuple(row.rate for row in rows)
        if not rows:
            return FundingCharge(0.0, 0, rates)
        amount = _finite(base_amount)
        if amount is None or amount <= 0.0:
            raise IncompleteFundingEvidence(
                "base quantity is required for settlement-mark funding"
            )
        signed_costs = []
        for row in rows:
            mark = _finite(row.mark_price)
            if mark is None and mark_price_resolver is not None:
                try:
                    mark = _finite(mark_price_resolver(row.settlement_time))
                except Exception as exc:
                    raise IncompleteFundingEvidence(
                        "settlement mark funding evidence is unavailable"
                    ) from exc
            if mark is None or mark <= 0.0:
                raise IncompleteFundingEvidence(
                    "settlement mark funding evidence is unavailable"
                )
            signed_costs.append(amount * mark * row.rate)
        signed = math.fsum(signed_costs)
        if direction == "SHORT":
            signed = -signed
        return FundingCharge(signed, len(rows), rates)


def _coverage_is_complete(
    observations: tuple[datetime, ...],
    entry: datetime,
    exit_: datetime,
) -> bool:
    if not observations:
        return False
    left = bisect_left(observations, entry)
    start_index = max(0, left - 1)
    right = bisect_right(observations, exit_)
    relevant = observations[start_index:right]
    if not relevant:
        return False
    if relevant[0] > entry or relevant[-1] > exit_:
        return False
    if (
        (entry - relevant[0]).total_seconds()
        > MAX_FUNDING_ENDPOINT_AGE_SECONDS
        or (exit_ - relevant[-1]).total_seconds()
        > MAX_FUNDING_ENDPOINT_AGE_SECONDS
    ):
        return False
    # Interior recorder gaps do not imply missing funding: each upcoming
    # settlement is announced for hours and is validated independently below
    # by the age of its newest pre-settlement rate.  Requiring uninterrupted
    # minute observations here would discard every symbol for a venue-wide
    # capture pause that occurred far away from a settlement.
    return True


def _settled_coverage_is_complete(
    settlements: tuple[datetime, ...],
    entry: datetime,
    exit_: datetime,
) -> bool:
    if len(settlements) < 2:
        return False
    gaps = [
        (right - left).total_seconds()
        for left, right in zip(settlements, settlements[1:])
    ]
    if not gaps or any(gap <= 0.0 or gap > 12 * 3600 for gap in gaps):
        return False
    maximum_interval = max(gaps)
    return bool(
        settlements[0] <= entry
        and settlements[-1] >= exit_ - timedelta(seconds=maximum_interval)
    )


def _require_aware(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _as_utc_datetime(value, label: str) -> datetime:
    if isinstance(value, datetime):
        return _require_aware(value, label)
    timestamp = getattr(value, "to_pydatetime", None)
    if callable(timestamp):
        return _require_aware(timestamp(), label)
    parsed = _utc(value)
    if parsed is None:
        raise ValueError(f"{label} must be a UTC timestamp")
    return parsed


def overview_partition_fingerprint(paths: Iterable[str | Path]) -> str:
    """Hash ordered partition bytes, including relative-independent names."""
    digest = hashlib.sha256()
    normalized = sorted(Path(path).resolve() for path in paths)
    if not normalized:
        raise ValueError("at least one overview partition is required")
    for path in normalized:
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"overview partition is missing or linked: {path}")
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def load_overview_snapshots(
    source: str | Path | Iterable[str | Path],
    *,
    reject_quality_flags: Iterable[str] = (
        "invalid_tickers_payload",
        "invalid_exchange_timestamp",
        "stale_exchange_timestamp",
    ),
) -> list[OverviewSnapshot]:
    """Read copied overview SQLite partitions deterministically and read-only."""
    paths = _partition_paths(source)
    rejected = set(reject_quality_flags)
    by_event: dict[str, OverviewSnapshot] = {}
    for path in paths:
        connection = None
        try:
            uri = f"file:{path.as_posix()}?mode=ro&immutable=1"
            connection = sqlite3.connect(uri, uri=True)
            rows = connection.execute(
                "SELECT event_id,exchange_time,received_time,"
                "quality_flags_json,payload_json FROM venue_events "
                "WHERE market_id='ALL_USDT_SWAPS' "
                "AND length(CAST(payload_json AS BLOB))<=? "
                "ORDER BY received_time,event_id",
                (MAX_OVERVIEW_PAYLOAD_BYTES,),
            )
            for event_id, exchange_raw, received_raw, flags_raw, payload_raw in rows:
                exchange_time = _utc(exchange_raw)
                received_time = _utc(received_raw)
                if exchange_time is None or received_time is None:
                    continue
                try:
                    flags_value = json.loads(flags_raw)
                    payload = json.loads(payload_raw)
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        f"invalid overview event JSON in {path}: {event_id}"
                    ) from exc
                if not isinstance(flags_value, list) or not all(
                    isinstance(flag, str) for flag in flags_value
                ):
                    continue
                flags = tuple(flags_value)
                if rejected.intersection(flags):
                    continue
                raw_markets = payload.get("markets") if isinstance(payload, dict) else None
                if not isinstance(raw_markets, dict):
                    continue
                markets = {}
                for market_id, raw in raw_markets.items():
                    market = _market(str(market_id), raw)
                    if market is not None:
                        markets[market.symbol] = market
                if not markets:
                    continue
                snapshot = OverviewSnapshot(
                    event_id=str(event_id),
                    exchange_time=exchange_time,
                    received_time=received_time,
                    markets=markets,
                    quality_flags=flags,
                )
                previous = by_event.get(snapshot.event_id)
                if previous is not None and previous != snapshot:
                    raise ValueError(
                        f"conflicting overview event id: {snapshot.event_id}"
                    )
                by_event[snapshot.event_id] = snapshot
        except sqlite3.Error as exc:
            raise ValueError(f"invalid overview partition: {path}") from exc
        finally:
            if connection is not None:
                connection.close()
    return sorted(
        by_event.values(),
        key=lambda row: (row.received_time, row.exchange_time, row.event_id),
    )


def _market(market_id: str, raw) -> HistoricalMarket | None:
    if not isinstance(raw, dict):
        return None
    symbol = _canonical_symbol(raw.get("symbol") or market_id)
    last = _finite(raw.get("last"))
    quote_volume = _finite(raw.get("quote_volume"))
    if not symbol or last is None or last <= 0.0:
        return None
    if quote_volume is None or quote_volume < 0.0:
        return None
    bid = _finite(raw.get("bid"))
    ask = _finite(raw.get("ask"))
    if bid is not None and bid <= 0.0:
        bid = None
    if ask is not None and ask <= 0.0:
        ask = None
    if bid is not None and ask is not None and bid > ask:
        bid = ask = None
    return HistoricalMarket(
        market_id=market_id,
        symbol=symbol,
        last=last,
        bid=bid,
        ask=ask,
        quote_volume=quote_volume,
        funding_rate=_finite(raw.get("funding_rate")),
        next_settlement=_utc(raw.get("next_settle_time")),
    )


def _partition_paths(source) -> list[Path]:
    if isinstance(source, (str, Path)):
        root = Path(source).expanduser().resolve()
        paths = sorted(root.glob("*.sqlite3")) if root.is_dir() else [root]
    else:
        paths = sorted(Path(path).expanduser().resolve() for path in source)
    if not paths:
        raise ValueError("no overview SQLite partitions found")
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"overview partition is missing or linked: {path}")
    return paths
