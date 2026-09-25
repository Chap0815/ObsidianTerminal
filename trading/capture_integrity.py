"""Bounded, fail-closed acceptance contract for closed venue-capture days."""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import uuid
from collections import defaultdict
from contextlib import nullcontext
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path

from trading.capture_trade_identity import (
    read_trade_identity,
    validate_trade_identity_schema,
    validate_ws_trade_payload,
)


_DATETIME_TYPE = datetime
CORE_STREAMS = ("overview", "depth", "trades", "l2_stream")
EVENT_COLUMNS = (
    "event_id",
    "market_id",
    "exchange_time",
    "received_time",
    "schema_version",
    "quality_flags_json",
    "payload_json",
)
EVENT_SCHEMA = (
    ("event_id", "TEXT", 1, 1),
    ("market_id", "TEXT", 1, 0),
    ("exchange_time", "TEXT", 1, 0),
    ("received_time", "TEXT", 1, 0),
    ("schema_version", "INTEGER", 1, 0),
    ("quality_flags_json", "TEXT", 1, 0),
    ("payload_json", "TEXT", 1, 0),
)
EVENT_TIME_INDEX_XINFO = (
    ("exchange_time", 0, "BINARY", 1),
    ("market_id", 0, "BINARY", 1),
    ("event_id", 0, "BINARY", 0),
)
PARTITION_SCHEMA_OBJECTS = (
    ("index", "idx_venue_events_time", "venue_events"),
    ("table", "venue_events", "venue_events"),
)
TRADE_IDENTITY_SCHEMA_OBJECT = (
    "table", "websocket_trade_identities", "websocket_trade_identities"
)
SEAL_GRACE = timedelta(hours=1)
GAP_EXCLUSION_BUFFER = timedelta(minutes=2)
DAY_MAX_SINGLE_OUTAGE = timedelta(minutes=45)
DAY_MAX_TOTAL_OUTAGE = timedelta(hours=1)
DAY_MAX_OUTAGE_EPISODES = 6
CONTINUITY_WINDOW_DAYS = 30
CONTINUITY_MAX_DEGRADED_RATIO = 0.20
INTEGRITY_REPORT_MAX_BYTES = 4 * 1024 * 1024
EVENT_PAYLOAD_JSON_MAX_BYTES = 16 * 1024 * 1024
EVENT_QUALITY_FLAGS_JSON_MAX_BYTES = 64 * 1024
REST_RECEIPT_CLOCK_TOLERANCE_MS = 30_000
WEBSOCKET_TRADE_EVENT_MAX_ROWS = 20_000
WEBSOCKET_TRADE_FUTURE_TOLERANCE_MS = 30_000
WEBSOCKET_TRADE_MAX_AGE_MS = 86_400_000
L2_BASE_QUALITY_FLAGS = ("sequence_unverified",)
L2_STALE_QUALITY_FLAGS = (
    *L2_BASE_QUALITY_FLAGS,
    "stale_exchange_timestamp",
)
L2_ALLOWED_QUALITY_FLAGS = frozenset(L2_STALE_QUALITY_FLAGS)
L2_COVERAGE_EXCLUDED_FLAGS = frozenset({"stale_exchange_timestamp"})
L2_STALE_WARNING = "l2_stale_exchange_timestamp_excluded"
UNIVERSE_CONTRACT = "persisted_point_in_time_dynamic"
SEQUENCE_CONTRACT = "l2_sequence_unverified_snapshot_only"


def _capture_now_utc() -> datetime:
    """Exchange-anchored time for capture validation and sealing boundaries."""
    from core.clock import now_utc

    observed = now_utc()
    try:
        utc_offset = observed.utcoffset()
    except (AttributeError, OverflowError, ValueError) as exc:
        raise RuntimeError("capture clock is not UTC-aware") from exc
    if not isinstance(observed, _DATETIME_TYPE) or utc_offset != timedelta(0):
        raise RuntimeError("capture clock is not UTC-aware")
    return observed


def _capture_policy_gaps(
    *,
    max_symbols: int,
    micro_interval_seconds: float,
    l2_sample_interval_seconds: float,
) -> tuple[timedelta, timedelta]:
    if type(max_symbols) is not int or max_symbols < 1:
        raise ValueError("max_symbols must be a positive integer")
    intervals = []
    for label, value in (
        ("micro", micro_interval_seconds),
        ("l2 sample", l2_sample_interval_seconds),
    ):
        if type(value) not in {int, float}:
            raise ValueError(f"{label} interval must be finite and positive")
        number = float(value)
        if not math.isfinite(number) or number <= 0.0:
            raise ValueError(f"{label} interval must be finite and positive")
        intervals.append(number)
    try:
        rest_seconds = max(30.0, intervals[0] * max_symbols * 2.5)
        l2_seconds = max(10.0, intervals[1] * 10.0)
        return (
            timedelta(seconds=rest_seconds),
            timedelta(seconds=l2_seconds),
        )
    except OverflowError as exc:
        raise ValueError("capture policy interval is out of range") from exc


def _reject_constant(value: str):
    raise ValueError(f"invalid JSON constant {value}")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key}")
        result[key] = value
    return result


def _json(value: str):
    return json.loads(
        value,
        parse_constant=_reject_constant,
        object_pairs_hook=_unique_object,
    )


def _bounded_json(value, *, max_bytes: int, label: str):
    if not isinstance(value, str):
        raise ValueError(f"{label} is invalid")
    if len(value) > max_bytes:
        raise ValueError(f"{label} exceeds size limit")
    try:
        encoded_size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise ValueError(f"{label} is invalid") from exc
    if encoded_size > max_bytes:
        raise ValueError(f"{label} exceeds size limit")
    return _json(value)


def _utc(value) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp is missing")
    parsed = datetime.fromisoformat(
        value[:-1] + "+00:00" if value.endswith("Z") else value
    )
    if parsed.tzinfo is None:
        raise ValueError("timestamp is naive")
    try:
        return parsed.astimezone(timezone.utc)
    except OverflowError as exc:
        raise ValueError("timestamp is outside UTC range") from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest().upper()


def _manifest_file(path: Path, relative_path: str, **metadata) -> dict:
    """Hash-bind one stable capture artifact, including invalid partitions."""
    before = path.stat()
    digest = _sha256(path)
    after = path.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
    ):
        raise RuntimeError("capture artifact changed during manifest hashing")
    return {
        "path": relative_path,
        "bytes": after.st_size,
        "sha256": digest,
        **metadata,
    }


def _raise_partition_drift(day: str, path: str, reason: str, exc=None) -> None:
    """Fail closed while retaining the exact immutable artifact diagnosis."""
    message = (
        "sealed capture partition drifted: "
        f"day={day} path={path} reason={reason}"
    )
    if exc is not None:
        raise RuntimeError(message) from exc
    raise RuntimeError(message)


def _linklike(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = path.stat(follow_symlinks=False).st_file_attributes
    except (AttributeError, OSError):
        return False
    return bool(attributes & 0x400)


def _path_has_links(path: Path) -> bool:
    return any(_linklike(component) for component in (path, *path.parents))


def _file_signature(stat_result) -> tuple:
    return (
        stat_result.st_size,
        stat_result.st_mtime_ns,
        stat_result.st_ctime_ns,
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_nlink,
    )


def _path_identity(stat_result) -> tuple:
    # Windows can expose a different ctime for the open handle and its path
    # even when device/inode and all content-bearing metadata are identical.
    return (
        stat_result.st_size,
        stat_result.st_mtime_ns,
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_nlink,
    )


def _read_integrity_report_bytes(
    path: Path,
    *,
    max_bytes: int = INTEGRITY_REPORT_MAX_BYTES,
) -> bytes:
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError("capture integrity report size limit is invalid")
    if _path_has_links(path):
        raise RuntimeError("capture integrity report path is linked")
    with path.open("rb") as handle:
        before = os.fstat(handle.fileno())
        if before.st_nlink != 1:
            raise RuntimeError("capture integrity report is hard-linked")
        encoded = handle.read(max_bytes + 1)
        after = os.fstat(handle.fileno())
    if _file_signature(after) != _file_signature(before):
        raise RuntimeError("capture integrity report changed during read")
    if _path_has_links(path):
        raise RuntimeError("capture integrity report path is linked")
    current = path.stat(follow_symlinks=False)
    if current.st_nlink != 1:
        raise RuntimeError("capture integrity report is hard-linked")
    if _path_identity(current) != _path_identity(after):
        raise RuntimeError("capture integrity report changed during read")
    if len(encoded) > max_bytes:
        raise ValueError("capture integrity report exceeds size limit")
    return encoded


def _read_integrity_report(
    path: Path,
    *,
    max_bytes: int = INTEGRITY_REPORT_MAX_BYTES,
) -> dict:
    try:
        text = _read_integrity_report_bytes(
            path,
            max_bytes=max_bytes,
        ).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("capture integrity report is not valid UTF-8") from exc
    return _json(text)


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, datetime_time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _gap_record(
    stream: str,
    market: str,
    reason: str,
    started: datetime,
    ended: datetime,
) -> dict:
    return {
        "stream": stream,
        "market": market,
        "reason": reason,
        "start": started,
        "end": ended,
    }


def _merge_gap_windows(
    gaps: list[dict],
    day: date,
    *,
    buffer: timedelta,
) -> list[dict]:
    day_start, day_end = _day_bounds(day)
    windows = []
    for gap in gaps:
        started = max(day_start, gap["start"] - buffer)
        ended = min(day_end, gap["end"] + buffer)
        if ended <= started:
            continue
        windows.append({
            "start": started,
            "end": ended,
            "streams": {gap["stream"]},
            "markets": {gap["market"]},
            "reasons": {gap["reason"]},
        })
    windows.sort(key=lambda item: (item["start"], item["end"]))
    merged = []
    for window in windows:
        if merged and window["start"] <= merged[-1]["end"]:
            current = merged[-1]
            current["end"] = max(current["end"], window["end"])
            current["streams"].update(window["streams"])
            current["markets"].update(window["markets"])
            current["reasons"].update(window["reasons"])
            continue
        merged.append(window)
    return merged


def _availability_summary(
    gaps: list[dict],
    day: date,
    *,
    connection_epochs: list[int],
) -> dict:
    raw_windows = _merge_gap_windows(gaps, day, buffer=timedelta(0))
    exclusions = _merge_gap_windows(
        gaps,
        day,
        buffer=GAP_EXCLUSION_BUFFER,
    )
    maximum_gap_seconds = max(
        (
            max(0.0, (window["end"] - window["start"]).total_seconds())
            for window in raw_windows
        ),
        default=0.0,
    )
    total_gap_seconds = sum(
        (window["end"] - window["start"]).total_seconds()
        for window in raw_windows
    )
    within_daily_budget = (
        maximum_gap_seconds <= DAY_MAX_SINGLE_OUTAGE.total_seconds()
        and total_gap_seconds <= DAY_MAX_TOTAL_OUTAGE.total_seconds()
        and len(raw_windows) <= DAY_MAX_OUTAGE_EPISODES
    )
    transport_changed = len(connection_epochs) > 1
    serialized_exclusions = [
        {
            "start": _iso_z(window["start"]),
            "end": _iso_z(window["end"]),
            "duration_seconds": round(
                (window["end"] - window["start"]).total_seconds(), 6
            ),
            "streams": sorted(window["streams"]),
            "markets": sorted(window["markets"]),
            "reasons": sorted(window["reasons"]),
        }
        for window in exclusions
    ]
    warnings = []
    if transport_changed:
        warnings.append("transport_connection_epoch_changed")
    if gaps:
        warnings.append("bounded_capture_outage_excluded")
    return {
        "contract": "bounded_outage_exclusion_v1",
        "state": (
            "degraded" if gaps or transport_changed else "complete"
        ),
        "within_daily_budget": within_daily_budget,
        "maximum_single_outage_seconds": (
            DAY_MAX_SINGLE_OUTAGE.total_seconds()
        ),
        "maximum_total_outage_seconds": DAY_MAX_TOTAL_OUTAGE.total_seconds(),
        "maximum_outage_episodes": DAY_MAX_OUTAGE_EPISODES,
        "outage_episodes": len(raw_windows),
        "maximum_gap_seconds": round(maximum_gap_seconds, 6),
        "total_gap_seconds": round(total_gap_seconds, 6),
        "exclusion_buffer_seconds": GAP_EXCLUSION_BUFFER.total_seconds(),
        "excluded_seconds": round(sum(
            (window["end"] - window["start"]).total_seconds()
            for window in exclusions
        ), 6),
        "transport_connection_epochs": connection_epochs,
        "warnings": warnings,
        "exclusion_intervals": serialized_exclusions,
    }


def _connection_epoch_gaps(
    observations: list[tuple[datetime, int]],
    day: date,
) -> tuple[list[dict], list[str]]:
    """Turn each observed L2 transport generation change into an outage."""
    day_start, day_end = _day_bounds(day)
    ordered = sorted(observations, key=lambda item: (item[0], item[1]))
    if not ordered:
        return [], []
    gaps = []
    issues = []
    previous_time, previous_epoch = ordered[0]
    for observed_time, epoch in ordered[1:]:
        if epoch == previous_epoch:
            previous_time = max(previous_time, observed_time)
            continue
        if epoch < previous_epoch:
            issues.append("l2_stream_connection_epoch_regressed")
            continue
        started = max(day_start, previous_time)
        ended = min(day_end, observed_time)
        if ended <= started and started < day_end:
            ended = min(day_end, started + timedelta(microseconds=1))
        if ended > started:
            gaps.append(_gap_record(
                "transport",
                "ALL_USDT_SWAPS",
                "connection_epoch_change",
                started,
                ended,
            ))
        previous_time = observed_time
        previous_epoch = epoch
    return gaps, sorted(set(issues))


class _CoverageGapTracker:
    """Compute exact receipt-time gaps without retaining every observation."""

    def __init__(
        self,
        intervals: dict[str, list[tuple[datetime, datetime]]],
        *,
        stream: str,
        maximum_gap: timedelta,
    ) -> None:
        self._stream = stream
        self._maximum_gap = maximum_gap
        self._states = {
            market: [
                {
                    "start": started,
                    "end": ended,
                    "first": None,
                    "last": None,
                    "cadence_gaps": [],
                }
                for started, ended in expected
                if ended - started >= maximum_gap
            ]
            for market, expected in intervals.items()
        }
        self._cursor = {market: 0 for market in self._states}

    def observe(self, market: str, observed_at: datetime) -> None:
        states = self._states.get(market)
        if not states:
            return
        cursor = self._cursor[market]
        while cursor < len(states) and states[cursor]["end"] < observed_at:
            cursor += 1
        self._cursor[market] = cursor
        index = cursor
        while index < len(states) and states[index]["start"] <= observed_at:
            state = states[index]
            if observed_at <= state["end"]:
                previous = state["last"]
                if (
                    previous is not None
                    and observed_at - previous > self._maximum_gap
                ):
                    state["cadence_gaps"].append(_gap_record(
                        self._stream,
                        market,
                        "cadence_gap",
                        previous,
                        observed_at,
                    ))
                if state["first"] is None:
                    state["first"] = observed_at
                state["last"] = observed_at
            index += 1

    def finish(self) -> list[dict]:
        gaps = []
        for market, states in self._states.items():
            for state in states:
                first = state["first"]
                last = state["last"]
                if first is None:
                    gaps.append(_gap_record(
                        self._stream,
                        market,
                        "missing_interval",
                        state["start"],
                        state["end"],
                    ))
                    continue
                if first - state["start"] > self._maximum_gap:
                    gaps.append(_gap_record(
                        self._stream,
                        market,
                        "start_gap",
                        state["start"],
                        first,
                    ))
                if state["end"] - last > self._maximum_gap:
                    gaps.append(_gap_record(
                        self._stream,
                        market,
                        "end_gap",
                        last,
                        state["end"],
                    ))
                gaps.extend(state["cadence_gaps"])
        return gaps


class _ConnectionEpochGapTracker:
    """Stream epoch transitions in receipt order with bounded pending state."""

    def __init__(self, day: date) -> None:
        self._day_start, self._day_end = _day_bounds(day)
        self._pending_time: datetime | None = None
        self._pending_epochs: set[int] = set()
        self._previous_time: datetime | None = None
        self._previous_epoch: int | None = None
        self._gaps: list[dict] = []
        self._issues: set[str] = set()

    def observe(self, observed_at: datetime, epoch: int) -> None:
        if self._pending_time is None:
            self._pending_time = observed_at
        elif observed_at != self._pending_time:
            self._flush_pending()
            self._pending_time = observed_at
        self._pending_epochs.add(epoch)

    def _flush_pending(self) -> None:
        observed_at = self._pending_time
        if observed_at is None:
            return
        for epoch in sorted(self._pending_epochs):
            if self._previous_time is None or self._previous_epoch is None:
                self._previous_time = observed_at
                self._previous_epoch = epoch
                continue
            if epoch == self._previous_epoch:
                self._previous_time = max(self._previous_time, observed_at)
                continue
            if epoch < self._previous_epoch:
                self._issues.add("l2_stream_connection_epoch_regressed")
                continue
            started = max(self._day_start, self._previous_time)
            ended = min(self._day_end, observed_at)
            if ended <= started and started < self._day_end:
                ended = min(
                    self._day_end,
                    started + timedelta(microseconds=1),
                )
            if ended > started:
                self._gaps.append(_gap_record(
                    "transport",
                    "ALL_USDT_SWAPS",
                    "connection_epoch_change",
                    started,
                    ended,
                ))
            self._previous_time = observed_at
            self._previous_epoch = epoch
        self._pending_epochs.clear()

    def finish(self) -> tuple[list[dict], list[str]]:
        self._flush_pending()
        return self._gaps, sorted(self._issues)


def capture_continuity_health(reports: list[dict]) -> dict:
    """Grade the latest consecutive 30-day window without hiding gap days."""
    normalized = []
    for report in reports:
        day_text = report.get("day")
        if not isinstance(day_text, str):
            continue
        try:
            parsed_day = date.fromisoformat(day_text)
        except ValueError:
            continue
        if parsed_day.isoformat() != day_text:
            continue
        normalized.append((parsed_day, str(report.get("status") or "")))
    normalized.sort(key=lambda item: item[0])
    window = normalized[-CONTINUITY_WINDOW_DAYS:]
    maximum_degraded = math.floor(
        CONTINUITY_WINDOW_DAYS * CONTINUITY_MAX_DEGRADED_RATIO
    )
    result = {
        "window_days": CONTINUITY_WINDOW_DAYS,
        "observed_days": len(window),
        "maximum_degraded_days": maximum_degraded,
        "ready": len(window) == CONTINUITY_WINDOW_DAYS,
        "ok": None,
        "reason": "collecting_closed_days",
        "degraded_days": sum(
            status == "usable_with_gaps" for _, status in window
        ),
        "invalid_days": sum(status == "invalid" for _, status in window),
        "start_day": window[0][0].isoformat() if window else None,
        "end_day": window[-1][0].isoformat() if window else None,
    }
    if not result["ready"]:
        return result
    expected = [
        window[0][0] + timedelta(days=index)
        for index in range(CONTINUITY_WINDOW_DAYS)
    ]
    if [day for day, _ in window] != expected:
        result.update(ok=False, reason="closed_day_calendar_gap")
        return result
    if any(status not in {"valid", "usable_with_gaps"} for _, status in window):
        result.update(ok=False, reason="invalid_closed_day")
        return result
    if result["degraded_days"] > maximum_degraded:
        result.update(ok=False, reason="degraded_day_budget_exceeded")
        return result
    result.update(ok=True, reason="")
    return result


def _positive_number(value, label: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{label} is boolean")
    if type(value) not in {int, float}:
        raise ValueError(f"{label} is not numeric")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} is not numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{label} is not positive")
    return number


def _validate_book_payload(payload: dict, label: str) -> None:
    best = {}
    for side, descending in (("bids", True), ("asks", False)):
        levels = payload.get(side)
        if not isinstance(levels, list) or not levels:
            raise ValueError(f"{label} {side} is empty")
        previous = None
        for index, level in enumerate(levels):
            if not isinstance(level, list) or len(level) != 2:
                raise ValueError(f"{label} {side}[{index}] is malformed")
            price = _positive_number(
                level[0], f"{label} {side}[{index}].price"
            )
            _positive_number(
                level[1], f"{label} {side}[{index}].amount"
            )
            if previous is not None and (
                (descending and price >= previous)
                or (not descending and price <= previous)
            ):
                raise ValueError(f"{label} {side} is not strictly sorted")
            if index == 0:
                best[side] = price
            previous = price
    if best["bids"] >= best["asks"]:
        raise ValueError(f"{label} book is crossed or locked")
    nonce = payload.get("nonce")
    if nonce is not None and (
        type(nonce) is not int
        and (
            not isinstance(nonce, str)
            or not nonce
            or nonce != nonce.strip()
            or len(nonce) > 256
            or any(
                codepoint < 32
                or codepoint == 127
                or 0xD800 <= codepoint <= 0xDFFF
                for codepoint in map(ord, nonce)
            )
        )
    ):
        raise ValueError(f"{label} nonce is invalid")


def _validate_l2_payload(payload: dict) -> None:
    if (
        payload.get("stream_source") != "ccxt_pro"
        or payload.get("sequence_valid") is not False
        or payload.get("sequence_status") != "unverified_unified_orderbook"
    ):
        raise ValueError("l2 sequence contract is invalid")
    _validate_book_payload(payload, "l2")


def _valid_trade_id(value) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and len(value) <= 256
        and not any(
            codepoint < 32
            or codepoint == 127
            or 0xD800 <= codepoint <= 0xDFFF
            for codepoint in map(ord, value)
        )
    )


def _validate_rest_trades_payload(
    payload: dict,
    day: date,
    *,
    receipt_ms: int | None = None,
) -> None:
    trades = payload.get("trades")
    if not isinstance(trades, list) or len(trades) > 100:
        raise ValueError("REST trades payload is malformed")
    seen = set()
    previous_time = None
    for index, trade in enumerate(trades):
        if not isinstance(trade, dict):
            raise ValueError(f"REST trade[{index}] is malformed")
        trade_id = trade.get("id")
        if trade_id is not None and not _valid_trade_id(trade_id):
            raise ValueError("REST trade id is invalid")
        trade_time = trade.get("timestamp")
        if type(trade_time) is not int or trade_time <= 0:
            raise ValueError("REST trade timestamp is invalid")
        try:
            trade_day = datetime.fromtimestamp(
                trade_time / 1000, tz=timezone.utc
            ).date()
        except (OSError, OverflowError, ValueError) as exc:
            raise ValueError("REST trade timestamp is invalid") from exc
        if receipt_ms is None:
            if trade_day != day:
                raise ValueError("REST trade escaped UTC partition")
        elif not receipt_ms - 86_400_000 <= trade_time <= receipt_ms + 30_000:
            raise ValueError("REST trade escaped receipt window")
        price = _positive_number(trade.get("price"), "REST trade price")
        amount = _positive_number(trade.get("amount"), "REST trade amount")
        side = trade.get("side")
        if side not in {"buy", "sell"}:
            raise ValueError("REST trade side is invalid")
        identity = (
            ("id", trade_id)
            if trade_id is not None
            else ("fallback", trade_time, price, amount, side)
        )
        if identity in seen:
            raise ValueError("REST trade identity is duplicated")
        if previous_time is not None and trade_time < previous_time:
            raise ValueError("REST trade chronology is invalid")
        seen.add(identity)
        previous_time = trade_time


def _event_diagnostic(value, max_chars: int = 120) -> str:
    """Bound and single-line one immutable capture-row locator field."""
    rendered = str(value)
    return (
        rendered.replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")[:max_chars]
    )


def _validate_event_universe(payload: dict) -> None:
    """Require point-in-time universe evidence on non-overview events."""
    universe = payload.get("universe")
    if not isinstance(universe, list):
        raise ValueError("capture universe is not a list")
    if not universe:
        raise ValueError("capture universe is empty")
    if any(
        not isinstance(symbol, str)
        or not symbol
        or symbol != symbol.strip()
        or any(
            codepoint < 32
            or codepoint == 127
            or 0xD800 <= codepoint <= 0xDFFF
            for codepoint in map(ord, symbol)
        )
        for symbol in universe
    ):
        raise ValueError("capture universe contains an invalid symbol")
    if len(universe) != len(set(universe)):
        raise ValueError("capture universe contains duplicate symbols")


def _validate_rest_request_provenance(
    payload: dict,
    received_time: datetime,
) -> None:
    fields = ("request_started_ms", "request_ended_ms", "latency_ms")
    if any(field not in payload for field in fields):
        raise ValueError("REST request provenance is incomplete")
    started_ms, ended_ms, latency_ms = (payload[field] for field in fields)
    if (
        type(started_ms) is not int
        or type(ended_ms) is not int
        or type(latency_ms) is not int
        or started_ms <= 0
        or ended_ms <= 0
        or latency_ms < 0
    ):
        raise ValueError("REST request provenance is invalid")
    if started_ms > ended_ms or latency_ms != ended_ms - started_ms:
        raise ValueError("REST request chronology is invalid")
    received_ms = round(received_time.timestamp() * 1000)
    if abs(received_ms - ended_ms) > REST_RECEIPT_CLOCK_TOLERANCE_MS:
        raise ValueError("REST receipt provenance mismatch")


def _partition_rows(
    path: Path,
    stream: str,
    day: date,
    *,
    coverage_intervals: (
        dict[str, list[tuple[datetime, datetime]]] | None
    ) = None,
    maximum_gap: timedelta | None = None,
) -> tuple[dict, dict]:
    if _linklike(path):
        raise ValueError(f"{stream} partition is linked")
    if path.stat(follow_symlinks=False).st_nlink != 1:
        raise ValueError(f"{stream} partition is hard-linked")
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError(f"{stream} partition has active {suffix[1:]} data")
    relative_path = f"{stream}/{path.name}"
    initial_manifest = _manifest_file(path, relative_path)
    connection = sqlite3.connect(
        f"file:{path.as_posix()}?mode=ro&immutable=1",
        uri=True,
        timeout=30.0,
    )
    primary_error: BaseException | None = None
    try:
        connection.row_factory = sqlite3.Row
        overview_rows = []
        connection_epochs = set()
        warnings = set()
        event_count = 0
        websocket_trade_events = 0
        if (coverage_intervals is None) != (maximum_gap is None):
            raise ValueError("coverage contract is incomplete")
        coverage_tracker = None
        if coverage_intervals is not None and maximum_gap is not None:
            coverage_tracker = _CoverageGapTracker(
                coverage_intervals,
                stream=stream,
                maximum_gap=maximum_gap,
            )
        epoch_tracker = (
            _ConnectionEpochGapTracker(day)
            if stream == "l2_stream"
            else None
        )
        future_limit = _capture_now_utc() + timedelta(minutes=1)
        quick = connection.execute("PRAGMA quick_check").fetchall()
        if [str(row[0]) for row in quick] != ["ok"]:
            raise ValueError(f"{stream} quick_check failed")
        try:
            trade_identity_version = validate_trade_identity_schema(connection)
        except RuntimeError as exc:
            raise ValueError(f"{stream} trade identity schema mismatch: {exc}") from exc
        if trade_identity_version == 2 and stream != "trades":
            raise ValueError(f"{stream} trade identity schema is unexpected")
        table_info = connection.execute(
            "PRAGMA table_xinfo(venue_events)"
        ).fetchall()
        columns = tuple(str(row[1]) for row in table_info)
        schema = tuple(
            (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
            for row in table_info
        )
        if columns != EVENT_COLUMNS or schema != EVENT_SCHEMA:
            raise ValueError(f"{stream} schema mismatch")
        table_contract = tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                int(row[3]),
                int(row[4]),
                int(row[5]),
            )
            for row in connection.execute(
                "PRAGMA table_list('venue_events')"
            ).fetchall()
        )
        if table_contract != (("main", "venue_events", "table", 7, 1, 0),):
            raise ValueError(f"{stream} table contract mismatch")
        trigger = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name='venue_events' LIMIT 1"
        ).fetchone()
        if trigger is not None:
            raise ValueError(f"{stream} trigger contract mismatch")
        index_rows = tuple(
            row
            for row in connection.execute(
                "PRAGMA index_list(venue_events)"
            ).fetchall()
            if str(row[1]) == "idx_venue_events_time"
        )
        index_xinfo = tuple(
            (str(row[2]), int(row[3]), str(row[4]), int(row[5]))
            for row in connection.execute(
                "PRAGMA index_xinfo(idx_venue_events_time)"
            ).fetchall()
        )
        if (
            len(index_rows) != 1
            or int(index_rows[0][2]) != 0
            or str(index_rows[0][3]) != "c"
            or int(index_rows[0][4]) != 0
            or index_xinfo != EVENT_TIME_INDEX_XINFO
        ):
            raise ValueError(f"{stream} index contract mismatch")
        schema_objects = tuple(
            (str(row[0]), str(row[1]), str(row[2]))
            for row in connection.execute(
                "SELECT type,name,tbl_name FROM sqlite_master "
                "ORDER BY type,name"
            ).fetchall()
        )
        expected_objects = PARTITION_SCHEMA_OBJECTS
        if trade_identity_version == 2:
            expected_objects = tuple(sorted((
                *PARTITION_SCHEMA_OBJECTS, TRADE_IDENTITY_SCHEMA_OBJECT,
            )))
        if schema_objects != expected_objects:
            raise ValueError(f"{stream} schema objects mismatch")
        receipt_ordered = stream in {"depth", "l2_stream"}
        order_expression = (
            "julianday(received_time),received_time"
            if receipt_ordered
            else "exchange_time"
        )
        if receipt_ordered or stream == "trades":
            connection.execute("PRAGMA temp_store=FILE")
        if stream == "trades":
            # A liquid closed day can contain millions of public trades. Keep
            # the strict cross-event identity contract without retaining one
            # Python object per trade for the entire validation pass.
            connection.execute(
                """CREATE TEMP TABLE temp_websocket_trade_identities (
                       market_id TEXT NOT NULL,
                       trade_id TEXT NOT NULL,
                       trade_time INTEGER NOT NULL,
                       trade_price REAL NOT NULL,
                       trade_amount REAL NOT NULL,
                       trade_side TEXT NOT NULL,
                       PRIMARY KEY (market_id, trade_id)
                   ) WITHOUT ROWID"""
            )
        for row in connection.execute(
            "SELECT event_id,market_id,exchange_time,received_time,"
            "schema_version,quality_flags_json,payload_json "
            f"FROM venue_events ORDER BY {order_expression},event_id"
        ):
            event_count += 1
            for identity_field, max_chars in (
                ("event_id", 2048),
                ("market_id", 256),
            ):
                identity = row[identity_field]
                if (
                    not isinstance(identity, str)
                    or not identity
                    or identity != identity.strip()
                    or len(identity) > max_chars
                    or any(
                        codepoint < 32
                        or codepoint == 127
                        or 0xD800 <= codepoint <= 0xDFFF
                        for codepoint in map(ord, identity)
                    )
                ):
                    raise ValueError(
                        f"{stream} {identity_field} identity is invalid"
                    )
            if row["schema_version"] != 1:
                raise ValueError(f"{stream} schema_version mismatch")
            for clock_field in ("exchange_time", "received_time"):
                clock_text = row[clock_field]
                if (
                    not isinstance(clock_text, str)
                    or not clock_text
                    or clock_text != clock_text.strip()
                    or len(clock_text) > 64
                    or any(
                        codepoint < 32
                        or codepoint == 127
                        or 0xD800 <= codepoint <= 0xDFFF
                        for codepoint in map(ord, clock_text)
                    )
                ):
                    raise ValueError(
                        f"{stream} {clock_field} is invalid"
                    )
            exchange_time = _utc(row["exchange_time"])
            received_time = _utc(row["received_time"])
            if exchange_time.date() != day:
                raise ValueError(f"{stream} row escaped UTC partition")
            if received_time > future_limit:
                raise ValueError(f"{stream} received_time is in the future")
            flags = _bounded_json(
                row["quality_flags_json"],
                max_bytes=EVENT_QUALITY_FLAGS_JSON_MAX_BYTES,
                label=f"{stream} quality_flags_json",
            )
            payload = _bounded_json(
                row["payload_json"],
                max_bytes=EVENT_PAYLOAD_JSON_MAX_BYTES,
                label=f"{stream} payload_json",
            )
            coverage_eligible = True
            if (
                not isinstance(flags, list)
                or any(not isinstance(flag, str) or not flag for flag in flags)
                or len(flags) != len(set(flags))
                or not isinstance(payload, dict)
            ):
                raise ValueError(f"{stream} JSON contract mismatch")
            rest_event = stream in {"overview", "depth"} or (
                stream == "trades" and payload.get("stream_source") != "ccxt_pro"
            )
            receipt_keys = ("rest_snapshot_version", "partition_basis")
            receipt_marked = any(key in payload for key in receipt_keys)
            writer_keys = ("writer_input_sha256", "writer_input_exchange_time")
            writer_marked = any(key in payload for key in writer_keys)
            receipt_ms = None
            if receipt_marked:
                if (
                    stream != "trades"
                    or payload.get("stream_source") is not None
                    or type(payload.get("rest_snapshot_version")) is not int
                    or payload["rest_snapshot_version"] != 1
                    or payload.get("partition_basis") != "request_received"
                ):
                    raise ValueError("REST receipt partition contract is invalid")
            if rest_event:
                _validate_rest_request_provenance(payload, received_time)
            if writer_marked:
                writer_hash = payload.get("writer_input_sha256")
                original_exchange_text = payload.get("writer_input_exchange_time")
                if (
                    trade_identity_version != 2
                    or stream != "trades"
                    or payload.get("stream_source") != "ccxt_pro"
                    or not isinstance(writer_hash, str)
                    or len(writer_hash) != 64
                    or any(char not in "0123456789abcdef" for char in writer_hash)
                    or not isinstance(original_exchange_text, str)
                    or not original_exchange_text
                    or original_exchange_text != original_exchange_text.strip()
                    or len(original_exchange_text) > 64
                    or any(
                        codepoint < 32
                        or codepoint == 127
                        or 0xD800 <= codepoint <= 0xDFFF
                        for codepoint in map(ord, original_exchange_text)
                    )
                ):
                    raise ValueError("websocket writer input marker is invalid")
                original_exchange = _utc(original_exchange_text)
                received_ms = round(received_time.timestamp() * 1000)
                original_ms = round(original_exchange.timestamp() * 1000)
                if (
                    original_exchange.date() != day
                    or original_exchange.microsecond % 1000 != 0
                    or original_exchange < exchange_time
                    or original_ms > received_ms + WEBSOCKET_TRADE_FUTURE_TOLERANCE_MS
                    or received_ms - original_ms > WEBSOCKET_TRADE_MAX_AGE_MS
                ):
                    raise ValueError("websocket writer input clock mismatch")
            if receipt_marked:
                receipt_ms = payload["request_ended_ms"]
                if (
                    exchange_time != received_time
                    or exchange_time.microsecond % 1000 != 0
                    or round(exchange_time.timestamp() * 1000) != receipt_ms
                ):
                    raise ValueError("REST receipt partition clock mismatch")
            if stream != "overview":
                _validate_event_universe(payload)
            allowed = (
                L2_ALLOWED_QUALITY_FLAGS
                if stream == "l2_stream"
                else set()
            )
            if stream == "trades" and payload.get("stream_source") != "ccxt_pro":
                allowed.add("saturated_trade_payload")
                if "saturated_trade_payload" in flags:
                    warnings.add("rest_trade_window_saturated")
            disqualifying = set(flags) - allowed
            if disqualifying:
                raise ValueError(
                    f"{stream} disqualifying flags={sorted(disqualifying)} "
                    f"market={_event_diagnostic(row['market_id'])} "
                    f"exchange_time={_event_diagnostic(row['exchange_time'])} "
                    f"event_id={_event_diagnostic(row['event_id'])}"
                )
            if stream == "depth":
                _validate_book_payload(payload, "depth")
            elif (
                stream == "trades"
                and payload.get("stream_source") != "ccxt_pro"
            ):
                _validate_rest_trades_payload(
                    payload, day, receipt_ms=receipt_ms)
                if (
                    len(payload["trades"]) == 100
                    and "saturated_trade_payload" not in flags
                ):
                    raise ValueError(
                        "REST trade saturation evidence is missing"
                    )
            elif stream == "l2_stream":
                if tuple(flags) not in (
                    L2_BASE_QUALITY_FLAGS,
                    L2_STALE_QUALITY_FLAGS,
                ):
                    raise ValueError("l2 quality contract is incomplete")
                if L2_COVERAGE_EXCLUDED_FLAGS.intersection(flags):
                    warnings.add(L2_STALE_WARNING)
                    coverage_eligible = False
                _validate_l2_payload(payload)
            market_id = str(row["market_id"])
            if coverage_tracker is not None and coverage_eligible:
                coverage_tracker.observe(market_id, received_time)
            if stream == "overview":
                overview_rows.append({
                    "received_time": received_time,
                    "payload": payload,
                })
            elif (
                stream == "trades"
                and payload.get("stream_source") == "ccxt_pro"
            ):
                websocket_trade_events += 1
                if trade_identity_version == 2:
                    validate_ws_trade_payload(
                        payload,
                        exchange_time=row["exchange_time"],
                        received_time=row["received_time"],
                        day=day,
                    )
                trades = payload.get("trades")
                if (
                    payload.get("continuity_status")
                    != "websocket_observed_id_deduplicated"
                    or not isinstance(trades, list)
                    or not trades
                ):
                    raise ValueError("trades websocket payload is empty")
                if len(trades) > WEBSOCKET_TRADE_EVENT_MAX_ROWS:
                    raise ValueError("trades websocket payload is oversized")
                trade_ids = set()
                previous_trade_order = None
                for trade in trades:
                    if not isinstance(trade, dict):
                        raise ValueError("trades websocket payload is malformed")
                    trade_id = trade.get("id")
                    trade_time = trade.get("timestamp")
                    if (
                        not _valid_trade_id(trade_id)
                        or trade_id in trade_ids
                        or isinstance(trade_time, bool)
                        or not isinstance(trade_time, int)
                    ):
                        raise ValueError("trades websocket identity is invalid")
                    trade_ids.add(trade_id)
                    if trade_time <= 0:
                        raise ValueError(
                            "trades websocket timestamp is invalid"
                        )
                    try:
                        trade_day = datetime.fromtimestamp(
                            trade_time / 1000, tz=timezone.utc
                        ).date()
                    except (OSError, OverflowError, ValueError) as exc:
                        raise ValueError(
                            "trades websocket timestamp is invalid"
                        ) from exc
                    if trade_day != day:
                        raise ValueError("trade escaped UTC partition")
                    received_ms = round(received_time.timestamp() * 1000)
                    if (
                        trade_time
                        > received_ms + WEBSOCKET_TRADE_FUTURE_TOLERANCE_MS
                        or received_ms - trade_time
                        > WEBSOCKET_TRADE_MAX_AGE_MS
                    ):
                        raise ValueError(
                            "trades websocket receipt provenance mismatch"
                        )
                    trade_price = _positive_number(
                        trade.get("price"), "trade price"
                    )
                    trade_amount = _positive_number(
                        trade.get("amount"), "trade amount"
                    )
                    if trade.get("side") not in {"buy", "sell"}:
                        raise ValueError("trade side is invalid")
                    trade_order = (trade_time, trade_id)
                    if (
                        previous_trade_order is not None
                        and trade_order < previous_trade_order
                    ):
                        raise ValueError(
                            "trades websocket chronology is invalid"
                        )
                    previous_trade_order = trade_order
                    identity_key = (market_id, trade_id)
                    identity_evidence = (
                        trade_time,
                        trade_price,
                        trade_amount,
                        trade.get("side"),
                    )
                    identity_values = (*identity_key, *identity_evidence)
                    identity_cursor = connection.execute(
                        """INSERT OR IGNORE INTO temp.temp_websocket_trade_identities
                           (market_id, trade_id, trade_time, trade_price,
                            trade_amount, trade_side)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        identity_values,
                    )
                    if identity_cursor.rowcount == 0:
                        previous_identity = connection.execute(
                            """SELECT trade_time, trade_price, trade_amount,
                                      trade_side
                                 FROM temp.temp_websocket_trade_identities
                                WHERE market_id=? AND trade_id=?""",
                            identity_key,
                        ).fetchone()
                        previous_identity = (
                            tuple(previous_identity)
                            if previous_identity is not None
                            else None
                        )
                        if previous_identity == identity_evidence:
                            problem = "repeats across events"
                        else:
                            problem = "conflicts across events"
                        raise ValueError(
                            "websocket trade identity "
                            f"{problem} market={_event_diagnostic(market_id)} "
                            f"trade_id={_event_diagnostic(trade_id)} "
                            f"event_id={_event_diagnostic(row['event_id'])}"
                        )
                    if trade_identity_version == 2:
                        recorded_identity = read_trade_identity(
                            connection, market_id, trade_id,
                        )
                        if recorded_identity != (*identity_evidence, row["event_id"]):
                            raise ValueError(
                                "websocket trade identity ledger mismatch "
                                f"market={_event_diagnostic(market_id)} "
                                f"trade_id={_event_diagnostic(trade_id)}"
                            )
                latest_trade_time = datetime.fromtimestamp(
                    previous_trade_order[0] / 1000,
                    tz=timezone.utc,
                )
                if exchange_time != latest_trade_time:
                    raise ValueError(
                        "trades websocket exchange_time mismatch"
                    )
            elif writer_marked:
                raise ValueError("websocket writer input marker is invalid")
            if stream in {"trades", "l2_stream"}:
                epoch = payload.get("connection_epoch")
                epoch_required = stream == "l2_stream" or (
                    stream == "trades"
                    and payload.get("stream_source") == "ccxt_pro"
                )
                if epoch_required and (
                    isinstance(epoch, bool)
                    or not isinstance(epoch, int)
                    or epoch < 1
                ):
                    raise ValueError(f"{stream} connection epoch is invalid")
                if epoch_required:
                    connection_epochs.add(epoch)
                    if epoch_tracker is not None:
                        epoch_tracker.observe(received_time, epoch)
        if trade_identity_version == 2:
            ledger_count = connection.execute(
                "SELECT COUNT(*) FROM main.websocket_trade_identities"
            ).fetchone()[0]
            trade_count = connection.execute(
                "SELECT COUNT(*) FROM temp.temp_websocket_trade_identities"
            ).fetchone()[0]
            if ledger_count != trade_count:
                raise ValueError("websocket trade identity ledger count mismatch")
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            connection.close()
        except BaseException as close_error:
            if primary_error is None:
                raise
            try:
                primary_error.add_note(
                    "capture partition connection close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass
    if not event_count:
        raise ValueError(f"{stream} partition is empty")
    coverage_gaps = coverage_tracker.finish() if coverage_tracker else []
    if epoch_tracker is None:
        connection_epoch_gaps, connection_epoch_issues = [], []
    else:
        connection_epoch_gaps, connection_epoch_issues = epoch_tracker.finish()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError(f"{stream} partition has active {suffix[1:]} data")
    final_manifest = _manifest_file(
        path,
        relative_path,
        events=event_count,
        warnings=sorted(warnings),
    )
    if (
        final_manifest["bytes"] != initial_manifest["bytes"]
        or final_manifest["sha256"] != initial_manifest["sha256"]
    ):
        raise RuntimeError("capture artifact changed during validation")
    return final_manifest, {
        "overview": overview_rows,
        "websocket_trade_events": websocket_trade_events,
        "connection_epochs": sorted(connection_epochs),
        "coverage_gaps": coverage_gaps,
        "connection_epoch_gaps": connection_epoch_gaps,
        "connection_epoch_issues": connection_epoch_issues,
    }


def _membership_intervals(
    overview: list[dict],
    day: date,
) -> tuple[dict, list[str], list[dict]]:
    day_start, day_end = _day_bounds(day)
    issues = []
    gaps = []
    overview = sorted(overview, key=lambda row: row["received_time"])
    if overview[0]["received_time"] > day_start + timedelta(minutes=3):
        gaps.append(_gap_record(
            "overview",
            "ALL_USDT_SWAPS",
            "start_gap",
            day_start,
            overview[0]["received_time"],
        ))
    if overview[-1]["received_time"] < day_end - timedelta(minutes=3):
        gaps.append(_gap_record(
            "overview",
            "ALL_USDT_SWAPS",
            "end_gap",
            overview[-1]["received_time"],
            day_end,
        ))
    intervals: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
    active: dict[str, datetime] = {}
    previous_time = None
    for row in overview:
        current = row["received_time"]
        if previous_time is not None and current - previous_time > timedelta(minutes=3):
            gaps.append(_gap_record(
                "overview",
                "ALL_USDT_SWAPS",
                "cadence_gap",
                previous_time,
                current,
            ))
        previous_time = current
        payload = row["payload"]
        markets = payload.get("markets")
        universe = payload.get("universe")
        if not isinstance(markets, dict) or not isinstance(universe, list):
            issues.append("overview_universe_contract_invalid")
            continue
        if not universe or any(
            not isinstance(symbol, str)
            or not symbol
            or symbol != symbol.strip()
            or any(
                codepoint < 32
                or codepoint == 127
                or 0xD800 <= codepoint <= 0xDFFF
                for codepoint in map(ord, symbol)
            )
            for symbol in universe
        ):
            issues.append("overview_universe_contract_invalid")
            continue
        markets_by_symbol: dict[str, list[str]] = defaultdict(list)
        invalid_market_identity = False
        for market_id, item in markets.items():
            if (
                not isinstance(market_id, str)
                or not market_id
                or market_id != market_id.strip()
                or any(
                    codepoint < 32
                    or codepoint == 127
                    or 0xD800 <= codepoint <= 0xDFFF
                    for codepoint in map(ord, market_id)
                )
                or not isinstance(item, dict)
            ):
                invalid_market_identity = True
                continue
            symbol = item.get("symbol")
            if (
                not isinstance(symbol, str)
                or not symbol
                or symbol != symbol.strip()
                or any(
                    codepoint < 32
                    or codepoint == 127
                    or 0xD800 <= codepoint <= 0xDFFF
                    for codepoint in map(ord, symbol)
                )
            ):
                invalid_market_identity = True
                continue
            markets_by_symbol[symbol].append(market_id)
        if invalid_market_identity:
            issues.append("overview_market_identity_invalid")
        valid_symbols = [symbol for symbol in universe if isinstance(symbol, str)]
        selected = {
            markets_by_symbol[symbol][0]
            for symbol in valid_symbols
            if len(markets_by_symbol.get(symbol, ())) == 1
        }
        if (
            len(valid_symbols) != len(universe)
            or len(valid_symbols) != len(set(valid_symbols))
            or len(selected) != len(set(valid_symbols))
        ):
            issues.append("overview_universe_identity_incomplete")
        for market in set(active) - selected:
            intervals[market].append((active.pop(market), current))
        for market in selected - set(active):
            active[market] = current
    for market, started in active.items():
        intervals[market].append((started, day_end))
    return dict(intervals), sorted(set(issues)), gaps


def _coverage_gaps(
    by_market: dict[str, list[datetime]],
    intervals: dict[str, list[tuple[datetime, datetime]]],
    *,
    stream: str,
    maximum_gap: timedelta,
) -> list[dict]:
    gaps = []
    for market, expected in intervals.items():
        # Rows are read in exchange-time order, while freshness is measured by
        # received_time. Exchange timestamps can legitimately arrive out of
        # order, so establish the receipt chronology before gap arithmetic.
        samples = sorted(by_market.get(market, []))
        for started, ended in expected:
            if ended - started < maximum_gap:
                continue
            selected = [value for value in samples if started <= value <= ended]
            if not selected:
                gaps.append(_gap_record(
                    stream, market, "missing_interval", started, ended
                ))
                continue
            if selected[0] - started > maximum_gap:
                gaps.append(_gap_record(
                    stream, market, "start_gap", started, selected[0]
                ))
            if ended - selected[-1] > maximum_gap:
                gaps.append(_gap_record(
                    stream, market, "end_gap", selected[-1], ended
                ))
            for left, right in zip(selected, selected[1:]):
                if right - left > maximum_gap:
                    gaps.append(_gap_record(
                        stream, market, "cadence_gap", left, right
                    ))
    return gaps


def validate_capture_day(
    root: str | Path,
    day: date,
    *,
    max_symbols: int,
    micro_interval_seconds: float,
    l2_sample_interval_seconds: float,
) -> dict:
    """Validate one closed UTC day while retaining only bounded index fields."""
    if type(day) is not date:
        raise ValueError("capture day must be a date")
    rest_gap, l2_gap = _capture_policy_gaps(
        max_symbols=max_symbols,
        micro_interval_seconds=micro_interval_seconds,
        l2_sample_interval_seconds=l2_sample_interval_seconds,
    )
    root = Path(root)
    if _linklike(root):
        raise ValueError("capture root is linked")
    resolved_root = root.resolve()
    if day >= _capture_now_utc().date():
        raise ValueError("capture day is not closed")
    issues = []
    availability_gaps = []
    manifest = []
    stream_data = {}
    intervals = {}
    for stream in CORE_STREAMS:
        path = root / stream / f"{day.isoformat()}.sqlite3"
        try:
            resolved_path = path.resolve()
            resolved_path.relative_to(resolved_root)
        except (OSError, ValueError):
            issues.append(f"{stream}:partition_escaped_root")
            continue
        if _linklike(path.parent) or resolved_path != path.absolute():
            issues.append(f"{stream}:partition_parent_linked")
            continue
        if not path.is_file():
            issues.append(f"{stream}:partition_missing")
            continue
        try:
            coverage_gap = {
                "depth": rest_gap,
                "l2_stream": l2_gap,
            }.get(stream)
            entry, data = _partition_rows(
                path,
                stream,
                day,
                coverage_intervals=(
                    intervals if coverage_gap is not None else None
                ),
                maximum_gap=coverage_gap,
            )
        except (OSError, sqlite3.Error, ValueError) as exc:
            issues.append(f"{stream}:{type(exc).__name__}:{str(exc)[:400]}")
            try:
                artifacts = [path]
                for suffix in ("-wal", "-shm"):
                    sidecar = Path(f"{path}{suffix}")
                    if sidecar.is_file() and sidecar.stat().st_size:
                        artifacts.append(sidecar)
                manifest.extend(
                    _manifest_file(
                        artifact,
                        f"{stream}/{artifact.name}",
                        validation="failed",
                    )
                    for artifact in artifacts
                )
            except (OSError, RuntimeError) as manifest_exc:
                issues.append(
                    f"{stream}:manifest_binding_failed:"
                    f"{type(manifest_exc).__name__}:"
                    f"{str(manifest_exc)[:240]}"
                )
            continue
        manifest.append(entry)
        stream_data[stream] = data
        if stream == "overview":
            intervals, overview_issues, overview_gaps = _membership_intervals(
                data["overview"], day
            )
            issues.extend(overview_issues)
            availability_gaps.extend(overview_gaps)
        elif stream in {"depth", "l2_stream"}:
            availability_gaps.extend(
                data.get("coverage_gaps", [])
            )
        if stream == "l2_stream":
            availability_gaps.extend(
                data.get("connection_epoch_gaps", [])
            )
            issues.extend(data.get("connection_epoch_issues", []))
    observed_epochs = {
        epoch
        for stream in ("trades", "l2_stream")
        for epoch in stream_data.get(stream, {}).get("connection_epochs", [])
    }
    if (
        "trades" in stream_data
        and not stream_data["trades"].get("websocket_trade_events")
    ):
        issues.append("trades_ws_primary_stream_missing")
    # Public trades are event-driven. Silence is not proof of transport loss,
    # so it must never invent an outage for a low-activity market. Shared
    # connection epochs remain visible in the availability report; actual
    # outage windows come from periodic overview/depth and sampled L2.
    availability = _availability_summary(
        availability_gaps,
        day,
        connection_epochs=sorted(observed_epochs),
    )
    if not availability["within_daily_budget"]:
        issues.append("availability_daily_budget_exceeded")
    issues = sorted(set(issues))
    if issues:
        status = "invalid"
    elif availability["state"] == "degraded":
        status = "usable_with_gaps"
    else:
        status = "valid"
    result = {
        "schema_version": 1,
        "day": day.isoformat(),
        "status": status,
        "issues": issues,
        "availability": availability,
        "universe_contract": UNIVERSE_CONTRACT,
        "sequence_contract": SEQUENCE_CONTRACT,
        "manifest": sorted(manifest, key=lambda item: item["path"]),
    }
    result["report_sha256"] = hashlib.sha256(
        json.dumps(
            result,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest().upper()
    return result


def _atomic_create(path: Path, payload: dict) -> None:
    if _path_has_links(path):
        raise RuntimeError("capture integrity report path is linked")
    path.parent.mkdir(parents=True, exist_ok=True)
    if _path_has_links(path):
        raise RuntimeError("capture integrity report path is linked")
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > INTEGRITY_REPORT_MAX_BYTES:
        raise ValueError("capture integrity report exceeds size limit")
    if path.exists():
        existing = _read_integrity_report_bytes(path)
        if existing != encoded:
            raise RuntimeError("sealed capture report changed")
        _fsync_parent_directory(path.parent)
        return
    temporary = path.parent / (
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    temporary_owned = False
    primary_error: BaseException | None = None
    try:
        handle = temporary.open("xb")
        temporary_owned = True
        with handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if _read_integrity_report_bytes(path) != encoded:
                raise RuntimeError("sealed capture report changed")
        _fsync_parent_directory(path.parent)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if temporary_owned:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as cleanup_error:
                if primary_error is None:
                    raise
                primary_error.add_note(
                    "capture integrity temporary cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )


def _fsync_parent_directory(path: Path) -> None:
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path, flags)
    except AttributeError:
        return
    except OSError as exc:
        # CPython on Windows cannot open directories through os.open and
        # reports EACCES/PermissionError.  Keep that platform limitation as a
        # best-effort fallback, but never hide real POSIX I/O/open failures.
        if os.name == "nt" and isinstance(exc, PermissionError):
            return
        raise
    try:
        os.fsync(directory_fd)
    finally:
        try:
            os.close(directory_fd)
        except OSError:
            pass


def _verify_sealed_report(
    root: Path,
    report: dict,
    *,
    expected_day: date | str,
    verification_cache: dict[str, tuple] | None = None,
) -> None:
    if (
        not isinstance(report, dict)
        or type(report.get("schema_version")) is not int
        or report.get("schema_version") != 1
    ):
        raise RuntimeError("sealed capture report schema is invalid")
    expected_day_text = (
        expected_day.isoformat() if isinstance(expected_day, date)
        else str(expected_day)
    )
    try:
        parsed_expected_day = date.fromisoformat(expected_day_text)
    except ValueError as exc:
        raise RuntimeError("sealed capture report day is invalid") from exc
    if parsed_expected_day.isoformat() != expected_day_text:
        raise RuntimeError("sealed capture report day is invalid")
    if report.get("day") != expected_day_text:
        raise RuntimeError("sealed capture report day does not match seal path")
    status = report.get("status")
    if status not in {"valid", "usable_with_gaps", "invalid"}:
        raise RuntimeError("sealed capture report status is invalid")
    if status in {"valid", "usable_with_gaps"} and set(report) != {
        "schema_version",
        "day",
        "status",
        "issues",
        "availability",
        "universe_contract",
        "sequence_contract",
        "manifest",
        "report_sha256",
    }:
        raise RuntimeError("sealed capture report fields are invalid")
    if report.get("universe_contract") != UNIVERSE_CONTRACT:
        raise RuntimeError("sealed capture universe contract is invalid")
    if report.get("sequence_contract") != SEQUENCE_CONTRACT:
        raise RuntimeError("sealed capture sequence contract is invalid")
    if status in {"valid", "usable_with_gaps"}:
        availability = report.get("availability")
        availability_fields = {
            "contract",
            "state",
            "within_daily_budget",
            "maximum_single_outage_seconds",
            "maximum_total_outage_seconds",
            "maximum_outage_episodes",
            "outage_episodes",
            "maximum_gap_seconds",
            "total_gap_seconds",
            "exclusion_buffer_seconds",
            "excluded_seconds",
            "transport_connection_epochs",
            "warnings",
            "exclusion_intervals",
        }
        expected_availability_state = (
            "complete" if status == "valid" else "degraded"
        )
        if (
            not isinstance(availability, dict)
            or set(availability) != availability_fields
            or availability.get("contract") != "bounded_outage_exclusion_v1"
            or availability.get("state") != expected_availability_state
            or availability.get("within_daily_budget") is not True
            or not isinstance(availability.get("exclusion_intervals"), list)
            or (
                status == "valid"
                and bool(availability.get("exclusion_intervals"))
            )
        ):
            raise RuntimeError("sealed capture availability is invalid")
        expected_policy_seconds = {
            "maximum_single_outage_seconds": (
                DAY_MAX_SINGLE_OUTAGE.total_seconds()
            ),
            "maximum_total_outage_seconds": (
                DAY_MAX_TOTAL_OUTAGE.total_seconds()
            ),
            "exclusion_buffer_seconds": GAP_EXCLUSION_BUFFER.total_seconds(),
        }
        if any(
            type(availability.get(field)) not in {int, float}
            or not math.isfinite(float(availability[field]))
            or float(availability[field]) != expected
            for field, expected in expected_policy_seconds.items()
        ) or (
            type(availability.get("maximum_outage_episodes")) is not int
            or availability["maximum_outage_episodes"]
            != DAY_MAX_OUTAGE_EPISODES
        ):
            raise RuntimeError("sealed capture availability is invalid")
        try:
            day_start, day_end = _day_bounds(
                date.fromisoformat(expected_day_text)
            )
            previous_end = None
            total_excluded_seconds = 0.0
            exclusion_intervals = availability["exclusion_intervals"]
            for interval in exclusion_intervals:
                if (
                    not isinstance(interval, dict)
                    or set(interval) != {
                        "start",
                        "end",
                        "duration_seconds",
                        "streams",
                        "markets",
                        "reasons",
                    }
                ):
                    raise ValueError("exclusion interval is not an object")
                started = _utc(interval.get("start"))
                ended = _utc(interval.get("end"))
                if (
                    started < day_start
                    or ended > day_end
                    or ended <= started
                    or (previous_end is not None and started <= previous_end)
                ):
                    raise ValueError("exclusion interval bounds are invalid")
                duration_seconds = interval.get("duration_seconds")
                raw_duration = (ended - started).total_seconds()
                expected_duration = round(raw_duration, 6)
                if (
                    type(duration_seconds) not in {int, float}
                    or not math.isfinite(float(duration_seconds))
                    or float(duration_seconds) != expected_duration
                ):
                    raise ValueError("exclusion interval duration is invalid")
                for field in ("streams", "markets", "reasons"):
                    values = interval.get(field)
                    if (
                        not isinstance(values, list)
                        or not values
                        or any(
                            not isinstance(value, str)
                            or not value
                            or value != value.strip()
                            or any(
                                codepoint < 32
                                or codepoint == 127
                                or 0xD800 <= codepoint <= 0xDFFF
                                for codepoint in map(ord, value)
                            )
                            for value in values
                        )
                        or values != sorted(set(values))
                    ):
                        raise ValueError(
                            "exclusion interval provenance is invalid"
                        )
                previous_end = ended
                total_excluded_seconds += raw_duration
            excluded_seconds = availability.get("excluded_seconds")
            if (
                type(excluded_seconds) not in {int, float}
                or not math.isfinite(float(excluded_seconds))
                or float(excluded_seconds)
                != round(total_excluded_seconds, 6)
            ):
                raise ValueError("excluded duration is invalid")
            outage_episodes = availability.get("outage_episodes")
            maximum_gap_seconds = availability.get("maximum_gap_seconds")
            total_gap_seconds = availability.get("total_gap_seconds")
            if (
                type(outage_episodes) is not int
                or outage_episodes < 0
                or outage_episodes > DAY_MAX_OUTAGE_EPISODES
                or type(maximum_gap_seconds) not in {int, float}
                or type(total_gap_seconds) not in {int, float}
                or not math.isfinite(float(maximum_gap_seconds))
                or not math.isfinite(float(total_gap_seconds))
                or float(maximum_gap_seconds) < 0.0
                or float(total_gap_seconds) < float(maximum_gap_seconds)
                or float(maximum_gap_seconds)
                > DAY_MAX_SINGLE_OUTAGE.total_seconds()
                or float(total_gap_seconds)
                > DAY_MAX_TOTAL_OUTAGE.total_seconds()
                or bool(outage_episodes) != bool(exclusion_intervals)
                or (
                    outage_episodes == 0
                    and (
                        float(maximum_gap_seconds) != 0.0
                        or float(total_gap_seconds) != 0.0
                    )
                )
                or (
                    outage_episodes > 0
                    and float(maximum_gap_seconds) <= 0.0
                )
            ):
                raise ValueError("outage metrics are invalid")
            connection_epochs = availability.get(
                "transport_connection_epochs"
            )
            if (
                not isinstance(connection_epochs, list)
                or any(
                    type(epoch) is not int or epoch < 1
                    for epoch in connection_epochs
                )
                or connection_epochs != sorted(set(connection_epochs))
            ):
                raise ValueError("transport epochs are invalid")
            expected_warnings = []
            if len(connection_epochs) > 1:
                expected_warnings.append(
                    "transport_connection_epoch_changed"
                )
            if exclusion_intervals:
                expected_warnings.append("bounded_capture_outage_excluded")
            expected_state = (
                "degraded" if expected_warnings else "complete"
            )
            if (
                availability.get("warnings") != expected_warnings
                or availability.get("state") != expected_state
            ):
                raise ValueError("availability state evidence is invalid")
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                "sealed capture availability is invalid"
            ) from exc
    reported_hash = report.get("report_sha256")
    body = dict(report)
    body.pop("report_sha256", None)
    expected_hash = hashlib.sha256(
        json.dumps(
            body,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest().upper()
    if reported_hash != expected_hash:
        raise RuntimeError("sealed capture report hash is invalid")
    issues = report.get("issues")
    if (
        not isinstance(issues, list)
        or any(
            not isinstance(issue, str)
            or not issue
            or issue != issue.strip()
            or len(issue) > 512
            or any(
                codepoint < 32
                or codepoint == 127
                or 0xD800 <= codepoint <= 0xDFFF
                for codepoint in map(ord, issue)
            )
            for issue in issues
        )
        or len(issues) != len(set(issues))
    ):
        raise RuntimeError("sealed capture report issues are invalid")
    if (status == "invalid") != bool(issues):
        raise RuntimeError("sealed capture report status and issues disagree")
    manifest_items = report.get("manifest")
    if not isinstance(manifest_items, list):
        raise RuntimeError("sealed capture manifest is invalid")
    manifest_path_list = [
        item.get("path")
        for item in manifest_items
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    ]
    if len(manifest_path_list) != len(manifest_items) or len(
        manifest_path_list
    ) != len(set(manifest_path_list)):
        raise RuntimeError("sealed capture manifest is invalid")
    manifest_paths = set(manifest_path_list)
    required_paths = {
        f"{stream}/{expected_day_text}.sqlite3" for stream in CORE_STREAMS
    }
    if status in {"valid", "usable_with_gaps"}:
        if manifest_paths != required_paths:
            raise RuntimeError(
                "sealed capture manifest core partition bindings are incomplete"
            )
    else:
        issues = report.get("issues")
        issue_rows = issues if isinstance(issues, list) else []
        # Legacy invalid seals omitted both genuinely missing partitions and
        # partitions that existed but failed validation.  The hash-bound issue
        # identifies which state was sealed.  Preserve that existence truth;
        # never reinterpret every omission as a missing file.
        for relative_path in required_paths - manifest_paths:
            path = root / relative_path
            stream = relative_path.split("/", 1)[0]
            stream_issues = [
                issue for issue in issue_rows
                if isinstance(issue, str) and issue.startswith(f"{stream}:")
            ]
            absent_markers = (
                f"{stream}:partition_missing",
                f"{stream}:partition_escaped_root",
                f"{stream}:partition_parent_linked",
            )
            sealed_as_present = bool(stream_issues) and not any(
                issue.startswith(absent_markers) for issue in stream_issues
            )
            if sealed_as_present:
                if not path.is_file():
                    _raise_partition_drift(
                        expected_day_text, relative_path, "missing_sealed_file"
                    )
                continue
            if path.exists():
                _raise_partition_drift(
                    expected_day_text, relative_path, "unexpected_file"
                )
            for suffix in ("-wal", "-shm"):
                sidecar = Path(f"{path}{suffix}")
                if sidecar.exists() and sidecar.stat().st_size:
                    _raise_partition_drift(
                        expected_day_text,
                        f"{relative_path}{suffix}",
                        "unexpected_nonempty_sidecar",
                    )
    for item in manifest_items:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise RuntimeError("sealed capture manifest is invalid")
        if status in {"valid", "usable_with_gaps"}:
            if set(item) != {
                "path",
                "bytes",
                "sha256",
                "events",
                "warnings",
            }:
                raise RuntimeError(
                    "sealed capture manifest fields are invalid"
                )
            events = item.get("events")
            if type(events) is not int or events < 1:
                raise RuntimeError(
                    "sealed capture manifest event count is invalid"
                )
            stream = item["path"].split("/", 1)[0]
            allowed_warnings = {
                "trades": {"rest_trade_window_saturated"},
                "l2_stream": {L2_STALE_WARNING},
            }.get(stream, set())
            warnings = item.get("warnings")
            if (
                not isinstance(warnings, list)
                or warnings != sorted(set(warnings))
                or any(
                    not isinstance(warning, str)
                    or warning not in allowed_warnings
                    for warning in warnings
                )
            ):
                raise RuntimeError(
                    "sealed capture manifest warnings are invalid"
                )
        reported_bytes = item.get("bytes")
        if type(reported_bytes) is not int or reported_bytes < 0:
            raise RuntimeError("sealed capture manifest byte length is invalid")
        path = root / item["path"]
        try:
            relative = path.resolve().relative_to(root.resolve())
        except (OSError, ValueError) as exc:
            raise RuntimeError("sealed capture manifest escaped root") from exc
        if relative.as_posix() != item["path"] or _linklike(path):
            raise RuntimeError("sealed capture manifest path is invalid")
        if not path.is_file():
            _raise_partition_drift(
                expected_day_text, item["path"], "missing_manifest_file"
            )
        try:
            stat_before = path.stat()
            if stat_before.st_nlink != 1:
                raise RuntimeError(
                    "sealed capture manifest file is hard-linked"
                )
            cache_key = str(path.resolve())
            signature = (
                item.get("sha256"),
                item.get("bytes"),
                stat_before.st_size,
                stat_before.st_mtime_ns,
                stat_before.st_ctime_ns,
                stat_before.st_dev,
                stat_before.st_ino,
                stat_before.st_nlink,
            )
        except OSError as exc:
            _raise_partition_drift(
                expected_day_text, item["path"], "stat_unavailable", exc
            )
        if stat_before.st_size != reported_bytes:
            if verification_cache is not None:
                verification_cache.pop(cache_key, None)
            _raise_partition_drift(
                expected_day_text, item["path"], "byte_length_mismatch"
            )
        if verification_cache is None or verification_cache.get(
            cache_key
        ) != signature:
            if _sha256(path) != item.get("sha256"):
                if verification_cache is not None:
                    verification_cache.pop(cache_key, None)
                _raise_partition_drift(
                    expected_day_text, item["path"], "sha256_mismatch"
                )
            try:
                stat_after = path.stat()
            except OSError as exc:
                _raise_partition_drift(
                    expected_day_text, item["path"], "restat_unavailable", exc
                )
            after_signature = (
                item.get("sha256"),
                item.get("bytes"),
                stat_after.st_size,
                stat_after.st_mtime_ns,
                stat_after.st_ctime_ns,
                stat_after.st_dev,
                stat_after.st_ino,
                stat_after.st_nlink,
            )
            if after_signature != signature:
                if verification_cache is not None:
                    verification_cache.pop(cache_key, None)
                _raise_partition_drift(
                    expected_day_text, item["path"], "changed_during_hash"
                )
            if verification_cache is not None:
                verification_cache[cache_key] = signature
        if path.name.endswith(".sqlite3"):
            for suffix in ("-wal", "-shm"):
                sidecar = Path(f"{path}{suffix}")
                sidecar_relative = f"{item['path']}{suffix}"
                if (
                    sidecar_relative not in manifest_paths
                    and sidecar.is_file()
                    and sidecar.stat().st_size
                ):
                    _raise_partition_drift(
                        expected_day_text,
                        sidecar_relative,
                        "unexpected_nonempty_sidecar",
                    )


def _remove_empty_sidecars(root: Path, report: dict) -> None:
    for item in report.get("manifest") or ():
        path = root / item["path"]
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{path}{suffix}")
            if sidecar.exists() and sidecar.stat().st_size == 0:
                sidecar.unlink()


def _prune_verification_cache(
    root: Path,
    reports: list[dict],
    verification_cache: dict[str, tuple] | None,
) -> None:
    if verification_cache is None:
        return
    retained_keys = {
        str((root / item["path"]).resolve())
        for report in reports
        for item in (report.get("manifest") or ())
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    }
    for cache_key in tuple(verification_cache):
        if cache_key not in retained_keys:
            verification_cache.pop(cache_key, None)


def seal_closed_capture_days(
    root: str | Path,
    *,
    max_symbols: int,
    micro_interval_seconds: float,
    l2_sample_interval_seconds: float,
    partition_guard=None,
    verification_cache: dict[str, tuple] | None = None,
) -> dict:
    _capture_policy_gaps(
        max_symbols=max_symbols,
        micro_interval_seconds=micro_interval_seconds,
        l2_sample_interval_seconds=l2_sample_interval_seconds,
    )
    root = Path(root)
    now = _capture_now_utc()
    today = now.date()
    partition_days = {
        parsed.date()
        for stream in CORE_STREAMS
        for path in (root / stream).glob("*.sqlite3")
        if (parsed := _partition_date(path)) is not None
        and parsed.date() < today
        and parsed + timedelta(days=1) + SEAL_GRACE <= now
    }
    sealed_days = {
        parsed.date()
        for path in (root / "integrity").glob("*.json")
        if (parsed := _partition_date(path)) is not None
        and parsed.date() < today
        and parsed + timedelta(days=1) + SEAL_GRACE <= now
    }
    known_days = partition_days | sealed_days
    days_to_seal = set(known_days)
    if known_days:
        # A day with no partition in any stream has no filesystem entry from
        # which the old discovery loop could learn about it.  Materialize the
        # closed tail of the active continuity window explicitly so a total
        # outage cannot remain invisible merely because the next day has not
        # produced a partition yet.  Bound inferred days to the contractual
        # 30-day window; older known artifacts are still verified below.
        latest_closed_day = (now - SEAL_GRACE).date() - timedelta(days=1)
        inferred_start = max(
            min(known_days),
            latest_closed_day - timedelta(days=CONTINUITY_WINDOW_DAYS - 1),
        )
        cursor = inferred_start
        while cursor <= latest_closed_day:
            days_to_seal.add(cursor)
            cursor += timedelta(days=1)
    days = sorted(days_to_seal)
    reports = []
    for day in days:
        guard = (
            partition_guard(day)
            if callable(partition_guard)
            else nullcontext()
        )
        with guard:
            report_path = root / "integrity" / f"{day.isoformat()}.json"
            if report_path.exists():
                report = _read_integrity_report(report_path)
                _verify_sealed_report(
                    root,
                    report,
                    expected_day=day,
                    verification_cache=verification_cache,
                )
            else:
                report = validate_capture_day(
                    root,
                    day,
                    max_symbols=max_symbols,
                    micro_interval_seconds=micro_interval_seconds,
                    l2_sample_interval_seconds=l2_sample_interval_seconds,
                )
                if report.get("status") in {"valid", "usable_with_gaps"}:
                    _remove_empty_sidecars(root, report)
                _atomic_create(report_path, report)
                # The validation hashes precede publication. Re-read the
                # create-once artifact and force an uncached verification while
                # the official writer is still excluded by the day guard, so a
                # concurrent/non-cooperating filesystem mutation cannot be
                # reported healthy for one integrity cycle.
                report = _read_integrity_report(report_path)
                _verify_sealed_report(
                    root,
                    report,
                    expected_day=day,
                    verification_cache=None,
                )
        reports.append(report)
    invalid = [
        report["day"]
        for report in reports
        if report.get("status") not in {"valid", "usable_with_gaps"}
    ]
    degraded = [
        report["day"]
        for report in reports
        if report.get("status") == "usable_with_gaps"
    ]
    strict_valid = [
        report["day"] for report in reports if report.get("status") == "valid"
    ]
    _prune_verification_cache(root, reports, verification_cache)
    # Keep the runtime health payload useful without copying complete sealed
    # reports into every heartbeat.  The reports remain authoritative; this
    # is only a bounded diagnostic projection of the newest invalid days.
    invalid_day_issues = [
        {
            "day": str(report.get("day") or "")[:16],
            "issues": [
                str(issue)[:240]
                for issue in (report.get("issues") or ())[:4]
            ],
        }
        for report in reports
        if report.get("status") == "invalid"
    ][-8:]
    return {
        "ok": not invalid,
        "sealed_days": len(reports),
        "valid_days": len(strict_valid),
        "usable_days": len(strict_valid) + len(degraded),
        "degraded_days": degraded,
        "invalid_days": invalid,
        "invalid_day_issues": invalid_day_issues,
        "latest_day": reports[-1]["day"] if reports else None,
        "continuity": capture_continuity_health(reports),
    }


def _partition_date(path: Path) -> datetime | None:
    try:
        parsed = datetime.strptime(path.stem, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return parsed if parsed.date().isoformat() == path.stem else None
