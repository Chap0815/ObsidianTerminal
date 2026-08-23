"""Bounded, fail-closed acceptance contract for closed venue-capture days."""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path


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
SEAL_GRACE = timedelta(hours=1)
GAP_EXCLUSION_BUFFER = timedelta(minutes=2)
DAY_MAX_SINGLE_OUTAGE = timedelta(minutes=45)
DAY_MAX_TOTAL_OUTAGE = timedelta(hours=1)
DAY_MAX_OUTAGE_EPISODES = 6
CONTINUITY_WINDOW_DAYS = 30
CONTINUITY_MAX_DEGRADED_RATIO = 0.20


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


def _utc(value) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp is missing")
    parsed = datetime.fromisoformat(
        value[:-1] + "+00:00" if value.endswith("Z") else value
    )
    if parsed.tzinfo is None:
        raise ValueError("timestamp is naive")
    return parsed.astimezone(timezone.utc)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest().upper()


def _linklike(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = path.stat(follow_symlinks=False).st_file_attributes
    except (AttributeError, OSError):
        return False
    return bool(attributes & 0x400)


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
            max(0.0, (gap["end"] - gap["start"]).total_seconds())
            for gap in gaps
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


def capture_continuity_health(reports: list[dict]) -> dict:
    """Grade the latest consecutive 30-day window without hiding gap days."""
    normalized = []
    for report in reports:
        try:
            parsed_day = date.fromisoformat(str(report.get("day") or ""))
        except (TypeError, ValueError):
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
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} is not numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{label} is not positive")
    return number


def _validate_l2_payload(payload: dict) -> None:
    if (
        payload.get("stream_source") != "ccxt_pro"
        or payload.get("sequence_valid") is not False
        or payload.get("sequence_status") != "unverified_unified_orderbook"
    ):
        raise ValueError("l2 sequence contract is invalid")
    best = {}
    for side, descending in (("bids", True), ("asks", False)):
        levels = payload.get(side)
        if not isinstance(levels, list) or not levels:
            raise ValueError(f"l2 {side} is empty")
        previous = None
        for index, level in enumerate(levels):
            if not isinstance(level, list) or len(level) != 2:
                raise ValueError(f"l2 {side}[{index}] is malformed")
            price = _positive_number(level[0], f"l2 {side}[{index}].price")
            _positive_number(level[1], f"l2 {side}[{index}].amount")
            if previous is not None and (
                (descending and price >= previous)
                or (not descending and price <= previous)
            ):
                raise ValueError(f"l2 {side} is not strictly sorted")
            if index == 0:
                best[side] = price
            previous = price
    if best["bids"] >= best["asks"]:
        raise ValueError("l2 book is crossed or locked")


def _partition_rows(path: Path, stream: str, day: date) -> tuple[dict, dict]:
    if _linklike(path):
        raise ValueError(f"{stream} partition is linked")
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError(f"{stream} partition has active {suffix[1:]} data")
    connection = sqlite3.connect(
        f"file:{path.as_posix()}?mode=ro&immutable=1",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    samples: dict[str, list[datetime]] = defaultdict(list)
    overview_rows = []
    connection_epochs = set()
    warnings = set()
    event_count = 0
    websocket_trade_events = 0
    future_limit = datetime.now(timezone.utc) + timedelta(minutes=1)
    try:
        quick = connection.execute("PRAGMA quick_check").fetchall()
        if [str(row[0]) for row in quick] != ["ok"]:
            raise ValueError(f"{stream} quick_check failed")
        table_info = connection.execute(
            "PRAGMA table_info(venue_events)"
        ).fetchall()
        columns = tuple(str(row[1]) for row in table_info)
        schema = tuple(
            (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
            for row in table_info
        )
        if columns != EVENT_COLUMNS or schema != EVENT_SCHEMA:
            raise ValueError(f"{stream} schema mismatch")
        for row in connection.execute(
            "SELECT event_id,market_id,exchange_time,received_time,"
            "schema_version,quality_flags_json,payload_json "
            "FROM venue_events ORDER BY exchange_time,event_id"
        ):
            event_count += 1
            if row["schema_version"] != 1:
                raise ValueError(f"{stream} schema_version mismatch")
            exchange_time = _utc(row["exchange_time"])
            received_time = _utc(row["received_time"])
            if exchange_time.date() != day:
                raise ValueError(f"{stream} row escaped UTC partition")
            if received_time > future_limit:
                raise ValueError(f"{stream} received_time is in the future")
            flags = _json(row["quality_flags_json"])
            payload = _json(row["payload_json"])
            if (
                not isinstance(flags, list)
                or len(flags) != len(set(flags))
                or any(not isinstance(flag, str) or not flag for flag in flags)
                or not isinstance(payload, dict)
            ):
                raise ValueError(f"{stream} JSON contract mismatch")
            allowed = {"sequence_unverified"} if stream == "l2_stream" else set()
            if stream == "trades" and payload.get("stream_source") != "ccxt_pro":
                allowed.add("saturated_trade_payload")
                if "saturated_trade_payload" in flags:
                    warnings.add("rest_trade_window_saturated")
            disqualifying = set(flags) - allowed
            if disqualifying:
                raise ValueError(
                    f"{stream} disqualifying flags: {sorted(disqualifying)}"
                )
            if stream == "l2_stream":
                if flags != ["sequence_unverified"]:
                    raise ValueError("l2 quality contract is incomplete")
                _validate_l2_payload(payload)
            market_id = str(row["market_id"])
            if stream in {"depth", "l2_stream"}:
                samples[market_id].append(received_time)
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
                trades = payload.get("trades")
                if (
                    payload.get("continuity_status")
                    != "websocket_observed_id_deduplicated"
                    or not isinstance(trades, list)
                    or not trades
                ):
                    raise ValueError("trades websocket payload is empty")
                trade_ids = set()
                for trade in trades:
                    if not isinstance(trade, dict):
                        raise ValueError("trades websocket payload is malformed")
                    trade_id = trade.get("id")
                    trade_time = trade.get("timestamp")
                    if (
                        not isinstance(trade_id, str)
                        or not trade_id
                        or trade_id in trade_ids
                        or isinstance(trade_time, bool)
                        or not isinstance(trade_time, int)
                    ):
                        raise ValueError("trades websocket identity is invalid")
                    trade_ids.add(trade_id)
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
                    _positive_number(trade.get("price"), "trade price")
                    _positive_number(trade.get("amount"), "trade amount")
                    if trade.get("side") not in {"buy", "sell"}:
                        raise ValueError("trade side is invalid")
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
    finally:
        connection.close()
    if not event_count:
        raise ValueError(f"{stream} partition is empty")
    return {
        "path": f"{stream}/{path.name}",
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "events": event_count,
        "warnings": sorted(warnings),
    }, {
        "samples": dict(samples),
        "overview": overview_rows,
        "websocket_trade_events": websocket_trade_events,
        "connection_epochs": sorted(connection_epochs),
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
        symbol_to_market = {
            str(item.get("symbol")): str(market_id)
            for market_id, item in markets.items()
            if isinstance(item, dict) and item.get("symbol")
        }
        valid_symbols = [symbol for symbol in universe if isinstance(symbol, str)]
        selected = {
            symbol_to_market[symbol]
            for symbol in valid_symbols
            if symbol in symbol_to_market
        }
        if (
            len(valid_symbols) != len(universe)
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
        samples = by_market.get(market, [])
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
    root = Path(root)
    if _linklike(root):
        raise ValueError("capture root is linked")
    resolved_root = root.resolve()
    if day >= datetime.now(timezone.utc).date():
        raise ValueError("capture day is not closed")
    issues = []
    availability_gaps = []
    manifest = []
    stream_data = {}
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
            entry, data = _partition_rows(path, stream, day)
        except (OSError, sqlite3.Error, ValueError) as exc:
            issues.append(f"{stream}:{type(exc).__name__}:{str(exc)[:160]}")
            continue
        manifest.append(entry)
        stream_data[stream] = data
    if "overview" in stream_data:
        intervals, overview_issues, overview_gaps = _membership_intervals(
            stream_data["overview"]["overview"], day
        )
        issues.extend(overview_issues)
        availability_gaps.extend(overview_gaps)
    else:
        intervals = {}
    rest_gap = timedelta(
        seconds=max(
            30.0,
            float(micro_interval_seconds) * max(1, int(max_symbols)) * 2.5,
        )
    )
    l2_gap = timedelta(
        seconds=max(10.0, float(l2_sample_interval_seconds) * 10.0)
    )
    if intervals:
        if "depth" in stream_data:
            availability_gaps.extend(
                _coverage_gaps(
                    stream_data["depth"]["samples"],
                    intervals,
                    stream="depth",
                    maximum_gap=rest_gap,
                )
            )
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
    if intervals:
        if "l2_stream" in stream_data:
            availability_gaps.extend(
                _coverage_gaps(
                    stream_data["l2_stream"]["samples"],
                    intervals,
                    stream="l2_stream",
                    maximum_gap=l2_gap,
                )
            )
        # Public trades are event-driven.  Silence is not proof of transport
        # loss, so it must never invent an outage for a low-activity market.
        # Shared connection epochs remain visible in the availability report;
        # actual outage windows are established by periodic overview/depth and
        # the sampled L2 stream.
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
        "universe_contract": "persisted_point_in_time_dynamic",
        "sequence_contract": "l2_sequence_unverified_snapshot_only",
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
    if _linklike(path.parent) or _linklike(path):
        raise RuntimeError("capture integrity report path is linked")
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if path.exists():
        existing = path.read_bytes()
        if existing != encoded:
            raise RuntimeError("sealed capture report changed")
        return
    temporary = path.parent / f".{path.name}.{os.getpid()}.tmp"
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != encoded:
                raise RuntimeError("sealed capture report changed")
    finally:
        temporary.unlink(missing_ok=True)


def _verify_sealed_report(root: Path, report: dict) -> None:
    if not isinstance(report, dict) or report.get("schema_version") != 1:
        raise RuntimeError("sealed capture report schema is invalid")
    status = report.get("status")
    if status not in {"valid", "usable_with_gaps", "invalid"}:
        raise RuntimeError("sealed capture report status is invalid")
    if status == "usable_with_gaps":
        availability = report.get("availability")
        if (
            not isinstance(availability, dict)
            or availability.get("contract") != "bounded_outage_exclusion_v1"
            or availability.get("within_daily_budget") is not True
            or not isinstance(availability.get("exclusion_intervals"), list)
        ):
            raise RuntimeError("sealed capture availability is invalid")
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
    for item in report.get("manifest") or ():
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise RuntimeError("sealed capture manifest is invalid")
        path = root / item["path"]
        try:
            relative = path.resolve().relative_to(root.resolve())
        except (OSError, ValueError) as exc:
            raise RuntimeError("sealed capture manifest escaped root") from exc
        if relative.as_posix() != item["path"] or _linklike(path):
            raise RuntimeError("sealed capture manifest path is invalid")
        if (
            not path.is_file()
            or path.stat().st_size != item.get("bytes")
            or _sha256(path) != item.get("sha256")
        ):
            raise RuntimeError("sealed capture partition drifted")


def _remove_empty_sidecars(root: Path, report: dict) -> None:
    for item in report.get("manifest") or ():
        path = root / item["path"]
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{path}{suffix}")
            if sidecar.exists() and sidecar.stat().st_size == 0:
                sidecar.unlink()


def seal_closed_capture_days(
    root: str | Path,
    *,
    max_symbols: int,
    micro_interval_seconds: float,
    l2_sample_interval_seconds: float,
) -> dict:
    root = Path(root)
    now = datetime.now(timezone.utc)
    today = now.date()
    days = sorted({
        parsed.date()
        for stream in CORE_STREAMS
        for path in (root / stream).glob("*.sqlite3")
        if (parsed := _partition_date(path)) is not None
        and parsed.date() < today
        and parsed + timedelta(days=1) + SEAL_GRACE <= now
    })
    reports = []
    for day in days:
        report_path = root / "integrity" / f"{day.isoformat()}.json"
        if report_path.exists():
            report = _json(report_path.read_text(encoding="utf-8"))
            _verify_sealed_report(root, report)
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
    return {
        "ok": not invalid,
        "sealed_days": len(reports),
        "valid_days": len(strict_valid),
        "usable_days": len(strict_valid) + len(degraded),
        "degraded_days": degraded,
        "invalid_days": invalid,
        "latest_day": reports[-1]["day"] if reports else None,
        "continuity": capture_continuity_health(reports),
    }


def _partition_date(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.stem, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
