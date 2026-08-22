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
    websocket_trade_samples: dict[str, list[datetime]] = defaultdict(list)
    connection_epochs = set()
    warnings = set()
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
                websocket_trade_samples[market_id].append(received_time)
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
    event_count = sum(len(values) for values in samples.values())
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
        "websocket_trade_samples": dict(websocket_trade_samples),
        "connection_epochs": sorted(connection_epochs),
    }


def _membership_intervals(overview: list[dict], day: date) -> tuple[dict, list[str]]:
    day_start, day_end = _day_bounds(day)
    issues = []
    overview = sorted(overview, key=lambda row: row["received_time"])
    if overview[0]["received_time"] > day_start + timedelta(minutes=3):
        issues.append("overview_start_boundary_gap")
    if overview[-1]["received_time"] < day_end - timedelta(minutes=3):
        issues.append("overview_end_boundary_gap")
    intervals: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
    active: dict[str, datetime] = {}
    previous_time = None
    for row in overview:
        current = row["received_time"]
        if previous_time is not None and current - previous_time > timedelta(minutes=3):
            issues.append("overview_cadence_gap")
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
    return dict(intervals), sorted(set(issues))


def _coverage_issues(
    by_market: dict[str, list[datetime]],
    intervals: dict[str, list[tuple[datetime, datetime]]],
    *,
    stream: str,
    maximum_gap: timedelta,
) -> list[str]:
    issues = []
    for market, expected in intervals.items():
        samples = by_market.get(market, [])
        for started, ended in expected:
            if ended - started < maximum_gap:
                continue
            selected = [value for value in samples if started <= value <= ended]
            if not selected:
                issues.append(f"{stream}:{market}:missing_interval")
                continue
            if selected[0] - started > maximum_gap:
                issues.append(f"{stream}:{market}:start_gap")
            if ended - selected[-1] > maximum_gap:
                issues.append(f"{stream}:{market}:end_gap")
            if any(
                right - left > maximum_gap
                for left, right in zip(selected, selected[1:])
            ):
                issues.append(f"{stream}:{market}:cadence_gap")
    return issues


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
        intervals, overview_issues = _membership_intervals(
            stream_data["overview"]["overview"], day
        )
        issues.extend(overview_issues)
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
            issues.extend(
                _coverage_issues(
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
    if len(observed_epochs) != 1:
        issues.append("transport_connection_epoch_not_stable")
    if intervals:
        if "l2_stream" in stream_data:
            issues.extend(
                _coverage_issues(
                    stream_data["l2_stream"]["samples"],
                    intervals,
                    stream="l2_stream",
                    maximum_gap=l2_gap,
                )
            )
        if "trades" in stream_data:
            issues.extend(
                _coverage_issues(
                    stream_data["trades"]["websocket_trade_samples"],
                    intervals,
                    stream="trades_ws",
                    maximum_gap=timedelta(minutes=5),
                )
            )
    issues = sorted(set(issues))
    result = {
        "schema_version": 1,
        "day": day.isoformat(),
        "status": "valid" if not issues else "invalid",
        "issues": issues,
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
            if report.get("status") == "valid":
                _remove_empty_sidecars(root, report)
            _atomic_create(report_path, report)
        reports.append(report)
    invalid = [report["day"] for report in reports if report.get("status") != "valid"]
    return {
        "ok": not invalid,
        "sealed_days": len(reports),
        "valid_days": len(reports) - len(invalid),
        "invalid_days": invalid,
        "latest_day": reports[-1]["day"] if reports else None,
    }


def _partition_date(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.stem, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
