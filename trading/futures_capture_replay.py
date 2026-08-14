"""Reproducible 150-second FUTURES replay from MEXC overview captures.

Overview observations define the historical point-in-time universe, prices,
spreads and funding.  Native 15m/1h/4h candles define indicators only after a
candle has closed.  The resulting index plugs into ``tools.backtester`` so the
existing position lifecycle and accounting remain authoritative.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import math
import os
import shutil
import sqlite3
import stat
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from bot_utils.indicators import atr, ema, macd_signal, rsi
from core.constants import (
    MAX_24H_PUMP_PCT,
    MIN_VOLUME_USDT_FUTURES_LIVE,
    NONCRYPTO_BASES,
    STOCK_TOKEN_BASES,
)
from trading.historical_futures_evidence import (
    HistoricalFundingTimeline,
    OverviewSnapshot,
    load_overview_snapshots,
)


REPLAY_DATASET_SCHEMA = 2
REPLAY_SCAN_SECONDS = 150
REPLAY_MAX_SNAPSHOT_AGE_SECONDS = 120
REPLAY_MANIFEST_MAX_BYTES = 16 * 1024 * 1024
_TIMEFRAME_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}


@dataclass(frozen=True)
class ReplayDataset:
    root: Path
    manifest: dict
    snapshots: tuple[OverviewSnapshot, ...]
    ohlcv: dict[tuple[str, str], tuple[tuple[float, ...], ...]]
    funding: HistoricalFundingTimeline


def _canonical_bytes(value) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, raw: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _normalized_bars(raw_rows, timeframe: str) -> list[list[float | int]]:
    interval = _TIMEFRAME_MS.get(timeframe)
    if interval is None:
        raise ValueError(f"unsupported replay timeframe: {timeframe}")
    rows = []
    previous = None
    for raw in raw_rows:
        if not isinstance(raw, (list, tuple)) or len(raw) < 6:
            raise ValueError("OHLCV row must contain timestamp/OHLCV")
        if isinstance(raw[0], bool):
            raise ValueError("OHLCV timestamp must be an integer")
        try:
            timestamp_number = float(raw[0])
            values = [float(value) for value in raw[1:6]]
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("OHLCV evidence must be numeric") from exc
        if (
            not math.isfinite(timestamp_number)
            or not timestamp_number.is_integer()
            or timestamp_number <= 0.0
            or any(not math.isfinite(value) for value in values)
        ):
            raise ValueError("OHLCV evidence must be finite")
        timestamp = int(timestamp_number)
        if previous is not None and timestamp - previous != interval:
            raise ValueError(f"{timeframe} OHLCV must be contiguous")
        opening, high, low, close, volume = values
        if (
            min(opening, high, low, close) <= 0.0
            or volume < 0.0
            or low > min(opening, close)
            or high < max(opening, close)
        ):
            raise ValueError("OHLCV candle geometry is invalid")
        rows.append([timestamp, opening, high, low, close, volume])
        previous = timestamp
    if len(rows) < 60:
        raise ValueError(f"{timeframe} OHLCV requires at least 60 bars")
    return rows


def _normalized_funding(raw_rows) -> list[list[float | int]]:
    by_timestamp = {}
    for raw in raw_rows:
        if not isinstance(raw, (list, tuple)) or len(raw) < 2:
            raise ValueError("funding row must contain timestamp and rate")
        if isinstance(raw[0], bool) or isinstance(raw[1], bool):
            raise ValueError("funding timestamp/rate must be numeric")
        try:
            timestamp_number = float(raw[0])
            rate = float(raw[1])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("funding evidence must be numeric") from exc
        if (
            not math.isfinite(timestamp_number)
            or not timestamp_number.is_integer()
            or timestamp_number <= 0.0
            or not math.isfinite(rate)
        ):
            raise ValueError("funding evidence must be finite")
        by_timestamp[int(timestamp_number)] = rate
    rows = [[timestamp, by_timestamp[timestamp]] for timestamp in sorted(by_timestamp)]
    if len(rows) < 2:
        raise ValueError("funding history requires at least two settlements")
    if any(
        not 0 < right[0] - left[0] <= 12 * 3_600_000
        for left, right in zip(rows, rows[1:])
    ):
        raise ValueError("funding settlement history is incomplete")
    return rows


def freeze_replay_dataset(
    overview_partitions: Iterable[str | Path],
    ohlcv: dict[tuple[str, str], Iterable],
    funding_history: dict[str, Iterable],
    workspace_root: str | Path,
    *,
    venue: str,
    source: dict | None = None,
) -> Path:
    """Publish a byte-verified immutable replay dataset below ``datasets``."""
    normalized_venue = "".join(char for char in venue.lower() if char.isalnum())
    if normalized_venue != "mexc":
        raise ValueError("FUTURES capture replay currently requires venue=mexc")
    overview = sorted(Path(path).expanduser().resolve() for path in overview_partitions)
    if not overview:
        raise ValueError("overview_partitions must not be empty")
    workspace = Path(workspace_root).expanduser().resolve()
    datasets = workspace / "datasets"
    datasets.mkdir(parents=True, exist_ok=True)
    staging = datasets / f".replay-{os.getpid()}-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        overview_manifest = []
        overview_dir = staging / "overview"
        overview_dir.mkdir()
        for index, path in enumerate(overview):
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"overview partition is missing or linked: {path}")
            target = overview_dir / f"{index:04d}-{path.name}"
            source_connection = destination_connection = None
            try:
                source_connection = sqlite3.connect(
                    f"file:{path.as_posix()}?mode=ro",
                    uri=True,
                )
                destination_connection = sqlite3.connect(target)
                source_connection.backup(destination_connection)
                destination_connection.commit()
                integrity = destination_connection.execute(
                    "PRAGMA quick_check"
                ).fetchone()
                if integrity != ("ok",):
                    raise ValueError(f"overview partition failed quick_check: {path}")
            except sqlite3.Error as exc:
                raise ValueError(f"overview partition backup failed: {path}") from exc
            finally:
                if destination_connection is not None:
                    destination_connection.close()
                if source_connection is not None:
                    source_connection.close()
            overview_manifest.append({
                "path": target.relative_to(staging).as_posix(),
                "bytes": target.stat().st_size,
                "sha256": _sha256_file(target),
            })

        series_manifest = []
        series_dir = staging / "series"
        series_dir.mkdir()
        normalized_series = {}
        for key in sorted(ohlcv):
            if not isinstance(key, tuple) or len(key) != 2:
                raise ValueError("OHLCV keys must be (symbol, timeframe)")
            symbol, timeframe = key
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("OHLCV symbol must be a non-empty string")
            rows = _normalized_bars(ohlcv[key], timeframe)
            normalized_series[(symbol, timeframe)] = rows
            raw = _canonical_bytes(rows)
            identity = hashlib.sha256(f"{symbol}\0{timeframe}".encode()).hexdigest()
            relative = f"series/{identity}.json"
            target = staging / relative
            with target.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            series_manifest.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "path": relative,
                "bytes": len(raw),
                "sha256": _sha256_bytes(raw),
                "rows": len(rows),
                "first_ts": rows[0][0],
                "last_ts": rows[-1][0],
            })
        symbols = {item["symbol"] for item in series_manifest}
        for symbol in symbols:
            missing = set(_TIMEFRAME_MS) - {
                item["timeframe"]
                for item in series_manifest
                if item["symbol"] == symbol
            }
            if missing:
                raise ValueError(
                    f"{symbol} is missing replay timeframes: {', '.join(sorted(missing))}"
                )
        if not isinstance(funding_history, dict) or not funding_history:
            raise ValueError("funding_history must be a non-empty symbol mapping")
        funding_manifest = []
        funding_dir = staging / "funding"
        funding_dir.mkdir()
        for symbol in sorted(funding_history):
            if symbol not in symbols:
                raise ValueError(f"funding symbol has no OHLCV evidence: {symbol}")
            rows = _normalized_funding(funding_history[symbol])
            raw = _canonical_bytes(rows)
            identity = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
            relative = f"funding/{identity}.json"
            target = staging / relative
            with target.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            funding_manifest.append({
                "symbol": symbol,
                "path": relative,
                "bytes": len(raw),
                "sha256": _sha256_bytes(raw),
                "rows": len(rows),
                "first_ts": rows[0][0],
                "last_ts": rows[-1][0],
            })
        funding_symbols = {item["symbol"] for item in funding_manifest}
        if funding_symbols != symbols:
            missing = sorted(symbols - funding_symbols)
            raise ValueError(
                "missing funding history for OHLCV symbols: "
                + ", ".join(missing[:5])
            )
        payload = {
            "schema_version": REPLAY_DATASET_SCHEMA,
            "kind": "mexc_futures_capture_replay",
            "venue": normalized_venue,
            "scan_interval_seconds": REPLAY_SCAN_SECONDS,
            "overview": overview_manifest,
            "series": series_manifest,
            "funding": funding_manifest,
        }
        fingerprint = _sha256_bytes(_canonical_bytes(payload))
        manifest = {
            "dataset_fingerprint": fingerprint,
            "fingerprint_payload": payload,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": source or {},
        }
        _atomic_write(staging / "dataset_manifest.json", _canonical_bytes(manifest))
        final = datasets / fingerprint
        if final.exists():
            verify_replay_dataset(final)
            return final
        os.replace(staging, final)
        for path in final.rglob("*"):
            if path.is_file():
                try:
                    path.chmod(stat.S_IREAD)
                except OSError:
                    pass
        return final
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _read_manifest(path: Path) -> dict:
    with path.open("rb") as handle:
        raw = handle.read(REPLAY_MANIFEST_MAX_BYTES + 1)
    if len(raw) > REPLAY_MANIFEST_MAX_BYTES:
        raise ValueError("replay manifest is oversized")
    try:
        value = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("replay manifest is invalid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("replay manifest must be an object")
    return value


def verify_replay_dataset(dataset_root: str | Path) -> dict:
    root = Path(dataset_root).expanduser().resolve()
    if root.is_symlink() or not root.is_dir():
        raise ValueError("replay dataset root must be a real directory")
    manifest = _read_manifest(root / "dataset_manifest.json")
    payload = manifest.get("fingerprint_payload")
    fingerprint = manifest.get("dataset_fingerprint")
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != REPLAY_DATASET_SCHEMA
        or payload.get("kind") != "mexc_futures_capture_replay"
        or payload.get("venue") != "mexc"
        or payload.get("scan_interval_seconds") != REPLAY_SCAN_SECONDS
    ):
        raise ValueError("unsupported replay dataset contract")
    if fingerprint != _sha256_bytes(_canonical_bytes(payload)) or root.name != fingerprint:
        raise ValueError("replay dataset fingerprint mismatch")
    expected = {"dataset_manifest.json"}
    manifest_paths = set()
    series_identities = set()
    series_timeframes = {}
    funding_symbols = set()
    for group in ("overview", "series", "funding"):
        items = payload.get(group)
        if not isinstance(items, list) or not items:
            raise ValueError(f"replay dataset {group} must not be empty")
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                raise ValueError(f"invalid replay {group} manifest entry")
            relative = item["path"]
            if relative in manifest_paths:
                raise ValueError("replay dataset manifest paths must be unique")
            manifest_paths.add(relative)
            if group == "series":
                symbol = item.get("symbol")
                timeframe = item.get("timeframe")
                if (
                    not isinstance(symbol, str)
                    or not symbol.strip()
                    or timeframe not in _TIMEFRAME_MS
                ):
                    raise ValueError("invalid replay series identity")
                identity = (symbol, timeframe)
                if identity in series_identities:
                    raise ValueError("replay series identities must be unique")
                series_identities.add(identity)
                series_timeframes.setdefault(symbol, set()).add(timeframe)
                expected_name = hashlib.sha256(
                    f"{symbol}\0{timeframe}".encode()
                ).hexdigest()
                if relative != f"series/{expected_name}.json":
                    raise ValueError("replay series path conflicts with identity")
            elif group == "funding":
                symbol = item.get("symbol")
                if not isinstance(symbol, str) or not symbol.strip():
                    raise ValueError("invalid replay funding identity")
                if symbol in funding_symbols:
                    raise ValueError("replay funding identities must be unique")
                funding_symbols.add(symbol)
                expected_name = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
                if relative != f"funding/{expected_name}.json":
                    raise ValueError("replay funding path conflicts with identity")
            elif not relative.startswith("overview/"):
                raise ValueError("replay overview path conflicts with group")
            candidate = root / relative
            if candidate.is_symlink() or candidate.parent.is_symlink():
                raise ValueError(f"replay dataset file missing or linked: {relative}")
            path = candidate.resolve()
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise ValueError("replay dataset path escapes root") from exc
            if not path.is_file():
                raise ValueError(f"replay dataset file missing: {relative}")
            if path.stat().st_size != item.get("bytes") or _sha256_file(path) != item.get("sha256"):
                raise ValueError(f"replay dataset file fingerprint mismatch: {relative}")
            expected.add(relative)
    required_timeframes = set(_TIMEFRAME_MS)
    if not series_timeframes or any(
        timeframes != required_timeframes
        for timeframes in series_timeframes.values()
    ):
        raise ValueError("replay dataset timeframe coverage is incomplete")
    if funding_symbols != set(series_timeframes):
        raise ValueError("replay funding symbols must match OHLCV symbols")
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    if actual != expected:
        raise ValueError("replay dataset contains unmanifested files")
    return manifest


def load_replay_dataset(dataset_root: str | Path) -> ReplayDataset:
    manifest = verify_replay_dataset(dataset_root)
    root = Path(dataset_root).expanduser().resolve()
    payload = manifest["fingerprint_payload"]
    overview_paths = [root / item["path"] for item in payload["overview"]]
    snapshots = tuple(load_overview_snapshots(overview_paths))
    if len(snapshots) < 2:
        raise ValueError("replay dataset requires at least two overview snapshots")
    ohlcv = {}
    for item in payload["series"]:
        with (root / item["path"]).open("rb") as handle:
            rows = json.loads(handle.read().decode("utf-8"))
        normalized = _normalized_bars(rows, item["timeframe"])
        if (
            len(normalized) != item["rows"]
            or normalized[0][0] != item["first_ts"]
            or normalized[-1][0] != item["last_ts"]
        ):
            raise ValueError(f"replay OHLCV metadata mismatch: {item['path']}")
        ohlcv[(item["symbol"], item["timeframe"])] = tuple(
            tuple(row) for row in normalized
        )
    funding_history = {}
    for item in payload["funding"]:
        with (root / item["path"]).open("rb") as handle:
            rows = json.loads(handle.read().decode("utf-8"))
        normalized = _normalized_funding(rows)
        if (
            len(normalized) != item["rows"]
            or normalized[0][0] != item["first_ts"]
            or normalized[-1][0] != item["last_ts"]
        ):
            raise ValueError(f"replay funding metadata mismatch: {item['path']}")
        funding_history[item["symbol"]] = tuple(
            (
                datetime.fromtimestamp(row[0] / 1000.0, tz=timezone.utc),
                row[1],
            )
            for row in normalized
        )
    return ReplayDataset(
        root=root,
        manifest=manifest,
        snapshots=snapshots,
        ohlcv=ohlcv,
        funding=HistoricalFundingTimeline.from_settled_history(funding_history),
    )


def _indicator_panel(rows, timeframe: str) -> tuple[list[datetime], list[dict]]:
    frame = pd.DataFrame(
        rows, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    rsi_values = rsi(frame["close"], length=14)
    macd_values = macd_signal(frame["close"], fast=12, slow=26, signal=9)
    atr_values = atr(frame["high"], frame["low"], frame["close"], length=14)
    ema_values = ema(frame["close"], length=50)
    vol_average = frame["volume"].shift(1).rolling(20, min_periods=20).mean()
    interval = _TIMEFRAME_MS[timeframe]
    times = []
    values = []
    for index, row in frame.iterrows():
        available = datetime.fromtimestamp(
            (int(row["timestamp"]) + interval) / 1000.0,
            tz=timezone.utc,
        )
        close = float(row["close"])
        candle_range = float(row["high"] - row["low"])
        body = abs(float(row["close"] - row["open"]))
        average = vol_average.iloc[index]
        surge = (
            float(row["volume"] / average)
            if pd.notna(average) and float(average) > 0.0
            else 1.0
        )
        current_ema = ema_values.iloc[index]
        current_atr = atr_values.iloc[index]
        values.append({
            "rsi": float(rsi_values.iloc[index]) if pd.notna(rsi_values.iloc[index]) else None,
            "macd_h": float(macd_values.iloc[index]) if pd.notna(macd_values.iloc[index]) else None,
            "atr_pct": float(current_atr / close * 100.0) if pd.notna(current_atr) else None,
            "ema_ratio": float(close / current_ema - 1.0) * 100.0 if pd.notna(current_ema) and current_ema > 0.0 else None,
            "vol_surge": surge,
            "body_ratio": body / candle_range if candle_range > 0.0 else 1.0,
            "candle_dir": -1.0 if row["close"] < row["open"] else (1.0 if row["close"] > row["open"] else 0.0),
        })
        times.append(available)
    return times, values


def _asof(times: list[datetime], values: list[dict], when: datetime) -> dict | None:
    index = bisect.bisect_right(times, when) - 1
    return values[index] if index >= 0 else None


def _is_crypto_symbol(symbol: str) -> bool:
    base = symbol.split("/", 1)[0].upper()
    return (
        base not in NONCRYPTO_BASES
        and base not in STOCK_TOKEN_BASES
        and not base.endswith("STOCK")
    )


def build_capture_replay_index(
    dataset: ReplayDataset,
    *,
    min_pump: float,
    maximum_snapshot_age_seconds: int = REPLAY_MAX_SNAPSHOT_AGE_SECONDS,
) -> tuple[dict, list, dict]:
    """Create a sparse 150-second backtester index without future evidence."""
    if not math.isfinite(float(min_pump)) or float(min_pump) <= 0.0:
        raise ValueError("min_pump must be finite and positive")
    snapshots = dataset.snapshots
    available_times = [snapshot.received_time for snapshot in snapshots]
    start = available_times[0] + timedelta(hours=24)
    end = available_times[-1]
    anchor = available_times[0]
    elapsed = max(0.0, (start - anchor).total_seconds())
    step = REPLAY_SCAN_SECONDS
    first_step = math.ceil(elapsed / step)
    scan = anchor + timedelta(seconds=first_step * step)
    scan_times = []
    while scan <= end:
        scan_times.append(scan)
        scan += timedelta(seconds=step)

    indicator_panels = {}
    incomplete_funding_symbols = []
    symbols = sorted({key[0] for key in dataset.ohlcv})
    for symbol in symbols:
        if not all((symbol, timeframe) in dataset.ohlcv for timeframe in _TIMEFRAME_MS):
            continue
        if not dataset.funding.has_complete_coverage(symbol, start, end):
            incomplete_funding_symbols.append(symbol)
            continue
        indicator_panels[symbol] = {
            timeframe: _indicator_panel(dataset.ohlcv[(symbol, timeframe)], timeframe)
            for timeframe in _TIMEFRAME_MS
        }

    indexed = {symbol: {} for symbol in indicator_panels}
    usable_scans = stale_scans = 0
    market_observations = 0
    prior_scan = None
    snapshot_cursor = 0
    window_snapshots: list[OverviewSnapshot] = []
    for when in scan_times:
        while snapshot_cursor < len(snapshots) and available_times[snapshot_cursor] <= when:
            snapshot = snapshots[snapshot_cursor]
            if prior_scan is None or snapshot.received_time > prior_scan:
                window_snapshots.append(snapshot)
            snapshot_cursor += 1
        latest_index = bisect.bisect_right(available_times, when) - 1
        if latest_index < 0:
            prior_scan = when
            window_snapshots.clear()
            continue
        latest = snapshots[latest_index]
        if (when - latest.received_time).total_seconds() > maximum_snapshot_age_seconds:
            stale_scans += 1
            prior_scan = when
            window_snapshots.clear()
            continue
        prior_index = bisect.bisect_right(
            available_times, when - timedelta(hours=24)
        ) - 1
        prior = snapshots[prior_index] if prior_index >= 0 else None
        if prior is None or abs(
            (when - timedelta(hours=24) - prior.received_time).total_seconds()
        ) > maximum_snapshot_age_seconds:
            prior_scan = when
            window_snapshots.clear()
            continue
        usable_scans += 1
        btc_now = latest.markets.get("BTC/USDT:USDT")
        btc_prior = prior.markets.get("BTC/USDT:USDT")
        btc_change = (
            (btc_now.last / btc_prior.last - 1.0) * 100.0
            if btc_now is not None and btc_prior is not None
            else None
        )
        for symbol, panels in indicator_panels.items():
            market = latest.markets.get(symbol)
            old_market = prior.markets.get(symbol)
            if market is None or old_market is None or not _is_crypto_symbol(symbol):
                continue
            market_observations += 1
            prices = [
                snap.markets[symbol].last
                for snap in window_snapshots
                if symbol in snap.markets
            ]
            last = market.last
            change = (last / old_market.last - 1.0) * 100.0
            tf_values = {
                timeframe: _asof(*panels[timeframe], when)
                for timeframe in _TIMEFRAME_MS
            }
            one = tf_values["1h"]
            multi_ready = all(
                value is not None and value.get("rsi") is not None
                for value in tf_values.values()
            )
            is_candidate = bool(
                market.quote_volume >= MIN_VOLUME_USDT_FUTURES_LIVE
                and float(min_pump) <= abs(change) <= MAX_24H_PUMP_PCT
                and multi_ready
                and one is not None
                and all(
                    one.get(field) is not None
                    for field in (
                        "macd_h", "atr_pct", "ema_ratio", "vol_surge",
                        "body_ratio", "candle_dir",
                    )
                )
            )
            tick = {
                "price": last,
                "high": max(prices, default=last),
                "low": min(prices, default=last),
                "next_open": last,
                "long_entry_price": market.ask,
                "short_entry_price": market.bid,
                "change": change if is_candidate else None,
                "rsi": one["rsi"] if one else None,
                "macd_h": one["macd_h"] if one else None,
                "ema_ratio": one["ema_ratio"] if one else None,
                "vol_surge": one["vol_surge"] if one else None,
                "atr_pct": one["atr_pct"] if one else None,
                "body_ratio": one["body_ratio"] if one else None,
                "candle_dir": one["candle_dir"] if one else None,
                "rsi_15m": tf_values["15m"]["rsi"] if tf_values["15m"] else None,
                "rsi_4h": tf_values["4h"]["rsi"] if tf_values["4h"] else None,
                "btc_change": btc_change,
                "funding_rate_pct": (
                    market.funding_rate * 100.0
                    if market.funding_rate is not None else None
                ),
                "spread_pct": (
                    (market.ask - market.bid) / market.last * 100.0
                    if market.ask is not None and market.bid is not None
                    else None
                ),
                "scan_due": True,
            }
            # Missing executable quotes fail closed for entry, but the mark is
            # still retained so an already-open position can be monitored.
            indexed[symbol][when] = tick
        prior_scan = when
        window_snapshots.clear()
    indexed = {symbol: ticks for symbol, ticks in indexed.items() if ticks}
    report = {
        "dataset_fingerprint": dataset.manifest["dataset_fingerprint"],
        "scan_interval_seconds": REPLAY_SCAN_SECONDS,
        "scheduled_scans": len(scan_times),
        "usable_scans": usable_scans,
        "stale_scans": stale_scans,
        "symbols_with_mtf_history": len(indicator_panels),
        "symbols_excluded_incomplete_funding": len(incomplete_funding_symbols),
        "symbols_indexed": len(indexed),
        "market_observations": market_observations,
        "funding_evidence": dataset.funding.summary(start, end),
        "first_scan": scan_times[0].isoformat() if scan_times else None,
        "last_scan": scan_times[-1].isoformat() if scan_times else None,
        "causality": {
            "snapshot_selector": "latest received_time <= scan_time",
            "change_24h": "latest received snapshots at scan and scan-24h",
            "indicators": "native candle close <= scan_time",
            "historical_universe": True,
            "future_ticker_access": False,
        },
        "known_parity_gaps": [
            "historical database win-rate gates are not reconstructed",
            "open-interest history is unavailable in overview captures",
            "historical listing/active/RWA metadata predating recorder schema is unavailable",
            "intra-scan extrema use captured last prices, not continuous trades",
            "runtime risk/bad-hour/regime gates are not reconstructed",
        ],
    }
    return indexed, sorted({time for ticks in indexed.values() for time in ticks}), report
