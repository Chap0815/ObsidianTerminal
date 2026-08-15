"""Fail-closed cost and temporal validation for immutable capture replays."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from core.constants import SIM_CAPTURE_CONTRACT_SCHEMA
from trading.execution_cost_model import (
    ExecutionCostObservation,
    decode_execution_cost_payload,
    execution_cost_payloads_match,
    execution_cost_stages_are_causal,
    execution_cost_volatility_bps,
    normalize_execution_cost_minimum_samples,
    resolve_execution_cost_notional,
    validate_execution_cost_arrival,
    validate_execution_cost_fill,
)


COST_EVIDENCE_SCHEMA = 5
SNAPSHOT_FINGERPRINT_SCHEMA = 1
DEFAULT_MARKOUT_HORIZONS = (1, 10, 60, 300, 900)
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
MIN_COST_EVIDENCE_SAMPLES = 50
MIN_COST_EVIDENCE_COVERAGE = 0.95


def _canonical_bytes(value) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _snapshot_sha256(
    tca_rows,
    markout_rows,
    *,
    tca_orphan_entries: int,
    markout_orphan_entries: int,
    maximum_tca,
    maximum_markout,
) -> str:
    """Hash the exact ordered SQLite snapshot with unambiguous framing."""
    digest = hashlib.sha256()

    def add(value) -> None:
        encoded = _canonical_bytes(value)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)

    add({
        "schema_version": SNAPSHOT_FINGERPRINT_SCHEMA,
        "tca_columns": [
            "tca_id",
            "entry_id",
            "measured_at",
            "stage",
            "payload_json",
            "bot_name",
            "symbol",
            "candidate_time",
        ],
        "markout_columns": [
            "horizon_seconds",
            "status",
            "markout_bps",
            "entry_id",
            "due_at",
            "measured_at",
            "symbol",
            "side",
            "reference_price",
        ],
    })
    for row in tca_rows:
        add(["tca", list(row)])
    for row in markout_rows:
        add(["markout", list(row)])
    add({
        "tca_orphan_entries": int(tca_orphan_entries),
        "markout_orphan_entries": int(markout_orphan_entries),
        "maximum_tca": list(maximum_tca),
        "maximum_markout": list(maximum_markout),
    })
    return digest.hexdigest()


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_json_constant(value: str):
    raise ValueError(f"non-standard JSON constant: {value}")


def _finite(value, name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(number) or (minimum is not None and number < minimum):
        raise ValueError(f"{name} is out of range")
    return number


def _evidence_count(value, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"cost evidence {name} is inconsistent")
    return value


def _evidence_coverage(value, name: str) -> float:
    if type(value) not in {int, float}:
        raise ValueError(f"cost evidence {name} is inconsistent")
    try:
        coverage = _finite(value, name, minimum=0.0)
    except ValueError as exc:
        raise ValueError(f"cost evidence {name} is inconsistent") from exc
    if coverage > 1.0:
        raise ValueError(f"cost evidence {name} is inconsistent")
    return coverage


def _evidence_rejection_total(value, name: str) -> int:
    if type(value) is not dict:
        raise ValueError(f"cost evidence {name} is inconsistent")
    total = 0
    for reason, count in value.items():
        if not isinstance(reason, str) or not reason:
            raise ValueError(f"cost evidence {name} is inconsistent")
        total += _evidence_count(count, name)
    return total


def _quantile(values: Iterable[float], probability: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: Path) -> Path:
    requested = path.expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError("evidence path must not contain links")
    return requested


def _read_connection(path: Path) -> sqlite3.Connection:
    try:
        requested = _absolute_without_links(path)
    except ValueError as exc:
        raise FileNotFoundError(path) from exc
    if not requested.is_file():
        raise FileNotFoundError(requested)
    resolved = requested.resolve(strict=True)
    connection = sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _validated_horizons(values: Iterable[int]) -> tuple[int, ...]:
    horizons = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("markout horizons must be positive integers")
        horizons.append(value)
    normalized = tuple(sorted(set(horizons)))
    if not normalized:
        raise ValueError("at least one markout horizon is required")
    return normalized


def _evidence_timestamp(value) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None


def _capture_contract_anchor(arrival: dict, fill: dict) -> datetime | None:
    """Return the current producer anchor, or None for an exact legacy pair."""
    fields = ("capture_contract_schema", "capture_anchor_utc")
    present = [field in payload for payload in (arrival, fill) for field in fields]
    if not any(present):
        return None
    if not all(present):
        raise ValueError("SIM capture contract is incomplete")
    schemas = (
        arrival["capture_contract_schema"],
        fill["capture_contract_schema"],
    )
    if any(type(value) is not int for value in schemas) or any(
        value != SIM_CAPTURE_CONTRACT_SCHEMA for value in schemas
    ):
        raise ValueError("SIM capture contract schema is unsupported")
    anchors = (arrival["capture_anchor_utc"], fill["capture_anchor_utc"])
    if anchors[0] != anchors[1]:
        raise ValueError("SIM capture contract anchors conflict")
    anchor = _evidence_timestamp(anchors[0])
    if (
        anchor is None
        or not isinstance(anchors[0], str)
        or anchor.strftime("%Y-%m-%d %H:%M:%S") != anchors[0]
    ):
        raise ValueError("SIM capture contract anchor is invalid")
    return anchor


def _capture_contract_signaled(payload: dict) -> bool:
    """Return whether one decoded stage claims any versioned contract field."""
    return any(
        field in payload
        for field in ("capture_contract_schema", "capture_anchor_utc")
    )


def _markout_horizon_summary(
    rows,
    *,
    horizon: int,
    valid_entry_ids: set[str],
    valid_fill_times: dict[str, object],
    expected_symbols: dict[str, object] | None = None,
    expected_sides: dict[str, object] | None = None,
    expected_reference_prices: dict[str, object] | None = None,
    validate_contract_rows: bool = False,
) -> tuple[dict, bool]:
    relevant = [row for row in rows if row["horizon_seconds"] == horizon]
    complete_by_entry = {}
    rejected = Counter()
    invalid_complete_entries = set()
    ambiguous_entries = set()
    if validate_contract_rows:
        scoped_counts = Counter(
            str(row["entry_id"])
            for row in relevant
            if str(row["entry_id"]) in valid_entry_ids
        )
        missing_entries = {
            entry_id for entry_id in valid_entry_ids if scoped_counts[entry_id] == 0
        }
        duplicate_entries = {
            entry_id for entry_id, count in scoped_counts.items() if count > 1
        }
        ambiguous_entries = missing_entries | duplicate_entries
        if missing_entries:
            rejected["missing_markout_row"] += len(missing_entries)
        if duplicate_entries:
            rejected["duplicate_markout_row"] += len(duplicate_entries)
    for row in relevant:
        entry_id = str(row["entry_id"])
        if entry_id not in valid_entry_ids or entry_id in ambiguous_entries:
            continue
        fill_at = _evidence_timestamp(valid_fill_times.get(entry_id))
        due_at = _evidence_timestamp(row["due_at"])
        if validate_contract_rows:
            raw_due_at = row["due_at"]
            try:
                reference = _finite(
                    row["reference_price"], "reference_price", minimum=0.0
                )
            except ValueError:
                reference = 0.0
            expected_symbol = (
                expected_symbols.get(entry_id)
                if isinstance(expected_symbols, dict)
                else None
            )
            expected_side = (
                expected_sides.get(entry_id)
                if isinstance(expected_sides, dict)
                else None
            )
            try:
                expected_reference = _finite(
                    expected_reference_prices.get(entry_id),
                    "expected_reference_price",
                    minimum=0.0,
                )
            except (AttributeError, ValueError):
                expected_reference = 0.0
            side = row["side"]
            status = row["status"]
            if status not in {"PENDING", "COMPLETE", "FAILED"}:
                rejected["invalid_markout_status"] += 1
                continue
            if (
                not isinstance(row["symbol"], str)
                or row["symbol"] != expected_symbol
                or not isinstance(side, str)
                or side not in {"buy", "sell"}
                or side != expected_side
                or reference <= 0.0
                or reference != expected_reference
            ):
                rejected["invalid_markout_identity"] += 1
                continue
            if (
                fill_at is None
                or due_at is None
                or not isinstance(raw_due_at, str)
                or due_at.strftime("%Y-%m-%d %H:%M:%S") != raw_due_at
                or due_at < fill_at + timedelta(seconds=horizon)
            ):
                rejected["noncausal_markout_schedule"] += 1
                continue
        if row["status"] != "COMPLETE":
            continue
        if entry_id in complete_by_entry or entry_id in invalid_complete_entries:
            complete_by_entry.pop(entry_id, None)
            invalid_complete_entries.add(entry_id)
            rejected["duplicate_complete_markout"] += 1
            continue
        try:
            markout = _finite(row["markout_bps"], "markout_bps")
        except ValueError:
            invalid_complete_entries.add(entry_id)
            rejected["invalid_complete_markout"] += 1
            continue
        measured_at = _evidence_timestamp(row["measured_at"])
        if (
            fill_at is None
            or due_at is None
            or measured_at is None
            or due_at < fill_at + timedelta(seconds=horizon)
            or measured_at < due_at
        ):
            invalid_complete_entries.add(entry_id)
            rejected["noncausal_complete_markout"] += 1
            continue
        complete_by_entry[entry_id] = markout
    paired = len(valid_entry_ids)
    complete_entries = len(complete_by_entry)
    coverage = complete_entries / paired if paired else 0.0
    signed = list(complete_by_entry.values())
    return ({
        "rows": len(relevant),
        "complete_entries": complete_entries,
        "coverage": coverage,
        "median_signed_bps": _quantile(signed, 0.50),
        "p95_adverse_bps": _quantile(
            (max(0.0, -value) for value in signed), 0.95
        ),
        "rejected": dict(sorted(rejected.items())),
    }, not rejected)


def collect_sim_cost_evidence(
    runtime_root: str | Path,
    *,
    minimum_samples: int = 50,
    minimum_quality_coverage: float = 0.95,
    minimum_markout_coverage: float = 0.95,
    required_horizons: Iterable[int] = DEFAULT_MARKOUT_HORIZONS,
) -> dict:
    """Read one consistent SIM-TCA/markout evidence view without DB writes."""
    required = max(
        MIN_COST_EVIDENCE_SAMPLES,
        normalize_execution_cost_minimum_samples(minimum_samples),
    )
    quality_floor = max(
        MIN_COST_EVIDENCE_COVERAGE,
        _finite(
            minimum_quality_coverage, "minimum_quality_coverage", minimum=0.0
        ),
    )
    markout_floor = max(
        MIN_COST_EVIDENCE_COVERAGE,
        _finite(
            minimum_markout_coverage, "minimum_markout_coverage", minimum=0.0
        ),
    )
    if quality_floor > 1.0 or markout_floor > 1.0:
        raise ValueError("coverage thresholds must be in [0, 1]")
    horizons = _validated_horizons(required_horizons)
    try:
        root = _absolute_without_links(Path(runtime_root))
    except ValueError as exc:
        raise ValueError(
            "runtime root must be a real directory without links"
        ) from exc
    if not root.is_dir():
        raise ValueError("runtime root must be a real directory without links")
    database = root / "data" / "trading_bot.db"

    connection = _read_connection(database)
    try:
        connection.execute("BEGIN")
        required_tables = {
            "expectancy_candidates",
            "sim_execution_tca",
            "sim_execution_markouts",
        }
        if any(not _table_exists(connection, table) for table in required_tables):
            raise ValueError("SIM execution evidence tables are incomplete")
        tca_rows = connection.execute(
            """SELECT t.id AS tca_id, t.entry_id, t.measured_at,
                      t.stage, t.payload_json, e.bot_name, e.symbol,
                      e.candidate_time
                 FROM sim_execution_tca t
                 JOIN expectancy_candidates e ON e.entry_id=t.entry_id
                WHERE e.mode='SIM' AND t.stage IN ('arrival','fill')
                ORDER BY t.id DESC"""
        ).fetchall()
        markout_rows = connection.execute(
            """SELECT m.horizon_seconds, m.status, m.markout_bps,
                      m.entry_id, m.due_at, m.measured_at, m.symbol,
                      m.side, m.reference_price
                 FROM sim_execution_markouts m
                 JOIN expectancy_candidates e ON e.entry_id=m.entry_id
                WHERE e.mode='SIM'
                ORDER BY m.entry_id, m.horizon_seconds"""
        ).fetchall()
        tca_orphan_entries = connection.execute(
            """SELECT COUNT(DISTINCT t.entry_id)
                 FROM sim_execution_tca t
                 LEFT JOIN expectancy_candidates e ON e.entry_id=t.entry_id
                WHERE e.entry_id IS NULL"""
        ).fetchone()[0]
        markout_orphan_entries = connection.execute(
            """SELECT COUNT(DISTINCT m.entry_id)
                 FROM sim_execution_markouts m
                 LEFT JOIN expectancy_candidates e ON e.entry_id=m.entry_id
                WHERE e.entry_id IS NULL"""
        ).fetchone()[0]
        maximum_tca = connection.execute(
            "SELECT MAX(id), MAX(measured_at) FROM sim_execution_tca"
        ).fetchone()
        maximum_markout = connection.execute(
            "SELECT MAX(rowid), MAX(measured_at) FROM sim_execution_markouts"
        ).fetchone()
        snapshot_sha256 = _snapshot_sha256(
            tca_rows,
            markout_rows,
            tca_orphan_entries=int(tca_orphan_entries),
            markout_orphan_entries=int(markout_orphan_entries),
            maximum_tca=maximum_tca,
            maximum_markout=maximum_markout,
        )
    finally:
        if connection.in_transaction:
            connection.rollback()
        connection.close()

    paired_payloads: dict[str, dict[str, tuple[dict, object, object]]] = defaultdict(dict)
    metadata: dict[str, tuple[object, object, object]] = {}
    invalid_payloads = set()
    contract_signaled_entries = set()
    conflicting_stages = set()
    duplicate_stages = set()
    stage_counts: dict[str, Counter] = defaultdict(Counter)
    for row in tca_rows:
        entry_id = str(row["entry_id"])
        stage = str(row["stage"])
        stage_counts[entry_id][stage] += 1
        if stage_counts[entry_id][stage] > 1:
            duplicate_stages.add(entry_id)
        metadata[entry_id] = (
            row["bot_name"], row["symbol"], row["candidate_time"]
        )
        try:
            payload = decode_execution_cost_payload(row["payload_json"])
        except ValueError:
            invalid_payloads.add(entry_id)
            continue
        if _capture_contract_signaled(payload):
            contract_signaled_entries.add(entry_id)
        existing = paired_payloads[entry_id].get(stage)
        if existing is not None:
            if not execution_cost_payloads_match(existing[0], payload):
                conflicting_stages.add(entry_id)
            continue
        paired_payloads[entry_id][stage] = (
            payload,
            row["tca_id"],
            row["measured_at"],
        )

    observations: list[ExecutionCostObservation] = []
    contract_observations: list[ExecutionCostObservation] = []
    valid_entry_ids = set()
    valid_fill_times = {}
    contract_valid_entry_ids = set()
    contract_valid_fill_times = {}
    contract_valid_symbols = {}
    contract_valid_sides = {}
    contract_valid_reference_prices = {}
    fee_components = []
    contract_component_count = 0
    signed_shortfalls = []
    rejected = Counter()
    contract_rejected = Counter()
    legacy_entries = 0
    unclassified_entries = 0
    if tca_orphan_entries:
        rejected["orphan_sim_tca_entry"] += int(tca_orphan_entries)
    bot_samples = Counter()
    for entry_id in stage_counts:
        stages = paired_payloads.get(entry_id, {})
        if entry_id in invalid_payloads:
            rejected["invalid_payload"] += 1
            if entry_id in contract_signaled_entries:
                contract_rejected["invalid_payload"] += 1
            else:
                unclassified_entries += 1
            continue
        arrival_stage = stages.get("arrival")
        fill_stage = stages.get("fill")
        if arrival_stage is None or fill_stage is None:
            rejected["incomplete_pair"] += 1
            if entry_id in contract_signaled_entries:
                contract_rejected["incomplete_pair"] += 1
            else:
                unclassified_entries += 1
            continue
        arrival, arrival_id, arrival_time = arrival_stage
        fill, fill_id, fill_time = fill_stage
        try:
            contract_anchor = _capture_contract_anchor(arrival, fill)
        except ValueError:
            rejected["invalid_capture_contract"] += 1
            contract_rejected["invalid_capture_contract"] += 1
            continue
        is_contract = contract_anchor is not None
        if not is_contract:
            legacy_entries += 1
        if entry_id in conflicting_stages:
            rejected["conflicting_sim_stage"] += 1
            if is_contract:
                contract_rejected["conflicting_sim_stage"] += 1
            continue
        if entry_id in duplicate_stages:
            rejected["duplicate_sim_stage"] += 1
            if is_contract:
                contract_rejected["duplicate_sim_stage"] += 1
            continue
        if not execution_cost_stages_are_causal(
            arrival_id=arrival_id,
            arrival_time=arrival_time,
            fill_id=fill_id,
            fill_time=fill_time,
        ):
            rejected["noncausal_pair"] += 1
            if is_contract:
                contract_rejected["noncausal_pair"] += 1
            continue
        bot_name, symbol, candidate_time = metadata[entry_id]
        candidate_at = _evidence_timestamp(candidate_time)
        arrival_at = _evidence_timestamp(arrival_time)
        fill_at = _evidence_timestamp(fill_time)
        if is_contract and (
            arrival_at != contract_anchor or fill_at != contract_anchor
        ):
            rejected["capture_contract_anchor_mismatch"] += 1
            contract_rejected["capture_contract_anchor_mismatch"] += 1
            continue
        if (
            candidate_at is None
            or arrival_at is None
            or candidate_at > arrival_at
        ):
            rejected["noncausal_candidate_anchor"] += 1
            if is_contract:
                contract_rejected["noncausal_candidate_anchor"] += 1
            continue
        try:
            validate_execution_cost_arrival(arrival)
            validate_execution_cost_fill(fill)
            observation = ExecutionCostObservation(
                symbol=symbol,
                total_cost_bps=fill["total_cost_bps"],
                spread_bps=arrival["spread_bps"],
                depth_coverage=arrival["depth_coverage"],
                notional_usdt=resolve_execution_cost_notional(None, arrival),
                regime=arrival.get("regime", "unknown"),
                volatility_bps=execution_cost_volatility_bps(arrival),
            )
        except (KeyError, TypeError, ValueError):
            rejected["invalid_observation"] += 1
            if is_contract:
                contract_rejected["invalid_observation"] += 1
            continue
        observations.append(observation)
        valid_entry_ids.add(entry_id)
        valid_fill_times[entry_id] = fill_time
        if is_contract:
            contract_valid_entry_ids.add(entry_id)
            contract_valid_fill_times[entry_id] = fill_time
            contract_valid_symbols[entry_id] = symbol
            contract_valid_sides[entry_id] = arrival.get("side")
            contract_valid_reference_prices[entry_id] = fill.get(
                "average_fill_price"
            )
            contract_observations.append(observation)
        bot_samples[str(bot_name)] += 1
        try:
            fee_components.append(_finite(fill["fee_bps"], "fee_bps", minimum=0.0))
            if is_contract:
                contract_component_count += 1
            signed_shortfalls.append(
                _finite(fill["shortfall_vs_mid_bps"], "shortfall_vs_mid_bps")
            )
        except (KeyError, ValueError):
            # Legacy total-cost observations remain useful, but component
            # coverage is exposed and gates calibrated profiles below.
            pass

    paired = len(valid_entry_ids)
    costs = [row.total_cost_bps for row in observations]
    spreads = [row.spread_bps for row in observations]
    symbol_samples = Counter(row.symbol for row in observations)
    total_cost_samples = len(observations) + sum(rejected.values())
    quality_coverage = (
        len(observations) / total_cost_samples if total_cost_samples else 0.0
    )
    component_coverage = (
        len(fee_components) / len(observations) if observations else 0.0
    )
    contract_valid_samples = len(contract_valid_entry_ids)
    contract_total_samples = contract_valid_samples + sum(
        contract_rejected.values()
    )
    contract_quality_coverage = (
        contract_valid_samples / contract_total_samples
        if contract_total_samples
        else 0.0
    )
    contract_component_coverage = (
        contract_component_count / contract_valid_samples
        if contract_valid_samples
        else 0.0
    )
    contract_costs = [row.total_cost_bps for row in contract_observations]
    markout_report = {}
    horizon_coverages = []
    markout_integrity_valid = True
    contract_markout_report = {}
    contract_horizon_coverages = []
    contract_markout_integrity_valid = True
    for horizon in horizons:
        summary, valid = _markout_horizon_summary(
            markout_rows,
            horizon=horizon,
            valid_entry_ids=valid_entry_ids,
            valid_fill_times=valid_fill_times,
        )
        markout_report[str(horizon)] = summary
        horizon_coverages.append(summary["coverage"])
        markout_integrity_valid = markout_integrity_valid and valid
        contract_summary, contract_valid = _markout_horizon_summary(
            markout_rows,
            horizon=horizon,
            valid_entry_ids=contract_valid_entry_ids,
            valid_fill_times=contract_valid_fill_times,
            expected_symbols=contract_valid_symbols,
            expected_sides=contract_valid_sides,
            expected_reference_prices=contract_valid_reference_prices,
            validate_contract_rows=True,
        )
        contract_summary["rows"] = sum(
            str(row["entry_id"]) in contract_valid_entry_ids
            and row["horizon_seconds"] == horizon
            for row in markout_rows
        )
        contract_markout_report[str(horizon)] = contract_summary
        contract_horizon_coverages.append(contract_summary["coverage"])
        contract_markout_integrity_valid = (
            contract_markout_integrity_valid and contract_valid
        )

    body = {
        "schema_version": COST_EVIDENCE_SCHEMA,
        "kind": "mexc_sim_execution_cost_calibration",
        "source": {
            "snapshot_schema_version": SNAPSHOT_FINGERPRINT_SCHEMA,
            "snapshot_tca_rows": len(tca_rows),
            "snapshot_markout_rows": len(markout_rows),
            "snapshot_sha256": snapshot_sha256,
            "max_tca_id": maximum_tca[0],
            "max_tca_measured_at": maximum_tca[1],
            "max_markout_rowid": maximum_markout[0],
            "max_markout_measured_at": maximum_markout[1],
        },
        "costs": {
            "paired_entries": int(paired),
            "valid_samples": len(observations),
            "total_samples": total_cost_samples,
            "quality_coverage": quality_coverage,
            "component_coverage": component_coverage,
            "minimum_samples": required,
            "minimum_quality_coverage": quality_floor,
            "median_cost_bps_per_side": _quantile(costs, 0.50),
            "p75_cost_bps_per_side": _quantile(costs, 0.75),
            "p95_cost_bps_per_side": _quantile(costs, 0.95),
            "median_fee_bps_per_side": _quantile(fee_components, 0.50),
            "p95_fee_bps_per_side": _quantile(fee_components, 0.95),
            "median_signed_slippage_bps_per_side": _quantile(
                signed_shortfalls, 0.50
            ),
            "p75_adverse_slippage_bps_per_side": _quantile(
                (max(0.0, value) for value in signed_shortfalls), 0.75
            ),
            "p95_adverse_slippage_bps_per_side": _quantile(
                (max(0.0, value) for value in signed_shortfalls), 0.95
            ),
            "median_spread_bps": _quantile(spreads, 0.50),
            "p95_spread_bps": _quantile(spreads, 0.95),
            "rejected": dict(sorted(rejected.items())),
            "bot_samples": dict(sorted(bot_samples.items())),
            "symbol_samples": dict(sorted(symbol_samples.items())),
            "capture_contract": {
                "schema_version": SIM_CAPTURE_CONTRACT_SCHEMA,
                "valid_samples": contract_valid_samples,
                "total_samples": contract_total_samples,
                "quality_coverage": contract_quality_coverage,
                "component_samples": contract_component_count,
                "component_coverage": contract_component_coverage,
                "p75_cost_bps_per_side": _quantile(contract_costs, 0.75),
                "p95_cost_bps_per_side": _quantile(contract_costs, 0.95),
                "legacy_entries_excluded": legacy_entries,
                "unclassified_entries_excluded": unclassified_entries,
                "rejected": dict(sorted(contract_rejected.items())),
            },
        },
        "markouts": {
            "required_horizons_seconds": list(horizons),
            "minimum_coverage": markout_floor,
            "minimum_observed_coverage": min(horizon_coverages, default=0.0),
            "orphan_entries": int(markout_orphan_entries),
            "integrity_valid": markout_integrity_valid,
            "by_horizon": markout_report,
            "capture_contract": {
                "schema_version": SIM_CAPTURE_CONTRACT_SCHEMA,
                "minimum_observed_coverage": min(
                    contract_horizon_coverages, default=0.0
                ),
                "integrity_valid": contract_markout_integrity_valid,
                "by_horizon": contract_markout_report,
            },
        },
    }
    cost_ready = bool(
        contract_valid_samples >= required
        and contract_quality_coverage >= quality_floor
        and contract_component_coverage >= quality_floor
    )
    markouts_ready = bool(
        contract_horizon_coverages
        and min(contract_horizon_coverages) >= markout_floor
        and markout_orphan_entries == 0
        and contract_markout_integrity_valid
    )
    body["readiness"] = {
        "cost_samples_ready": cost_ready,
        "markouts_ready": markouts_ready,
        "simulation_ready": cost_ready,
        "promotion_evidence_ready": cost_ready and markouts_ready,
        "classification": (
            "simulation_ready"
            if cost_ready and markouts_ready
            else ("exploratory_only" if cost_ready else "collect_more")
        ),
    }
    return {**body, "evidence_sha256": _sha256(body)}


def validate_cost_evidence(evidence: dict) -> dict:
    """Validate a sealed cost report and reconstruct every readiness decision."""
    if type(evidence) is not dict:
        raise ValueError("cost evidence contract is inconsistent")
    expected_hash = evidence.get("evidence_sha256")
    body = {
        key: value for key, value in evidence.items() if key != "evidence_sha256"
    }
    try:
        actual_hash = _sha256(body)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("cost evidence contract is inconsistent") from exc
    if (
        body.get("schema_version") != COST_EVIDENCE_SCHEMA
        or body.get("kind") != "mexc_sim_execution_cost_calibration"
        or not isinstance(expected_hash, str)
        or expected_hash != actual_hash
    ):
        raise ValueError("cost evidence integrity check failed")

    source = body.get("source")
    if type(source) is not dict:
        raise ValueError("cost evidence snapshot identity is inconsistent")
    _evidence_count(source.get("snapshot_tca_rows"), "snapshot_tca_rows")
    _evidence_count(source.get("snapshot_markout_rows"), "snapshot_markout_rows")
    snapshot_sha256 = source.get("snapshot_sha256")
    if (
        source.get("snapshot_schema_version") != SNAPSHOT_FINGERPRINT_SCHEMA
        or not isinstance(snapshot_sha256, str)
        or len(snapshot_sha256) != 64
        or any(character not in "0123456789abcdef" for character in snapshot_sha256)
    ):
        raise ValueError("cost evidence snapshot identity is inconsistent")

    costs = body.get("costs")
    markouts = body.get("markouts")
    readiness = body.get("readiness")
    if type(costs) is not dict or type(markouts) is not dict:
        raise ValueError("cost evidence contract is inconsistent")
    required_samples = _evidence_count(
        costs.get("minimum_samples"),
        "minimum_samples",
        minimum=MIN_COST_EVIDENCE_SAMPLES,
    )
    quality_floor = _evidence_coverage(
        costs.get("minimum_quality_coverage"), "minimum_quality_coverage"
    )
    if quality_floor < MIN_COST_EVIDENCE_COVERAGE:
        raise ValueError("cost evidence minimum_quality_coverage is inconsistent")
    contract_costs = costs.get("capture_contract")
    if (
        type(contract_costs) is not dict
        or contract_costs.get("schema_version") != SIM_CAPTURE_CONTRACT_SCHEMA
    ):
        raise ValueError("cost evidence capture contract is inconsistent")
    valid_samples = _evidence_count(
        contract_costs.get("valid_samples"), "valid_samples"
    )
    total_samples = _evidence_count(
        contract_costs.get("total_samples"), "total_samples"
    )
    component_samples = _evidence_count(
        contract_costs.get("component_samples"), "component_samples"
    )
    rejected_samples = _evidence_rejection_total(
        contract_costs.get("rejected"), "cost rejections"
    )
    _evidence_count(
        contract_costs.get("legacy_entries_excluded"), "legacy_entries_excluded"
    )
    _evidence_count(
        contract_costs.get("unclassified_entries_excluded"),
        "unclassified_entries_excluded",
    )
    quality_coverage = _evidence_coverage(
        contract_costs.get("quality_coverage"), "quality_coverage"
    )
    component_coverage = _evidence_coverage(
        contract_costs.get("component_coverage"), "component_coverage"
    )
    expected_quality = valid_samples / total_samples if total_samples else 0.0
    expected_components = (
        component_samples / valid_samples if valid_samples else 0.0
    )
    if (
        total_samples != valid_samples + rejected_samples
        or component_samples > valid_samples
        or quality_coverage != expected_quality
        or component_coverage != expected_components
    ):
        raise ValueError("cost evidence sample coverage is inconsistent")
    p75_raw = contract_costs.get("p75_cost_bps_per_side")
    p95_raw = contract_costs.get("p95_cost_bps_per_side")
    if valid_samples == 0:
        if p75_raw is not None or p95_raw is not None:
            raise ValueError("cost evidence quantiles are inconsistent")
    else:
        if type(p75_raw) not in {int, float} or type(p95_raw) not in {int, float}:
            raise ValueError("cost evidence quantiles are inconsistent")
        p75 = _finite(p75_raw, "p75 cost", minimum=0.0)
        p95 = _finite(p95_raw, "p95 cost", minimum=0.0)
        if p95 < p75:
            raise ValueError("cost evidence quantiles are inconsistent")

    raw_horizons = markouts.get("required_horizons_seconds")
    if type(raw_horizons) is not list or any(
        type(value) is not int or value <= 0 for value in raw_horizons
    ):
        raise ValueError("cost evidence markout horizons are inconsistent")
    horizons = tuple(raw_horizons)
    if not horizons or list(horizons) != sorted(set(horizons)):
        raise ValueError("cost evidence markout horizons are inconsistent")
    markout_floor = _evidence_coverage(
        markouts.get("minimum_coverage"), "minimum_markout_coverage"
    )
    if markout_floor < MIN_COST_EVIDENCE_COVERAGE:
        raise ValueError("cost evidence minimum_markout_coverage is inconsistent")
    orphan_entries = _evidence_count(
        markouts.get("orphan_entries"), "orphan_markout_entries"
    )
    if type(markouts.get("integrity_valid")) is not bool:
        raise ValueError("cost evidence markout integrity is inconsistent")
    contract_markouts = markouts.get("capture_contract")
    if (
        type(contract_markouts) is not dict
        or contract_markouts.get("schema_version")
        != SIM_CAPTURE_CONTRACT_SCHEMA
        or type(contract_markouts.get("integrity_valid")) is not bool
    ):
        raise ValueError("cost evidence markout contract is inconsistent")
    by_horizon = contract_markouts.get("by_horizon")
    expected_horizon_keys = {str(value) for value in horizons}
    if type(by_horizon) is not dict or set(by_horizon) != expected_horizon_keys:
        raise ValueError("cost evidence markout horizons are inconsistent")
    horizon_coverages = []
    horizon_integrity = True
    for horizon in horizons:
        summary = by_horizon[str(horizon)]
        if type(summary) is not dict:
            raise ValueError("cost evidence markout summary is inconsistent")
        rows = _evidence_count(summary.get("rows"), "markout rows")
        complete = _evidence_count(
            summary.get("complete_entries"), "complete markouts"
        )
        coverage = _evidence_coverage(
            summary.get("coverage"), "markout coverage"
        )
        rejected = _evidence_rejection_total(
            summary.get("rejected"), "markout rejections"
        )
        expected_coverage = complete / valid_samples if valid_samples else 0.0
        if rows < complete or complete > valid_samples or coverage != expected_coverage:
            raise ValueError("cost evidence markout coverage is inconsistent")
        horizon_coverages.append(coverage)
        horizon_integrity = horizon_integrity and rejected == 0
    minimum_markout_coverage = min(horizon_coverages)
    reported_minimum = _evidence_coverage(
        contract_markouts.get("minimum_observed_coverage"),
        "minimum_observed_markout_coverage",
    )
    if (
        reported_minimum != minimum_markout_coverage
        or contract_markouts.get("integrity_valid") is not horizon_integrity
    ):
        raise ValueError("cost evidence markout integrity is inconsistent")

    cost_ready = bool(
        valid_samples >= required_samples
        and quality_coverage >= quality_floor
        and component_coverage >= quality_floor
    )
    markouts_ready = bool(
        minimum_markout_coverage >= markout_floor
        and orphan_entries == 0
        and horizon_integrity
    )
    expected_readiness = {
        "cost_samples_ready": cost_ready,
        "markouts_ready": markouts_ready,
        "simulation_ready": cost_ready,
        "promotion_evidence_ready": cost_ready and markouts_ready,
        "classification": (
            "simulation_ready"
            if cost_ready and markouts_ready
            else ("exploratory_only" if cost_ready else "collect_more")
        ),
    }
    if readiness != expected_readiness:
        raise ValueError("cost evidence readiness is inconsistent")
    return body


def load_cost_evidence(path: str | Path) -> dict:
    try:
        evidence_path = _absolute_without_links(Path(path))
    except ValueError as exc:
        raise ValueError("cost evidence must be a real file") from exc
    if not evidence_path.is_file():
        raise ValueError("cost evidence must be a real file")
    with evidence_path.open("rb") as handle:
        raw = handle.read(MAX_EVIDENCE_BYTES + 1)
    if len(raw) > MAX_EVIDENCE_BYTES:
        raise ValueError("cost evidence is oversized")
    try:
        value = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"cost evidence is invalid JSON: {exc}") from exc
    validate_cost_evidence(value)
    return value


def calibrated_cost_profiles(evidence: dict, *, default_round_trip_bps: float) -> dict:
    """Create conservative all-in round-trip profiles from per-side SIM TCA."""
    default = _finite(default_round_trip_bps, "default_round_trip_bps", minimum=0.0)
    if default > 1_000.0:
        raise ValueError("default_round_trip_bps is out of range")
    body = validate_cost_evidence(evidence)
    expected_hash = evidence["evidence_sha256"]
    costs = body.get("costs")
    readiness = body.get("readiness")
    markouts = body.get("markouts")
    if (
        not isinstance(costs, dict)
        or not isinstance(readiness, dict)
        or not isinstance(markouts, dict)
    ):
        raise ValueError("cost evidence contract is incomplete")
    contract_costs = costs.get("capture_contract")
    contract_markouts = markouts.get("capture_contract")
    if (
        not isinstance(contract_costs, dict)
        or contract_costs.get("schema_version") != SIM_CAPTURE_CONTRACT_SCHEMA
        or not isinstance(contract_markouts, dict)
        or contract_markouts.get("schema_version") != SIM_CAPTURE_CONTRACT_SCHEMA
        or type(markouts.get("integrity_valid")) is not bool
        or type(contract_markouts.get("integrity_valid")) is not bool
    ):
        raise ValueError("cost evidence capture contract is incomplete")
    p75 = _finite(
        contract_costs.get("p75_cost_bps_per_side"),
        "p75 cost",
        minimum=0.0,
    )
    p95 = _finite(
        contract_costs.get("p95_cost_bps_per_side"),
        "p95 cost",
        minimum=0.0,
    )
    if p95 < p75:
        raise ValueError("cost evidence quantiles are inconsistent")
    normal = max(default, 2.0 * p75)
    stressed = max(normal, 2.0 * p95)
    if stressed > 1_000.0:
        raise ValueError("empirical cost profile is out of range")
    return {
        "method": "two_sided_sim_tca_quantiles",
        "default_round_trip_bps": default,
        "normal_round_trip_bps": normal,
        "stressed_round_trip_bps": stressed,
        "markouts_used_as": "coverage_and_adverse_selection_diagnostic_only",
        "markouts_added_to_cost": False,
        "simulation_ready": readiness.get("simulation_ready") is True,
        "promotion_evidence_ready": bool(
            readiness.get("promotion_evidence_ready") is True
            and contract_markouts.get("integrity_valid") is True
        ),
        "evidence_sha256": expected_hash,
    }


def _time_number(value) -> float:
    if isinstance(value, bool):
        raise ValueError("timestamps must be finite and chronological")
    try:
        if isinstance(value, (int, float)):
            number = float(value)
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            number = parsed.timestamp()
        else:
            parsed = value
            if isinstance(parsed, datetime) and parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            number = float(parsed.timestamp())
    except (AttributeError, TypeError, ValueError, OverflowError, OSError) as exc:
        raise ValueError("timestamps must be finite and chronological") from exc
    if not math.isfinite(number):
        raise ValueError("timestamps must be finite and chronological")
    return number


def _time_fingerprint(values: list) -> str:
    return _sha256([_time_number(value) for value in values])


def _bounds(values: list) -> dict:
    return {
        "start": values[0].isoformat() if hasattr(values[0], "isoformat") else values[0],
        "end": values[-1].isoformat() if hasattr(values[-1], "isoformat") else values[-1],
        "timestamps": len(values),
        "timestamps_sha256": _time_fingerprint(values),
    }


@dataclass(frozen=True)
class TemporalReplaySplits:
    initial_train: tuple
    walk_forward_tests: tuple[tuple, ...]
    final_holdout: tuple
    embargo_bars: int
    specification: dict


def build_temporal_replay_splits(
    all_times: Iterable,
    *,
    walk_forward_steps: int = 4,
    holdout_fraction: float = 0.20,
    embargo_bars: int = 24,
    minimum_test_bars: int = 10,
) -> TemporalReplaySplits:
    """Build expanding chronological tests plus a never-tuned final holdout."""
    times = list(all_times)
    if isinstance(walk_forward_steps, bool) or not isinstance(walk_forward_steps, int):
        raise ValueError("walk_forward_steps must be an integer")
    if walk_forward_steps < 2:
        raise ValueError("walk_forward_steps must be at least two")
    if isinstance(embargo_bars, bool) or not isinstance(embargo_bars, int) or embargo_bars < 1:
        raise ValueError("embargo_bars must be a positive integer")
    if isinstance(minimum_test_bars, bool) or not isinstance(minimum_test_bars, int) or minimum_test_bars < 1:
        raise ValueError("minimum_test_bars must be a positive integer")
    holdout = _finite(holdout_fraction, "holdout_fraction", minimum=0.0)
    if not 0.10 <= holdout <= 0.40:
        raise ValueError("holdout_fraction must be in [0.10, 0.40]")
    numbers = [_time_number(value) for value in times]
    if any(current <= previous for previous, current in zip(numbers, numbers[1:])):
        raise ValueError("timestamps must be unique and strictly increasing")
    holdout_start = math.floor(len(times) * (1.0 - holdout))
    tune_count = holdout_start - embargo_bars
    segment = tune_count // (walk_forward_steps + 1)
    if (
        segment < minimum_test_bars
        or segment <= embargo_bars
        or len(times) - holdout_start < minimum_test_bars
    ):
        raise ValueError("timeline is too short for requested causal splits")
    initial_train = tuple(times[: segment - embargo_bars])
    folds = []
    fold_specs = []
    for index in range(walk_forward_steps):
        raw_start = (index + 1) * segment
        raw_end = (index + 2) * segment if index < walk_forward_steps - 1 else tune_count
        train_end = raw_start - embargo_bars
        test = tuple(times[raw_start:raw_end])
        if train_end < minimum_test_bars or len(test) < minimum_test_bars:
            raise ValueError("timeline is too short after walk-forward embargo")
        folds.append(test)
        fold_specs.append({
            "step": index,
            "train": _bounds(times[:train_end]),
            "embargo": _bounds(times[train_end:raw_start]),
            "test": _bounds(list(test)),
        })
    final_holdout = tuple(times[holdout_start:])
    specification = {
        "method": "expanding_walk_forward_with_final_holdout_v1",
        "timeline": _bounds(times),
        "walk_forward_steps": walk_forward_steps,
        "holdout_fraction": holdout,
        "embargo_bars": embargo_bars,
        "initial_train": _bounds(list(initial_train)),
        "walk_forward": fold_specs,
        "final_training_window": _bounds(times[:tune_count]),
        "final_holdout_embargo": _bounds(times[tune_count:holdout_start]),
        "final_holdout": _bounds(list(final_holdout)),
    }
    return TemporalReplaySplits(
        initial_train=initial_train,
        walk_forward_tests=tuple(folds),
        final_holdout=final_holdout,
        embargo_bars=embargo_bars,
        specification=specification,
    )


def _entry_regime(indexed: dict, symbol: str, entry_time) -> str:
    tick = indexed.get(symbol, {}).get(entry_time)
    if not isinstance(tick, dict):
        return "UNKNOWN"
    try:
        change = _finite(tick.get("btc_change"), "btc_change")
    except ValueError:
        return "UNKNOWN"
    if change > 3.0:
        return "BULL"
    if change < -3.0:
        return "BEAR"
    return "CHOP"


def analyze_independent_positions(
    stats: dict,
    *,
    indexed: dict,
    allowed_times: Iterable,
    maximum_symbol_profit_share: float = 0.25,
    maximum_regime_profit_share: float = 0.80,
) -> dict:
    """Validate fill grouping and expose sample/profit concentration."""
    symbol_limit = _finite(
        maximum_symbol_profit_share, "maximum_symbol_profit_share", minimum=0.0
    )
    regime_limit = _finite(
        maximum_regime_profit_share, "maximum_regime_profit_share", minimum=0.0
    )
    if symbol_limit > 1.0 or regime_limit > 1.0:
        raise ValueError("concentration limits must be in [0, 1]")
    trades = stats.get("closed_trades") if isinstance(stats, dict) else None
    if not isinstance(trades, list):
        return {"evidence_valid": False, "reason": "closed_trades_missing"}
    allowed = set(allowed_times)
    groups = defaultdict(list)
    for row in trades:
        if not isinstance(row, dict):
            return {"evidence_valid": False, "reason": "trade_fragment_invalid"}
        position_id = row.get("position_id")
        if isinstance(position_id, bool) or not isinstance(position_id, int) or position_id < 1:
            return {"evidence_valid": False, "reason": "position_id_invalid"}
        groups[position_id].append(row)
    positions = []
    for position_id, fragments in groups.items():
        terminal = [row for row in fragments if row.get("is_partial") is False]
        if (
            len(terminal) != 1
            or fragments[-1] is not terminal[0]
            or any(
                row.get("is_partial") is not (index < len(fragments) - 1)
                for index, row in enumerate(fragments)
            )
        ):
            return {"evidence_valid": False, "reason": "position_fragments_incomplete"}
        symbol = fragments[0].get("symbol")
        side = fragments[0].get("side")
        entry_time = fragments[0].get("entry_time")
        exit_time = terminal[0].get("exit_time")
        if (
            not isinstance(symbol, str)
            or not symbol
            or side not in {"LONG", "SHORT"}
            or any(
                row.get("symbol") != symbol
                or row.get("side") != side
                or row.get("entry_time") != entry_time
                or row.get("exit_time") not in allowed
                for row in fragments
            )
            or entry_time not in allowed
            or exit_time not in allowed
        ):
            return {"evidence_valid": False, "reason": "position_boundary_or_identity_invalid"}
        try:
            entry_number = _time_number(entry_time)
            exit_numbers = [_time_number(row.get("exit_time")) for row in fragments]
        except ValueError:
            return {"evidence_valid": False, "reason": "position_chronology_invalid"}
        if (
            any(exit_number < entry_number for exit_number in exit_numbers)
            or any(
                current <= previous
                for previous, current in zip(exit_numbers, exit_numbers[1:])
            )
        ):
            return {"evidence_valid": False, "reason": "position_chronology_invalid"}
        try:
            net = math.fsum(_finite(row.get("net"), "position net") for row in fragments)
        except (ArithmeticError, ValueError):
            return {"evidence_valid": False, "reason": "position_net_invalid"}
        positions.append({
            "position_id": position_id,
            "symbol": symbol,
            "side": side,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "regime": _entry_regime(indexed, symbol, entry_time),
            "net": net,
        })
    reported = stats.get("full_trades")
    if reported != len(positions):
        return {"evidence_valid": False, "reason": "position_count_mismatch"}
    symbol_counts = Counter(row["symbol"] for row in positions)
    regime_counts = Counter(row["regime"] for row in positions)
    positive_by_symbol = Counter()
    positive_by_regime = Counter()
    for row in positions:
        if row["net"] > 0.0:
            positive_by_symbol[row["symbol"]] += row["net"]
            positive_by_regime[row["regime"]] += row["net"]
    total_positive = math.fsum(positive_by_symbol.values())

    def maximum_share(values: Counter) -> float | None:
        if total_positive <= 0.0 or not values:
            return None
        return max(values.values()) / total_positive

    symbol_share = maximum_share(positive_by_symbol)
    regime_share = maximum_share(positive_by_regime)
    observed_regimes = sorted(name for name in regime_counts if name != "UNKNOWN")
    return {
        "evidence_valid": True,
        "positions": positions,
        "independent_positions": len(positions),
        "position_ids_unique_within_period": len(groups) == len(positions),
        "symbol_counts": dict(sorted(symbol_counts.items())),
        "regime_counts": dict(sorted(regime_counts.items())),
        "positive_net_by_symbol": dict(sorted(positive_by_symbol.items())),
        "positive_net_by_regime": dict(sorted(positive_by_regime.items())),
        "maximum_symbol_profit_share": symbol_share,
        "maximum_regime_profit_share": regime_share,
        "symbol_concentration_pass": bool(
            symbol_share is not None and symbol_share <= symbol_limit
        ),
        "regime_concentration_pass": bool(
            regime_share is not None
            and regime_share <= regime_limit
            and len(observed_regimes) >= 2
        ),
        "regimes_observed": observed_regimes,
    }
