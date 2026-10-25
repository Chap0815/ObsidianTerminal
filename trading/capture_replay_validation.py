"""Fail-closed cost and temporal validation for immutable capture replays."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from time import monotonic as _calibration_monotonic
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from sqlite_safety import harden_sqlite_connection
from core.constants import SIM_CAPTURE_CONTRACT_SCHEMA
from trading.sim_evidence_availability import (
    read_sim_availability_facts,
    sim_availability_proof_scope,
    validate_sim_availability_proof,
)
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


COST_EVIDENCE_SCHEMA = 8
SNAPSHOT_FINGERPRINT_SCHEMA = 4
DEFAULT_MARKOUT_HORIZONS = (1, 10, 60, 300, 900)
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
MIN_COST_EVIDENCE_SAMPLES = 50
MIN_COST_EVIDENCE_COVERAGE = 0.95
# Existing runtime markout budget: late diagnostic rows do not certify a horizon.
MAX_MARKOUT_EVIDENCE_LAG_SECONDS = 30
# Whole-fit limits, including original-fact revalidation. Crossing one aborts
# the complete fit; no latest-N or partially read sample may certify a model.
MAX_CALIBRATION_READ_ROWS = 250_000
MAX_CALIBRATION_READ_BYTES = 64 * 1024 * 1024
MAX_CALIBRATION_ROW_BYTES = 256 * 1024
MAX_CALIBRATION_READ_SECONDS = 30.0


class _CalibrationReadBudget:
    def __init__(self):
        self.deadline = _calibration_monotonic() + MAX_CALIBRATION_READ_SECONDS
        self.rows = 0
        self.bytes = 0

    def check(self):
        if _calibration_monotonic() >= self.deadline:
            raise RuntimeError("calibration read budget exceeded: time")

    def progress(self):
        # SQLite callbacks signal cancellation by returning nonzero. Raising
        # here would be swallowed by sqlite3's callback bridge.
        return int(_calibration_monotonic() >= self.deadline)

    def row(self, cursor, values):
        self.check()
        self.rows += 1
        self.bytes += sum(len(value.encode("utf-8")) if isinstance(value, str)
                          else len(value) if isinstance(value, bytes) else 8
                          for value in values)
        if self.rows > MAX_CALIBRATION_READ_ROWS or self.bytes > MAX_CALIBRATION_READ_BYTES:
            raise RuntimeError("calibration read budget exceeded: rows/bytes")
        return sqlite3.Row(cursor, values)


def _visible_scope_sql(connection, scope, entry_expression):
    """Exclude foreign/later facts before materialization, using frozen scope.

    Discovery still independently validates every visible original proof, so
    corrupting mutable candidate fields cannot hide a broken scoped campaign.
    """
    if scope is None:
        return "", ()
    if not _table_exists(connection, "sim_execution_availability"):
        return " AND 0", ()
    columns = {row[1] for row in connection.execute("PRAGMA table_info(sim_execution_availability)")}
    if not {"bot_name", "mode", "venue", "market_type", "clock_bound_kind"} <= columns:
        return " AND 0", ()
    return (f""" AND EXISTS (SELECT 1 FROM sim_execution_availability proof
        WHERE proof.entry_id={entry_expression} AND proof.fact_key='entry_pair'
        AND proof.bot_name=? AND proof.mode=? AND proof.venue=? AND proof.market_type=?
        AND proof.state IN ('ACKED','CONFLICT')
        AND proof.clock_bound_kind IS NOT NULL
        AND proof.available_by_ms>0 AND proof.available_by_ms<=?)""",
        (scope["bot_name"], scope["mode"], scope["venue"], scope["market_type"],
         _evidence_timestamp(scope["calibration_as_of"]).timestamp() * 1000))


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
    calibration_scope=None,
    arrival_bindings=(),
    availability_bindings=(),
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
        "calibration_scope": calibration_scope,
    })
    for row in tca_rows:
        add(["tca", list(row)])
    # Only the Base-only identity path consumes these additional input facts.
    # Canonical-symbol legacy snapshots retain their existing fingerprint.
    for row in arrival_bindings:
        add(["arrival_binding", list(row)])
    for row in markout_rows:
        add(["markout", [row[column] for column in (
            "horizon_seconds", "status", "markout_bps", "entry_id", "due_at",
            "measured_at", "symbol", "side", "reference_price")]])
    for binding in availability_bindings:
        add(["original_fact_availability", binding])
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
    connection = harden_sqlite_connection(sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True))
    try:
        connection.row_factory = sqlite3.Row
    except BaseException as setup_error:
        try:
            connection.close()
        except BaseException as close_error:
            try:
                setup_error.add_note(
                    "close replay evidence connection after setup failure: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass
        raise
    return connection


def _note_replay_cleanup_error(
    primary: BaseException,
    context: str,
    secondary: BaseException,
) -> None:
    try:
        primary.add_note(
            f"{context}: {type(secondary).__name__}: {secondary}"
        )
    except BaseException:
        pass


def _finish_read_transaction(
    connection: sqlite3.Connection,
    primary_error: BaseException | None,
) -> None:
    """Rollback and close independently while preserving error priority."""
    cleanup_errors: list[tuple[str, BaseException]] = []
    try:
        if connection.in_transaction:
            connection.rollback()
    except BaseException as rollback_error:
        cleanup_errors.append((
            "rollback replay evidence connection after read",
            rollback_error,
        ))
    try:
        connection.close()
    except BaseException as close_error:
        cleanup_errors.append((
            "close replay evidence connection after read",
            close_error,
        ))
    if not cleanup_errors:
        return
    if primary_error is not None:
        for context, cleanup_error in cleanup_errors:
            _note_replay_cleanup_error(
                primary_error,
                context,
                cleanup_error,
            )
        return
    _context, cleanup_primary = cleanup_errors[0]
    for context, secondary in cleanup_errors[1:]:
        _note_replay_cleanup_error(cleanup_primary, context, secondary)
    raise cleanup_primary


def _start_evidence_read_budget(connection):
    """Install whole-read guards on a caller-owned, read-only connection."""
    budget = _CalibrationReadBudget()
    connection.execute("PRAGMA busy_timeout=1000")
    connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_CALIBRATION_ROW_BYTES)
    connection.row_factory = budget.row
    connection.set_progress_handler(budget.progress, 1000)
    return budget


def _finish_bounded_read_transaction(connection, primary_error):
    """Remove the callback before independent rollback/close, preserving errors."""
    try:
        connection.set_progress_handler(None, 0)
    except BaseException as cleanup_error:
        if primary_error is None:
            _finish_read_transaction(connection, cleanup_error)
            raise
        _note_replay_cleanup_error(primary_error, "clear calibration progress handler", cleanup_error)
        _finish_read_transaction(connection, primary_error)
    else:
        _finish_read_transaction(connection, primary_error)


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
    exact_fields = ("arrival_receipt_ms", "fill_time_ms", "capture_recorded_at_ms",
                    "capture_anchor_semantics")
    if any(key in payload for payload in (arrival, fill) for key in exact_fields):
        if any(key not in payload for payload in (arrival, fill) for key in exact_fields):
            raise ValueError("exact SIM capture timing is incomplete")
        if any(arrival[key] != fill[key] for key in exact_fields):
            raise ValueError("exact SIM capture timing conflicts")
        receipt, filled, recorded = (arrival[key] for key in exact_fields[:3])
        source_time = arrival.get("exchange_time_ms")
        if (any(type(value) not in (int, float) or not math.isfinite(value)
                or value <= 0 for value in (receipt, filled, recorded))
                or not 0 <= filled - receipt <= 1000 or recorded < filled
                or type(source_time) not in (int, float) or not math.isfinite(source_time)
                or source_time <= 0 or source_time > filled
                or arrival["capture_anchor_semantics"] != "ceil_fill_markout_schedule"
                or anchor.timestamp() != math.ceil(filled / 1000)):
            raise ValueError("exact SIM capture timing is noncausal")
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
    availability_rejections=None,
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
            if status not in {"PENDING", "COMPLETE", "FAILED", "UNOBSERVED"}:
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
        if validate_contract_rows and availability_rejections is not None:
            reason = availability_rejections.get((entry_id, f"markout_{horizon}"))
            if reason:
                rejected[reason] += 1
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
        if measured_at > fill_at + timedelta(
            seconds=horizon + MAX_MARKOUT_EVIDENCE_LAG_SECONDS
        ):
            invalid_complete_entries.add(entry_id)
            rejected["late_complete_markout"] += 1
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


def _bound_arrival_symbol(candidate_symbol, bot_name, arrival_time, arrival, bindings):
    """Resolve a Base-only candidate from its exact persisted arrival evidence."""
    if not isinstance(candidate_symbol, str) or not candidate_symbol:
        raise ValueError("candidate identity is invalid")
    if "/" in candidate_symbol:
        return candidate_symbol
    if (candidate_symbol != candidate_symbol.strip() or ":" in candidate_symbol
            or len(bindings) != 1):
        raise ValueError("Base candidate requires one bound arrival book")
    binding = bindings[0]
    symbol = binding["symbol"]
    if (binding["bot_name"] != bot_name or binding["mode"] != "SIM"
            or binding["measured_at"] != arrival_time
            or not isinstance(symbol, str) or symbol != symbol.strip()
            or symbol.count("/") != 1 or not symbol.partition("/")[2]
            or symbol.partition("/")[0].casefold() != candidate_symbol.casefold()
            or _canonical_bytes(decode_execution_cost_payload(binding["payload_json"]))
            != _canonical_bytes(arrival)):
        raise ValueError("arrival book conflicts with candidate or TCA evidence")
    return symbol


def _calibration_scope(*, bot_name, venue, market_type, calibration_as_of):
    values = (bot_name, venue, market_type, calibration_as_of)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ValueError("complete calibration scope and UTC cutoff are required")
    if not all(isinstance(value, str) and value.strip() for value in values[:3]):
        raise ValueError("calibration scope is invalid")
    bot = bot_name.strip().upper()
    venue = venue.strip().lower()
    market_type = market_type.strip().lower()
    if (
        bot not in {"SPOT", "TREND", "FUTURES", "FUTREND", "CROSS"}
        or not venue.isascii()
        or not venue.replace("_", "").replace("-", "").isalnum()
        or market_type not in {"spot", "swap"}
    ):
        raise ValueError("calibration scope is invalid")
    try:
        cutoff = (
            calibration_as_of if isinstance(calibration_as_of, datetime)
            else datetime.fromisoformat(calibration_as_of.replace("Z", "+00:00"))
        )
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("calibration cutoff requires an explicit UTC timezone")
        cutoff = cutoff.astimezone(timezone.utc)
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("calibration cutoff requires an explicit UTC timezone") from exc
    return {
        "schema_version": 1, "bot_name": bot, "mode": "SIM", "venue": venue,
        "market_type": market_type, "calibration_as_of": cutoff.isoformat(),
    }


def _validated_calibration_scope(scope):
    if scope is None:
        return None
    if type(scope) is not dict or set(scope) != {
        "schema_version", "bot_name", "mode", "venue", "market_type", "calibration_as_of"
    } or type(scope.get("schema_version")) is not int or scope["schema_version"] != 1:
        raise ValueError("calibration scope is inconsistent")
    normalized = _calibration_scope(
        bot_name=scope["bot_name"], venue=scope["venue"],
        market_type=scope["market_type"], calibration_as_of=scope["calibration_as_of"],
    )
    if normalized != scope:
        raise ValueError("calibration scope is inconsistent")
    return scope


def _payload_matches_calibration_scope(payload, scope):
    return bool(
        isinstance(payload, dict)
        and type(payload.get("capture_scope_schema")) is int
        and payload["capture_scope_schema"] == 1
        and all(payload.get(key) == scope[key] for key in (
            "bot_name", "mode", "venue", "market_type"
        ))
    )


def _select_calibration_snapshot(tca_rows, markout_rows, scope):
    cutoff = _evidence_timestamp(scope["calibration_as_of"])
    early_rows = []
    matching_entries = set()
    for row in tca_rows:
        candidate = _evidence_timestamp(row["candidate_time"])
        measured = _evidence_timestamp(row["measured_at"])
        if row["bot_name"] != scope["bot_name"] or candidate is None or measured is None:
            continue
        if candidate > cutoff or measured > cutoff:
            continue
        early_rows.append(row)
        try:
            payload = decode_execution_cost_payload(row["payload_json"])
        except ValueError:
            continue
        if "capture_recorded_at_ms" in payload:
            recorded = payload["capture_recorded_at_ms"]
            if (type(recorded) not in (int, float) or not math.isfinite(recorded)
                    or recorded <= 0 or recorded > cutoff.timestamp() * 1000):
                # This is a serialization-time lower bound, not proof of the
                # eventual SQLite commit. A late State ACK cannot backdate it.
                early_rows.pop()
                continue
        if _payload_matches_calibration_scope(payload, scope):
            matching_entries.add(str(row["entry_id"]))
    selected_tca = [row for row in early_rows if str(row["entry_id"]) in matching_entries]
    selected_markouts = []
    for row in markout_rows:
        if str(row["entry_id"]) not in matching_entries:
            continue
        due = _evidence_timestamp(row["due_at"])
        measured = _evidence_timestamp(row["measured_at"])
        # Mutable pending rows do not reconstruct historical status. Only rows
        # with an actual pre-cutoff observation belong to this frozen cohort.
        if due is not None and measured is not None and due <= cutoff and measured <= cutoff:
            selected_markouts.append(row)
    return selected_tca, selected_markouts


def _read_commit_availability(connection, entry_ids, markout_rows, cutoff, scope):
    """Bind witnesses and original facts in the caller's consistent snapshot.

    A missing, pending or post-cutoff witness has the same historical meaning:
    availability was not proved by this cutoff. Later ACK writes cannot change
    that cohort's hash or retrospectively certify its original observations.
    """
    cutoff_ms = cutoff.timestamp() * 1000 if cutoff is not None else None
    keys = {(entry_id, "entry_pair") for entry_id in entry_ids}
    keys.update((str(row["entry_id"]), f"markout_{row['horizon_seconds']}")
                for row in markout_rows if row["status"] == "COMPLETE"
                and str(row["entry_id"]) in entry_ids)
    rejections, bindings = {}, []
    present = _table_exists(connection, "sim_execution_availability")
    if present:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(sim_execution_availability)")}
        present = {"bot_name", "mode", "venue", "market_type", "clock_bound_kind"} <= columns
    if present and scope is not None:
        # Discovery uses the scope frozen at ORIGINAL staging, never mutable
        # candidate joins, payload scope, or a potentially corrupted timestamp.
        keys.update((str(row[0]), "entry_pair") for row in connection.execute("""SELECT entry_id
            FROM sim_execution_availability WHERE fact_key='entry_pair'
            AND bot_name=? AND mode=? AND venue=? AND market_type=?
            AND state IN ('ACKED','CONFLICT') AND clock_bound_kind IS NOT NULL
            AND available_by_ms>0 AND available_by_ms<=?""",
            (scope["bot_name"], scope["mode"], scope["venue"], scope["market_type"], cutoff_ms)))
    pending_keys = deque(sorted(keys))
    visited = set()
    while pending_keys:
        entry_id, fact_key = pending_keys.popleft()
        if (entry_id, fact_key) in visited:
            continue
        visited.add((entry_id, fact_key))
        reason = "unproven_commit_availability"
        proof = None
        if present:
            row = connection.execute("SELECT entry_id,fact_key,schema_version,facts_sha256,"
                                     "state,available_by_ms,witness_kind,bot_name,mode,venue,market_type,clock_bound_kind "
                                     "FROM sim_execution_availability "
                                     "WHERE entry_id=? AND fact_key=?",
                                     (entry_id, fact_key)).fetchone()
            proof = dict(row) if row is not None else None
        stamp = proof.get("available_by_ms") if proof else None
        frozen_scope = sim_availability_proof_scope(proof) if proof is not None else None
        matches_scope = (scope is None or frozen_scope == {key: scope[key] for key in (
            "bot_name", "mode", "venue", "market_type")})
        visible = (proof is not None and frozen_scope is not None and matches_scope
                   and proof.get("clock_bound_kind") is not None
                   and proof.get("state") in {"ACKED", "CONFLICT"}
                   and type(stamp) in (int, float) and math.isfinite(stamp)
                   and stamp > 0 and (cutoff_ms is None or stamp <= cutoff_ms))
        if visible:
            try:
                facts = read_sim_availability_facts(connection, entry_id, fact_key)
                reason = validate_sim_availability_proof(proof, facts, cutoff_ms=cutoff_ms)
                bindings.append({"proof": proof, "facts": facts if reason is None else None})
                if fact_key == "entry_pair" and reason is None:
                    pending_keys.extend((entry_id, f"markout_{row['horizon_seconds']}")
                                        for row in facts["scheduled_markouts"])
            except (sqlite3.Error, KeyError, TypeError, ValueError, OverflowError) as exc:
                if (isinstance(exc, sqlite3.Error) and getattr(exc, "sqlite_errorcode", None)
                        in {sqlite3.SQLITE_TOOBIG, sqlite3.SQLITE_INTERRUPT}):
                    # Resource guards abort the entire fit, including reads
                    # below the original-fact validation boundary.
                    raise
                reason = "invalid_commit_availability"
                bindings.append({"proof": proof, "facts": None})
        rejections[(entry_id, fact_key)] = reason
    return rejections, bindings


def _project_historical_commit_snapshot(tca_rows, markout_rows, arrival_bindings,
                                        rejections, availability_bindings):
    """Project only provably visible facts, using an immutable queue schedule.

    UNOBSERVED describes the absence of a proved completion by the cutoff; it
    does not reconstruct a historical mutable PENDING/FAILED queue status.
    Visible conflicting witnesses remain present and veto readiness.
    """
    pairs = {binding["proof"]["entry_id"]: binding["facts"]
             for binding in availability_bindings
             if binding["proof"]["fact_key"] == "entry_pair"}
    tca_rows = [row for row in tca_rows if str(row["entry_id"]) in pairs]
    arrival_bindings = [row for row in arrival_bindings if str(row["entry_id"]) in pairs]
    actual = {(str(row["entry_id"]), row["horizon_seconds"]): row for row in markout_rows}
    projected = []
    for identity, facts in sorted(pairs.items()):
        if facts is None:
            continue
        for basis in facts["scheduled_markouts"]:
            horizon = basis["horizon_seconds"]
            reason = rejections.get((identity, f"markout_{horizon}"), "unproven_commit_availability")
            row = actual.get((identity, horizon))
            if row is not None and reason != "unproven_commit_availability":
                projected.append({**dict(row), "markout_rowid": None})
            else:
                projected.append({**basis, "status": "UNOBSERVED", "measured_at": None,
                                  "markout_bps": None, "markout_rowid": None})
    bindings = [binding for binding in availability_bindings
                if binding["proof"]["entry_id"] in pairs]
    return tca_rows, projected, arrival_bindings, bindings


def collect_sim_cost_evidence(
    runtime_root: str | Path,
    *,
    minimum_samples: int = 50,
    minimum_quality_coverage: float = 0.95,
    minimum_markout_coverage: float = 0.95,
    required_horizons: Iterable[int] = DEFAULT_MARKOUT_HORIZONS,
    bot_name: str | None = None,
    venue: str | None = None,
    market_type: str | None = None,
    calibration_as_of: str | datetime | None = None,
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
    scope = _calibration_scope(
        bot_name=bot_name, venue=venue, market_type=market_type,
        calibration_as_of=calibration_as_of,
    )
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
    primary_error: BaseException | None = None
    try:
        budget = _start_evidence_read_budget(connection)
        connection.execute("BEGIN")
        required_tables = {
            "expectancy_candidates",
            "sim_execution_tca",
            "sim_execution_markouts",
        }
        if any(not _table_exists(connection, table) for table in required_tables):
            raise ValueError("SIM execution evidence tables are incomplete")
        tca_scope, tca_parameters = _visible_scope_sql(connection, scope, "t.entry_id")
        tca_rows = connection.execute(
            """SELECT t.id AS tca_id, t.entry_id, t.measured_at,
                      t.stage, t.payload_json, e.bot_name, e.symbol,
                      e.candidate_time
                 FROM sim_execution_tca t
                 JOIN expectancy_candidates e ON e.entry_id=t.entry_id
                WHERE e.mode='SIM' AND t.stage IN ('arrival','fill')
                """ + tca_scope + " ORDER BY t.id DESC", tca_parameters
        ).fetchall()
        markout_scope, markout_parameters = _visible_scope_sql(connection, scope, "m.entry_id")
        markout_rows = connection.execute(
            """SELECT m.horizon_seconds, m.status, m.markout_bps,
                      m.entry_id, m.due_at, m.measured_at, m.symbol,
                      m.side, m.reference_price, m.rowid AS markout_rowid
                 FROM sim_execution_markouts m
                 JOIN expectancy_candidates e ON e.entry_id=m.entry_id
                WHERE e.mode='SIM'
                """ + markout_scope + " ORDER BY m.entry_id, m.horizon_seconds, m.rowid",
            markout_parameters,
        ).fetchall()
        tca_orphan_entries = 0 if scope is not None else connection.execute(
            """SELECT COUNT(DISTINCT t.entry_id)
                 FROM sim_execution_tca t
                 LEFT JOIN expectancy_candidates e ON e.entry_id=t.entry_id
                WHERE e.entry_id IS NULL"""
        ).fetchone()[0]
        markout_orphan_entries = 0 if scope is not None else connection.execute(
            """SELECT COUNT(DISTINCT m.entry_id)
                 FROM sim_execution_markouts m
                 LEFT JOIN expectancy_candidates e ON e.entry_id=m.entry_id
                WHERE e.entry_id IS NULL"""
        ).fetchone()[0]
        maximum_tca = (None, None) if scope is not None else connection.execute(
            "SELECT MAX(id), MAX(measured_at) FROM sim_execution_tca"
        ).fetchone()
        maximum_markout = (None, None) if scope is not None else connection.execute(
            "SELECT MAX(rowid), MAX(measured_at) FROM sim_execution_markouts"
        ).fetchone()
        arrival_bindings = []
        if _table_exists(connection, "candidate_microstructure"):
            book_scope, book_parameters = _visible_scope_sql(connection, scope, "book.entry_id")
            arrival_bindings = connection.execute(
                """SELECT book.entry_id,book.bot_name,book.mode,book.symbol,
                          book.measured_at,book.payload_json
                     FROM candidate_microstructure AS book
                     JOIN expectancy_candidates AS candidate ON candidate.entry_id=book.entry_id
                    WHERE book.stage='arrival_book' AND candidate.mode='SIM'
                      AND instr(candidate.symbol,'/')=0
                    """ + book_scope + """ ORDER BY book.entry_id,book.bot_name,book.symbol,
                         book.measured_at,book.payload_json""", book_parameters,
            ).fetchall()
        if scope is not None:
            tca_rows, markout_rows = _select_calibration_snapshot(tca_rows, markout_rows, scope)
            # Orphans have no attributable bot/venue/time and cannot certify
            # this cohort. The unscoped diagnostic report retains their count.
            tca_orphan_entries = markout_orphan_entries = 0
            maximum_tca = (
                max((row["tca_id"] for row in tca_rows), default=None),
                max((row["measured_at"] for row in tca_rows), default=None),
            )
            maximum_markout = (
                max((row["markout_rowid"] for row in markout_rows), default=None),
                max((row["measured_at"] for row in markout_rows), default=None),
            )
        selected_entries = {str(row["entry_id"]) for row in tca_rows}
        cutoff = _evidence_timestamp(scope["calibration_as_of"]) if scope else None
        arrival_bindings = [row for row in arrival_bindings
                            if str(row["entry_id"]) in selected_entries
                            and (cutoff is None or (
                                _evidence_timestamp(row["measured_at"]) is not None
                                and _evidence_timestamp(row["measured_at"]) <= cutoff))]
        selected_stage_counts = Counter((str(row["entry_id"]), row["stage"]) for row in tca_rows)
        complete_selected_entries = {identity for identity in selected_entries
                                     if selected_stage_counts[(identity, "arrival")] == 1
                                     and selected_stage_counts[(identity, "fill")] == 1}
        availability_rejections, availability_bindings = _read_commit_availability(
            connection, complete_selected_entries, markout_rows, cutoff, scope
        )
        if scope is not None:
            tca_rows, markout_rows, arrival_bindings, availability_bindings = _project_historical_commit_snapshot(
                tca_rows, markout_rows, arrival_bindings, availability_rejections, availability_bindings)
            visible_pairs = {binding["proof"]["entry_id"] for binding in availability_bindings
                             if binding["proof"]["fact_key"] == "entry_pair"}
            availability_rejections = {key: reason for key, reason in availability_rejections.items()
                                       if key[0] in visible_pairs}
            maximum_tca = (
                max((row["tca_id"] for row in tca_rows), default=None),
                max((row["measured_at"] for row in tca_rows), default=None))
            maximum_markout = (None, max((row["measured_at"] for row in markout_rows
                                          if row["measured_at"] is not None), default=None))
        availability_integrity_valid = not any(reason not in (None, "unproven_commit_availability")
                                               for reason in availability_rejections.values())
        snapshot_sha256 = _snapshot_sha256(
            tca_rows,
            markout_rows,
            tca_orphan_entries=int(tca_orphan_entries),
            markout_orphan_entries=int(markout_orphan_entries),
            maximum_tca=maximum_tca,
            maximum_markout=maximum_markout,
            calibration_scope=scope,
            arrival_bindings=arrival_bindings,
            availability_bindings=availability_bindings,
        )
        budget.check()
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        _finish_bounded_read_transaction(connection, primary_error)

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
    bindings_by_entry = defaultdict(list)
    for binding in arrival_bindings:
        bindings_by_entry[str(binding["entry_id"])].append(binding)
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
        if scope is not None and not (
            _payload_matches_calibration_scope(arrival, scope)
            and _payload_matches_calibration_scope(fill, scope)
        ):
            rejected["capture_scope_mismatch"] += 1
            contract_rejected["capture_scope_mismatch"] += 1
            continue
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
        bot_samples[str(bot_name)] += 1
        components_valid = False
        try:
            fee_components.append(_finite(fill["fee_bps"], "fee_bps", minimum=0.0))
            signed_shortfalls.append(_finite(fill["shortfall_vs_mid_bps"], "shortfall_vs_mid_bps"))
            components_valid = True
        except (KeyError, ValueError):
            pass
        if is_contract:
            try:
                venue_symbol = _bound_arrival_symbol(
                    symbol, bot_name, arrival_time, arrival, bindings_by_entry[entry_id])
            except (TypeError, ValueError):
                contract_rejected["arrival_book_binding_invalid"] += 1
                continue
            availability_reason = availability_rejections.get((entry_id, "entry_pair"))
            if availability_reason:
                contract_rejected[availability_reason] += 1
                continue
            contract_valid_entry_ids.add(entry_id)
            contract_valid_fill_times[entry_id] = fill_time
            contract_valid_symbols[entry_id] = venue_symbol
            contract_valid_sides[entry_id] = arrival.get("side")
            contract_valid_reference_prices[entry_id] = fill.get(
                "average_fill_price"
            )
            contract_observations.append(observation)
            if components_valid:
                contract_component_count += 1

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
            availability_rejections=availability_rejections,
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
        "calibration_scope": scope,
        "source": {
            "snapshot_schema_version": SNAPSHOT_FINGERPRINT_SCHEMA,
            "availability_schema_version": 1,
            "availability_integrity_valid": availability_integrity_valid,
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
        scope is not None
        and availability_integrity_valid
        and contract_valid_samples >= required
        and contract_quality_coverage >= quality_floor
        and contract_component_coverage >= quality_floor
    )
    markouts_ready = bool(
        contract_horizon_coverages
        and availability_integrity_valid
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
    result = {**body, "evidence_sha256": _sha256(body)}
    budget.check()
    return result


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
        body.get("schema_version") not in (5, 6, 7, COST_EVIDENCE_SCHEMA)
        or body.get("kind") != "mexc_sim_execution_cost_calibration"
        or not isinstance(expected_hash, str)
        or expected_hash != actual_hash
    ):
        raise ValueError("cost evidence integrity check failed")

    source = body.get("source")
    is_current = body.get("schema_version") == COST_EVIDENCE_SCHEMA
    scope = (_validated_calibration_scope(body.get("calibration_scope"))) if body.get("schema_version") >= 6 else None
    if type(source) is not dict:
        raise ValueError("cost evidence snapshot identity is inconsistent")
    _evidence_count(source.get("snapshot_tca_rows"), "snapshot_tca_rows")
    _evidence_count(source.get("snapshot_markout_rows"), "snapshot_markout_rows")
    snapshot_sha256 = source.get("snapshot_sha256")
    if (
        source.get("snapshot_schema_version") != {5: 1, 6: 2, 7: 3, 8: 4}[body["schema_version"]]
        or not isinstance(snapshot_sha256, str)
        or len(snapshot_sha256) != 64
        or any(character not in "0123456789abcdef" for character in snapshot_sha256)
    ):
        raise ValueError("cost evidence snapshot identity is inconsistent")
    if is_current and (type(source.get("availability_schema_version")) is not int
                       or source["availability_schema_version"] != 1):
        raise ValueError("cost evidence availability contract is inconsistent")
    if is_current and type(source.get("availability_integrity_valid")) is not bool:
        raise ValueError("cost evidence availability integrity is inconsistent")
    availability_integrity_valid = source.get("availability_integrity_valid", True)
    if scope is not None:
        cutoff = _evidence_timestamp(scope["calibration_as_of"])
        for field in ("max_tca_measured_at", "max_markout_measured_at"):
            value = source.get(field)
            if value is not None:
                stamp = _evidence_timestamp(value)
                if stamp is None or stamp > cutoff:
                    raise ValueError("cost evidence calibration cutoff is inconsistent")

    costs = body.get("costs")
    markouts = body.get("markouts")
    readiness = body.get("readiness")
    if type(costs) is not dict or type(markouts) is not dict:
        raise ValueError("cost evidence contract is inconsistent")
    if scope is not None:
        bot_samples = costs.get("bot_samples")
        if (
            type(bot_samples) is not dict
            or set(bot_samples) - {scope["bot_name"]}
            or sum(_evidence_count(count, "bot sample count") for count in bot_samples.values())
            != costs.get("valid_samples")
        ):
            raise ValueError("cost evidence bot scope is inconsistent")
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
        (scope is not None or not is_current)
        and availability_integrity_valid
        and valid_samples >= required_samples
        and quality_coverage >= quality_floor
        and component_coverage >= quality_floor
    )
    markouts_ready = bool(
        minimum_markout_coverage >= markout_floor
        and availability_integrity_valid
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


def require_cost_calibration_scope(
    evidence: dict, *, bot_name: str, venue: str, market_type: str, not_after=None
) -> dict:
    """Bind a replay consumer to its ex-ante, venue-specific calibration."""
    body = validate_cost_evidence(evidence)
    scope = body.get("calibration_scope")
    if body.get("schema_version") != COST_EVIDENCE_SCHEMA or scope is None:
        raise ValueError("cost calibration requires verified scope and UTC cutoff")
    scope = _validated_calibration_scope(scope)
    if any(scope[key] != expected for key, expected in (
        ("bot_name", bot_name), ("venue", venue), ("market_type", market_type),
        ("mode", "SIM"),
    )):
        raise ValueError("cost calibration scope does not match replay")
    if not_after is not None and _time_number(scope["calibration_as_of"]) > _time_number(not_after):
        raise ValueError("cost calibration cutoff is after initial training period")
    return body


def calibrated_cost_profiles(evidence: dict, *, default_round_trip_bps: float) -> dict:
    """Create conservative all-in round-trip profiles from per-side SIM TCA."""
    default = _finite(default_round_trip_bps, "default_round_trip_bps", minimum=0.0)
    if default > 1_000.0:
        raise ValueError("default_round_trip_bps is out of range")
    body = validate_cost_evidence(evidence)
    scope_ready = bool(
        body.get("schema_version") == COST_EVIDENCE_SCHEMA
        and _validated_calibration_scope(body.get("calibration_scope")) is not None
    )
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
        "simulation_ready": scope_ready and readiness.get("simulation_ready") is True,
        "promotion_evidence_ready": bool(
            scope_ready and readiness.get("promotion_evidence_ready") is True
            and contract_markouts.get("integrity_valid") is True
        ),
        "evidence_sha256": expected_hash,
        "calibration_scope": body.get("calibration_scope"),
        "calibration_method": (
            "fixed_pre_initial_train_cutoff_no_oos_recalibration"
            if scope_ready else "unbound_diagnostic_no_readiness"
        ),
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
            or any(not isinstance(row.get("liquidated"), bool) for row in fragments)
            or any(row.get("liquidated") is True for row in fragments[:-1])
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
            any(exit_number <= entry_number for exit_number in exit_numbers)
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
            "exit_time": exit_numbers[-1],
            "regime": _entry_regime(indexed, symbol, entry_time),
            "net": net,
            "liquidated": terminal[0]["liquidated"],
        })
    reported = stats.get("full_trades")
    if reported != len(positions):
        return {"evidence_valid": False, "reason": "position_count_mismatch"}
    try:
        initial_capital = _finite(
            stats.get("initial_capital"), "initial capital", minimum=0.0
        )
        reported_drawdown = _finite(
            stats.get("max_dd"), "maximum drawdown", minimum=0.0
        )
    except ValueError:
        return {"evidence_valid": False, "reason": "position_summary_invalid"}
    reported_liquidations = stats.get("liquidation_count")
    if (
        initial_capital <= 0.0
        or isinstance(reported_liquidations, bool)
        or not isinstance(reported_liquidations, int)
        or reported_liquidations < 0
    ):
        return {"evidence_valid": False, "reason": "position_summary_invalid"}
    equity = initial_capital
    peak = initial_capital
    reconstructed_drawdown = 0.0
    nets_by_exit = defaultdict(list)
    for position in positions:
        nets_by_exit[position["exit_time"]].append(position["net"])
    for exit_time in sorted(nets_by_exit):
        equity += math.fsum(nets_by_exit[exit_time])
        if not math.isfinite(equity):
            return {"evidence_valid": False, "reason": "position_summary_invalid"}
        peak = max(peak, equity)
        if peak > 0.0:
            reconstructed_drawdown = max(
                reconstructed_drawdown,
                (peak - equity) / peak * 100.0,
            )
    reconstructed_liquidations = sum(
        position["liquidated"] for position in positions
    )
    if not math.isclose(
        reported_drawdown,
        reconstructed_drawdown,
        rel_tol=1e-9,
        abs_tol=1e-9,
    ):
        return {"evidence_valid": False, "reason": "position_drawdown_mismatch"}
    if reported_liquidations != reconstructed_liquidations:
        return {"evidence_valid": False, "reason": "position_liquidation_mismatch"}
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
        "initial_capital": initial_capital,
        "max_drawdown_pct": reconstructed_drawdown,
        "liquidation_count": reconstructed_liquidations,
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
