"""Read-only profit research orchestration with explicit promotion boundaries."""
from __future__ import annotations

import json
import math
import sqlite3
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from trading.carry_sim import CarryEngine, CarryTerms
from trading.execution_cost_model import (
    ExecutionCostObservation,
    decode_execution_cost_payload,
    execution_cost_payloads_match,
    execution_cost_volatility_bps,
    execution_cost_stages_are_causal,
    normalize_execution_cost_limit,
    normalize_execution_cost_minimum_samples,
    resolve_execution_cost_notional,
    validate_execution_cost_arrival,
    validate_execution_cost_fill,
)
from trading.execution_policy import (
    ExecutionPolicyEvidence,
    evaluate_shadow_execution_policy,
)
from trading.experiment_registry_contract import (
    encode_experiment_params,
    normalize_experiment_metadata,
)
from trading.expectancy_telemetry import (
    EXPECTANCY_FEATURES,
    EXPECTANCY_FEATURE_SCHEMAS,
)
from trading.expectancy_training import (
    CandidateLabel,
    expanding_walk_forward_fit,
    normalize_walk_forward_sizes,
)
from trading.orderflow_experiment import build_ofi_window
from trading.promotion_gate import PromotionEvidence, evaluate_promotion


def _finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _positive_integer(value, name: str) -> int:
    number = _finite(value)
    if number is None or number <= 0.0 or not number.is_integer():
        raise ValueError(f"{name} must be a positive integer")
    return int(number)


def _nonnegative_integer(value, name: str) -> int:
    number = _finite(value)
    if number is None or number < 0.0 or not number.is_integer():
        raise ValueError(f"{name} must be a non-negative integer")
    return int(number)


def _positive_number(value, name: str) -> float:
    number = _finite(value)
    if number is None or number <= 0.0:
        raise ValueError(f"{name} must be positive and finite")
    return number


def _nonnegative_number(value, name: str) -> float:
    number = _finite(value)
    if number is None or number < 0.0:
        raise ValueError(f"{name} must be non-negative and finite")
    return number


def _utc_datetime(value) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _read_connection(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _candidate_events(
    root: Path,
    bot: str,
    mode: str,
    *,
    diagnostics: Counter | None = None,
) -> dict[str, dict]:
    selected: dict[str, dict] = {}
    database = root / "data" / "trading_bot.db"
    try:
        conn = _read_connection(database)
    except FileNotFoundError:
        conn = None
    if conn is not None:
        try:
            if _table_exists(conn, "expectancy_candidates"):
                rows = conn.execute(
                    """SELECT entry_id, candidate_time, schema_version, features_json
                         FROM expectancy_candidates
                        WHERE bot_name=? AND mode=?
                        ORDER BY candidate_time, entry_id""",
                    (bot, mode),
                ).fetchall()
                for row in rows:
                    candidate_time = _utc_datetime(row["candidate_time"])
                    try:
                        features = json.loads(row["features_json"])
                        schema_version = _positive_integer(
                            row["schema_version"], "schema_version"
                        )
                    except (TypeError, ValueError, json.JSONDecodeError, OverflowError):
                        if diagnostics is not None:
                            diagnostics["candidate_records_rejected"] += 1
                        continue
                    entry_id = str(row["entry_id"] or "").strip()
                    if entry_id and candidate_time is not None and isinstance(features, dict):
                        selected[entry_id] = {
                            "candidate_time": candidate_time,
                            "features": features,
                            "schema_version": schema_version,
                            "source": "sqlite",
                        }
                    elif diagnostics is not None:
                        diagnostics["candidate_records_rejected"] += 1
        finally:
            conn.close()
    logs = root / "logs"
    if not logs.is_dir():
        return selected
    paths = []
    for path in logs.glob("*/structured.jsonl*"):
        suffix = path.name.removeprefix("structured.jsonl")
        if suffix == "" or (suffix.startswith(".") and suffix[1:].isdigit()):
            paths.append(path)
    for path in sorted(paths):
        try:
            handle = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        with handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except (TypeError, ValueError, json.JSONDecodeError):
                    if diagnostics is not None:
                        diagnostics["candidate_records_rejected"] += 1
                    continue
                if not isinstance(event, dict):
                    if diagnostics is not None:
                        diagnostics["candidate_records_rejected"] += 1
                    continue
                if event.get("event") != "expectancy_candidate":
                    continue
                if str(event.get("bot", "")).strip().upper() != bot:
                    continue
                if str(event.get("mode", "")).strip().upper() != mode:
                    continue
                entry_id = str(event.get("entry_id") or "").strip()
                candidate_time = _utc_datetime(
                    event.get("candidate_time") or event.get("ts")
                )
                features = event.get("features")
                try:
                    schema_version = _positive_integer(
                        event.get("schema_version", 1), "schema_version"
                    )
                except (TypeError, ValueError, OverflowError):
                    if diagnostics is not None:
                        diagnostics["candidate_records_rejected"] += 1
                    continue
                if not entry_id or candidate_time is None or not isinstance(features, dict):
                    if diagnostics is not None:
                        diagnostics["candidate_records_rejected"] += 1
                    continue
                current = selected.get(entry_id)
                if current is None or (
                    current.get("source") != "sqlite"
                    and candidate_time < current["candidate_time"]
                ):
                    selected[entry_id] = {
                        "candidate_time": candidate_time,
                        "features": features,
                        "schema_version": schema_version,
                        "source": "structured_log_legacy",
                    }
    return selected


def build_expectancy_candidates(
    root: str | Path,
    *,
    bot: str,
    mode: str,
    schema_version: int = 1,
    strategy_exits_only: bool = True,
) -> list[CandidateLabel]:
    project = Path(root)
    normalized_bot = str(bot).strip().upper()
    normalized_mode = str(mode).strip().upper()
    selected_schema = _positive_integer(schema_version, "schema_version")
    schemas = EXPECTANCY_FEATURE_SCHEMAS.get(normalized_bot)
    if not schemas:
        raise ValueError("unsupported bot")
    feature_order = schemas.get(selected_schema)
    if feature_order is None:
        raise ValueError("unsupported expectancy schema version")
    if normalized_mode not in {"LIVE", "SIM"}:
        raise ValueError("unsupported mode")
    candidates = _candidate_events(project, normalized_bot, normalized_mode)
    if not candidates:
        return []
    try:
        conn = _read_connection(project / "data" / "trading_bot.db")
    except FileNotFoundError:
        return []
    try:
        if not _table_exists(conn, "trades"):
            return []
        trade_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(trades)")
        }
        reason_sql = "reason" if "reason" in trade_columns else "NULL AS reason"
        trade_rows = conn.execute(
            "SELECT entry_id, sell_time, profit_usdt, invested_usdt, "
            "COALESCE(is_partial,0) AS is_partial, is_sim, "
            + reason_sql
            + " "
            "FROM trades WHERE entry_id IS NOT NULL ORDER BY sell_time"
        ).fetchall()
    finally:
        conn.close()
    expected_is_sim = 1 if normalized_mode == "SIM" else 0
    labels = []
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for trade in trade_rows:
        grouped[str(trade["entry_id"] or "").strip()].append(trade)
    for entry_id, campaign in grouped.items():
        event = candidates.get(entry_id)
        if event is None or not campaign:
            continue
        try:
            event_schema = _positive_integer(
                event.get("schema_version"), "schema_version"
            )
        except ValueError:
            continue
        if event_schema != selected_schema:
            continue
        if not any(int(row["is_partial"] or 0) == 0 for row in campaign):
            continue
        terminal_rows = [
            row for row in campaign if int(row["is_partial"] or 0) == 0
        ]
        if strategy_exits_only:
            from trading.exit_evidence import is_strategy_exit

            if not is_strategy_exit(terminal_rows[-1]["reason"]):
                continue
        if any(row["is_sim"] != expected_is_sim for row in campaign):
            continue
        closed_values = [_utc_datetime(row["sell_time"]) for row in campaign]
        profits = [_finite(row["profit_usdt"]) for row in campaign]
        invested_values = [_finite(row["invested_usdt"]) for row in campaign]
        if (
            any(value is None for value in closed_values + profits + invested_values)
            or any(float(value) < 0.0 for value in invested_values)
        ):
            continue
        closed = max(value for value in closed_values if value is not None)
        profit = sum(float(value) for value in profits if value is not None)
        invested = sum(
            float(value) for value in invested_values if value is not None
        )
        if (
            not math.isfinite(profit)
            or not math.isfinite(invested)
            or invested <= 0.0
        ):
            continue
        net_return_bps = profit / invested * 10_000.0
        if not math.isfinite(net_return_bps):
            continue
        if closed < event["candidate_time"]:
            continue
        features = {}
        for name in feature_order:
            value = _finite(event["features"].get(name))
            if value is None:
                break
            features[name] = value
        if len(features) != len(feature_order):
            continue
        labels.append(
            CandidateLabel(
                candidate_time=event["candidate_time"],
                label_closed_time=closed,
                features=features,
                net_return_bps=net_return_bps,
            )
        )
    return sorted(labels, key=lambda row: row.candidate_time)


def select_expectancy_schema(
    root: str | Path,
    *,
    bot: str,
    mode: str,
    minimum_rows: int,
) -> tuple[int, tuple[str, ...], list[CandidateLabel], dict[int, int]]:
    """Prefer the newest adequately sampled schema, without mixing versions."""
    required = _positive_integer(minimum_rows, "minimum_rows")
    normalized_bot = str(bot).strip().upper()
    schemas = EXPECTANCY_FEATURE_SCHEMAS.get(normalized_bot)
    if not schemas:
        raise ValueError("unsupported bot")
    labels_by_version = {
        version: build_expectancy_candidates(
            root, bot=normalized_bot, mode=mode, schema_version=version
        )
        for version in sorted(schemas)
    }
    adequate = [
        version for version, rows in labels_by_version.items()
        if len(rows) >= required
    ]
    if adequate:
        selected = max(adequate)
    else:
        selected = max(
            labels_by_version,
            key=lambda version: (len(labels_by_version[version]), version == 1),
        )
    return (
        selected,
        schemas[selected],
        labels_by_version[selected],
        {version: len(rows) for version, rows in labels_by_version.items()},
    )


def load_execution_cost_observations(
    root: str | Path,
    *,
    bot: str | None = None,
    mode: str | None = None,
    limit: int = 10_000,
    rejection_counts: Counter | None = None,
) -> list[ExecutionCostObservation]:
    normalized_limit = normalize_execution_cost_limit(limit)
    normalized_bot, normalized_mode = _normalize_execution_cost_scope(
        bot, mode
    )
    path = Path(root) / "data" / "trading_bot.db"

    def record_key(record) -> tuple[bool, datetime, int, str]:
        source, row = record
        measured_at = _utc_datetime(row["measured_at"])
        try:
            tca_id = int(row["tca_id"])
        except (TypeError, ValueError, OverflowError):
            tca_id = -1
        return (
            measured_at is not None,
            measured_at or datetime.min.replace(tzinfo=timezone.utc),
            tca_id,
            source,
        )

    try:
        conn = _read_connection(path)
    except FileNotFoundError:
        return []
    try:
        conn.execute("BEGIN")
        if not _table_exists(conn, "execution_tca") or not (
            _table_exists(conn, "order_intents")
            or _table_exists(conn, "expectancy_candidates")
        ):
            return []
        has_intents = _table_exists(conn, "order_intents")
        has_candidates = _table_exists(conn, "expectancy_candidates")
        intent_columns = (
            {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(order_intents)")
            }
            if has_intents else set()
        )
        execution_tca_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(execution_tca)")
        }
        measured_at_expr = (
            "t.measured_at" if "measured_at" in execution_tca_columns
            else "NULL"
        )
        intent_join = (
            "LEFT JOIN order_intents i ON i.intent_id=t.intent_id "
            if has_intents else ""
        )
        candidate_join = (
            "LEFT JOIN expectancy_candidates e ON e.entry_id=t.intent_id "
            if has_candidates else ""
        )
        bot_expr = (
            "COALESCE(i.bot_name,e.bot_name)" if has_intents and has_candidates
            else ("i.bot_name" if has_intents else "e.bot_name")
        )
        mode_expr = (
            "COALESCE(i.mode,e.mode)" if has_intents and has_candidates
            else ("i.mode" if has_intents else "e.mode")
        )
        symbol_expr = (
            "COALESCE(i.symbol,e.symbol)" if has_intents and has_candidates
            else ("i.symbol" if has_intents else "e.symbol")
        )
        notional_expr = (
            "i.filled_notional"
            if has_intents and "filled_notional" in intent_columns
            else "NULL"
        )
        scope_sql = ""
        params: tuple = ()
        if normalized_bot is not None:
            scope_sql = (
                f"AND UPPER({bot_expr})=? AND UPPER({mode_expr})=? "
            )
            params = (normalized_bot, normalized_mode)
        execution_select = (
            "SELECT t.id AS tca_id, t.intent_id, t.stage, t.payload_json, "
            f"{measured_at_expr} AS measured_at, "
            f"{symbol_expr} AS symbol, {notional_expr} AS filled_notional "
            "FROM execution_tca t "
            + intent_join
            + candidate_join
            + "WHERE t.stage IN ('arrival','fill') "
            + scope_sql
        )
        candidate_records = [
            ("execution", row)
            for row in conn.execute(
                execution_select
                + "ORDER BY julianday(measured_at) DESC, t.id DESC LIMIT ?",
                (*params, normalized_limit),
            ).fetchall()
        ]
        has_sim_tca = (
            has_candidates
            and _table_exists(conn, "sim_execution_tca")
            and normalized_mode in {None, "SIM"}
        )
        sim_select = ""
        sim_scope_sql = ""
        sim_params: tuple = ()
        if has_sim_tca:
            if normalized_bot is not None:
                sim_scope_sql = "AND UPPER(e.bot_name)=? AND UPPER(e.mode)=? "
                sim_params = (normalized_bot, normalized_mode)
            sim_select = (
                """SELECT t.id AS tca_id, t.entry_id AS intent_id,
                          t.measured_at, t.stage, t.payload_json,
                          e.symbol AS symbol, NULL AS filled_notional
                     FROM sim_execution_tca t
                     JOIN expectancy_candidates e ON e.entry_id=t.entry_id
                    WHERE t.stage IN ('arrival','fill') """
                + sim_scope_sql
            )
            candidate_records.extend(
                ("sim", row)
                for row in conn.execute(
                    sim_select
                    + "ORDER BY julianday(t.measured_at) DESC, t.id DESC LIMIT ?",
                    (*sim_params, normalized_limit),
                ).fetchall()
            )
        anchors = sorted(
            candidate_records, key=record_key, reverse=True
        )[:normalized_limit]
        selected = defaultdict(set)
        for source, row in anchors:
            selected[source].add(str(row["intent_id"]))

        records = []
        for source, identifiers in selected.items():
            ordered = sorted(identifiers)
            for offset in range(0, len(ordered), 500):
                chunk = ordered[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                if source == "execution":
                    query = (
                        execution_select
                        + f"AND t.intent_id IN ({placeholders}) "
                        + "ORDER BY julianday(measured_at) DESC, t.id DESC"
                    )
                    query_params = (*params, *chunk)
                else:
                    query = (
                        sim_select
                        + f"AND t.entry_id IN ({placeholders}) "
                        + "ORDER BY julianday(t.measured_at) DESC, t.id DESC"
                    )
                    query_params = (*sim_params, *chunk)
                records.extend(
                    (source, row)
                    for row in conn.execute(query, query_params).fetchall()
                )
    finally:
        try:
            if conn.in_transaction:
                conn.rollback()
        finally:
            conn.close()

    records = sorted(records, key=record_key, reverse=True)
    paired: dict[
        tuple[str, str], dict[str, tuple[dict, object, object]]
    ] = defaultdict(dict)
    metadata = {}
    invalid_payloads: set[tuple[str, str]] = set()
    conflicting_stages: set[tuple[str, str]] = set()
    for source, row in records:
        key = (source, str(row["intent_id"]))
        stage = str(row["stage"])
        metadata[key] = (row["symbol"], row["filled_notional"])
        try:
            payload = decode_execution_cost_payload(row["payload_json"])
        except ValueError:
            if stage not in paired[key]:
                invalid_payloads.add(key)
            continue
        existing = paired[key].get(stage)
        if existing is not None:
            if not execution_cost_payloads_match(existing[0], payload):
                conflicting_stages.add(key)
            continue
        paired[key][stage] = (payload, row["tca_id"], row["measured_at"])
    observations = []
    for key, stages in paired.items():
        if key in invalid_payloads:
            if rejection_counts is not None:
                rejection_counts["invalid_payload"] += 1
            continue
        if key in conflicting_stages:
            if rejection_counts is not None:
                rejection_key = (
                    "conflicting_sim_stage"
                    if key[0] == "sim"
                    else "conflicting_execution_stage"
                )
                rejection_counts[rejection_key] += 1
            continue
        arrival_stage = stages.get("arrival")
        fill_stage = stages.get("fill")
        if arrival_stage is None or fill_stage is None:
            if rejection_counts is not None:
                rejection_counts["incomplete_pair"] += 1
            continue
        arrival, arrival_id, arrival_time = arrival_stage
        fill, fill_id, fill_time = fill_stage
        if not execution_cost_stages_are_causal(
            arrival_id=arrival_id,
            arrival_time=arrival_time,
            fill_id=fill_id,
            fill_time=fill_time,
        ):
            if rejection_counts is not None:
                rejection_counts["noncausal_pair"] += 1
            continue
        symbol, notional = metadata[key]
        try:
            validate_execution_cost_arrival(arrival)
            validate_execution_cost_fill(fill)
            observed_notional = resolve_execution_cost_notional(
                notional, arrival
            )
            observations.append(
                ExecutionCostObservation(
                    symbol=symbol,
                    total_cost_bps=fill["total_cost_bps"],
                    spread_bps=arrival["spread_bps"],
                    depth_coverage=arrival["depth_coverage"],
                    notional_usdt=observed_notional,
                    regime=arrival.get("regime", "unknown"),
                    volatility_bps=execution_cost_volatility_bps(arrival),
                )
            )
        except (KeyError, TypeError, ValueError):
            if rejection_counts is not None:
                rejection_counts["invalid_observation"] += 1
            continue
    return observations


def _normalize_execution_cost_scope(
    bot, mode
) -> tuple[str | None, str | None]:
    if (bot is None) != (mode is None):
        raise ValueError("bot and mode must be supplied together")
    if bot is None:
        return None, None
    if not isinstance(bot, str) or not bot.strip():
        raise ValueError("bot must be a non-empty string")
    if not isinstance(mode, str):
        raise ValueError("mode must be LIVE or SIM")
    normalized_bot = bot.strip().upper()
    normalized_mode = mode.strip().upper()
    if normalized_mode not in {"LIVE", "SIM"}:
        raise ValueError("mode must be LIVE or SIM")
    return normalized_bot, normalized_mode


def _quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def build_execution_cost_report(
    root: str | Path,
    *,
    bot: str | None = None,
    mode: str | None = None,
    minimum_samples: int = 50,
) -> dict:
    required = normalize_execution_cost_minimum_samples(minimum_samples)
    normalized_bot, normalized_mode = _normalize_execution_cost_scope(bot, mode)
    rejected = Counter()
    observations = load_execution_cost_observations(
        root,
        bot=normalized_bot,
        mode=normalized_mode,
        rejection_counts=rejected,
    )
    costs = [row.total_cost_bps for row in observations]
    by_symbol = Counter(row.symbol for row in observations)
    total = len(observations) + sum(rejected.values())
    quality_coverage = len(observations) / total if total else 0.0
    return {
        "scope": {
            "bot": normalized_bot,
            "mode": normalized_mode,
        },
        "valid_samples": len(observations),
        "total_samples": total,
        "quality_coverage": quality_coverage,
        "minimum_quality_coverage": 0.95,
        "rejected": dict(sorted(rejected.items())),
        "minimum_samples": required,
        "ready": (
            len(observations) >= required and quality_coverage >= 0.95
        ),
        "median_cost_bps": _quantile(costs, 0.5),
        "p75_cost_bps": _quantile(costs, 0.75),
        "p95_cost_bps": _quantile(costs, 0.95),
        "symbol_samples": dict(sorted(by_symbol.items())),
    }


def build_execution_policy_report(
    root: str | Path,
    *,
    minimum_samples: int = 50,
) -> dict:
    """Evaluate immutable shadow samples without changing order execution.

    ``execution_shadow`` events must be produced by a sequence-valid replay or
    recorder.  ``maker_crossed_through`` is deliberately only a conservative
    fill proxy; it is never presented as reconstructed queue position.
    """
    minimum = normalize_execution_cost_minimum_samples(minimum_samples)
    valid: list[dict] = []
    rejected = Counter()
    for row in _venue_rows(Path(root), "execution_shadow", limit=50_000):
        payload = row.get("payload") or {}
        flags = row.get("flags") or ()
        if "invalid_event_envelope" in flags:
            rejected["invalid_event_envelope"] += 1
            continue
        if flags:
            rejected["quality_flags"] += 1
            continue
        if payload.get("sequence_valid") is not True:
            rejected["sequence_invalid"] += 1
            continue
        parsed = {
            name: _finite(payload.get(name))
            for name in (
                "gross_edge_bps",
                "taker_cost_bps",
                "maker_fee_bps",
                "maker_adverse_selection_bps",
                "missed_fill_cost_bps",
            )
        }
        if any(value is None for value in parsed.values()):
            rejected["incomplete_cost_fields"] += 1
            continue
        if not isinstance(payload.get("maker_crossed_through"), bool):
            rejected["missing_cross_through_label"] += 1
            continue
        if any(
            parsed[name] < 0.0
            for name in (
                "taker_cost_bps",
                "maker_fee_bps",
                "maker_adverse_selection_bps",
                "missed_fill_cost_bps",
            )
        ):
            rejected["negative_cost"] += 1
            continue
        valid.append({**parsed, "crossed": bool(payload["maker_crossed_through"])})

    evidence = ExecutionPolicyEvidence(
        gross_edge_bps=(
            _quantile([row["gross_edge_bps"] for row in valid], 0.5)
            if valid else None
        ),
        taker_cost_bps=(
            _quantile([row["taker_cost_bps"] for row in valid], 0.5)
            if valid else None
        ),
        maker_fee_bps=(
            _quantile([row["maker_fee_bps"] for row in valid], 0.5)
            if valid else None
        ),
        maker_adverse_selection_bps=(
            _quantile(
                [row["maker_adverse_selection_bps"] for row in valid], 0.5
            )
            if valid else None
        ),
        maker_fill_probability=(
            statistics.mean([float(row["crossed"]) for row in valid])
            if valid else None
        ),
        missed_fill_cost_bps=(
            _quantile([row["missed_fill_cost_bps"] for row in valid], 0.5)
            if valid else None
        ),
        samples=len(valid),
        sequence_valid=bool(valid),
    )
    decision = evaluate_shadow_execution_policy(
        evidence, minimum_samples=minimum
    )
    total = len(valid) + sum(rejected.values())
    quality_coverage = len(valid) / total if total else 0.0
    return {
        # The bundled REST venue recorder cannot prove continuous L2 sequence
        # validity. Only externally recorded, sequence-valid shadow events may
        # satisfy this contract; absence remains explicitly fail-closed.
        "producer": "external_sequence_valid_l2_recorder",
        "producer_wired": total > 0,
        "ready": (
            decision.action != "INSUFFICIENT_DATA" and quality_coverage >= 0.95
        ),
        "samples": len(valid),
        "total_samples": total,
        "quality_coverage": quality_coverage,
        "minimum_quality_coverage": 0.95,
        "minimum_samples": minimum,
        "rejected": dict(sorted(rejected.items())),
        "fill_proxy": "opposite_touch_cross_through",
        "queue_position_reconstructed": False,
        "decision": asdict(decision),
        "changes_orders": False,
        "research_only": True,
    }


def _venue_partition_key(path: Path) -> tuple[bool, datetime, str]:
    try:
        partition_day = datetime.strptime(path.stem, "%Y-%m-%d")
    except ValueError:
        return False, datetime.min, path.name
    return True, partition_day, path.name


def _venue_row_key(row: dict) -> tuple[bool, datetime, str, str, str]:
    exchange_time = str(row["exchange_time"])
    timestamp = _utc_datetime(exchange_time)
    return (
        timestamp is not None,
        timestamp or datetime.min.replace(tzinfo=timezone.utc),
        exchange_time,
        str(row["market_id"]),
        str(row["event_id"]),
    )


_LATEST_VENUE_ROWS_SQL = (
    "SELECT event_id, market_id, exchange_time, quality_flags_json, payload_json "
    "FROM venue_events "
    "ORDER BY exchange_time DESC, market_id DESC, event_id DESC "
    "LIMIT ?"
)


def _venue_rows(root: Path, stream: str, *, limit: int) -> list[dict]:
    rows = []
    folder = root / "data" / "venue_native" / stream
    for path in sorted(
        folder.glob("*.sqlite3"), key=_venue_partition_key, reverse=True
    ):
        if len(rows) >= limit:
            break
        conn = None
        try:
            conn = _read_connection(path)
            fetched = conn.execute(
                _LATEST_VENUE_ROWS_SQL,
                (limit - len(rows),),
            ).fetchall()
        except (OSError, sqlite3.Error):
            continue
        finally:
            if conn is not None:
                conn.close()
        for row in fetched:
            event_id = str(row["event_id"])
            market_id = str(row["market_id"])
            exchange_time = str(row["exchange_time"])
            try:
                raw_flags = json.loads(row["quality_flags_json"])
                if not isinstance(raw_flags, list) or any(
                    not isinstance(flag, str) or not flag.strip()
                    for flag in raw_flags
                ):
                    raise ValueError("invalid venue quality flags")
                payload = decode_execution_cost_payload(row["payload_json"])
                rows.append(
                    {
                        "event_id": event_id,
                        "market_id": market_id,
                        "exchange_time": exchange_time,
                        "flags": tuple(raw_flags),
                        "payload": payload,
                    }
                )
            except (TypeError, ValueError, OverflowError):
                rows.append(
                    {
                        "event_id": event_id,
                        "market_id": market_id,
                        "exchange_time": exchange_time,
                        "flags": ("invalid_event_envelope",),
                        "payload": {},
                    }
                )
    return sorted(rows, key=_venue_row_key)


def build_ofi_report(
    root: str | Path, *, max_events: int = 5_000, max_windows: int = 500
) -> dict:
    normalized_max_events = _positive_integer(max_events, "max_events")
    normalized_max_windows = _positive_integer(max_windows, "max_windows")
    project = Path(root)
    depth_by_market: dict[str, list[dict]] = defaultdict(list)
    for row in _venue_rows(project, "depth", limit=normalized_max_events):
        payload = row["payload"]
        bids, asks = payload.get("bids") or [], payload.get("asks") or []
        event_time = _utc_datetime(row["exchange_time"])
        if event_time is None or not bids or not asks or row["flags"]:
            continue
        try:
            observation = {
                "event_time": event_time,
                "bid_price": bids[0][0],
                "bid_size": bids[0][1],
                "ask_price": asks[0][0],
                "ask_size": asks[0][1],
            }
        except (IndexError, TypeError):
            continue
        depth_by_market[row["market_id"]].append(observation)
    trades_by_market: dict[str, list[dict]] = defaultdict(list)
    seen_trades = set()
    for row in _venue_rows(project, "trades", limit=normalized_max_events):
        if row["flags"]:
            continue
        payload = row["payload"]
        trade_rows = payload.get("trades") if isinstance(payload, dict) else None
        if not isinstance(trade_rows, (list, tuple)):
            continue
        for trade in trade_rows:
            if not isinstance(trade, dict):
                continue
            timestamp = _finite(trade.get("timestamp"))
            price = _finite(trade.get("price"))
            amount = _finite(trade.get("amount"))
            side = (
                trade.get("side").strip().lower()
                if isinstance(trade.get("side"), str)
                else ""
            )
            if (
                timestamp is None
                or timestamp <= 0.0
                or not timestamp.is_integer()
                or price is None
                or price <= 0.0
                or amount is None
                or amount <= 0.0
                or side not in {"buy", "sell"}
            ):
                continue
            try:
                event_time = datetime.fromtimestamp(
                    timestamp / 1000.0, tz=timezone.utc
                )
            except (OverflowError, OSError, ValueError):
                continue
            trade_id = (
                trade.get("id").strip()
                if isinstance(trade.get("id"), str)
                else ""
            )
            key = (
                (row["market_id"], "id", trade_id)
                if trade_id
                else (
                    row["market_id"], "fallback",
                    timestamp, price, amount, side,
                )
            )
            if key in seen_trades:
                continue
            seen_trades.add(key)
            trades_by_market[row["market_id"]].append(
                {
                    "event_time": event_time,
                    "side": side,
                    "amount": amount,
                }
            )
    windows = []
    for market_id, observations in depth_by_market.items():
        observations.sort(key=lambda item: item["event_time"])
        for first, second in zip(observations, observations[1:]):
            gap = (second["event_time"] - first["event_time"]).total_seconds()
            if gap <= 0.0 or gap > 180.0:
                continue
            try:
                window = build_ofi_window(
                    exchange_time=first["event_time"],
                    book_observations=[first, second],
                    trade_observations=trades_by_market.get(market_id, []),
                    observation_seconds=max(1, int(math.ceil(gap)) + 1),
                    continuous_book=False,
                    max_staleness_seconds=5.0,
                )
            except (TypeError, ValueError, OverflowError):
                continue
            windows.append(
                {
                    "market_id": market_id,
                    "feature_cutoff": window.feature_cutoff.isoformat(),
                    "normalized_ofi": window.normalized_ofi,
                    "trade_imbalance": window.trade_imbalance,
                    "book_events": window.book_events,
                    "trade_events": window.trade_events,
                    "promotable": window.promotable,
                    "quality_flags": list(window.quality_flags),
                }
            )
    windows.sort(key=lambda item: item["feature_cutoff"])
    windows = windows[-normalized_max_windows:]
    return {
        "windows": len(windows),
        "promotable_windows": sum(bool(row["promotable"]) for row in windows),
        "research_only": True,
        "latest": windows[-10:],
    }


def build_carry_preview(
    root: str | Path,
    *,
    notional_usdt: float = 100.0,
    expected_funding_periods: float = 1.0,
    expected_funding_periods_by_market: Mapping[str, float] | None = None,
    taker_fee_rate: float = 0.001,
    maker_fee_rate: float = 0.0002,
    entry_slippage_bps_per_leg: float = 2.0,
    exit_slippage_bps_per_leg: float = 2.0,
    limit: int = 20,
) -> dict:
    normalized_limit = _positive_integer(limit, "limit")
    normalized_notional = _positive_number(notional_usdt, "notional_usdt")
    normalized_periods = _positive_number(
        expected_funding_periods, "expected_funding_periods"
    )
    normalized_taker_fee = _nonnegative_number(
        taker_fee_rate, "taker_fee_rate"
    )
    normalized_maker_fee = _nonnegative_number(
        maker_fee_rate, "maker_fee_rate"
    )
    normalized_entry_slippage = _nonnegative_number(
        entry_slippage_bps_per_leg, "entry_slippage_bps_per_leg"
    )
    normalized_exit_slippage = _nonnegative_number(
        exit_slippage_bps_per_leg, "exit_slippage_bps_per_leg"
    )
    if expected_funding_periods_by_market is None:
        normalized_periods_by_market = {}
    elif not isinstance(expected_funding_periods_by_market, Mapping):
        raise ValueError(
            "expected_funding_periods_by_market must be a mapping"
        )
    else:
        normalized_periods_by_market = {}
        for market_id, periods in expected_funding_periods_by_market.items():
            if (
                not isinstance(market_id, str)
                or not market_id.strip()
                or market_id != market_id.strip()
            ):
                raise ValueError(
                    "expected_funding_periods_by_market keys must be "
                    "non-empty strings"
                )
            normalized_periods_by_market[market_id] = _positive_number(
                periods, "expected_funding_periods_by_market"
            )
    overview = [
        row
        for row in _venue_rows(Path(root), "overview", limit=100)
        if not row["flags"]
    ]
    if not overview:
        return {"candidates": 0, "accepted": 0, "rows": [], "simulation_only": True}
    markets = {}
    for row in reversed(overview):
        payload = row["payload"]
        candidate = payload.get("markets") if isinstance(payload, dict) else None
        if isinstance(candidate, dict):
            markets = candidate
            break
    engine = CarryEngine()
    previews = []
    for market_id, market in markets.items():
        if not isinstance(market, Mapping):
            continue
        funding = _finite(market.get("funding_rate"))
        if funding is None or funding <= 0.0:
            continue
        if market.get("spot_available") is not True:
            previews.append(
                {
                    "market_id": str(market_id),
                    "symbol": market.get("symbol"),
                    "funding_rate": funding,
                    "state": "REJECTED",
                    "reason": "spot market availability not verified",
                    "projected_net_pnl": 0.0,
                }
            )
            continue
        market_periods = normalized_periods_by_market.get(
            str(market_id), normalized_periods
        )
        terms = CarryTerms(
            notional_usdt=normalized_notional,
            expected_funding_rate=funding,
            taker_fee_rate=normalized_taker_fee,
            maker_fee_rate=normalized_maker_fee,
            expected_funding_periods=market_periods,
            entry_slippage_bps_per_leg=normalized_entry_slippage,
            exit_slippage_bps_per_leg=normalized_exit_slippage,
        )
        campaign = engine.start(f"preview-{market_id}", terms)
        previews.append(
            {
                "market_id": str(market_id),
                "symbol": market.get("symbol"),
                "funding_rate": funding,
                "expected_funding_periods": market_periods,
                "state": campaign.state.value,
                "reason": campaign.reason,
                "projected_net_pnl": campaign.projected_net_pnl,
            }
        )
    previews.sort(key=lambda row: row["projected_net_pnl"], reverse=True)
    previews = previews[:normalized_limit]
    return {
        "candidates": len(previews),
        "accepted": sum(row["state"] == "CAPITAL_RESERVED" for row in previews),
        "rows": previews,
        "simulation_only": True,
    }


def _registry_status(root: Path) -> dict:
    path = root / "data" / "trading_bot.db"
    try:
        conn = _read_connection(path)
    except FileNotFoundError:
        return {"trials": 0, "trial_statuses": {}, "carry_campaigns": 0}
    try:
        trial_rows = (
            conn.execute(
                "SELECT status, COUNT(*) n FROM experiment_registry GROUP BY status"
            ).fetchall()
            if _table_exists(conn, "experiment_registry")
            else []
        )
        carry_count = (
            conn.execute("SELECT COUNT(*) FROM carry_campaigns").fetchone()[0]
            if _table_exists(conn, "carry_campaigns")
            else 0
        )
    finally:
        conn.close()
    statuses = {str(row["status"]): int(row["n"]) for row in trial_rows}
    return {
        "trials": sum(statuses.values()),
        "trial_statuses": statuses,
        "carry_campaigns": int(carry_count),
    }


def build_observation_readiness(
    root: str | Path,
    *,
    bot: str,
    mode: str,
    minimum_observed_days: int = 30,
    minimum_regimes: int = 2,
) -> dict:
    """Fail-closed coverage gate for any later strategy/OOS claim."""
    project = Path(root)
    normalized_bot = str(bot).strip().upper()
    normalized_mode = str(mode).strip().upper()
    if normalized_bot not in EXPECTANCY_FEATURES:
        raise ValueError("unsupported bot")
    if normalized_mode not in {"LIVE", "SIM"}:
        raise ValueError("mode must be LIVE or SIM")
    required_days = _positive_integer(
        minimum_observed_days, "minimum_observed_days"
    )
    required_regimes = _positive_integer(minimum_regimes, "minimum_regimes")
    diagnostics: Counter = Counter()
    candidate_times = sorted(
        event["candidate_time"]
        for event in _candidate_events(
            project,
            normalized_bot,
            normalized_mode,
            diagnostics=diagnostics,
        ).values()
        if isinstance(event.get("candidate_time"), datetime)
    )
    observed_days = sorted({stamp.date().isoformat() for stamp in candidate_times})
    span_days = (
        (candidate_times[-1].date() - candidate_times[0].date()).days + 1
        if candidate_times
        else 0
    )
    regimes: set[str] = set()
    path = project / "data" / "trading_bot.db"
    if candidate_times:
        try:
            conn = _read_connection(path)
        except FileNotFoundError:
            conn = None
        if conn is not None:
            try:
                if _table_exists(conn, "market_regime"):
                    for row in conn.execute(
                        """SELECT regime FROM market_regime
                            WHERE timestamp >= ? AND timestamp <= ?""",
                        (
                            candidate_times[0].strftime("%Y-%m-%d %H:%M:%S"),
                            candidate_times[-1].strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                    ):
                        regime = str(row["regime"] or "").strip().upper()
                        if regime and regime not in {"UNKNOWN", "CACHED_FG"}:
                            regimes.add(regime)
            finally:
                conn.close()
    days_ready = len(observed_days) >= required_days
    regimes_ready = len(regimes) >= required_regimes
    return {
        "first_candidate_at": (
            candidate_times[0].isoformat() if candidate_times else None
        ),
        "last_candidate_at": (
            candidate_times[-1].isoformat() if candidate_times else None
        ),
        "calendar_span_days": span_days,
        "observed_utc_days": len(observed_days),
        "minimum_observed_days": required_days,
        "regimes": sorted(regimes),
        "minimum_regimes": required_regimes,
        "candidate_records_rejected": diagnostics[
            "candidate_records_rejected"
        ],
        "days_ready": days_ready,
        "regimes_ready": regimes_ready,
        "ready": days_ready and regimes_ready,
    }


def build_exit_evidence_report(root: str | Path) -> dict:
    """Separate strategy exits from shutdown/reconcile/risk observations."""
    path = Path(root) / "data" / "trading_bot.db"
    try:
        conn = _read_connection(path)
    except FileNotFoundError:
        return {"terminal_exits": 0, "by_class": {}, "by_scope": {}}
    try:
        if not _table_exists(conn, "trades"):
            return {"terminal_exits": 0, "by_class": {}, "by_scope": {}}
        columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(trades)")
        }
        required = {"bot_name", "is_sim", "is_partial"}
        if not required <= columns:
            return {"terminal_exits": 0, "by_class": {}, "by_scope": {}}
        reason_sql = "reason" if "reason" in columns else "NULL AS reason"
        rows = conn.execute(
            "SELECT bot_name, is_sim, " + reason_sql + " "
            "FROM trades WHERE COALESCE(is_partial,0)=0"
        ).fetchall()
    finally:
        conn.close()
    from trading.exit_evidence import classify_exit_reason

    by_class: Counter = Counter()
    by_scope: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        exit_class = classify_exit_reason(row["reason"])
        bot = str(row["bot_name"] or "UNKNOWN").strip().upper()
        mode = "SIM" if int(row["is_sim"] or 0) == 1 else "LIVE"
        by_class[exit_class] += 1
        by_scope[f"{bot}|{mode}"][exit_class] += 1
    return {
        "terminal_exits": len(rows),
        "by_class": dict(sorted(by_class.items())),
        "by_scope": {
            scope: dict(sorted(counts.items()))
            for scope, counts in sorted(by_scope.items())
        },
        "training_label_policy": "strategy_exit_only",
    }


def build_research_status(
    root: str | Path,
    *,
    minimum_cost_samples: int = 50,
    minimum_expectancy_rows: int = 1_200,
) -> dict:
    required_expectancy_rows = _positive_integer(
        minimum_expectancy_rows, "minimum_expectancy_rows"
    )
    project = Path(root)
    expectancy = {}
    for bot in EXPECTANCY_FEATURES:
        expectancy[bot] = {}
        for mode in ("LIVE", "SIM"):
            events = _candidate_events(project, bot, mode)
            selected, _features, labels, schema_counts = select_expectancy_schema(
                project,
                bot=bot,
                mode=mode,
                minimum_rows=required_expectancy_rows,
            )
            expectancy[bot][mode] = {
                "candidate_events": len(events),
                "closed_labels": len(labels),
                "schema_version": selected,
                "closed_labels_by_schema": schema_counts,
                "minimum_rows": required_expectancy_rows,
                "ready": len(labels) >= required_expectancy_rows,
                "observation_readiness": build_observation_readiness(
                    project, bot=bot, mode=mode
                ),
            }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root": str(project.resolve()),
        "execution_cost": build_execution_cost_report(
            project, minimum_samples=minimum_cost_samples
        ),
        "ofi": build_ofi_report(project),
        "carry": build_carry_preview(project),
        "expectancy": expectancy,
        "exit_evidence": build_exit_evidence_report(project),
        "experiments": _registry_status(project),
        "safety": {
            "writes_live_model": False,
            "changes_orders": False,
            "automatic_promotion": False,
        },
    }


def train_expectancy_candidate(
    root: str | Path,
    *,
    bot: str,
    mode: str,
    min_train: int = 1_000,
    test_size: int = 200,
    purge_days: int = 8,
    schema_version: int | None = None,
) -> dict:
    project = Path(root)
    normalized_bot = str(bot).strip().upper()
    normalized_mode = str(mode).strip().upper()
    schemas = EXPECTANCY_FEATURE_SCHEMAS.get(normalized_bot)
    if schemas is None:
        raise ValueError("unsupported bot")
    normalized_min_train, normalized_test_size = normalize_walk_forward_sizes(
        min_train, test_size
    )
    normalized_purge_days = _nonnegative_integer(purge_days, "purge_days")
    if normalized_purge_days > timedelta.max.days:
        raise ValueError("purge_days exceeds timedelta range")
    if schema_version is None:
        selected_schema, feature_order, labels, schema_counts = select_expectancy_schema(
            project,
            bot=normalized_bot,
            mode=normalized_mode,
            minimum_rows=normalized_min_train + normalized_test_size,
        )
    else:
        selected_schema = _positive_integer(schema_version, "schema_version")
        feature_order = schemas.get(selected_schema)
        if feature_order is None:
            raise ValueError("unsupported expectancy schema version")
        labels = build_expectancy_candidates(
            project,
            bot=normalized_bot,
            mode=normalized_mode,
            schema_version=selected_schema,
        )
        schema_counts = {selected_schema: len(labels)}
    result = expanding_walk_forward_fit(
        labels,
        feature_order=feature_order,
        min_train=normalized_min_train,
        test_size=normalized_test_size,
        purge=timedelta(days=normalized_purge_days),
    )
    candidate_dir = project / "data" / "research" / "expectancy"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    model_path = candidate_dir / (
        f"{normalized_bot.lower()}_{normalized_mode.lower()}_v{selected_schema}.candidate.json"
    )
    from trading.expectancy_runtime import save_expectancy_model

    save_expectancy_model(model_path, result.final_model)
    report = {
        "bot": normalized_bot,
        "mode": normalized_mode,
        "feature_order": list(feature_order),
        "schema_version": selected_schema,
        "closed_labels_by_schema": schema_counts,
        "labels": len(labels),
        "folds": [asdict(fold) for fold in result.folds],
        "predictions": len(result.predictions),
        "calibration": asdict(result.calibration),
        "candidate_model": str(model_path),
        "runtime_model_changed": False,
    }
    report_path = model_path.with_suffix(".report.json")
    temp = report_path.with_suffix(".tmp")
    temp.write_text(
        json.dumps(report, default=str, sort_keys=True, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    temp.replace(report_path)
    report["report"] = str(report_path)
    return report


def check_promotion(
    evidence: dict,
    *,
    minimum_samples: int,
    manual_live_approval: bool = False,
) -> dict:
    try:
        normalized_evidence = PromotionEvidence(**dict(evidence))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("promotion evidence is invalid") from exc
    decision = evaluate_promotion(
        normalized_evidence,
        minimum_samples=minimum_samples,
        explicit_manual_live_approval=manual_live_approval,
    )
    return {
        "research_passed": decision.research_passed,
        "live_allowed": decision.live_allowed,
        "reasons": list(decision.reasons),
        "deployment_performed": False,
    }


def register_experiment_trial(
    root: str | Path,
    *,
    trial_id: str,
    experiment_name: str,
    params: dict,
    status: str,
) -> dict:
    """Explicitly append one immutable research trial; never changes trading."""
    trial_id, experiment_name, normalized_status = normalize_experiment_metadata(
        trial_id, experiment_name, status
    )
    encoded = encode_experiment_params(params)
    db_path = Path(root) / "data" / "trading_bot.db"
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    conn = sqlite3.connect(db_path, timeout=15)
    try:
        conn.execute("PRAGMA busy_timeout=15000")
        if not _table_exists(conn, "experiment_registry"):
            raise ValueError("experiment registry is unavailable")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO experiment_registry "
            "(trial_id, experiment_name, params_json, status, created_at) "
            "VALUES (?,?,?,?,?)",
            (
                trial_id,
                experiment_name,
                encoded,
                normalized_status,
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise ValueError("experiment trial ids are immutable and unique") from exc
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "trial_id": trial_id,
        "experiment_name": experiment_name,
        "status": normalized_status,
        "trading_changed": False,
    }
