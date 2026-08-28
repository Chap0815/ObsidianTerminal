"""Read-only profit research orchestration with explicit promotion boundaries."""
from __future__ import annotations

import bisect
import hashlib
import json
import math
import sqlite3
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from core.constants import MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS
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


_CANDIDATE_LOG_DIRS = {
    "CROSS": "Cross",
    "FUTREND": "FuTrend",
    "FUTURES": "Futures",
    "SPOT": "Spot",
    "TREND": "Trend",
}
_MINIMUM_REGIME_CANDIDATE_COVERAGE = 0.95
_MINIMUM_DIRECTION_CANDIDATE_COVERAGE = 0.95
_MINIMUM_LIFECYCLE_TERMINAL_COVERAGE = 0.95
_PROMOTION_DECISION_INPUT_SCHEMA = 1
_TERMINAL_LIFECYCLE_STAGES = frozenset({
    "opened", "blocked", "aborted", "order_failed", "state_failed",
})


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


def _candidate_direction(value) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized if normalized in {"LONG", "SHORT"} else None


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


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: Path, *, label: str) -> Path:
    requested = path.expanduser().absolute()
    components = (requested, *requested.parents)
    if any(_is_linklike(component) for component in components):
        raise ValueError(f"{label} must not contain links")
    return requested


def _real_project_root(root: str | Path) -> Path:
    try:
        project = _absolute_without_links(Path(root), label="project root")
    except ValueError as exc:
        raise ValueError("project root must be a real directory without links") from exc
    if not project.is_dir():
        raise ValueError("project root must be a real directory without links")
    return project


def _read_connection(path: Path) -> sqlite3.Connection:
    try:
        requested = _absolute_without_links(path, label="research SQLite source")
    except ValueError as exc:
        raise FileNotFoundError(path) from exc
    if not requested.is_file():
        raise FileNotFoundError(requested)
    conn = sqlite3.connect(
        f"file:{requested.as_posix()}?mode=ro", uri=True, timeout=15
    )
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
    enforce_tca_causality: bool = False,
) -> dict[str, dict]:
    selected: dict[str, dict] = {}
    causal_anchors: dict[str, datetime] = {}
    database = root / "data" / "trading_bot.db"
    try:
        conn = _read_connection(database)
    except FileNotFoundError:
        conn = None
    if conn is not None:
        try:
            conn.execute("BEGIN")
            if _table_exists(conn, "expectancy_candidates"):
                context_available = False
                if _table_exists(conn, "expectancy_candidate_context"):
                    context_columns = {
                        str(row["name"])
                        for row in conn.execute(
                            "PRAGMA table_info(expectancy_candidate_context)"
                        )
                    }
                    context_available = {
                        "entry_id", "direction"
                    } <= context_columns
                context_join = (
                    "LEFT JOIN expectancy_candidate_context context "
                    "ON context.entry_id=candidate.entry_id"
                    if context_available else ""
                )
                direction_expr = (
                    "context.direction" if context_available else "NULL"
                )
                regime_context_available = False
                if _table_exists(conn, "expectancy_candidate_regime_context"):
                    regime_context_columns = {
                        str(row["name"])
                        for row in conn.execute(
                            "PRAGMA table_info(expectancy_candidate_regime_context)"
                        )
                    }
                    regime_context_available = {
                        "entry_id", "evidence_state", "reason", "regime",
                        "observed_at", "btc_24h", "btc_7d", "fear_greed",
                        "source",
                    } <= regime_context_columns
                regime_context_join = (
                    "LEFT JOIN expectancy_candidate_regime_context regime_context "
                    "ON regime_context.entry_id=candidate.entry_id"
                    if regime_context_available else ""
                )
                regime_exprs = (
                    "regime_context.evidence_state AS regime_evidence_state, "
                    "regime_context.reason AS regime_reason, "
                    "regime_context.regime AS candidate_regime, "
                    "regime_context.observed_at AS regime_observed_at, "
                    "regime_context.btc_24h AS regime_btc_24h, "
                    "regime_context.btc_7d AS regime_btc_7d, "
                    "regime_context.fear_greed AS regime_fear_greed, "
                    "regime_context.source AS regime_source "
                    if regime_context_available else
                    "NULL AS regime_evidence_state, NULL AS regime_reason, "
                    "NULL AS candidate_regime, NULL AS regime_observed_at, "
                    "NULL AS regime_btc_24h, NULL AS regime_btc_7d, "
                    "NULL AS regime_fear_greed, NULL AS regime_source "
                )
                rows = conn.execute(
                    "SELECT candidate.entry_id, candidate.candidate_time, "
                    "candidate.schema_version, candidate.features_json, "
                    f"{direction_expr} AS candidate_direction, "
                    f"{regime_exprs}"
                    "FROM expectancy_candidates candidate "
                    f"{context_join} "
                    f"{regime_context_join} "
                    "WHERE candidate.bot_name=? AND candidate.mode=? "
                    "ORDER BY candidate.candidate_time, candidate.entry_id",
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
                        raw_direction = row["candidate_direction"]
                        direction = _candidate_direction(raw_direction)
                        raw_regime = row["candidate_regime"]
                        direct_regime = None
                        direct_regime_status = "missing"
                        direct_regime_reason = None
                        direct_observed_at = _utc_datetime(
                            row["regime_observed_at"]
                        )
                        direct_present = any(
                            row[name] is not None
                            for name in (
                                "regime_evidence_state", "regime_reason",
                                "candidate_regime",
                                "regime_observed_at", "regime_btc_24h",
                                "regime_btc_7d", "regime_fear_greed",
                                "regime_source",
                            )
                        )
                        if direct_present:
                            evidence_state = str(
                                row["regime_evidence_state"] or ""
                            ).strip().upper()
                            evidence_reason = str(
                                row["regime_reason"] or ""
                            ).strip()
                            normalized_regime = str(raw_regime or "").strip().upper()
                            btc_24h = _finite(row["regime_btc_24h"])
                            btc_7d = _finite(row["regime_btc_7d"])
                            fear_greed = _finite(row["regime_fear_greed"])
                            age = (
                                (candidate_time - direct_observed_at).total_seconds()
                                if direct_observed_at is not None else None
                            )
                            if (
                                evidence_state == "CAPTURED"
                                and evidence_reason == ""
                                and normalized_regime
                                in {"BULL", "BEAR", "NEUTRAL"}
                                and btc_24h is not None
                                and btc_7d is not None
                                and fear_greed is not None
                                and fear_greed.is_integer()
                                and 0.0 <= fear_greed <= 100.0
                                and row["regime_source"] == "market_regime_snapshot"
                                and age is not None
                                and 0.0 <= age
                                <= MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS
                            ):
                                direct_regime = normalized_regime
                                direct_regime_status = "valid"
                            elif (
                                evidence_state in {"MISSING", "STALE", "INVALID"}
                                and evidence_reason
                                and raw_regime is None
                                and direct_observed_at is None
                                and row["regime_btc_24h"] is None
                                and row["regime_btc_7d"] is None
                                and row["regime_fear_greed"] is None
                                and row["regime_source"]
                                == "market_regime_snapshot"
                            ):
                                direct_regime_status = "unavailable"
                                direct_regime_reason = (
                                    f"{evidence_state.lower()}:{evidence_reason}"
                                )
                            else:
                                direct_regime_status = "invalid"
                        selected[entry_id] = {
                            "entry_id": entry_id,
                            "candidate_time": candidate_time,
                            "features": features,
                            "schema_version": schema_version,
                            "direction": direction,
                            "direction_status": (
                                "valid" if direction is not None
                                else "missing" if raw_direction is None
                                or raw_direction == ""
                                else "invalid"
                            ),
                            "direct_regime": direct_regime,
                            "direct_regime_status": direct_regime_status,
                            "direct_regime_reason": direct_regime_reason,
                            "direct_regime_context_available": (
                                regime_context_available
                            ),
                            "source": "sqlite",
                        }
                    elif diagnostics is not None:
                        diagnostics["candidate_records_rejected"] += 1
            if enforce_tca_causality:
                anchor_rows = []
                if mode == "SIM" and _table_exists(conn, "sim_execution_tca"):
                    sim_tca_columns = {
                        str(row["name"])
                        for row in conn.execute(
                            "PRAGMA table_info(sim_execution_tca)"
                        )
                    }
                    if "measured_at" not in sim_tca_columns:
                        sim_tca_columns.clear()
                else:
                    sim_tca_columns = set()
                if mode == "SIM" and "measured_at" in sim_tca_columns:
                    anchor_rows.extend(
                        conn.execute(
                            "SELECT t.entry_id,t.measured_at "
                            "FROM sim_execution_tca t "
                            "JOIN expectancy_candidates e "
                            "ON e.entry_id=t.entry_id "
                            "WHERE t.stage IN ('arrival','fill') "
                            "AND UPPER(e.bot_name)=? AND UPPER(e.mode)=?",
                            (bot, mode),
                        ).fetchall()
                    )
                elif (
                    mode == "LIVE"
                    and _table_exists(conn, "execution_tca")
                    and _table_exists(conn, "order_intents")
                ):
                    execution_tca_columns = {
                        str(row["name"])
                        for row in conn.execute(
                            "PRAGMA table_info(execution_tca)"
                        )
                    }
                    if "measured_at" in execution_tca_columns:
                        anchor_rows.extend(
                            conn.execute(
                                "SELECT t.intent_id AS entry_id,t.measured_at "
                                "FROM execution_tca t JOIN order_intents i "
                                "ON i.intent_id=t.intent_id "
                                "WHERE t.stage IN ('arrival','fill') "
                                "AND UPPER(i.bot_name)=? AND UPPER(i.mode)=?",
                                (bot, mode),
                            ).fetchall()
                        )
                if mode == "SIM" and _table_exists(
                    conn, "candidate_microstructure"
                ):
                    anchor_rows.extend(
                        conn.execute(
                            "SELECT entry_id,measured_at "
                            "FROM candidate_microstructure "
                            "WHERE UPPER(bot_name)=? AND UPPER(mode)=? "
                            "AND stage='arrival_book_unavailable'",
                            (bot, mode),
                        ).fetchall()
                    )
                for row in anchor_rows:
                    entry_id = str(row["entry_id"] or "").strip()
                    anchor = _utc_datetime(row["measured_at"])
                    current = causal_anchors.get(entry_id)
                    if entry_id and anchor is not None and (
                        current is None or anchor < current
                    ):
                        causal_anchors[entry_id] = anchor
        finally:
            try:
                if conn.in_transaction:
                    conn.rollback()
            finally:
                conn.close()
    log_dir_name = _CANDIDATE_LOG_DIRS.get(bot)
    logs = root / "logs" / log_dir_name if log_dir_name else None
    paths = []
    if logs is not None and logs.is_dir():
        for path in logs.glob("structured.jsonl*"):
            suffix = path.name.removeprefix("structured.jsonl")
            if suffix == "" or (
                suffix.startswith(".") and suffix[1:].isdigit()
            ):
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
                raw_direction = event.get("direction")
                direction = _candidate_direction(raw_direction)
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
                        "entry_id": entry_id,
                        "candidate_time": candidate_time,
                        "features": features,
                        "schema_version": schema_version,
                        "direction": direction,
                        "direction_status": (
                            "valid" if direction is not None
                            else "missing" if raw_direction is None
                            or raw_direction == ""
                            else "invalid"
                        ),
                        "source": "structured_log_legacy",
                    }
    if enforce_tca_causality:
        for entry_id, event in list(selected.items()):
            anchor = causal_anchors.get(entry_id)
            if anchor is not None and event["candidate_time"] > anchor:
                selected.pop(entry_id)
                if diagnostics is not None:
                    diagnostics["noncausal_candidate_anchor"] += 1
    return selected


def _complete_expectancy_campaign(
    campaign: list[sqlite3.Row],
    *,
    normalized_bot: str,
    expected_is_sim: int,
) -> tuple[dict | None, str | None]:
    """Validate and aggregate one authoritative entry-id trade campaign."""
    allowed_bot_names = {normalized_bot}
    if expected_is_sim:
        allowed_bot_names.add(f"{normalized_bot} (SIM)")
    bot_names = set()
    scopes = set()
    parsed_rows = []
    for row in campaign:
        partial = row["is_partial"]
        row_is_sim = row["is_sim"]
        if (
            isinstance(partial, bool)
            or not isinstance(partial, int)
            or partial not in (0, 1)
            or isinstance(row_is_sim, bool)
            or not isinstance(row_is_sim, int)
            or row_is_sim not in (0, 1)
        ):
            return None, "invalid_trade_fragment"
        raw_bot_name = row["bot_name"]
        if (
            not isinstance(raw_bot_name, str)
            or not raw_bot_name.strip()
            or raw_bot_name != raw_bot_name.strip()
        ):
            return None, "invalid_trade_fragment"
        symbol = row["symbol"]
        is_futures = row["is_futures"]
        position_type = row["position_type"]
        if symbol is not None and (
            not isinstance(symbol, str)
            or not symbol.strip()
            or symbol != symbol.strip()
        ):
            return None, "invalid_trade_fragment"
        if is_futures is not None and (
            isinstance(is_futures, bool)
            or not isinstance(is_futures, int)
            or is_futures not in (0, 1)
        ):
            return None, "invalid_trade_fragment"
        if position_type is not None and (
            not isinstance(position_type, str)
            or not position_type.strip()
            or position_type != position_type.strip()
        ):
            return None, "invalid_trade_fragment"
        bot_names.add(raw_bot_name)
        scopes.add((
            raw_bot_name,
            symbol,
            is_futures,
            position_type,
            row_is_sim,
        ))
        parsed_rows.append((row, partial))
    if (
        len(bot_names) != 1
        or not bot_names.issubset(allowed_bot_names)
        or len(scopes) != 1
        or any(row["is_sim"] != expected_is_sim for row, _ in parsed_rows)
    ):
        return None, "trade_scope_conflict"
    terminal_rows = [row for row, partial in parsed_rows if partial == 0]
    if len(terminal_rows) != 1:
        return None, "terminal_count_invalid"
    opened_values = [_utc_datetime(row["buy_time"]) for row in campaign]
    opened = {value for value in opened_values if value is not None}
    if any(value is None for value in opened_values) or len(opened) != 1:
        return None, "missing_or_ambiguous_entry_anchor"
    closed_values = [_utc_datetime(row["sell_time"]) for row in campaign]
    if any(value is None for value in closed_values):
        return None, "invalid_trade_fragment"
    ordered = sorted(
        zip(campaign, closed_values),
        key=lambda item: (item[1], int(item[0]["_row_id"])),
    )
    terminal_row = terminal_rows[0]
    if ordered[-1][0]["_row_id"] != terminal_row["_row_id"]:
        return None, "terminal_not_last"
    opened_at = next(iter(opened))
    if any(value < opened_at for value in closed_values):
        return None, "noncausal_trade_close"
    profits = [_finite(row["profit_usdt"]) for row in campaign]
    invested_values = [_finite(row["invested_usdt"]) for row in campaign]
    if (
        any(value is None for value in profits + invested_values)
        or any(float(value) <= 0.0 for value in invested_values)
    ):
        return None, "invalid_trade_cashflow"
    try:
        profit = math.fsum(float(value) for value in profits)
        invested = math.fsum(float(value) for value in invested_values)
    except OverflowError:
        return None, "invalid_trade_cashflow"
    if (
        not math.isfinite(profit)
        or not math.isfinite(invested)
        or invested <= 0.0
    ):
        return None, "invalid_trade_cashflow"
    closed_at = _utc_datetime(terminal_row["sell_time"])
    if closed_at is None:
        return None, "invalid_trade_fragment"
    return {
        "opened_at": opened_at,
        "closed_at": closed_at,
        "terminal_reason": terminal_row["reason"],
        "profit": profit,
        "invested": invested,
    }, None


def build_expectancy_candidates(
    root: str | Path,
    *,
    bot: str,
    mode: str,
    schema_version: int = 1,
    strategy_exits_only: bool = True,
    rejection_counts: Counter | None = None,
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
    candidates = _candidate_events(
        project,
        normalized_bot,
        normalized_mode,
        diagnostics=rejection_counts,
    )
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
        optional_columns = ("reason", "symbol", "is_futures", "position_type")
        optional_sql = [
            name if name in trade_columns else f"NULL AS {name}"
            for name in optional_columns
        ]
        trade_rows = conn.execute(
            "SELECT rowid AS _row_id, entry_id, bot_name, buy_time, sell_time, "
            "profit_usdt, invested_usdt, is_partial, is_sim, "
            + ", ".join(optional_sql)
            + " "
            "FROM trades WHERE entry_id IS NOT NULL ORDER BY sell_time"
        ).fetchall()
    finally:
        conn.close()
    expected_is_sim = 1 if normalized_mode == "SIM" else 0
    labels = []
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for trade in trade_rows:
        entry_id = trade["entry_id"]
        if (
            isinstance(entry_id, str)
            and entry_id
            and entry_id == entry_id.strip()
        ):
            grouped[entry_id].append(trade)
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
        complete, rejection_reason = _complete_expectancy_campaign(
            campaign,
            normalized_bot=normalized_bot,
            expected_is_sim=expected_is_sim,
        )
        if rejection_reason is not None:
            if rejection_counts is not None:
                rejection_counts[rejection_reason] += 1
            continue
        if complete is None:
            continue
        if event["candidate_time"] > complete["opened_at"]:
            if rejection_counts is not None:
                rejection_counts["noncausal_candidate_entry"] += 1
            continue
        if strategy_exits_only:
            from trading.exit_evidence import is_strategy_exit

            if not is_strategy_exit(complete["terminal_reason"]):
                continue
        try:
            net_return_bps = (
                complete["profit"] / complete["invested"] * 10_000.0
            )
        except (OverflowError, ZeroDivisionError):
            net_return_bps = math.nan
        if not math.isfinite(net_return_bps):
            if rejection_counts is not None:
                rejection_counts["invalid_trade_cashflow"] += 1
            continue
        if complete["closed_at"] < event["candidate_time"]:
            if rejection_counts is not None:
                rejection_counts["noncausal_trade_close"] += 1
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
                    label_closed_time=complete["closed_at"],
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
    rejection_counts: Counter | None = None,
) -> tuple[int, tuple[str, ...], list[CandidateLabel], dict[int, int]]:
    """Prefer the newest adequately sampled schema, without mixing versions."""
    required = _positive_integer(minimum_rows, "minimum_rows")
    normalized_bot = str(bot).strip().upper()
    schemas = EXPECTANCY_FEATURE_SCHEMAS.get(normalized_bot)
    if not schemas:
        raise ValueError("unsupported bot")
    labels_by_version = {}
    rejections_by_version = {}
    for version in sorted(schemas):
        version_rejections = Counter()
        labels_by_version[version] = build_expectancy_candidates(
            root,
            bot=normalized_bot,
            mode=mode,
            schema_version=version,
            rejection_counts=version_rejections,
        )
        rejections_by_version[version] = version_rejections
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
    if rejection_counts is not None:
        rejection_counts.update(rejections_by_version[selected])
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
        candidate_columns = (
            {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(expectancy_candidates)"
                )
            }
            if has_candidates else set()
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
        candidate_time_expr = (
            "e.candidate_time" if "candidate_time" in candidate_columns
            else "NULL"
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
            f"{symbol_expr} AS symbol, {notional_expr} AS filled_notional, "
            f"{candidate_time_expr} AS candidate_time "
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
                f"""SELECT t.id AS tca_id, t.entry_id AS intent_id,
                          t.measured_at, t.stage, t.payload_json,
                          e.symbol AS symbol, NULL AS filled_notional,
                          {candidate_time_expr} AS candidate_time
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
        metadata[key] = (
            row["symbol"], row["filled_notional"], row["candidate_time"]
        )
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
        symbol, notional, candidate_time = metadata[key]
        if candidate_time is not None:
            candidate_at = _utc_datetime(candidate_time)
            arrival_at = _utc_datetime(arrival_time)
            if (
                candidate_at is None
                or arrival_at is None
                or candidate_at > arrival_at
            ):
                if rejection_counts is not None:
                    rejection_counts["noncausal_candidate_anchor"] += 1
                continue
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
    decision_payload = asdict(decision)
    decision_payload["reasons"] = list(decision.reasons)
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
        "decision": decision_payload,
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
    candidate_events = list(
        _candidate_events(
            project,
            normalized_bot,
            normalized_mode,
            diagnostics=diagnostics,
            enforce_tca_causality=True,
        ).values()
    )
    candidate_times = sorted(
        event["candidate_time"]
        for event in candidate_events
        if isinstance(event.get("candidate_time"), datetime)
    )
    direction_counts = Counter(
        event["direction"]
        for event in candidate_events
        if event.get("direction") in {"LONG", "SHORT"}
    )
    direction_rejections = Counter()
    for event in candidate_events:
        if event.get("direction_status") == "invalid":
            direction_rejections["invalid_candidate_direction"] += 1
        elif event.get("direction") not in {"LONG", "SHORT"}:
            direction_rejections["missing_candidate_direction"] += 1
    lifecycle_stages: dict[str, set[str]] = defaultdict(set)
    lifecycle_stage_counts: Counter = Counter()
    observed_days = sorted({stamp.date().isoformat() for stamp in candidate_times})
    span_days = (
        (candidate_times[-1].date() - candidate_times[0].date()).days + 1
        if candidate_times
        else 0
    )
    direct_regimes: set[str] = set()
    inferred_regimes: set[str] = set()
    regime_rejections: Counter = Counter()
    invalid_regime_rows = 0
    direct_regime_candidate_records = 0
    inferred_regime_candidate_records = 0
    direct_regime_context_available = any(
        bool(event.get("direct_regime_context_available"))
        for event in candidate_events
    )
    for event in candidate_events:
        if event.get("direct_regime_status") == "valid":
            direct_regime_candidate_records += 1
            direct_regimes.add(str(event["direct_regime"]))
        elif event.get("direct_regime_status") == "invalid":
            regime_rejections["invalid_direct_candidate_regime"] += 1
        elif event.get("direct_regime_status") == "unavailable":
            reason = str(event.get("direct_regime_reason") or "unknown")
            regime_rejections[
                f"direct_candidate_regime_{reason}"
            ] += 1
    inferable_regime_candidates = sum(
        event.get("direct_regime_status") == "missing"
        for event in candidate_events
    )
    path = project / "data" / "trading_bot.db"
    if candidate_times:
        try:
            conn = _read_connection(path)
        except FileNotFoundError:
            conn = None
        if conn is not None:
            try:
                if _table_exists(conn, "market_regime"):
                    regime_rows = conn.execute(
                        """SELECT timestamp,regime FROM market_regime
                            WHERE timestamp >= ? AND timestamp <= ?
                              AND regime <> 'CACHED_FG'
                            ORDER BY timestamp""",
                        (
                            (
                                candidate_times[0]
                                - timedelta(
                                    seconds=MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS
                                )
                            ).strftime("%Y-%m-%d %H:%M:%S"),
                            candidate_times[-1].strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                    ).fetchall()
                    by_timestamp: dict[datetime, str | None] = {}
                    for row in regime_rows:
                        timestamp = _utc_datetime(row["timestamp"])
                        regime = str(row["regime"] or "").strip().upper()
                        if timestamp is None:
                            invalid_regime_rows += 1
                            continue
                        if regime not in {"BULL", "BEAR", "NEUTRAL"}:
                            invalid_regime_rows += 1
                            by_timestamp[timestamp] = None
                            continue
                        current = by_timestamp.get(timestamp)
                        if current is not None and current != regime:
                            by_timestamp[timestamp] = None
                        elif timestamp not in by_timestamp:
                            by_timestamp[timestamp] = regime
                    regime_times = sorted(by_timestamp)
                    for event in candidate_events:
                        candidate_time = event.get("candidate_time")
                        if not isinstance(candidate_time, datetime):
                            continue
                        # Only schema-legacy rows may use mutable historical
                        # inference.  Every explicit capture outcome is final.
                        if event.get("direct_regime_status") != "missing":
                            continue
                        index = bisect.bisect_right(
                            regime_times, candidate_time
                        ) - 1
                        if index < 0:
                            regime_rejections["missing_prior_regime"] += 1
                            continue
                        regime_time = regime_times[index]
                        regime = by_timestamp[regime_time]
                        if regime is None:
                            regime_rejections[
                                "invalid_or_ambiguous_regime_anchor"
                            ] += 1
                            continue
                        age_seconds = (
                            candidate_time - regime_time
                        ).total_seconds()
                        if age_seconds > MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS:
                            regime_rejections["stale_regime_candidate"] += 1
                            continue
                        inferred_regimes.add(regime)
                        inferred_regime_candidate_records += 1
                else:
                    regime_rejections[
                        "missing_prior_regime"
                    ] += inferable_regime_candidates
                if _table_exists(conn, "entry_lifecycle_events"):
                    lifecycle_columns = {
                        str(row["name"])
                        for row in conn.execute(
                            "PRAGMA table_info(entry_lifecycle_events)"
                        )
                    }
                    required_lifecycle_columns = {
                        "entry_id", "stage", "bot_name", "mode"
                    }
                    if required_lifecycle_columns <= lifecycle_columns:
                        selected_ids = {
                            event["entry_id"] for event in candidate_events
                        }
                        for row in conn.execute(
                            "SELECT entry_id,stage FROM entry_lifecycle_events "
                            "WHERE UPPER(bot_name)=? AND UPPER(mode)=?",
                            (normalized_bot, normalized_mode),
                        ):
                            entry_id = str(row["entry_id"] or "").strip()
                            stage = str(row["stage"] or "").strip().lower()
                            if entry_id in selected_ids and stage:
                                lifecycle_stages[entry_id].add(stage)
                                lifecycle_stage_counts[stage] += 1
            finally:
                conn.close()
        else:
            regime_rejections[
                "missing_prior_regime"
            ] += inferable_regime_candidates
    days_ready = len(observed_days) >= required_days
    regimes_ready = len(direct_regimes) >= required_regimes
    valid_candidate_records = len(candidate_times)
    rejected_candidate_records = sum(diagnostics.values())
    total_candidate_records = valid_candidate_records + rejected_candidate_records
    candidate_quality_coverage = (
        valid_candidate_records / total_candidate_records
        if total_candidate_records else 0.0
    )
    minimum_candidate_quality_coverage = 0.95
    quality_ready = (
        total_candidate_records > 0
        and candidate_quality_coverage >= minimum_candidate_quality_coverage
    )
    regime_candidate_records = (
        direct_regime_candidate_records + inferred_regime_candidate_records
    )
    regime_candidate_coverage = (
        regime_candidate_records / valid_candidate_records
        if valid_candidate_records else 0.0
    )
    direct_regime_candidate_coverage = (
        direct_regime_candidate_records / valid_candidate_records
        if valid_candidate_records else 0.0
    )
    regime_quality_ready = (
        direct_regime_context_available
        and valid_candidate_records > 0
        and direct_regime_candidate_coverage
        >= _MINIMUM_REGIME_CANDIDATE_COVERAGE
    )
    direction_candidate_records = sum(direction_counts.values())
    direction_candidate_coverage = (
        direction_candidate_records / valid_candidate_records
        if valid_candidate_records else 0.0
    )
    direction_quality_ready = (
        valid_candidate_records > 0
        and direction_candidate_coverage
        >= _MINIMUM_DIRECTION_CANDIDATE_COVERAGE
    )
    lifecycle_rejections: Counter = Counter()
    lifecycle_terminal_records = 0
    for event in candidate_events:
        terminal_stages = (
            lifecycle_stages.get(event["entry_id"], set())
            & _TERMINAL_LIFECYCLE_STAGES
        )
        if len(terminal_stages) == 1:
            lifecycle_terminal_records += 1
        elif not terminal_stages:
            lifecycle_rejections["missing_terminal_lifecycle"] += 1
        else:
            lifecycle_rejections["conflicting_terminal_lifecycle"] += 1
    lifecycle_terminal_coverage = (
        lifecycle_terminal_records / valid_candidate_records
        if valid_candidate_records else 0.0
    )
    lifecycle_quality_ready = (
        valid_candidate_records > 0
        and lifecycle_terminal_coverage
        >= _MINIMUM_LIFECYCLE_TERMINAL_COVERAGE
    )
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
        "regimes": sorted(direct_regimes),
        "inferred_regimes": sorted(inferred_regimes),
        "minimum_regimes": required_regimes,
        "regime_candidate_records": regime_candidate_records,
        "regime_candidate_coverage": regime_candidate_coverage,
        "direct_regime_context_available": direct_regime_context_available,
        "direct_regime_candidate_records": direct_regime_candidate_records,
        "direct_regime_candidate_coverage": direct_regime_candidate_coverage,
        "inferred_regime_candidate_records": inferred_regime_candidate_records,
        "minimum_regime_candidate_coverage": (
            _MINIMUM_REGIME_CANDIDATE_COVERAGE
        ),
        "regime_evidence_max_age_seconds": (
            MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS
        ),
        "regime_rejections": dict(sorted(regime_rejections.items())),
        "invalid_regime_rows": invalid_regime_rows,
        "valid_candidate_records": valid_candidate_records,
        "total_candidate_records": total_candidate_records,
        "candidate_quality_coverage": candidate_quality_coverage,
        "minimum_candidate_quality_coverage": minimum_candidate_quality_coverage,
        "candidate_rejections": dict(sorted(diagnostics.items())),
        "candidate_records_rejected": diagnostics[
            "candidate_records_rejected"
        ],
        "days_ready": days_ready,
        "regimes_ready": regimes_ready,
        "quality_ready": quality_ready,
        "regime_quality_ready": regime_quality_ready,
        "directions": dict(sorted(direction_counts.items())),
        "direction_candidate_records": direction_candidate_records,
        "direction_candidate_coverage": direction_candidate_coverage,
        "minimum_direction_candidate_coverage": (
            _MINIMUM_DIRECTION_CANDIDATE_COVERAGE
        ),
        "direction_rejections": dict(sorted(direction_rejections.items())),
        "direction_quality_ready": direction_quality_ready,
        "lifecycle_stage_counts": dict(sorted(lifecycle_stage_counts.items())),
        "lifecycle_terminal_records": lifecycle_terminal_records,
        "lifecycle_terminal_coverage": lifecycle_terminal_coverage,
        "minimum_lifecycle_terminal_coverage": (
            _MINIMUM_LIFECYCLE_TERMINAL_COVERAGE
        ),
        "lifecycle_rejections": dict(sorted(lifecycle_rejections.items())),
        "lifecycle_quality_ready": lifecycle_quality_ready,
        "ready": (
            days_ready
            and regimes_ready
            and quality_ready
            and regime_quality_ready
            and direction_quality_ready
            and lifecycle_quality_ready
        ),
    }


def _empty_exit_evidence_report(*, schema_ready: bool = True) -> dict:
    rejections = {} if schema_ready else {"schema_unavailable": 1}
    return {
        "terminal_exits": 0,
        "by_class": {},
        "by_scope": {},
        "training_label_policy": "strategy_exit_only",
        "position_integrity": {
            "trade_fragments": 0,
            "valid_positions": 0,
            "rejected_positions": 0,
            "unkeyed_fragments": 0,
            "rejections": rejections,
            "ready": schema_ready,
        },
    }


def _canonical_exit_bot(raw_bot, is_sim) -> tuple[str, int] | None:
    """Normalize the legacy ``BOT (SIM)`` storage name without coercion."""
    if (
        isinstance(is_sim, bool)
        or not isinstance(is_sim, int)
        or is_sim not in (0, 1)
        or not isinstance(raw_bot, str)
        or not raw_bot
        or raw_bot != raw_bot.strip()
    ):
        return None
    suffix = " (SIM)"
    if raw_bot.endswith(suffix):
        if is_sim != 1:
            return None
        raw_bot = raw_bot[:-len(suffix)]
    if not raw_bot or raw_bot != raw_bot.strip() or raw_bot.endswith(suffix):
        return None
    canonical_bot = raw_bot.upper()
    if raw_bot != canonical_bot:
        return None
    return canonical_bot, is_sim


def _canonical_exit_scope(campaign: list[sqlite3.Row]) -> tuple[str, int] | None:
    """Return canonical bot plus SIM flag without hiding scope conflicts."""
    if not campaign:
        return None
    row = campaign[0]
    return _canonical_exit_bot(row["bot_name"], row["is_sim"])


def build_exit_evidence_report(root: str | Path) -> dict:
    """Classify exits from complete, scope-canonical independent positions."""
    path = Path(root) / "data" / "trading_bot.db"
    try:
        conn = _read_connection(path)
    except FileNotFoundError:
        return _empty_exit_evidence_report(schema_ready=False)
    try:
        if not _table_exists(conn, "trades"):
            return _empty_exit_evidence_report(schema_ready=False)
        columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(trades)")
        }
        required = {
            "entry_id", "bot_name", "buy_time", "sell_time", "profit_usdt",
            "invested_usdt", "is_sim", "is_partial",
        }
        if not required <= columns:
            return _empty_exit_evidence_report(schema_ready=False)
        optional_columns = ("reason", "symbol", "is_futures", "position_type")
        optional_sql = [
            name if name in columns else f"NULL AS {name}"
            for name in optional_columns
        ]
        rows = conn.execute(
            "SELECT rowid AS _row_id, entry_id, bot_name, buy_time, sell_time, "
            "profit_usdt, invested_usdt, is_partial, is_sim, "
            + ", ".join(optional_sql)
            + " FROM trades ORDER BY rowid"
        ).fetchall()
    finally:
        conn.close()
    from trading.exit_evidence import classify_exit_reason

    by_class: Counter = Counter()
    by_scope: dict[str, Counter] = defaultdict(Counter)
    rejections: Counter = Counter()
    unkeyed_fragments = 0
    grouped: dict[tuple, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        entry_id = row["entry_id"]
        if (
            isinstance(entry_id, str)
            and entry_id
            and entry_id == entry_id.strip()
            and len(entry_id) <= 64
            and not any(ord(char) < 32 or ord(char) == 127 for char in entry_id)
        ):
            key = ("entry_id", entry_id)
        elif entry_id is None or entry_id == "":
            legacy_scope = _canonical_exit_bot(row["bot_name"], row["is_sim"])
            key = (
                "legacy",
                legacy_scope, row["symbol"], row["buy_time"],
            )
        else:
            rejections["invalid_entry_id"] += 1
            unkeyed_fragments += 1
            continue
        grouped[key].append(row)

    valid_positions = 0
    for campaign in grouped.values():
        scope = _canonical_exit_scope(campaign)
        if scope is None:
            rejections["trade_scope_conflict"] += 1
            continue
        bot, is_sim = scope
        complete, rejection_reason = _complete_expectancy_campaign(
            campaign,
            normalized_bot=bot,
            expected_is_sim=is_sim,
        )
        if rejection_reason is not None or complete is None:
            rejections[rejection_reason or "invalid_trade_position"] += 1
            continue
        exit_class = classify_exit_reason(complete["terminal_reason"])
        mode = "SIM" if is_sim == 1 else "LIVE"
        by_class[exit_class] += 1
        by_scope[f"{bot}|{mode}"][exit_class] += 1
        valid_positions += 1
    return {
        "terminal_exits": valid_positions,
        "by_class": dict(sorted(by_class.items())),
        "by_scope": {
            scope: dict(sorted(counts.items()))
            for scope, counts in sorted(by_scope.items())
        },
        "training_label_policy": "strategy_exit_only",
        "position_integrity": {
            "trade_fragments": len(rows),
            "valid_positions": valid_positions,
            "rejected_positions": sum(rejections.values()) - unkeyed_fragments,
            "unkeyed_fragments": unkeyed_fragments,
            "rejections": dict(sorted(rejections.items())),
            "ready": not rejections,
        },
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
            label_rejections = Counter()
            selected, _features, labels, schema_counts = select_expectancy_schema(
                project,
                bot=bot,
                mode=mode,
                minimum_rows=required_expectancy_rows,
                rejection_counts=label_rejections,
            )
            observation_readiness = build_observation_readiness(
                project, bot=bot, mode=mode
            )
            expectancy[bot][mode] = {
                "candidate_events": len(events),
                "closed_labels": len(labels),
                "schema_version": selected,
                "closed_labels_by_schema": schema_counts,
                "label_rejections": dict(sorted(label_rejections.items())),
                "minimum_rows": required_expectancy_rows,
                "ready": bool(
                    len(labels) >= required_expectancy_rows
                    and observation_readiness["ready"] is True
                ),
                "observation_readiness": observation_readiness,
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
        label_rejections = Counter()
        selected_schema, feature_order, labels, schema_counts = select_expectancy_schema(
            project,
            bot=normalized_bot,
            mode=normalized_mode,
            minimum_rows=normalized_min_train + normalized_test_size,
            rejection_counts=label_rejections,
        )
    else:
        label_rejections = Counter()
        selected_schema = _positive_integer(schema_version, "schema_version")
        feature_order = schemas.get(selected_schema)
        if feature_order is None:
            raise ValueError("unsupported expectancy schema version")
        labels = build_expectancy_candidates(
            project,
            bot=normalized_bot,
            mode=normalized_mode,
            schema_version=selected_schema,
            rejection_counts=label_rejections,
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
    from trading.expectancy_runtime import (
        save_expectancy_model,
        save_expectancy_training_report,
    )

    model_identity = save_expectancy_model(model_path, result.final_model)
    folds = []
    for fold in result.folds:
        fold_payload = asdict(fold)
        for key, value in tuple(fold_payload.items()):
            if isinstance(value, datetime):
                fold_payload[key] = value.isoformat()
        folds.append(fold_payload)
    report = {
        "bot": normalized_bot,
        "mode": normalized_mode,
        "feature_order": list(feature_order),
        "schema_version": selected_schema,
        "closed_labels_by_schema": {
            str(key): int(value) for key, value in schema_counts.items()
        },
        "label_rejections": dict(sorted(label_rejections.items())),
        "labels": len(labels),
        "folds": folds,
        "predictions": len(result.predictions),
        "calibration": asdict(result.calibration),
        "candidate_model": str(model_path),
        "candidate_model_sha256": model_identity["sha256"],
        "candidate_model_bytes": model_identity["bytes"],
        "candidate_model_version": result.final_model.version,
        "runtime_model_changed": False,
    }
    report_path = model_path.with_suffix(".report.json")
    save_expectancy_training_report(report_path, report)
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
    decision_input = {
        "schema": _PROMOTION_DECISION_INPUT_SCHEMA,
        "evidence": asdict(normalized_evidence),
        "controls": {
            "minimum_samples": int(float(minimum_samples)),
            "manual_live_approval": manual_live_approval,
        },
    }
    try:
        canonical_input = json.dumps(
            decision_input,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("promotion evidence is not canonical JSON") from exc
    return {
        "research_passed": decision.research_passed,
        "live_allowed": decision.live_allowed,
        "reasons": list(decision.reasons),
        "deployment_performed": False,
        "decision_input_schema": _PROMOTION_DECISION_INPUT_SCHEMA,
        "decision_input_sha256": hashlib.sha256(canonical_input).hexdigest(),
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
    project = _real_project_root(root)
    try:
        db_path = _absolute_without_links(
            project / "data" / "trading_bot.db",
            label="experiment registry database",
        )
    except ValueError as exc:
        raise ValueError(
            "experiment registry database must be a real file without links"
        ) from exc
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
