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
from trading.execution_cost_model import ExecutionCostObservation
from trading.execution_policy import (
    ExecutionPolicyEvidence,
    evaluate_shadow_execution_policy,
)
from trading.expectancy_telemetry import (
    EXPECTANCY_FEATURES,
    EXPECTANCY_FEATURE_SCHEMAS,
)
from trading.expectancy_training import CandidateLabel, expanding_walk_forward_fit
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


def _utc_datetime(value) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def _candidate_events(root: Path, bot: str, mode: str) -> dict[str, dict]:
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
                        schema_version = int(row["schema_version"])
                    except (TypeError, ValueError, json.JSONDecodeError, OverflowError):
                        continue
                    entry_id = str(row["entry_id"] or "").strip()
                    if entry_id and candidate_time is not None and isinstance(features, dict):
                        selected[entry_id] = {
                            "candidate_time": candidate_time,
                            "features": features,
                            "schema_version": schema_version,
                            "source": "sqlite",
                        }
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
                    schema_version = int(event.get("schema_version") or 1)
                except (TypeError, ValueError, OverflowError):
                    continue
                if not entry_id or candidate_time is None or not isinstance(features, dict):
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
    schemas = EXPECTANCY_FEATURE_SCHEMAS.get(normalized_bot) or {}
    feature_order = schemas.get(int(schema_version))
    if feature_order is None or normalized_mode not in {"LIVE", "SIM"}:
        raise ValueError("unsupported bot or mode")
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
        if int(event.get("schema_version") or 1) != int(schema_version):
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
        if invested <= 0.0:
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
                net_return_bps=profit / invested * 10_000.0,
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
    required = max(1, int(minimum_rows))
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
) -> list[ExecutionCostObservation]:
    if (bot is None) != (mode is None):
        raise ValueError("bot and mode must be supplied together")
    normalized_bot = str(bot).strip().upper() if bot is not None else None
    normalized_mode = str(mode).strip().upper() if mode is not None else None
    if normalized_mode is not None and normalized_mode not in {"LIVE", "SIM"}:
        raise ValueError("mode must be LIVE or SIM")
    path = Path(root) / "data" / "trading_bot.db"
    try:
        conn = _read_connection(path)
    except FileNotFoundError:
        return []
    try:
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
        records = list(conn.execute(
            "SELECT t.intent_id, t.stage, t.payload_json, "
            f"{symbol_expr} AS symbol, {notional_expr} AS filled_notional "
            "FROM execution_tca t "
            + intent_join
            + candidate_join
            + "WHERE t.stage IN ('arrival','fill') "
            + scope_sql
            + "ORDER BY t.id DESC LIMIT ?",
            (*params, max(1, min(100_000, int(limit)))),
        ).fetchall())
        if (
            has_candidates
            and _table_exists(conn, "sim_execution_tca")
            and normalized_mode in {None, "SIM"}
        ):
            sim_scope_sql = ""
            sim_params: tuple = ()
            if normalized_bot is not None:
                sim_scope_sql = "AND UPPER(e.bot_name)=? AND UPPER(e.mode)=? "
                sim_params = (normalized_bot, normalized_mode)
            records.extend(
                conn.execute(
                    """SELECT t.entry_id AS intent_id, t.stage, t.payload_json,
                              e.symbol AS symbol, NULL AS filled_notional
                         FROM sim_execution_tca t
                         JOIN expectancy_candidates e ON e.entry_id=t.entry_id
                        WHERE t.stage IN ('arrival','fill') """
                    + sim_scope_sql
                    + "ORDER BY t.id DESC LIMIT ?",
                    (
                        *sim_params,
                        max(1, min(100_000, int(limit))),
                    ),
                ).fetchall()
            )
    finally:
        conn.close()
    paired: dict[str, dict[str, dict]] = defaultdict(dict)
    metadata = {}
    for row in records:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        paired[str(row["intent_id"])].setdefault(str(row["stage"]), payload)
        metadata[str(row["intent_id"])] = (row["symbol"], row["filled_notional"])
    observations = []
    for intent_id, stages in paired.items():
        arrival, fill = stages.get("arrival"), stages.get("fill")
        if not isinstance(arrival, dict) or not isinstance(fill, dict):
            continue
        symbol, notional = metadata[intent_id]
        observed_notional = _finite(notional)
        if observed_notional is None or observed_notional <= 0.0:
            observed_notional = _finite(arrival.get("notional_usdt"))
        try:
            observations.append(
                ExecutionCostObservation(
                    symbol=str(symbol),
                    total_cost_bps=fill["total_cost_bps"],
                    spread_bps=arrival["spread_bps"],
                    depth_coverage=arrival["depth_coverage"],
                    notional_usdt=observed_notional,
                    regime=str(arrival.get("regime") or "unknown"),
                    volatility_bps=arrival.get("volatility_bps") or 0.0,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return observations


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
    observations = load_execution_cost_observations(root, bot=bot, mode=mode)
    costs = [max(0.0, row.total_cost_bps) for row in observations]
    by_symbol = Counter(row.symbol for row in observations)
    return {
        "scope": {
            "bot": str(bot).strip().upper() if bot is not None else None,
            "mode": str(mode).strip().upper() if mode is not None else None,
        },
        "valid_samples": len(observations),
        "minimum_samples": max(5, int(minimum_samples)),
        "ready": len(observations) >= max(5, int(minimum_samples)),
        "median_cost_bps": statistics.median(costs) if costs else None,
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
    valid: list[dict] = []
    rejected = Counter()
    for row in _venue_rows(Path(root), "execution_shadow", limit=50_000):
        payload = row.get("payload") or {}
        if row.get("flags"):
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

    minimum = max(5, int(minimum_samples))
    evidence = ExecutionPolicyEvidence(
        gross_edge_bps=(
            statistics.median([row["gross_edge_bps"] for row in valid])
            if valid else None
        ),
        taker_cost_bps=(
            statistics.median([row["taker_cost_bps"] for row in valid])
            if valid else None
        ),
        maker_fee_bps=(
            statistics.median([row["maker_fee_bps"] for row in valid])
            if valid else None
        ),
        maker_adverse_selection_bps=(
            statistics.median(
                [row["maker_adverse_selection_bps"] for row in valid]
            ) if valid else None
        ),
        maker_fill_probability=(
            statistics.mean([float(row["crossed"]) for row in valid])
            if valid else None
        ),
        missed_fill_cost_bps=(
            statistics.median([row["missed_fill_cost_bps"] for row in valid])
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


def _venue_rows(root: Path, stream: str, *, limit: int) -> list[dict]:
    rows = []
    folder = root / "data" / "venue_native" / stream
    for path in sorted(folder.glob("*.sqlite3"), reverse=True):
        if len(rows) >= limit:
            break
        conn = None
        try:
            conn = _read_connection(path)
            fetched = conn.execute(
                "SELECT market_id, exchange_time, quality_flags_json, payload_json "
                "FROM venue_events ORDER BY exchange_time DESC LIMIT ?",
                (limit - len(rows),),
            ).fetchall()
        except (OSError, sqlite3.Error):
            continue
        finally:
            if conn is not None:
                conn.close()
        for row in fetched:
            try:
                rows.append(
                    {
                        "market_id": str(row["market_id"]),
                        "exchange_time": str(row["exchange_time"]),
                        "flags": tuple(json.loads(row["quality_flags_json"])),
                        "payload": json.loads(row["payload_json"]),
                    }
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
    return sorted(rows, key=lambda row: row["exchange_time"])


def build_ofi_report(
    root: str | Path, *, max_events: int = 5_000, max_windows: int = 500
) -> dict:
    project = Path(root)
    depth_by_market: dict[str, list[dict]] = defaultdict(list)
    for row in _venue_rows(project, "depth", limit=max_events):
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
    for row in _venue_rows(project, "trades", limit=max_events):
        for trade in (row["payload"].get("trades") or []):
            key = (
                row["market_id"], str(trade.get("id") or ""),
                trade.get("timestamp"), trade.get("price"), trade.get("amount"),
            )
            if key in seen_trades:
                continue
            seen_trades.add(key)
            timestamp = _finite(trade.get("timestamp"))
            if timestamp is None or timestamp <= 0.0:
                continue
            trades_by_market[row["market_id"]].append(
                {
                    "event_time": datetime.fromtimestamp(
                        timestamp / 1000.0, tz=timezone.utc
                    ),
                    "side": trade.get("side"),
                    "amount": trade.get("amount"),
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
    windows = windows[-max(1, int(max_windows)) :]
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
    overview = _venue_rows(Path(root), "overview", limit=1)
    if not overview:
        return {"candidates": 0, "accepted": 0, "rows": [], "simulation_only": True}
    markets = overview[-1]["payload"].get("markets") or {}
    engine = CarryEngine()
    previews = []
    for market_id, market in markets.items():
        funding = _finite((market or {}).get("funding_rate"))
        if funding is None or funding <= 0.0:
            continue
        if (market or {}).get("spot_available") is not True:
            previews.append(
                {
                    "market_id": str(market_id),
                    "symbol": (market or {}).get("symbol"),
                    "funding_rate": funding,
                    "state": "REJECTED",
                    "reason": "spot market availability not verified",
                    "projected_net_pnl": 0.0,
                }
            )
            continue
        market_periods = float(
            (expected_funding_periods_by_market or {}).get(
                str(market_id), expected_funding_periods
            )
        )
        terms = CarryTerms(
            notional_usdt=float(notional_usdt),
            expected_funding_rate=funding,
            taker_fee_rate=float(taker_fee_rate),
            maker_fee_rate=float(maker_fee_rate),
            expected_funding_periods=market_periods,
            entry_slippage_bps_per_leg=float(entry_slippage_bps_per_leg),
            exit_slippage_bps_per_leg=float(exit_slippage_bps_per_leg),
        )
        campaign = engine.start(f"preview-{market_id}", terms)
        previews.append(
            {
                "market_id": str(market_id),
                "symbol": (market or {}).get("symbol"),
                "funding_rate": funding,
                "expected_funding_periods": market_periods,
                "state": campaign.state.value,
                "reason": campaign.reason,
                "projected_net_pnl": campaign.projected_net_pnl,
            }
        )
    previews.sort(key=lambda row: row["projected_net_pnl"], reverse=True)
    previews = previews[: max(1, int(limit))]
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
    required_days = max(1, int(minimum_observed_days))
    required_regimes = max(1, int(minimum_regimes))
    candidate_times = sorted(
        event["candidate_time"]
        for event in _candidate_events(
            project, normalized_bot, normalized_mode
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
                minimum_rows=minimum_expectancy_rows,
            )
            expectancy[bot][mode] = {
                "candidate_events": len(events),
                "closed_labels": len(labels),
                "schema_version": selected,
                "closed_labels_by_schema": schema_counts,
                "minimum_rows": int(minimum_expectancy_rows),
                "ready": len(labels) >= int(minimum_expectancy_rows),
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
    if schema_version is None:
        selected_schema, feature_order, labels, schema_counts = select_expectancy_schema(
            project,
            bot=normalized_bot,
            mode=normalized_mode,
            minimum_rows=int(min_train) + int(test_size),
        )
    else:
        selected_schema = int(schema_version)
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
        min_train=int(min_train),
        test_size=int(test_size),
        purge=timedelta(days=max(0, int(purge_days))),
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
    decision = evaluate_promotion(
        PromotionEvidence(**dict(evidence)),
        minimum_samples=int(minimum_samples),
        explicit_manual_live_approval=bool(manual_live_approval),
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
    normalized_status = str(status).strip().upper()
    if normalized_status not in {"PLANNED", "RUNNING", "REJECTED", "COMPLETE"}:
        raise ValueError("unsupported experiment status")
    if not str(trial_id).strip() or not str(experiment_name).strip():
        raise ValueError("trial id and experiment name are required")
    try:
        encoded = json.dumps(params, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("experiment params must be finite JSON") from exc
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
                str(trial_id),
                str(experiment_name),
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
        "trial_id": str(trial_id),
        "experiment_name": str(experiment_name),
        "status": normalized_status,
        "trading_changed": False,
    }
