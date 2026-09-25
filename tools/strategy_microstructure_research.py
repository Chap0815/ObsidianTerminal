"""Read-only strategy research over a copied runtime/venue snapshot.

The tool deliberately treats unified CCXT L2 as sequence-unverified.  It may
derive snapshot features (spread, depth, imbalance and microprice), but never
queue position or sequence continuity.
"""
from __future__ import annotations

import argparse
import bisect
import json
import math
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

sys.dont_write_bytecode = True
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.constants import MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS  # noqa: E402
from trading.exit_evidence import classify_exit_reason  # noqa: E402
from trading.profit_research_runner import _complete_expectancy_campaign  # noqa: E402


UTC = timezone.utc
ALLOWED_L2_FLAGS = {"sequence_unverified"}
FEATURE_DIRECTIONS = {
    "spread_bps": -1.0,
    "depth5_quote_proxy": 1.0,
    "book_imbalance_aligned": 1.0,
    "microprice_aligned_bps": 1.0,
    "trade_pressure_aligned": 1.0,
    "entry_quality_score": 1.0,
}
ARRIVAL_CAPTURE_FAILURE_REASONS = {
    "api_budget_denied",
    "api_budget_check_failed",
    "arrival_tca_invalid",
    "bundle_persist_failed",
    "capture_validation_failed",
    "markout_schedule_failed",
    "orderbook_fetch_failed",
}
MINIMUM_DIRECTION_COVERAGE = 0.95
MINIMUM_DIRECT_REGIME_COVERAGE = 0.95
_UNSPECIFIED = object()


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _candidate_direction(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized if normalized in {"LONG", "SHORT"} else None


def _validated_complete_markout(
    row: sqlite3.Row,
    anchors: list[datetime | None],
) -> tuple[int, float] | str | None:
    """Return one causally anchored COMPLETE markout or a bounded rejection."""
    if str(row["status"]) != "COMPLETE":
        return None
    value = _finite(row["markout_bps"])
    raw_horizon = row["horizon_seconds"]
    if type(raw_horizon) is not int:
        return "invalid_markout_horizon"
    horizon = raw_horizon
    if value is None or horizon <= 0:
        return "invalid_complete_markout"
    if len(anchors) != 1 or anchors[0] is None:
        return "missing_or_ambiguous_markout_anchor"
    due_at = _time(row["due_at"])
    measured_at = _time(row["measured_at"])
    if due_at is None or measured_at is None:
        return "invalid_markout_time"
    anchor = anchors[0]
    if anchor is None:
        return "missing_or_ambiguous_markout_anchor"
    if due_at < anchor + timedelta(seconds=horizon) or measured_at < due_at:
        return "noncausal_complete_markout"
    return horizon, value


def _json(value: Any, default: Any) -> Any:
    try:
        result = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return result


def _ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _arrival_capture_status(
    evidence: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    rows = evidence or {}
    failure = rows.get("arrival_book_unavailable")
    arrival = rows.get("arrival_book")
    recovery = rows.get("arrival_book_recovered")
    failed_at = _time(failure.get("measured_at")) if failure else None
    captured_at = _time(arrival.get("measured_at")) if arrival else None
    recovered_at = _time(recovery.get("measured_at")) if recovery else None
    payload = failure.get("payload") if failure else None
    raw_reason = payload.get("reason") if isinstance(payload, dict) else None
    reason = (
        str(raw_reason)
        if raw_reason in ARRIVAL_CAPTURE_FAILURE_REASONS
        else ("unknown" if failure else None)
    )
    recovery_payload = recovery.get("payload") if recovery else None
    explicit_recovery = bool(
        recovery
        and recovery.get("source") == "sim_tca_capture"
        and recovery.get("sequence_status") == "capture_recovered"
        and isinstance(recovery_payload, dict)
        and recovery_payload.get("reason") == "arrival_book_available"
    )
    legacy_recovery = (
        failure is not None
        and failed_at is not None
        and captured_at is not None
        and captured_at > failed_at
    )
    if explicit_recovery or legacy_recovery:
        state = "recovered"
        effective_recovery = recovered_at or captured_at
    elif failure is not None:
        state = "failed"
        effective_recovery = None
    elif arrival is not None:
        state = "captured"
        effective_recovery = None
    else:
        state = "unobserved"
        effective_recovery = None
    return {
        "state": state,
        "reason": reason,
        "failed_at": failed_at.isoformat() if failed_at else None,
        "captured_at": captured_at.isoformat() if captured_at else None,
        "recovered_at": effective_recovery.isoformat()
        if effective_recovery else None,
    }


def _market_id(symbol: str) -> str:
    base = str(symbol).split(":", 1)[0].strip().upper()
    if "/" not in base and "_" not in base:
        base = f"{base}/USDT"
    return base.replace("/", "_").replace("-", "_")


def book_metrics(payload: dict[str, Any]) -> dict[str, float] | None:
    bids, asks = payload.get("bids"), payload.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
        return None

    def side(rows: list[Any], reverse: bool) -> list[tuple[float, float]] | None:
        parsed = []
        for row in rows[:20]:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                return None
            price, amount = _finite(row[0]), _finite(row[1])
            if price is None or amount is None or price <= 0.0 or amount <= 0.0:
                return None
            parsed.append((price, amount))
        if any(
            (parsed[index][0] <= parsed[index + 1][0] if reverse else
             parsed[index][0] >= parsed[index + 1][0])
            for index in range(len(parsed) - 1)
        ):
            return None
        return parsed

    bid_rows, ask_rows = side(bids, True), side(asks, False)
    if bid_rows is None or ask_rows is None:
        return None
    bid, bid_qty = bid_rows[0]
    ask, ask_qty = ask_rows[0]
    if bid >= ask:
        return None
    mid = (bid + ask) / 2.0
    top_sum = bid_qty + ask_qty
    microprice = (ask * bid_qty + bid * ask_qty) / top_sum
    bid5 = sum(price * amount for price, amount in bid_rows[:5])
    ask5 = sum(price * amount for price, amount in ask_rows[:5])
    depth_sum = bid5 + ask5
    if not all(math.isfinite(value) for value in (mid, microprice, depth_sum)):
        return None
    return {
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_bps": (ask - bid) / mid * 10_000.0,
        "book_imbalance": (bid5 - ask5) / depth_sum if depth_sum > 0.0 else 0.0,
        "microprice_bps": (microprice - mid) / mid * 10_000.0,
        # A cross-symbol dollar-depth claim would require contract multipliers.
        "depth5_quote_proxy": depth_sum,
    }


def _campaigns(
    conn: sqlite3.Connection,
    diagnostics: Counter | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute(
        "SELECT id AS _row_id,* FROM trades "
        "WHERE entry_id IS NOT NULL ORDER BY sell_time,id"
    ):
        grouped[str(row["entry_id"])].append(row)
    result = {}
    invalid_entry_anchors = {}
    for entry_id, rows in grouped.items():
        first = rows[0]
        raw_bot = first["bot_name"]
        is_sim = first["is_sim"]
        if (not isinstance(raw_bot, str) or type(is_sim) is not int
                or is_sim not in (0, 1)):
            if diagnostics is not None:
                diagnostics["invalid_trade_fragment"] += 1
            continue
        if raw_bot.endswith(" (SIM)"):
            if is_sim != 1:
                if diagnostics is not None:
                    diagnostics["trade_scope_conflict"] += 1
                continue
            bot = raw_bot[:-6]
        else:
            bot = raw_bot
        scope = {
            "bot": bot,
            "mode": "SIM" if is_sim else "LIVE",
            "symbol": first["symbol"],
            "position_type": str(first["position_type"] or "").upper(),
        }
        validated, error = _complete_expectancy_campaign(
            rows, normalized_bot=bot, expected_is_sim=is_sim,
        )
        if validated is None:
            if error == "missing_or_ambiguous_entry_anchor":
                invalid_entry_anchors[entry_id] = scope
            if diagnostics is not None:
                diagnostics[error or "invalid_trade_fragment"] += 1
            continue
        capital = validated["invested"]
        pnl = validated["profit"]
        net_return = pnl / capital * 10_000.0
        if not math.isfinite(net_return):
            if diagnostics is not None:
                diagnostics["invalid_trade_cashflow"] += 1
            continue
        fees = sum(
            value for value in (_finite(row["fees_usdt"]) for row in rows)
            if value is not None
        )
        funding = sum(
            value for value in (_finite(row["funding_paid"]) for row in rows)
            if value is not None
        )
        result[entry_id] = {
            "opened_at": validated["opened_at"].isoformat(),
            "closed_at": validated["closed_at"].isoformat(),
            "net_profit_usdt": pnl,
            "invested_usdt": capital,
            "net_return_bps": net_return,
            "fees_usdt": fees,
            "funding_paid_usdt": funding,
            "rows": len(rows),
            "reason": str(validated["terminal_reason"] or "UNKNOWN"),
            "exit_class": classify_exit_reason(validated["terminal_reason"]),
            **scope,
            "is_futures": first["is_futures"],
            "mfe_pct": max(
                (value for value in (_finite(row["mfe_pct"]) for row in rows)
                 if value is not None),
                default=None,
            ),
            "mae_pct": min(
                (value for value in (_finite(row["mae_pct"]) for row in rows)
                 if value is not None),
                default=None,
            ),
            "giveback_pct": max(
                (value for value in (_finite(row["giveback_pct"]) for row in rows)
                 if value is not None),
                default=None,
            ),
        }
    return result, invalid_entry_anchors


def _side(
    candidate_direction: str | None,
    features: dict[str, Any],
    campaign: dict[str, Any] | None,
    intent: sqlite3.Row | None,
) -> float | None:
    evidence: list[float] = []
    if candidate_direction == "LONG":
        evidence.append(1.0)
    elif candidate_direction == "SHORT":
        evidence.append(-1.0)
    if intent is not None:
        direction = str(intent["direction"] or "").strip().lower()
        if direction in {"buy", "long"}:
            evidence.append(1.0)
        elif direction in {"sell", "short"}:
            evidence.append(-1.0)
    signed = _finite(features.get("side_sign"))
    if signed is not None and signed != 0.0:
        evidence.append(1.0 if signed > 0.0 else -1.0)
    position = str((campaign or {}).get("position_type") or "").lower()
    if position in {"long", "buy"}:
        evidence.append(1.0)
    elif position in {"short", "sell"}:
        evidence.append(-1.0)
    if not evidence or len(set(evidence)) != 1:
        return None
    return evidence[0]


def _campaign_scope_matches(
    campaign: dict[str, Any], *, bot: str, mode: str, symbol: str,
    direction: str | None,
) -> bool:
    return (
        campaign["bot"] == bot
        and campaign["mode"] == mode
        and campaign["symbol"] == symbol
        and not (
            direction in {"LONG", "SHORT"}
            and campaign["position_type"] in {"LONG", "SHORT"}
            and campaign["position_type"] != direction
        )
    )


def _execution_symbol_matches(candidate_symbol: Any, evidence_symbol: Any) -> bool:
    """Mirror the persisted candidate/evidence symbol contract, not a transitive base join."""
    if not isinstance(candidate_symbol, str) or not isinstance(evidence_symbol, str):
        return False
    candidate = candidate_symbol.strip().casefold()
    evidence = evidence_symbol.strip().casefold()
    if not candidate or not evidence:
        return False
    return candidate == evidence or (
        candidate.partition("/")[0] == evidence.partition("/")[0]
        and ("/" not in candidate or "/" not in evidence)
    )


def _execution_side_matches(direction: str | None, side: Any) -> bool:
    normalized = str(side).strip().lower()
    if normalized not in {"buy", "sell", "long", "short"}:
        return False
    return direction is None or (
        (direction == "LONG" and normalized in {"buy", "long"})
        or (direction == "SHORT" and normalized in {"sell", "short"})
    )


def _order_direction(value: Any) -> str | None:
    normalized = str(value).strip().lower()
    if normalized in {"buy", "long"}:
        return "LONG"
    if normalized in {"sell", "short"}:
        return "SHORT"
    return None


class VenueReader:
    def __init__(self, venue_root: Path):
        self.root = venue_root
        self.connections: dict[tuple[str, str], sqlite3.Connection] = {}

    def close(self) -> None:
        for conn in self.connections.values():
            conn.close()

    def _conn(self, stream: str, day: str) -> sqlite3.Connection | None:
        key = (stream, day)
        if key in self.connections:
            return self.connections[key]
        path = self.root / stream / f"{day}.sqlite3"
        if not path.is_file():
            return None
        self.connections[key] = _ro(path)
        return self.connections[key]

    def rows(
        self,
        stream: str,
        market: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        rows = []
        day = (start - timedelta(days=1)).date()
        final_day = (end + timedelta(days=1)).date()
        while day <= final_day:
            conn = self._conn(stream, day.isoformat())
            if conn is not None:
                # julianday rounds sub-millisecond receipts; Python applies exact bounds.
                raw = conn.execute(
                    "SELECT event_id,market_id,exchange_time,received_time,"
                    "quality_flags_json,payload_json FROM venue_events "
                    "WHERE market_id=? AND julianday(received_time) "
                    "BETWEEN julianday(?) AND julianday(?) "
                    "ORDER BY received_time,event_id",
                    (
                        market,
                        (start.astimezone(UTC) - timedelta(milliseconds=2)).isoformat(),
                        (end.astimezone(UTC) + timedelta(milliseconds=2)).isoformat(),
                    ),
                )
                for row in raw:
                    received, exchange = _time(row["received_time"]), _time(row["exchange_time"])
                    if received is None or exchange is None or not start <= received <= end:
                        continue
                    rows.append(
                        {
                            "event_id": str(row["event_id"]),
                            "received": received,
                            "exchange": exchange,
                            "flags": _json(row["quality_flags_json"], ["invalid_json"]),
                            "payload": _json(row["payload_json"], {}),
                        }
                    )
            day += timedelta(days=1)
        rows.sort(key=lambda item: (item["received"], item["event_id"]))
        return rows


def _microstructure(
    venue: VenueReader,
    symbol: str,
    cutoff: datetime,
    side: float | None,
) -> dict[str, Any]:
    market = _market_id(symbol)
    l2 = venue.rows("l2_stream", market, cutoff - timedelta(seconds=90), cutoff + timedelta(seconds=75))
    parsed = []
    for row in l2:
        flags = set(row["flags"]) if isinstance(row["flags"], list) else {"invalid_flags"}
        payload = row["payload"]
        metrics = book_metrics(payload) if isinstance(payload, dict) else None
        if (
            metrics is None
            or flags - ALLOWED_L2_FLAGS
            or payload.get("sequence_valid") is not False
            or payload.get("sequence_status") != "unverified_unified_orderbook"
        ):
            continue
        parsed.append({**row, **metrics})

    pre = [row for row in parsed if row["received"] <= cutoff]
    result: dict[str, Any] = {
        "market_id": market,
        "sequence_valid": False,
        "sequence_status": "unverified_unified_orderbook",
        "book_source": None,
        "l2_pre_samples": len(pre),
    }
    chosen = pre[-1] if pre else None
    if chosen is None:
        depth = venue.rows(
            "depth", market, cutoff - timedelta(seconds=180), cutoff
        )
        valid_depth = []
        for row in depth:
            if row["flags"]:
                continue
            metrics = book_metrics(row["payload"]) if isinstance(row["payload"], dict) else None
            if metrics:
                valid_depth.append({**row, **metrics})
        chosen = valid_depth[-1] if valid_depth else None
        result["book_source"] = "rest_depth" if chosen else None
    else:
        result["book_source"] = "ccxt_unified_l2_snapshot"

    if chosen is not None:
        for name in (
            "mid", "spread_bps", "book_imbalance", "microprice_bps",
            "depth5_quote_proxy",
        ):
            result[name] = chosen[name]
        result["book_age_ms"] = (cutoff - chosen["received"]).total_seconds() * 1000.0
        if side is not None:
            result["book_imbalance_aligned"] = chosen["book_imbalance"] * side
            result["microprice_aligned_bps"] = chosen["microprice_bps"] * side

    if len(pre) >= 2:
        gaps = [
            (right["received"] - left["received"]).total_seconds()
            for left, right in zip(pre, pre[1:])
        ]
        result["l2_median_interval_ms"] = statistics.median(gaps) * 1000.0
        result["l2_max_gap_ms"] = max(gaps) * 1000.0
        result["clock_skew_median_ms"] = statistics.median(
            (row["received"] - row["exchange"]).total_seconds() * 1000.0
            for row in pre
        )

    if chosen is not None:
        target = min(
            (row for row in parsed if row["received"] >= cutoff + timedelta(seconds=45)),
            key=lambda row: abs((row["received"] - cutoff).total_seconds() - 60.0),
            default=None,
        )
        if target is not None and target["received"] <= cutoff + timedelta(seconds=75):
            move = (target["mid"] - chosen["mid"]) / chosen["mid"] * 10_000.0
            result["future_mid_move_60_bps"] = move
            result["future_spread_change_60_bps"] = (
                target["spread_bps"] - chosen["spread_bps"]
            )
            if side is not None:
                result["future_mid_move_60_bps_aligned"] = move * side

    trade_events = venue.rows(
        "trades", market, cutoff - timedelta(seconds=180), cutoff
    )
    seen: set[str] = set()
    buy = sell = 0.0
    count = 0
    offset = cutoff.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    end_us = ((offset.days * 86_400 + offset.seconds) * 1_000_000
              + offset.microseconds)
    start_us = end_us - 60_000_000
    # Millisecond trade clocks need a ceil start and an inclusive floor end.
    start_ms = -(-start_us // 1000)
    end_ms = end_us // 1000
    for event in trade_events:
        if event["flags"] or not isinstance(event["payload"], dict):
            continue
        for trade in event["payload"].get("trades") or []:
            if not isinstance(trade, dict):
                continue
            trade_id = str(trade.get("id") or "")
            timestamp = _finite(trade.get("timestamp"))
            price, amount = _finite(trade.get("price")), _finite(trade.get("amount"))
            if (
                not trade_id or trade_id in seen or timestamp is None
                or not start_ms <= timestamp <= end_ms
                or price is None or amount is None or price <= 0.0 or amount <= 0.0
            ):
                continue
            seen.add(trade_id)
            notional = price * amount
            if str(trade.get("side") or "").lower() == "buy":
                buy += notional
            elif str(trade.get("side") or "").lower() == "sell":
                sell += notional
            else:
                continue
            count += 1
    total = buy + sell
    if total > 0.0:
        pressure = (buy - sell) / total
        result["trade_pressure"] = pressure
        result["trade_pressure_aligned"] = pressure * side if side is not None else None
        result["trade_count_60s"] = count
        result["trade_notional_proxy_60s"] = total
    return result


def _quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return ordered[low]
    weight = position - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _venue_quality(
    venue_root: Path, relevant_markets: set[str]
) -> dict[str, Any]:
    result = {}
    for stream in ("overview", "depth", "trades", "l2_stream"):
        folder = venue_root / stream
        files = sorted(folder.glob("*.sqlite3")) if folder.is_dir() else []
        stream_result: dict[str, Any] = {"files": len(files), "bytes": 0, "markets": {}}
        state: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "first": None, "last": None, "gaps": [],
                     "skews": [], "flags": Counter(), "duplicates": 0, "seen": set()}
        )
        for path in files:
            stream_result["bytes"] += path.stat().st_size
            conn = _ro(path)
            try:
                rows = conn.execute(
                    "SELECT event_id,market_id,exchange_time,received_time,"
                    "quality_flags_json FROM venue_events ORDER BY received_time,event_id"
                )
                for row in rows:
                    market = str(row["market_id"])
                    if stream != "overview" and market not in relevant_markets:
                        continue
                    received, exchange = _time(row["received_time"]), _time(row["exchange_time"])
                    if received is None or exchange is None:
                        continue
                    item = state[market]
                    if row["event_id"] in item["seen"]:
                        item["duplicates"] += 1
                    item["seen"].add(row["event_id"])
                    if item["last"] is not None:
                        item["gaps"].append((received - item["last"]).total_seconds())
                    item["count"] += 1
                    item["first"] = item["first"] or received
                    item["last"] = received
                    item["skews"].append((received - exchange).total_seconds() * 1000.0)
                    flags = _json(row["quality_flags_json"], ["invalid_json"])
                    item["flags"].update(flags if isinstance(flags, list) else ["invalid_flags"])
            finally:
                conn.close()
        for market, item in state.items():
            stream_result["markets"][market] = {
                "events": item["count"],
                "first": item["first"].isoformat() if item["first"] else None,
                "last": item["last"].isoformat() if item["last"] else None,
                "duplicate_event_ids": item["duplicates"],
                "gap_p50_seconds": _quantile(item["gaps"], 0.50),
                "gap_p95_seconds": _quantile(item["gaps"], 0.95),
                "gap_max_seconds": max(item["gaps"], default=None),
                "clock_skew_p50_ms": _quantile(item["skews"], 0.50),
                "clock_skew_p95_abs_ms": _quantile(
                    [abs(value) for value in item["skews"]], 0.95
                ),
                "flags": dict(item["flags"]),
            }
        result[stream] = stream_result
    return result


def extract_snapshot(snapshot_root: Path) -> dict[str, Any]:
    database = snapshot_root / "trading_bot.db"
    conn = _ro(database)
    try:
        conn.execute("BEGIN")
        return _extract_snapshot_with_connection(snapshot_root, conn)
    finally:
        conn.close()


def _extract_snapshot_with_connection(
    snapshot_root: Path, conn: sqlite3.Connection
) -> dict[str, Any]:
    database = snapshot_root / "trading_bot.db"
    venue_root = snapshot_root / "venue_native"
    integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    campaign_rejections = Counter()
    campaigns, invalid_campaign_anchors = _campaigns(
        conn, diagnostics=campaign_rejections
    )
    context_available = False
    if conn.execute(
        """SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='expectancy_candidate_context'"""
    ).fetchone():
        context_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(expectancy_candidate_context)")
        }
        context_available = {"entry_id", "direction"} <= context_columns
    context_join = (
        "LEFT JOIN expectancy_candidate_context context "
        "ON context.entry_id=candidate.entry_id"
        if context_available else ""
    )
    direction_expr = "context.direction" if context_available else "NULL"
    candidate_scopes = {
        str(row["entry_id"]): {
            "bot": str(row["bot_name"]).strip().upper(),
            "mode": str(row["mode"]).strip().upper(),
            "symbol": row["symbol"],
            "direction": _candidate_direction(row["candidate_direction"]),
        }
        for row in conn.execute(
            "SELECT candidate.entry_id,candidate.bot_name,candidate.mode,"
            f"candidate.symbol,{direction_expr} AS candidate_direction "
            "FROM expectancy_candidates candidate "
            f"{context_join}"
        )
    }
    execution_evidence_rejections = Counter()

    def scope_for(identity: Any, *, mode: Any = _UNSPECIFIED,
                  bot: Any = _UNSPECIFIED,
                  symbol: Any = _UNSPECIFIED,
                  side: Any = _UNSPECIFIED,
                  payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
        scope = candidate_scopes.get(str(identity))
        if scope is None:
            execution_evidence_rejections["missing_candidate"] += 1
            return None
        if scope["mode"] not in {"LIVE", "SIM"} or (
            mode is not _UNSPECIFIED and str(mode).strip().upper() != scope["mode"]
        ):
            execution_evidence_rejections["mode_mismatch"] += 1
            return None
        if bot is not _UNSPECIFIED and str(bot).strip().upper() != scope["bot"]:
            execution_evidence_rejections["bot_mismatch"] += 1
            return None
        if symbol is not _UNSPECIFIED and not _execution_symbol_matches(
            scope["symbol"], symbol
        ):
            execution_evidence_rejections["symbol_mismatch"] += 1
            return None
        if side is not _UNSPECIFIED and not _execution_side_matches(
            scope["direction"], side
        ):
            execution_evidence_rejections["side_mismatch"] += 1
            return None
        if payload is not None:
            if "bot_name" in payload and str(payload["bot_name"]).strip().upper() != scope["bot"]:
                execution_evidence_rejections["payload_bot_mismatch"] += 1
                return None
            if "mode" in payload and str(payload["mode"]).strip().upper() != scope["mode"]:
                execution_evidence_rejections["payload_mode_mismatch"] += 1
                return None
            if "research_simulated" in payload and payload["research_simulated"] is not (
                scope["mode"] == "SIM"
            ):
                execution_evidence_rejections["payload_mode_mismatch"] += 1
                return None
            if "symbol" in payload and not _execution_symbol_matches(
                scope["symbol"], payload["symbol"]
            ):
                execution_evidence_rejections["payload_symbol_mismatch"] += 1
                return None
            if "side" in payload and not _execution_side_matches(
                scope["direction"], payload["side"]
            ):
                execution_evidence_rejections["payload_side_mismatch"] += 1
                return None
        return scope

    def matching_intent(intent: sqlite3.Row | None) -> sqlite3.Row | None:
        if intent is None:
            return None
        scope = scope_for(
            intent["intent_id"], mode=intent["mode"], bot=intent["bot_name"],
            symbol=intent["symbol"], side=intent["direction"],
        )
        return intent if scope is not None else None

    intents = {
        str(row["intent_id"]): row
        for row in conn.execute("SELECT * FROM order_intents")
    }
    tca: dict[tuple[str, str], dict[str, Any]] = defaultdict(dict)
    fill_anchors: dict[tuple[str, str], list[datetime | None]] = defaultdict(list)
    candidate_anchors: dict[
        tuple[str, str], list[datetime | None]
    ] = defaultdict(list)
    execution_metadata: dict[
        tuple[str, str], list[tuple[Any, Any]]
    ] = defaultdict(list)
    for intent in intents.values():
        if matching_intent(intent) is not None:
            execution_metadata[
                (str(intent["mode"]).strip().upper(), str(intent["intent_id"]))
            ].append((intent["symbol"], intent["direction"]))
    for row in conn.execute(
        "SELECT t.intent_id,o.mode,t.stage,t.measured_at,t.payload_json "
        "FROM execution_tca t JOIN order_intents o ON o.intent_id=t.intent_id "
        "ORDER BY t.id"
    ):
        payload = _json(row["payload_json"], None)
        intent = intents.get(str(row["intent_id"]))
        if matching_intent(intent) is None:
            continue
        if isinstance(payload, dict):
            if "symbol" in payload and not _execution_symbol_matches(
                intent["symbol"], payload["symbol"]
            ):
                execution_evidence_rejections["payload_intent_symbol_mismatch"] += 1
                continue
            if "side" in payload and not _execution_side_matches(
                _order_direction(intent["direction"]), payload["side"]
            ):
                execution_evidence_rejections["payload_intent_side_mismatch"] += 1
                continue
        if isinstance(payload, dict) and scope_for(
            row["intent_id"], payload=payload
        ) is None:
            continue
        scope = str(row["mode"]).strip().upper()
        key = (scope, str(row["intent_id"]))
        stage = str(row["stage"])
        if stage in {"arrival", "fill"}:
            candidate_anchors[key].append(_time(row["measured_at"]))
        if stage == "fill":
            fill_anchors[key].append(_time(row["measured_at"]))
        if isinstance(payload, dict):
            tca[key][stage] = payload
            if stage in {"arrival", "fill"}:
                execution_metadata[key].append(
                    (payload.get("symbol", _UNSPECIFIED),
                     payload.get("side", _UNSPECIFIED))
                )
    if conn.execute(
        """SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='sim_execution_tca'"""
    ).fetchone():
        for row in conn.execute(
            "SELECT entry_id,stage,measured_at,payload_json "
            "FROM sim_execution_tca ORDER BY id"
        ):
            payload = _json(row["payload_json"], None)
            if scope_for(
                row["entry_id"], mode="SIM",
                payload=payload if isinstance(payload, dict) else None,
            ) is None:
                continue
            key = ("SIM", str(row["entry_id"]))
            stage = str(row["stage"])
            if stage in {"arrival", "fill"}:
                candidate_anchors[key].append(_time(row["measured_at"]))
            if stage == "fill":
                fill_anchors[key].append(_time(row["measured_at"]))
            if isinstance(payload, dict):
                tca[key][stage] = payload
                if stage in {"arrival", "fill"}:
                    execution_metadata[key].append(
                        (payload.get("symbol", _UNSPECIFIED),
                         payload.get("side", _UNSPECIFIED))
                    )
    unavailable_anchors: dict[str, list[datetime | None]] = defaultdict(list)
    arrival_capture_evidence: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    if conn.execute(
        """SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='candidate_microstructure'"""
    ).fetchone():
        for row in conn.execute(
            """SELECT entry_id,stage,bot_name,mode,symbol,measured_at,
                      source,sequence_status,payload_json
                 FROM candidate_microstructure
                 WHERE stage IN ('arrival_book', 'arrival_book_unavailable',
                                 'arrival_book_recovered')"""
        ):
            payload = _json(row["payload_json"], {})
            if scope_for(
                row["entry_id"], mode=row["mode"], bot=row["bot_name"],
                symbol=row["symbol"],
                payload=payload if isinstance(payload, dict) else None,
            ) is None:
                continue
            if (
                isinstance(payload, dict)
                and "symbol" in payload
                and not _execution_symbol_matches(row["symbol"], payload["symbol"])
            ):
                execution_evidence_rejections["capture_payload_symbol_mismatch"] += 1
                continue
            arrival_capture_evidence[str(row["entry_id"])][str(row["stage"])] = {
                "measured_at": row["measured_at"],
                "source": str(row["source"]),
                "sequence_status": str(row["sequence_status"]),
                "payload": payload if isinstance(payload, dict) else {},
            }
            mode = str(row["mode"]).strip().upper()
            if mode == "SIM" and str(row["stage"]) == "arrival_book_unavailable":
                unavailable_anchors[str(row["entry_id"])].append(
                    _time(row["measured_at"])
                )
            key = (mode, str(row["entry_id"]))
            execution_metadata[key].append(
                (row["symbol"], payload.get("side", _UNSPECIFIED)
                 if isinstance(payload, dict) else _UNSPECIFIED)
            )
            if isinstance(payload, dict) and "symbol" in payload:
                execution_metadata[key].append(
                    (payload["symbol"], payload.get("side", _UNSPECIFIED))
                )
    inconsistent_execution_groups: set[tuple[str, str]] = set()
    for key, constraints in execution_metadata.items():
        candidate_scope = candidate_scopes.get(key[1])
        if candidate_scope is None:
            continue
        symbols = [candidate_scope["symbol"]]
        symbols.extend(
            symbol for symbol, _ in constraints if symbol is not _UNSPECIFIED
        )
        normalized_symbols = [str(symbol).strip().casefold() for symbol in symbols]
        qualified = {symbol for symbol in normalized_symbols if "/" in symbol}
        bases = {symbol.partition("/")[0] for symbol in normalized_symbols}
        directions = {
            direction
            for direction in (
                _order_direction(side)
                for side in [candidate_scope["direction"], *(
                    side for _, side in constraints if side is not _UNSPECIFIED
                )]
            )
            if direction is not None
        }
        if len(bases) > 1 or len(qualified) > 1 or len(directions) > 1:
            inconsistent_execution_groups.add(key)
            execution_evidence_rejections["inconsistent_execution_group"] += 1
            tca.pop(key, None)
            fill_anchors.pop(key, None)
            candidate_anchors.pop(key, None)
            if key[0] == "SIM":
                unavailable_anchors.pop(key[1], None)
            arrival_capture_evidence.pop(key[1], None)
    markouts: dict[tuple[str, str], dict[int, float]] = defaultdict(dict)
    rejected_markouts = Counter()
    seen_complete_markouts: set[tuple[str, str, int]] = set()

    def accept_markout(row: sqlite3.Row, scope: str, identity: str) -> None:
        key = (scope, identity)
        if str(row["status"]) != "COMPLETE":
            return
        if key in inconsistent_execution_groups:
            return
        for evidence_symbol, evidence_side in execution_metadata.get(key, ()):
            if evidence_symbol is not _UNSPECIFIED and not _execution_symbol_matches(
                row["symbol"], evidence_symbol
            ):
                execution_evidence_rejections["markout_execution_symbol_mismatch"] += 1
                return
            if evidence_side is not _UNSPECIFIED and not _execution_side_matches(
                _order_direction(evidence_side), row["side"]
            ):
                execution_evidence_rejections["markout_execution_side_mismatch"] += 1
                return
        raw_horizon = row["horizon_seconds"]
        if type(raw_horizon) is not int:
            rejected_markouts["invalid_markout_horizon"] += 1
            return
        evidence_key = (scope, identity, raw_horizon)
        if evidence_key in seen_complete_markouts:
            markouts[key].pop(raw_horizon, None)
            rejected_markouts["duplicate_complete_markout"] += 1
            return
        seen_complete_markouts.add(evidence_key)
        anchors = list(fill_anchors.get(key, ()))
        if scope == "SIM":
            anchors.extend(unavailable_anchors.get(identity, ()))
        result = _validated_complete_markout(row, anchors)
        if result is None:
            return
        if isinstance(result, str):
            rejected_markouts[result] += 1
            return
        horizon, value = result
        markouts[key][horizon] = value

    for row in conn.execute(
        "SELECT m.intent_id,o.mode,m.horizon_seconds,m.markout_bps,m.status,"
        "m.due_at,m.measured_at,m.symbol,m.side "
        "FROM execution_markouts m "
        "JOIN order_intents o ON o.intent_id=m.intent_id"
    ):
        intent = intents.get(str(row["intent_id"]))
        if matching_intent(intent) is None:
            continue
        if not _execution_symbol_matches(intent["symbol"], row["symbol"]):
            execution_evidence_rejections["markout_intent_symbol_mismatch"] += 1
            continue
        if not _execution_side_matches(_order_direction(intent["direction"]), row["side"]):
            execution_evidence_rejections["markout_intent_side_mismatch"] += 1
            continue
        if scope_for(
            row["intent_id"], symbol=row["symbol"], side=row["side"]
        ) is None:
            continue
        scope = str(row["mode"]).strip().upper()
        accept_markout(row, scope, str(row["intent_id"]))
    if conn.execute(
        """SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='sim_execution_markouts'"""
    ).fetchone():
        for row in conn.execute(
            "SELECT entry_id,horizon_seconds,markout_bps,status,due_at,"
            "measured_at,symbol,side "
            "FROM sim_execution_markouts"
        ):
            if scope_for(
                row["entry_id"], mode="SIM", symbol=row["symbol"],
                side=row["side"],
            ) is None:
                continue
            accept_markout(row, "SIM", str(row["entry_id"]))
    regime_by_time: dict[datetime, str | None] = {}
    invalid_regime_rows = 0
    for row in conn.execute(
        "SELECT timestamp,regime FROM market_regime "
        "WHERE regime <> 'CACHED_FG' ORDER BY timestamp,id"
    ):
        stamp = _time(row["timestamp"])
        regime = str(row["regime"] or "").strip().upper()
        if stamp is None or regime not in {"BULL", "BEAR", "NEUTRAL"}:
            invalid_regime_rows += 1
            if stamp is not None:
                regime_by_time[stamp] = None
            continue
        if stamp in regime_by_time and regime_by_time[stamp] != regime:
            regime_by_time[stamp] = None
        elif stamp not in regime_by_time:
            regime_by_time[stamp] = regime
    regime_times = sorted(regime_by_time)
    regime_context_available = False
    if conn.execute(
        """SELECT 1 FROM sqlite_master
            WHERE type='table'
              AND name='expectancy_candidate_regime_context'"""
    ).fetchone():
        regime_context_columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(expectancy_candidate_regime_context)"
            )
        }
        regime_context_available = {
            "entry_id", "evidence_state", "reason", "regime",
            "observed_at", "btc_24h", "btc_7d", "fear_greed", "source",
        } <= regime_context_columns
    regime_context_join = (
        "LEFT JOIN expectancy_candidate_regime_context regime_context "
        "ON regime_context.entry_id=candidate.entry_id"
        if regime_context_available else ""
    )
    regime_exprs = (
        "regime_context.evidence_state AS regime_evidence_state,"
        "regime_context.reason AS regime_reason,"
        "regime_context.regime AS candidate_regime,"
        "regime_context.observed_at AS regime_observed_at,"
        "regime_context.btc_24h AS regime_btc_24h,"
        "regime_context.btc_7d AS regime_btc_7d,"
        "regime_context.fear_greed AS regime_fear_greed,"
        "regime_context.source AS regime_source "
        if regime_context_available else
        "NULL AS regime_evidence_state,NULL AS regime_reason,"
        "NULL AS candidate_regime,NULL AS regime_observed_at,"
        "NULL AS regime_btc_24h,NULL AS regime_btc_7d,"
        "NULL AS regime_fear_greed,NULL AS regime_source "
    )
    raw_candidates = list(conn.execute(
        "SELECT candidate.entry_id,candidate.bot_name,candidate.symbol,"
        "candidate.mode,candidate.candidate_time,candidate.schema_version,"
        f"candidate.features_json,{direction_expr} AS candidate_direction,"
        f"{regime_exprs}"
        "FROM expectancy_candidates candidate "
        f"{context_join} "
        f"{regime_context_join} "
        "ORDER BY candidate.candidate_time,candidate.entry_id"
    ))
    conn.close()

    venue = VenueReader(venue_root)
    candidates = []
    rejected_candidates = Counter()
    regime_rejections = Counter()
    try:
        for row in raw_candidates:
            stamp = _time(row["candidate_time"])
            features = _json(row["features_json"], {})
            if stamp is None or not isinstance(features, dict):
                continue
            entry_id = str(row["entry_id"])
            mode = str(row["mode"]).strip().upper()
            anchors = list(candidate_anchors.get((mode, entry_id), ()))
            if mode == "SIM":
                anchors.extend(unavailable_anchors.get(entry_id, ()))
            valid_anchors = [anchor for anchor in anchors if anchor is not None]
            if valid_anchors and stamp > min(valid_anchors):
                rejected_candidates["noncausal_candidate_anchor"] += 1
                continue
            candidate_direction = _candidate_direction(
                row["candidate_direction"]
            )
            scope_args = {
                "bot": str(row["bot_name"]).strip().upper(),
                "mode": mode,
                "symbol": row["symbol"],
                "direction": candidate_direction,
            }
            invalid_anchor = invalid_campaign_anchors.get(entry_id)
            if invalid_anchor is not None:
                if _campaign_scope_matches(invalid_anchor, **scope_args):
                    rejected_candidates["missing_or_ambiguous_entry_anchor"] += 1
                    continue
                campaign_rejections["candidate_scope_mismatch"] += 1
            campaign = campaigns.get(entry_id)
            if campaign is not None and not _campaign_scope_matches(
                campaign, **scope_args
            ):
                campaign_rejections["candidate_scope_mismatch"] += 1
                campaign = None
            if campaign is not None:
                opened_at = _time(campaign.get("opened_at"))
                if opened_at is None:
                    rejected_candidates["missing_or_ambiguous_entry_anchor"] += 1
                    continue
                if stamp > opened_at:
                    rejected_candidates["noncausal_candidate_entry"] += 1
                    continue
            intent = (
                matching_intent(intents.get(entry_id))
                if (mode, entry_id) not in inconsistent_execution_groups
                else None
            )
            side = _side(candidate_direction, features, campaign, intent)
            regime: str | None = None
            regime_provenance = "missing"
            raw_direct_regime = row["candidate_regime"]
            direct_present = any(
                row[name] is not None
                for name in (
                    "regime_evidence_state", "regime_reason",
                    "candidate_regime",
                    "regime_observed_at", "regime_btc_24h",
                    "regime_btc_7d", "regime_fear_greed", "regime_source",
                )
            )
            if direct_present:
                evidence_state = str(
                    row["regime_evidence_state"] or ""
                ).strip().upper()
                evidence_reason = str(row["regime_reason"] or "").strip()
                direct_regime = str(raw_direct_regime or "").strip().upper()
                observed_at = _time(row["regime_observed_at"])
                btc_24h = _finite(row["regime_btc_24h"])
                btc_7d = _finite(row["regime_btc_7d"])
                fear_greed = _finite(row["regime_fear_greed"])
                age = (
                    (stamp - observed_at).total_seconds()
                    if observed_at is not None else None
                )
                if (
                    evidence_state == "CAPTURED"
                    and evidence_reason == ""
                    and direct_regime in {"BULL", "BEAR", "NEUTRAL"}
                    and btc_24h is not None
                    and btc_7d is not None
                    and fear_greed is not None
                    and fear_greed.is_integer()
                    and 0.0 <= fear_greed <= 100.0
                    and row["regime_source"] == "market_regime_snapshot"
                    and age is not None
                    and 0.0 <= age <= MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS
                ):
                    regime = direct_regime
                    regime_provenance = "direct_candidate_context"
                elif (
                    evidence_state in {"MISSING", "STALE", "INVALID"}
                    and evidence_reason
                    and raw_direct_regime is None
                    and observed_at is None
                    and row["regime_btc_24h"] is None
                    and row["regime_btc_7d"] is None
                    and row["regime_fear_greed"] is None
                    and row["regime_source"] == "market_regime_snapshot"
                ):
                    regime_provenance = "direct_candidate_context_unavailable"
                    regime_rejections[
                        "direct_candidate_regime_"
                        f"{evidence_state.lower()}:{evidence_reason}"
                    ] += 1
                else:
                    regime_rejections["invalid_direct_candidate_regime"] += 1
                    regime_provenance = "invalid_direct_candidate_context"
            else:
                regime_index = bisect.bisect_right(regime_times, stamp) - 1
                if regime_index < 0:
                    regime_rejections["missing_prior_regime"] += 1
                else:
                    regime_time = regime_times[regime_index]
                    regime = regime_by_time[regime_time]
                    if regime is None:
                        regime_rejections[
                            "invalid_or_ambiguous_regime_anchor"
                        ] += 1
                    elif (
                        stamp - regime_time
                    ).total_seconds() > MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS:
                        regime = None
                        regime_rejections["stale_regime_candidate"] += 1
                    else:
                        regime_provenance = "legacy_inferred"
            micro = _microstructure(venue, str(row["symbol"]), stamp, side)
            candidates.append(
                {
                    "entry_id": entry_id,
                    "bot": str(row["bot_name"]).strip().upper(),
                    "mode": mode,
                    "symbol": str(row["symbol"]),
                    "candidate_time": stamp.isoformat(),
                    "schema_version": int(row["schema_version"]),
                    "direction": candidate_direction,
                    "side": side,
                    "regime": regime,
                    "regime_provenance": regime_provenance,
                    "entry_quality_score": _finite(features.get("score")),
                    "campaign": campaign,
                    "tca": tca.get((mode, entry_id)),
                    "markouts": markouts.get((mode, entry_id)),
                    "arrival_capture": _arrival_capture_status(
                        arrival_capture_evidence.get(entry_id)
                    ),
                    "microstructure": micro,
                }
            )
    finally:
        venue.close()
    arrival_capture_counts = Counter(
        item["arrival_capture"]["state"] for item in candidates
    )
    relevant_markets = {_market_id(row["symbol"]) for row in candidates}
    return {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": {
            "snapshot_root": str(snapshot_root),
            "database_integrity": integrity,
            "database_bytes": database.stat().st_size,
            "sequence_contract": "unverified_unified_orderbook",
            "queue_position_claimed": False,
            "regime_evidence_max_age_seconds": (
                MARKET_REGIME_EVIDENCE_MAX_AGE_SECONDS
            ),
        },
        "counts": {
            "candidates": len(candidates),
            "campaigns": sum(item["campaign"] is not None for item in candidates),
            "tca_candidates": sum(bool(item["tca"]) for item in candidates),
            "markout_candidates": sum(bool(item["markouts"]) for item in candidates),
            "rejected_candidates": sum(rejected_candidates.values()),
            "rejected_markouts": sum(rejected_markouts.values()),
            "arrival_capture_failed": arrival_capture_counts["failed"],
            "arrival_capture_recovered": arrival_capture_counts["recovered"],
            "arrival_capture_captured": arrival_capture_counts["captured"],
            "arrival_capture_unobserved": arrival_capture_counts["unobserved"],
            "regime_candidates": sum(
                item["regime"] is not None for item in candidates
            ),
            "direct_regime_candidates": sum(
                item["regime_provenance"] == "direct_candidate_context"
                for item in candidates
            ),
            "inferred_regime_candidates": sum(
                item["regime_provenance"] == "legacy_inferred"
                for item in candidates
            ),
            "invalid_regime_rows": invalid_regime_rows,
        },
        "venue_quality": _venue_quality(venue_root, relevant_markets),
        "candidate_rejections": dict(sorted(rejected_candidates.items())),
        "campaign_rejections": dict(sorted(campaign_rejections.items())),
        "execution_evidence_rejections": dict(
            sorted(execution_evidence_rejections.items())
        ),
        "regime_rejections": dict(sorted(regime_rejections.items())),
        "markout_rejections": dict(sorted(rejected_markouts.items())),
        "candidates": candidates,
    }


def _mean(values: Iterable[float]) -> float | None:
    rows = list(values)
    return statistics.mean(rows) if rows else None


def _campaign_exit_class(campaign: dict[str, Any]) -> str:
    explicit = str(campaign.get("exit_class") or "").strip()
    return explicit or classify_exit_reason(campaign.get("reason"))


def _scope_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    observed_completed = [row for row in rows if row["campaign"]]
    completed = [
        row for row in observed_completed
        if _campaign_exit_class(row["campaign"]) == "strategy_exit"
    ]
    returns = [row["campaign"]["net_return_bps"] for row in completed]
    dates = {
        day for row in completed
        if (day := _utc_research_day(row.get("candidate_time"))) is not None
    }
    symbol_rows: dict[str, list[float]] = defaultdict(list)
    for row in completed:
        symbol_rows[row["symbol"]].append(row["campaign"]["net_return_bps"])
    costs = [
        _finite(row["tca"].get("fill", {}).get("total_cost_bps"))
        for row in rows if isinstance(row.get("tca"), dict)
    ]
    costs = [value for value in costs if value is not None]
    spreads = [
        _finite(row["tca"].get("arrival", {}).get("spread_bps"))
        for row in rows if isinstance(row.get("tca"), dict)
    ]
    spreads = [value for value in spreads if value is not None]
    depth_coverage = [
        _finite(row["tca"].get("arrival", {}).get("depth_coverage"))
        for row in rows if isinstance(row.get("tca"), dict)
    ]
    depth_coverage = [value for value in depth_coverage if value is not None]
    markouts: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        for horizon, value in (row.get("markouts") or {}).items():
            parsed = _finite(value)
            if parsed is not None:
                markouts[int(horizon)].append(parsed)
    directions = Counter(
        direction
        for row in rows
        if (direction := _candidate_direction(row.get("direction"))) is not None
        and (
            (direction == "LONG" and row.get("side") == 1.0)
            or (direction == "SHORT" and row.get("side") == -1.0)
        )
    )
    direction_coverage = sum(directions.values()) / len(rows) if rows else 0.0
    direct_regime_coverage = (
        sum(
            row.get("regime_provenance") == "direct_candidate_context"
            for row in rows
        ) / len(rows)
        if rows else 0.0
    )
    return {
        "candidates": len(rows),
        "completed_campaigns": len(completed),
        "excluded_non_strategy_exits": len(observed_completed) - len(completed),
        "calendar_days": len(dates),
        "observed_candidate_days": len(
            {
                day for row in rows
                if (day := _utc_research_day(row.get("candidate_time"))) is not None
            }
        ),
        "directions": dict(sorted(directions.items())),
        "direction_coverage": direction_coverage,
        "minimum_direction_coverage": MINIMUM_DIRECTION_COVERAGE,
        "direction_ready": (
            bool(rows) and direction_coverage >= MINIMUM_DIRECTION_COVERAGE
        ),
        "direct_regime_coverage": direct_regime_coverage,
        "minimum_direct_regime_coverage": MINIMUM_DIRECT_REGIME_COVERAGE,
        "direct_regime_ready": (
            bool(rows)
            and direct_regime_coverage >= MINIMUM_DIRECT_REGIME_COVERAGE
        ),
        "book_coverage": sum(
            row["microstructure"].get("book_source") is not None for row in rows
        ) / len(rows) if rows else 0.0,
        "l2_coverage": sum(
            row["microstructure"].get("book_source") == "ccxt_unified_l2_snapshot"
            for row in rows
        ) / len(rows) if rows else 0.0,
        "trade_pressure_coverage": sum(
            row["microstructure"].get("trade_pressure") is not None for row in rows
        ) / len(rows) if rows else 0.0,
        "tca_coverage": sum(bool(row["tca"]) for row in rows) / len(rows) if rows else 0.0,
        "markout_coverage": sum(bool(row["markouts"]) for row in rows) / len(rows)
        if rows else 0.0,
        "arrival_capture": dict(sorted(Counter(
            str((row.get("arrival_capture") or {}).get("state") or "unobserved")
            for row in rows
        ).items())),
        "mean_net_return_bps": _mean(returns),
        "median_net_return_bps": statistics.median(returns) if returns else None,
        "positive_rate": _mean(float(value > 0.0) for value in returns),
        "measured_fees_usdt": sum(
            row["campaign"].get("fees_usdt", 0.0) for row in completed
        ),
        "measured_funding_paid_usdt": sum(
            row["campaign"].get("funding_paid_usdt", 0.0) for row in completed
        ),
        "cost_stress_mean_bps": {
            str(stress): _mean(value - stress for value in returns)
            for stress in (5, 10, 20)
        },
        "regimes": dict(Counter(row["regime"] or "UNKNOWN" for row in rows)),
        "verified_regime_count": len(
            {
                str(row["regime"]).strip().upper()
                for row in rows
                if row.get("regime_provenance") == "direct_candidate_context"
                and str(row.get("regime") or "").strip().upper()
                not in {"", "UNKNOWN", "CACHED_FG"}
            }
        ),
        "symbols": dict(Counter(row["symbol"] for row in rows)),
        "symbol_performance": {
            symbol: {
                "campaigns": len(values),
                "mean_net_return_bps": statistics.mean(values),
                "median_net_return_bps": statistics.median(values),
                "positive_rate": statistics.mean(value > 0.0 for value in values),
            }
            for symbol, values in sorted(symbol_rows.items())
        },
        "execution": {
            "fill_cost_samples": len(costs),
            "median_total_cost_bps": statistics.median(costs) if costs else None,
            "p95_total_cost_bps": _quantile(costs, 0.95),
            "median_arrival_spread_bps": (
                statistics.median(spreads) if spreads else None
            ),
            "median_depth_coverage": (
                statistics.median(depth_coverage) if depth_coverage else None
            ),
            "markouts": {
                str(horizon): {
                    "samples": len(values),
                    "median_bps": statistics.median(values),
                    "mean_bps": statistics.mean(values),
                    "positive_rate": statistics.mean(value > 0.0 for value in values),
                }
                for horizon, values in sorted(markouts.items())
            },
        },
    }


def _scope_has_promotion_evidence(
    summary: dict[str, Any],
    *,
    significant: bool,
) -> bool:
    stress_value = _finite(
        (summary.get("cost_stress_mean_bps") or {}).get("10")
    )
    direction_coverage = _finite(summary.get("direction_coverage"))
    direct_regime_coverage = _finite(summary.get("direct_regime_coverage"))
    return (
        summary.get("completed_campaigns", 0) >= 100
        and summary.get("calendar_days", 0) >= 30
        and summary.get("verified_regime_count", 0) >= 2
        and stress_value is not None
        and stress_value > 0.0
        and direction_coverage is not None
        and direction_coverage >= MINIMUM_DIRECTION_COVERAGE
        and direct_regime_coverage is not None
        and direct_regime_coverage >= MINIMUM_DIRECT_REGIME_COVERAGE
        and significant
    )


def _effect(high: list[float], low: list[float]) -> dict[str, Any]:
    if len(high) < 3 or len(low) < 3:
        return {"ready": False, "high_n": len(high), "low_n": len(low)}
    difference = statistics.mean(high) - statistics.mean(low)
    high_var = statistics.variance(high) if len(high) > 1 else 0.0
    low_var = statistics.variance(low) if len(low) > 1 else 0.0
    standard_error = math.sqrt(high_var / len(high) + low_var / len(low))
    z_score = difference / standard_error if standard_error > 0.0 else 0.0
    p_value = 0.5 * math.erfc(z_score / math.sqrt(2.0))
    return {
        "ready": True,
        "high_n": len(high),
        "low_n": len(low),
        "high_mean_bps": statistics.mean(high),
        "low_mean_bps": statistics.mean(low),
        "effect_bps": difference,
        "one_sided_p": p_value,
    }


def _bh(results: list[dict[str, Any]]) -> None:
    ready = [(index, row["holdout"]["one_sided_p"]) for index, row in enumerate(results)
             if row["holdout"].get("ready")]
    total = len(ready)
    adjusted: dict[int, float] = {}
    running = 1.0
    for rank, (index, p_value) in reversed(list(enumerate(
        sorted(ready, key=lambda item: item[1]), start=1
    ))):
        running = min(running, p_value * total / rank)
        adjusted[index] = running
    for index, row in enumerate(results):
        row["holdout"]["bh_q"] = adjusted.get(index)


def _feature_value(row: dict[str, Any], feature: str) -> float | None:
    if feature == "entry_quality_score":
        value = _finite(row.get(feature))
    else:
        value = _finite(row["microstructure"].get(feature))
    return value * FEATURE_DIRECTIONS[feature] if value is not None else None


def _utc_research_day(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC).date().isoformat() if parsed.tzinfo else None
    except (OverflowError, ValueError):
        return None


def _walk_forward_effect(
    usable: list[tuple[dict[str, Any], float]],
    *,
    before_date: str,
    embargo: timedelta,
) -> dict[str, Any]:
    development = [
        (row, value) for row, value in usable
        if (day := _utc_research_day(row["candidate_time"])) is not None
        and day < before_date
    ]
    dates = sorted({_utc_research_day(row["candidate_time"])
                    for row, _ in development})
    high: list[float] = []
    low: list[float] = []
    folds = []
    for test_date in dates[2:]:
        test_start = datetime.fromisoformat(test_date).replace(tzinfo=UTC)
        training = [
            (row, value) for row, value in development
            if _utc_research_day(row["candidate_time"]) < test_date
            and _time(row["campaign"]["closed_at"]) < test_start - embargo
        ]
        testing = [
            (row, value) for row, value in development
            if _utc_research_day(row["candidate_time"]) == test_date
        ]
        if not training or not testing:
            continue
        threshold = statistics.median(value for _, value in training)
        for row, value in testing:
            target = row["campaign"]["net_return_bps"]
            (high if value >= threshold else low).append(target)
        folds.append(
            {
                "test_date": test_date,
                "training_n": len(training),
                "test_n": len(testing),
                "frozen_threshold": threshold,
            }
        )
    return {**_effect(high, low), "folds": folds}


def analyze_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = list(snapshot.get("candidates") or [])
    scoped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        scoped[(row["bot"], row["mode"])].append(row)
    dates = sorted({day for row in rows
                    if (day := _utc_research_day(row.get("candidate_time"))) is not None})
    generated_day = _utc_research_day(snapshot.get("generated_at"))
    completed_dates = [day for day in dates
                       if generated_day is not None and day < generated_day]
    holdout_date = completed_dates[-1] if completed_dates else None
    partial_date = generated_day if generated_day in dates else None
    embargo = timedelta(minutes=15)
    hypotheses = []
    for (bot, mode), scope_rows in sorted(scoped.items()):
        for feature in FEATURE_DIRECTIONS:
            usable = [
                (row, _feature_value(row, feature))
                for row in scope_rows
                if row.get("campaign")
                and _campaign_exit_class(row["campaign"]) == "strategy_exit"
                and _feature_value(row, feature) is not None
                and holdout_date is not None
                and (day := _utc_research_day(row.get("candidate_time"))) is not None
                and day <= holdout_date
            ]
            development = [(row, value) for row, value in usable
                           if _utc_research_day(row["candidate_time"]) < holdout_date]
            holdout = [(row, value) for row, value in usable
                       if _utc_research_day(row["candidate_time"]) == holdout_date]
            cutoff = (
                datetime.fromisoformat(holdout_date).replace(tzinfo=UTC) - embargo
                if holdout_date else None
            )
            clean_development = [
                (row, value) for row, value in development
                if _time(row["campaign"]["closed_at"]) < cutoff
            ] if cutoff else []
            threshold = statistics.median(value for _, value in clean_development) \
                if clean_development else None
            high, low = [], []
            if threshold is not None:
                for row, value in holdout:
                    target = row["campaign"]["net_return_bps"]
                    (high if value >= threshold else low).append(target)
            hypotheses.append(
                {
                    "bot": bot,
                    "mode": mode,
                    "feature": feature,
                    "a_priori_direction": "higher_better_after_transform",
                    "development_n": len(clean_development),
                    "holdout_n": len(holdout),
                    "frozen_threshold": threshold,
                    "purged_walk_forward": _walk_forward_effect(
                        usable,
                        before_date=str(holdout_date),
                        embargo=embargo,
                    ) if holdout_date else {"ready": False, "folds": []},
                    "holdout": _effect(high, low),
                }
            )
    _bh(hypotheses)

    scopes = {
        f"{bot}|{mode}": _scope_summary(scope_rows)
        for (bot, mode), scope_rows in sorted(scoped.items())
    }
    exits = {}
    for key, scope_rows in sorted(scoped.items()):
        completed = [row for row in scope_rows if row["campaign"]]
        mfe_values = [
            row["campaign"]["mfe_pct"] for row in completed
            if row["campaign"]["mfe_pct"] is not None
        ]
        mae_values = [
            row["campaign"]["mae_pct"] for row in completed
            if row["campaign"]["mae_pct"] is not None
        ]
        giveback_values = [
            row["campaign"]["giveback_pct"] for row in completed
            if row["campaign"]["giveback_pct"] is not None
        ]
        exits["|".join(key)] = {
            "reasons": dict(Counter(row["campaign"]["reason"] for row in completed)),
            "classes": dict(
                Counter(_campaign_exit_class(row["campaign"]) for row in completed)
            ),
            "median_mfe_pct": statistics.median(mfe_values) if mfe_values else None,
            "median_mae_pct": statistics.median(mae_values) if mae_values else None,
            "median_giveback_pct": (
                statistics.median(giveback_values) if giveback_values else None
            ),
        }
    recommendations = {}
    for key, summary in scopes.items():
        significant = [
            row for row in hypotheses
            if f"{row['bot']}|{row['mode']}" == key
            and row["holdout"].get("bh_q") is not None
            and row["holdout"]["bh_q"] <= 0.05
            and row["holdout"].get("effect_bps", 0.0) > 0.0
        ]
        robust = _scope_has_promotion_evidence(
            summary,
            significant=bool(significant),
        )
        recommendations[key] = (
            "als_begrenzten_shadow_kandidaten_vorbereiten" if robust
            else "weiter_beobachten"
        )
    observed_days = max(
        (summary["observed_candidate_days"] for summary in scopes.values()),
        default=0,
    )
    tca_scopes = [
        key for key, summary in scopes.items()
        if summary["execution"]["fill_cost_samples"] > 0
    ]
    markout_scopes = [
        key for key, summary in scopes.items()
        if summary["execution"]["markouts"]
    ]
    return {
        "schema_version": 1,
        "method": {
            "frozen_holdout_utc_date": holdout_date,
            "excluded_partial_utc_date": partial_date,
            "purge_rule": "labels must close before holdout minus 15 minute embargo",
            "walk_forward_rule": (
                "expanding daily folds; each threshold fitted only on earlier "
                "candidates whose labels closed before test minus embargo"
            ),
            "multiple_testing": "Benjamini-Hochberg across fixed directional tests",
            "sequence_contract": "sequence_unverified; no queue or continuity inference",
            "survivorship_boundary": (
                "all persisted candidates included; PnL labels exist only for completed "
                "campaigns, so rejected/unfilled counterfactual PnL is not invented"
            ),
            "pbo": None,
            "dsr": None,
            "promotion_blocked": True,
        },
        "data_counts": snapshot.get("counts"),
        "scopes": scopes,
        "hypotheses": hypotheses,
        "exit_behavior": exits,
        "recommendations": recommendations,
        "uncertainties": [
            (
                f"Only {observed_days} observed UTC candidate days; "
                "30-day stability gate remains closed."
                if observed_days < 30
                else "The 30-day coverage gate alone does not establish an edge."
            ),
            "Unified CCXT L2 is snapshot-only and sequence-unverified.",
            "Depth is a contract-quantity quote proxy without venue contract multipliers.",
            (
                "Measured execution coverage is scope-limited: "
                f"TCA={tca_scopes or ['none']}; "
                f"markouts={markout_scopes or ['none']}."
            ),
            "One frozen day is diagnostic, not promotion-grade OOS evidence.",
        ],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-root", type=Path)
    parser.add_argument("--snapshot-json", type=Path)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if (args.snapshot_root is None) == (args.snapshot_json is None):
        raise SystemExit("supply exactly one of --snapshot-root or --snapshot-json")
    if args.snapshot_root is not None:
        snapshot = extract_snapshot(args.snapshot_root.resolve())
        result = {"snapshot": snapshot, "analysis": analyze_snapshot(snapshot)}
    else:
        snapshot = json.loads(args.snapshot_json.read_text(encoding="utf-8"))
        result = {"analysis": analyze_snapshot(snapshot)}
    encoded = json.dumps(result, indent=2, sort_keys=True, allow_nan=False)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temp = args.output.with_suffix(args.output.suffix + ".tmp")
        temp.write_text(encoded, encoding="utf-8")
        temp.replace(args.output)
    else:
        print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
