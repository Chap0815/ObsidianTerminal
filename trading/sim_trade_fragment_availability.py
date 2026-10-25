"""Forward-only availability of exact persisted SIM financial fragments.

Pure reads on a caller-owned SQLite transaction; no I/O fallback or clock.
This family does not alter entry/markout facts or certify campaign completion.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math

from trading.sim_evidence_availability import CLOCK_BOUND_KIND

FRAGMENT_SCHEMA = 1
MAX_FRAGMENT_BYTES = 64 * 1024
MAX_FIELD_BYTES = 16 * 1024
TRADE_FIELDS = (
    "id", "bot_name", "symbol", "buy_price", "sell_price", "buy_time", "sell_time",
    "profit_pct", "profit_usdt", "invested_usdt", "reason", "rsi_15m", "rsi_1h",
    "rsi_4h", "change_pct", "hour_of_day", "day_of_week", "is_win", "is_partial",
    "btc_trend", "fear_greed", "is_futures", "position_type", "leverage",
    "liquidation_price", "funding_paid", "funding_evidence_json", "fees_usdt",
    "mfe_pct", "mae_pct", "giveback_pct", "is_sim", "mode_source",
    "entry_quality_score", "entry_quality_label", "entry_quality_reasons", "entry_id",
    "exchange_order_id",
)


def _canonical(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, RecursionError, OverflowError) as exc:
        raise ValueError("invalid trade fragment facts") from exc
    if len(raw) > MAX_FRAGMENT_BYTES:
        raise ValueError("trade fragment facts exceed read budget")
    return raw


def _trade_id(value):
    if type(value) is not int or not 0 < value <= 2**63 - 1:
        raise ValueError("trade fragment id is invalid")
    return value


def read_trade_fragment_facts(connection, trade_id):
    """Read one stable row, with SQL byte bounds before materialization."""
    identity = _trade_id(trade_id)
    lengths = [f"COALESCE(length(CAST({field} AS BLOB)),0)" for field in TRADE_FIELDS]
    bounds = " AND ".join(f"{length}<=?" for length in lengths)
    sql = ("SELECT " + ",".join(TRADE_FIELDS) + " FROM trades WHERE id=? AND "
           + bounds + " AND (" + "+".join(lengths) + ")<=? LIMIT 2")
    cursor = connection.execute(sql, (identity, *([MAX_FIELD_BYTES] * len(lengths)), MAX_FRAGMENT_BYTES))
    rows = cursor.fetchmany(2)
    if len(rows) != 1:
        raise ValueError("trade fragment is missing or exceeds read budget")
    row = dict(zip(TRADE_FIELDS, rows[0]))
    if (_trade_id(row["id"]) != identity or row["bot_name"] != "FUTURES (SIM)"
            or type(row["is_sim"]) is not int or row["is_sim"] != 1
            or type(row["is_futures"]) is not int or row["is_futures"] != 1
            or row["position_type"] not in {"LONG", "SHORT"}
            or not isinstance(row["entry_id"], str) or not row["entry_id"]
            or row["is_partial"] not in (0, 1)):
        raise ValueError("trade fragment scope is unsupported")
    facts = {"schema_version": FRAGMENT_SCHEMA, "kind": "sim_trade_fragment_financials",
             "trade_id": identity, "trade": row}
    _canonical(facts)
    return facts


def trade_fragment_facts_digest(facts):
    if (type(facts) is not dict or set(facts) != {"schema_version", "kind", "trade_id", "trade"}
            or type(facts["schema_version"]) is not int or facts["schema_version"] != FRAGMENT_SCHEMA
            or facts["kind"] != "sim_trade_fragment_financials" or type(facts["trade"]) is not dict
            or set(facts["trade"]) != set(TRADE_FIELDS)
            or _trade_id(facts["trade_id"]) != _trade_id(facts["trade"]["id"])):
        raise ValueError("trade fragment facts identity is invalid")
    return hashlib.sha256(_canonical(facts)).hexdigest()


def trade_fragment_minimum_time_ms(facts):
    values = []
    for field in ("buy_time", "sell_time"):
        stamp = datetime.fromisoformat(facts["trade"][field])
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        values.append(stamp.timestamp() * 1000)
    if not all(math.isfinite(value) and value > 0 for value in values):
        raise ValueError("trade fragment time is invalid")
    return max(values)


def validate_trade_fragment_proof(proof, facts, *, cutoff_ms=None):
    if proof is None or proof.get("clock_bound_kind") is None:
        return "unproven_trade_fragment_availability"
    try:
        row = facts["trade"]
        if (type(proof.get("schema_version")) is not int or proof["schema_version"] != FRAGMENT_SCHEMA
                or proof.get("state") != "ACKED" or proof.get("clock_bound_kind") != CLOCK_BOUND_KIND
                or proof.get("witness_kind") not in {"commit_ack", "recovery_read"}
                or _trade_id(proof.get("trade_id")) != _trade_id(facts["trade_id"])
                or type(proof.get("is_sim")) is not int or proof["is_sim"] != 1
                or tuple(proof.get(key) for key in ("entry_id", "bot_name", "is_sim"))
                   != tuple(row[key] for key in ("entry_id", "bot_name", "is_sim"))
                or proof.get("facts_sha256") != trade_fragment_facts_digest(facts)):
            return "invalid_trade_fragment_availability"
        available = proof.get("available_by_ms")
        if (type(available) not in (int, float) or not math.isfinite(available)
                or available < trade_fragment_minimum_time_ms(facts)):
            return "invalid_trade_fragment_availability"
        if cutoff_ms is not None:
            if type(cutoff_ms) not in (int, float) or not math.isfinite(cutoff_ms) or cutoff_ms <= 0:
                return "invalid_trade_fragment_availability"
            if available > cutoff_ms:
                return "late_trade_fragment_availability"
    except (KeyError, TypeError, ValueError, OverflowError):
        return "invalid_trade_fragment_availability"
    return None
