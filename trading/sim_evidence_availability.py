"""Canonical original-fact bindings for post-commit SIM availability witnesses.

This module never opens a database. A caller supplies its existing connection
and owns the read transaction. A witness certifies that the original facts were
available by a time; it does not claim that the witness itself existed then.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone

from trading.execution_cost_model import decode_execution_cost_payload


AVAILABILITY_SCHEMA = 1
CLOCK_BOUND_KIND = "exchange_rtt_upper_v1"
_SCOPE_FIELDS = ("bot_name", "mode", "venue", "market_type")


def sim_availability_proof_scope(proof):
    """Return only an explicit complete canonical frozen scope, else unknown."""
    scope = {key: proof.get(key) for key in _SCOPE_FIELDS}
    venue = scope["venue"]
    if (scope["bot_name"] not in {"FUTURES", "FUTREND", "CROSS", "SPOT", "TREND"}
            or scope["mode"] != "SIM" or scope["market_type"] not in {"spot", "swap"}
            or not isinstance(venue, str) or not venue or venue != venue.lower()
            or not venue.isascii() or not venue.replace("_", "").replace("-", "").isalnum()):
        return None
    return scope


def _capture_scope(candidate, stages):
    payloads = [row["payload"] for row in stages]
    scope = sim_availability_proof_scope(payloads[0])
    if (scope is None or any(type(payload.get("capture_scope_schema")) is not int
                            or payload["capture_scope_schema"] != 1 for payload in payloads)
            or any(payload.get(key) != value for payload in payloads for key, value in scope.items())
            or any(candidate.get(key) != scope[key] for key in ("bot_name", "mode"))):
        return None
    return scope


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _rows(connection, sql, parameters):
    cursor = connection.execute(sql + " LIMIT 3", parameters)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchmany(3)]


def _one(rows):
    if len(rows) != 1:
        raise ValueError("SIM availability requires unique original facts")
    return rows[0]


def _payload(row):
    raw = row.pop("payload_json")
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > 65536:
        raise ValueError("SIM availability payload is oversized")
    try:
        row["payload"] = decode_execution_cost_payload(raw)
        _canonical(row)  # Reject non-finite numbers, including overflowed JSON floats.
    except (RecursionError, OverflowError) as exc:
        raise ValueError("SIM availability payload is excessively nested") from exc
    return row


def _time_ms(value):
    if not isinstance(value, str):
        raise ValueError("SIM availability timestamp is invalid")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return _positive_ms(stamp.timestamp() * 1000)
    except (ValueError, OverflowError) as exc:
        raise ValueError("SIM availability timestamp is invalid") from exc


def _positive_ms(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("SIM availability clock is invalid")
    return float(value)


def read_sim_availability_facts(connection, entry_id, fact_key):
    """Read a small, exact, indexed projection of one original fact bundle."""
    if not isinstance(entry_id, str) or not entry_id or len(entry_id) > 200:
        raise ValueError("SIM availability entry identity is invalid")
    candidate = _one(_rows(connection, """SELECT entry_id,bot_name,mode,symbol,candidate_time
        FROM expectancy_candidates WHERE entry_id=?""", (entry_id,)))
    if candidate["mode"] != "SIM":
        raise ValueError("SIM availability requires a SIM candidate")
    stages = [_payload(row) for row in _rows(connection, """SELECT id,entry_id,
        measured_at,stage,payload_json FROM sim_execution_tca
        WHERE entry_id=? AND stage IN ('arrival','fill') ORDER BY id""", (entry_id,))]
    if len(stages) != 2 or [row["stage"] for row in stages] != ["arrival", "fill"]:
        raise ValueError("SIM availability requires one ordered arrival/fill pair")
    book = _payload(_one(_rows(connection, """SELECT entry_id,bot_name,mode,symbol,
        measured_at,source,sequence_status,payload_json FROM candidate_microstructure
        WHERE entry_id=? AND stage='arrival_book'""", (entry_id,))))
    symbol = candidate["symbol"]
    venue_symbol = book["symbol"]
    identity_matches = (isinstance(symbol, str) and isinstance(venue_symbol, str)
                        and (symbol == venue_symbol or (
                            "/" not in symbol and ":" not in symbol and "/" in venue_symbol
                            and symbol.casefold() == venue_symbol.partition("/")[0].casefold())))
    if (not identity_matches or book["bot_name"] != candidate["bot_name"] or book["mode"] != "SIM"
            or book["measured_at"] != stages[0]["measured_at"]
            or _canonical(book["payload"]) != _canonical(stages[0]["payload"])):
        raise ValueError("SIM availability arrival binding conflicts")
    cursor = connection.execute("""SELECT entry_id,horizon_seconds,symbol,side,
        reference_price,due_at FROM sim_execution_markouts WHERE entry_id=?
        ORDER BY horizon_seconds LIMIT 65""", (entry_id,))
    names = [column[0] for column in cursor.description]
    schedule = [dict(zip(names, row)) for row in cursor.fetchall()]
    if not schedule or len(schedule) > 64 or len({row["horizon_seconds"] for row in schedule}) != len(schedule):
        raise ValueError("SIM availability schedule is missing or ambiguous")
    facts = {"schema_version": AVAILABILITY_SCHEMA, "entry_id": entry_id,
             "fact_key": "entry_pair", "candidate": candidate,
             "stages": stages, "arrival_book": book, "scheduled_markouts": schedule,
             "capture_scope": _capture_scope(candidate, stages)}
    if fact_key == "entry_pair":
        return facts
    if not isinstance(fact_key, str) or not fact_key.startswith("markout_"):
        raise ValueError("SIM availability fact identity is invalid")
    suffix = fact_key[8:]
    if not suffix.isascii() or not suffix.isdecimal() or not 0 < len(suffix) <= 8:
        raise ValueError("SIM availability horizon is invalid")
    horizon = int(suffix)
    if horizon <= 0 or str(horizon) != suffix:
        raise ValueError("SIM availability horizon is invalid")
    markout = _one(_rows(connection, """SELECT entry_id,
        horizon_seconds,status,symbol,side,reference_price,due_at,measured_at,
        mark_price,markout_bps,attempts FROM sim_execution_markouts
        WHERE entry_id=? AND horizon_seconds=?""", (entry_id, horizon)))
    mirror = _payload(_one(_rows(connection, """SELECT id,entry_id,measured_at,stage,
        payload_json FROM sim_execution_tca WHERE entry_id=? AND stage=?""",
        (entry_id, f"markout_{horizon}s"))))
    payload = mirror["payload"]
    if (markout["status"] != "COMPLETE" or mirror["measured_at"] != markout["measured_at"]
            or payload.get("horizon_seconds") != horizon
            or payload.get("mark_price") != markout["mark_price"]
            or payload.get("markout_bps") != markout["markout_bps"]
            or payload.get("reference_price") != markout["reference_price"]):
        raise ValueError("SIM availability markout binding conflicts")
    return {"schema_version": AVAILABILITY_SCHEMA, "entry_id": entry_id,
            "fact_key": fact_key, "entry_pair_sha256": sim_availability_facts_digest(facts),
            "markout": markout, "mirror": mirror, "capture_scope": facts["capture_scope"]}


def sim_availability_facts_digest(facts):
    return hashlib.sha256(_canonical(facts)).hexdigest()


def sim_availability_minimum_time_ms(facts):
    """Conservative observation/scheduling lower bound; never a commit proof."""
    if facts["fact_key"] == "entry_pair":
        times = [_time_ms(facts["candidate"]["candidate_time"])]
        for row in facts["stages"]:
            times.append(_time_ms(row["measured_at"]))
            for field in ("fill_time_ms", "arrival_receipt_ms", "capture_recorded_at_ms"):
                if field in row["payload"]:
                    times.append(_positive_ms(row["payload"][field]))
        return max(times)
    return max(_time_ms(facts["markout"]["measured_at"]),
               _time_ms(facts["markout"]["due_at"]),
               _time_ms(facts["mirror"]["measured_at"]))


def validate_sim_availability_proof(proof, facts, *, cutoff_ms=None):
    """Return a rejection reason, or None for a matching original-fact witness."""
    if proof is None:
        return "missing_commit_availability"
    if proof.get("clock_bound_kind") is None:
        return "unproven_clock_availability"
    if (proof.get("schema_version") != AVAILABILITY_SCHEMA
            or type(proof.get("schema_version")) is not int
            or proof.get("entry_id") != facts["entry_id"]
            or proof.get("fact_key") != facts["fact_key"]
            or proof.get("state") != "ACKED"
            or proof.get("witness_kind") not in {"commit_ack", "recovery_read"}
            or proof.get("clock_bound_kind") != CLOCK_BOUND_KIND
            or sim_availability_proof_scope(proof) is None
            or sim_availability_proof_scope(proof) != facts.get("capture_scope")
            or proof.get("facts_sha256") != sim_availability_facts_digest(facts)):
        return "invalid_commit_availability"
    try:
        available = _positive_ms(proof.get("available_by_ms"))
        if available < sim_availability_minimum_time_ms(facts):
            return "noncausal_commit_availability"
        if cutoff_ms is not None and available > _positive_ms(cutoff_ms):
            return "late_commit_availability"
    except (KeyError, TypeError, ValueError, OverflowError):
        return "invalid_commit_availability"
    return None
