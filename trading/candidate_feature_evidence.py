"""Bounded feature/receipt codecs, independent of database, clocks and runtime.

These facts certify producer observations and causal ordering, not historical
market publication, settled funding, a strategy trajectory or training readiness.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import TypedDict


CLOCK_BOUND_KIND = "exchange_rtt_upper_v1"
DECISION_CLOCK_KIND = "exchange_rtt_interval_v1"
FEATURE_POLICY_VERSION = "futures_expectancy_features_v1"
FEATURE_POLICY_VERSION_V2 = "futures_expectancy_features_consumed_regime_v2"
MAX_FEATURE_EVIDENCE_BYTES = 8192
_FEATURES = frozenset(("score", "spread_bps", "funding_rate_pct", "oi_change_pct",
                       "change_pct", "btc_change_pct"))


class FeatureContext(TypedDict):
    schema_version: int
    bot_name: str
    mode: str
    symbol: str
    venue_symbol: str
    venue: str
    market_type: str
    direction: str
    feature_policy_version: str


class ConsumedRegimeSnapshot(TypedDict):
    schema_version: int
    policy_version: str
    observation_basis: str
    regime: str
    btc_24h: float
    btc_7d: float
    confidence: str
    gate_enabled: bool
    bear_7d_threshold: float
    bull_7d_threshold: float


class FeatureContextV2(FeatureContext):
    consumed_regime: ConsumedRegimeSnapshot


class FeatureFact(TypedDict):
    schema_version: int
    entry_id: str
    context: FeatureContext | FeatureContextV2
    candidate_time: str
    feature_schema_version: int
    features: dict[str, float]


class FeatureReceipt(TypedDict):
    schema_version: int
    entry_id: str
    context: FeatureContext | FeatureContextV2
    candidate_time: str
    feature_schema_version: int
    feature_fact_sha256: str
    available_by_ms: float
    clock_bound_kind: str
    receipt_sha256: str


class FeatureReceiptOutput(TypedDict):
    receipt: FeatureReceipt


class PrefillObservation(TypedDict):
    schema_version: int
    receipt: FeatureReceipt
    process_nonce: str
    retrieved_monotonic: float
    before_sim_execution_monotonic: float
    decision_lower_ms: int
    decision_upper_ms: int
    decision_clock_kind: str


class FeatureCapsule(TypedDict):
    schema_version: int
    prefill: PrefillObservation
    fill_time_ms: float
    capsule_sha256: str


class FeatureEntryBinding(TypedDict):
    schema_version: int
    entry_id: str
    context: FeatureContext | FeatureContextV2
    feature_fact_sha256: str
    feature_receipt_sha256: str
    entry_pair_facts_sha256: str
    capsule_sha256: str
    fill_time_ms: float


def _encoded(value):
    try:
        data = json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("invalid feature evidence JSON") from exc
    if len(data) > MAX_FEATURE_EVIDENCE_BYTES:
        raise ValueError("feature evidence byte budget exceeded")
    return data


def _shape(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ValueError("feature evidence keys differ from schema")
    _encoded(value)
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise ValueError("unknown feature evidence schema")


def _number(value, *, positive=False):
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and not (positive and value <= 0)
    except (OverflowError, TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("feature evidence number is invalid")
    return float(value)


def _identity(value):
    if (not isinstance(value, str) or not 0 < len(value) <= 64
            or re.fullmatch(r"[A-Za-z0-9_.:-]+", value) is None):
        raise ValueError("feature entry identity is invalid")
    return value


def _digest(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("feature evidence digest is invalid")
    return value


def _stamp(value):
    if not isinstance(value, str):
        raise ValueError("feature candidate timestamp is invalid")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        if parsed.strftime("%Y-%m-%d %H:%M:%S") != value or parsed.timestamp() <= 0:
            raise ValueError("noncanonical candidate timestamp")
    except (ValueError, OverflowError, OSError) as exc:
        raise ValueError("feature candidate timestamp is invalid") from exc
    return value


def normalize_consumed_regime_snapshot(value) -> ConsumedRegimeSnapshot:
    _shape(value, ConsumedRegimeSnapshot.__annotations__)
    if (value["policy_version"] != "futures_regime_direction_gate_v1"
            or value["observation_basis"] != "actual_runtime_gate_operands"
            or value["regime"] not in ("BULL", "BEAR", "NEUTRAL")
            or value["confidence"] not in ("HIGH", "MEDIUM", "LOW")
            or type(value["gate_enabled"]) is not bool):
        raise ValueError("unsupported consumed regime operands")
    return {**value, **{key: _number(value[key]) for key in (
        "btc_24h", "btc_7d", "bear_7d_threshold", "bull_7d_threshold")}}


def normalize_feature_context(value) -> FeatureContext | FeatureContextV2:
    if type(value) is not dict or type(value.get("schema_version")) is not int:
        raise ValueError("unknown feature context schema")
    version = value["schema_version"]
    if version == 1:
        _shape(value, FeatureContext.__annotations__)
        policy = FEATURE_POLICY_VERSION
    elif version == 2:
        if set(value) != set(FeatureContextV2.__annotations__):
            raise ValueError("feature context keys differ from schema")
        _encoded(value)
        policy = FEATURE_POLICY_VERSION_V2
    else:
        raise ValueError("unknown feature context schema")
    if (value["bot_name"] != "FUTURES" or value["mode"] != "SIM"
            or value["venue"] != "mexc" or value["market_type"] != "swap"
            or value["feature_policy_version"] != policy
            or value["direction"] not in ("LONG", "SHORT")):
        raise ValueError("unsupported feature evidence scope")
    symbol, venue_symbol = value["symbol"], value["venue_symbol"]
    if (not isinstance(venue_symbol, str)
            or re.fullmatch(r"[A-Z0-9]{1,24}/USDT:USDT", venue_symbol) is None
            or symbol not in (venue_symbol, venue_symbol.partition("/")[0])):
        raise ValueError("feature symbols do not match bound market")
    if version == 2:
        return {**value, "consumed_regime": normalize_consumed_regime_snapshot(value["consumed_regime"])}
    return dict(value)


def normalize_feature_fact(value) -> FeatureFact:
    _shape(value, FeatureFact.__annotations__)
    context = normalize_feature_context(value["context"])
    if type(value["feature_schema_version"]) is not int or value["feature_schema_version"] != 1:
        raise ValueError("unknown FUTURES feature schema")
    features = value["features"]
    if type(features) is not dict or set(features) != _FEATURES:
        raise ValueError("FUTURES feature vector differs from schema")
    features = {name: _number(number) for name, number in features.items()}
    if not 0 <= features["score"] <= 100:
        raise ValueError("feature score is outside its range")
    return {**value, "entry_id": _identity(value["entry_id"]), "context": context,
            "candidate_time": _stamp(value["candidate_time"]), "features": features}


def build_feature_fact(*, entry_id, context, candidate_time, feature_schema_version, features) -> FeatureFact:
    return normalize_feature_fact({"schema_version": 1, "entry_id": entry_id, "context": context,
                                  "candidate_time": candidate_time,
                                  "feature_schema_version": feature_schema_version, "features": features})


def feature_fact_sha256(value) -> str:
    return hashlib.sha256(_encoded(normalize_feature_fact(value))).hexdigest()


def normalize_feature_receipt(value, *, expected_fact_sha256=None, expected_context=None,
                              expected_entry_id=None) -> FeatureReceipt:
    _shape(value, FeatureReceipt.__annotations__)
    context = normalize_feature_context(value["context"])
    if (type(value["feature_schema_version"]) is not int or value["feature_schema_version"] != 1
            or value["clock_bound_kind"] != CLOCK_BOUND_KIND):
        raise ValueError("unproven feature clock or feature schema")
    normalized = {**value, "context": context, "entry_id": _identity(value["entry_id"]),
                  "candidate_time": _stamp(value["candidate_time"]),
                  "available_by_ms": _number(value["available_by_ms"], positive=True)}
    _digest(value["feature_fact_sha256"])
    body = {key: item for key, item in normalized.items() if key != "receipt_sha256"}
    if _digest(value["receipt_sha256"]) != hashlib.sha256(_encoded(body)).hexdigest():
        raise ValueError("feature receipt digest conflicts")
    if ((expected_fact_sha256 is not None and value["feature_fact_sha256"] != expected_fact_sha256)
            or (expected_context is not None and context != normalize_feature_context(expected_context))
            or (expected_entry_id is not None and value["entry_id"] != expected_entry_id)):
        raise ValueError("feature receipt binding conflicts")
    return normalized


def build_feature_receipt(fact, *, available_by_ms, clock_bound_kind=CLOCK_BOUND_KIND) -> FeatureReceipt:
    fact = normalize_feature_fact(fact)
    body = {key: fact[key] for key in ("entry_id", "context", "candidate_time", "feature_schema_version")}
    body.update(schema_version=1, feature_fact_sha256=feature_fact_sha256(fact),
                available_by_ms=_number(available_by_ms, positive=True), clock_bound_kind=clock_bound_kind)
    return normalize_feature_receipt({**body, "receipt_sha256": hashlib.sha256(_encoded(body)).hexdigest()})


def normalize_prefill_observation(value) -> PrefillObservation:
    _shape(value, PrefillObservation.__annotations__)
    receipt = normalize_feature_receipt(value["receipt"])
    if (not isinstance(value["process_nonce"], str)
            or re.fullmatch(r"[0-9a-f]{32}", value["process_nonce"]) is None
            or value["decision_clock_kind"] != DECISION_CLOCK_KIND):
        raise ValueError("feature prefill process or interval authority is invalid")
    retrieved = _number(value["retrieved_monotonic"])
    before = _number(value["before_sim_execution_monotonic"])
    low, high = value["decision_lower_ms"], value["decision_upper_ms"]
    if (type(low) is not int or type(high) is not int or not 0 < low <= high or before < retrieved):
        raise ValueError("feature prefill causal interval is invalid")
    _number(low, positive=True)
    _number(high, positive=True)
    return {**value, "receipt": receipt, "retrieved_monotonic": retrieved,
            "before_sim_execution_monotonic": before}


def build_prefill_observation(receipt, *, process_nonce, retrieved_monotonic,
                              before_sim_execution_monotonic, decision_interval_ms) -> PrefillObservation:
    if type(decision_interval_ms) is not tuple or len(decision_interval_ms) != 2:
        raise ValueError("feature prefill requires an authoritative interval")
    return normalize_prefill_observation({"schema_version": 1, "receipt": receipt,
        "process_nonce": process_nonce, "retrieved_monotonic": retrieved_monotonic,
        "before_sim_execution_monotonic": before_sim_execution_monotonic,
        "decision_lower_ms": decision_interval_ms[0], "decision_upper_ms": decision_interval_ms[1],
        "decision_clock_kind": DECISION_CLOCK_KIND})


def normalize_feature_capsule(value, *, expected_entry_id=None, expected_context=None,
                              expected_fill_time_ms=None) -> FeatureCapsule:
    _shape(value, FeatureCapsule.__annotations__)
    prefill = normalize_prefill_observation(value["prefill"])
    normalize_feature_receipt(prefill["receipt"], expected_entry_id=expected_entry_id,
                              expected_context=expected_context)
    normalized = {**value, "prefill": prefill, "fill_time_ms": _number(value["fill_time_ms"], positive=True)}
    if expected_fill_time_ms is not None and normalized["fill_time_ms"] != expected_fill_time_ms:
        raise ValueError("feature capsule fill identity conflicts")
    body = {key: item for key, item in normalized.items() if key != "capsule_sha256"}
    if _digest(value["capsule_sha256"]) != hashlib.sha256(_encoded(body)).hexdigest():
        raise ValueError("feature capsule digest conflicts")
    return normalized


def bind_prefill_observation(observation, *, fill_time_ms) -> FeatureCapsule:
    body = {"schema_version": 1, "prefill": normalize_prefill_observation(observation),
            "fill_time_ms": _number(fill_time_ms, positive=True)}
    return normalize_feature_capsule({**body, "capsule_sha256": hashlib.sha256(_encoded(body)).hexdigest()})


def build_feature_entry_binding(capsule, *, entry_pair_facts_sha256) -> FeatureEntryBinding:
    capsule = normalize_feature_capsule(capsule)
    receipt = capsule["prefill"]["receipt"]
    return {"schema_version": 1, "entry_id": receipt["entry_id"], "context": receipt["context"],
            "feature_fact_sha256": receipt["feature_fact_sha256"],
            "feature_receipt_sha256": receipt["receipt_sha256"],
            "entry_pair_facts_sha256": _digest(entry_pair_facts_sha256),
            "capsule_sha256": capsule["capsule_sha256"], "fill_time_ms": capsule["fill_time_ms"]}


def feature_capsule_for_pending(pending) -> FeatureCapsule | None:
    """Decode optional research evidence without invalidating financial WALs."""
    try:
        if (type(pending) is not dict or pending.get("bot_name") != "FUTURES"
                or type(pending.get("version")) is not int or pending["version"] != 1
                or pending.get("side") not in ("buy", "sell")):
            return None
        fill_ms = _number(pending.get("fill_time_ms"), positive=True)
        capsule = normalize_feature_capsule(pending.get("feature_capsule_v1"),
            expected_entry_id=_identity(pending.get("entry_id")), expected_fill_time_ms=fill_ms)
        context = capsule["prefill"]["receipt"]["context"]
        if (context["venue_symbol"] != pending.get("symbol")
                or context["direction"] != ("LONG" if pending["side"] == "buy" else "SHORT")):
            return None
        return capsule
    except Exception:
        return None


def normalize_feature_entry_binding(value) -> FeatureEntryBinding:
    _shape(value, FeatureEntryBinding.__annotations__)
    return {**value, "entry_id": _identity(value["entry_id"]),
            "context": normalize_feature_context(value["context"]),
            **{key: _digest(value[key]) for key in ("feature_fact_sha256", "feature_receipt_sha256",
                "entry_pair_facts_sha256", "capsule_sha256")},
            "fill_time_ms": _number(value["fill_time_ms"], positive=True)}


def decode_feature_evidence(raw, *, kind):
    """Decode bounded strict JSON; a codec result alone is not DB authority."""
    if not isinstance(raw, (str, bytes)):
        raise ValueError("feature evidence must be JSON text")
    if len(raw.encode("utf-8") if isinstance(raw, str) else raw) > MAX_FEATURE_EVIDENCE_BYTES:
        raise ValueError("feature evidence byte budget exceeded")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate feature evidence key")
            result[key] = value
        return result

    def invalid_constant(_value):
        raise ValueError("invalid feature evidence number")

    normalizers = {"fact": normalize_feature_fact, "receipt": normalize_feature_receipt,
                   "capsule": normalize_feature_capsule, "binding": normalize_feature_entry_binding}
    try:
        normalizer = normalizers[kind]
        return normalizer(json.loads(raw, object_pairs_hook=unique, parse_constant=invalid_constant))
    except (KeyError, TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("invalid feature evidence JSON contract") from exc
