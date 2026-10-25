"""Bounded funding provenance, separate from the journal's booked numeric cost.

Complete model inputs do not turn an estimate into an account payment. Legacy
NULL and explicit unknown evidence never certify that the real cost was zero.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime


FUNDING_EVIDENCE_SCHEMA = 1
MAX_FUNDING_EVIDENCE_BYTES = 8 * 1024
_BOTS = frozenset({"SPOT", "TREND", "FUTURES", "FUTREND", "CROSS"})
_STATUSES = frozenset({"verified_account_payment", "estimated_public_model", "unknown"})
_SOURCE_BASES = frozenset({
    ("private_account_payments", "account_payment_sum_v1"),
    ("public_settlements", "settlement_mark_quantity_v1"),
    ("public_current_rate_schedule", "entry_notional_current_rate_schedule_v1"),
    ("unavailable", "unknown"),
})
_TOKEN = re.compile(r"[A-Za-z0-9_.:/@+\-]+\Z", re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_FIELDS = frozenset({
    "schema_version", "status", "binding", "cost_usdt", "booked_cost_usdt",
    "source", "model_basis", "coverage", "allocation",
})
_BINDING_FIELDS = frozenset({
    "bot_name", "mode", "symbol", "entry_id", "buy_time", "sell_time",
    "is_futures", "is_partial", "exchange_order_id",
})
_COVERAGE_FIELDS = frozenset({"complete", "observed_settlements", "source_sha256"})
_ALLOCATION_FIELDS = frozenset({
    "fragment_base_quantity", "pos_base_quantity", "previously_booked_cost_usdt",
})


def _keys(value, fields, label):
    if type(value) is not dict or set(value) != fields:
        raise ValueError(f"funding evidence {label} structure is invalid")


def _number(value, *, nullable=False, positive=False):
    if value is None and nullable:
        return None
    if type(value) not in (int, float):
        raise ValueError("funding evidence numeric field is invalid")
    try:
        number = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError("funding evidence numeric field is invalid") from exc
    if not math.isfinite(number) or (positive and number <= 0):
        raise ValueError("funding evidence numeric field is invalid")
    return 0.0 if number == 0.0 else number


def _token(value, maximum, *, nullable=False):
    if value is None and nullable:
        return None
    if type(value) is not str or not 1 <= len(value) <= maximum or not _TOKEN.fullmatch(value):
        raise ValueError("funding evidence identity field is invalid")
    return value


def _timestamp(value):
    if type(value) is not str or len(value) != 19:
        raise ValueError("funding evidence requires frozen canonical UTC times")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError("funding evidence requires frozen canonical UTC times") from exc
    if parsed.strftime("%Y-%m-%d %H:%M:%S") != value:
        raise ValueError("funding evidence requires frozen canonical UTC times")
    return parsed


def _canonical(value):
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("funding evidence JSON is invalid") from exc
    if len(encoded.encode("utf-8")) > MAX_FUNDING_EVIDENCE_BYTES:
        raise ValueError("funding evidence exceeds its byte limit")
    return encoded


def validate_funding_evidence(value: dict) -> dict:
    """Validate a closed evidence contract; no account authenticity is inferred."""
    _keys(value, _FIELDS, "root")
    if type(value["schema_version"]) is not int or value["schema_version"] != FUNDING_EVIDENCE_SCHEMA:
        raise ValueError("funding evidence schema is unsupported")
    status = value["status"]
    if type(status) is not str or len(status) > 32 or status not in _STATUSES:
        raise ValueError("funding evidence status is invalid")
    binding = value["binding"]
    _keys(binding, _BINDING_FIELDS, "binding")
    if type(binding["bot_name"]) is not str or len(binding["bot_name"]) > 16 or binding["bot_name"] not in _BOTS:
        raise ValueError("funding evidence bot binding is invalid")
    if type(binding["mode"]) is not str or binding["mode"] not in {"LIVE", "SIM"}:
        raise ValueError("funding evidence mode binding is invalid")
    _token(binding["symbol"], 64)
    _token(binding["entry_id"], 64, nullable=True)
    _token(binding["exchange_order_id"], 128, nullable=True)
    if type(binding["is_futures"]) is not bool or type(binding["is_partial"]) is not bool:
        raise ValueError("funding evidence fragment flags are invalid")
    if _timestamp(binding["sell_time"]) < _timestamp(binding["buy_time"]):
        raise ValueError("funding evidence close precedes its open")
    source, basis = value["source"], value["model_basis"]
    if (
        type(source) is not str or len(source) > 64
        or type(basis) is not str or len(basis) > 80
        or (source, basis) not in _SOURCE_BASES
    ):
        raise ValueError("funding evidence source/model basis is invalid")
    coverage = value["coverage"]
    _keys(coverage, _COVERAGE_FIELDS, "coverage")
    if type(coverage["complete"]) is not bool:
        raise ValueError("funding evidence coverage flag is invalid")
    observed = coverage["observed_settlements"]
    if type(observed) is not int or not 0 <= observed <= 1_000_000:
        raise ValueError("funding evidence observation count is invalid")
    digest = coverage["source_sha256"]
    if digest is not None and (type(digest) is not str or not _SHA256.fullmatch(digest)):
        raise ValueError("funding evidence source fingerprint is invalid")
    allocation = value["allocation"]
    _keys(allocation, _ALLOCATION_FIELDS, "allocation")
    fragment = _number(allocation["fragment_base_quantity"], nullable=True, positive=True)
    position = _number(allocation["pos_base_quantity"], nullable=True, positive=True)
    previously = _number(allocation["previously_booked_cost_usdt"], nullable=True)
    if fragment is not None and position is not None and fragment > position:
        raise ValueError("funding evidence fragment quantity exceeds its position")
    cost = _number(value["cost_usdt"], nullable=True)
    booked = _number(value["booked_cost_usdt"])
    if status == "unknown":
        if cost is not None or coverage["complete"]:
            raise ValueError("unknown funding must not certify a numeric cost or complete coverage")
    else:
        if cost is None or not math.isclose(cost, booked, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("funding evidence cost conflicts with its booked amount")
        if not binding["is_futures"] or not coverage["complete"] or digest is None or fragment is None:
            raise ValueError("funding evidence cost lacks complete model/source inputs")
        if status == "verified_account_payment":
            if (
                binding["mode"] != "LIVE" or source != "private_account_payments"
                or binding["is_partial"] or position != fragment
                or previously not in (None, 0.0) or observed < 1
            ):
                raise ValueError("verified funding requires an exact LIVE account payment allocation")
        elif source not in {"public_settlements", "public_current_rate_schedule"}:
            raise ValueError("estimated funding requires an explicit public model")
    result = {
        **value, "binding": dict(binding), "coverage": dict(coverage),
        "cost_usdt": cost, "booked_cost_usdt": booked,
        "allocation": {"fragment_base_quantity": fragment, "pos_base_quantity": position,
                       "previously_booked_cost_usdt": previously},
    }
    _canonical(result)
    return result


def build_funding_evidence(
    *, status, bot_name, mode_is_sim, symbol, entry_id, buy_time, sell_time,
    booked_cost_usdt, source, model_basis, is_futures=True, is_partial=False,
    exchange_order_id=None, cost_usdt=None, coverage_complete=False,
    observed_settlements=0, source_sha256=None, fragment_base_quantity=None,
    pos_base_quantity=None, previously_booked_cost_usdt=None,
) -> dict:
    """Build a WAL-safe object from already frozen fragment facts."""
    if type(mode_is_sim) is not bool:
        raise ValueError("funding evidence requires explicit accounting mode")
    if type(bot_name) is str and bot_name.endswith(" (SIM)"):
        if not mode_is_sim:
            raise ValueError("funding evidence bot conflicts with its accounting mode")
        bot_name = bot_name[:-6]
    return validate_funding_evidence({
        "schema_version": FUNDING_EVIDENCE_SCHEMA, "status": status,
        "binding": {"bot_name": bot_name, "mode": "SIM" if mode_is_sim else "LIVE",
                    "symbol": symbol, "entry_id": entry_id, "buy_time": buy_time,
                    "sell_time": sell_time, "is_futures": is_futures,
                    "is_partial": is_partial, "exchange_order_id": exchange_order_id},
        "cost_usdt": cost_usdt, "booked_cost_usdt": booked_cost_usdt,
        "source": source, "model_basis": model_basis,
        "coverage": {"complete": coverage_complete, "observed_settlements": observed_settlements,
                     "source_sha256": source_sha256},
        "allocation": {"fragment_base_quantity": fragment_base_quantity,
                       "pos_base_quantity": pos_base_quantity,
                       "previously_booked_cost_usdt": previously_booked_cost_usdt},
    })


def serialize_funding_evidence(
    value, *, bot_name, mode_is_sim, symbol, entry_id, buy_time, sell_time,
    is_futures, is_partial, exchange_order_id, funding_paid,
) -> str | None:
    """Bind evidence to the exact journal fragment before a DB transaction."""
    if value is None:
        return None
    normalized = validate_funding_evidence(value)
    expected = build_funding_evidence(
        status="unknown", bot_name=bot_name, mode_is_sim=mode_is_sim, symbol=symbol,
        entry_id=entry_id, buy_time=buy_time, sell_time=sell_time,
        is_futures=is_futures, is_partial=is_partial, exchange_order_id=exchange_order_id,
        booked_cost_usdt=_number(funding_paid), source="unavailable", model_basis="unknown",
    )
    if normalized["binding"] != expected["binding"] or not math.isclose(
        normalized["booked_cost_usdt"], expected["booked_cost_usdt"], rel_tol=0.0, abs_tol=1e-9
    ):
        raise ValueError("funding evidence does not match the frozen trade fragment")
    return _canonical(normalized)


def decode_funding_evidence(raw) -> dict | None:
    """Read legacy NULL or strictly bounded JSON without accepting duplicate keys."""
    if raw is None:
        return None
    if type(raw) not in (str, bytes):
        raise ValueError("funding evidence JSON must be text")
    encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
    if len(encoded) > MAX_FUNDING_EVIDENCE_BYTES:
        raise ValueError("funding evidence exceeds its byte limit")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate funding evidence field")
            result[key] = value
        return result

    def reject_constant(_value):
        raise ValueError("funding evidence JSON constant is invalid")

    try:
        value = json.loads(encoded.decode("utf-8"), object_pairs_hook=unique,
                           parse_constant=reject_constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("funding evidence JSON is invalid") from exc
    return validate_funding_evidence(value)


def funding_quality_for_trade(value, **binding) -> str:
    """Classify the bound evidence without upgrading a model to an account fact.

    A settled-rate model has its own quality tag. Its native rates, marks and
    causal quantity timeline still need a verified provider before it can label
    net returns; this rule depends on proof, not on SIM mode alone.
    """
    bot = binding.get("bot_name")
    if isinstance(bot, str) and bot.endswith(" (SIM)"):
        bot = bot[:-6]
    is_futures = binding.get("is_futures")
    if is_futures is False and bot in {"SPOT", "TREND"}:
        return "not_applicable"
    if is_futures is not True or value is None:
        return "unknown"
    try:
        evidence = validate_funding_evidence(value) if type(value) is dict else decode_funding_evidence(value)
        serialize_funding_evidence(evidence, **binding)
    except (TypeError, ValueError, OverflowError):
        return "invalid"
    if evidence["status"] == "verified_account_payment":
        return "verified_account_payment"
    if evidence["status"] == "estimated_public_model":
        return (
            "estimated_current_rate_schedule"
            if evidence["source"] == "public_current_rate_schedule"
            else "estimated_settlement_model_unverified"
        )
    return "unknown"


def funding_net_label_is_trusted(quality: str) -> bool:
    """Current verified sources; public model provider verification is separate."""
    return quality in {"not_applicable", "verified_account_payment"}
