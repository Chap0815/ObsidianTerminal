"""Opt-in offline SIM cost diagnostics from validated native settlement facts.

No network, clock attestation, journal writes, training labels or account-payment
claims. Input hashes identify the observed facts, not historical availability.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


DIAGNOSTIC_BASIS = "settled_fair_min1_diagnostic_v1"
MAX_CAMPAIGNS = 10_000
MAX_TRADE_ROWS = 50_000
MAX_FRAGMENTS = 256
_FACTORY = object()


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _provider_digest(sources):
    return _digest({"diagnostic_basis": DIAGNOSTIC_BASIS,
                    "sources": [{"symbol": s.symbol, "source_sha256": s.source_sha256,
                        "start_ms": s.start_ms, "end_ms": s.end_ms, "periods": s.periods}
                        for s in sources]})


def _number(value, *, positive=False):
    if type(value) not in (int, float) or not math.isfinite(value) or (positive and value <= 0):
        raise ValueError("invalid_model_number")
    return float(value)


def _stamp(value):
    if not isinstance(value, str):
        raise ValueError("invalid_model_timestamp")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)


@dataclass(frozen=True)
class _Source:
    symbol: str
    source_sha256: str
    start_ms: int
    end_ms: int
    # settlement ms, rate, cycle hours, model close, stress low, stress high
    periods: tuple[tuple, ...]


@dataclass(frozen=True)
class SettledFundingProvider:
    sources: tuple[_Source, ...]
    fingerprint: str
    _authority: object

    def provenance(self):
        return {"diagnostic_basis": DIAGNOSTIC_BASIS, "status": "estimated_public_model",
                "model_basis": "last_closed_fair_min1_close_v1",
                "provider_sha256": self.fingerprint,
                "source_sha256": [s.source_sha256 for s in self.sources],
                "source_validation": "native_inputs_validated",
                "training_eligible": False,
                "historical_label_availability_verified": False,
                "historical_feature_availability_verified": False,
                "historical_publication_verified": False,
                "account_payment_verified": False, "promotion_eligible": False}


def load_settled_funding_provider(paths):
    """Fully validate bounded inputs for diagnostics, without a temporal grant.

    Tool imports are deliberately confined to this explicit offline operation.
    Neither mtime, API receipt nor the caller's clock establishes label PIT.
    """
    from tools import native_funding_snapshot as native

    if not isinstance(paths, (list, tuple)) or not 1 <= len(paths) <= 16:
        raise ValueError("model_requires_1_to_16_native_snapshots")
    snapshots, size = [], 0
    for path in paths:
        path = native._plain_path(Path(path))
        size += path.stat().st_size
        if size > 64 * 1024 * 1024:
            raise ValueError("model_snapshot_byte_budget_exceeded")
        snapshots.append(native.read_snapshot(path))
    return provider_from_native_snapshots(snapshots)


def provider_from_native_snapshots(snapshots):
    """Validate archived public input content; this establishes no availability.

    The forward-corpus reader separately requires its persisted first witness.
    File and archive loaders share exactly the same native model construction.
    """
    from tools import native_funding_snapshot as native

    if type(snapshots) not in (list, tuple) or not 1 <= len(snapshots) <= 16:
        raise ValueError("model_requires_1_to_16_native_snapshots")
    sources, size = [], 0
    for snapshot in snapshots:
        size += len(_canonical(snapshot))
        if size > 64 * 1024 * 1024:
            raise ValueError("model_snapshot_byte_budget_exceeded")
        rows, marks = native._validate(snapshot)
        periods = tuple((row["settleTime"], row["fundingRate"], row["collectCycle"],
                         mark["response"]["data"]["close"][0],
                         min(mark["response"]["data"]["low"]),
                         max(mark["response"]["data"]["high"]))
                        for row, mark in zip(rows, marks))
        sources.append(_Source(snapshot["symbol"], snapshot["source_sha256"],
                               snapshot["start_ms"], snapshot["end_ms"], periods))
    if len({s.symbol for s in sources}) != len(sources):
        raise ValueError("ambiguous_model_symbol_sources")
    sources = tuple(sorted(sources, key=lambda item: item.symbol))
    fingerprint = _provider_digest(sources)
    return SettledFundingProvider(sources, fingerprint, _FACTORY)


def require_provider(provider):
    if type(provider) is not SettledFundingProvider or provider._authority is not _FACTORY:
        raise ValueError("model_requires_native_provider_factory")
    if provider.fingerprint != _provider_digest(provider.sources):
        raise ValueError("model_provider_binding_changed")


def read_model_candidates(connection):
    """Only original SQLite candidates in the caller's consistent read snapshot."""
    if not connection.in_transaction:
        raise ValueError("model_requires_read_transaction")
    rows = connection.execute("""SELECT entry_id,candidate_time,schema_version,
        features_json,symbol FROM expectancy_candidates
        WHERE bot_name='FUTURES' AND mode='SIM' ORDER BY candidate_time,entry_id LIMIT ?""",
        (MAX_CAMPAIGNS + 1,)).fetchall()
    if len(rows) > MAX_CAMPAIGNS:
        raise ValueError("model_candidate_budget_exceeded")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate_model_candidate_field")
            result[key] = value
        return result

    def reject_constant(_value):
        raise ValueError("invalid_model_feature")

    result = {}
    for row in rows:
        identity, raw = row["entry_id"], row["features_json"]
        if (not isinstance(identity, str) or not 1 <= len(identity) <= 200
                or identity != identity.strip() or identity in result
                or not isinstance(raw, str) or len(raw.encode()) > 65536):
            raise ValueError("invalid_model_candidate_identity")
        features = json.loads(raw, object_pairs_hook=unique, parse_constant=reject_constant)
        if not isinstance(features, dict) or len(features) > 128:
            raise ValueError("invalid_model_features")
        for value in features.values():
            _number(value)
        if type(row["schema_version"]) is not int or row["schema_version"] <= 0:
            raise ValueError("invalid_model_candidate_schema")
        result[identity] = {"entry_id": identity, "candidate_time": _stamp(row["candidate_time"]),
                            "schema_version": row["schema_version"], "features": features,
                            "symbol": row["symbol"], "source": "sqlite"}
    return result


@dataclass(frozen=True)
class CampaignCostDiagnostic:
    net_profit: float
    proof_sha256: str
    funding_cost: float
    funding_stress_low: float
    funding_stress_high: float


def diagnose_campaign(connection, provider, campaign, event):
    """Diagnose a complete no-addon FUTURES SIM campaign; otherwise raise veto.

    All original facts, fragments and their digest witnesses are read through
    the same transaction. BASE comes from the entry's actual notional/fill,
    never today's contractSize or a future native snapshot's contractSize.
    """
    require_provider(provider)
    if not connection.in_transaction or not 1 <= len(campaign) <= MAX_FRAGMENTS:
        raise ValueError("model_requires_bounded_complete_campaign")
    from trading.sim_evidence_availability import (
        read_sim_availability_facts, sim_availability_facts_digest,
        validate_sim_availability_proof,
    )
    from trading.funding_evidence import decode_funding_evidence, serialize_funding_evidence
    from trading.historical_futures_evidence import HistoricalFundingTimeline
    from trading.execution_cost_model import validate_execution_cost_arrival, validate_execution_cost_fill
    from trading.capture_replay_validation import _capture_contract_anchor

    identity = event["entry_id"]
    if (sum(row["is_partial"] == 0 for row in campaign) != 1
            or len({row["buy_time"] for row in campaign}) != 1
            or any(type(row["is_partial"]) is not int or row["is_partial"] not in (0, 1)
                   for row in campaign)):
        raise ValueError("model_requires_complete_frozen_fragments")
    facts = read_sim_availability_facts(connection, identity, "entry_pair")
    if facts.get("capture_scope") != {"bot_name": "FUTURES", "mode": "SIM", "venue": "mexc", "market_type": "swap"}:
        raise ValueError("model_entry_scope_unknown")
    candidate = facts["candidate"]
    if candidate["symbol"] != event["symbol"] or _stamp(candidate["candidate_time"]) != event["candidate_time"]:
        raise ValueError("model_candidate_identity_conflict")
    proofs = connection.execute("SELECT * FROM sim_execution_availability WHERE entry_id=? AND fact_key='entry_pair' LIMIT 2",
                                (identity,)).fetchall()
    if len(proofs) != 1:
        raise ValueError("missing_model_entry_commit_proof")
    proof = dict(proofs[0])
    reason = validate_sim_availability_proof(proof, facts)
    if reason:
        raise ValueError(reason)
    arrival, fill = [row["payload"] for row in facts["stages"]]
    anchor = _capture_contract_anchor(arrival, fill)
    if anchor is None or any(_stamp(row["measured_at"]) != anchor for row in facts["stages"]):
        raise ValueError("model_current_entry_capture_contract_required")
    validate_execution_cost_arrival(arrival)
    validate_execution_cost_fill(fill)
    entry_price = _number(fill.get("average_fill_price"), positive=True)
    entry_notional = _number(arrival.get("notional_usdt"), positive=True)
    _number(arrival.get("amount"), positive=True)
    entry_base = _number(entry_notional / entry_price, positive=True)
    filled_ms = _number(fill.get("fill_time_ms"), positive=True)
    if (arrival.get("fill_time_ms") != filled_ms
            or arrival.get("bot_name") != "FUTURES" or fill.get("bot_name") != "FUTURES"
            or arrival.get("mode") != "SIM" or fill.get("mode") != "SIM"
            or arrival.get("research_simulated") is not True or fill.get("research_simulated") is not True):
        raise ValueError("model_actual_entry_tca_required")
    symbol = facts["arrival_book"]["symbol"]
    source = next((s for s in provider.sources if s.symbol == symbol), None)
    if source is None:
        raise ValueError("model_native_symbol_unavailable")
    contexts = connection.execute("SELECT direction FROM expectancy_candidate_context WHERE entry_id=? LIMIT 2", (identity,)).fetchall()
    if len(contexts) != 1 or contexts[0][0] not in {"LONG", "SHORT"}:
        raise ValueError("model_entry_direction_unknown")
    side = contexts[0][0]
    if arrival.get("side") != ("buy" if side == "LONG" else "sell"):
        raise ValueError("model_entry_side_conflict")
    values = [(datetime.fromtimestamp(ms / 1000, timezone.utc), rate, price, cycle)
              for ms, rate, cycle, price, _low, _high in source.periods]
    timeline = HistoricalFundingTimeline.from_settled_history({symbol: values})
    quantities, profits, booked, costs, lows, highs, bindings = [], [], [], [], [], [], []
    for row in campaign:
        if (row["bot_name"] not in {"FUTURES", "FUTURES (SIM)"} or row["is_sim"] != 1
                or row["is_futures"] != 1 or row["position_type"] != side
                or row["entry_id"] != identity or row["symbol"] not in {symbol, symbol.partition("/")[0]}):
            raise ValueError("model_fragment_scope_conflict")
        opened, closed = _stamp(row["buy_time"]), _stamp(row["sell_time"])
        opened_ms, closed_ms = opened.timestamp() * 1000, closed.timestamp() * 1000
        if not opened_ms <= filled_ms < opened_ms + 1000 or closed_ms < filled_ms:
            raise ValueError("model_entry_fill_time_conflict")
        if not source.start_ms <= filled_ms <= closed_ms <= source.end_ms:
            raise ValueError("model_native_window_incomplete")
        evidence = decode_funding_evidence(row["funding_evidence_json"])
        if evidence is None:
            raise ValueError("model_frozen_fragment_quantity_missing")
        serialize_funding_evidence(evidence, bot_name=row["bot_name"], mode_is_sim=True,
            symbol=row["symbol"], entry_id=identity, buy_time=row["buy_time"], sell_time=row["sell_time"],
            is_futures=True, is_partial=row["is_partial"] == 1,
            exchange_order_id=row["exchange_order_id"], funding_paid=row["funding_paid"])
        allocation = evidence["allocation"]
        quantity = _number(allocation["fragment_base_quantity"], positive=True)
        original = _number(allocation["pos_base_quantity"], positive=True)
        price, leverage = _number(row["buy_price"], positive=True), _number(row["leverage"], positive=True)
        margin = _number(row["invested_usdt"], positive=True)
        if (not math.isclose(original, entry_base, rel_tol=1e-9, abs_tol=0.0)
                or not math.isclose(price, entry_price, rel_tol=1e-9, abs_tol=0.0)
                or not math.isclose(margin * leverage, quantity * entry_price, rel_tol=1e-9, abs_tol=0.0)):
            raise ValueError("model_no_addon_quantity_conflict")
        # Second-resolution close strings are frozen accounting cutoffs; the
        # uncertainty veto prevents claiming precise membership near a batch.
        if any(abs(ms - filled_ms) <= 15_000 or abs(ms - closed_ms) <= 15_000
               for ms, *_rest in source.periods):
            raise ValueError("model_settlement_membership_unknown")
        entry = datetime.fromtimestamp(filled_ms / 1000, timezone.utc)
        charge = timeline.charge(symbol, side, quantity * entry_price, entry, closed,
                                 base_amount=quantity, require_complete=True)
        stress = [sorted(((-1 if side == "SHORT" else 1) * rate * quantity * low,
                          (-1 if side == "SHORT" else 1) * rate * quantity * high))
                  for ms, rate, _cycle, _price, low, high in source.periods if filled_ms < ms <= closed_ms]
        quantities.append(quantity)
        profits.append(_number(row["profit_usdt"]))
        booked.append(_number(row["funding_paid"]))
        costs.append(charge.cost_usdt)
        lows.append(math.fsum(pair[0] for pair in stress))
        highs.append(math.fsum(pair[1] for pair in stress))
        bindings.append({key: row[key] for key in row.keys()})
    if not math.isclose(math.fsum(quantities), entry_base, rel_tol=1e-9, abs_tol=0.0):
        raise ValueError("model_fragment_quantity_not_conserved")
    net = _number(math.fsum([*profits, *booked, *(-cost for cost in costs)]))
    digest = _digest({"diagnostic_basis": DIAGNOSTIC_BASIS, "provider_sha256": provider.fingerprint,
                      "entry_facts_sha256": sim_availability_facts_digest(facts),
                      "candidate_features": event["features"], "fragments": bindings})
    return CampaignCostDiagnostic(net, digest,
                         math.fsum(costs), math.fsum(lows), math.fsum(highs))
