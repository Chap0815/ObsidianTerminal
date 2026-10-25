"""Immutable, explicitly estimated funding labels for forward research only.

Original journal cashflows are never changed. First generation per entry and
policy is reserved even while unavailable; later results cannot replace it.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import math

from trading.funding_forward_corpus import (
    CLOCK_KIND, _canonical, _decode, _identity, _one, _rollback_or_close, _sha,
    read_registered_funding_sources,
)


MAX_GENERATIONS = 10_000
MAX_GENERATION_BYTES = 64 * 1024
MAX_SOURCE_SETS = 10_000
MAX_CACHED_SOURCES = 128
MAX_CACHED_SOURCE_BYTES = 64 * 1024 * 1024
MAX_CONSUMED_REGIME_PROJECTION_BYTES = 4 * 1024 * 1024
POLICY_FILES = (
    "trading/funding_forward_generation.py", "trading/settled_funding_model.py",
    "trading/historical_futures_evidence.py", "trading/execution_cost_model.py",
    "trading/sim_evidence_availability.py", "trading/sim_trade_fragment_availability.py",
    "trading/candidate_feature_evidence.py", "trading/funding_evidence.py",
    "trading/funding_forward_corpus.py", "trading/profit_research_runner.py",
    "trading/exit_evidence.py", "tools/native_funding_snapshot.py",
    "trading/capture_replay_validation.py", "core/database.py", "core/constants.py",
    "trading/expectancy_training.py",
)


class FundingCorpusUnavailable(ValueError):
    """An expected unsupported or not-yet-proven campaign; never corruption."""


_MODEL_ELIGIBILITY_VETOES = frozenset({
    "model_requires_complete_frozen_fragments", "model_native_symbol_unavailable",
    "model_frozen_fragment_quantity_missing", "model_no_addon_quantity_conflict",
    "model_settlement_membership_unknown", "model_fragment_quantity_not_conserved",
    "model_native_window_incomplete", "model_entry_fill_time_conflict",
    "model_actual_entry_tca_required", "model_current_entry_capture_contract_required",
    "model_entry_direction_unknown",
})


def current_funding_model_policy():
    from tools.native_funding_snapshot import _plain_path

    root = Path(__file__).resolve().parent.parent
    hashes = {}
    for name in POLICY_FILES:
        with _plain_path(root / name).open("rb") as stream:
            raw = stream.read(2 * 1024 * 1024 + 1)
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError("funding_policy_source_byte_budget_exceeded")
        hashes[name] = _sha(raw)
    body = {"schema_version": 1, "basis": "settled_public_fair_min1_forward_v1",
            "feature_schema_version": 1, "source_code_sha256": hashes,
            "source_selection": "earliest_acked_covering_upper_then_source_sha_at_calculation_v1",
            "generation_selection": "first_original_ordinal_no_fallback_v1",
            "training_grouping_policy": "entry_utc_day_v1",
            "strategy_exits_only": True, "complete_no_addon_only": True,
            "account_payment_verified": False, "promotion_eligible": False}
    return {**body, "policy_id": _sha(_canonical(body))}


def initialize_funding_generation_schema(connection):
    connection.execute("""CREATE TABLE IF NOT EXISTS funding_forward_generations (
        generation_ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
        generation_id TEXT NOT NULL UNIQUE, inputs_sha256 TEXT NOT NULL UNIQUE,
        policy_id TEXT NOT NULL, entry_id TEXT NOT NULL, source_set_id TEXT NOT NULL,
        generation_json TEXT NOT NULL, facts_sha256 TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'PENDING', available_by_ms REAL,
        witness_kind TEXT, clock_bound_kind TEXT, retirement_reason TEXT,
        FOREIGN KEY(entry_id) REFERENCES expectancy_candidates(entry_id) ON DELETE CASCADE,
        FOREIGN KEY(source_set_id) REFERENCES funding_forward_source_sets(source_set_id))""")
    connection.execute("""CREATE INDEX IF NOT EXISTS idx_funding_forward_generation_entry
        ON funding_forward_generations(policy_id,entry_id,generation_ordinal)""")
    connection.execute("""CREATE INDEX IF NOT EXISTS idx_funding_forward_generation_pending
        ON funding_forward_generations(state,generation_ordinal)""")


def _stamp(value):
    if isinstance(value, datetime):
        result = value
    elif type(value) is str:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
    else:
        raise ValueError("invalid_funding_generation_timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("funding_generation_requires_aware_timestamp")
    result = result.astimezone(timezone.utc)
    if not math.isfinite(result.timestamp()):
        raise ValueError("invalid_funding_generation_timestamp")
    return result


def _availability(proof):
    stamp = proof["available_by_ms"]
    if type(stamp) not in (int, float) or not math.isfinite(stamp) or stamp <= 0:
        raise ValueError("invalid_funding_generation_availability")
    return stamp


def _source_for_campaign(connection, *, symbol, opened_ms, closed_ms):
    # Select inputs before computing their PnL effect. Metadata is bounded and
    # content-addressed; only the chosen source's complete bytes are decoded.
    candidates = []
    cursor = connection.execute("""SELECT source_set_id,available_by_ms,
        CASE WHEN length(CAST(manifest_json AS BLOB))<=32768 THEN manifest_json END
        FROM funding_forward_source_sets WHERE state='ACKED'
        ORDER BY available_by_ms,source_set_id LIMIT ?""", (MAX_SOURCE_SETS + 1,))
    for number, row in enumerate(cursor):
        if number >= MAX_SOURCE_SETS:
            raise ValueError("funding_source_set_row_budget_exceeded")
        _identity(row[0])
        manifest = _decode(row[2]) if row[2] is not None else None
        if (type(manifest) is not dict or _sha(_canonical(manifest)) != row[0]
                or type(manifest.get("sources")) is not list
                or type(row[1]) not in (int, float) or not math.isfinite(row[1]) or row[1] <= 0):
            raise ValueError("invalid_funding_source_selection_metadata")
        for item in manifest["sources"]:
            if (type(item) is not dict or type(item.get("start_ms")) is not int
                    or type(item.get("end_ms")) is not int):
                raise ValueError("invalid_funding_source_selection_metadata")
            if item.get("symbol") == symbol and item["start_ms"] <= opened_ms <= closed_ms <= item["end_ms"]:
                candidates.append((row[1], row[0]))
                break
    if not candidates:
        raise FundingCorpusUnavailable("model_native_window_unavailable")
    return min(candidates)[1]


def _collect_inputs(connection, entry_id, policy, *, source_set_id=None, source_cache=None):
    from core.database import read_candidate_feature_evidence, read_sim_feature_entry_binding
    from trading.sim_evidence_availability import read_sim_availability_facts, validate_sim_availability_proof, sim_availability_facts_digest
    from trading.sim_trade_fragment_availability import read_trade_fragment_facts, trade_fragment_facts_digest, validate_trade_fragment_proof
    from trading.settled_funding_model import diagnose_campaign, MAX_FRAGMENTS
    from trading.profit_research_runner import _complete_expectancy_campaign
    from trading.exit_evidence import is_strategy_exit

    if not connection.in_transaction:
        raise ValueError("funding_generation_requires_read_snapshot")
    feature = read_candidate_feature_evidence(connection, entry_id)
    binding = read_sim_feature_entry_binding(connection, entry_id)
    if feature is None or binding is None:
        raise FundingCorpusUnavailable("missing_prefill_feature_evidence")
    fact, capsule = feature["fact"], binding["capsule"]
    entry_facts = read_sim_availability_facts(connection, entry_id, "entry_pair")
    entry_proof = _one(connection, "SELECT * FROM sim_execution_availability WHERE entry_id=? AND fact_key='entry_pair'", (entry_id,))
    if entry_proof is None or entry_proof.get("state") == "PENDING":
        raise FundingCorpusUnavailable("missing_entry_commit_availability")
    reason = validate_sim_availability_proof(entry_proof, entry_facts)
    if reason:
        raise ValueError(reason)
    identities = connection.execute("SELECT id FROM trades WHERE entry_id=? ORDER BY sell_time,id LIMIT ?",
                                    (entry_id, MAX_FRAGMENTS + 1)).fetchall()
    if not identities:
        raise FundingCorpusUnavailable("missing_closed_campaign")
    if len(identities) > MAX_FRAGMENTS:
        raise ValueError("funding_generation_fragment_budget_exceeded")
    campaign, fragments, times = [], [], [_availability(feature["proof"]), _availability(binding["proof"]), _availability(entry_proof)]
    for row in identities:
        fragment = read_trade_fragment_facts(connection, row[0])
        proof = _one(connection, "SELECT * FROM sim_trade_fragment_availability WHERE trade_id=?", (row[0],))
        if proof is None or proof.get("state") == "PENDING":
            raise FundingCorpusUnavailable("missing_fragment_commit_availability")
        reason = validate_trade_fragment_proof(proof, fragment)
        if reason:
            raise ValueError(reason)
        campaign.append({**fragment["trade"], "_row_id": row[0]})
        fragments.append({"trade_id": row[0], "facts_sha256": trade_fragment_facts_digest(fragment),
                          "available_by_ms": _availability(proof)})
        times.append(_availability(proof))
    complete, reason = _complete_expectancy_campaign(campaign, normalized_bot="FUTURES", expected_is_sim=1)
    if reason or complete is None:
        raise FundingCorpusUnavailable(reason or "incomplete_funding_campaign")
    if not is_strategy_exit(complete["terminal_reason"]):
        raise FundingCorpusUnavailable("non_strategy_funding_exit")
    if _stamp(fact["candidate_time"]) > complete["closed_at"]:
        raise FundingCorpusUnavailable("candidate_time_after_campaign_close")
    symbol = fact["context"]["venue_symbol"]
    if source_set_id is None:
        source_set_id = _source_for_campaign(connection, symbol=symbol,
            opened_ms=capsule["fill_time_ms"], closed_ms=complete["closed_at"].timestamp() * 1000)
    cache = source_cache if source_cache is not None else {}
    if source_set_id not in cache:
        if len(cache) >= MAX_CACHED_SOURCES:
            raise ValueError("funding_generation_source_cache_budget_exceeded")
        sizes = [item["bytes"] for _proof, _provider, manifest in cache.values()
                 for item in manifest["sources"]]
        declared = _one(connection, """SELECT CASE WHEN length(CAST(manifest_json AS BLOB))<=32768
            THEN manifest_json END AS manifest_json FROM funding_forward_source_sets WHERE source_set_id=?""",
            (source_set_id,))
        if declared is None or declared["manifest_json"] is None:
            raise ValueError("missing_forward_source_set")
        manifest = _decode(declared["manifest_json"])
        if type(manifest) is not dict or type(manifest.get("sources")) is not list:
            raise ValueError("invalid_forward_source_manifest")
        for item in manifest["sources"]:
            if type(item) is not dict or type(item.get("bytes")) is not int or item["bytes"] <= 0:
                raise ValueError("invalid_forward_source_manifest")
            sizes.append(item["bytes"])
        if sum(sizes) > MAX_CACHED_SOURCE_BYTES:
            raise ValueError("funding_generation_source_cache_byte_budget_exceeded")
        cache[source_set_id] = read_registered_funding_sources(connection, source_set_id)
    source_proof, provider, _manifest = cache[source_set_id]
    event = {"entry_id": entry_id, "symbol": fact["context"]["symbol"],
             "candidate_time": _stamp(fact["candidate_time"]), "features": fact["features"]}
    try:
        result = diagnose_campaign(connection, provider, campaign, event)
    except ValueError as exc:
        # Eligibility/model limitations only; cryptographic/SQLite checks above
        # remain hard errors and must never turn into a partial training set.
        if str(exc) not in _MODEL_ELIGIBILITY_VETOES:
            raise
        raise FundingCorpusUnavailable(str(exc)) from exc
    times.extend((_availability(source_proof), complete["closed_at"].timestamp() * 1000))
    base = {"schema_version": 1, "kind": "funding_forward_generation", "policy": policy,
            "entry_id": entry_id, "source_set_id": source_set_id,
            "source_available_by_ms": _availability(source_proof),
            "feature_fact": fact, "feature_receipt": feature["receipt"],
            "feature_binding": binding["binding"], "entry_facts_sha256": sim_availability_facts_digest(entry_facts),
            "entry_available_by_ms": _availability(entry_proof), "fragments": fragments,
            "candidate_time": fact["candidate_time"], "actual_closed_time": complete["closed_at"].isoformat(),
            "decision_lower_ms": capsule["prefill"]["decision_lower_ms"],
            "decision_upper_ms": capsule["prefill"]["decision_upper_ms"],
            "invested_usdt": complete["invested"], "net_return_bps": result.net_profit / complete["invested"] * 10_000,
            "diagnostic": {name: getattr(result, name) for name in result.__dataclass_fields__},
            "minimum_available_by_ms": max(times)}
    if not math.isfinite(base["net_return_bps"]):
        raise ValueError("invalid_funding_model_return")
    return base


def _header(connection, *, generation_id=None, policy_id=None, entry_id=None):
    if generation_id is not None:
        condition, parameters = "generation_id=?", (generation_id,)
    else:
        condition, parameters = "policy_id=? AND entry_id=?", (policy_id, entry_id)
    return _one(connection, f"""SELECT generation_ordinal,generation_id,inputs_sha256,
        policy_id,entry_id,source_set_id,facts_sha256,state,available_by_ms,witness_kind,
        clock_bound_kind,retirement_reason,CASE WHEN length(CAST(generation_json AS BLOB))<=?
        THEN generation_json END AS generation_json
        FROM funding_forward_generations WHERE {condition}
        ORDER BY generation_ordinal LIMIT 1""", (MAX_GENERATION_BYTES, *parameters))


def _generation_body(header, policy):
    if header["generation_json"] is None:
        raise ValueError("funding_generation_byte_budget_exceeded")
    body = _decode(header["generation_json"])
    if (type(body) is not dict or type(body.get("generation_ordinal")) is not int
            or body["generation_ordinal"] <= 0
            or body["generation_ordinal"] != header["generation_ordinal"]
            or _sha(_canonical(body)) != header["generation_id"]
            or header["facts_sha256"] != header["generation_id"]
            or body.get("policy") != policy or header["policy_id"] != policy["policy_id"]
            or body.get("entry_id") != header["entry_id"]
            or body.get("source_set_id") != header["source_set_id"]):
        raise ValueError("funding_generation_facts_changed")
    base = {key: value for key, value in body.items() if key != "generation_ordinal"}
    if _sha(_canonical(base)) != header["inputs_sha256"]:
        raise ValueError("funding_generation_input_binding_changed")
    return body


def _verify_generation(connection, header, policy, *, source_cache=None):
    body = _generation_body(header, policy)
    if header["state"] == "RETIRED":
        _generation_status(header, body)
    try:
        base = _collect_inputs(connection, body["entry_id"], policy,
                               source_set_id=body["source_set_id"], source_cache=source_cache)
    except FundingCorpusUnavailable as exc:
        raise ValueError("funding_generation_original_evidence_changed") from exc
    if _canonical(base) != _canonical({key: value for key, value in body.items()
                                     if key != "generation_ordinal"}):
        raise ValueError("funding_generation_original_evidence_changed")
    return body


def _generation_status(header, body, *, cutoff_ms=None):
    if header["state"] == "RETIRED":
        if header["retirement_reason"] != "journal_tca_retention":
            raise ValueError("invalid_funding_generation_retirement")
        original = {**header, "retirement_reason": None}
        if all(header[name] is None for name in
               ("available_by_ms", "witness_kind", "clock_bound_kind")):
            original["state"] = "PENDING"
        else:
            original["state"] = "ACKED"
        try:
            _generation_status(original, body)
        except FundingCorpusUnavailable as exc:
            if str(exc) != "unproven_funding_generation":
                raise
        raise FundingCorpusUnavailable("retired_funding_generation")
    if header["retirement_reason"] is not None:
        raise ValueError("invalid_funding_generation_retirement")
    if header["state"] not in {"PENDING", "ACKED"}:
        raise ValueError("invalid_funding_generation_proof")
    if header["state"] == "PENDING":
        if any(header[name] is not None for name in
               ("available_by_ms", "witness_kind", "clock_bound_kind")):
            raise ValueError("invalid_funding_generation_proof")
        raise FundingCorpusUnavailable("unproven_funding_generation")
    if (header["state"] != "ACKED" or header["clock_bound_kind"] != CLOCK_KIND
            or header["witness_kind"] not in {"commit_ack", "recovery_read"}):
        raise ValueError("invalid_funding_generation_proof")
    stamp = _availability(header)
    if stamp < body["minimum_available_by_ms"]:
        raise ValueError("funding_generation_availability_precedes_inputs")
    if cutoff_ms is not None and stamp > cutoff_ms:
        raise FundingCorpusUnavailable("late_funding_generation")
    return stamp


def _ack_generation(connection, generation_id, policy, *, recovery=False):
    from core.clock import availability_upper_bound_ms

    if connection.in_transaction:
        raise ValueError("funding_generation_ack_requires_idle_connection")
    connection.execute("BEGIN")
    try:
        header = _header(connection, generation_id=generation_id)
        if header is None:
            raise ValueError("missing_funding_generation")
        body = _verify_generation(connection, header, policy)
        if header["state"] == "ACKED":
            _generation_status(header, body)
            _rollback_or_close(connection)
            return True
        if (header["state"] != "PENDING" or header["available_by_ms"] is not None
                or header["witness_kind"] is not None or header["clock_bound_kind"] is not None
                or header["retirement_reason"] is not None):
            raise ValueError("conflicting_funding_generation_proof")
    except BaseException as primary:
        _rollback_or_close(connection, primary)
        raise
    _rollback_or_close(connection)
    stamp = availability_upper_bound_ms()
    if type(stamp) is not int or stamp <= 0:
        return False
    available = max(stamp, math.ceil(body["minimum_available_by_ms"]))
    if current_funding_model_policy() != policy:
        raise ValueError("funding_model_code_changed_during_generation")
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute("""UPDATE funding_forward_generations SET
            state='ACKED',available_by_ms=?,witness_kind=?,clock_bound_kind=?
            WHERE generation_id=? AND facts_sha256=? AND generation_json=?
            AND inputs_sha256=? AND policy_id=? AND entry_id=? AND source_set_id=?
            AND generation_ordinal=? AND state='PENDING' AND available_by_ms IS NULL
            AND witness_kind IS NULL AND clock_bound_kind IS NULL AND retirement_reason IS NULL""",
            (available, "recovery_read" if recovery else "commit_ack", CLOCK_KIND,
             generation_id, generation_id, header["generation_json"], header["inputs_sha256"],
             policy["policy_id"], body["entry_id"], body["source_set_id"], body["generation_ordinal"]))
        connection.commit()
        if changed.rowcount != 1:
            raise ValueError("funding_generation_ack_cas_conflict")
        return True
    except BaseException as primary:
        _rollback_or_close(connection, primary)
        raise


def seal_funding_model_generation(connection, *, entry_id):
    """Compute or recover the first immutable generation; never redate reads."""
    if connection.in_transaction:
        raise ValueError("funding_generation_seal_requires_idle_connection")
    policy = current_funding_model_policy()
    connection.execute("BEGIN")
    try:
        existing = _header(connection, policy_id=policy["policy_id"], entry_id=entry_id)
        if existing is not None:
            _verify_generation(connection, existing, policy)
            base = None
        else:
            base = _collect_inputs(connection, entry_id, policy)
    except BaseException as primary:
        _rollback_or_close(connection, primary)
        raise
    _rollback_or_close(connection)
    if current_funding_model_policy() != policy:
        raise ValueError("funding_model_code_changed_during_generation")
    if existing is None:
        inputs_sha = _sha(_canonical(base))
        connection.execute("BEGIN IMMEDIATE")
        try:
            # Serialize first-generation allocation. A concurrent sealer may
            # already have committed another first generation for this entry.
            existing = _header(connection, policy_id=policy["policy_id"], entry_id=entry_id)
            if existing is None:
                cursor = connection.execute("""INSERT INTO funding_forward_generations
                    (generation_id,inputs_sha256,policy_id,entry_id,source_set_id,generation_json,facts_sha256)
                    VALUES (?,?,?,?,?,'{}','')""", ("allocating:" + inputs_sha, inputs_sha,
                    policy["policy_id"], entry_id, base["source_set_id"]))
                ordinal = cursor.lastrowid
                if type(ordinal) is not int or ordinal <= 0:
                    raise ValueError("invalid_funding_generation_ordinal")
                body = {**base, "generation_ordinal": ordinal}
                raw = _canonical(body)
                if len(raw) > MAX_GENERATION_BYTES:
                    raise ValueError("funding_generation_byte_budget_exceeded")
                identity = _sha(raw)
                connection.execute("""UPDATE funding_forward_generations SET
                    generation_id=?,generation_json=?,facts_sha256=? WHERE generation_ordinal=?""",
                    (identity, raw.decode(), identity, ordinal))
            else:
                identity = existing["generation_id"]
            connection.commit()
        except BaseException as primary:
            _rollback_or_close(connection, primary)
            raise
    else:
        identity = existing["generation_id"]
    _ack_generation(connection, identity, policy, recovery=existing is not None)
    header = _header(connection, generation_id=identity)
    return {"entry_id": entry_id, "generation_id": identity, "policy_id": policy["policy_id"],
            "state": header["state"], "available_by_ms": header["available_by_ms"],
            "training_eligible": False, "account_payment_verified": False, "promotion_eligible": False}


def recover_funding_forward_generations(connection, *, limit=25):
    if connection.in_transaction or type(limit) is not int or not 1 <= limit <= 25:
        raise ValueError("invalid_funding_generation_recovery_request")
    policy = current_funding_model_policy()
    ids = connection.execute("""SELECT generation_id FROM funding_forward_generations
        WHERE policy_id=? AND state='PENDING' ORDER BY generation_ordinal LIMIT ?""",
        (policy["policy_id"], limit)).fetchall()
    return sum(_ack_generation(connection, row[0], policy, recovery=True) for row in ids)


def _current_journal_inventory(connection):
    rows = connection.execute("""SELECT CASE WHEN typeof(trade.entry_id)='text'
            AND length(CAST(trade.entry_id AS BLOB)) BETWEEN 1 AND 200 THEN trade.entry_id END,
            EXISTS (SELECT 1 FROM expectancy_candidates candidate WHERE candidate.entry_id=trade.entry_id
                AND candidate.bot_name='FUTURES' AND candidate.mode='SIM')
        FROM trades trade WHERE trade.bot_name IN ('FUTURES (SIM)','FUTURES')
            AND trade.is_sim=1 AND trade.is_futures=1 AND trade.is_partial=0
        ORDER BY trade.id LIMIT ?""",
        (MAX_GENERATIONS + 1,)).fetchall()
    if len(rows) > MAX_GENERATIONS:
        raise ValueError("funding_journal_inventory_row_budget_exceeded")
    identified, linked, invalid = set(), set(), 0
    for identity, candidate_linked in rows:
        if type(identity) is not str or not identity.strip() or identity != identity.strip():
            invalid += 1
            continue
        identified.add(identity)
        if candidate_linked:
            linked.add(identity)
    return identified, {"terminal_trade_rows": len(rows),
                        "terminal_rows_missing_or_invalid_entry_identity": invalid,
                        "closed_campaigns_missing_candidate": len(identified - linked)}


def read_funding_forward_labels(connection, *, policy_id, as_of, include_consumed_regime=False):
    from trading.expectancy_training import GroupedCandidateLabel

    if type(include_consumed_regime) is not bool:
        raise ValueError("include_consumed_regime_requires_bool")
    if not connection.in_transaction:
        raise ValueError("funding_forward_labels_require_read_snapshot")
    if not isinstance(as_of, datetime) or as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("funding_forward_as_of_requires_aware_datetime")
    cutoff = _stamp(as_of).timestamp() * 1000
    policy = current_funding_model_policy()
    if _identity(policy_id) != policy["policy_id"]:
        raise ValueError("funding_forward_policy_does_not_match_current_code")
    ids = connection.execute("""SELECT generation_id,entry_id FROM funding_forward_generations
        WHERE policy_id=? ORDER BY generation_ordinal LIMIT ?""", (policy_id, MAX_GENERATIONS + 1)).fetchall()
    if len(ids) > MAX_GENERATIONS:
        raise ValueError("funding_generation_row_budget_exceeded")
    inventory, inventory_exclusions = _current_journal_inventory(connection)
    seen, cache, labels, selected, rejected = set(), {}, [], [], Counter()
    contexts, context_bytes, unknown_contexts = [], 0, 0
    for identity, entry_id in ids:
        if entry_id in seen:
            continue
        seen.add(entry_id)  # First ORIGINAL row is reserved, even if unproven.
        header = _header(connection, generation_id=identity)
        try:
            body = _generation_body(header, policy)
            available = _generation_status(header, body, cutoff_ms=cutoff)
            body = _verify_generation(connection, header, policy, source_cache=cache)
        except FundingCorpusUnavailable as exc:
            rejected[str(exc)] += 1
            continue
        candidate_time = _stamp(body["candidate_time"])
        labels.append(GroupedCandidateLabel(
            candidate_time=_stamp(body["candidate_time"]), label_closed_time=_stamp(body["actual_closed_time"]),
            features=body["feature_fact"]["features"], net_return_bps=body["net_return_bps"],
            label_available_time=datetime.fromtimestamp(available / 1000, timezone.utc),
            decision_lower_time=datetime.fromtimestamp(body["decision_lower_ms"] / 1000, timezone.utc),
            entry_id=body["entry_id"], cluster_id="utc-day:" + candidate_time.strftime("%Y-%m-%d")))
        if include_consumed_regime:
            from trading.candidate_feature_evidence import feature_fact_sha256, normalize_feature_context

            context = normalize_feature_context(body["feature_fact"]["context"])
            observed = context["schema_version"] == 2
            projected = {"entry_id": entry_id, "generation_id": identity,
                         "feature_fact_sha256": feature_fact_sha256(body["feature_fact"]),
                         "feature_context": context,
                         "state": "OBSERVED_RUNTIME_GATE_OPERANDS" if observed else "UNKNOWN_LEGACY_CONTEXT"}
            context_bytes += len(_canonical(projected)) + 1
            if context_bytes > MAX_CONSUMED_REGIME_PROJECTION_BYTES:
                raise ValueError("consumed_regime_projection_byte_budget_exceeded")
            contexts.append(projected)
            unknown_contexts += not observed
        selected.append({"generation_id": identity, "entry_id": entry_id,
                         "source_set_id": body["source_set_id"], "available_by_ms": available})
    if current_funding_model_policy() != policy:
        raise ValueError("funding_model_code_changed_during_read")
    dataset = {"policy_id": policy_id, "as_of": _stamp(as_of).isoformat(), "generations": selected}
    metadata = {
        "schema_version": 1, "label_basis": policy["basis"], "policy_id": policy_id,
        "dataset_sha256": _sha(_canonical(dataset)), "selected_generations": selected,
        "source_set_ids": sorted({row["source_set_id"] for row in selected}),
        "first_generation_entries": len(seen), "qualified_labels": len(labels),
        "journal_coverage": {"basis": "current_inventory_not_historical_as_of",
                             **inventory_exclusions,
                             "closed_entries": len(inventory),
                             "closed_entries_without_generation": len(inventory - seen),
                             "closed_entries_with_generation": len(inventory & seen)},
        "rejections": dict(sorted(rejected.items())), "account_payment_verified": False,
        "historical_as_of_reconstructible": False,
        "retention_policy": "first_ordinal_tombstone_no_full_input_archive_v1",
        "independent_holdout_verified": False, "promotion_eligible": False,
        "selection_policy": policy["generation_selection"],
        "grouping_policy": policy["training_grouping_policy"]}
    if include_consumed_regime:
        metadata["consumed_regime_projection"] = {
            "schema_version": 1, "basis": "original_verified_feature_context_v1",
            "entries": contexts, "unknown_entries": unknown_contexts,
            "historical_regime_publication_verified": False, "promotion_eligible": False}
        if len(_canonical(metadata["consumed_regime_projection"])) > MAX_CONSUMED_REGIME_PROJECTION_BYTES:
            raise ValueError("consumed_regime_projection_byte_budget_exceeded")
    return sorted(labels, key=lambda row: row.candidate_time), metadata
