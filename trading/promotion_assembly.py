"""Sealed, provenance-bound assembly of shared promotion evidence."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import fields as dataclass_fields
from typing import Any

from trading.promotion_gate import PromotionEvidence

FRAGMENT_SCHEMA = 1
PROMOTION_EVIDENCE_SCHEMA = 2

PROMOTION_FRAGMENT_FIELDS: dict[str, tuple[str, ...]] = {
    "optimizer": (
        "dsr",
        "pbo",
        "tail_risk_passed",
        "parameter_stability_passed",
        "multiple_testing_adjusted",
        "experiment_trials",
        "monte_carlo_passed",
        "monte_carlo_runs",
        "monte_carlo_positive_share",
        "parameter_perturbation_count",
        "parameter_sensitivity_avg_change_pct",
        "parameter_sensitivity_max_change_pct",
        "oos_outlier_position_count",
        "oos_net_after_best_position_removed",
        "stressed_oos_net_after_best_position_removed",
    ),
    "validation": (
        "causality_passed",
        "coverage",
        "oos_net",
        "profit_factor",
        "confidence_lower_bound",
        "max_symbol_profit_share",
        "cost_stress_passed",
        "sample_count",
        "regime_stability_passed",
        "walk_forward_passed",
        "walk_forward_positive_folds",
        "stressed_walk_forward_positive_folds",
        "walk_forward_total_folds",
        "final_holdout_net_after_cost",
        "stressed_final_holdout_net_after_cost",
        "oos_max_drawdown_pct",
        "stressed_oos_max_drawdown_pct",
        "independent_position_count",
        "observed_regime_count",
        "max_regime_profit_share",
        "oos_liquidation_count",
        "stressed_oos_liquidation_count",
    ),
    "forward_shadow": (
        "forward_shadow_days",
        "calibration_brier",
        "calibration_ece",
        "calibration_slope",
        "capacity_passed",
        "forward_shadow_net_after_cost",
        "forward_shadow_position_count",
        "forward_shadow_cost_stress_passed",
        "capacity_sample_count",
        "maximum_capacity_participation_rate",
    ),
}

_FRAGMENT_KEYS = {
    "fragment_schema",
    "source_type",
    "strategy",
    "candidate_fingerprint",
    "source_run_id",
    "dataset_fingerprint",
    "fields",
    "fragment_sha256",
}
_ENVELOPE_KEYS = {
    "evidence",
    "minimum_samples",
    "manual_live_approval",
    "decision_input_schema",
    "decision_input_sha256",
    "source_fragments",
    "source_bundle_sha256",
}
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_STRATEGY_RE = re.compile(r"[A-Z][A-Z0-9_]{0,31}")


def _normalize_json(value: Any) -> Any:
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("promotion evidence must contain finite numbers")
        return value
    if type(value) is list:
        return [_normalize_json(item) for item in value]
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ValueError("promotion evidence keys must be strings")
        normalized = {}
        for key in sorted(value):
            normalized[key] = _normalize_json(value[key])
        return normalized
    raise ValueError(
        f"unsupported promotion evidence type: {type(value).__name__}"
    )


def _canonical_sha256(value: Any) -> str:
    normalized = _normalize_json(value)
    raw = json.dumps(
        normalized,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _require_hash(value: Any, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a canonical SHA-256")
    return value


def _require_strategy(value: Any) -> str:
    if type(value) is not str or _STRATEGY_RE.fullmatch(value) is None:
        raise ValueError("strategy must be canonical")
    return value


def _validate_evidence_value(name: str, value: Any) -> Any:
    dataclass_field = next(
        field for field in dataclass_fields(PromotionEvidence) if field.name == name
    )
    annotation = dataclass_field.type
    annotation_text = str(annotation)
    optional = "None" in annotation_text
    if value is None:
        if optional:
            return None
        raise ValueError(f"{name} must not be null")
    if annotation is bool or "bool" in annotation_text:
        if type(value) is not bool:
            raise ValueError(f"{name} must be boolean")
        return value
    if annotation is int or "int" in annotation_text:
        if type(value) is not int:
            raise ValueError(f"{name} must be an integer")
        return value
    if "float" in annotation_text:
        if type(value) not in (int, float) or not math.isfinite(float(value)):
            raise ValueError(f"{name} must be a finite number")
        return value
    raise ValueError(f"unsupported promotion evidence field type: {name}")


def seal_promotion_fragment(
    *,
    source_type: str,
    strategy: str,
    candidate_fingerprint: str,
    source_run_id: str,
    dataset_fingerprint: str,
    fields: dict,
) -> dict:
    """Seal one complete source-owned fragment without evaluating its metrics."""
    if type(source_type) is not str or source_type not in PROMOTION_FRAGMENT_FIELDS:
        raise ValueError("promotion source type is invalid")
    if type(fields) is not dict:
        raise ValueError("promotion fragment fields must be an object")
    expected = set(PROMOTION_FRAGMENT_FIELDS[source_type])
    if set(fields) != expected:
        raise ValueError(f"{source_type} fragment violates fixed field ownership")
    body = {
        "fragment_schema": FRAGMENT_SCHEMA,
        "source_type": source_type,
        "strategy": _require_strategy(strategy),
        "candidate_fingerprint": _require_hash(
            candidate_fingerprint, "candidate fingerprint"
        ),
        "source_run_id": _require_hash(source_run_id, "source run id"),
        "dataset_fingerprint": _require_hash(
            dataset_fingerprint, "dataset fingerprint"
        ),
        "fields": {
            key: _validate_evidence_value(key, fields[key])
            for key in sorted(fields)
        },
    }
    return {**body, "fragment_sha256": _canonical_sha256(body)}


def verify_promotion_fragment(fragment: Any) -> dict:
    if type(fragment) is not dict or set(fragment) != _FRAGMENT_KEYS:
        raise ValueError("promotion fragment structure is invalid")
    if (
        type(fragment.get("fragment_schema")) is not int
        or fragment.get("fragment_schema") != FRAGMENT_SCHEMA
    ):
        raise ValueError("promotion fragment schema is invalid")
    sealed = seal_promotion_fragment(
        source_type=fragment.get("source_type"),
        strategy=fragment.get("strategy"),
        candidate_fingerprint=fragment.get("candidate_fingerprint"),
        source_run_id=fragment.get("source_run_id"),
        dataset_fingerprint=fragment.get("dataset_fingerprint"),
        fields=fragment.get("fields"),
    )
    if fragment.get("fragment_sha256") != sealed["fragment_sha256"]:
        raise ValueError("promotion fragment fingerprint does not match")
    return sealed


def _validate_controls(
    minimum_samples: Any, manual_live_approval: Any
) -> tuple[int, bool]:
    if (
        type(minimum_samples) is not int
        or minimum_samples < 1
        or type(manual_live_approval) is not bool
    ):
        raise ValueError("promotion controls are invalid")
    return minimum_samples, manual_live_approval


def assemble_promotion_envelope(
    fragments: Sequence[Mapping[str, Any]],
    *,
    minimum_samples: int,
    manual_live_approval: bool,
) -> dict:
    """Build one complete deterministic envelope from the three source reports."""
    minimum_samples, manual_live_approval = _validate_controls(
        minimum_samples, manual_live_approval
    )
    identity_fields = {
        "promotion_evidence_schema",
        "optimizer_strategy",
        "optimizer_run_id",
        "dataset_fingerprint",
        "optimizer_candidate_fingerprint",
        "promotion_source_bundle_fingerprint",
    }
    owned_fields = {
        field
        for fragment_fields in PROMOTION_FRAGMENT_FIELDS.values()
        for field in fragment_fields
    }
    expected_fields = {
        field.name for field in dataclass_fields(PromotionEvidence)
    } - identity_fields
    if owned_fields != expected_fields:
        raise ValueError("promotion fragment field ownership schema is incomplete")
    if type(fragments) not in (list, tuple):
        raise ValueError("promotion fragments must be a sequence")
    verified = [verify_promotion_fragment(fragment) for fragment in fragments]
    by_source = {fragment["source_type"]: fragment for fragment in verified}
    if len(verified) != len(PROMOTION_FRAGMENT_FIELDS) or set(by_source) != set(
        PROMOTION_FRAGMENT_FIELDS
    ):
        raise ValueError("promotion source quorum must contain each source exactly once")

    ordered = [by_source[source] for source in PROMOTION_FRAGMENT_FIELDS]
    strategies = {fragment["strategy"] for fragment in ordered}
    candidates = {fragment["candidate_fingerprint"] for fragment in ordered}
    if len(strategies) != 1:
        raise ValueError("promotion source strategies conflict")
    if len(candidates) != 1:
        raise ValueError("promotion source candidates conflict")

    source_bundle_sha256 = _canonical_sha256(ordered)
    optimizer = by_source["optimizer"]
    evidence = {}
    for fragment in ordered:
        evidence.update(fragment["fields"])
    evidence.update(
        {
            "promotion_evidence_schema": PROMOTION_EVIDENCE_SCHEMA,
            "optimizer_strategy": optimizer["strategy"],
            "optimizer_run_id": optimizer["source_run_id"],
            "dataset_fingerprint": optimizer["dataset_fingerprint"],
            "optimizer_candidate_fingerprint": optimizer[
                "candidate_fingerprint"
            ],
            "promotion_source_bundle_fingerprint": source_bundle_sha256,
        }
    )

    from trading.profit_research_runner import check_promotion

    decision = check_promotion(
        evidence,
        minimum_samples=minimum_samples,
        manual_live_approval=manual_live_approval,
    )
    return {
        "evidence": evidence,
        "minimum_samples": minimum_samples,
        "manual_live_approval": manual_live_approval,
        "decision_input_schema": decision["decision_input_schema"],
        "decision_input_sha256": decision["decision_input_sha256"],
        "source_fragments": ordered,
        "source_bundle_sha256": source_bundle_sha256,
    }


def verify_promotion_envelope(envelope: Any) -> dict:
    """Rebuild an envelope from its sealed sources and reject every mismatch."""
    if type(envelope) is not dict or set(envelope) != _ENVELOPE_KEYS:
        raise ValueError("promotion envelope structure is invalid")
    rebuilt = assemble_promotion_envelope(
        envelope.get("source_fragments"),
        minimum_samples=envelope.get("minimum_samples"),
        manual_live_approval=envelope.get("manual_live_approval"),
    )
    if _canonical_sha256(envelope) != _canonical_sha256(rebuilt):
        raise ValueError("promotion envelope does not match its sealed sources")
    return rebuilt
