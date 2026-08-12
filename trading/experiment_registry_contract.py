"""Shared data contracts for immutable experiment-registry writes."""
from __future__ import annotations

import json


EXPERIMENT_STATUSES = frozenset(
    {"PLANNED", "RUNNING", "REJECTED", "COMPLETE"}
)


def normalize_experiment_metadata(
    trial_id: str,
    experiment_name: str,
    status: str,
) -> tuple[str, str, str]:
    if not isinstance(trial_id, str) or not trial_id.strip():
        raise ValueError("trial id must be a non-empty string")
    if not isinstance(experiment_name, str) or not experiment_name.strip():
        raise ValueError("experiment name must be a non-empty string")
    if not isinstance(status, str):
        raise ValueError("unsupported experiment status")
    normalized_status = status.strip().upper()
    if normalized_status not in EXPERIMENT_STATUSES:
        raise ValueError("unsupported experiment status")
    return trial_id.strip(), experiment_name.strip(), normalized_status


def encode_experiment_params(params: dict) -> str:
    if not isinstance(params, dict):
        raise ValueError("experiment params must be a JSON object")
    pending = [params]
    visited: set[int] = set()
    while pending:
        value = pending.pop()
        if value is None or isinstance(value, (str, bool, int, float)):
            continue
        if not isinstance(value, (dict, list)):
            raise ValueError("experiment params must contain only JSON values")
        identity = id(value)
        if identity in visited:
            continue
        visited.add(identity)
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise ValueError("experiment params object keys must be strings")
            pending.extend(value.values())
        else:
            pending.extend(value)
    try:
        return json.dumps(params, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("experiment params must be finite JSON") from exc
