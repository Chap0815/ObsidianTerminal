"""Dependency-light causal feature logging for later expectancy research."""
from __future__ import annotations

import math
import os
import threading
import time


EXPECTANCY_FEATURES: dict[str, tuple[str, ...]] = {
    "CROSS": ("score", "spread_bps", "side_sign", "target_side_count"),
    "FUTURES": (
        "score", "spread_bps", "funding_rate_pct", "oi_change_pct",
        "change_pct", "btc_change_pct",
    ),
    "FUTREND": (
        "score", "spread_bps", "funding_rate_pct", "trend_votes",
        "realized_vol",
    ),
    "SPOT": (
        "score", "confidence", "change_pct", "rsi_15m", "rsi_1h", "rsi_4h",
    ),
    "TREND": ("trend_votes", "realized_vol", "size_multiplier"),
}

# Version 1 remains the runtime-compatible baseline.  New schemas are additive
# and research tooling must never mix rows from different versions.
EXPECTANCY_FEATURE_SCHEMAS: dict[str, dict[int, tuple[str, ...]]] = {
    bot: {1: features} for bot, features in EXPECTANCY_FEATURES.items()
}
EXPECTANCY_FEATURE_SCHEMAS["CROSS"][2] = (
    *EXPECTANCY_FEATURES["CROSS"],
    "rank_position",
    "rank_count",
    "return_pct",
    "funding_rate_pct",
    "universe_count",
    "universe_median_return_pct",
    "universe_dispersion_pct",
    "market_breadth_positive_pct",
    "long_short_separation_pct",
    "separation_to_dispersion",
    "btc_return_pct",
    "btc_realized_vol_24h_pct",
    "average_pairwise_correlation",
    "correlation_pair_count",
    "liquidity_max_symbol_share",
    "expected_funding_carry_8h_pct",
    "funding_coverage",
)

_RETENTION_LOCK = threading.Lock()
_NEXT_RETENTION_CHECK = 0.0
_RETENTION_RUNNING = False
_RETENTION_INTERVAL_SECONDS = 3600.0
_RETENTION_RETRY_SECONDS = 60.0
_RETENTION_ADVISORY_LOCK_NAME = "research_telemetry_retention"
_RETENTION_ADVISORY_TTL_SECONDS = 900


def _log_telemetry_error(context: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, exc)
    except Exception:
        pass


def _run_retention_cleanup() -> None:
    global _NEXT_RETENTION_CHECK, _RETENTION_RUNNING
    failure = None
    acquired = False
    contended = False
    holder_id = (
        f"retention:{os.getpid()}:{threading.get_native_id()}:"
        f"{time.monotonic_ns()}"
    )
    try:
        from core.database import (
            acquire_advisory_lock,
            enforce_research_telemetry_retention,
            release_advisory_lock,
        )

        acquired = acquire_advisory_lock(
            _RETENTION_ADVISORY_LOCK_NAME,
            holder_id,
            ttl_sec=_RETENTION_ADVISORY_TTL_SECONDS,
        )
        if not acquired:
            contended = True
            return
        enforce_research_telemetry_retention()
    except Exception as exc:
        failure = exc
        _log_telemetry_error("research telemetry retention", exc)
    finally:
        if acquired:
            try:
                if not release_advisory_lock(
                    _RETENTION_ADVISORY_LOCK_NAME, holder_id
                ):
                    raise RuntimeError("retention advisory lock release failed")
            except Exception as exc:
                if failure is None:
                    failure = exc
                _log_telemetry_error(
                    "release research telemetry retention lock", exc
                )
        with _RETENTION_LOCK:
            if failure is not None or contended:
                retry_at = time.monotonic() + _RETENTION_RETRY_SECONDS
                _NEXT_RETENTION_CHECK = min(_NEXT_RETENTION_CHECK, retry_at)
            _RETENTION_RUNNING = False


def _schedule_retention_cleanup(now: float | None = None) -> None:
    global _NEXT_RETENTION_CHECK, _RETENTION_RUNNING
    observed = time.monotonic() if now is None else now
    if observed < _NEXT_RETENTION_CHECK:
        return
    start_error = None
    with _RETENTION_LOCK:
        if observed < _NEXT_RETENTION_CHECK or _RETENTION_RUNNING:
            return
        _RETENTION_RUNNING = True
        _NEXT_RETENTION_CHECK = observed + _RETENTION_INTERVAL_SECONDS
        try:
            threading.Thread(
                target=_run_retention_cleanup,
                name="research-telemetry-retention",
                daemon=True,
            ).start()
        except Exception as exc:
            start_error = exc
            _RETENTION_RUNNING = False
            _NEXT_RETENTION_CHECK = observed + _RETENTION_RETRY_SECONDS
    if start_error is not None:
        _log_telemetry_error("start research telemetry retention", start_error)


def _finite(value) -> float | None:
    if (
        value is None
        or isinstance(value, bool)
        or not isinstance(value, (int, float))
    ):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def expectancy_feature_value(value, *, missing_default: float = 0.0):
    """Default only absent evidence without erasing an invalid source type."""
    return missing_default if value is None or value == "" else value


def expectancy_feature_bps(value):
    """Scale numeric percentages while preserving invalid source evidence."""
    normalized = expectancy_feature_value(value)
    if isinstance(normalized, bool) or not isinstance(normalized, (int, float)):
        return normalized
    return normalized * 100.0


def _quality_decision(value: dict | None) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        return None
    score = _finite(value.get("score"))
    minimum_score = _finite(value.get("minimum_score"))
    label = str(value.get("label") or "").strip().upper()
    reasons = value.get("reasons")
    would_block = value.get("would_block")
    if (
        score is None
        or not 0.0 <= score <= 100.0
        or minimum_score is None
        or not 0.0 <= minimum_score <= 100.0
        or not label
        or len(label) > 32
        or not isinstance(reasons, (list, tuple))
        or len(reasons) > 32
        or not isinstance(would_block, bool)
    ):
        return None
    normalized_reasons = []
    for reason in reasons:
        if not isinstance(reason, str):
            return None
        normalized_reason = reason.strip()
        if not normalized_reason or len(normalized_reason) > 64:
            return None
        if normalized_reason not in normalized_reasons:
            normalized_reasons.append(normalized_reason)
    return {
        "score": score,
        "minimum_score": minimum_score,
        "label": label,
        "reasons": normalized_reasons,
        "would_block": would_block,
    }


def emit_expectancy_candidate(
    *,
    bot: str,
    entry_id: str,
    symbol: str,
    mode: str,
    direction: str,
    features: dict,
    quality_decision: dict | None = None,
    log_struct=None,
    persist_candidate=None,
    venue_symbol: str | None = None,
) -> bool:
    """Persist the exact causal feature vector later joined to a closed trade."""
    normalized_bot = str(bot).strip().upper()
    normalized_direction = str(direction).strip().upper()
    schemas = EXPECTANCY_FEATURE_SCHEMAS.get(normalized_bot)
    if (
        not schemas
        or not str(entry_id).strip()
        or not str(symbol).strip()
        or normalized_direction not in {"LONG", "SHORT"}
    ):
        return False
    schema_version = None
    normalized_features = None
    for version in sorted(schemas, reverse=True):
        candidate = {}
        for name in schemas[version]:
            value = _finite((features or {}).get(name))
            if value is None:
                break
            candidate[name] = value
        if len(candidate) == len(schemas[version]):
            schema_version = version
            normalized_features = candidate
            break
    if schema_version is None or normalized_features is None:
        return False
    feature_score = normalized_features.get("score")
    if feature_score is not None and not 0.0 <= feature_score <= 100.0:
        return False
    normalized_decision = _quality_decision(quality_decision)
    if quality_decision is not None and normalized_decision is None:
        return False
    if (
        normalized_decision is not None
        and normalized_decision["score"] != normalized_features.get("score")
    ):
        return False
    if normalized_bot == "CROSS":
        side_sign = normalized_features.get("side_sign")
        expected_sign = 1.0 if normalized_direction == "LONG" else -1.0
        if side_sign is None or side_sign == 0.0 or side_sign * expected_sign <= 0.0:
            return False
    from core.clock import utc_now_str

    candidate_time = utc_now_str()
    snapshot_payload = {
        **normalized_features,
        "direction": normalized_direction,
        "sequence_valid": False,
        "sequence_status": "not_applicable_strategy_features",
        "queue_position_claimed": False,
    }
    persisted = False
    default_persistence = persist_candidate is None
    persistence_error = None
    try:
        if default_persistence:
            from core.database import save_expectancy_candidate as persister
        else:
            persister = persist_candidate
        persistence_fields = dict(
            entry_id=str(entry_id),
            bot_name=normalized_bot,
            symbol=str(symbol),
            mode=str(mode).strip().upper(),
            candidate_time=candidate_time,
            schema_version=schema_version,
            features=normalized_features,
            direction=normalized_direction,
        )
        if default_persistence:
            persistence_fields["feature_snapshot"] = snapshot_payload
        if normalized_decision is not None:
            persistence_fields["quality_decision"] = normalized_decision
        persisted = bool(persister(**persistence_fields))
    except Exception as exc:
        persistence_error = exc
        persisted = False
    if default_persistence and not persisted:
        _log_telemetry_error(
            "persist expectancy candidate",
            persistence_error
            or RuntimeError("candidate persistence returned false"),
        )
    if persisted and default_persistence:
        try:
            from core.database import request_venue_capture_priority

            if (
                normalized_bot in {"FUTURES", "CROSS", "FUTREND"}
                and not (
                    normalized_decision is not None
                    and normalized_decision["would_block"]
                )
            ):
                priority_persisted = request_venue_capture_priority(
                    symbol=str(venue_symbol or symbol),
                    bot_name=normalized_bot,
                    mode=str(mode).strip().upper(),
                    reason="expectancy_candidate",
                )
                if not priority_persisted:
                    _log_telemetry_error(
                        "persist candidate venue priority",
                        RuntimeError(
                            "candidate venue priority persistence returned false"
                        ),
                    )
        except Exception as exc:
            _log_telemetry_error("persist candidate enrichment", exc)
        _schedule_retention_cleanup()
    try:
        if log_struct is None:
            from core.logger import log_struct as writer
        else:
            writer = log_struct
        log_fields = dict(
            schema_version=schema_version,
            bot=normalized_bot,
            entry_id=str(entry_id),
            symbol=str(symbol),
            mode=str(mode).strip().upper(),
            direction=normalized_direction,
            candidate_time=candidate_time,
            durable=persisted,
            features=normalized_features,
        )
        if normalized_decision is not None:
            log_fields["quality_decision"] = normalized_decision
        writer("expectancy_candidate", **log_fields)
    except Exception:
        pass
    return persisted
