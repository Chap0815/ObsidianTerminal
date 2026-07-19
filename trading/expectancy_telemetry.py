"""Dependency-light causal feature logging for later expectancy research."""
from __future__ import annotations

import math
from datetime import datetime, timezone


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


def _finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def emit_expectancy_candidate(
    *,
    bot: str,
    entry_id: str,
    symbol: str,
    mode: str,
    features: dict,
    log_struct=None,
    persist_candidate=None,
) -> bool:
    """Persist the exact causal feature vector later joined to a closed trade."""
    normalized_bot = str(bot).strip().upper()
    schemas = EXPECTANCY_FEATURE_SCHEMAS.get(normalized_bot)
    if not schemas or not str(entry_id).strip() or not str(symbol).strip():
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
    candidate_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    persisted = False
    try:
        if persist_candidate is None:
            from core.database import save_expectancy_candidate as persister
        else:
            persister = persist_candidate
        persisted = bool(persister(
            entry_id=str(entry_id),
            bot_name=normalized_bot,
            symbol=str(symbol),
            mode=str(mode).strip().upper(),
            candidate_time=candidate_time,
            schema_version=schema_version,
            features=normalized_features,
        ))
    except Exception:
        persisted = False
    try:
        if log_struct is None:
            from core.logger import log_struct as writer
        else:
            writer = log_struct
        writer(
            "expectancy_candidate",
            schema_version=schema_version,
            bot=normalized_bot,
            entry_id=str(entry_id),
            symbol=str(symbol),
            mode=str(mode).strip().upper(),
            candidate_time=candidate_time,
            durable=persisted,
            features=normalized_features,
        )
    except Exception:
        pass
    return persisted
