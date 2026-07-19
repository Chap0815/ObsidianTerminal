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
    expected = EXPECTANCY_FEATURES.get(normalized_bot)
    if not expected or not str(entry_id).strip() or not str(symbol).strip():
        return False
    normalized_features = {}
    for name in expected:
        value = _finite((features or {}).get(name))
        if value is None:
            return False
        normalized_features[name] = value
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
            schema_version=1,
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
            schema_version=1,
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
