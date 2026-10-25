"""Pure mechanical signal assessments; runtime policy is always explicit.

These are confidence assessments, not order admission. Legacy FUTURES numeric
coercion intentionally differs from SPOT and is preserved by this extraction.
No environment, configuration, market data, database or account is read here.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import math


@dataclass(frozen=True)
class FuturesSignalPolicy:
    ext_filter: bool = True
    max_ext_pct: float = 18.0
    vol_filter: bool = True
    min_vol_surge: float = 1.0
    rs_filter: bool = True
    min_rs_pct: float = -2.0
    funding_edge: bool = True

    def as_snapshot(self) -> dict:
        return {key: getattr(self, field) for key, field in _POLICY_FIELDS}


@dataclass(frozen=True)
class FuturesRegimePolicy:
    enabled: bool
    bear_7d: float
    bull_7d: float

    def as_snapshot(self) -> dict:
        return {"FUT_REGIME_DIRECTION_GATE": self.enabled,
                "FUT_REGIME_BEAR_7D": self.bear_7d, "FUT_REGIME_BULL_7D": self.bull_7d}


def futures_regime_policy_from_snapshot(snapshot: dict) -> FuturesRegimePolicy:
    keys = {"FUT_REGIME_DIRECTION_GATE", "FUT_REGIME_BEAR_7D", "FUT_REGIME_BULL_7D"}
    if type(snapshot) is not dict or set(snapshot) != keys:
        raise ValueError("regime policy requires exactly three effective fields")
    enabled = snapshot["FUT_REGIME_DIRECTION_GATE"]
    bear, bull = snapshot["FUT_REGIME_BEAR_7D"], snapshot["FUT_REGIME_BULL_7D"]
    if type(enabled) is not bool or type(bear) not in (int, float) or type(bull) not in (int, float):
        raise ValueError("regime policy requires an effective boolean and numeric thresholds")
    try:
        bear, bull = float(bear), float(bull)
    except (ValueError, OverflowError) as exc:
        raise ValueError("invalid regime thresholds") from exc
    if not math.isfinite(bear) or not math.isfinite(bull) or not -100 <= bear <= 0 <= bull <= 100:
        raise ValueError("regime thresholds exceed the existing runtime bounds")
    return FuturesRegimePolicy(enabled, bear, bull)


def parse_futures_regime_policy_snapshot(raw: bytes | str) -> FuturesRegimePolicy:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate regime policy key")
            result[key] = value
        return result

    def invalid(_value):
        raise ValueError("non-finite regime policy value")

    if type(raw) not in (bytes, str) or len(raw) > 8192:
        raise ValueError("regime policy must be bounded JSON")
    if isinstance(raw, str) and len(raw.encode("utf-8")) > 8192:
        raise ValueError("regime policy must be bounded JSON")
    try:
        value = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid regime policy JSON") from exc
    return futures_regime_policy_from_snapshot(value)


def assess_futures_regime_gate(direction, confidence, regime, btc_7d,
                              policy: FuturesRegimePolicy, *, allow_high_countertrend=True):
    """Existing directional gate, including its asymmetric BULL condition.

    A disabled gate does not touch unused operands. Missing historical inputs
    must be enumerated by research callers, not converted to NEUTRAL or zero.
    """
    if not policy.enabled:
        return True, ""
    confidence = str(confidence).upper()
    clear_bear = regime == "BEAR" or btc_7d <= policy.bear_7d
    clear_bull = regime == "BULL" and btc_7d >= policy.bull_7d
    requires_veto = confidence != "HIGH" or not allow_high_countertrend
    if clear_bear and direction == "LONG" and requires_veto:
        return False, "BEAR"
    if clear_bull and direction == "SHORT" and requires_veto:
        return False, "BULL"
    return True, ""


_POLICY_FIELDS = (
    ("FUT_EXT_FILTER", "ext_filter"), ("FUT_MAX_EXT_PCT", "max_ext_pct"),
    ("FUT_VOL_FILTER", "vol_filter"), ("FUT_MIN_VOL_SURGE", "min_vol_surge"),
    ("FUT_RS_FILTER", "rs_filter"), ("FUT_MIN_RS_PCT", "min_rs_pct"),
    ("FUT_FUNDING_EDGE", "funding_edge"),
)


def futures_signal_policy_from_snapshot(snapshot: dict) -> FuturesSignalPolicy:
    """Accept exactly seven effective policy values without reinterpreting them.

    Finite negative thresholds are allowed because the runtime's existing env
    contract permits them. This factory does not invent new strategy bounds.
    """
    if type(snapshot) is not dict or set(snapshot) != {key for key, _ in _POLICY_FIELDS}:
        raise ValueError("signal policy requires exactly seven effective fields")
    values = {}
    for key, field in _POLICY_FIELDS:
        value = snapshot[key]
        if field in {"ext_filter", "vol_filter", "rs_filter", "funding_edge"}:
            if type(value) is not bool:
                raise ValueError("signal toggles must be effective booleans")
        else:
            if type(value) not in (int, float):
                raise ValueError("signal thresholds must be effective finite numbers")
            try:
                value = float(value)
            except (ValueError, OverflowError) as exc:
                raise ValueError("signal threshold is out of range") from exc
            if not math.isfinite(value):
                raise ValueError("signal threshold is out of range")
        values[field] = value
    return FuturesSignalPolicy(**values)


def parse_futures_signal_policy_snapshot(raw: bytes | str) -> FuturesSignalPolicy:
    """Decode one bounded, unambiguous JSON object containing no extra fields."""
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate signal policy key")
            value[key] = item
        return value

    def invalid_constant(_value):
        raise ValueError("nonfinite signal policy number")

    if type(raw) not in (bytes, str):
        raise ValueError("signal policy JSON must be bytes or text")
    try:
        if len(raw) > 8192 or (isinstance(raw, str) and len(raw.encode("utf-8")) > 8192):
            raise ValueError("signal policy JSON exceeds byte limit")
        value = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid_constant)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid signal policy JSON") from exc
    return futures_signal_policy_from_snapshot(value)


def _spot_finite_float(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def assess_spot_entry_signal(row) -> str:
    rsi_values = tuple(_spot_finite_float(row.get(key))
                       for key in ("rsi_15m", "rsi_1h", "rsi_4h"))
    if any(value is None for value in rsi_values):
        return "LOW"
    tf_score = sum(1 for value in rsi_values if 50 <= value <= 75)
    indicators = (
        _spot_finite_float(row.get("vol_surge", 1.0)),
        _spot_finite_float(row.get("macd_hist", 0.0)),
        _spot_finite_float(row.get("body_ratio", 1.0)),
        _spot_finite_float(row.get("change_percent", 0.0)),
    )
    if any(value is None for value in indicators):
        return "LOW"
    vol_surge, macd_hist, body_ratio, change_percent = indicators
    score = (tf_score + int(vol_surge >= 1.15) + int(macd_hist >= 0.0)
             + int(body_ratio >= 0.35) + int(change_percent > 0.0))
    if tf_score >= 2 and score >= 5:
        return "HIGH"
    if tf_score >= 2 and score >= 4:
        return "MEDIUM"
    return "LOW"


def assess_futures_entry_signal(row, direction, funding_rate, btc_chg,
                               policy: FuturesSignalPolicy) -> tuple[bool, str, str]:
    """Preserve runtime coercion, boundary comparisons, confidence and reasons.

    ``funding_rate`` is percentage points from runtime ``get_funding_info``;
    native fractional rates are multiplied by 100 exactly once before this
    assessment. Settlement-cost calculations use the original fractional rate.
    """
    try:
        rsi_15 = float(row.get("rsi_15m", 50))
        rsi_h = float(row.get("rsi_1h", 50))
        rsi_4 = float(row.get("rsi_4h", 50))
    except (TypeError, ValueError):
        rsi_15 = rsi_h = rsi_4 = 50.0
    ema_ratio = float(row.get("ema_ratio", 0.0) or 0.0)
    vol_surge = float(row.get("vol_surge", 1.0) or 1.0)
    chg = float(row.get("change_percent", 0.0) or 0.0)
    is_long = direction == "LONG"
    score = 0
    if is_long:
        score += sum(1 for value in (rsi_15, rsi_h, rsi_4) if 50 <= value <= 75)
    else:
        score += sum(1 for value in (rsi_15, rsi_h, rsi_4) if 25 <= value <= 50)
    if policy.ext_filter:
        max_ext = policy.max_ext_pct
        ext = ema_ratio if is_long else -ema_ratio
        if ext > max_ext:
            return False, "LOW", f"over-extended {ext:+.1f}% vs EMA50 (chasing)"
        if 0.0 <= ext <= max_ext * 0.45:
            score += 1
    if policy.vol_filter:
        min_vs = policy.min_vol_surge
        if vol_surge < min_vs:
            return False, "LOW", f"weak volume (surge {vol_surge:.2f} < {min_vs:g})"
        if vol_surge >= 1.8:
            score += 1
    if policy.rs_filter:
        rs = (chg - btc_chg) if is_long else (btc_chg - chg)
        if rs < policy.min_rs_pct:
            return False, "LOW", f"weak rel-strength vs BTC ({rs:+.1f}%)"
        if rs >= 3.0:
            score += 1
    if policy.funding_edge:
        funding = float(funding_rate or 0.0)
        if abs(funding) >= 0.0001:
            paying = (funding > 0) if is_long else (funding < 0)
            score += -1 if paying else 1
    confidence = "HIGH" if score >= 4 else "MEDIUM" if score >= 2 else "LOW"
    return True, confidence, ""
