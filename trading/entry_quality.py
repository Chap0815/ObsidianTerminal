"""Entry quality scoring for live gates and closed-trade analysis."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EntryQuality:
    score: int
    label: str
    reasons: tuple[str, ...]
    components: dict[str, float]
    raw_score: float | None = None

    def as_log_fields(self) -> dict[str, Any]:
        fields = {
            "entry_quality_score": self.score,
            "entry_quality_label": self.label,
            "entry_quality_reasons": ",".join(self.reasons),
            "entry_quality_components": self.components,
        }
        if self.raw_score is not None:
            fields["entry_quality_raw_score"] = round(self.raw_score, 4)
            fields["entry_quality_saturated"] = not 0.0 < self.raw_score < 100.0
        return fields


def _finite_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        out = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return out if math.isfinite(out) else None


def _component(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def label_for_score(score: float | int | None) -> str:
    value = _finite_float(score)
    if value is None:
        return "UNKNOWN"
    if value >= 75:
        return "HIGH"
    if value >= 50:
        return "MID"
    return "LOW"


def score_futrend_entry(
    *,
    trend_votes: Any = None,
    spread_pct: Any = None,
    spread_limit_pct: Any = 0.25,
    funding_rate_pct: Any = None,
    funding_limit_pct: Any = 0.05,
    realized_vol: Any = None,
    vol_size_mult: Any = None,
    would_block: Any = False,
) -> EntryQuality:
    """Return a stable 0..100 score for FUTREND candidate telemetry.

    Inputs are deliberately optional and defensive because exchange tickers and
    runtime metadata can be incomplete. Positive funding is adverse for FUTREND
    because it is long-only.
    """
    votes = _finite_float(trend_votes)
    spread = _finite_float(spread_pct)
    spread_limit = _finite_float(spread_limit_pct) or 0.25
    funding = _finite_float(funding_rate_pct)
    funding_limit = _finite_float(funding_limit_pct) or 0.05
    vol = _finite_float(realized_vol)
    size_mult = _finite_float(vol_size_mult)

    components: dict[str, float] = {}
    reasons: list[str] = []

    if votes is None:
        trend_component = -10.0
        reasons.append("votes_missing")
    elif votes >= 3:
        trend_component = 20.0
    elif votes >= 2:
        trend_component = 8.0
        reasons.append("weak_votes")
    else:
        trend_component = -20.0
        reasons.append("low_votes")
    components["trend"] = trend_component

    if spread is None:
        spread_component = -10.0
        reasons.append("spread_missing")
    elif spread <= spread_limit * 0.5:
        spread_component = 10.0
    elif spread <= spread_limit:
        spread_component = 4.0
    else:
        ratio = spread / max(spread_limit, 1e-9)
        spread_component = -_component(10.0 + (ratio - 1.0) * 15.0, 10.0, 35.0)
        reasons.append("wide_spread")
    components["spread"] = spread_component

    if funding is None:
        funding_component = 0.0
    elif funding <= 0:
        funding_component = 5.0
    elif funding <= funding_limit:
        funding_component = -_component((funding / max(funding_limit, 1e-9)) * 8.0, 0.0, 8.0)
    else:
        ratio = funding / max(funding_limit, 1e-9)
        funding_component = -_component(12.0 + (ratio - 1.0) * 12.0, 12.0, 32.0)
        reasons.append("long_pays_funding")
    components["funding"] = funding_component

    if vol is None:
        vol_component = 0.0
    elif vol <= 0.02:
        vol_component = 10.0
    elif vol <= 0.05:
        vol_component = 2.0
    elif vol <= 0.08:
        vol_component = -10.0
        reasons.append("high_vol")
    else:
        vol_component = -25.0
        reasons.append("extreme_vol")
    components["volatility"] = vol_component

    if size_mult is None:
        size_component = 0.0
    elif size_mult < 0.6:
        size_component = -5.0
        reasons.append("low_vol_size_mult")
    elif size_mult > 1.5:
        size_component = 4.0
    else:
        size_component = 0.0
    components["vol_size"] = size_component

    blocked_component = -30.0 if bool(would_block) else 0.0
    if blocked_component:
        reasons.append("would_block")
    components["block_state"] = blocked_component

    raw = 60.0 + sum(components.values())
    score = int(round(_component(raw, 0.0, 100.0)))
    return EntryQuality(
        score=score,
        label=label_for_score(score),
        reasons=tuple(dict.fromkeys(reasons)),
        components={key: round(value, 4) for key, value in components.items()},
        raw_score=raw,
    )


def score_futures_entry(
    *,
    direction: Any = None,
    confidence: Any = None,
    rsi_15m: Any = None,
    rsi_1h: Any = None,
    rsi_4h: Any = None,
    change_pct: Any = None,
    btc_change_pct: Any = None,
    funding_rate_pct: Any = None,
    oi_change_pct: Any = None,
    spread_pct: Any = None,
    regime: Any = None,
) -> EntryQuality:
    """Return a 0..100 entry score for the directional FUTURES bot."""
    side = str(direction or "").upper()
    conf = str(confidence or "").upper()
    r15 = _finite_float(rsi_15m)
    r1h = _finite_float(rsi_1h)
    r4h = _finite_float(rsi_4h)
    chg = _finite_float(change_pct)
    btc = _finite_float(btc_change_pct)
    funding = _finite_float(funding_rate_pct)
    oi_change = _finite_float(oi_change_pct)
    spread = _finite_float(spread_pct)
    reg = str(regime or "").upper()

    components: dict[str, float] = {}
    reasons: list[str] = []

    if conf == "HIGH":
        signal_component = 18.0
    elif conf in {"MEDIUM", "MID"}:
        signal_component = 6.0
    else:
        signal_component = -18.0
        reasons.append("low_confidence")
    components["signal"] = signal_component

    rsis = [v for v in (r15, r1h, r4h) if v is not None]
    if len(rsis) < 3:
        rsi_component = -8.0
        reasons.append("rsi_missing")
    elif side == "LONG":
        aligned = sum(1 for v in rsis if 50.0 <= v <= 75.0)
        extended = sum(1 for v in rsis if v > 78.0)
        rsi_component = aligned * 5.0 - extended * 8.0
        if aligned < 2:
            reasons.append("weak_rsi_alignment")
        if extended:
            reasons.append("overextended_rsi")
    elif side == "SHORT":
        aligned = sum(1 for v in rsis if 25.0 <= v <= 50.0)
        extended = sum(1 for v in rsis if v < 22.0)
        rsi_component = aligned * 5.0 - extended * 8.0
        if aligned < 2:
            reasons.append("weak_rsi_alignment")
        if extended:
            reasons.append("overextended_rsi")
    else:
        rsi_component = -18.0
        reasons.append("direction_missing")
    components["rsi"] = _component(rsi_component, -18.0, 15.0)

    if chg is None or btc is None or side not in {"LONG", "SHORT"}:
        rs_component = -4.0
        reasons.append("relative_strength_missing")
    else:
        rel = (chg - btc) if side == "LONG" else (btc - chg)
        if rel >= 4.0:
            rs_component = 10.0
        elif rel >= 0.0:
            rs_component = 4.0
        elif rel >= -2.0:
            rs_component = -6.0
            reasons.append("weak_relative_strength")
        else:
            rs_component = -16.0
            reasons.append("bad_relative_strength")
    components["relative_strength"] = rs_component

    if funding is None or side not in {"LONG", "SHORT"}:
        funding_component = 0.0
    else:
        paying = (funding > 0.0) if side == "LONG" else (funding < 0.0)
        if abs(funding) < 0.01:
            funding_component = 0.0
        elif paying:
            funding_component = -_component(abs(funding) * 120.0, 4.0, 18.0)
            reasons.append("pays_funding")
        else:
            funding_component = _component(abs(funding) * 80.0, 3.0, 10.0)
    components["funding"] = funding_component

    if spread is None:
        spread_component = 0.0
    elif spread <= 0.10:
        spread_component = 5.0
    elif spread <= 0.30:
        spread_component = 0.0
    else:
        spread_component = -_component((spread - 0.30) * 35.0, 6.0, 24.0)
        reasons.append("wide_spread")
    components["spread"] = spread_component

    if oi_change is None:
        oi_component = 0.0
    elif abs(oi_change) >= 35.0:
        oi_component = -10.0
        reasons.append("oi_extreme")
    elif abs(oi_change) >= 15.0:
        oi_component = -4.0
    else:
        oi_component = 2.0
    components["open_interest"] = oi_component

    if reg == "BEAR" and side == "LONG":
        regime_component = -10.0
        reasons.append("bear_long")
    elif reg == "BULL" and side == "SHORT":
        regime_component = -10.0
        reasons.append("bull_short")
    else:
        regime_component = 0.0
    components["regime"] = regime_component

    raw = 55.0 + sum(components.values())
    score = int(round(_component(raw, 0.0, 100.0)))
    return EntryQuality(
        score=score,
        label=label_for_score(score),
        reasons=tuple(dict.fromkeys(reasons)),
        components={key: round(value, 4) for key, value in components.items()},
        raw_score=raw,
    )


def score_spot_entry(
    *,
    confidence: Any = None,
    rsi_15m: Any = None,
    rsi_1h: Any = None,
    rsi_4h: Any = None,
    change_pct: Any = None,
    btc_change_pct: Any = None,
    regime: Any = None,
    vol_surge: Any = None,
    body_ratio: Any = None,
    macd_hist: Any = None,
    historical_winrate: Any = None,
    spread_pct: Any = None,
) -> EntryQuality:
    """Return a 0..100 entry score for SPOT momentum candidates."""
    conf = str(confidence or "").upper()
    r15 = _finite_float(rsi_15m)
    r1h = _finite_float(rsi_1h)
    r4h = _finite_float(rsi_4h)
    chg = _finite_float(change_pct)
    btc = _finite_float(btc_change_pct)
    reg = str(regime or "").upper()
    vol = _finite_float(vol_surge)
    body = _finite_float(body_ratio)
    macd = _finite_float(macd_hist)
    hist_wr = _finite_float(historical_winrate)
    spread = _finite_float(spread_pct)

    components: dict[str, float] = {}
    reasons: list[str] = []

    if conf == "HIGH":
        signal_component = 16.0
    elif conf in {"MEDIUM", "MID"}:
        signal_component = 5.0
    else:
        signal_component = -20.0
        reasons.append("low_confidence")
    components["signal"] = signal_component

    rsis = [v for v in (r15, r1h, r4h) if v is not None]
    if len(rsis) < 3:
        rsi_component = -8.0
        reasons.append("rsi_missing")
    else:
        constructive = sum(1 for v in rsis if 50.0 <= v <= 72.0)
        heated = sum(1 for v in rsis if 72.0 < v <= 80.0)
        exhausted = sum(1 for v in rsis if v > 80.0)
        weak = sum(1 for v in rsis if v < 45.0)
        rsi_component = constructive * 4.0 - heated * 4.0 - exhausted * 10.0 - weak * 5.0
        if constructive < 2:
            reasons.append("weak_rsi_alignment")
        if heated or exhausted:
            reasons.append("overheated_rsi")
    components["rsi"] = _component(rsi_component, -18.0, 12.0)

    if chg is None:
        pump_component = -8.0
        reasons.append("pump_missing")
    elif 3.0 <= chg <= 12.0:
        pump_component = 10.0
    elif 12.0 < chg <= 25.0:
        pump_component = -2.0
        reasons.append("late_pump")
    elif chg > 25.0:
        pump_component = -24.0
        reasons.append("blowoff_pump")
    else:
        pump_component = -10.0
        reasons.append("weak_pump")
    components["pump"] = pump_component

    if chg is None or btc is None:
        rel_component = -4.0
        reasons.append("relative_strength_missing")
    else:
        rel = chg - btc
        if rel >= 4.0:
            rel_component = 8.0
        elif rel >= 0.0:
            rel_component = 3.0
        elif rel >= -2.0:
            rel_component = -6.0
            reasons.append("weak_relative_strength")
        else:
            rel_component = -16.0
            reasons.append("bad_relative_strength")
    components["relative_strength"] = rel_component

    if reg == "BULL":
        regime_component = 4.0
    elif reg == "BEAR":
        regime_component = -12.0
        reasons.append("bear_market")
    else:
        regime_component = 0.0
    components["regime"] = regime_component

    if vol is None:
        volume_component = 0.0
    elif vol >= 1.5:
        volume_component = 5.0
    elif vol < 0.8:
        volume_component = -8.0
        reasons.append("weak_volume")
    else:
        volume_component = 0.0
    components["volume"] = volume_component

    if body is None:
        candle_component = 0.0
    elif body >= 0.45:
        candle_component = 4.0
    elif body < 0.20:
        candle_component = -8.0
        reasons.append("weak_candle")
    else:
        candle_component = 0.0
    components["candle"] = candle_component

    if macd is None:
        macd_component = 0.0
    elif macd > 0:
        macd_component = 4.0
    else:
        macd_component = -8.0
        reasons.append("negative_macd")
    components["macd"] = macd_component

    if hist_wr is None:
        history_component = 0.0
    elif hist_wr >= 0.55:
        history_component = 6.0
    elif hist_wr < 0.30:
        history_component = -14.0
        reasons.append("poor_setup_history")
    else:
        history_component = 0.0
    components["history"] = history_component

    if spread is None:
        spread_component = 0.0
    elif spread <= 0.10:
        spread_component = 3.0
    elif spread <= 0.30:
        spread_component = 0.0
    else:
        spread_component = -_component((spread - 0.30) * 35.0, 8.0, 24.0)
        reasons.append("wide_spread")
    components["spread"] = spread_component

    raw = 55.0 + sum(components.values())
    score = int(round(_component(raw, 0.0, 100.0)))
    return EntryQuality(
        score=score,
        label=label_for_score(score),
        reasons=tuple(dict.fromkeys(reasons)),
        components={key: round(value, 4) for key, value in components.items()},
        raw_score=raw,
    )


def score_cross_leg_entry(
    *,
    side: Any = None,
    rank_position: Any = None,
    rank_count: Any = None,
    return_pct: Any = None,
    spread_pct: Any = None,
    max_spread_pct: Any = 0.5,
    funding_rate_pct: Any = None,
    max_funding_pct: Any = 0.1,
    book_side_count: Any = None,
    target_side_count: Any = None,
    is_claimed: Any = False,
) -> EntryQuality:
    """Return a 0..100 score for one CROSS long/short basket leg."""
    leg_side = str(side or "").upper()
    rank_pos = _finite_float(rank_position)
    ranks = _finite_float(rank_count)
    ret = _finite_float(return_pct)
    spread = _finite_float(spread_pct)
    spread_limit = _finite_float(max_spread_pct) or 0.5
    funding = _finite_float(funding_rate_pct)
    funding_limit = _finite_float(max_funding_pct) or 0.1
    book_n = _finite_float(book_side_count)
    target_n = _finite_float(target_side_count)

    components: dict[str, float] = {}
    reasons: list[str] = []

    if leg_side not in {"LONG", "SHORT"}:
        side_component = -20.0
        reasons.append("side_missing")
    else:
        side_component = 0.0
    components["side"] = side_component

    if rank_pos is None or ranks is None or ranks <= 0:
        rank_component = -10.0
        reasons.append("rank_missing")
    else:
        pct = rank_pos / max(ranks, 1.0)
        if pct <= 0.15:
            rank_component = 18.0
        elif pct <= 0.33:
            rank_component = 14.0
        elif pct <= 0.66:
            rank_component = 5.0
        else:
            rank_component = -14.0
            reasons.append("weak_rank")
    components["rank"] = rank_component

    if ret is None or leg_side not in {"LONG", "SHORT"}:
        momentum_component = -8.0
        reasons.append("momentum_missing")
    else:
        aligned_move = ret if leg_side == "LONG" else -ret
        if aligned_move >= 5.0:
            momentum_component = 14.0
        elif aligned_move >= 1.0:
            momentum_component = 8.0
        elif aligned_move >= 0.0:
            momentum_component = -5.0
            reasons.append("weak_momentum")
        else:
            momentum_component = -22.0
            reasons.append("adverse_momentum")
    components["momentum"] = momentum_component

    if spread is None:
        spread_component = -8.0
        reasons.append("spread_missing")
    elif spread <= spread_limit * 0.5:
        spread_component = 8.0
    elif spread <= spread_limit:
        spread_component = 2.0
    else:
        ratio = spread / max(spread_limit, 1e-9)
        spread_component = -_component(12.0 + (ratio - 1.0) * 12.0, 12.0, 32.0)
        reasons.append("wide_spread")
    components["spread"] = spread_component

    if funding is None or leg_side not in {"LONG", "SHORT"}:
        funding_component = 0.0
    else:
        paying = (funding > 0.0) if leg_side == "LONG" else (funding < 0.0)
        if abs(funding) <= funding_limit * 0.25:
            funding_component = 0.0
        elif paying and abs(funding) > funding_limit:
            funding_component = -22.0
            reasons.append("adverse_funding")
        elif paying:
            funding_component = -8.0
            reasons.append("pays_funding")
        else:
            funding_component = 5.0
    components["funding"] = funding_component

    if target_n is None or target_n <= 0:
        balance_component = -16.0
        reasons.append("book_unbalanced")
    elif book_n is not None and book_n < target_n:
        balance_component = -5.0
        reasons.append("thin_side")
    else:
        balance_component = 2.0
    components["book_balance"] = balance_component

    claimed_component = -30.0 if bool(is_claimed) else 0.0
    if claimed_component:
        reasons.append("claimed")
    components["claim"] = claimed_component

    raw = 55.0 + sum(components.values())
    score = int(round(_component(raw, 0.0, 100.0)))
    return EntryQuality(
        score=score,
        label=label_for_score(score),
        reasons=tuple(dict.fromkeys(reasons)),
        components={key: round(value, 4) for key, value in components.items()},
        raw_score=raw,
    )
