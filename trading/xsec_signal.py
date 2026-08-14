"""
trading/xsec_signal.py  Cross-sectional momentum signal (PURE, no I/O).

The validated logic from tools/xsec_momentum.py, extracted as a pure, unit-
testable module (mirrors trading/trend_signal.py for the trend bot). The
orchestrator (core/cross_bot.py) handles all exchange I/O, state and execution.

Strategy: long the strongest K coins by lookback return / short the weakest K,
DOLLAR-NEUTRAL, with an own-momentum CRASH FILTER (exposure overlay that goes
flat after the strategy's own recent rebalances turn net-negative  the robust
drawdown-halving overlay from the research).

NO look-ahead: rankings use only past closes; the crash filter uses only PAST
realized rebalance returns.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class XSecParams:
    lookback_hours: int = 24      # ranking window
    k_per_side: int = 6           # coins long AND short (dollar-neutral)
    crash_filter: bool = True
    crash_window: int = 4         # rebalances looked back for the crash filter


@dataclass
class TargetBook:
    longs: List[str]              # symbols to hold LONG
    shorts: List[str]             # symbols to hold SHORT
    exposure_mult: float          # crash-filter exposure multiplier in [0, 1]

    @property
    def is_flat(self) -> bool:
        return self.exposure_mult <= 0.0 or (not self.longs and not self.shorts)


def lookback_return(prices: List[float], lookback: int) -> Optional[float]:
    """Return over the last ``lookback`` bars, or None if insufficient/invalid.

    ``prices`` is an ascending list of closes. Defensive against gaps/zeros."""
    if prices is None or len(prices) <= lookback or lookback <= 0:
        return None
    p_now = prices[-1]
    p_then = prices[-1 - lookback]
    if isinstance(p_now, bool) or isinstance(p_then, bool):
        return None
    try:
        p_now, p_then = float(p_now), float(p_then)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        not math.isfinite(p_now)
        or not math.isfinite(p_then)
        or p_now <= 0
        or p_then <= 0
    ):
        return None
    result = p_now / p_then - 1.0
    return result if math.isfinite(result) else None


def rank_and_select(returns: Dict[str, float], k: int) -> Tuple[List[str], List[str]]:
    """From {symbol: lookback_return} return (longs, shorts).

    Top-k by return = longs, bottom-k = shorts. Enforces a dollar-neutral,
    NON-OVERLAPPING split: if fewer than 2k symbols are available, shrink k
    symmetrically so the two legs stay equal-sized and never share a symbol.
    """
    valid = {}
    for symbol, raw_return in returns.items():
        if raw_return is None or isinstance(raw_return, bool):
            continue
        try:
            value = float(raw_return)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(value):
            valid[symbol] = value
    n = len(valid)
    if n < 2 or k <= 0:
        return [], []
    ranked = sorted(valid, key=lambda s: valid[s], reverse=True)
    k_eff = min(k, n // 2)        # never overlap; keep both legs equal-sized
    if k_eff < 1:
        return [], []
    return ranked[:k_eff], ranked[-k_eff:]


def crash_exposure(recent_rebalance_returns: List[float],
                   params: XSecParams) -> float:
    """Own-momentum crash filter  exposure multiplier in {0.0, 1.0}.

    Go FLAT (0.0) when the strategy's own last ``crash_window`` rebalance
    returns are net-negative; otherwise full exposure. Uses only PAST realized
    returns (no look-ahead). During warmup (too few rebalances)  full exposure.
    """
    if not params.crash_filter:
        return 1.0
    try:
        w = max(1, int(params.crash_window))
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if len(recent_rebalance_returns) < w:
        return 1.0
    window = []
    for raw_return in recent_rebalance_returns[-w:]:
        if isinstance(raw_return, bool):
            return 0.0
        try:
            value = float(raw_return)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if not math.isfinite(value):
            return 0.0
        window.append(value)
    return 0.0 if statistics.mean(window) < 0.0 else 1.0


def advance_crash_history(
    recent_rebalance_returns: List[float],
    realized_return: Optional[float],
    *,
    was_crash_flat: bool,
    slot_advanced: bool,
    max_history: int = 50,
) -> List[float]:
    """Return the next bounded own-momentum history.

    A crash-flat book has no realized price move.  Recording one neutral sample
    per *new anchored rebalance slot* lets the bounded filter cool down without
    inventing profit and without allowing repeated polls/manual retries to age
    the history.  This also makes the live transition reproducible in replay.
    """
    if not isinstance(recent_rebalance_returns, list):
        raise ValueError("recent rebalance returns must be a list")
    if isinstance(max_history, bool) or not isinstance(max_history, int):
        raise ValueError("max_history must be an integer")
    if max_history <= 0 or max_history > 1_000:
        raise ValueError("max_history is outside the supported range")
    if not isinstance(was_crash_flat, bool) or not isinstance(slot_advanced, bool):
        raise ValueError("crash-flat transition flags must be boolean")

    history = []
    for raw_return in recent_rebalance_returns[-max_history:]:
        if isinstance(raw_return, bool):
            raise ValueError("rebalance history contains a boolean")
        try:
            value = float(raw_return)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("rebalance history contains a non-number") from exc
        if not math.isfinite(value):
            raise ValueError("rebalance history contains a non-finite value")
        history.append(value)

    if realized_return is not None:
        if isinstance(realized_return, bool):
            raise ValueError("realized return must be finite numeric")
        try:
            value = float(realized_return)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("realized return must be finite numeric") from exc
        if not math.isfinite(value):
            raise ValueError("realized return must be finite numeric")
        history.append(value)
    elif was_crash_flat and slot_advanced:
        history.append(0.0)
    return history[-max_history:]


def compute_target_book(prices_by_symbol: Dict[str, List[float]],
                        recent_rebalance_returns: List[float],
                        params: XSecParams) -> TargetBook:
    """PURE: from price histories + own recent realized PnL  the target book."""
    mult = crash_exposure(recent_rebalance_returns, params)
    if mult <= 0.0:
        return TargetBook(longs=[], shorts=[], exposure_mult=0.0)
    returns = {
        s: lookback_return(p, params.lookback_hours)
        for s, p in prices_by_symbol.items()
    }
    returns = {s: r for s, r in returns.items() if r is not None}
    longs, shorts = rank_and_select(returns, params.k_per_side)
    return TargetBook(longs=longs, shorts=shorts, exposure_mult=mult)


def leg_notional(equity: float, leverage: float, exposure_mult: float,
                 k_per_side: int) -> float:
    """Per-coin notional for a dollar-neutral book.

    gross = equity  leverage  exposure_mult, split 50/50 long/short, then
    divided across ``k_per_side`` coins per leg. Returns 0 on degenerate input.
    """
    if any(
        isinstance(value, bool)
        for value in (equity, leverage, exposure_mult, k_per_side)
    ):
        return 0.0
    try:
        equity_value = float(equity)
        leverage_value = float(leverage)
        exposure_value = float(exposure_mult)
        k_value = int(k_per_side)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if (
        k_value <= 0
        or k_value != k_per_side
        or not math.isfinite(equity_value)
        or not math.isfinite(leverage_value)
        or not math.isfinite(exposure_value)
        or equity_value <= 0
        or leverage_value <= 0
    ):
        return 0.0
    mult = max(0.0, min(1.0, exposure_value))
    gross = equity_value * leverage_value * mult
    result = (gross / 2.0) / k_value
    return result if math.isfinite(result) else 0.0


#  Self-test (no pytest dir in this project  run directly) 
if __name__ == "__main__":
    p = XSecParams(lookback_hours=2, k_per_side=2, crash_filter=True, crash_window=3)

    # 6 coins, clearly ordered momentum over the last 2 bars.
    prices = {
        "AAA": [10, 10, 13.0],  # +30%  strongest
        "BBB": [10, 10, 12.0],   # +20%
        "CCC": [10, 10, 11.0],   # +10%
        "DDD": [10, 10, 10.0],   #   0%
        "EEE": [10, 10,  9.0],   # -10%
        "FFF": [10, 10,  8.0],  # -20%  weakest
        "GAP": [10],  # insufficient history  excluded
    }
    tb = compute_target_book(prices, recent_rebalance_returns=[], params=p)
    assert set(tb.longs) == {"AAA", "BBB"}, tb.longs       # 2 strongest
    assert set(tb.shorts) == {"EEE", "FFF"}, tb.shorts     # 2 weakest
    assert "GAP" not in tb.longs and "GAP" not in tb.shorts  # too little history
    assert tb.exposure_mult == 1.0
    assert not tb.is_flat

    # Crash filter: last 3 rebalances net-negative  flat.
    tb2 = compute_target_book(prices, [-0.02, -0.03, -0.01], p)
    assert tb2.is_flat and tb2.exposure_mult == 0.0, tb2

    # Crash filter: recovering  full exposure again.
    tb3 = compute_target_book(prices, [-0.02, +0.05, +0.03], p)
    assert not tb3.is_flat, tb3

    # Neutrality shrink: only 3 valid coins, k=2  k_eff=1 (equal legs, no overlap).
    small = {"X": [1, 1, 2.0], "Y": [1, 1, 1.5], "Z": [1, 1, 1.0]}
    tb4 = compute_target_book(small, [], p)
    assert tb4.longs == ["X"] and tb4.shorts == ["Z"], (tb4.longs, tb4.shorts)
    assert len(tb4.longs) == len(tb4.shorts) == 1   # neutral shrink, no overlap

    # Sizing: 1000 equity, 1x lev, full exposure, k=2  gross 1000, leg 250.
    assert leg_notional(1000, 1.0, 1.0, 2) == 250.0
    assert leg_notional(1000, 1.0, 0.0, 2) == 0.0   # flat
    assert leg_notional(1000, 1.5, 1.0, 6) == 125.0  # 1500 gross / 2 / 6

    print("xsec_signal self-test: ALL PASS")
