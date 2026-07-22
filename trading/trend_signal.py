"""
trading/trend_signal.py  Majors trend-following signal (validated 2026-06-13).

This is the heart of the "Trend" bot (the repurposed TREND slot). It is a
pure, dependency-light, unit-testable function: given a coin's recent DAILY
closes, decide whether we should be LONG (in trend) or FLAT (cash).

Validated in tools/trend_check.py over 720d of MEXC majors:
  ENSEMBLE = long when a MAJORITY (>=2 of 3) of these agree:
        1. price > SMA(fast)        (default 50)
        2. price > SMA(slow)        (default 100)
        3. SMA(cross_fast) > SMA(cross_slow)   (default 20 > 50)
  Robust across SMA lengths 30120 (7/7 beat buy-and-hold) and across coins
    (4/5), beating B&H on return AND halving drawdown over a full bull+bear.

Why an ensemble, not a single MA: the sweep showed every length works, so we
vote across a few rather than betting on one  removes single-parameter
overfit. Hysteresis (separate exit_vote) optionally damps whipsaw at the edge.

NO leverage anywhere here  this is a long/flat spot signal by design.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict


@dataclass(frozen=True)
class TrendParams:
    sma_fast:   int = 50
    sma_slow:   int = 100
    cross_fast: int = 20
    cross_slow: int = 50
    vote_min:   int = 2      # votes (of 3) needed to be IN trend
    exit_vote:  int = 2      # fall BELOW this to exit (>= for hysteresis: set < vote_min)


def _sma(vals: List[float], n: int) -> Optional[float]:
    """Simple moving average of the last n values, or None if too few."""
    if n <= 0 or len(vals) < n:
        return None
    return sum(vals[-n:]) / float(n)


def bars_required(p: TrendParams) -> int:
    return max(p.sma_slow, p.cross_slow, p.sma_fast, p.cross_fast) + 1


def has_full_history(closes: List[float], p: TrendParams) -> bool:
    return len(closes) >= bars_required(p)


def trend_votes(closes: List[float], p: TrendParams) -> Tuple[int, Dict[str, bool]]:
    """Return (vote_count, per-rule detail) for the LATEST bar.

    Defensive: rules whose MA can't be computed yet (not enough history) simply
    don't vote  they never block, never falsely fire."""
    if not closes:
        return 0, {}
    normalized = []
    for raw_close in closes:
        if isinstance(raw_close, bool):
            return 0, {}
        try:
            close = float(raw_close)
        except (TypeError, ValueError, OverflowError):
            return 0, {}
        if not math.isfinite(close) or close <= 0:
            return 0, {}
        normalized.append(close)
    closes = normalized
    last = closes[-1]
    detail: Dict[str, bool] = {}

    f = _sma(closes, p.sma_fast)
    detail[f"P>SMA{p.sma_fast}"] = bool(f is not None and last > f)

    s = _sma(closes, p.sma_slow)
    detail[f"P>SMA{p.sma_slow}"] = bool(s is not None and last > s)

    cf = _sma(closes, p.cross_fast)
    cs = _sma(closes, p.cross_slow)
    detail[f"SMA{p.cross_fast}>SMA{p.cross_slow}"] = bool(
        cf is not None and cs is not None and cf > cs)

    return sum(1 for v in detail.values() if v), detail


def is_in_trend(closes: List[float], p: TrendParams,
                currently_held: bool = False) -> Tuple[bool, int, Dict[str, bool]]:
    """Decide LONG (True) vs FLAT (False) for the latest bar.

    Hysteresis: when we are already holding, we only EXIT once votes fall below
    ``exit_vote`` (which can be set lower than ``vote_min`` to avoid flip-flop
    at the boundary). When flat, we ENTER at ``vote_min``. With the defaults
    (both = 2) there is no hysteresis  set exit_vote=1 to add some.

    Returns (in_trend, votes, detail)."""
    votes, detail = trend_votes(closes, p)
    if currently_held:
        in_trend = votes >= p.exit_vote
    else:
        in_trend = votes >= p.vote_min
    return in_trend, votes, detail


def params_from_cfg(cfg_get) -> TrendParams:
    """Build TrendParams from a bot config accessor ``cfg_get(key, default)``
    (e.g. the bot's ``self.C``). Keeps the bot decoupled from this module's
    field names."""
    def _i(key, default):
        try:
            value = float(cfg_get(key, default))
            if not math.isfinite(value):
                return default
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return default
    return TrendParams(
        sma_fast=_i("TREND_SMA_FAST", 50),
        sma_slow=_i("TREND_SMA_SLOW", 100),
        cross_fast=_i("TREND_CROSS_FAST", 20),
        cross_slow=_i("TREND_CROSS_SLOW", 50),
        vote_min=_i("TREND_VOTE_MIN", 2),
        exit_vote=_i("TREND_EXIT_VOTE", 2),
    )
