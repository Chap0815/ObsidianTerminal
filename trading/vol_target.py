"""trading/vol_target.py  inverse-volatility position sizing for the trend bots.

Risk-parity sizing: scale each coin's size by the basket's median volatility over
its own  calm coins get a bigger slot, wild coins a smaller one, so every leg
contributes similar dollar risk. Self-calibrating against the basket median, so
there is no magic target constant and the AVERAGE multiplier stays ~1 (total
deployed capital ~unchanged). Pure + timeframe-agnostic. Off unless enabled.
"""
from __future__ import annotations

import statistics
from typing import List, Optional


def realized_vol(closes: List[float], lookback: int = 30) -> Optional[float]:
    """Population stdev of the last ``lookback`` simple bar-returns, or None if
    there isn't enough history."""
    if not closes or lookback < 2 or len(closes) < lookback + 1:
        return None
    window = closes[-(lookback + 1):]
    rets = [window[i] / window[i - 1] - 1
            for i in range(1, len(window)) if window[i - 1]]
    if len(rets) < 2:
        return None
    v = statistics.pstdev(rets)
    return v if v > 0 else None


def vol_target_multiplier(coin_vol: Optional[float],
                          basket_median_vol: Optional[float],
                          lo: float = 0.33, hi: float = 1.0) -> float:
    """Size multiplier = basket_median_vol / coin_vol, clamped to [lo, hi].
    Falls back to 1.0 (flat sizing) on missing/degenerate inputs."""
    if not coin_vol or not basket_median_vol or coin_vol <= 0:
        return 1.0
    # Risk overlays may shrink a validated base size, never increase it before
    # their own OOS promotion gate has passed.
    safe_hi = min(1.0, float(hi))
    return max(lo, min(safe_hi, basket_median_vol / coin_vol))


def basket_median_vol(vols: List[Optional[float]]) -> Optional[float]:
    """Median of the usable (positive) per-coin vols, or None."""
    usable = [v for v in vols if v and v > 0]
    if not usable:
        return None
    return statistics.median(usable)
