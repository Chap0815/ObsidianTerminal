"""
bot_utils/balance_resolver.py  Robust balance reading for reconciliation.

Reconciliation must use ``total`` (= free + used), not just ``free``. If the
user manually places a limit-sell on a bot-managed position (or moves it into
an Earn/Lend product), ``free`` drops to 0 while ``total`` stays  using
``free`` alone would wrongly declare the position a phantom and queue it for
removal. A position counts as REALLY gone only when total is ~0 (see
DUST_THRESHOLD).
"""
from __future__ import annotations

import math


def effective_balance(info: dict, dust_threshold: float = 1e-8) -> float:
    """Return the EFFECTIVE held balance for reconciliation purposes.

    Reconciliation should treat a coin as "still on exchange" if ANY of
    these is nonzero:
      free  immediately available
      used  locked in open orders
      total  sometimes the exchange reports this differently

    Strategy: prefer ``total`` (sum of free + used + earn-locked); fall
    back to ``free + used`` if total is missing; never return 0 just
    because the user has an open limit order on the coin.

    Returns 0.0 if the result is below dust_threshold.
    """
    if not isinstance(info, dict):
        return 0.0

    try:
        threshold = float(dust_threshold)
    except (TypeError, ValueError, OverflowError):
        threshold = 1e-8
    if not (math.isfinite(threshold) and threshold >= 0):
        threshold = 1e-8

    def _safe_float(key: str) -> float | None:
        try:
            v = info.get(key)
            if v is None:
                return None
            if isinstance(v, bool):
                return None
            parsed = float(v)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) and parsed >= 0 else None

    total = _safe_float("total")
    free  = _safe_float("free")
    used  = _safe_float("used")

    # Prefer total when it exists and is sane
    if total is not None and total > 0:
        result = total
    elif (free or 0.0) > 0 or (used or 0.0) > 0:
        # Fallback: sum free + used. Some exchanges miss the total field.
        result = (free or 0.0) + (used or 0.0)
        if not math.isfinite(result):
            result = max(free or 0.0, used or 0.0)
    else:
        result = 0.0

    return result if result > threshold else 0.0


def is_position_held(info: dict, dust_threshold: float = 1e-8) -> bool:
    """True if there's a non-dust balance for this coin on the exchange.

    Use this in reconcile_with_exchange instead of ``free > 0``.
    """
    return effective_balance(info, dust_threshold) > 0
