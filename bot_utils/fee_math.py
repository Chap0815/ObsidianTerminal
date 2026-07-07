"""
bot_utils/fee_math.py  Safe proportional fee + funding calculations.

The pattern ``initial_fee * (current_amount / original_amount)`` is used in
several places (spot/futures  full-exit / emergency-close / partial-TP). When
``original_amount`` is 0 or missing (manual trades.json edits, pre-v2.0 rows,
an un-backfilled migration, state corruption), a naive ``else: initial_fee``
fallback charges the FULL entry fee against the remaining slice  which
double-deducts the entry fee on partial-sold positions (the partial-TP row
already booked a proportional slice). This module centralizes the safe pattern.
"""
from __future__ import annotations

from typing import Any


DEFAULT_TAKER_FEE_RATE = 0.001


def taker_fee_rate(ex: Any, symbol: str, default: float = DEFAULT_TAKER_FEE_RATE) -> float:
    """Return the market's real taker fee rate, falling back to ``default``.

    Reads ``ex.markets[symbol]['taker']`` when it is a positive float; otherwise
    returns ``default``. Never raises  any missing/malformed metadata yields the
    default so backtests/simulations stay deterministic.
    """
    try:
        markets = getattr(ex, "markets", None) or {}
        m = markets.get(symbol) or {}
        t = m.get("taker")
        if t is not None:
            tv = float(t)
            if tv > 0:
                return tv
    except (TypeError, ValueError, AttributeError):
        pass
    return default


def safe_proportional_fee(initial_fee: Any,
                            current_amount: Any,
                            original_amount: Any,
                            partial_sold: bool = False) -> float:
    """Safely calculate the proportional entry fee for a remaining slice.

    Parameters
    ----------
    initial_fee : numeric
        The full entry fee paid when the position was opened, in USDT.
    current_amount : numeric
        How many contracts / coins remain to be closed RIGHT NOW.
    original_amount : numeric
        How many contracts / coins the position was opened with (immutable 
        should equal the initial filled amount).
    partial_sold : bool
        True if a partial-TP already executed on this position. Affects
        the corruption-fallback strategy.

    Returns
    -------
    float
        The proportional entry fee in USDT, clamped to ``[0, initial_fee]``.

    Behavior
    --------
    Normal case (``original_amount > 0``):
        Returns ``initial_fee * min(1.0, current_amount / original_amount)``.
        The clamp prevents over-charging if the state ever has
        ``current > original`` (which shouldn't happen but we defend).

    Corruption case (``original_amount <= 0`` or None):
        If ``partial_sold=False``: assume current_amount IS the original
          (no partials happened, so the remaining slice IS the full
          position). Returns ``initial_fee`` (safe here because no partials
          happened).
        If ``partial_sold=True``: returns ``0.0``. Better to under-report
          by a few cents than to double-deduct the full entry fee, which
          would silently distort PnL accounting.
    """
    try:
        init = float(initial_fee or 0)
        curr = float(current_amount or 0)
        orig = float(original_amount or 0)
    except (TypeError, ValueError):
        return 0.0

    if init <= 0 or curr <= 0:
        return 0.0

    if orig > 0:
        # Clamp ratio to [0, 1]  slice can't exceed original.
        # Without the min(), a state where current > original (e.g. user
        # bought more on the exchange manually) would over-attribute fees.
        ratio = min(1.0, curr / orig)
        return round(init * ratio, 6)

    # original_amount missing/corrupted/zero
    if partial_sold:
        # Conservative fallback: 0.0 under-reports by a few cents but avoids
        # double-deducting the entry fee (the partial-TP row already
        # attributed a slice to this position).
        return 0.0

    # No partial-TP happened  current IS the original position
    return round(init, 6)


def safe_funding_scale(funding_total: Any,
                         current_amount: Any,
                         original_amount: Any,
                         partial_sold: bool = False) -> float:
    """Same corruption-safe pattern for funding paid.

    Returns the funding portion attributable to the remaining slice.
    Behavior mirrors ``safe_proportional_fee``  see its docstring.
    """
    try:
        f = float(funding_total or 0)
        curr = float(current_amount or 0)
        orig = float(original_amount or 0)
    except (TypeError, ValueError):
        return 0.0

    if curr <= 0:
        return 0.0

    if orig > 0:
        ratio = min(1.0, curr / orig)
        return round(f * ratio, 6)

    if partial_sold:
        return 0.0
    return round(f, 6)


def safe_remaining_funding(funding_total: Any,
                           current_amount: Any,
                           original_amount: Any,
                           partial_sold: bool = False,
                           booked_on_partials: Any = 0.0) -> float:
    """Funding attributable to the current final-close slice.

    Partial-TP rows already write their own funding slice to the trade DB. When
    a later final close fetches/estimates total funding since entry, subtract the
    amount already booked on partial rows instead of charging it twice.

    If no partial funding was recorded, fall back to the legacy proportional
    scale so old state files remain valid.
    """
    scaled = safe_funding_scale(
        funding_total, current_amount, original_amount,
        partial_sold=partial_sold,
    )
    try:
        booked = float(booked_on_partials or 0.0)
    except (TypeError, ValueError):
        booked = 0.0
    if not partial_sold or abs(booked) <= 1e-12:
        return scaled
    try:
        total = float(funding_total or 0.0)
    except (TypeError, ValueError):
        return scaled
    return round(total - booked, 6)
