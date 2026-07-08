"""Helpers for accumulating verified close-fill fragments.

Full-close reduce-only market orders can fill in pieces on thin books. We only
book the trade after the exchange verifies the position is flat, but we must
preserve earlier partial fill prices/fees so final accounting uses the weighted
close price instead of only the last retry's price.
"""
from __future__ import annotations

import math
from typing import Any


def _f(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except (TypeError, ValueError, OverflowError):
        return default


def pending_close_values(state: dict) -> tuple[float, float, float, str | None]:
    amount = max(0.0, _f(state.get("pending_close_filled_amount")))
    notional = max(0.0, _f(state.get("pending_close_notional_sum")))
    fee = _f(state.get("pending_close_fee"))
    fallback_price = max(0.0, _f(state.get("pending_close_price")))
    price = (
        (notional / amount)
        if amount > 0 and notional > 0
        else fallback_price
    )
    oid = state.get("pending_close_order_id")
    return amount, price, fee, str(oid) if oid else None


def add_close_fragment_update(
    state: dict,
    *,
    amount: float,
    price: float,
    fee: float = 0.0,
    order_id=None,
) -> dict:
    amount = max(0.0, _f(amount))
    price = max(0.0, _f(price))
    fee = _f(fee)
    prev_amount, _prev_price, prev_fee, prev_oid = pending_close_values(state)
    prev_notional = max(0.0, _f(state.get("pending_close_notional_sum")))
    if prev_notional <= 0.0 and prev_amount > 0 and _prev_price > 0:
        prev_notional = prev_amount * _prev_price
    elif prev_notional <= 0.0 and prev_amount > 0:
        prev_amount = 0.0
        prev_fee = 0.0
        prev_oid = None

    new_amount = prev_amount + amount
    new_notional = prev_notional + (amount * price if amount > 0 else 0.0)
    new_fee = prev_fee + fee
    avg_price = (new_notional / new_amount) if new_amount > 0 else price
    oid = order_id or prev_oid
    return {
        "pending_close_filled_amount": new_amount,
        "pending_close_notional_sum": new_notional,
        "pending_close_price": avg_price,
        "pending_close_fee": new_fee,
        "pending_close_order_id": str(oid) if oid else None,
    }
