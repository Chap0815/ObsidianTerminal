"""Helpers for accumulating verified close-fill fragments.

Full-close reduce-only market orders can fill in pieces on thin books. We only
book the trade after the exchange verifies the position is flat, but we must
preserve earlier partial fill prices/fees so final accounting uses the weighted
close price instead of only the last retry's price.
"""
from __future__ import annotations

import math
from typing import Any

from bot_utils.order_utils import order_id_text_or_none


def _f(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _order_id_text_or_none(value: Any) -> str | None:
    return order_id_text_or_none(value)


def pending_close_values(state: dict) -> tuple[float, float, float, str | None]:
    amount = max(0.0, _f(state.get("pending_close_filled_amount")))
    notional = max(0.0, _f(state.get("pending_close_notional_sum")))
    fee = _f(state.get("pending_close_fee"))
    fallback_price = max(0.0, _f(state.get("pending_close_price")))
    price = (notional / amount) if amount > 0 and notional > 0 else fallback_price
    if not math.isfinite(price):
        amount = 0.0
        price = 0.0
        fee = 0.0
    oid = _order_id_text_or_none(state.get("pending_close_order_id"))
    return amount, price, fee, oid


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
    prev_notional = (
        max(0.0, _f(state.get("pending_close_notional_sum")))
        if prev_amount > 0
        else 0.0
    )
    if prev_notional <= 0.0 and prev_amount > 0 and _prev_price > 0:
        prev_notional = prev_amount * _prev_price
        if not math.isfinite(prev_notional):
            prev_notional = 0.0
            prev_amount = 0.0
            prev_fee = 0.0
            prev_oid = None
    elif prev_notional <= 0.0 and prev_amount > 0:
        prev_amount = 0.0
        prev_fee = 0.0
        prev_oid = None

    fragment_notional = amount * price if amount > 0 else 0.0
    if not math.isfinite(fragment_notional):
        amount = 0.0
        price = 0.0
        fee = 0.0
        fragment_notional = 0.0

    new_amount = prev_amount + amount
    new_notional = prev_notional + fragment_notional
    new_fee = prev_fee + fee
    if not all(math.isfinite(v) for v in (new_amount, new_notional, new_fee)):
        new_amount = prev_amount
        new_notional = prev_notional
        new_fee = prev_fee
    avg_price = (new_notional / new_amount) if new_amount > 0 else price
    if not math.isfinite(avg_price):
        new_amount = prev_amount
        new_notional = prev_notional
        new_fee = prev_fee
        avg_price = _prev_price if prev_amount > 0 else 0.0
    oid = _order_id_text_or_none(order_id) or prev_oid
    return {
        "pending_close_filled_amount": new_amount,
        "pending_close_notional_sum": new_notional,
        "pending_close_price": avg_price,
        "pending_close_fee": new_fee,
        "pending_close_order_id": oid,
    }
