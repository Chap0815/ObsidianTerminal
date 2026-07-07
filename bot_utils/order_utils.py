"""
bot_utils/order_utils.py  CCXT order parsing helpers, shared by all bots.

  extract_fill_price(order, fallback)  float
  extract_order_fee(order)  float (USDT)
  convert_fee_to_usdt(fee, order)  float
  safe_remaining(current, sold)  float (Decimal-safe subtraction)
  extract_base_fee_amount(order, base)  float

safe_remaining uses ``Decimal(str(x))`` (not a fixed-width f-string): the str()
path preserves sub-nano amounts (1e-12) that ``f"{x:.10f}"`` would round to 0.
"""
from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation


#  Fill price 

def order_was_filled(order: dict, requested_amount: float = 0.0,
                     min_fill_ratio: float = 0.90) -> bool:
    """True only if a CCXT order actually executed.

    A market sell can return an order object that did NOT fill (status
    'new'/'open'/'rejected', filled=0)  e.g. when the remaining size is below
    the exchange min-notional. Treating any returned order as success would
    write a closed trade while the coin still sits in the wallet, so we check
    the order genuinely filled before treating the position as closed.

    Failure is recognised ONLY by EXPLICIT evidence (a 'new'/'open'/'rejected'/
    'canceled'/'expired' status with insufficient filled). An ACCEPTED order
    (has an id, no such status) is treated as filled  even when the response
    carries no status/filled/cost at all: MEXC's spot market-sell response is
    routinely minimal (status=None, no filled, no cost) yet the order DID
    execute, and a genuine rejection raises in ccxt or carries an explicit
    status (caught above).
    """
    if not isinstance(order, dict):
        return False
    status = str(order.get("status", "")).lower()
    if status in ("closed", "filled"):
        return True
    if status in ("canceled", "cancelled", "rejected", "expired", "new",
                  "open", "pending"):
        # Explicit non-filled state. Allow only if a real partial fill exists.
        filled = order.get("filled")
        try:
            f = float(filled) if filled is not None else 0.0
        except (ValueError, TypeError):
            f = 0.0
        if requested_amount > 0 and f >= requested_amount * min_fill_ratio:
            return True
        return False
    # No usable status  fall back to filled / cost evidence.
    try:
        filled = float(order.get("filled") or 0)
    except (ValueError, TypeError):
        filled = 0.0
    if filled > 0:
        if requested_amount > 0:
            return filled >= requested_amount * min_fill_ratio
        return True
    try:
        cost = float(order.get("cost") or 0)
    except (ValueError, TypeError):
        cost = 0.0
    if cost > 0:
        return True
    # AMBIGUOUS: no status, no fill data. If the exchange ACCEPTED the order
    # (it has an id and create_*_order didn't raise), treat it as filled 
    # this is MEXC's normal minimal market-order response, not a failure.
    # An empty/garbage dict (no id) is still treated as NOT filled.
    return bool(order.get("id") or order.get("orderId"))


def extract_fill_price(order: dict, fallback: float) -> float:
    """Get the actual market-order fill price from a CCXT order dict.

    Tries `average`  `price`  `cost/filled`. Falls back to the supplied
    value (usually the ticker price) when none is available.
    """
    if not isinstance(order, dict):
        return fallback
    for key in ("average", "price"):
        v = order.get(key)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (ValueError, TypeError):
                continue
    try:
        cost = float(order.get("cost") or 0)
        filled = float(order.get("filled") or 0)
        if cost > 0 and filled > 0:
            return cost / filled
    except (ValueError, TypeError):
        pass
    return fallback


#  Fee extraction 

def convert_fee_to_usdt(fee_dict, order_dict) -> float:
    """Convert a single CCXT fee dict to USDT.

    Returns 0.0 if conversion isn't safely possible (e.g. fee in unknown
    discount token like BNB/BGB/KCS  we'd need an external price feed).
    """
    if not isinstance(fee_dict, dict):
        return 0.0
    try:
        cost = abs(float(fee_dict.get("cost", 0) or 0))
    except (TypeError, ValueError):
        return 0.0
    if cost <= 0:
        return 0.0

    currency = (fee_dict.get("currency") or "").upper()
    if not currency or currency in ("USDT", "USD", "BUSD", "USDC", "FDUSD"):
        return cost

    symbol = (order_dict.get("symbol") or "") if isinstance(order_dict, dict) else ""
    base = symbol.split("/")[0].upper() if "/" in symbol else ""
    if currency == base:
        fill_price = 0.0
        for k in ("average", "price"):
            v = order_dict.get(k) if isinstance(order_dict, dict) else None
            try:
                fv = float(v) if v is not None else 0.0
                if fv > 0:
                    fill_price = fv
                    break
            except (TypeError, ValueError):
                continue
        if fill_price > 0:
            return cost * fill_price
        return 0.0

    return 0.0


def extract_order_fee(order: dict) -> float:
    """Total fee paid for an order, in USDT.

    On most CCXT integrations order["fee"] is a SUMMARY of order["fees"] (same
    total), so adding both would double-count. Prefer the plural (more granular)
    when valid entries exist; fall back to the singular. Uses ``is not None``
    checks for the cost key so legitimate cost=0 maker rebates aren't dropped.
    """
    if not isinstance(order, dict):
        return 0.0

    fees_list = order.get("fees") or []
    if isinstance(fees_list, list):
        # cost of 0 (legit maker rebate) must count  `is not None`, not truthy.
        valid = [f for f in fees_list
                  if isinstance(f, dict) and f.get("cost") is not None]
        if valid:
            return sum(convert_fee_to_usdt(f, order) for f in valid)

    return convert_fee_to_usdt(order.get("fee") or {}, order)


def extract_base_fee_amount(order: dict, base_symbol: str) -> float:
    """Sum BASE-currency fees from both singular `fee` and plural `fees`."""
    if not isinstance(order, dict) or not base_symbol:
        return 0.0
    base_upper = base_symbol.upper()
    sources = []
    f = order.get("fee")
    if isinstance(f, dict):
        sources.append(f)
    fl = order.get("fees")
    if isinstance(fl, list):
        sources.extend(x for x in fl if isinstance(x, dict))
    total = 0.0
    for fo in sources:
        fc = (fo.get("currency") or "").upper()
        if fc != base_upper:
            continue
        try:
            fv = float(fo.get("cost", 0) or 0)
        except (TypeError, ValueError):
            continue
        if fv > 0:
            total += fv
    return total


#  Safe remaining 

def _is_safe_finite(value) -> bool:
    """Return True iff value can be cast to a finite float."""
    if value is None:
        return False
    try:
        fv = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(fv)


def safe_remaining(current_amount: float, sold_amount: float,
                    dust_threshold: float = 1e-8) -> float:
    """Subtract sold from current using Decimal to avoid float drift.

    Float subtraction can produce results like 0.30000000000004 from what
    should be exactly 0.3  exchanges reject this as below precision.
    Snapping anything below `dust_threshold` (default 1e-8  1 satoshi) to
    zero is safe: far below tradeable lot size on any major exchange.

    NaN/Inf inputs are coerced to 0.0 BEFORE conversion  ``Decimal('NaN') <
    anything`` raises InvalidOperation in many Python builds.
    """
    # pre-filter non-finite inputs
    if not _is_safe_finite(current_amount):
        return 0.0
    if not _is_safe_finite(sold_amount):
        # Sold is corrupt but current is finite  treat sold as 0
        sold_amount = 0.0

    try:
        rem = Decimal(str(current_amount)) - Decimal(str(sold_amount))
    except (InvalidOperation, TypeError, ValueError):
        return 0.0

    # Defensive: ensure rem itself is finite (Decimal can hold NaN/Inf
    # but is_finite() catches it explicitly)
    if not rem.is_finite():
        return 0.0

    try:
        threshold = Decimal(str(dust_threshold))
        if rem < threshold:
            return 0.0
    except (InvalidOperation, TypeError, ValueError):
        return 0.0

    return float(rem)
