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


def _finite_float_or_none(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _upper_text(value) -> str:
    return value.upper() if isinstance(value, str) else ""


def _order_id_text(value) -> str:
    if value is None or isinstance(value, bool):
        return ""
    try:
        return str(value).strip()
    except Exception:
        return ""


def _safe_positive_price(value) -> float:
    parsed = _finite_float_or_none(value)
    return parsed if parsed is not None and parsed > 0 else 0.0


#  Fill price 

def order_was_filled(order: dict, requested_amount: float = 0.0,
                     min_fill_ratio: float = 0.90,
                     *,
                     trust_terminal_status_with_bad_numbers: bool = False) -> bool:
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
    raw_status = order.get("status")
    status_is_malformed = (
        raw_status is not None
        and not isinstance(raw_status, str)
    )
    status = raw_status.strip().lower() if isinstance(raw_status, str) else ""
    if status in ("closed", "filled"):
        has_fill_or_cost = False
        has_positive_fill_or_cost = False
        for key in ("filled", "cost"):
            if order.get(key) is None:
                continue
            parsed = _finite_float_or_none(order.get(key))
            if parsed is None:
                if trust_terminal_status_with_bad_numbers:
                    continue
                return False
            has_fill_or_cost = True
            if parsed > 0:
                has_positive_fill_or_cost = True
        if has_fill_or_cost and not has_positive_fill_or_cost:
            return False
        if has_positive_fill_or_cost:
            return True
        return bool(_order_id_text(order.get("id")) or
                    _order_id_text(order.get("orderId")))
    if status in ("canceled", "cancelled", "rejected", "expired", "new",
                  "open", "pending"):
        # Explicit non-filled state. Allow only if a real partial fill exists.
        filled = order.get("filled")
        f = _finite_float_or_none(filled) if filled is not None else 0.0
        if f is None:
            return False
        req = _finite_float_or_none(requested_amount) or 0.0
        if (
            math.isfinite(f)
            and math.isfinite(req)
            and req > 0
            and f >= req * min_fill_ratio
        ):
            return True
        return False
    # No usable status  fall back to filled / cost evidence.
    filled_present = order.get("filled") is not None
    filled = _finite_float_or_none(order.get("filled")) if filled_present else 0.0
    if filled is None:
        return False
    if math.isfinite(filled) and filled > 0:
        req = _finite_float_or_none(requested_amount) or 0.0
        if math.isfinite(req) and req > 0:
            return filled >= req * min_fill_ratio
        return True
    cost_present = order.get("cost") is not None
    cost = _finite_float_or_none(order.get("cost")) if cost_present else 0.0
    if cost is None:
        return False
    if math.isfinite(cost) and cost > 0:
        return True
    if status_is_malformed:
        return False
    # AMBIGUOUS: no status, no fill data. If the exchange ACCEPTED the order
    # (it has an id and create_*_order didn't raise), treat it as filled 
    # this is MEXC's normal minimal market-order response, not a failure.
    # An empty/garbage dict (no id) is still treated as NOT filled.
    return bool(_order_id_text(order.get("id")) or
                _order_id_text(order.get("orderId")))


def extract_fill_price(order: dict, fallback: float) -> float:
    """Get the actual market-order fill price from a CCXT order dict.

    Tries `average`  `price`  `cost/filled`. Falls back to the supplied
    value (usually the ticker price) when none is available.
    """
    if not isinstance(order, dict):
        return _safe_positive_price(fallback)
    for key in ("average", "price"):
        v = order.get(key)
        if v is not None:
            fv = _finite_float_or_none(v)
            if fv is not None and fv > 0:
                return fv
    cost = _finite_float_or_none(order.get("cost"))
    filled = _finite_float_or_none(order.get("filled"))
    if cost is not None and filled is not None and cost > 0 and filled > 0:
        ratio = cost / filled
        if math.isfinite(ratio) and ratio > 0:
            return ratio
    return _safe_positive_price(fallback)


#  Fee extraction 

def convert_fee_to_usdt(fee_dict, order_dict) -> float:
    """Convert a single CCXT fee dict to USDT.

    Returns 0.0 if conversion isn't safely possible (e.g. fee in unknown
    discount token like BNB/BGB/KCS  we'd need an external price feed).
    """
    fee, _known = _convert_fee_to_usdt_known(fee_dict, order_dict)
    return fee


def _convert_fee_to_usdt_known(fee_dict, order_dict) -> tuple[float, bool]:
    if not isinstance(fee_dict, dict):
        return 0.0, False
    parsed_cost = _finite_float_or_none(fee_dict.get("cost"))
    if parsed_cost is None:
        return 0.0, False
    cost = parsed_cost
    if not math.isfinite(cost):
        return 0.0, False

    raw_currency = fee_dict.get("currency")
    if raw_currency is not None and not isinstance(raw_currency, str):
        return 0.0, False
    currency = _upper_text(raw_currency)
    if not currency:
        return 0.0, False
    if cost == 0:
        return 0.0, True
    if currency in ("USDT", "USD", "BUSD", "USDC", "FDUSD"):
        return cost, True

    symbol = (order_dict.get("symbol") or "") if isinstance(order_dict, dict) else ""
    symbol = symbol if isinstance(symbol, str) else ""
    base = symbol.split("/")[0].upper() if "/" in symbol else ""
    if currency == base:
        fill_price = 0.0
        for k in ("average", "price"):
            v = order_dict.get(k) if isinstance(order_dict, dict) else None
            fv = _finite_float_or_none(v) if v is not None else None
            if fv is not None and fv > 0:
                fill_price = fv
                break
        if fill_price > 0:
            converted = cost * fill_price
            return (converted, True) if math.isfinite(converted) else (0.0, False)
        return 0.0, False

    return 0.0, False


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
            saw_known = False
            plural_total = 0.0
            for fee_dict in valid:
                fee, known = _convert_fee_to_usdt_known(fee_dict, order)
                if known:
                    saw_known = True
                    plural_total += fee
            if saw_known:
                return plural_total if math.isfinite(plural_total) else 0.0
            singular_fee = order.get("fee")
            if isinstance(singular_fee, dict) and singular_fee.get("cost") is not None:
                return convert_fee_to_usdt(singular_fee, order)
            return 0.0

    return convert_fee_to_usdt(order.get("fee") or {}, order)


def _base_fee_amount_from_fee(fee_dict, base_upper: str) -> float:
    if not isinstance(fee_dict, dict):
        return 0.0
    raw_currency = fee_dict.get("currency")
    if raw_currency is not None and not isinstance(raw_currency, str):
        return 0.0
    if _upper_text(raw_currency) != base_upper:
        return 0.0
    fv = _finite_float_or_none(fee_dict.get("cost"))
    if fv is None:
        return 0.0
    return fv if math.isfinite(fv) and fv > 0 else 0.0


def extract_base_fee_amount(order: dict, base_symbol: str) -> float:
    """Return BASE-currency fee without double-counting summary fee fields."""
    if not isinstance(order, dict) or not base_symbol:
        return 0.0
    base_upper = _upper_text(base_symbol)
    if not base_upper:
        return 0.0
    fl = order.get("fees")
    if isinstance(fl, list):
        plural_total = sum(
            _base_fee_amount_from_fee(fee, base_upper)
            for fee in fl
            if isinstance(fee, dict)
        )
        if math.isfinite(plural_total) and plural_total > 0:
            return plural_total
    return _base_fee_amount_from_fee(order.get("fee"), base_upper)


#  Safe remaining 

def _is_safe_finite(value) -> bool:
    """Return True iff value can be cast to a finite float."""
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    try:
        fv = float(value)
    except (TypeError, ValueError, OverflowError):
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
    current_value = float(current_amount)
    if current_value <= 0:
        return 0.0
    if not _is_safe_finite(sold_amount):
        # Sold is corrupt but current is finite  treat sold as 0
        sold_amount = 0.0
    else:
        sold_value = float(sold_amount)
        sold_amount = sold_value if sold_value > 0 else 0.0

    try:
        rem = Decimal(str(current_value)) - Decimal(str(sold_amount))
    except (InvalidOperation, TypeError, ValueError):
        return 0.0

    # Defensive: ensure rem itself is finite (Decimal can hold NaN/Inf
    # but is_finite() catches it explicitly)
    if not rem.is_finite():
        return 0.0

    try:
        threshold = Decimal(str(dust_threshold))
        if not threshold.is_finite() or threshold < 0:
            threshold = Decimal("0")
    except (InvalidOperation, TypeError, ValueError):
        threshold = Decimal("0")
    if rem < threshold:
        return 0.0

    return float(rem)
