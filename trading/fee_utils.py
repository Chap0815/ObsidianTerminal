"""
fee_utils.py  Shared fee-extraction utilities.

Bitget liefert bei Market-Orders oft eine erste Response ohne Fee-Info; eine
zweite fetch_order(id) hat dann die Details (siehe
extract_or_estimate_with_refetch).
"""
from __future__ import annotations
from typing import Optional

try:
    from core.constants import STABLECOIN_EQUIVALENTS, DEFAULT_TAKER_FEE
except ImportError:
    STABLECOIN_EQUIVALENTS = frozenset(
        {"USDT", "USD", "USDC", "FDUSD", "TUSD", "DAI"}
    )
    DEFAULT_TAKER_FEE = 0.001


DISCOUNT_TOKEN_FALLBACK_RATE = DEFAULT_TAKER_FEE


def fee_to_usdt(fee_dict: dict, order_dict: dict,
                base_override: str = "") -> float:
    """Convert a single CCXT fee dict to USDT.

    Rules in order:
      1. Stablecoin currency  return cost directly.
      2. Currency matches base coin  cost  fill price.
      3. Unknown discount token  conservative estimate.
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

    if not currency or currency in STABLECOIN_EQUIVALENTS:
        return cost

    base = base_override.upper() if base_override else ""
    if not base and isinstance(order_dict, dict):
        symbol = order_dict.get("symbol") or ""
        if "/" in symbol:
            base = symbol.split("/")[0].upper()
        elif symbol and symbol.endswith("USDT"):
            base = symbol[:-4].upper()

    if currency == base:
        fill_price = _safe_fill_price(order_dict)
        if fill_price > 0:
            return cost * fill_price

    return _discount_fallback(order_dict)


def extract_fee_usdt(order: dict, base_override: str = "") -> float:
    """Extract total fees from a CCXT order dict, in USDT.
    Avoids double-counting by using `fees` list only when non-empty
    with valid entries (else falls back to singular `fee`)."""
    if not isinstance(order, dict):
        return 0.0

    fees_list = order.get("fees") or []
    if isinstance(fees_list, list):
        valid_entries = [f for f in fees_list if isinstance(f, dict)]
        if valid_entries:
            return sum(
                fee_to_usdt(f, order, base_override) for f in valid_entries
            )

    return fee_to_usdt(order.get("fee"), order, base_override)


def estimate_fee_usdt(amount_coins: float, fill_price: float,
                      taker_rate: Optional[float] = None) -> float:
    """Estimate the USDT fee when the exchange didn't return one."""
    if taker_rate is None:
        taker_rate = DEFAULT_TAKER_FEE
    try:
        amt = max(0.0, float(amount_coins))
        px  = max(0.0, float(fill_price))
    except (TypeError, ValueError):
        return 0.0
    return round(amt * px * max(0.0, taker_rate), 6)


def extract_or_estimate(order: dict, fill_price: float,
                        taker_rate: Optional[float] = None,
                        base_override: str = "") -> float:
    """Real extracted fee if > 0; otherwise estimate as fallback."""
    real = extract_fee_usdt(order, base_override)
    if real > 0:
        return real
    filled = float(order.get("filled") or order.get("amount") or 0)
    if taker_rate is None:
        taker_rate = DEFAULT_TAKER_FEE
    return estimate_fee_usdt(filled, fill_price, taker_rate)


def extract_or_estimate_with_refetch(ex, order: dict, symbol_full: str,
                                      fill_price: float,
                                      taker_rate: Optional[float] = None,
                                      base_override: str = "",
                                      max_attempts: int = 2,
                                      retry_delay: float = 0.4) -> float:
    """
    Wenn `extract_fee_usdt(order) == 0`, ldt das Order via `fetch_order(id)`
    neu bevor geschtzt wird. Bitget liefert bei Market-Orders erste Response
    ohne Fee-Info; nach ~300-500ms hat die Order finale Fee-Daten.

    Args:
        ex: ccxt exchange
        order: das CCXT order dict aus create_order/create_market_*
        symbol_full: e.g. "BTC/USDT:USDT"
        fill_price: actual fill price for estimate fallback
        taker_rate: optional override
        base_override: e.g. "BTC"
        max_attempts: re-fetches versucht
        retry_delay: Sekunden zwischen Re-Fetches

    Returns:
        Fee in USDT (real when extractable, otherwise estimated).
    """
    import time as _time

    real = extract_fee_usdt(order, base_override)
    if real > 0:
        return real

    # Re-fetch once the exchange has had time to attach fee details.
    order_id = order.get("id") or order.get("orderId")
    if order_id and ex is not None and symbol_full:
        for attempt in range(max_attempts):
            try:
                _time.sleep(retry_delay)
                refreshed = ex.fetch_order(str(order_id), symbol_full)
                if refreshed:
                    real = extract_fee_usdt(refreshed, base_override)
                    if real > 0:
                        return real
            except Exception:
                continue

    # Letzter Resort: estimate
    filled = float(order.get("filled") or order.get("amount") or 0)
    if taker_rate is None:
        taker_rate = DEFAULT_TAKER_FEE
    return estimate_fee_usdt(filled, fill_price, taker_rate)


def base_currency_fee_amount(order: dict, base_currency: str) -> float:
    """Return total fee amount paid in the BASE currency (z.B. PEPE).
    Was Bots subtrahieren um real coin balance nach Trade zu berechnen."""
    if not isinstance(order, dict) or not base_currency:
        return 0.0

    base_upper = base_currency.upper()
    total = 0.0

    sources = []
    fees_list = order.get("fees") or []
    if isinstance(fees_list, list):
        valid = [f for f in fees_list if isinstance(f, dict)]
        if valid:
            sources = valid

    if not sources:
        single = order.get("fee")
        if isinstance(single, dict):
            sources = [single]

    for fee in sources:
        try:
            currency = (fee.get("currency") or "").upper()
            if currency == base_upper:
                cost = abs(float(fee.get("cost", 0) or 0))
                if cost > 0:
                    total += cost
        except (TypeError, ValueError):
            continue

    return total


def _safe_fill_price(order_dict: dict) -> float:
    """Read fill price from order dict, preferring `average`."""
    if not isinstance(order_dict, dict):
        return 0.0
    for key in ("average", "price"):
        val = order_dict.get(key)
        try:
            fv = float(val) if val is not None else 0.0
            if fv > 0:
                return fv
        except (TypeError, ValueError):
            continue
    try:
        cost = float(order_dict.get("cost") or 0)
        filled = float(order_dict.get("filled") or 0)
        if cost > 0 and filled > 0:
            return cost / filled
    except (TypeError, ValueError):
        pass
    return 0.0


def _discount_fallback(order_dict: dict) -> float:
    """Conservative fee estimate when currency is unknown discount token."""
    if not isinstance(order_dict, dict):
        return 0.0
    filled = float(order_dict.get("filled") or order_dict.get("amount") or 0)
    price  = _safe_fill_price(order_dict)
    if filled > 0 and price > 0:
        return round(filled * price * DISCOUNT_TOKEN_FALLBACK_RATE, 6)
    return 0.0
