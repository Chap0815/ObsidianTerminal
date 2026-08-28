"""
bot_utils/spot_fee_settle.py  Async-settle-aware base fee extraction.

Bitget and Binance settle the base-currency fee asynchronously  the fee field
is often 0 in the first order response and populated 200-500ms later. Recording
amount = gross_filled then makes later sells try to sell more than the wallet
holds  InsufficientBalance  endless retry  position stuck open. This refetches
the order a few times with a small delay, then estimates from the taker rate as a
last resort (mirrors ``extract_or_estimate_futures_fee``).
"""
from __future__ import annotations

import math
import time

from bot_utils.api_budget import try_consume_api_call
from bot_utils.order_utils import extract_base_fee_amount, order_id_text_or_none


# Conservative default taker fee  Bitget/Binance spot is 0.1%.
# We use this only as last-resort estimate when the exchange never
# settles the fee within the refetch window.
SPOT_DEFAULT_TAKER_FEE = 0.001


def _order_id_text(value) -> str:
    """Return a fetch_order-safe order id string, or empty for bogus ids."""
    return order_id_text_or_none(value) or ""


def _order_id_for_refetch(order: dict) -> str:
    if not isinstance(order, dict):
        return ""
    candidates = [
        order.get("id"),
        order.get("orderId"),
        order.get("order_id"),
    ]
    info = order.get("info")
    if isinstance(info, dict):
        candidates.extend([
            info.get("orderId"),
            info.get("order_id"),
            info.get("orderID"),
            info.get("id"),
        ])
    for candidate in candidates:
        order_id = _order_id_text(candidate)
        if order_id:
            return order_id
    return ""


def _extract_base_fee_known(order: dict, base_symbol: str) -> tuple[float, bool]:
    """Return base fee and whether settled currency evidence proves it."""
    fee = extract_base_fee_amount(order, base_symbol)
    if fee > 0:
        return fee, True
    if not isinstance(order, dict) or not isinstance(base_symbol, str):
        return 0.0, False
    base_upper = base_symbol.strip().upper()
    if not base_upper:
        return 0.0, False

    sources = []
    fees = order.get("fees")
    if isinstance(fees, list):
        sources.extend(item for item in fees if isinstance(item, dict))
    singular = order.get("fee")
    if isinstance(singular, dict):
        sources.append(singular)

    for fee_dict in sources:
        raw_currency = fee_dict.get("currency")
        if not isinstance(raw_currency, str) or not raw_currency.strip():
            continue
        raw_cost = fee_dict.get("cost")
        if isinstance(raw_cost, bool):
            continue
        try:
            cost = float(raw_cost)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(cost) and cost != 0:
            # A settled non-base fee proves no base deduction. A negative base
            # rebate is conservatively treated as zero so callers never
            # fabricate an additional deduction or overstate sellable coins.
            return 0.0, True
    return 0.0, False


def extract_or_estimate_base_fee(ex,
                                    order: dict,
                                    symbol_pair: str,
                                    base_symbol: str,
                                    *,
                                    max_attempts: int = 3,
                                    retry_delay: float = 0.3,
                                    taker_rate: float = SPOT_DEFAULT_TAKER_FEE,
                                    fallback_filled: float = 0.0,
                                    log_event=None,
                                    shutdown_event=None,
                                    ) -> float:
    """Resolve the BASE-currency fee paid on a market buy.

    Three-tier strategy:
      1. Read from the order dict directly.
      2. If 0, refetch via ``ex.fetch_order(id)`` up to ``max_attempts``
         times (with cancellable sleep).
      3. As a last resort, estimate ``filled * taker_rate``.

    Returns the amount of BASE coin paid as fee. Callers MUST subtract
    this from the recorded position amount to prevent the
    InsufficientBalance death loop.

    Note: this is the SAME function shape as futures'
    ``extract_or_estimate_futures_fee`` but for SPOT base-currency
    fees (i.e. when the wallet receives ``filled - fee_in_base`` of
    the bought coin instead of the gross filled amount).
    """
    # Step 1: direct extract
    fee, fee_known = _extract_base_fee_known(order, base_symbol)
    if fee_known:
        return fee

    # Step 2: refetch loop (cancellable)
    order_id = _order_id_for_refetch(order)
    if order_id and ex is not None and symbol_pair:
        for _ in range(max_attempts):
            # Cancellable sleep  exit early on SIGTERM
            if shutdown_event is not None:
                if shutdown_event.wait(timeout=retry_delay):
                    break
            else:
                time.sleep(retry_delay)
            try:
                allowed = try_consume_api_call(
                    "spot_fee_fetch_order", critical=True
                )
            except Exception:
                break
            if not allowed:
                break
            try:
                refreshed = ex.fetch_order(order_id, symbol_pair)
                if isinstance(refreshed, dict):
                    fee, fee_known = _extract_base_fee_known(
                        refreshed, base_symbol
                    )
                    if fee_known:
                        return fee
            except Exception as e:
                if log_event:
                    try:
                        log_event(
                            f"spot fee refetch failed for {symbol_pair}: "
                            f"{type(e).__name__}", "WARN"
                        )
                    except Exception:
                        pass

    # Step 3: estimate. The exchange WILL have charged a fee  we just
    # can't read it yet. Assuming the standard taker rate is FAR safer
    # than recording the gross amount (which leads to InsufficientBalance).
    filled = 0.0
    for candidate in (
        order.get("filled") if isinstance(order, dict) else None,
        order.get("amount") if isinstance(order, dict) else None,
        fallback_filled,
    ):
        if candidate is None:
            continue
        try:
            parsed = float(candidate)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(parsed) and parsed > 0:
            filled = parsed
            break
    if not math.isfinite(filled) or filled <= 0:
        return 0.0
    try:
        rate = float(taker_rate)
    except (TypeError, ValueError, OverflowError):
        rate = SPOT_DEFAULT_TAKER_FEE
    if not math.isfinite(rate) or rate < 0:
        rate = SPOT_DEFAULT_TAKER_FEE

    # Conservative over-estimate: assume the fee WAS charged in base
    # coin even if we couldn't verify. Worst case we record a slightly
    # too-small amount, which means a later sell tries to sell SLIGHTLY
    # less than we own  that's safe (leaves dust on exchange).
    estimated = filled * rate
    if not math.isfinite(estimated):
        estimated = 0.0
    if log_event:
        try:
            log_event(
                f"spot fee for {symbol_pair} not settled after "
                f"{max_attempts} refetches  using {rate*100:.3f}% "
                f"estimate ({estimated:.8f} {base_symbol}) to avoid "
                f"InsufficientBalance death loop", "WARN"
            )
        except Exception:
            pass
    return estimated
