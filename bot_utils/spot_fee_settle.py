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

import time
from typing import Optional

from bot_utils.order_utils import extract_base_fee_amount


# Conservative default taker fee  Bitget/Binance spot is 0.1%.
# We use this only as last-resort estimate when the exchange never
# settles the fee within the refetch window.
SPOT_DEFAULT_TAKER_FEE = 0.001


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
    fee = extract_base_fee_amount(order, base_symbol)
    if fee > 0:
        return fee

    # Step 2: refetch loop (cancellable)
    order_id = order.get("id") if isinstance(order, dict) else None
    if order_id and ex is not None and symbol_pair:
        for _ in range(max_attempts):
            # Cancellable sleep  exit early on SIGTERM
            if shutdown_event is not None:
                if shutdown_event.wait(timeout=retry_delay):
                    break
            else:
                time.sleep(retry_delay)
            try:
                refreshed = ex.fetch_order(str(order_id), symbol_pair)
                if isinstance(refreshed, dict):
                    fee = extract_base_fee_amount(refreshed, base_symbol)
                    if fee > 0:
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
    try:
        filled = float(
            order.get("filled")
            or order.get("amount")
            or fallback_filled
            or 0
        )
    except (TypeError, ValueError):
        filled = 0.0
    if filled <= 0:
        return 0.0

    # Conservative over-estimate: assume the fee WAS charged in base
    # coin even if we couldn't verify. Worst case we record a slightly
    # too-small amount, which means a later sell tries to sell SLIGHTLY
    # less than we own  that's safe (leaves dust on exchange).
    estimated = filled * max(0.0, taker_rate)
    if log_event:
        try:
            log_event(
                f"spot fee for {symbol_pair} not settled after "
                f"{max_attempts} refetches  using {taker_rate*100:.3f}% "
                f"estimate ({estimated:.8f} {base_symbol}) to avoid "
                f"InsufficientBalance death loop", "WARN"
            )
        except Exception:
            pass
    return estimated
