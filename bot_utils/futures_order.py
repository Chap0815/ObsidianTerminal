"""
bot_utils/futures_order.py  Futures-specific order helpers.

Extracted from main_bot_futures.py:
  create_order_with_retry  exponential-backoff order placement
  classify_order_state  CCXT status  bot state machine
  is_terminal_order_state
  convert_fee_to_usdt_futures  futures fees handle inverse contracts
  extract_order_fee_futures
  verify_position_closed  confirm contracts==0 after close
  get_maintenance_margin_rate
  get_exchange_liq_price
  Adaptive retry backoff (module-level)
"""
from __future__ import annotations

import re
import threading
import time
from typing import Tuple, Optional

from bot_utils.api_budget import record_api_call


def _utc_now_str() -> str:
    """Lazy UTC timestamp helper (avoids circular import)."""
    from core.logger import _date
    return _date()


#  Order state machine 

ORDER_STATE_CREATED = "created"
ORDER_STATE_SUBMITTED = "submitted"
ORDER_STATE_ACKNOWLEDGED = "acknowledged"
ORDER_STATE_PARTIALLY_FILLED = "partially_filled"
ORDER_STATE_FILLED = "filled"
ORDER_STATE_FAILED = "failed"
ORDER_STATE_CANCELED = "canceled"
ORDER_STATE_EXPIRED = "expired"

# Fraction of the order amount that must be filled to count as FILLED.
# Conservative default: 0.9999 (1bp tolerance).
ORDER_FILL_THRESHOLD = 0.9999


def classify_order_state(order) -> str:
    """Map a CCXT order dict to a bot state-machine string."""
    if not isinstance(order, dict):
        return ORDER_STATE_FAILED
    status = (order.get("status") or "").lower()
    try:
        filled = float(order.get("filled") or 0)
    except (TypeError, ValueError):
        filled = 0.0
    try:
        amount = float(order.get("amount") or 0)
    except (TypeError, ValueError):
        amount = 0.0

    if status == "closed":
        return ORDER_STATE_FILLED
    if status in ("canceled", "cancelled"):
        return ORDER_STATE_CANCELED
    if status == "expired":
        return ORDER_STATE_EXPIRED
    if status == "rejected":
        return ORDER_STATE_FAILED
    if status == "open":
        if filled > 0 and amount > 0 and filled < amount:
            return ORDER_STATE_PARTIALLY_FILLED
        return ORDER_STATE_ACKNOWLEDGED

    if amount > 0 and filled >= amount * ORDER_FILL_THRESHOLD:
        return ORDER_STATE_FILLED
    if filled > 0:
        return ORDER_STATE_PARTIALLY_FILLED
    return ORDER_STATE_SUBMITTED


def is_terminal_order_state(state: str) -> bool:
    return state in (ORDER_STATE_FILLED, ORDER_STATE_FAILED,
                     ORDER_STATE_CANCELED, ORDER_STATE_EXPIRED)


#  Adaptive retry backoff 

_RETRY_FAIL_WINDOW = 60.0
_retry_fail_log: list = []
_retry_fail_lock = threading.Lock()


def _record_retry_failure() -> None:
    with _retry_fail_lock:
        _retry_fail_log.append(time.monotonic())


def _retry_backoff_multiplier() -> float:
    """1-8 multiplier based on transient-failure density in last 60s."""
    now = time.monotonic()
    with _retry_fail_lock:
        cutoff = now - _RETRY_FAIL_WINDOW
        _retry_fail_log[:] = [t for t in _retry_fail_log if t >= cutoff]
        n = len(_retry_fail_log)
    if n <= 2:
        return 1.0
    if n <= 4:
        return 2.0
    if n <= 6:
        return 4.0
    return 8.0


# Permanent (non-retryable) order-error patterns, compiled once at module
# load into a single regex.
_PERMANENT_PATTERN_STRINGS = (
    r"insufficient\s+balance",
    r"insufficient\s+funds",
    r"insufficient\s+margin",
    r"invalid\s+symbol",
    r"symbol\s+not",
  r"market\s+not\s+(?:found|exist)",
    r"does\s+not\s+exist",
    r"min\s+notional",
    r"below\s+min",
    r"precision",
    r"lot\s+size",
    r"exceeds\s+maximum",
    r"reduce[-\s]only",
    r"position\s+not\s+exist",
    r"position\s+is\s+nonexistent",
    r"nonexistent\s+or\s+closed",
    r"\b2009\b",
    r"no\s+open\s+position",
  # MEXC 8823  pair being DELISTED, "new positions cannot be opened".
    # Retrying never helps (exchange-side block); fail fast and skip the leg.
    r"\b8823\b",
    r"will\s+be\s+delisted",
    r"new\s+positions\s+cannot\s+be\s+opened",
)
_PERMANENT_PATTERN_RE = re.compile(
    "|".join(f"(?:{p})" for p in _PERMANENT_PATTERN_STRINGS),
    re.IGNORECASE
)


def _is_permanent_error(err_str: str) -> bool:
    """True if the error message matches a non-retryable pattern."""
    return bool(_PERMANENT_PATTERN_RE.search(err_str))


_NO_POSITION_PATTERN_RE = re.compile(
    r"position\s+(?:does\s+not\s+exist|not\s+exist|not\s+found)"
    r"|position\s+is\s+nonexistent"
    r"|nonexistent\s+or\s+closed"
    r"|no\s+(?:open\s+)?position"
    r"|zero\s+position"
    r"|\b2009\b",
    re.IGNORECASE,
)


def is_no_position_error(exc_or_text) -> bool:
    """True only for exchange errors that imply the position is already flat.

    This is narrower than the generic permanent-order classifier: precision,
    min-notional, balance, or symbol errors are permanent too, but must not
    trigger local close accounting/cleanup.
    """
    return bool(_NO_POSITION_PATTERN_RE.search(str(exc_or_text or "")))


def _is_rate_limit_error(err_str: str) -> bool:
    """MEXC code 510 / 429 / 'too frequent' - transient; needs a longer wait."""
    s = err_str.lower()
    return ("510" in s or "too frequent" in s or "too many request" in s
            or "rate limit" in s or "ratelimit" in s or "429" in s)


# Exchange-specific keys that carry the client order id inside the raw
# ``info`` payload. CCXT normalises most into top-level ``clientOrderId``, but
# not every exchange/version does  when it doesn't, matching only the
# top-level key silently fails and the retry can open a SECOND position. We
# also scan these nested aliases so the idempotency guard holds on more venues.
_CLIENT_ID_INFO_KEYS = (
    "clientOrderId", "clientOid", "client_oid", "newClientOrderId",
    "origClientOrderId", "clientOrderID", "cl_ord_id", "clOrdId",
)


def _order_client_id_matches(o: dict, cid: str) -> bool:
    """True if order dict ``o`` carries client id ``cid`` - top-level first,
    then the raw ``info`` payload under known per-exchange aliases."""
    if not isinstance(o, dict):
        return False
    if o.get("clientOrderId") == cid:
        return True
    info = o.get("info")
    if isinstance(info, dict):
        for k in _CLIENT_ID_INFO_KEYS:
            if info.get(k) == cid:
                return True
    return False


def _order_landed(o: dict) -> bool:
    """True if a recovered order should be treated as already placed.

    Accept it as "landed" when it filled, OR is live/accepted (status
    open/new/closed/partially_filled), OR carries a real exchange id - but
    NEVER when it is genuinely rejected/canceled/expired. MEXC/Bitget can
    report filled=0 for hundreds of ms after an order actually landed, so a
    filled>0-only check would let a retry fire a duplicate."""
    if not isinstance(o, dict):
        return False
    status = (o.get("status") or "").lower()
    if status in ("rejected", "canceled", "cancelled", "expired"):
        return False
    try:
        if float(o.get("filled") or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    if status in ("open", "new", "closed", "partially_filled", "partiallyfilled"):
        return True
    oid = o.get("id") or o.get("orderId")
    return bool(oid)


def _find_order_by_client_id(ex, symbol_full: str, cid: str, log_event=None):
    """Locate an order by clientOrderId - open orders first, then recent
    history. Used to recover from a lost-response timeout so a retry doesn't
    open a SECOND position. Best-effort; never raises. Returns dict or None.

    Matches both the CCXT-normalised ``clientOrderId`` and the raw ``info``
    aliases (``clOrdId``, ``newClientOrderId``, ...) so the guard works on
    exchanges that don't surface the id at the top level.

    A failed lookup is logged (when ``log_event`` is supplied) rather than
    silently swallowed: it means the duplicate-order guard could not be
    verified, so the caller falls back to clientOrderId server-side dedup."""
    if not cid:
        return None
    try:
        for o in (ex.fetch_open_orders(symbol_full) or []):
            if _order_client_id_matches(o, cid):
                return o
    except Exception as e:
        if log_event:
            log_event(f"clientOrderId lookup (open orders) failed for "
                      f"{symbol_full}: {type(e).__name__} - relying on "
                      f"server-side dedup", "WARN")
    has = getattr(ex, "has", {}) or {}
    try:
        if has.get("fetchOrders"):
            for o in (ex.fetch_orders(symbol_full, limit=20) or []):
                if _order_client_id_matches(o, cid):
                    return o
    except Exception as e:
        if log_event:
            log_event(f"clientOrderId lookup (history) failed for "
                      f"{symbol_full}: {type(e).__name__}", "WARN")
    # A just-filled MARKET order is no longer "open", and several supported
  # venues (bitget  the DEFAULT  okx, bybit, kucoin, gate) don't expose the
    # unified fetchOrders. Such a fill surfaces in closed orders / my-trades, so
  # consult those before giving up  otherwise the lost-response retry fires a
    # SECOND entry in exactly the fill-but-no-ack window this guard exists for.
    try:
        if has.get("fetchClosedOrders"):
            for o in (ex.fetch_closed_orders(symbol_full, limit=20) or []):
                if _order_client_id_matches(o, cid):
                    return o
    except Exception as e:
        if log_event:
            log_event(f"clientOrderId lookup (closed orders) failed for "
                      f"{symbol_full}: {type(e).__name__}", "WARN")
    try:
        if has.get("fetchMyTrades"):
            for t in (ex.fetch_my_trades(symbol_full, limit=20) or []):
                if _order_client_id_matches(t, cid):
                    return t
    except Exception as e:
        if log_event:
            log_event(f"clientOrderId lookup (my trades) failed for "
                      f"{symbol_full}: {type(e).__name__}", "WARN")
    return None


def create_order_with_retry(ex,
                              symbol_full: str,
                              side: str,
                              amount: float,
                              params: dict,
                              shutdown_event: threading.Event,
                              max_attempts: int = 3,
                              action_label: str = "order",
                              log_event=None,
                              log_struct=None,
                              abort_on_shutdown: bool = True) -> dict:
    """Place a market order with exponential backoff retry."""
    last_err = None
    t_start = time.monotonic()
    # Idempotency for EVERY caller: ensure a clientOrderId so the lost-response
    # recovery below also protects the close/emergency paths (which pass
    # reduce_only_params and supply no cid). Without one, a transient timeout
    # that hid a fill makes the retry double the action (over-close / phantom
    # size). Copy the dict so the caller's is untouched; stable across THIS
    # call's internal retries.
    if isinstance(params, dict) and not params.get("clientOrderId"):
        import uuid as _uuid
        params = dict(params)
        params["clientOrderId"] = "obx-" + _uuid.uuid4().hex[:20]
    for attempt in range(1, max_attempts + 1):
        try:
            order = ex.create_order(symbol_full, "market", side, amount, params=params)
            record_api_call()
            order_state = classify_order_state(order)
            if isinstance(order, dict):
                order["_bot_state"] = order_state
                order["_bot_attempts"] = attempt
                order["_bot_placed_at"] = _utc_now_str()
            if log_struct:
                try:
                    latency_ms = int((time.monotonic() - t_start) * 1000)
                    log_struct(
                        "order_placed",
                        symbol=symbol_full, side=side, amount=amount,
                        attempts=attempt, latency_ms=latency_ms,
                        state=order_state,
                        order_id=order.get("id") if isinstance(order, dict) else None,
                        filled=order.get("filled") if isinstance(order, dict) else None,
                        avg_price=order.get("average") if isinstance(order, dict) else None,
                        action=action_label,
                    )
                except Exception:
                    pass
            return order
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            is_permanent = _is_permanent_error(err_str)
            if is_permanent:
                if log_struct:
                    try:
                        latency_ms = int((time.monotonic() - t_start) * 1000)
                        log_struct(
                            "order_failed",
                            symbol=symbol_full, side=side, amount=amount,
                            attempts=attempt, latency_ms=latency_ms,
                            error_type=type(e).__name__,
                            error_msg=str(e)[:200],
                            permanent=True, action=action_label,
                        )
                    except Exception:
                        pass
                raise
            # Before retrying, check whether the order actually LANDED under
            # our clientOrderId. create_order has no idempotency of its own, so
            # a lost-response timeout that hid a successful fill would otherwise
            # cause the retry to open a SECOND position. If the prior attempt
            # filled, return it instead of re-firing.
            cid = params.get("clientOrderId") if isinstance(params, dict) else None
            if cid:
                existing = _find_order_by_client_id(ex, symbol_full, cid,
                                                    log_event=log_event)
                if existing is not None and _order_landed(existing):
                    if log_event:
                        try:
                            log_event(
                                f"{action_label}: recovered already-landed order "
  f"via clientOrderId after {type(e).__name__}  "
                                f"NOT retrying (prevents duplicate position)",
                                "WARN")
                        except Exception:
                            pass
                    return existing
            if attempt < max_attempts:
                _record_retry_failure()
                multiplier = _retry_backoff_multiplier()
                wait = 0.5 * (2 ** (attempt - 1)) * multiplier
                if _is_rate_limit_error(err_str):
                    wait = max(wait, 1.5 * (2 ** (attempt - 1)))
                if log_event:
                    log_event(
                        f"{action_label} attempt {attempt}/{max_attempts} "
  f"failed: {e}  retry in {wait:.1f}s "
  f"(backoff {multiplier:.0f})",
                        "WARN"
                    )
                if abort_on_shutdown:
                    if shutdown_event.wait(timeout=wait):
                        raise last_err
                else:
                    time.sleep(wait)
    if log_struct:
        try:
            latency_ms = int((time.monotonic() - t_start) * 1000)
            log_struct(
                "order_failed",
                symbol=symbol_full, side=side, amount=amount,
                attempts=max_attempts, latency_ms=latency_ms,
                error_type=type(last_err).__name__ if last_err else "Unknown",
                error_msg=str(last_err)[:200] if last_err else "",
                permanent=False, action=action_label,
            )
        except Exception:
            pass
    raise last_err


#  Fee extraction (futures variant  fees in USDT, not base) 

_FUTURES_DISCOUNT_TOKENS = ("MX", "BNB", "BGB", "OKB", "HT", "KCS", "GT")


def convert_fee_to_usdt_futures(fee_dict, order_dict) -> float:
    """Futures fees are typically quoted in USDT directly.

  Discount-token fees (MEXC MX, Binance BNB, ) are paid in a non-USDT coin;
    rather than discard them, best-effort convert to USDT via that token's
    current price, falling back to a notional-based estimate (mirrors the spot
  path's discount fallback). Any failure  notional estimate.
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
    if currency in _FUTURES_DISCOUNT_TOKENS:
        return _discount_token_fee_to_usdt(currency, cost, order_dict)
    return 0.0


def _discount_token_fee_to_usdt(currency: str, cost: float,
                                order_dict) -> float:
    """Convert a fee paid in a discount token to USDT (best-effort).

    Prefers the token's current price (``ex.fetch_ticker('<TOKEN>/USDT')`` via
    the exchange stashed on the order); falls back to a notional-based estimate
    using the order's own fill. Returns 0.0 only when nothing is usable."""
    if not isinstance(order_dict, dict):
        return 0.0
    ex = order_dict.get("_bot_ex")
    if ex is not None:
        try:
            ticker = ex.fetch_ticker(f"{currency}/USDT")
            px = float((ticker or {}).get("last")
                       or (ticker or {}).get("close") or 0)
            if px > 0:
                return round(cost * px, 6)
        except Exception:
            pass
    try:
        filled = float(order_dict.get("filled")
                       or order_dict.get("amount") or 0)
        fp = float(order_dict.get("average") or order_dict.get("price") or 0)
        if filled > 0 and fp > 0:
            return round(filled * fp * FUTURES_DEFAULT_TAKER_FEE, 6)
    except (TypeError, ValueError):
        pass
    return 0.0


def extract_order_fee_futures(order) -> float:
    """Total fee paid for a futures order in USDT.

    Uses ``is not None`` for the cost-present check so legitimate cost=0
    maker rebates aren't silently dropped.
    """
    if not isinstance(order, dict):
        return 0.0
    fees_list = order.get("fees") or []
    if isinstance(fees_list, list):
        valid = [f for f in fees_list
                  if isinstance(f, dict) and f.get("cost") is not None]
        if valid:
            return sum(convert_fee_to_usdt_futures(f, order) for f in valid)
    return convert_fee_to_usdt_futures(order.get("fee") or {}, order)


FUTURES_DEFAULT_TAKER_FEE = 0.0006


def futures_contract_size(ex, symbol_full: str) -> float:
    """Contract size for a futures market from CCXT metadata (default 1.0).

    USDT-M perpetuals are usually 1, but some contracts (1000SATS, MEME, Bybit
  inverse, ) use 100/1000/etc. Any fee/notional math MUST multiply by this,
    otherwise the value is wrong by the contractSize factor.
    """
    try:
        markets = getattr(ex, "markets", None) or {}
        m = markets.get(symbol_full) or {}
        cs = m.get("contractSize") or m.get("contract_size")
        if cs is None:
            info = m.get("info") or {}
            cs = info.get("contractSize") or info.get("contract_size")
        if cs is None:
            return 1.0
        v = float(cs)
        return v if v > 0 else 1.0
    except (TypeError, ValueError, AttributeError):
        return 1.0


def filled_margin_usdt(amount: float,
                       contract_size: float,
                       fill_price: float,
                       leverage: float,
                       fallback_margin: float = 0.0) -> tuple[float, bool]:
    """Return margin implied by the actual filled futures position.

    Futures PnL math uses ``margin * leverage`` as notional. After a partial
    entry fill, storing the intended margin would overstate open PnL and risk
    gates. The exchange truth is contracts * contractSize * fill / leverage.
    """
    try:
        amt = float(amount)
        cs = float(contract_size)
        px = float(fill_price)
        lev = float(leverage)
        raw = (amt * cs * px) / lev if lev > 0 else 0.0
        if raw > 0:
            return raw, True
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    try:
        return float(fallback_margin or 0.0), False
    except (TypeError, ValueError):
        return 0.0, False


def extract_or_estimate_futures_fee(ex,
                                      order: dict,
                                      symbol_full: str,
                                      fill_price: float,
                                      taker_rate: float = FUTURES_DEFAULT_TAKER_FEE,
                                      amount: Optional[float] = None,
                                      contract_size: Optional[float] = None,
                                      max_attempts: int = 2,
                                      retry_delay: float = 0.4,
                                      shutdown_event: Optional[threading.Event] = None
                                      ) -> float:
    """Futures equivalent of spot's `extract_or_estimate_with_refetch`.

    The optional ``shutdown_event`` aborts the refetch loop early when the bot
    is shutting down (otherwise the sleep would block SIGTERM).

    The estimate fallback
      (a) multiplies by contractSize, so contract_size != 1 coins (1000SATS,
          MEME, ...) are priced by that factor; and
      (b) falls back to the caller-supplied ``amount`` when the order dict has no
          ``filled``. MEXC/Bitget routinely return filled=0 on the immediate
          market-order response (the fill settles a few hundred ms later).
    ``contract_size`` is auto-derived from ``ex`` when not provided, so even
    callers that don't pass it are correct.
    """
    real = extract_order_fee_futures(order)
    if real > 0:
        return real

    order_id = order.get("id") or order.get("orderId")
    if order_id and ex is not None and symbol_full:
        for _ in range(max_attempts):
            # Cancellable sleep
            if shutdown_event is not None:
                if shutdown_event.wait(timeout=retry_delay):
                    break  # shutting down - accept estimate
            else:
                time.sleep(retry_delay)
            try:
                refreshed = ex.fetch_order(str(order_id), symbol_full)
                if isinstance(refreshed, dict):
                    real = extract_order_fee_futures(refreshed)
                    if real > 0:
                        return real
            except Exception:
                continue

    # Estimate fallback
    # Prefer the order's own fill; fall back to what the CALLER actually traded
    # when the response carries no fill yet. Notional ALWAYS includes contractSize.
    try:
        filled = float(order.get("filled") or order.get("amount") or 0)
    except (TypeError, ValueError):
        filled = 0.0
    if filled <= 0 and amount is not None:
        try:
            filled = float(amount)
        except (TypeError, ValueError):
            filled = 0.0
    if contract_size is None:
        contract_size = futures_contract_size(ex, symbol_full)
    try:
        cs = float(contract_size)
        if cs <= 0:
            cs = 1.0
    except (TypeError, ValueError):
        cs = 1.0
    try:
        fp = float(fill_price)
    except (TypeError, ValueError):
        fp = 0.0
    if filled <= 0 or fp <= 0:
        return 0.0
    return round(filled * cs * fp * max(0.0, taker_rate), 6)


#  Position verification 

def verify_position_closed(ex, symbol_full: str, timeout: float = 5.0
                            ) -> Tuple[bool, float]:
    """Confirm via fetch_positions that contracts == 0 after a close.

    The ``timeout`` parameter is enforced via a monotonic deadline.

    Returns (closed, remaining_contracts).
    remaining_contracts == -1.0 indicates verification itself failed
  (auth/network/timeout)  caller should keep state and retry. When
    ``safe_fetch_positions`` returns None (auth/network glitch), return
  ``(False, -1.0)``  same pessimistic signal as an exception.
    """
    deadline = time.monotonic() + max(0.0, timeout)
    try:
        from config.exchange_config import safe_fetch_positions
    except Exception:
        return False, -1.0

    # Do at most 2 attempts within the window, with a small gap. If both
    # exceed the deadline, report "not verified" so the caller keeps state.
    last_remaining = -1.0
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        try:
            record_api_call()
            positions = safe_fetch_positions(ex, [symbol_full])
            scoped_has_symbol = False
            if positions is not None:
                try:
                    scoped_has_symbol = any(
                        (p.get("symbol") or "") == symbol_full for p in positions
                    )
                except Exception:
                    scoped_has_symbol = False
            if positions is None or not scoped_has_symbol:
                global_positions = safe_fetch_positions(ex)
                positions = global_positions if global_positions is not None else None
            if positions is None:
  # Don't immediately give up  small backoff and retry
                # within remaining deadline.
                pass
            else:
                found_open = False
                for p in positions:
                    if (p.get("symbol") or "") != symbol_full:
                        continue
                    try:
                        contracts = abs(float(p.get("contracts") or p.get("size") or 0))
                        if contracts > 1e-8:
                            last_remaining = contracts
                            found_open = True
                            break
                    except (TypeError, ValueError):
                        continue
                if not found_open:
                    return True, 0.0
                last_remaining = float(last_remaining) if last_remaining > 0 else 0.0
        except Exception:
            pass

        # Small backoff between retries, capped by remaining deadline
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.5, max(0.05, remaining / 2)))

    # Deadline exhausted
    if last_remaining > 0:
        return False, last_remaining
    return False, -1.0


def get_maintenance_margin_rate(ex, symbol_full: str,
                                  default: float = 0.01) -> float:
    """Try to read the exchange's actual maintenance margin rate."""
    try:
        markets = getattr(ex, "markets", None) or {}
        m = markets.get(symbol_full) or {}
        for key in ("maintenanceMarginRate", "maintMarginRate",
                     "maintenance_margin", "mm"):
            v = m.get(key) or m.get("info", {}).get(key)
            if v is not None:
                fv = float(v)
                if 0 < fv < 0.5:
                    return fv
        return default
    except Exception:
        return default


def get_exchange_liq_price(ex, symbol_full: str) -> float:
    """Fetch exchange-reported liquidation price."""
    try:
        from config.exchange_config import safe_fetch_positions
        record_api_call()
        positions = safe_fetch_positions(ex)
        if positions is None:
            return 0.0
        for p in positions:
            if (p.get("symbol") or "") != symbol_full:
                continue
            for k in ("liquidationPrice", "liquidation_price"):
                v = p.get(k)
                if v is None and isinstance(p.get("info"), dict):
                    v = p["info"].get(k)
                if v is not None:
                    try:
                        fv = float(v)
                        if fv > 0:
                            return fv
                    except (TypeError, ValueError):
                        continue
        return 0.0
    except Exception:
        return 0.0
