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

import math
import re
import threading
import time
from typing import Tuple, Optional

from bot_utils.api_budget import record_api_call, try_consume_api_call


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


def _finite_nonnegative_order_value(value) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return parsed if math.isfinite(parsed) and parsed >= 0 else 0.0


def _normalize_order_symbol(symbol_full) -> str:
    if not isinstance(symbol_full, str):
        return ""
    return symbol_full.strip()


def _normalize_order_side(side) -> str:
    if not isinstance(side, str):
        return ""
    normalized = side.strip().lower()
    return normalized if normalized in {"buy", "sell"} else ""


def _normalize_order_status(status) -> str:
    if not isinstance(status, str):
        return ""
    return status.strip().lower()


def _normalize_max_attempts(max_attempts) -> int:
    if isinstance(max_attempts, bool):
        return 0
    try:
        attempts = int(max_attempts)
    except (TypeError, ValueError, OverflowError):
        return 0
    return attempts if attempts > 0 else 0


def _normalize_order_params(params):
    if params is None:
        return {}
    if not isinstance(params, dict):
        return None
    return dict(params)


def classify_order_state(order) -> str:
    """Map a CCXT order dict to a bot state-machine string."""
    if not isinstance(order, dict):
        return ORDER_STATE_FAILED
    status = _normalize_order_status(order.get("status"))
    filled = _finite_nonnegative_order_value(order.get("filled"))
    amount = _finite_nonnegative_order_value(order.get("amount"))

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
    r"|zero\s+position",
    re.IGNORECASE,
)

_RATE_LIMIT_CODE_RE = re.compile(
    r'(?:["\']?code["\']?\s*[:=]\s*510\b|\bcode\s+510\b)',
    re.IGNORECASE,
)


class _InvalidOrderResponse(RuntimeError):
    pass


def is_no_position_error(exc_or_text) -> bool:
    """True only for exchange errors that imply the position is already flat.

    This is narrower than the generic permanent-order classifier: precision,
    min-notional, balance, or symbol errors are permanent too, but must not
    trigger local close accounting/cleanup.
    """
    return bool(_NO_POSITION_PATTERN_RE.search(str(exc_or_text or "")))


def _is_rate_limit_error(err_str: str) -> bool:
    """MEXC code 510 / 429 / 'too frequent' - transient; needs a longer wait."""
    try:
        from bot_utils.network_retry import is_rate_limited
        return is_rate_limited(Exception(err_str))
    except Exception:
        s = err_str.lower()
        return ("too frequent" in s or "too many request" in s
                or "rate limit" in s or "ratelimit" in s or "429" in s
                or bool(_RATE_LIMIT_CODE_RE.search(s)))


# Exchange-specific keys that carry the client order id inside the raw
# ``info`` payload. CCXT normalises most into top-level ``clientOrderId``, but
# not every exchange/version does  when it doesn't, matching only the
# top-level key silently fails and the retry can open a SECOND position. We
# also scan these nested aliases so the idempotency guard holds on more venues.
_CLIENT_ID_INFO_KEYS = (
    "clientOrderId", "clientOid", "client_oid", "newClientOrderId",
    "origClientOrderId", "clientOrderID", "cl_ord_id", "clOrdId",
    "externalOid", "external_oid",
)


def _order_id_text(value) -> str:
    if value is None or isinstance(value, bool):
        return ""
    try:
        text = str(value).strip()
    except Exception:
        return ""
    return text


def _first_order_id_text(*values) -> str:
    for value in values:
        text = _order_id_text(value)
        if text:
            return text
    return ""


def _order_client_id_matches(o: dict, cid: str) -> bool:
    """True if order dict ``o`` carries client id ``cid`` - top-level first,
    then the raw ``info`` payload under known per-exchange aliases."""
    if not isinstance(o, dict):
        return False
    cid_text = _order_id_text(cid)
    if not cid_text:
        return False
    if _order_id_text(o.get("clientOrderId")) == cid_text:
        return True
    info = o.get("info")
    if isinstance(info, dict):
        for k in _CLIENT_ID_INFO_KEYS:
            if _order_id_text(info.get(k)) == cid_text:
                return True
    return False


def _order_response_has_evidence(order: dict) -> bool:
    """True when a create_order response contains minimal exchange evidence."""
    if not isinstance(order, dict):
        return False
    if _first_order_id_text(
        order.get("id"),
        order.get("orderId"),
        order.get("order_id"),
        order.get("orderID"),
        order.get("clientOrderId"),
    ):
        return True
    info = order.get("info")
    if isinstance(info, dict):
        if any(_order_id_text(info.get(k)) for k in _CLIENT_ID_INFO_KEYS):
            return True
        if _first_order_id_text(
            info.get("orderId"),
            info.get("order_id"),
            info.get("orderID"),
            info.get("id"),
        ):
            return True
    status = _normalize_order_status(order.get("status"))
    if status in (
        "open", "new", "closed", "filled", "partially_filled",
        "partiallyfilled", "canceled", "cancelled", "expired", "rejected",
    ):
        return True
    return (
        _finite_nonnegative_order_value(order.get("filled")) > 0
        or _finite_nonnegative_order_value(order.get("cost")) > 0
    )


def _order_response_has_fill_evidence(order: dict) -> bool:
    if not isinstance(order, dict):
        return False
    return (
        _finite_nonnegative_order_value(order.get("filled")) > 0
        or _finite_nonnegative_order_value(order.get("cost")) > 0
    )


def _order_landed(o: dict) -> bool:
    """True if a recovered order should be treated as already placed.

    Accept it as "landed" when it filled, OR is live/accepted (status
    open/new/closed/partially_filled), OR carries a real exchange id - but
    NEVER when it is genuinely rejected/canceled/expired. MEXC/Bitget can
    report filled=0 for hundreds of ms after an order actually landed, so a
    filled>0-only check would let a retry fire a duplicate."""
    if not isinstance(o, dict):
        return False
    status = _normalize_order_status(o.get("status"))
    if status in ("rejected", "canceled", "cancelled", "expired"):
        return False
    if _finite_nonnegative_order_value(o.get("filled")) > 0:
        return True
    if status in ("open", "new", "closed", "partially_filled", "partiallyfilled"):
        return True
    oid = _first_order_id_text(
        o.get("id"),
        o.get("orderId"),
        o.get("order_id"),
        o.get("orderID"),
    )
    if not oid:
        info = o.get("info")
        if isinstance(info, dict):
            oid = _first_order_id_text(
                info.get("id"),
                info.get("orderId"),
                info.get("order_id"),
                info.get("orderID"),
            )
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
    def _budgeted(endpoint: str, fn):
        try:
            from bot_utils.api_budget import try_consume_api_call
            try_consume_api_call(endpoint, critical=True)
        except Exception:
            pass
        return fn()
    # MEXC exposes an exact, venue-native lookup by externalOid. It is both
    # faster and safer than scanning a short recent-order window, so use it
    # before unified fallbacks.
    if _exchange_id(ex) == "mexc":
        native = getattr(
            ex, "contractPrivateGetOrderExternalSymbolExternalOid", None
        )
        market = (getattr(ex, "markets", None) or {}).get(symbol_full) or {}
        market_id = market.get("id")
        if callable(native) and market_id:
            try:
                raw = _budgeted(
                    "order_recovery_mexc_external_oid",
                    lambda: native({"symbol": market_id, "externalOid": cid}),
                )
                data = raw.get("data") if isinstance(raw, dict) else None
                if isinstance(data, dict) and _order_id_text(
                    data.get("externalOid") or data.get("external_oid")
                ) == _order_id_text(cid):
                    return {
                        "id": data.get("orderId") or data.get("id"),
                        "clientOrderId": cid,
                        "info": data,
                    }
            except Exception as e:
                if log_event:
                    try:
                        log_event(
                            "MEXC externalOid lookup failed for "
                            f"{symbol_full}: {type(e).__name__}",
                            "WARN",
                        )
                    except Exception:
                        pass
    try:
        for o in (_budgeted(
                "order_recovery_fetch_open_orders",
                lambda: ex.fetch_open_orders(symbol_full)) or []):
            if _order_client_id_matches(o, cid):
                return o
    except Exception as e:
        if log_event:
            try:
                log_event(f"clientOrderId lookup (open orders) failed for "
                          f"{symbol_full}: {type(e).__name__} - relying on "
                          f"server-side dedup", "WARN")
            except Exception:
                pass
    has = getattr(ex, "has", {}) or {}
    try:
        if has.get("fetchOrders"):
            for o in (_budgeted(
                    "order_recovery_fetch_orders",
                    lambda: ex.fetch_orders(symbol_full, limit=20)) or []):
                if _order_client_id_matches(o, cid):
                    return o
    except Exception as e:
        if log_event:
            try:
                log_event(f"clientOrderId lookup (history) failed for "
                          f"{symbol_full}: {type(e).__name__}", "WARN")
            except Exception:
                pass
    # A just-filled MARKET order is no longer "open", and several supported
  # venues (bitget  the DEFAULT  okx, bybit, kucoin, gate) don't expose the
    # unified fetchOrders. Such a fill surfaces in closed orders / my-trades, so
  # consult those before giving up  otherwise the lost-response retry fires a
    # SECOND entry in exactly the fill-but-no-ack window this guard exists for.
    try:
        if has.get("fetchClosedOrders"):
            for o in (_budgeted(
                    "order_recovery_fetch_closed_orders",
                    lambda: ex.fetch_closed_orders(symbol_full, limit=20)) or []):
                if _order_client_id_matches(o, cid):
                    return o
    except Exception as e:
        if log_event:
            try:
                log_event(f"clientOrderId lookup (closed orders) failed for "
                          f"{symbol_full}: {type(e).__name__}", "WARN")
            except Exception:
                pass
    try:
        if has.get("fetchMyTrades"):
            for t in (_budgeted(
                    "order_recovery_fetch_my_trades",
                    lambda: ex.fetch_my_trades(symbol_full, limit=20)) or []):
                if _order_client_id_matches(t, cid):
                    return t
    except Exception as e:
        if log_event:
            try:
                log_event(f"clientOrderId lookup (my trades) failed for "
                          f"{symbol_full}: {type(e).__name__}", "WARN")
            except Exception:
                pass
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
    order_symbol = _normalize_order_symbol(symbol_full)
    if not order_symbol:
        raise ValueError(
            f"invalid order symbol for {action_label}: {symbol_full!r}")
    order_side = _normalize_order_side(side)
    if not order_side:
        raise ValueError(f"invalid order side for {action_label}: {side!r}")
    order_amount = _finite_nonnegative_order_value(amount)
    if order_amount <= 0:
        raise ValueError(f"invalid order amount for {action_label}: {amount!r}")
    attempts_limit = _normalize_max_attempts(max_attempts)
    if attempts_limit <= 0:
        raise ValueError(
            f"invalid max_attempts for {action_label}: {max_attempts!r}")
    order_params = _normalize_order_params(params)
    if order_params is None:
        raise ValueError(f"invalid order params for {action_label}: {params!r}")
    # Idempotency for EVERY caller: ensure a clientOrderId so the lost-response
    # recovery below also protects the close/emergency paths (which pass
    # reduce_only_params and supply no cid). Without one, a transient timeout
    # that hid a fill makes the retry double the action (over-close / phantom
    # size). Copy the dict so the caller's is untouched; stable across THIS
    # call's internal retries.
    if not order_params.get("clientOrderId"):
        import uuid as _uuid
        order_params["clientOrderId"] = "obx-" + _uuid.uuid4().hex[:20]
    reduce_only = False
    raw_reduce = order_params.get("reduceOnly")
    reduce_only = (
        raw_reduce is True
        or str(raw_reduce).strip().lower() in ("1", "true", "yes")
    )
    endpoint = f"create_order:{action_label or 'order'}"
    for attempt in range(1, attempts_limit + 1):
        if not try_consume_api_call(endpoint, critical=reduce_only):
            raise RuntimeError(f"API budget exhausted before {action_label}")
        try:
            order = ex.create_order(
                order_symbol, "market", order_side, order_amount,
                params=order_params)
            if not _order_response_has_evidence(order):
                cid = order_params.get("clientOrderId")
                existing = _find_order_by_client_id(
                    ex, order_symbol, cid, log_event=log_event) if cid else None
                if existing is not None and _order_landed(existing):
                    if log_event:
                        try:
                            log_event(
                                f"{action_label}: recovered already-landed order "
                                "after malformed exchange response  NOT retrying",
                                "WARN")
                        except Exception:
                            pass
                    return existing
                raise _InvalidOrderResponse(
                    f"{action_label}: invalid exchange order response "
                    f"{type(order).__name__}; missing order id/status/fill "
                    "evidence; not retrying to avoid duplicate market order"
                )
            order_state = classify_order_state(order)
            if (
                order_state in (
                    ORDER_STATE_FAILED,
                    ORDER_STATE_CANCELED,
                    ORDER_STATE_EXPIRED,
                )
                and not _order_response_has_fill_evidence(order)
            ):
                raise _InvalidOrderResponse(
                    f"{action_label}: exchange returned terminal order state "
                    f"{order_state}; not treating it as placed"
                )
            if isinstance(order, dict):
                order["_bot_state"] = order_state
                order["_bot_attempts"] = attempt
                order["_bot_placed_at"] = _utc_now_str()
            if log_struct:
                try:
                    latency_ms = int((time.monotonic() - t_start) * 1000)
                    log_struct(
                        "order_placed",
                        symbol=order_symbol, side=order_side,
                        amount=order_amount,
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
            if isinstance(e, _InvalidOrderResponse):
                if log_struct:
                    try:
                        latency_ms = int((time.monotonic() - t_start) * 1000)
                        log_struct(
                            "order_failed",
                            symbol=order_symbol, side=order_side,
                            amount=order_amount,
                            attempts=attempt, latency_ms=latency_ms,
                            error_type=type(e).__name__,
                            error_msg=str(e)[:200],
                            permanent=True, action=action_label,
                        )
                    except Exception:
                        pass
                raise
            is_permanent = _is_permanent_error(err_str)
            if is_permanent:
                if log_struct:
                    try:
                        latency_ms = int((time.monotonic() - t_start) * 1000)
                        log_struct(
                            "order_failed",
                            symbol=order_symbol, side=order_side,
                            amount=order_amount,
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
            cid = order_params.get("clientOrderId")
            if cid:
                existing = _find_order_by_client_id(ex, order_symbol, cid,
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
            if attempt < attempts_limit:
                _record_retry_failure()
                multiplier = _retry_backoff_multiplier()
                wait = 0.5 * (2 ** (attempt - 1)) * multiplier
                if _is_rate_limit_error(err_str):
                    wait = max(wait, 1.5 * (2 ** (attempt - 1)))
                if log_event:
                    try:
                        log_event(
                            f"{action_label} attempt {attempt}/{attempts_limit} "
  f"failed: {e}  retry in {wait:.1f}s "
  f"(backoff {multiplier:.0f})",
                            "WARN"
                        )
                    except Exception:
                        pass
                if abort_on_shutdown and shutdown_event is not None:
                    if shutdown_event.wait(timeout=wait):
                        raise last_err
                else:
                    time.sleep(wait)
    if log_struct:
        try:
            latency_ms = int((time.monotonic() - t_start) * 1000)
            log_struct(
                "order_failed",
                symbol=order_symbol, side=order_side, amount=order_amount,
                attempts=attempts_limit, latency_ms=latency_ms,
                error_type=type(last_err).__name__ if last_err else "Unknown",
                error_msg=str(last_err)[:200] if last_err else "",
                permanent=False, action=action_label,
            )
        except Exception:
            pass
    raise last_err


#  Fee extraction (futures variant  fees in USDT, not base) 

_FUTURES_DISCOUNT_TOKENS = ("MX", "BNB", "BGB", "OKB", "HT", "KCS", "GT")


def _finite_fee_cost(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        cost = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return cost if math.isfinite(cost) else None


def _positive_float(value) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return parsed if math.isfinite(parsed) and parsed > 0 else 0.0


def _first_positive_float(*values) -> float:
    for value in values:
        parsed = _positive_float(value)
        if parsed > 0:
            return parsed
    return 0.0


def _position_contracts_abs(pos: dict) -> Optional[float]:
    """Parse exchange position size; malformed payload means untrusted state."""
    if not isinstance(pos, dict):
        return None
    raw = pos.get("contracts")
    if raw in (None, "", 0, 0.0):
        raw = pos.get("size")
    if isinstance(raw, bool):
        return None
    try:
        contracts = abs(float(raw or 0.0))
    except (TypeError, ValueError, OverflowError):
        return None
    return contracts if math.isfinite(contracts) else None


def _upper_currency_text(value) -> str:
    return value.strip().upper() if isinstance(value, str) else ""


def _order_with_fee_context(order, ex=None, symbol_full: str = "",
                            contract_size: Optional[float] = None) -> dict:
    payload = dict(order) if isinstance(order, dict) else {}
    if not payload:
        return payload
    if contract_size is not None:
        payload.setdefault("_fee_contract_size", contract_size)
    if symbol_full:
        payload.setdefault("_fee_symbol_full", symbol_full)
    if ex is not None and "_bot_ex" not in payload:
        payload["_bot_ex"] = ex
    return payload


def _convert_fee_to_usdt_futures_known(fee_dict, order_dict) -> tuple[float, bool]:
    if not isinstance(fee_dict, dict) or fee_dict.get("cost") is None:
        return 0.0, False
    cost = _finite_fee_cost(fee_dict.get("cost"))
    if cost is None:
        return 0.0, False

    raw_currency = fee_dict.get("currency")
    if raw_currency is not None and not isinstance(raw_currency, str):
        return 0.0, False
    currency = _upper_currency_text(raw_currency)
    if not currency:
        return 0.0, False
    if cost == 0:
        return 0.0, True
    if currency in ("USDT", "USD", "BUSD", "USDC", "FDUSD"):
        return cost, True
    if currency in _FUTURES_DISCOUNT_TOKENS:
        converted = _discount_token_fee_to_usdt(currency, cost, order_dict)
        if converted != 0 and math.isfinite(converted):
            return converted, True
        if cost < 0:
            return 0.0, True
        return converted, False
    return 0.0, False


def convert_fee_to_usdt_futures(fee_dict, order_dict) -> float:
    """Futures fees are typically quoted in USDT directly.

  Discount-token fees (MEXC MX, Binance BNB, ) are paid in a non-USDT coin;
    rather than discard them, best-effort convert to USDT via that token's
    current price, falling back to a notional-based estimate (mirrors the spot
  path's discount fallback). Any failure  notional estimate.
    """
    fee, _known = _convert_fee_to_usdt_futures_known(fee_dict, order_dict)
    return fee


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
            px = _first_positive_float(
                (ticker or {}).get("last"),
                (ticker or {}).get("close"),
            )
            if px > 0:
                converted = round(cost * px, 6)
                if math.isfinite(converted):
                    return converted
        except Exception:
            pass
    if cost < 0:
        return 0.0
    try:
        filled = _first_positive_float(
            order_dict.get("filled"),
            order_dict.get("amount"),
        )
        fp = _first_positive_float(order_dict.get("average"), order_dict.get("price"))
        cs = _first_positive_float(
            order_dict.get("_fee_contract_size"),
            order_dict.get("contract_size"),
            order_dict.get("contractSize"),
        )
        if cs <= 0:
            symbol_full = order_dict.get("_fee_symbol_full") or order_dict.get("symbol")
            if ex is not None and isinstance(symbol_full, str) and symbol_full:
                try:
                    cs = futures_contract_size(ex, symbol_full)
                except Exception:
                    cs = 0.0
        if cs <= 0:
            cs = 1.0
        if filled > 0 and fp > 0:
            estimated = round(filled * cs * fp * FUTURES_DEFAULT_TAKER_FEE, 6)
            if math.isfinite(estimated):
                return estimated
    except (TypeError, ValueError, OverflowError):
        pass
    return 0.0


def extract_order_fee_futures(order) -> float:
    """Total fee paid for a futures order in USDT.

    Uses ``is not None`` for the cost-present check so legitimate cost=0
    maker rebates aren't silently dropped.
    """
    fee, _known = _extract_order_fee_futures_known(order)
    return fee


def _extract_order_fee_futures_known(order) -> tuple[float, bool]:
    """Return ``(fee_usdt, known)`` for futures order fee extraction.

    ``fee_usdt == 0`` can mean either a real exchange-reported zero fee or no
    usable fee data. Callers that refetch/estimate need the distinction.
    """
    if not isinstance(order, dict):
        return 0.0, False
    fees_list = order.get("fees") or []
    if isinstance(fees_list, list):
        saw_fee = False
        saw_known = False
        total = 0.0
        for fee_dict in fees_list:
            if not isinstance(fee_dict, dict):
                saw_fee = True
                continue
            if "cost" not in fee_dict or fee_dict.get("cost") is None:
                saw_fee = True
                continue
            saw_fee = True
            fee, known = _convert_fee_to_usdt_futures_known(fee_dict, order)
            if known:
                saw_known = True
                total += fee
        if saw_fee:
            if saw_known:
                return total if math.isfinite(total) else 0.0, math.isfinite(total)
            singular_fee, singular_known = _convert_fee_to_usdt_futures_known(
                order.get("fee") or {}, order)
            if singular_known:
                return singular_fee, True
            return 0.0, False
    return _convert_fee_to_usdt_futures_known(order.get("fee") or {}, order)


FUTURES_DEFAULT_TAKER_FEE = 0.001


def futures_contract_size(ex, symbol_full: str) -> float:
    """Contract size for a futures market from CCXT metadata (default 1.0).

    USDT-M perpetuals are usually 1, but some contracts (1000SATS, MEME, Bybit
  inverse, ) use 100/1000/etc. Any fee/notional math MUST multiply by this,
    otherwise the value is wrong by the contractSize factor.
    """
    try:
        markets = getattr(ex, "markets", None) or {}
        m = markets.get(symbol_full) or {}
        info = m.get("info") or {}
        if not isinstance(info, dict):
            info = {}
        for cs in (
            m.get("contractSize"),
            m.get("contract_size"),
            info.get("contractSize"),
            info.get("contract_size"),
        ):
            if cs is None:
                continue
            try:
                v = float(cs)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(v) and v > 0:
                return v
        return 1.0
    except (TypeError, ValueError, OverflowError, AttributeError):
        return 1.0


def _exchange_id(ex) -> str:
    return str(getattr(ex, "id", None) or getattr(ex, "name", None) or "").lower()


def _is_mexc_swap_symbol(ex, symbol_full: str) -> bool:
    if _exchange_id(ex) != "mexc":
        return False
    try:
        market = (getattr(ex, "markets", None) or {}).get(symbol_full) or {}
        if market.get("swap"):
            return True
    except Exception:
        pass
    return ":USDT" in str(symbol_full)


def _order_id_is_fetchable(ex, symbol_full: str, order_id) -> bool:
    """MEXC swap fetch_order only accepts the numeric exchange order id."""
    oid = _order_id_text(order_id)
    if not oid:
        return False
    if _is_mexc_swap_symbol(ex, symbol_full):
        return oid.isdigit()
    return True


def _order_id_for_fee_refetch(ex, symbol_full: str, order: dict):
    """Return an exchange order id that is safe for ``fetch_order``.

    MEXC swap responses can carry our clientOrderId in the top-level ``id``;
    its ``fetch_order`` endpoint rejects that with code 600. Prefer a numeric
    exchange id when available and otherwise skip the refetch.
    """
    if not isinstance(order, dict):
        return None
    candidates = [order.get("id"), order.get("orderId")]
    info = order.get("info")
    if isinstance(info, dict):
        candidates.extend((
            info.get("orderId"),
            info.get("order_id"),
            info.get("orderID"),
            info.get("id"),
        ))
    if _is_mexc_swap_symbol(ex, symbol_full):
        for candidate in candidates:
            oid = _order_id_text(candidate)
            if oid.isdigit():
                return oid
        return None
    for candidate in candidates:
        oid = _order_id_text(candidate)
        if oid:
            return oid
    return None


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
        if any(isinstance(v, bool) for v in (
            amount, contract_size, fill_price, leverage
        )):
            raise ValueError("boolean margin input")
        amt = float(amount)
        cs = float(contract_size)
        px = float(fill_price)
        lev = float(leverage)
        if not all(math.isfinite(v) for v in (amt, cs, px, lev)):
            raise ValueError("non-finite margin input")
        raw = (amt * cs * px) / lev if lev > 0 else 0.0
        if math.isfinite(raw) and raw > 0:
            return raw, True
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        pass
    try:
        if isinstance(fallback_margin, bool):
            raise ValueError("boolean fallback margin")
        fallback = float(fallback_margin or 0.0)
        if math.isfinite(fallback):
            return fallback, False
    except (TypeError, ValueError, OverflowError):
        pass
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
    order_payload = _order_with_fee_context(
        order, ex=ex, symbol_full=symbol_full, contract_size=contract_size
    )
    real, fee_known = _extract_order_fee_futures_known(order_payload)
    if fee_known:
        return real

    order_id = _order_id_for_fee_refetch(ex, symbol_full, order)
    if (
        order_id
        and ex is not None
        and symbol_full
        and _order_id_is_fetchable(ex, symbol_full, order_id)
    ):
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
                    refreshed_payload = _order_with_fee_context(
                        refreshed,
                        ex=ex,
                        symbol_full=symbol_full,
                        contract_size=contract_size,
                    )
                    real, fee_known = _extract_order_fee_futures_known(
                        refreshed_payload
                    )
                    if fee_known:
                        return real
            except Exception:
                continue

    # Estimate fallback
    # Prefer the order's own fill; fall back to what the CALLER actually traded
    # when the response carries no fill yet. Notional ALWAYS includes contractSize.
    filled = 0.0
    for candidate in (order_payload.get("filled"), order_payload.get("amount")):
        if isinstance(candidate, bool):
            continue
        try:
            filled_candidate = float(candidate)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(filled_candidate) and filled_candidate > 0:
            filled = filled_candidate
            break
    if (not math.isfinite(filled) or filled <= 0) and amount is not None:
        try:
            if isinstance(amount, bool):
                raise ValueError("boolean amount")
            filled = float(amount)
        except (TypeError, ValueError, OverflowError):
            filled = 0.0
    if not math.isfinite(filled):
        filled = 0.0
    if contract_size is None:
        contract_size = futures_contract_size(ex, symbol_full)
    try:
        if isinstance(contract_size, bool):
            raise ValueError("boolean contract size")
        cs = float(contract_size)
        if not math.isfinite(cs) or cs <= 0:
            cs = 1.0
    except (TypeError, ValueError, OverflowError):
        cs = 1.0
    try:
        if isinstance(fill_price, bool):
            raise ValueError("boolean fill price")
        fp = float(fill_price)
    except (TypeError, ValueError, OverflowError):
        fp = 0.0
    if not math.isfinite(fp) or filled <= 0 or fp <= 0:
        return 0.0
    if _is_mexc_swap_symbol(ex, symbol_full):
        try:
            from trading.execution_quality import mexc_private_fee_quote

            taker_rate = mexc_private_fee_quote(
                ex, symbol_full, "taker"
            ).rate
        except Exception:
            taker_rate = FUTURES_DEFAULT_TAKER_FEE
    try:
        if isinstance(taker_rate, bool):
            raise ValueError("boolean taker rate")
        rate = float(taker_rate)
    except (TypeError, ValueError, OverflowError):
        rate = FUTURES_DEFAULT_TAKER_FEE
    if not math.isfinite(rate) or rate < 0:
        rate = FUTURES_DEFAULT_TAKER_FEE
    try:
        estimated = round(filled * cs * fp * rate, 6)
    except OverflowError:
        return 0.0
    return estimated if math.isfinite(estimated) else 0.0


#  Position verification 

def fetch_open_position(ex, symbol_full: str) -> Tuple[Optional[dict], bool]:
    """Return the open exchange position for ``symbol_full``.

    MEXC/CCXT can occasionally return an empty list for symbol-scoped
    ``fetch_positions([symbol])`` while the global positions endpoint still
    contains the just-filled position. Entry paths must treat that as
    "verification incomplete", not "no position", otherwise a live order can
    become an unmanaged orphan.

    Returns ``(position, unavailable)``. ``unavailable=True`` means the
    exchange position endpoint could not be trusted and callers should keep or
    create provisional state instead of deleting claims/state.
    """
    try:
        from config.exchange_config import safe_fetch_positions
    except Exception:
        return None, True

    try:
        if not try_consume_api_call("fetch_positions:scoped"):
            return None, True
        positions = safe_fetch_positions(ex, [symbol_full])
        scoped_has_symbol = False
        if positions is not None:
            try:
                scoped_has_symbol = any(
                    isinstance(p, dict)
                    and (p.get("symbol") or "") == symbol_full
                    for p in positions
                )
            except Exception:
                scoped_has_symbol = False
        if positions is None or not scoped_has_symbol:
            if not try_consume_api_call("fetch_positions:global"):
                return None, True
            global_positions = safe_fetch_positions(ex)
            if global_positions is None:
                return None, True
            positions = global_positions
    except Exception:
        return None, True

    for pos in positions or []:
        if not isinstance(pos, dict):
            continue
        if (pos.get("symbol") or "") != symbol_full:
            continue
        contracts = _position_contracts_abs(pos)
        if contracts is None:
            return None, True
        if contracts > 1e-8:
            return pos, False
    return None, False

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
    # Do at most 2 attempts within the window, with a small gap. If both
    # exceed the deadline, report "not verified" so the caller keeps state.
    last_remaining = -1.0
    while time.monotonic() < deadline:
        try:
            pos, unavailable = fetch_open_position(ex, symbol_full)
            if unavailable:
                pass
            elif pos is None:
                return True, 0.0
            else:
                parsed_remaining = _position_contracts_abs(pos)
                if parsed_remaining is None:
                    last_remaining = -1.0
                else:
                    last_remaining = parsed_remaining
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
                    if isinstance(v, bool):
                        continue
                    try:
                        fv = float(v)
                        if math.isfinite(fv) and fv > 0:
                            return fv
                    except (TypeError, ValueError, OverflowError):
                        continue
        return 0.0
    except Exception:
        return 0.0
