"""
bot_utils/futures_order.py  Futures-specific order helpers.

Extracted from main_bot_futures.py:
  create_order_with_retry  fail-closed entry / bounded reduce-only placement
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

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.network_retry import RetryForbiddenError
from bot_utils.order_utils import (
    explicit_trade_symbol_matches,
    order_id_text_or_none,
    strict_order_snapshot_equal,
)


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


def _finite_order_telemetry_value(value, *, positive: bool = False):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed < 0:
        return None
    if positive and parsed <= 0:
        return None
    return parsed


def _normalize_order_symbol(symbol_full) -> str:
    if not isinstance(symbol_full, str):
        return ""
    return symbol_full.strip()


def _normalize_order_side(side, exchange_id: str = "") -> str:
    if not isinstance(side, str):
        return ""
    normalized = side.strip().lower()
    if exchange_id == "mexc":
        if normalized in {"1", "2"}:
            return "buy"
        if normalized in {"3", "4"}:
            return "sell"
    return normalized if normalized in {"buy", "sell"} else ""


def _normalize_order_position_side(position_side) -> str:
    if not isinstance(position_side, str):
        return ""
    normalized = position_side.strip().lower()
    return normalized if normalized in {"long", "short"} else ""


def _normalize_order_bool(value) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes"}:
            return True
        if normalized in {"0", "false", "no"}:
            return False
    return None


def _mexc_contract_action(side) -> tuple[str, str, bool] | None:
    return {
        "1": ("buy", "long", False),
        "2": ("buy", "short", True),
        "3": ("sell", "short", False),
        "4": ("sell", "long", True),
    }.get(_order_id_text(side))


def _trade_info_for_order_evidence(
    trade: dict,
    exchange_id: str = "",
) -> tuple[dict, bool]:
    """Return trade info safe to validate with order-response semantics."""
    raw_info = trade.get("info") if isinstance(trade, dict) else None
    info = dict(raw_info) if isinstance(raw_info, dict) else {}
    if str(exchange_id or "").strip().lower() != "mexc":
        return info, True
    raw_side = info.get("side")
    if raw_side in (None, ""):
        return info, True
    # MEXC contract deal rows use the same 1..4 action codes as orders:
    # 1=open long, 2=close short, 3=open short, 4=close long. Verify the
    # resulting order direction against CCXT's unified side. Retain numeric
    # action evidence so later validation can also prove leg and reduce-only.
    mexc_action = _mexc_contract_action(raw_side)
    trade_side = (
        mexc_action[0]
        if mexc_action is not None
        else {"buy": "buy", "sell": "sell"}.get(
            str(raw_side).strip().lower()
        )
    )
    unified_side = _normalize_order_side(trade.get("side"))
    if not trade_side or unified_side != trade_side:
        return info, False
    if mexc_action is None:
        # Some adapters expose only a second unified side string in ``info``;
        # it carries no leg/action evidence and must not be parsed as 1..4.
        info.pop("side", None)
    return info, True


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

    # Some venue/native recovery payloads use ``filled`` instead of CCXT's
    # canonical ``closed``.  Require physical fill evidence before accepting
    # that alias as terminal; status text alone must not create a phantom fill.
    if status == "filled" and filled > 0:
        return ORDER_STATE_FILLED
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
    "externalOid", "external_oid", "orderLinkId", "order_link_id",
)


def _order_id_text(value) -> str:
    return order_id_text_or_none(value) or ""


class FuturesOrderOutcomeUnknown(RetryForbiddenError):
    """A Futures order may exist, but its create response was not recovered."""

    def __init__(self, client_order_id):
        cid = _order_id_text(client_order_id) or "[invalid]"
        self.client_order_id = cid
        super().__init__(
            "futures order outcome unknown "
            f"(clientOrderId={cid})"
        )


class FuturesOrderNotSubmitted(RuntimeError):
    """The API budget blocked an order before ``create_order`` was called."""


class _TradeRecoveryOrder(dict):
    """Order-shaped snapshot created only from internally aggregated trades."""


def _first_order_id_text(*values) -> str:
    for value in values:
        text = _order_id_text(value)
        if text:
            return text
    return ""


def _explicit_order_ids(order: dict) -> set[str]:
    """Return every explicit venue order id carried by an order snapshot."""
    if not isinstance(order, dict):
        return set()
    info = order.get("info")
    if not isinstance(info, dict):
        info = {}
    info_id = None if isinstance(order, _TradeRecoveryOrder) else info.get("id")
    return {
        value
        for value in (
            _order_id_text(order.get("id")),
            _order_id_text(order.get("orderId")),
            _order_id_text(order.get("order_id")),
            _order_id_text(order.get("orderID")),
            _order_id_text(info_id),
            _order_id_text(info.get("orderId")),
            _order_id_text(info.get("order_id")),
            _order_id_text(info.get("orderID")),
            _order_id_text(info.get("ordId")),
        )
        if value
    }


def _explicit_client_order_ids(order: dict) -> set[str]:
    """Return every explicit client order id carried by an order snapshot."""
    if not isinstance(order, dict):
        return set()
    info = order.get("info")
    if not isinstance(info, dict):
        info = {}
    return {
        value
        for value in (
            _order_id_text(order.get("clientOrderId")),
            *(_order_id_text(info.get(key)) for key in _CLIENT_ID_INFO_KEYS),
        )
        if value
    }


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


def _order_client_id_conflicts(o: dict, cid: str) -> bool:
    """True when any explicit client-id evidence differs from the request."""
    if not isinstance(o, dict):
        return False
    expected = _order_id_text(cid)
    if not expected:
        return False
    observed = _explicit_client_order_ids(o)
    return bool(observed) and observed != {expected}


def _order_request_conflicts(
    order: dict,
    expected_symbol: str,
    expected_side: str,
    expected_position_side: str = "",
    exchange_id: str = "",
    expected_reduce_only: Optional[bool] = None,
    allow_one_way_position_side: bool = False,
    expected_client_id: Optional[str] = None,
    expected_amount: Optional[float] = None,
) -> bool:
    """Reject explicit response evidence that contradicts the request."""
    if not isinstance(order, dict):
        return False
    if order.get("_bot_recovery_conflict") is True:
        return True
    if len(_explicit_order_ids(order)) > 1:
        return True
    if expected_client_id and _order_client_id_conflicts(
        order, expected_client_id
    ):
        return True
    if expected_amount is not None:
        requested = _finite_order_telemetry_value(
            expected_amount, positive=True
        )
        if requested is None:
            return True
        tolerance = max(1e-12, requested * 1e-9)
        raw_amount = order.get("amount")
        if raw_amount not in (None, ""):
            observed_amount = _finite_order_telemetry_value(
                raw_amount, positive=True
            )
            if observed_amount is None:
                return True
            if isinstance(order, _TradeRecoveryOrder):
                if observed_amount > requested + tolerance:
                    return True
            elif abs(observed_amount - requested) > tolerance:
                return True
        raw_filled = order.get("filled")
        if raw_filled not in (None, ""):
            observed_filled = _finite_order_telemetry_value(raw_filled)
            if (
                observed_filled is None
                or observed_filled > requested + tolerance
            ):
                return True
    if (
        _normalize_order_status(order.get("status")) == "rejected"
        and _order_response_has_fill_evidence(order)
    ):
        return True
    raw_symbol = order.get("symbol")
    if raw_symbol not in (None, ""):
        observed_symbol = _normalize_order_symbol(raw_symbol)
        if not observed_symbol or observed_symbol != expected_symbol:
            return True
    expected_leg = _normalize_order_position_side(expected_position_side)
    raw_info = order.get("info")
    if raw_info not in (None, "") and not isinstance(raw_info, dict):
        return True
    info = raw_info if isinstance(raw_info, dict) else {}
    raw_side = order.get("side")
    mexc_action = None
    if exchange_id == "mexc":
        raw_venue_side = info.get("side")
        if raw_venue_side not in (None, ""):
            venue_side = _order_id_text(raw_venue_side)
            if venue_side not in {"1", "2", "3", "4"}:
                return True
            raw_side = venue_side
            mexc_action = _mexc_contract_action(venue_side)
    if exchange_id == "mexc" and mexc_action is None:
        mexc_action = _mexc_contract_action(raw_side)
    if mexc_action is not None:
        observed_side, observed_leg, observed_reduce_only = mexc_action
        if observed_side != expected_side:
            return True
        if expected_leg and observed_leg != expected_leg:
            return True
        if (
            expected_reduce_only is not None
            and observed_reduce_only is not expected_reduce_only
        ):
            return True
    if raw_side not in (None, ""):
        observed_side = _normalize_order_side(raw_side, exchange_id)
        if not observed_side or observed_side != expected_side:
            return True
    sources = (order, info)
    if expected_reduce_only is not None:
        okx_raw_reduce_evidence = exchange_id != "okx" or any(
            key in info
            and info.get(key) is not None
            and not (
                isinstance(info.get(key), str)
                and not info.get(key).strip()
            )
            for key in ("reduceOnly", "reduce_only")
        )
        for source in sources:
            for key in ("reduceOnly", "reduce_only"):
                if key not in source:
                    continue
                raw_reduce_only = source.get(key)
                if raw_reduce_only is None or (
                    isinstance(raw_reduce_only, str)
                    and not raw_reduce_only.strip()
                ):
                    continue
                observed_reduce_only = _normalize_order_bool(raw_reduce_only)
                if (
                    source is order
                    and exchange_id == "okx"
                    and raw_reduce_only is False
                    and not okx_raw_reduce_evidence
                ):
                    # CCXT synthesizes top-level False for OKX place-order
                    # acknowledgements even though the venue ACK contains no
                    # reduce-only evidence.  Only raw info is authoritative.
                    continue
                if observed_reduce_only is None:
                    return True
                if observed_reduce_only is not expected_reduce_only:
                    return True
    for source in sources:
        for key in ("positionSide", "posSide", "holdSide"):
            raw_leg = source.get(key)
            if raw_leg in (None, ""):
                continue
            observed_leg = _normalize_order_position_side(raw_leg)
            raw_leg_text = str(raw_leg).strip().lower()
            if expected_leg:
                if (
                    not observed_leg
                    and allow_one_way_position_side
                    and raw_leg_text in {"both", "net"}
                ):
                    continue
                if not observed_leg or observed_leg != expected_leg:
                    return True
            elif raw_leg_text != "both":
                return True
    return False


def _order_refresh_conflicts(
    original_order: dict,
    refreshed_order: dict,
    expected_symbol: str,
    expected_side: str,
    expected_position_side: str = "",
    exchange_id: str = "",
    expected_reduce_only: Optional[bool] = None,
    allow_one_way_position_side: bool = False,
    expected_client_id: Optional[str] = None,
    expected_amount: Optional[float] = None,
) -> bool:
    """Reject a refresh that cannot be bound to its original request."""
    if not isinstance(original_order, dict) or not isinstance(
        refreshed_order, dict
    ):
        return True
    original_ids = _explicit_order_ids(original_order)
    refreshed_ids = _explicit_order_ids(refreshed_order)
    if len(original_ids) != 1 or len(refreshed_ids) > 1:
        return True
    refresh_can_release_state = (
        _order_response_has_fill_evidence(refreshed_order)
        or _order_confirmed_terminal_zero_fill(refreshed_order)
    )
    if refresh_can_release_state and refreshed_ids != original_ids:
        return True
    if refreshed_ids and refreshed_ids != original_ids:
        return True
    request = (
        expected_symbol,
        expected_side,
        expected_position_side,
        exchange_id,
    )
    request_options = {
        "expected_reduce_only": expected_reduce_only,
        "allow_one_way_position_side": allow_one_way_position_side,
        "expected_client_id": expected_client_id,
        "expected_amount": expected_amount,
    }
    return _order_request_conflicts(
        original_order,
        *request,
        **request_options,
    ) or _order_request_conflicts(
        refreshed_order,
        *request,
        **request_options,
    )


def _requested_position_side(params: dict) -> str:
    """Return a normalized explicit hedge leg from outbound order params."""
    if not isinstance(params, dict):
        return ""
    for key in ("positionSide", "posSide", "holdSide"):
        raw_leg = params.get(key)
        if raw_leg not in (None, ""):
            return _normalize_order_position_side(raw_leg)
    return ""


def _order_response_has_evidence(order: dict) -> bool:
    """True when a create_order response contains minimal exchange evidence."""
    if not isinstance(order, dict):
        return False
    if _explicit_order_ids(order):
        return True
    if _order_id_text(order.get("clientOrderId")):
        return True
    info = order.get("info")
    if isinstance(info, dict):
        if any(_order_id_text(info.get(k)) for k in _CLIENT_ID_INFO_KEYS):
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

    Accept it when it has physical fill evidence, is live/accepted, or carries
    a real exchange id. A terminal canceled/expired/rejected status only proves
    no placement when there is no fill: exchanges can cancel the unfilled
    remainder after a partial execution."""
    if not isinstance(o, dict):
        return False
    order_ids = _explicit_order_ids(o)
    if len(order_ids) > 1:
        return False
    status = _normalize_order_status(o.get("status"))
    if status == "rejected":
        return False
    if _order_response_has_fill_evidence(o):
        return True
    if status in ("canceled", "cancelled", "expired"):
        return False
    if status in ("open", "new", "closed", "partially_filled", "partiallyfilled"):
        return True
    return bool(order_ids)


def _order_confirmed_terminal_zero_fill(order: dict) -> bool:
    """True only for explicit terminal status plus explicit zero fill."""
    if not isinstance(order, dict):
        return False
    if _normalize_order_status(order.get("status")) not in (
        "canceled",
        "cancelled",
        "expired",
        "rejected",
    ):
        return False
    filled = _finite_order_telemetry_value(order.get("filled"))
    if filled != 0:
        return False
    info = order.get("info")
    if isinstance(info, dict):
        for key in (
            "baseVolume",
            "dealVol",
            "dealSize",
            "executedQty",
            "filledSize",
            "filledQty",
            "filled_qty",
            "filled_amount",
            "cumExecQty",
            "cumQty",
            "accFillSz",
        ):
            if key not in info:
                continue
            raw_value = info.get(key)
            if raw_value in (None, ""):
                continue
            observed = _finite_order_telemetry_value(raw_value)
            if observed is None or observed > 0:
                return False
        for key in (
            "dealMoney",
            "cumQuote",
            "cumExecValue",
            "executedValue",
            "fillNotional",
            "filledValue",
            "filled_total",
            "quoteVolume",
            "quoteSize",
        ):
            if key not in info:
                continue
            raw_value = info.get(key)
            if raw_value in (None, ""):
                continue
            observed = _finite_order_telemetry_value(raw_value)
            if observed is None or observed > 0:
                return False
        gate_totals = []
        for source in (
            info,
            info.get("put"),
            info.get("initial"),
        ):
            if source is None:
                continue
            if not isinstance(source, dict):
                return False
            for key in ("amount", "size"):
                raw_value = source.get(key)
                if raw_value in (None, ""):
                    continue
                if isinstance(raw_value, bool):
                    return False
                try:
                    parsed = abs(float(raw_value))
                except (TypeError, ValueError, OverflowError):
                    return False
                if not math.isfinite(parsed):
                    return False
                gate_totals.append(parsed)
        raw_left = info.get("left")
        if gate_totals and raw_left not in (None, ""):
            if isinstance(raw_left, bool):
                return False
            try:
                left = abs(float(raw_left))
            except (TypeError, ValueError, OverflowError):
                return False
            if not math.isfinite(left):
                return False
            for total in gate_totals:
                tolerance = max(1e-12, total * 1e-9)
                if left > total + tolerance or total - left > tolerance:
                    return False
    raw_cost = order.get("cost")
    if raw_cost in (None, ""):
        return True
    cost = _finite_order_telemetry_value(raw_cost)
    return cost == 0


def _order_from_recovery_trades(
    trades: list[dict],
    cid: str,
    symbol_full: str,
    expected_amount: Optional[float] = None,
    exchange_id: str = "",
) -> dict:
    """Build one order-shaped snapshot from matching CCXT trade fills."""
    order_ids: set[str] = set()
    symbols: set[str] = set()
    sides: set[str] = set()
    fees: list[dict] = []
    filled = 0.0
    cost = 0.0
    cost_known = True
    price_notional = 0.0
    malformed = False
    first_info = {}
    unique_trades: list[dict] = []
    trades_by_id: dict[str, dict] = {}

    for trade in trades:
        info = trade.get("info")
        if not isinstance(info, dict):
            info = {}
        trade_ids = {
            value
            for value in (
                _order_id_text(trade.get("id")),
                _order_id_text(trade.get("tradeId")),
                _order_id_text(info.get("tradeId")),
                _order_id_text(info.get("trade_id")),
                _order_id_text(info.get("execId")),
                _order_id_text(info.get("fillId")),
            )
            if value
        }
        if len(trade_ids) != 1:
            malformed = True
            unique_trades.append(trade)
            continue
        trade_id = next(iter(trade_ids))
        previous = trades_by_id.get(trade_id)
        if previous is not None:
            if not strict_order_snapshot_equal(previous, trade):
                malformed = True
            continue
        trades_by_id[trade_id] = trade
        unique_trades.append(trade)

    for trade in unique_trades:
        info, trade_side_valid = _trade_info_for_order_evidence(
            trade, exchange_id
        )
        if not trade_side_valid:
            malformed = True
        if not first_info:
            first_info = dict(info)
        observed_order_ids = {
            value
            for value in (
                _order_id_text(trade.get("order")),
                _order_id_text(trade.get("orderId")),
                _order_id_text(trade.get("order_id")),
                _order_id_text(trade.get("orderID")),
                _order_id_text(info.get("orderId")),
                _order_id_text(info.get("order_id")),
                _order_id_text(info.get("orderID")),
                _order_id_text(info.get("ordId")),
            )
            if value
        }
        if len(observed_order_ids) == 1:
            order_ids.update(observed_order_ids)
        else:
            malformed = True
        if _order_client_id_conflicts(trade, cid):
            malformed = True

        raw_symbol = trade.get("symbol")
        if raw_symbol not in (None, ""):
            normalized_symbol = _normalize_order_symbol(raw_symbol)
            if normalized_symbol:
                symbols.add(normalized_symbol)
            else:
                malformed = True

        raw_side = trade.get("side")
        if raw_side not in (None, ""):
            normalized_side = _normalize_order_side(raw_side)
            if normalized_side:
                sides.add(normalized_side)
            else:
                malformed = True

        amount = _finite_order_telemetry_value(
            trade.get("amount"), positive=True
        )
        if amount is None:
            malformed = True
            continue
        filled += amount
        price = _finite_order_telemetry_value(
            trade.get("price"), positive=True
        )
        if price is None:
            malformed = True
        else:
            price_notional += amount * price
        raw_cost = trade.get("cost")
        explicit_cost = "cost" in trade and raw_cost not in (None, "")
        trade_cost = _finite_order_telemetry_value(raw_cost)
        if explicit_cost and trade_cost is None:
            malformed = True
        elif trade_cost is None:
            # Derivative trade cost includes contractSize.  Without an
            # explicit venue/CCXT cost it cannot be inferred from contracts.
            cost_known = False
        if trade_cost is not None:
            cost += trade_cost

        trade_fees = trade.get("fees")
        if isinstance(trade_fees, list) and trade_fees:
            fees.extend(dict(fee) for fee in trade_fees if isinstance(fee, dict))
        elif isinstance(trade.get("fee"), dict):
            fees.append(dict(trade["fee"]))

    if len(order_ids) != 1 or len(symbols) > 1 or len(sides) > 1:
        malformed = True
    if symbols and symbol_full not in symbols:
        malformed = True
    if not math.isfinite(filled) or filled <= 0:
        malformed = True
    if expected_amount is not None:
        expected = _finite_order_telemetry_value(
            expected_amount, positive=True
        )
        if expected is None:
            malformed = True
        else:
            tolerance = max(1e-12, expected * 1e-9)
            if filled > expected + tolerance:
                malformed = True
    if not math.isfinite(cost):
        malformed = True
    if not math.isfinite(price_notional):
        malformed = True
    total_cost = cost if cost_known and not malformed else None
    average = (
        price_notional / filled
        if not malformed and filled > 0
        else None
    )
    return _TradeRecoveryOrder({
        "id": next(iter(order_ids), None),
        "clientOrderId": cid,
        "symbol": next(iter(symbols), symbol_full),
        "side": next(iter(sides), None),
        "status": "closed",
        "filled": filled if filled > 0 and math.isfinite(filled) else None,
        "amount": filled if filled > 0 and math.isfinite(filled) else None,
        "cost": total_cost,
        "average": average,
        "price": average,
        "fee": fees[0] if len(fees) == 1 else None,
        "fees": fees,
        "trades": [dict(trade) for trade in unique_trades],
        "info": first_info,
        "_bot_recovery_from_trades": True,
        "_bot_recovery_conflict": malformed,
    })


def _find_order_by_client_id(
    ex,
    symbol_full: str,
    cid: str,
    log_event=None,
    lookup_status: Optional[dict] = None,
    expected_amount: Optional[float] = None,
    expected_side: Optional[str] = None,
    expected_position_side: str = "",
    exchange_id: str = "",
    expected_reduce_only: Optional[bool] = None,
    lookup_since_ms: Optional[int] = None,
):
    """Locate an order by clientOrderId - open orders first, then recent
    history. Used to recover from a lost-response timeout so a retry doesn't
    open a SECOND position. Best-effort; never raises. Returns dict or None.

    Matches both the CCXT-normalised ``clientOrderId`` and the raw ``info``
    aliases (``clOrdId``, ``newClientOrderId``, ...) so the guard works on
    exchanges that don't surface the id at the top level.

    A failed lookup is logged (when ``log_event`` is supplied) and recorded in
    ``lookup_status``. The order wrapper is stricter still: any unresolved
    non-reduce-only create outcome is blocked because an instant market fill
    may legitimately be absent from open-order results."""
    source_status: dict[str, str] = {}
    source_complete: dict[str, bool] = {}
    if isinstance(lookup_status, dict):
        lookup_status["complete_negative"] = False

    def _set_source(source: str, state: str) -> None:
        source_status[source] = state
        if isinstance(lookup_status, dict):
            lookup_status["sources"] = dict(source_status)

    def _set_result(state: str) -> None:
        if isinstance(lookup_status, dict):
            lookup_status["result"] = state

    def _mark_unavailable(source: str = "request") -> None:
        _set_source(source, "unavailable")
        if isinstance(lookup_status, dict):
            lookup_status["unavailable"] = True

    if not cid:
        _mark_unavailable()
        return None
    if (
        isinstance(lookup_since_ms, bool)
        or not isinstance(lookup_since_ms, int)
        or lookup_since_ms <= 0
    ):
        lookup_since_ms = None

    source_requests: dict[str, tuple[str, object]] = {}
    source_request_errors: set[str] = set()

    def _record_source_error(source: str) -> None:
        request = source_requests.get(source)
        if request is None or source in source_request_errors:
            return
        endpoint, reservation = request
        if not isinstance(reservation, ApiCallReservation):
            return
        source_request_errors.add(source)
        try:
            record_api_error(endpoint, reservation)
        except Exception:
            pass

    def _budgeted(endpoint: str, source: str, fn):
        try:
            reservation = try_consume_api_call(
                endpoint,
                critical=True,
                return_reservation=True,
            )
        except Exception:
            _mark_unavailable(source)
            return None
        if not reservation:
            _mark_unavailable(source)
            if isinstance(lookup_status, dict):
                lookup_status["budget_denied"] = True
            return None
        source_requests[source] = (endpoint, reservation)
        try:
            return fn()
        except Exception:
            _record_source_error(source)
            raise

    def _recovery_rows(raw, source: str, *, page_limit: int | None = None):
        if not isinstance(raw, list):
            _record_source_error(source)
            _mark_unavailable(source)
            source_complete[source] = False
            return ()
        source_complete[source] = bool(
            page_limit is None or len(raw) < page_limit
        )
        rows = []
        for row in raw:
            if not isinstance(row, dict):
                _record_source_error(source)
                _mark_unavailable(source)
                continue
            rows.append(row)
        if source_status.get(source) != "unavailable":
            _set_source(source, "empty")
        return rows

    terminal_zero_fill = None
    observed_order_ids: set[str] = set()

    def _defer_terminal_zero_fill(order: dict) -> bool:
        return (
            isinstance(order, dict)
            and order.get("_bot_recovery_conflict") is not True
            and _normalize_order_status(order.get("status"))
            in ("canceled", "cancelled", "expired")
            and not _order_response_has_fill_evidence(order)
        )

    def _select_recovery_candidate(order: dict):
        nonlocal terminal_zero_fill, observed_order_ids
        current_ids = _explicit_order_ids(order)
        if (
            len(current_ids) > 1
            or (
                observed_order_ids
                and current_ids
                and observed_order_ids != current_ids
            )
        ):
            conflict = dict(order)
            conflict["_bot_recovery_conflict"] = True
            return conflict
        observed_order_ids.update(current_ids)
        if expected_side and _order_request_conflicts(
            order,
            symbol_full,
            expected_side,
            expected_position_side,
            exchange_id,
            expected_reduce_only=expected_reduce_only,
            expected_client_id=cid,
            expected_amount=expected_amount,
        ):
            conflict = dict(order)
            conflict["_bot_recovery_conflict"] = True
            return conflict
        if not _defer_terminal_zero_fill(order):
            return order
        terminal_zero_fill = order
        return None

    def _select_recovery_batch(rows):
        selected = None
        for order in rows:
            if not _order_client_id_matches(order, cid):
                continue
            candidate = _select_recovery_candidate(order)
            if candidate is None:
                continue
            if candidate.get("_bot_recovery_conflict") is True:
                return candidate
            if selected is None:
                selected = candidate
            elif not strict_order_snapshot_equal(selected, candidate):
                conflict = dict(candidate)
                conflict["_bot_recovery_conflict"] = True
                return conflict
        return selected

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
                    "mexc_exact",
                    lambda: native({"symbol": market_id, "externalOid": cid}),
                )
                data = raw.get("data") if isinstance(raw, dict) else None
                if isinstance(data, dict):
                    expected_cid = _order_id_text(cid)
                    external_ids = {
                        value
                        for value in (
                            _order_id_text(data.get("externalOid")),
                            _order_id_text(data.get("external_oid")),
                        )
                        if value
                    }
                    raw_state = _order_id_text(data.get("state"))
                    status = {
                        "2": "open",
                        "3": "closed",
                        "4": "canceled",
                        "5": "rejected",
                    }.get(raw_state)
                    filled = _finite_order_telemetry_value(
                        data.get("dealVol")
                    )
                    amount = _finite_order_telemetry_value(data.get("vol"))
                    average = _finite_order_telemetry_value(
                        data.get("dealAvgPrice"), positive=True
                    )
                    order_ids = {
                        value
                        for value in (
                            _order_id_text(data.get("orderId")),
                            _order_id_text(data.get("id")),
                        )
                        if value
                    }
                    order_id = next(iter(order_ids), "")
                    raw_symbol = _order_id_text(data.get("symbol"))
                    expected_native_symbols = {
                        _order_id_text(market_id),
                        _order_id_text(symbol_full),
                    }
                    malformed = (
                        status is None
                        or external_ids != {expected_cid}
                        or len(order_ids) != 1
                        or (
                            bool(raw_symbol)
                            and raw_symbol not in expected_native_symbols
                        )
                        or ("dealVol" in data and filled is None)
                        or (
                            "vol" in data
                            and _finite_order_telemetry_value(
                                data.get("vol"), positive=True
                            ) is None
                        )
                    )
                    if filled is not None and amount is not None:
                        tolerance = max(1e-12, amount * 1e-9)
                        if filled > amount + tolerance:
                            malformed = True
                        if status == "closed" and abs(filled - amount) > tolerance:
                            malformed = True
                    expected = _finite_order_telemetry_value(
                        expected_amount, positive=True
                    )
                    if expected is not None:
                        tolerance = max(1e-12, expected * 1e-9)
                        if (
                            (amount is not None and amount > expected + tolerance)
                            or (filled is not None and filled > expected + tolerance)
                        ):
                            malformed = True
                    native_order = {
                        "id": order_id,
                        "clientOrderId": cid,
                        "symbol": symbol_full,
                        "side": data.get("side"),
                        "status": status,
                        "filled": filled,
                        "amount": amount,
                        "average": average,
                        "info": data,
                        "_bot_recovery_conflict": malformed,
                    }
                    selected = _select_recovery_candidate(native_order)
                    if selected is not None:
                        state = (
                            "conflict"
                            if selected.get("_bot_recovery_conflict") is True
                            else "found"
                        )
                        _set_source("mexc_exact", state)
                        _set_result(state)
                        return selected
                    _set_source("mexc_exact", "found")
                elif (
                    isinstance(raw, dict)
                    and raw.get("success") is True
                    and raw.get("data") in (None, [])
                ):
                    _set_source("mexc_exact", "empty")
                    source_complete["mexc_exact"] = True
                elif source_status.get("mexc_exact") != "unavailable":
                    _mark_unavailable("mexc_exact")
            except Exception as e:
                _mark_unavailable("mexc_exact")
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
        selected = _select_recovery_batch(_recovery_rows(_budgeted(
            "order_recovery_fetch_open_orders",
            "open_orders",
            lambda: ex.fetch_open_orders(symbol_full),
        ), "open_orders"))
        if selected is not None:
            state = (
                "conflict"
                if selected.get("_bot_recovery_conflict") is True
                else "found"
            )
            _set_source("open_orders", state)
            _set_result(state)
            return selected
    except Exception as e:
        _mark_unavailable("open_orders")
        if log_event:
            try:
                log_event(f"clientOrderId lookup (open orders) failed for "
                          f"{symbol_full}: {type(e).__name__} - relying on "
                          f"server-side dedup", "WARN")
            except Exception:
                pass
    has = getattr(ex, "has", {}) or {}
    is_anchored_mexc_lookup = (
        _exchange_id(ex) == "mexc" and lookup_since_ms is not None
    )
    history_limit = 100 if is_anchored_mexc_lookup else 20

    def _fetch_history(method):
        if is_anchored_mexc_lookup:
            return method(
                symbol_full,
                since=lookup_since_ms,
                limit=history_limit,
            )
        return method(symbol_full, limit=history_limit)

    try:
        if has.get("fetchOrders"):
            selected = _select_recovery_batch(_recovery_rows(_budgeted(
                "order_recovery_fetch_orders",
                "orders",
                lambda: _fetch_history(ex.fetch_orders),
            ), "orders", page_limit=history_limit))
            if selected is not None:
                state = (
                    "conflict"
                    if selected.get("_bot_recovery_conflict") is True
                    else "found"
                )
                _set_source("orders", state)
                _set_result(state)
                return selected
    except Exception as e:
        _mark_unavailable("orders")
        if log_event:
            try:
                log_event(f"clientOrderId lookup (history) failed for "
                          f"{symbol_full}: {type(e).__name__}", "WARN")
            except Exception:
                pass
    # A just-filled MARKET order is no longer "open", and several supported
    # venues do not expose the unified fetchOrders. Such a fill surfaces in
    # closed orders / my-trades, so consult those before giving up; otherwise
    # the lost-response retry can fire a second entry in the fill/no-ack window.
    try:
        if has.get("fetchClosedOrders"):
            selected = _select_recovery_batch(_recovery_rows(_budgeted(
                "order_recovery_fetch_closed_orders",
                "closed_orders",
                lambda: _fetch_history(ex.fetch_closed_orders),
            ), "closed_orders", page_limit=history_limit))
            if selected is not None:
                state = (
                    "conflict"
                    if selected.get("_bot_recovery_conflict") is True
                    else "found"
                )
                _set_source("closed_orders", state)
                _set_result(state)
                return selected
    except Exception as e:
        _mark_unavailable("closed_orders")
        if log_event:
            try:
                log_event(f"clientOrderId lookup (closed orders) failed for "
                          f"{symbol_full}: {type(e).__name__}", "WARN")
            except Exception:
                pass
    try:
        if has.get("fetchMyTrades"):
            matching_trades = [
                trade
                for trade in _recovery_rows(_budgeted(
                    "order_recovery_fetch_my_trades",
                    "my_trades",
                    lambda: _fetch_history(ex.fetch_my_trades),
                ), "my_trades", page_limit=history_limit)
                if _order_client_id_matches(trade, cid)
            ]
            if matching_trades:
                recovered = _order_from_recovery_trades(
                    matching_trades,
                    cid,
                    symbol_full,
                    expected_amount=expected_amount,
                    exchange_id=exchange_id or _exchange_id(ex),
                )
                selected = _select_recovery_candidate(recovered)
                if selected is not None:
                    state = (
                        "conflict"
                        if selected.get("_bot_recovery_conflict") is True
                        else "found"
                    )
                    _set_source("my_trades", state)
                    _set_result(state)
                    return selected
    except Exception as e:
        _mark_unavailable("my_trades")
        if log_event:
            try:
                log_event(f"clientOrderId lookup (my trades) failed for "
                          f"{symbol_full}: {type(e).__name__}", "WARN")
            except Exception:
                pass
    if terminal_zero_fill is not None:
        _set_result("found")
        return terminal_zero_fill
    result = "unavailable" if any(
        state == "unavailable" for state in source_status.values()
    ) else "empty"
    if isinstance(lookup_status, dict):
        # Automatic absence recovery is intentionally MEXC-only.  A complete
        # negative quorum requires the venue-native exact lookup, the live
        # order book, at least one supported order-history endpoint, and the
        # trade ledger.  Every source that was attempted must be conclusively
        # empty; unsupported, denied, malformed, conflicting, or positive
        # evidence can therefore never be mistaken for absence.
        lookup_status["complete_negative"] = bool(
            _exchange_id(ex) == "mexc"
            and source_status.get("mexc_exact") == "empty"
            and source_status.get("open_orders") == "empty"
            and source_status.get("my_trades") == "empty"
            and (
                source_status.get("orders") == "empty"
                or source_status.get("closed_orders") == "empty"
            )
            and bool(source_status)
            and all(state == "empty" for state in source_status.values())
            and lookup_since_ms is not None
            and all(source_complete.get(source) is True for source in source_status)
            and lookup_status.get("budget_denied") is not True
        )
    _set_result(result)
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
    """Place a market order without blindly re-firing ambiguous entries.

    Reduce-only orders may retry with bounded exponential backoff because they
    cannot increase or reverse the venue position. A non-reduce-only create
    exception is reconciled once by client id and otherwise surfaced as
    ``FuturesOrderOutcomeUnknown``.
    """
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
    order_position_side = _requested_position_side(order_params)
    exchange_id = _exchange_id(ex)
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
    submission_attempted = False
    for attempt in range(1, attempts_limit + 1):
        try:
            reservation = try_consume_api_call(
                endpoint,
                critical=reduce_only,
                return_reservation=True,
            )
        except Exception as exc:
            error = f"API budget gate unavailable before {action_label}"
            if not submission_attempted:
                raise FuturesOrderNotSubmitted(error) from exc
            raise RuntimeError(error) from exc
        if not reservation:
            error = f"API budget exhausted before {action_label}"
            if not submission_attempted:
                raise FuturesOrderNotSubmitted(error)
            raise RuntimeError(error)

        def _record_create_error() -> None:
            if isinstance(reservation, ApiCallReservation):
                try:
                    record_api_error(endpoint, reservation)
                except Exception:
                    pass

        try:
            submission_attempted = True
            try:
                order = ex.create_order(
                    order_symbol, "market", order_side, order_amount,
                    params=order_params)
            except Exception:
                _record_create_error()
                raise
            cid = order_params.get("clientOrderId")
            client_id_conflict = _order_client_id_conflicts(order, cid)
            request_conflict = _order_request_conflicts(
                order,
                order_symbol,
                order_side,
                order_position_side,
                exchange_id,
                expected_reduce_only=reduce_only,
                expected_client_id=cid,
                expected_amount=order_amount,
            )
            if (
                not _order_response_has_evidence(order)
                or client_id_conflict
                or request_conflict
            ):
                _record_create_error()
                lookup_status = {}
                existing = _find_order_by_client_id(
                    ex,
                    order_symbol,
                    cid,
                    log_event=log_event,
                    lookup_status=lookup_status,
                    expected_amount=order_amount,
                    expected_side=order_side,
                    expected_position_side=order_position_side,
                    exchange_id=exchange_id,
                    expected_reduce_only=reduce_only,
                ) if cid else None
                recovered_request_conflict = _order_request_conflicts(
                    existing,
                    order_symbol,
                    order_side,
                    order_position_side,
                    exchange_id,
                    expected_reduce_only=reduce_only,
                    expected_client_id=cid,
                    expected_amount=order_amount,
                )
                if (
                    existing is not None
                    and _order_landed(existing)
                    and not recovered_request_conflict
                ):
                    if log_event:
                        try:
                            log_event(
                                f"{action_label}: recovered already-landed order "
                                "after malformed exchange response  NOT retrying",
                                "WARN")
                        except Exception:
                            pass
                    return existing
                if request_conflict or recovered_request_conflict:
                    raise FuturesOrderOutcomeUnknown(cid)
                if not reduce_only:
                    raise FuturesOrderOutcomeUnknown(cid)
                detail = (
                    "conflicting clientOrderId evidence"
                    if client_id_conflict
                    else (
                        "conflicting symbol/side evidence"
                        if request_conflict
                        else "missing order id/status/fill evidence"
                    )
                )
                raise _InvalidOrderResponse(
                    f"{action_label}: invalid exchange order response "
                    f"{type(order).__name__}; {detail}; not retrying to "
                    "avoid duplicate market order"
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
                _record_create_error()
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
                        order_id=(
                            _first_order_id_text(
                                order.get("id"),
                                order.get("orderId"),
                                order.get("order_id"),
                                order.get("orderID"),
                            ) or None
                        ) if isinstance(order, dict) else None,
                        filled=_finite_order_telemetry_value(
                            order.get("filled")
                        ) if isinstance(order, dict) else None,
                        avg_price=_finite_order_telemetry_value(
                            order.get("average"), positive=True
                        ) if isinstance(order, dict) else None,
                        action=action_label,
                    )
                except Exception:
                    pass
            return order
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if isinstance(e, (FuturesOrderOutcomeUnknown,
                              _InvalidOrderResponse)):
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
            # Reconcile whether the order actually LANDED under our
            # clientOrderId. A clean empty open-order result is not proof of
            # absence: an instant MARKET fill is already closed. Therefore an
            # unresolved entry is never re-fired; only reduce-only work may use
            # the bounded retry loop below.
            cid = order_params.get("clientOrderId")
            if cid:
                lookup_status = {}
                existing = _find_order_by_client_id(
                    ex,
                    order_symbol,
                    cid,
                    log_event=log_event,
                    lookup_status=lookup_status,
                    expected_amount=order_amount,
                    expected_side=order_side,
                    expected_position_side=order_position_side,
                    exchange_id=exchange_id,
                    expected_reduce_only=reduce_only,
                )
                recovered_request_conflict = _order_request_conflicts(
                    existing,
                    order_symbol,
                    order_side,
                    order_position_side,
                    exchange_id,
                    expected_reduce_only=reduce_only,
                    expected_client_id=cid,
                    expected_amount=order_amount,
                )
                if (
                    existing is not None
                    and _order_landed(existing)
                    and not recovered_request_conflict
                ):
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
                if recovered_request_conflict:
                    raise FuturesOrderOutcomeUnknown(cid) from e
                if not reduce_only:
                    raise FuturesOrderOutcomeUnknown(cid) from e
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
    observed: list[float] = []
    for key in ("contracts", "size"):
        raw = pos.get(key)
        if raw in (None, ""):
            continue
        if isinstance(raw, bool):
            return None
        try:
            parsed = float(raw)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(parsed):
            return None
        observed.append(parsed)
    if not observed:
        return None
    nonzero = [abs(value) for value in observed if value != 0.0]
    if not nonzero:
        return 0.0
    contracts = nonzero[0]
    for alias_value in nonzero[1:]:
        tolerance = max(1e-12, max(contracts, alias_value) * 1e-9)
        if abs(alias_value - contracts) > tolerance:
            return None
    return contracts


def _upper_currency_text(value) -> str:
    return value.strip().upper() if isinstance(value, str) else ""


def _order_with_fee_context(order, ex=None, symbol_full: str = "",
                            contract_size: Optional[float] = None) -> dict:
    payload = dict(order) if isinstance(order, dict) else {}
    if not payload:
        return payload
    if contract_size is not None:
        payload["_fee_contract_size"] = contract_size
    if symbol_full:
        payload["_fee_symbol_full"] = symbol_full
    if ex is not None:
        payload["_bot_ex"] = ex
    return payload


def _convert_fee_to_usdt_futures_known(
    fee_dict,
    order_dict,
    *,
    allow_discount_estimate: bool = True,
) -> tuple[float, bool]:
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
        converted = _discount_token_fee_to_usdt(
            currency,
            cost,
            order_dict,
            allow_estimate=allow_discount_estimate,
        )
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
                                order_dict, *,
                                allow_estimate: bool = True) -> float:
    """Convert a fee paid in a discount token to USDT (best-effort).

    Prefers the token's current price (``ex.fetch_ticker('<TOKEN>/USDT')`` via
    the exchange stashed on the order); falls back to a notional-based estimate
    using the order's own fill. Returns 0.0 only when nothing is usable."""
    if not isinstance(order_dict, dict):
        return 0.0
    ex = order_dict.get("_bot_ex")
    if ex is not None:
        try:
            reservation = try_consume_api_call(
                "futures_fee_conversion_fetch_ticker",
                critical=True,
                return_reservation=True,
            )
        except Exception:
            reservation = False
        if reservation:
            try:
                try:
                    symbol = f"{currency}/USDT"
                    ticker = ex.fetch_ticker(symbol)
                    if not explicit_trade_symbol_matches(ticker, symbol):
                        raise ValueError(
                            "futures fee ticker returned a symbol mismatch"
                        )
                    px = _first_positive_float(
                        (ticker or {}).get("last"),
                        (ticker or {}).get("close"),
                    )
                    if px <= 0:
                        raise ValueError(
                            "futures fee ticker returned no positive price"
                        )
                except Exception:
                    if isinstance(reservation, ApiCallReservation):
                        try:
                            record_api_error(
                                "futures_fee_conversion_fetch_ticker",
                                reservation,
                            )
                        except Exception:
                            pass
                    raise
                converted = round(cost * px, 6)
                if math.isfinite(converted):
                    return converted
            except Exception:
                pass
    if cost < 0 or not allow_estimate:
        return 0.0
    return _estimate_futures_order_fee_from_payload(order_dict)


def _estimate_futures_order_fee_from_payload(order_dict: dict) -> float:
    """Estimate the whole order fee once from fill notional."""
    if not isinstance(order_dict, dict):
        return 0.0
    ex = order_dict.get("_bot_ex")
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
        saw_unknown = False
        total = 0.0
        for fee_dict in fees_list:
            if not isinstance(fee_dict, dict):
                saw_fee = True
                saw_unknown = True
                continue
            if "cost" not in fee_dict or fee_dict.get("cost") is None:
                saw_fee = True
                saw_unknown = True
                continue
            saw_fee = True
            fee, known = _convert_fee_to_usdt_futures_known(
                fee_dict,
                order,
                allow_discount_estimate=False,
            )
            if known:
                saw_known = True
                total += fee
            else:
                saw_unknown = True
        if saw_fee:
            if saw_unknown:
                singular_fee, singular_known = _convert_fee_to_usdt_futures_known(
                    order.get("fee") or {}, order
                )
                if singular_known:
                    return singular_fee, True
                # A notional fallback describes the whole order, not one fee
                # row. Apply it once and never add it per discount-token item.
                estimate = _estimate_futures_order_fee_from_payload(order)
                best = max(total, estimate)
                return (
                    best if math.isfinite(best) else 0.0,
                    False,
                )
            if saw_known:
                return total if math.isfinite(total) else 0.0, math.isfinite(total)
            singular_fee, singular_known = _convert_fee_to_usdt_futures_known(
                order.get("fee") or {}, order)
            if singular_known:
                return singular_fee, True
            return 0.0, False
    return _convert_fee_to_usdt_futures_known(order.get("fee") or {}, order)


FUTURES_DEFAULT_TAKER_FEE = 0.001


def futures_contract_size_or_none(ex, symbol_full: str) -> float | None:
    """Return a positive contract size only when market metadata proves it."""
    try:
        markets = getattr(ex, "markets", None) or {}
        market = markets.get(symbol_full) or {}
        info = market.get("info") or {}
        if not isinstance(info, dict):
            info = {}
        for candidate in (
            market.get("contractSize"),
            market.get("contract_size"),
            info.get("contractSize"),
            info.get("contract_size"),
        ):
            if candidate is None or isinstance(candidate, bool):
                continue
            try:
                value = float(candidate)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(value) and value > 0:
                return value
    except (TypeError, ValueError, OverflowError, AttributeError):
        pass
    return None


def futures_contract_size(ex, symbol_full: str) -> float:
    """Contract size for general futures calculations (legacy default 1.0).

    USDT-M perpetuals are usually 1, but some contracts (1000SATS, MEME, Bybit
  inverse, ) use 100/1000/etc. Any fee/notional math MUST multiply by this,
    otherwise the value is wrong by the contractSize factor.
    """
    return futures_contract_size_or_none(ex, symbol_full) or 1.0


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
    estimate_payload = order_payload

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
                reservation = try_consume_api_call(
                    "futures_fee_fetch_order",
                    critical=True,
                    return_reservation=True,
                )
            except Exception:
                break
            if not reservation:
                break
            try:
                refreshed = ex.fetch_order(str(order_id), symbol_full)
                if not isinstance(refreshed, dict) or not refreshed:
                    raise TypeError(
                        "futures fee refetch returned no order object"
                    )
                refreshed_order_ids = _explicit_order_ids(refreshed)
                if len(refreshed_order_ids) > 1 or (
                    refreshed_order_ids
                    and refreshed_order_ids != {str(order_id)}
                ):
                    raise ValueError(
                        "futures fee refetch changed order id"
                    )
                raw_refreshed_symbol = refreshed.get("symbol")
                if raw_refreshed_symbol not in (None, "") and (
                    _normalize_order_symbol(raw_refreshed_symbol)
                    != _normalize_order_symbol(symbol_full)
                ):
                    raise ValueError(
                        "futures fee refetch changed order symbol"
                    )
                refreshed_payload = _order_with_fee_context(
                    refreshed,
                    ex=ex,
                    symbol_full=symbol_full,
                    contract_size=contract_size,
                )
                if _first_positive_float(
                    refreshed_payload.get("filled"),
                    refreshed_payload.get("amount"),
                ) > 0:
                    estimate_payload = refreshed_payload
                real, fee_known = _extract_order_fee_futures_known(
                    refreshed_payload
                )
                if fee_known:
                    return real
            except Exception:
                if isinstance(reservation, ApiCallReservation):
                    try:
                        record_api_error(
                            "futures_fee_fetch_order", reservation
                        )
                    except Exception:
                        pass
                continue

    # Estimate fallback
    # Prefer the order's own fill; fall back to what the CALLER actually traded
    # when the response carries no fill yet. Notional ALWAYS includes contractSize.
    filled = 0.0
    for candidate in (
        estimate_payload.get("filled"),
        estimate_payload.get("amount"),
    ):
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

def position_row_side(pos: object) -> tuple[str, bool]:
    """Return (long|short|unknown, contradictory) for a position row."""
    if not isinstance(pos, dict):
        return "", True
    info = pos.get("info") if isinstance(pos.get("info"), dict) else {}
    observed: set[str] = set()
    unknown_explicit = False
    for key in ("side", "positionSide", "posSide", "holdSide", "direction"):
        for raw in (pos.get(key), info.get(key)):
            if raw in (None, ""):
                continue
            if not isinstance(raw, str):
                unknown_explicit = True
                continue
            normalized = raw.strip().lower()
            if normalized in {"long", "buy"}:
                observed.add("long")
            elif normalized in {"short", "sell"}:
                observed.add("short")
            elif normalized not in {"", "both", "net", "oneway"}:
                unknown_explicit = True
    if unknown_explicit or len(observed) > 1:
        return "", True

    signed_short = False
    for key in ("contracts", "size"):
        raw = pos.get(key)
        if raw in (None, ""):
            continue
        if isinstance(raw, bool):
            return "", True
        try:
            parsed = float(raw)
        except (TypeError, ValueError, OverflowError):
            return "", True
        if not math.isfinite(parsed):
            return "", True
        signed_short |= parsed < 0
    if observed:
        side = next(iter(observed))
        if signed_short and side != "short":
            return "", True
        return side, False
    return ("short", False) if signed_short else ("", False)


def _validated_position_rows(raw) -> Optional[list[dict]]:
    """Return a trusted CCXT position list or ``None`` for malformed data."""
    if not isinstance(raw, (list, tuple)):
        return None
    rows: list[dict] = []
    for row in raw:
        if not isinstance(row, dict):
            return None
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            return None
        rows.append(row)
    return rows


def fetch_open_position(
    ex,
    symbol_full: str,
    *,
    expected_position_side: Optional[str] = None,
    _api_endpoint_prefix: str = "fetch_positions:",
    _api_critical: bool = False,
) -> Tuple[Optional[dict], bool]:
    """Return the open exchange position for ``symbol_full``.

    MEXC/CCXT can occasionally return an empty list for symbol-scoped
    ``fetch_positions([symbol])`` while the global positions endpoint still
    contains the just-filled position. Entry paths must treat that as
    "verification incomplete", not "no position", otherwise a live order can
    become an unmanaged orphan.

    Returns ``(position, unavailable)``. ``unavailable=True`` means the
    exchange position endpoint could not be trusted and callers should keep or
    create provisional state instead of deleting claims/state.

    The private endpoint options let reconciliation use its dedicated critical
    API-budget namespace while reusing this exact position-selection contract.
    """
    if (
        not isinstance(symbol_full, str)
        or not symbol_full.strip()
        or symbol_full != symbol_full.strip()
    ):
        return None, True
    try:
        from config.exchange_config import safe_fetch_positions
    except Exception:
        return None, True

    expected_side = ""
    if expected_position_side not in (None, ""):
        if (
            not isinstance(expected_position_side, str)
            or expected_position_side != expected_position_side.strip()
        ):
            return None, True
        expected_side = _normalize_order_position_side(expected_position_side)
        if not expected_side:
            return None, True

    try:
        scoped_raw = safe_fetch_positions(
            ex,
            [symbol_full],
            endpoint=f"{_api_endpoint_prefix}scoped",
            critical=_api_critical,
            raise_on_budget_denied=True,
        )
        if scoped_raw is None:
            positions = None
        else:
            positions = _validated_position_rows(scoped_raw)
            if positions is None:
                return None, True
        scoped_has_symbol = False
        scoped_unavailable = False
        if positions is not None:
            for candidate in positions:
                if candidate["symbol"] != symbol_full:
                    continue
                scoped_contracts = _position_contracts_abs(candidate)
                if scoped_contracts is None:
                    scoped_unavailable = True
                    continue
                if scoped_contracts <= 1e-8:
                    continue
                if not expected_side:
                    scoped_has_symbol = True
                    continue
                observed_side, contradictory = position_row_side(candidate)
                if contradictory or not observed_side:
                    scoped_unavailable = True
                    continue
                if observed_side == expected_side:
                    scoped_has_symbol = True
        if positions is None or not scoped_has_symbol:
            global_raw = safe_fetch_positions(
                ex,
                endpoint=f"{_api_endpoint_prefix}global",
                critical=_api_critical,
                raise_on_budget_denied=True,
            )
            if global_raw is None:
                return None, True
            positions = _validated_position_rows(global_raw)
            if positions is None:
                return None, True
        if scoped_unavailable:
            return None, True
    except Exception:
        return None, True

    matching: list[dict] = []
    unknown_matching_leg = False
    for pos in positions:
        if pos["symbol"] != symbol_full:
            continue
        contracts = _position_contracts_abs(pos)
        if contracts is None:
            return None, True
        if contracts > 1e-8:
            if not expected_side:
                matching.append(pos)
                continue
            observed_side, contradictory = position_row_side(pos)
            if contradictory or not observed_side:
                unknown_matching_leg = True
                continue
            if observed_side == expected_side:
                matching.append(pos)
    if unknown_matching_leg or len(matching) > 1:
        return None, True
    if matching:
        return matching[0], False
    return None, False


def verify_position_closed(
    ex,
    symbol_full: str,
    timeout: float = 5.0,
    *,
    expected_position_side: Optional[str] = None,
) -> Tuple[bool, float]:
    """Confirm via fetch_positions that contracts == 0 after a close.

    The ``timeout`` parameter is enforced via a monotonic deadline.

    Returns (closed, remaining_contracts).
    remaining_contracts == -1.0 indicates verification itself failed
  (auth/network/timeout)  caller should keep state and retry. When
    ``safe_fetch_positions`` returns None (auth/network glitch), return
  ``(False, -1.0)``  same pessimistic signal as an exception.
    """
    if (
        not isinstance(symbol_full, str)
        or not symbol_full.strip()
        or symbol_full != symbol_full.strip()
    ):
        return False, -1.0
    if expected_position_side not in (None, ""):
        if (
            not isinstance(expected_position_side, str)
            or expected_position_side != expected_position_side.strip()
        ):
            return False, -1.0
        normalized_side = _normalize_order_position_side(expected_position_side)
        if not normalized_side:
            return False, -1.0
        expected_position_side = normalized_side
    if isinstance(timeout, bool):
        return False, -1.0
    try:
        timeout_value = float(timeout)
    except (TypeError, ValueError, OverflowError):
        return False, -1.0
    if not math.isfinite(timeout_value) or timeout_value < 0.0:
        return False, -1.0
    deadline = time.monotonic() + timeout_value
    # Do at most 2 attempts within the window, with a small gap. If both
    # exceed the deadline, report "not verified" so the caller keeps state.
    last_remaining = -1.0
    attempts = 0
    while attempts < 2 and (attempts == 0 or time.monotonic() < deadline):
        attempts += 1
        try:
            pos, unavailable = fetch_open_position(
                ex,
                symbol_full,
                expected_position_side=expected_position_side,
                _api_critical=True,
            )
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

        if attempts >= 2:
            break
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
        info = m.get("info") if isinstance(m, dict) else None
        info = info if isinstance(info, dict) else {}
        for key in ("maintenanceMarginRate", "maintMarginRate",
                     "maintenance_margin", "mm"):
            for source in (m, info):
                v = source.get(key)
                if v is None or isinstance(v, bool):
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError, OverflowError):
                    continue
                if math.isfinite(fv) and 0 < fv < 0.5:
                    return fv
        return default
    except Exception:
        return default


def get_exchange_liq_price(
    ex,
    symbol_full: str,
    *,
    expected_position_side: Optional[str] = None,
) -> float:
    """Fetch the liquidation price of one unambiguous open position leg."""
    try:
        from config.exchange_config import safe_fetch_positions
        expected_side = ""
        if expected_position_side not in (None, ""):
            expected_side = _normalize_order_position_side(
                expected_position_side
            )
            if not expected_side:
                return 0.0
        positions = safe_fetch_positions(
            ex,
            endpoint="futures_liquidation_fetch_positions",
            critical=True,
        )
        positions = _validated_position_rows(positions)
        if positions is None:
            return 0.0
        candidates = []
        for p in positions:
            if p["symbol"] != symbol_full:
                continue
            quantity_present = any(
                p.get(key) not in (None, "") for key in ("contracts", "size")
            )
            if quantity_present:
                contracts = _position_contracts_abs(p)
                if contracts is None:
                    return 0.0
                if contracts <= 1e-8:
                    continue
            observed_side, contradictory = position_row_side(p)
            if contradictory:
                return 0.0
            liq_price = 0.0
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
                            liq_price = fv
                            break
                    except (TypeError, ValueError, OverflowError):
                        continue
            candidates.append((observed_side, liq_price))
        if expected_side:
            matching = [
                price
                for side, price in candidates
                if side == expected_side
            ]
            unknown = [price for side, price in candidates if not side]
            if len(matching) == 1 and not unknown:
                return matching[0]
            # One-way venues commonly omit an explicit position side.  Such a
            # row is safe only when it is the sole open symbol candidate.
            if not matching and len(candidates) == 1 and len(unknown) == 1:
                return unknown[0]
            return 0.0
        return candidates[0][1] if len(candidates) == 1 else 0.0
    except Exception:
        return 0.0
