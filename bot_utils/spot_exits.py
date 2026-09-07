"""
bot_utils/spot_exits.py  Spot market-sell + emergency-close helpers.
"""
from __future__ import annotations


import math
from decimal import Decimal, ROUND_DOWN
from typing import Tuple, Callable, Optional

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.network_retry import RetryForbiddenError
from bot_utils.order_utils import (
    explicit_trade_symbol_matches,
    extract_fill_price,
    extract_order_fee,
    order_has_proven_zero_fill,
    order_id_text_or_none,
    strict_order_snapshot_equal,
)

_EMERGENCY_RESIDUAL_DUST_USDT = 1.0
_SPOT_KNOWN_ORDER_STATUSES = frozenset({
    "new",
    "open",
    "pending",
    "partially_filled",
    "partiallyfilled",
    "closed",
    "filled",
    "canceled",
    "cancelled",
    "expired",
    "rejected",
})
_SPOT_TERMINAL_ORDER_STATUSES = frozenset({
    "closed",
    "filled",
    "canceled",
    "cancelled",
    "expired",
    "rejected",
})


#  Helpers 

def _finite_float(value, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if math.isfinite(parsed) else default


def _positive_finite(value, default: float = 0.0) -> float:
    parsed = _finite_float(value, default)
    return parsed if parsed > 0 else default


def has_pending_spot_partial_exit(row: dict) -> bool:
    """Return whether a physical SPOT partial exit still needs recovery."""
    if not isinstance(row, dict) or row.get("partial_sold"):
        return False
    return bool(
        row.get("partial_exit_outcome_uncertain")
        or order_id_text_or_none(row.get("partial_exit_client_order_id"))
    )


def _validated_expected_spot_amount(value) -> Optional[float]:
    if value is None:
        return None
    parsed = _positive_finite(value)
    if parsed <= 0:
        raise RuntimeError("spot order recovery expected amount invalid")
    return parsed


def _external_text(value) -> str:
    try:
        return str(value).strip().lower()
    except Exception:
        return ""


def normalize_spot_order_status(value) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower().replace("-", "_")


def spot_sell_requires_terminal_recovery(
    order,
    expected_amount,
) -> bool:
    """Return whether a sell ACK cannot yet be booked safely.

    A positive partial fill is not sufficient evidence that the remainder is
    no longer live at the venue. Only an explicit terminal status permits
    accounting/state mutation. Statusless full fills remain compatible with
    exchanges that omit status but report the complete requested quantity.
    """
    if not isinstance(order, dict):
        return True
    raw_status = order.get("status")
    if raw_status not in (None, ""):
        if not isinstance(raw_status, str):
            return True
        normalized_status = normalize_spot_order_status(raw_status)
        if normalized_status not in _SPOT_KNOWN_ORDER_STATUSES:
            return True
        return (
            normalized_status not in _SPOT_TERMINAL_ORDER_STATUSES
        )

    requested = _positive_finite(expected_amount)
    raw_filled = order.get("filled")
    if "filled" in order and raw_filled not in (None, ""):
        if isinstance(raw_filled, bool):
            return True
        try:
            explicit_filled = float(raw_filled)
        except (TypeError, ValueError, OverflowError):
            return True
        if not math.isfinite(explicit_filled) or explicit_filled <= 0:
            return True
    filled = _positive_finite(raw_filled)
    remaining = _positive_finite(order.get("remaining"))
    if remaining > 0:
        return True
    if requested <= 0 or filled <= 0:
        return False
    tolerance = max(1e-12, requested * 1e-9)
    return filled + tolerance < requested


def _positive_finite_decimal(value) -> Optional[Decimal]:
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _precision_mode_is_tick_size(ex, market: dict) -> bool:
    mode = market.get("precisionMode")
    if mode is None:
        mode = getattr(ex, "precisionMode", None)
    if isinstance(mode, str):
        return mode.strip().lower() in {"tick_size", "ticksize", "tick-size"}
    try:
        # ccxt.TICK_SIZE is 4; avoid importing ccxt in this hot helper.
        return int(mode) == 4
    except (TypeError, ValueError, OverflowError):
        return False


def _utc_now_str() -> str:
    """UTC timestamp  lazy import to avoid circular dependency."""
    from core.logger import _date
    return _date()


def _exchange_min_amount(ex, symbol_pair: str) -> Optional[Decimal]:
    """Read the minimum tradable BASE amount (lot step) from CCXT market
    metadata.

    Returns a Decimal value or None if metadata unavailable. Caller
    uses this as the step for rounding instead of a hardcoded 0.0001.
    """
    try:
        markets = getattr(ex, "markets", None)
        if not isinstance(markets, dict):
            return None
        mkt = markets.get(symbol_pair)
        if not isinstance(mkt, dict):
            return None
        limits = mkt.get("limits") or {}
        amt_limits = limits.get("amount") or {}
        mn = amt_limits.get("min")
        if mn is None:
            return None
        return _positive_finite_decimal(mn)
    except Exception:
        return None


def _exchange_precision_step(ex, symbol_pair: str) -> Optional[Decimal]:
    """Get the precision step from market metadata (e.g. 0.001 for BTC,
    1 for SHIB-style coins). Returns None if unavailable."""
    try:
        markets = getattr(ex, "markets", None)
        if not isinstance(markets, dict):
            return None
        mkt = markets.get(symbol_pair)
        if not isinstance(mkt, dict):
            return None
        prec = (mkt.get("precision") or {}).get("amount")
        if isinstance(prec, bool):
            return None
        if prec is None:
            return None
        # CCXT precision can be either an int (decimal places) or
        # a Decimal-like step depending on the exchange.
        try:
            pf = float(prec)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(pf):
            return None
        if _precision_mode_is_tick_size(ex, mkt):
            return _positive_finite_decimal(prec)
        if pf == 0:
            return Decimal("1")
        if pf >= 1 and pf == int(pf):
            # Likely "number of decimal places"  convert to step
            return Decimal(10) ** -int(pf)
        if pf > 0:
            return _positive_finite_decimal(pf)
        return None
    except Exception:
        return None


def _round_to_step(amount: Decimal, step: Decimal) -> Decimal:
    """Round amount down to the nearest multiple of step."""
    if step <= 0:
        return amount
    return (amount / step).to_integral_value(rounding=ROUND_DOWN) * step


#  Precision-safe market sell 

class InsufficientSellBalance(Exception):
    """Raised by ``spot_market_sell_safe`` when the free BASE balance is below
    the minimum sellable amount  i.e. there is effectively nothing left to sell
    (coins already gone, or only dust remains). Callers should treat the
    position as closed/orphaned and remove it from state instead of retrying
    forever  otherwise MEXC ``30005 'Oversold'`` loops every monitor tick.

    The message contains 'insufficient balance' on purpose so the
    network-retry layer classifies it as permanent (no wasted retries).
    """
    def __init__(self, symbol_pair: str, requested: float, free: float):
        self.symbol_pair = symbol_pair
        self.requested = requested
        self.free = free
        super().__init__(
            f"{symbol_pair}: insufficient balance to sell  free base "
            f"{free:.10g}, requested {requested:.10g} (nothing left to sell)")


class _SpotSellBudgetUnavailable(RuntimeError):
    """A sell request was not sent because its atomic budget gate failed."""


class SpotSellOutcomeUnknown(RetryForbiddenError):
    """A SPOT sell may exist, but its create response was not recovered."""

    def __init__(self, client_order_id: str):
        self.client_order_id = client_order_id
        super().__init__(
            "spot sell outcome unknown "
            f"(clientOrderId={client_order_id})"
        )


def ensure_spot_exit_client_order_id(state, sym: str, row: dict,
                                     leg: str, bot_name: str) -> str:
    """Confirm a generation-bound, durable id before a physical SPOT sell."""
    from bot_utils.trade_state import same_position_generation, update_many_if_current

    key = f"{leg}_exit_client_order_id"
    getter = getattr(state, "get", None)
    current = getter(sym) if callable(getter) else row
    if callable(getter) and not same_position_generation(current, row):
        raise RuntimeError(
            f"cannot persist {key}: position generation changed for {sym}"
        )
    existing = order_id_text_or_none(current.get(key))
    if existing and existing.isascii() and len(existing) <= 32:
        client_order_id = existing
    else:
        from uuid import uuid4
        from trading.execution_quality import make_client_order_id

        entry_id = order_id_text_or_none(row.get("entry_id")) or "legacy"
        intent_id = f"{entry_id}:{uuid4().hex}"
        client_order_id = make_client_order_id(
            intent_id, f"{bot_name}:{sym}:{leg}", prefix="sx"
        )
    # A failed write leaves the id in RAM. Retry the same id, not a new intent.
    if callable(getattr(state, "update_many", None)):
        persisted = update_many_if_current(state, sym, {key: client_order_id}, row)
    else:
        persisted = state.update(sym, key, client_order_id)
    if persisted is not None and persisted is not True:
        raise RuntimeError(
            f"cannot persist {key} before live SPOT sell for {sym}"
        )
    if callable(getter):
        current = getter(sym)
        # An obsolete conditional update is a successful no-op, not permission
        # to submit an order against the successor position.
        if (
            not same_position_generation(current, row)
            or current.get(key) != client_order_id
        ):
            raise RuntimeError(
                f"cannot persist {key}: position generation changed for {sym}"
            )
    row[key] = client_order_id
    return client_order_id


def rollback_spot_entry_after_state_failure(
        ex, symbol_pair: str, raw_amount: float, *, entry_id, bot_name: str
        ) -> Tuple[dict, float]:
    """Sell an unpersisted live entry with a restart-stable client id.

    The entry state could not be written, so there is no safe place to persist
    a fresh exit intent.  The already-stable entry id therefore anchors the
    rollback id and lets ``spot_market_sell_safe`` reconcile a lost response.
    """
    stable_entry_id = order_id_text_or_none(entry_id)
    stable_bot_name = order_id_text_or_none(bot_name)
    stable_symbol = order_id_text_or_none(symbol_pair)
    if not stable_entry_id:
        raise ValueError("stable entry_id required for live SPOT rollback")
    if not stable_bot_name or not stable_symbol:
        raise ValueError("bot_name and symbol required for live SPOT rollback")

    from trading.execution_quality import make_client_order_id

    client_order_id = make_client_order_id(
        stable_entry_id,
        f"{stable_bot_name}:{stable_symbol}:state-rollback",
        prefix="sx",
    )
    return spot_market_sell_safe(
        ex,
        stable_symbol,
        raw_amount,
        client_order_id=client_order_id,
    )


def spot_entry_rollback_was_fully_filled(order: dict,
                                         requested_amount: float) -> bool:
    """Require quantitative proof that an untracked entry was fully sold."""
    requested = _positive_finite(requested_amount)
    if not isinstance(order, dict) or requested <= 0:
        return False
    filled = _positive_finite(order.get("filled"))
    if filled <= 0:
        return False
    tolerance = max(1e-12, requested * 1e-9)
    return filled + tolerance >= requested


def _create_market_sell_budgeted(ex, symbol_pair: str, amount: float,
                                 client_order_id: Optional[str] = None):
    valid_symbol = (
        isinstance(symbol_pair, str)
        and symbol_pair.count("/") == 1
        and all(part.strip() for part in symbol_pair.split("/", 1))
    )
    stable_client_order_id = (
        order_id_text_or_none(client_order_id)
        if client_order_id is not None
        else None
    )
    if (
        not valid_symbol
        or _positive_finite(amount) <= 0.0
        or (
            client_order_id is not None
            and stable_client_order_id is None
        )
    ):
        raise ValueError("invalid SPOT market sell request")
    client_order_id = stable_client_order_id
    try:
        reservation = try_consume_api_call(
            "spot_exit_create_market_sell",
            critical=True,
            return_reservation=True,
        )
    except Exception as exc:
        raise _SpotSellBudgetUnavailable(
            "API budget gate unavailable before spot market sell"
        ) from exc
    if not reservation:
        raise _SpotSellBudgetUnavailable(
            "API budget exhausted before spot market sell"
        )
    try:
        if client_order_id:
            return ex.create_market_sell_order(
                symbol_pair, amount, {"clientOrderId": client_order_id}
            )
        return ex.create_market_sell_order(symbol_pair, amount)
    except Exception:
        if isinstance(reservation, ApiCallReservation):
            try:
                record_api_error(
                    "spot_exit_create_market_sell", reservation
                )
            except Exception:
                pass
        raise


def _spot_order_ids(row: dict, *, trade: bool) -> set[str]:
    keys = (
        ("order", "orderId", "order_id", "orderID")
        if trade
        else ("id", "orderId", "order_id", "orderID")
    )
    values = {
        value
        for key in keys
        if (value := order_id_text_or_none(row.get(key)))
    }
    info = row.get("info")
    if isinstance(info, dict):
        info_keys = ("orderId", "order_id", "orderID", "ordId")
        if not trade:
            info_keys += ("id",)
        for key in info_keys:
            value = order_id_text_or_none(info.get(key))
            if value:
                values.add(value)
    return values


def _spot_trade_ids(trade: dict) -> set[str]:
    values = {
        value
        for key in (
            "id", "tradeId", "trade_id", "tradeID", "execId",
            "exec_id", "executionId", "fillId", "fill_id",
        )
        if (value := order_id_text_or_none(trade.get(key)))
    }
    info = trade.get("info")
    if isinstance(info, dict):
        for key in (
            "id",
            "tradeId",
            "trade_id",
            "tradeID",
            "execId",
            "exec_id",
            "executionId",
            "fillId",
            "fill_id",
        ):
            value = order_id_text_or_none(info.get(key))
            if value:
                values.add(value)
    return values


def aggregate_spot_order_trades(
    trades: list,
    client_order_id: str,
    *,
    symbol_pair: Optional[str] = None,
    expected_side: Optional[str] = None,
    expected_amount: Optional[float] = None,
) -> dict:
    """Normalize every trade fill for one client id into one order-shaped row."""
    from bot_utils.futures_order import (
        _order_client_id_conflicts,
        _order_client_id_matches,
    )
    requested_amount = _validated_expected_spot_amount(expected_amount)

    amounts = []
    costs = []
    fees = []
    order_ids = set()
    unique_trades = []
    seen_trade_ids = {}
    for trade in trades:
        if not isinstance(trade, dict):
            raise RuntimeError("spot order trade reconciliation row malformed")
        _validate_explicit_spot_identity_fields(
            trade,
            context="order trade reconciliation",
            trade=True,
        )
        if (
            _order_client_id_conflicts(trade, client_order_id)
            or not _order_client_id_matches(trade, client_order_id)
        ):
            raise RuntimeError(
                "spot order trade reconciliation client id conflict"
            )
        if symbol_pair is not None:
            observed_symbol = trade.get("symbol")
            if observed_symbol not in (None, "") and (
                not isinstance(observed_symbol, str)
                or observed_symbol.strip() != symbol_pair
            ):
                raise RuntimeError(
                    "spot order trade reconciliation symbol conflict"
                )
        if expected_side is not None:
            observed_side = trade.get("side")
            if observed_side not in (None, "") and (
                not isinstance(observed_side, str)
                or observed_side.strip().lower() != expected_side
            ):
                raise RuntimeError(
                    "spot order trade reconciliation side conflict"
                )
        trade_ids = _spot_trade_ids(trade)
        if len(trade_ids) != 1:
            raise RuntimeError(
                "spot order trade reconciliation requires one stable execution id"
            )
        trade_id = next(iter(trade_ids))
        previous = seen_trade_ids.get(trade_id)
        if previous is not None:
            if not strict_order_snapshot_equal(previous, trade):
                raise RuntimeError(
                    "spot order trade reconciliation duplicate trade conflict"
                )
            continue
        seen_trade_ids[trade_id] = dict(trade)
        unique_trades.append(trade)

    for trade in unique_trades:
        amount = _positive_finite(trade.get("amount"))
        if amount <= 0:
            raise RuntimeError("spot order trade reconciliation has invalid amount")
        amounts.append(amount)

        cost = _positive_finite(trade.get("cost"))
        if cost <= 0:
            price = _positive_finite(trade.get("price"))
            cost = amount * price if price > 0 else 0.0
        costs.append(cost)

        order_ids.update(_spot_order_ids(trade, trade=True))
        trade_fees = trade.get("fees")
        valid_trade_fees = (
            [dict(item) for item in trade_fees if isinstance(item, dict)]
            if isinstance(trade_fees, list) else []
        )
        if valid_trade_fees:
            fees.extend(valid_trade_fees)
        else:
            fee = trade.get("fee")
            if isinstance(fee, dict):
                fees.append(dict(fee))

    if len(order_ids) > 1:
        raise RuntimeError("spot order trade reconciliation matched many orders")
    filled = math.fsum(amounts)
    if not math.isfinite(filled) or filled <= 0:
        raise RuntimeError("spot order trade reconciliation has no valid fill")
    if requested_amount is not None and filled > requested_amount + max(
        1e-12, requested_amount * 1e-9
    ):
        raise RuntimeError("spot order recovery exceeded expected amount")

    recovered = dict(unique_trades[0])
    recovered.pop("id", None)
    recovered.pop("tradeId", None)
    raw_info = recovered.get("info")
    if isinstance(raw_info, dict):
        recovered["info"] = dict(raw_info)
        recovered["info"].pop("id", None)
    recovered["clientOrderId"] = client_order_id
    recovered["filled"] = filled
    if order_ids:
        recovered["id"] = next(iter(order_ids))
    if all(cost > 0 for cost in costs):
        total_cost = math.fsum(costs)
        if math.isfinite(total_cost) and total_cost > 0:
            recovered["cost"] = total_cost
            recovered["average"] = total_cost / filled
    if fees:
        recovered["fees"] = fees
    return recovered


def _aggregate_spot_exit_trades(trades: list,
                                client_order_id: str) -> dict:
    """Backward-compatible private alias for existing exit callers/tests."""
    return aggregate_spot_order_trades(trades, client_order_id)


def spot_recovery_terminal_disposition(order: dict) -> str:
    if not isinstance(order, dict):
        return "other"
    status = order.get("status")
    normalized_status = normalize_spot_order_status(status)
    if normalized_status not in {
        "canceled", "cancelled", "expired", "rejected", "closed", "filled",
    }:
        return "other"
    parsed_values = {}
    for key in ("filled", "cost"):
        value = order.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            raise RuntimeError("spot terminal fill evidence invalid")
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            raise RuntimeError("spot terminal fill evidence invalid")
        if not math.isfinite(parsed) or parsed < 0:
            raise RuntimeError("spot terminal fill evidence invalid")
        parsed_values[key] = parsed
    filled = parsed_values.get("filled", 0.0)
    cost = parsed_values.get("cost", 0.0)
    if normalized_status == "rejected" and (filled > 0 or cost > 0):
        raise RuntimeError("spot rejected order has fill evidence")
    if filled > 0:
        return "filled"
    if normalized_status in {"closed", "filled"}:
        return "unresolved"
    if cost > 0:
        return "unresolved"
    if order_has_proven_zero_fill(order):
        return "zero"
    return "unresolved"


def spot_recovery_is_terminal_zero_fill(order: dict) -> bool:
    return spot_recovery_terminal_disposition(order) == "zero"


def _validate_explicit_spot_identity_fields(
    order: dict,
    *,
    context: str,
    trade: bool = False,
) -> None:
    """Reject identities that cannot be stable scalar venue references."""
    from bot_utils.futures_order import _CLIENT_ID_INFO_KEYS

    raw_info = order.get("info")
    if raw_info not in (None, "") and not isinstance(raw_info, dict):
        raise RuntimeError(f"spot {context} info malformed")
    info = raw_info if isinstance(raw_info, dict) else {}

    def _reject_malformed(source: dict, keys, label: str) -> None:
        for key in keys:
            if key not in source:
                continue
            raw_value = source.get(key)
            if raw_value is None or (
                isinstance(raw_value, str) and not raw_value.strip()
            ):
                continue
            if isinstance(raw_value, bool) or not isinstance(
                raw_value, (str, int)
            ):
                raise RuntimeError(f"spot {context} {label} invalid")
            if order_id_text_or_none(raw_value) is None:
                raise RuntimeError(f"spot {context} {label} invalid")

    order_keys = (
        ("order", "orderId", "order_id", "orderID")
        if trade else ("id", "orderId", "order_id", "orderID")
    )
    _reject_malformed(order, order_keys, "order id")
    _reject_malformed(
        info,
        (
            ("orderId", "order_id", "orderID", "ordId")
            if trade
            else ("id", "orderId", "order_id", "orderID", "ordId")
        ),
        "order id",
    )
    if trade:
        execution_keys = (
            "id",
            "tradeId",
            "trade_id",
            "tradeID",
            "execId",
            "exec_id",
            "executionId",
            "fillId",
            "fill_id",
        )
        _reject_malformed(order, execution_keys, "execution id")
        _reject_malformed(info, execution_keys, "execution id")
    _reject_malformed(order, ("clientOrderId",), "client id")
    _reject_malformed(info, _CLIENT_ID_INFO_KEYS, "client id")


def _validate_spot_quote_cost(
    order: dict,
    max_quote_cost: Optional[float],
    *,
    error_prefix: str,
) -> None:
    authorized_quote_cost = _validated_expected_spot_amount(max_quote_cost)
    if authorized_quote_cost is None:
        return
    raw_cost = order.get("cost")
    observed_cost = 0.0
    if raw_cost not in (None, ""):
        if isinstance(raw_cost, bool):
            raise RuntimeError(f"{error_prefix} cost invalid")
        try:
            observed_cost = float(raw_cost)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(f"{error_prefix} cost invalid") from exc
        if not math.isfinite(observed_cost) or observed_cost < 0:
            raise RuntimeError(f"{error_prefix} cost invalid")
    if observed_cost == 0:
        filled = _positive_finite(order.get("filled"))
        fill_price = (
            _positive_finite(order.get("average"))
            or _positive_finite(order.get("price"))
        )
        if filled > 0 and fill_price > 0:
            observed_cost = filled * fill_price
        else:
            return
    cost_tolerance = max(1e-12, authorized_quote_cost * 1e-9)
    if (
        not math.isfinite(observed_cost)
        or observed_cost > authorized_quote_cost * 1.05 + cost_tolerance
    ):
        raise RuntimeError(f"{error_prefix} cost conflict")


def validate_spot_submit_ack(
    order,
    *,
    symbol_pair: str,
    client_order_id: Optional[str],
    expected_amount: Optional[float],
    expected_side: str = "sell",
    max_quote_cost: Optional[float] = None,
) -> str:
    """Bind a direct create-order ACK to one submitted SPOT request."""
    from bot_utils.futures_order import (
        _order_client_id_conflicts,
        _order_client_id_matches,
    )

    normalized_side = (
        expected_side.strip().lower()
        if isinstance(expected_side, str) else ""
    )
    if normalized_side not in {"buy", "sell"}:
        raise RuntimeError("spot acknowledgement expected side invalid")
    context = f"{normalized_side} acknowledgement"
    error_prefix = f"spot {context}"
    if not isinstance(order, dict):
        raise RuntimeError(f"{error_prefix} malformed")
    _validate_explicit_spot_identity_fields(
        order,
        context=context,
    )

    expected_client_id = order_id_text_or_none(client_order_id)
    if client_order_id is not None and not expected_client_id:
        raise RuntimeError(f"{error_prefix} client id invalid")
    if (
        expected_client_id
        and _order_client_id_conflicts(order, expected_client_id)
    ):
        raise RuntimeError(f"{error_prefix} client id conflict")
    client_id_matches = bool(
        expected_client_id
        and _order_client_id_matches(order, expected_client_id)
    )

    requested_amount = _validated_expected_spot_amount(expected_amount)
    tolerance = (
        max(1e-12, requested_amount * 1e-9)
        if requested_amount is not None else None
    )
    raw_amount = order.get("amount")
    if raw_amount not in (None, ""):
        observed_amount = None
        try:
            if not isinstance(raw_amount, bool):
                observed_amount = float(raw_amount)
        except (TypeError, ValueError, OverflowError):
            observed_amount = None
        if observed_amount is None or not math.isfinite(observed_amount):
            if normalized_side == "sell":
                raise RuntimeError(f"{error_prefix} amount invalid")
        elif observed_amount <= 0:
            if normalized_side == "sell":
                raise RuntimeError(f"{error_prefix} amount invalid")
        elif (
                requested_amount is not None
                and tolerance is not None
                and abs(observed_amount - requested_amount) > tolerance
        ):
            raise RuntimeError(f"{error_prefix} amount conflict")

    raw_filled = order.get("filled")
    if raw_filled not in (None, ""):
        observed_filled = None
        try:
            if not isinstance(raw_filled, bool):
                observed_filled = float(raw_filled)
        except (TypeError, ValueError, OverflowError):
            observed_filled = None
        if observed_filled is None or not math.isfinite(observed_filled):
            if normalized_side == "sell":
                raise RuntimeError(f"{error_prefix} fill invalid")
        elif observed_filled < 0:
            if normalized_side == "sell":
                raise RuntimeError(f"{error_prefix} fill invalid")
        elif (
                requested_amount is not None
                and tolerance is not None
                and observed_filled > requested_amount + tolerance
        ):
            raise RuntimeError(f"{error_prefix} fill conflict")

    _validate_spot_quote_cost(
        order,
        max_quote_cost,
        error_prefix=error_prefix,
    )

    raw_status = order.get("status")
    if raw_status not in (None, ""):
        if not isinstance(raw_status, str):
            raise RuntimeError(f"{error_prefix} status invalid")
        normalized_status = normalize_spot_order_status(raw_status)
        if normalized_status not in _SPOT_KNOWN_ORDER_STATUSES:
            raise RuntimeError(f"{error_prefix} status invalid")
        if normalized_side == "sell" or normalized_status == "rejected":
            spot_recovery_terminal_disposition(order)

    observed_symbol = order.get("symbol")
    if observed_symbol not in (None, "") and (
        not isinstance(observed_symbol, str)
        or observed_symbol.strip() != symbol_pair
    ):
        raise RuntimeError(f"{error_prefix} symbol conflict")
    observed_side = order.get("side")
    if observed_side not in (None, "") and (
        not isinstance(observed_side, str)
        or observed_side.strip().lower() != normalized_side
    ):
        raise RuntimeError(f"{error_prefix} side conflict")

    order_ids = _spot_order_ids(order, trade=False)
    if len(order_ids) > 1:
        raise RuntimeError(f"{error_prefix} order id conflict")
    venue_order_id = next(iter(order_ids)) if order_ids else None
    stable_identity = venue_order_id or (
        expected_client_id if client_id_matches else None
    )
    if not stable_identity:
        raise RuntimeError(f"{error_prefix} identity missing")
    return stable_identity


def validate_spot_recovery_candidate(
    order: dict,
    *,
    symbol_pair: str,
    expected_side: str,
    client_order_id: str,
    bound_order_id: Optional[str],
    expected_amount: Optional[float] = None,
    max_quote_cost: Optional[float] = None,
) -> Optional[str]:
    from bot_utils.futures_order import (
        _order_client_id_conflicts,
        _order_client_id_matches,
    )

    if not isinstance(order, dict):
        raise RuntimeError("spot order recovery candidate malformed")
    _validate_explicit_spot_identity_fields(
        order,
        context="order recovery",
    )
    if (
        _order_client_id_conflicts(order, client_order_id)
        or not _order_client_id_matches(order, client_order_id)
    ):
        raise RuntimeError("spot order recovery client id conflict")
    requested_amount = _validated_expected_spot_amount(expected_amount)
    if requested_amount is not None:
        tolerance = max(1e-12, requested_amount * 1e-9)
        for key in ("amount", "filled"):
            raw_value = order.get(key)
            if raw_value in (None, ""):
                continue
            if isinstance(raw_value, bool):
                raise RuntimeError("spot order recovery amount invalid")
            try:
                observed_amount = float(raw_value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise RuntimeError("spot order recovery amount invalid") from exc
            if not math.isfinite(observed_amount) or observed_amount < 0:
                raise RuntimeError("spot order recovery amount invalid")
            if observed_amount > requested_amount + tolerance:
                raise RuntimeError(
                    "spot order recovery exceeded expected amount"
                )
    _validate_spot_quote_cost(
        order,
        max_quote_cost,
        error_prefix="spot order recovery",
    )
    raw_status = order.get("status")
    if raw_status not in (None, ""):
        if not isinstance(raw_status, str):
            raise RuntimeError("spot order recovery status invalid")
        normalized_status = normalize_spot_order_status(raw_status)
        if normalized_status not in _SPOT_KNOWN_ORDER_STATUSES:
            raise RuntimeError("spot order recovery status invalid")
    observed_symbol = order.get("symbol")
    if observed_symbol not in (None, ""):
        if not isinstance(observed_symbol, str) or (
            observed_symbol.strip() != symbol_pair
        ):
            raise RuntimeError("spot order recovery symbol conflict")
    observed_side = order.get("side")
    if observed_side not in (None, ""):
        if not isinstance(observed_side, str) or (
            observed_side.strip().lower() != expected_side
        ):
            raise RuntimeError("spot order recovery side conflict")
    order_ids = _spot_order_ids(order, trade=False)
    if len(order_ids) > 1:
        raise RuntimeError("spot order recovery order id conflict")
    observed_order_id = next(iter(order_ids)) if order_ids else None
    if (
        bound_order_id
        and observed_order_id
        and observed_order_id != bound_order_id
    ):
        raise RuntimeError("spot order recovery order id conflict")
    return observed_order_id or bound_order_id


def _find_spot_exit_order_by_client_id(
    ex,
    symbol_pair: str,
    client_order_id: str,
    expected_amount: Optional[float] = None,
):
    """Recover a SPOT sell whose create response may have been lost.

    ``None`` means every supported lookup completed and found no match.
    Missing, denied or malformed lookup evidence raises so callers never
    interpret uncertainty as proof that it is safe to place a different sell.
    """
    from bot_utils.futures_order import _order_client_id_matches

    expected_amount = _validated_expected_spot_amount(expected_amount)
    has = getattr(ex, "has", {}) or {}
    if not isinstance(has, dict):
        has = {}
    attempted = False
    uncertain = False
    deferred_terminal = None
    terminal_fill_unresolved = False
    terminal_rejected = False
    bound_order_id = None
    selected_order = None

    def _query(endpoint: str, fetch):
        nonlocal attempted, uncertain
        try:
            reservation = try_consume_api_call(
                endpoint,
                critical=True,
                return_reservation=True,
            )
        except Exception as exc:
            raise RuntimeError(
                f"spot sell reconciliation unavailable: {endpoint}"
            ) from exc
        if not reservation:
            raise RuntimeError(
                f"spot sell reconciliation unavailable: {endpoint}"
            )
        attempted = True
        try:
            rows = fetch()
            if not isinstance(rows, list):
                raise TypeError(
                    "spot sell reconciliation returned no order list"
                )
            if any(not isinstance(row, dict) for row in rows):
                raise TypeError(
                    "spot sell reconciliation returned a malformed row"
                )
        except Exception:
            if isinstance(reservation, ApiCallReservation):
                try:
                    record_api_error(endpoint, reservation)
                except Exception:
                    pass
            uncertain = True
            return []
        return rows

    def _select_batch(rows):
        nonlocal bound_order_id, deferred_terminal
        nonlocal terminal_fill_unresolved, terminal_rejected
        selected = None
        for order in rows:
            if not _order_client_id_matches(order, client_order_id):
                continue
            bound_order_id = validate_spot_recovery_candidate(
                order,
                symbol_pair=symbol_pair,
                expected_side="sell",
                client_order_id=client_order_id,
                bound_order_id=bound_order_id,
                expected_amount=expected_amount,
            )
            disposition = spot_recovery_terminal_disposition(order)
            status = order.get("status")
            if isinstance(status, str) and status.strip().lower() == "rejected":
                terminal_rejected = True
            if disposition in {"zero", "unresolved"}:
                deferred_terminal = deferred_terminal or order
                terminal_fill_unresolved |= disposition == "unresolved"
            elif selected is None:
                selected = order
            elif not strict_order_snapshot_equal(selected, order):
                raise RuntimeError(
                    "spot order recovery duplicate snapshot conflict"
                )
        return selected

    fetch_open = getattr(ex, "fetch_open_orders", None)
    if callable(fetch_open) and has.get("fetchOpenOrders") is not False:
        selected = _select_batch(_query(
            "spot_exit_reconcile_fetch_open_orders",
            lambda: fetch_open(symbol_pair),
        ))
        if selected is not None:
            selected_order = selected

    fetch_orders = getattr(ex, "fetch_orders", None)
    if has.get("fetchOrders") and callable(fetch_orders):
        selected = _select_batch(_query(
            "spot_exit_reconcile_fetch_orders",
            lambda: fetch_orders(symbol_pair, limit=20),
        ))
        if selected is not None:
            selected_order = selected

    fetch_closed = getattr(ex, "fetch_closed_orders", None)
    if has.get("fetchClosedOrders") and callable(fetch_closed):
        selected = _select_batch(_query(
            "spot_exit_reconcile_fetch_closed_orders",
            lambda: fetch_closed(symbol_pair, limit=20),
        ))
        if selected is not None:
            selected_order = selected

    if selected_order is not None:
        if terminal_rejected:
            raise RuntimeError("spot rejected order has fill evidence")
        return selected_order

    fetch_trades = getattr(ex, "fetch_my_trades", None)
    if has.get("fetchMyTrades") and callable(fetch_trades):
        trade_limit = 20
        trade_rows = _query(
            "spot_exit_reconcile_fetch_my_trades",
            lambda: fetch_trades(symbol_pair, limit=trade_limit),
        )
        matching_trades = [
            trade for trade in trade_rows
            if _order_client_id_matches(trade, client_order_id)
        ]
        if matching_trades:
            recovered = aggregate_spot_order_trades(
                matching_trades,
                client_order_id,
                symbol_pair=symbol_pair,
                expected_side="sell",
                expected_amount=expected_amount,
            )
            validate_spot_recovery_candidate(
                recovered,
                symbol_pair=symbol_pair,
                expected_side="sell",
                client_order_id=client_order_id,
                bound_order_id=bound_order_id,
                expected_amount=expected_amount,
            )
            recovered_fill = _positive_finite(recovered.get("filled"))
            if terminal_rejected and recovered_fill > 0:
                raise RuntimeError("spot rejected order has fill evidence")
            incomplete_fill = (
                expected_amount is None
                or recovered_fill + max(1e-12, expected_amount * 1e-9)
                < expected_amount
            )
            if (
                incomplete_fill
                and (
                    deferred_terminal is None
                    or len(trade_rows) >= trade_limit
                )
            ):
                raise RuntimeError(
                    "spot trade-only recovery lacks terminal/full-fill proof"
                )
            return recovered

    if not attempted or uncertain:
        raise RuntimeError("spot sell reconciliation unavailable")
    if terminal_fill_unresolved:
        raise RuntimeError("spot terminal fill amount unavailable")
    return deferred_terminal


def recover_spot_sell_by_client_id(ex, symbol_pair: str,
                                   client_order_id: str,
                                   expected_amount: Optional[float] = None):
    """Public recovery boundary for persisted SPOT sell intents."""
    return _find_spot_exit_order_by_client_id(
        ex, symbol_pair, client_order_id, expected_amount=expected_amount
    )


def _free_base_balance(ex, symbol_pair: str):
    """Free balance of the BASE asset of ``symbol_pair`` (e.g. FET for
    FET/USDT), or None if it can't be read."""
    if not isinstance(symbol_pair, str) or symbol_pair.count("/") != 1:
        return None
    base, quote = (part.strip() for part in symbol_pair.split("/", 1))
    if not base or not quote:
        return None
    try:
        reservation = try_consume_api_call(
            "spot_exit_fetch_balance",
            critical=True,
            return_reservation=True,
        )
    except Exception:
        return None
    if not reservation:
        return None

    def _record_response_error() -> None:
        if isinstance(reservation, ApiCallReservation):
            try:
                record_api_error("spot_exit_fetch_balance", reservation)
            except Exception:
                pass

    try:
        bal = ex.fetch_balance()
        if not isinstance(bal, dict):
            raise TypeError("spot exit balance returned no balance object")
        aggregate = bal.get("free")
        if aggregate is not None and not isinstance(aggregate, dict):
            raise TypeError("spot exit balance returned malformed free totals")
        currency = bal.get(base)
        if currency is not None and not isinstance(currency, dict):
            raise TypeError("spot exit balance returned malformed currency row")

        values = []
        for raw in (
            aggregate.get(base) if isinstance(aggregate, dict) else None,
            currency.get("free") if isinstance(currency, dict) else None,
        ):
            if raw is None:
                continue
            if isinstance(raw, bool):
                raise ValueError("spot exit balance returned boolean free amount")
            parsed = float(raw)
            if not math.isfinite(parsed) or parsed < 0:
                raise ValueError("spot exit balance returned invalid free amount")
            values.append(parsed)
        if not values:
            raise ValueError("spot exit balance returned no free base amount")
        if any(
            not math.isclose(
                value,
                values[0],
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            for value in values[1:]
        ):
            raise ValueError("spot exit balance returned conflicting free amounts")
        return values[0]
    except Exception:
        _record_response_error()
        return None


def _filled_base_amount(order, wrapper_sold, requested_amount: float) -> float:
    requested = _positive_finite(requested_amount)
    if isinstance(order, dict):
        filled = _positive_finite(order.get("filled"))
        if filled > 0:
            return min(requested, filled)
    sold = _positive_finite(wrapper_sold)
    if sold > 0:
        return min(requested, sold)
    return 0.0


def _apply_state_updates(state, sym: str, updates: dict) -> None:
    if hasattr(state, "update_many"):
        state.update_many(sym, updates)
    else:
        for key, value in updates.items():
            state.update(sym, key, value)


def _persist_spot_exit_fields(state, sym: str, row: dict,
                              updates: dict) -> bool:
    """Durably persist exit-control fields and mirror them into ``row``."""
    try:
        if callable(getattr(state, "update_many", None)):
            from bot_utils.trade_state import (
                same_position_generation,
                update_many_if_current,
            )

            persisted = update_many_if_current(state, sym, updates, row)
            getter = getattr(state, "get", None)
            if persisted and callable(getter):
                current = getter(sym)
                persisted = bool(
                    isinstance(current, dict)
                    and (
                        same_position_generation(current, row)
                        or current == {**row, **updates}
                    )
                    and all(
                        current.get(key) == value
                        for key, value in updates.items()
                    )
                )
        else:
            persisted = True
            for key, value in updates.items():
                result = state.update(sym, key, value)
                if result is not None and result is not True:
                    persisted = False
                    break
    except Exception:
        return False
    if persisted is not None and persisted is not True:
        return False
    row.update(updates)
    return True


def _remove_accounted_state(
    state,
    sym: str,
    restore_fields: dict,
    *,
    expected_row: dict | None = None,
) -> bool:
    try:
        from bot_utils.trade_state import (
            remove_with_restore_fields,
            update_many_if_current,
        )
        ok = bool(remove_with_restore_fields(
            state,
            sym,
            restore_fields,
            expected_row=expected_row,
        ))
    except Exception:
        # A generation-guarded production cleanup must never retry through an
        # unguarded compatibility path: that could delete a replacement row.
        if expected_row is not None:
            return False
        if not hasattr(state, "remove"):
            raise
        try:
            result = state.remove(sym, restore_fields)
        except TypeError:
            result = state.remove(sym)
        ok = True if result is None else bool(result)
    if not ok:
        try:
            if expected_row is not None:
                update_many_if_current(
                    state,
                    sym,
                    restore_fields,
                    expected_row,
                )
            else:
                _apply_state_updates(state, sym, restore_fields)
        except Exception:
            pass
    return ok


def _order_has_open_remainder(order) -> bool:
    if not isinstance(order, dict):
        return False
    status = normalize_spot_order_status(order.get("status"))
    if status in ("closed", "canceled", "cancelled", "expired", "rejected"):
        return False
    if status in ("open", "new", "partially_filled", "partiallyfilled"):
        return True
    if status:
        return False
    return _positive_finite(order.get("remaining")) > 0


def _emergency_residual_amount(ex, symbol_pair: str, requested_amount: float,
                               sold_amount: float, fill_price: float,
                               order=None) -> float:
    requested = _positive_finite(requested_amount)
    sold = _positive_finite(sold_amount)
    by_fill = max(0.0, requested - sold)
    if by_fill <= 0 or fill_price <= 0:
        return 0.0
    if _order_has_open_remainder(order):
        if by_fill * fill_price <= _EMERGENCY_RESIDUAL_DUST_USDT:
            return 0.0
        return min(by_fill, requested)
    try:
        free = _free_base_balance(ex, symbol_pair)
    except Exception:
        free = None
    residual = by_fill
    if free is not None and free >= 0:
        residual = min(residual, _positive_finite(free))
    if residual * fill_price <= _EMERGENCY_RESIDUAL_DUST_USDT:
        return 0.0
    return min(residual, requested)


def _emergency_residual_updates(amount: float, residual_amount: float,
                                margin: float, initial_entry_fee: float,
                                original_amount: float, exch_oid,
                                reason: str) -> dict:
    remaining_invested = (
        margin * (residual_amount / amount)
        if amount > 0 else 0.0
    )
    from bot_utils import safe_proportional_fee
    remaining_entry_fee = safe_proportional_fee(
        initial_entry_fee,
        residual_amount,
        original_amount,
        partial_sold=True,
    )
    return {
        "amount": residual_amount,
        "original_amount": residual_amount,
        "invested_usdt": remaining_invested,
        "initial_entry_fee": remaining_entry_fee,
        "fees_paid": remaining_entry_fee,
        "partial_sold": False,
        "closing_retry_pending": True,
        "closing_retry_reason": reason,
        "last_partial_fill_order_id": exch_oid,
        "emergency_exit_client_order_id": None,
        "emergency_exit_outcome_uncertain": False,
    }


def spot_market_sell_safe(ex, symbol_pair: str, raw_amount: float,
                          *, client_order_id: Optional[str] = None
                          ) -> Tuple[dict, float]:
    """Wraps create_market_sell_order with precision rounding.

    The step is derived from ``markets[symbol].limits.amount.min`` or
    ``markets[symbol].precision.amount`` (so coins with batch sizes >= 1 like
    many meme-coin listings work), falling back to 0.0001 only as a last resort.
    When a stable ``client_order_id`` is supplied, an ambiguous create failure
    is reconciled before the caller may retry the sell.
    """
    rounded: float
    safe_raw_amount = _positive_finite(raw_amount)
    if safe_raw_amount <= 0:
        raise ValueError(f"amount {raw_amount} is not positive finite")
    try:
        native_rounded = ex.amount_to_precision(
            symbol_pair, safe_raw_amount
        )
        if isinstance(native_rounded, bool):
            raise ValueError("native sell precision returned boolean")
        native_dec = Decimal(str(native_rounded))
        raw_dec = Decimal(str(safe_raw_amount))
        if not native_dec.is_finite() or native_dec > raw_dec:
            raise ValueError("native sell precision amplified the amount")
        rounded = _positive_finite(native_rounded)
    except Exception:
        # Prefer market-metadata-derived step
        step = (_exchange_precision_step(ex, symbol_pair)
                 or _exchange_min_amount(ex, symbol_pair)
                 or Decimal("0.0001"))
        amt_dec = Decimal(str(safe_raw_amount))
        try:
            rounded = _positive_finite(_round_to_step(amt_dec, step))
        except Exception:
            rounded = _positive_finite(
                amt_dec.quantize(Decimal("0.0001"), rounding=ROUND_DOWN))

    if rounded <= 0:
        raise ValueError(f"amount {raw_amount} rounded to {rounded}  invalid")

    # Progressive precision retry spanning both sub-unit and large batch
    # sizes. Start at the exchange's reported min/precision (most accurate),
    # then progressively coarsen if that fails. For coins with min_amount > 1
    # we also try multiples of the min step.
    min_step = _exchange_min_amount(ex, symbol_pair)
    prec_step = _exchange_precision_step(ex, symbol_pair)

    # Build retry sequence of step sizes from finest to coarsest.
    steps_to_try: list = [None]  # First attempt: use 'rounded' as-is
    if prec_step:
        steps_to_try.append(prec_step)
    if min_step and min_step != prec_step:
        steps_to_try.append(min_step)
    # Coarsening fallbacks  span both directions (sub-unit and >=1)
    for s in ("0.001", "0.01", "0.1", "1", "10", "100", "1000", "10000"):
        sd = Decimal(s)
        if not any(sd == existing for existing in steps_to_try if isinstance(existing, Decimal)):
            steps_to_try.append(sd)

    last_exc: Optional[Exception] = None
    raw_dec = Decimal(str(safe_raw_amount))
    _balance_capped = False   # re-read free balance at most once on Oversold

    def _recover_submit_error(submit_error: Exception,
                              submitted_amount: float):
        try:
            recovered = _find_spot_exit_order_by_client_id(
                ex,
                symbol_pair,
                client_order_id,
                expected_amount=submitted_amount,
            )
        except Exception as recovery_error:
            raise SpotSellOutcomeUnknown(
                client_order_id
            ) from recovery_error
        if recovered is not None:
            return recovered
        raise SpotSellOutcomeUnknown(client_order_id) from submit_error

    for step in steps_to_try:
        if step is None:
            amt = rounded
        else:
            amt = _positive_finite(_round_to_step(raw_dec, step))
        if amt <= 0:
            continue
        try:
            order = _create_market_sell_budgeted(
                ex, symbol_pair, amt, client_order_id
            )
            validate_spot_submit_ack(
                order,
                symbol_pair=symbol_pair,
                client_order_id=client_order_id,
                expected_amount=amt,
            )
            return order, amt
        except Exception as e:
            es = str(e).lower()
            # 1. Precision / lot-size error  coarsen the step and retry.
            if any(m in es for m in ("precision", "lot", "step",
                                       "below", "minimum", "min ")):
                last_exc = e
                continue
            # 2. Oversold / insufficient balance (MEXC 30005 et al.): we asked to
            #    sell more BASE than is actually free. Cause is almost always
            #    fee-in-base on the buy (you receive ~0.1% fewer coins than the
            #    order amount) or a prior partial that already reduced it. Re-read
            #    the REAL free balance ONCE and retry with that (rounded down). If
            #    nothing sellable remains, raise a typed error so the caller
            #    removes the orphan from state instead of looping 30005 forever.
            if (not _balance_capped
                    and any(m in es for m in ("oversold", "30005",
                                               "insufficient", "not enough"))):
                _balance_capped = True
                free = _free_base_balance(ex, symbol_pair)
                if free is None:
                    raise e
                if free <= 0:
                    raise InsufficientSellBalance(
                        symbol_pair, requested=amt, free=free
                    )

                capped = free
                try:
                    precise = ex.amount_to_precision(symbol_pair, free)
                    if not isinstance(precise, bool):
                        parsed = float(precise)
                        if (
                            math.isfinite(parsed)
                            and parsed >= 0
                            and Decimal(str(parsed)) <= Decimal(str(free))
                        ):
                            capped = parsed
                except Exception:
                    pass
                min_amt = _exchange_min_amount(ex, symbol_pair)
                sellable = (
                    capped > 0
                    and (min_amt is None or Decimal(str(capped)) >= min_amt)
                )
                if not sellable:
                    raise InsufficientSellBalance(
                        symbol_pair, requested=amt, free=free
                    )
                if capped >= amt:
                    raise e
                try:
                    order = _create_market_sell_budgeted(
                        ex, symbol_pair, capped, client_order_id
                    )
                    validate_spot_submit_ack(
                        order,
                        symbol_pair=symbol_pair,
                        client_order_id=client_order_id,
                        expected_amount=capped,
                    )
                except Exception as capped_error:
                    if (
                        client_order_id
                        and not isinstance(
                            capped_error, _SpotSellBudgetUnavailable
                        )
                    ):
                        order = _recover_submit_error(
                            capped_error, capped
                        )
                    else:
                        raise
                return order, capped
            # 3. An ambiguous create failure may have happened after the venue
            #    accepted the sell. Recover by the stable client id; otherwise
            #    surface a typed unknown outcome so callers persist a retry
            #    barrier instead of issuing a fresh sell next monitor tick.
            if client_order_id and not isinstance(e, _SpotSellBudgetUnavailable):
                return _recover_submit_error(e, amt), amt
            raise
    raise last_exc or ValueError(f"all precision retries failed for {raw_amount}")


#  Emergency close-all (spot) 

def emergency_close_all_spot(*,
                              ex,
                              state,                       # TradeState
                              bot_name: str,
                              log_dir: str,
                              simulation: bool,
                              reason: str = "Shutdown",
                              telegram_token: Optional[str] = None,
                              telegram_chat_id: Optional[str] = None,
                              log_event: Callable,
                              log_sell: Callable,
                              save_trade_db: Callable,
                              save_trade: Callable,
                              send_telegram: Callable,
                              error_logger: Optional[Callable] = None,
                              close_lock_factory: Optional[Callable] = None,
                              release_lock: Optional[Callable] = None,
                              ) -> dict:
    """Close ALL open spot positions cleanly on shutdown.

    Returns {"closed_count", "failed_count", "failed"} so shutdown handlers
    latch only after a complete flatten and can retry failed legs.
    """
    snapshot = state.get_all()
    if not snapshot:
        log_event("Emergency close: no open positions", "INFO")
        return {"closed_count": 0, "failed_count": 0, "failed": []}

    log_event(
        f" EMERGENCY CLOSE ALL ({reason})  {len(snapshot)} positions",
        "WARN"
    )

    closed_count = 0
    failed: list = []
    total_pnl = 0.0

    for sym, d in snapshot.items():
        from contextlib import ExitStack
        with ExitStack() as stack:
            if close_lock_factory:
                try:
                    lock_ctx = close_lock_factory(
                        sym, timeout=2.0, bot_name=bot_name, fail_open=True)
                except TypeError:
                    lock_ctx = close_lock_factory(
                        sym, timeout=2.0, bot_name=bot_name)
                got = stack.enter_context(lock_ctx)
                if not got:
                    failed.append(f"{sym}: close lock not acquired")
                    log_event(
                        f"Emergency: could not acquire lock for {sym} in 2s "
                        f"(main loop may already be closing it)", "WARN"
                    )
                    continue

            try:
                # The snapshot predates the close lock. Re-read under the lock
                # so a concurrent partial exit cannot be overwritten by an
                # emergency sell based on stale state.
                d_live = state.get(sym)
                if not isinstance(d_live, dict):
                    continue
                d = d_live

                if d.get("accounting_already_booked") is True:
                    restore_fields = {
                        "accounting_already_booked": True,
                        "accounting_booked_sell_time": d.get(
                            "accounting_booked_sell_time"
                        ),
                        "accounting_booked_exchange_order_id": d.get(
                            "accounting_booked_exchange_order_id"
                        ),
                        "accounting_booked_reason": d.get(
                            "accounting_booked_reason"
                        ),
                    }
                    if _remove_accounted_state(
                        state, sym, restore_fields, expected_row=d
                    ):
                        closed_count += 1
                    else:
                        failed.append(
                            f"{sym}: cleanup failed after booked close"
                        )
                        log_event(
                            f"Emergency: {sym} was already booked, but "
                            "state/claim cleanup still failed",
                            "WARN",
                        )
                    continue

                if d.get("accounting_pending") is True:
                    failed.append(f"{sym}: full exit accounting pending")
                    log_event(
                        f"Emergency: {sym} has full-exit accounting pending; "
                        "physical sell remains blocked until reconciliation",
                        "WARN",
                    )
                    continue

                if has_pending_spot_partial_exit(d):
                    failed.append(f"{sym}: partial exit reconciliation pending")
                    log_event(
                        f"Emergency: {sym} has a pending partial-exit intent; "
                        "keeping state for reconciliation",
                        "WARN",
                    )
                    continue

                from bot_utils.trade_state import (
                    normalize_pending_accounting_items,
                )
                if normalize_pending_accounting_items(
                    d.get("accounting_pending_partials")
                ):
                    failed.append(f"{sym}: partial exit accounting pending")
                    log_event(
                        f"Emergency: {sym} has pending partial accounting; "
                        "keeping state for retry",
                        "WARN",
                    )
                    continue

                buy_price = _positive_finite(d.get("buy"))
                amount = _positive_finite(d.get("amount"))
                margin = _positive_finite(d.get("invested_usdt"))
                symbol_pair = f"{sym}/USDT"
                if amount <= 0:
                    failed.append(f"{sym}: invalid state amount")
                    log_event(
                        f"Emergency: invalid amount for {sym} "
                        f"({d.get('amount')!r})  keeping state",
                        "WARN",
                    )
                    continue
                if buy_price <= 0 or margin <= 0:
                    failed.append(f"{sym}: invalid state cost basis")
                    log_event(
                        f"Emergency: invalid cost basis for {sym} "
                        f"(buy={d.get('buy')!r}, invested={d.get('invested_usdt')!r}) "
                        f" keeping state",
                        "WARN",
                    )
                    continue

                curr = 0.0
                try:
                    price_reservation = try_consume_api_call(
                        "spot_emergency_exit_fetch_ticker",
                        critical=True,
                        return_reservation=True,
                    )
                except Exception as gate_error:
                    log_event(
                        f"  Price for {sym} unavailable: API budget gate "
                        f"failed ({gate_error})", "WARN"
                    )
                    price_reservation = None
                if price_reservation:
                    try:
                        ticker = ex.fetch_ticker(symbol_pair)
                        if not explicit_trade_symbol_matches(
                            ticker, symbol_pair
                        ):
                            raise ValueError(
                                "emergency spot ticker changed requested symbol"
                            )
                        curr = _positive_finite(ticker.get("last"))
                        if curr <= 0:
                            curr = _positive_finite(ticker.get("close"))
                        if curr <= 0:
                            raise ValueError(
                                "emergency spot ticker returned no positive price"
                            )
                    except Exception as e:
                        if isinstance(price_reservation, ApiCallReservation):
                            try:
                                record_api_error(
                                    "spot_emergency_exit_fetch_ticker",
                                    price_reservation,
                                )
                            except Exception:
                                pass
                        log_event(f"  Price for {sym} unavailable: {e}", "WARN")
                if curr <= 0:
                    curr = buy_price  # fallback

                profit_pct = ((curr - buy_price) / buy_price * 100) if buy_price > 0 else 0.0

                partial_sold = bool(d.get("partial_sold"))
                initial_entry_fee = _finite_float(
                    d.get(
                        "initial_entry_fee",
                        0.0 if partial_sold else d.get("fees_paid", 0.0),
                    )
                )
                original_amount = _positive_finite(d.get("original_amount"))
                if original_amount <= 0 and not partial_sold:
                    original_amount = amount
                from bot_utils import safe_proportional_fee
                proportional_entry_fee = safe_proportional_fee(
                    initial_entry_fee, amount, original_amount,
                    partial_sold=partial_sold
                )
                sold_amount = amount
                booked_invested = sold_amount * buy_price if buy_price > 0 else margin
                profit_usdt = round(
                    sold_amount * (curr - buy_price) - proportional_entry_fee,
                    2
                )

                close_fee = 0.0
                fill_price = curr
                exch_oid = None
                order = None
                client_order_id = None
                if simulation and amount > 0 and fill_price > 0:
                    close_fee = sold_amount * fill_price * 0.001
                    profit_usdt = round(
                        sold_amount * (fill_price - buy_price)
                        - proportional_entry_fee - close_fee,
                        2
                    )

                if not simulation and amount > 0:
                    try:
                        persisted_client_order_id = order_id_text_or_none(
                            d.get("emergency_exit_client_order_id")
                        )
                        client_order_id = ensure_spot_exit_client_order_id(
                            state, sym, d, "emergency", bot_name
                        )
                        order = None
                        if (
                            persisted_client_order_id == client_order_id
                            or d.get("emergency_exit_outcome_uncertain")
                        ):
                            try:
                                order = _find_spot_exit_order_by_client_id(
                                    ex,
                                    symbol_pair,
                                    client_order_id,
                                    expected_amount=amount,
                                )
                            except Exception as recovery_error:
                                failed.append(
                                    f"{sym}: emergency sell outcome still unknown"
                                )
                                log_event(
                                    f"  [LIVE] {sym}: emergency sell outcome "
                                    f"still unknown (clientOrderId="
                                    f"{client_order_id}): {recovery_error}",
                                    "WARN",
                                )
                                continue
                        from bot_utils.network_retry import with_network_retry
                        # Idempotency guard: each attempt (including retries
                        # after a lost response that may have already executed)
                        # re-reads the live free base balance and never sells
                        # more than is actually held  so a second market sell
                        # can't fire for an already-executed sell.
                        def _sell_capped(_sym=sym, _pair=symbol_pair,
                                         _want=amount):
                            free = _free_base_balance(ex, _pair)
                            sell_amt = _want
                            if free is not None and free > 0:
                                sell_amt = min(_want, free)
                            elif free is not None and free <= 0:
                                raise InsufficientSellBalance(
                                    _pair, requested=_want, free=0.0)
                            return spot_market_sell_safe(
                                ex, _pair, sell_amt,
                                client_order_id=client_order_id,
                            )
                        if order is None:
                            from bot_utils.trade_state import registry_order_guard
                            with registry_order_guard(
                                state, sym, d
                            ) as ownership_live:
                                if not isinstance(ownership_live, dict):
                                    continue
                                if ownership_live.get("claim_conflict"):
                                    failed.append(
                                        f"{sym}: registry claim conflict"
                                    )
                                    log_event(
                                        f"  [LIVE] {sym}: emergency sell blocked "
                                        f"by registry claim conflict",
                                        "ERROR",
                                    )
                                    continue
                                order, _sold = with_network_retry(
                                    operation=lambda: _sell_capped(),
                                    action_label=f"emergency sell {sym}",
                                    max_attempts=3,
                                    base_delay=0.5,
                                    shutdown_event=None,
                                    log_event=log_event,
                                )
                        else:
                            _sold = amount
                        raw_status = (
                            order.get("status")
                            if isinstance(order, dict) else None
                        )
                        normalized_status = normalize_spot_order_status(
                            raw_status
                        )
                        if spot_sell_requires_terminal_recovery(
                            order, amount
                        ):
                            barrier_updates = {
                                "emergency_exit_outcome_uncertain": True,
                            }
                            explicit_unknown_status = (
                                raw_status not in (None, "")
                                and (
                                    not isinstance(raw_status, str)
                                    or normalized_status
                                    not in _SPOT_KNOWN_ORDER_STATUSES
                                )
                            )
                            observed_residual = (
                                _emergency_residual_amount(
                                    ex,
                                    symbol_pair,
                                    amount,
                                    _sold,
                                    curr,
                                    order=order,
                                )
                                if explicit_unknown_status
                                else amount
                            )
                            if observed_residual < amount:
                                barrier_updates.update(
                                    _emergency_residual_updates(
                                        amount,
                                        observed_residual,
                                        margin,
                                        initial_entry_fee,
                                        original_amount,
                                        None,
                                        reason,
                                    )
                                )
                                barrier_updates.pop(
                                    "emergency_exit_client_order_id", None
                                )
                                barrier_updates[
                                    "emergency_exit_outcome_uncertain"
                                ] = True
                            persisted = _persist_spot_exit_fields(
                                state, sym, d,
                                barrier_updates,
                            )
                            if not persisted:
                                log_event(
                                    f"  [LIVE] {sym}: could not persist "
                                    f"emergency outcome barrier",
                                    "ERROR",
                                )
                            failed.append(
                                f"{sym}: emergency sell still "
                                f"{normalized_status}"
                            )
                            log_event(
                                f"  [LIVE] {sym}: emergency sell still "
                                f"{normalized_status or 'unresolved'}; "
                                "booking and retry "
                                f"deferred until terminal state",
                                "WARN",
                            )
                            continue
                        from bot_utils.order_utils import (
                            order_has_proven_zero_fill,
                            order_has_unquantified_fill_notional,
                            order_was_filled,
                        )
                        # Verify the order ACTUALLY filled  MEXC can return an
                        # order object that never executed (status new/open,
                        # filled=0, e.g. remaining size below min-notional);
                        # booking it as sold would write a phantom closed trade.
                        unquantified_fill = (
                            order_has_unquantified_fill_notional(order)
                        )
                        if (
                            unquantified_fill
                            or not order_was_filled(
                                order,
                                _sold,
                                min_fill_ratio=1e-9,
                            )
                        ):
                            if (
                                normalized_status in {
                                "canceled", "cancelled", "rejected", "expired",
                                }
                                and order_has_proven_zero_fill(order)
                            ):
                                persisted = _persist_spot_exit_fields(
                                    state, sym, d, {
                                        "emergency_exit_client_order_id": None,
                                        "emergency_exit_outcome_uncertain": False,
                                    }
                                )
                            else:
                                persisted = _persist_spot_exit_fields(
                                    state, sym, d,
                                    {"emergency_exit_outcome_uncertain": True},
                                )
                            if not persisted:
                                log_event(
                                    f"  [LIVE] {sym}: could not persist "
                                    f"emergency order state",
                                    "ERROR",
                                )
                            failed.append(f"{sym}: order not filled "
                                          f"(status={order.get('status') if isinstance(order, dict) else '?'})")
                            log_event(
                                f"  [LIVE] {sym}: sell order did NOT fill "
                                f"(still in wallet)  sell MANUALLY!", "WARN")
                            continue
                        exch_oid = (
                            order_id_text_or_none(order.get("id"))
                            or order_id_text_or_none(order.get("orderId"))
                            or order_id_text_or_none(client_order_id)
                        )
                        sold_amount = _filled_base_amount(order, _sold, amount)
                        fill_price = _positive_finite(
                            extract_fill_price(order, curr), curr)
                        proportional_entry_fee = safe_proportional_fee(
                            initial_entry_fee, sold_amount, original_amount,
                            partial_sold=partial_sold
                        )
                        booked_invested = (sold_amount * buy_price
                                           if buy_price > 0 else margin)
                        try:
                            from trading.fee_utils import extract_or_estimate_with_refetch
                            close_fee = extract_or_estimate_with_refetch(
                                ex, order, symbol_pair, fill_price,
                                base_override=sym
                            )
                        except Exception:
                            close_fee = extract_order_fee(order)
                        real_pct = ((fill_price - buy_price) / buy_price * 100) if buy_price > 0 else 0.0
                        profit_pct = real_pct
                        profit_usdt = round(
                            sold_amount * (fill_price - buy_price)
                            - proportional_entry_fee - close_fee,
                            2
                        )
                        log_event(
                            f"  [LIVE] Sold {sym} @ {fill_price:.6f}  "
                            f"{profit_usdt:+.2f} USDT", "INFO"
                        )
                    except SpotSellOutcomeUnknown as unknown:
                        persisted = _persist_spot_exit_fields(
                            state, sym, d,
                            {"emergency_exit_outcome_uncertain": True},
                        )
                        if not persisted:
                            log_event(
                                f"  [LIVE] {sym}: could not persist emergency "
                                f"outcome barrier",
                                "ERROR",
                            )
                        failed.append(f"{sym}: {unknown}")
                        log_event(
                            f"  [LIVE] Sell {sym} outcome unknown; retry "
                            f"blocked pending clientOrderId reconciliation",
                            "WARN",
                        )
                        continue
                    except Exception as e:
                        failed.append(f"{sym}: {e}")
                        log_event(f"  [LIVE] Sell {sym} FAILED: {e}", "WARN")
                        continue

                buy_time = d.get("buy_time", _utc_now_str())
                sell_time = _utc_now_str()
                fees_for_booked_slice = proportional_entry_fee + close_fee
                residual_amount = (
                    _emergency_residual_amount(
                        ex, symbol_pair, amount, sold_amount,
                        fill_price, order=order
                    )
                    if not simulation and amount > 0 else 0.0
                )
                is_partial_close = residual_amount > 0
                from core.spot_bot_exits import _spot_excursion_metrics
                mfe_pct, mae_pct, giveback_pct = _spot_excursion_metrics(
                    d, fill_price
                )
                booked_reason = f"Emergency Close ({reason})"
                trade_row = dict(
                    bot_name=bot_name,
                    mode_is_sim=simulation,
                    symbol=sym,
                    buy_price=buy_price,
                    sell_price=fill_price,
                    buy_time=buy_time,
                    sell_time=sell_time,
                    profit_pct=profit_pct,
                    profit_usdt=profit_usdt,
                    invested_usdt=booked_invested,
                    reason=booked_reason,
                    rsi_15m=d.get("rsi_15m"),
                    rsi_1h=d.get("rsi_1h"),
                    rsi_4h=d.get("rsi_4h"),
                    change_pct=d.get("change_pct"),
                    btc_trend=d.get("btc_trend"),
                    fear_greed=d.get("fear_greed"),
                    is_futures=False,
                    fees_usdt=fees_for_booked_slice,
                    exchange_order_id=exch_oid,
                    entry_quality_score=d.get("entry_quality_score"),
                    entry_quality_label=d.get("entry_quality_label"),
                    entry_quality_reasons=d.get("entry_quality_reasons"),
                    entry_id=d.get("entry_id"),
                    mfe_pct=mfe_pct,
                    mae_pct=mae_pct,
                    giveback_pct=giveback_pct,
                )
                pending_partials = []
                if is_partial_close:
                    trade_row["is_partial"] = True
                    from bot_utils.trade_state import (
                        normalize_pending_accounting_items,
                    )
                    pending_partials = normalize_pending_accounting_items(
                        d.get("accounting_pending_partials")
                    )
                    pending_partials.append(dict(trade_row))
                    write_ahead = _emergency_residual_updates(
                        amount, residual_amount, margin,
                        initial_entry_fee, original_amount,
                        exch_oid, reason
                    )
                    write_ahead["accounting_pending_partials"] = (
                        pending_partials
                    )
                else:
                    write_ahead = {
                        "accounting_pending": True,
                        "accounting_pending_reason": booked_reason,
                        "accounting_pending_sell_price": fill_price,
                        "accounting_pending_sell_time": sell_time,
                        "accounting_pending_profit_pct": profit_pct,
                        "accounting_pending_profit_usdt": profit_usdt,
                        "accounting_pending_invested_usdt": (
                            booked_invested
                        ),
                        "accounting_pending_mode_is_sim": simulation,
                        "accounting_pending_fees_usdt": (
                            fees_for_booked_slice
                        ),
                        "accounting_pending_exchange_order_id": exch_oid,
                        "accounting_pending_mfe_pct": mfe_pct,
                        "accounting_pending_mae_pct": mae_pct,
                        "accounting_pending_giveback_pct": giveback_pct,
                    }

                state_persisted = _persist_spot_exit_fields(
                    state, sym, d, write_ahead
                )
                if not state_persisted:
                    if not simulation:
                        _persist_spot_exit_fields(
                            state,
                            sym,
                            d,
                            {"emergency_exit_outcome_uncertain": True},
                        )
                    failed.append(
                        f"{sym}: accounting state not durable after close"
                    )
                    log_event(
                        f"  {sym}: physical emergency fill was not booked "
                        "because its accounting recovery state was not durable",
                        "ERROR",
                    )
                    continue

                try:
                    accounting_ok = save_trade_db(**trade_row) is True
                    if not accounting_ok:
                        raise RuntimeError("save_trade_db returned False")
                except Exception as e:
                    log_event(
                        f"  DB accounting for {sym} failed after close: {e}. "
                        f"Durable retry state retained.", "WARN")
                    failed.append(f"{sym}: accounting failed after close")
                    continue

                if is_partial_close:
                    remaining_pending = pending_partials[:-1]
                    cleared = _persist_spot_exit_fields(
                        state,
                        sym,
                        d,
                        {
                            "accounting_pending_partials": (
                                remaining_pending
                            )
                        },
                    )
                    if not cleared:
                        log_event(
                            f"  [LIVE] {sym}: partial emergency fill was "
                            "booked, but its durable pending marker could not "
                            "be cleared; idempotent retry retained",
                            "ERROR",
                        )

                # The canonical DB booking is authoritative. Optional file and
                # console logs must never turn an already-booked close into a
                # false accounting_pending retry.
                try:
                    save_trade(
                        log_dir=log_dir, symbol=sym,
                        buy_price=buy_price, buy_time=buy_time,
                        sell_price=fill_price, profit_pct=profit_pct,
                        profit_usdt=profit_usdt,
                        reason=f"Emergency Close ({reason})"
                    )
                except Exception as e:
                    log_event(
                        f"  File trade log for {sym} could not be saved "
                        f"({type(e).__name__})",
                        "WARN",
                    )
                    if error_logger:
                        try:
                            error_logger(f"emergency file log {sym}", e)
                        except Exception:
                            pass
                try:
                    log_sell(
                        bot_name,
                        sym,
                        profit_pct,
                        profit_usdt,
                        "Emergency Close",
                    )
                except Exception as e:
                    log_event(
                        f"  Sell log for {sym} could not be written "
                        f"({type(e).__name__})",
                        "WARN",
                    )
                    if error_logger:
                        try:
                            error_logger(f"emergency sell log {sym}", e)
                        except Exception:
                            pass

                if is_partial_close:
                    log_event(
                        f"  [LIVE] {sym}: {residual_amount:.6f} remains "
                        "after emergency sell and stays durable for "
                        "retry/reconcile",
                        "WARN",
                    )
                    failed.append(
                        f"{sym}: residual kept in state "
                        f"({residual_amount:.6f})"
                    )
                    total_pnl += profit_usdt
                    continue

                removed = _remove_accounted_state(state, sym, {
                    "accounting_already_booked": True,
                    "accounting_booked_sell_time": sell_time,
                    "accounting_booked_exchange_order_id": exch_oid,
                    "accounting_booked_reason": f"Emergency Close ({reason})",
                }, expected_row=d)
                if not removed:
                    failed.append(f"{sym}: cleanup failed after booked close")
                    log_event(
                        f"  {sym}: close already booked, but claim/state "
                        f"cleanup failed; state kept for retry",
                        "WARN")
                    total_pnl += profit_usdt
                    continue
                closed_count += 1
                total_pnl += profit_usdt

            except Exception as e:
                failed.append(f"{sym}: {e}")
                log_event(f"Emergency close {sym} error: {e}", "WARN")
                if error_logger:
                    error_logger(f"emergency_close {sym}", e)

    log_event(
        f"Emergency close result: {closed_count} closed "
        f"(Total PnL: {total_pnl:+.2f} USDT), {len(failed)} failed",
        "INFO"
    )

    if failed and not simulation and telegram_token and telegram_chat_id:
        try:
            shown = failed[:5]
            more = len(failed) - 5
            extra = f" (+{more} more)" if more > 0 else ""
            send_telegram(telegram_token, telegram_chat_id,
                f" [{bot_name}] EMERGENCY CLOSE INCOMPLETE\n"
                f"Failed: {', '.join(shown)}{extra}\n"
                f"Close MANUALLY on the exchange!"
            )
        except Exception as e:
            log_event(f"Telegram failed: {e}", "WARN")

    return {
        "closed_count": closed_count,
        "failed_count": len(failed),
        "failed": failed,
    }
