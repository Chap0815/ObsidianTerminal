"""Persistent entry-order journal and optional maker-first execution."""
from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.futures_order import (
    FuturesOrderNotSubmitted,
    _TradeRecoveryOrder,
    _exchange_id,
    _explicit_order_ids,
    _order_confirmed_terminal_zero_fill,
    _order_client_id_conflicts,
    _order_request_conflicts,
    _requested_position_side,
)
from bot_utils.order_utils import explicit_trade_symbol_matches
from bot_utils.silent_log import silent_log
from core import clock as exchange_clock
from core.constants import DEFAULT_TAKER_FEE


def _finite_float(value, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be a finite number")
    return parsed


def _positive_finite_float(value, field_name: str) -> float:
    parsed = _finite_float(value, field_name)
    if parsed <= 0.0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _fallback_client_order_id(intent_id: str, primary_client_order_id: str) -> str:
    from trading.execution_quality import make_client_order_id

    prefix = str(primary_client_order_id).partition("-")[0]
    fallback_id = make_client_order_id(intent_id, "fallback", prefix)
    if fallback_id == str(primary_client_order_id):
        raise ValueError("fallback client order id must differ from primary id")
    return fallback_id


@dataclass(frozen=True)
class MakerFirstConfig:
    mode: str = "disabled"
    ttl_seconds: float = 3.0
    market_fallback: bool = False
    depth_levels: int = 20
    tca_enabled: bool = True
    market_reconcile_attempts: int = 3
    market_reconcile_delay_seconds: float = 0.35

    def __post_init__(self) -> None:
        if self.mode not in {"disabled", "shadow", "enforce"}:
            raise ValueError(f"invalid maker-first mode: {self.mode!r}")
        ttl = _finite_float(self.ttl_seconds, "maker-first TTL")
        if not 0.0 <= ttl <= 30.0:
            raise ValueError("maker-first TTL must be between 0 and 30 seconds")
        if (
            isinstance(self.market_reconcile_attempts, bool)
            or not isinstance(self.market_reconcile_attempts, int)
            or not 0 <= self.market_reconcile_attempts <= 10
        ):
            raise ValueError("market reconciliation attempts must be between 0 and 10")
        delay = _finite_float(
            self.market_reconcile_delay_seconds,
            "market reconciliation delay",
        )
        if not 0.0 <= delay <= 5.0:
            raise ValueError("market reconciliation delay must be between 0 and 5 seconds")
        if (
            isinstance(self.depth_levels, bool)
            or not isinstance(self.depth_levels, int)
            or not 5 <= self.depth_levels <= 100
        ):
            raise ValueError("TCA depth levels must be between 5 and 100")
        if not isinstance(self.market_fallback, bool):
            raise ValueError("market fallback must be boolean")
        if not isinstance(self.tca_enabled, bool):
            raise ValueError("TCA enabled must be boolean")


class _DatabaseJournal:
    @staticmethod
    def create(intent_id, **fields):
        from core.database import create_order_intent

        create_order_intent(intent_id, **fields)

    @staticmethod
    def transition(intent_id, status, **fields):
        from core.database import transition_order_intent

        transition_order_intent(intent_id, status, **fields)

    @staticmethod
    def get(intent_id):
        from core.database import get_order_intent

        return get_order_intent(intent_id)

    @staticmethod
    def record_tca(intent_id, stage, payload):
        from core.database import record_execution_tca

        record_execution_tca(intent_id, stage, payload)

    @staticmethod
    def schedule_markouts(intent_id, **fields):
        from core.database import schedule_execution_markouts

        schedule_execution_markouts(intent_id, **fields)

    @staticmethod
    def record_fallback_evidence(intent_id, **fields):
        from core.database import record_order_intent_fallback_evidence

        return record_order_intent_fallback_evidence(intent_id, **fields)


def _mark_recovery_required_after_error(
    journal,
    intent_id: str,
    original_error: Exception,
) -> None:
    """Best-effort recovery marker that never masks the trading failure."""
    try:
        journal.transition(
            intent_id,
            "RECOVERY_REQUIRED",
            error=f"{type(original_error).__name__}: {original_error}",
        )
    except Exception as journal_error:
        try:
            original_error.add_note(
                "RECOVERY_REQUIRED journal update failed: "
                f"{type(journal_error).__name__}"
            )
        except Exception:
            pass
        try:
            silent_log("entry recovery journal", journal_error)
        except Exception:
            pass


def _durable_finalized_order_matches(
    journal,
    intent_id: str,
    order: object,
    target_amount: float,
) -> bool:
    """Prove a complete venue result survived an ambiguous finalization error."""
    getter = getattr(journal, "get", None)
    if not callable(getter) or not isinstance(order, dict):
        return False
    try:
        intent = getter(intent_id)
        if not isinstance(intent, Mapping) or intent.get("status") != "FINALIZED":
            return False
        persisted_target = _positive_finite_float(
            intent.get("target_amount"), "persisted target amount"
        )
        persisted_filled = _positive_finite_float(
            intent.get("filled_amount"), "persisted filled amount"
        )
        persisted_notional = _positive_finite_float(
            intent.get("filled_notional"), "persisted filled notional"
        )
        order_filled = _positive_finite_float(order.get("filled"), "order fill")
        order_notional = _recovered_fill_notional(order, order_filled)
        order_ids = _explicit_order_ids(order)
        persisted_order_id = _external_text(intent.get("exchange_order_id"))
    except Exception:
        return False
    amount_tolerance = max(1e-12, target_amount * 1e-9)
    notional_tolerance = max(1e-12, persisted_notional * 1e-9)
    return (
        abs(persisted_target - target_amount) <= amount_tolerance
        and abs(persisted_filled - target_amount) <= amount_tolerance
        and abs(order_filled - persisted_filled) <= amount_tolerance
        and order_notional is not None
        and abs(order_notional - persisted_notional) <= notional_tolerance
        and len(order_ids) == 1
        and order_ids == {persisted_order_id}
    )


def _number(value, default=0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) and result >= 0.0 else default


def _recovered_fill_notional(order: dict, filled: float) -> float | None:
    if not isinstance(order, dict) or filled <= 0.0:
        return None
    cost = _number(order.get("cost"))
    if cost > 0.0:
        return cost
    for price_field in ("average", "price"):
        price = _number(order.get(price_field))
        if price <= 0.0:
            continue
        notional = price * filled
        if math.isfinite(notional) and notional > 0.0:
            return notional
    return None


def _has_fill_notional_without_amount(order: dict) -> bool:
    if not isinstance(order, dict):
        return False
    return _number(order.get("filled")) <= 0.0 and _number(order.get("cost")) > 0.0


def _verified_final_canceled_fill(order: dict) -> float:
    if not isinstance(order, dict) or "filled" not in order:
        raise RuntimeError("final canceled maker fill amount unavailable")
    try:
        filled = _finite_float(order["filled"], "final canceled maker fill amount")
    except ValueError as exc:
        raise RuntimeError("final canceled maker fill amount unavailable") from exc
    if filled < 0.0:
        raise RuntimeError("final canceled maker fill amount unavailable")
    return filled


def _with_verified_fill_notional(order: dict, context: str) -> dict:
    if not isinstance(order, dict):
        raise RuntimeError(f"{context} returned no order object")
    filled = _number(order.get("filled"))
    notional = _recovered_fill_notional(order, filled)
    if filled <= 0.0 or notional is None:
        raise RuntimeError(f"{context} fill notional unavailable")
    normalized = dict(order)
    normalized["cost"] = notional
    return normalized


def _external_text(value) -> str:
    """Normalize untrusted exchange text without letting rendering abort state."""
    try:
        return str(value).strip().lower()
    except Exception:
        return ""


def _order_status(order: dict, target_amount: float) -> str:
    filled = _number(order.get("filled")) if isinstance(order, dict) else 0.0
    status = _external_text(order.get("status", "")) if isinstance(order, dict) else ""
    if filled >= target_amount * (1.0 - 1e-9):
        return "FILLED"
    if filled > 0.0:
        return "PARTIAL"
    if status in {"filled", "closed"}:
        return "FILLED"
    if status in {"partially_filled", "partiallyfilled"}:
        return "PARTIAL"
    return "OPEN"


def _require_positive_partial_fill(order: dict, context: str) -> None:
    raw_status = (
        _external_text(order.get("status", ""))
        if isinstance(order, dict)
        else ""
    )
    if (
        raw_status in {"partially_filled", "partiallyfilled"}
        and _number(order.get("filled")) <= 0.0
    ):
        raise _OrderSnapshotConflict(
            f"{context} partial status without positive fill evidence"
        )


_TERMINAL_NO_FILL_STATUSES = frozenset({
    "rejected", "canceled", "cancelled", "expired",
})
_TERMINAL_PARTIAL_STATUSES = _TERMINAL_NO_FILL_STATUSES | {"filled", "closed"}


class _OrderSnapshotConflict(ValueError):
    """Conflicting cumulative fill evidence must not be retried away."""


class _MalformedExchangeResponse(RuntimeError):
    """A budgeted exchange call returned no usable response object."""


class _RefreshIdentityConflict(_MalformedExchangeResponse):
    """A refresh returned explicit evidence for a different order."""


def _is_explicit_finite_zero(value) -> bool:
    if value is None or isinstance(value, bool):
        return False
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return math.isfinite(parsed) and parsed == 0.0


def _recovered_terminal_without_fill(order: dict, intent: dict) -> str | None:
    if not isinstance(order, dict) or not isinstance(intent, dict):
        return None
    status = _external_text(order.get("status", ""))
    if status not in _TERMINAL_NO_FILL_STATUSES:
        return None
    zero_fields = (
        order.get("filled"),
        order.get("cost"),
        intent.get("filled_amount"),
        intent.get("filled_notional"),
    )
    return status if all(_is_explicit_finite_zero(v) for v in zero_fields) else None


def _order_id(order: dict) -> str | None:
    if not isinstance(order, dict):
        return None
    explicit_ids = _explicit_order_ids(order)
    return next(iter(explicit_ids)) if len(explicit_ids) == 1 else None


def _fee_usdt(order: dict) -> float:
    fee, _known = _fee_usdt_known(order)
    return fee


def _fee_usdt_known(order: dict) -> tuple[float, bool]:
    try:
        from trading.fee_utils import extract_fee_usdt_known

        fee, known = extract_fee_usdt_known(order)
        if known is not True:
            return 0.0, False
        validated_fee = _finite_float(fee, "actual fee")
        if validated_fee < 0.0:
            raise ValueError("actual fee must be non-negative")
        return validated_fee, True
    except Exception as exc:
        silent_log("entry fee extraction", exc)
        return 0.0, False


def _max_known_order_fee(*orders: dict) -> tuple[float, bool]:
    known_fees = []
    for order in orders:
        fee, known = _fee_usdt_known(order)
        if known:
            known_fees.append(fee)
    if not known_fees:
        return 0.0, False
    return max(known_fees), True


def _require_api_budget(endpoint: str, *, critical: bool = False) -> None:
    allowed = (
        try_consume_api_call(endpoint, critical=True)
        if critical
        else try_consume_api_call(endpoint)
    )
    if not allowed:
        raise RuntimeError(f"API budget denied: {endpoint}")


def _budgeted_exchange_call(
    endpoint: str,
    call: Callable[[], object],
    *,
    critical: bool = False,
    denied_exception=RuntimeError,
    response_validator: Callable[[object], bool] | None = None,
):
    try:
        reservation = (
            try_consume_api_call(
                endpoint,
                critical=True,
                return_reservation=True,
            )
            if critical
            else try_consume_api_call(endpoint, return_reservation=True)
        )
    except Exception as exc:
        raise denied_exception(
            f"API budget gate unavailable: {endpoint}"
        ) from exc
    if not reservation:
        raise denied_exception(f"API budget denied: {endpoint}")
    try:
        response = call()
    except Exception:
        if isinstance(reservation, ApiCallReservation):
            try:
                record_api_error(endpoint, reservation)
            except Exception:
                pass
        raise
    if response_validator is not None:
        validation_error = None
        try:
            response_valid = bool(response_validator(response))
        except (_MalformedExchangeResponse, _OrderSnapshotConflict) as exc:
            validation_error = exc
            response_valid = False
        except Exception:
            response_valid = False
        if not response_valid:
            if isinstance(reservation, ApiCallReservation):
                try:
                    record_api_error(endpoint, reservation)
                except Exception:
                    pass
            if validation_error is not None:
                raise validation_error
            raise _MalformedExchangeResponse(
                f"malformed exchange response: {endpoint}"
            )
    return response


def _is_nonempty_order_response(response: object) -> bool:
    return isinstance(response, dict) and bool(response)


def _is_two_sided_order_book_response(
    response: object,
    expected_symbol: str | None = None,
) -> bool:
    if not isinstance(response, Mapping):
        return False
    if expected_symbol is not None:
        try:
            symbol_matches = explicit_trade_symbol_matches(
                dict(response),
                expected_symbol,
            )
        except Exception:
            return False
        if not symbol_matches:
            return False
    bids = response.get("bids") or []
    asks = response.get("asks") or []
    try:
        bid = _positive_finite_float(bids[0][0], "book bid")
        ask = _positive_finite_float(asks[0][0], "book ask")
    except (IndexError, KeyError, TypeError, ValueError):
        return False
    return bid < ask


def _finalize_not_submitted_intent(
    journal,
    intent_id: str,
    error: Exception | str,
    *,
    current_status: str = "SUBMITTING",
    filled_amount: float = 0.0,
    filled_notional: float = 0.0,
    fee_usdt: float = 0.0,
) -> None:
    reason = f"entry order not submitted: {error}"
    transition = (
        journal.transition
        if callable(getattr(journal, "transition", None))
        else journal
    )
    if current_status != "RECOVERY_REQUIRED":
        transition(intent_id, "RECOVERY_REQUIRED", error=reason)
    transition(
        intent_id,
        "CANCELED",
        filled_amount=filled_amount,
        filled_notional=filled_notional,
        fee_usdt=fee_usdt,
        error=reason,
    )
    transition(
        intent_id,
        "FINALIZED",
        release_terminal_zero_claim=(
            filled_amount == 0.0 and filled_notional == 0.0
        ),
        error=reason,
    )


def _finalize_pre_submit_rejection(
    journal,
    intent_id: str,
    blocked: FuturesOrderNotSubmitted,
    *,
    current_status: str = "PREPARED",
    filled_amount: float = 0.0,
    filled_notional: float = 0.0,
    fee_usdt: float = 0.0,
) -> None:
    """Preserve the proven no-submit outcome if journal cleanup fails."""
    try:
        _finalize_not_submitted_intent(
            journal,
            intent_id,
            blocked,
            current_status=current_status,
            filled_amount=filled_amount,
            filled_notional=filled_notional,
            fee_usdt=fee_usdt,
        )
    except Exception as cleanup_error:
        try:
            blocked.add_note(
                "pre-submit intent finalization failed: "
                f"{type(cleanup_error).__name__}"
            )
        except Exception:
            pass
        try:
            silent_log("pre-submit intent finalization", cleanup_error)
        except Exception:
            pass


def _legacy_budget_denial_proves_not_submitted(intent: Mapping) -> bool:
    def explicit_zero(value) -> bool:
        if value is None or isinstance(value, bool):
            return False
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return False
        return math.isfinite(number) and number == 0.0

    return (
        intent.get("status") == "RECOVERY_REQUIRED"
        and str(intent.get("last_error") or "").startswith(
            "RuntimeError: API budget exhausted before open "
        )
        and bool(str(intent.get("client_order_id") or "").strip())
        and not str(intent.get("exchange_order_id") or "").strip()
        and not str(intent.get("fallback_client_order_id") or "").strip()
        and not str(intent.get("fallback_exchange_order_id") or "").strip()
        and all(
            explicit_zero(intent.get(field))
            for field in (
                "filled_amount",
                "filled_notional",
                "fee_usdt",
                "fallback_filled_amount",
                "fallback_filled_notional",
                "fallback_fee_usdt",
            )
        )
        and intent.get("fallback_notional_complete") in (0, False)
    )


def _refresh_order(
    exchange,
    order: dict,
    symbol: str,
    *,
    target_amount: float | None = None,
    response_validator: Callable[[object], bool] | None = None,
) -> dict:
    order_id = _order_id(order)
    fetch_order = getattr(exchange, "fetch_order", None)
    if not order_id or not callable(fetch_order):
        return order
    monotonic_fields = None

    def _validate_refresh_response(response: object) -> bool:
        nonlocal monotonic_fields
        if not _is_nonempty_order_response(response):
            return False
        if response_validator is not None and not response_validator(response):
            return False
        if target_amount is not None:
            response_ids = _explicit_order_ids(response)
            if len(response_ids) > 1 or (
                response_ids and response_ids != {order_id}
            ):
                raise _RefreshIdentityConflict(
                    "market order reconciliation changed order id"
                )
            monotonic_fields = _monotonic_order_fill_fields(
                order,
                response,
                target_amount,
            )
        return True

    try:
        refreshed = _budgeted_exchange_call(
            "entry_executor_fetch_order",
            lambda: fetch_order(order_id, symbol),
            critical=True,
            response_validator=_validate_refresh_response,
        )
    except _RefreshIdentityConflict:
        raise
    except _MalformedExchangeResponse:
        return order
    if target_amount is not None:
        if monotonic_fields is None:
            return order
    merged = (
        _TradeRecoveryOrder(order)
        if (
            isinstance(order, _TradeRecoveryOrder)
            and refreshed.get("amount") in (None, "")
        )
        else dict(order)
    )
    merged.update(refreshed)
    if monotonic_fields is not None:
        merged["filled"] = monotonic_fields["filled_amount"]
        if "filled_notional" in monotonic_fields:
            merged["cost"] = monotonic_fields["filled_notional"]
        if "fee_usdt" in monotonic_fields:
            fee = {
                "cost": monotonic_fields["fee_usdt"],
                "currency": "USDT",
            }
            merged["fee"] = fee
            merged["fees"] = [dict(fee)]
    return merged


def _reconcile_market_response(
    exchange,
    order: dict,
    *,
    symbol: str,
    amount: float,
    config: MakerFirstConfig,
) -> dict:
    """Boundedly enrich a submitted order; never submits or cancels anything."""
    latest = order
    expected_order_id = _order_id(latest)
    if (
        _order_status(latest, amount) == "FILLED"
        and _recovered_fill_notional(latest, _number(latest.get("filled")))
        is not None
    ) or not expected_order_id:
        return latest
    for _attempt in range(config.market_reconcile_attempts):
        if config.market_reconcile_delay_seconds:
            time.sleep(config.market_reconcile_delay_seconds)
        try:
            latest = _refresh_order(
                exchange,
                latest,
                symbol,
                target_amount=amount,
            )
        except (_OrderSnapshotConflict, _RefreshIdentityConflict):
            raise
        except Exception:
            continue
        if (
            _order_status(latest, amount) == "FILLED"
            and _recovered_fill_notional(latest, _number(latest.get("filled")))
            is not None
        ):
            break
    return latest


def _transition_from_order(journal, intent_id: str, order: dict, amount: float) -> str:
    _require_positive_partial_fill(order, "entry order")
    status = _order_status(order, amount)
    filled = _number(order.get("filled"))
    cost = _number(order.get("cost"))
    journal.transition(
        intent_id,
        status,
        exchange_order_id=_order_id(order),
        filled_amount=filled,
        filled_notional=cost,
        fee_usdt=_fee_usdt(order),
    )
    return status


def _monotonic_order_fill_fields(
    current_order: dict,
    observed_order: dict,
    target_amount: float,
) -> dict | None:
    current_filled = _number(current_order.get("filled"))
    observed_filled = _number(observed_order.get("filled"))
    tolerance = max(1e-12, target_amount * 1e-9)
    if observed_filled + tolerance < current_filled:
        return None
    current_notional = _recovered_fill_notional(
        current_order,
        current_filled,
    )
    observed_notional = _recovered_fill_notional(
        observed_order,
        observed_filled,
    )
    notional_tolerance = max(
        1e-12,
        max(current_notional or 0.0, observed_notional or 0.0) * 1e-9,
    )
    if (
        observed_filled > current_filled + tolerance
        and current_notional is not None
    ):
        if observed_notional is None:
            raise _OrderSnapshotConflict(
                "partial fill notional unavailable as fill amount grew"
            )
        if observed_notional + notional_tolerance < current_notional:
            raise _OrderSnapshotConflict(
                "partial fill notional decreased as fill amount grew"
            )
    fields = {
        "exchange_order_id": _order_id(observed_order),
        "filled_amount": max(current_filled, observed_filled),
    }
    notionals = tuple(
        value
        for value in (
            current_notional,
            observed_notional,
        )
        if value is not None
    )
    if notionals:
        fields["filled_notional"] = max(notionals)
    strongest_fee, fee_known = _max_known_order_fee(
        current_order,
        observed_order,
    )
    if fee_known:
        fields["fee_usdt"] = strongest_fee
    return fields


def _strongest_order_fill_evidence(
    orders: tuple[dict, ...],
    target_amount: float,
) -> tuple[float, float | None]:
    strongest = dict(orders[0])
    for observed in orders[1:]:
        fields = _monotonic_order_fill_fields(
            strongest,
            observed,
            target_amount,
        )
        if fields is None:
            continue
        merged = dict(observed)
        merged["filled"] = fields["filled_amount"]
        if "filled_notional" in fields:
            merged["cost"] = fields["filled_notional"]
        else:
            merged.pop("cost", None)
        strongest = merged
    filled = _number(strongest.get("filled"))
    return filled, _recovered_fill_notional(strongest, filled)


def execute_entry_order(
    *,
    exchange,
    symbol: str,
    side: str,
    amount: float,
    intent_id: str,
    client_order_id: str,
    bot_name: str,
    mode: str,
    reference_price: float,
    market_order,
    maker_order_params: Mapping | None = None,
    config: MakerFirstConfig,
    journal=None,
    pre_submit_guard: Callable[[], bool] | None = None,
) -> dict:
    """Execute one entry intent; unknown cancel state never falls back."""
    amount = _positive_finite_float(amount, "entry amount")
    reference_price = _positive_finite_float(
        reference_price,
        "entry reference price",
    )
    normalized_side = str(side).strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("entry side must be buy or sell")
    side = normalized_side
    if maker_order_params is None:
        maker_params = {}
    elif isinstance(maker_order_params, Mapping):
        maker_params = dict(maker_order_params)
    else:
        raise ValueError("maker order params must be a mapping")
    reduce_only = maker_params.get("reduceOnly")
    if reduce_only is True or str(reduce_only).strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        raise ValueError("entry maker order cannot be reduce-only")
    maker_position_side = _requested_position_side(maker_params)
    exchange_id = _exchange_id(exchange)
    journal = journal or _DatabaseJournal()
    journal.create(
        intent_id,
        bot_name=bot_name,
        mode=mode,
        symbol=symbol,
        direction="LONG" if side == "buy" else "SHORT",
        target_amount=amount,
        target_price=reference_price,
        client_order_id=client_order_id,
    )
    def _require_pre_submit_guard() -> None:
        if pre_submit_guard is None:
            return
        try:
            admission_allowed = pre_submit_guard()
        except Exception as exc:
            raise FuturesOrderNotSubmitted(
                "entry admission guard unavailable"
            ) from exc
        if admission_allowed is not True:
            raise FuturesOrderNotSubmitted(
                "entry blocked by pre-submit runtime safety gate"
            )

    try:
        _require_pre_submit_guard()
    except FuturesOrderNotSubmitted as blocked:
        _finalize_pre_submit_rejection(journal, intent_id, blocked)
        raise
    journal.transition(intent_id, "SUBMITTING")

    arrival = None
    book = None
    book_budget_denied = False
    if config.tca_enabled or config.mode == "enforce":
        try:
            book_reservation = try_consume_api_call(
                "entry_executor_fetch_order_book",
                return_reservation=True,
            )
            if not book_reservation:
                book_budget_denied = True
            else:
                try:
                    book = exchange.fetch_order_book(
                        symbol, limit=config.depth_levels
                    )
                    if not _is_two_sided_order_book_response(book, symbol):
                        raise _MalformedExchangeResponse(
                            "malformed exchange response: "
                            "entry_executor_fetch_order_book"
                        )
                except Exception:
                    if isinstance(book_reservation, ApiCallReservation):
                        try:
                            record_api_error(
                                "entry_executor_fetch_order_book",
                                book_reservation,
                            )
                        except Exception:
                            pass
                    raise
        except Exception as exc:
            silent_log("entry arrival TCA", exc)
            book = None
        if book is not None:
            try:
                from trading.execution_quality import build_arrival_tca

                arrival = build_arrival_tca(
                    book,
                    side=side,
                    amount=amount,
                    local_time_ms=int(exchange_clock.now_ms()),
                )
            except Exception as exc:
                silent_log("entry arrival TCA", exc)
                arrival = None
                if config.mode == "enforce":
                    blocked = FuturesOrderNotSubmitted(
                        "maker-first order book validation failed"
                    )
                    _finalize_pre_submit_rejection(
                        journal,
                        intent_id,
                        blocked,
                        current_status="SUBMITTING",
                    )
                    raise blocked from exc
            else:
                recorder = getattr(journal, "record_tca", None)
                if callable(recorder):
                    try:
                        recorder(intent_id, "arrival", asdict(arrival))
                    except Exception as exc:
                        silent_log("entry arrival TCA persistence", exc)

    if config.mode != "enforce":
        order = None
        try:
            _require_pre_submit_guard()
            order = market_order()
            if not isinstance(order, dict):
                raise RuntimeError("market order returned no order object")
            order = _reconcile_market_response(
                exchange,
                order,
                symbol=symbol,
                amount=amount,
                config=config,
            )
            terminal_zero_fill = _order_confirmed_terminal_zero_fill(order)
            market_status = _order_status(order, amount)
            explicit_order_ids = _explicit_order_ids(order)
            if len(explicit_order_ids) > 1:
                raise _OrderSnapshotConflict(
                    "market order response has conflicting order ids"
                )
            if (
                market_status in {"FILLED", "PARTIAL"}
                and len(explicit_order_ids) != 1
            ):
                raise _OrderSnapshotConflict(
                    "positive market fill has missing or conflicting order ids"
                )
            if terminal_zero_fill and len(explicit_order_ids) != 1:
                raise _OrderSnapshotConflict(
                    "terminal zero-fill has missing or conflicting order ids"
                )
            if (
                (terminal_zero_fill or market_status in {"FILLED", "PARTIAL"})
                and _order_request_conflicts(
                    order,
                    symbol,
                    side,
                    maker_position_side,
                    exchange_id,
                    expected_reduce_only=False,
                    allow_one_way_position_side=True,
                    expected_client_id=client_order_id,
                    expected_amount=amount,
                )
            ):
                raise _OrderSnapshotConflict(
                    "market order response conflicts with submitted request"
                )
            if market_status == "FILLED":
                order = _with_verified_fill_notional(order, "market")
            status = _transition_from_order(journal, intent_id, order, amount)
            if status == "FILLED":
                journal.transition(intent_id, "FINALIZED")
            elif terminal_zero_fill:
                terminal_status = _external_text(order.get("status", ""))
                reason = f"market order terminal zero-fill: {terminal_status}"
                journal.transition(intent_id, "CANCELING")
                journal.transition(
                    intent_id,
                    "CANCELED",
                    exchange_order_id=_order_id(order),
                    filled_amount=0.0,
                    filled_notional=0.0,
                    fee_usdt=0.0,
                    error=reason,
                )
                journal.transition(
                    intent_id,
                    "FINALIZED",
                    release_terminal_zero_claim=True,
                    error=reason,
                )
            elif (
                status == "PARTIAL"
                and _external_text(order.get("status", ""))
                in _TERMINAL_PARTIAL_STATUSES
            ):
                journal.transition(
                    intent_id,
                    "RECOVERY_REQUIRED",
                    error="terminal market partial fill requires reconciliation",
                )
            _record_fill_tca(
                journal, intent_id, arrival, order, symbol=symbol, side=side
            )
            return order
        except FuturesOrderNotSubmitted as exc:
            _finalize_pre_submit_rejection(
                journal,
                intent_id,
                exc,
                current_status="SUBMITTING",
            )
            raise
        except Exception as exc:
            if _durable_finalized_order_matches(
                journal,
                intent_id,
                order,
                amount,
            ):
                try:
                    from core.logger import log_struct

                    log_struct(
                        "entry_finalization_converged",
                        bot=bot_name,
                        mode=mode,
                        symbol=symbol,
                        intent_id=intent_id,
                        error_type=type(exc).__name__,
                        outcome="durable_exact_match",
                    )
                except Exception:
                    pass
                return order
            _mark_recovery_required_after_error(journal, intent_id, exc)
            raise

    fallback_not_submitted_fields = None
    fallback_not_submitted_order = None
    try:
        if not book:
            if book_budget_denied:
                raise FuturesOrderNotSubmitted(
                    "API budget denied: entry_executor_fetch_order_book"
                )
            try:
                book = _budgeted_exchange_call(
                    "entry_executor_fetch_order_book",
                    lambda: exchange.fetch_order_book(
                        symbol, limit=config.depth_levels
                    ),
                    denied_exception=FuturesOrderNotSubmitted,
                    response_validator=lambda response: (
                        _is_two_sided_order_book_response(response, symbol)
                    ),
                )
            except _MalformedExchangeResponse as exc:
                raise FuturesOrderNotSubmitted(
                    "maker-first order book validation failed"
                ) from exc
            try:
                from trading.execution_quality import build_arrival_tca

                arrival = build_arrival_tca(
                    book,
                    side=side,
                    amount=amount,
                    local_time_ms=int(exchange_clock.now_ms()),
                )
            except Exception as exc:
                raise FuturesOrderNotSubmitted(
                    "maker-first order book validation failed"
                ) from exc
            recorder = getattr(journal, "record_tca", None)
            if callable(recorder):
                try:
                    recorder(intent_id, "arrival", asdict(arrival))
                except Exception as exc:
                    silent_log("entry arrival TCA persistence", exc)
        levels = book.get("bids") if str(side).lower() == "buy" else book.get("asks")
        if not levels:
            raise RuntimeError("maker-first top of book unavailable")
        maker_price = _positive_finite_float(levels[0][0], "maker price")
        maker_params.update(
            {
                "postOnly": True,
                "clientOrderId": client_order_id,
                "externalOid": client_order_id,
            }
        )
        _require_pre_submit_guard()

        def _validate_maker_create_response(response: object) -> bool:
            if not _is_nonempty_order_response(response):
                return False
            response_ids = _explicit_order_ids(response)
            if len(response_ids) > 1:
                raise _MalformedExchangeResponse(
                    "maker create changed order id"
                )
            if _order_client_id_conflicts(response, client_order_id):
                raise _MalformedExchangeResponse(
                    "maker create changed client order id"
                )
            if _order_request_conflicts(
                response,
                symbol,
                side,
                maker_position_side,
                exchange_id,
                expected_reduce_only=False,
                expected_amount=amount,
            ):
                raise _MalformedExchangeResponse(
                    "maker create changed symbol or side"
                )
            response_status = _order_status(response, amount)
            if (
                response_status in {"FILLED", "PARTIAL"}
                and len(response_ids) != 1
            ):
                raise _MalformedExchangeResponse(
                    "maker create changed order id"
                )
            return True

        maker_order = _budgeted_exchange_call(
            "entry_executor_create_maker",
            lambda: exchange.create_order(
                symbol,
                "limit",
                side,
                amount,
                maker_price,
                params=maker_params,
            ),
            denied_exception=FuturesOrderNotSubmitted,
            response_validator=_validate_maker_create_response,
        )
        initial_status = _order_status(maker_order, amount)
        if initial_status == "FILLED":
            try:
                maker_order = _with_verified_fill_notional(
                    maker_order,
                    "maker",
                )
            except RuntimeError:
                order_id = _order_id(maker_order)
                if not order_id:
                    raise

                def _validate_maker_notional_recovery(response: object) -> bool:
                    if not _is_nonempty_order_response(response):
                        return False
                    if _explicit_order_ids(response) != {order_id}:
                        raise _MalformedExchangeResponse(
                            "maker fill notional recovery changed order id"
                        )
                    if _order_client_id_conflicts(response, client_order_id):
                        raise _MalformedExchangeResponse(
                            "maker fill notional recovery changed client order id"
                        )
                    if _order_request_conflicts(
                        response,
                        symbol,
                        side,
                        maker_position_side,
                        exchange_id,
                        expected_reduce_only=False,
                        expected_amount=amount,
                    ):
                        raise _MalformedExchangeResponse(
                            "maker fill notional recovery changed symbol or side"
                        )
                    if _order_status(response, amount) != "FILLED":
                        raise _MalformedExchangeResponse(
                            "maker fill notional recovery was inconclusive"
                        )
                    return True

                refreshed_maker = _budgeted_exchange_call(
                    "entry_executor_fetch_order",
                    lambda: exchange.fetch_order(order_id, symbol),
                    critical=True,
                    response_validator=_validate_maker_notional_recovery,
                )
                maker_order = _with_verified_fill_notional(
                    refreshed_maker,
                    "maker",
                )
        status = _transition_from_order(journal, intent_id, maker_order, amount)
        if (
            status == "PARTIAL"
            and _external_text(maker_order.get("status", ""))
            in _TERMINAL_PARTIAL_STATUSES
        ):
            _record_fill_tca(
                journal,
                intent_id,
                arrival,
                maker_order,
                symbol=symbol,
                side=side,
            )
            journal.transition(
                intent_id,
                "RECOVERY_REQUIRED",
                error="terminal maker partial fill requires reconciliation",
            )
            return maker_order
        if status == "FILLED":
            journal.transition(intent_id, "FINALIZED")
            _record_fill_tca(
                journal,
                intent_id,
                arrival,
                maker_order,
                symbol=symbol,
                side=side,
            )
            return maker_order
        if config.ttl_seconds:
            time.sleep(config.ttl_seconds)
        order_id = _order_id(maker_order)
        if not order_id:
            raise RuntimeError("maker order returned no usable order id")

        def _validate_maker_status_refresh(response: object) -> bool:
            if not _is_nonempty_order_response(response):
                return False
            response_ids = _explicit_order_ids(response)
            if len(response_ids) > 1 or (
                response_ids and response_ids != {order_id}
            ):
                raise _MalformedExchangeResponse(
                    "maker status refresh changed order id"
                )
            if _order_client_id_conflicts(response, client_order_id):
                raise _MalformedExchangeResponse(
                    "maker status refresh changed client order id"
                )
            if _order_request_conflicts(
                response,
                symbol,
                side,
                maker_position_side,
                exchange_id,
                expected_reduce_only=False,
                expected_amount=amount,
            ):
                raise _MalformedExchangeResponse(
                    "maker status refresh changed symbol or side"
                )
            if (
                _order_status(response, amount) in {"FILLED", "PARTIAL"}
                and response_ids != {order_id}
            ):
                raise _MalformedExchangeResponse(
                    "maker status refresh changed order id"
                )
            return True

        latest = _budgeted_exchange_call(
            "entry_executor_fetch_order",
            lambda: exchange.fetch_order(order_id, symbol),
            critical=True,
            response_validator=_validate_maker_status_refresh,
        )
        _require_positive_partial_fill(latest, "maker status refresh")
        latest_status = _order_status(latest, amount)
        if latest_status == "FILLED":
            latest = _with_verified_fill_notional(latest, "maker")
            fill_fields = {
                "exchange_order_id": _order_id(latest),
                "filled_amount": _number(latest.get("filled")),
                "filled_notional": _number(latest.get("cost")),
            }
            latest_fee, latest_fee_known = _fee_usdt_known(latest)
            if latest_fee_known:
                fill_fields["fee_usdt"] = latest_fee
            journal.transition(intent_id, "FILLED", **fill_fields)
            journal.transition(intent_id, "FINALIZED")
            _record_fill_tca(
                journal, intent_id, arrival, latest, symbol=symbol, side=side
            )
            return latest
        if latest_status == "PARTIAL":
            partial_fields = _monotonic_order_fill_fields(
                maker_order,
                latest,
                amount,
            )
            if partial_fields is not None:
                journal.transition(
                    intent_id,
                    "PARTIAL",
                    **partial_fields,
                )
            status = "PARTIAL"
            if (
                _external_text(latest.get("status", ""))
                in _TERMINAL_PARTIAL_STATUSES
            ):
                _record_fill_tca(
                    journal,
                    intent_id,
                    arrival,
                    latest,
                    symbol=symbol,
                    side=side,
                )
                journal.transition(
                    intent_id,
                    "RECOVERY_REQUIRED",
                    error="terminal maker partial fill requires reconciliation",
                )
                return latest
        journal.transition(intent_id, "CANCELING")
        _budgeted_exchange_call(
            "entry_executor_cancel_maker",
            lambda: exchange.cancel_order(order_id, symbol),
            critical=True,
        )

        def _validate_maker_cancel_verification(response: object) -> bool:
            if not _is_nonempty_order_response(response):
                return False
            if _explicit_order_ids(response) != {order_id}:
                raise _MalformedExchangeResponse(
                    "maker cancel verification changed order id"
                )
            if _order_client_id_conflicts(response, client_order_id):
                raise _MalformedExchangeResponse(
                    "maker cancel verification changed client order id"
                )
            response_request = dict(response)
            response_request.pop("filled", None)
            if _order_request_conflicts(
                response_request,
                symbol,
                side,
                maker_position_side,
                exchange_id,
                expected_reduce_only=False,
                expected_amount=amount,
            ):
                raise _MalformedExchangeResponse(
                    "maker cancel verification changed symbol or side"
                )
            response_status = _external_text(response.get("status", ""))
            if response_status not in {"canceled", "cancelled", "expired"}:
                raise _MalformedExchangeResponse(
                    "maker cancel was not verified; market fallback refused"
                )
            try:
                _verified_final_canceled_fill(response)
            except RuntimeError as exc:
                raise _MalformedExchangeResponse(str(exc)) from exc
            return True

        canceled = _budgeted_exchange_call(
            "entry_executor_fetch_order",
            lambda: exchange.fetch_order(order_id, symbol),
            critical=True,
            response_validator=_validate_maker_cancel_verification,
        )
        _verified_final_canceled_fill(canceled)
        if any(
            _has_fill_notional_without_amount(snapshot)
            for snapshot in (maker_order, latest, canceled)
        ):
            raise RuntimeError(
                "maker reported positive fill notional without fill amount"
            )
        filled, maker_notional = _strongest_order_fill_evidence(
            (maker_order, latest, canceled),
            amount,
        )
        fill_tolerance = max(1e-12, amount * 1e-9)
        if filled > amount + fill_tolerance:
            raise RuntimeError("canceled maker fill exceeded target")
        if filled > 0.0 and maker_notional is None:
            raise RuntimeError("canceled maker fill notional unavailable")
        canceled = dict(canceled)
        canceled["filled"] = filled
        if maker_notional is not None:
            canceled["cost"] = maker_notional
        maker_fee, maker_fee_known = _max_known_order_fee(
            maker_order,
            latest,
            canceled,
        )
        canceled_fields = {
            "filled_amount": filled,
            "filled_notional": maker_notional or 0.0,
            "fee_usdt": (
                maker_fee
                if maker_fee_known
                else (maker_notional or 0.0) * DEFAULT_TAKER_FEE
            ),
        }
        maker_fee = canceled_fields["fee_usdt"]
        journal.transition(
            intent_id,
            "CANCELED",
            **canceled_fields,
        )
        residual = max(0.0, amount - filled)
        if residual <= amount * 1e-9 or not config.market_fallback:
            journal.transition(
                intent_id,
                "FINALIZED",
                release_terminal_zero_claim=(filled == 0.0),
            )
            if filled > 0.0:
                _record_fill_tca(
                    journal,
                    intent_id,
                    arrival,
                    canceled,
                    symbol=symbol,
                    side=side,
                )
            return canceled
        fallback_client_order_id = _fallback_client_order_id(
            intent_id,
            client_order_id,
        )
        journal.transition(
            intent_id,
            "FALLBACK_SUBMITTING",
            fallback_client_order_id=fallback_client_order_id,
        )
        fallback_not_submitted_fields = dict(canceled_fields)
        fallback_not_submitted_order = canceled
        _require_pre_submit_guard()
        fallback = market_order(residual, fallback_client_order_id)
        if not isinstance(fallback, dict):
            raise RuntimeError("market fallback returned no order object")
        _require_positive_partial_fill(fallback, "market fallback")
        fallback_status = _order_status(fallback, residual)
        fallback_order_ids = _explicit_order_ids(fallback)
        if len(fallback_order_ids) > 1 or (
            fallback_status in {"FILLED", "PARTIAL"}
            and len(fallback_order_ids) != 1
        ):
            raise RuntimeError("market fallback changed order id")
        fallback_filled = _number(fallback.get("filled"))
        if fallback_filled > residual + fill_tolerance:
            raise RuntimeError("market fallback exceeded verified residual")
        if _order_request_conflicts(
            fallback,
            symbol,
            side,
            maker_position_side,
            exchange_id,
            expected_reduce_only=False,
            allow_one_way_position_side=True,
            expected_client_id=fallback_client_order_id,
            expected_amount=residual,
        ):
            raise RuntimeError("market fallback changed submitted request")
        fallback_notional = _recovered_fill_notional(fallback, fallback_filled)
        fallback_fee, fallback_fee_known = _fee_usdt_known(fallback)
        if fallback_filled > 0.0 and fallback_notional is None:
            raise RuntimeError("market fallback fill notional unavailable")
        fallback_cost = fallback_notional or 0.0
        if not fallback_fee_known:
            fallback_fee = fallback_cost * DEFAULT_TAKER_FEE
        total_filled = filled + fallback_filled
        total_cost = (maker_notional or 0.0) + fallback_cost
        if total_filled < amount * (1.0 - 1e-9):
            raise RuntimeError("market fallback did not fill verified residual")
        fallback_recorder = getattr(
            journal,
            "record_fallback_evidence",
            None,
        )
        if callable(fallback_recorder):
            fallback_recorder(
                intent_id,
                fallback_exchange_order_id=_order_id(fallback),
                fallback_filled_amount=fallback_filled,
                fallback_filled_notional=fallback_cost,
                fallback_notional_complete=True,
                fallback_fee_usdt=fallback_fee,
                error="fallback fill verified before finalization",
            )
        filled_fields = {
            "exchange_order_id": _order_id(fallback),
            "filled_amount": total_filled,
            "filled_notional": total_cost,
        }
        filled_fields["fee_usdt"] = maker_fee + fallback_fee
        journal.transition(intent_id, "FILLED", **filled_fields)
        journal.transition(intent_id, "FINALIZED")
        result = dict(fallback)
        result["filled"] = total_filled
        result["cost"] = total_cost
        result["maker_filled"] = filled
        total_fee = maker_fee + fallback_fee
        result["fee"] = {"cost": total_fee, "currency": "USDT"}
        result["fees"] = [dict(result["fee"])]
        _record_fill_tca(
            journal, intent_id, arrival, result, symbol=symbol, side=side
        )
        return result
    except FuturesOrderNotSubmitted as exc:
        resolution_fields = fallback_not_submitted_fields or {}
        _finalize_pre_submit_rejection(
            journal,
            intent_id,
            exc,
            current_status=(
                "FALLBACK_SUBMITTING"
                if fallback_not_submitted_fields is not None
                else "SUBMITTING"
            ),
            **resolution_fields,
        )
        if (
            resolution_fields.get("filled_amount", 0.0) > 0.0
            and fallback_not_submitted_order is not None
        ):
            _record_fill_tca(
                journal,
                intent_id,
                arrival,
                fallback_not_submitted_order,
                symbol=symbol,
                side=side,
            )
            return fallback_not_submitted_order
        raise
    except Exception as exc:
        _mark_recovery_required_after_error(journal, intent_id, exc)
        raise


def _record_fill_tca(
    journal,
    intent_id: str,
    arrival,
    order: dict,
    *,
    symbol: str,
    side: str,
) -> None:
    recorder = getattr(journal, "record_tca", None)
    if arrival is None or not callable(recorder) or not isinstance(order, dict):
        return
    try:
        from trading.execution_quality import compute_fill_tca

        average = order.get("average")
        if average is None:
            filled = _number(order.get("filled"))
            cost = _number(order.get("cost"))
            average = cost / filled if filled else None
        if average is None:
            return
        fee_cost, fee_known = _fee_usdt_known(order)
        notional = _number(order.get("cost"))
        if fee_known and notional:
            fee_rate = fee_cost / notional
        else:
            from core.constants import DEFAULT_TAKER_FEE

            fee_rate = DEFAULT_TAKER_FEE
        fill_tca = compute_fill_tca(
            arrival,
            average_fill_price=float(average),
            fee_rate=fee_rate,
        )
        recorder(
            intent_id,
            "fill",
            asdict(fill_tca),
        )
        scheduler = getattr(journal, "schedule_markouts", None)
        if callable(scheduler):
            scheduler(
                intent_id,
                symbol=symbol,
                side=side,
                reference_price=fill_tca.average_fill_price,
            )
    except Exception as exc:
        silent_log("entry fill TCA", exc)
        return


def recover_nonterminal_order_intents(
    exchange,
    bot_name: str,
    log_event=None,
    *,
    intent_ids: set[str] | None = None,
    recovery_report: dict | None = None,
) -> list[dict]:
    """Reconcile persisted intents without ever submitting a replacement order."""
    from bot_utils.futures_order import _find_order_by_client_id
    from core.database import (
        get_order_intent,
        list_nonterminal_order_intents,
        record_order_intent_fallback_evidence,
        transition_order_intent,
    )

    unresolved = []
    selected_ids = None if intent_ids is None else set(intent_ids)
    if selected_ids is not None and (
        len(selected_ids) > 32
        or any(not isinstance(value, str) or not value for value in selected_ids)
    ):
        raise ValueError("recovery intent filter is invalid")
    if recovery_report is not None:
        if not isinstance(recovery_report, dict):
            raise ValueError("recovery report must be a dictionary")
        recovery_report.clear()

    def report_evidence(intent: dict, lookup_status: dict) -> None:
        if recovery_report is None or len(recovery_report) >= 32:
            return
        result = str(lookup_status.get("result") or "unavailable").lower()
        if result not in {"found", "empty", "unavailable", "conflict"}:
            result = "unavailable"
        sources = lookup_status.get("sources")
        if not isinstance(sources, dict):
            sources = {}
        recovery_report[intent["intent_id"]] = {
            "symbol": str(intent.get("symbol") or "")[:64],
            "evidence_state": result,
            "sources": dict(list(sources.items())[:8]),
            "budget_denied": lookup_status.get("budget_denied") is True,
            "complete_negative": (
                result == "empty"
                and lookup_status.get("complete_negative") is True
                and lookup_status.get("budget_denied") is not True
            ),
        }

    def persisted_snapshot(intent_id: str, fallback: dict) -> dict:
        try:
            row = get_order_intent(intent_id)
        except (TypeError, ValueError):
            # The identity came from durable storage.  Preserve fail-closed
            # recovery of legacy/corrupt rows without penalizing the normal
            # primary-key lookup path.
            return next(
                (
                    candidate
                    for candidate in list_nonterminal_order_intents(bot_name)
                    if candidate.get("intent_id") == intent_id
                ),
                dict(fallback),
            )
        if row is not None and row.get("status") != "FINALIZED":
            return row
        return dict(fallback)

    def lookup_since_ms(intent: dict) -> int | None:
        try:
            created = datetime.strptime(
                str(intent["created_at"]), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)
            # Include a bounded pre-submit margin for exchange timestamp skew.
            return max(1, int(created.timestamp() * 1000) - 60_000)
        except (KeyError, TypeError, ValueError, OverflowError):
            return None

    for intent in list_nonterminal_order_intents(bot_name):
        intent_id = intent["intent_id"]
        if selected_ids is not None and intent_id not in selected_ids:
            continue
        current = intent["status"]
        raw_direction = intent.get("direction")
        direction = (
            raw_direction.strip().upper()
            if isinstance(raw_direction, str)
            else ""
        )
        if direction not in {"LONG", "SHORT"}:
            error = "persisted order intent direction is invalid"
            try:
                if current != "RECOVERY_REQUIRED":
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error=error,
                    )
            except (TypeError, ValueError):
                pass
            if log_event:
                try:
                    log_event(
                        f"Entry recovery blocked for {intent_id}: {error}",
                        "ERROR",
                    )
                except Exception:
                    pass
            unresolved.append(persisted_snapshot(intent_id, intent))
            continue
        fallback_client_order_id = intent.get("fallback_client_order_id")
        lookup_client_order_id = (
            fallback_client_order_id or intent["client_order_id"]
        )
        lookup_status: dict = {}
        order = _find_order_by_client_id(
            exchange,
            intent["symbol"],
            lookup_client_order_id,
            log_event=log_event,
            lookup_status=lookup_status,
            lookup_since_ms=lookup_since_ms(intent),
        )
        report_evidence(intent, lookup_status)
        if order is None:
            if _legacy_budget_denial_proves_not_submitted(intent):
                try:
                    _finalize_not_submitted_intent(
                        transition_order_intent,
                        intent_id,
                        intent["last_error"],
                        current_status=current,
                    )
                except (TypeError, ValueError):
                    unresolved.append(persisted_snapshot(intent_id, intent))
                continue
            if current in {"PREPARED", "SUBMITTING"}:
                transition_order_intent(
                    intent_id,
                    "RECOVERY_REQUIRED",
                    error="startup lookup found no conclusive exchange order",
                )
                intent = dict(intent)
                intent["status"] = "RECOVERY_REQUIRED"
            unresolved.append(intent)
            continue
        try:
            target_amount = _positive_finite_float(
                intent["target_amount"],
                "persisted target amount",
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            error = "persisted order intent target amount is invalid"
            if recovery_report is not None and intent_id in recovery_report:
                recovery_report[intent_id]["evidence_state"] = "conflict"
            try:
                if current != "RECOVERY_REQUIRED":
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error=error,
                    )
            except (TypeError, ValueError):
                pass
            if log_event:
                try:
                    log_event(
                        f"Entry recovery blocked for {intent_id}: {error}",
                        "ERROR",
                    )
                except Exception:
                    pass
            try:
                silent_log("entry recovery target amount", exc)
            except Exception:
                pass
            unresolved.append(persisted_snapshot(intent_id, intent))
            continue
        expected_snapshot_amount = target_amount
        if fallback_client_order_id:
            persisted_fallback_amount = _number(
                intent.get("fallback_filled_amount")
            )
            persisted_total_amount = _number(intent.get("filled_amount"))
            persisted_maker_amount = max(
                0.0,
                persisted_total_amount - persisted_fallback_amount,
            )
            expected_snapshot_amount = max(
                0.0,
                target_amount - persisted_maker_amount,
            )
        expected_order_ids = _explicit_order_ids(order)
        expected_side = "buy" if direction == "LONG" else "sell"

        def _validate_startup_refresh(response: object) -> bool:
            response_ids = _explicit_order_ids(response)
            if len(response_ids) > 1:
                raise _RefreshIdentityConflict(
                    "startup refresh has missing or conflicting order ids"
                )
            if (
                len(expected_order_ids) == 1
                and response_ids
                and response_ids != expected_order_ids
            ):
                raise _RefreshIdentityConflict(
                    "startup refresh changed order id"
                )
            if _order_client_id_conflicts(response, lookup_client_order_id):
                raise _RefreshIdentityConflict(
                    "startup refresh changed client order id"
                )
            if _order_request_conflicts(
                response,
                str(intent["symbol"]),
                expected_side,
                direction.lower(),
                _exchange_id(exchange),
                expected_reduce_only=False,
                allow_one_way_position_side=True,
            ):
                raise _RefreshIdentityConflict(
                    "startup refresh changed symbol, side, or position side"
                )
            response_request = dict(response)
            response_request.pop("filled", None)
            if _order_request_conflicts(
                response_request,
                str(intent["symbol"]),
                expected_side,
                direction.lower(),
                _exchange_id(exchange),
                expected_reduce_only=False,
                allow_one_way_position_side=True,
                expected_amount=expected_snapshot_amount,
            ):
                raise _RefreshIdentityConflict(
                    "startup refresh changed order amount"
                )
            return True

        refresh_identity_error = None
        try:
            refreshed_order = _refresh_order(
                exchange,
                order,
                str(intent["symbol"]),
                response_validator=_validate_startup_refresh,
            )
        except _RefreshIdentityConflict as exc:
            refreshed_order = order
            refresh_identity_error = str(exc)
        except Exception:
            refreshed_order = order
        refreshed_order_ids = _explicit_order_ids(refreshed_order)
        if refresh_identity_error is not None:
            pass
        elif len(expected_order_ids) != 1 or len(refreshed_order_ids) != 1:
            refresh_identity_error = (
                "startup refresh has missing or conflicting order ids"
            )
        elif expected_order_ids and refreshed_order_ids != expected_order_ids:
            refresh_identity_error = "startup refresh changed order id"
        elif _order_client_id_conflicts(
            refreshed_order,
            lookup_client_order_id,
        ):
            refresh_identity_error = "startup refresh changed client order id"
        else:
            if _order_request_conflicts(
                refreshed_order,
                str(intent["symbol"]),
                expected_side,
                direction.lower(),
                _exchange_id(exchange),
                expected_reduce_only=False,
                allow_one_way_position_side=True,
            ):
                refresh_identity_error = (
                    "startup refresh changed symbol, side, or position side"
                )
            else:
                refreshed_request = (
                    _TradeRecoveryOrder(refreshed_order)
                    if isinstance(refreshed_order, _TradeRecoveryOrder)
                    else dict(refreshed_order)
                )
                refreshed_request.pop("filled", None)
                if _order_request_conflicts(
                    refreshed_request,
                    str(intent["symbol"]),
                    expected_side,
                    direction.lower(),
                    _exchange_id(exchange),
                    expected_reduce_only=False,
                    allow_one_way_position_side=True,
                    expected_amount=expected_snapshot_amount,
                ):
                    refresh_identity_error = (
                        "startup refresh changed order amount"
                    )
        if refresh_identity_error is not None:
            if recovery_report is not None and intent_id in recovery_report:
                recovery_report[intent_id]["evidence_state"] = "conflict"
            if current != "RECOVERY_REQUIRED":
                transition_order_intent(
                    intent_id,
                    "RECOVERY_REQUIRED",
                    error=refresh_identity_error,
                )
            unresolved.append(persisted_snapshot(intent_id, intent))
            continue
        order = refreshed_order
        terminal_without_fill = _recovered_terminal_without_fill(order, intent)
        if terminal_without_fill is not None:
            reason = (
                "startup recovery proved terminal zero-fill order: "
                f"{terminal_without_fill}"
            )
            fields = {
                "exchange_order_id": _order_id(order),
                "filled_amount": 0.0,
                "filled_notional": 0.0,
                "error": reason,
            }
            try:
                if current == "CANCELED":
                    transition_order_intent(
                        intent_id,
                        "FINALIZED",
                        release_terminal_zero_claim=True,
                        **fields,
                    )
                else:
                    if current != "RECOVERY_REQUIRED":
                        transition_order_intent(
                            intent_id, "RECOVERY_REQUIRED", **fields)
                    transition_order_intent(
                        intent_id,
                        "CANCELED",
                        **fields,
                    )
                    transition_order_intent(
                        intent_id,
                        "FINALIZED",
                        release_terminal_zero_claim=True,
                        error=reason,
                    )
            except ValueError as exc:
                persisted_intent = persisted_snapshot(intent_id, intent)
                if persisted_intent.get("status") != "RECOVERY_REQUIRED":
                    try:
                        transition_order_intent(
                            intent_id,
                            "RECOVERY_REQUIRED",
                            error=(
                                "recovered terminal zero-fill evidence "
                                f"rejected: {exc}"
                            ),
                        )
                    except ValueError:
                        pass
                    persisted_intent = persisted_snapshot(intent_id, intent)
                unresolved.append(persisted_intent)
                continue
            continue
        effective_order = order
        effective_fee_usdt, effective_fee_known = _fee_usdt_known(order)
        if fallback_client_order_id and current == "FILLED":
            effective_order = dict(order)
            effective_order["filled"] = _number(intent.get("filled_amount"))
            effective_order["cost"] = _number(intent.get("filled_notional"))
            status = "FILLED"
        elif fallback_client_order_id:
            persisted_fallback_filled = _number(
                intent.get("fallback_filled_amount")
            )
            persisted_fallback_cost = _number(
                intent.get("fallback_filled_notional")
            )
            persisted_fallback_fee = _number(intent.get("fallback_fee_usdt"))
            maker_filled = max(
                0.0,
                _number(intent.get("filled_amount")) - persisted_fallback_filled,
            )
            maker_cost = max(
                0.0,
                _number(intent.get("filled_notional")) - persisted_fallback_cost,
            )
            maker_fee = max(
                0.0,
                _number(intent.get("fee_usdt")) - persisted_fallback_fee,
            )
            residual = max(0.0, target_amount - maker_filled)
            recovered_fallback_filled = _number(order.get("filled"))
            recovered_fallback_notional = _recovered_fill_notional(
                order,
                recovered_fallback_filled,
            )
            fallback_filled = max(
                persisted_fallback_filled,
                recovered_fallback_filled,
            )
            fallback_cost = max(
                persisted_fallback_cost,
                recovered_fallback_notional or 0.0,
            )
            tolerance = max(1e-12, target_amount * 1e-9)
            prior_notional_complete = (
                intent.get("fallback_notional_complete") == 1
            )
            if recovered_fallback_filled > persisted_fallback_filled + tolerance:
                fallback_notional_complete = (
                    recovered_fallback_notional is not None
                )
            elif recovered_fallback_filled + tolerance < persisted_fallback_filled:
                fallback_notional_complete = prior_notional_complete
            else:
                fallback_notional_complete = (
                    prior_notional_complete
                    or recovered_fallback_notional is not None
                )
            recovered_fallback_fee = effective_fee_usdt
            if (
                not effective_fee_known
                and recovered_fallback_notional is not None
            ):
                recovered_fallback_fee = (
                    recovered_fallback_notional * DEFAULT_TAKER_FEE
                )
            fallback_fee = max(persisted_fallback_fee, recovered_fallback_fee)
            effective_fee_usdt = maker_fee + fallback_fee
            fallback_error = "recovered fallback fill pending reconciliation"
            if fallback_filled > residual + tolerance:
                fallback_error = "recovered fallback exceeded verified residual"
            elif fallback_filled < residual - tolerance:
                fallback_error = "recovered fallback did not fill verified residual"
            elif not fallback_notional_complete:
                fallback_error = "recovered fallback fill notional unavailable"
            if fallback_filled > 0.0 or persisted_fallback_filled > 0.0:
                try:
                    intent = record_order_intent_fallback_evidence(
                        intent_id,
                        fallback_exchange_order_id=_order_id(order),
                        fallback_filled_amount=recovered_fallback_filled,
                        fallback_filled_notional=(
                            recovered_fallback_notional or 0.0
                        ),
                        fallback_notional_complete=(
                            recovered_fallback_notional is not None
                        ),
                        fallback_fee_usdt=recovered_fallback_fee,
                        error=fallback_error,
                    )
                    current = "RECOVERY_REQUIRED"
                    fallback_filled = _number(
                        intent.get("fallback_filled_amount")
                    )
                    fallback_cost = _number(
                        intent.get("fallback_filled_notional")
                    )
                    fallback_fee = _number(intent.get("fallback_fee_usdt"))
                    fallback_notional_complete = (
                        intent.get("fallback_notional_complete") == 1
                    )
                    effective_fee_usdt = maker_fee + fallback_fee
                except ValueError:
                    if current != "RECOVERY_REQUIRED":
                        transition_order_intent(
                            intent_id,
                            "RECOVERY_REQUIRED",
                            error="recovered fallback evidence conflicts with journal",
                        )
                        current = "RECOVERY_REQUIRED"
                    intent = dict(intent)
                    intent["status"] = current
                    unresolved.append(intent)
                    continue
            if fallback_filled > residual + tolerance:
                intent = dict(intent)
                intent["status"] = "RECOVERY_REQUIRED"
                unresolved.append(intent)
                continue
            if fallback_filled < residual - tolerance:
                if (
                    _external_text(order.get("status", ""))
                    in {"filled", "closed", "canceled", "cancelled", "expired"}
                    and current != "RECOVERY_REQUIRED"
                ):
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error="recovered fallback did not fill verified residual",
                    )
                    intent = dict(intent)
                    intent["status"] = "RECOVERY_REQUIRED"
                unresolved.append(intent)
                continue
            if not fallback_notional_complete:
                intent = dict(intent)
                intent["status"] = "RECOVERY_REQUIRED"
                unresolved.append(intent)
                continue
            effective_order = dict(order)
            effective_order["filled"] = maker_filled + fallback_filled
            effective_order["cost"] = maker_cost + fallback_cost
            status = "FILLED"
        else:
            status = _order_status(order, target_amount)
        if not fallback_client_order_id and status == "FILLED":
            verified_filled = max(
                _number(effective_order.get("filled")),
                _number(intent.get("filled_amount")),
            )
            tolerance = max(1e-12, target_amount * 1e-9)
            if verified_filled < target_amount - tolerance:
                if current != "RECOVERY_REQUIRED":
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error=(
                            "recovered terminal status without verified "
                            "target fill"
                        ),
                    )
                    current = "RECOVERY_REQUIRED"
                intent = dict(intent)
                intent["status"] = current
                unresolved.append(intent)
                continue
            recovered_notional = _recovered_fill_notional(
                effective_order,
                _number(effective_order.get("filled")),
            )
            persisted_notional = _number(intent.get("filled_notional"))
            persisted_fill_is_complete = (
                current == "FILLED"
                and _number(intent.get("filled_amount"))
                >= target_amount - tolerance
                and persisted_notional > 0.0
            )
            if recovered_notional is None and not persisted_fill_is_complete:
                if current != "RECOVERY_REQUIRED":
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error="recovered full fill notional unavailable",
                    )
                    current = "RECOVERY_REQUIRED"
                intent = dict(intent)
                intent["status"] = current
                unresolved.append(intent)
                continue
            if recovered_notional is not None:
                effective_order = dict(effective_order)
                effective_order["cost"] = recovered_notional
        if not fallback_client_order_id and status == "PARTIAL":
            partial_amount = _number(effective_order.get("filled"))
            if partial_amount <= 0.0:
                try:
                    if current != "RECOVERY_REQUIRED":
                        transition_order_intent(
                            intent_id,
                            "RECOVERY_REQUIRED",
                            error=(
                                "recovered partial status without valid "
                                "positive fill evidence"
                            ),
                        )
                        current = "RECOVERY_REQUIRED"
                except ValueError:
                    pass
                intent = dict(intent)
                intent["status"] = current
                unresolved.append(intent)
                continue
            partial_fields = {
                "exchange_order_id": _order_id(effective_order),
                "filled_amount": partial_amount,
                "filled_notional": _number(effective_order.get("cost")),
                "fee_usdt": _fee_usdt(order),
            }
            try:
                if current == "PREPARED":
                    transition_order_intent(intent_id, "SUBMITTING")
                    current = "SUBMITTING"
                if current in {"SUBMITTING", "OPEN", "RECOVERY_REQUIRED"}:
                    transition_order_intent(
                        intent_id, "PARTIAL", **partial_fields)
                    current = "PARTIAL"
                elif current == "CANCELING":
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error="recovered partial fill during cancellation",
                        **partial_fields,
                    )
                    current = "RECOVERY_REQUIRED"
                elif current == "PARTIAL":
                    persisted_id = intent.get("exchange_order_id")
                    recovered_id = partial_fields["exchange_order_id"]
                    if (
                        persisted_id is not None
                        and recovered_id is not None
                        and str(persisted_id) != str(recovered_id)
                    ):
                        transition_order_intent(
                            intent_id,
                            "RECOVERY_REQUIRED",
                            error=(
                                "recovered partial fill order id conflicts "
                                "with persisted evidence"
                            ),
                        )
                        persisted_intent = persisted_snapshot(intent_id, intent)
                        unresolved.append(persisted_intent)
                        continue
                    persisted_amount = _number(intent.get("filled_amount"))
                    recovered_amount = partial_fields["filled_amount"]
                    fill_tolerance = max(1e-12, target_amount * 1e-9)
                    partial_snapshot_error = None
                    if recovered_amount + fill_tolerance < persisted_amount:
                        partial_fields = {
                            "exchange_order_id": persisted_id or recovered_id,
                            "filled_amount": persisted_amount,
                            "filled_notional": _number(
                                intent.get("filled_notional")
                            ),
                            "fee_usdt": _number(intent.get("fee_usdt")),
                        }
                    else:
                        persisted_notional = _number(
                            intent.get("filled_notional")
                        )
                        recovered_notional = partial_fields["filled_notional"]
                        notional_tolerance = max(
                            1e-12,
                            max(persisted_notional, recovered_notional) * 1e-9,
                        )
                        if (
                            recovered_amount > persisted_amount + fill_tolerance
                            and recovered_notional + notional_tolerance
                            < persisted_notional
                        ):
                            partial_snapshot_error = (
                                "recovered partial fill notional decreased "
                                "as fill amount grew"
                            )
                        partial_fields = {
                            "exchange_order_id": persisted_id or recovered_id,
                            "filled_amount": max(
                                persisted_amount,
                                recovered_amount,
                            ),
                            "filled_notional": max(
                                persisted_notional,
                                recovered_notional,
                            ),
                            "fee_usdt": max(
                                _number(intent.get("fee_usdt")),
                                partial_fields["fee_usdt"],
                            ),
                        }
                    if partial_snapshot_error is not None:
                        transition_order_intent(
                            intent_id,
                            "RECOVERY_REQUIRED",
                            error=partial_snapshot_error,
                            **partial_fields,
                        )
                        current = "RECOVERY_REQUIRED"
                    else:
                        transition_order_intent(
                            intent_id, "PARTIAL", **partial_fields)
                raw_status = _external_text(order.get("status", ""))
                if (
                    current == "PARTIAL"
                    and raw_status in _TERMINAL_PARTIAL_STATUSES
                ):
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error=(
                            "recovered terminal partial fill requires "
                            f"position reconciliation: {raw_status}"
                        ),
                    )
                    current = "RECOVERY_REQUIRED"
            except ValueError as exc:
                try:
                    if current != "RECOVERY_REQUIRED":
                        transition_order_intent(
                            intent_id,
                            "RECOVERY_REQUIRED",
                            error=f"recovered partial evidence rejected: {exc}",
                        )
                except ValueError:
                    pass
                persisted_intent = persisted_snapshot(intent_id, intent)
                unresolved.append(persisted_intent)
                continue
            intent = dict(intent)
            intent["status"] = current
            intent.update(partial_fields)
            unresolved.append(intent)
            continue
        try:
            if status == "FILLED":
                if current == "PREPARED":
                    transition_order_intent(intent_id, "SUBMITTING")
                if current != "FILLED":
                    transition_order_intent(
                        intent_id,
                        "FILLED",
                        exchange_order_id=_order_id(effective_order),
                        filled_amount=_number(effective_order.get("filled")),
                        filled_notional=_number(effective_order.get("cost")),
                        fee_usdt=effective_fee_usdt,
                    )
                try:
                    from core.database import get_latest_execution_tca_payload
                    from trading.execution_quality import ArrivalTCA

                    arrival_payload = get_latest_execution_tca_payload(
                        intent_id, "arrival"
                    )
                    fill_payload = get_latest_execution_tca_payload(intent_id, "fill")
                    if arrival_payload is not None and fill_payload is None:
                        _record_fill_tca(
                            _DatabaseJournal(),
                            intent_id,
                            ArrivalTCA(**arrival_payload),
                            effective_order,
                            symbol=str(intent["symbol"]),
                            side=(
                                "buy"
                                if str(intent["direction"]).upper() == "LONG"
                                else "sell"
                            ),
                        )
                except Exception as exc:
                    silent_log("recover entry TCA", exc)
                transition_order_intent(intent_id, "FINALIZED")
                continue
        except ValueError as exc:
            persisted_intent = persisted_snapshot(intent_id, intent)
            if persisted_intent.get("status") != "RECOVERY_REQUIRED":
                try:
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error=f"recovered full fill evidence rejected: {exc}",
                    )
                except ValueError:
                    pass
                persisted_intent = persisted_snapshot(intent_id, intent)
            unresolved.append(persisted_intent)
            continue
        unresolved.append(intent)
    if selected_ids is not None:
        return list_nonterminal_order_intents(bot_name)
    return unresolved
