"""Persistent entry-order journal and optional maker-first execution."""
from __future__ import annotations

import math
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass

from bot_utils.api_budget import try_consume_api_call
from bot_utils.futures_order import _order_client_id_conflicts
from bot_utils.silent_log import silent_log


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
    def record_tca(intent_id, stage, payload):
        from core.database import record_execution_tca

        record_execution_tca(intent_id, stage, payload)

    @staticmethod
    def schedule_markouts(intent_id, **fields):
        from core.database import schedule_execution_markouts

        schedule_execution_markouts(intent_id, **fields)


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


_TERMINAL_NO_FILL_STATUSES = frozenset({
    "rejected", "canceled", "cancelled", "expired",
})
_TERMINAL_PARTIAL_STATUSES = _TERMINAL_NO_FILL_STATUSES | {"filled", "closed"}


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
    from bot_utils.order_utils import order_id_text_or_none

    for field in ("id", "orderId"):
        text = order_id_text_or_none(order.get(field))
        if text is not None:
            return text
    return None


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


def _refresh_order(exchange, order: dict, symbol: str) -> dict:
    order_id = _order_id(order)
    fetch_order = getattr(exchange, "fetch_order", None)
    if not order_id or not callable(fetch_order):
        return order
    _require_api_budget("entry_executor_fetch_order", critical=True)
    refreshed = fetch_order(order_id, symbol)
    if not isinstance(refreshed, dict):
        return order
    merged = dict(order)
    merged.update(refreshed)
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
            latest = _refresh_order(exchange, latest, symbol)
        except Exception:
            continue
        refreshed_order_id = _order_id(latest)
        if (
            refreshed_order_id is not None
            and refreshed_order_id != expected_order_id
        ):
            raise RuntimeError("market order reconciliation changed order id")
        if (
            _order_status(latest, amount) == "FILLED"
            and _recovered_fill_notional(latest, _number(latest.get("filled")))
            is not None
        ):
            break
    return latest


def _transition_from_order(journal, intent_id: str, order: dict, amount: float) -> str:
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
    fields = {
        "exchange_order_id": _order_id(observed_order),
        "filled_amount": max(current_filled, observed_filled),
    }
    notionals = tuple(
        value
        for value in (
            _recovered_fill_notional(current_order, current_filled),
            _recovered_fill_notional(observed_order, observed_filled),
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
    journal.transition(intent_id, "SUBMITTING")

    arrival = None
    book = None
    book_budget_denied = False
    if config.tca_enabled or config.mode == "enforce":
        try:
            from trading.execution_quality import build_arrival_tca

            if not try_consume_api_call("entry_executor_fetch_order_book"):
                book_budget_denied = True
            else:
                book = exchange.fetch_order_book(
                    symbol, limit=config.depth_levels
                )
                arrival = build_arrival_tca(
                    book,
                    side=side,
                    amount=amount,
                    local_time_ms=int(time.time() * 1000),
                )
                recorder = getattr(journal, "record_tca", None)
                if callable(recorder):
                    recorder(intent_id, "arrival", asdict(arrival))
        except Exception as exc:
            silent_log("entry arrival TCA", exc)
            arrival = None

    if config.mode != "enforce":
        try:
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
            if _order_status(order, amount) == "FILLED":
                order = _with_verified_fill_notional(order, "market")
            status = _transition_from_order(journal, intent_id, order, amount)
            if status == "FILLED":
                journal.transition(intent_id, "FINALIZED")
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
        except Exception as exc:
            journal.transition(
                intent_id, "RECOVERY_REQUIRED", error=f"{type(exc).__name__}: {exc}"
            )
            raise

    try:
        if not book:
            if book_budget_denied or not try_consume_api_call(
                "entry_executor_fetch_order_book"
            ):
                raise RuntimeError(
                    "API budget denied: entry_executor_fetch_order_book"
                )
            book = exchange.fetch_order_book(
                symbol, limit=config.depth_levels
            )
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
        _require_api_budget("entry_executor_create_maker")
        maker_order = exchange.create_order(
            symbol,
            "limit",
            side,
            amount,
            maker_price,
            params=maker_params,
        )
        if not isinstance(maker_order, dict):
            raise RuntimeError("maker order returned no order object")
        if _order_client_id_conflicts(maker_order, client_order_id):
            raise RuntimeError("maker create changed client order id")
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
                _require_api_budget("entry_executor_fetch_order", critical=True)
                refreshed_maker = exchange.fetch_order(order_id, symbol)
                if not isinstance(refreshed_maker, dict):
                    raise RuntimeError(
                        "maker fill notional recovery returned no order object"
                    )
                refreshed_id = _order_id(refreshed_maker)
                if refreshed_id is not None and refreshed_id != order_id:
                    raise RuntimeError(
                        "maker fill notional recovery changed order id"
                    )
                if _order_client_id_conflicts(
                    refreshed_maker,
                    client_order_id,
                ):
                    raise RuntimeError(
                        "maker fill notional recovery changed client order id"
                    )
                if _order_status(refreshed_maker, amount) != "FILLED":
                    raise RuntimeError(
                        "maker fill notional recovery was inconclusive"
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
        _require_api_budget("entry_executor_fetch_order", critical=True)
        latest = exchange.fetch_order(order_id, symbol)
        latest_order_id = _order_id(latest)
        if latest_order_id is not None and latest_order_id != order_id:
            raise RuntimeError("maker status refresh changed order id")
        if _order_client_id_conflicts(latest, client_order_id):
            raise RuntimeError("maker status refresh changed client order id")
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
        _require_api_budget("entry_executor_cancel_maker", critical=True)
        exchange.cancel_order(order_id, symbol)
        _require_api_budget("entry_executor_fetch_order", critical=True)
        canceled = exchange.fetch_order(order_id, symbol)
        canceled_order_id = _order_id(canceled)
        if canceled_order_id is not None and canceled_order_id != order_id:
            raise RuntimeError("maker cancel verification changed order id")
        if _order_client_id_conflicts(canceled, client_order_id):
            raise RuntimeError(
                "maker cancel verification changed client order id"
            )
        canceled_status = _external_text(canceled.get("status", ""))
        if canceled_status not in {"canceled", "cancelled", "expired"}:
            raise RuntimeError("maker cancel was not verified; market fallback refused")
        if _has_fill_notional_without_amount(
            latest
        ) or _has_fill_notional_without_amount(canceled):
            raise RuntimeError(
                "maker reported positive fill notional without fill amount"
            )
        latest_filled = _number(latest.get("filled"))
        canceled_filled = _number(canceled.get("filled"))
        latest_notional = _recovered_fill_notional(latest, latest_filled)
        canceled_notional = _recovered_fill_notional(
            canceled,
            canceled_filled,
        )
        fill_tolerance = max(1e-12, amount * 1e-9)
        if latest_filled > canceled_filled + fill_tolerance:
            filled = latest_filled
            maker_notional = latest_notional
        elif canceled_filled > latest_filled + fill_tolerance:
            filled = canceled_filled
            maker_notional = canceled_notional
        else:
            filled = max(latest_filled, canceled_filled)
            maker_notionals = tuple(
                value
                for value in (latest_notional, canceled_notional)
                if value is not None
            )
            maker_notional = max(maker_notionals, default=None)
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
        }
        if maker_fee_known:
            canceled_fields["fee_usdt"] = maker_fee
        journal.transition(
            intent_id,
            "CANCELED",
            **canceled_fields,
        )
        if filled > 0.0 and maker_notional is None:
            raise RuntimeError("canceled maker fill notional unavailable")
        residual = max(0.0, amount - filled)
        if residual <= amount * 1e-9 or not config.market_fallback:
            journal.transition(intent_id, "FINALIZED")
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
        fallback = market_order(residual, fallback_client_order_id)
        if not isinstance(fallback, dict):
            raise RuntimeError("market fallback returned no order object")
        fallback_filled = _number(fallback.get("filled"))
        fallback_notional = _recovered_fill_notional(fallback, fallback_filled)
        fallback_fee, fallback_fee_known = _fee_usdt_known(fallback)
        if fallback_filled > 0.0 and fallback_notional is None:
            raise RuntimeError("market fallback fill notional unavailable")
        fallback_cost = fallback_notional or 0.0
        if fallback_filled > residual + fill_tolerance:
            raise RuntimeError("market fallback exceeded verified residual")
        total_filled = filled + fallback_filled
        total_cost = (maker_notional or 0.0) + fallback_cost
        if total_filled < amount * (1.0 - 1e-9):
            raise RuntimeError("market fallback did not fill verified residual")
        filled_fields = {
            "exchange_order_id": _order_id(fallback),
            "filled_amount": total_filled,
            "filled_notional": total_cost,
        }
        if maker_fee_known or fallback_fee_known:
            filled_fields["fee_usdt"] = maker_fee + fallback_fee
        journal.transition(intent_id, "FILLED", **filled_fields)
        journal.transition(intent_id, "FINALIZED")
        result = dict(fallback)
        result["filled"] = total_filled
        result["cost"] = total_cost
        result["maker_filled"] = filled
        if maker_fee_known and fallback_fee_known:
            total_fee = maker_fee + fallback_fee
            result["fee"] = {"cost": total_fee, "currency": "USDT"}
            result["fees"] = [dict(result["fee"])]
        _record_fill_tca(
            journal, intent_id, arrival, result, symbol=symbol, side=side
        )
        return result
    except Exception as exc:
        try:
            journal.transition(
                intent_id, "RECOVERY_REQUIRED", error=f"{type(exc).__name__}: {exc}"
            )
        except Exception:
            pass
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


def recover_nonterminal_order_intents(exchange, bot_name: str, log_event=None) -> list[dict]:
    """Reconcile persisted intents without ever submitting a replacement order."""
    from bot_utils.futures_order import _find_order_by_client_id
    from core.database import (
        list_nonterminal_order_intents,
        record_order_intent_fallback_evidence,
        transition_order_intent,
    )

    unresolved = []

    def persisted_snapshot(intent_id: str, fallback: dict) -> dict:
        return next(
            (
                row
                for row in list_nonterminal_order_intents(bot_name)
                if row.get("intent_id") == intent_id
            ),
            dict(fallback),
        )

    for intent in list_nonterminal_order_intents(bot_name):
        intent_id = intent["intent_id"]
        current = intent["status"]
        fallback_client_order_id = intent.get("fallback_client_order_id")
        lookup_client_order_id = (
            fallback_client_order_id or intent["client_order_id"]
        )
        order = _find_order_by_client_id(
            exchange,
            intent["symbol"],
            lookup_client_order_id,
            log_event=log_event,
        )
        if order is None:
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
            refreshed_order = _refresh_order(
                exchange,
                order,
                str(intent["symbol"]),
            )
        except Exception:
            refreshed_order = order
        expected_order_id = _order_id(order)
        refreshed_order_id = _order_id(refreshed_order)
        refresh_identity_error = None
        if (
            expected_order_id is not None
            and refreshed_order_id is not None
            and refreshed_order_id != expected_order_id
        ):
            refresh_identity_error = "startup refresh changed order id"
        elif _order_client_id_conflicts(
            refreshed_order,
            lookup_client_order_id,
        ):
            refresh_identity_error = "startup refresh changed client order id"
        if refresh_identity_error is not None:
            if current != "RECOVERY_REQUIRED":
                transition_order_intent(
                    intent_id,
                    "RECOVERY_REQUIRED",
                    error=refresh_identity_error,
                )
            unresolved.append(persisted_snapshot(intent_id, intent))
            continue
        order = refreshed_order
        target_amount = _positive_finite_float(
            intent["target_amount"],
            "persisted target amount",
        )
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
                    transition_order_intent(intent_id, "FINALIZED", **fields)
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
        effective_fee_usdt = _fee_usdt(order)
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
                        partial_fields = {
                            "exchange_order_id": persisted_id or recovered_id,
                            "filled_amount": max(
                                persisted_amount,
                                recovered_amount,
                            ),
                            "filled_notional": max(
                                _number(intent.get("filled_notional")),
                                partial_fields["filled_notional"],
                            ),
                            "fee_usdt": max(
                                _number(intent.get("fee_usdt")),
                                partial_fields["fee_usdt"],
                            ),
                        }
                    if current == "PARTIAL":
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
    return unresolved
