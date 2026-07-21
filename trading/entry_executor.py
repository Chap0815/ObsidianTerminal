"""Persistent entry-order journal and optional maker-first execution."""
from __future__ import annotations

import math
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass

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


def _external_text(value) -> str:
    """Normalize untrusted exchange text without letting rendering abort state."""
    try:
        return str(value).strip().lower()
    except Exception:
        return ""


def _order_status(order: dict, target_amount: float) -> str:
    filled = _number(order.get("filled")) if isinstance(order, dict) else 0.0
    status = _external_text(order.get("status", "")) if isinstance(order, dict) else ""
    if filled >= target_amount * (1.0 - 1e-9) or status in {"filled", "closed"}:
        return "FILLED"
    if filled > 0.0 or status in {"partially_filled", "partiallyfilled"}:
        return "PARTIAL"
    return "OPEN"


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


def _refresh_order(exchange, order: dict, symbol: str) -> dict:
    order_id = _order_id(order)
    fetch_order = getattr(exchange, "fetch_order", None)
    if not order_id or not callable(fetch_order):
        return order
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
    if _order_status(latest, amount) == "FILLED" or not _order_id(latest):
        return latest
    for _attempt in range(config.market_reconcile_attempts):
        if config.market_reconcile_delay_seconds:
            time.sleep(config.market_reconcile_delay_seconds)
        try:
            latest = _refresh_order(exchange, latest, symbol)
        except Exception:
            continue
        if _order_status(latest, amount) == "FILLED":
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
    if config.tca_enabled or config.mode == "enforce":
        try:
            from trading.execution_quality import build_arrival_tca

            book = exchange.fetch_order_book(symbol, limit=config.depth_levels)
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
            status = _transition_from_order(journal, intent_id, order, amount)
            if status == "FILLED":
                journal.transition(intent_id, "FINALIZED")
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
        book = book or exchange.fetch_order_book(symbol, limit=config.depth_levels)
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
        status = _transition_from_order(journal, intent_id, maker_order, amount)
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
        latest = exchange.fetch_order(order_id, symbol)
        latest_status = _order_status(latest, amount)
        if latest_status == "FILLED":
            if status == "OPEN":
                journal.transition(
                    intent_id,
                    "FILLED",
                    exchange_order_id=_order_id(latest),
                    filled_amount=_number(latest.get("filled")),
                    filled_notional=_number(latest.get("cost")),
                )
            else:
                journal.transition(intent_id, "FILLED")
            journal.transition(intent_id, "FINALIZED")
            _record_fill_tca(
                journal, intent_id, arrival, latest, symbol=symbol, side=side
            )
            return latest
        if latest_status == "PARTIAL" and status == "OPEN":
            journal.transition(
                intent_id,
                "PARTIAL",
                filled_amount=_number(latest.get("filled")),
                filled_notional=_number(latest.get("cost")),
            )
            status = "PARTIAL"
        journal.transition(intent_id, "CANCELING")
        exchange.cancel_order(order_id, symbol)
        canceled = exchange.fetch_order(order_id, symbol)
        canceled_status = _external_text(canceled.get("status", ""))
        if canceled_status not in {"canceled", "cancelled", "expired"}:
            raise RuntimeError("maker cancel was not verified; market fallback refused")
        journal.transition(
            intent_id,
            "CANCELED",
            filled_amount=_number(canceled.get("filled")),
            filled_notional=_number(canceled.get("cost")),
        )
        filled = _number(canceled.get("filled"))
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
        fallback_cost = _number(fallback.get("cost"))
        fill_tolerance = max(1e-12, amount * 1e-9)
        if fallback_filled > residual + fill_tolerance:
            raise RuntimeError("market fallback exceeded verified residual")
        total_filled = filled + fallback_filled
        total_cost = _number(canceled.get("cost")) + fallback_cost
        if total_filled < amount * (1.0 - 1e-9):
            raise RuntimeError("market fallback did not fill verified residual")
        journal.transition(
            intent_id,
            "FILLED",
            exchange_order_id=_order_id(fallback),
            filled_amount=total_filled,
            filled_notional=total_cost,
        )
        journal.transition(intent_id, "FINALIZED")
        result = dict(fallback)
        result["filled"] = total_filled
        result["cost"] = total_cost
        result["maker_filled"] = filled
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
        transition_order_intent,
    )

    unresolved = []
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
            order = _refresh_order(exchange, order, str(intent["symbol"]))
        except Exception:
            pass
        target_amount = _positive_finite_float(
            intent["target_amount"],
            "persisted target amount",
        )
        effective_order = order
        if fallback_client_order_id and current == "FILLED":
            effective_order = dict(order)
            effective_order["filled"] = _number(intent.get("filled_amount"))
            effective_order["cost"] = _number(intent.get("filled_notional"))
            status = "FILLED"
        elif fallback_client_order_id:
            maker_filled = _number(intent.get("filled_amount"))
            maker_cost = _number(intent.get("filled_notional"))
            residual = max(0.0, target_amount - maker_filled)
            fallback_filled = _number(order.get("filled"))
            tolerance = max(1e-12, target_amount * 1e-9)
            if fallback_filled > residual + tolerance:
                if current != "RECOVERY_REQUIRED":
                    transition_order_intent(
                        intent_id,
                        "RECOVERY_REQUIRED",
                        error="recovered fallback exceeded verified residual",
                    )
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
            effective_order = dict(order)
            effective_order["filled"] = maker_filled + fallback_filled
            effective_order["cost"] = maker_cost + _number(order.get("cost"))
            status = "FILLED"
        else:
            status = _order_status(order, target_amount)
        try:
            if status == "FILLED":
                if current == "PREPARED":
                    transition_order_intent(intent_id, "SUBMITTING")
                if current != "FILLED":
                    transition_order_intent(
                        intent_id,
                        "FILLED",
                        exchange_order_id=effective_order.get("id"),
                        filled_amount=_number(effective_order.get("filled")),
                        filled_notional=_number(effective_order.get("cost")),
                        fee_usdt=(
                            _number(intent.get("fee_usdt")) + _fee_usdt(order)
                            if fallback_client_order_id
                            else _fee_usdt(order)
                        ),
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
        except ValueError:
            pass
        unresolved.append(intent)
    return unresolved
