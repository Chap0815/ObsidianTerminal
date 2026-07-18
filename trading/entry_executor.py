"""Persistent entry-order journal and optional maker-first execution."""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class MakerFirstConfig:
    mode: str = "disabled"
    ttl_seconds: float = 3.0
    market_fallback: bool = False
    depth_levels: int = 20
    tca_enabled: bool = True

    def __post_init__(self) -> None:
        if self.mode not in {"disabled", "shadow", "enforce"}:
            raise ValueError(f"invalid maker-first mode: {self.mode!r}")
        if self.ttl_seconds < 0.0:
            raise ValueError("maker-first TTL cannot be negative")


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
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result >= 0.0 else default


def _order_status(order: dict, target_amount: float) -> str:
    filled = _number(order.get("filled")) if isinstance(order, dict) else 0.0
    status = str(order.get("status", "")).strip().lower() if isinstance(order, dict) else ""
    if filled >= target_amount * (1.0 - 1e-9) or status in {"filled", "closed"}:
        return "FILLED"
    if filled > 0.0 or status in {"partially_filled", "partiallyfilled"}:
        return "PARTIAL"
    return "OPEN"


def _transition_from_order(journal, intent_id: str, order: dict, amount: float) -> str:
    status = _order_status(order, amount)
    filled = _number(order.get("filled"))
    cost = _number(order.get("cost"))
    journal.transition(
        intent_id,
        status,
        exchange_order_id=order.get("id"),
        filled_amount=filled,
        filled_notional=cost,
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
    reference_price: float,
    market_order,
    config: MakerFirstConfig,
    journal=None,
) -> dict:
    """Execute one entry intent; unknown cancel state never falls back."""
    journal = journal or _DatabaseJournal()
    amount = float(amount)
    journal.create(
        intent_id,
        bot_name=bot_name,
        symbol=symbol,
        direction="LONG" if str(side).lower() == "buy" else "SHORT",
        target_amount=amount,
        target_price=float(reference_price),
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
        except Exception:
            arrival = None

    if config.mode != "enforce":
        try:
            order = market_order()
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
        maker_price = float(levels[0][0])
        maker_order = exchange.create_order(
            symbol,
            "limit",
            side,
            amount,
            maker_price,
            params={
                "postOnly": True,
                "clientOrderId": client_order_id,
                "externalOid": client_order_id,
            },
        )
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
        order_id = maker_order.get("id")
        latest = exchange.fetch_order(order_id, symbol)
        latest_status = _order_status(latest, amount)
        if latest_status == "FILLED":
            if status == "OPEN":
                journal.transition(
                    intent_id,
                    "FILLED",
                    exchange_order_id=latest.get("id"),
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
        canceled_status = str(canceled.get("status", "")).strip().lower()
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
        journal.transition(intent_id, "FALLBACK_SUBMITTING")
        fallback = market_order()
        fallback_filled = _number(fallback.get("filled"))
        fallback_cost = _number(fallback.get("cost"))
        total_filled = filled + fallback_filled
        total_cost = _number(canceled.get("cost")) + fallback_cost
        if total_filled < amount * (1.0 - 1e-9):
            raise RuntimeError("market fallback did not fill verified residual")
        journal.transition(
            intent_id,
            "FILLED",
            exchange_order_id=fallback.get("id"),
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
        fee = order.get("fee") or {}
        fee_cost = _number(fee.get("cost")) if isinstance(fee, dict) else 0.0
        notional = _number(order.get("cost"))
        fee_rate = fee_cost / notional if notional else 0.001
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
    except Exception:
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
        order = _find_order_by_client_id(
            exchange,
            intent["symbol"],
            intent["client_order_id"],
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
        status = _order_status(order, float(intent["target_amount"]))
        try:
            if status == "FILLED":
                if current == "PREPARED":
                    transition_order_intent(intent_id, "SUBMITTING")
                transition_order_intent(
                    intent_id,
                    "FILLED",
                    exchange_order_id=order.get("id"),
                    filled_amount=_number(order.get("filled")),
                    filled_notional=_number(order.get("cost")),
                )
                transition_order_intent(intent_id, "FINALIZED")
                continue
        except ValueError:
            pass
        unresolved.append(intent)
    return unresolved
