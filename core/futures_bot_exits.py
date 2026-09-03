"""
core/futures_bot_exits.py  Monitor thread + exit logic for FuturesBot.

Runs at MONITOR_INTERVAL (~20s) and handles ONLY exits:
  Per-position price fetch (cached + bounded thread pool)
  Highest-favorable-move tracking (LONG up, SHORT down)
  Liquidation buffer check (priority 1  relative to initial distance)
  Breakeven activation with fee-buffer
  Partial take-profit at ACTIVATION_PROFIT
  Full exit: liq protection / break-even stop / trailing stop / SL

LIVE state for dashboard is upserted on every tick (futures_state table).
"""
from __future__ import annotations

from core.logger import _date as _utc_now_str

import inspect
import math
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

from bot_utils import (
    budget_exhausted,
    calc_liquidation_price,
    distance_to_liquidation_pct,
    liq_buffer_consumed_pct,
    calc_unrealized_pnl,
    price_move_pct,
    is_new_high,
    trailing_stop_hit,
    breakeven_stop_hit,
    fee_buffered_breakeven,
    get_exchange_liq_price,
    get_maintenance_margin_rate,
    safe_proportional_fee,
    safe_funding_scale,
    safe_remaining_funding,
)
from bot_utils.api_budget import try_consume_api_call
from bot_utils.order_utils import order_id_text_or_none


def _fetch_positions_compat_once(exchange, symbol_full: str):
    """Call one adapter shape without retrying a remote ``TypeError``."""
    fetch_positions = exchange.fetch_positions
    try:
        parameters = inspect.signature(fetch_positions).parameters.values()
        needs_symbols = any(
            parameter.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
            and parameter.default is inspect.Parameter.empty
            for parameter in parameters
        )
    except (TypeError, ValueError):
        needs_symbols = False
    if needs_symbols:
        return fetch_positions([symbol_full])
    return fetch_positions()


_PARTIAL_EXIT_CLIENT_ID = "partial_exit_client_order_id"
_PARTIAL_EXIT_AMOUNT = "partial_exit_requested_amount"
_PARTIAL_EXIT_SIDE = "partial_exit_position_side"
_PARTIAL_EXIT_MODE = "partial_exit_mode"
_PARTIAL_EXIT_OBSERVED_FILLED = "partial_exit_observed_filled"
_PARTIAL_EXIT_CREATED_AT = "partial_exit_created_at"
_FULL_EXIT_CLIENT_ID = "full_exit_client_order_id"
_FULL_EXIT_AMOUNT = "full_exit_requested_amount"
_FULL_EXIT_SIDE = "full_exit_position_side"
_FULL_EXIT_MODE = "full_exit_mode"
_FULL_EXIT_BASE_FILLED = "full_exit_base_filled_amount"
_FULL_EXIT_CREATED_AT = "full_exit_created_at"


def _futures_order_lookup_since_ms(value) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)):
            seconds = float(value)
            if seconds > 100_000_000_000:
                seconds /= 1000.0
        else:
            text = str(value).strip().replace("Z", "+00:00")
            if not text:
                return None
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                parsed = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            seconds = parsed.timestamp()
        if not math.isfinite(seconds) or seconds <= 0:
            return None
        milliseconds = int(seconds * 1000)
        if milliseconds > int(time.time() * 1000) + 60_000:
            return None
        return milliseconds
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _futures_partial_exit_lookup_since_ms(row: dict) -> int | None:
    if not isinstance(row, dict):
        return None
    return (
        _futures_order_lookup_since_ms(row.get(_PARTIAL_EXIT_CREATED_AT))
        or _futures_order_lookup_since_ms(row.get("buy_time"))
    )


def _futures_full_exit_lookup_since_ms(row: dict) -> int | None:
    if not isinstance(row, dict):
        return None
    return (
        _futures_order_lookup_since_ms(row.get(_FULL_EXIT_CREATED_AT))
        or _futures_order_lookup_since_ms(row.get("buy_time"))
    )


def _has_futures_partial_exit_intent(row: dict) -> bool:
    return isinstance(row, dict) and any(
        row.get(key) not in (None, "")
        for key in (
            _PARTIAL_EXIT_CLIENT_ID,
            _PARTIAL_EXIT_AMOUNT,
            _PARTIAL_EXIT_SIDE,
            _PARTIAL_EXIT_MODE,
            _PARTIAL_EXIT_OBSERVED_FILLED,
            _PARTIAL_EXIT_CREATED_AT,
        )
    )


def _futures_exit_intent_schema_status(
    row: dict,
    leg: str,
) -> tuple[bool, bool]:
    """Return ``(present, valid)`` for one durable futures exit intent."""
    if not isinstance(row, dict) or leg not in {"partial", "full"}:
        return False, False
    if leg == "partial":
        client_id_key = _PARTIAL_EXIT_CLIENT_ID
        amount_key = _PARTIAL_EXIT_AMOUNT
        side_key = _PARTIAL_EXIT_SIDE
        mode_key = _PARTIAL_EXIT_MODE
        progress_key = _PARTIAL_EXIT_OBSERVED_FILLED
        created_at_key = _PARTIAL_EXIT_CREATED_AT
    else:
        client_id_key = _FULL_EXIT_CLIENT_ID
        amount_key = _FULL_EXIT_AMOUNT
        side_key = _FULL_EXIT_SIDE
        mode_key = _FULL_EXIT_MODE
        progress_key = _FULL_EXIT_BASE_FILLED
        created_at_key = _FULL_EXIT_CREATED_AT
    fields = (
        client_id_key,
        amount_key,
        side_key,
        mode_key,
        progress_key,
        created_at_key,
    )
    present = any(row.get(key) not in (None, "") for key in fields)
    if not present:
        return False, True

    client_id = order_id_text_or_none(row.get(client_id_key))
    if not client_id or not client_id.isascii() or len(client_id) > 32:
        return True, False
    raw_amount = row.get(amount_key)
    raw_progress = row.get(progress_key)
    if isinstance(raw_amount, bool) or isinstance(raw_progress, bool):
        return True, False
    try:
        amount = float(raw_amount)
        progress = float(raw_progress)
    except (TypeError, ValueError, OverflowError):
        return True, False
    if (
        not math.isfinite(amount)
        or amount <= 0.0
        or not math.isfinite(progress)
        or progress < 0.0
    ):
        return True, False
    tolerance = max(1e-12, max(amount, progress) * 1e-9)
    if leg == "partial" and progress > amount + tolerance:
        return True, False

    raw_side = row.get(side_key)
    side = raw_side.strip().upper() if isinstance(raw_side, str) else ""
    if (
        side not in {"LONG", "SHORT"}
        or side != row.get("position_type")
        or row.get(mode_key) != "LIVE"
    ):
        return True, False
    created_at = row.get(created_at_key)
    if (
        created_at not in (None, "")
        and _futures_order_lookup_since_ms(created_at) is None
    ):
        return True, False
    if leg == "full":
        try:
            from bot_utils.close_fragments import pending_close_values

            current_filled, _price, _fee, _order_id = pending_close_values(row)
        except Exception:
            return True, False
        if current_filled + tolerance < progress:
            return True, False
    return True, True


def _futures_partial_exit_order_is_terminal(
    order: dict,
    *,
    requested_amount: float,
    filled_amount: float,
) -> bool:
    if not isinstance(order, dict):
        return False
    raw_status = order.get("status")
    status = raw_status.strip().lower() if isinstance(raw_status, str) else ""
    if status in {
        "closed",
        "filled",
        "canceled",
        "cancelled",
        "expired",
        "rejected",
    }:
        return True
    tolerance = max(1e-12, requested_amount * 1e-9)
    return filled_amount >= requested_amount - tolerance


def _ensure_futures_partial_exit_intent(
    state,
    sym: str,
    row: dict,
    *,
    requested_amount: float,
    position_side: str,
    bot_name: str,
) -> tuple[str, float, bool]:
    """Return a durable partial-close intent and whether this call created it."""
    existing = order_id_text_or_none(row.get(_PARTIAL_EXIT_CLIENT_ID))
    raw_amount = row.get(_PARTIAL_EXIT_AMOUNT)
    raw_side = row.get(_PARTIAL_EXIT_SIDE)
    raw_mode = row.get(_PARTIAL_EXIT_MODE)
    raw_observed_filled = row.get(_PARTIAL_EXIT_OBSERVED_FILLED)
    raw_created_at = row.get(_PARTIAL_EXIT_CREATED_AT)
    if existing:
        if not existing.isascii() or len(existing) > 32:
            raise RuntimeError(f"invalid pending partial-exit client id for {sym}")
        if isinstance(raw_amount, bool):
            raise RuntimeError(f"invalid pending partial-exit amount for {sym}")
        try:
            stored_amount = float(raw_amount)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"invalid pending partial-exit amount for {sym}"
            ) from exc
        if not math.isfinite(stored_amount) or stored_amount <= 0:
            raise RuntimeError(f"invalid pending partial-exit amount for {sym}")
        stored_side = raw_side.strip().upper() if isinstance(raw_side, str) else ""
        if stored_side not in {"LONG", "SHORT"} or stored_side != position_side:
            raise RuntimeError(f"invalid pending partial-exit side for {sym}")
        if raw_mode != "LIVE":
            raise RuntimeError(f"invalid pending partial-exit mode for {sym}")
        if (
            raw_created_at not in (None, "")
            and _futures_order_lookup_since_ms(raw_created_at) is None
        ):
            raise RuntimeError(
                f"invalid pending partial-exit creation time for {sym}"
            )
        if isinstance(raw_observed_filled, bool):
            raise RuntimeError(
                f"invalid pending partial-exit observed fill for {sym}"
            )
        try:
            observed_filled = float(raw_observed_filled)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"invalid pending partial-exit observed fill for {sym}"
            ) from exc
        tolerance = max(1e-12, stored_amount * 1e-9)
        if (
            not math.isfinite(observed_filled)
            or observed_filled < 0
            or observed_filled > stored_amount + tolerance
        ):
            raise RuntimeError(
                f"invalid pending partial-exit observed fill for {sym}"
            )
        return existing, stored_amount, False

    if any(
        value not in (None, "")
        for value in (
            row.get(_PARTIAL_EXIT_CLIENT_ID),
            raw_amount,
            raw_side,
            raw_mode,
            raw_observed_filled,
            raw_created_at,
        )
    ):
        raise RuntimeError(f"incomplete pending partial-exit intent for {sym}")
    if (
        isinstance(requested_amount, bool)
        or not math.isfinite(requested_amount)
        or requested_amount <= 0
        or position_side not in {"LONG", "SHORT"}
    ):
        raise RuntimeError(f"invalid new partial-exit intent for {sym}")

    from uuid import uuid4
    from trading.execution_quality import make_client_order_id

    entry_id = order_id_text_or_none(row.get("entry_id")) or "legacy"
    client_order_id = make_client_order_id(
        f"{entry_id}:{uuid4().hex}",
        f"{bot_name}:{sym}:partial",
        prefix="fx",
    )
    updates = {
        _PARTIAL_EXIT_CLIENT_ID: client_order_id,
        _PARTIAL_EXIT_AMOUNT: requested_amount,
        _PARTIAL_EXIT_SIDE: position_side,
        _PARTIAL_EXIT_MODE: "LIVE",
        _PARTIAL_EXIT_OBSERVED_FILLED: 0.0,
        _PARTIAL_EXIT_CREATED_AT: _utc_now_str(),
    }
    persisted = state.update_many(sym, updates)
    if persisted is not None and persisted is not True:
        raise RuntimeError(
            f"cannot persist partial-exit intent before live order for {sym}"
        )
    row.update(updates)
    return client_order_id, requested_amount, True


def _clear_futures_partial_exit_intent(state, sym: str, row: dict) -> bool:
    updates = {
        _PARTIAL_EXIT_CLIENT_ID: None,
        _PARTIAL_EXIT_AMOUNT: None,
        _PARTIAL_EXIT_SIDE: None,
        _PARTIAL_EXIT_MODE: None,
        _PARTIAL_EXIT_OBSERVED_FILLED: None,
        _PARTIAL_EXIT_CREATED_AT: None,
    }
    persisted = state.update_many(sym, updates)
    if persisted is not None and persisted is not True:
        return False
    row.update(updates)
    return True


def _ensure_futures_full_exit_intent(
    state,
    sym: str,
    row: dict,
    *,
    requested_amount: float,
    position_side: str,
    bot_name: str,
) -> tuple[str, float, bool]:
    """Return one durable full-close intent and whether this call created it."""
    existing = order_id_text_or_none(row.get(_FULL_EXIT_CLIENT_ID))
    raw_amount = row.get(_FULL_EXIT_AMOUNT)
    raw_side = row.get(_FULL_EXIT_SIDE)
    raw_mode = row.get(_FULL_EXIT_MODE)
    raw_base_filled = row.get(_FULL_EXIT_BASE_FILLED)
    raw_created_at = row.get(_FULL_EXIT_CREATED_AT)
    if existing:
        if not existing.isascii() or len(existing) > 32:
            raise RuntimeError(f"invalid pending full-exit client id for {sym}")
        if isinstance(raw_amount, bool):
            raise RuntimeError(f"invalid pending full-exit amount for {sym}")
        try:
            stored_amount = float(raw_amount)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"invalid pending full-exit amount for {sym}"
            ) from exc
        if not math.isfinite(stored_amount) or stored_amount <= 0:
            raise RuntimeError(f"invalid pending full-exit amount for {sym}")
        stored_side = raw_side.strip().upper() if isinstance(raw_side, str) else ""
        if stored_side not in {"LONG", "SHORT"} or stored_side != position_side:
            raise RuntimeError(f"invalid pending full-exit side for {sym}")
        if raw_mode != "LIVE":
            raise RuntimeError(f"invalid pending full-exit mode for {sym}")
        if (
            raw_created_at not in (None, "")
            and _futures_order_lookup_since_ms(raw_created_at) is None
        ):
            raise RuntimeError(
                f"invalid pending full-exit creation time for {sym}"
            )
        if isinstance(raw_base_filled, bool):
            raise RuntimeError(
                f"invalid pending full-exit fill baseline for {sym}"
            )
        try:
            base_filled = float(raw_base_filled)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"invalid pending full-exit fill baseline for {sym}"
            ) from exc
        if not math.isfinite(base_filled) or base_filled < 0:
            raise RuntimeError(
                f"invalid pending full-exit fill baseline for {sym}"
            )
        try:
            from bot_utils.close_fragments import pending_close_values

            current_filled, _price, _fee, _order_id = pending_close_values(row)
        except Exception as exc:
            raise RuntimeError(
                f"invalid pending full-exit fill evidence for {sym}"
            ) from exc
        tolerance = max(1e-12, max(stored_amount, base_filled) * 1e-9)
        if current_filled + tolerance < base_filled:
            raise RuntimeError(
                f"pending full-exit fill baseline exceeds evidence for {sym}"
            )
        durable_fields = {
            _FULL_EXIT_CLIENT_ID: existing,
            _FULL_EXIT_AMOUNT: stored_amount,
            _FULL_EXIT_SIDE: stored_side,
            _FULL_EXIT_MODE: "LIVE",
            _FULL_EXIT_BASE_FILLED: base_filled,
        }
        if raw_created_at not in (None, ""):
            durable_fields[_FULL_EXIT_CREATED_AT] = raw_created_at
        try:
            persisted = state.update_many(sym, durable_fields)
        except Exception as exc:
            raise RuntimeError(
                f"cannot confirm durable full-exit intent for {sym}"
            ) from exc
        if persisted is not None and persisted is not True:
            raise RuntimeError(
                f"cannot confirm durable full-exit intent for {sym}"
            )
        row.update(durable_fields)
        return existing, stored_amount, False

    if any(
        value not in (None, "")
        for value in (
            row.get(_FULL_EXIT_CLIENT_ID),
            raw_amount,
            raw_side,
            raw_mode,
            raw_base_filled,
            raw_created_at,
        )
    ):
        raise RuntimeError(f"incomplete pending full-exit intent for {sym}")
    if (
        isinstance(requested_amount, bool)
        or not math.isfinite(requested_amount)
        or requested_amount <= 0
        or position_side not in {"LONG", "SHORT"}
    ):
        raise RuntimeError(f"invalid new full-exit intent for {sym}")

    from uuid import uuid4
    from trading.execution_quality import make_client_order_id

    entry_id = order_id_text_or_none(row.get("entry_id")) or "legacy"
    try:
        from bot_utils.close_fragments import pending_close_values

        base_filled, _price, _fee, _order_id = pending_close_values(row)
    except Exception as exc:
        raise RuntimeError(
            f"invalid new full-exit fill baseline for {sym}"
        ) from exc
    client_order_id = make_client_order_id(
        f"{entry_id}:{uuid4().hex}",
        f"{bot_name}:{sym}:full",
        prefix="fx",
    )
    updates = {
        _FULL_EXIT_CLIENT_ID: client_order_id,
        _FULL_EXIT_AMOUNT: requested_amount,
        _FULL_EXIT_SIDE: position_side,
        _FULL_EXIT_MODE: "LIVE",
        _FULL_EXIT_BASE_FILLED: base_filled,
        _FULL_EXIT_CREATED_AT: _utc_now_str(),
    }
    persisted = state.update_many(sym, updates)
    if persisted is not None and persisted is not True:
        raise RuntimeError(
            f"cannot persist full-exit intent before live order for {sym}"
        )
    row.update(updates)
    return client_order_id, requested_amount, True


def _futures_full_exit_clear_fields() -> dict:
    return {
        _FULL_EXIT_CLIENT_ID: None,
        _FULL_EXIT_AMOUNT: None,
        _FULL_EXIT_SIDE: None,
        _FULL_EXIT_MODE: None,
        _FULL_EXIT_BASE_FILLED: None,
        _FULL_EXIT_CREATED_AT: None,
    }


def _clear_futures_full_exit_intent(state, sym: str, row: dict) -> bool:
    updates = _futures_full_exit_clear_fields()
    previous = {
        _FULL_EXIT_CLIENT_ID: row.get(_FULL_EXIT_CLIENT_ID),
        _FULL_EXIT_AMOUNT: row.get(_FULL_EXIT_AMOUNT),
        _FULL_EXIT_SIDE: row.get(_FULL_EXIT_SIDE),
        _FULL_EXIT_MODE: row.get(_FULL_EXIT_MODE),
        _FULL_EXIT_BASE_FILLED: row.get(_FULL_EXIT_BASE_FILLED),
        _FULL_EXIT_CREATED_AT: row.get(_FULL_EXIT_CREATED_AT),
    }
    try:
        persisted = state.update_many(sym, updates)
    except Exception:
        persisted = False
    if persisted is not None and persisted is not True:
        try:
            state.update_many(sym, previous)
        except Exception:
            pass
        row.update(previous)
        return False
    row.update(updates)
    return True


def _recover_or_submit_futures_full_exit(
    bot,
    sym: str,
    row: dict,
    *,
    symbol_full: str,
    requested_amount: float,
    position_side: str,
    close_side: str,
    margin_mode: str,
    leverage: int,
    action_label: str,
    log_event,
    log_struct=None,
):
    """Recover a durable full-close intent or submit it exactly once."""
    client_order_id, close_amount, created_intent = (
        _ensure_futures_full_exit_intent(
            bot.state,
            sym,
            row,
            requested_amount=requested_amount,
            position_side=position_side,
            bot_name=bot.BOT_NAME,
        )
    )
    intent_base_filled = FuturesExitsMixin._safe_nonnegative_amount(
        row.get(_FULL_EXIT_BASE_FILLED)
    )
    from config.exchange_config import reduce_only_params
    from bot_utils.futures_order import (
        _exchange_id,
        _find_order_by_client_id,
        _order_confirmed_terminal_zero_fill,
        _order_landed,
        _requested_position_side,
        classify_order_state,
        is_terminal_order_state,
    )

    order_params = reduce_only_params(
        position_side=("long" if position_side == "LONG" else "short"),
        margin_mode=margin_mode,
        leverage=leverage,
        client_order_id=client_order_id,
    )
    requested_position_side = _requested_position_side(order_params)
    order = None
    if not created_intent:
        lookup_status = {}
        order = _find_order_by_client_id(
            bot.ex,
            symbol_full,
            client_order_id,
            log_event=log_event,
            lookup_status=lookup_status,
            expected_amount=close_amount,
            expected_side=close_side,
            expected_position_side=requested_position_side,
            exchange_id=_exchange_id(bot.ex),
            expected_reduce_only=True,
            lookup_since_ms=_futures_full_exit_lookup_since_ms(row),
        )
        recovery_conflict = (
            isinstance(order, dict)
            and order.get("_bot_recovery_conflict") is True
        )
        if lookup_status.get("unavailable") or recovery_conflict:
            log_event(
                f"{action_label}: pending full-close recovery unavailable "
                "or conflicting - additional submit remains blocked",
                "ERROR" if recovery_conflict else "WARN",
            )
            return None, close_amount, False
        if order is None:
            lookup_since_ms = _futures_full_exit_lookup_since_ms(row)
            if (
                lookup_since_ms is not None
                and lookup_status.get("complete_negative") is True
            ):
                if not _clear_futures_full_exit_intent(bot.state, sym, row):
                    log_event(
                        f"{action_label}: complete-negative intent could not "
                        "be cleared durably",
                        "ERROR",
                    )
                return None, close_amount, False
            log_event(
                f"{action_label}: pending full-close order is not proven "
                "absent - additional submit remains blocked",
                "WARN",
            )
            return None, close_amount, False
        if _order_confirmed_terminal_zero_fill(order):
            try:
                from bot_utils.close_fragments import pending_close_values

                prior_filled, _price, _fee, _oid = pending_close_values(row)
            except Exception:
                log_event(
                    f"{action_label}: terminal zero-fill conflicts with "
                    "invalid pending close evidence",
                    "ERROR",
                )
                return None, close_amount, False
            tolerance = max(1e-12, close_amount * 1e-9)
            if abs(prior_filled - intent_base_filled) > tolerance:
                log_event(
                    f"{action_label}: terminal zero-fill conflicts with "
                    "fill evidence recorded after this intent was created",
                    "ERROR",
                )
                return None, close_amount, False
            if not _clear_futures_full_exit_intent(
                bot.state, sym, row
            ):
                log_event(
                    f"{action_label}: terminal zero-fill intent could not "
                    "be cleared durably",
                    "ERROR",
                )
            return None, close_amount, False
        if order is not None and not _order_landed(order):
            log_event(
                f"{action_label}: pending full-close order is not proven "
                "landed - additional submit remains blocked",
                "WARN",
            )
            return None, close_amount, False

    if order is None:
        from bot_utils import create_order_with_retry
        from bot_utils.trade_state import registry_order_guard

        with registry_order_guard(
            bot.state, sym, row
        ) as ownership_live:
            if not isinstance(ownership_live, dict):
                return None, close_amount, False
            if ownership_live.get("claim_conflict"):
                log_event(
                    f"{action_label}: close blocked by registry claim conflict",
                    "ERROR",
                )
                return None, close_amount, False
            live_client_order_id = order_id_text_or_none(
                ownership_live.get(_FULL_EXIT_CLIENT_ID)
            )
            live_amount = FuturesExitsMixin._safe_positive_float(
                ownership_live.get(_FULL_EXIT_AMOUNT), 0.0
            )
            live_base_filled = FuturesExitsMixin._safe_nonnegative_amount(
                ownership_live.get(_FULL_EXIT_BASE_FILLED)
            )
            tolerance = max(1e-12, close_amount * 1e-9)
            if (
                live_client_order_id != client_order_id
                or abs(live_amount - close_amount) > tolerance
                or ownership_live.get(_FULL_EXIT_SIDE) != position_side
                or ownership_live.get(_FULL_EXIT_MODE) != "LIVE"
                or abs(live_base_filled - intent_base_filled) > tolerance
            ):
                log_event(
                    f"{action_label}: durable full-close intent changed "
                    "before submit",
                    "ERROR",
                )
                return None, close_amount, False
            order = create_order_with_retry(
                bot.ex,
                symbol_full,
                close_side,
                close_amount,
                params=order_params,
                shutdown_event=bot._shutdown_event,
                max_attempts=1,
                action_label=action_label,
                log_event=log_event,
                log_struct=log_struct,
            )

    terminal = is_terminal_order_state(classify_order_state(order))
    return order, close_amount, terminal


class FuturesExitsMixin:
    _MARK_FALLBACK_CACHE_TTL_SEC = 2.0

    def _record_futures_exit_shadow(
        self, sym: str, d: dict, *, move_pct: float,
        mfe_pct: float, mae_pct: float, now=None,
    ) -> None:
        """Persist first-hit counterfactual telemetry without changing exits."""
        try:
            from core.logger import log_struct
            from trading.futures_exit_shadow import evaluate_exit_shadow_rules

            triggers = evaluate_exit_shadow_rules(
                buy_time=d.get("buy_time"),
                move_pct=move_pct,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                now=now,
            )
            raw_seen = d.get("exit_shadow_triggered_rules", [])
            if isinstance(raw_seen, (list, tuple, set)):
                seen = {str(item) for item in raw_seen if str(item)}
            elif isinstance(raw_seen, str):
                seen = {item for item in raw_seen.split(",") if item}
            else:
                seen = set()
            new_triggers = [
                item for item in triggers if str(item.get("rule")) not in seen
            ]
            if not new_triggers:
                return

            margin = self._safe_positive_float(d.get("invested_usdt"), 0.0)
            leverage = self._safe_positive_float(d.get("leverage"), 1.0)
            for trigger in new_triggers:
                trigger_move = self._safe_finite_float(
                    trigger.get("trigger_move_pct"), move_pct)
                log_struct(
                    "futures_exit_shadow",
                    bot=self.BOT_NAME,
                    symbol=sym,
                    mode="SIM" if self.simulation else "LIVE",
                    entry_id=d.get("entry_id", ""),
                    direction=d.get("position_type", ""),
                    entry_quality_score=d.get("entry_quality_score"),
                    entry_quality_label=d.get("entry_quality_label"),
                    gross_pnl_usdt=(
                        margin * leverage * trigger_move / 100.0),
                    actual_initial_stop_pct=self.C(
                        "INITIAL_STOP_LOSS", -100.0),
                    actual_breakeven_trigger_pct=self.C(
                        "BREAKEVEN_TRIGGER", 0.0),
                    actual_activation_profit_pct=self.C(
                        "ACTIVATION_PROFIT", 0.0),
                    actual_trailing_distance_pct=self.C(
                        "TRAILING_DISTANCE", 0.0),
                    **trigger,
                )
                seen.add(str(trigger["rule"]))
            persisted = sorted(seen)
            persist_result = self.state.update_many(
                sym, {"exit_shadow_triggered_rules": persisted})
            d["exit_shadow_triggered_rules"] = persisted
            if persist_result is not None and persist_result is not True:
                raise RuntimeError("state update returned non-success")
        except Exception as exc:
            # Observability must never disturb exit supervision. Log once per
            # process so a programming error remains visible without spamming.
            if not getattr(self, "_exit_shadow_error_logged", False):
                self._exit_shadow_error_logged = True
                try:
                    self._log_error(f"futures exit shadow {sym}", exc)
                except Exception:
                    pass

    def _record_peak_trail_execution(
        self, sym: str, d: dict, *, reason: str,
        decision_price: float, fill_price: float,
        decision_move_pct: float, fill_move_pct: float,
        mfe_pct: float, latency_ms: float, profit_usdt: float,
        fees_usdt: float, funding_paid: float,
        accounting_saved: bool, exchange_order_id=None,
    ) -> None:
        """Persist verified peak-exit execution quality without affecting close."""
        if reason != "Pre-Activation Giveback Stop":
            return
        try:
            from core.logger import log_struct
            from trading.futures_peak_trail import build_execution_metrics

            metrics = build_execution_metrics(
                position_type=d.get("position_type"),
                decision_price=decision_price,
                fill_price=fill_price,
                decision_move_pct=decision_move_pct,
                fill_move_pct=fill_move_pct,
                mfe_pct=mfe_pct,
                latency_ms=latency_ms,
            )
            log_struct(
                "futures_peak_trail_execution",
                bot=self.BOT_NAME,
                symbol=sym,
                mode="SIM" if self.simulation else "LIVE",
                entry_id=d.get("entry_id", ""),
                direction=d.get("position_type", ""),
                reason=reason,
                accounting_saved=bool(accounting_saved),
                exchange_order_id=exchange_order_id,
                profit_usdt=round(float(profit_usdt), 6),
                fees_usdt=round(float(fees_usdt), 6),
                funding_paid=round(float(funding_paid), 6),
                configured_activation_mfe_pct=self.C(
                    "PRE_ACTIVATION_MIN_MFE_PCT", 1.5),
                configured_giveback_pct=self.C(
                    "PRE_ACTIVATION_GIVEBACK_PCT", 0.75),
                **metrics,
            )
        except Exception as exc:
            if not getattr(self, "_peak_trail_execution_error_logged", False):
                self._peak_trail_execution_error_logged = True
                try:
                    self._log_error(f"peak trail execution telemetry {sym}", exc)
                except Exception:
                    pass

    def _prepare_peak_trail_decision(
        self, sym: str, d: dict, *, reason: str,
        decision_price: float, decision_move_pct: float, mfe_pct: float,
    ) -> dict:
        """Keep the first peak-exit decision stable across close retries."""
        if reason != "Pre-Activation Giveback Stop":
            return d

        decision_at = FuturesExitsMixin._safe_positive_float(
            d.get("peak_trail_decision_at"), 0.0)
        stored_price = FuturesExitsMixin._safe_positive_price(
            d.get("peak_trail_decision_price"))
        stored_move = FuturesExitsMixin._safe_finite_float(
            d.get("peak_trail_decision_move_pct"), None)
        stored_mfe = FuturesExitsMixin._safe_finite_float(
            d.get("peak_trail_decision_mfe_pct"), None)
        if (decision_at > 0.0 and stored_price > 0.0
                and stored_move is not None and stored_mfe is not None):
            return d

        fields = {
            "peak_trail_decision_at": time.time(),
            "peak_trail_decision_price": decision_price,
            "peak_trail_decision_move_pct": decision_move_pct,
            "peak_trail_decision_mfe_pct": mfe_pct,
        }
        prepared = dict(d)
        prepared.update(fields)
        try:
            persisted = self.state.update_many(sym, fields)
            if persisted is not None and persisted is not True:
                raise RuntimeError("state update returned False")
        except Exception as exc:
            if not getattr(self, "_peak_trail_decision_error_logged", False):
                self._peak_trail_decision_error_logged = True
                try:
                    self._log_error(
                        f"persist peak trail decision {sym}", exc)
                except Exception:
                    pass
        return prepared

    @staticmethod
    def _safe_positive_price(value) -> float:
        if isinstance(value, bool):
            return 0.0
        try:
            price = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        return price if math.isfinite(price) and price > 0 else 0.0

    @staticmethod
    def _safe_nonnegative_amount(value) -> float:
        if isinstance(value, bool):
            return 0.0
        try:
            amount = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        return amount if math.isfinite(amount) and amount >= 0 else 0.0

    @staticmethod
    def _safe_finite_float(value, default: float = 0.0) -> float:
        if isinstance(value, bool):
            return default
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return parsed if math.isfinite(parsed) else default

    @classmethod
    def _safe_positive_float(cls, value, default: float = 0.0) -> float:
        parsed = cls._safe_finite_float(value, default)
        return parsed if parsed > 0 else default

    @classmethod
    def _reconstructed_margin_usdt(
        cls, *, amount, contract_size, entry_price, leverage
    ) -> float | None:
        amount_value = cls._safe_positive_float(amount, 0.0)
        contract_value = cls._safe_positive_float(contract_size, 0.0)
        entry_value = cls._safe_positive_float(entry_price, 0.0)
        leverage_value = cls._safe_positive_float(leverage, 0.0)
        if not all(
            value > 0.0
            for value in (
                amount_value,
                contract_value,
                entry_value,
                leverage_value,
            )
        ):
            return None
        try:
            margin = amount_value * contract_value * entry_value / leverage_value
        except OverflowError:
            return None
        return margin if math.isfinite(margin) and margin > 0.0 else None

    @staticmethod
    def _precision_amount_or_none(value):
        if isinstance(value, bool):
            return None
        try:
            amount = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return amount if math.isfinite(amount) else None

    @classmethod
    def _order_fill_price(cls, order: dict) -> float:
        if not isinstance(order, dict):
            return 0.0
        for key in ("average", "price"):
            price = cls._safe_positive_price(order.get(key))
            if price > 0:
                return price
        return 0.0

    @classmethod
    def _ticker_price(cls, ticker: dict) -> float:
        if not isinstance(ticker, dict):
            return 0.0
        price = cls._safe_positive_price(ticker.get("last"))
        if price <= 0:
            price = cls._safe_positive_price(ticker.get("close"))
        return price

    def _retry_pending_partial_accounting(self, sym: str, d: dict) -> None:
        from bot_utils.trade_state import (
            normalize_pending_accounting_items,
            validated_pending_partial_accounting_item,
        )
        from core.futures_bot_reconcile import (
            _defer_offline_accounting_retry,
            _offline_accounting_retry_due,
        )
        if not normalize_pending_accounting_items(
            d.get("accounting_pending_partials")
        ):
            return
        from core.database import save_trade_db
        from core.logger import log_event
        from core.symbol_locks import close_lock

        with close_lock(
            sym,
            timeout=2.0,
            bot_name=getattr(self, "BOT_NAME", "FUTURES"),
        ) as got:
            if not got:
                return
            live = self.state.get(sym)
            if not isinstance(live, dict):
                return
            pending = normalize_pending_accounting_items(
                live.get("accounting_pending_partials"))
            if not pending:
                return
            if not _offline_accounting_retry_due(live):
                return
            try:
                durable = self.state.update_many(
                    sym, {"accounting_pending_partials": pending}
                )
            except Exception as exc:
                self._log_error(
                    f"futures partial accounting write-ahead {sym}", exc
                )
                _defer_offline_accounting_retry(self, sym, live)
                return
            if durable is not None and durable is not True:
                retry_delay = _defer_offline_accounting_retry(
                    self, sym, live
                )
                log_event(
                    f"{sym}: futures partial accounting state is not durable "
                    f"- DB retry deferred for {retry_delay:.0f}s",
                    "ERROR",
                )
                return
            remaining = []
            for item in pending:
                try:
                    retry_item = validated_pending_partial_accounting_item(
                        item,
                        symbol=sym,
                        bot_name=self.BOT_NAME,
                        mode_is_sim=self.simulation,
                        is_futures=True,
                    )
                    saved = save_trade_db(**retry_item) is True
                except Exception as exc:
                    saved = False
                    self._log_error(f"futures partial accounting retry {sym}", exc)
                if not saved:
                    remaining.append(item)
            try:
                cleared = self.state.update(
                    sym, "accounting_pending_partials", remaining
                )
            except Exception as exc:
                cleared = False
                self._log_error(
                    f"futures partial accounting clear {sym}", exc
                )
            if cleared is not None and cleared is not True:
                retry_delay = _defer_offline_accounting_retry(
                    self, sym, live
                )
                log_event(
                    f"{sym}: futures partial accounting was booked but its "
                    f"durable pending marker could not be updated; idempotent "
                    f"retry retained for {retry_delay:.0f}s",
                    "ERROR",
                )
                return
            if remaining:
                retry_delay = _defer_offline_accounting_retry(
                    self, sym, live
                )
                log_event(
                    f"{sym}: {len(remaining)} futures partial accounting "
                    f"event(s) still pending; retry in {retry_delay:.0f}s",
                    "WARN",
                )
            else:
                if (
                    "accounting_retry_attempts" in live
                    or "accounting_retry_next_at" in live
                ):
                    try:
                        self.state.update_many(
                            sym,
                            {
                                "accounting_retry_attempts": 0,
                                "accounting_retry_next_at": 0.0,
                            },
                        )
                    except Exception as exc:
                        self._log_error(
                            f"futures partial accounting backoff reset {sym}",
                            exc,
                        )
                log_event(
                    f"{sym}: pending futures partial accounting flushed",
                    "INFO",
                )

    def _cleanup_accounted_close_state(self, sym: str, d: dict) -> bool:
        """Remove dashboard/local state after PnL was already booked.

        If cleanup fails, keep a marked local row so the next monitor/reconcile
        pass retries cleanup instead of sending another reduce-only close.
        """
        from core.logger import log_event
        from core.database import remove_futures_state
        from bot_utils.trade_state import (
            remove_with_restore_fields,
            update_many_if_current,
        )
        from core.futures_bot_reconcile import (
            _defer_offline_accounting_retry,
            _offline_accounting_retry_due,
        )

        if not _offline_accounting_retry_due(d):
            return False

        restore = {
            "accounting_already_booked": True,
            "accounting_booked_sell_time": d.get("accounting_booked_sell_time")
                                      or d.get("sell_time"),
            "accounting_booked_exchange_order_id": (
                d.get("accounting_booked_exchange_order_id")
                or d.get("exchange_order_id")
            ),
            "accounting_booked_reason": (
                d.get("accounting_booked_reason")
                or d.get("accounting_pending_reason")
                or d.get("reason")
                or "Close"
            ),
        }
        try:
            remove_futures_state(
                sym, self.BOT_NAME,
                mode_is_sim=getattr(self, "simulation", None),
                expected_opened_at=d.get("buy_time"),
                expected_entry_id=d.get("entry_id"),
            )
        except Exception as exc:
            self._log_error(f"remove_futures_state accounted {sym}", exc)
            try:
                keep = dict(restore)
                keep["futures_state_cleanup_pending"] = True
                update_many_if_current(self.state, sym, keep, d)
            except Exception as state_exc:
                self._log_error(f"mark futures cleanup pending {sym}", state_exc)
            retry_delay = _defer_offline_accounting_retry(self, sym, d)
            log_event(
                f"{sym}: close already booked, but futures_state cleanup "
                f"failed; state kept for retry in {retry_delay:.0f}s",
                "WARN",
            )
            return False

        try:
            ok = remove_with_restore_fields(
                self.state, sym, restore, expected_row=d
            )
        except Exception as exc:
            ok = False
            self._log_error(f"remove accounted futures state {sym}", exc)
        if not ok:
            retry_delay = _defer_offline_accounting_retry(self, sym, d)
            log_event(
                f"{sym}: close already booked, but claim/state cleanup "
                f"failed; state kept for retry in {retry_delay:.0f}s",
                "WARN",
            )
            return False
        entry_id = d.get("entry_id")
        if isinstance(entry_id, str) and entry_id.strip():
            try:
                # Close a narrow race where an already-running old monitor
                # wrote once more after the pre-release delete. LIVE upserts
                # are claim-bound, so no old generation can reappear after
                # this pass. Legacy rows have no exact identity and must not
                # be deleted a second time by timestamp alone.
                remove_futures_state(
                    sym, self.BOT_NAME,
                    mode_is_sim=getattr(self, "simulation", None),
                    expected_opened_at=d.get("buy_time"),
                    expected_entry_id=entry_id,
                )
            except Exception as exc:
                self._log_error(f"finalize futures_state cleanup {sym}", exc)
        return True

    def _monitor_loop(self):
        """Thread body - runs forever until shutdown_event is set."""
        from core.logger import log_event

        monitor_interval = int(self.C("MONITOR_INTERVAL",
                                       self.DEFAULT_MONITOR_INTERVAL))
        log_event(
            f"Position-Monitor started (interval: {monitor_interval}s)",
            "INFO"
        )

        idle_ticks = 0
        last_idle_log = 0.0
        last_killswitch_monotonic = None
        try:
            from core.constants import KILLSWITCH_CHECK_INTERVAL_SEC as KILLSWITCH_INTERVAL
        except Exception:
            KILLSWITCH_INTERVAL = 60.0
        ks_interval = KILLSWITCH_INTERVAL    # adaptive: tightens near the limit
        consecutive_errors = 0
        MONITOR_ERR_THRESHOLD = 5

        while not self._shutdown_event.is_set():
            try:
                trades = self.state.get_all()
                now = time.time()
                now_monotonic = time.monotonic()

  # Killswitch  ADAPTIVE cadence: normally 60s, but tighten to
                # ~8s once today's loss is past 70% of the limit, so a fast 20%
                # drop can't overshoot the cap between checks. This runs even
                # with an empty book so a just-realized loss still blocks the
                # next entry.
                if (
                    last_killswitch_monotonic is None
                    or now_monotonic - last_killswitch_monotonic >= ks_interval
                ):
                    ks_ok = self._check_killswitch(trades)
                    if ks_ok is not False:
                        last_killswitch_monotonic = now_monotonic
                        try:
                            _today = getattr(self, "_ks_last_total", 0.0)
                            _maxloss = float(self.C("MAX_DAILY_LOSS", -30.0))
                            ks_interval = (
                                8.0
                                if (_maxloss < 0 and _today <= _maxloss * 0.7)
                                else KILLSWITCH_INTERVAL
                            )
                        except Exception:
                            ks_interval = KILLSWITCH_INTERVAL

                if not trades:
                    idle_ticks += 1
                    if now - last_idle_log >= 300:
                        log_event(
                            f"Monitor idle - no open positions "
                            f"(Tick #{idle_ticks})", "INFO"
                        )
                        last_idle_log = now
                    if self._shutdown_event.wait(timeout=min(monitor_interval, 10)):
                        return
                    continue

                # Non-critical scanners may exhaust their budget, but exit
                # price calls are critical and explicitly bypass that budget.
                # Never add a blind window before bot-side stops.
                if budget_exhausted():
                    last_budget_warn = float(
                        getattr(self, "_last_budget_monitor_warn", 0.0)
                    )
                    if now - last_budget_warn >= 60.0:
                        self._last_budget_monitor_warn = now
                        log_event(
                            "[Monitor] non-critical API budget exhausted - "
                            "prioritizing critical position monitoring",
                            "WARN",
                        )

                # Killswitch evaluation runs above the empty-book branch so
                # realized losses remain protected without open positions.
                # Periodic funding persistence: refresh funding_paid every ~4h
                # (between Bitget's 8h settlements) so that even an API outage
                # at the close call still has a recent value to fall back on
                # instead of the 0.0 written at entry.
                self._maybe_persist_funding_for_all(trades, now)

                # Per-position exit check
                for sym, d in trades.items():
                    if self._shutdown_event.is_set():
                        break
                    try:
                        self._check_position_exits(sym, d)
                    except Exception as e:
                        self._log_error(f"monitor {sym}", e)

                consecutive_errors = 0

            except Exception as e:
                consecutive_errors += 1
                log_event(f"Monitor tick error: {e}", "WARN")
                self._log_error("monitor loop", e)
                if consecutive_errors >= MONITOR_ERR_THRESHOLD:
                    log_event(
                        f"Monitor: {consecutive_errors} consecutive errors - "
                        f"investigation needed (check error_log.txt)",
                        "WARN"
                    )

            if self._shutdown_event.wait(timeout=monitor_interval):
                return

  #  Killswitch 

    def _check_killswitch(self, trades: dict) -> bool:
        """Two-tier daily-loss killswitch.

  SOFT tier (MAX_DAILY_LOSS): trigger SAFE_MODE  stop opening NEW
          entries but let existing positions manage themselves out. Reversible.

  HARD tier (MAX_DAILY_LOSS  MAX_DAILY_LOSS_HARD_MULT, default 1.5):
          flatten ALL open positions immediately. The soft tier only blocks
          entries, so a leveraged book can keep bleeding far past the limit
  while losers run  this tier is what makes MAX_DAILY_LOSS an actual
          cap. Fires once per process (idempotent via ``_hard_kill_fired``):
          a hard stop should require a human to look before trading resumes.
        """
        try:
            from core.database import get_today_pnl, opened_today_local
            from core.logger import log_event
            if not getattr(self, "safe_mode", None):
                return True  # bot not fully initialized yet
            retry_required = False
            pnl_info = get_today_pnl(self.BOT_NAME, mode_is_sim=self.simulation)
            today_realized = pnl_info.get("total_profit", 0.0)
            # Add unrealized from open positions (best-effort). Track two totals:
  #  unrealized_all  every open position (genuine current risk)
  #  unrealized_today  only positions OPENED TODAY (local tz)
            unrealized_all = 0.0
            unrealized_today = 0.0
            for sym, d in trades.items():
                try:
                    entry = FuturesExitsMixin._safe_positive_price(
                        d.get("buy"))
                    margin = FuturesExitsMixin._safe_positive_float(
                        d.get("invested_usdt"), 0.0)
                    lev = FuturesExitsMixin._safe_positive_float(
                        d.get("leverage"),
                        FuturesExitsMixin._safe_positive_float(
                            self.C("LEVERAGE", 3), 3.0),
                    )
                    pt = d.get("position_type", "LONG")
                    last = self._killswitch_price(sym, d, entry)
                    if entry > 0 and margin > 0 and last > 0:
                        u, _ = calc_unrealized_pnl(entry, last, margin, lev, pt)
                        unrealized_all += u
  # buy_time is UTC; the daily bucket is local  compare in
                        # one frame (opened_today_local), not a UTC-string prefix.
                        if opened_today_local(d.get("buy_time", "")):
                            unrealized_today += u
                except (TypeError, ValueError):
                    continue
  # HARD/SYSTEMIC tiers act on the FULL drawdown  an old underwater
            # leveraged position is real current risk and must be flattened. The
            # SOFT tier (only blocks NEW entries) ignores pre-today positions so
            # an old bag can't freeze fresh trades (mirrors the spot daily gate).
            total_all = today_realized + unrealized_all
            total_soft = today_realized + unrealized_today
            self._ks_last_total = total_all   # adaptive cadence tracks the hard cap
            max_loss = float(self.C("MAX_DAILY_LOSS", -30.0))

  #  SOFT tier: block new entries (reversible) 
            # Guard on is_active() so we don't re-log/re-trigger every 60s.
            if total_soft <= max_loss and not self.safe_mode.is_active():
                log_event(
                    f"KILLSWITCH (soft): today's loss {total_soft:+.2f} USDT "
                    f"<= {max_loss} USDT - SAFE_MODE, no new entries",
                    "WARN"
                )
                self.safe_mode.trigger(
                    f"daily-loss killswitch ({total_soft:+.2f} USDT)"
                )

  #  HARD tier: flatten everything (one-shot per process) 
            try:
                hard_mult = float(self.C("MAX_DAILY_LOSS_HARD_MULT", 1.5))
            except (TypeError, ValueError):
                hard_mult = 1.5
  # both operands negative  hard_limit is MORE negative than max_loss
            hard_limit = max_loss * hard_mult
            if (total_all <= hard_limit
                    and not getattr(self, "_hard_kill_fired", False)):
                log_event(
                    f"HARD KILLSWITCH: daily loss {total_all:+.2f} USDT "
                    f"<= {hard_limit:.2f} USDT - FLATTENING ALL POSITIONS NOW",
                    "ERROR"
                )
                if not self.safe_mode.is_active():
                    self.safe_mode.trigger(
                        f"HARD daily-loss killswitch ({total_all:+.2f} USDT)"
                    )
                try:
                    self._emergency_close_all(
                        reason=f"HARD daily-loss killswitch "
                               f"{total_all:+.2f} USDT"
                    )
                except Exception as _ce:
                    self._log_error("hard killswitch flatten", _ce)
                if not self.state.get_all():
                    self._hard_kill_fired = True
                else:
                    retry_required = True
                    log_event(
                        "HARD KILLSWITCH: positions remain after flatten - "
                        "will retry on next tick", "WARN"
                    )

  #  SYSTEMIC tier: flatten on a severe BTC crash (Threat 5) 
  # A market-wide crash is a clear 'get out' signal  a leveraged book
            # of correlated alts follows BTC down. Separate, configurable
            # threshold (default -12% in 4h = a REAL crash, beyond the -8% that
            # elsewhere merely pauses NEW entries). One-shot via _hard_kill_fired
            # so it requires a human before resuming. Set
            # FUT_FLATTEN_BTC_CRASH_PCT=0 to disable. NOTE: flattening into a
  # crash can deepen slippage  but an unmanaged leveraged book in a
            # -12% BTC move is the bigger risk; reduce-only market exits cap it.
            if not getattr(self, "_hard_kill_fired", False):
                try:
                    crash_pct = float(self.C("FUT_FLATTEN_BTC_CRASH_PCT", -12.0))
                except (TypeError, ValueError):
                    crash_pct = -12.0
                if crash_pct < 0:
                    try:
                        from trading.market_filters import get_btc_change
                        # closed_only: a forming-candle wick must not trip the
                        # one-shot systemic FLATTEN (needs a human to resume).
                        btc_4h = float(get_btc_change(self.ex, hours=4,
                                                      closed_only=True))
                    except Exception:
                        btc_4h = 0.0
                    if btc_4h <= crash_pct:
                        log_event(
                            f"SYSTEMIC KILLSWITCH: BTC {btc_4h:+.1f}% in 4h "
                            f"<= {crash_pct:.0f}% - FLATTENING ALL POSITIONS NOW",
                            "ERROR")
                        if not self.safe_mode.is_active():
                            self.safe_mode.trigger(f"BTC crash {btc_4h:+.1f}%/4h")
                        try:
                            self._emergency_close_all(
                                reason=f"BTC crash {btc_4h:+.1f}%/4h")
                        except Exception as _ce:
                            self._log_error("BTC-crash flatten", _ce)
                        if not self.state.get_all():
                            self._hard_kill_fired = True
                        else:
                            retry_required = True
                            log_event(
                                "SYSTEMIC KILLSWITCH: positions remain after "
                                "flatten - will retry on next tick", "WARN")
            return not retry_required
        except Exception as e:
            self._log_error("killswitch check", e)
            return False

    def _killswitch_price(self, sym: str, d: dict, entry: float) -> float:
        """Best-effort fresh price for daily-loss killswitch accounting.

        The killswitch runs before per-position monitor updates on some ticks.
        Reading only the previous ``last_price`` can delay a hard flatten after
        a gap. Use the existing ticker cache with a short timeout, then the
        mark-price fallback, and only then fall back to stale state.
        """
        symbol_full = f"{sym}/USDT:USDT"
        try:
            tk = self.ticker_cache.get(self.ex, symbol_full, timeout=1.5,
                                       critical=True)
            px = FuturesExitsMixin._safe_positive_price(tk.get("last"))
            if px <= 0:
                px = FuturesExitsMixin._safe_positive_price(tk.get("close"))
            if px > 0:
                return px
        except Exception:
            pass
        try:
            px = FuturesExitsMixin._safe_positive_price(
                self._fallback_mark_price(symbol_full))
            if px > 0:
                return px
        except Exception:
            pass
        px = FuturesExitsMixin._safe_positive_price(d.get("last_price"))
        if px > 0:
            return px
        return FuturesExitsMixin._safe_positive_price(entry)

  #  Periodic funding-paid persistence 

  # How often to refresh per position (4h is  of Bitget's 8h settlement
  # window  guarantees at most one missed settlement on API outage).
    _FUNDING_REFRESH_INTERVAL_SEC = 4 * 3600

    # How often to re-pull the exchange's liquidation price per position. The
    # liq price moves only on margin/size/funding changes, so a real API call
    # every monitor tick (~20s) is wasteful; the liq-distance check still uses
    # the cached value + live price every tick.
    LIQ_REFRESH_INTERVAL_SEC = 90.0

    def _maybe_persist_funding_for_all(self, trades: dict, now_epoch: float
                                         ) -> None:
        """Refresh ``funding_paid`` in state for positions that haven't
  been checked recently. Silent on failure  best-effort only.

        Keeps a recent value so that if Bitget's funding-history endpoint is
        briefly down or rate-limited at close time, the fallback
        ``funding_paid`` isn't the 0.0 written at entry (which would
  over-report PnL by the real funding, 1 USDT/day on a sizable
        position).
        """
        try:
            from bot_utils import fetch_or_estimate_funding
            from bot_utils.futures_funding import estimate_funding_paid
        except ImportError:
            return

        def _finite_money(value):
            if isinstance(value, bool):
                return None
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return None
            return parsed if math.isfinite(parsed) else None

        for sym, d in trades.items():
            try:
                next_check = FuturesExitsMixin._safe_finite_float(
                    d.get("funding_next_check_at", 0), 0.0)
                if next_check <= 0:
                    # First sight (just opened / adopted): stagger the initial
                    # fetch instead of bursting every position at once on the
                    # first monitor tick. Per-symbol jitter spreads later
                    # refreshes too. Funding right after open is ~0, so deferring
                    # the first read by one interval loses nothing.
                    jitter = abs(hash(sym)) % 600
                    self.state.update(
                        sym, "funding_next_check_at",
                        now_epoch + self._FUNDING_REFRESH_INTERVAL_SEC + jitter)
                    continue
                if now_epoch < next_check:
                    continue  # not yet due
                buy_time = d.get("buy_time")
                if not buy_time:
                    continue
                lev = FuturesExitsMixin._safe_positive_float(
                    d.get("leverage"), 0.0)
                entry = FuturesExitsMixin._safe_positive_price(d.get("buy"))
  # WHOLE-position notional (original_amount  contract_size  entry),
  # NOT the partial-reduced invested_usdt  lev. funding_paid must
                # stay the funding of the FULL position so the close-time
  # proportional scaling (remaining/original) is the SOLE scaling 
                # using the reduced notional here AND scaling at close double-counted
  # the reduction (remaining/original) and understated funding.
                pos_type = d.get("position_type", "LONG")
                symbol_full = f"{sym}/USDT:USDT"
                orig_amt = FuturesExitsMixin._safe_positive_float(
                    d.get("original_amount", d.get("amount", 0)), 0.0)
                if lev <= 0 or entry <= 0 or orig_amt <= 0:
                    continue
                contract_size = FuturesExitsMixin._safe_positive_float(
                    self._get_contract_size(symbol_full), 0.0)
                if contract_size <= 0:
                    continue
                notional = orig_amt * contract_size * entry
                current = _finite_money(d.get("funding_paid"))
                if self.simulation:
  # No real exchange position to query  estimate funding from
  # the live rate  settlements crossed so paper PnL carries it.
                    realized = estimate_funding_paid(
                        self.ex, symbol_full, buy_time, notional, pos_type)
                elif d.get("entry_funding_window_unverified") is True:
                    from bot_utils.futures_funding import fetch_realized_funding

                    realized = fetch_realized_funding(
                        self.ex,
                        symbol_full,
                        buy_time,
                        notional_usdt=notional,
                    )
                else:
                    realized = fetch_or_estimate_funding(
                        self.ex, symbol_full, buy_time,
                        notional_usdt=notional, pos_type=pos_type,
                        fallback_state_value=current if current is not None else 0.0,
                    )
                update = {
                    "funding_next_check_at": now_epoch + self._FUNDING_REFRESH_INTERVAL_SEC,
                }
                realized = _finite_money(realized)
                if realized is not None:
                    update["funding_paid"] = realized
                elif current is None:
                    update["funding_paid"] = 0.0
                self.state.update_many(sym, update)
            except Exception as _fr_err:
  # Best-effort  don't fail the monitor over a funding refresh.
                try:
                    from bot_utils import silent_log
                    silent_log(f"funding-refresh {sym}", _fr_err)
                except Exception:
                    pass

  #  Price-feed resilience 

    def _fallback_mark_price(self, symbol_full: str) -> float:
        """Mark price from the exchange POSITION when the ticker feed is dead.

        Keeps the liq-buffer protection and the killswitch's unrealized-PnL read
        alive for a symbol whose ticker stopped being fetchable (delist/halt/
        symbol-specific error) while the leveraged position is still open. SIM has
        no real position to query, so it returns 0 (paper exits wait for a tick).
        """
        if self.simulation:
            return 0.0
        now = time.monotonic()
        exchange_id = id(getattr(self, "ex", None))
        cached_at = getattr(self, "_fallback_marks_cached_at", None)
        cached_exchange_id = getattr(
            self,
            "_fallback_marks_exchange_id",
            None,
        )
        cached_marks = getattr(self, "_fallback_marks", None)
        if (
            isinstance(cached_marks, dict)
            and cached_exchange_id == exchange_id
            and isinstance(cached_at, (int, float))
            and not isinstance(cached_at, bool)
            and 0.0 <= now - float(cached_at)
            <= FuturesExitsMixin._MARK_FALLBACK_CACHE_TTL_SEC
        ):
            return FuturesExitsMixin._safe_positive_price(
                cached_marks.get(symbol_full)
            )
        try:
            reservation = try_consume_api_call(
                "futures_exit_mark_fetch_positions",
                critical=True,
                return_reservation=True,
            )
            if not reservation:
                return 0.0
            try:
                poss = _fetch_positions_compat_once(
                    self.ex,
                    symbol_full,
                ) or []
            except Exception:
                try:
                    from bot_utils.api_budget import (
                        ApiCallReservation,
                        record_api_error,
                    )

                    if isinstance(reservation, ApiCallReservation):
                        record_api_error(
                            "futures_exit_mark_fetch_positions",
                            reservation,
                        )
                except Exception:
                    pass
                raise
            valid_positions = [p for p in poss if isinstance(p, dict)]
            marks = {}
            for p in valid_positions:
                position_symbol = p.get("symbol")
                if not isinstance(position_symbol, str) or not position_symbol:
                    if len(valid_positions) != 1:
                        continue
                    position_symbol = symbol_full
                raw_info = p.get("info")
                info = raw_info if isinstance(raw_info, dict) else {}
                for raw_mark in (
                    p.get("markPrice"),
                    info.get("markPrice"),
                    info.get("marketPrice"),
                    p.get("lastPrice"),
                ):
                    mark = FuturesExitsMixin._safe_positive_price(raw_mark)
                    if mark > 0:
                        marks[position_symbol] = mark
                        break
            self._fallback_marks = marks
            self._fallback_marks_cached_at = time.monotonic()
            self._fallback_marks_exchange_id = exchange_id
            return FuturesExitsMixin._safe_positive_price(
                marks.get(symbol_full)
            )
        except Exception:
            pass
        return 0.0

    def _note_price_unavailable(self, sym: str) -> None:
        """Count consecutive ticks with NO usable price (ticker AND mark both
  dead) and report once past a threshold. LIVE positions require a safety
        escalation; SIM positions receive a non-actionable research notice."""
        from core.logger import log_event
        counts = getattr(self, "_price_unavail_counts", None)
        if counts is None:
            counts = {}
            self._price_unavail_counts = counts
        counts[sym] = counts.get(sym, 0) + 1
        if counts[sym] == 5:                      # ~5 consecutive monitor ticks
            if bool(getattr(self, "simulation", False)):
                log_event(
                    f"{sym}: SIM price unavailable for {counts[sym]} "
                    "consecutive ticks (ticker unavailable; no exchange "
                    "position exists) - paper exit evaluation deferred",
                    "INFO",
                )
            else:
                log_event(
                    f"{sym}: price unavailable for {counts[sym]} consecutive "
                    "ticks (ticker AND mark) - liq protection is BLIND on this "
                    "leveraged position. Check the symbol on the exchange / "
                    "close manually.",
                    "WARN",
                )
            try:
                if not self.simulation:
                    from core.logger import send_telegram
                    from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                        f"[{self.BOT_NAME}] {sym}: no price (ticker+mark) - "
                        f"liq protection blind. Check/close manually.")
            except Exception:
                pass

    def _clear_price_unavailable(self, sym: str) -> None:
        counts = getattr(self, "_price_unavail_counts", None)
        if counts:
            counts.pop(sym, None)

  #  Per-position exit check 

    def _claim_conflict_blocks_monitor(self, sym: str, row: dict) -> bool:
        if not isinstance(row, dict) or not row.get("claim_conflict"):
            return False
        from core.logger import log_event
        warned = getattr(self, "_claim_conflict_warned", set())
        if sym not in warned:
            log_event(
                f"[{self.BOT_NAME}] {sym}: claim conflict - monitor skipped "
                f"fail-closed; run claim/state repair",
                "ERROR",
            )
            warned.add(sym)
            self._claim_conflict_warned = warned
        return True

    def _check_position_exits(self, sym: str, d: dict) -> None:
        """Run all exit checks for ONE futures position.

        Priority order:
          1. Liquidation protection (highest priority)
          2. Breakeven activation (sets be_active + be_price)
          3. Breakeven stop (when be_active)
          4. Partial take-profit at ACTIVATION_PROFIT
          5. Trailing stop / SL (depends on partial_sold state)
        """
        from core.logger import log_event
        from core.database import upsert_futures_state
        from bot_utils.trade_state import (
            same_position_generation,
            update_many_if_current,
        )

        if d.get("accounting_already_booked"):
            # The monitor row is a snapshot.  Serialize cleanup with every
            # other close/reconcile path and re-read under the lock before the
            # dashboard delete; otherwise a completed cleanup followed by a
            # fast same-symbol re-entry can make this stale snapshot target the
            # replacement generation.
            from core.symbol_locks import close_lock
            try:
                with close_lock(
                    sym,
                    timeout=2.0,
                    bot_name=getattr(self, "BOT_NAME", "FUTURES"),
                ) as acquired:
                    if not acquired:
                        return
                    live = self.state.get(sym)
                    if not isinstance(live, dict):
                        return
                    if live.get("accounting_already_booked") is True:
                        FuturesExitsMixin._cleanup_accounted_close_state(
                            self, sym, live
                        )
                        return
                    d = live
            except Exception as exc:
                self._log_error(f"futures accounted cleanup lock {sym}", exc)
                return
        if d.get("accounting_pending"):
            from core.symbol_locks import close_lock
            try:
                with close_lock(
                    sym,
                    timeout=2.0,
                    bot_name=getattr(self, "BOT_NAME", "FUTURES"),
                ) as acquired:
                    if not acquired:
                        return
                    live = self.state.get(sym)
                    if not isinstance(live, dict) or not live.get(
                        "accounting_pending"
                    ):
                        return
                    from core.futures_bot_reconcile import (
                        _defer_offline_accounting_retry,
                        _offline_accounting_retry_due,
                    )
                    if not _offline_accounting_retry_due(live):
                        return
                    pending_fields = {
                        key: value
                        for key, value in live.items()
                        if key == "accounting_pending"
                        or key.startswith("accounting_pending_")
                    }
                    durable = update_many_if_current(
                        self.state, sym, pending_fields, live
                    )
                    if durable is not None and durable is not True:
                        retry_delay = _defer_offline_accounting_retry(
                            self, sym, live
                        )
                        log_event(
                            f"{sym}: pending full accounting retry deferred; "
                            f"recovery marker is not durable; retry in "
                            f"{retry_delay:.0f}s",
                            "ERROR",
                        )
                        return
                    if self._record_offline_close(sym, live):
                        booked = dict(live)
                        booked.update({
                            "accounting_already_booked": True,
                            "accounting_booked_sell_time": (
                                live.get("accounting_pending_sell_time")),
                            "accounting_booked_exchange_order_id": (
                                live.get("accounting_pending_exchange_order_id")),
                            "accounting_booked_reason": (
                                live.get("accounting_pending_reason")
                                or "Offline close"),
                        })
                        FuturesExitsMixin._cleanup_accounted_close_state(
                            self, sym, booked
                        )
                    else:
                        _defer_offline_accounting_retry(self, sym, live)
            except Exception as exc:
                self._log_error(f"futures pending accounting retry {sym}", exc)
            return
        if FuturesExitsMixin._claim_conflict_blocks_monitor(self, sym, d):
            return
        live = self.state.get(sym)
        if live == {}:
            # Compatibility for narrow legacy/test state adapters.  Real
            # TradeState rows are schema-validated and never empty.
            live = d
        if not isinstance(live, dict):
            return
        if not same_position_generation(live, d) and live != d:
            return
        d = live
        if FuturesExitsMixin._claim_conflict_blocks_monitor(self, sym, d):
            return
        if d.get("oversize_rollback_pending"):
            from core.symbol_locks import close_lock

            try:
                with close_lock(sym, bot_name=self.BOT_NAME) as acquired:
                    if not acquired:
                        return
                    live = self.state.get(sym)
                    if not isinstance(live, dict) or not live.get(
                        "oversize_rollback_pending"
                    ):
                        return
                    if FuturesExitsMixin._claim_conflict_blocks_monitor(
                        self, sym, live
                    ):
                        return
                    pos_type = str(live.get("position_type") or "").upper()
                    entry = FuturesExitsMixin._safe_positive_price(
                        live.get("buy")
                    )
                    margin = FuturesExitsMixin._safe_positive_float(
                        live.get("invested_usdt"), 0.0
                    )
                    leverage = FuturesExitsMixin._safe_positive_float(
                        live.get("leverage"), 0.0
                    )
                    amount = FuturesExitsMixin._safe_nonnegative_amount(
                        live.get("amount")
                    )
                    if (
                        pos_type not in ("LONG", "SHORT")
                        or entry <= 0
                        or margin <= 0
                        or leverage <= 0
                        or amount <= 0
                    ):
                        log_event(
                            f"{sym}: persisted oversize rollback has invalid "
                            f"state; close deferred fail-closed",
                            "ERROR",
                        )
                        return
                    liq_price = FuturesExitsMixin._safe_positive_price(
                        live.get("liquidation_price")
                    )
                    current = FuturesExitsMixin._safe_positive_price(
                        live.get("last_price")
                    ) or entry
                    reason = str(
                        live.get("oversize_rollback_reason")
                        or "Oversized Entry Rollback"
                    )
                    self._execute_full_close(
                        sym,
                        live,
                        current,
                        0.0,
                        0.0,
                        entry,
                        liq_price,
                        margin,
                        leverage,
                        pos_type,
                        reason=reason,
                    )
            except Exception as exc:
                self._log_error(f"retry oversized-entry rollback {sym}", exc)
            return
        self._retry_pending_partial_accounting(sym, d)
        live = self.state.get(sym)
        if live == {}:
            live = d
        if not isinstance(live, dict):
            return
        if not same_position_generation(live, d) and live != d:
            return
        d = live
        if FuturesExitsMixin._claim_conflict_blocks_monitor(self, sym, d):
            return
        if d.get("accounting_pending_partials"):
            return

        symbol_full = f"{sym}/USDT:USDT"

        # Fetch current price (cached, timeout-protected)
        try:
  # critical=True  this is the price for an OPEN position; it must
            # not be dropped by the API-budget gate under contention, or the
            # stop-loss/liquidation checks below silently stop running.
            ticker = self.ticker_cache.get(self.ex, symbol_full, timeout=5.0,
                                           critical=True)
            curr = FuturesExitsMixin._ticker_price(ticker)
        except Exception:
            curr = 0.0
        if not math.isfinite(curr) or curr <= 0:
            # DON'T go blind: every safety check below (liq-buffer protection,
            # SL/trailing) and the last_price write the killswitch reads live
            # PAST this point. When the ticker feed is down (delist/halt/symbol
            # error), fall back to the exchange position's mark price so the
            # leveraged position keeps its liq protection and the daily-loss
            # killswitch keeps seeing a real drawdown instead of a stale value.
            curr = self._fallback_mark_price(symbol_full)
        if not math.isfinite(curr) or curr <= 0:
            self._note_price_unavailable(sym)
            return
        self._clear_price_unavailable(sym)

        pos_type = d["position_type"]
        lev = FuturesExitsMixin._safe_positive_float(
            d.get("leverage"),
            FuturesExitsMixin._safe_positive_float(self.C("LEVERAGE", 3), 3.0),
        )
        margin = FuturesExitsMixin._safe_positive_float(
            d.get("invested_usdt"), 0.0)
        entry = FuturesExitsMixin._safe_positive_price(d.get("buy"))

        # Guard: a corrupted/zero entry price would cause a ZeroDivisionError
        # in calc_unrealized_pnl (divides by entry). This can happen if a
        # state row was written before the fill price came back, or after a
        # partial DB write. Skip this monitor tick rather than crashing the
        # whole monitor thread for this position.
        if entry <= 0:
            log_event(
                f"Monitor: {sym} has invalid entry price ({entry}) - "
                f"skipping tick, will retry next cycle", "WARN"
            )
            return

        # Sanity: heal margin if broken (defensive)
        if margin <= 0:
            try:
                from bot_utils import futures_contract_size_or_none

                # contract_size-aware: _amt is in CONTRACTS, so notional =
                # amount * contract_size * price (required for contract_size!=1
                # coins, else the healed margin feeds the full-close PnL wrong).
                repaired_margin = FuturesExitsMixin._reconstructed_margin_usdt(
                    amount=d.get("amount"),
                    contract_size=futures_contract_size_or_none(
                        self.ex, f"{sym}/USDT:USDT"
                    ),
                    entry_price=entry,
                    leverage=lev,
                )
                if repaired_margin is not None:
                    margin = round(repaired_margin, 4)
            except Exception:
                pass
        if margin <= 0:
            log_event(
                f"Monitor: {sym} has invalid margin - skipping tick, "
                f"will retry next cycle", "WARN"
            )
            return
        if FuturesExitsMixin._safe_positive_float(
                d.get("invested_usdt"), 0.0) != margin:
            d["invested_usdt"] = margin
            try:
                update_many_if_current(
                    self.state, sym, {"invested_usdt": margin}, d
                )
            except Exception as e:
                self._log_error(f"futures margin repair {sym}", e)

        # Liquidation price: exchange-reported > local approximation.
        # Feed the exchange's ACTUAL maintenance-margin tier into the
        # approximation instead of the flat 0.01 default: a higher MM moves the
        # liq price closer to entry (conservative), so the buffer trigger fires
        # earlier rather than too late.
        mm_rate = get_maintenance_margin_rate(self.ex, symbol_full)
        if not self.simulation:
            now_ts = time.time()
            # Throttle the exchange-liq API call; between refreshes use the
            # cached liquidation_price (or a local estimate if none stored yet).
            if now_ts >= FuturesExitsMixin._safe_finite_float(
                    d.get("liq_next_check_at"), 0.0):
                exch_liq = get_exchange_liq_price(
                    self.ex,
                    symbol_full,
                    expected_position_side=pos_type,
                )
                interval = FuturesExitsMixin._safe_positive_float(
                    getattr(self, "LIQ_REFRESH_INTERVAL_SEC", 90.0), 90.0)
                upd = {"liq_next_check_at": now_ts + interval}
                if exch_liq > 0:
                    liq_price = exch_liq
                    upd["liquidation_price"] = exch_liq
                else:
                    liq_price = FuturesExitsMixin._safe_positive_float(
                        d.get("liquidation_price"),
                        calc_liquidation_price(entry, lev, pos_type, mm_rate))
                update_many_if_current(self.state, sym, upd, d)
            else:
                liq_price = FuturesExitsMixin._safe_positive_float(
                    d.get("liquidation_price"),
                    calc_liquidation_price(entry, lev, pos_type, mm_rate))
        else:
            liq_price = FuturesExitsMixin._safe_positive_float(
                d.get("liquidation_price"),
                calc_liquidation_price(entry, lev, pos_type, mm_rate))

        pnl_usdt, pnl_pct_margin = calc_unrealized_pnl(entry, curr, margin, lev, pos_type)
        move_pct = price_move_pct(entry, curr, pos_type)
        liq_dist = distance_to_liquidation_pct(curr, liq_price, pos_type)
        telemetry = {}
        try:
            prev_max_pct = FuturesExitsMixin._safe_finite_float(
                d.get("max_profit_pct"), move_pct)
        except (TypeError, ValueError):
            prev_max_pct = move_pct
        try:
            prev_min_pct = FuturesExitsMixin._safe_finite_float(
                d.get("min_profit_pct"), move_pct)
        except (TypeError, ValueError):
            prev_min_pct = move_pct
        try:
            prev_max_usdt = FuturesExitsMixin._safe_finite_float(
                d.get("max_profit_usdt"), pnl_usdt)
        except (TypeError, ValueError):
            prev_max_usdt = pnl_usdt
        try:
            prev_min_usdt = FuturesExitsMixin._safe_finite_float(
                d.get("min_profit_usdt"), pnl_usdt)
        except (TypeError, ValueError):
            prev_min_usdt = pnl_usdt
        if "max_profit_pct" not in d or move_pct > prev_max_pct:
            telemetry["max_profit_pct"] = move_pct
        if "min_profit_pct" not in d or move_pct < prev_min_pct:
            telemetry["min_profit_pct"] = move_pct
        if "max_profit_usdt" not in d or pnl_usdt > prev_max_usdt:
            telemetry["max_profit_usdt"] = pnl_usdt
        if "min_profit_usdt" not in d or pnl_usdt < prev_min_usdt:
            telemetry["min_profit_usdt"] = pnl_usdt
        if telemetry:
            update_many_if_current(self.state, sym, telemetry, d)
            d.update(telemetry)

        FuturesExitsMixin._record_futures_exit_shadow(
            self,
            sym,
            d,
            move_pct=move_pct,
            mfe_pct=FuturesExitsMixin._safe_finite_float(
                d.get("max_profit_pct"), move_pct),
            mae_pct=FuturesExitsMixin._safe_finite_float(
                d.get("min_profit_pct"), move_pct),
        )

        # Update highest favorable move
        highest = FuturesExitsMixin._safe_positive_float(
            d.get("highest"), entry)
        if is_new_high(curr, highest, pos_type):
            update_many_if_current(
                self.state, sym, {"highest": curr}, d
            )
            highest = curr

        # Stash last_price for emergency-close fallback
        update_many_if_current(
            self.state, sym, {"last_price": curr}, d
        )

        # ``_maybe_persist_funding_for_all`` already runs once per monitor
        # tick and refreshes funding_paid for every open position whose
  # ``funding_next_check_at`` deadline has passed  no per-position call
        # needed here.

        # Dashboard state
        try:
            upsert_futures_state(
                symbol=sym, bot_name=self.BOT_NAME, mode_is_sim=self.simulation,
                position_type=pos_type,
                entry_price=entry, current_price=curr,
                leverage=lev, margin_usdt=margin,
                position_size_usdt=margin * lev,
                unrealized_pnl=pnl_usdt, unrealized_pct=pnl_pct_margin,
                liquidation_price=liq_price, liq_distance_pct=liq_dist,
                funding_paid=d.get("funding_paid", 0.0),
                opened_at=d["buy_time"], entry_id=d.get("entry_id")
            )
        except Exception:
            pass  # dashboard state is best-effort

  #  Breakeven activation 
        be_trigger = FuturesExitsMixin._safe_finite_float(
            self.C("BREAKEVEN_TRIGGER", 0), 0.0)
        if be_trigger > 0 and not d.get("be_active", False) and move_pct >= be_trigger:
            safe_be_price = fee_buffered_breakeven(entry, pos_type, fee_buffer=0.003)
            update_many_if_current(
                self.state,
                sym,
                {"be_active": True, "be_price": safe_be_price},
                d,
            )
            # Update local copy so subsequent checks this tick see it
            d["be_active"] = True
            d["be_price"] = safe_be_price
            log_event(
                f"BREAKEVEN activated: {sym} ({pos_type}) @ +{move_pct:.2f}% - "
                f"SL at {safe_be_price:.6f} (entry {entry:.6f} +0.3% fee buffer)",
                "INFO"
            )

  #  Decide exit 
        sell_trigger, reason = self._evaluate_futures_exit(
            sym, d, curr, move_pct, highest, entry, liq_price, liq_dist, lev, pos_type
        )

  #  Partial TP (only if no exit fired) 
        activation = FuturesExitsMixin._safe_finite_float(
            self.C("ACTIVATION_PROFIT"), 0.0)
        partial_blocked_until = FuturesExitsMixin._safe_finite_float(
            d.get("partial_tp_blocked_min_notional_until"), 0.0)
        partial_block_active = (
            bool(d.get("partial_tp_blocked_min_notional"))
            and time.time() < partial_blocked_until
        )
        pending_partial_intent = _has_futures_partial_exit_intent(d)
        if (
            pending_partial_intent
            or (
                not sell_trigger
                and not d.get("partial_sold")
                and not partial_block_active
                and move_pct >= activation
            )
        ):
            from core.symbol_locks import close_lock
            partial_path_taken = False
            with close_lock(sym, bot_name=self.BOT_NAME) as got:
                if not got or not self.state.has(sym):
                    return
                live = self.state.get(sym) or d
                if not same_position_generation(live, d) and live != d:
                    return
                if FuturesExitsMixin._claim_conflict_blocks_monitor(
                    self, sym, live
                ):
                    return
                live_pending_partial = _has_futures_partial_exit_intent(live)
                live_partial_blocked_until = (
                    FuturesExitsMixin._safe_finite_float(
                        live.get("partial_tp_blocked_min_notional_until"), 0.0
                    )
                )
                live_partial_block_active = (
                    bool(live.get("partial_tp_blocked_min_notional"))
                    and time.time() < live_partial_blocked_until
                )
                if live_pending_partial or (
                    not sell_trigger
                    and not live.get("partial_sold")
                    and not live_partial_block_active
                    and move_pct >= activation
                ):
                    self._execute_partial_tp(
                        sym,
                        live,
                        curr,
                        move_pct,
                        pnl_pct_margin,
                        entry,
                        liq_price,
                        margin,
                        lev,
                        pos_type,
                    )
                    partial_path_taken = True
            if partial_path_taken:
                return

        if sell_trigger:
            # Serialize the flatten against the reconcile-thread offline-close
            # (same per-symbol lock) so it can't be double-booked as a phantom
  # offline-close. Skip on contention  retried next monitor tick.
            from core.symbol_locks import close_lock
            with close_lock(sym, bot_name=self.BOT_NAME) as got:
                if not got or not self.state.has(sym):
                    return
                live = self.state.get(sym) or d
                if not same_position_generation(live, d) and live != d:
                    return
                if FuturesExitsMixin._claim_conflict_blocks_monitor(
                    self, sym, live
                ):
                    return
                if _has_futures_partial_exit_intent(live):
                    self._execute_partial_tp(
                        sym,
                        live,
                        curr,
                        move_pct,
                        pnl_pct_margin,
                        entry,
                        liq_price,
                        margin,
                        lev,
                        pos_type,
                    )
                    return
                live_pos_type = live.get("position_type")
                if live_pos_type not in {"LONG", "SHORT"}:
                    log_event(
                        f"{sym}: full close blocked by invalid live position side",
                        "ERROR",
                    )
                    return
                live_entry = FuturesExitsMixin._safe_positive_price(
                    live.get("buy")
                )
                live_lev = FuturesExitsMixin._safe_positive_float(
                    live.get("leverage"), 0.0
                )
                live_margin = FuturesExitsMixin._safe_positive_float(
                    live.get("invested_usdt"), 0.0
                )
                if live_entry <= 0 or live_lev <= 0 or live_margin <= 0:
                    log_event(
                        f"{sym}: full close blocked by invalid live accounting "
                        "basis",
                        "ERROR",
                    )
                    return
                live_move_pct = price_move_pct(
                    live_entry, curr, live_pos_type
                )
                live_pnl_usdt, _ = calc_unrealized_pnl(
                    live_entry,
                    curr,
                    live_margin,
                    live_lev,
                    live_pos_type,
                )
                live_liq_price = FuturesExitsMixin._safe_positive_float(
                    live.get("liquidation_price"),
                    calc_liquidation_price(
                        live_entry, live_lev, live_pos_type, mm_rate
                    ),
                )
                live_liq_dist = distance_to_liquidation_pct(
                    curr, live_liq_price, live_pos_type
                )
                live_highest = FuturesExitsMixin._safe_positive_float(
                    live.get("highest"), live_entry
                )
                live_sell_trigger, live_reason = self._evaluate_futures_exit(
                    sym,
                    live,
                    curr,
                    live_move_pct,
                    live_highest,
                    live_entry,
                    live_liq_price,
                    live_liq_dist,
                    live_lev,
                    live_pos_type,
                )
                if not live_sell_trigger:
                    return
                live = FuturesExitsMixin._prepare_peak_trail_decision(
                    self, sym, live, reason=live_reason,
                    decision_price=curr, decision_move_pct=live_move_pct,
                    mfe_pct=FuturesExitsMixin._safe_finite_float(
                        live.get("max_profit_pct"), live_move_pct),
                )
                self._execute_full_close(
                    sym,
                    live,
                    curr,
                    live_move_pct,
                    live_pnl_usdt,
                    live_entry,
                    live_liq_price,
                    live_margin,
                    live_lev,
                    live_pos_type,
                    live_reason,
                )

  #  Exit evaluation (pure decision) 

    def _evaluate_futures_exit(self, sym, d, curr, move_pct, highest, entry,
                                 liq_price, liq_dist, lev, pos_type):
        """Decide whether to close. Returns (should_close, reason).

        Priority:
          1. Previously verified partial full-close retry
          2. Liquidation buffer consumed  "Liq protection"
          3. Breakeven stop (when be_active)
          4. (Partial TP handled by caller)
          5. Pre-activation peak giveback (FUTURES only, before first partial)
          6. Age-gated MFE fallback (owned FUTURES positions only)
          7. Trailing or SL  depends on partial_sold state

  ``sym`` is passed explicitly (the trades dict has no "symbol" field 
        sym is the outer key) so the ``initial_liq_distance`` heal-write
        targets the right row.
        """
        from core.logger import log_event

        # A verified fragment from a previous full-close attempt commits the
        # position to flattening. Do not let a price rebound cancel the retry.
        pending_filled = FuturesExitsMixin._safe_nonnegative_amount(
            d.get("pending_close_filled_amount"))
        if pending_filled > 0.0:
            pending_reason = str(d.get("pending_close_reason") or "").strip()
            return True, pending_reason or "Pending Full Close Retry"

  #  1. Liquidation protection (highest priority) 
        initial_liq_dist = FuturesExitsMixin._safe_positive_float(
            d.get("initial_liq_distance"), 0.0)
        if initial_liq_dist <= 0:
            initial_liq_dist = max(1.0, 100.0 / max(1.0, lev))
            try:
                self.state.update(sym, "initial_liq_distance", initial_liq_dist)
            except Exception:
                pass

        consumed = liq_buffer_consumed_pct(initial_liq_dist, liq_dist)
        panic_threshold = 100.0 - FuturesExitsMixin._safe_finite_float(
            self.C("LIQ_SAFETY_PCT", 25.0), 25.0)
        if consumed >= panic_threshold:
            log_event(
                f"EMERGENCY: Liquidation buffer "
                f"{consumed:.0f}% consumed (current dist {liq_dist:.1f}%, "
                f"initial {initial_liq_dist:.1f}%) - closing!",
                "WARN"
            )
            return True, (f"Liq protection ({liq_dist:.1f}% left, "
                          f"{consumed:.0f}% buffer consumed)")

  #  2. Breakeven stop (when be_active) 
        if d.get("be_active"):
            be_price = FuturesExitsMixin._safe_positive_float(
                d.get("be_price"), entry)
            if breakeven_stop_hit(curr, be_price, pos_type):
                return True, "Breakeven-Stop"

        # Protect a proven favorable move before the regular partial-TP arms.
        # This is deliberately FUTURES-only: CROSS shares this class but has a
        # separate portfolio exit model, while FUTREND owns an independent
        # pre-activation giveback implementation.
        if (str(getattr(self, "BOT_NAME", "")).upper() == "FUTURES"
                and not d.get("partial_sold")
                and not d.get("break_even")):
            from trading.futures_peak_trail import (
                PEAK_TRAIL_EXIT_REASON,
                peak_trail_hit,
                validate_peak_trail_config,
            )

            peak_config, peak_error = validate_peak_trail_config(
                enabled=self.C(
                    "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED", True),
                activation_mfe_pct=self.C(
                    "PRE_ACTIVATION_MIN_MFE_PCT", 1.5),
                giveback_pct=self.C(
                    "PRE_ACTIVATION_GIVEBACK_PCT", 0.75),
            )
            if peak_error:
                if getattr(self, "_peak_trail_config_warning", "") != peak_error:
                    self._peak_trail_config_warning = peak_error
                    log_event(
                        f"[FUTURES] peak trail config ignored: {peak_error}",
                        "WARN",
                    )
            else:
                self._peak_trail_config_warning = ""
                mfe_pct = FuturesExitsMixin._safe_finite_float(
                    d.get("max_profit_pct"), move_pct)
                if peak_trail_hit(
                        move_pct=move_pct, mfe_pct=mfe_pct,
                        config=peak_config):
                    return True, PEAK_TRAIL_EXIT_REASON

        # Cut a failed favorable excursion only after enough observation time.
        # Adopted positions have incomplete path history and an adoption-time
        # timestamp, so they are intentionally outside this rule.
        if (str(getattr(self, "BOT_NAME", "")).upper() == "FUTURES"
                and not d.get("partial_sold")
                and not d.get("break_even")
                and not d.get("adopted")):
            from trading.futures_mfe_fallback import (
                MFE_FALLBACK_EXIT_REASON,
                mfe_fallback_hit,
                validate_mfe_fallback_config,
            )

            fallback_config, fallback_error = validate_mfe_fallback_config(
                enabled=self.C("MFE_FALLBACK_STOP_ENABLED", True),
                min_age_minutes=self.C(
                    "MFE_FALLBACK_MIN_AGE_MINUTES", 45.0),
                min_mfe_pct=self.C("MFE_FALLBACK_MIN_MFE_PCT", 0.8),
                exit_move_pct=self.C("MFE_FALLBACK_EXIT_MOVE_PCT", -1.5),
                initial_stop_loss_pct=self.C("INITIAL_STOP_LOSS", -4.0),
            )
            if fallback_error:
                if (getattr(self, "_mfe_fallback_config_warning", "")
                        != fallback_error):
                    self._mfe_fallback_config_warning = fallback_error
                    log_event(
                        f"[FUTURES] MFE fallback config ignored: "
                        f"{fallback_error}",
                        "WARN",
                    )
            else:
                self._mfe_fallback_config_warning = ""
                mfe_pct = FuturesExitsMixin._safe_finite_float(
                    d.get("max_profit_pct"), move_pct)
                if mfe_fallback_hit(
                        buy_time=d.get("buy_time"), move_pct=move_pct,
                        mfe_pct=mfe_pct, config=fallback_config):
                    return True, MFE_FALLBACK_EXIT_REASON

  #  3. Trailing / SL 
        trailing_dist = FuturesExitsMixin._safe_finite_float(
            self.C("TRAILING_DISTANCE"), 0.0)
        post_partial_trailing_dist = FuturesExitsMixin._safe_finite_float(
            self.C("POST_PARTIAL_TRAILING_DISTANCE", trailing_dist),
            trailing_dist)
        activation = FuturesExitsMixin._safe_finite_float(
            self.C("ACTIVATION_PROFIT"), 0.0)
        if (post_partial_trailing_dist <= 0.0
                or (activation > 0.0 and post_partial_trailing_dist >= activation)):
            post_partial_trailing_dist = trailing_dist
        initial_sl = FuturesExitsMixin._safe_finite_float(
            self.C("INITIAL_STOP_LOSS"), -100.0)

        if d.get("break_even"):
            # Post-partial-TP: trailing fully armed
            if trailing_stop_hit(curr, highest, post_partial_trailing_dist, pos_type):
                return True, "Trailing Stop"
            be_floor = FuturesExitsMixin._safe_positive_float(
                d.get("be_price"),
                fee_buffered_breakeven(entry, pos_type, fee_buffer=0.003),
            )
            if breakeven_stop_hit(curr, be_floor, pos_type):
                return True, "Break-Even Stop"
        else:
            # Pre-partial. Trailing only after ACTIVATION_PROFIT high-water.
            # Gate on high_prof (peak), not current move_pct, so a position
            # that hits +5% then pulls back to +4% keeps the trailing block
            # active instead of falling through to only INITIAL_STOP_LOSS.
            high_prof = price_move_pct(entry, highest, pos_type)
            if high_prof >= activation:
                if trailing_stop_hit(curr, highest, trailing_dist, pos_type):
                    return True, "Trailing Stop"
            if not d.get("be_active") and move_pct <= initial_sl:
                return True, "Stop-Loss"

        # Lowest-priority capital-efficiency experiment. Shadow is the default:
        # it records what would have exited without changing position handling.
        from trading.profit_experiments import (
            position_age_minutes,
            time_decay_decision,
        )
        age_minutes = position_age_minutes(d.get("buy_time"))
        if age_minutes is not None:
            decay = time_decay_decision(
                age_minutes=age_minutes,
                max_age_minutes=FuturesExitsMixin._safe_finite_float(
                    self.C("TIME_DECAY_MAX_AGE_MINUTES", 360.0), 360.0
                ),
                mfe_pct=FuturesExitsMixin._safe_finite_float(
                    d.get("max_profit_pct"), move_pct
                ),
                min_mfe_pct=FuturesExitsMixin._safe_finite_float(
                    self.C("TIME_DECAY_MIN_MFE_PCT", 0.5), 0.5
                ),
                mode=str(self.C("TIME_DECAY_MODE", "shadow") or "shadow").lower(),
            )
            shadow_identity = str(
                d.get("entry_id")
                or d.get("buy_time")
                or d.get("buy")
                or "unknown"
            )[:192]
            shadow_key = f"{str(sym)[:64]}:{shadow_identity}"
            shadow_cache = getattr(
                self, "_time_decay_shadow_log_cache", None
            )
            if not isinstance(shadow_cache, dict):
                shadow_cache = {}
                self._time_decay_shadow_log_cache = shadow_cache
            shadow_seen = bool(
                d.get("time_decay_shadow_seen")
                or shadow_key in shadow_cache
            )
            should_log_decay = bool(
                decay.shadow_should_exit
                and (
                    decay.should_exit
                    or not shadow_seen
                )
            )
            shadow_logged = False
            if should_log_decay:
                try:
                    from core.logger import log_struct

                    log_struct(
                        "time_decay_decision",
                        bot=self.BOT_NAME,
                        symbol=sym,
                        mode=str(self.C("TIME_DECAY_MODE", "shadow")),
                        age_minutes=age_minutes,
                        mfe_pct=d.get("max_profit_pct"),
                        enforced=decay.should_exit,
                        emission_reason=(
                            "enforced"
                            if decay.should_exit
                            else "first_shadow_candidate"
                        ),
                    )
                    shadow_logged = True
                except Exception:
                    pass
            if shadow_logged and not decay.should_exit:
                shadow_cache.pop(shadow_key, None)
                shadow_cache[shadow_key] = True
                while len(shadow_cache) > 128:
                    shadow_cache.pop(next(iter(shadow_cache)))
                d["time_decay_shadow_seen"] = True
                try:
                    self.state.update(
                        sym, "time_decay_shadow_seen", True
                    )
                except Exception:  # noqa: BLE001 -- telemetry must not block exits
                    self._time_decay_shadow_persist_failed = True
            if decay.should_exit:
                return True, "Time Decay"

        return False, ""

  #  Partial TP 

    def _get_contract_size(self, symbol_full: str) -> float:
        """Contract size from CCXT market metadata (1.0 default).

  Thin wrapper over ``bot_utils.futures_contract_size``  single source of
        truth so the open/exit/reconcile paths all read the size identically
        (incl. the ``info.contractSize`` fallback for markets that only expose
        it there).
        """
        from bot_utils import futures_contract_size
        return futures_contract_size(self.ex, symbol_full)

    def _execute_partial_tp(self, sym, d, curr, move_pct, pnl_pct_margin,
                              entry, liq_price, margin, lev, pos_type) -> bool:
        """Sell PARTIAL_SELL_PCT% of the position at market.

        On success:
  Persist actual fill price, sold amount, fee
  Compute partial PnL with proportional entry fee
  Update state: partial_sold=True, break_even=True (arms trailing)
  partial_profit_realized accumulated for total-PnL Telegram messages
        """
        from core.logger import log_event, send_telegram
        from core.database import save_trade_db
        from config.exchange_config import reduce_only_params
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from bot_utils import (create_order_with_retry,
                                 extract_or_estimate_futures_fee,
                                 safe_remaining)

        pending_partial_intent = _has_futures_partial_exit_intent(d)
        if self.simulation and pending_partial_intent:
            log_event(
                f"{sym}: LIVE partial-TP intent present in SIM mode - "
                "manual recovery required",
                "ERROR",
            )
            return False
        partial_pct = FuturesExitsMixin._safe_finite_float(
            self.C("PARTIAL_SELL_PCT"), 0.0
        )
        if not pending_partial_intent:
            if partial_pct <= 0:
                return False
            if partial_pct > 1:
                partial_pct = 1.0
        symbol_full = f"{sym}/USDT:USDT"
        if (
            not self.simulation
            and d.get("entry_funding_window_unverified") is True
        ):
            from bot_utils.futures_funding import fetch_realized_funding

            exact_funding = fetch_realized_funding(
                self.ex,
                symbol_full,
                d.get("buy_time"),
                notional_usdt=(margin * lev if margin > 0 and lev > 0 else None),
            )
            if exact_funding is None:
                log_event(
                    f"Partial-TP {sym}: exact recovered funding history "
                    "unavailable; submit deferred",
                    "ERROR",
                )
                return False
            try:
                funding_persisted = self.state.update(
                    sym,
                    "funding_paid",
                    exact_funding,
                )
            except Exception:
                funding_persisted = False
            if funding_persisted is not None and funding_persisted is not True:
                log_event(
                    f"Partial-TP {sym}: exact recovered funding was not "
                    "durable; submit deferred",
                    "ERROR",
                )
                return False
            d = dict(d)
            d["funding_paid"] = exact_funding
        amount_total = FuturesExitsMixin._safe_nonnegative_amount(
            d.get("amount", 0))
        if amount_total <= 0:
            return False
        raw_partial = (
            FuturesExitsMixin._safe_nonnegative_amount(
                d.get(_PARTIAL_EXIT_AMOUNT)
            )
            if pending_partial_intent
            else amount_total * partial_pct
        )
        margin_mode = str(d.get("margin_mode") or self.C("MARGIN_MODE", "isolated") or "isolated").lower()

        # Include contract size in the notional computation. For typical Bitget
        # USDT-M perpetuals contract_size=1; for inverse/COIN-M or markets with
        # contract multipliers (Bybit inverse) it's the difference between TP
        # firing correctly and skipping erroneously.
        contract_size = FuturesExitsMixin._safe_positive_float(
            self._get_contract_size(symbol_full), 0.0)
        remaining_after = amount_total - raw_partial
        slice_notional = raw_partial * curr * contract_size
        rem_notional   = remaining_after * curr * contract_size
        # Min-notional pre-check against the REAL per-market floor
  # (limits.cost.min via the shared helper), not a hardcoded 5.0  some
        # perps floor at 1 USDT, others at 10. contract_size is folded into the
        # effective amount so the helper's amount*price equals our notional.
        from bot_utils.futures_exits import _check_min_notional
        ok_slice, ok_rem = True, True
        if not pending_partial_intent:
            ok_slice, _ = _check_min_notional(
                self.ex, symbol_full, raw_partial * contract_size, curr
            )
            ok_rem, _ = _check_min_notional(
                self.ex, symbol_full, remaining_after * contract_size, curr
            )
        if not ok_slice or not ok_rem:
            log_event(
                f"{sym}: partial-TP would violate min-notional "
                f"(slice={slice_notional:.2f} USDT, "
                f"remainder={rem_notional:.2f} USDT, "
                f"contract_size={contract_size}) - "
                f"skipping, transition to trailing",
                "WARN"
            )
            self.state.update_many(sym, {
                "break_even": True,
                "partial_tp_blocked_min_notional": True,
                "partial_tp_blocked_min_notional_until": time.time() + 300.0,
            })
            return False

        fill_price = curr
        fill_src = "simulation" if self.simulation else "initial"
        partial_amount = raw_partial
        partial_fee = 0.0
        exch_oid = None  # real order id - unique trade-dedup key

        if self.simulation:
            from bot_utils.fee_math import taker_fee_rate
            partial_fee = (raw_partial * contract_size * curr
                           * taker_fee_rate(self.ex, symbol_full))
        else:
            try:
                if not pending_partial_intent:
                    try:
                        from config.exchange_config import safe_amount_to_precision
                        partial_amount = FuturesExitsMixin._precision_amount_or_none(
                            safe_amount_to_precision(
                                self.ex, symbol_full, raw_partial
                            )
                        )
                        if partial_amount is None:
                            log_event(
                                f"{sym}: partial amount precision invalid - skip",
                                "WARN",
                            )
                            return False
                    except Exception:
                        try:
                            partial_amount = float(
                                Decimal(str(raw_partial)).quantize(
                                    Decimal("0.0001"), rounding=ROUND_DOWN
                                )
                            )
                        except (TypeError, ValueError, ArithmeticError):
                            partial_amount = 0.0
                if partial_amount <= 0:
                    log_event(f"{sym}: partial amount below exchange minimum - skip", "WARN")
                    return False
                close_side = "sell" if pos_type == "LONG" else "buy"
                client_order_id, partial_amount, created_intent = (
                    _ensure_futures_partial_exit_intent(
                        self.state,
                        sym,
                        d,
                        requested_amount=partial_amount,
                        position_side=pos_type,
                        bot_name=self.BOT_NAME,
                    )
                )
                observed_filled = FuturesExitsMixin._safe_nonnegative_amount(
                    d.get(_PARTIAL_EXIT_OBSERVED_FILLED)
                )
                order_params = reduce_only_params(
                    position_side=(
                        "long" if pos_type == "LONG" else "short"
                    ),
                    margin_mode=margin_mode,
                    leverage=max(1, int(math.ceil(lev))),
                    client_order_id=client_order_id,
                )
                from bot_utils.futures_order import (
                    _exchange_id,
                    _find_order_by_client_id,
                    _order_confirmed_terminal_zero_fill,
                    _order_landed,
                    _order_refresh_conflicts,
                    _requested_position_side,
                )
                requested_position_side = _requested_position_side(order_params)
                order = None
                if not created_intent:
                    lookup_status = {}
                    lookup_since_ms = _futures_partial_exit_lookup_since_ms(d)
                    order = _find_order_by_client_id(
                        self.ex,
                        symbol_full,
                        client_order_id,
                        log_event=log_event,
                        lookup_status=lookup_status,
                        expected_amount=partial_amount,
                        expected_side=close_side,
                        expected_position_side=requested_position_side,
                        exchange_id=_exchange_id(self.ex),
                        expected_reduce_only=True,
                        lookup_since_ms=lookup_since_ms,
                    )
                    recovery_conflict = (
                        isinstance(order, dict)
                        and order.get("_bot_recovery_conflict") is True
                    )
                    if lookup_status.get("unavailable") or recovery_conflict:
                        log_event(
                            f"Partial-TP {sym}: pending order recovery "
                            "unavailable or conflicting - retry remains blocked",
                            "ERROR" if recovery_conflict else "WARN",
                        )
                        return False
                    if (
                        order is None
                        and lookup_since_ms is not None
                        and lookup_status.get("complete_negative") is True
                    ):
                        if observed_filled > 0:
                            log_event(
                                f"Partial-TP {sym}: complete negative order "
                                "evidence contradicts a durable earlier "
                                "partial fill - retry remains blocked",
                                "ERROR",
                            )
                            return False
                        if not _clear_futures_partial_exit_intent(
                            self.state, sym, d
                        ):
                            log_event(
                                f"Partial-TP {sym}: complete negative intent "
                                "could not be cleared durably",
                                "ERROR",
                            )
                        else:
                            log_event(
                                f"Partial-TP {sym}: complete negative MEXC "
                                "order evidence cleared stale intent",
                                "WARN",
                            )
                        return False
                    if _order_confirmed_terminal_zero_fill(order):
                        if observed_filled > 0:
                            log_event(
                                f"Partial-TP {sym}: terminal zero-fill "
                                "snapshot contradicts a durable earlier "
                                "partial fill - retry remains blocked",
                                "ERROR",
                            )
                            return False
                        if not _clear_futures_partial_exit_intent(
                            self.state, sym, d
                        ):
                            log_event(
                                f"Partial-TP {sym}: terminal zero-fill intent "
                                "could not be cleared durably",
                                "ERROR",
                            )
                        return False
                    if order is None or not _order_landed(order):
                        log_event(
                            f"Partial-TP {sym}: pending order not proven "
                            "terminal - retry remains blocked",
                            "WARN",
                        )
                        return False
                if order is None:
                    from bot_utils.trade_state import registry_order_guard
                    with registry_order_guard(
                        self.state, sym, d
                    ) as ownership_live:
                        if not isinstance(ownership_live, dict):
                            return False
                        if ownership_live.get("claim_conflict"):
                            log_event(
                                f"{sym}: partial-TP blocked by registry claim "
                                f"conflict",
                                "ERROR",
                            )
                            return False
                        live_client_order_id = order_id_text_or_none(
                            ownership_live.get(_PARTIAL_EXIT_CLIENT_ID)
                        )
                        live_requested_amount = (
                            FuturesExitsMixin._safe_positive_float(
                                ownership_live.get(_PARTIAL_EXIT_AMOUNT), 0.0
                            )
                        )
                        amount_tolerance = max(1e-12, partial_amount * 1e-9)
                        if (
                            live_client_order_id != client_order_id
                            or abs(live_requested_amount - partial_amount)
                            > amount_tolerance
                            or ownership_live.get(_PARTIAL_EXIT_SIDE) != pos_type
                            or ownership_live.get(_PARTIAL_EXIT_MODE) != "LIVE"
                        ):
                            log_event(
                                f"{sym}: partial-TP durable intent changed "
                                "before submit",
                                "ERROR",
                            )
                            return False
                        order = create_order_with_retry(
                            self.ex,
                            symbol_full,
                            close_side,
                            partial_amount,
                            params=order_params,
                            shutdown_event=self._shutdown_event,
                            max_attempts=1,
                            action_label=f"partial-TP {sym}",
                            log_event=log_event,
                        )
                exch_oid = (
                    order_id_text_or_none(order.get("id"))
                    or order_id_text_or_none(order.get("orderId"))
                )
                actual_filled = 0.0
                actual_filled = FuturesExitsMixin._safe_nonnegative_amount(
                    order.get("filled") if isinstance(order, dict) else None)
                try:
                    from bot_utils.futures_exits import _resolve_fill_price
                    fill_price, fill_src = _resolve_fill_price(
                        self.ex,
                        symbol_full,
                        order,
                        fill_price,
                        log_event,
                        expected_side=close_side,
                        expected_position_side=pos_type,
                        expected_client_id=client_order_id,
                        expected_amount=partial_amount,
                    )
                except Exception:
                    fallback_fill = FuturesExitsMixin._order_fill_price(order)
                    if fallback_fill > 0:
                        fill_price = fallback_fill
                if actual_filled <= 0 and exch_oid:
                    try:
                        import time as _t
                        for _att in range(2):
                            _t.sleep(0.35 * (1 + _att))
                            try:
                                allowed = try_consume_api_call(
                                    "futures_partial_tp_fill_fetch_order",
                                    critical=True,
                                )
                            except Exception as budget_exc:
                                log_event(
                                    f"Partial-TP {sym}: fill refresh skipped - "
                                    f"API budget gate unavailable "
                                    f"({type(budget_exc).__name__})",
                                    "WARN",
                                )
                                break
                            if not allowed:
                                log_event(
                                    f"Partial-TP {sym}: fill refresh skipped - "
                                    f"API budget exhausted",
                                    "WARN",
                                )
                                break
                            refreshed = self.ex.fetch_order(str(exch_oid), symbol_full)
                            if refreshed and (
                                not isinstance(refreshed, dict)
                                or _order_refresh_conflicts(
                                    order,
                                    refreshed,
                                    symbol_full,
                                    close_side,
                                    pos_type,
                                    _exchange_id(self.ex),
                                    expected_reduce_only=True,
                                    allow_one_way_position_side=True,
                                    expected_client_id=client_order_id,
                                    expected_amount=partial_amount,
                                )
                            ):
                                log_event(
                                    f"Partial-TP {sym}: fill refresh conflicts "
                                    "with the durable close intent",
                                    "ERROR",
                                )
                                break
                            actual_filled = FuturesExitsMixin._safe_nonnegative_amount(
                                (refreshed or {}).get("filled"))
                            if actual_filled > 0:
                                fallback_fill = FuturesExitsMixin._order_fill_price(refreshed or {})
                                if fallback_fill > 0:
                                    fill_price = fallback_fill
                                order = refreshed or order
                                break
                    except Exception:
                        actual_filled = 0.0
                order_is_terminal = _futures_partial_exit_order_is_terminal(
                    order,
                    requested_amount=partial_amount,
                    filled_amount=actual_filled,
                )
                if actual_filled <= 0 and order_is_terminal:
                    try:
                        from bot_utils import fetch_open_position
                        old_amount = float(d.get("amount", 0) or 0)
                        pos, unavailable = fetch_open_position(
                            self.ex,
                            symbol_full,
                            expected_position_side=pos_type,
                        )
                        if pos is not None:
                            from bot_utils.futures_order import (
                                _position_contracts_abs,
                            )

                            remaining_live = _position_contracts_abs(pos)
                            if remaining_live is None:
                                raise ValueError(
                                    "position quantity aliases conflict"
                                )
                            actual_filled = max(0.0, old_amount - remaining_live)
                        elif unavailable:
                            actual_filled = 0.0
                    except Exception:
                        actual_filled = 0.0
                if actual_filled <= 0:
                    log_event(f"Partial-TP {sym}: fill unverified - no PnL booked; "
                              f"state unchanged, retry next tick", "WARN")
                    return False
                fill_tolerance = max(1e-12, partial_amount * 1e-9)
                if not order_is_terminal:
                    if actual_filled > observed_filled + fill_tolerance:
                        try:
                            observed_persisted = self.state.update(
                                sym,
                                _PARTIAL_EXIT_OBSERVED_FILLED,
                                actual_filled,
                            )
                        except Exception as e:
                            observed_persisted = False
                            log_event(
                                f"Partial-TP {sym}: open partial-fill "
                                f"checkpoint failed: {e}",
                                "ERROR",
                            )
                        if observed_persisted is None or observed_persisted is True:
                            d[_PARTIAL_EXIT_OBSERVED_FILLED] = actual_filled
                        else:
                            log_event(
                                f"Partial-TP {sym}: open partial-fill "
                                "checkpoint was not durable; in-memory "
                                "evidence unchanged",
                                "ERROR",
                            )
                    log_event(
                        f"Partial-TP {sym}: order still nonterminal with "
                        f"cumulative fill {actual_filled:.12g}/"
                        f"{partial_amount:.12g} - accounting and further "
                        "additional partial submits remain blocked",
                        "WARN",
                    )
                    return False
                if actual_filled + fill_tolerance < observed_filled:
                    log_event(
                        f"Partial-TP {sym}: terminal fill regressed below "
                        "durable cumulative evidence - retry remains blocked",
                        "ERROR",
                    )
                    return False
                if d.get("entry_funding_window_unverified") is True:
                    exact_funding = fetch_realized_funding(
                        self.ex,
                        symbol_full,
                        d.get("buy_time"),
                        notional_usdt=(
                            margin * lev if margin > 0 and lev > 0 else None
                        ),
                    )
                    exact_funding = FuturesExitsMixin._safe_finite_float(
                        exact_funding,
                        None,
                    )
                    if exact_funding is None:
                        log_event(
                            f"Partial-TP {sym}: physical fill retained for "
                            "recovery because exact post-fill funding history "
                            "is unavailable",
                            "ERROR",
                        )
                        return False
                    d = dict(d)
                    d["funding_paid"] = exact_funding
                partial_amount = min(
                    partial_amount, max(actual_filled, observed_filled)
                )
                # Pass the real slice size + contractSize so the estimate is
                # correct even when the exchange returns the market-order
                # response with no fee AND filled=0 (else the exit fee books as
                # 0 and partial PnL is overstated).
                partial_fee = extract_or_estimate_futures_fee(
                    self.ex, order, symbol_full, fill_price,
                    amount=partial_amount, contract_size=contract_size,
                )
            except Exception as e:
                log_event(f"Partial-TP {sym} failed: {e}", "WARN")
                return False

        # Realized PnL on the partial slice (proportional entry-fee).
        # notional MUST include contract_size: partial_amount is in CONTRACTS,
        # so for contract_size != 1 coins omitting it inflates notional by the
        # contract_size factor (wrong PnL + invested_usdt). Reuse the
        # contract_size fetched at the top so the whole method works from ONE
        # consistent number.
        notional_partial = partial_amount * contract_size * entry
        pnl_partial, _ = calc_unrealized_pnl(entry, fill_price,
                                              notional_partial / max(lev, 1),
                                              lev, pos_type)
        initial_entry_fee = float(d.get("initial_entry_fee",
                                          d.get("fees_paid", 0.0)))
        original_amount = float(d.get("original_amount", d.get("amount", 0)))
  # Safe helpers  partial_sold=False because THIS IS the partial. They
        # defend against state corruption leaving original and amount = 0
        # (division crash).
        prop_entry_fee = safe_proportional_fee(
            initial_entry_fee, partial_amount, original_amount,
            partial_sold=False
        )
        funding_partial = safe_funding_scale(
            float(d.get("funding_paid", 0.0)), partial_amount,
            original_amount, partial_sold=False
        )
        profit_partial = round(pnl_partial - prop_entry_fee - partial_fee - funding_partial, 2)

        # DB row
        sell_time = _utc_now_str()
        partial_trade = dict(
            bot_name=self.BOT_NAME,
            mode_is_sim=self.simulation,
            symbol=sym,
            buy_price=entry, sell_price=fill_price,
            buy_time=d.get("buy_time", ""),
            sell_time=sell_time,
            profit_pct=move_pct, profit_usdt=profit_partial,
            invested_usdt=notional_partial / max(lev, 1),
            reason=("Partial Take-Profit"
                    + (" [estimated fill price]" if fill_src == "fallback" else "")),
            rsi_15m=d.get("rsi_15m"), rsi_1h=d.get("rsi_1h"),
            rsi_4h=d.get("rsi_4h"), change_pct=d.get("change_pct"),
            btc_trend=d.get("btc_trend"), fear_greed=d.get("fear_greed"),
            is_futures=True, position_type=pos_type, leverage=lev,
            liquidation_price=liq_price,
            funding_paid=funding_partial,
            is_partial=True,
            fees_usdt=prop_entry_fee + partial_fee,
            exchange_order_id=exch_oid,
            entry_quality_score=d.get("entry_quality_score"),
            entry_quality_label=d.get("entry_quality_label"),
            entry_quality_reasons=d.get("entry_quality_reasons"),
            entry_id=d.get("entry_id"),
        )
        # Write the physical shrink and the exact accounting event durably
        # before committing PnL. This makes a crash/restart replay idempotent.
        new_amount = safe_remaining(
            FuturesExitsMixin._safe_nonnegative_amount(d.get("amount", 0)),
            partial_amount,
        )
        new_invested = max(0.0, margin - notional_partial / max(lev, 1))
        new_fees = (
            FuturesExitsMixin._safe_finite_float(d.get("fees_paid"), 0.0)
            + partial_fee
        )
        prev_realized = FuturesExitsMixin._safe_finite_float(
            d.get("partial_profit_realized"), 0.0)
        prev_funding_booked = FuturesExitsMixin._safe_finite_float(
            d.get("funding_booked_on_partials"), 0.0)
        updates = {
            "partial_sold": True,
            "break_even": True,
            "partial_tp_blocked_min_notional": False,
            "partial_tp_blocked_min_notional_until": 0.0,
            "amount": new_amount,
            "invested_usdt": new_invested,
            "fees_paid": new_fees,
            "partial_profit_realized": prev_realized + profit_partial,
            "funding_booked_on_partials": prev_funding_booked + funding_partial,
            "funding_booked_on_partials_known": True,
            _PARTIAL_EXIT_CLIENT_ID: None,
            _PARTIAL_EXIT_AMOUNT: None,
            _PARTIAL_EXIT_SIDE: None,
            _PARTIAL_EXIT_MODE: None,
            _PARTIAL_EXIT_OBSERVED_FILLED: None,
        }
        if d.get("entry_funding_window_unverified") is True:
            updates["funding_paid"] = d["funding_paid"]
        from bot_utils.trade_state import normalize_pending_accounting_items
        pending = normalize_pending_accounting_items(
            d.get("accounting_pending_partials"))
        pending.append(partial_trade)
        updates["accounting_pending_partials"] = pending
        try:
            state_persisted = self.state.update_many(sym, updates)
        except Exception as e:
            state_persisted = False
            log_event(f"Partial-TP {sym}: state write-ahead failed: {e}", "ERROR")
        if state_persisted is not None and state_persisted is not True:
            log_event(
                f"{sym}: futures partial-TP state write-ahead failed - DB "
                f"booking deferred fail-closed",
                "ERROR",
            )
            return False

        try:
            accounting_ok = save_trade_db(**partial_trade) is True
        except Exception as e:
            accounting_ok = False
            log_event(f"save_trade_db partial {sym} failed: {e}", "WARN")
        if accounting_ok:
            try:
                cleared = self.state.update(
                    sym, "accounting_pending_partials", pending[:-1]
                )
            except Exception as e:
                cleared = False
                log_event(
                    f"Partial-TP {sym}: pending clear failed: {e}", "ERROR"
                )
            if cleared is not None and cleared is not True:
                log_event(
                    f"{sym}: futures partial-TP booked but durable pending "
                    f"clear failed; idempotent retry retained",
                    "ERROR",
                )
        else:
            log_event(
                f"{sym}: futures partial-TP DB save failed - slice kept "
                f"for accounting retry", "WARN")

        log_event(
            f"[{self.BOT_NAME}] PARTIAL {int(partial_pct*100)}% of {sym} ({pos_type}) "
            f"at +{move_pct:.2f}% (+{profit_partial:.2f} USDT, fee {partial_fee:.3f}) - "
            f"Stop on Break-Even",
            "WIN"
        )
        try:
            if not self.simulation:
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f"[{self.BOT_NAME}] PARTIAL {pos_type} {sym}\n"
                    f"{int(partial_pct*100)}% @ {fill_price:.6f} "
                    f"(+{move_pct:.2f}%, +{profit_partial:.2f} USDT)\n"
                    f"Stop: Break-Even"
                )
        except Exception as e:
            log_event(f"Telegram failed: {e}", "WARN")
        return True

    def _block_close_fragment_recovery(
        self,
        sym: str,
        log_event,
        detail: str,
    ) -> None:
        blocked = getattr(self, "_close_fragment_recovery_blocked", None)
        if not isinstance(blocked, set):
            blocked = set()
            self._close_fragment_recovery_blocked = blocked
        blocked.add(sym)
        log_event(
            f"{sym}: verified close fragment was not durable ({detail}); "
            f"further close attempts are blocked until restart reconciliation",
            "ERROR",
        )

    def _persist_close_fragment(
        self,
        sym: str,
        fields: dict,
        log_event,
    ) -> bool:
        try:
            persisted = self.state.update_many(sym, fields)
        except Exception as exc:
            FuturesExitsMixin._block_close_fragment_recovery(
                self, sym, log_event, type(exc).__name__
            )
            self._log_error(f"persist close fragment {sym}", exc)
            return False
        if persisted is not None and persisted is not True:
            FuturesExitsMixin._block_close_fragment_recovery(
                self, sym, log_event, "state persistence returned non-success"
            )
            return False
        blocked = getattr(self, "_close_fragment_recovery_blocked", None)
        if isinstance(blocked, set):
            blocked.discard(sym)
        return True

  #  Full close 

    def _execute_full_close(self, sym, d, curr, move_pct, pnl_usdt,
                              entry, liq_price, margin, lev, pos_type,
                              reason) -> None:
        """Close the remaining position via reduce-only market order.

        Verifies via fetch_positions that contracts==0 before removing
  local state  without this, IOC partial fills leave ghost
        positions on the exchange.
        """
        from core.logger import log_event, log_sell, send_telegram, save_trade, log_struct
        from core.database import save_trade_db
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from trading.risk_manager import analyze_and_adapt
        from bot_utils import (extract_or_estimate_futures_fee,
                                 fetch_or_estimate_funding,
                                 is_no_position_error,
                                 verify_position_closed)
        from bot_utils.futures_order import _extract_order_fee_futures_known

        close_started_mono = time.monotonic()
        symbol_full = f"{sym}/USDT:USDT"
        blocked_fragments = getattr(
            self, "_close_fragment_recovery_blocked", None
        )
        if isinstance(blocked_fragments, set) and sym in blocked_fragments:
            log_event(
                f"{sym}: close blocked pending restart reconciliation of an "
                f"undurable verified fill fragment",
                "ERROR",
            )
            return
        if d.get("verified_flat_pending_accounting"):
            log_event(
                f"{sym}: position already verified flat; waiting for "
                f"reconcile/offline accounting",
                "WARN",
            )
            return
        if not self.simulation:
            try:
                from bot_utils.close_fragments import pending_close_values
                pending_close_values(d)
            except Exception as fragment_error:
                FuturesExitsMixin._block_close_fragment_recovery(
                    self,
                    sym,
                    log_event,
                    type(fragment_error).__name__,
                )
                self._log_error(
                    f"read durable close fragment {sym}", fragment_error
                )
                return
        fill_price = curr
        close_fee = 0.0
        raw_amount = FuturesExitsMixin._safe_nonnegative_amount(
            d.get("amount", 0))
        if raw_amount <= 0:
            log_event(
                f"{sym}: close skipped, invalid state amount "
                f"({d.get('amount')!r})",
                "ERROR",
            )
            return
        contract_size = FuturesExitsMixin._safe_positive_float(
            self._get_contract_size(symbol_full), 0.0)
        exch_oid = None  # real order id - unique trade-dedup key
        margin_mode = str(d.get("margin_mode") or self.C("MARGIN_MODE", "isolated") or "isolated").lower()

        if self.simulation:
            if raw_amount > 0 and fill_price > 0:
                from bot_utils.fee_math import taker_fee_rate
                if contract_size > 0:
                    close_fee = (raw_amount * contract_size * fill_price
                                 * taker_fee_rate(self.ex, symbol_full))

        if not self.simulation:
            order = None
            close_amount = raw_amount
            order_filled = 0.0
            full_exit_order_terminal = False
            pending_fee_from_fallback = False
            try:
                close_side = "sell" if pos_type == "LONG" else "buy"
                try:
                    from bot_utils.close_fragments import pending_close_values

                    prior_filled, _px, _fee, _oid = pending_close_values(d)
                except Exception:
                    prior_filled = 0.0
                requested_residual = max(0.0, raw_amount - prior_filled)
                try:
                    from config.exchange_config import safe_amount_to_precision
                    rounded_amount = FuturesExitsMixin._precision_amount_or_none(
                        safe_amount_to_precision(
                            self.ex, symbol_full, requested_residual
                        )
                    )
                    if rounded_amount is None:
                        log_event(
                            f"{sym}: close skipped, invalid precision amount",
                            "ERROR",
                        )
                        return
                    close_amount = (
                        rounded_amount
                        if rounded_amount > 0
                        else requested_residual
                    )
                except Exception:
                    close_amount = requested_residual
                if close_amount <= 0:
                    log_event(
                        f"{sym}: no unaccounted residual remains for a new "
                        "full-close order",
                        "WARN",
                    )
                    return
                (
                    order,
                    close_amount,
                    full_exit_order_terminal,
                ) = _recover_or_submit_futures_full_exit(
                    self,
                    sym,
                    d,
                    symbol_full=symbol_full,
                    requested_amount=close_amount,
                    position_side=pos_type,
                    close_side=close_side,
                    margin_mode=margin_mode,
                    leverage=max(1, int(math.ceil(lev))),
                    action_label=f"close {sym}",
                    log_event=log_event,
                    log_struct=log_struct,
                )
                if order is None:
                    return
                exch_oid = (
                    order_id_text_or_none(order.get("id"))
                    or order_id_text_or_none(order.get("orderId"))
                )
                order_filled = FuturesExitsMixin._safe_nonnegative_amount(
                    order.get("filled") if isinstance(order, dict) else None)
                try:
                    from bot_utils.futures_exits import _resolve_fill_price
                    fill_price, _fill_src = _resolve_fill_price(
                        self.ex,
                        symbol_full,
                        order,
                        fill_price,
                        log_event,
                        expected_side=close_side,
                        expected_position_side=pos_type,
                        expected_client_id=d.get(_FULL_EXIT_CLIENT_ID),
                        expected_amount=close_amount,
                    )
                except Exception:
                    fallback_fill = FuturesExitsMixin._order_fill_price(order)
                    if fallback_fill > 0:
                        fill_price = fallback_fill
                close_fee = 0.0
            except Exception as e:
                if is_no_position_error(e):
                    pending_oid = d.get("pending_close_order_id")
                    if not pending_oid:
                        try:
                            closed, remaining = verify_position_closed(
                                self.ex,
                                symbol_full,
                                expected_position_side=pos_type,
                            )
                        except Exception as ve:
                            self._log_error(f"verify-flat-after-no-position {sym}", ve)
                            log_event(
                                f"{sym}: close error looked already-flat but "
                                f"verification failed - keeping state for reconcile",
                                "WARN",
                            )
                            return
                        if closed:
                            try:
                                marker_persisted = self.state.update_many(sym, {
                                    "verified_flat_pending_accounting": True,
                                    "verified_flat_reason": reason,
                                    "verified_flat_at": _utc_now_str(),
                                })
                            except Exception as state_err:
                                marker_persisted = False
                                self._log_error(
                                    f"mark verified-flat {sym}", state_err
                                )
                            if (
                                marker_persisted is not None
                                and marker_persisted is not True
                            ):
                                log_event(
                                    f"{sym}: verified-flat recovery marker "
                                    "was not durable",
                                    "ERROR",
                                )
                            log_event(
                                f"{sym}: position already flat on exchange "
                                f"({str(e)[:80]}) - keeping state for "
                                f"reconcile/offline accounting",
                                "WARN",
                            )
                            return
                        log_event(
                            f"{sym}: close error looked already-flat but "
                            f"{remaining:.6f} contracts remain - keeping state "
                            f"for retry",
                            "WARN",
                        )
                        return
                    try:
                        from bot_utils.close_fragments import pending_close_values
                        _amt, _px, _fee, _oid = pending_close_values(d)
                        if _px > 0:
                            fill_price = _px
                        close_fee = _fee
                        if _oid:
                            pending_oid = _oid
                    except Exception:
                        pending_price = FuturesExitsMixin._safe_positive_price(
                            d.get("pending_close_price"))
                        if pending_price > 0:
                            fill_price = pending_price
                        pending_fee = FuturesExitsMixin._safe_finite_float(
                            d.get("pending_close_fee"), None)
                        if pending_fee is not None:
                            close_fee = pending_fee
                            pending_fee_from_fallback = True
                    exch_oid = pending_oid or exch_oid
                    log_event(
                        f"{sym}: position no longer exists on exchange "
                        f"({str(e)[:80]}) after our close order - "
                        f"booking pending close.",
                        "WARN"
                    )
                else:
                    log_event(
                        f"Close-order {sym} FAILED: {e} - position remains OPEN",
                        "WARN"
                    )
                    self._log_error(f"Close {sym}", e)
                    return  # leave state, retry next tick

  #  VERIFY BEFORE BOOKING 
        # Confirm the position is flat (verify_position_closed) BEFORE writing
        # PnL  DB  Telegram. On a partial OR unverifiable close, keep the state
        # open and retry next tick. Verified partial fill fragments are stored
        # as weighted pending close data so the eventual confirmed full-close
        # books the correct total exactly once.
        funding_requires_history = (
            d.get("entry_funding_window_unverified") is True
            or d.get("accounting_pending_funding_unverified") is True
        )
        funding_history_resolved = False
        if not self.simulation:
            try:
                closed, remaining = verify_position_closed(
                    self.ex,
                    symbol_full,
                    expected_position_side=pos_type,
                )
            except Exception as e:
                if order_filled > 0 and fill_price > 0:
                    try:
                        from bot_utils.close_fragments import (
                            add_close_fragment_update,
                            pending_close_values,
                        )

                        prev_amount, _px, _fee, _oid = pending_close_values(d)
                        if prev_amount <= 0:
                            frag_fee = extract_or_estimate_futures_fee(
                                self.ex, order or {}, symbol_full, fill_price,
                                amount=order_filled,
                                contract_size=contract_size,
                            )
                            fragment_update = add_close_fragment_update(
                                d, amount=order_filled, price=fill_price,
                                fee=frag_fee, order_id=exch_oid,
                            )
                            fragment_update["pending_close_reason"] = str(
                                reason or "Pending Full Close Retry")
                            if full_exit_order_terminal:
                                fragment_update.update(
                                    _futures_full_exit_clear_fields()
                                )
                            if not FuturesExitsMixin._persist_close_fragment(
                                self, sym, fragment_update, log_event
                            ):
                                return
                    except Exception as fragment_error:
                        FuturesExitsMixin._block_close_fragment_recovery(
                            self,
                            sym,
                            log_event,
                            type(fragment_error).__name__,
                        )
                        self._log_error(
                            f"build close fragment {sym}", fragment_error
                        )
                self._log_error(f"verify-close {sym}", e)
                log_event(
                    f"{sym}: close verification raised - keeping state, "
                    f"will retry next tick", "WARN")
                return
            if not closed:
                if remaining > 0:
                    try:
                        from bot_utils.close_fragments import (
                            add_close_fragment_update, pending_close_values)
                        prev_amount, _px, _fee, _oid = pending_close_values(d)
                        total_filled = max(0.0, raw_amount - float(remaining))
                        fragment = max(0.0, total_filled - prev_amount)
                        if fragment > 0 and fill_price > 0:
                            frag_fee = extract_or_estimate_futures_fee(
                                self.ex, order or {}, symbol_full, fill_price,
                                amount=fragment, contract_size=contract_size,
                            )
                            fragment_update = add_close_fragment_update(
                                d, amount=fragment, price=fill_price,
                                fee=frag_fee, order_id=exch_oid,
                            )
                            fragment_update["pending_close_reason"] = str(
                                reason or "Pending Full Close Retry")
                            if full_exit_order_terminal:
                                fragment_update.update(
                                    _futures_full_exit_clear_fields()
                                )
                            if not FuturesExitsMixin._persist_close_fragment(
                                self, sym, fragment_update, log_event
                            ):
                                return
                        elif full_exit_order_terminal:
                            tolerance = max(1e-12, raw_amount * 1e-9)
                            if abs(total_filled - prev_amount) <= tolerance:
                                if not FuturesExitsMixin._persist_close_fragment(
                                    self,
                                    sym,
                                    _futures_full_exit_clear_fields(),
                                    log_event,
                                ):
                                    return
                    except Exception as fragment_error:
                        FuturesExitsMixin._block_close_fragment_recovery(
                            self,
                            sym,
                            log_event,
                            type(fragment_error).__name__,
                        )
                        self._log_error(
                            f"build close fragment {sym}", fragment_error
                        )
                    log_event(
                        f"{sym}: close incomplete - {remaining:.6f} contracts "
                        f"still open. Keeping full state, retry next tick "
                        f"(partial fill accounted pending).", "WARN")
                else:
                    log_event(
                        f"{sym}: close could not be verified (API glitch). "
                        f"Keeping state, retry next tick.", "WARN")
                return
            try:
                from bot_utils.close_fragments import (
                    add_close_fragment_update, pending_close_values)
                prev_amount, _px, _fee, _oid = pending_close_values(d)
                intent_base_filled = FuturesExitsMixin._safe_nonnegative_amount(
                    d.get(_FULL_EXIT_BASE_FILLED)
                )
                evidenced_amount = max(
                    prev_amount,
                    intent_base_filled + min(order_filled, close_amount),
                )
                evidence_tolerance = max(1e-12, raw_amount * 1e-9)
                if evidenced_amount + evidence_tolerance < raw_amount:
                    known_delta = max(0.0, evidenced_amount - prev_amount)
                    flat_pending = {
                        "verified_flat_pending_accounting": True,
                        "verified_flat_reason": reason,
                        "verified_flat_at": _utc_now_str(),
                        "verified_flat_sell_price": fill_price,
                        "verified_flat_exchange_order_id": exch_oid,
                    }
                    if known_delta > 0.0 and fill_price > 0.0:
                        known_fee = extract_or_estimate_futures_fee(
                            self.ex,
                            order or {},
                            symbol_full,
                            fill_price,
                            amount=known_delta,
                            contract_size=contract_size,
                        )
                        flat_pending.update(add_close_fragment_update(
                            d,
                            amount=known_delta,
                            price=fill_price,
                            fee=known_fee,
                            order_id=exch_oid,
                        ))
                    flat_pending.update(_futures_full_exit_clear_fields())
                    if not FuturesExitsMixin._persist_close_fragment(
                        self, sym, flat_pending, log_event
                    ):
                        return
                    log_event(
                        f"{sym}: position verified flat but only "
                        f"{evidenced_amount:.12g}/{raw_amount:.12g} contracts "
                        "have causal fill evidence; accounting deferred",
                        "ERROR",
                    )
                    return
                if order is None and prev_amount <= 0 and _px > 0:
                    fill_price = _px
                    close_fee = _fee
                    exch_oid = _oid or exch_oid
                else:
                    fragment = max(0.0, raw_amount - prev_amount)
                    if fragment > 0 and fill_price > 0:
                        frag_fee = extract_or_estimate_futures_fee(
                            self.ex, order or {}, symbol_full, fill_price,
                            amount=fragment, contract_size=contract_size,
                        )
                        pending_view = dict(d)
                        pending_view.update(add_close_fragment_update(
                            d, amount=fragment, price=fill_price,
                            fee=frag_fee, order_id=exch_oid,
                        ))
                        _amt, _price, _fee, _oid = pending_close_values(pending_view)
                        if _amt > 0 and _price > 0:
                            fill_price = _price
                            close_fee = _fee
                            exch_oid = _oid or exch_oid
                    else:
                        _amt, _price, _fee, _oid = pending_close_values(d)
                        if _amt > 0 and _price > 0:
                            fill_price = _price
                            close_fee = _fee
                            exch_oid = _oid or exch_oid
            except Exception:
                close_fee_known = pending_fee_from_fallback
                if not close_fee_known:
                    close_fee, close_fee_known = _extract_order_fee_futures_known(
                        order or {})
                if not close_fee_known:
                    try:
                        close_fee = extract_or_estimate_futures_fee(
                            self.ex, order or {}, symbol_full, fill_price,
                            amount=raw_amount, contract_size=contract_size,
                        )
                    except Exception:
                        close_fee = 0.0

            if d.get("unpriced_external_partials"):
                flat_pending = {
                    "verified_flat_pending_accounting": True,
                    "verified_flat_reason": reason,
                    "verified_flat_at": _utc_now_str(),
                    "verified_flat_sell_price": fill_price,
                    "verified_flat_exchange_order_id": exch_oid,
                }
                flat_pending.update(_futures_full_exit_clear_fields())
                try:
                    flat_persisted = self.state.update_many(sym, flat_pending)
                except Exception as state_err:
                    flat_persisted = False
                    self._log_error(
                        f"futures verified-flat combined accounting {sym}",
                        state_err,
                    )
                level = (
                    "WARN"
                    if flat_persisted is None or flat_persisted is True
                    else "ERROR"
                )
                log_event(
                    f"{sym}: final close verified flat with unpriced earlier "
                    f"partials; direct accounting deferred to combined offline "
                    f"reconcile",
                    level,
                )
                return

        # PnL with real fill + funding + proportional entry fee
        move_pct_real = price_move_pct(entry, fill_price, pos_type) if entry > 0 else move_pct
        peak_decision_at = FuturesExitsMixin._safe_positive_float(
            d.get("peak_trail_decision_at"), 0.0)
        if reason == "Pre-Activation Giveback Stop" and peak_decision_at > 0.0:
            execution_latency_ms = max(
                0.0, (time.time() - peak_decision_at) * 1000.0)
        else:
            execution_latency_ms = max(
                0.0, (time.monotonic() - close_started_mono) * 1000.0)
        pnl_real, _ = (calc_unrealized_pnl(entry, fill_price, margin, lev, pos_type)
                        if entry > 0 and margin > 0 else (pnl_usdt, 0.0))
        mfe_pct = FuturesExitsMixin._safe_finite_float(
            d.get("max_profit_pct"), move_pct_real)
        peak_decision_price = FuturesExitsMixin._safe_positive_float(
            d.get("peak_trail_decision_price"), curr)
        peak_decision_move_pct = FuturesExitsMixin._safe_finite_float(
            d.get("peak_trail_decision_move_pct"), move_pct)
        peak_decision_mfe_pct = FuturesExitsMixin._safe_finite_float(
            d.get("peak_trail_decision_mfe_pct"), mfe_pct)
        mae_pct = FuturesExitsMixin._safe_finite_float(
            d.get("min_profit_pct"), move_pct_real)
        giveback_pct = max(0.0, mfe_pct - move_pct_real)

        initial_entry_fee = FuturesExitsMixin._safe_finite_float(
            d.get("initial_entry_fee", d.get("fees_paid", 0.0)), 0.0)
        original_amount = FuturesExitsMixin._safe_nonnegative_amount(
            d.get("original_amount", d.get("amount", 0)))
        current_amount = FuturesExitsMixin._safe_nonnegative_amount(
            d.get("amount", 0))
  # Safe helper  defends against the original_amount=0 + partial_sold=True
        # combination (would otherwise double-deduct the entry fee).
        partial_sold = bool(d.get("partial_sold"))
        proportional_entry_fee = safe_proportional_fee(
            initial_entry_fee, current_amount, original_amount,
            partial_sold=partial_sold
        )

        funding_pd = FuturesExitsMixin._safe_finite_float(
            d.get("funding_paid"), 0.0)
        funding_resolution_pending = False
        funding_booked = FuturesExitsMixin._safe_finite_float(
            d.get("funding_booked_on_partials"), 0.0)
        # Scale by ACTUAL remaining ratio via the safe helper so a corrupted
        # original_amount can't crash the math.
        if partial_sold and original_amount > 0:
            funding_pd = safe_remaining_funding(
                funding_pd, current_amount, original_amount,
                partial_sold=True, booked_on_partials=funding_booked,
                booked_on_partials_known=(
                    d.get("funding_booked_on_partials_known") is True
                ),
            )
        if not self.simulation:
            try:
                notional = margin * lev if margin > 0 else 0.0
                if partial_sold and current_amount > 0 and original_amount > 0:
                    remaining_ratio = current_amount / original_amount
                    if 0 < remaining_ratio < 1:
                        notional = notional / remaining_ratio
                if funding_requires_history:
                    from bot_utils.futures_funding import fetch_realized_funding

                    realized = fetch_realized_funding(
                        self.ex,
                        symbol_full,
                        d.get("buy_time"),
                        notional_usdt=notional,
                    )
                else:
                    realized = fetch_or_estimate_funding(
                        self.ex, symbol_full, d.get("buy_time"),
                        notional_usdt=notional, pos_type=pos_type,
                        fallback_state_value=(
                            FuturesExitsMixin._safe_finite_float(
                                d.get("funding_paid"), 0.0
                            )
                        ),
                    )
                realized = FuturesExitsMixin._safe_finite_float(realized, None)
                funding_resolution_pending = realized is None
                if funding_requires_history:
                    funding_history_resolved = realized is not None
                if realized is not None:
                    # Safe helper for the realized-funding scaling too
                    if partial_sold and original_amount > 0:
                        realized = safe_remaining_funding(
                            realized, current_amount, original_amount,
                            partial_sold=True,
                            booked_on_partials=funding_booked,
                            booked_on_partials_known=(
                                d.get("funding_booked_on_partials_known")
                                is True
                            ),
                        )
                    funding_pd = realized
            except Exception:
                funding_resolution_pending = True

        slice_fees = proportional_entry_fee + close_fee
        profit_usdt = round(pnl_real - slice_fees - funding_pd, 2)
        # Round-trip fee total for the whole trade (Telegram display only).
        lifetime_fees = FuturesExitsMixin._safe_finite_float(
            d.get("fees_paid"), 0.0) + close_fee

        # Persist the exact replay key before accounting. Once a live close is
        # verified flat, local state is the last recoverable source for PnL.
        buy_time = d.get("buy_time", "")
        sell_time = _utc_now_str()
        trade_row = {
            "bot_name": self.BOT_NAME,
            "mode_is_sim": self.simulation,
            "symbol": sym,
            "buy_price": entry,
            "sell_price": fill_price,
            "buy_time": buy_time,
            "sell_time": sell_time,
            "profit_pct": move_pct_real,
            "profit_usdt": profit_usdt,
            "invested_usdt": margin,
            "reason": reason,
            "rsi_15m": d.get("rsi_15m"),
            "rsi_1h": d.get("rsi_1h"),
            "rsi_4h": d.get("rsi_4h"),
            "change_pct": d.get("change_pct"),
            "btc_trend": d.get("btc_trend"),
            "fear_greed": d.get("fear_greed"),
            "is_futures": True,
            "position_type": pos_type,
            "leverage": lev,
            "liquidation_price": liq_price,
            "funding_paid": funding_pd,
            "fees_usdt": slice_fees,
            "exchange_order_id": exch_oid,
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "giveback_pct": giveback_pct,
            "entry_quality_score": d.get("entry_quality_score"),
            "entry_quality_label": d.get("entry_quality_label"),
            "entry_quality_reasons": d.get("entry_quality_reasons"),
            "entry_id": d.get("entry_id"),
        }
        pending_close = {
            "accounting_pending": True,
            "accounting_pending_reason": reason,
            "accounting_pending_sell_price": fill_price,
            "accounting_pending_sell_time": sell_time,
            "accounting_pending_profit_pct": move_pct_real,
            "accounting_pending_profit_usdt": profit_usdt,
            "accounting_pending_mode_is_sim": self.simulation,
            "accounting_pending_fees_usdt": slice_fees,
            "accounting_pending_funding_paid": funding_pd,
            "accounting_pending_exchange_order_id": exch_oid,
            "accounting_pending_mfe_pct": mfe_pct,
            "accounting_pending_mae_pct": mae_pct,
            "accounting_pending_giveback_pct": giveback_pct,
            "accounting_pending_entry_quality_score": d.get(
                "entry_quality_score"
            ),
            "accounting_pending_entry_quality_label": d.get(
                "entry_quality_label"
            ),
            "accounting_pending_entry_quality_reasons": d.get(
                "entry_quality_reasons"
            ),
        }
        if funding_resolution_pending:
            pending_close["accounting_pending_funding_unverified"] = True
        elif funding_history_resolved:
            pending_close["entry_funding_window_unverified"] = False
            pending_close["accounting_pending_funding_unverified"] = False
        if not self.simulation:
            pending_close.update(_futures_full_exit_clear_fields())
        try:
            pending_persisted = self.state.update_many(sym, pending_close)
        except Exception as state_err:
            pending_persisted = False
            self._log_error(
                f"futures full accounting write-ahead {sym}", state_err
            )
        if pending_persisted is not None and pending_persisted is not True:
            log_event(
                f"{sym}: verified flat close was not booked because its "
                f"accounting recovery marker was not durable",
                "ERROR",
            )
            FuturesExitsMixin._record_peak_trail_execution(
                self, sym, d, reason=reason,
                decision_price=peak_decision_price, fill_price=fill_price,
                decision_move_pct=peak_decision_move_pct,
                fill_move_pct=move_pct_real,
                mfe_pct=peak_decision_mfe_pct,
                latency_ms=execution_latency_ms,
                profit_usdt=profit_usdt, fees_usdt=slice_fees,
                funding_paid=funding_pd, accounting_saved=False,
                exchange_order_id=exch_oid,
            )
            return
        if funding_resolution_pending:
            log_event(
                f"{sym}: verified flat close kept for accounting recovery "
                "because exact funding history is unavailable",
                "ERROR",
            )
            return
        accounting_ok = False
        try:
            accounting_ok = save_trade_db(**trade_row) is True
            if not accounting_ok:
                raise RuntimeError("save_trade_db returned False")
        except Exception as e:
            log_event(
                f"save_trade_db {sym} failed after verified flat close: {e}. "
                f"State kept for accounting recovery.", "WARN")
            FuturesExitsMixin._record_peak_trail_execution(
                self, sym, d, reason=reason,
                decision_price=peak_decision_price, fill_price=fill_price,
                decision_move_pct=peak_decision_move_pct,
                fill_move_pct=move_pct_real,
                mfe_pct=peak_decision_mfe_pct,
                latency_ms=execution_latency_ms,
                profit_usdt=profit_usdt, fees_usdt=slice_fees,
                funding_paid=funding_pd, accounting_saved=False,
                exchange_order_id=exch_oid,
            )
            return
        FuturesExitsMixin._record_peak_trail_execution(
            self, sym, d, reason=reason,
            decision_price=peak_decision_price, fill_price=fill_price,
            decision_move_pct=peak_decision_move_pct,
            fill_move_pct=move_pct_real,
            mfe_pct=peak_decision_mfe_pct,
            latency_ms=execution_latency_ms,
            profit_usdt=profit_usdt, fees_usdt=slice_fees,
            funding_paid=funding_pd, accounting_saved=True,
            exchange_order_id=exch_oid,
        )
        try:
            save_trade(
                log_dir=self.LOG_DIR, symbol=sym,
                buy_price=entry, buy_time=buy_time,
                sell_price=fill_price, profit_pct=move_pct_real,
                profit_usdt=profit_usdt, reason=f"{reason} ({pos_type})"
            )
        except Exception as e:
            log_event(f"save_trade {sym} failed: {e}", "WARN")
        try:
            log_sell(self.BOT_NAME, sym, move_pct_real, profit_usdt,
                      f"{reason} | {pos_type} @ {lev}x")
        except Exception as e:
            log_event(f"log_sell {sym} failed: {e}", "WARN")

        try:
            partial_realized = FuturesExitsMixin._safe_finite_float(
                d.get("partial_profit_realized"), 0.0)
            if d.get("partial_sold") and abs(partial_realized) > 0.005:
                total = profit_usdt + partial_realized
                total_line = (f"Total PnL: {total:+.2f} USDT "
                              f"(partial {partial_realized:+.2f} + final {profit_usdt:+.2f})\n")
            else:
                total_line = ""
            if not self.simulation:
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f"[{self.BOT_NAME}] "
                    f"{pos_type} CLOSE {sym}\n"
                    f"Move: {move_pct_real:+.2f}% ({profit_usdt:+.2f} USDT auf "
                    f"{margin:.0f} Margin @ {lev}x)\n"
                    f"{total_line}"
                    f"Fees: {lifetime_fees:.4f} | Funding: {funding_pd:+.3f}\n"
                    f"Reason: {reason}"
                )
        except Exception as e:
            log_event(f"telegram send for {sym} failed: {e}", "WARN")

        # Cooldown on liquidation-protection or ANY losing protective stop
  # (SL / trailing / break-even)  outcome-gated via the shared classifier
        # so a losing trailing/BE close also blocks immediate re-entry, not just
        # an exact "Stop-Loss".
        from trading.cooldown_utils import should_cooldown_after_exit
        if should_cooldown_after_exit(reason, profit_usdt):
            try:
                from trading.cooldown_utils import set_cooldown
                with self._cooldown_lock:
                    set_cooldown(self.cool, sym,
                                  int(self.C("COOLDOWN_AFTER_SL", 120)),
                                  self.COOLDOWN_FILE)
            except Exception as e:
                self._log_error(f"cooldown set {sym}", e)

  # State removal  the close was already CONFIRMED flat at the top of
        # this method, so booking + removal here are unconditional.
        # Scope by bot: FUTURES + CROSS share futures_state; an unscoped delete
        # would wipe the OTHER bot's dashboard row for the same base coin.
        cleanup_row = dict(d)
        cleanup_row.update({
            "accounting_already_booked": True,
            "accounting_booked_sell_time": sell_time,
            "accounting_booked_exchange_order_id": exch_oid,
            "accounting_booked_reason": reason,
        })
        FuturesExitsMixin._cleanup_accounted_close_state(self, sym, cleanup_row)
        try:
            analyze_and_adapt(self.BOT_NAME)
        except Exception as e:
            self._log_error("analyze_and_adapt", e)
