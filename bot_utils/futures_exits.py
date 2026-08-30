"""
bot_utils/futures_exits.py  Futures emergency-close-all helper.

Closes all open futures positions in parallel (bounded ThreadPoolExecutor),
with async fill-price recovery, a min-notional pre-check, and post-close
verification via verify_position_closed.
"""
from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_DOWN
from threading import Lock
from typing import Callable, Optional, Tuple

from bot_utils.api_budget import try_consume_api_call
from bot_utils.futures_order import (
    _explicit_client_order_ids,
    _explicit_order_ids,
    _order_id_text,
    _order_from_recovery_trades,
    _order_refresh_conflicts,
    create_order_with_retry,
    extract_or_estimate_futures_fee,
    futures_contract_size,
    is_no_position_error,
    verify_position_closed,
)
from bot_utils.futures_math import calc_unrealized_pnl, price_move_pct
from bot_utils.futures_funding import (
    fetch_or_estimate_funding,
    fetch_realized_funding,
)
from bot_utils.fee_math import taker_fee_rate
from bot_utils.order_utils import order_id_text_or_none


_MAX_PARALLEL_CLOSES = 3
_FILL_RESOLVE_DELAY_SEC = 0.8
_FILL_RESOLVE_MAX_RETRIES = 2
_VERIFY_CLOSE_TIMEOUT_SEC = 5.0
_FALLBACK_MIN_NOTIONAL = 5.0
_EMERGENCY_CLOSE_FRAGMENT_BLOCKED: set[tuple[str, str]] = set()
_EMERGENCY_CLOSE_FRAGMENT_BLOCKED_LOCK = Lock()


def _emergency_fragment_key(bot_name, sym) -> tuple[str, str]:
    return (str(bot_name or "").strip().upper(), str(sym or "").strip().upper())


def _emergency_fragment_is_blocked(bot_name, sym) -> bool:
    key = _emergency_fragment_key(bot_name, sym)
    with _EMERGENCY_CLOSE_FRAGMENT_BLOCKED_LOCK:
        return key in _EMERGENCY_CLOSE_FRAGMENT_BLOCKED


def _block_emergency_fragment(bot_name, sym) -> None:
    key = _emergency_fragment_key(bot_name, sym)
    with _EMERGENCY_CLOSE_FRAGMENT_BLOCKED_LOCK:
        _EMERGENCY_CLOSE_FRAGMENT_BLOCKED.add(key)


def _persist_emergency_fragment(
    *, state, sym, fields, bot_name, log_event, error_logger
) -> bool:
    try:
        persisted = state.update_many(sym, fields)
    except Exception as exc:
        persisted = False
        if error_logger:
            try:
                error_logger(f"emergency close fragment {sym}", exc)
            except Exception:
                pass
    if persisted is False:
        _block_emergency_fragment(bot_name, sym)
        log_event(
            f"  {sym}: close fragment recovery marker was not durable; "
            f"further emergency close orders are blocked until restart",
            "ERROR",
        )
        return False
    return True


def _utc_now_str() -> str:
    from core.logger import _date
    return _date()


def _extract_fill_from_order(order: dict) -> Optional[float]:
    if not isinstance(order, dict):
        return None
    for key in ("average", "price"):
        val = order.get(key)
        if isinstance(val, bool):
            continue
        if val is None:
            continue
        try:
            v = float(val)
            if math.isfinite(v) and v > 0:
                return v
        except (TypeError, ValueError, OverflowError):
            continue
    info = order.get("info")
    if isinstance(info, dict):
        for key in ("avgPrice", "averagePrice", "filledAvgPrice",
                     "fillPrice", "price"):
            val = info.get(key)
            if isinstance(val, bool):
                continue
            if val is None:
                continue
            try:
                v = float(val)
                if math.isfinite(v) and v > 0:
                    return v
            except (TypeError, ValueError, OverflowError):
                continue
    return None


def _positive_finite_or_zero(value) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return parsed if math.isfinite(parsed) and parsed > 0 else 0.0


def _finite_or_default(value, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if math.isfinite(parsed) else default


def _positive_finite_or_default(value, default: float = 0.0) -> float:
    parsed = _finite_or_default(value, default)
    return parsed if parsed > 0 else default


def _finite_precision_amount_or_none(value) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


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
    """MEXC swap fetch_order only accepts the numeric exchange order_id."""
    oid = _order_id_text(order_id)
    if not oid:
        return False
    if _is_mexc_swap_symbol(ex, symbol_full):
        return oid.isdigit()
    return True


def _trade_order_ids(trade: dict) -> set[str]:
    if not isinstance(trade, dict):
        return set()
    info = trade.get("info")
    if not isinstance(info, dict):
        info = {}
    return {
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


def _trade_as_order_snapshot(trade: dict, order_id: str) -> dict:
    snapshot = dict(trade)
    snapshot["id"] = order_id
    snapshot.pop("order", None)
    for key in ("orderId", "order_id", "orderID"):
        snapshot.pop(key, None)
    info = snapshot.get("info")
    if isinstance(info, dict):
        info = dict(info)
        info.pop("id", None)
        snapshot["info"] = info
    return snapshot


def _resolve_fill_price(ex,
                        symbol_full: str,
                        order: dict,
                        fallback_price: float,
                        log_event: Callable,
                        *,
                        expected_side: str,
                        expected_position_side: str = "",
                        expected_client_id: Optional[str] = None,
                        expected_amount: Optional[float] = None,
                        ) -> Tuple[float, str]:
    """Multi-stage fill-price recovery  returns (price, source_label)."""
    fallback = _positive_finite_or_zero(fallback_price)
    fp = _extract_fill_from_order(order)
    if fp is not None:
        return fp, "order"

    order_ids = _explicit_order_ids(order)
    if len(order_ids) != 1:
        return fallback, "fallback"
    order_id = next(iter(order_ids))
    client_ids = _explicit_client_order_ids(order)
    requested_client_id = _order_id_text(expected_client_id)
    if expected_client_id is not None and not requested_client_id:
        return fallback, "fallback"
    if len(client_ids) > 1:
        return fallback, "fallback"
    if requested_client_id and client_ids != set() and client_ids != {
        requested_client_id
    }:
        return fallback, "fallback"
    bound_client_id = (
        requested_client_id
        or next(iter(client_ids), "")
    )

    if _order_id_is_fetchable(ex, symbol_full, order_id):
        for attempt in range(_FILL_RESOLVE_MAX_RETRIES):
            try:
                allowed = try_consume_api_call(
                    "futures_exit_fill_fetch_order", critical=True
                )
            except Exception:
                return fallback, "fallback"
            if not allowed:
                return fallback, "fallback"
            try:
                time.sleep(_FILL_RESOLVE_DELAY_SEC * (1 + attempt))
                fetched = ex.fetch_order(order_id, symbol_full)
                if _order_refresh_conflicts(
                    order,
                    fetched,
                    symbol_full,
                    expected_side,
                    expected_position_side,
                    _exchange_id(ex),
                    expected_reduce_only=True,
                    allow_one_way_position_side=True,
                    expected_client_id=bound_client_id or None,
                    expected_amount=expected_amount,
                ):
                    try:
                        log_event(
                            "fetch_order fill price conflicts with close "
                            "order identity; ignoring refresh",
                            "ERROR",
                        )
                    except Exception:
                        pass
                    continue
                fp = _extract_fill_from_order(fetched)
                if fp is not None:
                    return fp, "fetch_order"
            except Exception as e:
                if attempt == _FILL_RESOLVE_MAX_RETRIES - 1:
                    try:
                        try:
                            from bot_utils.silent_log import silent_log
                            silent_log(
                                f"fetch_order fill price lookup {symbol_full}",
                                e)
                        except Exception:
                            pass
                        log_event(
                            "fetch_order fill price unavailable; using fallback",
                            "WARN"
                        )
                    except Exception:
                        pass

    try:
        allowed = try_consume_api_call(
            "futures_exit_fill_fetch_trades", critical=True
        )
    except Exception:
        return fallback, "fallback"
    if not allowed:
        return fallback, "fallback"

    try:
        trades = ex.fetch_my_trades(symbol_full, limit=10) or []
        if isinstance(trades, list) and trades:
            trades = [trade for trade in trades if isinstance(trade, dict)]
            same_order = []
            for trade in trades:
                trade_order_ids = _trade_order_ids(trade)
                if order_id in trade_order_ids and trade_order_ids != {
                    order_id
                }:
                    return fallback, "fallback"
                if trade_order_ids == {order_id}:
                    same_order.append(trade)
            if not same_order:
                return fallback, "fallback"
            for trade in same_order:
                if _order_refresh_conflicts(
                    order,
                    _trade_as_order_snapshot(trade, order_id),
                    symbol_full,
                    expected_side,
                    expected_position_side,
                    _exchange_id(ex),
                    expected_reduce_only=True,
                    allow_one_way_position_side=True,
                    expected_client_id=bound_client_id or None,
                ):
                    return fallback, "fallback"
            recovered = _order_from_recovery_trades(
                same_order,
                bound_client_id,
                symbol_full,
                expected_amount=expected_amount,
            )
            expected_trade_filled = _positive_finite_or_zero(
                order.get("filled") if isinstance(order, dict) else None
            ) or _positive_finite_or_zero(expected_amount)
            recovered_filled = _positive_finite_or_zero(
                recovered.get("filled")
            )
            if expected_trade_filled > 0:
                tolerance = max(1e-12, expected_trade_filled * 1e-9)
                if (
                    recovered_filled <= 0
                    or abs(recovered_filled - expected_trade_filled)
                    > tolerance
                ):
                    return fallback, "fallback"
            if _order_refresh_conflicts(
                order,
                recovered,
                symbol_full,
                expected_side,
                expected_position_side,
                _exchange_id(ex),
                expected_reduce_only=True,
                allow_one_way_position_side=True,
                expected_client_id=bound_client_id or None,
                expected_amount=expected_amount,
            ):
                return fallback, "fallback"
            fp = _extract_fill_from_order(recovered)
            if fp is not None:
                return fp, "trades"
    except Exception as e:
        try:
            log_event(
                f"  fetch_my_trades for fill price unavailable: {e}", "WARN"
            )
        except Exception:
            pass

    return fallback, "fallback"


def _check_min_notional(ex, symbol_full: str,
                          amount: float, price: float) -> Tuple[bool, str]:
    """Return (ok, reason)."""
    if amount <= 0:
        return False, "amount<=0"
    if price <= 0:
        return True, ""
    notional = amount * price

    min_cost = _FALLBACK_MIN_NOTIONAL
    try:
        if hasattr(ex, "markets") and isinstance(ex.markets, dict):
            mkt = ex.markets.get(symbol_full) or {}
            limits = mkt.get("limits") or {}
            cost = limits.get("cost") or {}
            mc = cost.get("min")
            if mc is not None:
                min_cost = float(mc)
    except Exception:
        pass

    if notional < min_cost:
        return False, (f"notional {notional:.4f} USDT < exchange min "
                        f"{min_cost:.4f} USDT")
    return True, ""


def _close_single_position(**kw):
    """Serialize the per-symbol emergency close against the monitor/reconcile
    close (the SAME close_lock those paths use) so a concurrent close can't
    double-book the trade or double-remove state. Fail-open: if the lock can't be
    acquired we still close  flattening a live position outranks the double-book
    guard on shutdown. If a concurrent close already booked+removed the symbol
    while we waited, bail without re-booking.
    """
    from core.symbol_locks import close_lock
    sym = kw.get("sym")
    state = kw.get("state")
    bot_name = kw.get("bot_name")
    if _emergency_fragment_is_blocked(bot_name, sym):
        return (
            sym,
            "failed",
            0.0,
            "close fragment recovery blocked; physical close not repeated",
        )
    with close_lock(sym, timeout=15.0, bot_name=bot_name,
                    fail_open=True) as got:
        if _emergency_fragment_is_blocked(bot_name, sym):
            return (
                sym,
                "failed",
                0.0,
                "close fragment recovery blocked; physical close not repeated",
            )
        if not got:
            return _flatten_without_accounting(**kw)
        if got and state is not None:
            try:
                if not state.has(sym):
                    return (sym, "closed", 0.0, None)
                live = state.get(sym)
                if isinstance(live, dict):
                    if live.get("accounting_pending") or live.get(
                        "accounting_already_booked"
                    ) or live.get("verified_flat_pending_accounting"):
                        return (
                            sym,
                            "failed",
                            0.0,
                            "accounting recovery pending; physical close "
                            "not repeated",
                        )
                    kw = dict(kw)
                    kw["d"] = live
            except Exception:
                pass
        return _close_single_position_impl(**kw)


def _flatten_without_accounting(**kw):
    sym = kw.get("sym")
    d = kw.get("d") or {}
    ex = kw.get("ex")
    state = kw.get("state")
    simulation = bool(kw.get("simulation"))
    margin_mode = kw.get("margin_mode") or "isolated"
    reduce_only_params = kw.get("reduce_only_params")
    log_event = kw.get("log_event") or (lambda *_a, **_k: None)
    shutdown_event = kw.get("shutdown_event")
    if simulation:
        return (sym, "failed", 0.0, "close lock held")
    try:
        pos_type = d.get("position_type", "LONG")
        amount = abs(_finite_or_default(d.get("amount", 0), 0.0))
        lev = _positive_finite_or_default(
            d.get("leverage", kw.get("default_leverage", 1)),
            kw.get("default_leverage", 1) or 1,
        )
        if amount <= 0:
            return (sym, "failed", 0.0, "close lock held and amount invalid")
        symbol_full = f"{sym}/USDT:USDT"
        side = "sell" if pos_type == "LONG" else "buy"
        try:
            close_amount = _finite_precision_amount_or_none(
                ex.amount_to_precision(symbol_full, amount)
            )
            if close_amount is None or Decimal(str(close_amount)) > Decimal(
                str(amount)
            ):
                return (sym, "failed", 0.0,
                        "close lock held and close amount invalid")
        except Exception:
            try:
                close_amount = float(
                    Decimal(str(amount)).quantize(
                        Decimal("0.0001"), rounding=ROUND_DOWN
                    )
                )
            except (TypeError, ValueError, ArithmeticError):
                close_amount = 0.0
        if not math.isfinite(close_amount) or close_amount <= 0:
            return (sym, "failed", 0.0,
                    "close lock held and close amount invalid")
        params = reduce_only_params(
            position_side=("long" if pos_type == "LONG" else "short"),
            margin_mode=margin_mode,
            leverage=max(1, int(__import__("math").ceil(lev))),
        )
        from bot_utils.trade_state import registry_order_guard
        with registry_order_guard(state, sym, d) as ownership_live:
            if not isinstance(ownership_live, dict):
                return (sym, "failed", 0.0, "managed state unavailable")
            if ownership_live.get("claim_conflict"):
                return (sym, "failed", 0.0, "registry claim conflict")
            create_order_with_retry(
                ex, symbol_full, side, close_amount, params=params,
                shutdown_event=shutdown_event, max_attempts=5,
                action_label=f"emergency flatten {sym}",
                log_event=log_event, abort_on_shutdown=False,
            )
        closed_ok, remaining = verify_position_closed(
            ex,
            symbol_full,
            timeout=_VERIFY_CLOSE_TIMEOUT_SEC,
            expected_position_side=pos_type,
        )
        if closed_ok:
            log_event(
                f"  [LIVE] {sym}: flattened while accounting lock was held; "
                f"state kept for reconcile/accounting", "WARN")
            return (sym, "failed", 0.0, "flattened without accounting")
        return (sym, "failed", 0.0,
                f"close lock held; flatten unverified ({remaining})")
    except Exception as e:
        return (sym, "failed", 0.0, f"close lock held; flatten failed: {e}")


def _close_single_position_impl(*,
                              sym: str,
                              d: dict,
                              ex,
                              state,
                              bot_name: str,
                              log_dir: str,
                              simulation: bool,
                              default_leverage: float,
                              margin_mode: str,
                              shutdown_event,
                              reason: str,
                              ticker_cache,
                              log_event: Callable,
                              log_sell: Callable,
                              log_struct: Optional[Callable],
                              save_trade_db: Callable,
                              save_trade: Callable,
                              remove_futures_state: Callable,
                              reduce_only_params: Callable,
                              error_logger: Optional[Callable],
                              ) -> Tuple[str, str, float, Optional[str]]:
    """Close ONE position. Returns (symbol, status, pnl, error_message)."""
    try:
        pos_type = d.get("position_type", "LONG")
        entry = _positive_finite_or_zero(d.get("buy", 0))
        margin = _positive_finite_or_zero(d.get("invested_usdt", 0))
        lev = _positive_finite_or_default(d.get("leverage", default_leverage),
                                          default_leverage or 1)
        amount = abs(_finite_or_default(d.get("amount", 0), 0.0))
        if amount <= 0:
            return (sym, "failed", 0.0, "amount invalid")
        if entry <= 0 or margin <= 0:
            return (sym, "failed", 0.0, "entry/margin invalid")
        symbol_full = f"{sym}/USDT:USDT"
        pending_fragment_amount = 0.0
        pending_fragment_price = 0.0
        if not simulation:
            from bot_utils.close_fragments import pending_close_values
            pending_fragment_amount, pending_fragment_price, _fee, _oid = \
                pending_close_values(d)
            has_fragment_marker = any(
                d.get(key) not in (None, "", 0, 0.0)
                for key in (
                    "pending_close_filled_amount",
                    "pending_close_notional_sum",
                    "pending_close_price",
                    "pending_close_fee",
                    "pending_close_order_id",
                )
            )
            if has_fragment_marker and (
                pending_fragment_amount <= 0
                or pending_fragment_price <= 0
                or pending_fragment_amount > amount + 1e-12
            ):
                return (
                    sym,
                    "failed",
                    0.0,
                    "close fragment recovery state invalid",
                )

        # Price fallback chain
        curr = 0.0
        if ticker_cache is not None:
            try:
                ticker = ticker_cache.get(
                    ex, symbol_full, timeout=5.0, critical=True
                )
                curr = _positive_finite_or_zero(ticker.get("last"))
                if curr <= 0:
                    curr = _positive_finite_or_zero(ticker.get("close"))
            except Exception as e:
                log_event(
                    f"  Price (cached) for {sym} unavailable: {e} - "
                    f"trying direct fetch", "WARN"
                )
        if curr <= 0:
            try:
                allowed = try_consume_api_call(
                    "futures_emergency_exit_fetch_ticker", critical=True
                )
            except Exception as gate_error:
                log_event(
                    f"  Price (direct) for {sym} unavailable: "
                    f"API budget gate failed ({gate_error})", "WARN"
                )
                allowed = False
            if allowed:
                try:
                    ticker = ex.fetch_ticker(symbol_full)
                    curr = _positive_finite_or_zero(ticker.get("last"))
                    if curr <= 0:
                        curr = _positive_finite_or_zero(ticker.get("close"))
                except Exception as e2:
                    log_event(
                        f"  Price (direct) for {sym} unavailable: {e2}", "WARN"
                    )
        if curr <= 0:
            curr = _positive_finite_or_zero(d.get("last_price", 0))
        used_entry_fallback = False
        if curr <= 0:
            curr = entry
            used_entry_fallback = True
            log_event(
                f"  {sym}: NO market price available - using entry as "
                f"approximation. PnL may be unreliable.", "WARN"
            )

        # Order placement
        fill_price: Optional[float] = None
        fill_source = "n/a"
        exch_oid: Optional[str] = None  # real order id  trade-dedup key (G2)
        close_fee = 0.0
        order = None
        # contractSize-aware fee math  needed for contract_size != 1 coins
        # (1000SATS, MEME, ).
        _cs = futures_contract_size(ex, symbol_full)

        if simulation:
            fill_price = curr
            fill_source = "simulation"
            if amount > 0 and fill_price > 0:
                # CROSS paper state stores ``amount`` in base coins, not in
                # exchange contracts (see CrossBot._open_leg). Applying the
                # venue contract size a second time can inflate low-price-coin
                # fees by orders of magnitude and strand the durable
                # accounting marker when the DB sanity guard rejects it.
                simulation_contract_size = (
                    1.0
                    if str(bot_name or "").strip().upper() == "CROSS"
                    else _cs
                )
                close_fee = (
                    amount
                    * simulation_contract_size
                    * fill_price
                    * taker_fee_rate(ex, symbol_full)
                )
        else:
            try:
                close_side = "sell" if pos_type == "LONG" else "buy"
                raw_amount = max(0.0, amount - pending_fragment_amount)
                if raw_amount <= 1e-12:
                    return (
                        sym,
                        "failed",
                        0.0,
                        "close fragment recovery pending; physical close "
                        "not repeated",
                    )
                try:
                    from config.exchange_config import safe_amount_to_precision
                    close_amount = _finite_precision_amount_or_none(
                        safe_amount_to_precision(ex, symbol_full, raw_amount)
                    )
                    if close_amount is None:
                        return (sym, "failed", 0.0, "close amount invalid")
                except Exception:
                    close_amount = float(raw_amount)
                if close_amount <= 0 and raw_amount > 0:
                    close_amount = float(raw_amount)
                if not math.isfinite(close_amount) or close_amount <= 0:
                    return (sym, "failed", 0.0, "close amount invalid")

                ok, why = _check_min_notional(ex, symbol_full,
                                                 close_amount, curr)
                if not ok:
                    # Do NOT skip: a reduce-only close is exempt from the
                    # OPEN-min-notional on most venues, and the normal full-close
                    # path submits below-min reduce-only orders directly. Pre-
                    # filtering here made the BACKSTOP give up on exactly the
                    # residual/dust legs the routine exit would have flattened.
                    # Attempt the order; let the exchange reject if truly invalid.
                    log_event(
                        f"  [LIVE] {sym}: notional below open-min ({why}) - "
                        f"attempting reduce-only close anyway (reduce-only is "
                        f"usually exempt).", "INFO"
                    )

                import uuid

                from bot_utils.trade_state import registry_order_guard
                client_order_id = "obx-" + uuid.uuid4().hex[:20]
                with registry_order_guard(state, sym, d) as ownership_live:
                    if not isinstance(ownership_live, dict):
                        return (sym, "failed", 0.0, "managed state unavailable")
                    if ownership_live.get("claim_conflict"):
                        return (sym, "failed", 0.0, "registry claim conflict")
                    order = create_order_with_retry(
                        ex, symbol_full, close_side, close_amount,
                        params=reduce_only_params(
                            position_side=(
                                "long" if pos_type == "LONG" else "short"
                            ),
                            # MUST mirror the normal close path
                            # (futures_bot_exits._execute_full_close): MEXC
                            # rejects an isolated/cross reduce-only order that
                            # omits leverage. margin_mode is threaded from the
                            # bot so CROSS/FUTURES send the right shape.
                            margin_mode=margin_mode,
                            leverage=(
                                max(1, int(__import__("math").ceil(lev)))
                                if lev else None
                            ),
                            client_order_id=client_order_id,
                        ),
                        shutdown_event=shutdown_event,
                        max_attempts=5,
                        action_label=f"emergency close {sym}",
                        log_event=log_event,
                        log_struct=log_struct,
                        abort_on_shutdown=False,
                    )

                exch_oid = (
                    order_id_text_or_none(order.get("id"))
                    or order_id_text_or_none(order.get("orderId"))
                )
                resolved, fill_source = _resolve_fill_price(
                    ex,
                    symbol_full,
                    order,
                    curr,
                    log_event,
                    expected_side=close_side,
                    expected_position_side=pos_type,
                    expected_client_id=client_order_id,
                    expected_amount=close_amount,
                )
                if resolved is not None and resolved > 0:
                    fill_price = resolved
                else:
                    fill_price = curr
                    fill_source = "fallback"

                if used_entry_fallback and fill_source == "fallback":
                    log_event(
                        f"  {sym}: fill_price could not be confirmed "
                        f"(source=fallback, entry-priced) - DB PnL "
                        f"reading will be ~0.0 but REAL P&L may differ "
                        f"significantly. Reconcile manually.", "WARN"
                    )

                close_fee = extract_or_estimate_futures_fee(
                    ex, order, symbol_full, fill_price,
                    amount=close_amount, contract_size=_cs,
                    shutdown_event=shutdown_event,
                )

                def _fee_for_verified_fragment(fragment_amount: float) -> float:
                    reported_fill = _positive_finite_or_zero(
                        order.get("filled") if isinstance(order, dict) else None
                    )
                    tolerance = max(1e-12, fragment_amount * 1e-9)
                    if (
                        reported_fill > 0
                        and abs(reported_fill - fragment_amount) <= tolerance
                    ) or (
                        reported_fill <= 0
                        and abs(raw_amount - fragment_amount) <= tolerance
                    ):
                        return close_fee
                    fragment_order = {
                        "id": exch_oid,
                        "filled": fragment_amount,
                        "amount": fragment_amount,
                    }
                    return extract_or_estimate_futures_fee(
                        ex,
                        fragment_order,
                        symbol_full,
                        fill_price,
                        amount=fragment_amount,
                        contract_size=_cs,
                        shutdown_event=shutdown_event,
                    )

                def _persist_verified_fragment(fragment_amount: float) -> bool:
                    fragment_fee = _fee_for_verified_fragment(fragment_amount)
                    from bot_utils.close_fragments import (
                        add_close_fragment_update,
                    )
                    fragment_update = add_close_fragment_update(
                        d,
                        amount=fragment_amount,
                        price=fill_price,
                        fee=fragment_fee,
                        order_id=exch_oid,
                    )
                    fragment_update["pending_close_reason"] = str(
                        reason or "Emergency Close Retry"
                    )
                    return _persist_emergency_fragment(
                        state=state,
                        sym=sym,
                        fields=fragment_update,
                        bot_name=bot_name,
                        log_event=log_event,
                        error_logger=error_logger,
                    )

                # verify_position_closed returns (closed, remaining).
                try:
                    closed_ok, remaining = verify_position_closed(
                        ex, symbol_full,
                        timeout=_VERIFY_CLOSE_TIMEOUT_SEC,
                        expected_position_side=pos_type,
                    )
                except Exception as ve:
                    closed_ok, remaining = False, -1.0
                    reported_fill = _positive_finite_or_zero(
                        order.get("filled") if isinstance(order, dict) else None
                    )
                    fragment_amount = min(raw_amount, reported_fill)
                    if fragment_amount > 0 and fill_price > 0:
                        if not _persist_verified_fragment(fragment_amount):
                            return (
                                sym,
                                "failed",
                                0.0,
                                "partial close fragment recovery marker "
                                "undurable",
                            )
                    log_event(
                        f"  [LIVE] {sym}: verify_position_closed raised "
                        f"{ve} - assuming NOT closed", "WARN"
                    )

                if not closed_ok:
                    if remaining > 0:
                        total_filled = max(0.0, amount - float(remaining))
                        fragment_amount = max(
                            0.0, total_filled - pending_fragment_amount
                        )
                        if fragment_amount > 0 and fill_price > 0:
                            if not _persist_verified_fragment(fragment_amount):
                                return (
                                    sym,
                                    "failed",
                                    0.0,
                                    "partial close fragment recovery marker "
                                    "undurable",
                                )
                        log_event(
                            f"  [LIVE] {sym}: partial close detected, "
                            f"{remaining:.6f} contracts still open. "
                            f"Position stays in state for retry.", "WARN"
                        )
                        return (sym, "failed", 0.0,
                                 f"partial close, {remaining:.6f} remaining")
                    else:
                        log_event(
                            f"  [LIVE] {sym}: could not verify close "
                            f"(API glitch). Position stays in state "
                            f"for next-tick reconciliation.", "WARN"
                        )
                        return (sym, "failed", 0.0, "verify failed")

            except Exception as e:
                if is_no_position_error(e):
                    try:
                        closed_ok, remaining = verify_position_closed(
                            ex, symbol_full,
                            timeout=_VERIFY_CLOSE_TIMEOUT_SEC,
                            expected_position_side=pos_type,
                        )
                    except Exception as ve:
                        closed_ok, remaining = False, -1.0
                        log_event(
                            f"  [LIVE] {sym}: close error looked flat but "
                            f"verification failed: {ve}", "WARN"
                        )
                    if closed_ok:
                        log_event(
                            f"  [LIVE] {sym}: position already flat on "
                            f"exchange ({str(e)[:80]}) - booking local close",
                            "WARN",
                        )
                        fill_source = "already_flat"
                    else:
                        if remaining > 0:
                            msg = f"{remaining:.6f} contracts remain"
                        else:
                            msg = "flat verification failed"
                        log_event(
                            f"  [LIVE] {sym}: no-position close error but "
                            f"{msg}; state kept for retry", "WARN"
                        )
                        return (sym, "failed", 0.0, msg)
                else:
                    log_event(f"  [LIVE] Emergency close {sym} could not complete: {e}", "WARN")
                    if error_logger:
                        try:
                            error_logger(f"emergency close {sym}", e)
                        except Exception:
                            pass
                    return (sym, "failed", 0.0, str(e))

        if fill_price is None or fill_price <= 0:
            fill_price = curr

        if not simulation and order is not None:
            from bot_utils.close_fragments import (
                add_close_fragment_update,
                pending_close_values,
            )
            final_fragment_amount = max(
                0.0, amount - pending_fragment_amount
            )
            reported_fill = _positive_finite_or_zero(order.get("filled"))
            if reported_fill > 0 and abs(
                reported_fill - final_fragment_amount
            ) > max(1e-12, final_fragment_amount * 1e-9):
                if reported_fill <= final_fragment_amount:
                    gap_update = add_close_fragment_update(
                        d,
                        amount=reported_fill,
                        price=fill_price,
                        fee=_fee_for_verified_fragment(reported_fill),
                        order_id=exch_oid,
                    )
                else:
                    gap_update = {}
                gap_update.update({
                    "pending_close_reason": str(
                        reason or "Emergency Close Fill Gap"
                    ),
                    "verified_flat_pending_accounting": True,
                    "verified_flat_reason": (
                        "Emergency close explicit fill evidence does not "
                        "explain verified-flat position"
                    ),
                    "verified_flat_at": _utc_now_str(),
                })
                _persist_emergency_fragment(
                    state=state,
                    sym=sym,
                    fields=gap_update,
                    bot_name=bot_name,
                    log_event=log_event,
                    error_logger=error_logger,
                )
                _block_emergency_fragment(bot_name, sym)
                log_event(
                    f"  {sym}: verified flat but explicit order fill evidence "
                    f"does not explain the remaining close amount; accounting "
                    f"and further emergency orders are blocked",
                    "ERROR",
                )
                return (
                    sym,
                    "failed",
                    0.0,
                    "final close fill evidence incomplete",
                )
            pending_view = dict(d)
            pending_view.update(add_close_fragment_update(
                d,
                amount=final_fragment_amount,
                price=fill_price,
                fee=_fee_for_verified_fragment(final_fragment_amount),
                order_id=exch_oid,
            ))
            aggregate_amount, aggregate_price, aggregate_fee, aggregate_oid = \
                pending_close_values(pending_view)
            if (
                aggregate_amount <= 0
                or aggregate_price <= 0
                or abs(aggregate_amount - amount) > max(1e-12, amount * 1e-9)
            ):
                return (
                    sym,
                    "failed",
                    0.0,
                    "close fragment accounting aggregate invalid",
                )
            fill_price = aggregate_price
            close_fee = aggregate_fee
            exch_oid = aggregate_oid or exch_oid
        elif not simulation and pending_fragment_amount > 0:
            return (
                sym,
                "failed",
                0.0,
                "position flat with incomplete close fragment accounting",
            )

        # PnL
        move_pct = (price_move_pct(entry, fill_price, pos_type)
                     if entry > 0 else 0.0)
        pnl_usdt, _ = (calc_unrealized_pnl(entry, fill_price, margin, lev, pos_type)
                        if entry > 0 and margin > 0 else (0.0, 0.0))

        initial_entry_fee = _positive_finite_or_zero(d.get(
            "initial_entry_fee", d.get("fees_paid", 0.0)))
        original_amount = _positive_finite_or_default(
            d.get("original_amount", amount), amount)
        from bot_utils import safe_proportional_fee, safe_remaining_funding
        partial_sold_flag = bool(d.get("partial_sold"))
        proportional_entry_fee = safe_proportional_fee(
            initial_entry_fee, amount, original_amount,
            partial_sold=partial_sold_flag
        )

        funding_pd = _finite_or_default(d.get("funding_paid", 0.0), 0.0)
        funding_booked = _finite_or_default(
            d.get("funding_booked_on_partials", 0.0), 0.0)
        if partial_sold_flag:
            funding_pd = safe_remaining_funding(
                funding_pd, amount, original_amount,
                partial_sold=True, booked_on_partials=funding_booked,
                booked_on_partials_known=(
                    d.get("funding_booked_on_partials_known") is True
                ),
            )
        funding_resolution_pending = False
        funding_window_unverified = (
            d.get("entry_funding_window_unverified") is True
            or d.get("accounting_pending_funding_unverified") is True
        )
        if not simulation:
            try:
                notional = margin * lev if margin > 0 else 0.0
                if partial_sold_flag and amount > 0 and original_amount > 0:
                    remaining_ratio = amount / original_amount
                    if 0 < remaining_ratio < 1:
                        notional = notional / remaining_ratio
                if funding_window_unverified:
                    realized = fetch_realized_funding(
                        ex,
                        symbol_full,
                        d.get("buy_time"),
                        notional_usdt=notional,
                    )
                else:
                    realized = fetch_or_estimate_funding(
                        ex, symbol_full, d.get("buy_time"),
                        notional_usdt=notional, pos_type=pos_type,
                        fallback_state_value=funding_pd,
                    )
                funding_resolution_pending = realized is None
                if realized is not None:
                    realized = _finite_or_default(realized, math.nan)
                    if not math.isfinite(realized):
                        realized = None
                        funding_resolution_pending = True
                if realized is not None:
                    if partial_sold_flag:
                        realized = safe_remaining_funding(
                            realized, amount, original_amount,
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
        profit_usdt = round(pnl_usdt - slice_fees - funding_pd, 2)

        buy_time = d.get("buy_time", _utc_now_str())
        sell_time = _utc_now_str()
        trade_reason = f"Emergency Close ({reason}) [fill_src={fill_source}]"
        mfe_pct = _finite_or_default(d.get("max_profit_pct"), move_pct)
        mae_pct = _finite_or_default(d.get("min_profit_pct"), move_pct)
        giveback_pct = max(0.0, mfe_pct - move_pct)
        pending_close = {
            "accounting_pending": True,
            "accounting_pending_reason": trade_reason,
            "accounting_pending_sell_price": fill_price,
            "accounting_pending_sell_time": sell_time,
            "accounting_pending_profit_pct": move_pct,
            "accounting_pending_profit_usdt": profit_usdt,
            "accounting_pending_mode_is_sim": simulation,
            "accounting_pending_fees_usdt": slice_fees,
            "accounting_pending_funding_paid": funding_pd,
            "accounting_pending_exchange_order_id": exch_oid,
            "accounting_pending_mfe_pct": mfe_pct,
            "accounting_pending_mae_pct": mae_pct,
            "accounting_pending_giveback_pct": giveback_pct,
            "accounting_pending_entry_quality_score": d.get(
                "entry_quality_score"),
            "accounting_pending_entry_quality_label": d.get(
                "entry_quality_label"),
            "accounting_pending_entry_quality_reasons": d.get(
                "entry_quality_reasons"),
        }
        if funding_resolution_pending:
            pending_close["accounting_pending_funding_unverified"] = True
        elif funding_window_unverified:
            pending_close["entry_funding_window_unverified"] = False
            pending_close["accounting_pending_funding_unverified"] = False
        try:
            pending_persisted = state.update_many(sym, pending_close)
        except Exception as state_err:
            pending_persisted = False
            if error_logger:
                try:
                    error_logger(
                        f"emergency accounting write-ahead {sym}", state_err
                    )
                except Exception:
                    pass
        if pending_persisted is False:
            log_event(
                f"  {sym}: verified emergency close was not booked because "
                f"its accounting recovery marker was not durable",
                "ERROR",
            )
            return (
                sym,
                "failed",
                0.0,
                "closed but accounting recovery marker undurable",
            )
        if funding_resolution_pending:
            log_event(
                f"  {sym}: verified emergency close kept for accounting "
                f"recovery because exact funding history is unavailable",
                "ERROR",
            )
            return (
                sym,
                "failed",
                0.0,
                "closed but exact funding accounting is pending",
            )
        accounting_ok = False
        accounting_error = None
        try:
            accounting_ok = bool(save_trade_db(
                bot_name=bot_name,
                mode_is_sim=simulation,
                symbol=sym,
                buy_price=entry, sell_price=fill_price,
                buy_time=buy_time, sell_time=sell_time,
                profit_pct=move_pct, profit_usdt=profit_usdt,
                invested_usdt=margin,
                reason=trade_reason,
                rsi_15m=d.get("rsi_15m"), rsi_1h=d.get("rsi_1h"),
                rsi_4h=d.get("rsi_4h"), change_pct=d.get("change_pct"),
                btc_trend=d.get("btc_trend"), fear_greed=d.get("fear_greed"),
                is_futures=True, position_type=pos_type,
                leverage=lev,
                liquidation_price=_finite_or_default(
                    d.get("liquidation_price", 0), 0.0),
                funding_paid=funding_pd,
                fees_usdt=slice_fees,
                exchange_order_id=exch_oid,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                giveback_pct=giveback_pct,
                entry_quality_score=d.get("entry_quality_score"),
                entry_quality_label=d.get("entry_quality_label"),
                entry_quality_reasons=d.get("entry_quality_reasons"),
                entry_id=d.get("entry_id"),
            ))
            if not accounting_ok:
                raise RuntimeError("save_trade_db returned False")
        except Exception as e:
            accounting_error = e
            log_event(
                f"  DB accounting for {sym} could not be saved after close: {e}. "
                f"State kept for reconcile/accounting recovery.", "WARN")

        if not accounting_ok:
            return (sym, "failed", 0.0,
                    f"closed but accounting failed: {accounting_error}")

        try:
            save_trade(
                log_dir=log_dir, symbol=sym,
                buy_price=entry, buy_time=buy_time,
                sell_price=fill_price, profit_pct=move_pct,
                profit_usdt=profit_usdt,
                reason=f"Emergency Close ({reason}) ({pos_type})"
            )
        except Exception as e:
            log_event(f"  File trade log for {sym} could not be saved: {e}", "WARN")

        try:
            log_sell(bot_name, sym, move_pct, profit_usdt,
                      f"Emergency Close | {pos_type} @ {lev}x")
        except Exception as e:
            log_event(f"  Sell log for {sym} could not be written: {e}", "WARN")

        restore_fields = {
            "accounting_already_booked": True,
            "accounting_booked_sell_time": sell_time,
            "accounting_booked_exchange_order_id": exch_oid,
            "accounting_booked_reason": f"Emergency Close ({reason})",
        }
        try:
            # Scope by bot: FUTURES + CROSS share futures_state; unscoped would
            # delete the other bot's dashboard row for the same base coin.
            remove_futures_state(sym, bot_name, mode_is_sim=simulation)
        except Exception as cleanup_error:
            if error_logger:
                try:
                    error_logger(
                        f"emergency remove_futures_state {sym}", cleanup_error
                    )
                except Exception:
                    pass
            try:
                keep = dict(restore_fields)
                keep["futures_state_cleanup_pending"] = True
                state.update_many(sym, keep)
            except Exception as state_error:
                if error_logger:
                    try:
                        error_logger(
                            f"emergency mark futures cleanup pending {sym}",
                            state_error,
                        )
                    except Exception:
                        pass
            log_event(
                f"  {sym}: emergency close was booked, but futures_state "
                f"cleanup failed; state kept for retry",
                "WARN",
            )
            return (
                sym,
                "failed",
                0.0,
                f"closed but futures_state cleanup failed: {cleanup_error}",
            )

        try:
            from bot_utils.trade_state import remove_with_restore_fields
            removed_state = remove_with_restore_fields(
                state, sym, restore_fields
            )
        except Exception as cleanup_error:
            removed_state = False
            try:
                state.update_many(sym, restore_fields)
            except Exception as state_error:
                if error_logger:
                    try:
                        error_logger(
                            f"emergency restore accounted state {sym}",
                            state_error,
                        )
                    except Exception:
                        pass
            if error_logger:
                try:
                    error_logger(
                        f"emergency remove accounted state {sym}",
                        cleanup_error,
                    )
                except Exception:
                    pass
        if not removed_state:
            log_event(
                f"  {sym}: emergency close was booked, but claim/state "
                f"cleanup failed; state kept for retry",
                "WARN",
            )
            return (
                sym,
                "failed",
                0.0,
                "closed but claim/state cleanup failed",
            )

        return (sym, "closed", profit_usdt, None)

    except Exception as e:
        if error_logger:
            try:
                error_logger(f"emergency_close {sym}", e)
            except Exception:
                pass
        return (sym, "failed", 0.0, str(e))


def emergency_close_all_futures(*,
                                  ex,
                                  state,
                                  bot_name: str,
                                  log_dir: str,
                                  simulation: bool,
                                  default_leverage: float,
                                  margin_mode: str = "isolated",
                                  shutdown_event,
                                  reason: str = "Shutdown",
                                  ticker_cache=None,
                                  telegram_token: Optional[str] = None,
                                  telegram_chat_id: Optional[str] = None,
                                  log_event: Callable,
                                  log_sell: Callable,
                                  log_struct: Optional[Callable] = None,
                                  save_trade_db: Callable,
                                  save_trade: Callable,
                                  send_telegram: Callable,
                                  remove_futures_state: Callable,
                                  reduce_only_params: Callable,
                                  error_logger: Optional[Callable] = None,
                                  max_parallel: int = _MAX_PARALLEL_CLOSES,
                                  ) -> dict:
    """Close ALL open futures positions cleanly on shutdown.

    Returns {"closed_count", "failed_count", "failed"} so the caller can decide
    whether to latch shutdown as complete or allow a retry of failed legs."""
    snapshot = state.get_all()
    if not snapshot:
        log_event("Emergency close: no open positions", "INFO")
        return {"closed_count": 0, "failed_count": 0, "failed": []}

    log_event(
        f"EMERGENCY CLOSE ALL ({reason}) - {len(snapshot)} positions "
        f"(parallel workers: {min(max_parallel, len(snapshot))})",
        "WARN"
    )

    closed_count = 0
    failed: list = []
    total_pnl = 0.0
    aggr_lock = Lock()

    worker_kwargs_common = dict(
        ex=ex, state=state, bot_name=bot_name, log_dir=log_dir,
        simulation=simulation, default_leverage=default_leverage,
        margin_mode=margin_mode,
        shutdown_event=shutdown_event, reason=reason,
        ticker_cache=ticker_cache,
        log_event=log_event, log_sell=log_sell, log_struct=log_struct,
        save_trade_db=save_trade_db, save_trade=save_trade,
        remove_futures_state=remove_futures_state,
        reduce_only_params=reduce_only_params,
        error_logger=error_logger,
    )

    workers = max(1, min(max_parallel, len(snapshot)))
    with ThreadPoolExecutor(max_workers=workers,
                               thread_name_prefix="emergency-close") as ex_pool:
        futures = {
            ex_pool.submit(_close_single_position,
                             sym=sym, d=d,
                             **worker_kwargs_common): sym
            for sym, d in snapshot.items()
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                result_sym, status, pnl, err = fut.result()
            except Exception as e:
                with aggr_lock:
                    failed.append(f"{sym}: {e}")
                log_event(f"Emergency close worker for {sym} crashed: {e}",
                           "WARN")
                continue
            with aggr_lock:
                if status == "closed":
                    closed_count += 1
                    total_pnl += pnl
                else:
                    failed.append(f"{result_sym}: {err or status}")

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
                f"[{bot_name}] EMERGENCY CLOSE INCOMPLETE\n"
                f"Incomplete: {', '.join(shown)}{extra}\n"
                f"Close MANUALLY on the exchange!"
            )
        except Exception as e:
            log_event(f"Telegram unavailable: {e}", "WARN")

    # Structured result so the shutdown handler can decide whether to LATCH
    # (fully flat  don't retry) or leave room for a repeat-signal/atexit retry
    # of the still-open legs.
    return {"closed_count": closed_count,
            "failed_count": len(failed),
            "failed": failed}
