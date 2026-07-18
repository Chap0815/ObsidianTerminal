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
from threading import Lock
from typing import Callable, Optional, Tuple

from bot_utils.futures_order import (
    create_order_with_retry,
    extract_or_estimate_futures_fee,
    futures_contract_size,
    is_no_position_error,
    verify_position_closed,
)
from bot_utils.futures_math import calc_unrealized_pnl, price_move_pct
from bot_utils.futures_funding import fetch_or_estimate_funding
from bot_utils.fee_math import taker_fee_rate


_MAX_PARALLEL_CLOSES = 3
_FILL_RESOLVE_DELAY_SEC = 0.8
_FILL_RESOLVE_MAX_RETRIES = 2
_VERIFY_CLOSE_TIMEOUT_SEC = 5.0
_FALLBACK_MIN_NOTIONAL = 5.0


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
    oid = str(order_id or "").strip()
    if not oid:
        return False
    if _is_mexc_swap_symbol(ex, symbol_full):
        return oid.isdigit()
    return True


def _resolve_fill_price(ex,
                          symbol_full: str,
                          order: dict,
                          fallback_price: float,
                          log_event: Callable) -> Tuple[float, str]:
    """Multi-stage fill-price recovery  returns (price, source_label)."""
    fp = _extract_fill_from_order(order)
    if fp is not None:
        return fp, "order"

    order_id = order.get("id") if isinstance(order, dict) else None

    if _order_id_is_fetchable(ex, symbol_full, order_id):
        for attempt in range(_FILL_RESOLVE_MAX_RETRIES):
            try:
                time.sleep(_FILL_RESOLVE_DELAY_SEC * (1 + attempt))
                fetched = ex.fetch_order(order_id, symbol_full)
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
        trades = ex.fetch_my_trades(symbol_full, limit=10) or []
        if isinstance(trades, list) and trades:
            same_order = [t for t in trades
                            if order_id and str(t.get("order") or "") == str(order_id)]
            if order_id and not same_order:
                return _positive_finite_or_zero(fallback_price), "fallback"
            pool = same_order if order_id else trades
            try:
                pool = sorted(pool,
                               key=lambda t: t.get("timestamp") or 0,
                               reverse=True)
            except Exception:
                pass
            for t in pool:
                v = t.get("price")
                try:
                    fv = float(v)
                    if math.isfinite(fv) and fv > 0:
                        return fv, "trades"
                except (TypeError, ValueError, OverflowError):
                    continue
    except Exception as e:
        try:
            log_event(
                f"  fetch_my_trades for fill price unavailable: {e}", "WARN"
            )
        except Exception:
            pass

    return _positive_finite_or_zero(fallback_price), "fallback"


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
    with close_lock(sym, timeout=15.0, bot_name=bot_name,
                    fail_open=True) as got:
        if not got:
            return _flatten_without_accounting(**kw)
        if got and state is not None:
            try:
                if not state.has(sym):
                    return (sym, "closed", 0.0, None)
            except Exception:
                pass
        return _close_single_position_impl(**kw)


def _flatten_without_accounting(**kw):
    sym = kw.get("sym")
    d = kw.get("d") or {}
    ex = kw.get("ex")
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
            if close_amount is None:
                return (sym, "failed", 0.0,
                        "close lock held and close amount invalid")
        except Exception:
            close_amount = round(amount, 4)
        if close_amount <= 0 and amount > 0:
            close_amount = round(amount, 4)
        if not math.isfinite(close_amount) or close_amount <= 0:
            return (sym, "failed", 0.0,
                    "close lock held and close amount invalid")
        params = reduce_only_params(
            position_side=("long" if pos_type == "LONG" else "short"),
            margin_mode=margin_mode,
            leverage=max(1, int(__import__("math").ceil(lev))),
        )
        create_order_with_retry(
            ex, symbol_full, side, close_amount, params=params,
            shutdown_event=shutdown_event, max_attempts=5,
            action_label=f"emergency flatten {sym}",
            log_event=log_event, abort_on_shutdown=False,
        )
        closed_ok, remaining = verify_position_closed(
            ex, symbol_full, timeout=_VERIFY_CLOSE_TIMEOUT_SEC)
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

        # Price fallback chain
        curr = 0.0
        if ticker_cache is not None:
            try:
                ticker = ticker_cache.get(ex, symbol_full, timeout=5.0)
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
                ticker = ex.fetch_ticker(symbol_full)
                curr = _positive_finite_or_zero(ticker.get("last"))
                if curr <= 0:
                    curr = _positive_finite_or_zero(ticker.get("close"))
            except Exception as e2:
                log_event(f"  Price (direct) for {sym} unavailable: {e2}", "WARN")
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
        # contractSize-aware fee math  needed for contract_size != 1 coins
        # (1000SATS, MEME, ).
        _cs = futures_contract_size(ex, symbol_full)

        if simulation:
            fill_price = curr
            fill_source = "simulation"
            if amount > 0 and fill_price > 0:
                close_fee = amount * _cs * fill_price * taker_fee_rate(
                    ex, symbol_full)
        else:
            try:
                close_side = "sell" if pos_type == "LONG" else "buy"
                raw_amount = amount
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

                order = create_order_with_retry(
                    ex, symbol_full, close_side, close_amount,
                    params=reduce_only_params(
                        position_side=("long" if pos_type == "LONG" else "short"),
                        # MUST mirror the normal close path
                        # (futures_bot_exits._execute_full_close): MEXC rejects an
                        # isolated/cross reduce-only order that omits leverage
                        # ("unexpected keyword argument 'leverage'" / margin
                        # errors) and the position then stays OPEN  exactly the
                        # "close manually" failure this emergency path is meant to
                        # prevent. margin_mode is threaded from the bot so CROSS
                        # (cross) and FUTURES (isolated) each send the right shape.
                        margin_mode=margin_mode,
                        leverage=max(1, int(__import__("math").ceil(lev))) if lev else None,
                    ),
                    shutdown_event=shutdown_event,
                    max_attempts=5,
                    action_label=f"emergency close {sym}",
                    log_event=log_event,
                    log_struct=log_struct,
                    abort_on_shutdown=False,
                )

                exch_oid = order.get("id") or order.get("orderId")
                resolved, fill_source = _resolve_fill_price(
                    ex, symbol_full, order, curr, log_event
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

                # verify_position_closed returns (closed, remaining).
                try:
                    closed_ok, remaining = verify_position_closed(
                        ex, symbol_full,
                        timeout=_VERIFY_CLOSE_TIMEOUT_SEC,
                    )
                except Exception as ve:
                    closed_ok, remaining = False, -1.0
                    log_event(
                        f"  [LIVE] {sym}: verify_position_closed raised "
                        f"{ve} - assuming NOT closed", "WARN"
                    )

                if not closed_ok:
                    if remaining > 0:
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
            )
        if not simulation:
            try:
                notional = margin * lev if margin > 0 else 0.0
                if partial_sold_flag and amount > 0 and original_amount > 0:
                    remaining_ratio = amount / original_amount
                    if 0 < remaining_ratio < 1:
                        notional = notional / remaining_ratio
                realized = fetch_or_estimate_funding(
                    ex, symbol_full, d.get("buy_time"),
                    notional_usdt=notional, pos_type=pos_type,
                    fallback_state_value=funding_pd,
                )
                if realized is not None:
                    if partial_sold_flag:
                        realized = safe_remaining_funding(
                            realized, amount, original_amount,
                            partial_sold=True,
                            booked_on_partials=funding_booked,
                        )
                    funding_pd = realized
            except Exception:
                pass

        slice_fees = proportional_entry_fee + close_fee
        profit_usdt = round(pnl_usdt - slice_fees - funding_pd, 2)

        buy_time = d.get("buy_time", _utc_now_str())
        sell_time = _utc_now_str()
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
                reason=f"Emergency Close ({reason}) [fill_src={fill_source}]",
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
            ))
            if not accounting_ok:
                raise RuntimeError("save_trade_db returned False")
        except Exception as e:
            accounting_error = e
            log_event(
                f"  DB accounting for {sym} could not be saved after close: {e}. "
                f"State kept for reconcile/accounting recovery.", "WARN")
            try:
                state.update_many(sym, {
                    "accounting_pending": True,
                    "accounting_pending_reason": (
                        f"Emergency Close ({reason}) [fill_src={fill_source}]"
                    ),
                    "accounting_pending_sell_price": fill_price,
                    "accounting_pending_sell_time": sell_time,
                    "accounting_pending_profit_pct": move_pct,
                    "accounting_pending_profit_usdt": profit_usdt,
                    "accounting_pending_mode_is_sim": simulation,
                    "accounting_pending_fees_usdt": slice_fees,
                    "accounting_pending_funding_paid": funding_pd,
                    "accounting_pending_exchange_order_id": exch_oid,
                })
            except Exception as state_err:
                log_event(
                    f"  Could not mark {sym} accounting_pending: "
                    f"{state_err}", "WARN")

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

        try:
            # Scope by bot: FUTURES + CROSS share futures_state; unscoped would
            # delete the other bot's dashboard row for the same base coin.
            remove_futures_state(sym, bot_name, mode_is_sim=simulation)
        except Exception:
            pass
        try:
            from bot_utils.trade_state import remove_with_restore_fields
            remove_with_restore_fields(state, sym, {
                "accounting_already_booked": True,
                "accounting_booked_sell_time": sell_time,
                "accounting_booked_exchange_order_id": exch_oid,
                "accounting_booked_reason": f"Emergency Close ({reason})",
            })
        except KeyError:
            pass
        except Exception:
            pass

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
