"""
bot_utils/spot_exits.py  Spot market-sell + emergency-close helpers.
"""
from __future__ import annotations


from decimal import Decimal, ROUND_DOWN
from typing import Tuple, Callable, Optional

from bot_utils.order_utils import extract_fill_price, extract_order_fee

_EMERGENCY_RESIDUAL_DUST_USDT = 1.0


#  Helpers 

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
        return Decimal(str(mn))
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
        if prec is None:
            return None
        # CCXT precision can be either an int (decimal places) or
        # a Decimal-like step depending on the exchange.
        try:
            pf = float(prec)
        except (TypeError, ValueError):
            return None
        if pf >= 1 and pf == int(pf):
            # Likely "number of decimal places"  convert to step
            return Decimal(10) ** -int(pf)
        if pf > 0:
            return Decimal(str(pf))
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


def _free_base_balance(ex, symbol_pair: str):
    """Free balance of the BASE asset of ``symbol_pair`` (e.g. FET for
    FET/USDT), or None if it can't be read."""
    try:
        base = symbol_pair.split("/")[0]
        bal = ex.fetch_balance()
        free = (bal.get("free") or {}).get(base)
        if free is None:
            sub = bal.get(base)
            if isinstance(sub, dict):
                free = sub.get("free")
        return float(free) if free is not None else None
    except Exception:
        return None


def _filled_base_amount(order, wrapper_sold, requested_amount: float) -> float:
    requested = max(0.0, float(requested_amount or 0.0))
    if isinstance(order, dict):
        try:
            filled = float(order.get("filled") or 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        if filled > 0:
            return min(requested, filled)
    try:
        sold = float(wrapper_sold or 0.0)
    except (TypeError, ValueError):
        sold = 0.0
    if sold > 0:
        return min(requested, sold)
    return 0.0


def _apply_state_updates(state, sym: str, updates: dict) -> None:
    if hasattr(state, "update_many"):
        state.update_many(sym, updates)
    else:
        for key, value in updates.items():
            state.update(sym, key, value)


def _remove_accounted_state(state, sym: str, restore_fields: dict) -> bool:
    try:
        from bot_utils.trade_state import remove_with_restore_fields
        ok = bool(remove_with_restore_fields(state, sym, restore_fields))
    except Exception:
        if not hasattr(state, "remove"):
            raise
        try:
            result = state.remove(sym, restore_fields)
        except TypeError:
            result = state.remove(sym)
        ok = True if result is None else bool(result)
    if not ok:
        try:
            _apply_state_updates(state, sym, restore_fields)
        except Exception:
            pass
    return ok


def _order_has_open_remainder(order) -> bool:
    if not isinstance(order, dict):
        return False
    status = str(order.get("status") or "").strip().lower()
    if status in ("closed", "canceled", "cancelled", "expired", "rejected"):
        return False
    if status in ("open", "new", "partially_filled", "partiallyfilled"):
        return True
    if status:
        return False
    try:
        return float(order.get("remaining") or 0.0) > 0
    except (TypeError, ValueError):
        return False


def _emergency_residual_amount(ex, symbol_pair: str, requested_amount: float,
                               sold_amount: float, fill_price: float,
                               order=None) -> float:
    requested = max(0.0, float(requested_amount or 0.0))
    sold = max(0.0, float(sold_amount or 0.0))
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
        residual = min(residual, float(free))
    if residual * fill_price <= _EMERGENCY_RESIDUAL_DUST_USDT:
        return 0.0
    return min(residual, requested)


def _emergency_residual_updates(amount: float, residual_amount: float,
                                margin: float, initial_entry_fee: float,
                                proportional_entry_fee: float, exch_oid,
                                reason: str) -> dict:
    remaining_invested = (
        margin * (residual_amount / amount)
        if amount > 0 else 0.0
    )
    remaining_entry_fee = max(0.0, initial_entry_fee - proportional_entry_fee)
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
    }


def spot_market_sell_safe(ex, symbol_pair: str, raw_amount: float
                          ) -> Tuple[dict, float]:
    """Wraps create_market_sell_order with precision rounding.

    The step is derived from ``markets[symbol].limits.amount.min`` or
    ``markets[symbol].precision.amount`` (so coins with batch sizes >= 1 like
    many meme-coin listings work), falling back to 0.0001 only as a last resort.
    """
    rounded: float
    try:
        rounded = float(ex.amount_to_precision(symbol_pair, raw_amount))
    except Exception:
        # Prefer market-metadata-derived step
        step = (_exchange_precision_step(ex, symbol_pair)
                 or _exchange_min_amount(ex, symbol_pair)
                 or Decimal("0.0001"))
        amt_dec = Decimal(str(raw_amount))
        try:
            rounded = float(_round_to_step(amt_dec, step))
        except Exception:
            rounded = float(amt_dec.quantize(Decimal("0.0001"),
                                              rounding=ROUND_DOWN))

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
    raw_dec = Decimal(str(raw_amount))
    _balance_capped = False   # re-read free balance at most once on Oversold
    for step in steps_to_try:
        if step is None:
            amt = rounded
        else:
            amt = float(_round_to_step(raw_dec, step))
        if amt <= 0:
            continue
        try:
            order = ex.create_market_sell_order(symbol_pair, amt)
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
                if free is not None and free > 0:
                    capped = free
                    try:
                        capped = float(ex.amount_to_precision(symbol_pair, free))
                    except Exception:
                        pass
                    min_amt = _exchange_min_amount(ex, symbol_pair)
                    sellable = (capped > 0 and (min_amt is None
                                or Decimal(str(capped)) >= min_amt))
                    if sellable and capped < amt:
                        try:
                            order = ex.create_market_sell_order(symbol_pair, capped)
                            return order, capped
                        except Exception as e2:
                            last_exc = e2
                raise InsufficientSellBalance(symbol_pair, requested=amt,
                                              free=(free or 0.0))
            # 3. Any other error  propagate immediately.
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

            if not state.has(sym):
                continue

            try:
                buy_price = float(d.get("buy", 0))
                amount = float(d.get("amount", 0))
                margin = float(d.get("invested_usdt", 0))
                symbol_pair = f"{sym}/USDT"

                curr = 0.0
                try:
                    ticker = ex.fetch_ticker(symbol_pair)
                    curr = float(ticker.get("last") or ticker.get("close") or 0)
                except Exception as e:
                    log_event(f"  Price for {sym} unavailable: {e}", "WARN")
                if curr <= 0:
                    curr = buy_price  # fallback

                profit_pct = ((curr - buy_price) / buy_price * 100) if buy_price > 0 else 0.0

                initial_entry_fee = float(d.get("initial_entry_fee",
                                                  d.get("fees_paid", 0.0)))
                original_amount = float(d.get("original_amount", amount))
                from bot_utils import safe_proportional_fee
                proportional_entry_fee = safe_proportional_fee(
                    initial_entry_fee, amount, original_amount,
                    partial_sold=bool(d.get("partial_sold"))
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
                if simulation and amount > 0 and fill_price > 0:
                    close_fee = sold_amount * fill_price * 0.001
                    profit_usdt = round(
                        sold_amount * (fill_price - buy_price)
                        - proportional_entry_fee - close_fee,
                        2
                    )

                if not simulation and amount > 0:
                    try:
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
                            return spot_market_sell_safe(ex, _pair, sell_amt)
                        order, _sold = with_network_retry(
                            operation=_sell_capped,
                            action_label=f"emergency sell {sym}",
                            max_attempts=3,
                            base_delay=0.5,
                            shutdown_event=None,
                            log_event=log_event,
                        )
                        from bot_utils.order_utils import order_was_filled
                        # Verify the order ACTUALLY filled  MEXC can return an
                        # order object that never executed (status new/open,
                        # filled=0, e.g. remaining size below min-notional);
                        # booking it as sold would write a phantom closed trade.
                        if not order_was_filled(order, _sold,
                                                min_fill_ratio=1e-9):
                            failed.append(f"{sym}: order not filled "
                                          f"(status={order.get('status') if isinstance(order, dict) else '?'})")
                            log_event(
                                f"  [LIVE] {sym}: sell order did NOT fill "
                                f"(still in wallet)  sell MANUALLY!", "WARN")
                            continue
                        exch_oid = order.get("id") or order.get("orderId")
                        sold_amount = _filled_base_amount(order, _sold, amount)
                        fill_price = extract_fill_price(order, curr)
                        proportional_entry_fee = safe_proportional_fee(
                            initial_entry_fee, sold_amount, original_amount,
                            partial_sold=bool(d.get("partial_sold"))
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
                    except Exception as e:
                        failed.append(f"{sym}: {e}")
                        log_event(f"  [LIVE] Sell {sym} FAILED: {e}", "WARN")
                        continue

                buy_time = d.get("buy_time", _utc_now_str())
                sell_time = _utc_now_str()
                fees_for_booked_slice = proportional_entry_fee + close_fee
                try:
                    accounting_ok = bool(save_trade_db(
                        bot_name=bot_name,
                        mode_is_sim=simulation,
                        symbol=sym,
                        buy_price=buy_price, sell_price=fill_price,
                        buy_time=buy_time, sell_time=sell_time,
                        profit_pct=profit_pct, profit_usdt=profit_usdt,
                        invested_usdt=booked_invested,
                        reason=f"Emergency Close ({reason})",
                        rsi_15m=d.get("rsi_15m"), rsi_1h=d.get("rsi_1h"),
                        rsi_4h=d.get("rsi_4h"), change_pct=d.get("change_pct"),
                        btc_trend=d.get("btc_trend"), fear_greed=d.get("fear_greed"),
                        is_futures=False,
                        fees_usdt=fees_for_booked_slice
                    ))
                    if not accounting_ok:
                        raise RuntimeError("save_trade_db returned False")
                    save_trade(
                        log_dir=log_dir, symbol=sym,
                        buy_price=buy_price, buy_time=buy_time,
                        sell_price=fill_price, profit_pct=profit_pct,
                        profit_usdt=profit_usdt,
                        reason=f"Emergency Close ({reason})"
                    )
                    log_sell(bot_name, sym, profit_pct, profit_usdt, "Emergency Close")
                except Exception as e:
                    log_event(
                        f"  DB accounting for {sym} failed after close: {e}. "
                        f"State kept for recovery.", "WARN")
                    try:
                        residual_amount = (
                            _emergency_residual_amount(
                                ex, symbol_pair, amount, sold_amount,
                                fill_price, order=order)
                            if not simulation and amount > 0 else 0.0
                        )
                        if residual_amount > 0:
                            pending_trade = dict(
                                bot_name=bot_name, symbol=sym,
                                buy_price=buy_price, sell_price=fill_price,
                                buy_time=buy_time, sell_time=sell_time,
                                profit_pct=profit_pct,
                                profit_usdt=profit_usdt,
                                invested_usdt=booked_invested,
                                reason=f"Emergency Close ({reason})",
                                rsi_15m=d.get("rsi_15m"),
                                rsi_1h=d.get("rsi_1h"),
                                rsi_4h=d.get("rsi_4h"),
                                change_pct=d.get("change_pct"),
                                btc_trend=d.get("btc_trend"),
                                fear_greed=d.get("fear_greed"),
                                is_futures=False,
                                is_partial=True,
                                fees_usdt=fees_for_booked_slice,
                                exchange_order_id=exch_oid,
                            )
                            pending = list(
                                d.get("accounting_pending_partials") or [])
                            pending.append(pending_trade)
                            updates = _emergency_residual_updates(
                                amount, residual_amount, margin,
                                initial_entry_fee, proportional_entry_fee,
                                exch_oid, reason)
                            updates["accounting_pending_partials"] = pending
                            _apply_state_updates(state, sym, updates)
                            log_event(
                                f"  [LIVE] {sym}: sold slice accounting pending; "
                                f"{residual_amount:.6f} kept in state for retry",
                                "WARN")
                        else:
                            updates = {
                                "accounting_pending": True,
                                "accounting_pending_reason": f"Emergency Close ({reason})",
                                "accounting_pending_sell_price": fill_price,
                                "accounting_pending_sell_time": sell_time,
                                "accounting_pending_profit_pct": profit_pct,
                                "accounting_pending_profit_usdt": profit_usdt,
                                "accounting_pending_fees_usdt": fees_for_booked_slice,
                                "accounting_pending_exchange_order_id": exch_oid,
                            }
                            _apply_state_updates(state, sym, updates)
                    except Exception as se:
                        log_event(
                            f"  Could not mark {sym} accounting_pending: {se}",
                            "WARN")
                    failed.append(f"{sym}: accounting failed after close")
                    continue

                # A balance-capped emergency sell can partial-fill. Keep a
                # meaningful remainder in state with its original cost basis.
                if not simulation and amount > 0:
                    residual_amount = _emergency_residual_amount(
                        ex, symbol_pair, amount, sold_amount, fill_price,
                        order=order)
                    if residual_amount > 0:
                        updates = _emergency_residual_updates(
                            amount, residual_amount, margin,
                            initial_entry_fee, proportional_entry_fee,
                            exch_oid, reason)
                        try:
                            _apply_state_updates(state, sym, updates)
                        except Exception as se:
                            failed.append(
                                f"{sym}: residual state update failed: {se}")
                            log_event(
                                f"  [LIVE] {sym}: residual state update failed "
                                f"({se}) - NOT removing state", "WARN")
                            continue
                        log_event(
                            f"  [LIVE] {sym}: {residual_amount:.6f} still open after "
                            f"emergency sell (>5% of {amount:.6f}) - kept in "
                            f"state for retry/reconcile.",
                            "WARN")
                        failed.append(
                            f"{sym}: residual kept in state ({residual_amount:.6f})")
                        total_pnl += profit_usdt
                        continue

                removed = _remove_accounted_state(state, sym, {
                    "accounting_already_booked": True,
                    "accounting_booked_sell_time": sell_time,
                    "accounting_booked_exchange_order_id": exch_oid,
                    "accounting_booked_reason": f"Emergency Close ({reason})",
                })
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
