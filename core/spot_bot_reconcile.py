"""
core/spot_bot_reconcile.py  Reconciliation logic for SpotBot.

Contains:
  startup_reconciliation(bot)  boot-time exchangestate alignment
  ReconcileMixin  periodic reconcile-thread body

Both compare local trades.json against actual exchange holdings and
handle the four crash/manual-interaction scenarios:
  1. Local has it, exchange doesn't  remove (phantom position)
  2. Local has X, exchange has < X  adjust amount
  3. Local has X, exchange has > X  log warning (manual buy)
  4. SQLite has it, JSON doesn't  rehydrate (ghost recovery)
"""
from __future__ import annotations

import time


def _record_spot_offline_close(bot, sym: str, state_row: dict) -> bool:
    """Spot equivalent of FuturesReconcileMixin._record_offline_close.

    Called when reconciliation finds a coin in trades.json but no
    matching exchange balance  meaning the position was closed while
    the bot was offline (manual sell, hard SL via exchange OCO, or
    some other external action).

    Writes a trade record to the DB so the realized PnL shows up in
    dashboards. Uses ``fetch_my_trades`` to find the actual sell price,
    falls back to current ticker if needed.

    Returns True only after DB accounting succeeded or was already present.
    Reconcile must keep state on False so the next cycle can retry.
    """
    from core.logger import log_event, save_trade, send_telegram
    from core.database import save_trade_db
    from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

    try:
        buy = float(state_row.get("buy", 0) or 0)
        amount = float(state_row.get("amount", 0) or 0)
        buy_time = state_row.get("buy_time", "")
        invested = float(state_row.get("invested_usdt",
                                          buy * amount if buy > 0 else 0))
        entry_fee = float(state_row.get("fees_paid",
                              state_row.get("initial_entry_fee", 0)) or 0)
        if buy <= 0 or amount <= 0:
            log_event(
                f" {sym}: offline-close skipped  invalid state "
                f"(buy={buy}, amount={amount})", "WARN"
            )
            return False

        pair = f"{sym}/USDT"

        close_price = 0.0
        close_source = "estimate"
        pending_accounting = bool(state_row.get("accounting_pending"))
        if pending_accounting:
            try:
                close_price = float(
                    state_row.get("accounting_pending_sell_price") or 0)
                if close_price > 0:
                    close_source = "accounting_pending"
            except (TypeError, ValueError):
                close_price = 0.0

        # Try to find the actual sell price from order history
        try:
            if close_price <= 0 and hasattr(bot.ex, "fetch_my_trades"):
                trades = bot.ex.fetch_my_trades(pair, limit=20) or []
                for t in reversed(trades):
                    side = t.get("side", "").lower()
                    if side == "sell":
                        close_price = float(t.get("price", 0) or 0)
                        close_source = "fetch_my_trades"
                        break
        except Exception:
            pass

        if close_price <= 0:
            try:
                ticker = bot.ex.fetch_ticker(pair)
                close_price = float(ticker.get("last", 0) or 0)
                close_source = "current_ticker"
            except Exception:
                pass

        if close_price <= 0:
            log_event(
                f" {sym}: cannot determine offline-close price  "
                f"skipping DB record. State kept for retry.", "WARN"
            )
            return False

        # Calculate net PnL: gross gain on amount minus fees
        # Spot taker fee  0.1% on Bitget
        from bot_utils import safe_proportional_fee
        SPOT_TAKER_FEE = 0.001
        close_fee = amount * close_price * SPOT_TAKER_FEE
        initial_entry_fee = float(state_row.get(
            "initial_entry_fee", state_row.get("fees_paid", 0)) or 0)
        original_amount = float(state_row.get(
            "original_amount", state_row.get("amount", 0)) or 0)
        entry_fee = safe_proportional_fee(
            initial_entry_fee, amount, original_amount,
            partial_sold=bool(state_row.get("partial_sold")),
        )
        gross = amount * (close_price - buy)
        net_pnl = round(gross - entry_fee - close_fee, 4)
        profit_pct = ((close_price - buy) / buy) * 100 if buy > 0 else 0.0

        from core.clock import now_utc
        sell_time = str(
            state_row.get("accounting_pending_sell_time")
            or now_utc().strftime("%Y-%m-%d %H:%M:%S"))
        reason = str(
            state_row.get("accounting_pending_reason")
            or f"Offline close ({close_source})")
        if pending_accounting and close_source == "accounting_pending":
            try:
                net_pnl = float(state_row.get("accounting_pending_profit_usdt"))
            except (TypeError, ValueError):
                pass
            try:
                profit_pct = float(state_row.get("accounting_pending_profit_pct"))
            except (TypeError, ValueError):
                pass
            try:
                fees_total = float(state_row.get("accounting_pending_fees_usdt"))
                entry_fee = 0.0
                close_fee = fees_total
            except (TypeError, ValueError):
                pass

        saved = save_trade_db(
            bot_name=bot.BOT_NAME, symbol=sym,
            buy_price=buy, sell_price=close_price,
            buy_time=buy_time, sell_time=sell_time,
            profit_pct=profit_pct, profit_usdt=net_pnl,
            invested_usdt=invested,
            reason=reason,
            is_futures=False,
            fees_usdt=entry_fee + close_fee,
            exchange_order_id=state_row.get("accounting_pending_exchange_order_id"),
        )
        if not saved:
            log_event(
                f" {sym}: offline-close DB save failed  state kept "
                f"for accounting retry.", "WARN")
            return False

        log_event(
            f" {sym}: offline-close recorded "
            f"({buy:.6f}  {close_price:.6f}, "
            f"PnL={net_pnl:+.2f} USDT, source={close_source})",
            "WARN"
        )
        try:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                f" [{bot.BOT_NAME}] Offline close detected\n"
                f"{sym}: {buy:.6f}  {close_price:.6f}\n"
                f"PnL: {net_pnl:+.2f} USDT\n"
                f"Source: {close_source}"
            )
        except Exception:
            pass
        return True
    except Exception as e:
        try:
            bot._log_error(f"spot-offline-close {sym}", e)
        except Exception:
            pass
        return False


def _is_fresh_position(state_row, max_age_s: float) -> bool:
    """True if the position was opened within max_age_s (M-7).

    A just-bought spot position's exchange balance may not have propagated yet
    (200ms2s on some venues), so a drift-shrink against the lagging balance
    would WRONGLY shrink a live position. Skipping the shrink for one reconcile
    interval lets the balance settle; a real external partial-sell is adjusted
    on the next cycle once the position is older. Missing/unparseable buy_time
    treated as NOT fresh (don't block the normal adjust)."""
    bt = state_row.get("buy_time", "")
    if not bt:
        return False
    from datetime import datetime, timezone
    try:
        opened = datetime.strptime(str(bt), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    return (datetime.now(timezone.utc) - opened).total_seconds() < max_age_s


def _still_held_on_spot_exchange(bot, sym: str, dust_usdt: float = 1.0) -> bool:
    """Authoritative single-coin balance re-fetch before an irreversible
    offline-close (M-6 parity with the futures reconcile). Returns True if the
    coin is still held (removal must be DEFERRED). Fail-safe: any fetch error
    returns True  never book a close we couldn't confirm. Sub-dust holdings
    count as not-held (matches the $1 spot dust filter)."""
    try:
        bal = bot.ex.fetch_balance()
        total = float((bal.get(sym) or {}).get("total", 0) or 0)
    except Exception:
        return True
    if total <= 0:
        return False
    try:
        px = float((bot.ex.fetch_ticker(f"{sym}/USDT") or {}).get("last") or 0)
        if px > 0 and total * px < dust_usdt:
            return False
    except Exception:
        pass
    return True


def _gate_missing_for_removal(bot, sym, state_row, strikes, threshold: int = 2) -> bool:
    """Strike-gated 'coin missing from exchange' decision, shared by BOTH the
    startup and the periodic spot reconcile so neither can drop a live position
    on a single transient/partial zero-balance read.

    Increments the consecutive-miss counter for ``sym``; only once it reaches
    ``threshold`` misses does it remove the position AND book the offline-close.
    Returns True iff it removed. A first miss merely seeds the counter (startup
    therefore defers to the next periodic cycle for confirmation).

    Mirrors the futures M-6 guards before the irreversible booking: take the
    per-symbol close_lock, re-check live state (the monitor may have closed+booked
    this leg meanwhile), and do an authoritative per-coin balance re-fetch  so a
    transient zero-balance read can't double-book the close or free a live claim.
    """
    strikes[sym] = strikes.get(sym, 0) + 1
    if strikes[sym] < threshold:
        return False
    from core.symbol_locks import close_lock
    from core.logger import log_event
    with close_lock(sym, bot_name=bot.BOT_NAME) as got:
        if not got:
            return False  # contended (monitor closing)  retry next cycle
        if not bot.state.has(sym):
            strikes.pop(sym, None)
            return False                      # monitor already booked + removed it
        if _still_held_on_spot_exchange(bot, sym):
            strikes.pop(sym, None)
            log_event(
                f" Spot reconciliation: {sym} present on authoritative balance "
                f"re-fetch  NOT booking offline-close (transient zero read)",
                "WARN")
            return False
        strikes.pop(sym, None)
        if not _record_spot_offline_close(bot, sym, state_row):
            log_event(
                f" Spot reconciliation: {sym} missing on exchange but "
                f"offline-close accounting failed  state kept for retry",
                "WARN")
            return False
        bot.state.remove(sym)
        return True


# 
# Startup reconciliation (called once from SpotBot.run())
# 

def startup_reconciliation(bot) -> None:
    """Boot-time spot reconciliation.

    Cases handled (per balanced/aggressive original):
      Local says we hold X coins, exchange shows 0  phantom, remove.
      Local says we hold X, exchange shows < X  drift, adjust amount.
      Local says we hold X, exchange shows > X  manual buy, log only.
      SQLite has position, JSON doesn't  rehydrate (ghost recovery).
    """
    from core.logger import log_event

    trades = bot.state.get_all()

    bal_data = None
    try:
        bal_data = bot.ex.fetch_balance()
    except Exception as e:
        bot._log_error("startup reconciliation fetch_balance", e)
        return

    if not isinstance(bal_data, dict):
        return

    if trades:
        # Safety gate: if we hold positions but the balance read shows NONE of
        # them, treat it as an empty/partial fetch and skip removing anything.
        present = sum(1 for sym in trades
                      if float((bal_data.get(sym) or {}).get("total", 0) or 0) > 1e-8)
        if present == 0:
            log_event(
                f" Spot reconciliation SKIPPED: fetch_balance shows none of "
                f"{len(trades)} held coin(s)  likely an empty/partial balance read. "
                f"NOT wiping state.", "WARN")
            return

        strikes = getattr(bot, "_recon_missing_strikes", None)
        if strikes is None:
            strikes = bot._recon_missing_strikes = {}

        removed = []
        adjusted = []
        for sym, d in list(trades.items()):
            try:
                local_amt = float(d.get("amount", 0))
                coin_bal = bal_data.get(sym) or {}
                exch_amt = float(coin_bal.get("total", 0) or 0)
                if exch_amt >= 1e-8:
                    strikes.pop(sym, None)
                if exch_amt < 1e-8 and local_amt > 0:
                    # Do NOT remove on a single startup read  it may be a
                    # partial/transient balance fetch. Seed the shared strike
                    # counter; the periodic reconcile confirms (2 consecutive
                    # misses) before removing + booking the offline-close.
                    if _gate_missing_for_removal(bot, sym, dict(d), strikes):
                        removed.append(sym)
                elif (exch_amt < local_amt * 0.95
                      and not _is_fresh_position(d, bot.RECONCILE_INTERVAL_SEC)):
                    # Skip shrinking a just-opened position whose balance may
                    # not have propagated yet (M-7).
                    adjusted.append(f"{sym}: {local_amt:.6f}{exch_amt:.6f}")
                    bot.state.update(sym, "amount", exch_amt)
            except (TypeError, ValueError):
                continue

        if removed:
            log_event(
                f" Spot reconciliation: removed {len(removed)} "
                f"phantom position(s): {', '.join(removed)}", "WARN"
            )
        if adjusted:
            log_event(
                f" Spot reconciliation: adjusted amounts: "
                f"{', '.join(adjusted)}", "WARN"
            )
        if not removed and not adjusted:
            log_event(
                f" Spot reconciliation: {bot.state.count()} position(s) "
                f"in sync with exchange", "INFO"
            )

    #  Reverse ghost detection (SQLite has it, JSON doesn't) 
    try:
        from core.database import get_open_positions_db
        import json as _json
        db_positions = get_open_positions_db(bot.BOT_NAME)
        rehydrated = []
        current_syms = set(bot.state.keys())
        for pos in db_positions:
            sym = pos.get("symbol", "")
            if not sym or sym in current_syms:
                continue
            # Verify the coin actually exists on exchange
            exch_check = (bal_data or {}).get(sym) or {}
            exch_amt = float(exch_check.get("total", 0) or 0)
            if exch_amt <= 1e-8:
                continue
            try:
                extra = _json.loads(pos.get("extra_json") or "{}")
            except Exception:
                extra = {}
            bot.state.add(sym, {
                "buy": float(pos["buy_price"]),
                "buy_time": pos["buy_time"],
                "amount": min(exch_amt, float(pos["amount"])),
                "invested_usdt": float(pos["invested_usdt"]),
                "original_amount": float(pos.get("original_amount") or pos["amount"]),
                "rsi_15m": pos.get("rsi_15m"),
                "rsi_1h": pos.get("rsi_1h"),
                "rsi_4h": pos.get("rsi_4h"),
                "change_pct": pos.get("change_pct"),
                "partial_sold": extra.get("partial_sold", False),
                "break_even": extra.get("break_even", False),
                "be_active": extra.get("be_active", False),
                # Preserve trailing-stop high-water-mark (resetting to
                # buy_price would discard a running position's trailing progress).
                "highest": float(pos.get("highest_price") or pos["buy_price"]),
                # Restore fee accounting so partial/full close doesn't
                # double-charge the entry fee.
                "initial_entry_fee": float(extra.get("initial_entry_fee", 0.0)),
                "fees_paid": float(extra.get("fees_paid", 0.0)),
            })
            rehydrated.append(sym)
        if rehydrated:
            log_event(
                f" Crash recovery: re-hydrated {len(rehydrated)} "
                f"ghost position(s) from SQLite: {rehydrated}",
                "WARN"
            )
    except Exception as gh_err:
        bot._log_error("ghost position detection", gh_err)

    #  Spot orphan adoption (exchange balance, bot unaware) 
    try:
        from core.database import get_all_claimed_bases, try_claim_orphan, _base_symbol, remove_open_position
        from core.clock import now_utc
        from core.logger import log_event, send_telegram
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        _SPOT_DUST_USDT = 1.0

        current_syms = set(bot.state.keys())
        _other_spot = get_all_claimed_bases(exclude_bot=bot.BOT_NAME, is_futures=False)

        adopted, unadoptable = [], []
        for coin, coin_bal in bal_data.items():
            if not isinstance(coin_bal, dict):
                continue
            if coin in ("USDT", "USD", "BUSD", "USDC", "FDUSD"):
                continue
            base = _base_symbol(coin)
            if not base or base in current_syms:
                continue
            if base in _other_spot:
                continue
            exch_amt = float(coin_bal.get("total", 0) or 0)
            if exch_amt <= 1e-8:
                continue
            try:
                ticker = bot.ex.fetch_ticker(f"{base}/USDT")
                price = float(ticker.get("last", 0) or 0)
            except Exception:
                price = 0.0
            if price <= 0:
                unadoptable.append(base)
                continue
            value_usdt = exch_amt * price
            if value_usdt < _SPOT_DUST_USDT:
                continue
            if not try_claim_orphan(bot.BOT_NAME, base, "SPOT"):
                continue
            try:
                from core.clock import now_utc as _now_utc
                bot.state.add(base, {
                    "buy": price,
                    "buy_time": _now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                    "amount": exch_amt,
                    "invested_usdt": value_usdt,
                    "original_amount": exch_amt,
                    "highest": price,
                    "initial_entry_fee": 0.0,
                    "fees_paid": 0.0,
                    "partial_sold": False,
                    "break_even": False,
                    "be_active": False,
                    "adopted": True,
                })
                adopted.append(base)
            except Exception as _ae:
                remove_open_position(bot.BOT_NAME, base)
                bot._log_error(f"spot adopt orphan {base}", _ae)
                unadoptable.append(base)

        if adopted:
            log_event(
                f" Spot reconciliation: ADOPTED {len(adopted)} untracked "
                f"balance(s)  now managed: {', '.join(sorted(adopted))}",
                "WARN")
            try:
                if not bool(getattr(bot, "simulation", True)):
                    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                        f" [{bot.BOT_NAME}] Adopted {len(adopted)} untracked "
                        f"spot balance(s):\n{', '.join(sorted(adopted))}\n"
                        f"Now managed (stop-loss + trailing).")
            except Exception:
                pass
        if unadoptable:
            log_event(
                f" Spot reconciliation: {len(unadoptable)} balance(s) "
                f"could NOT be adopted (no price)  review manually: "
                f"{', '.join(sorted(unadoptable))}",
                "WARN")
    except Exception as _oa_err:
        bot._log_error("spot orphan adoption", _oa_err)


# 
# Reconcile-thread Mixin
# 

class ReconcileMixin:
    """Periodic reconciliation thread body  drift check between local
    state and exchange wallet."""

    def _reconcile_loop(self):
        """Thread body: every RECONCILE_INTERVAL_SEC, run an exchange drift
        check. Skipped in SIMULATION mode (no real wallet to compare against).
        """
        from core.logger import log_event, send_telegram
        from core.symbol_locks import gc_idle_locks
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        if self.simulation:
            # SIM: nothing to reconcile against the exchange; stay quiet (no
            # log line). Lock-gc still runs periodically below.
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(timeout=self.GC_LOCKS_INTERVAL_SEC)
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")
            return

        log_event(
            f"Reconcile thread started (interval: "
            f"{self.RECONCILE_INTERVAL_SEC}s)",
            "INFO"
        )

        last_gc = 0.0
        while not self._shutdown_event.is_set():
            # Sleep first  startup_reconciliation already ran
            if self._shutdown_event.wait(timeout=self.RECONCILE_INTERVAL_SEC):
                return

            # GC idle symbol locks
            now = time.monotonic()
            if now - last_gc >= self.GC_LOCKS_INTERVAL_SEC:
                last_gc = now
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")

            # Exchange drift check
            try:
                bal_data = self.ex.fetch_balance()
                if not isinstance(bal_data, dict):
                    continue
                phantoms = []
                adjusted = []
                trades = self.state.get_all()
                present = sum(1 for sym in trades
                              if float((bal_data.get(sym) or {}).get("total", 0) or 0) > 1e-8)
                if trades and present == 0:
                    log_event(
                        f"[{self.BOT_NAME}] reconcile skipped  balance shows none "
                        f"of {len(trades)} held coin(s) (empty/partial read)", "WARN")
                    continue
                strikes = getattr(self, "_recon_missing_strikes", None)
                if strikes is None:
                    strikes = self._recon_missing_strikes = {}
                for sym, d in list(trades.items()):
                    try:
                        local_amt = float(d.get("amount", 0))
                        exch_amt = float((bal_data.get(sym) or {}).get("total", 0) or 0)
                        if exch_amt >= 1e-8:
                            strikes.pop(sym, None)
                        if exch_amt < 1e-8 and local_amt > 0:
                            # Shared strike gate: 2 consecutive misses before
                            # removing + booking the offline-close, so a single
                            # transient zero can't drop a live position.
                            if _gate_missing_for_removal(self, sym, d, strikes):
                                phantoms.append(sym)
                        elif (local_amt > 0
                              and abs(local_amt - exch_amt) / local_amt > 0.05
                              and exch_amt < local_amt
                              and not _is_fresh_position(d, self.RECONCILE_INTERVAL_SEC)):
                            # Skip shrinking a just-opened position whose balance
                            # may not have propagated yet (M-7).
                            # original_amount stays IMMUTABLE
                            ratio = exch_amt / local_amt
                            adjusted.append(f"{sym}: {local_amt:.6f}{exch_amt:.6f}")
                            new_invested = float(d.get("invested_usdt", 0)) * ratio
                            fields = {"amount": exch_amt, "invested_usdt": new_invested}
                            if "original_amount" not in d:
                                fields["original_amount"] = local_amt
                            self.state.update_many(sym, fields)
                    except (TypeError, ValueError):
                        continue
                if phantoms:
                    log_event(
                        f" Periodic reconciliation: removed "
                        f"{len(phantoms)} externally-closed: "
                        f"{', '.join(phantoms)}", "WARN"
                    )
                    try:
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] Reconciliation: "
                            f"{len(phantoms)} position(s) closed externally:\n"
                            f"{', '.join(phantoms)}"
                        )
                    except Exception:
                        pass
                if adjusted:
                    log_event(
                        f" Periodic reconciliation: adjusted "
                        f"{len(adjusted)}: {', '.join(adjusted)}", "WARN"
                    )
            except Exception as e:
                # Transient network blips (DNS fail, SSL EOF, timeout) are not
                # bugs  log them compactly and retry next cycle instead of
                # spamming the error log with full tracebacks.
                try:
                    from bot_utils.network_retry import is_transient_network
                    _net = is_transient_network(e)
                except Exception:
                    _net = False
                if _net:
                    log_event(
                        f"[{self.BOT_NAME}] reconcile skipped  network "
                        f"unreachable, retry next cycle ({type(e).__name__})",
                        "WARN")
                else:
                    self._log_error("periodic reconciliation", e)
