"""
core/futures_bot_reconcile.py  Reconciliation for FuturesBot.

Two reconciliation paths:
  1. startup_reconciliation()  boot-time: compares state vs exchange
     fetch_positions; removes local-only positions, warns on orphans
  2. _reconcile_loop()  every RECONCILE_INTERVAL_SEC, repeats
     the drift check + adjusts amounts on partial drift
"""
from __future__ import annotations

import time

from core.clock import now_utc


class FuturesReconcileMixin:

    def _startup_reconciliation(self) -> None:
        """Boot-time reconciliation  compares state vs exchange.

        Defensive against ``safe_fetch_positions`` returning an EMPTY list
        (could be a real exchange state, or an auth/network glitch). If we have
        local positions but the exchange returns nothing, refuse to wipe state
        log a loud warning and skip removal. Only proceed with removal when
        the exchange shows at least *some* positions OR local state was empty to
        begin with.
        """
        from core.logger import log_event, send_telegram
        from config.exchange_config import safe_fetch_positions
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from core.database import remove_futures_state
        from bot_utils import record_api_call

        try:
            local_state = self.state.get_all()
            record_api_call()
            exchange_positions = safe_fetch_positions(self.ex)
            if exchange_positions is None:
                log_event(
                    "Reconciliation: fetch_positions unavailable on this "
                    "exchange  skipping. Local state used as-is.", "WARN"
                )
                return

            # Build set of symbols with non-zero contracts on exchange
            exchange_open: dict = {}
            for p in exchange_positions:
                try:
                    contracts = abs(float(p.get("contracts") or p.get("size") or 0))
                    if contracts > 0:
                        full_sym = p.get("symbol", "")
                        base = full_sym.split("/")[0] if "/" in full_sym else full_sym
                        if base:
                            exchange_open[base] = p
                except (TypeError, ValueError):
                    continue

            # SAFETY GATE  if local state has positions but exchange shows
            # ZERO, refuse to wipe state. Protects against auth/network glitches
            # returning [] when positions actually exist on the exchange.
            # Manual intervention required.
            if local_state and not exchange_open:
                log_event(
                    f" Reconciliation ABORT: {len(local_state)} local "
                    f"position(s) but exchange returned 0  possible API "
                    f"glitch. Refusing to wipe state. Verify manually "
                    f"and restart bot if exchange truly is empty.",
                    "WARN"
                )
                try:
                    if not bool(getattr(self, "simulation", True)):
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] Reconcile aborted!\n"
                            f"{len(local_state)} local position(s) but "
                            f"exchange shows 0. Check the exchange manually."
                        )
                except Exception:
                    pass
                # Continue into the per-symbol 2-strike + authoritative
                # re-fetch path below. A real "all positions manually closed"
                # state also appears as an empty batch result.

            # Local-only  likely closed (manual close, liquidation, or
            # SL/TP that the bot couldn't see because it was offline).
            # Before removing from state, write a final trade-record to the DB
            # so the PnL is reflected in dashboards  otherwise a liquidation
            # during downtime would vanish from the user's PnL history.
            from core.symbol_locks import close_lock
            strikes = getattr(self, "_recon_missing_strikes", None)
            if strikes is None:
                strikes = self._recon_missing_strikes = {}
            for sym in list(local_state.keys()):
                if sym in exchange_open:
                    strikes.pop(sym, None)
                    continue
                # Require 2 consecutive cycles absent before booking an offline
                # close  a transient fetch_positions glitch (a real position
                # briefly reported with 0 contracts) would otherwise book a
                # phantom close and re-adopt next cycle (double-count).
                strikes[sym] = strikes.get(sym, 0) + 1
                if strikes[sym] < 2:
                    log_event(
                        f" Reconciliation: {sym} missing from exchange "
                        f"(strike {strikes[sym]}/2)  deferring removal", "WARN")
                    continue
                # Lock + re-check live state: the monitor thread may have closed
                # and booked this leg between the snapshot and now.
                with close_lock(sym, bot_name=self.BOT_NAME) as got:
                    if not got:
                        continue
                    if not self.state.has(sym):
                        strikes.pop(sym, None)
                        continue
                    # Authoritative re-fetch before the irreversible booking: a
                    # position that reappears was a transient snapshot glitch,
                    # not an offline close (M-6).
                    if self._still_open_on_exchange(sym):
                        strikes.pop(sym, None)
                        log_event(
                            f" Reconciliation: {sym} present on authoritative "
                            f"re-fetch  NOT booking a close (transient "
                            f"snapshot glitch)", "WARN")
                        continue
                    recorded = self._record_offline_close(
                        sym, self.state.get(sym) or local_state[sym])
                    if not recorded:
                        log_event(
                            f" Reconciliation: {sym} is absent on exchange "
                            f"but close accounting failed  keeping state "
                            f"for retry/recovery.",
                            "WARN"
                        )
                        continue
                    log_event(
                        f" Reconciliation: {sym} in local state but NOT "
                        f"on exchange  removing (likely manually closed "
                        f"or liquidated while bot was offline)",
                        "WARN"
                    )
                    try:
                        self.state.remove(sym)
                        # Scope by bot: FUTURES + CROSS share futures_state;
                        # unscoped would wipe the other bot's dashboard row.
                        remove_futures_state(sym, self.BOT_NAME)
                    except Exception as e:
                        self._log_error(f"reconcile-remove {sym}", e)
                    strikes.pop(sym, None)

            # Exchange-only positions. COEXISTENCE: a coin held by ANOTHER bot
            # (shared claims registry) is NOT our orphan  subtract those.
            orphan_syms = set(exchange_open.keys()) - set(local_state.keys())
            if orphan_syms:
                try:
                    from core.database import get_all_claimed_bases, _base_symbol
                    _other = get_all_claimed_bases(exclude_bot=self.BOT_NAME,
                                                   is_futures=True,
                                                   fail_closed=True)
                    if _other is None:
                        log_event(
                            " Reconciliation: claim registry unavailable; "
                            "skipping exchange-only adoption this cycle",
                            "WARN",
                        )
                        orphan_syms = set()
                    else:
                        orphan_syms = {s for s in orphan_syms
                                       if _base_symbol(s) not in _other}
                except Exception:
                    orphan_syms = set()

            # ADOPT: a LIVE trading bot must NEVER leave a leveraged exchange
            # position unmanaged. On ANY stateexchange desync (SIM/LIVE toggle,
            # crash, lost state file) pull the REAL entry/size/side/leverage from
            # the exchange and add the position to state: the monitor manages its
            # exits, and the write claims the coin so the scan/rebalance never
            # re-opens it (which would net).
            adopted, unadoptable = [], []
            try:
                from core.database import try_claim_orphan, remove_open_position
            except Exception:
                def try_claim_orphan(*a, **k): return False
                def remove_open_position(*a, **k): return None
            for base in sorted(orphan_syms):
                # ATOMIC claim: SQLite serialises INSERTWHERE NOT EXISTS, so when
                # BOTH bots' reconciles race to adopt the same orphan, exactly ONE
                # wins. The loser skips  never a double-adopt / double-manage.
                # (Whichever bot wins manages it safely; ownership info is lost
                # once the state desynced, so first-come is the best we can do 
                # the alternative, leaving it unmanaged, is worse.)
                if not try_claim_orphan(self.BOT_NAME, base):
                    continue
                p = exchange_open.get(base) or {}
                info = p.get("info") if isinstance(p.get("info"), dict) else {}
                try:
                    entry = float(p.get("entryPrice") or info.get("entryPrice")
                                  or info.get("openAvgPrice") or 0)
                    raw_contracts = float(p.get("contracts") or p.get("size") or 0)
                    contracts = abs(raw_contracts)
                    side = str(p.get("side") or "").lower()
                    if not side and raw_contracts < 0:
                        side = "short"
                    lev = float(p.get("leverage") or self.C("LEVERAGE", 3) or 3)
                    liq = float(p.get("liquidationPrice")
                                or info.get("liquidationPrice") or 0)
                    mm_mode = str(p.get("marginMode") or info.get("marginMode")
                                  or info.get("marginType") or "").lower()
                except (TypeError, ValueError):
                    entry = contracts = lev = liq = 0.0
                    side = ""
                    mm_mode = ""
                if entry <= 0 or contracts <= 0 or side not in ("long", "short"):
                    # Won the claim but can't adopt safely  RELEASE it so the
                    # coin isn't blocked-but-unmanaged.
                    remove_open_position(self.BOT_NAME, base)
                    unadoptable.append(base)
                    continue
                full = f"{base}/USDT:USDT"
                try:
                    cs = self._get_contract_size(full)
                except Exception:
                    cs = 1.0
                margin = (contracts * cs * entry) / lev if lev > 0 else 0.0
                pos_type = "LONG" if side == "long" else "SHORT"
                try:
                    from bot_utils import get_maintenance_margin_rate
                    from bot_utils.futures_math import (
                        calc_liquidation_price,
                        distance_to_liquidation_pct,
                    )
                    mm = get_maintenance_margin_rate(self.ex, full)
                    if liq <= 0:
                        liq = calc_liquidation_price(entry, lev, pos_type, mm)
                    initial_liq_distance = distance_to_liquidation_pct(
                        entry, liq, pos_type) if liq > 0 else 0.0
                except Exception:
                    initial_liq_distance = 0.0
                if initial_liq_distance <= 0:
                    initial_liq_distance = max(1.0, 100.0 / max(1.0, lev))
                try:
                    self.state.add(base, {
                        "position_type": pos_type, "buy": entry, "highest": entry,
                        "last_price": entry,
                        "buy_time": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                        "invested_usdt": margin, "leverage": lev,
                        "amount": contracts, "original_amount": contracts,
                        "liquidation_price": liq, "funding_paid": 0.0,
                        "initial_liq_distance": initial_liq_distance,
                        "initial_entry_fee": 0.0, "fees_paid": 0.0,
                        "partial_sold": False, "break_even": False,
                        "be_active": False, "adopted": True,
                        "margin_mode": mm_mode or self.C("MARGIN_MODE", "isolated"),
                    })
                    adopted.append(base)
                except Exception as _ae:
                    remove_open_position(self.BOT_NAME, base)  # release on failure
                    self._log_error(f"adopt orphan {base}", _ae)
                    unadoptable.append(base)

            if adopted:
                log_event(
                    f" Reconciliation: ADOPTED {len(adopted)} untracked exchange "
                    f"position(s)  now MANAGED (SL/Liq/Trailing) + claimed: "
                    f"{', '.join(sorted(adopted))}", "WARN")
                try:
                    if not bool(getattr(self, "simulation", True)):
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] Adopted {len(adopted)} untracked "
                            f"position(s):\n{', '.join(sorted(adopted))}\n"
                            f"Now managed (stop-loss + liquidation protection).")
                except Exception as e:
                    log_event(f"Telegram failed: {e}", "WARN")
            if unadoptable:
                log_event(
                    f" Reconciliation: {len(unadoptable)} exchange position(s) "
                    f"could NOT be adopted (no entry/size/side)  CLOSE MANUALLY: "
                    f"{', '.join(sorted(unadoptable))}", "WARN")
                try:
                    if not bool(getattr(self, "simulation", True)):
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] {len(unadoptable)} position(s) could "
                            f"not be adopted:\n{', '.join(sorted(unadoptable))}\n"
                            f"Close manually on the exchange.")
                except Exception as e:
                    log_event(f"Telegram failed: {e}", "WARN")
            if not orphan_syms:
                log_event(
                    f" Reconciliation: {len(local_state)} position(s) "
                    f"in sync with exchange", "INFO")
        except Exception as e:
            self._log_error("reconciliation", e)

    def _still_open_on_exchange(self, sym: str) -> bool:
        """Authoritative single-symbol position re-fetch (M-6).

        The batch snapshot can transiently report a real position with 0
        contracts; two such glitches in a row pass the 2-strike gate and would
        book a phantom offline-close, then re-adopt next cycle. Before that
        irreversible booking we re-fetch JUST this symbol. Returns True if the
        position is still present OR the fetch can't confirm it's gone (errors,
        empty)  so the caller DEFERS rather than booking on doubt; False only
        when the exchange authoritatively reports no contracts."""
        full = f"{sym}/USDT:USDT"
        try:
            from config.exchange_config import safe_fetch_positions
            poss = safe_fetch_positions(self.ex, [full])
            scoped_has_symbol = False
            if poss is not None:
                try:
                    scoped_has_symbol = any(
                        (p.get("symbol") or "") == full for p in poss
                    )
                except Exception:
                    scoped_has_symbol = False
            if poss is None or not scoped_has_symbol:
                poss = safe_fetch_positions(self.ex)
                if poss is None:
                    return True
        except Exception as e:
            self._log_error(f"reconcile re-fetch {sym}", e)
            return True  # can't confirm closed  conservative: defer
        for p in poss or []:
            if (p.get("symbol") or "") != full:
                continue
            try:
                contracts = abs(float(p.get("contracts") or p.get("size") or 0))
            except (TypeError, ValueError):
                contracts = 0.0
            if contracts > 0:
                return True
        return False

    def _record_offline_close(self, sym: str, state_row: dict) -> bool:
        """When reconciliation finds a position gone from the exchange,
        write a trade record so the PnL shows up in dashboards.

        Sources for the close price (priority order):
          1. fetch_order_history for this symbol  find the most recent
             reduceOnly fill (the actual close price)
          2. Current market price (worst case  slightly stale, but
             better than no record at all)

        Sign: matches the bot's normal close logic. The fee is estimated
        from the standard taker rate since we have no order dict.

        Notes:
          Best-effort only. Wrapped in broad except so a failure here
            returns False when accounting could not be written; callers must
            keep state in that case so recovery can retry.
          A Telegram heads-up is sent so the user notices the close
            (especially important for liquidations during long downtime).
        """
        from core.logger import log_event, save_trade, send_telegram
        from core.database import save_trade_db
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from bot_utils import safe_proportional_fee, safe_remaining_funding
        from bot_utils.futures_order import FUTURES_DEFAULT_TAKER_FEE

        try:
            entry = float(state_row.get("buy", 0) or 0)
            amount = float(state_row.get("amount", 0) or 0)
            pos_type = state_row.get("position_type", "LONG")
            lev = float(state_row.get("leverage", 1) or 1)
            margin = float(state_row.get("invested_usdt", 0) or 0)
            buy_time = state_row.get("buy_time", "")
            initial_entry_fee = float(state_row.get(
                "initial_entry_fee", state_row.get("fees_paid", 0)) or 0)
            funding_total = float(state_row.get("funding_paid", 0) or 0)
            funding_booked = float(state_row.get(
                "funding_booked_on_partials", 0) or 0)
            original_amount = float(state_row.get(
                "original_amount", state_row.get("amount", 0)) or 0)
            partial_sold = bool(state_row.get("partial_sold"))
            liq_price = float(state_row.get("liquidation_price", 0) or 0)

            if entry <= 0 or amount <= 0:
                log_event(
                    f" {sym}: offline-close record skipped  invalid "
                    f"state (entry={entry}, amount={amount})", "WARN"
                )
                return False

            symbol_full = f"{sym}/USDT:USDT"

            # Try to find the actual close price from order history
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
            try:
                if close_price <= 0 and hasattr(self.ex, "fetch_my_trades"):
                    trades = self.ex.fetch_my_trades(symbol_full, limit=20) or []
                    # Most recent reduceOnly trade = the close
                    for t in reversed(trades):
                        info = t.get("info", {}) or {}
                        raw_reduce_only = (
                            info.get("reduceOnly")
                            if "reduceOnly" in info else info.get("reduce_only")
                        )
                        reduce_only = raw_reduce_only is True or (
                            isinstance(raw_reduce_only, str)
                            and raw_reduce_only.strip().lower() in ("1", "true", "yes")
                        )
                        if reduce_only:
                            close_price = float(t.get("price", 0) or 0)
                            close_source = "fetch_my_trades"
                            break
            except Exception:
                pass

            # Fallback: current market price
            if close_price <= 0:
                try:
                    ticker = self.ex.fetch_ticker(symbol_full)
                    close_price = float(ticker.get("last", 0) or 0)
                    close_source = "current_ticker"
                except Exception:
                    pass

            if close_price <= 0:
                # Last-ditch: assume liquidation if liq price was set
                if liq_price > 0:
                    close_price = liq_price
                    close_source = "liquidation_price"
                else:
                    log_event(
                        f" {sym}: cannot determine offline-close price  "
                        f"skipping DB record. State kept for retry.",
                        "WARN"
                    )
                    return False

            # Calculate PnL the same way the bot's normal close does
            if pos_type == "LONG":
                price_move_pct = ((close_price - entry) / entry) * 100
            else:  # SHORT
                price_move_pct = ((entry - close_price) / entry) * 100

            # Margin  move  leverage = realized P&L on margin
            pnl_pct_margin = price_move_pct * lev
            gross_pnl = (margin * pnl_pct_margin / 100) if margin > 0 else 0.0

            # Estimate close fee (no order dict available).
            # Include contract_size  `amount` is in CONTRACTS, so for
            # contract_size != 1 coins the fee would otherwise be understated by
            # that factor. Mirrors the live close/partial paths.
            try:
                _cs = self._get_contract_size(symbol_full)
            except Exception:
                _cs = 1.0
            close_fee = amount * _cs * close_price * FUTURES_DEFAULT_TAKER_FEE
            entry_fee = safe_proportional_fee(
                initial_entry_fee, amount, original_amount,
                partial_sold=partial_sold,
            )
            funding_pd = safe_remaining_funding(
                funding_total, amount, original_amount,
                partial_sold=partial_sold,
                booked_on_partials=funding_booked,
            )

            # Net PnL  fees and funding deducted. If emergency-close already
            # flattened the exchange position but DB accounting failed, prefer
            # its captured realized values over a later ticker estimate.
            net_pnl = round(gross_pnl - entry_fee - close_fee - funding_pd, 4)
            if pending_accounting and close_source == "accounting_pending":
                try:
                    net_pnl = float(
                        state_row.get("accounting_pending_profit_usdt"))
                except (TypeError, ValueError):
                    pass
                try:
                    funding_pd = float(
                        state_row.get("accounting_pending_funding_paid"))
                except (TypeError, ValueError):
                    pass
                try:
                    close_fee_total = float(
                        state_row.get("accounting_pending_fees_usdt"))
                    entry_fee = 0.0
                    close_fee = close_fee_total
                except (TypeError, ValueError):
                    pass
                try:
                    price_move_pct = float(
                        state_row.get("accounting_pending_profit_pct"))
                except (TypeError, ValueError):
                    pass
            mfe_pct = state_row.get("accounting_pending_mfe_pct")
            mae_pct = state_row.get("accounting_pending_mae_pct")
            giveback_pct = state_row.get("accounting_pending_giveback_pct")

            sell_time = str(
                state_row.get("accounting_pending_sell_time")
                or now_utc().strftime("%Y-%m-%d %H:%M:%S"))
            reason = str(
                state_row.get("accounting_pending_reason")
                or (f"Offline close ({close_source})"
                    if close_source != "liquidation_price"
                    else "LIQUIDATED (offline)"))

            saved = save_trade_db(
                bot_name=self.BOT_NAME, symbol=sym,
                buy_price=entry, sell_price=close_price,
                buy_time=buy_time, sell_time=sell_time,
                profit_pct=price_move_pct, profit_usdt=net_pnl,
                invested_usdt=margin,
                reason=reason,
                is_futures=True, position_type=pos_type,
                leverage=lev,
                liquidation_price=liq_price,
                funding_paid=funding_pd,
                fees_usdt=entry_fee + close_fee,
                exchange_order_id=state_row.get(
                    "accounting_pending_exchange_order_id"),
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                giveback_pct=giveback_pct,
            )
            if not saved:
                log_event(
                    f" {sym}: offline-close DB save failed  state kept "
                    f"for accounting retry.", "WARN")
                return False

            log_event(
                f" {sym}: offline-close recorded "
                f"({pos_type}, entry={entry:.4f}, close={close_price:.4f}, "
                f"PnL={net_pnl:+.2f} USDT, source={close_source})",
                "WARN"
            )
            try:
                if not bool(getattr(self, "simulation", True)):
                    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                        f" [{self.BOT_NAME}] Offline close detected\n"
                        f"{sym} {pos_type}: {entry:.4f}  {close_price:.4f}\n"
                        f"PnL: {net_pnl:+.2f} USDT\n"
                        f"Reason: {reason}"
                    )
            except Exception:
                pass
            return True
        except Exception as e:
            self._log_error(f"offline-close record {sym}", e)
            return False

    #  Periodic reconcile thread 

    def _reconcile_loop(self) -> None:
        from core.logger import log_event
        from core.symbol_locks import gc_idle_locks

        if self.simulation:
            # SIM: reconcile against the exchange is a no-op; stay quiet (no
            # log line). We still run lock-gc / clone-reaping periodically.
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(timeout=self.GC_LOCKS_INTERVAL_SEC)
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")
                # Reap CCXT clones whose worker thread has exited so transient
                # screener/pool threads don't leak deep-copied markets maps
                # over long uptime.
                try:
                    reaper = getattr(self.ex, "reap_dead_thread_clones", None)
                    if callable(reaper):
                        reaped = reaper()
                        if reaped:
                            log_event(f"[maintenance] reaped {reaped} dead-thread "
                                      f"exchange clones", "INFO")
                except Exception:
                    pass
            return

        log_event(
            f"Futures reconcile thread started (interval: "
            f"{self.RECONCILE_INTERVAL_SEC}s)",
            "INFO"
        )

        last_gc = 0.0
        while not self._shutdown_event.is_set():
            if self._shutdown_event.wait(timeout=self.RECONCILE_INTERVAL_SEC):
                return

            # GC idle locks
            now = time.monotonic()
            if now - last_gc >= self.GC_LOCKS_INTERVAL_SEC:
                last_gc = now
                try:
                    n = gc_idle_locks()
                    if n:
                        log_event(f"[maintenance] dropped {n} idle locks", "INFO")
                except Exception as e:
                    log_event(f"[maintenance] gc_idle_locks failed: {e}", "WARN")
                # Reap CCXT clones whose worker thread has exited so transient
                # screener/pool threads don't leak deep-copied markets maps
                # over long uptime.
                try:
                    reaper = getattr(self.ex, "reap_dead_thread_clones", None)
                    if callable(reaper):
                        reaped = reaper()
                        if reaped:
                            log_event(f"[maintenance] reaped {reaped} dead-thread "
                                      f"exchange clones", "INFO")
                except Exception:
                    pass

            # Drift check (same logic as startup)
            try:
                self._startup_reconciliation()
            except Exception as e:
                self._log_error("periodic reconciliation", e)
