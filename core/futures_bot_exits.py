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

import time

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


class FuturesExitsMixin:

    def _retry_pending_partial_accounting(self, sym: str, d: dict) -> None:
        pending = list(d.get("accounting_pending_partials") or [])
        if not pending:
            return
        from core.database import save_trade_db
        from core.logger import log_event

        remaining = []
        for item in pending:
            try:
                retry_item = dict(item)
                retry_item.setdefault("mode_is_sim", self.simulation)
                saved = bool(save_trade_db(**retry_item))
            except Exception as exc:
                saved = False
                self._log_error(f"futures partial accounting retry {sym}", exc)
            if not saved:
                remaining.append(item)
        self.state.update(sym, "accounting_pending_partials", remaining)
        if remaining:
            log_event(
                f"{sym}: {len(remaining)} futures partial accounting "
                f"event(s) still pending", "WARN")
        else:
            log_event(f"{sym}: pending futures partial accounting flushed", "INFO")

    def _cleanup_accounted_close_state(self, sym: str, d: dict) -> bool:
        """Remove dashboard/local state after PnL was already booked.

        If cleanup fails, keep a marked local row so the next monitor/reconcile
        pass retries cleanup instead of sending another reduce-only close.
        """
        from core.logger import log_event
        from core.database import remove_futures_state
        from bot_utils.trade_state import remove_with_restore_fields

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
                mode_is_sim=getattr(self, "simulation", None))
        except Exception as exc:
            self._log_error(f"remove_futures_state accounted {sym}", exc)
            try:
                keep = dict(restore)
                keep["futures_state_cleanup_pending"] = True
                self.state.update_many(sym, keep)
            except Exception as state_exc:
                self._log_error(f"mark futures cleanup pending {sym}", state_exc)
            log_event(
                f"{sym}: close already booked, but futures_state cleanup "
                f"failed; state kept for retry",
                "WARN",
            )
            return False

        ok = remove_with_restore_fields(self.state, sym, restore)
        if not ok:
            log_event(
                f"{sym}: close already booked, but claim/state cleanup "
                f"failed; state kept for retry",
                "WARN",
            )
            return False
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
        last_killswitch = 0.0
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

                # API budget guard: throttle monitor if exhausted
                if budget_exhausted():
                    extra = min(monitor_interval, 30)
                    log_event(
                        f"[Monitor] API budget exhausted - adding {extra}s "
                        f"throttle (positions still monitored)", "WARN"
                    )
                    if self._shutdown_event.wait(timeout=extra):
                        return

  # Killswitch  ADAPTIVE cadence: normally 60s, but tighten to
                # ~8s once today's loss is past 70% of the limit, so a fast 20%
                # drop can't overshoot the cap between checks.
                if (now - last_killswitch) >= ks_interval:
                    last_killswitch = now
                    self._check_killswitch(trades)
                    try:
                        _today = getattr(self, "_ks_last_total", 0.0)
                        _maxloss = float(self.C("MAX_DAILY_LOSS", -30.0))
                        ks_interval = (8.0 if (_maxloss < 0 and _today <= _maxloss * 0.7)
                                       else KILLSWITCH_INTERVAL)
                    except Exception:
                        ks_interval = KILLSWITCH_INTERVAL

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

    def _check_killswitch(self, trades: dict) -> None:
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
                return  # bot not fully initialized yet
            pnl_info = get_today_pnl(self.BOT_NAME, mode_is_sim=self.simulation)
            today_realized = pnl_info.get("total_profit", 0.0)
            # Add unrealized from open positions (best-effort). Track two totals:
  #  unrealized_all  every open position (genuine current risk)
  #  unrealized_today  only positions OPENED TODAY (local tz)
            unrealized_all = 0.0
            unrealized_today = 0.0
            for sym, d in trades.items():
                try:
                    entry = float(d.get("buy", 0))
                    margin = float(d.get("invested_usdt", 0))
                    lev = float(d.get("leverage", self.C("LEVERAGE", 3)))
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
                        self._hard_kill_fired = True
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
        except Exception as e:
            self._log_error("killswitch check", e)

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
            px = float(tk.get("last") or tk.get("close") or 0)
            if px > 0:
                return px
        except Exception:
            pass
        try:
            px = float(self._fallback_mark_price(symbol_full) or 0.0)
            if px > 0:
                return px
        except Exception:
            pass
        try:
            return float(d.get("last_price", entry) or entry)
        except (TypeError, ValueError):
            return float(entry or 0.0)

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

        for sym, d in trades.items():
            try:
                next_check = float(d.get("funding_next_check_at", 0))
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
                lev = float(d.get("leverage", 1))
                entry = float(d.get("buy", 0))
  # WHOLE-position notional (original_amount  contract_size  entry),
  # NOT the partial-reduced invested_usdt  lev. funding_paid must
                # stay the funding of the FULL position so the close-time
  # proportional scaling (remaining/original) is the SOLE scaling 
                # using the reduced notional here AND scaling at close double-counted
  # the reduction (remaining/original) and understated funding.
                pos_type = d.get("position_type", "LONG")
                symbol_full = f"{sym}/USDT:USDT"
                orig_amt = float(d.get("original_amount", d.get("amount", 0)))
                if lev <= 0 or entry <= 0 or orig_amt <= 0:
                    continue
                notional = orig_amt * self._get_contract_size(symbol_full) * entry
                if self.simulation:
  # No real exchange position to query  estimate funding from
  # the live rate  settlements crossed so paper PnL carries it.
                    realized = estimate_funding_paid(
                        self.ex, symbol_full, buy_time, notional, pos_type)
                else:
                    realized = fetch_or_estimate_funding(
                        self.ex, symbol_full, buy_time,
                        notional_usdt=notional, pos_type=pos_type,
                        fallback_state_value=float(d.get("funding_paid", 0.0)),
                    )
                update = {
                    "funding_next_check_at": now_epoch + self._FUNDING_REFRESH_INTERVAL_SEC,
                }
                if realized is not None:
                    update["funding_paid"] = float(realized)
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
        try:
            poss = self.ex.fetch_positions([symbol_full]) or []
            for p in poss:
                info = p.get("info", {}) or {}
                mark = (p.get("markPrice") or info.get("markPrice")
                        or info.get("marketPrice") or p.get("lastPrice"))
                if mark and float(mark) > 0:
                    return float(mark)
        except Exception:
            pass
        return 0.0

    def _note_price_unavailable(self, sym: str) -> None:
        """Count consecutive ticks with NO usable price (ticker AND mark both
  dead) and escalate once past a threshold  a leveraged position with no
        price is running without liq protection, so it must not fail silently."""
        from core.logger import log_event
        counts = getattr(self, "_price_unavail_counts", None)
        if counts is None:
            counts = {}
            self._price_unavail_counts = counts
        counts[sym] = counts.get(sym, 0) + 1
        if counts[sym] == 5:                      # ~5 consecutive monitor ticks
            log_event(
                f"{sym}: price unavailable for {counts[sym]} consecutive ticks "
                f"(ticker AND mark) - liq protection is BLIND on this leveraged "
                f"position. Check the symbol on the exchange / close manually.",
                "WARN")
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

        if d.get("accounting_already_booked"):
            FuturesExitsMixin._cleanup_accounted_close_state(self, sym, d)
            return
        if d.get("accounting_pending"):
            try:
                if self._record_offline_close(sym, d):
                    booked = dict(d)
                    booked.update({
                        "accounting_already_booked": True,
                        "accounting_booked_sell_time": (
                            d.get("accounting_pending_sell_time")),
                        "accounting_booked_exchange_order_id": (
                            d.get("accounting_pending_exchange_order_id")),
                        "accounting_booked_reason": (
                            d.get("accounting_pending_reason")
                            or "Offline close"),
                    })
                    FuturesExitsMixin._cleanup_accounted_close_state(
                        self, sym, booked)
            except Exception as exc:
                self._log_error(f"futures pending accounting retry {sym}", exc)
            return
        if d.get("claim_conflict"):
            warned = getattr(self, "_claim_conflict_warned", set())
            if sym not in warned:
                log_event(
                    f"[{self.BOT_NAME}] {sym}: registry claim conflict - "
                    f"monitor skipped fail-closed; run claim/state repair",
                    "ERROR",
                )
                warned.add(sym)
                self._claim_conflict_warned = warned
            return
        self._retry_pending_partial_accounting(sym, d)
        d = self.state.get(sym) or d
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
            curr = float(ticker.get("last") or ticker.get("close") or 0)
        except Exception as e:
            curr = 0.0
            log_event(f"Monitor: Price for {sym} unavailable: {e}", "WARN")
        if curr <= 0:
            # DON'T go blind: every safety check below (liq-buffer protection,
            # SL/trailing) and the last_price write the killswitch reads live
            # PAST this point. When the ticker feed is down (delist/halt/symbol
            # error), fall back to the exchange position's mark price so the
            # leveraged position keeps its liq protection and the daily-loss
            # killswitch keeps seeing a real drawdown instead of a stale value.
            curr = self._fallback_mark_price(symbol_full)
        if curr <= 0:
            self._note_price_unavailable(sym)
            return
        self._clear_price_unavailable(sym)

        pos_type = d["position_type"]
        lev = float(d.get("leverage", self.C("LEVERAGE", 3)))
        margin = float(d["invested_usdt"])
        entry = float(d["buy"])

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

        # Sanity: heal margin/leverage if broken (defensive)
        if lev <= 0:
            lev = float(self.C("LEVERAGE", 3))
        if margin <= 0:
            try:
                _amt = float(d.get("amount", 0))
                # contract_size-aware: _amt is in CONTRACTS, so notional =
                # amount * contract_size * price (required for contract_size!=1
                # coins, else the healed margin feeds the full-close PnL wrong).
                _cs = self._get_contract_size(f"{sym}/USDT:USDT")
                if _amt > 0 and entry > 0 and lev > 0:
                    margin = round((_amt * _cs * entry) / lev, 4)
            except Exception:
                pass

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
            if now_ts >= float(d.get("liq_next_check_at", 0)):
                exch_liq = get_exchange_liq_price(self.ex, symbol_full)
                upd = {"liq_next_check_at": now_ts + self.LIQ_REFRESH_INTERVAL_SEC}
                if exch_liq > 0:
                    liq_price = exch_liq
                    upd["liquidation_price"] = exch_liq
                else:
                    liq_price = float(d.get("liquidation_price",
                                              calc_liquidation_price(entry, lev, pos_type, mm_rate)))
                self.state.update_many(sym, upd)
            else:
                liq_price = float(d.get("liquidation_price",
                                          calc_liquidation_price(entry, lev, pos_type, mm_rate)))
        else:
            liq_price = float(d.get("liquidation_price",
                                      calc_liquidation_price(entry, lev, pos_type, mm_rate)))

        pnl_usdt, pnl_pct_margin = calc_unrealized_pnl(entry, curr, margin, lev, pos_type)
        move_pct = price_move_pct(entry, curr, pos_type)
        liq_dist = distance_to_liquidation_pct(curr, liq_price, pos_type)
        telemetry = {}
        try:
            prev_max_pct = float(d.get("max_profit_pct", move_pct))
        except (TypeError, ValueError):
            prev_max_pct = move_pct
        try:
            prev_min_pct = float(d.get("min_profit_pct", move_pct))
        except (TypeError, ValueError):
            prev_min_pct = move_pct
        try:
            prev_max_usdt = float(d.get("max_profit_usdt", pnl_usdt))
        except (TypeError, ValueError):
            prev_max_usdt = pnl_usdt
        try:
            prev_min_usdt = float(d.get("min_profit_usdt", pnl_usdt))
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
            self.state.update_many(sym, telemetry)
            d.update(telemetry)

        # Update highest favorable move
        highest = d.get("highest", entry)
        if is_new_high(curr, highest, pos_type):
            self.state.update(sym, "highest", curr)
            highest = curr

        # Stash last_price for emergency-close fallback
        self.state.update(sym, "last_price", curr)

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
                opened_at=d["buy_time"]
            )
        except Exception:
            pass  # dashboard state is best-effort

  #  Breakeven activation 
        be_trigger = float(self.C("BREAKEVEN_TRIGGER", 0))
        if be_trigger > 0 and not d.get("be_active", False) and move_pct >= be_trigger:
            safe_be_price = fee_buffered_breakeven(entry, pos_type, fee_buffer=0.003)
            self.state.update_many(sym, {"be_active": True, "be_price": safe_be_price})
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
        activation = float(self.C("ACTIVATION_PROFIT"))
        if (not sell_trigger
                and not d.get("partial_sold")
                and move_pct >= activation):
            from core.symbol_locks import close_lock
            with close_lock(sym, bot_name=self.BOT_NAME) as got:
                if not got or not self.state.has(sym):
                    return
                live = self.state.get(sym) or d
                if live.get("partial_sold"):
                    return
                self._execute_partial_tp(sym, live, curr, move_pct, pnl_pct_margin,
                                          entry, liq_price, margin, lev, pos_type)
            return

        if sell_trigger:
            # Serialize the flatten against the reconcile-thread offline-close
            # (same per-symbol lock) so it can't be double-booked as a phantom
  # offline-close. Skip on contention  retried next monitor tick.
            from core.symbol_locks import close_lock
            with close_lock(sym, bot_name=self.BOT_NAME) as got:
                if not got or not self.state.has(sym):
                    return
                self._execute_full_close(sym, d, curr, move_pct, pnl_usdt,
                                           entry, liq_price, margin, lev, pos_type, reason)

  #  Exit evaluation (pure decision) 

    def _evaluate_futures_exit(self, sym, d, curr, move_pct, highest, entry,
                                 liq_price, liq_dist, lev, pos_type):
        """Decide whether to close. Returns (should_close, reason).

        Priority:
  1. Liquidation buffer consumed  "Liq protection"
          2. Breakeven stop (when be_active)
          3. (Partial TP handled by caller)
  4. Trailing or SL  depends on partial_sold state

  ``sym`` is passed explicitly (the trades dict has no "symbol" field 
        sym is the outer key) so the ``initial_liq_distance`` heal-write
        targets the right row.
        """
        from core.logger import log_event

  #  1. Liquidation protection (highest priority) 
        initial_liq_dist = float(d.get("initial_liq_distance", 0))
        if initial_liq_dist <= 0:
            initial_liq_dist = max(1.0, 100.0 / max(1.0, lev))
            try:
                self.state.update(sym, "initial_liq_distance", initial_liq_dist)
            except Exception:
                pass

        consumed = liq_buffer_consumed_pct(initial_liq_dist, liq_dist)
        panic_threshold = 100.0 - float(self.C("LIQ_SAFETY_PCT", 25.0))
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
            be_price = float(d.get("be_price", entry))
            if breakeven_stop_hit(curr, be_price, pos_type):
                return True, "Breakeven-Stop"

  #  3. Trailing / SL 
        trailing_dist = float(self.C("TRAILING_DISTANCE"))
        post_partial_trailing_dist = float(
            self.C("POST_PARTIAL_TRAILING_DISTANCE", trailing_dist))
        activation = float(self.C("ACTIVATION_PROFIT"))
        if (post_partial_trailing_dist <= 0.0
                or (activation > 0.0 and post_partial_trailing_dist >= activation)):
            post_partial_trailing_dist = trailing_dist
        initial_sl = float(self.C("INITIAL_STOP_LOSS"))

        if d.get("break_even"):
            # Post-partial-TP: trailing fully armed
            if trailing_stop_hit(curr, highest, post_partial_trailing_dist, pos_type):
                return True, "Trailing Stop"
            be_floor = float(d.get(
                "be_price",
                fee_buffered_breakeven(entry, pos_type, fee_buffer=0.003),
            ))
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

        partial_pct = float(self.C("PARTIAL_SELL_PCT"))
        symbol_full = f"{sym}/USDT:USDT"
        raw_partial = float(d.get("amount", 0)) * partial_pct
        margin_mode = str(d.get("margin_mode") or self.C("MARGIN_MODE", "isolated") or "isolated").lower()

        # Include contract size in the notional computation. For typical Bitget
        # USDT-M perpetuals contract_size=1; for inverse/COIN-M or markets with
        # contract multipliers (Bybit inverse) it's the difference between TP
        # firing correctly and skipping erroneously.
        contract_size = self._get_contract_size(symbol_full)
        remaining_after = float(d.get("amount", 0)) - raw_partial
        slice_notional = raw_partial * curr * contract_size
        rem_notional   = remaining_after * curr * contract_size

        # Min-notional pre-check against the REAL per-market floor
  # (limits.cost.min via the shared helper), not a hardcoded 5.0  some
        # perps floor at 1 USDT, others at 10. contract_size is folded into the
        # effective amount so the helper's amount*price equals our notional.
        from bot_utils.futures_exits import _check_min_notional
        ok_slice, _ = _check_min_notional(self.ex, symbol_full,
                                          raw_partial * contract_size, curr)
        ok_rem, _ = _check_min_notional(self.ex, symbol_full,
                                        remaining_after * contract_size, curr)
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
                           * taker_fee_rate(self.ex, symbol_full, 0.0006))
        else:
            try:
                try:
                    from config.exchange_config import safe_amount_to_precision
                    partial_amount = float(safe_amount_to_precision(
                        self.ex, symbol_full, raw_partial))
                except Exception:
                    partial_amount = round(raw_partial, 4)
                if partial_amount <= 0:
                    log_event(f"{sym}: partial amount below exchange minimum - skip", "WARN")
                    return False
                close_side = "sell" if pos_type == "LONG" else "buy"
                order = create_order_with_retry(
                    self.ex, symbol_full, close_side, partial_amount,
                    params=reduce_only_params(
                        position_side=("long" if pos_type == "LONG" else "short"),
                        margin_mode=margin_mode,
                        leverage=max(1, int(__import__("math").ceil(lev))),
                    ),
                    shutdown_event=self._shutdown_event,
                    action_label=f"partial-TP {sym}",
                    log_event=log_event,
                )
                exch_oid = order.get("id") or order.get("orderId")
                actual_filled = 0.0
                try:
                    actual_filled = float(order.get("filled") or 0.0)
                except (TypeError, ValueError):
                    actual_filled = 0.0
                try:
                    from bot_utils.futures_exits import _resolve_fill_price
                    fill_price, fill_src = _resolve_fill_price(
                        self.ex, symbol_full, order, fill_price, log_event)
                except Exception:
                    for k in ("average", "price"):
                        v = order.get(k)
                        if v:
                            try:
                                fv = float(v)
                                if fv > 0:
                                    fill_price = fv
                                    break
                            except (TypeError, ValueError):
                                continue
                if actual_filled <= 0 and exch_oid:
                    try:
                        import time as _t
                        for _att in range(2):
                            _t.sleep(0.35 * (1 + _att))
                            refreshed = self.ex.fetch_order(str(exch_oid), symbol_full)
                            actual_filled = float((refreshed or {}).get("filled") or 0.0)
                            if actual_filled > 0:
                                for k in ("average", "price"):
                                    v = (refreshed or {}).get(k)
                                    if v:
                                        try:
                                            fv = float(v)
                                            if fv > 0:
                                                fill_price = fv
                                                break
                                        except (TypeError, ValueError):
                                            continue
                                order = refreshed or order
                                break
                    except Exception:
                        actual_filled = 0.0
                if actual_filled <= 0:
                    try:
                        from config.exchange_config import safe_fetch_positions
                        positions = safe_fetch_positions(self.ex, [symbol_full]) or []
                        remaining_live = None
                        old_amount = float(d.get("amount", 0) or 0)
                        for pos in positions:
                            if (pos.get("symbol") or "") != symbol_full:
                                continue
                            remaining_live = abs(float(pos.get("contracts")
                                                       or pos.get("size") or 0.0))
                            break
                        if remaining_live is not None:
                            actual_filled = max(0.0, old_amount - remaining_live)
                    except Exception:
                        actual_filled = 0.0
                if actual_filled <= 0:
                    log_event(f"Partial-TP {sym}: fill unverified - no PnL booked; "
                              f"state unchanged, retry next tick", "WARN")
                    return False
                partial_amount = min(partial_amount, actual_filled)
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
        )
        try:
            accounting_ok = bool(save_trade_db(**partial_trade))
        except Exception as e:
            accounting_ok = False
            log_event(f"save_trade_db partial {sym} failed: {e}", "WARN")

        # Update state
        new_amount = safe_remaining(float(d.get("amount", 0)), partial_amount)
        new_invested = max(0.0, margin - notional_partial / max(lev, 1))
        new_fees = float(d.get("fees_paid", 0.0)) + partial_fee
        prev_realized = float(d.get("partial_profit_realized", 0.0))
        prev_funding_booked = float(d.get("funding_booked_on_partials", 0.0))
        updates = {
            "partial_sold": True,
            "break_even": True,
            "partial_tp_blocked_min_notional": False,
            "amount": new_amount,
            "invested_usdt": new_invested,
            "fees_paid": new_fees,
            "partial_profit_realized": prev_realized + profit_partial,
            "funding_booked_on_partials": prev_funding_booked + funding_partial,
        }
        if not accounting_ok:
            pending = list(d.get("accounting_pending_partials") or [])
            pending.append(partial_trade)
            updates["accounting_pending_partials"] = pending
            log_event(
                f"{sym}: futures partial-TP DB save failed - slice kept "
                f"for accounting retry", "WARN")
        self.state.update_many(sym, updates)

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
        from config.exchange_config import reduce_only_params
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from trading.risk_manager import analyze_and_adapt
        from bot_utils import (create_order_with_retry,
                                 extract_order_fee_futures,
                                 extract_or_estimate_futures_fee,
                                 fetch_or_estimate_funding,
                                 is_no_position_error,
                                 verify_position_closed)

        symbol_full = f"{sym}/USDT:USDT"
        if d.get("verified_flat_pending_accounting"):
            log_event(
                f"{sym}: position already verified flat; waiting for "
                f"reconcile/offline accounting",
                "WARN",
            )
            return
        fill_price = curr
        close_fee = 0.0
        raw_amount = abs(float(d.get("amount", 0)))
        contract_size = self._get_contract_size(symbol_full)
        exch_oid = None  # real order id - unique trade-dedup key
        margin_mode = str(d.get("margin_mode") or self.C("MARGIN_MODE", "isolated") or "isolated").lower()

        if self.simulation:
            if raw_amount > 0 and fill_price > 0:
                from bot_utils.fee_math import taker_fee_rate
                close_fee = (raw_amount * contract_size * fill_price
                             * taker_fee_rate(self.ex, symbol_full, 0.0006))

        if not self.simulation:
            order = None
            close_amount = raw_amount
            order_filled = 0.0
            try:
                close_side = "sell" if pos_type == "LONG" else "buy"
                try:
                    from config.exchange_config import safe_amount_to_precision
                    close_amount = float(safe_amount_to_precision(
                        self.ex, symbol_full, raw_amount))
                except Exception:
                    close_amount = float(raw_amount)
                if close_amount <= 0 and raw_amount > 0:
                    close_amount = float(raw_amount)

                order = create_order_with_retry(
                    self.ex, symbol_full, close_side, close_amount,
                    params=reduce_only_params(
                        position_side=("long" if pos_type == "LONG" else "short"),
                        margin_mode=margin_mode,
                        leverage=max(1, int(__import__("math").ceil(lev))),
                    ),
                    shutdown_event=self._shutdown_event,
                    action_label=f"close {sym}",
                    log_event=log_event,
                    log_struct=log_struct,
                )
                exch_oid = order.get("id") or order.get("orderId")
                try:
                    order_filled = max(0.0, float(order.get("filled") or 0.0))
                except (TypeError, ValueError):
                    order_filled = 0.0
                try:
                    from bot_utils.futures_exits import _resolve_fill_price
                    fill_price, _fill_src = _resolve_fill_price(
                        self.ex, symbol_full, order, fill_price, log_event)
                except Exception:
                    for k in ("average", "price"):
                        v = order.get(k)
                        if v:
                            try:
                                fv = float(v)
                                if fv > 0:
                                    fill_price = fv
                                    break
                            except (TypeError, ValueError):
                                continue
                close_fee = 0.0
            except Exception as e:
                if is_no_position_error(e):
                    pending_oid = d.get("pending_close_order_id")
                    if not pending_oid:
                        try:
                            closed, remaining = verify_position_closed(self.ex, symbol_full)
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
                                self.state.update_many(sym, {
                                    "verified_flat_pending_accounting": True,
                                    "verified_flat_reason": reason,
                                    "verified_flat_at": _utc_now_str(),
                                })
                            except Exception as state_err:
                                log_event(
                                    f"{sym}: failed to mark verified-flat "
                                    f"state: {state_err}",
                                    "WARN",
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
                        try:
                            fill_price = float(d.get("pending_close_price") or fill_price)
                        except (TypeError, ValueError):
                            pass
                        try:
                            close_fee = float(d.get("pending_close_fee") or close_fee)
                        except (TypeError, ValueError):
                            pass
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
        if not self.simulation:
            try:
                closed, remaining = verify_position_closed(self.ex, symbol_full)
            except Exception as e:
                if order_filled > 0 and fill_price > 0:
                    try:
                        from bot_utils.close_fragments import add_close_fragment_update
                        frag_fee = extract_or_estimate_futures_fee(
                            self.ex, order or {}, symbol_full, fill_price,
                            amount=order_filled, contract_size=contract_size,
                        )
                        self.state.update_many(sym, add_close_fragment_update(
                            d, amount=order_filled, price=fill_price,
                            fee=frag_fee, order_id=exch_oid,
                        ))
                    except Exception:
                        pass
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
                            self.state.update_many(sym, add_close_fragment_update(
                                d, amount=fragment, price=fill_price,
                                fee=frag_fee, order_id=exch_oid,
                            ))
                    except Exception:
                        pass
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
                close_fee = extract_order_fee_futures(order or {})
                if close_fee <= 0:
                    try:
                        close_fee = extract_or_estimate_futures_fee(
                            self.ex, order or {}, symbol_full, fill_price,
                            amount=raw_amount, contract_size=contract_size,
                        )
                    except Exception:
                        close_fee = 0.0

        # PnL with real fill + funding + proportional entry fee
        move_pct_real = price_move_pct(entry, fill_price, pos_type) if entry > 0 else move_pct
        pnl_real, _ = (calc_unrealized_pnl(entry, fill_price, margin, lev, pos_type)
                        if entry > 0 and margin > 0 else (pnl_usdt, 0.0))
        mfe_pct = float(d.get("max_profit_pct", move_pct_real) or move_pct_real)
        mae_pct = float(d.get("min_profit_pct", move_pct_real) or move_pct_real)
        giveback_pct = max(0.0, mfe_pct - move_pct_real)

        initial_entry_fee = float(d.get("initial_entry_fee", d.get("fees_paid", 0.0)))
        original_amount = float(d.get("original_amount", d.get("amount", 0)))
        current_amount = float(d.get("amount", 0))
  # Safe helper  defends against the original_amount=0 + partial_sold=True
        # combination (would otherwise double-deduct the entry fee).
        partial_sold = bool(d.get("partial_sold"))
        proportional_entry_fee = safe_proportional_fee(
            initial_entry_fee, current_amount, original_amount,
            partial_sold=partial_sold
        )

        funding_pd = float(d.get("funding_paid", 0.0))
        funding_booked = float(d.get("funding_booked_on_partials", 0.0))
        # Scale by ACTUAL remaining ratio via the safe helper so a corrupted
        # original_amount can't crash the math.
        if partial_sold:
            funding_pd = safe_remaining_funding(
                funding_pd, current_amount, original_amount,
                partial_sold=True, booked_on_partials=funding_booked,
            )
        if not self.simulation:
            try:
                notional = margin * lev if margin > 0 else 0.0
                if partial_sold and current_amount > 0 and original_amount > 0:
                    remaining_ratio = current_amount / original_amount
                    if 0 < remaining_ratio < 1:
                        notional = notional / remaining_ratio
                realized = fetch_or_estimate_funding(
                    self.ex, symbol_full, d.get("buy_time"),
                    notional_usdt=notional, pos_type=pos_type,
                    fallback_state_value=float(d.get("funding_paid", 0.0))
                )
                if realized is not None:
                    # Safe helper for the realized-funding scaling too
                    if partial_sold:
                        realized = safe_remaining_funding(
                            realized, current_amount, original_amount,
                            partial_sold=True,
                            booked_on_partials=funding_booked,
                        )
                    funding_pd = realized
            except Exception:
                pass

        slice_fees = proportional_entry_fee + close_fee
        profit_usdt = round(pnl_real - slice_fees - funding_pd, 2)
        # Round-trip fee total for the whole trade (Telegram display only).
        lifetime_fees = float(d.get("fees_paid", 0.0)) + close_fee

        # Persist accounting first. Once a live close is verified flat, the
        # local state is the last recoverable source for PnL/accounting. Do not
        # remove it unless the DB trade row was actually written.
        buy_time = d.get("buy_time", "")
        sell_time = _utc_now_str()
        accounting_ok = False
        accounting_error = None
        try:
            accounting_ok = bool(save_trade_db(
                bot_name=self.BOT_NAME, mode_is_sim=self.simulation, symbol=sym,
                buy_price=entry, sell_price=fill_price,
                buy_time=buy_time, sell_time=sell_time,
                profit_pct=move_pct_real, profit_usdt=profit_usdt,
                invested_usdt=margin, reason=reason,
                rsi_15m=d.get("rsi_15m"), rsi_1h=d.get("rsi_1h"),
                rsi_4h=d.get("rsi_4h"), change_pct=d.get("change_pct"),
                btc_trend=d.get("btc_trend"), fear_greed=d.get("fear_greed"),
                is_futures=True, position_type=pos_type, leverage=lev,
                liquidation_price=liq_price, funding_paid=funding_pd,
                # This row's OWN slice fees (proportional entry + close), so
                # SUM(fees_usdt) over a trade's rows = the true round-trip total
                # and never double-counts the entry fee against the partial row.
                fees_usdt=slice_fees,
                exchange_order_id=exch_oid,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                giveback_pct=giveback_pct,
            ))
            if not accounting_ok:
                raise RuntimeError("save_trade_db returned False")
        except Exception as e:
            accounting_error = e
            log_event(
                f"save_trade_db {sym} failed after verified flat close: {e}. "
                f"State kept for accounting recovery.", "WARN")
            try:
                self.state.update_many(sym, {
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
                })
            except Exception as state_err:
                log_event(
                    f"mark accounting_pending {sym} failed: {state_err}",
                    "WARN")
            return
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
            partial_realized = float(d.get("partial_profit_realized", 0.0))
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
