"""
core/trend_bot.py  Majors trend-following bot (the repurposed "Trend" slot).

Validated 2026-06-13 (tools/trend_check.py, 720d MEXC majors): a long/flat
ENSEMBLE trend rule on BTC/ETH/BNB/XRP/SOL beat buy-and-hold on BOTH return
AND drawdown, robustly across SMA lengths (7/7) and coins (4/5). SPOT, NO
LEVERAGE  the edge is risk-adjusted; leverage would re-inflate the very
drawdown the strategy removes.

Design: reuse the whole SpotBot lifecycle (state, SIM accounting, killswitch,
shutdown, reconcile, UI state) and ONLY swap the two decision loops:

  SCAN loop  buy each major that has turned IN-trend and isn't held
  MONITOR loop sell each held major that has fallen OUT of trend, + a simple
                  daily-loss killswitch

Both decisions come from trading.trend_signal (pure, unit-tested). The buy path
reuses ScanMixin._place_buy_order; the sell path reuses
ExitsMixin._execute_full_exit  so order placement, fills, fees, PnL, SIM and
state removal are exactly the proven spot machinery.

Cadence is daily-ish (TREND_CHECK_HOURS, default 12h)  the signal only changes
on new daily candles, so there is nothing to gain from scanning faster.
"""
from __future__ import annotations

import time as _time
from typing import Optional, Tuple, Dict, List

from bot_utils.api_budget import try_consume_api_call
from core.spot_bot import SpotBot
from bot_utils.safe_numeric import parse_ohlcv_closes, safe_positive_float
from trading.trend_signal import (is_in_trend, params_from_cfg, TrendParams,
                                   has_full_history, bars_required)
from trading.vol_target import (realized_vol, vol_target_multiplier,
                                basket_median_vol)


class TrendBot(SpotBot):
    """Long/flat ensemble trend-following over a fixed basket of majors."""

    # Purely mechanical strategy  no news, no LLM. USES_LLM=False makes
    # run() skip the eager news-module import entirely (see SpotBot.run).
    USES_LLM = False

    DAILY_CACHE_TTL_SEC = 3600        # re-fetch daily candles at most hourly

    #  Config helpers 

    def _trend_universe(self) -> List[str]:
        raw = str(self.C("TREND_UNIVERSE", "BTC,ETH,BNB,XRP,SOL"))
        return [s.strip().upper() for s in raw.split(",") if s.strip()]

    def _trend_params(self) -> TrendParams:
        return params_from_cfg(self.C)

    def _check_interval_sec(self) -> int:
        try:
            hours = float(self.C("TREND_CHECK_HOURS", 12))
        except (TypeError, ValueError):
            hours = 12.0
        return max(300, int(hours * 3600))            # floor at 5 min

    #  Market data 

    def _get_daily_closes(self, sym: str, need: int) -> List[float]:
        """Daily closes for `sym`, cached ~1h (signal only moves on new bars)."""
        wall_now = _time.time()
        cache_now = _time.monotonic()
        cache = getattr(self, "_dc_cache", None)
        if cache is None:
            cache = self._dc_cache = {}
        ent = cache.get(sym)
        cache_age = cache_now - ent[0] if ent else None
        if (
            ent
            and cache_age is not None
            and 0 <= cache_age < self.DAILY_CACHE_TTL_SEC
            and len(ent[1]) >= need
        ):
            return ent[1]
        try:
            if not try_consume_api_call("trend_fetch_ohlcv"):
                return []
        except Exception:
            return []
        try:
            bars = self.ex.fetch_ohlcv(f"{sym}/USDT", "1d", limit=need + 6)
            # Drop the still-FORMING current-day candle so the signal is based
            # on COMPLETED daily closes  exactly like the validated backtest
            # (which acted on closed candles). Without this the live bot reacts
            # to an intraday partial close and whipsaws more than tested.
            try:
                from core.clock import now_ms as _clock_now_ms
                current_time_ms = int(_clock_now_ms())
            except Exception:
                current_time_ms = int(wall_now * 1000)
            closes = parse_ohlcv_closes(
                bars,
                expected_interval_ms=86_400_000,
                now_ms=current_time_ms,
            )
            if closes is None:
                raise ValueError("invalid OHLCV close snapshot")
        except Exception as e:
            self._log_error(f"trend fetch_ohlcv {sym}", e)
            return []
        cache[sym] = (cache_now, closes)
        return closes

    def _current_price(self, sym: str) -> float:
        try:
            if not try_consume_api_call("trend_fetch_ticker"):
                return 0.0
        except Exception:
            return 0.0
        try:
            t = self.ex.fetch_ticker(f"{sym}/USDT")
            price = safe_positive_float(t.get("last"), 0.0)
            if price > 0:
                return price
            return safe_positive_float(t.get("close"), 0.0)
        except Exception:
            return 0.0

    def _handle_exit_recovery_gate(self, sym: str, d: dict) -> bool:
        """Handle pending accounting/conflict state before TREND exits."""
        if d.get("accounting_already_booked"):
            from core.spot_bot_exits import ExitsMixin
            ExitsMixin._cleanup_accounted_close_state(self, sym, d)
            return True
        if d.get("accounting_pending"):
            from core.spot_bot_reconcile import (
                _defer_spot_accounting_retry,
                _record_spot_offline_close,
                _spot_accounting_retry_due,
            )
            if not _spot_accounting_retry_due(d):
                return True
            try:
                if _record_spot_offline_close(self, sym, d):
                    booked = dict(d)
                    booked.update({
                        "accounting_already_booked": True,
                        "accounting_booked_sell_time": (
                            d.get("accounting_pending_sell_time")),
                        "accounting_booked_exchange_order_id": (
                            d.get("accounting_pending_exchange_order_id")),
                        "accounting_booked_reason": (
                            d.get("accounting_pending_reason")
                            or "Trend offline close"),
                    })
                    from core.spot_bot_exits import ExitsMixin
                    ExitsMixin._cleanup_accounted_close_state(self, sym, booked)
                else:
                    _defer_spot_accounting_retry(self, sym, d)
            except Exception as exc:
                self._log_error(f"trend pending accounting retry {sym}", exc)
                _defer_spot_accounting_retry(self, sym, d)
            return True
        if d.get("claim_conflict"):
            try:
                from core.logger import log_event
                warned = getattr(self, "_claim_conflict_warned", set())
                if sym not in warned:
                    log_event(
                        f"[{self.BOT_NAME}] {sym}: registry claim conflict - "
                        f"monitor skipped fail-closed; run claim/state repair",
                        "ERROR",
                    )
                    warned.add(sym)
                    self._claim_conflict_warned = warned
            except Exception:
                pass
            return True
        self._retry_pending_partial_accounting(sym, d)
        d_live = self.state.get(sym) or d
        return bool(d_live.get("accounting_pending_partials"))

    def _evaluate(self, sym: str, held: bool,
                  p: Optional[TrendParams] = None) -> Tuple[bool, int, Dict[str, bool]]:
        """Return (in_trend, votes, detail). On insufficient data, do NOT act:
        keep a held position held and a flat coin flat."""
        if p is None:
            p = self._trend_params()
        need = bars_required(p)
        closes = self._get_daily_closes(sym, need)
        if not has_full_history(closes, p):
            self._warn_short_history(sym, len(closes), need)
            return held, 0, {}
        return is_in_trend(closes, p, currently_held=held)

    def _warn_short_history(self, sym: str, got: int, need: int) -> None:
        from core.logger import log_event
        seen = getattr(self, "_short_hist_warned", None)
        if seen is None:
            seen = self._short_hist_warned = set()
        if sym in seen:
            return
        seen.add(sym)
        log_event(f"Trend {sym}: insufficient daily history "
                  f"({got}/{need} closed bars); symbol skipped", "INFO")

    #  SCAN loop: enter coins that turned in-trend 

    def _scan_loop(self):
        from core.logger import log_event
        interval = self._check_interval_sec()
        log_event(f"Trend scan-loop started (every {interval/3600:.1f}h)", "INFO")
        while not self._shutdown_event.is_set():
            try:
                self._trend_buy_pass()
            except Exception as e:
                log_event(f"Trend scan error: {e}", "WARN")
                self._log_error("trend scan", e)
            if self._shutdown_event.wait(timeout=interval):
                return

    def _trend_buy_pass(self):
        from core.logger import log_event, log_struct
        if self.safe_mode is not None and self.safe_mode.is_active():
            return
        try:
            from trading.risk_manager import is_bot_paused
            paused, why = is_bot_paused(
                self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
        except Exception as e:
            log_event(f"Trend pause check failed ({type(e).__name__}) - "
                      f"blocking new entries", "WARN")
            self._log_error("trend pause check", e)
            return
        if paused:
            log_event(f"Trend paused: {why}", "WAIT")
            return
        p = self._trend_params()
        size = float(self.C("POSITION_SIZE", 20.0))
        try:
            max_trades = int(float(self.C("MAX_OPEN_TRADES", 5)))
        except (TypeError, ValueError):
            max_trades = 5

        # Inverse-vol sizing (opt-in): scale each coin's size by the basket's
        # median vol / its own  calm coins bigger, wild coins smaller, total
        # ~unchanged. Vols come from the daily closes already cached for the
        # signal, so no extra fetches.
        vt_on = str(self.C("TREND_VOL_TARGET", 0)).strip().lower() in (
            "1", "true", "yes", "on")
        vols, med = {}, None
        if vt_on:
            _lb = int(float(self.C("TREND_VOL_TARGET_LOOKBACK", 30)))
            _need = max(bars_required(p), _lb + 1)
            for _s in self._trend_universe():
                _v = realized_vol(self._get_daily_closes(_s, _need), _lb)
                if _v:
                    vols[_s] = _v
            med = basket_median_vol(list(vols.values()))
            if med is None:
                seen = getattr(self, "_vol_target_warned", False)
                if not seen:
                    self._vol_target_warned = True
                    log_event("Trend vol-targeting unavailable "
                              "(insufficient volatility history); using flat "
                              "position size", "INFO")

        free = None
        if not self.simulation:
            try:
                from bot_utils import safe_fetch_balance_usdt
                free = safe_fetch_balance_usdt(self.ex)
            except Exception:
                free = None
            if free is None:
                log_event(
                    "Trend live balance unavailable - blocking buy-side "
                    "fail-closed",
                    "WAIT",
                )
                return

        opened = 0
        parts = []                                        # per-coin vote summary
        regime_refresh_attempted = False
        for sym in self._trend_universe():
            if self._shutdown_event.is_set():
                return
            held = self.state.has(sym)
            in_trend, votes, detail = self._evaluate(sym, held=held, p=p)
            # Leeres detail = _evaluate hat den Insufficient-Data-Guard getroffen
            # (zu wenige Kerzen) und sich enthalten  NICHT dasselbe wie ein
            # echtes 0/3 "kein Aufwrtstrend". Als 'n/a' ausweisen, damit eine
            # Datenlcke nicht wie ein klares No-Trend aussieht.
            vote_str = "n/a" if not detail else f"{votes}/3"
            parts.append(f"{sym} {vote_str}" + ("(held)" if held else ""))
            if held:
                continue                                  # already long this coin
            if not in_trend:
                continue  # not in an uptrend  stay flat
            # COEXISTENCE: skip a coin another bot on this account already holds.
            try:
                from core.database import is_claimed_by_other
                if is_claimed_by_other(sym, self.BOT_NAME, is_futures=False):
                    continue
            except Exception as exc:
                log_event(
                    f"Trend entry scan aborted: claim registry unavailable "
                    f"({type(exc).__name__})",
                    "WARN",
                )
                return
            if self.state.count() >= max_trades:
                continue                                  # at Max Open Trades
            coin_size = size * (vol_target_multiplier(vols.get(sym), med)
                                if vt_on else 1.0)
            try:
                size_cap = float(self.C("POSITION_SIZE_MAX", 0.0) or 0.0)
            except (TypeError, ValueError):
                size_cap = 0.0
            if size_cap > 0:
                coin_size = min(coin_size, size_cap)
            if not self.simulation and free is not None and free < coin_size:
                log_event(f"Trend: skip {sym}  need {coin_size:.0f} USDT, "
                          f"have {free:.0f}", "INFO")
                continue
            price = self._current_price(sym)
            if price <= 0:
                continue
            try:
                paused, why = is_bot_paused(
                    self.BOT_NAME,
                    exchange=self.ex,
                    simulation=self.simulation,
                )
            except Exception as e:
                log_event(f"Trend pause recheck failed ({type(e).__name__}) - "
                          f"blocking new entries", "WARN")
                self._log_error("trend pause recheck", e)
                return
            if paused:
                log_event(f"Trend paused before {sym}: {why}", "WAIT")
                return
            # Defense-in-depth: re-check we still don't hold this coin right
            # before committing capital  closes any has()->add() window.
            if self.state.has(sym):
                continue
            from trading.entry_lifecycle import (emit_entry_lifecycle,
                                                 new_entry_id)

            entry_mode = "SIM" if self.simulation else "LIVE"
            entry_id = new_entry_id(
                bot=self.BOT_NAME,
                symbol=sym,
                mode=entry_mode,
                direction="BUY",
            )
            from trading.expectancy_telemetry import expectancy_feature_value

            expectancy_features = {
                "trend_votes": expectancy_feature_value(votes),
                "realized_vol": expectancy_feature_value(vols.get(sym)),
                "size_multiplier": float(
                    vol_target_multiplier(vols.get(sym), med)
                    if vt_on
                    else 1.0
                ),
            }
            if not regime_refresh_attempted:
                regime_refresh_attempted = True
                try:
                    # TREND scans only about twice per day and otherwise never
                    # calls the shared regime reader.  Refresh once immediately
                    # before the first Candidate so the atomic Candidate bundle
                    # can bind fresh evidence.  The result is telemetry only:
                    # it must not alter this strategy's signal or entry outcome.
                    from trading.market_filters import get_market_regime

                    get_market_regime(self.ex)
                except Exception as exc:
                    self._log_error("trend candidate regime refresh", exc)
            from trading.expectancy_telemetry import emit_expectancy_candidate

            emit_expectancy_candidate(
                bot=self.BOT_NAME,
                entry_id=entry_id,
                symbol=sym,
                mode=entry_mode,
                direction="LONG",
                features=expectancy_features,
            )
            if not self.simulation:
                from trading.entry_admission import evaluate_entry_admission
                from trading.portfolio_risk import portfolio_limits_from_config

                from shared_limits import normalize_gate_mode
                portfolio_mode = normalize_gate_mode(
                    self.C("PORTFOLIO_RISK_MODE", "shadow")
                )
                expectancy_mode = normalize_gate_mode(
                    self.C("NET_EXPECTANCY_MODE", "shadow")
                )
                admission = evaluate_entry_admission(
                    exchange=self.ex,
                    intent_id=entry_id,
                    bot_name=self.BOT_NAME,
                    symbol=f"{sym}/USDT",
                    side="LONG",
                    requested_notional=coin_size,
                    portfolio_mode=portfolio_mode,
                    expectancy_mode=expectancy_mode,
                    account_type="spot",
                    features=expectancy_features,
                    limits=portfolio_limits_from_config(
                        self.C, max_net_default=100.0,
                        max_beta_default=100.0,
                    ),
                )
                log_struct(
                    "entry_admission",
                    bot=self.BOT_NAME,
                    entry_id=entry_id,
                    symbol=sym,
                    portfolio_mode=portfolio_mode,
                    portfolio_allowed=admission.portfolio.allowed,
                    portfolio_shadow_allowed=admission.portfolio.shadow_allowed,
                    portfolio_reasons=list(admission.portfolio.reasons),
                    expectancy_mode=expectancy_mode,
                    expectancy_allowed=admission.expectancy.allowed,
                    expectancy_shadow_allowed=admission.expectancy.shadow_allowed,
                    expected_net_bps=admission.expectancy.expected_net_bps,
                    model_version=admission.expectancy.model_version,
                )
                if not admission.allowed:
                    emit_entry_lifecycle(
                        entry_id,
                        bot=self.BOT_NAME,
                        symbol=sym,
                        stage="blocked",
                        mode=entry_mode,
                        reason="entry_admission",
                    )
                    continue
            if not self.simulation:
                from core.database import claim_symbol_for_entry
                if not claim_symbol_for_entry(
                    self.BOT_NAME,
                    sym,
                    "SPOT",
                    intent_id=entry_id,
                    notional_usdt=coin_size,
                    mode=entry_mode,
                    reservation_ceiling_usdt=(
                        admission.portfolio.reservation_ceiling_usdt
                        if portfolio_mode == "enforce" else None
                    ),
                ):
                    from core.logger import log_event as _lev
                    _lev(f"Trend: {sym} claimed by another bot  skip "
                         f"(coexistence)", "WAIT")
                    emit_entry_lifecycle(
                        entry_id,
                        bot=self.BOT_NAME,
                        symbol=sym,
                        stage="blocked",
                        mode=entry_mode,
                        reason="claim_conflict",
                    )
                    continue
            _trend_claimed = not self.simulation
            emit_entry_lifecycle(
                entry_id,
                bot=self.BOT_NAME,
                symbol=sym,
                stage="order_attempt",
                mode=entry_mode,
            )
            from core.spot_bot_scan import SpotBuyOutcomeUnknown

            try:
                entry = self._place_buy_order_with_runtime_guard(
                    sym,
                    {"price": price},
                    coin_size,
                    entry_id=entry_id,
                )
            except SpotBuyOutcomeUnknown as _buy_exc:
                emit_entry_lifecycle(
                    entry_id,
                    bot=self.BOT_NAME,
                    symbol=sym,
                    stage="order_unknown",
                    mode=entry_mode,
                    reason=type(_buy_exc).__name__,
                )
                raise
            except Exception as _buy_exc:
                emit_entry_lifecycle(
                    entry_id,
                    bot=self.BOT_NAME,
                    symbol=sym,
                    stage="order_failed",
                    mode=entry_mode,
                    reason=type(_buy_exc).__name__,
                )
                clean_failure = not self.state.has(sym)
                released = not _trend_claimed
                if _trend_claimed:
                    released = self._release_entry_claim_if_untracked(
                        sym,
                        entry_id=entry_id,
                    )
                    if released:
                        from core.database import release_portfolio_reservation

                        release_portfolio_reservation(entry_id)
                if clean_failure and released:
                    log_event(
                        f"Trend entry {sym} failed "
                        f"({type(_buy_exc).__name__}); continuing candidate scan",
                        "WARN",
                    )
                    self._log_error(f"trend entry {sym}", _buy_exc)
                    continue
                # A provisional row or unreleased ownership means the failure
                # is no longer clean. Abort the pass so no later entry can race
                # unresolved post-submit recovery.
                raise
            if entry is None:
                emit_entry_lifecycle(
                    entry_id,
                    bot=self.BOT_NAME,
                    symbol=sym,
                    stage="order_failed",
                    mode=entry_mode,
                    reason="no_verified_fill",
                )
                if _trend_claimed:
                    released = self._release_entry_claim_if_untracked(
                        sym,
                        entry_id=entry_id,
                    )
                    if released:
                        from core.database import release_portfolio_reservation

                        release_portfolio_reservation(entry_id)
                continue
            amount, fill_price, gross_amount, invested_usdt, entry_fee = entry
            sim_tca_pending = None
            if self.simulation:
                sim_tca_pending = self._new_simulated_entry_tca_pending(
                    entry_id=entry_id,
                    symbol=f"{sym}/USDT",
                    amount=amount,
                    fill_price=fill_price,
                    fee_rate=(entry_fee / invested_usdt),
                    notional_usdt=invested_usdt,
                )
            state_ok = self._add_trend_state(
                sym, fill_price, amount, gross_amount, invested_usdt,
                entry_fee, votes, entry_id,
                sim_tca_pending=sim_tca_pending,
            )
            if state_ok is None:
                log_event(
                    f"Trend BUY {sym}: final state promotion was superseded "
                    "by another entry generation; no rollback was sent",
                    "ERROR",
                )
                continue
            if state_ok is False:
                emit_entry_lifecycle(
                    entry_id,
                    bot=self.BOT_NAME,
                    symbol=sym,
                    stage="state_failed",
                    mode=entry_mode,
                    reason="post_fill_state_write",
                )
            if state_ok is False and not self.simulation:
                log_event(
                    f"Trend BUY {sym}: state write failed after LIVE fill - "
                    f"attempting immediate rollback sell",
                    "ERROR",
                )
                try:
                    from bot_utils.spot_exits import (
                        rollback_spot_entry_after_state_failure,
                        spot_entry_rollback_was_fully_filled,
                    )
                    order, sold_amount = rollback_spot_entry_after_state_failure(
                        self.ex,
                        f"{sym}/USDT",
                        amount,
                        entry_id=entry_id,
                        bot_name=self.BOT_NAME,
                    )
                    if spot_entry_rollback_was_fully_filled(
                            order, sold_amount):
                        completed = (
                            self._complete_verified_spot_entry_rollback(
                                sym,
                                entry_id=entry_id,
                                reason=(
                                    "trend state write failed after live buy"
                                ),
                            )
                        )
                        if completed:
                            log_event(
                                f"Trend BUY {sym}: rollback sell and cleanup "
                                "completed after state failure",
                                "WARN",
                            )
                        else:
                            log_event(
                                f"Trend BUY {sym}: rollback sell verified "
                                "flat but durable claim/state cleanup is "
                                "incomplete",
                                "ERROR",
                            )
                    else:
                        log_event(
                            f"Trend BUY {sym}: CRITICAL rollback sell not "
                            f"verified after state failure; claim kept for "
                            f"manual recovery",
                            "ERROR",
                        )
                except Exception as rb_exc:
                    log_event(
                        f"Trend BUY {sym}: CRITICAL untracked live position "
                        f"risk after state failure; rollback sell failed "
                        f"({rb_exc})",
                        "ERROR",
                    )
                    self._log_error(
                        f"trend rollback after state failure {sym}", rb_exc)
                continue
            if state_ok is False:
                # SIM has no exchange position to roll back. Do not report an
                # opened position when its state was not persisted.
                continue
            if sim_tca_pending is not None:
                self._finalize_simulated_entry_tca(sym, sim_tca_pending)
            if not self.simulation:
                from core.database import release_portfolio_reservation

                try:
                    release_portfolio_reservation(entry_id, status="CONSUMED")
                except Exception as reservation_exc:
                    try:
                        log_event(
                            f"Trend BUY {sym}: portfolio reservation consume "
                            "failed; reservation remains ACTIVE fail-closed",
                            "ERROR",
                        )
                    except Exception:
                        pass
                    try:
                        self._log_error(
                            f"consume trend portfolio reservation {sym}",
                            reservation_exc,
                        )
                    except Exception:
                        pass
            emit_entry_lifecycle(
                entry_id,
                bot=self.BOT_NAME,
                symbol=sym,
                stage="opened",
                mode=entry_mode,
                fill_price=fill_price,
                size_usdt=float(coin_size),
            )
            if free is not None:
                free = max(0.0, free - coin_size)
            opened += 1
            log_event(f" Trend BUY {sym} @ {fill_price:.6f}  "
                      f"({votes}/3 votes, {coin_size:.0f} USDT)", "INFO")

        # Visibility: ALWAYS report what the bot saw, so a 'no trade' scan is
        # clearly 'working, just no uptrend' rather than looking dead.
        log_event(f"Trend scan: {'  '.join(parts)}  opened {opened}, "
                  f"holding {self.state.count()}/{max_trades}", "SCAN")

    def _add_trend_state(self, sym, fill_price, amount, gross_amount,
                         invested_usdt, entry_fee, votes, entry_id,
                         *, sim_tca_pending=None):
        # _place_buy_order already wrote a PROVISIONAL row (zombie protection);
        # patch it in place with the corrected NET amount + fees instead of a
        # second full add. Fall back to add() if the provisional didn't land.
        from core.logger import _date as _utc_now_str
        fields = {
            "buy": fill_price,
            "highest": fill_price,
            "invested_usdt": invested_usdt,
            "amount": amount,
            "original_amount": amount,
            "partial_sold": False,
            "be_active": False,
            "break_even": False,
            "initial_entry_fee": entry_fee,
            "fees_paid": entry_fee,
            "strategy": "trend",
            "entry_votes": votes,
            "entry_id": entry_id,
            "provisional": False,
        }
        if sim_tca_pending is not None:
            fields[self._SIM_TCA_PENDING_FIELD] = sim_tca_pending
        from bot_utils.trade_state import promote_position_generation

        create_fields = dict(fields)
        create_fields["buy_time"] = _utc_now_str()
        return promote_position_generation(
            self.state,
            sym,
            fields,
            {"entry_id": entry_id},
            create_fields=create_fields,
        )

    #  MONITOR loop: exit coins that fell out of trend, + killswitch 

    def _monitor_loop(self):
        from core.logger import log_event
        interval = self._check_interval_sec()
        log_event(f"Trend monitor-loop started (every {interval/3600:.1f}h)",
                  "INFO")
        while not self._shutdown_event.is_set():
            try:
                self._trend_killswitch()
                self._trend_exit_pass()
            except Exception as e:
                log_event(f"Trend monitor error: {e}", "WARN")
                self._log_error("trend monitor", e)
            if self._shutdown_event.wait(timeout=interval):
                return

    def _trend_exit_pass(self):
        from core.logger import log_event
        p = self._trend_params()
        try:
            disaster = float(self.C("INITIAL_STOP_LOSS", -30.0))
        except (TypeError, ValueError):
            disaster = -30.0
        for sym in list(self.state.keys()):
            if self._shutdown_event.is_set():
                return
            try:
                self._trend_exit_one(sym, p, disaster)
            except Exception as e:
                log_event(
                    f"Trend exit {sym} failed: {type(e).__name__}",
                    "WARN",
                )
                self._log_error(f"trend exit {sym}", e)

    def _trend_exit_one(self, sym: str, p, disaster: float) -> None:
        from core.logger import log_event

        d = self.state.get(sym)
        if not d or self._handle_exit_recovery_gate(sym, d):
            return
        curr = self._current_price(sym)
        buy = float(d.get("buy", 0) or 0)

        # Disaster brake: hard stop far below entry (flash-crash / gap
        # protection). The trend exit normally fires well before this; pure
        # last-resort safety so an overnight gap can't run unbounded.
        if curr > 0 and buy > 0 and (curr / buy - 1.0) * 100.0 <= disaster:
            log_event(f" Trend disaster-stop {sym}: "
                      f"{(curr/buy-1)*100:.1f}% <= {disaster:.0f}%", "WARN")
            self._execute_full_exit(sym, d, curr, "Disaster stop")
            return

        in_trend, votes, _ = self._evaluate(sym, held=True, p=p)
        if in_trend or curr <= 0:
            return
        log_event(f" Trend EXIT {sym}  out of trend ({votes}/3 votes)",
                  "INFO")
        self._execute_full_exit(sym, d, curr, f"Trend exit ({votes}/3 votes)")

    def _trend_killswitch(self):
        """Simple daily-loss killswitch (spot, no leverage  no liquidation, so
        a soft SAFE_MODE stop on new buys is sufficient)."""
        try:
            from core.database import get_today_pnl
            from core.logger import log_event
            if self.safe_mode is None or self.safe_mode.is_active():
                return
            info = get_today_pnl(self.BOT_NAME, mode_is_sim=self.simulation)
            today = info.get("total_profit", 0.0)
            try:
                max_loss = float(self.C("MAX_DAILY_LOSS", -50.0))
            except (TypeError, ValueError):
                max_loss = -50.0
            if today <= max_loss:
                log_event(f" KILLSWITCH (Trend): daily {today:+.2f} USDT "
                          f"<= {max_loss}  SAFE_MODE, no new buys", "WARN")
                self.safe_mode.trigger(
                    f"daily-loss killswitch ({today:+.2f} USDT)")
        except Exception as e:
            self._log_error("trend killswitch", e)
