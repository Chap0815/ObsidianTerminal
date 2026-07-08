"""
core/futures_bot_scan.py  Scan thread + LONG/SHORT entry logic.

Runs at SCAN_INTERVAL (~150s) and ONLY opens new entries:
  Risk gates (paused / bad hour / max trades / safe mode / can_buy_now)
  Screener fetch
  Per-candidate:
      Cooldown / blacklist / market-exists check
      Funding rate + OI snapshot
      LLM analysis (with funding context)
      Direction-aware quality filters:
          LONG : block all-TF overbought, weak multi-TF + non-HIGH conf,
                 historical low winrate, funding too crowded
          SHORT: block all-TF oversold, weak multi-TF + non-HIGH conf,
                 historical low winrate, funding crowded-short (squeeze)
      Spread check (block on wide bid/ask)
      Balance pre-check
      Set leverage + isolated margin (hard fail)
      Place market order with reduce-only off, idempotent clientOrderId
      Provisional state write IMMEDIATELY on fill (zombie-protection)
      Slippage tracking  may trigger SAFE_MODE
      Final state write with real fill price + actual margin
"""
from __future__ import annotations

from core.logger import _date as _utc_now_str

import os

from bot_utils import (
    budget_exhausted,
    get_funding_info,
    funding_oi_filter,
    calc_liquidation_price,
    distance_to_liquidation_pct,
    check_spread_ok,
    record_slippage,
    create_order_with_retry,
    extract_or_estimate_futures_fee,
    filled_margin_usdt,
    get_maintenance_margin_rate,
)


class FuturesScanMixin:

    @staticmethod
    def _bool_cfg_value(value, default: bool = False) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("1", "true", "yes", "on")

    def _scan_loop(self):
        from core.logger import log_event, send_telegram
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        scan_interval = int(self.C("SCAN_INTERVAL"))
        log_event(f"Scan-Loop started (interval: {scan_interval}s)", "INFO")

        cb_failures = 0
        cb_backoff = self.CB_INITIAL_BACKOFF

        while not self._shutdown_event.is_set():
            try:
                self._scan_tick()
                if cb_failures > 0:
                    if cb_failures >= self.CB_FAILURE_THRESHOLD:
                        log_event(" Circuit breaker reset after recovery", "INFO")
                    cb_failures = 0
                    cb_backoff = self.CB_INITIAL_BACKOFF
            except Exception as e:
                log_event(f"Scan tick error: {e}", "WARN")
                self._log_error("Scan loop", e)
                cb_failures += 1
                if cb_failures >= self.CB_FAILURE_THRESHOLD:
                    log_event(
                        f"  Circuit breaker: {cb_failures} consecutive failures  "
                        f"backing off {cb_backoff:.0f}s", "WARN"
                    )
                    if cb_failures == self.CB_FAILURE_THRESHOLD:
                        try:
                            if not self.simulation:
                                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                                    f" [{self.BOT_NAME}] Circuit breaker tripped\n"
                                    f"{cb_failures} consecutive errors. "
                                    f"Check error_log.txt."
                                )
                        except Exception:
                            pass
                    if self._shutdown_event.wait(timeout=cb_backoff):
                        return
                    cb_backoff = min(cb_backoff * 2, self.CB_MAX_BACKOFF)
                    continue

            if self._shutdown_event.wait(timeout=scan_interval):
                return

    #  One scan tick 

    def _scan_tick(self) -> None:
        from core.logger import log_event
        from trading.market_filters import (can_buy_now, get_market_regime)
        from trading.risk_manager import is_bot_paused, is_bad_hour
        from trading.screener import get_top_momentum_coins

        # SAFE_MODE: stop opening new entries
        if self.safe_mode.is_active():
            log_event(
                f"[{self.BOT_NAME}] SAFE_MODE: {self.safe_mode.reason()}  "
                f"new entries disabled",
                "WAIT"
            )
            return

        # API budget guard
        if budget_exhausted():
            log_event(
                "[Scan] API budget exhausted  skipping this cycle",
                "WAIT"
            )
            return

        max_trades = int(self.C("MAX_OPEN_TRADES"))
        min_pump = float(self.C("MIN_PUMP"))

        # Risk gates
        paused, pause_reason = is_bot_paused(
            self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
        if paused:
            log_event(f"[{self.BOT_NAME}] Pausiert: {pause_reason}", "WAIT")
            return
        if self.state.count() >= max_trades:
            log_event(f"Max. Trades erreicht ({max_trades})", "WAIT")
            return
        if is_bad_hour(self.BOT_NAME):
            from trading.risk_manager import _get_local_hour
            hour = _get_local_hour()
            import os as _os
            tz = _os.getenv("BOT_TIMEZONE", "UTC")
            log_event(f"[{self.BOT_NAME}] Bad hour ({hour}:xx {tz})", "WAIT")
            return

        open_syms = self.state.keys()
        # allow_shorts=True: the futures bot can trade SHORT positions, so
        # BEAR market regime and moderate BTC dumps must NOT block the entire
        # scan. The direction-specific quality filters (_quality_filters, RSI
        # gates, funding/OI checks) decide whether to go LONG or SHORT per
        # candidate  that asymmetric logic is already in place. Without
        # allow_shorts=True, a BEAR regime silently blocked ALL futures
        # entries even though BEAR is exactly when shorts should be active.
        allowed, reason = can_buy_now(
            self.ex, bot_name=self.BOT_NAME, open_symbols=open_syms,
            allow_shorts=True,
        )
        if not allowed:
            log_event(f"[{self.BOT_NAME}] Markt-Filter: {reason}", "WAIT")
            return

        regime = get_market_regime(self.ex)
        log_event(
            f"Scanning | Pump {min_pump}% | Phase: {regime['regime']} | "
            f"F&G: {regime['fear_greed']}",
            "SCAN"
        )
        # Detect quiet market for adaptive vol_surge threshold.
        # Same logic as spot bots: NEUTRAL + BTC 24h in 3% range.
        _btc24 = float(regime.get("btc_24h", 0.0))
        _is_quiet = (regime.get("regime") == "NEUTRAL"
                      and -3.0 <= _btc24 <= 3.0)

        try:
            cand = get_top_momentum_coins(
                exchange=self.ex, min_pump=min_pump,
                limit=30,
                bot_name=self.BOT_NAME,
                direction="both",
                quiet_market=_is_quiet,
            )
        except Exception as e:
            log_event(f"Screener error: {e}", "WARN")
            return

        if cand is None or cand.empty:
            return

        # LLM compute budget for THIS scan tick. The screener returns up to 30
        # candidates ranked by momentum; each survivor triggers a blocking
        # local-LLM inference (~3-6s). Evaluating all 30 sequentially can
        # exceed SCAN_INTERVAL, so the loop never finishes and later candidates
        # never get a chance. This is a COMPUTE cap, not a trade filter: the
        # best (highest-ranked) candidates are evaluated first, the loop already
        # breaks at MAX_OPEN_TRADES, so a budget comfortably above that never
        # reduces real fills  it only kills the pathological full-scan stall.
        self._llm_calls_this_tick = 0

        for _, r in cand.iterrows():
            if self._shutdown_event.is_set():
                return
            if self.state.count() >= max_trades:
                break
            if self.safe_mode.is_active():
                return
            # Re-check the daily-loss / kill-switch per candidate (M-3 parity
            # with spot): a concurrent close mid-scan can push realized PnL past
            # the cap; without this the remaining candidates would still fire.
            # No exchange arg  cheap DB read, doesn't repeat the per-cycle
            # BTC-crash API call from the pre-loop gate.
            _paused, _pr = is_bot_paused(
                self.BOT_NAME, simulation=self.simulation)
            if _paused:
                log_event(f"[{self.BOT_NAME}] Kill-switch mid-scan ({_pr})  "
                          f"stopping further entries this cycle", "WAIT")
                break
            try:
                self._try_open_trade(r, regime)
            except Exception as e:
                log_event(f"Open-trade {r['symbol']} failed: {e}", "WARN")
                self._log_error(f"_try_open_trade {r['symbol']}", e)

    #  Cooldown 

    def _is_in_cooldown(self, sym: str) -> bool:
        try:
            from trading.cooldown_utils import check_in_cooldown
            return check_in_cooldown(self.cool, sym)
        except ImportError:
            return False

    #  One candidate 

    def _try_open_trade(self, r, regime: dict) -> None:
        """Analyze one screener candidate and place a LONG/SHORT order
        if all quality + risk filters pass."""
        from core.logger import log_event, log_buy, send_telegram, log_struct
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from trading.risk_manager import (check_blacklist, get_position_size)
        from trading.market_filters import get_btc_change, get_fear_greed
        from config.exchange_config import (must_set_leverage,
                                              LeverageNotSetError,
                                              safe_set_margin_mode,
                                              entry_params)

        sym = r["symbol"].split("/")[0]
        symbol_full = f"{sym}/USDT:USDT"

        # Pre-checks
        if self._is_in_cooldown(sym):
            return
        if check_blacklist(sym, self.BOT_NAME):
            log_event(f"{sym} auf Blacklist", "WAIT")
            return
        # COEXISTENCE: skip a coin another bot on this account already holds
        # (exclusive symbol claims  two futures bots would net on one perp).
        try:
            from core.database import is_claimed_by_other
            if is_claimed_by_other(sym, self.BOT_NAME, is_futures=True):
                log_event(f"{sym} held by another bot  skipping (coexistence)", "WAIT")
                return
        except Exception:
            pass
        if self.state.has(sym):
            return
        if symbol_full not in self.ex.markets:
            return

        # Per-coin tradability gate. Futures calls can_buy_now() without a
        # candidate_symbol, so the STOCK/illiquid/innovation filter inside
        # can_buy_now never fires for futures  check the concrete symbol here,
        # before any analysis.
        try:
            from trading.market_filters import check_tradability
            _ok, _why = check_tradability(self.ex, symbol_full)
            if not _ok:
                log_event(f"{sym}: {_why}", "WAIT")
                return
        except Exception as _te:
            # Fail-closed on the STOCK suffix even if check_tradability errors.
            if sym.upper().endswith("STOCK"):
                log_event(f"{sym}: tokenized stock  skipping (fail-safe)",
                          "WAIT")
                return

        # LLM budget only applies when the strategy is explicitly configured to
        # use LLM analysis. With USE_LLM=false: no news, no model call, no veto.
        use_llm = self._bool_cfg_value(self.C("USE_LLM", False), False)
        if use_llm:
            try:
                _llm_budget = int(os.getenv("FUT_LLM_BUDGET_PER_SCAN", "0"))
            except (ValueError, TypeError):
                _llm_budget = 0
            if (_llm_budget > 0
                    and getattr(self, "_llm_calls_this_tick", 0) >= _llm_budget):
                log_event(
                    f"{sym}: LLM budget ({_llm_budget}/scan) reached - "
                    f"deferring to next scan tick", "INFO")
                return
            self._llm_calls_this_tick = (
                getattr(self, "_llm_calls_this_tick", 0) + 1)

        log_event(
            f"{'Analyzing' if use_llm else 'Signal check'} {sym} (Futures) ...",
            "INFO")
        # Funding + OI
        funding_rate, oi_usdt, oi_change = get_funding_info(self.ex, symbol_full)

        # screener_direction is "LONG" or "SHORT" based on which pipeline
        # found this candidate. Pass it to the LLM as a bias hint so the
        # model doesn't have to infer direction from scratch. When the
        # screener found a SHORT candidate (coin dumping + MACD < 0), the
        # LLM should start with that context instead of defaulting to LONG.
        screener_dir = str(r.get("screener_direction", "LONG")).upper()

        # FUT_LLM_VETO_ONLY only matters when USE_LLM=true. With USE_LLM=false,
        # hard price/screener signals decide and the LLM is never touched.
        veto_only = True
        if use_llm:
            veto_only = os.getenv("FUT_LLM_VETO_ONLY", "1").strip().lower() \
                not in ("0", "false", "no", "off")
        # BTC 24h move for the relative-strength gate (#3), from the regime
        # snapshot already computed this tick - NO extra API call.
        btc_chg = float(regime.get("btc_24h", 0.0))

        ans = None
        news = ""
        llm_dir, llm_conf = "", "LOW"
        analysis_is_keyword = False

        if use_llm:
            # LLM analysis: in veto-only mode a failure must NOT block the trade.
            try:
                news = self._news.get_latest_news(sym)
                try:
                    ans = self._news.analyze_sentiment(
                        sym, r["change_percent"],
                        r["rsi_15m"], r["rsi_1h"], r["rsi_4h"], news,
                        market_regime=regime, price=r["price"],
                        leverage=int(self.C("LEVERAGE")),
                        funding_rate=funding_rate,
                        open_interest_usdt=oi_usdt, oi_change=oi_change,
                        screener_direction=screener_dir,
                    )
                except TypeError:
                    # Older news_brain without the screener_direction kwarg.
                    ans = self._news.analyze_sentiment(
                        sym, r["change_percent"],
                        r["rsi_15m"], r["rsi_1h"], r["rsi_4h"], news,
                        market_regime=regime, price=r["price"],
                        leverage=int(self.C("LEVERAGE")),
                        funding_rate=funding_rate,
                        open_interest_usdt=oi_usdt, oi_change=oi_change,
                    )
            except Exception as e:
                if not veto_only:
                    log_event(f"Analysis {sym} failed: {e}", "WARN")
                    self._log_error(f"Analyze {sym}", e)
                    return
                log_event(f"{sym}: LLM unavailable ({type(e).__name__}) - "
                          f"proceeding on price signal", "INFO")

            # Parse the LLM verdict (best-effort).
            analysis_is_keyword = "[keyword fallback" in str(ans or "").lower()
            try:
                if ans is not None:
                    llm_dir, llm_conf = (
                        self._news.parse_direction_and_confidence(ans))
            except Exception as e:
                if not veto_only:
                    log_event(f"parse_direction {sym} failed: {e}", "WARN")
                    return
                llm_dir, llm_conf = "", "LOW"
        if veto_only:
            # LLM = veto only: it can force a skip (WAIT), not choose direction.
            if use_llm:
                llm_vote = str(llm_dir or "").upper()
                source = "Keyword fallback" if analysis_is_keyword else "LLM veto"
                if llm_vote == "WAIT":
                    log_event(f"{sym}: {source} (WAIT) - skipped", "WAIT")
                    return
                if (llm_vote in ("LONG", "SHORT")
                        and screener_dir in ("LONG", "SHORT")
                        and llm_vote != screener_dir):
                    log_event(
                        f"{sym}: {source} ({llm_vote}) conflicts with "
                        f"screener ({screener_dir}) - skipped", "WAIT")
                    return
            direction = screener_dir
            ok_sig, confidence, sig_why = self._assess_entry_signal(
                r, direction, funding_rate, btc_chg)
            if not ok_sig:
                log_event(f"{sym}: {direction} blocked  {sig_why}", "WAIT")
                return
        else:
            direction, confidence = llm_dir, llm_conf
            if direction == "WAIT":
                source = "Keyword fallback" if analysis_is_keyword else "AI"
                log_event(f"{sym}: {source} says WAIT - skipped", "WAIT")
                return
            # Screener-direction override on conflict (LLM LONG-bias).
            # The screener's direction is grounded in hard price action; the
            # LLM's is not. FUT_DIRECTION_CONFLICT = screener (default) | skip | llm
            if direction != screener_dir and screener_dir in ("LONG", "SHORT"):
                mode = os.getenv("FUT_DIRECTION_CONFLICT", "screener").strip().lower()
                if mode == "llm":
                    log_event(
                        f"{sym}: screener={screener_dir}, LLM={direction} "
                        f"(LLM override  proceeding with {direction})", "INFO")
                elif mode == "skip":
                    log_event(
                        f"{sym}: screener={screener_dir} vs LLM={direction} "
                        f"conflict  skipping (FUT_DIRECTION_CONFLICT=skip)", "WAIT")
                    return
                else:  # 'screener' (default)
                    log_event(
                        f"{sym}: screener={screener_dir} vs LLM={direction} "
                        f"conflict  following screener ({screener_dir}), "
                        f"LLM bias overridden", "INFO")
                    direction = screener_dir
            # Apply the hard-signal gates (#2-#5) in legacy mode too.
            ok_sig, _conf_hs, sig_why = self._assess_entry_signal(
                r, direction, funding_rate, btc_chg)
            if not ok_sig:
                log_event(f"{sym}: {direction} blocked  {sig_why}", "WAIT")
                return

        #  Regime-adaptive direction gate 
        # Trading against the regime needs a much stronger signal. In a clear
        # BEAR, only allow LONG on HIGH confidence (and vice-versa in a clear
        # BULL for SHORT). NEUTRAL lets both through. Disable via
        # FUT_REGIME_DIRECTION_GATE=0.
        try:
            _gate_on = os.getenv("FUT_REGIME_DIRECTION_GATE", "1").strip() \
                not in ("0", "false", "no", "off")
        except Exception:
            _gate_on = True
        if _gate_on:
            _reg = regime["regime"]
            _btc7 = float(regime.get("btc_7d", 0.0))
            _conf = str(confidence).upper()
            try:
                _bear_7d = float(os.getenv("FUT_REGIME_BEAR_7D", "-5.0"))
                _bull_7d = float(os.getenv("FUT_REGIME_BULL_7D", "5.0"))
            except ValueError:
                _bear_7d, _bull_7d = -5.0, 5.0
            # A "clear" trend = explicit regime OR 7d move past the threshold.
            # The milder -3%..-5% zone is handled by the existing margin-
            # damping below (dampen, don't block) so the bot still trades in
            # light headwind but won't fight a clear downtrend.
            clear_bear = (_reg == "BEAR") or (_btc7 <= _bear_7d)
            clear_bull = (_reg == "BULL") and (_btc7 >= _bull_7d)
            if clear_bear and direction == "LONG" and _conf != "HIGH":
                log_event(
                    f"{sym}: LONG blocked  BEAR regime (BTC 7d {_btc7:+.1f}%), "
                    f"counter-trend needs HIGH conf (got {_conf})", "WAIT")
                return
            if clear_bull and direction == "SHORT" and _conf != "HIGH":
                log_event(
                    f"{sym}: SHORT blocked  BULL regime (BTC 7d {_btc7:+.1f}%), "
                    f"counter-trend needs HIGH conf (got {_conf})", "WAIT")
                return

        # Quality filters
        allow, why = self._quality_filters(sym, r, direction, confidence,
                                             funding_rate, oi_change)
        if not allow:
            log_event(f"{sym}: {direction} blocked  {why}", "WAIT")
            return

        # Spread check (live only)
        if not self.simulation:
            try:
                entry_ticker = self.ticker_cache.get(self.ex, symbol_full, timeout=4.0)
                if not entry_ticker.get("bid") or not entry_ticker.get("ask"):
                    ob = self.ex.fetch_order_book(symbol_full, limit=5)
                    bids = (ob or {}).get("bids") or []
                    asks = (ob or {}).get("asks") or []
                    entry_ticker = dict(entry_ticker)
                    if bids:
                        entry_ticker["bid"] = bids[0][0]
                    if asks:
                        entry_ticker["ask"] = asks[0][0]
                if not check_spread_ok(entry_ticker, log_event=log_event,
                                       symbol=sym, missing_ok=False):
                    log_event(f"{sym}: {direction} blocked  spread too wide", "WAIT")
                    return
            except Exception as e:
                log_event(f"{sym}: {direction} blocked  spread check unavailable "
                          f"({type(e).__name__})", "WAIT")
                return

        #  Bull/Bear devil's-advocate veto (2nd LLM call, ~3-4s) 
        # Skipped in veto-only mode by default: direction is already price-based
        # and the hard-signal filters (#2-#5) gate quality, so a second LLM veto
        # just doubles inference time for little gain. Force it with
        # FUT_BULL_BEAR_IN_VETO=1; in LLM-led mode it always runs.
        _do_challenge = (not veto_only) or os.getenv(
            "FUT_BULL_BEAR_IN_VETO", "0").strip().lower() in ("1", "true", "yes", "on")
        if _do_challenge and ans is not None:
            try:
                challenge = getattr(self._news, "futures_bull_bear_challenge", None)
                if callable(challenge):
                    verdict = challenge(sym, direction, ans,
                                         context_brief=(news or "")[:300])
                    if verdict == "OVERRIDE_WAIT":
                        log_event(
                            f"{sym}: Bull/Bear-Veto  {direction} verworfen "
                            f"(Risiken berwiegen)", "WAIT"
                        )
                        return
            except Exception as e:
                # Veto ist best-effort; ein Fehler darf den Trade nicht hart
                # blockieren (auer fail-closed greift bereits in der Fn).
                log_event(f"{sym}: Bull/Bear-Veto uebersprungen "
                          f"({type(e).__name__})", "INFO")

        # Position sizing
        margin_usdt = get_position_size(self.BOT_NAME)
        if regime["regime"] == "BEAR" and direction == "LONG":
            margin_usdt = max(5.0, margin_usdt * 0.5)
            log_event(f"BEAR phase + LONG: margin halved to {margin_usdt} USDT", "INFO")
        elif regime["regime"] == "NEUTRAL":
            margin_usdt = max(5.0, margin_usdt * 0.75)
            log_event(f"NEUTRAL phase: margin reduced to {margin_usdt} USDT", "INFO")

        # Additional BTC-7d headwind check (independent of regime).
        # Even if the regime is NEUTRAL, a sustained negative BTC trend
        # (~-5% over 7 days) is a real headwind for LONGs. Adds a soft margin
        # damping for LONGs in negative-trend NEUTRAL markets without blocking
        # them entirely.
        btc_7d = regime.get("btc_7d", 0.0)
        btc_24h = regime.get("btc_24h", 0.0)
        if direction == "LONG" and btc_7d < -3.0 and regime["regime"] == "NEUTRAL":
            old_margin = margin_usdt
            margin_usdt = max(5.0, margin_usdt * 0.75)
            log_event(
                f"NEUTRAL + BTC 7d {btc_7d:+.1f}% headwind: LONG margin "
                f"reduced {old_margin:.1f} -> {margin_usdt:.1f} USDT", "INFO"
            )

        # SHORT in NEUTRAL with sideways BTC is risky: when BTC is flat
        # (~1.5% in 24h), shorts are often catching falling knives that bounce
        # immediately. Reduce margin similarly.
        if (direction == "SHORT" and regime["regime"] == "NEUTRAL"
                and -1.5 <= btc_24h <= 1.5):
            old_margin = margin_usdt
            margin_usdt = max(5.0, margin_usdt * 0.75)
            log_event(
                f"NEUTRAL + flat BTC ({btc_24h:+.1f}%): SHORT margin "
                f"reduced {old_margin:.1f} -> {margin_usdt:.1f} USDT", "INFO"
            )

        # Balance pre-check (live only)  avoid InsufficientBalance retry spam
        if not self.simulation:
            try:
                from bot_utils.balance import safe_fetch_balance_usdt
                available = safe_fetch_balance_usdt(self.ex,
                                                     error_logger=self._log_error)
                if available is not None and available > 0:
                    required = margin_usdt * 1.05
                    if required > available:
                        log_event(
                            f"{sym}: skipping {direction}  need {required:.2f} USDT "
                            f"but only {available:.2f} USDT free", "WARN"
                        )
                        return
            except Exception:
                pass  # best-effort

        #  Order placement 
        leverage = int(self.C("LEVERAGE"))
        # Validate price BEFORE order  illiquid coins / cache misses sometimes
        # return price=0, which would otherwise create an orphan position.
        try:
            entry_price = float(r.get("price") or 0)
        except (TypeError, ValueError):
            entry_price = 0
        if entry_price <= 0:
            log_event(
                f"{sym}: {direction} skipped  invalid screener price "
                f"{r.get('price')!r}",
                "WARN"
            )
            return
        # Real maintenance-margin TIER (not the flat 0.01 default) for BOTH SIM
        # and LIVE (M-1) so the liq price isn't optimistic on high-MM small-caps.
        # The tier is market metadata  available without a live position  so
        # SIM gets the same liq accuracy the exits path already uses.
        try:
            mm_rate = get_maintenance_margin_rate(self.ex, symbol_full)
        except Exception:
            mm_rate = 0.01
        liq_price = calc_liquidation_price(entry_price, leverage, direction, mm_rate)

        amount = 0.0
        fill_price = entry_price
        fees_paid = 0.0

        if self.simulation:
            try:
                notional = float(margin_usdt) * float(leverage)
                contract_size = self._get_contract_size(symbol_full)
                coins = (notional / entry_price) if entry_price > 0 else 0.0
                amount = coins / max(contract_size, 1e-9)
                from bot_utils.fee_math import taker_fee_rate
                fees_paid = notional * taker_fee_rate(
                    self.ex, symbol_full, 0.0006)
            except (TypeError, ValueError, ZeroDivisionError):
                amount = 0.0
                fees_paid = 0.0
            if amount <= 0:
                log_event(f"{sym}: SIM {direction} amount=0  skip", "WARN")
                return
        else:
            # mm_rate + liq_price already computed above with the real tier.
            margin_mode = str(self.C("MARGIN_MODE", "isolated") or "isolated").lower()
            # must_set_leverage raises on failure  ABORT trade
            try:
                must_set_leverage(self.ex, leverage, symbol_full,
                                  direction=direction, margin_mode=margin_mode)
            except LeverageNotSetError as lev_e:
                log_event(
                    f"set_leverage failed for {sym} at {leverage}x  "
                    f"ABORTING trade ({lev_e})", "WARN"
                )
                log_struct("futures_open_aborted",
                            symbol=sym, leverage=leverage,
                            reason="set_leverage_failed")
                return
            safe_set_margin_mode(self.ex, margin_mode, symbol_full,
                                 leverage=leverage, direction=direction)

            try:
                notional = margin_usdt * leverage
                amount_coins = notional / entry_price
                # MEXC swap contracts have a contractSize (e.g. MEME=100). The
                # order `amount` must be in CONTRACTS, not raw coins:
                #   contracts = coins / contractSize
                contract_size = self._get_contract_size(symbol_full)
                amount_contracts = amount_coins / contract_size

                # MIN-AMOUNT GATE: some markets (e.g. KAS) require a minimum
                # order amount / integer step. With halved BEAR-LONG margin the
                # computed contracts can fall below that floor; the exchange
                # rejects with "must be greater than minimum amount precision".
                # Skip cleanly instead of failing the order 3x in a row.
                try:
                    _mkt = (getattr(self.ex, "markets", {}) or {}).get(symbol_full, {})
                    _min_amt = (((_mkt.get("limits") or {}).get("amount") or {}).get("min"))
                    if _min_amt and amount_contracts < float(_min_amt):
                        log_event(
                            f"{sym}: order amount {amount_contracts:g} < exchange "
                            f"min {_min_amt:g} (margin too small for this "
                            f"contract) - skipping", "INFO")
                        return
                except Exception:
                    pass

                try:
                    from config.exchange_config import safe_amount_to_precision
                    amount_contracts = float(safe_amount_to_precision(
                        self.ex, symbol_full, amount_contracts))
                except Exception:
                    pass
                if amount_contracts <= 0:
                    log_event(f"Order {sym}: amount = 0 - skip", "WARN")
                    return

                # SAFETY GATE: never let the real cost exceed the intended
                # notional by more than a small tolerance. Catches any
                # contract_size / sizing regression BEFORE it hits the exchange.
                est_cost = amount_contracts * contract_size * entry_price
                if est_cost > notional * 1.5:
                    log_event(
                        f"{sym}: SIZING ABORT - est cost {est_cost:.2f} >> "
                        f"intended notional {notional:.2f} "
                        f"(contract_size={contract_size}); refusing order",
                        "ERROR")
                    log_struct("futures_open_aborted", symbol=sym,
                               reason="sizing_safety_gate",
                               est_cost=est_cost, notional=notional)
                    return

                side = "buy" if direction == "LONG" else "sell"
                # MEXC requires `leverage` in params for isolated-margin swap
                # orders (else: "createSwapOrder() requires a leverage
                # parameter"). Include it alongside marginMode/positionSide.
                try:
                    _lev_int = int(leverage)
                except (ValueError, TypeError):
                    _lev_int = leverage
                # Stable per-intent clientOrderId so create_order_with_retry's
                # transient-error retry can't open a SECOND position on a
                # lost-response timeout. The 30s bucket makes a retry of the
                # SAME intent reuse the SAME id; the BOT_NAME component keeps it
                # unique across bots.
                import hashlib as _hashlib
                import time as _time
                _bucket = int(_time.time() // 30)
                _cid = (f"{self.BUY_PREFIX}-{sym}-"
                        + _hashlib.sha256(
                            f"{self.BOT_NAME}:{sym}:{_bucket}".encode()
                        ).hexdigest()[:10])
                _entry_params = entry_params(
                    position_side="long" if direction == "LONG" else "short",
                    margin_mode=margin_mode, leverage=_lev_int,
                    client_order_id=_cid)
                from core.database import (claim_symbol_for_entry,
                                           remove_open_position)
                if not claim_symbol_for_entry(self.BOT_NAME, sym, direction):
                    log_event(f"{sym} claimed by another bot  skip "
                              f"(coexistence)", "WAIT")
                    return
                order = create_order_with_retry(
                    self.ex, symbol_full, side, amount_contracts,
                    params=_entry_params,
                    shutdown_event=self._shutdown_event,
                    action_label=f"open {sym}",
                    log_event=log_event, log_struct=log_struct,
                )
                # Prefer the ACTUAL filled amount. Bitget often returns
                # filled=0/None on the initial market-order response (the fill
                # settles ~200-500ms later); on a missing filled, briefly
                # reload via fetch_order so Monitor/PnL/Funding use the real
                # size instead of the requested one.
                filled_raw = order.get("filled")
                amount = float(filled_raw) if filled_raw else 0.0
                entry_verified = amount > 0
                if amount <= 0:
                    oid = order.get("id") or order.get("orderId")
                    if oid:
                        import time as _t
                        for _att in range(2):
                            _t.sleep(0.4 * (1 + _att))
                            try:
                                refreshed = self.ex.fetch_order(str(oid), symbol_full)
                                rf = float((refreshed or {}).get("filled") or 0)
                                if rf > 0:
                                    amount = rf
                                    entry_verified = True
                                    # Realen Fill-Preis gleich mitnehmen
                                    for _k in ("average", "price"):
                                        _v = (refreshed or {}).get(_k)
                                        if _v:
                                            try:
                                                _fv = float(_v)
                                                if _fv > 0:
                                                    fill_price = _fv
                                                    break
                                            except (ValueError, TypeError):
                                                pass
                                    break
                            except Exception:
                                continue
                if amount <= 0:
                    positions_verified = False
                    positions_unavailable = False
                    try:
                        from bot_utils import fetch_open_position
                        pos, positions_unavailable = fetch_open_position(
                            self.ex, symbol_full)
                        if pos is not None:
                            real_amt = abs(float(pos.get("contracts")
                                                 or pos.get("size") or 0.0))
                            if real_amt > 0:
                                amount = real_amt
                                positions_verified = True
                                for _k in ("entryPrice", "entry_price"):
                                    _v = pos.get(_k)
                                    if _v:
                                        try:
                                            _fv = float(_v)
                                            if _fv > 0:
                                                fill_price = _fv
                                                break
                                        except (ValueError, TypeError):
                                            pass
                    except Exception:
                        positions_unavailable = True
                    if amount <= 0 and not positions_unavailable:
                        log_event(
                            f"{sym}: order returned no fill and no exchange "
                            f"position was found  aborting state write",
                            "WARN")
                        remove_open_position(self.BOT_NAME, sym)
                        return
                    if amount > 0 and positions_verified:
                        entry_verified = True
                        log_event(f"{sym}: entry amount verified from exchange "
                                  f"position ({amount:g} contracts)", "INFO")
                if amount <= 0:
                    # Nothing could be reloaded. If the positions API is
                    # unavailable, track the requested amount provisionally so a
                    # possible live position is not left unmanaged.
                    amount = float(amount_contracts)
                    entry_verified = False

                #  POST-OPEN VERIFICATION 
                # Check against REALITY: after the fill, compute the true
                # notional from the actual filled amount and contractSize, and
                # compare to what we intended (margin * leverage). If the real
                # position is wildly larger (contract_size bug), the order was
                # mis-sized: emergency-close it immediately and abort. Measures
                # the real order rather than trusting the formula.
                intended_notional = float(margin_usdt) * float(leverage)
                try:
                    real_notional = float(amount) * float(contract_size) * float(fill_price)
                except (ValueError, TypeError):
                    real_notional = 0.0
                #  Oversize detection  RATIO-FIRST, never on healthy size 
                # A position whose REAL notional matches what we INTENDED
                # (ratio ~1.0x) is correctly sized by definition and is never
                # emergency-closed, no matter how large in absolute USDT.
                #
                # Only two things are genuine sizing problems:
                #   1. real >> intended (ratio > 2): the contract_size-bug
                #  signature  the real order is a MULTIPLE of what we sized
                #      for. Reliable detector; cannot trip at ratio 1.0.
                #   2. intended itself is absurd (corrupted margin/leverage).
                #      Bound it by the most the bot could ever legitimately
                #      deploy = POSITION_SIZE_MAX * leverage, with 3x headroom
                #      for config drift / Kelly growth. Checks INTENDED (not
                #      real), so a correctly-sized position can never trip it.
                #      Env FUT_MAX_NOTIONAL_USDT overrides the derived ceiling.
                ratio = real_notional / max(intended_notional, 1e-9)
                try:
                    _max_margin = float(self.C("POSITION_SIZE_MAX", 50.0))
                except (ValueError, TypeError):
                    _max_margin = 50.0
                _derived_ceiling = max(1.0, _max_margin) * float(leverage) * 3.0
                _env_cap = os.getenv("FUT_MAX_NOTIONAL_USDT")
                try:
                    ceiling = float(_env_cap) if _env_cap else _derived_ceiling
                except (ValueError, TypeError):
                    ceiling = _derived_ceiling
                oversized = (real_notional > 0
                             and (ratio > 2.0
                                  or intended_notional > ceiling))
                if oversized:
                    log_event(
                        f" {sym}: POSITION OVERSIZED  real notional "
                        f"{real_notional:.0f} USDT vs intended "
                        f"{intended_notional:.0f} USDT "
                        f"({real_notional/intended_notional:.1f}x, "
                        f"contract_size={contract_size}). EMERGENCY CLOSING.",
                        "ERROR")
                    log_struct("futures_oversized_emergency_close",
                               symbol=sym, real_notional=real_notional,
                               intended_notional=intended_notional,
                               contract_size=contract_size,
                               ratio=real_notional / max(intended_notional, 1))
                    # Emergency reduce-only close of exactly what we hold. Use
                    # the retry wrapper (transient errors) and VERIFY flat  a
                    # partial fill must not leave an untracked oversized orphan.
                    _closed_ok = False
                    try:
                        from bot_utils import verify_position_closed
                        from config.exchange_config import reduce_only_params
                        close_side = "sell" if direction == "LONG" else "buy"
                        # Route through the single source of truth (per-exchange:
                        # OKX tdMode, Binance one-way omits positionSide  -4061)
                        # instead of a hand-built dict, matching every other close.
                        close_params = reduce_only_params(
                            position_side=("long" if direction == "LONG" else "short"),
                            margin_mode=margin_mode,
                            leverage=_lev_int,
                        )
                        create_order_with_retry(
                            self.ex, symbol_full, close_side, amount,
                            params=close_params,
                            shutdown_event=self._shutdown_event,
                            action_label=f"oversize-close {sym}",
                            log_event=log_event, log_struct=log_struct,
                        )
                        closed, _rem = verify_position_closed(self.ex, symbol_full)
                        _closed_ok = bool(closed)
                        if _closed_ok:
                            log_event(f"{sym}: oversized position emergency-closed "
                                      f"(verified flat)", "WARN")
                        else:
                            log_event(f" {sym}: oversize close incomplete "
                                      f"({_rem:.6f} left)  handing residual to "
                                      f"monitor", "ERROR")
                    except Exception as _ce:
                        _exch = getattr(self.ex, "name", None) or "the exchange"
                        log_event(
                            f" {sym}: EMERGENCY CLOSE FAILED ({_ce})  "
                            f"CLOSE MANUALLY ON {_exch} NOW!", "ERROR")
                        log_struct("futures_emergency_close_failed",
                                   symbol=sym, error=str(_ce))

                    if not _closed_ok:
                        # Position (or a residual) is still open on the exchange.
                        # Write provisional state so it stays visible to
                        # self.state and the normal exit machinery keeps reducing
                        # it (reduce-only caps to the real size; reconcile heals
                        # the rest)  otherwise a leveraged position runs
                        # completely unmanaged.
                        try:
                            added_residual = self.state.add(sym, {
                                "position_type": direction,
                                "buy": entry_price,
                                "highest": entry_price,
                                "buy_time": _utc_now_str(),
                                "invested_usdt": margin_usdt,
                                "leverage": leverage,
                                "margin_mode": margin_mode,
                                "liquidation_price": liq_price,
                                "amount": amount,
                                "original_amount": amount,
                                "funding_paid": 0.0,
                                "initial_entry_fee": 0.0,
                                "fees_paid": 0.0,
                                "partial_sold": False,
                                "break_even": False,
                                "be_active": False,
                                "provisional": True,
                            })
                            if added_residual is False:
                                log_event(
                                    f" {sym}: residual state registration "
                                    f"returned False; claim kept, manual "
                                    f"recovery required", "ERROR")
                            else:
                                log_event(
                                    f"{sym}: failed/partial close handed to "
                                    f"monitor for managed exit", "WARN")
                        except Exception as _se:
                            log_event(
                                f" {sym}: could not register orphan for "
                                f"monitoring ({_se})  CLOSE MANUALLY!", "ERROR")
                    # Cooldown so we don't immediately re-open the same trap.
                    try:
                        from trading.cooldown_utils import set_cooldown
                        _cdfile = getattr(self, "COOLDOWN_FILE", None)
                        _lock = getattr(self, "_cooldown_lock", None)
                        if _lock is not None:
                            with _lock:
                                set_cooldown(self.cool, sym, 120, _cdfile)
                        else:
                            set_cooldown(self.cool, sym, 120, _cdfile)
                    except Exception as _cde:
                        log_event(f"{sym}: cooldown set failed ({_cde})", "WARN")
                    if _closed_ok:
                        remove_open_position(self.BOT_NAME, sym)
                    return

                # Orphan detection. If amount is still 0 after the order, the
                # position IS on the exchange but TradeState will reject the
                # add()  position becomes invisible to the bot. Loud warning +
                # struct log so the user can manually verify and close.
                if amount <= 0:
                    log_event(
                        f" {sym}: ORPHAN RISK  order placed but "
                        f"filled=0 returned. Check Bitget manually!",
                        "WARN"
                    )
                    log_struct("futures_orphan_risk",
                                symbol=sym, direction=direction,
                                requested=amount_contracts,
                                order_id=str(order.get("id", "")))
                    remove_open_position(self.BOT_NAME, sym)
                    return

                #  Zombie protection: write provisional state IMMEDIATELY 
                provisional_ok = self.state.add(sym, {
                    "position_type": direction,
                    "buy": entry_price,
                    "highest": entry_price,
                    "buy_time": _utc_now_str(),
                    "invested_usdt": margin_usdt,
                    "leverage": leverage,
                    "margin_mode": margin_mode,
                    "liquidation_price": liq_price,
                    "initial_liq_distance": distance_to_liquidation_pct(
                        entry_price, liq_price, direction),
                    "amount": amount,
                    "original_amount": amount,
                    "funding_paid": 0.0,
                    "initial_entry_fee": 0.0,
                    "fees_paid": 0.0,
                    "partial_sold": False,
                    "break_even": False,
                    "be_active": False,
                    "provisional": True,
                })
                if provisional_ok is False:
                    log_event(
                        f"{sym}: provisional state-write returned False; "
                        f"final state write must recover before claim release",
                        "WARN")

                # Real fill price for accurate PnL
                for k in ("average", "price"):
                    v = order.get(k)
                    if v:
                        try:
                            fv = float(v)
                            if fv > 0:
                                fill_price = fv
                                break
                        except (ValueError, TypeError):
                            continue

                try:
                    # Refetch+estimate variant so the entry fee is recorded
                    # even when Bitget returns fee=0 on the initial market-order
                    # response (very common).
                    fees_paid = extract_or_estimate_futures_fee(
                        self.ex, order, symbol_full, fill_price,
                        amount=amount, contract_size=contract_size,
                    )
                except Exception as _fee_e:
                    # A transient fetch hiccup here must NOT bubble to the outer
                    # handler, which would mistake this filled order for a failed
                    # one and skip the final trade record. Record 0 and continue.
                    fees_paid = 0.0
                    log_event(
                        f"{sym}: entry fee extraction failed "
                        f"({type(_fee_e).__name__})  recorded 0", "INFO")

                # Slippage tracking + circuit breaker
                record_slippage(
                    entry_price, fill_price,
                    symbol=sym,
                    side="sell" if direction == "SHORT" else "buy",
                    trigger_safe_mode=self.safe_mode.trigger,
                    log_event=log_event,
                )

                # Recompute liq price with real fill
                liq_price = calc_liquidation_price(fill_price, leverage,
                                                     direction, mm_rate)
            except Exception as e:
                log_event(f"Order {sym} failed: {e}", "WARN")
                self._log_error(f"Open {sym}", e)
                _landed = False
                # ORPHAN PREVENTION: create_order can RAISE after the order
                # actually LANDED (lost response on the final retry). Check by
                # clientOrderId  if it filled, TRACK it (provisional) instead of
                # leaving an untracked orphan. Reconcile-adoption is the backstop;
                # this closes the window at the source.
                try:
                    from bot_utils.futures_order import _find_order_by_client_id
                    landed = _find_order_by_client_id(self.ex, symbol_full, _cid)
                    if landed is not None and float(landed.get("filled") or 0) > 0:
                        _amt = float(landed.get("filled") or 0) or float(amount_contracts)
                        added = self.state.add(sym, {
                            "position_type": direction, "buy": entry_price,
                            "highest": entry_price, "buy_time": _utc_now_str(),
                            "invested_usdt": margin_usdt, "leverage": leverage,
                            "margin_mode": margin_mode,
                            "liquidation_price": liq_price,
                            "initial_liq_distance": distance_to_liquidation_pct(
                                entry_price, liq_price, direction),
                            "amount": _amt, "original_amount": _amt,
                            "funding_paid": 0.0, "initial_entry_fee": 0.0,
                            "fees_paid": 0.0, "partial_sold": False,
                            "break_even": False, "be_active": False,
                            "provisional": True,
                        })
                        if added is False:
                            try:
                                from config.exchange_config import reduce_only_params
                                params = reduce_only_params(
                                    ex_name=getattr(self.ex, "id", None),
                                    position_side=("long" if direction == "LONG" else "short"),
                                    margin_mode=margin_mode,
                                    leverage=int(leverage),
                                    hedge_mode=bool(self.C("HEDGE_MODE", False)),
                                )
                                create_order_with_retry(
                                    self.ex, symbol_full,
                                    "sell" if direction == "LONG" else "buy",
                                    _amt, params, self._shutdown_event,
                                    action_label=f"rollback close {sym}",
                                    log_event=log_event,
                                )
                                from bot_utils import verify_position_closed
                                closed, remaining = verify_position_closed(
                                    self.ex, symbol_full)
                                if closed:
                                    remove_open_position(self.BOT_NAME, sym)
                                    log_event(
                                        f" {sym}: landed order rolled back "
                                        f"after state-write failure", "WARN")
                                else:
                                    _landed = True
                                    log_event(
                                        f" {sym}: rollback close not verified "
                                        f"({remaining:.6f} left); claim kept",
                                        "ERROR")
                            except Exception as rb_exc:
                                _landed = True
                                log_event(
                                    f" {sym}: CRITICAL untracked live futures "
                                    f"position risk; rollback failed ({rb_exc})",
                                    "WARN")
                                self._log_error(
                                    f"futures landed rollback after state failure {sym}",
                                    rb_exc)
                        else:
                            _landed = True
                            log_event(f" {sym}: order landed despite error  "
                                      f"tracked provisionally", "WARN")
                except Exception:
                    pass
                if not _landed:
                    remove_open_position(self.BOT_NAME, sym)
                return

        #  Persist full trade record (overwrites provisional) 
        log_buy(
            self.BOT_NAME, f"{sym} [{direction}@{leverage}x]",
            fill_price, margin_usdt,
            (r["rsi_15m"], r["rsi_1h"], r["rsi_4h"]),
            news, ans
        )

        # Actual margin from the filled position. A partial entry fill must not
        # keep the intended margin, otherwise open PnL and risk gates scale too
        # high. ContractSize matters for MEXC-style swap contracts.
        _csize = self._get_contract_size(symbol_full)
        raw_margin, margin_from_fill = filled_margin_usdt(
            amount, _csize, fill_price, leverage, margin_usdt)
        actual_margin = round(float(raw_margin), 6)
        try:
            if margin_from_fill and margin_usdt and margin_usdt > 0:
                fill_ratio = actual_margin / max(float(margin_usdt), 1e-9)
                if fill_ratio < 0.90 or fill_ratio > 1.10:
                    log_event(
                        f"{sym}: entry margin adjusted from fill "
                        f"{margin_usdt:.2f} -> {actual_margin:.2f} USDT "
                        f"(filled={amount:g}, contract_size={_csize})",
                        "INFO")
        except Exception:
            pass

        trade_data = {
            "position_type": direction,
            "buy": fill_price,
            "highest": fill_price,
            "buy_time": _utc_now_str(),
            "invested_usdt": actual_margin,
            "leverage": leverage,
            "liquidation_price": liq_price,
            "initial_liq_distance": distance_to_liquidation_pct(
                fill_price, liq_price, direction),
            "amount": amount,
            "original_amount": amount,
            "rsi_15m": r["rsi_15m"],
            "rsi_1h": r["rsi_1h"],
            "rsi_4h": r["rsi_4h"],
            "change_pct": r["change_percent"],
            "btc_trend": get_btc_change(self.ex, hours=1),
            "fear_greed": get_fear_greed(),
            "funding_paid": 0.0,
            "initial_entry_fee": fees_paid,
            "fees_paid": fees_paid,
            "partial_sold": False,
            "break_even": False,
            "be_active": False,
            "provisional": not entry_verified,
        }
        # Merge instead of replace: update_many keeps monitor-set fields that
        # the Monitor-Thread may have written between the provisional add and
        # this point (a plain state.add would wipe them).
        if self.state.has(sym):
            state_ok = self.state.update_many(sym, trade_data)
        else:
            # First-write path (SIM mode never wrote provisional)
            state_ok = self.state.add(sym, trade_data)
        if state_ok is False and not self.simulation:
            log_event(
                f"{sym}: state write failed after LIVE entry  attempting "
                f"immediate reduce-only rollback", "WARN")
            try:
                from config.exchange_config import reduce_only_params
                params = reduce_only_params(
                    ex_name=getattr(self.ex, "id", None),
                    position_side=("long" if direction == "LONG" else "short"),
                    margin_mode=margin_mode,
                    leverage=int(leverage),
                    hedge_mode=bool(self.C("HEDGE_MODE", False)),
                )
                create_order_with_retry(
                    self.ex, symbol_full,
                    "sell" if direction == "LONG" else "buy",
                    amount, params, self._shutdown_event,
                    action_label=f"rollback close {sym}",
                    log_event=log_event,
                )
                from bot_utils import verify_position_closed
                closed, remaining = verify_position_closed(self.ex, symbol_full)
                if closed:
                    remove_open_position(self.BOT_NAME, sym)
                    log_event(
                        f"{sym}: rollback close verified after state failure",
                        "WARN")
                else:
                    log_event(
                        f"{sym}: CRITICAL rollback close not verified "
                        f"({remaining:.6f} left); claim kept", "ERROR")
            except Exception as rb_exc:
                log_event(
                    f"{sym}: CRITICAL untracked live futures position risk "
                    f"after state failure; rollback failed ({rb_exc})", "WARN")
                self._log_error(f"futures rollback after state failure {sym}", rb_exc)
            return

        try:
            from core.logger import log_struct
            from news.news_brain_core import parse_rationale
            event_name = (
                "keyword_decision" if analysis_is_keyword
                else ("llm_decision" if use_llm else "signal_decision")
            )
            log_struct(
                event_name,
                bot=self.BOT_NAME, symbol=sym, action=direction,
                direction=direction, confidence=confidence,
                screener_direction=screener_dir,
                llm_direction=llm_dir, llm_confidence=llm_conf,
                source=("keyword" if analysis_is_keyword
                        else ("signal" if veto_only else "llm")),
                fallback=bool(analysis_is_keyword or (veto_only and ans is None)),
                rationale=parse_rationale(ans) if ans is not None else "",
                leverage=int(leverage), entry=float(fill_price),
                margin_usdt=float(actual_margin),
                intended_margin_usdt=float(margin_usdt),
                sim=bool(self.simulation),
            )
        except Exception:
            pass

        try:
            if not self.simulation:
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f"[{self.BOT_NAME}] {direction} {sym} @ {leverage}x\n"
                    f"Entry: {fill_price:.6f} USDT\n"
                    f"Margin: {actual_margin:.2f} USDT "
                    f"(Notional: {actual_margin*leverage:.2f})\n"
                    f"Liq: {liq_price:.6f} (Buffer: {self.C('LIQ_SAFETY_PCT')}%)\n"
                    f"BE-Trigger: +{self.C('BREAKEVEN_TRIGGER')}% | "
                    f"RSI: 15m {r['rsi_15m']:.1f}|1h {r['rsi_1h']:.1f}|4h {r['rsi_4h']:.1f}"
                )
        except Exception as e:
            log_event(f"Telegram failed: {e}", "WARN")

    #  Quality filters (direction-aware) 

    def _quality_filters(self, sym, r, direction, confidence,
                          funding_pct, oi_change_pct):
        """Returns (allow, reason).

        Asymmetric for LONG vs SHORT: different RSI overbought/oversold,
        different funding/OI rules. See bot_utils.futures_math.funding_oi_filter
        for the funding/OI gates.
        """
        # 1. Confidence
        if confidence == "LOW":
            return False, "LOW confidence"

        rsi_15, rsi_h, rsi_4 = r["rsi_15m"], r["rsi_1h"], r["rsi_4h"]

        # 2. RSI direction-specific block
        if direction == "LONG":
            if rsi_15 > 80 and rsi_h > 75 and rsi_4 > 70:
                return False, (f"RSI overbought on all TFs "
                                f"(15m={rsi_15:.0f}, 1h={rsi_h:.0f}, 4h={rsi_4:.0f})")
            tf_score = sum(1 for v in (rsi_15, rsi_h, rsi_4) if 50 <= v <= 75)
        else:  # SHORT
            if rsi_15 < 20 and rsi_h < 25 and rsi_4 < 30:
                return False, "RSI oversold on all TFs"
            tf_score = sum(1 for v in (rsi_15, rsi_h, rsi_4) if 25 <= v <= 50)

        # 3. Weak multi-TF + non-HIGH conf
        if tf_score < 2 and confidence != "HIGH":
            return False, (f"weak multi-TF ({tf_score}/3) + {confidence} confidence")

        # 4. Funding / OI filter
        ok, reason = funding_oi_filter(direction, funding_pct, oi_change_pct, confidence)
        if not ok:
            return False, reason

        # 5. Historical winrate (direction-filtered)
        try:
            from core.database import get_historical_winrate_for_setup
            rsi_bucket = (0 if rsi_h < 40 else 1 if rsi_h < 55
                          else 2 if rsi_h < 70 else 3 if rsi_h < 85 else 4)
            chg_abs = abs(r["change_percent"])
            chg_bucket = (0 if chg_abs < 3 else 1 if chg_abs < 8
                          else 2 if chg_abs < 15 else 3 if chg_abs < 25 else 4)
            hist = get_historical_winrate_for_setup(
                self.BOT_NAME, rsi_bucket, chg_bucket, min_sample=8,
                direction=direction,   # NEW: LONG vs SHORT history
            )
            if (hist["winrate"] is not None
                    and hist["winrate"] < 0.30
                    and confidence != "HIGH"):
                return False, (f"historical winrate {hist['winrate']*100:.0f}% "
                                f"over {hist['trade_count']} trades")
        except Exception as e:
            self._log_error(f"historical winrate lookup {sym}", e)

        return True, ""

    def _assess_entry_signal(self, r, direction, funding_rate, btc_chg):
        """Hard-signal entry assessment (#2-#5)  price-grounded, no LLM, no
        extra API calls (uses screener-computed ema_ratio / vol_surge and the
        regime's BTC move). Returns (proceed, confidence, reason).

        Each sub-check is individually toggleable via env and defaults are
        LENIENT: they mostly raise/lower a confidence score and only HARD-block
        on clear extremes, so normal trade flow is preserved. The resulting
        confidence ("HIGH"/"MEDIUM"/"LOW") feeds the regime gate + quality
        filters exactly like the LLM's used to.

        Toggles:  FUT_EXT_FILTER (#2)  FUT_VOL_FILTER (#5) 
                  FUT_RS_FILTER (#3)  FUT_FUNDING_EDGE (#4)
        """
        def _on(flag):
            return os.getenv(flag, "1").strip().lower() not in ("0","false","no","off")
        def _f(flag, default):
            try:
                return float(os.getenv(flag, str(default)))
            except (ValueError, TypeError):
                return default

        try:
            rsi_15 = float(r.get("rsi_15m", 50))
            rsi_h = float(r.get("rsi_1h", 50))
            rsi_4  = float(r.get("rsi_4h", 50))
        except (TypeError, ValueError):
            rsi_15 = rsi_h = rsi_4 = 50.0
        ema_ratio = float(r.get("ema_ratio", 0.0) or 0.0)
        vol_surge = float(r.get("vol_surge", 1.0) or 1.0)
        chg       = float(r.get("change_percent", 0.0) or 0.0)
        is_long   = direction == "LONG"
        score     = 0

        # Base: multi-TF RSI continuation alignment (0..3).
        if is_long:
            score += sum(1 for v in (rsi_15, rsi_h, rsi_4) if 50 <= v <= 75)
        else:
            score += sum(1 for v in (rsi_15, rsi_h, rsi_4) if 25 <= v <= 50)

        # #2 Extension/pullback via distance from EMA50. Far above EMA on a LONG
        # = chasing a vertical move (bad entry  forces a wide stop). Block only
        # the parabolic extreme; reward entries NEAR the EMA (continuation).
        if _on("FUT_EXT_FILTER"):
            max_ext = _f("FUT_MAX_EXT_PCT", 18.0)
            ext = ema_ratio if is_long else -ema_ratio
            if ext > max_ext:
                return False, "LOW", f"over-extended {ext:+.1f}% vs EMA50 (chasing)"
            if 0.0 <= ext <= max_ext * 0.45:
                score += 1

        # #5 Volume surge on the impulse. The screener already requires a base
        # surge, so this only ADDS confidence for strong flow + blocks the rare
        # sub-threshold leftover.
        if _on("FUT_VOL_FILTER"):
            min_vs = _f("FUT_MIN_VOL_SURGE", 1.0)
            if vol_surge < min_vs:
                return False, "LOW", f"weak volume (surge {vol_surge:.2f} < {min_vs:g})"
            if vol_surge >= 1.8:
                score += 1

        # #3 Relative strength vs BTC (same 24h window). LONG should outperform
        # BTC, SHORT underperform. Lenient floor  only blocks clear counter-RS.
        if _on("FUT_RS_FILTER"):
            min_rs = _f("FUT_MIN_RS_PCT", -2.0)
            rs = (chg - btc_chg) if is_long else (btc_chg - chg)
            if rs < min_rs:
                return False, "LOW", f"weak rel-strength vs BTC ({rs:+.1f}%)"
            if rs >= 3.0:
                score += 1

        # #4 Funding as an edge (soft tilt only; funding_oi_filter still hard-
        # blocks crowded extremes). You'd rather be PAID funding than pay it:
        # LONG favoured when funding <= 0, SHORT when funding >= 0. Deadband so
        # negligible funding tilts nothing.
        if _on("FUT_FUNDING_EDGE"):
            f = float(funding_rate or 0.0)
            if abs(f) >= 0.0001:
                paying = (f > 0) if is_long else (f < 0)   # your side PAYS funding
                score += -1 if paying else 1

        # Map score  confidence. Tuned LENIENT (a normal pumping candidate with
        # aligned RSI lands MEDIUM+), so only genuinely weak setups go LOW.
        if score >= 4:
            conf = "HIGH"
        elif score >= 2:
            conf = "MEDIUM"
        else:
            conf = "LOW"
        return True, conf, ""
