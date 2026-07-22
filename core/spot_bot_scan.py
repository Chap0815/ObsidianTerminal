"""
core/spot_bot_scan.py  Scan-thread + new-entry logic for SpotBot.

ScanMixin runs at SCAN_INTERVAL (~150s) and handles ONLY new entries.
Exits are handled by ExitsMixin's monitor thread (every ~20s).

Flow per tick:
  1. Risk gates (paused / bad-hour / max-trades / can_buy_now)
  2. Screener fetch
  3. Per-candidate:
     cooldown / blacklist / correlation check
     LLM analysis via self._news.analyze_sentiment
     Quality filters (confidence, RSI, historical winrate, quality score)
     Position-size scaling (regime + quality)
     Balance pre-check
     Place market buy with clientOrderId
     Record fill price, deduct base-currency fee from recorded amount
     Persist position
"""
from __future__ import annotations

import math

from bot_utils import (
    safe_fetch_balance_usdt,
    extract_fill_price,
    budget_exhausted,
)
from bot_utils.api_budget import try_consume_api_call
from bot_utils.network_retry import RetryForbiddenError


class _SpotBuyBudgetUnavailable(RuntimeError):
    """A SPOT buy was not sent because its atomic budget gate failed."""


class SpotBuyOutcomeUnknown(RetryForbiddenError):
    """A SPOT buy may exist, so its claim must remain reserved."""

    def __init__(self, client_order_id: str):
        self.client_order_id = client_order_id
        super().__init__(
            "spot buy outcome unknown "
            f"(intentClientOrderId={client_order_id})"
        )


def _create_market_buy_budgeted(ex, symbol_pair: str, amount: float,
                                *, params=None):
    try:
        allowed = try_consume_api_call("spot_entry_create_market_buy")
    except Exception as exc:
        raise _SpotBuyBudgetUnavailable(
            "API budget gate unavailable before SPOT market buy"
        ) from exc
    if not allowed:
        raise _SpotBuyBudgetUnavailable(
            "API budget exhausted before SPOT market buy"
        )
    if params is None:
        return ex.create_market_buy_order(symbol_pair, amount)
    return ex.create_market_buy_order(symbol_pair, amount, params=params)


def _client_order_id_parameter_rejected(exc: Exception) -> bool:
    """True only for explicit evidence that the parameter is unsupported."""
    try:
        text = " ".join(str(exc).lower().split())
    except Exception:
        return False
    if "clientorderid" not in text:
        return False
    if type(exc).__name__.lower() == "notsupported":
        return True
    return any(
        phrase in text
        for phrase in (
            "clientorderid unsupported",
            "clientorderid is unsupported",
            "clientorderid not supported",
            "clientorderid is not supported",
            "parameter clientorderid is unsupported",
            "parameter clientorderid not supported",
            "does not support clientorderid",
            "unsupported parameter clientorderid",
            "unknown parameter clientorderid",
            "unrecognized parameter clientorderid",
        )
    )


class ScanMixin:
    """Scan thread + new-entry decision logic."""
    @staticmethod
    def _bool_cfg_value(value, default: bool = False) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("1", "true", "yes", "on")

    @staticmethod
    def _positive_float(value, default: float = 0.0) -> float:
        if isinstance(value, bool):
            return float(default)
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return float(default)
        return parsed if math.isfinite(parsed) and parsed > 0 else float(default)

    @staticmethod
    def _finite_float(value) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) else None

    def _rsi_triplet(self, row) -> tuple[float, float, float] | None:
        values = tuple(
            self._finite_float(row.get(key))
            for key in ("rsi_15m", "rsi_1h", "rsi_4h")
        )
        if any(value is None for value in values):
            return None
        return values

    def _entry_quality_min_score(self) -> float:
        try:
            raw = float(self.C("ENTRY_QUALITY_MIN_SCORE", 75.0))
        except (TypeError, ValueError, OverflowError):
            raw = 75.0
        return max(0.0, min(100.0, raw)) if math.isfinite(raw) else 75.0

    def _entry_quality_filter_enabled(self) -> bool:
        return self._bool_cfg_value(
            self.C("ENTRY_QUALITY_FILTER_ENABLED", True), True)

    def _remember_spot_setup_history(self, sym: str, hist: dict) -> None:
        try:
            cache = getattr(self, "_spot_entry_quality_history", None)
            if not isinstance(cache, dict):
                cache = {}
                setattr(self, "_spot_entry_quality_history", cache)
            cache[str(sym).upper()] = dict(hist or {})
        except Exception:
            pass

    def _score_spot_entry_quality(self, sym: str, r: dict, regime: dict,
                                  confidence: str, entry_id: str = ""):
        from trading.entry_quality import EntryQuality, score_spot_entry

        try:
            hist = getattr(self, "_spot_entry_quality_history", {}).get(
                str(sym).upper(), {})
            quality = score_spot_entry(
                confidence=confidence,
                rsi_15m=r.get("rsi_15m"),
                rsi_1h=r.get("rsi_1h"),
                rsi_4h=r.get("rsi_4h"),
                change_pct=r.get("change_percent"),
                btc_change_pct=(regime or {}).get("btc_24h"),
                regime=(regime or {}).get("regime"),
                vol_surge=r.get("vol_surge"),
                body_ratio=r.get("body_ratio"),
                macd_hist=r.get("macd_hist"),
                historical_winrate=hist.get("winrate"),
                spread_pct=None,
            )
        except Exception:
            quality = EntryQuality(
                score=0, label="LOW", reasons=("score_error",),
                components={})

        try:
            from core.logger import log_struct
            fields = quality.as_log_fields()
            fields.update({
                "bot": self.BOT_NAME,
                "symbol": sym,
                "stage": "candidate_pre_sizing",
                "mode": "SIM" if self.simulation else "LIVE",
                "confidence": confidence,
                "regime": (regime or {}).get("regime"),
                "btc_change_pct": (regime or {}).get("btc_24h"),
                "entry_quality_min_score": self._entry_quality_min_score(),
                "entry_id": entry_id,
            })
            log_struct("spot_entry_quality", **fields)
        except Exception:
            pass
        return quality

    @staticmethod
    def _quote_cost_or_fallback(order, fallback: float,
                                max_expected: float = 0.0) -> float:
        fallback_value = ScanMixin._positive_float(fallback)
        if not isinstance(order, dict):
            return fallback_value
        max_expected_value = ScanMixin._positive_float(max_expected)
        if max_expected_value > 0 and fallback_value > max_expected_value * 10.0:
            fallback_value = max_expected_value
        cost = ScanMixin._positive_float(order.get("cost"))
        if cost <= 0:
            return fallback_value
        if max_expected_value > 0 and cost > max_expected_value * 10.0:
            return fallback_value
        if fallback_value > 0:
            ratio = cost / fallback_value
            if ratio < 0.1 or ratio > 10.0:
                return fallback_value
        return cost

    def _release_entry_claim_if_untracked(self, sym: str) -> bool:
        try:
            if self.state.has(sym):
                from core.logger import log_event
                log_event(
                    f"{sym}: provisional state exists after buy failure - "
                    f"keeping claim for monitor/reconcile recovery",
                    "WARN",
                )
                return False
        except Exception:
            pass
        from core.database import remove_open_position
        remove_open_position(self.BOT_NAME, sym)
        return True

    def _cleanup_rolled_back_entry_state(self, sym: str, reason: str) -> bool:
        """Remove a state row after a verified rollback sell.

        ``TradeState.add`` can return False after already mutating memory/JSON.
        If rollback flattens the exchange position, we must not only release
        the claim; we must remove or mark any phantom state row too.
        """
        restore = {
            "provisional": True,
            "entry_aborted": True,
            "entry_abort_reason": reason,
            "claim_release_pending": True,
        }
        try:
            removed = self.state.remove(sym, restore)
        except Exception as exc:
            self._log_error(f"cleanup rolled-back entry {sym}", exc)
            removed = False
        if removed:
            return True
        try:
            if self.state.has(sym):
                self.state.update_many(sym, restore)
                return False
        except Exception as exc:
            self._log_error(f"mark rolled-back entry cleanup pending {sym}", exc)
        from core.database import remove_open_position
        remove_open_position(self.BOT_NAME, sym)
        return False

    def _assess_spot_entry_signal(self, r, regime: dict):
        rsi_values = self._rsi_triplet(r)
        if rsi_values is None:
            return "LOW"
        rsi_15, rsi_h, rsi_4 = rsi_values
        tf_score = sum(1 for v in (rsi_15, rsi_h, rsi_4) if 50 <= v <= 75)
        indicators = (
            self._finite_float(r.get("vol_surge", 1.0)),
            self._finite_float(r.get("macd_hist", 0.0)),
            self._finite_float(r.get("body_ratio", 1.0)),
            self._finite_float(r.get("change_percent", 0.0)),
        )
        if any(value is None for value in indicators):
            return "LOW"
        vol_surge, macd_hist, body_ratio, change_percent = indicators
        vol_ok = vol_surge >= 1.15
        macd_ok = macd_hist >= 0.0
        body_ok = body_ratio >= 0.35
        pump_ok = change_percent > 0.0

        score = tf_score + int(vol_ok) + int(macd_ok) + int(body_ok) + int(pump_ok)
        if tf_score >= 2 and score >= 5:
            return "HIGH"
        if tf_score >= 2 and score >= 4:
            return "MEDIUM"
        return "LOW"

    #  Scan-thread body

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
                        f" Circuit breaker: {cb_failures} consecutive failures  "
                        f"backing off {cb_backoff:.0f}s",
                        "WARN"
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
        from trading.market_filters import can_buy_now, get_market_regime
        from trading.risk_manager import is_bot_paused, is_bad_hour
        from trading.screener import get_top_momentum_coins

        # API budget check  shared rate guard across all 3 bots so one bot
        # can't burn the quota and earn a 429 IP-ban for all.
        if budget_exhausted():
            log_event("API budget exhausted  skipping scan cycle", "WAIT")
            return

        # SAFE_MODE check (daily-loss killswitch tripped)  skip buys but
        # don't return error. Monitor still runs and closes existing
        # positions normally.
        if self.safe_mode is not None and self.safe_mode.is_active():
            log_event(
                f" SAFE_MODE active ({self.safe_mode.reason()})  "
                f"no new entries this cycle",
                "WAIT"
            )
            return

        max_trades = int(self.C("MAX_OPEN_TRADES"))
        min_pump = float(self.C("MIN_PUMP"))

        # Dynamic min_pump adjustment for quiet markets: in NEUTRAL regime with
        # sideways/mildly-trending BTC (3%), few coins reach the configured
        # pump threshold, so lower the bar by 0.5% to surface legitimate setups
        # for the LLM to assess.
        _is_quiet = False
        try:
            from trading.market_filters import get_market_regime as _gmr
            _early_regime = _gmr(self.ex)
            _btc24 = float(_early_regime.get("btc_24h", 0.0))
            _is_quiet = (_early_regime.get("regime") == "NEUTRAL"
                          and -3.0 <= _btc24 <= 3.0)
            if _is_quiet and min_pump >= 1.5:
                adj_min_pump = max(1.0, min_pump - 0.5)
                if adj_min_pump < min_pump:
                    log_event(
                        f"Quiet market (NEUTRAL, BTC 24h {_btc24:+.1f}%)  "
                        f"min_pump adjusted {min_pump}% -> {adj_min_pump}% "
                        f"to find more candidates", "INFO"
                    )
                    min_pump = adj_min_pump
        except Exception:
            pass

        # Balance check (live only)
        if self.simulation:
            balance = 1000.0
        else:
            balance = safe_fetch_balance_usdt(
                self.ex, error_logger=self._log_error
            )
            if balance is None:
                log_event(
                    "Balance unavailable  skipping buy-side this cycle",
                    "WAIT"
                )
                return

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
            import os as _os
            hour = _get_local_hour()
            tz = _os.getenv("BOT_TIMEZONE", "UTC")
            log_event(
                f"[{self.BOT_NAME}] Bad hour ({hour}:xx {tz})", "WAIT"
            )
            return

        open_syms = self.state.keys()
        allowed, reason = can_buy_now(
            self.ex, bot_name=self.BOT_NAME, open_symbols=open_syms
        )
        if not allowed:
            log_event(f"[{self.BOT_NAME}] Markt-Filter: {reason}", "WAIT")
            return

        # Regime + screener
        regime = get_market_regime(self.ex)
        log_event(
            f"Scanning | Pump {min_pump}% | "
            f"Phase: {regime['regime']} | F&G: {regime['fear_greed']}",
            "SCAN"
        )
        try:
            cand = get_top_momentum_coins(
                exchange=self.ex,
                min_pump=min_pump,
                limit=min(12, max(6, max_trades)),
                bot_name=self.BOT_NAME,
                quiet_market=_is_quiet,
            )
        except Exception as e:
            log_event(f"Screener error: {e}", "WARN")
            return

        if cand is None or cand.empty:
            return

        from trading.risk_manager import get_rsi_max, check_blacklist
        rsi_max = get_rsi_max(self.BOT_NAME)

        for _, r in cand.iterrows():
            if self._shutdown_event.is_set():
                return
            if self.state.count() >= max_trades:
                break
            # M-3: re-check the daily-loss / kill-switch per candidate, not just
            # once per scan. An earlier fill THIS cycle can push realized PnL
            # past the cap; without this the remaining candidates would still
            # fire (overshoot). Cheap DB read  no exchange arg, so the
            # per-cycle BTC-crash API call (pre-loop gate above) isn't repeated.
            _paused, _pr = is_bot_paused(
                self.BOT_NAME, simulation=self.simulation)
            if _paused:
                log_event(f"[{self.BOT_NAME}] Kill-switch mid-scan ({_pr})  "
                          f"stopping further entries this cycle", "WAIT")
                break
            sym = r["symbol"].split("/")[0]

            # Quick pre-checks
            if self._is_in_cooldown(sym):
                continue
            if check_blacklist(sym, self.BOT_NAME):
                log_event(f"{sym} auf Blacklist", "WAIT")
                continue
            # COEXISTENCE: skip a coin another bot on this account already holds.
            try:
                from core.database import is_claimed_by_other
                if is_claimed_by_other(sym, self.BOT_NAME, is_futures=False):
                    log_event(f"{sym} held by another bot  skipping (coexistence)", "WAIT")
                    continue
            except Exception as exc:
                log_event(
                    f"{sym} entry scan aborted: claim registry unavailable "
                    f"({type(exc).__name__})",
                    "WARN",
                )
                return
            if self.state.has(sym):
                continue

            # Per-candidate correlation + price check
            try:
                ok, reason = can_buy_now(
                    self.ex, bot_name=self.BOT_NAME,
                    open_symbols=self.state.keys(),
                    candidate_symbol=f"{sym}/USDT",
                    known_price=self._positive_float(r.get("price")) or None,
                )
                if not ok:
                    log_event(f"{sym} skipped  {reason}", "WAIT")
                    continue
            except Exception as _filt_err:
                # Normal feed/API failures are handled inside can_buy_now().
                # Anything escaping that safety boundary means the gate itself
                # is unavailable, so a new money-bearing entry must not proceed.
                log_event(f"{sym} filter check error "
                          f"({type(_filt_err).__name__}: {_filt_err})  "
                          f"entry skipped fail-closed", "WARN")
                continue

            # Multi-RSI gate
            rsi_values = self._rsi_triplet(r)
            if rsi_values is None:
                log_event(f"{sym} skipped  invalid RSI payload", "WARN")
                continue
            rsi_too_high = sum(1 for v in rsi_values if v > rsi_max)
            if rsi_too_high >= 2:
                log_event(
                    f"{sym} skipped  {rsi_too_high}/3 RSIs > {rsi_max:.0f}",
                    "WAIT"
                )
                continue

            # Try the entry  encapsulates LLM, quality filters, order
            try:
                opened_usdt = self._try_open_trade(r, regime, balance)
                if opened_usdt is not None and not self.simulation and balance is not None:
                    balance = max(0.0, balance - float(opened_usdt))
            except Exception as e:
                log_event(f"Open-trade {sym} failed: {e}", "WARN")
                self._log_error(f"_try_open_trade {sym}", e)

    #  Cooldown helpers

    def _is_in_cooldown(self, sym: str) -> bool:
        """Read-only cooldown check (doesn't write to disk)."""
        try:
            from trading.cooldown_utils import check_in_cooldown
            return check_in_cooldown(self.cool, sym)
        except ImportError:
            return False

    #  Try opening one trade (per candidate)

    def _try_open_trade(self, r, regime: dict, balance: float) -> float | None:
        """Run LLM analysis + quality filters + place order if everything
        passes. Mutates self.state on success and returns reserved USDT."""
        from core.logger import log_event, log_buy, log_struct, send_telegram
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        sym = r["symbol"].split("/")[0]
        signal_price = self._positive_float(r.get("price"))
        if signal_price <= 0:
            log_event(
                f"{sym}: spot entry skipped  invalid screener price "
                f"{r.get('price')!r}",
                "WARN",
            )
            return None

        use_llm = self._bool_cfg_value(self.C("USE_LLM", False), False)
        news = ""
        ans = None
        analysis_is_keyword = False
        direction = "BUY"
        if use_llm:
            log_event(f"Analyzing {sym} ...", "INFO")
            try:
                news = self._news.get_latest_news(sym)
                ans = self._news.analyze_sentiment(
                    sym, r["change_percent"],
                    r["rsi_15m"], r["rsi_1h"], r["rsi_4h"], news,
                    market_regime=regime,
                )
            except Exception as e:
                log_event(f"Analysis {sym} failed: {e}", "WARN")
                return None

            analysis_is_keyword = "[keyword fallback" in str(ans or "").lower()
            try:
                direction, confidence = (
                    self._news.parse_direction_and_confidence(ans))
            except Exception as e:
                log_event(f"parse_direction {sym} failed: {e}", "WARN")
                return None
            if direction != "BUY":
                source = "Keyword fallback" if analysis_is_keyword else "AI"
                log_event(f"{sym}: {source} says {direction} - skipped", "WAIT")
                return None
        else:
            log_event(f"Signal check {sym} (Spot) ...", "INFO")
            confidence = self._assess_spot_entry_signal(r, regime)

        allow, size_mult, why = self._quality_filters(
            sym, r, ans, regime,
            confidence_override=None if use_llm else confidence)
        if not allow:
            log_event(f"{sym}: {why}", "WAIT")
            return None

        from trading.entry_lifecycle import (emit_entry_lifecycle,
                                             new_entry_id)
        entry_mode = "SIM" if self.simulation else "LIVE"
        entry_id = new_entry_id(
            bot=self.BOT_NAME, symbol=sym, mode=entry_mode, direction="BUY")
        quality = self._score_spot_entry_quality(
            sym, r, regime, confidence, entry_id)
        if (not self.simulation and self._entry_quality_filter_enabled()
                and ("score_error" in quality.reasons
                     or quality.score < self._entry_quality_min_score())):
            log_event(
                f"{sym}: BUY blocked  entry quality "
                f"{quality.score} < {self._entry_quality_min_score():.0f} "
                f"({quality.label}; {','.join(quality.reasons) or 'no_reason'})",
                "WAIT")
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="blocked", mode=entry_mode, reason="entry_quality")
            return None

        expectancy_features = {
            "score": float(quality.score),
            "confidence": {
                "LOW": 0.0, "MEDIUM": 0.5, "HIGH": 1.0,
            }.get(str(confidence).strip().upper(), 0.0),
            "change_pct": float(r.get("change_percent") or 0.0),
            "rsi_15m": float(r.get("rsi_15m") or 0.0),
            "rsi_1h": float(r.get("rsi_1h") or 0.0),
            "rsi_4h": float(r.get("rsi_4h") or 0.0),
        }
        from trading.expectancy_telemetry import emit_expectancy_candidate

        emit_expectancy_candidate(
            bot=self.BOT_NAME,
            entry_id=entry_id,
            symbol=sym,
            mode=entry_mode,
            features=expectancy_features,
        )

        # Bull/Bear devil's-advocate veto is handled inside
        # news_brain.analyze_sentiment() when direction=="BUY" (it has full
        # prompt context); if it vetoes, direction is "WAIT" and we already
        # returned above. Nothing to do here.

        # Position sizing
        from trading.risk_manager import get_position_size
        trade_usdt = get_position_size(self.BOT_NAME) * size_mult
        try:
            trade_usdt = float(trade_usdt)
        except (TypeError, ValueError, OverflowError):
            trade_usdt = 0.0
        if not math.isfinite(trade_usdt) or trade_usdt <= 0:
            log_event(
                f"{sym}: spot entry skipped  invalid trade size "
                f"{trade_usdt!r}",
                "WARN",
            )
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="aborted", mode=entry_mode, reason="invalid_size")
            return None
        if regime["regime"] == "BEAR":
            trade_usdt = max(5.0, trade_usdt * 0.5)
            log_event(f"BEAR phase: position halved to {trade_usdt} USDT", "INFO")
        elif regime["regime"] == "NEUTRAL":
            trade_usdt = max(5.0, trade_usdt * 0.75)
            log_event(f"NEUTRAL phase: position reduced to {trade_usdt} USDT", "INFO")

        # Balance pre-check (5% safety margin)
        if not self.simulation and balance is not None:
            required = trade_usdt * 1.05
            if required > balance:
                log_event(
                    f"Skipping {sym}: need ~{required:.2f} USDT "
                    f"(trade {trade_usdt:.2f} + 5% buffer) but "
                    f"only {balance:.2f} USDT free.",
                    "WARN"
                )
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=sym,
                    stage="blocked", mode=entry_mode,
                    reason="insufficient_balance")
                return None

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
                requested_notional=trade_usdt,
                portfolio_mode=portfolio_mode,
                expectancy_mode=expectancy_mode,
                account_type="spot",
                features=expectancy_features,
                limits=portfolio_limits_from_config(
                    self.C, max_net_default=100.0, max_beta_default=100.0
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
                return None

        log_buy(
            self.BOT_NAME, sym, signal_price, trade_usdt,
            (r["rsi_15m"], r["rsi_1h"], r["rsi_4h"]),
            news, "" if analysis_is_keyword else ans
        )

        # Place the order (or simulate)
        if not self.simulation:
            from core.database import claim_symbol_for_entry
            if not claim_symbol_for_entry(
                self.BOT_NAME,
                sym,
                "SPOT",
                intent_id=entry_id,
                notional_usdt=trade_usdt,
                mode=entry_mode,
            ):
                log_event(f"{sym} claimed by another bot  skip "
                          f"(coexistence)", "WAIT")
                emit_entry_lifecycle(
                    entry_id, bot=self.BOT_NAME, symbol=sym,
                    stage="blocked", mode=entry_mode,
                    reason="claim_conflict")
                return None
        _claimed = not self.simulation
        emit_entry_lifecycle(
            entry_id, bot=self.BOT_NAME, symbol=sym,
            stage="order_attempt", mode=entry_mode)
        try:
            entry = self._place_buy_order(
                sym, r, trade_usdt, entry_id=entry_id
            )
        except SpotBuyOutcomeUnknown as _buy_exc:
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="order_unknown", mode=entry_mode,
                reason=type(_buy_exc).__name__)
            raise
        except Exception as _buy_exc:
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="order_failed", mode=entry_mode,
                reason=type(_buy_exc).__name__)
            if _claimed:
                released = self._release_entry_claim_if_untracked(sym)
                if released:
                    from core.database import release_portfolio_reservation

                    release_portfolio_reservation(entry_id)
            raise _buy_exc
        if entry is None:
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="order_failed", mode=entry_mode,
                reason="no_verified_fill")
            if _claimed:
                released = self._release_entry_claim_if_untracked(sym)
                if released:
                    from core.database import release_portfolio_reservation

                    release_portfolio_reservation(entry_id)
            return None  # buy failed  already logged

        amount, fill_price, gross_amount, invested_usdt, entry_fee = entry

        # Persist the final position. _place_buy_order already wrote a
        # PROVISIONAL row (zombie protection) before the slow fee refetches, so
        # patch that row in place with the corrected NET amount + fees +
        # metadata instead of a second full add  one row identity, one claim
        # refresh (now carrying the net amount). Fall back to add() only if the
        # provisional write didn't land.
        from trading.market_filters import get_btc_change, get_fear_greed
        from core.logger import _date as _utc_now_str   # UTC timestamps
        position_fields = {
            "buy": fill_price,
            "highest": fill_price,
            "lowest": fill_price,
            "invested_usdt": invested_usdt,
            "amount": amount,
            "original_amount": amount,
            "rsi_15m": r["rsi_15m"],
            "rsi_1h": r["rsi_1h"],
            "rsi_4h": r["rsi_4h"],
            "change_pct": r["change_percent"],
            "btc_trend": get_btc_change(self.ex, hours=1),
            "fear_greed": get_fear_greed(),
            "partial_sold": False,
            "be_active": False,
            "break_even": False,
            "initial_entry_fee": entry_fee,
            "fees_paid": entry_fee,
            "entry_quality_score": quality.score,
            "entry_quality_label": quality.label,
            "entry_quality_reasons": ",".join(quality.reasons),
            "entry_id": entry_id,
            "provisional": False,
        }
        if self.state.has(sym):
            # Keep the provisional buy_time (earliest, most accurate entry time).
            state_ok = self.state.update_many(sym, position_fields)
        else:
            position_fields["buy_time"] = _utc_now_str()
            state_ok = self.state.add(sym, position_fields)
        if state_ok is False and not self.simulation:
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="state_failed", mode=entry_mode,
                reason="post_fill_state_write")
            log_event(
                f"Buy {sym}: state write failed after LIVE fill  "
                f"attempting immediate rollback sell", "WARN")
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
                if spot_entry_rollback_was_fully_filled(order, sold_amount):
                    self._cleanup_rolled_back_entry_state(
                        sym, "state write failed after live buy")
                    log_event(
                        f"Buy {sym}: rollback sell filled after state failure",
                        "WARN")
                else:
                    log_event(
                        f"Buy {sym}: CRITICAL rollback sell not verified "
                        f"after state failure; claim kept for manual recovery",
                        "ERROR")
            except Exception as rb_exc:
                log_event(
                    f"Buy {sym}: CRITICAL untracked live position risk after "
                    f"state failure; rollback sell failed ({rb_exc})", "WARN")
                self._log_error(f"spot rollback after state failure {sym}", rb_exc)
            return

        if state_ok is False:
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="state_failed", mode=entry_mode,
                reason="post_fill_state_write")
        else:
            if not self.simulation:
                from core.database import release_portfolio_reservation

                release_portfolio_reservation(entry_id, status="CONSUMED")
            emit_entry_lifecycle(
                entry_id, bot=self.BOT_NAME, symbol=sym,
                stage="opened", mode=entry_mode, fill_price=fill_price,
                size_usdt=float(trade_usdt))

        try:
            from core.logger import log_struct
            from news.news_brain_core import parse_rationale
            event_name = (
                "keyword_decision" if analysis_is_keyword
                else ("llm_decision" if use_llm else "signal_decision")
            )
            log_struct(
                event_name,
                bot=self.BOT_NAME, symbol=sym, action="BUY",
                direction=direction, confidence=confidence,
                source=("keyword" if analysis_is_keyword
                        else ("llm" if use_llm else "signal")),
                fallback=bool(analysis_is_keyword),
                rationale=parse_rationale(ans) if ans is not None else "",
                price=signal_price, size_usdt=float(trade_usdt),
                sim=bool(self.simulation),
            )
        except Exception:
            pass

        try:
            if not self.simulation:
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f" [{self.BOT_NAME}] KAUF {sym}\n"
                    f"Preis: {signal_price:.6f} USDT\n"
                    f"Einsatz: {trade_usdt:.2f} USDT (Kelly)\n"
                    f"RSI: 15m {r['rsi_15m']:.1f}|1h {r['rsi_1h']:.1f}|4h {r['rsi_4h']:.1f}\n"
                    f"Pump: {r['change_percent']:.1f}% | Phase: {regime['regime']}"
                )
        except Exception as e:
            log_event(f"Telegram failed: {e}", "WARN")
        return float(trade_usdt)

    #  Quality filters

    def _quality_filters(self, sym: str, r, ans: str, regime: dict,
                         confidence_override: str = None):
        """Run all quality gates. Returns (allow, size_mult, reason).

        Subclasses can override this to plug in/out individual checks.
        Default implementation matches balanced + aggressive (identical).
        """
        from core.logger import log_event

        # 1. Confidence. With USE_LLM=false this is a mechanical signal score,
        # not an AI verdict; do not touch self._news on that path.
        if confidence_override is None:
            confidence = self._news.parse_confidence(ans)
            low_reason = "AI says BUY but LOW confidence - skipped"
        else:
            confidence = str(confidence_override).upper()
            low_reason = "Signal confidence LOW - skipped"
        if confidence == "LOW":
            return False, 0.0, low_reason
        rsi_15, rsi_h, rsi_4 = r["rsi_15m"], r["rsi_1h"], r["rsi_4h"]
        tf_score = sum(1 for v in (rsi_15, rsi_h, rsi_4) if 50 <= v <= 75)

        # 2. RSI overbought-on-all-TFs block
        if rsi_15 > 80 and rsi_h > 75 and rsi_4 > 70:
            return False, 0.0, (
                f"BUY blocked  RSI overbought on all TFs "
                f"(15m={rsi_15:.0f}, 1h={rsi_h:.0f}, 4h={rsi_4:.0f})"
            )

        # 3. Weak multi-TF + non-HIGH conf
        if tf_score < 2 and confidence != "HIGH":
            return False, 0.0, (
                f"BUY blocked  weak multi-TF ({tf_score}/3) + "
                f"{confidence} confidence"
            )

        # 4. Historical setup winrate
        try:
            from core.database import get_historical_winrate_for_setup
            rsi_bucket = (0 if rsi_h < 40
                          else 1 if rsi_h < 55
                          else 2 if rsi_h < 70
                          else 3 if rsi_h < 85
                          else 4)
            chg = r["change_percent"]
            chg_bucket = (0 if chg < 3
                          else 1 if chg < 8
                          else 2 if chg < 15
                          else 3 if chg < 25
                          else 4)
            hist = get_historical_winrate_for_setup(
                self.BOT_NAME, rsi_bucket, chg_bucket, min_sample=8
            )
            self._remember_spot_setup_history(sym, hist)
            if (hist["winrate"] is not None
                    and hist["winrate"] < 0.30
                    and confidence != "HIGH"):
                return False, 0.0, (
                    f"BUY blocked  historical winrate "
                    f"{hist['winrate']*100:.0f}% over "
                    f"{hist['trade_count']} trades"
                )
        except Exception as e:
            self._log_error(f"historical winrate lookup {sym}", e)

        # 5. Quality score
        size_mult = 1.0
        try:
            from trading.risk_manager import score_trade_quality
            q = score_trade_quality(
                symbol=sym, bot_name=self.BOT_NAME,
                rsi_1h=r["rsi_1h"],
                atr_pct=r.get("atr_pct", 0),
                vol_surge=r.get("vol_surge", 1.0),
                body_ratio=r.get("body_ratio", 1.0),
                macd_hist=r.get("macd_hist", 0),
                change_pct=r["change_percent"],
                fear_greed=regime.get("fear_greed", 50),
                regime=regime.get("regime", "NEUTRAL"),
                price=r["price"],
            )
            if q.get("verdict") == "SKIP":
                return False, 0.0, (
                    f"Quality-Score {q.get('score', 0):.0f}/100 (SKIP)  "
                    f"{q.get('reason', '')}"
                )
            if q.get("verdict") == "WARN":
                size_mult = float(q.get("size_multiplier", 0.5))
                log_event(
                    f"{sym}: Quality {q.get('score', 0):.0f}/100 (WARN)  "
                    f"size  {size_mult:.2f}", "INFO"
                )
            else:
                log_event(
                    f"{sym}: Quality {q.get('score', 0):.0f}/100 (PASS)",
                    "INFO"
                )
        except Exception as e:
            self._log_error(f"score_trade_quality {sym}", e)

        return True, size_mult, ""

    #  Order placement

    def _find_order_by_cid(self, symbol_pair: str, cid: str):
        """Locate an order by OUR clientOrderId (open orders first, then recent
        history). Used to recover from a lost-response timeout so a retry doesn't
        place a SECOND live buy. Returns ``None`` only when every supported
        lookup completed successfully and proved absence; uncertainty raises.
        """
        try:
            from bot_utils.futures_order import _order_client_id_matches
        except Exception:
            def _order_client_id_matches(o, c):
                return o.get("clientOrderId") == c

        has = getattr(self.ex, "has", {}) or {}
        if not isinstance(has, dict):
            has = {}
        attempted = False
        uncertain = False

        def _query(endpoint, fetch):
            nonlocal attempted, uncertain
            try:
                allowed = try_consume_api_call(endpoint, critical=True)
            except Exception as exc:
                raise RuntimeError(
                    f"order reconciliation unavailable: {endpoint}"
                ) from exc
            if not allowed:
                raise RuntimeError(
                    f"order reconciliation unavailable: {endpoint}"
                )
            attempted = True
            try:
                rows = fetch()
            except Exception as exc:
                uncertain = True
                try:
                    self._log_error(f"spot cid reconcile {endpoint}", exc)
                except Exception:
                    pass
                return []
            if not isinstance(rows, list):
                uncertain = True
                return []
            return rows

        fetch_open = getattr(self.ex, "fetch_open_orders", None)
        if callable(fetch_open) and has.get("fetchOpenOrders") is not False:
            for o in _query(
                "spot_reconcile_fetch_open_orders",
                lambda: fetch_open(symbol_pair),
            ):
                if _order_client_id_matches(o, cid):
                    return o

        fetch_orders = getattr(self.ex, "fetch_orders", None)
        if has.get("fetchOrders") and callable(fetch_orders):
            for o in _query(
                "spot_reconcile_fetch_orders",
                lambda: fetch_orders(symbol_pair, limit=20),
            ):
                if _order_client_id_matches(o, cid):
                    return o
        # A just-filled MARKET buy isn't "open", and several venues (bitget  the
        # default  okx, bybit, kucoin, gate) lack unified fetchOrders; the fill
        # shows in closed orders / my-trades. Consult those before giving up so a
        # lost-response retry can't place a SECOND live buy.
        fetch_closed = getattr(self.ex, "fetch_closed_orders", None)
        if has.get("fetchClosedOrders") and callable(fetch_closed):
            for o in _query(
                "spot_reconcile_fetch_closed_orders",
                lambda: fetch_closed(symbol_pair, limit=20),
            ):
                if _order_client_id_matches(o, cid):
                    return o

        fetch_trades = getattr(self.ex, "fetch_my_trades", None)
        if has.get("fetchMyTrades") and callable(fetch_trades):
            matching_trades = [
                trade for trade in _query(
                    "spot_reconcile_fetch_my_trades",
                    lambda: fetch_trades(symbol_pair, limit=20),
                )
                if _order_client_id_matches(trade, cid)
            ]
            if matching_trades:
                from bot_utils.spot_exits import aggregate_spot_order_trades

                return aggregate_spot_order_trades(matching_trades, cid)
        if not attempted or uncertain:
            raise RuntimeError("order reconciliation unavailable")
        return None

    def _execution_quality_gate(self, sym: str, pair: str) -> bool:
        """Pre-trade spread gate for SPOT entries.

        A market buy into a blown-out or vacuum order book fills at the far side
        of the spread. ``check_spread_ok`` aborts on an abnormal spread AND
        records the bad reading against the shared SafeMode circuit breaker.

        When the exchange returns a ticker WITHOUT bid/ask (common for thin
        micro-caps on MEXC), we fall back to the ORDER BOOK for the real
        top-of-book spread and fail CLOSED on a missing/illiquid book. A
        ticker-fetch error fails CLOSED in LIVE so we never place a naked
        market buy without a verified executable quote; SIM stays tolerant
        so paper data collection can continue through transient quote gaps.
        """
        from core.logger import log_event
        try:
            from bot_utils import check_spread_ok
        except Exception as e:
            self._log_error("exec-quality import", e)
            return True
        if not try_consume_api_call("spot_entry_fetch_ticker"):
            log_event(
                f"{sym}: spread gate blocked - API budget exhausted",
                "WAIT",
            )
            return False
        try:
            ticker = self.ex.fetch_ticker(pair)
        except Exception as e:
            log_event(f"{sym}: spread gate skipped  ticker fetch failed ({e})",
                      "WARN")
            return bool(self.simulation)
        # Order-book fallback when the exchange didn't populate bid/ask. Without
        # this, check_spread_ok returns True on missing quotes  illiquid coins
        # slip ~5% on entry. Fail-CLOSED: no readable book = skip the trade.
        ob_derived = False
        if (not self.simulation
                and (not isinstance(ticker, dict)
                     or ticker.get("bid") is None or ticker.get("ask") is None)):
            if not try_consume_api_call("spot_entry_fetch_spread_book"):
                log_event(
                    f"{sym}: order-book spread check blocked - "
                    "API budget exhausted",
                    "WAIT",
                )
                return False
            try:
                ob = self.ex.fetch_order_book(pair, limit=5)
                bids = (ob or {}).get("bids") or []
                asks = (ob or {}).get("asks") or []
                if not bids or not asks:
                    log_event(f"{sym}: empty order book  blocking entry "
                              f"(illiquid, fail-closed)", "WARN")
                    return False
                best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
                if best_bid <= 0 or best_ask <= 0:
                    return False
                ticker = dict(ticker if isinstance(ticker, dict) else {})
                ticker["bid"], ticker["ask"] = best_bid, best_ask
                ob_derived = True
            except Exception as e:
                log_event(f"{sym}: order-book spread check failed ({e})  "
                          f"blocking entry (fail-closed)", "WARN")
                return False
        try:
            # A chronically illiquid micro-cap (order-book-derived wide spread)
            # must be SKIPPED quietly  it must NOT feed the flash-crash circuit
            # breaker, else routine illiquid picks would trip SAFE_MODE and pause
            # ALL entries. The breaker only fires on EXCHANGE-QUOTED spreads
            # (a genuine flash-crash signal on a coin the venue actively quotes).
            from core.constants import MAX_SPREAD_PCT_SPOT
            return bool(check_spread_ok(
                ticker, log_event=log_event, symbol=sym,
                max_spread_pct=MAX_SPREAD_PCT_SPOT,
                safe_mode_instance=(None if ob_derived else self.safe_mode)))
        except Exception as e:
            self._log_error(f"spread gate {sym}", e)
            return bool(self.simulation)

    def _record_entry_slippage(self, sym: str, expected_price: float,
                               fill_price: float) -> None:
        """Feed the slippage circuit breaker after a SPOT fill (recording, not
        blocking  the buy already happened). Repeated abnormal fills trip
        SAFE_MODE. Never raises  telemetry must not break the trade path."""
        try:
            if not getattr(self, "safe_mode", None):
                return
            if expected_price <= 0 or fill_price <= 0:
                return
            self.safe_mode.record_slippage(
                expected_price=expected_price, actual_fill=fill_price,
                symbol=sym, side="buy")
        except Exception as e:
            self._log_error(f"record entry slippage {sym}", e)

    def _place_buy_order(
        self,
        sym: str,
        r,
        trade_usdt: float,
        *,
        entry_id: str | None = None,
    ):
        """Place a market buy. Returns (amount, fill_price, gross_amount,
        invested_usdt, entry_fee) on success, None on failure.

        Validates r["price"] > 0 before division (illiquid coins sometimes
        return price=0) and fill_price > 0 from the exchange (a malformed order
        response would otherwise save buy=0 while the LIVE order is already on
        the exchange  orphan position).
        """
        from core.logger import log_event

        # defensive price validation
        price = ScanMixin._positive_float(r.get("price"))
        if price <= 0:
            log_event(f"Buy {sym}: invalid screener price {r.get('price')!r}  skip", "WARN")
            return None
        try:
            trade_usdt = float(trade_usdt)
        except (TypeError, ValueError, OverflowError):
            trade_usdt = 0.0
        if not math.isfinite(trade_usdt) or trade_usdt <= 0:
            log_event(
                f"Buy {sym}: invalid trade size {trade_usdt!r}  skip",
                "WARN",
            )
            return None

        if self.simulation:
            # Apply realistic adverse slippage in SIM too (same default as
            # core.constants.BACKTEST_SLIPPAGE_PER_SIDE) so SIM fills aren't
            # systematically better than LIVE.
            try:
                from core.constants import BACKTEST_SLIPPAGE_PER_SIDE as _SLP
            except Exception:
                _SLP = 0.001
            sim_fill_price = price * (1.0 + _SLP)  # buy side  adverse = higher
            amount = trade_usdt / sim_fill_price
            invested_usdt = amount * sim_fill_price
            try:
                from core.constants import DEFAULT_TAKER_FEE as _TKR
            except Exception:
                _TKR = 0.001
            entry_fee = invested_usdt * _TKR
            return amount, sim_fill_price, amount, invested_usdt, entry_fee

        # Pre-trade spread gate: refuse a market buy into a blown-out/vacuum
        # book; repeated abnormal spreads also trip SAFE_MODE.
        if not self._execution_quality_gate(sym, f"{sym}/USDT"):
            log_event(
                f"Buy {sym}: ABORT  spread too wide / liquidity vacuum "
                f"(execution-quality gate)  refusing naked market buy", "WARN")
            return None

        # Ask guard: the bot decides on the ticker 'last' price, but a naked
        # market buy fills at the EXECUTABLE ASK. On a fast-pumping coin 'last'
        # lags the book (stale-low) while the ask is already higher. Compare the
        # signal price against the live ORDER-BOOK ASK and abort if the ask ran
        # past SPOT_MAX_CHASE_PCT; otherwise adopt the ask as the buy basis so
        # amount/slippage math reflect what we actually pay.
        from core.constants import SPOT_MAX_CHASE_PCT
        _chase_max = SPOT_MAX_CHASE_PCT
        if not try_consume_api_call("spot_entry_fetch_chase_book"):
            log_event(
                f"Buy {sym}: ABORT - API budget exhausted before "
                "order-book ask",
                "WAIT",
            )
            return None
        try:
            _ob = self.ex.fetch_order_book(f"{sym}/USDT", limit=5)
            _asks = (_ob or {}).get("asks") or []
            _ask = float(_asks[0][0]) if _asks else 0.0
            if _ask > 0 and price > 0:
                _run_pct = (_ask / price - 1.0) * 100.0
                if _run_pct > _chase_max:
                    log_event(
                        f"Buy {sym}: ABORT  ask ran +{_run_pct:.1f}% above signal "
                        f"({price:.6f} -> ask {_ask:.6f}, max {_chase_max:g}%)  "
                        f"not buying into a pumped/illiquid book", "WAIT")
                    return None
                price = _ask  # buy basis = executable ask  honest slippage math
        except Exception as e:
            log_event(
                f"Buy {sym}: ABORT  order-book ask unavailable ({e}) "
                f"(execution-quality gate)", "WARN")
            return None

        try:
            # CRITICAL: CCXT amount is BASE currency, NOT quote.
            amount_coins = trade_usdt / price
            raw_amount_coins = amount_coins
            try:
                raw_precision_amount = self.ex.amount_to_precision(
                    f"{sym}/USDT", amount_coins)
                amount_coins = (
                    0.0 if isinstance(raw_precision_amount, bool)
                    else float(raw_precision_amount)
                )
            except Exception:
                amount_coins = raw_amount_coins
            if not math.isfinite(amount_coins) or amount_coins <= 0:
                log_event(
                    f"Buy {sym}: invalid amount after precision  skip",
                    "WARN")
                return None
            try:
                _mkt = (getattr(self.ex, "markets", {}) or {}).get(f"{sym}/USDT", {})
                _min_amount = ((_mkt.get("limits") or {}).get("amount") or {}).get("min")
                if isinstance(_min_amount, bool):
                    _min_amount = 0.0
                else:
                    _min_amount = float(_min_amount or 0.0)
                if (math.isfinite(_min_amount) and _min_amount > 0.0
                        and amount_coins < _min_amount):
                    log_event(
                        f"Buy {sym}: precision amount {amount_coins:g} < "
                        f"exchange min {_min_amount:g}  skip", "INFO")
                    return None
            except Exception:
                pass
            if amount_coins <= 0:
                log_event(f"Buy {sym}: computed amount is 0  skip", "WARN")
                return None

            # Stable per-intent clientOrderId: the DB claim and lifecycle use
            # this same entry_id, so retries/recovery remain identical across
            # time buckets while different entry intents can never collide.
            from bot_utils.order_utils import order_id_text_or_none
            from trading.execution_quality import make_client_order_id

            stable_entry_id = order_id_text_or_none(entry_id)
            if not stable_entry_id:
                log_event(
                    f"Buy {sym}: missing stable entry_id - refusing LIVE order",
                    "ERROR",
                )
                return None
            cid = make_client_order_id(
                stable_entry_id,
                f"{self.BOT_NAME}:{sym}:entry",
                prefix=self.BUY_PREFIX,
            )

            # On MEXC/Binance spot, CCXT interprets the `amount` arg of a market
            # buy as USDT COST (quote), not COIN QUANTITY (base). Detect via
            # ex.id and pass the USDT cost (with createMarketBuyOrderRequiresPrice
            # disabled). Other exchanges (Bitget/Kraken/Kucoin) use standard
            # coin-amount semantics.
            ex_id = (getattr(self.ex, "id", "") or "").lower()
            quote_first_buy = ex_id in ("mexc", "binance", "binanceusdm",
                                          "binancecoinm")
            try:
                if quote_first_buy:
                    # Disable the price-requirement check so we can pass
                    # cost directly. Some ccxt versions need this opt-in.
                    try:
                        self.ex.options["createMarketBuyOrderRequiresPrice"] = False
                    except Exception:
                        pass
                    # On MEXC/Binance: the FIRST positional arg becomes
                    # the USDT cost when this option is False. We also
                    # send 'cost' in params as belt-and-suspenders for
                    # ccxt versions that read it from params.
                    order = _create_market_buy_budgeted(
                        self.ex,
                        f"{sym}/USDT", trade_usdt,
                        params={"clientOrderId": cid, "cost": trade_usdt}
                    )
                else:
                    # Bitget/Kraken/etc: standard coin-amount semantics
                    order = _create_market_buy_budgeted(
                        self.ex,
                        f"{sym}/USDT", amount_coins,
                        params={"clientOrderId": cid}
                    )
            except _SpotBuyBudgetUnavailable as _budget_err:
                log_event(f"Buy {sym}: {_budget_err}", "WAIT")
                return None
            except Exception as _place_err:
                # A placement exception does NOT prove the order never reached
                # the exchange  it may be a lost-response timeout AFTER a fill.
                # Reconcile by our stable clientOrderId first; only re-fire when
                # we can prove no order exists AND the failure was the exchange
                # rejecting the param itself (avoids a double-buy).
                try:
                    recovered = self._find_order_by_cid(
                        f"{sym}/USDT", cid
                    )
                except Exception as _recovery_err:
                    raise SpotBuyOutcomeUnknown(cid) from _recovery_err
                if recovered is not None:
                    # An order carrying our clientOrderId EXISTS on the exchange
                    # (filled OR still resting)  adopt it; NEVER re-fire, that
                    # would double-buy. The fill guard below books only what
                    # actually filled. (Previously this required filled>0, so an
                    # accepted-but-not-yet-filled order fell through to a re-fire.)
                    order = recovered
                elif _client_order_id_parameter_rejected(_place_err):
                    # Param rejected  first call never placed an order; safe
                    # to retry once WITHOUT the clientOrderId param.
                    try:
                        if quote_first_buy:
                            order = _create_market_buy_budgeted(
                                self.ex,
                                f"{sym}/USDT",
                                trade_usdt,
                                params={"cost": trade_usdt},
                            )
                        else:
                            order = _create_market_buy_budgeted(
                                self.ex,
                                f"{sym}/USDT",
                                amount_coins,
                            )
                    except _SpotBuyBudgetUnavailable as _budget_err:
                        log_event(f"Buy {sym}: {_budget_err}", "WAIT")
                        return None
                    except Exception as _fallback_err:
                        raise SpotBuyOutcomeUnknown(cid) from _fallback_err
                else:
                    # Unknown failure and no confirmable fill. Do NOT re-fire
                    # (double-buy risk). Skip; if an order DID land, the
                    # exchange-reconcile path will flag it as an orphan.
                    log_event(
                        f"Buy {sym}: placement failed ({_place_err}) and no "
                        f"order found for cid={cid}  skipping (no blind "
                        f"retry, to avoid a duplicate position)", "WARN")
                    raise SpotBuyOutcomeUnknown(cid) from _place_err

            raw_order_status = (
                order.get("status") if isinstance(order, dict) else None
            )
            order_status = (
                raw_order_status.strip().lower()
                if isinstance(raw_order_status, str) else ""
            )
            if order_status in ("new", "open", "pending"):
                raise SpotBuyOutcomeUnknown(cid)

            # Symmetric to the sell path's order_was_filled guard: never book an
            # unfilled buy as an open position. An accepted order with no
            # fill/cost data is MEXC's normal minimal response and DID execute
            # (treated as filled); a truly empty response (no id, no fill, no
            # cost) or an explicit rejection is a phantom  skip it so a later
            # sell can't loop on 30005 oversold. min_fill_ratio0 keeps real
            # partial fills (handled just below), rejecting only nothing-filled.
            from bot_utils.order_utils import order_was_filled
            if not order_was_filled(
                order,
                amount_coins,
                min_fill_ratio=1e-9,
                trust_terminal_status_with_bad_numbers=True,
            ):
                log_event(
                    f"Buy {sym}: order returned but NOT filled "
                    f"(order={order!r})  skipping, no position booked", "WARN")
                return None

            provisional_written = False
            provisional_fill_price = extract_fill_price(order, price)
            if (not math.isfinite(provisional_fill_price)
                    or provisional_fill_price <= 0):
                provisional_fill_price = price
            try:
                provisional_amount = float(order.get("filled") or amount_coins)
                if not math.isfinite(provisional_amount) or provisional_amount <= 0:
                    raise ValueError("non-finite filled amount")
            except (TypeError, ValueError, OverflowError):
                provisional_amount = amount_coins
            try:
                from core.logger import _date as _utc_now_str_inner
                provisional_invested_usdt = ScanMixin._quote_cost_or_fallback(
                    order, provisional_amount * provisional_fill_price,
                    trade_usdt)
                provisional_ok = self.state.add(sym, {
                    "buy": provisional_fill_price,
                    "highest": provisional_fill_price,
                    "lowest": provisional_fill_price,
                    "buy_time": _utc_now_str_inner(),
                    "invested_usdt": provisional_invested_usdt,
                    "amount": provisional_amount,
                    "original_amount": provisional_amount,
                    "partial_sold": False,
                    "be_active": False,
                    "break_even": False,
                    "initial_entry_fee": 0.0,
                    "fees_paid": 0.0,
                    "entry_id": entry_id,
                    "provisional": True,
                })
                provisional_written = provisional_ok is not False
                if provisional_ok is False:
                    log_event(
                        f"Buy {sym}: early provisional state-write returned "
                        f"False; final state write must recover before claim "
                        f"release",
                        "WARN")
            except Exception as _prov_e:
                log_event(
                    f"Buy {sym}: early provisional state-write failed "
                    f"({_prov_e})  relying on final write", "WARN")

            try:
                amount = float(order.get("filled") or amount_coins)
                if not math.isfinite(amount) or amount <= 0:
                    raise ValueError("non-finite filled amount")
            except (TypeError, ValueError, OverflowError):
                amount = amount_coins
                log_event(
                    f"Buy {sym}: filled amount malformed after accepted order; "
                    f"using intended amount {amount:.8f} for tracking", "WARN")
            # detect partial fill
            if amount < amount_coins * 0.95:
                log_event(
                    f"Buy {sym}: PARTIAL FILL "
                    f"{amount:.6f}/{amount_coins:.6f} "
                    f"({100*amount/amount_coins:.1f}%)  using filled amount",
                    "WARN"
                )

            fill_price = extract_fill_price(order, price)

            # Record realized entry slippage so repeated bad fills trip
            # SAFE_MODE (no-op when fill_price<=0; handled by the screener-price
            # recovery just below).
            self._record_entry_slippage(sym, expected_price=price,
                                        fill_price=fill_price)

            # fill_price validation
            if not math.isfinite(fill_price) or fill_price <= 0:
                # Rather than leave an orphan on the exchange, persist a
                # provisional state with the screener price as the entry
                # estimate so the monitor tracks the position from the next
                # tick (slight PnL-accounting drift, but never an untracked
                # position).
                log_event(
                    f"Buy {sym}: fill_price=0 returned by exchange  "
                    f"recovering by using screener price ({price:.6f}) "
                    f"as entry estimate so monitor tracks the position",
                    "WARN"
                )
                self._log_error(
                    f"recovered orphan candidate {sym}",
                    Exception(f"clientOrderId={cid}, order={order!r}")
                )
                fill_price = price

            invested_usdt = ScanMixin._quote_cost_or_fallback(
                order, amount * fill_price, trade_usdt)

            # Zombie protection  write a PROVISIONAL state row immediately
            # after the order returns, BEFORE the slow fee refetches (~1.8s). If
            # the bot is SIGKILLed in that window, the position would otherwise
            # be on the exchange with no state-tracking  no SL/TP  orphan.
            # Final fees + amount adjustment happen further down (the final
            # state.add corrects the provisional values).
            try:
                from core.logger import _date as _utc_now_str_inner
                if not provisional_written:
                    provisional_ok = self.state.add(sym, {
                        "buy": fill_price,
                        "highest": fill_price,
                        "lowest": fill_price,
                        "buy_time": _utc_now_str_inner(),
                        "invested_usdt": invested_usdt,
                        "amount": amount,
                        "original_amount": amount,
                        "partial_sold": False,
                        "be_active": False,
                        "break_even": False,
                        "initial_entry_fee": 0.0,
                        "fees_paid": 0.0,
                        "entry_id": entry_id,
                        "provisional": True,
                    })
                    if provisional_ok is False:
                        log_event(
                            f"Buy {sym}: provisional state-write returned "
                            f"False; final state write must recover before "
                            f"claim release",
                            "WARN")
            except Exception as _prov_e:
                # State write failure is non-fatal here  the final state.add
                # below will retry. We just log.
                log_event(
                    f"Buy {sym}: provisional state-write failed "
                    f"({_prov_e})  relying on final write", "INFO"
                )

            # Entry fee. Once a live buy has filled, fee extraction must not
            # turn the whole entry into "failed"; otherwise the provisional
            # state remains rough and the final net amount is never written.
            try:
                from trading.fee_utils import extract_or_estimate_with_refetch
                entry_fee = extract_or_estimate_with_refetch(
                    self.ex, order, f"{sym}/USDT", fill_price,
                    base_override=sym
                )
            except Exception as fee_exc:
                try:
                    from bot_utils.order_utils import extract_order_fee as _eof
                    entry_fee = _eof(order)
                except Exception:
                    entry_fee = max(0.0, amount * fill_price * 0.001)
                log_event(
                    f"Buy {sym}: entry-fee lookup failed "
                    f"({type(fee_exc).__name__}); using fallback fee "
                    f"{entry_fee:.6f} USDT",
                    "WARN",
                )

            # Async-aware base-fee resolution: the fee-in-base may not be
            # settled in the initial order response (200-500ms async delay on
            # Bitget/Binance). If unaccounted, the recorded amount would exceed
            # the real wallet balance and the later sell would hit
            # InsufficientBalance. This helper refetches up to 3 and falls back
            # to a taker-rate estimate.
            gross_amount = amount
            try:
                from bot_utils.spot_fee_settle import extract_or_estimate_base_fee
                base_fee = extract_or_estimate_base_fee(
                    self.ex, order, f"{sym}/USDT", sym,
                    max_attempts=3,
                    retry_delay=0.3,
                    fallback_filled=gross_amount,
                    log_event=log_event,
                    shutdown_event=getattr(self, "_shutdown_event", None),
                )
            except Exception as base_fee_exc:
                base_fee = max(0.0, gross_amount * 0.001)
                try:
                    from bot_utils.spot_fee_settle import SPOT_DEFAULT_TAKER_FEE
                    base_fee = max(0.0, gross_amount * SPOT_DEFAULT_TAKER_FEE)
                except Exception:
                    pass
                log_event(
                    f"Buy {sym}: base-fee lookup failed "
                    f"({type(base_fee_exc).__name__}); using fallback "
                    f"{base_fee:.8f} {sym} so the entry is still tracked",
                    "WARN",
                )
            try:
                raw_base_fee = base_fee
                if isinstance(raw_base_fee, bool):
                    raise ValueError("boolean base fee")
                base_fee = float(raw_base_fee)
                max_plausible_base_fee = gross_amount * 0.05
                if (not math.isfinite(base_fee)
                        or base_fee < 0.0
                        or base_fee > max_plausible_base_fee):
                    raise ValueError("invalid base fee")
            except (TypeError, ValueError, OverflowError):
                try:
                    from bot_utils.spot_fee_settle import SPOT_DEFAULT_TAKER_FEE
                    fee_rate = float(SPOT_DEFAULT_TAKER_FEE)
                except Exception:
                    fee_rate = 0.001
                if not math.isfinite(fee_rate) or fee_rate < 0.0 or fee_rate >= 1.0:
                    fee_rate = 0.001
                base_fee = max(0.0, gross_amount * fee_rate)
                log_event(
                    f"Buy {sym}: base-fee lookup returned invalid "
                    f"{raw_base_fee!r}; using fallback {base_fee:.8f} {sym}",
                    "WARN",
                )
            if base_fee > 0:
                amount = max(0.0, amount - base_fee)

            return amount, fill_price, gross_amount, invested_usdt, entry_fee

        except SpotBuyOutcomeUnknown:
            raise
        except Exception as e:
            log_event(f"Buy order {sym} failed: {e}", "WARN")
            return None
