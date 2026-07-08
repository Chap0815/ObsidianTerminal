"""
core/trend_futures_bot.py  Per-coin LEVERAGED trend-following futures bot.

A DIRECTIONAL long/flat trend-follower on perpetual futures  NOT market-neutral
(that's CrossBot). Reuses FuturesBot's whole lifecycle (connect, state, ticker
cache, safe-mode, daily killswitch, reconcile, emergency-close) and overrides
only the two trading loops:

  _scan_loop  TREND engine: every TREND_CHECK_MINUTES, fetch OHLCV for the
                    universe + every held coin, compute the SMA-ensemble signal
                    (trading/trend_signal.py), OPEN longs on a new trend, CLOSE on
                    trend-off (with hysteresis).
  _monitor_loop  fast safety net (~MONITOR_INTERVAL): per held position a
                    price-based hard stop (INITIAL_STOP_LOSS), a liquidation-buffer
                    guard (LIQ_SAFETY_PCT), and the inherited daily killswitch.

Edge basis: the SMA-ensemble trend edge validated in tools/trend_check.py and
tools/trend_leverage_check.py  beats buy-and-hold and halves drawdown on a
diversified universe. LEVERAGE AMPLIFIES the (large) trend drawdowns: the tested
data showed 3 is account-ruinous, 1 is the sane default. Effective leverage may
be FRACTIONAL  the bot sizes notional = margin  LEVERAGE and sends ceil(LEVERAGE)
to the exchange as the integer cap. Long-only by design. SIM-first.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from core.futures_bot import FuturesBot
from core.cross_bot import _is_crypto_base   # shared crypto-only perp filter
from trading.trend_signal import is_in_trend, params_from_cfg, has_full_history
from trading.vol_target import (realized_vol, vol_target_multiplier,
                                basket_median_vol)


class TrendFuturesBot(FuturesBot):
    BOT_NAME = "FUTREND"
    BUY_PREFIX = "ftr"
    NEWS_MODULE_PATH = ""  # no LLM/news  see _news override

    #  No news/LLM: satisfy FuturesBot.run()'s `_ = self._news` probe 
    @property
    def _news(self):
        return None

    #  Config helpers 
    def _f(self, key, default):
        try:
            return float(self.C(key, default))
        except (TypeError, ValueError):
            return default

    def _i(self, key, default):
        try:
            return int(float(self.C(key, default)))
        except (TypeError, ValueError):
            return default

    def _timeframe(self) -> str:
        tf = str(self.C("TREND_TIMEFRAME", "1h")).strip().lower()
        return tf if tf in ("1h", "4h", "1d") else "1h"

    def _check_interval_sec(self) -> int:
        # How often to recompute the trend signal (one candle is the natural
        # cadence; default 30 min for the 1h timeframe).
        return max(60, self._i("TREND_CHECK_MINUTES", 30) * 60)

    def _bars_needed(self, p) -> int:
        # Enough history for the slowest SMA + warmup headroom.
        return max(p.sma_slow, p.cross_slow, p.sma_fast, p.cross_fast) + 60

    def _safe_float(self, value, default: float = 0.0) -> float:
        if isinstance(value, bool):
            return default
        try:
            out = float(value)
            return out if math.isfinite(out) else default
        except (TypeError, ValueError, OverflowError):
            return default

    @staticmethod
    def _precision_amount_or_none(value):
        if isinstance(value, bool):
            return None
        try:
            amount = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return amount if math.isfinite(amount) else None

    @staticmethod
    def _finite_float_or_none(value) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            out = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return out if math.isfinite(out) else None

    def _entry_contract_size(self, full: str, fallback_reader) -> float:
        markets = getattr(self.ex, "markets", None) or {}
        market = markets.get(full) or {}
        info = market.get("info") if isinstance(market, dict) else {}
        if not isinstance(info, dict):
            info = {}
        candidates = []
        if isinstance(market, dict):
            candidates.extend([
                market.get("contractSize"),
                market.get("contract_size"),
            ])
        candidates.extend([
            info.get("contractSize"),
            info.get("contract_size"),
        ])
        explicit = [value for value in candidates if value is not None]
        for value in explicit:
            cs = self._safe_float(value, 0.0)
            if cs > 0.0:
                return cs
        if explicit:
            return 0.0
        return self._safe_float(fallback_reader(self.ex, full), 0.0)

    def _bool_cfg(self, key: str, default: bool = False) -> bool:
        value = self.C(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on")
        return bool(default)

    def _position_age_minutes(self, d: dict) -> Optional[float]:
        raw = d.get("buy_time") or d.get("opened_at")
        if not raw:
            return None
        try:
            if isinstance(raw, (int, float)):
                ts = float(raw)
            else:
                text = str(raw).strip().replace("Z", "+00:00")
                try:
                    opened = datetime.fromisoformat(text)
                except ValueError:
                    opened = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                ts = opened.timestamp()
            return max(0.0, (time.time() - ts) / 60.0)
        except Exception:
            return None

    def _failed_entry_stop_hit(self, d: dict, move: float, high_move: float) -> bool:
        if not self._bool_cfg("FAILED_ENTRY_STOP_ENABLED", False):
            return False
        if d.get("adopted"):
            return False
        if d.get("partial_sold") or d.get("be_active") or d.get("break_even"):
            return False
        max_age = max(1, self._i("FAILED_ENTRY_MAX_AGE_MIN", 120))
        age = self._position_age_minutes(d)
        if age is None or age > max_age:
            return False
        min_mfe = max(0.0, self._f("FAILED_ENTRY_MIN_MFE_PCT", 0.5))
        loss_pct = self._f("FAILED_ENTRY_LOSS_PCT", -2.5)
        if loss_pct >= 0:
            return False
        return high_move < min_mfe and move <= loss_pct

    def _pre_activation_giveback_stop_hit(
        self, d: dict, move: float, high_move: float
    ) -> bool:
        if not self._bool_cfg("PRE_ACTIVATION_GIVEBACK_STOP_ENABLED", False):
            return False
        if d.get("adopted"):
            return False
        if d.get("partial_sold") or d.get("be_active") or d.get("break_even"):
            return False
        activation = self._f("ACTIVATION_PROFIT", 0.0)
        if activation <= 0.0 or high_move >= activation:
            return False
        min_mfe = max(0.0, self._f("PRE_ACTIVATION_MIN_MFE_PCT", 0.8))
        giveback_limit = max(
            0.1, self._f("PRE_ACTIVATION_GIVEBACK_PCT", 2.75)
        )
        giveback = high_move - move
        return high_move >= min_mfe and giveback >= giveback_limit

    def _ticker_field(self, ticker: dict, *keys, default=None):
        if not isinstance(ticker, dict):
            return default
        for key in keys:
            if key in ticker and ticker.get(key) is not None:
                return ticker.get(key)
        info = ticker.get("info") if isinstance(ticker, dict) else None
        if isinstance(info, dict):
            for key in keys:
                if key in info and info.get(key) is not None:
                    return info.get(key)
        return default

    def _entry_shadow_snapshot(self, base: str, full: str, ticker: dict,
                               price: float, margin: float, eff_lev: float,
                               entry_meta: Optional[dict] = None) -> dict:
        bid = self._safe_float(self._ticker_field(ticker, "bid", "bidPrice"))
        ask = self._safe_float(self._ticker_field(ticker, "ask", "askPrice"))
        if (bid <= 0 or ask <= 0) and not self.simulation:
            try:
                ob = self.ex.fetch_order_book(full, limit=1)
                bids = (ob or {}).get("bids") or []
                asks = (ob or {}).get("asks") or []
                if bids and asks:
                    bid = self._safe_float(bids[0][0])
                    ask = self._safe_float(asks[0][0])
            except Exception:
                pass
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else price
        spread_pct = ((ask - bid) / mid * 100.0
                      if bid > 0 and ask > 0 and mid > 0 else None)
        funding_raw = self._ticker_field(
            ticker, "fundingRate", "funding_rate", "fundingRateLong")
        funding_rate_pct = None
        if funding_raw is not None:
            funding_rate_pct = self._safe_float(funding_raw) * 100.0

        meta = dict(entry_meta or {})
        spread_limit = self._f("SHADOW_MAX_SPREAD_PCT", 0.25)
        funding_limit = self._f("SHADOW_MAX_FUNDING_RATE_PCT", 0.05)
        reasons = []
        if spread_pct is None and not self.simulation:
            reasons.append("spread_unavailable")
        elif spread_pct is not None and spread_pct > spread_limit:
            reasons.append("wide_spread")
        if funding_rate_pct is not None and funding_rate_pct > funding_limit:
            reasons.append("long_pays_funding")

        return {
            "bot": self.BOT_NAME,
            "symbol": base,
            "full_symbol": full,
            "stage": "candidate_pre_order",
            "mode": "SIM" if self.simulation else "LIVE",
            "would_block": bool(reasons),
            "reasons": ",".join(reasons),
            "spread_pct": spread_pct,
            "funding_rate_pct": funding_rate_pct,
            "price": price,
            "margin": margin,
            "effective_leverage": eff_lev,
            "notional": margin * eff_lev,
            "timeframe": self._timeframe(),
            "closed_bars": meta.get("closed_bars"),
            "needed_bars": meta.get("needed_bars"),
            "trend_votes": meta.get("votes"),
            "realized_vol": meta.get("vol"),
            "vol_size_mult": meta.get("size_mult"),
            "spread_limit_pct": spread_limit,
            "funding_limit_pct": funding_limit,
        }

    def _trailing_audit_log_interval_sec(self) -> float:
        try:
            interval = float(self.C("TRAILING_AUDIT_LOG_INTERVAL_SEC", 300) or 300)
        except (TypeError, ValueError, OverflowError):
            interval = 300.0
        return max(30.0, interval) if math.isfinite(interval) else 300.0

    def _trailing_audit_signature(self, audit: dict) -> tuple:
        """Fields whose changes should be logged immediately.

        Price and move fields are still persisted to state; this signature only
        throttles high-volume structured telemetry.
        """
        return (
            bool(audit.get("trailing_enabled")),
            bool(audit.get("trailing_armed")),
            bool(audit.get("post_partial_trailing_active")),
            round(self._safe_float(audit.get("trailing_activation_pct")), 4),
            round(self._safe_float(audit.get("trailing_distance_pct")), 4),
            round(self._safe_float(audit.get("trailing_base_distance_pct")), 4),
        )

    def _should_log_trailing_audit(self, base: str, audit: dict) -> bool:
        cache = getattr(self, "_trailing_audit_log_cache", None)
        if not isinstance(cache, dict):
            cache = {}
            self._trailing_audit_log_cache = cache
        sig = self._trailing_audit_signature(audit)
        now = time.monotonic()
        last_sig, last_ts = cache.get(base, (None, 0.0))
        if sig != last_sig or now - last_ts >= self._trailing_audit_log_interval_sec():
            cache[base] = (sig, now)
            return True
        return False

    #  Universe 
    def _is_in_cooldown(self, base: str) -> bool:
        try:
            from trading.cooldown_utils import check_in_cooldown
            return check_in_cooldown(getattr(self, "cool", {}) or {}, base)
        except Exception:
            return False

    def _set_stop_cooldown(self, base: str, reason: str, profit_usdt: float,
                           log_event=None) -> None:
        try:
            from trading.cooldown_utils import should_cooldown_after_exit, set_cooldown
            if not should_cooldown_after_exit(reason, profit_usdt):
                return
            minutes = int(float(self.C("COOLDOWN_AFTER_SL", 120) or 0))
            if minutes <= 0:
                return
            with self._cooldown_lock:
                persisted = set_cooldown(
                    self.cool, base, minutes, self.COOLDOWN_FILE)
            if log_event:
                if persisted:
                    log_event(f"[{self.BOT_NAME}] {base}: cooldown "
                              f"{minutes}min after {reason}", "WAIT")
                else:
                    log_event(f"[{self.BOT_NAME}] {base}: cooldown active in "
                              f"memory but not persisted after {reason}",
                              "WARN")
        except Exception as e:
            self._log_error(f"trend cooldown set {base}", e)

    def _post_partial_trailing(self, base_distance: float, d: dict) -> tuple[float, bool]:
        amount = abs(self._safe_float(d.get("amount"), 0.0))
        if "original_amount" in d:
            original = abs(self._safe_float(d.get("original_amount"), 0.0))
        else:
            original = amount
        if not d.get("partial_sold") or amount <= 0 or original <= 0 or amount >= original:
            return base_distance, False
        configured = self._f("POST_PARTIAL_TRAILING_DISTANCE", 1.0)
        if configured <= 0:
            return base_distance, False
        return max(0.1, min(base_distance, configured)), True

    def _handle_exit_recovery_gate(self, base: str, d: dict) -> bool:
        """Handle recovery/conflict state before any FUTREND exit action."""
        if d.get("accounting_already_booked"):
            TrendFuturesBot._cleanup_accounted_close_state(self, base, d)
            return True
        if d.get("accounting_pending"):
            try:
                if self._record_offline_close(base, d):
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
                    TrendFuturesBot._cleanup_accounted_close_state(
                        self, base, booked)
            except Exception as exc:
                self._log_error(f"trend pending accounting retry {base}", exc)
            return True
        if d.get("claim_conflict"):
            try:
                from core.logger import log_event
                warned = getattr(self, "_claim_conflict_warned", set())
                if base not in warned:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: registry claim conflict - "
                        f"monitor skipped fail-closed; run claim/state repair",
                        "ERROR",
                    )
                    warned.add(base)
                    self._claim_conflict_warned = warned
            except Exception:
                pass
            return True
        self._retry_pending_partial_accounting(base, d)
        d_live = self.state.get(base) or d
        return bool(d_live.get("accounting_pending_partials"))

    def _maybe_blacklist_bad_symbol(self, base: str, reason: str,
                                    profit_usdt: float, move_pct: float,
                                    log_event=None) -> None:
        profit = self._finite_float_or_none(profit_usdt)
        if profit is None or profit >= 0:
            return
        if str(self.C("BAD_SYMBOL_FILTER", True)).strip().lower() not in (
                "1", "true", "yes", "on"):
            return
        try:
            from core.database import add_to_blacklist, get_recent_trades
            r = str(reason or "").lower()
            single_stop_pct = self._f("SINGLE_STOP_MIN_LOSS_PCT", 5.0)
            single_stop_hours = self._i("SINGLE_STOP_BLACKLIST_HOURS", 4)
            move = self._finite_float_or_none(move_pct)
            if (move is not None and "stop-loss" in r
                    and move <= -abs(single_stop_pct)
                    and single_stop_hours > 0):
                add_to_blacklist(
                    base, self.BOT_NAME, profit, hours=single_stop_hours,
                    reason=f"futrend single stop {move:.2f}%",
                    incremental=True,
                )
                if log_event:
                    log_event(f"[{self.BOT_NAME}] {base}: bad-symbol pause "
                              f"{single_stop_hours}h after {move:.2f}% stop",
                              "WARN")
                return

            loss_count_min = max(1, self._i("BAD_SYMBOL_LOSS_COUNT", 2))
            lookback_days = max(1, self._i("BAD_SYMBOL_LOOKBACK_DAYS", 1))
            total_loss_min = max(0.0, self._f("BAD_SYMBOL_MIN_TOTAL_LOSS_USDT", 6.0))
            hours = self._i("BAD_SYMBOL_BLACKLIST_HOURS", 24)
            if hours <= 0:
                return
            recent = get_recent_trades(self.BOT_NAME, limit=40, days=lookback_days)
            losses = []
            for t in recent:
                if str(t.get("symbol", "")).upper() != base.upper():
                    continue
                pnl = self._finite_float_or_none(t.get("profit_usdt"))
                if pnl is not None and pnl < 0:
                    losses.append(pnl)
            total_loss = abs(sum(losses))
            if len(losses) >= loss_count_min or total_loss >= total_loss_min:
                add_to_blacklist(
                    base, self.BOT_NAME, total_loss or abs(profit),
                    hours=hours,
                    reason=(f"futrend repeated damage: {len(losses)} losses, "
                            f"-{total_loss:.2f} USDT/{lookback_days}d"),
                    incremental=False,
                )
                if log_event:
                    log_event(f"[{self.BOT_NAME}] {base}: bad-symbol pause "
                              f"{hours}h ({len(losses)} losses, -{total_loss:.2f} "
                              f"USDT)", "WARN")
        except Exception as e:
            self._log_error(f"trend bad-symbol filter {base}", e)

    def _build_universe(self) -> Dict[str, str]:
        """{base: full_symbol} for the top-N liquid crypto perps, excluding
        non-crypto perps, delisted/blacklisted coins, and coins another bot
        already holds (coexistence)."""
        from core.logger import log_event
        from core.database import is_claimed_by_other, is_blacklisted
        try:
            n = self._i("TREND_UNIVERSE_SIZE", 30)
        except (TypeError, ValueError):
            n = 30
        min_vol = self._f("MIN_VOLUME", 10_000_000.0)
        try:
            from bot_utils.api_budget import try_consume_api_call
            if not try_consume_api_call("futrend_fetch_tickers"):
                log_event(f"[{self.BOT_NAME}] universe scan skipped "
                          f"(API budget exhausted)", "WARN")
                return {}
        except Exception:
            pass
        try:
            tickers = self.ex.fetch_tickers()
        except Exception as e:
            log_event(f"[{self.BOT_NAME}] ticker fetch failed: {e}", "WARN")
            return {}
        cands: List[Tuple[float, str, str]] = []
        for sym, t in tickers.items():
            if not sym.endswith(":USDT"):
                continue
            base = sym.split("/")[0].upper()
            if not _is_crypto_base(base):
                continue
            try:
                mkt = (getattr(self.ex, "markets", {}) or {}).get(sym) or {}
                if mkt.get("active") is False:
                    continue
            except Exception:
                pass
            try:
                if is_blacklisted(base, self.BOT_NAME):
                    continue
            except Exception:
                pass
            if self._is_in_cooldown(base):
                continue
            qv = t.get("quoteVolume") or 0
            try:
                qv = float(qv)
            except (TypeError, ValueError):
                qv = 0.0
            if qv < min_vol:
                continue
            if is_claimed_by_other(sym, self.BOT_NAME, is_futures=True):
                continue
            cands.append((qv, sym, base))
        cands.sort(reverse=True)
        return {base: sym for _qv, sym, base in cands[:n]}

    def _fetch_closes(self, full_symbol: str, need: int) -> Optional[List[float]]:
        from bot_utils.network_retry import with_network_retry
        tf = self._timeframe()
        try:
            from bot_utils.api_budget import try_consume_api_call
            if not try_consume_api_call("futrend_fetch_ohlcv"):
                return None
        except Exception:
            pass
        try:
            bars = with_network_retry(
                operation=lambda: self.ex.fetch_ohlcv(full_symbol, tf, limit=need + 2),
                action_label=f"ohlcv {full_symbol}",
                max_attempts=2, base_delay=0.5,
                shutdown_event=self._shutdown_event,
            )
        except Exception:
            return None
        closes = [float(b[4]) for b in (bars or []) if b and b[4]]
        # Drop the still-FORMING last candle so the signal acts on CLOSED bars
        # only  matches the validated backtest and avoids intra-candle flip-flop.
        if len(closes) >= 2:
            closes = closes[:-1]
        return closes if len(closes) >= 2 else None

    def _warn_short_history(self, base: str, got: int, need: int,
                            held: bool = False) -> None:
        from core.logger import log_event, log_struct
        seen = getattr(self, "_short_hist_warned", None)
        if seen is None:
            seen = self._short_hist_warned = set()
        if base in seen:
            return
        seen.add(base)
        action = "monitoring held position" if held else "symbol skipped"
        log_event(f"[{self.BOT_NAME}] {base}: insufficient {self._timeframe()} "
                  f"history ({got}/{need} closed bars); {action}", "INFO")
        try:
            log_struct("futrend_symbol_quality", bot=self.BOT_NAME, symbol=base,
                       timeframe=self._timeframe(), closed_bars=got,
                       needed_bars=need, held=held,
                       status="insufficient_history")
        except Exception:
            pass

    #  Trend engine (reuses the 'Scan' thread) 
    def _scan_loop(self):
        from core.logger import log_event
        iv = self._check_interval_sec()
        log_event(f"Trend-Futures engine started (tf={self._timeframe()}, "
                  f"check every {iv//60}min, long-only)", "INFO")
        last_check = 0.0
        while not self._shutdown_event.is_set():
            try:
                now = time.time()
                if now - last_check >= iv:
                    last_check = now
                    self._trend_tick()
            except Exception as e:
                log_event(f"Trend tick error: {e}", "WARN")
                self._log_error("trend tick", e)
            if self._shutdown_event.wait(timeout=min(iv, 30)):
                return

    def _trend_tick(self) -> None:
        from core.logger import log_event, log_struct
        from trading.risk_manager import is_bot_paused

        p = params_from_cfg(self.C)
        need = self._bars_needed(p)
        held = dict(self.state.get_all())                       # {base: state}
        max_open = self._i("MAX_OPEN_TRADES", 6)
        slots_available = max(0, max_open - len(held))
        universe = self._build_universe() if slots_available > 0 else {}

        # Evaluate the signal for the union of universe + currently held coins
        # (held coins must always get an exit check, even if they dropped out of
        # the universe or were adopted by reconcile).
        bases = set(universe) | set(held)
        signals: Dict[str, bool] = {}
        vols: Dict[str, float] = {}
        signal_meta: Dict[str, dict] = {}
        lookback = self._i("TREND_VOL_TARGET_LOOKBACK", 30)
        signal_failures = getattr(self, "_signal_failures", None)
        if signal_failures is None:
            signal_failures = self._signal_failures = {}
        stale_limit = max(1, self._i("TREND_EXIT_STALE_LIMIT", 3))
        for base in bases:
            if not _is_crypto_base(base):
                if base in held:
                    seen = getattr(self, "_noncrypto_held_warned", None)
                    if seen is None:
                        seen = self._noncrypto_held_warned = set()
                    if base not in seen:
                        seen.add(base)
                        log_event(
                            f"[{self.BOT_NAME}] {base}: non-crypto/stock "
                            f"state row ignored by trend engine; monitor "
                            f"safety only", "WARN")
                continue
            full = universe.get(base) or f"{base}/USDT:USDT"
            closes = self._fetch_closes(full, need)
            if closes is None:
                if base in held:
                    n = int(signal_failures.get(base, 0)) + 1
                    signal_failures[base] = n
                    if n >= stale_limit:
                        signals[base] = False
                        log_event(f"[{self.BOT_NAME}] {base}: trend data stale "
                                  f"({n}/{stale_limit})  defensive exit", "WARN")
                continue
            if not has_full_history(closes, p):
                self._warn_short_history(base, len(closes), need,
                                         held=(base in held))
                if base in held:
                    n = int(signal_failures.get(base, 0)) + 1
                    signal_failures[base] = n
                    if n >= stale_limit:
                        signals[base] = False
                        log_event(f"[{self.BOT_NAME}] {base}: insufficient trend "
                                  f"history while held ({n}/{stale_limit})  "
                                  f"defensive exit", "WARN")
                continue
            in_trend, votes, _ = is_in_trend(closes, p, currently_held=(base in held))
            signals[base] = in_trend
            signal_failures.pop(base, None)
            _v = realized_vol(closes, lookback)
            signal_meta[base] = {
                "closed_bars": len(closes),
                "needed_bars": need,
                "votes": votes,
                "signal": bool(in_trend),
            }
            if _v:
                vols[base] = _v
                signal_meta[base]["vol"] = _v

        #  EXITS: close held coins whose trend turned off 
        for base, d in held.items():
            if base in signals and not signals[base]:
                if self._handle_exit_recovery_gate(base, d):
                    continue
                self._close_position(base, d, reason="Trend-Exit")

        #  ENTRIES: open new trends (respect pause / safe-mode / slots) 
        if self.safe_mode is not None and self.safe_mode.is_active():
            return
        try:
            paused, why = is_bot_paused(
                self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
        except Exception as exc:
            log_event(f"[{self.BOT_NAME}] risk gate unavailable "
                      f"({type(exc).__name__})  entries skipped", "WARN")
            log_struct("trend_tick", bot=self.BOT_NAME,
                       universe=len(universe), held=self.state.count(),
                       in_trend=sum(1 for v in signals.values() if v),
                       risk_gate="unavailable")
            return
        if paused:
            log_event(f"[{self.BOT_NAME}] paused: {why}", "WAIT")
            return

        vt_on = str(self.C("TREND_VOL_TARGET", 0)).strip().lower() in (
            "1", "true", "yes", "on")
        med = basket_median_vol(list(vols.values())) if vt_on else None
        if vt_on and med is None:
            seen = getattr(self, "_vol_target_warned", False)
            if not seen:
                self._vol_target_warned = True
                log_event(f"[{self.BOT_NAME}] vol-targeting unavailable "
                          f"(insufficient volatility history); using flat "
                          f"position size", "INFO")

        default_new_limit = max_open if self.simulation else 1
        max_new = max(0, self._i("MAX_NEW_TRADES_PER_TICK", default_new_limit))
        if max_new <= 0:
            log_event(f"[{self.BOT_NAME}] entries disabled "
                      f"(MAX_NEW_TRADES_PER_TICK=0)", "WAIT")
            log_struct("trend_tick", bot=self.BOT_NAME,
                       universe=len(universe), held=self.state.count(),
                       in_trend=sum(1 for v in signals.values() if v))
            return
        opened_this_tick = 0
        for base, full in universe.items():
            if self._shutdown_event.is_set():
                return
            if self._is_in_cooldown(base):
                continue
            try:
                from core.database import is_blacklisted
                if is_blacklisted(base, self.BOT_NAME):
                    continue
            except Exception:
                pass
            if self.state.count() >= max_open:
                break
            if opened_this_tick >= max_new:
                log_event(
                    f"[{self.BOT_NAME}] entry ramp limit reached "
                    f"({opened_this_tick}/{max_new})  remaining trend "
                    f"signals deferred to next scan", "WAIT")
                break
            if self.state.has(base):
                continue
            if not signals.get(base):
                continue
            mult = vol_target_multiplier(vols.get(base), med) if vt_on else 1.0
            entry_meta = dict(signal_meta.get(base, {}))
            entry_meta["size_mult"] = mult
            entry_meta["vol_target_on"] = vt_on
            self._open_position(base, full, size_mult=mult,
                                 entry_meta=entry_meta)
            if self.state.has(base):
                opened_this_tick += 1

        log_struct("trend_tick", bot=self.BOT_NAME,
                   universe=len(universe), held=self.state.count(),
                   in_trend=sum(1 for v in signals.values() if v))

    #  Leverage helper (fractional effective  integer exchange cap) 
    def _effective_leverage(self) -> Tuple[float, int]:
        eff = self._f("LEVERAGE", 1.0)
        eff = max(1.0, min(6.0, eff))  # bot hard range 16
        return eff, int(math.ceil(eff))

    #  Open one leveraged LONG 
    def _open_position(self, base: str, full: str, size_mult: float = 1.0,
                       entry_meta: Optional[dict] = None) -> None:
        from core.logger import log_event, log_struct, send_telegram
        from core.database import (is_claimed_by_other, claim_symbol_for_entry,
                                   remove_open_position)
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from bot_utils import (create_order_with_retry,
                               extract_or_estimate_futures_fee,
                               filled_margin_usdt,
                               futures_contract_size,
                               get_maintenance_margin_rate)
        from config.exchange_config import (must_set_leverage, LeverageNotSetError,
                                            safe_set_margin_mode, safe_amount_to_precision,
                                            entry_params)

        # Race re-check: another bot may have claimed it since the scan.
        if is_claimed_by_other(full, self.BOT_NAME, is_futures=True):
            return

        eff_lev, lev_cap = self._effective_leverage()
        margin = self._f("POSITION_SIZE", 20.0) * max(0.0, size_mult)
        margin_cap = self._f("POSITION_SIZE_MAX", 0.0)
        if margin_cap > 0:
            margin = min(margin, margin_cap)
        if margin <= 0:
            return

        # Entry price from the ticker (cross-the-ask realism left to the live fill)
        try:
            tk = self.ticker_cache.get(self.ex, full, timeout=5.0)
            price = self._safe_float(tk.get("last"), 0.0)
            if price <= 0:
                price = self._safe_float(tk.get("close"), 0.0)
        except Exception:
            return
        if not math.isfinite(price) or price <= 0:
            return

        notional = margin * eff_lev
        shadow = self._entry_shadow_snapshot(
            base, full, tk, price, margin, eff_lev, entry_meta)
        try:
            log_struct("futrend_entry_shadow", **shadow)
        except Exception:
            pass
        if not self.simulation and shadow.get("would_block"):
            log_event(
                f"[{self.BOT_NAME}] {base}: entry blocked by live shadow "
                f"filter ({shadow.get('reasons')})", "WAIT")
            return
        if not self.simulation:
            try:
                from bot_utils.balance import safe_fetch_balance_usdt
                available = safe_fetch_balance_usdt(
                    self.ex, error_logger=self._log_error)
            except Exception as exc:
                self._log_error(f"{base} balance precheck", exc)
                available = None
            if available is None:
                log_event(
                    f"[{self.BOT_NAME}] {base}: entry skipped - free USDT "
                    f"balance unavailable", "WARN")
                return
            required = margin * 1.05
            if required > available:
                log_event(
                    f"[{self.BOT_NAME}] {base}: entry skipped - need "
                    f"{required:.2f} USDT free margin, available "
                    f"{available:.2f}", "WARN")
                return
        try:
            mm = get_maintenance_margin_rate(self.ex, full)
        except Exception:
            mm = 0.01
        cs = self._entry_contract_size(full, futures_contract_size)
        if not math.isfinite(cs) or cs <= 0:
            log_event(f"[{self.BOT_NAME}] {base}: invalid contract size - skip",
                      "WARN")
            return
        contracts = (notional / price) / max(cs, 1e-9)
        if not math.isfinite(contracts) or contracts <= 0:
            return
        fill = price
        fees = 0.0
        amount = contracts
        provisional = False
        margin_mode = str(self.C("MARGIN_MODE", "isolated") or "isolated").lower()

        if not self.simulation:
            min_contracts = 0.0
            try:
                _lim = ((getattr(self.ex, "markets", {}) or {}).get(full, {})
                        .get("limits") or {})
                _min = (_lim.get("amount") or {}).get("min")
                min_contracts = self._safe_float(_min, 0.0)
                if min_contracts > 0.0 and contracts < min_contracts:
                    log_event(f"[{self.BOT_NAME}] {base}: {contracts:g} < min "
                              f"{min_contracts:g} - skip (notional too small)",
                              "INFO")
                    return
                _cmin = (_lim.get("cost") or {}).get("min")
                min_cost = self._safe_float(_cmin, 0.0)
                if min_cost > 0.0 and notional < min_cost:
                    log_event(f"[{self.BOT_NAME}] {base}: notional {notional:.2f} < "
                              f"exchange min-cost {min_cost:.2f} - skip", "INFO")
                    return
            except Exception:
                pass
            try:
                raw_contracts = safe_amount_to_precision(self.ex, full, contracts)
                contracts = 0.0 if isinstance(raw_contracts, bool) else float(raw_contracts)
            except Exception:
                raw_contracts = contracts
                contracts = 0.0
            if not math.isfinite(contracts) or contracts <= 0:
                log_event(f"[{self.BOT_NAME}] {base}: invalid contracts "
                          f"{raw_contracts!r} after precision - skip", "WARN")
                return
            if min_contracts > 0.0 and contracts < min_contracts:
                log_event(f"[{self.BOT_NAME}] {base}: precision amount "
                          f"{contracts:g} < min {min_contracts:g} - skip",
                          "INFO")
                return
            try:
                must_set_leverage(self.ex, lev_cap, full, direction="LONG",
                                  margin_mode=margin_mode)
            except LeverageNotSetError as e:
                log_event(f"[{self.BOT_NAME}] {base}: set_leverage failed ({e}) "
                          f" skip", "WARN")
                return
            safe_set_margin_mode(self.ex, margin_mode, full, leverage=lev_cap,
                                 direction="LONG")
            import hashlib as _h
            _cid = (f"{self.BUY_PREFIX}-{base}-"
                    + _h.sha256(f"{self.BOT_NAME}:{base}:{int(time.time()//30)}"
                                .encode()).hexdigest()[:10])
            params = entry_params(position_side="long", margin_mode=margin_mode,
                                  leverage=lev_cap, client_order_id=_cid)
            if not claim_symbol_for_entry(self.BOT_NAME, full, "LONG"):
                log_event(f"[{self.BOT_NAME}] {base}: claimed by another bot "
                          f" skip", "WAIT")
                return
            if not self._record_open(
                base, fill, margin, eff_lev, contracts, 0.0,
                provisional=True, lev_cap=lev_cap, mm_rate=mm,
                margin_mode=margin_mode,
                entry_inflight=True,
            ):
                log_event(
                    f"[{self.BOT_NAME}] {base}: state write failed before "
                    f"LIVE entry - aborting open",
                    "ERROR",
                )
                self._cleanup_untracked_entry_state(
                    base, "state write failed before entry")
                return
            try:
                order = create_order_with_retry(
                    self.ex, full, "buy", contracts, params=params,
                    shutdown_event=self._shutdown_event,
                    action_label=f"trend open {base}",
                    log_event=log_event, log_struct=log_struct)
            except Exception as e:
                log_event(f"[{self.BOT_NAME}] {base}: open failed ({e})", "WARN")
                self._log_error(f"trend open {base}", e)
                # Orphan-prevention: create_order can RAISE after the order
                # actually landed (lost response). Recover via clientOrderId.
                _landed = False
                try:
                    from bot_utils.futures_order import _find_order_by_client_id
                    landed = _find_order_by_client_id(self.ex, full, _cid,
                                                      log_event=log_event)
                    landed_amount = self._safe_float(
                        (landed or {}).get("filled"), 0.0)
                    if landed and landed_amount > 0:
                        amount = landed_amount
                        landed_fill = price
                        for _k in ("average", "price"):
                            _fv = self._safe_float(landed.get(_k), 0.0)
                            if _fv > 0:
                                landed_fill = _fv
                                break
                        actual_margin, _ = filled_margin_usdt(
                            amount, cs, landed_fill, eff_lev, margin)
                        tracked = self._record_open(
                            base, landed_fill, actual_margin,
                            eff_lev, amount,
                            0.0, provisional=False,
                            lev_cap=lev_cap, mm_rate=mm,
                            entry_shadow=shadow,
                            margin_mode=margin_mode,
                        )
                        if not tracked:
                            self._rollback_untracked_live_entry(
                                base, full, amount, eff_lev, margin_mode,
                                "landed order after open error",
                            )
                            return
                        _landed = True
                        log_event(f"[{self.BOT_NAME}]  {base}: landed despite "
                                  f"error  tracked and monitoring enabled", "WARN")
                except Exception:
                    pass
                if not _landed:
                    removed_state = False
                    try:
                        removed_state = self.state.remove(base)
                    except Exception:
                        pass
                    if removed_state:
                        remove_open_position(self.BOT_NAME, base)
                return
            amount, fill, positions_unavailable, verified_source = (
                self._verify_entry_fill(base, full, order, contracts, fill)
            )
            if amount <= 0 and not positions_unavailable:
                log_event(
                    f"[{self.BOT_NAME}] {base}: order returned no fill and "
                    f"no exchange position was found - aborting state write",
                    "WARN")
                try:
                    removed_state = self.state.remove(base)
                except Exception:
                    removed_state = False
                if removed_state:
                    remove_open_position(self.BOT_NAME, base)
                return
            provisional = False
            if amount <= 0:
                amount = contracts
                provisional = True
                log_event(
                    f"[{self.BOT_NAME}] {base}: entry fill not yet verified "
                    f"(positions unavailable) - tracking provisionally", "WARN")
            elif verified_source == "position":
                log_event(f"[{self.BOT_NAME}] {base}: entry amount verified "
                          f"from exchange position ({amount:g} contracts)",
                          "INFO")
            try:
                fees = extract_or_estimate_futures_fee(
                    self.ex, order, full, fill, amount=amount, contract_size=cs)
            except Exception:
                fees = 0.0
        else:
            from bot_utils.fee_math import taker_fee_rate
            fees = notional * taker_fee_rate(self.ex, full, 0.0006)

        actual_margin, margin_from_fill = filled_margin_usdt(
            amount, cs, fill, eff_lev, margin)
        try:
            if margin_from_fill and margin > 0:
                fill_ratio = actual_margin / max(float(margin), 1e-9)
                if fill_ratio < 0.90 or fill_ratio > 1.10:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: entry margin adjusted "
                        f"from fill {margin:.2f} -> {actual_margin:.2f} USDT "
                        f"(filled={amount:g}, contract_size={cs})",
                        "INFO")
        except Exception:
            pass

        tracked = self._record_open(
            base, fill, actual_margin, eff_lev, amount, fees,
            provisional=provisional, lev_cap=lev_cap, mm_rate=mm,
            entry_shadow=shadow, margin_mode=margin_mode,
        )
        if not tracked:
            log_event(
                f"[{self.BOT_NAME}] {base}: state write failed after LIVE "
                f"entry - attempting immediate rollback close",
                "ERROR",
            )
            self._rollback_untracked_live_entry(
                base, full, amount, eff_lev, margin_mode,
                "state write failed after entry",
            )
            return
        if not provisional:
            log_event(f"[{self.BOT_NAME}] OPEN LONG {base} @ {fill:.6f} "
                      f"({eff_lev:g}x, notional {actual_margin * eff_lev:.1f}, "
                      f"fee {fees:.4f})", "INFO")
            try:
                if not self.simulation:
                    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                        f" [{self.BOT_NAME}] LONG {base} @ {eff_lev:g}x\n"
                        f"Entry: {fill:.6f}  Margin: {actual_margin:.2f} "
                        f"(Notional {actual_margin * eff_lev:.2f})")
            except Exception:
                pass

    def _verify_entry_fill(self, base: str, full: str, order: dict,
                           requested_amount: float,
                           fallback_fill: float) -> tuple[float, float, bool, str]:
        """Return verified contracts/fill after a market entry.

        ``positions_unavailable=True`` means the exchange position endpoint could
        not be trusted; callers should keep a provisional state instead of
        creating either a phantom final position or an unmanaged live orphan.
        """
        amount = self._safe_float((order or {}).get("filled"), 0.0)
        fill = fallback_fill
        for key in ("average", "price"):
            value = (order or {}).get(key)
            fv = self._safe_float(value, 0.0)
            if fv > 0:
                fill = fv
                break
        if amount > 0:
            return amount, fill, False, "order"

        oid = (order or {}).get("id") or (order or {}).get("orderId")
        if oid:
            for attempt in range(2):
                time.sleep(0.4 * (1 + attempt))
                try:
                    refreshed = self.ex.fetch_order(str(oid), full) or {}
                except Exception:
                    continue
                rf = self._safe_float(refreshed.get("filled"), 0.0)
                if rf > 0:
                    for key in ("average", "price"):
                        fv = self._safe_float(refreshed.get(key), 0.0)
                        if fv > 0:
                            fill = fv
                            break
                    return rf, fill, False, "order_refresh"

        pos, unavailable = self._fetch_exchange_position(full)
        if pos:
            contracts = self._safe_float(pos.get("contracts") or pos.get("size"), 0.0)
            contracts = abs(contracts)
            for key in ("entryPrice", "entry_price"):
                fv = self._safe_float(pos.get(key), 0.0)
                if fv > 0:
                    fill = fv
                    break
            return contracts, fill, False, "position"
        return 0.0, fill, unavailable, "none"

    def _fetch_exchange_position(self, full: str) -> tuple[dict | None, bool]:
        from bot_utils import fetch_open_position
        return fetch_open_position(self.ex, full)

    def _cleanup_untracked_entry_state(self, base: str, reason: str) -> bool:
        from core.database import remove_open_position

        restore = {
            "provisional": True,
            "entry_inflight_until": 0.0,
            "entry_aborted": True,
            "entry_abort_reason": reason,
            "claim_release_pending": True,
        }
        try:
            removed = self.state.remove(base, restore)
        except Exception as exc:
            self._log_error(f"trend cleanup untracked state {base}", exc)
            removed = False
        if removed:
            return True
        try:
            self.state.update_many(base, restore)
        except Exception as exc:
            self._log_error(f"trend mark untracked cleanup pending {base}", exc)
        try:
            remove_open_position(self.BOT_NAME, base)
        except Exception as exc:
            self._log_error(f"trend release untracked claim {base}", exc)
        return False

    def _record_open(self, base, fill, margin, eff_lev, amount, fees,
                      provisional: bool = False, lev_cap=None,
                      mm_rate: float = 0.01,
                      entry_shadow: Optional[dict] = None,
                      margin_mode: str = "isolated",
                      entry_inflight: bool = False) -> bool:
        from core.logger import _date as _utc
        from bot_utils import calc_liquidation_price, distance_to_liquidation_pct
        # Liquidation uses the INTEGER leverage the exchange runs (ceil) + real
        # maintenance margin, so the stored liq distance matches the exchange's
        # actual liquidation, not the fractional eff_lev / default 1% mm.
        liq_lev = lev_cap if lev_cap else math.ceil(eff_lev)
        liq = calc_liquidation_price(fill, liq_lev, "LONG", mm_rate)
        row = {
            "position_type": "LONG", "buy": fill, "highest": fill,
            "last_price": fill, "buy_time": _utc(), "invested_usdt": margin,
            "margin_mode": margin_mode,
            "leverage": eff_lev, "liquidation_price": liq,
            "initial_liq_distance": distance_to_liquidation_pct(fill, liq, "LONG"),
            "amount": amount, "original_amount": amount, "funding_paid": 0.0,
            "initial_entry_fee": fees, "fees_paid": fees,
            "strategy": "trend", "provisional": provisional,
        }
        if provisional and entry_inflight:
            row["entry_inflight_until"] = time.time() + 120.0
        if entry_shadow:
            row.update({
                "entry_shadow_would_block": bool(entry_shadow.get("would_block")),
                "entry_shadow_reasons": entry_shadow.get("reasons", ""),
                "entry_spread_pct": entry_shadow.get("spread_pct"),
                "entry_funding_rate_pct": entry_shadow.get("funding_rate_pct"),
                "entry_closed_bars": entry_shadow.get("closed_bars"),
                "entry_needed_bars": entry_shadow.get("needed_bars"),
                "entry_trend_votes": entry_shadow.get("trend_votes"),
                "entry_realized_vol": entry_shadow.get("realized_vol"),
                "entry_vol_size_mult": entry_shadow.get("vol_size_mult"),
            })
        return self.state.add(base, row) is not False

    def _rollback_untracked_live_entry(
        self,
        base: str,
        full: str,
        amount: float,
        leverage: float,
        margin_mode: str,
        reason: str,
    ) -> bool:
        """Close a live entry when durable state could not be written."""
        from core.logger import log_event
        from bot_utils import create_order_with_retry, verify_position_closed
        from config.exchange_config import reduce_only_params, safe_amount_to_precision

        try:
            amt = TrendFuturesBot._precision_amount_or_none(
                safe_amount_to_precision(self.ex, full, amount)
            )
            if amt is None:
                log_event(
                    f"[{self.BOT_NAME}] {base}: cannot rollback untracked live "
                    f"entry ({reason}) - invalid precision amount",
                    "ERROR",
                )
                return False
        except Exception:
            amt = TrendFuturesBot._precision_amount_or_none(amount)
            if amt is None:
                log_event(
                    f"[{self.BOT_NAME}] {base}: cannot rollback untracked live "
                    f"entry ({reason}) - invalid raw amount",
                    "ERROR",
                )
                return False
        if amt <= 0:
            log_event(
                f"[{self.BOT_NAME}] {base}: cannot rollback untracked live "
                f"entry ({reason}) - invalid amount",
                "ERROR",
            )
            return False
        lev_int = max(1, int(math.ceil(float(leverage or 1.0))))
        try:
            create_order_with_retry(
                self.ex,
                full,
                "sell",
                amt,
                params=reduce_only_params(
                    position_side="long",
                    margin_mode=margin_mode,
                    leverage=lev_int,
                ),
                shutdown_event=self._shutdown_event,
                action_label=f"trend rollback untracked {base}",
                log_event=log_event,
            )
            closed, remaining = verify_position_closed(self.ex, full)
            if closed:
                self._cleanup_untracked_entry_state(base, reason)
                log_event(
                    f"[{self.BOT_NAME}] {base}: untracked live entry "
                    f"rollback verified flat ({reason})",
                    "WARN",
                )
                return True
            log_event(
                f"[{self.BOT_NAME}] {base}: rollback close not verified "
                f"(remaining {remaining:g}) - claim kept for reconcile",
                "ERROR",
            )
            return False
        except Exception as exc:
            self._log_error(f"trend rollback untracked {base}", exc)
            log_event(
                f"[{self.BOT_NAME}] {base}: rollback close failed after "
                f"state write failure - claim kept for reconcile: {exc}",
                "ERROR",
            )
            return False

    #  Close one position (reduce-only, verify-before-book) 
    def _close_position(self, base: str, d: dict, reason: str) -> None:
        from core.logger import log_event, send_telegram, _date as _utc
        from core.symbol_locks import close_lock
        from bot_utils.futures_math import calc_unrealized_pnl, price_move_pct
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        with close_lock(base, timeout=5.0, bot_name=self.BOT_NAME) as got:
            if not got:
                return
            d_live = self.state.get(base)
            if d_live is None:
                return
            d = d_live
            self._close_inner(base, d, reason, calc_unrealized_pnl, price_move_pct,
                              log_event, send_telegram, _utc,
                              TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    def _cleanup_accounted_close_state(self, base: str, d: dict,
                                       log_event=None) -> bool:
        """Remove FUTREND dashboard/local state after close accounting exists."""
        if log_event is None:
            from core.logger import log_event as _log_event
            log_event = _log_event
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
                or "Trend close"
            ),
        }
        try:
            remove_futures_state(
                base, self.BOT_NAME,
                mode_is_sim=getattr(self, "simulation", None))
        except Exception as exc:
            self._log_error(f"trend remove_futures_state accounted {base}", exc)
            try:
                keep = dict(restore)
                keep["futures_state_cleanup_pending"] = True
                self.state.update_many(base, keep)
            except Exception as state_exc:
                self._log_error(f"trend mark cleanup pending {base}", state_exc)
            log_event(
                f"[{self.BOT_NAME}] {base}: close already booked, but "
                f"futures_state cleanup failed; state kept for retry",
                "WARN",
            )
            return False

        ok = remove_with_restore_fields(self.state, base, restore)
        if not ok:
            log_event(
                f"[{self.BOT_NAME}] {base}: close already booked, but "
                f"claim/state cleanup failed; state kept for retry",
                "WARN",
            )
            return False
        return True

    def _close_inner(self, base, d, reason, calc_unrealized_pnl, price_move_pct,
                     log_event, send_telegram, _utc, tg_token, tg_chat) -> None:
        from core.database import save_trade_db
        from core.logger import log_struct
        full = f"{base}/USDT:USDT"

        sentinel = object()

        def _finite_float(value, default=0.0):
            if isinstance(value, bool):
                return default
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return default
            return parsed if math.isfinite(parsed) else default

        def _positive_float(value, default=0.0):
            parsed = _finite_float(value, default)
            return parsed if parsed is not None and parsed > 0 else default

        def _nonnegative_float(value, default=0.0):
            parsed = _finite_float(value, default)
            return parsed if parsed is not None and parsed >= 0 else default

        def _required_positive(key):
            return _positive_float(d.get(key), None)

        def _optional_positive(key, default):
            if d.get(key, sentinel) is sentinel:
                return default
            return _positive_float(d.get(key), None)

        def _finite_non_bool_number(value) -> bool:
            if isinstance(value, bool):
                return False
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return False
            return math.isfinite(parsed)

        def _pending_fragment_is_complete(p_amount, p_price, p_fee, state) -> bool:
            if p_amount + 1e-12 < amt or p_price <= 0:
                return False
            for key in (
                "pending_close_filled_amount",
                "pending_close_notional_sum",
                "pending_close_price",
                "pending_close_fee",
            ):
                raw = state.get(key)
                if raw is not None and not _finite_non_bool_number(raw):
                    return False
            return _finite_non_bool_number(p_fee)

        def _log_invalid_pending_fragment() -> None:
            log_event(
                f"[{self.BOT_NAME}] {base}: invalid pending close fragment - "
                f"state kept for reconcile/offline accounting",
                "WARN")

        if d.get("verified_flat_pending_accounting"):
            log_event(
                f"[{self.BOT_NAME}] {base}: position already verified flat; "
                f"waiting for reconcile/offline accounting",
                "WARN")
            return
        pos_type = d.get("position_type", "LONG")
        entry = _required_positive("buy")
        margin = _required_positive("invested_usdt")
        lev = _required_positive("leverage")
        amt = _required_positive("amount")
        if entry is None or margin is None or lev is None or amt is None:
            log_event(
                f"[{self.BOT_NAME}] {base}: close accounting skipped due to "
                f"invalid core state - state kept for review", "WARN")
            return
        close_fee = 0.0
        initial_entry_fee = _finite_float(
            d.get("initial_entry_fee", d.get("fees_paid", 0.0)), 0.0)
        original_amount = _nonnegative_float(
            d.get("original_amount", amt), 0.0)
        close_price = _optional_positive("last_price", entry)
        if close_price is None:
            log_event(
                f"[{self.BOT_NAME}] {base}: close accounting skipped due to "
                f"invalid close price - state kept for review", "WARN")
            return
        exch_oid = None
        try:
            from bot_utils import futures_contract_size
            cs = _positive_float(futures_contract_size(self.ex, full), 0.0)
        except Exception:
            cs = 1.0
        live_close_already_verified = False
        if not self.simulation and d.get("pending_close_price"):
            try:
                from bot_utils import verify_position_closed
                from bot_utils.close_fragments import pending_close_values
                _closed, _remaining = verify_position_closed(self.ex, full)
                if _closed:
                    _amt, _price, _fee, _oid = pending_close_values(d)
                    if not _pending_fragment_is_complete(_amt, _price, _fee, d):
                        _log_invalid_pending_fragment()
                        return
                    if _price > 0:
                        close_price = _price
                    close_fee = _fee
                    exch_oid = _oid or exch_oid
                    live_close_already_verified = True
            except Exception:
                pass

        if self.simulation:
            from bot_utils.fee_math import taker_fee_rate
            if cs > 0:
                close_fee += amt * cs * close_price * taker_fee_rate(
                    self.ex, f"{base}/USDT:USDT", 0.0006)
        elif amt > 0 and not live_close_already_verified:
            from bot_utils import (create_order_with_retry,
                                    extract_or_estimate_futures_fee,
                                    futures_contract_size, is_no_position_error,
                                    verify_position_closed)
            from config.exchange_config import (reduce_only_params,
                                                safe_amount_to_precision)
            lev_cap = max(1, int(math.ceil(lev)))
            try:
                amt = _nonnegative_float(
                    safe_amount_to_precision(self.ex, full, amt), 0.0)
            except Exception:
                pass
            if amt <= 0:
                log_event(f"[{self.BOT_NAME}] {base}: close amount rounded to 0 "
                          f"- keeping state for retry", "WARN")
                return
            margin_mode = str(self.C("MARGIN_MODE", "isolated") or "isolated").lower()
            try:
                close_side = "sell" if pos_type == "LONG" else "buy"
                close_pos_side = "long" if pos_type == "LONG" else "short"
                order = create_order_with_retry(
                    self.ex, full, close_side, amt,
                    params=reduce_only_params(position_side=close_pos_side,
                                              margin_mode=margin_mode,
                                              leverage=lev_cap),
                    shutdown_event=self._shutdown_event,
                    action_label=f"trend close {base}", log_event=log_event)
                try:
                    order_filled = _nonnegative_float(order.get("filled"), 0.0)
                except AttributeError:
                    order_filled = 0.0
            except Exception as e:
                if is_no_position_error(e):
                    try:
                        _closed, _remaining = verify_position_closed(self.ex, full)
                    except Exception as ve:
                        self._log_error(f"trend verify-close-after-error {base}", ve)
                        log_event(
                            f"[{self.BOT_NAME}]  {base}: close error looked flat "
                            f"but verification failed - kept for reconcile",
                            "WARN")
                        return
                    if _closed:
                        if not d.get("pending_close_order_id"):
                            try:
                                self.state.update_many(base, {
                                    "verified_flat_pending_accounting": True,
                                    "verified_flat_reason": reason,
                                    "verified_flat_at": _utc(),
                                })
                            except Exception as state_err:
                                self._log_error(
                                    f"trend mark verified-flat {base}",
                                    state_err)
                            log_event(
                                f"[{self.BOT_NAME}] {base}: position already "
                                f"flat on exchange ({str(e)[:80]}) - keeping "
                                f"state for reconcile/offline accounting",
                                "WARN")
                            return
                        log_event(
                            f"[{self.BOT_NAME}] {base}: position already flat "
                            f"on exchange ({str(e)[:80]}) after our close "
                            f"order - booking pending close",
                            "WARN")
                        try:
                            from bot_utils.close_fragments import pending_close_values
                            _amt, _price, _fee, _oid = pending_close_values(d)
                            if not _pending_fragment_is_complete(
                                _amt, _price, _fee, d
                            ):
                                _log_invalid_pending_fragment()
                                return
                            if _price > 0:
                                close_price = _price
                            close_fee = _fee
                            exch_oid = _oid or d.get("pending_close_order_id") or exch_oid
                        except Exception:
                            pending_price = _positive_float(
                                d.get("pending_close_price"), close_price)
                            if pending_price > 0:
                                close_price = pending_price
                            close_fee = _nonnegative_float(
                                d.get("pending_close_fee"), close_fee)
                            exch_oid = d.get("pending_close_order_id") or exch_oid
                        live_close_already_verified = True
                    else:
                        log_event(
                            f"[{self.BOT_NAME}]  {base}: close error but "
                            f"{_remaining:.6f} contracts remain - kept for retry",
                            "WARN")
                        return
                else:
                    log_event(f"[{self.BOT_NAME}]  {base}: close FAILED ({e})  "
                              f"KEPT for retry", "ERROR")
                    self._log_error(f"trend close {base}", e)
                    return
            if not live_close_already_verified:
                exch_oid = order.get("id") or order.get("orderId")
                try:
                    from bot_utils.futures_exits import _resolve_fill_price
                    close_price, _fill_src = _resolve_fill_price(
                        self.ex, full, order, close_price, log_event)
                except Exception:
                    for _k in ("average", "price"):
                        _v = order.get(_k)
                        if _v:
                            _fv = _positive_float(_v, 0.0)
                            if _fv > 0:
                                close_price = _fv
                                break
            # VERIFY before booking  keep state on a partial/unverifiable close.
            if not live_close_already_verified:
                try:
                    _closed, _remaining = verify_position_closed(self.ex, full)
                except Exception as e:
                    if order_filled > 0 and close_price > 0:
                        try:
                            from bot_utils.close_fragments import add_close_fragment_update
                            frag_fee = extract_or_estimate_futures_fee(
                                self.ex, order, full, close_price,
                                amount=order_filled, contract_size=cs)
                            self.state.update_many(base, add_close_fragment_update(
                                d, amount=order_filled, price=close_price,
                                fee=frag_fee, order_id=exch_oid))
                        except Exception:
                            pass
                    self._log_error(f"trend verify-close {base}", e)
                    log_event(f"[{self.BOT_NAME}]  {base}: close unverified  keep, "
                              f"retry", "WARN")
                    return
                if not _closed:
                    try:
                        from bot_utils.close_fragments import (
                            add_close_fragment_update, pending_close_values)
                        prev_amount, _px, _fee, _oid = pending_close_values(d)
                        total_filled = max(
                            0.0, amt - _nonnegative_float(_remaining, 0.0))
                        fragment = max(0.0, total_filled - prev_amount)
                        if fragment > 0 and close_price > 0:
                            frag_fee = extract_or_estimate_futures_fee(
                                self.ex, order, full, close_price,
                                amount=fragment, contract_size=cs)
                            self.state.update_many(base, add_close_fragment_update(
                                d, amount=fragment, price=close_price,
                                fee=frag_fee, order_id=exch_oid))
                    except Exception:
                        pass
                    log_event(f"[{self.BOT_NAME}]  {base}: close incomplete "
                              f"(remaining {_remaining:.6f})  keep, retry "
                              f"(partial fill accounted pending)", "WARN")
                    return
                try:
                    from bot_utils.close_fragments import (
                        add_close_fragment_update, pending_close_values)
                    prev_amount, _px, _fee, _oid = pending_close_values(d)
                    fragment = max(0.0, amt - prev_amount)
                    if fragment > 0 and close_price > 0:
                        frag_fee = extract_or_estimate_futures_fee(
                            self.ex, order, full, close_price,
                            amount=fragment, contract_size=cs)
                        pending_view = dict(d)
                        pending_view.update(add_close_fragment_update(
                            d, amount=fragment, price=close_price,
                            fee=frag_fee, order_id=exch_oid))
                        _amt, _price, _fee, _oid = pending_close_values(pending_view)
                        validation_state = pending_view
                    else:
                        _amt, _price, _fee, _oid = pending_close_values(d)
                        validation_state = d
                    if not _pending_fragment_is_complete(
                        _amt, _price, _fee, validation_state
                    ):
                        _log_invalid_pending_fragment()
                        return
                    if _amt > 0 and _price > 0:
                        close_price = _price
                        close_fee = _fee
                        exch_oid = _oid or exch_oid
                except Exception:
                    try:
                        close_fee += extract_or_estimate_futures_fee(
                            self.ex, order, full, close_price, amount=amt,
                            contract_size=cs)
                    except Exception:
                        pass

        #  Book realized PnL 
        profit_usdt = 0.0
        if entry > 0 and close_price > 0 and margin > 0:
            pnl, _ = calc_unrealized_pnl(entry, close_price, margin, lev, pos_type)
            from bot_utils import safe_remaining_funding, safe_proportional_fee
            partial_sold = bool(d.get("partial_sold"))
            funding = _finite_float(d.get("funding_paid"), 0.0)
            funding_booked = _finite_float(
                d.get("funding_booked_on_partials"), 0.0)
            if not self.simulation:
                try:
                    from bot_utils.futures_funding import fetch_or_estimate_funding
                    notional = margin * lev if margin > 0 else 0.0
                    if partial_sold and amt > 0 and original_amount > 0:
                        remaining_ratio = amt / original_amount
                        if 0 < remaining_ratio < 1:
                            notional = notional / remaining_ratio
                    fresh_funding = fetch_or_estimate_funding(
                        self.ex, full, d.get("buy_time"),
                        notional_usdt=notional, pos_type=pos_type,
                        fallback_state_value=funding,
                    )
                    fresh_funding = _finite_float(fresh_funding, None)
                    if fresh_funding is not None:
                        funding = fresh_funding
                except Exception:
                    pass
            if partial_sold and original_amount > 0:
                funding = safe_remaining_funding(
                    funding, amt, original_amount, partial_sold=True,
                    booked_on_partials=funding_booked,
                )
            entry_fee = safe_proportional_fee(initial_entry_fee, amt,
                                              original_amount,
                                              partial_sold=partial_sold)
            profit_usdt = round(pnl - entry_fee - close_fee - funding, 4)
            move = price_move_pct(entry, close_price, pos_type)
            mfe_pct = _finite_float(d.get("max_profit_pct"), move)
            mae_pct = _finite_float(d.get("min_profit_pct"), move)
            giveback_pct = max(0.0, mfe_pct - move)
            try:
                log_struct("futrend_close_audit", bot=self.BOT_NAME, symbol=base,
                           mode="SIM" if self.simulation else "LIVE",
                           reason=reason, entry_price=entry,
                           close_price=close_price, move_pct=move,
                           profit_usdt=profit_usdt, mfe_pct=mfe_pct,
                           mae_pct=mae_pct, giveback_pct=giveback_pct,
                           max_profit_usdt=d.get("max_profit_usdt"),
                           min_profit_usdt=d.get("min_profit_usdt"),
                           trailing_armed=d.get("trailing_armed"),
                           trailing_peak_move_pct=d.get("trailing_peak_move_pct"),
                           trailing_stop_price=d.get("trailing_stop_price"),
                           entry_shadow_would_block=d.get("entry_shadow_would_block"),
                           entry_shadow_reasons=d.get("entry_shadow_reasons"))
            except Exception:
                pass
            sell_time = _utc()
            accounting_ok = False
            try:
                accounting_mode_is_sim = d.get(
                    "accounting_pending_mode_is_sim", self.simulation)
                accounting_ok = bool(save_trade_db(
                    bot_name=self.BOT_NAME, mode_is_sim=accounting_mode_is_sim, symbol=base, buy_price=entry,
                    sell_price=close_price, buy_time=d.get("buy_time", ""),
                    sell_time=sell_time, profit_pct=move, profit_usdt=profit_usdt,
                    invested_usdt=margin, reason=f"Trend {reason}",
                    is_futures=True, position_type=pos_type, leverage=lev,
                    funding_paid=funding, fees_usdt=entry_fee + close_fee,
                    exchange_order_id=exch_oid,
                    mfe_pct=mfe_pct, mae_pct=mae_pct,
                    giveback_pct=giveback_pct))
                if not accounting_ok:
                    raise RuntimeError("save_trade_db returned False")
            except Exception as e:
                self._log_error(f"trend save_trade {base}", e)
                log_event(
                    f"[{self.BOT_NAME}] {base}: DB accounting failed after "
                    f"verified close ({e}) - state kept for recovery", "WARN")
                try:
                    self.state.update_many(base, {
                        "accounting_pending": True,
                        "accounting_pending_reason": f"Trend {reason}",
                        "accounting_pending_sell_price": close_price,
                        "accounting_pending_sell_time": sell_time,
                        "accounting_pending_profit_pct": move,
                        "accounting_pending_profit_usdt": profit_usdt,
                        "accounting_pending_mode_is_sim": self.simulation,
                        "accounting_pending_fees_usdt": entry_fee + close_fee,
                        "accounting_pending_funding_paid": funding,
                        "accounting_pending_exchange_order_id": exch_oid,
                        "accounting_pending_mfe_pct": mfe_pct,
                        "accounting_pending_mae_pct": mae_pct,
                        "accounting_pending_giveback_pct": giveback_pct,
                    })
                except Exception as state_err:
                    self._log_error(f"trend mark accounting_pending {base}",
                                    state_err)
                return
        else:
            log_event(
                f"[{self.BOT_NAME}] {base}: close accounting skipped due to "
                f"invalid state - state kept for review", "WARN")
            return

        cleanup_row = dict(d)
        cleanup_row.update({
            "accounting_already_booked": True,
            "accounting_booked_sell_time": sell_time,
            "accounting_booked_exchange_order_id": exch_oid,
            "accounting_booked_reason": f"Trend {reason}",
        })
        TrendFuturesBot._cleanup_accounted_close_state(
            self, base, cleanup_row, log_event=log_event)
        self._set_stop_cooldown(base, reason, profit_usdt, log_event=log_event)
        self._maybe_blacklist_bad_symbol(base, reason, profit_usdt, move,
                                         log_event=log_event)
        log_event(f"[{self.BOT_NAME}] CLOSE {pos_type} {base} @ {close_price:.6f} "
                  f"({reason}, PnL {profit_usdt:+.2f})", "INFO")
        try:
            if not self.simulation:
                send_telegram(tg_token, tg_chat,
                    f"{'' if profit_usdt >= 0 else ''} [{self.BOT_NAME}] CLOSE "
                    f"{pos_type} {base}\nPnL {profit_usdt:+.2f} USDT  {reason}")
        except Exception:
            pass

    #  Fast safety monitor (reuses the 'Monitor' thread) 
    def _monitor_loop(self):
        from core.logger import log_event
        interval = self._i("MONITOR_INTERVAL", self.DEFAULT_MONITOR_INTERVAL)
        log_event(f"Trend-Futures safety monitor started "
                  f"(interval {interval}s)", "INFO")
        last_ks = 0.0
        while not self._shutdown_event.is_set():
            try:
                trades = dict(self.state.get_all())
                now = time.time()
                if trades:
                    if now - last_ks >= 60:
                        last_ks = now
                        try:
                            self._check_killswitch(trades)
                        except Exception as e:
                            self._log_error("trend killswitch", e)
                    self._maybe_persist_funding_for_all(trades, now)
                    for base, d in trades.items():
                        if self._shutdown_event.is_set():
                            break
                        if d.get("provisional"):
                            if not self._heal_provisional_position(base, d):
                                continue
                            d = self.state.get(base) or d
                        try:
                            self._check_safety(base, d)
                        except Exception as e:
                            self._log_error(f"trend safety {base}", e)
            except Exception as e:
                log_event(f"Trend monitor error: {e}", "WARN")
                self._log_error("trend monitor", e)
            if self._shutdown_event.wait(timeout=interval):
                return

    def _heal_provisional_position(self, base: str, d: dict) -> bool:
        """Resolve a provisional entry state after restart/API uncertainty."""
        from core.logger import log_event
        from core.database import remove_open_position
        from bot_utils import calc_liquidation_price, distance_to_liquidation_pct

        full = f"{base}/USDT:USDT"
        inflight_active = self._safe_float(
            d.get("entry_inflight_until"), 0.0) > time.time()
        pos, unavailable = self._fetch_exchange_position(full)
        if pos is None:
            if inflight_active:
                return False
            if unavailable:
                return False
            log_event(f"[{self.BOT_NAME}] {base}: provisional state had no "
                      f"exchange position - removing stale claim", "WARN")
            removed_state = False
            try:
                removed_state = self.state.remove(base)
            except Exception:
                pass
            if removed_state:
                remove_open_position(self.BOT_NAME, base)
            return False

        contracts = abs(self._safe_float(pos.get("contracts") or pos.get("size"), 0.0))
        entry = 0.0
        for key in ("entryPrice", "entry_price"):
            entry = self._safe_float(pos.get(key), 0.0)
            if entry > 0:
                break
        if entry <= 0:
            entry = self._safe_float(d.get("buy"), 0.0)
        if contracts <= 0 or entry <= 0:
            return False

        lev = self._safe_float(d.get("leverage"), 1.0) or 1.0
        mm = self._safe_float(d.get("maintenance_margin_rate"), 0.01) or 0.01
        try:
            liq = self._safe_float(pos.get("liquidationPrice"), 0.0)
        except Exception:
            liq = 0.0
        if liq <= 0:
            liq = calc_liquidation_price(entry, math.ceil(lev), "LONG", mm)
        fields = {
            "buy": entry,
            "highest": max(self._safe_float(d.get("highest"), entry), entry),
            "last_price": entry,
            "amount": contracts,
            "original_amount": contracts,
            "provisional": False,
            "liquidation_price": liq,
            "initial_liq_distance": distance_to_liquidation_pct(entry, liq, "LONG"),
        }
        self.state.update_many(base, fields)
        log_event(f"[{self.BOT_NAME}] {base}: provisional entry verified "
                  f"from exchange position ({contracts:g} contracts)", "WARN")
        return True

    def _check_safety(self, base: str, d: dict) -> None:
        from core.database import upsert_futures_state
        from bot_utils import (price_move_pct, calc_unrealized_pnl,
                               calc_liquidation_price, distance_to_liquidation_pct,
                               liq_buffer_consumed_pct, get_exchange_liq_price,
                               get_maintenance_margin_rate)
        if d.get("provisional"):
            return
        if self._handle_exit_recovery_gate(base, d):
            return
        d = self.state.get(base) or d
        full = f"{base}/USDT:USDT"
        try:
            tk = self.ticker_cache.get(self.ex, full, timeout=5.0, critical=True)
            curr = self._safe_float(tk.get("last"), 0.0)
            if curr <= 0:
                curr = self._safe_float(tk.get("close"), 0.0)
        except Exception:
            curr = 0.0
        if not math.isfinite(curr) or curr <= 0:
            try:
                curr = self._safe_float(self._fallback_mark_price(full), 0.0)
            except Exception:
                curr = 0.0
        if not math.isfinite(curr) or curr <= 0:
            try:
                self._note_price_unavailable(base)
            except Exception:
                pass
            return
        try:
            self._clear_price_unavailable(base)
        except Exception:
            pass
        entry = self._safe_float(d.get("buy"), 0.0)
        if entry <= 0:
            return
        lev = self._safe_float(d.get("leverage"), 1.0)
        if lev <= 0:
            lev = 1.0
        amount = abs(self._safe_float(d.get("amount"), 0.0))
        margin = self._safe_float(d.get("invested_usdt"), 0.0)
        if margin <= 0 and amount > 0:
            try:
                from bot_utils import futures_contract_size
                cs = self._safe_float(futures_contract_size(self.ex, full), 1.0)
                if cs <= 0:
                    cs = 1.0
            except Exception:
                cs = 1.0
            margin = (amount * cs * entry) / lev
        if margin <= 0:
            return
        if self._safe_float(d.get("invested_usdt"), 0.0) != margin:
            d["invested_usdt"] = margin
            try:
                self.state.update(base, "invested_usdt", margin)
            except Exception as e:
                self._log_error(f"trend margin repair {base}", e)
        pos_type = d.get("position_type", "LONG")
        d["last_price"] = curr
        try:
            self.state.update(base, "last_price", curr)
        except Exception as e:
            self._log_error(f"trend last_price update {base}", e)

        move = price_move_pct(entry, curr, pos_type)
        pnl_now, _pct_margin_now = calc_unrealized_pnl(
            entry, curr, margin, lev, pos_type)
        telemetry = {}
        try:
            prev_max_pct = self._safe_float(d.get("max_profit_pct"), move)
        except (TypeError, ValueError):
            prev_max_pct = move
        try:
            prev_min_pct = self._safe_float(d.get("min_profit_pct"), move)
        except (TypeError, ValueError):
            prev_min_pct = move
        try:
            prev_max_usdt = self._safe_float(d.get("max_profit_usdt"), pnl_now)
        except (TypeError, ValueError):
            prev_max_usdt = pnl_now
        try:
            prev_min_usdt = self._safe_float(d.get("min_profit_usdt"), pnl_now)
        except (TypeError, ValueError):
            prev_min_usdt = pnl_now
        if "max_profit_pct" not in d or move > prev_max_pct:
            telemetry["max_profit_pct"] = move
        if "min_profit_pct" not in d or move < prev_min_pct:
            telemetry["min_profit_pct"] = move
        if "max_profit_usdt" not in d or pnl_now > prev_max_usdt:
            telemetry["max_profit_usdt"] = pnl_now
        if "min_profit_usdt" not in d or pnl_now < prev_min_usdt:
            telemetry["min_profit_usdt"] = pnl_now
        if telemetry:
            d.update(telemetry)
            try:
                self.state.update_many(base, telemetry)
            except Exception as e:
                self._log_error(f"trend telemetry update {base}", e)

        highest = self._safe_float(d.get("highest"), entry)
        if pos_type == "LONG" and curr > highest:
            highest = curr
            d["highest"] = highest
            try:
                self.state.update(base, "highest", highest)
            except Exception as e:
                self._log_error(f"trend highest update {base}", e)
        lowest = self._safe_float(d.get("lowest"), entry)
        if pos_type == "SHORT" and curr < lowest:
            lowest = curr
            d["lowest"] = lowest
            try:
                self.state.update(base, "lowest", lowest)
            except Exception as e:
                self._log_error(f"trend lowest update {base}", e)

        # 1) Hard price stop (INITIAL_STOP_LOSS, fast  between candle checks).
        extreme = highest if pos_type == "LONG" else lowest
        high_move = price_move_pct(entry, extreme, pos_type)

        hard_stop = self._f("INITIAL_STOP_LOSS", -12.0)
        if hard_stop < 0 and move <= hard_stop:
            self._close_position(base, d, reason="Stop-Loss")
            return
        if self._failed_entry_stop_hit(d, move, high_move):
            self._close_position(base, d, reason="Failed-Entry Stop")
            return

        # 2) Liquidation-buffer guard (last resort).
        mm = get_maintenance_margin_rate(self.ex, full)
        liq = self._safe_float(
            d.get("liquidation_price"),
            calc_liquidation_price(entry, lev, pos_type, mm),
        )
        if not self.simulation:
            now_ts = time.time()
            if now_ts >= self._safe_float(d.get("liq_next_check_at"), 0.0):
                interval = self._safe_float(
                    getattr(self, "LIQ_REFRESH_INTERVAL_SEC", 90.0), 90.0)
                upd = {"liq_next_check_at": now_ts + interval}
                try:
                    exch_liq = get_exchange_liq_price(self.ex, full)
                except Exception:
                    exch_liq = 0.0
                if exch_liq > 0:
                    liq = exch_liq
                    upd["liquidation_price"] = exch_liq
                try:
                    self.state.update_many(base, upd)
                except Exception:
                    pass
        cur_dist = distance_to_liquidation_pct(curr, liq, pos_type)
        init_dist = self._safe_float(d.get("initial_liq_distance"), 0.0)
        if init_dist <= 0:
            init_dist = max(1.0, 100.0 / max(1.0, lev))
        consumed = liq_buffer_consumed_pct(init_dist, cur_dist)
        if consumed >= 100.0 - self._f("LIQ_SAFETY_PCT", 20.0):
            self._close_position(base, d, reason="Liq protection")
            return
        if self._pre_activation_giveback_stop_hit(d, move, high_move):
            self._close_position(base, d, reason="Pre-Activation Giveback Stop")
            return

        # 3) Profit harvesting: partial TP, breakeven and trailing run on the
        # fast monitor, not on the slow candle-based trend tick.
        try:
            from bot_utils import trailing_stop_hit, breakeven_stop_hit
            from bot_utils.futures_math import fee_buffered_breakeven
            be_trigger = self._f("BREAKEVEN_TRIGGER", 0.0)
            if be_trigger > 0 and not d.get("be_active") and move >= be_trigger:
                be_price = fee_buffered_breakeven(entry, pos_type, fee_buffer=0.003)
                d["be_active"] = True
                d["be_price"] = be_price
                try:
                    self.state.update_many(base, {"be_active": True, "be_price": be_price})
                except Exception as e:
                    self._log_error(f"trend breakeven audit update {base}", e)

            activation = self._f("ACTIVATION_PROFIT", 0.0)
            partial_pct = self._f("PARTIAL_SELL_PCT", 0.0)
            if (activation > 0 and partial_pct > 0 and not d.get("partial_sold")
                    and move >= activation):
                pnl, pct = calc_unrealized_pnl(entry, curr, margin, lev, pos_type)
                from core.symbol_locks import close_lock
                with close_lock(base, bot_name=self.BOT_NAME) as got:
                    if not got:
                        return
                    try:
                        if not self.state.has(base):
                            return
                        live = self.state.get(base) or d
                        if live.get("partial_sold"):
                            return
                    except Exception:
                        live = d
                    partial_done = self._execute_partial_tp(
                        base, live, curr, move, pct, entry, liq, margin, lev,
                        pos_type)
                if partial_done:
                    return

            try:
                trailing = float(self.C("TRAILING_DISTANCE", 0.0))
            except (TypeError, ValueError, OverflowError):
                trailing = float("nan")
            trailing_invalid = (not math.isfinite(trailing)) or trailing < 0
            if trailing > 0 or trailing_invalid:
                if trailing_invalid:
                    effective_trailing = trailing
                    audit_trailing = 0.0
                    audit_base_trailing = 0.0
                    post_partial_trailing = False
                else:
                    effective_trailing, post_partial_trailing = (
                        self._post_partial_trailing(trailing, d)
                    )
                    audit_trailing = effective_trailing
                    audit_base_trailing = trailing
                trailing_armed = bool(d.get("break_even")) or (
                    activation > 0 and high_move >= activation)
                trail_stop = (extreme * (1.0 - effective_trailing / 100.0)
                              if pos_type == "LONG"
                              else extreme * (1.0 + effective_trailing / 100.0))
                if not math.isfinite(trail_stop):
                    trail_stop = 0.0
                trail_audit = {
                    "trailing_enabled": True,
                    "trailing_armed": trailing_armed,
                    "trailing_activation_pct": activation,
                    "trailing_distance_pct": audit_trailing,
                    "trailing_base_distance_pct": audit_base_trailing,
                    "post_partial_trailing_active": post_partial_trailing,
                    "trailing_peak_move_pct": high_move,
                    "trailing_current_move_pct": move,
                    "trailing_giveback_pct": max(0.0, high_move - move),
                    "trailing_stop_price": trail_stop,
                }
                d.update(trail_audit)
                try:
                    self.state.update_many(base, trail_audit)
                except Exception as e:
                    self._log_error(f"trend trailing audit update {base}", e)
                if self._should_log_trailing_audit(base, trail_audit):
                    try:
                        from core.logger import log_struct
                        log_struct("futrend_trailing_audit",
                                   bot=self.BOT_NAME, symbol=base,
                                   mode="SIM" if self.simulation else "LIVE",
                                   **trail_audit)
                    except Exception:
                        pass
                if (trailing_armed and
                        trailing_stop_hit(curr, extreme, effective_trailing, pos_type)):
                    self._close_position(base, d, reason="Trailing Stop")
                    return
            if d.get("be_active") and breakeven_stop_hit(
                    curr, self._safe_float(d.get("be_price"), entry), pos_type):
                self._close_position(base, d, reason="Breakeven-Stop")
                return
            if d.get("break_even") and breakeven_stop_hit(curr, entry, pos_type):
                self._close_position(base, d, reason="Break-Even Stop")
                return
        except Exception as e:
            self._log_error(f"trend profit safety {base}", e)

        # 4) Dashboard live-state (best-effort).
        try:
            upsert_futures_state(
                symbol=base, bot_name=self.BOT_NAME, mode_is_sim=self.simulation, position_type=pos_type,
                entry_price=entry, current_price=curr, leverage=lev,
                margin_usdt=margin, position_size_usdt=margin * lev,
                unrealized_pnl=pnl_now, unrealized_pct=_pct_margin_now,
                liquidation_price=liq,
                liq_distance_pct=cur_dist, funding_paid=d.get("funding_paid", 0.0),
                opened_at=d.get("buy_time", ""))
        except Exception:
            pass
