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
        try:
            out = float(value)
            return out if math.isfinite(out) else default
        except (TypeError, ValueError):
            return default

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

    def _audit_changed(self, d: dict, updates: dict) -> bool:
        for key, val in updates.items():
            old = d.get(key)
            if isinstance(val, float):
                if old is None:
                    return True
                tolerance = max(abs(val) * 0.001, 1e-8) if key.endswith("_price") else 0.05
                if abs(self._safe_float(old) - val) >= tolerance:
                    return True
            elif old != val:
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
                set_cooldown(self.cool, base, minutes, self.COOLDOWN_FILE)
            if log_event:
                log_event(f"[{self.BOT_NAME}] {base}: cooldown {minutes}min after "
                          f"{reason}", "WAIT")
        except Exception as e:
            self._log_error(f"trend cooldown set {base}", e)

    def _post_partial_trailing(self, base_distance: float, d: dict) -> tuple[float, bool]:
        try:
            amount = abs(float(d.get("amount", 0) or 0))
            original = abs(float(d.get("original_amount", amount) or amount))
        except (TypeError, ValueError):
            return base_distance, False
        if not d.get("partial_sold") or amount <= 0 or original <= 0 or amount >= original:
            return base_distance, False
        configured = self._f("POST_PARTIAL_TRAILING_DISTANCE", 1.0)
        if configured <= 0:
            return base_distance, False
        return max(0.1, min(base_distance, configured)), True

    def _maybe_blacklist_bad_symbol(self, base: str, reason: str,
                                    profit_usdt: float, move_pct: float,
                                    log_event=None) -> None:
        if profit_usdt >= 0:
            return
        if str(self.C("BAD_SYMBOL_FILTER", True)).strip().lower() not in (
                "1", "true", "yes", "on"):
            return
        try:
            from core.database import add_to_blacklist, get_recent_trades
            r = str(reason or "").lower()
            single_stop_pct = self._f("SINGLE_STOP_MIN_LOSS_PCT", 5.0)
            single_stop_hours = self._i("SINGLE_STOP_BLACKLIST_HOURS", 4)
            if "stop-loss" in r and move_pct <= -abs(single_stop_pct) and single_stop_hours > 0:
                add_to_blacklist(
                    base, self.BOT_NAME, profit_usdt, hours=single_stop_hours,
                    reason=f"futrend single stop {move_pct:.2f}%",
                    incremental=True,
                )
                if log_event:
                    log_event(f"[{self.BOT_NAME}] {base}: bad-symbol pause "
                              f"{single_stop_hours}h after {move_pct:.2f}% stop",
                              "WARN")
                return

            loss_count_min = max(1, self._i("BAD_SYMBOL_LOSS_COUNT", 2))
            lookback_days = max(1, self._i("BAD_SYMBOL_LOOKBACK_DAYS", 1))
            total_loss_min = max(0.0, self._f("BAD_SYMBOL_MIN_TOTAL_LOSS_USDT", 6.0))
            hours = self._i("BAD_SYMBOL_BLACKLIST_HOURS", 24)
            if hours <= 0:
                return
            recent = get_recent_trades(self.BOT_NAME, limit=40, days=lookback_days)
            losses = [
                float(t.get("profit_usdt", 0) or 0)
                for t in recent
                if str(t.get("symbol", "")).upper() == base.upper()
                and float(t.get("profit_usdt", 0) or 0) < 0
            ]
            total_loss = abs(sum(losses))
            if len(losses) >= loss_count_min or total_loss >= total_loss_min:
                add_to_blacklist(
                    base, self.BOT_NAME, total_loss or abs(profit_usdt),
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
        universe = self._build_universe()

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
                self._close_position(base, d, reason="Trend-Exit")

        #  ENTRIES: open new trends (respect pause / safe-mode / slots) 
        if self.safe_mode is not None and self.safe_mode.is_active():
            return
        try:
            paused, why = is_bot_paused(self.BOT_NAME, exchange=self.ex)
        except Exception:
            paused, why = False, ""
        if paused:
            log_event(f"[{self.BOT_NAME}] paused: {why}", "WAIT")
            return

        vt_on = str(self.C("TREND_VOL_TARGET", 0)).strip().lower() in (
            "1", "true", "yes", "on")
        med = basket_median_vol(list(vols.values())) if vt_on else None

        max_open = self._i("MAX_OPEN_TRADES", 6)
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
        from core.logger import log_event, log_struct, send_telegram, _date as _utc
        from core.database import (is_claimed_by_other, claim_symbol_for_entry,
                                   remove_open_position)
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from bot_utils import (create_order_with_retry,
                               extract_or_estimate_futures_fee,
                               futures_contract_size,
                               calc_liquidation_price,
                               distance_to_liquidation_pct,
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
            price = float(tk.get("last") or tk.get("close") or 0)
        except Exception:
            return
        if price <= 0:
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
        try:
            mm = (get_maintenance_margin_rate(self.ex, full)
                  if not self.simulation else 0.01)
        except Exception:
            mm = 0.01
        cs = futures_contract_size(self.ex, full)
        contracts = (notional / price) / max(cs, 1e-9)
        fill = price
        fees = 0.0
        amount = contracts
        exch_oid = None
        margin_mode = str(self.C("MARGIN_MODE", "isolated") or "isolated").lower()

        if not self.simulation:
            try:
                must_set_leverage(self.ex, lev_cap, full, direction="LONG",
                                  margin_mode=margin_mode)
            except LeverageNotSetError as e:
                log_event(f"[{self.BOT_NAME}] {base}: set_leverage failed ({e}) "
                          f" skip", "WARN")
                return
            safe_set_margin_mode(self.ex, margin_mode, full, leverage=lev_cap,
                                 direction="LONG")
            try:
                _lim = ((getattr(self.ex, "markets", {}) or {}).get(full, {})
                        .get("limits") or {})
                _min = (_lim.get("amount") or {}).get("min")
                if _min and contracts < float(_min):
                    log_event(f"[{self.BOT_NAME}] {base}: {contracts:g} < min "
                              f"{_min:g} - skip (notional too small)", "INFO")
                    return
                _cmin = (_lim.get("cost") or {}).get("min")
                if _cmin and notional < float(_cmin):
                    log_event(f"[{self.BOT_NAME}] {base}: notional {notional:.2f} < "
                              f"exchange min-cost {float(_cmin):.2f} - skip", "INFO")
                    return
            except Exception:
                pass
            contracts = float(safe_amount_to_precision(self.ex, full, contracts))
            if contracts <= 0:
                return
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
            self._record_open(base, fill, margin, eff_lev, contracts, 0.0,
                              provisional=True, lev_cap=lev_cap, mm_rate=mm,
                              margin_mode=margin_mode)
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
                    if landed and float(landed.get("filled") or 0) > 0:
                        amount = float(landed.get("filled") or 0) or contracts
                        self._record_open(base, price, margin, eff_lev, amount,
                                          0.0, provisional=False,
                                          lev_cap=lev_cap, mm_rate=mm,
                                          entry_shadow=shadow,
                                          margin_mode=margin_mode)
                        _landed = True
                        log_event(f"[{self.BOT_NAME}]  {base}: landed despite "
                                  f"error  tracked and monitoring enabled", "WARN")
                except Exception:
                    pass
                if not _landed:
                    try:
                        self.state.remove(base)
                    except Exception:
                        pass
                    remove_open_position(self.BOT_NAME, base)
                return
            exch_oid = order.get("id") or order.get("orderId")
            amount = float(order.get("filled") or 0) or contracts
            for _k in ("average", "price"):
                _v = order.get(_k)
                if _v:
                    try:
                        _fv = float(_v)
                        if _fv > 0:
                            fill = _fv
                            break
                    except (TypeError, ValueError):
                        pass
            try:
                fees = extract_or_estimate_futures_fee(
                    self.ex, order, full, fill, amount=amount, contract_size=cs)
            except Exception:
                fees = 0.0
        else:
            from bot_utils.fee_math import taker_fee_rate
            fees = notional * taker_fee_rate(self.ex, full, 0.0006)

        self._record_open(base, fill, margin, eff_lev, amount, fees,
                          lev_cap=lev_cap, mm_rate=mm,
                          entry_shadow=shadow, margin_mode=margin_mode)
        log_event(f"[{self.BOT_NAME}] OPEN LONG {base} @ {fill:.6f} "
                  f"({eff_lev:g}x, notional {notional:.1f}, fee {fees:.4f})", "INFO")
        try:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                f" [{self.BOT_NAME}] LONG {base} @ {eff_lev:g}x\n"
                f"Entry: {fill:.6f}  Margin: {margin:.2f} (Notional {notional:.2f})")
        except Exception:
            pass

    def _record_open(self, base, fill, margin, eff_lev, amount, fees,
                      provisional: bool = False, lev_cap=None,
                      mm_rate: float = 0.01,
                      entry_shadow: Optional[dict] = None,
                      margin_mode: str = "isolated") -> None:
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
        self.state.add(base, row)

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

    def _close_inner(self, base, d, reason, calc_unrealized_pnl, price_move_pct,
                     log_event, send_telegram, _utc, tg_token, tg_chat) -> None:
        from core.database import save_trade_db, remove_futures_state
        from core.logger import log_struct
        full = f"{base}/USDT:USDT"
        pos_type = d.get("position_type", "LONG")
        entry = float(d.get("buy", 0) or 0)
        margin = float(d.get("invested_usdt", 0) or 0)
        lev = float(d.get("leverage", 1) or 1)
        amt = abs(float(d.get("amount", 0) or 0))
        close_fee = 0.0
        initial_entry_fee = float(d.get("initial_entry_fee",
                                        d.get("fees_paid", 0.0)) or 0.0)
        original_amount = float(d.get("original_amount", amt) or amt)
        close_price = float(d.get("last_price", entry) or entry)
        exch_oid = None
        try:
            from bot_utils import futures_contract_size
            cs = futures_contract_size(self.ex, full)
        except Exception:
            cs = 1.0
        live_close_already_verified = False
        if not self.simulation and d.get("pending_close_price"):
            try:
                from bot_utils import verify_position_closed
                _closed, _remaining = verify_position_closed(self.ex, full)
                if _closed:
                    close_price = float(d.get("pending_close_price") or close_price)
                    close_fee = float(d.get("pending_close_fee") or close_fee)
                    exch_oid = d.get("pending_close_order_id") or exch_oid
                    live_close_already_verified = True
            except Exception:
                pass

        if self.simulation:
            from bot_utils.fee_math import taker_fee_rate
            close_fee += amt * cs * close_price * taker_fee_rate(
                self.ex, f"{base}/USDT:USDT", 0.0006)
        elif amt > 0 and not live_close_already_verified:
            from bot_utils import (create_order_with_retry,
                                    extract_or_estimate_futures_fee,
                                    futures_contract_size, verify_position_closed)
            from config.exchange_config import (reduce_only_params,
                                                safe_amount_to_precision)
            lev_cap = max(1, int(math.ceil(lev)))
            try:
                amt = float(safe_amount_to_precision(self.ex, full, amt))
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
            except Exception as e:
                log_event(f"[{self.BOT_NAME}]  {base}: close FAILED ({e})  "
                          f"KEPT for retry", "ERROR")
                self._log_error(f"trend close {base}", e)
                return
            exch_oid = order.get("id") or order.get("orderId")
            for _k in ("average", "price"):
                _v = order.get(_k)
                if _v:
                    try:
                        _fv = float(_v)
                        if _fv > 0:
                            close_price = _fv
                            break
                    except (TypeError, ValueError):
                        pass
            try:
                close_fee += extract_or_estimate_futures_fee(
                    self.ex, order, full, close_price, amount=amt, contract_size=cs)
            except Exception:
                pass
            try:
                self.state.update_many(base, {
                    "pending_close_price": close_price,
                    "pending_close_fee": close_fee,
                    "pending_close_order_id": exch_oid,
                })
            except Exception:
                pass
            # VERIFY before booking  keep state on a partial/unverifiable close.
            try:
                _closed, _remaining = verify_position_closed(self.ex, full)
            except Exception as e:
                self._log_error(f"trend verify-close {base}", e)
                log_event(f"[{self.BOT_NAME}]  {base}: close unverified  keep, "
                          f"retry", "WARN")
                return
            if not _closed:
                log_event(f"[{self.BOT_NAME}]  {base}: close incomplete "
                          f"(remaining {_remaining:.6f})  keep, retry (no "
                          f"double-count)", "WARN")
                return

        #  Book realized PnL 
        profit_usdt = 0.0
        if entry > 0 and close_price > 0 and margin > 0:
            pnl, _ = calc_unrealized_pnl(entry, close_price, margin, lev, pos_type)
            from bot_utils import safe_remaining_funding, safe_proportional_fee
            partial_sold = bool(d.get("partial_sold"))
            funding = float(d.get("funding_paid", 0.0) or 0.0)
            funding_booked = float(d.get("funding_booked_on_partials", 0.0) or 0.0)
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
                    if fresh_funding is not None:
                        funding = float(fresh_funding)
                except Exception:
                    pass
            if partial_sold:
                funding = safe_remaining_funding(
                    funding, amt, original_amount, partial_sold=True,
                    booked_on_partials=funding_booked,
                )
            entry_fee = safe_proportional_fee(initial_entry_fee, amt,
                                              original_amount,
                                              partial_sold=partial_sold)
            profit_usdt = round(pnl - entry_fee - close_fee - funding, 4)
            move = price_move_pct(entry, close_price, pos_type)
            mfe_pct = float(d.get("max_profit_pct", move) or move)
            mae_pct = float(d.get("min_profit_pct", move) or move)
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
                accounting_ok = bool(save_trade_db(
                    bot_name=self.BOT_NAME, symbol=base, buy_price=entry,
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

        try:
            remove_futures_state(base, self.BOT_NAME)
        except Exception:
            pass
        self._set_stop_cooldown(base, reason, profit_usdt, log_event=log_event)
        self._maybe_blacklist_bad_symbol(base, reason, profit_usdt, move,
                                         log_event=log_event)
        self.state.remove(base)
        log_event(f"[{self.BOT_NAME}] CLOSE {pos_type} {base} @ {close_price:.6f} "
                  f"({reason}, PnL {profit_usdt:+.2f})", "INFO")
        try:
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
                            continue
                        try:
                            self._check_safety(base, d)
                        except Exception as e:
                            self._log_error(f"trend safety {base}", e)
            except Exception as e:
                log_event(f"Trend monitor error: {e}", "WARN")
                self._log_error("trend monitor", e)
            if self._shutdown_event.wait(timeout=interval):
                return

    def _check_safety(self, base: str, d: dict) -> None:
        from core.database import upsert_futures_state
        from bot_utils import (price_move_pct, calc_unrealized_pnl,
                               calc_liquidation_price, distance_to_liquidation_pct,
                               liq_buffer_consumed_pct, get_exchange_liq_price,
                               get_maintenance_margin_rate)
        if d.get("provisional"):
            return
        full = f"{base}/USDT:USDT"
        try:
            tk = self.ticker_cache.get(self.ex, full, timeout=5.0, critical=True)
            curr = float(tk.get("last") or tk.get("close") or 0)
        except Exception:
            curr = 0.0
            try:
                curr = float(self._fallback_mark_price(full) or 0.0)
            except Exception:
                curr = 0.0
            if curr <= 0:
                try:
                    self._note_price_unavailable(base)
                except Exception:
                    pass
                return
        if curr <= 0:
            return
        entry = float(d.get("buy", 0) or 0)
        if entry <= 0:
            return
        lev = float(d.get("leverage", 1) or 1)
        margin = float(d.get("invested_usdt", 0) or 0)
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
            prev_max_pct = float(d.get("max_profit_pct", move))
        except (TypeError, ValueError):
            prev_max_pct = move
        try:
            prev_min_pct = float(d.get("min_profit_pct", move))
        except (TypeError, ValueError):
            prev_min_pct = move
        try:
            prev_max_usdt = float(d.get("max_profit_usdt", pnl_now))
        except (TypeError, ValueError):
            prev_max_usdt = pnl_now
        try:
            prev_min_usdt = float(d.get("min_profit_usdt", pnl_now))
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

        highest = float(d.get("highest", entry) or entry)
        if pos_type == "LONG" and curr > highest:
            highest = curr
            d["highest"] = highest
            try:
                self.state.update(base, "highest", highest)
            except Exception as e:
                self._log_error(f"trend highest update {base}", e)
        lowest = float(d.get("lowest", entry) or entry)
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
        liq = float(d.get("liquidation_price",
                          calc_liquidation_price(entry, lev, pos_type, mm)))
        if not self.simulation:
            exch_liq = get_exchange_liq_price(self.ex, full)
            if exch_liq > 0:
                liq = exch_liq
        cur_dist = distance_to_liquidation_pct(curr, liq, pos_type)
        init_dist = float(d.get("initial_liq_distance", 0) or 0)
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

            trailing = self._f("TRAILING_DISTANCE", 0.0)
            if trailing > 0:
                effective_trailing, post_partial_trailing = (
                    self._post_partial_trailing(trailing, d)
                )
                trailing_armed = bool(d.get("break_even")) or (
                    activation > 0 and high_move >= activation)
                trail_stop = (extreme * (1.0 - effective_trailing / 100.0)
                              if pos_type == "LONG"
                              else extreme * (1.0 + effective_trailing / 100.0))
                trail_audit = {
                    "trailing_enabled": True,
                    "trailing_armed": trailing_armed,
                    "trailing_activation_pct": activation,
                    "trailing_distance_pct": effective_trailing,
                    "trailing_base_distance_pct": trailing,
                    "post_partial_trailing_active": post_partial_trailing,
                    "trailing_peak_move_pct": high_move,
                    "trailing_current_move_pct": move,
                    "trailing_giveback_pct": max(0.0, high_move - move),
                    "trailing_stop_price": trail_stop,
                }
                if self._audit_changed(d, trail_audit):
                    d.update(trail_audit)
                    try:
                        self.state.update_many(base, trail_audit)
                    except Exception as e:
                        self._log_error(f"trend trailing audit update {base}", e)
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
                    curr, float(d.get("be_price", entry) or entry), pos_type):
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
                symbol=base, bot_name=self.BOT_NAME, position_type=pos_type,
                entry_price=entry, current_price=curr, leverage=lev,
                margin_usdt=margin, position_size_usdt=margin * lev,
                unrealized_pnl=pnl_now, unrealized_pct=_pct_margin_now,
                liquidation_price=liq,
                liq_distance_pct=cur_dist, funding_paid=d.get("funding_paid", 0.0),
                opened_at=d.get("buy_time", ""))
        except Exception:
            pass
