"""
core/cross_bot.py - Cross-sectional momentum bot (market-neutral, cross-margin).

A 4th bot that is FUNDAMENTALLY different from the per-coin scanners (Spot/
Futures): it rebalances a dollar-neutral PORTFOLIO - long the strongest K coins
/ short the weakest K by lookback return - every REBALANCE_HOURS, with an
own-momentum crash filter. Pure ranking lives in trading/xsec_signal.py.

Reuses ALL of FuturesBot's lifecycle (connect, TradeState, SafeMode, shutdown,
heartbeat, emergency-close, the coexistence-aware reconcile). We only override
the two trading loops:

  _scan_loop  -> REBALANCE loop  (every REBALANCE_HOURS, anchored)
  _monitor_loop -> CROSS monitor  (per-leg disaster stop, killswitch,
                                     liq-buffer, live-state for the UI)

Status: SIM-first. Live-capable (cross margin), but the strategy is regime-
dependent (net-negative over 365d in research) - keep in SIMULATION until it
proves out over weeks of paper trading.
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

from core.futures_bot import FuturesBot
from trading.xsec_signal import XSecParams, compute_target_book


#  Crypto-only universe filter 
# MEXC also lists NON-crypto USDT perps (oil, metals, indices, forex, stocks).
# CROSS trades CRYPTO ONLY - these are excluded from the ranking universe so the
# market-neutral book never longs/shorts e.g. USOIL/UKOIL. Exact bases only (no
# bare "OIL"/"GAS"/"GOLD" which collide with real crypto tickers). Extend via
# env CROSS_EXCLUDE_BASES="FOO,BAR".
from core.constants import NONCRYPTO_BASES, STOCK_TOKEN_BASES
_NONCRYPTO_BASES = set(NONCRYPTO_BASES)   # shared list; CROSS adds its env extension
_STOCK_TOKEN_BASES = set(STOCK_TOKEN_BASES)
try:
    import os as _os
    _NONCRYPTO_BASES |= {b.strip().upper()
                         for b in _os.getenv("CROSS_EXCLUDE_BASES", "").split(",")
                         if b.strip()}
except Exception:
    pass


def _is_crypto_base(base: str) -> bool:
    """True unless the base is a known non-crypto perp (oil/metal/index/forex)
    or a stock perp (MEXC names those *STOCK, e.g. SKHYNIXSTOCK)."""
    b = (base or "").upper()
    if b in _NONCRYPTO_BASES:
        return False
    if b in _STOCK_TOKEN_BASES:
        return False
    if "STOCK" in b:
        return False
    return True


class CrossBot(FuturesBot):
    BOT_NAME = "CROSS"
    BUY_PREFIX = "xmom"
    NEWS_MODULE_PATH = ""          # no LLM - overridden _news below

    #  No news/LLM: satisfy FuturesBot.run()'s `_ = self._news` check 
    @property
    def _news(self):
        return None

    def C(self, key: str, default=None):
        # The cross bot's position count is driven by XSEC_K (2xK total: K long +
        # K short), NOT MAX_OPEN_TRADES. Make the inherited heartbeat/banner
        # denominator track 2xK so "Open: 8/8" (not a misleading "8/12") for K=4.
        if key == "MAX_OPEN_TRADES":
            try:
                return 2 * int(float(super().C("XSEC_K", 6)))
            except (TypeError, ValueError):
                return 12
        return super().C(key, default)

    #  Cross-bot params (read live via self.C) 
    def _xsec_params(self) -> XSecParams:
        def _i(k, d):
            try:
                return int(float(self.C(k, d)))
            except (TypeError, ValueError):
                return d
        def _b(k, d):
            v = self.C(k, d)
            return str(v).strip().lower() not in ("0", "false", "no", "off") if v is not None else d
        return XSecParams(
            lookback_hours=_i("XSEC_LOOKBACK_HOURS", 24),
            k_per_side=_i("XSEC_K", 6),
            crash_filter=_b("CRASH_FILTER", True),
            crash_window=_i("CRASH_WINDOW", 4),
        )

    def _f(self, key, default):
        try:
            return float(self.C(key, default))
        except (TypeError, ValueError):
            return default

    # Cross-margin safety ceiling in code (UI caps at 3; a hand-edited config
    # must not push the shared cross-margin book to the global 25x clamp).
    def _leverage(self) -> float:
        return max(1.0, min(3.0, self._f("LEVERAGE", 1.0)))

    def _notional_from_state(self, d: dict) -> float:
        try:
            margin = float(d.get("invested_usdt", 0.0) or 0.0)
            lev = float(d.get("leverage", 1.0) or 1.0)
            notional = margin * max(lev, 0.0)
            return notional if notional > 0 else 0.0
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _is_active_leg(d: dict) -> bool:
        return not (
            d.get("provisional")
            or d.get("claim_conflict")
            or d.get("verified_flat_pending_accounting")
            or d.get("accounting_pending")
            or d.get("accounting_already_booked")
        )

    def _active_legs(self, rows: dict | None = None) -> dict:
        rows = self.state.get_all() if rows is None else rows
        return {base: d for base, d in rows.items()
                if CrossBot._is_active_leg(d)}

    def _safe_float(self, value, default: float = 0.0) -> float:
        try:
            out = float(value)
            return out if out == out else default
        except (TypeError, ValueError):
            return default

    def _fetch_exchange_position(self, full: str) -> tuple[dict | None, bool]:
        from bot_utils import fetch_open_position
        return fetch_open_position(self.ex, full)

    def _verify_entry_fill(self, full: str, order: dict,
                           fallback_fill: float) -> tuple[float, float, bool, str]:
        amount = self._safe_float((order or {}).get("filled"), 0.0)
        fill = fallback_fill
        for key in ("average", "price"):
            fv = self._safe_float((order or {}).get(key), 0.0)
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
            contracts = abs(self._safe_float(pos.get("contracts") or pos.get("size"), 0.0))
            for key in ("entryPrice", "entry_price"):
                fv = self._safe_float(pos.get(key), 0.0)
                if fv > 0:
                    fill = fv
                    break
            return contracts, fill, False, "position"
        return 0.0, fill, unavailable, "none"

    def _heal_provisional_leg(self, base: str, d: dict) -> bool:
        from core.logger import log_event
        from core.database import remove_open_position

        full = f"{base}/USDT:USDT"
        try:
            if float(d.get("entry_inflight_until") or 0.0) > time.time():
                return False
        except (TypeError, ValueError):
            pass
        pos, unavailable = self._fetch_exchange_position(full)
        if pos is None:
            if unavailable:
                return False
            log_event(f"[{self.BOT_NAME}] {base}: provisional leg had no "
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
        try:
            from bot_utils import futures_contract_size
            cs = futures_contract_size(self.ex, full)
        except Exception:
            cs = self._safe_float(d.get("contract_size"), 1.0) or 1.0

        self.state.update_many(base, {
            "buy": entry,
            "highest": max(self._safe_float(d.get("highest"), entry), entry),
            "last_price": entry,
            "amount": contracts,
            "original_amount": contracts,
            "contract_size": cs,
            "invested_usdt": (
                contracts * cs * entry
                / max(1.0, self._safe_float(d.get("leverage"), 1.0))
            ),
            "provisional": False,
        })
        log_event(f"[{self.BOT_NAME}] {base}: provisional leg verified "
                  f"from exchange position ({contracts:g} contracts)", "WARN")
        return True

    def _cross_sim_roundtrip_fee(self, full_symbol: str, notional: float) -> float:
        if notional <= 0:
            return 0.0
        try:
            from bot_utils.fee_math import taker_fee_rate
            rate = taker_fee_rate(self.ex, full_symbol, 0.0006)
        except Exception:
            rate = 0.0006
        return round(max(0.0, notional * rate * 2.0), 6)

    def _telegram_enabled(self) -> bool:
        return not bool(getattr(self, "simulation", True))

    def _clamp_cross_sim_costs(self, full_symbol: str, d: dict,
                               close_fee: float, funding: float,
                               log_event=None) -> tuple[float, float]:
        """Cross SIM stores amount in coins, while inherited futures helpers
        treat amount as contracts. Low-price coins can therefore inflate costs
        by contract_size. Keep paper costs tied to the actual leg notional."""
        if not self.simulation:
            return close_fee, funding
        notional = self._notional_from_state(d)
        if notional <= 0:
            return 0.0, 0.0
        expected_fee = self._cross_sim_roundtrip_fee(full_symbol, notional)
        fee_cap = max(expected_fee * 5.0, notional * 0.02)
        funding_cap = notional * 0.20
        try:
            fee = float(close_fee or 0.0)
        except (TypeError, ValueError):
            fee = 0.0
        try:
            fund = float(funding or 0.0)
        except (TypeError, ValueError):
            fund = 0.0
        if fee < 0 or fee > fee_cap:
            if log_event:
                log_event(f"[{self.BOT_NAME}] SIM cost clamp {full_symbol}: "
                          f"fee {fee:.4f} -> {expected_fee:.4f}", "WARN")
            fee = expected_fee
        if abs(fund) > funding_cap:
            if log_event:
                log_event(f"[{self.BOT_NAME}] SIM cost clamp {full_symbol}: "
                          f"funding {fund:.4f} -> 0.0000", "WARN")
            fund = 0.0
        return fee, fund

    def _maybe_persist_funding_for_all(self, trades: dict,
                                       now_epoch: float) -> None:
        if not self.simulation:
            return super()._maybe_persist_funding_for_all(trades, now_epoch)
        try:
            from bot_utils.futures_funding import estimate_funding_paid
        except Exception:
            return
        for base, d in (trades or {}).items():
            try:
                next_check = float(d.get("funding_next_check_at", 0) or 0)
                if now_epoch < next_check:
                    continue
                notional = self._notional_from_state(d)
                realized = estimate_funding_paid(
                    self.ex, f"{base}/USDT:USDT", d.get("buy_time", ""),
                    notional, d.get("position_type", "LONG"),
                )
                _fee, realized = self._clamp_cross_sim_costs(
                    f"{base}/USDT:USDT", d, 0.0, realized)
                update = {
                    "funding_next_check_at": (
                        now_epoch + self._FUNDING_REFRESH_INTERVAL_SEC
                    )
                }
                if realized != float(d.get("funding_paid", 0.0) or 0.0):
                    update["funding_paid"] = float(realized)
                self.state.update_many(base, update)
            except Exception as e:
                try:
                    self._log_error(f"cross sim funding-refresh {base}", e)
                except Exception:
                    pass

    def _funding_ok(self, full_symbol: str, max_pct: float) -> bool:
        """Keep the market-neutral book out of extreme-funding coins (which bleed
        funding on BOTH legs and wreck the thin edge). Unknown funding is not a
        valid new-entry signal; skip the coin and let the screener pick another."""
        if max_pct <= 0:
            return True
        try:
            from config.exchange_config import safe_fetch_funding_rate
            fr = safe_fetch_funding_rate(self.ex, full_symbol)
            rate = float((fr or {}).get("fundingRate") or 0.0)
            return abs(rate) * 100.0 <= max_pct
        except Exception:
            return False

    #  Rebalance cadence (anchored, not per-restart) 
    def _rebalance_interval_sec(self) -> int:
        try:
            return max(3600, int(float(self.C("XSEC_REBALANCE_HOURS", 72)) * 3600))
        except (TypeError, ValueError):
            return 72 * 3600

    def _due_for_rebalance(self) -> bool:
        """Anchored to a fixed epoch grid AND PERSISTED across restarts.

        The consumed slot is persisted (bot_params), so a relaunch inside the
        same slot RESUMES the existing book instead of re-opening one (which
        would stack a second, unbalanced book on top). A rebalance is due only
        when the slot actually advances; a fresh DB has no marker (-1) -> the
        first run establishes the book once.
        """
        iv = self._rebalance_interval_sec()
        slot = int(time.time()) // iv
        if self._last_rebalance_slot is None:
            try:
                from core.database import get_param
                self._last_rebalance_slot = int(
                    get_param(self.BOT_NAME, "REBALANCE_SLOT", -1))
            except Exception:
                self._last_rebalance_slot = -1
        # Rebalance when the slot advances OR when we currently hold NOTHING.
        # The empty-book case cannot accumulate (nothing to stack onto), so
        # (re)establishing a book after a restart - or after the crash-filter /
        # disaster-stops emptied it - is safe and expected. A restart with a
        # HELD book in the same slot still resumes WITHOUT re-rebalancing.
        try:
            empty = (len(CrossBot._active_legs(self)) == 0)
        except Exception:
            empty = False
        return slot != self._last_rebalance_slot or empty

    def _mark_rebalanced(self) -> None:
        """Persist the current slot as consumed - called only AFTER a rebalance
        actually applied a book, so an interrupted/failed rebalance retries on
        the next loop instead of being skipped until the next slot."""
        iv = self._rebalance_interval_sec()
        slot = int(time.time()) // iv
        self._last_rebalance_slot = slot
        try:
            from core.database import set_param
            set_param(self.BOT_NAME, "REBALANCE_SLOT", slot,
                      reason="cross rebalance applied")
        except Exception:
            pass

    def _should_topup(self) -> bool:
        """True when we hold a partial book (some legs, but UNDER target K/side)
        and the per-slot budget isn't spent. Independent of whether a rebalance
        ran THIS session - so a restart that adopted a partial book also fills."""
        if self.safe_mode is not None and self.safe_mode.is_active():
            return False
        try:
            from trading.risk_manager import is_bot_paused
            paused, _why = is_bot_paused(
                self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
            if paused:
                return False
        except Exception:
            # Opening missing legs is optional; if the risk layer is unavailable
            # the only safe choice is to leave the current book untouched.
            return False
        if self._topup_attempts >= self._topup_max:
            return False
        try:
            k = int(self._xsec_params().k_per_side)
        except Exception:
            return False
        active_count = len(CrossBot._active_legs(self))
        return 0 < active_count < 2 * k

    @staticmethod
    def _topup_counts(held_l, held_s, k, n_cand_l, n_cand_s):
        """How many legs to add per side to fill toward K/side without ever
        worsening neutrality: first close a one-sided gap (bring the lagging
        side up to the leading side), then add balanced pairs. Bounded by K and
        available candidates. Returns (add_long, add_short)."""
        cap_l = max(0, k - held_l)
        cap_s = max(0, k - held_s)
        cand_l = max(0, n_cand_l)
        cand_s = max(0, n_cand_s)
        catch_l = min(max(0, held_s - held_l), cap_l, cand_l)
        catch_s = min(max(0, held_l - held_s), cap_s, cand_s)
        pairs = max(0, min(cap_l - catch_l, cap_s - catch_s,
                           cand_l - catch_l, cand_s - catch_s))
        return catch_l + pairs, catch_s + pairs

    def _topup_tick(self) -> None:
        """Fill missing legs toward K/side from the CURRENT signal - closes a
        one-sided gap first, then adds balanced pairs. Only opens new, claimable
        legs; never closes held/adopted ones (no churn). Works on a
        restart-adopted book (no cached book needed)."""
        from core.logger import log_event
        from core.database import is_claimed_by_other
        from trading.risk_manager import is_bot_paused
        if self.safe_mode is not None and self.safe_mode.is_active():
            log_event(f"[{self.BOT_NAME}] SAFE_MODE - top-up skipped "
                      f"({self.safe_mode.reason()})", "WAIT")
            return
        try:
            paused, why = is_bot_paused(
                self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
        except Exception as exc:
            log_event(f"[{self.BOT_NAME}] top-up skipped - risk gate "
                      f"unavailable ({type(exc).__name__})", "WARN")
            return
        if paused:
            log_event(f"[{self.BOT_NAME}] paused: {why}", "WAIT")
            return
        params = self._xsec_params()
        k = int(params.k_per_side)
        prices, sym_map = self._fetch_universe_prices(params.lookback_hours)
        if not prices:
            log_event(f"[{self.BOT_NAME}] top-up: no universe data - skipped", "WARN")
            return
        book = compute_target_book(prices, self._recent_rebalance_returns, params)
        if getattr(book, "is_flat", False):
            return
        cur = CrossBot._active_legs(self)
        held_l = sum(1 for d in cur.values() if d.get("position_type") == "LONG")
        held_s = sum(1 for d in cur.values() if d.get("position_type") == "SHORT")

        def _cand(side_list):
            return [b for b in side_list
                    if not self.state.has(b) and sym_map.get(b)
                    and prices.get(b, [0])[-1] > 0
                    and not is_claimed_by_other(sym_map[b], self.BOT_NAME, is_futures=True)]
        cand_l = _cand(book.longs)
        cand_s = _cand(book.shorts)
        add_l, add_s = self._topup_counts(held_l, held_s, k,
                                          len(cand_l), len(cand_s))
        if add_l <= 0 and add_s <= 0:
            return
        lev = self._leverage()
        equity = self._equity()
        gross = equity * lev * max(0.0, min(1.0, book.exposure_mult))
        gross = min(gross, equity * max(0.0, self._f("MAX_GROSS_EXPOSURE_PCT", 100.0)) / 100.0)
        notional = (gross / 2.0) / k if k > 0 else 0.0
        if notional <= 0:
            return
        retained_gross = sum(self._notional_from_state(d)
                             for d in CrossBot._active_legs(self).values())
        new_count = add_l + add_s
        if new_count > 0:
            remaining_gross = max(0.0, gross - retained_gross)
            notional = min(notional, remaining_gross / new_count)
            if notional <= 0:
                log_event(f"[{self.BOT_NAME}] top-up skipped: retained gross "
                          f"{retained_gross:.1f} already reaches cap "
                          f"{gross:.1f}", "WAIT")
                return
        log_event(f"[{self.BOT_NAME}] top-up {self._topup_attempts}/{self._topup_max}: "
                  f"book {held_l}L/{held_s}S -> adding {add_l}L/{add_s}S", "SCAN")
        self._rebalance_in_progress = True
        try:
            for b in cand_l[:add_l]:
                self._open_leg(b, sym_map[b], "LONG", notional, prices[b][-1], lev)
            for b in cand_s[:add_s]:
                self._open_leg(b, sym_map[b], "SHORT", notional, prices[b][-1], lev)
        finally:
            self._rebalance_in_progress = False
            cur = CrossBot._active_legs(self)
            long_n = sum(1 for d in cur.values()
                         if d.get("position_type") == "LONG")
            short_n = sum(1 for d in cur.values()
                          if d.get("position_type") == "SHORT")
            if long_n == short_n:
                self._neutrality_settle_until = time.time() + 120.0
            else:
                self._neutrality_settle_until = 0.0
                self._last_neutrality_check = 0.0
                self._neutrality_guard(force=True)

    #  REBALANCE loop (reuses the 'Scan' thread) 
    def _scan_loop(self):
        from core.logger import log_event
        log_event("Cross rebalance-loop started "
                  f"(every {self._rebalance_interval_sec()//3600}h, anchored)", "INFO")
        # init crash-filter history + slot marker
        if not hasattr(self, "_recent_rebalance_returns"):
            self._recent_rebalance_returns: List[float] = []
        # Realized move-fractions of legs closed EARLY (disaster-stop / daily
        # killswitch) since the last rebalance. Folded into the book-return that
        # feeds the crash filter so a stopped-out loser isn't invisible to it.
        if not hasattr(self, "_closed_leg_moves_since_rebalance"):
            self._closed_leg_moves_since_rebalance: List[float] = []
        self._last_rebalance_slot = None
        # Suppress the monitor's neutrality-guard while a rebalance is mid-flight
        # (the book is transiently one-sided during the sequential opens) and for
        # a short settle window afterwards.
        self._rebalance_in_progress = False
        self._neutrality_settle_until = 0.0
        self._last_rebalance_attempt = 0.0
        self._last_topup_attempt = 0.0
        # Top-up: re-attempt filling missing balanced pairs within a slot when
        # the book is under target (coins claimed by other bots / partial adopt).
        self._topup_attempts = 0
        try:
            self._topup_max = int(float(self.C("XSEC_TOPUP_MAX_ATTEMPTS", 12)))
        except (TypeError, ValueError):
            self._topup_max = 12
        # Seed the manual-force token with the CURRENT stored value so a stale
        # button press from a PREVIOUS run doesn't fire a rebalance on boot.
        try:
            from core.database import get_param as _gp
            self._force_token_seen = float(_gp(self.BOT_NAME, "FORCE_REBALANCE", 0) or 0)
        except Exception:
            self._force_token_seen = 0.0
        POLL_SEC = 30
        while not self._shutdown_event.is_set():
            try:
                forced = self._consume_force_rebalance()
                now = time.time()
                rebal_ready = forced or (now - self._last_rebalance_attempt) >= 290.0
                if (forced or self._due_for_rebalance()) and rebal_ready:
                    # Manual force runs immediately; automatic (slot/empty)
                    # attempts are throttled so a crash-flat empty book doesn't
                    # re-fetch the whole universe on every 30s poll.
                    self._last_rebalance_attempt = now
                    if forced:
                        log_event(f"[{self.BOT_NAME}] manual rebalance requested "
                                  f"- rebalancing now", "SCAN")
                    self._rebalance_tick()
                elif (self._should_topup()
                      and (now - self._last_topup_attempt) >= 290.0):
                    # Own throttle so a rebalance-due-but-throttled partial book
                    # still gets filled instead of waiting out the rebalance gap.
                    self._last_topup_attempt = now
                    self._topup_attempts += 1
                    log_event(f"[{self.BOT_NAME}] top-up {self._topup_attempts}/"
                              f"{self._topup_max}: book has {len(CrossBot._active_legs(self))} "
                              f"leg(s) under target - filling balanced pairs", "SCAN")
                    self._topup_tick()
            except Exception as e:
                log_event(f"Cross rebalance error: {e}", "WARN")
                self._log_error("cross rebalance", e)
            if self._shutdown_event.wait(timeout=POLL_SEC):
                return

    def _consume_force_rebalance(self) -> bool:
        """True (once) when the launcher's manual-rebalance button wrote a fresh
        FORCE_REBALANCE token since we last acted. Cross-process via bot_params;
        seen within ~one param-cache TTL (60s) of the press."""
        try:
            from core.database import get_param
            token = float(get_param(self.BOT_NAME, "FORCE_REBALANCE", 0) or 0)
        except Exception:
            return False
        if token > getattr(self, "_force_token_seen", 0.0):
            self._force_token_seen = token
            return True
        return False

    def _rebalance_tick(self) -> None:
        from core.logger import log_event, log_struct
        from trading.risk_manager import is_bot_paused

        if self.safe_mode is not None and self.safe_mode.is_active():
            log_event(f"[{self.BOT_NAME}] SAFE_MODE - rebalance skipped "
                      f"({self.safe_mode.reason()})", "WAIT")
            return
        paused, why = is_bot_paused(
            self.BOT_NAME, exchange=self.ex, simulation=self.simulation)
        if paused:
            log_event(f"[{self.BOT_NAME}] paused: {why}", "WAIT")
            return

        params = self._xsec_params()
        prices, sym_map = self._fetch_universe_prices(params.lookback_hours)
        if not prices:
            log_event(f"[{self.BOT_NAME}] no universe data - rebalance skipped "
                      f"(book held)", "WARN")
            return

        # 1. realize the PnL of the CURRENT book for the crash-filter signal,
        #    BEFORE we change it (so the filter learns from what just happened).
        realized = self._book_return_since_last()
        if realized is not None:
            self._recent_rebalance_returns.append(realized)
            self._recent_rebalance_returns = self._recent_rebalance_returns[-50:]
        # The early-close moves were just folded into ``realized`` - clear them
        # so the next cycle starts fresh and can't double-count them.
        self._closed_leg_moves_since_rebalance = []

        # 2. target book from the pure signal module
        book = compute_target_book(prices, self._recent_rebalance_returns, params)
        self._topup_attempts = 0   # reset in-slot top-up budget on a real rebalance
        log_event(
            f"[{self.BOT_NAME}] Rebalance | exposure x{book.exposure_mult:.0f} | "
            f"long {book.longs} | short {book.shorts}", "SCAN")
        log_struct("cross_rebalance", longs=book.longs, shorts=book.shorts,
                   exposure_mult=book.exposure_mult,
                   recent_returns=self._recent_rebalance_returns[-params.crash_window:])

        # 3. diff target vs current and execute. Suppress the monitor's
        #    neutrality-guard for the duration + a short settle window: during
        #    the sequential opens the book is TRANSIENTLY one-sided, and the
        #    guard would otherwise trim legs the rebalance is still opening.
        #    _apply_target_book runs its OWN count-based _enforce_neutrality at
        #    the end; the monitor guard is only for drift BETWEEN rebalances.
        self._rebalance_in_progress = True
        try:
            self._apply_target_book(book, params, sym_map, prices)
        finally:
            self._rebalance_in_progress = False
            self._neutrality_settle_until = time.time() + 120.0
        # Mark this slot consumed ONLY now that a book was actually applied, so
        # a restart inside the same slot resumes instead of re-rebalancing.
        self._mark_rebalanced()

        # 4. Telegram summary - ONE message per rebalance (not per leg: a 12-leg
        #    book would otherwise fire 12 opens + N closes = spam). CROSS sent
        #    nothing to Telegram before this.
        try:
            if self._telegram_enabled():
                from core.logger import send_telegram
                from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                prev = (f"Prev book return: {realized * 100:+.2f}%\n"
                        if realized is not None else "")
                # Report the ACTUAL book that ended up open (read from state
                # AFTER execution), not the target. Some legs may have been
                # skipped and neutrality may trim the excess side.
                _cur = CrossBot._active_legs(self)
                act_l = sorted(b for b, d in _cur.items()
                               if d.get("position_type") == "LONG")
                act_s = sorted(b for b, d in _cur.items()
                               if d.get("position_type") == "SHORT")
                if book.is_flat or (not act_l and not act_s):
                    body = (f"[{self.BOT_NAME}] REBALANCE -> FLAT (LIVE)\n"
                            f"{prev}Crash filter active (own momentum negative) - "
                            f"all legs closed, holding cash until it recovers.")
                else:
                    tgt = ""
                    if len(act_l) < len(book.longs) or len(act_s) < len(book.shorts):
                        tgt = (f"(target {len(book.longs)}/{len(book.shorts)} - some "
                               f"legs skipped: below exchange min size / illiquid)\n")
                    body = (f"[{self.BOT_NAME}] REBALANCE (LIVE) x{book.exposure_mult:.0f}\n"
                            f"{prev}{tgt}"
                            f"LONG ({len(act_l)}): {', '.join(act_l) or '-'}\n"
                            f"SHORT ({len(act_s)}): {', '.join(act_s) or '-'}")
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, body)
        except Exception as _te:
            if self._telegram_enabled():
                log_event(f"[{self.BOT_NAME}] telegram rebalance summary failed: {_te}",
                          "WARN")

    #  Universe + prices 
    def _fetch_universe_prices(self, lookback: int) -> Tuple[Dict[str, List[float]], Dict[str, str]]:
        """Return ({base: [hourly closes]}, {base: full_symbol}) for the top-N
        liquid perps by volume, excluding coins claimed by ANOTHER bot."""
        from core.logger import log_event
        from core.database import is_claimed_by_other
        try:
            n = int(float(self.C("XSEC_UNIVERSE_SIZE", 40)))
        except (TypeError, ValueError):
            n = 40
        min_vol = self._f("MIN_VOLUME", 5_000_000.0)
        try:
            from bot_utils.api_budget import try_consume_api_call
            if not try_consume_api_call("cross_fetch_tickers"):
                log_event(f"[{self.BOT_NAME}] universe scan skipped "
                          f"(API budget exhausted)", "WARN")
                return {}, {}
        except Exception:
            pass
        try:
            tickers = self.ex.fetch_tickers()
        except Exception as e:
            log_event(f"[{self.BOT_NAME}] ticker fetch failed: {e}", "WARN")
            return {}, {}
        cands = []
        for sym, t in tickers.items():
            if not sym.endswith(":USDT"):
                continue
            base = sym.split("/")[0].upper()
            # Crypto-only: skip oil/metal/index/forex/stock perps.
            if not _is_crypto_base(base):
                continue
            # Skip markets the exchange marks inactive/suspended/delisted - opening
            # there fails (MEXC 8823) and then neutrality trims the other side,
            # shrinking the book. ccxt's `active` flag catches fully-delisted ones
            # (a "delisting-soon" perp may still read active -> the 8823 skip in
            # _open_leg remains the backstop).
            try:
                _mkt = (getattr(self.ex, "markets", {}) or {}).get(sym) or {}
                if _mkt.get("active") is False:
                    continue
            except Exception:
                pass
            # Skip coins we marked untradeable after a delisting open-failure
            # (MEXC 8823) - a "delisting-soon" perp can still read active=True,
            # so this session/persisted exclusion is what actually keeps it out
            # and lets a replacement fill the slot.
            try:
                from core.database import is_blacklisted
                if is_blacklisted(base, self.BOT_NAME):
                    continue
            except Exception:
                pass
            qv = t.get("quoteVolume") or 0
            try:
                qv = float(qv)
            except (TypeError, ValueError):
                qv = 0.0
            if qv < min_vol:
                continue
            # coexistence: don't trade a coin another bot already holds
            if is_claimed_by_other(sym, self.BOT_NAME, is_futures=True):
                continue
            cands.append((qv, sym, base))
        cands.sort(reverse=True)
        cands = cands[:n]

        need = lookback + 6
        max_fund = self._f("XSEC_MAX_FUNDING_PCT", 0.1)
        skipped_fund = []
        prices: Dict[str, List[float]] = {}
        sym_map: Dict[str, str] = {}
        for _qv, sym, base in cands:
            if self._shutdown_event.is_set():
                break
            try:
                try:
                    from bot_utils.api_budget import try_consume_api_call
                    if not try_consume_api_call("cross_fetch_ohlcv"):
                        break
                except Exception:
                    pass
                bars = self.ex.fetch_ohlcv(sym, timeframe="1h", limit=need)
                closes = [float(b[4]) for b in bars if b and b[4]]
                if len(closes) > 1:
                    closes = closes[:-1]          # drop still-forming candle
                if len(closes) >= lookback + 1:
                    if not self._funding_ok(sym, max_fund):
                        skipped_fund.append(base)
                        continue
                    prices[base] = closes
                    sym_map[base] = sym
            except Exception:
                continue
        if skipped_fund:
            log_event(f"[{self.BOT_NAME}] funding filter excluded "
                      f"{len(skipped_fund)} coin(s) > {max_fund:g}%/8h: "
                      f"{', '.join(skipped_fund[:8])}", "INFO")
        return prices, sym_map

    #  Equity + sizing 
    def _equity(self) -> float:
        cap = self._f("BASE_CAPITAL_USDT", 0.0)
        if self.simulation:
            return cap if cap > 0 else 1000.0
        bal = None
        try:
            from bot_utils.balance import safe_fetch_balance_usdt
            bal = safe_fetch_balance_usdt(self.ex, error_logger=self._log_error)
        except Exception:
            bal = None
        try:
            bal = float(bal) if bal is not None else 0.0
        except (TypeError, ValueError):
            bal = 0.0
        if bal <= 0:
            try:
                from core.logger import log_event
                log_event(f"[{self.BOT_NAME}] live balance unavailable - "
                          "skip sizing fail-closed", "WARN")
            except Exception:
                pass
            return 0.0
        # Per-bot capital allocation: never size off more than the configured
        # BASE_CAPITAL_USDT, so CROSS uses only ITS share of a shared account
        # (you set 250 -> it deploys 250, not the whole wallet). 0 = use full free.
        if cap > 0:
            bal = min(bal, cap)
        return bal

    def _snapshot_cross_legs(self, trades) -> dict:
        """{base: (entry, qty_signed, mm_rate, mark)} for the equity-aware cross
        liq. Marks from the ticker cache, mm from the exchange tier (0.01 if
        unavailable). Legs with bad/missing data are skipped - they simply don't
        contribute to the other legs' liq estimate."""
        snap: Dict[str, tuple] = {}
        try:
            from bot_utils import get_maintenance_margin_rate
        except Exception:
            get_maintenance_margin_rate = None
        try:
            from bot_utils import futures_contract_size
        except Exception:
            futures_contract_size = None
        for base, d in trades.items():
            try:
                entry = float(d.get("buy", 0) or 0)
                amt = float(d.get("amount", 0) or 0)
                if entry <= 0 or amt <= 0:
                    continue
                full = f"{base}/USDT:USDT"
                tk = self.ticker_cache.get(self.ex, full, timeout=5.0)
                mark = float((tk or {}).get("last") or (tk or {}).get("close") or 0)
                # `not (mark > 0)` rejects NaN too (NaN <= 0 is False, so a NaN
                # mark would otherwise slip in and poison the whole book's liq).
                if not (mark > 0):
                    continue
                # LIVE state amount is in CONTRACTS; SIM state amount is already
                # in coins because no exchange contract order exists.
                cs = 1.0
                if futures_contract_size is not None:
                    try:
                        cs = float(futures_contract_size(self.ex, full) or 1.0)
                    except Exception:
                        cs = 1.0
                if cs <= 0:
                    cs = 1.0
                coins = amt if getattr(self, "simulation", False) else amt * cs
                qty = coins if d.get("position_type", "LONG") == "LONG" else -coins
                mm = 0.01
                if get_maintenance_margin_rate is not None:
                    try:
                        r = float(get_maintenance_margin_rate(self.ex, full) or 0.0)
                        if r > 0:
                            mm = r
                    except Exception:
                        pass
                snap[base] = (entry, qty, mm, mark)
            except Exception:
                continue
        return snap

    #  Diff target vs current and execute opens/closes 
    def _apply_target_book(self, book, params: XSecParams,
                           sym_map: Dict[str, str], prices: Dict[str, List[float]]) -> None:
        from core.logger import log_event

        current = CrossBot._active_legs(self)             # {base: active state-dict}
        target: Dict[str, str] = {}
        for b in book.longs:
            target[b] = "LONG"
        for b in book.shorts:
            target[b] = "SHORT"

        # CLOSE first (frees margin - critical for cross margin): held coins that
        # left the book OR flipped side.
        for base in list(current.keys()):
            held_side = current[base].get("position_type", "LONG")
            if base not in target or target[base] != held_side:
                self._close_leg(base, current[base], reason="rebalance-out")

        # OPEN new legs (size from equity x leverage x crash-mult).
        if book.is_flat:
            return
        lev = self._leverage()
        equity = self._equity()
        # Gross-exposure CAP: never deploy more than MAX_GROSS_EXPOSURE_PCT% of
        # equity, regardless of leverage (hard backstop against a mis-set lever).
        gross = equity * lev * max(0.0, min(1.0, book.exposure_mult))
        cap_pct = self._f("MAX_GROSS_EXPOSURE_PCT", 100.0)
        gross = min(gross, equity * max(0.0, cap_pct) / 100.0)
        notional = (gross / 2.0) / params.k_per_side if params.k_per_side > 0 else 0.0
        if notional <= 0:
            log_event(f"[{self.BOT_NAME}] computed leg notional 0 - nothing opened", "WARN")
            return
        # Pre-balance against claimability BEFORE opening: another bot may have
        # claimed target coins since the universe scan. Open only as many longs
        # as shorts that are actually free, so we never open an orphan leg that
        # neutrality would immediately close again (open-then-close churn).
        from core.database import is_claimed_by_other
        held_l = [b for b in book.longs if self.state.has(b)]
        held_s = [b for b in book.shorts if self.state.has(b)]
        new_l = [b for b in book.longs
                 if not self.state.has(b) and sym_map.get(b)
                 and prices.get(b, [0])[-1] > 0
                 and not is_claimed_by_other(sym_map[b], self.BOT_NAME, is_futures=True)]
        new_s = [b for b in book.shorts
                 if not self.state.has(b) and sym_map.get(b)
                 and prices.get(b, [0])[-1] > 0
                 and not is_claimed_by_other(sym_map[b], self.BOT_NAME, is_futures=True)]
        final = min(len(held_l) + len(new_l), len(held_s) + len(new_s))
        # Cap new legs to what FREE balance can actually margin (shared cross
        # account). Reduce BOTH sides equally so the book stays dollar-neutral
        # instead of opening legs the exchange would reject for InsufficientBalance.
        if not self.simulation and final > max(len(held_l), len(held_s)):
            margin_per_leg = notional / max(lev, 1.0)
            free = None
            try:
                from bot_utils.balance import safe_fetch_balance_usdt
                free = safe_fetch_balance_usdt(self.ex, error_logger=self._log_error)
            except Exception:
                free = None
            if free and free > 0 and margin_per_leg > 0:
                floor = max(len(held_l), len(held_s))
                cap_n = final
                while cap_n > floor:
                    new_needed = (max(0, cap_n - len(held_l))
                                  + max(0, cap_n - len(held_s)))
                    if new_needed * margin_per_leg <= free * 0.95:
                        break
                    cap_n -= 1
                if cap_n < final:
                    log_event(f"[{self.BOT_NAME}] free balance {free:.1f} USDT caps "
                              f"new legs to {cap_n}/side (margin {margin_per_leg:.1f}/leg) "
                              f"- opening fewer balanced pairs", "WAIT")
                    final = cap_n
        to_open = ([(b, "LONG") for b in new_l[:max(0, final - len(held_l))]]
                   + [(b, "SHORT") for b in new_s[:max(0, final - len(held_s))]])
        if to_open:
            retained_gross = sum(self._notional_from_state(d)
                                 for d in CrossBot._active_legs(self).values())
            remaining_gross = max(0.0, gross - retained_gross)
            notional = min(notional, remaining_gross / len(to_open))
            if notional <= 0:
                log_event(f"[{self.BOT_NAME}] gross cap reached by retained "
                          f"legs ({retained_gross:.1f}/{gross:.1f}) - "
                          f"no new legs opened", "WAIT")
                to_open = []
        if not to_open:
            log_event(f"[{self.BOT_NAME}] no balanced book openable this cycle "
                      f"(coins claimed by other bots / illiquid) - staying as-is",
                      "WAIT")
        for base, side in to_open:
            if self._shutdown_event.is_set():
                return
            self._open_leg(base, sym_map[base], side, notional,
                           prices[base][-1], lev)

        # Backstop: a leg can still fail mid-open (min-size / claim race) and
        # leave the book net-directional - trim the excess to stay neutral.
        self._enforce_neutrality(book)

    def _enforce_neutrality(self, book) -> None:
        """Ensure equal LONG and SHORT count (= equal notional -> dollar-neutral).
        If a leg failed to open, close the WEAKEST-conviction excess on the
        heavier side rather than run net-directional until the next rebalance."""
        from core.logger import log_event
        cur = CrossBot._active_legs(self)
        open_l = [b for b, d in cur.items() if d.get("position_type") == "LONG"]
        open_s = [b for b, d in cur.items() if d.get("position_type") == "SHORT"]
        nl, ns = len(open_l), len(open_s)
        if nl == ns:
            return
        if nl > ns:
            # book.longs ranked strongest-first -> weakest conviction at the end
            excess = [b for b in reversed(book.longs) if b in open_l][:nl - ns]
        else:
            # book.shorts ranked: weakest performer last -> weakest conviction first
            excess = [b for b in book.shorts if b in open_s][:ns - nl]
        if excess:
            log_event(f"[{self.BOT_NAME}] neutrality: book imbalanced "
                      f"({nl}L/{ns}S) - closing {len(excess)} excess leg(s) "
                      f"{excess}", "WARN")
            for b in excess:
                if self.state.has(b):
                    self._close_leg(b, cur[b], reason="neutrality")

    #  Leg execution (SIM-first; live uses the proven futures helpers) 
    def _open_leg(self, base: str, full: str, side: str, notional: float,
                  price: float, lev: float) -> None:
        from core.logger import log_event, log_struct, _date as _utc
        from core.database import (is_claimed_by_other, claim_symbol_for_entry,
                                   remove_open_position)
        # Re-check the claim right before opening (race with another bot).
        if is_claimed_by_other(full, self.BOT_NAME, is_futures=True):
            log_event(f"[{self.BOT_NAME}] {base} claimed by another bot - skip", "WAIT")
            return
        if price <= 0:
            return
        margin = notional / max(lev, 1.0)
        fees = 0.0
        provisional = False
        cs = 1.0

        #  Realistic execution + liquidity gate 
        # Use the ORDER-BOOK price you'd actually CROSS (ask for LONG, bid for
        # SHORT) so SIM reflects real slippage, not the mid/signal price. A wide
        # spread = an illiquid junk perp -> skip the leg entirely. LIVE must not
        # open without a fresh bid/ask; SIM may fall back to the signal price.
        exec_price = price
        book_ok = False
        try:
            max_spread = self._f("XSEC_MAX_SPREAD_PCT", 0.5)
            ob = self.ex.fetch_order_book(full, limit=5)
            bids = (ob or {}).get("bids") or []
            asks = (ob or {}).get("asks") or []
            if bids and asks:
                bid, ask = float(bids[0][0]), float(asks[0][0])
                if bid > 0 and ask > 0:
                    spread_pct = (ask - bid) / ((ask + bid) / 2.0) * 100.0
                    if spread_pct > max_spread:
                        log_event(f"[{self.BOT_NAME}] {base}: spread "
                                  f"{spread_pct:.2f}% > {max_spread:g}% - skip "
                                  f"leg (illiquid)", "WAIT")
                        return
                    exec_price = ask if side == "LONG" else bid
                    book_ok = True
        except Exception as exc:
            if not self.simulation:
                log_event(f"[{self.BOT_NAME}] {base}: orderbook unavailable "
                          f"({type(exc).__name__}) - skip live leg", "WARN")
                return
        if not book_ok and not self.simulation:
            log_event(f"[{self.BOT_NAME}] {base}: orderbook empty - skip live leg",
                      "WARN")
            return

        if self.simulation:
            fill = exec_price
            # SIM `amount` is in COINS (not exchange CONTRACTS - there is no real
            # order). It only feeds the paper fee below; PnL/neutrality work off
            # marginxleverage, so the coins-vs-contracts distinction is moot here.
            amount = notional / fill
            if amount <= 0:
                return
            from bot_utils.fee_math import taker_fee_rate
            fees = amount * fill * taker_fee_rate(self.ex, full, 0.0006)
        else:
            #  LIVE: cross-margin market order 
            from bot_utils import (create_order_with_retry,
                                   extract_or_estimate_futures_fee,
                                   futures_contract_size)
            from config.exchange_config import (must_set_leverage,
                                                LeverageNotSetError,
                                                safe_set_margin_mode,
                                                safe_amount_to_precision,
                                                entry_params)
            lev_int = max(1, int(__import__("math").ceil(lev)))
            # CROSS margin + leverage. HARD fail -> skip the leg; NEVER open at
            # the account-default leverage (could be 20x -> instant liquidation).
            try:
                must_set_leverage(self.ex, lev_int, full, direction=side,
                                  margin_mode="cross")
            except LeverageNotSetError as e:
                log_event(f"[{self.BOT_NAME}] {base}: set_leverage failed "
                          f"({e}) - skipping leg", "WARN")
                return
            safe_set_margin_mode(self.ex, "cross", full, leverage=lev_int,
                                 direction=side.upper())

            cs = futures_contract_size(self.ex, full)
            contracts = (notional / exec_price) / max(cs, 1e-9)
            try:
                _mkt = (getattr(self.ex, "markets", {}) or {}).get(full, {})
                _lim = (_mkt.get("limits") or {})
                _min = ((_lim.get("amount") or {}).get("min"))
                if _min and contracts < float(_min):
                    log_event(f"[{self.BOT_NAME}] {base}: contracts {contracts:g} < "
                              f"exchange min {_min:g} - skip (notional too small)", "INFO")
                    return
                _cmin = ((_lim.get("cost") or {}).get("min"))
                if _cmin and notional < float(_cmin):
                    log_event(f"[{self.BOT_NAME}] {base}: notional {notional:.2f} < "
                              f"exchange min-cost {float(_cmin):.2f} - skip leg", "INFO")
                    return
            except Exception:
                pass
            try:
                contracts = float(safe_amount_to_precision(self.ex, full, contracts))
            except Exception:
                pass
            if contracts <= 0:
                log_event(f"[{self.BOT_NAME}] {base}: contracts rounded to 0 - skip", "WARN")
                return
            order_side = "buy" if side == "LONG" else "sell"
            import hashlib as _h
            import time as _t
            _cid = (f"{self.BUY_PREFIX}-{base}-"
                    + _h.sha256(f"{self.BOT_NAME}:{base}:{int(_t.time()//30)}".encode()
                                ).hexdigest()[:10])
            params = entry_params(
                position_side="long" if side == "LONG" else "short",
                margin_mode="cross", leverage=lev_int, client_order_id=_cid)
            if not claim_symbol_for_entry(self.BOT_NAME, full, side):
                log_event(f"[{self.BOT_NAME}] {base}: claimed by another bot "
                          f"- skip", "WAIT")
                return
            self.state.add(base, {
                "position_type": side,
                "buy": exec_price,
                "highest": exec_price,
                "last_price": exec_price,
                "buy_time": _utc(),
                "invested_usdt": margin,
                "leverage": lev,
                "amount": contracts,
                "original_amount": contracts,
                "funding_paid": 0.0,
                "fees_paid": 0.0,
                "strategy": "xsec",
                "contract_size": cs,
                "provisional": True,
                "entry_inflight_until": time.time() + 120.0,
            })
            try:
                order = create_order_with_retry(
                    self.ex, full, order_side, contracts, params=params,
                    shutdown_event=self._shutdown_event,
                    action_label=f"cross open {base}",
                    log_event=log_event, log_struct=log_struct)
            except Exception as e:
                log_event(f"[{self.BOT_NAME}] {base}: open failed ({e})", "WARN")
                self._log_error(f"cross open {base}", e)
                _landed = False
                # Delisting / permanently-untradeable pair (MEXC 8823): exclude it
                # from the universe so the NEXT rebalance picks a tradeable
                # replacement instead of repeatedly selecting it and ending the
                # book short. Bot-scoped, auto-expiring blacklist.
                _es = str(e).lower()
                if "8823" in _es or "delist" in _es or "cannot be opened" in _es:
                    try:
                        from core.database import add_to_blacklist
                        add_to_blacklist(base, self.BOT_NAME, 0.0, hours=720,
                                         reason="delisting/untradeable (MEXC 8823)")
                        log_event(f"[{self.BOT_NAME}] {base}: excluded from universe "
                                  f"(delisting) - replacement picked next rebalance",
                                  "WARN")
                    except Exception:
                        pass
                # ORPHAN PREVENTION: create_order can RAISE after the order
                # actually LANDED (lost response on the final retry). Check by
                # clientOrderId - if it filled, TRACK it (provisional) instead of
                # leaving an untracked orphan. Reconcile-adoption is the backstop;
                # this closes the window at the source.
                try:
                    from bot_utils.futures_order import _find_order_by_client_id
                    landed = _find_order_by_client_id(self.ex, full, _cid)
                    _amt = float((landed or {}).get("filled")
                                 or (landed or {}).get("amount") or 0)
                    if landed is not None and _amt > 0:
                        try:
                            from bot_utils import filled_margin_usdt
                            _filled_margin, _ = filled_margin_usdt(
                                _amt, cs, exec_price, lev, margin)
                        except Exception:
                            _filled_margin = margin
                        self.state.add(base, {
                            "position_type": side, "buy": exec_price,
                            "highest": exec_price, "last_price": exec_price,
                            "buy_time": _utc(), "invested_usdt": _filled_margin,
                            "leverage": lev, "amount": _amt,
                            "original_amount": _amt, "funding_paid": 0.0,
                            "fees_paid": 0.0, "strategy": "xsec",
                            "contract_size": cs,
                            "provisional": True,
                        })
                        _landed = True
                        log_event(f"[{self.BOT_NAME}] {base}: order landed "
                                  f"despite error - tracked provisionally", "WARN")
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
                self._verify_entry_fill(full, order, exec_price)
            )
            provisional = False
            if amount <= 0 and not positions_unavailable:
                log_event(
                    f"[{self.BOT_NAME}] {base}: order returned no fill and "
                    f"no exchange position was found - aborting state write",
                    "WARN")
                removed_state = False
                try:
                    removed_state = self.state.remove(base)
                except Exception:
                    pass
                if removed_state:
                    remove_open_position(self.BOT_NAME, base)
                return
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
        try:
            from bot_utils import filled_margin_usdt
            stored_margin, _margin_verified = filled_margin_usdt(
                amount, cs, fill, lev, margin)
        except Exception:
            stored_margin = margin
        try:
            stored_notional = float(stored_margin) * float(lev)
        except Exception:
            stored_notional = notional

        self.state.add(base, {
            "position_type": side,
            "buy": fill,
            "highest": fill,
            "last_price": fill,
            "buy_time": _utc(),
            "invested_usdt": stored_margin,
            "leverage": lev,
            "amount": amount,
            "original_amount": amount,
            "funding_paid": 0.0,
            "fees_paid": fees,
            "strategy": "xsec",
            "contract_size": cs,
            "provisional": provisional,
        })
        if not provisional:
            log_event(f"[{self.BOT_NAME}] OPEN {side} {base} @ {fill:.6f} "
                      f"(notional {stored_notional:.1f}, margin {stored_margin:.1f}, fee {fees:.4f})", "INFO")
            try:
                # Same format + symbol as the FUTURES open notification.
                if self._telegram_enabled():
                    from core.logger import send_telegram
                    from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                    send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                        f"[{self.BOT_NAME}] {side} {base} @ {lev:g}x\n"
                        f"Entry: {fill:.6f} USDT\n"
                        f"Margin: {stored_margin:.2f} USDT (Notional: {stored_notional:.2f})")
            except Exception:
                pass

    def _close_leg(self, base: str, d: dict, reason: str) -> None:
        # Serialize closes per coin: monitor (disaster/killswitch) and scan
        # (rebalance/neutrality) threads can target the same leg at once. The
        # lock + state re-check prevents two reduce-only orders -> double-booked PnL.
        from core.symbol_locks import close_lock
        with close_lock(base, bot_name=self.BOT_NAME) as got:
            if not got:
                return
            if not self.state.has(base):
                return
            live = self.state.get(base)
            self._close_leg_inner(base, live if live else d, reason)

    def _cleanup_accounted_close_state(self, base: str, d: dict,
                                       log_event=None) -> bool:
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
                or "Cross close"
            ),
        }
        try:
            remove_futures_state(
                base, self.BOT_NAME,
                mode_is_sim=getattr(self, "simulation", None))
        except Exception as exc:
            self._log_error(f"cross remove_futures_state accounted {base}", exc)
            try:
                keep = dict(restore)
                keep["futures_state_cleanup_pending"] = True
                self.state.update_many(base, keep)
            except Exception as state_exc:
                self._log_error(f"cross mark cleanup pending {base}", state_exc)
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

    def _close_leg_inner(self, base: str, d: dict, reason: str) -> None:
        from core.logger import log_event, _date as _utc
        from bot_utils.futures_math import calc_unrealized_pnl, price_move_pct
        full = f"{base}/USDT:USDT"
        pos_type = d.get("position_type", "LONG")
        entry = float(d.get("buy", 0) or 0)
        if d.get("accounting_already_booked"):
            CrossBot._cleanup_accounted_close_state(
                self, base, d, log_event=log_event)
            return
        if d.get("verified_flat_pending_accounting"):
            log_event(
                f"[{self.BOT_NAME}] {base}: position already verified flat; "
                f"waiting for reconcile/offline accounting",
                "WARN",
            )
            return
        margin = float(d.get("invested_usdt", 0) or 0)
        lev = float(d.get("leverage", 1) or 1)
        amt = float(d.get("amount", 0) or 0)
        entry_fee = float(d.get("fees_paid", 0.0) or 0.0)
        close_fee = 0.0
        close_fee_is_total = False
        close_price = float(d.get("last_price", entry) or entry)
        exch_oid = None
        profit_usdt = 0.0
        live_close_already_verified = False
        pending_accounting = bool(d.get("accounting_pending"))
        pending_oid = d.get("pending_close_order_id")

        if d.get("claim_conflict") and not pending_accounting:
            warned = getattr(self, "_claim_conflict_warned", set())
            if base not in warned:
                log_event(
                    f"[{self.BOT_NAME}] {base}: registry claim conflict - "
                    f"close skipped fail-closed; run claim/state repair",
                    "ERROR",
                )
                warned.add(base)
                self._claim_conflict_warned = warned
            return

        if pending_accounting:
            try:
                close_price = float(
                    d.get("accounting_pending_sell_price")
                    or d.get("pending_close_price")
                    or close_price
                )
            except (TypeError, ValueError):
                pass
            try:
                if d.get("accounting_pending_fees_usdt") is not None:
                    close_fee = float(d.get("accounting_pending_fees_usdt") or 0.0)
                    close_fee_is_total = True
                else:
                    close_fee = float(d.get("pending_close_fee") or close_fee)
            except (TypeError, ValueError):
                pass
            exch_oid = (
                d.get("accounting_pending_exchange_order_id")
                or d.get("pending_close_order_id")
                or exch_oid
            )
            live_close_already_verified = True

        if not self.simulation and d.get("pending_close_price"):
            try:
                from bot_utils import verify_position_closed
                from bot_utils.close_fragments import pending_close_values
                _closed, _remaining = verify_position_closed(self.ex, full)
                if _closed:
                    _amt, _price, _fee, _oid = pending_close_values(d)
                    if _price > 0:
                        close_price = _price
                    close_fee = _fee
                    close_fee_is_total = False
                    exch_oid = _oid or exch_oid
                    live_close_already_verified = True
            except Exception:
                pass

        if self.simulation:
            # Realistic exit: cross the spread (long -> sell into the bid, short ->
            # buy at the ask) so SIM pays the round-trip spread, not the mid.
            try:
                ob = self.ex.fetch_order_book(full, limit=5)
                bids = (ob or {}).get("bids") or []
                asks = (ob or {}).get("asks") or []
                if pos_type == "LONG" and bids:
                    close_price = float(bids[0][0])
                elif pos_type == "SHORT" and asks:
                    close_price = float(asks[0][0])
            except Exception:
                pass
            from bot_utils.fee_math import taker_fee_rate
            close_fee = amt * close_price * taker_fee_rate(self.ex, full, 0.0006)
        elif amt > 0 and not live_close_already_verified:
            #  LIVE: reduce-only market close 
            from bot_utils import (create_order_with_retry,
                                   extract_or_estimate_futures_fee,
                                   futures_contract_size,
                                   is_no_position_error,
                                   verify_position_closed)
            from config.exchange_config import reduce_only_params, safe_amount_to_precision
            lev_int = max(1, int(__import__("math").ceil(lev)))
            try:
                amt = float(safe_amount_to_precision(self.ex, full, amt))
            except Exception:
                pass
            if amt <= 0:
                log_event(f"[{self.BOT_NAME}] {base}: close amount rounded to 0 "
                          f"- keeping state for retry", "WARN")
                return
            close_side = "sell" if pos_type == "LONG" else "buy"
            try:
                order = create_order_with_retry(
                    self.ex, full, close_side, amt,
                    params=reduce_only_params(
                        position_side="long" if pos_type == "LONG" else "short",
                        margin_mode="cross", leverage=lev_int),
                    shutdown_event=self._shutdown_event,
                    action_label=f"cross close {base}", log_event=log_event)
                try:
                    order_filled = max(0.0, float(order.get("filled") or 0.0))
                except (TypeError, ValueError):
                    order_filled = 0.0
            except Exception as e:
                if is_no_position_error(e):
                    try:
                        _closed, _remaining = verify_position_closed(self.ex, full)
                    except Exception as ve:
                        self._log_error(f"cross verify-close-after-error {base}", ve)
                        log_event(
                            f"[{self.BOT_NAME}] {base}: close error looked flat "
                            f"but verification failed - kept for reconcile",
                            "WARN",
                        )
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
                                    f"cross mark verified-flat {base}",
                                    state_err)
                            log_event(
                                f"[{self.BOT_NAME}] {base}: position already "
                                f"flat on exchange ({str(e)[:80]}) - keeping "
                                f"state for reconcile/offline accounting",
                                "WARN",
                            )
                            return
                        log_event(
                            f"[{self.BOT_NAME}] {base}: position already flat "
                            f"on exchange ({str(e)[:80]}) after our close "
                            f"order - booking pending close",
                            "WARN",
                        )
                        try:
                            from bot_utils.close_fragments import pending_close_values
                            _amt, _price, _fee, _oid = pending_close_values(d)
                            if _price > 0:
                                close_price = _price
                            close_fee = _fee
                            close_fee_is_total = False
                            exch_oid = _oid or pending_oid or exch_oid
                        except Exception:
                            try:
                                close_price = float(
                                    d.get("pending_close_price") or close_price)
                            except (TypeError, ValueError):
                                pass
                            try:
                                close_fee = float(
                                    d.get("pending_close_fee") or close_fee)
                                close_fee_is_total = False
                            except (TypeError, ValueError):
                                pass
                            exch_oid = pending_oid or exch_oid
                        live_close_already_verified = True
                    else:
                        log_event(
                            f"[{self.BOT_NAME}] {base}: close error but "
                            f"{_remaining:.6f} contracts remain - kept for retry",
                            "WARN",
                        )
                        return
                else:
                    log_event(f"[{self.BOT_NAME}] {base}: close FAILED ({e}) - "
                              f"position KEPT for retry; close MANUALLY if it persists",
                              "ERROR")
                    self._log_error(f"cross close {base}", e)
                    return   # keep state -> monitor / next rebalance retries
            if live_close_already_verified:
                order = {}
            else:
                exch_oid = order.get("id") or order.get("orderId")
                try:
                    from bot_utils.futures_exits import _resolve_fill_price
                    close_price, _fill_src = _resolve_fill_price(
                        self.ex, full, order, close_price, log_event)
                except Exception:
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
                    cs = futures_contract_size(self.ex, full)
                except Exception:
                    cs = 1.0

                #  VERIFY THE CLOSE BEFORE BOOKING
                # Confirm the leg is flat (fetch_positions) BEFORE booking PnL and
                # dropping it. On a partial fill in a thin alt book (routine on
                # cross-margin alts) or an unverifiable close, keep the leg and let
                # the monitor / next rebalance retry - the reduce-only retry caps to
                # the true remaining size, so the eventual confirmed close books
                # once and never leaves an unmanaged orphan or double-counts PnL.
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
                    self._log_error(f"cross verify-close {base}", e)
                    log_event(f"[{self.BOT_NAME}] {base}: close unverified - "
                              f"keeping leg, retry next tick", "WARN")
                    return
                if not _closed:
                    try:
                        from bot_utils.close_fragments import (
                            add_close_fragment_update, pending_close_values)
                        prev_amount, _px, _fee, _oid = pending_close_values(d)
                        total_filled = max(0.0, amt - float(_remaining))
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
                    log_event(
                        f"[{self.BOT_NAME}] {base}: close incomplete "
                        f"(remaining {_remaining:.6f}) - keeping leg, retry next "
                        f"tick (partial fill accounted pending)",
                        "WARN")
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
                    else:
                        _amt, _price, _fee, _oid = pending_close_values(d)
                    if _amt > 0 and _price > 0:
                        close_price = _price
                        close_fee = _fee
                        close_fee_is_total = False
                        exch_oid = _oid or exch_oid
                except Exception:
                    try:
                        close_fee = extract_or_estimate_futures_fee(
                            self.ex, order, full, close_price, amount=amt,
                            contract_size=cs)
                        close_fee_is_total = False
                    except Exception:
                        pass

        #  Record REALIZED PnL (so get_today_pnl / metrics / killswitch work) 
        if entry > 0 and close_price > 0 and margin > 0:
            pnl_usdt, _ = calc_unrealized_pnl(entry, close_price, margin, lev, pos_type)
            funding = float(d.get("funding_paid", 0.0) or 0.0)
            if not self.simulation:
                try:
                    from bot_utils import fetch_or_estimate_funding
                    realized = fetch_or_estimate_funding(
                        self.ex, full, d.get("buy_time"),
                        notional_usdt=margin * lev if margin > 0 else 0.0,
                        pos_type=pos_type,
                        fallback_state_value=funding,
                    )
                    if realized is not None:
                        funding = float(realized)
                except Exception:
                    pass
            total_fees = close_fee if close_fee_is_total else entry_fee + close_fee
            total_fees, funding = self._clamp_cross_sim_costs(
                full, d, total_fees, funding, log_event=log_event)
            profit_usdt = round(pnl_usdt - total_fees - funding, 4)
            profit_pct = price_move_pct(entry, close_price, pos_type)
            try:
                mfe_pct = max(float(d.get("max_profit_pct", profit_pct) or profit_pct), profit_pct)
            except (TypeError, ValueError):
                mfe_pct = profit_pct
            try:
                mae_pct = min(float(d.get("min_profit_pct", profit_pct) or profit_pct), profit_pct)
            except (TypeError, ValueError):
                mae_pct = profit_pct
            giveback_pct = max(0.0, mfe_pct - profit_pct)
            sell_time = _utc()
            reason_for_db = f"Cross {reason}"
            if pending_accounting:
                try:
                    profit_usdt = float(
                        d.get("accounting_pending_profit_usdt", profit_usdt))
                except (TypeError, ValueError):
                    pass
                try:
                    profit_pct = float(
                        d.get("accounting_pending_profit_pct", profit_pct))
                except (TypeError, ValueError):
                    pass
                try:
                    funding = float(
                        d.get("accounting_pending_funding_paid", funding))
                except (TypeError, ValueError):
                    pass
                try:
                    mfe_pct = float(d.get("accounting_pending_mfe_pct", mfe_pct))
                except (TypeError, ValueError):
                    pass
                try:
                    mae_pct = float(d.get("accounting_pending_mae_pct", mae_pct))
                except (TypeError, ValueError):
                    pass
                try:
                    giveback_pct = float(
                        d.get("accounting_pending_giveback_pct", giveback_pct))
                except (TypeError, ValueError):
                    pass
                sell_time = d.get("accounting_pending_sell_time") or sell_time
                reason_for_db = d.get("accounting_pending_reason") or reason_for_db
            try:
                from core.database import save_trade_db
                accounting_mode_is_sim = d.get(
                    "accounting_pending_mode_is_sim", self.simulation)
                saved_ok = bool(save_trade_db(
                    bot_name=self.BOT_NAME, mode_is_sim=accounting_mode_is_sim, symbol=base,
                    buy_price=entry, sell_price=close_price,
                    buy_time=d.get("buy_time", ""), sell_time=sell_time,
                    profit_pct=profit_pct, profit_usdt=profit_usdt,
                    invested_usdt=margin, reason=reason_for_db,
                    is_futures=True, position_type=pos_type, leverage=lev,
                    funding_paid=funding, fees_usdt=total_fees,
                    exchange_order_id=exch_oid,
                    mfe_pct=mfe_pct, mae_pct=mae_pct,
                    giveback_pct=giveback_pct))
                if not saved_ok:
                    raise RuntimeError("save_trade_db returned False")
            except Exception as e:
                self._log_error(f"cross save_trade {base}", e)
                log_event(
                    f"[{self.BOT_NAME}] {base}: DB accounting failed after "
                    f"verified close ({e}) - state kept for recovery", "WARN")
                try:
                    self.state.update_many(base, {
                        "accounting_pending": True,
                        "accounting_pending_reason": f"Cross {reason}",
                        "accounting_pending_sell_price": close_price,
                        "accounting_pending_sell_time": sell_time,
                        "accounting_pending_profit_pct": profit_pct,
                        "accounting_pending_profit_usdt": profit_usdt,
                        "accounting_pending_mode_is_sim": self.simulation,
                        "accounting_pending_fees_usdt": total_fees,
                        "accounting_pending_funding_paid": funding,
                        "accounting_pending_exchange_order_id": exch_oid,
                        "accounting_pending_mfe_pct": mfe_pct,
                        "accounting_pending_mae_pct": mae_pct,
                        "accounting_pending_giveback_pct": giveback_pct,
                    })
                except Exception as state_err:
                    self._log_error(f"cross mark accounting_pending {base}",
                                    state_err)
                return
        else:
            log_event(
                f"[{self.BOT_NAME}] {base}: close accounting skipped due to "
                f"invalid state - state kept for review", "WARN")
            return

        # Feed between-rebalance closes into the crash-filter book-return
        # accumulator. rebalance-out is already counted as a survivor when
        # _book_return_since_last runs at the start of the rebalance.
        if reason in ("disaster-stop", "daily-loss killswitch", "neutrality-guard") and entry > 0 and close_price > 0:
            try:
                acc = getattr(self, "_closed_leg_moves_since_rebalance", None)
                if acc is None:
                    acc = self._closed_leg_moves_since_rebalance = []
                acc.append((price_move_pct(entry, close_price, pos_type) / 100.0,
                            margin * lev))
            except Exception:
                pass
        if reason == "disaster-stop":
            self._blacklist_disaster_symbol(base, profit_usdt)

        cleanup_row = dict(d)
        cleanup_row.update({
            "accounting_already_booked": True,
            "accounting_booked_sell_time": sell_time,
            "accounting_booked_exchange_order_id": exch_oid,
            "accounting_booked_reason": reason_for_db,
        })
        CrossBot._cleanup_accounted_close_state(
            self, base, cleanup_row, log_event=log_event)
        log_event(f"[{self.BOT_NAME}] CLOSE {pos_type} {base} @ {close_price:.6f} "
                  f"({reason}, PnL {profit_usdt:+.2f})"
                  if entry > 0 else
                  f"[{self.BOT_NAME}] CLOSE {pos_type} {base} ({reason})", "INFO")
        try:
            # Same format + symbols as the FUTURES close notification (WIN/LOSS).
            if self._telegram_enabled():
                from core.logger import send_telegram
                from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                _move = price_move_pct(entry, close_price, pos_type) if entry > 0 else 0.0
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f"{'WIN' if profit_usdt >= 0 else 'LOSS'} [{self.BOT_NAME}] "
                    f"{pos_type} CLOSE {base}\n"
                    f"Move: {_move:+.2f}% ({profit_usdt:+.2f} USDT auf "
                    f"{margin:.0f} Margin @ {lev:g}x)\n"
                    f"Reason: {reason}")
        except Exception:
            pass

    #  Crash-filter signal: return of the held book since last rebalance 
    def _blacklist_disaster_symbol(self, base: str, loss_usdt: float) -> None:
        """Temporarily exclude a symbol after a CROSS disaster stop."""
        try:
            hours = int(float(self.C("CROSS_DISASTER_BLACKLIST_HOURS", 72)))
        except (TypeError, ValueError):
            hours = 72
        if hours <= 0:
            return
        try:
            from core.database import add_to_blacklist
            from core.logger import log_event
            add_to_blacklist(
                base,
                self.BOT_NAME,
                loss_usdt,
                hours=hours,
                reason="cross disaster-stop",
            )
            log_event(
                f"[{self.BOT_NAME}] {base}: disaster-stop blacklist for "
                f"{hours}h",
                "WARN",
            )
        except Exception as e:
            self._log_error(f"cross disaster blacklist {base}", e)

    def _book_return_since_last(self) -> Optional[float]:
        """Approximate market-neutral return of the CURRENT book since entry,
        as a fraction of gross. Used only to feed the own-momentum crash filter
        (not for accounting). None when nothing happened this cycle.

        Includes BOTH still-open legs (unrealized move) AND legs closed early
        this cycle by the disaster-stop / daily killswitch (their realized move,
        accumulated in ``_closed_leg_moves_since_rebalance``). Without the latter
        the signal would be survivorship-biased UP - the big losers that already
        stopped out would be invisible and the filter could stay invested when
        it should go flat."""
        from bot_utils.futures_math import price_move_pct
        wmoves = []   # (move_fraction, notional_weight)
        for base, d in CrossBot._active_legs(self).items():
            entry = float(d.get("buy", 0) or 0)
            last = float(d.get("last_price", entry) or entry)
            margin = float(d.get("invested_usdt", 0) or 0)
            lev = float(d.get("leverage", 1) or 1)
            if entry > 0 and last > 0:
                w = margin * lev if margin > 0 else 1.0
                wmoves.append(
                    (price_move_pct(entry, last, d.get("position_type", "LONG")) / 100.0, w))
        wmoves += [(m, max(w, 0.0))
                   for (m, w) in getattr(self, "_closed_leg_moves_since_rebalance", [])]
        if not wmoves:
            return None
        tot_w = sum(w for _, w in wmoves)
        if tot_w <= 0:
            return sum(m for m, _ in wmoves) / len(wmoves)
        return sum(m * w for m, w in wmoves) / tot_w   # notional-weighted book return

    #  CROSS monitor (reuses the 'Monitor' thread) 
    def _monitor_loop(self):
        from core.logger import log_event
        interval = int(self.C("MONITOR_INTERVAL", self.DEFAULT_MONITOR_INTERVAL))
        log_event(f"Cross monitor-loop started (interval: {interval}s)", "INFO")
        while not self._shutdown_event.is_set():
            try:
                self._monitor_tick()
            except Exception as e:
                log_event(f"Cross monitor error: {e}", "WARN")
                self._log_error("cross monitor", e)
            if self._shutdown_event.wait(timeout=interval):
                return

    def _check_daily_killswitch(self, trades: dict) -> None:
        """Flatten the book + SAFE_MODE when today's realized+unrealized PnL
        breaches MAX_DAILY_LOSS. Throttled to ~60s. One-shot SAFE_MODE then
        blocks the next rebalance from re-opening."""
        now = time.time()
        if now - getattr(self, "_last_ks_check", 0.0) < 60.0:
            return
        self._last_ks_check = now
        try:
            from core.database import get_today_pnl
            from bot_utils.futures_math import calc_unrealized_pnl
            from core.logger import log_event
            realized = float(get_today_pnl(self.BOT_NAME, mode_is_sim=self.simulation).get("total_profit", 0.0) or 0.0)
            unreal = 0.0
            for _b, d in trades.items():
                if not self._is_active_leg(d):
                    continue
                entry = float(d.get("buy", 0) or 0)
                last = float(d.get("last_price", entry) or entry)
                margin = float(d.get("invested_usdt", 0) or 0)
                lev = float(d.get("leverage", 1) or 1)
                if entry > 0 and last > 0 and margin > 0:
                    u, _ = calc_unrealized_pnl(entry, last, margin, lev,
                                                d.get("position_type", "LONG"))
                    unreal += u - (margin * lev * 0.0006)   # est. taker exit fee
            total = realized + unreal
            max_loss = self._f("MAX_DAILY_LOSS", -50.0)
            if max_loss < 0 and total <= max_loss:
                log_event(f"[{self.BOT_NAME}] DAILY-LOSS KILLSWITCH "
                          f"{total:+.2f} <= {max_loss:.0f} USDT - flattening book "
                          f"+ SAFE_MODE", "ERROR")
                try:
                    if self._telegram_enabled():
                        from core.logger import send_telegram
                        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f"[{self.BOT_NAME}] DAILY-LOSS KILLSWITCH\n"
                            f"Today {total:+.2f} USDT <= limit {max_loss:.0f} USDT.\n"
                            f"Flattening the whole book + SAFE_MODE (no new entries).")
                except Exception:
                    pass
                if self.safe_mode is not None and not self.safe_mode.is_active():
                    self.safe_mode.trigger(f"daily-loss killswitch ({total:+.2f} USDT)")
                for base in list(trades.keys()):
                    if self.state.has(base):
                        self._close_leg(base, trades[base], reason="daily-loss killswitch")
                if CrossBot._active_legs(self):
                    self._last_ks_check = 0.0
        except Exception as e:
            self._log_error("cross daily killswitch", e)

    def _monitor_tick(self) -> None:
        from core.database import upsert_futures_state
        from core.logger import log_event
        from bot_utils.futures_math import price_move_pct, calc_unrealized_pnl
        raw_trades = self.state.get_all()
        if not raw_trades:
            return
        for base, d in list(raw_trades.items()):
            if d.get("provisional"):
                if self._heal_provisional_leg(base, d):
                    healed = self.state.get(base)
                    if healed:
                        raw_trades[base] = healed
                else:
                    raw_trades.pop(base, None)
        for base, d in list(raw_trades.items()):
            if d.get("accounting_already_booked"):
                CrossBot._cleanup_accounted_close_state(
                    self, base, d, log_event=log_event)
        trades = CrossBot._active_legs(self, raw_trades)
        if not trades:
            return
        # Account-level daily-loss killswitch (flatten + SAFE_MODE). Without
        # this, only the next rebalance (up to REBALANCE_HOURS away) would stop
        # new entries - a bleeding book would run unprotected between rebalances.
        CrossBot._check_daily_killswitch(self, trades)
        self._maybe_persist_funding_for_all(trades, time.time())
        disaster = self._f("PER_LEG_DISASTER_STOP", -25.0)
        liq_safety = max(0.0, min(95.0, self._f("LIQ_SAFETY_PCT", 20.0)))
        # Snapshot every leg once for the equity-aware CROSS liq below - each
        # leg's liq depends on the OTHER legs' uPnL + maintenance margin.
        try:
            _collateral = self._equity()
            _legs = self._snapshot_cross_legs(trades)
        except Exception:
            _collateral, _legs = 0.0, {}
        for base, d in trades.items():
            if self._shutdown_event.is_set():
                return
            if d.get("claim_conflict"):
                warned = getattr(self, "_claim_conflict_warned", set())
                if base not in warned:
                    log_event(
                        f"[{self.BOT_NAME}] {base}: registry claim conflict - "
                        f"monitor skipped fail-closed; run claim/state repair",
                        "ERROR",
                    )
                    warned.add(base)
                    self._claim_conflict_warned = warned
                continue
            if not self.state.has(base):   # closed this tick (killswitch) - skip
                continue
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
                    continue
            if curr <= 0:
                continue
            self.state.update(base, "last_price", curr)
            pos_type = d.get("position_type", "LONG")
            entry = float(d.get("buy", 0) or 0)
            if entry <= 0:
                continue
            move = price_move_pct(entry, curr, pos_type)
            try:
                prev_mfe = float(d.get("max_profit_pct", move) or move)
            except (TypeError, ValueError):
                prev_mfe = move
            try:
                prev_mae = float(d.get("min_profit_pct", move) or move)
            except (TypeError, ValueError):
                prev_mae = move
            mfe_pct = max(prev_mfe, move)
            mae_pct = min(prev_mae, move)
            telemetry = {
                "max_profit_pct": mfe_pct,
                "min_profit_pct": mae_pct,
                "giveback_pct": max(0.0, mfe_pct - move),
            }
            if str(pos_type).upper() == "LONG":
                try:
                    telemetry["highest"] = max(float(d.get("highest", curr) or curr), curr)
                except (TypeError, ValueError):
                    telemetry["highest"] = curr
            try:
                self.state.update_many(base, telemetry)
            except Exception:
                pass
            liq_price = 0.0
            liq_dist = 0.0
            try:
                if not self.simulation:
                    now_ts = time.time()
                    if now_ts >= float(d.get("liq_next_check_at", 0) or 0):
                        try:
                            from bot_utils import get_exchange_liq_price
                            liq_price = float(get_exchange_liq_price(self.ex, full) or 0.0)
                        except Exception:
                            liq_price = 0.0
                        upd = {"liq_next_check_at": now_ts + float(
                            getattr(self, "LIQ_REFRESH_INTERVAL_SEC", 90.0))}
                        if liq_price > 0:
                            upd["liquidation_price"] = liq_price
                        try:
                            self.state.update_many(base, upd)
                        except Exception:
                            pass
                    else:
                        liq_price = float(d.get("liquidation_price", 0.0) or 0.0)
                if liq_price <= 0:
                    try:
                        from bot_utils.futures_math import cross_liquidation_price
                        tgt = _legs.get(base)
                        if tgt is not None:
                            others = [v for b, v in _legs.items() if b != base]
                            cp = cross_liquidation_price(tgt[0], tgt[1], tgt[2],
                                                         tgt[3], others, _collateral)
                            liq_price = float(cp) if cp else 0.0
                    except Exception:
                        liq_price = 0.0
                if liq_price > 0 and curr > 0:
                    try:
                        from bot_utils import distance_to_liquidation_pct
                        liq_dist = distance_to_liquidation_pct(curr, liq_price,
                                                               pos_type)
                    except Exception:
                        liq_dist = 0.0
            except Exception:
                liq_price = 0.0
                liq_dist = 0.0
            # Per-leg disaster stop, leverage-aware: take whichever fires EARLIER
            # - the user's flat stop OR a liq-safety stop that sits LIQ_SAFETY_PCT
            # before the best available liquidation estimate. Prefer exchange /
            # cross-margin liquidation; fall back to isolated ~100/lev.
            lev_leg = float(d.get("leverage", 1) or 1)
            liq_move = 100.0 / max(lev_leg, 1.0)
            if liq_price > 0 and entry > 0:
                if str(pos_type).upper() == "LONG" and liq_price < entry:
                    liq_move = abs((entry - liq_price) / entry * 100.0)
                elif str(pos_type).upper() == "SHORT" and liq_price > entry:
                    liq_move = abs((liq_price - entry) / entry * 100.0)
                if liq_move <= 0:
                    liq_move = 100.0 / max(lev_leg, 1.0)
            eff_stop = max(disaster, -(liq_move * (1.0 - liq_safety / 100.0)))
            if move <= eff_stop:
                from core.logger import log_event
                log_event(f"[{self.BOT_NAME}] disaster-stop {base} ({pos_type}) "
                          f"{move:.1f}% <= {eff_stop:.1f}% (lev {lev_leg:g}x)", "WARN")
                try:
                    if self._telegram_enabled():
                        from core.logger import send_telegram
                        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f"[{self.BOT_NAME}] DISASTER-STOP {base} ({pos_type})\n"
                            f"Leg moved {move:.1f}% (<= {eff_stop:.1f}%) - closing early.")
                except Exception:
                    pass
                self._close_leg(base, d, reason="disaster-stop")
                self._neutrality_settle_until = 0.0
                self._last_neutrality_check = 0.0
                self._neutrality_guard(force=True)
                continue
            # Live-state for the UI (reuse futures_state, keyed by bot_name).
            try:
                lev = float(d.get("leverage", 1.0))
                margin = float(d.get("invested_usdt", 0))
                u, upct = calc_unrealized_pnl(entry, curr, margin, lev, pos_type)
                # Liquidation price for dashboard and disaster-stop. LIVE:
                # prefer the exchange's OWN liq price - the real account is
                # cross-margined across ALL bots, so only it is authoritative.
                # Fallback (and SIM primary): the equity-aware CROSS estimate
                # over THIS bot's legs, which is exact in SIM (the bot is alone
                # with its collateral) and accounts for the correlated-drawdown
                # danger the old isolated formula hid (CR-1).
                upsert_futures_state(
                    symbol=base, bot_name=self.BOT_NAME, mode_is_sim=self.simulation, position_type=pos_type,
                    entry_price=entry, current_price=curr, leverage=lev,
                    margin_usdt=margin, position_size_usdt=margin * lev,
                    unrealized_pnl=u, unrealized_pct=upct,
                    liquidation_price=liq_price, liq_distance_pct=liq_dist,
                    funding_paid=d.get("funding_paid", 0.0),
                    opened_at=d.get("buy_time", ""))
            except Exception:
                pass

        # Continuous dollar-neutrality guard (throttled). Runs EVERY monitor
        # tick window, not just at the 72h rebalance, so a book that turned
        # net-directional between rebalances - a disaster-stopped leg, an
        # interrupted rebalance, an open that was skipped - is rebalanced back
        # toward neutral within minutes instead of staying directional for up
        # to REBALANCE_HOURS.
        self._neutrality_guard()

    def _neutrality_guard(self, force: bool = False) -> None:
        """Trim the heavier side back to dollar-neutral by NOTIONAL.

        Equal leg COUNT only equals dollar-neutral when every leg carries the
        same notional - which stops being true after a single-leg close or a
        partially-applied rebalance. This guard measures real net notional
        (margin x leverage per leg) and, when |net|/gross exceeds the tolerance,
        closes the worst-performing legs on the heavy side (keep the winners,
        cut the laggards) until the book is back inside the band.
        """
        # Never run while a rebalance is opening/closing legs (book is
        # intentionally transient then) or during the post-rebalance settle
        # window - the rebalance does its own neutrality pass.
        if getattr(self, "_rebalance_in_progress", False):
            return
        now = time.time()
        if not force and now < getattr(self, "_neutrality_settle_until", 0.0):
            return
        if not force and now - getattr(self, "_last_neutrality_check", 0.0) < 180.0:
            return
        self._last_neutrality_check = now

        from core.logger import log_event
        from bot_utils.futures_math import calc_unrealized_pnl

        trades = CrossBot._active_legs(self)
        if not trades:
            return
        tol = max(0.0, self._f("CROSS_NEUTRALITY_TOL_PCT", 15.0)) / 100.0

        legs = []   # (base, side, notional, upnl, state_dict)
        gross = 0.0
        net = 0.0  # +long  short notional
        for base, d in trades.items():
            side = d.get("position_type", "LONG")
            entry = float(d.get("buy", 0) or 0)
            last = float(d.get("last_price", entry) or entry)
            margin = float(d.get("invested_usdt", 0) or 0)
            lev = float(d.get("leverage", 1) or 1)
            notional = margin * lev
            if notional <= 0:
                continue
            upnl = 0.0
            if entry > 0 and last > 0:
                upnl, _ = calc_unrealized_pnl(entry, last, margin, lev, side)
            legs.append((base, side, notional, upnl, d))
            gross += notional
            net += notional if side == "LONG" else -notional

        if gross <= 0 or abs(net) / gross <= tol:
            return

        heavy = "LONG" if net > 0 else "SHORT"
        # Cut the worst-performing legs on the heavy side first (ascending uPnL).
        candidates = sorted((leg for leg in legs if leg[1] == heavy),
                            key=lambda leg: leg[3])
        log_event(f"[{self.BOT_NAME}] neutrality-guard: net notional "
                  f"{net:+.1f}/{gross:.1f} ({abs(net)/gross*100:.0f}% > "
                  f"{tol*100:.0f}%) - trimming {heavy} side", "WARN")
        for base, side, notional, _upnl, d in candidates:
            if gross <= 0 or abs(net) / gross <= tol:
                break
            if not self.state.has(base):
                continue
            self._close_leg(base, d, reason="neutrality-guard")
            if self.state.has(base):
                continue
            # removing a LONG lowers net; removing a SHORT raises it
            net += -notional if heavy == "LONG" else notional
            gross -= notional
