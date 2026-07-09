"""
core/spot_bot_exits.py  Monitor-thread + exit logic for SpotBot.

ExitsMixin runs at MONITOR_INTERVAL (~20s) and handles ONLY exits:
  Per-position price fetch (batched fetch_tickers)
  Highest-price tracking
  Breakeven activation
  Partial take-profit at ACTIVATION_PROFIT
  Full exit: trailing-stop, hard stop-loss, break-even stop
  BTC stress override (force break-even on profitable positions)

Open positions get SL/TP attention every ~20s, not blocked behind a 150s+
scan cycle that may include 30-90s LLM-analysis calls.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Optional

from core.clock import now_utc
from bot_utils import (
    spot_market_sell_safe,
    InsufficientSellBalance,
    extract_fill_price,
    extract_order_fee,
    safe_remaining,
    safe_proportional_fee,
)
from bot_utils.safe_numeric import safe_positive_float


# Minimum notional safety margin  exchanges reject sells below this.
MIN_NOTIONAL_BUFFER = 5.5

# Chunk fetch_tickers calls so the URL stays well under the HTTP 414 limit.
# At MAX_OPEN_TRADES=40+ an un-chunked URL would exceed it.
TICKER_BATCH_SIZE = 20

# Persist last_price only every N seconds, not on every monitor tick  at
# MAX_OPEN_TRADES=30 every-tick persistence meant ~90 atomic JSON writes/min.
# 60s drops that I/O while keeping the Launcher Stop-Dialog and crash-recovery
# price close enough to real.
LAST_PRICE_PERSIST_INTERVAL_SEC = 60.0

# Close-fee estimate for the killswitch's unrealized-PnL computation.
# Conservative  slightly over-estimates so the killswitch fires earlier.
_UNREALIZED_CLOSE_FEE_RATE = 0.001

# Breakeven stop sits this far ABOVE entry so a breakeven exit covers the
# round-trip taker fee (~20.1%) + a little slippage and nets ~0 instead of a
# small fee loss. Mirrors the futures fee_buffered_breakeven (0.3%).
_BE_FEE_BUFFER = 0.003

# Residual live fills above this value stay tracked instead of being treated as
# dust. This avoids losing real base coins from state after a partial exchange
# fill.
_LIVE_RESIDUAL_DUST_USDT = 1.0


def _finite_float(value, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if math.isfinite(parsed) else default


def _positive_finite(value, default: float = 0.0) -> float:
    parsed = _finite_float(value, default)
    return parsed if parsed > 0 else default


def _utc_now_str() -> str:
    """Exchange-anchored UTC timestamp. sell_time is later read by funding
    estimation that assumes UTC, so a local-time stamp would mis-attribute
    funding cost off-UTC; the exchange anchor also makes it drift-proof."""
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def _position_age_hours(d: dict) -> Optional[float]:
    """Hours since the position's buy_time (UTC), or None if unparseable."""
    bt = str(d.get("buy_time") or "")
    if not bt:
        return None
    try:
        opened = datetime.strptime(bt, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    return (now_utc() - opened).total_seconds() / 3600.0


def _filled_base_amount(order, wrapper_sold, requested_amount: float) -> float:
    """Best-effort base amount that actually filled, capped to the request."""
    requested = _positive_finite(requested_amount)
    if isinstance(order, dict):
        for key in ("filled",):
            value = _positive_finite(order.get(key))
            if value > 0:
                return min(requested, value)
    sold = _positive_finite(wrapper_sold)
    if sold > 0:
        return min(requested, sold)
    return 0.0


class ExitsMixin:
    """Monitor thread + exit decision logic."""

    def _retry_pending_partial_accounting(self, sym: str, d: dict) -> None:
        from bot_utils.trade_state import normalize_pending_accounting_items
        pending = normalize_pending_accounting_items(
            d.get("accounting_pending_partials"))
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
                self._log_error(f"spot partial accounting retry {sym}", exc)
            if not saved:
                remaining.append(item)
        self.state.update(sym, "accounting_pending_partials", remaining)
        if remaining:
            log_event(
                f" {sym}: {len(remaining)} partial accounting event(s) "
                f"still pending", "WARN")
        else:
            log_event(f"{sym}: pending partial accounting flushed", "INFO")

    def _cleanup_accounted_close_state(self, sym: str, d: dict) -> bool:
        """Remove local/claim state after realized PnL was already booked."""
        from core.logger import log_event
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
        ok = remove_with_restore_fields(self.state, sym, restore)
        if not ok:
            log_event(
                f" {sym}: close already booked, but claim/state cleanup "
                f"failed; state kept for retry",
                "WARN",
            )
            return False
        return True

    #  Monitor-thread body 

    def _monitor_loop(self):
        """Thread body  runs forever until shutdown_event is set.

        Per tick:
          1. Snapshot open positions
          2. BTC stress override (force BE on profitable positions)
          3. Daily-loss killswitch (every 60s  not every tick)
          4. Batch fetch_tickers for all open symbols
          5. For each position: check exits via _check_position_exits
        """
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
        KILLSWITCH_INTERVAL = 60.0   # check daily-loss every 60s
        consecutive_errors = 0
        MONITOR_ERR_THRESHOLD = 5

        while not self._shutdown_event.is_set():
            try:
                trades = self.state.get_all()
                now = time.time()

                # Daily-loss killswitch (independent of open positions 
                # we want to trip even AFTER all positions closed at a
                # loss so the bot doesn't keep opening new ones)
                if (now - last_killswitch) >= KILLSWITCH_INTERVAL:
                    last_killswitch = now
                    self._check_daily_killswitch(trades)

                if not trades:
                    idle_ticks += 1
                    if now - last_idle_log >= 300:
                        log_event(
                            f" Monitor idle  no open positions "
                            f"(Tick #{idle_ticks})",
                            "INFO"
                        )
                        last_idle_log = now
                    if self._shutdown_event.wait(timeout=min(monitor_interval, 10)):
                        return
                    continue

                # BTC stress override
                self._btc_stress_override(trades)

                # Batch ticker fetch
                batch = self._batch_tickers(list(trades.keys()))

                for sym, d in trades.items():
                    if self._shutdown_event.is_set():
                        break
                    curr = self._get_current_price(sym, batch, d)
                    if curr <= 0:
                        self._note_spot_price_unavailable(sym, log_event)
                        continue
                    self._clear_spot_price_unavailable(sym)
                    try:
                        self._check_position_exits(sym, d, curr)
                    except Exception as e:
                        self._log_error(f"monitor {sym}", e)

                consecutive_errors = 0  # tick succeeded  reset

            except Exception as e:
                consecutive_errors += 1
                log_event(f"Monitor tick error: {e}", "WARN")
                self._log_error("monitor loop", e)
                if consecutive_errors >= MONITOR_ERR_THRESHOLD:
                    log_event(
                        f" Monitor: {consecutive_errors} consecutive errors  "
                        f"investigation needed (check error_log.txt)",
                        "WARN"
                    )

            if self._shutdown_event.wait(timeout=monitor_interval):
                return

    def _check_daily_killswitch(self, trades: dict) -> None:
        """Daily-loss killswitch for SPOT bots.

        Triggers SAFE_MODE when today's realized + unrealized PnL falls
        below MAX_DAILY_LOSS. SAFE_MODE blocks new entries but keeps the
        monitor running so existing positions can still close. Decomposed
        into _compute_today_pnl + _should_kill for testability.
        """
        try:
            from core.logger import log_event
            if not hasattr(self, "safe_mode") or self.safe_mode is None:
                return  # bot not fully initialized yet
            if self.safe_mode.is_active():
                return  # already tripped  no need to re-check

            total_today = self._compute_today_pnl(trades)
            max_loss = float(self.C("MAX_DAILY_LOSS", -50.0))

            if self._should_kill(total_today, max_loss):
                log_event(
                    f" KILLSWITCH: daily loss {total_today:+.2f} USDT "
                    f"<= {max_loss} USDT  entering SAFE_MODE",
                    "WARN"
                )
                self.safe_mode.trigger(
                    f"daily-loss killswitch ({total_today:+.2f} USDT)"
                )
        except Exception as e:
            self._log_error("spot killswitch check", e)

    def _compute_today_pnl(self, trades: dict) -> float:
        """Return today's realized PnL + estimated unrealized for spot
        positions OPENED TODAY. Pure-ish  only reads DB + state.

        Only positions opened today count toward the daily-loss gate: an older
        underwater position is still protected by its own SL/trailing in the
        monitor, and counting its full unrealized here could pin SAFE_MODE and
        freeze all new entries indefinitely. Realized PnL from closing an old
        position today IS included (it's a today event) via get_today_pnl.

        Price fallback is strictly last_price  buy (``highest`` would overstate
        value after a reversal). The subtracted entry fee is the REMAINING
        proportional share (mirroring the full-exit booking)  not ``fees_paid``,
        which also carries already-realized partial-close fees and would
        double-count.
        """
        from core.database import get_today_pnl, opened_today_local
        pnl_info = get_today_pnl(self.BOT_NAME, mode_is_sim=self.simulation)
        today_realized = _finite_float(pnl_info.get("total_profit", 0.0))

        unrealized = 0.0
        for sym, d in trades.items():
            try:
                # Only positions opened today affect the daily gate. buy_time is
                # UTC, the daily bucket is local (BOT_TIMEZONE)  compare in one
                # frame via opened_today_local, not a naive UTC-string prefix.
                if not opened_today_local(d.get("buy_time", "")):
                    continue
                buy = _positive_finite(d.get("buy"))
                amt = _positive_finite(d.get("amount"))
                # last_price first; fall back to buy (NOT highest).
                last = _positive_finite(d.get("last_price"))
                if last <= 0:
                    last = buy
                if buy <= 0 or amt <= 0 or last <= 0:
                    continue

                # Gross unrealized move
                gross = amt * (last - buy)

                # Remaining proportional ENTRY fee  same basis the real close
                # uses (safe_proportional_fee), so partial-sold positions don't
                # re-subtract the already-realized partial-close fee.
                initial_entry_fee = _positive_finite(
                    d.get("initial_entry_fee", d.get("fees_paid", 0.0)))
                original_amount = _positive_finite(
                    d.get("original_amount"), amt)
                remaining_entry_fee = safe_proportional_fee(
                    initial_entry_fee, amt, original_amount,
                    partial_sold=bool(d.get("partial_sold")),
                )
                # If we close NOW, we'd pay this taker fee.
                close_fee_est = amt * last * _UNREALIZED_CLOSE_FEE_RATE

                unrealized += gross - remaining_entry_fee - close_fee_est
            except (TypeError, ValueError, OverflowError):
                continue
        total = today_realized + unrealized
        return total if math.isfinite(total) else today_realized

    @staticmethod
    def _should_kill(total_today_pnl: float, max_daily_loss: float) -> bool:
        """Pure decision: should we trip the killswitch?

        max_daily_loss is negative (e.g. -50.0). Trip when total  limit.
        """
        return total_today_pnl <= max_daily_loss

    #  Helpers 

    def _batch_tickers(self, symbols: list) -> dict:
        """Batched fetch_tickers  chunked to TICKER_BATCH_SIZE per call to
        keep each URL under the HTTP 414 (URI Too Long) limit.

        On a chunk failure the missing symbols are simply absent from the
        result  _get_current_price then does a per-symbol fetch for them.
        The atomic API budget gate runs before each exchange call.
        """
        if not symbols:
            return {}
        out: dict = {}
        try:
            from bot_utils.api_budget import record_api_error, try_consume_api_call
        except ImportError:
            def record_api_error(**kw):
                return None

            def try_consume_api_call(*a, **kw):
                return True

        pairs = [f"{s}/USDT" for s in symbols]
        for i in range(0, len(pairs), TICKER_BATCH_SIZE):
            chunk = pairs[i:i + TICKER_BATCH_SIZE]
            try:
                if not try_consume_api_call("fetch_tickers", critical=True):
                    continue
                result = self.ex.fetch_tickers(chunk) or {}
                out.update(result)
            except Exception as e:
                try:
                    record_api_error(endpoint="fetch_tickers")
                except Exception:
                    pass
                if self._is_rate_limited(e):
                    self._set_ticker_backoff()
                # Continue with whatever we have  _get_current_price
                # will fall back to per-symbol fetches for missing ones.
        return out

    def _is_rate_limited(self, exc: BaseException) -> bool:
        try:
            from bot_utils.network_retry import is_rate_limited
            return is_rate_limited(exc)
        except Exception:
            s = str(exc).lower()
            return ("429" in s or "too frequent" in s
                    or "too many requests" in s or "rate limit" in s)

    def _set_ticker_backoff(self, seconds: float = 12.0) -> None:
        until = time.monotonic() + seconds
        self._ticker_backoff_until = max(
            until, float(getattr(self, "_ticker_backoff_until", 0.0) or 0.0))

    def _ticker_backoff_active(self) -> bool:
        return time.monotonic() < float(
            getattr(self, "_ticker_backoff_until", 0.0) or 0.0)

    def _note_spot_price_unavailable(self, sym: str, log_event) -> None:
        counts = getattr(self, "_price_unavail_counts", None)
        if counts is None:
            counts = {}
            self._price_unavail_counts = counts
        counts[sym] = counts.get(sym, 0) + 1
        if counts[sym] in (1, 5) or counts[sym] % 10 == 0:
            log_event(f"Price for {sym} unavailable", "WARN")

    def _clear_spot_price_unavailable(self, sym: str) -> None:
        counts = getattr(self, "_price_unavail_counts", None)
        if counts:
            counts.pop(sym, None)

    def _maybe_persist_last_price(self, sym: str, curr: float) -> None:
        """Persist last_price only every LAST_PRICE_PERSIST_INTERVAL_SEC, not
        every tick.

        The killswitch reads the in-memory ``trades`` dict (refreshed every tick
        by the caller); disk persistence is only used by the Launcher Stop-Dialog
        and crash recovery, both of which tolerate that staleness. The
        ``_last_price_persist_at`` dict is lazily initialised  this mixin
        doesn't own __init__.
        """
        now = time.monotonic()
        if not hasattr(self, "_last_price_persist_at"):
            self._last_price_persist_at = {}
        prev = self._last_price_persist_at.get(sym, 0.0)
        if (now - prev) < LAST_PRICE_PERSIST_INTERVAL_SEC:
            return  # too soon  skip disk write
        self._last_price_persist_at[sym] = now
        try:
            self.state.update(sym, "last_price", curr)
        except Exception:
            # Persist failures are non-fatal  in-memory state is the
            # source of truth for this tick.
            pass

    def _get_current_price(self, sym: str, batch: dict,
                           position: dict | None = None) -> float:
        """Get last price for sym  use batch first, fall back to per-symbol."""
        pair = f"{sym}/USDT"
        t = batch.get(pair)
        if isinstance(t, dict):
            v = safe_positive_float(t.get("last"), 0.0)
            if v <= 0:
                v = safe_positive_float(t.get("close"), 0.0)
            if v > 0:
                return v
        if self._ticker_backoff_active():
            return 0.0
        try:
            from bot_utils.api_budget import try_consume_api_call
            if not try_consume_api_call("fetch_ticker", critical=True):
                return 0.0
            ticker = self.ex.fetch_ticker(pair) or {}
            result = safe_positive_float(ticker.get("last"), 0.0)
            if result <= 0:
                result = safe_positive_float(ticker.get("close"), 0.0)
            return result
        except Exception as e:
            try:
                from bot_utils.api_budget import record_api_error
                record_api_error(endpoint="fetch_ticker")
            except Exception:
                pass
            if self._is_rate_limited(e):
                self._set_ticker_backoff()
            return 0.0

    def _btc_stress_override(self, trades: dict) -> None:
        """BTC dump protection  force break-even on profitable positions
        when BTC drops fast. Spot can't go negative but profits evaporate.

        Defensive against API failures: if get_btc_change() returns None or
        exactly 0 (API down / no data), we do NOT treat it as stress, so a
        glitch can't force every profitable position to break-even. Uses a
        batch fetch_tickers rather than N single-ticker calls.
        """
        from core.logger import log_event
        from trading.market_filters import get_btc_change
        try:
            try:
                # closed_only: BTC-stress is a safety gate (forces positions to
                # break-even)  don't let a forming-candle wick trigger it.
                btc_1h = get_btc_change(self.ex, hours=1, closed_only=True)
                btc_24h = get_btc_change(self.ex, hours=24, closed_only=True)
            except Exception as e:
                log_event(
                    f"BTC-Stress check skipped  BTC data unavailable: {e}",
                    "WARN"
                )
                return

            # Explicit None/0 handling: None means "no data"; exactly 0.0 on
            # both windows is implausibly precise  treat as a data gap, not a
            # real stress trigger.
            if btc_1h is None or btc_24h is None:
                return
            try:
                btc_1h = float(btc_1h)
                btc_24h = float(btc_24h)
            except (TypeError, ValueError):
                return
            if btc_1h == 0.0 and btc_24h == 0.0:
                return  # both exact zero  almost certainly API gap

            if btc_1h >= -1.5 and btc_24h >= -3.0:
                return

            # Stress active  chunked batch-fetch all open symbols.
            candidates = [(sym, d) for sym, d in trades.items()
                          if not d.get("break_even")]
            if not candidates:
                return
            batch = self._batch_tickers([sym for sym, _ in candidates])

            for sym, d in candidates:
                try:
                    curr = self._get_current_price(sym, batch, d)
                    if curr <= 0:
                        continue
                    if curr > d.get("buy", 0):
                        self.state.update(sym, "break_even", True)
                        log_event(
                            f" BTC stress (1h: {btc_1h:+.2f}%)  "
                            f"{sym} forced to break-even", "WARN"
                        )
                except Exception as e:
                    self._log_error(f"BTC override per-symbol {sym}", e)
        except Exception as e:
            self._log_error("BTC override check", e)

    #  Exit decision for one position 

    def _check_position_exits(self, sym: str, d: dict, curr: float) -> None:
        """Run all exit checks for one position.

        Order matters:
          1. Update highest_price
          2. Activate breakeven if profit  BREAKEVEN_TRIGGER
          3. Partial-TP at ACTIVATION_PROFIT
          4. Full exit: trailing stop / hard stop / break-even stop
        """
        from core.logger import log_event

        if d.get("accounting_already_booked"):
            ExitsMixin._cleanup_accounted_close_state(self, sym, d)
            return
        if d.get("accounting_pending"):
            try:
                from core.spot_bot_reconcile import _record_spot_offline_close
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
                            or "Offline close"),
                    })
                    ExitsMixin._cleanup_accounted_close_state(self, sym, booked)
            except Exception as exc:
                self._log_error(f"spot pending accounting retry {sym}", exc)
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

        buy = _positive_finite(d.get("buy"))
        curr = _positive_finite(curr)
        if buy <= 0 or curr <= 0:
            return
        d["buy"] = buy
        prof = ((curr - buy) / buy) * 100

        # Keep the in-memory ``d["last_price"]`` fresh every tick (all the
        # killswitch needs); persist to disk only every
        # LAST_PRICE_PERSIST_INTERVAL_SEC.
        self._maybe_persist_last_price(sym, curr)
        d["last_price"] = curr

        # Update highest
        if curr > d.get("highest", buy):
            self.state.update(sym, "highest", curr)
            d["highest"] = curr  # keep local copy in sync for later math

        if d.get("closing_retry_pending"):
            self._execute_full_exit(
                sym, d, curr, d.get("closing_retry_reason") or "Close Retry")
            return

        highest = d.get("highest", buy)
        high_prof = ((highest - buy) / buy) * 100

        # Breakeven activation
        be_trigger = float(self.C("BREAKEVEN_TRIGGER", 0))
        if (be_trigger > 0
                and not d.get("be_active")
                and prof >= be_trigger):
            be_price = round(buy * (1.0 + _BE_FEE_BUFFER), 8)
            self.state.update_many(sym, {"be_active": True, "be_price": be_price})
            log_event(
                f" BREAKEVEN activated: {sym} @ +{prof:.2f}%  "
                f"SL at {be_price:.6f} (entry {buy:.6f} +fee buffer)",
                "INFO"
            )
            d["be_active"] = True
            d["be_price"] = be_price

        # Partial Take-Profit
        activation_profit = float(self.C("ACTIVATION_PROFIT"))
        partial_blocked_until = _finite_float(
            d.get("partial_tp_blocked_min_notional_until"), 0.0)
        partial_block_active = (
            bool(d.get("partial_tp_blocked_min_notional"))
            and time.time() < partial_blocked_until
        )
        if (not d.get("partial_sold")
                and not partial_block_active
                and prof >= activation_profit):
            handled = self._execute_partial_tp(sym, d, curr)
            if handled:
                return  # state already mutated

        # Full Exit decision
        sell_trigger, reason = self._evaluate_full_exit(d, curr, prof, high_prof)
        if sell_trigger:
            self._execute_full_exit(sym, d, curr, reason)

    def _evaluate_full_exit(self, d: dict, curr: float,
                             prof: float, high_prof: float):
        """Pure decision function: return (should_sell, reason)."""
        initial_sl = _finite_float(self.C("INITIAL_STOP_LOSS"))
        activation_profit = _finite_float(self.C("ACTIVATION_PROFIT"))
        trailing_dist = _positive_finite(self.C("TRAILING_DISTANCE"))
        post_partial_trailing_dist = _positive_finite(
            self.C("POST_PARTIAL_TRAILING_DISTANCE", trailing_dist),
            trailing_dist)
        if (post_partial_trailing_dist <= 0.0
                or (activation_profit > 0.0
                    and post_partial_trailing_dist >= activation_profit)):
            post_partial_trailing_dist = trailing_dist
        buy = _positive_finite(d.get("buy"))
        highest = _positive_finite(d.get("highest"), buy)

        # be_active: BREAKEVEN_TRIGGER fired  SL tightened to entry + fee buffer
        if d.get("be_active") and not d.get("break_even"):
            be_price = _positive_finite(d.get("be_price"), buy)
            if curr <= be_price:
                return True, "Break-Even Stop"
            if prof <= initial_sl:
                return True, "Stop-Loss"

        if d.get("break_even"):
            if curr <= highest * (1 - post_partial_trailing_dist / 100):
                return True, "Trailing Stop"
            if curr <= buy:
                return True, "Break-Even Stop"
        else:
            if (high_prof >= activation_profit
                    and curr <= highest * (1 - trailing_dist / 100)):
                return True, "Trailing Stop"
            if prof <= initial_sl:
                return True, "Stop-Loss"

        # Time-stop (opt-in, lowest priority): nothing above fired but the
        # position is stale and never reached the trailing/profit-lock state 
        # close it to free capital from chop / slow-bleed. Runners are left
        # alone (already break_even, or prof past ACTIVATION_PROFIT). Disabled
        # by default (MAX_HOLD_HOURS=0); enabling it only ADDS exits for stuck
        # positions, so it frees capital for new entries rather than cutting
        # frequency.
        try:
            max_hold_h = _finite_float(self.C("MAX_HOLD_HOURS", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            max_hold_h = 0.0
        if (max_hold_h > 0 and not d.get("break_even")
                and prof < activation_profit):
            age_h = _position_age_hours(d)
            if age_h is not None and age_h >= max_hold_h:
                return True, "Max Hold Time"

        return False, ""

    #  Partial TP execution 

    def _execute_partial_tp(self, sym: str, d: dict, curr: float) -> bool:
        """Execute partial take-profit. Returns True if any state mutation
        occurred (skip further exit checks this tick).

        Acquires close_lock for the whole critical section (via a ``with``
        block, so it's always released)  otherwise emergency_close_all_spot
        could fire a concurrent sell against the same wallet balance and one
        attempt would hit InsufficientBalance.
        """
        from core.logger import log_event
        try:
            from core.symbol_locks import close_lock
        except ImportError:
            close_lock = None

        partial_pct = _finite_float(self.C("PARTIAL_SELL_PCT"))
        if not 0 < partial_pct < 1:
            log_event(
                f" {sym}: invalid PARTIAL_SELL_PCT={partial_pct!r}  "
                f"partial-TP skipped",
                "WARN",
            )
            return False
        amount = _positive_finite(d.get("amount"))
        invested = _positive_finite(d.get("invested_usdt"))
        curr = _positive_finite(curr)
        if amount <= 0 or invested <= 0 or curr <= 0:
            log_event(
                f" {sym}: invalid partial-TP basis "
                f"(amount={d.get('amount')!r}, "
                f"invested={d.get('invested_usdt')!r}, price={curr!r})  "
                f"skipped",
                "WARN",
            )
            return False

        # Scale the partial up if the slice would be too small (rather than
        # skipping), and read per-symbol min-notional from exchange markets
        # metadata (it differs by exchange/symbol) instead of the hardcoded
        # buffer.
        try:
            markets = getattr(self.ex, "markets", {}) or {}
            mkt = markets.get(f"{sym}/USDT") or {}
            cost_min = ((mkt.get("limits") or {}).get("cost") or {}).get("min")
            min_notional = _positive_finite(cost_min, MIN_NOTIONAL_BUFFER)
        except Exception:
            min_notional = MIN_NOTIONAL_BUFFER

        sld_test = amount * partial_pct
        rem_test = amount - sld_test
        if (sld_test * curr < min_notional
                or rem_test * curr < min_notional):
            adjusted = False
            for try_pct in (0.40, 0.50, 0.60):
                if try_pct <= partial_pct:
                    continue
                try_sld = amount * try_pct
                try_rem = amount - try_sld
                if (try_sld * curr >= min_notional
                        and try_rem * curr >= min_notional):
                    log_event(
                        f"{sym}: partial-TP slice too small at "
                        f"{int(partial_pct*100)}% (slice={sld_test*curr:.2f}), "
                        f"adjusting to {int(try_pct*100)}% "
                        f"(slice={try_sld*curr:.2f}, rem={try_rem*curr:.2f})",
                        "INFO"
                    )
                    partial_pct = try_pct
                    sld_test = try_sld
                    rem_test = try_rem
                    adjusted = True
                    break
            if not adjusted:
                log_event(
                    f" {sym}: partial-TP impossible (pos too small): "
                    f"amount_notional={amount*curr:.2f}, "
                    f"min_per_side={min_notional:.2f}  skipping, going trailing",
                    "WARN"
                )
                self.state.update_many(sym, {
                    "partial_tp_blocked_min_notional": True,
                    "partial_tp_blocked_min_notional_until": time.time() + 300.0,
                    "break_even": True,
                })
                return True

        # Acquire close_lock before any sell-side state change.
        if close_lock is None:
            # No lock module available  fall through (best-effort).
            return self._execute_partial_tp_unlocked(
                sym, d, curr, partial_pct, sld_test
            )

        with close_lock(sym, timeout=2.0, bot_name=self.BOT_NAME) as got:
            if not got:
                # Another path (probably emergency-close) holds the lock.
                # Skip this partial-TP  we'll either retry next tick OR
                # the position will have been fully closed by then.
                log_event(
                    f" {sym}: partial-TP skipped  close_lock contended "
                    f"(emergency close likely in progress)", "WARN"
                )
                return False

            # Re-check after acquiring: maybe state was already mutated
            # by the lock-holder (e.g. emergency-close marked sold).
            d_live = self.state.get(sym)
            live_blocked_until = _finite_float(
                (d_live or {}).get("partial_tp_blocked_min_notional_until"), 0.0)
            live_block_active = (
                bool((d_live or {}).get("partial_tp_blocked_min_notional"))
                and time.time() < live_blocked_until
            )
            if (d_live is None or d_live.get("partial_sold")
                    or live_block_active):
                return False
            try:
                live_amount = _positive_finite(d_live.get("amount"))
            except (TypeError, ValueError):
                live_amount = 0.0
            live_invested = _positive_finite(d_live.get("invested_usdt"))
            live_sld_test = live_amount * partial_pct
            live_rem_test = live_amount - live_sld_test
            if live_amount <= 0 or live_invested <= 0:
                log_event(
                    f" {sym}: invalid live partial-TP state "
                    f"(amount={d_live.get('amount')!r}, "
                    f"invested={d_live.get('invested_usdt')!r})  skipped",
                    "WARN",
                )
                return False
            if (live_sld_test * curr < min_notional
                    or live_rem_test * curr < min_notional):
                self.state.update_many(sym, {
                    "partial_tp_blocked_min_notional": True,
                    "partial_tp_blocked_min_notional_until": time.time() + 300.0,
                    "break_even": True,
                })
                return True

            return self._execute_partial_tp_unlocked(
                sym, d_live, curr, partial_pct, live_sld_test
            )

    def _execute_partial_tp_unlocked(self, sym: str, d: dict, curr: float,
                                       partial_pct: float,
                                       sld_test: float) -> bool:
        """Inner partial-TP  assumes caller already holds close_lock(sym).

        Body matches the original _execute_partial_tp post-min-notional-check
        portion verbatim; split out so the lock-acquisition path is clean.
        """
        from core.logger import log_event, send_telegram
        from core.database import save_trade_db
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        buy = _positive_finite(d.get("buy"))
        amount = _positive_finite(d.get("amount"))
        current_invested = _positive_finite(d.get("invested_usdt"))
        curr = _positive_finite(curr)
        sld_test = _positive_finite(sld_test)
        if buy <= 0 or amount <= 0 or current_invested <= 0 or curr <= 0:
            log_event(
                f" {sym}: invalid partial-TP state "
                f"(buy={d.get('buy')!r}, amount={d.get('amount')!r}, "
                f"invested={d.get('invested_usdt')!r}, price={curr!r})  "
                f"skipped",
                "WARN",
            )
            return False

        # Execute the partial sell
        fill_price = curr
        sold_amount = sld_test
        partial_fee = 0.0
        exch_oid = None  # real order id  unique trade-dedup key
        if self.simulation:
            if sold_amount > 0 and fill_price > 0:
                partial_fee = sold_amount * fill_price * 0.001
        else:
            try:
                order, sold_amount = spot_market_sell_safe(
                    self.ex, f"{sym}/USDT", amount * partial_pct
                )
                # Phantom-fill guard: a market sell that returns
                # status=new/filled=0 (MEXC reject) must NOT be booked as a
                # partial-TP  that would shrink the tracked `amount`, mark
                # partial_sold=True and arm break-even while the coins are still
                # in the wallet. Don't mutate state; return False so the position
                # is re-evaluated next tick.
                from bot_utils.order_utils import order_was_filled
                if not order_was_filled(order, sold_amount):
                    _st = order.get("status") if isinstance(order, dict) else "?"
                    log_event(
                        f" {sym}: partial-TP order did NOT fill "
                        f"(status={_st})  NOT booking, retry next tick", "WARN")
                    return False
                sold_amount = _filled_base_amount(order, sold_amount, sold_amount)
                exch_oid = order.get("id") or order.get("orderId")
                fill_price = _positive_finite(
                    extract_fill_price(order, curr), curr)
                try:
                    from trading.fee_utils import extract_or_estimate_with_refetch
                    partial_fee = extract_or_estimate_with_refetch(
                        self.ex, order, f"{sym}/USDT", fill_price,
                        base_override=sym
                    )
                except Exception:
                    partial_fee = extract_order_fee(order)
            except Exception as e:
                log_event(f"Partial-Sell {sym} failed: {e}", "WARN")
                return False

        # Compute PnL with proportional entry fee
        real_prof_pct = ((fill_price - buy) / buy) * 100 if buy > 0 else 0.0
        initial_entry_fee = _positive_finite(
            d.get("initial_entry_fee", d.get("fees_paid", 0.0)))
        original_amount = _positive_finite(d.get("original_amount"), amount)
        # Safe proportional fee  won't double-deduct if state corrupted
        prop_entry_fee = safe_proportional_fee(
            initial_entry_fee, sold_amount, original_amount,
            partial_sold=False   # this IS the partial event
        )
        profit_partial = round(
            sold_amount * (fill_price - buy) - prop_entry_fee - partial_fee, 2
        )

        sold_invested = sold_amount * buy
        slice_fees_total = prop_entry_fee + partial_fee

        partial_trade = dict(
            bot_name=self.BOT_NAME,
            mode_is_sim=self.simulation,
            symbol=sym,
            buy_price=buy, sell_price=fill_price,
            buy_time=d.get("buy_time", ""),
            sell_time=_utc_now_str(),
            profit_pct=real_prof_pct, profit_usdt=profit_partial,
            invested_usdt=sold_invested, reason="Partial Take-Profit",
            rsi_15m=d.get("rsi_15m"), rsi_1h=d.get("rsi_1h"),
            rsi_4h=d.get("rsi_4h"), change_pct=d.get("change_pct"),
            is_partial=True, btc_trend=d.get("btc_trend"),
            fear_greed=d.get("fear_greed"),
            fees_usdt=slice_fees_total,
            exchange_order_id=exch_oid,
            entry_quality_score=d.get("entry_quality_score"),
            entry_quality_label=d.get("entry_quality_label"),
            entry_quality_reasons=d.get("entry_quality_reasons"),
        )
        try:
            accounting_ok = bool(save_trade_db(**partial_trade))
        except Exception as e:
            accounting_ok = False
            self._log_error(f"spot partial save_trade_db {sym}", e)

        # Update state
        new_invested = max(0.0, current_invested - sold_invested)
        new_amount = safe_remaining(amount, sold_amount)
        new_fees_paid = _positive_finite(d.get("fees_paid", 0.0)) + partial_fee
        updates = {
            "partial_sold": True,
            "invested_usdt": new_invested,
            "amount": new_amount,
            "fees_paid": new_fees_paid,
            "break_even": True,
            "partial_tp_blocked_min_notional": False,
            "partial_tp_blocked_min_notional_until": 0.0,
        }
        if not accounting_ok:
            from bot_utils.trade_state import normalize_pending_accounting_items
            pending = normalize_pending_accounting_items(
                d.get("accounting_pending_partials"))
            pending.append(partial_trade)
            updates["accounting_pending_partials"] = pending
            log_event(
                f" {sym}: partial-TP DB save failed  slice kept "
                f"for accounting retry", "WARN")
        self.state.update_many(sym, updates)

        log_event(
            f"[{self.BOT_NAME}]  {int(partial_pct*100)}% von {sym} bei "
            f"+{real_prof_pct:.2f}% (+{profit_partial:.2f} USDT, "
            f"fee {partial_fee:.3f})  Stop on Break-Even",
            "WIN"
        )
        try:
            if not self.simulation:
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f" [{self.BOT_NAME}] PARTIAL TAKE-PROFIT {sym}\n"
                    f"{int(partial_pct*100)}% @ {fill_price:.6f} "
                    f"(+{real_prof_pct:.2f}%, +{profit_partial:.2f} USDT)\n"
                    f"Stop: Break-Even"
                )
        except Exception as e:
            log_event(f"Telegram failed: {e}", "WARN")
        return True

    #  Full exit execution 

    def _execute_full_exit(self, sym: str, d: dict, curr: float, reason: str) -> None:
        """Execute the full close  sell remaining amount, write DB rows,
        set SL cooldown, remove from state. Uses a per-symbol close lock via a
        ``with`` statement so the lock is always released, even on early raises.
        """
        from core.logger import log_event
        try:
            from core.symbol_locks import close_lock
        except ImportError:
            close_lock = None

        if close_lock is None:
            # No lock module available  do the work directly (best-effort).
            self._execute_full_exit_inner(sym, d, curr, reason)
            return

        with close_lock(sym, timeout=5.0, bot_name=self.BOT_NAME) as got:
            if not got:
                log_event(
                    f"Could not acquire close lock for {sym} in 5s  "
                    f"another thread is closing it", "WARN"
                )
                return
            self._execute_full_exit_inner(sym, d, curr, reason)

    def _execute_full_exit_inner(self, sym: str, d: dict, curr: float, reason: str) -> None:
        """Inner full-exit body  assumes caller already holds close_lock(sym).
        Split out so the lock-acquisition path stays clean.
        """
        from core.logger import log_event, log_sell, send_telegram, save_trade
        from core.database import save_trade_db
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from trading.risk_manager import analyze_and_adapt
        try:
            from core.symbol_locks import release_lock
        except ImportError:
            release_lock = None

        # Re-check position still exists
        d_live = self.state.get(sym)
        if d_live is None:
            return
        # Work from the freshly re-read live state, not the snapshot captured
        # before the lock  a partial-TP that ran in between could otherwise
        # make us sell a stale `amount`.
        d = d_live

        buy = _positive_finite(d.get("buy"))
        remaining_amount = _positive_finite(d.get("amount"))
        current_invested = _positive_finite(d.get("invested_usdt"))
        if remaining_amount <= 0:
            log_event(
                f" {sym}: invalid spot close amount in state "
                f"({d.get('amount')!r})  keeping position for reconcile",
                "WARN",
            )
            return
        if buy <= 0 or current_invested <= 0:
            log_event(
                f" {sym}: invalid spot cost basis in state "
                f"(buy={d.get('buy')!r}, invested={d.get('invested_usdt')!r}) "
                f" keeping position for manual/reconcile review",
                "WARN",
            )
            return

        fill_price = _positive_finite(curr, buy)
        close_fee = 0.0
        requested_amount = remaining_amount
        partial_live_fill = False
        exch_oid = None  # real order id  unique trade-dedup key
        if self.simulation:
            if remaining_amount > 0 and fill_price > 0:
                close_fee = remaining_amount * fill_price * 0.001
        else:
            try:
                # Network retry  so a single transient API error (DNS, VPN
                # flap) doesn't abort the sell and hold the position an extra
                # tick. 3 attempts with backoff usually complete inside the
                # monitor-tick budget.
                from bot_utils.network_retry import with_network_retry
                order, sold = with_network_retry(
                    operation=lambda: spot_market_sell_safe(
                        self.ex, f"{sym}/USDT", remaining_amount
                    ),
                    action_label=f"sell {sym}",
                    max_attempts=3,
                    base_delay=0.5,
                    shutdown_event=self._shutdown_event,
                    log_event=log_event,
                )
                filled_amount = _filled_base_amount(order, sold, requested_amount)
                # Verify the sell ACTUALLY filled before booking a closed trade.
                # spot_market_sell_safe returns (order, amt) the instant
                # create_market_sell_order returns, but MEXC can return an order
                # with status=new/open and filled=0 that never executed
                # (remainder below min-notional, matching-engine reject). Keep
                # the position + a short cooldown so the next tick retries; do
                # NOT book and do NOT remove state.
                from bot_utils.order_utils import order_was_filled
                if not order_was_filled(order, filled_amount):
                    _st = order.get("status") if isinstance(order, dict) else "?"
                    log_event(
                        f" {sym}: sell order did NOT fill (status={_st}, "
                        f"coins still in wallet)  NOT booking, will retry "
                        f"next tick", "WARN")
                    try:
                        from trading.cooldown_utils import set_cooldown as _scd
                        with self._cooldown_lock:
                            _scd(self.cool, sym, 15, self.COOLDOWN_FILE)
                    except Exception as ce:
                        self._log_error(f"sell-unfilled cooldown set {sym}", ce)
                    return
                exch_oid = order.get("id") or order.get("orderId")
                fill_price = _positive_finite(
                    extract_fill_price(order, fill_price), fill_price)
                residual_amount = safe_remaining(requested_amount, filled_amount)
                if residual_amount * fill_price > _LIVE_RESIDUAL_DUST_USDT:
                    partial_live_fill = True
                try:
                    # max_attempts=0  NO blocking fetch_order re-fetch on the
                    # close critical path (it would sleep up to 0.8s WHILE
                    # HOLDING close_lock, compounding across sequential closes
                    # during a crash). For fixed-rate spot taker fees the
                    # estimate equals the real fee within rounding.
                    from trading.fee_utils import extract_or_estimate_with_refetch
                    close_fee = extract_or_estimate_with_refetch(
                        self.ex, order, f"{sym}/USDT", fill_price,
                        base_override=sym, max_attempts=0
                    )
                except Exception:
                    close_fee = extract_order_fee(order)
                remaining_amount = filled_amount
            except InsufficientSellBalance as ib:
                # The BASE coins are not actually on the exchange (already sold,
                # or only dust below the min lot). Selling will never succeed,
                # but dropping state before accounting can lose realized PnL
                # after a manual/external sell.
                log_event(
                    f"{sym}: nothing left to sell ({ib})  booking offline "
                    f"close before removing state", "WARN")
                try:
                    from core.spot_bot_reconcile import _record_spot_offline_close
                    if not _record_spot_offline_close(self, sym, d):
                        log_event(
                            f"{sym}: orphan accounting failed  state kept "
                            f"for retry", "WARN")
                        return
                except Exception as acc_err:
                    self._log_error(f"spot orphan accounting {sym}", acc_err)
                    return
                try:
                    if not self.simulation:
                        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                            f" [{self.BOT_NAME}] {sym} orphan cleared\n"
                            f"Base balance ~0 on exchange  offline close booked "
                            f"and position removed from tracking."
                        )
                except Exception:
                    pass
                cleanup_row = dict(d)
                cleanup_row.update({
                    "accounting_already_booked": True,
                    "accounting_booked_reason": "Offline orphan close",
                })
                ExitsMixin._cleanup_accounted_close_state(self, sym, cleanup_row)
                return
            except Exception as e:
                log_event(f"Sell order {sym} failed: {e}", "WARN")
                # short cooldown to prevent retry-spam
                try:
                    from trading.cooldown_utils import set_cooldown as _scd
                    with self._cooldown_lock:
                        _scd(self.cool, sym, 15, self.COOLDOWN_FILE)
                except Exception as ce:
                    self._log_error(f"sell-fail cooldown set {sym}", ce)
                return

        real_prof_pct = ((fill_price - buy) / buy) * 100 if buy > 0 else 0.0

        # Proportional entry fee  safe helper won't double-deduct if
        # original_amount is missing AND a partial-TP already executed.
        initial_entry_fee = _positive_finite(
            d.get("initial_entry_fee", d.get("fees_paid", 0.0)))
        original_amount = _positive_finite(
            d.get("original_amount"), remaining_amount)
        proportional_entry_fee = safe_proportional_fee(
            initial_entry_fee, remaining_amount, original_amount,
            partial_sold=bool(d.get("partial_sold"))
        )
        accumulated_fees = _positive_finite(d.get("fees_paid", 0.0)) + close_fee
        profit_usdt = round(
            remaining_amount * (fill_price - buy)
            - proportional_entry_fee - close_fee, 2
        )
        if partial_live_fill and requested_amount > 0:
            booked_invested = current_invested * (remaining_amount / requested_amount)
            booked_reason = f"{reason} (partial fill)"
        else:
            booked_invested = current_invested
            booked_reason = reason

        sell_time = _utc_now_str()
        accounting_mode_is_sim = d.get("accounting_pending_mode_is_sim",
                                       self.simulation)
        trade_row = dict(
            bot_name=self.BOT_NAME,
            mode_is_sim=accounting_mode_is_sim,
            symbol=sym,
            buy_price=buy, sell_price=fill_price,
            buy_time=d.get("buy_time", ""),
            sell_time=sell_time,
            profit_pct=real_prof_pct, profit_usdt=profit_usdt,
            invested_usdt=booked_invested, reason=booked_reason,
            rsi_15m=d.get("rsi_15m"), rsi_1h=d.get("rsi_1h"),
            rsi_4h=d.get("rsi_4h"), change_pct=d.get("change_pct"),
            btc_trend=d.get("btc_trend"), fear_greed=d.get("fear_greed"),
            fees_usdt=proportional_entry_fee + close_fee,
            exchange_order_id=exch_oid,
            entry_quality_score=d.get("entry_quality_score"),
            entry_quality_label=d.get("entry_quality_label"),
            entry_quality_reasons=d.get("entry_quality_reasons"),
        )
        if partial_live_fill:
            trade_row["is_partial"] = True
        try:
            accounting_ok = bool(save_trade_db(**trade_row))
        except Exception as e:
            accounting_ok = False
            self._log_error(f"spot full save_trade_db {sym}", e)
        if not accounting_ok:
            if partial_live_fill:
                remaining_after_fill = safe_remaining(
                    requested_amount, remaining_amount)
                from bot_utils.trade_state import normalize_pending_accounting_items
                pending = normalize_pending_accounting_items(
                    d.get("accounting_pending_partials"))
                pending.append(trade_row)
                try:
                    self.state.update_many(sym, {
                        "amount": remaining_after_fill,
                        "invested_usdt": max(0.0, current_invested - booked_invested),
                        "fees_paid": accumulated_fees,
                        "accounting_pending_partials": pending,
                        "closing_retry_pending": True,
                        "closing_retry_reason": reason,
                    })
                except Exception as state_err:
                    self._log_error(
                        f"spot mark partial accounting_pending {sym}", state_err)
                log_event(
                    f" {sym}: partial full-exit fill DB save failed - "
                    f"residual position kept for accounting retry", "WARN")
                return
            try:
                self.state.update_many(sym, {
                    "accounting_pending": True,
                    "accounting_pending_reason": reason,
                    "accounting_pending_sell_price": fill_price,
                    "accounting_pending_sell_time": sell_time,
                    "accounting_pending_profit_pct": real_prof_pct,
                    "accounting_pending_profit_usdt": profit_usdt,
                    "accounting_pending_mode_is_sim": self.simulation,
                    "accounting_pending_fees_usdt": proportional_entry_fee + close_fee,
                    "accounting_pending_exchange_order_id": exch_oid,
                })
            except Exception as state_err:
                self._log_error(f"spot mark accounting_pending {sym}", state_err)
            log_event(
                f" {sym}: full-exit DB save failed after filled sell  "
                f"state kept for accounting retry", "WARN")
            return
        if partial_live_fill:
            remaining_after_fill = safe_remaining(requested_amount, remaining_amount)
            self.state.update_many(sym, {
                "amount": remaining_after_fill,
                "invested_usdt": max(0.0, current_invested - booked_invested),
                "fees_paid": accumulated_fees,
                "last_partial_fill_reason": reason,
                "last_partial_fill_order_id": exch_oid,
                "closing_retry_pending": True,
                "closing_retry_reason": reason,
            })
            log_event(
                f" {sym}: full-exit order partially filled "
                f"({remaining_amount:.8f}/{requested_amount:.8f}); "
                f"residual kept in state", "WARN")
            return
        cleanup_row = dict(d)
        cleanup_row.update({
            "accounting_already_booked": True,
            "accounting_booked_sell_time": sell_time,
            "accounting_booked_exchange_order_id": exch_oid,
            "accounting_booked_reason": reason,
        })
        ExitsMixin._cleanup_accounted_close_state(self, sym, cleanup_row)
        try:
            save_trade(
                log_dir=self.LOG_DIR, symbol=sym,
                buy_price=buy, buy_time=d.get("buy_time", ""),
                sell_price=fill_price, profit_pct=real_prof_pct,
                profit_usdt=profit_usdt, reason=reason
            )
            log_sell(self.BOT_NAME, sym, real_prof_pct, profit_usdt, reason)
        except Exception as e:
            self._log_error(f"spot post-close logging {sym}", e)

        try:
            if not self.simulation:
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f"{'' if real_prof_pct >= 0 else ''} "
                    f"[{self.BOT_NAME}] SELL {sym}\n"
                    f"Profit: {'+' if real_prof_pct >= 0 else ''}"
                    f"{real_prof_pct:.2f}% ({profit_usdt:+.2f} USDT)\n"
                    f"Fees: {accumulated_fees:.3f} | Reason: {reason}"
                )
        except Exception as e:
            log_event(f"Telegram failed: {e}", "WARN")

        # cooldown via canonical helper (UTC, not local time). Arm on ANY
        # losing protective stop (SL / trailing / break-even), not only an exact
        # "Stop-Loss", so a coin chopping through the trailing/BE stop can't be
        # re-bought next tick (outcome-gated, shared classifier).
        from trading.cooldown_utils import should_cooldown_after_exit
        if should_cooldown_after_exit(reason, profit_usdt):
            from trading.cooldown_utils import set_cooldown as _scd
            with self._cooldown_lock:
                _scd(self.cool, sym, int(self.C("COOLDOWN_AFTER_SL", 120)),
                     self.COOLDOWN_FILE)

        try:
            analyze_and_adapt(self.BOT_NAME)
        except Exception as e:
            self._log_error("analyze_and_adapt", e)

        # Release the named lock (release_lock is idempotent). The `with` block
        # releases the context-manager acquire; this clears the per-symbol
        # registry entry.
        if release_lock is not None:
            try:
                release_lock(sym)
            except Exception:
                pass
