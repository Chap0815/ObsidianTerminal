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
    SpotSellOutcomeUnknown,
    extract_fill_price,
    extract_order_fee,
    safe_remaining,
    safe_proportional_fee,
    normalize_spot_order_status,
    spot_sell_requires_terminal_recovery,
)
from bot_utils.safe_numeric import safe_positive_float
from bot_utils.order_utils import order_id_text_or_none


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


def _ensure_spot_exit_client_order_id(bot, sym: str, row: dict,
                                      leg: str) -> str:
    """Persist one stable client id before a retryable live SPOT sell."""
    from bot_utils.spot_exits import ensure_spot_exit_client_order_id

    return ensure_spot_exit_client_order_id(
        bot.state, sym, row, leg, getattr(bot, "BOT_NAME", "SPOT")
    )


def _mark_spot_exit_outcome_uncertain(bot, sym: str, row: dict,
                                      leg: str) -> bool:
    key = f"{leg}_exit_outcome_uncertain"
    persisted = bot.state.update(
        sym, key, True
    )
    row[key] = True
    return persisted is not False


def _reset_spot_exit_intent(bot, sym: str, row: dict, leg: str) -> bool:
    updates = {
        f"{leg}_exit_client_order_id": None,
        f"{leg}_exit_outcome_uncertain": False,
        f"{leg}_exit_requested_amount": None,
    }
    persisted = bot.state.update_many(sym, updates)
    if persisted is not False:
        row.update(updates)
        return True
    return False


def _has_pending_spot_partial_exit(row: dict) -> bool:
    from bot_utils.spot_exits import has_pending_spot_partial_exit

    return has_pending_spot_partial_exit(row)


def _spot_excursion_metrics(d: dict, exit_price: float) -> tuple[float, float, float]:
    """Return finite SPOT MFE, MAE and close-time giveback percentages.

    SPOT positions are long-only. The actual fill participates in the extrema
    so close slippage is not hidden. Legacy rows without ``lowest`` start their
    adverse history at entry/the observed fill instead of inventing a historic
    low that was never recorded.
    """
    buy = _positive_finite(d.get("buy_price"), 0.0)
    if buy <= 0:
        buy = _positive_finite(d.get("buy"), 0.0)
    fill = _positive_finite(exit_price, buy)
    if buy <= 0 or fill <= 0:
        return 0.0, 0.0, 0.0

    highest = _positive_finite(d.get("highest"), buy)
    lowest = _positive_finite(d.get("lowest"), min(buy, fill))
    highest = max(buy, fill, highest)
    lowest = min(buy, fill, lowest)
    move_pct = ((fill - buy) / buy) * 100.0
    mfe_pct = max(0.0, ((highest - buy) / buy) * 100.0)
    mae_pct = min(0.0, ((lowest - buy) / buy) * 100.0)
    giveback_pct = max(0.0, mfe_pct - move_pct)
    return mfe_pct, mae_pct, giveback_pct


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

    def _spot_exit_shadow_enabled(self) -> bool:
        if str(getattr(self, "BOT_NAME", "")).upper() != "SPOT":
            return False
        try:
            from bot_utils.config import parse_explicit_bool

            return parse_explicit_bool(
                self.C("SPOT_EXIT_SHADOW_ENABLED", False)
            ) is True
        except Exception:
            return False

    def _record_spot_exit_shadow(
        self, sym: str, d: dict, *, move_pct: float,
        mfe_pct: float, mae_pct: float, now=None,
    ) -> None:
        """Persist first-hit SPOT counterfactuals without changing exits."""
        if not ExitsMixin._spot_exit_shadow_enabled(self):
            return
        if d.get("partial_sold") or d.get("break_even"):
            return
        try:
            from core.logger import log_struct
            from trading.spot_exit_shadow import evaluate_spot_exit_shadow_rules

            triggers = evaluate_spot_exit_shadow_rules(
                buy_time=d.get("buy_time"),
                move_pct=move_pct,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                now=now,
            )
            raw_seen = d.get("spot_exit_shadow_triggered_rules", [])
            if isinstance(raw_seen, (list, tuple, set)):
                seen = {str(item) for item in raw_seen if str(item)}
            elif isinstance(raw_seen, str):
                seen = {item for item in raw_seen.split(",") if item}
            else:
                seen = set()
            new_triggers = [
                item for item in triggers
                if str(item.get("rule")) not in seen
            ]
            if not new_triggers:
                return

            invested = _positive_finite(d.get("invested_usdt"), 0.0)
            for trigger in new_triggers:
                trigger_move = _finite_float(
                    trigger.get("trigger_move_pct"), move_pct
                )
                log_struct(
                    "spot_exit_shadow",
                    bot=self.BOT_NAME,
                    symbol=sym,
                    mode="SIM" if self.simulation else "LIVE",
                    entry_id=d.get("entry_id", ""),
                    entry_quality_score=d.get("entry_quality_score"),
                    entry_quality_label=d.get("entry_quality_label"),
                    gross_pnl_usdt=invested * trigger_move / 100.0,
                    actual_initial_stop_pct=self.C(
                        "INITIAL_STOP_LOSS", -100.0),
                    actual_activation_profit_pct=self.C(
                        "ACTIVATION_PROFIT", 0.0),
                    actual_trailing_distance_pct=self.C(
                        "TRAILING_DISTANCE", 0.0),
                    **trigger,
                )
                seen.add(str(trigger["rule"]))
            persisted = sorted(seen)
            self.state.update_many(
                sym, {"spot_exit_shadow_triggered_rules": persisted}
            )
            d["spot_exit_shadow_triggered_rules"] = persisted
        except Exception as exc:
            if not getattr(self, "_spot_exit_shadow_error_logged", False):
                self._spot_exit_shadow_error_logged = True
                try:
                    self._log_error(f"spot exit shadow {sym}", exc)
                except Exception:
                    pass

    def _retry_pending_partial_accounting(self, sym: str, d: dict) -> None:
        from bot_utils.trade_state import normalize_pending_accounting_items
        if not normalize_pending_accounting_items(
            d.get("accounting_pending_partials")
        ):
            return
        from core.database import save_trade_db
        from core.logger import log_event
        from core.symbol_locks import close_lock

        with close_lock(
            sym,
            timeout=2.0,
            bot_name=getattr(self, "BOT_NAME", "SPOT"),
        ) as got:
            if not got:
                return
            live = self.state.get(sym)
            if not isinstance(live, dict):
                return
            pending = normalize_pending_accounting_items(
                live.get("accounting_pending_partials"))
            if not pending:
                return
            try:
                durable = self.state.update_many(
                    sym, {"accounting_pending_partials": pending}
                )
            except Exception as exc:
                self._log_error(
                    f"spot partial accounting write-ahead {sym}", exc
                )
                return
            if durable is False:
                log_event(
                    f" {sym}: partial accounting state is not durable - "
                    f"DB retry deferred",
                    "ERROR",
                )
                return
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
            try:
                cleared = self.state.update(
                    sym, "accounting_pending_partials", remaining
                )
            except Exception as exc:
                cleared = False
                self._log_error(
                    f"spot partial accounting clear {sym}", exc
                )
            if cleared is False:
                log_event(
                    f" {sym}: partial accounting was booked but its durable "
                    f"pending marker could not be updated; idempotent retry "
                    f"retained",
                    "ERROR",
                )
                return
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
                    if self._check_daily_killswitch(trades) is not False:
                        last_killswitch = now

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
                    self._clear_spot_price_unavailable(sym, log_event)
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

    def _check_daily_killswitch(self, trades: dict) -> bool:
        """Daily-loss killswitch for SPOT bots.

        Triggers SAFE_MODE when today's realized + unrealized PnL falls
        below MAX_DAILY_LOSS. SAFE_MODE blocks new entries but keeps the
        monitor running so existing positions can still close. Decomposed
        into _compute_today_pnl + _should_kill for testability.
        """
        try:
            from core.logger import log_event
            if not hasattr(self, "safe_mode") or self.safe_mode is None:
                return True  # bot not fully initialized yet
            if self.safe_mode.is_active():
                return True  # already tripped  no need to re-check

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
            return True
        except Exception as e:
            self._log_error("spot killswitch check", e)
            return False

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
                partial_sold = bool(d.get("partial_sold"))
                initial_entry_fee = _positive_finite(
                    d.get(
                        "initial_entry_fee",
                        0.0 if partial_sold else d.get("fees_paid", 0.0),
                    )
                )
                original_amount = _positive_finite(d.get("original_amount"))
                if original_amount <= 0 and not partial_sold:
                    original_amount = amt
                remaining_entry_fee = safe_proportional_fee(
                    initial_entry_fee, amt, original_amount,
                    partial_sold=partial_sold,
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

        A symbol omitted by a successful chunk may use one direct fallback.
        A failed chunk must not fan out into one request per open position and
        amplify the same transport outage. The atomic API budget gate runs
        before each exchange call.
        """
        if not symbols:
            self._batch_ticker_failed_pairs = frozenset()
            return {}
        out: dict = {}
        failed_pairs: set[str] = set()
        pairs = [f"{s}/USDT" for s in symbols]
        try:
            from bot_utils.api_budget import record_api_error, try_consume_api_call
        except ImportError as exc:
            self._batch_ticker_failed_pairs = frozenset(pairs)
            try:
                self._log_error("spot ticker API budget import", exc)
            except Exception:
                pass
            return {}

        for i in range(0, len(pairs), TICKER_BATCH_SIZE):
            chunk = pairs[i:i + TICKER_BATCH_SIZE]
            reservation = None
            try:
                reservation = try_consume_api_call(
                    "fetch_tickers",
                    critical=True,
                    return_reservation=True,
                )
            except Exception as e:
                try:
                    self._log_error("spot batch ticker API budget", e)
                except Exception:
                    pass
                failed_pairs.update(chunk)
                continue
            if not reservation:
                failed_pairs.update(chunk)
                continue
            try:
                result = self.ex.fetch_tickers(chunk) or {}
                out.update(result)
            except Exception as e:
                failed_pairs.update(chunk)
                try:
                    record_api_error(
                        endpoint="fetch_tickers",
                        reservation=reservation,
                    )
                except Exception:
                    pass
                if self._is_rate_limited(e):
                    self._set_ticker_backoff()
        self._batch_ticker_failed_pairs = frozenset(failed_pairs)
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
        count = counts[sym]
        if count == 5 or count % 10 == 0:
            log_event(
                f"{sym}: price unavailable for {count} consecutive ticks - "
                f"exit protection is blind; monitoring continues",
                "WARN",
            )

    def _clear_spot_price_unavailable(self, sym: str, log_event=None) -> None:
        counts = getattr(self, "_price_unavail_counts", None)
        if counts:
            previous = counts.pop(sym, 0)
            if previous >= 5 and callable(log_event):
                log_event(
                    f"{sym}: price feed recovered after {previous} "
                    f"unavailable ticks; exit protection restored",
                    "OK",
                )

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
        failed_pairs = getattr(self, "_batch_ticker_failed_pairs", ())
        if pair in failed_pairs:
            return 0.0
        if self._ticker_backoff_active():
            return 0.0
        reservation = None
        try:
            from bot_utils.api_budget import try_consume_api_call
            reservation = try_consume_api_call(
                "fetch_ticker",
                critical=True,
                return_reservation=True,
            )
        except Exception as e:
            try:
                self._log_error("spot ticker API budget", e)
            except Exception:
                pass
            return 0.0
        if not reservation:
            return 0.0
        try:
            ticker = self.ex.fetch_ticker(pair) or {}
            result = safe_positive_float(ticker.get("last"), 0.0)
            if result <= 0:
                result = safe_positive_float(ticker.get("close"), 0.0)
            return result
        except Exception as e:
            try:
                from bot_utils.api_budget import record_api_error
                record_api_error(
                    endpoint="fetch_ticker",
                    reservation=reservation,
                )
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
        warned_incidents = getattr(
            self, "_verified_flat_accounting_incidents", None
        )
        if not isinstance(warned_incidents, dict):
            warned_incidents = {}
            self._verified_flat_accounting_incidents = warned_incidents
        if d.get("verified_flat_pending_accounting"):
            incident = (
                str(d.get("verified_flat_at") or ""),
                str(d.get("verified_flat_reason") or ""),
            )
            if warned_incidents.get(sym) != incident:
                log_event(
                    f"{sym}: position already verified flat; waiting for "
                    f"combined offline accounting",
                    "WARN",
                )
                warned_incidents[sym] = incident
            return
        warned_incidents.pop(sym, None)
        if d.get("accounting_pending"):
            from core.symbol_locks import close_lock
            try:
                with close_lock(
                    sym,
                    timeout=2.0,
                    bot_name=getattr(self, "BOT_NAME", "SPOT"),
                ) as acquired:
                    if not acquired:
                        return
                    live = self.state.get(sym)
                    if not isinstance(live, dict) or not live.get(
                        "accounting_pending"
                    ):
                        return
                    from core.spot_bot_reconcile import (
                        _defer_spot_accounting_retry,
                        _record_spot_offline_close,
                        _spot_accounting_retry_due,
                    )
                    if not _spot_accounting_retry_due(live):
                        return
                    pending_fields = {
                        key: value
                        for key, value in live.items()
                        if key == "accounting_pending"
                        or key.startswith("accounting_pending_")
                    }
                    durable = self.state.update_many(sym, pending_fields)
                    if durable is False:
                        log_event(
                            f" {sym}: pending full accounting retry deferred; "
                            f"recovery marker is not durable",
                            "ERROR",
                        )
                        return
                    if _record_spot_offline_close(self, sym, live):
                        booked = dict(live)
                        booked.update({
                            "accounting_already_booked": True,
                            "accounting_booked_sell_time": (
                                live.get("accounting_pending_sell_time")),
                            "accounting_booked_exchange_order_id": (
                                live.get("accounting_pending_exchange_order_id")),
                            "accounting_booked_reason": (
                                live.get("accounting_pending_reason")
                                or "Offline close"),
                        })
                        ExitsMixin._cleanup_accounted_close_state(
                            self, sym, booked
                        )
                    else:
                        _defer_spot_accounting_retry(self, sym, live)
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

        # Track both favorable and adverse excursion. Older positions may not
        # yet have ``lowest``; seed them from entry/current on the first tick.
        highest = _positive_finite(d.get("highest"), buy)
        lowest = _positive_finite(d.get("lowest"), min(buy, curr))
        extrema_updates = {}
        if curr > highest:
            highest = curr
            extrema_updates["highest"] = curr
        elif "highest" not in d:
            extrema_updates["highest"] = highest
        if curr < lowest:
            lowest = curr
            extrema_updates["lowest"] = curr
        elif "lowest" not in d:
            extrema_updates["lowest"] = lowest
        if extrema_updates:
            self.state.update_many(sym, extrema_updates)
            d.update(extrema_updates)

        if _has_pending_spot_partial_exit(d):
            self._execute_partial_tp(sym, d, curr)
            return

        if d.get("closing_retry_pending"):
            self._execute_full_exit(
                sym, d, curr, d.get("closing_retry_reason") or "Close Retry")
            return

        highest = _positive_finite(d.get("highest"), buy)
        high_prof = ((highest - buy) / buy) * 100
        lowest = _positive_finite(d.get("lowest"), min(buy, curr))
        low_prof = ((lowest - buy) / buy) * 100

        ExitsMixin._record_spot_exit_shadow(
            self,
            sym,
            d,
            move_pct=prof,
            mfe_pct=max(0.0, high_prof),
            mae_pct=min(0.0, low_prof),
        )

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
            live_after_partial = self.state.get(sym) or d
            if handled or _has_pending_spot_partial_exit(live_after_partial):
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
        from trading.profit_experiments import time_decay_decision

        age_h = _position_age_hours(d)
        if age_h is not None and not d.get("break_even"):
            max_age_minutes = _finite_float(
                self.C("TIME_DECAY_MAX_AGE_MINUTES", 360.0), 360.0
            )
            legacy_hours = _finite_float(self.C("MAX_HOLD_HOURS", 0) or 0)
            if legacy_hours > 0:
                max_age_minutes = legacy_hours * 60.0
            decay = time_decay_decision(
                age_minutes=age_h * 60.0,
                max_age_minutes=max_age_minutes,
                mfe_pct=high_prof,
                min_mfe_pct=_finite_float(
                    self.C("TIME_DECAY_MIN_MFE_PCT", 0.5), 0.5
                ),
                mode=str(self.C("TIME_DECAY_MODE", "shadow") or "shadow").lower(),
            )
            if decay.should_exit:
                return True, "Time Decay"

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
        from core.symbol_locks import close_lock

        amount = _positive_finite(d.get("amount"))
        invested = _positive_finite(d.get("invested_usdt"))
        curr = _positive_finite(curr)
        recovering = _has_pending_spot_partial_exit(d)
        if amount <= 0 or invested <= 0 or curr <= 0:
            log_event(
                f" {sym}: invalid partial-TP basis "
                f"(amount={d.get('amount')!r}, "
                f"invested={d.get('invested_usdt')!r}, price={curr!r})  "
                f"skipped",
                "WARN",
            )
            return False
        if recovering:
            durable_request = _positive_finite(
                d.get("partial_exit_requested_amount")
            )
            if durable_request <= 0:
                legacy_pct = _finite_float(self.C("PARTIAL_SELL_PCT"))
                if not 0 < legacy_pct < 1:
                    log_event(
                        f" {sym}: legacy pending partial-TP has no durable "
                        "amount and current PARTIAL_SELL_PCT is invalid",
                        "ERROR",
                    )
                    return False
                durable_request = amount * legacy_pct
                persisted = self.state.update(
                    sym, "partial_exit_requested_amount", durable_request
                )
                if persisted is False:
                    log_event(
                        f" {sym}: legacy partial-TP amount backfill is not "
                        "durable; recovery deferred",
                        "ERROR",
                    )
                    return False
                d["partial_exit_requested_amount"] = durable_request
            if durable_request > amount:
                log_event(
                    f" {sym}: pending partial-TP has invalid durable amount "
                    f"({d.get('partial_exit_requested_amount')!r})",
                    "ERROR",
                )
                return False
            partial_pct = durable_request / amount
        else:
            partial_pct = _finite_float(self.C("PARTIAL_SELL_PCT"))
            if not 0 < partial_pct < 1:
                log_event(
                    f" {sym}: invalid PARTIAL_SELL_PCT={partial_pct!r}  "
                    f"partial-TP skipped",
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

        sld_test = (
            _positive_finite(d.get("partial_exit_requested_amount"))
            if recovering else 0.0
        ) or amount * partial_pct
        rem_test = amount - sld_test
        if (not recovering
                and (sld_test * curr < min_notional
                     or rem_test * curr < min_notional)):
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
                    or (live_block_active and not recovering)):
                return False
            live_recovering = _has_pending_spot_partial_exit(d_live)
            if recovering and not live_recovering:
                return False
            recovering = live_recovering
            try:
                live_amount = _positive_finite(d_live.get("amount"))
            except (TypeError, ValueError):
                live_amount = 0.0
            live_invested = _positive_finite(d_live.get("invested_usdt"))
            live_sld_test = (
                _positive_finite(d_live.get("partial_exit_requested_amount"))
                if recovering else 0.0
            ) or live_amount * partial_pct
            live_rem_test = live_amount - live_sld_test
            if live_amount <= 0 or live_invested <= 0:
                log_event(
                    f" {sym}: invalid live partial-TP state "
                    f"(amount={d_live.get('amount')!r}, "
                    f"invested={d_live.get('invested_usdt')!r})  skipped",
                    "WARN",
                )
                return False
            if (not recovering
                    and (live_sld_test * curr < min_notional
                         or live_rem_test * curr < min_notional)):
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
                recovering_intent = _has_pending_spot_partial_exit(d)
                requested_sell = _positive_finite(sld_test)
                if requested_sell <= 0 or requested_sell > amount:
                    log_event(
                        f" {sym}: invalid partial-TP requested amount "
                        f"({sld_test!r})  skipped",
                        "ERROR",
                    )
                    return False
                request_key = "partial_exit_requested_amount"
                durable_request = _positive_finite(d.get(request_key))
                if durable_request <= 0:
                    persisted = self.state.update(
                        sym, request_key, requested_sell
                    )
                    if persisted is False:
                        log_event(
                            f" {sym}: cannot persist partial-TP amount before "
                            "live sell",
                            "ERROR",
                        )
                        return False
                    d[request_key] = requested_sell
                else:
                    requested_sell = durable_request
                client_order_id = _ensure_spot_exit_client_order_id(
                    self, sym, d, "partial"
                )
                order = None
                if recovering_intent:
                    from bot_utils.spot_exits import (
                        _find_spot_exit_order_by_client_id,
                    )
                    try:
                        order = _find_spot_exit_order_by_client_id(
                            self.ex,
                            f"{sym}/USDT",
                            client_order_id,
                            expected_amount=requested_sell,
                        )
                    except Exception as recovery_error:
                        log_event(
                            f"Partial-Sell {sym} outcome still unknown "
                            f"(clientOrderId={client_order_id}): "
                            f"{recovery_error}",
                            "WARN",
                        )
                        return False
                if order is None:
                    from bot_utils.trade_state import registry_order_guard
                    with registry_order_guard(
                        self.state, sym, d
                    ) as ownership_live:
                        if not isinstance(ownership_live, dict):
                            return False
                        if ownership_live.get("claim_conflict"):
                            log_event(
                                f"{sym}: partial sell blocked by registry "
                                f"claim conflict",
                                "ERROR",
                            )
                            return False
                        order, sold_amount = spot_market_sell_safe(
                            self.ex, f"{sym}/USDT", requested_sell,
                            client_order_id=client_order_id,
                        )
                else:
                    sold_amount = requested_sell
                # Phantom-fill guard: a market sell that returns
                # status=new/filled=0 (MEXC reject) must NOT be booked as a
                # partial-TP  that would shrink the tracked `amount`, mark
                # partial_sold=True and arm break-even while the coins are still
                # in the wallet. Don't mutate state; return False so the position
                # is re-evaluated next tick.
                from bot_utils.order_utils import (
                    order_has_proven_zero_fill,
                    order_has_unquantified_fill_notional,
                    order_was_filled,
                )
                raw_status = (
                    order.get("status") if isinstance(order, dict) else None
                )
                normalized_status = normalize_spot_order_status(raw_status)
                if spot_sell_requires_terminal_recovery(
                    order, requested_sell
                ):
                    _mark_spot_exit_outcome_uncertain(
                        self, sym, d, "partial"
                    )
                    log_event(
                        f" {sym}: partial-TP order is still "
                        f"{normalized_status or 'unresolved'}; "
                        "booking deferred until "
                        "terminal state",
                        "WARN",
                    )
                    return False
                unquantified_fill = order_has_unquantified_fill_notional(order)
                if (
                    unquantified_fill
                    or not order_was_filled(
                        order,
                        sold_amount,
                        min_fill_ratio=1e-9,
                    )
                ):
                    if (
                        normalized_status in {
                        "canceled", "cancelled", "rejected", "expired",
                        }
                        and order_has_proven_zero_fill(order)
                    ):
                        if not _reset_spot_exit_intent(
                            self, sym, d, "partial"
                        ):
                            _mark_spot_exit_outcome_uncertain(
                                self, sym, d, "partial"
                            )
                    else:
                        _mark_spot_exit_outcome_uncertain(
                            self, sym, d, "partial"
                        )
                    log_event(
                        f" {sym}: partial-TP order did NOT fill "
                        f"(status={raw_status})  NOT booking, client-id reconcile "
                        f"required", "WARN")
                    return False
                sold_amount = _filled_base_amount(order, sold_amount, sold_amount)
                exch_oid = (
                    order_id_text_or_none(order.get("id"))
                    or order_id_text_or_none(order.get("orderId"))
                )
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
            except SpotSellOutcomeUnknown as e:
                persisted = _mark_spot_exit_outcome_uncertain(
                    self, sym, d, "partial"
                )
                level = "WARN" if persisted else "ERROR"
                log_event(
                    f"Partial-Sell {sym} outcome unknown; retry blocked "
                    f"pending clientOrderId reconciliation ({e})",
                    level,
                )
                return False
            except Exception as e:
                log_event(f"Partial-Sell {sym} failed: {e}", "WARN")
                return False

        # Compute PnL with proportional entry fee
        real_prof_pct = ((fill_price - buy) / buy) * 100 if buy > 0 else 0.0
        initial_entry_fee = _positive_finite(
            d.get("initial_entry_fee", d.get("fees_paid", 0.0)))
        original_amount = _positive_finite(d.get("original_amount"))
        if original_amount <= 0:
            original_amount = amount
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
        mfe_pct, mae_pct, giveback_pct = _spot_excursion_metrics(d, fill_price)

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
            entry_id=d.get("entry_id"),
            mfe_pct=mfe_pct,
            mae_pct=mae_pct,
            giveback_pct=giveback_pct,
        )
        # Persist the physical position shrink and its exact accounting event
        # before DB booking. A crash must never leave the old full position on
        # disk after its partial PnL was already committed.
        new_invested = max(0.0, current_invested - sold_invested)
        new_amount = safe_remaining(amount, sold_amount)
        new_fees_paid = _positive_finite(d.get("fees_paid", 0.0)) + partial_fee
        updates = {
            "partial_sold": True,
            "invested_usdt": new_invested,
            "amount": new_amount,
            "original_amount": original_amount,
            "initial_entry_fee": initial_entry_fee,
            "fees_paid": new_fees_paid,
            "break_even": True,
            "partial_exit_client_order_id": None,
            "partial_exit_outcome_uncertain": False,
            "partial_exit_requested_amount": None,
            "partial_tp_blocked_min_notional": False,
            "partial_tp_blocked_min_notional_until": 0.0,
        }
        from bot_utils.trade_state import normalize_pending_accounting_items
        pending = normalize_pending_accounting_items(
            d.get("accounting_pending_partials"))
        pending.append(partial_trade)
        updates["accounting_pending_partials"] = pending
        try:
            state_persisted = self.state.update_many(sym, updates)
        except Exception as e:
            state_persisted = False
            self._log_error(f"spot partial state write-ahead {sym}", e)
        if state_persisted is False:
            log_event(
                f" {sym}: partial-TP state write-ahead failed - DB booking "
                f"deferred fail-closed",
                "ERROR",
            )
            # The physical partial fill already happened. Report this tick as
            # handled so the caller cannot immediately run a second full-exit
            # decision from its stale pre-fill snapshot.
            return True

        try:
            accounting_ok = bool(save_trade_db(**partial_trade))
        except Exception as e:
            accounting_ok = False
            self._log_error(f"spot partial save_trade_db {sym}", e)
        if accounting_ok:
            try:
                cleared = self.state.update(
                    sym, "accounting_pending_partials", pending[:-1]
                )
            except Exception as e:
                cleared = False
                self._log_error(f"spot partial pending clear {sym}", e)
            if cleared is False:
                log_event(
                    f" {sym}: partial-TP booked but durable pending clear "
                    f"failed; idempotent retry retained",
                    "ERROR",
                )
        else:
            log_event(
                f" {sym}: partial-TP DB save failed  slice kept "
                f"for accounting retry", "WARN")

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
        from core.symbol_locks import close_lock

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

        if _has_pending_spot_partial_exit(d):
            log_event(
                f" {sym}: full exit deferred while partial-TP client-id "
                "reconciliation is pending",
                "WARN",
            )
            return

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
                persisted_client_order_id = order_id_text_or_none(
                    d.get("full_exit_client_order_id")
                )
                client_order_id = _ensure_spot_exit_client_order_id(
                    self, sym, d, "full"
                )
                order = None
                if (
                    persisted_client_order_id == client_order_id
                    or d.get("full_exit_outcome_uncertain")
                ):
                    from bot_utils.spot_exits import (
                        _find_spot_exit_order_by_client_id,
                    )
                    try:
                        order = _find_spot_exit_order_by_client_id(
                            self.ex,
                            f"{sym}/USDT",
                            client_order_id,
                            expected_amount=requested_amount,
                        )
                    except Exception as recovery_error:
                        log_event(
                            f"Sell order {sym} outcome still unknown "
                            f"(clientOrderId={client_order_id}): "
                            f"{recovery_error}",
                            "WARN",
                        )
                        return
                # Network retry  so a single transient API error (DNS, VPN
                # flap) doesn't abort the sell and hold the position an extra
                # tick. Outcome-ambiguous errors explicitly forbid wrapper
                # retries until their stable client id has been reconciled.
                if order is None:
                    from bot_utils.network_retry import with_network_retry
                    from bot_utils.trade_state import registry_order_guard
                    with registry_order_guard(
                        self.state, sym, d
                    ) as ownership_live:
                        if not isinstance(ownership_live, dict):
                            return
                        if ownership_live.get("claim_conflict"):
                            log_event(
                                f"{sym}: sell blocked by registry claim conflict",
                                "ERROR",
                            )
                            return
                        order, sold = with_network_retry(
                            operation=lambda: spot_market_sell_safe(
                                self.ex, f"{sym}/USDT", remaining_amount,
                                client_order_id=client_order_id,
                            ),
                            action_label=f"sell {sym}",
                            max_attempts=3,
                            base_delay=0.5,
                            shutdown_event=self._shutdown_event,
                            log_event=log_event,
                        )
                else:
                    sold = remaining_amount
                raw_status = order.get("status") if isinstance(order, dict) else None
                normalized_status = normalize_spot_order_status(raw_status)
                if spot_sell_requires_terminal_recovery(
                    order, requested_amount
                ):
                    _mark_spot_exit_outcome_uncertain(
                        self, sym, d, "full"
                    )
                    log_event(
                        f"Sell order {sym} is still "
                        f"{normalized_status or 'unresolved'}; "
                        f"booking and any retry deferred until terminal state",
                        "WARN",
                    )
                    try:
                        from trading.cooldown_utils import set_cooldown as _scd
                        with self._cooldown_lock:
                            _scd(self.cool, sym, 15, self.COOLDOWN_FILE)
                    except Exception as cooldown_error:
                        self._log_error(
                            f"sell-pending cooldown set {sym}", cooldown_error
                        )
                    return
                filled_amount = _filled_base_amount(
                    order, sold, requested_amount
                )
                # Verify the sell ACTUALLY filled before booking a closed trade.
                # spot_market_sell_safe returns (order, amt) the instant
                # create_market_sell_order returns, but MEXC can return an order
                # with status=new/open and filled=0 that never executed
                # (remainder below min-notional, matching-engine reject). Keep
                # the position + a short cooldown so the next tick retries; do
                # NOT book and do NOT remove state.
                from bot_utils.order_utils import (
                    order_has_proven_zero_fill,
                    order_has_unquantified_fill_notional,
                    order_was_filled,
                )
                unquantified_fill = order_has_unquantified_fill_notional(order)
                if (
                    unquantified_fill
                    or not order_was_filled(order, filled_amount)
                ):
                    _st = raw_status if raw_status is not None else "?"
                    if (
                        normalized_status in {
                        "canceled", "cancelled", "rejected", "expired",
                        }
                        and order_has_proven_zero_fill(order)
                    ):
                        if not _reset_spot_exit_intent(
                            self, sym, d, "full"
                        ):
                            _mark_spot_exit_outcome_uncertain(
                                self, sym, d, "full"
                            )
                    else:
                        _mark_spot_exit_outcome_uncertain(
                            self, sym, d, "full"
                        )
                    log_event(
                        f" {sym}: sell order did NOT fill (status={_st}, "
                        f"coins still in wallet)  NOT booking, client-id "
                        f"reconcile required", "WARN")
                    try:
                        from trading.cooldown_utils import set_cooldown as _scd
                        with self._cooldown_lock:
                            _scd(self.cool, sym, 15, self.COOLDOWN_FILE)
                    except Exception as ce:
                        self._log_error(f"sell-unfilled cooldown set {sym}", ce)
                    return
                exch_oid = (
                    order_id_text_or_none(order.get("id"))
                    or order_id_text_or_none(order.get("orderId"))
                )
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
            except SpotSellOutcomeUnknown as unknown:
                persisted = _mark_spot_exit_outcome_uncertain(
                    self, sym, d, "full"
                )
                level = "WARN" if persisted else "ERROR"
                log_event(
                    f"Sell order {sym} outcome unknown; retry blocked "
                    f"pending clientOrderId reconciliation ({unknown})",
                    level,
                )
                return
            except InsufficientSellBalance as ib:
                # The BASE coins are not actually on the exchange (already sold,
                # or only dust below the min lot). Selling will never succeed,
                # but dropping state before accounting can lose realized PnL
                # after a manual/external sell.
                log_event(
                    f"{sym}: nothing left to sell ({ib})  booking offline "
                    f"close before removing state", "WARN")
                if d.get("unpriced_external_partials"):
                    flat_pending = {
                        "verified_flat_pending_accounting": True,
                        "verified_flat_reason": reason,
                        "verified_flat_at": _utc_now_str(),
                    }
                    try:
                        flat_persisted = self.state.update_many(
                            sym, flat_pending
                        )
                    except Exception as state_err:
                        flat_persisted = False
                        self._log_error(
                            f"spot orphan combined accounting {sym}", state_err
                        )
                    level = "WARN" if flat_persisted is not False else "ERROR"
                    log_event(
                        f"{sym}: base balance is zero with unpriced earlier "
                        f"partials; direct accounting deferred to combined "
                        f"offline reconcile",
                        level,
                    )
                    return
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
        partial_sold = bool(d.get("partial_sold"))
        initial_entry_fee = _positive_finite(
            d.get(
                "initial_entry_fee",
                0.0 if partial_sold else d.get("fees_paid", 0.0),
            )
        )
        original_amount = _positive_finite(d.get("original_amount"))
        if original_amount <= 0 and not partial_sold:
            original_amount = requested_amount
        proportional_entry_fee = safe_proportional_fee(
            initial_entry_fee, remaining_amount, original_amount,
            partial_sold=partial_sold
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

        residual_intent_updates = {}
        if partial_live_fill:
            residual_intent_updates = {
                "full_exit_client_order_id": None,
                "full_exit_outcome_uncertain": False,
            }

        sell_time = _utc_now_str()
        if (
            not partial_live_fill
            and d.get("unpriced_external_partials")
        ):
            flat_pending = {
                "verified_flat_pending_accounting": True,
                "verified_flat_reason": reason,
                "verified_flat_at": sell_time,
                "verified_flat_sell_price": fill_price,
                "verified_flat_exchange_order_id": exch_oid,
            }
            try:
                flat_persisted = self.state.update_many(sym, flat_pending)
            except Exception as state_err:
                flat_persisted = False
                self._log_error(
                    f"spot verified-flat combined accounting {sym}", state_err
                )
            level = "WARN" if flat_persisted is not False else "ERROR"
            log_event(
                f" {sym}: final sell filled with unpriced earlier partials; "
                f"direct accounting deferred to combined offline reconcile",
                level,
            )
            return
        accounting_mode_is_sim = d.get("accounting_pending_mode_is_sim",
                                       self.simulation)
        mfe_pct, mae_pct, giveback_pct = _spot_excursion_metrics(d, fill_price)
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
            entry_id=d.get("entry_id"),
            mfe_pct=mfe_pct,
            mae_pct=mae_pct,
            giveback_pct=giveback_pct,
        )
        pending_partials = None
        if partial_live_fill:
            trade_row["is_partial"] = True
            remaining_after_fill = safe_remaining(
                requested_amount, remaining_amount
            )
            from bot_utils.trade_state import normalize_pending_accounting_items
            pending_partials = normalize_pending_accounting_items(
                d.get("accounting_pending_partials")
            )
            pending_partials.append(trade_row)
            partial_state = {
                "amount": remaining_after_fill,
                "invested_usdt": max(
                    0.0, current_invested - booked_invested
                ),
                "original_amount": original_amount,
                "initial_entry_fee": initial_entry_fee,
                "fees_paid": accumulated_fees,
                "accounting_pending_partials": pending_partials,
                "last_partial_fill_reason": reason,
                "last_partial_fill_order_id": exch_oid,
                "closing_retry_pending": True,
                "closing_retry_reason": reason,
                **residual_intent_updates,
            }
            try:
                pending_persisted = self.state.update_many(sym, partial_state)
            except Exception as state_err:
                pending_persisted = False
                self._log_error(
                    f"spot partial full-exit write-ahead {sym}", state_err
                )
            if pending_persisted is False:
                log_event(
                    f" {sym}: partial full-exit fill was not booked because "
                    f"its residual accounting state was not durable",
                    "ERROR",
                )
                return
        else:
            pending_close = {
                "accounting_pending": True,
                "accounting_pending_reason": reason,
                "accounting_pending_sell_price": fill_price,
                "accounting_pending_sell_time": sell_time,
                "accounting_pending_profit_pct": real_prof_pct,
                "accounting_pending_profit_usdt": profit_usdt,
                "accounting_pending_invested_usdt": booked_invested,
                "accounting_pending_mode_is_sim": accounting_mode_is_sim,
                "accounting_pending_fees_usdt": (
                    proportional_entry_fee + close_fee
                ),
                "accounting_pending_exchange_order_id": exch_oid,
                "accounting_pending_mfe_pct": mfe_pct,
                "accounting_pending_mae_pct": mae_pct,
                "accounting_pending_giveback_pct": giveback_pct,
            }
            try:
                pending_persisted = self.state.update_many(sym, pending_close)
            except Exception as state_err:
                pending_persisted = False
                self._log_error(
                    f"spot full accounting write-ahead {sym}", state_err
                )
            if pending_persisted is False:
                log_event(
                    f" {sym}: filled full exit was not booked because its "
                    f"accounting recovery marker was not durable",
                    "ERROR",
                )
                return
        try:
            accounting_ok = bool(save_trade_db(**trade_row))
        except Exception as e:
            accounting_ok = False
            self._log_error(f"spot full save_trade_db {sym}", e)
        if not accounting_ok:
            if partial_live_fill:
                log_event(
                    f" {sym}: partial full-exit fill DB save failed - "
                    f"residual position kept for accounting retry", "WARN")
                return
            log_event(
                f" {sym}: full-exit DB save failed after filled sell  "
                f"state kept for accounting retry", "WARN")
            return
        if partial_live_fill:
            try:
                cleared = self.state.update(
                    sym,
                    "accounting_pending_partials",
                    pending_partials[:-1],
                )
            except Exception as state_err:
                cleared = False
                self._log_error(
                    f"spot partial full-exit pending clear {sym}", state_err
                )
            if cleared is False:
                log_event(
                    f" {sym}: partial full-exit was booked but its durable "
                    f"pending marker could not be cleared; idempotent retry kept",
                    "ERROR",
                )
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
