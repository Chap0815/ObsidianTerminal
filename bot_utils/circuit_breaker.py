"""
bot_utils/circuit_breaker.py  Slippage / spread / safe-mode guard.

Real market problems (illiquidity, cascading liquidations, low-volume
sessions) often show up FIRST as bad fills and widening spreads before
the API itself errors. This module watches:
  per-trade slippage (actual fill vs expected ticker)
  ticker bid/ask spread before entry (with a trip counter)
  trip count over a rolling window  SAFE_MODE

In SAFE_MODE the bot stops opening new entries but continues monitoring
and closing existing positions. State is reset by restarting the bot.
"""

from __future__ import annotations

import atexit
import hashlib
import json
import math
import os
import threading
import time
from typing import Callable, Optional


_SAFE_MODE_STATE_JSON_MAX_BYTES = 64 * 1024
_SAFE_MODE_ALERT_PERSIST_RETRY_SEC = 30.0
_THREADING_TIMER_TYPE = threading.Timer
_THREADING_THREAD_TYPE = threading.Thread


class _AlertPersistGeneration:
    __slots__ = ("done", "start_raised", "timer")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.start_raised = False
        self.timer: Optional[threading.Timer] = None


class _AlertFlushGeneration:
    __slots__ = ("done", "result", "start_raised", "worker")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.result = False
        self.start_raised = False
        self.worker: Optional[threading.Thread] = None


class _AlertDirectPersistGeneration:
    __slots__ = ("done", "result")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.result = False


def _timer_definitely_never_started(timer: object) -> bool:
    """Recognize only an exact stdlib Timer with no launch identity."""
    if type(timer) is not _THREADING_TIMER_TYPE:
        return False
    try:
        return timer.ident is None and not timer.is_alive()
    except BaseException:
        return False


def _thread_definitely_never_started(worker: object) -> bool:
    """Recognize only an exact stdlib Thread with no launch identity."""
    if type(worker) is not _THREADING_THREAD_TYPE:
        return False
    try:
        return worker.ident is None and not worker.is_alive()
    except BaseException:
        return False


def _alert_flush_generation_unresolved(
    generation: Optional[_AlertFlushGeneration],
) -> bool:
    if generation is None:
        return False
    if not generation.done.is_set():
        return True
    worker = generation.worker
    if worker is None:
        return False
    try:
        return bool(worker.is_alive())
    except BaseException:
        return True


def _acquire_before(lock: threading.Lock, deadline: float) -> bool:
    remaining = deadline - time.monotonic()
    if remaining <= 0.0:
        return lock.acquire(blocking=False)
    return lock.acquire(timeout=remaining)


def _log_safe_mode_error(context: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, exc)
    except BaseException:
        pass


def _unique_safe_mode_state_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate safe-mode state JSON key: {key}")
        result[key] = value
    return result


def _read_safe_mode_state_json(path: str) -> dict:
    with open(path, "rb") as stream:
        raw = stream.read(_SAFE_MODE_STATE_JSON_MAX_BYTES + 1)
    if len(raw) > _SAFE_MODE_STATE_JSON_MAX_BYTES:
        raise ValueError("safe-mode state JSON exceeds size limit")
    data = json.loads(
        raw.decode("utf-8-sig"),
        object_pairs_hook=_unique_safe_mode_state_object,
    )
    return data if isinstance(data, dict) else {}


# Tunable thresholds from core.constants (single source of truth).
# Fallback values only if constants isn't importable.
try:
    from core.constants import (
        MAX_SLIPPAGE_PCT,
        MAX_SPREAD_PCT,
        SLIPPAGE_TRIP_COUNT,
        SLIPPAGE_WINDOW_SEC,
        SPREAD_TRIP_COUNT,
        SPREAD_WINDOW_SEC,
    )
except Exception:
    MAX_SLIPPAGE_PCT = 0.5
    MAX_SPREAD_PCT = 0.3
    SLIPPAGE_TRIP_COUNT = 3
    SLIPPAGE_WINDOW_SEC = 300
    SPREAD_TRIP_COUNT = 5
    SPREAD_WINDOW_SEC = 180


#  Slippage tracking (legacy module-level fallback)

_slippage_observations: list = []
_slippage_lock = threading.Lock()


def record_slippage(
    expected_price: float,
    actual_fill: float,
    symbol: str = "",
    side: str = "buy",
    trigger_safe_mode: Optional[Callable] = None,
    log_event: Optional[Callable] = None,
    safe_mode_instance: Optional["SafeMode"] = None,
) -> float:
    """Record one slippage measurement. Returns adverse slippage %.

    Auto-triggers SAFE_MODE via `trigger_safe_mode` callback when
    SLIPPAGE_TRIP_COUNT abnormal readings occur in SLIPPAGE_WINDOW_SEC.

    If ``safe_mode_instance`` is provided, observations are stored on
    the instance (recommended). Otherwise fall back to module-level state.
    """
    if safe_mode_instance is not None:
        return safe_mode_instance.record_slippage(
            expected_price, actual_fill, symbol, side, trigger_safe_mode, log_event
        )
    return _record_slippage_into(
        _slippage_observations,
        _slippage_lock,
        expected_price,
        actual_fill,
        symbol,
        side,
        trigger_safe_mode,
        log_event,
    )


def _record_slippage_into(
    observations: list,
    lock: threading.Lock,
    expected_price: float,
    actual_fill: float,
    symbol: str,
    side: str,
    trigger_safe_mode: Optional[Callable],
    log_event: Optional[Callable],
) -> float:
    def positive_finite_price(value) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) and parsed > 0.0 else None

    expected = positive_finite_price(expected_price)
    actual = positive_finite_price(actual_fill)
    if expected is None or actual is None:
        # A completed fill whose reference or execution price cannot be
        # quantified is never evidence of zero slippage. Count it as adverse
        # so repeated malformed fill evidence trips the existing circuit.
        slippage_pct = math.inf
    else:
        normalized_side = str(side).strip().lower()
        if normalized_side == "buy":
            adverse_delta = actual - expected
        elif normalized_side == "sell":
            adverse_delta = expected - actual
        else:
            # Unknown direction cannot be classified safely; retain the
            # conservative absolute-distance behavior for malformed callers.
            adverse_delta = abs(actual - expected)
        slippage_pct = max(0.0, adverse_delta) / expected * 100
        if not math.isfinite(slippage_pct):
            slippage_pct = math.inf
    now = time.monotonic()
    abnormal = slippage_pct > MAX_SLIPPAGE_PCT

    with lock:
        observations.append((now, slippage_pct))
        cutoff = now - SLIPPAGE_WINDOW_SEC
        observations[:] = [(t, p) for t, p in observations if t >= cutoff]
        recent_abnormal = sum(1 for _, p in observations if p > MAX_SLIPPAGE_PCT)

    if abnormal and log_event:
        try:
            log_event(
                f" Slippage {slippage_pct:.3f}% on {symbol} {side} "
                f"({recent_abnormal}/{SLIPPAGE_TRIP_COUNT} in last "
                f"{SLIPPAGE_WINDOW_SEC}s)",
                "WARN",
            )
        except Exception as exc:
            _log_safe_mode_error("slippage circuit-breaker warning", exc)

    if recent_abnormal >= SLIPPAGE_TRIP_COUNT and trigger_safe_mode:
        trigger_safe_mode(
            f"slippage CB tripped  {recent_abnormal} abnormal readings "
            f"in {SLIPPAGE_WINDOW_SEC}s"
        )

    return slippage_pct


#  Spread check


def has_valid_spread_quotes(ticker: dict | None) -> bool:
    """Return whether ticker bid/ask form a finite executable top of book."""
    if not isinstance(ticker, dict):
        return False
    bid = ticker.get("bid")
    ask = ticker.get("ask")
    if bid is None or ask is None or isinstance(bid, bool) or isinstance(ask, bool):
        return False
    try:
        bid_f = float(bid)
        ask_f = float(ask)
    except (TypeError, ValueError, OverflowError):
        return False
    return (
        math.isfinite(bid_f)
        and math.isfinite(ask_f)
        and bid_f > 0.0
        and ask_f > 0.0
        and ask_f >= bid_f
    )


def extract_valid_top_of_book(order_book: dict | None) -> tuple[float, float] | None:
    """Return finite positive ``(bid, ask)`` from strict CCXT book rows."""
    if not isinstance(order_book, dict):
        return None
    bids = order_book.get("bids")
    asks = order_book.get("asks")
    if (
        not isinstance(bids, (list, tuple))
        or not isinstance(asks, (list, tuple))
        or not bids
        or not asks
    ):
        return None
    bid_row = bids[0]
    ask_row = asks[0]
    if (
        not isinstance(bid_row, (list, tuple))
        or not isinstance(ask_row, (list, tuple))
        or len(bid_row) < 2
        or len(ask_row) < 2
    ):
        return None
    bid_amount = bid_row[1]
    ask_amount = ask_row[1]
    if isinstance(bid_amount, bool) or isinstance(ask_amount, bool):
        return None
    try:
        bid_amount_f = float(bid_amount)
        ask_amount_f = float(ask_amount)
    except (TypeError, ValueError, OverflowError):
        return None
    quotes = {"bid": bid_row[0], "ask": ask_row[0]}
    if (
        not math.isfinite(bid_amount_f)
        or not math.isfinite(ask_amount_f)
        or bid_amount_f <= 0.0
        or ask_amount_f <= 0.0
        or not has_valid_spread_quotes(quotes)
    ):
        return None
    return float(quotes["bid"]), float(quotes["ask"])


def check_spread_ok(
    ticker: dict,
    log_event: Optional[Callable] = None,
    symbol: str = "",
    safe_mode_instance: Optional["SafeMode"] = None,
    max_spread_pct: Optional[float] = None,
    missing_ok: bool = True,
) -> bool:
    """Return True if the bid/ask spread is acceptable for entry.

    By default, missing bid/ask is tolerated for legacy spot callers. Futures
    live entries should pass ``missing_ok=False`` and fail closed.

    If a SafeMode instance is provided, abnormal spreads are recorded and trip
    the breaker on repeated occurrence (a minutes-long flash-crash spread would
    otherwise block trades only transiently, with SAFE_MODE never persisting).
    """
    if not isinstance(ticker, dict):
        return bool(missing_ok)
    bid = ticker.get("bid")
    ask = ticker.get("ask")
    if bid is None or ask is None:
        if log_event and not missing_ok:
            log_event("Spread check unavailable  blocking entry", "WARN")
        return bool(missing_ok)
    if not has_valid_spread_quotes(ticker):
        if log_event:
            log_event("Spread check invalid bid/ask  blocking entry", "WARN")
        return False
    try:
        bid_f = float(bid)
        ask_f = float(ask)
        threshold = max_spread_pct if max_spread_pct is not None else MAX_SPREAD_PCT
        if isinstance(threshold, bool):
            raise ValueError("boolean spread threshold")
        threshold = float(threshold)
        if not math.isfinite(threshold) or threshold < 0.0:
            raise ValueError("invalid spread threshold")
        mid = (ask_f + bid_f) / 2.0
        if not math.isfinite(mid) or mid <= 0.0:
            raise ValueError("invalid spread midpoint")
        spread_pct = (ask_f - bid_f) / mid * 100
        if not math.isfinite(spread_pct) or spread_pct < 0.0:
            raise ValueError("invalid spread result")
        if spread_pct > threshold:
            if log_event:
                log_event(
                    f" Spread check: {spread_pct:.3f}% > {threshold}% "
                    f"max (bid={bid_f}, ask={ask_f})  blocking entry",
                    "WARN",
                )
            # record the bad reading and possibly trip
            if safe_mode_instance is not None:
                safe_mode_instance.record_spread_abnormal(
                    spread_pct, symbol=symbol, log_event=log_event
                )
            return False
    except (TypeError, ValueError, OverflowError):
        if log_event:
            log_event("Spread check invalid ticker values  blocking entry", "WARN")
        return False
    return True


#  Safe mode


class SafeMode:
    """Per-bot SAFE_MODE state holder.

    Each bot constructs its own SafeMode instance so two bots in the same
    process don't share state. Idempotent  first trigger sends the alert,
    subsequent triggers are no-ops.

    The state file path includes a hash of an account-scope string (env var
    BOT_ACCOUNT_ID, or PID as last resort) so two instances of the same
    bot_name don't overwrite each other.
    """

    def __init__(
        self,
        bot_name: str = "BOT",
        telegram_send: Optional[Callable] = None,
        telegram_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None,
        log_event: Optional[Callable] = None,
        log_struct: Optional[Callable] = None,
        state_dir: Optional[str] = None,
        instance_id: Optional[str] = None,
    ):
        self._event = threading.Event()
        self._reason = "normal"
        self._alert_sent = False
        self._alert_inflight = False
        self._lock = threading.Lock()
        self._alert_persist_pending = False
        self._alert_persist_timer: Optional[threading.Timer] = None
        self._alert_persist_generation: Optional[
            _AlertPersistGeneration
        ] = None
        self._alert_flush_generation: Optional[_AlertFlushGeneration] = None
        self._alert_persist_active: set[threading.Thread] = set()
        self._alert_persist_retiring: set[threading.Thread] = set()
        self._alert_direct_persist_active: set[
            _AlertDirectPersistGeneration
        ] = set()
        self._alert_persist_shutdown = False
        self.bot_name = bot_name
        self._telegram_send = telegram_send
        self._telegram_token = telegram_token
        self._telegram_chat_id = telegram_chat_id
        self._log_event = log_event
        self._log_struct = log_struct
        # per-instance slippage observations
        self._slippage_observations: list = []
        self._slippage_lock = threading.Lock()
        # per-instance spread observations
        self._spread_observations: list = []
        self._spread_lock = threading.Lock()
        # per-instance state file with unique suffix
        self._alert_state_file: Optional[str] = None
        if state_dir:
            try:
                os.makedirs(state_dir, exist_ok=True)
                suffix = self._compute_instance_suffix(instance_id)
                self._alert_state_file = os.path.join(
                    state_dir, f"safe_mode_{bot_name.lower()}_{suffix}.json"
                )
                self._load_alert_state()
                atexit.register(self.flush_alert_state_pending)
            except Exception:
                self._alert_state_file = None

    @staticmethod
    def _compute_instance_suffix(instance_id: Optional[str]) -> str:
        """Produce a short hash that distinguishes simultaneous
        instances. Priority:
          1. explicit instance_id arg
          2. BOT_ACCOUNT_ID env var (e.g. account UUID)
          3. stable default (the pre-start guard prevents duplicate bot
             processes; a PID here would defeat restart persistence)
        """
        seed = instance_id or os.getenv("BOT_ACCOUNT_ID") or "default-instance"
        # Stable filename suffix only, never a signature or secret hash.
        return hashlib.sha1(seed.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]

    def _load_alert_state(self) -> None:
        """Load persisted alert state  sets _alert_sent if last alert
        was on the current UTC day (so a restart same-day won't re-fire)."""
        if not self._alert_state_file:
            return
        try:
            from datetime import datetime as _dt, timezone as _tz

            if not os.path.exists(self._alert_state_file):
                return
            data = _read_safe_mode_state_json(self._alert_state_file)
            last_day = data.get("alert_sent_day")
            today = _dt.now(_tz.utc).strftime("%Y-%m-%d")
            if last_day == today:
                self._alert_sent = True
        except Exception:
            pass

    def _save_alert_state(self) -> bool:
        """Persist that we sent a Telegram alert today (UTC)."""
        if not self._alert_state_file:
            return True
        try:
            from datetime import datetime as _dt, timezone as _tz
            from bot_utils.state_persist import atomic_save_json

            today = _dt.now(_tz.utc).strftime("%Y-%m-%d")
            persisted = atomic_save_json(
                self._alert_state_file,
                {"alert_sent_day": today, "reason": self._reason},
            )
            if persisted is not True:
                raise OSError("safe-mode alert state persistence failed")
            return True
        except Exception as exc:
            _log_safe_mode_error("safe-mode alert state persistence", exc)
            return False

    def _schedule_alert_state_retry(self) -> None:
        with self._lock:
            if self._alert_persist_shutdown:
                return
            self._alert_persist_pending = True
            generation = self._alert_persist_generation
            if generation is not None and not generation.done.is_set():
                return
            timer = self._alert_persist_timer
            if timer is not None and timer.is_alive():
                return
            generation = _AlertPersistGeneration()
            try:
                timer = threading.Timer(
                    _SAFE_MODE_ALERT_PERSIST_RETRY_SEC,
                    lambda: self._retry_alert_state_persist(generation),
                )
            except Exception as exc:
                _log_safe_mode_error(
                    "construct safe-mode alert state persistence retry", exc
                )
                return
            timer.daemon = True
            generation.timer = timer
            self._alert_persist_generation = generation
            self._alert_persist_timer = timer
            try:
                timer.start()
            except BaseException as exc:
                # start() is an ownership boundary: it may raise after the
                # worker was launched.  Retain the exact generation so
                # shutdown cannot report success while that worker is live.
                generation.start_raised = True
                _log_safe_mode_error(
                    "schedule safe-mode alert state persistence retry", exc
                )
                if (
                    isinstance(exc, Exception)
                    and _timer_definitely_never_started(timer)
                ):
                    generation.done.set()
                    if self._alert_persist_generation is generation:
                        self._alert_persist_generation = None
                        self._alert_persist_timer = None
                if not isinstance(exc, Exception):
                    raise

    def _retry_alert_state_persist(
        self,
        generation: _AlertPersistGeneration,
    ) -> None:
        current = threading.current_thread()
        with self._lock:
            self._alert_persist_active.add(current)
            if self._alert_persist_generation is generation:
                self._alert_persist_timer = None
            pending = (
                self._alert_persist_pending
                and not self._alert_persist_shutdown
            )
        reschedule = False
        try:
            if not pending:
                return
            if self._save_alert_state():
                with self._lock:
                    self._alert_persist_pending = False
                return
            reschedule = True
        finally:
            with self._lock:
                self._alert_persist_active.discard(current)
                generation.done.set()
                if self._alert_persist_generation is generation:
                    self._alert_persist_generation = None
                    self._alert_persist_timer = None
            if reschedule:
                self._schedule_alert_state_retry()

    def _run_shutdown_alert_flush(
        self,
        generation: _AlertFlushGeneration,
    ) -> None:
        persisted = False
        try:
            persisted = self._save_alert_state()
        except BaseException as exc:
            _log_safe_mode_error("shutdown safe-mode alert state flush", exc)
        finally:
            with self._lock:
                generation.result = bool(persisted)
                if persisted:
                    self._alert_persist_pending = False
                generation.done.set()

    def _start_shutdown_alert_flush_locked(
        self,
    ) -> Optional[_AlertFlushGeneration]:
        generation = self._alert_flush_generation
        if _alert_flush_generation_unresolved(generation):
            return generation
        if generation is not None and generation.result:
            return generation
        self._alert_flush_generation = None
        generation = _AlertFlushGeneration()
        try:
            worker = threading.Thread(
                target=self._run_shutdown_alert_flush,
                args=(generation,),
                name=f"safe-mode-flush-{self.bot_name.lower()}",
                daemon=True,
            )
        except Exception as exc:
            _log_safe_mode_error("construct shutdown safe-mode flush", exc)
            return None
        generation.worker = worker
        self._alert_flush_generation = generation
        try:
            worker.start()
        except BaseException as exc:
            generation.start_raised = True
            _log_safe_mode_error("start shutdown safe-mode flush", exc)
            if (
                isinstance(exc, Exception)
                and _thread_definitely_never_started(worker)
            ):
                generation.done.set()
                if self._alert_flush_generation is generation:
                    self._alert_flush_generation = None
            if not isinstance(exc, Exception):
                raise
        return generation

    def shutdown_alert_state_persistence(self, timeout: float = 0.0) -> bool:
        """Terminally stop retry timers and flush the safety marker."""
        if isinstance(timeout, bool):
            return False
        try:
            requested_timeout = float(timeout)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(requested_timeout):
            return False
        bounded_timeout = min(
            max(0.0, requested_timeout),
            float(threading.TIMEOUT_MAX),
        )
        deadline = time.monotonic() + bounded_timeout
        if not _acquire_before(self._lock, deadline):
            return False
        try:
            self._alert_persist_shutdown = True
            generation = self._alert_persist_generation
            timer = (
                generation.timer
                if generation is not None
                else self._alert_persist_timer
            )
            self._alert_persist_timer = None
            workers = list(self._alert_persist_active)
            workers.extend(self._alert_persist_retiring)
            direct_generations = list(self._alert_direct_persist_active)
            if timer is not None:
                workers.append(timer)
            self._alert_persist_retiring.update(workers)
        finally:
            self._lock.release()
        if timer is not None:
            cancel_succeeded = False
            try:
                timer.cancel()
                cancel_succeeded = True
            except Exception:
                pass
        else:
            cancel_succeeded = False
        still_alive = []
        for worker in dict.fromkeys(workers):
            join = getattr(worker, "join", None)
            if callable(join) and worker is not threading.current_thread():
                try:
                    join(timeout=max(0.0, deadline - time.monotonic()))
                except Exception:
                    pass
            try:
                if worker.is_alive():
                    still_alive.append(worker)
            except Exception:
                still_alive.append(worker)
        if not _acquire_before(self._lock, deadline):
            return False
        try:
            self._alert_persist_retiring.intersection_update(still_alive)
            if (
                generation is not None
                and not generation.done.is_set()
                and not generation.start_raised
                and cancel_succeeded
                and timer not in still_alive
            ):
                # A successfully cancelled generation whose timer is no
                # longer alive will never enter the callback; shutdown owns
                # its terminal transition.
                generation.done.set()
            generation_done = (
                generation is None or generation.done.is_set()
            )
            if (
                generation_done
                and self._alert_persist_generation is generation
            ):
                self._alert_persist_generation = None
        finally:
            self._lock.release()
        if still_alive:
            return False
        if not generation_done:
            return False
        for direct_generation in direct_generations:
            if not direct_generation.done.wait(
                timeout=max(0.0, deadline - time.monotonic())
            ):
                return False
        if not _acquire_before(self._lock, deadline):
            return False
        try:
            if self._alert_direct_persist_active:
                return False
            if not self._alert_persist_pending:
                flush_generation = self._alert_flush_generation
                if flush_generation is None:
                    return True
                if _alert_flush_generation_unresolved(flush_generation):
                    return False
                return flush_generation.result
            flush_generation = self._start_shutdown_alert_flush_locked()
        finally:
            self._lock.release()
        if flush_generation is None:
            return False
        worker = flush_generation.worker
        if worker is not None and worker is not threading.current_thread():
            try:
                worker.join(timeout=max(0.0, deadline - time.monotonic()))
            except BaseException:
                return False
        elif not flush_generation.done.wait(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        if not _acquire_before(self._lock, deadline):
            return False
        try:
            completed = (
                self._alert_flush_generation is flush_generation
                and not _alert_flush_generation_unresolved(flush_generation)
            )
            persisted = (
                completed
                and flush_generation.result
                and not self._alert_persist_pending
            )
            return persisted
        finally:
            self._lock.release()

    def flush_alert_state_pending(self) -> bool:
        """Interpreter-exit fallback for the managed runtime closer."""
        return self.shutdown_alert_state_persistence(timeout=1.0)

    def record_slippage(
        self,
        expected_price: float,
        actual_fill: float,
        symbol: str = "",
        side: str = "buy",
        trigger_safe_mode: Optional[Callable] = None,
        log_event: Optional[Callable] = None,
    ) -> float:
        """Per-instance slippage recording."""
        if trigger_safe_mode is None:
            trigger_safe_mode = self.trigger
        if log_event is None:
            log_event = self._log_event
        return _record_slippage_into(
            self._slippage_observations,
            self._slippage_lock,
            expected_price,
            actual_fill,
            symbol,
            side,
            trigger_safe_mode,
            log_event,
        )

    def record_spread_abnormal(
        self, spread_pct: float, symbol: str = "", log_event: Optional[Callable] = None
    ) -> None:
        """Record an abnormal spread reading. Trips SAFE_MODE when
        SPREAD_TRIP_COUNT abnormal readings occur in SPREAD_WINDOW_SEC."""
        if log_event is None:
            log_event = self._log_event
        now = time.monotonic()
        with self._spread_lock:
            self._spread_observations.append((now, spread_pct, symbol))
            cutoff = now - SPREAD_WINDOW_SEC
            self._spread_observations[:] = [
                row for row in self._spread_observations if row[0] >= cutoff
            ]
            recent = len(self._spread_observations)

        if recent >= SPREAD_TRIP_COUNT:
            self.trigger(
                f"spread CB tripped  {recent} abnormal spread readings "
                f"in {SPREAD_WINDOW_SEC}s (latest: {spread_pct:.3f}% on {symbol})"
            )

    def is_active(self) -> bool:
        return self._event.is_set()

    def reason(self) -> str:
        return self._reason

    def trigger(self, reason: str) -> None:
        """Enter SAFE_MODE. Idempotent  first call only sends the alert."""
        with self._lock:
            already_active = self._event.is_set()
            if already_active:
                persist_retry_pending = self._alert_persist_pending
            else:
                persist_retry_pending = False
                self._event.set()
                self._reason = reason
            send_alert = bool(
                not self._alert_sent
                and not self._alert_inflight
                and self._telegram_send
            )
            if send_alert:
                self._alert_inflight = True
        if already_active and not send_alert:
            if persist_retry_pending:
                self._schedule_alert_state_retry()
            return
        if not already_active and self._log_struct:
            try:
                self._log_struct(
                    "safe_mode_triggered", reason=reason, bot=self.bot_name
                )
            except Exception:
                pass
        if not already_active and self._log_event:
            try:
                self._log_event(
                    f" SAFE_MODE activated: {reason}. New entries "
                    f"DISABLED, monitoring & closing continues.",
                    "WARN",
                )
            except Exception:
                pass
        if send_alert:
            try:
                accepted = self._telegram_send(
                    self._telegram_token,
                    self._telegram_chat_id,
                    f" [{self.bot_name}] SAFE_MODE activated\n"
                    f"Reason: {reason}\n\n"
                    f"No new entries until bot is restarted.\n"
                    f"Existing positions are still being monitored & closed.",
                )
                if accepted is False:
                    raise RuntimeError(
                        "safe-mode Telegram alert was not accepted"
                    )
            except Exception as exc:
                _log_safe_mode_error("safe-mode Telegram alert", exc)
            else:
                direct_generation = None
                with self._lock:
                    self._alert_sent = True
                    if not self._alert_persist_shutdown:
                        direct_generation = _AlertDirectPersistGeneration()
                        self._alert_direct_persist_active.add(direct_generation)
                        self._alert_persist_pending = True
                persisted = False
                if direct_generation is not None:
                    try:
                        persisted = self._save_alert_state()
                    finally:
                        with self._lock:
                            direct_generation.result = bool(persisted)
                            if persisted:
                                self._alert_persist_pending = False
                            direct_generation.done.set()
                            self._alert_direct_persist_active.discard(
                                direct_generation
                            )
                if direct_generation is not None and not persisted:
                    self._schedule_alert_state_retry()
            finally:
                with self._lock:
                    self._alert_inflight = False
