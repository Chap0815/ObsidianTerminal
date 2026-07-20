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

import hashlib
import os
import threading
import time
from typing import Callable, Optional


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
    """Record one slippage measurement. Returns abs slippage %.

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
    if expected_price <= 0 or actual_fill <= 0:
        return 0.0
    slippage_pct = abs(actual_fill - expected_price) / expected_price * 100
    now = time.monotonic()
    abnormal = slippage_pct > MAX_SLIPPAGE_PCT

    with lock:
        observations.append((now, slippage_pct))
        cutoff = now - SLIPPAGE_WINDOW_SEC
        observations[:] = [(t, p) for t, p in observations if t >= cutoff]
        recent_abnormal = sum(1 for _, p in observations if p > MAX_SLIPPAGE_PCT)

    if abnormal and log_event:
        log_event(
            f" Slippage {slippage_pct:.3f}% on {symbol} {side} "
            f"({recent_abnormal}/{SLIPPAGE_TRIP_COUNT} in last "
            f"{SLIPPAGE_WINDOW_SEC}s)",
            "WARN",
        )

    if recent_abnormal >= SLIPPAGE_TRIP_COUNT and trigger_safe_mode:
        trigger_safe_mode(
            f"slippage CB tripped  {recent_abnormal} abnormal readings "
            f"in {SLIPPAGE_WINDOW_SEC}s"
        )

    return slippage_pct


#  Spread check


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
    try:
        bid_f, ask_f = float(bid), float(ask)
        if bid_f <= 0 or ask_f <= 0:
            if log_event and not missing_ok:
                log_event("Spread check invalid bid/ask  blocking entry", "WARN")
            return bool(missing_ok)
        threshold = max_spread_pct if max_spread_pct is not None else MAX_SPREAD_PCT
        spread_pct = (ask_f - bid_f) / ((ask_f + bid_f) / 2) * 100
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
    except (TypeError, ValueError):
        if log_event and not missing_ok:
            log_event("Spread check invalid ticker values  blocking entry", "WARN")
        return bool(missing_ok)
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
        self._lock = threading.Lock()
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
            except Exception:
                self._alert_state_file = None

    @staticmethod
    def _compute_instance_suffix(instance_id: Optional[str]) -> str:
        """Produce a short hash that distinguishes simultaneous
        instances. Priority:
          1. explicit instance_id arg
          2. BOT_ACCOUNT_ID env var (e.g. account UUID)
          3. PID (last resort  changes on restart, but at least
             distinguishes co-running instances)
        """
        seed = instance_id or os.getenv("BOT_ACCOUNT_ID") or f"pid{os.getpid()}"
        # Stable filename suffix only, never a signature or secret hash.
        return hashlib.sha1(seed.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]

    def _load_alert_state(self) -> None:
        """Load persisted alert state  sets _alert_sent if last alert
        was on the current UTC day (so a restart same-day won't re-fire)."""
        if not self._alert_state_file:
            return
        try:
            import json as _json
            from datetime import datetime as _dt, timezone as _tz

            if not os.path.exists(self._alert_state_file):
                return
            with open(self._alert_state_file, encoding="utf-8") as f:
                data = _json.load(f) or {}
            last_day = data.get("alert_sent_day")
            today = _dt.now(_tz.utc).strftime("%Y-%m-%d")
            if last_day == today:
                self._alert_sent = True
        except Exception:
            pass

    def _save_alert_state(self) -> None:
        """Persist that we sent a Telegram alert today (UTC)."""
        if not self._alert_state_file:
            return
        try:
            import json as _json
            from datetime import datetime as _dt, timezone as _tz

            today = _dt.now(_tz.utc).strftime("%Y-%m-%d")
            with open(self._alert_state_file, "w", encoding="utf-8") as f:
                _json.dump({"alert_sent_day": today, "reason": self._reason}, f)
        except Exception:
            pass

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
            if self._event.is_set():
                return
            self._event.set()
            self._reason = reason
        if self._log_struct:
            try:
                self._log_struct(
                    "safe_mode_triggered", reason=reason, bot=self.bot_name
                )
            except Exception:
                pass
        if self._log_event:
            try:
                self._log_event(
                    f" SAFE_MODE activated: {reason}. New entries "
                    f"DISABLED, monitoring & closing continues.",
                    "WARN",
                )
            except Exception:
                pass
        if not self._alert_sent and self._telegram_send:
            self._alert_sent = True
            self._save_alert_state()
            try:
                self._telegram_send(
                    self._telegram_token,
                    self._telegram_chat_id,
                    f" [{self.bot_name}] SAFE_MODE activated\n"
                    f"Reason: {reason}\n\n"
                    f"No new entries until bot is restarted.\n"
                    f"Existing positions are still being monitored & closed.",
                )
            except Exception:
                pass
