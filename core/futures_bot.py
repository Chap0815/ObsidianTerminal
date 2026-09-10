"""
core/futures_bot.py  Concurrent futures trading bot base class.

Replaces main_bot_futures.run_bot()'s ~300-line lifecycle. The class
inherits exits/scan/reconcile mixins (analog to SpotBot) but is NOT
related to SpotBot via inheritance  futures and spot have different
order types (reduce-only vs spot-sell), different state schema
(position_type LONG/SHORT), and different exit logic (liquidation,
funding).

Architecture (same shape as SpotBot):
  Monitor-Thread  (every MONITOR_INTERVAL ~20s)  exits + liq guard
  Scan-Thread  (every SCAN_INTERVAL ~150s)  LONG/SHORT entries
  Reconcile-Thread  (every RECONCILE_INTERVAL ~600s)  drift check
  Markout-Thread  (every futures bot, every ~1s)  global LIVE/SIM TCA;
                  advisory-lock serialized for cross-process failover
  Main-Thread  heartbeat + shutdown coordination

Plus futures-specific infrastructure built into the lifecycle:
  API budget guard  (shared across all bots)
  Ticker cache + threadpool (futures-grade: 8 workers, 32 in-flight cap)
  Safe-mode circuit breaker

Subclass contract:
  Required class attributes:
    BOT_NAME, BOT_COLOR, LOG_DIR, DB_FILE, COOLDOWN_FILE
    DEFAULTS               dict including LEVERAGE, LIQ_SAFETY_PCT,
                           MAX_DAILY_LOSS, MONITOR_INTERVAL, etc.
    NEWS_MODULE_PATH       e.g. "news.news_brain_futures"
    BUY_PREFIX             clientOrderId prefix (e.g. "fut")
    EXCHANGE_FACTORY       returns connected ccxt futures exchange
"""
from __future__ import annotations

import atexit
import copy
import importlib
import math
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import cached_property, partial
from typing import Optional, Dict, Any

from bot_utils import (
    log_error as _ext_log_error,
    load_runtime_config,
    TradeState,
    TickerCache,
    SafeMode,
    emergency_close_all_futures,
)
from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.runtime_threads import (wait_for_runtime_shutdown,
                                       format_runtime_thread_liveness,
                                       shared_runtime_resource_closers,
                                       start_threads_or_shutdown,
                                       thread_definitely_never_started)
from bot_utils.silent_log import silent_log
from bot_utils.trade_state import state_exposure_count, state_rows_exposure_count
from core.clock import now_utc

from core.futures_bot_exits import (
    FuturesExitsMixin,
    _futures_exit_intent_schema_status,
)
from core.futures_bot_scan import FuturesScanMixin
from core.futures_bot_reconcile import FuturesReconcileMixin


def _record_reserved_api_error(endpoint, reservation) -> None:
    if not isinstance(reservation, ApiCallReservation):
        return
    try:
        record_api_error(endpoint, reservation)
    except Exception:
        pass


_STATE_ROWS_UNSET = object()


class FuturesBot(FuturesExitsMixin, FuturesScanMixin,
                  FuturesReconcileMixin, ABC):
    _SIM_TCA_PENDING_FIELD = "sim_tca_pending_v1"
    #  Subclass-overridable class attributes 
    BOT_NAME: str = "FUTURES"
    BOT_COLOR: str = "\033[95m"
    LOG_DIR: str = ""
    DB_FILE: str = ""
    COOLDOWN_FILE: str = ""
    DEFAULTS: Dict[str, Any] = {}
    NEWS_MODULE_PATH: str = "news.news_brain_futures"
    BUY_PREFIX: str = "fut"
    BACKTEST_NOTE: str = ""
    # Only the directional FUTURES implementation uses the aged MFE fallback
    # exit in FuturesExitsMixin. CROSS and FUTREND share this lifecycle class
    # but have separate exit engines and must not advertise that control.
    USES_AGED_MFE_FALLBACK: bool = False

    # Reconciliation cadence (constants  could be overridden if needed)
    RECONCILE_INTERVAL_SEC: int = 300   # orphan-adoption safety net cadence
    GC_LOCKS_INTERVAL_SEC: int = 300
    HEARTBEAT_INTERVAL_SEC: int = 60
    MARKOUT_POLL_INTERVAL_SEC: float = 1.0
    MARKOUT_MAX_OVERDUE_SEC: float = 30.0
    MARKOUT_POLL_STALE_SEC: float = 15.0
    VENUE_HEALTH_MIN_STALE_SEC: float = 30.0
    ENTRY_RECOVERY_RETRY_BASE_SEC: int = 15
    ENTRY_RECOVERY_RETRY_MAX_SEC: int = 300
    ENTRY_RECOVERY_BATCH_MAX: int = 8
    ENTRY_RECOVERY_LOG_REMINDER_SEC: float = 300.0

    # Monitor / Scan defaults
    DEFAULT_MONITOR_INTERVAL: int = 20

    # Ticker cache pool sizing (bounded backpressure)
    TICKER_POOL_SIZE: int = 8
    TICKER_INFLIGHT_MAX: int = 32
    TICKER_HEALTH_FAILURE_THRESHOLD: int = 8
    TICKER_HEALTH_SUCCESS_STALE_SEC: float = 30.0

    # Circuit breaker
    CB_FAILURE_THRESHOLD: int = 5
    CB_MAX_BACKOFF: float = 600.0
    CB_INITIAL_BACKOFF: float = 30.0

    # Shutdown deadline  emergency close has this many seconds before
    # we give up and let the process exit
    SHUTDOWN_DEADLINE_SEC: float = 45.0

    #  Lifecycle 

    def __init__(self, simulation: bool = True):
        self.simulation = simulation
        # Isolate SIM state from LIVE state: redirect ALL state/cooldown file
        # usage (load, TradeState, exit-path cooldowns) to a .sim variant by
        # reassigning the instance attrs here, before they're first used.
        from bot_utils.sim_flag import sim_state_path
        self.DB_FILE = sim_state_path(self.DB_FILE, simulation)
        self.COOLDOWN_FILE = sim_state_path(self.COOLDOWN_FILE, simulation)
        self.cfg = load_runtime_config(self.BOT_NAME, self.DEFAULTS)
        self._shutdown_event = threading.Event()
        self._reconcile_wakeup_event = threading.Event()
        self._entry_recovery_lock = threading.Lock()
        self._entry_recovery_generation = 0
        self._shutdown_lock = threading.Lock()
        self._shutdown_handler_lock = threading.Lock()
        self._shutdown_request_publish_lock = threading.RLock()
        self._shutdown_close_requested = threading.Event()
        self._shutdown_close_request_generation = None
        self._cooldown_lock = threading.Lock()
        self._markout_health_lock = threading.Lock()
        self._venue_health_lock = threading.Lock()
        self._private_api_health_lock = threading.Lock()
        self._position_integrity_health_lock = threading.Lock()
        # populated in run()
        self.ex = None
        self.state: Optional[TradeState] = None
        self.cool: dict = {}
        self.ticker_cache: Optional[TickerCache] = None
        self.safe_mode: Optional[SafeMode] = None
        # Idempotency flag for emergency close (prevents double-run when both
        # SIGINT and atexit fire)
        self._emergency_closed = False
        self._emergency_close_generation = None
        self._shutdown_positions_preserved = False

        # Threads
        self._monitor_thread: Optional[threading.Thread] = None
        self._scan_thread: Optional[threading.Thread] = None
        self._reconcile_thread: Optional[threading.Thread] = None
        self._markout_thread: Optional[threading.Thread] = None
        self._venue_recorder_thread: Optional[threading.Thread] = None
        self._venue_recorder = None
        self._markout_started_monotonic: float | None = None
        self._sim_evidence_health_cache: dict[str, Any] | None = None
        self._sim_evidence_health_last_monotonic: float | None = None
        self._venue_started_monotonic: float | None = None
        self._position_integrity_started_monotonic = time.monotonic()
        self._position_integrity_health: dict[str, Any] = {}
        self._venue_health_stale_sec = self.VENUE_HEALTH_MIN_STALE_SEC
        self._markout_health = {
            "ok": True,
            "last_poll_monotonic": None,
            "last_poll_wall_ts": None,
            "consecutive_errors": 0,
            "last_error": "",
            "due_count": 0,
            "oldest_due_at": None,
            "oldest_overdue_seconds": 0.0,
            "next_runnable_at": None,
            "next_runnable_seconds": None,
            "timestamps_valid": True,
            "reason": "",
            "scopes": {
                scope: {
                    "due_count": 0,
                    "oldest_due_at": None,
                    "oldest_overdue_seconds": 0.0,
                    "next_runnable_at": None,
                    "next_runnable_seconds": None,
                    "timestamps_valid": True,
                }
                for scope in ("LIVE", "SIM")
            },
            "completed": 0,
            "completed_batch": 0,
            "completed_total": 0,
            "polls_total": 0,
            "errors_total": 0,
            "poll_wait_seconds": self.MARKOUT_POLL_INTERVAL_SEC,
            "wake_strategy": "starting",
            "last_completed_wall_ts": None,
            "lock_state": "starting",
            "worker_family": "futures",
            "producer_bots": ["CROSS", "FUTREND", "FUTURES"],
            "progress_scope": "worker_market_family",
            "progress_is_bot_scoped": False,
        }
        self._venue_health: dict[str, Any] = {}
        self._private_api_health: dict[str, Any] = {
            "ok": None,
            "component": "private_api",
            "state": "unverified",
            "reason": "private_api_unverified",
            "error_type": "",
        }
        self._entry_recovery_blocked = False
        self._entry_recovery_health: dict[str, Any] = {
            "ok": True,
            "component": "entry_recovery",
            "state": "clear",
            "reason": "",
            "unresolved_count": 0,
        }
        self._entry_recovery_last_log_key = None
        self._entry_recovery_last_log_monotonic = 0.0
        self._entry_recovery_position_snapshot = None

    # Route through bot_utils.config.get_live_value so that user-edited values
    # in the launcher's settings UI take effect within ~5 s without a bot
    # restart. Falls back to the cached boot-time snapshot when the live read
    # isn't possible.
    def C(self, key: str, default=None):
        try:
            from bot_utils.config import get_live_value
            return get_live_value(self.BOT_NAME, key, default,
                                   fallback_cfg=self.cfg)
        except Exception:
            if key == "NEW_ENTRIES_ENABLED":
                return False
            return self.cfg.get(key, default)

    def _venue_recorder_requested(self) -> bool:
        """Return whether this installation explicitly enables research capture."""
        return bool(
            self.BOT_NAME == "FUTURES"
            and str(
                self.C("VENUE_RECORDER_MODE", "disabled")
            ).strip().lower() == "enabled"
        )

    def _new_simulated_entry_tca_pending(
        self,
        *,
        entry_id: str,
        symbol: str,
        side: str,
        amount: float,
        fill_price: float,
        fee_rate: float,
        notional_usdt: float,
        arrival_unavailable_reason: str | None = None,
    ) -> dict[str, Any]:
        """Build the bounded SIM-evidence WAL committed with position state."""
        from core.logger import _date as _utc_now_str

        pending = {
            "version": 1,
            "entry_id": str(entry_id),
            "bot_name": str(self.BOT_NAME).upper(),
            "symbol": str(symbol),
            "side": str(side).lower(),
            "amount": float(amount),
            "fill_price": float(fill_price),
            "fee_rate": float(fee_rate),
            "notional_usdt": float(notional_usdt),
            "filled_at": _utc_now_str(),
        }
        if arrival_unavailable_reason is not None:
            allowed_reasons = {
                "capture_contract_size_unavailable",
                "entry_orderbook_unavailable",
            }
            if arrival_unavailable_reason not in allowed_reasons:
                raise ValueError("SIM TCA arrival-unavailable reason is invalid")
            pending["arrival_unavailable_reason"] = arrival_unavailable_reason
        return pending

    def _finalize_simulated_entry_tca(
        self,
        base: str,
        pending: dict,
        *,
        restart_recovery: bool = False,
        arrival_book: dict | None = None,
    ) -> bool:
        """Persist post-state SIM evidence; retain WAL until it is durable."""
        context = f"{self.BOT_NAME} {base} SIM TCA durable finalize"
        try:
            if not isinstance(pending, dict) or pending.get("version") != 1:
                raise ValueError("invalid SIM TCA pending schema")
            row = self.state.get(base)
            if (
                not isinstance(row, dict)
                or row.get("entry_id") != pending.get("entry_id")
                or row.get(self._SIM_TCA_PENDING_FIELD) != pending
                or pending.get("bot_name") != str(self.BOT_NAME).upper()
                or pending.get("side") not in {"buy", "sell"}
            ):
                raise ValueError("SIM TCA pending evidence conflicts with state")

            from core.database import has_durable_simulated_entry_tca

            evidence_durable = has_durable_simulated_entry_tca(
                pending["entry_id"], pending["bot_name"]
            )
            unavailable_reason = str(
                pending.get("arrival_unavailable_reason") or ""
            )[:64]
            if not evidence_durable and (restart_recovery or unavailable_reason):
                from core.database import (
                    persist_simulated_entry_tca_unavailable_bundle,
                )

                persist_simulated_entry_tca_unavailable_bundle(
                    pending["entry_id"],
                    bot_name=pending["bot_name"],
                    symbol=pending["symbol"],
                    side=pending["side"],
                    reference_price=pending["fill_price"],
                    measured_at=pending["filled_at"],
                    reason=(
                        unavailable_reason
                        or "restart_recovery_without_arrival_book"
                    ),
                    error_type=(
                        "ContractSizeUnavailable"
                        if unavailable_reason
                        == "capture_contract_size_unavailable"
                        else "EntryOrderBookUnavailable"
                        if unavailable_reason == "entry_orderbook_unavailable"
                        else "ArrivalBookNotRecoverable"
                    ),
                )
            elif not evidence_durable:
                from trading.candidate_microstructure import (
                    capture_simulated_entry_tca,
                )

                capture_simulated_entry_tca(
                    exchange=self.ex,
                    entry_id=pending["entry_id"],
                    bot_name=pending["bot_name"],
                    mode="SIM",
                    symbol=pending["symbol"],
                    side=pending["side"],
                    amount=pending["amount"],
                    fill_price=pending["fill_price"],
                    fee_rate=pending["fee_rate"],
                    notional_usdt=pending["notional_usdt"],
                    filled_at=pending["filled_at"],
                    depth_levels=int(self.C("TCA_DEPTH_LEVELS", 20)),
                    arrival_book=arrival_book,
                )

            if not has_durable_simulated_entry_tca(
                pending["entry_id"], pending["bot_name"]
            ):
                raise RuntimeError("SIM TCA evidence is not durable")
            latest = self.state.get(base)
            if (
                not isinstance(latest, dict)
                or latest.get("entry_id") != pending["entry_id"]
                or latest.get(self._SIM_TCA_PENDING_FIELD) != pending
            ):
                raise RuntimeError("SIM TCA state changed before WAL clear")
            from bot_utils.trade_state import (
                same_position_generation,
                update_many_if_current,
            )

            clear_fields = {self._SIM_TCA_PENDING_FIELD: None}
            cleared = update_many_if_current(
                self.state, base, clear_fields, latest,
            )
            if cleared is not None and cleared is not True:
                raise RuntimeError("SIM TCA WAL clear was not durable")
            current = self.state.get(base)
            if not (
                isinstance(current, dict)
                and (
                    same_position_generation(current, latest)
                    or current == {**latest, **clear_fields}
                )
                and current.get(self._SIM_TCA_PENDING_FIELD) is None
            ):
                raise RuntimeError("SIM TCA WAL clear was not durable")
            return True
        except Exception as exc:
            silent_log(context, exc)
            return False

    def _recover_simulated_entry_tca_pending(self) -> int:
        """Close restart-surviving SIM evidence WALs without false book data."""
        if not self.simulation:
            return 0
        recovered = 0
        for base, row in self.state.get_all().items():
            pending = (
                row.get(self._SIM_TCA_PENDING_FIELD)
                if isinstance(row, dict)
                else None
            )
            if isinstance(pending, dict) and self._finalize_simulated_entry_tca(
                base, pending, restart_recovery=True
            ):
                recovered += 1
        return recovered

    @cached_property
    def _news(self):
        return importlib.import_module(self.NEWS_MODULE_PATH)

    def _log_error(self, context: str, exc: Exception) -> None:
        _ext_log_error(self.BOT_NAME, context, exc)

    def _startup_mfe_fallback_fields(self) -> dict[str, Any]:
        """Return only telemetry for an exit capability this bot actually uses."""
        if not self.USES_AGED_MFE_FALLBACK:
            return {}
        return {
            "mfe_fallback_enabled": self.C(
                "MFE_FALLBACK_STOP_ENABLED", True),
            "mfe_fallback_min_age_minutes": self.C(
                "MFE_FALLBACK_MIN_AGE_MINUTES", 45.0),
            "mfe_fallback_min_mfe_pct": self.C(
                "MFE_FALLBACK_MIN_MFE_PCT", 0.8),
            "mfe_fallback_exit_move_pct": self.C(
                "MFE_FALLBACK_EXIT_MOVE_PCT", -1.5),
        }

    def _strategy_runtime_health(self) -> dict[str, Any]:
        """Subclass hook for live strategy-loop health beyond thread liveness."""
        return {}

    def _ticker_runtime_health(self) -> dict[str, Any]:
        """Expose a sustained ticker outage without flagging idle startup."""
        cache = getattr(self, "ticker_cache", None)
        health_fn = getattr(cache, "health", None)
        if not callable(health_fn):
            return {}
        try:
            health = health_fn(
                failure_threshold=self.TICKER_HEALTH_FAILURE_THRESHOLD,
                success_stale_after=self.TICKER_HEALTH_SUCCESS_STALE_SEC,
            )
            if not isinstance(health, dict):
                return {
                    "ok": False,
                    "component": "ticker_cache",
                    "error_type": "invalid_ticker_health_payload",
                }
            return health
        except Exception as exc:
            return {
                "ok": False,
                "component": "ticker_cache",
                "error_type": type(exc).__name__,
            }

    def _entry_recovery_runtime_health(self) -> dict[str, Any]:
        """Expose a blocked entry journal as degraded runtime health."""
        missing_health = object()
        lock = getattr(self, "_entry_recovery_lock", None)
        if lock is None:
            blocked = bool(getattr(self, "_entry_recovery_blocked", False))
            value = getattr(self, "_entry_recovery_health", missing_health)
            health = (
                missing_health
                if value is missing_health
                else copy.deepcopy(value)
            )
        else:
            with lock:
                blocked = bool(
                    getattr(self, "_entry_recovery_blocked", False)
                )
                value = getattr(
                    self, "_entry_recovery_health", missing_health
                )
                health = (
                    missing_health
                    if value is missing_health
                    else copy.deepcopy(value)
                )
        if health is missing_health:
            # Support lifecycle-light diagnostic hosts. Fully initialized bots
            # always own this field before any worker or status publication.
            if not blocked:
                return {}
            health = {}
        if not isinstance(health, dict) or not health:
            return {
                "ok": False,
                "component": "entry_recovery",
                "state": "unavailable",
                "reason": "invalid_health_payload",
                "unresolved_count": 1,
            }
        result = health
        if not blocked and result.get("ok") is True:
            return {}
        if blocked and result.get("ok") is not False:
            try:
                unresolved_count = max(
                    1, int(result.get("unresolved_count") or 0)
                )
            except (TypeError, ValueError, OverflowError):
                unresolved_count = 1
            result = {
                "ok": False,
                "component": "entry_recovery",
                "state": "blocked",
                "reason": "barrier_blocked",
                "unresolved_count": unresolved_count,
            }
        elif not result:
            result = {
                "ok": not blocked,
                "component": "entry_recovery",
                "state": "blocked" if blocked else "clear",
                "reason": "barrier_blocked" if blocked else "",
                "unresolved_count": 1 if blocked else 0,
            }
        return result

    def _entry_recovery_allows_live_open(self) -> bool:
        """Fail closed when the shared futures entry barrier is unavailable."""
        if bool(getattr(self, "simulation", True)):
            return True
        try:
            health = self._entry_recovery_runtime_health()
        except Exception:
            return False
        return isinstance(health, dict) and (
            not health or health.get("ok") is True
        )

    def _exit_recovery_runtime_health(
        self,
        *,
        state_rows: Any = _STATE_ROWS_UNSET,
        state_error_type: str = "",
    ) -> dict[str, Any]:
        """Expose durable LIVE exit-order intents as degraded health."""
        if bool(getattr(self, "simulation", True)):
            return {}
        if state_error_type:
            return {
                "ok": False,
                "component": "exit_recovery",
                "state": "unavailable",
                "reason": "exit_state_unavailable",
                "error_type": state_error_type[:64],
            }
        if state_rows is _STATE_ROWS_UNSET:
            state = getattr(self, "state", None)
            get_all = getattr(state, "get_all", None)
            if not callable(get_all):
                return {
                    "ok": False,
                    "component": "exit_recovery",
                    "state": "unavailable",
                    "reason": "exit_state_unavailable",
                }
            try:
                rows = get_all()
            except Exception as exc:
                return {
                    "ok": False,
                    "component": "exit_recovery",
                    "state": "unavailable",
                    "reason": "exit_state_unavailable",
                    "error_type": type(exc).__name__,
                }
        else:
            rows = state_rows
        if not isinstance(rows, dict):
            return {
                "ok": False,
                "component": "exit_recovery",
                "state": "unavailable",
                "reason": "invalid_exit_state_payload",
            }
        partial_symbols = []
        full_symbols = []
        invalid_row_count = 0
        for symbol, row in rows.items():
            if not isinstance(row, dict):
                invalid_row_count += 1
                continue
            try:
                safe_symbol = str(symbol)[:32]
            except BaseException:
                invalid_row_count += 1
                continue
            partial_present, partial_valid = (
                _futures_exit_intent_schema_status(row, "partial")
            )
            full_present, full_valid = _futures_exit_intent_schema_status(
                row, "full"
            )
            row_invalid = False
            if partial_present:
                if partial_valid:
                    partial_symbols.append(safe_symbol)
                else:
                    row_invalid = True
            if full_present:
                if full_valid:
                    full_symbols.append(safe_symbol)
                else:
                    row_invalid = True
            if row_invalid:
                invalid_row_count += 1
        if invalid_row_count:
            return {
                "ok": False,
                "component": "exit_recovery",
                "state": "unavailable",
                "reason": "invalid_exit_state_payload",
                "invalid_row_count": invalid_row_count,
            }
        unresolved_count = len(partial_symbols) + len(full_symbols)
        if unresolved_count == 0:
            return {}
        return {
            "ok": False,
            "component": "exit_recovery",
            "state": "blocked",
            "reason": "unresolved_exit_order_intent",
            "unresolved_count": unresolved_count,
            "partial_count": len(partial_symbols),
            "full_count": len(full_symbols),
            "symbols": sorted(set(partial_symbols + full_symbols))[:16],
        }

    def _record_position_integrity_health(
        self,
        report: dict,
        *,
        telemetry_phase: str,
    ) -> None:
        """Store one bounded State/Claim/Exchange comparison for status."""
        from trading.runtime_observability import (
            position_integrity_runtime_report,
        )

        projected = position_integrity_runtime_report(
            report,
            telemetry_phase=telemetry_phase,
            checked_monotonic=time.monotonic(),
            checked_wall_ts=time.time(),
        )
        with self._position_integrity_health_lock:
            self._position_integrity_health = projected

    def _position_integrity_runtime_health(self) -> dict[str, Any]:
        """Return freshness-aware integrity health for LIVE trading only."""
        if bool(getattr(self, "simulation", True)):
            return {}
        lock = getattr(self, "_position_integrity_health_lock", None)
        if lock is None:
            return {}
        with lock:
            health = dict(
                getattr(self, "_position_integrity_health", {}) or {}
            )
        from trading.runtime_observability import (
            position_integrity_runtime_snapshot,
        )

        return position_integrity_runtime_snapshot(
            health,
            started_monotonic=getattr(
                self, "_position_integrity_started_monotonic", None
            ),
            reconcile_interval_seconds=self.RECONCILE_INTERVAL_SEC,
            now_monotonic=time.monotonic(),
        )

    def _record_private_api_health(
        self,
        *,
        ok: bool,
        reason: str = "",
        error_type: str = "",
    ) -> bool:
        """Store authentication health and report whether its state changed."""
        valid_ok = type(ok) is bool
        healthy = ok is True

        def bounded_text(value, default: str) -> str:
            if value is None:
                return default
            try:
                rendered = str(value)[:80]
            except BaseException:
                return "unrenderable_health_detail"
            return rendered or default

        if not valid_ok:
            health_reason = "invalid_health_payload"
            health_error_type = "invalid_ok_type"
        elif healthy:
            health_reason = ""
            health_error_type = ""
        else:
            health_reason = bounded_text(reason, "authentication_failed")
            health_error_type = bounded_text(error_type, "unknown")
        snapshot = {
            "ok": healthy,
            "component": "private_api",
            "state": "authenticated" if healthy else "authentication_failed",
            "reason": health_reason,
            "error_type": health_error_type,
            "checked_monotonic": time.monotonic(),
            "checked_wall_ts": time.time(),
        }
        lock = getattr(self, "_private_api_health_lock", None)
        if lock is None:
            previous = dict(getattr(self, "_private_api_health", {}) or {})
            self._private_api_health = snapshot
        else:
            with lock:
                previous = dict(getattr(self, "_private_api_health", {}) or {})
                self._private_api_health = snapshot
        return (
            previous.get("ok"),
            previous.get("reason"),
            previous.get("error_type"),
        ) != (
            snapshot["ok"],
            snapshot["reason"],
            snapshot["error_type"],
        )

    def _private_api_runtime_health(self) -> dict[str, Any]:
        """Expose explicit private-API authentication state for LIVE bots."""
        if bool(getattr(self, "simulation", True)):
            return {}
        lock = getattr(self, "_private_api_health_lock", None)
        if lock is None:
            return {}
        with lock:
            snapshot = dict(getattr(self, "_private_api_health", {}) or {})
        if snapshot.get("ok") is not True:
            return snapshot
        try:
            interval = float(self.RECONCILE_INTERVAL_SEC)
            if not math.isfinite(interval) or interval < 0.0:
                raise ValueError("invalid reconcile interval")
        except (AttributeError, TypeError, ValueError, OverflowError):
            interval = 300.0
        stale_after = max(30.0, 2.0 * interval + 15.0)
        checked = snapshot.get("checked_monotonic")
        try:
            check_age = time.monotonic() - float(checked)
            if not math.isfinite(check_age) or check_age < 0.0:
                raise ValueError("private API check timestamp is invalid")
        except (TypeError, ValueError, OverflowError):
            snapshot.update({
                "ok": False,
                "state": "invalid",
                "reason": "private_api_timestamp_invalid",
                "check_age_seconds": None,
                "stale_after_seconds": stale_after,
            })
            return snapshot
        snapshot.update({
            "check_age_seconds": check_age,
            "stale_after_seconds": stale_after,
        })
        if check_age > stale_after:
            snapshot.update({
                "ok": False,
                "state": "stale",
                "reason": "private_api_check_stale",
            })
        return snapshot

    def _runtime_status_health(
        self,
        threads: dict[str, bool],
        *,
        state_rows: Any = _STATE_ROWS_UNSET,
        state_error_type: str = "",
    ) -> tuple[str, dict[str, Any]]:
        """Combine worker liveness with reported component health."""
        def read_health(reader) -> dict[str, Any]:
            try:
                health = reader()
            except Exception as exc:
                return {
                    "ok": False,
                    "error_type": type(exc).__name__,
                }
            if not isinstance(health, dict):
                return {
                    "ok": False,
                    "error_type": "invalid_health_payload",
                }
            try:
                snapshot = copy.deepcopy(health)
            except Exception as exc:
                return {
                    "ok": False,
                    "error_type": type(exc).__name__,
                }
            if not isinstance(snapshot, dict):
                return {
                    "ok": False,
                    "error_type": "invalid_health_payload",
                }
            return snapshot

        required_threads = ("monitor", "reconcile", "scan")
        thread_payload_valid = isinstance(threads, dict)
        missing_threads = (
            sorted(name for name in required_threads if name not in threads)
            if thread_payload_valid
            else list(required_threads)
        )
        invalid_threads = (
            sorted(
                name
                for name, value in threads.items()
                if isinstance(name, str) and not isinstance(value, bool)
            )
            if thread_payload_valid
            else []
        )
        invalid_thread_keys = bool(
            thread_payload_valid
            and any(not isinstance(name, str) for name in threads)
        )
        thread_payload_valid = bool(
            thread_payload_valid
            and not missing_threads
            and not invalid_threads
            and not invalid_thread_keys
        )
        threads_ok = bool(
            thread_payload_valid and all(value is True for value in threads.values())
        )
        thread_health = {}
        if not thread_payload_valid:
            thread_health = {
                "ok": False,
                "component": "runtime_threads",
                "state": "invalid",
                "reason": "invalid_thread_liveness",
                "missing_threads": missing_threads,
                "invalid_threads": invalid_threads,
            }
            if invalid_thread_keys:
                thread_health["invalid_key_count"] = sum(
                    1 for name in threads if not isinstance(name, str)
                )
        state_snapshot_health = {}
        if state_error_type:
            state_snapshot_health = {
                "ok": False,
                "component": "state_snapshot",
                "state": "unavailable",
                "reason": "state_snapshot_unavailable",
                "error_type": state_error_type[:64],
            }

        strategy_health = read_health(self._strategy_runtime_health)
        if strategy_health.get("error_type") == "invalid_health_payload":
            strategy_health = {
                "ok": False,
                "error_type": "invalid_strategy_health_payload",
            }
        strategy_ok = not strategy_health or strategy_health.get("ok") is True
        ticker_health = read_health(self._ticker_runtime_health)
        ticker_ok = not ticker_health or ticker_health.get("ok") is True
        markout_health = read_health(self._markout_runtime_health)
        markout_ok = not markout_health or markout_health.get("ok") is True
        sim_evidence_health = read_health(self._sim_evidence_runtime_health)
        sim_evidence_ok = (
            not sim_evidence_health
            or sim_evidence_health.get(
                "runtime_ok", sim_evidence_health.get("ok")
            ) is True
        )
        venue_health = read_health(self._venue_runtime_health)
        venue_ok = not venue_health or venue_health.get("ok") is True
        private_api_health = read_health(self._private_api_runtime_health)
        private_api_ok = (
            not private_api_health
            or private_api_health.get("ok") is True
        )
        entry_recovery_health = read_health(
            self._entry_recovery_runtime_health
        )
        entry_recovery_ok = (
            not entry_recovery_health
            or entry_recovery_health.get("ok") is True
        )
        exit_recovery_health = read_health(
            lambda: self._exit_recovery_runtime_health(
                state_rows=state_rows,
                state_error_type=state_error_type,
            )
        )
        exit_recovery_ok = (
            not exit_recovery_health
            or exit_recovery_health.get("ok") is True
        )
        position_integrity_health = read_health(
            self._position_integrity_runtime_health
        )
        position_integrity_ok = (
            not position_integrity_health
            or position_integrity_health.get(
                "runtime_ok", position_integrity_health.get("ok")
            ) is True
        )
        status = (
            "ready"
            if (
                threads_ok
                and not state_snapshot_health
                and strategy_ok
                and ticker_ok
                and markout_ok
                and sim_evidence_ok
                and venue_ok
                and private_api_ok
                and entry_recovery_ok
                and exit_recovery_ok
                and position_integrity_ok
            )
            else "degraded"
        )
        extra = {}
        if thread_health:
            extra["thread_health"] = thread_health
        if state_snapshot_health:
            extra["state_snapshot_health"] = state_snapshot_health
        if strategy_health:
            extra["strategy_health"] = strategy_health
        if ticker_health:
            extra["ticker_health"] = ticker_health
        if markout_health:
            extra["markout_health"] = markout_health
        if sim_evidence_health:
            extra["sim_evidence_health"] = sim_evidence_health
        if venue_health:
            extra["venue_health"] = venue_health
        if private_api_health:
            extra["private_api_health"] = private_api_health
        if entry_recovery_health:
            extra["entry_recovery_health"] = entry_recovery_health
        if exit_recovery_health:
            extra["exit_recovery_health"] = exit_recovery_health
        if position_integrity_health:
            extra["position_integrity_health"] = position_integrity_health
        return status, extra

    def _owns_markout_worker(self) -> bool:
        """Let any futures strategy provide failover for the global queue."""
        return str(self.BOT_NAME).upper() in {"FUTURES", "CROSS", "FUTREND"}

    def _record_markout_worker_health(self, report: dict) -> None:
        """Receive one sanitized progress report from the markout thread."""
        if not isinstance(report, dict):
            with self._markout_health_lock:
                try:
                    previous_errors = max(
                        0,
                        int(
                            self._markout_health.get("consecutive_errors")
                            or 0
                        ),
                    )
                except (TypeError, ValueError, OverflowError):
                    previous_errors = 0
                self._markout_health.update({
                    "ok": False,
                    "reason": "invalid_health_payload",
                    "timestamps_valid": False,
                    "consecutive_errors": previous_errors + 1,
                    "last_error": "invalid markout health payload",
                })
            return
        report_ok = report.get("ok") is True
        payload_contract_valid = True

        def invalidate_payload_contract() -> None:
            nonlocal payload_contract_valid
            payload_contract_valid = False

        def bounded_text(value, *, max_chars: int, default: str = "") -> str:
            if value is None:
                return default[:max_chars]
            try:
                return str(value)[:max_chars]
            except BaseException:
                invalidate_payload_contract()
                return default[:max_chars]

        def optional_bounded_text(value, *, max_chars: int) -> str | None:
            if value is None:
                return None
            try:
                if not value:
                    return None
            except BaseException:
                invalidate_payload_contract()
                return None
            return bounded_text(value, max_chars=max_chars) or None

        def nonnegative_int(value) -> int:
            if isinstance(value, bool):
                return 0
            try:
                return max(0, int(value or 0))
            except BaseException:
                invalidate_payload_contract()
                return 0

        def nonnegative_float(value) -> float:
            if isinstance(value, bool):
                return 0.0
            try:
                parsed = float(value or 0.0)
            except BaseException:
                invalidate_payload_contract()
                return 0.0
            if not math.isfinite(parsed):
                invalidate_payload_contract()
                return 0.0
            return max(0.0, parsed)

        def optional_nonnegative_float(value) -> float | None:
            if value is None or isinstance(value, bool):
                return None
            try:
                parsed = float(value)
            except BaseException:
                invalidate_payload_contract()
                return None
            if not math.isfinite(parsed):
                invalidate_payload_contract()
                return None
            return max(0.0, parsed)

        def optional_timestamp(value) -> float | None:
            if value is None:
                return None
            if isinstance(value, bool):
                invalidate_payload_contract()
                return None
            try:
                parsed = float(value)
            except BaseException:
                invalidate_payload_contract()
                return None
            if not math.isfinite(parsed) or parsed < 0.0:
                invalidate_payload_contract()
                return None
            return parsed

        raw_scopes = report.get("scopes")
        raw_scopes = raw_scopes if isinstance(raw_scopes, dict) else {}
        scopes = {}
        for scope in ("LIVE", "SIM"):
            raw = raw_scopes.get(scope)
            raw = raw if isinstance(raw, dict) else {}
            due_count = nonnegative_int(raw.get("due_count"))
            overdue = nonnegative_float(raw.get("oldest_overdue_seconds"))
            oldest = raw.get("oldest_due_at")
            next_runnable = raw.get("next_runnable_at")
            scopes[scope] = {
                "due_count": due_count,
                "oldest_due_at": optional_bounded_text(
                    oldest, max_chars=32
                ),
                "oldest_overdue_seconds": overdue,
                "next_runnable_at": optional_bounded_text(
                    next_runnable, max_chars=32
                ),
                "next_runnable_seconds": optional_nonnegative_float(
                    raw.get("next_runnable_seconds")
                ),
                "timestamps_valid": raw.get("timestamps_valid", True) is True,
            }
        allowed_reasons = {
            "",
            "due_queue_overdue",
            "due_queue_time_invalid",
            "worker_error",
            "worker_lock_error",
            "worker_lock_integrity_error",
            "worker_lock_lost",
            "worker_lock_renewal_error",
            "worker_lock_release_failed",
            "worker_persistence_error",
            "worker_queue_error",
        }
        reason = bounded_text(report.get("reason"), max_chars=64)
        if report_ok:
            reason = ""
        elif reason not in allowed_reasons or not reason:
            reason = "worker_error"
        wake_strategy = bounded_text(
            report.get("wake_strategy"),
            max_chars=32,
            default="fixed_interval",
        )
        if wake_strategy not in {
            "deadline_or_local_commit",
            "fixed_interval",
            "fixed_error_backoff",
        }:
            wake_strategy = "fixed_interval"
        lock_state = bounded_text(
            report.get("lock_state"),
            max_chars=32,
            default="unknown",
        )
        if lock_state not in {
            "starting",
            "idle",
            "unknown",
            "acquired",
            "contended",
            "error",
            "integrity_error",
            "lost",
            "persistence_error",
            "queue_error",
            "renewal_error",
            "release_failed",
        } or report_ok and lock_state in {
            "error",
            "integrity_error",
            "lost",
            "persistence_error",
            "queue_error",
            "renewal_error",
            "release_failed",
        }:
            invalidate_payload_contract()
        with self._markout_health_lock:
            previous_errors = nonnegative_int(
                self._markout_health.get("consecutive_errors")
            )
            self._markout_health.update({
                "ok": report_ok,
                "last_poll_monotonic": optional_timestamp(
                    report.get("last_poll_monotonic")
                ),
                "last_poll_wall_ts": optional_timestamp(
                    report.get("last_poll_wall_ts")
                ),
                "consecutive_errors": (
                    0 if report_ok else previous_errors + 1
                ),
                "last_error": (
                    ""
                    if report_ok
                    else bounded_text(
                        report.get("error")
                        or report.get("reason")
                        or "markout worker unhealthy",
                        max_chars=200,
                    )
                ),
                "due_count": nonnegative_int(report.get("due_count")),
                "oldest_due_at": optional_bounded_text(
                    report.get("oldest_due_at"),
                    max_chars=32,
                ),
                "oldest_overdue_seconds": nonnegative_float(
                    report.get("oldest_overdue_seconds")
                ),
                "next_runnable_at": optional_bounded_text(
                    report.get("next_runnable_at"),
                    max_chars=32,
                ),
                "next_runnable_seconds": optional_nonnegative_float(
                    report.get("next_runnable_seconds")
                ),
                "timestamps_valid": report.get("timestamps_valid", True) is True,
                "reason": reason,
                "scopes": scopes,
                "completed": nonnegative_int(report.get("completed")),
                "completed_batch": nonnegative_int(
                    report.get("completed_batch")
                ),
                "completed_total": nonnegative_int(
                    report.get("completed_total")
                ),
                "polls_total": nonnegative_int(report.get("polls_total")),
                "errors_total": nonnegative_int(report.get("errors_total")),
                "poll_wait_seconds": nonnegative_float(
                    report.get("poll_wait_seconds")
                ),
                "wake_strategy": wake_strategy,
                "last_completed_wall_ts": optional_timestamp(
                    report.get("last_completed_wall_ts")
                ),
                "lock_state": lock_state,
                "worker_family": "futures",
                "producer_bots": ["CROSS", "FUTREND", "FUTURES"],
                "progress_scope": "worker_market_family",
                "progress_is_bot_scoped": False,
            })
            if report_ok and not payload_contract_valid:
                self._markout_health.update({
                    "ok": False,
                    "reason": "invalid_health_payload",
                    "timestamps_valid": False,
                    "consecutive_errors": previous_errors + 1,
                    "last_error": "invalid markout health payload",
                })

    def _markout_runtime_health(self) -> dict[str, Any]:
        if not self._owns_markout_worker() or not hasattr(
            self, "_markout_health_lock"
        ):
            return {}
        with self._markout_health_lock:
            snapshot = copy.deepcopy(self._markout_health)
        last_poll = snapshot.get("last_poll_monotonic")
        if last_poll is None:
            started = getattr(self, "_markout_started_monotonic", None)
            if started is None:
                startup_age = 0.0
            else:
                try:
                    startup_age = time.monotonic() - float(started)
                    if not math.isfinite(startup_age) or startup_age < 0.0:
                        raise ValueError("markout startup timestamp is invalid")
                except BaseException:
                    snapshot.update({
                        "ok": False,
                        "component": "execution_markout_worker",
                        "state": "invalid",
                        "reason": "startup_timestamp_invalid",
                        "startup_age_seconds": None,
                    })
                    return snapshot
            startup_stale = startup_age > self.MARKOUT_POLL_STALE_SEC
            snapshot.update({
                "ok": not startup_stale,
                "component": "execution_markout_worker",
                "state": "stalled" if startup_stale else "starting",
                "startup_age_seconds": startup_age,
            })
            if startup_stale:
                snapshot["reason"] = "startup_poll_stale"
            return snapshot
        try:
            poll_age = time.monotonic() - float(last_poll)
            if not math.isfinite(poll_age) or poll_age < 0.0:
                raise ValueError("markout poll timestamp is invalid")
        except BaseException:
            snapshot.update({
                "ok": False,
                "component": "execution_markout_worker",
                "state": "invalid",
                "reason": "poll_timestamp_invalid",
                "poll_age_seconds": None,
            })
            return snapshot
        if poll_age > self.MARKOUT_POLL_STALE_SEC:
            snapshot["ok"] = False
            snapshot["reason"] = "poll_stale"
        snapshot.update({
            "component": "execution_markout_worker",
            "poll_age_seconds": poll_age,
        })
        return snapshot

    @staticmethod
    def _sim_tca_pending_state_health(
        state_rows: Any,
        bot_name: str,
        *,
        now_wall: float | None = None,
        grace_seconds: int = 60,
    ) -> dict[str, int]:
        """Validate the bounded futures SIM-evidence WAL projection."""
        result = {
            "pending_state_count": 0,
            "pending_state_grace_count": 0,
            "pending_state_overdue_count": 0,
            "invalid_pending_state_count": 0,
        }
        if not isinstance(state_rows, dict) or len(state_rows) > 4_096:
            result["invalid_pending_state_count"] = 1
            return result
        wall = time.time() if now_wall is None else now_wall
        if (
            isinstance(wall, bool)
            or not isinstance(wall, (int, float))
            or not math.isfinite(float(wall))
        ):
            result["invalid_pending_state_count"] = 1
            return result
        normalized_bot = str(bot_name).strip().upper()
        for state_symbol, row in state_rows.items():
            if not isinstance(row, dict):
                continue
            pending = row.get("sim_tca_pending_v1")
            if pending is None:
                continue
            result["pending_state_count"] += 1
            valid = isinstance(pending, dict)
            if valid:
                entry_id = pending.get("entry_id")
                symbol = pending.get("symbol")
                filled_at = pending.get("filled_at")
                valid = bool(
                    type(pending.get("version")) is int
                    and pending["version"] == 1
                    and pending.get("bot_name") == normalized_bot
                    and pending.get("side") in {"buy", "sell"}
                    and isinstance(entry_id, str)
                    and 0 < len(entry_id) <= 64
                    and entry_id == entry_id.strip()
                    and row.get("entry_id") == entry_id
                    and isinstance(symbol, str)
                    and symbol
                    == f"{str(state_symbol).strip().upper()}/USDT:USDT"
                    and isinstance(filled_at, str)
                )
            if valid:
                for field, allow_zero in (
                    ("amount", False),
                    ("fill_price", False),
                    ("fee_rate", True),
                    ("notional_usdt", False),
                ):
                    value = pending.get(field)
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, (int, float))
                        or not math.isfinite(float(value))
                        or (float(value) < 0 if allow_zero else float(value) <= 0)
                    ):
                        valid = False
                        break
                if valid and float(pending["fee_rate"]) >= 1.0:
                    valid = False
                unavailable = pending.get("arrival_unavailable_reason")
                if unavailable is not None and unavailable not in {
                    "capture_contract_size_unavailable",
                    "entry_orderbook_unavailable",
                }:
                    valid = False
            filled_epoch = 0.0
            if valid:
                try:
                    parsed = datetime.strptime(
                        pending["filled_at"], "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    if parsed.strftime("%Y-%m-%d %H:%M:%S") != pending[
                        "filled_at"
                    ]:
                        raise ValueError("non-canonical timestamp")
                    filled_epoch = parsed.timestamp()
                    if filled_epoch > float(wall) + 5.0:
                        raise ValueError("future timestamp")
                except (TypeError, ValueError, OverflowError, OSError):
                    valid = False
            if not valid:
                result["invalid_pending_state_count"] += 1
            elif float(wall) - filled_epoch > float(grace_seconds):
                result["pending_state_overdue_count"] += 1
            else:
                result["pending_state_grace_count"] += 1
        return result

    def _sim_evidence_runtime_health(
        self, state_rows: Any | None = None
    ) -> dict[str, Any]:
        """Expose origin-bot SIM evidence independently from global workers."""
        if not bool(getattr(self, "simulation", False)):
            return {}
        bot_name = str(getattr(self, "BOT_NAME", "")).strip().upper()
        if bot_name not in {"CROSS", "FUTREND"}:
            return {}
        now = time.monotonic()
        cached = getattr(self, "_sim_evidence_health_cache", None)
        last = getattr(self, "_sim_evidence_health_last_monotonic", None)
        if (
            isinstance(cached, dict)
            and not isinstance(last, bool)
            and isinstance(last, (int, float))
            and math.isfinite(float(last))
        ):
            age = now - float(last)
            if 0.0 <= age < 30.0:
                health = dict(cached)
            else:
                cached = None
        else:
            cached = None
        if not isinstance(cached, dict):
            try:
                from core.database import simulated_execution_evidence_health

                health = simulated_execution_evidence_health(bot_name)
                if not isinstance(health, dict):
                    raise TypeError("invalid simulated evidence health payload")
            except Exception as exc:
                health = {
                    "ok": False,
                    "component": "sim_execution_evidence",
                    "state": "degraded",
                    "reason": "health_query_failed",
                    "bot_name": bot_name,
                    "error_type": type(exc).__name__,
                }
            self._sim_evidence_health_cache = dict(health)
            self._sim_evidence_health_last_monotonic = now
        try:
            rows = self.state.get_all() if state_rows is None else state_rows
            pending_health = self._sim_tca_pending_state_health(rows, bot_name)
        except Exception as exc:
            pending_health = {
                "pending_state_count": 0,
                "pending_state_grace_count": 0,
                "pending_state_overdue_count": 0,
                "invalid_pending_state_count": 1,
            }
            health = dict(health)
            health["state_health_error_type"] = type(exc).__name__
            health.update({
                "runtime_ok": False,
                "runtime_state": "degraded",
                "runtime_reason": "state_health_query_failed",
                "data_quality_ok": False,
            })
        else:
            health = dict(health)
            if pending_health["invalid_pending_state_count"]:
                health.update({
                    "runtime_ok": False,
                    "runtime_state": "degraded",
                    "runtime_reason": "state_pending_invalid",
                    "data_quality_ok": False,
                })
            elif pending_health["pending_state_overdue_count"]:
                health.update({
                    "runtime_ok": False,
                    "runtime_state": "degraded",
                    "runtime_reason": "state_capture_pending_overdue",
                    "data_quality_ok": False,
                })
        health.update(pending_health)
        return health

    def _record_venue_recorder_health(self, report: dict) -> None:
        """Receive one bounded health report from the venue recorder."""
        if not isinstance(report, dict):
            return

        payload_contract_valid = True

        def invalidate_payload_contract() -> None:
            nonlocal payload_contract_valid
            payload_contract_valid = False

        def bounded_text(value, *, max_chars: int) -> str:
            if value is None:
                return ""
            try:
                return str(value)[:max_chars]
            except BaseException:
                invalidate_payload_contract()
                return "[UNRENDERABLE]"[:max_chars]

        def nonnegative_int(key: str) -> int:
            try:
                return max(0, int(report.get(key) or 0))
            except BaseException:
                invalidate_payload_contract()
                return 0

        def bounded_strings(value, *, limit: int = 32) -> list[str]:
            if not isinstance(value, (list, tuple)):
                return []
            try:
                items = value[:limit]
            except BaseException:
                invalidate_payload_contract()
                return []
            return [bounded_text(item, max_chars=100) for item in items]

        def bounded_invalid_day_issues(value) -> list[dict[str, object]]:
            if not isinstance(value, (list, tuple)):
                return []
            bounded = []
            for row in value[-8:]:
                if not isinstance(row, dict):
                    continue
                raw_issues = row.get("issues")
                issues = (
                    [
                        bounded_text(issue, max_chars=240)
                        for issue in raw_issues[:4]
                    ]
                    if isinstance(raw_issues, (list, tuple))
                    else []
                )
                bounded.append({
                    "day": bounded_text(row.get("day"), max_chars=16),
                    "issues": issues,
                })
            return bounded

        raw_rest = report.get("rest_data_health")
        raw_rest = raw_rest if isinstance(raw_rest, dict) else {}
        raw_stream = report.get("stream_health")
        raw_stream = raw_stream if isinstance(raw_stream, dict) else {}
        raw_integrity = report.get("integrity_health")
        raw_integrity = raw_integrity if isinstance(raw_integrity, dict) else {}
        raw_continuity = raw_integrity.get("continuity")
        raw_continuity = (
            raw_continuity if isinstance(raw_continuity, dict) else {}
        )
        raw_storage = report.get("storage_health")
        raw_storage = raw_storage if isinstance(raw_storage, dict) else {}

        def strict_connection_number(key: str) -> int | None:
            if key not in raw_stream:
                return None
            value = raw_stream.get(key)
            if type(value) is not int or value < 0:
                invalidate_payload_contract()
                return None
            return value

        connection_epoch = strict_connection_number("connection_epoch")
        reconnect_attempts = strict_connection_number("reconnect_attempts")
        connection_error_type = ""
        if "connection_error_type" in raw_stream:
            raw_connection_error = raw_stream.get("connection_error_type")
            if raw_connection_error is None:
                pass
            elif isinstance(raw_connection_error, str):
                connection_error_type = raw_connection_error[:100]
            else:
                invalidate_payload_contract()

        def bounded_nonnegative(value) -> int | None:
            if value is None or isinstance(value, bool):
                return None
            try:
                return max(0, int(value))
            except BaseException:
                invalidate_payload_contract()
                return None

        def bounded_float(value) -> float | None:
            if value is None or isinstance(value, bool):
                return None
            try:
                parsed = float(value)
            except BaseException:
                invalidate_payload_contract()
                return None
            if not math.isfinite(parsed):
                invalidate_payload_contract()
                return None
            return parsed

        def bounded_monotonic(value) -> float | None:
            parsed = bounded_float(value)
            return parsed if parsed is not None and parsed >= 0.0 else None

        last_poll_monotonic = bounded_monotonic(
            report.get("last_poll_monotonic")
        )
        last_poll_wall_ts = bounded_float(report.get("last_poll_wall_ts"))
        rest_ok = report.get("rest_ok") is True
        l2_enabled = report.get("l2_enabled") is True
        l2_ok = report.get("l2_ok") is True
        l2_data_healthy = report.get("l2_data_healthy") is True
        trade_stream_healthy = report.get("trade_stream_healthy") is True
        retention_ok = report.get("retention_ok") is True
        integrity_ok = raw_integrity.get("ok") is True
        for missing_key, healthy_marker in (
            ("l2_missing_or_stale", l2_data_healthy),
            ("trade_missing_or_stale", trade_stream_healthy),
        ):
            if missing_key not in raw_stream:
                continue
            missing_value = raw_stream.get(missing_key)
            if (
                not isinstance(missing_value, (list, tuple))
                or (healthy_marker and bool(missing_value))
            ):
                invalidate_payload_contract()
        capacity_value = raw_storage.get("capacity_ok")
        capacity_ok = (
            capacity_value if isinstance(capacity_value, bool) else None
        )
        reported_ok = report.get("ok") is True
        boolean_contract_complete = all(
            type(report.get(key)) is bool
            for key in (
                "ok",
                "rest_ok",
                "l2_enabled",
                "l2_ok",
                "l2_data_healthy",
                "trade_stream_healthy",
                "retention_ok",
            )
        )
        capacity_contract_complete = (
            "capacity_ok" in raw_storage
            and (
                capacity_value is None
                or type(capacity_value) is bool
            )
        )
        healthy_contract_complete = (
            boolean_contract_complete
            and capacity_contract_complete
            and last_poll_monotonic is not None
            and last_poll_wall_ts is not None
            and rest_ok
            and l2_ok
            and l2_data_healthy
            and trade_stream_healthy
            and retention_ok
            and integrity_ok
            and capacity_ok is not False
        )
        health_ok = reported_ok and healthy_contract_complete
        reason = bounded_text(report.get("reason"), max_chars=64)
        if reported_ok and not healthy_contract_complete:
            reason = "invalid_health_payload"

        with self._venue_health_lock:
            self._venue_health = {
                "ok": health_ok,
                "reason": reason,
                "last_poll_monotonic": last_poll_monotonic,
                "last_poll_wall_ts": last_poll_wall_ts,
                "rest_ok": rest_ok,
                "l2_enabled": l2_enabled,
                "l2_ok": l2_ok,
                "l2_data_healthy": l2_data_healthy,
                "trade_stream_healthy": trade_stream_healthy,
                "rest_data_health": {
                    "ok": raw_rest.get("ok") is True,
                    "stale_after_seconds": bounded_float(
                        raw_rest.get("stale_after_seconds")
                    ),
                    "missing_or_invalid": bounded_strings(
                        raw_rest.get("missing_or_invalid")
                    ),
                    "trade_audit_warnings": bounded_strings(
                        raw_rest.get("trade_audit_warnings")
                    ),
                },
                "stream_health": {
                    "connection_epoch": connection_epoch,
                    **(
                        {
                            "reconnect_attempts": reconnect_attempts or 0,
                        }
                        if "reconnect_attempts" in raw_stream else {}
                    ),
                    **(
                        {
                            "connection_error_type": connection_error_type,
                        }
                        if "connection_error_type" in raw_stream else {}
                    ),
                    "l2_missing_or_stale": bounded_strings(
                        raw_stream.get("l2_missing_or_stale")
                    ),
                    "trade_missing_or_stale": bounded_strings(
                        raw_stream.get("trade_missing_or_stale")
                    ),
                    "trade_duplicates_suppressed": bounded_nonnegative(
                        raw_stream.get("trade_duplicates_suppressed")
                    ) or 0,
                    "transport_errors_total": bounded_nonnegative(
                        raw_stream.get("transport_errors_total")
                    ) or 0,
                    "transport_errors_consecutive": bounded_nonnegative(
                        raw_stream.get("transport_errors_consecutive")
                    ) or 0,
                    "last_transport_error": bounded_text(
                        raw_stream.get("last_transport_error"),
                        max_chars=200,
                    ),
                    "last_interruption_wall_ts": bounded_float(
                        raw_stream.get("last_interruption_wall_ts")
                    ),
                },
                "integrity_health": {
                    "ok": raw_integrity.get("ok") is True,
                    "sealed_days": bounded_nonnegative(
                        raw_integrity.get("sealed_days")
                    ) or 0,
                    "valid_days": bounded_nonnegative(
                        raw_integrity.get("valid_days")
                    ) or 0,
                    "usable_days": bounded_nonnegative(
                        raw_integrity.get("usable_days")
                    ) or 0,
                    "degraded_days": bounded_strings(
                        raw_integrity.get("degraded_days")
                    ),
                    "invalid_days": bounded_strings(
                        raw_integrity.get("invalid_days")
                    ),
                    "invalid_day_issues": bounded_invalid_day_issues(
                        raw_integrity.get("invalid_day_issues")
                    ),
                    "latest_day": bounded_text(
                        raw_integrity.get("latest_day"),
                        max_chars=16,
                    ),
                    "continuity": {
                        "window_days": bounded_nonnegative(
                            raw_continuity.get("window_days")
                        ),
                        "observed_days": bounded_nonnegative(
                            raw_continuity.get("observed_days")
                        ),
                        "maximum_degraded_days": bounded_nonnegative(
                            raw_continuity.get("maximum_degraded_days")
                        ),
                        "degraded_days": bounded_nonnegative(
                            raw_continuity.get("degraded_days")
                        ),
                        "invalid_days": bounded_nonnegative(
                            raw_continuity.get("invalid_days")
                        ),
                        "ready": raw_continuity.get("ready") is True,
                        "ok": (
                            raw_continuity.get("ok")
                            if isinstance(raw_continuity.get("ok"), bool)
                            else None
                        ),
                        "reason": bounded_text(
                            raw_continuity.get("reason"),
                            max_chars=64,
                        ),
                        "start_day": bounded_text(
                            raw_continuity.get("start_day"),
                            max_chars=16,
                        ),
                        "end_day": bounded_text(
                            raw_continuity.get("end_day"),
                            max_chars=16,
                        ),
                    },
                },
                "integrity_errors_total": nonnegative_int(
                    "integrity_errors_total"
                ),
                "last_integrity_error": bounded_text(
                    report.get("last_integrity_error"),
                    max_chars=200,
                ),
                "storage_health": {
                    "capacity_ok": capacity_ok,
                    "measurement_complete": (
                        raw_storage.get("measurement_complete")
                        if type(raw_storage.get("measurement_complete")) is bool
                        else None
                    ),
                    "measurement_errors": bounded_nonnegative(
                        raw_storage.get("measurement_errors")
                    ),
                    "total_bytes": bounded_nonnegative(
                        raw_storage.get("total_bytes")
                    ),
                    "max_storage_bytes": bounded_nonnegative(
                        raw_storage.get("max_storage_bytes")
                    ),
                    "closed_days_observed": bounded_nonnegative(
                        raw_storage.get("closed_days_observed")
                    ),
                    "peak_closed_day_bytes": bounded_nonnegative(
                        raw_storage.get("peak_closed_day_bytes")
                    ),
                    "projected_required_bytes": bounded_nonnegative(
                        raw_storage.get("projected_required_bytes")
                    ),
                    "required_capacity_bytes": bounded_nonnegative(
                        raw_storage.get("required_capacity_bytes")
                    ),
                    "filesystem_free_bytes": bounded_nonnegative(
                        raw_storage.get("filesystem_free_bytes")
                    ),
                    "filesystem_capacity_bytes": bounded_nonnegative(
                        raw_storage.get("filesystem_capacity_bytes")
                    ),
                    "filesystem_probe_error": bounded_text(
                        raw_storage.get("filesystem_probe_error"),
                        max_chars=164,
                    ),
                    "headroom_ratio": bounded_float(
                        raw_storage.get("headroom_ratio")
                    ),
                    "operational_reserve_ratio": bounded_float(
                        raw_storage.get("operational_reserve_ratio")
                    ),
                    "capacity_state": bounded_text(
                        raw_storage.get("capacity_state"),
                        max_chars=32,
                    ),
                },
                "consecutive_capture_errors": nonnegative_int(
                    "consecutive_capture_errors"
                ),
                "capture_errors_total": nonnegative_int(
                    "capture_errors_total"
                ),
                "captures_total": nonnegative_int("captures_total"),
                "overview_captures_total": nonnegative_int(
                    "overview_captures_total"
                ),
                "overview_errors_total": nonnegative_int(
                    "overview_errors_total"
                ),
                "microstructure_captures_total": nonnegative_int(
                    "microstructure_captures_total"
                ),
                "microstructure_errors_total": nonnegative_int(
                    "microstructure_errors_total"
                ),
                "last_capture_success_wall_ts": bounded_float(
                    report.get("last_capture_success_wall_ts")
                ),
                "last_capture_error": bounded_text(
                    report.get("last_capture_error"),
                    max_chars=200,
                ),
                "last_overview_error": bounded_text(
                    report.get("last_overview_error"),
                    max_chars=200,
                ),
                "last_microstructure_error": bounded_text(
                    report.get("last_microstructure_error"),
                    max_chars=200,
                ),
                "retention_ok": retention_ok,
                "retention_errors_total": nonnegative_int(
                    "retention_errors_total"
                ),
                "last_retention_error": bounded_text(
                    report.get("last_retention_error"),
                    max_chars=200,
                ),
                "l2_consecutive_errors": nonnegative_int(
                    "l2_consecutive_errors"
                ),
                "l2_errors_total": nonnegative_int("l2_errors_total"),
                "last_l2_error": bounded_text(
                    report.get("last_l2_error"),
                    max_chars=200,
                ),
            }
            if reported_ok and not payload_contract_valid:
                self._venue_health["ok"] = False
                self._venue_health["reason"] = "invalid_health_payload"

    def _venue_runtime_health(self) -> dict[str, Any]:
        recorder_thread = getattr(self, "_venue_recorder_thread", None)
        if recorder_thread is None or not hasattr(
            self, "_venue_health_lock"
        ):
            return {}
        with self._venue_health_lock:
            snapshot = copy.deepcopy(self._venue_health)
        stale_sec = max(
            self.VENUE_HEALTH_MIN_STALE_SEC,
            float(getattr(self, "_venue_health_stale_sec", 0.0) or 0.0),
        )
        last_poll = snapshot.get("last_poll_monotonic")
        if last_poll is None:
            if snapshot:
                snapshot.update({
                    "ok": False,
                    "component": "venue_recorder",
                    "state": "invalid",
                    "reason": "poll_timestamp_invalid",
                    "poll_age_seconds": None,
                })
                return snapshot
            started = getattr(self, "_venue_started_monotonic", None)
            try:
                startup_age = (
                    0.0
                    if started is None
                    else time.monotonic() - float(started)
                )
                if not math.isfinite(startup_age) or startup_age < 0.0:
                    raise ValueError("venue startup timestamp is invalid")
            except BaseException:
                snapshot.update({
                    "ok": False,
                    "component": "venue_recorder",
                    "state": "invalid",
                    "reason": "startup_timestamp_invalid",
                    "startup_age_seconds": None,
                })
                return snapshot
            startup_stale = startup_age > stale_sec
            snapshot.update({
                "ok": not startup_stale,
                "component": "venue_recorder",
                "state": "stalled" if startup_stale else "starting",
                "startup_age_seconds": startup_age,
            })
            if startup_stale:
                snapshot["reason"] = "startup_poll_stale"
            return snapshot
        try:
            poll_age = time.monotonic() - float(last_poll)
            if not math.isfinite(poll_age) or poll_age < 0.0:
                raise ValueError("venue poll timestamp is invalid")
        except (TypeError, ValueError, OverflowError):
            snapshot.update({
                "ok": False,
                "component": "venue_recorder",
                "state": "invalid",
                "reason": "poll_timestamp_invalid",
                "poll_age_seconds": None,
            })
            return snapshot
        if poll_age > stale_sec:
            snapshot["ok"] = False
            snapshot["reason"] = "poll_stale"
        snapshot.update({
            "component": "venue_recorder",
            "poll_age_seconds": poll_age,
        })
        return snapshot

    def _runtime_threads(self) -> dict[str, bool]:
        """Report every production-critical futures lifecycle worker."""
        threads = {
            "monitor": bool(
                self._monitor_thread and self._monitor_thread.is_alive()
            ),
            "scan": bool(self._scan_thread and self._scan_thread.is_alive()),
            "reconcile": bool(
                self._reconcile_thread and self._reconcile_thread.is_alive()
            ),
        }
        if self._owns_markout_worker():
            threads["markout"] = bool(
                self._markout_thread and self._markout_thread.is_alive()
            )
        venue_recorder_thread = getattr(self, "_venue_recorder_thread", None)
        if venue_recorder_thread is not None:
            threads["venue_recorder"] = bool(
                venue_recorder_thread.is_alive()
            )
        return threads

    def _publish_periodic_runtime_status(
        self,
        status_writer,
        *,
        log_snapshot: bool,
    ) -> None:
        """Supervise exits before optional observability/status publication."""
        threads = self._runtime_threads()
        if (
            not threads["monitor"]
            and self.safe_mode is not None
            and not self.safe_mode.is_active()
        ):
            self.safe_mode.trigger(
                "monitor thread stopped - exits not supervised"
            )
        try:
            state_error_type = ""
            try:
                state_rows = self.state.get_all()
                if not isinstance(state_rows, dict):
                    state_rows = None
                    state_error_type = "invalid_state_payload"
            except Exception as exc:
                state_rows = None
                state_error_type = type(exc).__name__
            runtime_health_reader = self._runtime_status_health
            if (
                getattr(runtime_health_reader, "__func__", None)
                is FuturesBot._runtime_status_health
            ):
                status, strategy_health = runtime_health_reader(
                    threads,
                    state_rows=state_rows,
                    state_error_type=state_error_type,
                )
            else:
                # Preserve the long-standing subclass/test-double contract.
                status, strategy_health = runtime_health_reader(threads)
            from trading.runtime_observability import (
                guarded_runtime_observability,
            )

            observability, observability_health, observability_error = (
                guarded_runtime_observability(
                    log_snapshot=log_snapshot,
                    bot_name=self.BOT_NAME,
                    mode="SIM" if self.simulation else "LIVE",
                    state_rows=state_rows,
                    ticker_cache=self.ticker_cache,
                )
            )
            if observability_error is not None:
                phase = "heartbeat" if log_snapshot else "periodic"
                silent_log(
                    f"{self.BOT_NAME} {phase} runtime observability",
                    observability_error,
                )
            if observability_health:
                status = "degraded"
            published = status_writer(
                self.LOG_DIR,
                self.BOT_NAME,
                status,
                self.simulation,
                threads=threads,
                extra={
                    "open_positions": (
                        None
                        if state_rows is None
                        else state_rows_exposure_count(state_rows)
                    ),
                    "open_positions_known": state_rows is not None,
                    "safe_mode": bool(self.safe_mode.is_active()),
                    **self._entry_admission_runtime_fields(),
                    **observability,
                    **strategy_health,
                    **(
                        {"observability_health": observability_health}
                        if observability_health else {}
                    ),
                },
            )
            if published is not True:
                raise RuntimeError("runtime status publication failed")
        except Exception as exc:
            phase = "heartbeat" if log_snapshot else "periodic"
            silent_log(f"{self.BOT_NAME} {phase} runtime status", exc)

    def _publish_runtime_heartbeat(self, status_writer, log_event) -> None:
        """Publish core health even when the human-readable heartbeat fails."""
        def report_error(context: str, exc: Exception) -> None:
            try:
                self._log_error(context, exc)
            except Exception:
                pass

        try:
            self._publish_periodic_runtime_status(
                status_writer, log_snapshot=True
            )
        except Exception as exc:
            report_error("runtime heartbeat status", exc)
        try:
            tc = state_exposure_count(self.state)
            thread_liveness = format_runtime_thread_liveness(
                self._runtime_threads()
            )
            sm_marker = (
                "  SAFE_MODE" if self.safe_mode.is_active() else ""
            )
            log_event(
                f" {self.BOT_NAME} heartbeat  "
                f"Open: {tc}/{self.C('MAX_OPEN_TRADES')}  "
                f"Threads: {thread_liveness}"
                f"{sm_marker}",
                "INFO",
            )
        except Exception as exc:
            report_error("runtime heartbeat display", exc)

    def _cleanup_stale_futures_dashboard_state(
        self,
        get_futures_state,
        remove_futures_state,
        log_event,
    ) -> None:
        """Remove only dashboard rows absent from or older than local state."""

        def _entry_id(value):
            if not isinstance(value, str):
                return None
            entry_id = value.strip()
            if (
                not entry_id
                or len(entry_id) > 64
                or any(ord(char) < 32 or ord(char) == 127 for char in entry_id)
            ):
                return None
            return entry_id

        actual = set(self.state.keys())
        for entry in get_futures_state(
                self.BOT_NAME, mode_is_sim=self.simulation):
            sym = entry.get("symbol")
            if not sym:
                continue
            current = self.state.get(sym) if sym in actual else None
            current_entry_id = _entry_id(
                current.get("entry_id") if isinstance(current, dict) else None
            )
            dashboard_entry_id = _entry_id(entry.get("entry_id"))
            generation_stale = (
                current_entry_id is not None
                and dashboard_entry_id != current_entry_id
            )
            if current is not None and not generation_stale:
                continue
            if dashboard_entry_id is not None:
                removed = remove_futures_state(
                    sym,
                    self.BOT_NAME,
                    mode_is_sim=self.simulation,
                    expected_opened_at=entry.get("opened_at"),
                    expected_entry_id=dashboard_entry_id,
                )
            else:
                removed = remove_futures_state(
                    sym, self.BOT_NAME, mode_is_sim=self.simulation
                )
            if removed is True:
                detail = " generation" if generation_stale else " entry"
                log_event(
                    f"Stale futures_state{detail} cleaned: {sym}", "INFO"
                )
            else:
                log_event(
                    f"Stale futures_state cleanup skipped after concurrent "
                    f"change: {sym}",
                    "WARN",
                )

    #  Run 

    def run(self) -> None:
        from core.logger import (log_event, log_separator, log_struct,
                                  set_structured_log_dir, load_j)
        from core.database import (init_db, get_futures_state,
                                    remove_futures_state, set_metrics_sim_mode)
        from core.runtime_status import get_build_info, write_runtime_status
        from trading.risk_manager import validate_config_or_die
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from core.logger import send_telegram

        init_db()
        set_metrics_sim_mode(self.simulation)   # pin SIM/LIVE metrics namespace
        set_structured_log_dir(self.LOG_DIR)
        build_info = get_build_info()
        write_runtime_status = partial(
            write_runtime_status, build_info=build_info
        )
        write_runtime_status(
            self.LOG_DIR, self.BOT_NAME, "starting", self.simulation,
            extra={"phase": "config_validate"})
        try:
            validate_config_or_die(self.BOT_NAME)
        except BaseException as e:
            write_runtime_status(
                self.LOG_DIR, self.BOT_NAME, "failed", self.simulation,
                extra={"phase": "config_validate", "error": str(e)[:300]})
            raise

        #  Banner 
        log_separator("", color=self.BOT_COLOR)
        log_event(
            f"{self.BOT_NAME}-Bot v2 started (dual-thread architecture)",
            "START"
        )
        log_event(
            f"Build: {build_info.get('build_id', 'unknown')} "
            f"({build_info.get('source', 'fallback')})",
            "START"
        )
        log_event(
            f"Simulation: {self.simulation}  "
            f"Leverage: {self.C('LEVERAGE')}x  "
            f"Max trades: {self.C('MAX_OPEN_TRADES')}",
            "START"
        )
        if str(self.BOT_NAME).upper() == "FUTURES":
            log_event(
                "New entries: "
                f"{'enabled' if self._new_entries_enabled() else 'disabled'} "
                "(monitor/reconcile/exits remain active)",
                "START",
            )
        log_event(
            f"Margin/Trade: {self.C('POSITION_SIZE')} USDT  "
            f"Liq buffer: {self.C('LIQ_SAFETY_PCT')}%",
            "START"
        )
        log_event(
            f"Stop: {self.C('INITIAL_STOP_LOSS')}%  "
            f"Activation TP: +{self.C('ACTIVATION_PROFIT')}%  "
            f"Trail: {self.C('TRAILING_DISTANCE')}%",
            "START"
        )
        log_event(
            f"Breakeven: +{self.C('BREAKEVEN_TRIGGER')}%  "
            f"SL-Cooldown: {self.C('COOLDOWN_AFTER_SL')}min  "
            f"Daily-Loss-Limit: {self.C('MAX_DAILY_LOSS')}",
            "START"
        )
        log_event(
            f"Pre-Activation Peak Trail: "
            f"{self.C('PRE_ACTIVATION_GIVEBACK_STOP_ENABLED', True)}  "
            f"MFE: +{self.C('PRE_ACTIVATION_MIN_MFE_PCT', 1.5)}%  "
            f"Giveback: {self.C('PRE_ACTIVATION_GIVEBACK_PCT', 0.75)}%",
            "START"
        )
        mfe_fallback_fields = self._startup_mfe_fallback_fields()
        if mfe_fallback_fields:
            log_event(
                f"Aged MFE Fallback: "
                f"{mfe_fallback_fields['mfe_fallback_enabled']}  "
                f"Age: "
                f"{mfe_fallback_fields['mfe_fallback_min_age_minutes']}min  "
                f"MFE: +"
                f"{mfe_fallback_fields['mfe_fallback_min_mfe_pct']}%  "
                f"Exit: "
                f"{mfe_fallback_fields['mfe_fallback_exit_move_pct']}%",
                "START"
            )
        log_event(
            f"Scan: {self.C('SCAN_INTERVAL')}s  "
            f"Monitor: {self.C('MONITOR_INTERVAL', self.DEFAULT_MONITOR_INTERVAL)}s",
            "START"
        )
        log_separator("", color=self.BOT_COLOR)

        started_fields = {
            "bot": self.BOT_NAME,
            "simulation": self.simulation,
            "build_id": build_info.get("build_id", "unknown"),
            "build_source": build_info.get("source", "fallback"),
            "leverage": self.C("LEVERAGE"),
            "max_trades": self.C("MAX_OPEN_TRADES"),
            "position_size": self.C("POSITION_SIZE"),
            "liq_safety_pct": self.C("LIQ_SAFETY_PCT"),
            "stop_loss": self.C("INITIAL_STOP_LOSS"),
            "activation_tp": self.C("ACTIVATION_PROFIT"),
            "trail": self.C("TRAILING_DISTANCE"),
            "breakeven_trigger": self.C("BREAKEVEN_TRIGGER"),
            "pre_activation_peak_trail_enabled": self.C(
                "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED", True),
            "pre_activation_min_mfe_pct": self.C(
                "PRE_ACTIVATION_MIN_MFE_PCT", 1.5),
            "pre_activation_giveback_pct": self.C(
                "PRE_ACTIVATION_GIVEBACK_PCT", 0.75),
            "entry_quality_shadow_enabled": self.C(
                "ENTRY_QUALITY_SHADOW_ENABLED", False),
            "entry_quality_shadow_min_score": self.C(
                "ENTRY_QUALITY_SHADOW_MIN_SCORE", 85.0),
            "scan_interval": self.C("SCAN_INTERVAL"),
            "monitor_interval": self.C(
                "MONITOR_INTERVAL", self.DEFAULT_MONITOR_INTERVAL),
        }
        started_fields.update(self._entry_admission_runtime_fields())
        started_fields.update(mfe_fallback_fields)
        log_struct("bot_started", **started_fields)

        # Validate the news module only when the strategy is configured to use
        # it. With USE_LLM=false the bot is a signal engine and must not emit
        # KI/News startup lines.
        if self._bool_cfg_value(self.C("USE_LLM", False), False):
            try:
                _ = self._news
                log_event(f"News module loaded: {self.NEWS_MODULE_PATH}", "INFO")
            except Exception as e:
                write_runtime_status(
                    self.LOG_DIR, self.BOT_NAME, "failed", self.simulation,
                    extra={"phase": "news_import", "error": str(e)[:300]})
                log_event(
                    f"FATAL: news module '{self.NEWS_MODULE_PATH}' failed: {e}",
                    "WARN"
                )
                self._log_error("news module import", e)
                sys.exit(1)
        #  Connect 
        if not self._connect_with_retry():
            write_runtime_status(
                self.LOG_DIR, self.BOT_NAME, "failed", self.simulation,
                extra={"phase": "exchange_connect"})
            sys.exit(1)

        #  Init shared infrastructure 
        self.ticker_cache = TickerCache(
            pool_size=self.TICKER_POOL_SIZE,
            in_flight_max=self.TICKER_INFLIGHT_MAX,
            thread_name_prefix=f"{self.BOT_NAME.lower()}-ticker",
        )
        self.safe_mode = SafeMode(
            bot_name=self.BOT_NAME,
            telegram_send=send_telegram,
            telegram_token=TELEGRAM_TOKEN,
            telegram_chat_id=TELEGRAM_CHAT_ID,
            log_event=log_event,
            log_struct=log_struct,
            state_dir=self.LOG_DIR,   # H-6: persist alert state
        )

        #  LIVE-start alert 
        if not self.simulation:  # real money only  SIM stays silent
            try:
                send_telegram(
                    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f" [{self.BOT_NAME}] LIVE gestartet  echtes Geld aktiv. "
                    f"Leverage {self.C('LEVERAGE')}x, "
                    f"Max {self.C('MAX_OPEN_TRADES')} trades, "
                    f"Daily-Loss {self.C('MAX_DAILY_LOSS')} USDT.",
                )
            except Exception as e:
                self._log_error("live-start telegram alert", e)

        #  Load state 
        trades_raw = load_j(self.DB_FILE, preserve_corrupt=True)
        from trading.cooldown_utils import load_cooldown_state
        self.cool = load_cooldown_state(self.COOLDOWN_FILE)
        if not self.cool.source_valid:
            log_event(
                f"Cooldown reload failed closed; new entries remain blocked: "
                f"{self.cool.source_error}",
                "ERROR",
            )

        # Purge only expired cooldowns. Malformed evidence is retained and
        # keeps entries blocked until an operator repairs it.
        try:
            try:
                from trading.cooldown_utils import purge_expired
            except ImportError:
                from bot_utils.cooldown_utils import purge_expired
            removed = purge_expired(self.cool, self.COOLDOWN_FILE)
            if removed > 0:
                log_event(
                    f"Cooldown reload: purged {removed} expired "
                    f"entry/entries from {self.COOLDOWN_FILE}", "INFO")
        except Exception as cd_err:
            log_event(
                f"Cooldown purge on startup skipped: {cd_err}", "WARN")

        # Claims registry is for REAL-account coexistence only  a SIM bot
        # holds nothing real and must not claim coins away from a LIVE bot.
        # bot_name=None disables the registry writes for SIM (TradeState guards).
        self.state = TradeState(self.DB_FILE, trades_raw, is_futures=True,
                                bot_name=(self.BOT_NAME if not self.simulation else None))
        if self.state.init_rejected:
            r = self.state.init_rejected
            log_event(
                f" Futures state load: dropped {len(r)} invalid trade(s): "
                f"{', '.join(r[:10])}"
                + (f" (+{len(r)-10} more)" if len(r) > 10 else ""),
                "WARN"
            )

        if self.simulation:
            self._recover_simulated_entry_tca_pending()

        #  Stale-state cleanup 
        # SCOPED to THIS bot: futures_state is shared with the CROSS bot, so an
        # unscoped read here made the FUTURES startup delete the CROSS bot's
        # live dashboard rows (and vice versa)  they look "stale" because the
        # other bot's symbols aren't in this bot's state. Only clean our own.
        try:
            self._cleanup_stale_futures_dashboard_state(
                get_futures_state,
                remove_futures_state,
                log_event,
            )
        except Exception as e:
            self._log_error("startup-cleanup", e)

        #  Startup reconciliation (in mixin) 
        if not self.simulation:
            startup_recovery_cutoff = now_utc().strftime("%Y-%m-%d %H:%M:%S")
            recovery_ok, recovery_generation = self._refresh_entry_recovery_barrier(
                log_event,
                context="startup",
            )
            reconciliation_ok = self._startup_reconciliation()
            if reconciliation_ok:
                recovery_ok = self._finalize_qualified_zero_fill_recoveries(
                    log_event,
                    reconciliation_ok=reconciliation_ok,
                )
                self._finalize_interrupted_candidates(
                    log_event,
                    reconciliation_ok=reconciliation_ok,
                    startup_cutoff=startup_recovery_cutoff,
                )
            self._complete_entry_recovery_barrier(
                recovery_generation,
                recovery_ok=recovery_ok,
                reconciliation_ok=reconciliation_ok,
            )

        # Register every shutdown handler before worker threads start.  The
        # Windows launcher uses CTRL_BREAK_EVENT, so silently missing SIGBREAK
        # would turn a requested graceful close into an unhandled termination.
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, self._shutdown_handler)
        atexit.register(self._shutdown_handler)

        #  Start threads 
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True,
            name=f"{self.BOT_NAME}Monitor",
        )
        self._scan_thread = threading.Thread(
            target=self._scan_loop, daemon=True,
            name=f"{self.BOT_NAME}Scan",
        )
        self._reconcile_thread = threading.Thread(
            target=self._reconcile_loop, daemon=True,
            name=f"{self.BOT_NAME}Reconcile",
        )
        if self._owns_markout_worker():
            from trading.execution_quality import run_tca_markout_worker

            self._markout_thread = threading.Thread(
                target=run_tca_markout_worker,
                args=(self.ex, self._shutdown_event),
                kwargs={
                    "poll_interval_seconds": self.MARKOUT_POLL_INTERVAL_SEC,
                    "limit": 25,
                    "max_overdue_seconds": self.MARKOUT_MAX_OVERDUE_SEC,
                    "health_callback": self._record_markout_worker_health,
                    "worker_family": "futures",
                },
                daemon=True,
                name=f"{self.BOT_NAME}Markouts",
            )
        if self._venue_recorder_requested():
            from core.paths import DATA_DIR
            from trading.venue_recorder import VenueRecorder

            recorder = VenueRecorder(
                self.ex,
                DATA_DIR / "venue_native",
                max_symbols=int(self.C("VENUE_RECORDER_MAX_SYMBOLS", 8)),
                depth_levels=int(self.C("VENUE_RECORDER_DEPTH_LEVELS", 20)),
                micro_interval_seconds=float(
                    self.C("VENUE_RECORDER_MICRO_INTERVAL_SECONDS", 6.0)
                ),
                overview_interval_seconds=float(
                    self.C("VENUE_RECORDER_OVERVIEW_INTERVAL_SECONDS", 60.0)
                ),
                retention_days=int(
                    self.C("VENUE_RECORDER_RETENTION_DAYS", 30)
                ),
                max_storage_gib=float(
                    self.C("VENUE_RECORDER_MAX_STORAGE_GIB", 150.0)
                ),
                log_event=log_event,
                l2_mode=str(self.C("VENUE_L2_MODE", "disabled")),
                l2_sample_interval_seconds=float(
                    self.C("VENUE_L2_SAMPLE_INTERVAL_SECONDS", 1.0)
                ),
                l2_stale_after_ms=int(
                    self.C("VENUE_L2_STALE_AFTER_MS", 5_000)
                ),
                health_callback=self._record_venue_recorder_health,
            )
            self._venue_recorder = recorder
            self._venue_health_stale_sec = max(
                self.VENUE_HEALTH_MIN_STALE_SEC,
                recorder.micro_interval * 3.0,
            )
            self._venue_recorder_thread = threading.Thread(
                target=recorder.run,
                args=(self._shutdown_event,),
                daemon=True,
                name="FUTURESVenueRecorder",
            )
        if self._markout_thread is not None:
            self._markout_started_monotonic = time.monotonic()
        if self._venue_recorder_thread is not None:
            self._venue_started_monotonic = time.monotonic()
        try:
            start_threads_or_shutdown(
                (
                    thread
                    for thread in (
                        self._monitor_thread,
                        self._scan_thread,
                        self._reconcile_thread,
                        self._markout_thread,
                        self._venue_recorder_thread,
                    )
                    if thread is not None
                ),
                self._shutdown_event,
                wakeup_events=(self._reconcile_wakeup_event,),
            )

            thread_names = "monitor, scan, reconcile"
            if self._markout_thread is not None:
                thread_names += ", markout"
            if self._venue_recorder_thread is not None:
                thread_names += ", venue recorder"
            log_event(f"Core threads running ({thread_names}).", "START")
            self._publish_periodic_runtime_status(
                write_runtime_status, log_snapshot=False
            )

            #  Main thread: heartbeat + shutdown wait
            try:
                last_heartbeat = 0.0
                last_runtime_status = 0.0
                self._last_hourly_status = 0.0
                while not self._shutdown_event.is_set():
                    try:
                        if self._consume_launcher_shutdown_request(log_event):
                            break
                        # Scheduling must be immune to wall-clock corrections. The
                        # coordinator itself must also survive a transient DB,
                        # status or diagnostic failure while its safety workers are
                        # still alive.
                        now = time.monotonic()
                        if now - last_heartbeat >= self.HEARTBEAT_INTERVAL_SEC:
                            self._publish_runtime_heartbeat(
                                write_runtime_status, log_event
                            )
                            last_heartbeat = now
                        if now - last_runtime_status >= 5.0:
                            self._publish_periodic_runtime_status(
                                write_runtime_status, log_snapshot=False
                            )
                            last_runtime_status = now
                        # Self-throttling hourly status; never lifecycle-critical.
                        try:
                            from bot_utils.status_report import maybe_send_hourly_status
                            self._last_hourly_status = maybe_send_hourly_status(
                                bot_name=self.BOT_NAME, is_futures=True,
                                simulation=self.simulation, state=self.state,
                                last_sent=self._last_hourly_status,
                                safe_mode_active=self.safe_mode.is_active(),
                            )
                        except Exception:
                            pass
                    except Exception as exc:
                        try:
                            self._log_error("runtime coordinator", exc)
                        except Exception:
                            pass
                    self._shutdown_event.wait(timeout=2)
            except KeyboardInterrupt:
                self._shutdown_handler(signum="KeyboardInterrupt")
        finally:
            self._shutdown_event.set()
            self._reconcile_wakeup_event.set()
            try:
                try:
                    log_event("Waiting for threads to finish...", "INFO")
                except Exception:
                    pass
                # Short joins are followed by the synchronous shutdown barrier.
                for t in (
                    self._monitor_thread,
                    self._scan_thread,
                    self._reconcile_thread,
                    self._markout_thread,
                    self._venue_recorder_thread,
                ):
                    try:
                        if t and t.is_alive():
                            t.join(timeout=2)
                    except Exception:
                        pass
                # A fetch that was already running during the signal handler may have
                # completed while core threads were joining. Confirm pool teardown now
                # so a non-daemon executor worker cannot be reported as cleanly closed.
            finally:
                resource_closers = shared_runtime_resource_closers()
                resource_closers["ticker_cache"] = self._shutdown_ticker_cache_if_flat
                ws_feed = getattr(self, "_ws_feed", None)
                if ws_feed is not None:
                    resource_closers["price_feed_resources"] = (
                        lambda feed=ws_feed: feed.stop(timeout=0.0) is True
                    )
                state_flush = getattr(self.state, "finalize_pending", None)
                if callable(state_flush):
                    resource_closers["trade_state_persistence"] = state_flush
                venue_recorder = getattr(self, "_venue_recorder", None)
                if venue_recorder is not None:
                    resource_closers["venue_recorder_resources"] = (
                        lambda recorder=venue_recorder: recorder.shutdown_resources(
                            timeout=0.0
                        )
                    )
                if self.safe_mode is not None:
                    resource_closers["safe_mode_persistence"] = (
                        lambda safe_mode=self.safe_mode: safe_mode.shutdown_alert_state_persistence(
                            timeout=0.0
                        )
                    )
                wait_for_runtime_shutdown(
                    self,
                    write_runtime_status,
                    log_event,
                    resource_closers=resource_closers,
                )

    #  Exchange connect 

    def _connect_with_retry(self) -> bool:
        """Connect + smoke-test the exchange.

        Wraps raw CCXT in ``ThreadLocalExchange`` so the scan/monitor/reconcile
        threads get isolated clones  CCXT is not thread-safe.

        After load_markets succeeds, performs an auth smoke-test
        (``fetch_balance``) so an expired API key or wrong passphrase fails
        fast at startup instead of on the first trade attempt.
        """
        from config.exchange_config import is_authentication_error
        from core.logger import log_event

        def _startup_probe_allowed(endpoint: str):
            try:
                return try_consume_api_call(
                    endpoint,
                    return_reservation=True,
                )
            except Exception as budget_exc:
                log_event(
                    f"Futures startup probe skipped - API budget gate "
                    f"unavailable ({type(budget_exc).__name__})",
                    "WARN",
                )
                return False

        raw_ex = None
        wrapped_ex = None
        connected = False

        def _cleanup_failed_connection() -> None:
            target = wrapped_ex if wrapped_ex is not None else raw_ex
            if target is None:
                return
            method_name = "shutdown" if wrapped_ex is not None else "close"
            closer = getattr(target, method_name, None)
            if callable(closer):
                try:
                    result = closer()
                    if result is not None and result is not True:
                        raise RuntimeError(
                            "failed startup exchange cleanup remained incomplete"
                        )
                except Exception as close_err:
                    self._log_error(
                        "exchange cleanup after failed startup", close_err
                    )
            if getattr(self, "ex", None) is target:
                self.ex = None

        try:
            raw_ex = self.EXCHANGE_FACTORY()
            # HTTP timeout  higher at startup for slow load_markets
            raw_ex.timeout = 30_000
            for attempt in range(1, 4):
                try:
                    try:
                        markets_reservation = try_consume_api_call(
                            "futures_startup_load_markets",
                            critical=True,
                            return_reservation=True,
                        )
                    except Exception as budget_exc:
                        raise RuntimeError(
                            "futures load_markets API budget gate unavailable"
                        ) from budget_exc
                    if not markets_reservation:
                        raise RuntimeError(
                            "futures load_markets API budget exhausted"
                        )
                    try:
                        raw_ex.load_markets()
                        if hasattr(raw_ex, "markets"):
                            market_snapshot = raw_ex.markets
                            if (
                                not isinstance(market_snapshot, dict)
                                or not market_snapshot
                                or any(
                                    not isinstance(symbol, str)
                                    or not symbol.strip()
                                    or not isinstance(market, dict)
                                    for symbol, market in market_snapshot.items()
                                )
                            ):
                                raise ValueError(
                                    "futures load_markets returned no market snapshot"
                                )
                    except Exception:
                        _record_reserved_api_error(
                            "futures_startup_load_markets",
                            markets_reservation,
                        )
                        raise
                    break
                except Exception as le:
                    if is_authentication_error(le):
                        log_event(
                            "load_markets authentication failed; "
                            "startup retry skipped until credentials are fixed",
                            "WARN",
                        )
                        raise
                    if attempt == 3:
                        raise
                    wait = 5 * (2 ** (attempt - 1))
                    log_event(
                        f"load_markets attempt {attempt}/3 failed "
                        f"({type(le).__name__}: {str(le)[:80]})  "
                        f"retrying in {wait}s...",
                        "WARN"
                    )
                    time.sleep(wait)
            raw_ex.timeout = 10_000  # tight trading timeout after init

            # Auth smoke-test BEFORE going threaded, so a dead key surfaces
            # here instead of on the first close-order.
            auth_reservation = _startup_probe_allowed(
                "futures_startup_auth_fetch_balance"
            )
            auth_probe_allowed = bool(auth_reservation)
            if not auth_probe_allowed and not self.C("SIMULATION", True):
                log_event(
                    "Futures LIVE startup blocked - authentication could not "
                    "be verified within the API budget.",
                    "WARN",
                )
                return False
            if auth_probe_allowed:
                try:
                    _bal = raw_ex.fetch_balance()
                    # We only care that the call succeeded; ignore content.
                    if not isinstance(_bal, dict):
                        raise TypeError(
                            "futures auth probe returned no balance object"
                        )
                    _ = _bal.get("USDT", {})
                except Exception as se:
                    _record_reserved_api_error(
                        "futures_startup_auth_fetch_balance",
                        auth_reservation,
                    )
                    # Don't fail the connect on a transient network blip:
                    # log and continue. Credential failures fail closed before
                    # worker threads start or a connected status is emitted.
                    if is_authentication_error(se):
                        log_event(
                            f"Futures auth smoke-test FAILED ({type(se).__name__}: "
                            f"{str(se)[:120]})  bot will not be able to "
                            f"place orders. Check API key/secret/passphrase.",
                            "WARN")
                        # Treat auth errors as fatal  the bot should not
                        # silently run with a dead key.
                        return False
                    log_event(
                        f"Futures auth smoke-test transient error "
                        f"(non-auth: {type(se).__name__})  continuing",
                        "INFO")

            # Per-thread clones from here on
            try:
                from bot_utils.thread_exchange import ThreadLocalExchange
                self.ex = ThreadLocalExchange(raw_ex)
                wrapped_ex = self.ex
            except Exception as wrap_err:
                # A shared CCXT instance is not safe across the scan, monitor
                # and reconcile threads.  Refuse startup instead of reviving
                # the invalid-signature race that this wrapper prevents.
                self.ex = None
                log_event(
                    f"ThreadLocalExchange initialization failed "
                    f"({type(wrap_err).__name__}: {str(wrap_err)[:120]})  "
                    f"refusing unsafe shared exchange startup",
                    "WARN",
                )
                return False

            # Smoke test of the futures API surface.
            # In SIMULATION mode fetch_balance is not critical  skip to
            # avoid false-alarm ERROR badges when Bitget is temporarily
            # unreachable. In LIVE mode we distinguish transient network
            # errors (INFO) from auth failures (WARN) which are actionable.
            if not self.C("SIMULATION", True):
                live_balance_reservation = _startup_probe_allowed(
                    "futures_startup_live_fetch_balance"
                )
                if live_balance_reservation:
                    try:
                        live_balance = self.ex.fetch_balance()
                        if not isinstance(live_balance, dict):
                            raise TypeError(
                                "futures live probe returned no balance object"
                            )
                    except Exception as bal_err:
                        _record_reserved_api_error(
                            "futures_startup_live_fetch_balance",
                            live_balance_reservation,
                        )
                        if is_authentication_error(bal_err):
                            log_event(
                                f"H-6 smoke test: fetch_balance  AUTH FAILURE "
                                f"({type(bal_err).__name__}: {str(bal_err)[:80]}) "
                                f" check API key permissions", "WARN")
                            return False
                        else:
                            # NetworkError / timeout  transient, not actionable
                            log_event(
                                f"H-6 smoke test: fetch_balance transient error "
                                f"({type(bal_err).__name__})  bot will retry on "
                                f"first reconcile cycle", "INFO")
            positions_reservation = _startup_probe_allowed(
                "futures_startup_fetch_positions"
            )
            if positions_reservation:
                try:
                    # fetch_positions sometimes needs a symbol on Bitget; we
                    # only need to know the call path WORKS, not the data.
                    probe_positions = self.ex.fetch_positions(
                        ["BTC/USDT:USDT"]
                    )
                    if not isinstance(probe_positions, list):
                        raise TypeError(
                            "futures position probe returned no position list"
                        )
                except Exception as pos_err:
                    _record_reserved_api_error(
                        "futures_startup_fetch_positions",
                        positions_reservation,
                    )
                    if is_authentication_error(pos_err):
                        log_event(
                            f"H-6 smoke test: fetch_positions AUTH FAILURE "
                            f"({type(pos_err).__name__}: {str(pos_err)[:80]}) "
                            f" check API key permissions", "WARN")
                        return False
                    log_event(
                        f"H-6 smoke test note: fetch_positions raised "
                        f"{type(pos_err).__name__} (non-fatal; reconcile "
                        f"will retry)", "INFO")

            log_event("Futures API connection established", "INFO")
            connected = True
            return True
        except Exception as e:
            log_event(f"Futures connection failed: {e}", "WARN")
            self._log_error("Connection", e)
            return False
        finally:
            if not connected:
                _cleanup_failed_connection()

    #  Shutdown 

    def _consume_launcher_shutdown_request(self, log_event) -> bool:
        """Consume one launcher request on the bot's main thread."""
        from bot_utils.shutdown_control import (
            CLOSE_POSITIONS,
            PRESERVE_POSITIONS,
            consume_shutdown_request,
        )

        try:
            mode = consume_shutdown_request()
        except Exception as exc:
            self._log_error("Launcher shutdown control", exc)
            return False
        if mode is None:
            return False
        if mode == CLOSE_POSITIONS:
            self._shutdown_handler(signum="Launcher close request")
            return True
        if mode != PRESERVE_POSITIONS:
            return False
        handler_lock = getattr(self, "_shutdown_handler_lock", None)
        if handler_lock is None:  # compatibility for lightweight test hosts
            handler_lock = threading.Lock()
            self._shutdown_handler_lock = handler_lock
        close_requested = getattr(self, "_shutdown_close_requested", None)
        if close_requested is None:
            close_requested = threading.Event()
            self._shutdown_close_requested = close_requested
        preserve_refused = False
        with handler_lock:
            with self._shutdown_lock:
                if getattr(self, "_emergency_in_progress", False):
                    log_event(
                        "Preserve-position shutdown refused while emergency "
                        "close is already in progress",
                        "WARN",
                    )
                    preserve_refused = True
                else:
                    self._shutdown_positions_preserved = True
                    self._shutdown_event.set()
                    self._reconcile_wakeup_event.set()
        if close_requested.is_set():
            self._drain_shutdown_close_requests(
                signum="Deferred shutdown signal"
            )
        if preserve_refused:
            return False
        log_event(
            "Launcher preserve-position shutdown received; positions remain "
            "open while runtime resources close cleanly",
            "INFO",
        )
        return True

    def _shutdown_ticker_cache_if_flat(self) -> bool:
        """Keep price fetching alive while an emergency retry is still needed."""
        if not (
            getattr(self, "_emergency_closed", False)
            or getattr(self, "_shutdown_positions_preserved", False)
        ):
            return False
        cache = getattr(self, "ticker_cache", None)
        if cache is None:
            return True
        try:
            shutdown_result = cache.shutdown()
            if shutdown_result is not None and shutdown_result is not True:
                raise RuntimeError("ticker pool still has running work")
            return True
        except Exception as exc:
            self._log_error("Ticker pool shutdown", exc)
            return False

    def _shutdown_handler(self, signum=None, frame=None):
        """Publish one close request and drain it when this caller owns it."""
        requested = getattr(self, "_shutdown_close_requested", None)
        if requested is None:  # compatibility for lightweight test hosts
            requested = threading.Event()
            self._shutdown_close_requested = requested
        publish_lock = getattr(self, "_shutdown_request_publish_lock", None)
        if publish_lock is None:
            publish_lock = threading.RLock()
            self._shutdown_request_publish_lock = publish_lock
        with publish_lock:
            self._shutdown_close_request_generation = object()
            requested.set()
        self._drain_shutdown_close_requests(signum=signum, frame=frame)

    def _drain_shutdown_close_requests(self, signum=None, frame=None):
        """Drain already-published requests without creating a new one."""
        requested = getattr(self, "_shutdown_close_requested", None)
        if requested is None or not requested.is_set():
            return
        handler_lock = getattr(self, "_shutdown_handler_lock", None)
        if handler_lock is None:  # compatibility for lightweight test hosts
            handler_lock = threading.Lock()
            self._shutdown_handler_lock = handler_lock
        if not handler_lock.acquire(blocking=False):
            self._shutdown_event.set()
            self._reconcile_wakeup_event.set()
            return
        publish_lock = getattr(self, "_shutdown_request_publish_lock", None)
        if publish_lock is None:
            publish_lock = threading.RLock()
            self._shutdown_request_publish_lock = publish_lock
        deferred_request_generation = None
        defer_same_attempt = False
        primary_error = None
        primary_traceback = None
        try:
            while True:
                with publish_lock:
                    if not requested.is_set():
                        break
                    requested.clear()
                    attempt_generation = getattr(
                        self,
                        "_shutdown_close_request_generation",
                        None,
                    )
                with self._shutdown_lock:
                    active_before_attempt_generation = getattr(
                        self,
                        "_emergency_close_generation",
                        None,
                    )
                    active_before_attempt = bool(
                        getattr(self, "_emergency_in_progress", False)
                        or (
                            active_before_attempt_generation is not None
                            and not active_before_attempt_generation[
                                "done"
                            ].is_set()
                        )
                    )
                try:
                    handled = self._shutdown_handler_once(
                        signum=signum,
                        frame=frame,
                    )
                except BaseException as exc:
                    with publish_lock:
                        requested.set()
                    with self._shutdown_lock:
                        generation = getattr(
                            self,
                            "_emergency_close_generation",
                            None,
                        )
                        active_generation = bool(
                            getattr(self, "_emergency_in_progress", False)
                            or (
                                generation is not None
                                and not generation["done"].is_set()
                            )
                        )
                        same_attempt_generation = bool(
                            generation is not None
                            and generation.get("request_generation")
                            is attempt_generation
                        )
                    defer_same_attempt = (
                        same_attempt_generation
                        or (
                            not active_generation
                            and not active_before_attempt
                        )
                    )
                    deferred_request_generation = attempt_generation
                    primary_error = exc
                    primary_traceback = exc.__traceback__
                    break
                if not handled:
                    with publish_lock:
                        requested.set()
                    with self._shutdown_lock:
                        generation = getattr(
                            self,
                            "_emergency_close_generation",
                            None,
                        )
                        active_generation = bool(
                            getattr(self, "_emergency_in_progress", False)
                            or (
                                generation is not None
                                and not generation["done"].is_set()
                            )
                        )
                        same_attempt_generation = bool(
                            generation is not None
                            and generation.get("request_generation")
                            is attempt_generation
                        )
                    defer_same_attempt = (
                        same_attempt_generation
                        or (
                            not active_generation
                            and not active_before_attempt
                        )
                    )
                    deferred_request_generation = attempt_generation
                    break
        finally:
            handler_lock.release()
        # Close the release/check race: a request arriving before release saw
        # the busy gate; one arriving afterwards can become the next owner.
        with publish_lock:
            pending_request = requested.is_set()
            current_request_generation = getattr(
                self,
                "_shutdown_close_request_generation",
                None,
            )
        if pending_request:
            with self._shutdown_lock:
                generation = getattr(
                    self,
                    "_emergency_close_generation",
                    None,
                )
                emergency_closed = bool(
                    getattr(self, "_emergency_closed", False)
                )
                active_generation = bool(
                    getattr(self, "_emergency_in_progress", False)
                    or (
                        generation is not None
                        and not generation["done"].is_set()
                    )
                )
            same_failed_attempt = (
                defer_same_attempt
                and current_request_generation is deferred_request_generation
                and not emergency_closed
            )
            if (
                pending_request
                and not active_generation
                and not same_failed_attempt
            ):
                if primary_error is None:
                    self._drain_shutdown_close_requests(
                        signum=signum,
                        frame=frame,
                    )
                else:
                    try:
                        self._drain_shutdown_close_requests(
                            signum=signum,
                            frame=frame,
                        )
                    except BaseException as secondary:
                        try:
                            primary_error.add_note(
                                "newer shutdown request drain failed: "
                                f"{type(secondary).__name__}: {secondary}"
                            )
                        except BaseException:
                            pass
        if primary_error is not None:
            raise primary_error.with_traceback(primary_traceback)

    def _shutdown_handler_once(self, signum=None, frame=None) -> bool:
        """Signal handler  sets shutdown event and triggers emergency close.

        Emergency close runs in a side-thread with a hard deadline so a hanging
        exchange API can't block the process forever; the ticker pool is torn
        down after close to prevent zombie threads.

        Fast-path: zero open positions  return immediately, skipping the full
        close machinery (thread, fetch_tickers, state loop).
        """
        from core.logger import log_event as _log_event

        def log_event(*args, **kwargs):
            try:
                _log_event(*args, **kwargs)
            except Exception:
                pass

        # Latch only when the flatten has FULLY succeeded  a partial-failure
        # shutdown must stay un-latched so a repeat signal / atexit can RETRY the
        # still-open legs (the emergency helper re-snapshots state, so only the
        # remaining legs are retried). _emergency_in_progress prevents a concurrent
        # second run; _shutdown_event is still set so the rest of the bot winds down.
        with self._shutdown_lock:
            if getattr(self, "_shutdown_positions_preserved", False):
                return True
            if getattr(self, "_emergency_closed", False):
                return True
            self._shutdown_event.set()
            self._reconcile_wakeup_event.set()
            generation = getattr(
                self,
                "_emergency_close_generation",
                None,
            )
            if (
                getattr(self, "_emergency_in_progress", False)
                or (
                    generation is not None
                    and not generation["done"].is_set()
                )
            ):
                self._emergency_in_progress = True
                return False
            self._emergency_in_progress = True

        # FAST-PATH: nothing to close
        try:
            open_count = self.state.count()
        except Exception:
            open_count = -1

        if open_count == 0:
            log_event(
                f" Shutdown signal {signum if signum else 'atexit'}  "
                f"no open positions, clean exit",
                "INFO"
            )
            self._emergency_closed = True
            self._emergency_in_progress = False
            self._shutdown_ticker_cache_if_flat()
            return True

        log_event(
            f" Shutdown signal {signum if signum else 'atexit'} received  "
            f"closing {open_count if open_count > 0 else 'all'} position(s)",
            "WARN"
        )

        publish_lock = getattr(self, "_shutdown_request_publish_lock", None)
        if publish_lock is None:
            publish_lock = threading.RLock()
            self._shutdown_request_publish_lock = publish_lock
        with publish_lock:
            close_request_generation = getattr(
                self,
                "_shutdown_close_request_generation",
                None,
            )
        result = {"done": False, "error": None, "failed_count": 0}
        generation = {
            "done": threading.Event(),
            "runner": None,
            "start_raised": False,
            "result": result,
            "request_generation": close_request_generation,
        }

        def _close_runner():
            try:
                res = self._emergency_close_all(reason=f"Shutdown signal {signum}")
                if not isinstance(res, dict):
                    raise RuntimeError("Invalid emergency close result: expected dict")
                failed_count = res.get("failed_count")
                if type(failed_count) is not int or failed_count < 0:
                    raise RuntimeError(
                        "Invalid emergency close result: failed_count must be "
                        "a non-negative integer"
                    )
                result["failed_count"] = failed_count
                result["done"] = True
            except Exception as e:
                result["error"] = e
            finally:
                with self._shutdown_lock:
                    owns_generation = (
                        getattr(self, "_emergency_close_generation", None)
                        is generation
                    )
                    if owns_generation:
                        if result["done"] and result["failed_count"] == 0:
                            self._emergency_closed = True
                        self._emergency_in_progress = False
                    generation["done"].set()
                with publish_lock:
                    pending = getattr(
                        self,
                        "_shutdown_close_requested",
                        None,
                    )
                    pending_request = bool(
                        pending is not None and pending.is_set()
                    )
                    current_request_generation = getattr(
                        self,
                        "_shutdown_close_request_generation",
                        None,
                    )
                close_succeeded = (
                    result["done"] and result["failed_count"] == 0
                )
                if (
                    owns_generation
                    and pending_request
                    and (
                        close_succeeded
                        or current_request_generation
                        is not generation["request_generation"]
                    )
                ):
                    self._drain_shutdown_close_requests(
                        signum="Deferred repeat shutdown signal"
                    )

        try:
            runner = threading.Thread(target=_close_runner,
                                      daemon=True,
                                      name=f"{self.BOT_NAME}EmergencyClose")
        except BaseException as exc:
            with self._shutdown_lock:
                self._emergency_in_progress = False
            try:
                self._log_error("Emergency close thread construction", exc)
            except BaseException:
                pass
            if not isinstance(exc, Exception):
                raise
            return False
        generation["runner"] = runner
        with self._shutdown_lock:
            self._emergency_close_generation = generation
        try:
            runner.start()
        except BaseException as exc:
            generation["start_raised"] = True
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(runner)
            ):
                with self._shutdown_lock:
                    generation["done"].set()
                    if self._emergency_close_generation is generation:
                        self._emergency_close_generation = None
                        self._emergency_in_progress = False
            try:
                self._log_error("Emergency close thread start", exc)
            except BaseException:
                pass
            if not isinstance(exc, Exception):
                raise
            return False
        try:
            runner.join(timeout=self.SHUTDOWN_DEADLINE_SEC)
        except Exception as exc:
            self._log_error("Emergency close thread join", exc)
            return False
        # The worker owns the in-progress latch and clears it in ``finally``.
        # Never write a sampled ``True`` back here: the worker can terminate
        # between is_alive() returning and the assignment, which would relatch
        # an already-finished partial close and block every later retry.
        try:
            runner_alive = runner.is_alive()
        except Exception:
            runner_alive = True
        generation_done = generation["done"].is_set()
        if not runner_alive and not generation_done:
            return False
        if not (result["done"] and result["failed_count"] == 0):
            # Leave UN-latched so a repeat SIGTERM / atexit retries the rest.
            if runner_alive:
                log_event(
                    f" Emergency close exceeded {self.SHUTDOWN_DEADLINE_SEC}s "
                    f"deadline. Open positions may remain  a repeat shutdown "
                    f"signal will retry; close MANUALLY if exiting now.",
                    "WARN"
                )
            elif result["error"]:
                self._log_error("Shutdown handler", result["error"])
            elif result["failed_count"]:
                log_event(
                    f" Emergency close incomplete  {result['failed_count']} "
                    f"leg(s) failed. A repeat shutdown signal will retry them; "
                    f"Telegram alert lists them. Close MANUALLY if exiting now.",
                    "WARN"
                )

        self._shutdown_ticker_cache_if_flat()
        return True

    def _emergency_close_all(self, reason: str = "Shutdown") -> None:
        from core.logger import log_event, log_sell, send_telegram, save_trade, log_struct
        from core.database import save_trade_db, remove_futures_state
        from config.exchange_config import reduce_only_params
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        return emergency_close_all_futures(
            ex=self.ex,
            state=self.state,
            bot_name=self.BOT_NAME,
            log_dir=self.LOG_DIR,
            simulation=self.simulation,
            default_leverage=float(self.C("LEVERAGE", 3)),
            # CROSS overrides MARGIN_MODE="cross"; FUTURES stays isolated. The
            # emergency close MUST send the same margin_mode the position was
            # opened with or MEXC rejects the reduce-only order.
            margin_mode=str(self.C("MARGIN_MODE", "isolated")),
            shutdown_event=self._shutdown_event,
            reason=reason,
            ticker_cache=self.ticker_cache,
            telegram_token=TELEGRAM_TOKEN,
            telegram_chat_id=TELEGRAM_CHAT_ID,
            log_event=log_event,
            log_sell=log_sell,
            log_struct=log_struct,
            save_trade_db=save_trade_db,
            save_trade=save_trade,
            send_telegram=send_telegram,
            remove_futures_state=remove_futures_state,
            reduce_only_params=reduce_only_params,
            error_logger=self._log_error,
        )

    #  Abstract: subclass-required 

    @staticmethod
    @abstractmethod
    def EXCHANGE_FACTORY():
        """Return a connected ccxt futures exchange instance."""
