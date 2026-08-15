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
from bot_utils.api_budget import try_consume_api_call
from bot_utils.runtime_threads import (finalize_runtime_shutdown,
                                       start_threads_or_shutdown)
from bot_utils.silent_log import silent_log
from bot_utils.trade_state import state_exposure_count
from core.clock import now_utc

from core.futures_bot_exits import FuturesExitsMixin
from core.futures_bot_scan import FuturesScanMixin
from core.futures_bot_reconcile import FuturesReconcileMixin


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
        self._cooldown_lock = threading.Lock()
        self._markout_health_lock = threading.Lock()
        self._venue_health_lock = threading.Lock()
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

        # Threads
        self._monitor_thread: Optional[threading.Thread] = None
        self._scan_thread: Optional[threading.Thread] = None
        self._reconcile_thread: Optional[threading.Thread] = None
        self._markout_thread: Optional[threading.Thread] = None
        self._venue_recorder_thread: Optional[threading.Thread] = None
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
            return self.cfg.get(key, default)

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
    ) -> dict[str, Any]:
        """Build the bounded SIM-evidence WAL committed with position state."""
        from core.logger import _date as _utc_now_str

        return {
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

    def _finalize_simulated_entry_tca(
        self,
        base: str,
        pending: dict,
        *,
        restart_recovery: bool = False,
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
            if not self.state.update_many(
                base, {self._SIM_TCA_PENDING_FIELD: None}
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
        lock = getattr(self, "_entry_recovery_lock", None)
        if lock is None:
            blocked = bool(getattr(self, "_entry_recovery_blocked", False))
            health = getattr(self, "_entry_recovery_health", None)
        else:
            with lock:
                blocked = bool(
                    getattr(self, "_entry_recovery_blocked", False)
                )
                health = getattr(self, "_entry_recovery_health", None)
        if isinstance(health, dict):
            result = dict(health)
        else:
            result = {}
        if not blocked and (not result or result.get("ok") is True):
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

    def _runtime_status_health(
        self,
        threads: dict[str, bool],
    ) -> tuple[str, dict[str, Any]]:
        """Combine worker liveness with reported component health."""
        try:
            strategy_health = self._strategy_runtime_health()
            if not isinstance(strategy_health, dict):
                strategy_health = {
                    "ok": False,
                    "error_type": "invalid_strategy_health_payload",
                }
        except Exception as exc:
            strategy_health = {
                "ok": False,
                "error_type": type(exc).__name__,
            }
        strategy_ok = not strategy_health or strategy_health.get("ok") is True
        ticker_health = self._ticker_runtime_health()
        ticker_ok = not ticker_health or ticker_health.get("ok") is True
        markout_health = self._markout_runtime_health()
        markout_ok = not markout_health or markout_health.get("ok") is True
        sim_evidence_health = self._sim_evidence_runtime_health()
        sim_evidence_ok = (
            not sim_evidence_health
            or sim_evidence_health.get(
                "runtime_ok", sim_evidence_health.get("ok")
            ) is True
        )
        venue_health = self._venue_runtime_health()
        venue_ok = not venue_health or venue_health.get("ok") is True
        entry_recovery_health = self._entry_recovery_runtime_health()
        entry_recovery_ok = (
            not entry_recovery_health
            or entry_recovery_health.get("ok") is True
        )
        position_integrity_health = self._position_integrity_runtime_health()
        position_integrity_ok = (
            not position_integrity_health
            or position_integrity_health.get(
                "runtime_ok", position_integrity_health.get("ok")
            ) is True
        )
        status = (
            "ready"
            if (
                all(threads.values())
                and strategy_ok
                and ticker_ok
                and markout_ok
                and sim_evidence_ok
                and venue_ok
                and entry_recovery_ok
                and position_integrity_ok
            )
            else "degraded"
        )
        extra = {}
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
        if entry_recovery_health:
            extra["entry_recovery_health"] = entry_recovery_health
        if position_integrity_health:
            extra["position_integrity_health"] = position_integrity_health
        return status, extra

    def _owns_markout_worker(self) -> bool:
        """Let any futures strategy provide failover for the global queue."""
        return str(self.BOT_NAME).upper() in {"FUTURES", "CROSS", "FUTREND"}

    def _record_markout_worker_health(self, report: dict) -> None:
        """Receive one sanitized progress report from the markout thread."""
        if not isinstance(report, dict):
            return
        report_ok = report.get("ok") is True

        def nonnegative_int(value) -> int:
            if isinstance(value, bool):
                return 0
            try:
                return max(0, int(value or 0))
            except (TypeError, ValueError, OverflowError):
                return 0

        def nonnegative_float(value) -> float:
            if isinstance(value, bool):
                return 0.0
            try:
                parsed = float(value or 0.0)
            except (TypeError, ValueError, OverflowError):
                return 0.0
            return max(0.0, parsed) if math.isfinite(parsed) else 0.0

        def optional_nonnegative_float(value) -> float | None:
            if value is None or isinstance(value, bool):
                return None
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return None
            return max(0.0, parsed) if math.isfinite(parsed) else None

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
                "oldest_due_at": str(oldest)[:32] if oldest else None,
                "oldest_overdue_seconds": overdue,
                "next_runnable_at": (
                    str(next_runnable)[:32] if next_runnable else None
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
            "worker_lock_release_failed",
        }
        reason = str(report.get("reason") or "")
        if report_ok:
            reason = ""
        elif reason not in allowed_reasons or not reason:
            reason = "worker_error"
        wake_strategy = str(report.get("wake_strategy") or "fixed_interval")
        if wake_strategy not in {
            "deadline_or_local_commit",
            "fixed_interval",
            "fixed_error_backoff",
        }:
            wake_strategy = "fixed_interval"
        with self._markout_health_lock:
            previous_errors = int(
                self._markout_health.get("consecutive_errors") or 0
            )
            self._markout_health.update({
                "ok": report_ok,
                "last_poll_monotonic": report.get("last_poll_monotonic"),
                "last_poll_wall_ts": report.get("last_poll_wall_ts"),
                "consecutive_errors": (
                    0 if report_ok else previous_errors + 1
                ),
                "last_error": (
                    ""
                    if report_ok
                    else str(
                        report.get("error")
                        or report.get("reason")
                        or "markout worker unhealthy"
                    )[:200]
                ),
                "due_count": nonnegative_int(report.get("due_count")),
                "oldest_due_at": (
                    str(report.get("oldest_due_at"))[:32]
                    if report.get("oldest_due_at") else None
                ),
                "oldest_overdue_seconds": nonnegative_float(
                    report.get("oldest_overdue_seconds")
                ),
                "next_runnable_at": (
                    str(report.get("next_runnable_at"))[:32]
                    if report.get("next_runnable_at") else None
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
                "last_completed_wall_ts": report.get(
                    "last_completed_wall_ts"
                ),
                "lock_state": str(report.get("lock_state") or "unknown")[:32],
                "worker_family": "futures",
                "producer_bots": ["CROSS", "FUTREND", "FUTURES"],
                "progress_scope": "worker_market_family",
                "progress_is_bot_scoped": False,
            })

    def _markout_runtime_health(self) -> dict[str, Any]:
        if not self._owns_markout_worker() or not hasattr(
            self, "_markout_health_lock"
        ):
            return {}
        with self._markout_health_lock:
            snapshot = dict(self._markout_health)
        last_poll = snapshot.get("last_poll_monotonic")
        if last_poll is None:
            started = getattr(self, "_markout_started_monotonic", None)
            if started is None:
                startup_age = 0.0
            else:
                try:
                    startup_age = max(
                        0.0, time.monotonic() - float(started)
                    )
                except (TypeError, ValueError, OverflowError):
                    startup_age = float("inf")
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
            poll_age = max(0.0, time.monotonic() - float(last_poll))
        except (TypeError, ValueError, OverflowError):
            poll_age = float("inf")
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
                if unavailable is not None and unavailable != (
                    "capture_contract_size_unavailable"
                ):
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

        def nonnegative_int(key: str) -> int:
            try:
                return max(0, int(report.get(key) or 0))
            except (TypeError, ValueError, OverflowError):
                return 0

        with self._venue_health_lock:
            self._venue_health = {
                "ok": report.get("ok") is True,
                "reason": str(report.get("reason") or "")[:64],
                "last_poll_monotonic": report.get("last_poll_monotonic"),
                "last_poll_wall_ts": report.get("last_poll_wall_ts"),
                "rest_ok": report.get("rest_ok") is True,
                "l2_enabled": report.get("l2_enabled") is True,
                "l2_ok": report.get("l2_ok") is True,
                "l2_data_healthy": report.get(
                    "l2_data_healthy", True
                ) is True,
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
                "last_capture_success_wall_ts": report.get(
                    "last_capture_success_wall_ts"
                ),
                "last_capture_error": str(
                    report.get("last_capture_error") or ""
                )[:200],
                "last_overview_error": str(
                    report.get("last_overview_error") or ""
                )[:200],
                "last_microstructure_error": str(
                    report.get("last_microstructure_error") or ""
                )[:200],
                "retention_ok": report.get("retention_ok", True) is True,
                "retention_errors_total": nonnegative_int(
                    "retention_errors_total"
                ),
                "last_retention_error": str(
                    report.get("last_retention_error") or ""
                )[:200],
                "l2_consecutive_errors": nonnegative_int(
                    "l2_consecutive_errors"
                ),
                "l2_errors_total": nonnegative_int("l2_errors_total"),
                "last_l2_error": str(
                    report.get("last_l2_error") or ""
                )[:200],
            }

    def _venue_runtime_health(self) -> dict[str, Any]:
        recorder_thread = getattr(self, "_venue_recorder_thread", None)
        if recorder_thread is None or not hasattr(
            self, "_venue_health_lock"
        ):
            return {}
        with self._venue_health_lock:
            snapshot = dict(self._venue_health)
        stale_sec = max(
            self.VENUE_HEALTH_MIN_STALE_SEC,
            float(getattr(self, "_venue_health_stale_sec", 0.0) or 0.0),
        )
        last_poll = snapshot.get("last_poll_monotonic")
        if last_poll is None:
            started = getattr(self, "_venue_started_monotonic", None)
            try:
                startup_age = (
                    0.0
                    if started is None
                    else max(0.0, time.monotonic() - float(started))
                )
            except (TypeError, ValueError, OverflowError):
                startup_age = float("inf")
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
            poll_age = max(0.0, time.monotonic() - float(last_poll))
        except (TypeError, ValueError, OverflowError):
            poll_age = float("inf")
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
            status, strategy_health = self._runtime_status_health(threads)
            state_rows = self.state.get_all()
            if log_snapshot:
                from trading.runtime_observability import (
                    log_runtime_observability,
                )

                observability = log_runtime_observability(
                    bot_name=self.BOT_NAME,
                    mode="SIM" if self.simulation else "LIVE",
                    state_rows=state_rows,
                    ticker_cache=self.ticker_cache,
                )
            else:
                from trading.runtime_observability import (
                    runtime_observability_snapshot,
                )

                observability = runtime_observability_snapshot(
                    state_rows=state_rows,
                    ticker_cache=self.ticker_cache,
                )
            status_writer(
                self.LOG_DIR,
                self.BOT_NAME,
                status,
                self.simulation,
                threads=threads,
                extra={
                    "open_positions": state_exposure_count(self.state),
                    "safe_mode": bool(self.safe_mode.is_active()),
                    **observability,
                    **strategy_health,
                },
            )
        except Exception as exc:
            phase = "heartbeat" if log_snapshot else "periodic"
            silent_log(f"{self.BOT_NAME} {phase} runtime status", exc)

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
        self.cool = load_j(self.COOLDOWN_FILE) or {}
        if not isinstance(self.cool, dict):
            self.cool = {}

        # M-7 fix: purge expired/malformed cooldown entries on startup so
        # an old entry from a previous format/run can't block a coin
        # indefinitely. See spot_bot.py for the same change.
        try:
            try:
                from trading.cooldown_utils import purge_expired
            except ImportError:
                from bot_utils.cooldown_utils import purge_expired
            removed = purge_expired(self.cool, self.COOLDOWN_FILE)
            if removed > 0:
                log_event(
                    f"Cooldown reload: purged {removed} expired/malformed "
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
            actual = set(self.state.keys())
            for entry in get_futures_state(
                    self.BOT_NAME, mode_is_sim=self.simulation):
                sym = entry.get("symbol")
                if sym and sym not in actual:
                    remove_futures_state(
                        sym, self.BOT_NAME, mode_is_sim=self.simulation)
                    log_event(f"Stale futures_state entry cleaned: {sym}", "INFO")
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

        #  Register shutdown handlers 
        try:
            signal.signal(signal.SIGINT, self._shutdown_handler)
            signal.signal(signal.SIGTERM, self._shutdown_handler)
            if hasattr(signal, "SIGBREAK"):
                signal.signal(signal.SIGBREAK, self._shutdown_handler)
        except Exception:
            pass
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
        if (
            self.BOT_NAME == "FUTURES"
            and str(self.C("VENUE_RECORDER_MODE", "enabled")).lower() == "enabled"
        ):
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
                    self.C("VENUE_RECORDER_MAX_STORAGE_GIB", 20.0)
                ),
                log_event=log_event,
                l2_mode=str(self.C("VENUE_L2_MODE", "shadow")),
                l2_sample_interval_seconds=float(
                    self.C("VENUE_L2_SAMPLE_INTERVAL_SECONDS", 1.0)
                ),
                l2_stale_after_ms=int(
                    self.C("VENUE_L2_STALE_AFTER_MS", 5_000)
                ),
                health_callback=self._record_venue_recorder_health,
            )
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
        threads = self._runtime_threads()
        status, strategy_health = self._runtime_status_health(threads)
        write_runtime_status(
            self.LOG_DIR, self.BOT_NAME,
            status,
            self.simulation,
            threads=threads,
            extra={
                "open_positions": state_exposure_count(self.state),
                **strategy_health,
            })

        #  Main thread: heartbeat + shutdown wait 
        try:
            last_heartbeat = 0.0
            last_runtime_status = 0.0
            self._last_hourly_status = 0.0
            while not self._shutdown_event.is_set():
                now = time.time()
                if now - last_heartbeat >= self.HEARTBEAT_INTERVAL_SEC:
                    tc = state_exposure_count(self.state)
                    sm_marker = "  SAFE_MODE" if self.safe_mode.is_active() else ""
                    log_event(
                        f" {self.BOT_NAME} heartbeat  "
                        f"Open: {tc}/{self.C('MAX_OPEN_TRADES')}  "
                        f"Monitor={'' if self._monitor_thread.is_alive() else ''}  "
                        f"Scan={'' if self._scan_thread.is_alive() else ''}"
                        f"{sm_marker}",
                        "INFO"
                    )
                    self._publish_periodic_runtime_status(
                        write_runtime_status, log_snapshot=True
                    )
                    last_heartbeat = now
                if now - last_runtime_status >= 5.0:
                    self._publish_periodic_runtime_status(
                        write_runtime_status, log_snapshot=False
                    )
                    last_runtime_status = now
                # Hourly Telegram status (realized + unrealized PnL + positions).
                # Self-throttling; covers FUTURES and CROSS (both FuturesBot).
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
                self._shutdown_event.wait(timeout=2)
        except KeyboardInterrupt:
            self._shutdown_handler(signum="KeyboardInterrupt")

        log_event("Waiting for threads to finish...", "INFO")
        # Short join timeout  daemon threads die at interpreter exit anyway.
        for t in (
            self._monitor_thread,
            self._scan_thread,
            self._reconcile_thread,
            self._markout_thread,
            self._venue_recorder_thread,
        ):
            if t and t.is_alive():
                t.join(timeout=2)
        # A fetch that was already running during the signal handler may have
        # completed while core threads were joining. Confirm pool teardown now
        # so a non-daemon executor worker cannot be reported as cleanly closed.
        finalize_runtime_shutdown(
            self,
            write_runtime_status,
            log_event,
            resource_closers={
                "ticker_cache": self._shutdown_ticker_cache_if_flat,
            },
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
        from core.logger import log_event

        def _startup_probe_allowed(endpoint: str) -> bool:
            try:
                return bool(try_consume_api_call(endpoint))
            except Exception as budget_exc:
                log_event(
                    f"Futures startup probe skipped - API budget gate "
                    f"unavailable ({type(budget_exc).__name__})",
                    "WARN",
                )
                return False

        try:
            raw_ex = self.EXCHANGE_FACTORY()
            # HTTP timeout  higher at startup for slow load_markets
            raw_ex.timeout = 30_000
            for attempt in range(1, 4):
                try:
                    try:
                        markets_allowed = bool(try_consume_api_call(
                            "futures_startup_load_markets",
                            critical=True,
                        ))
                    except Exception as budget_exc:
                        raise RuntimeError(
                            "futures load_markets API budget gate unavailable"
                        ) from budget_exc
                    if not markets_allowed:
                        raise RuntimeError(
                            "futures load_markets API budget exhausted"
                        )
                    raw_ex.load_markets()
                    break
                except Exception as le:
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
            if _startup_probe_allowed("futures_startup_auth_fetch_balance"):
                try:
                    _bal = raw_ex.fetch_balance()
                    # We only care that the call succeeded; ignore content.
                    _ = (_bal or {}).get("USDT", {})
                except Exception as se:
                    # Don't fail the connect on a transient network blip:
                    # log and continue. If the error is a real 401/403 the
                    # next fetch_balance will surface it too.
                    err_lc = str(se).lower()
                    if any(m in err_lc for m in (
                            "auth", "signature", "permission", "forbidden",
                            "401", "403", "ip", "passphrase")):
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
            except Exception as wrap_err:
                log_event(
                    f"ThreadLocalExchange unavailable ({wrap_err})  "
                    f"falling back to shared exchange instance", "WARN")
                self.ex = raw_ex

            # Smoke test of the futures API surface.
            # In SIMULATION mode fetch_balance is not critical  skip to
            # avoid false-alarm ERROR badges when Bitget is temporarily
            # unreachable. In LIVE mode we distinguish transient network
            # errors (INFO) from auth failures (WARN) which are actionable.
            if (not self.C("SIMULATION", True)
                    and _startup_probe_allowed(
                        "futures_startup_live_fetch_balance")):
                try:
                    self.ex.fetch_balance()
                except Exception as bal_err:
                    err_str = str(bal_err).lower()
                    is_auth = any(x in err_str for x in
                                  ("401", "403", "invalid api",
                                   "apikey", "unauthorized", "forbidden"))
                    if is_auth:
                        log_event(
                            f"H-6 smoke test: fetch_balance  AUTH FAILURE "
                            f"({type(bal_err).__name__}: {str(bal_err)[:80]}) "
                            f" check API key permissions", "WARN")
                    else:
                        # NetworkError / timeout  transient, not actionable
                        log_event(
                            f"H-6 smoke test: fetch_balance transient error "
                            f"({type(bal_err).__name__})  bot will retry on "
                            f"first reconcile cycle", "INFO")
            if _startup_probe_allowed("futures_startup_fetch_positions"):
                try:
                    # fetch_positions sometimes needs a symbol on Bitget; we
                    # only need to know the call path WORKS, not the data.
                    self.ex.fetch_positions(["BTC/USDT:USDT"])
                except Exception as pos_err:
                    log_event(
                        f"H-6 smoke test note: fetch_positions raised "
                        f"{type(pos_err).__name__} (non-fatal; reconcile "
                        f"will retry)", "INFO")

            log_event("Futures API connection established", "INFO")
            return True
        except Exception as e:
            log_event(f"Futures connection failed: {e}", "WARN")
            self._log_error("Connection", e)
            return False

    #  Shutdown 

    def _shutdown_ticker_cache_if_flat(self) -> bool:
        """Keep price fetching alive while an emergency retry is still needed."""
        if not getattr(self, "_emergency_closed", False):
            return False
        cache = getattr(self, "ticker_cache", None)
        if cache is None:
            return True
        try:
            shutdown_result = cache.shutdown()
            if shutdown_result is False:
                raise RuntimeError("ticker pool still has running work")
            return True
        except Exception as exc:
            self._log_error("Ticker pool shutdown", exc)
            return False

    def _shutdown_handler(self, signum=None, frame=None):
        """Signal handler  sets shutdown event and triggers emergency close.

        Emergency close runs in a side-thread with a hard deadline so a hanging
        exchange API can't block the process forever; the ticker pool is torn
        down after close to prevent zombie threads.

        Fast-path: zero open positions  return immediately, skipping the full
        close machinery (thread, fetch_tickers, state loop).
        """
        from core.logger import log_event

        # Latch only when the flatten has FULLY succeeded  a partial-failure
        # shutdown must stay un-latched so a repeat signal / atexit can RETRY the
        # still-open legs (the emergency helper re-snapshots state, so only the
        # remaining legs are retried). _emergency_in_progress prevents a concurrent
        # second run; _shutdown_event is still set so the rest of the bot winds down.
        with self._shutdown_lock:
            if getattr(self, "_emergency_closed", False):
                return
            self._shutdown_event.set()
            self._reconcile_wakeup_event.set()
            if getattr(self, "_emergency_in_progress", False):
                return
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
            return

        log_event(
            f" Shutdown signal {signum if signum else 'atexit'} received  "
            f"closing {open_count if open_count > 0 else 'all'} position(s)",
            "WARN"
        )

        result = {"done": False, "error": None, "failed_count": 0}

        def _close_runner():
            try:
                res = self._emergency_close_all(reason=f"Shutdown signal {signum}")
                result["failed_count"] = int((res or {}).get("failed_count", 0))
                result["done"] = True
            except Exception as e:
                result["error"] = e
            finally:
                with self._shutdown_lock:
                    if result["done"] and result["failed_count"] == 0:
                        self._emergency_closed = True
                    self._emergency_in_progress = False

        try:
            runner = threading.Thread(target=_close_runner,
                                      daemon=True,
                                      name=f"{self.BOT_NAME}EmergencyClose")
            runner.start()
        except Exception as exc:
            with self._shutdown_lock:
                self._emergency_in_progress = False
            self._log_error("Emergency close thread start", exc)
            return
        runner.join(timeout=self.SHUTDOWN_DEADLINE_SEC)
        self._emergency_in_progress = runner.is_alive()
        if result["done"] and result["failed_count"] == 0:
            # Fully flat  latch so atexit/repeat-signal won't redo the work.
            self._emergency_closed = True
            self._emergency_in_progress = False
        else:
            # Leave UN-latched so a repeat SIGTERM / atexit retries the rest.
            if runner.is_alive():
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
