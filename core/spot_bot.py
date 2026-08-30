"""
core/spot_bot.py  Multi-thread spot trading bot base class.

Replaces the duplicated run_bot() loops in main_bot_balanced.py and
main_bot_aggressive.py with a single SpotBot class.

Architecture (mirrors futures bot v6.1):
  MONITOR-Thread  (every MONITOR_INTERVAL ~20s)  exits only
  SCAN-Thread  (every SCAN_INTERVAL ~150s)  new entries only
  RECONCILE-Thread  (every RECONCILE_INTERVAL_SEC ~600s)  drift check
  MARKOUT-Thread  restart-safe execution-evidence processing + failover
  MAIN-Thread  heartbeat + shutdown coordination

Open positions are no longer blocked behind slow LLM-analysis scan cycles.

Subclass contract:
  Required class attributes:
    BOT_NAME  str  "TREND" / "SPOT"
    BOT_COLOR  str  ANSI color for separator log lines
    LOG_DIR  str  absolute log directory path
    DB_FILE  str  "{LOG_DIR}/trades.json"
    COOLDOWN_FILE  str  "{LOG_DIR}/cooldown.json"
    DEFAULTS  dict  default config values (see balanced/aggressive)
    NEWS_MODULE_PATH  str  dotted import path to news_brain variant
    BUY_PREFIX  str  clientOrderId prefix (e.g. "bal" or "agg")
    EXCHANGE_FACTORY  callable  get_spot_exchange_connection or similar

  Optional class attributes:
    BACKTEST_NOTE  str  shown on startup, e.g. "~+4% ROI 60d"
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
    emergency_close_all_spot,
    SafeMode,
)
from bot_utils.api_budget import try_consume_api_call
from bot_utils.runtime_threads import (finalize_runtime_shutdown,
                                       format_runtime_thread_liveness,
                                       shared_runtime_resource_closers,
                                       start_threads_or_shutdown)
from bot_utils.silent_log import silent_log

# Mixins: split across files to keep this module focused on lifecycle.
from core.spot_bot_exits import ExitsMixin
from core.spot_bot_scan import ScanMixin
from core.spot_bot_reconcile import ReconcileMixin


# 
# SpotBot  multiple-inheritance composition
#
#  ExitsMixin  _monitor_loop, _check_position_exits, exits execution
#  ScanMixin  _scan_loop,  _try_open_trade, quality filters
#  ReconcileMixin  _reconcile_loop
# 

class SpotBot(ExitsMixin, ScanMixin, ReconcileMixin, ABC):
    #  Subclass-overridable class attributes 
    BOT_NAME: str = "BASE"
    BOT_COLOR: str = "\033[96m"
    LOG_DIR: str = ""
    DB_FILE: str = ""
    COOLDOWN_FILE: str = ""
    DEFAULTS: Dict[str, Any] = {}
    NEWS_MODULE_PATH: str = "news.news_brain_spot"
    BUY_PREFIX: str = "spot"
    BACKTEST_NOTE: str = ""
    # USES_LLM=False (e.g. the mechanical Trend bot) skips the eager
    # news-module import at startup and never touches the LLM at all.
    USES_LLM: bool = True

    # Reconciliation cadence (constant across bots  could be overridden)
    RECONCILE_INTERVAL_SEC: int = 300  # 5 min  orphan-adoption safety net cadence
    GC_LOCKS_INTERVAL_SEC: int = 300   # 5 min
    HEARTBEAT_INTERVAL_SEC: int = 60   # main-thread heartbeat
    MARKOUT_POLL_INTERVAL_SEC: float = 1.0
    MARKOUT_MAX_OVERDUE_SEC: float = 30.0
    MARKOUT_POLL_STALE_SEC: float = 15.0
    # Hard deadline for emergency_close_all during shutdown, so a hanging
    # exchange API can't block the bot forever (and force a kill -9 that could
    # corrupt trades.json mid-write).
    SHUTDOWN_DEADLINE_SEC: float = 45.0
    # Monitor / Scan defaults  subclass can override DEFAULTS["MONITOR_INTERVAL"]
    DEFAULT_MONITOR_INTERVAL: int = 20

    # Circuit breaker constants
    CB_FAILURE_THRESHOLD: int = 5
    CB_MAX_BACKOFF: float = 600.0
    CB_INITIAL_BACKOFF: float = 30.0

    #  Lifecycle 

    def __init__(self, simulation: bool = True):
        self.simulation = simulation
        # Isolate SIM state from LIVE state: redirect state/cooldown files to a
        # .sim variant so a SIM/LIVE toggle never mixes paper and real state.
        from bot_utils.sim_flag import sim_state_path
        self.DB_FILE = sim_state_path(self.DB_FILE, simulation)
        self.COOLDOWN_FILE = sim_state_path(self.COOLDOWN_FILE, simulation)
        self.cfg = load_runtime_config(self.BOT_NAME, self.DEFAULTS)
        self._shutdown_event = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._cooldown_lock = threading.Lock()
        self._markout_health_lock = threading.Lock()
        self._position_integrity_health_lock = threading.Lock()
        # populated in run()
        self.ex = None
        # Spot uses batched ticker fetches instead of FuturesBot's TickerCache.
        # Runtime observability accepts None and reports an empty cache section.
        self.ticker_cache = None
        self.state: Optional[TradeState] = None
        self.cool: dict = {}
        # Safe-mode killswitch (initialized in run() after telegram config)
        self.safe_mode: Optional[SafeMode] = None
        # idempotency flag for emergency close
        self._emergency_closed = False
        self._shutdown_positions_preserved = False
        # Threads
        self._monitor_thread: Optional[threading.Thread] = None
        self._scan_thread: Optional[threading.Thread] = None
        self._reconcile_thread: Optional[threading.Thread] = None
        self._markout_thread: Optional[threading.Thread] = None
        self._markout_started_monotonic: float | None = None
        self._sim_evidence_health_cache: dict[str, Any] | None = None
        self._sim_evidence_health_last_monotonic: float | None = None
        self._position_integrity_started_monotonic = time.monotonic()
        self._position_integrity_health: dict[str, Any] = {}
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
            "worker_family": "spot",
            "producer_bots": ["SPOT", "TREND"],
            "progress_scope": "worker_market_family",
            "progress_is_bot_scoped": False,
        }

    # Convenience: config access as attribute-style. Routes through
    # bot_utils.config.get_live_value so user-edited values in the launcher's
    # settings UI take effect within ~5s without a bot restart; falls back to
    # the cached boot-time snapshot when the live read isn't possible.
    def C(self, key: str, default=None):
        try:
            from bot_utils.config import get_live_value
            return get_live_value(self.BOT_NAME, key, default,
                                   fallback_cfg=self.cfg)
        except Exception:
            return self.cfg.get(key, default)

    @cached_property
    def _news(self):
        """Lazy-import the news_brain module for this bot variant."""
        return importlib.import_module(self.NEWS_MODULE_PATH)

    #  Logging helpers (proxy to core.logger via local imports for hot reload) 

    def _log_error(self, context: str, exc: Exception) -> None:
        _ext_log_error(self.BOT_NAME, context, exc)

    def _runtime_threads(self) -> dict[str, bool]:
        return {
            "monitor": bool(
                self._monitor_thread and self._monitor_thread.is_alive()
            ),
            "scan": bool(self._scan_thread and self._scan_thread.is_alive()),
            "reconcile": bool(
                self._reconcile_thread and self._reconcile_thread.is_alive()
            ),
            "markout": bool(
                self._markout_thread and self._markout_thread.is_alive()
            ),
        }

    def _record_markout_worker_health(self, report: dict) -> None:
        """Receive one bounded health report from the shared queue worker."""
        if not isinstance(report, dict):
            return

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
            oldest = raw.get("oldest_due_at")
            next_runnable = raw.get("next_runnable_at")
            scopes[scope] = {
                "due_count": nonnegative_int(raw.get("due_count")),
                "oldest_due_at": str(oldest)[:32] if oldest else None,
                "oldest_overdue_seconds": nonnegative_float(
                    raw.get("oldest_overdue_seconds")
                ),
                "next_runnable_at": (
                    str(next_runnable)[:32] if next_runnable else None
                ),
                "next_runnable_seconds": optional_nonnegative_float(
                    raw.get("next_runnable_seconds")
                ),
                "timestamps_valid": raw.get("timestamps_valid", True) is True,
            }
        report_ok = report.get("ok") is True
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
            previous_errors = nonnegative_int(
                self._markout_health.get("consecutive_errors")
            )
            self._markout_health.update({
                "ok": report_ok,
                "last_poll_monotonic": report.get("last_poll_monotonic"),
                "last_poll_wall_ts": report.get("last_poll_wall_ts"),
                "consecutive_errors": 0 if report_ok else previous_errors + 1,
                "last_error": (
                    "" if report_ok else str(
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
                "worker_family": "spot",
                "producer_bots": ["SPOT", "TREND"],
                "progress_scope": "worker_market_family",
                "progress_is_bot_scoped": False,
            })

    def _markout_runtime_health(self) -> dict[str, Any]:
        with self._markout_health_lock:
            snapshot = dict(self._markout_health)
        last_poll = snapshot.get("last_poll_monotonic")
        if last_poll is None:
            started = self._markout_started_monotonic
            try:
                startup_age = (
                    0.0 if started is None
                    else max(0.0, time.monotonic() - float(started))
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

    @staticmethod
    def _sim_tca_pending_state_health(
        state_rows: Any,
        bot_name: str,
        *,
        now_wall: float | None = None,
        grace_seconds: int = 60,
    ) -> dict[str, int]:
        """Validate the bounded state-first SIM evidence WAL projection."""
        result = {
            "pending_state_count": 0,
            "pending_state_grace_count": 0,
            "pending_state_overdue_count": 0,
            "invalid_pending_state_count": 0,
        }
        if not isinstance(state_rows, dict):
            result["invalid_pending_state_count"] = 1
            return result
        if len(state_rows) > 4_096:
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
                    and pending.get("side") == "buy"
                    and isinstance(entry_id, str)
                    and 0 < len(entry_id) <= 64
                    and entry_id == entry_id.strip()
                    and row.get("entry_id") == entry_id
                    and isinstance(symbol, str)
                    and symbol == f"{str(state_symbol).strip().upper()}/USDT"
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
            filled_epoch = 0.0
            if valid:
                try:
                    parsed = datetime.strptime(
                        pending["filled_at"], "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    if parsed.strftime("%Y-%m-%d %H:%M:%S") != pending["filled_at"]:
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
        if bot_name not in {"SPOT", "TREND"}:
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
            if health.get("ok") is True:
                health.update({
                    "ok": False,
                    "state": "degraded",
                    "reason": "state_health_query_failed",
                })
        else:
            health = dict(health)
            if (
                pending_health["invalid_pending_state_count"]
            ):
                health.update({
                    "runtime_ok": False,
                    "runtime_state": "degraded",
                    "runtime_reason": "state_pending_invalid",
                    "data_quality_ok": False,
                })
                if health.get("ok") is True:
                    health.update({
                        "ok": False,
                        "state": "degraded",
                        "reason": "state_pending_invalid",
                    })
            elif (
                pending_health["pending_state_overdue_count"]
            ):
                health.update({
                    "runtime_ok": False,
                    "runtime_state": "degraded",
                    "runtime_reason": "state_capture_pending_overdue",
                    "data_quality_ok": False,
                })
                if health.get("ok") is True:
                    health.update({
                        "ok": False,
                        "state": "degraded",
                        "reason": "state_capture_pending_overdue",
                    })
        health.update(pending_health)
        return health

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
            markout_health = self._markout_runtime_health()
            evidence_health = self._sim_evidence_runtime_health(state_rows)
            integrity_reader = getattr(
                self, "_position_integrity_runtime_health", None
            )
            position_integrity_health = (
                integrity_reader() if callable(integrity_reader) else {}
            )
            status_writer(
                self.LOG_DIR,
                self.BOT_NAME,
                (
                    "ready"
                    if (
                        all(threads.values())
                        and markout_health.get("ok") is True
                        and (
                            not evidence_health
                            or evidence_health.get(
                                "runtime_ok", evidence_health.get("ok")
                            ) is True
                        )
                        and (
                            not position_integrity_health
                            or position_integrity_health.get(
                                "runtime_ok",
                                position_integrity_health.get("ok"),
                            ) is True
                        )
                    )
                    else "degraded"
                ),
                self.simulation,
                threads=threads,
                extra={
                    "open_positions": self.state.count(),
                    "safe_mode": bool(self.safe_mode.is_active()),
                    "markout_health": markout_health,
                    "sim_evidence_health": evidence_health,
                    **(
                        {
                            "position_integrity_health": (
                                position_integrity_health
                            )
                        }
                        if position_integrity_health else {}
                    ),
                    **observability,
                },
            )
        except Exception as exc:
            phase = "heartbeat" if log_snapshot else "periodic"
            silent_log(f"{self.BOT_NAME} {phase} runtime status", exc)

    #  Run 

    def run(self) -> None:
        """Entry point  replaces the old run_bot() function."""
        from core.logger import (log_event, log_separator, log_struct,
                                  set_structured_log_dir, load_j)
        from core.database import init_db, set_metrics_sim_mode
        from core.runtime_status import get_build_info, write_runtime_status
        from trading.risk_manager import validate_config_or_die

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

        # fail fast on bad config
        try:
            validate_config_or_die(self.BOT_NAME)
        except BaseException as e:
            write_runtime_status(
                self.LOG_DIR, self.BOT_NAME, "failed", self.simulation,
                extra={"phase": "config_validate", "error": str(e)[:300]})
            raise

        log_separator("", color=self.BOT_COLOR)
        log_event(
            f"{self.BOT_NAME}-Bot v2 started (multi-thread architecture)",
            "START"
        )
        log_event(
            f"Build: {build_info.get('build_id', 'unknown')} "
            f"({build_info.get('source', 'fallback')})",
            "START"
        )
        log_struct("bot_started",
                    bot=self.BOT_NAME, simulation=self.simulation,
                    build_id=build_info.get("build_id", "unknown"),
                    build_source=build_info.get("source", "fallback"),
                    max_trades=self.C("MAX_OPEN_TRADES"),
                    scan=self.C("SCAN_INTERVAL"),
                    monitor=self.C("MONITOR_INTERVAL", self.DEFAULT_MONITOR_INTERVAL),
                    spot_exit_shadow_enabled=self.C(
                        "SPOT_EXIT_SHADOW_ENABLED", False))
        log_event(
            f"Simulation: {self.simulation}  "
            f"Max trades: {self.C('MAX_OPEN_TRADES')}",
            "START"
        )
        # Momentum-strategy banner  defensive: a subclass (e.g. the trend
        # bot) may not define these keys, so never let the startup banner crash
        # the bot over a cosmetic log line.
        try:
            log_event(
                f"Pump: {self.C('MIN_PUMP')}%  "
                f"Stop: {self.C('INITIAL_STOP_LOSS')}%  "
                f"TP-{int(self.C('PARTIAL_SELL_PCT') * 100)}%: +{self.C('ACTIVATION_PROFIT')}%  "
                f"Trailing: {self.C('TRAILING_DISTANCE')}%",
                "START"
            )
        except Exception:
            pass
        log_event(
            f"Scan: {self.C('SCAN_INTERVAL')}s  "
            f"Monitor: {self.C('MONITOR_INTERVAL', self.DEFAULT_MONITOR_INTERVAL)}s",
            "START"
        )
        log_separator("", color=self.BOT_COLOR)

        #  Validate news module 
        # cached_property would lazy-load on first scan, but an import-time
        # error there would silently kill the Scan-Thread. Force the import NOW
        # so a typo or missing dep fails LOUD before threads start. USES_LLM=False
        # bots (Trend) are purely mechanical  skip entirely.
        if (self.USES_LLM
                and self._bool_cfg_value(self.C("USE_LLM", False), False)):
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

        #  Connect to exchange 
        if not self._connect_with_retry():
            write_runtime_status(
                self.LOG_DIR, self.BOT_NAME, "failed", self.simulation,
                extra={"phase": "exchange_connect"})
            sys.exit(1)

        #  Init SafeMode (daily-loss killswitch) 
        # Same safe-mode mechanism as futures: trigger blocks new entries while
        # existing positions still close normally (drives the UI MAX_DAILY_LOSS).
        try:
            from core.logger import send_telegram
            from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        except Exception:
            send_telegram = None
            TELEGRAM_TOKEN = TELEGRAM_CHAT_ID = None
        self.safe_mode = SafeMode(
            bot_name=self.BOT_NAME,
            telegram_send=send_telegram,
            telegram_token=TELEGRAM_TOKEN,
            telegram_chat_id=TELEGRAM_CHAT_ID,
            log_event=log_event,
            log_struct=log_struct,
            state_dir=self.LOG_DIR,   # persist alert state
        )

        #  LIVE-start alert 
        if not self.simulation and send_telegram:  # real money only  SIM stays silent
            try:
                send_telegram(
                    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    f" [{self.BOT_NAME}] LIVE gestartet  echtes Geld aktiv. "
                    f"Spot (unleveraged), "
                    f"Max {self.C('MAX_OPEN_TRADES')} trades, "
                    f"Daily-Loss {self.C('MAX_DAILY_LOSS', -50.0)} USDT.",
                )
            except Exception as e:
                self._log_error("live-start telegram alert", e)

        #  Load and validate state 
        trades_raw = load_j(self.DB_FILE, preserve_corrupt=True)
        from trading.cooldown_utils import load_cooldown_state
        self.cool = load_cooldown_state(self.COOLDOWN_FILE)
        if not self.cool.source_valid:
            log_event(
                f"Cooldown reload failed closed; new entries remain blocked: "
                f"{self.cool.source_error}",
                "ERROR",
            )

        # After restart, purge only expired cooldowns. Malformed evidence is
        # retained and keeps entries blocked until an operator repairs it.
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

        # Claims registry is REAL-account-only  SIM bots must not claim coins.
        self.state = TradeState(self.DB_FILE, trades_raw, is_futures=False,
                                bot_name=(self.BOT_NAME if not self.simulation else None))
        if self.state.init_rejected:
            r = self.state.init_rejected
            log_event(
                f" State load: dropped {len(r)} invalid trade(s): "
                f"{', '.join(r[:10])}"
                + (f" (+{len(r)-10} more)" if len(r) > 10 else ""),
                "WARN"
            )

        if self.simulation:
            self._recover_simulated_entry_tca_pending()

        #  Startup reconciliation 
        if not self.simulation:
            self._startup_reconciliation()

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
        from trading.execution_quality import run_tca_markout_worker

        self._markout_thread = threading.Thread(
            target=run_tca_markout_worker,
            args=(self.ex, self._shutdown_event),
            kwargs={
                "poll_interval_seconds": self.MARKOUT_POLL_INTERVAL_SEC,
                "limit": 25,
                "max_overdue_seconds": self.MARKOUT_MAX_OVERDUE_SEC,
                "health_callback": self._record_markout_worker_health,
                "worker_family": "spot",
            },
            daemon=True,
            name=f"{self.BOT_NAME}Markouts",
        )
        self._markout_started_monotonic = time.monotonic()
        start_threads_or_shutdown(
            (
                self._monitor_thread,
                self._scan_thread,
                self._reconcile_thread,
                self._markout_thread,
            ),
            self._shutdown_event,
        )

        log_event(
            "Four threads running (monitor, scan, reconcile, markout).",
            "START",
        )
        threads = self._runtime_threads()
        markout_health = self._markout_runtime_health()
        evidence_health = self._sim_evidence_runtime_health()
        position_integrity_health = self._position_integrity_runtime_health()
        write_runtime_status(
            self.LOG_DIR, self.BOT_NAME,
            (
                "ready"
                if (
                    all(threads.values())
                    and markout_health.get("ok") is True
                    and (
                        not evidence_health
                        or evidence_health.get(
                            "runtime_ok", evidence_health.get("ok")
                        ) is True
                    )
                    and (
                        not position_integrity_health
                        or position_integrity_health.get(
                            "runtime_ok", position_integrity_health.get("ok")
                        ) is True
                    )
                )
                else "degraded"
            ),
            self.simulation,
            threads=threads,
            extra={
                "open_positions": self.state.count(),
                "markout_health": markout_health,
                "sim_evidence_health": evidence_health,
                **(
                    {"position_integrity_health": position_integrity_health}
                    if position_integrity_health else {}
                ),
            })

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
                        tc = self.state.count()
                        thread_liveness = format_runtime_thread_liveness(
                            self._runtime_threads()
                        )
                        log_event(
                            f" {self.BOT_NAME} heartbeat  "
                            f"Open: {tc}/{self.C('MAX_OPEN_TRADES')}  "
                            f"Threads: {thread_liveness}",
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
                    # Self-throttling hourly status; never lifecycle-critical.
                    try:
                        from bot_utils.status_report import maybe_send_hourly_status
                        self._last_hourly_status = maybe_send_hourly_status(
                            bot_name=self.BOT_NAME, is_futures=False,
                            simulation=self.simulation, state=self.state,
                            last_sent=self._last_hourly_status,
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

        log_event("Waiting for threads to finish...", "INFO")
        # All four threads are daemon=True, so Python reaps them at interpreter
        # exit anyway. This join is best-effort (gives them a chance to write a
        # last log line); 2s is plenty since the loops check _shutdown_event
        # every iteration.
        for t in (
            self._monitor_thread,
            self._scan_thread,
            self._reconcile_thread,
            self._markout_thread,
        ):
            if t and t.is_alive():
                t.join(timeout=2)
        finalize_runtime_shutdown(
            self,
            write_runtime_status,
            log_event,
            resource_closers=shared_runtime_resource_closers(),
        )

    #  Connection 

    def _connect_with_retry(self) -> bool:
        """Connect to exchange with 3-attempt retry + backoff.

        Wraps the raw CCXT exchange in a ``ThreadLocalExchange`` so the
        scan / monitor / reconcile threads each get their own clone  the shared
        instance is not thread-safe and caused sporadic ``invalid signature``
        errors under load.
        """
        from config.exchange_config import is_authentication_error
        from core.logger import log_event
        try:
            raw_ex = self.EXCHANGE_FACTORY()
            # HTTP timeout  prevent hanging socket
            raw_ex.timeout = 10_000

            for attempt in range(1, 4):
                try:
                    try:
                        markets_allowed = bool(try_consume_api_call(
                            "spot_startup_load_markets",
                            critical=True,
                        ))
                    except Exception as budget_exc:
                        raise RuntimeError(
                            "spot load_markets API budget gate unavailable"
                        ) from budget_exc
                    if not markets_allowed:
                        raise RuntimeError(
                            "spot load_markets API budget exhausted"
                        )
                    raw_ex.load_markets()
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
            # Auth smoke-test BEFORE wrapping in ThreadLocalExchange  an
            # expired key or wrong passphrase would otherwise only surface on the
            # first scan-thread fetch, after the other threads are running.
            try:
                auth_probe_allowed = bool(try_consume_api_call(
                    "spot_startup_auth_fetch_balance"
                ))
            except Exception as budget_exc:
                auth_probe_allowed = False
                log_event(
                    f"Spot auth smoke-test skipped - API budget gate "
                    f"unavailable ({type(budget_exc).__name__})",
                    "WARN",
                )
            if not auth_probe_allowed and not bool(
                getattr(self, "simulation", True)
            ):
                log_event(
                    "Spot LIVE startup blocked - authentication could not be "
                    "verified within the API budget.",
                    "WARN",
                )
                return False
            if auth_probe_allowed:
                try:
                    _bal = raw_ex.fetch_balance()
                    _ = (_bal or {}).get("USDT", {})
                except Exception as se:
                    if is_authentication_error(se):
                        log_event(
                            f"Spot auth smoke-test FAILED ({type(se).__name__}: "
                            f"{str(se)[:120]})  check API credentials.",
                            "WARN")
                        return False
                    log_event(
                        f"Spot auth smoke-test transient error "
                        f"(non-auth: {type(se).__name__})  continuing",
                        "INFO")

            # Thread-local wrapper: from here on, every self.ex.fetch_*/create_*
            # call routes to a per-thread CCXT clone with isolated session+state.
            try:
                from bot_utils.thread_exchange import ThreadLocalExchange
                self.ex = ThreadLocalExchange(raw_ex)
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
                try:
                    raw_ex.close()
                except Exception as close_err:
                    self._log_error("exchange cleanup after wrapper failure", close_err)
                return False
            _exname = getattr(self.ex, "name", None) or "Exchange"
            log_event(f"{_exname} API connection established", "INFO")
            return True
        except Exception as e:
            log_event(f"Connection failed after 3 attempts: {e}", "WARN")
            self._log_error("Connection", e)
            return False

    #  Startup reconciliation 

    def _startup_reconciliation(self) -> None:
        """Compare local state vs exchange wallet on boot. Implementation
        in core/spot_bot_reconcile.py (separate module to keep this file
        focused on the main flow)."""
        from core.spot_bot_reconcile import startup_reconciliation
        startup_reconciliation(self)

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
        with self._shutdown_lock:
            if getattr(self, "_emergency_in_progress", False):
                log_event(
                    "Preserve-position shutdown refused while emergency close "
                    "is already in progress",
                    "WARN",
                )
                return False
            self._shutdown_positions_preserved = True
            self._shutdown_event.set()
        log_event(
            "Launcher preserve-position shutdown received; positions remain "
            "open while runtime resources close cleanly",
            "INFO",
        )
        return True

    def _shutdown_handler(self, signum=None, frame=None):
        """Signal handler  sets shutdown event and triggers emergency close.

        The close runs in a side-thread with a SHUTDOWN_DEADLINE_SEC deadline so
        a hanging exchange API can't block the process forever. The
        `_emergency_closed` flag prevents it running twice (SIGINT + atexit).

        Fast path: with zero open positions there's nothing to close, so we just
        set the shutdown_event and return immediately rather than spinning up the
        full emergency-close machinery.
        """
        from core.logger import log_event
        with self._shutdown_lock:
            if getattr(self, "_shutdown_positions_preserved", False):
                return
            if getattr(self, "_emergency_closed", False):
                return
            self._shutdown_event.set()
            if getattr(self, "_emergency_in_progress", False):
                return
            self._emergency_in_progress = True

        # FAST-PATH: nothing to close  no emergency close
        try:
            open_count = self.state.count()
        except Exception:
            open_count = -1  # unknown  fall through to safe path

        if open_count == 0:
            log_event(
                f" Shutdown signal {signum if signum else 'atexit'}  "
                f"no open positions, clean exit",
                "INFO"
            )
            self._emergency_closed = True   # prevent atexit re-entry
            self._emergency_in_progress = False
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

        # The worker owns the in-progress latch and clears it in ``finally``.
        # Never write a sampled ``True`` back here: the worker can terminate
        # between is_alive() returning and the assignment, which would relatch
        # an already-finished partial close and block every later retry.
        runner_alive = runner.is_alive()
        if result["done"] and result["failed_count"] == 0:
            self._emergency_closed = True
            self._emergency_in_progress = False
        else:
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
                    f"position(s) failed. A repeat shutdown signal will retry "
                    f"them; close MANUALLY if exiting now.",
                    "WARN"
                )

    def _emergency_close_all(self, reason: str = "Shutdown") -> None:
        from core.logger import log_event, log_sell, send_telegram, save_trade
        from core.database import save_trade_db
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from core.symbol_locks import close_lock, release_lock
        return emergency_close_all_spot(
            ex=self.ex,
            state=self.state,
            bot_name=self.BOT_NAME,
            log_dir=self.LOG_DIR,
            simulation=self.simulation,
            reason=reason,
            telegram_token=TELEGRAM_TOKEN,
            telegram_chat_id=TELEGRAM_CHAT_ID,
            log_event=log_event,
            log_sell=log_sell,
            save_trade_db=save_trade_db,
            save_trade=save_trade,
            send_telegram=send_telegram,
            error_logger=self._log_error,
            close_lock_factory=close_lock,
            release_lock=release_lock,
        )

    #  Abstract: must be set by subclass 

    @staticmethod
    @abstractmethod
    def EXCHANGE_FACTORY():
        """Return a connected ccxt exchange instance."""
