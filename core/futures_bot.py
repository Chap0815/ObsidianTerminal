"""
core/futures_bot.py  Dual-thread futures trading bot base class.

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
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Optional, Dict, Any

from bot_utils import (
    log_error as _ext_log_error,
    load_runtime_config,
    TradeState,
    TickerCache,
    SafeMode,
    emergency_close_all_futures,
)

from core.futures_bot_exits import FuturesExitsMixin
from core.futures_bot_scan import FuturesScanMixin
from core.futures_bot_reconcile import FuturesReconcileMixin


class FuturesBot(FuturesExitsMixin, FuturesScanMixin,
                  FuturesReconcileMixin, ABC):
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

    # Reconciliation cadence (constants  could be overridden if needed)
    RECONCILE_INTERVAL_SEC: int = 300   # orphan-adoption safety net cadence
    GC_LOCKS_INTERVAL_SEC: int = 300
    HEARTBEAT_INTERVAL_SEC: int = 60

    # Monitor / Scan defaults
    DEFAULT_MONITOR_INTERVAL: int = 20

    # Ticker cache pool sizing (bounded backpressure)
    TICKER_POOL_SIZE: int = 8
    TICKER_INFLIGHT_MAX: int = 32

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
        self._shutdown_lock = threading.Lock()
        self._cooldown_lock = threading.Lock()
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

    @cached_property
    def _news(self):
        return importlib.import_module(self.NEWS_MODULE_PATH)

    def _log_error(self, context: str, exc: Exception) -> None:
        _ext_log_error(self.BOT_NAME, context, exc)

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
            f"Scan: {self.C('SCAN_INTERVAL')}s  "
            f"Monitor: {self.C('MONITOR_INTERVAL', self.DEFAULT_MONITOR_INTERVAL)}s",
            "START"
        )
        log_separator("", color=self.BOT_COLOR)

        log_struct("bot_started",
                    bot=self.BOT_NAME, simulation=self.simulation,
                    build_id=build_info.get("build_id", "unknown"),
                    build_source=build_info.get("source", "fallback"),
                    leverage=self.C("LEVERAGE"),
                    max_trades=self.C("MAX_OPEN_TRADES"),
                    position_size=self.C("POSITION_SIZE"),
                    liq_safety_pct=self.C("LIQ_SAFETY_PCT"),
                    stop_loss=self.C("INITIAL_STOP_LOSS"),
                    activation_tp=self.C("ACTIVATION_PROFIT"),
                    trail=self.C("TRAILING_DISTANCE"),
                    breakeven_trigger=self.C("BREAKEVEN_TRIGGER"),
                    scan_interval=self.C("SCAN_INTERVAL"),
                    monitor_interval=self.C("MONITOR_INTERVAL",
                                             self.DEFAULT_MONITOR_INTERVAL))

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
        trades_raw = load_j(self.DB_FILE)
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
            self._startup_reconciliation()

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
        self._monitor_thread.start()
        self._scan_thread.start()
        self._reconcile_thread.start()

        log_event("Three threads running (monitor, scan, reconcile).", "START")
        threads = {
            "monitor": self._monitor_thread.is_alive(),
            "scan": self._scan_thread.is_alive(),
            "reconcile": self._reconcile_thread.is_alive(),
        }
        write_runtime_status(
            self.LOG_DIR, self.BOT_NAME,
            "ready" if all(threads.values()) else "degraded",
            self.simulation,
            threads=threads,
            extra={"open_positions": self.state.count()})

        #  Main thread: heartbeat + shutdown wait 
        try:
            last_heartbeat = 0.0
            last_runtime_status = 0.0
            self._last_hourly_status = 0.0
            while not self._shutdown_event.is_set():
                now = time.time()
                if now - last_heartbeat >= self.HEARTBEAT_INTERVAL_SEC:
                    tc = self.state.count()
                    sm_marker = "  SAFE_MODE" if self.safe_mode.is_active() else ""
                    log_event(
                        f" {self.BOT_NAME} heartbeat  "
                        f"Open: {tc}/{self.C('MAX_OPEN_TRADES')}  "
                        f"Monitor={'' if self._monitor_thread.is_alive() else ''}  "
                        f"Scan={'' if self._scan_thread.is_alive() else ''}"
                        f"{sm_marker}",
                        "INFO"
                    )
                    try:
                        from trading.runtime_observability import (
                            log_runtime_observability)
                        observability = log_runtime_observability(
                            bot_name=self.BOT_NAME,
                            mode="SIM" if self.simulation else "LIVE",
                            state_rows=self.state.get_all(),
                            ticker_cache=self.ticker_cache,
                        )
                        threads = {
                            "monitor": self._monitor_thread.is_alive(),
                            "scan": self._scan_thread.is_alive(),
                            "reconcile": self._reconcile_thread.is_alive(),
                        }
                        status = "ready" if all(threads.values()) else "degraded"
                        if (not threads["monitor"]
                                and self.safe_mode is not None
                                and not self.safe_mode.is_active()):
                            self.safe_mode.trigger(
                                "monitor thread stopped - exits not supervised")
                        write_runtime_status(
                            self.LOG_DIR, self.BOT_NAME, status,
                            self.simulation,
                            threads=threads,
                            extra={
                                "open_positions": tc,
                                "safe_mode": bool(self.safe_mode.is_active()),
                                **observability,
                            })
                    except Exception:
                        pass
                    last_heartbeat = now
                if now - last_runtime_status >= 5.0:
                    try:
                        from trading.runtime_observability import (
                            runtime_observability_snapshot)
                        observability = runtime_observability_snapshot(
                            state_rows=self.state.get_all(),
                            ticker_cache=self.ticker_cache,
                        )
                        threads = {
                            "monitor": self._monitor_thread.is_alive(),
                            "scan": self._scan_thread.is_alive(),
                            "reconcile": self._reconcile_thread.is_alive(),
                        }
                        status = "ready" if all(threads.values()) else "degraded"
                        if (not threads["monitor"]
                                and self.safe_mode is not None
                                and not self.safe_mode.is_active()):
                            self.safe_mode.trigger(
                                "monitor thread stopped - exits not supervised")
                        write_runtime_status(
                            self.LOG_DIR, self.BOT_NAME, status,
                            self.simulation, threads=threads,
                            extra={
                                "open_positions": self.state.count(),
                                "safe_mode": bool(self.safe_mode.is_active()),
                                **observability,
                            })
                    except Exception:
                        pass
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
        for t in (self._monitor_thread, self._scan_thread,
                   self._reconcile_thread):
            if t and t.is_alive():
                t.join(timeout=2)
        try:
            write_runtime_status(
                self.LOG_DIR, self.BOT_NAME, "stopped", self.simulation,
                threads={
                    "monitor": bool(self._monitor_thread and self._monitor_thread.is_alive()),
                    "scan": bool(self._scan_thread and self._scan_thread.is_alive()),
                    "reconcile": bool(self._reconcile_thread and self._reconcile_thread.is_alive()),
                })
        except Exception:
            pass
        # Close all per-thread CCXT clones so their HTTP sessions release
        # file descriptors.
        try:
            closer = getattr(self.ex, "close_all", None)
            if callable(closer):
                closer()
        except Exception:
            pass
        log_event("Bot shutdown complete.", "INFO")

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
        try:
            raw_ex = self.EXCHANGE_FACTORY()
            # HTTP timeout  higher at startup for slow load_markets
            raw_ex.timeout = 30_000
            for attempt in range(1, 4):
                try:
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
                else:
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
            if not self.C("SIMULATION", True):
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

        runner = threading.Thread(target=_close_runner,
                                    daemon=True,
                                    name=f"{self.BOT_NAME}EmergencyClose")
        runner.start()
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

        # Tear down ticker pool to kill non-daemon worker threads
        if self.ticker_cache:
            try:
                self.ticker_cache.shutdown()
            except Exception as e:
                self._log_error("Ticker pool shutdown", e)

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
