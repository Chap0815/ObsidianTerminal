"""
base_bot.py  Shared Trading Engine Foundation

STATUS NOTE (Phase 2 review):
    None of main_bot_balanced / _aggressive / _futures currently inherits
    from BaseBot. They are independent 1500-4000-line modules with ~80%
    duplicated logic. Migrating all three onto BaseBot would eliminate
    the duplication and remove an entire category of "bug-fixed once but
    only in two of three bots" defects. Until that migration happens,
    this module is kept as a reference for the target architecture  DO
    NOT delete it.

    The Phase-2 patches applied directly to the main bots (UTC cooldown,
    clientOrderId idempotency, candidate correlation check, kill-switch
    invocation via the updated is_bot_paused) bring all three bots in
    line with the safety guarantees this class would provide.

Extracts the common logic from main_bot_balanced.py, main_bot_aggressive.py,
and main_bot_futures.py into a single reusable base class.

What lives here (shared across all bots):
  Startup exchange reconciliation
  Risk check pipeline (paused? bad hour? can_buy_now? max trades?)
  Order placement with retry and slippage recording
  Stop-loss / trailing-stop evaluation (shared math)
  Cooldown management
  SQLite state via StateManager
  Event bus integration
  Shutdown handler

What subclasses override:
  get_signal(symbol, row)  Signal
  execute_buy(signal)  Position | None
  execute_sell(pos, reason) bool
  calculate_stop_price(pos) float
  main_loop_body()  runs once per scan cycle

Usage:
    from trading.base_bot import BaseBot
    from core.models import PositionType

    class BalancedBot(BaseBot):
        BOT_NAME     = "TREND"
        POSITION_TYPE = PositionType.SPOT

        def get_signal(self, symbol, row):
            ...
        def execute_buy(self, signal):
            ...

    bot = BalancedBot(config_path="bot_config.json")
    bot.run()
"""

from __future__ import annotations

import os
import json
import time
import threading
import signal
import atexit
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from core.models import Position, Signal, PositionType
from core.state_manager import StateManager
from core.event_bus import get_bus, register_console_logger, register_structured_logger
from bot_utils.safe_numeric import safe_positive_float


#  Helpers 

def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _utcnow_str() -> str:
    return _utcnow().strftime("%Y-%m-%d %H:%M:%S")


# 
#  BaseBot 
# 

class BaseBot(ABC):
    """
    Abstract base class for all trading bots.

    Concrete subclasses must set class attributes:
        BOT_NAME      str           "TREND" / "SPOT" / "FUTURES"
        POSITION_TYPE PositionType  SPOT / LONG / SHORT

    And implement abstract methods:
        get_signal()  screener + LLM  Signal
        execute_buy()  place buy order  Position | None
        execute_sell()  place sell order  bool
        calculate_stop_price()  float
        main_loop_body()  one full scan cycle
    """

    #  Subclass must set these 
    BOT_NAME:      str          = "BASE"
    POSITION_TYPE: PositionType = PositionType.SPOT

    #  Defaults (overridable via bot_config.json) 
    DEFAULT_SCAN_INTERVAL_SEC  = 20
    DEFAULT_MAX_OPEN_TRADES    = 3
    DEFAULT_POSITION_USDT      = 10.0
    DEFAULT_STOP_LOSS_PCT      = -3.0

    def __init__(self, config_path: str = "bot_config.json"):
        self._config_path   = config_path
        self._config        = self._load_config()
        self._exchange      = None   # set in _connect()
        self._state_manager: Optional[StateManager] = None
        self._ws_feed       = None
        self._bus           = get_bus()
        self._shutdown_event = threading.Event()
        self._shutdown_lock  = threading.Lock()
        self._trades_lock    = threading.Lock()

        # Register console + structured loggers once per process
        register_console_logger(self._bus)
        register_structured_logger(self._bus)

        # Shutdown handlers
        atexit.register(self._handle_shutdown)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._signal_handler)
            except (OSError, ValueError):
                pass  # Windows / non-main thread

    #  Config 

    def _load_config(self) -> dict:
        try:
            if os.path.exists(self._config_path):
                with open(self._config_path, encoding="utf-8-sig") as f:
                    full = json.load(f)
                    return full.get(self.BOT_NAME, {})
        except Exception as e:
            try:
                from core.logger import log_event
                log_event(f"[{self.BOT_NAME}] Config load error: {e}", "WARN")
            except Exception:
                print(f"[{self.BOT_NAME}] Config load error: {e}")
        return {}

    def cfg(self, key: str, default=None):
        return self._config.get(key, default)

    @property
    def max_open_trades(self) -> int:
        return int(self.cfg("MAX_OPEN_TRADES", self.DEFAULT_MAX_OPEN_TRADES))

    @property
    def scan_interval(self) -> int:
        return int(self.cfg("SCAN_INTERVAL", self.DEFAULT_SCAN_INTERVAL_SEC))

    @property
    def log_dir(self) -> str:
        name = self.BOT_NAME.lower()
        d = os.path.join("logs", name)
        os.makedirs(d, exist_ok=True)
        return d

    #  Exchange connection 

    def _connect(self):
        """Connect to the exchange. Retries 3 with backoff."""
        from config.exchange_config import (get_exchange_connection,
                                      get_futures_exchange_connection)
        for attempt in range(3):
            try:
                if self.POSITION_TYPE == PositionType.SPOT:
                    self._exchange = get_exchange_connection()
                else:
                    self._exchange = get_futures_exchange_connection()
                self._exchange.load_markets()
                self._bus.emit("BOT_STARTED", {
                    "bot_name": self.BOT_NAME,
                    "position_type": self.POSITION_TYPE.value,
                })
                return
            except Exception as e:
                if attempt < 2:
                    wait = 2 ** attempt * 5
                    try:
                        from core.logger import log_event
                        log_event(f"[{self.BOT_NAME}] Connection attempt "
                                  f"{attempt+1} failed: {e}  retry in "
                                  f"{wait}s", "WARN")
                    except Exception:
                        print(f"[{self.BOT_NAME}] Connection attempt "
                              f"{attempt+1} failed: {e}  retry in {wait}s")
                    time.sleep(wait)
                else:
                    try:
                        from core.logger import log_event
                        log_event(f"[{self.BOT_NAME}] Fatal: could not "
                                  f"connect after 3 attempts", "ERROR")
                    except Exception:
                        print(f"[{self.BOT_NAME}] Fatal: could not connect "
                              f"after 3 attempts")
                    raise RuntimeError(f"[{self.BOT_NAME}] Fatal: could not connect to exchange after 3 attempts")

    #  State management 

    def _init_state(self) -> Dict[str, Position]:
        """Initialize StateManager and load open positions."""
        json_path = os.path.join(self.log_dir, "trades.json")
        self._state_manager = StateManager(
            bot_name   = self.BOT_NAME,
            log_dir    = self.log_dir,
            json_path  = json_path,
            write_json = True,
        )
        return self._state_manager.load()

    #  Risk checks (shared) 

    def _check_risk_pipeline(self,
                              positions: Dict[str, Position],
                              candidate_symbol: str = ""
                              ) -> Tuple[bool, str]:
        """
        Run all shared pre-trade risk checks.
        Returns (allowed, reason).
        """
        from trading.risk_manager import is_bot_paused, is_bad_hour
        from trading.market_filters import can_buy_now

        paused, reason = is_bot_paused(
            self.BOT_NAME, simulation=self.simulation)
        if paused:
            return False, f"Bot paused: {reason}"

        if len(positions) >= self.max_open_trades:
            return False, f"Max open trades reached ({self.max_open_trades})"

        if is_bad_hour(self.BOT_NAME):
            # is_bad_hour() entscheidet in LOKALER Zeit (BOT_TIMEZONE)  die
            # Meldung muss dieselbe Zeitachse zeigen, nicht UTC, sonst wirkt der
            # angezeigte Wert je nach Zeitzone um Stunden falsch".
            try:
                import os as _os
                from zoneinfo import ZoneInfo as _ZI
                _tz = _os.getenv("BOT_TIMEZONE", "UTC")
                _h = datetime.now(_ZI(_tz)).hour
                _label = f"{_h}:xx {_tz}"
            except Exception:
                _label = f"{_utcnow().hour}:xx UTC"
            return False, f"Bad hour ({_label})"

        open_syms = list(positions.keys())
        ok, reason = can_buy_now(
            self._exchange,
            bot_name=self.BOT_NAME,
            open_symbols=open_syms,
            candidate_symbol=candidate_symbol,
        )
        if not ok:
            return False, reason

        return True, "OK"

    def _check_position_size(self, atr_pct: float = 0.0) -> float:
        """Return the volatility + Kelly adjusted position size."""
        try:
            from trading.risk_manager import get_combined_position_size
            return get_combined_position_size(self.BOT_NAME, atr_pct)
        except Exception:
            return float(self.cfg("POSITION_SIZE", self.DEFAULT_POSITION_USDT))

    #  Stop-loss evaluation (shared math) 

    def evaluate_stops(self, pos: Position, current_price: float,
                       initial_sl_pct: float,
                       trailing_pct: float = 0.0) -> Optional[str]:
        """
        Evaluate stop-loss and trailing-stop for a position.

        Returns the trigger reason string if a stop should fire, else None.

        Stop types:
          Initial hard stop:  price < buy  (1 + sl_pct/100)
          Trailing stop:  price < highest  (1 - trail_pct/100)
            (activates only when profitable  highest > buy)

        This shared implementation ensures TREND and SPOT apply
        identical stop math.
        """
        if not current_price or current_price <= 0:
            return None

        # Update trailing high
        if current_price > pos.highest_price:
            pos.highest_price = current_price

        # Hard stop
        sl_price = pos.buy_price * (1.0 + initial_sl_pct / 100.0)
        if current_price <= sl_price:
            return "Stop-Loss"

        # Trailing stop (only when in profit)
        if trailing_pct > 0 and pos.highest_price > pos.buy_price:
            trail_price = pos.highest_price * (1.0 - trailing_pct / 100.0)
            if current_price <= trail_price:
                return "Trailing-Stop"

        return None

    #  Cooldown helpers 

    def _set_cooldown(self, cool: dict, symbol: str, minutes: int) -> None:
        """Set cooldown via unified UTC helper."""
        from trading.cooldown_utils import set_cooldown
        cooldown_path = os.path.join(self.log_dir, "cooldown.json")
        set_cooldown(cool, symbol, minutes, cooldown_path)

    def _is_in_cooldown(self, cool: dict, symbol: str) -> bool:
        """Check cooldown via unified UTC helper."""
        from trading.cooldown_utils import is_in_cooldown as _check
        cooldown_path = os.path.join(self.log_dir, "cooldown.json")
        return _check(cool, symbol, cooldown_path)

    #  Shutdown 

    def _signal_handler(self, signum, frame):
        with self._shutdown_lock:
            if not self._shutdown_event.is_set():
                self._shutdown_event.set()
                self._handle_shutdown(signum=signum)

    def _handle_shutdown(self, signum=None):
        """Graceful shutdown: flush state, emit event, stop WS feed."""
        print(f"\n[{self.BOT_NAME}] Shutting down...")
        if self._state_manager:
            self._state_manager.save_snapshot()
        if self._ws_feed:
            self._ws_feed.stop()
        self._bus.emit_sync("BOT_STOPPED", {
            "bot_name": self.BOT_NAME,
            "reason":   f"signal {signum}" if signum else "atexit",
        })
        self._bus.shutdown(timeout=3.0)

    #  WebSocket feed 

    def _init_ws_feed(self, symbols: List[str]):
        """Initialize and start the WebSocket feed for the given symbols."""
        from trading.ws_feed import WebSocketFeed
        self._ws_feed = WebSocketFeed(self._exchange, bot_name=self.BOT_NAME)
        self._ws_feed.start(symbols)
        print(f"[{self.BOT_NAME}] Price feed started (mode={self._ws_feed.mode})")

    def get_price(self, symbol: str, fallback: float = 0.0) -> float:
        """Get latest price: WebSocket cache first, REST fallback."""
        if self._ws_feed:
            price = self._ws_feed.get_price(symbol)
            if price > 0:
                return price
        # REST fallback
        try:
            ticker = self._exchange.fetch_ticker(symbol)
            price = safe_positive_float(ticker.get("last"), 0.0)
            if price > 0:
                return price
            price = safe_positive_float(ticker.get("close"), 0.0)
            return price if price > 0 else safe_positive_float(fallback, 0.0)
        except Exception:
            return safe_positive_float(fallback, 0.0)

    #  Abstract methods (subclasses must implement) 

    @abstractmethod
    def get_signal(self, symbol: str, screener_row: dict) -> Optional[Signal]:
        """
        Analyze a screener candidate and return a trading Signal.
        Return None to skip this symbol.
        """

    @abstractmethod
    def execute_buy(self, signal: Signal) -> Optional[Position]:
        """
        Place a buy/open order and return the resulting Position.
        Return None if the order failed or was skipped.
        """

    @abstractmethod
    def execute_sell(self, position: Position, reason: str) -> bool:
        """
        Place a sell/close order.
        Return True if fully closed, False on failure.
        """

    @abstractmethod
    def calculate_stop_price(self, position: Position) -> float:
        """Return the current stop-loss price for this position."""

    @abstractmethod
    def main_loop_body(self) -> None:
        """One complete scan + monitor cycle."""

    #  Main run loop 

    def run(self) -> None:
        """
        Start the bot.

        1. Connect to exchange
        2. Load state (StateManager with ghost position recovery)
        3. Start WebSocket feed
        4. Enter main loop
        """
        from core.logger import log_event
        log_event(f"[{self.BOT_NAME}] Starting...", "START")

        try:
            self._connect()
        except RuntimeError as e:
            print(str(e))
            return
        positions = self._init_state()

        log_event(
            f"[{self.BOT_NAME}] {len(positions)} open position(s) loaded",
            "INFO"
        )

        # Start WS feed for all currently tracked symbols
        if positions:
            try:
                self._init_ws_feed(list(positions.keys()))
            except Exception as e:
                log_event(f"[{self.BOT_NAME}] WS feed init failed: {e}  REST only", "WARN")

        self._bus.emit("BOT_STARTED", {
            "bot_name":     self.BOT_NAME,
            "open_trades":  len(positions),
        })

        try:
            while not self._shutdown_event.is_set():
                try:
                    self.main_loop_body()
                except Exception as e:
                    log_event(
                        f"[{self.BOT_NAME}] Main loop error: {type(e).__name__}: {e}",
                        "WARN"
                    )
                    self._bus.emit("BOT_ERROR", {
                        "bot_name": self.BOT_NAME,
                        "error":    str(e),
                    })
                self._shutdown_event.wait(timeout=self.scan_interval)
        finally:
            self._handle_shutdown()
