"""
bots/main_bot_futures.py  -  FUTURES trading bot (slim subclass).

All the heavy lifting lives in core/futures_bot.py (dual-thread
architecture: monitor + scan + reconcile + emergency-close + safe-mode +
ticker cache + API budget guard). This file only specifies:
  - Bot identity (name, color, log dir, clientOrderId prefix)
  - Default config values
  - Which news_brain module to use
  - Which exchange to connect to (futures-mode)

Strategy parameters from the optimizer's validated futures run:
  - ACTIVATION_PROFIT 4.5%   (partial-TP fires earlier than spot)
  - INITIAL_STOP_LOSS -3.5%  (tighter than spot  -  leverage amplifies)
  - LEVERAGE 3x              (max recommended on USDT-M perpetuals)
  - LIQ_SAFETY_PCT 25%       (close when 75% of liq buffer is consumed)
  - MAX_DAILY_LOSS -10 USDT  (daily kill-switch -> SAFE_MODE)
"""
from __future__ import annotations

try:
    from bots._bootstrap import prepare_entrypoint, require_portalocker
except ModuleNotFoundError:
    from _bootstrap import prepare_entrypoint, require_portalocker

prepare_entrypoint(__file__, __name__)

require_portalocker()

from core import FuturesBot
from core.paths import LOG_DIR_FUTURES
from config.exchange_config import get_futures_exchange_connection
from bot_utils.sim_flag import read_simulation_flag


class FuturesExchangeBot(FuturesBot):
    BOT_NAME = "FUTURES"
    BOT_COLOR = "\033[95m"
    LOG_DIR = str(LOG_DIR_FUTURES)
    DB_FILE = f"{LOG_DIR}/trades.json"
    COOLDOWN_FILE = f"{LOG_DIR}/cooldown.json"
    NEWS_MODULE_PATH = "news.news_brain_futures"
    BUY_PREFIX = "fut"
    BACKTEST_NOTE = (
        "Futures: LONG/SHORT at 3x leverage. "
        "Strict daily-loss killswitch + safe-mode on slippage anomalies."
    )

    DEFAULTS = {
        "MIN_PUMP":          2.0,
        "ACTIVATION_PROFIT": 4.5,
        "TRAILING_DISTANCE": 2.5,
        "INITIAL_STOP_LOSS": -3.5,
        "PARTIAL_SELL_PCT":  0.40,
        "POSITION_SIZE":     10.0,
        "POSITION_SIZE_MAX": 25.0,
        "MAX_OPEN_TRADES":   3,
        "LEVERAGE":          3,
        "LIQ_SAFETY_PCT":    25.0,
        "SCAN_INTERVAL":     150,
        "MONITOR_INTERVAL":  20,
        "BREAKEVEN_TRIGGER": 2.0,
        "USE_TREND_FILTER":  0,
        "COOLDOWN_AFTER_SL": 120,
        "MAX_DAILY_LOSS":    -10.0,  # konservativer Starter (Hebel verstrkt)
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_futures_exchange_connection()


def run_bot():
    """Entry point  -  kept for backward compat with launcher scripts."""
    FuturesExchangeBot(simulation=read_simulation_flag("FUTURES")).run()


if __name__ == "__main__":
    run_bot()
