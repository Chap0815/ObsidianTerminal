"""
bots/main_bot_aggressive.py  -  SPOT spot trading bot (slim subclass).

Strategy parameters from optimizer-validated 60-day run:
  - avg_net 60d = +1.90 USDT (+2.05% ROI)
  - Win-Rate    = 44.8%
  - Max DD      =  1.6%
  - Sharpe      =  0.077

 SPOT has profitable trades across all 4 folds but with high
   variance (Konsistenz ~0). Live performance can diverge from backtest  - 
   only run in SIMULATION until >=20 real trades stabilize behavior.
"""
from __future__ import annotations

try:
    from bots._bootstrap import prepare_entrypoint, require_portalocker
except ModuleNotFoundError:
    from _bootstrap import prepare_entrypoint, require_portalocker

prepare_entrypoint(__file__, __name__)

# startup dependency smoke-test  -  failing here is much clearer than failing
require_portalocker()

from core import SpotBot
from core.paths import LOG_DIR_SPOT
from config.exchange_config import get_spot_exchange_connection
from bot_utils.sim_flag import read_simulation_flag


class AggressiveBot(SpotBot):
    BOT_NAME = "SPOT"
    BOT_COLOR = "\033[93m"
    LOG_DIR = str(LOG_DIR_SPOT)
    DB_FILE = f"{LOG_DIR}/trades.json"
    COOLDOWN_FILE = f"{LOG_DIR}/cooldown.json"
    NEWS_MODULE_PATH = "news.news_brain_spot"
    BUY_PREFIX = "agg"
    BACKTEST_NOTE = (
        "Backtest expectation: ~+2% ROI in 60 days | "
        "Max DD ~1.6% | WR ~45% (HIGH variance  -  verify in SIMULATION)"
    )

    DEFAULTS = {
        "MIN_PUMP":          6.0,
        "ACTIVATION_PROFIT": 9.0,
        "TRAILING_DISTANCE": 3.0,
        "BREAKEVEN_TRIGGER": 2.5,
        "INITIAL_STOP_LOSS": -6.0,
        "PARTIAL_SELL_PCT":  0.60,
        "POSITION_SIZE":     10.0,
        "POSITION_SIZE_MAX": 25.0,
        "MAX_OPEN_TRADES":   3,    # konservativer Starter (hohe Varianz)
        "SCAN_INTERVAL":     150,
        "MONITOR_INTERVAL":  20,
        "COOLDOWN_AFTER_SL": 60,
        "MAX_DAILY_LOSS":    -15.0,  # konservativer Starter
        "RSI_MAX":           65.0,
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_spot_exchange_connection()


def run_bot():
    """Entry point  -  kept for backward compat with launcher scripts."""
    AggressiveBot(simulation=read_simulation_flag("SPOT")).run()


if __name__ == "__main__":
    run_bot()
