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
    from bots._bootstrap import guard_pre_start, prepare_entrypoint, require_portalocker
except ModuleNotFoundError:
    from _bootstrap import guard_pre_start, prepare_entrypoint, require_portalocker

prepare_entrypoint(__file__, __name__)

# startup dependency smoke-test  -  failing here is much clearer than failing
require_portalocker()

from core import SpotBot  # noqa: E402
from core.paths import LOG_DIR_SPOT  # noqa: E402
from config.exchange_config import get_spot_exchange_connection  # noqa: E402
from bot_utils.sim_flag import read_simulation_flag  # noqa: E402


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
        "MIN_PUMP":          5.0,
        "ACTIVATION_PROFIT": 4.0,
        "TRAILING_DISTANCE": 1.5,
        "POST_PARTIAL_TRAILING_DISTANCE": 1.0,
        "BREAKEVEN_TRIGGER": 0.0,
        "INITIAL_STOP_LOSS": -10.0,
        "PARTIAL_SELL_PCT":  0.60,
        "POSITION_SIZE":     10.0,
        "POSITION_SIZE_MAX": 25.0,
        "MAX_OPEN_TRADES":   3,    # konservativer Starter (hohe Varianz)
        "SCAN_INTERVAL":     150,
        "MONITOR_INTERVAL":  20,
        "USE_TREND_FILTER":  0,
        "USE_LLM":           False,
        "COOLDOWN_AFTER_SL": 60,
        "MAX_DAILY_LOSS":    -50.0,
        "OWN_MOMENTUM_FILTER": True,
        "OWN_MOMENTUM_WINDOW": 8,
        "OWN_MOMENTUM_MIN_LOSS_PCT": 20.0,
        "SIMULATION":        True,
        "RSI_MAX":           85.0,
        "ENTRY_QUALITY_FILTER_ENABLED": True,
        "ENTRY_QUALITY_MIN_SCORE": 75.0,
        "SPOT_EXIT_SHADOW_ENABLED": False,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 360,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_spot_exchange_connection()


def run_bot():
    """Entry point  -  kept for backward compat with launcher scripts."""
    guard_pre_start("SPOT")
    AggressiveBot(simulation=read_simulation_flag("SPOT")).run()


if __name__ == "__main__":
    run_bot()
