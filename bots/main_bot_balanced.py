"""
bots/main_bot_balanced.py  -  TREND spot trading bot (slim subclass).

Strategy parameters from optimizer-validated 60-day run:
  - avg_net 60d = +6.05 USDT (+4.31% ROI)
  - Consistency  = 40% over 4 folds
  - Win-Rate     = 44.4%
  - Max DD       =  1.3%
  - Sharpe       =  0.20
"""
from __future__ import annotations

try:
    from bots._bootstrap import prepare_entrypoint, require_portalocker
except ModuleNotFoundError:
    from _bootstrap import prepare_entrypoint, require_portalocker

prepare_entrypoint(__file__, __name__)

require_portalocker()

from core.trend_bot import TrendBot
from core.paths import LOG_DIR_TREND
from config.exchange_config import get_spot_exchange_connection
from bot_utils.sim_flag import read_simulation_flag


class BalancedBot(TrendBot):
    """The "Trend" bot (validated majors trend-following). Internal key stays
    TREND so DB / state / config / history remain intact; the UI label and the
    strategy differ from the legacy "Balanced" name. SPOT, no leverage."""
    BOT_NAME = "TREND"
    BOT_COLOR = "\033[96m"
    LOG_DIR = str(LOG_DIR_TREND)
    DB_FILE = f"{LOG_DIR}/trades.json"
    COOLDOWN_FILE = f"{LOG_DIR}/cooldown.json"
    BUY_PREFIX = "trend"
    BACKTEST_NOTE = (
        "Trend-following on majors | 720d: +37% (ensemble) vs B&H -0% | "
        "DD ~38% vs 64% | spot, no leverage"
    )

    DEFAULTS = {
        # Sizing
        "POSITION_SIZE":     20.0,   # USDT per coin when in-trend
        "MAX_OPEN_TRADES":   12,
        # Trend universe + signal
        "TREND_UNIVERSE":    "BTC,ETH,BNB,XRP,SOL,ADA,AVAX,LINK,DOT,LTC,DOGE,TRX",
        "TREND_SMA_FAST":    50,
        "TREND_SMA_SLOW":    100,
        "TREND_CROSS_FAST":  50,
        "TREND_CROSS_SLOW":  150,
        "TREND_VOTE_MIN":    2,      # of 3 rules -> in trend
        "TREND_EXIT_VOTE":   2,      # set to 1 for hysteresis (less whipsaw)
        "TREND_CHECK_HOURS": 12,     # re-evaluate twice a day
        # Risk
        "INITIAL_STOP_LOSS": -30.0,  # disaster stop (gap protection); trend
                                     # exit normally fires long before this
        "MAX_DAILY_LOSS":    -50.0,
        "LEARNING_DISABLED": True,   # trend sizes from POSITION_SIZE directly +
                                     # fixed basket -> skip Kelly/RSI/blacklist
        "TREND_VOL_TARGET":          0,
        "TREND_VOL_TARGET_LOOKBACK": 30,
        "SIMULATION":        True,
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_spot_exchange_connection()


def run_bot():
    """Entry point  -  kept for backward compat with launcher scripts."""
    BalancedBot(simulation=read_simulation_flag("TREND")).run()


if __name__ == "__main__":
    run_bot()
