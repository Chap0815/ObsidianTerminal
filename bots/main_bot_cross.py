"""
bots/main_bot_cross.py  -  Cross-sectional momentum bot (slim subclass).

Market-neutral: long the strongest K / short the weakest K liquid perps by
lookback return, rebalanced every XSEC_REBALANCE_HOURS, cross-margin, with an
own-momentum crash filter. All logic in core/cross_bot.py + trading/xsec_signal.py.

Status: SIM-first (regime-dependent edge  -  keep paper until proven).
Start:  python -m bots.main_bot_cross
"""
from __future__ import annotations

try:
    from bots._bootstrap import prepare_entrypoint, require_portalocker
except ModuleNotFoundError:
    from _bootstrap import prepare_entrypoint, require_portalocker

prepare_entrypoint(__file__, __name__)

require_portalocker(exit_on_missing=True)

from core.cross_bot import CrossBot
from core.paths import LOG_DIR_CROSS
from config.exchange_config import get_futures_exchange_connection
from bot_utils.sim_flag import read_simulation_flag


class CrossMomentumBot(CrossBot):
    # Exchange-agnostic: the connection reads EXCHANGE from .env (MEXC, Bitget,
    # Binance, ...) via EXCHANGE_FACTORY below  -  NOT hardcoded to any venue.
    BOT_NAME = "CROSS"
    BOT_COLOR = "\033[96m"
    LOG_DIR = str(LOG_DIR_CROSS)
    DB_FILE = f"{LOG_DIR}/trades.json"
    COOLDOWN_FILE = f"{LOG_DIR}/cooldown.json"
    BUY_PREFIX = "xmom"
    BACKTEST_NOTE = (
        "Cross-sectional momentum: long top-K / short bottom-K, market-neutral, "
        "cross-margin, own-momentum crash filter. Experimental  -  SIM first."
    )

    DEFAULTS = {
        # -- Cross-sectional strategy --
        "XSEC_LOOKBACK_HOURS":   24,
        "XSEC_REBALANCE_HOURS":  72,
        "XSEC_K":                6,      # per side -> 12 positions
        "XSEC_UNIVERSE_SIZE":    40,
        "CRASH_FILTER":          1,
        "CRASH_WINDOW":          4,
        "CROSS_NEUTRALITY_TOL_PCT": 15.0,  # trim book if |net|/gross notional > this
        "PER_LEG_DISASTER_STOP": -25.0,  # single-coin gap protection
        "MIN_VOLUME":            10_000_000.0,  # liquid perps only
        "XSEC_MAX_SPREAD_PCT":   0.5,    # skip a leg if its book spread is wider
        "BASE_CAPITAL_USDT":     1000.0, # SIM equity / live sizing fallback
        # -- Risk / margin --
        "LEVERAGE":              1.0,    # cross-margin -> keep low (max ~1.5)
        "MARGIN_MODE":           "cross",
        "MAX_GROSS_EXPOSURE_PCT": 100.0, # cap deployed notional vs equity
        "MAX_DAILY_LOSS":        -50.0,  # account killswitch
        "MONITOR_INTERVAL":      30,
        # -- Keys required by validate_config_or_die / the shared banner --
        "POSITION_SIZE":         10.0,   # equity-based sizing is used; fallback
        "MAX_OPEN_TRADES":       12,     # 2 x K
        "INITIAL_STOP_LOSS":     -25.0,  # mirrors PER_LEG_DISASTER_STOP
        "ACTIVATION_PROFIT":     0.0,    # n/a (no per-trade TP)  -  banner only
        "TRAILING_DISTANCE":     0.0,    # n/a  -  banner only
        "LIQ_SAFETY_PCT":        20.0,
        "BREAKEVEN_TRIGGER":     0.0,
        "COOLDOWN_AFTER_SL":     0,
        "SCAN_INTERVAL":         300,    # banner only (rebalance uses XSEC_*)
        "SIMULATION":            True,   # paper first
    }

    @staticmethod
    def EXCHANGE_FACTORY():
        return get_futures_exchange_connection()


def run_bot():
    CrossMomentumBot(simulation=read_simulation_flag("CROSS")).run()


if __name__ == "__main__":
    run_bot()
