"""
Central configuration: design tokens, paths, bot metadata, parameter
definitions, and the load/save helpers for ``bot_config.json``.

This module owns all values that used to live as globals at the top of
``launcher.pyw`` so the rest of the package can import them from a single
authoritative place.
"""

from __future__ import annotations

import json
import math
import os
import sys
import subprocess
import threading       # tmp filename uses get_ident()
import time            # retry sleep between os.replace attempts
from contextlib import contextmanager
from tkinter import font as tkfont

from core.constants import CONFIG_AUDIT_BACKUPS, CONFIG_AUDIT_MAX_BYTES


#  Path anchors 
#
# This module sits two levels deep (``launcher/config/settings.py``), so a
# naive ``dirname(__file__)`` would resolve to ``launcher/config/`` and break
# subprocess ``cwd=``, DB lookups, prompt loading, etc. ``PROJECT_ROOT`` is
# pinned here once and imported by everything else.

_THIS_DIR  = os.path.dirname(os.path.abspath(__file__))  # /launcher/config
LAUNCHER_DIR = os.path.dirname(_THIS_DIR)  # /launcher
PROJECT_ROOT = os.path.dirname(LAUNCHER_DIR)  # /  (project root)

DB_PATH      = os.path.join(PROJECT_ROOT, "data", "trading_bot.db")
CONFIG_FILE  = os.path.join(PROJECT_ROOT, "bot_config.json")
DEFAULT_CONFIG_FILE = os.path.join(PROJECT_ROOT, "bot_config.default.json")


def _reject_json_constant(value: str):
    raise ValueError(f"non-standard JSON constant rejected: {value}")

OLLAMA_URL   = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
if not OLLAMA_URL.startswith(("http://", "https://")):
    OLLAMA_URL = "http://" + OLLAMA_URL


#  Design tokens 

COLORS = {
    #  Obsidian (logo-true) 
    # Derived from launcher/ui/components/obsidian.ico: a deep purple-black
    # volcanic-glass stone with a neon CYANVIOLET trending-up arrow. Base =
    # faceted obsidian; the SIGNATURE accent is the arrow's electric cyan,
    # with violet as a sparing secondary. Semantic green/red P&L stays.
    "bg":  "#0a0810",  # obsidian  deep purple-black
    "bg_alt":       "#0d0b14",
    "panel":        "#16131f",   # faceted stone surface
    "panel_hover":  "#201c2b",
    "panel_alt":    "#121019",
    "border":       "#2b2740",   # purple-slate hairline
    "border_soft":  "#1d1a2b",

    "text":         "#e9e7f2",   # cool white
    "text_dim":     "#9d9ab6",
    "text_muted":   "#6b6788",
    "text_subtle":  "#46425f",

    # These keys feed the system-monitor bars + Available Capital (referenced
    # directly); the BOT accents are overridden to the cyan signature below.
    "balanced":  "#4fbe8e",  # green  profit / Available Capital
    "balanced_dim":  "#235a43",
    "aggressive":  "#c98be0",  # violet  system bar
    "aggressive_dim":"#5e3a72",
    "futures":  "#5aa6e6",  # blue  system bar
    "futures_dim":   "#274a6b",
    "cross":  "#5a82b0",  # steel  system bar
    "cross_dim":     "#2c3e58",
    "futrend":  "#b07ae0",  # violet  system bar
    "futrend_dim":   "#4f2f6b",

    "purple":       "#6a5cc0",   # SIGNATURE = indigo-violet (logo arrowhead)
    "purple_dim":   "#322a5e",
    "violet":  "#b07ae0",  # secondary neon (arrowhead)  sparing
    "violet_dim":   "#4f2f6b",
    "success":      "#4fbe8e",
    "warning":      "#e0a23d",
    "danger":       "#e35349",
    "info":         "#6a5cc0",

    "bar_bg":       "#1d1a2b",
}

FONT_BODY = "Segoe UI"


#  Bot metadata 

# Order matters for layout  list + dict lookup.
BOT_ORDER = ["TREND", "SPOT", "FUTURES", "CROSS", "FUTREND"]

BOT_META = {
    "TREND": {
        # Display-only rename  "Trend" (the validated majors trend-following
        # bot). Internal key stays "TREND" so DB / state / config plumbing
        # and historical trades remain intact.
        "label":   "Trend",
        "subtitle":"Spot  Trend-Following",
        "accent":  COLORS["balanced"],
        "accent_dim": COLORS["balanced_dim"],
        "icon":  "",
        # Bots live in the bots/ subpackage and are started via
        # ``python -m bots.main_bot_balanced`` so their relative imports
        # (``from core.X import ``) resolve correctly. The ``script`` field
        # is kept for human reference; ``module`` is what BotProcess uses.
        "module":  "bots.main_bot_balanced",
        "script":  "bots/main_bot_balanced.py",
        "log_dir": "logs/Trend",
        # Mechanical strategy  no LLM, hence no editable prompt.
        "uses_llm": False,
        "is_futures": False,
    },
    "SPOT": {
        # Display-only rename  "Spot" (the momentum spot bot). Internal key
        # stays "SPOT".
        "label":   "Spot",
        "subtitle":"Spot  Momentum",
        "accent":  COLORS["aggressive"],
        "accent_dim": COLORS["aggressive_dim"],
        "icon":  "",
        "module":  "bots.main_bot_aggressive",
        "script":  "bots/main_bot_aggressive.py",
        "log_dir": "logs/Spot",
        "prompt":  "prompts/spot.txt",
        "prompt_default": "prompts/spot_default.txt",
        "is_futures": False,
    },
    "FUTURES": {
        "label":   "FUTURES",
        "subtitle":"Perpetuals  Long/Short",
        "accent":  COLORS["futures"],
        "accent_dim": COLORS["futures_dim"],
        "icon":  "",
        "module":  "bots.main_bot_futures",
        "script":  "bots/main_bot_futures.py",
        "log_dir": "logs/Futures",
        "prompt":  "prompts/futures.txt",
        "prompt_default": "prompts/futures_default.txt",
        "is_futures": True,
    },
    # Cross-sectional momentum (market-neutral, cross-margin). No LLM
    # (mechanical ranking)  no editable prompt. Trades futures perps.
    "CROSS": {
        "label":   "Cross",
        "subtitle":"Perps  Market-Neutral",
        "accent":  COLORS["cross"],
        "accent_dim": COLORS["cross_dim"],
        "icon":  "",
        "module":  "bots.main_bot_cross",
        "script":  "bots/main_bot_cross.py",
        "log_dir": "logs/Cross",
        "uses_llm": False,
        "is_futures": True,
    },
    # Leveraged trend-following futures (per-coin, directional long/flat). No LLM
    # (mechanical SMA-ensemble)  no editable prompt. Trades futures perps.
    "FUTREND": {
        "label":   "Future Trend",
        "subtitle":"Perps  Trend-Following",
        "accent":  COLORS["futrend"],
        "accent_dim": COLORS["futrend_dim"],
        "icon":  "",
        "module":  "bots.main_bot_trendfut",
        "script":  "bots/main_bot_trendfut.py",
        "log_dir": "logs/FuTrend",
        "uses_llm": False,
        "is_futures": True,
    },
}

# Obsidian  ONE signature accent (the logo's cyan) for all bots (no rainbow).
# Bot identity is carried by the name (display font) + icon, not a colour, for a
# monochrome-premium terminal. Semantic colour (green/red P&L, LIVE/SIM) is
# untouched. Remove this loop to restore the per-bot colours.
for _meta in BOT_META.values():
    _meta["accent"] = COLORS["purple"]
    _meta["accent_dim"] = COLORS["purple_dim"]


#  Default parameter values per bot 

DEFAULT_CONFIG = {
    # "Trend" bot (internal key TREND): large-cap trend-following over a
    # 12 large-cap universe, unleveraged spot.
    "TREND": {
        "POSITION_SIZE":  20.0,
        "MAX_OPEN_TRADES":   3,
        "TREND_UNIVERSE":    "BTC,ETH,BNB,XRP,SOL,ADA,AVAX,LINK,DOT,LTC,DOGE,TRX",
        "TREND_SMA_FAST":    50,
        "TREND_SMA_SLOW":    100,
        "TREND_CROSS_FAST":  50,     # slowed from 20/50 (drag-opt 2026-06-20): the
        "TREND_CROSS_SLOW":  150,    # fast cross was the whipsaw/turnover source
        "TREND_VOTE_MIN":  2,  # of 3 rules  in trend (1=aggressive, 3=strict)
        "TREND_EXIT_VOTE":   2,      # set 1 for hysteresis (less whipsaw)
        "TREND_CHECK_HOURS": 12,     # re-evaluate twice a day
        "POSITION_SIZE_MAX": 2500.0,
        "INITIAL_STOP_LOSS": -30.0,  # disaster stop (gap protection)
        "MAX_DAILY_LOSS":    -50.0,
        "LEARNING_DISABLED": True,   # skip Kelly/RSI/blacklist learning
        "TREND_VOL_TARGET":          0,   # 0=flat sizing, 1=inverse-vol (risk-parity)
        "TREND_VOL_TARGET_LOOKBACK": 30,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 1440,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "TREND_VOL_TARGET_MODE": "shadow",
        "SIMULATION":        True,
    },
    "SPOT": {
        "MIN_PUMP":          5.0,
        "ACTIVATION_PROFIT": 4.0,
        "TRAILING_DISTANCE": 1.5,
        "POST_PARTIAL_TRAILING_DISTANCE": 1.0,
        "INITIAL_STOP_LOSS": -10.0,
        "PARTIAL_SELL_PCT":  0.60,
        "RSI_MAX":           85.0,
        "POSITION_SIZE":     10.0,
        "POSITION_SIZE_MAX": 25.0,
        "MAX_OPEN_TRADES":   3,
        "SCAN_INTERVAL":  150,  # 2.5 min  fast for momentum
        "MONITOR_INTERVAL":  20,     # V2: open positions checked every 20s (dual-loop)
        "BREAKEVEN_TRIGGER": 0.0,
        "USE_TREND_FILTER":  0,
        "USE_LLM":           False,
        "COOLDOWN_AFTER_SL": 60,
        "MAX_DAILY_LOSS":    -50.0,
        "OWN_MOMENTUM_FILTER": True,
        "OWN_MOMENTUM_WINDOW": 8,
        "OWN_MOMENTUM_MIN_LOSS_PCT": 20.0,
        "ENTRY_QUALITY_FILTER_ENABLED": True,
        "ENTRY_QUALITY_MIN_SCORE": 75.0,
        "SPOT_EXIT_SHADOW_ENABLED": False,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 360,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "SIMULATION":        True,
    },
    "FUTURES": {
        # Enter when momentum STARTS, not when it's already over.
        "MIN_PUMP":          1.0,
        "ACTIVATION_PROFIT": 4.0,
        "TRAILING_DISTANCE": 1.5,
        "POST_PARTIAL_TRAILING_DISTANCE": 1.0,
        "INITIAL_STOP_LOSS": -4.0,
        "PARTIAL_SELL_PCT":  0.50,
        "RSI_MAX":           60.0,
        "POSITION_SIZE":     40.0,
        "POSITION_SIZE_MAX": 80.0,
        "MAX_OPEN_TRADES":   3,      # less cluster risk
        "LEVERAGE":          1.0,
        "LIQ_SAFETY_PCT":    15.0,
        "SCAN_INTERVAL":     150,    # 5min would be too long for futures
        "MONITOR_INTERVAL":  18.0,
        "BREAKEVEN_TRIGGER": 2.0,    # Move SL to BE at +2% price move
        "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED": True,
        "PRE_ACTIVATION_MIN_MFE_PCT": 1.5,
        "PRE_ACTIVATION_GIVEBACK_PCT": 0.75,
        "MFE_FALLBACK_STOP_ENABLED": True,
        "MFE_FALLBACK_MIN_AGE_MINUTES": 45.0,
        "MFE_FALLBACK_MIN_MFE_PCT": 0.8,
        "MFE_FALLBACK_EXIT_MOVE_PCT": -1.5,
        "USE_TREND_FILTER":  0,      # EMA200 filter off (toggle)
        "USE_LLM":           False,
        "COOLDOWN_AFTER_SL": 240,
        "MAX_DAILY_LOSS":    -20.0,
        "MAX_DAILY_LOSS_HARD_MULT": 1.5,
        "OWN_MOMENTUM_FILTER": True,
        "OWN_MOMENTUM_WINDOW": 5,
        "OWN_MOMENTUM_MIN_LOSS_PCT": 20.0,
        "ENTRY_QUALITY_FILTER_ENABLED": True,
        "ENTRY_QUALITY_MIN_SCORE": 75.0,
        "ENTRY_QUALITY_SHADOW_ENABLED": False,
        "ENTRY_QUALITY_SHADOW_MIN_SCORE": 85.0,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 360,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "MAKER_FIRST_MODE": "disabled",
        "MAKER_FIRST_TTL_SECONDS": 3.0,
        "MAKER_FIRST_MARKET_FALLBACK": False,
        "TCA_ENABLED": True,
        "TCA_DEPTH_LEVELS": 20,
        "DEPTH_GATE_MODE": "shadow",
        "VENUE_RECORDER_MODE": "enabled",
        "VENUE_RECORDER_MAX_SYMBOLS": 8,
        "VENUE_RECORDER_MICRO_INTERVAL_SECONDS": 6.0,
        "VENUE_RECORDER_OVERVIEW_INTERVAL_SECONDS": 60.0,
        "VENUE_RECORDER_DEPTH_LEVELS": 20,
        "VENUE_RECORDER_RETENTION_DAYS": 30,
        "VENUE_RECORDER_MAX_STORAGE_GIB": 20.0,
        "VENUE_L2_MODE": "shadow",
        "VENUE_L2_SAMPLE_INTERVAL_SECONDS": 1.0,
        "VENUE_L2_STALE_AFTER_MS": 5000,
        "SIMULATION":        True,
    },
    # Cross-sectional momentum  market-neutral, cross-margin, experimental.
    # Keep SIMULATION=True until it proves out over weeks of paper trading.
    "CROSS": {
        "XSEC_LOOKBACK_HOURS":   24,
        "XSEC_REBALANCE_HOURS":  48.0,
        "XSEC_K":  2.0,
        "XSEC_UNIVERSE_SIZE":    30.0,
        "CRASH_FILTER":          1,
        "CRASH_WINDOW":          4,
        "PER_LEG_DISASTER_STOP": -8.0,
        "CROSS_DISASTER_BLACKLIST_HOURS": 72,
        "MIN_VOLUME":            10000000.0,
        "XSEC_MAX_SPREAD_PCT":   0.5,
        "BASE_CAPITAL_USDT":     150.0,
        "LEVERAGE":  1.0,  # cross-margin  keep low (max ~1.5)
        "MAX_GROSS_EXPOSURE_PCT": 100.0,   # cap deployed notional vs equity
        "MAX_DAILY_LOSS":        -50.0,
        "MONITOR_INTERVAL":      30,
        # required by validate_config_or_die / shared banner
        "POSITION_SIZE":         10.0,
        "MAX_OPEN_TRADES":       12,
        "INITIAL_STOP_LOSS":     -25.0,
        "ENTRY_QUALITY_FILTER_ENABLED": True,
        "ENTRY_QUALITY_MIN_SCORE": 75.0,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 720,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "TCA_ENABLED": True,
        "TCA_DEPTH_LEVELS": 20,
        "DEPTH_GATE_MODE": "shadow",
        "SIMULATION":            True,
    },
    # Leveraged trend-following futures (per-coin, long/flat). SIM-first.
    # Conservative 4h setup; risk thresholds are raw price %, not leverage-scaled.
    "FUTREND": {
        "TREND_TIMEFRAME":       "4h",
        "TREND_CHECK_MINUTES":   60,
        "TREND_SMA_FAST":        300,
        "TREND_SMA_SLOW":        600,
        "TREND_CROSS_FAST":      120,
        "TREND_CROSS_SLOW":      300,
        "TREND_VOTE_MIN":        2,
        "TREND_EXIT_VOTE":       2,
        "TREND_UNIVERSE_SIZE":   30,
        "MIN_VOLUME":            10000000.0,
        "LEVERAGE":              1.0,
        "POSITION_SIZE":         50.0,
        "POSITION_SIZE_MAX":     2500.0,
        "MAX_OPEN_TRADES":       6,
        "MAX_NEW_TRADES_PER_TICK": 1,
        "INITIAL_STOP_LOSS":     -6.0,
        "FAILED_ENTRY_STOP_ENABLED": True,
        "FAILED_ENTRY_MAX_AGE_MIN": 120,
        "FAILED_ENTRY_MIN_MFE_PCT": 0.5,
        "FAILED_ENTRY_LOSS_PCT": -2.5,
        "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED": True,
        "PRE_ACTIVATION_MIN_MFE_PCT": 0.8,
        "PRE_ACTIVATION_GIVEBACK_PCT": 2.75,
        "LIQ_SAFETY_PCT":        20.0,
        "MAX_DAILY_LOSS":        -50.0,
        "MAX_DAILY_LOSS_HARD_MULT": 1.5,
        "MONITOR_INTERVAL":      20,
        "ACTIVATION_PROFIT":     2.25,
        "TRAILING_DISTANCE":     1.5,
        "POST_PARTIAL_TRAILING_DISTANCE": 1.0,
        "BREAKEVEN_TRIGGER":     1.8,
        "PARTIAL_SELL_PCT":      0.5,
        "TREND_EXIT_STALE_LIMIT": 3,
        "TREND_VOL_TARGET":          0,
        "TREND_VOL_TARGET_LOOKBACK": 30,
        "TREND_VOL_TARGET_MODE": "shadow",
        "ENTRY_QUALITY_FILTER_ENABLED": True,
        "ENTRY_QUALITY_MIN_SCORE": 75.0,
        "COOLDOWN_AFTER_SL": 240,
        "BAD_SYMBOL_FILTER": True,
        "SINGLE_STOP_MIN_LOSS_PCT": 5.0,
        "SINGLE_STOP_BLACKLIST_HOURS": 4,
        "BAD_SYMBOL_LOSS_COUNT": 2,
        "BAD_SYMBOL_MIN_TOTAL_LOSS_USDT": 6.0,
        "BAD_SYMBOL_BLACKLIST_HOURS": 24,
        "BAD_SYMBOL_LOOKBACK_DAYS": 1,
        "PORTFOLIO_RISK_MODE": "shadow",
        "NET_EXPECTANCY_MODE": "shadow",
        "TIME_DECAY_MODE": "shadow",
        "TIME_DECAY_MAX_AGE_MINUTES": 1440,
        "TIME_DECAY_MIN_MFE_PCT": 0.5,
        "MAKER_FIRST_MODE": "disabled",
        "TCA_ENABLED": True,
        "TCA_DEPTH_LEVELS": 20,
        "DEPTH_GATE_MODE": "shadow",
        "SIMULATION":            True,
    },
    "UI": {
        "VISIBLE_BOTS":   ["TREND", "SPOT", "FUTURES", "CROSS", "FUTREND"],
        "COLLAPSED_BOTS": [],
        # Secure default for new installations. Existing installations that
        # predate this explicit setting retain LAN access during migration and
        # can later be restricted after their firewall/VPN path is configured.
        "DASHBOARD_BIND_ADDRESS": "127.0.0.1",
        "params_collapsed": {
            "FUTREND": True,
            "FUTURES": True,
            "CROSS": True,
            "SPOT": True,
            "TREND": True,
        },
    },
}


#  Parameter editor definitions 
# (key, label, step, min, max, format, suffix, tooltip)
# Suffix with USDT moved into the label so the input value stays compact.

PARAM_DEFS_SPOT = [
    ("MIN_PUMP",          "Min. Pump",            0.5,   0.5,  20.0,  ".1f", "%",
     "Minimum 24h price change a coin must show to be scanned. Lower = more candidates, more noise."),
    ("ACTIVATION_PROFIT", "Activation TP",        0.5,   1.0,  20.0,  ".1f", "%",
     "Profit % at which the partial take-profit triggers and the stop is moved to break-even."),
    ("TRAILING_DISTANCE", "Trailing Distance",    0.25,  0.25, 10.0,  ".2f", "%",
     "How far the trailing stop sits below the highest seen price (in %)."),
    ("POST_PARTIAL_TRAILING_DISTANCE", "Post-Partial Trail", 0.25, 0.25, 10.0, ".2f", "%",
     "Trailing distance after the partial take-profit fired. Lower locks in runners faster."),
    ("INITIAL_STOP_LOSS", "Stop Loss",            0.5, -15.0, -0.5,  ".1f", "%",
     "Initial stop-loss level. Tighter (closer to 0) = quicker exit on bad trades."),
    ("BREAKEVEN_TRIGGER", "Breakeven At",         0.25,  0.0,  10.0,  ".2f", "%",
     "Move stop-loss to entry price when profit reaches this %. 0 = disabled. "
     "Makes trades risk-free once the move starts working."),
    ("PARTIAL_SELL_PCT",  "Partial Sell",         0.05,  0.05, 1.00,  ".2f", "",
     "Fraction of position sold at Activation TP. 0.30 = sell 30%, keep 70% for trailing."),
    ("RSI_MAX",           "RSI Max",              1.0,  40.0, 90.0,  ".0f", "",
     "Maximum RSI on entry. Coins with 2+ timeframes above this RSI are skipped."),
    ("POSITION_SIZE",     "Pos. Size (USDT)",     1.0,   1.0, 500.0, ".0f", "",
     "Base USDT amount per trade. Risk-Manager may scale this via Kelly criterion."),
    ("POSITION_SIZE_MAX", "Kelly Cap (USDT)",     5.0,   5.0, 500.0, ".0f", "",
     "Upper cap for the dynamically scaled position size."),
    ("MAX_OPEN_TRADES",   "Max Open Trades",      1.0,   1.0,  30.0, ".0f", "",
     "How many parallel positions the bot may run. 1-30."),
    ("SCAN_INTERVAL",     "Scan Interval",        15.0,  30.0, 600.0, ".0f", "s",
     "Seconds between scan cycles. Shorter = faster reaction, more API calls."),
    ("MONITOR_INTERVAL",  "Monitor Interval",     5.0,   5.0,  120.0, ".0f", "s",
     "V2 DUAL-LOOP: seconds between exit checks on OPEN positions. "
     "Shorter = tighter trailing stops + faster SL/TP reaction, but more API calls. "
     "20s is a good middle ground."),
    ("COOLDOWN_AFTER_SL", "SL Cooldown",          15.0,  0.0, 1440.0, ".0f", "m",
     "Minutes a coin is blacklisted after hitting stop-loss. Prevents revenge trading."),
    ("MAX_DAILY_LOSS",    "Daily Loss Limit",     5.0, -500.0, -5.0, ".0f", "$",
     "Bot pauses for the day if accumulated daily loss exceeds this USDT amount (negative)."),
    ("OWN_MOMENTUM_WINDOW", "Momentum Window",     1.0,   3.0,  50.0, ".0f", "",
     "Number of recent own trades used by the self-momentum pause filter."),
    ("OWN_MOMENTUM_MIN_LOSS_PCT", "Momentum Loss", 1.0,   1.0,  50.0, ".0f", "%",
     "Self-momentum pause threshold as loss % of invested capital in the recent window."),
    ("ENTRY_QUALITY_FILTER_ENABLED", "Entry Filter", 1.0, 0.0, 1.0, ".0f", "",
     "1 = block live entries below the configured minimum score. 0 = log only."),
    ("ENTRY_QUALITY_MIN_SCORE", "Entry Min Score", 5.0, 0.0, 100.0, ".0f", "",
     "Minimum live entry quality score. Default 75 blocks LOW/MID and allows HIGH."),
]

PARAM_DEFS_FUTURES = [
    ("MIN_PUMP",          "Min. Move",            0.5,   0.5,  20.0,  ".1f", "%",
     "Minimum 24h price move (absolute) to qualify as a candidate. "
     "Applies in BOTH directions: +X% for LONG candidates, -X% for SHORT candidates. "
     "1.0% scans more coins with weaker signals; 2-3% filters for stronger momentum."),
    ("ACTIVATION_PROFIT", "Activation TP",        0.5,   1.0,  20.0,  ".1f", "%",
     "Raw price-move % that triggers partial close. 4.5% is realistic for hit-rate."),
    ("TRAILING_DISTANCE", "Trailing Distance",    0.25,  0.25, 10.0,  ".2f", "%",
     "Trailing stop distance. 2.5% gives crypto room to breathe before stop-out."),
    ("POST_PARTIAL_TRAILING_DISTANCE", "Post-Partial Trail", 0.25, 0.25, 10.0, ".2f", "%",
     "Trailing distance after partial close. Lower locks in remaining profit faster."),
    ("INITIAL_STOP_LOSS", "Stop Loss",            0.5, -15.0, -0.5,  ".1f", "%",
     "Initial stop in raw price terms. -3.5% sits above normal coin noise."),
    ("BREAKEVEN_TRIGGER", "Breakeven At",         0.25,  0.0,  10.0,  ".2f", "%",
     "Move SL to entry when profit reaches this %. 0 = disabled. Risk-free trades once moving."),
    ("PRE_ACTIVATION_GIVEBACK_STOP_ENABLED", "Peak Trail", 1.0, 0.0, 1.0, ".0f", "",
     "1 = protect favorable moves before the partial close. 0 = disabled."),
    ("PRE_ACTIVATION_MIN_MFE_PCT", "Peak Activation", 0.25, 0.25, 10.0, ".2f", "%",
     "Favorable raw price move required before the pre-activation peak trail is armed."),
    ("PRE_ACTIVATION_GIVEBACK_PCT", "Peak Giveback", 0.25, 0.25, 10.0, ".2f", "%",
     "Raw percentage-point giveback from the best seen move that closes the position."),
    ("MFE_FALLBACK_STOP_ENABLED", "Aged MFE Stop", 1.0, 0.0, 1.0, ".0f", "",
     "1 = close an aged pre-partial position after a favorable move fully fails."),
    ("MFE_FALLBACK_MIN_AGE_MINUTES", "MFE Stop Age", 5.0, 5.0, 1440.0, ".0f", "min",
     "Minimum position age before the failed-MFE fallback can close it."),
    ("MFE_FALLBACK_MIN_MFE_PCT", "MFE Stop Arm", 0.1, 0.1, 5.0, ".2f", "%",
     "Minimum favorable raw price move observed before the fallback is armed."),
    ("MFE_FALLBACK_EXIT_MOVE_PCT", "MFE Stop Exit", 0.25, -10.0, -0.25, ".2f", "%",
     "Current raw price move at or below which an armed aged position closes."),
    ("PARTIAL_SELL_PCT",  "Partial Close",        0.05,  0.05, 1.00,  ".2f", "",
     "Fraction closed at Activation TP. Rest moves to break-even with trailing."),
    ("RSI_MAX",           "RSI Max",              1.0,  40.0, 90.0,  ".0f", "",
     "RSI ceiling for LONG entries. SHORTs look for RSI ABOVE this value."),
    ("POSITION_SIZE",     "Margin (USDT)",        1.0,   1.0, 500.0, ".0f", "",
     "Margin per position. Notional exposure = margin  leverage."),
    ("POSITION_SIZE_MAX", "Kelly Cap (USDT)",     5.0,   5.0, 500.0, ".0f", "",
     "Cap for dynamically scaled margin amount."),
    ("MAX_OPEN_TRADES",   "Max Open Trades",      1.0,   1.0,  30.0, ".0f", "",
     "Number of simultaneous futures positions. 3 is safer; more = cluster risk."),
    ("LEVERAGE",          "Leverage",             1.0,   1.0,  10.0, ".0f", "x",
     "Leverage 1-10x. 3x default = total loss at ~33% price move against."),
    ("LIQ_SAFETY_PCT",    "Liq Safety Buffer",    1.0,   5.0,  50.0, ".0f", "%",
     "Auto-close when distance to liquidation drops below this %."),
    ("SCAN_INTERVAL",     "Scan Interval",        15.0,  30.0, 600.0, ".0f", "s",
     "Seconds between scans for NEW trades. Open positions are monitored more often."),
    ("MONITOR_INTERVAL",  "Monitor Interval",     5.0,   5.0, 120.0, ".0f", "s",
     "Seconds between liquidation/SL checks on OPEN positions. Tighter = faster reaction."),
    ("COOLDOWN_AFTER_SL", "SL Cooldown",          15.0,  0.0, 1440.0, ".0f", "m",
     "Minutes a coin is blacklisted after stop-loss/liquidation."),
    ("MAX_DAILY_LOSS",    "Daily Loss Limit",     5.0, -500.0, -5.0, ".0f", "$",
     "Bot pauses for the day if accumulated daily loss exceeds this USDT amount."),
    ("MAX_DAILY_LOSS_HARD_MULT", "Hard Loss Mult", 0.25,  1.0,   5.0, ".2f", "x",
     "Hard daily-loss multiplier used as an emergency extension over the normal bot limit."),
    ("OWN_MOMENTUM_WINDOW", "Momentum Window",     1.0,   3.0,  50.0, ".0f", "",
     "Number of recent own trades used by the self-momentum pause filter."),
    ("OWN_MOMENTUM_MIN_LOSS_PCT", "Momentum Loss", 1.0,   1.0,  50.0, ".0f", "%",
     "Self-momentum pause threshold as loss % of invested capital in the recent window."),
]

# "Trend" bot (TREND slot)  majors trend-following (spot, no leverage).
# Edit the coin universe (TREND_UNIVERSE) directly in bot_config.json; the
# slider editor below covers the numeric knobs.
PARAM_DEFS_TREND = [
    ("POSITION_SIZE",     "Pos. Size (USDT/coin)", 1.0,   1.0, 2500.0, ".0f", "",
     "USDT bought per coin when it is in an uptrend. With N coins held, max "
     "deployed = N  this. Spot, no leverage."),
    ("POSITION_SIZE_MAX", "Hard Cap (USDT)",       5.0,   5.0, 2500.0, ".0f", "",
     "Hard cap for per-coin size after optional vol-targeting. Keep >= Pos. Size."),
    ("MAX_OPEN_TRADES",   "Max Open Trades",       1.0,   1.0,  20.0, ".0f", "",
     "Maximum number of coins held at once. With the 12-major universe, 12 "
     "means all of them when in trend."),
    ("TREND_VOTE_MIN",    "Trend Sensitivity",     1.0,   1.0,   3.0, ".0f", "",
     "How many of the 3 trend rules (P>SMA50, P>SMA100, SMA20>50) must agree to "
     "go long. 1 = aggressive (more time in market), 2 = balanced, 3 = strict."),
    ("TREND_EXIT_VOTE",   "Exit Threshold",        1.0,   1.0,   3.0, ".0f", "",
     "Sell when votes fall BELOW this. Set lower than Sensitivity for hysteresis "
     "(fewer whipsaw round-trips at the edge)."),
    ("TREND_CHECK_HOURS", "Check Interval",        1.0,   1.0,  24.0, ".0f", "h",
     "How often the daily trend signal is re-evaluated. 12 = twice a day. The "
     "signal only changes on new daily candles, so faster gains little."),
    ("MAX_DAILY_LOSS",    "Daily Loss Limit",      5.0, -500.0, -5.0, ".0f", "$",
     "Killswitch: stop opening new positions for the day past this USDT loss."),
    ("INITIAL_STOP_LOSS", "Disaster Stop",         5.0, -90.0, -10.0, ".0f", "%",
     "Hard safety stop per coin for gap/flash-crash protection. The trend exit "
     "normally fires long before this  it's a last-resort brake, not the main "
     "exit. -30% is a sane default."),
    ("TREND_SMA_FAST",    "SMA Fast",              5.0,  10.0, 200.0, ".0f", "d",
     "Fast price moving-average length in days (rule: price > SMA). Default 50. "
     "Validated robust across 30120  rarely needs changing."),
    ("TREND_SMA_SLOW",    "SMA Slow",              5.0,  20.0, 300.0, ".0f", "d",
     "Slow price moving-average length in days. Default 100."),
    ("TREND_CROSS_FAST",  "Cross Fast",            5.0,   5.0, 100.0, ".0f", "d",
     "Fast MA for the cross rule (SMA_fast > SMA_slow). Default 20."),
    ("TREND_CROSS_SLOW",  "Cross Slow",            5.0,  10.0, 200.0, ".0f", "d",
     "Slow MA for the cross rule. Default 50."),
    ("TREND_VOL_TARGET",  "Vol-Targeting (0/1)",   1.0,   0.0,   1.0, ".0f", "",
     "0 = flat sizing (same USDT per coin). 1 = inverse-volatility sizing: calm "
     "coins get a bigger slot, wild coins a smaller one, so each contributes "
     "similar risk. Backtests: higher return AND lower drawdown; total exposure "
     "stays ~the same."),
    ("TREND_VOL_TARGET_LOOKBACK", "Vol Lookback",  5.0,  10.0, 120.0, ".0f", "d",
     "Days of returns used to gauge each coin's volatility for vol-targeting."),
]

PARAM_DEFS_CROSS = [
    ("XSEC_K",               "Coins per side",     1.0,   2.0,    15.0, ".0f", "",
     "Coins held LONG and SHORT each (market-neutral). 6 = 12 positions total. "
     "More = more diversified, needs more capital."),
    ("XSEC_LOOKBACK_HOURS",  "Lookback",           6.0,   6.0,   336.0, ".0f", "h",
     "Ranking window  coins ranked by their return over the last N hours. "
     "24h was the best in research."),
    ("XSEC_REBALANCE_HOURS", "Rebalance every",    6.0,   6.0,   336.0, ".0f", "h",
     "How often the long/short basket is rebuilt. 72h keeps fees/funding low "
     "while capturing the multi-day drift."),
    ("XSEC_UNIVERSE_SIZE",   "Universe size",      5.0,  10.0,   100.0, ".0f", "",
     "How many of the most-liquid perps to rank across."),
    ("MIN_VOLUME",           "Min Volume",   1000000.0, 1000000.0, 100000000.0, ".0f", "$",
     "Minimum 24h quote volume for a perp to enter the ranked CROSS universe."),
    ("LEVERAGE",             "Leverage",           0.5,   1.0,     3.0, ".1f", "x",
     "Cross-margin leverage. Keep LOW (11.5x): cross margin means one bad "
     "position can draw down the WHOLE account."),
    ("MAX_GROSS_EXPOSURE_PCT","Max Gross Exposure",10.0,  20.0,   200.0, ".0f", "%",
     "Hard cap on deployed notional vs equity, regardless of leverage. 100 = "
     "long+short notional sums to your equity. Backstop against a mis-set lever."),
    ("CRASH_FILTER",         "Crash Filter",       1.0,   0.0,     1.0, ".0f", "",
     "1 = on. Goes flat after the strategy's own recent rebalances turn "
     "net-negative (halves drawdowns). 0 = off."),
    ("PER_LEG_DISASTER_STOP","Per-Leg Stop",       1.0, -90.0,    -5.0, ".0f", "%",
     "A single coin moving this far against its leg is closed early (doesn't "
     "wait for the next rebalance). Idiosyncratic tail protection."),
    ("XSEC_MAX_SPREAD_PCT",  "Max Spread",         0.1,   0.1,     3.0, ".1f", "%",
     "Skip a coin whose order-book spread is wider than this  keeps the bot on "
     "LIQUID perps (illiquid junk would bleed on slippage). Also makes SIM "
     "realistic: it fills at the real ask/bid, not the mid."),
    ("ENTRY_QUALITY_FILTER_ENABLED", "Entry Filter", 1.0, 0.0, 1.0, ".0f", "",
     "1 = block live entries below the configured minimum score. 0 = log only."),
    ("ENTRY_QUALITY_MIN_SCORE", "Entry Min Score", 5.0, 0.0, 100.0, ".0f", "",
     "Minimum live entry quality score. Default 75 blocks LOW/MID and allows HIGH."),
    ("MAX_DAILY_LOSS",       "Daily Loss Limit",   5.0,-500.0,    -5.0, ".0f", "$",
     "Account killswitch: stop opening new positions past this USDT loss/day."),
    ("BASE_CAPITAL_USDT",    "Base Capital",      50.0,  50.0,100000.0, ".0f", "$",
     "Equity used for sizing (SIM) / fallback when the live balance read fails. "
     "Gross exposure = this  leverage."),
]


PARAM_DEFS_FUTREND = [
    ("LEVERAGE",             "Leverage",           0.5,   1.0,     6.0, ".1f", "x",
     "EFFECTIVE leverage (fractional ok). Notional = margin  this; the exchange "
     "gets ceil() as the integer cap. WARNING: trend-following has large "
     "drawdowns  backtests show 3 is account-ruinous. 11.5 is sane."),
    ("POSITION_SIZE",        "Margin / Trade",     1.0,   5.0,  2500.0, ".0f", "$",
     "Margin (USDT) posted per position. Notional = this  leverage."),
    ("POSITION_SIZE_MAX",    "Hard Cap (USDT)",    5.0,   5.0,  2500.0, ".0f", "$",
     "Hard cap for margin after optional vol-targeting. Keep >= Margin / Trade."),
    ("MAX_OPEN_TRADES",      "Max Positions",      1.0,   1.0,    20.0, ".0f", "",
     "How many trending coins to hold at once."),
    ("MAX_NEW_TRADES_PER_TICK", "New / Scan",      1.0,   0.0,    10.0, ".0f", "",
     "Entry ramp limiter per signal scan. 0 freezes new entries while monitoring stays active."),
    ("INITIAL_STOP_LOSS",    "Hard Stop",          0.5, -30.0,    -2.0, ".1f", "%",
     "Fast price stop between candle checks (the primary exit is trend-off). "
     "Must be > -90/leverage or the bot refuses to start (would sit past liq)."),
    ("FAILED_ENTRY_STOP_ENABLED", "Failed Entry Stop", 1.0, 0.0, 1.0, ".0f", "",
     "1 = close fresh trades that never moved into profit and quickly fail."),
    ("FAILED_ENTRY_MAX_AGE_MIN", "Failed Entry Age", 5.0, 15.0, 360.0, ".0f", "m",
     "Only evaluate the failed-entry stop during this many minutes after entry."),
    ("FAILED_ENTRY_MIN_MFE_PCT", "Failed Entry MFE", 0.25, 0.0, 5.0, ".2f", "%",
     "Minimum favorable move required to avoid failed-entry classification."),
    ("FAILED_ENTRY_LOSS_PCT", "Failed Entry Loss", 0.25, -10.0, -0.5, ".2f", "%",
     "Close a fresh unproven entry once price moves this far against it."),
    ("PRE_ACTIVATION_GIVEBACK_STOP_ENABLED", "Pre-Act Stop", 1.0, 0.0, 1.0, ".0f", "",
     "1 = close trades that made a small favorable move but gave it back before Activation TP."),
    ("PRE_ACTIVATION_MIN_MFE_PCT", "Pre-Act MFE", 0.25, 0.0, 5.0, ".2f", "%",
     "Minimum favorable move before the pre-activation giveback stop can fire."),
    ("PRE_ACTIVATION_GIVEBACK_PCT", "Giveback %", 0.25, 0.25, 10.0, ".2f", "%",
     "Close before Activation TP after this much giveback from the best seen move."),
    ("ACTIVATION_PROFIT",    "Activation TP",      0.5,   1.0,    20.0, ".1f", "%",
     "Raw price move that arms partial take-profit and pre-partial trailing."),
    ("TRAILING_DISTANCE",    "Trailing Distance",  0.25,  0.25,   10.0, ".2f", "%",
     "Retrace from the highest price that closes the remaining trend position."),
    ("POST_PARTIAL_TRAILING_DISTANCE", "Post-Partial Trail", 0.25, 0.25, 10.0, ".2f", "%",
     "Retrace after partial TP. Lower values lock in trend runners faster."),
    ("BREAKEVEN_TRIGGER",    "Breakeven At",       0.25,  0.0,    10.0, ".2f", "%",
     "Move the protective stop to fee-buffered breakeven at this raw price move."),
    ("PARTIAL_SELL_PCT",     "Partial Close",      0.05,  0.05,   1.00, ".2f", "",
     "Fraction closed at Activation TP. 0.50 = close half, trail the rest."),
    ("ENTRY_QUALITY_FILTER_ENABLED", "Entry Filter", 1.0, 0.0, 1.0, ".0f", "",
     "1 = block live entries below the configured minimum score. 0 = log only."),
    ("ENTRY_QUALITY_MIN_SCORE", "Entry Min Score", 5.0, 0.0, 100.0, ".0f", "",
     "Minimum live entry quality score. Default 75 blocks LOW/MID and allows HIGH."),
    ("LIQ_SAFETY_PCT",       "Liq Safety",         1.0,   5.0,    50.0, ".0f", "%",
     "Force-close when this much of the liquidation buffer remains  last-resort "
     "catastrophe guard."),
    ("TREND_CHECK_MINUTES",  "Signal Check",       5.0,   5.0,   240.0, ".0f", "m",
     "How often the trend signal is recomputed (one candle is natural)."),
    ("TREND_UNIVERSE_SIZE",  "Universe size",      5.0,  10.0,   100.0, ".0f", "",
     "How many of the most-liquid perps to scan for trends."),
    ("TREND_SMA_FAST",       "SMA Fast",          10.0,  20.0,  1000.0, ".0f", "",
     "Fast SMA length (bars). For 1h candles, 300  the validated daily SMA50."),
    ("TREND_SMA_SLOW",       "SMA Slow",          10.0,  40.0,  2000.0, ".0f", "",
     "Slow SMA length (bars). For 1h candles, 600  the validated daily SMA100."),
    ("TREND_VOTE_MIN",       "Vote to Enter",      1.0,   1.0,     3.0, ".0f", "",
     "How many of the 3 trend rules must agree to OPEN (2 = robust default)."),
    ("MAX_DAILY_LOSS",       "Daily Loss Limit",   5.0,-500.0,    -5.0, ".0f", "$",
     "Account killswitch: stop opening new positions past this USDT loss/day."),
    ("TREND_VOL_TARGET",     "Vol-Targeting (0/1)", 1.0,   0.0,    1.0, ".0f", "",
     "0 = flat margin per coin. 1 = inverse-volatility sizing (risk-parity): "
     "calm coins bigger, wild smaller, same total. Backtests: better return/DD."),
    ("TREND_VOL_TARGET_LOOKBACK", "Vol Lookback",  5.0,  10.0,  200.0, ".0f", "bars",
     "Bars of returns used to gauge each coin's volatility for vol-targeting."),
    ("TREND_EXIT_STALE_LIMIT", "Stale Exit Limit", 1.0,   1.0,    10.0, ".0f", "",
     "Consecutive failed trend-data checks on a held coin before defensive exit."),
]


#  Python interpreter resolution 

def _get_python_exe() -> str:
    """Path to the python executable used to launch bot subprocesses.

    Keep the same interpreter order as the launcher start scripts: install
    venv first, then portable python, then the current interpreter.
    """
    venv_python = os.path.join(PROJECT_ROOT, ".venv", "Scripts", "python.exe")
    if os.path.exists(venv_python):
        return venv_python
    local_python = os.path.join(PROJECT_ROOT, "python", "python.exe")
    if os.path.exists(local_python):
        return local_python
    return sys.executable


def _get_pythonw_exe() -> str:
    """Windowless variant of :func:`_get_python_exe` for GUI subprocesses."""
    venv = os.path.join(PROJECT_ROOT, ".venv", "Scripts", "pythonw.exe")
    if os.path.exists(venv):
        return venv
    local = os.path.join(PROJECT_ROOT, "python", "pythonw.exe")
    if os.path.exists(local):
        return local
    py = sys.executable
    if os.path.basename(py).lower() == "python.exe":
        return os.path.join(os.path.dirname(py), "pythonw.exe")
    return py


#  Font helper 

def _safe_mono_font() -> str:
    """First installed monospaced font from a small preference list."""
    try:
        avail = set(tkfont.families())
        for f in ("Cascadia Mono", "JetBrains Mono", "Consolas", "Courier New"):
            if f in avail:
                return f
    except Exception:
        pass
    return "Consolas"


def _safe_display_font() -> str:
    """First installed display/heading face from a small preference list.
    Bahnschrift (a technical DIN-style grotesk that ships with Windows 10/11)
    gives the terminal its 'instrument' character for titles + eyebrows; falls
    back gracefully to Segoe UI so the launcher never renders a missing font."""
    try:
        avail = set(tkfont.families())
        for f in ("Bahnschrift", "Bahnschrift SemiBold", "Segoe UI Semibold",
                  "Segoe UI"):
            if f in avail:
                return f
    except Exception:
        pass
    return "Segoe UI"


#  Config IO 

_LAUNCHER_CONFIG_JSON_MAX_BYTES = 2 * 1024 * 1024


def _read_launcher_config_json(path: str):
    with open(path, "rb") as stream:
        raw = stream.read(_LAUNCHER_CONFIG_JSON_MAX_BYTES + 1)
    if len(raw) > _LAUNCHER_CONFIG_JSON_MAX_BYTES:
        raise ValueError("launcher config JSON exceeds size limit")
    return json.loads(
        raw.decode("utf-8-sig"),
        parse_constant=_reject_json_constant,
    )


def effective_default_config() -> dict:
    """Return the user-facing default config.

    ``DEFAULT_CONFIG`` is the in-code safety fallback used when the packaged
    default file is missing or broken. ``bot_config.default.json`` is the
    release/user default and must drive first-run creation and Reset buttons so
    both paths restore the same values.
    """
    if not os.path.exists(DEFAULT_CONFIG_FILE):
        return json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        candidate = _read_launcher_config_json(DEFAULT_CONFIG_FILE)
    except Exception:
        return json.loads(json.dumps(DEFAULT_CONFIG))
    if not isinstance(candidate, dict):
        return json.loads(json.dumps(DEFAULT_CONFIG))
    cfg = json.loads(json.dumps(candidate))
    for section, defaults in DEFAULT_CONFIG.items():
        value = candidate.get(section)
        if isinstance(defaults, dict):
            merged = dict(defaults)
            if isinstance(value, dict):
                merged.update(value)
            cfg[section] = merged
        elif section not in cfg:
            cfg[section] = defaults
    return cfg

def load_config() -> dict:
    """Load ``bot_config.json``, filling in any missing keys from
    :data:`DEFAULT_CONFIG`. Creates the file on first run."""
    defaults_cfg = effective_default_config()
    if not os.path.exists(CONFIG_FILE):
        cfg = json.loads(json.dumps(defaults_cfg))
        save_config(cfg)
        return json.loads(json.dumps(cfg))
    try:
        cfg = _read_launcher_config_json(CONFIG_FILE)
    except Exception as e:
        raise RuntimeError(
            f"bot_config.json corrupt or unreadable: {e}. "
            "Refusing to replace it with defaults."
        ) from e
    if not isinstance(cfg, dict):
        raise RuntimeError("bot_config.json root must be an object")
    existing_ui = cfg.get("UI")
    legacy_dashboard_binding = not (
        isinstance(existing_ui, dict)
        and "DASHBOARD_BIND_ADDRESS" in existing_ui
    )
    # Fill missing keys from defaults
    for bot, defaults in defaults_cfg.items():
        if not isinstance(defaults, dict):
            cfg.setdefault(bot, defaults)
            continue
        if bot not in cfg:
            cfg[bot] = dict(defaults)
        else:
            for k, v in defaults.items():
                cfg[bot].setdefault(k, v)
    # UI defaults
    if "UI" not in cfg:
        cfg["UI"] = dict(defaults_cfg["UI"])
    else:
        for k, v in defaults_cfg["UI"].items():
            cfg["UI"].setdefault(k, v)
    if legacy_dashboard_binding:
        # Before this option existed Streamlit implicitly listened on all
        # interfaces. Preserve that established remote-access contract for an
        # existing user config; fresh installs use the loopback-only default.
        cfg["UI"]["DASHBOARD_BIND_ADDRESS"] = "0.0.0.0"
    return cfg


def _audit_log_path() -> str:
    """Bounded config audit trail. Resolved via core.paths if available."""
    try:
        from core.paths import LOGS_DIR
        return os.path.join(str(LOGS_DIR), "config_audit.jsonl")
    except Exception:
        return os.path.join(PROJECT_ROOT, "logs", "config_audit.jsonl")


def _config_diff(old: dict, new: dict) -> dict:
    """Per-bot {key: [old, new]} for changed/added/removed keys. Best-effort."""
    diff: dict = {}
    keys = set(old or {}) | set(new or {})
    for bot in keys:
        o = (old or {}).get(bot, {})
        n = (new or {}).get(bot, {})
        if not isinstance(o, dict) or not isinstance(n, dict):
            if o != n:
                diff[bot] = {"_value": [o, n]}
            continue
        sub: dict = {}
        for k in set(o) | set(n):
            ov = o.get(k, "__absent__")
            nv = n.get(k, "__absent__")
            if ov != nv:
                sub[k] = [ov, nv]
        if sub:
            diff[bot] = sub
    return diff


def _read_config_for_audit() -> dict:
    try:
        if os.path.exists(CONFIG_FILE):
            data = _read_launcher_config_json(CONFIG_FILE)
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


_CONFIG_AUDIT_LOCK = threading.Lock()
_CONFIG_AUDIT_SECRET_KEY_PARTS = (
    "apikey",
    "apisecret",
    "secret",
    "password",
    "passphrase",
    "token",
    "credential",
    "privatekey",
    "accesskey",
    "authkey",
    "authorization",
    "signature",
    "chatid",
    "proxyurl",
    "proxyuser",
    "proxypass",
)


def _scrub_config_audit_structure(value):
    if isinstance(value, dict):
        scrubbed = {}
        for key, item in value.items():
            normalized = "".join(
                char for char in str(key).lower() if char.isalnum()
            )
            if any(part in normalized for part in _CONFIG_AUDIT_SECRET_KEY_PARTS):
                scrubbed[key] = "***REDACTED***"
            else:
                scrubbed[key] = _scrub_config_audit_structure(item)
        return scrubbed
    if isinstance(value, (list, tuple)):
        return [_scrub_config_audit_structure(item) for item in value]
    return value


def _redact_config_audit_fields(fields: dict) -> tuple[dict, bool]:
    """Return redacted fields, dropping payload data if redaction is unsafe."""
    try:
        from core.logger import redact
        structurally_scrubbed = _scrub_config_audit_structure(fields)
        redacted = json.loads(
            redact(json.dumps(
                structurally_scrubbed,
                ensure_ascii=False,
                allow_nan=False,
            ))
        )
        if not isinstance(redacted, dict):
            raise ValueError("redacted audit fields are not an object")
        return redacted, False
    except Exception:
        # Audit metadata is still useful, but raw config/event fields may
        # contain API keys, tokens or proxy credentials and must never be the
        # fallback when the sanitizer is unavailable.
        return {}, True


def _append_config_audit(record: dict) -> None:
    """Rotate and append one audit record. Best-effort; never raises."""
    try:
        path = _audit_log_path()
        line = json.dumps(
            record,
            ensure_ascii=False,
            allow_nan=False,
        ) + "\n"
        try:
            from core.logger import _append_rotating_text
        except Exception:
            _append_rotating_text = None
        with _CONFIG_AUDIT_LOCK:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if _append_rotating_text is not None:
                _append_rotating_text(
                    path,
                    line,
                    CONFIG_AUDIT_MAX_BYTES,
                    CONFIG_AUDIT_BACKUPS,
                    jsonl=True,
                )
    except Exception:
        pass


def _write_config_audit(new_cfg: dict, previous_cfg: dict | None = None) -> None:
    """Append a compact record of what changed vs the previous on-disk config.

    Best-effort and bounded; never raises, never blocks save_config.
    Secret-looking values are redacted before writing.
    """
    try:
        prev = previous_cfg if isinstance(previous_cfg, dict) else _read_config_for_audit()
        diff = _config_diff(prev, new_cfg)
        if not diff:
            return
        try:
            from core.clock import now_utc
            ts = now_utc().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        diff, redaction_failed = _redact_config_audit_fields(diff)
        record = {"ts": ts, "source": "save_config", "changes": diff}
        if redaction_failed:
            record["redaction_failed"] = True
        _append_config_audit(record)
    except Exception:
        pass


def audit_event(source: str, **fields) -> None:
    """Append a non-diff audit record (e.g. a restart applying new params).

    Best-effort, bounded, redacted; never raises.
    """
    try:
        try:
            from core.clock import now_utc
            ts = now_utc().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        fields, redaction_failed = _redact_config_audit_fields(fields)
        # Caller fields are context only; canonical audit metadata must not be
        # replaceable by an accidental or untrusted ``ts``/``source`` key.
        record = {**fields, "ts": ts, "source": source}
        if redaction_failed:
            record["redaction_failed"] = True
        _append_config_audit(record)
    except Exception:
        pass


_CONFIG_WRITE_LOCK = threading.RLock()
_CONFIG_PROCESS_LOCK = CONFIG_FILE + ".lock"
_CONFIG_PROCESS_LOCK_STATE = threading.local()


def _validated_config_lock_timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("config lock timeout must be a number")
    timeout = float(value)
    if not math.isfinite(timeout) or not 0.0 <= timeout <= 300.0:
        raise ValueError("config lock timeout must be between 0 and 300 seconds")
    return timeout


@contextmanager
def _config_process_lock(timeout_s: float = 10.0):
    """Serialize config read-modify-write across launcher/tool processes."""
    timeout_s = _validated_config_lock_timeout(timeout_s)
    depth = getattr(_CONFIG_PROCESS_LOCK_STATE, "depth", 0)
    if depth:
        _CONFIG_PROCESS_LOCK_STATE.depth = depth + 1
        try:
            yield
        finally:
            _CONFIG_PROCESS_LOCK_STATE.depth = depth
        return

    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    fh = open(_CONFIG_PROCESS_LOCK, "a+b")
    locked = False
    try:
        if os.name == "nt":
            import msvcrt
            if fh.seek(0, os.SEEK_END) == 0:
                fh.write(b"\0")
                fh.flush()
            deadline = time.monotonic() + timeout_s
            while True:
                try:
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    locked = True
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Timed out waiting for bot_config.json lock")
                    time.sleep(0.05)
        else:
            import fcntl
            deadline = time.monotonic() + timeout_s
            while True:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Timed out waiting for bot_config.json lock")
                    time.sleep(0.05)
        _CONFIG_PROCESS_LOCK_STATE.depth = 1
        yield
    finally:
        _CONFIG_PROCESS_LOCK_STATE.depth = 0
        try:
            if locked:
                try:
                    if os.name == "nt":
                        import msvcrt
                        fh.seek(0)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            fh.close()


def save_config(cfg: dict) -> None:
    """Atomically save ``cfg`` to ``bot_config.json``.

    The tmp filename includes pid + thread-id so two concurrent
    save_config() calls (UI double-click, racing restart paths) can't
    clobber each other's tmp file before ``os.replace``. Failure is
    logged (rate-limited) rather than silently dropped. A compact diff
    vs the previous on-disk config is appended to logs/config_audit.jsonl
    after the atomic replace succeeds. The previous snapshot is captured
    before the overwrite so the diff remains accurate.
    """
    with _CONFIG_WRITE_LOCK:
        with _config_process_lock():
            _save_config_unlocked(cfg)


def validate_config_for_save(cfg: dict) -> None:
    """Reject configs that would make bot pre-start validation fail.

    This guards UI/optimizer writes before they hit bot_config.json, so a bad
    parameter combination cannot strand the next launcher/bot start.
    """
    try:
        from core.pre_start_check import _check_config, format_issues, has_errors
        issues = _check_config(None, cfg, BOT_META)
        if has_errors(issues):
            lines = format_issues(i for i in issues if i.severity == "error")
            raise ValueError("; ".join(lines))
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"config validation failed: {exc}") from exc

def _save_config_unlocked(cfg: dict) -> None:
    previous_cfg = _read_config_for_audit()
    try:
        tmp = f"{CONFIG_FILE}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, allow_nan=False)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        # Windows retries: target may be open by the launcher poller.
        last_err = None
        for _ in range(8):
            try:
                os.replace(tmp, CONFIG_FILE)
                last_err = None
                break
            except PermissionError as pe:
                last_err = pe
                time.sleep(0.05)
        if last_err is not None:
            # Clean up the leftover tmp before propagating
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise last_err
        _write_config_audit(cfg, previous_cfg=previous_cfg)
    except Exception as e:
        try:
            from bot_utils.silent_log import silent_log
            silent_log("save_config", e)
        except Exception:
            sys.stderr.write(f"[save_config] {type(e).__name__}: {e}\n")
        # Final cleanup attempt for the per-writer tmp
        try:
            if 'tmp' in locals() and os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        raise


def save_config_merge(section_updates: dict | None = None,
                      section_replacements: dict | None = None) -> dict:
    """Merge selected config sections into the latest on-disk config.

    Use this from the launcher UI when saving a bot's parameter edits or UI
    preferences. It avoids writing a stale full-memory snapshot over newer
    values changed by hotfixes, another UI action, or a running bot.
    """
    section_updates = section_updates or {}
    section_replacements = section_replacements or {}
    with _CONFIG_WRITE_LOCK:
        with _config_process_lock():
            cfg = load_config()
            for section, value in section_replacements.items():
                cfg[section] = dict(value) if isinstance(value, dict) else value
            for section, values in section_updates.items():
                if isinstance(values, dict):
                    cur = cfg.get(section)
                    if not isinstance(cur, dict):
                        cur = {}
                    cur.update(values)
                    cfg[section] = cur
                else:
                    cfg[section] = values
            validate_config_for_save(cfg)
            _save_config_unlocked(cfg)
            return cfg


#  Misc shared subprocess kwargs 

def subprocess_no_window_kwargs() -> dict:
    """Return ``creationflags`` kwargs so a child process opens no console
    window on Windows. Empty dict on other platforms."""
    if sys.platform == "win32":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        return {
            "creationflags": subprocess.CREATE_NO_WINDOW,
            "startupinfo": startupinfo,
        }
    return {}
