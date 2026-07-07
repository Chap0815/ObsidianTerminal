"""
dashboard.py  Obsidian Trading Terminal v5.0
Comprehensive analytics for Spot + Futures bots

Tabs:
  Overview  KPIs, equity curve, drawdown, live positions
  Trades  Filterable trade journal with analytics
  Positions  Live spot & futures positions (with liquidation info)
  Performance  Sharpe, Sortino, Profit Factor, heatmaps, hour/weekday grids
  Bots  Side-by-side comparison, parameter timeline, learning log
"""

import os
import json
import sqlite3
import math
import html
from datetime import datetime, timedelta, timezone

import streamlit as st

from bot_utils.pnl_view import (
    futures_state_age_sec,
    futures_unrealized_from_row,
    is_futures_state_fresh,
    spot_unrealized_pnl,
)
import pandas as pd
import numpy as np
import plotly.graph_objects as go

#  Page Setup 
st.set_page_config(
    page_title="Obsidian Terminal",
    layout="wide",
    page_icon="",
    initial_sidebar_state="expanded"
)

# Resolve DB and log paths via core.paths (single source of truth), NOT the
# cwd. Streamlit may run the dashboard from arbitrary working directories
# (system service, launcher process group, etc); a relative "trading_bot.db"
# would silently create an empty DB next to wherever streamlit was launched
# from, and every "no trades yet" reading would look like a working DB.
import sys as _sys
_DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_DASHBOARD_DIR)  # tools/  project root
if _PROJECT_ROOT not in _sys.path:
    _sys.path.insert(0, _PROJECT_ROOT)
try:
    from core.paths import (
        DB_PATH_STR as DB_PATH,
        LOG_DIR_SPOT, LOG_DIR_TREND, LOG_DIR_FUTURES, LOG_DIR_CROSS,
        LOG_DIR_FUTREND,
    )
    LOG_DIRS = {
        "TREND":   str(LOG_DIR_TREND),
        "SPOT": str(LOG_DIR_SPOT),
        "FUTURES":    str(LOG_DIR_FUTURES),
        "CROSS":   str(LOG_DIR_CROSS),
        "FUTREND": str(LOG_DIR_FUTREND),
    }
except Exception:
    # Fallback (should never fire in production)
    DB_PATH = os.path.join(_PROJECT_ROOT, "data", "trading_bot.db")
    LOG_DIRS = {
        "TREND":   os.path.join(_PROJECT_ROOT, "logs", "Trend"),
        "SPOT": os.path.join(_PROJECT_ROOT, "logs", "Spot"),
        "FUTURES":    os.path.join(_PROJECT_ROOT, "logs", "Futures"),
        "CROSS":   os.path.join(_PROJECT_ROOT, "logs", "Cross"),
        "FUTREND": os.path.join(_PROJECT_ROOT, "logs", "FuTrend"),
    }
try:
    from core.database import get_local_today_str, utc_to_local_date_str
except Exception:
    def get_local_today_str():
        return datetime.now().strftime("%Y-%m-%d")

    def utc_to_local_date_str(value):
        try:
            return pd.to_datetime(value, errors="coerce").strftime("%Y-%m-%d")
        except Exception:
            return None

_SIGNATURE = "#6a5cc0"   # Obsidian signature cyan (from the logo arrow)
BOT_ACCENTS = {
    # Monochrome: one signature accent for all bots (no per-bot rainbow);
    # identity is carried by the name, not a colour. Matches the launcher.
    "TREND":   _SIGNATURE,
    "SPOT":    _SIGNATURE,
    "FUTURES": _SIGNATURE,
    "CROSS":   _SIGNATURE,
    "FUTREND": _SIGNATURE,
}

#  SIM / LIVE mode detection
# Closed trades are trusted only when the row itself carries is_sim or an
# explicit "(SIM)" suffix. Old raw rows stay LEGACY so current bot_config mode
# changes cannot re-label historical paper money as live profit.
_BOT_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "bot_config.json")

# Canonical bot list  single source so every view (modes, filters, side-by-side
# comparison, parameter timeline, learning log) covers all bots.
ALL_BOTS = ["TREND", "SPOT", "FUTURES", "CROSS", "FUTREND"]
MODE_NAMESPACE_CUTOVER_UTC = pd.Timestamp("2026-06-18 00:00:00", tz="UTC")


def _load_bot_modes() -> dict:
    """Return {bot_name: 'LIVE'|'SIM'} from bot_config.json. Defaults to SIM
    (safer: never claim paper gains are real money without confirmation)."""
    modes = {b: "SIM" for b in ALL_BOTS}
    # Env override wins (comma-separated list of LIVE bots)
    env_live = os.getenv("BOT_LIVE_MODES", "").strip()
    if env_live:
        live = {b.strip().upper() for b in env_live.split(",") if b.strip()}
        for b in modes:
            modes[b] = "LIVE" if b in live else "SIM"
        return modes
    try:
        with open(_BOT_CONFIG_PATH, "r", encoding="utf-8-sig") as f:
            cfg = json.load(f)
        for bot in modes:
            # config may be nested per-bot or flat; handle both
            bot_cfg = cfg.get(bot, cfg) if isinstance(cfg, dict) else {}
            sim = bot_cfg.get("SIMULATION_MODE",
                              bot_cfg.get("SIMULATION",
                                          bot_cfg.get("simulation_mode")))
            if sim is None:
                continue
            # truthy SIM flag  SIM, else LIVE
            is_sim = str(sim).strip().lower() in ("1", "true", "yes", "on")
            modes[bot] = "SIM" if is_sim else "LIVE"
    except Exception:
        pass
    return modes


BOT_MODES = _load_bot_modes()


def _base_bot_name(bot_name: str) -> str:
    return str(bot_name or "").replace(" (SIM)", "").upper()


def _is_post_namespace_cutover(value) -> bool:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return False
    return bool(ts >= MODE_NAMESPACE_CUTOVER_UTC)


def _mode_for_historical_row(row_or_name, sell_time=None) -> str:
    if isinstance(row_or_name, pd.Series):
        bot_name = row_or_name.get("bot_name")
        sell_time = row_or_name.get("sell_time", row_or_name.get("buy_time"))
    else:
        bot_name = row_or_name
    name = str(bot_name or "").upper()
    if name.endswith(" (SIM)"):
        return "SIM"
    base = _base_bot_name(name)
    if base in ALL_BOTS:
        if _is_post_namespace_cutover(sell_time):
            return "LIVE"
        return "LEGACY"
    return BOT_MODES.get(base, "SIM")


def _mode_for_current_row(bot_name: str) -> str:
    name = str(bot_name or "").upper()
    if name.endswith(" (SIM)"):
        return "SIM"
    base = _base_bot_name(name)
    if base in ALL_BOTS and name == base:
        return "LIVE"
    return BOT_MODES.get(base, "SIM")


def _mode_badge(mode: str) -> str:
    mode = str(mode or "SIM").upper()
    if mode == "LIVE":
        color, label = "#22c55e", "LIVE"
    elif mode == "LEGACY":
        color, label = "#94a3b8", "LEGACY"
    else:
        color, label = "#f59e0b", "SIM"
    return (
        f'<span style="background:{color}22; color:{color}; '
        'font-size:0.6rem; padding:2px 7px; border-radius:4px; '
        'margin-left:8px; font-weight:800; text-transform:uppercase; '
        f'letter-spacing:0.06em;">{label}</span>'
    )

BOT_SUBTITLES = {
    "TREND":  "Spot  Risk-Aware",
    "SPOT": "Spot  Momentum",
    "FUTURES":  "Perpetuals  Long/Short",
    "CROSS":  "Perpetuals  Market-Neutral",
    "FUTREND": "Perpetuals  Trend-Following",
}


#  CSS 
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600;700;800&display=swap');

.stApp {
    background:
        radial-gradient(ellipse 80% 60% at 15% 0%, rgba(106,92,192,0.06) 0%, transparent 60%),
        radial-gradient(ellipse 80% 60% at 85% 0%, rgba(79,180,192,0.04) 0%, transparent 60%),
        radial-gradient(ellipse 60% 50% at 50% 100%, rgba(106,92,192,0.03) 0%, transparent 60%),
        #0a0810 !important;
    color: #e9e7f2;
    font-family: 'Inter', sans-serif;
}
header[data-testid="stHeader"]   { display: none !important; }
[data-testid="stStatusWidget"]   { display: none !important; }
[data-stale="true"]              { opacity: 1 !important; filter: none !important; }
section[data-testid="stSidebar"] {
    background: rgba(8,11,16,0.85) !important;
    border-right: 1px solid rgba(255,255,255,0.05);
    backdrop-filter: blur(20px);
}

.kpi-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    margin-bottom: 6px;
    font-weight: 600;
}
.kpi-value {
    font-size: 2.1rem;
    font-weight: 800;
    line-height: 1.1;
    letter-spacing: -0.02em;
}
.kpi-sub {
    font-size: 0.75rem;
    color: #64748b;
    margin-top: 6px;
    font-weight: 500;
}
.pos { color: #22c55e; }
.neg { color: #ef4444; }
.neu { color: #94a3b8; }

.stTabs [data-baseweb="tab-list"] {
    background: rgba(15,20,29,0.6);
    backdrop-filter: blur(12px);
    gap: 4px;
    border-radius: 12px;
    padding: 6px;
    border: 1px solid rgba(255,255,255,0.05);
    margin-bottom: 24px;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    border-radius: 8px;
    color: #64748b;
    padding: 10px 22px;
    font-weight: 600;
    font-size: 0.85rem;
    transition: all 0.2s ease;
}
.stTabs [data-baseweb="tab"]:hover {
    background: rgba(255,255,255,0.04);
    color: #cbd5e1;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, rgba(106,92,192,0.18), rgba(79,180,192,0.12)) !important;
    color: #b3a8e8 !important;
    box-shadow: 0 0 0 1px rgba(106,92,192,0.25), 0 4px 16px rgba(106,92,192,0.15);
}

div[data-testid="metric-container"] {
    background: rgba(15,20,29,0.6);
    backdrop-filter: blur(12px);
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,0.06);
    padding: 16px 20px;
}

.stButton > button {
    background: linear-gradient(135deg, rgba(106,92,192,0.18), rgba(106,92,192,0.12));
    border: 1px solid rgba(106,92,192,0.3);
    color: #b3a8e8;
    border-radius: 10px;
    font-weight: 600;
    transition: all 0.15s ease;
}
.stButton > button:hover {
    background: linear-gradient(135deg, rgba(106,92,192,0.25), rgba(106,92,192,0.18));
    border-color: rgba(106,92,192,0.5);
    color: #ffffff;
}

.streamlit-expanderHeader {
    background: rgba(15,20,29,0.5);
    border-radius: 10px;
    border: 1px solid rgba(255,255,255,0.05);
    font-weight: 600;
}

.stDataFrame { border-radius: 12px; overflow: hidden; }
h1, h2, h3 { letter-spacing: -0.02em; }

::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: rgba(0,0,0,0.2); }
::-webkit-scrollbar-thumb { background: rgba(106,92,192,0.3); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: rgba(106,92,192,0.5); }

.section-title {
    font-size: 0.7rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    font-weight: 700;
    margin: 24px 0 12px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}

/*  DataFrame / Tabellen Dark-Theme  */
[data-testid="stDataFrame"],
[data-testid="stDataFrame"] > div,
[data-testid="stTable"],
[data-testid="stTable"] > div {
    background: rgba(12,17,26,0.9) !important;
    border-radius: 12px !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
}
[data-testid="stDataFrame"] table,
[data-testid="stTable"] table {
    background: transparent !important;
    color: #cbd5e1 !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.78rem !important;
}
[data-testid="stDataFrame"] thead tr,
[data-testid="stTable"] thead tr {
    background: rgba(15,20,29,0.9) !important;
}
[data-testid="stDataFrame"] thead th,
[data-testid="stTable"] thead th {
    background: rgba(15,20,29,0.9) !important;
    color: #94a3b8 !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    font-size: 0.66rem !important;
    border-bottom: 1px solid rgba(255,255,255,0.08) !important;
}
[data-testid="stDataFrame"] tbody tr:nth-child(even),
[data-testid="stTable"] tbody tr:nth-child(even) {
    background: rgba(255,255,255,0.015) !important;
}
[data-testid="stDataFrame"] tbody tr:hover,
[data-testid="stTable"] tbody tr:hover {
    background: rgba(106,92,192,0.06) !important;
}
[data-testid="stDataFrame"] tbody td,
[data-testid="stTable"] tbody td {
    border-bottom: 1px solid rgba(255,255,255,0.03) !important;
    color: #cbd5e1 !important;
}
/* Pandas-Styler Cell-Hintergrnde (z.B. row coloring) abdunkeln */
[data-testid="stDataFrame"] tbody td[style*="background-color"] {
    color: #e2e8f0 !important;
}
/* Scrollbar in Dataframes  dezenter */
[data-testid="stDataFrame"] ::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}
[data-testid="stDataFrame"] ::-webkit-scrollbar-thumb {
    background: rgba(106,92,192,0.3);
    border-radius: 4px;
}

/* Multiselect-Dropdowns dunkel */
[data-baseweb="select"] {
    background: rgba(15,20,29,0.6) !important;
}
[data-baseweb="select"] > div {
    background: rgba(15,20,29,0.6) !important;
    border-color: rgba(255,255,255,0.08) !important;
    color: #cbd5e1 !important;
}
/* Tags in Multiselect (z.B. "TREND x")  eigene Farben statt Streamlit-Rot */
[data-baseweb="tag"] {
    background: rgba(106,92,192,0.18) !important;
    border: 1px solid rgba(106,92,192,0.4) !important;
    color: #b3a8e8 !important;
}

/* Streamlit info/success/warning Boxen */
[data-testid="stAlert"] {
    background: rgba(12,17,26,0.9) !important;
    border-radius: 10px !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    color: #cbd5e1 !important;
}

/* Tab-Container border-line dezenter */
[data-baseweb="tab-list"] {
    border-bottom: 1px solid rgba(255,255,255,0.06) !important;
}
[data-baseweb="tab"] {
    color: #64748b !important;
}
[data-baseweb="tab"][aria-selected="true"] {
    color: #b3a8e8 !important;
}
</style>
""", unsafe_allow_html=True)


#  HTML-Helfer 

def _card(content: str, border_color: str = "rgba(255,255,255,0.07)") -> str:
    return (
        f'<div style="background:rgba(12,17,26,0.9); border-radius:14px; '
        f'border:1px solid rgba(255,255,255,0.07); '
        f'border-left:4px solid {border_color}; '
        f'padding:20px; margin-bottom:14px;">'
        f'{content}</div>'
    )


def _render_dark_table(df, max_rows: int = 200, height_px: int = None,
                        align_right_cols: list = None,
                        color_cols: dict = None) -> None:
    """
    Rendert ein DataFrame als reine HTML-Tabelle (umgeht das Streamlit-Glide-
    Data-Grid Canvas-Rendering, damit unser Dark-Theme greift).

    Parameters:
        df  pandas DataFrame oder Series
        max_rows  clipping limit
        height_px  optional scrollable container height
        align_right_cols  column names to align right
        color_cols  dict {col_name: 'pnl'} for color-coded cells
    """
    import html as _html
    if df is None or len(df) == 0:
        st.markdown(
            '<div style="background:rgba(12,17,26,0.9); border-radius:10px; '
            'border:1px dashed rgba(255,255,255,0.08); padding:30px; '
            'text-align:center; color:#64748b; font-size:0.85rem;">'
            'No data to display.'
            '</div>',
            unsafe_allow_html=True
        )
        return

    df = df.head(max_rows)
    align_right_cols = set(align_right_cols or [])
    color_cols = color_cols or {}

    # Header
    th_html = "".join(
        f'<th style="text-align:{"right" if c in align_right_cols else "left"};">'
        f'{_html.escape(str(c))}</th>'
        for c in df.columns
    )

    # Container styling
    container_style = (
        "background:rgba(12,17,26,0.9); border-radius:12px; "
        "border:1px solid rgba(255,255,255,0.07); "
        "overflow:hidden;"
    )
    if height_px:
        container_style += f" max-height:{height_px}px; overflow-y:auto;"

    table_html = (
        f'<div style="{container_style}">'
        '<table style="width:100%; border-collapse:collapse; '
        'font-family:JetBrains Mono, monospace; font-size:0.76rem; '
        'color:#cbd5e1;">'
        '<thead>'
        '<tr style="background:rgba(15,20,29,0.95);">'
        + "".join(
            f'<th style="padding:10px 14px; text-align:left; '
            f'color:#94a3b8; font-weight:700; font-size:0.66rem; '
            f'text-transform:uppercase; letter-spacing:0.08em; '
            f'border-bottom:1px solid rgba(255,255,255,0.08);">'
            f'{_html.escape(str(c))}</th>'
            for c in df.columns
        )
        + '</tr></thead>'
        '<tbody>'
    )

    # Striped Body
    for i, idx in enumerate(df.index):
        zebra = "background:rgba(255,255,255,0.015);" if i % 2 == 1 else ""
        row_html = f'<tr style="{zebra}">'
        for c in df.columns:
            v = df.loc[idx, c]
            try:
                display = "" if pd.isna(v) else str(v)
            except (TypeError, ValueError):
                display = "" if v is None else str(v)
            extra_style = ""
            if c in color_cols and color_cols[c] == "pnl":
                try:
                    s = display.replace("+", "").replace("%", "").replace(",", "").replace(" USDT", "")
                    num = float(s)
                    if num > 0:
                        extra_style = "color:#10b981; font-weight:700;"
                    elif num < 0:
                        extra_style = "color:#ef4444; font-weight:700;"
                except (ValueError, TypeError):
                    pass
            align = "right" if c in align_right_cols else "left"
            row_html += (
                f'<td style="padding:8px 14px; text-align:{align}; '
                f'border-bottom:1px solid rgba(255,255,255,0.03); '
                f'{extra_style}">{_html.escape(display)}</td>'
            )
        row_html += '</tr>'
        table_html += row_html

    table_html += '</tbody></table></div>'
    st.markdown(table_html, unsafe_allow_html=True)


def _kpi_card(label: str, value, sub: str, accent_color: str = "#475569",
                value_fmt: str = ".2f", suffix: str = "USDT",
                sign: bool = True) -> str:
    if isinstance(value, (int, float)):
        if sign:
            css = "pos" if value >= 0 else "neg"
            sign_char = "+" if value >= 0 else ""
        else:
            css = "neu"
            sign_char = ""
        value_str = f"{sign_char}{value:{value_fmt}}"
    else:
        css = "neu"
        value_str = str(value)
    label_html = html.escape(str(label))
    value_html = html.escape(str(value_str))
    suffix_html = html.escape(str(suffix))
    sub_html = html.escape(str(sub))
    return (
        f'<div style="position:relative; background:rgba(15,20,29,0.6); '
        f'backdrop-filter:blur(12px); border-radius:14px; '
        f'border:1px solid rgba(255,255,255,0.06); padding:20px 22px; '
        f'overflow:hidden;">'
        f'  <div style="position:absolute; top:0; left:0; right:0; height:2px; '
        f'background:linear-gradient(90deg, {accent_color}, transparent);"></div>'
        f'  <div class="kpi-label" style="color:{accent_color};">{label_html}</div>'
        f'  <div class="kpi-value {css}">{value_html} <span style="font-size:0.85rem; color:#475569; font-weight:600;">{suffix_html}</span></div>'
        f'  <div class="kpi-sub">{sub_html}</div>'
        f'</div>'
    )


def _section(text: str):
    st.markdown(
        f'<div class="section-title">{html.escape(str(text))}</div>',
        unsafe_allow_html=True,
    )


def _futures_row_unrealized(row) -> float:
    """Use stored futures unrealized PnL, with raw-field fallback.

    Fresh state rows can have current_price updated before unrealized_pnl is
    populated. The launcher already uses this fallback; dashboard/reporting
    must match it to avoid showing a false zero MTM.
    """
    return futures_unrealized_from_row(row)[0]


def _futures_row_unrealized_pct(row) -> float:
    return futures_unrealized_from_row(row)[1]


#  Daten-Loader 

def _ro_connect():
    """Read-only-ish SQLite connection with a busy_timeout.

    With multiple bots writing to the WAL DB, a heavy write burst can raise
    ``database is locked``, which the loaders' ``except`` would swallow by
    returning an EMPTY DataFrame (the dashboard then flashes to "no trades yet"
    / zeros for a cache cycle). A 20s busy_timeout (matching metrics_service and
    core.database) makes the read wait out the write instead of failing.
    """
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    try:
        conn.execute("PRAGMA busy_timeout=20000")
    except Exception:
        pass
    return conn


@st.cache_data(ttl=5, show_spinner=False)
def load_all_trades() -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = _ro_connect()
        df = pd.read_sql_query(
            # is_partial=1 trades are partial-close events (Futures TP at 50%).
            # They represent REAL realized P&L and are included in the total.
            # Win-rate uses is_partial=0 separately (see _compute_stats) to
            # avoid double-counting wins on the same position.
            "SELECT * FROM trades ORDER BY sell_time DESC", conn
        )
        conn.close()
        if df.empty:
            return df
        df["buy_time"]  = pd.to_datetime(df["buy_time"],  errors="coerce", utc=True)
        df["sell_time"] = pd.to_datetime(df["sell_time"], errors="coerce", utc=True)
        df["is_futures"] = df.get("is_futures", 0).fillna(0).astype(int)
        df["base_bot"] = df["bot_name"].map(_base_bot_name)
        # Tag closed trades by row evidence, not current bot_config. Historical
        # raw bot names cannot be safely relabelled after SIM/LIVE mode changes.
        if "is_sim" in df.columns:
            row_modes = df["is_sim"].map(
                lambda v: None if pd.isna(v) else ("SIM" if int(v) == 1 else "LIVE")
            )
            fallback_modes = df.apply(_mode_for_historical_row, axis=1)
            df["mode"] = row_modes.where(row_modes.notna(), fallback_modes)
        else:
            df["mode"] = df.apply(_mode_for_historical_row, axis=1)
        return df
    except Exception as exc:
        st.warning(f"Trades konnten nicht geladen werden: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=5, show_spinner=False)
def load_futures_live() -> pd.DataFrame:
    def _json_futures_rows(existing: set[tuple[str, str, str]]) -> list[dict]:
        from bot_utils.sim_flag import sim_state_path

        rows: list[dict] = []
        for bot in ("FUTURES", "CROSS", "FUTREND"):
            base_path = os.path.join(LOG_DIRS[bot], "trades.json")
            for mode, path in (
                ("LIVE", sim_state_path(base_path, False)),
                ("SIM", sim_state_path(base_path, True)),
            ):
                try:
                    if not os.path.exists(path):
                        continue
                    with open(path, "r", encoding="utf-8-sig") as fh:
                        state = json.load(fh) or {}
                    if not isinstance(state, dict):
                        continue
                except Exception:
                    continue
                for sym, d in state.items():
                    if not isinstance(d, dict):
                        continue
                    if str(d.get("state", "OPEN")).upper() == "CLOSED":
                        continue
                    key = (bot, mode, str(sym).upper())
                    if key in existing:
                        continue
                    try:
                        entry = float(d.get("buy_price") or d.get("buy") or 0)
                        current = float(d.get("last_price") or entry or 0)
                        margin = float(d.get("margin_usdt")
                                       or d.get("invested_usdt") or 0)
                        leverage = float(d.get("leverage") or 1)
                    except (TypeError, ValueError):
                        continue
                    if entry <= 0 or margin <= 0:
                        continue
                    bot_name = bot if mode == "LIVE" else f"{bot} (SIM)"
                    rows.append({
                        "bot_name": bot_name,
                        "base_bot": bot,
                        "mode": mode,
                        "symbol": str(sym).upper(),
                        "position_type": d.get("position_type", "LONG"),
                        "entry_price": entry,
                        "current_price": current,
                        "margin_usdt": margin,
                        "leverage": leverage,
                        "unrealized_pnl": 0.0,
                        "unrealized_pct": 0.0,
                        "liquidation_price": float(
                            d.get("liquidation_price", 0) or 0),
                        "last_update": d.get("last_update", ""),
                        "opened_at": d.get("buy_time") or d.get("opened_at"),
                        "state_is_stale": True,
                        "state_age_sec": None,
                        "_state_source": "json_state",
                    })
        return rows

    existing: set[tuple[str, str, str]] = set()
    frames: list[pd.DataFrame] = []
    if not os.path.exists(DB_PATH):
        db_error = None
    else:
        try:
            conn = _ro_connect()
            df = pd.read_sql_query("SELECT * FROM futures_state", conn)
            conn.close()
            if not df.empty and "bot_name" in df.columns:
                df["base_bot"] = df["bot_name"].map(_base_bot_name)
                df["mode"] = df["bot_name"].map(_mode_for_current_row)
                df["unrealized_pnl"] = df.apply(_futures_row_unrealized, axis=1)
                df["unrealized_pct"] = df.apply(_futures_row_unrealized_pct, axis=1)
                df["state_is_stale"] = df.apply(
                    lambda row: not is_futures_state_fresh(row), axis=1)
                df["state_age_sec"] = df.apply(futures_state_age_sec, axis=1)
                for _, row in df.iterrows():
                    existing.add((
                        str(row.get("base_bot") or _base_bot_name(row.get("bot_name", ""))).upper(),
                        str(row.get("mode") or _mode_for_current_row(row.get("bot_name", ""))).upper(),
                        str(row.get("symbol") or "").upper(),
                    ))
            frames.append(df)
            db_error = None
        except Exception as exc:
            st.warning(f"Futures-Status konnte nicht geladen werden: {type(exc).__name__}: {exc}")
            db_error = exc
    json_rows = _json_futures_rows(existing)
    if json_rows:
        frames.append(pd.DataFrame(json_rows))
    if frames:
        return pd.concat(frames, ignore_index=True, sort=False)
    if db_error is not None:
        return pd.DataFrame()
    return pd.DataFrame()


@st.cache_data(ttl=5, show_spinner=False)
def load_open_spot_trades() -> list:
    from bot_utils.sim_flag import sim_state_path

    rows = []
    for bot in ("TREND", "SPOT"):
        live_path = f"{LOG_DIRS[bot]}/trades.json"
        for mode, path in (
            ("LIVE", sim_state_path(live_path, False)),
            ("SIM", sim_state_path(live_path, True)),
        ):
            if not os.path.exists(path):
                continue
            try:
                with open(path, encoding="utf-8") as f:
                    trades = json.load(f) or {}
                for sym, d in trades.items():
                    if not isinstance(d, dict):
                        continue
                    if str(d.get("state", "OPEN")).upper() == "CLOSED":
                        continue
                    try:
                        amount = float(d.get("amount", 0) or 0)
                        buy = float(d.get("buy_price") or d.get("buy") or 0)
                    except (TypeError, ValueError):
                        continue
                    if amount <= 0 or buy <= 0:
                        continue
                    rows.append({
                        "bot": bot,
                        "mode": mode,
                        "symbol": sym,
                        **d,
                    })
            except Exception:
                continue
    return rows


@st.cache_resource(show_spinner=False)
def _dash_spot_exchange():
    """Persistent (cross-rerun) spot connection to the CONFIGURED exchange
    (EXCHANGE in .env). Built once via cache_resource so markets aren't reloaded
    every refresh. Inherits the recvWindow + clock-skew self-heal from
    exchange_config, so it works on every supported venue behind a proxy/GFW."""
    from config.exchange_config import get_spot_exchange_connection
    ex = get_spot_exchange_connection()
    ex.timeout = 6000
    try:
        ex.load_markets()
    except Exception:
        pass
    return ex


@st.cache_data(ttl=3, show_spinner=False)
def get_live_prices(symbols: tuple[str, ...] = ()) -> dict:
    """Live spot last-prices from the configured exchange, keyed as
    '<BASE>USDT' to match the position cards. Exchange-agnostic (ccxt)  works
    on all supported venues. Only requested open-position symbols are fetched;
    a dashboard refresh must not pull the full spot market."""
    wanted = tuple(sorted({str(s).upper().replace("/USDT", "").replace("USDT", "")
                           for s in symbols if str(s).strip()}))
    if not wanted:
        return {}
    try:
        ex = _dash_spot_exchange()
        pairs = [f"{base}/USDT" for base in wanted]
        tickers = ex.fetch_tickers(pairs) or {}
        out: dict = {}
        for sym, t in tickers.items():
            # Spot USDT pairs only ('BTC/USDT'); skip perps ('BTC/USDT:USDT').
            if ":" in sym or not sym.endswith("/USDT"):
                continue
            base = sym.split("/")[0].upper()
            if base not in wanted:
                continue
            price = t.get("last") or t.get("close")
            if price:
                try:
                    out[f"{base}USDT"] = float(price)
                except (TypeError, ValueError):
                    pass
        for base in wanted:
            if f"{base}USDT" in out:
                continue
            try:
                t = ex.fetch_ticker(f"{base}/USDT") or {}
                price = t.get("last") or t.get("close")
                if price:
                    out[f"{base}USDT"] = float(price)
            except Exception:
                continue
        return out
    except Exception:
        out = {}
        try:
            ex = _dash_spot_exchange()
            for base in wanted:
                try:
                    t = ex.fetch_ticker(f"{base}/USDT") or {}
                    price = t.get("last") or t.get("close")
                    if price:
                        out[f"{base}USDT"] = float(price)
                except Exception:
                    continue
        except Exception:
            pass
        return out


@st.cache_data(ttl=30, show_spinner=False)
def load_bot_params() -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = _ro_connect()
        df = pd.read_sql_query(
            "SELECT bot_name, param_name, param_value, updated_at, reason "
            "FROM bot_params ORDER BY updated_at DESC", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30, show_spinner=False)
def load_learning_log() -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = _ro_connect()
        df = pd.read_sql_query(
            "SELECT * FROM learning_log ORDER BY timestamp DESC LIMIT 200", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_market_regime() -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = _ro_connect()
        df = pd.read_sql_query(
            "SELECT * FROM market_regime ORDER BY timestamp DESC LIMIT 500", conn
        )
        conn.close()
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


#  Risk-Metrics Berechnung 

def aggregate_positions(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Group partial and final close rows into one trading idea.

    Cash PnL is still summed from every row, but winrate/payoff should not count
    a partial TP as an additional independent winning trade.
    """
    if trades_df is None or trades_df.empty:
        return pd.DataFrame(columns=["profit_usdt", "sell_time"])
    df = trades_df.copy()
    defaults = {
        "bot_name": "",
        "symbol": "",
        "buy_time": "",
        "sell_time": "",
        "profit_usdt": 0.0,
        "is_partial": 0,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    df["_position_key"] = (
        df["bot_name"].astype(str) + "|" +
        df["symbol"].astype(str) + "|" +
        df["buy_time"].astype(str)
    )
    grouped = df.groupby("_position_key", dropna=False).agg(
        bot_name=("bot_name", "first"),
        symbol=("symbol", "first"),
        buy_time=("buy_time", "first"),
        sell_time=("sell_time", "max"),
        profit_usdt=("profit_usdt", "sum"),
        fills=("profit_usdt", "size"),
        partial_events=("is_partial", lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
    ).reset_index(drop=True)
    return grouped


def build_pnl_snapshot(
    trades_df: pd.DataFrame,
    open_spot_rows: list,
    open_fut_df: pd.DataFrame,
    live_prices: dict,
    bot_filter: set | None = None,
) -> dict:
    """Single source for dashboard money numbers.

    Realized comes from closed trade rows. Unrealized comes from currently open
    spot/futures state. Net MTM = realized + unrealized.
    """
    def _bucket() -> dict:
        return {
            "realized": 0.0, "unrealized": 0.0, "net": 0.0,
            "open_count": 0, "open_spot": 0, "open_futures": 0,
            "fills": 0, "positions": 0, "price_unavailable": 0,
        }

    modes = {mode: _bucket() for mode in ("LIVE", "SIM", "LEGACY")}
    bots = {
        bot: {
            "mode": BOT_MODES.get(bot, "SIM"),
            **_bucket(),
        }
        for bot in ALL_BOTS
    }
    bot_modes = {
        bot: {mode: _bucket() for mode in ("LIVE", "SIM", "LEGACY")}
        for bot in ALL_BOTS
    }
    filter_active = bot_filter is not None
    bot_filter = set(bot_filter or [])

    if trades_df is not None and not trades_df.empty:
        tdf = trades_df.copy()
        if "mode" not in tdf.columns:
            tdf["mode"] = tdf.apply(_mode_for_historical_row, axis=1)
        else:
            tdf["mode"] = tdf["mode"].fillna("LEGACY").astype(str).str.upper()
        if "base_bot" not in tdf.columns and "bot_name" in tdf.columns:
            tdf["base_bot"] = tdf["bot_name"].map(_base_bot_name)
        if "profit_usdt" not in tdf.columns:
            tdf["profit_usdt"] = 0.0

        for mode, mdf in tdf.groupby("mode"):
            if filter_active:
                bot_col = "base_bot" if "base_bot" in mdf.columns else "bot_name"
                mdf = mdf[mdf[bot_col].map(_base_bot_name).isin(bot_filter)]
                if mdf.empty:
                    continue
            mode = str(mode or "LEGACY").upper()
            if mode not in modes:
                modes[mode] = _bucket()
            modes[mode]["realized"] += float(mdf["profit_usdt"].fillna(0).sum())
            modes[mode]["fills"] += int(len(mdf))
            modes[mode]["positions"] += int(len(aggregate_positions(mdf)))
        bot_col = "base_bot" if "base_bot" in tdf.columns else "bot_name"
        for (bot, mode), bdf in tdf.groupby([bot_col, "mode"]):
            bot = _base_bot_name(bot)
            mode = str(mode or "LEGACY").upper()
            if filter_active and bot not in bot_filter:
                continue
            if bot not in bots:
                continue
            if mode not in bot_modes[bot]:
                bot_modes[bot][mode] = _bucket()
            realized = float(bdf["profit_usdt"].fillna(0).sum())
            fills = int(len(bdf))
            positions = int(len(aggregate_positions(bdf)))
            for bucket in (bots[bot], bot_modes[bot][mode]):
                bucket["realized"] += realized
                bucket["fills"] += fills
                bucket["positions"] += positions

    for _t in open_spot_rows or []:
        bot = _base_bot_name(_t.get("bot_name") or _t.get("bot") or "")
        if filter_active and bot not in bot_filter:
            continue
        mode = str(_t.get("mode", "SIM")).upper()
        mode_bucket = modes.get(mode)
        bot_bucket = bots.get(bot)
        bot_mode_bucket = (
            bot_modes[bot].setdefault(mode, _bucket())
            if bot in bots else None
        )
        try:
            buy = float(_t.get("buy_price") or _t.get("buy") or 0)
            amount = float(_t.get("amount", 0) or 0)
            base = str(_t.get("symbol", "")).upper().replace("/USDT", "").replace("USDT", "")
            live_raw = live_prices.get(f"{base}USDT")
            if live_raw is None:
                for bucket in (mode_bucket, bot_bucket, bot_mode_bucket):
                    if bucket is None:
                        continue
                    bucket["open_count"] += 1
                    bucket["open_spot"] += 1
                    bucket["price_unavailable"] += 1
                continue
            live = float(live_raw)
            unr = spot_unrealized_pnl(buy, live, amount)
        except Exception:
            for bucket in (mode_bucket, bot_bucket, bot_mode_bucket):
                if bucket is None:
                    continue
                bucket["open_count"] += 1
                bucket["open_spot"] += 1
                bucket["price_unavailable"] += 1
            continue
        for bucket in (mode_bucket, bot_bucket, bot_mode_bucket):
            if bucket is None:
                continue
            bucket["unrealized"] += unr
            bucket["open_count"] += 1
            bucket["open_spot"] += 1

    if open_fut_df is not None and not open_fut_df.empty:
        for _, row in open_fut_df.iterrows():
            if bool(row.get("state_is_stale", False)):
                continue
            bot = _base_bot_name(row.get("bot_name", ""))
            if filter_active and bot not in bot_filter:
                continue
            mode = str(row.get("mode", "SIM")).upper()
            try:
                unr = float(row.get("unrealized_pnl", 0) or 0)
            except Exception:
                unr = 0.0
            if mode in modes:
                modes[mode]["unrealized"] += unr
                modes[mode]["open_count"] += 1
                modes[mode]["open_futures"] += 1
            if bot in bots:
                for bucket in (bots[bot], bot_modes[bot].setdefault(mode, _bucket())):
                    bucket["unrealized"] += unr
                    bucket["open_count"] += 1
                    bucket["open_futures"] += 1

    for bucket in list(modes.values()) + list(bots.values()):
        bucket["net"] = float(bucket["realized"]) + float(bucket["unrealized"])
    for mode_map in bot_modes.values():
        for bucket in mode_map.values():
            bucket["net"] = float(bucket["realized"]) + float(bucket["unrealized"])
    return {"modes": modes, "bots": bots, "bot_modes": bot_modes}


def compute_metrics(trades_df: pd.DataFrame) -> dict:
    """
    Liefert das volle Set Performance-Metriken.
    Returns 'N/A' when there is not enough data.

    P&L-Summe: alle Trades inkl. is_partial=1 (Partial-Close Ereignisse
    are real realized gains and must be counted).
    Win-Rate: nur is_partial=0 (Final-Closes), damit ein Trade der einen
    Partial-Win + final Win is not counted twice as a "Win".
    """
    n = len(trades_df)
    if n == 0:
        return {"trades": 0}

    pnl = trades_df["profit_usdt"].astype(float)
    pos_df = aggregate_positions(trades_df)
    pos_pnl = pos_df["profit_usdt"].astype(float) if not pos_df.empty else pnl

    wins = pos_pnl[pos_pnl > 0]
    losses = pos_pnl[pos_pnl < 0]
    n_positions = len(pos_pnl)

    total_pnl = pnl.sum()   # alle Fills inkl. Partials
    win_rate = (len(wins) / n_positions * 100) if n_positions > 0 else 0.0
    avg_win = wins.mean() if len(wins) > 0 else 0.0
    avg_loss = losses.mean() if len(losses) > 0 else 0.0
    avg_trade = pos_pnl.mean() if n_positions > 0 else 0.0

    # Profit Factor  sum gains / |sum losses|
    sum_wins = wins.sum() if len(wins) > 0 else 0
    sum_losses = abs(losses.sum()) if len(losses) > 0 else 0
    pf = (sum_wins / sum_losses) if sum_losses > 0 else float("inf") if sum_wins > 0 else 0.0

    # Win/Loss Ratio
    wl_ratio = (avg_win / abs(avg_loss)) if avg_loss != 0 else float("inf") if avg_win > 0 else 0.0

    # Expectancy
    expectancy = avg_trade

    # Sharpe & Sortino  basierend auf trade-PnL (per-trade returns)
    if n_positions >= 3:
        returns = pos_pnl.values
        std = np.std(returns, ddof=1)
        sharpe = (np.mean(returns) / std * math.sqrt(n_positions)) if std > 0 else 0.0
        downside = returns[returns < 0]
        d_std = np.std(downside, ddof=1) if len(downside) >= 2 else 0
        sortino = (np.mean(returns) / d_std * math.sqrt(n_positions)) if d_std > 0 else 0.0
    else:
        sharpe = sortino = 0.0

    # Max Drawdown  auf der kumulativen Equity-Curve
    sorted_df = trades_df.sort_values("sell_time")
    equity = sorted_df["profit_usdt"].cumsum().values
    if len(equity) > 0:
        peak = np.maximum.accumulate(equity)
        drawdown = peak - equity  # in USDT
        max_dd = drawdown.max() if len(drawdown) > 0 else 0.0
    else:
        max_dd = 0.0

    # Best & Worst
    best = pos_pnl.max()
    worst = pos_pnl.min()

    # Streaks
    is_win_arr = (pos_pnl > 0).astype(int).values
    cur, best_streak, worst_streak, cur_loss = 0, 0, 0, 0
    for v in is_win_arr:
        if v == 1:
            cur += 1
            cur_loss = 0
            best_streak = max(best_streak, cur)
        else:
            cur_loss += 1
            cur = 0
            worst_streak = max(worst_streak, cur_loss)

    return {
        "trades":   n_positions,
        "fills":    n,
        "total":    total_pnl,
        "win_rate": win_rate,
        "wins":     len(wins),
        "losses":   len(losses),
        "avg_win":  avg_win,
        "avg_loss": avg_loss,
        "avg_trade": avg_trade,
        "profit_factor": pf,
        "wl_ratio": wl_ratio,
        "expectancy": expectancy,
        "sharpe":   sharpe,
        "sortino":  sortino,
        "max_dd":   max_dd,
        "best":     best,
        "worst":    worst,
        "best_streak":  best_streak,
        "worst_streak": worst_streak,
    }


#  Header 

st.markdown(
    """
    <div style="display:flex; align-items:center; gap:14px; margin-bottom:6px;">
        <span style="font-size:1.8rem; color:#6a5cc0;"></span>
        <span style="font-size:1.5rem; font-weight:800; letter-spacing:-0.02em;">OBSIDIAN <span style="color:#94a3b8;">TERMINAL</span></span>
        <span style="font-size:0.7rem; color:#64748b; background:rgba(106,92,192,0.1); padding:3px 9px; border-radius:6px; margin-left:6px; font-family:'JetBrains Mono'; font-weight:600;">v5.0</span>
    </div>
    <div style="color:#64748b; font-size:0.85rem; margin-bottom:24px;">Spot &amp; Futures bot suite  Real-time analytics</div>
    """,
    unsafe_allow_html=True
)

# Auto-Refresh + Status in Sidebar
with st.sidebar:
    st.markdown('<div class="section-title" style="margin-top:0;">REFRESH</div>',
                  unsafe_allow_html=True)
    auto_refresh = st.checkbox("Auto-refresh (10s)", value=True)
    if st.button("  Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.markdown('<div class="section-title">FILTERS</div>', unsafe_allow_html=True)
    period_choice = st.selectbox("Time period",
                                    ["All time", "Last 24h", "Last 7 days",
                                     "Last 30 days", "Last 90 days"],
                                    index=0)
    bot_filter = st.multiselect("Bots",
                                  ALL_BOTS,
                                  default=ALL_BOTS)
    selected_bots = set(bot_filter)

    st.markdown(
        '<div style="margin-top:24px; padding:14px; background:rgba(15,20,29,0.6); '
        'border-radius:10px; border:1px solid rgba(255,255,255,0.05); font-size:0.75rem;">'
        f'<div style="color:#64748b;">Last update</div>'
        f'<div style="color:#cbd5e1; font-family:JetBrains Mono; font-weight:600; margin-top:4px;">'
        f'{datetime.now().strftime("%H:%M:%S")}</div></div>',
        unsafe_allow_html=True
    )


#  Filter anwenden 

trades_raw = load_all_trades()
trades = trades_raw.copy()

if not trades.empty:
    if period_choice == "Last 24h":
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        trades = trades[trades["sell_time"] >= cutoff]
    elif period_choice == "Last 7 days":
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        trades = trades[trades["sell_time"] >= cutoff]
    elif period_choice == "Last 30 days":
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        trades = trades[trades["sell_time"] >= cutoff]
    elif period_choice == "Last 90 days":
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        trades = trades[trades["sell_time"] >= cutoff]
    bot_col = "base_bot" if "base_bot" in trades.columns else "bot_name"
    trades = trades[trades[bot_col].map(_base_bot_name).isin(selected_bots)]


#  TABS 

tab_overview, tab_trades, tab_positions, tab_performance, tab_bots = st.tabs(
    ["  Overview  ", "  Trades  ", "  Positions  ", "  Performance  ", "  Bots  "]
)


# 
# TAB: OVERVIEW
# 
with tab_overview:

    #  Hero KPIs 
    c1, c2, c3, c4, c5, c6 = st.columns(6)


    #  Separate LIVE (real money) from SIM (paper) 
    # The headline number is LIVE-only (real money, matches the exchange); SIM
    # paper gains are reported separately so they're never mistaken for profit.
    if not trades.empty and "mode" in trades.columns:
        live_trades = trades[trades["mode"] == "LIVE"]
        sim_trades  = trades[trades["mode"] == "SIM"]
        legacy_trades = trades[trades["mode"] == "LEGACY"]
    else:
        live_trades = trades
        sim_trades  = trades.iloc[0:0] if not trades.empty else trades
        legacy_trades = trades.iloc[0:0] if not trades.empty else trades
    live_total = float(live_trades["profit_usdt"].sum()) if not live_trades.empty else 0.0
    sim_total  = float(sim_trades["profit_usdt"].sum())  if not sim_trades.empty  else 0.0
    live_n     = len(live_trades)
    sim_n      = len(sim_trades)
    legacy_n   = len(legacy_trades)
    metrics = compute_metrics(live_trades)
    total = metrics.get("total", 0)
    n_tr  = metrics.get("trades", 0)
    wr    = metrics.get("win_rate", 0)

    today_str = get_local_today_str()
    if not trades.empty:
        today_mask = trades["sell_time"].apply(utc_to_local_date_str) == today_str
        today_trades = trades[today_mask]
    else:
        today_trades = trades
    # Today's P&L: LIVE only (real money), matching the exchange's "Today PNL"
    if not today_trades.empty and "mode" in today_trades.columns:
        today_live = today_trades[today_trades["mode"] == "LIVE"]
    else:
        today_live = today_trades
    today_pnl = today_live["profit_usdt"].sum() if not today_live.empty else 0.0
    today_positions = aggregate_positions(today_live)
    today_count = len(today_positions)

    # Open positions count (spot + futures)
    open_spot_all = load_open_spot_trades()
    open_spot_all = [
        row for row in open_spot_all
        if _base_bot_name(row.get("bot") or row.get("bot_name") or "") in selected_bots
    ]
    open_spot_live = [t for t in open_spot_all if t.get("mode", "SIM") == "LIVE"]
    open_spot_sim = [t for t in open_spot_all if t.get("mode", "SIM") == "SIM"]
    open_fut_all  = load_futures_live()
    if not open_fut_all.empty:
        _overview_fut_col = "base_bot" if "base_bot" in open_fut_all.columns else "bot_name"
        if _overview_fut_col in open_fut_all.columns:
            open_fut_all = open_fut_all[
                open_fut_all[_overview_fut_col].map(_base_bot_name).isin(selected_bots)
            ].copy()
    if not open_fut_all.empty and "state_is_stale" in open_fut_all.columns:
        open_fut_active_all = open_fut_all[~open_fut_all["state_is_stale"].fillna(False)].copy()
        open_fut_stale_all = open_fut_all[open_fut_all["state_is_stale"].fillna(False)].copy()
    else:
        open_fut_active_all = open_fut_all
        open_fut_stale_all = open_fut_all.iloc[0:0] if not open_fut_all.empty else open_fut_all
    if not open_fut_all.empty and "mode" in open_fut_all.columns:
        open_fut_live = open_fut_active_all[open_fut_active_all["mode"] == "LIVE"].copy()
        open_fut_sim = open_fut_active_all[open_fut_active_all["mode"] == "SIM"].copy()
    else:
        open_fut_live = open_fut_active_all
        open_fut_sim = open_fut_active_all.iloc[0:0] if not open_fut_active_all.empty else open_fut_active_all
    spot_symbols = tuple(sorted({
        str(t.get("symbol", "")).upper().replace("/USDT", "").replace("USDT", "")
        for t in open_spot_all
        if isinstance(t, dict) and str(t.get("symbol", "")).strip()
    }))
    live_prices_kpi = get_live_prices(spot_symbols)
    pnl_snapshot = build_pnl_snapshot(
        trades, open_spot_all, open_fut_active_all, live_prices_kpi,
        selected_bots,
    )
    live_money = pnl_snapshot["modes"]["LIVE"]
    sim_money = pnl_snapshot["modes"]["SIM"]
    legacy_money = pnl_snapshot["modes"]["LEGACY"]
    stale_live_futures = 0
    if not open_fut_stale_all.empty and "mode" in open_fut_stale_all.columns:
        stale_live_futures = int((open_fut_stale_all["mode"] == "LIVE").sum())
    live_total = float(live_money["realized"])
    sim_total = float(sim_money["realized"])
    total_unreal = float(live_money["unrealized"])
    net_pnl = float(live_money["net"])
    total_open = int(live_money["open_count"])
    live_price_missing = int(live_money.get("price_unavailable", 0) or 0)
    unreal_spot = 0.0
    unreal_fut = 0.0
    if total_open:
        unreal_fut = total_unreal
        try:
            _fut_for_kpi = open_fut_live
            unreal_fut = float(_fut_for_kpi["unrealized_pnl"].fillna(0).sum()) if not _fut_for_kpi.empty else 0.0
            unreal_spot = total_unreal - unreal_fut
        except Exception:
            unreal_fut = total_unreal
            unreal_spot = 0.0
    open_spot = open_spot_live
    open_fut = open_fut_live

    with c1:
        # Headline = closed LIVE trades. Unrealized is separate, net combines both.
        if sim_n > 0:
            sub = f"net MTM {net_pnl:+.2f} / SIM realized {sim_total:+.2f}"
        else:
            sub = f"{live_money['fills']} live fill{'s' if live_money['fills'] != 1 else ''} / net MTM {net_pnl:+.2f}"
        if legacy_n:
            sub += f" / legacy {legacy_money['realized']:+.2f}"
        st.markdown(_kpi_card("REALIZED (LIVE)", live_total,
                                 sub, "#6a5cc0"), unsafe_allow_html=True)
    with c2:
        st.markdown(_kpi_card("TODAY", today_pnl,
                                 f"{today_count} position{'s' if today_count != 1 else ''}",
                                 "#b07ae0"), unsafe_allow_html=True)
    with c3:
        wr_color = "#22c55e" if wr >= 55 else "#f59e0b" if wr >= 45 else "#ef4444"
        st.markdown(_kpi_card("WIN RATE", f"{wr:.1f}",
                                 f"{metrics.get('wins',0)}W / {metrics.get('losses',0)}L",
                                 wr_color, value_fmt="", suffix="%",
                                 sign=False), unsafe_allow_html=True)
    with c4:
        pf = metrics.get("profit_factor", 0)
        pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
        pf_color = "#22c55e" if pf >= 1.5 else "#f59e0b" if pf >= 1.0 else "#ef4444"
        st.markdown(_kpi_card("PROFIT FACTOR", pf_str,
                                 "gains / losses",
                                 pf_color, value_fmt="", suffix="",
                                 sign=False), unsafe_allow_html=True)
    with c5:
        sim_open = int(sim_money["open_count"])
        open_color = "#f97316" if len(open_fut_live) > 0 else "#b07ae0"
        live_fut_visible = int(live_money["open_futures"])
        open_sub = f"{int(live_money['open_spot'])} live spot / {live_fut_visible} live futures"
        if sim_open:
            open_sub += f" / {sim_open} SIM"
        if stale_live_futures:
            open_sub += f" / {stale_live_futures} stale hidden"
        elif not open_fut_stale_all.empty:
            open_sub += f" / {len(open_fut_stale_all)} stale SIM"
        st.markdown(_kpi_card("OPEN POSITIONS", total_open,
                                 open_sub,
                                 open_color, value_fmt="", suffix="",
                                 sign=False), unsafe_allow_html=True)
    with c6:
        unr_color = "#22c55e" if total_unreal >= 0 else "#ef4444"
        unr_sub = f"{unreal_spot:+.2f} spot / {unreal_fut:+.2f} perp"
        if live_price_missing:
            unr_sub += f" / {live_price_missing} price missing"
        st.markdown(_kpi_card("UNREALIZED", total_unreal,
                                 unr_sub,
                                 unr_color), unsafe_allow_html=True)

    cnet1, cnet2, cnet3 = st.columns([1, 1, 4])
    with cnet1:
        st.markdown(_kpi_card("NET MTM (LIVE)", net_pnl,
                                 "realized + unrealized",
                                 "#22c55e" if net_pnl >= 0 else "#ef4444"),
                    unsafe_allow_html=True)
    with cnet2:
        st.markdown(_kpi_card("LEGACY / UNCLASSIFIED", legacy_money["realized"],
                                 f"{legacy_money['fills']} fill(s) excluded from LIVE",
                                 "#94a3b8"),
                    unsafe_allow_html=True)
    if not open_fut_stale_all.empty:
        stale_bots = ", ".join(
            sorted({str(v) for v in open_fut_stale_all.get("bot_name", []) if str(v).strip()})
        )
        st.warning(
            f"{len(open_fut_stale_all)} stale futures_state row(s) excluded from live KPIs"
            + (f": {stale_bots}" if stale_bots else "")
        )
    if live_price_missing:
        st.warning(
            f"{live_price_missing} live spot position(s) have no fresh price; "
            "their unrealized PnL is excluded from Net MTM."
        )

    #  Per-Bot Summary 
    _section("Per-Bot Performance")
    bot_cols = st.columns(len(ALL_BOTS))
    bot_trade_col = "base_bot" if not trades.empty and "base_bot" in trades.columns else "bot_name"
    for idx, bot in enumerate(ALL_BOTS):
        configured_mode = BOT_MODES.get(bot, "SIM")
        mode_buckets = pnl_snapshot.get("bot_modes", {}).get(bot, {}) or {}

        def _bucket_has_money(mode: str) -> bool:
            bucket = mode_buckets.get(mode, {}) or {}
            try:
                return (
                    abs(float(bucket.get("realized", 0.0) or 0.0)) > 1e-9
                    or abs(float(bucket.get("unrealized", 0.0) or 0.0)) > 1e-9
                    or int(bucket.get("open_count", 0) or 0) > 0
                    or int(bucket.get("fills", 0) or 0) > 0
                )
            except Exception:
                return False

        bot_mode = configured_mode
        if not _bucket_has_money(bot_mode):
            for candidate in ("LIVE", "SIM", "LEGACY"):
                if candidate != bot_mode and _bucket_has_money(candidate):
                    bot_mode = candidate
                    break
        mode_trades = (
            trades[trades["mode"] == bot_mode]
            if not trades.empty and "mode" in trades.columns
            else trades
        )
        bot_trades = mode_trades[mode_trades[bot_trade_col] == bot] if not mode_trades.empty else pd.DataFrame()
        bot_metrics = compute_metrics(bot_trades)
        accent = BOT_ACCENTS[bot]
        with bot_cols[idx]:
            n = bot_metrics.get("trades", 0)
            bot_money = mode_buckets.get(bot_mode, {}) or {}
            pnl = float(bot_money.get("realized", bot_metrics.get("total", 0)))
            unr = float(bot_money.get("unrealized", 0.0))
            net = float(bot_money.get("net", pnl + unr))
            open_n = int(bot_money.get("open_count", 0))
            wr_b = bot_metrics.get("win_rate", 0)

            pnl_color = "#22c55e" if net > 0 else "#ef4444" if net < 0 else "#94a3b8"
            sign = "+" if pnl >= 0 else ""
            net_sign = "+" if net >= 0 else ""
            unr_sign = "+" if unr >= 0 else ""

            badge = _mode_badge(bot_mode)

            bot_card_html = (
                f'<div style="background:rgba(15,20,29,0.6); border-radius:14px; '
                f'border:1px solid rgba(255,255,255,0.06); border-left:4px solid {accent}; '
                f'padding:18px 22px;">'
                f'<div style="display:flex; justify-content:space-between; align-items:baseline;">'
                f'<span style="font-size:1.1rem; font-weight:700; color:{accent};">{bot} {badge}</span>'
                f'<span style="font-size:0.7rem; color:#64748b; font-family:JetBrains Mono;">{BOT_SUBTITLES[bot]}</span>'
                f'</div>'
                f'<div style="font-size:1.9rem; font-weight:800; color:{pnl_color}; margin-top:10px; letter-spacing:-0.02em;">'
                f'{net_sign}{net:.2f} <span style="font-size:0.85rem; color:#475569;">USDT net</span>'
                f'</div>'
                f'<div style="display:flex; flex-wrap:wrap; gap:14px; margin-top:14px; font-size:0.75rem; color:#94a3b8; font-family:JetBrains Mono;">'
                f'<span>Real <b style="color:#e2e8f0;">{sign}{pnl:.2f}</b></span>'
                f'<span>Open <b style="color:#e2e8f0;">{unr_sign}{unr:.2f}</b>/{open_n}</span>'
                f'<span>Pos <b style="color:#e2e8f0;">{n}</b></span>'
                f'<span>WR <b style="color:#e2e8f0;">{wr_b:.0f}%</b></span>'
                f'<span>Avg <b style="color:#e2e8f0;">{bot_metrics.get("avg_trade", 0):+.2f}</b></span>'
                f'</div>'
                f'</div>'
            )
            st.markdown(bot_card_html, unsafe_allow_html=True)

    #  Equity Curve + Drawdown 
    _section("Equity Curve & Drawdown")
    if live_trades.empty:
        st.info("No trades yet  equity curve will appear once the bots make trades.")
    else:
        eq_df = live_trades.sort_values("sell_time").copy()
        eq_df["cum_pnl"] = eq_df["profit_usdt"].cumsum()
        eq_df["peak"]    = eq_df["cum_pnl"].cummax()
        eq_df["drawdown"] = eq_df["peak"] - eq_df["cum_pnl"]  # positive = drawdown amount

        fig = go.Figure()

        # Equity main
        fig.add_trace(go.Scatter(
            x=eq_df["sell_time"], y=eq_df["cum_pnl"],
            mode="lines",
            line=dict(color="#b07ae0", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(79,180,192,0.08)",
            name="Equity"
        ))

        # Peak line
        fig.add_trace(go.Scatter(
            x=eq_df["sell_time"], y=eq_df["peak"],
            mode="lines",
            line=dict(color="rgba(106,92,192,0.4)", width=1, dash="dot"),
            name="Peak"
        ))

        # Per-Bot Punkte
        for bot in ALL_BOTS:
            bot_eq = eq_df[eq_df[bot_trade_col] == bot]
            if not bot_eq.empty:
                fig.add_trace(go.Scatter(
                    x=bot_eq["sell_time"], y=bot_eq["cum_pnl"],
                    mode="markers",
                    marker=dict(color=BOT_ACCENTS[bot], size=5,
                                  line=dict(color="#04060a", width=1)),
                    name=bot,
                    hovertemplate=("<b>" + bot + "</b><br>"
                                    "%{x|%Y-%m-%d %H:%M}<br>"
                                    "Equity: %{y:.2f} USDT<extra></extra>")
                ))

        fig.update_layout(
            height=380, margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#94a3b8", size=11),
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)", showline=False),
            yaxis=dict(gridcolor="rgba(255,255,255,0.04)", title="USDT", showline=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
        )
        st.plotly_chart(fig, width="stretch")

        # Drawdown unten
        col_left, col_right = st.columns([3, 1])
        with col_left:
            fig_dd = go.Figure()
            fig_dd.add_trace(go.Scatter(
                x=eq_df["sell_time"], y=-eq_df["drawdown"],
                mode="lines", fill="tozeroy",
                line=dict(color="#ef4444", width=1.5),
                fillcolor="rgba(239,68,68,0.15)",
                name="Drawdown"
            ))
            fig_dd.update_layout(
                title=dict(text="Drawdown over time",
                            font=dict(size=12, color="#64748b"), x=0.01),
                height=200, margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#94a3b8", size=10),
                xaxis=dict(gridcolor="rgba(255,255,255,0.04)", showline=False),
                yaxis=dict(gridcolor="rgba(255,255,255,0.04)", title="USDT", showline=False),
                showlegend=False
            )
            st.plotly_chart(fig_dd, width="stretch")
        with col_right:
            max_dd = metrics.get("max_dd", 0)
            current_dd = eq_df["drawdown"].iloc[-1] if not eq_df.empty else 0
            st.markdown(_kpi_card("MAX DRAWDOWN", -max_dd, "worst peak-to-trough",
                                     "#ef4444"), unsafe_allow_html=True)
            st.markdown(_kpi_card("CURRENT DD", -current_dd, "from latest peak",
                                     "#f59e0b"), unsafe_allow_html=True)


# 
# TAB: TRADES
# 
with tab_trades:
    _section("Trade Journal")

    if trades.empty:
        st.info("No trades match the current filters.")
    else:
        #  Filter-Zeile 
        fcols = st.columns([1, 1, 1, 1, 1])
        with fcols[0]:
            mode_options = ["LIVE only", "SIM only", "LEGACY only", "All"]
            mode_f = st.selectbox(
                "Mode",
                mode_options,
                index=0,
            )
        with fcols[1]:
            outcome_f = st.selectbox("Outcome",
                                       ["All", "Wins only", "Losses only"], index=0)
        with fcols[2]:
            unique_coins = sorted(trades["symbol"].unique().tolist())
            coin_f = st.multiselect("Coins", unique_coins, default=[])
        with fcols[3]:
            reasons = sorted(trades["reason"].fillna("").unique().tolist())
            reason_f = st.multiselect("Reasons", reasons, default=[])
        with fcols[4]:
            type_f = st.selectbox("Type",
                                    ["All", "Spot only", "Futures only"], index=0)

        view = trades.copy()
        if "mode" in view.columns and mode_f != "All":
            wanted = mode_f.split()[0]
            view = view[view["mode"] == wanted]
        if outcome_f == "Wins only":  view = view[view["profit_usdt"] > 0]
        elif outcome_f == "Losses only": view = view[view["profit_usdt"] < 0]
        if coin_f:  view = view[view["symbol"].isin(coin_f)]
        if reason_f: view = view[view["reason"].fillna("").isin(reason_f)]
        if type_f == "Spot only":    view = view[view["is_futures"] == 0]
        elif type_f == "Futures only": view = view[view["is_futures"] == 1]

        # Summary over filtered data.
        sub_metrics = compute_metrics(view)
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(_kpi_card("FILTERED P&L", sub_metrics.get("total", 0),
                                     f"{sub_metrics.get('trades', 0)} trades",
                                     "#6a5cc0"), unsafe_allow_html=True)
        with m2:
            st.markdown(_kpi_card("AVG TRADE",
                                     sub_metrics.get("avg_trade", 0),
                                     "per trade",
                                     "#b07ae0"), unsafe_allow_html=True)
        with m3:
            st.markdown(_kpi_card("BEST", sub_metrics.get("best", 0),
                                     "single trade",
                                     "#22c55e"), unsafe_allow_html=True)
        with m4:
            st.markdown(_kpi_card("WORST", sub_metrics.get("worst", 0),
                                     "single trade",
                                     "#ef4444"), unsafe_allow_html=True)

        #  Table 
        st.markdown('<div style="margin-top:18px;"></div>', unsafe_allow_html=True)

        display = view.copy()
        display["sell_time"] = display["sell_time"].dt.strftime("%Y-%m-%d %H:%M")
        display["buy_time"]  = display["buy_time"].dt.strftime("%Y-%m-%d %H:%M")

        def _type_label(row):
            if row.get("is_futures", 0) == 1:
                lev = row.get("leverage", 0)
                pt = row.get("position_type", "")
                return f"{pt} {int(lev) if lev else ''}x"
            return "Spot"

        display["type"] = display.apply(_type_label, axis=1)

        show_cols = ["sell_time", "mode", "bot_name", "symbol", "type",
                       "buy_price", "sell_price", "profit_pct", "profit_usdt",
                       "invested_usdt", "reason"]
        # Add RSI if present
        if "rsi_1h" in display.columns:
            show_cols.insert(9, "rsi_1h")

        display = display[show_cols].rename(columns={
            "sell_time": "Closed",
            "mode":      "Mode",
            "bot_name":  "Bot",
            "symbol":    "Coin",
            "type":      "Type",
            "buy_price": "Entry",
            "sell_price": "Exit",
            "profit_pct": "P&L %",
            "profit_usdt": "P&L USDT",
            "invested_usdt": "Size",
            "rsi_1h":    "RSI 1h",
            "reason":    "Reason",
        })

        # Numerische Werte formatieren BEVOR sie an die HTML-Tabelle gehen
        display_fmt = display.copy()
        display_fmt["Entry"]    = display_fmt["Entry"].apply(lambda v: f"{v:.6f}" if pd.notna(v) else "")
        display_fmt["Exit"]     = display_fmt["Exit"].apply(lambda v: f"{v:.6f}" if pd.notna(v) else "")
        display_fmt["P&L %"]    = display_fmt["P&L %"].apply(lambda v: f"{v:+.2f}%" if pd.notna(v) else "")
        display_fmt["P&L USDT"] = display_fmt["P&L USDT"].apply(lambda v: f"{v:+.2f}" if pd.notna(v) else "")
        display_fmt["Size"]     = display_fmt["Size"].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "")
        if "RSI 1h" in display_fmt.columns:
            display_fmt["RSI 1h"] = display_fmt["RSI 1h"].apply(
                lambda v: f"{v:.1f}" if pd.notna(v) else "")

        _render_dark_table(
            display_fmt,
            max_rows=300,
            height_px=460,
            align_right_cols=["Entry", "Exit", "P&L %", "P&L USDT", "Size", "RSI 1h"],
            color_cols={"P&L %": "pnl", "P&L USDT": "pnl"}
        )

        #  Profit per Coin 
        _section("Profit by Coin")
        coin_pnl = view.groupby("symbol").agg(
            trades=("profit_usdt", "size"),
            total=("profit_usdt", "sum"),
            wins=("is_win", "sum"),
        ).reset_index().sort_values("total", ascending=False).head(20)

        if not coin_pnl.empty:
            coin_pnl["wr"] = (coin_pnl["wins"] / coin_pnl["trades"] * 100).round(0)
            fig_coin = go.Figure()
            colors = ["#22c55e" if v >= 0 else "#ef4444" for v in coin_pnl["total"]]
            fig_coin.add_trace(go.Bar(
                x=coin_pnl["symbol"], y=coin_pnl["total"],
                marker=dict(color=colors),
                text=[f"{v:+.2f}" for v in coin_pnl["total"]],
                textposition="outside",
                textfont=dict(color="#cbd5e1", size=11),
                hovertemplate="<b>%{x}</b><br>P&L: %{y:.2f} USDT<extra></extra>"
            ))
            fig_coin.update_layout(
                height=340, margin=dict(l=20, r=20, t=20, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#94a3b8", size=11),
                xaxis=dict(showline=False),
                yaxis=dict(gridcolor="rgba(255,255,255,0.04)", title="USDT")
            )
            st.plotly_chart(fig_coin, width="stretch")


# 
# TAB: POSITIONS
# 
with tab_positions:
    open_spot = load_open_spot_trades()
    open_spot = [
        row for row in open_spot
        if _base_bot_name(row.get("bot") or row.get("bot_name") or "") in selected_bots
    ]
    position_spot_symbols = tuple(sorted({
        str(t.get("symbol", "")).upper().replace("/USDT", "").replace("USDT", "")
        for t in open_spot
        if isinstance(t, dict) and str(t.get("symbol", "")).strip()
    }))
    live_prices = get_live_prices(position_spot_symbols)
    open_fut  = load_futures_live()
    if not open_fut.empty:
        _filter_col = "base_bot" if "base_bot" in open_fut.columns else "bot_name"
        if _filter_col in open_fut.columns:
            open_fut = open_fut[
                open_fut[_filter_col].map(_base_bot_name).isin(selected_bots)
            ].copy()
    if not open_fut.empty and "state_is_stale" in open_fut.columns:
        open_fut_active = open_fut[~open_fut["state_is_stale"].fillna(False)].copy()
        open_fut_stale = open_fut[open_fut["state_is_stale"].fillna(False)].copy()
    else:
        open_fut_active = open_fut
        open_fut_stale = open_fut.iloc[0:0] if not open_fut.empty else open_fut

    #  Per-bot position card renderers (rendered in 4 sections below) 
    def _render_spot_cards(_rows):
        cols = st.columns(min(3, max(1, len(_rows))))
        for idx, t in enumerate(_rows):
            with cols[idx % len(cols)]:
                sym = t["symbol"]
                sym_html = html.escape(str(sym))
                bot_html = html.escape(str(t.get("bot", "")))
                buy = float(t.get("buy_price") or t.get("buy") or 0)
                amount = float(t.get("amount", 0) or 0)
                entry_notional = amount * buy
                live_raw = live_prices.get(f"{sym}USDT")
                price_missing = live_raw is None
                live = float(live_raw) if live_raw is not None else buy
                pct = ((live - buy) / buy * 100) if (buy > 0 and not price_missing) else 0
                pnl = spot_unrealized_pnl(buy, live, amount) if not price_missing else 0.0
                accent = BOT_ACCENTS.get(t["bot"], "#b07ae0")
                partial = t.get("partial_sold", False)
                try:
                    rsi_1h = float(t.get("rsi_1h") or 0.0)
                except (TypeError, ValueError):
                    rsi_1h = 0.0
                mode_badge = _mode_badge(t.get("mode", "SIM"))

                pct_class = "neu" if price_missing else "pos" if pct >= 0 else "neg"
                sign = "+" if pct >= 0 else ""
                pct_text = "PRICE N/A" if price_missing else f"{sign}{pct:.2f}%"
                pnl_text = "price unavailable" if price_missing else f"{sign}{pnl:.2f} USDT"
                partial_badge = (
                    '<span style="background:rgba(245,158,11,0.15); color:#f59e0b; '
                    'font-size:0.65rem; padding:2px 7px; border-radius:4px; '
                    'margin-left:6px;">PARTIAL SOLD</span>'
                    if partial else ""
                )

                card_html = (
                    f'<div style="background:rgba(12,17,26,0.9); border-radius:14px; '
                    f'border:1px solid rgba(255,255,255,0.07); '
                    f'border-left:4px solid {accent}; padding:18px;">'
                    f'<div style="display:flex; justify-content:space-between;">'
                    f'<div>'
                    f'<span style="font-size:1.1rem; font-weight:700; color:#e2e8f0;">{sym_html}</span>'
                    f'<span style="font-size:0.7rem; color:#64748b; margin-left:8px;">{bot_html}</span>'
                    f'{mode_badge}'
                    f'{partial_badge}'
                    f'</div>'
                    f'<div style="text-align:right;">'
                    f'<div class="kpi-value {pct_class}" style="font-size:1.6rem;">{pct_text}</div>'
                    f'<div class="kpi-sub">{pnl_text}</div>'
                    f'</div>'
                    f'</div>'
                    f'<div style="display:flex; gap:14px; margin-top:14px; font-size:0.72rem; '
                    f'font-family:JetBrains Mono; color:#64748b; flex-wrap:wrap;">'
                    f'<span>Entry <b style="color:#94a3b8;">{buy:.6f}</b></span>'
                    f'<span>Now <b style="color:#e2e8f0;">{live:.6f}</b></span>'
                f'<span>Size <b style="color:#94a3b8;">{entry_notional:.2f}</b></span>'
                    f'<span>RSI <b style="color:#94a3b8;">{rsi_1h:.1f}</b></span>'
                    f'</div>'
                    f'</div>'
                )
                st.markdown(card_html, unsafe_allow_html=True)

    def _render_perp_cards(_df):
        cols = st.columns(min(3, max(1, len(_df))))
        for idx, row in enumerate(_df.itertuples()):
            with cols[idx % len(cols)]:
                _bn = getattr(row, "bot_name", "FUTURES")
                _base_bn = getattr(row, "base_bot", _base_bot_name(_bn))
                mode_badge = _mode_badge(getattr(row, "mode", "SIM"))
                accent = BOT_ACCENTS.get(_base_bn, BOT_ACCENTS["FUTURES"])
                pos_type = row.position_type
                pos_color = "#22c55e" if pos_type == "LONG" else "#ef4444"
                pct_class = "pos" if row.unrealized_pnl >= 0 else "neg"
                sign = "+" if row.unrealized_pnl >= 0 else ""

                # Liquidation-Distanz Farbe.
                # liq_distance_pct = how far price must move AGAINST the
                # position to hit liquidation, relative to current price.
                # For LONGs this is bounded 0100% (liq price >= 0). For
                # SHORTs  and especially CROSS-margin legs, where the liq
                # price sits far ABOVE current because the whole account
                # backs the leg  the raw value is unbounded and balloons to
                # 1000%+, which reads as nonsense. As a safety gauge it only
                # makes sense in 0100% (100% = maximally far from liq), so
                # clamp the DISPLAY here. The stored raw value is left intact:
                # the bots' panic-close buffer math relies on true distance.
                liq_pct = min(float(row.liq_distance_pct), 100.0)
                liq_capped = float(row.liq_distance_pct) > 100.0
                if liq_pct < 15:
                    liq_color = "#ef4444"
                    liq_warn = " DANGER"
                elif liq_pct < 30:
                    liq_color = "#f59e0b"
                    liq_warn = " Watch"
                else:
                    liq_color = "#22c55e"
                    liq_warn = "Safe"

                lev = int(row.leverage) if row.leverage else 1
                sym_html = html.escape(str(row.symbol))
                bot_html = html.escape(str(_bn))
                pos_type_html = html.escape(str(pos_type))

                card_html = (
                    f'<div style="background:rgba(12,17,26,0.9); border-radius:14px; '
                    f'border:1px solid rgba(255,255,255,0.07); '
                    f'border-left:4px solid {accent}; padding:18px;">'
                    f'<div style="display:flex; justify-content:space-between;">'
                    f'<div>'
                    f'<span style="font-size:1.1rem; font-weight:700; color:#e2e8f0;">{sym_html}</span>'
                    f'<span style="background:{accent}22; color:{accent}; '
                    f'font-size:0.6rem; padding:2px 7px; border-radius:4px; '
                    f'margin-left:8px; font-weight:800; text-transform:uppercase; '
                    f'letter-spacing:0.06em;">{bot_html}</span>'
                    f'{mode_badge}'
                    f'<span style="background:rgba(249,115,22,0.15); color:{pos_color}; '
                    f'font-size:0.7rem; padding:2px 8px; border-radius:4px; '
                    f'margin-left:8px; font-weight:700;">{pos_type_html} {lev}x</span>'
                    f'</div>'
                    f'<div style="text-align:right;">'
                    f'<div class="kpi-value {pct_class}" style="font-size:1.5rem;">'
                    f'{sign}{row.unrealized_pct:.2f}%'
                    f'</div>'
                    f'<div class="kpi-sub">{sign}{row.unrealized_pnl:.2f} USDT on margin</div>'
                    f'</div>'
                    f'</div>'
                    f'<div style="display:flex; gap:14px; margin-top:14px; font-size:0.72rem; '
                    f'font-family:JetBrains Mono; color:#64748b; flex-wrap:wrap;">'
                    f'<span>Entry <b style="color:#94a3b8;">{row.entry_price:.6f}</b></span>'
                    f'<span>Mark <b style="color:#e2e8f0;">{row.current_price:.6f}</b></span>'
                    f'<span>Margin <b style="color:#94a3b8;">{row.margin_usdt:.2f}</b></span>'
                    f'<span>Notional <b style="color:#94a3b8;">{row.position_size_usdt:.2f}</b></span>'
                    f'</div>'
                    f'<div style="margin-top:12px; padding:10px 12px; background:rgba(0,0,0,0.3); '
                    f'border-radius:8px; border-left:3px solid {liq_color}; '
                    f'display:flex; justify-content:space-between; align-items:center;">'
                    f'<div>'
                    f'<div style="font-size:0.62rem; color:#64748b; text-transform:uppercase; '
                    f'letter-spacing:0.14em; font-weight:600;">Liquidation</div>'
                    f'<div style="color:#cbd5e1; font-family:JetBrains Mono; font-size:0.78rem; font-weight:700;">'
                    f'{row.liquidation_price:.6f}'
                    f'</div>'
                    f'</div>'
                    f'<div style="text-align:right;">'
                    f'<div style="font-size:0.62rem; color:{liq_color}; text-transform:uppercase; '
                    f'letter-spacing:0.14em; font-weight:700;">{liq_warn}</div>'
                    f'<div style="color:{liq_color}; font-family:JetBrains Mono; font-size:0.95rem; font-weight:800;">'
                    f'{">" if liq_capped else ""}{liq_pct:.1f}%'
                    f'</div>'
                    f'</div>'
                    f'</div>'
                    f'</div>'
                )
                st.markdown(card_html, unsafe_allow_html=True)

    #  Separate sections per bot and mode. LIVE positions are real exposure;
    # SIM positions are shown only as paper positions.
    for _label, _bot in (("Trend", "TREND"), ("Spot", "SPOT")):
        _rows = [r for r in open_spot if r.get("bot") == _bot]
        _live_rows = [r for r in _rows if r.get("mode", "SIM") == "LIVE"]
        _sim_rows = [r for r in _rows if r.get("mode", "SIM") == "SIM"]
        _section(f"Open {_label} LIVE Positions")
        if _live_rows:
            _render_spot_cards(_live_rows)
        else:
            st.info(f"No open {_label.lower()} live positions.")
        if _sim_rows:
            _section(f"Open {_label} SIM Positions")
            _render_spot_cards(_sim_rows)
    for _label, _bot in (("Futures", "FUTURES"), ("Cross", "CROSS"),
                         ("Future Trend", "FUTREND")):
        _fut_bot_col = "base_bot" if "base_bot" in open_fut_active.columns else "bot_name"
        _sub = (open_fut_active[open_fut_active[_fut_bot_col] == _bot]
                if (not open_fut_active.empty and _fut_bot_col in open_fut_active.columns)
                else open_fut_active.iloc[0:0])
        _live_sub = (_sub[_sub["mode"] == "LIVE"]
                     if "mode" in _sub.columns else _sub)
        _sim_sub = (_sub[_sub["mode"] == "SIM"]
                    if "mode" in _sub.columns else _sub.iloc[0:0])
        _section(f"Open {_label} LIVE Positions")
        if not _live_sub.empty:
            _render_perp_cards(_live_sub)
        else:
            st.info(f"No open {_label.lower()} live positions.")
        if not _sim_sub.empty:
            _section(f"Open {_label} SIM Positions")
            _render_perp_cards(_sim_sub)

    if not open_fut_stale.empty:
        _section("Stale Futures State")
        st.warning(
            f"{len(open_fut_stale)} stale futures_state row(s) are hidden from open-position KPIs. "
            "Use Reconciliation/repair before treating them as live exposure."
        )
        stale_view = open_fut_stale.copy()
        show_cols = [
            c for c in ("bot_name", "symbol", "mode", "position_type",
                        "unrealized_pnl", "unrealized_pct", "last_update",
                        "state_age_sec")
            if c in stale_view.columns
        ]
        if show_cols:
            st.dataframe(stale_view[show_cols], width="stretch", hide_index=True)


# 
# TAB: PERFORMANCE
# 
with tab_performance:
    if trades.empty:
        st.info("No trades  performance metrics will appear once trades are recorded.")
    elif live_trades.empty:
        st.info("No LIVE trades in the selected period. SIM/LEGACY trades are excluded from money performance.")
    else:
        perf_trades = live_trades
        #  Risk-Metrics Row 
        _section("Risk Metrics")
        m = compute_metrics(perf_trades)

        rc1, rc2, rc3, rc4 = st.columns(4)
        with rc1:
            sharpe = m.get("sharpe", 0)
            color = "#22c55e" if sharpe >= 1.0 else "#f59e0b" if sharpe >= 0 else "#ef4444"
            st.markdown(_kpi_card("SHARPE RATIO", f"{sharpe:.2f}",
                                     "risk-adjusted return",
                                     color, value_fmt="", suffix="",
                                     sign=False), unsafe_allow_html=True)
        with rc2:
            sortino = m.get("sortino", 0)
            color = "#22c55e" if sortino >= 1.5 else "#f59e0b" if sortino >= 0 else "#ef4444"
            st.markdown(_kpi_card("SORTINO RATIO", f"{sortino:.2f}",
                                     "downside-adjusted",
                                     color, value_fmt="", suffix="",
                                     sign=False), unsafe_allow_html=True)
        with rc3:
            wlr = m.get("wl_ratio", 0)
            wlr_str = f"{wlr:.2f}" if wlr != float("inf") else "inf"
            color = "#22c55e" if wlr >= 1.5 else "#f59e0b" if wlr >= 1.0 else "#ef4444"
            st.markdown(_kpi_card("WIN/LOSS RATIO", wlr_str,
                                     "avg win / avg loss",
                                     color, value_fmt="", suffix="",
                                     sign=False), unsafe_allow_html=True)
        with rc4:
            exp = m.get("expectancy", 0)
            st.markdown(_kpi_card("EXPECTANCY", exp,
                                     "expected $ per trade",
                                     "#6a5cc0"), unsafe_allow_html=True)

        rc5, rc6, rc7, rc8 = st.columns(4)
        with rc5:
            st.markdown(_kpi_card("BEST TRADE", m.get("best", 0),
                                     "single profit",
                                     "#22c55e"), unsafe_allow_html=True)
        with rc6:
            st.markdown(_kpi_card("WORST TRADE", m.get("worst", 0),
                                     "single loss",
                                     "#ef4444"), unsafe_allow_html=True)
        with rc7:
            st.markdown(_kpi_card("BEST STREAK",
                                     m.get("best_streak", 0),
                                     "consecutive wins",
                                     "#22c55e", value_fmt="", suffix="W",
                                     sign=False), unsafe_allow_html=True)
        with rc8:
            st.markdown(_kpi_card("WORST STREAK",
                                     m.get("worst_streak", 0),
                                     "consecutive losses",
                                     "#ef4444", value_fmt="", suffix="L",
                                     sign=False), unsafe_allow_html=True)

        #  Hour & Weekday Heatmap 
        _section("When trades happen  hour & weekday performance")

        hm_left, hm_right = st.columns(2)
        with hm_left:
            hour_df = perf_trades.dropna(subset=["hour_of_day"]).copy()
            hour_stats = hour_df.groupby("hour_of_day").agg(
                pnl=("profit_usdt", "sum"),
                trades=("profit_usdt", "size"),
                wins=("is_win", "sum")
            ).reset_index()
            all_hours = pd.DataFrame({"hour_of_day": range(24)})
            hour_stats = all_hours.merge(hour_stats, on="hour_of_day", how="left").fillna(0)

            fig_h = go.Figure()
            colors = ["#22c55e" if v > 0 else "#ef4444" if v < 0 else "#475569"
                       for v in hour_stats["pnl"]]
            fig_h.add_trace(go.Bar(
                x=hour_stats["hour_of_day"], y=hour_stats["pnl"],
                marker=dict(color=colors),
                customdata=hour_stats[["trades", "wins"]],
                hovertemplate=("Hour <b>%{x}:00</b><br>"
                                "P&L: %{y:.2f} USDT<br>"
                                "Trades: %{customdata[0]}<br>"
                                "Wins: %{customdata[1]}<extra></extra>")
            ))
            fig_h.update_layout(
                title=dict(text="By hour of day (UTC)",
                            font=dict(size=12, color="#64748b"), x=0.01),
                height=300, margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#94a3b8", size=10),
                xaxis=dict(title="Hour", tickmode="array",
                            tickvals=[0, 6, 12, 18, 23], showline=False),
                yaxis=dict(gridcolor="rgba(255,255,255,0.04)", title="USDT")
            )
            st.plotly_chart(fig_h, width="stretch")

        with hm_right:
            day_df = perf_trades.dropna(subset=["day_of_week"]).copy()
            day_stats = day_df.groupby("day_of_week").agg(
                pnl=("profit_usdt", "sum"),
                trades=("profit_usdt", "size"),
            ).reset_index()
            all_days = pd.DataFrame({"day_of_week": range(7)})
            day_stats = all_days.merge(day_stats, on="day_of_week", how="left").fillna(0)
            day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

            fig_d = go.Figure()
            colors = ["#22c55e" if v > 0 else "#ef4444" if v < 0 else "#475569"
                       for v in day_stats["pnl"]]
            fig_d.add_trace(go.Bar(
                x=[day_names[int(i)] for i in day_stats["day_of_week"]],
                y=day_stats["pnl"],
                marker=dict(color=colors),
                customdata=day_stats["trades"],
                hovertemplate=("<b>%{x}</b><br>P&L: %{y:.2f} USDT<br>"
                                "Trades: %{customdata}<extra></extra>")
            ))
            fig_d.update_layout(
                title=dict(text="By weekday",
                            font=dict(size=12, color="#64748b"), x=0.01),
                height=300, margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#94a3b8", size=10),
                xaxis=dict(showline=False),
                yaxis=dict(gridcolor="rgba(255,255,255,0.04)", title="USDT")
            )
            st.plotly_chart(fig_d, width="stretch")

        #  Hour x Weekday Heatmap 
        _section("Time-of-Week Heatmap")
        hw_df = perf_trades.dropna(subset=["hour_of_day", "day_of_week"]).copy()
        if not hw_df.empty:
            grid = hw_df.groupby(["day_of_week", "hour_of_day"]).agg(
                pnl=("profit_usdt", "sum")
            ).reset_index()
            # Volle 7x24 Matrix
            mat = np.zeros((7, 24))
            for _, r in grid.iterrows():
                mat[int(r["day_of_week"]), int(r["hour_of_day"])] = r["pnl"]

            day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            fig_hm = go.Figure(go.Heatmap(
                z=mat, x=list(range(24)), y=day_names,
                colorscale=[
                    [0.0, "#ef4444"], [0.45, "#1a1a1a"],
                    [0.55, "#1a1a1a"], [1.0, "#22c55e"]
                ],
                zmid=0,
                hovertemplate="<b>%{y} %{x}:00</b><br>P&L: %{z:.2f} USDT<extra></extra>",
                colorbar=dict(title="USDT", tickfont=dict(color="#94a3b8"))
            ))
            fig_hm.update_layout(
                height=280, margin=dict(l=20, r=20, t=20, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#94a3b8", size=11),
                xaxis=dict(title="Hour (UTC)", side="bottom"),
                yaxis=dict(autorange="reversed")
            )
            st.plotly_chart(fig_hm, width="stretch")

        #  Reason Breakdown 
        _section("Exit Reason Breakdown")
        reason_stats = perf_trades.groupby("reason").agg(
            count=("profit_usdt", "size"),
            total_pnl=("profit_usdt", "sum"),
            avg=("profit_usdt", "mean")
        ).reset_index().sort_values("count", ascending=False)

        rcol_l, rcol_r = st.columns([1, 1])
        with rcol_l:
            fig_r = go.Figure(go.Bar(
                x=reason_stats["count"], y=reason_stats["reason"],
                orientation="h",
                marker=dict(color="#6a5cc0"),
                text=reason_stats["count"], textposition="outside",
                textfont=dict(color="#cbd5e1"),
                hovertemplate="<b>%{y}</b><br>Count: %{x}<extra></extra>"
            ))
            fig_r.update_layout(
                title=dict(text="Frequency", font=dict(size=12, color="#64748b"), x=0.01),
                height=320, margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#94a3b8", size=11),
                xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
                yaxis=dict(autorange="reversed")
            )
            st.plotly_chart(fig_r, width="stretch")
        with rcol_r:
            colors = ["#22c55e" if v >= 0 else "#ef4444" for v in reason_stats["total_pnl"]]
            fig_rp = go.Figure(go.Bar(
                x=reason_stats["total_pnl"], y=reason_stats["reason"],
                orientation="h",
                marker=dict(color=colors),
                text=[f"{v:+.2f}" for v in reason_stats["total_pnl"]],
                textposition="outside",
                textfont=dict(color="#cbd5e1"),
                hovertemplate="<b>%{y}</b><br>P&L: %{x:.2f} USDT<extra></extra>"
            ))
            fig_rp.update_layout(
                title=dict(text="P&L per reason", font=dict(size=12, color="#64748b"), x=0.01),
                height=320, margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", color="#94a3b8", size=11),
                xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
                yaxis=dict(autorange="reversed", showticklabels=False)
            )
            st.plotly_chart(fig_rp, width="stretch")


# 
# TAB: BOTS
# 
with tab_bots:
    _section("LIVE Bot Side-by-Side Comparison")

    comp_rows = []
    bot_scope = live_trades
    comp_bot_col = "base_bot" if not bot_scope.empty and "base_bot" in bot_scope.columns else "bot_name"
    for bot in ALL_BOTS:
        bdf = bot_scope[bot_scope[comp_bot_col] == bot] if not bot_scope.empty else pd.DataFrame()
        bm = compute_metrics(bdf)
        comp_rows.append({
            "Bot":          bot,
            "Trades":       bm.get("trades", 0),
            "Total P&L":    bm.get("total", 0),
            "Win Rate":     bm.get("win_rate", 0),
            "Avg Trade":    bm.get("avg_trade", 0),
            "Profit Factor": bm.get("profit_factor", 0)
                              if bm.get("profit_factor") != float("inf") else 999,
            "Sharpe":       bm.get("sharpe", 0),
            "Max DD":       -bm.get("max_dd", 0),
            "Best":         bm.get("best", 0),
            "Worst":        bm.get("worst", 0),
        })
    comp_df = pd.DataFrame(comp_rows)
    # Pre-format for HTML table rendering.
    comp_df_fmt = comp_df.copy()
    comp_df_fmt["Total P&L"]     = comp_df_fmt["Total P&L"].apply(lambda v: f"{v:+.2f} USDT")
    comp_df_fmt["Win Rate"]      = comp_df_fmt["Win Rate"].apply(lambda v: f"{v:.1f}%")
    comp_df_fmt["Avg Trade"]     = comp_df_fmt["Avg Trade"].apply(lambda v: f"{v:+.2f}")
    comp_df_fmt["Profit Factor"] = comp_df_fmt["Profit Factor"].apply(
        lambda v: "inf" if v >= 999 else f"{v:.2f}")
    comp_df_fmt["Sharpe"]        = comp_df_fmt["Sharpe"].apply(lambda v: f"{v:.2f}")
    comp_df_fmt["Max DD"]        = comp_df_fmt["Max DD"].apply(lambda v: f"{v:.2f} USDT")
    comp_df_fmt["Best"]          = comp_df_fmt["Best"].apply(lambda v: f"{v:+.2f}")
    comp_df_fmt["Worst"]         = comp_df_fmt["Worst"].apply(lambda v: f"{v:+.2f}")

    _render_dark_table(
        comp_df_fmt,
        align_right_cols=["Trades", "Total P&L", "Win Rate", "Avg Trade",
                            "Profit Factor", "Sharpe", "Max DD", "Best", "Worst"],
        color_cols={"Total P&L": "pnl", "Avg Trade": "pnl",
                     "Best": "pnl", "Worst": "pnl"}
    )

    #  Per-Bot Equity Curve 
    _section("LIVE Per-Bot Equity Curves")
    if not bot_scope.empty:
        fig_be = go.Figure()
        for bot in ALL_BOTS:
            bdf = bot_scope[bot_scope[comp_bot_col] == bot].sort_values("sell_time")
            if bdf.empty:
                continue
            bdf = bdf.copy()
            bdf["cum"] = bdf["profit_usdt"].cumsum()
            fig_be.add_trace(go.Scatter(
                x=bdf["sell_time"], y=bdf["cum"],
                mode="lines",
                line=dict(color=BOT_ACCENTS[bot], width=2.2),
                name=bot,
                hovertemplate=f"<b>{bot}</b><br>%{{x|%Y-%m-%d %H:%M}}<br>"
                                "Equity: %{y:.2f} USDT<extra></extra>"
            ))
        fig_be.update_layout(
            height=380, margin=dict(l=20, r=20, t=10, b=20),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#94a3b8", size=11),
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.04)", title="Cumulative USDT"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
        )
        st.plotly_chart(fig_be, width="stretch")
    else:
        st.info("No LIVE trades yet.")

    #  Active Parameters per Bot 
    _section("Active Parameters")
    params_df = load_bot_params()
    if not params_df.empty:
        param_cols = st.columns(len(ALL_BOTS))
        for idx, bot in enumerate(ALL_BOTS):
            with param_cols[idx]:
                accent = BOT_ACCENTS[bot]
                bot_params = params_df[params_df["bot_name"] == bot].head(20)
                rows_html = ""
                if bot_params.empty:
                    rows_html = ('<div style="color:#64748b; padding:8px; font-size:0.8rem;">'
                                  'No dynamic parameters yet</div>')
                else:
                    for _, r in bot_params.iterrows():
                        param_name = html.escape(str(r["param_name"]))
                        param_value = html.escape(str(r["param_value"]))
                        updated_at = html.escape(str(r["updated_at"]))
                        reason = html.escape(str(r["reason"] or "")[:50])
                        rows_html += (
                            '<div style="padding:8px 0; border-bottom:1px solid rgba(255,255,255,0.04);">'
                            '<div style="display:flex; justify-content:space-between;">'
                            f'<span style="color:#94a3b8; font-size:0.78rem;">{param_name}</span>'
                            '<span style="color:#e2e8f0; font-family:JetBrains Mono; '
                            f'font-weight:700; font-size:0.78rem;">{param_value}</span>'
                            '</div>'
                            '<div style="color:#475569; font-size:0.65rem; margin-top:2px;">'
                            f'{updated_at}  {reason}'
                            '</div>'
                            '</div>'
                        )
                bot_html = html.escape(str(bot))
                bot_card_html = (
                    f'<div style="background:rgba(12,17,26,0.9); border-radius:12px; '
                    f'border:1px solid rgba(255,255,255,0.06); '
                    f'border-left:3px solid {accent}; padding:14px 18px;">'
                    f'<div style="font-weight:700; color:{accent}; font-size:0.95rem; margin-bottom:8px;">'
                    f'{bot_html}'
                    f'</div>'
                    f'{rows_html}'
                    f'</div>'
                )
                st.markdown(bot_card_html, unsafe_allow_html=True)

    #  Learning Log 
    _section("AI Learning Timeline")
    learning_df = load_learning_log()
    if learning_df.empty:
        st.info("No learning events yet  Risk Manager will adapt parameters over time.")
    else:
        display_l = learning_df[["timestamp", "bot_name", "action", "param_name",
                                   "old_value", "new_value", "reason"]].rename(columns={
            "timestamp":  "Time",
            "bot_name":   "Bot",
            "action":     "Action",
            "param_name": "Param",
            "old_value":  "Old",
            "new_value":  "New",
            "reason":     "Reason",
        })
        _render_dark_table(
            display_l, max_rows=200, height_px=380,
            align_right_cols=["Old", "New"]
        )


#  Auto-Refresh 

if auto_refresh:
    # st_autorefresh schedules the rerun client-side (a browser timer) so the
    # Python thread is never blocked and the UI stays responsive between
    # refreshes (Streamlit serialises script runs per session, so a blocking
    # sleep would lag widget clicks / tab switches). Falls back to the blocking
    # sleep+rerun if the optional dependency isn't installed.
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=10_000, key="obsidian_autorefresh")
    except Exception:
        import time as _time
        _time.sleep(10)
        st.rerun()
