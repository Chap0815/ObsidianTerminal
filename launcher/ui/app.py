"""
launcher.ui.app  the slimmed-down ObsidianApp main window.

The heavy lifting (bot lifecycle, position queries, log severity
classification, all dialogs) has been extracted to dedicated modules
under :mod:`launcher.core`, :mod:`launcher.ui.dialogs`, and
:mod:`launcher.ui.logging_panel`. The methods that remain on the class
are limited to UI construction, the periodic ``_refresh`` loop, and a
handful of pure-UI helpers. Methods that were extracted survive only as
thin shims so the dozens of internal call-sites (``self._start_bot``,
``self._get_open_futures_positions`` etc.) keep working without
touching the caller methods.
"""

from __future__ import annotations

import json
import os
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import tkinter as tk
import webbrowser
from contextlib import suppress
from datetime import datetime, timedelta
from tkinter import font as tkfont

import customtkinter as ctk
import requests as _req

try:
    import psutil  # noqa: F401  (used inside _refresh via HAS_PSUTIL)
except ImportError:
    pass

# Modular launcher imports
from launcher.config.settings import (
    BOT_META,
    BOT_ORDER,
    COLORS,
    CONFIG_FILE,
    DB_PATH,
    DEFAULT_CONFIG,
    FONT_BODY,
    OLLAMA_URL,
    PARAM_DEFS_FUTURES,
    PARAM_DEFS_SPOT,
    PROJECT_ROOT,
    _get_python_exe,
    _get_pythonw_exe,
    _safe_mono_font,
    _safe_display_font,
    effective_default_config,
    load_config,
    save_config,
    save_config_merge,
    subprocess_no_window_kwargs,
)
# Defensive: PARAM_DEFS_TREND is new (v3.5). If an OLDER settings.py is
# deployed alongside this app.py, a hard import would blank the entire UI.
# Fall back to the spot param set so the launcher always loads.
try:
    from launcher.config.settings import PARAM_DEFS_TREND
except ImportError:
    PARAM_DEFS_TREND = PARAM_DEFS_SPOT
# Defensive: PARAM_DEFS_CROSS is new (4th bot). Fall back to futures so the
# launcher always loads against an older settings.py.
try:
    from launcher.config.settings import PARAM_DEFS_CROSS
except ImportError:
    PARAM_DEFS_CROSS = PARAM_DEFS_FUTURES
# Defensive: PARAM_DEFS_FUTREND is new (5th bot  leveraged trend futures).
try:
    from launcher.config.settings import PARAM_DEFS_FUTREND
except ImportError:
    PARAM_DEFS_FUTREND = PARAM_DEFS_FUTURES
from launcher.core.metrics_service import (
    get_bot_stats,
    get_exchange_status,
    get_futures_state_count,
    get_llm_info,
    get_market_info,
    get_open_trades,
    get_unrealized_pnl_futures,
    get_unrealized_pnl_spot,
    load_json,
    query_db,
)
from launcher.core.process_manager import BotProcess
from core.runtime_status import read_runtime_status
from launcher.core.system_monitor import HAS_PSUTIL, get_system_stats
from launcher.state.poller import DataPoller, _runtime_status_is_fresh
from launcher.ui.components.widgets import (
    BadHoursRow,
    MiniBar,
    ParamRow,
    Sparkline,
    Tooltip,
    attach_tooltip,
)
from launcher.ui.dialogs.prompt_editor import PromptEditor
from launcher.ui.scaling import apply_scaling
from launcher.ui.theme import force_dark_titlebar


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")


class ObsidianApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.config = load_config()

        self.title("Obsidian Trading Terminal v5.0")
        # Resolution-adaptive scaling: shrink the 2K/4K-baseline layout so it
        # fits the real screen (1080p .. 4K, ultrawide). Override via env
        # UI_SCALE=<factor> or config UI.SCALE. Must run before geometry.
        self._ui_scale, _win_w, _win_h = apply_scaling(self, self.config)
        self.geometry(f"{_win_w}x{_win_h}+0+0")
        # minsize derived from the design baseline at the active scale so it
        # never exceeds the fitted window on small screens.
        self.minsize(min(_win_w, int(1400 * self._ui_scale)),
                     min(_win_h, int(1000 * self._ui_scale)))
        self.configure(fg_color=COLORS["bg"])
        self.update_idletasks()

        # Windows Titlebar dunkel machen
        force_dark_titlebar(self)

        # Taskbar / window icon  looks for obsidian.ico in several
        # candidate locations. Best-effort: gracefully no-ops on Linux/Mac
        # (where .ico isn't the native format) and when the file is
        # missing. On Windows we ALSO set the AppUserModelID so the icon
        # shows correctly in the taskbar (without this, Windows groups
        # the app under the Python interpreter icon even though the
        # window itself shows the custom icon).
        try:
            import os as _os, sys as _sys
            from launcher.config.settings import PROJECT_ROOT
            _script_dir = _os.path.dirname(_os.path.abspath(__file__))
            # Search candidates in priority order
            _candidates = [
                _os.path.join(_script_dir, "components", "obsidian.ico"),  # launcher/ui/components/  NEW
                _os.path.join(_script_dir, "obsidian.ico"),                # launcher/ui/
                _os.path.join(PROJECT_ROOT, "obsidian.ico"),               # project root (legacy)
                _os.path.join(PROJECT_ROOT, "assets", "obsidian.ico"),     # assets/
            ]
            _icon_path = next((p for p in _candidates if _os.path.isfile(p)), None)
            if _icon_path:
                try:
                    self.iconbitmap(_icon_path)
                except Exception:
                    pass
                if _sys.platform == "win32":
                    try:
                        import ctypes
                        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                            "anthropic.obsidian.trading.terminal"
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        # Always start maximized  content is scaled to fit, so it never clips
        # on any resolution/aspect ratio (16:9, 16:10, 21:9, 4:3). Re-asserted
        # after the UI is realized (some WMs ignore an early 'zoomed'). F11
        # toggles true borderless fullscreen, Esc leaves it.
        self._is_fullscreen = False
        self._maximize()
        self.after(120, self._reassert_chrome)
        self.bind("<F11>", self._toggle_fullscreen)
        self.bind("<Escape>", self._exit_fullscreen)


        self.mono_font = _safe_mono_font()
        self.display_font = _safe_display_font()

        # Self-heal missing prompt files.
        self._ensure_prompt_files_exist()

        # Per bot: queue, process wrapper, card refs. Bound log queues so long
        # runs cannot grow memory unbounded if UI reads slower than bots write.
        self.log_queues = {bot: queue.Queue(maxsize=5000) for bot in BOT_ORDER}
        # All bots support graceful close: SIGINT/SIGTERM/SIGBREAK handlers
        # are registered in each bot's run_bot()  they close all open positions
        # via market orders before exit.
        # Pass the `module` kwarg so the subprocess uses `python -m bots.main_bot_X`
        # (correct sys.path for sub-package imports) instead of running the script
        # path directly (which would break `from core.X import ...`).
        self.bots = {bot: BotProcess(BOT_META[bot]["script"], self.log_queues[bot],
                                       supports_graceful=True,
                                       module=BOT_META[bot].get("module", ""),
                                       bot_name=bot)
                      for bot in BOT_ORDER}
        self.cards = {}  # bot_name  card dict
        self.streamlit = None
        self._dashboard_port = None

        self._pulse_step = 0
        self._vc_offset  = 0.0   # Virtual Capital Anzeigeoffset (kein DB-Reset)
        self._bad_hours_refresh_ctr = 0   # throttle: refresh BadHoursRow every ~30s
        self.param_rows     = {bot: {} for bot in BOT_ORDER}
        self._dirty_param_keys = {bot: set() for bot in BOT_ORDER}
        self.bad_hours_rows = {}  # bot_name  BadHoursRow widget
        self._collapsed = {bot: False for bot in BOT_ORDER}
        self._pending_update_data = None
        self._update_status_override = ""
        self._update_status_override_until = 0.0
        self._update_notice_shown = False
        # Load visibility from config.
        ui_cfg = self.config.get("UI", {})
        self._visible = {bot: (bot in ui_cfg.get("VISIBLE_BOTS", list(BOT_ORDER)))
                          for bot in BOT_ORDER}
        # Collapse support was removed; old COLLAPSED_BOTS config is ignored.

        if HAS_PSUTIL:
            with suppress(Exception):
                psutil.cpu_percent(interval=None)

        self.poller = DataPoller()

        self._build_ui()
        self._apply_initial_visibility()
        self._set_content_minsize()
        self._refresh_tick()
        self._pulse()
        self.after(2500, self._check_for_updates_async)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _set_content_minsize(self) -> None:
        """Floor the window at the measured natural content size so dragging it
        smaller stops right before the layout would clip. Measured (not guessed)
        so it auto-tracks the active scaling; capped to the usable screen."""
        try:
            self.update_idletasks()
            try:
                sc = float(ctk.ScalingTracker.get_window_scaling(self)) or 1.0
            except Exception:
                sc = 1.0
            # winfo_req* is physical px; CTk.minsize() scales its args by the
            # window factor, so convert physical  logical ( sc).
            req_w = self.winfo_reqwidth()
            req_h = self.winfo_reqheight()
            phys_w = max(800, min(req_w, self.winfo_screenwidth() - 16))
            phys_h = max(600, min(req_h, self.winfo_screenheight() - 80))
            self.minsize(int(phys_w / sc), int(phys_h / sc))
        except Exception:
            pass

    def _reassert_chrome(self) -> None:
        """Re-maximize + re-apply the dark titlebar once the window is mapped.
        An early DWM dark-mode call gets reverted to a light frame after the
        first post-realization state change on Windows, so re-assert it here."""
        self._maximize()
        try:
            force_dark_titlebar(self)
        except Exception:
            pass

    def _maximize(self) -> None:
        """Maximize to the work area (keeps the titlebar  still movable)."""
        try:
            self.state("zoomed")              # Windows / most WMs
        except Exception:
            try:
                self.attributes("-zoomed", True)   # some Linux WMs
            except Exception:
                try:
                    self.geometry(f"{self.winfo_screenwidth()}x"
                                  f"{self.winfo_screenheight()}+0+0")
                except Exception:
                    pass

    def _toggle_fullscreen(self, _evt=None) -> str:
        """F11: toggle true borderless fullscreen (no titlebar/taskbar)."""
        self._is_fullscreen = not getattr(self, "_is_fullscreen", False)
        try:
            self.attributes("-fullscreen", self._is_fullscreen)
        except Exception:
            self._is_fullscreen = False
        if not self._is_fullscreen:
            self.after(50, self._reassert_chrome)
        return "break"

    def _exit_fullscreen(self, _evt=None) -> None:
        """Esc: leave borderless fullscreen back to maximized."""
        if getattr(self, "_is_fullscreen", False):
            self._is_fullscreen = False
            try:
                self.attributes("-fullscreen", False)
            except Exception:
                pass
            self.after(50, self._reassert_chrome)

    def _build_ui(self):
        # Layout: sidebar on the left, main content on the right.
        self.grid_columnconfigure(0, weight=0, minsize=240)  # Sidebar fix
        self.grid_columnconfigure(1, weight=1)               # Main flex
        self.grid_rowconfigure(0, weight=0)                  # Header
        self.grid_rowconfigure(1, weight=1)                  # Cards
        self.grid_rowconfigure(2, weight=0)                  # Footer

        # Sidebar spans all rows.
        self._build_sidebar()
        # Header in the main column.
        self._build_header()
        # Cards in der Mitte
        self._build_main()
        # Status-Footer ganz unten
        self._build_statusbar()

    #  HEADER (kompakte Bar in Main-Spalte) 

    def _build_header(self):
        bar = ctk.CTkFrame(self, fg_color=COLORS["panel"], height=62, corner_radius=0)
        bar.grid(row=0, column=1, sticky="ew")
        bar.grid_propagate(False)
        # Bottom-Border
        ctk.CTkFrame(self, fg_color=COLORS["border"], height=1, corner_radius=0
                      ).grid(row=0, column=1, sticky="sew")

        # Show + Pills (links)  Layout wie React Tabs
        pill_label = ctk.CTkLabel(bar, text="Show",
                                    font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                                    text_color=COLORS["text_muted"])
        pill_label.pack(side="left", padx=(22, 12), pady=14)

        # TabsList Container  bg-muted/50 border
        pill_box = ctk.CTkFrame(bar, fg_color=COLORS["bg_alt"], corner_radius=8,
                                  height=36,
                                  border_width=1,
                                  border_color=COLORS["border_soft"])
        pill_box.pack(side="left", pady=14)

        self.visibility_pills = {}
        inner_pills = ctk.CTkFrame(pill_box, fg_color="transparent")
        inner_pills.pack(padx=3, pady=3)

        for bot in BOT_ORDER:
            meta = BOT_META[bot]
            is_visible = self._visible[bot]
            # Layout: "<icon> <Label>"  like React TabsTrigger with lucide icon
            label_text = f"{meta.get('icon','')}  {meta['label'].title()}"
            pill = ctk.CTkButton(
                inner_pills, text=label_text,
                width=128, height=26, corner_radius=6,
                font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                # Aktiv = Indigo-Outline + Indigo-Text (kein gefuellter Block);
                # Inaktiv = dezenter Rand + muted text.
                fg_color="transparent",
                hover_color=COLORS["panel_hover"],
                text_color=(COLORS["purple"] if is_visible else COLORS["text_muted"]),
                border_width=1,
                border_color=(COLORS["purple"] if is_visible else COLORS["border"]),
                command=lambda b=bot: self._toggle_visibility(b)
            )
            pill.pack(side="left", padx=1)
            self.visibility_pills[bot] = pill
            attach_tooltip(
                pill,
                f"Show / hide the {meta['label']} bot card.\n"
                f"{meta['subtitle']}\n"
                f"Hidden bots keep running if started  purely visual.",
                delay_ms=400
            )

        # Open Dashboard (rechts)
        dash_btn = ctk.CTkButton(
            bar, text="  Open Dashboard",
            width=170, height=34, corner_radius=8,
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            fg_color="transparent", hover_color=COLORS["panel_hover"],
            text_color=COLORS["text"],
            border_width=1, border_color=COLORS["border"],
            command=self._open_dashboard
        )
        dash_btn.pack(side="right", padx=22, pady=14)
        attach_tooltip(
            dash_btn,
            "Open the full analytics dashboard in your browser.\n"
            "Streamlit app with charts, live positions and trade journal.",
            delay_ms=400
        )

        # Emergency-Close-Btn ist nicht mehr hier  er sitzt jetzt im FUTURES-Card
        self.emergency_btn = None  # Backward-Compat for older refresh paths
        self.emergency_btns = {}

    #  SIDEBAR 

    def _build_sidebar(self):
        sb = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0, width=240)
        sb.grid(row=0, column=0, rowspan=3, sticky="nsew")
        sb.grid_propagate(False)
        ctk.CTkFrame(self, fg_color=COLORS["border"], width=1, corner_radius=0
                    ).grid(row=0, column=0, rowspan=3, sticky="nse")

        # Logo oben (kompakt)
        logo_box = ctk.CTkFrame(sb, fg_color="transparent", height=48)
        logo_box.pack(fill="x", padx=14, pady=(10, 4))
        logo_box.pack_propagate(False)
        ctk.CTkLabel(logo_box, text="",
                      font=ctk.CTkFont(self.mono_font, 18, "bold"),
                      text_color=COLORS["purple"]
                      ).pack(side="left", padx=(0, 8), pady=2)
        logo_text = ctk.CTkFrame(logo_box, fg_color="transparent")
        logo_text.pack(side="left", fill="y")
        ctk.CTkLabel(logo_text, text="Obsidian",
                      font=ctk.CTkFont(self.display_font, 14, "bold"),
                      text_color=COLORS["text"], anchor="w"
                      ).pack(anchor="w", pady=(4, 0))
        ctk.CTkLabel(logo_text, text="Trading Terminal",
                      font=ctk.CTkFont(FONT_BODY, 8, "bold"),
                      text_color=COLORS["text_subtle"], anchor="w"
                      ).pack(anchor="w")

        # Separator unter Logo
        ctk.CTkFrame(sb, fg_color=COLORS["border_soft"], height=1).pack(fill="x", padx=10, pady=(2, 4))

        # Scrollable sidebar body so the lower cards stay reachable on short
        # windows. The scroll-frame can break _refresh()'s pack(before=...) of
        # the balance rows (TclError "isn't packed"), but _safe_pack_before()
        # catches that and _refresh runs inside _refresh_tick(), so a layout
        # glitch only reorders the balance rows cosmetically  it can't freeze
        # the UI.
        inner = ctk.CTkScrollableFrame(
            sb, fg_color="transparent",
            scrollbar_button_color=COLORS["border"],
            scrollbar_button_hover_color=COLORS["text_muted"],
        )
        inner.pack(fill="both", expand=True, padx=(10, 2), pady=2)

        # MARKET  gerahmte Karte
        _c = self._sb_card(inner)
        self._sb_section(_c, "MARKET")
        self.sb_phase = self._sb_metric(_c, "Phase", "", COLORS["text"])
        self.sb_btc  = self._sb_metric(_c, "BTC 24h", "", COLORS["text"])
        self.sb_fg  = self._sb_metric(_c, "Fear & Greed", "", COLORS["text"])

        # ACCOUNT  gerahmte Karte
        _c = self._sb_card(inner)
        self._sb_section(_c, "ACCOUNT")
        self.sb_balance_live = self._sb_metric_big(_c, "Available Capital", "", COLORS["balanced"])
        # Tooltip: explain that this is full equity, NOT just free balance.
        # The number now includes: free USDT + margin tied up in open
        # positions + unrealized PnL. So even with 0 free USDT, this shows
        # the real "money on the exchange" answer.
        attach_tooltip(
            self.sb_balance_live._lbl,
            "Total equity across all live wallets:\n"
            "  free USDT  +  margin in open positions  +  unrealized PnL\n\n"
            "This is the REAL number  what the exchange shows as your\n"
            "wallet balance. Read live every 15 s, never from the local DB.\n"
            "If a coin's price is unreachable it's silently excluded from\n"
            "the total rather than showing a wrong number.",
            delay_ms=500
        )
        # Separate spot/futures wallet rows. Shown only when both wallet types
        # are relevant; otherwise the main Available Capital row is enough.
        self.sb_balance_spot  = self._sb_metric(_c, "  Spot Wallet",  "", COLORS["text_dim"])
        self.sb_balance_futures = self._sb_metric(_c, "  Futures Wallet", "", COLORS["text_dim"])
        attach_tooltip(
            self.sb_balance_spot._lbl,
            "Spot wallet equity:\n"
            "  free USDT (+ other stables 1:1)\n"
            "  +  (coin holding  current ticker price)\n\n"
            "Coins worth < 0.50 USDT are filtered out as dust.",
            delay_ms=500
        )
        # Capture the Futures-Wallet tooltip so we can update its text
        # each refresh with the live positions breakdown. _refresh() looks
        # for this attribute and calls update_text() on it.
        self._futures_wallet_tooltip = attach_tooltip(
            self.sb_balance_futures._lbl,
            "Futures wallet equity:\n"
            "  free margin\n"
            "  +  initial margin of open positions\n"
            "  +  unrealized PnL\n\n"
            "NOTE: ccxt's 'used' field is unreliable on MEXC  we\n"
            "compute position margin directly from fetch_positions(),\n"
            "which works the same way across all ccxt exchanges.",
            delay_ms=500
        )
        # Initially hidden; refresh shows them when needed.
        self.sb_balance_spot._wrap.pack_forget()
        self.sb_balance_futures._wrap.pack_forget()

        # Virtual Capital row with reset button.
        sim_header_row = ctk.CTkFrame(_c, fg_color="transparent")
        sim_header_row.pack(fill="x", pady=0)

        # Create the button first and pack it on the right.
        self.sb_reset_btn = ctk.CTkButton(
            sim_header_row, text="", width=20, height=20, corner_radius=4,
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            fg_color="transparent", hover_color=COLORS["panel_hover"],
            text_color=COLORS["text_subtle"],
            command=self._reset_virtual_capital
        )
        self.sb_reset_btn.pack(side="right", padx=(4, 0))  # pack statt place
        attach_tooltip(self.sb_reset_btn, "Reset Virtual Capital to 1000 USDT", delay_ms=600)

        # Then the metric fills the remaining space.
        self.sb_balance_sim = self._sb_metric(sim_header_row, "Virtual Capital", "", COLORS["purple"])

        # Performance card.
        _c = self._sb_card(inner)
        self._sb_section(_c, "PERFORMANCE")
        self.sb_total   = self._sb_metric_big(_c, "Mode PnL", "+0.00 USDT", COLORS["text_dim"])
        self.sb_today   = self._sb_metric(_c, "Today", "+0.00 USDT", COLORS["text"])
        self.sb_unr_total = self._sb_metric(_c, "Unrealized", "", COLORS["text_muted"])
        self.sb_trades  = self._sb_metric(_c, "Total Trades", "0", COLORS["text"])
        self.sb_winrate = self._sb_metric(_c, "Win Rate", "", COLORS["text"])
        self.sb_trend_pos = self._sb_metric(_c, "Trend Open", "0", COLORS["text"])
        self.sb_spot_pos  = self._sb_metric(_c, "Spot Open",  "0", COLORS["text"])
        self.sb_fut_pos = self._sb_metric(_c, "Futures Open", "0", COLORS["text"])
        self.sb_cross_pos = self._sb_metric(_c, "Cross Open", "0", COLORS["text"])
        self.sb_futrend_pos = self._sb_metric(_c, "Future Trend Open", "0", COLORS["text"])

        # System monitor card.
        _c = self._sb_card(inner)
        self._sb_section(_c, "SYSTEM MONITOR")
        bars_box = ctk.CTkFrame(_c, fg_color="transparent")
        bars_box.pack(fill="x", pady=(0, 2))
        self.bar_cpu  = MiniBar(bars_box, "CPU",  COLORS["balanced"],   height=20)
        self.bar_cpu.pack(fill="x", pady=1)
        self.bar_ram  = MiniBar(bars_box, "RAM",  COLORS["aggressive"], height=20)
        self.bar_ram.pack(fill="x", pady=1)
        self.bar_gpu  = MiniBar(bars_box, "GPU",  COLORS["purple"],     height=20)
        self.bar_gpu.pack(fill="x", pady=1)
        self.bar_vram = MiniBar(bars_box, "VRAM", COLORS["violet"],     height=20)
        self.bar_vram.pack(fill="x", pady=1)

        # Larger font for GPU model text.
        self.gpu_name_lbl = ctk.CTkLabel(bars_box, text="",
                                          font=ctk.CTkFont(self.mono_font, 11, "bold"),
                                          text_color=COLORS["text_dim"], anchor="w")
        self.gpu_name_lbl.pack(fill="x", pady=(2, 0))

        # Connections card.
        _c = self._sb_card(inner)
        self._sb_section(_c, "CONNECTIONS")
        self.sb_db = self._sb_status(_c, "Database", "", COLORS["text_muted"])
        self.sb_exchange = self._sb_status(_c, "Exchange", "", COLORS["text_muted"])
        self.sb_llm = self._sb_status(_c, "LLM", "", COLORS["text_muted"])

        # Larger font for LLM model text.
        self.sb_llm_model = ctk.CTkLabel(_c, text="",
                                          font=ctk.CTkFont(self.mono_font, 11, "bold"),
                                          text_color=COLORS["text_dim"], anchor="w")
        self.sb_llm_model.pack(fill="x", pady=(2, 2))

        ctk.CTkButton(_c, text="Change Model", height=22, corner_radius=4,
                       font=ctk.CTkFont(FONT_BODY, 9, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_muted"],
                       border_width=1, border_color=COLORS["border"],
                       command=self._show_model_selector
                       ).pack(fill="x", pady=(0, 3))

        # Errors card.
        _c = self._sb_card(inner)
        err_row = ctk.CTkFrame(_c, fg_color="transparent")
        err_row.pack(fill="x", pady=(0, 2))

        # Slightly larger font for error status.
        ctk.CTkLabel(err_row, text=" Errors",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_subtle"], anchor="w"
                      ).pack(side="left")
        self.sb_errors = ctk.StringVar(value="No errors")
        err_lbl = ctk.CTkLabel(err_row, textvariable=self.sb_errors,
                                font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                                text_color=COLORS["success"], anchor="e",
                                cursor="hand2")
        err_lbl.pack(side="right")
        err_lbl.bind("<Button-1>", lambda e: self._show_error_log())
        self.sb_errors._lbl = err_lbl

        err_box = ctk.CTkFrame(_c, fg_color=COLORS["bg_alt"], corner_radius=5,
                                  border_width=1, border_color=COLORS["border_soft"])
        err_box.pack(fill="x", pady=(0, 4))
        self.sb_errors_text = ctk.CTkLabel(err_box, text="No recent errors",
                                              font=ctk.CTkFont(FONT_BODY, 10, slant="italic"),
                                              text_color=COLORS["text_subtle"], anchor="w")
        self.sb_errors_text.pack(fill="x", padx=8, pady=6)

        # TOOLS  gerahmte Karte
        _c = self._sb_card(inner)
        self._sb_section(_c, "TOOLS")
        tools_box = ctk.CTkFrame(_c, fg_color="transparent")
        tools_box.pack(fill="x", pady=(0, 2))

        bt_btn = ctk.CTkButton(
            tools_box, text=" Run Backtest", height=26, corner_radius=5,
            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
            fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
            text_color=COLORS["balanced"],
            border_width=1, border_color=COLORS["border"],
            command=self._open_backtest_dialog
        )
        bt_btn.pack(fill="x", pady=(0, 3))
        attach_tooltip(
            bt_btn,
            "Run a historical backtest with current parameters.\n"
            "Useful after editing the AI prompt.",
            delay_ms=400
        )

        opt_btn = ctk.CTkButton(
            tools_box, text=" Optimize Parameters", height=26, corner_radius=5,
            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
            fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
            text_color=COLORS["aggressive"],
            border_width=1, border_color=COLORS["border"],
            command=self._open_optimizer_dialog
        )
        opt_btn.pack(fill="x", pady=(0, 3))
        attach_tooltip(
            opt_btn,
            "Run K-Fold parameter optimization.\n"
            "5-90 min runtime depending on settings.",
            delay_ms=400
        )

        heatmap_btn = ctk.CTkButton(
            tools_box, text=" Win/Loss Heatmap", height=26, corner_radius=5,
            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
            fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
            text_color="#60a5fa",
            border_width=1, border_color=COLORS["border"],
            command=self._open_heatmap_dialog
        )
        heatmap_btn.pack(fill="x", pady=(0, 3))
        attach_tooltip(
            heatmap_btn,
            "Show win rate by hour of day and day of week.\n"
            "Helps identify when your bots perform best/worst.",
            delay_ms=400
        )

        selftest_btn = ctk.CTkButton(
            tools_box, text=" Run Self-Test", height=26, corner_radius=5,
            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
            fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
            text_color="#22c55e",
            border_width=1, border_color=COLORS["border"],
            command=self._open_selftest_dialog
        )
        selftest_btn.pack(fill="x", pady=(0, 3))
        attach_tooltip(
            selftest_btn,
            "Run the built-in test suite (pytest).\n"
            "Verifies the money-path invariants. Run after code edits,\n"
            "before going live.",
            delay_ms=400
        )

        env_btn = ctk.CTkButton(
            tools_box, text=" Env Settings (.env)", height=26, corner_radius=5,
            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
            fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
            text_color=COLORS["text_muted"],
            border_width=1, border_color=COLORS["border"],
            command=self._open_env_reference
        )
        env_btn.pack(fill="x", pady=(0, 3))
        attach_tooltip(
            env_btn,
            "Open env_parameter.txt  the full reference of every .env\n"
            "setting you can change and what it does (English).",
            delay_ms=400
        )

        # KEINE Status/Time section  Time wird im Footer angezeigt
        # Backward-Compat: dummy StringVar damit Refresh-Loop nicht crasht
        self.sb_clk = ctk.StringVar(value="")

    def _open_env_reference(self):
        """Open env_parameter.txt (the full .env reference) in the OS default
        text viewer. Project root = three levels up from launcher/ui/app.py."""
        import os, sys, subprocess
        root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))))
        path = os.path.join(root, "env_parameter.txt")
        try:
            if not os.path.exists(path):
                print(f"[Env] env_parameter.txt not found at {path}")
                return
            if sys.platform.startswith("win"):
                os.startfile(path)            # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            print(f"[Env] could not open {path}: {e}")

    def _sb_card(self, parent):
        """Framed sidebar section container; returns its inner frame."""
        card = ctk.CTkFrame(parent, fg_color=COLORS["panel_alt"],
                             border_color=COLORS["border"], border_width=1,
                             corner_radius=10)
        card.pack(fill="x", pady=(0, 8))
        inner = ctk.CTkFrame(card, fg_color="transparent")
        inner.pack(fill="x", padx=11, pady=9)
        return inner

    def _sb_section(self, parent, text):
        ctk.CTkLabel(parent, text=text,
                      font=ctk.CTkFont(self.display_font, 10, "bold"),
                      text_color=COLORS["text_dim"], anchor="w"
                      ).pack(fill="x", pady=(2, 4))

    def _sb_metric(self, parent, label, value, color):
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", pady=2)
        ctk.CTkLabel(row, text=label,
                      font=ctk.CTkFont(FONT_BODY, 11, "normal"),
                      text_color=COLORS["text_muted"], anchor="w"
                      ).pack(side="left")
        var = ctk.StringVar(value=value)
        lbl = ctk.CTkLabel(row, textvariable=var,
                            font=ctk.CTkFont(self.mono_font, 10, "bold"),
                            text_color=color, anchor="e")
        lbl.pack(side="right")
        var._lbl  = lbl
        var._wrap = row  # Reference for pack_forget/pack.
        return var

    def _sb_metric_big(self, parent, label, value, color):
        wrap = ctk.CTkFrame(parent, fg_color="transparent")
        wrap.pack(fill="x", pady=(0, 4))  # reduziert
        header_lbl = ctk.CTkLabel(wrap, text=label,
                      font=ctk.CTkFont(self.display_font, 10, "bold"),
                      text_color=COLORS["text_muted"], anchor="w")
        header_lbl.pack(fill="x")
        var = ctk.StringVar(value=value)
        lbl = ctk.CTkLabel(wrap, textvariable=var,
                            font=ctk.CTkFont(self.mono_font, 16, "bold"),
                            text_color=color, anchor="w")
        lbl.pack(fill="x", pady=(0, 0))
        var._lbl        = lbl
        var._header_lbl = header_lbl  # Reference for dynamic label changes.
        var._wrap  = wrap  # Reference for pack_forget/pack.
        return var

    def _sb_status(self, parent, label, value, color):
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", pady=1)
        ctk.CTkLabel(row, text=label,
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_muted"], anchor="w"
                      ).pack(side="left")
        var = ctk.StringVar(value=value)
        lbl = ctk.CTkLabel(row, textvariable=var,
                            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                            text_color=color, anchor="e")
        lbl.pack(side="right")
        var._lbl = lbl
        return var

    #  MAIN  Bot-Cards in dynamischem Grid 

    MAX_CARD_COLUMNS = 3

    def _card_columns_for_width(self, width: int) -> int:
        """Responsive card grid tuned for readable parameter controls."""
        try:
            width = int(width)
        except (TypeError, ValueError):
            width = 0
        if width < 900:
            return 1
        if width < 1350:
            return 2
        return self.MAX_CARD_COLUMNS

    def _build_main(self):
        main = ctk.CTkScrollableFrame(
            self,
            fg_color=COLORS["bg"],
            scrollbar_button_color=COLORS["border"],
            scrollbar_button_hover_color=COLORS["text_muted"],
        )
        main.grid(row=1, column=1, sticky="nsew", padx=20, pady=20)
        self._current_card_columns = self.MAX_CARD_COLUMNS
        for i in range(self.MAX_CARD_COLUMNS):
            main.grid_columnconfigure(i, weight=1)
        for i in range((len(BOT_ORDER) + self.MAX_CARD_COLUMNS - 1) // self.MAX_CARD_COLUMNS):
            main.grid_rowconfigure(i, weight=0)
        self.main_frame = main
        main.bind("<Configure>", self._on_main_resize)
        main.bind("<Configure>", lambda _e: self.after_idle(self._refresh_main_scrollregion), add="+")
        self._bind_card_area_mousewheel(main)

        for col, bot in enumerate(BOT_ORDER):
            self.cards[bot] = self._build_bot_card(main, bot, col)
        self.after_idle(self._refresh_main_scrollregion)

    def _bind_card_area_mousewheel(self, frame) -> None:
        """Make mouse-wheel scrolling work over child widgets in bot cards."""
        canvas = getattr(frame, "_parent_canvas", None)
        if canvas is None:
            return
        state = {"inside": False}

        def _is_log_text(widget) -> bool:
            while widget is not None:
                if isinstance(widget, tk.Text):
                    return True
                widget = getattr(widget, "master", None)
            return False

        def _pointer_over_canvas(event) -> bool:
            try:
                x_root = int(getattr(event, "x_root", canvas.winfo_pointerx()))
                y_root = int(getattr(event, "y_root", canvas.winfo_pointery()))
                left = int(canvas.winfo_rootx())
                top = int(canvas.winfo_rooty())
                right = left + int(canvas.winfo_width())
                bottom = top + int(canvas.winfo_height())
                return left <= x_root <= right and top <= y_root <= bottom
            except Exception:
                return bool(state["inside"])

        def _wheel(event):
            if not state["inside"] and not _pointer_over_canvas(event):
                return None
            if _is_log_text(getattr(event, "widget", None)):
                return None
            speed = 36
            if getattr(event, "num", None) == 4:
                units = -speed
            elif getattr(event, "num", None) == 5:
                units = speed
            else:
                delta = int(getattr(event, "delta", 0) or 0)
                if delta == 0:
                    return None
                steps = max(1, abs(delta) // 120) * speed
                units = -steps if delta > 0 else steps
            canvas.yview_scroll(units, "units")
            self.after_idle(self._refresh_main_scrollregion)
            return "break"

        def _enter(_event=None):
            state["inside"] = True

        def _leave(_event=None):
            state["inside"] = False

        # Bind immediately: users often start the wheel over a child widget,
        # where the scroll frame itself may never receive the first Enter event.
        self.bind_all("<MouseWheel>", _wheel, add="+")
        self.bind_all("<Button-4>", _wheel, add="+")
        self.bind_all("<Button-5>", _wheel, add="+")
        frame.bind("<Enter>", _enter)
        frame.bind("<Leave>", _leave)

    def _refresh_main_scrollregion(self) -> None:
        """Force CTk's canvas to know the full dynamic card grid height."""
        frame = getattr(self, "main_frame", None)
        if frame is None:
            return
        canvas = getattr(frame, "_parent_canvas", None)
        if canvas is None:
            return
        try:
            frame.update_idletasks()
            bbox = canvas.bbox("all")
            req_w = max(canvas.winfo_width(), frame.winfo_reqwidth())
            req_h = max(canvas.winfo_height(), frame.winfo_reqheight()) + 24
            if bbox:
                canvas.configure(
                    scrollregion=(
                        min(0, int(bbox[0])),
                        min(0, int(bbox[1])),
                        max(req_w, int(bbox[2])),
                        max(req_h, int(bbox[3]) + 24),
                    )
                )
            else:
                canvas.configure(scrollregion=(0, 0, req_w, req_h))
        except Exception:
            pass

    def _on_main_resize(self, event):
        cols = self._card_columns_for_width(getattr(event, "width", 0))
        if cols != getattr(self, "_current_card_columns", None):
            self._current_card_columns = cols
            self._refresh_visibility_layout()
        else:
            self.after_idle(self._refresh_main_scrollregion)

    def _build_bot_card(self, parent, name: str, col: int):
        meta = BOT_META[name]
        accent = meta["accent"]
        is_futures = meta["is_futures"]

        card = ctk.CTkFrame(parent, fg_color=COLORS["panel"], corner_radius=14,
                             border_width=1, border_color=COLORS["border"])
        # Initial placement is corrected by _refresh_visibility_layout after
        # all cards exist and persisted visibility is known.
        grid_col = col % self.MAX_CARD_COLUMNS
        grid_row = col // self.MAX_CARD_COLUMNS
        card.grid(row=grid_row, column=grid_col, sticky="new", padx=8, pady=(0, 14))
        card.grid_columnconfigure(0, weight=1)
        # Row 3 (params) and row 9 (log) share vertical space. The cards area
        # scrolls, so five expanded bot cards keep their natural height instead
        # of clipping parameter rows on smaller displays.
        card.grid_rowconfigure(3, weight=2, minsize=390)

        #  ROW 0: Header 
        head = ctk.CTkFrame(card, fg_color="transparent")
        head.grid(row=0, column=0, sticky="ew", padx=22, pady=(20, 6))
        head.grid_columnconfigure(0, weight=1)
        head.grid_columnconfigure(1, weight=0)

        name_block = ctk.CTkFrame(head, fg_color="transparent")
        name_block.grid(row=0, column=0, sticky="nw")
        ctk.CTkLabel(name_block, text=meta["label"],
                      font=ctk.CTkFont(self.display_font, 19, "bold"),
                      text_color=COLORS["text"], anchor="w"
                      ).pack(anchor="w")
        # Signature underline  the one accent that carries "Obsidian"
        # (brighter violet from the logo arrowhead, slightly bolder).
        ctk.CTkFrame(name_block, fg_color=COLORS["violet"], height=3, width=48,
                      corner_radius=2).pack(anchor="w", pady=(5, 2))

        subtitle_row = ctk.CTkFrame(name_block, fg_color="transparent")
        subtitle_row.pack(anchor="w", pady=(2, 0))
        ctk.CTkLabel(subtitle_row, text=meta["subtitle"],
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(side="left")

        status_row = ctk.CTkFrame(name_block, fg_color="transparent")
        status_row.pack(anchor="w", pady=(4, 0))
        led = ctk.CTkLabel(status_row, text="",
                            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                            text_color=COLORS["text_muted"])
        led.pack(side="left", padx=(0, 6))
        status_var = ctk.StringVar(value="Stopped")
        ctk.CTkLabel(status_row, textvariable=status_var,
                      font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(side="left")

        # AI-Mode Badge (LLM vs Keyword Fallback)
        # Wird in _refresh aktualisiert basierend auf Ollama-Status + Bot-Logs
        ctk.CTkLabel(status_row, text="  ",
                      font=ctk.CTkFont(FONT_BODY, 11),
                      text_color=COLORS["text_subtle"]
                      ).pack(side="left", padx=(8, 0))
        ai_badge_var = ctk.StringVar(value="AI: ")
        ai_badge = ctk.CTkLabel(status_row, textvariable=ai_badge_var,
                                  font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                                  text_color=COLORS["text_muted"])
        ai_badge.pack(side="left")
        attach_tooltip(
            ai_badge,
            "Decision engine for this bot:\n"
            " LLM = Ollama AI is analyzing each setup (full prompt)\n"
            " Keyword = Fallback mode using keyword rules only\n"
            "  (used when LLM is offline or unreachable)",
            delay_ms=400
        )

        action_frame = ctk.CTkFrame(head, fg_color="transparent")
        action_frame.grid(row=0, column=1, sticky="ne", padx=(8, 0))

        prompt_btn = None
        if meta.get("uses_llm", True):
            prompt_btn = ctk.CTkButton(
                action_frame, text="Prompt",
                width=58, height=22, corner_radius=4,
                font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                fg_color="transparent", hover_color=COLORS["panel_hover"],
                text_color=COLORS["text_dim"],
                border_width=1, border_color=COLORS["border"],
                command=lambda n=name: self._open_prompt_editor(n)
            )
            prompt_btn.pack(side="left", padx=(0, 5))
            attach_tooltip(
                prompt_btn,
                "Edit the AI prompt for this bot.\n"
                "Changes are saved to prompts/*.txt and loaded fresh on the\n"
                "next LLM call  restart the bot to be safe.",
                delay_ms=400
            )

        if name == "CROSS":
            reb = ctk.CTkButton(
                action_frame, text="Rebal",
                width=58, height=22, corner_radius=4,
                font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                fg_color="#6D28D9", hover_color="#5b21b6",
                text_color="#ffffff", border_width=0,
                command=self._force_cross_rebalance
            )
            reb.pack(side="left", padx=(0, 5))
            attach_tooltip(
                reb,
                "Force CROSS to rebuild its long/short book NOW\n"
                "(re-ranks the universe, dollar-neutral). Acts within ~60s,\n"
                "no restart. Use when the book opened short  e.g. a target\n"
                "coin was delisting. Only works while CROSS is running.",
                delay_ms=400
            )

        emerg = None
        if is_futures:
            emerg = ctk.CTkButton(
                action_frame, text="Close Pos",
                width=76, height=22, corner_radius=4,
                font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                fg_color=COLORS["danger"], hover_color="#b91c1c",
                text_color="#ffffff",
                border_width=0,
                command=lambda n=name: self._emergency_close_futures_and_stop(n)
            )
            emerg.pack(side="left", padx=(0, 5))
            self.emergency_btns[name] = emerg
            if name == "FUTURES":
                self.emergency_btn = emerg
            attach_tooltip(
                emerg,
                f"Immediately stop the {meta['label']} bot.\n"
                "In SIMULATION: clears all virtual positions.\n"
                "In LIVE: closes positions with reduceOnly orders.",
                delay_ms=400
            )

        # SIM/LIVE Badge  cyan im SIM-Mode wie React-Mockup
        is_sim = self.config[name].get("SIMULATION", True)
        sim_btn = ctk.CTkButton(
            action_frame, text="SIM" if is_sim else "LIVE",
            width=60, height=22, corner_radius=4,
            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
            fg_color="#1b1730" if is_sim else "#2d0f0c",       # indigo-tint / red-tint
            hover_color="#241f40" if is_sim else "#3d1410",
            text_color="#b07ae0" if is_sim else "#e88a6a",     # violet (paper) / warm rose
            border_width=0,
            command=lambda n=name: self._toggle_simulation(n)
        )
        sim_btn.pack(side="left")
        attach_tooltip(
            sim_btn,
            "Toggle SIMULATION  LIVE mode.\n"
            "SIM: paper trading with a fake 1000 USDT balance.\n"
            "LIVE: real money via the configured exchange API.\n"
            "Switching to LIVE requires explicit confirmation.",
            delay_ms=400
        )

        # Collapse-Toggle entfernt: Karten lassen sich nicht mehr zu einem
        # schmalen Streifen einklappen. Die _collapsed-Struktur bleibt als
        # immer-False bestehen, damit _persist_ui_prefs() und der Start-Restore
        # weiterhin sauber laufen.

        #  ROW 1: PnL Hero in tinted Container-Box 
        hero_wrap = ctk.CTkFrame(card, fg_color=COLORS["bg"], corner_radius=10,
                                    border_width=1, border_color=COLORS["border_soft"])
        hero_wrap.grid(row=1, column=0, sticky="ew", padx=18, pady=(10, 10))

        hero = ctk.CTkFrame(hero_wrap, fg_color="transparent")
        hero.pack(fill="x", padx=14, pady=12)
        hero.grid_columnconfigure(0, weight=1)

        pnl_block = ctk.CTkFrame(hero, fg_color="transparent")
        pnl_block.grid(row=0, column=0, sticky="w")
        pnl_title_var = ctk.StringVar(value="REALIZED")
        ctk.CTkLabel(pnl_block, textvariable=pnl_title_var,
                      font=ctk.CTkFont(self.display_font, 10, "bold"),
                      text_color=COLORS["text_dim"], anchor="w"
                      ).pack(fill="x", pady=(0, 3))

        # Profit inline mit USDT-Suffix daneben (statt darunter)
        pnl_row = ctk.CTkFrame(pnl_block, fg_color="transparent")
        pnl_row.pack(anchor="w")
        pnl_var = ctk.StringVar(value="+0.00")
        pnl_lbl = ctk.CTkLabel(pnl_row, textvariable=pnl_var,
                                font=ctk.CTkFont(self.mono_font, 30, "bold"),
                                text_color=COLORS["text_dim"], anchor="w")
        pnl_lbl.pack(side="left")
        ctk.CTkLabel(pnl_row, text=" USDT",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack(side="left", anchor="s", pady=(0, 6))

        #  Unrealized PnL (Live-Wert offener Positionen) 
        unr_row = ctk.CTkFrame(pnl_block, fg_color="transparent")
        unr_row.pack(anchor="w", pady=(4, 0))
        ctk.CTkLabel(unr_row, text="UNREALIZED",
                      font=ctk.CTkFont(FONT_BODY, 9, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack(side="left", padx=(0, 6))
        unr_var = ctk.StringVar(value="")
        unr_lbl = ctk.CTkLabel(unr_row, textvariable=unr_var,
                                font=ctk.CTkFont(self.mono_font, 13, "bold"),
                                text_color=COLORS["text_muted"])
        unr_lbl.pack(side="left")
        ctk.CTkLabel(unr_row, text="USDT",
                      font=ctk.CTkFont(FONT_BODY, 9, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack(side="left", anchor="s", pady=(0, 3), padx=(4, 0))
        attach_tooltip(unr_lbl,
                        "Aktueller nicht-realisierter Gewinn/Verlust aller offenen Positionen.\n"
                        "Futures: Live aus der Positions-Tabelle.\n"
                        "Spot: berechnet aus aktuellen Tickerpreisen (alle 15s).\n"
                        " = keine offenen Positionen.",
                        delay_ms=400)

        # 4 Mini-Stats  kompakter und ohne eigene Container
        stats = ctk.CTkFrame(hero, fg_color="transparent")
        stats.grid(row=0, column=1, sticky="e")

        open_var = ctk.StringVar(value="0")
        total_var = ctk.StringVar(value="0")
        today_var = ctk.StringVar(value="0")
        wr_var = ctk.StringVar(value="")

        cells_data = [("OPEN", open_var, COLORS["text"]),
                       ("TOTAL", total_var, COLORS["text"]),
                       ("TODAY", today_var, COLORS["text"]),
                       ("WIN-%", wr_var, accent)]
        for col_i, (label, var, col_color) in enumerate(cells_data):
            cell = ctk.CTkFrame(stats, fg_color="transparent")
            cell.grid(row=0, column=col_i, padx=10)
            ctk.CTkLabel(cell, text=label,
                          font=ctk.CTkFont(self.display_font, 9, "bold"),
                          text_color=COLORS["text_muted"]
                          ).pack()
            ctk.CTkLabel(cell, textvariable=var,
                          font=ctk.CTkFont(self.mono_font, 14, "bold"),
                          text_color=col_color
                          ).pack(pady=(2, 0))

        #  PAYOFF-Zelle (-Win / |-Loss|)  die Edge-Kennzahl 
        payoff_var  = ctk.StringVar(value="")
        payoff_detail = ctk.StringVar(value="")
        payoff_cell = ctk.CTkFrame(stats, fg_color="transparent")
        payoff_cell.grid(row=0, column=len(cells_data), padx=10)
        ctk.CTkLabel(payoff_cell, text="PAYOFF",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack()
        payoff_lbl = ctk.CTkLabel(payoff_cell, textvariable=payoff_var,
                                   font=ctk.CTkFont(self.mono_font, 14, "bold"),
                                   text_color=COLORS["text_dim"])
        payoff_lbl.pack(pady=(2, 0))
        ctk.CTkLabel(payoff_cell, textvariable=payoff_detail,
                      font=ctk.CTkFont(self.mono_font, 8, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack()
        attach_tooltip(payoff_lbl,
                        "Payoff-Faktor = -Gewinn / |-Verlust|.\n"
                        "Grn = positiver Erwartungswert bei aktueller Win-Rate,\n"
                        "Gelb = grenzwertig, Rot = negativer Erwartungswert.\n"
                        "Faustregel: bei 67% WR brauchst du  ~0.49.",
                        delay_ms=400)

        #  Sparkline: kumulierte PnL-Kurve der letzten ~30 Trades 
        # row=2 ist frei (hero_wrap=1, param_wrap=3). Gerahmtes Mini-Chart-
        # Panel, damit die Linie nicht "schwebt", sondern in einer klar
        # abgegrenzten Flche liegt (wie ein kleines Chart-Widget).
        spark_wrap = ctk.CTkFrame(card, fg_color=COLORS["panel_alt"],
                                   border_color=COLORS["border"], border_width=1,
                                   corner_radius=10)
        spark_wrap.grid(row=2, column=0, sticky="ew", padx=18, pady=(0, 8))
        spark_wrap.grid_columnconfigure(0, weight=1)

        spark_head = ctk.CTkFrame(spark_wrap, fg_color="transparent")
        spark_head.grid(row=0, column=0, sticky="ew", padx=12, pady=(7, 1))
        ctk.CTkLabel(spark_head, text="PNL TREND",
                      font=ctk.CTkFont(self.display_font, 10, "bold"),
                      text_color=COLORS["text_dim"]
                      ).pack(side="left")
        ctk.CTkLabel(spark_head, text="last ~30 closed trades",
                      font=ctk.CTkFont(FONT_BODY, 8, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack(side="right")

        sparkline = Sparkline(spark_wrap, height=40)
        sparkline.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 8))
        attach_tooltip(sparkline,
                        "Kumulierter realisierter PnL ueber die letzten ~30\n"
                        "abgeschlossenen Trades. Gruen = aktuell im Plus,\n"
                        "rot = im Minus. Aktualisiert alle 15s.",
                        delay_ms=400)
        # Alias used by body_widgets visibility handling.
        spark_row = spark_wrap

        #  ROW 3: Parameters in tinted Container-Box 
        # Sektion-Wrapper mit dunklem Hintergrund (entspricht bg-black/20 im Redesign)
        # COLLAPSIBLE: click the header chevron to fold the params away and
        # give the activity log all the vertical space. The collapsed state
        # is per-bot and persists in app.config["UI"]["params_collapsed"][name].
        param_wrap = ctk.CTkFrame(card, fg_color="#0a0e15", corner_radius=10,
                                     border_width=1, border_color=COLORS["border_soft"])
        param_wrap.grid(row=3, column=0, sticky="nsew", padx=18, pady=(0, 8))

        # Read persisted collapsed state (default: expanded)
        try:
            collapsed_state = (self.config.get("UI", {})
                                .get("params_collapsed", {})
                                .get(name, False))
        except Exception:
            collapsed_state = False
        collapsed_flag = {"value": bool(collapsed_state)}

        # Header IM Container
        param_header = ctk.CTkFrame(param_wrap, fg_color="transparent")
        param_header.pack(fill="x", padx=14, pady=(10, 6))

        # Chevron toggle  clickable label that expands/collapses the body
        chevron_var = ctk.StringVar(
            value=("> PARAMETERS" if collapsed_flag["value"]
                   else "v PARAMETERS")
        )
        chevron_lbl = ctk.CTkLabel(
            param_header, textvariable=chevron_var,
            font=ctk.CTkFont(self.display_font, 10, "bold"),
            text_color=COLORS["text_dim"], cursor="hand2",
        )
        chevron_lbl.pack(side="left")

        unsaved_lbl = ctk.CTkLabel(param_header, text="",
                                    font=ctk.CTkFont(FONT_BODY, 9, "bold"),
                                    text_color=COLORS["warning"])
        unsaved_lbl.pack(side="left", padx=(10, 0))

        ctk.CTkButton(param_header, text=" Default",
                       width=72, height=22, corner_radius=5,
                       font=ctk.CTkFont(FONT_BODY, 9, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_muted"],
                       border_width=1, border_color=COLORS["border"],
                       command=lambda n=name: self._reset_params(n)
                       ).pack(side="right", padx=(4, 0))

        save_btn = ctk.CTkButton(
            param_header, text=" Save",
            width=72, height=22, corner_radius=5,
            font=ctk.CTkFont(FONT_BODY, 9, "bold"),
            fg_color="transparent", hover_color=COLORS["panel_hover"],
            text_color=accent, border_width=2, border_color=accent,
            command=lambda n=name: self._save_params(n)
        )
        save_btn.pack(side="right")

        #  Parameters (2 Spalten) IM Container 
        params_box = ctk.CTkFrame(param_wrap, fg_color="transparent")
        params_box.pack(fill="x", padx=10, pady=(2, 12))
        params_box.grid_columnconfigure(0, weight=1, uniform="params")
        params_box.grid_columnconfigure(1, weight=1, uniform="params")

        col_left = ctk.CTkFrame(params_box, fg_color="transparent")
        col_right = ctk.CTkFrame(params_box, fg_color="transparent")
        col_left.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        col_right.grid(row=0, column=1, sticky="nsew", padx=(5, 0))

        # Param-Set abhngig von Bot-Typ. TREND ist jetzt der "Trend"-Bot
        # (eigene Parameter), SPOT bleibt der Momentum-Spot-Bot.
        if name == "CROSS":
            params = PARAM_DEFS_CROSS
        elif name == "FUTREND":
            params = PARAM_DEFS_FUTREND
        elif is_futures:
            params = PARAM_DEFS_FUTURES
        elif name == "TREND":
            params = PARAM_DEFS_TREND
        else:
            params = PARAM_DEFS_SPOT
        split = (len(params) + 1) // 2

        for i, param_def in enumerate(params):
            # Defs sind 8-tuple (mit tooltip)  7-tuple fallback for safety
            if len(param_def) == 8:
                key, label, step, vmin, vmax, fmt, unit, tip = param_def
            else:
                key, label, step, vmin, vmax, fmt, unit = param_def
                tip = ""
            default_val = DEFAULT_CONFIG[name].get(key, 0)
            current = self.config[name].get(key, default_val)
            target = col_left if i < split else col_right
            row = ParamRow(
                target, self.mono_font,
                key, label, current, step, vmin, vmax, fmt, unit, accent,
                on_change=lambda k, v, n=name: self._on_param_change(n, k, v),
                tooltip=tip
            )
            row.pack(fill="x", pady=1)
            self.param_rows[name][key] = row

        #  Blocked Hours (UTC)  full-width text entry 
        # Thin divider so the row is visually separated from the numeric params
        ctk.CTkFrame(params_box, fg_color=COLORS["border_soft"], height=1
                     ).grid(row=1, column=0, columnspan=2, sticky="ew",
                            padx=0, pady=(6, 4))

        bhr = BadHoursRow(params_box, bot_name=name, accent=accent,
                          mono_font=self.mono_font)
        bhr.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.bad_hours_rows[name] = bhr

        # COLLAPSIBLE: bind the chevron click to fold/unfold the body.
        #
        # KEY INSIGHT: hiding ``params_box`` with ``pack_forget()`` shrinks
        # only the box itself  but ``params_wrap`` still sits in row=3 of
        # the card grid, which was configured with ``minsize=390``. Tk's
        # grid manager honours that minsize regardless of what's inside,
        # so the log (row=9) couldn't grow.
        #
        # The fix has TWO parts:
        #  1) Hide the body (pack_forget)  already worked
        #   2) Reset row 3's minsize to 0 AND weight to 0 so the row
        #      shrinks to just the header (~36px). The log row already
        #      has weight=1, so it expands to fill the freed space.
        # When expanding, restore the original minsize=390, weight=2.
        _PARAMS_ROW_EXPANDED  = {"weight": 2, "minsize": 390}
        _PARAMS_ROW_COLLAPSED = {"weight": 0, "minsize": 0}

        def _toggle_params(_event=None, n=name, body=params_box,
                            cv=chevron_var, flag=collapsed_flag,
                            the_card=card):
            new_state = not flag["value"]
            flag["value"] = new_state
            if new_state:
                body.pack_forget()
                cv.set("> PARAMETERS")
                the_card.grid_rowconfigure(3, **_PARAMS_ROW_COLLAPSED)
            else:
                body.pack(fill="x", padx=10, pady=(2, 12))
                cv.set("v PARAMETERS")
                the_card.grid_rowconfigure(3, **_PARAMS_ROW_EXPANDED)
            # Persist so the user's choice survives launcher restart
            try:
                ui_cfg = self.config.setdefault("UI", {})
                cs = ui_cfg.setdefault("params_collapsed", {})
                cs[n] = new_state
                self.config = save_config_merge(
                    {"UI": {"params_collapsed": dict(cs)}})
            except Exception:
                pass
            self.after_idle(self._refresh_main_scrollregion)

        chevron_lbl.bind("<Button-1>", _toggle_params)
        # Also let the user click the "PARAMETERS" label text itself, not
        # just the tiny chevron  bigger hit target.
        chevron_lbl.configure(cursor="hand2")

        # Apply initial collapsed state if persisted  must adjust BOTH
        # the body visibility and the row weight/minsize.
        if collapsed_flag["value"]:
            params_box.pack_forget()
            card.grid_rowconfigure(3, **_PARAMS_ROW_COLLAPSED)

        #  ROW 5: Restart Hint 
        restart_hint = ctk.CTkLabel(card, text="",
                                     font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                                     text_color=COLORS["warning"], anchor="w")
        restart_hint.grid(row=5, column=0, sticky="ew", padx=22, pady=(2, 0))

        #  ROW 6: Separator 
        sep_6 = ctk.CTkFrame(card, fg_color=COLORS["border_soft"], height=1)
        sep_6.grid(row=6, column=0, sticky="ew", padx=22, pady=(8, 0))

        #  ROW 7: Action Buttons (kompakter) 
        actions = ctk.CTkFrame(card, fg_color="transparent", height=46)
        actions.grid(row=7, column=0, sticky="ew", padx=18, pady=8)
        actions.grid_propagate(False)
        actions.pack_propagate(False)

        # Container mit pack  die 3 Buttons teilen sich die Breite gleich auf
        btn_row = ctk.CTkFrame(actions, fg_color="transparent")
        btn_row.pack(fill="both", expand=True)

        start_btn = ctk.CTkButton(
            btn_row, text="Start", height=32, corner_radius=6,
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            fg_color=COLORS["success"],
            hover_color="#0d9b6c",
            text_color="#ffffff",
            border_width=2,
            border_color="#a7f3d0",
            command=lambda n=name: self._start_bot(n)
        )
        start_btn.pack(side="left", fill="both", expand=True, padx=(0, 3))

        restart_btn = ctk.CTkButton(
            btn_row, text="Restart", height=32, corner_radius=6,
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            fg_color="transparent",
            hover_color="#3a2a10",
            text_color=COLORS["warning"],
            border_width=2, border_color=COLORS["warning"],
            command=lambda n=name: self._restart_bot(n)
        )
        restart_btn.pack(side="left", fill="both", expand=True, padx=3)

        stop_btn = ctk.CTkButton(
            btn_row, text="Stop", height=32, corner_radius=6,
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            fg_color="transparent",
            hover_color="#3a1820",
            text_color=COLORS["danger"],
            border_width=2, border_color=COLORS["danger"],
            command=lambda n=name: self._stop_bot(n)
        )
        stop_btn.pack(side="left", fill="both", expand=True, padx=(3, 0))

        #  ROW 8: Separator 
        sep_8 = ctk.CTkFrame(card, fg_color=COLORS["border_soft"], height=1)
        sep_8.grid(row=8, column=0, sticky="ew", padx=22)

        #  ROW 9: Activity Log  flexible height, grows with window 
        # Log has weight=1 (half of params' weight=2) with a compact minimum, so
        # the log and the params section scale together instead of the log
        # taking a fixed slice and clipping the bottom parameter rows.
        log_outer = ctk.CTkFrame(card, fg_color="transparent")
        log_outer.grid(row=9, column=0, sticky="nsew", padx=22, pady=(8, 18))
        card.grid_rowconfigure(9, weight=1, minsize=118)
        log_outer.grid_columnconfigure(0, weight=1)
        log_outer.grid_rowconfigure(1, weight=1)

        log_head = ctk.CTkFrame(log_outer, fg_color="transparent")
        log_head.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        ctk.CTkLabel(log_head, text="ACTIVITY LOG",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(side="left")

        auto_var = ctk.BooleanVar(value=True)
        ctk.CTkSwitch(log_head, text="Auto", variable=auto_var,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       text_color=COLORS["text_dim"],
                       progress_color=accent,
                       button_color=COLORS["text"], width=36, height=18
                       ).pack(side="right", padx=(8, 0))

        ctk.CTkButton(log_head, text="Clear", width=58, height=22,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_muted"],
                       border_width=1, border_color=COLORS["border"], corner_radius=4,
                       command=lambda n=name: self._clear_card_log(n)
                       ).pack(side="right")

        log_wrap = ctk.CTkFrame(log_outer, fg_color=COLORS["bg"], corner_radius=8)
        log_wrap.grid(row=1, column=0, sticky="nsew")

        log_box = tk.Text(
            log_wrap, bg=COLORS["bg"], fg=COLORS["text_dim"],
            font=(self.mono_font, 11, "bold"),
            insertbackground=COLORS["text"],
            highlightthickness=0, bd=0, relief="flat",
            padx=12, pady=10, wrap="word",
            selectbackground=COLORS["border"]
        )
        log_box.pack(side="left", fill="both", expand=True)

        scrollbar = tk.Scrollbar(log_wrap, command=log_box.yview,
                                  bg=COLORS["panel"], troughcolor=COLORS["bg"],
                                  activebackground=COLORS["text_muted"],
                                  borderwidth=0, highlightthickness=0)
        scrollbar.pack(side="right", fill="y")
        log_box.config(yscrollcommand=scrollbar.set)

        # Wrap-Lines: kleine Einrckung damit Continuations sichtbar zur
        # gleichen Message gehren, aber NICHT so viel dass bei 3 schmalen
        # Cards der Text gequetscht wird. KEIN extra vertikales Spacing
        # zwischen Entries  das erzeugte nervige Gaps.
        _WRAP_INDENT = 20

        log_box.tag_configure("time",   foreground=COLORS["text_muted"])
        log_box.tag_configure("info",   foreground=COLORS["text_dim"],
                                lmargin2=_WRAP_INDENT)
        log_box.tag_configure("warn",   foreground=COLORS["warning"],
                                lmargin2=_WRAP_INDENT)
        log_box.tag_configure("error",  foreground=COLORS["danger"],
                                lmargin2=_WRAP_INDENT)
        log_box.tag_configure("win",    foreground=COLORS["success"],
                                lmargin2=_WRAP_INDENT)
        log_box.tag_configure("monitor", foreground="#60a5fa",
                                lmargin2=_WRAP_INDENT)
        log_box.tag_configure("buy",    foreground=COLORS["success"],
                                lmargin2=_WRAP_INDENT)
        log_box.tag_configure("sell",   foreground="#22d3ee",
                                lmargin2=_WRAP_INDENT)
        log_box.tag_configure("system", foreground=COLORS["purple"],
                                font=(self.mono_font, 11, "bold"),
                                lmargin2=_WRAP_INDENT)

        # Badge-Style Tags  kein spacing1/3 damit keine Gaps entstehen
        _badge_font = (self.mono_font, 9, "bold")
        log_box.tag_configure(
            "badge_info",
            background="#0b2a3a", foreground="#22d3ee",
            font=_badge_font, lmargin1=2, lmargin2=2
        )
        log_box.tag_configure(
            "badge_ki",
            background="#1e1b3a", foreground="#a78bfa",
            font=_badge_font, lmargin1=2, lmargin2=2
        )
        log_box.tag_configure(
            "badge_error",
            background="#3a1820", foreground="#f87171",
            font=_badge_font, lmargin1=2, lmargin2=2
        )
        log_box.tag_configure(
            "badge_warn",
            background="#3a2a10", foreground="#fbbf24",
            font=_badge_font, lmargin1=2, lmargin2=2
        )
        log_box.tag_configure(
            "badge_trade",
            background="#0b3a25", foreground="#34d399",
            font=_badge_font, lmargin1=2, lmargin2=2
        )
        log_box.tag_configure(
            "badge_monitor",
            background="#1a2a4a", foreground="#60a5fa",
            font=_badge_font, lmargin1=2, lmargin2=2
        )

        log_box.config(state="disabled")

        self._write_log_to_box(log_box, auto_var, "system",
                                 f"{meta['label']} bot ready. Click Start to begin.")

        body_widgets = [hero_wrap, spark_row, param_wrap, restart_hint,
                         sep_6, actions, sep_8, log_outer]

        return {
            "led":          led,
            "status":       status_var,
            "sparkline":    sparkline,
            "pnl_title_var": pnl_title_var,
            "pnl_var":      pnl_var,
            "pnl_lbl":      pnl_lbl,
            "unr_var":      unr_var,
            "unr_lbl":      unr_lbl,
            "open_var":     open_var,
            "total_var":    total_var,
            "today_var":    today_var,
            "wr_var":       wr_var,
            "payoff_var":   payoff_var,
            "payoff_lbl":   payoff_lbl,
            "payoff_detail": payoff_detail,
            "accent":       accent,
            "name":         name,
            "start_btn":    start_btn,
            "restart_btn":  restart_btn,
            "stop_btn":     stop_btn,
            "restart_hint": restart_hint,
            "log_box":      log_box,
            "auto_var":     auto_var,
            "sim_btn":      sim_btn,
            "save_btn":     save_btn,
            "unsaved_lbl":  unsaved_lbl,
            "prompt_btn":   prompt_btn,
            "rebalance_btn": reb if name == "CROSS" else None,
            "ai_badge":     ai_badge,
            "ai_badge_var": ai_badge_var,
            "ai_mode":      "unknown",   # 'llm', 'keyword', 'unknown'
            "ai_mode_ts":   0,           # last update time
            "body_widgets": body_widgets,
            "col":          col,
            "frame":        card,
        }

    def _compute_card_pad(self, visible_index: int, visible_count: int) -> tuple[tuple, tuple]:
        """
        Return (padx, pady) for a visible card in the responsive grid.
        """
        cols = max(1, int(getattr(self, "_current_card_columns",
                                  self.MAX_CARD_COLUMNS)))
        col = visible_index % cols
        row = visible_index // cols
        last_row = (visible_count - 1) // cols if visible_count else 0
        left = 0 if col == 0 else 8
        right = 0 if col == cols - 1 else 8
        top = 0 if row == 0 else 14
        bottom = 0 if row == last_row else 14
        return (left, right), (top, bottom)

    #  Visibility

    def _apply_initial_visibility(self):
        """Apply config visibility and pill state on startup."""
        for bot in BOT_ORDER:
            self._update_pill_appearance(bot)
        # Re-apply all together for consistent layout.
        self._refresh_visibility_layout()
        # Collapse support was removed; old COLLAPSED_BOTS entries are ignored.

    def _toggle_visibility(self, bot: str):
        """Toggle visible/hidden."""
        self._visible[bot] = not self._visible.get(bot, True)
        self._update_pill_appearance(bot)
        self._refresh_visibility_layout()
        self._persist_ui_prefs()

    def _update_pill_appearance(self, bot: str):
        meta = BOT_META[bot]
        pill = self.visibility_pills[bot]
        is_visible = self._visible.get(bot, True)
        if is_visible:
            pill.configure(fg_color="transparent",
                            hover_color=COLORS["panel_hover"],
                            text_color=COLORS["purple"],
                            border_width=1, border_color=COLORS["purple"])
        else:
            pill.configure(fg_color="transparent",
                            hover_color=COLORS["panel_hover"],
                            text_color=COLORS["text_muted"],
                            border_width=1, border_color=COLORS["border"])

    def _refresh_visibility_layout(self):
        """
        Single source of truth for card layout.
        Hides/shows each card and re-grids with consistent padding.
        """
        visible = [b for b in BOT_ORDER if self._visible.get(b, True)]
        visible_count = len(visible)
        cols = max(1, int(getattr(self, "_current_card_columns",
                                  self.MAX_CARD_COLUMNS)))
        for idx, bot in enumerate(visible):
            card = self.cards[bot]
            col = idx % cols
            row = idx // cols
            padx, pady = self._compute_card_pad(idx, visible_count)
            card["row"] = row
            card["col"] = col
            card["frame"].grid(row=row, column=col, sticky="new",
                                 padx=padx, pady=pady)

        for bot in BOT_ORDER:
            if bot not in visible:
                self.cards[bot]["frame"].grid_remove()

        for i in range(self.MAX_CARD_COLUMNS):
            self.main_frame.grid_columnconfigure(
                i, weight=1 if i < cols else 0, minsize=0)
        max_rows = max(1, (visible_count + cols - 1) // cols)
        for i in range(len(BOT_ORDER)):
            self.main_frame.grid_rowconfigure(i, weight=0, minsize=0)

        # Force redraw  wichtig bei manchen tk-Versionen
        try:
            self.main_frame.update_idletasks()
            self._refresh_main_scrollregion()
        except Exception:
            pass

    #  COLLAPSE (innerhalb sichtbar) 

    def _toggle_collapse(self, name: str):
        # Collapse-Funktion entfernt. Methode bleibt als No-Op erhalten, falls
        # noch eine Referenz darauf zeigt  so kann nichts zur Laufzeit crashen.
        return

    def _persist_ui_prefs(self):
        ui_cfg = dict(self.config.get("UI", {}))
        ui_cfg.update({
            "VISIBLE_BOTS":   [b for b in BOT_ORDER if self._visible[b]],
            "COLLAPSED_BOTS": [b for b in BOT_ORDER if self._collapsed[b]],
        })
        self.config["UI"] = ui_cfg
        self.config = save_config_merge({"UI": ui_cfg})

    #  STATUSBAR 

    def _build_statusbar(self):
        bar = ctk.CTkFrame(self, fg_color=COLORS["panel"], height=42, corner_radius=0)
        bar.grid(row=2, column=1, sticky="ew")
        bar.grid_propagate(False)
        ctk.CTkFrame(self, fg_color=COLORS["border"], height=1, corner_radius=0
                      ).grid(row=2, column=1, sticky="new")

        # Linke Seite: Status-Badge + Bot-Status-Text
        left = ctk.CTkFrame(bar, fg_color="transparent")
        left.pack(side="left", fill="x", expand=True, padx=20, pady=8)

        # "Ready" Pill mit grnem Punkt
        self.status_badge = ctk.CTkFrame(left, fg_color="#0b3a25",
                                            corner_radius=12,
                                            border_width=1, border_color="#1a4d35")
        self.status_badge.pack(side="left")
        ctk.CTkLabel(self.status_badge, text="",
                      font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                      text_color="#2ecc8b"
                      ).pack(side="left", padx=(10, 4), pady=4)
        self.status_badge_text = ctk.StringVar(value="Ready")
        ctk.CTkLabel(self.status_badge, textvariable=self.status_badge_text,
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color="#2ecc8b"
                      ).pack(side="left", padx=(0, 12))

        # Aktive-Bots-Anzeige rechts daneben
        self.status_text = ctk.StringVar(value="No bots active")
        ctk.CTkLabel(left, textvariable=self.status_text,
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_muted"],
                      width=520,
                      anchor="w"
                      ).pack(side="left", fill="x", expand=True, padx=(14, 0))

        # Rechte Seite: Update-CTA + Version Info
        right = ctk.CTkFrame(bar, fg_color="transparent")
        right.pack(side="right", padx=22, pady=6)

        self.update_button = ctk.CTkButton(
            right,
            text="Update",
            width=82,
            height=26,
            corner_radius=6,
            border_width=2,
            border_color="#fbbf24",
            fg_color="#2b2110",
            hover_color="#3a2a10",
            text_color="#fbbf24",
            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
            command=self._show_pending_update_prompt,
        )

        ctk.CTkLabel(right,
                      text="v5.0  Trend  Spot  Futures  Cross  Future Trend  Local-AI  SIM/LIVE",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack(side="right")

    def _update_statusbar_state(self):
        """
        Wird vom Refresh-Loop aufgerufen  zeigt Anzahl aktiver Bots und
        passt die Status-Pill an (grn=ready, orange=Live, cyan=Sim).
        """
        try:
            running = [b for b in BOT_ORDER if self.bots[b].is_running()]
            external = self._externally_active_bots()
            active = running + [b for b in external if b not in running]
            if not active:
                self.status_badge_text.set("Ready")
                self.status_badge.configure(fg_color="#0b3a25",
                                              border_color="#1a4d35")
                for lbl in self.status_badge.winfo_children():
                    if isinstance(lbl, ctk.CTkLabel):
                        lbl.configure(text_color="#2ecc8b")
                self.status_text.set("No bots active")
            else:
                mode_cache = {}
                try:
                    mode_cache = (self.poller.get_all() or {}).get("mode_is_sim") or {}
                except Exception:
                    mode_cache = {}

                def _running_bot_is_sim(bot: str) -> bool:
                    rs = external.get(bot)
                    if rs is not None and "simulation" in rs:
                        return bool(rs.get("simulation"))
                    if bot in mode_cache:
                        return bool(mode_cache[bot])
                    return bool(self.config.get(bot, {}).get("SIMULATION", True))

                # Prfen, ob IRGENDEIN laufender Bot im LIVE-Modus ist
                any_live = any(not _running_bot_is_sim(b) for b in active)

                if any_live:
                    # Mindestens einer ist Live  orange Warnung
                    self.status_badge_text.set("Live")
                    self.status_badge.configure(fg_color="#3a2a10",
                                                  border_color="#4d3a18")
                    for lbl in self.status_badge.winfo_children():
                        if isinstance(lbl, ctk.CTkLabel):
                            lbl.configure(text_color="#fbbf24")
                else:
                    # Alle laufenden Bots sind in Simulation  entspanntes Cyan
                    self.status_badge_text.set("Sim")
                    self.status_badge.configure(fg_color="#0c2a3a",
                                                  border_color="#0e3850")
                    for lbl in self.status_badge.winfo_children():
                        if isinstance(lbl, ctk.CTkLabel):
                            lbl.configure(text_color="#22d3ee")

                bot_names = ", ".join(active)
                suffix = f" ({len(external)} external)" if external else ""
                self.status_text.set(
                    f"{len(active)} bot(s) active: {bot_names}{suffix}")
            override = self._active_update_status_override()
            if override:
                self.status_text.set(override)
            if getattr(self, "_pending_update_data", None):
                self._set_update_cta_visible(True)
        except Exception:
            pass

    def _set_update_status_override(self, message: str, *, ttl_sec: float | None = None) -> None:
        self._update_status_override = self._compact_statusbar_message(message)
        if ttl_sec is None or not self._update_status_override:
            self._update_status_override_until = 0.0
        else:
            self._update_status_override_until = time.monotonic() + max(0.0, float(ttl_sec))
        if self._update_status_override:
            self.status_text.set(self._update_status_override)

    @staticmethod
    def _compact_statusbar_message(message: str, limit: int = 145) -> str:
        text = " ".join(str(message or "").split())
        if len(text) <= limit:
            return text
        return text[: max(0, limit - 3)].rstrip() + "..."

    def _clear_update_status_override(self) -> None:
        self._update_status_override = ""
        self._update_status_override_until = 0.0

    def _active_update_status_override(self) -> str:
        override = getattr(self, "_update_status_override", "")
        if not override:
            return ""
        until = float(getattr(self, "_update_status_override_until", 0.0) or 0.0)
        if until and time.monotonic() > until:
            self._clear_update_status_override()
            return ""
        return override

    def _set_update_cta_visible(self, visible: bool) -> None:
        try:
            if not hasattr(self, "update_button"):
                return
            packed = bool(self.update_button.winfo_manager())
            if visible and not packed:
                self.update_button.pack(side="left", padx=(0, 12))
            elif not visible and packed:
                self.update_button.pack_forget()
        except Exception:
            pass

    #  PARAMETER ACTIONS 

    def _on_param_change(self, bot_name, key, value):
        self.config[bot_name][key] = value
        self._dirty_param_keys.setdefault(bot_name, set()).add(key)
        self._mark_dirty(bot_name, True)

    def _mark_dirty(self, bot_name: str, dirty: bool):
        card = self.cards.get(bot_name)
        if not card:
            return
        if dirty:
            card["unsaved_lbl"].configure(text=" Unsaved")
        else:
            card["unsaved_lbl"].configure(text="")

            self._dirty_param_keys.setdefault(bot_name, set()).clear()

    def _save_params(self, bot_name: str):
        card = self.cards[bot_name]
        dirty = set(self._dirty_param_keys.get(bot_name) or set())
        if not dirty:
            self._log_to_card(card, "system", "No parameter changes to save")
            return
        rows = self.param_rows.get(bot_name, {})
        updates = {k: rows[k].value for k in dirty if k in rows}
        try:
            self.config = save_config_merge({bot_name: updates})
        except Exception as exc:
            self._log_to_card(card, "error", f"Config save failed: {exc}")
            return
        self._mark_dirty(bot_name, False)
        self._log_to_card(card, "system", "Parameters saved to bot_config.json")
        accent = card["accent"]
        card["save_btn"].configure(text=" Saved", fg_color=COLORS["success"],
                                   text_color=COLORS["bg"],
                                   border_width=2,
                                   border_color=COLORS["success"])
        self.after(1200, lambda: card["save_btn"].configure(
            text=" Save", fg_color="transparent", text_color=accent,
            border_width=2, border_color=accent
        ) if card["save_btn"].winfo_exists() else None)

    def _reset_params(self, bot_name):
        card = self.cards[bot_name]
        try:
            from tkinter import messagebox
            ok = messagebox.askyesno(
                "Reset parameters",
                f"Reset visible {bot_name} parameters to defaults?\n\n"
                "SIM/LIVE mode and hidden technical config are not changed.",
            )
            if not ok:
                return
        except Exception:
            return
        defaults = effective_default_config().get(bot_name, {})
        rows = self.param_rows.get(bot_name, {})
        updates = {
            k: defaults[k]
            for k in rows
            if k != "SIMULATION" and k in defaults
        }
        if updates:
            try:
                self.config = save_config_merge({bot_name: updates})
            except Exception as exc:
                self._log_to_card(card, "error", f"Config reset failed: {exc}")
                self._mark_dirty(bot_name, True)
                return
        for key in updates:
            self.config[bot_name][key] = updates[key]
        for key, row in rows.items():
            if key not in updates:
                continue
            row.set_value(updates[key])
        self._mark_dirty(bot_name, False)
        self._log_to_card(card, "system", "Parameters reset to defaults")

    def _toggle_simulation(self, bot_name: str):
        card = self.cards[bot_name]
        is_sim = self.config[bot_name].get("SIMULATION", True)
        new_sim = not is_sim
        is_futures = BOT_META[bot_name]["is_futures"]

        if not new_sim:
            dlg = ctk.CTkToplevel(self)
            dlg.title("Switch to LIVE Trading")
            dlg.geometry("480x280" if is_futures else "460x240")
            dlg.configure(fg_color=COLORS["panel"])
            dlg.grab_set()
            dlg.transient(self)
            force_dark_titlebar(dlg)

            ctk.CTkLabel(dlg, text="!",
                          font=ctk.CTkFont(self.mono_font, 36, "bold"),
                          text_color=COLORS["danger"]
                          ).pack(pady=(20, 4))
            ctk.CTkLabel(dlg, text="Switch to LIVE Trading?",
                          font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                          text_color=COLORS["text"]
                          ).pack()

            if is_futures:
                lev = int(self.config[bot_name].get("LEVERAGE", 3))
                warn_text = (
                    f"Real money on FUTURES with {lev} leverage.\n"
                    f"Liquidation possible. At {lev} a ~{int(100/lev)}% move\n"
                    f"against you wipes the margin.\n\n"
                    f"Liquidation safety buffer will close BEFORE liquidation,\n"
                    f"but does not eliminate risk."
                )
            else:
                warn_text = (
                    f"Real money will be used for {bot_name}.\n"
                    f"Make sure your API key has Trade permission\n"
                    f"and you understand the risks."
                )

            ctk.CTkLabel(dlg, text=warn_text,
                          font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                          text_color=COLORS["text_dim"], justify="center"
                          ).pack(pady=(8, 16))

            row = ctk.CTkFrame(dlg, fg_color="transparent")
            row.pack()

            def _confirm():
                dlg.destroy()
                self._apply_simulation(bot_name, False, card)

            ctk.CTkButton(row, text="Yes, go LIVE",
                           fg_color=COLORS["danger"], hover_color="#dc2626",
                           text_color="#ffffff", width=140, height=34, corner_radius=8,
                           font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                           command=_confirm
                           ).pack(side="left", padx=6)
            ctk.CTkButton(row, text="Cancel",
                           fg_color="transparent", hover_color=COLORS["panel_hover"],
                           text_color=COLORS["text_dim"], width=110, height=34, corner_radius=8,
                           font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                           border_width=1, border_color=COLORS["border"],
                           command=dlg.destroy
                           ).pack(side="left", padx=6)
        else:
            self._apply_simulation(bot_name, True, card)

    def _apply_simulation(self, bot_name, sim, card):
        from launcher.core.bot_controller import apply_simulation
        apply_simulation(self, bot_name, sim, card)

    def _open_prompt_editor(self, bot_name: str):
        def _on_save(b):
            card = self.cards[b]
            self._log_to_card(card, "system",
                                "Prompt saved  restart bot to load new prompt")

        meta = BOT_META[bot_name]
        # Mechanical bots (Trend) have no LLM/prompt  nothing to edit.
        if not meta.get("uses_llm", True) or "prompt" not in meta:
            return
        PromptEditor(self, bot_name, meta["accent"], self.mono_font,
                      on_save_callback=_on_save)

    #  EMERGENCY CLOSE (Futures only) 

    def _emergency_close_futures(self):
        """Delegated to launcher.ui.dialogs.shutdown."""
        from launcher.ui.dialogs.shutdown import emergency_close_futures
        emergency_close_futures(self)

    def _force_cross_rebalance(self):
        """Manual-rebalance button (CROSS card): write a FORCE_REBALANCE token
        the running CROSS bot polls. Cross-process via bot_params; the bot acts
        within ~60s without a restart. No-op if CROSS isn't running (the bot
        seeds the token on start, so a press while stopped won't fire later)."""
        card = self.cards.get("CROSS")
        try:
            running = False
            try:
                running = self.bots["CROSS"].is_running()
            except Exception:
                pass
            if not running:
                if card:
                    self._log_to_card(card, "warn",
                        "Rebalance ignored  CROSS is not running.")
                return
            import time as _t
            from core.database import set_param
            set_param("CROSS", "FORCE_REBALANCE", _t.time(),
                      reason="manual UI rebalance button")
            if card:
                self._log_to_card(card, "system",
                    " Rebalance requested  CROSS rebuilds its book within ~60s.")
        except Exception as e:
            if card:
                self._log_to_card(card, "warn", f"Rebalance request failed: {e}")

    def _async_emergency_close(self, card, bot_was_running, update):
        from launcher.core.bot_controller import async_emergency_close
        async_emergency_close(self, card, bot_was_running, update)

    def _start_bot(self, name):
        from launcher.core.bot_controller import start_bot
        start_bot(self, name)

    def _stop_bot(self, name):
        from launcher.core.bot_controller import stop_bot
        stop_bot(self, name)

    def _get_open_spot_positions(self, bot_name):
        from launcher.core.bot_controller import open_positions_for_stop
        return open_positions_for_stop(bot_name)

    def _refresh_spot_positions_with_live_prices(self, bot_name, positions):
        from launcher.core.positions import refresh_spot_positions_with_live_prices
        return refresh_spot_positions_with_live_prices(positions)

    def _show_spot_stop_dialog(self, name, positions):
        from launcher.ui.dialogs.shutdown import show_spot_stop_dialog
        show_spot_stop_dialog(self, name, positions)

    def _async_close_and_stop_spot(self, name, update):
        from launcher.core.bot_controller import async_close_and_stop_spot
        async_close_and_stop_spot(self, name, update)

    def _async_simple_stop(self, name, card, update):
        from launcher.core.bot_controller import async_simple_stop
        async_simple_stop(self, name, card, update)

    def _get_open_futures_positions(self, bot_name: str = None):
        if bot_name is None:
            from launcher.core.positions import get_open_futures_positions
            return get_open_futures_positions(None)
        from launcher.core.bot_controller import open_positions_for_stop
        return open_positions_for_stop(bot_name)

    def _refresh_positions_with_live_prices(self, positions):
        from launcher.core.positions import refresh_positions_with_live_prices
        return refresh_positions_with_live_prices(positions)

    def _show_futures_stop_dialog(self, name, positions):
        from launcher.ui.dialogs.shutdown import show_futures_stop_dialog
        show_futures_stop_dialog(self, name, positions)

    def _emergency_close_futures_and_stop(self, name):
        from launcher.ui.dialogs.shutdown import emergency_close_futures_and_stop
        emergency_close_futures_and_stop(self, name)

    def _async_close_and_stop(self, name, card, update):
        from launcher.core.bot_controller import async_close_and_stop_futures
        async_close_and_stop_futures(self, name, card, update)

    def _show_busy_dialog(self, title, intro, worker, **worker_kwargs):
        from launcher.ui.dialogs.shutdown import show_busy_dialog
        show_busy_dialog(self, title, intro, worker, **worker_kwargs)

    def _restart_bot(self, name):
        from launcher.core.bot_controller import restart_bot
        restart_bot(self, name)

    def _config_changed_since_start(self, name):
        from launcher.core.bot_controller import config_changed_since_start
        return config_changed_since_start(self, name)

    def _config_restart_required_reason(self, name):
        from launcher.core.bot_controller import config_restart_required_reason
        return config_restart_required_reason(self, name)

    def _open_backtest_dialog(self):
        from launcher.ui.dialogs.tools import open_backtest_dialog
        open_backtest_dialog(self)

    def _open_optimizer_dialog(self):
        from launcher.ui.dialogs.tools import open_optimizer_dialog
        open_optimizer_dialog(self)

    def _open_selftest_dialog(self):
        from launcher.ui.dialogs.tools import open_selftest_dialog
        open_selftest_dialog(self)

    def _open_heatmap_dialog(self):
        from launcher.ui.dialogs.tools import open_heatmap_dialog
        open_heatmap_dialog(self)

    def _run_tool_dialog(self, title, tool_name, description):
        from launcher.ui.dialogs.tools import run_tool_dialog
        run_tool_dialog(self, title, tool_name, description)

    def _ensure_prompt_files_exist(self):
        """
        Self-Healing: legt prompts/*.txt an wenn fehlend, mittels prompts_defaults.py.
        Wird einmal beim Launcher-Start aufgerufen. Pfade sind PROJECT_ROOT-relativ.
        """
        try:
            from launcher.config.settings import PROJECT_ROOT
            here = PROJECT_ROOT
            prompts_dir = os.path.join(here, "prompts")

            # Sind ALLE bentigten Dateien da?
            # Trend has no prompt (mechanical, no LLM)  not listed here.
            needed = ["spot.txt", "spot_default.txt",
                       "futures.txt", "futures_default.txt"]
            missing = [n for n in needed
                         if not os.path.exists(os.path.join(prompts_dir, n))]

            if not missing:
                return  # alles gut

            # Try import: project_root may be on sys.path so `news.` works
            try:
                from news import prompts_defaults
                written = prompts_defaults.write_all_defaults(prompts_dir)
                still_missing = [
                    n for n in needed
                    if not os.path.exists(os.path.join(prompts_dir, n))
                ]
                if still_missing:
                    print(
                        f"[Launcher] WARNING: prompt self-heal incomplete. "
                        f"Missing: {still_missing}"
                    )
                elif written:
                    print(
                        f"[Launcher] Self-heal: restored prompt files in "
                        f"{prompts_dir}: {written}"
                    )
            except ImportError:
                print(f"[Launcher] WARNING: news.prompts_defaults not found  "
                       f"prompt files cannot be auto-created. Missing: {missing}")
            except Exception as e:
                print(f"[Launcher] Self-heal failed: {e}")
        except Exception as e:
            print(f"[Launcher] _ensure_prompt_files_exist crashed: {e}")

    @staticmethod
    def _find_dashboard_port(preferred: int = 8501) -> int:
        """Return a local port for Streamlit without assuming 8501 is free."""
        for port in (preferred, 0):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    sock.bind(("127.0.0.1", port))
                except OSError:
                    continue
                return int(sock.getsockname()[1])
        raise OSError("no local dashboard port available")

    def _dashboard_url(self) -> str:
        return f"http://127.0.0.1:{int(self._dashboard_port or 8501)}"

    @staticmethod
    def _dashboard_health_ok(port: int) -> bool:
        try:
            response = _req.get(f"http://127.0.0.1:{int(port)}/_stcore/health", timeout=0.35)
            return response.status_code < 500
        except Exception:
            return False

    @staticmethod
    def _running_dashboard_ports() -> list[int]:
        try:
            import psutil  # type: ignore
        except Exception:
            return []
        root_text = str(PROJECT_ROOT).lower()
        ports: list[int] = []
        for proc in psutil.process_iter(["pid", "cmdline", "cwd"]):
            try:
                parts = [str(p) for p in (proc.info.get("cmdline") or [])]
                cmd = " ".join(parts)
                low = cmd.lower()
                cwd = str(proc.info.get("cwd") or "").lower()
            except Exception:
                continue
            if "streamlit" not in low or "tools/dashboard.py" not in low:
                continue
            if root_text not in low and cwd != root_text:
                continue
            port = 8501
            for idx, part in enumerate(parts[:-1]):
                if part == "--server.port":
                    try:
                        port = int(parts[idx + 1])
                    except Exception:
                        port = 8501
                    break
            ports.append(port)
        return ports

    def _open_existing_dashboard_if_healthy(self) -> bool:
        """Reuse an already-running dashboard after launcher restarts."""
        ports = []
        if self._dashboard_port:
            ports.append(int(self._dashboard_port))
        ports.extend(self._running_dashboard_ports())
        for port in dict.fromkeys(ports):
            if self._dashboard_health_ok(port):
                self._dashboard_port = port
                webbrowser.open(self._dashboard_url())
                return True
        return False

    def _open_dashboard_when_ready(self, url: str, attempt: int = 0) -> None:
        """Open only the dashboard process this launcher started."""
        if self.streamlit is None or self.streamlit.poll() is not None:
            stderr = sys.stderr
            if stderr is not None:
                try:
                    stderr.write("[Dashboard] process is not running\n")
                except Exception:
                    pass
            return
        try:
            response = _req.get(f"{url}/_stcore/health", timeout=0.6)
            if response.status_code < 500:
                webbrowser.open(url)
                return
        except Exception:
            pass
        if attempt < 30:
            self.after(500, lambda: self._open_dashboard_when_ready(url, attempt + 1))
            return
        stderr = sys.stderr
        if stderr is not None:
            try:
                stderr.write(f"[Dashboard] not ready at {url}\n")
            except Exception:
                pass

    def _open_dashboard(self):
        """Open the streamlit dashboard.

        ``cwd=PROJECT_ROOT`` because "tools/dashboard.py" is a path relative
        to the project root, not to this module's directory.
        """
        if self.streamlit is None or self.streamlit.poll() is not None:
            if self._open_existing_dashboard_if_healthy():
                self.streamlit = None
                return
            try:
                self._dashboard_port = self._find_dashboard_port(8501)
            except Exception as e:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(f"[Dashboard] failed to allocate port: {e}\n")
                    except Exception:
                        pass
                return
            kw = subprocess_no_window_kwargs()
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"] = "1"

            try:
                self.streamlit = subprocess.Popen(
                    [_get_python_exe(), "-m", "streamlit", "run",
                     "tools/dashboard.py",
                     "--server.port", str(self._dashboard_port),
                     "--server.headless", "true"],
                    env=env, cwd=PROJECT_ROOT, **kw
                )
            except Exception as e:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(f"[Dashboard] failed to start: {e}\n")
                    except Exception:
                        pass
                return

            self.after(500, lambda: self._open_dashboard_when_ready(self._dashboard_url()))
        else:
            self._open_dashboard_when_ready(self._dashboard_url())

    def _check_for_updates_async(self) -> None:
        """Check private Git updates without blocking the launcher startup."""
        if getattr(self, "_update_check_started", False):
            return
        self._update_check_started = True

        def _worker() -> None:
            try:
                r = subprocess.run(
                    [_get_python_exe(), "-m", "tools.update_check", "--json"],
                    cwd=PROJECT_ROOT,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    **subprocess_no_window_kwargs(),
                )
                raw = (r.stdout or "").strip()
                data = json.loads(raw) if raw else {}
                if r.returncode != 0 and not data:
                    data = {
                        "ok": False,
                        "reason": "check_failed",
                        "message": (r.stderr or r.stdout or "Update-Check fehlgeschlagen").strip(),
                    }
            except Exception as exc:
                data = {"ok": False, "reason": "check_failed", "message": str(exc)}
            self.after(0, lambda d=data: self._apply_update_check_result(d))

        threading.Thread(target=_worker, daemon=True).start()

    def _apply_update_check_result(self, data: dict) -> None:
        try:
            last_update = data.get("last_update") if isinstance(data.get("last_update"), dict) else {}
            if not data.get("ok"):
                reason = str(data.get("reason") or "")
                if reason == "git_missing":
                    msg = "Update-System nicht bereit: Git for Windows fehlt."
                elif reason == "repo_missing":
                    msg = "Update-System nicht konfiguriert."
                elif reason == "remote_unreachable":
                    msg = "Update-Check nicht erreichbar. Pruefe SSH-Key/GitHub-Zugriff."
                elif reason == "check_failed":
                    detail = str(data.get("message") or "").strip()
                    msg = "Update-Check fehlgeschlagen" + (f": {detail[:180]}" if detail else ".")
                else:
                    msg = ""
                if msg:
                    self._set_update_status_override(msg, ttl_sec=30.0)
                    if reason not in {"repo_missing", "git_missing", "remote_unreachable"} and not getattr(self, "_update_notice_shown", False):
                        self._update_notice_shown = True
                        try:
                            from tkinter import messagebox
                            messagebox.showwarning("Obsidian Update", msg)
                        except Exception:
                            pass
                return
            if not data.get("update_available"):
                self._pending_update_data = None
                self._clear_update_status_override()
                self._set_update_cta_visible(False)
                if last_update.get("status") == "failed":
                    self._set_update_status_override(
                        f"Letztes Update fehlgeschlagen: {last_update.get('message', '')}",
                        ttl_sec=45.0,
                    )
                return
            remote = str(data.get("remote") or "")[:8]
            failed_note = ""
            same_failed_remote = (
                last_update.get("status") == "failed"
                and str(last_update.get("remote") or "")
                and str(last_update.get("remote") or "") == str(data.get("remote") or "")
            )
            if last_update.get("status") == "failed":
                failed_note = f"\n\nLetzter Update-Versuch: {last_update.get('message', '')}"
            if same_failed_remote:
                self._pending_update_data = data
                self._set_update_cta_visible(True)
                detail = str(last_update.get("message") or "").strip()
                msg = (
                    f"Update {remote} ist verfuegbar, letzter Versuch fuer diesen Stand ist fehlgeschlagen."
                    + (f" Grund: {detail[:180]}" if detail else "")
                    + " Nach Behebung erneut per Update-Button starten."
                )
                self._set_update_status_override(msg, ttl_sec=60.0)
                return
            if data.get("bootstrap_required"):
                msg = ("Einmalige Update-Einrichtung ist verfuegbar. "
                       "Der Launcher wird geschlossen, der private Git-Stand "
                       "initialisiert und danach automatisch neu gestartet."
                       + failed_note)
            else:
                msg = (f"Update verfuegbar ({remote}). Der Launcher wird geschlossen, "
                       "das Update installiert und danach automatisch neu gestartet."
                       + failed_note)
            self._pending_update_data = data
            self._set_update_status_override(msg)
            self._set_update_cta_visible(True)
            if getattr(self, "_update_notice_shown", False):
                return
            self._update_notice_shown = True
            self._show_pending_update_prompt()
        except Exception:
            pass

    def _show_pending_update_prompt(self) -> None:
        try:
            data = getattr(self, "_pending_update_data", None)
            if not data:
                return
            remote = str(data.get("remote") or "")[:8]
            last_update = data.get("last_update") if isinstance(data.get("last_update"), dict) else {}
            failed_note = ""
            if last_update.get("status") == "failed":
                failed_note = f"\n\nLetzter Update-Versuch: {last_update.get('message', '')}"
            if data.get("bootstrap_required"):
                msg = ("Einmalige Update-Einrichtung ist verfuegbar. "
                       "Der Launcher wird geschlossen, der private Git-Stand "
                       "initialisiert und danach automatisch neu gestartet."
                       + failed_note)
            else:
                msg = (f"Update verfuegbar ({remote}). Der Launcher wird geschlossen, "
                       "das Update installiert und danach automatisch neu gestartet."
                       + failed_note)
            try:
                from tkinter import messagebox
                if messagebox.askyesno("Obsidian Update", msg + "\n\nJetzt installieren?"):
                    self._start_external_update_and_exit()
            except Exception:
                pass
        except Exception:
            pass

    def _start_external_update_and_exit(self) -> None:
        """Start the out-of-process updater and close the GUI.

        The updater waits for this process to disappear before touching files,
        so the user does not have to run update.bat manually.
        """
        try:
            running = [name for name, bot in self.bots.items() if bot.is_running()]
            external = self._externally_active_bots()
            blockers = running + [b for b in external if b not in running]
            if blockers:
                from tkinter import messagebox
                messagebox.showwarning(
                    "Obsidian Update",
                    "Update nicht gestartet. Stoppe zuerst alle laufenden Bots: "
                    + ", ".join(blockers)
                    + "\n\nDanach kannst du das Update ueber den gelben Update-Button starten.",
                )
                self._set_update_cta_visible(True)
                return
            runner = os.path.join(PROJECT_ROOT, "tools", "update_launcher.py")
            if not os.path.exists(runner):
                from tkinter import messagebox
                messagebox.showerror("Obsidian Update", "Update-Runner fehlt: tools/update_launcher.py")
                return
            pyw = _get_pythonw_exe()
            exe = pyw if pyw and os.path.exists(str(pyw)) else _get_python_exe()
            cmd = [exe, runner, "--parent-pid", str(os.getpid()), "--restart"]
            cmd.append("--progress-ui")
            pending = getattr(self, "_pending_update_data", None)
            if isinstance(pending, dict):
                remote = str(pending.get("remote") or "").strip()
                branch = str(pending.get("branch") or "").strip()
                if remote:
                    cmd.extend(["--remote", remote])
                if branch:
                    cmd.extend(["--branch", branch])
            subprocess.Popen(
                cmd,
                cwd=PROJECT_ROOT,
                **subprocess_no_window_kwargs(),
            )
            self._set_update_cta_visible(False)
            self._clear_update_status_override()
            self.status_text.set("Update startet, Launcher wird geschlossen ...")
            self.after(250, self._shutdown_clean)
        except Exception as exc:
            try:
                from tkinter import messagebox
                messagebox.showerror("Obsidian Update", f"Update konnte nicht gestartet werden:\n{exc}")
            except Exception:
                pass

    def _clear_card_log(self, name):
        card = self.cards[name]
        log_box = card["log_box"]
        log_box.config(state="normal")
        log_box.delete("1.0", "end")
        log_box.config(state="disabled")

    #  LLM MODEL SELECTOR 

    def _switch_ollama_model_async(self, new_model: str) -> None:
        """Actively switch the active model in Ollama's VRAM.

        Runs in a background thread (called from _apply via Thread.start).
        Three steps:

        1. List currently-loaded models via ``/api/ps``.
        2. For each model that's NOT the new one, send a generate request
           with ``keep_alive=0``  Ollama interprets that as "use it now
           then immediately unload". Empty prompt + num_predict=0 makes
           this a no-op that just frees VRAM.
        3. Warm up the new model with a 1-token generate + a long
           keep_alive (24h) so it stays resident.

        On success the sidebar's /api/ps probe will report ONLY the new
        model as loaded  which makes get_llm_info() return it
        unambiguously.

        We log progress to TREND's card by default (or whichever's
        running) so the user can see what's happening.
        """
        import requests as _rq
        import json as _json
        from launcher.config.settings import OLLAMA_URL

        # Pick a card to log to (prefer a running bot, fallback to first)
        running = [n for n in BOT_ORDER if self.bots[n].is_running()]
        log_target = running[0] if running else BOT_ORDER[0]

        def _log(severity: str, msg: str) -> None:
            try:
                self.after(0, lambda: self._log_to_card(
                    self.cards[log_target], severity, msg))
            except Exception:
                pass

        try:
            #  Step 1: probe what's loaded 
            try:
                r = _rq.get(f"{OLLAMA_URL}/api/ps", timeout=2.0)
                if r.status_code == 200:
                    loaded = [m["name"] for m in r.json().get("models", [])]
                else:
                    loaded = []
            except Exception as e:
                _log("warn", f"Ollama probe failed ({type(e).__name__})  "
                              f"new model will load on first bot request")
                return

            #  Step 2: unload everything that isn't the new one 
            unloaded = []
            for m in loaded:
                if m == new_model:
                    continue
                try:
                    _rq.post(
                        f"{OLLAMA_URL}/api/generate",
                        json={
                            "model": m,
                            "prompt": "",
                            "keep_alive": 0,   # unload immediately
                            "options": {"num_predict": 0},
                        },
                        timeout=10,
                    )
                    unloaded.append(m)
                except Exception:
                    pass  # silent  the load step below will succeed anyway

            #  Step 3: warm up the new model 
            # 24h keep_alive so the model stays resident across bot scan
            # cycles. The bot's own generate() calls will reset this
            # timer on each use.
            try:
                r = _rq.post(
                    f"{OLLAMA_URL}/api/generate",
                    json={
                        "model": new_model,
                        "prompt": "hi",
                        "keep_alive": "24h",
                        "options": {"num_predict": 1},
                    },
                    timeout=120,   # cold-load of 14B can take 30-60s
                )
                if r.status_code == 200:
                    # ONE concise summary line, not three.
                    suffix = (f" (replaced {', '.join(unloaded)})"
                                if unloaded else "")
                    _log("system",
                          f" Ollama: {new_model} active{suffix}")
                    # Refresh sidebar  the next poller tick will confirm
                    try:
                        with self.poller.lock:
                            self.poller.cache["llm"] = {
                                "online": True, "model": new_model,
                                "loaded": True,
                            }
                    except Exception:
                        pass
                elif r.status_code == 404:
                    _log("warn", f" Model {new_model} not installed. "
                                  f"Run: ollama pull {new_model}")
                else:
                    _log("warn", f" Ollama load: HTTP {r.status_code}")
            except _rq.Timeout:
                _log("warn", f"Ollama: {new_model} load timed out (>120s)")
            except Exception as e:
                _log("warn", f"Ollama: load failed: {type(e).__name__}: {e}")
        except Exception as e:
            _log("warn", f"Model switch crashed: {type(e).__name__}: {e}")

    def _show_model_selector(self):
        dlg = ctk.CTkToplevel(self)
        dlg.title("Select LLM Model")
        dlg.geometry("480x600")
        dlg.minsize(480, 560)
        dlg.configure(fg_color=COLORS["panel"])
        dlg.grab_set()
        dlg.transient(self)
        force_dark_titlebar(dlg)

        #  Apply / Cancel buttons  packed FIRST with side="bottom" so they
        # are always visible even when the radio-button list is dynamically
        # inserted above them by the background model-fetch thread.
        # (Old: packed last  130px ScrollableFrame pushed btn_row off-screen)
        btn_row = ctk.CTkFrame(dlg, fg_color="transparent")
        btn_row.pack(side="bottom", fill="x", padx=24, pady=16)

        def _apply():
            new_model = manual_entry.get().strip() or selected_var.get()
            if not new_model:
                return
            save_failed_reason = None
            cfg_written = None
            try:
                self.config = save_config_merge({"LLM_MODEL": new_model})
                cfg_written = self.config.get("LLM_MODEL")
            except Exception as e:
                save_failed_reason = f"{type(e).__name__}: {e}"

            if save_failed_reason or cfg_written != new_model:
                reason = (save_failed_reason
                            or f"verify failed (disk: {cfg_written!r})")
                self._log_to_card(self.cards[BOT_ORDER[0]], "warn",
                                    f"Failed to save LLM_MODEL: {reason}")
                # Show in the dialog too so the user doesn't miss it
                try:
                    self.sb_llm_model.configure(text=f"  (save failed)")
                except Exception:
                    pass
                return

            #  In-memory sync 
            # Keep the in-memory ``self.config`` in step with what we just
            # wrote to disk. Otherwise the next ``save_config(self.config)``
            # (parameter save, bot-visibility change  all write the FULL
            # dict) would overwrite the file with the stale LLM_MODEL.
            self.config["LLM_MODEL"] = new_model

            # Update sidebar immediately so the user sees feedback
            self.sb_llm_model.configure(text=f"  {new_model}")

            # Invalidate the poller's LLM cache so the next tick re-reads
            # bot_config.json and reflects the new model in the sidebar
            # without waiting for the 10s ping cycle.
            try:
                with self.poller.lock:
                    self.poller.cache["llm"] = {
                        "online": True, "model": new_model, "loaded": False,
                    }
            except Exception:
                pass

            dlg.destroy()

            # Writing bot_config.json alone doesn't make Ollama use the new
            # model  Ollama keeps the last-requested model in VRAM until its
            # keep_alive timer fires. So switch ACTIVELY in a background thread:
            #   1) unload the previous model (keep_alive=0 on a no-op generate)
            #   2) warm up the new model     (1-token generate, long keep_alive)
            # The bot subprocesses then see the new model already in VRAM on
            # their next generate()  no scan-cycle latency.
            threading.Thread(
                target=self._switch_ollama_model_async,
                args=(new_model,),
                name=f"ollama-switch-{new_model}",
                daemon=True,
            ).start()

            # Single concise log line per card  the async switch worker
            # below adds 2-3 more lines to ONE card (the primary one) so
            # the user can follow progress. Logging to every card causes
            # visual noise with no extra info.
            primary_card = next(
                (n for n in BOT_ORDER if self.bots[n].is_running()),
                BOT_ORDER[0],
            )
            self._log_to_card(self.cards[primary_card], "system",
                                f"LLM_MODEL  {new_model} "
                                f"(loading into VRAM)")

        ctk.CTkButton(btn_row, text="Apply", height=36, corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       fg_color=COLORS["purple"], hover_color=COLORS["purple_dim"],
                       text_color="#ffffff", width=120, command=_apply
                       ).pack(side="right")
        ctk.CTkButton(btn_row, text="Cancel", height=36, corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_muted"],
                       border_width=1, border_color=COLORS["border"], width=90,
                       command=dlg.destroy
                       ).pack(side="right", padx=(0, 8))

        #  Content (top-anchored, fills space above the fixed buttons) 
        ctk.CTkLabel(dlg, text="Select LLM Model",
                      font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                      text_color=COLORS["text"]
                      ).pack(padx=24, pady=(20, 4), anchor="w")
        ctk.CTkLabel(dlg,
                      text="Any Ollama model that generates text works.\n"
                            "Reasoning models (deepseek-r1, qwen) give best results.",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_muted"], justify="left"
                      ).pack(padx=24, anchor="w")

        # Zentraler Default statt hartkodiertem Modellnamen
        try:
            from core.constants import LLM_MODEL_DEFAULT as _LLM_DEFAULT
        except Exception:
            _LLM_DEFAULT = "qwen2.5:14b"
        current = _LLM_DEFAULT
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, encoding="utf-8-sig") as f:
                    current = json.load(f).get("LLM_MODEL", current)
        except Exception:
            pass

        # Show the currently-configured model prominently so the user can
        # tell at a glance whether their last "Apply" stuck.
        current_box = ctk.CTkFrame(dlg, fg_color=COLORS["bg"], corner_radius=6,
                                     border_width=1, border_color=COLORS["border"])
        current_box.pack(fill="x", padx=24, pady=(12, 4))
        ctk.CTkLabel(current_box, text="CURRENT (from bot_config.json):",
                      font=ctk.CTkFont(FONT_BODY, 9, "bold"),
                      text_color=COLORS["text_subtle"]
                      ).pack(side="left", padx=(10, 6), pady=8)
        ctk.CTkLabel(current_box, text=current,
                      font=ctk.CTkFont(self.mono_font, 12, "bold"),
                      text_color=COLORS["balanced"]
                      ).pack(side="left", pady=8)

        installed = []
        status_lbl = ctk.CTkLabel(dlg, text="Loading installed models...",
                                    font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                                    text_color=COLORS["text_muted"])
        status_lbl.pack(padx=24, pady=(12, 4), anchor="w")

        # Fetch the model list in a background thread, then hand the result
        # back to the UI via self.after()  a synchronous `_req.get` here would
        # freeze the Tkinter main thread for ~2s whenever Ollama is unreachable.
        scroll_container = {"widget": None}

        def _build_radio_list():
            """Build the radio button list. Called once installed is known."""
            if scroll_container["widget"] is not None:
                return  # already built
            if installed:
                scroll = ctk.CTkScrollableFrame(dlg, fg_color=COLORS["bg"], height=130,
                                                  scrollbar_button_color=COLORS["border"])
                # Pack BEFORE the "Or enter model name manually" label so the
                # visual order matches the original (radio list  divider  entry)
                scroll.pack(fill="x", padx=24, pady=(4, 0),
                              before=manual_label_ref.get("widget") if manual_label_ref.get("widget") else None)
                scroll_container["widget"] = scroll
                for model_name in installed:
                    is_current = model_name == current
                    ctk.CTkRadioButton(
                        scroll, text=model_name,
                        variable=selected_var, value=model_name,
                        font=ctk.CTkFont(self.mono_font, 11, "bold"),
                        text_color=COLORS["balanced"] if is_current else COLORS["text_dim"],
                        fg_color=COLORS["purple"], hover_color=COLORS["purple_dim"],
                        command=lambda m=model_name: manual_entry.delete(0, "end") or manual_entry.insert(0, m)
                    ).pack(anchor="w", padx=8, pady=3)

        def _fetch_models_bg():
            """Background worker: fetches model list, schedules UI update."""
            local_installed = []
            try:
                r = _req.get(f"{OLLAMA_URL}/api/tags", timeout=2)
                if r.status_code == 200:
                    local_installed = [m["name"] for m in r.json().get("models", [])]
            except Exception:
                pass
            # Apply on UI thread
            def _apply_models_to_ui():
                """Check widget existence before touching it  the user can
                close the model-selector dialog before Ollama responds, and
                without winfo_exists() the configure() would raise TclError.
                """
                import tkinter as _tk

                try:
                    if not status_lbl.winfo_exists():
                        return  # dialog was closed  bail silently
                except (_tk.TclError, AttributeError):
                    return

                try:
                    installed.extend(local_installed)
                    if installed:
                        status_lbl.configure(text=f"{len(installed)} model(s) installed")
                        _build_radio_list()
                    else:
                        status_lbl.configure(text="Ollama offline  enter model name manually")
                except _tk.TclError:
                    return  # widget destroyed mid-update
                except Exception:
                    return
            try:
                self.after(0, _apply_models_to_ui)
            except Exception:
                pass

        import threading as _threading
        _threading.Thread(target=_fetch_models_bg, daemon=True,
                            name="ModelListFetch").start()

        selected_var = ctk.StringVar(value=current)

        manual_label_ref = {}
        # side="bottom": Feld + Label direkt ueber den (ebenfalls bottom
        # gepackten) Apply/Cancel-Buttons verankern, damit die dynamisch
        # eingefgte Modell-Liste sie nie aus dem Fenster drckt. Wegen
        # bottom-Stacking zuerst das Feld (landet unten), dann das Label.
        manual_entry = ctk.CTkEntry(dlg, height=34, corner_radius=6,
                                      font=ctk.CTkFont(self.mono_font, 12),
                                      fg_color=COLORS["bg"], border_color=COLORS["border"],
                                      text_color=COLORS["text"],
                                      placeholder_text="e.g. llama3.2, qwen2.5:7b, mistral")
        manual_entry.pack(side="bottom", fill="x", padx=24, pady=(0, 4))
        manual_entry.insert(0, current)

        manual_label = ctk.CTkLabel(dlg, text="Or enter model name manually:",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_muted"])
        manual_label.pack(side="bottom", padx=24, pady=(12, 4), anchor="w")
        manual_label_ref["widget"] = manual_label

    #  ERROR LOG VIEWER 

    def _show_error_log(self):
        dlg = ctk.CTkToplevel(self)
        dlg.title("Error Log")
        dlg.geometry("780x520")
        dlg.configure(fg_color=COLORS["panel"])
        dlg.transient(self)
        force_dark_titlebar(dlg)

        head = ctk.CTkFrame(dlg, fg_color="transparent")
        head.pack(fill="x", padx=20, pady=(16, 8))
        ctk.CTkLabel(head, text="Error Log",
                      font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                      text_color=COLORS["text"]
                      ).pack(side="left")

        def _clear():
            """Surface PermissionError instead of a silent green tick. On
            Windows, error_log.txt may be held open by a bot subprocess; if the
            clear fails we must tell the user rather than falsely report "Error
            log cleared" (the badge would reappear on the next poll).
            """
            try:
                with open("error_log.txt", "w") as f:
                    f.close()
                box.config(state="normal")
                box.delete("1.0", "end")
                box.insert("end", "Error log cleared.")
                box.config(state="disabled")
                self.sb_errors.set("No errors")
                self.sb_errors._lbl.configure(text_color=COLORS["success"])
            except PermissionError as e:
                # Show the actual error in the UI
                box.config(state="normal")
                box.insert(
                    "end",
                    f"\n\n Clear FAILED: a bot has the file open.\n"
                    f"Stop the bots first or wait for the next log rotation.\n"
                    f"({e})\n"
                )
                box.config(state="disabled")
                box.see("end")
            except OSError as e:
                box.config(state="normal")
                box.insert("end", f"\n\n Clear FAILED: {e}\n")
                box.config(state="disabled")
                box.see("end")

        def _copy():
            try:
                content = box.get("1.0", "end")
                self.clipboard_clear()
                self.clipboard_append(content)
            except Exception:
                pass

        btns = ctk.CTkFrame(head, fg_color="transparent")
        btns.pack(side="right")
        ctk.CTkButton(btns, text="Copy All", width=90, height=28,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color=COLORS["purple"], hover_color=COLORS["purple_dim"],
                       text_color="#ffffff", corner_radius=6, command=_copy
                       ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btns, text="Clear Log", width=90, height=28,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["danger"], border_width=1,
                       border_color=COLORS["border"], corner_radius=6, command=_clear
                       ).pack(side="left")

        log_frame = ctk.CTkFrame(dlg, fg_color=COLORS["bg"], corner_radius=8)
        log_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))

        box = tk.Text(log_frame, bg=COLORS["bg"], fg=COLORS["text_dim"],
                       font=(self.mono_font, 10, "bold"),
                       highlightthickness=0, bd=0, padx=12, pady=10,
                       wrap="word", state="disabled")
        box.pack(side="left", fill="both", expand=True)
        sb = tk.Scrollbar(log_frame, command=box.yview,
                           bg=COLORS["panel"], troughcolor=COLORS["bg"],
                           borderwidth=0, highlightthickness=0)
        sb.pack(side="right", fill="y")
        box.config(yscrollcommand=sb.set)
        box.config(state="normal")
        if os.path.exists("error_log.txt"):
            try:
                with open("error_log.txt", encoding="utf-8") as f:
                    content = f.read().strip()
                box.insert("end", content if content else "No errors logged.")
            except Exception:
                box.insert("end", "Could not read error log.")
        else:
            box.insert("end", "No errors logged yet.")
        box.config(state="disabled")
        box.see("end")

    #  LOG OUTPUT 

    def _log_to_card(self, card, severity, msg):
        from launcher.ui.logging_panel import log_to_card
        log_to_card(card, severity, msg)

    def _severity_to_badge(self, severity, msg=''):
        from launcher.ui.logging_panel import _severity_to_badge
        return _severity_to_badge(severity, msg)

    def _write_log_to_box(self, box, auto_var, severity, msg):
        from launcher.ui.logging_panel import write_log_to_box
        write_log_to_box(box, auto_var, severity, msg)

    def _update_ai_badge(self, card, running, global_llm_online, now_ts,
                         uses_llm=True):
        """Shim  delegates to launcher.ui.logging_panel.update_ai_badge.

        update_ai_badge reads ai_mode/ai_mode_ts directly from `card` and
        takes only `now_ts`.
        """
        from launcher.ui.logging_panel import update_ai_badge
        update_ai_badge(card, running, global_llm_online, now_ts, uses_llm)

    def _classify_severity(self, line):
        from launcher.ui.logging_panel import classify_severity
        return classify_severity(line)

    def _detect_ai_mode_from_log(self, line):
        from launcher.ui.logging_panel import detect_ai_mode_from_log
        return detect_ai_mode_from_log(line)

    def _reset_virtual_capital(self):
        """Reset the virtual-capital display to 1000 USDT.

        No DB rows are deleted; only the display offset is stored.
        """
        dlg = ctk.CTkToplevel(self)
        dlg.title("Reset Virtual Capital")
        dlg.geometry("400x190")
        dlg.configure(fg_color=COLORS["panel"])
        dlg.grab_set()
        dlg.transient(self)
        force_dark_titlebar(dlg)

        ctk.CTkLabel(dlg, text="!",
                      font=ctk.CTkFont(self.mono_font, 36, "bold"),
                      text_color=COLORS["warning"]).pack(pady=(20, 4))
        ctk.CTkLabel(dlg, text="Reset Virtual Capital Display?",
                      font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                      text_color=COLORS["text"]).pack()
        ctk.CTkLabel(dlg,
                      text="The display resets to 1000 USDT.\nNo trades are deleted.",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_dim"], justify="center").pack(pady=(8, 16))

        row = ctk.CTkFrame(dlg, fg_color="transparent")
        row.pack()

        def _confirm():
            # Store current PnL as offset so the display jumps back to 1000.
            try:
                cache = self.poller.get_all()
                all_stats = cache.get("stats", {})
                current_pnl = sum(
                    s["pnl"] for b, s in all_stats.items()
                    if self.config.get(b, {}).get("SIMULATION", True)
                )
                self._vc_offset = current_pnl
            except Exception:
                self._vc_offset = 0.0
            dlg.destroy()

        ctk.CTkButton(row, text="Reset Display",
                       fg_color=COLORS["warning"], hover_color="#d97706",
                       text_color="#000000", width=130, height=34, corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       command=_confirm).pack(side="left", padx=6)
        ctk.CTkButton(row, text="Cancel",
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"], width=100, height=34, corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       border_width=1, border_color=COLORS["border"],
                       command=dlg.destroy).pack(side="left", padx=6)

    def _refresh_tick(self):
        """Guarded driver for the 500ms refresh loop. A widget/data error in
        _refresh must NEVER kill the loop  otherwise the whole UI freezes at
        defaults/0 (which is exactly what happened). Reschedule ALWAYS."""
        try:
            self._refresh()
        except Exception as e:
            if not getattr(self, "_refresh_err_logged", False):
                try:
                    import sys as _sys
                    _sys.stderr.write(f"[UI] _refresh error (loop kept alive): {e}\n")
                except Exception:
                    pass
                self._refresh_err_logged = True
        finally:
            self.after(500, self._refresh_tick)

    def _safe_pack_before(self, widget, before, **kw):
        """pack(before=...) but never raise: on TclError (e.g. the reference
        widget isn't packed) fall back to a plain pack so one ordering glitch
        can't crash the entire refresh."""
        try:
            widget.pack(before=before, **kw)
        except Exception:
            try:
                widget.pack(**kw)
            except Exception:
                pass

    def _apply_sim_badge(self, bot: str, is_sim: bool):
        """Set a card's SIM/LIVE badge text+colours to match the given mode."""
        card = self.cards.get(bot)
        if not card:
            return
        btn = card.get("sim_btn")
        if not btn:
            return
        try:
            if not btn.winfo_exists():
                return
            btn.configure(
                text="  SIM  " if is_sim else "  LIVE  ",
                fg_color="#1b1730" if is_sim else "#2d0f0c",
                hover_color="#241f40" if is_sim else "#3d1410",
                text_color="#b07ae0" if is_sim else "#e88a6a",
            )
        except Exception:
            pass

    def _runtime_sim_for_running_bot(self, bot: str) -> bool | None:
        """Return a running bot's own SIM/LIVE mode when the heartbeat is fresh."""
        try:
            if not self.bots[bot].is_running():
                return None
            rs = read_runtime_status(BOT_META[bot]["log_dir"])
            run_id = str(getattr(self.bots[bot], "run_id", "") or "")
            if run_id and str(rs.get("run_id") or "") != run_id:
                return None
            try:
                age = time.monotonic() - float(rs.get("monotonic_ts") or 0.0)
            except (TypeError, ValueError):
                return None
            if age < -5.0 or age > 45.0:
                return None
            if "simulation" in rs:
                return bool(rs.get("simulation"))
        except Exception:
            return None
        return None

    def _external_runtime_status(self, bot: str) -> dict | None:
        """Fresh runtime_status from a bot process not owned by this launcher."""
        try:
            if self.bots[bot].is_running():
                return None
            rs = read_runtime_status(BOT_META[bot]["log_dir"])
            if not _runtime_status_is_fresh(rs):
                return None
            try:
                pid = int(rs.get("pid") or 0)
            except (TypeError, ValueError):
                pid = 0
            from core.process_identity import pid_matches_bot
            if not pid_matches_bot(pid, bot):
                return None
            return rs
        except Exception:
            return None

    def _externally_active_bots(self) -> dict[str, dict]:
        """Fresh runtime_status rows for bot processes owned elsewhere."""
        out: dict[str, dict] = {}
        for bot in BOT_ORDER:
            rs = self._external_runtime_status(bot)
            if rs is not None:
                out[bot] = rs
        return out

    def _sync_sim_state(self):
        """Keep the in-memory SIMULATION flags AND the per-card SIM/LIVE badge in
        sync with bot_config.json on disk. The flag can change OUTSIDE the launcher
        (a bot process, an external edit, the flag-file restore on restart); the
        launcher only loaded self.config once at startup. Without this the badge
        shows a STALE mode (e.g. LIVE while the bot is really SIM) and a later
        Save would write the stale in-memory mode back over the real on-disk one.
        Re-reading the authoritative flag fixes both. Throttled to ~2s."""
        now = time.time()
        if now - getattr(self, "_sim_sync_ts", 0.0) < 2.0:
            return
        self._sim_sync_ts = now
        try:
            from bot_utils.sim_flag import read_simulation_flag
        except Exception:
            return
        for bot in BOT_ORDER:
            runtime_sim = self._runtime_sim_for_running_bot(bot)
            if runtime_sim is not None:
                if bool(self.config.get(bot, {}).get("SIMULATION", True)) != runtime_sim:
                    self.config.setdefault(bot, {})["SIMULATION"] = runtime_sim
                    self._apply_sim_badge(bot, runtime_sim)
                continue
            if self.bots[bot].is_running():
                continue
            external_rs = self._external_runtime_status(bot)
            if external_rs is not None and "simulation" in external_rs:
                external_sim = bool(external_rs.get("simulation"))
                if bool(self.config.get(bot, {}).get("SIMULATION", True)) != external_sim:
                    self.config.setdefault(bot, {})["SIMULATION"] = external_sim
                    self._apply_sim_badge(bot, external_sim)
                continue
            try:
                # raise_on_corrupt=False: a transiently locked/half-written config
                # must not crash the UI loop  keep the last known badge instead.
                disk_sim = bool(read_simulation_flag(bot, raise_on_corrupt=False))
            except Exception:
                continue
            if bool(self.config.get(bot, {}).get("SIMULATION", True)) != disk_sim:
                self.config.setdefault(bot, {})["SIMULATION"] = disk_sim
                self._apply_sim_badge(bot, disk_sim)

    def _refresh(self):
        # Logs verarbeiten
        now_ts = time.time()
        self._sync_sim_state()
        for bot in BOT_ORDER:
            card = self.cards[bot]
            q = self.log_queues[bot]
            count = 0
            # If the queue has built up > 1000 entries (bot in error-loop or
            # backtest burst producing 100+ lines/sec) we can never catch up
            # reading 25 lines every 500 ms.  Drain aggressively: discard
            # everything except the most recent 50 entries and insert a gap
            # notice so the user knows logs were skipped.
            try:
                backlog = q.qsize()
            except Exception:
                backlog = 0
            if backlog > 1000:
                discarded = 0
                while q.qsize() > 50:
                    try:
                        q.get_nowait()
                        discarded += 1
                    except queue.Empty:
                        break
                if discarded:
                    self._log_to_card(
                        card, "warn",
                        f"[System] {discarded} log lines skipped  UI lagging, "
                        f"catching up to present ({backlog} queued)"
                    )

            while not q.empty() and count < 25:
                try:
                    line = q.get_nowait()
                    sev = self._classify_severity(line)
                    clean = re.sub(r'\x1b\[[0-9;]*m', '', line)
                    self._log_to_card(card, sev, clean)

                    # AI-Mode aus Logs ableiten
                    mode = self._detect_ai_mode_from_log(clean)
                    if mode is not None:
                        card["ai_mode"] = mode
                        card["ai_mode_ts"] = now_ts

                    count += 1
                except queue.Empty:
                    break

        cache = self.poller.get_all()
        global_llm = cache.get("llm") or {"online": False}
        global_llm_online = bool(global_llm.get("online"))

        # Pro Bot
        for bot in BOT_ORDER:
            card = self.cards[bot]
            running = self.bots[bot].is_running()
            external_rs = None if running else self._external_runtime_status(bot)
            external_running = external_rs is not None
            _is_sim = bool(self.config.get(bot, {}).get("SIMULATION", True))
            if running:
                status_label = "Active"
                try:
                    rs = read_runtime_status(BOT_META[bot]["log_dir"])
                    run_id = str(getattr(self.bots[bot], "run_id", "") or "")
                    if str(rs.get("run_id") or "") == run_id:
                        try:
                            stale_age = time.monotonic() - float(
                                rs.get("monotonic_ts") or 0.0)
                        except (TypeError, ValueError):
                            stale_age = 0.0
                        status_label = str(rs.get("status") or "starting").title()
                        build = str(rs.get("build_id") or "")
                        if build and build != "unknown":
                            status_label += f" - {build[:8]}"
                        if "simulation" in rs and -5.0 <= stale_age <= 45.0:
                            _is_sim = bool(rs.get("simulation"))
                        if stale_age > 45.0:
                            status_label = f"Stale {int(stale_age)}s"
                    else:
                        status_label = "Starting"
                except Exception:
                    status_label = "Active"
                # Health-at-a-glance: green = running LIVE, cyan = running SIM.
                card["status"].set("Active - " + ("SIM" if _is_sim else "LIVE"))
                try:
                    card["status"].set(
                        status_label + " - " + ("SIM" if _is_sim else "LIVE"))
                    card["led"].configure(
                        text_color=COLORS["info"] if _is_sim else COLORS["success"])
                except Exception:
                    pass
                card["start_btn"].configure(state="disabled",
                                              fg_color=COLORS["border"],
                                              border_width=2,
                                              border_color="#4b4664",
                                              text_color=COLORS["text_muted"])
                card["stop_btn"].configure(state="normal",
                                           fg_color="transparent",
                                           hover_color="#3a1820",
                                           text_color=COLORS["danger"],
                                           border_width=2,
                                           border_color=COLORS["danger"])
            elif external_running:
                try:
                    if "simulation" in external_rs:
                        _is_sim = bool(external_rs.get("simulation"))
                    status_label = str(external_rs.get("status") or "running").title()
                    build = str(external_rs.get("build_id") or "")
                    if build and build != "unknown":
                        status_label += f" - {build[:8]}"
                    card["status"].set(
                        f"External {status_label} - "
                        f"{'SIM' if _is_sim else 'LIVE'}")
                    card["led"].configure(text_color=COLORS["warning"])
                except Exception:
                    card["status"].set("External Active")
                card["start_btn"].configure(state="disabled",
                                              fg_color=COLORS["border"],
                                              border_width=2,
                                              border_color="#4b4664",
                                              text_color=COLORS["text_muted"])
                card["stop_btn"].configure(state="disabled",
                                           fg_color="transparent",
                                           hover_color=COLORS["panel_hover"],
                                           text_color=COLORS["text_muted"],
                                           border_width=2,
                                           border_color=COLORS["border"])
            else:
                card["status"].set("Stopped")
                try:
                    card["led"].configure(text_color=COLORS["text_muted"])
                except Exception:
                    pass
                card["start_btn"].configure(state="normal",
                                              fg_color=COLORS["success"],
                                              hover_color="#1ea350",
                                              border_width=2,
                                              border_color="#a7f3d0",
                                              text_color="#ffffff")
                card["stop_btn"].configure(state="disabled",
                                           fg_color="transparent",
                                           hover_color=COLORS["panel_hover"],
                                           text_color=COLORS["text_muted"],
                                           border_width=2,
                                           border_color=COLORS["border"])

            uses_llm = bool(BOT_META[bot].get("uses_llm", True)
                            and self.config.get(bot, {}).get("USE_LLM", False))
            # AI-Mode Badge updaten
            self._update_ai_badge(card, running or external_running,
                                  global_llm_online, now_ts,
                                  uses_llm=uses_llm)
            try:
                card["pnl_title_var"].set(
                    "REALIZED (SIM)" if _is_sim else "REALIZED (LIVE)")
            except Exception:
                pass
            rebalance_btn = card.get("rebalance_btn")
            if rebalance_btn is not None:
                if running:
                    rebalance_btn.configure(
                        state="normal",
                        fg_color="#6D28D9",
                        hover_color="#5b21b6",
                        text_color="#ffffff",
                        border_width=0,
                    )
                else:
                    rebalance_btn.configure(
                        state="disabled",
                        fg_color="transparent",
                        hover_color=COLORS["panel_hover"],
                        text_color=COLORS["text_muted"],
                        border_width=1,
                        border_color=COLORS["border"],
                    )

            restart_reason = self._config_restart_required_reason(bot)
            if restart_reason:
                card["restart_hint"].configure(text=f"! {restart_reason}")
            else:
                card["restart_hint"].configure(text="")

            stats = cache.get("stats", {}).get(bot, {"pnl": 0, "total": 0, "wr": 0,
                                                          "today_pnl": 0, "today_cnt": 0})
            sign = "+" if stats["pnl"] >= 0 else ""
            card["pnl_var"].set(f"{sign}{stats['pnl']:.2f}")
            color = COLORS["success"] if stats["pnl"] > 0 else COLORS["danger"] if stats["pnl"] < 0 else COLORS["text_dim"]
            card["pnl_lbl"].configure(text_color=color)
            card["total_var"].set(str(stats["total"]))
            today_pnl = float(stats.get("today_pnl", 0.0) or 0.0)
            card["today_var"].set(f"{today_pnl:+.2f}")
            if stats["total"] > 0:
                card["wr_var"].set(f"{stats['wr']:.0f}%")
            else:
                card["wr_var"].set("")
            card["open_var"].set(str(cache.get("open", {}).get(bot, 0)))

            #  Sparkline (PnL-Trend) aktualisieren 
            try:
                spark_vals = cache.get("sparkline", {}).get(bot, [])
                card["sparkline"].set_values(spark_vals)
            except Exception:
                pass

            #  Payoff-Kennzahl aktualisieren 
            # payoff = -Win / |-Loss|. Farbe: grn wenn der Erwartungswert
            # bei aktueller Win-Rate positiv ist, gelb (knapp), rot (negativ).
            # WICHTIG: payoff=0 (nur Verluste) ist eine ECHTE Aussage (rot),
            # nicht "keine Daten"  daher reicht total>0 als Bedingung.
            try:
                payoff   = stats.get("payoff", 0.0)
                avg_win  = stats.get("avg_win", 0.0)
                avg_loss = stats.get("avg_loss", 0.0)
                if stats["total"] > 0:
                    wr_frac = stats["wr"] / 100.0
                    # Erwartungswert pro Trade in "R": wr*payoff - (1-wr)
                    ev = wr_frac * payoff - (1.0 - wr_frac)
                    if ev > 0.05:
                        pcol = COLORS["success"]
                    elif ev >= -0.05:
                        pcol = COLORS["warning"]
                    else:
                        pcol = COLORS["danger"]
                    card["payoff_var"].set(f"{payoff:.2f}")
                    card["payoff_lbl"].configure(text_color=pcol)
                    # Detail-Zeile: nur die Seiten zeigen, die Daten haben
                    if avg_win > 0 and avg_loss < 0:
                        card["payoff_detail"].set(f"+{avg_win:.2f}/{avg_loss:.2f}")
                    elif avg_loss < 0:
                        card["payoff_detail"].set(f"{avg_loss:.2f} (0 Wins)")
                    elif avg_win > 0:
                        card["payoff_detail"].set(f"+{avg_win:.2f} (0 Loss)")
                    else:
                        card["payoff_detail"].set("")
                else:
                    card["payoff_var"].set("")
                    card["payoff_lbl"].configure(text_color=COLORS["text_dim"])
                    card["payoff_detail"].set("")
            except Exception:
                pass

            # Unrealized PnL  live aus DataPoller-Cache
            unr_val = cache.get("unrealized", {}).get(bot, 0.0)
            open_count = cache.get("open", {}).get(bot, 0)
            if open_count == 0:
                card["unr_var"].set("")
                card["unr_lbl"].configure(text_color=COLORS["text_muted"])
            else:
                sign = "+" if unr_val >= 0 else ""
                card["unr_var"].set(f"{sign}{unr_val:.2f}")
                unr_color = (COLORS["success"] if unr_val > 0
                              else COLORS["danger"] if unr_val < 0
                              else COLORS["text_muted"])
                card["unr_lbl"].configure(text_color=unr_color)
        m = cache.get("market")
        if m:
            phase = m["regime"]
            phase_color = (COLORS["success"] if phase == "BULL" else
                            COLORS["danger"]  if phase == "BEAR" else
                            COLORS["warning"])
            self.sb_phase.set(phase)
            self.sb_phase._lbl.configure(text_color=phase_color)
            btc_sign = "+" if m["btc_24h"] >= 0 else ""
            self.sb_btc.set(f"{btc_sign}{m['btc_24h']:.2f}%")
            btc_color = COLORS["success"] if m["btc_24h"] >= 0 else COLORS["danger"]
            self.sb_btc._lbl.configure(text_color=btc_color)
            fg_label = ("Extreme Fear" if m["fg"] <= 25 else
                         "Fear"          if m["fg"] <= 45 else
                         "Neutral"       if m["fg"] <= 55 else
                         "Greed"         if m["fg"] <= 75 else
                         "Extreme Greed")
            self.sb_fg.set(f"{m['fg']}  {fg_label}")
        else:
            self.sb_phase.set("offline")

        # Sidebar Account  Dynamisches Ein-/Ausblenden je nach Bot-Modus
        live_bal = cache.get("balance_live",  "")
        sim_bal  = cache.get("balance_paper", "")

        mode_cache = cache.get("mode_is_sim") or {}

        def _cache_sim(bot: str) -> bool:
            if bot in mode_cache:
                return bool(mode_cache[bot])
            return bool(self.config.get(bot, {}).get("SIMULATION", True))

        any_live = any(not _cache_sim(b) for b in BOT_ORDER)
        any_sim  = any(_cache_sim(b) for b in BOT_ORDER)

        # Live-Guthaben Sektion
        if any_live:
            self._safe_pack_before(self.sb_balance_live._wrap,
                                    self.sb_balance_sim._wrap.master,
                                    fill="x", pady=(0, 4))
            if "Error" in live_bal:
                self.sb_balance_live.set(live_bal)
                self.sb_balance_live._lbl.configure(text_color=COLORS["danger"])
            elif live_bal == "":
                self.sb_balance_live.set("Connecting...")
                self.sb_balance_live._lbl.configure(text_color=COLORS["text_muted"])
            else:
                self.sb_balance_live.set(live_bal)
                self.sb_balance_live._lbl.configure(text_color=COLORS["balanced"])

            # Aufschluesselung Spot/Futures anzeigen, wenn:
            #  Beide Wallets LIVE (Trennung sinnvoll), ODER
            #  Ein Wallet hat OFFENE POSITIONEN (equity > free  User
            #     sollte sehen, dass das Geld in Trades steckt, nicht
            #     "verschwunden" ist).
            spot_active    = cache.get("live_spot_active",    False)
            futures_active = cache.get("live_futures_active", False)
            spot_eq    = cache.get("spot_equity")    or {}
            futures_eq = cache.get("futures_equity") or {}
            spot_has_positions    = bool(spot_eq.get("open_count", 0))
            futures_has_positions = bool(futures_eq.get("open_count", 0))

            show_breakdown = (
                (spot_active and futures_active)
                or spot_has_positions
                or futures_has_positions
            )

            if show_breakdown:
                # Nur Wallets zeigen, die WIRKLICH aktiv sind
                if spot_active:
                    self._safe_pack_before(self.sb_balance_spot._wrap,
                                            self.sb_balance_sim._wrap.master,
                                            fill="x", pady=0)
                    self.sb_balance_spot.set(cache.get("balance_live_spot", ""))
                else:
                    self.sb_balance_spot._wrap.pack_forget()

                if futures_active:
                    self._safe_pack_before(self.sb_balance_futures._wrap,
                                            self.sb_balance_sim._wrap.master,
                                            fill="x", pady=(0, 4))
                    self.sb_balance_futures.set(cache.get("balance_live_futures", ""))

                    # Live-Update des Tooltip-Textes mit der Positions-Aufschluesselung
                    tip = getattr(self, "_futures_wallet_tooltip", None)
                    if tip is not None and futures_eq:
                        try:
                            positions = futures_eq.get("positions") or []
                            free_v   = futures_eq.get("free", 0.0)
                            in_pos_v = futures_eq.get("in_positions", 0.0)
                            upnl_v   = futures_eq.get("unrealized", 0.0)
                            equity_v = futures_eq.get("equity", 0.0)

                            lines = [f"Futures wallet equity: {equity_v:.2f} USDT", ""]
                            if positions:
                                lines.append(f"Open positions ({len(positions)}):")
                                # Fixed-width columns: symbol(8) side(5) margin(8) upnl(8)
                                for p in positions:
                                    sym = (p.get("symbol") or "?")[:8].ljust(8)
                                    side = (p.get("side") or "?")[:5].ljust(5)
                                    m = p.get("margin", 0.0)
                                    u = p.get("unrealized", 0.0)
                                    sign = "+" if u >= 0 else ""
                                    lines.append(
                                        f"  {sym} {side} {m:>6.2f}  {sign}{u:.2f}"
                                    )
                                lines.append("  " + "" * 30)
                                lines.append(f"  Free margin:   {free_v:>6.2f}")
                                lines.append(f"  in positions: {in_pos_v:>6.2f}")
                                sign_u = "+" if upnl_v >= 0 else ""
                                lines.append(f"  unrealized:  {sign_u}{upnl_v:>5.2f}")
                            else:
                                lines.append("No open positions.")
                                lines.append("")
                                lines.append(f"  Free margin:   {free_v:.2f} USDT")

                            tip.update_text("\n".join(lines))
                        except Exception:
                            # Bei jedem Fehler: statischer Default-Text bleibt aktiv
                            pass
                else:
                    self.sb_balance_futures._wrap.pack_forget()
            else:
                self.sb_balance_spot._wrap.pack_forget()
                self.sb_balance_futures._wrap.pack_forget()
        else:
            self.sb_balance_live._wrap.pack_forget()
            self.sb_balance_spot._wrap.pack_forget()
            self.sb_balance_futures._wrap.pack_forget()

        # Virtual Capital Sektion (nur sichtbar wenn mind. 1 Bot auf SIM steht)
        if any_sim:
            self.sb_balance_sim._wrap.master.pack(fill="x", pady=0)
            # Offset abziehen damit die Anzeige nach Reset bei 1000 USDT startet.
            # Only SIM bots count toward the paper Virtual Capital  a LIVE
            # bot's realized PnL must not leak into it (separate worlds).
            all_stats = cache.get("stats", {})
            raw_pnl   = sum(s["pnl"] for b, s in all_stats.items()
                            if _cache_sim(b))
            vc_value  = 1000.0 + raw_pnl - self._vc_offset
            self.sb_balance_sim.set(f"{vc_value:.2f} USDT")
        else:
            self.sb_balance_sim._wrap.master.pack_forget()

        # Sidebar performance follows the active money scope: LIVE when at
        # least one live bot exists, otherwise SIM. This avoids mixing paper
        # and real PnL in one headline number.
        all_stats = cache.get("stats", {})
        money_scope_live = any_live
        scoped_stats = [
            s for b, s in all_stats.items()
            if _cache_sim(b) != money_scope_live
        ]
        total       = sum(float(s.get("pnl", 0.0) or 0.0) for s in scoped_stats)
        total_today = sum(float(s.get("today_pnl", 0.0) or 0.0) for s in scoped_stats)
        total_count = sum(int(s.get("total", 0) or 0) for s in scoped_stats)
        total_wins  = sum(
            (float(s.get("wr", 0.0) or 0.0) / 100.0 * int(s.get("total", 0) or 0))
            for s in scoped_stats
        )
        avg_wr      = (total_wins / total_count * 100) if total_count > 0 else 0

        try:
            self.sb_total._header_lbl.configure(text=("LIVE PnL" if money_scope_live else "SIM PnL"))
        except Exception:
            pass
        sign = "+" if total >= 0 else ""
        self.sb_total.set(f"{sign}{total:.2f} USDT")
        total_color = COLORS["success"] if total > 0 else COLORS["danger"] if total < 0 else COLORS["text_dim"]
        self.sb_total._lbl.configure(text_color=total_color)

        sign_t = "+" if total_today >= 0 else ""
        self.sb_today.set(f"{sign_t}{total_today:.2f} USDT")
        today_color = COLORS["success"] if total_today > 0 else COLORS["danger"] if total_today < 0 else COLORS["text_dim"]
        self.sb_today._lbl.configure(text_color=today_color)

        self.sb_trades.set(str(total_count))
        if total_count > 0:
            self.sb_winrate.set(f"{avg_wr:.0f}%")
            wr_color = COLORS["success"] if avg_wr >= 50 else COLORS["warning"] if avg_wr >= 40 else COLORS["danger"]
            self.sb_winrate._lbl.configure(text_color=wr_color)
        else:
            self.sb_winrate.set("")

        # Sidebar: total unrealized PnL across all bots.
        unr_cache = cache.get("unrealized", {})
        open_cache = cache.get("open", {})
        total_open = sum(
            int(open_cache.get(b, 0) or 0)
            for b in BOT_ORDER
            if _cache_sim(b) != money_scope_live
        )
        if total_open > 0:
            total_unr = sum(
                float(unr_cache.get(b, 0.0) or 0.0)
                for b in BOT_ORDER
                if _cache_sim(b) != money_scope_live
            )
            sign_u = "+" if total_unr >= 0 else ""
            self.sb_unr_total.set(f"{sign_u}{total_unr:.2f} USDT")
            unr_color = (COLORS["success"] if total_unr > 0
                          else COLORS["danger"] if total_unr < 0
                          else COLORS["text_muted"])
            self.sb_unr_total._lbl.configure(text_color=unr_color)
        else:
            self.sb_unr_total.set("")
            self.sb_unr_total._lbl.configure(text_color=COLORS["text_muted"])
        # Per-bot open counts (same 'open' cache the poller fills for ALL bots,
        # scoped per bot). Futures-type bots show in their accent colour.
        for _pos_var, _key, _hl in (
                (self.sb_trend_pos,   "TREND",   COLORS["balanced"]),
                (self.sb_spot_pos,    "SPOT",    COLORS["balanced"]),
                (self.sb_cross_pos,   "CROSS",   COLORS["cross"]),
                (self.sb_futrend_pos, "FUTREND", COLORS["futrend"])):
            _n = open_cache.get(_key, 0)
            _pos_var.set(str(_n))
            try:
                _pos_var._lbl.configure(
                    text_color=_hl if _n > 0 else COLORS["text"])
            except Exception:
                pass
        fut_open = open_cache.get("FUTURES", 0)
        self.sb_fut_pos.set(str(fut_open))
        if fut_open > 0:
            self.sb_fut_pos._lbl.configure(text_color=COLORS["futures"])
        else:
            self.sb_fut_pos._lbl.configure(text_color=COLORS["text"])

        for _bot, _count in (
                ("FUTURES", fut_open),
                ("CROSS", open_cache.get("CROSS", 0)),
                ("FUTREND", open_cache.get("FUTREND", 0))):
            _btn = self.emergency_btns.get(_bot)
            if _btn is None:
                continue
            if _count > 0:
                _btn.configure(
                    fg_color="#5d1010", hover_color="#7d1818",
                    text_color="#ffffff"
                )
            else:
                _btn.configure(
                    fg_color=COLORS["danger"], hover_color="#b91c1c",
                    text_color="#ffffff"
                )

        # System Monitor
        sys_stats = cache.get("system") or {}
        if sys_stats.get("cpu") is not None:
            self.bar_cpu.update_value(sys_stats["cpu"])
        else:
            self.bar_cpu.value_var.set("N/A")
            self.bar_cpu.bar.set(0)
        if sys_stats.get("ram") is not None:
            ram_text = f"{sys_stats['ram_used_gb']:.1f}/{sys_stats['ram_total_gb']:.0f}GB"
            self.bar_ram.update_value(sys_stats["ram"], subtitle=ram_text)
        else:
            self.bar_ram.value_var.set("N/A")
            self.bar_ram.bar.set(0)
        if sys_stats.get("gpu") is not None:
            self.bar_gpu.update_value(sys_stats["gpu"])
        else:
            self.bar_gpu.value_var.set("No NVIDIA")
            self.bar_gpu.bar.set(0)
            self.bar_gpu.bar.configure(progress_color=COLORS["bar_bg"])
        if sys_stats.get("vram_pct") is not None:
            used_gb = sys_stats["vram_used_mb"] / 1024
            total_gb = sys_stats["vram_total_mb"] / 1024
            vram_text = f"{used_gb:.1f}/{total_gb:.0f}GB"
            self.bar_vram.update_value(sys_stats["vram_pct"], subtitle=vram_text)
        else:
            self.bar_vram.value_var.set("")
            self.bar_vram.bar.set(0)
            self.bar_vram.bar.configure(progress_color=COLORS["bar_bg"])
        if sys_stats.get("gpu_name"):
            name = sys_stats["gpu_name"]
            for prefix in ("NVIDIA GeForce ", "NVIDIA "):
                if name.startswith(prefix):
                    name = name[len(prefix):]
                    break
            self.gpu_name_lbl.configure(text=f"  {name}")
        else:
            self.gpu_name_lbl.configure(text="")

        # Connections
        if os.path.exists(DB_PATH):
            self.sb_db.set("Connected")
            self.sb_db._lbl.configure(text_color=COLORS["success"])
        else:
            self.sb_db.set("Not found")
            self.sb_db._lbl.configure(text_color=COLORS["danger"])

        ex = cache.get("exchange") or {"active": False, "label": ""}
        self.sb_exchange.set(ex["label"])
        self.sb_exchange._lbl.configure(
            text_color=COLORS["success"] if ex["active"] else COLORS["text_muted"]
        )

        llm_enabled = any(
            BOT_META.get(bot, {}).get("uses_llm", True)
            and self.config.get(bot, {}).get("USE_LLM", False)
            for bot in BOT_ORDER
        )
        llm = cache.get("llm") or {"online": False, "model": None, "loaded": False}
        if not llm_enabled:
            self.sb_llm.set("Disabled")
            self.sb_llm._lbl.configure(text_color=COLORS["text_muted"])
            self.sb_llm_model.configure(text="  USE_LLM=false")
        elif llm["online"]:
            if llm["loaded"]:
                self.sb_llm.set("Active")
                self.sb_llm._lbl.configure(text_color=COLORS["success"])
            else:
                self.sb_llm.set("Ready")
                self.sb_llm._lbl.configure(text_color=COLORS["balanced"])
            self.sb_llm_model.configure(text=f"  {llm['model']}")
        else:
            self.sb_llm.set("Offline")
            self.sb_llm._lbl.configure(text_color=COLORS["warning"])
            self.sb_llm_model.configure(text="  Keyword fallback active")

        self.sb_clk.set(datetime.now().strftime("%H:%M:%S"))

        err_count = cache.get("error_count", 0)
        if err_count > 0:
            self.sb_errors.set(f"{err_count} error{'s' if err_count > 1 else ''}")
            self.sb_errors._lbl.configure(text_color=COLORS["danger"], cursor="hand2")
        else:
            self.sb_errors.set("No errors")
            self.sb_errors._lbl.configure(text_color=COLORS["success"])

        # Statusbar mit Ready/Live Badge updaten
        self._update_statusbar_state()

        #  Blocked Hours display: refresh from DB every ~30 s 
        # The risk_manager auto-updates bad_hours after learning from trades.
        # Polling at 500 ms  60 ticks = 30 s keeps the display in sync
        # without spamming the SQLite connection.
        self._bad_hours_refresh_ctr += 1
        if self._bad_hours_refresh_ctr >= 60:
            self._bad_hours_refresh_ctr = 0
            for bot, bhr in self.bad_hours_rows.items():
                try:
                    if bhr.winfo_exists():
                        bhr.refresh()
                except Exception:
                    pass

        # NOTE: rescheduling is handled by _refresh_tick() (the guarded driver),
        # so a crash anywhere above can never stop the refresh loop.

    def _pulse(self):
        self._pulse_step = (self._pulse_step + 1) % 20
        phase = abs(10 - self._pulse_step) / 10.0

        # Use global LLM status only while a bot's local mode is unknown.
        cache = self.poller.get_all()
        global_llm_online = bool((cache.get("llm") or {}).get("online"))

        for bot in BOT_ORDER:
            card = self.cards[bot]
            if self.bots[bot].is_running():
                # Mechanical bots (uses_llm=False) must not inherit the global
                # LLM status; show a neutral running indicator instead.
                uses_llm = bool(BOT_META[bot].get("uses_llm", True)
                                and self.config.get(bot, {}).get("USE_LLM", False))
                if not uses_llm:
                    base_color = COLORS.get("text_muted", COLORS["balanced"])
                else:
                    mode = card.get("ai_mode", "unknown")

                    if mode == "keyword":
                        base_color = COLORS["warning"]  # Keyword fallback.
                    elif mode == "llm":
                        base_color = COLORS["success"]  # Full LLM active.
                    else:
                        # No log signal yet: use Ollama status as estimate.
                        base_color = COLORS["success"] if global_llm_online else COLORS["warning"]

                # Soft pulse.
                color = base_color if phase > 0.3 else self._darker(base_color, 0.4)
                card["led"].configure(text_color=color)
            else:
                # Stopped: static red, no blinking.
                card["led"].configure(text_color=COLORS["danger"])

        self.after(150, self._pulse)

    def _darker(self, hex_color, factor=0.65):
        h = hex_color.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        r, g, b = int(r*factor), int(g*factor), int(b*factor)
        return f"#{r:02x}{g:02x}{b:02x}"

    #  CLOSE 

    def _on_close(self):
        """Handle the user clicking the X button on the main window.

        Workflow:
          1. If any bot has open positions  show comprehensive shutdown dialog
             with three options: Close All & Quit / Stop without closing / Cancel
          2. If bots running but no positions  ask Stop & Quit or Background
          3. If nothing running  close immediately
        """
        running = [name for name, bot in self.bots.items() if bot.is_running()]
        external = self._externally_active_bots()
        if not running and not external:
            self._shutdown_clean()
            return

        if external:
            from tkinter import messagebox
            names = ", ".join(external.keys())
            messagebox.showwarning(
                "External bots running",
                "Der Launcher sieht laufende Bot-Prozesse, die nicht von "
                f"diesem Fenster gestartet wurden: {names}.\n\n"
                "Bitte diese Session zuerst sauber stoppen. Der Launcher "
                "schliesst jetzt nicht, damit kein Live-Prozess unbeaufsichtigt "
                "weiterlaeuft."
            )
            return

        # Check if any bot has open positions
        open_summary = {}  # bot_name  {"count": int, "modes": set[str]}
        state_read_errors = []
        # Count per bot using the SAME source the close path uses: futures-type
        # bots (FUTURES, CROSS) from their scoped futures_state, spot bots from
        # their trades.json. Keeps the quit summary consistent with what each
        # bot's close path actually flattens.
        from launcher.config.settings import BOT_META as _BM, BOT_ORDER as _ORDER
        for _bot in _ORDER:
            try:
                if _BM[_bot].get("is_futures"):
                    positions = self._get_open_futures_positions(_bot)
                else:
                    positions = self._get_open_spot_positions(_bot)
                n_open = len(positions)
                if n_open > 0:
                    modes = {
                        str(p.get("mode") or "").upper()
                        for p in positions
                        if isinstance(p, dict) and p.get("mode")
                    }
                    open_summary[_bot] = {"count": n_open, "modes": modes}
            except Exception as exc:
                state_read_errors.append(f"{_bot}: {exc}")

        if state_read_errors:
            from launcher.ui.dialogs.shutdown import show_state_read_error_dialog
            show_state_read_error_dialog(
                self, "Quit Application", "\n".join(state_read_errors))
            return

        if open_summary:
            self._show_quit_with_positions_dialog(open_summary)
        else:
            self._show_quit_no_positions_dialog()

    def _shutdown_clean(self):
        """Clean shutdown: stop poller, close streamlit, destroy window."""
        try:
            self.poller.stop()
        except Exception:
            pass
        try:
            if self.streamlit:
                self.streamlit.terminate()
        except Exception:
            pass
        self.destroy()

    def _show_quit_no_positions_dialog(self):
        from launcher.ui.dialogs.shutdown import show_quit_no_positions_dialog
        show_quit_no_positions_dialog(self)

    def _show_quit_with_positions_dialog(self, open_summary):
        from launcher.ui.dialogs.shutdown import show_quit_with_positions_dialog
        show_quit_with_positions_dialog(self, open_summary)

    def _async_stop_all_and_quit(self, update, close_positions):
        from launcher.ui.dialogs.shutdown import async_stop_all_and_quit
        async_stop_all_and_quit(self, update, close_positions)
