"""
setup_wizard.pyw - Obsidian Trading Terminal - First-Time Setup

Auto-launched by the launcher when no .env exists.

4 steps:
  1. Select exchange (9 supported)
  2. API credentials
  3. Proxy settings (optional)
  4. Telegram + CryptoPanic (optional) + connection test
"""

import sys
import os
import json
import tempfile
import threading
from tkinter import font as tkfont

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from update_barrier import (  # noqa: E402 - project root bootstrap above
    UpdateInProgressError,
    assert_process_start_allowed,
    process_start_guard,
    update_lifecycle_lock,
)
from env_setup_files import (  # noqa: E402 - project root bootstrap above
    ENV_SETUP_TEMP_PREFIX,
    ENV_SETUP_TEMP_SUFFIX,
    cleanup_stale_env_temps,
    harden_windows_private_file,
    unlink_env_temp,
)

try:
    assert_process_start_allowed(_PROJECT_ROOT)
except UpdateInProgressError as exc:
    raise SystemExit(str(exc)) from exc

import customtkinter as ctk  # noqa: E402 - update barrier precedes UI import

# -- Design (matches launcher.pyw) ---------------------------------------------
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

# FIX: FONT_BODY was missing - caused NameError crash preventing wizard from opening
FONT_BODY = "Segoe UI"

COLORS = {
    "bg":           "#0a0e14",
    "bg_alt":       "#0d121a",
    "panel":        "#131822",
    "panel_hover":  "#1a2030",
    "border":       "#1f2937",
    "border_soft":  "#1a2030",

    "text":         "#f0f5fa",
    "text_dim":     "#a3b1c2",
    "text_muted":   "#5a6b80",
    "text_subtle":  "#3d4858",

    "balanced":     "#06b6d4",
    "aggressive":   "#10b981",
    "purple":       "#8b5cf6",
    "purple_dim":   "#7c3aed",
    "success":      "#22c55e",
    "warning":      "#f59e0b",
    "danger":       "#ef4444",

    "input_bg":     "#0d1118",
}

def _get_pythonw_exe() -> str:
    """Lokales pythonw.exe (embedded) oder System-pythonw."""
    base = os.path.dirname(os.path.abspath(__file__))
    local = os.path.join(base, "python", "pythonw.exe")
    if os.path.exists(local):
        return local
    py = sys.executable
    if os.path.basename(py).lower() == "python.exe":
        return os.path.join(os.path.dirname(py), "pythonw.exe")
    return py

def _safe_mono_font():
    try:
        avail = set(tkfont.families())
        for f in ("Cascadia Mono", "JetBrains Mono", "Consolas", "Courier New"):
            if f in avail:
                return f
    except Exception:
        pass
    return "Consolas"


def _write_new_private_text_file(path: str, content: str) -> None:
    """Durably publish a complete private file without replacing an existing one."""
    directory = os.path.dirname(os.path.abspath(path))
    basename = os.path.basename(path)
    fd, temp_path = tempfile.mkstemp(
        prefix=ENV_SETUP_TEMP_PREFIX if basename == ".env" else f"{basename}.",
        suffix=ENV_SETUP_TEMP_SUFFIX,
        dir=directory,
    )
    try:
        stream = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        fd = -1
        with stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        try:
            import stat as _stat
            os.chmod(temp_path, _stat.S_IRUSR | _stat.S_IWUSR)
        except (OSError, AttributeError):
            # mkstemp is already 0600 on POSIX; Windows uses the user ACL.
            pass

        try:
            if os.name == "nt":
                os.rename(temp_path, path)
                temp_path = ""
            else:
                os.link(temp_path, path)
                unlink_env_temp(temp_path)
                temp_path = ""
        except FileExistsError as exc:
            raise RuntimeError(
                ".env already exists; edit Env Settings instead of rerunning setup"
            ) from exc

        if os.name == "nt":
            try:
                harden_windows_private_file(path)
            except Exception as acl_error:
                # The file was created by this call and has not been exposed to
                # any caller yet. Fail closed instead of leaving credentials
                # protected only by an inherited directory ACL.
                try:
                    os.unlink(path)
                except OSError as cleanup_error:
                    acl_error.add_note(
                        "Private file cleanup after ACL failure also failed: "
                        f"{type(cleanup_error).__name__}"
                    )
                raise

        if os.name != "nt":
            dir_fd = -1
            try:
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                dir_fd = os.open(directory, flags)
                os.fsync(dir_fd)
            finally:
                if dir_fd >= 0:
                    os.close(dir_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temp_path:
            active_error = sys.exc_info()[1]
            try:
                unlink_env_temp(temp_path)
            except OSError as cleanup_error:
                if active_error is None:
                    raise
                active_error.add_note(str(cleanup_error))


# -- Exchange Definitions ------------------------------------------------------
# USDT-linear-perp exchanges supported by ccxt. Kraken & Coinbase are excluded:
# their perps are USD/multi-collateral margined, which the bot's hardcoded
# USDT-settle symbol scheme doesn't resolve.

EXCHANGES = {
    "bitget": {
        "label":       "Bitget",
        "passphrase":  True,
        "tested_with": "Adapter available; verify account permissions and behavior in SIM",
        "color":       COLORS["balanced"],
    },
    "binance": {
        "label":       "Binance",
        "passphrase":  False,
        "tested_with": "Adapter available; verify account permissions and behavior in SIM",
        "color":       "#f0b90b",
    },
    "okx": {
        "label":       "OKX",
        "passphrase":  True,
        "tested_with": "Adapter available; verify account permissions and behavior in SIM",
        "color":       COLORS["purple"],
    },
    "bybit": {
        "label":       "Bybit",
        "passphrase":  False,
        "tested_with": "Adapter available; verify account permissions and behavior in SIM",
        "color":       COLORS["warning"],
    },
    "kucoin": {
        "label":       "KuCoin",
        "passphrase":  True,
        "tested_with": "Adapter available; verify account permissions and behavior in SIM",
        "color":       COLORS["success"],
    },
    "gateio": {
        "label":       "Gate.io",
        "passphrase":  False,
        "tested_with": "Adapter available; verify account permissions and behavior in SIM",
        "color":       COLORS["aggressive"],
    },
    "mexc": {
        "label":       "MEXC",
        "passphrase":  False,
        "tested_with": "Adapter available; verify account permissions and behavior in SIM",
        "color":       "#0EA5E9",
    },
}


# -- Wizard App ----------------------------------------------------------------

class SetupWizard(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Obsidian - First-Time Setup")
        screen_w = max(640, self.winfo_screenwidth())
        screen_h = max(560, self.winfo_screenheight())
        win_w = min(780, max(640, screen_w - 80))
        win_h = min(680, max(560, screen_h - 120))
        self.geometry(f"{win_w}x{win_h}")
        self.minsize(min(760, win_w), min(620, win_h))
        self.resizable(True, True)
        self.configure(fg_color=COLORS["bg"])

        self.mono_font = _safe_mono_font()

        self.current_step = 1
        self.total_steps  = 4
        self.data = {
            "exchange":         "bitget",
            "api_key":          "",
            "api_secret":       "",
            "passphrase":       "",
            "use_proxy":        False,
            "proxy_host":       "127.0.0.1",
            "proxy_port":       "10808",
            "use_telegram":     False,
            "telegram_token":   "",
            "telegram_chat_id": "",
            "use_cryptopanic":  False,
            "cryptopanic_token":"",
            "cmc_api_key":      "",
            "timezone":         "Asia/Shanghai",
        }

        self._build_ui()
        self._show_step(1)

        # Center on screen
        self.update_idletasks()
        w, h = self.winfo_width(), self.winfo_height()
        x = (self.winfo_screenwidth()  - w) // 2
        y = (self.winfo_screenheight() - h) // 2
        self.geometry(f"+{x}+{y}")

    # -- UI --------------------------------------------------------------------

    def _build_ui(self):
        top = ctk.CTkFrame(self, fg_color=COLORS["panel"], height=62, corner_radius=0)
        top.pack(fill="x")
        top.pack_propagate(False)

        ctk.CTkFrame(self, fg_color=COLORS["border"], height=1, corner_radius=0).pack(fill="x")

        logo = ctk.CTkFrame(top, fg_color="transparent")
        logo.pack(side="left", padx=26, pady=14)

        ctk.CTkLabel(
            logo, text="*",
            font=ctk.CTkFont(self.mono_font, 24, "bold"),
            text_color=COLORS["purple"]
        ).pack(side="left", padx=(0, 12))

        ctk.CTkLabel(
            logo, text="OBSIDIAN",
            font=ctk.CTkFont(FONT_BODY, 18, "bold"),
            text_color=COLORS["text"]
        ).pack(side="left")

        ctk.CTkLabel(
            logo, text="SETUP",
            font=ctk.CTkFont(FONT_BODY, 13, "bold"),
            text_color=COLORS["text_muted"]
        ).pack(side="left", padx=(8, 0))

        self.progress_lbl = ctk.CTkLabel(
            top, text=f"Step 1 / {self.total_steps}",
            font=ctk.CTkFont(FONT_BODY, 13, "bold"),
            text_color=COLORS["text_dim"]
        )
        self.progress_lbl.pack(side="right", padx=26, pady=14)

        self.progress_bar = ctk.CTkProgressBar(
            self, height=3, corner_radius=0,
            progress_color=COLORS["purple"], fg_color=COLORS["panel"]
        )
        self.progress_bar.pack(fill="x")
        self.progress_bar.set(0.25)

        self.content = ctk.CTkFrame(self, fg_color="transparent")
        self.content.pack(fill="both", expand=True, padx=40, pady=30)

        bottom = ctk.CTkFrame(self, fg_color=COLORS["panel"], height=72, corner_radius=0)
        bottom.pack(fill="x", side="bottom")
        bottom.pack_propagate(False)

        ctk.CTkFrame(self, fg_color=COLORS["border"], height=1, corner_radius=0).pack(
            fill="x", side="bottom"
        )

        btn_box = ctk.CTkFrame(bottom, fg_color="transparent")
        btn_box.pack(fill="x", padx=26, pady=18)

        self.btn_back = ctk.CTkButton(
            btn_box, text="<  Back",
            width=120, height=36, corner_radius=8,
            font=ctk.CTkFont(FONT_BODY, 13, "bold"),
            fg_color="transparent", hover_color=COLORS["panel_hover"],
            text_color=COLORS["text_dim"],
            border_width=1, border_color=COLORS["border"],
            command=self._prev_step
        )
        self.btn_back.pack(side="left")

        self.btn_next = ctk.CTkButton(
            btn_box, text="Next  >",
            width=140, height=36, corner_radius=8,
            font=ctk.CTkFont(FONT_BODY, 13, "bold"),
            fg_color=COLORS["purple"], hover_color=COLORS["purple_dim"],
            text_color="#ffffff",
            command=self._next_step
        )
        self.btn_next.pack(side="right")

        self.skip_lbl = ctk.CTkLabel(
            btn_box, text="",
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_muted"]
        )
        self.skip_lbl.pack(side="left", padx=(20, 0))

    def _show_step(self, n):
        for w in self.content.winfo_children():
            w.destroy()

        self.current_step = n
        self.progress_lbl.configure(text=f"Step {n} / {self.total_steps}")
        self.progress_bar.set(n / self.total_steps)

        if n == 1:
            self._build_step1_exchange()
        elif n == 2:
            self._build_step2_credentials()
        elif n == 3:
            self._build_step3_proxy()
        elif n == 4:
            self._build_step4_optional()

        self.btn_back.configure(state="disabled" if n == 1 else "normal")
        if n == self.total_steps:
            self.btn_next.configure(text="OK  Complete Setup",
                                     fg_color=COLORS["success"],
                                     hover_color="#16a34a")
        else:
            self.btn_next.configure(text="Next  >",
                                     fg_color=COLORS["purple"],
                                     hover_color=COLORS["purple_dim"])

    def _next_step(self):
        if not self._validate_step(self.current_step):
            return
        if self.current_step < self.total_steps:
            self._show_step(self.current_step + 1)
        else:
            self._finish()

    def _prev_step(self):
        if self.current_step > 1:
            self._show_step(self.current_step - 1)

    # -- STEP 1: Exchange Selection --------------------------------------------

    def _build_step1_exchange(self):
        self._heading(self.content, "Select Your Exchange",
                       "Which crypto exchange would you like to connect to?")

        # Scrollable container - supports 9+ exchanges nicely
        scroll = ctk.CTkScrollableFrame(
            self.content, fg_color="transparent",
            scrollbar_button_color=COLORS["border"], height=320
        )
        scroll.pack(fill="both", expand=True, pady=(20, 0))

        grid = ctk.CTkFrame(scroll, fg_color="transparent")
        grid.pack(fill="x")
        grid.grid_columnconfigure((0, 1, 2), weight=1, uniform="ex")

        self.exchange_buttons = {}
        row = col = 0
        for key, info in EXCHANGES.items():
            btn = self._exchange_card(grid, key, info, row, col)
            self.exchange_buttons[key] = btn
            col += 1
            if col > 2:
                col = 0
                row += 1

        self._highlight_exchange(self.data["exchange"])

        info_box = ctk.CTkFrame(self.content, fg_color=COLORS["panel"], corner_radius=8,
                                 border_width=1, border_color=COLORS["border"])
        info_box.pack(fill="x", pady=(20, 0))

        self.exchange_info_lbl = ctk.CTkLabel(
            info_box,
            text="i  Verify your exchange access in simulation; LIVE compatibility is account-dependent.",
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            text_color=COLORS["text_dim"], anchor="w", justify="left",
            wraplength=640
        )
        self.exchange_info_lbl.pack(fill="x", padx=16, pady=12)

    def _exchange_card(self, parent, key, info, row, col):
        card = ctk.CTkFrame(
            parent, fg_color=COLORS["panel"], corner_radius=12,
            border_width=2, border_color=COLORS["border"],
            height=72
        )
        card.grid(row=row, column=col, padx=6, pady=6, sticky="nsew")
        card.grid_propagate(False)
        card.bind("<Button-1>", lambda e, k=key: self._select_exchange(k))

        lbl = ctk.CTkLabel(
            card, text=info["label"],
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            text_color=info["color"]
        )
        lbl.place(relx=0.5, rely=0.5, anchor="center")
        lbl.bind("<Button-1>", lambda e, k=key: self._select_exchange(k))

        return card

    def _select_exchange(self, key):
        self.data["exchange"] = key
        self._highlight_exchange(key)
        self.exchange_info_lbl.configure(
            text=f"i  {EXCHANGES[key]['tested_with']}"
        )

    def _highlight_exchange(self, key):
        for k, btn in self.exchange_buttons.items():
            if k == key:
                btn.configure(border_color=EXCHANGES[k]["color"], border_width=2)
            else:
                btn.configure(border_color=COLORS["border"], border_width=1)

    # -- STEP 2: API Credentials -----------------------------------------------

    def _build_step2_credentials(self):
        ex = EXCHANGES[self.data["exchange"]]
        self._heading(self.content, f"API Access - {ex['label']}",
                       f"Create an API key in your {ex['label']} account and paste it here.")

        info = ctk.CTkFrame(self.content, fg_color=COLORS["panel"], corner_radius=8,
                            border_width=1, border_color=COLORS["border"])
        info.pack(fill="x", pady=(10, 20))
        ctk.CTkLabel(
            info,
            text="[secure]  Enable 'Trade' permission  -  DO NOT enable Withdraw permission!",
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            text_color=COLORS["warning"], wraplength=640
        ).pack(padx=16, pady=12)

        form = ctk.CTkFrame(self.content, fg_color="transparent")
        form.pack(fill="x")

        self.entry_key    = self._labeled_entry(form, "API Key",    self.data["api_key"], False)
        self.entry_secret = self._labeled_entry(form, "API Secret", self.data["api_secret"], True)

        if ex["passphrase"]:
            self.entry_pass = self._labeled_entry(form, "Passphrase", self.data["passphrase"], True)
        else:
            self.entry_pass = None

        self.skip_lbl.configure(text="")

    # -- STEP 3: Proxy ---------------------------------------------------------

    def _build_step3_proxy(self):
        self._heading(self.content, "Proxy Settings",
                       "If you're in a region where the exchange is blocked, "
                       "you can use a proxy (e.g. V2Ray, SSR).")

        switch_box = ctk.CTkFrame(self.content, fg_color=COLORS["panel"], corner_radius=10,
                                    border_width=1, border_color=COLORS["border"])
        switch_box.pack(fill="x", pady=(20, 20))

        switch_row = ctk.CTkFrame(switch_box, fg_color="transparent")
        switch_row.pack(fill="x", padx=20, pady=16)

        ctk.CTkLabel(
            switch_row, text="Use Proxy",
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            text_color=COLORS["text"], anchor="w"
        ).pack(side="left")

        self.proxy_var = ctk.BooleanVar(value=self.data["use_proxy"])
        ctk.CTkSwitch(
            switch_row, text="", variable=self.proxy_var,
            progress_color=COLORS["purple"],
            button_color=COLORS["text"], width=46,
            command=self._toggle_proxy_fields
        ).pack(side="right")

        # Fields box shown/hidden based on proxy toggle
        self.proxy_fields_box = ctk.CTkFrame(self.content, fg_color="transparent")

        form = ctk.CTkFrame(self.proxy_fields_box, fg_color="transparent")
        form.pack(fill="x")

        self.entry_proxy_host = self._labeled_entry(form, "Proxy Host (IP)", self.data["proxy_host"], False)
        self.entry_proxy_port = self._labeled_entry(form, "Proxy Port",      self.data["proxy_port"], False)

        ctk.CTkLabel(
            self.proxy_fields_box,
            text="Default for V2Ray/SSR is usually 127.0.0.1 with port 10808 (HTTP) or 10809 (SOCKS5)",
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_muted"], wraplength=640, justify="left"
        ).pack(fill="x", pady=(6, 0))

        self._toggle_proxy_fields()
        self.skip_lbl.configure(text="If unsure: leave proxy off")

    def _toggle_proxy_fields(self):
        if self.proxy_var.get():
            self.proxy_fields_box.pack(fill="x")
        else:
            self.proxy_fields_box.pack_forget()

    # -- STEP 4: Telegram + CryptoPanic + Test ---------------------------------

    def _build_step4_optional(self):
        self._heading(self.content, "Optional Services",
                       "Telegram notifications and News API. Both optional, recommended.")

        scroll = ctk.CTkScrollableFrame(
            self.content, fg_color="transparent",
            scrollbar_button_color=COLORS["border"]
        )
        scroll.pack(fill="both", expand=True, pady=(15, 0))

        # Timezone
        tz_box = ctk.CTkFrame(scroll, fg_color=COLORS["panel"], corner_radius=10,
                              border_width=1, border_color=COLORS["border"])
        tz_box.pack(fill="x", pady=(0, 14))
        ctk.CTkLabel(
            tz_box, text="[tz]  Deine Zeitzone / Your Timezone",
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            text_color=COLORS["text"], anchor="w"
        ).pack(fill="x", padx=20, pady=(14, 2))
        ctk.CTkLabel(
            tz_box,
            text="Wird fuer die Lernanalyse genutzt: schlechte Handelszeiten\n"
                 "werden in deiner Lokalzeit erkannt, nicht UTC.",
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_muted"], anchor="w", wraplength=640
        ).pack(fill="x", padx=20, pady=(0, 6))
        tz_inner = ctk.CTkFrame(tz_box, fg_color="transparent")
        tz_inner.pack(fill="x", padx=20, pady=(0, 14))
        ctk.CTkLabel(tz_inner, text="Timezone",
                     font=ctk.CTkFont(FONT_BODY, 11),
                     text_color=COLORS["text_muted"], width=120, anchor="w"
                     ).pack(side="left")
        # Common timezones - sorted by UTC offset, with friendly labels
        TZ_OPTIONS = [
            "UTC",
            "Asia/Shanghai      (UTC+8, China)",
            "Asia/Hong_Kong     (UTC+8)",
            "Asia/Singapore     (UTC+8)",
            "Asia/Tokyo         (UTC+9, Japan)",
            "Asia/Seoul         (UTC+9, Korea)",
            "Asia/Kolkata       (UTC+5:30, India)",
            "Asia/Dubai         (UTC+4)",
            "Europe/Berlin      (UTC+1/+2, Germany)",
            "Europe/Paris       (UTC+1/+2, France)",
            "Europe/London      (UTC+0/+1, UK)",
            "Europe/Moscow      (UTC+3)",
            "America/New_York   (UTC-5/-4, US East)",
            "America/Chicago    (UTC-6/-5, US Central)",
            "America/Denver     (UTC-7/-6, US Mountain)",
            "America/Los_Angeles (UTC-8/-7, US West)",
            "America/Sao_Paulo  (UTC-3, Brazil)",
            "Australia/Sydney   (UTC+10/+11)",
        ]
        # Find current value in list
        current_tz = self.data.get("timezone", "Asia/Shanghai")
        default_label = next(
            (o for o in TZ_OPTIONS if o.startswith(current_tz)),
            "Asia/Shanghai      (UTC+8, China)"
        )
        self.tz_var = ctk.StringVar(value=default_label)
        self.entry_timezone = ctk.CTkOptionMenu(
            tz_inner, variable=self.tz_var, values=TZ_OPTIONS,
            fg_color=COLORS["bg"], button_color=COLORS["border"],
            button_hover_color=COLORS["text_muted"],
            text_color=COLORS["text"], width=320, height=32,
            dropdown_fg_color=COLORS["panel"],
            dropdown_hover_color=COLORS["bg"],
            dropdown_text_color=COLORS["text"],
        )
        self.entry_timezone.pack(side="left", fill="x", expand=True)

        # Telegram
        tg_box = ctk.CTkFrame(scroll, fg_color=COLORS["panel"], corner_radius=10,
                              border_width=1, border_color=COLORS["border"])
        tg_box.pack(fill="x", pady=(0, 14))

        tg_head = ctk.CTkFrame(tg_box, fg_color="transparent")
        tg_head.pack(fill="x", padx=20, pady=(14, 4))

        ctk.CTkLabel(
            tg_head, text="[telegram]  Telegram Notifications",
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            text_color=COLORS["text"]
        ).pack(side="left")

        self.tg_var = ctk.BooleanVar(value=self.data["use_telegram"])
        ctk.CTkSwitch(
            tg_head, text="", variable=self.tg_var,
            progress_color=COLORS["balanced"],
            button_color=COLORS["text"], width=46,
            command=self._toggle_tg_fields
        ).pack(side="right")

        ctk.CTkLabel(
            tg_box,
            text="Bot token from @BotFather, chat ID from @userinfobot",
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_muted"], anchor="w", wraplength=640
        ).pack(fill="x", padx=20, pady=(0, 10))

        self.tg_fields_box = ctk.CTkFrame(tg_box, fg_color="transparent")
        self.tg_fields_box.pack(fill="x", padx=20, pady=(0, 14))

        self.entry_tg_token   = self._labeled_entry(self.tg_fields_box, "Bot Token", self.data["telegram_token"], True, compact=True)
        self.entry_tg_chat_id = self._labeled_entry(self.tg_fields_box, "Chat ID",   self.data["telegram_chat_id"], False, compact=True)

        # CryptoPanic
        cp_box = ctk.CTkFrame(scroll, fg_color=COLORS["panel"], corner_radius=10,
                              border_width=1, border_color=COLORS["border"])
        cp_box.pack(fill="x", pady=(0, 14))

        cp_head = ctk.CTkFrame(cp_box, fg_color="transparent")
        cp_head.pack(fill="x", padx=20, pady=(14, 4))

        ctk.CTkLabel(
            cp_head, text="[news]  CryptoPanic API",
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            text_color=COLORS["text"]
        ).pack(side="left")

        self.cp_var = ctk.BooleanVar(value=self.data["use_cryptopanic"])
        ctk.CTkSwitch(
            cp_head, text="", variable=self.cp_var,
            progress_color=COLORS["aggressive"],
            button_color=COLORS["text"], width=46,
            command=self._toggle_cp_fields
        ).pack(side="right")

        ctk.CTkLabel(
            cp_box,
            text="Free API key at cryptopanic.com -> Account -> API. Better news quality than RSS feeds.",
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_muted"], anchor="w", wraplength=640
        ).pack(fill="x", padx=20, pady=(0, 10))

        self.cp_fields_box = ctk.CTkFrame(cp_box, fg_color="transparent")
        self.cp_fields_box.pack(fill="x", padx=20, pady=(0, 14))

        self.entry_cp_token = self._labeled_entry(self.cp_fields_box, "API Token", self.data["cryptopanic_token"], True, compact=True)

        # CoinMarketCap API
        cmc_box = ctk.CTkFrame(scroll, fg_color=COLORS["panel"], corner_radius=10,
                                border_width=1, border_color=COLORS["border"])
        cmc_box.pack(fill="x", pady=(0, 14))
        cmc_head = ctk.CTkFrame(cmc_box, fg_color="transparent")
        cmc_head.pack(fill="x", padx=20, pady=(14, 4))
        ctk.CTkLabel(
            cmc_head, text="[data]  CoinMarketCap API (Fear & Greed)",
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            text_color=COLORS["text"]
        ).pack(side="left")
        ctk.CTkLabel(
            cmc_box,
            text="Free API key at coinmarketcap.com/api/ - used as the primary\n"
                 "source for Fear & Greed Index. Without a key, the bot falls\n"
                 "back to the unofficial endpoint (works but less reliable).",
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_muted"], anchor="w", wraplength=640
        ).pack(fill="x", padx=20, pady=(0, 10))
        cmc_fields_box = ctk.CTkFrame(cmc_box, fg_color="transparent")
        cmc_fields_box.pack(fill="x", padx=20, pady=(0, 14))
        self.entry_cmc_key = self._labeled_entry(
            cmc_fields_box, "API Key", self.data.get("cmc_api_key", ""),
            True, compact=True
        )

        # Connection Test
        test_box = ctk.CTkFrame(scroll, fg_color=COLORS["panel"], corner_radius=10,
                                  border_width=1, border_color=COLORS["border"])
        test_box.pack(fill="x", pady=(0, 4))

        test_head = ctk.CTkFrame(test_box, fg_color="transparent")
        test_head.pack(fill="x", padx=20, pady=(14, 6))

        ctk.CTkLabel(
            test_head, text="[link]  Connection Test",
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            text_color=COLORS["text"]
        ).pack(side="left")

        ctk.CTkButton(
            test_head, text="Run Test",
            width=130, height=30, corner_radius=6,
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            fg_color=COLORS["purple"], hover_color=COLORS["purple_dim"],
            text_color="#ffffff",
            command=self._run_connection_test
        ).pack(side="right")

        self.test_result = ctk.CTkLabel(
            test_box, text="Click 'Run Test' to verify the connection",
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            text_color=COLORS["text_muted"], anchor="w", wraplength=640, justify="left"
        )
        self.test_result.pack(fill="x", padx=20, pady=(0, 14))

        self._toggle_tg_fields()
        self._toggle_cp_fields()
        self.skip_lbl.configure(text="Both services are optional")

    def _toggle_tg_fields(self):
        if self.tg_var.get():
            self.tg_fields_box.pack(fill="x", padx=20, pady=(0, 14))
        else:
            self.tg_fields_box.pack_forget()

    def _toggle_cp_fields(self):
        if self.cp_var.get():
            self.cp_fields_box.pack(fill="x", padx=20, pady=(0, 14))
        else:
            self.cp_fields_box.pack_forget()

    # -- Helper Widgets --------------------------------------------------------

    def _heading(self, parent, title, subtitle):
        ctk.CTkLabel(
            parent, text=title,
            font=ctk.CTkFont(FONT_BODY, 22, "bold"),
            text_color=COLORS["text"], anchor="w"
        ).pack(fill="x")
        ctk.CTkLabel(
            parent, text=subtitle,
            font=ctk.CTkFont(FONT_BODY, 13, "bold"),
            text_color=COLORS["text_muted"], anchor="w", wraplength=640, justify="left"
        ).pack(fill="x", pady=(6, 0))

    def _labeled_entry(self, parent, label, value, hide=False, compact=False):
        wrap = ctk.CTkFrame(parent, fg_color="transparent")
        wrap.pack(fill="x", pady=(0 if compact else 6, 8))

        ctk.CTkLabel(
            wrap, text=label,
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            text_color=COLORS["text_dim"], anchor="w"
        ).pack(fill="x")

        entry = ctk.CTkEntry(
            wrap, height=36, corner_radius=6,
            font=ctk.CTkFont(self.mono_font, 13),
            fg_color=COLORS["input_bg"],
            border_color=COLORS["border"], border_width=1,
            text_color=COLORS["text"], show="*" if hide else ""
        )
        entry.pack(fill="x", pady=(4, 0))
        entry.insert(0, value or "")
        return entry

    # -- Validation ------------------------------------------------------------

    def _validate_step(self, n):
        if n == 1:
            return True

        if n == 2:
            self.data["api_key"]    = self.entry_key.get().strip()
            self.data["api_secret"] = self.entry_secret.get().strip()
            if self.entry_pass:
                self.data["passphrase"] = self.entry_pass.get().strip()

            if not self.data["api_key"] or not self.data["api_secret"]:
                self._show_error("API key and secret are required")
                return False

            ex = EXCHANGES[self.data["exchange"]]
            if ex["passphrase"] and not self.data["passphrase"]:
                self._show_error("Passphrase required for this exchange")
                return False
            return True

        if n == 3:
            self.data["use_proxy"] = self.proxy_var.get()
            if self.data["use_proxy"]:
                self.data["proxy_host"] = self.entry_proxy_host.get().strip()
                self.data["proxy_port"] = self.entry_proxy_port.get().strip()
                if not self.data["proxy_host"] or not self.data["proxy_port"]:
                    self._show_error("Proxy host and port required")
                    return False
            return True

        if n == 4:
            self.data["use_telegram"] = self.tg_var.get()
            if self.data["use_telegram"]:
                self.data["telegram_token"]   = self.entry_tg_token.get().strip()
                self.data["telegram_chat_id"] = self.entry_tg_chat_id.get().strip()
            self.data["use_cryptopanic"] = self.cp_var.get()
            if self.data["use_cryptopanic"]:
                self.data["cryptopanic_token"] = self.entry_cp_token.get().strip()
            cmc = getattr(self, "entry_cmc_key", None)
            self.data["cmc_api_key"] = cmc.get().strip() if cmc else ""
            # Timezone: dropdown label looks like "Asia/Shanghai      (UTC+8, China)"
            # - strip everything after first whitespace to get the IANA name.
            tz_label = self.tz_var.get().strip() if hasattr(self, "tz_var") else "UTC"
            self.data["timezone"] = tz_label.split()[0] if tz_label else "UTC"
            return True

        return True

    def _show_error(self, msg):
        if hasattr(self, "_toast") and self._toast.winfo_exists():
            self._toast.destroy()
        self._toast = ctk.CTkLabel(
            self, text=f"WARN  {msg}",
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            text_color=COLORS["danger"], fg_color="#1a0e10",
            corner_radius=6
        )
        self._toast.place(relx=0.5, rely=0.85, anchor="center", relwidth=0.7, height=32)
        self.after(3000, lambda: self._toast.destroy() if self._toast.winfo_exists() else None)

    # -- Connection Test -------------------------------------------------------

    def _run_connection_test(self):
        self.test_result.configure(
            text="...  Testing connection...",
            text_color=COLORS["text_dim"]
        )
        self.update()

        def _test():
            try:
                if not self.data["api_key"] or not self.data["api_secret"]:
                    self.after(0, lambda: self.test_result.configure(
                        text="X  API key or secret missing - check Step 2",
                        text_color=COLORS["danger"]
                    ))
                    return

                ex_info = EXCHANGES[self.data["exchange"]]
                if ex_info["passphrase"] and not self.data["passphrase"]:
                    self.after(0, lambda: self.test_result.configure(
                        text="X  Passphrase missing - check Step 2",
                        text_color=COLORS["danger"]
                    ))
                    return

                try:
                    import ccxt
                except ImportError:
                    self.after(0, lambda: self.test_result.configure(
                        text="WARN  ccxt not installed. Run: pip install -r requirements.txt",
                        text_color=COLORS["warning"]
                    ))
                    return

                exchange_class = getattr(ccxt, self.data["exchange"])
                opts = {
                    "apiKey":          self.data["api_key"],
                    "secret":          self.data["api_secret"],
                    "enableRateLimit": True,
                    "timeout":         15000,
                }
                if ex_info["passphrase"]:
                    opts["password"] = self.data["passphrase"]

                if self.data.get("use_proxy"):
                    proxy_url = f"http://{self.data['proxy_host']}:{self.data['proxy_port']}"
                    opts["proxies"] = {"http": proxy_url, "https": proxy_url}

                ex = exchange_class(opts)
                ex.load_markets()
                bal = ex.fetch_balance()

                usdt = bal.get("USDT", {}).get("free", 0)
                msg = (f"OK  Connection successful  -  "
                       f"USDT balance: {usdt:.2f}  -  "
                       f"{len(ex.markets)} markets available")
                self.after(0, lambda m=msg: self.test_result.configure(
                    text=m, text_color=COLORS["success"]
                ))

            except Exception as e:
                err_msg = str(e)[:200]
                self.after(0, lambda em=err_msg: self.test_result.configure(
                    text=f"X  Test failed: {em}",
                    text_color=COLORS["danger"]
                ))

        threading.Thread(target=_test, daemon=True).start()

    # -- Finish ----------------------------------------------------------------

    def _finish(self):
        try:
            self._write_env_file()
            self._show_success_dialog()
        except Exception as e:
            self._show_error(f"Could not write .env: {e}")

    def _write_env_file(self):
        def _env_line(key: str, value) -> str:
            clean = str(value or "").strip().replace("\r", "").replace("\n", "")
            return f"{key}={json.dumps(clean)}"

        lines = [
            "# Auto-generated by Obsidian Setup Wizard",
            f"# Created: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "# -- Exchange ---------------------------------------",
            _env_line("EXCHANGE", self.data["exchange"]),
            _env_line("API_KEY", self.data["api_key"]),
            _env_line("API_SECRET", self.data["api_secret"]),
        ]

        if EXCHANGES[self.data["exchange"]]["passphrase"]:
            lines.append(_env_line("API_PASSPHRASE", self.data["passphrase"]))

        lines += ["", "# -- Proxy ------------------------------------------"]
        lines.append(_env_line("USE_PROXY", "true" if self.data["use_proxy"] else "false"))
        if self.data["use_proxy"]:
            lines.append(_env_line("PROXY_HOST", self.data["proxy_host"]))
            lines.append(_env_line("PROXY_PORT", self.data["proxy_port"]))

        if self.data["use_telegram"]:
            lines += ["", "# -- Telegram ----------------------------------------"]
            lines.append(_env_line("TELEGRAM_TOKEN", self.data["telegram_token"]))
            lines.append(_env_line("TELEGRAM_CHAT_ID", self.data["telegram_chat_id"]))

        if self.data["use_cryptopanic"]:
            lines += ["", "# -- CryptoPanic API ---------------------------------"]
            lines.append(_env_line("CRYPTOPANIC_TOKEN", self.data["cryptopanic_token"]))

        if self.data.get("cmc_api_key"):
            lines += ["", "# -- CoinMarketCap API (Fear & Greed) ----------------"]
            lines.append(_env_line("CMC_API_KEY", self.data["cmc_api_key"]))

        # Performance defaults - written unconditionally so the bot
        # starts with a sane budget out-of-the-box without the user
        # needing to know this env-var exists.
        # 600 = Bitget's practical public-endpoint rate limit.
        # The three bots share this pool; raise to 900 if you have
        # Bitget VIP status and see "API budget exhausted" in logs.
        lines += [
            "",
            "# -- Performance -------------------------------------",
            "API_BUDGET_PER_MINUTE=600",
            _env_line("BOT_TIMEZONE", self.data.get("timezone", "UTC")),
        ]

        lines.append("")
        # Schreibe .env IMMER in das Script-Verzeichnis, nicht ins CWD
        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path   = os.path.join(script_dir, ".env")
        with update_lifecycle_lock(script_dir, timeout=15.0):
            cleanup_stale_env_temps(script_dir)
            if os.path.lexists(env_path):
                raise RuntimeError(
                    ".env already exists; edit Env Settings instead of rerunning setup"
                )
            _write_new_private_text_file(env_path, "\n".join(lines))

    def _show_success_dialog(self):
        for w in self.content.winfo_children():
            w.destroy()

        success_box = ctk.CTkFrame(self.content, fg_color="transparent")
        success_box.pack(fill="both", expand=True)

        ctk.CTkLabel(
            success_box, text="OK",
            font=ctk.CTkFont(self.mono_font, 64, "bold"),
            text_color=COLORS["success"]
        ).pack(pady=(60, 16))

        ctk.CTkLabel(
            success_box, text="Setup complete!",
            font=ctk.CTkFont(FONT_BODY, 22, "bold"),
            text_color=COLORS["text"]
        ).pack()

        ctk.CTkLabel(
            success_box,
            text=f"Configuration for {EXCHANGES[self.data['exchange']]['label']} has been saved.\n"
                 f"You can now start the Trading Terminal.",
            font=ctk.CTkFont(FONT_BODY, 13, "bold"),
            text_color=COLORS["text_dim"], justify="center"
        ).pack(pady=(10, 30))

        ctk.CTkButton(
            success_box, text="Start Trading Terminal",
            width=240, height=42, corner_radius=8,
            font=ctk.CTkFont(FONT_BODY, 14, "bold"),
            fg_color=COLORS["purple"], hover_color=COLORS["purple_dim"],
            text_color="#ffffff",
            command=self._launch_terminal_and_close
        ).pack()

        ctk.CTkButton(
            success_box, text="Close",
            width=140, height=32, corner_radius=8,
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            fg_color="transparent", hover_color=COLORS["panel_hover"],
            text_color=COLORS["text_dim"],
            border_width=1, border_color=COLORS["border"],
            command=self.destroy
        ).pack(pady=12)

        self.btn_back.pack_forget()
        self.btn_next.pack_forget()
        self.skip_lbl.pack_forget()

    def _launch_terminal_and_close(self):
        import subprocess
        try:
            root = os.path.dirname(os.path.abspath(__file__))
            launcher = os.path.join(root, "launcher.pyw")
            kw = {}
            if sys.platform == "win32":
                kw["creationflags"] = subprocess.CREATE_NO_WINDOW
            with process_start_guard(root):
                subprocess.Popen(
                    [_get_pythonw_exe(), launcher],
                    cwd=root,
                    **kw
                )
        except Exception:
            pass
        self.destroy()


if __name__ == "__main__":
    # Working directory MUST be the script directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    app = SetupWizard()
    app.mainloop()
