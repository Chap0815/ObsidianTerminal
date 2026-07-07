"""
The Prompt-Editor modal.

Edits a per-bot ``prompts/*.txt`` file with a side panel listing the
placeholders the bot understands and a "required output format"
reminder. Validates that the saved prompt still contains the magic
``RESULT: `` line  without that the bot would silently fall back to
keyword analysis.
"""

from __future__ import annotations

import os
import sys

import customtkinter as ctk
import tkinter as tk

from launcher.config.settings import BOT_META, COLORS, FONT_BODY, PROJECT_ROOT
from launcher.ui.components.widgets import safe_geometry
from launcher.ui.theme import force_dark_titlebar


class PromptEditor(ctk.CTkToplevel):
    """Editor for the AI prompts.

    Shows the active prompt + the default for comparison, with a reset
    button and inline validation.
    """

    def __init__(self, parent, bot_name: str, accent: str, mono_font: str,
                 on_save_callback=None):
        super().__init__(parent)
        self.bot_name = bot_name
        self.accent = accent
        self.mono_font = mono_font
        self.on_save_callback = on_save_callback

        meta = BOT_META[bot_name]

        # Robust path resolution  tries several candidates:
        #   1) PROJECT_ROOT-relative (the new canonical place)
        #   2) ``__file__``-relative (compat with legacy layouts)
        #   3) ``sys.argv[0]``-relative
        #   4) cwd-relative
        # ...walking until a ``prompts/`` directory turns up.
        prompt_rel  = meta["prompt"]
        default_rel = meta["prompt_default"]

        self._tried_paths: list[str] = []  # diagnostics

        def _try_resolve(rel: str) -> str:
            candidates: list[str] = []

            # 1) PROJECT_ROOT-relative (preferred)
            candidates.append(os.path.join(PROJECT_ROOT, rel))

            # 2) __file__-relative (and its parent)
            try:
                here = os.path.dirname(os.path.abspath(__file__))
                candidates.append(os.path.join(here, rel))
                candidates.append(os.path.join(os.path.dirname(here), rel))
            except Exception:
                pass

            # 3) sys.argv[0]-relative
            try:
                if sys.argv and sys.argv[0]:
                    argv_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
                    candidates.append(os.path.join(argv_dir, rel))
            except Exception:
                pass

            # 4) cwd-relative
            try:
                candidates.append(os.path.join(os.getcwd(), rel))
            except Exception:
                pass

            # The bare path (if it's already absolute)
            candidates.append(rel)

            # Dedupe and probe
            seen: set[str] = set()
            for c in candidates:
                try:
                    norm = os.path.abspath(c)
                except Exception:
                    norm = c
                if norm in seen:
                    continue
                seen.add(norm)
                self._tried_paths.append(norm)
                if os.path.exists(norm):
                    return norm

            # Nothing found  return the first candidate (used for later saves)
            return candidates[0] if candidates else rel

        self.prompt_path  = _try_resolve(prompt_rel)
        self.default_path = _try_resolve(default_rel)

        self.title(f"Prompt Editor  {meta['label']}")
        self.configure(fg_color=COLORS["bg"])
        self.transient(parent)
        self.grab_set()
        # Clamp+center to usable screen; cap minsize so a short screen can still
        # reach the footer buttons (the editor body already scrolls).
        safe_geometry(self, 980, 720, parent=parent)
        try:
            self.minsize(min(800, self.winfo_screenwidth() - 80),
                         min(600, self.winfo_screenheight() - 80))
        except Exception:
            pass

        self._dirty = False
        self._build_ui()
        self._load_prompt()

    #  Layout 

    def _build_ui(self):
        #  Header 
        head = ctk.CTkFrame(self, fg_color=COLORS["panel"], height=64, corner_radius=0)
        head.pack(fill="x")
        head.pack_propagate(False)
        ctk.CTkFrame(self, fg_color=COLORS["border"], height=1).pack(fill="x")

        inner = ctk.CTkFrame(head, fg_color="transparent")
        inner.pack(fill="both", expand=True, padx=20, pady=12)

        title_box = ctk.CTkFrame(inner, fg_color="transparent")
        title_box.pack(side="left")

        ctk.CTkLabel(title_box, text=" Prompt Editor",
                      font=ctk.CTkFont(FONT_BODY, 16, "bold"),
                      text_color=COLORS["text"]
                      ).pack(anchor="w")

        ctk.CTkLabel(title_box,
                      text=f"{BOT_META[self.bot_name]['label']}  "
                           f"{BOT_META[self.bot_name]['subtitle']}",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=self.accent
                      ).pack(anchor="w", pady=(2, 0))

        self.status_lbl = ctk.CTkLabel(inner, text="",
                                        font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                                        text_color=COLORS["warning"])
        self.status_lbl.pack(side="right")

        #  Body  editor + sidebar with placeholders 
        body = ctk.CTkFrame(self, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=20, pady=12)
        body.grid_columnconfigure(0, weight=3)
        body.grid_columnconfigure(1, weight=1, minsize=240)
        body.grid_rowconfigure(0, weight=1)

        # Editor (left)
        editor_wrap = ctk.CTkFrame(body, fg_color=COLORS["panel"], corner_radius=8,
                                     border_width=1, border_color=COLORS["border"])
        editor_wrap.grid(row=0, column=0, sticky="nsew", padx=(0, 12))

        editor_head = ctk.CTkFrame(editor_wrap, fg_color="transparent", height=36)
        editor_head.pack(fill="x", padx=12, pady=(10, 0))
        editor_head.pack_propagate(False)

        ctk.CTkLabel(editor_head, text=f"prompts/{os.path.basename(self.prompt_path)}",
                      font=ctk.CTkFont(self.mono_font, 11, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(side="left")

        self.char_count_lbl = ctk.CTkLabel(editor_head, text="",
                                            font=ctk.CTkFont(self.mono_font, 11, "bold"),
                                            text_color=COLORS["text_muted"])
        self.char_count_lbl.pack(side="right")

        text_box_wrap = ctk.CTkFrame(editor_wrap, fg_color=COLORS["bg"], corner_radius=6)
        text_box_wrap.pack(fill="both", expand=True, padx=12, pady=12)

        self.editor = tk.Text(
            text_box_wrap,
            bg=COLORS["bg"], fg=COLORS["text"],
            font=(self.mono_font, 11),
            insertbackground=COLORS["text"],
            highlightthickness=0, bd=0, relief="flat",
            padx=12, pady=10, wrap="word",
            selectbackground=COLORS["border"],
            undo=True
        )
        self.editor.pack(side="left", fill="both", expand=True)
        self.editor.bind("<<Modified>>", self._on_modified)

        sb = tk.Scrollbar(text_box_wrap, command=self.editor.yview,
                          bg=COLORS["panel"], troughcolor=COLORS["bg"],
                          activebackground=COLORS["text_muted"],
                          borderwidth=0, highlightthickness=0)
        sb.pack(side="right", fill="y")
        self.editor.config(yscrollcommand=sb.set)

        #  Sidebar right: placeholder list + help 
        side = ctk.CTkFrame(body, fg_color=COLORS["panel"], corner_radius=8,
                              border_width=1, border_color=COLORS["border"])
        side.grid(row=0, column=1, sticky="nsew")

        ctk.CTkLabel(side, text="AVAILABLE PLACEHOLDERS",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(anchor="w", padx=14, pady=(14, 6))

        placeholders = self._get_placeholders()
        ph_scroll = ctk.CTkScrollableFrame(side, fg_color="transparent",
                                             scrollbar_button_color=COLORS["border"])
        ph_scroll.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        for ph, desc in placeholders:
            row = ctk.CTkFrame(ph_scroll, fg_color="transparent")
            row.pack(fill="x", pady=2)
            ctk.CTkLabel(row, text="{" + ph + "}",
                          font=ctk.CTkFont(self.mono_font, 11, "bold"),
                          text_color=self.accent, anchor="w", cursor="hand2"
                          ).pack(anchor="w")
            ctk.CTkLabel(row, text=desc,
                          font=ctk.CTkFont(FONT_BODY, 10),
                          text_color=COLORS["text_muted"],
                          anchor="w", justify="left", wraplength=210
                          ).pack(anchor="w")

        # Required output format reminder
        warn = ctk.CTkFrame(side, fg_color=COLORS["bg"], corner_radius=6,
                              border_width=1, border_color=COLORS["warning"])
        warn.pack(fill="x", padx=12, pady=(6, 12))

        ctk.CTkLabel(warn, text=" Required output",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["warning"], anchor="w"
                      ).pack(fill="x", padx=10, pady=(8, 2))

        if BOT_META[self.bot_name]["is_futures"]:
            req_text = ("Last line MUST be exactly:\n"
                        "RESULT: LONG\nRESULT: SHORT\nRESULT: WAIT")
        else:
            req_text = ("Last line MUST be exactly:\n"
                        "RESULT: BUY\nRESULT: WAIT")

        ctk.CTkLabel(warn, text=req_text,
                      font=ctk.CTkFont(self.mono_font, 10, "bold"),
                      text_color=COLORS["text_dim"], anchor="w", justify="left"
                      ).pack(fill="x", padx=10, pady=(0, 8))

        #  Footer with actions 
        ctk.CTkFrame(self, fg_color=COLORS["border"], height=1).pack(fill="x")
        footer = ctk.CTkFrame(self, fg_color=COLORS["panel"], height=64, corner_radius=0)
        footer.pack(fill="x")
        footer.pack_propagate(False)

        f_inner = ctk.CTkFrame(footer, fg_color="transparent")
        f_inner.pack(fill="both", expand=True, padx=20, pady=14)

        ctk.CTkButton(f_inner, text=" Reset to default",
                       width=160, height=36, corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"],
                       border_width=1, border_color=COLORS["border"],
                       command=self._reset_to_default
                       ).pack(side="left")

        ctk.CTkLabel(f_inner,
                      text="Speichern berschreibt die Datei  Bot muss danach neugestartet werden",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(side="left", padx=(16, 0))

        ctk.CTkButton(f_inner, text="Cancel",
                       width=100, height=36, corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"],
                       border_width=1, border_color=COLORS["border"],
                       command=self.destroy
                       ).pack(side="right", padx=(8, 0))

        ctk.CTkButton(f_inner, text=" Save Prompt",
                       width=150, height=36, corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       fg_color=self.accent,
                       hover_color=COLORS["panel_hover"],
                       text_color="#ffffff",
                       command=self._save
                       ).pack(side="right")

    #  Data 

    def _get_placeholders(self) -> list:
        base = [
            ("symbol",  "Coin ticker (BTC, ETH, )"),
            ("change",   "24h change in % (numeric)"),
            ("rsi_15m",  "15-minute RSI value"),
            ("rsi_1h",   "1-hour RSI value"),
            ("rsi_4h",   "4-hour RSI value"),
            ("news",     "Joined news headlines"),
            ("regime",   "Market regime: BULL / NEUTRAL / BEAR"),
            ("btc_24h",  "BTC 24h change in %"),
            ("btc_7d",   "BTC 7-day change in %"),
        ]
        if self.bot_name == "TREND":
            base += [
                ("fg",       "Fear & Greed index (0-100)"),
                ("fg_label", "Fear & Greed text label"),
            ]
        if BOT_META[self.bot_name]["is_futures"]:
            base += [
                ("price",              "Current mark price"),
                ("leverage",           "Configured leverage (1-10)"),
                ("funding_rate",       "Funding rate in % per 8h"),
                ("open_interest_usdt", "Open Interest (Mio USDT)"),
                ("oi_change",          "OI 24h change in %"),
                ("liq_long_pct",       "Approx liq distance for LONG"),
                ("liq_short_pct",      "Approx liq distance for SHORT"),
                ("fg",                 "Fear & Greed index"),
                ("fg_label",           "Fear & Greed label"),
            ]
        return base

    def _load_prompt(self):
        path = self.prompt_path
        loaded_from = None
        loaded_path = None
        content = ""

        # Try the active prompt first
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    content = f.read()
                loaded_from = "active"
                loaded_path = path
            except Exception:
                content = ""

        # Fall back to the default
        if not content and os.path.exists(self.default_path):
            try:
                with open(self.default_path, encoding="utf-8") as f:
                    content = f.read()
                loaded_from = "default"
                loaded_path = self.default_path
            except Exception:
                content = ""

        # Last resort: hardcoded fallback if both files are missing
        if not content:
            content = self._get_hardcoded_fallback()
            loaded_from = "hardcoded"

        self.editor.delete("1.0", "end")
        self.editor.insert("1.0", content)
        self.editor.edit_reset()
        self._update_char_count()
        self._dirty = False

        # Status: show where it was loaded from so the user knows
        if loaded_from == "active":
            short = self._short_path(loaded_path)
            self.status_lbl.configure(text=f" Loaded from {short}",
                                        text_color=COLORS["success"])
        elif loaded_from == "default":
            short = self._short_path(loaded_path)
            self.status_lbl.configure(text=f" Loaded default from {short}",
                                        text_color=COLORS["balanced"])
        elif loaded_from == "hardcoded":
            self.status_lbl.configure(
                text=" Prompt-Dateien nicht gefunden  Click here to see searched paths",
                text_color=COLORS["warning"],
                cursor="hand2"
            )
            self.status_lbl.bind("<Button-1>", lambda e: self._show_path_diagnostics())

        self.editor.edit_modified(False)

    def _short_path(self, p: str) -> str:
        """Shorten the path for the status label."""
        try:
            cwd = os.getcwd()
            if p.startswith(cwd):
                return "." + p[len(cwd):]
            home = os.path.expanduser("~")
            if p.startswith(home):
                return "~" + p[len(home):]
        except Exception:
            pass
        return p

    def _show_path_diagnostics(self):
        """Popup with all the paths we tried  useful for debugging."""
        dlg = ctk.CTkToplevel(self)
        dlg.title("Prompt File Search  Diagnostics")
        dlg.configure(fg_color=COLORS["panel"])
        dlg.grab_set()
        dlg.transient(self)
        force_dark_titlebar(dlg)
        safe_geometry(dlg, 700, 440, parent=self)

        ctk.CTkLabel(dlg, text="Prompt-Dateien nicht gefunden",
                      font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                      text_color=COLORS["warning"]
                      ).pack(pady=(16, 4), padx=20, anchor="w")

        ctk.CTkLabel(
            dlg,
            text="Folgende Pfade wurden geprft (aber keiner existiert).\n"
                  "Erwartet werden die Dateien:  "
                  f"prompts/{os.path.basename(self.prompt_path)}  "
                  f"oder  prompts/{os.path.basename(self.default_path)}",
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_muted"], justify="left", wraplength=640
        ).pack(pady=(0, 8), padx=20, anchor="w")

        list_frame = ctk.CTkFrame(dlg, fg_color=COLORS["bg"], corner_radius=8,
                                     border_width=1, border_color=COLORS["border"])
        list_frame.pack(fill="both", expand=True, padx=20, pady=(0, 12))

        text = ctk.CTkTextbox(
            list_frame, fg_color=COLORS["bg"],
            text_color=COLORS["text_dim"],
            font=ctk.CTkFont(self.mono_font, 10),
            wrap="none", corner_radius=8, border_width=0
        )
        text.pack(fill="both", expand=True, padx=2, pady=2)

        paths = getattr(self, "_tried_paths", [])
        if paths:
            for p in paths:
                text.insert("end", f" {p}\n")
        else:
            text.insert("end", "(no diagnostic paths recorded)\n")

        text.insert("end", f"\n\nCurrent working directory:\n  {os.getcwd()}\n")
        text.insert("end", f"\nProject root:\n  {PROJECT_ROOT}\n")
        text.insert("end", f"\nsys.argv[0]:\n  {sys.argv[0] if sys.argv else '(none)'}\n")
        text.configure(state="disabled")

        #  Action buttons 
        btns = ctk.CTkFrame(dlg, fg_color="transparent")
        btns.pack(side="bottom", fill="x", padx=20, pady=(0, 16))

        def _create_now():
            """Self-heal: write the default prompts to disk and reload."""
            try:
                from news import prompts_defaults  # type: ignore
                prompts_defaults.write_all_defaults(
                    os.path.join(PROJECT_ROOT, "prompts")
                )
                # After writing: re-resolve paths + reload the prompt
                meta = BOT_META[self.bot_name]
                self._tried_paths = []
                self.prompt_path  = os.path.join(PROJECT_ROOT, meta["prompt"])
                self.default_path = os.path.join(PROJECT_ROOT, meta["prompt_default"])
                dlg.destroy()
                # Remove the diagnostic click binding
                try:
                    self.status_lbl.unbind("<Button-1>")
                    self.status_lbl.configure(cursor="")
                except Exception:
                    pass
                self._load_prompt()
            except ImportError:
                ctk.CTkLabel(dlg, text=" prompts_defaults.py not found  cannot self-heal",
                              font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                              text_color=COLORS["danger"]
                              ).pack(side="bottom", pady=(0, 8))
            except Exception as e:
                ctk.CTkLabel(dlg, text=f" Create failed: {e}",
                              font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                              text_color=COLORS["danger"]
                              ).pack(side="bottom", pady=(0, 8))

        ctk.CTkButton(btns, text=" Create default files now",
                       height=32, corner_radius=6, width=220,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color=COLORS["success"], hover_color="#0d9b6c",
                       text_color="#ffffff",
                       command=_create_now
                       ).pack(side="right")

        ctk.CTkButton(btns, text="Close",
                       height=32, corner_radius=6, width=100,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"],
                       border_width=1, border_color=COLORS["border"],
                       command=dlg.destroy
                       ).pack(side="right", padx=(0, 8))

    def _get_hardcoded_fallback(self) -> str:
        """Minimal built-in prompt for when the file is missing."""
        is_futures = BOT_META[self.bot_name]["is_futures"]
        if is_futures:
            return (
                "You are a crypto FUTURES trader. Decide LONG, SHORT, or WAIT.\n\n"
                "Symbol: {symbol}\nPrice: {price:.6f}\n24h: {change:+.2f}%\n"
                "RSI 15m/1h/4h: {rsi_15m:.1f}/{rsi_1h:.1f}/{rsi_4h:.1f}\n"
                "Leverage: {leverage}x\nFunding: {funding_rate:+.4f}%\n"
                "Market: {regime}, BTC: {btc_24h:+.2f}%\nNews: {news}\n\n"
                "Provide a brief analysis. End with EXACTLY ONE line:\n"
                "RESULT: LONG\nRESULT: SHORT\nRESULT: WAIT"
            )
        return (
            "You are a crypto SPOT trader. Decide BUY or WAIT.\n\n"
            "Symbol: {symbol}\n24h: +{change}%\n"
            "RSI 15m/1h/4h: {rsi_15m:.1f}/{rsi_1h:.1f}/{rsi_4h:.1f}\n"
            "Market: {regime}, BTC: {btc_24h:+.2f}%\nNews: {news}\n\n"
            "Provide a brief analysis. End with EXACTLY ONE line:\n"
            "RESULT: BUY\nRESULT: WAIT"
        )

    #  Editor events 

    def _on_modified(self, *args):
        if self.editor.edit_modified():
            self._dirty = True
            self.status_lbl.configure(text=" Unsaved changes")
            self._update_char_count()
            self.editor.edit_modified(False)

    def _update_char_count(self):
        text = self.editor.get("1.0", "end-1c")
        chars = len(text)
        lines = text.count("\n") + 1 if text else 0
        self.char_count_lbl.configure(text=f"{lines} lines  {chars} chars")

    def _validate(self, content: str) -> tuple[bool, str]:
        """Make sure the prompt still has a parseable output format.

        Accepts two formats:
        1. Legacy free-text:  must mention RESULT: LONG/SHORT/WAIT (futures)
                              or RESULT: BUY/WAIT (spot)
        2. New JSON format:   must mention "direction" field in output schema.
                              Pure JSON prompts don't need a RESULT line 
                              the bot reads direction directly from the JSON.
        """
        is_futures = BOT_META[self.bot_name]["is_futures"]
        upper = content.upper()

        # New JSON format: prompt specifies "direction" as output field
        # Heuristic: contains "direction" AND one of the valid values
        is_json_format = (
            '"DIRECTION"' in upper and
            ('"LONG"' in upper or '"SHORT"' in upper or '"WAIT"' in upper
             or '"BUY"' in upper)
        )
        if is_json_format:
            return True, ""

        # Legacy free-text format
        if is_futures:
            required_any = ["RESULT: LONG", "RESULT: SHORT", "RESULT: WAIT"]
            if not any(r in upper for r in required_any):
                return (False,
                        "Prompt must mention RESULT: LONG / SHORT / WAIT "
                        "instructions (or use JSON format with \"direction\" field)")
        else:
            if "RESULT: BUY" not in upper or "RESULT: WAIT" not in upper:
                return (False,
                        "Prompt must mention both RESULT: BUY and RESULT: WAIT "
                        "(or use JSON format with \"direction\" field)")
        return True, ""

    #  Actions 

    def _save(self):
        content = self.editor.get("1.0", "end-1c")
        ok, err = self._validate(content)
        if not ok:
            self._show_toast(err, color=COLORS["danger"])
            return

        # Ensure the prompts/ directory exists. Without this the write
        # fails silently on a fresh install where prompts/ doesn't exist
        # yet (os.makedirs needs the DIRECTORY, not the file path).
        prompt_dir = os.path.dirname(self.prompt_path)
        if prompt_dir:
            os.makedirs(prompt_dir, exist_ok=True)
        try:
            tmp = self.prompt_path + f".tmp.{os.getpid()}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                try: os.fsync(f.fileno())
                except Exception: pass
            os.replace(tmp, self.prompt_path)
            self._dirty = False
            self.status_lbl.configure(
                text=f" Saved  {os.path.basename(self.prompt_path)}",
                text_color=COLORS["success"])
            if self.on_save_callback:
                self.on_save_callback(self.bot_name)
            # Don't auto-destroy  leave the window open so the user sees the
            # save confirmation and closes it manually.
        except Exception as e:
            self._show_toast(f"Save failed: {e}", color=COLORS["danger"])

    def _reset_to_default(self):
        if not os.path.exists(self.default_path):
            self._show_toast("No default file found", color=COLORS["danger"])
            return

        dlg = ctk.CTkToplevel(self)
        dlg.title("Reset prompt")
        dlg.configure(fg_color=COLORS["panel"])
        dlg.grab_set()
        dlg.transient(self)
        force_dark_titlebar(dlg)
        safe_geometry(dlg, 400, 180, parent=self)

        ctk.CTkLabel(dlg, text="Reset to default prompt?",
                      font=ctk.CTkFont(FONT_BODY, 13, "bold"),
                      text_color=COLORS["text"]
                      ).pack(pady=(20, 4))

        ctk.CTkLabel(dlg, text="Your current changes will be lost.",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_dim"]
                      ).pack(pady=(0, 16))

        row = ctk.CTkFrame(dlg, fg_color="transparent")
        row.pack()

        def _confirm():
            try:
                with open(self.default_path, encoding="utf-8") as f:
                    content = f.read()
                self.editor.delete("1.0", "end")
                self.editor.insert("1.0", content)
                self._dirty = True
                self.status_lbl.configure(text=" Reset (unsaved)",
                                            text_color=COLORS["warning"])
                self._update_char_count()
            except Exception:
                pass
            dlg.destroy()

        ctk.CTkButton(row, text="Reset", width=110, height=34,
                       fg_color=COLORS["warning"], hover_color="#d97706",
                       text_color="#ffffff", corner_radius=8,
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       command=_confirm
                       ).pack(side="left", padx=6)

        ctk.CTkButton(row, text="Cancel", width=110, height=34,
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"], corner_radius=8,
                       border_width=1, border_color=COLORS["border"],
                       font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                       command=dlg.destroy
                       ).pack(side="left", padx=6)

    def _show_toast(self, msg, color):
        if hasattr(self, "_toast") and self._toast.winfo_exists():
            self._toast.destroy()
        # CTk >= 5.2: 'height' must be passed to constructor, not .place()
        self._toast = ctk.CTkLabel(
            self, text=msg,
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=color, fg_color=COLORS["bg"], corner_radius=6,
            height=30,
        )
        self._toast.place(relx=0.5, rely=0.9, anchor="center", relwidth=0.7)
        self.after(2500, lambda: self._toast.destroy()
                    if self._toast.winfo_exists() else None)
