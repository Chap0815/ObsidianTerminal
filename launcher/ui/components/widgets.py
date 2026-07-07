"""
Re-usable widgets shared by the main layout and the bot cards.

* :class:`Tooltip` / :func:`attach_tooltip`  lightweight hover popups
* :class:`MiniBar`  sidebar progress-bar metric (CPU, RAM, GPU, VRAM)
* :class:`ParamRow`  /click-to-edit row used by each bot's parameter editor
* :class:`BadHoursRow`  comma-separated UTC-hour entry field for the
  risk-manager's ``bad_hours`` setting
"""

from __future__ import annotations

import customtkinter as ctk
import tkinter as tk
import tkinter.font as tkfont

from launcher.config.settings import COLORS, FONT_BODY, _safe_mono_font
from launcher.core.metrics_service import query_db


#  Resolution-safe geometry helper 
#
# ctk interprets geometry strings ("WxH+x+y") in *scaled* units: it multiplies
# them by the per-window scaling factor before handing them to Tk. winfo_screen*
# return *physical* pixels. To clamp/center correctly we convert the physical
# screen size into the same scaled units (divide by the window-scaling factor),
# do all math there, then emit a scaled geometry string ctk will re-scale back
# to physical. This avoids double-shrinking and works at any global scaling.

_SCREEN_MARGIN = 80   # physical px kept free (taskbar + window chrome)


def _window_scaling(win) -> float:
    """Effective ctk window-scaling factor for this toplevel (1.0 fallback)."""
    try:
        return float(ctk.ScalingTracker.get_window_scaling(win))
    except Exception:
        return 1.0


def safe_geometry(win, desired_w: int, desired_h: int,
                  parent=None, center: bool = True,
                  margin: int = _SCREEN_MARGIN) -> tuple[int, int]:
    """Clamp (desired_w, desired_h) to the usable screen and place ``win`` so it
    is fully on-screen. Sizes are given in scaled units (the same units passed to
    ``geometry("WxH")``). Returns the clamped (w, h) in scaled units.

    * Width/height are capped to (physical screen  margin), converted to scaled
      units so the cap is correct regardless of global widget scaling.
    * When ``center`` is True the window is centered over ``parent`` (or the
      screen if no parent), then the offset is clamped so no edge goes off-screen
      or negative.
    """
    try:
        win.update_idletasks()
    except Exception:
        pass

    scaling = _window_scaling(win) or 1.0
    try:
        phys_sw = win.winfo_screenwidth()
        phys_sh = win.winfo_screenheight()
    except Exception:
        phys_sw, phys_sh = 1920, 1080

    # Usable area in scaled units (geometry strings are scaled units).
    max_w = max(200, int((phys_sw - margin) / scaling))
    max_h = max(200, int((phys_sh - margin) / scaling))

    w = min(int(desired_w), max_w)
    h = min(int(desired_h), max_h)

    # Position in scaled units. ctk scales +x+y too, so work in scaled units and
    # only convert the parent's physical root coords down into scaled units.
    scaled_sw = phys_sw / scaling
    scaled_sh = phys_sh / scaling

    x = int((scaled_sw - w) / 2)
    y = int((scaled_sh - h) / 2)

    if center and parent is not None:
        try:
            parent.update_idletasks()
            px = parent.winfo_rootx() / scaling
            py = parent.winfo_rooty() / scaling
            pw = parent.winfo_width() / scaling
            ph = parent.winfo_height() / scaling
            if pw > 1 and ph > 1:
                x = int(px + (pw - w) / 2)
                y = int(py + (ph - h) / 2)
        except Exception:
            pass

    # Clamp on-screen (scaled units), never negative.
    x = max(0, min(x, int(scaled_sw - w)))
    y = max(0, min(y, int(scaled_sh - h)))

    try:
        win.geometry(f"{w}x{h}+{x}+{y}")
    except Exception:
        try:
            win.geometry(f"{w}x{h}")
        except Exception:
            pass
    return w, h


def clamp_offset(win, x: int, y: int, w: int, h: int,
                 margin: int = _SCREEN_MARGIN) -> tuple[int, int]:
    """Clamp a manual (+x+y) placement (scaled units) so a wh window stays fully
    on-screen and never at a negative offset. Returns clamped (x, y)."""
    scaling = _window_scaling(win) or 1.0
    try:
        scaled_sw = win.winfo_screenwidth() / scaling
        scaled_sh = win.winfo_screenheight() / scaling
    except Exception:
        scaled_sw, scaled_sh = 1920 / scaling, 1080 / scaling
    x = max(0, min(int(x), max(0, int(scaled_sw - w))))
    y = max(0, min(int(y), max(0, int(scaled_sh - h))))
    return x, y


#  Sparkline (mini PnL trend line in each bot card) 

class Sparkline(tk.Frame):
    """Kleine Verlaufslinie der kumulierten PnL ber die letzten Trades.

    Reine Anzeige  zeichnet eine Liste von Floats auf ein tk.Canvas.
    Farbe richtet sich nach dem letzten Wert (grn = im Plus, rot = im
    Minus). Wird vom Refresh-Loop via ``set_values()`` aktualisiert; die
    Daten kommen aus ``metrics_service.get_pnl_sparkline``.

    HINWEIS: erbt bewusst von ``tk.Frame`` (nicht ``ctk.CTkFrame``).
    Ein rohes ``tk.Canvas`` als Kind eines CTkFrame fhrt zu
    ``TypeError: unsupported operand 'int'+'str'`` beim Master-Setup,
    weil CTkFrame intern keinen sauberen Tk-Master fr klassische
    Tk-Widgets bereitstellt. tk.Frame umgeht das.
    """

    def __init__(self, parent, width=180, height=34):
        super().__init__(parent, bg=COLORS["panel_alt"],
                         highlightthickness=0, bd=0, height=height)
        self.pack_propagate(False)
        # WICHTIG: NICHT self._w / self._h verwenden  Tkinter benutzt
        # self._w intern als Widget-Pfad-String. berschreiben mit einem
        # int (width) fhrt zu "TypeError: 'int'+'str'" sobald ein Kind-
        # Widget (tk.Canvas) seinen Pfad aus master._w bildet. Eigene
        # Namen mit Prefix vermeiden die Kollision.
        self._spark_w = width
        self._spark_h = height
        self.canvas = tk.Canvas(self, height=height, bg=COLORS["panel_alt"],
                                 highlightthickness=0, bd=0)
        self.canvas.pack(fill="both", expand=True)
        self._values: list = []
        self.canvas.bind("<Configure>", lambda e: self._redraw())

    def set_values(self, values: list) -> None:
        """Neue Datenreihe setzen + neu zeichnen. ``values`` ist die
        kumulierte PnL-Kurve (lteste zuerst)."""
        self._values = list(values) if values else []
        self._redraw()

    def _redraw(self) -> None:
        c = self.canvas
        c.delete("all")
        vals = self._values
        try:
            w = c.winfo_width() or self._spark_w
            h = c.winfo_height() or self._spark_h
        except Exception:
            w, h = self._spark_w, self._spark_h
        if not vals or len(vals) < 2 or w < 4 or h < 4:
            # Platzhalter: dezente Mittellinie wenn keine Daten
            c.create_line(2, h / 2, w - 2, h / 2,
                          fill=COLORS["border"], width=1)
            return

        pad = 3
        vmin = min(vals)
        vmax = max(vals)
        span = (vmax - vmin) or 1.0
        n = len(vals)
        step_x = (w - 2 * pad) / (n - 1)

        def _x(i): return pad + i * step_x
        def _y(v): return h - pad - ((v - vmin) / span) * (h - 2 * pad)

        # Farbe nach letztem Wert (Endstand der Kurve)
        last = vals[-1]
        line_color = (COLORS["success"] if last > 0
                      else COLORS["danger"] if last < 0
                      else COLORS["text_dim"])

        # Nulllinie (falls 0 im Wertebereich liegt) dezent einzeichnen
        if vmin < 0 < vmax:
            zy = _y(0.0)
            c.create_line(pad, zy, w - pad, zy,
                          fill=COLORS["border"], width=1, dash=(2, 3))

        # Punkte der Kurve
        pts = []
        for i, v in enumerate(vals):
            pts.extend([_x(i), _y(v)])

        # KLARE Linie  keine Flchenfllung (die wirkte als "Schattenpftze").
        c.create_line(*pts, fill=line_color, width=2,
                      capstyle="round", joinstyle="round", smooth=False)

        # Endpunkt-Markierung (klein)
        c.create_oval(_x(n - 1) - 2, _y(last) - 2,
                      _x(n - 1) + 2, _y(last) + 2,
                      fill=line_color, outline="")

    @staticmethod
    def _tint(hex_color: str) -> str:
        """Dunkle, dezente Tnung einer Hex-Farbe fr die Flchenfllung
        unter der Sparkline (~22% Helligkeit Richtung Panel-Hintergrund)."""
        try:
            hc = hex_color.lstrip("#")
            r, g, b = int(hc[0:2], 16), int(hc[2:4], 16), int(hc[4:6], 16)
            f = 0.22
            r = int(r * f); g = int(g * f); b = int(b * f)
            return f"#{r:02x}{g:02x}{b:02x}"
        except Exception:
            return COLORS["panel"]


#  Tooltip 

class Tooltip:
    """Lightweight hover tooltip  appears after a short delay when the
    mouse rests on a widget. Uses a borderless ``Toplevel`` so it works
    for any widget type.
    """

    _active_tip: "Tooltip | None" = None   # only one tooltip visible at a time

    def __init__(self, widget, text: str, delay_ms: int = 450):
        self.widget = widget
        self.text = text
        self.delay = delay_ms
        self.tipwin: tk.Toplevel | None = None
        self._after_id = None

        widget.bind("<Enter>",  self._schedule, add="+")
        widget.bind("<Leave>",  self._hide,     add="+")
        widget.bind("<Button>", self._hide,     add="+")
        widget.bind("<Destroy>", self._on_destroy, add="+")

    def _schedule(self, _evt=None):
        self._cancel()
        try:
            self._after_id = self.widget.after(self.delay, self._show)
        except Exception:
            pass

    def _cancel(self):
        if self._after_id is not None:
            try:
                self.widget.after_cancel(self._after_id)
            except Exception:
                pass
            self._after_id = None

    def _show(self):
        # Close any previous tooltip
        if Tooltip._active_tip is not None and Tooltip._active_tip is not self:
            try: Tooltip._active_tip._hide()
            except Exception: pass

        if self.tipwin or not self.text:
            return
        try:
            x = self.widget.winfo_rootx() + 14
            y = self.widget.winfo_rooty() + self.widget.winfo_height() + 6
        except Exception:
            return

        tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        try:
            tw.attributes("-topmost", True)
        except Exception:
            pass
        try:
            tw.attributes("-alpha", 0.95)
        except Exception:
            pass
        tw.configure(bg=COLORS["border"])

        # Inner padding via frame for a rounded look
        inner = tk.Frame(tw, bg=COLORS["panel"], padx=10, pady=6)
        inner.pack(padx=1, pady=1)

        tk.Label(
            inner, text=self.text,
            font=(FONT_BODY, 10, "bold"),
            fg=COLORS["text"], bg=COLORS["panel"],
            justify="left", wraplength=320
        ).pack()

        # Clamp tooltip on-screen (tk.Toplevel here, no ctk scaling  unit=1).
        try:
            sw = tw.winfo_screenwidth()
            sh = tw.winfo_screenheight()
            tw.update_idletasks()
            tw_w = tw.winfo_reqwidth() or 200
            tw_h = tw.winfo_reqheight() or 60
            x = max(0, min(x, sw - tw_w))
            y = max(0, min(y, sh - tw_h))
        except Exception:
            pass
        tw.wm_geometry(f"+{x}+{y}")
        self.tipwin = tw
        Tooltip._active_tip = self

    def _hide(self, _evt=None):
        self._cancel()
        if self.tipwin is not None:
            try: self.tipwin.destroy()
            except Exception: pass
            self.tipwin = None
        if Tooltip._active_tip is self:
            Tooltip._active_tip = None

    def _on_destroy(self, _evt=None):
        self._hide()

    def update_text(self, new_text: str):
        self.text = new_text


def attach_tooltip(widget, text: str, delay_ms: int = 450) -> Tooltip:
    """Convenience helper that constructs a :class:`Tooltip`."""
    return Tooltip(widget, text, delay_ms)


#  MiniBar (sidebar system monitor) 

class MiniBar(ctk.CTkFrame):
    """Compact "label + value + progress bar" sidebar row."""

    def __init__(self, parent, label, color, width=160, height=28):
        super().__init__(parent, fg_color="transparent", height=height)
        self.pack_propagate(False)
        top_row = ctk.CTkFrame(self, fg_color="transparent")
        top_row.pack(fill="x", pady=(0, 2))
        ctk.CTkLabel(top_row, text=label,
                      font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                      text_color=COLORS["text_muted"], anchor="w"
                      ).pack(side="left")
        self.value_var = ctk.StringVar(value="")
        ctk.CTkLabel(top_row, textvariable=self.value_var,
                      font=ctk.CTkFont(_safe_mono_font(), 12, "bold"),
                      text_color=COLORS["text"], anchor="e"
                      ).pack(side="right")
        self.bar = ctk.CTkProgressBar(self, height=6, corner_radius=3,
                                       progress_color=color, fg_color=COLORS["bar_bg"])
        self.bar.pack(fill="x", pady=(2, 0))
        self.bar.set(0)
        self.color = color

    def update_value(self, percent, suffix="%", subtitle=None):
        if percent is None:
            self.value_var.set("")
            self.bar.set(0)
            return
        if subtitle:
            self.value_var.set(f"{subtitle}")
        else:
            self.value_var.set(f"{percent:.0f}{suffix}")
        self.bar.set(min(1.0, percent / 100.0))
        if percent > 85:
            self.bar.configure(progress_color=COLORS["danger"])
        elif percent > 65:
            self.bar.configure(progress_color=COLORS["warning"])
        else:
            self.bar.configure(progress_color=self.color)


#  ParamRow (one row of the bot's parameter editor) 

_PARAM_LABEL_OVERRIDES = {
    "Pos. Size (USDT/coin)": "Pos Size",
    "Pos. Size (USD)": "Pos Size",
    "Margin (USDT)": "Margin",
    "Margin / Trade": "Margin",
    "Kelly Cap (USDT)": "Kelly Cap",
    "Max Open Trades": "Max Open",
    "Max Positions": "Max Pos",
    "Activation TP": "Activation",
    "Trailing Distance": "Trail Dist",
    "Post-Partial Trail": "Post Trail",
    "Breakeven At": "BE At",
    "Partial Close": "Partial",
    "Daily Loss Limit": "Daily Loss",
    "Hard Loss Mult": "Hard Mult",
    "Liq Safety Buffer": "Liq Safety",
    "Scan Interval": "Scan",
    "Monitor Interval": "Monitor",
    "SL Cooldown": "SL Cooldown",
    "Momentum Window": "Mom Window",
    "Momentum Loss": "Mom Loss",
    "Coins per side": "Coins/Side",
    "Max Gross Exposure": "Gross Cap",
    "Rebalance every": "Rebalance",
    "Universe size": "Universe",
    "Min Volume": "Min Vol",
    "Base Capital": "Capital",
    "Per-Leg Stop": "Leg Stop",
    "Blocked Hours": "Blocked",
    "Trend Sensitivity": "Sensitivity",
    "Exit Threshold": "Exit Vote",
    "Check Interval": "Check",
    "Disaster Stop": "Disaster",
    "Vol-Targeting (0/1)": "Vol Target",
    "Vol Lookback": "Vol Look",
    "Failed Entry Stop": "Fail Stop",
    "Failed Entry Age": "Fail Age",
    "Failed Entry MFE": "Fail MFE",
    "Failed Entry Loss": "Fail Loss",
    "Pre-Act Stop": "Pre Stop",
    "Giveback %": "Giveback",
    "Pre-Act MFE": "Pre MFE",
    "Signal Check": "Signal",
    "Vote to Enter": "Vote In",
    "Stale Exit Limit": "Stale Exit",
}


def _compact_param_label(label: str) -> str:
    return _PARAM_LABEL_OVERRIDES.get(label, label)


def _measure_text_px(family: str, size: int, weight: str, text: str) -> int:
    try:
        return int(tkfont.Font(family=family, size=size, weight=weight).measure(text))
    except Exception:
        return max(1, len(str(text)) * max(6, size // 2))


def _ellipsize_text(text: str, max_px: int, family: str, size: int, weight: str) -> str:
    if max_px <= 0:
        return text
    if _measure_text_px(family, size, weight, text) <= max_px:
        return text
    ellipsis = "..."
    available = max_px - _measure_text_px(family, size, weight, ellipsis)
    if available <= 0:
        return ellipsis
    out = ""
    for ch in text:
        nxt = out + ch
        if _measure_text_px(family, size, weight, nxt) > available:
            break
        out = nxt
    return (out.rstrip() + ellipsis) if out else ellipsis


class ParamRow(ctk.CTkFrame):
    """One editable numeric parameter:

    * ```` / ``+`` buttons step by ``step`` clamped to ``[vmin, vmax]``
    * The numeric value is also click-to-edit (opens a tiny modal)
    * Calls ``on_change(key, value)`` on any change
    """

    def __init__(self, parent, mono_font, key, label, value, step, vmin, vmax,
                 fmt, unit, accent, on_change, tooltip: str = ""):
        super().__init__(parent, fg_color="transparent", height=32)
        self.pack_propagate(False)
        self.grid_propagate(False)
        # Label takes the main space; controls fixed on the right
        self.grid_columnconfigure(0, weight=1)
        self.key = key
        self.value = value
        self.step = step
        self.vmin = vmin
        self.vmax = vmax
        self.fmt = fmt
        self.unit = unit
        self.on_change = on_change
        self.mono_font = mono_font
        self.accent = accent
        self._label_full = label
        self._label_display = _compact_param_label(label)
        self._label_font_family = FONT_BODY
        self._label_font_size = 12
        self._label_font_weight = "bold"
        display_label = self._label_display
        tooltip_text = tooltip
        if display_label != label:
            tooltip_text = f"{label}\n{tooltip}" if tooltip else label

        self.label_widget = ctk.CTkLabel(self, text=display_label,
                                          font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                                          text_color=COLORS["text_dim"], anchor="w",
                                          justify="left", width=20,
                                          cursor="question_arrow" if tooltip_text else "")
        self.label_widget.grid(row=0, column=0, sticky="ew", padx=(0, 3))
        self.label_widget.bind("<Configure>", self._update_label_ellipsis)

        if tooltip_text:
            attach_tooltip(self.label_widget, tooltip_text, delay_ms=600)

        # Compact controls  buttons directly next to the value, NO
        # container box.
        ctk.CTkButton(self, text="-", width=18, height=24,
                       font=ctk.CTkFont(FONT_BODY, 13, "bold"),
                       fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"], corner_radius=4,
                       command=self._decrement
                       ).grid(row=0, column=1, padx=(0, 1))

        self.value_var = ctk.StringVar(value=self._format_value(value))
        self.value_lbl = ctk.CTkLabel(self, textvariable=self.value_var,
                                       font=ctk.CTkFont(mono_font, 13, "bold"),
                                       text_color=COLORS["text"],
                                       width=self._value_width(), height=24,
                                       fg_color=COLORS["bg"],
                                       corner_radius=4,
                                       cursor="hand2")
        self.value_lbl.grid(row=0, column=2, padx=0)
        self.value_lbl.bind("<Button-1>", lambda e: self._open_edit())
        attach_tooltip(self.value_lbl, "Click to enter exact value", delay_ms=800)

        ctk.CTkButton(self, text="+", width=18, height=24,
                       font=ctk.CTkFont(FONT_BODY, 13, "bold"),
                       fg_color=COLORS["bg"], hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"], corner_radius=4,
                       command=self._increment
                       ).grid(row=0, column=3, padx=(1, 0))

    def _format_value(self, v):
        try:
            num = float(v)
            if self.unit == "$" and abs(num) >= 1_000_000:
                return f"{num / 1_000_000:g}M $"
        except (TypeError, ValueError):
            pass
        return f"{v:{self.fmt}}{self.unit}"

    def _value_width(self) -> int:
        values = [self.value, self.vmin, self.vmax]
        samples = []
        for val in values:
            try:
                samples.append(self._format_value(val))
            except Exception:
                pass
        samples.extend(["-500$", "100M $", "1000d"])
        measured = max(_measure_text_px(self.mono_font, 13, "bold", s) for s in samples)
        return max(56, min(86, measured + 16))

    def _update_label_ellipsis(self, event=None):
        width = getattr(event, "width", 0) or self.label_widget.winfo_width()
        text = _ellipsize_text(
            self._label_display,
            max(36, width - 2),
            self._label_font_family,
            self._label_font_size,
            self._label_font_weight,
        )
        if self.label_widget.cget("text") != text:
            self.label_widget.configure(text=text)

    def _increment(self):
        new_val = round(self.value + self.step, 4)
        if new_val <= self.vmax:
            self.value = new_val
            self.value_var.set(self._format_value(new_val))
            self.on_change(self.key, new_val)
            self._flash()

    def _decrement(self):
        new_val = round(self.value - self.step, 4)
        if new_val >= self.vmin:
            self.value = new_val
            self.value_var.set(self._format_value(new_val))
            self.on_change(self.key, new_val)
            self._flash()

    def _flash(self):
        self.value_lbl.configure(text_color=COLORS["warning"])
        self.after(400, lambda: self.value_lbl.configure(text_color=COLORS["text"])
                    if self.value_lbl.winfo_exists() else None)

    def set_value(self, v):
        self.value = v
        self.value_var.set(self._format_value(v))

    def _open_edit(self):
        root = self.winfo_toplevel()
        dlg = ctk.CTkToplevel(root)
        dlg.title("Edit value")
        dlg.configure(fg_color=COLORS["panel"])
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.transient(root)
        safe_geometry(dlg, 280, 180, parent=root)

        ctk.CTkLabel(dlg, text="Enter value",
                      font=ctk.CTkFont(FONT_BODY, 13, "bold"),
                      text_color=COLORS["text"]
                      ).pack(pady=(20, 4))

        ctk.CTkLabel(dlg, text=f"Range: {self.vmin:{self.fmt}} - {self.vmax:{self.fmt}}{self.unit}",
                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(pady=(0, 12))

        entry = ctk.CTkEntry(dlg, width=180, height=36, corner_radius=6,
                              font=ctk.CTkFont(self.mono_font, 14, "bold"),
                              fg_color=COLORS["bg"], border_color=self.accent,
                              text_color=COLORS["text"], justify="center")
        entry.pack()
        entry.insert(0, f"{self.value:{self.fmt}}")
        entry.select_range(0, "end")
        entry.focus()

        err_lbl = ctk.CTkLabel(dlg, text="",
                                font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                                text_color=COLORS["danger"])
        err_lbl.pack(pady=(4, 0))

        def _apply(*args):
            txt = entry.get().strip().replace(",", ".")
            try:
                val = float(txt)
            except ValueError:
                err_lbl.configure(text="Not a valid number")
                return
            if val < self.vmin or val > self.vmax:
                err_lbl.configure(text="Out of range")
                return
            val = round(val, 4)
            self.value = val
            self.value_var.set(self._format_value(val))
            self.on_change(self.key, val)
            self._flash()
            dlg.destroy()

        def _cancel(*args):
            dlg.destroy()

        btns = ctk.CTkFrame(dlg, fg_color="transparent")
        btns.pack(pady=12)

        ctk.CTkButton(btns, text="Apply", width=80, height=30, corner_radius=6,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color=self.accent, hover_color=COLORS["panel_hover"],
                       text_color="#ffffff", command=_apply
                       ).pack(side="left", padx=4)

        ctk.CTkButton(btns, text="Cancel", width=80, height=30, corner_radius=6,
                       font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                       fg_color="transparent", hover_color=COLORS["panel_hover"],
                       text_color=COLORS["text_dim"],
                       border_width=1, border_color=COLORS["border"],
                       command=_cancel
                       ).pack(side="left", padx=4)

        entry.bind("<Return>", _apply)
        entry.bind("<Escape>", _cancel)


#  BadHoursRow (risk-manager UTC hour blocklist) 

class BadHoursRow(ctk.CTkFrame):
    """Text-entry row for the risk-manager's ``bad_hours`` parameter.

    Stored as a comma-separated hour list in the ``bot_params`` DB
    table (e.g. ``"2,3,14"``). Hours are interpreted in the user's
    local timezone (BOT_TIMEZONE env var), so a Chinese user entering
    "19" blocks 19:00 CST locally, not 19:00 UTC.

    The widget makes this explicit in three places:
    1. The label says "Blocked Hours" + a "Local" badge with the TZ
    2. Hover tooltip explains the local-time semantics
    3. Learning system writes local-tz hours back to this same field
    """

    @staticmethod
    def _get_tz_label() -> str:
        """Return short timezone label for the badge  e.g. 'CST', 'CEST', 'UTC'."""
        import os as _os
        tz_name = _os.getenv("BOT_TIMEZONE", "UTC").strip()
        if tz_name == "UTC":
            return "UTC"
        # Map common IANA names to short labels
        short = {
            "Asia/Shanghai":     "CST+8",
            "Asia/Hong_Kong":    "HKT",
            "Asia/Singapore":    "SGT",
            "Asia/Tokyo":        "JST",
            "Asia/Seoul":        "KST",
            "Asia/Kolkata":      "IST",
            "Asia/Dubai":        "GST",
            "Europe/Berlin":     "CET",
            "Europe/Paris":      "CET",
            "Europe/London":     "GMT/BST",
            "Europe/Moscow":     "MSK",
            "America/New_York":  "EST/EDT",
            "America/Chicago":   "CST/CDT",
            "America/Denver":    "MST/MDT",
            "America/Los_Angeles": "PST/PDT",
            "America/Sao_Paulo": "BRT",
            "Australia/Sydney":  "AEST",
        }
        return short.get(tz_name, "Local")

    _TZ_TOOLTIP = (
        "Hours when the bot will NOT open new trades.\n"
        "Comma-separated, 023 format  e.g.  2,3,14,22\n"
        "\n"
        "Hours are in your LOCAL timezone (set in .env: BOT_TIMEZONE).\n"
        "Default for new installs: Asia/Shanghai (UTC+8).\n"
        "\n"
        "Examples (your local clock):\n"
        "  Block evening 22:0023:00  enter  22,23\n"
        "  Block midday  12:0013:00  enter  12,13\n"
        "\n"
        "Auto-learning: after 30+ trades, the bot detects which\n"
        "hours produced losses and adds them automatically  in\n"
        "your local timezone, so the numbers always make sense."
    )

    def __init__(self, parent, bot_name: str, accent: str, mono_font: str):
        super().__init__(parent, fg_color="transparent", height=30)
        self.pack_propagate(False)
        self.grid_propagate(False)
        self.grid_columnconfigure(0, weight=1)

        self.bot_name  = bot_name
        self.accent    = accent
        self.mono_font = mono_font

        #  Label (left) 
        lbl_frame = ctk.CTkFrame(self, fg_color="transparent")
        lbl_frame.grid(row=0, column=0, sticky="w")

        self.label_widget = ctk.CTkLabel(
            lbl_frame, text="Blocked",
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            text_color=COLORS["text_dim"], anchor="w",
            cursor="question_arrow"
        )
        self.label_widget.pack(side="left")

        # Local timezone badge (shows e.g. "CST+8", "CET", "UTC")
        tz_label = self._get_tz_label()
        utc_badge = ctk.CTkLabel(
            lbl_frame, text=f" {tz_label} ",
            font=ctk.CTkFont(FONT_BODY, 9, "bold"),
            text_color="#ffffff",
            fg_color=COLORS["warning"],
            corner_radius=4, width=0, height=16,
            cursor="question_arrow"
        )
        utc_badge.pack(side="left", padx=(4, 0))

        attach_tooltip(self.label_widget, self._TZ_TOOLTIP, delay_ms=400)
        attach_tooltip(utc_badge, self._TZ_TOOLTIP, delay_ms=400)

        #  Entry (right) 
        self._var = ctk.StringVar(value=self._read_from_db())
        self._entry = ctk.CTkEntry(
            self, textvariable=self._var,
            width=104, height=24, corner_radius=4,
            font=ctk.CTkFont(mono_font, 12, "bold"),
            fg_color=COLORS["bg"],
            border_color=COLORS["border"],
            text_color=COLORS["text"],
            placeholder_text="e.g. 2,3,14",
            justify="center"
        )
        self._entry.grid(row=0, column=1, padx=(4, 0))
        self._entry.bind("<Return>",    self._commit)
        self._entry.bind("<FocusOut>",  self._commit)
        attach_tooltip(self._entry, "Press Enter or click away to save  (values in local time)", delay_ms=600)

        self._err_lbl = ctk.CTkLabel(
            self, text="",
            font=ctk.CTkFont(FONT_BODY, 9, "bold"),
            text_color=COLORS["danger"]
        )
        self._err_lbl.grid(row=1, column=0, columnspan=2, sticky="w", pady=(1, 0))

    #  DB helpers 

    def _read_from_db(self) -> str:
        rows = query_db(
            "SELECT param_value FROM bot_params WHERE bot_name=? AND param_name=?",
            (self.bot_name, "bad_hours")
        )
        return rows[0][0] if rows else ""

    def refresh(self) -> None:
        """Pull the latest value from the DB (called by the UI poll cycle)."""
        self._var.set(self._read_from_db())
        self._err_lbl.configure(text="")

    #  Validation & save 

    def _commit(self, _evt=None):
        raw = self._var.get().strip()
        if not raw:
            # Empty = clear the setting (no hours blocked)
            self._write_to_db("")
            self._flash_ok()
            return
        # Validate: must be comma-separated integers 0-23
        try:
            hours = [int(h.strip()) for h in raw.split(",") if h.strip()]
            if not hours:
                raise ValueError("empty after parse")
            out_of_range = [h for h in hours if not (0 <= h <= 23)]
            if out_of_range:
                raise ValueError(f"out of range: {out_of_range}")
        except ValueError as exc:
            self._err_lbl.configure(text=f" {exc}  (use 023, comma-separated)")
            self._entry.configure(border_color=COLORS["danger"])
            return
        # Normalise: sort + deduplicate, then write
        clean = ",".join(str(h) for h in sorted(set(hours)))
        self._var.set(clean)
        self._write_to_db(clean)
        self._flash_ok()

    def _write_to_db(self, value: str) -> None:
        try:
            from core.database import set_param   # type: ignore
            set_param(self.bot_name, "bad_hours", value,
                      f"Gesperrte Stunden (lokal): {value or 'keine'}")
        except Exception as exc:
            self._err_lbl.configure(text=f"DB error: {exc}")

    def _flash_ok(self) -> None:
        self._err_lbl.configure(text="")
        self._entry.configure(border_color=COLORS["success"])
        self.after(800, lambda: self._entry.configure(
            border_color=COLORS["border"]
        ) if self._entry.winfo_exists() else None)
