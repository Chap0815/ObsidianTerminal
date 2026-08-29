"""
All the dialogs that ask the user "are you sure you want to stop / quit /
close positions?" and the modal busy-dialog that runs the actual work
asynchronously.

Every function takes the running :class:`ObsidianApp` as its first arg
(referred to internally as ``app``).
"""

from __future__ import annotations

import threading
from contextlib import ExitStack

import customtkinter as ctk

from launcher.config.settings import BOT_META, BOT_ORDER, COLORS, FONT_BODY
from launcher.core.positions import (
    normalize_futures_position_view_row,
    normalize_spot_position_view_row,
    refresh_positions_with_live_prices,
    refresh_spot_positions_with_live_prices,
)
from launcher.ui.components.widgets import safe_geometry
from launcher.ui.logging_panel import log_to_card
from launcher.ui.theme import force_dark_titlebar


class _DialogRefreshGate:
    """Allow one refresh worker and reject completions from stale generations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active = False
        self._generation = 0

    def claim(self) -> int | None:
        with self._lock:
            if self._active:
                return None
            self._generation += 1
            self._active = True
            return self._generation

    def finish(self, generation: int) -> bool:
        with self._lock:
            if not self._active or generation != self._generation:
                return False
            self._active = False
            return True


def _post_ui(app, callback, *, delay_ms: int = 0) -> bool:
    post = getattr(app, "post_ui", None)
    if callable(post):
        return post(callback, delay_ms=delay_ms) is not False
    app.after(delay_ms, callback)
    return True


def _start_critical_worker(app, target, *, name: str):
    registry = getattr(app, "critical_workers", None)
    start = getattr(registry, "start", None)
    if callable(start):
        return start(target, name=name, daemon=True)
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def show_state_read_error_dialog(app, title: str, detail: str) -> None:
    """Fail-closed dialog when position state cannot be read."""
    dlg = ctk.CTkToplevel(app)
    dlg.title(title)
    dlg.configure(fg_color=COLORS["panel"])
    dlg.grab_set()
    dlg.transient(app)
    force_dark_titlebar(dlg)
    safe_geometry(dlg, 500, 220, parent=app)

    ctk.CTkLabel(
        dlg, text=" Position State Check Failed",
        font=ctk.CTkFont(FONT_BODY, 16, "bold"),
        text_color=COLORS["danger"],
    ).pack(pady=(22, 8), padx=24, anchor="w")
    ctk.CTkLabel(
        dlg,
        text=(
            "The launcher could not prove that positions are flat.\n"
            "Stop/quit was aborted to avoid orphaning a live position."
        ),
        font=ctk.CTkFont(FONT_BODY, 11, "bold"),
        text_color=COLORS["text"],
        justify="left",
    ).pack(padx=24, pady=(0, 10), anchor="w")
    ctk.CTkLabel(
        dlg, text=str(detail)[:300],
        font=ctk.CTkFont(app.mono_font, 10),
        text_color=COLORS["text_muted"],
        justify="left",
        wraplength=440,
    ).pack(padx=24, pady=(0, 16), anchor="w")
    ctk.CTkButton(
        dlg, text="OK", height=34, width=100,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color=COLORS["danger"],
        hover_color="#b91c1c",
        text_color="#ffffff",
        command=dlg.destroy,
    ).pack(side="bottom", pady=18)


#  Generic busy dialog 

def show_busy_dialog(app, title: str, intro: str, worker, **worker_kwargs) -> None:
    """Modal busy-dialog with progress text.

    Runs ``worker(update_fn)`` in a background thread so the UI doesn't
    freeze. ``update_fn(msg)`` posts a new status line, thread-safe via
    ``.after()``.

    The dialog is informational only  it sits on top of the relevant
    card but does NOT ``grab_set()``, so the other bot cards and sidebar
    controls stay fully operable while the worker runs. The X button is
    disabled until the worker is done so the user can't accidentally
    dismiss a running operation.
    """
    dlg = ctk.CTkToplevel(app)
    dlg.title(title)
    dlg.configure(fg_color=COLORS["panel"])
    # NOTE: NO grab_set()  that blocked the entire UI. The dialog is
    # informational, not blocking. Other bot cards remain fully usable.
    dlg.transient(app)
    force_dark_titlebar(dlg)
    dlg.resizable(False, False)
    safe_geometry(dlg, 440, 180, parent=app)
    # Stop the user closing it with X while the worker is running
    dlg.protocol("WM_DELETE_WINDOW", lambda: None)

    ctk.CTkLabel(dlg, text=title,
                  font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                  text_color=COLORS["text"]
                  ).pack(pady=(20, 4))
    ctk.CTkLabel(dlg, text=intro,
                  font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                  text_color=COLORS["text_dim"]
                  ).pack(pady=(0, 12))

    status_var = ctk.StringVar(value="Starting")
    ctk.CTkLabel(dlg, textvariable=status_var,
                  font=ctk.CTkFont(app.mono_font, 11, "bold"),
                  text_color=COLORS["balanced"]
                  ).pack(pady=(0, 12))

    prog = ctk.CTkProgressBar(dlg, mode="indeterminate",
                                 height=4, corner_radius=2,
                                 progress_color=COLORS["balanced"],
                                 fg_color=COLORS["bar_bg"])
    prog.pack(fill="x", padx=30)
    prog.start()

    def update_fn(msg: str) -> None:
        # The worker only enqueues; Tcl is entered by the UI owner thread.
        try:
            _post_ui(app, lambda: status_var.set(msg))
        except Exception:
            pass

    def _run() -> None:
        try:
            worker(update_fn)
        except Exception as e:
            print(f"[BusyDialog] worker failed: {e}")
            update_fn(f"Failed: {e}")
        finally:
            # Clean up on the main thread
            _post_ui(
                app,
                lambda: (prog.stop(), dlg.destroy()),
                delay_ms=400,
            )

    try:
        _start_critical_worker(
            app,
            _run,
            name=f"busy-{title}",
        )
    except Exception as exc:
        try:
            prog.stop()
        except Exception:
            pass
        detail = str(exc).replace("\r", " ").replace("\n", " ")[:160]
        try:
            status_var.set(f"Failed to start: {detail}")
        except Exception:
            pass
        try:
            dlg.protocol("WM_DELETE_WINDOW", dlg.destroy)
        except Exception:
            pass


#  Spot stop dialog 

def show_spot_stop_dialog(app, name: str, positions: list) -> None:
    """Confirm dialog for stopping a SPOT bot that has open positions.

    Renders immediately with the cached prices from the poller (typically
    30s old) rather than fetching live tickers on the Tk thread (which
    would freeze the UI for ~400ms per position). A lightweight
    " Refresh" button pulls live prices on demand without blocking.

    Options: 'Close all & stop', 'Stop without closing', 'Cancel'.
    """
    import tkinter as tk

    from launcher.core.bot_controller import (
        async_close_and_stop_spot, async_simple_stop,
    )

    # No sync price refresh here  positions come from the poller cache
    # (30s old), accurate enough for a stop/close decision and without
    # blocking the Tk thread.

    # Dynamic height: ~340px base + 32px per row (clamped to usable screen;
    # the position list scrolls when it would otherwise exceed it).
    calc_height = 340 + (len(positions) * 32)
    dlg_height = max(500, calc_height)

    dlg = ctk.CTkToplevel(app)
    dlg.title(f"Stop {name} Bot")
    dlg.configure(fg_color=COLORS["panel"])
    dlg.grab_set()
    dlg.transient(app)
    force_dark_titlebar(dlg)
    safe_geometry(dlg, 620, dlg_height, parent=app)

    ctk.CTkLabel(dlg, text=" Open Positions Detected",
                  font=ctk.CTkFont(FONT_BODY, 16, "bold"),
                  text_color=COLORS["warning"]
                  ).pack(pady=(20, 4), padx=24, anchor="w")

    modes = sorted({str(p.get("mode") or "").upper() for p in positions
                    if isinstance(p, dict) and p.get("mode")})
    if modes == ["LIVE"]:
        mode_lbl = "LIVE"
        mode_color = COLORS["danger"]
    elif modes == ["SIM"]:
        mode_lbl = "SIMULATION"
        mode_color = COLORS["warning"]
    elif modes:
        mode_lbl = "/".join(modes)
        mode_color = COLORS["danger"]
    else:
        sim_mode = app.config[name].get("SIMULATION", True)
        mode_lbl = "SIMULATION" if sim_mode else "LIVE"
        mode_color = COLORS["warning"] if sim_mode else COLORS["danger"]
    sim_mode = (modes == ["SIM"]) if modes else bool(app.config[name].get("SIMULATION", True))

    ctk.CTkLabel(
        dlg,
        text=f"The {name} bot has {len(positions)} open position(s) in {mode_lbl} mode.",
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        text_color=mode_color, justify="left"
    ).pack(pady=(0, 12), padx=24, anchor="w")

    pos_frame = ctk.CTkScrollableFrame(dlg, fg_color=COLORS["bg"], corner_radius=8,
                                border_width=1, border_color=COLORS["border"],
                                scrollbar_button_color=COLORS["border"])
    pos_frame.pack(fill="both", expand=True, padx=24, pady=(0, 12))

    hdr = ctk.CTkFrame(pos_frame, fg_color="transparent")
    hdr.pack(fill="x", padx=10, pady=(8, 4))
    for col_name, w in [("Symbol", 80), ("Buy", 90),
                          ("Now", 90), ("PnL %", 80), ("PnL $", 90)]:
        ctk.CTkLabel(hdr, text=col_name,
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_subtle"], width=w, anchor="w"
                      ).pack(side="left")

    ctk.CTkFrame(pos_frame, fg_color=COLORS["border_soft"], height=1
                  ).pack(fill="x", padx=10)

    total_pnl = 0.0
    invalid_total = False
    for p in positions:
        p = normalize_spot_position_view_row(p)
        row = ctk.CTkFrame(pos_frame, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=2)

        sym = p["symbol"]
        buy = p["buy_price"]
        curr = p["current_price"]
        pnl_usdt = p["unrealized_pnl"]
        pnl_pct = p["unrealized_pct"]
        invalid = bool(p.get("invalid_state"))
        if invalid:
            invalid_total = True
        else:
            total_pnl += pnl_usdt

        pnl_color = COLORS["warning"] if invalid else (
            COLORS["success"] if pnl_usdt >= 0 else COLORS["danger"]
        )
        buy_text = "Invalid" if invalid else f"{buy:.6f}"
        current_text = "Invalid" if invalid else f"{curr:.6f}"
        pnl_pct_text = "stale" if invalid else f"{pnl_pct:+.2f}%"
        pnl_usdt_text = "stale" if invalid else f"{pnl_usdt:+.2f}"

        ctk.CTkLabel(row, text=sym, font=ctk.CTkFont(app.mono_font, 11, "bold"),
                      text_color=COLORS["text"], width=80, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=buy_text,
                      font=ctk.CTkFont(app.mono_font, 10),
                      text_color=COLORS["text_dim"], width=90, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=current_text,
                      font=ctk.CTkFont(app.mono_font, 10),
                      text_color=COLORS["text"], width=90, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=pnl_pct_text,
                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                      text_color=pnl_color, width=80, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=pnl_usdt_text,
                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                      text_color=pnl_color, width=90, anchor="w").pack(side="left")

    explanation = (
        "In SIMULATION mode: closes virtual positions and writes "
        "realized PnL to the database."
        if sim_mode else
        " LIVE MODE: market-sells your holdings on the exchange. "
        "This is irreversible and may have slippage!"
    )

    ctk.CTkLabel(dlg, text=explanation,
                  font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                  text_color=COLORS["text_subtle"],
                  justify="left").pack(padx=24, pady=(0, 4), anchor="w")

    # "Prices as of last poll" note + on-demand refresh button.
    # The poller updates every 30s  these prices are never more than 30s old.
    price_note = ctk.CTkLabel(
        dlg,
        text=" Prices from last poll  click Refresh to refresh live",
        font=ctk.CTkFont(FONT_BODY, 9),
        text_color=COLORS["text_subtle"],
    )
    price_note.pack(padx=24, pady=(0, 8), anchor="w")

    btns = ctk.CTkFrame(dlg, fg_color="transparent")
    btns.pack(side="bottom", fill="x", padx=20, pady=16)

    def _close_all_and_stop():
        dlg.destroy()
        show_busy_dialog(
            app,
            f"Stopping {name}",
            "Closing all positions and shutting down",
            lambda update: async_close_and_stop_spot(app, name, update),
        )

    def _stop_only():
        dlg.destroy()
        show_busy_dialog(
            app,
            f"Stopping {name}",
            "Shutting down (positions kept open)",
            lambda update: async_simple_stop(app, name, app.cards[name], update),
        )

    refresh_gate = _DialogRefreshGate()

    def _refresh_prices():
        """Live refresh on demand, in a background thread."""
        generation = refresh_gate.claim()
        if generation is None:
            price_note.configure(text=" Refresh already in progress")
            return
        price_note.configure(text=" Refreshing prices")

        def _bg():
            try:
                refreshed = refresh_spot_positions_with_live_prices(
                    list(positions)
                )
            except Exception:
                def _fail():
                    if not refresh_gate.finish(generation):
                        return
                    try:
                        if dlg.winfo_exists():
                            price_note.configure(
                                text=" Refresh failed  using last poll values"
                            )
                    except Exception:
                        pass
                try:
                    if not _post_ui(app, _fail):
                        refresh_gate.finish(generation)
                except Exception:
                    refresh_gate.finish(generation)
                return

            def _apply():
                if not refresh_gate.finish(generation):
                    return
                try:
                    if not dlg.winfo_exists():
                        return
                    price_note.configure(
                        text=" Prices refreshed live"
                    )
                    # Re-render pos_frame with new data
                    for widget in pos_frame.winfo_children():
                        widget.destroy()
                    hdr2 = ctk.CTkFrame(pos_frame, fg_color="transparent")
                    hdr2.pack(fill="x", padx=10, pady=(8, 4))
                    for col_name, w in [("Symbol", 80), ("Buy", 90),
                                         ("Now", 90), ("PnL %", 80),
                                         ("PnL $", 90)]:
                        ctk.CTkLabel(
                            hdr2, text=col_name,
                            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                            text_color=COLORS["text_subtle"],
                            width=w, anchor="w"
                        ).pack(side="left")
                    ctk.CTkFrame(
                        pos_frame, fg_color=COLORS["border_soft"], height=1
                    ).pack(fill="x", padx=10)
                    new_total = 0.0
                    new_invalid_total = False
                    for p in refreshed:
                        p = normalize_spot_position_view_row(p)
                        r = ctk.CTkFrame(pos_frame, fg_color="transparent")
                        r.pack(fill="x", padx=10, pady=2)
                        buy2 = p["buy_price"]
                        curr2 = p.get("current_price", buy2)
                        pnl_u = p.get("unrealized_pnl", 0.0)
                        pnl_p = p.get("unrealized_pct", 0.0)
                        invalid = bool(p.get("invalid_state"))
                        if invalid:
                            new_invalid_total = True
                        else:
                            new_total += pnl_u
                        pc = COLORS["warning"] if invalid else (
                            COLORS["success"] if pnl_u >= 0 else COLORS["danger"])
                        buy_text = "Invalid" if invalid else f"{buy2:.6f}"
                        curr_text = "Invalid" if invalid else f"{curr2:.6f}"
                        pnl_pct_text = "stale" if invalid else f"{pnl_p:+.2f}%"
                        pnl_usdt_text = "stale" if invalid else f"{pnl_u:+.2f}"
                        ctk.CTkLabel(r, text=p["symbol"],
                                      font=ctk.CTkFont(app.mono_font, 11, "bold"),
                                      text_color=COLORS["text"],
                                      width=80, anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=buy_text,
                                      font=ctk.CTkFont(app.mono_font, 10),
                                      text_color=COLORS["text_dim"],
                                      width=90, anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=curr_text,
                                      font=ctk.CTkFont(app.mono_font, 10),
                                      text_color=COLORS["text"],
                                      width=90, anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=pnl_pct_text,
                                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                                      text_color=pc,
                                      width=80, anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=pnl_usdt_text,
                                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                                      text_color=pc,
                                      width=90, anchor="w").pack(side="left")
                    refreshed_button_pnl = (
                        "PnL stale" if new_invalid_total
                        else f"{new_total:+.2f} USDT"
                    )
                    close_button.configure(
                        text=f" Close All & Stop ({refreshed_button_pnl})"
                    )
                except (tk.TclError, Exception):
                    pass

            try:
                if not _post_ui(app, _apply):
                    refresh_gate.finish(generation)
            except Exception:
                refresh_gate.finish(generation)

        try:
            threading.Thread(target=_bg, daemon=True,
                              name=f"refresh-spot-{name}").start()
        except Exception:
            refresh_gate.finish(generation)
            price_note.configure(text=" Refresh could not start")

    action_color = COLORS["danger"] if not sim_mode else COLORS["warning"]
    close_button_pnl = (
        "PnL stale" if invalid_total else f"{total_pnl:+.2f} USDT"
    )
    close_button = ctk.CTkButton(btns,
        text=f" Close All & Stop ({close_button_pnl})",
        height=36, corner_radius=8, width=260,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color=action_color, hover_color=COLORS["panel_hover"],
        text_color="#ffffff",
        command=_close_all_and_stop,
    )
    close_button.pack(side="right")

    ctk.CTkButton(btns, text=" Stop only",
        height=36, corner_radius=8, width=110,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=_stop_only,
    ).pack(side="right", padx=(0, 8))

    ctk.CTkButton(btns, text="Refresh",
        height=36, corner_radius=8, width=84,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=_refresh_prices,
    ).pack(side="right", padx=(0, 4))

    ctk.CTkButton(btns, text="Cancel",
        height=36, corner_radius=8, width=90,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=dlg.destroy,
    ).pack(side="right", padx=(0, 8))


#  Futures stop dialog 

def show_futures_stop_dialog(app, name: str, positions: list) -> None:
    """Confirm dialog for stopping FUTURES when positions are open.

    Renders immediately with cached poller prices rather than fetching
    live tickers on the Tk thread (which would freeze the UI); a  button
    provides on-demand live refresh.

    Options: 'Close all & stop', 'Stop without closing', 'Cancel'.
    """
    import tkinter as tk

    # No sync price refresh here  cached poller values (30s old) are
    # accurate enough for a stop decision and don't block the Tk thread.

    calc_height = 340 + (len(positions) * 32)
    dlg_height = max(540, calc_height)

    label = BOT_META.get(name, {}).get("label", name)
    dlg = ctk.CTkToplevel(app)
    dlg.title(f"Stop {label} Bot")
    dlg.configure(fg_color=COLORS["panel"])
    dlg.grab_set()
    dlg.transient(app)
    force_dark_titlebar(dlg)
    safe_geometry(dlg, 620, dlg_height, parent=app)

    ctk.CTkLabel(dlg, text=" Open Positions Detected",
                  font=ctk.CTkFont(FONT_BODY, 16, "bold"),
                  text_color=COLORS["warning"]
                  ).pack(pady=(20, 4), padx=24, anchor="w")

    modes = sorted({str(p.get("mode") or "").upper() for p in positions
                    if isinstance(p, dict) and p.get("mode")})
    if modes == ["LIVE"]:
        sim_mode = False
        mode_lbl = "LIVE"
        mode_color = COLORS["danger"]
    elif modes == ["SIM"]:
        sim_mode = True
        mode_lbl = "SIMULATION"
        mode_color = COLORS["warning"]
    elif modes:
        sim_mode = False
        mode_lbl = "/".join(modes)
        mode_color = COLORS["danger"]
    else:
        sim_mode = bool(app.config.get(name, {}).get("SIMULATION", True))
        mode_lbl = "SIMULATION" if sim_mode else "LIVE"
        mode_color = COLORS["warning"] if sim_mode else COLORS["danger"]

    ctk.CTkLabel(
        dlg,
        text=f"The {label} bot has {len(positions)} open position(s) in {mode_lbl} mode.",
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        text_color=mode_color, justify="left"
    ).pack(pady=(0, 12), padx=24, anchor="w")

    pos_frame = ctk.CTkScrollableFrame(dlg, fg_color=COLORS["bg"], corner_radius=8,
                                border_width=1, border_color=COLORS["border"],
                                scrollbar_button_color=COLORS["border"])
    pos_frame.pack(fill="both", expand=True, padx=24, pady=(0, 12))

    hdr = ctk.CTkFrame(pos_frame, fg_color="transparent")
    hdr.pack(fill="x", padx=10, pady=(8, 4))
    for col_name, w in [("Symbol", 70), ("Dir", 50), ("Entry", 80),
                          ("Now", 80), ("PnL %", 70), ("PnL $", 80)]:
        ctk.CTkLabel(hdr, text=col_name,
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_subtle"], width=w, anchor="w"
                      ).pack(side="left")

    ctk.CTkFrame(pos_frame, fg_color=COLORS["border_soft"], height=1).pack(fill="x", padx=10)

    total_pnl = 0.0
    invalid_total = False
    for raw_position in positions:
        p = normalize_futures_position_view_row(raw_position)
        row = ctk.CTkFrame(pos_frame, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=2)

        sym = p["symbol"]
        ptype = p["position_type"]
        entry = p["entry_price"]
        curr = p["current_price"]
        pnl_usdt = p["unrealized_pnl"]
        pnl_pct = p["unrealized_pct"]
        invalid = bool(p.get("invalid_state"))
        if invalid:
            invalid_total = True
        else:
            total_pnl += pnl_usdt

        ptype_color = COLORS["warning"] if invalid else (
            COLORS["success"] if ptype == "LONG" else COLORS["danger"]
        )
        pnl_color = COLORS["warning"] if invalid else (
            COLORS["success"] if pnl_usdt >= 0 else COLORS["danger"]
        )
        entry_text = "Invalid" if invalid else f"{entry:.4f}"
        current_text = "Invalid" if invalid else f"{curr:.4f}"
        pnl_pct_text = "stale" if invalid else f"{pnl_pct:+.2f}%"
        pnl_usdt_text = "stale" if invalid else f"{pnl_usdt:+.2f}"

        ctk.CTkLabel(row, text=sym, font=ctk.CTkFont(app.mono_font, 11, "bold"),
                      text_color=COLORS["text"], width=70, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=ptype, font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                      text_color=ptype_color, width=50, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=entry_text,
                      font=ctk.CTkFont(app.mono_font, 10),
                      text_color=COLORS["text_dim"], width=80, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=current_text,
                      font=ctk.CTkFont(app.mono_font, 10),
                      text_color=COLORS["text_dim"], width=80, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=pnl_pct_text,
                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                      text_color=pnl_color, width=70, anchor="w").pack(side="left")
        ctk.CTkLabel(row, text=pnl_usdt_text,
                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                      text_color=pnl_color, width=80, anchor="w").pack(side="left")

    # Total row
    ctk.CTkFrame(pos_frame, fg_color=COLORS["border_soft"], height=1).pack(
        fill="x", padx=10, pady=(4, 0))
    total_row = ctk.CTkFrame(pos_frame, fg_color="transparent")
    total_row.pack(fill="x", padx=10, pady=(4, 8))
    total_color = COLORS["warning"] if invalid_total else (
        COLORS["success"] if total_pnl >= 0 else COLORS["danger"]
    )
    total_text = "stale" if invalid_total else f"{total_pnl:+.2f} USDT"
    ctk.CTkLabel(total_row, text="TOTAL unrealized PnL:",
                  font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                  text_color=COLORS["text_muted"], anchor="w"
                  ).pack(side="left")
    ctk.CTkLabel(total_row, text=total_text,
                  font=ctk.CTkFont(app.mono_font, 13, "bold"),
                  text_color=total_color, anchor="e"
                  ).pack(side="right")

    if sim_mode:
        explanation = (
            "In SIMULATION mode: closing means the PnL is logged to your trade\n"
            "history at the current market price. No real money is involved.")
    else:
        explanation = (
            " LIVE MODE: closing will fire reduceOnly market orders on the\n"
            "exchange and lock in the current PnL. Slippage may apply!")

    ctk.CTkLabel(dlg, text=explanation,
                  font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                  text_color=COLORS["text_subtle"],
                  justify="left").pack(padx=24, pady=(0, 4), anchor="w")

    # Price-age note + on-demand live-refresh button.
    price_note_fut = ctk.CTkLabel(
        dlg,
        text=" Prices from last poll  click Refresh to refresh live",
        font=ctk.CTkFont(FONT_BODY, 9),
        text_color=COLORS["text_subtle"],
    )
    price_note_fut.pack(padx=24, pady=(0, 8), anchor="w")

    btns = ctk.CTkFrame(dlg, fg_color="transparent")
    btns.pack(side="bottom", fill="x", padx=20, pady=16)

    def _close_all_and_stop():
        dlg.destroy()
        emergency_close_futures_and_stop(app, name)

    def _stop_only():
        dlg.destroy()
        show_busy_dialog(
            app,
            f"Stopping {label}",
            "Stopping bot  positions stay open",
            lambda update: app._async_simple_stop(name, app.cards[name], update),
        )

    def _cancel():
        dlg.destroy()

    refresh_gate = _DialogRefreshGate()

    def _refresh_prices_fut():
        """Futures live-refresh in background."""
        generation = refresh_gate.claim()
        if generation is None:
            price_note_fut.configure(text=" Refresh already in progress")
            return
        price_note_fut.configure(text=" Refreshing prices")

        def _bg():
            try:
                refreshed = refresh_positions_with_live_prices(list(positions))
            except Exception:
                def _fail():
                    if not refresh_gate.finish(generation):
                        return
                    try:
                        if dlg.winfo_exists():
                            price_note_fut.configure(
                                text=" Refresh failed  using last poll values"
                            )
                    except Exception:
                        pass
                try:
                    if not _post_ui(app, _fail):
                        refresh_gate.finish(generation)
                except Exception:
                    refresh_gate.finish(generation)
                return

            def _apply():
                if not refresh_gate.finish(generation):
                    return
                try:
                    if not dlg.winfo_exists():
                        return
                    price_note_fut.configure(text=" Prices refreshed live")
                    for widget in pos_frame.winfo_children():
                        widget.destroy()
                    h2 = ctk.CTkFrame(pos_frame, fg_color="transparent")
                    h2.pack(fill="x", padx=10, pady=(8, 4))
                    for col_name, w in [("Symbol", 70), ("Dir", 50),
                                         ("Entry", 80), ("Now", 80),
                                         ("PnL %", 70), ("PnL $", 80)]:
                        ctk.CTkLabel(
                            h2, text=col_name,
                            font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                            text_color=COLORS["text_subtle"],
                            width=w, anchor="w"
                        ).pack(side="left")
                    ctk.CTkFrame(
                        pos_frame, fg_color=COLORS["border_soft"], height=1
                    ).pack(fill="x", padx=10)
                    new_total = 0.0
                    new_invalid_total = False
                    for p in refreshed:
                        p = normalize_futures_position_view_row(p)
                        r = ctk.CTkFrame(pos_frame, fg_color="transparent")
                        r.pack(fill="x", padx=10, pady=2)
                        pt = p.get("position_type", "?")
                        en = p["entry_price"]
                        cu = p["current_price"]
                        pu = p["unrealized_pnl"]
                        pp = p["unrealized_pct"]
                        invalid = bool(p.get("invalid_state"))
                        if invalid:
                            new_invalid_total = True
                        else:
                            new_total += pu
                        ptc = COLORS["warning"] if invalid else (
                            COLORS["success"] if pt == "LONG" else COLORS["danger"]
                        )
                        pc = COLORS["warning"] if invalid else (
                            COLORS["success"] if pu >= 0 else COLORS["danger"]
                        )
                        entry_text = "Invalid" if invalid else f"{en:.4f}"
                        current_text = "Invalid" if invalid else f"{cu:.4f}"
                        pnl_pct_text = "stale" if invalid else f"{pp:+.2f}%"
                        pnl_usdt_text = "stale" if invalid else f"{pu:+.2f}"
                        ctk.CTkLabel(r, text=p.get("symbol", "?"),
                                      font=ctk.CTkFont(app.mono_font, 11, "bold"),
                                      text_color=COLORS["text"], width=70,
                                      anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=pt,
                                      font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                                      text_color=ptc, width=50,
                                      anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=entry_text,
                                      font=ctk.CTkFont(app.mono_font, 10),
                                      text_color=COLORS["text_dim"], width=80,
                                      anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=current_text,
                                      font=ctk.CTkFont(app.mono_font, 10),
                                      text_color=COLORS["text_dim"], width=80,
                                      anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=pnl_pct_text,
                                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                                      text_color=pc, width=70,
                                      anchor="w").pack(side="left")
                        ctk.CTkLabel(r, text=pnl_usdt_text,
                                      font=ctk.CTkFont(app.mono_font, 10, "bold"),
                                      text_color=pc, width=80,
                                      anchor="w").pack(side="left")
                    ctk.CTkFrame(
                        pos_frame, fg_color=COLORS["border_soft"], height=1
                    ).pack(fill="x", padx=10, pady=(4, 0))
                    refreshed_total_row = ctk.CTkFrame(
                        pos_frame, fg_color="transparent"
                    )
                    refreshed_total_row.pack(
                        fill="x", padx=10, pady=(4, 8)
                    )
                    refreshed_total_color = (
                        COLORS["warning"] if new_invalid_total else (
                            COLORS["success"] if new_total >= 0
                            else COLORS["danger"]
                        )
                    )
                    refreshed_total_text = (
                        "stale" if new_invalid_total
                        else f"{new_total:+.2f} USDT"
                    )
                    ctk.CTkLabel(
                        refreshed_total_row,
                        text="TOTAL unrealized PnL:",
                        font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                        text_color=COLORS["text_muted"],
                        anchor="w",
                    ).pack(side="left")
                    ctk.CTkLabel(
                        refreshed_total_row,
                        text=refreshed_total_text,
                        font=ctk.CTkFont(app.mono_font, 13, "bold"),
                        text_color=refreshed_total_color,
                        anchor="e",
                    ).pack(side="right")
                    refreshed_button_pnl = (
                        "PnL stale" if new_invalid_total
                        else f"{new_total:+.2f} USDT"
                    )
                    close_button.configure(
                        text=f" Close All & Stop ({refreshed_button_pnl})"
                    )
                except (tk.TclError, Exception):
                    pass

            try:
                if not _post_ui(app, _apply):
                    refresh_gate.finish(generation)
            except Exception:
                refresh_gate.finish(generation)

        try:
            threading.Thread(target=_bg, daemon=True,
                              name="refresh-futures-stop").start()
        except Exception:
            refresh_gate.finish(generation)
            price_note_fut.configure(text=" Refresh could not start")

    action_color = COLORS["danger"] if not sim_mode else COLORS["warning"]
    close_button_pnl = (
        "PnL stale" if invalid_total else f"{total_pnl:+.2f} USDT"
    )
    close_button = ctk.CTkButton(btns,
        text=f" Close All & Stop ({close_button_pnl})",
        height=36, corner_radius=8, width=260,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color=action_color, hover_color=COLORS["panel_hover"],
        text_color="#ffffff",
        command=_close_all_and_stop,
    )
    close_button.pack(side="right")

    ctk.CTkButton(btns, text=" Stop only",
        height=36, corner_radius=8, width=110,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=_stop_only,
    ).pack(side="right", padx=(0, 8))

    ctk.CTkButton(btns, text="Refresh",
        height=36, corner_radius=8, width=84,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=_refresh_prices_fut,
    ).pack(side="right", padx=(0, 4))

    ctk.CTkButton(btns, text="Cancel",
        height=36, corner_radius=8, width=90,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=_cancel,
    ).pack(side="right", padx=(0, 8))


def _quick_close_bool_or_none(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"true", "1", "yes", "on", "y", "t"}:
            return True
        if s in {"false", "0", "no", "off", "n", "f"}:
            return False
    return None


def _quick_close_is_live(app, name: str) -> bool:
    """Fail closed to LIVE when the launcher cannot prove SIM mode."""
    try:
        runtime_sim = app._runtime_sim_for_running_bot(name)
        parsed = _quick_close_bool_or_none(runtime_sim)
        if parsed is not None:
            return not parsed
    except Exception:
        pass
    try:
        bot = app.bots.get(name)
        start_cfg = getattr(bot, "start_config", None) if bot is not None else None
        if isinstance(start_cfg, dict) and "SIMULATION" in start_cfg:
            parsed = _quick_close_bool_or_none(start_cfg.get("SIMULATION"))
            return True if parsed is None else not parsed
    except Exception:
        pass
    try:
        parsed = _quick_close_bool_or_none(app.config[name].get("SIMULATION"))
        return True if parsed is None else not parsed
    except Exception:
        return True


def _confirm_quick_close_if_live(app, name: str) -> bool:
    if not _quick_close_is_live(app, name):
        return True
    try:
        from tkinter import messagebox
        return bool(messagebox.askyesno(
            "Confirm LIVE close",
            f"{name} is in LIVE mode. Close all open positions and stop the bot?",
            parent=app,
        ))
    except Exception:
        return False


def emergency_close_futures_and_stop(app, name: str) -> None:
    """Close futures positions via graceful bot shutdown after LIVE confirm."""
    from launcher.core.bot_controller import async_close_and_stop_futures

    card = app.cards[name]
    in_flight = getattr(app, "_quick_close_in_progress", set())
    if not isinstance(in_flight, set):
        try:
            in_flight = set(in_flight or ())
        except TypeError:
            in_flight = set()
    if name in in_flight:
        log_to_card(card, "warn", f"{name}: close already in progress")
        return
    if not _confirm_quick_close_if_live(app, name):
        log_to_card(card, "system", f"{name}: close cancelled")
        return
    in_flight.add(name)
    app._quick_close_in_progress = in_flight

    def _worker(update):
        try:
            async_close_and_stop_futures(
                app, name, card, update, reason="Manual Close & Stop")
        finally:
            try:
                in_flight.discard(name)
            except Exception:
                pass

    try:
        show_busy_dialog(
            app,
            f"Stopping {name}",
            "Closing positions and shutting down",
            _worker,
        )
    except Exception:
        in_flight.discard(name)
        raise


#  Emergency-close (no confirmation) 

def emergency_close_futures(app) -> None:
    """Red 'Quick Close' button in the FUTURES card header.

    No confirmation dialog  in an emergency an extra click costs precious
    seconds, so the button fires straight into the close workflow. The
    only dialog shown is the lightweight, non-blocking busy-dialog with
    live progress (which positions closed, what PnL was locked in); other
    bot cards stay operable.
    """
    from launcher.core.bot_controller import async_emergency_close

    card = app.cards["FUTURES"]
    bot_was_running = app.bots["FUTURES"].is_running()

    show_busy_dialog(
        app,
        "Quick Close",
        "Terminating bot and closing all positions",
        lambda update: async_emergency_close(
            app, card, bot_was_running, update),
    )


#  Quit dialogs 

def show_quit_no_positions_dialog(app) -> None:
    """Quit dialog when bots are running but no open positions exist."""
    dlg = ctk.CTkToplevel(app)
    dlg.title("Quit")
    dlg.configure(fg_color=COLORS["panel"])
    dlg.grab_set()
    dlg.transient(app)
    force_dark_titlebar(dlg)
    safe_geometry(dlg, 440, 220, parent=app)

    ctk.CTkLabel(dlg, text="!",
                  font=ctk.CTkFont(app.mono_font, 28, "bold"),
                  text_color=COLORS["warning"]
                  ).pack(pady=(20, 4))
    ctk.CTkLabel(dlg, text="Bots are still running.",
                  font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                  text_color=COLORS["text"]
                  ).pack()
    ctk.CTkLabel(dlg, text="No open positions. What do you want to do?",
                  font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                  text_color=COLORS["text_dim"]
                  ).pack(pady=(4, 16))

    row = ctk.CTkFrame(dlg, fg_color="transparent")
    row.pack()

    def stop_quit():
        dlg.destroy()
        show_busy_dialog(
            app,
            "Quitting",
            "Stopping all bots and closing application",
            lambda update: async_stop_all_and_quit(app, update, close_positions=False),
        )

    def just_close():
        dlg.destroy()
        app._shutdown_clean()

    ctk.CTkButton(row, text="Stop & Quit",
                   fg_color=COLORS["danger"], hover_color="#dc2626",
                   text_color="#ffffff", width=180, height=36, corner_radius=8,
                   font=ctk.CTkFont(FONT_BODY, 13, "bold"),
                   command=stop_quit
                   ).pack(side="left", padx=6)
    ctk.CTkButton(row, text="Keep Running in Background",
                   fg_color="transparent", hover_color=COLORS["panel_hover"],
                   text_color=COLORS["text_dim"], width=220, height=36, corner_radius=8,
                   font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                   border_width=1, border_color=COLORS["border"],
                   command=just_close
                   ).pack(side="left", padx=6)


def show_quit_with_positions_dialog(app, open_summary: dict) -> None:
    """Quit dialog when bots have OPEN POSITIONS  three options:

    * Close All & Quit (graceful close every position, then exit)
    * Stop Without Closing (positions stay open on the exchange)
    * Cancel
    """
    calc_height = 300 + (len(open_summary) * 24)
    dlg_height = max(340, calc_height)

    dlg = ctk.CTkToplevel(app)
    dlg.title("Quit Application")
    dlg.configure(fg_color=COLORS["panel"])
    dlg.grab_set()
    dlg.transient(app)
    force_dark_titlebar(dlg)
    safe_geometry(dlg, 520, dlg_height, parent=app)

    ctk.CTkLabel(dlg, text=" Open Positions Detected",
                  font=ctk.CTkFont(FONT_BODY, 16, "bold"),
                  text_color=COLORS["warning"]
                  ).pack(pady=(20, 6), padx=24, anchor="w")

    def _summary_count(value) -> int:
        if isinstance(value, dict):
            try:
                return int(value.get("count", 0) or 0)
            except (TypeError, ValueError):
                return 0
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _summary_modes(value) -> set[str]:
        if not isinstance(value, dict):
            return set()
        raw = value.get("modes") or set()
        try:
            return {str(v).upper() for v in raw if str(v).strip()}
        except TypeError:
            return set()

    summary_text = "\n".join(
        f"  {bot}: {_summary_count(data)} open position(s)"
        for bot, data in open_summary.items()
    )
    ctk.CTkLabel(dlg, text=summary_text,
                  font=ctk.CTkFont(app.mono_font, 11, "bold"),
                  text_color=COLORS["text"], justify="left"
                  ).pack(padx=24, pady=(0, 12), anchor="w")

    any_live = any("LIVE" in _summary_modes(data)
                   for data in open_summary.values())
    if not any_live:
        any_live = any(
            not app.config[b].get("SIMULATION", True)
            for b, data in open_summary.items()
            if not _summary_modes(data)
        )
    warn_text = (
        " LIVE MODE detected. 'Close All' will place real market orders\n"
        "    on the exchange. Slippage applies."
        if any_live else
        "All affected bots are in SIMULATION mode. Closing only updates\n"
        "the database  no real orders are sent."
    )
    ctk.CTkLabel(dlg, text=warn_text,
                  font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                  text_color=(COLORS["danger"] if any_live else COLORS["text_subtle"]),
                  justify="left"
                  ).pack(padx=24, pady=(0, 16), anchor="w")

    ctk.CTkLabel(dlg, text="What do you want to do?",
                  font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                  text_color=COLORS["text_dim"]
                  ).pack(padx=24, pady=(0, 4), anchor="w")

    btns = ctk.CTkFrame(dlg, fg_color="transparent")
    btns.pack(side="bottom", fill="x", padx=20, pady=16)

    def close_all_and_quit():
        dlg.destroy()
        show_busy_dialog(
            app,
            "Quitting",
            "Closing all positions and shutting down",
            lambda update: async_stop_all_and_quit(app, update, close_positions=True),
        )

    def stop_no_close():
        dlg.destroy()
        show_busy_dialog(
            app,
            "Quitting",
            "Stopping bots (positions kept open)",
            lambda update: async_stop_all_and_quit(app, update, close_positions=False),
        )

    action_color = COLORS["danger"] if any_live else COLORS["warning"]
    ctk.CTkButton(btns,
        text=" Close All & Quit",
        height=36, corner_radius=8, width=170,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color=action_color, hover_color=COLORS["panel_hover"],
        text_color="#ffffff",
        command=close_all_and_quit
    ).pack(side="right")

    ctk.CTkButton(btns, text=" Stop without closing",
        height=36, corner_radius=8, width=170,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=stop_no_close
    ).pack(side="right", padx=(0, 8))

    ctk.CTkButton(btns, text="Cancel",
        height=36, corner_radius=8, width=90,
        font=ctk.CTkFont(FONT_BODY, 12, "bold"),
        fg_color="transparent", hover_color=COLORS["panel_hover"],
        text_color=COLORS["text_dim"],
        border_width=1, border_color=COLORS["border"],
        command=dlg.destroy
    ).pack(side="right", padx=(0, 8))


def async_stop_all_and_quit(app, update, close_positions: bool) -> None:
    """Keep every running bot restart-blocked through quit cleanup."""
    barriers = ExitStack()
    transferred_to_shutdown = False
    try:
        for bot_name in BOT_ORDER:
            barriers.enter_context(
                app.bots[bot_name].exclusive_stop_operation()
            )
        running_bots = [b for b in BOT_ORDER if app.bots[b].is_running()]

        if not running_bots and not close_positions:
            update("No bots running. Closing")
            shutdown_delay_ms = 300
        else:
            shutdown_ready = _async_stop_all_and_quit_owned(
                app,
                update,
                close_positions,
                running_bots,
            )
            if not shutdown_ready:
                return
            shutdown_delay_ms = 500

        def _shutdown_with_barrier_release() -> None:
            try:
                app._shutdown_clean()
            finally:
                barriers.close()

        _post_ui(
            app,
            _shutdown_with_barrier_release,
            delay_ms=shutdown_delay_ms,
        )
        transferred_to_shutdown = True
    finally:
        if not transferred_to_shutdown:
            barriers.close()


def _async_stop_all_and_quit_owned(
    app,
    update,
    close_positions: bool,
    running_bots: list[str],
) -> bool:
    """Worker: stop all running bots, optionally close their positions,
    then exit the application.

    The signal sent to each bot matches the user's intent:
      close_positions=True  graceful (SIGTERM/CTRL_BREAK_EVENT) 
                                bot's handler runs emergency_close_all
                                positions closed with PnL recorded.
      close_positions=False  hard kill (TerminateProcess/SIGKILL) 
                                bot dies instantly, signal handler NEVER
                                runs, positions stay open on exchange.
    """
    from launcher.core.positions import (
        direct_close_remaining_futures,
        direct_close_remaining_spot,
    )
    from launcher.core.bot_controller import (
        _release_dead_process_close_locks,
        _stop_bot_process_verified,
    )

    # Pick signal type based on user intent.
    if close_positions:
        update("Sending graceful shutdown  bots will close positions")
    else:
        update("Hard-stopping bots  positions will stay OPEN on exchange")

    stop_threads: list[threading.Thread] = []
    stop_errors: list[str] = []
    stop_errors_lock = threading.Lock()

    def _stop_worker(bot_name: str, graceful: bool):
        try:
            stopped_pid = _stop_bot_process_verified(
                app.bots[bot_name], graceful_close=graceful
            )
            _release_dead_process_close_locks(stopped_pid)
        except Exception as e:
            with stop_errors_lock:
                stop_errors.append(f"{bot_name}: {e}")
            import sys as _sys
            stderr = _sys.stderr
            if stderr is not None:
                try:
                    stderr.write(
                        f"[Quit] stop {bot_name} "
                        f"(graceful={graceful}) failed: {e}\n"
                    )
                except Exception:
                    pass

    for bot in running_bots:
        t = threading.Thread(target=_stop_worker,
                              args=(bot, close_positions), daemon=True)
        try:
            t.start()
        except Exception as exc:
            # Earlier stop workers already own live process mutations. Keep
            # every restart barrier until those workers have been joined; a
            # later Thread.start failure must never unwind the outer ExitStack
            # while an earlier stop is still in progress.
            with stop_errors_lock:
                stop_errors.append(
                    f"{bot}: stop worker start failed: {exc}"
                )
            try:
                if t.is_alive():
                    stop_threads.append(t)
            except Exception:
                pass
        else:
            stop_threads.append(t)

    for t in stop_threads:
        t.join()

    if stop_errors:
        update("Stop failed - bot is still running; application left open")
        return False

    # Fallback close path: ONLY when user WANTS positions closed.
    # The bot's handler may have failed (network glitch during close)
    #  direct_close_remaining_* is the second-chance net.
    if close_positions:
        update("Verifying and closing remaining positions")
        close_threads: list[threading.Thread] = []
        close_errors: list[str] = []
        close_errors_lock = threading.Lock()

        def _close_worker(bot_name: str):
            try:
                card = app.cards[bot_name]
                from launcher.core.bot_controller import close_modes_for_stop
                close_modes = close_modes_for_stop(bot_name)

                def _log(severity, msg, _c=card):
                    _post_ui(app, lambda: log_to_card(_c, severity, msg))

                # CROSS is futures-type (is_futures=True)  use the futures
                # close path so realized PnL is booked and futures_state cleared.
                from launcher.config.settings import BOT_META as _BM
                for sim_only in close_modes:
                    if _BM[bot_name].get("is_futures"):
                        result = direct_close_remaining_futures(
                            _log, sim_only, reason="Application Quit", bot_name=bot_name)
                    else:
                        result = direct_close_remaining_spot(
                            bot_name, _log, sim_only, reason="Application Quit")
                    failed = []
                    if isinstance(result, dict):
                        failed = list(result.get("failed") or [])
                    if failed:
                        raise RuntimeError(
                            f"fallback close failed for {', '.join(map(str, failed))}")
            except Exception as e:
                with close_errors_lock:
                    close_errors.append(f"{bot_name}: {e}")
                import sys as _sys
                stderr = _sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(
                            f"[Quit] direct close {bot_name} failed: {e}\n"
                        )
                    except Exception:
                        pass

        for bot in BOT_ORDER:
            t = threading.Thread(target=_close_worker, args=(bot,), daemon=True)
            try:
                t.start()
            except Exception as exc:
                # Preserve the same barrier-ownership contract during the
                # fallback-close phase. Join any worker that did start before
                # reporting failure and leaving the application open.
                with close_errors_lock:
                    close_errors.append(
                        f"{bot}: close worker start failed: {exc}"
                    )
                try:
                    if t.is_alive():
                        close_threads.append(t)
                except Exception:
                    pass
            else:
                close_threads.append(t)

        for t in close_threads:
            t.join()
        if close_errors:
            update("Close verification failed  application left open")
            try:
                from tkinter import messagebox
                _post_ui(app, lambda: messagebox.showerror(
                    "Close verification failed",
                    "Some positions could not be verified/closed:\n"
                    + "\n".join(close_errors[:8])
                    + "\n\nApplication was not closed."
                ))
            except Exception:
                pass
            return False

    process_errors: list[str] = []
    for bot_name in BOT_ORDER:
        try:
            if app.bots[bot_name].is_running():
                process_errors.append(bot_name)
        except Exception:
            process_errors.append(f"{bot_name} (unverifiable)")
    if process_errors:
        update("Shutdown aborted - a bot process is still running")
        return False

    msg = ("All bots stopped  positions preserved." if not close_positions
            else "All bots stopped and positions closed.")
    update(f"{msg} Closing application")
    return True
