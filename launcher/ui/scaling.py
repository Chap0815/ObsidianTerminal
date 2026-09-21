"""
Resolution-adaptive UI scaling for a scrolling, responsive card layout.

Fit a readable single-column layout horizontally. Short screens scroll
vertically instead of shrinking every label to fit all of the content.

DPI-safe: CTk already multiplies its widget factor by the OS display scaling.
Use DPI only to calculate the available logical area; do not multiply it into
the widget factor a second time. Window scaling remains at CTk's default.

Manual override: env ``UI_SCALE`` (e.g. ``UI_SCALE=0.85``) or config ``UI.SCALE``
sets CTk's widget factor when the automatic fit is unsuitable for a setup.
"""

from __future__ import annotations

import os

import customtkinter as ctk

# Comfortable logical width for the sidebar, toolbar and one card column.
DESIGN_W = 1280

# Preserve readable text on small screens; larger screens use the native size.
MIN_FIT = 0.85
MAX_FIT = 1.0

# Vertical/horizontal chrome not usable for the window (taskbar + titlebar).
SCREEN_MARGIN_H = 80
SCREEN_MARGIN_W = 16


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _dpi_scaling(window) -> float:
    """CTk's OS-DPI scaling for this window (1.0 at 100%, 1.5 at 150%, )."""
    try:
        dpi = float(ctk.ScalingTracker.get_window_dpi_scaling(window))
        return dpi if dpi > 0 else 1.0
    except Exception:
        return 1.0


def _override_scale(config: dict | None) -> float | None:
    """Absolute widget-scaling override from env UI_SCALE or config UI.SCALE."""
    raw = os.environ.get("UI_SCALE")
    if raw is None and config is not None:
        raw = config.get("UI", {}).get("SCALE")
    if raw in (None, "", 0):
        return None
    try:
        return _clamp(float(raw), 0.3, 2.0)
    except (TypeError, ValueError):
        return None


def compute_fit(logical_w: float, logical_h: float) -> float:
    """Fit horizontally; card wrapping and scrolling handle remaining space."""
    if logical_w <= 0 or logical_h <= 0:
        return MAX_FIT
    return _clamp(logical_w / DESIGN_W, MIN_FIT, MAX_FIT)


def apply_scaling(window, config: dict | None = None) -> tuple[float, int, int]:
    """Detect screen + DPI, apply CTk widget scaling, return (fit, win_w, win_h).

    win_w/win_h are LOGICAL geometry units (CTk multiplies them by the window's
    DPI scaling to get physical pixels), so they exactly fill the usable area at
    any DPI. window scaling is left at CTk's DPI default  never overridden.
    """
    window.update_idletasks()
    dpi = _dpi_scaling(window)
    screen_w = window.winfo_screenwidth()
    screen_h = window.winfo_screenheight()
    usable_w = max(640, screen_w - SCREEN_MARGIN_W)
    usable_h = max(480, screen_h - SCREEN_MARGIN_H)
    # Logical = physical / DPI  the coordinate space the layout is authored in.
    logical_w = usable_w / dpi
    logical_h = usable_h / dpi

    override = _override_scale(config)
    if override is not None:
        widget_scale = override
    else:
        widget_scale = compute_fit(logical_w, logical_h)

    # CTk applies OS DPI itself. Pass only the additional fit/override factor.
    ctk.set_widget_scaling(widget_scale)

    return widget_scale, int(logical_w), int(logical_h)
