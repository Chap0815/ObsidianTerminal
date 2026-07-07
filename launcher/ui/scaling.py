"""
Resolution-adaptive UI scaling.

The Obsidian layout uses fixed-pixel widgets sized for a 2K/4K design baseline
(~2200x1200 LOGICAL px). On smaller screens (1080p) that overflows and gets
clipped. This derives ONE extra CTk widget-scaling factor from the real screen
so every widget + font shrinks proportionally and the whole UI fits.

DPI-safe: CTk already auto-scales for the OS display scaling (a 4K screen at
150% renders bigger). We PRESERVE that  we never touch window scaling and we
fold the existing DPI factor into the widget scaling, then only shrink BELOW it
when the design baseline does not fit the logical usable area. So a 2K/4K screen
that already fit stays unchanged; only smaller screens shrink.

Manual override: env ``UI_SCALE`` (e.g. ``UI_SCALE=0.85``) or config ``UI.SCALE``
forces an absolute widget-scaling factor when auto-detection is off on a setup.
"""

from __future__ import annotations

import os

import customtkinter as ctk

# Design baseline (LOGICAL px) the fixed-pixel layout was authored against.
DESIGN_W = 2200
DESIGN_H = 1200

# Never shrink the layout below the design (4K/2K just stay crisp); only fit.
MIN_FIT = 0.55
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
    """Fraction of the design baseline that fits the logical usable area,
    clamped to [MIN_FIT, MAX_FIT]. 1.0 = design fits as-is (no shrink)."""
    if logical_w <= 0 or logical_h <= 0:
        return MAX_FIT
    return _clamp(min(logical_w / DESIGN_W, logical_h / DESIGN_H), MIN_FIT, MAX_FIT)


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
        widget_scale = dpi * compute_fit(logical_w, logical_h)

    # Fold DPI into widget scaling (preserve DPI comfort) and shrink to fit.
    # Window scaling deliberately untouched.
    ctk.set_widget_scaling(widget_scale)

    fit = widget_scale / dpi if dpi else widget_scale
    return fit, int(logical_w), int(logical_h)
