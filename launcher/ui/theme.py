"""
UI theme helpers that don't belong to a specific widget.
"""

from __future__ import annotations

import sys


def force_dark_titlebar(window) -> None:
    """Force the Windows window frame (titlebar) into dark mode.

    ``DWMWA_USE_IMMERSIVE_DARK_MODE``:
      * Attribute 20  Windows 11 and Windows 10 20H1+ (build 19041+)
      * Attribute 19  Windows 10 pre-20H1 (undocumented preview value)

    Both calls are intentional. On Win11 attr 19 is a no-op (harmless).
    On Win10, attr 20 raises ``OSError`` ("attribute unknown"), so the two
    calls sit in independent try/except blocks to ensure attr 19 always
    executes regardless of whether attr 20 succeeded.

    No-op on Linux/Mac.
    """
    try:
        if sys.platform != "win32":
            return
        import ctypes  # local import: Linux/Mac don't have windll
        window.update_idletasks()
        hwnd = ctypes.windll.user32.GetParent(window.winfo_id())
        val  = ctypes.c_int(1)
        # Win11 / Win10 20H1+ (DWMWA_USE_IMMERSIVE_DARK_MODE = 20)
        try:
            ctypes.windll.dwmapi.DwmSetWindowAttribute(
                hwnd, 20, ctypes.byref(val), ctypes.sizeof(val))
        except OSError:
            pass  # attr doesn't exist on pre-20H1 Win10  fall through
        # Win10 pre-20H1 (undocumented preview value = 19)
        try:
            ctypes.windll.dwmapi.DwmSetWindowAttribute(
                hwnd, 19, ctypes.byref(val), ctypes.sizeof(val))
        except OSError:
            pass  # no-op on Win11 where attr 19 is superseded by 20
    except Exception:
        pass
