"""
launcher.main  entry point for the Obsidian Trading Terminal.

This used to live at the bottom of ``launcher.pyw`` as the ``__main__``
block. Pulling it into its own module makes it both invokable as
``python -m launcher`` and importable from the thin ``launcher.pyw``
bootstrap at the project root.

Responsibilities:

1. Run the first-time setup wizard if there is no ``.env`` file yet.
2. Initialize the SQLite schema (idempotent, safe to call on every start).
3. Construct the :class:`ObsidianApp` and enter its Tk main loop.
"""

from __future__ import annotations

import os
import subprocess
import sys

from launcher.config.settings import PROJECT_ROOT, _get_python_exe


def _ensure_std_streams() -> None:
    """Ensure ``sys.stdout``/``sys.stderr`` exist and cap the stdio log.

    Under ``pythonw.exe`` there is no console, so ``sys.stderr`` and
    ``sys.stdout`` are ``None``  any later ``sys.stderr.write(...)`` would
    crash. This runs BEFORE anything else that might log and points both
    streams at ``logs/launcher_stdio.log``, rotating that file to ``.1``
    once it grows past 10 MB (best-effort rename on Windows, truncate on
    failure) so it can't fill the disk over a long-running session.
    """
    import io

    from launcher.config.settings import PROJECT_ROOT

    log_dir = os.path.join(PROJECT_ROOT, "logs")
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception:
        log_dir = PROJECT_ROOT  # fallback if logs/ can't be created
    log_path = os.path.join(log_dir, "launcher_stdio.log")

    # rotate if oversized (keep one backup)
    _MAX_BYTES = 10 * 1024 * 1024  # 10 MB
    try:
        if os.path.exists(log_path) and os.path.getsize(log_path) > _MAX_BYTES:
            backup = log_path + ".1"
            try:
                if os.path.exists(backup):
                    os.remove(backup)
            except OSError:
                pass
            try:
                os.rename(log_path, backup)
            except OSError:
                # Windows: file may still be held by an old python process.
                # Truncate instead of rename as a fallback.
                try:
                    with open(log_path, "w"):
                        pass
                except OSError:
                    pass
    except OSError:
        pass

    def _open_fallback():
        try:
            # Line-buffered, append, UTF-8 with error replacement so a
            # bad byte in an upstream exception message never poisons
            # the stream.
            return open(log_path, "a", buffering=1,
                          encoding="utf-8", errors="replace")
        except Exception:
            return io.StringIO()  # last-resort in-memory sink

    if sys.stderr is None:
        sys.stderr = _open_fallback()
    if sys.stdout is None:
        sys.stdout = _open_fallback()


def _relaunch_windowless() -> bool:
    """On Windows, if the launcher was started under console ``python.exe``,
    relaunch it under ``pythonw.exe`` (no console) and return True so the caller
    exits. Guarantees no CMD window stays open regardless of HOW it was started
    (a .bat fallback, a broken ``.pyw`` file association, or ``python
    launcher.pyw``). Set ``OBSIDIAN_NO_REEXEC=1`` to keep the console for
    debugging."""
    if sys.platform != "win32":
        return False
    if os.environ.get("OBSIDIAN_NO_REEXEC") == "1":
        return False
    exe = sys.executable or ""
    if os.path.basename(exe).lower() != "python.exe":
        return False  # already pythonw / frozen  leave it
    pythonw = os.path.join(os.path.dirname(exe), "pythonw.exe")
    entry = os.path.join(PROJECT_ROOT, "launcher.pyw")
    if not (os.path.isfile(pythonw) and os.path.isfile(entry)):
        return False  # can't relaunch  run as-is
    try:
        subprocess.Popen(
            [pythonw, entry], cwd=PROJECT_ROOT,
            creationflags=subprocess.CREATE_NO_WINDOW, close_fds=True,
            env=dict(os.environ, OBSIDIAN_NO_REEXEC="1"))
        return True
    except Exception:
        return False


def main() -> None:
    # No console window, however we were launched (relaunch under pythonw).
    if _relaunch_windowless():
        return

    # Must run BEFORE anything else that might log.
    _ensure_std_streams()

    # Anchor cwd to the project root so any legacy code that expects
    # `os.getcwd()` to be the launcher dir keeps working. The new
    # modules all use PROJECT_ROOT explicitly, so this is just belt &
    # braces for back-compat with the bots themselves.
    os.chdir(PROJECT_ROOT)

    env_path = os.path.join(PROJECT_ROOT, ".env")
    wizard_path = os.path.join(PROJECT_ROOT, "setup_wizard.pyw")

    if not os.path.exists(env_path) and os.path.exists(wizard_path):
        kw = {}
        if sys.platform == "win32":
            kw["creationflags"] = subprocess.CREATE_NO_WINDOW
        subprocess.Popen(
            [_get_python_exe(), wizard_path],
            cwd=PROJECT_ROOT,
            **kw,
        )
        sys.exit(0)

    # Ensure the DB schema exists BEFORE the UI queries it. get_bot_stats()
    # and the dashboard read bot_open_positions / trades / daily_pnl; if no
    # bot has started yet AND the DB doesn't exist those reads return empty.
    # init_db() is idempotent + schema-locked, safe to call at startup.
    try:
        from core.database import init_db as _init_db  # type: ignore
        _init_db()
    except Exception as _e:
        # Non-fatal  launcher can still show the UI, individual bot
        # starts will re-trigger init_db() and may succeed there.
        print(f"[launcher] init_db at startup failed (non-fatal): {_e}")

    # Import the app last  it transitively imports customtkinter, which
    # has nontrivial startup cost and is unnecessary if we bail out
    # above into the setup wizard.
    from launcher.ui.app import ObsidianApp

    app = ObsidianApp()
    app.mainloop()


if __name__ == "__main__":
    main()
