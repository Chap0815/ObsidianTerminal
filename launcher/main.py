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

from collections import deque
import io
import os
import shutil
import subprocess
import sys
import threading

_PREIMPORT_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PREIMPORT_PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PREIMPORT_PROJECT_ROOT)

from update_barrier import (  # noqa: E402 - pre-import root bootstrap above
    UpdateInProgressError,
    assert_process_start_allowed,
    process_start_guard,
    update_lifecycle_lock,
)
from env_setup_files import (  # noqa: E402 - pre-import root bootstrap above
    cleanup_stale_env_temps,
)

try:
    assert_process_start_allowed(_PREIMPORT_PROJECT_ROOT)
except UpdateInProgressError as exc:
    raise SystemExit(str(exc)) from exc

from core.constants import LAUNCHER_STDIO_MAX_BYTES  # noqa: E402 - barrier above
from launcher.config.settings import (  # noqa: E402 - barrier above
    PROJECT_ROOT,
    _get_python_exe,
)


class _BoundedTextStream:
    """Line-buffered UTF-8 stream with in-session size rotation.

    The open handle is truncated after copying its contents to ``.1``. This
    avoids Windows rename failures caused by trying to rotate our own open
    stdout/stderr handle.
    """

    encoding = "utf-8"
    errors = "replace"

    def __init__(self, path, *, max_bytes: int = LAUNCHER_STDIO_MAX_BYTES):
        self.name = os.fspath(path)
        self.mode = "a"
        self._max_bytes = max(1, int(max_bytes))
        self._lock = threading.RLock()
        self._fh = open(
            self.name,
            self.mode,
            buffering=1,
            encoding=self.encoding,
            errors=self.errors,
        )
        try:
            self._size = os.path.getsize(self.name)
        except OSError:
            self._size = 0

    @property
    def closed(self) -> bool:
        return self._fh.closed

    def _rotate_open_file(self) -> None:
        try:
            self._fh.flush()
        except OSError:
            pass
        try:
            shutil.copyfile(self.name, self.name + ".1")
        except OSError:
            pass
        try:
            self._fh.seek(0)
            self._fh.truncate(0)
            self._size = 0
        except OSError:
            try:
                self._size = os.path.getsize(self.name)
            except OSError:
                pass

    def write(self, value: str) -> int:
        if not isinstance(value, str):
            raise TypeError(f"write() argument must be str, not {type(value).__name__}")
        requested_chars = len(value)
        encoded = value.encode(self.encoding, errors=self.errors)
        truncated = len(encoded) > self._max_bytes
        if truncated:
            start = len(encoded) - self._max_bytes
            # Move to the next UTF-8 code-point boundary. Retaining the newest
            # complete suffix keeps the sink bounded without creating an
            # undecodable log file.
            while start < len(encoded) and encoded[start] & 0xC0 == 0x80:
                start += 1
            encoded = encoded[start:]
            value = encoded.decode(self.encoding)
        encoded_size = len(encoded)
        with self._lock:
            if self._size and self._size + encoded_size > self._max_bytes:
                self._rotate_open_file()
            try:
                written = self._fh.write(value)
                self._size += encoded_size
                return requested_chars if truncated else written
            except OSError:
                # A diagnostic sink must not crash the launcher on a full or
                # temporarily locked volume.
                return requested_chars

    def flush(self) -> None:
        with self._lock:
            try:
                self._fh.flush()
            except OSError:
                pass

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                try:
                    self._fh.close()
                except OSError:
                    pass

    def fileno(self) -> int:
        return self._fh.fileno()

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False


class _BoundedMemoryTextStream:
    """Last-resort text sink that retains only a bounded UTF-8 suffix."""

    encoding = "utf-8"
    errors = "replace"
    _COALESCE_MAX_BYTES = 64 * 1024

    def __init__(self, *, max_bytes: int = LAUNCHER_STDIO_MAX_BYTES):
        self._max_bytes = max(1, int(max_bytes))
        self._lock = threading.RLock()
        self._chunks: deque[bytes] = deque()
        self._size = 0
        self._closed = False

    @staticmethod
    def _complete_utf8_suffix(encoded: bytes, max_bytes: int) -> bytes:
        start = max(0, len(encoded) - max_bytes)
        while start < len(encoded) and encoded[start] & 0xC0 == 0x80:
            start += 1
        return encoded[start:]

    def write(self, value: str) -> int:
        if not isinstance(value, str):
            raise TypeError(f"write() argument must be str, not {type(value).__name__}")
        requested_chars = len(value)
        encoded = value.encode(self.encoding, errors=self.errors)
        with self._lock:
            if self._closed:
                raise ValueError("I/O operation on closed file")
            if len(encoded) >= self._max_bytes:
                retained = self._complete_utf8_suffix(encoded, self._max_bytes)
                self._chunks.clear()
                if retained:
                    self._chunks.append(retained)
                self._size = len(retained)
                return requested_chars
            if encoded:
                if (
                    self._chunks
                    and len(self._chunks[-1]) + len(encoded)
                    <= self._COALESCE_MAX_BYTES
                ):
                    self._chunks[-1] += encoded
                else:
                    self._chunks.append(encoded)
                self._size += len(encoded)
            while self._size > self._max_bytes and self._chunks:
                excess = self._size - self._max_bytes
                oldest = self._chunks[0]
                if excess >= len(oldest):
                    self._chunks.popleft()
                    self._size -= len(oldest)
                    continue
                cut = excess
                while cut < len(oldest) and oldest[cut] & 0xC0 == 0x80:
                    cut += 1
                if cut >= len(oldest):
                    self._chunks.popleft()
                else:
                    self._chunks[0] = oldest[cut:]
                self._size -= cut
        return requested_chars

    def getvalue(self) -> str:
        with self._lock:
            if self._closed:
                raise ValueError("I/O operation on closed file")
            return b"".join(self._chunks).decode(self.encoding)

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def flush(self) -> None:
        with self._lock:
            if self._closed:
                raise ValueError("I/O operation on closed file")

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._chunks.clear()
            self._size = 0

    def fileno(self) -> int:
        raise io.UnsupportedOperation("fileno")

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return not self.closed

    def seekable(self) -> bool:
        return False


def _ensure_std_streams() -> None:
    """Ensure ``sys.stdout``/``sys.stderr`` exist and cap the stdio log.

    Under ``pythonw.exe`` there is no console, so ``sys.stderr`` and
    ``sys.stdout`` are ``None``  any later ``sys.stderr.write(...)`` would
    crash. This runs BEFORE anything else that might log and points both
    streams at ``logs/launcher_stdio.log``. Startup rotation handles an old
    oversized file; the bounded stream also rotates during the same process,
    so one long-running launcher cannot grow the file indefinitely.
    """
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception:
        log_dir = PROJECT_ROOT  # fallback if logs/ can't be created
    log_path = os.path.join(log_dir, "launcher_stdio.log")

    # rotate if oversized (keep one backup)
    try:
        if (
            os.path.exists(log_path)
            and os.path.getsize(log_path) > LAUNCHER_STDIO_MAX_BYTES
        ):
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
            return _BoundedTextStream(log_path)
        except Exception:
            return _BoundedMemoryTextStream()

    if sys.stderr is None or sys.stdout is None:
        # One shared handle avoids Windows rotation/truncation races between
        # independently opened stdout and stderr streams.
        fallback = _open_fallback()
        if sys.stderr is None:
            sys.stderr = fallback
        if sys.stdout is None:
            sys.stdout = fallback


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
    spawned = None
    try:
        with process_start_guard(PROJECT_ROOT):
            spawned = subprocess.Popen(
                [pythonw, entry], cwd=PROJECT_ROOT,
                creationflags=subprocess.CREATE_NO_WINDOW, close_fds=True,
                env=dict(os.environ, OBSIDIAN_NO_REEXEC="1"))
        return True
    except UpdateInProgressError:
        # The updater can claim its lifecycle lock after the module-level
        # check.  Continuing the console parent here would bypass that late
        # barrier and start a launcher while product files are changing.
        raise
    except Exception:
        # If Popen succeeded but releasing the lifecycle lock failed, the
        # console parent must still exit. Continuing would leave two launchers.
        return spawned is not None


def _cleanup_setup_env_temps(project_root: str) -> None:
    """Recover secret setup temps even when a complete .env already exists."""
    with update_lifecycle_lock(project_root, timeout=15.0):
        cleanup_stale_env_temps(project_root)


def _initialize_database_for_launcher() -> None:
    """Prepare the schema without starting bot-owned DB worker threads."""
    from core.database import init_db as _init_db  # type: ignore
    _init_db(start_background_workers=False)
    from core.runtime_status import cleanup_runtime_status_temps
    cleanup_runtime_status_temps()


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

    try:
        _cleanup_setup_env_temps(PROJECT_ROOT)
    except Exception as exc:
        print(f"[launcher] private setup temp cleanup failed: {exc}")
        raise SystemExit(1) from exc

    env_path = os.path.join(PROJECT_ROOT, ".env")
    wizard_path = os.path.join(PROJECT_ROOT, "setup_wizard.pyw")

    if not os.path.lexists(env_path) and os.path.exists(wizard_path):
        kw = {}
        if sys.platform == "win32":
            kw["creationflags"] = subprocess.CREATE_NO_WINDOW
        with process_start_guard(PROJECT_ROOT):
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
        _initialize_database_for_launcher()
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
    if not bool(getattr(app, "_clean_shutdown_completed", False)):
        raise SystemExit(
            "launcher mainloop ended without verified clean shutdown"
        )


if __name__ == "__main__":
    main()
