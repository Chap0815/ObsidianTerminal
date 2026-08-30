"""Process identity helpers for bot runtime checks.

PID liveness alone is not enough on Windows because PIDs can be reused quickly.
These helpers only treat a process as one of our bots when the command line
also references this project and the bot's module or script path.
"""
from __future__ import annotations

import os
import re
from pathlib import Path


def _windows_pid_alive(pid: int) -> bool:
    """Probe a Windows PID without sending a console control signal.

    ``os.kill(pid, 0)`` is not a POSIX-style existence probe on Windows.  A
    failed query is therefore treated as alive unless Windows explicitly says
    that the PID is invalid; uncertainty must block lifecycle actions.
    """
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        open_process.restype = wintypes.HANDLE
        get_exit_code = kernel32.GetExitCodeProcess
        get_exit_code.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        get_exit_code.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = (wintypes.HANDLE,)
        close_handle.restype = wintypes.BOOL

        handle = open_process(0x1000, False, pid)
        if not handle:
            # ERROR_INVALID_PARAMETER is Windows' documented result for a PID
            # that does not identify a process.  Access denied and all other
            # failures remain conservatively alive/unknown.
            return ctypes.get_last_error() != 87
        try:
            exit_code = wintypes.DWORD()
            if not get_exit_code(handle, ctypes.byref(exit_code)):
                return True
            return exit_code.value == 259  # STILL_ACTIVE
        finally:
            close_handle(handle)
    except Exception:
        return True


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil  # type: ignore
        return psutil.pid_exists(pid)
    except Exception:
        pass
    if os.name == "nt":
        return _windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False
    except Exception:
        return False


def pid_cmdline(pid: int) -> str:
    if pid <= 0:
        return ""
    try:
        import psutil  # type: ignore
        proc = psutil.Process(pid)
        return " ".join(proc.cmdline() or [])
    except Exception:
        return ""


def pid_cwd(pid: int) -> str:
    if pid <= 0:
        return ""
    try:
        import psutil  # type: ignore
        return str(psutil.Process(pid).cwd() or "")
    except Exception:
        return ""


def _norm(text: str) -> str:
    return str(text or "").replace("\\", "/").lower()


def _module_token_seen(cmdline: str, module: str) -> bool:
    if not module:
        return False
    escaped = re.escape(module)
    return bool(
        re.search(
            rf"(?:^|\s)-m\s+(?:\"{escaped}\"|'{escaped}'|{escaped})(?=\s|$)",
            cmdline,
        )
    )


def _script_token_seen(cmdline: str, script: str) -> bool:
    if not script:
        return False
    return bool(
        re.search(
            rf"(?:^|[/\s\"']){re.escape(script)}(?=$|[\s\"'])",
            cmdline,
        )
    )


def cmdline_bot_match_kind(bot_name: str, cmdline: str) -> str:
    """Return the exact bot invocation kind: ``module``, ``script`` or empty."""
    if not cmdline:
        return ""
    try:
        from launcher.config.settings import BOT_META, PROJECT_ROOT
        meta = BOT_META.get(str(bot_name or "").upper()) or {}
    except Exception:
        return ""
    low = _norm(cmdline)
    module = _norm(meta.get("module") or "")
    script = _norm(meta.get("script") or "")
    script_name = _norm(Path(script).name) if script else ""
    if _module_token_seen(low, module):
        return "module"
    root = _norm(str(PROJECT_ROOT))
    root_seen = bool(root and root in low)
    script_seen = bool(
        _script_token_seen(low, script)
        or (
            _script_token_seen(low, script_name)
            and (root_seen or "/bots/" in low)
        )
    )
    return "script" if script_seen else ""


def cmdline_matches_bot(bot_name: str, cmdline: str) -> bool:
    return bool(cmdline_bot_match_kind(bot_name, cmdline))


def pid_matches_bot(pid: int, bot_name: str) -> bool:
    if not pid_alive(pid):
        return False
    cmdline = pid_cmdline(pid)
    match_kind = cmdline_bot_match_kind(bot_name, cmdline)
    if not match_kind:
        return False
    try:
        from launcher.config.settings import PROJECT_ROOT

        raw_process_root = pid_cwd(pid)
        if not raw_process_root:
            return False
        process_root = _norm(os.path.abspath(raw_process_root)).rstrip("/")
        expected_root = _norm(os.path.abspath(PROJECT_ROOT)).rstrip("/")
        return bool(process_root and process_root == expected_root)
    except Exception:
        return False
