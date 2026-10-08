"""Process identity helpers for bot runtime checks.

PID liveness alone is not enough on Windows because PIDs can be reused quickly.
These helpers only treat a process as one of our bots when the command line
also references this project and the bot's module or script path.
"""
from __future__ import annotations

import os
import ntpath
import re
import shlex
import subprocess


def _valid_pid(pid: object) -> bool:
    return (
        isinstance(pid, int)
        and not isinstance(pid, bool)
        and 0 < pid <= 0xFFFFFFFF
    )


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
    if not _valid_pid(pid):
        return False
    try:
        import psutil  # type: ignore

        try:
            if not psutil.pid_exists(pid):
                return False
            process = psutil.Process(pid)
            if not process.is_running():
                return False
            return process.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False
        except Exception:
            return True
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
    if not _valid_pid(pid):
        return ""
    try:
        import psutil  # type: ignore
        proc = psutil.Process(pid)
        return subprocess.list2cmdline(proc.cmdline() or [])
    except Exception:
        return ""


def pid_cwd(pid: int) -> str:
    if not _valid_pid(pid):
        return ""
    try:
        import psutil  # type: ignore
        return str(psutil.Process(pid).cwd() or "")
    except Exception:
        return ""


def _norm(text: str) -> str:
    return str(text or "").replace("\\", "/").lower()


def _unquote_cmdline_token(token: str) -> str:
    if (
        len(token) >= 2
        and token[0] == token[-1]
        and token[0] in {'"', "'"}
    ):
        return token[1:-1]
    return token


def _python_invocation_target(cmdline: str) -> tuple[str, str]:
    try:
        tokens = [
            _unquote_cmdline_token(token)
            for token in shlex.split(cmdline, posix=False)
        ]
    except (TypeError, ValueError):
        return "", ""
    kind, target, _arguments = python_invocation_from_argv(tokens)
    return kind, target


def python_invocation_from_argv(argv) -> tuple[str, str, tuple[str, ...]]:
    """Parse the actual Python target without joining/loss of argv boundaries."""
    if not isinstance(argv, (list, tuple)) or any(not isinstance(t, str) for t in argv):
        return "", "", ()
    tokens = [_unquote_cmdline_token(token) for token in argv]
    if len(tokens) < 2:
        return "", "", ()
    executable_name = _norm(tokens[0]).rsplit("/", 1)[-1]
    if not re.fullmatch(
        r"(?:python(?:w|\d+(?:\.\d+)*)?|pypy\d*|pyw?)(?:\.exe)?",
        executable_name,
    ):
        return "", "", ()
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token in {
            "-?",
            "-h",
            "--help",
            "--help-env",
            "--help-xoptions",
            "--help-all",
            "-0",
            "-0p",
            "--list",
            "--list-paths",
            "-V",
            "-VV",
            "--version",
        }:
            return "", "", ()
        if token == "-m":
            if index + 1 >= len(tokens):
                return "", "", ()
            return "module", _norm(tokens[index + 1]), tuple(tokens[index + 2:])
        if token == "-c":
            return "", "", ()
        if token == "--":
            index += 1
            if index >= len(tokens):
                return "", "", ()
            return "script", _norm(tokens[index]), tuple(tokens[index + 1:])
        if token == "-":
            return "", "", ()
        if token == "--check-hash-based-pycs":
            if (
                index + 1 >= len(tokens)
                or tokens[index + 1] not in {"always", "default", "never"}
            ):
                return "", "", ()
            index += 2
            continue
        if token.startswith("-"):
            index += 2 if token in {"-W", "-X"} else 1
            continue
        return "script", _norm(token), tuple(tokens[index + 1:])
    return "", "", ()


def commandline_argv(commandline: str) -> tuple[str, ...]:
    """Decode a Windows-style command line without executing or expanding it."""
    try:
        return tuple(_unquote_cmdline_token(token)
                     for token in shlex.split(commandline, posix=False))
    except (TypeError, ValueError):
        raise ValueError("process command line cannot be parsed") from None


def script_target_scope(target: str, relative_target: str, root, cwd=None) -> str:
    """Attribute one actual script path; unknown cwd is never foreign/flat proof."""
    root_path = ntpath.normcase(ntpath.normpath(str(root)))
    expected = ntpath.normcase(ntpath.normpath(ntpath.join(root_path, relative_target)))
    target_path = ntpath.normcase(ntpath.normpath(target))
    if ntpath.isabs(target_path):
        # A rooted path without drive depends on the process's current drive.
        if ntpath.splitdrive(root_path)[0] and not ntpath.splitdrive(target_path)[0]:
            if not cwd or not ntpath.splitdrive(str(cwd))[0]:
                return "unknown"
            target_path = ntpath.splitdrive(str(cwd))[0] + target_path
        return "root" if target_path == expected else "foreign"
    if ntpath.splitdrive(target_path)[0] or not cwd or not ntpath.isabs(str(cwd)):
        return "unknown"
    actual = ntpath.normcase(ntpath.normpath(ntpath.join(str(cwd), target_path)))
    return "root" if actual == expected else "foreign"


def dashboard_process_scope(argv, root, cwd=None) -> str | None:
    """Recognize only Python's real Streamlit run target, then bind its root."""
    kind, target, arguments = python_invocation_from_argv(argv)
    if kind != "module" or target != "streamlit" or len(arguments) < 2 or arguments[0] != "run":
        return None
    script = _norm(arguments[1])
    normalized = ntpath.normcase(ntpath.normpath(script))
    relative = ntpath.normcase(ntpath.normpath("tools/dashboard.py"))
    if normalized != relative and not normalized.endswith("\\" + relative):
        return None
    return script_target_scope(script, "tools/dashboard.py", root, cwd)


def bot_process_scope(argv, root, bot_meta: dict, cwd=None) -> str | None:
    """Conservative bot-module barrier and exact root attribution for scripts."""
    kind, target, _arguments = python_invocation_from_argv(argv)
    if kind == "module" and any(target == _norm(meta.get("module")) for meta in bot_meta.values()):
        return "root"
    if kind == "script":
        normalized = ntpath.normcase(ntpath.normpath(target))
        for meta in bot_meta.values():
            relative = str(meta.get("script") or "")
            expected = ntpath.normcase(ntpath.normpath(relative))
            if relative and (normalized == expected or normalized.endswith("\\" + expected)):
                return script_target_scope(target, relative, root, cwd)
    return None


def cmdline_bot_match_kind(bot_name: str, cmdline: str) -> str:
    """Return the exact bot invocation kind: ``module``, ``script`` or empty."""
    if not cmdline:
        return ""
    try:
        from launcher.config.settings import BOT_META
        meta = BOT_META.get(str(bot_name or "").upper()) or {}
    except Exception:
        return ""
    module = _norm(meta.get("module") or "")
    script = _norm(meta.get("script") or "")
    invocation_kind, target = _python_invocation_target(cmdline)
    if invocation_kind == "module" and target == module:
        return "module"
    script_seen = bool(
        invocation_kind == "script"
        and (
            target == script
            or target.endswith("/" + script)
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
