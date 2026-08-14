"""Launcher-owned analysis-tool process lifecycle.

Tool subprocesses are long lived and may keep Python modules or native wheels
open on Windows.  The launcher owns them centrally so update shutdown can reap
every child even when the originating dialog is merely hidden.
"""
from __future__ import annotations

import hashlib
import os
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Sequence


KNOWN_TOOL_MODULES = (
    "tools.backtester",
    "tools.futures_capture_phase2",
    "tools.optimizer",
    "tools.selftest",
    "tools.trend_check",
    "tools.trend_leverage_check",
    "tools.xsec_momentum",
)
_KNOWN_TOOL_MODULE_SET = frozenset(KNOWN_TOOL_MODULES)
_OPTIONS_WITH_SEPARATE_VALUE = frozenset({"-W", "-X"})


def tool_root_xoption(root: str | os.PathLike[str]) -> str:
    """Return a non-secret command-line root marker for CIM attribution."""
    normalized = os.path.normcase(str(Path(root).resolve(strict=False))).replace(
        "\\", "/"
    )
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:20]
    return f"obsidian_tool_root={digest}"


def _strip_token_quotes(value: Any) -> str:
    argument = str(value or "").strip()
    if (
        len(argument) >= 2
        and argument[0] == argument[-1]
        and argument[0] in {'"', "'"}
    ):
        return argument[1:-1]
    return argument


def _module_from_script_token(token: str) -> str | None:
    normalized = token.replace("\\", "/").lower()
    for module in KNOWN_TOOL_MODULES:
        suffix = module.replace(".", "/") + ".py"
        if normalized == suffix or normalized.endswith("/" + suffix):
            return module
    return None


def _tool_invocation_from_argv(
    argv: Sequence[Any] | None,
) -> tuple[str, str, str | None] | None:
    if not isinstance(argv, (list, tuple)) or len(argv) < 2:
        return None
    index = 1
    while index < len(argv):
        argument = _strip_token_quotes(argv[index])
        if argument == "-m":
            if index + 1 >= len(argv):
                return None
            module = _strip_token_quotes(argv[index + 1]).lower()
            if module in _KNOWN_TOOL_MODULE_SET:
                return ("module", module, None)
            return None
        if argument == "-c":
            return None
        if argument in _OPTIONS_WITH_SEPARATE_VALUE:
            index += 2
            continue
        if argument.startswith("-"):
            index += 1
            continue
        module = _module_from_script_token(argument)
        if module is None:
            return None
        return ("script", module, argument)
    return None


def tool_module_from_argv(argv: Sequence[Any] | None) -> str | None:
    """Parse a Python argv without matching strings passed to ``-c``."""
    invocation = _tool_invocation_from_argv(argv)
    return invocation[1] if invocation is not None else None


def _commandline_argv(commandline: str) -> list[str] | None:
    try:
        return shlex.split(commandline, posix=False)
    except (TypeError, ValueError):
        return None


def tool_module_from_commandline(commandline: str) -> str | None:
    """Extract an exact known ``python -m`` or tool-script invocation."""
    if not isinstance(commandline, str) or not commandline.strip():
        return None
    return tool_module_from_argv(_commandline_argv(commandline))


def _normalized_absolute_path(path: str | os.PathLike[str]) -> str:
    return os.path.normcase(str(Path(path).resolve(strict=False)))


def argv_has_root_tool_script(
    argv: Sequence[Any] | None,
    root: str | os.PathLike[str],
) -> bool:
    invocation = _tool_invocation_from_argv(argv)
    if invocation is None or invocation[0] != "script" or invocation[2] is None:
        return False
    script_path = Path(invocation[2])
    if not script_path.is_absolute():
        return False
    expected = Path(root) / (invocation[1].replace(".", "/") + ".py")
    return _normalized_absolute_path(script_path) == _normalized_absolute_path(expected)


def argv_has_absolute_tool_script(argv: Sequence[Any] | None) -> bool:
    invocation = _tool_invocation_from_argv(argv)
    return bool(
        invocation is not None
        and invocation[0] == "script"
        and invocation[2] is not None
        and Path(invocation[2]).is_absolute()
    )


def commandline_has_root_tool_script(
    commandline: str,
    root: str | os.PathLike[str],
) -> bool:
    if not isinstance(commandline, str) or not commandline.strip():
        return False
    return argv_has_root_tool_script(_commandline_argv(commandline), root)


def commandline_has_absolute_tool_script(commandline: str) -> bool:
    if not isinstance(commandline, str) or not commandline.strip():
        return False
    return argv_has_absolute_tool_script(_commandline_argv(commandline))


def commandline_has_tool_root_marker(
    commandline: str,
    root: str | os.PathLike[str],
) -> bool:
    argv = _commandline_argv(commandline)
    if argv is None:
        return False
    marker = tool_root_xoption(root).lower()
    for index, raw_token in enumerate(argv[1:], start=1):
        argument = _strip_token_quotes(raw_token)
        if argument == "-c" or argument == "-m":
            return False
        if argument == "-X" and index + 1 < len(argv):
            if _strip_token_quotes(argv[index + 1]).lower() == marker:
                return True
        if argument.lower() == "-x" + marker:
            return True
    return False


def guard_tool_entrypoint(file_path: str, module_name: str) -> None:
    """Block a direct tool entrypoint before it imports project/runtime code."""
    if module_name != "__main__":
        return
    root = str(Path(file_path).resolve(strict=False).parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)
    from update_barrier import UpdateInProgressError, assert_process_start_allowed

    try:
        assert_process_start_allowed(root)
    except UpdateInProgressError as exc:
        raise SystemExit(str(exc)) from exc


def _close_process_streams(proc: Any) -> None:
    seen: set[int] = set()
    for name in ("stdin", "stdout", "stderr"):
        stream = getattr(proc, name, None)
        if stream is None or id(stream) in seen:
            continue
        seen.add(id(stream))
        try:
            stream.close()
        except Exception:
            pass


def _alive(proc: Any) -> bool:
    try:
        return proc.poll() is None
    except Exception:
        return True


def _wait_group(processes: Iterable[Any], timeout: float) -> list[Any]:
    deadline = time.monotonic() + max(0.0, float(timeout))
    remaining: list[Any] = []
    for proc in processes:
        if not _alive(proc):
            try:
                proc.wait(timeout=0)
            except Exception:
                pass
            continue
        wait_for = max(0.0, deadline - time.monotonic())
        try:
            proc.wait(timeout=wait_for)
        except (subprocess.TimeoutExpired, TimeoutError):
            remaining.append(proc)
        except Exception:
            if _alive(proc):
                remaining.append(proc)
    return remaining


def stop_tool_processes(
    processes: Iterable[Any],
    *,
    terminate_timeout: float = 3.0,
    kill_timeout: float = 2.0,
) -> bool:
    """Terminate, kill if needed, and reap a process group within two bounds."""
    unique = {id(proc): proc for proc in processes if proc is not None}
    alive = [proc for proc in unique.values() if _alive(proc)]
    for proc in alive:
        try:
            proc.terminate()
        except Exception:
            pass
    survivors = _wait_group(alive, terminate_timeout)
    for proc in survivors:
        try:
            proc.kill()
        except Exception:
            pass
    survivors = _wait_group(survivors, kill_timeout)
    for proc in unique.values():
        if not _alive(proc):
            try:
                proc.wait(timeout=0)
            except Exception:
                pass
            _close_process_streams(proc)
    return not survivors


def start_registered_tool_process(
    registry: Any,
    command: Sequence[str],
    *,
    root: str | os.PathLike[str],
    **popen_kwargs: Any,
) -> Any:
    """Spawn under the update barrier and roll back every post-spawn failure."""
    if registry is None:
        raise RuntimeError("tool process registry unavailable")
    spawned = None
    try:
        from update_barrier import process_start_guard

        with process_start_guard(root):
            spawned = subprocess.Popen(command, **popen_kwargs)
        if not registry.register(spawned):
            raise RuntimeError("launcher shutdown is already in progress")
        return spawned
    except BaseException:
        if spawned is not None:
            stopped = False
            try:
                stopped = bool(registry.stop(spawned))
            except Exception:
                pass
            if not stopped and stop_tool_processes([spawned]):
                try:
                    registry.unregister(spawned)
                except Exception:
                    pass
        raise


class ToolProcessRegistry:
    """Thread-safe ownership registry for launcher-spawned tool children."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._processes: dict[int, Any] = {}
        self._closing = False

    def register(self, proc: Any) -> bool:
        if proc is None:
            raise ValueError("tool process is required")
        with self._lock:
            if not self._closing:
                if _alive(proc):
                    self._processes[id(proc)] = proc
                return True
        stop_tool_processes([proc])
        return False

    def unregister(self, proc: Any) -> None:
        if proc is None:
            return
        with self._lock:
            self._processes.pop(id(proc), None)

    def snapshot(self) -> list[Any]:
        with self._lock:
            exited = [key for key, proc in self._processes.items() if not _alive(proc)]
            for key in exited:
                self._processes.pop(key, None)
            return list(self._processes.values())

    def resume_after_aborted_shutdown(self) -> bool:
        """Reopen registrations only when shutdown left no owned survivor."""
        with self._lock:
            for key, proc in list(self._processes.items()):
                if not _alive(proc):
                    self._processes.pop(key, None)
            if self._processes:
                return False
            self._closing = False
            return True

    def stop(
        self,
        proc: Any,
        *,
        terminate_timeout: float = 3.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        stopped = stop_tool_processes(
            [proc],
            terminate_timeout=terminate_timeout,
            kill_timeout=kill_timeout,
        )
        with self._lock:
            if stopped or not _alive(proc):
                self._processes.pop(id(proc), None)
            else:
                # Preserve ownership of an unkillable child so a later
                # shutdown/retry cannot mistake an empty registry for safety.
                self._processes[id(proc)] = proc
        return stopped

    def stop_all(
        self,
        *,
        terminate_timeout: float = 3.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        with self._lock:
            self._closing = True
            processes = list(self._processes.values())
        stopped = stop_tool_processes(
            processes,
            terminate_timeout=terminate_timeout,
            kill_timeout=kill_timeout,
        )
        with self._lock:
            for key, proc in list(self._processes.items()):
                if not _alive(proc):
                    self._processes.pop(key, None)
            return stopped and not self._processes
