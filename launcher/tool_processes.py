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

import psutil

from bot_utils.subprocess_capture import (
    _new_process_job,
    _prepare_windows_gated_spawn,
)


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
_ORIGINAL_POPEN = subprocess.Popen
_TOOL_JOB_ATTRIBUTE = "_obsidian_tool_process_job"


class _StopDiagnostics:
    def __init__(self) -> None:
        self.discovery_uncertain = False
        self.uncertain_descendants: list[Any] = []


def _log_tool_stop_failure(context: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, exc)
    except BaseException:
        pass


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
        poll = getattr(proc, "poll", None)
        if callable(poll):
            return poll() is None
        is_running = getattr(proc, "is_running", None)
        if callable(is_running) and not is_running():
            return False
        status = getattr(proc, "status", None)
        if callable(status) and status() == psutil.STATUS_ZOMBIE:
            return False
        return callable(is_running)
    except Exception:
        return True


def _tool_process_job(proc: Any) -> Any | None:
    try:
        return getattr(proc, _TOOL_JOB_ATTRIBUTE, None)
    except Exception:
        return None


def _owned_tree_alive(proc: Any) -> bool:
    """Return fail-closed liveness for a registered root and its job tree."""
    job = _tool_process_job(proc)
    if job is None:
        return _alive(proc)
    try:
        return bool(job.has_live_processes())
    except Exception as exc:
        _log_tool_stop_failure("registered tool job liveness", exc)
        return True


def _release_quiescent_tool_job(proc: Any) -> bool:
    """Close and detach a Windows job only after exact tree quiescence."""
    job = _tool_process_job(proc)
    if job is None:
        if not _alive(proc):
            try:
                proc.wait(timeout=0)
            except Exception:
                pass
            _close_process_streams(proc)
        return not _alive(proc)
    try:
        if job.has_live_processes():
            return False
        job.close()
        delattr(proc, _TOOL_JOB_ATTRIBUTE)
    except Exception as exc:
        _log_tool_stop_failure("registered tool job release", exc)
        return False
    try:
        proc.wait(timeout=0)
    except Exception:
        pass
    _close_process_streams(proc)
    return True


def _stop_job_owned_tree(
    proc: Any,
    *,
    terminate_timeout: float,
    kill_timeout: float,
) -> bool:
    """Terminate one durable Windows job and retain it until it is empty."""
    job = _tool_process_job(proc)
    if job is None:
        return False
    try:
        if not job.has_live_processes():
            return _release_quiescent_tool_job(proc)
        job.terminate()
    except Exception as exc:
        _log_tool_stop_failure("registered tool job termination", exc)
        return False

    deadline = time.monotonic() + max(
        0.0,
        float(terminate_timeout) + float(kill_timeout),
    )
    while True:
        try:
            live = bool(job.has_live_processes())
        except Exception as exc:
            _log_tool_stop_failure("registered tool job drain", exc)
            return False
        if not live:
            return _release_quiescent_tool_job(proc)
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            return False
        time.sleep(min(0.01, remaining))


def _spawned_descendants(proc: Any) -> tuple[list[Any], bool]:
    """Snapshot descendants while their registered parent is still alive."""
    pid = getattr(proc, "pid", None)
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or not _alive(proc)
    ):
        return [], True
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        # ``proc`` is the original Popen handle, whereas ``parent`` was
        # reconstructed from a numeric PID. If the original exited during
        # discovery, that PID may already identify an unrelated process.
        # Never act on descendants whose ownership is no longer provable.
        if not _alive(proc):
            return children, False
        return children, True
    except psutil.NoSuchProcess:
        return [], True
    except (psutil.AccessDenied, OSError):
        # A parent-only stop is not proof that launcher-owned workers died.
        return [], False


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
    _diagnostics: _StopDiagnostics | None = None,
) -> bool:
    """Terminate, kill if needed, and reap process trees within two bounds."""
    unique = {id(proc): proc for proc in processes if proc is not None}
    job_roots = [
        proc for proc in unique.values() if _tool_process_job(proc) is not None
    ]
    jobs_ok = True
    for proc in job_roots:
        if not _stop_job_owned_tree(
            proc,
            terminate_timeout=terminate_timeout,
            kill_timeout=kill_timeout,
        ):
            jobs_ok = False
    roots = [
        proc
        for proc in unique.values()
        if _tool_process_job(proc) is None and _alive(proc)
    ]
    discovery_ok = True
    descendants: dict[int, Any] = {}
    for root in roots:
        children, discovered = _spawned_descendants(root)
        discovery_ok = discovery_ok and discovered
        if not discovered:
            if _diagnostics is not None:
                _diagnostics.discovery_uncertain = True
                _diagnostics.uncertain_descendants.extend(children)
            continue
        for child in children:
            try:
                descendants.setdefault(int(child.pid), child)
            except Exception:
                discovery_ok = False
    # Stop roots first so they cannot intentionally enqueue further workers,
    # then stop the already-snapshotted descendants.
    alive = roots + list(descendants.values())
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
    for proc in alive:
        if not _alive(proc):
            try:
                proc.wait(timeout=0)
            except Exception:
                pass
            _close_process_streams(proc)
    return jobs_ok and discovery_ok and not survivors


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
    job = None
    start_gate = None
    use_windows_containment = (
        os.name == "nt" and subprocess.Popen is _ORIGINAL_POPEN
    )
    try:
        from update_barrier import process_start_guard

        spawn_command = list(command)
        prepared_kwargs = dict(popen_kwargs)
        if use_windows_containment:
            job = _new_process_job()
            if job is None:
                raise RuntimeError("Windows tool process job unavailable")
            spawn_command, prepared_kwargs, start_gate = (
                _prepare_windows_gated_spawn(
                    command,
                    prepared_kwargs,
                    wrapper_python=None,
                )
            )
        with process_start_guard(root):
            spawned = subprocess.Popen(spawn_command, **prepared_kwargs)
            if start_gate is not None:
                start_gate.disable_inheritance()
            if job is not None:
                job.assign(spawned)
                setattr(spawned, _TOOL_JOB_ATTRIBUTE, job)
        if not registry.register(spawned):
            raise RuntimeError("launcher shutdown is already in progress")
        if start_gate is not None:
            start_gate.signal()
            start_gate.close()
            start_gate = None
        return spawned
    except BaseException as original_error:
        cleanup_errors: list[BaseException] = []
        if start_gate is not None:
            try:
                start_gate.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
        if spawned is not None:
            stopped = False
            try:
                stopped = bool(registry.stop(spawned))
            except BaseException as exc:
                cleanup_errors.append(exc)
            fallback_stopped = False
            if not stopped:
                try:
                    fallback_stopped = stop_tool_processes([spawned])
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if fallback_stopped:
                try:
                    registry.unregister(spawned)
                except BaseException as exc:
                    cleanup_errors.append(exc)
        if job is not None and (
            spawned is None or _tool_process_job(spawned) is None
        ):
            try:
                job.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
        for cleanup_error in cleanup_errors:
            try:
                _log_tool_stop_failure(
                    "registered tool spawn rollback",
                    cleanup_error,
                )
            except BaseException:
                pass
        if cleanup_errors:
            try:
                details = "; ".join(
                    f"{type(exc).__name__}: {exc}"
                    for exc in cleanup_errors
                )
                original_error.add_note(
                    f"tool process rollback failures: {details}"
                )
            except BaseException:
                pass
        raise


class ToolProcessRegistry:
    """Thread-safe ownership registry for launcher-spawned tool children."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._processes: dict[int, Any] = {}
        self._uncertain_processes: dict[int, Any] = {}
        self._unresolved_tree = False
        self._active_stops = 0
        self._closing = False

    def _prune_uncertain_locked(self) -> None:
        for key, proc in list(self._uncertain_processes.items()):
            if not _owned_tree_alive(proc):
                self._uncertain_processes.pop(key, None)

    def _record_diagnostics_locked(self, diagnostics: _StopDiagnostics) -> None:
        for proc in diagnostics.uncertain_descendants:
            if _owned_tree_alive(proc):
                self._uncertain_processes[id(proc)] = proc
        if diagnostics.discovery_uncertain:
            self._unresolved_tree = True

    def register(self, proc: Any) -> bool:
        if proc is None:
            raise ValueError("tool process is required")
        with self._lock:
            self._prune_uncertain_locked()
            if (
                not self._closing
                and self._active_stops == 0
                and not self._uncertain_processes
                and not self._unresolved_tree
            ):
                if _owned_tree_alive(proc):
                    self._processes[id(proc)] = proc
                return True
            if _owned_tree_alive(proc):
                self._processes[id(proc)] = proc
        self.stop(proc)
        return False

    def unregister(self, proc: Any) -> None:
        if proc is None:
            return
        with self._lock:
            if (
                _tool_process_job(proc) is not None
                and not _release_quiescent_tool_job(proc)
            ):
                self._processes[id(proc)] = proc
                return
            self._processes.pop(id(proc), None)

    def snapshot(self) -> list[Any]:
        with self._lock:
            exited = [
                key
                for key, proc in self._processes.items()
                if not _owned_tree_alive(proc)
                and _release_quiescent_tool_job(proc)
            ]
            for key in exited:
                self._processes.pop(key, None)
            self._prune_uncertain_locked()
            return list(self._processes.values())

    def resume_after_aborted_shutdown(self) -> bool:
        """Reopen registrations only when shutdown left no owned survivor."""
        with self._lock:
            for key, proc in list(self._processes.items()):
                if (
                    not _owned_tree_alive(proc)
                    and _release_quiescent_tool_job(proc)
                ):
                    self._processes.pop(key, None)
            self._prune_uncertain_locked()
            if (
                self._processes
                or self._uncertain_processes
                or self._unresolved_tree
                or self._active_stops
            ):
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
        with self._lock:
            initially_alive = _owned_tree_alive(proc)
            if initially_alive:
                self._processes[id(proc)] = proc
            self._active_stops += 1
        diagnostics = _StopDiagnostics()
        backend_failed = True
        stopped = False
        try:
            try:
                stopped = stop_tool_processes(
                    [proc],
                    terminate_timeout=terminate_timeout,
                    kill_timeout=kill_timeout,
                    _diagnostics=diagnostics,
                )
            except Exception as exc:
                _log_tool_stop_failure("registered tool process stop", exc)
            else:
                backend_failed = False
        finally:
            with self._lock:
                self._record_diagnostics_locked(diagnostics)
                if (
                    backend_failed
                    and initially_alive
                    and _tool_process_job(proc) is None
                    and not _alive(proc)
                ):
                    self._unresolved_tree = True
                if stopped or (
                    not _owned_tree_alive(proc)
                    and _release_quiescent_tool_job(proc)
                ):
                    self._processes.pop(id(proc), None)
                else:
                    # Preserve ownership of an unkillable child so a later
                    # shutdown/retry cannot mistake an empty registry for safety.
                    self._processes[id(proc)] = proc
                self._active_stops -= 1
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
            initially_alive = [proc for proc in processes if _owned_tree_alive(proc)]
            self._active_stops += 1
        diagnostics = _StopDiagnostics()
        backend_failed = True
        stopped = False
        try:
            try:
                stopped = stop_tool_processes(
                    processes,
                    terminate_timeout=terminate_timeout,
                    kill_timeout=kill_timeout,
                    _diagnostics=diagnostics,
                )
            except Exception as exc:
                _log_tool_stop_failure("registered tool process stop all", exc)
            else:
                backend_failed = False
        finally:
            with self._lock:
                self._record_diagnostics_locked(diagnostics)
                if backend_failed and any(
                    _tool_process_job(proc) is None and not _alive(proc)
                    for proc in initially_alive
                ):
                    self._unresolved_tree = True
                for key, proc in list(self._processes.items()):
                    if (
                        not _owned_tree_alive(proc)
                        and _release_quiescent_tool_job(proc)
                    ):
                        self._processes.pop(key, None)
                self._prune_uncertain_locked()
                self._active_stops -= 1
        with self._lock:
            return bool(
                stopped
                and not self._processes
                and not self._uncertain_processes
                and not self._unresolved_tree
                and self._active_stops == 0
            )
