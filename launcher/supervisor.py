"""Crash-only supervisor for the Windows launcher UI.

The trading bots are separate processes.  If Tk terminates ``pythonw.exe``
abnormally, this supervisor restores only the UI; it never stops, starts, or
restarts a bot.  A normal launcher close exits the supervisor immediately.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import contextmanager
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import BinaryIO

from update_barrier import (
    UpdateInProgressError,
    assert_process_start_allowed,
    ensure_runtime_install_mutex,
    process_start_guard,
)


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_RESTART_DELAYS = (1.0, 5.0, 15.0)
_WAIT_OBSERVATION_RETRY_DELAYS = (0.1, 0.5, 1.0)
_SUPERVISOR_LOG_MAX_BYTES = 1024 * 1024
_SUPERVISOR_LOG_BACKUPS = 2


def _append_supervisor_log(message: str, *, root: Path = _PROJECT_ROOT) -> None:
    """Persist a bounded native-crash trail without importing the GUI stack."""
    try:
        log_path = root / "logs" / "launcher_supervisor.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        line = (
            f"[{datetime.now().isoformat(timespec='seconds')}] "
            f"{str(message)[:1600]}\n"
        )
        encoded_size = len(line.encode("utf-8", errors="replace"))
        current_size = log_path.stat().st_size if log_path.is_file() else 0
        if current_size + encoded_size > _SUPERVISOR_LOG_MAX_BYTES:
            try:
                for index in range(_SUPERVISOR_LOG_BACKUPS, 0, -1):
                    source = (
                        log_path
                        if index == 1
                        else Path(f"{log_path}.{index - 1}")
                    )
                    target = Path(f"{log_path}.{index}")
                    if not source.is_file():
                        continue
                    target.unlink(missing_ok=True)
                    os.replace(source, target)
            except OSError:
                # Crash diagnostics are most valuable exactly when rotation is
                # disrupted by a transient Windows reader/AV lock. Sacrifice
                # the older primary rather than dropping the newest event.
                with log_path.open("w", encoding="utf-8", newline="") as stream:
                    stream.write(line)
                    stream.flush()
                return
        with log_path.open("a", encoding="utf-8", newline="") as stream:
            stream.write(line)
            stream.flush()
    except (OSError, TypeError, ValueError):
        return


def _emit_supervisor_event(
    sink: Callable[[str], object],
    message: str,
) -> None:
    try:
        sink(message)
    except Exception:
        pass


def _pythonw_executable() -> Path:
    executable = Path(sys.executable)
    if executable.name.lower() == "pythonw.exe":
        return executable
    sibling = executable.with_name("pythonw.exe")
    return sibling if sibling.is_file() else executable


def _spawn_launcher(command: list[str], **kwargs):
    return subprocess.Popen(command, **kwargs)


def _wait_for_child_returncode(
    process,
    *,
    sleep: Callable[[float], object],
    event_sink: Callable[[str], object],
) -> int:
    """Observe one child without ever converting uncertainty into a respawn."""
    last_error: Exception | None = None
    for attempt in range(len(_WAIT_OBSERVATION_RETRY_DELAYS) + 1):
        try:
            return int(process.wait())
        except Exception as exc:
            last_error = exc
            try:
                returncode = process.poll()
            except Exception:
                returncode = None
            if returncode is not None:
                return int(returncode)
            if attempt >= len(_WAIT_OBSERVATION_RETRY_DELAYS):
                break
            delay = _WAIT_OBSERVATION_RETRY_DELAYS[attempt]
            _emit_supervisor_event(
                event_sink,
                "Launcher-Beobachtung voruebergehend fehlgeschlagen: "
                f"{type(exc).__name__}; erneuter Wait auf dasselbe Kind "
                f"in {delay:.2f}s",
            )
            sleep(delay)
    assert last_error is not None
    raise RuntimeError(
        "launcher child state remained unavailable; refusing duplicate spawn"
    ) from last_error


def supervise_launcher(
    *,
    project_root: str | os.PathLike[str] = _PROJECT_ROOT,
    pythonw: str | os.PathLike[str] | None = None,
    spawn: Callable[..., object] = _spawn_launcher,
    sleep: Callable[[float], object] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    event_sink: Callable[[str], object] | None = None,
    assert_start_allowed: Callable[[str], object] = assert_process_start_allowed,
    restart_delays: Sequence[float] = _DEFAULT_RESTART_DELAYS,
    stable_run_seconds: float = 300.0,
) -> int:
    """Run the UI and recover boundedly from consecutive native crashes."""
    root = Path(project_root).resolve()
    executable = Path(pythonw) if pythonw is not None else _pythonw_executable()
    command = [str(executable), str(root / "launcher.pyw")]
    delays = tuple(max(0.0, float(delay)) for delay in restart_delays)
    stable_after = max(0.0, float(stable_run_seconds))
    sink = event_sink or (lambda message: _append_supervisor_log(message, root=root))
    last_returncode = 0
    crash_attempt = 0

    while True:
        started_at = None
        spawn_failed = False
        process = None
        try:
            assert_start_allowed(str(root))
        except UpdateInProgressError:
            return last_returncode
        except Exception as exc:
            # An unreadable lifecycle barrier is uncertainty, never permission
            # to spawn. Keep the supervisor alive and consume the same bounded
            # retry budget as a transient Popen failure.
            spawn_failed = True
            last_returncode = 1
            _emit_supervisor_event(
                sink,
                "Launcher-Startbarriere voruebergehend nicht lesbar: "
                f"{type(exc).__name__}; kein Start in diesem Versuch",
            )

        env = dict(os.environ)
        env["OBSIDIAN_SUPERVISED"] = "1"
        kwargs = {
            "cwd": str(root),
            "env": env,
            "close_fds": True,
        }
        if sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        guard_release_error: Exception | None = None
        if not spawn_failed:
            try:
                # Close the check-to-spawn race with the updater. The shared
                # barrier is held only through process creation, never for the
                # lifetime of the UI child.
                with process_start_guard(str(root)):
                    process = spawn(command, **kwargs)
                    # Only actual child uptime may reset the consecutive-crash
                    # budget. Barrier/Popen latency is not a stable UI run.
                    started_at = monotonic()
            except UpdateInProgressError as exc:
                if process is None:
                    return last_returncode
                guard_release_error = exc
            except Exception as exc:
                if process is not None:
                    guard_release_error = exc
                else:
                    spawn_failed = True
                    last_returncode = 1
                    detail = str(exc).replace("\r", " ").replace("\n", " ")[:400]
                    _emit_supervisor_event(
                        sink,
                        "Launcher-Start fehlgeschlagen: "
                        f"{type(exc).__name__}: {detail}",
                    )
        if guard_release_error is not None:
            _emit_supervisor_event(
                sink,
                "Launcher wurde erzeugt, aber die Startbarriere meldete beim "
                "Freigeben einen Fehler; das vorhandene Kind wird ohne "
                "Duplikat weiter beobachtet: "
                f"{type(guard_release_error).__name__}",
            )
        if process is not None:
            try:
                last_returncode = _wait_for_child_returncode(
                    process,
                    sleep=sleep,
                    event_sink=sink,
                )
            except RuntimeError as exc:
                _emit_supervisor_event(
                    sink,
                    "Launcher-Beobachtung dauerhaft fehlgeschlagen; "
                    "Supervisor endet fail-closed ohne zweiten Start: "
                    f"{exc}",
                )
                return 1
        run_seconds = (
            max(0.0, monotonic() - started_at)
            if started_at is not None
            else 0.0
        )
        if last_returncode == 0:
            return 0
        if not spawn_failed:
            _emit_supervisor_event(
                sink,
                "Launcher abnormal beendet: "
                f"returncode={last_returncode}, uptime_seconds={run_seconds:.1f}",
            )
        if run_seconds >= stable_after:
            if crash_attempt:
                _emit_supervisor_event(
                    sink,
                    "Crashbudget nach stabilem Lauf zurueckgesetzt: "
                    f"uptime_seconds={run_seconds:.1f}",
                )
            crash_attempt = 0
        if crash_attempt >= len(delays):
            _emit_supervisor_event(
                sink,
                "Launcher-Neustartbudget fuer aufeinanderfolgende Crashs erschoepft",
            )
            return last_returncode
        delay = delays[crash_attempt]
        _emit_supervisor_event(
            sink,
            f"Launcher-Neustart in {delay:.2f}s "
            f"(Versuch {crash_attempt + 1}/{len(delays)})",
        )
        sleep(delay)
        crash_attempt += 1


@contextmanager
def _single_supervisor_lock(root: Path):
    """Hold one process-scoped lock; OS release makes stale locks harmless."""
    lock_path = root / "logs" / "launcher_supervisor.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle: BinaryIO = open(lock_path, "a+b")
    acquired = False
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if sys.platform == "win32":
            import msvcrt

            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                acquired = True
            except OSError:
                acquired = False
        else:
            import fcntl

            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError:
                acquired = False
        yield acquired
    finally:
        if acquired:
            try:
                handle.seek(0)
                if sys.platform == "win32":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def main() -> int:
    ensure_runtime_install_mutex()
    with _single_supervisor_lock(_PROJECT_ROOT) as acquired:
        if not acquired:
            return 0
        return supervise_launcher(project_root=_PROJECT_ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
