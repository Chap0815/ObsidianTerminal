"""
Subprocess wrapper for the trading bots.

A :class:`BotProcess` owns one ``python -m bots.<name>`` subprocess, forwards
its stdout into a queue (for the activity log), and provides a graceful or
hard ``stop()``.

Notable design points:

* The bot's working directory is **always** the project root, not the location
  of any module file. That way the bot can find its sibling packages
  (``core/``, ``config/``, ``tools/``, ``prompts/``, ``data/``) regardless of
  whether you launch from a Desktop shortcut, a terminal, or via ``python -m``.

* On Windows the bot subprocess is started with
  ``CREATE_NEW_PROCESS_GROUP`` so the launcher can later send
  ``CTRL_BREAK_EVENT`` for graceful shutdowns. Without that flag the signal
  silently no-ops.

* The reader thread uses a *drop-oldest* policy when the queue is full so
  long-running bots cannot deadlock the launcher when the UI consumer falls
  behind.
"""

from __future__ import annotations

import os
import queue
import re
from collections import deque
from contextlib import contextmanager
import signal
import subprocess
import sys
import threading
import time
import uuid

from launcher.config.settings import PROJECT_ROOT, _get_python_exe, subprocess_no_window_kwargs
from launcher.core.runtime_status_values import (
    nonnegative_int_or_zero,
    positive_int_or_zero,
    strict_bool_or_none,
)
from update_barrier import process_start_guard


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
_TIMESTAMP_THEN_LEVEL_RE = re.compile(
    r"^\s*\[\d{2}:\d{2}:\d{2}\]\s+"
    r"(?P<level>INFO|OK|WARN|WARNING|ERROR|CRITICAL|FATAL|START|SCAN|WAIT|"
    r"BUY|SELL|WIN|LOSS)\s+",
    re.IGNORECASE,
)
_LEVEL_THEN_TIMESTAMP_RE = re.compile(
    r"^\s*(?P<level>INFO|OK|WARN|WARNING|ERROR|CRITICAL|FATAL|START|SCAN|WAIT|"
    r"BUY|SELL|WIN|LOSS)\s+\[\d{2}:\d{2}:\d{2}\]\s+",
    re.IGNORECASE,
)


class RepeatedLogCompactor:
    """Compact only consecutive duplicate UI lines with bounded state.

    Timestamps are ignored for comparison while severity is preserved. Unique lines,
    including unique traceback frames, are never dropped. A periodic summary
    keeps an endlessly repeating condition visible without flooding the UI.
    """

    def __init__(self, *, summary_interval_seconds: float = 30.0) -> None:
        self.summary_interval_seconds = max(
            1.0, float(summary_interval_seconds)
        )
        self._fingerprint: str | None = None
        self._level = "INFO"
        self._repeat_count = 0
        self._last_summary_at = 0.0

    @staticmethod
    def _line_fingerprint(line: str) -> str:
        clean = _ANSI_ESCAPE_RE.sub("", str(line))
        clean = _TIMESTAMP_THEN_LEVEL_RE.sub(r"\g<level> ", clean, count=1)
        clean = _LEVEL_THEN_TIMESTAMP_RE.sub(r"\g<level> ", clean, count=1)
        return clean.strip()

    @staticmethod
    def _summary(count: int) -> str:
        noun = "time" if count == 1 else "times"
        tail = "duplicate condensed" if count == 1 else "duplicates condensed"
        return f"[Log] Previous message repeated {count} {noun}; {tail}."

    @staticmethod
    def _line_level(line: str) -> str:
        clean = _ANSI_ESCAPE_RE.sub("", str(line))
        match = _TIMESTAMP_THEN_LEVEL_RE.match(clean)
        if match is None:
            match = _LEVEL_THEN_TIMESTAMP_RE.match(clean)
        if match is not None:
            return match.group("level").upper()
        if clean.lstrip().startswith(("Traceback", "During handling", "File \"")):
            return "ERROR"
        return "INFO"

    def _summary_line(self) -> str:
        return f"{self._level} {self._summary(self._repeat_count)}"

    def push(self, line: str, *, now: float | None = None) -> tuple[str, ...]:
        current = time.monotonic() if now is None else float(now)
        fingerprint = self._line_fingerprint(line)
        if self._fingerprint is None:
            self._fingerprint = fingerprint
            self._level = self._line_level(line)
            self._last_summary_at = current
            return (line,)
        if fingerprint != self._fingerprint:
            output = []
            if self._repeat_count:
                output.append(self._summary_line())
            output.append(line)
            self._fingerprint = fingerprint
            self._level = self._line_level(line)
            self._repeat_count = 0
            self._last_summary_at = current
            return tuple(output)

        self._repeat_count += 1
        if current - self._last_summary_at >= self.summary_interval_seconds:
            summary = self._summary_line()
            self._repeat_count = 0
            self._last_summary_at = current
            return (summary,)
        return ()

    def flush(self) -> tuple[str, ...]:
        if not self._repeat_count:
            return ()
        summary = self._summary_line()
        self._repeat_count = 0
        return (summary,)


_PRIORITY_LEVEL_RE = re.compile(
    r"^\s*(?:\[\d{2}:\d{2}:\d{2}\]\s+)?"
    r"(?:ERROR|CRITICAL|FATAL|TRACEBACK)\b",
    re.IGNORECASE,
)
_WARN_LEVEL_RE = re.compile(
    r"^\s*(?:\[\d{2}:\d{2}:\d{2}\]\s+)?"
    r"(?:WARN|WARNING|OK|START)\b",
    re.IGNORECASE,
)
_TRADE_LEVEL_RE = re.compile(
    r"^\s*(?:\[\d{2}:\d{2}:\d{2}\]\s+)?"
    r"(?:BUY|SELL|WIN|LOSS)\b",
    re.IGNORECASE,
)


class BoundedLogQueue:
    """Bounded UI queue that protects important lines and reports every drop."""

    def __init__(self, maxsize: int = 5000) -> None:
        self.maxsize = max(1, int(maxsize))
        self._items = deque()
        self._lock = threading.Lock()
        self._dropped_routine = 0
        self._dropped_important = 0

    @staticmethod
    def _priority(line: str) -> int:
        text = _ANSI_ESCAPE_RE.sub("", str(line))
        if (
            _PRIORITY_LEVEL_RE.match(text)
            or text.startswith("  File \"")
            or text.startswith("During handling of the above exception")
            or re.match(r"^[\w.]+(?:Error|Exception|Timeout):", text)
        ):
            return 3
        if _TRADE_LEVEL_RE.match(text):
            return 2
        if _WARN_LEVEL_RE.match(text):
            return 1
        return 0

    def _record_drop(self, priority: int) -> None:
        if priority:
            self._dropped_important += 1
        else:
            self._dropped_routine += 1

    def put_nowait(self, line: str) -> None:
        item = str(line)
        priority = self._priority(item)
        with self._lock:
            if len(self._items) < self.maxsize:
                self._items.append((item, priority))
                return

            victim = None
            if priority:
                for lower_level in range(priority):
                    victim = next(
                        (
                            i
                            for i, (_text, level) in enumerate(self._items)
                            if level == lower_level
                        ),
                        None,
                    )
                    if victim is not None:
                        break
                if victim is None and priority == 3:
                    victim = 0
            else:
                victim = next(
                    (i for i, (_text, level) in enumerate(self._items) if level == 0),
                    None,
                )

            if victim is None:
                self._record_drop(priority)
                return
            _dropped_text, dropped_priority = self._items[victim]
            del self._items[victim]
            self._record_drop(dropped_priority)
            self._items.append((item, priority))

    def get_nowait(self) -> str:
        with self._lock:
            if self._dropped_routine or self._dropped_important:
                routine = self._dropped_routine
                important = self._dropped_important
                self._dropped_routine = 0
                self._dropped_important = 0
                parts = []
                if routine:
                    parts.append(
                        f"{routine} routine line{'s' if routine != 1 else ''}"
                    )
                if important:
                    parts.append(
                        f"{important} important line{'s' if important != 1 else ''}"
                    )
                return (
                    "WARN [Log] " + " and ".join(parts)
                    + " omitted because the display queue was full."
                )
            if not self._items:
                raise queue.Empty
            return self._items.popleft()[0]

    def qsize(self) -> int:
        with self._lock:
            return len(self._items) + int(
                bool(self._dropped_routine or self._dropped_important)
            )

    def empty(self) -> bool:
        return self.qsize() == 0


class BotProcess:
    """One managed bot subprocess.

    Parameters
    ----------
    script :
        Legacy script path (e.g. ``"bots/main_bot_balanced.py"``). Kept for
        diagnostics. Only used as a fallback when ``module`` is empty.
    log_queue :
        Queue that the reader thread pushes stdout lines into.
    supports_graceful :
        If ``True``, ``stop(graceful_close=True)`` sends ``SIGTERM`` /
        ``CTRL_BREAK_EVENT`` and waits up to 45 s before resorting to a
        hard ``kill()``. All current bots support this.
    module :
        Module path for ``python -m`` invocation
        (e.g. ``"bots.main_bot_balanced"``). Preferred over ``script``
        because ``-m`` puts the project root on ``sys.path`` so the bot's
        ``from core.X import `` lines resolve correctly.
    """

    def __init__(self, script: str, log_queue: queue.Queue,
                 supports_graceful: bool = False, module: str = "",
                 bot_name: str = ""):
        self.script = script
        self.module = module
        self.bot_name = bot_name  # e.g. "TREND", "SPOT", "FUTURES"
        self.proc: subprocess.Popen | None = None
        self.log_queue = log_queue
        self.start_config: dict | None = None
        self.run_id: str | None = None
        self._failed_start_owned_proc: subprocess.Popen | None = None
        self.supports_graceful = supports_graceful
        self._lifecycle_lock = threading.Lock()
        self._stop_close_operation_active = False

    #  Lifecycle 

    def start(self, current_config_snapshot: dict | None = None) -> None:
        with self._lifecycle_lock:
            if self._stop_close_operation_active:
                raise RuntimeError("bot stop/close operation is still in progress")
            if self.is_running():
                return

            failed_start_proc = self._failed_start_owned_proc
            if failed_start_proc is not None:
                try:
                    failed_start_exited = failed_start_proc.poll() is not None
                except Exception as exc:
                    raise RuntimeError(
                        "failed-start bot process state cannot be determined"
                    ) from exc
                if not failed_start_exited:
                    raise RuntimeError(
                        "bot process from failed start is still running"
                    )
                self._close_process_stdout(failed_start_proc)
                if self.proc is failed_start_proc:
                    self.proc = None
                self._failed_start_owned_proc = None

            kw: dict = subprocess_no_window_kwargs()
            if sys.platform == "win32":
                flags = kw.get("creationflags", 0) | subprocess.CREATE_NO_WINDOW
                if self.supports_graceful:
                    # Required so CTRL_BREAK_EVENT can be sent later
                    flags |= subprocess.CREATE_NEW_PROCESS_GROUP
                kw["creationflags"] = flags

            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"] = "1"
            env["PYTHONLEGACYWINDOWSSTDIO"] = "0"
            # Tag the subprocess so api_budget can record per-bot API calls.
            # Without this, api_rate_global rows would show bot_name='unknown'
            # for any module that reaches api_budget via a non-bot thread
            # (reconcile, monitor running fetch_funding_rate, etc).
            if self.bot_name:
                env["BOT_NAME"] = self.bot_name
                env["OBSIDIAN_LAUNCHER_PRESTART_OK"] = "1"
            self.run_id = uuid.uuid4().hex
            env["BOT_RUN_ID"] = self.run_id

            # Module-style invocation is preferred (post-migration); fall back
            # to direct script path for ad-hoc test scripts.
            if self.module:
                argv = [_get_python_exe(), "-u", "-m", self.module]
            else:
                argv = [_get_python_exe(), "-u", self.script]

            spawned_proc = None
            try:
                with process_start_guard(PROJECT_ROOT):
                    spawned_proc = subprocess.Popen(
                        argv,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        # Python 3.13: bufsize=1 (line-buffered) raises RuntimeWarning
                        # on binary pipes. The bot uses -u (unbuffered stdout) anyway,
                        # so bufsize=-1 (OS default) gives identical behaviour.
                        text=True, bufsize=-1, encoding="utf-8", errors="replace",
                        env=env, cwd=PROJECT_ROOT, **kw
                    )
            except Exception:
                if spawned_proc is not None:
                    rollback_complete = self._rollback_failed_start(
                        spawned_proc
                    )
                    if not rollback_complete:
                        self.proc = spawned_proc
                        self.start_config = current_config_snapshot
                        self._failed_start_owned_proc = spawned_proc
                else:
                    rollback_complete = True
                if rollback_complete:
                    self.run_id = None
                raise
            self.proc = spawned_proc
            self.start_config = current_config_snapshot
            try:
                threading.Thread(
                    target=self._reader,
                    args=(spawned_proc,),
                    daemon=True,
                ).start()
            except Exception:
                rollback_complete = self._rollback_failed_start(spawned_proc)
                if rollback_complete:
                    if self.proc is spawned_proc:
                        self.proc = None
                    self.run_id = None
                else:
                    self._failed_start_owned_proc = spawned_proc
                raise

    # Graceful-close budget breakdown (must exceed bot.SHUTDOWN_DEADLINE_SEC):
    #  45s  bot's emergency-close deadline (closing positions)
    #  15s  bot's thread-join cleanup (monitor/scan/reconcile)
    #  ~5s  safety margin for Python interpreter shutdown
    # Total budget: 75s.
    _GRACEFUL_TIMEOUT_SEC: float = 75.0
    _FORCE_KILL_TIMEOUT_SEC: float = 5.0

    @staticmethod
    def _close_process_stdout(proc: subprocess.Popen) -> None:
        stdout = getattr(proc, "stdout", None)
        if stdout is None or getattr(stdout, "closed", False):
            return
        try:
            stdout.close()
        except Exception:
            pass

    @contextmanager
    def exclusive_stop_operation(self):
        """Block concurrent restart until launcher cleanup/fallback is done."""
        with self._lifecycle_lock:
            if self._stop_close_operation_active:
                raise RuntimeError("bot stop/close operation is already in progress")
            self._stop_close_operation_active = True
        try:
            yield
        finally:
            with self._lifecycle_lock:
                self._stop_close_operation_active = False

    def _rollback_failed_start(self, proc: subprocess.Popen) -> bool:
        """Return True only after a failed-start child is proven reaped."""
        reaped = False
        try:
            proc.terminate()
            proc.wait(timeout=5)
            reaped = True
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=self._FORCE_KILL_TIMEOUT_SEC)
                reaped = True
            except Exception:
                try:
                    reaped = proc.poll() is not None
                except Exception:
                    reaped = False
        if reaped:
            self._close_process_stdout(proc)
        return reaped

    def _mark_runtime_stopped(self, returncode=None, *,
                              expected_run_id: str | None = None,
                              expected_pid: int | None = None) -> None:
        if not self.bot_name:
            return
        try:
            from launcher.config.settings import BOT_META
            from core.runtime_status import read_runtime_status, write_runtime_status
            meta = BOT_META.get(self.bot_name, {})
            log_dir = meta.get("log_dir")
            if not log_dir:
                return
            last = read_runtime_status(log_dir)
            expected_run_id = expected_run_id or ""
            last_run_id = str(last.get("run_id") or "")
            last_pid = positive_int_or_zero(last.get("pid"))
            last_simulation = strict_bool_or_none(last.get("simulation"))
            if expected_run_id and last_run_id and last_run_id != expected_run_id:
                return
            if expected_pid and last_pid and last_pid != expected_pid:
                return
            write_runtime_status(
                log_dir,
                self.bot_name,
                "stopped",
                True if last_simulation is None else last_simulation,
                threads={"monitor": False, "scan": False, "reconcile": False},
                extra={
                    "open_positions": nonnegative_int_or_zero(
                        last.get("open_positions")
                    ),
                    "previous_status": str(last.get("status") or ""),
                    "stopped_by": "launcher",
                    "returncode": returncode,
                    "pid": expected_pid or last_pid or 0,
                    "run_id": expected_run_id or last_run_id,
                },
            )
        except Exception as e:
            stderr = sys.stderr
            if stderr is not None:
                try:
                    stderr.write(
                        f"[BotProcess] runtime stopped marker failed: {e}\n"
                    )
                except Exception:
                    pass

    def stop(self, graceful_close: bool = False) -> int | None:
        """Stop the bot subprocess.

        ``graceful_close``
            If ``True`` give the bot's atexit/signal handler a chance to run
            before terminating. On Windows the bot must have been started
            with ``CREATE_NEW_PROCESS_GROUP``.

        The graceful path allows 75s total (see ``_GRACEFUL_TIMEOUT_SEC``) so
        there is headroom past the bot's own SHUTDOWN_DEADLINE_SEC for the
        thread-join cleanup and interpreter shutdown; the force-kill kicks in
        after that. ``self.proc`` is cleared the moment the OS-level process
        exits  before the teardown finishes  so the launcher's
        ``is_running()`` poll sees the death immediately.
        """
        # Snapshot proc under the lock, then release the lock for the
        # long blocking wait. Holding the lifecycle_lock for 75s would
        # block ``is_running()`` if it ever needed the same lock  and
        # also block any concurrent start() call from happening once
        # the process is genuinely dead.
        already_exited: tuple[int | None, str, int] | None = None
        with self._lifecycle_lock:
            if self.proc is None:
                return None
            if self.proc.poll() is not None:
                proc_to_mark = self.proc
                run_id_to_mark = self.run_id or ""
                pid_to_mark = proc_to_mark.pid
                returncode_to_mark = proc_to_mark.poll()
                self.proc = None
                close_failed_start_stdout = (
                    self._failed_start_owned_proc is proc_to_mark
                )
                if close_failed_start_stdout:
                    self._failed_start_owned_proc = None
                already_exited = (
                    returncode_to_mark,
                    run_id_to_mark,
                    pid_to_mark,
                )
            else:
                proc_to_stop = self.proc
                run_id_to_stop = self.run_id or ""
                pid_to_stop = proc_to_stop.pid
        if already_exited is not None:
            returncode_to_mark, run_id_to_mark, pid_to_mark = already_exited
            if close_failed_start_stdout:
                self._close_process_stdout(proc_to_mark)
            self._mark_runtime_stopped(
                returncode_to_mark,
                expected_run_id=run_id_to_mark,
                expected_pid=pid_to_mark,
            )
            return pid_to_mark

        #  Send the signal OUTSIDE the lock 
        signal_ok = False
        if graceful_close and self.supports_graceful:
            try:
                if sys.platform == "win32":
                    # CTRL_BREAK_EVENT works only with CREATE_NEW_PROCESS_GROUP.
                    # On some Python 3.13 + Windows 11 combinations the handle
                    # becomes invalid ([WinError 6]) even when the flag was set.
                    # We catch this and fall through to terminate() immediately
                    # instead of waiting 75s for a graceful close that can't
                    # happen.
                    proc_to_stop.send_signal(signal.CTRL_BREAK_EVENT)
                    signal_ok = True
                else:
                    proc_to_stop.send_signal(signal.SIGTERM)
                    signal_ok = True
            except Exception as e:
                stdout = sys.stdout
                if stdout is not None:
                    try:
                        stdout.write(
                            f"[BotProcess] graceful signal unavailable: {e} "
                            f"- using terminate()\n"
                        )
                    except Exception:
                        pass

        if signal_ok:
            # Signal sent  give the bot time to clean up gracefully.
            try:
                proc_to_stop.wait(timeout=self._GRACEFUL_TIMEOUT_SEC)
            except Exception as exc:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(
                            f"[BotProcess] graceful close did not finish after "
                            f"{self._GRACEFUL_TIMEOUT_SEC}s ({exc})  force kill\n"
                        )
                    except Exception:
                        pass
        else:
            # Signal failed or not requested  hard terminate immediately.
            terminate_sent = False
            try:
                proc_to_stop.terminate()
                terminate_sent = True
            except Exception as exc:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(
                            f"[BotProcess] terminate failed ({exc})  force kill\n"
                        )
                    except Exception:
                        pass
            if terminate_sent and proc_to_stop.poll() is None:
                try:
                    proc_to_stop.wait(timeout=5)
                except Exception:
                    pass

        if proc_to_stop.poll() is None:
            try:
                proc_to_stop.kill()
            except Exception as exc:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(f"[BotProcess] force kill failed: {exc}\n")
                    except Exception:
                        pass
            try:
                proc_to_stop.wait(timeout=self._FORCE_KILL_TIMEOUT_SEC)
            except Exception:
                pass

        #  Mark dead under the lock 
        if proc_to_stop.poll() is None:
            raise RuntimeError(
                f"bot process {pid_to_stop} is still running after stop escalation"
            )
        with self._lifecycle_lock:
            # Only clear if we're still pointing at the proc we just stopped
            #  a concurrent start() shouldn't be clobbered.
            if self.proc is proc_to_stop:
                self.proc = None
            close_failed_start_stdout = (
                self._failed_start_owned_proc is proc_to_stop
            )
            if close_failed_start_stdout:
                self._failed_start_owned_proc = None
        if close_failed_start_stdout:
            self._close_process_stdout(proc_to_stop)
        self._mark_runtime_stopped(
            proc_to_stop.poll(),
            expected_run_id=run_id_to_stop,
            expected_pid=pid_to_stop,
        )
        return pid_to_stop

    def is_running(self) -> bool:
        # Lock-free read by design  Python's GIL makes the attribute load
        # atomic, and Popen.poll() is documented as thread-safe. This lets
        # the launcher's polling loop check status while a slow stop() is
        # still mid-wait, without deadlocking against the lifecycle lock.
        return self.proc is not None and self.proc.poll() is None

    #  Stdout reader 

    def _enqueue_log_line(self, line: str) -> None:
        """Queue one line without ever blocking the bot process."""
        try:
            self.log_queue.put_nowait(line)
        except queue.Full:
            try:
                self.log_queue.get_nowait()
                self.log_queue.put_nowait(line)
            except (queue.Empty, queue.Full):
                pass

    def _reader(self, proc: subprocess.Popen) -> None:
        if not proc or not proc.stdout:
            return
        stdout = proc.stdout
        compactor = RepeatedLogCompactor()
        try:
            for line in stdout:
                line = line.rstrip("\n").rstrip("\r")
                if line and "\r" not in line:
                    # Drop-oldest policy: if the queue is full, ditch the
                    # oldest line and append the new one. Without this the
                    # reader could block forever and new logs would stop
                    # appearing in the UI.
                    for output_line in compactor.push(line):
                        self._enqueue_log_line(output_line)
        finally:
            for output_line in compactor.flush():
                self._enqueue_log_line(output_line)
            try:
                stdout.close()
            except Exception:
                pass
