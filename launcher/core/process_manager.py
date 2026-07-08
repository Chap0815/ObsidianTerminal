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
import signal
import subprocess
import sys
import threading
import uuid

from launcher.config.settings import PROJECT_ROOT, _get_python_exe, subprocess_no_window_kwargs
from launcher.core.runtime_status_values import (
    nonnegative_int_or_zero,
    positive_int_or_zero,
    strict_bool_or_none,
)


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
        self.supports_graceful = supports_graceful
        self._lifecycle_lock = threading.Lock()

    #  Lifecycle 

    def start(self, current_config_snapshot: dict | None = None) -> None:
        with self._lifecycle_lock:
            if self.is_running():
                return

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

            self.proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                # Python 3.13: bufsize=1 (line-buffered) raises RuntimeWarning
                # on binary pipes. The bot uses -u (unbuffered stdout) anyway,
                # so bufsize=-1 (OS default) gives identical behaviour.
                text=True, bufsize=-1, encoding="utf-8", errors="replace",
                env=env, cwd=PROJECT_ROOT, **kw
            )
            self.start_config = current_config_snapshot
            threading.Thread(target=self._reader, daemon=True).start()

    # Graceful-close budget breakdown (must exceed bot.SHUTDOWN_DEADLINE_SEC):
    #  45s  bot's emergency-close deadline (closing positions)
    #  15s  bot's thread-join cleanup (monitor/scan/reconcile)
    #  ~5s  safety margin for Python interpreter shutdown
    # Total budget: 75s.
    _GRACEFUL_TIMEOUT_SEC: float = 75.0
    _FORCE_KILL_TIMEOUT_SEC: float = 5.0

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
            # Signal sent  give the bot time to clean up gracefully
            try:
                proc_to_stop.wait(timeout=self._GRACEFUL_TIMEOUT_SEC)
            except subprocess.TimeoutExpired:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(
                            f"[BotProcess] graceful close TIMEOUT after "
                            f"{self._GRACEFUL_TIMEOUT_SEC}s  force kill\n"
                        )
                    except Exception:
                        pass
                try:
                    proc_to_stop.kill()
                    proc_to_stop.wait(timeout=self._FORCE_KILL_TIMEOUT_SEC)
                except Exception:
                    pass
        else:
            # Signal failed or not requested  hard terminate immediately
            try:
                proc_to_stop.terminate()
                proc_to_stop.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    proc_to_stop.kill()
                    proc_to_stop.wait(timeout=self._FORCE_KILL_TIMEOUT_SEC)
                except Exception:
                    pass

        #  Mark dead under the lock 
        if proc_to_stop.poll() is None:
            return pid_to_stop
        with self._lifecycle_lock:
            # Only clear if we're still pointing at the proc we just stopped
            #  a concurrent start() shouldn't be clobbered.
            if self.proc is proc_to_stop:
                self.proc = None
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

    def _reader(self) -> None:
        proc = self.proc
        if not proc or not proc.stdout:
            return
        for line in proc.stdout:
            line = line.rstrip("\n").rstrip("\r")
            if line and "\r" not in line:
                # Drop-oldest policy: if the queue is full, ditch the
                # oldest line and append the new one. Without this the
                # reader could block forever and new logs would stop
                # appearing in the UI.
                try:
                    self.log_queue.put_nowait(line)
                except queue.Full:
                    try:
                        self.log_queue.get_nowait()   # drop oldest
                        self.log_queue.put_nowait(line)
                    except (queue.Empty, queue.Full):
                        pass
