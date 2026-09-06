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

* Every managed run owns an atomic PID-/run-ID-bound shutdown control record.
  ``CTRL_BREAK_EVENT`` is only a best-effort close-path wakeup on Windows;
  clean preserve-position shutdowns do not depend on console signals.

* The reader thread uses a *drop-oldest* policy when the queue is full so
  long-running bots cannot deadlock the launcher when the UI consumer falls
  behind.
"""

from __future__ import annotations

import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Mapping
from contextlib import contextmanager

from bot_utils.shutdown_control import (
    CLOSE_POSITIONS,
    PRESERVE_POSITIONS,
    SHUTDOWN_CONTROL_ENV,
    cleanup_shutdown_control,
    prepare_shutdown_control,
    publish_shutdown_request,
)
from bot_utils.runtime_threads import thread_definitely_never_started
from launcher.config.settings import (
    PROJECT_ROOT,
    _get_python_exe,
    subprocess_no_window_kwargs,
)
from launcher.core.runtime_status_values import (
    finite_float_or_none,
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
_UI_LOG_LINE_MAX_CHARS = 16 * 1024
_UI_LOG_LINE_TRUNCATED = " [launcher line truncated]"
_RUNTIME_SHUTDOWN_STATUS_MAX_AGE_SEC = 180.0
_PROCESS_EXIT_POLL_INTERVAL_SEC = 0.25
_TERMINAL_STATUS_PUBLISH_ATTEMPTS = 3
_TERMINAL_STATUS_RETRY_INTERVAL_SEC = 0.05


class _BoundedStdoutLineReader:
    """Read text-pipe lines without ever materializing an unbounded line."""

    def __init__(self, stream, *, max_chars: int = _UI_LOG_LINE_MAX_CHARS):
        self._stream = stream
        self._max_chars = max(1, int(max_chars))
        self._discarding = False

    @staticmethod
    def _has_delimiter(chunk: str) -> bool:
        return chunk.endswith(("\n", "\r"))

    def read(self) -> tuple[str, bool]:
        while True:
            chunk = self._stream.readline(self._max_chars + 1)
            if chunk == "":
                raise StopIteration
            if self._discarding:
                if self._has_delimiter(chunk):
                    self._discarding = False
                continue
            if self._has_delimiter(chunk):
                return chunk, False
            if len(chunk) <= self._max_chars:
                return chunk, False
            self._discarding = True
            return chunk[:self._max_chars], True


def _clean_shutdown_proven(shutdown: object) -> bool:
    if not isinstance(shutdown, Mapping):
        return False
    reasons = shutdown.get("reasons")
    resources = shutdown.get("resources")
    emergency_closed = shutdown.get("emergency_closed") is True
    positions_preserved = shutdown.get("positions_preserved") is True
    return (
        shutdown.get("complete") is True
        and emergency_closed != positions_preserved
        and shutdown.get("emergency_in_progress") is False
        and isinstance(reasons, list)
        and not reasons
        and isinstance(resources, Mapping)
        and bool(resources)
        and all(result is True for result in resources.values())
    )


def _launcher_shutdown_payload(shutdown: object) -> tuple[str, dict]:
    if not isinstance(shutdown, Mapping):
        return "degraded", {
            "complete": False,
            "reasons": ["shutdown_evidence_unavailable"],
            "resources": {},
        }
    payload = dict(shutdown)
    if shutdown.get("complete") is False:
        return "degraded", payload
    if _clean_shutdown_proven(shutdown):
        return "stopped", payload

    reported_complete = payload.get("complete")
    raw_reasons = payload.get("reasons")
    reasons = list(raw_reasons) if isinstance(raw_reasons, list) else []
    if "shutdown_evidence_inconsistent" not in reasons:
        reasons.append("shutdown_evidence_inconsistent")
    payload["complete"] = False
    payload["reasons"] = reasons
    if not isinstance(payload.get("resources"), Mapping):
        payload["resources"] = {}
    if reported_complete is not None:
        payload["reported_complete"] = reported_complete
    return "degraded", payload


def _fresh_runtime_status_requires_close_retention(
    runtime_status: object,
    *,
    expected_run_id: str,
    expected_pid: int,
    now_mono: float | None = None,
    now_wall: float | None = None,
) -> bool | None:
    """Return close retention verdict; ``None`` means evidence is untrusted."""
    if not isinstance(runtime_status, Mapping):
        return None
    expected_pid = positive_int_or_zero(expected_pid)
    if (
        not expected_run_id
        or not expected_pid
        or str(runtime_status.get("run_id") or "") != expected_run_id
        or positive_int_or_zero(runtime_status.get("pid")) != expected_pid
    ):
        return None

    mono = finite_float_or_none(runtime_status.get("monotonic_ts")) or 0.0
    if mono > 0.0:
        current = time.monotonic() if now_mono is None else now_mono
        age = current - mono
    else:
        wall = finite_float_or_none(runtime_status.get("wall_ts"))
        if wall is None or wall <= 0.0:
            wall = finite_float_or_none(runtime_status.get("epoch_ts")) or 0.0
        if wall <= 0.0:
            return None
        current = time.time() if now_wall is None else now_wall
        age = current - wall
    if not -5.0 <= age <= _RUNTIME_SHUTDOWN_STATUS_MAX_AGE_SEC:
        return None

    status = str(runtime_status.get("status") or "").lower()
    shutdown = runtime_status.get("shutdown")
    if not isinstance(shutdown, Mapping):
        return None
    if shutdown.get("complete") is True:
        return False if _clean_shutdown_proven(shutdown) else None
    if status != "degraded":
        return None
    if shutdown.get("complete") is not False:
        return None
    return (
        shutdown.get("emergency_closed") is not True
        or shutdown.get("emergency_in_progress") is True
        or shutdown.get("positions_preserved") is True
    )


def _redact_ui_log_line(line: str) -> str:
    """Redact subprocess output before it can enter any launcher UI queue."""
    try:
        from core.logger import redact
        safe = redact(str(line))
        if not isinstance(safe, str):
            raise TypeError("redactor returned non-text")
        if len(safe) > _UI_LOG_LINE_MAX_CHARS:
            safe = (
                safe[:_UI_LOG_LINE_MAX_CHARS - len(_UI_LOG_LINE_TRUNCATED)]
                + _UI_LOG_LINE_TRUNCATED
            )
        return safe
    except Exception:
        # Never expose the original line when the redaction layer is broken.
        return "WARN [launcher] subprocess line suppressed: redaction unavailable"


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
        self._items: OrderedDict[int, tuple[str, int]] = OrderedDict()
        self._priority_items = [OrderedDict() for _level in range(4)]
        self._next_item_id = 0
        self._priority_counts = [0, 0, 0, 0]
        self._lock = threading.Lock()
        self._dropped_routine = 0
        self._dropped_important = 0
        self._force_item_before_drop_summary = False

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

    def _append_item(self, item: str, priority: int) -> None:
        self._next_item_id += 1
        item_id = self._next_item_id
        self._items[item_id] = (item, priority)
        self._priority_items[priority][item_id] = None
        self._priority_counts[priority] += 1

    def _remove_item(self, item_id: int) -> tuple[str, int]:
        item, priority = self._items.pop(item_id)
        self._priority_items[priority].pop(item_id, None)
        self._priority_counts[priority] -= 1
        return item, priority

    def put_nowait(self, line: str) -> None:
        item = str(line)
        priority = self._priority(item)
        with self._lock:
            if len(self._items) < self.maxsize:
                self._append_item(item, priority)
                return

            victim = None
            if priority:
                for lower_level in range(priority):
                    candidates = self._priority_items[lower_level]
                    if not candidates:
                        continue
                    victim = next(iter(candidates))
                    break
                if victim is None:
                    # Under sustained warnings/trades/errors, retain current
                    # operational evidence instead of pinning the queue to its
                    # oldest same-priority snapshot.  The explicit drop notice
                    # still records that one important line was displaced.
                    candidates = self._priority_items[priority]
                    if candidates:
                        victim = next(iter(candidates))
            else:
                candidates = self._priority_items[0]
                if candidates:
                    victim = next(iter(candidates))

            if victim is None:
                self._record_drop(priority)
                return
            _dropped_text, dropped_priority = self._remove_item(victim)
            self._record_drop(dropped_priority)
            self._append_item(item, priority)

    def get_nowait(self) -> str:
        with self._lock:
            has_drops = bool(
                self._dropped_routine or self._dropped_important
            )
            if has_drops and (
                not self._force_item_before_drop_summary or not self._items
            ):
                routine = self._dropped_routine
                important = self._dropped_important
                self._dropped_routine = 0
                self._dropped_important = 0
                # Under sustained producer pressure, force one real queued
                # line after every summary. Otherwise each read can observe a
                # fresh drop and the bounded queue never drains at all.
                self._force_item_before_drop_summary = bool(self._items)
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
            self._force_item_before_drop_summary = False
            item_id = next(iter(self._items))
            item, _priority = self._remove_item(item_id)
            return item

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
        If ``True``, both close-position and preserve-position stops use the
        run-bound clean shutdown channel. All current bots support this.
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
        self._reader_state: dict | None = None
        self._shutdown_control_path: str | None = None
        self.supports_graceful = supports_graceful
        self._lifecycle_lock = threading.Lock()
        self._shutdown_request_publish_lock = threading.Lock()
        self._stop_close_operation_active = False
        self._stop_requested_run_id: str | None = None
        self._stop_requested_mode: str | None = None

    #  Lifecycle 

    def start(self, current_config_snapshot: dict | None = None) -> None:
        with self._lifecycle_lock:
            if self._stop_close_operation_active:
                raise RuntimeError("bot stop/close operation is still in progress")
            if (
                self._stop_requested_run_id
                and self._stop_requested_run_id == self.run_id
            ):
                raise RuntimeError("bot shutdown completion is still pending")
            process_running = self.is_running()
            reader_state = getattr(self, "_reader_state", None)
            if reader_state is not None and not process_running:
                if not reader_state["done"].is_set():
                    raise RuntimeError("bot stdout reader handoff is unresolved")
                # The exact reader is done and its child is proven dead. Retire
                # even a recorded reader failure now; while the child was live
                # the fatal state blocked duplicate starts, and its error was
                # already published by ``reader_target``.
                if self._reader_state is reader_state:
                    self._close_process_stdout(reader_state["proc"])
                    self._reader_state = None
                    reader_state = None
            if process_running:
                if (
                    reader_state is not None
                    and not reader_state["entered"].is_set()
                ):
                    raise RuntimeError("bot stdout reader handoff is unresolved")
                if (
                    reader_state is not None
                    and isinstance(reader_state.get("fatal"), BaseException)
                ):
                    raise RuntimeError("bot stdout reader failed") from (
                        reader_state["fatal"]
                    )
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

            if (
                self._shutdown_control_path
                and not self._cleanup_shutdown_control()
            ):
                raise RuntimeError(
                    "previous shutdown control cleanup is still pending"
                )

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
            self._stop_requested_run_id = None
            self._stop_requested_mode = None
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
                control_path = prepare_shutdown_control(
                    PROJECT_ROOT,
                    self.bot_name or "BOT",
                    self.run_id,
                )
                self._shutdown_control_path = control_path
                env[SHUTDOWN_CONTROL_ENV] = control_path
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
                    # Publish ownership before the guard's exit boundary. Its
                    # __exit__ may fail after Popen already created the child.
                    self.proc = spawned_proc
                    self.start_config = current_config_snapshot
            except BaseException as primary_error:
                self._settle_failed_start(
                    spawned_proc,
                    current_config_snapshot,
                    primary_error,
                )
                raise
            reader_run_id = self.run_id
            reader_state = {
                "proc": spawned_proc,
                "run_id": reader_run_id,
                "thread": None,
                "entered": threading.Event(),
                "done": threading.Event(),
                "fatal": None,
            }

            def reader_target() -> None:
                reader_state["entered"].set()
                try:
                    self._reader(
                        spawned_proc,
                        reader_run_id,
                        control_path,
                        True,
                    )
                except BaseException as exc:
                    reader_state["fatal"] = exc
                    try:
                        self._enqueue_log_line(
                            "ERROR [launcher] "
                            f"{self.bot_name or 'bot'} stdout reader failed "
                            f"({type(exc).__name__})",
                            expected_run_id=reader_run_id,
                        )
                    except BaseException:
                        pass
                finally:
                    reader_state["done"].set()
                    with self._lifecycle_lock:
                        if (
                            self._reader_state is reader_state
                            and reader_state["fatal"] is None
                        ):
                            self._reader_state = None

            try:
                reader_thread = threading.Thread(
                    target=reader_target,
                    daemon=True,
                )
            except BaseException as primary_error:
                self._settle_failed_start(
                    spawned_proc,
                    current_config_snapshot,
                    primary_error,
                )
                raise
            reader_state["thread"] = reader_thread
            self._reader_state = reader_state

        try:
            reader_thread.start()
        except BaseException as primary_error:
            safe_prelaunch = False
            if (
                isinstance(primary_error, Exception)
                and thread_definitely_never_started(reader_thread)
            ):
                with self._lifecycle_lock:
                    safe_prelaunch = self._reader_state is reader_state
                    if safe_prelaunch:
                        reader_state["done"].set()
                        self._reader_state = None
                        self._settle_failed_start(
                            spawned_proc,
                            current_config_snapshot,
                            primary_error,
                        )
            if safe_prelaunch:
                raise
            if (
                isinstance(primary_error, Exception)
                and reader_state["entered"].is_set()
                and not reader_state["done"].is_set()
                and reader_state["fatal"] is None
            ):
                # The exact reader generation demonstrably owns stdout.  A
                # post-launch error must not turn a valid bot start into an
                # active child rollback.
                return
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

    @staticmethod
    def _note_start_cleanup_failure(
        primary_error: BaseException,
        context: str,
        cleanup_error: BaseException,
    ) -> None:
        try:
            primary_error.add_note(
                f"bot start {context} failed: {type(cleanup_error).__name__}"
            )
        except BaseException:
            pass

    def _settle_failed_start(
        self,
        proc: subprocess.Popen | None,
        current_config_snapshot: dict | None,
        primary_error: BaseException,
    ) -> None:
        """Reap or retain exact ownership without masking a start failure."""
        rollback_complete = proc is None
        if proc is not None:
            try:
                rollback_complete = self._rollback_failed_start(proc)
            except BaseException as cleanup_error:
                rollback_complete = False
                self._note_start_cleanup_failure(
                    primary_error,
                    "child rollback",
                    cleanup_error,
                )
            if not rollback_complete:
                self.proc = proc
                self.start_config = current_config_snapshot
                self._failed_start_owned_proc = proc
                return
            if self.proc is proc:
                self.proc = None
            if self._failed_start_owned_proc is proc:
                self._failed_start_owned_proc = None

        self.run_id = None
        try:
            self._cleanup_shutdown_control()
        except BaseException as cleanup_error:
            self._note_start_cleanup_failure(
                primary_error,
                "shutdown-control cleanup",
                cleanup_error,
            )

    def _cleanup_shutdown_control(self, expected_path: str | None = None) -> bool:
        current_path = getattr(self, "_shutdown_control_path", None)
        path = expected_path or current_path
        if not path:
            return True
        try:
            cleanup_shutdown_control(path, project_root=PROJECT_ROOT)
        except Exception as exc:
            stderr = sys.stderr
            if stderr is not None:
                try:
                    stderr.write(
                        "[BotProcess] shutdown control cleanup failed: "
                        f"{type(exc).__name__}: {exc}\n"
                    )
                except Exception:
                    pass
            return False
        if getattr(self, "_shutdown_control_path", None) == path:
            self._shutdown_control_path = None
        return True

    def _request_shutdown_control(
        self,
        proc: subprocess.Popen,
        run_id: str,
        mode: str,
    ) -> bool:
        # Keep request publication linear per manager. A delayed preserve
        # publisher must never overwrite a concurrently upgraded close request.
        with self._shutdown_request_publish_lock:
            with self._lifecycle_lock:
                if (
                    self.proc is not proc
                    or self.run_id != run_id
                    or self._stop_requested_run_id != run_id
                ):
                    return False
                requested_mode = self._stop_requested_mode
                if requested_mode not in {CLOSE_POSITIONS, PRESERVE_POSITIONS}:
                    return False
                if mode == CLOSE_POSITIONS or requested_mode == CLOSE_POSITIONS:
                    publish_mode = CLOSE_POSITIONS
                else:
                    publish_mode = PRESERVE_POSITIONS
                path = self._shutdown_control_path
                if not path:
                    return False
            publish_shutdown_request(
                path,
                run_id=run_id,
                pid=int(proc.pid),
                mode=publish_mode,
                project_root=PROJECT_ROOT,
            )
        return True

    def _mark_runtime_stopped(self, returncode=None, *,
                              expected_run_id: str | None = None,
                              expected_pid: int | None = None,
                              stopped_by: str = "launcher") -> None:
        if not self.bot_name:
            return
        try:
            from core.runtime_status import read_runtime_status, write_runtime_status
            from launcher.config.settings import BOT_META
            with self._lifecycle_lock:
                expected_run_id = expected_run_id or ""
                expected_pid = positive_int_or_zero(expected_pid)
                current_run_id = str(self.run_id or "")
                current_pid = positive_int_or_zero(
                    getattr(self.proc, "pid", 0)
                )
                if (
                    expected_run_id
                    and current_run_id
                    and current_run_id != expected_run_id
                ):
                    return
                if expected_pid and current_pid and current_pid != expected_pid:
                    return

                meta = BOT_META.get(self.bot_name, {})
                log_dir = meta.get("log_dir")
                if not log_dir:
                    return
                last = read_runtime_status(log_dir)
                last_run_id = str(last.get("run_id") or "")
                last_pid = positive_int_or_zero(last.get("pid"))
                last_simulation = strict_bool_or_none(last.get("simulation"))
                start_config = (
                    self.start_config
                    if isinstance(self.start_config, Mapping)
                    else {}
                )
                start_simulation = strict_bool_or_none(
                    start_config.get("SIMULATION")
                )
                raw_open_positions = last.get("open_positions")
                open_positions = nonnegative_int_or_zero(raw_open_positions)
                open_positions_known = (
                    not isinstance(raw_open_positions, bool)
                    and isinstance(raw_open_positions, int)
                    and raw_open_positions >= 0
                    and strict_bool_or_none(
                        last.get("open_positions_known")
                    )
                    is not False
                )
                if (
                    expected_run_id
                    and last_run_id
                    and last_run_id != expected_run_id
                ):
                    return
                if expected_pid and last_pid and last_pid != expected_pid:
                    return
                if (
                    expected_run_id
                    and expected_pid
                    and last_run_id == expected_run_id
                    and last_pid == expected_pid
                    and last.get("launcher_terminal_marker") is True
                    and str(last.get("status") or "").lower()
                    in {"stopped", "degraded"}
                    and str(last.get("stopped_by") or "")
                    in {"launcher", "process_exit"}
                    and last.get("returncode") == returncode
                ):
                    return

                status, shutdown = _launcher_shutdown_payload(
                    last.get("shutdown")
                )
                extra = {
                    "open_positions": open_positions,
                    "open_positions_known": open_positions_known,
                    "launcher_terminal_marker": True,
                    "previous_status": str(last.get("status") or ""),
                    "stopped_by": (
                        stopped_by
                        if stopped_by in {"launcher", "process_exit"}
                        else "launcher"
                    ),
                    "returncode": returncode,
                    "shutdown": shutdown,
                }
                attempts = max(1, int(_TERMINAL_STATUS_PUBLISH_ATTEMPTS))
                published = False
                for attempt in range(attempts):
                    write_result = write_runtime_status(
                        log_dir,
                        self.bot_name,
                        status,
                        (
                            last_simulation
                            if last_simulation is not None
                            else start_simulation
                            if start_simulation is not None
                            else True
                        ),
                        threads={
                            "monitor": False,
                            "scan": False,
                            "reconcile": False,
                        },
                        process_pid=expected_pid or last_pid or 0,
                        process_run_id=expected_run_id or last_run_id,
                        extra=extra,
                    )
                    if write_result is not False:
                        published = True
                        break
                    if attempt + 1 < attempts:
                        time.sleep(
                            max(
                                0.0,
                                float(_TERMINAL_STATUS_RETRY_INTERVAL_SEC),
                            )
                        )
                if not published:
                    raise RuntimeError(
                        "runtime status writer reported publish failure"
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
            If ``True``, request a clean shutdown which closes positions. If
            ``False``, request a clean shutdown which preserves positions.
            Unmanaged legacy children retain the hard-stop fallback.

        The graceful path allows 75s total (see ``_GRACEFUL_TIMEOUT_SEC``) so
        there is headroom past the bot's own SHUTDOWN_DEADLINE_SEC for the
        thread-join cleanup and interpreter shutdown. Escalation after that is
        withheld while a fresh PID-/run-ID-bound status proves that the
        position action remains incomplete. ``self.proc`` is cleared the
        moment the OS-level process exits  before the teardown finishes  so
        the launcher's
        ``is_running()`` poll sees the death immediately.
        """
        # Snapshot proc under the lock, then release the lock for the
        # long blocking wait. Holding the lifecycle_lock for 75s would
        # block ``is_running()`` if it ever needed the same lock  and
        # also block any concurrent start() call from happening once
        # the process is genuinely dead.
        control_mode = CLOSE_POSITIONS if graceful_close else PRESERVE_POSITIONS
        already_exited: tuple[int | None, str, int, str | None] | None = None
        previous_stop_mode = None
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
                if self._stop_requested_run_id == run_id_to_mark:
                    self._stop_requested_run_id = None
                    self._stop_requested_mode = None
                already_exited = (
                    returncode_to_mark,
                    run_id_to_mark,
                    pid_to_mark,
                    getattr(self, "_shutdown_control_path", None),
                )
            else:
                proc_to_stop = self.proc
                run_id_to_stop = self.run_id or ""
                pid_to_stop = proc_to_stop.pid
                control_path_to_stop = getattr(
                    self, "_shutdown_control_path", None
                )
                if self._stop_requested_run_id == run_id_to_stop:
                    previous_stop_mode = self._stop_requested_mode
                if (
                    self._stop_requested_run_id == run_id_to_stop
                    and self._stop_requested_mode == CLOSE_POSITIONS
                    and control_mode != CLOSE_POSITIONS
                ):
                    raise RuntimeError(
                        "close-position shutdown is already pending; "
                        "preserve-position stop rejected"
                    )
                self._stop_requested_run_id = run_id_to_stop
                self._stop_requested_mode = control_mode
        if already_exited is not None:
            (
                returncode_to_mark,
                run_id_to_mark,
                pid_to_mark,
                control_path_to_mark,
            ) = already_exited
            if close_failed_start_stdout:
                self._close_process_stdout(proc_to_mark)
            self._cleanup_shutdown_control(control_path_to_mark)
            self._mark_runtime_stopped(
                returncode_to_mark,
                expected_run_id=run_id_to_mark,
                expected_pid=pid_to_mark,
            )
            return pid_to_mark

        # Publish the authoritative run-bound request before attempting the
        # optional console signal. Hidden Windows children can acknowledge
        # CTRL_BREAK without ever receiving it; the control record is the
        # reliable path and is consumed by the bot's main thread.
        control_ok = False
        if self.supports_graceful:
            try:
                control_ok = self._request_shutdown_control(
                    proc_to_stop,
                    run_id_to_stop,
                    control_mode,
                )
            except Exception as exc:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(
                            "[BotProcess] shutdown control request failed: "
                            f"{type(exc).__name__}: {exc}\n"
                        )
                    except Exception:
                        pass

        # Send the signal OUTSIDE the lock only as a fallback when publishing
        # the authoritative run-bound request failed. Hidden ``pythonw``
        # children commonly have no usable Windows console handle; attempting
        # CTRL_BREAK after a successful control publication only creates a
        # misleading WinError 6 while adding no shutdown guarantee.
        signal_ok = control_ok
        if (
            graceful_close
            and self.supports_graceful
            and (
                not control_ok
                or previous_stop_mode == PRESERVE_POSITIONS
            )
        ):
            try:
                if sys.platform == "win32":
                    # CTRL_BREAK_EVENT works only with CREATE_NEW_PROCESS_GROUP.
                    # Best-effort wakeup only. The run-bound control request
                    # remains authoritative if the console handle is invalid
                    # or Windows reports success without delivering the event.
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
                            + (
                                "- shutdown control remains active\n"
                                if control_ok
                                else "- using terminate()\n"
                            )
                        )
                    except Exception:
                        pass

        if not graceful_close and self.supports_graceful and not control_ok:
            with self._lifecycle_lock:
                if self._stop_requested_run_id == run_id_to_stop:
                    self._stop_requested_run_id = None
                    self._stop_requested_mode = None
            raise RuntimeError(
                "preserve-position shutdown control unavailable; "
                "bot process left running"
            )

        if signal_ok:
            # Signal sent  give the bot time to clean up gracefully.
            try:
                proc_to_stop.wait(timeout=self._GRACEFUL_TIMEOUT_SEC)
            except Exception as exc:
                if not graceful_close and control_ok:
                    raise RuntimeError(
                        "preserve-position shutdown did not finish; "
                        "bot process left running"
                    ) from exc
                retention_decision: bool | None = False
                if graceful_close and self.bot_name:
                    retention_decision = None
                    try:
                        from core.runtime_status import read_runtime_status
                        from launcher.config.settings import BOT_META

                        log_dir = BOT_META.get(self.bot_name, {}).get("log_dir")
                        if log_dir:
                            retention_decision = (
                                _fresh_runtime_status_requires_close_retention(
                                    read_runtime_status(log_dir),
                                    expected_run_id=run_id_to_stop,
                                    expected_pid=pid_to_stop,
                                )
                            )
                    except Exception:
                        retention_decision = None
                if retention_decision is not False and proc_to_stop.poll() is None:
                    raise RuntimeError(
                        "close-position shutdown remains incomplete; "
                        "bot process left running"
                    ) from exc
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
            if self._stop_requested_run_id == run_id_to_stop:
                self._stop_requested_run_id = None
                self._stop_requested_mode = None
            close_failed_start_stdout = (
                self._failed_start_owned_proc is proc_to_stop
            )
            if close_failed_start_stdout:
                self._failed_start_owned_proc = None
        if close_failed_start_stdout:
            self._close_process_stdout(proc_to_stop)
        self._cleanup_shutdown_control(control_path_to_stop)
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
        proc = self.proc
        if proc is None:
            return False
        try:
            return proc.poll() is None
        except Exception:
            # An invalid/transient Windows process handle is an unknown
            # lifecycle state, never proof of exit. Keep the bot owned and
            # block duplicate start/stop decisions until a later probe can
            # determine the real state.
            return True

    def close_shutdown_pending(self) -> bool:
        """Return whether the current run owns an unresolved close request."""
        with self._lifecycle_lock:
            return bool(
                self.run_id
                and self._stop_requested_run_id == self.run_id
                and self._stop_requested_mode == CLOSE_POSITIONS
            )

    #  Stdout reader 

    def _enqueue_log_line(
        self,
        line: str,
        *,
        expected_run_id: str | None = None,
    ) -> None:
        """Queue one line without ever blocking the bot process."""
        self._enqueue_redacted_log_line(
            _redact_ui_log_line(line),
            expected_run_id=expected_run_id,
        )

    def _enqueue_redacted_log_line(
        self,
        line: str,
        *,
        expected_run_id: str | None = None,
    ) -> None:
        """Queue text that already crossed the subprocess redaction boundary."""
        if expected_run_id is not None and self.run_id != expected_run_id:
            return
        try:
            self.log_queue.put_nowait(line)
        except queue.Full:
            try:
                self.log_queue.get_nowait()
                self.log_queue.put_nowait(line)
            except (queue.Empty, queue.Full):
                pass

    def _reader(
        self,
        proc: subprocess.Popen,
        expected_run_id: str | None = None,
        expected_control_path: str | None = None,
        require_owned_exit: bool = False,
    ) -> None:
        if not proc or not proc.stdout:
            return
        if expected_run_id is None:
            expected_run_id = self.run_id
        stdout = proc.stdout
        compactor = RepeatedLogCompactor()
        pipe_error_reported = False
        try:
            reader = _BoundedStdoutLineReader(stdout)
            while self.run_id == expected_run_id:
                try:
                    line, line_truncated = reader.read()
                except StopIteration:
                    break
                except OSError as exc:
                    try:
                        process_alive = proc.poll() is None
                    except Exception:
                        process_alive = True
                    if not process_alive:
                        break
                    if not pipe_error_reported:
                        self._enqueue_log_line(
                            "WARN [launcher] "
                            f"{self.bot_name or 'bot'} stdout read interrupted "
                            f"({type(exc).__name__}); retrying",
                            expected_run_id=expected_run_id,
                        )
                        pipe_error_reported = True
                    # A transient Windows pipe read error must not abandon the
                    # live child's only consumer. Retain the same iterator and
                    # run generation, with a bounded retry cadence so a broken
                    # handle cannot spin one CPU core.
                    time.sleep(0.1)
                    continue
                if self.run_id != expected_run_id:
                    break
                line = line.rstrip("\n").rstrip("\r")
                if line and "\r" not in line:
                    if line_truncated:
                        line += _UI_LOG_LINE_TRUNCATED
                    line = _redact_ui_log_line(line)
                    # Drop-oldest policy: if the queue is full, ditch the
                    # oldest line and append the new one. Without this the
                    # reader could block forever and new logs would stop
                    # appearing in the UI.
                    for output_line in compactor.push(line):
                        self._enqueue_redacted_log_line(
                            output_line,
                            expected_run_id=expected_run_id,
                        )
        finally:
            if self.run_id == expected_run_id:
                for output_line in compactor.flush():
                    self._enqueue_redacted_log_line(
                        output_line,
                        expected_run_id=expected_run_id,
                    )
            try:
                stdout.close()
            except Exception:
                pass
            # EOF normally means the owned child exited.  Publish that fact
            # immediately instead of leaving its last ``ready`` heartbeat for
            # the next launcher session to diagnose as a stale live PID.  A
            # process is cleared only when both its object and run generation
            # are still current; concurrent stop/restart paths retain authority
            # over every other generation.
            returncode = None
            with self._lifecycle_lock:
                retain_exit_watch = (
                    self.proc is proc
                    and self.run_id == expected_run_id
                    and getattr(self, "_stop_requested_run_id", None)
                    == expected_run_id
                )
            while retain_exit_watch and (
                self.proc is proc and self.run_id == expected_run_id
            ):
                try:
                    returncode = proc.poll()
                except Exception:
                    returncode = None
                if returncode is not None:
                    break
                time.sleep(max(0.01, float(_PROCESS_EXIT_POLL_INTERVAL_SEC)))
            if not retain_exit_watch:
                try:
                    returncode = proc.poll()
                    if returncode is None:
                        try:
                            returncode = proc.wait(timeout=1.0)
                        except subprocess.TimeoutExpired:
                            returncode = None
                except Exception:
                    returncode = None
            if returncode is not None:
                owned_exit = False
                expected_stop = False
                expected_stop_mode = None
                with self._lifecycle_lock:
                    if self.proc is proc and self.run_id == expected_run_id:
                        expected_stop = (
                            getattr(self, "_stop_requested_run_id", None)
                            == expected_run_id
                        )
                        if expected_stop:
                            expected_stop_mode = getattr(
                                self,
                                "_stop_requested_mode",
                                None,
                            )
                        self.proc = None
                        if expected_stop:
                            self._stop_requested_run_id = None
                            self._stop_requested_mode = None
                        if self._failed_start_owned_proc is proc:
                            self._failed_start_owned_proc = None
                        owned_exit = True
                if owned_exit:
                    self._cleanup_shutdown_control(expected_control_path)
                    report_exit = not expected_stop or (
                        self.supports_graceful
                        and returncode != 0
                        and expected_stop_mode
                        in {CLOSE_POSITIONS, PRESERVE_POSITIONS}
                    )
                    if report_exit:
                        context = (
                            "during launcher stop"
                            if expected_stop
                            else "unexpectedly"
                        )
                        self._enqueue_log_line(
                            "ERROR [launcher] "
                            f"{self.bot_name or 'bot'} process exited {context} "
                            f"(returncode={returncode})",
                            expected_run_id=expected_run_id,
                        )
                    self._mark_runtime_stopped(
                        returncode,
                        expected_run_id=expected_run_id,
                        expected_pid=getattr(proc, "pid", 0),
                        stopped_by=(
                            "launcher" if expected_stop else "process_exit"
                        ),
                    )
            elif require_owned_exit:
                with self._lifecycle_lock:
                    owned_live_process = (
                        self.proc is proc and self.run_id == expected_run_id
                    )
                if owned_live_process:
                    raise RuntimeError(
                        "stdout reader reached EOF while its owned child "
                        "process is still running"
                    )
