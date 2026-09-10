"""
event_bus.py  Thread-safe pub/sub event bus.

High-frequency events (TICKER_UPDATED) are suppressed from both wildcard
subscribers and the history ring buffer; history entries larger than
_HISTORY_PAYLOAD_MAX_CHARS are truncated in the stored copy. The hot path
is unaffected.
"""
from __future__ import annotations

import copy
import json
import math
import queue
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from bot_utils.runtime_threads import thread_definitely_never_started

Handler = Callable[[str, Dict[str, Any]], None]

_CRITICAL_EVENTS = frozenset({
    "POSITION_OPENED", "POSITION_CLOSED", "POSITION_FAILED",
    "STOP_LOSS_TRIGGERED", "ORDER_PLACED", "ORDER_FILLED",
    "BOT_STARTED", "BOT_STOPPED", "BOT_PAUSED",
})

_SUPPRESS_FROM_WILDCARD = frozenset({"TICKER_UPDATED"})
# also suppress from history
_SUPPRESS_FROM_HISTORY  = frozenset({"TICKER_UPDATED"})

_CRITICAL_PUT_TIMEOUT = 2.0
# cap stored payload size in history (chars in JSON-dumped form)
_HISTORY_PAYLOAD_MAX_CHARS = 2048
_WORKER_STOP = object()
_START_QUARANTINE_LOCK = threading.Lock()
_START_QUARANTINE: list[dict] = []


def _quarantine_start_generations(generations) -> None:
    with _START_QUARANTINE_LOCK:
        for generation in generations:
            if not any(
                existing is generation for existing in _START_QUARANTINE
            ):
                _START_QUARANTINE.append(generation)


def _note_start_cleanup_error(
    primary: BaseException,
    cleanup: BaseException,
) -> None:
    try:
        primary.add_note(
            "event bus startup cleanup failed: "
            f"{type(cleanup).__name__}: {cleanup}"
        )
    except BaseException:
        pass


def _log_handler_error(context: str, exc: Exception) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(context, exc)
    except Exception:
        pass


class Event:
    __slots__ = ("event_type", "payload", "timestamp", "emitted_by", "seq")

    _seq_counter = 0
    _seq_lock    = threading.Lock()

    def __init__(self, event_type: str, payload: dict, emitted_by: str = ""):
        self.event_type = event_type
        self.payload    = payload
        self.timestamp  = datetime.now(timezone.utc).replace(tzinfo=None).strftime(
            "%Y-%m-%d %H:%M:%S")
        self.emitted_by = emitted_by
        with Event._seq_lock:
            Event._seq_counter += 1
            self.seq = Event._seq_counter

    def to_dict(self) -> dict:
        try:
            payload = copy.deepcopy(self.payload)
        except Exception:
            payload = dict(self.payload)
        return {
            "event_type": self.event_type,
            "payload":    payload,
            "timestamp":  self.timestamp,
            "emitted_by": self.emitted_by,
            "seq":        self.seq,
        }


def _truncate_for_history(payload: dict) -> dict:
    """Shrink large payloads to keep memory bounded."""
    try:
        s = json.dumps(payload, default=str, allow_nan=False)
        if len(s) <= _HISTORY_PAYLOAD_MAX_CHARS:
            # Round-tripping creates an immutable history snapshot instead of
            # retaining nested references owned by the emitter.
            return json.loads(s)
        # Keep only top-level scalar fields
        scalar_only = {
            k: v for k, v in payload.items()
            if isinstance(v, (str, int, float, bool, type(None)))
        }
        scalar_only["_truncated"] = True
        scalar_only["_orig_chars"] = len(s)
        if len(json.dumps(
            scalar_only,
            default=str,
            allow_nan=False,
        )) > _HISTORY_PAYLOAD_MAX_CHARS:
            return {
                "_truncated": True,
                "_orig_chars": len(s),
            }
        return scalar_only
    except Exception:
        return {"_unserializable": True}


def _payload_for_handler(event_type: str, payload: dict) -> dict:
    """Isolate critical handlers so one cannot corrupt another's view."""
    if event_type not in _CRITICAL_EVENTS:
        return payload
    try:
        return copy.deepcopy(payload)
    except Exception:
        return dict(payload)


def _coerce_payload(payload: Any) -> dict:
    if payload is None:
        return {}
    try:
        return dict(payload)
    except Exception:
        return {"_invalid_payload_type": type(payload).__name__}


class EventBus:
    def __init__(
        self,
        worker_threads: int = 2,
        history_size: int = 500,
        *,
        lazy_workers: bool = False,
    ):
        self._lock         = threading.Lock()
        self._handlers: Dict[str, List[Handler]] = defaultdict(list)
        self._wildcard: List[Handler]            = []
        self._work_queue   = queue.Queue(maxsize=2000)
        self._history      = deque(maxlen=history_size)
        self._history_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._publish_condition = threading.Condition(self._publish_lock)
        self._publish_inflight = 0
        self._sync_publish_local = threading.local()
        self._accepting = True
        self._shutdown_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._stopped      = False
        self._n_workers    = worker_threads
        self._worker_threads: List[threading.Thread] = []
        self._worker_generations: dict[object, dict] = {}
        self._startup_lock = threading.Lock()
        self._watchdog: threading.Thread | None = None
        self._watchdog_generation: dict | None = None
        if not lazy_workers:
            self._ensure_consumers_started()

    def _ensure_consumers_started(self) -> None:
        """Start consumers once, only when a lazy bus gains a subscriber."""
        with self._startup_lock:
            if self._stopped or self._shutdown_event.is_set():
                return
            if self._generation_unresolved(self._watchdog_generation):
                return
            self._worker_threads = self._retain_unresolved_workers()
            for _ in range(self._n_workers - len(self._worker_threads)):
                self._spawn_worker(len(self._worker_threads))
            generation = {"thread": None, "done": threading.Event()}

            def run_watchdog_generation() -> None:
                try:
                    self._watchdog_loop()
                finally:
                    generation["done"].set()

            try:
                candidate = threading.Thread(
                    target=run_watchdog_generation,
                    name="event-bus-watchdog",
                    daemon=True,
                )
            except BaseException as exc:
                self._accepting = False
                self._stopped = True
                self._shutdown_event.set()
                _quarantine_start_generations(
                    tuple(self._worker_generations.values())
                )
                for _ in self._worker_threads:
                    try:
                        self._work_queue.put_nowait(_WORKER_STOP)
                    except queue.Full:
                        break
                    except BaseException as cleanup_error:
                        _note_start_cleanup_error(exc, cleanup_error)
                        break
                for worker in self._worker_threads:
                    try:
                        worker.join(timeout=1.0)
                    except BaseException as cleanup_error:
                        _note_start_cleanup_error(exc, cleanup_error)
                _quarantine_start_generations(
                    tuple(self._worker_generations.values())
                )
                try:
                    _log_handler_error("event_bus watchdog construction", exc)
                except BaseException as cleanup_error:
                    _note_start_cleanup_error(exc, cleanup_error)
                raise
            generation["thread"] = candidate
            self._watchdog = candidate
            self._watchdog_generation = generation
            try:
                candidate.start()
            except BaseException as exc:
                definite_prelaunch = (
                    isinstance(exc, Exception)
                    and thread_definitely_never_started(candidate)
                )
                if definite_prelaunch:
                    generation["done"].set()
                    if self._watchdog_generation is generation:
                        self._watchdog_generation = None
                        self._watchdog = None
                else:
                    _quarantine_start_generations((generation,))
                # Constructor/lazy-start failure must not strand consumers.
                self._accepting = False
                self._stopped = True
                self._shutdown_event.set()
                _quarantine_start_generations(
                    tuple(self._worker_generations.values())
                )
                for _ in self._worker_threads:
                    try:
                        self._work_queue.put_nowait(_WORKER_STOP)
                    except queue.Full:
                        break
                    except BaseException as cleanup_error:
                        _note_start_cleanup_error(exc, cleanup_error)
                        break
                for worker in self._worker_threads:
                    try:
                        worker.join(timeout=1.0)
                    except BaseException as cleanup_error:
                        _note_start_cleanup_error(exc, cleanup_error)
                self._quarantine_unresolved_worker_generations()
                try:
                    _log_handler_error("event_bus watchdog start", exc)
                except BaseException as cleanup_error:
                    _note_start_cleanup_error(exc, cleanup_error)
                if not isinstance(exc, Exception):
                    raise
                raise

    def subscribe(self, event_type: str, handler: Handler,
                  *, replace: bool = False) -> None:
        self._ensure_consumers_started()
        with self._lock:
            if event_type == "*":
                if replace:
                    self._wildcard.clear()
                if handler not in self._wildcard:
                    self._wildcard.append(handler)
            else:
                if replace:
                    self._handlers[event_type] = [handler]
                elif handler not in self._handlers[event_type]:
                    self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: str, handler: Handler) -> None:
        with self._lock:
            if event_type == "*":
                try:
                    self._wildcard.remove(handler)
                except ValueError:
                    pass
            else:
                lst = self._handlers.get(event_type, [])
                try:
                    lst.remove(handler)
                except ValueError:
                    pass

    def emit(self, event_type: str, payload: dict = None,
             emitted_by: str = "") -> None:
        """Publish ``event_type`` to subscribers.

        Critical events (``_CRITICAL_EVENTS``) get a per-handler try/except so a
        transiently-full queue can't lose the event on every handler at once 
        queue.Full is recorded via ``_emergency_log`` for that handler only and
        the next handler still gets a chance. Their payload is also deep-copied
        before dispatch so one handler can't mutate another handler's view of a
        nested field.
        """
        # Deep-copy payload for critical events so handlers can't mutate each
        # other's view. Non-critical events keep a shallow copy (cheaper) 
        # those are usually high-frequency and the handlers are read-only.
        base_payload = _coerce_payload(payload)
        if event_type in _CRITICAL_EVENTS:
            try:
                safe_payload = copy.deepcopy(base_payload)
            except Exception:
                # Some payload contained an unpicklable object (e.g.
                # an exchange handle). Fall back to shallow.
                safe_payload = dict(base_payload)
        else:
            safe_payload = base_payload

        event = Event(event_type, safe_payload, emitted_by)

        # Admission is serialized with shutdown, but queue backpressure is not:
        # one full critical-handler queue must not freeze unrelated emitters.
        with self._publish_condition:
            if not self._accepting or self._stopped:
                return
            self._publish_inflight += 1

        try:
            # don't pollute history with high-frequency events
            if event_type not in _SUPPRESS_FROM_HISTORY:
                stored_payload = _truncate_for_history(safe_payload)
                # History is a view of the same emission, not a second event.
                # A shallow Event copy preserves timestamp/sequence without
                # consuming a hidden global sequence number.
                stored_event = copy.copy(event)
                stored_event.payload = stored_payload
                with self._history_lock:
                    self._history.append(stored_event)

            with self._lock:
                specific = list(self._handlers.get(event_type, []))
                if event_type in _SUPPRESS_FROM_WILDCARD:
                    handlers = specific
                else:
                    handlers = specific + list(self._wildcard)

            if not handlers:
                return

            is_critical = event_type in _CRITICAL_EVENTS
            if is_critical:
                # One total deadline bounds the calling trading thread even
                # when many subscribers share a full queue. Every handler is
                # still attempted or recorded via the emergency path.
                publish_deadline = time.monotonic() + _CRITICAL_PUT_TIMEOUT
                for handler in handlers:
                    try:
                        remaining = max(
                            0.0,
                            publish_deadline - time.monotonic(),
                        )
                        self._work_queue.put(
                            (handler, event), timeout=remaining)
                    except queue.Full:
                        # Record AND keep going  losing the DB-logger
                        # handler shouldn't lose the Telegram-alert one.
                        self._emergency_log(event)
            else:
                for handler in handlers:
                    try:
                        self._work_queue.put_nowait((handler, event))
                    except queue.Full:
                        pass
        finally:
            with self._publish_condition:
                self._publish_inflight -= 1
                if self._publish_inflight == 0:
                    # A bounded shutdown may have returned before this
                    # already-admitted publisher finished. Complete the
                    # terminal transition here so workers drain the accepted
                    # queue and then leave instead of surviving forever with
                    # admission and the watchdog already disabled.
                    if not self._accepting:
                        self._stopped = True
                    self._publish_condition.notify_all()

    def emit_sync(self, event_type: str, payload: dict = None,
                  emitted_by: str = "") -> None:
        # Serialize admission with shutdown just like emit(). During a bounded
        # shutdown an older async publisher may still be finishing while
        # ``_accepting`` is already false and ``_stopped`` is not set yet.
        # Synchronous critical events must not bypass that closed boundary.
        with self._publish_condition:
            if not self._accepting or self._stopped:
                return
            self._publish_inflight += 1
        previous_sync_depth = int(
            getattr(self._sync_publish_local, "depth", 0)
        )
        self._sync_publish_local.depth = previous_sync_depth + 1
        try:
            safe_payload = _coerce_payload(payload)
            event = Event(event_type, safe_payload, emitted_by)

            with self._lock:
                specific = list(self._handlers.get(event_type, []))
                if event_type in _SUPPRESS_FROM_WILDCARD:
                    handlers = specific
                else:
                    handlers = specific + list(self._wildcard)

            for handler in handlers:
                try:
                    handler(
                        event.event_type,
                        _payload_for_handler(event.event_type, event.payload),
                    )
                except Exception as exc:
                    _log_handler_error(
                        f"event_bus emit_sync {event.event_type}", exc
                    )
        finally:
            if previous_sync_depth:
                self._sync_publish_local.depth = previous_sync_depth
            else:
                try:
                    del self._sync_publish_local.depth
                except AttributeError:
                    pass
            with self._publish_condition:
                self._publish_inflight -= 1
                if self._publish_inflight == 0:
                    if not self._accepting:
                        self._stopped = True
                    self._publish_condition.notify_all()

    def get_history(self, event_type: str = None, limit: int = 100) -> List[dict]:
        if limit <= 0:
            return []
        with self._history_lock:
            events = list(self._history)
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        return [e.to_dict() for e in reversed(events[-limit:])]

    def _spawn_worker(self, idx: int) -> bool:
        generation = {"thread": None, "done": threading.Event()}

        def run_worker_generation() -> None:
            try:
                self._worker()
            finally:
                generation["done"].set()

        candidate = None
        try:
            candidate = threading.Thread(
                target=run_worker_generation,
                name=f"event-bus-worker-{idx}",
                daemon=True,
            )
        except BaseException as exc:
            if not isinstance(exc, Exception):
                self._abort_worker_startup(exc)
                raise
            self._log_recoverable_worker_start_error(
                "event_bus worker construction",
                exc,
            )
            return False
        generation["thread"] = candidate
        self._worker_threads.append(candidate)
        self._worker_generations[candidate] = generation
        try:
            candidate.start()
        except BaseException as exc:
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(candidate)
            ):
                generation["done"].set()
                if self._worker_generations.get(candidate) is generation:
                    self._worker_generations.pop(candidate, None)
                    self._worker_threads = [
                        thread
                        for thread in self._worker_threads
                        if thread is not candidate
                    ]
            if not isinstance(exc, Exception):
                self._abort_worker_startup(exc)
                try:
                    _log_handler_error("event_bus worker start", exc)
                except BaseException as cleanup_error:
                    _note_start_cleanup_error(exc, cleanup_error)
                raise
            self._log_recoverable_worker_start_error(
                "event_bus worker start",
                exc,
            )
            return False
        return True

    def _log_recoverable_worker_start_error(
        self,
        context: str,
        primary: Exception,
    ) -> None:
        """Report a recoverable start error without losing worker ownership."""
        try:
            _log_handler_error(context, primary)
        except BaseException as reporting_error:
            _note_start_cleanup_error(primary, reporting_error)
            try:
                self._abort_worker_startup(primary)
            except BaseException as cleanup_error:
                _note_start_cleanup_error(primary, cleanup_error)
            raise primary

    @staticmethod
    def _generation_unresolved(generation: dict | None) -> bool:
        if generation is None or not generation["done"].is_set():
            return generation is not None
        thread = generation.get("thread")
        if thread is None:
            return False
        try:
            return bool(thread.is_alive())
        except BaseException:
            return True

    def _worker_generation_unresolved(self, thread: object) -> bool:
        generation = self._worker_generations.get(thread)
        if generation is not None:
            return self._generation_unresolved(generation)
        try:
            return bool(thread.is_alive())
        except BaseException:
            return True

    def _retain_unresolved_workers(self) -> list:
        retained = []
        for thread in self._worker_threads:
            if self._worker_generation_unresolved(thread):
                retained.append(thread)
            else:
                self._worker_generations.pop(thread, None)
        return retained

    def _quarantine_unresolved_worker_generations(self) -> None:
        generations = []
        for thread in self._worker_threads:
            generation = self._worker_generations.get(thread)
            if (
                generation is not None
                and self._generation_unresolved(generation)
            ):
                generations.append(generation)
        _quarantine_start_generations(generations)

    def _abort_worker_startup(self, primary: BaseException) -> None:
        """Terminally retain every worker before propagating a fatal start."""
        self._accepting = False
        self._stopped = True
        self._shutdown_event.set()
        _quarantine_start_generations(
            tuple(self._worker_generations.values())
        )
        for _ in self._worker_threads:
            try:
                self._work_queue.put_nowait(_WORKER_STOP)
            except queue.Full:
                break
            except BaseException as cleanup_error:
                _note_start_cleanup_error(primary, cleanup_error)
                break
        for worker in self._worker_threads:
            try:
                worker.join(timeout=1.0)
            except BaseException as cleanup_error:
                _note_start_cleanup_error(primary, cleanup_error)
        self._quarantine_unresolved_worker_generations()

    def _worker(self) -> None:
        while True:
            try:
                try:
                    work_item = self._work_queue.get(timeout=1.0)
                except queue.Empty:
                    if self._stopped:
                        return
                    continue
                if work_item is _WORKER_STOP:
                    self._work_queue.task_done()
                    return
                handler, event = work_item
                try:
                    handler(
                        event.event_type,
                        _payload_for_handler(event.event_type, event.payload),
                    )
                except Exception as exc:
                    _log_handler_error(f"event_bus worker {event.event_type}", exc)
                finally:
                    try:
                        self._work_queue.task_done()
                    except Exception:
                        pass
            except Exception as exc:
                _log_handler_error("event_bus worker loop", exc)
                time.sleep(0.1)

    def _watchdog_loop(self) -> None:
        startup_lock = getattr(self, "_startup_lock", None)
        if startup_lock is None:
            startup_lock = threading.Lock()
            self._startup_lock = startup_lock
        while not self._shutdown_event.wait(5.0):
            try:
                with startup_lock:
                    alive = self._retain_unresolved_workers()
                    missing = self._n_workers - len(alive)
                    if (
                        missing > 0
                        and not self._stopped
                        and not self._shutdown_event.is_set()
                    ):
                        self._worker_threads = alive
                        for _ in range(missing):
                            self._spawn_worker(len(self._worker_threads))
            except Exception as exc:
                # Thread creation and liveness probes can fail transiently.
                # Keep the sole recovery owner alive for the next cycle.
                _log_handler_error("event_bus watchdog repair", exc)

    def _emergency_log(self, event: Event) -> None:
        try:
            from core.logger import log_struct
            scalar = {k: v for k, v in event.payload.items()
                      if isinstance(v, (str, int, float, bool, type(None)))}
            log_struct(
                "event_bus_overflow",
                event_type=event.event_type,
                emitted_by=event.emitted_by,
                seq=event.seq,
                **scalar,
            )
        except Exception:
            pass

    def shutdown(self, timeout: float = 5.0) -> bool:
        if isinstance(timeout, bool):
            return False
        try:
            requested_timeout = float(timeout)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(requested_timeout):
            return False
        budget = min(max(0.0, requested_timeout), threading.TIMEOUT_MAX)
        deadline = time.monotonic() + budget
        lock_timeout = min(
            max(0.0, deadline - time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        if not self._shutdown_lock.acquire(timeout=lock_timeout):
            return False
        try:
            startup_timeout = min(
                max(0.0, deadline - time.monotonic()),
                threading.TIMEOUT_MAX,
            )
            if not self._startup_lock.acquire(timeout=startup_timeout):
                with self._publish_condition:
                    self._accepting = False
                    self._shutdown_event.set()
                    self._publish_condition.notify_all()
                return False
            try:
                workers = list(self._worker_threads)
                if not self._stopped:
                    # Close admission first, then wait for already-admitted
                    # publishers. The startup lock makes the final worker
                    # snapshot authoritative against watchdog repair.
                    with self._publish_condition:
                        self._accepting = False
                        self._shutdown_event.set()
                        sync_depth = int(
                            getattr(self._sync_publish_local, "depth", 0)
                        )
                        while self._publish_inflight > sync_depth:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0.0:
                                break
                            self._publish_condition.wait(timeout=remaining)
                        publications_drained = (
                            self._publish_inflight <= sync_depth
                        )

                    if publications_drained:
                        workers = [
                            thread
                            for thread in self._worker_threads
                            if self._worker_generation_unresolved(thread)
                        ]
                        for _ in workers:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0.0:
                                break
                            try:
                                self._work_queue.put(
                                    _WORKER_STOP,
                                    timeout=remaining,
                                )
                            except queue.Full:
                                break
                        self._stopped = True
            finally:
                self._startup_lock.release()

            # Workers keep consuming already accepted work until the queue is
            # drained or the caller's shutdown budget is exhausted.
            current = threading.current_thread()
            called_from_worker = current in workers
            if not called_from_worker:
                while time.monotonic() < deadline:
                    try:
                        if self._work_queue.unfinished_tasks == 0:
                            break
                    except Exception:
                        break
                    time.sleep(0.05)

            self._shutdown_event.set()

            lifecycle_threads = list(workers)
            if self._watchdog_generation is not None:
                lifecycle_threads.append(self._watchdog)
            for thread in lifecycle_threads:
                if thread is current:
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    break
                try:
                    thread.join(timeout=remaining)
                except (RuntimeError, AttributeError):
                    pass
            with self._publish_condition:
                inflight = self._publish_inflight
            unfinished = self._work_queue.unfinished_tasks
            return (
                inflight == 0
                and unfinished == 0
                and not any(
                    self._generation_unresolved(
                        self._watchdog_generation
                    )
                    if thread is self._watchdog
                    else self._worker_generation_unresolved(thread)
                    for thread in lifecycle_threads
                )
            )
        finally:
            self._shutdown_lock.release()

    def __repr__(self) -> str:
        with self._lock:
            n_types    = len(self._handlers)
            n_handlers = sum(len(v) for v in self._handlers.values())
        return (f"EventBus(event_types={n_types}, "
                f"handlers={n_handlers}, history={len(self._history)})")


_BUS_LOCK: threading.Lock         = threading.Lock()
_BUS:      Optional[EventBus]     = None
_BUS_TERMINAL = False


def get_bus() -> EventBus:
    global _BUS
    with _BUS_LOCK:
        if _BUS_TERMINAL:
            raise RuntimeError("global event bus admission is terminally closed")
        if _BUS is None:
            _BUS = EventBus(
                worker_threads=2,
                history_size=500,
                lazy_workers=True,
            )
        return _BUS


def begin_global_bus_runtime() -> bool:
    """Explicitly reopen global admission before a new process runtime."""
    global _BUS_TERMINAL
    with _BUS_LOCK:
        if _BUS is not None:
            return False
        with _START_QUARANTINE_LOCK:
            if _START_QUARANTINE:
                return False
        _BUS_TERMINAL = False
        return True


def shutdown_global_bus(timeout: float = 2.0) -> bool:
    """Close and reset the process-global bus for truthful bot shutdown."""
    global _BUS, _BUS_TERMINAL
    if isinstance(timeout, bool):
        return False
    try:
        requested_timeout = float(timeout)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(requested_timeout):
        return False
    budget = min(max(0.0, requested_timeout), threading.TIMEOUT_MAX)
    deadline = time.monotonic() + budget
    if not _BUS_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        _BUS_TERMINAL = True
        bus = _BUS
    finally:
        _BUS_LOCK.release()
    if bus is not None and not bus.shutdown(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    if bus is not None:
        if not _BUS_LOCK.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        ):
            return False
        try:
            if _BUS is bus:
                _BUS = None
        finally:
            _BUS_LOCK.release()
    if not _START_QUARANTINE_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        generations = tuple(_START_QUARANTINE)
    finally:
        _START_QUARANTINE_LOCK.release()
    unresolved = []
    for generation in generations:
        thread = generation.get("thread")
        if thread is not None and thread is not threading.current_thread():
            try:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
            except Exception:
                unresolved.append(generation)
                continue
        if EventBus._generation_unresolved(generation):
            unresolved.append(generation)
    if not _START_QUARANTINE_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        _START_QUARANTINE[:] = [
            generation
            for generation in _START_QUARANTINE
            if generation not in generations or generation in unresolved
        ]
    finally:
        _START_QUARANTINE_LOCK.release()
    return not unresolved


def _console_log_handler(event_type: str, payload: dict) -> None:
    ts  = datetime.now().strftime("%H:%M:%S")
    sym = payload.get("symbol", "")
    bot = payload.get("bot_name", "")
    prefix = f"[{ts}][BUS][{bot}]" if bot else f"[{ts}][BUS]"
    print(f"{prefix} {event_type}" + (f" {sym}" if sym else ""))


def _struct_log_handler(event_type: str, payload: dict) -> None:
    try:
        from core.logger import log_struct
        log_struct(event_type, **payload)
    except Exception:
        pass


_REGISTER_LOCK      = threading.Lock()


def register_console_logger(bus: EventBus = None) -> None:
    """Idempotently register the console handler on this bus instance."""
    with _REGISTER_LOCK:
        if bus is None:
            bus = get_bus()
        for et in ("POSITION_OPENED", "POSITION_CLOSED", "STOP_LOSS_TRIGGERED",
                   "BOT_STARTED", "BOT_STOPPED", "BOT_PAUSED",
                   "API_RATE_LIMITED", "LLM_ONLINE", "LLM_OFFLINE",
                   "POSITION_FAILED"):
            bus.subscribe(et, _console_log_handler)


def register_structured_logger(bus: EventBus = None) -> None:
    """Idempotently register the structured handler on this bus instance."""
    with _REGISTER_LOCK:
        if bus is None:
            bus = get_bus()
        bus.subscribe("*", _struct_log_handler)
