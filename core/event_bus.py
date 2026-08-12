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
import queue
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

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
    def __init__(self, worker_threads: int = 2, history_size: int = 500):
        self._lock         = threading.Lock()
        self._handlers: Dict[str, List[Handler]] = defaultdict(list)
        self._wildcard: List[Handler]            = []
        self._work_queue   = queue.Queue(maxsize=2000)
        self._history      = deque(maxlen=history_size)
        self._history_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._publish_condition = threading.Condition(self._publish_lock)
        self._publish_inflight = 0
        self._accepting = True
        self._shutdown_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._stopped      = False
        self._n_workers    = worker_threads
        self._worker_threads: List[threading.Thread] = []

        for i in range(worker_threads):
            self._spawn_worker(i)

        self._watchdog = threading.Thread(
            target=self._watchdog_loop, name="event-bus-watchdog", daemon=True)
        try:
            self._watchdog.start()
        except Exception as exc:
            # Constructor failure must not strand already-started consumers
            # that no caller can ever shut down because the bus was not
            # returned. Close admission and wake every published worker.
            self._accepting = False
            self._stopped = True
            self._shutdown_event.set()
            for _ in self._worker_threads:
                try:
                    self._work_queue.put_nowait(_WORKER_STOP)
                except queue.Full:
                    break
            for worker in self._worker_threads:
                try:
                    worker.join(timeout=1.0)
                except (RuntimeError, AttributeError):
                    pass
            _log_handler_error("event_bus watchdog start", exc)
            raise

    def subscribe(self, event_type: str, handler: Handler,
                  *, replace: bool = False) -> None:
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
                # Per-handler deadline so one slow/full slot doesn't starve the
                # next. Each handler gets up to _CRITICAL_PUT_TIMEOUT seconds;
                # queue.Full on one handler is recorded via _emergency_log but
                # does NOT skip the remaining handlers.
                for handler in handlers:
                    try:
                        self._work_queue.put(
                            (handler, event), timeout=_CRITICAL_PUT_TIMEOUT)
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
        if self._stopped:
            return
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
                _log_handler_error(f"event_bus emit_sync {event.event_type}", exc)

    def get_history(self, event_type: str = None, limit: int = 100) -> List[dict]:
        with self._history_lock:
            events = list(self._history)
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        return [e.to_dict() for e in reversed(events[-limit:])]

    def _spawn_worker(self, idx: int) -> bool:
        try:
            candidate = threading.Thread(
                target=self._worker,
                name=f"event-bus-worker-{idx}",
                daemon=True,
            )
            candidate.start()
        except Exception as exc:
            _log_handler_error("event_bus worker start", exc)
            return False
        self._worker_threads.append(candidate)
        return True

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
        while not self._shutdown_event.wait(5.0):
            alive = [t for t in self._worker_threads if t.is_alive()]
            missing = self._n_workers - len(alive)
            if (missing > 0 and not self._stopped
                    and not self._shutdown_event.is_set()):
                self._worker_threads = alive
                for _ in range(missing):
                    self._spawn_worker(len(self._worker_threads))

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

    def shutdown(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout))
        with self._shutdown_lock:
            workers = list(self._worker_threads)
            if not self._stopped:
                # Close admission first, then wait for already-admitted
                # publishers without holding the admission lock. This preserves
                # FIFO sentinel ordering while avoiding a global emitter stall.
                with self._publish_condition:
                    self._accepting = False
                    self._shutdown_event.set()
                    while self._publish_inflight:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0.0:
                            break
                        self._publish_condition.wait(timeout=remaining)
                    publications_drained = self._publish_inflight == 0

                if publications_drained:
                    workers = [thread for thread in self._worker_threads
                               if thread.is_alive()]
                    for _ in workers:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0.0:
                            break
                        try:
                            self._work_queue.put(_WORKER_STOP, timeout=remaining)
                        except queue.Full:
                            break
                    self._stopped = True

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

            for thread in workers + [self._watchdog]:
                if thread is current:
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    break
                try:
                    thread.join(timeout=remaining)
                except (RuntimeError, AttributeError):
                    pass

    def __repr__(self) -> str:
        with self._lock:
            n_types    = len(self._handlers)
            n_handlers = sum(len(v) for v in self._handlers.values())
        return (f"EventBus(event_types={n_types}, "
                f"handlers={n_handlers}, history={len(self._history)})")


_BUS_LOCK: threading.Lock         = threading.Lock()
_BUS:      Optional[EventBus]     = None


def get_bus() -> EventBus:
    global _BUS
    if _BUS is None:
        with _BUS_LOCK:
            if _BUS is None:
                _BUS = EventBus(worker_threads=2, history_size=500)
    return _BUS


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


_CONSOLE_REGISTERED = False
_STRUCT_REGISTERED  = False
_REGISTER_LOCK      = threading.Lock()


def register_console_logger(bus: EventBus = None) -> None:
    """Idempotent. Multiple calls per process (e.g. bot restart in the
    same process) won't double-subscribe the handler."""
    global _CONSOLE_REGISTERED
    with _REGISTER_LOCK:
        if _CONSOLE_REGISTERED:
            return
        if bus is None:
            bus = get_bus()
        for et in ("POSITION_OPENED", "POSITION_CLOSED", "STOP_LOSS_TRIGGERED",
                   "BOT_STARTED", "BOT_STOPPED", "BOT_PAUSED",
                   "API_RATE_LIMITED", "LLM_ONLINE", "LLM_OFFLINE",
                   "POSITION_FAILED"):
            bus.subscribe(et, _console_log_handler)
        _CONSOLE_REGISTERED = True


def register_structured_logger(bus: EventBus = None) -> None:
    """Idempotent."""
    global _STRUCT_REGISTERED
    with _REGISTER_LOCK:
        if _STRUCT_REGISTERED:
            return
        if bus is None:
            bus = get_bus()
        bus.subscribe("*", _struct_log_handler)
        _STRUCT_REGISTERED = True
