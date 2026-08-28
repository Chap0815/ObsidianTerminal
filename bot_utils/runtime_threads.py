"""Small lifecycle helpers for bot-owned runtime threads."""
from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from collections.abc import Mapping


_SHUTDOWN_RETRY_INTERVAL_SEC = 0.25
_SHUTDOWN_RETRY_TIMEOUT_SEC = 10.0
_FINALIZATION_STATE_CREATION_LOCK = threading.Lock()


class _RuntimeShutdownFinalizationState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.retry_thread: threading.Thread | None = None
        self.complete = False
        self.closed_resources: set[str] = set()
        self.resource_closers: dict[str, object] = {}
        self.required_resources: set[str] = set()
        self.exchange_closed = False
        self.last_status_signature: tuple[object, ...] | None = None


def _shutdown_finalization_state(owner) -> _RuntimeShutdownFinalizationState:
    state = getattr(owner, "_runtime_shutdown_finalization_state", None)
    if isinstance(state, _RuntimeShutdownFinalizationState):
        return state
    with _FINALIZATION_STATE_CREATION_LOCK:
        state = getattr(owner, "_runtime_shutdown_finalization_state", None)
        if isinstance(state, _RuntimeShutdownFinalizationState):
            return state
        state = _RuntimeShutdownFinalizationState()
        setattr(owner, "_runtime_shutdown_finalization_state", state)
        return state


def _report_shutdown_error(owner, context: str, exc: Exception) -> None:
    reporter = getattr(owner, "_log_error", None)
    if callable(reporter):
        try:
            reporter(context, exc)
        except Exception:
            pass


def _close_exchange_for_shutdown(
    owner,
    *,
    allowed: bool = True,
    report_errors: bool = True,
) -> bool:
    exchange = getattr(owner, "ex", None)
    if exchange is None:
        return True
    if not allowed or getattr(owner, "_emergency_in_progress", False):
        if report_errors:
            _report_shutdown_error(
                owner,
                "Exchange shutdown",
                RuntimeError("runtime worker is still using the exchange"),
            )
        return False
    terminal = getattr(owner, "_emergency_closed", False) is True
    method_names = (
        ("shutdown", "close_all", "close")
        if terminal
        else ("close_all",)
    )
    closer = next(
        (
            candidate
            for name in method_names
            if callable(candidate := getattr(exchange, name, None))
        ),
        None,
    )
    if closer is None:
        return True
    try:
        result = closer()
    except Exception as exc:
        if report_errors:
            _report_shutdown_error(owner, "Exchange shutdown", exc)
        return False
    if result is False:
        if report_errors:
            _report_shutdown_error(
                owner,
                "Exchange shutdown",
                RuntimeError("exchange resources remain open"),
            )
        return False
    return True


def _schedule_shutdown_finalization_retry(
    owner,
    status_writer,
    log_event,
    state: _RuntimeShutdownFinalizationState,
    *,
    resource_results: Mapping[str, bool] | None,
    resource_closers: Mapping[str, object] | None,
) -> None:
    """Run one bounded, process-retaining convergence attempt per owner."""
    with state.lock:
        if state.complete:
            return
        retry = state.retry_thread
        if retry is not None and retry.is_alive():
            return
        interval = max(0.01, float(_SHUTDOWN_RETRY_INTERVAL_SEC))
        timeout = max(interval, float(_SHUTDOWN_RETRY_TIMEOUT_SEC))

        def retry_until_terminal() -> None:
            deadline = time.monotonic() + timeout
            converged = False
            try:
                while time.monotonic() < deadline:
                    time.sleep(interval)
                    converged = finalize_runtime_shutdown(
                        owner,
                        status_writer,
                        log_event,
                        resource_results=resource_results,
                        resource_closers=resource_closers,
                        _schedule_retry=False,
                        _quiet=True,
                    )
                    if converged:
                        return
                with state.lock:
                    completed_elsewhere = state.complete
                if not converged and not completed_elsewhere:
                    _report_shutdown_error(
                        owner,
                        "Runtime shutdown retry",
                        TimeoutError(
                            "shutdown did not converge within "
                            f"{timeout:.2f}s"
                        ),
                    )
            except Exception as exc:
                _report_shutdown_error(owner, "Runtime shutdown retry", exc)
            finally:
                with state.lock:
                    if state.retry_thread is threading.current_thread():
                        state.retry_thread = None

        retry = threading.Thread(
            target=retry_until_terminal,
            name=f"{getattr(owner, 'BOT_NAME', 'bot')}-shutdown-finalizer",
            daemon=False,
        )
        state.retry_thread = retry
        try:
            retry.start()
        except Exception as exc:
            state.retry_thread = None
            _report_shutdown_error(owner, "Runtime shutdown retry", exc)


def finalize_runtime_shutdown(
    owner,
    status_writer,
    log_event,
    *,
    resource_results: Mapping[str, bool] | None = None,
    resource_closers: Mapping[str, object] | None = None,
    _schedule_retry: bool = True,
    _quiet: bool = False,
) -> bool:
    """Publish a truthful terminal state after serialized bot teardown."""
    state = _shutdown_finalization_state(owner)
    with state.lock:
        if state.complete:
            return True

        threads_known = True
        try:
            raw_threads = owner._runtime_threads()
            if not isinstance(raw_threads, Mapping):
                raise TypeError("runtime thread status must be a mapping")
            threads = {
                str(name): bool(alive) for name, alive in raw_threads.items()
            }
        except Exception as exc:
            threads_known = False
            threads = {}
            if not _quiet:
                _report_shutdown_error(owner, "Runtime shutdown status", exc)

        emergency_in_progress = (
            getattr(owner, "_emergency_in_progress", False) is True
        )
        emergency_closed = getattr(owner, "_emergency_closed", False) is True
        teardown_allowed = (
            threads_known
            and not any(threads.values())
            and not emergency_in_progress
        )

        for raw_name, result in (resource_results or {}).items():
            name = str(raw_name)
            state.required_resources.add(name)
            if result is True:
                state.closed_resources.add(name)
        for raw_name, closer in (resource_closers or {}).items():
            name = str(raw_name)
            state.required_resources.add(name)
            state.resource_closers[name] = closer

        resources: dict[str, bool] = {}
        for name in sorted(state.required_resources):
            if name in state.closed_resources:
                resources[name] = True
                continue
            closer_registered = name in state.resource_closers
            closer = state.resource_closers.get(name)
            if not teardown_allowed:
                resources[name] = False
                continue
            if not callable(closer):
                resources[name] = False
                if closer_registered and not _quiet:
                    _report_shutdown_error(
                        owner,
                        f"{name} shutdown",
                        TypeError("resource closer must be callable"),
                    )
                continue
            try:
                result = closer()
            except Exception as exc:
                resources[name] = False
                if not _quiet:
                    _report_shutdown_error(owner, f"{name} shutdown", exc)
            else:
                resources[name] = result is not False
                if result is False:
                    if not _quiet:
                        _report_shutdown_error(
                            owner,
                            f"{name} shutdown",
                            RuntimeError(f"{name} resources remain open"),
                        )
                else:
                    state.closed_resources.add(name)

        if state.exchange_closed:
            resources["exchange"] = True
        else:
            exchange_closed = _close_exchange_for_shutdown(
                owner,
                allowed=teardown_allowed,
                report_errors=not _quiet,
            )
            resources["exchange"] = exchange_closed
            if exchange_closed:
                state.exchange_closed = True

        reasons = [
            f"thread_alive:{name}" for name, alive in threads.items() if alive
        ]
        if not threads_known:
            reasons.append("thread_status_unavailable")
        if emergency_in_progress:
            reasons.append("emergency_close_in_progress")
        elif not emergency_closed:
            reasons.append("emergency_close_incomplete")
        reasons.extend(
            f"{name}_close_failed"
            for name, succeeded in resources.items()
            if not succeeded
        )
        status = "stopped" if not reasons else "degraded"
        shutdown_payload = {
            "complete": not reasons,
            "reasons": reasons,
            "emergency_closed": emergency_closed,
            "emergency_in_progress": emergency_in_progress,
            "resources": resources,
        }
        status_signature = (
            status,
            tuple(sorted(threads.items())),
            tuple(reasons),
            emergency_closed,
            emergency_in_progress,
            tuple(sorted(resources.items())),
        )
        status_written = state.last_status_signature == status_signature
        if not status_written:
            try:
                write_result = status_writer(
                    owner.LOG_DIR,
                    owner.BOT_NAME,
                    status,
                    owner.simulation,
                    threads=threads,
                    extra={"shutdown": shutdown_payload},
                )
                if write_result is False:
                    raise RuntimeError(
                        "runtime status writer reported publish failure"
                    )
            except Exception as exc:
                if not _quiet:
                    _report_shutdown_error(owner, "Runtime shutdown status", exc)
            else:
                status_written = True
                state.last_status_signature = status_signature

        clean = not reasons and status_written
        if clean:
            state.complete = True
        if clean or not _quiet:
            try:
                if clean:
                    log_event("Bot shutdown complete.", "INFO")
                else:
                    detail = ", ".join(reasons) or "runtime status write failed"
                    log_event(f"Bot shutdown incomplete: {detail}", "WARN")
            except Exception as exc:
                _report_shutdown_error(owner, "Runtime shutdown log", exc)

    if not clean and _schedule_retry:
        _schedule_shutdown_finalization_retry(
            owner,
            status_writer,
            log_event,
            state,
            resource_results=resource_results,
            resource_closers=resource_closers,
        )
    return clean


def start_threads_or_shutdown(
    threads: Iterable[threading.Thread],
    shutdown_event: threading.Event,
    *,
    wakeup_events: Iterable[threading.Event] = (),
    join_timeout: float = 2.0,
) -> None:
    """Start all threads or stop and reap every thread already started.

    Publishing a partially started bot is unsafe: an entry/monitor worker can
    otherwise outlive a later startup failure while the lifecycle owner exits.
    The original start exception is deliberately preserved for the caller.
    """
    started: list[threading.Thread] = []
    candidate: threading.Thread | None = None
    try:
        for candidate in threads:
            candidate.start()
            started.append(candidate)
    except BaseException:
        shutdown_event.set()
        for event in wakeup_events:
            try:
                event.set()
            except Exception:
                pass

        # A non-standard Thread implementation could raise after launching.
        # Include that candidate when it reports itself alive.
        if candidate is not None and candidate not in started:
            try:
                if candidate.is_alive():
                    started.append(candidate)
            except Exception:
                pass

        deadline = time.monotonic() + max(0.0, float(join_timeout))
        for thread in reversed(started):
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                break
            try:
                if thread.is_alive():
                    thread.join(timeout=remaining)
            except Exception:
                pass
        raise
