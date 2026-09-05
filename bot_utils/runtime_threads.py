"""Small lifecycle helpers for bot-owned runtime threads."""
from __future__ import annotations

import sys
import threading
import time
from contextlib import contextmanager
from collections.abc import Iterable, Mapping

_SHUTDOWN_RETRY_INTERVAL_SEC = 0.25
_SHUTDOWN_RETRY_TIMEOUT_SEC = 15.0
_SHUTDOWN_STATUS_REFRESH_SEC = 30.0
_FINALIZATION_STATE_CREATION_LOCK = threading.Lock()
_THREADING_THREAD_TYPE = threading.Thread


def thread_definitely_never_started(worker: object) -> bool:
    """Recognize only an exact stdlib Thread with no published launch."""
    if type(worker) is not _THREADING_THREAD_TYPE:
        return False
    try:
        started = worker._started  # type: ignore[attr-defined]
        return (
            worker.ident is None
            and not worker.is_alive()
            and not started.is_set()
        )
    except BaseException:
        return False


def _shutdown_event_bus_resource() -> bool:
    from core.event_bus import shutdown_global_bus

    return shutdown_global_bus(timeout=2.0)


def _shutdown_state_json_resource() -> bool:
    from core.state_manager import shutdown_state_json_writers

    return shutdown_state_json_writers(timeout=2.0)


def _shutdown_database_resource() -> bool:
    from core.database import shutdown_database_background_workers

    return shutdown_database_background_workers(timeout=2.0)


def _shutdown_expectancy_telemetry_resource() -> bool:
    module = sys.modules.get("trading.expectancy_telemetry")
    if module is None:
        return True
    closer = getattr(module, "shutdown_expectancy_telemetry_resources", None)
    if not callable(closer):
        return False
    return closer(timeout=0.0) is True


def _shutdown_cooldown_resource() -> bool:
    module = sys.modules.get("trading.cooldown_utils")
    if module is None:
        return True
    closer = getattr(module, "shutdown_cooldown_persistence", None)
    if not callable(closer):
        return False
    return closer(timeout=0.0) is True


def _shutdown_logger_resource() -> bool:
    from core.logger import (
        flush_structured_logs,
        flush_telegram,
        shutdown_legacy_rebuild,
    )

    # Evaluate every owned logger resource independently, but share one
    # end-to-end budget so retries cannot multiply process-exit latency.
    deadline = time.monotonic() + 2.0
    structured = flush_structured_logs(timeout=2.0) is True
    telegram = flush_telegram(
        timeout=max(0.0, deadline - time.monotonic())
    ) is True
    legacy = shutdown_legacy_rebuild(
        timeout=max(0.0, deadline - time.monotonic())
    ) is True
    return structured and telegram and legacy


def _shutdown_news_resource() -> bool:
    # Importing news_sources here would create two executors during shutdown in
    # bot modes that never used news.  Only close an already-owned module.
    module = sys.modules.get("news.news_sources")
    if module is None:
        return True
    closer = getattr(module, "shutdown_news_resources", None)
    if not callable(closer):
        return False
    return closer(timeout=0.0) is True


def _shutdown_llm_resource() -> bool:
    # Avoid importing llm_utils during shutdown: imports initialize config and
    # callers that never used the LLM do not own these resources.
    module = sys.modules.get("news.llm_utils")
    if module is None:
        return True
    closer = getattr(module, "shutdown_llm_resources", None)
    if not callable(closer):
        return False
    return closer(timeout=0.0) is True


def _shutdown_screener_resource() -> bool:
    module = sys.modules.get("trading.screener")
    if module is None:
        return True
    closer = getattr(module, "shutdown_screener_resources", None)
    if not callable(closer):
        return False
    return closer(timeout=0.0) is True


def _shutdown_symbol_tracker_resource() -> bool:
    module = sys.modules.get("trading.symbol_tracker")
    if module is None:
        return True
    closer = getattr(module, "shutdown_symbol_tracker_resources", None)
    if not callable(closer):
        return False
    return closer(timeout=0.0) is True


def shared_runtime_resource_closers() -> dict[str, object]:
    """Return process-global resources owned by every current bot process."""
    return {
        "cooldown_persistence": _shutdown_cooldown_resource,
        "event_bus": _shutdown_event_bus_resource,
        "expectancy_telemetry": _shutdown_expectancy_telemetry_resource,
        "logger_queues": _shutdown_logger_resource,
        "llm_resources": _shutdown_llm_resource,
        "news_resources": _shutdown_news_resource,
        "screener_resources": _shutdown_screener_resource,
        "state_json_writers": _shutdown_state_json_resource,
        "symbol_tracker_persistence": _shutdown_symbol_tracker_resource,
        "database_background_workers": _shutdown_database_resource,
    }


def format_runtime_thread_liveness(threads: Mapping[str, bool]) -> str:
    """Render one compact, truthful heartbeat view of all runtime workers."""
    if not isinstance(threads, Mapping) or not threads:
        return "unavailable"
    return " ".join(
        f"{str(name)[:32]}={'UP' if alive is True else 'DOWN'}"
        for name, alive in threads.items()
    )


class _RuntimeShutdownFinalizationState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.retry_thread: threading.Thread | None = None
        self.retry_start_uncertain = False
        self.complete = False
        self.closed_resources: set[str] = set()
        self.resource_closers: dict[str, object] = {}
        self.required_resources: set[str] = set()
        self.exchange_closed = False
        self.last_status_signature: tuple[object, ...] | None = None
        self.last_status_written_at = 0.0


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
        else:
            return
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, exc)
    except Exception:
        pass


@contextmanager
def _shutdown_transition_gate(owner):
    """Serialize signal-sensitive finalizer decisions with close requests."""
    gate = getattr(owner, "_shutdown_handler_lock", None)
    if gate is None:
        yield
        return
    gate.acquire()
    try:
        yield
    finally:
        gate.release()
        pending = getattr(owner, "_shutdown_close_requested", None)
        drain = getattr(owner, "_drain_shutdown_close_requests", None)
        handler = getattr(owner, "_shutdown_handler", None)
        callback = drain if callable(drain) else handler
        if pending is not None and pending.is_set() and callable(callback):
            try:
                callback(signum="Deferred shutdown signal")
            except Exception as exc:
                pending.set()
                _report_shutdown_error(
                    owner,
                    "Deferred shutdown signal",
                    exc,
                )


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
    terminal = (
        getattr(owner, "_emergency_closed", False) is True
        or getattr(owner, "_shutdown_positions_preserved", False) is True
    )
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
    if result is not None and result is not True:
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
        if retry is not None:
            try:
                retry_alive = retry.is_alive()
            except BaseException:
                return
            if retry_alive:
                return
            if state.retry_start_uncertain:
                try:
                    if retry.ident is None:
                        return
                except BaseException:
                    return
                state.retry_start_uncertain = False
        interval = max(0.01, float(_SHUTDOWN_RETRY_INTERVAL_SEC))
        timeout = max(interval, float(_SHUTDOWN_RETRY_TIMEOUT_SEC))

        def retry_until_terminal() -> None:
            deadline = time.monotonic() + timeout
            converged = False
            timeout_reported = False
            try:
                while True:
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
                    if time.monotonic() < deadline:
                        continue
                    with state.lock:
                        completed_elsewhere = state.complete
                    if completed_elsewhere:
                        return
                    if not timeout_reported:
                        timeout_reported = True
                        _report_shutdown_error(
                            owner,
                            "Runtime shutdown retry",
                            TimeoutError(
                                "shutdown did not converge within "
                                f"{timeout:.2f}s"
                            ),
                        )
                    emergency_closed = (
                        getattr(owner, "_emergency_closed", False) is True
                    )
                    positions_preserved = (
                        getattr(
                            owner,
                            "_shutdown_positions_preserved",
                            False,
                        )
                        is True
                    )
                    preserved_workers_quiesced = False
                    if positions_preserved and not getattr(
                        owner,
                        "_emergency_in_progress",
                        False,
                    ):
                        try:
                            raw_threads = owner._runtime_threads()
                            preserved_workers_quiesced = (
                                isinstance(raw_threads, Mapping)
                                and bool(raw_threads)
                                and not any(bool(alive) for alive in raw_threads.values())
                            )
                        except Exception:
                            preserved_workers_quiesced = False
                    # The emergency runner is daemonized so a hung or partial
                    # close cannot by itself retain the process.  Keep this
                    # non-daemon finalizer alive until the position action is
                    # terminal; a later signal can then retry the remaining
                    # close with the still-live exchange resources. Preserved
                    # positions are terminal only after every runtime worker
                    # is known quiescent; otherwise a late entry/state publish
                    # can still occur after the shutdown snapshot.
                    if (
                        emergency_closed and not positions_preserved
                    ) or preserved_workers_quiesced:
                        return
            except Exception as exc:
                _report_shutdown_error(owner, "Runtime shutdown retry", exc)
            finally:
                with state.lock:
                    if state.retry_thread is threading.current_thread():
                        state.retry_thread = None
                        state.retry_start_uncertain = False

        try:
            retry = threading.Thread(
                target=retry_until_terminal,
                name=f"{getattr(owner, 'BOT_NAME', 'bot')}-shutdown-finalizer",
                daemon=False,
            )
        except Exception as exc:
            _report_shutdown_error(owner, "Runtime shutdown retry", exc)
            return
        state.retry_thread = retry
        state.retry_start_uncertain = False
        try:
            retry.start()
        except BaseException as exc:
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(retry)
                and state.retry_thread is retry
            ):
                state.retry_thread = None
                state.retry_start_uncertain = False
            elif state.retry_thread is retry:
                state.retry_start_uncertain = True
            if isinstance(exc, Exception):
                _report_shutdown_error(owner, "Runtime shutdown retry", exc)
                return
            raise


def wait_for_runtime_shutdown(
    owner,
    status_writer,
    log_event,
    *,
    resource_results: Mapping[str, bool] | None = None,
    resource_closers: Mapping[str, object] | None = None,
) -> bool:
    """Retain the main thread until the existing shutdown action is terminal.

    A non-daemon retry thread alone cannot do this: interpreter shutdown stops
    accepting executor submissions before joining those threads. This driver
    never requests a position close or changes a user's preserve decision.
    Only resource/status failures may exhaust the bounded retry budget.
    """
    interval = max(0.01, float(_SHUTDOWN_RETRY_INTERVAL_SEC))
    deadline = time.monotonic() + max(interval, float(_SHUTDOWN_RETRY_TIMEOUT_SEC))
    first_pass = True
    pause_before_retry = False
    interrupt_pending = False
    pending_error = None
    while True:
        try:
            if pending_error is not None:
                error, pending_error = pending_error, None
                _report_shutdown_error(owner, "Runtime shutdown wait", error)
            if interrupt_pending:
                interrupt_pending = False
                # Only an actually received interrupt authorizes this request.
                owner._shutdown_handler(signum="KeyboardInterrupt")
            if pause_before_retry:
                time.sleep(interval)
            pause_before_retry = True
            quiet, first_pass = not first_pass, False
            if finalize_runtime_shutdown(
                owner, status_writer, log_event,
                resource_results=resource_results,
                resource_closers=resource_closers,
                _schedule_retry=False,
                _quiet=quiet,
            ):
                return True
            if time.monotonic() >= deadline:
                with _shutdown_transition_gate(owner):
                    threads = owner._runtime_threads()
                    closed = getattr(owner, "_emergency_closed", False) is True
                    preserved = getattr(owner, "_shutdown_positions_preserved", False) is True
                    terminal = (
                        isinstance(threads, Mapping) and bool(threads)
                        and not any(bool(alive) for alive in threads.values())
                        and not getattr(owner, "_emergency_in_progress", False)
                        and closed != preserved
                    )
                    if terminal and closed:
                        count = getattr(getattr(owner, "state", None), "count", None)
                        if callable(count):
                            remaining = count()
                            terminal = type(remaining) is int and remaining == 0
                    if terminal:
                        return False
        except KeyboardInterrupt:
            # Handle the request inside the protected loop: the handler and
            # retry sleep can themselves raise while a close is still active.
            interrupt_pending = True
        except Exception as exc:
            pending_error = exc


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
    with _shutdown_transition_gate(owner), state.lock:
        if state.complete:
            return True

        threads_known = True
        try:
            raw_threads = owner._runtime_threads()
            if not isinstance(raw_threads, Mapping) or not raw_threads:
                raise TypeError(
                    "runtime thread status must be a non-empty mapping"
                )
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
        positions_preserved = (
            getattr(owner, "_shutdown_positions_preserved", False) is True
        )
        teardown_allowed = (
            threads_known
            and not any(threads.values())
            and not emergency_in_progress
        )
        observed_open_positions: int | None = None
        position_count_error: Exception | None = None
        count_positions = None
        if teardown_allowed:
            state_owner = getattr(owner, "state", None)
            count_positions = getattr(state_owner, "count", None)
            if callable(count_positions):
                try:
                    candidate_count = count_positions()
                    if (
                        isinstance(candidate_count, bool)
                        or not isinstance(candidate_count, int)
                        or candidate_count < 0
                    ):
                        raise ValueError(
                            "open-position count must be a non-negative integer"
                        )
                    observed_open_positions = candidate_count
                except Exception as exc:
                    position_count_error = exc
        position_action_reason = None
        if teardown_allowed and emergency_closed and not positions_preserved:
            if callable(count_positions):
                if position_count_error is not None:
                    emergency_closed = False
                    position_action_reason = "position_state_unavailable"
                    if not _quiet:
                        _report_shutdown_error(
                            owner,
                            "Runtime shutdown position state",
                            position_count_error,
                        )
                elif observed_open_positions is not None:
                    if observed_open_positions > 0:
                        emergency_closed = False
                        position_action_reason = (
                            "emergency_close_state_remaining"
                        )
                if not emergency_closed:
                    # A scan/entry worker can finish publishing an already
                    # landed order after the emergency helper took its state
                    # snapshot. Re-open the latch after all workers quiesce so
                    # a repeat signal can close that late generation. Keep the
                    # same owner lock used by the signal handler when present.
                    shutdown_lock = getattr(owner, "_shutdown_lock", None)
                    if shutdown_lock is None:
                        owner._emergency_closed = False
                    else:
                        def _reopen_close_latch() -> None:
                            with shutdown_lock:
                                if not getattr(
                                    owner,
                                    "_shutdown_positions_preserved",
                                    False,
                                ) and not getattr(
                                    owner,
                                    "_emergency_in_progress",
                                    False,
                                ):
                                    owner._emergency_closed = False

                        _reopen_close_latch()

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
        deferred_resources = {
            name for name in state.required_resources if name == "logger_queues"
        }
        llm_resources = {
            name for name in state.required_resources if name == "llm_resources"
        }
        expectancy_resources = {
            name
            for name in state.required_resources
            if name == "expectancy_telemetry"
        }
        trade_state_resources = {
            name
            for name in state.required_resources
            if name == "trade_state_persistence"
        }
        regular_resources = sorted(
            state.required_resources
            - deferred_resources
            - llm_resources
            - expectancy_resources
            - trade_state_resources
        )

        def _close_registered_resource(name: str) -> None:
            if name in state.closed_resources:
                resources[name] = True
                return
            closer_registered = name in state.resource_closers
            closer = state.resource_closers.get(name)
            if not teardown_allowed:
                resources[name] = False
                return
            if not callable(closer):
                resources[name] = False
                if closer_registered and not _quiet:
                    _report_shutdown_error(
                        owner,
                        f"{name} shutdown",
                        TypeError("resource closer must be callable"),
                    )
                return
            try:
                result = closer()
            except Exception as exc:
                resources[name] = False
                if not _quiet:
                    _report_shutdown_error(owner, f"{name} shutdown", exc)
            else:
                confirmed = result is None or result is True
                resources[name] = confirmed
                if not confirmed:
                    if not _quiet:
                        _report_shutdown_error(
                            owner,
                            f"{name} shutdown",
                            RuntimeError(f"{name} resources remain open"),
                        )
                else:
                    # A status-publish failure below can itself emit a final
                    # structured diagnostic. Keep the logger flush retryable
                    # until the whole finalizer reaches its terminal state.
                    if name != "logger_queues":
                        state.closed_resources.add(name)

        for name in sorted(expectancy_resources):
            _close_registered_resource(name)
        for name in sorted(trade_state_resources):
            _close_registered_resource(name)

        for name in regular_resources:
            if (
                name == "database_background_workers"
                and any(
                    resources.get(producer) is not True
                    for producer in (
                        expectancy_resources | trade_state_resources
                    )
                )
            ):
                resources[name] = False
            else:
                _close_registered_resource(name)

        # News executor tasks can be inside LLM inference. Do not close their
        # shared Ollama/httpx clients until those producers have converged.
        for name in sorted(llm_resources):
            if resources.get("news_resources", True) is True:
                _close_registered_resource(name)
            else:
                resources[name] = False

        position_action_terminal = emergency_closed or positions_preserved
        if state.exchange_closed:
            resources["exchange"] = True
        elif not position_action_terminal:
            # ``ThreadLocalExchange.close_all`` is deliberately reusable.  A
            # partial emergency close may need that wrapper again on a repeat
            # signal, so neither close nor cache it as terminal prematurely.
            resources["exchange"] = False
        else:
            exchange_closed = _close_exchange_for_shutdown(
                owner,
                allowed=teardown_allowed,
                report_errors=not _quiet,
            )
            resources["exchange"] = exchange_closed
            if exchange_closed:
                state.exchange_closed = True

        # Logger queues are the terminal sink for errors emitted by every
        # other closer and by exchange teardown. Flush them only after those
        # producers converge; otherwise an early successful flush would be
        # cached while retry diagnostics remain unflushed.
        for name in sorted(deferred_resources):
            prerequisites_closed = (
                teardown_allowed
                and resources.get("exchange") is True
                and all(
                    resources.get(other) is True
                    for other in (
                        regular_resources
                        + sorted(llm_resources)
                        + sorted(expectancy_resources)
                        + sorted(trade_state_resources)
                    )
                )
            )
            if name not in state.closed_resources and not prerequisites_closed:
                resources[name] = False
                continue
            _close_registered_resource(name)

        reasons = [
            f"thread_alive:{name}" for name, alive in threads.items() if alive
        ]
        if not threads_known:
            reasons.append("thread_status_unavailable")
        if emergency_in_progress:
            reasons.append("emergency_close_in_progress")
        elif emergency_closed and positions_preserved:
            reasons.append("shutdown_position_action_inconsistent")
        elif not emergency_closed and not positions_preserved:
            reasons.append(
                position_action_reason or "emergency_close_incomplete"
            )
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
        if positions_preserved:
            shutdown_payload["positions_preserved"] = True
        status_signature = (
            status,
            tuple(sorted(threads.items())),
            tuple(reasons),
            emergency_closed,
            emergency_in_progress,
            positions_preserved,
            observed_open_positions,
            tuple(sorted(resources.items())),
        )
        now_mono = time.monotonic()
        same_status = state.last_status_signature == status_signature
        refresh_due = (
            same_status
            and bool(reasons)
            and now_mono - state.last_status_written_at
            >= max(0.0, float(_SHUTDOWN_STATUS_REFRESH_SEC))
        )
        status_written = same_status and not refresh_due
        if not status_written:
            status_extra = {
                "shutdown": shutdown_payload,
                "open_positions_known": observed_open_positions is not None,
            }
            if observed_open_positions is not None:
                status_extra["open_positions"] = observed_open_positions
            try:
                write_result = status_writer(
                    owner.LOG_DIR,
                    owner.BOT_NAME,
                    status,
                    owner.simulation,
                    threads=threads,
                    extra=status_extra,
                )
                if write_result is not None and write_result is not True:
                    raise RuntimeError(
                        "runtime status writer reported publish failure"
                    )
            except Exception as exc:
                if not _quiet:
                    _report_shutdown_error(owner, "Runtime shutdown status", exc)
            else:
                status_written = True
                state.last_status_signature = status_signature
                state.last_status_written_at = time.monotonic()

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
    except BaseException as exc:
        noted_cleanup_failures: set[tuple[str, str]] = set()

        def note_cleanup_failure(
            context: str,
            cleanup_error: BaseException,
            primary_error: BaseException = exc,
        ) -> None:
            key = (context, type(cleanup_error).__name__)
            if key in noted_cleanup_failures:
                return
            noted_cleanup_failures.add(key)
            try:
                primary_error.add_note(
                    f"{context}: {type(cleanup_error).__name__}: "
                    f"{cleanup_error}"
                )
            except BaseException:
                pass

        pending_signals: list[tuple[object, str]] = []

        def publish_signal(event: object, context: str) -> None:
            try:
                event.set()
            except BaseException as cleanup_error:
                note_cleanup_failure(context, cleanup_error)
                pending_signals.append((event, context))

        def retry_pending_signals() -> None:
            if not pending_signals:
                return
            retry = list(pending_signals)
            pending_signals.clear()
            for event, context in retry:
                publish_signal(event, context)

        publish_signal(
            shutdown_event,
            "runtime rollback shutdown signal failed",
        )
        for event in wakeup_events:
            publish_signal(event, "runtime rollback wakeup failed")

        uncertain: set[int] = set()
        if (
            candidate is not None
            and all(candidate is not thread for thread in started)
            and not (
                isinstance(exc, Exception)
                and thread_definitely_never_started(candidate)
            )
        ):
            # After start() was invoked, a custom/fatal failure can still own
            # a delayed OS launch. Keep the exact candidate in rollback until
            # it has both published a start identity and terminated.
            started.append(candidate)
            uncertain.add(id(candidate))

        try:
            retry_interval = max(0.01, float(join_timeout))
        except BaseException as cleanup_error:
            note_cleanup_failure(
                "runtime rollback join interval invalid",
                cleanup_error,
            )
            retry_interval = 0.25
        for thread in reversed(started):
            while True:
                retry_pending_signals()
                try:
                    alive = thread.is_alive()
                except BaseException as cleanup_error:
                    note_cleanup_failure(
                        "runtime rollback liveness probe failed",
                        cleanup_error,
                    )
                    alive = True
                if not alive:
                    if id(thread) in uncertain:
                        try:
                            if thread.ident is None:
                                try:
                                    time.sleep(min(retry_interval, 0.25))
                                except BaseException as cleanup_error:
                                    note_cleanup_failure(
                                        "runtime rollback wait interrupted",
                                        cleanup_error,
                                    )
                                continue
                        except BaseException as cleanup_error:
                            note_cleanup_failure(
                                "runtime rollback identity probe failed",
                                cleanup_error,
                            )
                            try:
                                time.sleep(min(retry_interval, 0.25))
                            except BaseException as wait_error:
                                note_cleanup_failure(
                                    "runtime rollback wait interrupted",
                                    wait_error,
                                )
                            continue
                    break
                try:
                    thread.join(timeout=retry_interval)
                except BaseException as cleanup_error:
                    note_cleanup_failure(
                        "runtime rollback join failed",
                        cleanup_error,
                    )
                    try:
                        time.sleep(min(retry_interval, 0.25))
                    except BaseException as wait_error:
                        note_cleanup_failure(
                            "runtime rollback wait interrupted",
                            wait_error,
                        )
        while pending_signals:
            retry_pending_signals()
            if pending_signals:
                try:
                    time.sleep(min(retry_interval, 0.25))
                except BaseException as wait_error:
                    note_cleanup_failure(
                        "runtime rollback signal retry interrupted",
                        wait_error,
                    )
        raise
