"""Thread ownership and lifecycle helpers for launcher background work."""

from __future__ import annotations

import threading
from collections.abc import Callable

from bot_utils.runtime_threads import thread_definitely_never_started


class CriticalWorkerRegistry:
    """Own state-changing daemon workers until their target has returned."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 0
        self._workers: dict[int, tuple[str, threading.Thread | None]] = {}
        self._closing = False

    def start(
        self,
        target: Callable[[], object],
        *,
        name: str,
        daemon: bool = True,
    ) -> threading.Thread:
        if not callable(target):
            raise TypeError("critical worker target must be callable")
        worker_name = str(name or "critical-worker")[:120]
        with self._lock:
            if self._closing:
                raise RuntimeError(
                    "critical worker registry is sealed for shutdown"
                )
            self._next_id += 1
            worker_id = self._next_id
            self._workers[worker_id] = (worker_name, None)

        def owned_target() -> None:
            try:
                target()
            finally:
                with self._lock:
                    self._workers.pop(worker_id, None)

        try:
            thread = threading.Thread(
                target=owned_target,
                name=worker_name,
                daemon=bool(daemon),
            )
        except BaseException:
            with self._lock:
                self._workers.pop(worker_id, None)
            raise
        with self._lock:
            self._workers[worker_id] = (worker_name, thread)
        try:
            thread.start()
        except BaseException as exc:
            # Retire only an ordinary, exact-stdlib failure that demonstrably
            # happened before launch. Ambiguous/custom/fatal failures remain
            # owned until owned_target's identity-specific finalizer runs.
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(thread)
            ):
                with self._lock:
                    current = self._workers.get(worker_id)
                    if current is not None and current[1] is thread:
                        self._workers.pop(worker_id, None)
            raise
        return thread

    def active_names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(name for name, _thread in self._workers.values()))

    def prepare_shutdown(self) -> tuple[str, ...]:
        """Atomically report active workers or seal an idle registry."""
        with self._lock:
            active = tuple(
                sorted(name for name, _thread in self._workers.values())
            )
            if not active:
                self._closing = True
            return active

    def resume_after_aborted_shutdown(self) -> bool:
        """Reopen a sealed, idle registry when launcher shutdown aborts."""
        with self._lock:
            if self._workers:
                return False
            self._closing = False
            return True
