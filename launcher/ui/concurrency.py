"""Thread ownership and lifecycle helpers for launcher background work."""

from __future__ import annotations

import threading
from collections.abc import Callable


class CriticalWorkerRegistry:
    """Own state-changing daemon workers until their target has returned."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 0
        self._workers: dict[int, tuple[str, threading.Thread | None]] = {}

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
            with self._lock:
                self._workers[worker_id] = (worker_name, thread)
            thread.start()
        except Exception:
            with self._lock:
                self._workers.pop(worker_id, None)
            raise
        return thread

    def active_names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(name for name, _thread in self._workers.values()))
