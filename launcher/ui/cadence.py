"""Small monotonic cadence helpers for launcher UI hot paths."""

from __future__ import annotations

import math


class PeriodicGate:
    """Admit work at most once per monotonic interval.

    The first call is always admitted.  Scheduling from the observed time,
    instead of repeatedly adding the interval, avoids catch-up bursts after a
    slow Tk callback or a suspended workstation.
    """

    def __init__(self, *, interval_seconds: float) -> None:
        interval = float(interval_seconds)
        if not math.isfinite(interval) or interval <= 0.0:
            raise ValueError("interval_seconds must be finite and positive")
        self.interval_seconds = interval
        self._next_due: float | None = None

    def due(self, now: float) -> bool:
        current = float(now)
        if not math.isfinite(current):
            raise ValueError("now must be finite")
        if self._next_due is not None and current < self._next_due:
            return False
        self._next_due = current + self.interval_seconds
        return True

