"""
symbol_tracker.py  Persistent first-seen tracker.

New listings can appear in fetch_tickers() before they have enough candle
history. Mark symbols as fresh for their first 24h and suppress expected
indicator failures during that window.
"""
from __future__ import annotations
import atexit
import json
import math
import os
import threading
import time
from typing import Dict

import portalocker

from bot_utils.state_persist import atomic_save_json
from bot_utils.runtime_threads import thread_definitely_never_started
from core.paths import SYMBOL_FIRST_SEEN_STR as _TRACKER_FILE
_GRACE_PERIOD_SEC     = 24 * 3600    # 24 Stunden Stille nach erster Sichtung
_PERSIST_INTERVAL_SEC = 60.0          # max. einmal pro Minute auf Disk schreiben
_MAX_TRACKED          = 5000          # absolute Obergrenze (LRU prune)
_RETENTION_SEC        = _GRACE_PERIOD_SEC * 30   # 30 Tage Historie
_TRACKER_JSON_MAX_BYTES = 1024 * 1024

_data: Dict[str, float] = {}          # symbol -> first_seen_epoch
_lock = threading.Lock()
_last_persist: float = time.monotonic() - _PERSIST_INTERVAL_SEC
_dirty = False
_persist_timer: threading.Timer | None = None
_persist_shutdown = False
_persist_admission_lock = threading.Lock()
_persist_shutdown_lifecycle_lock = threading.Lock()
_persist_active_workers: set[threading.Thread] = set()
_persist_retiring_workers: set[threading.Thread] = set()
_persist_shutdown_generation: dict | None = None


def _flush_deferred(owner: threading.Timer | None = None) -> None:
    global _persist_timer
    current = threading.current_thread()
    with _lock:
        _persist_active_workers.add(current)
        try:
            if owner is None or _persist_timer is not owner:
                return
            _persist_timer = None
            if _persist_shutdown:
                return
            _persist_locked()
        finally:
            _persist_active_workers.discard(current)


def _schedule_persist_locked(delay: float) -> None:
    """Schedule one daemon flush; caller holds ``_lock``."""
    global _persist_timer
    if _persist_timer is not None and _persist_timer.is_alive():
        return
    with _persist_admission_lock:
        if _persist_shutdown:
            return
        timer = None

        def flush_owned_generation() -> None:
            _flush_deferred(timer)

        timer = threading.Timer(
            max(0.01, float(delay)),
            flush_owned_generation,
        )
        timer.daemon = True
        _persist_timer = timer
        try:
            timer.start()
        except Exception:
            if _persist_timer is timer:
                _persist_timer = None


def _validated_data(raw, now: float) -> Dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    clean: Dict[str, float] = {}
    for symbol, raw_ts in raw.items():
        if not isinstance(symbol, str) or isinstance(raw_ts, bool):
            continue
        try:
            timestamp = float(raw_ts)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(timestamp) and 0 < timestamp < now + 86400:
            clean[symbol] = timestamp
    return clean


def _read_tracker_data(now: float) -> Dict[str, float]:
    with open(_TRACKER_FILE, "rb") as fh:
        raw = fh.read(_TRACKER_JSON_MAX_BYTES + 1)
    if len(raw) > _TRACKER_JSON_MAX_BYTES:
        raise ValueError("symbol tracker JSON exceeds size limit")
    return _validated_data(json.loads(raw.decode("utf-8-sig")), now)


def _load() -> None:
    """Ldt die persistierten Daten beim Import. Fehlertolerant."""
    global _data
    try:
        if os.path.exists(_TRACKER_FILE):
            _data = _read_tracker_data(time.time())
    except Exception:
        _data = {}


def _persist_locked() -> bool:
    """Caller MUST hold _lock. Schreibt atomar auf Disk, debounced."""
    global _last_persist, _dirty
    now = time.time()
    persist_now = time.monotonic()
    if not _dirty:
        return True
    elapsed = persist_now - _last_persist
    if elapsed < _PERSIST_INTERVAL_SEC:
        _schedule_persist_locked(_PERSIST_INTERVAL_SEC - elapsed)
        return False
    try:
        os.makedirs(os.path.dirname(_TRACKER_FILE) or ".", exist_ok=True)
        with portalocker.Lock(
            _TRACKER_FILE + ".lock",
            mode="a",
            timeout=5.0,
            check_interval=0.05,
            fail_when_locked=False,
        ):
            disk_data: Dict[str, float] = {}
            try:
                if os.path.exists(_TRACKER_FILE):
                    disk_data = _read_tracker_data(now)
            except Exception:
                disk_data = {}

            merged = _validated_data(_data, now)
            for symbol, timestamp in disk_data.items():
                current = merged.get(symbol)
                if current is None or timestamp < current:
                    merged[symbol] = timestamp

            cutoff = now - _RETENTION_SEC
            snapshot = {
                symbol: timestamp
                for symbol, timestamp in merged.items()
                if timestamp >= cutoff
            }

            if len(snapshot) > _MAX_TRACKED:
                # LRU prune: behalte die NEWESTEN N
                sorted_items = sorted(
                    snapshot.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
                snapshot = dict(sorted_items[:_MAX_TRACKED])

            if atomic_save_json(_TRACKER_FILE, snapshot) is not True:
                _schedule_persist_locked(_PERSIST_INTERVAL_SEC)
                return False

        _data.clear()
        _data.update(snapshot)
        _last_persist = persist_now
        _dirty = False
        return True
    except Exception:
        _schedule_persist_locked(_PERSIST_INTERVAL_SEC)
        return False


def record_seen(symbol: str) -> None:
    """Markiert ein Symbol als 'erstmals gesehen' (no-op wenn bereits da)."""
    global _dirty
    if not symbol:
        return
    with _lock:
        if _persist_shutdown:
            return
        if symbol not in _data:
            _data[symbol] = time.time()
            _dirty = True
            _persist_locked()


def is_in_grace_period(symbol: str) -> bool:
    """True wenn das Symbol in den letzten _GRACE_PERIOD_SEC Sekunden
    erstmals gesichtet wurde."""
    if not symbol:
        return False
    with _lock:
        ts = _data.get(symbol)
    if ts is None:
        # Unbekanntes Symbol  noch nie gesehen  Grace gewhren
        # (vermeidet WARN beim ersten Bot-Start nach dem Tracker-Rollout)
        return True
    return (time.time() - ts) < _GRACE_PERIOD_SEC


def age_hours(symbol: str) -> float:
    """Stunden seit erster Sichtung. -1 wenn unbekannt."""
    if not symbol:
        return -1.0
    with _lock:
        ts = _data.get(symbol)
    if ts is None:
        return -1.0
    return (time.time() - ts) / 3600.0


def force_persist() -> bool:
    """Externer Trigger: schreib jetzt auf Disk (z.B. bei Shutdown)."""
    global _last_persist
    with _lock:
        _last_persist = 0.0
        return _persist_locked()


def _acquire_tracker_lock_until(deadline: float) -> bool:
    return _lock.acquire(timeout=max(0.0, deadline - time.monotonic()))


def _shutdown_tracker_flush(generation: dict) -> None:
    global _last_persist
    current = threading.current_thread()
    try:
        with _lock:
            _persist_active_workers.add(current)
            try:
                _last_persist = 0.0
                persisted = _persist_locked()
                if not persisted and _dirty:
                    try:
                        from bot_utils.silent_log import silent_log

                        silent_log(
                            "symbol tracker shutdown persistence",
                            OSError(
                                "dirty symbol tracker state remains non-durable"
                            ),
                        )
                    except Exception:
                        pass
            finally:
                _persist_active_workers.discard(current)
    finally:
        with _lock:
            generation["done"].set()


def _shutdown_symbol_tracker_resources_owned(timeout: float = 0.0) -> bool:
    """Terminally stop the debounce timer and durably flush dirty state."""
    global _persist_timer, _persist_shutdown, _persist_shutdown_generation
    try:
        budget = float(timeout)
    except (TypeError, ValueError, OverflowError):
        budget = 0.0
    if not math.isfinite(budget):
        budget = 0.0
    deadline = time.monotonic() + max(0.0, budget)

    if not _persist_admission_lock.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        _persist_shutdown = True
    finally:
        _persist_admission_lock.release()

    if not _acquire_tracker_lock_until(deadline):
        return False
    try:
        workers = list(_persist_active_workers)
        workers.extend(_persist_retiring_workers)
        flush_generation = _persist_shutdown_generation
        if flush_generation is not None:
            flush_worker = flush_generation.get("worker")
            if flush_worker is not None:
                workers.append(flush_worker)
        timer = _persist_timer
        if timer is not None:
            workers.append(timer)
        _persist_retiring_workers.update(workers)
        for worker in workers:
            try:
                worker.cancel()
            except Exception:
                pass
    finally:
        _lock.release()

    still_alive = []
    for worker in dict.fromkeys(workers):
        join = getattr(worker, "join", None)
        if callable(join) and worker is not threading.current_thread():
            try:
                join(timeout=max(0.0, deadline - time.monotonic()))
            except Exception:
                pass
        try:
            if worker.is_alive():
                still_alive.append(worker)
        except Exception:
            still_alive.append(worker)

    if not _acquire_tracker_lock_until(deadline):
        return False
    try:
        _persist_retiring_workers.intersection_update(still_alive)
        if still_alive:
            return False
        if (
            flush_generation is not None
            and not flush_generation["done"].is_set()
        ):
            return False
        if _persist_shutdown_generation is flush_generation:
            _persist_shutdown_generation = None
        if _persist_timer is timer:
            _persist_timer = None
        if not _dirty:
            return True
        generation = {
            "done": threading.Event(),
            "worker": None,
            "start_raised": False,
        }

        def run_owned_flush() -> None:
            _shutdown_tracker_flush(generation)

        try:
            flush = threading.Thread(
                target=run_owned_flush,
                name="symbol-tracker-shutdown-persist",
                daemon=True,
            )
        except BaseException as exc:
            try:
                from bot_utils.silent_log import silent_log

                silent_log("construct symbol tracker shutdown persistence", exc)
            except BaseException:
                pass
            if not isinstance(exc, Exception):
                raise
            return False
        generation["worker"] = flush
        _persist_shutdown_generation = generation
        _persist_retiring_workers.add(flush)
        try:
            flush.start()
        except BaseException as exc:
            generation["start_raised"] = True
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(flush)
            ):
                generation["done"].set()
                _persist_retiring_workers.discard(flush)
                if _persist_shutdown_generation is generation:
                    _persist_shutdown_generation = None
            try:
                from bot_utils.silent_log import silent_log

                silent_log("start symbol tracker shutdown persistence", exc)
            except BaseException:
                pass
            if not isinstance(exc, Exception):
                raise
            return False
    finally:
        _lock.release()

    try:
        flush.join(timeout=max(0.0, deadline - time.monotonic()))
    except Exception:
        return False
    try:
        flush_alive = flush.is_alive()
    except Exception:
        flush_alive = True
    if flush_alive:
        return False
    if not _acquire_tracker_lock_until(deadline):
        return False
    try:
        completed = (
            _persist_shutdown_generation is generation
            and generation["done"].is_set()
        )
        if completed:
            _persist_retiring_workers.discard(flush)
            _persist_shutdown_generation = None
        return completed and not _dirty
    finally:
        _lock.release()


def shutdown_symbol_tracker_resources(timeout: float = 0.0) -> bool:
    """Serialize the full shutdown generation handoff under one budget."""
    if isinstance(timeout, bool):
        return False
    try:
        budget = float(timeout)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(budget):
        return False
    bounded_budget = min(
        max(0.0, budget),
        float(threading.TIMEOUT_MAX),
    )
    deadline = time.monotonic() + bounded_budget
    if not _persist_shutdown_lifecycle_lock.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        return _shutdown_symbol_tracker_resources_owned(
            timeout=max(0.0, deadline - time.monotonic())
        )
    finally:
        _persist_shutdown_lifecycle_lock.release()


def flush_pending_at_exit() -> bool:
    """Interpreter-exit fallback for the managed runtime closer."""
    return shutdown_symbol_tracker_resources(timeout=1.0)


# Auto-load beim Import
_load()
atexit.register(flush_pending_at_exit)
