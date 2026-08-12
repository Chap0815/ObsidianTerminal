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


def _flush_deferred() -> None:
    global _persist_timer
    with _lock:
        _persist_timer = None
        _persist_locked()


def _schedule_persist_locked(delay: float) -> None:
    """Schedule one daemon flush; caller holds ``_lock``."""
    global _persist_timer
    if _persist_shutdown:
        return
    if _persist_timer is not None and _persist_timer.is_alive():
        return
    timer = threading.Timer(max(0.01, float(delay)), _flush_deferred)
    timer.daemon = True
    _persist_timer = timer
    try:
        timer.start()
    except Exception:
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

            if not atomic_save_json(_TRACKER_FILE, snapshot):
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


def flush_pending_at_exit() -> bool:
    """Cancel the debounce timer and make one synchronous final write."""
    global _last_persist, _persist_timer, _persist_shutdown
    with _lock:
        _persist_shutdown = True
        timer = _persist_timer
        _persist_timer = None
        if timer is not None:
            try:
                timer.cancel()
            except Exception:
                pass
        _last_persist = 0.0
        persisted = _persist_locked()
        if not persisted and _dirty:
            try:
                from bot_utils.silent_log import silent_log
                silent_log(
                    "symbol tracker shutdown persistence",
                    OSError("dirty symbol tracker state remains non-durable"),
                )
            except Exception:
                pass
        return persisted


# Auto-load beim Import
_load()
atexit.register(flush_pending_at_exit)
