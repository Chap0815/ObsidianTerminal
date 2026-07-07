"""
symbol_tracker.py  Persistent first-seen tracker.

New listings can appear in fetch_tickers() before they have enough candle
history. Mark symbols as fresh for their first 24h and suppress expected
indicator failures during that window.
"""
from __future__ import annotations
import json
import os
import threading
import time
from typing import Dict


from core.paths import SYMBOL_FIRST_SEEN_STR as _TRACKER_FILE
_GRACE_PERIOD_SEC     = 24 * 3600    # 24 Stunden Stille nach erster Sichtung
_PERSIST_INTERVAL_SEC = 60.0          # max. einmal pro Minute auf Disk schreiben
_MAX_TRACKED          = 5000          # absolute Obergrenze (LRU prune)
_RETENTION_SEC        = _GRACE_PERIOD_SEC * 30   # 30 Tage Historie

_data: Dict[str, float] = {}          # symbol -> first_seen_epoch
_lock = threading.Lock()
_last_persist: float = 0.0
_dirty = False


def _load() -> None:
    """Ldt die persistierten Daten beim Import. Fehlertolerant."""
    global _data
    try:
        if os.path.exists(_TRACKER_FILE):
            with open(_TRACKER_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                now = time.time()
                _data = {
                    str(s): float(ts)
                    for s, ts in raw.items()
                    if isinstance(s, str) and isinstance(ts, (int, float))
                    and 0 < float(ts) < now + 86400
                }
    except Exception:
        _data = {}


def _persist_locked() -> None:
    """Caller MUST hold _lock. Schreibt atomar auf Disk, debounced."""
    global _last_persist, _dirty
    now = time.time()
    if not _dirty:
        return
    if (now - _last_persist) < _PERSIST_INTERVAL_SEC:
        return
    try:
        cutoff = now - _RETENTION_SEC
        snapshot = {s: ts for s, ts in _data.items() if ts >= cutoff}

        if len(snapshot) > _MAX_TRACKED:
            # LRU prune: behalte die NEWESTEN N
            sorted_items = sorted(snapshot.items(), key=lambda kv: kv[1], reverse=True)
            snapshot = dict(sorted_items[:_MAX_TRACKED])

        tmp = _TRACKER_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=1)
            f.flush()
            try:
                os.fsync(f.fileno())
            except (AttributeError, OSError):
                pass
        os.replace(tmp, _TRACKER_FILE)

        _data.clear()
        _data.update(snapshot)
        _last_persist = now
        _dirty = False
    except Exception:
        pass


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


def force_persist() -> None:
    """Externer Trigger: schreib jetzt auf Disk (z.B. bei Shutdown)."""
    global _last_persist
    with _lock:
        _last_persist = 0.0
        _persist_locked()


# Auto-load beim Import
_load()
