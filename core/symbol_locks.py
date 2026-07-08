"""
symbol_locks.py  Per-symbol close locks.

Guards against a double-sell race between the signal handler and the main
loop. Locks are never removed while a caller might still resolve them  a
periodic GC (gc_idle_locks) drops idle, uncontended locks safely, so the
per-symbol lock dict doesn't leak.

Public API:
    with close_lock(sym) as got:
        if not got: continue
        ...

    gc_idle_locks()      # optional, call periodically (~5 min)
    lock_count()         # diagnostic
"""
from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Dict


# Single mutex protects ALL of: _LOCKS, _REFCOUNTS, _CREATED_AT
_MUTEX     = threading.Lock()
_LOCKS:     Dict[str, threading.Lock] = {}
_REFCOUNTS: Dict[str, int]            = {}

_ADVISORY_TTL_SEC   = 120
_ADVISORY_TIMEOUT   = 2.0


def _advisory_lock_name(bot_name: str, base: str) -> str:
    bot = (bot_name or "").upper()
    market = "spot" if bot in {"SPOT", "TREND"} else "fut"
    return f"close:{market}:{base}"


def _acquire_or_create(sym: str) -> threading.Lock:
    """Return the lock for `sym`. Increment its refcount under the mutex."""
    with _MUTEX:
        lock = _LOCKS.get(sym)
        if lock is None:
            lock = threading.Lock()
            _LOCKS[sym] = lock
            _REFCOUNTS[sym] = 0
        _REFCOUNTS[sym] = _REFCOUNTS.get(sym, 0) + 1
        return lock


def _decref(sym: str) -> None:
    """Decrement refcount. NEVER delete the lock here  that races with
    other threads who already resolved the lock and are about to acquire.
    Cleanup is the job of gc_idle_locks()."""
    with _MUTEX:
        n = _REFCOUNTS.get(sym, 0)
        if n > 0:
            _REFCOUNTS[sym] = n - 1


@contextmanager
def close_lock(sym: str, timeout: float = 5.0, bot_name: str = None,
               fail_open: bool = False):
    """
    Acquire the per-symbol close lock (in-process) AND a cross-process
    advisory lock backed by the advisory_locks SQLite table.

    Routine accounting paths fail closed when the cross-process advisory lock
    cannot be acquired. Emergency flatten paths may opt into fail_open=True and
    must avoid double-booking accounting.

        with close_lock("BTC/USDT") as got:
            if not got:
                # Another thread holds the lock; skip this iteration.
                return
            # ... do the sell ...
    """
    lock = _acquire_or_create(sym)
    got = False
    _adv_lock_name = None
    _adv_holder_id = None
    _adv_acquired  = False
    _renew_stop = None
    _renew_thread = None

    try:
        got = lock.acquire(timeout=timeout)
        if got:
            _bn = bot_name or ""
            _base = sym.split("/")[0].split(":")[0].strip().upper()
            if _bn and _base:
                _adv_lock_name = _advisory_lock_name(_bn, _base)
                _adv_holder_id = f"{os.getpid()}-{threading.get_ident()}"
                import time as _time
                _deadline = _time.monotonic() + _ADVISORY_TIMEOUT
                while _time.monotonic() < _deadline:
                    try:
                        from core.database import acquire_advisory_lock
                        if acquire_advisory_lock(_adv_lock_name, _adv_holder_id,
                                                 ttl_sec=_ADVISORY_TTL_SEC):
                            _adv_acquired = True
                            break
                    except Exception:
                        try:
                            from bot_utils.silent_log import silent_log
                            silent_log(
                                f"close_lock advisory acquire({_adv_lock_name})",
                                RuntimeError("advisory lock backend unavailable"),
                            )
                        except Exception:
                            pass
                        break
                    _time.sleep(0.05)
                if _adv_acquired:
                    _renew_stop = threading.Event()
                    _interval = max(0.05, min(30.0, _ADVISORY_TTL_SEC / 3.0))

                    def _renew_loop():
                        warned = False
                        while not _renew_stop.wait(_interval):
                            try:
                                from core.database import (
                                    acquire_advisory_lock,
                                    renew_advisory_lock,
                                )
                                if not renew_advisory_lock(
                                        _adv_lock_name, _adv_holder_id,
                                        ttl_sec=_ADVISORY_TTL_SEC):
                                    reacquired = False
                                    if not _renew_stop.is_set():
                                        try:
                                            reacquired = acquire_advisory_lock(
                                                _adv_lock_name, _adv_holder_id,
                                                ttl_sec=_ADVISORY_TTL_SEC)
                                        except Exception:
                                            reacquired = False
                                    if reacquired and _renew_stop.is_set():
                                        try:
                                            from core.database import release_advisory_lock
                                            release_advisory_lock(
                                                _adv_lock_name, _adv_holder_id)
                                        except Exception:
                                            pass
                                        reacquired = False
                                    if reacquired:
                                        warned = False
                                        continue
                                    if not warned:
                                        warned = True
                                        try:
                                            from bot_utils.silent_log import silent_log
                                            silent_log(
                                                f"close_lock advisory renew({_adv_lock_name})",
                                                RuntimeError("advisory lock renewal failed"),
                                            )
                                        except Exception:
                                            pass
                                    continue
                                warned = False
                            except Exception as exc:
                                if not warned:
                                    warned = True
                                    try:
                                        from bot_utils.silent_log import silent_log
                                        silent_log(
                                            f"close_lock advisory renew({_adv_lock_name})",
                                            exc,
                                        )
                                    except Exception:
                                        pass
                                continue

                    _renew_thread = threading.Thread(
                        target=_renew_loop, name=f"renew-{_adv_lock_name}",
                        daemon=True)
                    _renew_thread.start()
        if got and _adv_lock_name and not _adv_acquired and not fail_open:
            try:
                lock.release()
            except RuntimeError:
                pass
            got = False
        yield got
    finally:
        if _renew_stop is not None:
            try:
                _renew_stop.set()
            except Exception:
                pass
        if _renew_thread is not None:
            try:
                _renew_thread.join(
                    timeout=max(1.0, min(5.0, float(_ADVISORY_TIMEOUT) + 0.5)))
            except Exception:
                pass
        if _adv_acquired and _adv_lock_name and _adv_holder_id:
            try:
                from core.database import release_advisory_lock
                if not release_advisory_lock(_adv_lock_name, _adv_holder_id):
                    try:
                        from bot_utils.silent_log import silent_log
                        silent_log(
                            f"close_lock advisory release({_adv_lock_name})",
                            RuntimeError("advisory lock release failed"),
                        )
                    except Exception:
                        pass
            except Exception:
                try:
                    from bot_utils.silent_log import silent_log
                    silent_log(
                        f"close_lock advisory release({_adv_lock_name})",
                        RuntimeError("advisory lock release raised"),
                    )
                except Exception:
                    pass
        if got:
            try:
                lock.release()
            except RuntimeError:
                pass
        _decref(sym)


def release_lock(sym: str) -> None:
    """
    Hint that the position is fully gone. The lock object is NOT dropped
    here  gc_idle_locks() does that safely when no caller holds a reference.

    Kept as a no-op for API compatibility.
    """
    return None


def gc_idle_locks() -> int:
    """Drop locks whose refcount is 0 AND that are currently uncontended.

    Returns the number dropped. Safe to call from a periodic maintenance
    thread (~every 5 minutes).
    """
    dropped = 0
    with _MUTEX:
        for sym in list(_LOCKS.keys()):
            if _REFCOUNTS.get(sym, 0) > 0:
                continue
            lock = _LOCKS.get(sym)
            if lock is None:
                _REFCOUNTS.pop(sym, None)
                continue
            # Acquire non-blocking; only drop if no contention RIGHT NOW.
            if lock.acquire(blocking=False):
                try:
                    _LOCKS.pop(sym, None)
                    _REFCOUNTS.pop(sym, None)
                    dropped += 1
                finally:
                    lock.release()
    return dropped


def lock_count() -> int:
    with _MUTEX:
        return len(_LOCKS)
