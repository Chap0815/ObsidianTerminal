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

import hashlib
import os
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Dict

import portalocker

from bot_utils.runtime_threads import thread_definitely_never_started


# Single mutex protects ALL of: _LOCKS, _REFCOUNTS, _CREATED_AT
_MUTEX     = threading.Lock()
_LOCKS:     Dict[str, threading.Lock] = {}
_REFCOUNTS: Dict[str, int]            = {}

_ADVISORY_TTL_SEC   = 120
_ADVISORY_TIMEOUT   = 2.0
_RENEWAL_MUTEX = threading.Lock()
_RENEWAL_GENERATIONS: dict[int, dict] = {}
_RENEWAL_BY_LOCK: dict[str, dict] = {}


def _retire_renewal_generation(state: dict) -> None:
    with _RENEWAL_MUTEX:
        if _RENEWAL_GENERATIONS.get(id(state)) is state:
            _RENEWAL_GENERATIONS.pop(id(state), None)
        lock_name = state.get("lock_name")
        if _RENEWAL_BY_LOCK.get(lock_name) is state:
            _RENEWAL_BY_LOCK.pop(lock_name, None)


def _renewal_admission_blocked(lock_name: str) -> bool:
    with _RENEWAL_MUTEX:
        state = _RENEWAL_BY_LOCK.get(lock_name)
        if state is None:
            return False
        if not state["done"].is_set():
            return True
        if _RENEWAL_GENERATIONS.get(id(state)) is state:
            _RENEWAL_GENERATIONS.pop(id(state), None)
        if _RENEWAL_BY_LOCK.get(lock_name) is state:
            _RENEWAL_BY_LOCK.pop(lock_name, None)
        return False


def _publish_renewal_generation(state: dict) -> bool:
    lock_name = state["lock_name"]
    with _RENEWAL_MUTEX:
        current = _RENEWAL_BY_LOCK.get(lock_name)
        if current is not None and not current["done"].is_set():
            return False
        if current is not None:
            if _RENEWAL_GENERATIONS.get(id(current)) is current:
                _RENEWAL_GENERATIONS.pop(id(current), None)
            if _RENEWAL_BY_LOCK.get(lock_name) is current:
                _RENEWAL_BY_LOCK.pop(lock_name, None)
        _RENEWAL_GENERATIONS[id(state)] = state
        _RENEWAL_BY_LOCK[lock_name] = state
        return True


def _close_os_lock_relative(lock_name: str) -> Path:
    """Return a fixed-length, path-safe key for one close namespace."""
    digest = hashlib.sha256(lock_name.encode("utf-8")).hexdigest()
    return Path("logs") / ".close_locks" / f"{digest}.lock"


def _acquire_close_os_lock(lock_name: str):
    """Acquire the root-bound kernel lock that owns close exclusivity."""
    from core.paths import PROJECT_ROOT
    from update_barrier import (
        assert_root_bound_lock_handle,
        prepare_root_bound_lock_path,
    )

    relative = _close_os_lock_relative(lock_name)
    path = prepare_root_bound_lock_path(PROJECT_ROOT, relative)
    lease = portalocker.Lock(
        str(path),
        mode="a+b",
        timeout=_ADVISORY_TIMEOUT,
        check_interval=0.05,
        fail_when_locked=False,
    )
    handle = lease.acquire()
    try:
        assert_root_bound_lock_handle(PROJECT_ROOT, relative, handle)
    except BaseException:
        try:
            lease.release()
        except Exception:
            pass
        raise
    return lease


def _log_close_lock_failure(context: str, message: str) -> None:
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, RuntimeError(message))
    except Exception:
        pass


def _process_run_id() -> str:
    configured = os.getenv("BOT_RUN_ID", "").strip()
    if (
        len(configured) == 32
        and configured.isascii()
        and all(char in "0123456789abcdefABCDEF" for char in configured)
    ):
        return configured.lower()
    return uuid.uuid4().hex


_PROCESS_RUN_ID = _process_run_id()


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
    Acquire the per-symbol close lock (in-process), a root-bound OS file lock,
    and the diagnostic advisory lease backed by the SQLite table.  The OS lock
    owns mutual exclusion for the whole context even if the renewable DB lease
    expires temporarily.

    Routine accounting paths fail closed when either cross-process barrier
    cannot be acquired. Emergency paths may opt into ``fail_open=True`` only
    for a missing diagnostic DB lease while the OS lock is held. OS-lock
    contention always yields ``False`` so the caller can choose an explicit
    flatten-without-accounting path without claiming ownership.

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
    _renew_state = None
    _os_lease = None
    _renewal_blocked = False

    try:
        got = lock.acquire(timeout=timeout)
        if got:
            _bn = bot_name or ""
            _base = sym.split("/")[0].split(":")[0].strip().upper()
            if _bn and _base:
                _adv_lock_name = _advisory_lock_name(_bn, _base)
                _adv_holder_id = f"v2:{os.getpid()}:{_PROCESS_RUN_ID}"
                import time as _time
                if _renewal_admission_blocked(_adv_lock_name):
                    _renewal_blocked = True
                    _log_close_lock_failure(
                        f"close_lock renewal admission({_adv_lock_name})",
                        "prior renewal generation is unresolved",
                    )
                else:
                    try:
                        _os_lease = _acquire_close_os_lock(_adv_lock_name)
                    except Exception:
                        _log_close_lock_failure(
                            f"close_lock OS acquire({_adv_lock_name})",
                            "root-bound close lock unavailable",
                        )
                if (
                    _os_lease is not None
                    and _renewal_admission_blocked(_adv_lock_name)
                ):
                    _renewal_blocked = True
                    _log_close_lock_failure(
                        f"close_lock renewal handoff({_adv_lock_name})",
                        "prior renewal generation is unresolved",
                    )
                    try:
                        _os_lease.release()
                    except Exception:
                        _log_close_lock_failure(
                            f"close_lock OS release({_adv_lock_name})",
                            "root-bound close lock release failed",
                        )
                    else:
                        _os_lease = None
                if _os_lease is not None and not _renewal_blocked:
                    _deadline = _time.monotonic() + _ADVISORY_TIMEOUT
                    while _time.monotonic() < _deadline:
                        try:
                            from core.database import acquire_advisory_lock
                            if acquire_advisory_lock(
                                _adv_lock_name,
                                _adv_holder_id,
                                ttl_sec=_ADVISORY_TTL_SEC,
                            ):
                                _adv_acquired = True
                                break
                        except Exception:
                            _log_close_lock_failure(
                                f"close_lock advisory acquire({_adv_lock_name})",
                                "advisory lock backend unavailable",
                            )
                            break
                        _time.sleep(0.05)
                if _adv_acquired:
                    _renew_stop = threading.Event()
                    _interval = max(0.05, min(30.0, _ADVISORY_TTL_SEC / 3.0))
                    _renew_state = {
                        "stop": _renew_stop,
                        "done": threading.Event(),
                        "thread": None,
                        "lock_name": _adv_lock_name,
                        "holder_id": _adv_holder_id,
                    }

                    def _release_late_lease() -> None:
                        try:
                            from core.database import release_advisory_lock
                            if not release_advisory_lock(
                                _adv_lock_name,
                                _adv_holder_id,
                            ):
                                _log_close_lock_failure(
                                    "close_lock late advisory release"
                                    f"({_adv_lock_name})",
                                    "late advisory release was not confirmed",
                                )
                        except Exception:
                            _log_close_lock_failure(
                                "close_lock late advisory release"
                                f"({_adv_lock_name})",
                                "late advisory release raised",
                            )

                    def _renew_loop():
                        warned = False
                        while not _renew_stop.wait(_interval):
                            try:
                                from core.database import (
                                    acquire_advisory_lock,
                                    renew_advisory_lock,
                                )
                                renewed = renew_advisory_lock(
                                        _adv_lock_name, _adv_holder_id,
                                        ttl_sec=_ADVISORY_TTL_SEC)
                                if renewed and _renew_stop.is_set():
                                    _release_late_lease()
                                    break
                                if not renewed:
                                    reacquired = False
                                    if not _renew_stop.is_set():
                                        try:
                                            reacquired = acquire_advisory_lock(
                                                _adv_lock_name, _adv_holder_id,
                                                ttl_sec=_ADVISORY_TTL_SEC)
                                        except Exception:
                                            reacquired = False
                                    if reacquired and _renew_stop.is_set():
                                        _release_late_lease()
                                        reacquired = False
                                        break
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

                    def _owned_renew_loop():
                        try:
                            _renew_loop()
                        finally:
                            _renew_state["done"].set()
                            _retire_renewal_generation(_renew_state)

                    _renew_thread = threading.Thread(
                        target=_owned_renew_loop,
                        name=f"renew-{_adv_lock_name}",
                        daemon=True)
                    _renew_state["thread"] = _renew_thread
                    if not _publish_renewal_generation(_renew_state):
                        _renew_state["stop"].set()
                        _renew_state["done"].set()
                        try:
                            from core.database import release_advisory_lock
                            release_advisory_lock(
                                _adv_lock_name,
                                _adv_holder_id,
                            )
                        finally:
                            _adv_acquired = False
                            _renew_thread = None
                            _renew_state = None
                    else:
                        try:
                            _renew_thread.start()
                        except BaseException:
                            if thread_definitely_never_started(_renew_thread):
                                _renew_state["done"].set()
                                _retire_renewal_generation(_renew_state)
                            raise
        if (
            got
            and _adv_lock_name
            and (
                _os_lease is None
                or (not _adv_acquired and not fail_open)
            )
        ):
            try:
                lock.release()
            except RuntimeError:
                pass
            got = False
        yield got
    finally:
        if _renew_state is not None:
            try:
                _renew_state["stop"].set()
            except Exception:
                pass
        if _renew_state is not None and _renew_thread is not None:
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
        if _os_lease is not None:
            try:
                _os_lease.release()
            except Exception:
                _log_close_lock_failure(
                    f"close_lock OS release({_adv_lock_name})",
                    "root-bound close lock release failed",
                )
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
