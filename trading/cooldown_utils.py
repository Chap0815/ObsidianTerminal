"""
cooldown_utils.py  Thread- and process-safe, UTC-based cooldown helper.

Public API:
  set_cooldown(cool, symbol, minutes, cooldown_file)  schreibend
  check_in_cooldown(cool, symbol) -> bool  READ-ONLY (Hot-Path)
  is_in_cooldown(cool, symbol, file)  Legacy, auch read+purge
  purge_expired(cool, file) -> int  explizit, periodisch
"""
from __future__ import annotations

import atexit
import json
import math
import os
import socket
import stat
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from bot_utils.runtime_threads import thread_definitely_never_started


try:
    import portalocker  # type: ignore
    _HAS_PORTALOCKER = True
except ImportError:
    _HAS_PORTALOCKER = False
    import warnings
    warnings.warn(
        "portalocker is REQUIRED for cross-process cooldown locking. "
        "Install it with `pip install portalocker`.",
        stacklevel=2,
    )


def _utcnow() -> datetime:
    try:
        from core.clock import now_utc

        current = now_utc()
        if current.tzinfo is not None:
            current = current.astimezone(timezone.utc)
        return current.replace(tzinfo=None)
    except Exception:
        return datetime.now(timezone.utc).replace(tzinfo=None)


# Stop-type exit reasons emitted by the spot/futures exit evaluators. A LOSING
# close on ANY of these should arm the post-stop re-entry cooldown  not just a
# hard "Stop-Loss"  otherwise a coin chopping through the trailing/break-even
# stop can be re-bought on the very next scan tick (fee-bleed re-entry spam).
_STOP_EXIT_REASONS = ("Stop-Loss", "Trailing Stop",
                      "Break-Even Stop", "Breakeven-Stop",
                      "Pre-Activation Giveback Stop",
                      "Aged MFE Fallback Stop")


def should_cooldown_after_exit(reason: str, profit_usdt: float) -> bool:
    """True if a closed trade should arm the post-stop re-entry cooldown.

    Gated on OUTCOME, not just the reason string: a liquidation-protection exit
    always cools down; any other protective stop cools down only when it closed
    at a loss. Time-based exits (e.g. "Max Hold Time") never arm it. Shared by
    the spot and futures exit paths so both stay in sync (no per-site drift).
    """
    r = str(reason or "")
    if "Liq" in r:
        return True
    if profit_usdt < 0 and any(s in r for s in _STOP_EXIT_REASONS):
        return True
    return False


_COOLDOWN_LOCK = threading.Lock()
_COOLDOWN_ADMISSION_LOCK = threading.Lock()
_COOLDOWN_SHUTDOWN_LIFECYCLE_LOCK = threading.Lock()
_MAX_COOLDOWN_MINUTES = 366 * 24 * 60
_COOLDOWN_JSON_MAX_BYTES = 1024 * 1024
_COOLDOWN_PERSIST_RETRY_SEC = 30.0
_cooldown_retry_timers: dict[str, threading.Timer] = {}
_cooldown_retry_data: dict[str, dict] = {}
_cooldown_active_timers: set[threading.Thread] = set()
_cooldown_retiring_timers: set[threading.Thread] = set()
_cooldown_persist_shutdown = False
_cooldown_shutdown_generation: dict | None = None


class CooldownState(dict):
    """Cooldown mapping carrying whether its persisted source was trustworthy."""

    def __init__(self, *args, source_valid: bool = True, source_error: str = ""):
        super().__init__(*args)
        self.source_valid = source_valid
        self.source_error = source_error


def _read_cooldown_json(path: str):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate cooldown JSON key: {key}")
            result[key] = value
        return result

    with open(path, "rb") as stream:
        raw = stream.read(_COOLDOWN_JSON_MAX_BYTES + 1)
    if len(raw) > _COOLDOWN_JSON_MAX_BYTES:
        raise ValueError("cooldown JSON exceeds size limit")
    return json.loads(
        raw.decode("utf-8-sig"),
        object_pairs_hook=unique_object,
    )


def load_cooldown_state(path: str) -> CooldownState:
    """Load persisted cooldowns without turning corruption into no cooldowns.

    A missing file is a valid empty initial state.  Any existing unreadable or
    malformed source returns an invalid state whose hot-path checks block all
    entries; exits and position management remain available.
    """
    try:
        data = _read_cooldown_json(path)
        now = _utcnow()
        # Validate the entire source, including expired rows.  Expiry cleanup
        # is a separate operation and must never disguise malformed evidence.
        _active_cooldowns(data, now, include_expired=True)
        return CooldownState(data)
    except FileNotFoundError:
        return CooldownState()
    except Exception as exc:
        detail = str(exc).strip()
        error = type(exc).__name__ + (f": {detail[:200]}" if detail else "")
        return CooldownState(source_valid=False, source_error=error)


def _normalize_cooldown_minutes(value) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed > _MAX_COOLDOWN_MINUTES:
        return None
    return max(0, int(parsed))


def _boot_fingerprint() -> str:
    try:
        if os.name == "posix":
            try:
                with open("/proc/sys/kernel/random/boot_id", "r") as fh:
                    return fh.read().strip()
            except OSError:
                pass
            try:
                return str(int(os.stat("/proc/1").st_ctime))
            except OSError:
                pass
        try:
            return f"{socket.gethostname()}-{int(os.path.getctime(os.path.abspath(os.sep)))}"
        except OSError:
            return socket.gethostname()
    except Exception:
        return "unknown-boot"


_BOOT_FP = _boot_fingerprint()


def _pid_alive(pid: int, payload_boot_fp: str = "") -> bool:
    """True wenn PID laeuft UND from same boot. Recycled PIDs = dead."""
    if pid <= 0:
        return False
    if payload_boot_fp and payload_boot_fp != _BOOT_FP:
        return False
    from core.process_identity import pid_alive

    return pid_alive(pid)


@contextmanager
def _file_lock(path: str, timeout: float = 5.0):
    """Cross-process advisory lock keyed on path + '.lock'."""
    lock_path = path + ".lock"
    try:
        d = os.path.dirname(lock_path) or "."
        os.makedirs(d, exist_ok=True)
    except OSError:
        pass

    if _HAS_PORTALOCKER:
        with portalocker.Lock(
            lock_path, mode="a", timeout=timeout,
            check_interval=0.05, fail_when_locked=False,
        ):
            yield
        return

    # Fallback: TOCTOU-safe rename-takeover
    deadline = time.monotonic() + timeout
    payload = f"{os.getpid()}|{_BOOT_FP}|{time.time():.3f}".encode()
    acquired = False
    while time.monotonic() < deadline:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, payload)
                try:
                    os.fsync(fd)
                except (AttributeError, OSError):
                    pass
            finally:
                os.close(fd)
            acquired = True
            break
        except FileExistsError:
            holder_pid, holder_fp = None, ""
            try:
                with open(lock_path, "rb") as fh:
                    raw = fh.read(256).decode("utf-8", errors="replace").strip()
                parts = raw.split("|")
                if parts and parts[0].isdigit():
                    holder_pid = int(parts[0])
                if len(parts) > 1:
                    holder_fp = parts[1]
            except (OSError, ValueError):
                holder_pid = None

            stale = (holder_pid is None or
                     not _pid_alive(holder_pid, holder_fp))
            if stale:
                stale_path = f"{lock_path}.stale.{os.getpid()}.{int(time.time())}"
                try:
                    os.rename(lock_path, stale_path)
                    try:
                        os.remove(stale_path)
                    except OSError:
                        pass
                except OSError:
                    pass
                continue
            time.sleep(0.05)
        except OSError:
            time.sleep(0.05)

    try:
        if not acquired:
            raise TimeoutError(f"cooldown lock timeout: {lock_path}")
        yield
    finally:
        if acquired:
            try:
                os.remove(lock_path)
            except OSError:
                pass


#  Public API 

def _schedule_cooldown_retry_locked(path: str) -> None:
    """Schedule one daemon persistence retry; caller holds _COOLDOWN_LOCK."""
    existing = _cooldown_retry_timers.get(path)
    if existing is not None:
        try:
            if existing.is_alive():
                return
        except Exception:
            pass
    with _COOLDOWN_ADMISSION_LOCK:
        if _cooldown_persist_shutdown:
            return
        timer = None

        def retry_owned_generation() -> None:
            _retry_cooldown_persist(path, timer)

        timer = threading.Timer(
            _COOLDOWN_PERSIST_RETRY_SEC,
            retry_owned_generation,
        )
        timer.daemon = True
        _cooldown_retry_timers[path] = timer
        try:
            timer.start()
        except Exception as exc:
            if _cooldown_retry_timers.get(path) is timer:
                _cooldown_retry_timers.pop(path, None)
            try:
                from bot_utils.silent_log import silent_log
                silent_log("schedule cooldown persistence retry", exc)
            except Exception:
                pass


def _retry_cooldown_persist(
    path: str,
    owner: threading.Timer | None = None,
) -> None:
    current = threading.current_thread()
    with _COOLDOWN_LOCK:
        _cooldown_active_timers.add(current)
        try:
            if (
                owner is None
                or _cooldown_retry_timers.get(path) is not owner
            ):
                return
            _cooldown_retry_timers.pop(path, None)
            if _cooldown_persist_shutdown:
                return
            data = _cooldown_retry_data.get(path)
            if data is not None:
                _persist_with_retry_locked(path, data)
        finally:
            _cooldown_active_timers.discard(current)


def _persist_with_retry_locked(path: str, data: dict) -> bool:
    """Persist now and retain the latest live mapping until it is durable."""
    try:
        persisted = _persist(path, data) is True
    except Exception:
        persisted = False
    if persisted:
        _cooldown_retry_data.pop(path, None)
        timer = _cooldown_retry_timers.pop(path, None)
        if timer is not None:
            try:
                timer.cancel()
            except Exception:
                pass
        return True
    if not path:
        return False
    _cooldown_retry_data[path] = data
    _schedule_cooldown_retry_locked(path)
    return False


def _shutdown_cooldown_flush(generation: dict) -> None:
    """Flush one terminal snapshot batch without blocking the closer."""
    try:
        current = threading.current_thread()
        with _COOLDOWN_LOCK:
            _cooldown_active_timers.add(current)
            try:
                for path, data in list(_cooldown_retry_data.items()):
                    try:
                        persisted = _persist(path, data) is True
                    except Exception:
                        persisted = False
                    if persisted:
                        _cooldown_retry_data.pop(path, None)
            finally:
                _cooldown_active_timers.discard(current)
    finally:
        with _COOLDOWN_LOCK:
            generation["done"].set()


def _acquire_cooldown_lock_until(deadline: float) -> bool:
    return _COOLDOWN_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    )


def _shutdown_cooldown_persistence_owned(timeout: float = 0.0) -> bool:
    """Terminally stop retry timers and durably flush deferred snapshots."""
    global _cooldown_persist_shutdown, _cooldown_shutdown_generation
    try:
        budget = float(timeout)
    except (TypeError, ValueError, OverflowError):
        budget = 0.0
    if not math.isfinite(budget):
        budget = 0.0
    deadline = time.monotonic() + max(0.0, budget)

    # Serialize terminal publication against the short check+Timer.start
    # admission section.  This lock never protects persistence I/O.
    if not _COOLDOWN_ADMISSION_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        _cooldown_persist_shutdown = True
    finally:
        _COOLDOWN_ADMISSION_LOCK.release()
    if not _acquire_cooldown_lock_until(deadline):
        return False
    try:
        timers = list(_cooldown_retry_timers.values())
        timers.extend(_cooldown_active_timers)
        timers.extend(_cooldown_retiring_timers)
        flush_generation = _cooldown_shutdown_generation
        if flush_generation is not None:
            flush_worker = flush_generation.get("worker")
            if flush_worker is not None:
                timers.append(flush_worker)
        _cooldown_retiring_timers.update(timers)
        _cooldown_retry_timers.clear()
        for timer in timers:
            try:
                timer.cancel()
            except Exception:
                pass
    finally:
        _COOLDOWN_LOCK.release()

    still_alive = []
    for timer in dict.fromkeys(timers):
        join = getattr(timer, "join", None)
        if callable(join) and timer is not threading.current_thread():
            try:
                join(timeout=max(0.0, deadline - time.monotonic()))
            except Exception:
                still_alive.append(timer)
                continue
        try:
            if timer.is_alive():
                still_alive.append(timer)
        except Exception:
            still_alive.append(timer)

    if not _acquire_cooldown_lock_until(deadline):
        return False
    try:
        _cooldown_retiring_timers.intersection_update(still_alive)
        if still_alive:
            return False
        if (
            flush_generation is not None
            and not flush_generation["done"].is_set()
        ):
            return False
        if _cooldown_shutdown_generation is flush_generation:
            _cooldown_shutdown_generation = None
        if not _cooldown_retry_data:
            return True
        generation = {
            "done": threading.Event(),
            "worker": None,
            "start_raised": False,
        }

        def run_owned_flush() -> None:
            _shutdown_cooldown_flush(generation)

        try:
            flush = threading.Thread(
                target=run_owned_flush,
                name="cooldown-shutdown-persist",
                daemon=True,
            )
        except BaseException as exc:
            try:
                from bot_utils.silent_log import silent_log

                silent_log("construct cooldown shutdown persistence", exc)
            except BaseException:
                pass
            if not isinstance(exc, Exception):
                raise
            return False
        generation["worker"] = flush
        _cooldown_shutdown_generation = generation
        _cooldown_retiring_timers.add(flush)
        try:
            flush.start()
        except BaseException as exc:
            generation["start_raised"] = True
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(flush)
            ):
                generation["done"].set()
                _cooldown_retiring_timers.discard(flush)
                if _cooldown_shutdown_generation is generation:
                    _cooldown_shutdown_generation = None
            try:
                from bot_utils.silent_log import silent_log

                silent_log("start cooldown shutdown persistence", exc)
            except BaseException:
                pass
            if not isinstance(exc, Exception):
                raise
            return False
    finally:
        _COOLDOWN_LOCK.release()

    try:
        if flush is not threading.current_thread():
            flush.join(timeout=max(0.0, deadline - time.monotonic()))
    except Exception:
        return False
    try:
        flush_alive = flush.is_alive()
    except Exception:
        flush_alive = True
    if flush_alive:
        return False
    if not _acquire_cooldown_lock_until(deadline):
        return False
    try:
        completed = (
            _cooldown_shutdown_generation is generation
            and generation["done"].is_set()
        )
        if completed:
            _cooldown_retiring_timers.discard(flush)
            _cooldown_shutdown_generation = None
        return completed and not _cooldown_retry_data
    finally:
        _COOLDOWN_LOCK.release()


def shutdown_cooldown_persistence(timeout: float = 0.0) -> bool:
    """Serialize terminal cooldown persistence under one bounded budget."""
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
    if not _COOLDOWN_SHUTDOWN_LIFECYCLE_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        return _shutdown_cooldown_persistence_owned(
            timeout=max(0.0, deadline - time.monotonic())
        )
    finally:
        _COOLDOWN_SHUTDOWN_LIFECYCLE_LOCK.release()


def flush_pending_cooldowns() -> bool:
    """Interpreter-exit fallback for the managed runtime closer."""
    return shutdown_cooldown_persistence(timeout=1.0)

def set_cooldown(cool: dict, symbol: str, minutes: int,
                 cooldown_file: str) -> bool:
    """Set a cooldown for ``minutes`` minutes from now."""
    minutes = _normalize_cooldown_minutes(minutes)
    if minutes is None:
        return False
    if minutes == 0:
        return True
    expiry_iso = (_utcnow() + timedelta(minutes=minutes)).isoformat()
    with _COOLDOWN_LOCK:
        cool[symbol] = expiry_iso
        if isinstance(cool, CooldownState) and not cool.source_valid:
            return False
        return _persist_with_retry_locked(cooldown_file, cool)


def check_in_cooldown(cool: dict, symbol: str) -> bool:
    """READ-ONLY cooldown check. Mutiert nicht, schreibt nicht.
    Hot-Path-Aufrufe (Buy-Loop) sollten DIES verwenden, nicht is_in_cooldown."""
    if not isinstance(cool, dict):
        return True
    if isinstance(cool, CooldownState) and not cool.source_valid:
        return True
    raw = cool.get(symbol)
    if raw is None:
        return False
    try:
        expiry = datetime.fromisoformat(raw)
        if expiry.tzinfo is not None:
            expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
        return _utcnow() < expiry
    except (TypeError, ValueError, OverflowError):
        return True


def is_in_cooldown(cool: dict, symbol: str, cooldown_file: str) -> bool:
    """Legacy: read AND auto-purge expired entries. Schreibt die Datei
    on expired hits; do not use in hot paths."""
    if not isinstance(cool, dict):
        return True
    if isinstance(cool, CooldownState) and not cool.source_valid:
        return True
    with _COOLDOWN_LOCK:
        raw = cool.get(symbol)
        if raw is None:
            return False
        try:
            expiry = datetime.fromisoformat(raw)
            if expiry.tzinfo is not None:
                expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
            if _utcnow() < expiry:
                return True
            cool.pop(symbol, None)
        except (TypeError, ValueError, OverflowError):
            return True
        _persist_with_retry_locked(cooldown_file, cool)
    return False


def purge_expired(cool: dict, cooldown_file: str) -> int:
    """Explizit alle expired entries entfernen. Returns # removed."""
    if not isinstance(cool, dict):
        return 0
    if isinstance(cool, CooldownState) and not cool.source_valid:
        return 0
    now = _utcnow()
    removed = 0
    with _COOLDOWN_LOCK:
        for sym in list(cool.keys()):
            raw = cool[sym]
            try:
                expiry = datetime.fromisoformat(raw)
                if expiry.tzinfo is not None:
                    expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
                if now >= expiry:
                    cool.pop(sym, None)
                    removed += 1
            except (TypeError, ValueError, OverflowError):
                # Unknown expiry evidence is protective.  Preserve it and let
                # check_in_cooldown block that symbol instead of failing open.
                continue
        if removed > 0:
            _persist_with_retry_locked(cooldown_file, cool)
    return removed


#  Persistence 

def _active_cooldowns(
    data,
    now: datetime,
    *,
    include_expired: bool = False,
) -> dict[str, datetime]:
    if not isinstance(data, dict):
        raise ValueError("cooldown JSON root must be an object")
    active: dict[str, datetime] = {}
    for symbol, raw_expiry in data.items():
        if (
            not isinstance(symbol, str)
            or not symbol.strip()
            or len(symbol) > 64
            or any(ord(char) < 32 or ord(char) == 127 for char in symbol)
        ):
            raise ValueError("cooldown symbol is invalid")
        try:
            expiry = datetime.fromisoformat(raw_expiry)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                f"cooldown expiry is invalid for {symbol}"
            ) from exc
        if expiry.tzinfo is not None:
            expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
        if include_expired or expiry > now:
            active[symbol] = expiry
    return active


def _persist(path: str, data: dict) -> bool:
    if not path or not isinstance(data, dict):
        return False
    if isinstance(data, CooldownState) and not data.source_valid:
        return False
    try:
        with _file_lock(path):
            now = _utcnow()
            merged = _active_cooldowns(data, now)
            if os.path.exists(path):
                disk = _active_cooldowns(_read_cooldown_json(path), now)
                for symbol, expiry in disk.items():
                    current = merged.get(symbol)
                    if current is None or expiry > current:
                        merged[symbol] = expiry

            snapshot = {
                symbol: expiry.isoformat()
                for symbol, expiry in merged.items()
            }
            _atomic_write_json(path, snapshot)
            # Readers intentionally avoid the persistence lock in the entry
            # hot path. Publish additions/refreshes before removing expired
            # keys so they can observe an old protective cooldown briefly, but
            # never a transient empty mapping between clear() and update().
            data.update(snapshot)
            for symbol in tuple(data):
                if symbol not in snapshot:
                    data.pop(symbol, None)
        return True
    except Exception:
        try:
            from core.logger import log_event
            log_event(f"[cooldown] persist failed: {path}", "WARN")
        except Exception:
            pass
        return False


def _note_cooldown_cleanup_error(
    primary: BaseException,
    cleanup_error: BaseException,
) -> None:
    try:
        primary.add_note(
            "cooldown temporary cleanup failed: "
            f"{type(cleanup_error).__name__}: {cleanup_error}"
        )
    except BaseException:
        pass


def _fsync_cooldown_directory(path: Path) -> None:
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(str(path), flags)
    except AttributeError:
        return
    except OSError as exc:
        if os.name == "nt":
            if isinstance(exc, PermissionError):
                return
            if (
                isinstance(exc, FileNotFoundError)
                and path == Path(path.anchor)
                and path.is_dir()
            ):
                return
        raise
    primary_error: BaseException | None = None
    try:
        os.fsync(directory_fd)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            os.close(directory_fd)
        except BaseException as close_error:
            if primary_error is None:
                raise
            _note_cooldown_cleanup_error(primary_error, close_error)


def _same_cooldown_temp_generation(
    path: Path,
    identity: tuple[int, int],
) -> bool:
    try:
        current = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return False
    return (
        stat.S_ISREG(current.st_mode)
        and (current.st_dev, current.st_ino) == identity
    )


def _fsync_cooldown_parent_chain(path: Path) -> None:
    current = path
    while True:
        _fsync_cooldown_directory(current)
        if current == current.parent:
            return
        current = current.parent


def _atomic_write_json(path: str, data: dict) -> None:
    """Crash-durable exact-generation JSON publish with Windows retries."""
    directory = Path(os.path.dirname(path) or ".").absolute()
    os.makedirs(directory, exist_ok=True)
    # Retry every parent barrier, including entries created by an earlier
    # failed attempt.  Existence alone does not prove rename durability.
    _fsync_cooldown_parent_chain(directory)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{os.path.basename(path)}.tmp.",
        dir=str(directory),
    )
    tmp = Path(tmp_name)
    fd_owned = True
    temp_owned = True
    temp_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        temp_stat = os.fstat(fd)
        if not stat.S_ISREG(temp_stat.st_mode):
            raise OSError("cooldown temporary path is not a regular file")
        temp_identity = (temp_stat.st_dev, temp_stat.st_ino)
        handle = None
        handle_error: BaseException | None = None
        try:
            handle = os.fdopen(fd, "w", encoding="utf-8")
            fd_owned = False
            json.dump(
                data,
                handle,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            handle_error = exc
            raise
        finally:
            if handle is not None:
                try:
                    handle.close()
                except BaseException as close_error:
                    if handle_error is None:
                        raise
                    _note_cooldown_cleanup_error(handle_error, close_error)

        delays = (0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3)
        last_exc: Optional[BaseException] = None
        for delay in delays:
            try:
                os.replace(tmp, path)
                temp_owned = False
                last_exc = None
                break
            except PermissionError as exc:
                last_exc = exc
                time.sleep(delay)
        if last_exc:
            raise last_exc
        _fsync_cooldown_directory(directory)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        if fd_owned:
            try:
                os.close(fd)
            except BaseException as exc:
                cleanup_error = exc
        same_generation = False
        if temp_owned and temp_identity is not None:
            try:
                same_generation = _same_cooldown_temp_generation(
                    tmp,
                    temp_identity,
                )
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
                else:
                    _note_cooldown_cleanup_error(cleanup_error, exc)
        if same_generation:
            try:
                os.remove(tmp)
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
                else:
                    _note_cooldown_cleanup_error(cleanup_error, exc)
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            _note_cooldown_cleanup_error(primary_error, cleanup_error)


atexit.register(flush_pending_cooldowns)
