"""
cooldown_utils.py  Thread- and process-safe, UTC-based cooldown helper.

Public API:
  set_cooldown(cool, symbol, minutes, cooldown_file)  schreibend
  check_in_cooldown(cool, symbol) -> bool  READ-ONLY (Hot-Path)
  is_in_cooldown(cool, symbol, file)  Legacy, auch read+purge
  purge_expired(cool, file) -> int  explizit, periodisch
"""
from __future__ import annotations

import math
import os
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional


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
_MAX_COOLDOWN_MINUTES = 366 * 24 * 60


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
    try:
        if os.name == "nt":
            try:
                import psutil  # type: ignore
                return psutil.pid_exists(pid)
            except ImportError:
                return True
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


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
        return _persist(cooldown_file, cool)


def check_in_cooldown(cool: dict, symbol: str) -> bool:
    """READ-ONLY cooldown check. Mutiert nicht, schreibt nicht.
    Hot-Path-Aufrufe (Buy-Loop) sollten DIES verwenden, nicht is_in_cooldown."""
    raw = cool.get(symbol)
    if raw is None:
        return False
    try:
        expiry = datetime.fromisoformat(raw)
        if expiry.tzinfo is not None:
            expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
        return _utcnow() < expiry
    except (TypeError, ValueError):
        return False


def is_in_cooldown(cool: dict, symbol: str, cooldown_file: str) -> bool:
    """Legacy: read AND auto-purge expired entries. Schreibt die Datei
    on expired hits; do not use in hot paths."""
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
        except (TypeError, ValueError):
            cool.pop(symbol, None)
        _persist(cooldown_file, cool)
    return False


def purge_expired(cool: dict, cooldown_file: str) -> int:
    """Explizit alle expired entries entfernen. Returns # removed."""
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
            except (TypeError, ValueError):
                cool.pop(sym, None)
                removed += 1
        if removed > 0:
            _persist(cooldown_file, cool)
    return removed


#  Persistence 

def _active_cooldowns(data, now: datetime) -> dict[str, datetime]:
    if not isinstance(data, dict):
        return {}
    active: dict[str, datetime] = {}
    for symbol, raw_expiry in data.items():
        if not isinstance(symbol, str) or not symbol:
            continue
        try:
            expiry = datetime.fromisoformat(raw_expiry)
        except (TypeError, ValueError):
            continue
        if expiry.tzinfo is not None:
            expiry = expiry.astimezone(timezone.utc).replace(tzinfo=None)
        if expiry > now:
            active[symbol] = expiry
    return active


def _persist(path: str, data: dict) -> bool:
    if not path or not isinstance(data, dict):
        return False
    try:
        with _file_lock(path):
            now = _utcnow()
            merged = _active_cooldowns(data, now)
            try:
                import json
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8-sig") as fh:
                        disk = _active_cooldowns(json.load(fh), now)
                    for symbol, expiry in disk.items():
                        current = merged.get(symbol)
                        if current is None or expiry > current:
                            merged[symbol] = expiry
            except Exception:
                pass

            snapshot = {
                symbol: expiry.isoformat()
                for symbol, expiry in merged.items()
            }
            _atomic_write_json(path, snapshot)
            data.clear()
            data.update(snapshot)
        return True
    except Exception:
        try:
            from core.logger import log_event
            log_event(f"[cooldown] persist failed: {path}", "WARN")
        except Exception:
            pass
        return False


def _atomic_write_json(path: str, data: dict) -> None:
    """Retry budget for Windows AV scan interference."""
    import json
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(
                data,
                fh,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except (AttributeError, OSError):
                pass

        delays = (0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3)
        last_exc: Optional[BaseException] = None
        for d in delays:
            try:
                os.replace(tmp, path)
                return
            except PermissionError as e:
                last_exc = e
                time.sleep(d)
        if last_exc:
            raise last_exc
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
