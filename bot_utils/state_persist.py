"""
bot_utils/state_persist.py  Crash-safe JSON persistence and state schema
validation, shared by all bots.

Key behaviors:
  fsync the containing directory (POSIX) after rename so a power-loss
    cannot lose the directory entry.
  Windows: retry os.replace() up to 8 when a concurrent reader has the
    file open (the launcher polls trades.json every ~1.5s).
"""
from __future__ import annotations

import json
import math
import os
import threading
import time
from typing import Tuple, List, Optional, Callable


def _read_persist_retries(default: int = 8) -> int:
    raw = os.getenv("STATE_PERSIST_RETRIES", "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        return v if 1 <= v <= 100 else default
    except (TypeError, ValueError):
        return default


def _read_persist_retry_sleep(default: float = 0.05) -> float:
    raw = os.getenv("STATE_PERSIST_RETRY_SLEEP", "").strip()
    if not raw:
        return default
    try:
        v = float(raw)
        return v if 0.0 < v <= 5.0 else default
    except (TypeError, ValueError):
        return default


_PERSIST_RETRIES     = _read_persist_retries()
_PERSIST_RETRY_SLEEP = _read_persist_retry_sleep()


#  Atomic write 

def atomic_save_json(path: str, data) -> bool:
    """Crash-safe JSON write: tmp + fsync(file) + atomic rename + fsync(dir).

    Never raises. Returns True when either the atomic write or the fallback
    write completed, False when persistence failed completely.

    The tmp filename includes pid + thread-id so concurrent writers (launcher
    poller + bot subprocess, multiple TradeState instances) don't overwrite
    each other's tmp files.
    """
    # unique tmp per writer to avoid cross-process tmp collision
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    # defaults from module-level (read once from env at import)
    _max_retries = _PERSIST_RETRIES
    _retry_sleep = _PERSIST_RETRY_SLEEP
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except (AttributeError, OSError):
                pass

        # Windows retry: target file may be open by the launcher's poller.
        # Retry count + sleep configurable via env vars
        # (STATE_PERSIST_RETRIES, STATE_PERSIST_RETRY_SLEEP).
        # Default 8  50ms = 400ms  enough for the launcher's typical
        # 1.5s poll-cycle to release the read handle.
        last_err = None
        for _ in range(_max_retries):
            try:
                os.replace(tmp, path)
                last_err = None
                break
            except PermissionError as pe:
                last_err = pe
                time.sleep(_retry_sleep)
            except Exception:
                raise
        if last_err is not None:
            raise last_err

        # POSIX: durably persist the directory entry too
        try:
            d_fd = os.open(
                os.path.dirname(os.path.abspath(path)) or ".",
                os.O_RDONLY
            )
            try:
                os.fsync(d_fd)
            finally:
                os.close(d_fd)
        except (AttributeError, OSError):
            pass
        return True

    except Exception as exc:
        # Log instead of silent drop. Disk-full / permission errors otherwise
        # leave state divergent from disk indefinitely.
        try:
            from bot_utils.silent_log import silent_log
            silent_log(f"atomic_save_json({path})", exc)
        except Exception:
            pass
        # Fallback  better non-atomic than nothing
        try:
            from core.logger import save_j
            save_j(path, data)
            return True
        except Exception:
            return False
    finally:
        # Remove the per-writer tmp on any non-success exit path so persistent
        # replace failures don't accumulate orphaned *.tmp.<pid>.<tid> files.
        # On success os.replace already consumed tmp, so this is a no-op.
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


#  Generic state validator 

def _validate_state(trades: dict,
                     require_position_type: bool,
                     log_fn: Optional[Callable] = None,
                     ) -> Tuple[dict, List[str]]:
    """Shared validator for both spot and futures state.

    Required: buy (float > 0), amount (float >= 0), buy_time (str).
    For futures, also: position_type  {"LONG", "SHORT"}.

    Heals NaN/Inf in optional numeric fields (highest, invested_usdt,
    fees_paid, initial_entry_fee, original_amount, funding_paid,
    liquidation_price).

    Returns: (clean_trades, rejected_symbols).
    """
    clean: dict = {}
    rejected: List[str] = []

    if not isinstance(trades, dict):
        if log_fn:
            log_fn(
                f"state load: top-level not dict "
                f"({type(trades).__name__})  starting empty",
                "WARN",
            )
        return clean, rejected

    for sym, d in trades.items():
        if not isinstance(d, dict):
            rejected.append(f"{sym}(not-dict)")
            continue

        try:
            buy = float(d.get("buy_price") or d.get("buy") or 0)
            if not math.isfinite(buy) or buy <= 0:
                rejected.append(f"{sym}(buy={d.get('buy')})")
                continue
        except (TypeError, ValueError):
            rejected.append(f"{sym}(buy-cast)")
            continue

        try:
            amt = float(d.get("amount", 0))
            if not math.isfinite(amt) or amt <= 0:
                rejected.append(f"{sym}(amount={d.get('amount')})")
                continue
        except (TypeError, ValueError):
            rejected.append(f"{sym}(amount-cast)")
            continue

        if require_position_type and d.get("position_type") not in ("LONG", "SHORT"):
            rejected.append(f"{sym}(position_type)")
            continue

        if not isinstance(d.get("buy_time"), str):
            rejected.append(f"{sym}(buy_time)")
            continue

        # Heal NaN/Inf in optional numeric fields
        for field in ("highest", "invested_usdt", "fees_paid",
                       "initial_entry_fee", "original_amount",
                       "funding_paid", "liquidation_price"):
            v = d.get(field)
            if v is not None:
                try:
                    fv = float(v)
                    if not math.isfinite(fv):
                        d[field] = 0.0 if field != "highest" else buy
                except (TypeError, ValueError):
                    d[field] = 0.0 if field != "highest" else buy

        clean[sym] = d

    return clean, rejected


def validate_spot_state(trades: dict,
                         log_fn: Optional[Callable] = None,
                         ) -> Tuple[dict, List[str]]:
    """Validate a loaded spot trades.json schema."""
    return _validate_state(trades, require_position_type=False, log_fn=log_fn)


def validate_futures_state(trades: dict,
                            log_fn: Optional[Callable] = None,
                            ) -> Tuple[dict, List[str]]:
    """Validate a loaded futures trades.json schema (requires position_type)."""
    return _validate_state(trades, require_position_type=True, log_fn=log_fn)
