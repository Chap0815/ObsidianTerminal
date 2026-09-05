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

import copy
import json
import math
import os
import stat
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
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


# These flags directly steer position ownership, entry/exit recovery,
# accounting, or protective exits.  Truthiness is not a schema: for example,
# the JSON string ``"false"`` is true in Python and can therefore skip a
# partial exit or misclassify a recovery barrier.  Missing fields retain their
# existing backwards-compatible defaults, but present fields must be genuine
# JSON booleans.
_POSITION_BOOLEAN_FIELDS = frozenset((
    "partial_sold",
    "break_even",
    "be_active",
    "accounting_pending",
    "accounting_pending_mode_is_sim",
    "accounting_already_booked",
    "provisional",
    "adopted",
    "claim_conflict",
    "claim_release_pending",
    "entry_sizing_recovery_pending",
    "entry_sizing_recovery_unverified",
    "entry_funding_window_unverified",
    "accounting_pending_funding_unverified",
    "oversize_rollback_pending",
    "verified_flat_pending_accounting",
    "full_exit_outcome_uncertain",
    "partial_exit_outcome_uncertain",
    "emergency_exit_outcome_uncertain",
    "launcher_exit_outcome_uncertain",
    "partial_tp_blocked_min_notional",
    "closing_retry_pending",
    "funding_booked_on_partials_known",
))

_POSITION_SYMBOL_MAX_LENGTH = 64
_POSITION_SYMBOL_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)
_PENDING_ACCOUNTING_FIELDS = frozenset((
    "accounting_pending_partials",
    "unpriced_external_partials",
))


def _normalized_entry_id_or_none(value) -> str | None:
    """Return one canonical causal position ID or reject malformed identity."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("entry_id must be text")
    entry_id = value.strip()
    if not entry_id:
        return None
    if len(entry_id) > 64:
        raise ValueError("entry_id exceeds 64 characters")
    if any(ord(char) < 32 or ord(char) == 127 for char in entry_id):
        raise ValueError("entry_id contains control characters")
    return entry_id


def _valid_pending_accounting_items(value) -> bool:
    if isinstance(value, dict):
        return True
    return isinstance(value, list) and all(
        isinstance(item, dict) for item in value
    )


def is_canonical_position_symbol(value) -> bool:
    """Return whether a state key is one canonical exchange base code."""
    if not isinstance(value, str):
        return False
    if not (1 <= len(value) <= _POSITION_SYMBOL_MAX_LENGTH):
        return False
    if value != value.strip() or value != value.upper():
        return False
    return (
        all(char in _POSITION_SYMBOL_CHARS for char in value)
        and any(char.isascii() and char.isalnum() for char in value)
    )


def position_boolean_rejection_field(row) -> str | None:
    """Return the first present control flag that is not a JSON boolean."""
    if not isinstance(row, dict):
        return None
    return next(
        (
            field
            for field in _POSITION_BOOLEAN_FIELDS
            if field in row and not isinstance(row[field], bool)
        ),
        None,
    )


def persisted_epoch_ttl_active(
    deadlines: dict,
    key,
    *,
    enabled: bool,
    expires_at,
    max_ttl_sec: float,
    epoch_now=None,
) -> bool:
    """Evaluate a restartable epoch TTL monotonically within this process."""
    if enabled is not True:
        deadlines.pop(key, None)
        return False
    if isinstance(expires_at, bool) or isinstance(max_ttl_sec, bool):
        deadlines.pop(key, None)
        return False
    try:
        epoch_deadline = float(expires_at)
        ttl_limit = float(max_ttl_sec)
    except (TypeError, ValueError, OverflowError):
        deadlines.pop(key, None)
        return False
    if (
        not math.isfinite(epoch_deadline)
        or not math.isfinite(ttl_limit)
        or epoch_deadline <= 0.0
        or ttl_limit <= 0.0
    ):
        deadlines.pop(key, None)
        return False
    monotonic_now = time.monotonic()
    generation = deadlines.get(key)
    if (
        not isinstance(generation, tuple)
        or len(generation) != 2
        or generation[0] != epoch_deadline
    ):
        if isinstance(epoch_now, bool):
            deadlines.pop(key, None)
            return False
        try:
            current_epoch = float(time.time() if epoch_now is None else epoch_now)
        except (TypeError, ValueError, OverflowError):
            deadlines.pop(key, None)
            return False
        if not math.isfinite(current_epoch):
            deadlines.pop(key, None)
            return False
        remaining = min(
            ttl_limit,
            max(0.0, epoch_deadline - current_epoch),
        )
        generation = (epoch_deadline, monotonic_now + remaining)
        deadlines[key] = generation
    return monotonic_now < generation[1]


#  Atomic write 

def _log_atomic_save_failure(path: str, exc: BaseException) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(f"atomic_save_json({path})", exc)
    except BaseException:
        pass


def _fsync_parent_directory(path: str) -> None:
    """Durably publish one replaced directory entry where the OS supports it."""
    parent = os.path.dirname(os.path.abspath(path)) or "."
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        create_file.restype = wintypes.HANDLE
        flush_file_buffers = kernel32.FlushFileBuffers
        flush_file_buffers.argtypes = [wintypes.HANDLE]
        flush_file_buffers.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL

        handle = create_file(
            parent,
            0x40000000,  # GENERIC_WRITE
            0x00000007,  # FILE_SHARE_READ | WRITE | DELETE
            None,
            3,  # OPEN_EXISTING
            0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
            None,
        )
        invalid_handle = ctypes.c_void_p(-1).value
        if not handle or int(handle) == invalid_handle:
            raise ctypes.WinError(ctypes.get_last_error())
        primary: BaseException | None = None
        try:
            if not flush_file_buffers(handle):
                raise ctypes.WinError(ctypes.get_last_error())
        except BaseException as exc:
            primary = exc
            raise
        finally:
            close_error: BaseException | None = None
            try:
                if not close_handle(handle):
                    close_error = ctypes.WinError(ctypes.get_last_error())
            except BaseException as exc:
                close_error = exc
            if close_error is not None:
                if primary is None:
                    raise close_error
                try:
                    primary.add_note(
                        "close state directory after sync failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(parent, flags)

    primary: BaseException | None = None
    try:
        os.fsync(directory_fd)
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            os.close(directory_fd)
        except BaseException as close_error:
            if primary is None:
                raise
            try:
                primary.add_note(
                    "close parent directory after fsync failure: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def atomic_save_json(path: str, data) -> bool:
    """Crash-safe JSON write: tmp + fsync(file) + atomic rename + fsync(dir).

    Never raises. Returns True when either the atomic write or the fallback
    write completed, False when persistence failed completely.

    The tmp filename includes pid + thread-id + a UUID and is opened
    exclusively so concurrent or stale generations cannot be overwritten.
    """
    # Exclusive per-call generation: PID/thread IDs can be reused and must not
    # authorize truncating or deleting a pre-existing/foreign temp generation.
    tmp = (
        f"{path}.tmp.{os.getpid()}.{threading.get_ident()}."
        f"{uuid.uuid4().hex}"
    )
    # defaults from module-level (read once from env at import)
    _max_retries = _PERSIST_RETRIES
    _retry_sleep = _PERSIST_RETRY_SLEEP
    replaced = False
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    propagating_primary: BaseException | None = None

    def temporary_generation_matches() -> bool:
        if temporary_identity is None:
            return False
        current = os.stat(tmp, follow_symlinks=False)
        return (
            stat.S_ISREG(current.st_mode)
            and not os.path.islink(tmp)
            and (current.st_dev, current.st_ino) == temporary_identity
        )
    try:
        serialized = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
    except Exception as exc:
        _log_atomic_save_failure(path, exc)
        return False
    try:
        handle = open(tmp, "x", encoding="utf-8")
        temporary_owned = True
        write_primary: BaseException | None = None
        try:
            temporary_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(temporary_stat.st_mode):
                raise ValueError("state temporary must be a regular file")
            temporary_identity = (
                temporary_stat.st_dev,
                temporary_stat.st_ino,
            )
            handle.write(serialized)
            handle.flush()
            # Position state is restart truth. A failed durability flush must
            # not be converted into a successful publish: leave the last-good
            # target untouched and let the caller fail closed.
            os.fsync(handle.fileno())
        except BaseException as exc:
            write_primary = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if write_primary is None:
                    raise
                try:
                    write_primary.add_note(
                        "close state temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass

        # Windows retry: target file may be open by the launcher's poller.
        # Retry count + sleep configurable via env vars
        # (STATE_PERSIST_RETRIES, STATE_PERSIST_RETRY_SLEEP).
        # Default 8  50ms = 400ms  enough for the launcher's typical
        # 1.5s poll-cycle to release the read handle.
        last_err = None
        for attempt in range(_max_retries):
            if attempt:
                time.sleep(_retry_sleep)
                # The name may be replaced while this writer waits. Validate
                # immediately after the wait and before authorizing another
                # replace, not only when the previous attempt failed.
                try:
                    same_generation = temporary_generation_matches()
                except FileNotFoundError:
                    same_generation = False
                except BaseException as ownership_error:
                    try:
                        last_err.add_note(
                            "state temporary ownership verification failed: "
                            f"{type(ownership_error).__name__}: "
                            f"{ownership_error}"
                        )
                    except BaseException:
                        pass
                    break
                if not same_generation:
                    break
            try:
                os.replace(tmp, path)
                replaced = True
                temporary_owned = False
                last_err = None
                break
            except PermissionError as pe:
                last_err = pe
            except Exception:
                raise
        if last_err is not None:
            raise last_err

        # POSIX: durably persist the directory entry too. Once replace has
        # published the target, a failed barrier must not be converted into a
        # fallback-write success; an identical retry can heal the barrier.
        _fsync_parent_directory(path)
        return True

    except Exception as exc:
        # Log instead of silent drop. Disk-full / permission errors otherwise
        # leave state divergent from disk indefinitely.
        _log_atomic_save_failure(path, exc)
        if replaced:
            return False
        # Independent atomic fallback for a pre-publish primary failure.
        try:
            from core.logger import save_j

            if save_j(path, data) is not True:
                return False
            # The fallback has its own atomic replace, but success is not
            # durable until that directory entry is flushed too.
            _fsync_parent_directory(path)
            return True
        except Exception as fallback_exc:
            _log_atomic_save_failure(path, fallback_exc)
            return False
    except BaseException as exc:
        propagating_primary = exc
        raise
    finally:
        # Remove only the exact regular generation opened by this call.  An
        # ambiguous replace may already have consumed it and a foreign writer
        # may have reoccupied the same source name before the exception.
        same_generation = False
        cleanup_error: BaseException | None = None
        if temporary_owned and temporary_identity is not None:
            try:
                same_generation = temporary_generation_matches()
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if same_generation:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            if propagating_primary is not None:
                try:
                    propagating_primary.add_note(
                        "state temporary cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
            else:
                # Ordinary body failures are part of this function's boolean
                # contract. Cleanup failures must be visible in silent_errors
                # but cannot replace False (or a durable fallback success).
                _log_atomic_save_failure(path, cleanup_error)


#  Generic state validator 

def _finite_float_or_none(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _valid_buy_time(value) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError):
        return False
    if parsed.strftime("%Y-%m-%d %H:%M:%S") != value:
        return False
    # ``buy_time`` is a UTC recovery anchor.  A materially future anchor can
    # hide real fills from offline-close reconstruction and suppress age-based
    # protection indefinitely.  Retain a small clock-skew allowance.
    try:
        from core.clock import now_utc

        comparison_now = now_utc().astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        comparison_now = datetime.now(timezone.utc).replace(tzinfo=None)
    return parsed <= (
        comparison_now + timedelta(minutes=5)
    )


def _normalized_margin_mode_or_none(value):
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if normalized in {"isolated", "cross"} else None


def _replace_nonfinite_values(value):
    """Heal JSON-parsed NaN/Inf telemetry without dropping the position."""
    if isinstance(value, float):
        return (value, False) if math.isfinite(value) else (None, True)
    if isinstance(value, dict):
        changed = False
        healed = {}
        for key, item in value.items():
            clean_item, item_changed = _replace_nonfinite_values(item)
            healed[key] = clean_item
            changed = changed or item_changed
        return healed, changed
    if isinstance(value, list):
        changed = False
        healed = []
        for item in value:
            clean_item, item_changed = _replace_nonfinite_values(item)
            healed.append(clean_item)
            changed = changed or item_changed
        return healed, changed
    return value, False


def _validate_state(trades: dict,
                     require_position_type: bool,
                     log_fn: Optional[Callable] = None,
                     ) -> Tuple[dict, List[str]]:
    """Shared validator for both spot and futures state.

    Required: buy (float > 0), amount (float > 0), buy_time (str).
    For futures, also: position_type  {"LONG", "SHORT"}.

    Reconstructs invalid invested_usdt and heals reconstructible optional
    price telemetry.  Present non-finite fee/funding evidence rejects the row
    instead of silently changing realized accounting to zero.

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
        if not is_canonical_position_symbol(sym):
            rejected.append(f"{sym}(symbol)")
            continue
        if not isinstance(d, dict):
            rejected.append(f"{sym}(not-dict)")
            continue
        try:
            d = copy.deepcopy(d)
        except Exception:
            rejected.append(f"{sym}(uncopyable)")
            continue

        raw_buy = d.get("buy_price")
        buy = _finite_float_or_none(raw_buy)
        if buy is None or buy <= 0:
            raw_buy = d.get("buy")
            buy = _finite_float_or_none(raw_buy)
        if buy is None:
            rejected.append(f"{sym}(buy-cast)")
            continue
        if buy <= 0:
            rejected.append(f"{sym}(buy={raw_buy})")
            continue
        if "buy_price" in d:
            d["buy_price"] = buy
        if "buy" in d:
            d["buy"] = buy

        raw_amount = d.get("amount", 0)
        amt = _finite_float_or_none(raw_amount)
        if amt is None:
            rejected.append(f"{sym}(amount-cast)")
            continue
        if amt <= 0:
            rejected.append(f"{sym}(amount={raw_amount})")
            continue
        d["amount"] = amt

        raw_original_amount = d.get("original_amount")
        if raw_original_amount is not None:
            original_amount = _finite_float_or_none(raw_original_amount)
            tolerance = max(1e-12, amt * 1e-9)
            if (
                original_amount is None
                or original_amount <= 0.0
                or original_amount + tolerance < amt
            ):
                rejected.append(f"{sym}(original_amount)")
                continue
            d["original_amount"] = original_amount

        if require_position_type and d.get("position_type") not in ("LONG", "SHORT"):
            rejected.append(f"{sym}(position_type)")
            continue

        raw_margin_mode = d.get("margin_mode")
        if raw_margin_mode is not None:
            margin_mode = _normalized_margin_mode_or_none(raw_margin_mode)
            if margin_mode is None:
                rejected.append(f"{sym}(margin_mode)")
                continue
            d["margin_mode"] = margin_mode

        if not _valid_buy_time(d.get("buy_time")):
            rejected.append(f"{sym}(buy_time)")
            continue

        if "entry_id" in d:
            try:
                entry_id = _normalized_entry_id_or_none(d.get("entry_id"))
            except (TypeError, ValueError):
                rejected.append(f"{sym}(entry_id)")
                continue
            if entry_id is None:
                d.pop("entry_id", None)
            else:
                d["entry_id"] = entry_id

        invalid_boolean = position_boolean_rejection_field(d)
        if invalid_boolean is not None:
            rejected.append(f"{sym}({invalid_boolean}-boolean)")
            continue

        invalid_pending = next(
            (
                field
                for field in _PENDING_ACCOUNTING_FIELDS
                if field in d and not _valid_pending_accounting_items(d[field])
            ),
            None,
        )
        if invalid_pending is not None:
            rejected.append(f"{sym}({invalid_pending})")
            continue

        leverage = 1.0
        raw_leverage = d.get("leverage")
        if raw_leverage is not None:
            parsed_leverage = _finite_float_or_none(raw_leverage)
            if parsed_leverage is None or parsed_leverage <= 0:
                rejected.append(f"{sym}(leverage)")
                continue
            leverage = parsed_leverage
            d["leverage"] = leverage

        raw_invested = d.get("invested_usdt")
        if raw_invested is not None:
            invested = _finite_float_or_none(raw_invested)
            if invested is None or invested < 0:
                reconstructed = buy * amt / leverage
                if not math.isfinite(reconstructed):
                    rejected.append(f"{sym}(invested-overflow)")
                    continue
                d["invested_usdt"] = reconstructed
            else:
                d["invested_usdt"] = invested

        invalid_accounting = next(
            (
                field
                for field in (
                    "fees_paid",
                    "initial_entry_fee",
                    "funding_paid",
                    "funding_booked_on_partials",
                )
                if d.get(field) is not None
                and _finite_float_or_none(d[field]) is None
            ),
            None,
        )
        if invalid_accounting is not None:
            rejected.append(f"{sym}({invalid_accounting})")
            continue

        # Heal reconstructible NaN/Inf price telemetry.
        for field in ("highest", "liquidation_price"):
            v = d.get(field)
            if v is not None:
                if _finite_float_or_none(v) is None:
                    d[field] = 0.0 if field != "highest" else buy

        d, _ = _replace_nonfinite_values(d)
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
