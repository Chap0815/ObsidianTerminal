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
import threading
import time
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
    "accounting_already_booked",
    "provisional",
    "claim_release_pending",
    "entry_sizing_recovery_pending",
    "entry_sizing_recovery_unverified",
    "entry_funding_window_unverified",
    "accounting_pending_funding_unverified",
    "oversize_rollback_pending",
    "verified_flat_pending_accounting",
    "full_exit_outcome_uncertain",
    "partial_exit_outcome_uncertain",
    "partial_tp_blocked_min_notional",
))

_POSITION_SYMBOL_MAX_LENGTH = 64
_POSITION_SYMBOL_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)
_PENDING_ACCOUNTING_FIELDS = frozenset((
    "accounting_pending_partials",
    "unpriced_external_partials",
))


def _valid_pending_accounting_items(value) -> bool:
    if isinstance(value, dict):
        return True
    return isinstance(value, list) and all(
        isinstance(item, dict) for item in value
    )


def _is_canonical_position_symbol(value) -> bool:
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


#  Atomic write 

def _log_atomic_save_failure(path: str, exc: Exception) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(f"atomic_save_json({path})", exc)
    except Exception:
        pass


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
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(serialized)
            f.flush()
            # Position state is restart truth. A failed durability flush must
            # not be converted into a successful publish: leave the last-good
            # target untouched and let the caller fail closed.
            os.fsync(f.fileno())

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
        _log_atomic_save_failure(path, exc)
        # Fallback  better non-atomic than nothing
        try:
            from core.logger import save_j
            return bool(save_j(path, data))
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
    return parsed <= (
        datetime.now(timezone.utc).replace(tzinfo=None)
        + timedelta(minutes=5)
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
        if not _is_canonical_position_symbol(sym):
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

        invalid_boolean = next(
            (
                field
                for field in _POSITION_BOOLEAN_FIELDS
                if field in d and not isinstance(d[field], bool)
            ),
            None,
        )
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

        # Heal NaN/Inf in optional numeric fields
        for field in ("highest", "fees_paid",
                       "initial_entry_fee",
                       "funding_paid", "liquidation_price"):
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
