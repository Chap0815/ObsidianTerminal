"""
bot_utils/trade_state.py  Thread-safe state container for dual-thread bots
(Monitor + Scanner).

Design contract:
  All reads return DEEP COPIES  a Monitor thread iterating an entry
    while the Scan thread is mid-add would otherwise see torn state.
    Cost is small (3-5 open trades typical).
  Snapshot under the lock, save OUTSIDE the lock  holding the lock during
    the atomic save (~30ms SSD, up to 500ms on HDD or during AV scan) would
    block the Monitor and Scan threads from any state read.
"""
from __future__ import annotations

import copy
import inspect
import math
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Dict, Any, List

from bot_utils.state_persist import (atomic_save_json,
                                       _POSITION_BOOLEAN_FIELDS,
                                       _normalized_entry_id_or_none,
                                       _normalized_margin_mode_or_none,
                                       _valid_buy_time,
                                       _valid_pending_accounting_items,
                                       is_canonical_position_symbol,
                                       validate_spot_state,
                                       validate_futures_state)
from bot_utils.order_utils import order_id_text_or_none

# Fields that, when changed via update()/update_many(), must be mirrored to the
# shared bot_open_positions claim row. Deliberately EXCLUDES the high-frequency
# last_price/highest fields so the monitor's per-tick updates don't hammer the
# DB  only position- or recovery-defining changes (e.g. a partial sell
# shrinking amount/invested or clearing its accounting WAL) refresh the claim.
_CLAIM_FIELDS = frozenset((
    "amount", "invested_usdt", "buy_price", "buy",
    "leverage", "position_type", "buy_time",
    "accounting_pending_partials",
))

_CLAIM_NUMERIC_FIELDS = frozenset((
    "amount", "invested_usdt", "buy_price", "buy", "leverage",
    "original_amount",
))

_CLAIM_EXTRA_FIELDS = frozenset((
    "original_amount", "initial_entry_fee", "fees_paid", "funding_paid",
    "funding_booked_on_partials", "funding_booked_on_partials_known",
    "partial_sold", "break_even", "be_active",
    "be_price", "highest", "last_price", "liquidation_price",
    "initial_liq_distance", "margin_mode", "accounting_pending_partials",
    "unpriced_external_partials", "entry_id", "entry_quality_score",
    "entry_quality_label", "entry_quality_reasons", "provisional", "adopted",
    "claim_release_pending", "entry_intended_notional",
    "entry_contract_size", "entry_oversize_notional_ceiling",
    "entry_sizing_recovery_pending", "entry_sizing_recovery_unverified",
    "entry_claim_opened_at", "entry_funding_window_unverified",
    "accounting_pending_funding_unverified",
    "oversize_rollback_pending", "oversize_rollback_reason",
    "oversize_intended_notional", "oversize_real_notional",
))

# Every recovery-defining claim field must publish its new generation when it
# changes.  Only the two deliberately high-frequency price observations stay
# piggy-backed on the next recovery/position update instead of hammering the
# registry on every monitor tick.
_CLAIM_UPDATE_FIELDS = (
    _CLAIM_FIELDS | (_CLAIM_EXTRA_FIELDS - {"highest", "last_price"})
)


def _row_is_proven_flat_pending_accounting(row: dict) -> bool:
    if row.get("accounting_pending") is not True:
        return False
    if order_id_text_or_none(
        row.get("accounting_pending_exchange_order_id")
    ) is not None:
        return True
    sell_price = _finite_float_or_none(
        row.get("accounting_pending_sell_price")
    )
    raw_sell_time = row.get("accounting_pending_sell_time")
    try:
        parsed_sell_time = datetime.strptime(
            raw_sell_time, "%Y-%m-%d %H:%M:%S"
        )
        canonical_sell_time = (
            parsed_sell_time.strftime("%Y-%m-%d %H:%M:%S")
            == raw_sell_time
        )
    except (TypeError, ValueError, OverflowError):
        canonical_sell_time = False
    return bool(
        sell_price is not None
        and sell_price > 0.0
        and canonical_sell_time
        and isinstance(row.get("accounting_pending_reason"), str)
        and row["accounting_pending_reason"].strip()
    )


def _rows_exposure_count(rows) -> int:
    if not isinstance(rows, dict):
        return 0
    return sum(
        1
        for row in rows.values()
        if not (
            isinstance(row, dict)
            and (
                row.get("accounting_already_booked") is True
                or _row_is_proven_flat_pending_accounting(row)
            )
        )
    )


def state_rows_exposure_count(rows) -> int:
    """Return physical exposure count from one already-owned state snapshot."""
    return _rows_exposure_count(rows)


def state_exposure_count(state) -> int:
    """Return physical exposure count for TradeState and narrow test doubles."""
    counter = getattr(state, "exposure_count", None)
    if callable(counter):
        return max(0, int(counter()))
    getter = getattr(state, "get_all", None)
    if callable(getter):
        return state_rows_exposure_count(getter())
    fallback = getattr(state, "count", None)
    return max(0, int(fallback())) if callable(fallback) else 0


@contextmanager
def registry_order_guard(state, sym: str, fallback_row=None):
    """Use TradeState's ownership barrier, with a narrow test-double fallback."""
    guard = getattr(state, "registry_order_guard", None)
    if callable(guard):
        with guard(sym) as row:
            yield row
        return
    getter = getattr(state, "get", None)
    if callable(getter):
        yield getter(sym)
    else:
        yield copy.deepcopy(fallback_row)


def _finite_float(value, default: float = 0.0) -> float:
    parsed = _finite_float_or_none(value)
    return default if parsed is None else parsed


def _finite_float_or_none(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _validate_numeric_field(key: str, value) -> Optional[str]:
    if key not in _CLAIM_NUMERIC_FIELDS:
        return None
    parsed = _finite_float_or_none(value)
    if parsed is None:
        return f"invalid {key}=non-numeric"
    if key in (
        "buy", "buy_price", "amount", "leverage", "original_amount"
    ) and parsed <= 0:
        return f"invalid {key}={parsed!r}"
    if key == "invested_usdt" and parsed < 0:
        return f"invalid {key}={parsed!r}"
    return None


def _nonfinite_value_path(value, path: str = "value", seen=None) -> str | None:
    if isinstance(value, float):
        return None if math.isfinite(value) else path
    if isinstance(value, (str, bytes, bytearray, int, bool, type(None))):
        return None
    if seen is None:
        seen = set()
    if isinstance(value, dict):
        marker = id(value)
        if marker in seen:
            return f"{path}(cycle)"
        seen.add(marker)
        try:
            for key, item in value.items():
                found = _nonfinite_value_path(
                    item,
                    f"{path}.{key}",
                    seen,
                )
                if found is not None:
                    return found
        finally:
            seen.discard(marker)
    elif isinstance(value, (list, tuple)):
        marker = id(value)
        if marker in seen:
            return f"{path}(cycle)"
        seen.add(marker)
        try:
            for index, item in enumerate(value):
                found = _nonfinite_value_path(
                    item,
                    f"{path}[{index}]",
                    seen,
                )
                if found is not None:
                    return found
        finally:
            seen.discard(marker)
    return None


def _normalize_position_row(
    data: dict,
    *,
    is_futures: bool = False,
) -> tuple[Optional[dict], str]:
    if not isinstance(data, dict):
        return None, "not-dict"
    position_type = data.get("position_type")
    if is_futures and (
        not isinstance(position_type, str)
        or position_type not in ("LONG", "SHORT")
    ):
        return None, f"invalid position_type={position_type!r}"
    raw_margin_mode = data.get("margin_mode")
    if raw_margin_mode is not None:
        margin_mode = _normalized_margin_mode_or_none(raw_margin_mode)
        if margin_mode is None:
            return None, f"invalid margin_mode={raw_margin_mode!r}"
    if not _valid_buy_time(data.get("buy_time")):
        return None, f"invalid buy_time={data.get('buy_time')!r}"

    raw_buy = data.get("buy_price")
    buy_val = _finite_float_or_none(raw_buy)
    if buy_val is None or buy_val <= 0:
        raw_buy = data.get("buy")
        buy_val = _finite_float_or_none(raw_buy)
    if buy_val is None or buy_val <= 0:
        return None, "invalid buy=0.0"

    amount = _finite_float_or_none(data.get("amount"))
    if amount is None or amount <= 0:
        return None, "invalid amount=0.0"

    try:
        normalized = copy.deepcopy(data)
    except Exception:
        return None, "uncopyable-state"
    nonfinite_path = _nonfinite_value_path(normalized)
    if nonfinite_path is not None:
        return None, f"non-finite {nonfinite_path}"
    if "buy_price" in normalized:
        normalized["buy_price"] = buy_val
    if "buy" in normalized:
        normalized["buy"] = buy_val
    if raw_margin_mode is not None:
        normalized["margin_mode"] = margin_mode
    if "entry_id" in normalized:
        try:
            entry_id = _normalized_entry_id_or_none(normalized.get("entry_id"))
        except (TypeError, ValueError):
            return None, "invalid entry_id"
        if entry_id is None:
            normalized.pop("entry_id", None)
        else:
            normalized["entry_id"] = entry_id
    normalized["amount"] = amount

    raw_original_amount = normalized.get("original_amount")
    if raw_original_amount is not None:
        original_amount = _finite_float_or_none(raw_original_amount)
        tolerance = max(1e-12, amount * 1e-9)
        if (
            original_amount is None
            or original_amount <= 0.0
            or original_amount + tolerance < amount
        ):
            return None, f"invalid original_amount={raw_original_amount!r}"
        normalized["original_amount"] = original_amount

    raw_leverage = normalized.get("leverage")
    leverage = _finite_float_or_none(raw_leverage)
    if is_futures and "leverage" in normalized and (
        leverage is None or leverage <= 0
    ):
        return None, f"invalid leverage={raw_leverage!r}"
    if leverage is None or leverage <= 0:
        leverage = 1.0
    normalized["leverage"] = leverage

    invested = _finite_float_or_none(normalized.get("invested_usdt"))
    if invested is None or invested < 0:
        invested = buy_val * amount / leverage
    normalized["invested_usdt"] = invested
    return normalized, ""


def _reject_update_reason(fields: dict) -> Optional[str]:
    nonfinite_path = _nonfinite_value_path(fields)
    if nonfinite_path is not None:
        return f"non-finite {nonfinite_path}"
    for key, value in fields.items():
        if key == "entry_id":
            try:
                entry_id = _normalized_entry_id_or_none(value)
            except (TypeError, ValueError):
                return "invalid entry_id"
            if entry_id is None or entry_id != value:
                return "non-canonical entry_id"
        if key in _POSITION_BOOLEAN_FIELDS and not isinstance(value, bool):
            return f"invalid {key}=non-boolean"
        if key in (
            "accounting_pending_partials",
            "unpriced_external_partials",
        ) and not _valid_pending_accounting_items(value):
            return f"invalid {key} structure"
        reason = _validate_numeric_field(key, value)
        if reason is not None:
            return reason
    return None


def _update_still_current(
    current: Optional[dict],
    expected_row: Optional[dict],
    expected_fields: Dict[str, Any],
) -> bool:
    """Return whether an update still belongs to the current row generation."""
    if current is not expected_row or current is None:
        return False
    try:
        return all(
            key in current and current[key] == value
            for key, value in expected_fields.items()
        )
    except Exception:
        return False


def _position_generation_key(row: Any) -> Optional[tuple]:
    """Return a stable identity for one logical position generation."""
    if not isinstance(row, dict):
        return None
    try:
        entry_id = _normalized_entry_id_or_none(row.get("entry_id"))
    except (TypeError, ValueError):
        return None
    if entry_id is not None:
        return ("entry_id", entry_id)
    buy_time = row.get("buy_time")
    buy = _finite_float_or_none(
        row.get("buy_price")
        if row.get("buy_price") is not None
        else row.get("buy")
    )
    original_amount = _finite_float_or_none(row.get("original_amount"))
    if original_amount is None:
        original_amount = _finite_float_or_none(row.get("amount"))
    position_type = row.get("position_type")
    if not _valid_buy_time(buy_time) or buy is None or buy <= 0.0:
        return None
    if original_amount is None or original_amount <= 0.0:
        return None
    return (
        "legacy",
        buy_time,
        str(position_type or "").upper(),
        buy,
        original_amount,
    )


def _same_position_generation(current: Any, expected: Any) -> bool:
    current_key = _position_generation_key(current)
    expected_key = _position_generation_key(expected)
    return expected_key is not None and current_key == expected_key


def same_position_generation(current: Any, expected: Any) -> bool:
    """Public generation check for recovery paths outside this module."""
    return _same_position_generation(current, expected)


def normalize_pending_accounting_items(value) -> List[Dict[str, Any]]:
    """Return valid pending accounting items from legacy/corrupt state shapes."""
    if value is None:
        return []
    if isinstance(value, dict):
        return [dict(value)]
    if not _valid_pending_accounting_items(value):
        raise ValueError("pending accounting items have invalid structure")
    return [dict(item) for item in value]


def validated_close_accounting_mode_or_none(
    row: Dict[str, Any],
    runtime_mode_is_sim: bool,
) -> Optional[bool]:
    """Return the runtime-bound mode for a fresh close or pending replay."""
    if not isinstance(row, dict) or not isinstance(runtime_mode_is_sim, bool):
        return None
    pending = row.get("accounting_pending", False)
    if not isinstance(pending, bool):
        return None
    if not pending:
        # A stale recovery field must never redirect a new close into the
        # other accounting namespace.
        return runtime_mode_is_sim
    if "accounting_pending_mode_is_sim" not in row:
        # Legacy markers predate the explicit field. State paths are already
        # separated by runtime mode, so the active runtime is authoritative.
        return runtime_mode_is_sim
    stored_mode = row.get("accounting_pending_mode_is_sim")
    if not isinstance(stored_mode, bool) or stored_mode is not runtime_mode_is_sim:
        return None
    return stored_mode


def validated_pending_partial_accounting_item(
    value: Dict[str, Any],
    *,
    symbol: str,
    bot_name: str,
    mode_is_sim: bool,
    is_futures: bool,
) -> Dict[str, Any]:
    """Bind a pending partial DB replay to its owning runtime position."""
    item = dict(value)
    if not isinstance(mode_is_sim, bool):
        raise ValueError("runtime mode_is_sim must be boolean")
    item_mode = item.get("mode_is_sim", mode_is_sim)
    if not isinstance(item_mode, bool):
        raise ValueError("pending mode_is_sim must be boolean")
    if item_mode is not mode_is_sim:
        raise ValueError("pending mode_is_sim conflicts with runtime mode")
    if item.get("symbol") != symbol:
        raise ValueError("pending symbol conflicts with owning position")
    if item.get("bot_name") != bot_name:
        raise ValueError("pending bot_name conflicts with owning bot")
    if item.get("is_partial") is not True:
        raise ValueError("pending trade must be marked partial")
    if item.get("is_futures") is not is_futures:
        raise ValueError("pending market kind conflicts with owning bot")
    item["mode_is_sim"] = item_mode
    return item


def remove_with_restore_fields(
    state,
    sym: str,
    fields: Dict[str, Any],
    *,
    expected_row: Optional[dict] = None,
) -> bool:
    """Remove state while supporting older/fake state objects in tests/tools."""
    remove = state.remove
    try:
        params = inspect.signature(remove).parameters
    except (TypeError, ValueError):
        params = {}
    supports_restore = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL for p in params.values()
    ) or len(params) >= 2
    supports_expected = (
        "expected_row" in params
        or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
    )
    if expected_row is not None and supports_expected:
        result = remove(sym, fields, expected_row=expected_row)
        return result is None or result is True
    if expected_row is not None:
        getter = getattr(state, "get", None)
        if callable(getter):
            current = getter(sym)
            current_key = _position_generation_key(current)
            expected_key = _position_generation_key(expected_row)
            if (
                current is None
                or (
                    current_key is not None
                    and expected_key is not None
                    and current_key != expected_key
                )
            ):
                return True
    if not supports_restore:
        result = state.remove(sym)
        return result is None or result is True
    result = remove(sym, fields)
    return result is None or result is True


def update_many_if_current(
    state,
    sym: str,
    fields: Dict[str, Any],
    expected_row: dict,
) -> bool:
    """Update only the position generation represented by ``expected_row``.

    Real ``TradeState`` instances perform the check atomically.  Older test and
    tool doubles receive a conservative pre-check before their legacy method is
    called; an absent or replaced generation makes the stale update obsolete.
    """
    update_many = state.update_many
    try:
        params = inspect.signature(update_many).parameters
    except (TypeError, ValueError):
        params = {}
    supports_expected = (
        "expected_row" in params
        or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
    )
    if supports_expected:
        result = update_many(sym, fields, expected_row=expected_row)
        return result is None or result is True

    getter = getattr(state, "get", None)
    if callable(getter):
        current = getter(sym)
        current_key = _position_generation_key(current)
        expected_key = _position_generation_key(expected_row)
        if current is None or (
            current_key is not None
            and expected_key is not None
            and current_key != expected_key
        ):
            return True
    result = update_many(sym, fields)
    return result is None or result is True


def release_claim_if_absent_for_generation(
    state,
    sym: str,
    expected_row: dict,
) -> bool:
    """Release only the absent claim generation represented by a row."""
    release = state.release_claim_if_absent
    try:
        params = inspect.signature(release).parameters
    except (TypeError, ValueError):
        params = {}
    supports_expected = (
        "expected_row" in params
        or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
    )
    if supports_expected:
        result = release(sym, expected_row=expected_row)
        return result is None or result is True

    getter = getattr(state, "get", None)
    if callable(getter):
        current = getter(sym)
        if current is not None:
            if not _same_position_generation(current, expected_row):
                return True
            return False
    result = release(sym)
    return result is None or result is True


def promote_position_generation(
    state,
    sym: str,
    fields: Dict[str, Any],
    expected_row: dict,
    *,
    create_fields: Optional[Dict[str, Any]] = None,
) -> Optional[bool]:
    """Promote one entry generation without rewriting a replacement.

    ``True`` means the expected generation is current and durable, ``False``
    means its write failed, and ``None`` means another generation superseded
    the operation.  Callers must never rollback an exchange position on the
    ``None`` outcome because it may belong to that replacement generation.
    """
    has = getattr(state, "has", None)
    create_path = not (callable(has) and has(sym))
    if not create_path:
        result = update_many_if_current(state, sym, fields, expected_row)
    else:
        payload = fields if create_fields is None else create_fields
        add = state.add
        try:
            params = inspect.signature(add).parameters
        except (TypeError, ValueError):
            params = {}
        supports_if_absent = (
            "if_absent" in params
            or any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in params.values()
            )
        )
        if supports_if_absent:
            result = add(sym, payload, if_absent=True)
        else:
            result = add(sym, payload)
    getter = getattr(state, "get", None)
    if callable(getter):
        current = getter(sym)
        if current is None:
            # A failed first insert is a real durability failure.  Absence
            # after any other outcome means the generation became obsolete
            # while the write was in flight and must not trigger a rollback.
            if create_path and result is False:
                return False
            return None
        if not _same_position_generation(current, expected_row):
            return None
        if create_path and result is False:
            # The atomic create may have lost to the same generation, or its
            # first persistence attempt may have failed after memory mutation.
            # Retry as a guarded merge so monitor-written fields survive.
            update_many = getattr(state, "update_many", None)
            if callable(update_many):
                result = update_many_if_current(
                    state, sym, fields, expected_row
                )
                current = getter(sym)
                if current is None or not _same_position_generation(
                    current, expected_row
                ):
                    return None
    return result is None or result is True


def add_position_if_absent(
    state,
    sym: str,
    row: Dict[str, Any],
) -> Optional[bool]:
    """Create one recovered position without replacing concurrent state.

    ``True`` means this row was created, ``False`` means its write failed, and
    ``None`` means another position generation already exists or won the race.
    Production ``TradeState`` instances perform the absence check atomically;
    older test/tool doubles retain their legacy ``add`` contract.
    """
    add = state.add
    try:
        params = inspect.signature(add).parameters
    except (TypeError, ValueError):
        params = {}
    explicit_if_absent = "if_absent" in params
    supports_if_absent = (
        explicit_if_absent
        or any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in params.values()
        )
    )
    if supports_if_absent:
        result = add(sym, row, if_absent=True)
    else:
        result = add(sym, row)

    # TradeState reports an atomically occupied slot as ``None``.  Keep
    # legacy adapters that conventionally return None-for-success compatible
    # unless they explicitly expose the new keyword contract.
    if explicit_if_absent and result is None:
        return None

    getter = getattr(state, "get", None)
    if callable(getter):
        current = getter(sym)
        if current is None:
            return False if result is False else None
        if isinstance(current, dict):
            if not _same_position_generation(current, row):
                return None
            if result is False:
                return False
    return result is None or result is True


class TradeState:
    """Thread-safe wrapper around the trades dict + persistence.

    Parameters
    ----------
    db_file : str
        Path to trades.json on disk.
    initial : dict | None
        Pre-loaded raw state (already loaded from disk by caller). If
        None, starts empty.
    is_futures : bool
        Selects the validator: spot tolerates missing position_type,
        futures requires it.
    """

    def __init__(self,
                 db_file: str,
                 initial: Optional[dict] = None,
                 is_futures: bool = False,
                 bot_name: Optional[str] = None):
        self._db_file = db_file
        self._lock = threading.Lock()
        self._persist_lock = threading.Lock()
        # Registry callbacks/tests can synchronously trigger a newer state
        # update; reentrancy preserves ordering without self-deadlocking.
        self._registry_lock = threading.RLock()
        self._rev = 0
        self._persisted_rev = 0
        self._is_futures = is_futures
        self._registry_retry_pending: Dict[str, Dict[str, Any]] = {}
        self._registry_retry_generation: Dict[str, int] = {}
        self._registry_order_inflight: Dict[str, int] = {}
        self._registry_conflict_deferred: set[str] = set()
        self._registry_retry_interval_sec = 5.0
        self._registry_retry_next_at = 0.0
        # bot_name drives the SHARED multi-bot ownership registry
        # (bot_open_positions). When set, add()/remove() mirror the claim so
        # is_claimed_by_other() actually works (see _registry_* below).
        self._bot_name = bot_name
        # Stashed so caller can log rejections from main thread (we can't
        # log from __init__ without circular imports).
        if initial is None:
            initial = {}
        validator = validate_futures_state if is_futures else validate_spot_state
        clean, rejected = validator(initial)
        self._trades: Dict[str, Dict[str, Any]] = clean
        self.init_rejected: List[str] = rejected
        self._cleanup_orphaned_claim_release_markers(clean)
        # Startup resync: re-claim positions we just loaded. Do not delete
        # registry rows missing from local JSON here: a corrupted/lagging state
        # file can be empty while the exchange still holds a live position.
        # Exchange-aware close/reconcile paths release claims via remove().
        self._resync_registry(clean)

    #  Shared multi-bot ownership registry (bot_open_positions) 
    # The claim is written/removed at the canonical lifecycle choke-points
    # (add/remove) so EVERY path  normal open, provisional orphan write,
    # full close, emergency close, reconcile removal  keeps the registry in
    # sync from ONE place (no per-call-site patchwork).

    def _registry_upsert(self, sym: str, data: dict) -> bool:
        if not self._bot_name:
            return True
        try:
            from core.database import upsert_open_position
            normalized, reason = _normalize_position_row(
                data,
                is_futures=self._is_futures,
            )
            if normalized is None:
                self._log_registry_warning(
                    f"upsert rejected invalid numeric state for "
                    f"{self._bot_name}:{sym}: {reason}"
                )
                return False

            extra = {k: copy.deepcopy(normalized.get(k))
                     for k in _CLAIM_EXTRA_FIELDS if k in normalized}
            ok = upsert_open_position(
                bot_name=self._bot_name, symbol=sym,
                buy_price=_finite_float(
                    normalized.get("buy_price")
                    if normalized.get("buy_price") is not None
                    else normalized.get("buy")
                ),
                buy_time=normalized.get("buy_time") or "",
                amount=_finite_float(normalized.get("amount")),
                invested_usdt=_finite_float(normalized.get("invested_usdt")),
                position_type=normalized.get(
                    "position_type",
                    "FUTURES" if self._is_futures else "SPOT",
                ),
                leverage=_finite_float(normalized.get("leverage"), 1.0),
                state="OPEN",
                extra=extra,
                preserve_existing_generation=True,
            )
            if ok is not True:
                self._log_registry_warning(
                    f"upsert failed or blocked for {self._bot_name}:{sym}"
                )
            return ok is True
        except Exception:
            # Registry is a coordination mirror, never the position truth
            # (trades.json is). A failure here must never break a trade.
            self._log_registry_warning(
                f"upsert raised for {self._bot_name}:{sym}"
            )
            return False

    def _registry_upsert_current(self, sym: str) -> bool:
        """Serialize registry mirrors and upsert only the current state row."""
        if not self._bot_name:
            return True
        with self._registry_lock:
            with self._lock:
                current = self._trades.get(sym)
                data = copy.deepcopy(current) if current is not None else None
            if data is None:
                return True
            return self._registry_upsert(sym, data)

    def _registry_generation_conflict(
        self,
        sym: str,
        data: dict,
    ) -> Optional[bool]:
        """Read whether an active self-claim proves another generation."""
        if not self._bot_name:
            return False
        try:
            from core.database import open_position_claim_generation_conflicts

            return open_position_claim_generation_conflicts(
                self._bot_name,
                sym,
                data.get("position_type", "FUTURES" if self._is_futures else "SPOT"),
                data.get("entry_id"),
            )
        except Exception:
            return None

    def _registry_remove(
        self,
        sym: str,
        *,
        expected_entry_id: Optional[str] = None,
    ) -> bool:
        if not self._bot_name:
            return True
        try:
            from core.database import remove_open_position
            if expected_entry_id is None:
                ok = remove_open_position(self._bot_name, sym)
            else:
                ok = remove_open_position(
                    self._bot_name,
                    sym,
                    expected_entry_id=expected_entry_id,
                )
            if ok is not True:
                self._log_registry_warning(
                    f"remove failed for {self._bot_name}:{sym}"
                )
            return ok is True
        except Exception:
            self._log_registry_warning(
                f"remove raised for {self._bot_name}:{sym}"
            )
            return False

    def _registry_remove_expected(self, sym: str, expected_row: Any) -> bool:
        """Remove a claim generation while preserving legacy test doubles."""
        entry_id = (
            expected_row.get("entry_id")
            if isinstance(expected_row, dict)
            else None
        )
        if not isinstance(entry_id, str) or not entry_id.strip():
            return self._registry_remove(sym)
        remove = self._registry_remove
        try:
            params = inspect.signature(remove).parameters
        except (TypeError, ValueError):
            params = {}
        supports_expected = (
            "expected_entry_id" in params
            or any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in params.values()
            )
        )
        if supports_expected:
            return bool(remove(sym, expected_entry_id=entry_id.strip()))
        return bool(remove(sym))

    def _log_registry_warning(self, msg: str) -> None:
        try:
            from core.logger import log_event
            log_event(f"[TradeState] registry mirror warning: {msg}", "WARN")
        except Exception:
            try:
                from bot_utils.silent_log import silent_log
                silent_log("TradeState registry mirror", RuntimeError(msg))
            except Exception:
                pass

    def _mark_registry_pending(
        self,
        sym: str,
        reason: str,
        *,
        expected_row: Optional[dict] = None,
    ) -> bool:
        if not self._bot_name:
            return True
        with self._lock:
            current = self._trades.get(sym)
            if current is None or (
                expected_row is not None and current is not expected_row
            ):
                return False
            current["claim_registry_pending"] = True
            current["claim_registry_pending_reason"] = reason
            self._registry_retry_pending[sym] = copy.deepcopy(current)
            self._registry_retry_generation[sym] = (
                self._registry_retry_generation.get(sym, 0) + 1
            )
            self._registry_retry_next_at = max(
                self._registry_retry_next_at,
                time.monotonic() + self._registry_retry_interval_sec,
            )
            rev, snapshot = self._snapshot_locked()
        return self._persist_snapshot(rev, snapshot) in ("persisted", "stale")

    def _clear_registry_retry_if_absent(self, sym: str) -> None:
        """Drop retry metadata only after the position is still absent."""
        with self._lock:
            if sym in self._trades:
                return
            self._registry_retry_pending.pop(sym, None)
            self._registry_retry_generation.pop(sym, None)
            self._registry_conflict_deferred.discard(sym)
            if not self._registry_retry_pending:
                self._registry_retry_next_at = 0.0

    def _clear_registry_conflict_locked(self, sym: str) -> bool:
        current = self._trades.get(sym)
        if not current or current.get("claim_conflict_reason") not in {
            "registry_owner_conflict_on_startup",
            "registry_owner_conflict_after_retry",
            "registry_generation_conflict_on_startup",
            "registry_generation_conflict_after_retry",
        }:
            return False
        current.pop("claim_conflict", None)
        current.pop("claim_conflict_reason", None)
        return True

    def _clear_registry_success_locked(
        self,
        sym: str,
        expected_row: dict,
        pending_generation_at_mutation: int,
    ) -> Optional[tuple[int, dict]]:
        """Clear only retry state older than one successful mirror write."""
        if self._trades.get(sym) is not expected_row:
            return None
        newer_pending = (
            sym in self._registry_retry_pending
            and self._registry_retry_generation.get(sym, 0)
            != pending_generation_at_mutation
        )
        if newer_pending:
            return None
        self._registry_retry_pending.pop(sym, None)
        self._registry_retry_generation.pop(sym, None)
        self._registry_conflict_deferred.discard(sym)
        markers_cleared = self._clear_registry_conflict_locked(sym)
        for marker in (
            "claim_registry_pending",
            "claim_registry_pending_reason",
        ):
            if marker in expected_row:
                expected_row.pop(marker, None)
                markers_cleared = True
        if not self._registry_retry_pending:
            self._registry_retry_next_at = 0.0
        return self._snapshot_locked() if markers_cleared else None

    def _resync_registry(self, clean: Dict[str, Dict[str, Any]]) -> None:
        """Re-claim locally loaded positions at startup.

        This intentionally does not remove registry rows absent from `clean`.
        Only exchange-aware lifecycle paths can safely decide that a claim is
        stale; deleting here can free a still-live position after a bad JSON
        load and let another bot open/adopt the same symbol.
        """
        if not self._bot_name:
            return
        conflicted = {}
        pending = []
        pending_reasons = {}
        healed = []
        for sym, data in clean.items():
            if self._registry_upsert_current(sym):
                with self._lock:
                    current = self._trades.get(sym)
                    markers_cleared = self._clear_registry_conflict_locked(sym)
                    if isinstance(current, dict):
                        if "claim_registry_pending" in current:
                            current.pop("claim_registry_pending", None)
                            markers_cleared = True
                        if "claim_registry_pending_reason" in current:
                            current.pop("claim_registry_pending_reason", None)
                            markers_cleared = True
                    self._registry_retry_pending.pop(sym, None)
                    self._registry_retry_generation.pop(sym, None)
                    self._registry_conflict_deferred.discard(sym)
                    if markers_cleared:
                        healed.append(sym)
                continue
            if self._registry_generation_conflict(sym, data) is True:
                conflicted[sym] = "registry_generation_conflict_on_startup"
                self._log_registry_warning(
                    f"startup resync kept {self._bot_name}:{sym} fail-closed; "
                    "registry contains another position generation"
                )
                continue
            try:
                from core.database import get_all_claimed_bases, _base_symbol
                claimed = get_all_claimed_bases(
                    exclude_bot=self._bot_name,
                    is_futures=self._is_futures,
                    fail_closed=True,
                )
                claimed_elsewhere = (
                    _base_symbol(sym) in claimed
                    if claimed is not None
                    else False
                )
            except Exception:
                claimed = None
                claimed_elsewhere = False
            if claimed is None:
                self._log_registry_warning(
                    f"startup resync kept {self._bot_name}:{sym}; "
                    f"registry unavailable; retry pending"
                )
                pending.append(sym)
                pending_reasons[sym] = "registry_unavailable_on_startup"
                self._registry_retry_pending[sym] = copy.deepcopy(data)
                self._registry_retry_next_at = max(
                    self._registry_retry_next_at,
                    time.monotonic() + self._registry_retry_interval_sec,
                )
                continue
            if not claimed_elsewhere:
                self._log_registry_warning(
                    f"startup resync kept {self._bot_name}:{sym}; "
                    f"claim write failed and no owner was found; retry pending"
                )
                pending.append(sym)
                pending_reasons[sym] = "registry_unclaimed_on_startup"
                self._registry_retry_pending[sym] = copy.deepcopy(data)
                self._registry_retry_next_at = max(
                    self._registry_retry_next_at,
                    time.monotonic() + self._registry_retry_interval_sec,
                )
                continue
            conflicted[sym] = "registry_owner_conflict_on_startup"
            self._log_registry_warning(
                f"startup resync kept {self._bot_name}:{sym} fail-closed; "
                f"registry says another bot owns it"
            )
        if conflicted or pending or healed:
            with self._lock:
                for sym, reason in conflicted.items():
                    if sym in self._trades:
                        self._trades[sym]["claim_conflict"] = True
                        self._trades[sym]["claim_conflict_reason"] = reason
                for sym in pending:
                    if sym in self._trades:
                        self._trades[sym]["claim_registry_pending"] = True
                        self._trades[sym]["claim_registry_pending_reason"] = (
                            pending_reasons[sym]
                        )
                        self._registry_retry_pending[sym] = copy.deepcopy(
                            self._trades[sym])
                        self._registry_retry_generation[sym] = (
                            self._registry_retry_generation.get(sym, 0) + 1
                        )
                snapshot = copy.deepcopy(self._trades)
                self._rev += 1
                rev = self._rev
            self._persist_snapshot(rev, snapshot)

    def _cleanup_orphaned_claim_release_markers(
        self,
        clean: Dict[str, Dict[str, Any]],
    ) -> None:
        """Finish only explicitly staged claim releases after a JSON loss.

        An ordinary claim without local JSON may still represent a live
        exchange position and must remain for exchange-aware reconciliation.
        ``claim_release_pending`` is written to the claim before the local row
        can be deleted, so an absent local row plus this exact marker is the
        narrow durable proof that claim removal was already authorized.
        """
        if not self._bot_name:
            return
        try:
            from core.database import (
                _base_symbol,
                _strict_claim_extra_object,
                get_open_positions_db,
                remove_pending_open_position_claim,
            )

            local_bases = {
                _base_symbol(symbol) for symbol in clean if _base_symbol(symbol)
            }
            rows = get_open_positions_db(self._bot_name)
        except Exception:
            self._log_registry_warning(
                f"startup pending-release scan failed for {self._bot_name}"
            )
            return
        for row in rows:
            try:
                symbol = row.get("symbol", "")
                base = _base_symbol(symbol)
                extra = _strict_claim_extra_object(row.get("extra_json"))
            except (AttributeError, TypeError, ValueError):
                continue
            if (
                not base
                or base in local_bases
                or extra is None
                or extra.get("claim_release_pending") is not True
            ):
                continue
            with self._registry_lock:
                with self._lock:
                    current_bases = {
                        _base_symbol(current)
                        for current in self._trades
                        if _base_symbol(current)
                    }
                if base in current_bases:
                    continue
                if remove_pending_open_position_claim(
                    self._bot_name,
                    symbol,
                ) is True:
                    self._clear_registry_retry_if_absent(base)
                else:
                    self._log_registry_warning(
                        f"startup pending release kept for "
                        f"{self._bot_name}:{symbol}"
                    )

    def retry_registry_pending(self, *, force: bool = True) -> int:
        """Best-effort repair for claim rows that failed during startup."""
        if not self._bot_name:
            return 0
        with self._registry_lock:
            return self._retry_registry_pending_serialized(force=force)

    @contextmanager
    def registry_order_guard(self, sym: str):
        """Linearize a managed physical order with registry-conflict writes.

        Callers must acquire the per-symbol ``close_lock`` first and keep this
        guard only through the bounded exchange submit/recovery operation.
        Registry retries defer a conflict commit for this symbol while the
        reservation is live; unrelated symbols and state readers stay free.
        """
        # A physical exit is rare and ownership-sensitive: bypass the normal
        # read backoff so a recovered registry can publish a newly confirmed
        # foreign owner before the order linearization point.
        self.retry_registry_pending(force=True)
        with self._lock:
            current = self._trades.get(sym)
            row = copy.deepcopy(current) if current is not None else None
            conflict_deferred = sym in self._registry_conflict_deferred
            if conflict_deferred:
                if row is not None:
                    row["claim_conflict"] = True
                    row["claim_conflict_reason"] = (
                        "registry_owner_conflict_after_retry"
                    )
            else:
                self._registry_order_inflight[sym] = (
                    self._registry_order_inflight.get(sym, 0) + 1
                )
        try:
            yield row
        finally:
            if not conflict_deferred:
                with self._lock:
                    inflight = self._registry_order_inflight.get(sym, 0)
                    if inflight <= 1:
                        self._registry_order_inflight.pop(sym, None)
                        if sym in self._registry_retry_pending:
                            self._registry_retry_next_at = 0.0
                    else:
                        self._registry_order_inflight[sym] = inflight - 1

    def _retry_registry_pending_serialized(self, *, force: bool) -> int:
        """Evaluate and commit one retry generation as an atomic cycle."""
        with self._lock:
            if (
                not force
                and self._registry_retry_pending
                and time.monotonic() < self._registry_retry_next_at
            ):
                return 0
            pending = {
                sym: copy.deepcopy(self._trades.get(sym, data))
                for sym, data in self._registry_retry_pending.items()
                if sym in self._trades
            }
            pending_generation = {
                sym: self._registry_retry_generation.get(sym, 0)
                for sym in pending
            }
        if not pending:
            return 0
        next_retry_at = time.monotonic() + self._registry_retry_interval_sec
        repaired = []
        conflicted = {}
        for sym in pending:
            if self._registry_upsert_current(sym):
                repaired.append(sym)
                continue
            if self._registry_generation_conflict(sym, pending[sym]) is True:
                conflicted[sym] = "registry_generation_conflict_after_retry"
                continue
            try:
                from core.database import get_all_claimed_bases, _base_symbol
                claimed = get_all_claimed_bases(
                    exclude_bot=self._bot_name,
                    is_futures=self._is_futures,
                    fail_closed=True,
                )
                if claimed is not None and _base_symbol(sym) in claimed:
                    conflicted[sym] = "registry_owner_conflict_after_retry"
            except Exception:
                pass
        if not repaired and not conflicted:
            with self._lock:
                if self._registry_retry_pending:
                    self._registry_retry_next_at = max(
                        self._registry_retry_next_at, next_retry_at)
            return 0
        with self._lock:
            cleared_repaired = []
            for sym in repaired:
                if (
                    self._registry_retry_generation.get(sym, 0)
                    != pending_generation.get(sym, 0)
                ):
                    continue
                self._registry_retry_pending.pop(sym, None)
                self._registry_retry_generation.pop(sym, None)
                self._registry_conflict_deferred.discard(sym)
                if sym in self._trades:
                    self._trades[sym].pop("claim_registry_pending", None)
                    self._trades[sym].pop("claim_registry_pending_reason", None)
                    self._clear_registry_conflict_locked(sym)
                cleared_repaired.append(sym)
            for sym, conflict_reason in conflicted.items():
                # Foreign ownership or a mismatched self-generation is durable
                # conflict evidence.  If a newer failed mirror write advanced
                # the retry generation after the DB query, the evidence still
                # applies. Only a proven repair or row removal may clear it.
                if (
                    sym not in self._trades
                    or sym not in self._registry_retry_pending
                ):
                    self._registry_conflict_deferred.discard(sym)
                    continue
                # The order reservation and this commit both linearize under
                # _lock.  If submit won the race, retain the pending generation
                # and publish the confirmed conflict immediately after submit.
                if self._registry_order_inflight.get(sym, 0) > 0:
                    self._registry_conflict_deferred.add(sym)
                    continue
                self._registry_retry_pending.pop(sym, None)
                self._registry_retry_generation.pop(sym, None)
                self._registry_conflict_deferred.discard(sym)
                if sym in self._trades:
                    self._trades[sym]["claim_conflict"] = True
                    self._trades[sym]["claim_conflict_reason"] = conflict_reason
                    self._trades[sym].pop("claim_registry_pending", None)
                    self._trades[sym].pop("claim_registry_pending_reason", None)
            self._registry_retry_next_at = (
                0.0 if not self._registry_retry_pending else next_retry_at
            )
            rev, snapshot = self._snapshot_locked()
        self._persist_snapshot(rev, snapshot)
        return len(cleared_repaired)

    #  Reads (always return deep copies) 

    def _snapshot_locked(self) -> tuple[int, dict]:
        self._rev += 1
        return self._rev, copy.deepcopy(self._trades)

    def _persist_snapshot(self, rev: int, snapshot: dict) -> str:
        with self._persist_lock:
            if rev < self._persisted_rev:
                return "stale"
            if atomic_save_json(self._db_file, snapshot) is True:
                self._persisted_rev = rev
                return "persisted"
            return "failed"

    def flush_pending(self) -> bool:
        """Retry the latest full restart snapshot when a prior write failed."""
        with self._lock:
            rev = self._rev
            if rev <= self._persisted_rev:
                return True
            snapshot = copy.deepcopy(self._trades)
        return self._persist_snapshot(rev, snapshot) in ("persisted", "stale")

    def finalize_pending(self) -> bool:
        """Finalize durable JSON plus any unresolved shared claim generation."""
        registry_ok = True
        if self._bot_name:
            try:
                self.retry_registry_pending(force=True)
            except Exception as exc:
                self._log_registry_warning(
                    f"final registry retry raised for {self._bot_name}: "
                    f"{type(exc).__name__}"
                )
                registry_ok = False
            with self._lock:
                registry_ok = registry_ok and not bool(
                    self._registry_retry_pending
                    or self._registry_conflict_deferred
                    or self._registry_order_inflight
                )
        state_ok = self.flush_pending()
        return registry_ok and state_ok

    def get_all(self) -> dict:
        """Deep-copy snapshot of all trades  safe to iterate without lock."""
        self.retry_registry_pending(force=False)
        with self._lock:
            return copy.deepcopy(self._trades)

    def get(self, sym: str) -> Optional[dict]:
        """Deep-copy of one trade, or None."""
        self.retry_registry_pending(force=False)
        with self._lock:
            if sym not in self._trades:
                return None
            return copy.deepcopy(self._trades[sym])

    def count(self) -> int:
        self.retry_registry_pending(force=False)
        with self._lock:
            return len(self._trades)

    def exposure_count(self) -> int:
        """Count positions that can still represent exchange exposure.

        A full-close accounting WAL remains in ``trades.json`` until its DB
        row is durable, but ``accounting_pending=True`` is written only after
        the exchange close was verified flat. Keeping that recovery evidence
        must not consume a live strategy slot.
        """
        self.retry_registry_pending(force=False)
        with self._lock:
            return _rows_exposure_count(self._trades)

    def has(self, sym: str) -> bool:
        self.retry_registry_pending(force=False)
        with self._lock:
            return sym in self._trades

    def keys(self) -> List[str]:
        self.retry_registry_pending(force=False)
        with self._lock:
            return list(self._trades.keys())

    #  Writes (snapshot under lock, save outside) 

    def add(
        self,
        sym: str,
        data: dict,
        *,
        if_absent: bool = False,
    ) -> Optional[bool]:
        """Insert a new trade (or overwrite existing).

        With ``if_absent=True``, an already occupied symbol returns ``None``
        without mutation. Normal validation or durability failures return
        ``False``; callers can therefore distinguish contention from failure.

        DEFENSIVE: rejects entries with invalid buy/amount. If a bot's screener
        returned price=0 for some illiquid coin, the trade would otherwise be
        persisted with buy=0  launcher renders zeros forever.

        Silent rejects are also emitted on the event bus (TRADE_REJECTED) so a
        buggy screener producing amount=0 consistently is visible to the
        dashboard / Telegram instead of just silently yielding no trades.
        """
        if is_canonical_position_symbol(sym):
            normalized, rejection_reason = _normalize_position_row(
                data,
                is_futures=self._is_futures,
            )
        else:
            normalized, rejection_reason = None, "symbol"
        if normalized is not None:
            data = normalized
            rejection_reason = None
        source = data if isinstance(data, dict) else {}
        buy_val = _finite_float(
            source.get("buy_price")
            if source.get("buy_price") is not None
            else source.get("buy")
        )
        amt_val = _finite_float(source.get("amount"))

        if rejection_reason is not None:
            import sys as _sys
            try:
                stderr = _sys.stderr
                if stderr is not None:
                    stderr.write(
                        f"[TradeState] REJECT add({sym}): {rejection_reason}\n"
                    )
            except Exception:
                # The launcher owns the bot's stderr pipe. Losing that UI
                # consumer must never turn a state rejection into a worker
                # exception or change the fail-closed return value below.
                pass
            # surface the reject on the event bus so the dashboard /
            # logger / Telegram-alert can react. Wrapped in try/except so
            # the rejection still happens even if the event bus is down.
            try:
                from core.event_bus import get_bus
                get_bus().emit("TRADE_REJECTED", {
                    "symbol": sym,
                    "reason": rejection_reason,
                    "buy":    buy_val,
                    "amount": amt_val,
                    "source": "TradeState.add",
                })
            except Exception:
                pass
            return False

        with self._lock:
            if if_absent and sym in self._trades:
                return None
            self._trades[sym] = data
            inserted_row = self._trades[sym]
            pending_generation_at_insert = self._registry_retry_generation.get(
                sym, 0
            )
            rev, snapshot = self._snapshot_locked()
        status = self._persist_snapshot(rev, snapshot)
        state_ok = status in ("persisted", "stale")
        if not state_ok:
            # Never publish a claim generation that the restart truth does not
            # contain. The caller can clean up the in-memory candidate after
            # the failed add, while the durable registry remains unchanged.
            return False
        # Publish only a generation that still exists after the durable JSON
        # write. A concurrent remove can win while persistence is in flight;
        # returning True for that now-absent row would let pre-order callers
        # open a position without either local state or a registry claim.
        # Keep the registry lock through the presence check and mirror write so
        # remove() cannot cross that publication boundary.
        with self._registry_lock:
            with self._lock:
                current = self._trades.get(sym)
                if current is not inserted_row:
                    return False
                expected_row = inserted_row
                claim_data = (
                    copy.deepcopy(current) if current is not None else None
                )
            if claim_data is None:
                return False
            registry_mirrored = self._registry_upsert(sym, claim_data)
            registry_ok = registry_mirrored
            if not registry_mirrored:
                registry_ok = self._mark_registry_pending(
                    sym,
                    "registry_add_failed",
                    expected_row=expected_row,
                )
            clear_snapshot = None
            with self._lock:
                if self._trades.get(sym) is not expected_row:
                    return False
                if registry_mirrored:
                    cleared = self._clear_registry_success_locked(
                        sym,
                        expected_row,
                        pending_generation_at_insert,
                    )
                    if cleared is not None:
                        clear_rev, clear_snapshot = cleared
            if clear_snapshot is not None:
                if self._persist_snapshot(
                    clear_rev, clear_snapshot
                ) == "failed":
                    self._log_registry_warning(
                        f"add could not persist cleared registry markers for "
                        f"{self._bot_name}:{sym}"
                    )
        return state_ok and registry_ok

    def update(self, sym: str, key: str, value) -> bool:
        """Set one field on an existing trade. Returns False if not durable."""
        snapshot = None
        claim_row = None
        expected_row = None
        expected_fields = {}
        pending_generation_at_mutation = 0
        locked_reject_reason = None
        try:
            safe_value = copy.deepcopy(value)
        except Exception:
            self._log_registry_warning(
                f"update rejected uncopyable value for "
                f"{self._bot_name or '-'}:{sym}:{key}"
            )
            return False
        reject_reason = _reject_update_reason({key: safe_value})
        if reject_reason is not None:
            self._log_registry_warning(
                f"update rejected invalid numeric field for "
                f"{self._bot_name or '-'}:{sym}: {reject_reason}"
            )
            return False
        with self._lock:
            if sym in self._trades:
                if key in _CLAIM_UPDATE_FIELDS:
                    candidate = copy.deepcopy(self._trades[sym])
                    candidate[key] = safe_value
                    normalized, reason = _normalize_position_row(
                        candidate,
                        is_futures=self._is_futures,
                    )
                    if normalized is None:
                        locked_reject_reason = reason
                    else:
                        self._trades[sym] = normalized
                        claim_row = copy.deepcopy(normalized)
                        expected_row = self._trades[sym]
                        pending_generation_at_mutation = (
                            self._registry_retry_generation.get(sym, 0)
                        )
                        expected_fields = {key: copy.deepcopy(normalized[key])}
                        rev, snapshot = self._snapshot_locked()
                else:
                    self._trades[sym][key] = safe_value
                    expected_row = self._trades[sym]
                    expected_fields = {
                        key: copy.deepcopy(self._trades[sym][key])
                    }
                    rev, snapshot = self._snapshot_locked()
        if locked_reject_reason is not None:
            self._log_registry_warning(
                f"update rejected invalid state for "
                f"{self._bot_name or '-'}:{sym}: {locked_reject_reason}"
            )
            return False
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            state_ok = status in ("persisted", "stale")
            if not state_ok:
                return False
            # Keep the shared claim row in sync when a claim-relevant field
            # changed (e.g. amount/invested after a partial sell). Skipping the
            # frequent last_price/highest updates avoids hammering the DB.
            registry_ok = True
            if claim_row is not None:
                with self._registry_lock:
                    with self._lock:
                        current = self._trades.get(sym)
                        if not _update_still_current(
                            current, expected_row, expected_fields
                        ):
                            return False
                        claim_data = copy.deepcopy(current)
                    registry_mirrored = self._registry_upsert(sym, claim_data)
                    registry_ok = registry_mirrored
                    if not registry_mirrored:
                        registry_ok = self._mark_registry_pending(
                            sym,
                            "registry_update_failed",
                            expected_row=expected_row,
                        )
                    clear_snapshot = None
                    with self._lock:
                        if not _update_still_current(
                            self._trades.get(sym),
                            expected_row,
                            expected_fields,
                        ):
                            return False
                        if registry_mirrored:
                            cleared = self._clear_registry_success_locked(
                                sym,
                                expected_row,
                                pending_generation_at_mutation,
                            )
                            if cleared is not None:
                                clear_rev, clear_snapshot = cleared
                    if clear_snapshot is not None:
                        if self._persist_snapshot(
                            clear_rev, clear_snapshot
                        ) == "failed":
                            self._log_registry_warning(
                                f"update could not persist cleared registry "
                                f"markers for {self._bot_name}:{sym}"
                            )
            else:
                with self._lock:
                    if not _update_still_current(
                        self._trades.get(sym), expected_row, expected_fields
                    ):
                        return False
            return registry_ok
        return False

    def update_many(
        self,
        sym: str,
        fields: dict,
        *,
        expected_row: Optional[dict] = None,
    ) -> bool:
        """Set multiple fields atomically; returns False if not durable.

        When ``expected_row`` is supplied, an absent or replaced position makes
        the stale operation an idempotent no-op instead of modifying the new
        generation.
        """
        if not isinstance(fields, dict):
            self._log_registry_warning(
                f"update_many rejected non-dict fields for "
                f"{self._bot_name or '-'}:{sym}"
            )
            return False
        try:
            safe_fields = copy.deepcopy(fields)
        except Exception:
            self._log_registry_warning(
                f"update_many rejected uncopyable fields for "
                f"{self._bot_name or '-'}:{sym}"
            )
            return False
        safe_expected_row = None
        if expected_row is not None:
            try:
                safe_expected_row = copy.deepcopy(expected_row)
            except Exception:
                self._log_registry_warning(
                    f"update_many rejected uncopyable expected generation for "
                    f"{self._bot_name or '-'}:{sym}"
                )
                return False
            if _position_generation_key(safe_expected_row) is None:
                self._log_registry_warning(
                    f"update_many rejected invalid expected generation for "
                    f"{self._bot_name or '-'}:{sym}"
                )
                return False
        snapshot = None
        claim_row = None
        mutation_row = None
        expected_fields = {}
        pending_generation_at_mutation = 0
        locked_reject_reason = None
        generation_obsolete = False
        reject_reason = _reject_update_reason(safe_fields)
        if reject_reason is not None:
            self._log_registry_warning(
                f"update_many rejected invalid numeric field for "
                f"{self._bot_name or '-'}:{sym}: {reject_reason}"
            )
            return False
        with self._lock:
            current_row = self._trades.get(sym)
            if safe_expected_row is not None and not _same_position_generation(
                current_row, safe_expected_row
            ):
                generation_obsolete = True
            elif current_row is not None:
                if _CLAIM_UPDATE_FIELDS.intersection(safe_fields):
                    candidate = copy.deepcopy(current_row)
                    candidate.update(safe_fields)
                    normalized, reason = _normalize_position_row(
                        candidate,
                        is_futures=self._is_futures,
                    )
                    if normalized is None:
                        locked_reject_reason = reason
                    else:
                        self._trades[sym] = normalized
                        claim_row = copy.deepcopy(normalized)
                        mutation_row = self._trades[sym]
                        pending_generation_at_mutation = (
                            self._registry_retry_generation.get(sym, 0)
                        )
                        expected_fields = {
                            key: copy.deepcopy(normalized[key])
                            for key in safe_fields
                        }
                        rev, snapshot = self._snapshot_locked()
                else:
                    current_row.update(safe_fields)
                    mutation_row = current_row
                    expected_fields = {
                        key: copy.deepcopy(current_row[key])
                        for key in safe_fields
                    }
                    rev, snapshot = self._snapshot_locked()
        if generation_obsolete:
            return True
        if locked_reject_reason is not None:
            self._log_registry_warning(
                f"update_many rejected invalid state for "
                f"{self._bot_name or '-'}:{sym}: {locked_reject_reason}"
            )
            return False
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            state_ok = status in ("persisted", "stale")
            if not state_ok:
                return False
            registry_ok = True
            if claim_row is not None:
                with self._registry_lock:
                    with self._lock:
                        current = self._trades.get(sym)
                        if not _update_still_current(
                            current, mutation_row, expected_fields
                        ):
                            return False
                        claim_data = copy.deepcopy(current)
                    registry_mirrored = self._registry_upsert(sym, claim_data)
                    registry_ok = registry_mirrored
                    if not registry_mirrored:
                        registry_ok = self._mark_registry_pending(
                            sym,
                            "registry_update_failed",
                            expected_row=mutation_row,
                        )
                    clear_snapshot = None
                    with self._lock:
                        if not _update_still_current(
                            self._trades.get(sym),
                            mutation_row,
                            expected_fields,
                        ):
                            return False
                        if registry_mirrored:
                            cleared = self._clear_registry_success_locked(
                                sym,
                                mutation_row,
                                pending_generation_at_mutation,
                            )
                            if cleared is not None:
                                clear_rev, clear_snapshot = cleared
                    if clear_snapshot is not None:
                        if self._persist_snapshot(
                            clear_rev, clear_snapshot
                        ) == "failed":
                            self._log_registry_warning(
                                f"update_many could not persist cleared "
                                f"registry markers for {self._bot_name}:{sym}"
                            )
            else:
                with self._lock:
                    if not _update_still_current(
                        self._trades.get(sym), mutation_row, expected_fields
                    ):
                        return False
            return registry_ok
        return False

    def release_claim_if_absent(
        self,
        sym: str,
        *,
        expected_row: Optional[dict] = None,
    ) -> bool:
        """Release only a stale registry claim, never a concurrent state row.

        Entry rollback cleanup can observe an already-absent row after a
        transient registry failure. Hold both coordination locks across the
        final absence check and registry delete so a concurrent ``add`` cannot
        be mistaken for the stale generation being cleaned up.
        """
        safe_expected_row = None
        if expected_row is not None:
            try:
                safe_expected_row = copy.deepcopy(expected_row)
            except Exception:
                return False
            if _position_generation_key(safe_expected_row) is None:
                return False
        with self._registry_lock:
            with self._lock:
                current = self._trades.get(sym)
                if (
                    safe_expected_row is not None
                    and current is not None
                    and not _same_position_generation(current, safe_expected_row)
                ):
                    return True
                if current is not None:
                    return False
                if safe_expected_row is None:
                    removed = self._registry_remove(sym)
                else:
                    removed = self._registry_remove_expected(
                        sym,
                        safe_expected_row,
                    )
                if not removed:
                    return False
                self._registry_retry_pending.pop(sym, None)
                self._registry_retry_generation.pop(sym, None)
                self._registry_conflict_deferred.discard(sym)
                if not self._registry_retry_pending:
                    self._registry_retry_next_at = 0.0
                return True

    def remove(
        self,
        sym: str,
        restore_fields: Optional[Dict[str, Any]] = None,
        *,
        expected_row: Optional[dict] = None,
    ) -> bool:
        """Remove a trade and return whether removal was persisted.

        If JSON persistence fails, the in-memory row is restored and the shared
        claim is intentionally kept. ``restore_fields`` lets money paths mark
        a verified-flat row as already accounted so reconcile will not book it
        a second time while the state file is still locked.
        """
        safe_restore_fields = None
        safe_expected_row = None
        if restore_fields is not None:
            if not isinstance(restore_fields, dict):
                self._log_registry_warning(
                    f"remove rejected non-dict restore fields for "
                    f"{self._bot_name or '-'}:{sym}"
                )
                return False
            try:
                safe_restore_fields = copy.deepcopy(restore_fields)
            except Exception:
                self._log_registry_warning(
                    f"remove rejected uncopyable restore fields for "
                    f"{self._bot_name or '-'}:{sym}"
                )
                return False
        if expected_row is not None:
            try:
                safe_expected_row = copy.deepcopy(expected_row)
            except Exception:
                self._log_registry_warning(
                    f"remove rejected uncopyable expected generation for "
                    f"{self._bot_name or '-'}:{sym}"
                )
                return False
            if _position_generation_key(safe_expected_row) is None:
                self._log_registry_warning(
                    f"remove rejected invalid expected generation for "
                    f"{self._bot_name or '-'}:{sym}"
                )
                return False
        with self._registry_lock:
            marker_snapshot = None
            removed = None
            with self._lock:
                current = self._trades.get(sym)
                if (
                    safe_expected_row is not None
                    and current is not None
                    and not _same_position_generation(current, safe_expected_row)
                ):
                    # A newer position generation owns this symbol now.
                    return True
                if current is None:
                    state_absent = True
                else:
                    state_absent = False
                    if self._bot_name:
                        staged = copy.deepcopy(self._trades[sym])
                        if safe_restore_fields:
                            staged.update(safe_restore_fields)
                        staged["claim_release_pending"] = True
                        self._trades[sym] = staged
                        marker_expected = copy.deepcopy(staged)
                        marker_rev, marker_snapshot = self._snapshot_locked()

            if state_absent:
                # Idempotent state removal must also heal a stale coordination
                # mirror. Keep the state lock across the registry delete: add()
                # publishes its local generation before waiting for the registry
                # lock, so a check-then-delete gap here could delete that newer
                # generation's claim.
                with self._lock:
                    if sym in self._trades:
                        return True
                    if safe_expected_row is None:
                        registry_removed = self._registry_remove(sym)
                    else:
                        registry_removed = self._registry_remove_expected(
                            sym,
                            safe_expected_row,
                        )
                    if not registry_removed:
                        return False
                    self._registry_retry_pending.pop(sym, None)
                    self._registry_retry_generation.pop(sym, None)
                    self._registry_conflict_deferred.discard(sym)
                    if not self._registry_retry_pending:
                        self._registry_retry_next_at = 0.0
                    return True

            if marker_snapshot is not None:
                marker_status = self._persist_snapshot(
                    marker_rev,
                    marker_snapshot,
                )
                if marker_status == "failed":
                    self._log_registry_warning(
                        f"remove could not persist pending release for "
                        f"{self._bot_name}:{sym}"
                    )
                    return False
                with self._lock:
                    current = self._trades.get(sym)
                    if current != marker_expected:
                        if (
                            isinstance(current, dict)
                            and current.get("claim_release_pending") is not True
                        ):
                            # A concurrent add replaced the staged generation.
                            # The requested row is gone, but the new row and its
                            # claim must remain untouched.
                            return True
                        self._log_registry_warning(
                            f"remove deferred after concurrent state change for "
                            f"{self._bot_name}:{sym}"
                        )
                        return False
                    if not self._registry_upsert(sym, marker_expected):
                        self._log_registry_warning(
                            f"remove could not stage durable claim release for "
                            f"{self._bot_name}:{sym}"
                        )
                        return False
                    removed = self._trades.pop(sym)
                    rev, snapshot = self._snapshot_locked()
            else:
                with self._lock:
                    if sym in self._trades:
                        removed = self._trades.pop(sym)
                        rev, snapshot = self._snapshot_locked()
                    else:
                        return True

            status = self._persist_snapshot(rev, snapshot)
            if status in ("persisted", "stale"):
                # A newer durable snapshot may already include this removal,
                # so stale is successful too. Do not release the claim if a
                # concurrent add has since recreated the symbol. Keep the
                # state lock across the registry delete for the same reason as
                # the idempotent path above: the absence check and delete must
                # describe one local generation boundary.
                with self._lock:
                    if sym in self._trades:
                        return True
                    # Release the claim so other bots can trade this coin.
                    if self._registry_remove_expected(sym, removed):
                        self._registry_retry_pending.pop(sym, None)
                        self._registry_retry_generation.pop(sym, None)
                        self._registry_conflict_deferred.discard(sym)
                        if not self._registry_retry_pending:
                            self._registry_retry_next_at = 0.0
                        return True
                with self._lock:
                    restore_snapshot = None
                    if sym not in self._trades and removed is not None:
                        restored = copy.deepcopy(removed)
                        if safe_restore_fields:
                            restored.update(safe_restore_fields)
                        restored["claim_release_pending"] = True
                        self._trades[sym] = restored
                        restore_rev, restore_snapshot = (
                            self._snapshot_locked()
                        )
                if restore_snapshot is not None:
                    self._persist_snapshot(restore_rev, restore_snapshot)
                try:
                    from bot_utils.silent_log import silent_log
                    silent_log(
                        f"TradeState.remove({self._bot_name or '-'}:{sym})",
                        RuntimeError("claim release failed; state restored"),
                    )
                except Exception:
                    pass
                return False
            elif status == "failed":
                with self._lock:
                    if sym not in self._trades and removed is not None:
                        restored = copy.deepcopy(removed)
                        if safe_restore_fields:
                            restored.update(safe_restore_fields)
                        self._trades[sym] = restored
                try:
                    from bot_utils.silent_log import silent_log
                    silent_log(
                        f"TradeState.remove({self._bot_name or '-'}:{sym})",
                        RuntimeError("state persist failed; claim kept"),
                    )
                except Exception:
                    pass
                return False
        return True

    def save_now(self) -> bool:
        """Force a persist of the current state (used by emergency-close paths
        that mutated trades via direct refs without going through update())."""
        with self._lock:
            rev, snapshot = self._snapshot_locked()
        return self._persist_snapshot(rev, snapshot) in ("persisted", "stale")
