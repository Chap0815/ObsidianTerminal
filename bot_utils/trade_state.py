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
from typing import Optional, Dict, Any, List

from bot_utils.state_persist import (atomic_save_json,
                                       validate_spot_state,
                                       validate_futures_state)

# Fields that, when changed via update()/update_many(), must be mirrored to the
# shared bot_open_positions claim row. Deliberately EXCLUDES the high-frequency
# last_price/highest fields so the monitor's per-tick updates don't hammer the
# DB  only position-defining changes (e.g. a partial sell shrinking amount/
# invested) refresh the claim.
_CLAIM_FIELDS = frozenset((
    "amount", "invested_usdt", "buy_price", "buy",
    "leverage", "position_type", "buy_time",
))

_CLAIM_NUMERIC_FIELDS = frozenset((
    "amount", "invested_usdt", "buy_price", "buy", "leverage",
))

_CLAIM_EXTRA_FIELDS = frozenset((
    "original_amount", "initial_entry_fee", "fees_paid", "funding_paid",
    "funding_booked_on_partials", "partial_sold", "break_even", "be_active",
    "be_price", "highest", "last_price", "liquidation_price",
    "initial_liq_distance", "margin_mode", "accounting_pending_partials",
    "unpriced_external_partials", "entry_id", "entry_quality_score",
    "entry_quality_label", "entry_quality_reasons", "provisional", "adopted",
))


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
    if key in ("buy", "buy_price", "amount", "leverage") and parsed <= 0:
        return f"invalid {key}={parsed!r}"
    if key == "invested_usdt" and parsed < 0:
        return f"invalid {key}={parsed!r}"
    return None


def _normalize_position_row(data: dict) -> tuple[Optional[dict], str]:
    if not isinstance(data, dict):
        return None, "not-dict"

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
    if "buy_price" in normalized:
        normalized["buy_price"] = buy_val
    if "buy" in normalized:
        normalized["buy"] = buy_val

    leverage = _finite_float_or_none(normalized.get("leverage"))
    if leverage is None or leverage <= 0:
        leverage = 1.0
    normalized["leverage"] = leverage

    invested = _finite_float_or_none(normalized.get("invested_usdt"))
    if invested is None or invested < 0:
        invested = buy_val * amount / leverage
    normalized["invested_usdt"] = invested
    return normalized, ""


def _reject_update_reason(fields: dict) -> Optional[str]:
    for key, value in fields.items():
        reason = _validate_numeric_field(key, value)
        if reason is not None:
            return reason
    return None


def normalize_pending_accounting_items(value) -> List[Dict[str, Any]]:
    """Return valid pending accounting items from legacy/corrupt state shapes."""
    if not value:
        return []
    if isinstance(value, dict):
        return [dict(value)]
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def remove_with_restore_fields(state, sym: str, fields: Dict[str, Any]) -> bool:
    """Remove state while supporting older/fake state objects in tests/tools."""
    remove = state.remove
    try:
        params = inspect.signature(remove).parameters
    except (TypeError, ValueError):
        params = {}
    supports_restore = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL for p in params.values()
    ) or len(params) >= 2
    if not supports_restore:
        result = state.remove(sym)
        return True if result is None else bool(result)
    result = remove(sym, fields)
    return True if result is None else bool(result)


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
            normalized, reason = _normalize_position_row(data)
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
            )
            if not ok:
                self._log_registry_warning(
                    f"upsert failed or blocked for {self._bot_name}:{sym}"
                )
            return bool(ok)
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

    def _registry_remove(self, sym: str) -> bool:
        if not self._bot_name:
            return True
        try:
            from core.database import remove_open_position
            ok = remove_open_position(self._bot_name, sym)
            if not ok:
                self._log_registry_warning(
                    f"remove failed for {self._bot_name}:{sym}"
                )
            return bool(ok)
        except Exception:
            self._log_registry_warning(
                f"remove raised for {self._bot_name}:{sym}"
            )
            return False

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

    def _mark_registry_pending(self, sym: str, reason: str) -> bool:
        if not self._bot_name:
            return True
        with self._lock:
            if sym not in self._trades:
                return False
            self._trades[sym]["claim_registry_pending"] = True
            self._trades[sym]["claim_registry_pending_reason"] = reason
            self._registry_retry_pending[sym] = copy.deepcopy(self._trades[sym])
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
            if not self._registry_retry_pending:
                self._registry_retry_next_at = 0.0

    def _resync_registry(self, clean: Dict[str, Dict[str, Any]]) -> None:
        """Re-claim locally loaded positions at startup.

        This intentionally does not remove registry rows absent from `clean`.
        Only exchange-aware lifecycle paths can safely decide that a claim is
        stale; deleting here can free a still-live position after a bad JSON
        load and let another bot open/adopt the same symbol.
        """
        if not self._bot_name:
            return
        conflicted = []
        pending = []
        for sym, data in clean.items():
            if self._registry_upsert_current(sym):
                continue
            try:
                from core.database import get_all_claimed_bases, _base_symbol
                claimed = get_all_claimed_bases(
                    exclude_bot=self._bot_name,
                    is_futures=self._is_futures,
                    fail_closed=True,
                )
                if claimed is None:
                    self._log_registry_warning(
                        f"startup resync kept {self._bot_name}:{sym}; "
                        f"registry unavailable; retry pending"
                    )
                    pending.append(sym)
                    self._registry_retry_pending[sym] = copy.deepcopy(data)
                    self._registry_retry_next_at = max(
                        self._registry_retry_next_at,
                        time.monotonic() + self._registry_retry_interval_sec,
                    )
                    continue
                claimed_elsewhere = _base_symbol(sym) in claimed
            except Exception:
                claimed_elsewhere = False
            if claimed_elsewhere:
                conflicted.append(sym)
                self._log_registry_warning(
                    f"startup resync kept {self._bot_name}:{sym} fail-closed; "
                    f"registry says another bot owns it"
                )
        if conflicted or pending:
            with self._lock:
                for sym in conflicted:
                    if sym in self._trades:
                        self._trades[sym]["claim_conflict"] = True
                        self._trades[sym]["claim_conflict_reason"] = (
                            "registry_owner_conflict_on_startup"
                        )
                for sym in pending:
                    if sym in self._trades:
                        self._trades[sym]["claim_registry_pending"] = True
                        self._trades[sym]["claim_registry_pending_reason"] = (
                            "registry_unavailable_on_startup"
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

    def retry_registry_pending(self, *, force: bool = True) -> int:
        """Best-effort repair for claim rows that failed during startup."""
        if not self._bot_name:
            return 0
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
        conflicted = []
        for sym in pending:
            if self._registry_upsert_current(sym):
                repaired.append(sym)
                continue
            try:
                from core.database import get_all_claimed_bases, _base_symbol
                claimed = get_all_claimed_bases(
                    exclude_bot=self._bot_name,
                    is_futures=self._is_futures,
                    fail_closed=True,
                )
                if claimed is not None and _base_symbol(sym) in claimed:
                    conflicted.append(sym)
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
                if sym in self._trades:
                    self._trades[sym].pop("claim_registry_pending", None)
                    self._trades[sym].pop("claim_registry_pending_reason", None)
                cleared_repaired.append(sym)
            for sym in conflicted:
                if (
                    self._registry_retry_generation.get(sym, 0)
                    != pending_generation.get(sym, 0)
                ):
                    continue
                self._registry_retry_pending.pop(sym, None)
                self._registry_retry_generation.pop(sym, None)
                if sym in self._trades:
                    self._trades[sym]["claim_conflict"] = True
                    self._trades[sym]["claim_conflict_reason"] = (
                        "registry_owner_conflict_after_retry"
                    )
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
            if atomic_save_json(self._db_file, snapshot):
                self._persisted_rev = rev
                return "persisted"
            return "failed"

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

    def has(self, sym: str) -> bool:
        self.retry_registry_pending(force=False)
        with self._lock:
            return sym in self._trades

    def keys(self) -> List[str]:
        self.retry_registry_pending(force=False)
        with self._lock:
            return list(self._trades.keys())

    #  Writes (snapshot under lock, save outside) 

    def add(self, sym: str, data: dict) -> bool:
        """Insert a new trade (or overwrite existing).

        DEFENSIVE: rejects entries with invalid buy/amount. If a bot's screener
        returned price=0 for some illiquid coin, the trade would otherwise be
        persisted with buy=0  launcher renders zeros forever.

        Silent rejects are also emitted on the event bus (TRADE_REJECTED) so a
        buggy screener producing amount=0 consistently is visible to the
        dashboard / Telegram instead of just silently yielding no trades.
        """
        normalized, rejection_reason = _normalize_position_row(data)
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
            _sys.stderr.write(
                f"[TradeState] REJECT add({sym}): {rejection_reason}\n"
            )
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
            self._trades[sym] = data
            rev, snapshot = self._snapshot_locked()
        status = self._persist_snapshot(rev, snapshot)
        # Claim the coin in the shared multi-bot registry (outside the lock,
        # like the JSON write). This is what makes is_claimed_by_other() work
        # so another bot won't open the SAME perp and net against us.
        registry_ok = self._registry_upsert_current(sym)
        state_ok = status in ("persisted", "stale")
        if not registry_ok and status in ("persisted", "stale"):
            registry_ok = self._mark_registry_pending(
                sym, "registry_add_failed")
        return state_ok and registry_ok

    def update(self, sym: str, key: str, value) -> bool:
        """Set one field on an existing trade. Returns False if not durable."""
        snapshot = None
        claim_row = None
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
                if key in _CLAIM_FIELDS:
                    candidate = copy.deepcopy(self._trades[sym])
                    candidate[key] = safe_value
                    normalized, reason = _normalize_position_row(candidate)
                    if normalized is None:
                        locked_reject_reason = reason
                    else:
                        self._trades[sym] = normalized
                        claim_row = copy.deepcopy(normalized)
                        rev, snapshot = self._snapshot_locked()
                else:
                    self._trades[sym][key] = safe_value
                    rev, snapshot = self._snapshot_locked()
        if locked_reject_reason is not None:
            self._log_registry_warning(
                f"update rejected invalid state for "
                f"{self._bot_name or '-'}:{sym}: {locked_reject_reason}"
            )
            return False
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            # Keep the shared claim row in sync when a claim-relevant field
            # changed (e.g. amount/invested after a partial sell). Skipping the
            # frequent last_price/highest updates avoids hammering the DB.
            registry_ok = True
            if claim_row is not None:
                registry_ok = self._registry_upsert_current(sym)
                if not registry_ok:
                    registry_ok = self._mark_registry_pending(
                        sym, "registry_update_failed")
            return status in ("persisted", "stale") and registry_ok
        return False

    def update_many(self, sym: str, fields: dict) -> bool:
        """Set multiple fields atomically; returns False if not durable."""
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
        snapshot = None
        claim_row = None
        locked_reject_reason = None
        reject_reason = _reject_update_reason(safe_fields)
        if reject_reason is not None:
            self._log_registry_warning(
                f"update_many rejected invalid numeric field for "
                f"{self._bot_name or '-'}:{sym}: {reject_reason}"
            )
            return False
        with self._lock:
            if sym in self._trades:
                if _CLAIM_FIELDS.intersection(safe_fields):
                    candidate = copy.deepcopy(self._trades[sym])
                    candidate.update(safe_fields)
                    normalized, reason = _normalize_position_row(candidate)
                    if normalized is None:
                        locked_reject_reason = reason
                    else:
                        self._trades[sym] = normalized
                        claim_row = copy.deepcopy(normalized)
                        rev, snapshot = self._snapshot_locked()
                else:
                    self._trades[sym].update(safe_fields)
                    rev, snapshot = self._snapshot_locked()
        if locked_reject_reason is not None:
            self._log_registry_warning(
                f"update_many rejected invalid state for "
                f"{self._bot_name or '-'}:{sym}: {locked_reject_reason}"
            )
            return False
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            registry_ok = True
            if claim_row is not None:
                registry_ok = self._registry_upsert_current(sym)
                if not registry_ok:
                    registry_ok = self._mark_registry_pending(
                        sym, "registry_update_failed")
            return status in ("persisted", "stale") and registry_ok
        return False

    def remove(self, sym: str, restore_fields: Optional[Dict[str, Any]] = None) -> bool:
        """Remove a trade and return whether removal was persisted.

        If JSON persistence fails, the in-memory row is restored and the shared
        claim is intentionally kept. ``restore_fields`` lets money paths mark
        a verified-flat row as already accounted so reconcile will not book it
        a second time while the state file is still locked.
        """
        safe_restore_fields = None
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
        snapshot = None
        removed = None
        with self._lock:
            if sym in self._trades:
                removed = self._trades.pop(sym)
                rev, snapshot = self._snapshot_locked()
        if snapshot is None:
            # Idempotent state removal must also heal a stale coordination
            # mirror. Serialize with add/update registry writes and re-check
            # the current row so a concurrent re-add cannot lose its claim.
            with self._registry_lock:
                with self._lock:
                    if sym in self._trades:
                        return True
                if not self._registry_remove(sym):
                    return False
                self._clear_registry_retry_if_absent(sym)
                return True
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            if status in ("persisted", "stale"):
                # A newer durable snapshot may already include this removal,
                # so stale is successful too. Do not release the claim if a
                # concurrent add has since recreated the symbol.
                with self._registry_lock:
                    with self._lock:
                        symbol_still_absent = sym not in self._trades
                    if not symbol_still_absent:
                        return True
                    # Release the claim so other bots can trade this coin.
                    if self._registry_remove(sym):
                        self._clear_registry_retry_if_absent(sym)
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
                        if safe_restore_fields:
                            removed = copy.deepcopy(removed)
                            removed.update(safe_restore_fields)
                        self._trades[sym] = removed
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

    def save_now(self) -> None:
        """Force a persist of the current state (used by emergency-close paths
        that mutated trades via direct refs without going through update())."""
        with self._lock:
            rev, snapshot = self._snapshot_locked()
        self._persist_snapshot(rev, snapshot)
