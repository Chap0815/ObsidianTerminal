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
import threading
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

_CLAIM_EXTRA_FIELDS = frozenset((
    "original_amount", "initial_entry_fee", "fees_paid", "funding_paid",
    "funding_booked_on_partials", "partial_sold", "break_even", "be_active",
    "be_price", "highest", "last_price", "liquidation_price",
    "initial_liq_distance", "margin_mode", "accounting_pending_partials",
    "unpriced_external_partials",
))


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
        self._rev = 0
        self._persisted_rev = 0
        self._is_futures = is_futures
        self._registry_retry_pending: Dict[str, Dict[str, Any]] = {}
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
            extra = {k: copy.deepcopy(data.get(k))
                     for k in _CLAIM_EXTRA_FIELDS if k in data}
            ok = upsert_open_position(
                bot_name=self._bot_name, symbol=sym,
                buy_price=float(data.get("buy_price") or data.get("buy") or 0),
                buy_time=data.get("buy_time") or "",
                amount=float(data.get("amount") or 0),
                invested_usdt=float(data.get("invested_usdt") or 0),
                position_type=data.get("position_type",
                                       "FUTURES" if self._is_futures else "SPOT"),
                leverage=float(data.get("leverage") or 1),
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
            if self._registry_upsert(sym, data):
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
                snapshot = copy.deepcopy(self._trades)
                self._rev += 1
                rev = self._rev
            self._persist_snapshot(rev, snapshot)

    def retry_registry_pending(self) -> int:
        """Best-effort repair for claim rows that failed during startup."""
        if not self._bot_name:
            return 0
        with self._lock:
            pending = {
                sym: copy.deepcopy(self._trades.get(sym, data))
                for sym, data in self._registry_retry_pending.items()
                if sym in self._trades
            }
        if not pending:
            return 0
        repaired = []
        conflicted = []
        for sym, data in pending.items():
            if self._registry_upsert(sym, data):
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
            return 0
        with self._lock:
            for sym in repaired:
                self._registry_retry_pending.pop(sym, None)
                if sym in self._trades:
                    self._trades[sym].pop("claim_registry_pending", None)
                    self._trades[sym].pop("claim_registry_pending_reason", None)
            for sym in conflicted:
                self._registry_retry_pending.pop(sym, None)
                if sym in self._trades:
                    self._trades[sym]["claim_conflict"] = True
                    self._trades[sym]["claim_conflict_reason"] = (
                        "registry_owner_conflict_after_retry"
                    )
                    self._trades[sym].pop("claim_registry_pending", None)
                    self._trades[sym].pop("claim_registry_pending_reason", None)
            rev, snapshot = self._snapshot_locked()
        self._persist_snapshot(rev, snapshot)
        return len(repaired)

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
        self.retry_registry_pending()
        with self._lock:
            return copy.deepcopy(self._trades)

    def get(self, sym: str) -> Optional[dict]:
        """Deep-copy of one trade, or None."""
        self.retry_registry_pending()
        with self._lock:
            if sym not in self._trades:
                return None
            return copy.deepcopy(self._trades[sym])

    def count(self) -> int:
        self.retry_registry_pending()
        with self._lock:
            return len(self._trades)

    def has(self, sym: str) -> bool:
        self.retry_registry_pending()
        with self._lock:
            return sym in self._trades

    def keys(self) -> List[str]:
        self.retry_registry_pending()
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
        rejection_reason = None
        try:
            import math as _math
            buy_val = float(data.get("buy_price") or data.get("buy") or 0)
            amt_val = float(data.get("amount") or 0)
            if not (_math.isfinite(buy_val) and buy_val > 0):
                rejection_reason = f"invalid buy={buy_val!r}"
            elif not (_math.isfinite(amt_val) and amt_val > 0):
                rejection_reason = f"invalid amount={amt_val!r}"
        except (TypeError, ValueError):
            rejection_reason = "non-numeric buy/amount"
            buy_val = amt_val = 0.0

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
        registry_ok = self._registry_upsert(sym, data)
        return status in ("persisted", "stale") and registry_ok

    def update(self, sym: str, key: str, value) -> bool:
        """Set one field on an existing trade. Returns False if not durable."""
        snapshot = None
        claim_row = None
        with self._lock:
            if sym in self._trades:
                self._trades[sym][key] = value
                rev, snapshot = self._snapshot_locked()
                if key in _CLAIM_FIELDS:
                    claim_row = copy.deepcopy(self._trades[sym])
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            # Keep the shared claim row in sync when a claim-relevant field
            # changed (e.g. amount/invested after a partial sell). Skipping the
            # frequent last_price/highest updates avoids hammering the DB.
            registry_ok = True
            if claim_row is not None:
                registry_ok = self._registry_upsert(sym, claim_row)
            return status in ("persisted", "stale") and registry_ok
        return False

    def update_many(self, sym: str, fields: dict) -> bool:
        """Set multiple fields atomically; returns False if not durable."""
        snapshot = None
        claim_row = None
        with self._lock:
            if sym in self._trades:
                self._trades[sym].update(fields)
                rev, snapshot = self._snapshot_locked()
                if _CLAIM_FIELDS.intersection(fields):
                    claim_row = copy.deepcopy(self._trades[sym])
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            registry_ok = True
            if claim_row is not None:
                registry_ok = self._registry_upsert(sym, claim_row)
            return status in ("persisted", "stale") and registry_ok
        return False

    def remove(self, sym: str, restore_fields: Optional[Dict[str, Any]] = None) -> bool:
        """Remove a trade and return whether removal was persisted.

        If JSON persistence fails, the in-memory row is restored and the shared
        claim is intentionally kept. ``restore_fields`` lets money paths mark
        a verified-flat row as already accounted so reconcile will not book it
        a second time while the state file is still locked.
        """
        snapshot = None
        removed = None
        with self._lock:
            if sym in self._trades:
                removed = self._trades.pop(sym)
                rev, snapshot = self._snapshot_locked()
        if snapshot is not None:
            status = self._persist_snapshot(rev, snapshot)
            if status == "persisted":
                # Release the claim so other bots can trade this coin again.
                if self._registry_remove(sym):
                    return True
                with self._lock:
                    restore_snapshot = None
                    if sym not in self._trades and removed is not None:
                        restored = copy.deepcopy(removed)
                        if restore_fields:
                            restored.update(restore_fields)
                        restored["claim_release_pending"] = True
                        self._trades[sym] = restored
                        restore_rev, restore_snapshot = self._snapshot_locked()
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
                        if restore_fields:
                            removed = copy.deepcopy(removed)
                            removed.update(restore_fields)
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
