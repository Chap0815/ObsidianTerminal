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
        self._is_futures = is_futures
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

    def _registry_upsert(self, sym: str, data: dict) -> None:
        if not self._bot_name:
            return
        try:
            from core.database import upsert_open_position
            upsert_open_position(
                bot_name=self._bot_name, symbol=sym,
                buy_price=float(data.get("buy_price") or data.get("buy") or 0),
                buy_time=data.get("buy_time") or "",
                amount=float(data.get("amount") or 0),
                invested_usdt=float(data.get("invested_usdt") or 0),
                position_type=data.get("position_type",
                                       "FUTURES" if self._is_futures else "SPOT"),
                leverage=float(data.get("leverage") or 1),
                state="OPEN",
            )
        except Exception:
            # Registry is a coordination mirror, never the position truth
            # (trades.json is). A failure here must never break a trade.
            pass

    def _registry_remove(self, sym: str) -> None:
        if not self._bot_name:
            return
        try:
            from core.database import remove_open_position
            remove_open_position(self._bot_name, sym)
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
        for sym, data in clean.items():
            self._registry_upsert(sym, data)

    #  Reads (always return deep copies) 

    def get_all(self) -> dict:
        """Deep-copy snapshot of all trades  safe to iterate without lock."""
        with self._lock:
            return copy.deepcopy(self._trades)

    def get(self, sym: str) -> Optional[dict]:
        """Deep-copy of one trade, or None."""
        with self._lock:
            if sym not in self._trades:
                return None
            return copy.deepcopy(self._trades[sym])

    def count(self) -> int:
        with self._lock:
            return len(self._trades)

    def has(self, sym: str) -> bool:
        with self._lock:
            return sym in self._trades

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._trades.keys())

    #  Writes (snapshot under lock, save outside) 

    def add(self, sym: str, data: dict) -> None:
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
            return

        with self._lock:
            self._trades[sym] = data
            snapshot = copy.deepcopy(self._trades)  # true deep-copy
        atomic_save_json(self._db_file, snapshot)
        # Claim the coin in the shared multi-bot registry (outside the lock,
        # like the JSON write). This is what makes is_claimed_by_other() work
        # so another bot won't open the SAME perp and net against us.
        self._registry_upsert(sym, data)

    def update(self, sym: str, key: str, value) -> None:
        """Set one field on an existing trade. No-op if symbol missing."""
        snapshot = None
        claim_row = None
        with self._lock:
            if sym in self._trades:
                self._trades[sym][key] = value
                snapshot = copy.deepcopy(self._trades)  # true deep-copy
                if key in _CLAIM_FIELDS:
                    claim_row = copy.deepcopy(self._trades[sym])
        if snapshot is not None:
            atomic_save_json(self._db_file, snapshot)
            # Keep the shared claim row in sync when a claim-relevant field
            # changed (e.g. amount/invested after a partial sell). Skipping the
            # frequent last_price/highest updates avoids hammering the DB.
            if claim_row is not None:
                self._registry_upsert(sym, claim_row)

    def update_many(self, sym: str, fields: dict) -> None:
        """Set multiple fields atomically  single persist instead of N."""
        snapshot = None
        claim_row = None
        with self._lock:
            if sym in self._trades:
                self._trades[sym].update(fields)
                snapshot = copy.deepcopy(self._trades)  # true deep-copy
                if _CLAIM_FIELDS.intersection(fields):
                    claim_row = copy.deepcopy(self._trades[sym])
        if snapshot is not None:
            atomic_save_json(self._db_file, snapshot)
            if claim_row is not None:
                self._registry_upsert(sym, claim_row)

    def remove(self, sym: str) -> None:
        """Remove a trade. No-op if symbol missing."""
        snapshot = None
        with self._lock:
            if sym in self._trades:
                del self._trades[sym]
                snapshot = copy.deepcopy(self._trades)  # true deep-copy
        if snapshot is not None:
            persisted = atomic_save_json(self._db_file, snapshot)
            if persisted:
                # Release the claim so other bots can trade this coin again.
                self._registry_remove(sym)
            else:
                try:
                    from bot_utils.silent_log import silent_log
                    silent_log(
                        f"TradeState.remove({self._bot_name or '-'}:{sym})",
                        RuntimeError("state persist failed; claim kept"),
                    )
                except Exception:
                    pass

    def save_now(self) -> None:
        """Force a persist of the current state (used by emergency-close paths
        that mutated trades via direct refs without going through update())."""
        with self._lock:
            snapshot = copy.deepcopy(self._trades)  # true deep-copy
        atomic_save_json(self._db_file, snapshot)
