"""Entry-intent identity plus fail-soft lifecycle telemetry.

The identifier is authoritative for journals, claims, and client-order IDs and
must never be empty. Telemetry emission remains observational and fail-soft.
"""
from __future__ import annotations

import uuid
import threading
import time
from typing import Any


_TERMINAL_STAGES = frozenset((
    "opened", "blocked", "aborted", "order_failed", "state_failed",
))
_MAX_TRACKED_ENTRIES = 4096
_MAX_STAGES_PER_ENTRY = 32
_lock = threading.Lock()
_entries: dict[str, dict[str, Any]] = {}


def _last_activity_key(item: tuple[str, dict[str, Any]]) -> tuple[float, str]:
    entry_id, row = item
    try:
        last_at = float(row.get("last_at") or row.get("created_at") or 0.0)
    except (TypeError, ValueError, OverflowError):
        last_at = 0.0
    return last_at, entry_id


def _make_room_for_entry() -> None:
    """Bound telemetry memory while preserving active attempts when possible."""
    limit = max(1, int(_MAX_TRACKED_ENTRIES))
    if len(_entries) < limit:
        return
    terminal = [item for item in _entries.items()
                if item[1].get("terminal_at") is not None]
    pool = terminal or list(_entries.items())
    evicted_id, _ = min(pool, key=_last_activity_key)
    _entries.pop(evicted_id, None)


def _record_stage(entry_id: str, stage: str, fields: dict[str, Any]) -> None:
    if not entry_id:
        return
    now = time.monotonic()
    with _lock:
        row = _entries.get(entry_id)
        if row is None:
            _make_room_for_entry()
            row = {
                "created_at": now,
                "stages": [],
                "opened_count": 0,
            }
            _entries[entry_id] = row
        row["last_at"] = now
        row["last_stage"] = stage
        row["bot"] = str(fields.get("bot") or row.get("bot") or "")
        row["symbol"] = str(fields.get("symbol") or row.get("symbol") or "")
        stages = row["stages"]
        stages.append(stage)
        stage_limit = max(1, int(_MAX_STAGES_PER_ENTRY))
        if len(stages) > stage_limit:
            del stages[:-stage_limit]
        if stage == "candidate":
            row["candidate_at"] = now
        if stage == "order_attempt":
            row["order_attempt_at"] = now
        if stage in _TERMINAL_STAGES:
            row["terminal_at"] = now
        if stage == "opened":
            row["opened_at"] = now
            row["opened_count"] += 1


def lifecycle_health_snapshot(
    *, state_entry_ids: set[str] | None = None,
    max_order_age_sec: float = 120.0,
    max_candidate_age_sec: float = 120.0,
    opened_state_grace_sec: float = 30.0,
    opened_state_check_window_sec: float = 120.0,
    retention_sec: float = 3600.0,
) -> dict[str, Any]:
    """Return process-local lifecycle anomalies without raising."""
    now = time.monotonic()
    state_ids = set(state_entry_ids or set())
    anomalies: list[str] = []
    with _lock:
        expired = [entry_id for entry_id, row in _entries.items()
                   if now - float(row.get("last_at") or now) > retention_sec]
        for entry_id in expired:
            _entries.pop(entry_id, None)
        rows = {
            entry_id: {**row, "stages": list(row.get("stages") or [])}
            for entry_id, row in _entries.items()
        }

    for entry_id, row in rows.items():
        label = f"{row.get('bot', '')}:{row.get('symbol', '')}:{entry_id[:8]}"
        if (row.get("candidate_at") is not None
                and len(row.get("stages") or []) == 1):
            age = now - float(row["candidate_at"])
            if age >= max(0.0, max_candidate_age_sec):
                anomalies.append(
                    f"candidate_without_followup:{label}:{int(age)}s")
        attempt_at = row.get("order_attempt_at")
        if attempt_at is not None and row.get("terminal_at") is None:
            age = now - float(attempt_at)
            if age >= max(0.0, max_order_age_sec):
                anomalies.append(f"order_without_terminal:{label}:{int(age)}s")
        if int(row.get("opened_count") or 0) > 1:
            anomalies.append(f"duplicate_opened:{label}")
        opened_at = row.get("opened_at")
        if (opened_at is not None and state_entry_ids is not None
                and now - float(opened_at) >= opened_state_grace_sec
                and now - float(opened_at) <= max(
                    opened_state_check_window_sec, opened_state_grace_sec)
                and entry_id not in state_ids):
            anomalies.append(f"opened_without_state:{label}")
        if row.get("last_stage") == "state_failed":
            anomalies.append(f"state_failed:{label}")
    return {
        "available": True,
        "tracked_entries": len(rows),
        "open_attempts": sum(
            1 for row in rows.values()
            if row.get("order_attempt_at") is not None
            and row.get("terminal_at") is None
        ),
        "pending_candidates": sum(
            1 for row in rows.values()
            if row.get("candidate_at") is not None
            and len(row.get("stages") or []) == 1
        ),
        "anomalies": sorted(anomalies),
    }


def new_entry_id(
    *, bot: str = "", symbol: str = "", mode: str = "",
    direction: str = "",
) -> str:
    """Return a non-empty, process-independent identifier for one entry intent."""
    last_error = None
    entry_id = ""
    for factory in (uuid.uuid4, uuid.uuid1):
        try:
            candidate = str(factory().hex).strip().lower()
            if (
                len(candidate) == 32
                and all(char in "0123456789abcdef" for char in candidate)
            ):
                entry_id = candidate
                break
        except Exception as exc:
            last_error = exc
    if not entry_id:
        raise RuntimeError("entry id generation failed") from last_error
    if bot or symbol:
        try:
            _record_stage(entry_id, "candidate", {
                "bot": bot, "symbol": symbol, "mode": mode,
                "direction": direction,
            })
        except Exception:
            pass
    return entry_id


def emit_entry_lifecycle(
    entry_id: str,
    *,
    bot: str,
    symbol: str,
    stage: str,
    mode: str,
    reason: str = "",
    **fields: Any,
) -> None:
    """Write lifecycle telemetry without affecting the trading path."""
    try:
        _record_stage(str(entry_id or "")[:64], str(stage or "")[:48], {
            "bot": bot, "symbol": symbol, **fields,
        })
    except Exception:
        pass
    try:
        from core.logger import log_struct

        log_struct(
            "entry_lifecycle",
            entry_id=str(entry_id or "")[:64],
            bot=str(bot or "")[:32],
            symbol=str(symbol or "")[:64],
            stage=str(stage or "")[:48],
            mode=str(mode or "")[:8],
            reason=str(reason or "")[:256],
            **fields,
        )
    except Exception:
        pass
