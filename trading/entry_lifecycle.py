"""Fail-soft correlation telemetry for one entry intent.

The identifier is observational only. Trading paths must never depend on it.
"""
from __future__ import annotations

import uuid
import threading
import time
from typing import Any


_TERMINAL_STAGES = frozenset((
    "opened", "blocked", "aborted", "order_failed", "state_failed",
))
_lock = threading.Lock()
_entries: dict[str, dict[str, Any]] = {}


def _record_stage(entry_id: str, stage: str, fields: dict[str, Any]) -> None:
    if not entry_id:
        return
    now = time.monotonic()
    with _lock:
        row = _entries.setdefault(entry_id, {
            "created_at": now,
            "stages": [],
            "opened_count": 0,
        })
        row["last_at"] = now
        row["last_stage"] = stage
        row["bot"] = str(fields.get("bot") or row.get("bot") or "")
        row["symbol"] = str(fields.get("symbol") or row.get("symbol") or "")
        row["stages"].append(stage)
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
        rows = {entry_id: dict(row) for entry_id, row in _entries.items()}

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
    """Return a compact, process-independent identifier for one entry intent."""
    try:
        entry_id = uuid.uuid4().hex
    except Exception:
        return ""
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
