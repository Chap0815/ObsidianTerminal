"""Fail-soft correlation telemetry for one entry intent.

The identifier is observational only. Trading paths must never depend on it.
"""
from __future__ import annotations

import uuid
from typing import Any


def new_entry_id() -> str:
    """Return a compact, process-independent identifier for one entry intent."""
    try:
        return uuid.uuid4().hex
    except Exception:
        return ""


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
