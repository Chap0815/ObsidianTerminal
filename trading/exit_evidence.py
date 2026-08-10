"""Stable exit-label classification for strategy research."""
from __future__ import annotations


def classify_exit_reason(reason: str) -> str:
    normalized = str(reason or "").strip().lower()
    if "application quit" in normalized or "manual" in normalized:
        return "manual_shutdown"
    if any(
        token in normalized
        for token in ("reconcile", "external", "orphan", "exchange close")
    ):
        return "external_or_reconcile"
    if any(token in normalized for token in ("emergency", "liquidation", "kill")):
        return "risk_or_emergency"
    return "strategy_exit"


def is_strategy_exit(reason: str) -> bool:
    return classify_exit_reason(reason) == "strategy_exit"
