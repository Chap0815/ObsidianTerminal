"""Fail-soft runtime consistency and observability helpers."""
from __future__ import annotations

import json
import math
import time
from typing import Any, Mapping


def _base_symbol(value: Any) -> str:
    text = str(value or "").upper().strip()
    return text.split("/")[0].split(":")[0]


def _positive_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = abs(float(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _direction(value: Any) -> str:
    text = str(value or "").upper().strip()
    if text in {"BUY", "LONG"}:
        return "LONG"
    if text in {"SELL", "SHORT"}:
        return "SHORT"
    if text == "SPOT":
        return "SPOT"
    return text


def _claim_extra(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(str(row.get("extra_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def claim_recovery_metadata(
    claim_rows: list[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Return only observational/recovery fields safe to restore to state."""
    allowed = {
        "entry_id", "entry_quality_score", "entry_quality_label",
        "entry_quality_reasons", "provisional", "adopted",
    }
    recovered: dict[str, dict[str, Any]] = {}
    for row in claim_rows:
        symbol = _base_symbol(row.get("symbol"))
        if not symbol:
            continue
        extra = _claim_extra(row)
        recovered[symbol] = {
            key: extra[key] for key in allowed if key in extra
        }
    return recovered


def compare_position_layers(
    state_rows: Mapping[str, Mapping[str, Any]],
    claim_rows: list[Mapping[str, Any]] | None,
    exchange_rows: Mapping[str, Mapping[str, Any]],
    *,
    amount_tolerance: float = 0.05,
) -> dict[str, Any]:
    """Compare already-fetched position layers without making API calls."""
    states = {_base_symbol(key): dict(value)
              for key, value in state_rows.items() if _base_symbol(key)}
    claims = None
    if claim_rows is not None:
        claims = {_base_symbol(row.get("symbol")): dict(row)
                  for row in claim_rows if _base_symbol(row.get("symbol"))}
    exchange = {_base_symbol(key): dict(value)
                for key, value in exchange_rows.items() if _base_symbol(key)}

    state_symbols = set(states)
    claim_symbols = set(claims or {})
    exchange_symbols = set(exchange)
    money_issues: list[str] = []
    metadata_issues: list[str] = []

    if claims is not None:
        for symbol in sorted(state_symbols - claim_symbols):
            money_issues.append(f"state_without_claim:{symbol}")
        for symbol in sorted(claim_symbols - state_symbols):
            money_issues.append(f"claim_without_state:{symbol}")
    for symbol in sorted(state_symbols - exchange_symbols):
        money_issues.append(f"state_without_exchange:{symbol}")
    for symbol in sorted(exchange_symbols - state_symbols):
        money_issues.append(f"exchange_without_state:{symbol}")

    for symbol in sorted(state_symbols & exchange_symbols):
        state_amount = _positive_float(states[symbol].get("amount"))
        exchange_amount = _positive_float(exchange[symbol].get("amount"))
        if state_amount is not None and exchange_amount is not None:
            drift = abs(state_amount - exchange_amount) / max(
                state_amount, exchange_amount)
            if drift > max(0.0, amount_tolerance):
                money_issues.append(f"amount_mismatch:{symbol}:{drift:.3f}")
        state_direction = _direction(states[symbol].get("position_type"))
        exchange_direction = _direction(exchange[symbol].get("direction"))
        if (state_direction and exchange_direction
                and state_direction != exchange_direction
                and state_direction != "SPOT"):
            money_issues.append(
                f"direction_mismatch:{symbol}:{state_direction}:"
                f"{exchange_direction}")

    for symbol, row in sorted(states.items()):
        if not str(row.get("entry_id") or "").strip():
            metadata_issues.append(f"state_missing_entry_id:{symbol}")
        if row.get("entry_quality_score") is None:
            metadata_issues.append(f"state_missing_quality:{symbol}")
    if claims is not None:
        for symbol, row in sorted(claims.items()):
            extra = _claim_extra(row)
            if not str(extra.get("entry_id") or "").strip():
                metadata_issues.append(f"claim_missing_entry_id:{symbol}")
            if extra.get("entry_quality_score") is None:
                metadata_issues.append(f"claim_missing_quality:{symbol}")

    return {
        "ok": not money_issues,
        "metadata_complete": not metadata_issues,
        "state_count": len(states),
        "claim_count": len(claims or {}),
        "claims_available": claims is not None,
        "exchange_count": len(exchange),
        "money_issues": money_issues,
        "metadata_issues": metadata_issues,
    }


def emit_startup_integrity(
    *, bot_name: str, mode: str,
    state_rows: Mapping[str, Mapping[str, Any]],
    exchange_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Read claims, compare all layers and emit one passive startup report."""
    claims = None
    try:
        from core.database import get_open_positions_db
        claims = get_open_positions_db(bot_name)
    except Exception:
        pass
    owned_symbols = {_base_symbol(symbol) for symbol in state_rows}
    if claims is not None:
        owned_symbols.update(
            _base_symbol(row.get("symbol")) for row in claims)
    owned_symbols.discard("")
    scoped_exchange = {
        symbol: row for symbol, row in exchange_rows.items()
        if _base_symbol(symbol) in owned_symbols
    }
    report = compare_position_layers(state_rows, claims, scoped_exchange)
    report["account_exchange_count"] = len(exchange_rows)
    report["ignored_unowned_exchange_count"] = max(
        0, len(exchange_rows) - len(scoped_exchange))
    try:
        from core.logger import log_event, log_struct
        log_struct("startup_position_integrity", bot=bot_name, mode=mode,
                   **report)
        if report["money_issues"]:
            log_event(
                f"[{bot_name}] startup position integrity warning: "
                + ", ".join(report["money_issues"][:8]),
                "WARN",
            )
        elif report["metadata_issues"]:
            log_event(
                f"[{bot_name}] startup metadata incomplete: "
                + ", ".join(report["metadata_issues"][:8]),
                "WARN",
            )
    except Exception:
        pass
    return report


_last_watchdog_fingerprint: tuple[str, ...] = ()
_last_watchdog_log_at = 0.0


def runtime_observability_snapshot(
    *, state_rows: Mapping[str, Mapping[str, Any]] | None = None,
    ticker_cache: Any = None,
) -> dict[str, Any]:
    state_entry_ids = {
        str(row.get("entry_id") or "")
        for row in (state_rows or {}).values()
        if str(row.get("entry_id") or "")
    }
    try:
        from trading.entry_lifecycle import lifecycle_health_snapshot
        lifecycle = lifecycle_health_snapshot(state_entry_ids=state_entry_ids)
    except Exception:
        lifecycle = {"available": False}
    try:
        ticker = ticker_cache.stats() if ticker_cache is not None else {}
    except Exception:
        ticker = {}
    return {"entry_lifecycle_health": lifecycle, "ticker_cache": ticker}


def log_runtime_observability(
    *, bot_name: str, mode: str,
    state_rows: Mapping[str, Mapping[str, Any]] | None = None,
    ticker_cache: Any = None,
) -> dict[str, Any]:
    """Emit aggregate telemetry and rate-limited lifecycle warnings."""
    global _last_watchdog_fingerprint, _last_watchdog_log_at
    snapshot = runtime_observability_snapshot(
        state_rows=state_rows, ticker_cache=ticker_cache)
    lifecycle = snapshot.get("entry_lifecycle_health") or {}
    anomalies = tuple(sorted(str(item) for item in lifecycle.get(
        "anomalies", [])))
    try:
        from core.logger import log_event, log_struct
        log_struct("runtime_observability", bot=bot_name, mode=mode,
                   **snapshot)
        now = time.monotonic()
        if anomalies and (
            anomalies != _last_watchdog_fingerprint
            or now - _last_watchdog_log_at >= 300.0
        ):
            log_event(
                f"[{bot_name}] entry lifecycle watchdog: "
                + ", ".join(anomalies[:8]),
                "WARN",
            )
            _last_watchdog_fingerprint = anomalies
            _last_watchdog_log_at = now
    except Exception:
        pass
    return snapshot
