"""Shared read-side PnL helpers for launcher/dashboard views."""
from __future__ import annotations

import os
import math
from datetime import datetime, timezone
from typing import Any

from bot_utils.futures_math import calc_unrealized_pnl


FUTURES_STATE_STALE_SEC = 30 * 60
SPOT_STATE_STALE_SEC = 30 * 60


def _finite_float(value: Any, default: float = 0.0) -> float:
    if value is None or isinstance(value, bool):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if math.isfinite(parsed) else default


def spot_unrealized_pnl(entry_price: Any, current_price: Any, amount: Any) -> float:
    """Remaining spot-position MTM in USDT.

    Use the remaining base amount, not original invested notional. This keeps
    launcher and dashboard aligned after base-fees, partial sells, or dust.
    """
    entry = _finite_float(entry_price)
    current = _finite_float(current_price)
    qty = _finite_float(amount)
    if entry <= 0.0 or current <= 0.0 or qty <= 0.0:
        return 0.0
    return qty * (current - entry)


def futures_unrealized_from_row(row: Any) -> tuple[float, float]:
    """Return ``(unrealized_pnl, unrealized_pct_on_margin)`` for a DB row."""
    try:
        get = row.get
    except AttributeError:
        return 0.0, 0.0
    try:
        stored_pnl = _finite_float(get("unrealized_pnl", 0.0))
        stored_pct = _finite_float(get("unrealized_pct", 0.0))
        if stored_pnl != 0.0 and stored_pct != 0.0:
            return stored_pnl, stored_pct
        entry = _finite_float(get("entry_price", 0.0))
        current = _finite_float(get("current_price", 0.0))
        margin = _finite_float(get("margin_usdt", 0.0))
        leverage = _finite_float(get("leverage", 1.0), 1.0)
        side = str(get("position_type", "") or "").upper()
        if side not in {"LONG", "SHORT"}:
            return stored_pnl, stored_pct
        if entry <= 0.0 or current <= 0.0 or margin <= 0.0:
            return stored_pnl, stored_pct
        calc_pnl, calc_pct = calc_unrealized_pnl(
            entry, current, margin, leverage, side
        )
        return (
            stored_pnl if stored_pnl != 0.0 else calc_pnl,
            stored_pct if stored_pct != 0.0 else calc_pct,
        )
    except Exception:
        return 0.0, 0.0


def _parse_utc_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)


def futures_state_age_sec(row: Any, *, now: datetime | None = None) -> float | None:
    try:
        ts = row.get("last_update")
    except AttributeError:
        return None
    dt = _parse_utc_timestamp(ts)
    if dt is None:
        return None
    if now is None:
        try:
            from core.clock import now_utc

            ref = now_utc()
        except Exception:
            ref = datetime.now(timezone.utc)
    else:
        ref = now
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    return (ref.astimezone(timezone.utc) - dt).total_seconds()


def is_futures_state_fresh(
    row: Any,
    *,
    now: datetime | None = None,
    max_age_sec: float = FUTURES_STATE_STALE_SEC,
) -> bool:
    """Return whether a futures-state row is fresh enough for money views.

    Missing/unparseable timestamps are treated as stale. Production reporting
    must not count legacy or corrupt rows as open money by default.
    """
    age = futures_state_age_sec(row, now=now)
    if age is None:
        return False
    if age < -60.0:
        return False
    return age <= max_age_sec


def state_file_age_sec(path: str, *, now_ts: float | None = None) -> float | None:
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None
    ref = datetime.now(timezone.utc).timestamp() if now_ts is None else float(now_ts)
    return ref - float(mtime)


def is_state_file_fresh(
    path: str,
    *,
    now_ts: float | None = None,
    max_age_sec: float = SPOT_STATE_STALE_SEC,
) -> bool:
    """Return whether a JSON state file is fresh enough for money views.

    Spot JSON state has no per-row heartbeat, so dashboards use the file mtime
    as a conservative freshness proxy. Missing files, very old files, and files
    dated far in the future are excluded from live money KPIs.
    """
    age = state_file_age_sec(path, now_ts=now_ts)
    if age is None:
        return False
    if age < -60.0:
        return False
    return age <= max_age_sec
