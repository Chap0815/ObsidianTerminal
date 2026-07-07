"""Shared read-side PnL helpers for launcher/dashboard views."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bot_utils.futures_math import calc_unrealized_pnl


FUTURES_STATE_STALE_SEC = 30 * 60


def spot_unrealized_pnl(entry_price: Any, current_price: Any, amount: Any) -> float:
    """Remaining spot-position MTM in USDT.

    Use the remaining base amount, not original invested notional. This keeps
    launcher and dashboard aligned after base-fees, partial sells, or dust.
    """
    try:
        entry = float(entry_price or 0.0)
        current = float(current_price or 0.0)
        qty = float(amount or 0.0)
    except (TypeError, ValueError):
        return 0.0
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
        stored_pnl = float(get("unrealized_pnl", 0.0) or 0.0)
        stored_pct = float(get("unrealized_pct", 0.0) or 0.0)
        if stored_pnl != 0.0 and stored_pct != 0.0:
            return stored_pnl, stored_pct
        entry = float(get("entry_price", 0.0) or 0.0)
        current = float(get("current_price", 0.0) or 0.0)
        margin = float(get("margin_usdt", 0.0) or 0.0)
        leverage = float(get("leverage", 1.0) or 1.0)
        side = str(get("position_type", "") or "").upper()
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
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def futures_state_age_sec(row: Any, *, now: datetime | None = None) -> float | None:
    try:
        ts = row.get("last_update")
    except AttributeError:
        return None
    dt = _parse_utc_timestamp(ts)
    if dt is None:
        return None
    ref = now or datetime.now(timezone.utc)
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
