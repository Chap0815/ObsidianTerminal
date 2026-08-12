"""
bot_utils/status_report.py  hourly Telegram status summary (all 4 bots).

ONE shared builder called from both base-class heartbeat loops (SpotBot +
FuturesBot), so TREND/SPOT/FUTURES/CROSS all get the same report from a single
place. Reports: realized PnL today, total unrealized PnL, and every open
position with its % and PnL. Uses the state's cached last_price (refreshed every
monitor tick)  no extra API calls. Best-effort: never raises into the heartbeat.
"""
from __future__ import annotations

import math
import time


_STATUS_RETRY_SECONDS = 60.0


def _log_status_report_error(context: str, exc: Exception) -> None:
    try:
        from bot_utils.silent_log import silent_log

        silent_log(context, exc)
    except Exception:
        pass


def _retry_timestamp(now: float, interval_sec: float) -> float:
    retry_delay = min(_STATUS_RETRY_SECONDS, interval_sec)
    return now - max(0.0, interval_sec - retry_delay)


def _finite_float_or_none(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _positive_float_or_none(value):
    parsed = _finite_float_or_none(value)
    return parsed if parsed is not None and parsed > 0 else None


def _positions_summary(snapshot: dict, is_futures: bool):
    """Return (lines, total_unrealized_usdt). Branches on spot vs leveraged
    math. % is leveraged (margin) for futures/cross  matching the UI  and a
    plain price move for spot."""
    from bot_utils.futures_math import calc_unrealized_pnl, price_move_pct

    rows = []   # (pnl, side, symbol, pct)
    total = 0.0
    for sym, d in snapshot.items():
        try:
            entry = _positive_float_or_none(d.get("buy"))
            last = _positive_float_or_none(d.get("last_price"))
            if entry is None or last is None:
                continue
            if is_futures:
                side = d.get("position_type", "LONG")
                margin = _positive_float_or_none(d.get("invested_usdt"))
                lev = _positive_float_or_none(d.get("leverage"))
                if margin is None or lev is None:
                    continue
                pnl, pct = calc_unrealized_pnl(entry, last, margin, lev, side)
            else:
                side = "SPOT"
                amount = _positive_float_or_none(d.get("amount"))
                if amount is None:
                    continue
                pnl = amount * (last - entry)
                pct = price_move_pct(entry, last, "LONG")
            if not (math.isfinite(pnl) and math.isfinite(pct)):
                continue
            total += pnl
            rows.append((pnl, side, sym, pct))
        except Exception:
            continue
    rows.sort(reverse=True)   # best PnL first
    lines = [f" {sym} {side} {pct:+.1f}% ({pnl:+.2f})"
             for pnl, side, sym, pct in rows]
    return lines, total


def maybe_send_hourly_status(*, bot_name: str, is_futures: bool,
                             simulation: bool, state, last_sent: float,
                             interval_sec: int = 10800,
                             safe_mode_active: bool = False) -> float:
    """If at least ``interval_sec`` elapsed since ``last_sent``, build + send the
    status summary and return the new timestamp; otherwise return ``last_sent``
    unchanged. Safe to call every heartbeat (it self-throttles).

    Default cadence is every 3h (10800s) for all bots."""
    now = time.time()
    interval = _finite_float_or_none(interval_sec)
    if interval is None or interval < 0.0:
        interval = 10800.0
    previous = _finite_float_or_none(last_sent)
    if previous is None:
        previous = 0.0
    if simulation:
        return now
    if now - previous < interval:
        return previous

    try:
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from core.logger import send_telegram
        from core.database import get_today_pnl
    except Exception as exc:
        _log_status_report_error("3h status imports", exc)
        return _retry_timestamp(now, interval)

    try:
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return now

        snapshot = state.get_all() if state is not None else {}
        lines, unreal = _positions_summary(snapshot, is_futures)

        realized = None
        try:
            realized = _finite_float_or_none(get_today_pnl(
                bot_name, mode_is_sim=simulation).get("total_profit", 0.0) or 0.0)
            if realized is None:
                raise ValueError("today PnL is not finite")
        except Exception as exc:
            _log_status_report_error("3h status realized PnL", exc)

        mode = "SIM" if simulation else "LIVE"
        sm = "  SAFE_MODE" if safe_mode_active else ""
        realized_text = (
            f"{realized:+.2f} USDT" if realized is not None else "unknown"
        )
        header = (f" [{bot_name}] status 3h ({mode}){sm}\n"
                  f"Open: {len(lines)}  Realized today: {realized_text}\n"
                  f"Unrealized: {unreal:+.2f} USDT")
        # Cap the position list so a big book can't blow past Telegram's
        # message limit; the total still reflects ALL positions.
        MAX_LINES = 20
        body = "\n".join(lines[:MAX_LINES])
        if len(lines) > MAX_LINES:
            body += f"\n (+{len(lines) - MAX_LINES} more)"
        msg = header + ("\n" + body if body else "\n(no open positions)")

        accepted = send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)
        if accepted is False:
            raise RuntimeError("3h status Telegram message was not accepted")
    except Exception as exc:
        # Never let a status-report problem disturb the heartbeat loop.
        _log_status_report_error("3h status send", exc)
        return _retry_timestamp(now, interval)
    return now
