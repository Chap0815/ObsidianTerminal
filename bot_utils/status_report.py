"""
bot_utils/status_report.py  hourly Telegram status summary (all 4 bots).

ONE shared builder called from both base-class heartbeat loops (SpotBot +
FuturesBot), so TREND/SPOT/FUTURES/CROSS all get the same report from a single
place. Reports: realized PnL today, total unrealized PnL, and every open
position with its % and PnL. Uses the state's cached last_price (refreshed every
monitor tick)  no extra API calls. Best-effort: never raises into the heartbeat.
"""
from __future__ import annotations

import time


def _positions_summary(snapshot: dict, is_futures: bool):
    """Return (lines, total_unrealized_usdt). Branches on spot vs leveraged
    math. % is leveraged (margin) for futures/cross  matching the UI  and a
    plain price move for spot."""
    from bot_utils.futures_math import calc_unrealized_pnl, price_move_pct

    rows = []   # (pnl, side, symbol, pct)
    total = 0.0
    for sym, d in snapshot.items():
        try:
            entry = float(d.get("buy", 0) or 0)
            last = float(d.get("last_price", entry) or entry)
            if entry <= 0:
                continue
            if is_futures:
                side = d.get("position_type", "LONG")
                margin = float(d.get("invested_usdt", 0) or 0)
                lev = float(d.get("leverage", 1) or 1)
                pnl, pct = calc_unrealized_pnl(entry, last, margin, lev, side)
            else:
                side = "SPOT"
                amount = float(d.get("amount", 0) or 0)
                pnl = amount * (last - entry)
                pct = price_move_pct(entry, last, "LONG")
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
    if simulation:
        return now
    if now - last_sent < interval_sec:
        return last_sent

    try:
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        from core.logger import send_telegram
        from core.database import get_today_pnl
    except Exception:
        return now   # mark as sent so we don't hammer imports every 2s

    try:
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return now

        snapshot = state.get_all() if state is not None else {}
        lines, unreal = _positions_summary(snapshot, is_futures)

        realized = 0.0
        try:
            realized = float(get_today_pnl(
                bot_name, mode_is_sim=simulation).get("total_profit", 0.0) or 0.0)
        except Exception:
            pass

        mode = "SIM" if simulation else "LIVE"
        sm = "  SAFE_MODE" if safe_mode_active else ""
        header = (f" [{bot_name}] status 3h ({mode}){sm}\n"
                  f"Open: {len(lines)}  Realized today: {realized:+.2f} USDT\n"
                  f"Unrealized: {unreal:+.2f} USDT")
        # Cap the position list so a big book can't blow past Telegram's
        # message limit; the total still reflects ALL positions.
        MAX_LINES = 20
        body = "\n".join(lines[:MAX_LINES])
        if len(lines) > MAX_LINES:
            body += f"\n (+{len(lines) - MAX_LINES} more)"
        msg = header + ("\n" + body if body else "\n(no open positions)")

        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)
    except Exception:
        # Never let a status-report problem disturb the heartbeat loop.
        pass
    return now
