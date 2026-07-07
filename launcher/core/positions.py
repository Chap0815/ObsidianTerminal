"""
Position read + direct-close helpers used by the stop dialogs and the
emergency-close flow.

Two flavours:

* **Spot**  positions are stored in ``{log_dir}/trades.json``. Closing
  means market-selling the holding and writing the realized PnL to the
  ``trades`` DB table.

* **Futures**  positions are stored in the ``futures_state`` SQLite
  table (with ``trades.json`` as a fallback / metadata source). Closing
  means a reduceOnly market order in LIVE, or a pure DB update in SIM.

All "close" routines are designed to run inside a background thread.
They report progress through a ``log(severity, msg)`` callback supplied
by the caller  typically a thin wrapper around
``ObsidianApp._log_to_card`` that bounces back onto the Tk main thread
via ``app.after(0, )``.

Nothing here imports tkinter directly. Keep it that way.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from launcher.config.settings import BOT_META, PROJECT_ROOT


def _utc_now_str() -> str:
    """UTC timestamp in the format the bots use everywhere else.

    All bot writes use UTC, so the launcher's emergency-close paths must
    too  otherwise, for users in non-UTC timezones, launcher-closed trades
    would land on the wrong calendar day in dashboard aggregates and miss
    the "today" filter.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


_LIVE_RESIDUAL_DUST_USDT = 1.0


def _clamp_sim_cross_close_costs(
    *,
    bot_name: str,
    sim_only: bool,
    notional: float,
    taker_fee: float,
    entry_fee: float,
    exit_fee: float,
    funding_for_close: float,
) -> tuple[float, float, float]:
    """Keep CROSS SIM manual-close accounting on notional-based costs.

    CROSS SIM stores base-coin amounts in ``trades.sim.json``. The launcher
    close fallback also serves live futures bots where exchange contract size
    matters, so contract-size math stays untouched there. For CROSS SIM we
    clamp to the same notional-based envelope as the bot close path; otherwise
    low-price symbols can produce absurd fees when coin amount is interpreted
    like contracts.
    """
    if str(bot_name).upper() != "CROSS" or not sim_only or notional <= 0:
        return entry_fee, exit_fee, funding_for_close

    fee_rate = taker_fee if taker_fee > 0 else 0.0006
    expected_side_fee = notional * fee_rate
    max_side_fee = max(expected_side_fee * 5.0, notional * 0.02)

    if entry_fee < 0 or entry_fee > max_side_fee:
        entry_fee = expected_side_fee
    if exit_fee < 0 or exit_fee > max_side_fee:
        exit_fee = expected_side_fee

    max_funding = notional * 0.20
    if funding_for_close < 0 or funding_for_close > max_funding:
        funding_for_close = 0.0

    return entry_fee, exit_fee, funding_for_close


def _filled_base_amount(order, wrapper_sold, requested_amount: float) -> float:
    requested = max(0.0, float(requested_amount or 0.0))
    if isinstance(order, dict):
        try:
            filled = float(order.get("filled") or 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        if filled > 0:
            return min(requested, filled)
    try:
        sold = float(wrapper_sold or 0.0)
    except (TypeError, ValueError):
        sold = 0.0
    if sold > 0:
        return min(requested, sold)
    return 0.0


def _state_path_for(bot_name: str, simulation: bool | None = None) -> str:
    """Mode-aware state-file path for a bot.

    SIM and LIVE use SEPARATE state files (trades.json vs trades.sim.json) so a
    mode toggle never mixes paper and real positions. The launcher must read the
    SAME file the bot writes, otherwise it would show/close the wrong set (e.g.
    miss a SIM bot's positions entirely).
    """
    base = f"{BOT_META[bot_name]['log_dir']}/trades.json"
    try:
        from bot_utils.sim_flag import read_simulation_flag, sim_state_path
        if simulation is None:
            simulation = bool(read_simulation_flag(bot_name))
        return sim_state_path(base, bool(simulation))
    except Exception as e:
        raise RuntimeError(
            f"{bot_name}: cannot resolve SIM/LIVE state path safely: {e}"
        ) from e


def _state_path_for_mode(bot_name: str, simulation: bool) -> str:
    try:
        return _state_path_for(bot_name, simulation)
    except TypeError:
        return _state_path_for(bot_name)


def _get_futures_state_for_mode(get_futures_state_fn, bot_name: str,
                                simulation: bool) -> list:
    try:
        return get_futures_state_fn(bot_name, mode_is_sim=simulation)
    except TypeError:
        return get_futures_state_fn(bot_name)


def _state_paths_for_all_modes(bot_name: str) -> list[tuple[str, str]]:
    base = os.path.join(PROJECT_ROOT, BOT_META[bot_name]["log_dir"], "trades.json")
    try:
        from bot_utils.sim_flag import sim_state_path
        return [("LIVE", sim_state_path(base, False)),
                ("SIM", sim_state_path(base, True))]
    except Exception:
        root, ext = os.path.splitext(base)
        return [("LIVE", base), ("SIM", f"{root}.sim{ext}")]


def _json_state_has_open_position(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    for row in data.values():
        if not isinstance(row, dict):
            continue
        if str(row.get("state", "OPEN")).upper() == "CLOSED":
            continue
        try:
            amount = float(row.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        # Fail closed: if a state row still has size, missing/malformed entry
        # price must not make SIM/LIVE switching look safe.
        if amount > 0:
            return True
    return False


def mode_switch_blockers(bot_name: str) -> list[str]:
    """Open artifacts that make SIM/LIVE switching unsafe while stopped."""
    blockers: list[str] = []
    for mode, path in _state_paths_for_all_modes(bot_name):
        if _json_state_has_open_position(path):
            blockers.append(f"{mode} state file")

    try:
        from core.database import get_connection
        conn = get_connection()
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM bot_open_positions WHERE bot_name=?",
            (bot_name,),
        ).fetchone()
        if rows and int(rows["n"] if hasattr(rows, "keys") else rows[0]) > 0:
            blockers.append("claim registry")
        sim_bot = f"{bot_name} (SIM)"
        for label, db_bot in (("LIVE", bot_name), ("SIM", sim_bot)):
            rows = conn.execute(
                "SELECT COUNT(*) AS n FROM futures_state WHERE bot_name=?",
                (db_bot,),
            ).fetchone()
            if rows and int(rows["n"] if hasattr(rows, "keys") else rows[0]) > 0:
                blockers.append(f"{label} futures_state")
    except Exception:
        blockers.append("state DB unavailable")
    return sorted(set(blockers))


#  Spot  read open positions 

def get_open_spot_positions(bot_name: str) -> list:
    """Read open spot positions from ``{log_dir}/trades.json``.

    Returns a list of dicts with ``symbol``, ``buy_price``,
    ``current_price`` (best-effort), ``amount``, ``invested_usdt``,
    ``buy_time``.

    ``trades.json`` may contain TWO different schemas; we accept both and
    prefer the newer when both are present:

    1. Legacy bot schema: ``buy``, ``highest``, ``buy_time``
    2. StateManager (``Position.to_dict``): ``buy_price``,
       ``highest_price``, ``buy_time``, plus extras like ``position_type``,
       ``state``, ``leverage``, 
    """
    try:
        path = _state_path_for(bot_name)
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        return []

    positions: list = []
    for sym, d in data.items():
        if not isinstance(d, dict):
            continue
        # Skip entries explicitly marked CLOSED (StateManager schema)
        # so we don't show partial-TP residue or stale closed positions.
        if str(d.get("state", "OPEN")).upper() == "CLOSED":
            continue
        # Prefer the new key, fall back to the old. Entries may have only
        # one or the other depending on which writer ran last.
        buy_price = float(d.get("buy_price") or d.get("buy") or 0)
        # Skip corrupted entries with buy_price=0  these would render as
        # 0.000000 in the stop dialog. TradeState.add() rejects them on
        # insert, but old data could still be on disk.
        if buy_price <= 0:
            continue
        highest = float(d.get("highest_price") or d.get("highest") or buy_price or 0)
        # ``last_price`` is set by Monitor on every tick (V2)  the best
        # "current" estimate when ticker fetch failed in the launcher.
        last_price = float(d.get("last_price") or 0)
        # Without a last_price snapshot, fall back to highest (more
        # accurate than buy_price for the "Now" column).
        current_price = last_price if last_price > 0 else highest
        amount = float(d.get("amount", 0))
        # Compute unrealized PnL here so the spot stop dialog has it. Gross
        # (fee-free) is fine for a quick pre-close estimate; uses the monitor's
        # last_price snapshot (current_price).
        unreal_pnl = amount * (current_price - buy_price)
        unreal_pct = ((current_price - buy_price) / buy_price * 100.0
                      if buy_price > 0 else 0.0)
        positions.append({
            "symbol":         sym,
            "buy_price":      buy_price,
            "current_price":  current_price,
            "amount":         amount,
            "invested_usdt":  float(d.get("invested_usdt", 0)),
            "buy_time":       d.get("buy_time") or d.get("opened_at"),
            "unrealized_pnl": unreal_pnl,
            "unrealized_pct": unreal_pct,
        })
    return positions


def refresh_spot_positions_with_live_prices(positions: list) -> list:
    """Refresh spot positions' PnL from live ticker data.

    Recomputes from raw fields. If the exchange is unreachable, recomputes
    from the last stored price instead of silently returning stale or zero
    values.
    """
    if not positions:
        return positions

    ex = None
    try:
        from config.exchange_config import get_spot_exchange_connection  # type: ignore
        ex = get_spot_exchange_connection()
    except Exception as e:
        import logging
        logging.warning(
            "[StopDialog Spot] live-price refresh skipped "
            "(exchange unreachable: %s); PnL recomputed from last stored price.",
            type(e).__name__,
        )

    refreshed: list = []
    for p in positions:
        p2 = dict(p)
        try:
            sym       = p.get("symbol", "")
            buy_price = float(p.get("buy_price") or 0)
            margin    = float(p.get("invested_usdt") or 0)
            stored_curr = float(p.get("current_price") or 0)

            # Prefer live, fall back to stored
            curr = stored_curr
            if ex is not None and sym:
                try:
                    t = ex.fetch_ticker(f"{sym}/USDT")
                    live = float(t.get("last") or t.get("close") or 0)
                    if live > 0:
                        curr = live
                except Exception as e:
                    import logging
                    logging.debug(
                        "[StopDialog Spot] %s: ticker failed (%s); using stored price.",
                        sym, type(e).__name__,
                    )

            if curr > 0 and buy_price > 0:
                profit_pct  = (curr - buy_price) / buy_price * 100.0
                profit_usdt = round(margin * (profit_pct / 100.0), 4) \
                                if margin > 0 else 0.0
                p2["current_price"]  = curr
                p2["unrealized_pct"] = profit_pct
                p2["unrealized_pnl"] = profit_usdt
            else:
                import logging
                logging.warning(
                    "[StopDialog Spot] %s: cannot recompute "
                    "(buy=%s, curr=%s); keeping stored.",
                    sym, buy_price, curr,
                )
        except Exception as e:
            import logging
            logging.warning(
                "[StopDialog Spot] row recompute failed for %s: %s: %s",
                p.get("symbol", "?"), type(e).__name__, e,
            )

        refreshed.append(p2)
    return refreshed


#  Futures  read open positions 

def get_open_futures_positions(bot_name: str = None) -> list:
    """Read open futures positions from the ``futures_state`` table.

    Pass ``bot_name`` to scope to ONE bot  FUTURES and CROSS share this
    table, so an unscoped read mixes both bots' positions in the stop dialog.
    """
    try:
        from core.database import get_futures_state  # type: ignore
        return get_futures_state(bot_name)
    except Exception as e:
        # silent_log = rate-limited stderr
        try:
            from bot_utils.silent_log import silent_log
            silent_log("Launcher get_futures_state", e)
        except Exception:
            import sys as _sys
            _sys.stderr.write(f"[Launcher] Couldn't load futures state: {e}\n")
        return []


def refresh_positions_with_live_prices(positions: list) -> list:
    """Recompute ``unrealized_pnl`` + ``unrealized_pct`` from raw fields
    (``entry_price``, ``current_price``, ``margin_usdt``, ``leverage``).

    Why: the DB-stored ``unrealized_pnl`` is only as fresh as the last
    monitor tick (every 20 s) and may be 0 right after a position open,
    before the first monitor cycle. Also, three different code paths used
    to set ``unrealized_pct`` with three different conventions
    (price_move vs. margin-leveraged vs. raw fraction)  recomputing here
    gives ONE authoritative value.

    Live ticker fetch is best-effort: if Bitget is unreachable, the
    last DB-stored ``current_price`` is used as a fallback so we still
    show a meaningful PnL, not zero.

    PnL convention matches ``main_bot_futures._calc_unrealized_pnl``:
      * ``pnl_usdt``       = ``margin * leverage * price_move_fraction``
      * ``unrealized_pct`` = ``pnl_usdt / margin * 100``   (margin-leveraged %)
    """
    if not positions:
        return positions

    # Try once to grab a futures exchange handle so we can refresh prices.
    # If this fails (e.g. user in a blocked region), we still recompute
    # PnL from the DB-stored ``current_price``.
    ex = None
    try:
        from config.exchange_config import get_futures_exchange_connection  # type: ignore
        ex = get_futures_exchange_connection()
    except Exception as e:
        import logging
        logging.warning(
            "[StopDialog] live-price refresh skipped "
            "(exchange unreachable: %s); PnL is recomputed from last DB snapshot.",
            type(e).__name__,
        )

    refreshed: list = []
    for p in positions:
        p2 = dict(p)
        try:
            sym       = p.get("symbol") or ""
            pos_type  = (p.get("position_type") or "LONG").upper()
            entry     = float(p.get("entry_price") or 0)
            margin    = float(p.get("margin_usdt") or 0)
            lev       = float(p.get("leverage") or 0)
            db_curr   = float(p.get("current_price") or 0)

            # Try to refresh current price live; fall back to DB value.
            curr = db_curr
            if ex is not None and sym:
                try:
                    t = ex.fetch_ticker(f"{sym}/USDT:USDT")
                    live = float(t.get("last") or t.get("close") or 0)
                    if live > 0:
                        curr = live
                except Exception as e:
                    import logging
                    logging.debug(
                        "[StopDialog] %s: live ticker fetch failed (%s: %s); using DB price.",
                        sym, type(e).__name__, e,
                    )

            # Recompute PnL from whatever price we ended up with.
            if curr > 0 and entry > 0 and margin > 0:
                if pos_type == "LONG":
                    price_move_pct = (curr - entry) / entry * 100.0
                else:
                    price_move_pct = (entry - curr) / entry * 100.0
                notional = margin * max(lev, 1.0)
                pnl_usdt = round(notional * (price_move_pct / 100.0), 4)
                pnl_pct_margin = (pnl_usdt / margin) * 100.0
                p2["current_price"]  = curr
                p2["unrealized_pnl"] = pnl_usdt
                p2["unrealized_pct"] = pnl_pct_margin
            else:
                # Missing inputs  keep DB values rather than blank them
                import logging
                logging.warning(
                    "[StopDialog] %s: cannot recompute PnL "
                    "(entry=%s, curr=%s, margin=%s, lev=%s); falling back to stored values.",
                    sym, entry, curr, margin, lev,
                )
        except Exception as e:
            import logging
            logging.warning(
                "[StopDialog] row recompute failed for %s: %s: %s",
                p.get("symbol", "?"), type(e).__name__, e,
            )

        refreshed.append(p2)
    return refreshed


#  Futures  direct close (fallback when bot isn't running) 

def direct_close_remaining_futures(
    log,
    sim_only: bool,
    reason: str = "Manual Stop",
    bot_name: str = "FUTURES",
) -> None:
    """Close anything still in ``futures_state`` / ``trades.json`` directly.

    SCOPED to ``bot_name`` (default FUTURES). futures_state is shared with the
    CROSS bot, so without scoping this closed CROSS's (SIM!) positions and
    mis-attributed them to FUTURES in the trades table.

    Behaviour is **identical** for SIM and LIVE:

    * Live current price via ``ccxt.fetch_ticker`` per position
    * PnL computed as ``margin  leverage  price_move%``
    * ``save_trade_db``  row shows up in "Realized Profit"
    * LIVE-only: ``reduceOnly`` market order on the exchange
    * State cleanup (``futures_state`` table + ``trades.json``)

    ``log(severity, msg)`` is called for each step; pass in a function
    that marshals onto the Tk main thread.
    """
    try:
        from core.database import (get_connection, get_futures_state,  # type: ignore
                                   remove_futures_state, save_trade_db,
                                   remove_open_position)
    except Exception as e:
        log("error", f"DB-Import fehlgeschlagen: {e}")
        return

    # Per-bot margin mode (CROSS=cross, FUTURES=isolated). CROSS reduce-only
    # closes are rejected by MEXC if sent with the wrong margin mode.
    try:
        from bot_utils.config import read_bot_section
        _mm = read_bot_section(bot_name).get("MARGIN_MODE")
        margin_mode = str(_mm) if _mm else ("cross" if bot_name == "CROSS"
                                            else "isolated")
    except Exception:
        margin_mode = "cross" if bot_name == "CROSS" else "isolated"

    try:
        state_bot_name = f"{bot_name} (SIM)" if sim_only else bot_name
        positions = _get_futures_state_for_mode(
            get_futures_state, bot_name, sim_only)
    except Exception as e:
        log("error", f"futures_state nicht lesbar: {e}")
        positions = []

    try:
        trades_file = _state_path_for_mode(bot_name, sim_only)
    except Exception as e:
        log("error", str(e))
        return
    json_trades: dict = {}
    try:
        if os.path.exists(trades_file):
            with open(trades_file, "r", encoding="utf-8") as f:
                json_trades = json.load(f) or {}
    except Exception:
        pass

    # Union: ``futures_state`` has CURRENT values (from the monitor
    # thread), JSON has original data (entry_price, amount). Combine both.
    symbols_to_close: dict = {}
    for p in positions:
        sym = p.get("symbol")
        if not sym:
            continue
        jt = json_trades.get(sym) or {}  # fee keys come from JSON, not from futures_state
        symbols_to_close[sym] = {
            "symbol":        sym,
            "position_type": p.get("position_type", "LONG"),
            "entry_price":   float(p.get("entry_price", 0)),
            "current_price": float(p.get("current_price", 0)),
            # WICHTIG: the ``futures_state`` column is ``margin_usdt`` (not invested_usdt!)
            "margin_usdt":   float(p.get("margin_usdt", 0)),
            "leverage":      float(p.get("leverage", 3)),
            "unrealized_pnl": float(p.get("unrealized_pnl", 0)),
            "unrealized_pct": float(p.get("unrealized_pct", 0)),
            "liquidation_price": float(p.get("liquidation_price", 0)),
            "funding_paid":  float(p.get("funding_paid", 0.0)),
            "opened_at":     p.get("opened_at"),
            # amount + fee keys come from JSON  futures_state doesn't store them
            "amount":            float(jt.get("amount", 0)),
            "original_amount":   float(jt.get("original_amount",
                                              jt.get("amount", 0)) or 0),
            "partial_sold":      bool(jt.get("partial_sold")),
            "funding_booked_on_partials": jt.get("funding_booked_on_partials"),
            "initial_entry_fee": jt.get("initial_entry_fee"),
            "fees_paid":         jt.get("fees_paid"),
            "claim_release_pending": bool(jt.get("claim_release_pending")),
            "accounting_already_booked": bool(jt.get("accounting_already_booked")),
            "accounting_pending": bool(jt.get("accounting_pending")),
            "accounting_pending_reason": jt.get("accounting_pending_reason"),
            "accounting_pending_sell_price": jt.get("accounting_pending_sell_price"),
            "accounting_pending_sell_time": jt.get("accounting_pending_sell_time"),
            "accounting_pending_profit_pct": jt.get("accounting_pending_profit_pct"),
            "accounting_pending_profit_usdt": jt.get("accounting_pending_profit_usdt"),
            "accounting_pending_fees_usdt": jt.get("accounting_pending_fees_usdt"),
            "accounting_pending_funding_paid": jt.get("accounting_pending_funding_paid"),
            "accounting_pending_exchange_order_id": jt.get("accounting_pending_exchange_order_id"),
            "accounting_pending_mfe_pct": jt.get("accounting_pending_mfe_pct"),
            "accounting_pending_mae_pct": jt.get("accounting_pending_mae_pct"),
            "accounting_pending_giveback_pct": jt.get("accounting_pending_giveback_pct"),
            "_state_source": "futures_state",
        }

    for sym, j in json_trades.items():
        if sym not in symbols_to_close:
            # Accept legacy ("buy") or StateManager ("buy_price"). last_price
            # defaults to 0 when Monitor hasn't ticked yet.
            _buy = float(j.get("buy_price") or j.get("buy") or 0)
            _last = float(j.get("last_price") or 0)
            symbols_to_close[sym] = {
                "symbol":        sym,
                "position_type": j.get("position_type", "LONG"),
                "entry_price":   _buy,
                "current_price": _last if _last > 0 else _buy,
                "margin_usdt":   float(j.get("invested_usdt", 0)),
                "leverage":      float(j.get("leverage", 3)),
                "unrealized_pnl": 0.0,
                "unrealized_pct": 0.0,
                "liquidation_price": float(j.get("liquidation_price", 0)),
                "funding_paid":  float(j.get("funding_paid", 0.0)),
                "opened_at":     j.get("buy_time") or j.get("opened_at"),
                "amount":            float(j.get("amount", 0)),
                "original_amount":   float(j.get("original_amount",
                                                  j.get("amount", 0)) or 0),
                "partial_sold":      bool(j.get("partial_sold")),
                "funding_booked_on_partials": j.get("funding_booked_on_partials"),
                "initial_entry_fee": j.get("initial_entry_fee"),
                "fees_paid":         j.get("fees_paid"),
                "claim_release_pending": bool(j.get("claim_release_pending")),
                "accounting_already_booked": bool(j.get("accounting_already_booked")),
                "accounting_pending": bool(j.get("accounting_pending")),
                "accounting_pending_reason": j.get("accounting_pending_reason"),
                "accounting_pending_sell_price": j.get("accounting_pending_sell_price"),
                "accounting_pending_sell_time": j.get("accounting_pending_sell_time"),
                "accounting_pending_profit_pct": j.get("accounting_pending_profit_pct"),
                "accounting_pending_profit_usdt": j.get("accounting_pending_profit_usdt"),
                "accounting_pending_fees_usdt": j.get("accounting_pending_fees_usdt"),
                "accounting_pending_funding_paid": j.get("accounting_pending_funding_paid"),
                "accounting_pending_exchange_order_id": j.get("accounting_pending_exchange_order_id"),
                "accounting_pending_mfe_pct": j.get("accounting_pending_mfe_pct"),
                "accounting_pending_mae_pct": j.get("accounting_pending_mae_pct"),
                "accounting_pending_giveback_pct": j.get("accounting_pending_giveback_pct"),
                "_state_source": "json",
            }

    if not symbols_to_close:
        return  # nothing to close  bot handled everything

    # Exchange connection for current prices (same for SIM and LIVE)
    ex = None
    try:
        from config.exchange_config import get_futures_exchange_connection  # type: ignore
        ex = get_futures_exchange_connection()
    except Exception as e:
        log("warn", f"Exchange nicht erreichbar  nutze gespeicherte Werte: {e}")

    closed = 0
    total_pnl = 0.0
    failed_syms: list = []  # track close failures so we don't wipe their state
    failed_position_updates: dict = {}

    # MEXC rate-limit guard: with multiple positions to close, sending the
    # market orders in tight sequence (<1s apart) triggers code 510
    # "Requests are too frequent"  leaving the later positions OPEN on the
    # exchange. We import time here only when actually needed.
    import time as _close_time
    _last_close_at = 0.0
    _MIN_CLOSE_SPACING_SEC = 0.6   # >500ms between MEXC orders is safe

    def _json_state_from_futures_row(row: dict) -> dict:
        entry = float(row.get("entry_price") or 0.0)
        current = float(row.get("current_price") or entry or 0.0)
        margin = float(row.get("margin_usdt") or 0.0)
        amount = float(row.get("amount") or 0.0)
        out = {
            "buy": entry,
            "buy_price": entry,
            "last_price": current,
            "highest": current,
            "amount": amount,
            "original_amount": float(row.get("original_amount") or amount or 0.0),
            "invested_usdt": margin,
            "position_type": row.get("position_type", "LONG"),
            "leverage": float(row.get("leverage") or 1.0),
            "liquidation_price": float(row.get("liquidation_price") or 0.0),
            "funding_paid": float(row.get("funding_paid") or 0.0),
            "buy_time": row.get("opened_at") or _utc_now_str(),
            "partial_sold": bool(row.get("partial_sold")),
            "closing_retry_pending": True,
            "closing_retry_reason": reason,
        }
        for key in ("initial_entry_fee", "fees_paid",
                    "funding_booked_on_partials"):
            if row.get(key) is not None:
                out[key] = row.get(key)
        return out

    def _trade_already_booked(sym: str, row: dict) -> bool:
        buy_time = str(row.get("opened_at") or row.get("buy_time") or "")
        if not buy_time:
            return False
        try:
            conn = get_connection()
            found = conn.execute("""
                SELECT 1 FROM trades
                 WHERE bot_name=?
                   AND symbol=?
                   AND buy_time=?
                   AND COALESCE(is_partial,0)=0
                   AND COALESCE(is_futures,0)=1
                 LIMIT 1
            """, (state_bot_name, sym, buy_time)).fetchone()
            return bool(found)
        except Exception:
            return False

    try:
        from core.symbol_locks import close_lock as _close_lock
    except Exception:
        _close_lock = None

    for sym, p in symbols_to_close.items():
        if _close_lock is not None:
            _cl_ctx = _close_lock(sym, timeout=2.0, bot_name=bot_name)
            _cl_got = _cl_ctx.__enter__()
            if not _cl_got:
                log("warn", f"{sym}: close skipped  lock held by another process")
                failed_syms.append(sym)
                _cl_ctx.__exit__(None, None, None)
                continue
        else:
            _cl_ctx = None
            _cl_got = True
        try:
            if p.get("claim_release_pending") and not sim_only:
                try:
                    if remove_open_position(bot_name, sym) is False:
                        failed_syms.append(sym)
                        failed_position_updates[sym] = dict(p)
                        log("error",
                            f"{sym}: claim release still pending - state kept")
                        continue
                except Exception as e:
                    failed_syms.append(sym)
                    failed_position_updates[sym] = dict(p)
                    log("error", f"{sym}: claim release retry raised: {e}")
                    continue
                try:
                    remove_futures_state(sym, bot_name, sim_only)
                except Exception:
                    pass
                closed += 1
                log("win", f"{sym}: claim cleanup completed")
                continue

            if p.get("accounting_pending"):
                pending_kwargs = dict(
                    bot_name=bot_name,
                    symbol=sym,
                    buy_price=float(p.get("entry_price") or p.get("buy") or 0.0),
                    sell_price=float(p.get("accounting_pending_sell_price")
                                     or p.get("current_price")
                                     or p.get("entry_price") or 0.0),
                    buy_time=p.get("opened_at") or p.get("buy_time") or _utc_now_str(),
                    sell_time=p.get("accounting_pending_sell_time") or _utc_now_str(),
                    profit_pct=float(p.get("accounting_pending_profit_pct") or 0.0),
                    profit_usdt=float(p.get("accounting_pending_profit_usdt") or 0.0),
                    invested_usdt=float(p.get("margin_usdt")
                                        or p.get("invested_usdt") or 0.0),
                    reason=p.get("accounting_pending_reason") or reason,
                    is_futures=True,
                    position_type=p.get("position_type", "LONG"),
                    leverage=float(p.get("leverage") or 1.0),
                    liquidation_price=float(p.get("liquidation_price") or 0.0),
                    funding_paid=float(p.get("accounting_pending_funding_paid") or 0.0),
                    fees_usdt=float(p.get("accounting_pending_fees_usdt") or 0.0),
                    exchange_order_id=p.get("accounting_pending_exchange_order_id"),
                    mfe_pct=p.get("accounting_pending_mfe_pct"),
                    mae_pct=p.get("accounting_pending_mae_pct"),
                    giveback_pct=p.get("accounting_pending_giveback_pct"),
                    mode_is_sim=sim_only,
                )
                try:
                    saved_ok = bool(save_trade_db(**pending_kwargs))
                except Exception as e:
                    saved_ok = False
                    log("error", f"{sym}: pending futures DB retry raised: {e}")
                if not saved_ok:
                    failed_syms.append(sym)
                    failed_position_updates[sym] = dict(p)
                    continue
                if not sim_only:
                    try:
                        if remove_open_position(bot_name, sym) is False:
                            failed_syms.append(sym)
                            failed_position_updates[sym] = dict(p)
                            log("error",
                                f"{sym}: claim release failed after pending DB retry")
                            continue
                    except Exception as e:
                        failed_syms.append(sym)
                        failed_position_updates[sym] = dict(p)
                        log("error",
                            f"{sym}: claim release raised after pending DB retry: {e}")
                        continue
                try:
                    remove_futures_state(sym, bot_name, sim_only)
                except Exception:
                    pass
                closed += 1
                total_pnl += pending_kwargs["profit_usdt"]
                log("win",
                    f"{sym}: pending futures accounting booked "
                    f"({pending_kwargs['profit_usdt']:+.2f} USDT)")
                continue

            pos_type = p["position_type"]
            entry    = p["entry_price"]
            margin   = p["margin_usdt"]
            lev      = p["leverage"]
            amount   = abs(float(p["amount"]))
            original_amount = float(p.get("original_amount") or amount or 0.0)
            partial_sold = bool(p.get("partial_sold"))
            symbol_full = f"{sym}/USDT:USDT"

            if _trade_already_booked(sym, p):
                if not sim_only:
                    try:
                        from bot_utils.futures_order import verify_position_closed
                        _closed, _remaining = verify_position_closed(
                            ex, symbol_full, timeout=5.0)
                    except Exception as e:
                        failed_syms.append(sym)
                        failed_position_updates[sym] = dict(p)
                        log("error",
                            f"{sym}: trade already booked but flat check failed "
                            f"({e}) - state kept")
                        continue
                    if not _closed:
                        failed_syms.append(sym)
                        failed_position_updates[sym] = dict(p)
                        log("error",
                            f"{sym}: trade already booked but exchange still "
                            f"shows {_remaining} contracts - manual review")
                        continue
                    try:
                        if remove_open_position(bot_name, sym) is False:
                            retry_state = dict(p)
                            retry_state["claim_release_pending"] = True
                            retry_state["accounting_already_booked"] = True
                            failed_syms.append(sym)
                            failed_position_updates[sym] = retry_state
                            log("error",
                                f"{sym}: stale state found but claim release failed")
                            continue
                    except Exception as e:
                        retry_state = dict(p)
                        retry_state["claim_release_pending"] = True
                        retry_state["accounting_already_booked"] = True
                        failed_syms.append(sym)
                        failed_position_updates[sym] = retry_state
                        log("error", f"{sym}: stale state claim cleanup raised: {e}")
                        continue
                try:
                    remove_futures_state(sym, bot_name, sim_only)
                except Exception:
                    pass
                log("win", f"{sym}: stale state cleaned (trade already booked)")
                closed += 1
                continue

            if not sim_only and ex is None:
                log("error",
                    f"{sym}: LIVE close skipped - exchange unavailable. "
                    f"Position KEPT; close manually if needed.")
                failed_syms.append(sym)
                continue

            # Live price for accurate PnL (same for SIM and LIVE)
            curr = p["current_price"]
            if ex is not None:
                try:
                    t = ex.fetch_ticker(symbol_full)
                    fresh = float(t.get("last") or t.get("close") or 0)
                    if fresh > 0:
                        curr = fresh
                except Exception:
                    pass
            if curr <= 0:
                curr = entry  # last resort

            # Defaults ALWAYS set so the else-branch + later save_trade_db
            # can never hit UnboundLocalError on these locals.
            taker_fee  = 0.00055
            notional   = margin * lev if (margin > 0 and lev > 0) else 0.0
            try:
                from bot_utils import (futures_contract_size,
                                       safe_remaining_funding,
                                       safe_proportional_fee)
                contract_size = float(futures_contract_size(ex, symbol_full) or 1.0)
            except Exception:
                from bot_utils import safe_remaining_funding, safe_proportional_fee
                contract_size = 1.0
            initial_entry_fee = float(p.get("initial_entry_fee") or
                                      p.get("fees_paid") or
                                      (notional * taker_fee))
            entry_fee = safe_proportional_fee(
                initial_entry_fee, amount, original_amount,
                partial_sold=partial_sold,
            )
            funding_for_close = safe_remaining_funding(
                float(p.get("funding_paid", 0.0) or 0.0),
                amount, original_amount,
                partial_sold=partial_sold,
                booked_on_partials=float(
                    p.get("funding_booked_on_partials") or 0.0),
            )
            if amount > 0 and curr > 0:
                exit_fee = amount * contract_size * curr * taker_fee
            else:
                exit_fee = notional * taker_fee
            entry_fee, exit_fee, funding_for_close = _clamp_sim_cross_close_costs(
                bot_name=bot_name,
                sim_only=sim_only,
                notional=notional,
                taker_fee=taker_fee,
                entry_fee=entry_fee,
                exit_fee=exit_fee,
                funding_for_close=funding_for_close,
            )
            price_move = 0.0
            pnl_usdt   = 0.0

            # Compute PnL
            if entry > 0 and margin > 0:
                if pos_type == "LONG":
                    price_move = (curr - entry) / entry * 100.0
                else:  # SHORT
                    price_move = (entry - curr) / entry * 100.0
                pnl_usdt = round(
                    notional * (price_move / 100.0)
                    - entry_fee - exit_fee
                    - funding_for_close,
                    2
                )
            else:
                # Fallback: pre-computed unrealized from the bot. Inputs
                # were not valid for live recalc (margin or entry was
                # 0/missing), so trust whatever the bot last stored.
                log("warn",
                    f"{sym}: cannot recompute close PnL "
                    f"(entry={entry}, margin={margin}, lev={lev})  "
                    f"using bot-recorded values")
                price_move = float(p.get("unrealized_pct") or 0.0)
                pnl_usdt   = round(float(p.get("unrealized_pnl") or 0.0), 2)

            # LIVE-only: reduceOnly market order on the exchange
            if not sim_only and ex is not None and amount > 0:
                try:
                    close_side = "sell" if pos_type == "LONG" else "buy"
                    # Round to exchange precision  Bitget/Binance reject
                    # close orders with too many decimals (InvalidOrder).
                    try:
                        close_amt = float(ex.amount_to_precision(
                            f"{sym}/USDT:USDT", amount))
                    except Exception:
                        close_amt = amount
                    # Build reduce-only params via the SINGLE source of truth
                    # (exchange_config.reduce_only_params) so this matches the
                    # bot's own close paths exactly: correct per-bot margin_mode
                    # (cross for CROSS, isolated for FUTURES), the leverage param
                    # MEXC requires on margin orders, hedge-mode positionSide,
                    # and OKX tdMode.
                    from config.exchange_config import reduce_only_params
                    _lev_int = None
                    try:
                        import math as _math
                        _lev_int = max(1, int(_math.ceil(float(lev))))
                    except (ValueError, TypeError):
                        pass
                    close_params = reduce_only_params(
                        position_side=("long" if pos_type == "LONG" else "short"),
                        margin_mode=margin_mode,
                        leverage=_lev_int,
                    )

                    # Rate-limit guard: keep 0.6s between close orders to
                    # avoid MEXC code 510. The wait only ticks if the previous
                    # close was very recent  first close has no delay.
                    _since = _close_time.monotonic() - _last_close_at
                    if _last_close_at > 0 and _since < _MIN_CLOSE_SPACING_SEC:
                        _close_time.sleep(_MIN_CLOSE_SPACING_SEC - _since)

                    # Retry loop for rate-limit errors. Other exceptions
                    # propagate to the outer 'except' as before.
                    order = None
                    _last_rate_err: Exception | None = None
                    for _rl_attempt in range(3):  # 3 tries total
                        try:
                            order = ex.create_order(
                                symbol_full, "market", close_side,
                                close_amt, params=close_params,
                            )
                            break  # success
                        except Exception as _e:
                            _msg = str(_e).lower()
                            _is_rate = (
                                "too frequent" in _msg
                                or '"code":510' in _msg
                                or "code 510" in _msg
                                or "429" in _msg
                                or "too many requests" in _msg
                            )
                            if not _is_rate:
                                raise  # non-rate-limit  outer handler
                            _last_rate_err = _e
                            # Exponential backoff: 1s, 2s, 4s
                            _close_time.sleep(2 ** _rl_attempt)
                    if order is None:
                        # All 3 attempts hit rate-limit  give up cleanly
                        raise _last_rate_err or RuntimeError("rate-limit retry exhausted")
                    _last_close_at = _close_time.monotonic()

                    # Pull real fill price + exit fee from the order
                    if isinstance(order, dict):
                        for k in ("average", "price"):
                            v = order.get(k)
                            if v is not None:
                                try:
                                    fv = float(v)
                                    if fv > 0:
                                        curr = fv
                                        break
                                except (TypeError, ValueError):
                                    continue
                        try:
                            fee_obj = order.get("fee") or {}
                            fc_cost = float(fee_obj.get("cost", 0) or 0)
                            if fc_cost > 0:
                                exit_fee = fc_cost
                        except Exception:
                            pass
                        # Recompute PnL with the REAL fill + REAL exit fee
                        if pos_type == "LONG":
                            price_move = (curr - entry) / entry * 100.0
                        else:
                            price_move = (entry - curr) / entry * 100.0
                        pnl_usdt = round(
                            notional * (price_move / 100.0)
                            - entry_fee - exit_fee
                            - funding_for_close,
                            2
                        )

                    # Parity with the bot's own close paths: confirm the
                    # position is actually FLAT before booking + wiping state.
                    # A partial fill (routine on thin alt books) would otherwise
                    # leave an ORPHAN on the exchange while the bot believes it
                    # closed.
                    try:
                        from bot_utils.futures_order import verify_position_closed
                        _closed, _remaining = verify_position_closed(
                            ex, symbol_full, timeout=5.0)
                    except Exception as _ve:
                        _closed, _remaining = False, -1.0
                        log("warn", f"[LIVE] {sym}: close unverified ({_ve})  "
                                    f"keeping in state")
                    if not _closed:
                        log("error",
                            f"[LIVE] {sym}: close NOT confirmed "
                            f"(remaining {_remaining})  position KEPT, "
                            f"close manually!")
                        failed_syms.append(sym)
                        continue
                    log("win", f"[LIVE] {sym} ({pos_type}) closed @ {curr:.4f}")
                except Exception as e:
                    log("error",
                        f"[LIVE] {sym}: close FAILED: {e}  close manually!")
                    failed_syms.append(sym)
                    continue

            # LIVE safety (orphan prevention): if amount is unknown (0) we
            # could NOT have placed the reduce-only order above. Writing a
            # 'closed' DB row + wiping futures_state here would make the bot
            # forget a position that is STILL OPEN on the exchange. Keep it
            # in state and tell the user to close manually instead.
            if not sim_only and amount <= 0:
                try:
                    from bot_utils.futures_order import verify_position_closed
                    _closed, _remaining = verify_position_closed(
                        ex, symbol_full, timeout=5.0)
                except Exception as e:
                    _closed, _remaining = False, -1.0
                    log("error",
                        f"{sym}: LIVE close skipped - amount unknown and flat "
                        f"check failed ({e}). Position KEPT; review manually.")
                if _closed:
                    try:
                        if remove_open_position(bot_name, sym) is False:
                            failed_syms.append(sym)
                            failed_position_updates[sym] = _json_state_from_futures_row(p)
                            log("error",
                                f"{sym}: stale amount=0 state but claim release failed")
                            continue
                    except Exception as e:
                        failed_syms.append(sym)
                        failed_position_updates[sym] = _json_state_from_futures_row(p)
                        log("error",
                            f"{sym}: stale amount=0 claim cleanup raised: {e}")
                        continue
                    try:
                        remove_futures_state(sym, bot_name, sim_only)
                    except Exception:
                        pass
                    closed += 1
                    log("win", f"{sym}: stale amount=0 state cleaned (flat)")
                    continue
                log("error",
                    f"{sym}: LIVE close skipped  position amount unknown (0). "
                    f"Exchange remaining={_remaining}; close MANUALLY!")
                failed_syms.append(sym)
                failed_position_updates[sym] = _json_state_from_futures_row(p)
                continue

            # Write to DB  shows up in "Realized Profit"
            buy_time = p.get("opened_at") or _utc_now_str()
            sell_time = _utc_now_str()
            saved_ok = save_trade_db(
                bot_name=bot_name, symbol=sym,
                buy_price=entry, sell_price=curr,
                buy_time=buy_time, sell_time=sell_time,
                profit_pct=price_move, profit_usdt=pnl_usdt,
                invested_usdt=margin,
                reason=reason,
                is_futures=True, position_type=pos_type,
                leverage=lev,
                liquidation_price=p["liquidation_price"],
                funding_paid=funding_for_close,
                fees_usdt=entry_fee + exit_fee,
                mode_is_sim=sim_only,
            )
            if saved_ok is False:
                log("error",
                    f"{sym}: DB trade save failed - state/claim KEPT for review")
                failed_syms.append(sym)
                retry_state = _json_state_from_futures_row(p)
                retry_state.update({
                    "accounting_pending": True,
                    "accounting_pending_reason": reason,
                    "accounting_pending_sell_price": curr,
                    "accounting_pending_sell_time": sell_time,
                    "accounting_pending_profit_pct": price_move,
                    "accounting_pending_profit_usdt": pnl_usdt,
                    "accounting_pending_fees_usdt": entry_fee + exit_fee,
                    "accounting_pending_funding_paid": funding_for_close,
                })
                failed_position_updates[sym] = retry_state
                continue
            try:
                # Release the shared multi-bot claim. The launcher closes
                # OUTSIDE TradeState (separate process), so it must clear
                # bot_open_positions itself  otherwise the coin stays
                # "claimed" forever.
                if not sim_only and remove_open_position(bot_name, sym) is False:
                    failed_syms.append(sym)
                    retry_state = _json_state_from_futures_row(p)
                    retry_state["claim_release_pending"] = True
                    retry_state["accounting_already_booked"] = True
                    failed_position_updates[sym] = retry_state
                    log("error",
                        f"{sym}: claim release failed - state kept for review")
                    continue
            except Exception as e:
                failed_syms.append(sym)
                retry_state = _json_state_from_futures_row(p)
                retry_state["claim_release_pending"] = True
                retry_state["accounting_already_booked"] = True
                failed_position_updates[sym] = retry_state
                log("error", f"{sym}: claim release raised: {e}")
                continue
            try:
                # Scope by bot: FUTURES + CROSS share futures_state; an
                # unscoped delete would wipe the other bot's dashboard row
                # for the same base coin.
                remove_futures_state(sym, bot_name, sim_only)
            except Exception:
                pass

            closed += 1
            total_pnl += pnl_usdt
            log("win",
                f"Closed {sym} ({pos_type}): {pnl_usdt:+.2f} USDT ({price_move:+.2f}%)")
        except Exception as e:
            failed_syms.append(sym)
            log("error", f"Close {sym} failed: {e}")
        finally:
            if _cl_ctx is not None:
                try:
                    _cl_ctx.__exit__(None, None, None)
                except Exception:
                    pass
                _cl_ctx = None

    # Clean trades.json  but ONLY for positions we actually closed.
    # Wiping the whole file when some closes failed would mean those
    # positions stay open on the exchange while the bot loses all memory
    # of them  permanent orphans.
    try:
        remaining: dict = {}
        if failed_syms:
            cur = {}
            try:
                with open(trades_file, encoding="utf-8") as fr:
                    cur = json.load(fr) or {}
            except Exception as e:
                log("warn", f"State re-read failed during cleanup: {e}")
            for sym in failed_syms:
                if sym in failed_position_updates:
                    remaining[sym] = failed_position_updates[sym]
                elif sym in cur:
                    remaining[sym] = cur[sym]
                elif sym in symbols_to_close:
                    remaining[sym] = _json_state_from_futures_row(
                        symbols_to_close[sym])
        from bot_utils.state_persist import atomic_save_json
        if not atomic_save_json(trades_file, remaining):
            log("error", "State cleanup write failed - stale positions may remain")
        if failed_syms:
            log("warn",
                f" Kept {len(failed_syms)} position(s) in state due to "
                f"close failures: {', '.join(failed_syms)}  manual review needed")
    except Exception as e:
        log("error", f"State cleanup write raised: {e}")

    sev = "warn" if "Emergency" in reason else "system"
    log(sev,
        f" {closed} position(s) closed  Total realized: {total_pnl:+.2f} USDT")


#  Spot  direct close (fallback when bot's own handler couldn't) 

def direct_close_remaining_spot(
    bot_name: str,
    log,
    sim_only: bool,
    reason: str = "Manual Stop",
) -> None:
    """Spot equivalent of :func:`direct_close_remaining_futures`.

    Anything still in ``{log_dir}/trades.json`` after a graceful shutdown
    is closed here and persisted to the DB. Same SIM/LIVE code path.
    """
    try:
        trades_file = _state_path_for_mode(bot_name, sim_only)
    except Exception as e:
        log("error", str(e))
        return
    state_bot_name = f"{bot_name} (SIM)" if sim_only else bot_name
    try:
        from core.database import save_trade_db, remove_open_position  # type: ignore
    except Exception as e:
        log("error", f"DB import failed: {e}")
        return

    json_trades: dict = {}
    try:
        if os.path.exists(trades_file):
            with open(trades_file, "r", encoding="utf-8") as f:
                json_trades = json.load(f) or {}
    except Exception:
        pass

    if not json_trades:
        return  # nothing to clean up

    ex = None
    if not sim_only:
        try:
            from config.exchange_config import get_spot_exchange_connection  # type: ignore
            ex = get_spot_exchange_connection()
        except Exception as e:
            log("warn", f"Exchange unreachable - using stored values: {e}")

    closed = 0
    total_pnl = 0.0
    failed_syms: list = []
    failed_position_updates: dict = {}

    try:
        from core.symbol_locks import close_lock as _close_lock_spot
    except Exception:
        _close_lock_spot = None

    for sym, d in list(json_trades.items()):
        if _close_lock_spot is not None:
            _cl_ctx_s = _close_lock_spot(sym, timeout=2.0, bot_name=bot_name)
            _cl_got_s = _cl_ctx_s.__enter__()
            if not _cl_got_s:
                log("warn", f"{sym}: close skipped  lock held by another process")
                failed_syms.append(sym)
                _cl_ctx_s.__exit__(None, None, None)
                continue
        else:
            _cl_ctx_s = None
            _cl_got_s = True
        try:
            # Accept legacy ("buy") or StateManager ("buy_price").
            buy_price = float(d.get("buy_price") or d.get("buy") or 0)
            margin    = float(d.get("invested_usdt", 0))
            amount    = float(d.get("amount", 0))

            pending_partials = list(d.get("accounting_pending_partials") or [])
            if pending_partials:
                remaining_pending = []
                for item in pending_partials:
                    retry_item = dict(item)
                    retry_item.setdefault("mode_is_sim", sim_only)
                    try:
                        ok = bool(save_trade_db(**retry_item))
                    except Exception as e:
                        ok = False
                        log("error", f"{sym}: pending partial DB retry raised: {e}")
                    if not ok:
                        remaining_pending.append(item)
                if remaining_pending:
                    keep = dict(d)
                    keep["accounting_pending_partials"] = remaining_pending
                    failed_syms.append(sym)
                    failed_position_updates[sym] = keep
                    continue
                d = dict(d)
                d["accounting_pending_partials"] = []

            if d.get("accounting_pending"):
                pending_kwargs = dict(
                    bot_name=bot_name, symbol=sym,
                    buy_price=buy_price,
                    sell_price=float(d.get("accounting_pending_sell_price") or buy_price),
                    buy_time=d.get("buy_time") or _utc_now_str(),
                    sell_time=d.get("accounting_pending_sell_time") or _utc_now_str(),
                    profit_pct=float(d.get("accounting_pending_profit_pct") or 0.0),
                    profit_usdt=float(d.get("accounting_pending_profit_usdt") or 0.0),
                    invested_usdt=margin,
                    reason=d.get("accounting_pending_reason") or reason,
                    is_futures=False,
                    fees_usdt=float(d.get("accounting_pending_fees_usdt") or 0.0),
                    mode_is_sim=sim_only,
                )
                try:
                    saved_ok = bool(save_trade_db(**pending_kwargs))
                except Exception as e:
                    saved_ok = False
                    log("error", f"{sym}: pending full DB retry raised: {e}")
                if not saved_ok:
                    failed_syms.append(sym)
                    failed_position_updates[sym] = dict(d)
                    continue
                try:
                    if not sim_only and remove_open_position(bot_name, sym) is False:
                        failed_syms.append(sym)
                        failed_position_updates[sym] = dict(d)
                        log("error",
                            f"{sym}: claim release failed - state kept for review")
                        continue
                except Exception as e:
                    failed_syms.append(sym)
                    failed_position_updates[sym] = dict(d)
                    log("error", f"{sym}: claim release raised: {e}")
                    continue
                closed += 1
                total_pnl += pending_kwargs["profit_usdt"]
                log("win",
                    f"Closed {sym}: {pending_kwargs['profit_usdt']:+.2f} USDT "
                    f"({pending_kwargs['profit_pct']:+.2f}%)")
                continue

            if not sim_only and ex is None:
                log("error",
                    f"{sym}: LIVE sell skipped - exchange unavailable. "
                    f"Position KEPT; sell manually if needed.")
                failed_syms.append(sym)
                continue
            if not sim_only and amount <= 0:
                log("error",
                    f"{sym}: LIVE sell skipped - amount unknown (0). "
                    f"Position KEPT; sell manually if needed.")
                failed_syms.append(sym)
                continue

            try:
                stored_price = float(d.get("last_price")
                                     or d.get("current_price") or 0.0)
            except (TypeError, ValueError):
                stored_price = 0.0
            if stored_price <= 0 and sim_only:
                try:
                    stored_price = float(d.get("highest")
                                         or d.get("highest_price") or 0.0)
                except (TypeError, ValueError):
                    stored_price = 0.0
            curr = stored_price if stored_price > 0 else buy_price
            if ex is not None:
                try:
                    t = ex.fetch_ticker(f"{sym}/USDT")
                    fresh = float(t.get("last") or t.get("close") or 0)
                    if fresh > 0:
                        curr = fresh
                except Exception:
                    pass

            profit_pct  = ((curr - buy_price) / buy_price * 100) if buy_price > 0 else 0.0
            # Taker fee on entry AND exit (mirrors the bot logic)
            taker_fee   = 0.001   # Spot: 0.1% per side (standard taker)
            initial_entry_fee = float(d.get(
                "initial_entry_fee", d.get("fees_paid", margin * taker_fee)
            ) or 0.0)
            try:
                from bot_utils import safe_proportional_fee
                entry_fee = safe_proportional_fee(
                    initial_entry_fee,
                    amount,
                    float(d.get("original_amount", amount) or 0.0),
                    partial_sold=bool(d.get("partial_sold")),
                )
            except Exception:
                entry_fee = initial_entry_fee if not d.get("partial_sold") else 0.0
            exit_fee = (
                amount * curr * taker_fee
                if amount > 0 and curr > 0 else margin * taker_fee
            )
            profit_usdt = round(
                amount * (curr - buy_price) - entry_fee - exit_fee, 2
            ) if amount > 0 and buy_price > 0 else (
                round(margin * (profit_pct / 100) - entry_fee - exit_fee, 2)
                if margin > 0 else 0.0
            )

            # LIVE: market-sell the holding
            fill_price = curr        # default if order doesn't return fill
            close_fee  = 0.0
            partial_live_fill = False
            requested_amount = amount
            filled_amount = amount
            if not sim_only and ex is not None and amount > 0:
                try:
                    # Use the precision/lot-size-aware wrapper the BOT uses
                    # (spot_market_sell_safe) instead of a raw market sell:
                    # a raw create_market_sell_order rejects coins with batch
                    # lot sizes > 1 (many meme-coin listings). The wrapper
                    # rounds + retries across lot steps; on a genuine failure
                    # it still raises, so the except below keeps the position
                    # in state.
                    from bot_utils import spot_market_sell_safe
                    order, _sold = spot_market_sell_safe(ex, f"{sym}/USDT", amount)
                    filled_amount = _filled_base_amount(order, _sold, amount)
                    # Phantom-fill guard, same as the bot's own exit paths.
                    # spot_market_sell_safe returns the order the instant it's
                    # accepted; MEXC can hand back a status=new/filled=0 order
                    # that never executed. On non-fill: keep the position and
                    # tell the user to sell manually, rather than booking a
                    # phantom sell while the coins stay in the wallet.
                    from bot_utils.order_utils import order_was_filled
                    if not order_was_filled(order, filled_amount):
                        _st = order.get("status") if isinstance(order, dict) else "?"
                        log("error",
                            f"[LIVE] {sym}: sell did NOT fill (status={_st})  "
                            f"position KEPT, sell manually on the exchange!")
                        failed_syms.append(sym)
                        continue
                    # Capture real fill price + fee from the order response.
                    # Emergency closes often run during volatility where
                    # slippage is largest and the difference matters most
                    # for the DB row to be useful for performance analysis.
                    if isinstance(order, dict):
                        for k in ("average", "price"):
                            v = order.get(k)
                            if v is not None:
                                try:
                                    fv = float(v)
                                    if fv > 0:
                                        fill_price = fv
                                        break
                                except (TypeError, ValueError):
                                    continue
                        try:
                            fee_obj = order.get("fee") or {}
                            fc = (fee_obj.get("currency") or "").upper()
                            fc_cost = float(fee_obj.get("cost", 0) or 0)
                            # Convert fee to USDT (see ``_convert_fee_to_usdt``
                            # in main_bot_balanced.py for full rationale).
                            if not fc or fc in ("USDT", "USD", "BUSD", "USDC", "FDUSD"):
                                close_fee = abs(fc_cost)
                            elif fc == sym.upper() and fill_price > 0:
                                # Base-coin fee: convert via fill price
                                close_fee = abs(fc_cost) * fill_price
                            else:
                                # Unknown discount token (BNB/BGB/KCS/OKB) 
                                # can't convert without external price feed
                                close_fee = 0.0
                        except (TypeError, ValueError):
                            close_fee = 0.0
                    # Recompute PnL with real fill + real fee
                    residual_amount = max(0.0, requested_amount - filled_amount)
                    if residual_amount * fill_price > _LIVE_RESIDUAL_DUST_USDT:
                        partial_live_fill = True
                    if partial_live_fill:
                        sold_ratio = filled_amount / requested_amount if requested_amount > 0 else 0.0
                        margin = margin * sold_ratio
                        entry_fee = entry_fee * sold_ratio
                        amount = filled_amount
                    real_pct = ((fill_price - buy_price) / buy_price * 100
                                if buy_price > 0 else 0.0)
                    profit_pct = real_pct
                    profit_usdt = round(
                        margin * (real_pct / 100) - entry_fee - close_fee, 2
                    ) if margin > 0 else 0.0
                    log("win",
                        f"[LIVE] Sold {sym} @ {fill_price:.6f} "
                        f"(fee {close_fee:.4f})  {profit_usdt:+.2f} USDT")
                except Exception as e:
                    log("error",
                        f"[LIVE] {sym}: sell FAILED: {e}  sell manually!")
                    failed_syms.append(sym)
                    continue

            buy_time = d.get("buy_time") or _utc_now_str()
            sell_time = _utc_now_str()
            trade_kwargs = dict(
                bot_name=bot_name, symbol=sym,
                buy_price=buy_price, sell_price=fill_price,
                buy_time=buy_time, sell_time=sell_time,
                profit_pct=profit_pct, profit_usdt=profit_usdt,
                invested_usdt=margin,
                reason=(f"{reason} (partial fill)"
                        if partial_live_fill else reason),
                is_futures=False,
                is_partial=partial_live_fill,
                fees_usdt=entry_fee + (close_fee if not sim_only and amount > 0 else exit_fee),
                mode_is_sim=sim_only,
            )
            try:
                saved_ok = save_trade_db(**trade_kwargs)
            except Exception as e:
                saved_ok = False
                log("error", f"{sym}: DB trade save raised after sell: {e}")
            if saved_ok is False:
                log("error",
                    f"{sym}: DB trade save failed - state/claim KEPT for review")
                if partial_live_fill:
                    residual = dict(d)
                    residual["amount"] = max(
                        0.0, float(d.get("amount", 0) or 0) - amount)
                    residual["invested_usdt"] = max(
                        0.0, float(d.get("invested_usdt", 0) or 0) - margin)
                    residual["fees_paid"] = float(d.get("fees_paid", 0) or 0) + close_fee
                    pending = list(residual.get("accounting_pending_partials") or [])
                    pending.append(trade_kwargs)
                    residual["accounting_pending_partials"] = pending
                    failed_position_updates[sym] = residual
                elif not sim_only and amount > 0:
                    residual = dict(d)
                    residual.update({
                        "accounting_pending": True,
                        "accounting_pending_reason": reason,
                        "accounting_pending_sell_price": fill_price,
                        "accounting_pending_sell_time": sell_time,
                        "accounting_pending_profit_pct": profit_pct,
                        "accounting_pending_profit_usdt": profit_usdt,
                        "accounting_pending_fees_usdt": trade_kwargs["fees_usdt"],
                    })
                    failed_position_updates[sym] = residual
                failed_syms.append(sym)
                continue
            if partial_live_fill:
                residual = dict(d)
                residual["amount"] = max(
                    0.0, float(d.get("amount", 0) or 0) - amount)
                residual["invested_usdt"] = max(
                    0.0, float(d.get("invested_usdt", 0) or 0) - margin)
                residual["fees_paid"] = float(d.get("fees_paid", 0) or 0) + close_fee
                residual["last_partial_fill_reason"] = reason
                failed_position_updates[sym] = residual
                failed_syms.append(sym)
                total_pnl += profit_usdt
                log("warn",
                    f"{sym}: partial fill booked; residual position kept in state")
                continue
            try:
                # Release the shared multi-bot claim (launcher closes outside
                # TradeState  must clear bot_open_positions itself).
                if not sim_only and remove_open_position(bot_name, sym) is False:
                    log("error",
                        f"{sym}: claim release failed - state kept for review")
                    failed_syms.append(sym)
                    failed_position_updates[sym] = dict(d)
                    continue
            except Exception as e:
                log("error", f"{sym}: claim release raised: {e}")
                failed_syms.append(sym)
                failed_position_updates[sym] = dict(d)
                continue
            closed += 1
            total_pnl += profit_usdt
            log("win",
                f"Closed {sym}: {profit_usdt:+.2f} USDT ({profit_pct:+.2f}%)")
        except Exception as e:
            failed_syms.append(sym)
            log("error", f"Close {sym} failed: {e}")
        finally:
            if _cl_ctx_s is not None:
                try:
                    _cl_ctx_s.__exit__(None, None, None)
                except Exception:
                    pass
                _cl_ctx_s = None

    # Clean trades.json  but keep failed positions so the bot doesn't
    # forget about positions still open on the exchange.
    try:
        remaining: dict = {}
        if failed_syms:
            cur = {}
            try:
                with open(trades_file, encoding="utf-8") as fr:
                    cur = json.load(fr) or {}
            except Exception as e:
                log("warn", f"State re-read failed during cleanup: {e}")
            for sym in failed_syms:
                if sym in failed_position_updates:
                    remaining[sym] = failed_position_updates[sym]
                elif sym in cur:
                    remaining[sym] = cur[sym]
        from bot_utils.state_persist import atomic_save_json
        if not atomic_save_json(trades_file, remaining):
            log("error", "State cleanup write failed - stale positions may remain")
        if failed_syms:
            log("warn",
                f" Kept {len(failed_syms)} position(s) in state due to "
                f"close failures: {', '.join(failed_syms)}  manual review needed")
    except Exception as e:
        log("error", f"State cleanup write raised: {e}")

    sev = "warn" if "Emergency" in reason else "system"
    log(sev,
        f" {closed} position(s) closed  Total realized: {total_pnl:+.2f} USDT")
