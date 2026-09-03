"""
equity_utils.py  Portable equity calculation for the dashboard.

Two flavours:

  * compute_futures_equity(ex)  Free margin + sum of position margins +
    unrealized PnL. Uses fetch_balance() + fetch_positions(). Works on
    MEXC (where fetch_balance.used is wrong) and any other ccxt exchange
    (where it might be correct  we ADD position margin regardless,
    relying on the exchange-reported 'initialMargin' as the truth source).

  * compute_spot_equity(ex)  Free USDT + sum of (coin holding  current
    price). Spot positions ARE the coins themselves  there is no
    separate margin.

Both functions return a `dict` with the breakdown (so the dashboard can
show all components, not just one magic number), and a `None` if the
exchange is unreachable. They never throw.

Truth source priority:
  1. ALWAYS read live from the exchange  never from the bot's DB.
  2. If a field is missing/None/unparseable, treat as 0 and continue.
  3. If the call itself fails, return None  let the caller show "N/A".

Why not use bal['USDT']['used']?
  MEXC's V1 futures balance endpoint returns used=0.00 even with open
  positions. Other exchanges may report it correctly, but we don't trust it:
  we ALWAYS recompute margin from fetch_positions() so the same code path
  works everywhere.
"""
from __future__ import annotations
from functools import wraps
import math
from typing import Optional

from bot_utils.api_budget import try_consume_api_call

LIVE_FUTURES_BOTS = {"FUTURES", "CROSS", "FUTREND"}


#  Helpers 

def _finite_float_or_none(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _safe_float(value, default: float = 0.0) -> float:
    """Convert to float, swallow all conversion errors."""
    parsed = _finite_float_or_none(value)
    return default if parsed is None else parsed


def _finite_add(total: float, value: float) -> float:
    candidate = total + value
    return candidate if math.isfinite(candidate) else total


def _safe_label(value, default: str = "?", max_length: int = 128) -> str:
    if value is None:
        return default
    try:
        rendered = str(value).strip()
    except Exception:
        return default
    return rendered[:max_length] if rendered else default


def _never_raises_none(func):
    """Keep dashboard-only equity probes isolated from malformed APIs."""
    @wraps(func)
    def guarded(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            return None
    return guarded


def _read_usdt_free(bal: dict) -> Optional[float]:
    """Pull the FREE USDT amount from a ccxt balance dict.

    Tries the standard ccxt path first, then a few exchange-specific
    fallbacks. Returns None if nothing matched (caller should show N/A).
    """
    if not isinstance(bal, dict):
        return None
    paths = (
        ("USDT", "free"),
        ("USDT", "available"),
        ("free", "USDT"),
    )
    for path in paths:
        v = bal
        for key in path:
            if not isinstance(v, dict):
                v = None
                break
            v = v.get(key)
            if v is None:
                break
        if v is not None:
            f = _safe_float(v, -1.0)
            if f >= 0:
                return f
    return None


def _read_currency_free(data: dict) -> float:
    if not isinstance(data, dict):
        return 0.0
    for key in ("free", "available"):
        raw = data.get(key)
        if raw is None:
            continue
        parsed = _safe_float(raw, -1.0)
        if parsed >= 0:
            return parsed
    return 0.0


def _read_currency_total_or_free(data: dict) -> float:
    if not isinstance(data, dict):
        return 0.0
    total = _safe_float(data.get("total"), -1.0)
    if total >= 0:
        return total
    return _read_currency_free(data)


def _position_margin_or_none(position: dict) -> float | None:
    for field in ("initialMargin", "collateral", "margin"):
        candidate = _finite_float_or_none(position.get(field))
        if candidate is not None and candidate > 0.0:
            return candidate
    return None


def _sum_position_margin_and_upnl(positions: list) -> tuple[float, float, int, list]:
    """Walk a fetch_positions() result; sum margin and unrealized PnL.

    Returns (total_margin, total_unrealized, open_count, position_details).
    The 4th element is a list of per-position dicts for UI breakdown:
      [{"symbol": str, "side": "long"/"short",
        "margin": float, "unrealized": float}, ...]
    Robust against missing fields, weird types, and exchange quirks.
    """
    total_margin = 0.0
    total_unrealized = 0.0
    count = 0
    details: list = []

    if not isinstance(positions, list):
        return (0.0, 0.0, 0, [])

    for p in positions:
        if not isinstance(p, dict):
            continue
        # Skip closed/zero positions
        from bot_utils.futures_order import _position_contracts_abs

        contracts = _position_contracts_abs(p)
        if contracts is None or contracts <= 0.0:
            continue
        count += 1

        # Margin: try ccxt-standard fields in priority order
        margin = _position_margin_or_none(p) or 0.0
        total_margin = _finite_add(total_margin, margin)

        # Unrealized PnL: ccxt mostly normalizes to unrealizedPnl
        upnl = 0.0
        for field in ("unrealizedPnl", "unrealized_pnl"):
            candidate = _finite_float_or_none(p.get(field))
            if candidate is not None:
                upnl = candidate
                break
        total_unrealized = _finite_add(total_unrealized, upnl)

        # Per-position breakdown for the dashboard tooltip.
        # Symbol stripped to base (BRETT/USDT:USDT  BRETT) for short
        # display; full symbol kept around in case a caller wants it.
        sym_full = _safe_label(p.get("symbol"))
        base = sym_full.split("/")[0] if "/" in sym_full else sym_full
        side = _safe_label(p.get("side")).lower()
        details.append({
            "symbol":     base,
            "symbol_full": sym_full,
            "side":       side,
            "margin":     round(margin, 4),
            "unrealized": round(upnl, 4),
        })

    return (total_margin, total_unrealized, count, details)


def _configured_live_futures_bots() -> set[str]:
    live = set()
    try:
        from bot_utils.sim_flag import read_simulation_flag
        for bot_name in LIVE_FUTURES_BOTS:
            if not read_simulation_flag(
                    bot_name, raise_on_corrupt=False, default=True):
                live.add(bot_name)
    except Exception:
        pass
    return live


def _live_futures_state_by_symbol(db_rows: list,
                                  live_bots: set[str] | None = None) -> dict:
    live_bots = (live_bots if live_bots is not None
                 else _configured_live_futures_bots())
    db_by_sym = {}
    for row in db_rows:
        if not isinstance(row, dict):
            continue
        bot_name = _safe_label(row.get("bot_name"), "")
        if bot_name not in live_bots:
            continue
        sym = row.get("symbol")
        if not sym:
            continue
        base = _safe_label(sym, "").split("/")[0].split(":")[0]
        if not base:
            continue
        db_by_sym[base] = {
            "unrealized":      _safe_float(row.get("unrealized_pnl")),
            "unrealized_pct":  _safe_float(row.get("unrealized_pct")),
            "current_price":   _safe_float(row.get("current_price")),
            "entry_price":     _safe_float(row.get("entry_price")),
        }
    return db_by_sym


#  Public API: Futures 

@_never_raises_none
def compute_futures_equity(ex) -> Optional[dict]:
    """Compute futures equity breakdown.

    Returns a dict with keys:
      free  free USDT margin available for new trades
      in_positions  sum of margin currently in open positions
      unrealized  sum of unrealized PnL across open positions
      equity  free + in_positions + unrealized (the "wallet" total)
      open_count  number of currently-open positions
      source  short description of where the numbers came from

    Returns None if the exchange is unreachable. Never throws.
    """
    if ex is None:
        return None

    # Free USDT
    if not try_consume_api_call("dashboard_futures_fetch_balance"):
        return None
    try:
        bal = ex.fetch_balance()
    except Exception:
        return None

    free = _read_usdt_free(bal)
    if free is None:
        # We could not parse the balance  without 'free' the rest is
        # meaningless. Better to return None than show a wrong total.
        return None

    # Position margin + unrealized
    if not try_consume_api_call("dashboard_futures_fetch_positions"):
        return None
    try:
        positions = ex.fetch_positions()
    except Exception:
        # Free margin is not total wallet equity while positions are unknown.
        return None
    if not isinstance(positions, list):
        return None

    # A malformed row cannot safely be treated as a closed position: doing so
    # would silently omit its margin and PnL from an apparently complete total.
    from bot_utils.futures_order import _position_contracts_abs

    if any(
        not isinstance(position, dict)
        or _position_contracts_abs(position) is None
        for position in positions
    ):
        return None

    open_positions = [
        position
        for position in positions
        if (_position_contracts_abs(position) or 0.0) > 0.0
    ]
    if any(
        _position_margin_or_none(position) is None
        for position in open_positions
    ):
        return None

    margin_sum, upnl_sum, count, details = _sum_position_margin_and_upnl(positions)

    # MEXC currently omits unrealizedPnl from normalized position rows.  Fill
    # only those specifically missing values from the live bot state.  Every
    # missing exchange value must have a matching DB row; a partial merge would
    # make an understated account total look complete in the dashboard.
    augmented_source = None
    missing_upnl = [
        detail
        for position, detail in zip(open_positions, details)
        if not any(
            _finite_float_or_none(position.get(field)) is not None
            for field in ("unrealizedPnl", "unrealized_pnl")
        )
    ]
    if missing_upnl:
        try:
            # Lazy import  equity_utils mustn't hard-depend on the bot's
            # DB layer (it should still work in a fresh test environment).
            from core.database import (  # type: ignore
                get_claim_bound_live_futures_state,
            )
            from bot_utils.pnl_view import is_futures_state_fresh

            db_rows = get_claim_bound_live_futures_state() or []
            db_by_sym = _live_futures_state_by_symbol(
                [row for row in db_rows if is_futures_state_fresh(row)]
            )
            for d in missing_upnl:
                sym_base = d.get("symbol", "")
                row = db_by_sym.get(sym_base)
                if row is None:
                    return None
                d["unrealized"] = round(row["unrealized"], 4)
            upnl_sum = sum(d["unrealized"] for d in details)
            if not math.isfinite(upnl_sum):
                return None
            augmented_source = " (missing uPnL from futures_state DB)"
        except Exception:
            return None

    equity_total = free + margin_sum + upnl_sum
    if not math.isfinite(equity_total):
        return None
    return {
        "free":         round(free, 4),
        "in_positions": round(margin_sum, 4),
        "unrealized":   round(upnl_sum, 4),
        "equity":       round(equity_total, 4),
        "open_count":   count,
        "positions":    details,
        "source":       ("balance.free + positions.margin + positions.uPnL"
                          + (augmented_source or "")),
    }


#  Public API: Spot 

# These coins ARE quote currencies  we count them at face value (1:1).
# Anything else needs a price lookup to convert to USDT.
_QUOTE_ASSETS_AS_USDT = frozenset({
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USD1", "USDE",
})

# Tiny holdings (dust) are skipped  they're noise from rounding and
# tiny airdrops, not real positions.
_DUST_USDT_THRESHOLD = 0.5


@_never_raises_none
def compute_spot_equity(ex) -> Optional[dict]:
    """Compute spot equity breakdown.

    Spot is structurally different from futures:
      There is no margin and no leverage.
      A "position" is a coin you HOLD in the wallet.
      Equity = free USDT + sum of (coin  current price)

    Returns a dict with keys:
      free  free USDT (incl. other stablecoins counted 1:1)
      in_positions  sum of non-stablecoin coin values in USDT
      unrealized  always 0 for spot (no separate concept)
      equity  free + in_positions
      open_count  number of non-dust coin holdings
      source  description

    Returns None on exchange failure. Never throws.
    """
    if ex is None:
        return None

    if not try_consume_api_call("dashboard_spot_fetch_balance"):
        return None
    try:
        bal = ex.fetch_balance()
    except Exception:
        return None

    if not isinstance(bal, dict):
        return None

    # First pass: collect stablecoin free totals separately from stablecoin
    # wallet equity. Locked quote balances are still equity, but they must not
    # render as available/free capital.
    stable_free = 0.0
    stable_equity = 0.0
    non_stable_holdings: dict[str, float] = {}

    # Iterate top-level keys that are currency dicts (skip ccxt metadata
    # keys like 'info', 'free', 'used', 'total' which mirror everything).
    skip_keys = {"info", "free", "used", "total", "timestamp", "datetime"}

    for raw_symbol, data in bal.items():
        symbol = _safe_label(raw_symbol, "")
        if not symbol:
            continue
        if symbol in skip_keys:
            continue
        if not isinstance(data, dict):
            continue
        if symbol in _QUOTE_ASSETS_AS_USDT:
            free_amount = _read_currency_free(data)
            equity_amount = _read_currency_total_or_free(data)
            if free_amount > 0 or equity_amount > 0:
                stable_free = _finite_add(stable_free, free_amount)
                stable_equity = _finite_add(stable_equity, equity_amount)
        else:
            total = _safe_float(data.get("total"))
            if total <= 0:
                continue
            non_stable_holdings[symbol] = total

    # Second pass: fetch prices for non-stable holdings.
    # We use fetch_tickers in bulk where supported, else fall back to
    # per-symbol fetch_ticker. Wrap each lookup so one failure doesn't
    # kill the whole calculation.
    in_positions_value = 0.0
    open_count = 0

    if non_stable_holdings:
        # Build the symbol list we need pricing for.
        symbol_pairs = {coin: f"{coin}/USDT" for coin in non_stable_holdings}

        # Try bulk first  much faster, fewer API calls.
        ticker_cache: dict = {}
        batch_fetch_failed = False
        try:
            if (
                getattr(ex, "has", {}).get("fetchTickers")
                and try_consume_api_call("dashboard_spot_fetch_tickers")
            ):
                tickers = ex.fetch_tickers(list(symbol_pairs.values()))
                if isinstance(tickers, dict):
                    ticker_cache = tickers
        except Exception:
            ticker_cache = {}
            batch_fetch_failed = True

        if batch_fetch_failed:
            # Stablecoin free cash alone is not total spot equity when priced
            # holdings exist. Expose an unavailable snapshot instead of a
            # deceptively low but apparently complete dashboard balance.
            return None

        for coin, amount in non_stable_holdings.items():
            pair = symbol_pairs[coin]
            price = 0.0

            # Try the bulk cache first
            t = ticker_cache.get(pair)
            if isinstance(t, dict):
                price = _safe_float(t.get("last"))
                if price <= 0:
                    price = _safe_float(t.get("close"))

            # Fall back to per-symbol fetch
            if (
                price <= 0
                and not batch_fetch_failed
                and try_consume_api_call("dashboard_spot_fetch_ticker")
            ):
                try:
                    t = ex.fetch_ticker(pair)
                    if isinstance(t, dict):
                        price = _safe_float(t.get("last"))
                        if price <= 0:
                            price = _safe_float(t.get("close"))
                except Exception:
                    price = 0.0

            if price <= 0:
                # An amount cannot be classified as dust without a price.  A
                # partial wallet valuation would look complete to the launcher
                # and understate equity, so expose the whole snapshot as
                # unavailable until every actual holding can be priced.
                return None

            value_usdt = amount * price
            if (
                not math.isfinite(value_usdt)
                or value_usdt < _DUST_USDT_THRESHOLD
            ):
                continue  # dust filter

            next_positions_value = in_positions_value + value_usdt
            if not math.isfinite(next_positions_value):
                continue
            in_positions_value = next_positions_value
            open_count += 1

    equity = stable_equity + in_positions_value
    if not math.isfinite(equity):
        return None

    return {
        "free":         round(stable_free, 4),
        "in_positions": round(in_positions_value, 4),
        "unrealized":   0.0,
        "equity":       round(equity, 4),
        "open_count":   open_count,
        "source":  "balance (stables 1:1) + holdings  ticker",
    }


#  Optional: combined helper for "total exchange balance" 

def compute_combined_equity(spot_ex=None, futures_ex=None) -> dict:
    """Convenience wrapper that returns both equities + a grand total.

    Each side is independent  a failure on one side does not zero the
    other. Missing sides are simply omitted from the grand total.

    Returns a dict that's safe to render directly in a dashboard:
      {
        "spot":    {...} or None,
        "futures": {...} or None,
        "grand_total": float | None,
      }
    """
    spot = compute_spot_equity(spot_ex) if spot_ex is not None else None
    fut = compute_futures_equity(futures_ex) if futures_ex is not None else None

    grand = None
    parts = []
    if spot is not None:
        parts.append(spot["equity"])
    if fut is not None:
        parts.append(fut["equity"])
    if parts:
        total = sum(parts)
        grand = round(total, 4) if math.isfinite(total) else None

    return {"spot": spot, "futures": fut, "grand_total": grand}
