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
from typing import Optional

LIVE_FUTURES_BOTS = {"FUTURES", "CROSS", "FUTREND"}


#  Helpers 

def _safe_float(value, default: float = 0.0) -> float:
    """Convert to float, swallow all conversion errors."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
            try:
                f = float(v)
                if f >= 0:
                    return f
            except (TypeError, ValueError):
                continue
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
        contracts = _safe_float(p.get("contracts") or p.get("size") or 0)
        if not (abs(contracts) > 0):
            continue
        count += 1

        # Margin: try ccxt-standard fields in priority order
        margin = (_safe_float(p.get("initialMargin"))
                  or _safe_float(p.get("collateral"))
                  or _safe_float(p.get("margin"))
                  or 0.0)
        total_margin += margin

        # Unrealized PnL: ccxt mostly normalizes to unrealizedPnl
        upnl = (_safe_float(p.get("unrealizedPnl"))
                or _safe_float(p.get("unrealized_pnl"))
                or 0.0)
        total_unrealized += upnl

        # Per-position breakdown for the dashboard tooltip.
        # Symbol stripped to base (BRETT/USDT:USDT  BRETT) for short
        # display; full symbol kept around in case a caller wants it.
        sym_full = p.get("symbol") or "?"
        base = sym_full.split("/")[0] if "/" in sym_full else sym_full
        side = (p.get("side") or "?").lower()
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
        bot_name = str(row.get("bot_name") or "")
        if bot_name not in live_bots:
            continue
        sym = row.get("symbol")
        if not sym:
            continue
        base = str(sym).split("/")[0].split(":")[0]
        db_by_sym[base] = {
            "unrealized":      float(row.get("unrealized_pnl") or 0.0),
            "unrealized_pct":  float(row.get("unrealized_pct") or 0.0),
            "current_price":   float(row.get("current_price") or 0.0),
            "entry_price":     float(row.get("entry_price") or 0.0),
        }
    return db_by_sym


#  Public API: Futures 

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
    try:
        positions = ex.fetch_positions()
    except Exception:
        # Without positions we don't know what's bound. Return what we
        # know but flag that the unrealized side is incomplete.
        return {
            "free":         round(free, 4),
            "in_positions": 0.0,
            "unrealized":   0.0,
            "equity":       round(free, 4),
            "open_count":   0,
            "positions":    [],
            "source":       "free-only (fetch_positions unavailable)",
        }

    margin_sum, upnl_sum, count, details = _sum_position_margin_and_upnl(positions)

    # MEXC quirk: ccxt's fetch_positions() returns unrealizedPnl=0 for
    # every position because MEXC doesn't include it in the response.
    # If we detect this (we have open positions but uPnL is exactly 0),
    # augment from the bot's futures_state table  same data source the
    # sidebar's 'Unrealized' label uses, kept fresh every 15s by the
    # monitor loop.
    augmented_source = None
    if count > 0 and abs(upnl_sum) < 1e-9:
        try:
            # Lazy import  equity_utils mustn't hard-depend on the bot's
            # DB layer (it should still work in a fresh test environment).
            from core.database import get_futures_state  # type: ignore
            db_rows = get_futures_state() or []
            db_by_sym = _live_futures_state_by_symbol(db_rows)
            # Merge into details: each detail dict already has symbol (base).
            new_upnl_sum = 0.0
            had_any_match = False
            for d in details:
                sym_base = d.get("symbol", "")
                row = db_by_sym.get(sym_base)
                if row is not None:
                    d["unrealized"] = round(row["unrealized"], 4)
                    had_any_match = True
                new_upnl_sum += d["unrealized"]
            if had_any_match:
                upnl_sum = new_upnl_sum
                augmented_source = " (uPnL from futures_state DB)"
        except Exception:
            # DB unavailable / fresh install / different bot  keep the
            # exchange's zeroes. Better than crashing.
            pass

    return {
        "free":         round(free, 4),
        "in_positions": round(margin_sum, 4),
        "unrealized":   round(upnl_sum, 4),
        "equity":       round(free + margin_sum + upnl_sum, 4),
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

    try:
        bal = ex.fetch_balance()
    except Exception:
        return None

    if not isinstance(bal, dict):
        return None

    # First pass: collect stablecoin free totals (counted 1:1)
    # and other coins (need price lookup)
    stable_free = 0.0
    non_stable_holdings: dict[str, float] = {}

    # Iterate top-level keys that are currency dicts (skip ccxt metadata
    # keys like 'info', 'free', 'used', 'total' which mirror everything).
    skip_keys = {"info", "free", "used", "total", "timestamp", "datetime"}

    for symbol, data in bal.items():
        if symbol in skip_keys:
            continue
        if not isinstance(data, dict):
            continue
        total = _safe_float(data.get("total"))
        if total <= 0:
            continue

        if symbol in _QUOTE_ASSETS_AS_USDT:
            stable_free += total
        else:
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
        try:
            if getattr(ex, "has", {}).get("fetchTickers"):
                tickers = ex.fetch_tickers(list(symbol_pairs.values()))
                if isinstance(tickers, dict):
                    ticker_cache = tickers
        except Exception:
            ticker_cache = {}

        for coin, amount in non_stable_holdings.items():
            pair = symbol_pairs[coin]
            price = 0.0

            # Try the bulk cache first
            t = ticker_cache.get(pair)
            if isinstance(t, dict):
                price = _safe_float(t.get("last") or t.get("close"))

            # Fall back to per-symbol fetch
            if price <= 0:
                try:
                    t = ex.fetch_ticker(pair)
                    if isinstance(t, dict):
                        price = _safe_float(t.get("last") or t.get("close"))
                except Exception:
                    price = 0.0

            if price <= 0:
                # Can't price this coin  skip it (dust, delisted, etc.).
                # We DON'T silently substitute 0 into a "valid" total 
                # we just exclude it from the count.
                continue

            value_usdt = amount * price
            if value_usdt < _DUST_USDT_THRESHOLD:
                continue  # dust filter

            in_positions_value += value_usdt
            open_count += 1

    equity = stable_free + in_positions_value

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
        grand = round(sum(parts), 4)

    return {"spot": spot, "futures": fut, "grand_total": grand}
