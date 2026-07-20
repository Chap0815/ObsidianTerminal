"""
tools/trend_leverage_check.py  Universe  Leverage deployment surface for trend-following.

Maps {universe size 20/30/50}  {leverage 1..6} on the DAILY timeframe for the
long/flat SMA-ensemble trend strategy, reporting per cell:
  portfolio return %
  portfolio max-DD %
  liquidations
  trades/week
  B&H baseline

Flags deployable cells: PortDD < 40% AND 0 liquidations.

Run:
  python -m tools.trend_leverage_check          # 730d sweep
  python -m tools.trend_leverage_check 365      # 1 year sweep
  python -m tools.trend_leverage_check 730 BTC ETH SOL   # custom universe (no sweep)
"""

from __future__ import annotations

import os
import sys

if __name__ == "__main__" or not hasattr(sys.stdout, "reconfigure"):
    pass
else:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.exchange_config import get_exchange_connection, get_active_exchange_name
from tools.trend_check import _sig_price_ma, _sig_cross, _max_dd, COST
from tools.ohlcv_cache import get_series
from tools.backtester import get_top_volume_coins

MAINT_MARGIN = 0.005

UNIVERSE_30 = [
    "BTC",
    "ETH",
    "BNB",
    "XRP",
    "SOL",
    "ADA",
    "AVAX",
    "LINK",
    "DOT",
    "LTC",
    "DOGE",
    "TRX",
    "MATIC",
    "ATOM",
    "UNI",
    "ETC",
    "XLM",
    "BCH",
    "FIL",
    "NEAR",
    "ICP",
    "ALGO",
    "HBAR",
    "VET",
    "AAVE",
    "EGLD",
    "SAND",
    "MANA",
    "THETA",
    "AXS",
]

UNIVERSE_SIZES = [20, 30, 50]
LEVERAGES = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
DD_GATE = 40.0


def _fetch_daily(ex, symbol: str, since_ms: int) -> list:
    """Daily OHLCV via the cache layer."""
    return get_series(ex, symbol, "1d", since_ms)


def ensemble(c: np.ndarray, h: np.ndarray, low: np.ndarray) -> np.ndarray:
    """Trend ensemble (>=2 of P>SMA50 / P>SMA100 / SMA20>50) on daily bars."""
    votes = (
        _sig_price_ma(c, h, low, 50).astype(int)
        + _sig_price_ma(c, h, low, 100).astype(int)
        + _sig_cross(c, h, low, 20, 50).astype(int)
    )
    return votes >= 2


def backtest_lev(
    closes: np.ndarray,
    sig: np.ndarray,
    lev: float,
    cost: float = COST,
    mm: float = MAINT_MARGIN,
) -> dict:
    """All-in long/flat with leverage; models liquidation on close-to-close moves."""
    n = len(closes)
    eq = peak = 1.0
    maxdd = 0.0
    trades = liqs = bars_in = 0
    entry = None
    pos = 0
    curve = np.ones(n)
    for i in range(1, n):
        target = 1 if sig[i - 1] else 0
        if target == 1 and pos == 0:
            eq *= 1 - cost
            entry = eq
            pos = 1
        elif target == 0 and pos == 1:
            eq *= 1 - cost
            trades += 1
            entry = None
            pos = 0
        if pos == 1:
            bars_in += 1
            factor = 1.0 + lev * (closes[i] / closes[i - 1] - 1.0)
            if factor <= mm or (entry is not None and eq * factor <= entry * mm):
                eq = (entry if entry is not None else eq) * mm
                liqs += 1
                trades += 1
                entry = None
                pos = 0
            else:
                eq *= factor
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak
        if dd > maxdd:
            maxdd = dd
        curve[i] = eq
    return {
        "ret": (eq - 1) * 100.0,
        "maxdd": maxdd * 100.0,
        "trades": trades,
        "liqs": liqs,
        "pct_in": 100.0 * bars_in / max(1, n - 1),
        "curve": curve,
    }


def _load_data(ex, symbols: list[str], since_ms: int, min_bars: int) -> dict:
    """Fetch daily OHLCV for each symbol via cache; return {coin: (c,h,l)}."""
    data = {}
    for sym in symbols:
        coin = sym.split("/")[0]
        try:
            bars = _fetch_daily(ex, sym, since_ms)
        except Exception:
            continue
        bars = [b for b in bars if b[0] >= since_ms]
        if len(bars) < min_bars:
            continue
        arr = np.array(bars, dtype=float)
        data[coin] = (arr[:, 4], arr[:, 2], arr[:, 3])
    return data


def _get_symbols_for_size(
    ex, n: int, days: int, fallback_symbols: list[str]
) -> list[str]:
    """Return top-N USDT-pair symbols from exchange by $-volume, listing-age filtered."""
    syms = get_top_volume_coins(ex, n, days)
    if syms:
        return syms
    print(
        f"   [warn] volume fetch failed for N={n}, falling back to hardcoded list (first {n})"
    )
    return [f"{c}/USDT" for c in fallback_symbols[:n]]


def _run_sweep(ex, days: int, custom_coins: list[str]) -> None:
    """Execute the 36 matrix sweep and print the result table."""
    from datetime import datetime, timezone

    now_ms = ex.milliseconds()
    since_ms = now_ms - (days + 10) * 86_400_000
    min_bars = 150  # 100-bar SMA + 50 warmup
    weeks = days / 7.0

    print("=" * 100)
    print("  TREND-FOLLOWING  UNIVERSE  LEVERAGE DEPLOYMENT SURFACE  (DAILY TIMEFRAME)")
    print(
        f"  {days}d | leverage: {', '.join(f'{lev:g}x' for lev in LEVERAGES)} | "
        f"cost {COST * 100:.2f}%/switch | deployment gate: PortDD < {DD_GATE:.0f}% AND 0 liquidations"
    )
    print("=" * 100)

    universe_sets: dict[str, dict] = {}

    if custom_coins:
        syms = [f"{c}/USDT" for c in custom_coins if f"{c}/USDT" in ex.markets]
        data = _load_data(ex, syms, since_ms, min_bars)
        universe_sets["custom"] = data
        sweep_sizes = ["custom"]
    else:
        for n in UNIVERSE_SIZES:
            print(f"\n  Fetching top-{n} universe ...")
            syms = _get_symbols_for_size(ex, n, days, UNIVERSE_30)
            data = _load_data(ex, syms, since_ms, min_bars)
            universe_sets[n] = data
            print(
                f"   {len(data)}/{len(syms)} coins qualified with >={min_bars} daily bars"
            )
        sweep_sizes = UNIVERSE_SIZES

    print()

    best_cell = None  # (n, lev, port_ret) among deployable cells

    for n in sweep_sizes:
        data = universe_sets[n]
        label = f"N={n}" if n != "custom" else "custom"

        if not data:
            print(f"\n  [{label}] no data  skipped")
            continue

        series = [(c, h, low, ensemble(c, h, low)) for c, h, low in data.values()]
        K = min(len(c) for c, h, low, _ in series)
        series = [(c[-K:], s[-K:]) for c, h, low, s in series]
        bh = float(np.mean([(c[-1] / c[0] - 1) * 100 for c, _ in series]))

        print(f"  {label.upper()}  ({len(series)} coins, B&H avg {bh:+.1f}%)  ")
        print(
            f"  {'Lev':>4}  {'PortRet%':>9}  {'PortDD%':>8}  {'Liqs':>6}  "
            f"{'Trd/wk':>8}  {'Deploy?':>8}"
        )
        print("  " + "-" * 58)

        for lev in LEVERAGES:
            per = [backtest_lev(c, s, lev) for c, s in series]
            tot_liq = int(sum(p["liqs"] for p in per))
            per_week = sum(p["trades"] for p in per) / weeks
            port = np.mean([p["curve"] for p in per], axis=0)
            port_ret = (port[-1] - 1) * 100.0
            port_dd = _max_dd(port)
            deployable = port_dd < DD_GATE and tot_liq == 0
            flag = " <-- DEPLOY" if deployable else ""
            print(
                f"  {lev:>3g}x  {port_ret:>+8.1f}%  {port_dd:>7.1f}%  {tot_liq:>6}  "
                f"{per_week:>8.1f}  {'YES' if deployable else 'no':>8}{flag}"
            )
            if deployable:
                if best_cell is None or port_ret > best_cell[2]:
                    best_cell = (label, lev, port_ret, port_dd)

        print("  " + "-" * 58)

    print()
    if best_cell:
        label, lev, ret, dd = best_cell
        print(
            f"  CONCLUSION: Best risk-adjusted deployable cell = {label}, {lev:g}x leverage "
            f"({ret:+.1f}% return, {dd:.1f}% PortDD, 0 liquidations)."
        )
    else:
        print(
            "  CONCLUSION: No cell meets the deployment gate (PortDD<40% AND 0 liquidations) "
            "across the tested universe sizes and leverages."
        )

    print(f"\n  Generated: {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    print("=" * 100)


def main() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout = open(
                sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1
            )
        except Exception:
            pass

    args = sys.argv[1:]
    pos = [a for a in args if not a.startswith("--")]
    days = int(pos[0]) if pos and pos[0].isdigit() else 730
    custom_coins = [a.upper() for a in pos[1:]] if len(pos) > 1 else []

    print(f"\n  Connecting to {get_active_exchange_name().upper()} (SPOT) ...")
    ex = get_exchange_connection()
    ex.timeout = 30000
    try:
        ex.load_markets()
    except Exception as e:
        print(f"  Connection failed: {e}")
        sys.exit(1)

    _run_sweep(ex, days, custom_coins)


if __name__ == "__main__":
    main()
