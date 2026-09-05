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

# ruff: noqa: E402  # update barrier must run before project/runtime imports

from __future__ import annotations

import math
import os
import sys

if __name__ == "__main__" or not hasattr(sys.stdout, "reconfigure"):
    pass
else:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

import numpy as np

from config.exchange_config import get_exchange_connection, get_active_exchange_name
from tools.trend_check import (
    _sig_price_ma,
    _sig_cross,
    _max_dd,
    _safe_exc,
    _validated_trend_ohlc,
    COST,
)
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
MAX_TREND_DAYS = 3_650
MAX_CUSTOM_COINS = 100
MAX_COIN_LENGTH = 20


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
    try:
        closes = np.asarray(closes, dtype=float)
        sig = np.asarray(sig)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("closes and signal must be numeric vectors") from exc
    if sig.dtype.kind != "b":
        raise ValueError("signal must contain booleans")
    if (
        closes.ndim != 1
        or sig.ndim != 1
        or len(closes) < 2
        or len(sig) != len(closes)
        or not np.all(np.isfinite(closes))
        or np.any(closes <= 0.0)
    ):
        raise ValueError("closes and signal must be aligned finite vectors")
    checked = {}
    for name, value in (("lev", lev), ("cost", cost), ("mm", mm)):
        if isinstance(value, bool):
            raise ValueError(f"{name} must be finite")
        try:
            checked[name] = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be finite") from exc
        if not np.isfinite(checked[name]):
            raise ValueError(f"{name} must be finite")
    lev, cost, mm = checked["lev"], checked["cost"], checked["mm"]
    if lev <= 0.0 or not 0.0 <= cost < 1.0 or not 0.0 < mm < 1.0:
        raise ValueError("lev, cost, or mm is outside its valid range")
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
            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                factor = 1.0 + lev * (closes[i] / closes[i - 1] - 1.0)
                projected_equity = eq * factor
            if not np.isfinite(factor) or not np.isfinite(projected_equity):
                raise ValueError("non-finite leveraged equity transition")
            if factor <= mm or (
                entry is not None and projected_equity <= entry * mm
            ):
                eq = (entry if entry is not None else eq) * mm
                liqs += 1
                trades += 1
                entry = None
                pos = 0
            else:
                eq = projected_equity
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


def _load_data(
    ex,
    symbols: list[str],
    since_ms: int,
    min_bars: int,
    until_ms: int,
) -> dict:
    """Fetch daily OHLCV for each symbol via cache; return {coin: (c,h,l)}."""
    data = {}
    for sym in symbols:
        coin = sym.split("/")[0]
        try:
            bars = _fetch_daily(ex, sym, since_ms)
        except Exception:
            continue
        bars = _validated_trend_ohlc(
            bars,
            since_ms=since_ms,
            until_ms=until_ms,
        )
        if len(bars) < min_bars:
            continue
        arr = np.array(bars, dtype=float)
        data[coin] = (arr[:, 4], arr[:, 2], arr[:, 3])
    return data


def _get_symbols_for_size(
    ex, n: int, days: int, fallback_symbols: list[str]
) -> list[str]:
    """Return top-N USDT-pair symbols from exchange by $-volume, listing-age filtered."""
    fallback = [f"{c}/USDT" for c in fallback_symbols[:n]]
    try:
        syms = get_top_volume_coins(ex, n, days)
    except Exception as exc:
        print(
            f"   [warn] volume fetch failed for N={n}: {_safe_exc(exc)}; "
            f"falling back to hardcoded list (first {n})"
        )
        return fallback

    valid_symbols = (
        isinstance(syms, list)
        and 1 <= len(syms) <= n
        and all(
            isinstance(symbol, str)
            and symbol.isascii()
            and symbol == symbol.upper()
            and symbol.count("/") == 1
            and symbol.endswith("/USDT")
            and 1 <= len(symbol.split("/", 1)[0]) <= 20
            and symbol.split("/", 1)[0].isalnum()
            for symbol in syms
        )
        and len(set(syms)) == len(syms)
    )
    if valid_symbols:
        return syms
    print(
        f"   [warn] invalid volume symbols for N={n}, "
        f"falling back to hardcoded list (first {n})"
    )
    return fallback


def _run_sweep(
    ex,
    days: int,
    custom_coins: list[str],
    markets: dict | None = None,
) -> int:
    """Execute the 36 matrix sweep and print the result table."""
    from datetime import datetime, timezone

    if (
        isinstance(days, bool)
        or not isinstance(days, int)
        or not 1 <= days <= MAX_TREND_DAYS
    ):
        print("   [warn] days must be a positive integer")
        return 1
    try:
        raw_now_ms = ex.milliseconds()
    except Exception as e:
        print(f"   [warn] exchange clock failed: {_safe_exc(e)}")
        return 1
    if isinstance(raw_now_ms, bool):
        print("   [warn] invalid exchange clock")
        return 1
    try:
        numeric_now_ms = float(raw_now_ms)
    except (TypeError, ValueError, OverflowError):
        print("   [warn] invalid exchange clock")
        return 1
    if (
        not math.isfinite(numeric_now_ms)
        or numeric_now_ms <= 0.0
        or not numeric_now_ms.is_integer()
    ):
        print("   [warn] invalid exchange clock")
        return 1
    now_ms = int(numeric_now_ms)
    since_ms = now_ms - (days + 10) * 86_400_000
    min_bars = max(150, days)  # requested window plus model warmup floor
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
        market_catalog = markets if markets is not None else getattr(ex, "markets", {})
        syms = [
            f"{c}/USDT"
            for c in custom_coins
            if f"{c}/USDT" in market_catalog
        ]
        data = _load_data(ex, syms, since_ms, min_bars, now_ms)
        universe_sets["custom"] = data
        sweep_sizes = ["custom"]
    else:
        for n in UNIVERSE_SIZES:
            print(f"\n  Fetching top-{n} universe ...")
            syms = _get_symbols_for_size(ex, n, days, UNIVERSE_30)
            data = _load_data(ex, syms, since_ms, min_bars, now_ms)
            universe_sets[n] = data
            print(
                f"   {len(data)}/{len(syms)} coins qualified with >={min_bars} daily bars"
            )
        sweep_sizes = UNIVERSE_SIZES

    print()

    best_cell = None  # (n, lev, port_ret) among deployable cells
    evaluated = False

    for n in sweep_sizes:
        data = universe_sets[n]
        label = f"N={n}" if n != "custom" else "custom"

        if not data:
            print(f"\n  [{label}] no data  skipped")
            continue
        evaluated = True

        series = [(c, h, low, ensemble(c, h, low)) for c, h, low in data.values()]
        K = min(days, min(len(c) for c, h, low, _ in series))
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
    if not evaluated:
        print("  CONCLUSION: No data; no leverage cell could be evaluated.")
        return 1
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
    return 0


def main() -> int:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout = open(
                sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1
            )
        except Exception:
            pass

    args = sys.argv[1:]
    unknown_options = [arg for arg in args if arg.startswith("--")]
    if unknown_options:
        print(f"  Unknown option: {unknown_options[0]}")
        return 2
    pos = [a for a in args if not a.startswith("--")]
    if pos:
        try:
            days = int(pos[0])
        except (TypeError, ValueError, OverflowError):
            print(f"  Invalid days argument: {pos[0]!r}")
            return 2
        if not 1 <= days <= MAX_TREND_DAYS:
            print(f"  Invalid days argument: {pos[0]!r}")
            return 2
    else:
        days = 730
    custom_coins = [a.upper() for a in pos[1:]] if len(pos) > 1 else []
    if custom_coins and (
        len(custom_coins) > MAX_CUSTOM_COINS
        or len(set(custom_coins)) != len(custom_coins)
        or any(
            not coin
            or len(coin) > MAX_COIN_LENGTH
            or not coin.isascii()
            or not coin.isalnum()
            for coin in custom_coins
        )
    ):
        print("  Invalid coin scope: use 1-100 unique ASCII bases")
        return 2

    print(f"\n  Connecting to {get_active_exchange_name().upper()} (SPOT) ...")
    try:
        ex = get_exchange_connection()
        ex.timeout = 30000
        markets = ex.load_markets()
    except Exception as e:
        print(f"  Connection failed: {_safe_exc(e)}")
        return 1
    if (
        not isinstance(markets, dict)
        or not markets
        or any(not isinstance(symbol, str) for symbol in markets)
    ):
        print("  Invalid or empty market catalog; cannot run.")
        return 1

    return _run_sweep(ex, days, custom_coins, markets)


if __name__ == "__main__":
    sys.exit(main())
