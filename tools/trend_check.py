"""
tools/trend_check.py  Trend-following edge check on the majors (strategy B3).

Tests whether simple long/flat trend rules on BTC/ETH/BNB/XRP/SOL would have
beaten  or meaningfully de-risked  buy-and-hold over a long daily history.

Why daily + long history + vs buy-and-hold
------------------------------------------
Trend-following's edge is NOT "higher raw return in a bull"  buy-and-hold
usually wins there. Its edge is RISK-ADJUSTED: capturing most of the upside
while side-stepping deep drawdowns (going to cash below trend). So the honest
benchmark is buy-and-hold's return AND its max drawdown. We use DAILY candles
over ~1 year (configurable) because trend rules whipsaw on 1h noise and only
prove themselves over many months.

No lookahead: moving averages / breakouts use only past closes; a day's
position earns the NEXT day's return; switching pays a cost.

Rules tested (long when "in trend", else CASH  fits a spot bot, no shorting):
  P>SMA50, P>SMA100  price above its moving average
  SMA20>50, SMA50>200  fast MA above slow MA (classic cross)
  Donchian20  breakout above 20-day high, exit below 10-day low

We run SEVERAL rules so you can see if trend works ROBUSTLY (most rules help)
or only on one cherry-picked setting (overfit).

Run
---
  python trend_check.py                    # 365d daily, BTC ETH BNB XRP SOL
  python trend_check.py 540                 # ~18 months
  python trend_check.py 365 BTC ETH SOL     # custom universe
"""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

from __future__ import annotations

import math
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

import numpy as np
from config.exchange_config import get_exchange_connection, get_active_exchange_name
from core.clock import backtest_asof_ms
from tools.ohlcv_cache import get_series

DEFAULT_DAYS = 365
DEFAULT_COINS = ["BTC", "ETH", "BNB", "XRP", "SOL"]
MIN_TREND_DAYS = 200
MAX_TREND_DAYS = 3_650
MAX_TREND_COINS = 100
MAX_COIN_LENGTH = 20
# Cost per position switch (one side), spot-taker-ish. Round trip = 2. Few
# trades on daily trend, so this is minor. Override TREND_COST_PCT.
def _trend_cost_from_env(raw=None) -> float:
    value = os.getenv("TREND_COST_PCT", "0.10") if raw is None else raw
    if isinstance(value, bool):
        return 0.001
    try:
        percent = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.001
    if math.isfinite(percent) and 0.0 <= percent < 100.0:
        return percent / 100.0
    return 0.001


COST = _trend_cost_from_env()


def _safe_exc(exc: Exception) -> str:
    try:
        from core.logger import redact

        return f"{type(exc).__name__}: {redact(str(exc))}"
    except Exception:
        return f"{type(exc).__name__}: <redaction unavailable>"


def _validated_trend_ohlc(series, *, since_ms: int, until_ms: int) -> list:
    if not isinstance(series, (list, tuple)):
        return []
    candles = {}
    for row in series:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            return []
        if isinstance(row[0], bool):
            return []
        try:
            timestamp = float(row[0])
        except (TypeError, ValueError, OverflowError):
            return []
        if not math.isfinite(timestamp) or not timestamp.is_integer():
            return []
        if not since_ms <= timestamp <= until_ms:
            continue
        if any(isinstance(row[index], bool) for index in range(1, 5)):
            return []
        try:
            open_, high, low, close = (float(row[index]) for index in range(1, 5))
        except (TypeError, ValueError, OverflowError):
            return []
        if (
            any(
                not math.isfinite(value) or value <= 0.0
                for value in (open_, high, low, close)
            )
            or high < max(open_, close)
            or low > min(open_, close)
        ):
            return []
        timestamp_int = int(timestamp)
        candle = [timestamp_int, open_, high, low, close]
        if timestamp_int in candles and candles[timestamp_int] != candle:
            return []
        candles[timestamp_int] = candle
    timestamps = sorted(candles)
    if any(
        current - previous != 86_400_000
        for previous, current in zip(timestamps, timestamps[1:])
    ):
        return []
    return [candles[timestamp] for timestamp in timestamps]


def _fetch_ohlc(ex, symbol: str, days: int) -> list:
    """Cache-backed, rate-limit-safe DAILY OHLC  list of [ts,o,h,l,c,...]."""
    if (
        isinstance(days, bool)
        or not isinstance(days, int)
        or not 1 <= days <= MAX_TREND_DAYS
    ):
        return []
    tf_ms = 86_400_000
    try:
        raw_now_ms = ex.milliseconds()
    except Exception as e:
        print(f"   [WARN] {symbol}: {_safe_exc(e)}")
        return []
    if isinstance(raw_now_ms, bool):
        print(f"   [WARN] {symbol}: invalid exchange clock")
        return []
    try:
        numeric_now_ms = float(raw_now_ms)
    except (TypeError, ValueError, OverflowError):
        print(f"   [WARN] {symbol}: invalid exchange clock")
        return []
    if (
        not math.isfinite(numeric_now_ms)
        or numeric_now_ms <= 0.0
        or not numeric_now_ms.is_integer()
    ):
        print(f"   [WARN] {symbol}: invalid exchange clock")
        return []
    now_ms = int(numeric_now_ms)
    try:
        raw_asof = backtest_asof_ms()  # IS/OOS wall: cap "now" to cutoff
    except Exception as exc:
        print(f"   [WARN] {symbol}: invalid backtest cutoff: {_safe_exc(exc)}")
        return []
    if raw_asof is not None:
        if isinstance(raw_asof, bool):
            print(f"   [WARN] {symbol}: invalid backtest cutoff")
            return []
        try:
            numeric_asof = float(raw_asof)
        except (TypeError, ValueError, OverflowError):
            print(f"   [WARN] {symbol}: invalid backtest cutoff")
            return []
        if (
            not math.isfinite(numeric_asof)
            or numeric_asof <= 0.0
            or not numeric_asof.is_integer()
        ):
            print(f"   [WARN] {symbol}: invalid backtest cutoff")
            return []
        now_ms = min(now_ms, int(numeric_asof))
    since = now_ms - (days + 5) * tf_ms
    try:
        series = get_series(ex, symbol, "1d", since)
    except Exception as e:
        print(f"   [WARN] {symbol}: {_safe_exc(e)}")
        return []
    current_bucket_start = (now_ms // tf_ms) * tf_ms
    validated = _validated_trend_ohlc(
        series,
        since_ms=since,
        until_ms=current_bucket_start - 1,
    )
    if not validated or validated[-1][0] != current_bucket_start - tf_ms:
        print(f"   [WARN] {symbol}: stale daily history")
        return []
    return validated


def _sma(a: np.ndarray, n: int) -> np.ndarray:
    out = np.full(len(a), np.nan)
    if len(a) >= n:
        c = np.cumsum(np.insert(a, 0, 0.0))
        out[n - 1 :] = (c[n:] - c[:-n]) / n
    return out


def _sig_price_ma(c, h, low, n):
    ma = _sma(c, n)
    return (c > ma) & ~np.isnan(ma)


def _sig_cross(c, h, low, f, s):
    sf, ss = _sma(c, f), _sma(c, s)
    return (sf > ss) & ~np.isnan(ss)


def _sig_donchian(c, h, low, nb, ns):
    n = len(c)
    inm = np.zeros(n, dtype=bool)
    state = False
    for i in range(n):
        if i < nb:
            continue
        hh = h[i - nb : i].max()
        ll = low[max(0, i - ns) : i].min()
        if not state and c[i] > hh:
            state = True
        elif state and c[i] < ll:
            state = False
        inm[i] = state
    return inm


CONFIGS = [
    ("P>SMA50", lambda c, h, low: _sig_price_ma(c, h, low, 50)),
    ("P>SMA100", lambda c, h, low: _sig_price_ma(c, h, low, 100)),
    ("SMA20>50", lambda c, h, low: _sig_cross(c, h, low, 20, 50)),
    ("SMA50>200", lambda c, h, low: _sig_cross(c, h, low, 50, 200)),
    ("Donchian20", lambda c, h, low: _sig_donchian(c, h, low, 20, 10)),
]


def _max_dd(equity: np.ndarray) -> float:
    peak = np.maximum.accumulate(equity)
    return float(np.max((peak - equity) / peak)) * 100.0


def _backtest_trend(closes, highs, lows, signal_fn):
    """Long/flat trend backtest vs buy-and-hold. Returns per-coin metrics."""
    n = len(closes)
    in_mkt = np.asarray(signal_fn(closes, highs, lows))
    if in_mkt.dtype.kind != "b" or in_mkt.ndim != 1 or len(in_mkt) != n:
        raise ValueError("signal must be an aligned boolean vector")
    eq = np.ones(n)
    entry_eq = None
    trades, wins, days_in = [], 0, 0
    for i in range(1, n):
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            r = closes[i] / closes[i - 1] - 1.0
        held = 1 if in_mkt[i - 1] else 0
        e = eq[i - 1] * (1 + r) if held else eq[i - 1]
        if not np.isfinite(r) or not np.isfinite(e):
            raise ValueError("non-finite trend equity transition")
        if held:
            days_in += 1
        new = 1 if in_mkt[i] else 0
        if new != held:
            e *= 1 - COST  # pay the switch
            if held == 1 and new == 0 and entry_eq:
                tr = e / entry_eq - 1
                trades.append(tr)
                if tr > 0:
                    wins += 1
            if new == 1:
                entry_eq = e
        eq[i] = e

    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        bh = closes / closes[0]
    if not np.all(np.isfinite(eq)) or not np.all(np.isfinite(bh)):
        raise ValueError("non-finite trend equity transition")
    strat_ret = (eq[-1] - 1) * 100.0
    bh_ret = (bh[-1] - 1) * 100.0
    return {
        "strat_ret": strat_ret,
        "bh_ret": bh_ret,
        "strat_dd": _max_dd(eq),
        "bh_dd": _max_dd(bh),
        "n_trades": len(trades),
        "win_rate": (100.0 * wins / len(trades)) if trades else 0.0,
        "pct_in": 100.0 * days_in / max(1, n - 1),
    }


def _verdict(s_ret, b_ret, s_dd, b_dd) -> str:
    # Trend "wins" if it beats B&H return, OR keeps most upside with far less DD.
    if s_ret > b_ret and s_dd <= b_dd:
        return "BEATS B&H"
    if s_dd < 0.7 * b_dd and s_ret > max(0.0, 0.5 * b_ret):
        return "SAFER"
    if s_ret > b_ret:
        return "HIGHER RET"
    return "WORSE"


def _sig_ensemble(c, h, low):
    """Long when a MAJORITY of the fast rules agree  more robust than any one
    moving-average length (reduces overfit to a single parameter)."""
    votes = (
        _sig_price_ma(c, h, low, 50).astype(int)
        + _sig_price_ma(c, h, low, 100).astype(int)
        + _sig_cross(c, h, low, 20, 50).astype(int)
    )
    return votes >= 2


def _print_sweep(data):
    """Robustness: is the edge a PLATEAU across SMA lengths (robust) or a spike
    on one value (overfit)? Plus an ensemble + per-coin breadth check."""
    lengths = [30, 40, 50, 60, 80, 100, 120]
    print("\n  PARAMETER SWEEP  P > SMA(n)  (plateau = robust, spike = overfit)")
    print(
        f"  {'n':>5}{'Strat%':>10}{'B&H%':>9}{'StratDD':>9}{'B&HDD':>8}"
        f"{'Trades':>8}  Verdict"
    )
    print("  " + "-" * 66)
    beat = 0
    for n in lengths:

        def signal_for_length(c, h, low, length=n):
            return _sig_price_ma(c, h, low, length)

        per = [
            _backtest_trend(c, h, low, signal_for_length) for c, h, low in data.values()
        ]
        avg = {k: float(np.mean([p[k] for p in per])) for k in per[0]}
        v = _verdict(avg["strat_ret"], avg["bh_ret"], avg["strat_dd"], avg["bh_dd"])
        if v in ("BEATS B&H", "SAFER", "HIGHER RET"):
            beat += 1
        print(
            f"  {n:>5}{avg['strat_ret']:>9.1f}%{avg['bh_ret']:>8.1f}%"
            f"{avg['strat_dd']:>8.1f}%{avg['bh_dd']:>7.1f}%{avg['n_trades']:>8.0f}"
            f"  {v}"
        )
    tag = (
        "PLATEAU  robust"
        if beat >= len(lengths) - 1
        else "mostly robust"
        if beat >= len(lengths) - 2
        else "FRAGILE"
    )
    print(f"  -> {beat}/{len(lengths)} SMA lengths beat/de-risked B&H  ({tag})")

    per = [_backtest_trend(c, h, low, _sig_ensemble) for c, h, low in data.values()]
    avg = {k: float(np.mean([p[k] for p in per])) for k in per[0]}
    print("\n  ENSEMBLE  (long when >=2 of P>SMA50 / P>SMA100 / SMA20>50 agree)")
    print(
        f"    Strat {avg['strat_ret']:+.1f}%  vs  B&H {avg['bh_ret']:+.1f}%   "
        f"DD {avg['strat_dd']:.0f}% vs {avg['bh_dd']:.0f}%   "
        f"trades {avg['n_trades']:.0f}   in-market {avg['pct_in']:.0f}%"
    )

    print("\n  PER-COIN (ensemble)  broad, or driven by one coin?")
    bc = 0
    for coin, (c, h, low) in data.items():
        r = _backtest_trend(c, h, low, _sig_ensemble)
        win = r["strat_ret"] > r["bh_ret"]
        bc += 1 if win else 0
        print(
            f"    {coin:<6} strat {r['strat_ret']:+8.1f}%   "
            f"B&H {r['bh_ret']:+8.1f}%   DD {r['strat_dd']:.0f}%   "
            f"{'beat' if win else '-'}"
        )
    print(f"  -> ensemble beats B&H on {bc}/{len(data)} coins")


def main():
    args = sys.argv[1:]
    unknown_options = [arg for arg in args if arg.startswith("--") and arg != "--sweep"]
    if unknown_options:
        print(f"  Unknown option: {unknown_options[0]}")
        return 2
    do_sweep = "--sweep" in args
    pos = [a for a in args if not a.startswith("--")]
    if pos:
        try:
            days = int(pos[0])
        except (TypeError, ValueError, OverflowError):
            print(f"  Invalid days argument: {pos[0]!r}")
            return 2
        if not MIN_TREND_DAYS <= days <= MAX_TREND_DAYS:
            print(f"  Invalid days argument: {pos[0]!r}")
            return 2
    else:
        days = DEFAULT_DAYS
    coins = [a.upper() for a in pos[1:]] if len(pos) > 1 else DEFAULT_COINS
    if (
        not 1 <= len(coins) <= MAX_TREND_COINS
        or len(set(coins)) != len(coins)
        or any(
            not coin
            or len(coin) > MAX_COIN_LENGTH
            or not coin.isascii()
            or not coin.isalnum()
            for coin in coins
        )
    ):
        print("  Invalid coin scope: use 1-100 unique ASCII bases")
        return 2

    print("=" * 90)
    print("  TREND-FOLLOWING EDGE CHECK (long/flat, vs buy-and-hold)")
    print(
        f"  {days}d daily | cost {COST * 100:.2f}%/switch | "
        f"rules: {', '.join(n for n, _ in CONFIGS)}"
    )
    print("=" * 90)

    print(
        f"\n  Connecting to {get_active_exchange_name().upper()} (SPOT, for "
        f"long history) ..."
    )
    try:
        ex = get_exchange_connection()  # spot = years of data
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
    print("  Connected. Loading daily history ...\n")

    data = {}
    for coin in coins:
        sym = f"{coin}/USDT"  # spot symbol
        if sym not in markets:
            print(f"  {coin}: no spot market  skipped")
            continue
        ohlc = _fetch_ohlc(ex, sym, days)
        if len(ohlc) < days:
            print(f"  {coin}: only {len(ohlc)} daily bars  skipped")
            continue
        arr = np.array(ohlc, dtype=float)
        data[coin] = (arr[:, 4], arr[:, 2], arr[:, 3])  # close, high, low
        print(f"   {coin}: {len(ohlc)} daily bars")

    if not data:
        print("\n  No data  aborting.\n")
        return 1

    # Per config: average metrics across coins (equal weight).
    print(
        f"\n  {'Rule':<12}{'Strat%':>9}{'B&H%':>9}{'StratDD':>9}{'B&HDD':>8}"
        f"{'Trades':>8}{'%InMkt':>8}  Verdict"
    )
    print("  " + "-" * 86)

    summary = []
    for name, fn in CONFIGS:
        per = [_backtest_trend(c, h, low, fn) for c, h, low in data.values()]
        avg = {k: float(np.mean([p[k] for p in per])) for k in per[0]}
        v = _verdict(avg["strat_ret"], avg["bh_ret"], avg["strat_dd"], avg["bh_dd"])
        summary.append((name, avg, v))
        print(
            f"  {name:<12}{avg['strat_ret']:>8.1f}%{avg['bh_ret']:>8.1f}%"
            f"{avg['strat_dd']:>8.1f}%{avg['bh_dd']:>7.1f}%"
            f"{avg['n_trades']:>8.0f}{avg['pct_in']:>7.0f}%  {v}"
        )
    print("  " + "-" * 86)

    beats = [s for s in summary if s[2] in ("BEATS B&H", "SAFER", "HIGHER RET")]
    print("\n  READ-OUT")
    if len(beats) >= 3:
        print(
            f"  Trend-following looks PROMISING: {len(beats)}/{len(summary)} "
            f"rules beat or de-risked buy-and-hold ROBUSTLY (not one cherry-"
        )
        print("    picked setting). Next step: build the long/flat trend bot on")
        print("    the majors, validate in SIM. This is a real candidate.")
    elif beats:
        print(f"  MIXED: only {len(beats)}/{len(summary)} rules helped. Could be")
        print("    a specific-rule effect, not a robust edge. Borderline.")
    else:
        print("  NO trend edge here: every rule underperformed buy-and-hold on")
        print("    both return AND drawdown. On this universe/period, even trend-")
        print("  following doesn't add value  the honest answer is paper/flat.")

    if do_sweep:
        print("\n" + "=" * 90)
        print("  ROBUSTNESS SWEEP")
        print("=" * 90)
        _print_sweep(data)

    print("\n  NOTE: 'SAFER' = kept most upside with much smaller drawdown  the")
    print("  classic trend-following benefit. 'WORSE' = whipsawed vs just holding.")
    print(f"  Generated: {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    print("=" * 90)
    return 0


if __name__ == "__main__":
    sys.exit(main())
