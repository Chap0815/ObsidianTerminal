"""
tools/pairs_check.py  Pairs-trading (spread mean-reversion) edge scanner.

Answers BOTH questions for strategy B-pairs, BEFORE building any bot:
  1. WHICH pairs are worth trading?  ranks every coin pair empirically.
  2. Does pairs trading have an EDGE?  backtests a z-score strategy net of
     costs over N days and reports the result.

Method
------
For each pair (A, B) of the given coins:
  hedge ratio  = OLS slope of log(A) on log(B)
  spread  = log(A)  log(B)
  half-life  = Ornstein-Uhlenbeck mean-reversion speed (lower = faster,
                     tradeable; very high / negative = NOT mean-reverting)
  z-score  = (spread  rolling mean) / rolling std
  STRATEGY: short the spread when z > +entry (short A / long B), long the
    spread when z < entry, exit near z0, stop if |z| > stop. Each round-trip
    pays a cost on BOTH legs. We sum the net result over the period.

A good pair: low half-life, positive net return, enough trades, decent win
rate. The backtest IS the test  if z-score entries don't clear costs, the
"cointegration" is not tradeable.

What it does NOT model
----------------------
  Per-leg liquidation (pairs are usually run low-leverage / market-neutral).
  Live execution slip beyond the flat per-trade cost below.
  Yield is in spread units  % of ONE leg's notional.

Run
---
  python pairs_check.py                       # 90d, BTC ETH BNB XRP SOL
  python pairs_check.py 60                     # 60 days
  python pairs_check.py 90 BTC ETH SOL AVAX    # custom universe
"""
from __future__ import annotations

import os
import sys
import time
import itertools
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from config.exchange_config import (get_futures_exchange_connection,
                                      get_active_exchange_name)

DEFAULT_DAYS  = 90
DEFAULT_COINS = ["BTC", "ETH", "BNB", "XRP", "SOL"]

# Z-score strategy params (env-overridable).
WINDOW  = int(os.getenv("PAIR_WINDOW",  "48"))  # rolling bars (1h  2 days)
BETA_WINDOW = int(os.getenv("PAIR_BETA_WINDOW", "168"))  # trailing  window (1 week)
ENTRY_Z  = float(os.getenv("PAIR_ENTRY_Z", "2.0"))
EXIT_Z   = float(os.getenv("PAIR_EXIT_Z",  "0.5"))
STOP_Z   = float(os.getenv("PAIR_STOP_Z",  "3.5"))
# Cost of ONE complete pair trade (open+close, BOTH legs) as a fraction of one
# leg's notional. ~4 taker legs  ~0.06% + slippage. Override PAIR_COST_PCT.
COST     = float(os.getenv("PAIR_COST_PCT", "0.30")) / 100.0


def _fetch_closes(ex, symbol: str, days: int) -> dict:
    """Paginated 1h closes  {timestamp_ms: close}. Robust to per-request cap."""
    tf_ms  = 3_600_000
    needed = days * 24 + 5
    now_ms = ex.milliseconds()
    since  = now_ms - needed * tf_ms
    out, prev = {}, None
    rl = max(0.0, getattr(ex, "rateLimit", 100) / 1000.0)
    for _ in range(needed // 200 + 6):
        try:
            batch = ex.fetch_ohlcv(symbol, "1h", since=since, limit=1000)
        except Exception as e:
            print(f"   [WARN] {symbol}: {type(e).__name__}: {e}")
            break
        if not batch:
            break
        for c in batch:
            out[c[0]] = c[4]
        last = batch[-1][0]
        if prev is not None and last <= prev:
            break
        prev = last
        since = last + tf_ms
        if since >= now_ms or len(batch) < 2:
            break
        if rl:
            time.sleep(rl)
    return out


def _half_life(spread: np.ndarray) -> float:
    """Ornstein-Uhlenbeck half-life from s_t = a + bs_{t-1}. Lower = faster
    reversion. Returns +inf when not mean-reverting (b >= 0)."""
    s_lag = spread[:-1]
    d_s   = np.diff(spread)
    s_lag = s_lag - s_lag.mean()
    denom = float(np.dot(s_lag, s_lag))
    if denom <= 0:
        return float("inf")
    b = float(np.dot(s_lag, d_s) / denom)
    if b >= 0:
        return float("inf")
    return float(-np.log(2) / np.log(1 + b))


def _backtest_pair(la: np.ndarray, lb: np.ndarray):
    """Z-score spread backtest with NO lookahead. Returns stats or None.

    The hedge ratio  is estimated on a TRAILING window at every step (not the
    whole sample), and the z-score uses only past spread values. PnL is the
    realised return of holding the position with the ENTRY . This is what
    kills the in-sample- false positive: two unrelated random walks no longer
    look cointegrated because no stable trailing  / mean-reverting spread
    exists out-of-sample."""
    n  = len(la)
    wb = BETA_WINDOW
    if n < wb + WINDOW + 30:
        return None

    # Trailing hedge ratio + spread (past data only at each i).
    spread = np.full(n, np.nan)
    betas  = np.full(n, np.nan)
    for i in range(wb, n):
        x = lb[i - wb:i]; y = la[i - wb:i]
        xc = x - x.mean(); v = float(np.dot(xc, xc))
        if v <= 0:
            continue
        b = float(np.dot(xc, y - y.mean()) / v)
        betas[i]  = b
        spread[i] = la[i] - b * lb[i]

    valid = spread[~np.isnan(spread)]
    hl = _half_life(valid) if len(valid) > 30 else float("inf")

    pos = 0          # +1 long spread, -1 short spread, 0 flat
    e_i = 0
    e_beta = 0.0
    nets, wins = [], 0
    start = wb + WINDOW
    for i in range(start, n):
        if np.isnan(spread[i]):
            continue
        win = spread[i - WINDOW:i]
        win = win[~np.isnan(win)]
        if len(win) < WINDOW // 2:
            continue
        mu, sd = win.mean(), win.std()
        if sd <= 0:
            continue
        z = (spread[i] - mu) / sd

        if pos == 0:
            if z > ENTRY_Z:
                pos, e_i, e_beta = -1, i, betas[i]      # short spread
            elif z < -ENTRY_Z:
                pos, e_i, e_beta = +1, i, betas[i]      # long spread
        else:
            if abs(z) < EXIT_Z or abs(z) > STOP_Z:
                # realised PnL of holding the position with the ENTRY .
                dA  = la[i] - la[e_i]
                dB  = lb[i] - lb[e_i]
                raw = (dA - e_beta * dB) if pos == +1 else (e_beta * dB - dA)
                net = raw - COST
                nets.append(net)
                if net > 0:
                    wins += 1
                pos = 0

    if not nets:
        return None
    net_arr  = np.array(nets)
    total    = float(net_arr.sum()) * 100.0          # % of one-leg notional
    n_tr     = len(nets)
    win_rate = 100.0 * wins / n_tr
    ann      = total * (365.0 * 24.0 / max(1, n - start))
    return {
        "beta": float(np.nanmean(betas)), "half_life": hl, "net_pct": total,
        "ann_pct": ann, "n_trades": n_tr, "win_rate": win_rate,
    }


def _verdict(net_pct: float, n_tr: int, win_rate: float, hl: float) -> str:
    if hl == float("inf") or hl <= 0 or hl > 400:
        return "NOT MEAN-REV"
    if net_pct >= 5.0 and n_tr >= 10 and win_rate >= 55.0:
        return "PROMISING"
    if net_pct >= 1.0 and n_tr >= 8:
        return "MARGINAL"
    return "NO EDGE"


def main():
    args  = sys.argv[1:]
    days  = int(args[0]) if args and args[0].isdigit() else DEFAULT_DAYS
    coins = [a.upper() for a in args[1:]] if len(args) > 1 else DEFAULT_COINS

    print("=" * 86)
    print("  PAIRS-TRADING EDGE SCANNER (spread mean-reversion)")
    print(f"  {days}d | window {WINDOW}h | entry z={ENTRY_Z} exit z={EXIT_Z} "
          f"stop z={STOP_Z} | cost {COST*100:.2f}%/trade")
    print("=" * 86)

    print(f"\n  Connecting to {get_active_exchange_name().upper()} ...")
    ex = get_futures_exchange_connection()
    ex.timeout = 30000
    try:
        ex.load_markets()
    except Exception as e:
        print(f"  Connection failed: {e}")
        sys.exit(1)
    print("  Connected. Loading price history ...\n")

    series = {}
    for coin in coins:
        sym = f"{coin}/USDT:USDT"
        if sym not in ex.markets:
            alt = next((m for m in ex.markets
                        if m.upper().startswith(f"{coin}/USDT") and ":" in m), None)
            if alt is None:
                print(f"  {coin}: no perp market  skipped")
                continue
            sym = alt
        closes = _fetch_closes(ex, sym, days)
        if len(closes) < 100:
            print(f"  {coin}: too little data ({len(closes)})  skipped")
            continue
        series[coin] = closes
        print(f"   {coin}: {len(closes)} bars")

    if len(series) < 2:
        print("\n  Need at least 2 coins with data  aborting.\n")
        return

    # Align on common timestamps.
    common = sorted(set.intersection(*[set(s.keys()) for s in series.values()]))
    if len(common) < WINDOW + 50:
        print(f"\n  Only {len(common)} common bars  not enough overlap.\n")
        return
    logs = {c: np.log(np.array([series[c][t] for t in common], dtype=float))
            for c in series}
    _d0 = datetime.fromtimestamp(common[0] / 1000, timezone.utc)
    _d1 = datetime.fromtimestamp(common[-1] / 1000, timezone.utc)
    print(f"\n  Aligned on {len(common)} common bars "
          f"({_d0:%Y-%m-%d}  {_d1:%Y-%m-%d})\n")

    header = (f"  {'Pair':<14}{'HalfLife':>10}{'Trades':>8}{'WinRate':>9}"
              f"{'Net%':>9}{'~APR%':>9}  Verdict")
    print(header)
    print("  " + "-" * (len(header) - 2))

    rows = []
    for a, b in itertools.combinations(series.keys(), 2):
        res = _backtest_pair(logs[a], logs[b])
        if res is None:
            continue
        v = _verdict(res["net_pct"], res["n_trades"], res["win_rate"],
                     res["half_life"])
        rows.append((f"{a}/{b}", res, v))

    # Sort: promising first, then by net %.
    order = {"PROMISING": 0, "MARGINAL": 1, "NO EDGE": 2, "NOT MEAN-REV": 3}
    rows.sort(key=lambda r: (order.get(r[2], 9), -r[1]["net_pct"]))

    for name, res, v in rows:
        hl = res["half_life"]
        hl_s = "" if hl == float("inf") else f"{hl:.0f}h"
        print(f"  {name:<14}{hl_s:>10}{res['n_trades']:>8}"
              f"{res['win_rate']:>8.0f}%{res['net_pct']:>8.1f}%"
              f"{res['ann_pct']:>8.1f}%  {v}")

    print("  " + "-" * (len(header) - 2))

    promising = [r for r in rows if r[2] == "PROMISING"]
    print("\n  READ-OUT")
    if promising:
        best = promising[0]
        print(f"  {len(promising)} pair(s) look PROMISING. Best: "
              f"{best[0]} ({best[1]['net_pct']:.1f}% net over {days}d, "
              f"half-life {best[1]['half_life']:.0f}h).")
        print("    Next step: build the 2-leg z-score executor for the top")
        print("    pair(s) and validate in SIM before any real size.")
    elif any(r[2] == "MARGINAL" for r in rows):
        print("  Only MARGINAL pairs. Thin edge after costs  would need")
        print("    lower fees / maker entries to be worthwhile. Not yet.")
    else:
        print("  NO tradeable pair found. The spreads either don't mean-")
        print("    revert or don't clear costs. Don't build pairs trading on")
        print("  this universe  try a different coin set or strategy.")

    print("\n  NOTE: Net% is per one-leg notional. A pair trade uses capital on")
    print("  BOTH legs, so yield on deployed capital is roughly half this")
    print("  (before any leverage). ~APR is a crude annualisation of Net%.")
    print(f"  Generated: {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    print("=" * 86)


if __name__ == "__main__":
    main()
