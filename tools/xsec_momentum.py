"""Cross-sectional (relative-strength) momentum test.

Hypothese: long die strksten K Coins / short die schwchsten K (marktneutral),
periodisch rebalancen. Marktneutral  Edge unabhngig von der Marktrichtung
(funktioniert auch im aktuellen Angst-Markt). Diversifiziert  kein Einzelcoin-
Tail. Misst NETTO nach konservativen Kosten (Round-Trip 0.40%, one-way 0.20%
pro Namens-Rotation).

Run: PYTHONIOENCODING=utf-8 python -m tools.xsec_momentum [days]
"""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

import math
import os
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

_TOOL_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TOOL_PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _TOOL_PROJECT_ROOT)

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

import pandas as pd

from tools.backtester import connect_exchange, get_top_volume_coins, fetch_history

DEFAULT_DAYS = 180
MAX_DAYS = 3650


def _parse_xsec_days(args=None) -> int:
    values = sys.argv[1:] if args is None else list(args)
    if not values:
        return DEFAULT_DAYS
    try:
        days = int(values[0])
    except (TypeError, ValueError, OverflowError):
        return DEFAULT_DAYS
    return days if 1 <= days <= MAX_DAYS else DEFAULT_DAYS


DAYS = _parse_xsec_days()
FEE_ONE_WAY = 0.0006  # futures taker 0.01% + slippage 0.05% per side


def _validated_close_series(df):
    if not isinstance(df, pd.DataFrame) or not {"dt", "close"}.issubset(df.columns):
        return None
    frame = df.loc[:, ["dt", "close"]].copy()
    frame["dt"] = pd.to_datetime(frame["dt"], errors="coerce", utc=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame[
        frame["dt"].notna()
        & frame["close"].map(
            lambda value: math.isfinite(float(value)) and float(value) > 0.0
        )
    ]
    if frame.empty:
        return None
    frame = frame.drop_duplicates(subset="dt", keep="last").sort_values("dt")
    return frame.set_index("dt")["close"].astype(float)


def load_panel(ex, coins):
    hist = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(fetch_history, ex, c, DAYS): c for c in coins}
        for f in as_completed(futs):
            try:
                df = f.result()
                series = _validated_close_series(df)
                if series is not None:
                    hist[futs[f]] = series
            except Exception:
                pass
    panel = pd.DataFrame(hist).sort_index()
    panel = panel.resample("1h").last().ffill(limit=3)
    return panel


def run(prices, L, reb, K, fee=FEE_ONE_WAY, fund_day=0.0):
    """L=lookback (h), reb=rebalance (h), K=Korbgre je Seite,
    fee=one-way Kosten, fund_day=Funding-Drag pro Tag auf dem Buch."""
    n = len(prices)
    prev_long, prev_short = set(), set()
    ls_rets, lo_rets = [], []  # long-short (neutral) und long-only
    funding = fund_day * (reb / 24.0)  # Drag pro Hold-Periode
    i = L
    while i + reb < n:
        past = (prices.iloc[i] / prices.iloc[i - L] - 1).dropna()
        fwd_row = prices.iloc[i + reb] / prices.iloc[i] - 1
        valid = past.index[fwd_row.reindex(past.index).notna()]
        past = past.loc[valid]
        if len(past) < 2 * K + 2:
            i += reb
            continue
        ranked = past.sort_values(ascending=False)
        longs = list(ranked.index[:K])
        shorts = list(ranked.index[-K:])
        long_ret = float(fwd_row[longs].mean())
        short_ret = float(fwd_row[shorts].mean())

        ch_l = len(set(longs) ^ prev_long)
        ch_s = len(set(shorts) ^ prev_short)
        cost = 0.5 * (ch_l / K) * fee + 0.5 * (ch_s / K) * fee
        prev_long, prev_short = set(longs), set(shorts)

        ls_rets.append(0.5 * (long_ret - short_ret) - cost - funding)
        lo_rets.append(long_ret - (ch_l / K) * fee)
        i += reb
    return ls_rets, lo_rets


GRID = [(L, reb, K) for L in (24, 72, 168) for reb in (24, 72) for K in (5, 8)]


def best_config(seg, fee, fund_day):
    """Whle die Config mit hchstem Sharpe AUF DIESEM Segment (= Training)."""
    best = None
    for L, reb, K in GRID:
        s = stats(run(seg, L, reb, K, fee, fund_day)[0])
        if s and (best is None or s[4] > best[1]):
            best = ((L, reb, K), s[4])
    return best[0] if best else None


def stats(rets):
    if not rets:
        return None
    eq = 1.0
    for r in rets:
        eq *= 1 + r
    total = (eq - 1) * 100
    wr = 100 * sum(1 for r in rets if r > 0) / len(rets)
    mean = statistics.mean(rets)
    sd = statistics.stdev(rets) if len(rets) > 1 else 0
    sharpe = (mean / sd * math.sqrt(len(rets))) if sd > 0 else 0.0  # ber den Zeitraum
    return total, wr, mean * 100, len(rets), sharpe


def grid(prices, label):
    print(f"\n#### {label}  ({prices.shape[1]} coins  {prices.shape[0]} bars) ####")
    hdr = (
        f"{'L(h)':>5} {'reb(h)':>6} {'K':>3} | "
        f"{'LS tot%':>8} {'LS WR':>6} {'LS %':>7} {'LS Sharpe':>9} {'reb#':>5}"
    )
    print(hdr)
    print("-" * len(hdr))
    best = None
    for L in (24, 72, 168):
        for reb in (24, 72):
            for K in (5, 8):
                ls, _ = run(prices, L, reb, K)
                s = stats(ls)
                if not s:
                    continue
                lt, lwr, lmu, ln, lsh = s
                star = " *" if lt > 0 and lsh > 0.3 else ""
                print(
                    f"{L:>5} {reb:>6} {K:>3} | "
                    f"{lt:>+7.1f}% {lwr:>5.0f}% {lmu:>+6.2f}% {lsh:>+9.2f} {ln:>5d}{star}"
                )
                if best is None or lsh > best[0]:
                    best = (lsh, L, reb, K)
    return best


def equity_stats(rets):
    """(total%, maxDD%, Sharpe-t, n) aus einer per-Rebalance-Return-Liste."""
    if not rets:
        return (0.0, 0.0, 0.0, 0)
    eq = peak = 1.0
    mdd = 0.0
    for r in rets:
        eq *= 1 + r
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak)
    sd = statistics.stdev(rets) if len(rets) > 1 else 0
    sh = (statistics.mean(rets) / sd * math.sqrt(len(rets))) if sd > 0 else 0.0
    return ((eq - 1) * 100, mdd * 100, sh, len(rets))


def apply_filter(rets, kind, W=4, target=0.04):
    """Crash-Filter als Exposure-Overlay  nutzt NUR vergangene Returns (kein
    Look-Ahead). voltarget: Exposure ~ target/recent_vol (cap 1.0). ownmom:
    flach wenn letzte W Rebalances im Schnitt negativ. combo: beide."""
    if kind == "none":
        return list(rets)
    out = []
    for t in range(len(rets)):
        if t < W:
            out.append(rets[t])
            continue
        past = rets[t - W : t]
        exp = 1.0
        if kind in ("voltarget", "combo"):
            sd = statistics.stdev(past) or 1e-9
            exp *= min(1.0, target / sd)
        if kind in ("ownmom", "combo"):
            if statistics.mean(past) < 0:
                exp *= 0.0
        out.append(exp * rets[t])
    return out


def main():
    ex = connect_exchange()
    coins = get_top_volume_coins(ex, n=120)  # breiteres Universum
    print(f"Coins angefragt: {len(coins)} | loading {DAYS}d ...")
    prices = load_panel(ex, coins)

    if "BTC" in prices.columns:
        b = prices["BTC"].dropna()
        print(f"Benchmark BTC B&H: {(b.iloc[-1] / b.iloc[0] - 1) * 100:+.1f}%")

    #  De-Bias: nur Coins mit VOLLER Historie (am Anfang UND Ende vorhanden)
    head = prices.iloc[:48].notna().all()
    tail = prices.iloc[-48:].notna().all()
    stable = prices.loc[:, head & tail]
    print(
        f"Voll-Historie-Universum: {stable.shape[1]} von {prices.shape[1]} Coins "
        f"(frisch gelistete Pumper entfernt)"
    )

    REAL_FEE, REAL_FUND = 0.0006, 0.0006
    L, reb, K = 24, 72, 8

    #  B) CRASH-FILTER: Vergleich auf dem GANZEN Fenster
    base = run(stable, L, reb, K, REAL_FEE, REAL_FUND)[0]
    print(
        f"\n#### B) CRASH-FILTER  (Config L={L} reb={reb} K={K}, "
        f"fee {REAL_FEE * 100:.2f}% + funding {REAL_FUND * 100:.2f}%/d) ####"
    )
    print(f"{'Filter':>12} | {'total%':>8} {'maxDD%':>7} {'Sharpe':>7}")
    print("-" * 42)
    for kind in ("none", "voltarget", "ownmom", "combo"):
        t, dd, sh, _ = equity_stats(apply_filter(base, kind))
        print(f"{kind:>12} | {t:>+7.1f}% {dd:>6.1f}% {sh:>+6.2f}")

    #  Walk-Forward mit dem besten Filter (combo) vs ohne
    n = stable.shape[0]
    nwin = max(4, n // (45 * 24))  # ~45d je Fenster
    seg = n // nwin
    print(
        f"\n#### WALK-FORWARD  (~{seg // 24}d/Fenster, baseline vs combo-Filter) ####"
    )
    eq_b = eq_f = 1.0
    all_b, all_f = [], []
    for w in range(1, nwin):
        tr = stable.iloc[(w - 1) * seg : w * seg]
        te = stable.iloc[w * seg : (w + 1) * seg]
        cfg = best_config(tr, REAL_FEE, REAL_FUND)
        if cfg is None:
            continue
        raw = run(te, *cfg, REAL_FEE, REAL_FUND)[0]
        filt = apply_filter(raw, "combo")
        sb, sf = equity_stats(raw), equity_stats(filt)
        all_b += raw
        all_f += filt
        eq_b *= 1 + sb[0] / 100
        eq_f *= 1 + sf[0] / 100
        print(
            f"  Fenster {w} (cfg L={cfg[0]} reb={cfg[1]} K={cfg[2]}):  "
            f"baseline {sb[0]:>+6.1f}% (DD {sb[1]:.0f}%)   "
            f"combo-Filter {sf[0]:>+6.1f}% (DD {sf[1]:.0f}%)"
        )
    tb, tf = equity_stats(all_b), equity_stats(all_f)
    print(
        f"  kombiniert OOS:  baseline {(eq_b - 1) * 100:+.1f}% (maxDD {tb[1]:.0f}%)  "
        f"combo-Filter {(eq_f - 1) * 100:+.1f}% (maxDD {tf[1]:.0f}%)"
    )


if __name__ == "__main__":
    main()
