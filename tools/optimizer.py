"""
optimizer.py  Strategie-Optimierung mit Robustheits-Validierung

Features:
  K-Fold Cross-Validation (4 Perioden)
    Configs mssen in ALLEN Perioden profitabel sein
    Filtert Glcks-Configs heraus die nur 1 gute Phase erwischten
  Sensitivitts-Analyse fr Top-Configs
    Jeder Parameter wird 1 Schritt variiert
    Zeigt ob Config "robust" oder "fragil" (overfit) ist
  Robustness Score statt nur Composite Score
    score  (1 - variance_across_folds) belohnt Konsistenz

USAGE:
  python optimizer.py TREND
  python optimizer.py SPOT 60
  python optimizer.py TREND 90 --maker
  python optimizer.py SPOT 60 --top 20
  python optimizer.py TREND --quick
  python optimizer.py TREND --kfold 5     # 5 Folds statt 4
  python optimizer.py TREND --no-sensitivity  # ohne Sensitivitts-Analyse

DAUER:
  Standard (4 Folds + Sensitivity): ~30-90 Minuten TREND, ~10-25 Min. SPOT
  --quick: ~5-15 Minuten
"""

import sys
import os
import time
import math
import itertools
import csv
import json as _json
import random as _random
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

try:
    from scipy.stats import norm as _scipy_norm
except Exception:
    _scipy_norm = None

from config.exchange_config import get_spot_exchange_connection
from tools.backtester import (fetch_history, get_top_volume_coins,
                              filter_universe_by_history,
                              precompute_index, simulate_fast,
                              calc_round_trip, DEFAULT_DAYS, INITIAL_CAPITAL,
                              _compute_stats)
from core.logger import log_separator

#  Deterministic optimizer runs (fixed seed) 
# monte_carlo_perturbation seeds its RNG with this so runs are reproducible.
# Override via OPTIMIZER_SEED env.
_OPTIMIZER_SEED = int(os.getenv("OPTIMIZER_SEED", "42"))


#  Lpez de Prado anti-overfit constants 
# Indicators (RSI14, MACD 12/26/9, EMA50, 24h change) are computed over the FULL
# per-symbol series in the backtester, so the indicator state at a fold's first
# bars is warmed by the PREVIOUS fold. PURGE_BARS drops each fold's leading
# timestamps so its signals are warmed from WITHIN the fold; EMBARGO_FRAC adds a
# gap between adjacent folds. PURGE_BARS = max indicator lookback in bars (EMA50).
PURGE_BARS   = 50
EMBARGO_FRAC = 0.01
_EULER_GAMMA = 0.5772156649015329


def _norm_cdf(x: float) -> float:
    """Standard-normal CDF  (scipy if available, else math.erf)."""
    if _scipy_norm is not None:
        return float(_scipy_norm.cdf(x))
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    """Inverse standard-normal CDF (quantile). scipy if available, else the
    Acklam rational approximation (abs error < 1.15e-9)."""
    if _scipy_norm is not None:
        return float(_scipy_norm.ppf(p))
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1.0 - 0.02425
    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1.0)
    if p > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1.0)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5]) * q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1.0)


def deflated_sharpe_ratio(sr_list: list, sr_best: float, n_obs: int,
                          skew: float = 0.0, kurt: float = 3.0) -> dict:
    """Deflated Sharpe Ratio (Lpez de Prado / Bailey).

    sr_list : per-trade Sharpe of EVERY tested config (selection universe)
    sr_best : per-trade Sharpe of the selected (best) config
    n_obs   : number of return observations (trades) of the best config
    skew    : skewness of the best config's per-trade returns
    kurt    : kurtosis (NON-excess; Normal == 3) of the best config's returns

    sr0 = sqrt(var_sr) * ((1-)*Z(1-1/N) + *Z(1-1/(N*e)))
    DSR = ( (sr_best - sr0)*sqrt(T-1) /
             sqrt(1 - skew*sr_best + ((kurt-1)/4)*sr_best^2) )
    """
    finite = [s for s in sr_list if s is not None and math.isfinite(s)]
    n = len(finite)
    if n < 2 or n_obs is None or n_obs < 2:
        return {"dsr": None, "sr0": None, "n_trials": n,
                "reason": "insufficient_stats"}
    var_sr = statistics.variance(finite)
    if var_sr <= 0.0:
        return {"dsr": None, "sr0": None, "n_trials": n,
                "reason": "zero_variance"}
    z1 = _norm_ppf(1.0 - 1.0 / n)
    z2 = _norm_ppf(1.0 - 1.0 / (n * math.e))
    sr0 = math.sqrt(var_sr) * ((1.0 - _EULER_GAMMA) * z1 + _EULER_GAMMA * z2)
    denom = 1.0 - skew * sr_best + ((kurt - 1.0) / 4.0) * sr_best * sr_best
    if denom <= 0.0:
        return {"dsr": None, "sr0": sr0, "n_trials": n,
                "reason": "nonpositive_denominator"}
    z = (sr_best - sr0) * math.sqrt(n_obs - 1) / math.sqrt(denom)
    return {"dsr": _norm_cdf(z), "sr0": sr0, "n_trials": n, "n_obs": n_obs}


def _logit(x: float) -> float:
    eps = 1e-9
    x = min(1.0 - eps, max(eps, x))
    return math.log(x / (1.0 - x))


def probability_of_backtest_overfitting(perf_matrix: list) -> dict:
    """Probability of Backtest Overfitting (PBO) via CSCV.

    perf_matrix : list of per-config performance rows, each row a list of the
                  config's net across the k folds  matrix [config  fold].

    Partition the k folds into two equal halves in every C(k, k/2) way (one half
    = IS, the complement = OOS). For each partition pick the IS-best config, find
    its OOS rank, and compute the logit of its relative OOS rank. PBO = fraction
    of partitions whose IS-best config lands at or below the OOS median.
    """
    if not perf_matrix or len(perf_matrix) < 2:
        return {"pbo": None, "n_partitions": 0, "reason": "too_few_configs"}
    n_cfg = len(perf_matrix)
    k = len(perf_matrix[0])
    if k < 2 or any(len(row) != k for row in perf_matrix):
        return {"pbo": None, "n_partitions": 0, "reason": "ragged_or_too_few_folds"}
    half = k // 2
    if half < 1:
        return {"pbo": None, "n_partitions": 0, "reason": "too_few_folds"}
    all_folds = list(range(k))
    logits = []
    below_median = 0
    for is_idx in itertools.combinations(all_folds, half):
        is_set = set(is_idx)
        oos_idx = [j for j in all_folds if j not in is_set]
        is_perf = [statistics.mean(perf_matrix[c][j] for j in is_idx)
                   for c in range(n_cfg)]
        oos_perf = [statistics.mean(perf_matrix[c][j] for j in oos_idx)
                    for c in range(n_cfg)]
        best_c = max(range(n_cfg), key=lambda c: is_perf[c])
        ranked = sorted(range(n_cfg), key=lambda c: oos_perf[c])
        oos_rank = ranked.index(best_c)  # 0 = worst OOS
        rel_rank = (oos_rank + 1) / (n_cfg + 1)
        logits.append(_logit(rel_rank))
        if rel_rank <= 0.5:
            below_median += 1
    n_part = len(logits)
    if n_part == 0:
        return {"pbo": None, "n_partitions": 0, "reason": "no_partitions"}
    return {
        "pbo":          below_median / n_part,
        "n_partitions": n_part,
        "median_logit": statistics.median(logits),
    }


def _sample_skew_kurt(xs: list) -> tuple:
    """Sample skewness and NON-excess kurtosis (Normal  3.0) of a return list.
    Returns (0.0, 3.0) when stats are undefined so DSR degrades to the Normal
    case rather than failing."""
    n = len(xs)
    if n < 3:
        return 0.0, 3.0
    mean = statistics.mean(xs)
    m2 = sum((x - mean) ** 2 for x in xs) / n
    if m2 <= 0.0:
        return 0.0, 3.0
    m3 = sum((x - mean) ** 3 for x in xs) / n
    m4 = sum((x - mean) ** 4 for x in xs) / n
    skew = m3 / (m2 ** 1.5)
    kurt = m4 / (m2 ** 2)
    return skew, kurt


#  Incremental result persistence 
# The optimizer is a long job (30-90 min). Each completed result is streamed to
# a JSONL file as it's computed so a Ctrl+C / OOM never loses partial progress.
_INCREMENTAL_PATH = None


def _set_incremental_path(strategy: str, days: int) -> None:
    global _INCREMENTAL_PATH
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base, "logs")
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError:
        log_dir = base
    _INCREMENTAL_PATH = os.path.join(
        log_dir, f"optimizer_{strategy}_{days}d_{ts}.jsonl")


def _persist_result(record: dict) -> None:
    """Append one optimizer result to disk."""
    if not _INCREMENTAL_PATH:
        return
    try:
        # Drop non-JSON-serializable values defensively
        safe = {}
        for k, v in record.items():
            try:
                _json.dumps(v)
                safe[k] = v
            except (TypeError, ValueError):
                safe[k] = str(v)
        with open(_INCREMENTAL_PATH, "a", encoding="utf-8") as fh:
            fh.write(_json.dumps(safe, default=str) + "\n")
    except Exception:
        # NEVER let persistence kill the optimizer
        pass


#  Suchrume 

FULL_SPACE = {
    "TREND": {
        "min_pump":          [2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        "activation_profit": [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        "trailing_distance": [0.8, 1.0, 1.5, 2.0, 2.5, 3.0],
        "stop_loss":         [-1.0, -1.5, -2.0, -2.5, -3.0, -4.0],
        "partial_pct":       [0.30, 0.40, 0.50, 0.60],
        "rsi_max":           [60.0, 65.0, 70.0, 75.0, 80.0],
    },
    "SPOT": {
        "min_pump":          [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "activation_profit": [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "trailing_distance": [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0],
        # stop_loss is a searched dimension (wide range  spot is unleveraged
        # with 4-10% targets). NOTE: +6x grid size; use --quick for a fast scan.
        "stop_loss":         [-3.0, -4.0, -6.0, -8.0, -10.0, -12.0],
        "partial_pct":       [0.25, 0.30, 0.40, 0.50, 0.60],
        "rsi_max":           [65.0, 70.0, 75.0, 80.0, 85.0],
    },
    # FUTURES strategy  uses spot data as proxy (perpetual data is more
    # limited and often shorter history). Results are indicative.
    "FUTURES": {
        "min_pump":          [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0],
        "activation_profit": [3.0, 3.5, 4.0, 4.5, 5.0, 6.0, 7.0],
        "trailing_distance": [1.5, 2.0, 2.5, 3.0, 3.5, 4.0],
        "stop_loss":         [-2.0, -2.5, -3.0, -3.5, -4.0, -5.0],
        "partial_pct":       [0.30, 0.40, 0.50, 0.60],
        "rsi_max":           [60.0, 65.0, 70.0, 75.0, 80.0],
    },
}

QUICK_SPACE = {
    strat: {k: v[::2] for k, v in params.items()}
    for strat, params in FULL_SPACE.items()
}


#  Walk-Forward Validation 
# Walk-forward beats normal K-Fold for time-series strategy testing because
# financial data is NOT IID  regimes, trends, volatility clusters mean a
# random validation slice "leaks" the future into training. Walk-forward
# always trains on past and tests on the unseen next slice, exactly like
# real deployment.
#
# Implementation: split timeline into S+1 chronological segments. For
# step i, "train" = segments[0..i], "test" = segment[i+1]. We don't
# actually retrain params (this is parameter validation, not model
# fitting); we just check whether the SAME config remains profitable on
# each forward segment. A robust config is profitable on every test slice.


def walk_forward_simulate(indexed: dict, all_times: list, strategy: str,
                            use_maker: bool, params: dict,
                            n_steps: int = 4) -> dict:
    """Walk-forward validation: split timeline chronologically, test
    same config on each forward slice. Returns per-slice nets + summary."""
    if n_steps < 2:
        n_steps = 2
    total = len(all_times)
    slice_len = total // (n_steps + 1)
    if slice_len < 10:
        # Not enough data  fall back to single backtest
        s = simulate_fast(indexed, all_times, strategy, use_maker, params)
        return {
            "slices":       [s],
            "slice_nets":   [s.get("net", 0)],
            "consistency":  1.0 if s.get("net", 0) > 0 else 0.0,
            "all_profitable": s.get("net", 0) > 0,
            "fragility":    "single-slice (data too short)",
        }

    embargo = int(total * EMBARGO_FRAC) if EMBARGO_FRAC > 0 else 0
    slice_nets = []
    slice_results = []
    for step in range(n_steps):
        # Forward test slice: position (step+1) of (n_steps+1) total
        start = (step + 1) * slice_len
        end   = (step + 2) * slice_len if step < n_steps - 1 else total
        # Purge leading lookback bars + embargo gap so each forward slice's
        # signals are warmed from WITHIN the slice (no cross-fold leakage).
        purged_start = min(end, start + embargo + max(0, PURGE_BARS))
        period_times = all_times[purged_start:end]
        if len(period_times) < 10:
            period_times = all_times[start:end]
        period_data  = filter_to_period(indexed, set(period_times))
        s = simulate_fast(period_data, period_times, strategy, use_maker, params)
        slice_nets.append(s.get("net", 0))
        slice_results.append(s)

    profitable_count = sum(1 for n in slice_nets if n > 0)
    avg = statistics.mean(slice_nets)
    std = statistics.stdev(slice_nets) if len(slice_nets) > 1 else 0
    consistency = max(0.0, min(1.0,
        1 - (std / abs(avg)) if avg != 0 else 0.0
    ))
    # Walk-forward "passes" if every forward slice was profitable AND the
    # variance across slices is moderate (consistency > 0.3)
    return {
        "slices":         slice_results,
        "slice_nets":     slice_nets,
        "avg_net":        avg,
        "std_net":        std,
        "consistency":    consistency,
        "profitable_count": profitable_count,
        "all_profitable": profitable_count == n_steps,
        "robust":         profitable_count == n_steps and consistency > 0.3,
    }


#  Regime Splitting 
# Split the timeline by detected market regime and test the config
# separately on each. A "good" strategy on overall data can be a complete
# disaster in one specific regime (e.g. momentum works in bull, dies in chop).
#
# Regime detection: simple BTC-trend bucketing. Each tick is tagged
# BULL/BEAR/CHOP based on the rolling BTC change at that moment. This
# uses BTC change_% from the indexed data  no extra fetch needed.


def detect_regimes(indexed: dict, all_times: list) -> dict:
    """Classify each timestamp as BULL/BEAR/CHOP using BTC change_%.

    Buckets:
      BULL: BTC 24h change > +3%
      BEAR: BTC 24h change < -3%
      CHOP: in between (low volatility / sideways)
    """
    btc_data = indexed.get("BTC", {}) or indexed.get("BTC/USDT", {}) or {}
    if not btc_data:
        # No BTC reference  can't classify
        return {"BULL": [], "BEAR": [], "CHOP": all_times[:]}

    buckets = {"BULL": [], "BEAR": [], "CHOP": []}
    for t in all_times:
        tick = btc_data.get(t)
        chg = tick.get("change") if tick else None
        if chg is None:
            buckets["CHOP"].append(t)
        elif chg > 3.0:
            buckets["BULL"].append(t)
        elif chg < -3.0:
            buckets["BEAR"].append(t)
        else:
            buckets["CHOP"].append(t)
    return buckets


def regime_split_simulate(indexed: dict, all_times: list, strategy: str,
                           use_maker: bool, params: dict) -> dict:
    """Test BULL / BEAR / CHOP without cutting holes into the timeline.

    The simulation must run on continuous bars so stops, liquidation and trailing
    can fire after an entry even if the market regime changes. We then attribute
    each closed trade to the regime at entry time.
    """
    regimes = detect_regimes(indexed, all_times)
    regime_by_time = {
        t: regime_name
        for regime_name, regime_times in regimes.items()
        for t in regime_times
    }
    full = simulate_fast(indexed, all_times, strategy, use_maker, params)
    closed = list(full.get("closed_trades", []) or [])
    results = {}
    for regime_name, regime_times in regimes.items():
        if len(regime_times) < 10:
            results[regime_name] = {
                "net": 0, "trades": 0, "skipped": "insufficient_data"
            }
            continue
        regime_trades = [
            t for t in closed
            if regime_by_time.get(t.get("entry_time")) == regime_name
        ]
        if regime_trades:
            s = _compute_stats(
                regime_trades,
                sum(float(t.get("cost", 0.0) or 0.0) for t in regime_trades),
                sum(float(t.get("gross", 0.0) or 0.0) for t in regime_trades),
            )
        else:
            s = {"net": 0.0, "trade_count": 0, "edge": False}
        results[regime_name] = {
            "net":    s.get("net", 0),
            "trades": s.get("trade_count", 0),
            "edge":   s.get("edge", False),
            "ratio":  len(regime_times) / max(1, len(all_times)),
        }
    # "Survives all regimes" = profitable in every regime with sufficient data
    profitable_regimes = sum(
        1 for r in results.values()
        if r.get("net", 0) > 0 and "skipped" not in r
    )
    tested_regimes = sum(1 for r in results.values() if "skipped" not in r)
    return {
        "regimes":           results,
        "profitable_count":  profitable_regimes,
        "tested_count":      tested_regimes,
        "survives_all":      profitable_regimes == tested_regimes and tested_regimes >= 2,
    }


#  Outlier-Dependency Test 
# A strategy whose entire edge comes from 1-2 lucky home-run trades is
# fragile  in live trading those same outliers may never occur again.
# Test: remove the top N trades and recompute net. If the strategy still
# profits, the edge is broadly distributed.


def outlier_dependency_test(indexed: dict, all_times: list, strategy: str,
                              use_maker: bool, params: dict,
                              top_n_to_remove: list = (1, 5, 10)) -> dict:
    """Re-simulate while excluding the top N most-profitable trades."""
    s_full = simulate_fast(indexed, all_times, strategy, use_maker, params)
    full_net = s_full.get("net", 0)

    nets = sorted(s_full.get("net_trades") or [], reverse=True)
    trade_count = len(nets)

    results = {}
    for n in top_n_to_remove:
        if trade_count < n + 5:
            results[f"remove_top_{n}"] = {"skipped": "too_few_trades"}
            continue
        removed = sum(nets[:n])
        adjusted_net = full_net - removed
        results[f"remove_top_{n}"] = {
            "removed_net":    removed,
            "adjusted_net":   adjusted_net,
            "drop_pct":       (removed / abs(full_net) * 100) if full_net else 0,
            "still_positive": adjusted_net > 0,
        }

    r1 = results.get("remove_top_1", {})
    outlier_fragile = r1.get("still_positive") is False if "skipped" not in r1 else None

    return {
        "full_net":         full_net,
        "trade_count":      trade_count,
        "scenarios":        results,
        "outlier_fragile":  outlier_fragile,
    }


#  Monte-Carlo Equity Perturbation 
# Test if equity holds up when trade order/timing is shuffled. Many bad
# strategies only profit due to a lucky sequence; Monte Carlo destroys
# that illusion. We use the published net as the anchor and synthesize
# perturbations using random trade reordering (approximation; precise
# implementation needs per-trade detail).


def monte_carlo_perturbation(s_full: dict, n_runs: int = 100) -> dict:
    """Block-bootstrap the REAL per-trade net returns and report the share of
    resampled equity paths that end positive.

    Resamples contiguous blocks of actual trade P&L (USDT) with replacement,
    preserving the fat tails and local autocorrelation a Normal(avg, std)
    proxy destroys. Robust strategy >=90% positive; <70% concerning.
    """
    nets = list(s_full.get("net_trades") or [])
    trade_count = len(nets)
    if trade_count < 5:
        return {"runs": 0, "positive_share": None,
                "robust": None, "reason": "insufficient_stats"}

    rng = _random.Random(_OPTIMIZER_SEED)  # deterministic for reproducibility
    block = max(1, min(10, trade_count // 5))
    positive_runs = 0
    final_equities = []
    for _ in range(n_runs):
        seq = []
        while len(seq) < trade_count:
            start = rng.randrange(trade_count)
            seq.extend(nets[start:start + block])
        eq = sum(seq[:trade_count])
        final_equities.append(eq)
        if eq > 0:
            positive_runs += 1
    share = positive_runs / n_runs
    final_equities.sort()
    return {
        "runs":             n_runs,
        "positive_share":   share,
        "median_final":     final_equities[n_runs // 2],
        "worst_decile":     final_equities[max(0, n_runs // 10 - 1)],
        "robust":           share >= 0.90,
        "concerning":       share < 0.70,
        "method":           "block_bootstrap",
    }


#  K-Fold Validation 

def split_into_folds(all_times: list, k: int = 4,
                     purge_bars: int = PURGE_BARS,
                     embargo_frac: float = EMBARGO_FRAC) -> list:
    """Teilt Zeitstempel in K gleich groe, sortierte, PURGED+EMBARGOED Perioden.

    Each raw fold is a chronological slice. To kill cross-fold indicator leakage
    (indicators are warmed over the FULL series in the backtester), the leading
    `purge_bars` timestamps of every fold are PURGED so the fold's signals are
    warmed from WITHIN the fold, and an EMBARGO gap of `embargo_frac` of the
    total length is dropped from the FRONT of each fold (except the first) to
    separate adjacent folds. Resulting fold time-sets are disjoint with a gap."""
    n        = len(all_times)
    fold_len = n // k
    embargo  = int(n * embargo_frac) if embargo_frac > 0 else 0
    folds    = []
    for i in range(k):
        start = i * fold_len
        end   = (i + 1) * fold_len if i < k - 1 else n
        # Embargo gap before every fold after the first.
        purged_start = start + (embargo if i > 0 else 0) + max(0, purge_bars)
        if purged_start < end:
            folds.append(all_times[purged_start:end])
        else:
            folds.append([])
    return folds


def filter_to_period(indexed: dict, time_set: set) -> dict:
    """Filtert vorindexierte Daten auf eine Zeitperiode."""
    return {
        sym: {t: d for t, d in tick_map.items() if t in time_set}
        for sym, tick_map in indexed.items()
    }


def kfold_simulate(indexed: dict, folds: list, strategy: str,
                   use_maker: bool, params: dict) -> dict:
    """
    Testet die Config auf K unabhngigen Perioden.
    Eine Config gilt als robust wenn sie in ALLEN Folds profitabel ist.
    """
    fold_results = []
    for period_times in folds:
        period_data = filter_to_period(indexed, set(period_times))
        s           = simulate_fast(period_data, period_times, strategy, use_maker, params)
        fold_results.append(s)

    nets        = [s.get("net", -9999) for s in fold_results]
    edge_count  = sum(1 for s in fold_results if s.get("edge"))
    profit_cnt  = sum(1 for n in nets if n > 0)
    avg_net     = statistics.mean(nets)
    std_net     = statistics.stdev(nets) if len(nets) > 1 else 0
    consistency = 1 - (std_net / abs(avg_net)) if avg_net != 0 else 0
    consistency = max(0, min(1, consistency))  # auf [0,1] beschrnken

    return {
        "fold_nets":      nets,
        "fold_results":   fold_results,
        "edge_count":     edge_count,
        "profit_count":   profit_cnt,
        "avg_net":        avg_net,
        "std_net":        std_net,
        "consistency":    consistency,
        "robust":         profit_cnt == len(folds),
    }


def holdout_simulate(indexed: dict, holdout_times: list, strategy: str,
                     use_maker: bool, params: dict) -> dict:
    """Single OUT-OF-SAMPLE run on the reserved holdout slice  data the search
    never saw during tuning. Same engine as one K-fold period."""
    if not holdout_times:
        return {"net": 0.0, "trades": 0, "edge": False, "skipped": True}
    period_data = filter_to_period(indexed, set(holdout_times))
    return simulate_fast(period_data, holdout_times, strategy, use_maker, params)


#  Sensitivitts-Analyse 

def sensitivity_check(indexed: dict, all_times: list, base_params: dict,
                      strategy: str, use_maker: bool,
                      search_space: dict) -> dict:
    """
    Variiert jeden Parameter um 1 Schritt im Suchraum.
    Berechnet wie stark sich Netto-PnL dabei ndert.

    Niedrige Standardabweichung = robust
    Hohe Standardabweichung     = fragil (overfit)
    """
    base_s   = simulate_fast(indexed, all_times, strategy, use_maker, base_params)
    base_net = base_s.get("net", 0)

    perturbations = []  # (param_name, delta, new_net, change_pct)

    for param, values in search_space.items():
        if param not in base_params:
            continue
        base_val = base_params[param]
        try:
            idx = values.index(base_val)
        except ValueError:
            # Nchster Wert finden
            idx = min(range(len(values)), key=lambda i: abs(values[i] - base_val))

        # 1 Schritt prfen
        for delta in (-1, +1):
            new_idx = idx + delta
            if not (0 <= new_idx < len(values)):
                continue
            new_params      = dict(base_params)
            new_params[param] = values[new_idx]
            s = simulate_fast(indexed, all_times, strategy, use_maker, new_params)
            new_net = s.get("net", 0)
            change_pct = ((new_net - base_net) / abs(base_net) * 100) if base_net != 0 else 0
            perturbations.append({
                "param":     param,
                "from":      base_val,
                "to":        values[new_idx],
                "delta":     delta,
                "new_net":   new_net,
                "change_pct": change_pct,
            })

    if not perturbations:
        return {"perturbations": [], "max_change": 0, "avg_change": 0,
                "robust": False, "rating": ""}

    changes        = [abs(p["change_pct"]) for p in perturbations]
    max_change     = max(changes)
    avg_change     = statistics.mean(changes)

    # Bewertung:
    #  < 15% durchschnittliche nderung  ROBUST
    #  < 30%  DURCHSCHNITTLICH
    #  30%  FRAGIL (overfit)
    if avg_change < 15:
        rating = " ROBUST"
    elif avg_change < 30:
        rating = " DURCHSCHNITTLICH"
    else:
        rating = " FRAGIL"

    return {
        "base_net":      base_net,
        "perturbations": perturbations,
        "max_change":    max_change,
        "avg_change":    avg_change,
        "rating":        rating,
        "robust":        avg_change < 15,
    }


#  Robustness Score 

def robustness_score(s_full: dict, kfold: dict,
                       walk_forward: dict = None,
                       regime_split: dict = None,
                       outlier_test: dict = None,
                       monte_carlo: dict = None) -> float:
    """Composite score that punishes profit-but-fragile strategies.

    Base term: avg_net  consistency  (1+sharpe)  (1 - dd/100)
    Plus modifiers:
      Walk-forward survival: 1.2 if all forward slices profitable
      Regime survival: 1.3 if profits in every detected regime
      Outlier independence: 0.5 if strategy depends on top trades
      Monte-Carlo robustness: 1.1 if >90% positive trials, 0.7 if <70%

    The point: a strategy that scores 50% lower but survives ALL these tests
    is better than the apparent winner that crumbles on shifted regimes.
    """
    if not kfold["robust"]:
        # Configs die nicht in allen Folds profitabel sind: starkes Penalty
        return s_full.get("net", -9999) * 0.1

    avg_net  = kfold["avg_net"]
    consist  = kfold["consistency"]
    sharpe   = max(0, s_full.get("sharpe", 0))
    dd       = max(0.1, s_full.get("max_dd", 99))

    score = avg_net * consist * (1 + sharpe) * (1 - dd / 100)

    # Walk-forward modifier
    if walk_forward:
        if walk_forward.get("all_profitable"):
            score *= 1.2
        elif walk_forward.get("profitable_count", 0) <= 1:
            score *= 0.5  # only 1 of N forward slices profitable = serious red flag

    # Regime survival modifier
    if regime_split:
        if regime_split.get("survives_all"):
            score *= 1.3
        elif regime_split.get("profitable_count", 0) == 0:
            score *= 0.3  # profitable in zero regimes = lucky aggregate only

    # Outlier dependency penalty
    if outlier_test and outlier_test.get("outlier_fragile") is True:
        score *= 0.5

    # Monte-Carlo modifier
    if monte_carlo:
        share = monte_carlo.get("positive_share")
        if share is not None:
            if share >= 0.90:
                score *= 1.1
            elif share < 0.70:
                score *= 0.7

    return score


#  Hilfsfunktionen 

def _label(p, strategy):
    pump_label = "move" if strategy == "FUTURES" else "pump"
    s = (f"{pump_label}={p['min_pump']:.0f}%  TP={p['activation_profit']:.1f}%  "
         f"trail={p['trailing_distance']:.1f}%  partial={p['partial_pct']:.0%}  "
         f"rsi{p['rsi_max']:.0f}")
    if strategy in ("TREND", "FUTURES"):
        s += f"  stop={p.get('stop_loss', -2):.1f}%"
    return s


def _cmd(p, strategy, days, use_maker):
    c  = f"python backtester.py {strategy} {days}"
    c += f" --pump {p['min_pump']:.0f}"
    c += f" --activation {p['activation_profit']:.1f}"
    c += f" --trailing {p['trailing_distance']:.1f}"
    c += f" --partial {p['partial_pct']:.2f}"
    c += f" --rsimax {p['rsi_max']:.0f}"
    if strategy in ("TREND", "FUTURES"):
        c += f" --stop {abs(p.get('stop_loss', 2)):.1f}"
    # Carry the leverage through so the validation command reproduces the run
    # (backtester defaults to 3.0 otherwise).
    _lev = p.get("leverage")
    if _lev is not None:
        c += f" --leverage {float(_lev):g}"
    if use_maker:
        c += " --maker"
    return c


#  CSV-Export 

def export_csv(results, strategy, days, k_folds):
    ts          = datetime.now().strftime("%Y%m%d_%H%M")
    # optimizer_results/ lives at PROJECT ROOT (read by the launcher UI), not
    # inside tools/.
    try:
        from core.paths import OPT_RESULTS
        results_dir = str(OPT_RESULTS)
    except Exception:
        results_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "optimizer_results"
        )
    os.makedirs(results_dir, exist_ok=True)

    # Alte Runs aufrumen: behalte nur die letzten 20 CSVs pro Strategy
    try:
        prefix = f"optimizer_{strategy.lower()}_"
        existing = sorted(
            [f for f in os.listdir(results_dir) if f.startswith(prefix) and f.endswith(".csv")],
            reverse=True
        )
        for old in existing[19:]:  # behalte 19 (+ neue = 20)
            try:
                os.remove(os.path.join(results_dir, old))
            except Exception:
                pass
    except Exception:
        pass

    filename = os.path.join(results_dir,
                              f"optimizer_{strategy.lower()}_{days}d_{ts}.csv")
    fields   = [
        "rank", "robust", "deployment_validated", "holdout_net",
        "avg_net", "std_net", "consistency",
        "score", "fold_profitable_count",
        "full_net", "full_roi", "win_rate", "sharpe", "max_dd", "cost_pct", "trades",
        "min_pump", "activation_profit", "trailing_distance",
        "stop_loss", "partial_pct", "rsi_max",
        "fold_nets", "cli_command",
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for i, r in enumerate(results):
            s  = r["stats"]
            p  = r["params"]
            kf = r["kfold"]
            w.writerow({
                "rank":                  i + 1,
                "robust":                "Ja" if kf["robust"] else "Nein",
                "deployment_validated":  "Ja" if r.get("deployment_validated") else "Nein",
                "holdout_net":           (round((r.get("holdout") or {}).get("net", 0), 2)
                                          if r.get("holdout") and not (r.get("holdout") or {}).get("skipped")
                                          else ""),
                "avg_net":               round(kf["avg_net"], 2),
                "std_net":               round(kf["std_net"], 2),
                "consistency":           round(kf["consistency"], 3),
                "score":                 round(r["score"], 4),
                "fold_profitable_count": f"{kf['profit_count']}/{k_folds}",
                "full_net":              round(s.get("net", 0), 2),
                "full_roi":              round(s.get("roi", 0), 2),
                "win_rate":              round(s.get("win_rate", 0), 3),
                "sharpe":                round(s.get("sharpe", 0), 3),
                "max_dd":                round(s.get("max_dd", 0), 1),
                "cost_pct":              round(s.get("cost_pct", 0), 1),
                "trades":                s.get("trades", 0),
                "min_pump":              p.get("min_pump"),
                "activation_profit":     p.get("activation_profit"),
                "trailing_distance":     p.get("trailing_distance"),
                "stop_loss":             p.get("stop_loss", ""),
                "partial_pct":           p.get("partial_pct"),
                "rsi_max":               p.get("rsi_max"),
                "fold_nets":             ";".join(f"{n:.2f}" for n in kf["fold_nets"]),
                "cli_command":           r.get("cmd", ""),
            })
    rel_filename = os.path.relpath(filename)
    print(f"  Ergebnisse gespeichert: {rel_filename}")
    return filename


#  Fortschrittsbalken 

class Progress:
    """Progress tracker for the optimizer. Emits TWO kinds of output:

      Interactive (stderr, with \\r): nice live progress bar in a terminal
      Launcher-parseable (stdout): structured lines the UI can parse
        Format:  <<<PROGRESS>>>{"label":"Sim","done":42,"total":300,"best":1.23,"eta":120}<<<END>>>

    The launcher reads these markers and updates an in-UI progress bar without
    needing carriage-return parsing (which doesn't work line-by-line).
    """

    def __init__(self, total, label="Sim"):
        self.total = total
        self.done  = 0
        self.start = time.time()
        self.best  = None
        self.label = label
        # Emit ticks at every 1% OR every 5 seconds  whichever comes first
        self._last_emit = 0.0
        self._last_pct  = -1

    def update(self, net):
        self.done += 1
        if self.best is None or net > self.best:
            self.best = net
        elapsed = time.time() - self.start
        eta     = elapsed / self.done * (self.total - self.done) if self.done else 0
        pct     = self.done / self.total
        bar  = "" * int(pct * 26) + "" * (26 - int(pct * 26))
        best_s  = f"+{self.best:.2f}" if self.best else ""

        # Terminal-friendly live bar (stderr  bypasses the launcher's line reader)
        sys.stderr.write(
            f"\r  {self.label} [{bar}] {self.done}/{self.total} ({pct:.0%}) "
            f"ETA {int(eta)}s Best: {best_s}    "
        )
        sys.stderr.flush()

        # Launcher-parseable markers  emit on each 1% step OR every 5s
        now      = time.time()
        pct_int  = int(pct * 100)
        emit_now = (
            pct_int != self._last_pct or
            (now - self._last_emit) >= 5.0 or
            self.done == self.total
        )
        if emit_now:
            payload = _json.dumps({
                "label": self.label,
                "done":  self.done,
                "total": self.total,
                "pct":   round(pct, 4),
                "best":  round(self.best, 4) if self.best is not None else None,
                "eta_s": int(eta),
                "elapsed_s": int(elapsed),
            })
            print(f"<<<PROGRESS>>>{payload}<<<END>>>", flush=True)
            self._last_emit = now
            self._last_pct  = pct_int


#  Haupt-Optimizer 

def _resolve_leverage(strategy: str, override: float = None) -> float:
    """Determine the leverage the backtest should use.

    Priority: explicit CLI/arg override > the strategy's LIVE LEVERAGE in
    bot_config.json > the backtester's STRATEGY_DEFAULTS fallback.

    Reading the live config keeps the backtest honest: the modelled leverage
    matches what the bot actually trades, so PnL, stop-in-margin and
    liquidation/tail risk reflect the real position.
    """
    if override is not None:
        try:
            return max(1.0, float(override))
        except (ValueError, TypeError):
            pass
    # bot_config.json lives in the project root (one level above tools/)
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "bot_config.json"), encoding="utf-8") as fh:
            cfg = _json.load(fh)
        lev = cfg.get(strategy, {}).get("LEVERAGE")
        if lev is not None:
            return max(1.0, float(lev))
    except Exception:
        pass
    try:
        from tools.backtester import STRATEGY_DEFAULTS
        return max(1.0, float(STRATEGY_DEFAULTS.get(strategy, {}).get("leverage", 1.0)))
    except Exception:
        return 1.0


def _load_live_strategy_config(strategy: str) -> dict:
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "bot_config.json"), encoding="utf-8-sig") as fh:
            cfg = _json.load(fh)
        section = cfg.get(strategy, {})
        return section if isinstance(section, dict) else {}
    except Exception:
        return {}


def _resolve_live_risk_params(strategy: str) -> dict:
    section = _load_live_strategy_config(strategy)
    mapping = {
        "POSITION_SIZE": "position_size",
        "POSITION_SIZE_MAX": "position_size_max",
        "MAX_OPEN_TRADES": "max_open_trades",
        "MAX_NEW_TRADES_PER_TICK": "top_n_per_scan",
    }
    out = {}
    for src, dst in mapping.items():
        if src not in section:
            continue
        try:
            val = float(section[src])
            out[dst] = int(val) if dst in {"max_open_trades", "top_n_per_scan"} else val
        except (TypeError, ValueError):
            continue
    return out


def run_optimizer(strategy: str, days: int = DEFAULT_DAYS,
                  use_maker: bool = False, top_n: int = 10,
                  k_folds: int = 4, quick: bool = False,
                  do_sensitivity: bool = True, leverage: float = None,
                  holdout_frac: float = 0.2,
                  own_momentum: bool = False, om_window: int = 8,
                  regime: bool = False, funding_8h: float = 0.0):

    rt    = calc_round_trip(use_maker, strategy)
    space = QUICK_SPACE[strategy] if quick else FULL_SPACE[strategy]
    lev   = _resolve_leverage(strategy, leverage)
    live_risk_params = _resolve_live_risk_params(strategy)

    log_separator("", 78, color="\033[96m")
    print(f"  STRATEGIE-OPTIMIZER v3  {strategy}")
    print(f"  {days} Tage | RT {rt*100:.2f}% | Hebel: x{lev:g} | "
          f"K-Fold: {k_folds} Perioden | "
          f"Sensitivitt: {'Ja' if do_sensitivity else 'Nein'} | "
          f"Modus: {'Quick' if quick else 'Voll'}")
    if strategy == "FUTURES":
        _src = "CLI" if leverage is not None else "bot_config.json"
        print(f"  Hebel x{lev:g} aus {_src}  Stop/TP-% sind PREIS-Bewegungen, "
               f"Margin-Wirkung = %  {lev:g}")
    if live_risk_params:
        print("  Live-Risk-Sizing im Backtest: " + ", ".join(
            f"{k}={v:g}" for k, v in sorted(live_risk_params.items())))
    log_separator("", 78, color="\033[96m")

    keys   = list(space.keys())
    combos = list(itertools.product(*[space[k] for k in keys]))
    p_list = [dict(zip(keys, c)) for c in combos]
    # Inject the resolved leverage into EVERY config so simulate_fast uses it
    # (backtester reads p["leverage"], else falls back to its default of 3.0).
    for _p in p_list:
        _p["leverage"] = lev
        for _k, _v in live_risk_params.items():
            _p.setdefault(_k, _v)
        if own_momentum:
            _p["own_momentum_filter"] = True
            _p["own_momentum_window"] = om_window
        if regime:
            _p["regime_filter"] = True
        if funding_8h:
            _p["funding_rate_8h"] = funding_8h
    if funding_8h:
        print(f"  Funding-Sensitivitt AKTIV  {funding_8h*100:.4f}%/8h auf das "
              f"Notional pro Hold-Dauer (Kosten, beide Seiten)")
    elif strategy == "FUTURES":
        print("  FUNDING NICHT MODELLIERT (0%/8h)  echtes Leveraged-FUTURES "
              "zahlt alle 8h Funding; gemeldete Edge ist OPTIMISTISCH. Mit "
              "--funding R (z.B. 0.0001) realistisch rechnen, sonst kann eine "
              "net-negative Config als 'deployment_validated' durchrutschen.")
    if own_momentum:
        print(f"  Own-Momentum-Overlay AKTIV (Fenster {om_window})  blockt "
              f"Entries solange die letzten {om_window} Closes net-negativ sind")
    if regime:
        print("  Regime-Gate AKTIV  LONG nur wenn BTC>EMA50, SHORT nur wenn BTC<EMA50")

    # Display labels: FUTURES uses "min_move" in UI, internally still min_pump
    _display_labels = {
        "min_pump": "min_move" if strategy == "FUTURES" else "min_pump",
    }
    print(f"\n  Suchraum:")
    for k, v in space.items():
        label = _display_labels.get(k, k)
        print(f"    {label:<22} {len(v)} Werte")
    print(f"\n  Kombinationen:    {len(p_list):,}")
    print(f"  Simulationen total: {len(p_list) * k_folds:,} ({k_folds} K-Fold)\n")

    # Verbindung  get_spot_exchange_connection() is a legacy-named alias that
    # actually returns whatever EXCHANGE is configured (.env), so the backtest
    # uses the SAME venue the bot trades on.
    print(" Verbinde mit Exchange...")
    ex = get_spot_exchange_connection()
    ex.timeout = 30000
    for attempt in range(1, 4):
        try:
            ex.load_markets()
            print(f"  Verbunden mit {getattr(ex, 'name', None) or 'Exchange'}\n")
            break
        except Exception as e:
            if attempt == 3:
                print(f"\n Verbindung fehlgeschlagen: {e}")
                sys.exit(1)
            print(f"  Timeout  warte 5s ({attempt}/3)...")
            time.sleep(5)

    # Daten laden
    print(" Lade Top-Volumen-Coins...")
    coins = get_top_volume_coins(ex, n=30, days=days)
    print(f"   {len(coins)} Coins")
    print("  SURVIVORSHIP BIAS (reduziert): Universum = HEUTIGE Top-Volumen-Coins.")
    print(f"     Listing-Age-Filter entfernt Coins die VOR {days}d noch nicht handelten,")
    print("  aber DELISTETE Coins fehlen weiterhin  NICHT vollstndig unverzerrt.\n")

    print(f" Lade {days}-Tage-Historie...")
    t_load  = time.time()
    history = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_history, ex, c, days): c for c in coins}
        for f in as_completed(futures):
            coin = futures[f]
            try:
                df = f.result()
                if df is not None and not df.empty:
                    history[coin] = df
            except Exception as e:
                print(f"   [WARN] {coin}: {type(e).__name__}: {e}")
    history = filter_universe_by_history(history, days)
    print(f"   {len(history)} Coins in {time.time()-t_load:.1f}s\n")

    # Vorindexierung
    print(" Vorindexierung...")
    t_idx = time.time()
    indexed, all_times = precompute_index(history)
    print(f"   {len(all_times):,} Zeitstempel in {time.time()-t_idx:.1f}s\n")

    #  Out-of-sample HOLDOUT split 
    # Reserve the most RECENT holdout_frac of the timeline. It is excluded from
    # EVERY tuning step (folds, walk-forward, regime-split, full-period score,
    # monte-carlo, sensitivity) and only the finally-selected configs are scored
    # on it  an honest "deployment test" on data the search never saw. Without
    # it, K-fold/walk-forward can still collectively overfit the sampled period.
    holdout_times: list = []
    tune_times          = all_times
    if 0.0 < holdout_frac < 0.9 and len(all_times) >= 50:
        cut          = int(len(all_times) * (1.0 - holdout_frac))
        tune_times   = all_times[:cut]
        holdout_times = all_times[cut:]
        print(f"  Holdout (out-of-sample, NICHT im Tuning): "
              f"{len(holdout_times):,} Stempel "
              f"({holdout_times[0].strftime('%Y-%m-%d')}  "
              f"{holdout_times[-1].strftime('%Y-%m-%d')})\n")
    else:
        print("  Holdout uebersprungen (zu wenig Daten oder --holdout 0)\n")

    # K-Fold Splits  over the TUNING window only (holdout stays unseen)
    folds = split_into_folds(tune_times, k=k_folds)
    print(f"  K-Fold Split ({k_folds} Perioden):")
    for i, fold in enumerate(folds, 1):
        if fold:
            print(f"    Fold {i}: {len(fold):,} Stempel "
                  f"({fold[0].strftime('%Y-%m-%d')}  {fold[-1].strftime('%Y-%m-%d')})")
    print()

    # Open a per-run JSONL where every config result is appended incrementally
    # so a crash/abort doesn't lose the full run.
    _set_incremental_path(strategy, days)
    if _INCREMENTAL_PATH:
        print(f"   Incremental log: {_INCREMENTAL_PATH}")

    # Hauptsimulation
    print(f"  Simuliere {len(p_list):,} Configs  {k_folds} Folds = "
          f"{len(p_list) * k_folds:,} Sims...\n")
    t_sim    = time.time()
    progress = Progress(len(p_list), label="Optimize")
    results  = []

    def _run(params):
        # Tuning window only  the holdout slice never enters selection.
        s_full = simulate_fast(indexed, tune_times, strategy, use_maker, params)
        kf     = kfold_simulate(indexed, folds, strategy, use_maker, params)
        sc     = robustness_score(s_full, kf)
        progress.update(s_full.get("net", -9999))
        rec = {"params": params, "stats": s_full, "kfold": kf, "score": sc}
        # persist this result immediately
        _persist_result(rec)
        return rec

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(_run, p) for p in p_list]
        for f in as_completed(futs):
            results.append(f.result())

    elapsed = time.time() - t_sim
    print(f"\n\n  {len(results):,} Configs in {elapsed:.0f}s "
          f"({elapsed/len(results)*1000:.0f}ms/Config inkl. K-Fold)\n")

    # Sortierung: zuerst robuste Configs, dann nach Score
    robust_cfgs   = [r for r in results if r["kfold"]["robust"]]
    fragile_cfgs  = [r for r in results if not r["kfold"]["robust"]]
    robust_cfgs.sort(  key=lambda x: x["score"], reverse=True)
    fragile_cfgs.sort( key=lambda x: x["stats"].get("net", -9999), reverse=True)
    sorted_results = robust_cfgs + fragile_cfgs

    #  Deep robustness validation on the top candidates 
    # walk_forward / regime_split / outlier / monte_carlo feed robustness_score's
    # modifiers. Running them on the FULL grid would 2-3 the runtime; instead we
    # validate only the top-K already-robust k-fold winners (~8 extra sims each 
    # negligible beside the grid). The enriched score feeds back into the ranking
    # so a config that survives out-of-sample outranks a k-fold winner that
    # crumbles.
    deep_k = min(len(robust_cfgs), max(top_n, 10))
    if deep_k > 0:
        print(f"\n Deep-validating top {deep_k} robust config(s) "
              f"(walk-forward  regime  outlier  monte-carlo)")
        for r in robust_cfgs[:deep_k]:
            p = r["params"]
            def _safe(fn):
                try:
                    return fn()
                except Exception:
                    return None
            wf = _safe(lambda: walk_forward_simulate(indexed, tune_times, strategy, use_maker, p))
            rs = _safe(lambda: regime_split_simulate(indexed, tune_times, strategy, use_maker, p))
            ot = _safe(lambda: outlier_dependency_test(indexed, tune_times, strategy, use_maker, p))
            mc = _safe(lambda: monte_carlo_perturbation(r["stats"]))
            r["walk_forward"] = wf
            r["regime_split"] = rs
            r["outlier_test"] = ot
            r["monte_carlo"]  = mc
            # OUT-OF-SAMPLE deployment test on the untouched holdout. A config is
            # only "deployment-validated" if it is k-fold robust AND still nets
            # > 0 on data the entire search never saw.
            hd = (_safe(lambda: holdout_simulate(indexed, holdout_times,
                                                 strategy, use_maker, p))
                  if holdout_times else None)
            r["holdout"] = hd
            r["deployment_validated"] = bool(
                r["kfold"]["robust"] and hd and hd.get("net", -9999) > 0)
            # Recompute score WITH the deep modifiers.
            r["score"] = robustness_score(
                r["stats"], r["kfold"],
                walk_forward=wf, regime_split=rs,
                outlier_test=ot, monte_carlo=mc,
            )
        # Re-rank: holdout survivors first, then by enriched score. A config that
        # crumbles out-of-sample drops below one that survives, even if its
        # in-sample score was higher.
        robust_cfgs.sort(key=lambda x: (x.get("deployment_validated", False),
                                        x["score"]), reverse=True)
        sorted_results = robust_cfgs + fragile_cfgs

    # Score the TOP configs on the holdout regardless of k-fold robustness
    # (cheap, 1 sim each) so the report always shows out-of-sample behaviour of
    # the best available config  even when nothing passed k-fold.
    if holdout_times:
        for r in sorted_results[:max(top_n, 10)]:
            if "holdout" not in r:
                try:
                    hd = holdout_simulate(indexed, holdout_times,
                                          strategy, use_maker, r["params"])
                except Exception:
                    hd = None
                r["holdout"] = hd
                r["deployment_validated"] = bool(
                    r["kfold"]["robust"] and hd and hd.get("net", -9999) > 0)

    for r in sorted_results:
        r["cmd"] = _cmd(r["params"], strategy, days, use_maker)

    # Top-Tabelle
    log_separator("", 78, color="\033[96m")
    print(f"  TOP {top_n}  {strategy}  "
          f"|  Robust ({k_folds}/{k_folds} Folds): {len(robust_cfgs)}/{len(p_list)}")
    log_separator("", 78, color="\033[96m")

    hdr = (f"  {'#':>3}  {'Pump':>5}  {'TP':>6}  {'Trl':>5}  "
           f"{'Pt':>4}  {'RSI':>4}  ")
    if strategy in ("TREND", "FUTURES"):
        hdr += f"{'Stop':>5}  "
    hdr += f"{'AvgNet':>8}  {'StdNet':>7}  {'Cons':>5}  {'WR':>5}  {'DD':>5}  {'Folds':>5}"
    print(hdr)
    log_separator("", 78)

    for i, r in enumerate(sorted_results[:top_n]):
        s  = r["stats"]
        p  = r["params"]
        kf = r["kfold"]
        if not s.get("trades"):
            continue
        robust_mark = "" if kf["robust"] else "  "
        row = (f"  {i+1:>3}  "
               f"{p['min_pump']:>4.0f}%  "
               f"{p['activation_profit']:>5.1f}%  "
               f"{p['trailing_distance']:>4.1f}%  "
               f"{p['partial_pct']:>3.0%}  "
               f"{p['rsi_max']:>3.0f}  ")
        if strategy in ("TREND", "FUTURES"):
            row += f"{p.get('stop_loss',-2):>4.1f}%  "
        row += (f"{kf['avg_net']:>+7.2f}  "
                f"{kf['std_net']:>6.2f}  "
                f"{kf['consistency']:>5.0%}  "
                f"{s['win_rate']:>4.0%}  "
                f"{s['max_dd']:>4.1f}%  "
                f"{kf['profit_count']}/{k_folds}  "
                f"{robust_mark}")
        print(row)

    log_separator("", 78, color="\033[96m")

    # Near-miss report: configs profitable in exactly k_folds-1 folds
    near_miss_threshold = k_folds - 1
    near_misses = [
        r for r in results
        if not r["kfold"]["robust"]
        and r["kfold"]["profit_count"] == near_miss_threshold
    ]
    near_misses.sort(key=lambda x: x["kfold"]["avg_net"], reverse=True)
    if near_misses:
        print(f"\n  NEAR-MISS ({near_miss_threshold}/{k_folds} folds profitable, "
              f"{len(near_misses)} config(s)):")
        for nm in near_misses[:5]:
            nkf = nm["kfold"]
            np_ = nm["params"]
            fold_str = "  ".join(f"{n:+.2f}" for n in nkf["fold_nets"])
            print(f"    {_label(np_, strategy)}")
            print(f"      folds: [{fold_str}]  avg {nkf['avg_net']:+.2f}")

    # Beste Empfehlung
    if not sorted_results:
        print("\n  Keine Configs simuliert.\n")
        return []

    best = sorted_results[0]
    bs   = best["stats"]
    bp   = best["params"]
    bkf  = best["kfold"]

    #  Lpez de Prado trust diagnostics: DSR + PBO 
    # The best Sharpe is the MAX over all tested configs  selection-inflated.
    # DSR deflates it by the expected max under the null; PBO (via CSCV over the
    # k folds) estimates the probability the apparent edge is overfit. Both are
    # diagnostics layered ON TOP of the robust gate  they do NOT change it.
    sr_list = [r["stats"].get("sharpe", 0.0) for r in results
               if r["stats"].get("trades")]
    sr_best = bs.get("sharpe", 0.0)
    best_nets = list(bs.get("net_trades") or [])
    n_obs = len(best_nets)
    skew_b, kurt_b = _sample_skew_kurt(best_nets)
    dsr = deflated_sharpe_ratio(sr_list, sr_best, n_obs,
                                skew=skew_b, kurt=kurt_b)
    # PBO matrix: per-config net across the k folds (already simulated).
    perf_matrix = [r["kfold"]["fold_nets"] for r in results
                   if len(r["kfold"].get("fold_nets") or []) == k_folds]
    pbo = probability_of_backtest_overfitting(perf_matrix)
    dsr_val = dsr.get("dsr")
    pbo_val = pbo.get("pbo")
    deployment_trustworthy = bool(
        bkf["robust"]
        and dsr_val is not None and dsr_val >= 0.95
        and pbo_val is not None and pbo_val <= 0.5)
    best["dsr"] = dsr
    best["pbo"] = pbo
    best["deployment_trustworthy"] = deployment_trustworthy

    print(f"\n  BESTE KONFIGURATION:")
    print(f"     {_label(bp, strategy)}")
    print(f"  Avg Netto:  {bkf['avg_net']:+.2f} USDT (ber {k_folds} Folds)")
    print(f"     Konsistenz:   {bkf['consistency']:.0%}  "
          f"(StdAbw: {bkf['std_net']:.2f} USDT)")
    print(f"     Folds:        {bkf['profit_count']}/{k_folds} profitabel | "
          f"Robust: {'Ja ' if bkf['robust'] else 'Nein '}")
    print(f"     Vollperiode:  Netto {bs['net']:+.2f} USDT | "
          f"WR {bs['win_rate']:.1%} | Sharpe {bs['sharpe']:.3f}")

    # Lpez de Prado trust diagnostics
    if dsr_val is not None:
        print(f"     DSR:          {dsr_val:.3f}  "
              f"({' 0.95' if dsr_val >= 0.95 else ' <0.95  Edge womglich Selektionsrauschen'})"
              f"  [{dsr.get('n_trials', 0)} Trials, {dsr.get('n_obs', 0)} Trades]")
    else:
        print(f"  DSR:  ({dsr.get('reason', 'n/a')})")
    if pbo_val is not None:
        print(f"     PBO (CSCV):   {pbo_val:.3f}  "
              f"({' 0.5' if pbo_val <= 0.5 else ' >0.5  overfit'})"
              f"  [{pbo.get('n_partitions', 0)} Partitionen]")
    else:
        print(f"  PBO (CSCV):  ({pbo.get('reason', 'n/a')})")
    print(f"     Trustworthy:  "
          f"{'Ja ' if deployment_trustworthy else 'Nein '} "
          f"(robust  DSR0.95  PBO0.5)")

    # Deep-validation summary
    wf = best.get("walk_forward") or {}
    rs = best.get("regime_split") or {}
    mc = best.get("monte_carlo")  or {}
    ot = best.get("outlier_test") or {}
    if wf or rs or mc or ot:
        if wf:
            n_sl = len(wf.get("slice_nets", []))
            print(f"     Walk-Forward: "
                  + (" alle Slices profitabel"
                     if wf.get("all_profitable")
                     else f" {wf.get('profitable_count','?')}/{n_sl} Slices +"))
        if rs:
            print(f"     Regime-Test:  "
                  + (" bersteht alle Regimes"
                     if rs.get("survives_all")
                     else f" {rs.get('profitable_count','?')}/"
                          f"{rs.get('tested_count','?')} Regimes +"))
        if mc.get("positive_share") is not None:
            print(f"     Monte-Carlo:  {mc['positive_share']*100:.0f}% der "
                  f"Lufe positiv"
                  + ("  " if mc.get("robust")
                     else "  fragil" if mc.get("concerning") else ""))
        if ot.get("outlier_fragile") is not None:
            print(f"     Outlier-Dep.: "
                  + (" FRAGIL  Edge hngt an Top-Trades"
                     if ot["outlier_fragile"]
                     else " breit verteilte Edge"))

    # Out-of-sample holdout  the honest deployment test.
    hd = best.get("holdout")
    if holdout_times:
        if hd and not hd.get("skipped"):
            _ok = hd.get("net", 0) > 0
            print(f"  Holdout (OOS): {'' if _ok else ''} Netto "
                  f"{hd.get('net', 0):+.2f} USDT | WR {hd.get('win_rate', 0):.1%} | "
                  f"{hd.get('trades', 0)} Trades  "
                  f"{'DEPLOYMENT-VALIDATED' if best.get('deployment_validated') else 'NICHT besttigt (out-of-sample fragil)'}")
        else:
            print("  Holdout (OOS):  (keine Trades / uebersprungen)")

    # Maschinenlesbarer Marker fr UI-Parser  bleibt im Output unauffllig.
    # Format: ein einziger JSON-Block in einer Zeile, mit Markern davor/danach.
    # SAFETY: the live bot's validate_config_or_die HARD-REJECTS any
    # INITIAL_STOP_LOSS that is not strictly negative (-100 < x < 0): a
    # 0/positive stop = instant stop-out at entry, so the bot refuses to start.
    # Never emit a stop the live bot would reject  covers any strategy whose
    # grid omits the key.
    _stop_out = float(bp.get("stop_loss", 0) or 0)
    if not (-100.0 < _stop_out < 0.0):
        _grid = sorted(g for g in space.get("stop_loss", []) if g < 0)
        _stop_out = _grid[len(_grid) // 2] if _grid else -2.0
    best_payload = {
        "strategy":         strategy,
        "min_pump":         float(bp.get("min_pump", 0)),
        "activation_profit": float(bp.get("activation_profit", 0)),
        "trailing_distance": float(bp.get("trailing_distance", 0)),
        "stop_loss":        _stop_out,
        "partial_pct":      float(bp.get("partial_pct", 0)),
        "rsi_max":          float(bp.get("rsi_max", 0)),
        "avg_net":          float(bkf["avg_net"]),
        "consistency":      float(bkf["consistency"]),
        "win_rate":         float(bs.get("win_rate", 0)),
        "sharpe":           float(bs.get("sharpe", 0)),
        "robust":           bool(bkf["robust"]),
        "holdout_net":      (float(hd.get("net", 0)) if hd and not hd.get("skipped") else None),
        "deployment_validated": bool(best.get("deployment_validated", False)),
        "dsr":              (float(dsr_val) if dsr_val is not None else None),
        "pbo":              (float(pbo_val) if pbo_val is not None else None),
        "deployment_trustworthy": bool(deployment_trustworthy),
    }
    print(f"\n<<<BEST_CONFIG>>>{_json.dumps(best_payload)}<<<END_BEST_CONFIG>>>\n")

    # Sensitivitts-Analyse fr Top 3
    if do_sensitivity and len(sorted_results) > 0:
        print(f"\n  SENSITIVITTS-ANALYSE (Top 3 Configs):\n")
        for rank in range(min(3, len(sorted_results))):
            cfg  = sorted_results[rank]
            sens = sensitivity_check(
                indexed, tune_times, cfg["params"],
                strategy, use_maker, space
            )
            print(f"  Rang {rank+1}: {_label(cfg['params'], strategy)}")
            print(f"    Bewertung:        {sens['rating']}")
            print(f"  Avg nderung:  {sens['avg_change']:.1f}%  "
                  f"(Max: {sens['max_change']:.1f}%)")
            print(f"    Basis-Netto:      {sens['base_net']:+.2f} USDT")
            cfg["sensitivity"] = sens
            print()

    print(f"\n  Validierung der Top-1:")
    print(f"     {best['cmd']}\n")

    # CSV
    export_csv(sorted_results, strategy, days, k_folds)

    # Footer
    print(f"\n  Simulationen: {len(p_list)*k_folds:,} | "
          f"Robuste Configs: {len(robust_cfgs)} | "
          f"Zeit: {time.time()-t_sim:.0f}s")
    print(f"  Analyse: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_separator("", 78, color="\033[96m")

    return sorted_results


#  CLI 

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] not in ("TREND", "SPOT", "FUTURES"):
        print("Usage: python optimizer.py [TREND|SPOT|FUTURES] [days]")
        print("       [--maker] [--top N] [--kfold K] [--leverage N]")
        print("       [--no-sensitivity] [--quick] [--holdout F]")
        print("       [--own-momentum] [--om-window N] [--regime] [--funding R]")
        print("  --funding R     : model funding R per 8h (e.g. 0.0003) as a "
              "cost sweep on the notional per hold-duration (default 0 = off)")
        print("  --holdout F     : reserve most-recent fraction F out-of-sample "
              "(default 0.2; 0 disables)")
        print("  --own-momentum  : A/B test the own-momentum overlay "
              "(block entries after N net-negative closes)")
        print("  --leverage N : override leverage (default: read from bot_config.json)")
        print()
        print("  Beispiele:")
        print("  python optimizer.py TREND  # voll, K=4, mit Sensitivitt")
        print("    python optimizer.py SPOT 60 --maker # mit Maker-Orders")
        print("    python optimizer.py FUTURES 30 --quick    # Futures-Suchraum")
        print("    python optimizer.py TREND --kfold 5    # 5 Perioden statt 4")
        print("    python optimizer.py TREND --quick      # ~1/8 Suchraum")
        print("    python optimizer.py TREND --no-sensitivity  # ohne Robustheits-Test")
        sys.exit(1)

    strategy       = args[0]
    if strategy == "TREND":
        # TREND trades the SMA-ensemble, not the momentum grid below  route to
        # the validator's robustness sweep so tuning reflects the live signal.
        from tools import trend_check
        _days = args[1] if len(args) > 1 and args[1].isdigit() else str(DEFAULT_DAYS)
        sys.argv = ["trend_check", _days, "--sweep"]
        trend_check.main()
        sys.exit(0)
    days           = int(args[1]) if len(args) > 1 and args[1].isdigit() else DEFAULT_DAYS
    use_maker      = "--maker"          in args
    quick          = "--quick"          in args
    do_sensitivity = "--no-sensitivity" not in args

    top_n   = 10
    k_folds = 4
    if "--top" in args:
        try: top_n = int(args[args.index("--top") + 1])
        except (ValueError, IndexError): pass
    if "--kfold" in args:
        try: k_folds = int(args[args.index("--kfold") + 1])
        except (ValueError, IndexError): pass

    leverage = None  # None  resolve from bot_config.json inside run_optimizer
    if "--leverage" in args:
        try: leverage = float(args[args.index("--leverage") + 1])
        except (ValueError, IndexError): pass

    holdout_frac = 0.2   # fraction of the most-recent data reserved out-of-sample
    if "--holdout" in args:
        try: holdout_frac = float(args[args.index("--holdout") + 1])
        except (ValueError, IndexError): pass

    own_momentum = "--own-momentum" in args
    om_window    = 8
    if "--om-window" in args:
        try: om_window = int(args[args.index("--om-window") + 1])
        except (ValueError, IndexError): pass

    regime = "--regime" in args

    funding_8h = 0.0
    if "--funding" in args:
        try: funding_8h = float(args[args.index("--funding") + 1])
        except (ValueError, IndexError): pass

    run_optimizer(
        strategy, days,
        use_maker=use_maker, top_n=top_n,
        k_folds=k_folds, quick=quick,
        do_sensitivity=do_sensitivity, leverage=leverage,
        holdout_frac=holdout_frac,
        own_momentum=own_momentum, om_window=om_window,
        regime=regime, funding_8h=funding_8h,
    )
