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
  python optimizer.py FUTURES 90 --maker
  python optimizer.py SPOT 60 --top 20
  python optimizer.py FUTURES --quick
  python optimizer.py FUTURES --kfold 5     # 5 Folds statt 4
  python optimizer.py SPOT --no-sensitivity  # ohne Sensitivitts-Analyse

DAUER:
  Standard (4 Folds + Sensitivity): ~30-90 Minuten TREND, ~10-25 Min. SPOT
  --quick: ~5-15 Minuten
"""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

import sys
import os
import time
import math
import itertools
import multiprocessing
import csv
import json as _json
import random as _random
import statistics
import threading
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone

_TOOL_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TOOL_PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _TOOL_PROJECT_ROOT)

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

try:
    from scipy.stats import norm as _scipy_norm
except Exception:
    _scipy_norm = None

from config.exchange_config import (
    get_public_futures_exchange_connection,
    get_spot_exchange_connection,
)
from tools.backtester import (
    fetch_history,
    get_top_futures_volume_coins,
    get_top_volume_coins,
    filter_universe_by_history,
    precompute_index,
    simulate_fast,
    calc_round_trip,
    DEFAULT_DAYS,
    PNL_ZERO_TOLERANCE_PCT,
    _compute_stats,
)
from tools.simulation_workspace import (
    ReproducibleRun,
    freeze_history_dataset,
    load_history_dataset,
    split_boundaries,
)
from core.logger import log_separator

#  Deterministic optimizer runs (fixed seed)
# monte_carlo_perturbation seeds its RNG with this so runs are reproducible.
# Override via OPTIMIZER_SEED env.
def _optimizer_seed_from_env(raw=None) -> int:
    value = os.getenv("OPTIMIZER_SEED", "42") if raw is None else raw
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return 42


_OPTIMIZER_SEED = _optimizer_seed_from_env()
OPTIMIZER_CONFIG_MAX_BYTES = 2 * 1024 * 1024
_OPTIMIZER_POSITION_LIMITS = {
    "SPOT": 500.0,
    "FUTURES": 500.0,
    "CROSS": 500.0,
    "TREND": 2500.0,
    "FUTREND": 2500.0,
}


#  Lpez de Prado anti-overfit constants
# Indicators (RSI14, MACD 12/26/9, EMA50, 24h change) are computed over the FULL
# per-symbol series in the backtester, so the indicator state at a fold's first
# bars is warmed by the PREVIOUS fold. PURGE_BARS drops each fold's leading
# timestamps so its signals are warmed from WITHIN the fold; EMBARGO_FRAC adds a
# gap between adjacent folds. PURGE_BARS = max indicator lookback in bars (EMA50).
PURGE_BARS = 50
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
    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    ]
    plow, phigh = 0.02425, 1.0 - 0.02425
    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        )
    if p > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(
            ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
        ) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    q = p - 0.5
    r = q * q
    return (
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])
        * q
        / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    )


def _invalid_dsr_result(
    reason: str,
    n_trials: int = 0,
    valid_trial_count: int = 0,
    invalid_trial_count: int = 0,
) -> dict:
    return {
        "dsr": None,
        "sr0": None,
        "n_trials": n_trials,
        "valid_trial_count": valid_trial_count,
        "invalid_trial_count": invalid_trial_count,
        "best_consistent": False,
        "evidence_valid": False,
        "reason": reason,
    }


def deflated_sharpe_ratio(
    sr_list: list, sr_best: float, n_obs: int, skew: float = 0.0, kurt: float = 3.0
) -> dict:
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
    if not isinstance(sr_list, (list, tuple)):
        return _invalid_dsr_result("invalid_trial_container")
    normalized = [_finite_optimizer_number(value) for value in sr_list]
    invalid_trial_count = sum(value is None for value in normalized)
    n = len(normalized)
    valid_trial_count = n - invalid_trial_count
    if invalid_trial_count:
        return _invalid_dsr_result(
            "invalid_trial_sharpe",
            n_trials=n,
            valid_trial_count=valid_trial_count,
            invalid_trial_count=invalid_trial_count,
        )
    if n < 2:
        return _invalid_dsr_result(
            "insufficient_stats",
            n_trials=n,
            valid_trial_count=valid_trial_count,
        )
    if isinstance(n_obs, bool) or not isinstance(n_obs, int) or n_obs < 2:
        return _invalid_dsr_result(
            "invalid_observation_count",
            n_trials=n,
            valid_trial_count=valid_trial_count,
        )
    sr_best_value = _finite_optimizer_number(sr_best)
    skew_value = _finite_optimizer_number(skew)
    kurt_value = _finite_optimizer_number(kurt)
    if sr_best_value is None or skew_value is None or kurt_value is None:
        return _invalid_dsr_result(
            "invalid_distribution_stats",
            n_trials=n,
            valid_trial_count=valid_trial_count,
        )
    if kurt_value < 1.0:
        return _invalid_dsr_result(
            "invalid_kurtosis",
            n_trials=n,
            valid_trial_count=valid_trial_count,
        )
    best_consistent = any(
        math.isclose(sr_best_value, trial, rel_tol=1e-9, abs_tol=1e-12)
        for trial in normalized
    )
    if not best_consistent:
        return _invalid_dsr_result(
            "selected_sharpe_missing_from_trials",
            n_trials=n,
            valid_trial_count=valid_trial_count,
        )
    try:
        var_sr = statistics.variance(normalized)
        if not math.isfinite(var_sr) or var_sr <= 0.0:
            reason = "zero_variance" if var_sr == 0.0 else "invalid_variance"
            return _invalid_dsr_result(
                reason,
                n_trials=n,
                valid_trial_count=valid_trial_count,
            )
        z1 = _norm_ppf(1.0 - 1.0 / n)
        z2 = _norm_ppf(1.0 - 1.0 / (n * math.e))
        sr0 = math.sqrt(var_sr) * (
            (1.0 - _EULER_GAMMA) * z1 + _EULER_GAMMA * z2
        )
        denom = (
            1.0
            - skew_value * sr_best_value
            + ((kurt_value - 1.0) / 4.0) * sr_best_value * sr_best_value
        )
        if not math.isfinite(sr0) or not math.isfinite(denom):
            raise ArithmeticError
        if denom <= 0.0:
            return _invalid_dsr_result(
                "nonpositive_denominator",
                n_trials=n,
                valid_trial_count=valid_trial_count,
            )
        z = (sr_best_value - sr0) * math.sqrt(n_obs - 1) / math.sqrt(denom)
        dsr = _norm_cdf(z)
    except (ArithmeticError, OverflowError, ValueError):
        return _invalid_dsr_result(
            "invalid_dsr_summary",
            n_trials=n,
            valid_trial_count=valid_trial_count,
        )
    if not math.isfinite(z) or not math.isfinite(dsr) or not 0.0 <= dsr <= 1.0:
        return _invalid_dsr_result(
            "invalid_dsr_summary",
            n_trials=n,
            valid_trial_count=valid_trial_count,
        )
    return {
        "dsr": dsr,
        "sr0": sr0,
        "n_trials": n,
        "valid_trial_count": valid_trial_count,
        "invalid_trial_count": 0,
        "n_obs": n_obs,
        "best_consistent": True,
        "evidence_valid": True,
    }


def _logit(x: float) -> float:
    eps = 1e-9
    x = min(1.0 - eps, max(eps, x))
    return math.log(x / (1.0 - x))


MAX_PBO_CSCV_FOLDS = 12


def probability_of_backtest_overfitting(perf_matrix: list) -> dict:
    """Probability of Backtest Overfitting (PBO) via CSCV.

    perf_matrix : list of per-config performance rows, each row a list of the
                  config's net across the k folds  matrix [config  fold].

    Partition the k folds into two equal halves in every C(k, k/2) way (one half
    = IS, the complement = OOS). For each partition pick the IS-best config, find
    its OOS rank, and compute the logit of its relative OOS rank. PBO = fraction
    of partitions whose IS-best config lands at or below the OOS median.
    """
    if not isinstance(perf_matrix, (list, tuple)):
        return {"pbo": None, "n_partitions": 0, "reason": "invalid_matrix"}
    if len(perf_matrix) < 2:
        return {"pbo": None, "n_partitions": 0, "reason": "too_few_configs"}
    if any(not isinstance(row, (list, tuple)) for row in perf_matrix):
        return {"pbo": None, "n_partitions": 0, "reason": "invalid_matrix"}
    n_cfg = len(perf_matrix)
    k = len(perf_matrix[0])
    if k < 2 or any(len(row) != k for row in perf_matrix):
        return {"pbo": None, "n_partitions": 0, "reason": "ragged_or_too_few_folds"}
    if k % 2:
        return {"pbo": None, "n_partitions": 0, "reason": "odd_fold_count"}
    # Exact CSCV is combinatorial: C(12, 6)=924, but C(16, 8)=12,870
    # full candidate rankings. Keep diagnostics bounded and fail closed.
    if k > MAX_PBO_CSCV_FOLDS:
        return {"pbo": None, "n_partitions": 0, "reason": "too_many_folds"}
    normalized_matrix = []
    for row in perf_matrix:
        normalized_row = []
        for value in row:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return {
                    "pbo": None,
                    "n_partitions": 0,
                    "reason": "invalid_performance",
                }
            number = float(value)
            if not math.isfinite(number):
                return {
                    "pbo": None,
                    "n_partitions": 0,
                    "reason": "nonfinite_performance",
                }
            normalized_row.append(number)
        normalized_matrix.append(normalized_row)
    half = k // 2
    if half < 1:
        return {"pbo": None, "n_partitions": 0, "reason": "too_few_folds"}
    all_folds = list(range(k))
    logits = []
    below_median = 0.0
    try:
        for is_idx in itertools.combinations(all_folds, half):
            is_set = set(is_idx)
            oos_idx = [j for j in all_folds if j not in is_set]
            is_perf = [
                statistics.mean(normalized_matrix[c][j] for j in is_idx)
                for c in range(n_cfg)
            ]
            oos_perf = [
                statistics.mean(normalized_matrix[c][j] for j in oos_idx)
                for c in range(n_cfg)
            ]
            if not all(math.isfinite(value) for value in is_perf + oos_perf):
                raise ArithmeticError
            best_is = max(is_perf)
            best_configs = [c for c in range(n_cfg) if is_perf[c] == best_is]

            # Worker completion order must not decide equal-score ranks. Assign
            # every OOS tie its average ascending rank, then weight all IS-best
            # ties equally within this partition.
            oos_ranks = [0.0] * n_cfg
            ordered = sorted(range(n_cfg), key=lambda c: oos_perf[c])
            start = 0
            while start < n_cfg:
                end = start + 1
                while (
                    end < n_cfg
                    and oos_perf[ordered[end]] == oos_perf[ordered[start]]
                ):
                    end += 1
                average_rank = (start + end - 1) / 2.0
                for position in range(start, end):
                    oos_ranks[ordered[position]] = average_rank
                start = end
            rel_ranks = [
                (oos_ranks[c] + 1.0) / (n_cfg + 1.0) for c in best_configs
            ]
            logits.append(statistics.mean(_logit(rank) for rank in rel_ranks))
            below_median += statistics.mean(rank <= 0.5 for rank in rel_ranks)
    except (ArithmeticError, OverflowError, ValueError):
        return {"pbo": None, "n_partitions": 0, "reason": "invalid_summary"}
    n_part = len(logits)
    if n_part == 0:
        return {"pbo": None, "n_partitions": 0, "reason": "no_partitions"}
    pbo = below_median / n_part
    median_logit = statistics.median(logits)
    expected_partitions = math.comb(k, half)
    if (
        n_part != expected_partitions
        or not math.isfinite(pbo)
        or not 0.0 <= pbo <= 1.0
        or not math.isfinite(median_logit)
    ):
        return {"pbo": None, "n_partitions": 0, "reason": "invalid_summary"}
    return {
        "pbo": pbo,
        "n_partitions": n_part,
        "expected_partitions": expected_partitions,
        "median_logit": median_logit,
        "config_count": n_cfg,
        "fold_count": k,
        "evidence_valid": True,
    }


def _sample_skew_kurt(xs: list) -> tuple:
    """Sample skewness and NON-excess kurtosis (Normal  3.0) of a return list.
    Returns (0.0, 3.0) when stats are undefined so DSR degrades to the Normal
    case rather than failing."""
    if not isinstance(xs, (list, tuple)):
        return float("nan"), float("nan")
    normalized = [_finite_optimizer_number(value) for value in xs]
    if any(value is None for value in normalized):
        return float("nan"), float("nan")
    n = len(normalized)
    if n < 3:
        return 0.0, 3.0
    try:
        mean = statistics.mean(normalized)
        m2 = math.fsum((x - mean) ** 2 for x in normalized) / n
    except (ArithmeticError, OverflowError, ValueError):
        return float("nan"), float("nan")
    if not math.isfinite(mean) or not math.isfinite(m2):
        return float("nan"), float("nan")
    if m2 <= 0.0:
        return 0.0, 3.0
    try:
        m3 = math.fsum((x - mean) ** 3 for x in normalized) / n
        m4 = math.fsum((x - mean) ** 4 for x in normalized) / n
        skew = m3 / (m2**1.5)
        kurt = m4 / (m2**2)
    except (ArithmeticError, OverflowError, ValueError):
        return float("nan"), float("nan")
    if not math.isfinite(skew) or not math.isfinite(kurt):
        return float("nan"), float("nan")
    return skew, kurt


#  Incremental result persistence
# The optimizer is a long job (30-90 min). Each completed result is streamed to
# a JSONL file as it's computed so a Ctrl+C / OOM never loses partial progress.
_INCREMENTAL_PATH = None
_INCREMENTAL_WRITE_LOCK = threading.Lock()


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
        log_dir, f"optimizer_{strategy}_{days}d_{ts}_{os.getpid()}.jsonl"
    )


def _persist_result(record: dict) -> None:
    """Append one optimizer result to disk."""
    path = _INCREMENTAL_PATH
    if not path:
        return
    try:
        line = _json.dumps(record, default=str) + "\n"
        with _INCREMENTAL_WRITE_LOCK:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception:
        # NEVER let persistence kill the optimizer
        pass


#  Suchrume

FULL_SPACE = {
    "TREND": {
        "min_pump": [2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        "activation_profit": [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        "trailing_distance": [0.8, 1.0, 1.5, 2.0, 2.5, 3.0],
        "stop_loss": [-1.0, -1.5, -2.0, -2.5, -3.0, -4.0],
        "partial_pct": [0.30, 0.40, 0.50, 0.60],
        "rsi_max": [60.0, 65.0, 70.0, 75.0, 80.0],
    },
    "SPOT": {
        "min_pump": [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "activation_profit": [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "trailing_distance": [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0],
        # stop_loss is a searched dimension (wide range  spot is unleveraged
        # with 4-10% targets). NOTE: +6x grid size; use --quick for a fast scan.
        "stop_loss": [-3.0, -4.0, -6.0, -8.0, -10.0, -12.0],
        "partial_pct": [0.25, 0.30, 0.40, 0.50, 0.60],
        "rsi_max": [65.0, 70.0, 75.0, 80.0, 85.0],
    },
    # FUTURES strategy  uses spot data as proxy (perpetual data is more
    # limited and often shorter history). Results are indicative.
    "FUTURES": {
        "min_pump": [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0],
        "activation_profit": [3.0, 3.5, 4.0, 4.5, 5.0, 6.0, 7.0],
        "trailing_distance": [1.5, 2.0, 2.5, 3.0, 3.5, 4.0],
        "stop_loss": [-2.0, -2.5, -3.0, -3.5, -4.0, -5.0],
        "partial_pct": [0.30, 0.40, 0.50, 0.60],
        "rsi_max": [60.0, 65.0, 70.0, 75.0, 80.0],
    },
}

QUICK_SPACE = {
    strat: {k: v[::2] for k, v in params.items()}
    for strat, params in FULL_SPACE.items()
}


def _finite_optimizer_number(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _finite_optimizer_net(result: dict) -> float | None:
    """Return a trustworthy simulation net value, or None for bad evidence."""
    if not isinstance(result, dict):
        return None
    return _finite_optimizer_number(result.get("net"))


def _optimizer_net_summary(values: list[float]) -> tuple[float, float, float] | None:
    """Summarize finite nets without letting aggregate overflow pass as evidence."""
    try:
        avg = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 0.0
        consistency = 1 - (std / abs(avg)) if avg != 0 else 0.0
    except (ArithmeticError, AttributeError, TypeError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (avg, std, consistency)):
        return None
    return avg, std, max(0.0, min(1.0, consistency))


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


def walk_forward_simulate(
    indexed: dict,
    all_times: list,
    strategy: str,
    use_maker: bool,
    params: dict,
    n_steps: int = 4,
) -> dict:
    """Walk-forward validation: split timeline chronologically, test
    same config on each forward slice. Returns per-slice nets + summary."""
    if n_steps < 2:
        n_steps = 2
    total = len(all_times)
    slice_len = total // (n_steps + 1)
    if slice_len < 10:
        # Not enough data  fall back to single backtest
        s = simulate_fast(indexed, all_times, strategy, use_maker, params)
        net = _finite_optimizer_net(s)
        valid_net = net is not None
        safe_net = net if valid_net else 0.0
        edge_count = int(isinstance(s, dict) and s.get("edge") is True)
        all_profitable = bool(valid_net and safe_net > 0 and edge_count == 1)
        return {
            "slices": [s],
            "slice_nets": [safe_net],
            "consistency": 1.0 if valid_net and safe_net > 0 else 0.0,
            "profitable_count": int(valid_net and safe_net > 0),
            "edge_count": edge_count,
            "valid_slice_count": int(valid_net),
            "invalid_slice_count": int(not valid_net),
            "summary_valid": valid_net,
            "all_profitable": all_profitable,
            "robust": False,
            "fragility": "single-slice (data too short)",
        }

    embargo = int(total * EMBARGO_FRAC) if EMBARGO_FRAC > 0 else 0
    slice_nets = []
    slice_results = []
    valid_slice_count = 0
    edge_count = 0
    for step in range(n_steps):
        # Forward test slice: position (step+1) of (n_steps+1) total
        start = (step + 1) * slice_len
        end = (step + 2) * slice_len if step < n_steps - 1 else total
        # Purge leading lookback bars + embargo gap so each forward slice's
        # signals are warmed from WITHIN the slice (no cross-fold leakage).
        purged_start = min(end, start + embargo + max(0, PURGE_BARS))
        period_times = all_times[purged_start:end]
        if len(period_times) < 10:
            period_times = all_times[start:end]
        period_data = filter_to_period(indexed, set(period_times))
        s = simulate_fast(period_data, period_times, strategy, use_maker, params)
        net = _finite_optimizer_net(s)
        if net is None:
            slice_nets.append(0.0)
        else:
            slice_nets.append(net)
            valid_slice_count += 1
        edge_count += int(isinstance(s, dict) and s.get("edge") is True)
        slice_results.append(s)

    profitable_count = sum(1 for n in slice_nets if n > 0)
    summary = _optimizer_net_summary(slice_nets)
    summary_valid = summary is not None
    avg, std, consistency = summary or (0.0, 0.0, 0.0)
    invalid_slice_count = n_steps - valid_slice_count
    all_profitable = (
        summary_valid
        and
        valid_slice_count == n_steps
        and profitable_count == n_steps
        and edge_count == n_steps
    )
    # Walk-forward passes only with finite positive nets, explicit edge in every
    # slice and moderate variance (consistency > 0.3).
    return {
        "slices": slice_results,
        "slice_nets": slice_nets,
        "avg_net": avg,
        "std_net": std,
        "consistency": consistency,
        "profitable_count": profitable_count,
        "edge_count": edge_count,
        "valid_slice_count": valid_slice_count,
        "invalid_slice_count": invalid_slice_count,
        "summary_valid": summary_valid,
        "all_profitable": all_profitable,
        "robust": all_profitable and consistency > 0.3,
    }


#  Regime Splitting
# Split the timeline by detected market regime and test the config
# separately on each. A "good" strategy on overall data can be a complete
# disaster in one specific regime (e.g. momentum works in bull, dies in chop).
#
# Regime detection: simple BTC-trend bucketing. Each tick is tagged
# BULL/BEAR/CHOP based on the rolling BTC change at that moment. This
# uses BTC change_% from the indexed data  no extra fetch needed.


def _btc_regime_reference(indexed: dict):
    """Return the exact BTC series used by spot or linear-perpetual datasets."""
    if not isinstance(indexed, dict):
        return None
    for symbol in ("BTC", "BTC/USDT", "BTC/USDT:USDT"):
        if symbol in indexed:
            return indexed[symbol]
    return None


def detect_regimes(indexed: dict, all_times: list) -> dict:
    """Classify each timestamp as BULL/BEAR/CHOP using BTC change_%.

    Buckets:
      BULL: BTC 24h change > +3%
      BEAR: BTC 24h change < -3%
      CHOP: in between (low volatility / sideways)
    """
    buckets = {"BULL": [], "BEAR": [], "CHOP": []}
    btc_data = _btc_regime_reference(indexed)
    if not isinstance(btc_data, dict) or not btc_data:
        # Missing reference data is not evidence for a sideways market.
        return buckets
    for t in all_times:
        tick = btc_data.get(t)
        chg = tick.get("change") if isinstance(tick, dict) else None
        if chg is not None:
            chg = _finite_optimizer_number(chg)
        if chg is None:
            buckets["CHOP"].append(t)
        elif chg > 3.0:
            buckets["BULL"].append(t)
        elif chg < -3.0:
            buckets["BEAR"].append(t)
        else:
            buckets["CHOP"].append(t)
    return buckets


def _invalid_regime_split_result(
    regimes: dict,
    all_times: list,
    reason: str,
    invalid_source_sample_count: int = 0,
) -> dict:
    results = {}
    tested_count = 0
    for regime_name, regime_times in regimes.items():
        if len(regime_times) < 10:
            results[regime_name] = {
                "net": 0.0,
                "trades": 0,
                "skipped": "insufficient_data",
            }
            continue
        tested_count += 1
        results[regime_name] = {
            "net": 0.0,
            "trades": 0,
            "edge": False,
            "ratio": len(regime_times) / max(1, len(all_times)),
            "invalid_reason": reason,
        }
    return {
        "regimes": results,
        "profitable_count": 0,
        "edge_count": 0,
        "tested_count": tested_count,
        "valid_regime_count": 0,
        "invalid_regime_count": tested_count,
        "invalid_source_sample_count": invalid_source_sample_count,
        "evidence_valid": False,
        "survives_all": False,
        "reason": reason,
    }


def regime_split_simulate(
    indexed: dict, all_times: list, strategy: str, use_maker: bool, params: dict
) -> dict:
    """Test BULL / BEAR / CHOP without cutting holes into the timeline.

    The simulation must run on continuous bars so stops, liquidation and trailing
    can fire after an entry even if the market regime changes. We then attribute
    each closed trade to the regime at entry time.
    """
    if not isinstance(indexed, dict):
        regimes = detect_regimes(indexed, all_times)
        return _invalid_regime_split_result(
            regimes, all_times, "invalid_regime_source", 1
        )
    btc_data = _btc_regime_reference(indexed)
    invalid_source_sample_count = 0
    if not isinstance(btc_data, dict) or not btc_data:
        regimes = detect_regimes(indexed, all_times)
        return _invalid_regime_split_result(
            regimes, all_times, "invalid_regime_source", 1
        )
    for timestamp in all_times:
        tick = btc_data.get(timestamp)
        if not isinstance(tick, dict):
            invalid_source_sample_count += 1
            continue
        change = tick.get("change")
        if change is not None and _finite_optimizer_number(change) is None:
            invalid_source_sample_count += 1
    regimes = detect_regimes(indexed, all_times)
    if invalid_source_sample_count:
        return _invalid_regime_split_result(
            regimes,
            all_times,
            "invalid_regime_source",
            invalid_source_sample_count,
        )
    regime_by_time = {
        t: regime_name
        for regime_name, regime_times in regimes.items()
        for t in regime_times
    }
    full = simulate_fast(indexed, all_times, strategy, use_maker, params)
    if not isinstance(full, dict) or full.get("invalid_reason") is not None:
        return _invalid_regime_split_result(
            regimes, all_times, "invalid_full_simulation"
        )
    closed = full.get("closed_trades")
    if not isinstance(closed, list) or any(
        not isinstance(trade, dict) for trade in closed
    ):
        return _invalid_regime_split_result(
            regimes, all_times, "invalid_closed_trades"
        )
    results = {}
    valid_regime_count = 0
    invalid_regime_count = 0
    for regime_name, regime_times in regimes.items():
        if len(regime_times) < 10:
            results[regime_name] = {
                "net": 0,
                "trades": 0,
                "skipped": "insufficient_data",
            }
            continue
        regime_trades = [
            t for t in closed if regime_by_time.get(t.get("entry_time")) == regime_name
        ]
        if regime_trades:
            costs = [_finite_optimizer_number(t.get("cost")) for t in regime_trades]
            gross = [_finite_optimizer_number(t.get("gross")) for t in regime_trades]
            if any(value is None for value in costs + gross):
                s = {"invalid_reason": "invalid_regime_trade"}
            else:
                try:
                    total_costs = math.fsum(costs)
                    total_gross = math.fsum(gross)
                except (ArithmeticError, ValueError):
                    s = {"invalid_reason": "invalid_regime_trade"}
                else:
                    s = _compute_stats(regime_trades, total_costs, total_gross)
        else:
            s = {"net": 0.0, "trade_count": 0, "edge": False}
        net = _finite_optimizer_number(s.get("net"))
        trade_count = s.get("trade_count")
        edge = s.get("edge")
        invalid_reason = s.get("invalid_reason")
        if (
            invalid_reason is not None
            or net is None
            or isinstance(trade_count, bool)
            or not isinstance(trade_count, int)
            or trade_count < 0
            or not isinstance(edge, bool)
        ):
            invalid_regime_count += 1
            results[regime_name] = {
                "net": 0.0,
                "trades": 0,
                "edge": False,
                "ratio": len(regime_times) / max(1, len(all_times)),
                "invalid_reason": invalid_reason or "invalid_regime_stats",
            }
            continue
        valid_regime_count += 1
        results[regime_name] = {
            "net": net,
            "trades": trade_count,
            "edge": edge,
            "ratio": len(regime_times) / max(1, len(all_times)),
        }
    # "Survives all regimes" = profitable in every regime with sufficient data
    profitable_regimes = sum(
        1
        for r in results.values()
        if r.get("net", 0) > 0
        and r.get("edge") is True
        and "skipped" not in r
        and "invalid_reason" not in r
    )
    edge_count = sum(
        1
        for result in results.values()
        if result.get("edge") is True and "invalid_reason" not in result
    )
    tested_regimes = sum(1 for r in results.values() if "skipped" not in r)
    evidence_valid = (
        invalid_regime_count == 0
        and valid_regime_count == tested_regimes
        and invalid_source_sample_count == 0
    )
    return {
        "regimes": results,
        "profitable_count": profitable_regimes,
        "edge_count": edge_count,
        "tested_count": tested_regimes,
        "valid_regime_count": valid_regime_count,
        "invalid_regime_count": invalid_regime_count,
        "invalid_source_sample_count": invalid_source_sample_count,
        "evidence_valid": evidence_valid,
        "survives_all": evidence_valid
        and profitable_regimes == tested_regimes
        and tested_regimes >= 2,
    }


#  Outlier-Dependency Test
# A strategy whose entire edge comes from 1-2 lucky home-run trades is
# fragile  in live trading those same outliers may never occur again.
# Test: remove the top N independent positions and recompute net. If the strategy still
# profits, the edge is broadly distributed.


def outlier_dependency_test(
    indexed: dict,
    all_times: list,
    strategy: str,
    use_maker: bool,
    params: dict,
    top_n_to_remove: list = (1, 5, 10),
) -> dict:
    """Re-simulate and remove the top N independent position outcomes."""
    s_full = simulate_fast(indexed, all_times, strategy, use_maker, params)
    if not isinstance(s_full, dict) or s_full.get("invalid_reason") is not None:
        return _invalid_outlier_result("invalid_full_simulation")
    full_net = _finite_optimizer_net(s_full)
    raw_nets = s_full.get("position_net_trades")
    closed_positions = s_full.get("closed_positions")
    if (
        full_net is None
        or not isinstance(raw_nets, (list, tuple))
        or not isinstance(closed_positions, list)
        or len(closed_positions) != len(raw_nets)
    ):
        return _invalid_outlier_result("invalid_net_evidence")
    normalized_nets = [_finite_optimizer_number(value) for value in raw_nets]
    invalid_trade_count = sum(value is None for value in normalized_nets)
    if invalid_trade_count:
        return _invalid_outlier_result(
            "invalid_trade_net",
            trade_count=len(raw_nets),
            invalid_trade_count=invalid_trade_count,
        )
    try:
        trade_net_sum = math.fsum(normalized_nets)
    except (ArithmeticError, ValueError):
        return _invalid_outlier_result(
            "invalid_trade_net", trade_count=len(raw_nets)
        )
    if not math.isfinite(trade_net_sum) or not math.isclose(
        full_net, trade_net_sum, rel_tol=1e-9, abs_tol=1e-9
    ):
        return _invalid_outlier_result(
            "inconsistent_net_evidence", trade_count=len(raw_nets)
        )

    symbol_rows: dict[str, list[float]] = {}
    for position, position_net in zip(closed_positions, normalized_nets):
        if not isinstance(position, dict):
            return _invalid_outlier_result(
                "invalid_symbol_evidence", trade_count=len(raw_nets)
            )
        symbol = position.get("symbol")
        recorded_net = _finite_optimizer_number(position.get("net"))
        if (
            not isinstance(symbol, str)
            or not symbol.strip()
            or recorded_net is None
            or not math.isclose(
                recorded_net, position_net, rel_tol=1e-9, abs_tol=1e-9
            )
        ):
            return _invalid_outlier_result(
                "invalid_symbol_evidence", trade_count=len(raw_nets)
            )
        symbol_rows.setdefault(symbol.strip(), []).append(recorded_net)
    try:
        symbol_nets = {
            symbol: math.fsum(values)
            for symbol, values in sorted(symbol_rows.items())
        }
        symbol_net_sum = math.fsum(symbol_nets.values())
        positive_symbol_net = math.fsum(
            max(value, 0.0) for value in symbol_nets.values()
        )
    except (ArithmeticError, ValueError):
        return _invalid_outlier_result(
            "invalid_symbol_summary", trade_count=len(raw_nets)
        )
    if (
        not symbol_nets
        or not math.isfinite(symbol_net_sum)
        or not math.isfinite(positive_symbol_net)
        or positive_symbol_net <= 0.0
        or not math.isclose(symbol_net_sum, full_net, rel_tol=1e-9, abs_tol=1e-9)
    ):
        return _invalid_outlier_result(
            "invalid_symbol_summary", trade_count=len(raw_nets)
        )
    dominant_symbol, dominant_symbol_net = max(
        symbol_nets.items(), key=lambda item: (item[1], item[0])
    )
    dominant_positive_share = dominant_symbol_net / positive_symbol_net
    symbol_adjusted_net = full_net - dominant_symbol_net
    if not all(
        math.isfinite(value)
        for value in (
            dominant_symbol_net,
            dominant_positive_share,
            symbol_adjusted_net,
        )
    ):
        return _invalid_outlier_result(
            "invalid_symbol_summary", trade_count=len(raw_nets)
        )
    symbol_fragile = bool(
        len(symbol_nets) < 2
        or dominant_positive_share > MAX_DOMINANT_SYMBOL_POSITIVE_SHARE
        or symbol_adjusted_net <= 0.0
    )
    if (
        not isinstance(top_n_to_remove, (list, tuple))
        or 1 not in top_n_to_remove
        or any(
            isinstance(n, bool) or not isinstance(n, int) or n <= 0
            for n in top_n_to_remove
        )
    ):
        return _invalid_outlier_result(
            "invalid_removal_scenarios", trade_count=len(raw_nets)
        )

    nets = sorted(normalized_nets, reverse=True)
    trade_count = len(nets)

    results = {}
    for n in top_n_to_remove:
        if trade_count < n + 5:
            results[f"remove_top_{n}"] = {"skipped": "too_few_trades"}
            continue
        try:
            removed = math.fsum(nets[:n])
            adjusted_net = full_net - removed
            drop_pct = (removed / abs(full_net) * 100) if full_net else 0.0
        except (ArithmeticError, ValueError, ZeroDivisionError):
            return _invalid_outlier_result(
                "invalid_scenario_summary", trade_count=trade_count
            )
        if not all(math.isfinite(value) for value in (removed, adjusted_net, drop_pct)):
            return _invalid_outlier_result(
                "invalid_scenario_summary", trade_count=trade_count
            )
        results[f"remove_top_{n}"] = {
            "removed_net": removed,
            "adjusted_net": adjusted_net,
            "drop_pct": drop_pct,
            "still_positive": adjusted_net > 0,
        }

    r1 = results.get("remove_top_1", {})
    outlier_fragile = r1.get("still_positive") is False if "skipped" not in r1 else None

    return {
        "full_net": full_net,
        "trade_count": trade_count,
        "valid_trade_count": trade_count,
        "invalid_trade_count": 0,
        "full_net_consistent": True,
        "evidence_valid": True,
        "scenarios": results,
        "outlier_fragile": outlier_fragile,
        "symbol_nets": symbol_nets,
        "symbol_count": len(symbol_nets),
        "symbol_net_consistent": True,
        "positive_symbol_net": positive_symbol_net,
        "dominant_symbol": dominant_symbol,
        "dominant_symbol_net": dominant_symbol_net,
        "dominant_positive_share": dominant_positive_share,
        "remove_dominant_symbol": {
            "adjusted_net": symbol_adjusted_net,
            "still_positive": symbol_adjusted_net > 0.0,
        },
        "symbol_fragile": symbol_fragile,
    }


def _invalid_outlier_result(
    reason: str, trade_count: int = 0, invalid_trade_count: int = 0
) -> dict:
    return {
        "full_net": 0.0,
        "trade_count": trade_count,
        "valid_trade_count": max(0, trade_count - invalid_trade_count),
        "invalid_trade_count": invalid_trade_count,
        "full_net_consistent": False,
        "evidence_valid": False,
        "scenarios": {},
        "outlier_fragile": None,
        "symbol_nets": {},
        "symbol_count": 0,
        "symbol_net_consistent": False,
        "positive_symbol_net": 0.0,
        "dominant_symbol": None,
        "dominant_symbol_net": 0.0,
        "dominant_positive_share": None,
        "remove_dominant_symbol": {},
        "symbol_fragile": None,
        "reason": reason,
    }


#  Monte-Carlo Equity Perturbation
# Test if equity holds up when trade order/timing is shuffled. Many bad
# strategies only profit due to a lucky sequence; Monte Carlo destroys
# that illusion. We use the published net as the anchor and synthesize
# perturbations using random trade reordering (approximation; precise
# implementation needs per-trade detail).


def _invalid_monte_carlo_result(
    reason: str,
    requested_runs: int | None = None,
    trade_count: int = 0,
    valid_trade_count: int = 0,
    invalid_trade_count: int = 0,
) -> dict:
    return {
        "requested_runs": requested_runs,
        "runs": 0,
        "positive_run_count": 0,
        "positive_share": None,
        "trade_count": trade_count,
        "valid_trade_count": valid_trade_count,
        "invalid_trade_count": invalid_trade_count,
        "median_final": None,
        "worst_decile": None,
        "robust": None,
        "concerning": None,
        "method": "block_bootstrap",
        "evidence_valid": False,
        "reason": reason,
    }


def monte_carlo_perturbation(
    s_full: dict, n_runs: int = 100, seed: int | None = None
) -> dict:
    """Block-bootstrap the REAL per-position net returns and report the share of
    resampled equity paths that end positive.

    Resamples contiguous blocks of actual position P&L (USDT) with replacement,
    preserving the fat tails and local autocorrelation a Normal(avg, std)
    proxy destroys. Robust strategy >=90% positive; <70% concerning.
    """
    if isinstance(n_runs, bool) or not isinstance(n_runs, int) or n_runs <= 0:
        return _invalid_monte_carlo_result("invalid_run_count")
    if not isinstance(s_full, dict) or s_full.get("invalid_reason") is not None:
        return _invalid_monte_carlo_result(
            "invalid_full_simulation", requested_runs=n_runs
        )
    raw_nets = s_full.get("position_net_trades")
    if not isinstance(raw_nets, (list, tuple)):
        return _invalid_monte_carlo_result(
            "invalid_trade_container", requested_runs=n_runs
        )
    normalized = [_finite_optimizer_number(value) for value in raw_nets]
    invalid_trade_count = sum(value is None for value in normalized)
    trade_count = len(normalized)
    valid_trade_count = trade_count - invalid_trade_count
    if invalid_trade_count:
        return _invalid_monte_carlo_result(
            "invalid_trade_net",
            requested_runs=n_runs,
            trade_count=trade_count,
            valid_trade_count=valid_trade_count,
            invalid_trade_count=invalid_trade_count,
        )
    nets = normalized
    if trade_count < 5:
        return _invalid_monte_carlo_result(
            "insufficient_stats",
            requested_runs=n_runs,
            trade_count=trade_count,
            valid_trade_count=valid_trade_count,
        )

    run_seed = _OPTIMIZER_SEED if seed is None else seed
    if isinstance(run_seed, bool) or not isinstance(run_seed, int):
        return _invalid_monte_carlo_result("invalid_seed")
    rng = _random.Random(run_seed)  # deterministic for reproducibility
    block = max(1, min(10, trade_count // 5))
    positive_runs = 0
    final_equities = []
    for _ in range(n_runs):
        seq = []
        while len(seq) < trade_count:
            start = rng.randrange(trade_count)
            seq.extend(nets[start : start + block])
        try:
            eq = math.fsum(seq[:trade_count])
        except (ArithmeticError, ValueError):
            return _invalid_monte_carlo_result(
                "invalid_equity_summary",
                requested_runs=n_runs,
                trade_count=trade_count,
                valid_trade_count=valid_trade_count,
            )
        if not math.isfinite(eq):
            return _invalid_monte_carlo_result(
                "invalid_equity_summary",
                requested_runs=n_runs,
                trade_count=trade_count,
                valid_trade_count=valid_trade_count,
            )
        final_equities.append(eq)
        if eq > 0:
            positive_runs += 1
    share = positive_runs / n_runs
    final_equities.sort()
    return {
        "requested_runs": n_runs,
        "runs": n_runs,
        "positive_run_count": positive_runs,
        "positive_share": share,
        "trade_count": trade_count,
        "valid_trade_count": valid_trade_count,
        "invalid_trade_count": 0,
        "median_final": final_equities[n_runs // 2],
        "worst_decile": final_equities[max(0, n_runs // 10 - 1)],
        "robust": share >= 0.90,
        "concerning": share < 0.70,
        "method": "block_bootstrap",
        "evidence_valid": True,
    }


#  K-Fold Validation


def split_into_folds(
    all_times: list,
    k: int = 4,
    purge_bars: int = PURGE_BARS,
    embargo_frac: float = EMBARGO_FRAC,
) -> list:
    """Teilt Zeitstempel in K gleich groe, sortierte, PURGED+EMBARGOED Perioden.

    Each raw fold is a chronological slice. To kill cross-fold indicator leakage
    (indicators are warmed over the FULL series in the backtester), the leading
    `purge_bars` timestamps of every fold are PURGED so the fold's signals are
    warmed from WITHIN the fold, and an EMBARGO gap of `embargo_frac` of the
    total length is dropped from the FRONT of each fold (except the first) to
    separate adjacent folds. Resulting fold time-sets are disjoint with a gap."""
    if not isinstance(all_times, (list, tuple)):
        raise ValueError("all_times must be a chronological sequence")
    if isinstance(k, bool) or not isinstance(k, int) or k < 2:
        raise ValueError("k must be an integer >= 2")
    if (
        isinstance(purge_bars, bool)
        or not isinstance(purge_bars, int)
        or purge_bars < 0
    ):
        raise ValueError("purge_bars must be an integer >= 0")
    if isinstance(embargo_frac, bool) or not isinstance(
        embargo_frac, (int, float)
    ):
        raise ValueError("embargo_frac must be finite and in [0, 1)")
    embargo_frac = float(embargo_frac)
    if not math.isfinite(embargo_frac) or not 0.0 <= embargo_frac < 1.0:
        raise ValueError("embargo_frac must be finite and in [0, 1)")
    times = list(all_times)
    numeric_times = [_optimizer_time_number(value) for value in times]
    if any(value is None for value in numeric_times) or any(
        current <= previous
        for previous, current in zip(numeric_times, numeric_times[1:])
    ):
        raise ValueError("all_times must be finite, unique and strictly increasing")
    n = len(times)
    fold_len = n // k
    embargo = int(n * embargo_frac) if embargo_frac > 0 else 0
    folds = []
    for i in range(k):
        start = i * fold_len
        end = (i + 1) * fold_len if i < k - 1 else n
        # Embargo gap before every fold after the first.
        purged_start = start + (embargo if i > 0 else 0) + max(0, purge_bars)
        if purged_start < end:
            folds.append(times[purged_start:end])
        else:
            folds.append([])
    return folds


def filter_to_period(indexed: dict, time_set: set) -> dict:
    """Filtert vorindexierte Daten auf eine Zeitperiode."""
    return {
        sym: {t: d for t, d in tick_map.items() if t in time_set}
        for sym, tick_map in indexed.items()
    }


def _optimizer_time_number(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)):
            number = float(value)
        elif isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            parsed = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
            )
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            number = float(parsed.timestamp())
        elif isinstance(value, datetime):
            parsed = value
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            number = float(parsed.timestamp())
        else:
            timestamp = getattr(value, "timestamp", None)
            if not callable(timestamp):
                return None
            number = float(timestamp())
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return number if math.isfinite(number) else None


def _prepare_kfold_data(indexed: dict, folds: list) -> list[dict]:
    return [filter_to_period(indexed, set(period_times)) for period_times in folds]


def kfold_simulate(
    indexed: dict,
    folds: list,
    strategy: str,
    use_maker: bool,
    params: dict,
    *,
    prepared_folds: list[dict] | None = None,
) -> dict:
    """
    Testet die Config auf K unabhngigen Perioden.
    Eine Config gilt als robust wenn sie in ALLEN Folds profitabel ist.
    """
    if not folds:
        return {
            "fold_nets": [],
            "fold_results": [],
            "edge_count": 0,
            "valid_fold_count": 0,
            "invalid_fold_count": 0,
            "summary_valid": False,
            "profit_count": 0,
            "avg_net": 0.0,
            "std_net": 0.0,
            "consistency": 0.0,
            "robust": False,
        }
    if prepared_folds is None:
        prepared_folds = _prepare_kfold_data(indexed, folds)
    if len(prepared_folds) != len(folds):
        raise ValueError("prepared fold data must match fold boundaries")
    fold_results = []
    for period_times, period_data in zip(folds, prepared_folds):
        s = simulate_fast(period_data, period_times, strategy, use_maker, params)
        s["period_start_ts"] = (
            _optimizer_time_number(period_times[0]) if period_times else None
        )
        s["period_end_ts"] = (
            _optimizer_time_number(period_times[-1]) if period_times else None
        )
        fold_results.append(s)

    nets = []
    valid_fold_count = 0
    for result in fold_results:
        net = _finite_optimizer_net(result)
        if net is None:
            nets.append(-9999.0)
        else:
            nets.append(net)
            valid_fold_count += 1
    invalid_fold_count = len(folds) - valid_fold_count
    edge_count = sum(
        1
        for result in fold_results
        if isinstance(result, dict) and result.get("edge") is True
    )
    profit_cnt = sum(1 for n in nets if n > 0)
    summary = _optimizer_net_summary(nets)
    summary_valid = summary is not None
    avg_net, std_net, consistency = summary or (0.0, 0.0, 0.0)

    return {
        "fold_nets": nets,
        "fold_results": fold_results,
        "edge_count": edge_count,
        "valid_fold_count": valid_fold_count,
        "invalid_fold_count": invalid_fold_count,
        "summary_valid": summary_valid,
        "profit_count": profit_cnt,
        "avg_net": avg_net,
        "std_net": std_net,
        "consistency": consistency,
        "robust": bool(folds)
        and summary_valid
        and valid_fold_count == len(folds)
        and profit_cnt == len(folds)
        and edge_count == len(folds),
    }


def holdout_simulate(
    indexed: dict, holdout_times: list, strategy: str, use_maker: bool, params: dict
) -> dict:
    """Single OUT-OF-SAMPLE run on the reserved holdout slice  data the search
    never saw during tuning. Same engine as one K-fold period."""
    if not holdout_times:
        return {"net": 0.0, "trades": 0, "edge": False, "skipped": True}
    period_data = filter_to_period(indexed, set(holdout_times))
    return simulate_fast(period_data, holdout_times, strategy, use_maker, params)


FINAL_HOLDOUT_MIN_TRADES = 30
MAX_DOMINANT_SYMBOL_POSITIVE_SHARE = 0.50


def _candidate_rank_number(value) -> float:
    number = _finite_optimizer_number(value)
    return number if number is not None else float("-inf")


def _candidate_tie_key(candidate: dict) -> str:
    try:
        return _json.dumps(
            candidate.get("params") or {},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, RecursionError):
        return type(candidate.get("params")).__name__


def _candidate_ranking_error(candidate) -> str | None:
    """Return why a candidate is unsafe to rank, or None for valid evidence."""
    if not isinstance(candidate, dict):
        return "candidate must be a mapping"

    params = candidate.get("params")
    if not isinstance(params, dict):
        return "params must be a mapping"
    try:
        _json.dumps(
            params,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, RecursionError):
        return "params must be deterministic JSON"

    stats = candidate.get("stats")
    if not isinstance(stats, dict) or _finite_optimizer_net(stats) is None:
        return "stats.net must be a finite number"

    kfold = candidate.get("kfold")
    if not isinstance(kfold, dict):
        return "kfold must be a mapping"
    robust = kfold.get("robust")
    if not isinstance(robust, bool):
        return "kfold.robust must be boolean"

    missing = object()
    deep_complete = candidate.get("deep_validation_complete", missing)
    if deep_complete is not missing and not isinstance(deep_complete, bool):
        return "deep_validation_complete must be boolean when present"

    if not robust:
        return None
    if _finite_optimizer_number(candidate.get("score")) is None:
        return "robust candidate score must be a finite number"

    fold_nets = kfold.get("fold_nets")
    fold_results = kfold.get("fold_results")
    if (
        not isinstance(fold_nets, (list, tuple))
        or len(fold_nets) < 2
        or not isinstance(fold_results, (list, tuple))
        or len(fold_results) != len(fold_nets)
    ):
        return "robust candidate requires at least two matching fold results"

    finite_nets = [_finite_optimizer_number(value) for value in fold_nets]
    if any(value is None or value <= 0.0 for value in finite_nets):
        return "robust candidate requires finite positive fold nets"
    for fold_result, fold_net in zip(fold_results, finite_nets):
        result_net = _finite_optimizer_net(fold_result)
        if (
            result_net is None
            or fold_result.get("edge") is not True
            or not math.isclose(result_net, fold_net, rel_tol=1e-9, abs_tol=1e-9)
        ):
            return "robust candidate fold result evidence is inconsistent"

    fold_count = len(fold_nets)
    expected_counts = {
        "valid_fold_count": fold_count,
        "invalid_fold_count": 0,
        "profit_count": fold_count,
        "edge_count": fold_count,
    }
    for name, expected in expected_counts.items():
        value = kfold.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value != expected:
            return f"robust candidate {name} is inconsistent"
    if kfold.get("summary_valid") is not True:
        return "robust candidate summary_valid must be true"

    summary = _optimizer_net_summary(finite_nets)
    if summary is None:
        return "robust candidate fold summary is invalid"
    for name, expected in zip(("avg_net", "std_net", "consistency"), summary):
        value = _finite_optimizer_number(kfold.get(name))
        if value is None or not math.isclose(
            value, expected, rel_tol=1e-9, abs_tol=1e-9
        ):
            return f"robust candidate {name} is inconsistent"
    return None


def rank_optimizer_candidates(results: list[dict]) -> list[dict]:
    """Rank candidates using training and inner-validation evidence only.

    Final-holdout fields are deliberately ignored. Once the winner is frozen,
    holdout failure is a NO-GO rather than a reason to promote the runner-up.
    """
    if not isinstance(results, (list, tuple)):
        return []

    robust = []
    fragile = []
    for candidate in results:
        error = _candidate_ranking_error(candidate)
        if error is not None:
            if isinstance(candidate, dict):
                candidate["ranking_evidence_valid"] = False
                candidate["ranking_error"] = error
            continue
        candidate["ranking_evidence_valid"] = True
        candidate.pop("ranking_error", None)
        if candidate["kfold"]["robust"] is True:
            robust.append(candidate)
        else:
            fragile.append(candidate)
    robust.sort(
        key=lambda r: (
            0 if r.get("deep_validation_complete") is not False else 1,
            -_candidate_rank_number(r.get("score")),
            _candidate_tie_key(r),
        ),
    )
    fragile.sort(
        key=lambda r: (
            -_candidate_rank_number(r.get("stats", {}).get("net")),
            _candidate_tie_key(r),
        ),
    )
    return robust + fragile


def select_deep_validation_candidates(
    results: list[dict], top_n: int
) -> list[dict]:
    """Select the ranked robust top-K, independent of worker completion order."""
    if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n < 1:
        raise ValueError("top_n must be an integer >= 1")
    ranked_robust = [
        row
        for row in rank_optimizer_candidates(results)
        if row["kfold"]["robust"] is True
    ]
    deep_k = min(len(ranked_robust), max(top_n, 10))
    return ranked_robust[:deep_k]


def _finite_holdout_number(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _holdout_values_match(actual: float, expected: float) -> bool:
    try:
        difference = abs(actual - expected)
        tolerance = max(
            1e-12,
            8.0 * math.ulp(actual),
            8.0 * math.ulp(expected),
        )
    except (OverflowError, TypeError, ValueError):
        return False
    return math.isfinite(difference) and difference <= tolerance


def _holdout_trade_totals(
    stats: dict,
) -> tuple[float, float, float, float, float] | None:
    trades = stats.get("closed_trades") or []
    if not isinstance(trades, list) or not trades:
        return None
    gross_values = []
    cost_values = []
    net_values = []
    fee_values = []
    funding_values = []
    for trade in trades:
        if not isinstance(trade, dict):
            return None
        gross = _finite_holdout_number(trade.get("gross"))
        fees = _finite_holdout_number(trade.get("fees"))
        funding = _finite_holdout_number(trade.get("funding"))
        cost = _finite_holdout_number(trade.get("cost"))
        recorded_net = _finite_holdout_number(trade.get("net"))
        if None in (gross, fees, funding, cost, recorded_net) or fees < 0.0:
            return None
        expected_cost = fees + funding
        expected_net = gross - cost
        if (
            not math.isfinite(expected_cost)
            or not math.isfinite(expected_net)
            or not _holdout_values_match(cost, expected_cost)
            or not _holdout_values_match(recorded_net, expected_net)
        ):
            return None
        gross_values.append(gross)
        cost_values.append(cost)
        net_values.append(recorded_net)
        fee_values.append(fees)
        funding_values.append(funding)
    try:
        gross = math.fsum(gross_values)
        costs = math.fsum(cost_values)
        net = math.fsum(net_values)
        fees = math.fsum(fee_values)
        funding = math.fsum(funding_values)
    except (OverflowError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (gross, costs, net, fees, funding)):
        return None
    return gross, costs, net, fees, funding


def _holdout_aggregates_consistent(
    stats: dict, totals: tuple[float, float, float, float, float] | None
) -> bool:
    if totals is None:
        return False
    reported = tuple(
        _finite_holdout_number(stats.get(name))
        for name in ("gross", "costs", "net", "total_fees", "total_funding")
    )
    return all(
        actual is not None and _holdout_values_match(actual, expected)
        for actual, expected in zip(reported, totals)
    )


def _holdout_position_outcomes(stats: dict) -> tuple[list[float], list[float]] | None:
    """Rebuild independent position returns from partial and terminal fills."""
    trades = stats.get("closed_trades")
    net_trades = stats.get("net_trades")
    position_net_trades = stats.get("position_net_trades")
    closed_positions = stats.get("closed_positions")
    if (
        not isinstance(trades, list)
        or not trades
        or not isinstance(net_trades, list)
        or len(net_trades) != len(trades)
        or not isinstance(position_net_trades, list)
        or not isinstance(closed_positions, list)
    ):
        return None
    groups: dict[int, list[dict]] = {}
    for index, trade in enumerate(trades):
        position_id = trade.get("position_id") if isinstance(trade, dict) else None
        recorded_net = (
            _finite_holdout_number(trade.get("net"))
            if isinstance(trade, dict)
            else None
        )
        series_net = _finite_holdout_number(net_trades[index])
        notional = (
            _finite_holdout_number(trade.get("notional"))
            if isinstance(trade, dict)
            else None
        )
        if (
            isinstance(position_id, bool)
            or not isinstance(position_id, int)
            or position_id < 1
            or not isinstance(trade.get("is_partial"), bool)
            or recorded_net is None
            or series_net is None
            or notional is None
            or notional <= 0.0
            or not _holdout_values_match(series_net, recorded_net)
        ):
            return None
        groups.setdefault(position_id, []).append(trade)
    reconstructed = []
    for position_id, fragments in groups.items():
        terminal = [trade for trade in fragments if trade["is_partial"] is False]
        if len(terminal) != 1 or fragments[-1] is not terminal[0]:
            return None
        try:
            position_net = math.fsum(float(trade["net"]) for trade in fragments)
            position_notional = math.fsum(
                float(trade["notional"]) for trade in fragments
            )
            position_pct = position_net / position_notional * 100.0
        except (ArithmeticError, TypeError, ValueError, ZeroDivisionError):
            return None
        if not all(
            math.isfinite(value)
            for value in (position_net, position_notional, position_pct)
        ):
            return None
        exit_time = _optimizer_time_number(terminal[0].get("exit_time"))
        if exit_time is None:
            return None
        reconstructed.append(
            (exit_time, position_id, position_pct, position_net)
        )
    reconstructed.sort(key=lambda row: (row[0], row[1]))
    position_pcts = [row[2] for row in reconstructed]
    position_nets = [row[3] for row in reconstructed]
    if (
        len(position_nets) < 1
        or len(position_net_trades) != len(position_nets)
        or len(closed_positions) != len(position_nets)
    ):
        return None
    for index, (_exit_time, position_id, _position_pct, _position_net) in enumerate(
        reconstructed
    ):
        reported_net = _finite_holdout_number(position_net_trades[index])
        summary = closed_positions[index]
        if not isinstance(summary, dict):
            return None
        summary_net = _finite_holdout_number(summary.get("net"))
        summary_pct = _finite_holdout_number(summary.get("net_pct"))
        if (
            summary.get("position_id") != position_id
            or reported_net is None
            or summary_net is None
            or summary_pct is None
            or not _holdout_values_match(reported_net, position_nets[index])
            or not _holdout_values_match(summary_net, position_nets[index])
            or not _holdout_values_match(summary_pct, position_pcts[index])
        ):
            return None
    return position_pcts, position_nets


def _holdout_full_trade_count(stats: dict) -> int | None:
    """Reproduce the independent full-close sample from exact trade evidence."""
    trades = stats.get("closed_trades")
    if not isinstance(trades, list) or not trades:
        return None
    groups: dict[int, list[dict]] = {}
    for trade in trades:
        position_id = trade.get("position_id") if isinstance(trade, dict) else None
        if (
            isinstance(position_id, bool)
            or not isinstance(position_id, int)
            or position_id < 1
            or not isinstance(trade.get("is_partial"), bool)
        ):
            return None
        groups.setdefault(position_id, []).append(trade)
    if any(
        len([trade for trade in fragments if trade["is_partial"] is False]) != 1
        or fragments[-1]["is_partial"] is not False
        for fragments in groups.values()
    ):
        return None
    full_trade_count = len(groups)
    for name in ("full_trades", "trade_count"):
        reported = _finite_holdout_number(stats.get(name))
        if (
            reported is None
            or reported < 0.0
            or not reported.is_integer()
            or int(reported) != full_trade_count
        ):
            return None
    return full_trade_count


def _holdout_outcomes_consistent(stats: dict, full_trade_count: int | None) -> bool:
    """Reproduce outcome counts, win rate and additive PnL from trade rows."""
    outcomes = _holdout_position_outcomes(stats)
    if full_trade_count is None or outcomes is None:
        return False
    position_pcts, _position_nets = outcomes
    if len(position_pcts) != full_trade_count:
        return False
    expected_counts = (
        sum(pct > PNL_ZERO_TOLERANCE_PCT for pct in position_pcts),
        sum(pct < -PNL_ZERO_TOLERANCE_PCT for pct in position_pcts),
        sum(abs(pct) <= PNL_ZERO_TOLERANCE_PCT for pct in position_pcts),
    )
    reported_counts = []
    for name in ("win_count", "loss_count", "breakeven_count"):
        reported = _finite_holdout_number(stats.get(name))
        if reported is None or reported < 0.0 or not reported.is_integer():
            return False
        reported_counts.append(int(reported))
    expected_win_rate = expected_counts[0] / full_trade_count
    reported_win_rate = _finite_holdout_number(stats.get("win_rate"))
    return tuple(reported_counts) == expected_counts and bool(
        reported_win_rate is not None
        and 0.0 <= reported_win_rate <= 1.0
        and _holdout_values_match(reported_win_rate, expected_win_rate)
    )


def _holdout_cost_stress_pass(stats: dict) -> bool:
    """Require profit after conservative per-trade execution-cost stress.

    Fees and adverse funding charges are doubled. Beneficial funding credits
    are removed instead of doubled into a larger synthetic profit.
    """
    totals = _holdout_trade_totals(stats)
    if totals is None:
        return False
    trades = stats.get("closed_trades")
    if not isinstance(trades, list) or not trades:
        return False
    stressed_rows = []
    for trade in trades:
        if not isinstance(trade, dict):
            return False
        gross = _finite_holdout_number(trade.get("gross"))
        fees = _finite_holdout_number(trade.get("fees"))
        funding = _finite_holdout_number(trade.get("funding"))
        if None in (gross, fees, funding) or fees < 0.0:
            return False
        stressed_rows.append(gross - (2.0 * fees) - (2.0 * max(funding, 0.0)))
    try:
        stressed_net = math.fsum(stressed_rows)
    except (ArithmeticError, ValueError):
        return False
    return math.isfinite(stressed_net) and stressed_net > 0.0


def freeze_and_evaluate_final_holdout(
    candidates: list[dict], evaluator, *, min_trades: int = FINAL_HOLDOUT_MIN_TRADES
) -> dict:
    """Freeze one winner, evaluate it once, and attach fail-closed evidence."""
    if isinstance(min_trades, bool) or not isinstance(min_trades, int) or min_trades < 1:
        raise ValueError("min_trades must be an integer >= 1")
    ranked = rank_optimizer_candidates(candidates)
    if not ranked:
        raise ValueError("no optimizer candidates to freeze")
    winner = ranked[0]
    deep_validation_error = _deep_validation_admission_error(winner)
    deep_validation_complete = deep_validation_error is None
    winner["deep_validation_admission_error"] = deep_validation_error
    holdout_evaluation_attempted = False
    holdout_evaluation_error = None
    if not deep_validation_complete:
        holdout = {
            "skipped": True,
            "reason": "deep_validation_incomplete",
        }
    else:
        holdout_evaluation_attempted = True
        try:
            holdout = evaluator(winner["params"])
        except Exception as exc:
            holdout = {}
            try:
                detail = str(exc)[:160]
            except Exception:
                detail = "<unprintable exception>"
            holdout_evaluation_error = f"{type(exc).__name__}: {detail}"
        if not isinstance(holdout, dict):
            holdout_evaluation_error = (
                "InvalidResult: holdout evaluator returned "
                f"{type(holdout).__name__}, expected mapping"
            )
            holdout = {}
    holdout_net = _finite_holdout_number(holdout.get("net"))
    trade_number = _finite_holdout_number(holdout.get("trades"))
    trade_count_valid = bool(
        trade_number is not None
        and trade_number >= 0.0
        and trade_number.is_integer()
    )
    trades = int(trade_number) if trade_count_valid else 0
    closed_trades = holdout.get("closed_trades")
    trade_count_consistent = bool(
        isinstance(closed_trades, list) and len(closed_trades) == trades
    )
    trade_totals = _holdout_trade_totals(holdout)
    holdout_trade_accounting_valid = trade_totals is not None
    holdout_aggregates_consistent = _holdout_aggregates_consistent(
        holdout, trade_totals
    )
    holdout_full_trade_count = _holdout_full_trade_count(holdout)
    holdout_sample_consistent = holdout_full_trade_count is not None
    holdout_outcomes_consistent = _holdout_outcomes_consistent(
        holdout, holdout_full_trade_count
    )
    skipped = holdout.get("skipped", False)
    holdout_net_consistent = bool(
        holdout_net is not None
        and trade_totals is not None
        and _holdout_values_match(holdout_net, trade_totals[2])
    )
    holdout_evidence_valid = bool(
        holdout_evaluation_attempted
        and holdout_evaluation_error is None
        and isinstance(skipped, bool)
        and not skipped
        and holdout_net is not None
        and trade_count_valid
        and trade_count_consistent
        and holdout_trade_accounting_valid
        and holdout_aggregates_consistent
        and holdout_sample_consistent
        and holdout_outcomes_consistent
        and holdout_net_consistent
    )
    if (
        holdout_evaluation_attempted
        and holdout_evaluation_error is None
        and not holdout_evidence_valid
    ):
        if not holdout_trade_accounting_valid:
            holdout_evaluation_error = (
                "InvalidEvidence: inconsistent per-trade holdout accounting"
            )
        elif not holdout_aggregates_consistent:
            holdout_evaluation_error = (
                "InvalidEvidence: inconsistent aggregate holdout accounting"
            )
        elif not holdout_sample_consistent:
            holdout_evaluation_error = (
                "InvalidEvidence: inconsistent holdout full-trade sample"
            )
        elif not holdout_outcomes_consistent:
            holdout_evaluation_error = (
                "InvalidEvidence: inconsistent holdout outcome summary"
            )
        else:
            holdout_evaluation_error = (
                "InvalidEvidence: incomplete holdout trade evidence"
            )
    cost_stress_pass = bool(
        holdout_evidence_valid and _holdout_cost_stress_pass(holdout)
    )
    final_pass = bool(
        skipped is False
        and holdout_evidence_valid
        and holdout_net is not None
        and holdout_net > 0.0
        and trade_count_valid
        and trade_count_consistent
        and holdout_net_consistent
        and holdout_full_trade_count is not None
        and holdout_full_trade_count >= min_trades
        and cost_stress_pass
        and winner.get("kfold", {}).get("robust") is True
        and deep_validation_complete
    )
    winner["holdout"] = holdout
    winner["holdout_evaluation_attempted"] = holdout_evaluation_attempted
    winner["holdout_evaluation_error"] = holdout_evaluation_error
    winner["holdout_evidence_valid"] = holdout_evidence_valid
    winner["holdout_trade_accounting_valid"] = holdout_trade_accounting_valid
    winner["holdout_aggregates_consistent"] = holdout_aggregates_consistent
    winner["holdout_sample_consistent"] = holdout_sample_consistent
    winner["holdout_outcomes_consistent"] = holdout_outcomes_consistent
    winner["holdout_net"] = holdout_net
    winner["holdout_net_consistent"] = holdout_net_consistent
    winner["holdout_trades"] = trades
    winner["holdout_full_trades"] = holdout_full_trade_count or 0
    winner["cost_stress_pass"] = cost_stress_pass
    winner["final_holdout_pass"] = final_pass
    winner["deployment_validated"] = final_pass
    return winner


def build_pbo_block_matrix(
    results: list[dict], *, minimum_blocks: int = 8
) -> list[list[float]]:
    """Build aligned, time-ordered return blocks for CSCV/PBO.

    Aggregating each fold to one number leaves only six CSCV partitions with
    four folds. Split every fold's ordered trade returns into contiguous blocks
    and exclude candidates without enough observations rather than padding.
    """
    prepared = []
    for result in results:
        folds = result.get("kfold", {}).get("fold_results") or []
        if not folds:
            continue
        per_fold = max(1, math.ceil(minimum_blocks / len(folds)))
        blocks = []
        boundaries = []
        complete = True
        for fold in folds:
            start = _optimizer_time_number(fold.get("period_start_ts"))
            end = _optimizer_time_number(fold.get("period_end_ts"))
            positions = fold.get("closed_positions") or []
            if (
                start is None
                or end is None
                or end <= start
                or not isinstance(positions, list)
                or len(positions) < per_fold
            ):
                complete = False
                break
            fold_blocks = [0.0] * per_fold
            for position in positions:
                if not isinstance(position, dict):
                    complete = False
                    break
                exit_time = _optimizer_time_number(position.get("exit_time"))
                try:
                    if isinstance(position.get("net"), bool):
                        raise TypeError
                    net = float(position["net"])
                except (KeyError, TypeError, ValueError, OverflowError):
                    complete = False
                    break
                if (
                    exit_time is None
                    or exit_time < start
                    or exit_time > end
                    or not math.isfinite(net)
                ):
                    complete = False
                    break
                if exit_time >= end:
                    block_index = per_fold - 1
                else:
                    block_index = int(
                        (exit_time - start) * per_fold / (end - start)
                    )
                fold_blocks[block_index] += net
            if not complete:
                break
            boundaries.append((start, end))
            blocks.extend(fold_blocks)
        if complete and len(blocks) >= minimum_blocks:
            prepared.append((tuple(boundaries), blocks))
    if not prepared:
        return []
    reference_boundaries = prepared[0][0]
    if any(boundaries != reference_boundaries for boundaries, _ in prepared[1:]):
        return []
    return [blocks for _, blocks in prepared]


#  Sensitivitts-Analyse


def sensitivity_check(
    indexed: dict,
    all_times: list,
    base_params: dict,
    strategy: str,
    use_maker: bool,
    search_space: dict,
) -> dict:
    """
    Variiert jeden Parameter um 1 Schritt im Suchraum.
    Berechnet wie stark sich Netto-PnL dabei ndert.

    Niedrige Standardabweichung = robust
    Hohe Standardabweichung     = fragil (overfit)
    """
    base_s = simulate_fast(indexed, all_times, strategy, use_maker, base_params)
    base_net_value = _finite_optimizer_net(base_s)
    base_net_valid = base_net_value is not None
    base_net = base_net_value if base_net_valid else 0.0

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
            new_params = dict(base_params)
            new_params[param] = values[new_idx]
            s = simulate_fast(indexed, all_times, strategy, use_maker, new_params)
            new_net_value = _finite_optimizer_net(s)
            new_net_valid = new_net_value is not None
            new_net = new_net_value if new_net_valid else 0.0
            change_pct = None
            if base_net_valid and new_net_valid:
                try:
                    change = (
                        ((new_net - base_net) / abs(base_net) * 100)
                        if base_net != 0
                        else 0.0
                    )
                except ArithmeticError:
                    change = float("nan")
                if math.isfinite(change):
                    change_pct = change
            perturbations.append(
                {
                    "param": param,
                    "from": base_val,
                    "to": values[new_idx],
                    "delta": delta,
                    "new_net": new_net,
                    "change_pct": change_pct,
                    "evidence_valid": change_pct is not None,
                }
            )

    if not perturbations:
        return {
            "base_net": base_net,
            "base_net_valid": base_net_valid,
            "perturbations": [],
            "valid_perturbation_count": 0,
            "invalid_perturbation_count": 0,
            "max_change": 0,
            "avg_change": 0,
            "robust": False,
            "rating": "" if base_net_valid else " INSUFFICIENT EVIDENCE",
        }

    changes = [
        abs(p["change_pct"])
        for p in perturbations
        if p["change_pct"] is not None
    ]
    valid_count = len(changes)
    invalid_count = len(perturbations) - valid_count
    try:
        max_change = max(changes) if changes else 0.0
        avg_change = statistics.mean(changes) if changes else 0.0
        summary_valid = math.isfinite(max_change) and math.isfinite(avg_change)
    except (ArithmeticError, AttributeError, TypeError, ValueError):
        max_change = 0.0
        avg_change = 0.0
        summary_valid = False
    evidence_complete = (
        base_net_valid
        and invalid_count == 0
        and valid_count == len(perturbations)
        and summary_valid
    )

    # Bewertung:
    #  < 15% durchschnittliche nderung  ROBUST
    #  < 30%  DURCHSCHNITTLICH
    #  30%  FRAGIL (overfit)
    if not evidence_complete:
        rating = " INSUFFICIENT EVIDENCE"
    elif avg_change < 15:
        rating = " ROBUST"
    elif avg_change < 30:
        rating = " DURCHSCHNITTLICH"
    else:
        rating = " FRAGIL"

    return {
        "base_net": base_net,
        "base_net_valid": base_net_valid,
        "perturbations": perturbations,
        "valid_perturbation_count": valid_count,
        "invalid_perturbation_count": invalid_count,
        "max_change": max_change,
        "avg_change": avg_change,
        "rating": rating,
        "robust": evidence_complete and avg_change < 15,
    }


#  Robustness Score


def robustness_score(
    s_full: dict,
    kfold: dict,
    walk_forward: dict = None,
    regime_split: dict = None,
    outlier_test: dict = None,
    monte_carlo: dict = None,
) -> float:
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
    invalid_score = float("-inf")
    if not isinstance(s_full, dict) or not isinstance(kfold, dict):
        return invalid_score
    for evidence_name, evidence in (
        ("walk_forward", walk_forward),
        ("regime_split", regime_split),
        ("outlier_test", outlier_test),
        ("monte_carlo", monte_carlo),
    ):
        if evidence is not None and (
            not isinstance(evidence, dict)
            or _deep_validation_evidence_error(evidence_name, evidence) is not None
        ):
            return invalid_score
    robust = kfold.get("robust")
    if not isinstance(robust, bool):
        return invalid_score
    if not robust:
        # Configs die nicht in allen Folds profitabel sind: starkes Penalty
        net = _finite_optimizer_net(s_full)
        if net is None:
            return invalid_score
        score = net * 0.1
        return score if math.isfinite(score) else invalid_score

    avg_net = _finite_optimizer_number(kfold.get("avg_net"))
    consist = _finite_optimizer_number(kfold.get("consistency"))
    sharpe_value = _finite_optimizer_number(s_full.get("sharpe"))
    dd_value = _finite_optimizer_number(s_full.get("max_dd"))
    if None in (avg_net, consist, sharpe_value, dd_value):
        return invalid_score
    if not 0.0 <= consist <= 1.0:
        return invalid_score
    sharpe = max(0.0, sharpe_value)
    dd = max(0.1, dd_value)

    score = avg_net * consist * (1 + sharpe) * (1 - dd / 100)

    # Walk-forward modifier
    if walk_forward is not None:
        if not isinstance(walk_forward, dict):
            return invalid_score
        all_profitable = walk_forward.get("all_profitable")
        profitable_count = walk_forward.get("profitable_count")
        if (
            not isinstance(all_profitable, bool)
            or isinstance(profitable_count, bool)
            or not isinstance(profitable_count, int)
            or profitable_count < 0
        ):
            return invalid_score
        if all_profitable:
            score *= 1.2
        elif profitable_count <= 1:
            score *= 0.5  # only 1 of N forward slices profitable = serious red flag

    # Regime survival modifier
    if regime_split is not None:
        if not isinstance(regime_split, dict):
            return invalid_score
        survives_all = regime_split.get("survives_all")
        profitable_count = regime_split.get("profitable_count")
        if (
            not isinstance(survives_all, bool)
            or isinstance(profitable_count, bool)
            or not isinstance(profitable_count, int)
            or profitable_count < 0
        ):
            return invalid_score
        if survives_all:
            score *= 1.3
        elif profitable_count == 0:
            score *= 0.3  # profitable in zero regimes = lucky aggregate only

    # Outlier dependency penalty
    if outlier_test is not None:
        if (
            not isinstance(outlier_test, dict)
            or not isinstance(outlier_test.get("outlier_fragile"), bool)
            or not isinstance(outlier_test.get("symbol_fragile"), bool)
        ):
            return invalid_score
        if outlier_test["outlier_fragile"]:
            score *= 0.5
        if outlier_test["symbol_fragile"]:
            score *= 0.5

    # Monte-Carlo modifier
    if monte_carlo is not None:
        if not isinstance(monte_carlo, dict):
            return invalid_score
        share = _finite_optimizer_number(monte_carlo.get("positive_share"))
        if share is None or not 0.0 <= share <= 1.0:
            return invalid_score
        if share >= 0.90:
            score *= 1.1
        elif share < 0.70:
            score *= 0.7

    return score if math.isfinite(score) else invalid_score


_DEEP_VALIDATION_CHECKS = (
    "walk_forward",
    "regime_split",
    "outlier_test",
    "monte_carlo",
)


def _deep_validation_evidence_error(name: str, result: dict) -> str | None:
    def _finite_number(value) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return number if math.isfinite(number) else None

    if name == "walk_forward":
        slices = result.get("slice_nets")
        invalid_count = result.get("invalid_slice_count")
        valid_count = result.get("valid_slice_count")
        profitable_count = result.get("profitable_count")
        edge_count = result.get("edge_count")
        summary_valid = result.get("summary_valid")
        all_profitable = result.get("all_profitable")
        if (
            not isinstance(slices, (list, tuple))
            or len(slices) < 2
            or any(_finite_number(value) is None for value in slices)
            or isinstance(invalid_count, bool)
            or not isinstance(invalid_count, int)
            or invalid_count != 0
            or isinstance(valid_count, bool)
            or not isinstance(valid_count, int)
            or valid_count != len(slices)
            or isinstance(profitable_count, bool)
            or not isinstance(profitable_count, int)
            or not 0 <= profitable_count <= valid_count
            or isinstance(edge_count, bool)
            or not isinstance(edge_count, int)
            or not 0 <= edge_count <= valid_count
            or summary_valid is not True
            or not isinstance(all_profitable, bool)
            or all_profitable
            != (
                profitable_count == len(slices)
                and edge_count == len(slices)
            )
        ):
            return "walk-forward requires complete finite net and edge evidence"
    elif name == "regime_split":
        tested = result.get("tested_count")
        profitable = result.get("profitable_count")
        edge_count = result.get("edge_count")
        valid_count = result.get("valid_regime_count")
        invalid_count = result.get("invalid_regime_count")
        invalid_source_count = result.get("invalid_source_sample_count")
        survives_all = result.get("survives_all")
        if (
            isinstance(tested, bool)
            or not isinstance(tested, int)
            or tested < 2
            or isinstance(profitable, bool)
            or not isinstance(profitable, int)
            or not 0 <= profitable <= tested
            or isinstance(edge_count, bool)
            or not isinstance(edge_count, int)
            or not profitable <= edge_count <= tested
            or isinstance(valid_count, bool)
            or not isinstance(valid_count, int)
            or valid_count != tested
            or isinstance(invalid_count, bool)
            or not isinstance(invalid_count, int)
            or invalid_count != 0
            or isinstance(invalid_source_count, bool)
            or not isinstance(invalid_source_count, int)
            or invalid_source_count != 0
            or result.get("evidence_valid") is not True
            or not isinstance(survives_all, bool)
            or survives_all != (profitable == tested)
        ):
            return "regime split requires complete finite regime evidence"
    elif name == "outlier_test":
        trade_count = result.get("trade_count")
        valid_count = result.get("valid_trade_count")
        invalid_count = result.get("invalid_trade_count")
        full_net = _finite_number(result.get("full_net"))
        scenarios = result.get("scenarios")
        top_one = scenarios.get("remove_top_1") if isinstance(scenarios, dict) else None
        removed_net = (
            _finite_number(top_one.get("removed_net"))
            if isinstance(top_one, dict)
            else None
        )
        adjusted_net = (
            _finite_number(top_one.get("adjusted_net"))
            if isinstance(top_one, dict)
            else None
        )
        drop_pct = (
            _finite_number(top_one.get("drop_pct"))
            if isinstance(top_one, dict)
            else None
        )
        still_positive = (
            top_one.get("still_positive") if isinstance(top_one, dict) else None
        )
        outlier_fragile = result.get("outlier_fragile")
        symbol_nets = result.get("symbol_nets")
        symbol_count = result.get("symbol_count")
        positive_symbol_net = _finite_number(result.get("positive_symbol_net"))
        dominant_symbol = result.get("dominant_symbol")
        dominant_symbol_net = _finite_number(result.get("dominant_symbol_net"))
        dominant_share = _finite_number(result.get("dominant_positive_share"))
        remove_symbol = result.get("remove_dominant_symbol")
        symbol_adjusted_net = (
            _finite_number(remove_symbol.get("adjusted_net"))
            if isinstance(remove_symbol, dict)
            else None
        )
        symbol_still_positive = (
            remove_symbol.get("still_positive")
            if isinstance(remove_symbol, dict)
            else None
        )
        symbol_fragile = result.get("symbol_fragile")
        scenario_consistent = False
        if None not in (full_net, removed_net, adjusted_net, drop_pct):
            try:
                expected_adjusted = full_net - removed_net
                expected_drop_pct = (
                    removed_net / abs(full_net) * 100 if full_net else 0.0
                )
                scenario_consistent = (
                    math.isfinite(expected_adjusted)
                    and math.isfinite(expected_drop_pct)
                    and math.isclose(
                        adjusted_net,
                        expected_adjusted,
                        rel_tol=1e-9,
                        abs_tol=1e-9,
                    )
                    and math.isclose(
                        drop_pct,
                        expected_drop_pct,
                        rel_tol=1e-9,
                        abs_tol=1e-9,
                    )
                    and still_positive == (adjusted_net > 0)
                )
            except (ArithmeticError, TypeError, ValueError, ZeroDivisionError):
                scenario_consistent = False
        symbol_scenario_consistent = False
        if (
            isinstance(symbol_nets, dict)
            and symbol_nets
            and all(
                isinstance(symbol, str)
                and bool(symbol.strip())
                and symbol == symbol.strip()
                and _finite_number(value) is not None
                for symbol, value in symbol_nets.items()
            )
            and isinstance(dominant_symbol, str)
            and dominant_symbol in symbol_nets
            and None not in (
                full_net,
                positive_symbol_net,
                dominant_symbol_net,
                dominant_share,
                symbol_adjusted_net,
            )
        ):
            try:
                normalized_symbol_nets = {
                    symbol: float(value) for symbol, value in symbol_nets.items()
                }
                expected_dominant = max(
                    normalized_symbol_nets.items(),
                    key=lambda item: (item[1], item[0]),
                )
                expected_positive = math.fsum(
                    max(value, 0.0) for value in normalized_symbol_nets.values()
                )
                expected_symbol_adjusted = full_net - dominant_symbol_net
                expected_share = dominant_symbol_net / expected_positive
                expected_symbol_fragile = bool(
                    len(normalized_symbol_nets) < 2
                    or expected_share > MAX_DOMINANT_SYMBOL_POSITIVE_SHARE
                    or expected_symbol_adjusted <= 0.0
                )
                symbol_scenario_consistent = (
                    expected_positive > 0.0
                    and math.isclose(
                        math.fsum(normalized_symbol_nets.values()),
                        full_net,
                        rel_tol=1e-9,
                        abs_tol=1e-9,
                    )
                    and dominant_symbol == expected_dominant[0]
                    and math.isclose(
                        dominant_symbol_net,
                        expected_dominant[1],
                        rel_tol=1e-9,
                        abs_tol=1e-9,
                    )
                    and math.isclose(
                        positive_symbol_net,
                        expected_positive,
                        rel_tol=1e-9,
                        abs_tol=1e-9,
                    )
                    and math.isclose(
                        dominant_share,
                        expected_share,
                        rel_tol=1e-9,
                        abs_tol=1e-9,
                    )
                    and math.isclose(
                        symbol_adjusted_net,
                        expected_symbol_adjusted,
                        rel_tol=1e-9,
                        abs_tol=1e-9,
                    )
                    and symbol_still_positive == (symbol_adjusted_net > 0.0)
                    and symbol_fragile == expected_symbol_fragile
                )
            except (ArithmeticError, TypeError, ValueError, ZeroDivisionError):
                symbol_scenario_consistent = False
        if (
            isinstance(trade_count, bool)
            or not isinstance(trade_count, int)
            or trade_count < 6
            or isinstance(valid_count, bool)
            or not isinstance(valid_count, int)
            or valid_count != trade_count
            or isinstance(invalid_count, bool)
            or not isinstance(invalid_count, int)
            or invalid_count != 0
            or full_net is None
            or result.get("full_net_consistent") is not True
            or result.get("evidence_valid") is not True
            or removed_net is None
            or adjusted_net is None
            or drop_pct is None
            or not isinstance(still_positive, bool)
            or not scenario_consistent
            or not isinstance(outlier_fragile, bool)
            or outlier_fragile != (still_positive is False)
            or isinstance(symbol_count, bool)
            or not isinstance(symbol_count, int)
            or not isinstance(symbol_nets, dict)
            or symbol_count != len(symbol_nets)
            or result.get("symbol_net_consistent") is not True
            or positive_symbol_net is None
            or dominant_symbol_net is None
            or dominant_share is None
            or not 0.0 <= dominant_share <= 1.0
            or symbol_adjusted_net is None
            or not isinstance(symbol_still_positive, bool)
            or not isinstance(symbol_fragile, bool)
            or not symbol_scenario_consistent
        ):
            return (
                "outlier test requires complete finite trade and symbol evidence"
            )
    elif name == "monte_carlo":
        runs = result.get("runs")
        requested_runs = result.get("requested_runs")
        positive_run_count = result.get("positive_run_count")
        share = _finite_number(result.get("positive_share"))
        trade_count = result.get("trade_count")
        valid_trade_count = result.get("valid_trade_count")
        invalid_trade_count = result.get("invalid_trade_count")
        median_final = _finite_number(result.get("median_final"))
        worst_decile = _finite_number(result.get("worst_decile"))
        robust = result.get("robust")
        concerning = result.get("concerning")
        if (
            isinstance(requested_runs, bool)
            or not isinstance(requested_runs, int)
            or requested_runs <= 0
            or isinstance(runs, bool)
            or not isinstance(runs, int)
            or runs <= 0
            or runs != requested_runs
            or isinstance(positive_run_count, bool)
            or not isinstance(positive_run_count, int)
            or not 0 <= positive_run_count <= runs
            or share is None
            or not 0.0 <= share <= 1.0
            or not math.isclose(
                share,
                positive_run_count / runs,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            or isinstance(trade_count, bool)
            or not isinstance(trade_count, int)
            or trade_count < 5
            or isinstance(valid_trade_count, bool)
            or not isinstance(valid_trade_count, int)
            or valid_trade_count != trade_count
            or isinstance(invalid_trade_count, bool)
            or not isinstance(invalid_trade_count, int)
            or invalid_trade_count != 0
            or median_final is None
            or worst_decile is None
            or worst_decile > median_final
            or not isinstance(robust, bool)
            or robust != (share >= 0.90)
            or not isinstance(concerning, bool)
            or concerning != (share < 0.70)
            or result.get("method") != "block_bootstrap"
            or result.get("evidence_valid") is not True
        ):
            return "monte-carlo requires complete finite bootstrap evidence"
    return None


def _deep_validation_admission_error(candidate: dict) -> str | None:
    """Require explicit, internally valid deep evidence before holdout access."""
    kfold = candidate.get("kfold")
    if not isinstance(kfold, dict) or kfold.get("robust") is not True:
        return "kfold.robust is not explicitly true"
    if candidate.get("deep_validation_complete") is not True:
        return "deep_validation_complete is not explicitly true"
    errors = candidate.get("deep_validation_errors")
    if not isinstance(errors, dict) or errors:
        return "deep_validation_errors must be an empty mapping"
    for name in _DEEP_VALIDATION_CHECKS:
        evidence = candidate.get(name)
        if not isinstance(evidence, dict):
            return f"{name} evidence is missing"
        evidence_error = _deep_validation_evidence_error(name, evidence)
        if evidence_error is not None:
            return evidence_error
    if candidate["walk_forward"].get("all_profitable") is not True:
        return "walk-forward did not survive every slice"
    if candidate["regime_split"].get("survives_all") is not True:
        return "regime split did not survive every tested regime"
    if candidate["outlier_test"].get("outlier_fragile") is not False:
        return "performance depends on a single position outlier"
    if candidate["outlier_test"].get("symbol_fragile") is not False:
        return "performance is concentrated in one symbol"
    if candidate["monte_carlo"].get("robust") is not True:
        return "monte-carlo robustness threshold was not met"
    expected_score = robustness_score(
        candidate.get("stats"),
        kfold,
        walk_forward=candidate.get("walk_forward"),
        regime_split=candidate.get("regime_split"),
        outlier_test=candidate.get("outlier_test"),
        monte_carlo=candidate.get("monte_carlo"),
    )
    actual_score = _finite_optimizer_number(candidate.get("score"))
    if actual_score is None or not math.isfinite(expected_score) or not math.isclose(
        actual_score, expected_score, rel_tol=1e-9, abs_tol=1e-9
    ):
        return "deep validation score is inconsistent"
    return None


def evaluate_deep_validation_candidate(
    candidate: dict, validators: dict
) -> dict:
    """Run all deep checks and make incomplete evidence explicitly fail closed."""
    errors = {}
    for name in _DEEP_VALIDATION_CHECKS:
        validator = validators.get(name)
        try:
            if not callable(validator):
                raise TypeError("validator is unavailable")
            result = validator()
            if not isinstance(result, dict):
                raise TypeError("validator result is not a mapping")
            evidence_error = _deep_validation_evidence_error(name, result)
            if evidence_error is not None:
                errors[name] = f"InsufficientEvidence: {evidence_error}"
        except Exception as exc:
            result = None
            errors[name] = f"{type(exc).__name__}: {str(exc)[:160]}"
        candidate[name] = result

    score = None
    if not errors:
        try:
            score = _finite_optimizer_number(robustness_score(
                candidate["stats"],
                candidate["kfold"],
                walk_forward=candidate["walk_forward"],
                regime_split=candidate["regime_split"],
                outlier_test=candidate["outlier_test"],
                monte_carlo=candidate["monte_carlo"],
            ))
        except Exception as exc:
            try:
                detail = str(exc)[:160]
            except Exception:
                detail = "<unprintable exception>"
            errors["robustness_score"] = f"{type(exc).__name__}: {detail}"
        if score is None and "robustness_score" not in errors:
            errors["robustness_score"] = (
                "InsufficientEvidence: score is not a finite number"
            )
    candidate["deep_validation_errors"] = errors
    candidate["deep_validation_complete"] = not errors
    candidate["score"] = score if score is not None else float("-inf")
    return candidate


#  Hilfsfunktionen


def _label(p, strategy):
    pump_label = "move" if strategy == "FUTURES" else "pump"
    s = (
        f"{pump_label}={p['min_pump']:.0f}%  TP={p['activation_profit']:.1f}%  "
        f"trail={p['trailing_distance']:.1f}%  partial={p['partial_pct']:.0%}  "
        f"rsi{p['rsi_max']:.0f}"
    )
    if strategy in ("TREND", "FUTURES"):
        s += f"  stop={p.get('stop_loss', -2):.1f}%"
    return s


def _cmd(p, strategy, days, use_maker):
    c = f"python backtester.py {strategy} {days}"
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


@contextmanager
def _atomic_csv_writer(filename: str):
    temp_filename = f"{filename}.{uuid.uuid4().hex}.tmp"
    temp_created = False
    try:
        with open(temp_filename, "x", newline="", encoding="utf-8") as handle:
            temp_created = True
            yield handle
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_filename, filename)
        temp_created = False
    finally:
        if temp_created:
            try:
                os.remove(temp_filename)
            except OSError:
                pass


def export_csv(results, strategy, days, k_folds):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    # optimizer_results/ lives at PROJECT ROOT (read by the launcher UI), not
    # inside tools/.
    try:
        from core.paths import OPT_RESULTS

        results_dir = str(OPT_RESULTS)
    except Exception:
        results_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "optimizer_results",
        )
    os.makedirs(results_dir, exist_ok=True)

    filename = os.path.join(
        results_dir,
        f"optimizer_{strategy.lower()}_{days}d_{ts}_p{os.getpid()}_"
        f"{uuid.uuid4().hex[:12]}.csv",
    )
    fields = [
        "rank",
        "robust",
        "deep_validation_complete",
        "deep_validation_admission_error",
        "symbol_concentration_pass",
        "dominant_symbol",
        "dominant_positive_share",
        "deployment_validated",
        "holdout_evaluation_attempted",
        "holdout_evidence_valid",
        "holdout_trade_accounting_valid",
        "holdout_aggregates_consistent",
        "holdout_sample_consistent",
        "holdout_outcomes_consistent",
        "holdout_full_trades",
        "holdout_evaluation_error",
        "holdout_net",
        "avg_net",
        "std_net",
        "consistency",
        "score",
        "fold_profitable_count",
        "full_net",
        "full_roi",
        "win_rate",
        "sharpe",
        "max_dd",
        "cost_pct",
        "trades",
        "min_pump",
        "activation_profit",
        "trailing_distance",
        "stop_loss",
        "partial_pct",
        "rsi_max",
        "fold_nets",
        "cli_command",
    ]
    with _atomic_csv_writer(filename) as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for i, r in enumerate(results):
            s = r["stats"]
            p = r["params"]
            kf = r["kfold"]
            outlier = r.get("outlier_test") or {}
            holdout_net = _finite_holdout_number(r.get("holdout_net"))
            w.writerow(
                {
                    "rank": i + 1,
                    "robust": "Ja" if kf["robust"] else "Nein",
                    "deep_validation_complete": (
                        "Ja" if r.get("deep_validation_complete") is True
                        else "Nein"
                    ),
                    "deep_validation_admission_error": (
                        r.get("deep_validation_admission_error") or ""
                    ),
                    "symbol_concentration_pass": (
                        "Ja" if outlier.get("symbol_fragile") is False else "Nein"
                    ),
                    "dominant_symbol": outlier.get("dominant_symbol") or "",
                    "dominant_positive_share": (
                        outlier.get("dominant_positive_share")
                        if _finite_optimizer_number(
                            outlier.get("dominant_positive_share")
                        )
                        is not None
                        else ""
                    ),
                    "deployment_validated": "Ja"
                    if r.get("deployment_validated")
                    else "Nein",
                    "holdout_evaluation_attempted": (
                        "Ja" if r.get("holdout_evaluation_attempted") is True
                        else "Nein"
                    ),
                    "holdout_evidence_valid": (
                        "Ja" if r.get("holdout_evidence_valid") is True else "Nein"
                    ),
                    "holdout_trade_accounting_valid": (
                        "Ja" if r.get("holdout_trade_accounting_valid") is True
                        else "Nein"
                    ),
                    "holdout_aggregates_consistent": (
                        "Ja" if r.get("holdout_aggregates_consistent") is True
                        else "Nein"
                    ),
                    "holdout_sample_consistent": (
                        "Ja" if r.get("holdout_sample_consistent") is True else "Nein"
                    ),
                    "holdout_outcomes_consistent": (
                        "Ja"
                        if r.get("holdout_outcomes_consistent") is True
                        else "Nein"
                    ),
                    "holdout_full_trades": int(
                        r.get("holdout_full_trades", 0) or 0
                    ),
                    "holdout_evaluation_error": (
                        r.get("holdout_evaluation_error") or ""
                    ),
                    "holdout_net": (
                        round(holdout_net, 2)
                        if holdout_net is not None
                        else ""
                    ),
                    "avg_net": round(kf["avg_net"], 2),
                    "std_net": round(kf["std_net"], 2),
                    "consistency": round(kf["consistency"], 3),
                    "score": round(r["score"], 4),
                    "fold_profitable_count": f"{kf['profit_count']}/{k_folds}",
                    "full_net": round(s.get("net", 0), 2),
                    "full_roi": round(s.get("roi", 0), 2),
                    "win_rate": round(s.get("win_rate", 0), 3),
                    "sharpe": round(s.get("sharpe", 0), 3),
                    "max_dd": round(s.get("max_dd", 0), 1),
                    "cost_pct": round(s.get("cost_pct", 0), 1),
                    "trades": s.get("trades", 0),
                    "min_pump": p.get("min_pump"),
                    "activation_profit": p.get("activation_profit"),
                    "trailing_distance": p.get("trailing_distance"),
                    "stop_loss": p.get("stop_loss", ""),
                    "partial_pct": p.get("partial_pct"),
                    "rsi_max": p.get("rsi_max"),
                    "fold_nets": ";".join(f"{n:.2f}" for n in kf["fold_nets"]),
                    "cli_command": r.get("cmd", ""),
                }
            )

    # Only prune old results after the new CSV is durably published.
    try:
        prefix = f"optimizer_{strategy.lower()}_"
        current_name = os.path.basename(filename)
        previous = sorted(
            [
                name
                for name in os.listdir(results_dir)
                if name.startswith(prefix) and name.endswith(".csv")
                and name != current_name
            ],
            reverse=True,
        )
        for old in previous[19:]:
            try:
                os.remove(os.path.join(results_dir, old))
            except OSError:
                pass
    except OSError:
        pass
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
        self.done = 0
        self.start = time.time()
        self.best = None
        self.label = label
        # Emit ticks at every 1% OR every 5 seconds  whichever comes first
        self._last_emit = 0.0
        self._last_pct = -1
        self._lock = threading.Lock()

    def update(self, net):
        with self._lock:
            self._update_locked(net)

    def _update_locked(self, net):
        self.done += 1
        if self.best is None or net > self.best:
            self.best = net
        elapsed = time.time() - self.start
        eta = elapsed / self.done * (self.total - self.done) if self.done else 0
        pct = self.done / self.total
        bar = "" * int(pct * 26) + "" * (26 - int(pct * 26))
        best_s = f"+{self.best:.2f}" if self.best else ""

        # Terminal-friendly live bar (stderr  bypasses the launcher's line reader)
        sys.stderr.write(
            f"\r  {self.label} [{bar}] {self.done}/{self.total} ({pct:.0%}) "
            f"ETA {int(eta)}s Best: {best_s}    "
        )
        sys.stderr.flush()

        # Launcher-parseable markers  emit on each 1% step OR every 5s
        now = time.time()
        pct_int = int(pct * 100)
        emit_now = (
            pct_int != self._last_pct
            or (now - self._last_emit) >= 5.0
            or self.done == self.total
        )
        if emit_now:
            payload = _json.dumps(
                {
                    "label": self.label,
                    "done": self.done,
                    "total": self.total,
                    "pct": round(pct, 4),
                    "best": round(self.best, 4) if self.best is not None else None,
                    "eta_s": int(eta),
                    "elapsed_s": int(elapsed),
                }
            )
            print(f"<<<PROGRESS>>>{payload}<<<END>>>", flush=True)
            self._last_emit = now
            self._last_pct = pct_int


#  Haupt-Optimizer


def _bounded_optimizer_number(value, low: float, high: float) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return max(low, min(high, number))


def _validate_optimizer_run_inputs(
    strategy,
    days,
    top_n,
    k_folds,
    holdout_frac,
    om_window,
    funding_8h,
    leverage=None,
) -> None:
    if strategy not in FULL_SPACE or strategy not in QUICK_SPACE:
        raise ValueError(f"unsupported optimizer strategy {strategy!r}")
    for name, value, minimum in (
        ("days", days, 1),
        ("top_n", top_n, 1),
        ("k_folds", k_folds, 2),
        ("om_window", om_window, 1),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}")
    for name, value in (
        ("holdout_frac", holdout_frac),
        ("funding_8h", funding_8h),
    ):
        if isinstance(value, bool):
            raise ValueError(f"{name} must be finite")
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be finite") from exc
        if not math.isfinite(number):
            raise ValueError(f"{name} must be finite")
        if name == "holdout_frac" and not 0.0 <= number < 0.9:
            raise ValueError("holdout_frac must be >= 0 and < 0.9")
    if leverage is not None:
        if isinstance(leverage, bool):
            raise ValueError("leverage must be finite and between 1 and 25")
        try:
            leverage_number = float(leverage)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("leverage must be finite and between 1 and 25") from exc
        if not math.isfinite(leverage_number) or not 1.0 <= leverage_number <= 25.0:
            raise ValueError("leverage must be finite and between 1 and 25")


def _resolve_leverage(
    strategy: str,
    override: float = None,
    live_config: dict | None = None,
) -> float:
    """Determine the leverage the backtest should use.

    Priority: explicit CLI/arg override > the strategy's LIVE LEVERAGE in
    bot_config.json > the backtester's STRATEGY_DEFAULTS fallback.

    Reading the live config keeps the backtest honest: the modelled leverage
    matches what the bot actually trades, so PnL, stop-in-margin and
    liquidation/tail risk reflect the real position.
    """
    if override is not None:
        resolved = _bounded_optimizer_number(override, 1.0, 25.0)
        if resolved is not None:
            return resolved
    section = (
        _load_live_strategy_config(strategy)
        if live_config is None
        else live_config
    )
    if isinstance(section, dict):
        resolved = _bounded_optimizer_number(section.get("LEVERAGE"), 1.0, 25.0)
        if resolved is not None:
            return resolved
    try:
        from tools.backtester import STRATEGY_DEFAULTS

        resolved = _bounded_optimizer_number(
            STRATEGY_DEFAULTS.get(strategy, {}).get("leverage", 1.0),
            1.0,
            25.0,
        )
        return resolved if resolved is not None else 1.0
    except Exception:
        return 1.0


def _load_live_strategy_config(strategy: str) -> dict:
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "bot_config.json"), "rb") as fh:
            raw = fh.read(OPTIMIZER_CONFIG_MAX_BYTES + 1)
        if len(raw) > OPTIMIZER_CONFIG_MAX_BYTES:
            raise ValueError("optimizer config JSON exceeds size limit")
        cfg = _json.loads(raw.decode("utf-8-sig"))
        if not isinstance(cfg, dict):
            return {}
        section = cfg.get(strategy, {})
        return section if isinstance(section, dict) else {}
    except Exception:
        return {}


def _resolve_live_risk_params(
    strategy: str, live_config: dict | None = None
) -> dict:
    section = (
        _load_live_strategy_config(strategy)
        if live_config is None
        else live_config
    )
    if not isinstance(section, dict):
        section = {}
    mapping = {
        "MAX_OPEN_TRADES": ("max_open_trades", 1.0, 50.0, True),
        "MAX_NEW_TRADES_PER_TICK": ("top_n_per_scan", 0.0, 50.0, True),
    }
    out = {}
    position_limit = _OPTIMIZER_POSITION_LIMITS.get(str(strategy).upper(), 500.0)
    position_cap = None
    if "POSITION_SIZE_MAX" in section:
        position_cap = _bounded_optimizer_number(
            section["POSITION_SIZE_MAX"], 0.0, position_limit
        )
        if position_cap is not None:
            out["position_size_max"] = position_cap
    if "POSITION_SIZE" in section:
        position_size = _bounded_optimizer_number(
            section["POSITION_SIZE"],
            0.0,
            position_cap if position_cap is not None else position_limit,
        )
        if position_size is not None:
            out["position_size"] = position_size
    for src, (dst, low, high, integer) in mapping.items():
        if src not in section:
            continue
        val = _bounded_optimizer_number(section[src], low, high)
        if val is None:
            continue
        out[dst] = int(val) if integer else val
    return out


def _validate_reproducible_run_inputs(
    *,
    workspace,
    dataset,
    resume,
    seed,
    workers,
    backend,
    exchange,
) -> None:
    if workspace is not None and (not isinstance(workspace, str) or not workspace.strip()):
        raise ValueError("workspace must be a non-empty path")
    if dataset is not None and (not isinstance(dataset, str) or not dataset.strip()):
        raise ValueError("dataset must be a non-empty path")
    if not isinstance(resume, bool):
        raise ValueError("resume must be boolean")
    if resume and dataset is None:
        raise ValueError("resume requires an explicit immutable dataset")
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
        raise ValueError("seed must be an integer")
    if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 256:
        raise ValueError("workers must be an integer between 1 and 256")
    if backend not in {"process", "thread"}:
        raise ValueError("backend must be 'process' or 'thread'")
    if exchange is not None and (
        not isinstance(exchange, str) or not exchange.strip()
    ):
        raise ValueError("exchange must be a non-empty name")
    if (workspace is not None or dataset is not None) and exchange is None:
        raise ValueError(
            "reproducible optimizer runs require an explicit exchange"
        )


def _canonical_exchange_name(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("exchange identity is missing")
    normalized = "".join(char for char in value.strip().lower() if char.isalnum())
    aliases = {
        "mexcglobal": "mexc",
        "gate": "gateio",
        "kucoinfutures": "kucoin",
    }
    return aliases.get(normalized, normalized)


def _validate_dataset_exchange(dataset_manifest: dict, expected_exchange: str) -> str:
    if not isinstance(dataset_manifest, dict):
        raise ValueError("dataset manifest is invalid")
    provenance = dataset_manifest.get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError("dataset exchange provenance is missing")
    recorded = provenance.get("exchange_id") or provenance.get("exchange")
    actual = _canonical_exchange_name(recorded)
    expected = _canonical_exchange_name(expected_exchange)
    if actual != expected:
        raise ValueError(
            f"dataset exchange mismatch: expected {expected}, found {actual}"
        )
    return actual


def _history_cutoff_utc(history: dict) -> datetime:
    latest = None
    for frame in history.values():
        if frame is None or frame.empty or "ts" not in frame.columns:
            continue
        try:
            value = int(frame["ts"].iloc[-1])
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
        latest = value if latest is None else max(latest, value)
    if latest is None:
        raise ValueError("cannot derive immutable cutoff from empty history")
    cutoff_ms = ((latest // 3_600_000) + 1) * 3_600_000
    return datetime.fromtimestamp(cutoff_ms / 1000.0, tz=timezone.utc)


def _production_baseline_params(
    strategy: str,
    leverage: float,
    live_config: dict,
    live_risk_params: dict,
    *,
    own_momentum: bool,
    om_window: int,
    regime: bool,
    funding_8h: float,
) -> dict:
    from tools.backtester import STRATEGY_DEFAULTS

    defaults = STRATEGY_DEFAULTS[strategy]
    params = {
        "min_pump": defaults["pump"],
        "activation_profit": defaults["act"],
        "trailing_distance": defaults["trail"],
        "stop_loss": -abs(defaults["stop"]),
        "partial_pct": defaults["part"],
        "rsi_max": defaults["rsi"],
        "leverage": leverage,
    }
    supported = {
        "MIN_PUMP": ("min_pump", 0.0, 100.0),
        "ACTIVATION_PROFIT": ("activation_profit", 0.0, 100.0),
        "TRAILING_DISTANCE": ("trailing_distance", 0.0, 100.0),
        "PARTIAL_SELL_PCT": ("partial_pct", 0.0, 1.0),
        "RSI_MAX": ("rsi_max", 0.0, 100.0),
        "BREAKEVEN_TRIGGER": ("breakeven_trigger", 0.0, 100.0),
    }
    if isinstance(live_config, dict):
        for source, (target, low, high) in supported.items():
            if source not in live_config:
                continue
            value = _bounded_optimizer_number(live_config[source], low, high)
            if value is not None:
                params[target] = value
        if "INITIAL_STOP_LOSS" in live_config:
            stop = _bounded_optimizer_number(
                live_config["INITIAL_STOP_LOSS"], -100.0, 100.0
            )
            if stop is not None:
                params["stop_loss"] = -abs(stop)
        if live_config.get("OWN_MOMENTUM_FILTER") is True:
            params["own_momentum_filter"] = True
            window = _bounded_optimizer_number(
                live_config.get("OWN_MOMENTUM_WINDOW"), 1.0, 1_000.0
            )
            if window is not None:
                params["own_momentum_window"] = int(window)
    params.update(live_risk_params)
    if own_momentum:
        params["own_momentum_filter"] = True
        params["own_momentum_window"] = om_window
    if regime:
        params["regime_filter"] = True
    if funding_8h:
        params["funding_rate_8h"] = funding_8h
    return params


def _reproducible_code_files() -> list[str]:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return [
        os.path.join(root, "tools", "optimizer.py"),
        os.path.join(root, "tools", "backtester.py"),
        os.path.join(root, "tools", "simulation_workspace.py"),
        os.path.join(root, "bot_utils", "indicators.py"),
        os.path.join(root, "bot_utils", "futures_funding.py"),
        os.path.join(root, "core", "constants.py"),
        os.path.join(root, "trading", "simulation.py"),
    ]


_GRID_WORKER_CONTEXT = None


def _initialize_grid_worker(
    indexed, tune_times, folds, prepared_folds, strategy, use_maker
) -> None:
    global _GRID_WORKER_CONTEXT
    _GRID_WORKER_CONTEXT = (
        indexed,
        tune_times,
        folds,
        prepared_folds,
        strategy,
        use_maker,
    )


def _simulate_grid_candidate(task) -> dict:
    if _GRID_WORKER_CONTEXT is None:
        raise RuntimeError("optimizer grid worker was not initialized")
    candidate_index, params = task
    indexed, tune_times, folds, prepared_folds, strategy, use_maker = (
        _GRID_WORKER_CONTEXT
    )
    stats = simulate_fast(indexed, tune_times, strategy, use_maker, params)
    kfold = kfold_simulate(
        indexed,
        folds,
        strategy,
        use_maker,
        params,
        prepared_folds=prepared_folds,
    )
    return {
        "candidate_index": candidate_index,
        "params": params,
        "stats": stats,
        "kfold": kfold,
        "score": robustness_score(stats, kfold),
    }


def _simulate_grid_chunk(tasks) -> list[dict]:
    return [_simulate_grid_candidate(task) for task in tasks]


def run_optimizer(
    strategy: str,
    days: int = DEFAULT_DAYS,
    use_maker: bool = False,
    top_n: int = 10,
    k_folds: int = 4,
    quick: bool = False,
    do_sensitivity: bool = True,
    leverage: float = None,
    holdout_frac: float = 0.2,
    own_momentum: bool = False,
    om_window: int = 8,
    regime: bool = False,
    funding_8h: float = 0.0,
    workspace: str | None = None,
    dataset: str | None = None,
    resume: bool = False,
    seed: int | None = None,
    workers: int = 8,
    backend: str = "process",
    futures_screener_parity: bool = False,
    exchange: str | None = None,
):
    _validate_optimizer_run_inputs(
        strategy,
        days,
        top_n,
        k_folds,
        holdout_frac,
        om_window,
        funding_8h,
        leverage,
    )
    _validate_reproducible_run_inputs(
        workspace=workspace,
        dataset=dataset,
        resume=resume,
        seed=seed,
        workers=workers,
        backend=backend,
        exchange=exchange,
    )
    expected_exchange = (
        _canonical_exchange_name(exchange) if exchange is not None else None
    )
    if not isinstance(futures_screener_parity, bool):
        raise ValueError("futures_screener_parity must be boolean")
    if futures_screener_parity and strategy != "FUTURES":
        raise ValueError("futures screener parity is available only for FUTURES")
    run_seed = _OPTIMIZER_SEED if seed is None else seed
    effective_executor = (
        "sequential"
        if workers == 1
        else ("process_spawn" if backend == "process" else "thread_pool")
    )

    rt = calc_round_trip(use_maker, strategy)
    space = QUICK_SPACE[strategy] if quick else FULL_SPACE[strategy]
    live_config = _load_live_strategy_config(strategy)
    lev = _resolve_leverage(strategy, leverage, live_config)
    live_risk_params = _resolve_live_risk_params(strategy, live_config)

    log_separator("", 78, color="\033[96m")
    print(f"  STRATEGIE-OPTIMIZER v3  {strategy}")
    print(
        f"  {days} Tage | RT {rt * 100:.2f}% | Hebel: x{lev:g} | "
        f"K-Fold: {k_folds} Perioden | "
        f"Sensitivitt: {'Ja' if do_sensitivity else 'Nein'} | "
        f"Modus: {'Quick' if quick else 'Voll'}"
    )
    print(f"  Parallelisierung: {workers} Worker via {effective_executor}")
    if strategy == "FUTURES":
        _src = "CLI" if leverage is not None else "bot_config.json"
        print(
            f"  Hebel x{lev:g} aus {_src}  Stop/TP-% sind PREIS-Bewegungen, "
            f"Margin-Wirkung = %  {lev:g}"
        )
    if live_risk_params:
        print(
            "  Live-Risk-Sizing im Backtest: "
            + ", ".join(f"{k}={v:g}" for k, v in sorted(live_risk_params.items()))
        )
    log_separator("", 78, color="\033[96m")

    keys = list(space.keys())
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
        if futures_screener_parity:
            _p["futures_screener_parity"] = True
    if funding_8h:
        print(
            f"  Funding-Sensitivitt AKTIV  {funding_8h * 100:.4f}%/8h auf das "
            f"Notional pro Hold-Dauer (Kosten, beide Seiten)"
        )
    elif strategy == "FUTURES":
        print(
            "  FUNDING NICHT MODELLIERT (0%/8h)  echtes Leveraged-FUTURES "
            "zahlt alle 8h Funding; gemeldete Edge ist OPTIMISTISCH. Mit "
            "--funding R (z.B. 0.0001) realistisch rechnen, sonst kann eine "
            "net-negative Config als 'deployment_validated' durchrutschen."
        )
    if own_momentum:
        print(
            f"  Own-Momentum-Overlay AKTIV (Fenster {om_window})  blockt "
            f"Entries solange die letzten {om_window} Closes net-negativ sind"
        )
    if regime:
        print("  Regime-Gate AKTIV  LONG nur wenn BTC>EMA50, SHORT nur wenn BTC<EMA50")
    if futures_screener_parity:
        print(
            "  Futures-Screener-Paritaet AKTIV  konservative kausale 1h-Volumen-, "
            "ATR-, Kerzenkoerper-, EMA- und SHORT-Gates; Quiet-Regime ohne "
            "historisches F&G nicht abgesenkt"
        )

    # Display labels: FUTURES uses "min_move" in UI, internally still min_pump
    _display_labels = {
        "min_pump": "min_move" if strategy == "FUTURES" else "min_pump",
    }
    print("\n  Suchraum:")
    for k, v in space.items():
        label = _display_labels.get(k, k)
        print(f"    {label:<22} {len(v)} Werte")
    print(f"\n  Kombinationen:    {len(p_list):,}")
    print(f"  Simulationen total: {len(p_list) * k_folds:,} ({k_folds} K-Fold)\n")

    dataset_manifest = None
    dataset_path = os.path.abspath(dataset) if dataset is not None else None
    workspace_path = os.path.abspath(workspace) if workspace is not None else None
    if dataset_path is not None:
        if workspace_path is None:
            workspace_path = os.path.dirname(os.path.dirname(dataset_path))
        print(f" Lade und verifiziere unveraenderliches Dataset: {dataset_path}")
        t_load = time.time()
        history, dataset_manifest = load_history_dataset(dataset_path)
        _validate_dataset_exchange(dataset_manifest, expected_exchange)
        print(
            f"   {len(history)} Coins in {time.time() - t_load:.1f}s | "
            f"Fingerprint {dataset_manifest['dataset_fingerprint']}\n"
        )
    else:
        # Network data is allowed only for constructing a new snapshot; all
        # reproducible simulation work uses that immutable snapshot.
        print(" Verbinde mit Exchange...")
        ex = (
            get_public_futures_exchange_connection(expected_exchange)
            if strategy == "FUTURES"
            else get_spot_exchange_connection()
        )
        ex.timeout = 30000
        for attempt in range(1, 4):
            try:
                ex.load_markets()
                if expected_exchange is not None:
                    connected_exchange = _canonical_exchange_name(
                        str(getattr(ex, "id", None) or getattr(ex, "name", ""))
                    )
                    if connected_exchange != expected_exchange:
                        raise ValueError(
                            "connected exchange mismatch: expected "
                            f"{expected_exchange}, found {connected_exchange}"
                        )
                print(f"  Verbunden mit {getattr(ex, 'name', None) or 'Exchange'}\n")
                break
            except Exception as e:
                if attempt == 3:
                    print(f"\n Verbindung fehlgeschlagen: {e}")
                    sys.exit(1)
                print(f"  Timeout  warte 5s ({attempt}/3)...")
                time.sleep(5)

        print(" Lade Top-Volumen-Coins...")
        target_coin_count = 30
        if strategy == "FUTURES":
            # Fetch a wider ranked pool because the reliable first-bar check
            # below may remove newly listed contracts.
            coins = get_top_futures_volume_coins(
                ex, n=target_coin_count * 2, days=days
            )
        else:
            coins = get_top_volume_coins(ex, n=target_coin_count, days=days)
        print(f"   {len(coins)} Coins")
        print("  SURVIVORSHIP BIAS (reduziert): Universum = HEUTIGE Top-Volumen-Coins.")
        print(
            f"     Listing-Age-Filter entfernt Coins die VOR {days}d noch nicht handelten,"
        )
        print("  aber DELISTETE Coins fehlen weiterhin  NICHT vollstndig unverzerrt.\n")

        print(f" Lade {days}-Tage-Historie mit {workers} Worker(n)...")
        t_load = time.time()
        history = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
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
        if strategy == "FUTURES":
            # Restore quote-volume order after concurrent downloads and keep
            # the top N contracts that passed the full-history backstop.
            history = {
                symbol: history[symbol]
                for symbol in coins
                if symbol in history
            }
            history = dict(list(history.items())[:target_coin_count])
        print(f"   {len(history)} Coins in {time.time() - t_load:.1f}s\n")
        if workspace_path is not None:
            dataset_root = freeze_history_dataset(
                history,
                workspace_path,
                cutoff_utc=_history_cutoff_utc(history),
                provenance=(
                    {
                        "exchange": getattr(ex, "name", None) or "Exchange",
                        "exchange_id": _canonical_exchange_name(
                            str(getattr(ex, "id", None) or expected_exchange or "")
                        ),
                        "market_type": "linear_usdt_perpetual",
                        "days": days,
                        "universe_rule": (
                            "active_linear_usdt_swap_non_rwa_quote_volume_"
                            "with_listing_age_filter"
                        ),
                        "authenticated": False,
                        "survivorship_bias": True,
                    }
                    if strategy == "FUTURES"
                    else {
                        "exchange": getattr(ex, "name", None) or "Exchange",
                        "days": days,
                        "universe_rule": (
                            "current_top_quote_volume_with_listing_age_filter"
                        ),
                        "survivorship_bias": True,
                    }
                ),
            )
            dataset_path = str(dataset_root)
            history, dataset_manifest = load_history_dataset(dataset_root)
            print(
                "  Dataset eingefroren und rueckverifiziert: "
                f"{dataset_manifest['dataset_fingerprint']}\n"
            )
        else:
            print(
                "  WARNUNG: ohne --workspace ist dieser Lauf nicht "
                "dataset-reproduzierbar und nicht promotionsfaehig.\n"
            )

    # Vorindexierung
    print(" Vorindexierung...")
    t_idx = time.time()
    indexed, all_times = precompute_index(history)
    print(f"   {len(all_times):,} Zeitstempel in {time.time() - t_idx:.1f}s\n")

    #  Out-of-sample HOLDOUT split
    # Reserve the most RECENT holdout_frac of the timeline. It is excluded from
    # EVERY tuning step (folds, walk-forward, regime-split, full-period score,
    # monte-carlo, sensitivity) and only the finally-selected configs are scored
    # on it  an honest "deployment test" on data the search never saw. Without
    # it, K-fold/walk-forward can still collectively overfit the sampled period.
    holdout_times: list = []
    tune_times = all_times
    if 0.0 < holdout_frac < 0.9 and len(all_times) >= 50:
        cut = int(len(all_times) * (1.0 - holdout_frac))
        tune_times = all_times[:cut]
        holdout_times = all_times[cut:]
        print(
            f"  Holdout (out-of-sample, NICHT im Tuning): "
            f"{len(holdout_times):,} Stempel "
            f"({holdout_times[0].strftime('%Y-%m-%d')}  "
            f"{holdout_times[-1].strftime('%Y-%m-%d')})\n"
        )
    else:
        print("  Holdout uebersprungen (zu wenig Daten oder --holdout 0)\n")

    # K-Fold Splits  over the TUNING window only (holdout stays unseen)
    folds = split_into_folds(tune_times, k=k_folds)
    prepared_folds = _prepare_kfold_data(indexed, folds)
    print(f"  K-Fold Split ({k_folds} Perioden):")
    for i, fold in enumerate(folds, 1):
        if fold:
            print(
                f"    Fold {i}: {len(fold):,} Stempel "
                f"({fold[0].strftime('%Y-%m-%d')}  {fold[-1].strftime('%Y-%m-%d')})"
            )
    print()

    baseline_params = _production_baseline_params(
        strategy,
        lev,
        live_config,
        live_risk_params,
        own_momentum=own_momentum,
        om_window=om_window,
        regime=regime,
        funding_8h=funding_8h,
    )
    if futures_screener_parity:
        baseline_params["futures_screener_parity"] = True
    reproducible_run = None
    if dataset_manifest is not None and workspace_path is not None:
        run_config = {
            "strategy": strategy,
            "days": days,
            "use_maker": use_maker,
            "top_n": top_n,
            "k_folds": k_folds,
            "quick": quick,
            "do_sensitivity": do_sensitivity,
            "holdout_frac": holdout_frac,
            "own_momentum": own_momentum,
            "om_window": om_window,
            "regime": regime,
            "funding_8h": funding_8h,
            "exchange": expected_exchange,
            "futures_screener_parity": futures_screener_parity,
            "round_trip_cost_rate": rt,
            "purge_bars": PURGE_BARS,
            "embargo_frac": EMBARGO_FRAC,
            "resolved_leverage": lev,
            "resolved_live_risk": live_risk_params,
            "production_baseline": baseline_params,
            "parameter_space": space,
            "requested_parallel_backend": backend,
            "effective_executor": effective_executor,
        }
        reproducible_run = ReproducibleRun(
            workspace_path,
            dataset_path,
            run_config=run_config,
            splits=split_boundaries(tune_times, folds, holdout_times),
            seed=run_seed,
            workers=workers,
            code_files=_reproducible_code_files(),
            resume=resume,
        )
        print(f"  Reproduzierbarer Run: {reproducible_run.run_id}")

    # The unchanged production baseline is always evaluated before the grid.
    # A reproducible resume reuses only evidence bound to the exact run spec.
    baseline_evidence = reproducible_run.baseline() if reproducible_run else None
    if baseline_evidence is None:
        baseline_stats = simulate_fast(
            indexed, tune_times, strategy, use_maker, baseline_params
        )
        baseline_kfold = kfold_simulate(
            indexed,
            folds,
            strategy,
            use_maker,
            baseline_params,
            prepared_folds=prepared_folds,
        )
        baseline_result = {
            "stats": baseline_stats,
            "kfold": baseline_kfold,
            "score": robustness_score(baseline_stats, baseline_kfold),
        }
        if reproducible_run is not None:
            reproducible_run.record_baseline(baseline_params, baseline_result)
        print(
            "  Produktionsbaseline abgeschlossen: "
            f"Netto {baseline_stats.get('net', 0.0):+.2f} USDT"
        )
    else:
        if baseline_evidence.get("params") != baseline_params:
            raise ValueError("stored baseline conflicts with resolved production config")
        print("  Produktionsbaseline aus gebundenem Run-Manifest verifiziert")

    # Open a per-run JSONL where every config result is appended incrementally
    # so a crash/abort doesn't lose the full run.
    _set_incremental_path(strategy, days)
    if _INCREMENTAL_PATH:
        print(f"   Incremental log: {_INCREMENTAL_PATH}")

    # Hauptsimulation
    print(
        f"  Simuliere {len(p_list):,} Configs  {k_folds} Folds = "
        f"{len(p_list) * k_folds:,} Sims...\n"
    )
    t_sim = time.time()
    progress = Progress(len(p_list), label="Optimize")
    results = []

    pending = []
    for candidate_index, params in enumerate(p_list):
        cached = (
            reproducible_run.checkpoint(candidate_index, params)
            if reproducible_run is not None
            else None
        )
        if cached is None:
            pending.append((candidate_index, params))
        else:
            results.append(cached)
            progress.update(cached.get("stats", {}).get("net", -9999))

    def _persist_completed_candidate(rec):
        candidate_index = rec["candidate_index"]
        params = rec["params"]
        progress.update(rec.get("stats", {}).get("net", -9999))
        if reproducible_run is not None:
            reproducible_run.record_checkpoint(candidate_index, params, rec)
        _persist_result(rec)
        return rec

    def _simulate_local_candidate(task):
        candidate_index, params = task
        stats = simulate_fast(indexed, tune_times, strategy, use_maker, params)
        kfold = kfold_simulate(
            indexed,
            folds,
            strategy,
            use_maker,
            params,
            prepared_folds=prepared_folds,
        )
        return {
            "candidate_index": candidate_index,
            "params": params,
            "stats": stats,
            "kfold": kfold,
            "score": robustness_score(stats, kfold),
        }

    if backend == "process" and workers > 1 and pending:
        spawn_context = multiprocessing.get_context("spawn")
        chunksize = max(1, min(16, math.ceil(len(pending) / (workers * 8))))
        chunks = (
            pending[offset : offset + chunksize]
            for offset in range(0, len(pending), chunksize)
        )
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=spawn_context,
            initializer=_initialize_grid_worker,
            initargs=(
                indexed,
                tune_times,
                folds,
                prepared_folds,
                strategy,
                use_maker,
            ),
        ) as pool:
            in_flight = {}
            for _ in range(workers * 2):
                chunk = next(chunks, None)
                if chunk is None:
                    break
                in_flight[pool.submit(_simulate_grid_chunk, chunk)] = chunk
            while in_flight:
                future = next(as_completed(tuple(in_flight)))
                in_flight.pop(future)
                for record in future.result():
                    results.append(_persist_completed_candidate(record))
                chunk = next(chunks, None)
                if chunk is not None:
                    in_flight[pool.submit(_simulate_grid_chunk, chunk)] = chunk
    else:
        if backend == "thread" and workers > 1 and pending:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(_simulate_local_candidate, task) for task in pending]
                for future in as_completed(futures):
                    results.append(_persist_completed_candidate(future.result()))
        else:
            for task in pending:
                results.append(
                    _persist_completed_candidate(_simulate_local_candidate(task))
                )

    # Completion order is intentionally discarded.  Stable grid order makes
    # score ties and all downstream selection byte-reproducible across workers.
    results.sort(key=lambda result: result["candidate_index"])

    elapsed = time.time() - t_sim
    print(
        f"\n\n  {len(results):,} Configs in {elapsed:.0f}s "
        f"({elapsed / len(results) * 1000:.0f}ms/Config inkl. K-Fold)\n"
    )

    # Sortierung: zuerst robuste Configs, dann nach Score
    sorted_results = rank_optimizer_candidates(results)
    robust_cfgs = [r for r in sorted_results if r["kfold"]["robust"]]
    deep_candidates = select_deep_validation_candidates(results, top_n)
    for candidate in robust_cfgs:
        candidate["deep_validation_selected"] = False
        candidate["deep_validation_complete"] = False
        candidate["deep_validation_errors"] = {}

    #  Deep robustness validation on the top candidates
    # walk_forward / regime_split / outlier / monte_carlo feed robustness_score's
    # modifiers. Running them on the FULL grid would 2-3 the runtime; instead we
    # validate only the top-K already-robust k-fold winners (~8 extra sims each
    # negligible beside the grid). The enriched score feeds back into the ranking
    # so a config that survives out-of-sample outranks a k-fold winner that
    # crumbles.
    deep_k = len(deep_candidates)
    if deep_k > 0:
        print(
            f"\n Deep-validating top {deep_k} robust config(s) "
            f"(walk-forward  regime  outlier  monte-carlo)"
        )
        for r in deep_candidates:
            r["deep_validation_selected"] = True
            p = r["params"]
            evaluate_deep_validation_candidate(
                r,
                {
                    "walk_forward": lambda params=p: walk_forward_simulate(
                        indexed, tune_times, strategy, use_maker, params
                    ),
                    "regime_split": lambda params=p: regime_split_simulate(
                        indexed, tune_times, strategy, use_maker, params
                    ),
                    "outlier_test": lambda params=p: outlier_dependency_test(
                        indexed, tune_times, strategy, use_maker, params
                    ),
                    "monte_carlo": (
                        lambda result=r: monte_carlo_perturbation(
                            result["stats"], seed=run_seed
                        )
                    ),
                },
            )
        # Deep evidence can change the winner, but holdout evidence cannot.
        sorted_results = rank_optimizer_candidates(results)

    # Freeze exactly one winner before touching the final holdout. If it fails,
    # this optimizer run is a NO-GO; the same holdout is not reused to select a
    # runner-up.
    if holdout_times and sorted_results:
        freeze_and_evaluate_final_holdout(
            sorted_results,
            lambda params: holdout_simulate(
                indexed, holdout_times, strategy, use_maker, params
            ),
        )

    for r in sorted_results:
        r["cmd"] = _cmd(r["params"], strategy, days, use_maker)

    # Top-Tabelle
    log_separator("", 78, color="\033[96m")
    print(
        f"  TOP {top_n}  {strategy}  "
        f"|  Robust ({k_folds}/{k_folds} Folds): {len(robust_cfgs)}/{len(p_list)}"
    )
    log_separator("", 78, color="\033[96m")

    hdr = f"  {'#':>3}  {'Pump':>5}  {'TP':>6}  {'Trl':>5}  {'Pt':>4}  {'RSI':>4}  "
    if strategy in ("TREND", "FUTURES"):
        hdr += f"{'Stop':>5}  "
    hdr += (
        f"{'AvgNet':>8}  {'StdNet':>7}  {'Cons':>5}  {'WR':>5}  {'DD':>5}  {'Folds':>5}"
    )
    print(hdr)
    log_separator("", 78)

    for i, r in enumerate(sorted_results[:top_n]):
        s = r["stats"]
        p = r["params"]
        kf = r["kfold"]
        if not s.get("trades"):
            continue
        robust_mark = "" if kf["robust"] else "  "
        row = (
            f"  {i + 1:>3}  "
            f"{p['min_pump']:>4.0f}%  "
            f"{p['activation_profit']:>5.1f}%  "
            f"{p['trailing_distance']:>4.1f}%  "
            f"{p['partial_pct']:>3.0%}  "
            f"{p['rsi_max']:>3.0f}  "
        )
        if strategy in ("TREND", "FUTURES"):
            row += f"{p.get('stop_loss', -2):>4.1f}%  "
        row += (
            f"{kf['avg_net']:>+7.2f}  "
            f"{kf['std_net']:>6.2f}  "
            f"{kf['consistency']:>5.0%}  "
            f"{s['win_rate']:>4.0%}  "
            f"{s['max_dd']:>4.1f}%  "
            f"{kf['profit_count']}/{k_folds}  "
            f"{robust_mark}"
        )
        print(row)

    log_separator("", 78, color="\033[96m")

    # Near-miss report: configs profitable in exactly k_folds-1 folds
    near_miss_threshold = k_folds - 1
    near_misses = [
        r
        for r in results
        if not r["kfold"]["robust"]
        and r["kfold"]["profit_count"] == near_miss_threshold
    ]
    near_misses.sort(key=lambda x: x["kfold"]["avg_net"], reverse=True)
    if near_misses:
        print(
            f"\n  NEAR-MISS ({near_miss_threshold}/{k_folds} folds profitable, "
            f"{len(near_misses)} config(s)):"
        )
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
    bs = best["stats"]
    bp = best["params"]
    bkf = best["kfold"]

    #  Lpez de Prado trust diagnostics: DSR + PBO
    # The selected candidate's Sharpe belongs to the full tested universe. DSR
    # deflates it by the expected maximum under the null; PBO (via CSCV over
    # the k folds) estimates the probability the apparent edge is overfit. Both
    # are diagnostics layered ON TOP of the robust gate  they do NOT change it.
    sr_list = [
        r["stats"].get("sharpe", 0.0)
        for r in results
        if r["stats"].get("trade_count")
    ]
    sr_best = bs.get("sharpe", 0.0)
    best_nets = list(bs.get("position_net_trades") or [])
    n_obs = len(best_nets)
    skew_b, kurt_b = _sample_skew_kurt(best_nets)
    dsr = deflated_sharpe_ratio(sr_list, sr_best, n_obs, skew=skew_b, kurt=kurt_b)
    # PBO matrix: at least eight time-ordered return blocks. Four aggregate
    # fold totals yield only six CSCV partitions and are too coarse.
    perf_matrix = build_pbo_block_matrix(results, minimum_blocks=8)
    pbo = probability_of_backtest_overfitting(perf_matrix)
    dsr_val = dsr.get("dsr")
    pbo_val = pbo.get("pbo")
    deployment_trustworthy = bool(
        bkf["robust"]
        and best.get("deep_validation_complete") is True
        and best.get("final_holdout_pass") is True
        and best.get("cost_stress_pass") is True
        and dsr_val is not None
        and dsr_val >= 0.95
        and pbo_val is not None
        and pbo_val <= 0.25
        and pbo.get("evidence_valid") is True
    )
    best["dsr"] = dsr
    best["pbo"] = pbo
    best["deployment_trustworthy"] = deployment_trustworthy

    print("\n  BESTE KONFIGURATION:")
    print(f"     {_label(bp, strategy)}")
    print(f"  Avg Netto:  {bkf['avg_net']:+.2f} USDT (ber {k_folds} Folds)")
    print(
        f"     Konsistenz:   {bkf['consistency']:.0%}  "
        f"(StdAbw: {bkf['std_net']:.2f} USDT)"
    )
    print(
        f"     Folds:        {bkf['profit_count']}/{k_folds} profitabel | "
        f"Robust: {'Ja ' if bkf['robust'] else 'Nein '}"
    )
    print(
        f"     Vollperiode:  Netto {bs['net']:+.2f} USDT | "
        f"WR {bs['win_rate']:.1%} | Sharpe {bs['sharpe']:.3f}"
    )

    # Lpez de Prado trust diagnostics
    if dsr_val is not None:
        print(
            f"     DSR:          {dsr_val:.3f}  "
            f"({' 0.95' if dsr_val >= 0.95 else ' <0.95  Edge womglich Selektionsrauschen'})"
            f"  [{dsr.get('n_trials', 0)} Trials, {dsr.get('n_obs', 0)} Trades]"
        )
    else:
        print(f"  DSR:  ({dsr.get('reason', 'n/a')})")
    if pbo_val is not None:
        print(
            f"     PBO (CSCV):   {pbo_val:.3f}  "
            f"({' 0.5' if pbo_val <= 0.5 else ' >0.5  overfit'})"
            f"  [{pbo.get('n_partitions', 0)} Partitionen]"
        )
    else:
        print(f"  PBO (CSCV):  ({pbo.get('reason', 'n/a')})")
    print(
        f"     Trustworthy:  "
        f"{'Ja ' if deployment_trustworthy else 'Nein '} "
        f"(robust  final holdout  cost stress  DSR0.95  PBO0.25)"
    )

    # Deep-validation summary
    if best.get("deep_validation_complete") is False:
        error_names = ", ".join(
            sorted((best.get("deep_validation_errors") or {}).keys())
        )
        print(
            "     Deep-Validation: UNVOLLSTAENDIG"
            + (f" ({error_names})" if error_names else " (nicht ausgewaehlt)")
        )
    wf = best.get("walk_forward") or {}
    rs = best.get("regime_split") or {}
    mc = best.get("monte_carlo") or {}
    ot = best.get("outlier_test") or {}
    if wf or rs or mc or ot:
        if wf:
            n_sl = len(wf.get("slice_nets", []))
            print(
                "     Walk-Forward: "
                + (
                    " alle Slices profitabel"
                    if wf.get("all_profitable")
                    else (
                        f" {wf.get('profitable_count', '?')}/{n_sl} Slices +, "
                        f"{wf.get('edge_count', '?')}/{n_sl} Edge"
                    )
                )
            )
        if rs:
            print(
                "     Regime-Test:  "
                + (
                    " bersteht alle Regimes"
                    if rs.get("survives_all")
                    else f" {rs.get('profitable_count', '?')}/"
                    f"{rs.get('tested_count', '?')} Regimes +"
                )
            )
        if mc.get("positive_share") is not None:
            print(
                f"     Monte-Carlo:  {mc['positive_share'] * 100:.0f}% der "
                f"Lufe positiv"
                + (
                    "  "
                    if mc.get("robust")
                    else "  fragil"
                    if mc.get("concerning")
                    else ""
                )
            )
        if ot.get("outlier_fragile") is not None:
            print(
                "     Outlier-Dep.: "
                + (
                    " FRAGIL  Edge hngt an Top-Trades"
                    if ot["outlier_fragile"]
                    else " breit verteilte Edge"
                )
            )
        if ot.get("symbol_fragile") is not None:
            dominant_share = ot.get("dominant_positive_share")
            share_text = (
                f"{dominant_share:.1%}"
                if isinstance(dominant_share, (int, float))
                and not isinstance(dominant_share, bool)
                and math.isfinite(float(dominant_share))
                else "n/a"
            )
            print(
                "     Symbol-Dep.:  "
                f"{ot.get('dominant_symbol', 'n/a')} {share_text} der "
                "positiven Symbol-PnL  "
                + ("FRAGIL" if ot["symbol_fragile"] else "verteilt")
            )

    # Out-of-sample holdout  the honest deployment test.
    hd = best.get("holdout")
    if holdout_times:
        if best.get("holdout_evaluation_error"):
            print(
                "  Holdout (OOS):  FEHLER  "
                f"{best['holdout_evaluation_error']}"
            )
        elif hd and not hd.get("skipped"):
            holdout_net = best.get("holdout_net")
            _ok = holdout_net is not None and holdout_net > 0.0
            holdout_net_text = (
                f"{holdout_net:+.2f}" if holdout_net is not None else "n/a"
            )
            print(
                f"  Holdout (OOS): {'' if _ok else ''} Netto "
                f"{holdout_net_text} USDT | WR {hd.get('win_rate', 0):.1%} | "
                f"{hd.get('trades', 0)} Zeilen / "
                f"{best.get('holdout_full_trades', 0)} Full-Trades  "
                f"{'DEPLOYMENT-VALIDATED' if best.get('deployment_validated') else 'NICHT besttigt (out-of-sample fragil)'}"
            )
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
        "strategy": strategy,
        "min_pump": float(bp.get("min_pump", 0)),
        "activation_profit": float(bp.get("activation_profit", 0)),
        "trailing_distance": float(bp.get("trailing_distance", 0)),
        "stop_loss": _stop_out,
        "partial_pct": float(bp.get("partial_pct", 0)),
        "rsi_max": float(bp.get("rsi_max", 0)),
        "avg_net": float(bkf["avg_net"]),
        "consistency": float(bkf["consistency"]),
        "win_rate": float(bs.get("win_rate", 0)),
        "sharpe": float(bs.get("sharpe", 0)),
        "robust": bool(bkf["robust"]),
        "deep_validation_complete": bool(
            best.get("deep_validation_complete", False)
        ),
        "deep_validation_admission_error": best.get(
            "deep_validation_admission_error"
        ),
        "symbol_concentration_pass": (
            ot.get("symbol_fragile") is False
            if isinstance(ot, dict) and ot
            else False
        ),
        "dominant_symbol": ot.get("dominant_symbol"),
        "dominant_positive_share": ot.get("dominant_positive_share"),
        "holdout_net": best.get("holdout_net"),
        "holdout_trades": int(best.get("holdout_trades", 0) or 0),
        "holdout_full_trades": int(best.get("holdout_full_trades", 0) or 0),
        "holdout_evaluation_attempted": bool(
            best.get("holdout_evaluation_attempted", False)
        ),
        "holdout_evidence_valid": bool(
            best.get("holdout_evidence_valid", False)
        ),
        "holdout_trade_accounting_valid": bool(
            best.get("holdout_trade_accounting_valid", False)
        ),
        "holdout_aggregates_consistent": bool(
            best.get("holdout_aggregates_consistent", False)
        ),
        "holdout_sample_consistent": bool(
            best.get("holdout_sample_consistent", False)
        ),
        "holdout_outcomes_consistent": bool(
            best.get("holdout_outcomes_consistent", False)
        ),
        "holdout_evaluation_error": best.get("holdout_evaluation_error"),
        "holdout_net_consistent": bool(
            best.get("holdout_net_consistent", False)
        ),
        "cost_stress_pass": bool(best.get("cost_stress_pass", False)),
        "final_holdout_pass": bool(best.get("final_holdout_pass", False)),
        "deployment_validated": bool(best.get("deployment_validated", False)),
        "dsr": (float(dsr_val) if dsr_val is not None else None),
        "pbo": (float(pbo_val) if pbo_val is not None else None),
        "deployment_trustworthy": bool(deployment_trustworthy),
    }
    print(f"\n<<<BEST_CONFIG>>>{_json.dumps(best_payload)}<<<END_BEST_CONFIG>>>\n")

    # Sensitivitts-Analyse fr Top 3
    if do_sensitivity and len(sorted_results) > 0:
        print("\n  SENSITIVITTS-ANALYSE (Top 3 Configs):\n")
        for rank in range(min(3, len(sorted_results))):
            cfg = sorted_results[rank]
            sens = sensitivity_check(
                indexed, tune_times, cfg["params"], strategy, use_maker, space
            )
            print(f"  Rang {rank + 1}: {_label(cfg['params'], strategy)}")
            print(f"    Bewertung:        {sens['rating']}")
            print(
                f"  Avg nderung:  {sens['avg_change']:.1f}%  "
                f"(Max: {sens['max_change']:.1f}%)"
            )
            print(f"    Basis-Netto:      {sens['base_net']:+.2f} USDT")
            cfg["sensitivity"] = sens
            print()

    print("\n  Validierung der Top-1:")
    print(f"     {best['cmd']}\n")

    # CSV
    export_csv(sorted_results, strategy, days, k_folds)

    # Footer
    print(
        f"\n  Simulationen: {len(p_list) * k_folds:,} | "
        f"Robuste Configs: {len(robust_cfgs)} | "
        f"Zeit: {time.time() - t_sim:.0f}s"
    )
    print(f"  Analyse: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_separator("", 78, color="\033[96m")

    return sorted_results


#  CLI


_OPTIMIZER_CLI_BOOLEAN_OPTIONS = {
    "--maker": ("use_maker", True),
    "--quick": ("quick", True),
    "--no-sensitivity": ("do_sensitivity", False),
    "--own-momentum": ("own_momentum", True),
    "--regime": ("regime", True),
    "--resume": ("resume", True),
    "--futures-screener-parity": ("futures_screener_parity", True),
}
_OPTIMIZER_CLI_VALUE_OPTIONS = {
    "--top": ("top_n", int),
    "--kfold": ("k_folds", int),
    "--leverage": ("leverage", float),
    "--holdout": ("holdout_frac", float),
    "--om-window": ("om_window", int),
    "--funding": ("funding_8h", float),
    "--workspace": ("workspace", str),
    "--dataset": ("dataset", str),
    "--seed": ("seed", int),
    "--workers": ("workers", int),
    "--backend": ("backend", str),
    "--exchange": ("exchange", str),
}


def _parse_optimizer_cli_args(args: list[str]) -> dict:
    """Parse optimizer CLI arguments without silently replacing bad input."""
    if not isinstance(args, (list, tuple)) or not args:
        raise ValueError("optimizer strategy is required")
    strategy = args[0]
    if strategy not in ("TREND", "SPOT", "FUTURES"):
        raise ValueError(f"unsupported optimizer strategy {strategy!r}")

    cursor = 1
    days = DEFAULT_DAYS
    if cursor < len(args) and not str(args[cursor]).startswith("--"):
        try:
            days = int(args[cursor])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("days must be an integer >= 1") from exc
        cursor += 1

    if strategy == "TREND":
        if cursor != len(args):
            raise ValueError("TREND optimizer route accepts only an optional day count")
        _validate_optimizer_run_inputs(strategy, days, 10, 4, 0.2, 8, 0.0)
        return {"strategy": strategy, "days": days}

    parsed = {
        "strategy": strategy,
        "days": days,
        "use_maker": False,
        "top_n": 10,
        "k_folds": 4,
        "quick": False,
        "do_sensitivity": True,
        "leverage": None,
        "holdout_frac": 0.2,
        "own_momentum": False,
        "om_window": 8,
        "regime": False,
        "funding_8h": 0.0,
        "futures_screener_parity": False,
    }
    seen = set()
    while cursor < len(args):
        option = args[cursor]
        if option in seen:
            raise ValueError(f"duplicate optimizer option {option}")
        if option in _OPTIMIZER_CLI_BOOLEAN_OPTIONS:
            name, value = _OPTIMIZER_CLI_BOOLEAN_OPTIONS[option]
            parsed[name] = value
            seen.add(option)
            cursor += 1
            continue
        if option not in _OPTIMIZER_CLI_VALUE_OPTIONS:
            raise ValueError(f"unknown optimizer option {option!r}")
        if cursor + 1 >= len(args) or str(args[cursor + 1]).startswith("--"):
            raise ValueError(f"optimizer option {option} requires a value")
        name, converter = _OPTIMIZER_CLI_VALUE_OPTIONS[option]
        try:
            parsed[name] = converter(args[cursor + 1])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"invalid value for optimizer option {option}") from exc
        seen.add(option)
        cursor += 2

    _validate_optimizer_run_inputs(
        parsed["strategy"],
        parsed["days"],
        parsed["top_n"],
        parsed["k_folds"],
        parsed["holdout_frac"],
        parsed["om_window"],
        parsed["funding_8h"],
        parsed["leverage"],
    )
    _validate_reproducible_run_inputs(
        workspace=parsed.get("workspace"),
        dataset=parsed.get("dataset"),
        resume=parsed.get("resume", False),
        seed=parsed.get("seed"),
        workers=parsed.get("workers", 8),
        backend=parsed.get("backend", "process"),
        exchange=parsed.get("exchange"),
    )
    if parsed["futures_screener_parity"] and strategy != "FUTURES":
        raise ValueError("futures screener parity is available only for FUTURES")
    return parsed


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] not in ("TREND", "SPOT", "FUTURES"):
        print("Usage: python optimizer.py [TREND|SPOT|FUTURES] [days]")
        print("       [--maker] [--top N] [--kfold K] [--leverage N]")
        print("       [--no-sensitivity] [--quick] [--holdout F]")
        print("       [--own-momentum] [--om-window N] [--regime] [--funding R]")
        print("       [--futures-screener-parity]")
        print("       [--workspace PATH] [--dataset PATH] [--exchange NAME] [--resume]")
        print("       [--seed N] [--workers N] [--backend process|thread]")
        print(
            "  --funding R     : model funding R per 8h (e.g. 0.0003) as a "
            "cost sweep on the notional per hold-duration (default 0 = off)"
        )
        print(
            "  --holdout F     : reserve most-recent fraction F out-of-sample "
            "(default 0.2; 0 disables)"
        )
        print(
            "  --workspace PATH: freeze inputs and bind manifests/checkpoints below PATH"
        )
        print("  --dataset PATH  : run offline from a verified immutable dataset")
        print("  --exchange NAME : bind reproducible data and runs to this venue")
        print("  --resume        : resume the exact matching dataset/config/code run")
        print("  --workers N     : CPU workers (benchmark 1/8/16/24/32 on the 9950X)")
        print("  --backend MODE  : process for real CPU parallelism; thread saves RAM")
        print(
            "  --own-momentum  : A/B test the own-momentum overlay "
            "(block entries after N net-negative closes)"
        )
        print("  --leverage N : override leverage (default: read from bot_config.json)")
        print(
            "  --futures-screener-parity: reproduce causal 1h live screener gates"
        )
        print()
        print("  Beispiele:")
        print("  python optimizer.py TREND  # voll, K=4, mit Sensitivitt")
        print("    python optimizer.py SPOT 60 --maker # mit Maker-Orders")
        print("    python optimizer.py FUTURES 30 --quick    # Futures-Suchraum")
        print("    python optimizer.py FUTURES --kfold 5  # 5 Perioden statt 4")
        print("    python optimizer.py FUTURES --quick    # ~1/8 Suchraum")
        print("    python optimizer.py SPOT --no-sensitivity  # ohne Robustheits-Test")
        print("    python optimizer.py TREND 90           # separater SMA-Sweep")
        sys.exit(1)

    try:
        cli = _parse_optimizer_cli_args(args)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)

    strategy = cli["strategy"]
    if strategy == "TREND":
        # TREND trades the SMA-ensemble, not the momentum grid below  route to
        # the validator's robustness sweep so tuning reflects the live signal.
        from tools import trend_check

        _days = str(cli["days"])
        sys.argv = ["trend_check", _days, "--sweep"]
        trend_check.main()
        sys.exit(0)
    run_optimizer(**cli)
