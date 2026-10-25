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
  python trend_check.py 365 BTC --runtime-params 50,150,20,50,2,1
  python trend_check.py 365 BTC --runtime-snapshot path/to/sanitized.json

Runtime inputs are explicit effective signal values, not historical config
evidence. The default rules and --sweep remain separate research variants.
"""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

from __future__ import annotations

import math
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

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
MAX_RUNTIME_SNAPSHOT_BYTES = 8_192
_RUNTIME_FIELDS = (
    ("TREND_SMA_FAST", "sma_fast"), ("TREND_SMA_SLOW", "sma_slow"),
    ("TREND_CROSS_FAST", "cross_fast"), ("TREND_CROSS_SLOW", "cross_slow"),
    ("TREND_VOTE_MIN", "vote_min"), ("TREND_EXIT_VOTE", "exit_vote"),
)


def _params_from_snapshot(snapshot):
    """Accept only explicit effective signal values, using runtime sanitizing."""
    from bot_utils.config import merge_runtime_config
    from trading.trend_signal import TrendParams, params_from_cfg

    keys = {key for key, _field in _RUNTIME_FIELDS}
    if type(snapshot) is not dict or set(snapshot) != keys or any(type(v) is not int for v in snapshot.values()):
        raise ValueError("runtime snapshot must contain exactly six effective integer signal keys")
    defaults = {key: getattr(TrendParams(), field) for key, field in _RUNTIME_FIELDS}
    sanitized = merge_runtime_config("TREND", defaults, {"TREND": snapshot})
    if any(sanitized.get(key) != snapshot[key] for key in keys):
        raise ValueError("runtime snapshot contains values changed by runtime sanitizing")
    return params_from_cfg(sanitized.get)


def _load_runtime_snapshot(path):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate runtime snapshot key")
            result[key] = value
        return result

    def invalid_constant(_value):
        raise ValueError("invalid runtime snapshot number")

    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_RUNTIME_SNAPSHOT_BYTES + 1)
    if len(raw) > MAX_RUNTIME_SNAPSHOT_BYTES:
        raise ValueError("runtime snapshot exceeds byte limit")
    try:
        payload = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=unique,
                             parse_constant=invalid_constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("invalid runtime snapshot JSON") from exc
    return _params_from_snapshot(payload)


def _extract_runtime_options(args):
    remaining, params = [], None
    index = 0
    while index < len(args):
        argument = args[index]
        if argument not in {"--runtime-params", "--runtime-snapshot"}:
            remaining.append(argument)
            index += 1
            continue
        if params is not None or index + 1 >= len(args):
            raise ValueError("runtime signal input must be supplied exactly once")
        value = args[index + 1]
        if argument == "--runtime-snapshot":
            params = _load_runtime_snapshot(value)
        else:
            if len(value) > 128:
                raise ValueError("runtime signal input too long")
            values = value.split(",")
            if len(values) != len(_RUNTIME_FIELDS) or any(not part.isascii() or not part.isdigit() for part in values):
                raise ValueError("runtime params require six comma-separated integers")
            params = _params_from_snapshot({key: int(part) for (key, _field), part in zip(_RUNTIME_FIELDS, values)})
        index += 2
    return remaining, params


def _runtime_signal_trace(closes, params, *, initially_held=False):
    """Only runtime signals; retain its history guard and held-state hysteresis."""
    from trading.trend_signal import bars_required, has_full_history, is_in_trend

    if type(initially_held) is not bool:
        raise ValueError("initial holding state must be boolean")
    values = []
    for value in closes:
        if isinstance(value, bool):
            raise ValueError("invalid runtime signal close")
        parsed = float(value)
        if not math.isfinite(parsed) or parsed <= 0:
            raise ValueError("invalid runtime signal close")
        values.append(parsed)
    trace, held = [], initially_held
    window = bars_required(params) + 6
    for index in range(len(values)):
        history = values[max(0, index + 1 - window):index + 1]
        decision = is_in_trend(history, params, currently_held=held) if has_full_history(history, params) else (held, 0, {})
        trace.append(decision)
        held = decision[0]
    return trace


def _runtime_closed_ohlc(series, *, since_ms, now_ms):
    """Apply the runtime's daily close gate before research normalization."""
    from bot_utils.safe_numeric import parse_ohlcv_closes

    # Validate every observed row before clipping the research window. Dropping
    # future or malformed source rows first would conceal a runtime rejection.
    if parse_ohlcv_closes(series, expected_interval_ms=86_400_000, now_ms=now_ms) is None:
        return []
    bucket = now_ms // 86_400_000 * 86_400_000
    return [row for row in series if since_ms <= float(row[0]) < bucket]


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


def _fetch_ohlc(ex, symbol: str, days: int, *, runtime_signal: bool = False) -> list:
    """Cache-backed, rate-limit-safe DAILY OHLC  list of [ts,o,h,l,c,...]."""
    if (
        isinstance(days, bool)
        or not isinstance(days, int)
        or not 1 <= days <= MAX_TREND_DAYS
    ):
        return []
    tf_ms = 86_400_000
    try:
        if runtime_signal:
            from core.clock import now_ms as runtime_now_ms
            raw_now_ms = runtime_now_ms()
        else:
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
        or (not runtime_signal and not numeric_now_ms.is_integer())
    ):
        print(f"   [WARN] {symbol}: invalid exchange clock")
        return []
    # TrendBot also truncates its regular submillisecond clock to integer ms.
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
    if runtime_signal:
        validated = _runtime_closed_ohlc(series, since_ms=since, now_ms=now_ms)
    else:
        validated = _validated_trend_ohlc(
            series,
            since_ms=since,
            until_ms=current_bucket_start - 1,
        )
    if not validated or float(validated[-1][0]) != current_bucket_start - tf_ms:
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
            opening_equity = e
            e *= 1 - COST  # pay the switch
            if held == 1 and new == 0 and entry_eq:
                tr = e / entry_eq - 1
                trades.append(tr)
                if tr > 0:
                    wins += 1
            if new == 1:
                entry_eq = opening_equity
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
    """Research long/flat rule using a majority of three fast signals."""
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


MAX_SIZING_ROWS = 20_000
MAX_SIZING_REPORT_BYTES = 16 * 1024 * 1024
_SIZING_FIELDS = ("POSITION_SIZE", "POSITION_SIZE_MAX", "TREND_VOL_TARGET_LOOKBACK")
_SIZING_SOURCE_FILES = (
    "tools/trend_check.py", "trading/trend_signal.py", "trading/vol_target.py",
    "tools/native_ohlcv_snapshot.py", "tools/ohlcv_cache.py",
    "bot_utils/safe_numeric.py", "bot_utils/config.py", "bot_utils/atomic_publish.py",
    "tools/simulation_workspace.py", "trading/profit_research_runner.py",
    "trading/experiment_registry_contract.py", "sqlite_safety.py",
)


def _sizing_source_hashes(source_root=None):
    """Bind public code bytes, including the actual signal/input validators."""
    import hashlib
    from tools.native_ohlcv_snapshot import _plain_path

    root = _plain_path(Path(__file__).absolute().parents[1] if source_root is None else source_root)
    result = {}
    for relative in _SIZING_SOURCE_FILES:
        path = _plain_path(root / relative)
        with path.open("rb") as stream:
            before = os.fstat(stream.fileno())
            raw = stream.read(2 * 1024 * 1024 + 1)
            after = os.fstat(stream.fileno())
        if (len(raw) > 2 * 1024 * 1024 or len(raw) != before.st_size
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            raise ValueError("sizing source is oversized or changed during hashing")
        result[relative] = hashlib.sha256(raw).hexdigest()
    return result


def _sizing_values(size, cap, lookback):
    if (type(size) not in (int, float) or not math.isfinite(size) or not 0 < size <= 10_000
            or type(cap) not in (int, float) or not math.isfinite(cap) or not 0 <= cap <= 10_000
            or type(lookback) is not int or not 10 <= lookback <= 200):
        raise ValueError("sizing requires finite size (0,10000], cap [0,10000], integer lookback [10,200]")
    return float(size), float(cap), lookback


def _sizing_inputs(directory):
    """Read every declared daily Spot stream, without substitutes or subsets."""
    import csv
    import hashlib
    import io
    from tools import native_ohlcv_snapshot as native

    directory = native._plain_path(directory)
    manifest = native.verify_snapshot(directory)
    if (manifest["schema_version"], manifest["market_type"], manifest["endpoint"]) != (
            2, "spot", "/api/v3/klines"):
        raise ValueError("sizing diagnostics require a native Spot schema2 snapshot")
    daily = {item["symbol"]: item for item in manifest["streams"] if item["timeframe"] == "1d"}
    universe = {item["symbol"] for item in manifest["streams"]}
    days = (manifest["end_ms"] - manifest["start_ms"]) // 86_400_000
    if (set(daily) != universe or not 1 <= len(universe) <= MAX_TREND_COINS
            or not 1 <= days <= MAX_TREND_DAYS or days * len(universe) > MAX_SIZING_ROWS):
        raise ValueError("incomplete daily basket or sizing row budget exceeded")
    closes = {}
    for symbol, item in sorted(daily.items()):
        if item["rows"] != days:
            raise ValueError("daily basket grids differ")
        with native._plain_path(directory / item["file"]).open("rb") as stream:
            raw = stream.read(native.MAX_FILE_BYTES + 1)
        if len(raw) != item["bytes"] or hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("daily source changed during sizing read")
        reader = csv.reader(io.StringIO(raw.decode("utf-8"), newline=""))
        if next(reader, None) != list(native._HEADER):
            raise ValueError("daily CSV header differs from native contract")
        rows = []
        for index, row in enumerate(reader):
            if index >= days or len(row) != 6 or any(len(cell) > 64 for cell in row):
                raise ValueError("daily CSV exceeds its declared shape")
            stamp = manifest["start_ms"] + index * 86_400_000
            if row[0] != datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat():
                raise ValueError("daily CSV is not the identical closed UTC grid")
            values = [float(cell) for cell in row[1:]]
            op, high, low, close, volume = values
            if (any(not math.isfinite(value) for value in values) or min(values[:4]) <= 0
                    or volume < 0 or high < max(op, close) or low > min(op, close)):
                raise ValueError("invalid daily candle")
            rows.append([stamp, *values])
        if len(rows) != days or _runtime_closed_ohlc(
                rows, since_ms=manifest["start_ms"], now_ms=manifest["end_ms"]) != rows:
            raise ValueError("daily basket contains missing, forming or invalid candles")
        closes[symbol] = [row[4] for row in rows]
    if native.verify_snapshot(directory) != manifest:
        raise ValueError("native sizing snapshot changed during read")
    return manifest, closes


def _sizing_diagnostic_report(directory, params, *, size, cap, lookback,
                              registry_root=None, trial_id=None):
    """Conditional requested quotes, never cash allocations or a backtest."""
    import hashlib
    from trading.trend_signal import bars_required, has_full_history, is_in_trend
    from trading.vol_target import realized_vol, basket_median_vol, vol_target_multiplier
    from tools.simulation_workspace import register_planned_trial, validate_trial_registry_options

    validate_trial_registry_options(registry_root, trial_id)
    size, cap, lookback = _sizing_values(size, cap, lookback)
    supplied_signal = {key: getattr(params, field) for key, field in _RUNTIME_FIELDS}
    if _params_from_snapshot(supplied_signal) != params:
        raise ValueError("invalid effective signal parameters")
    sources = _sizing_source_hashes()
    manifest, series = _sizing_inputs(directory)
    inputs = {"signal_params": supplied_signal,
              "sizing_params": dict(zip(_SIZING_FIELDS, (size, cap, lookback))),
              "native_manifest": manifest}
    encoded_inputs = json.dumps(inputs, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    trials = []
    if registry_root is not None:
        for target in (False, True):
            suffix = "on" if target else "off"
            trials.append(register_planned_trial(registry_root, trial_id + ":" + suffix,
                experiment_name="trend_requested_quote_sizing", sources=sources,
                dataset={"native_manifest": manifest, "inputs_sha256": hashlib.sha256(encoded_inputs).hexdigest()},
                controls={"signal_params": supplied_signal, "size": size, "cap": cap,
                          "lookback": lookback, "vol_target": target},
                baseline={"vol_target": False, "size": size, "cap": cap, "signal_params": supplied_signal},
                split_policy={"method": "not_applicable_conditional_quote_sizing",
                              "portfolio_or_pnl_evaluation": False, "independent_holdout_verified": False}))
    day = 86_400_000
    count = (manifest["end_ms"] - manifest["start_ms"]) // day
    window = max(bars_required(params), lookback + 1) + 6
    rows = []
    for index in range(count):
        histories = {symbol: values[max(0, index + 1 - window):index + 1]
                     for symbol, values in series.items()}
        # Runtime computes the median before held/signal/claim gates. Do not
        # filter the declared basket to today's entry candidates.
        try:
            vols = {symbol: realized_vol(values, lookback) for symbol, values in histories.items()}
        except (ArithmeticError, AttributeError, TypeError, ValueError) as exc:
            raise ValueError("invalid runtime volatility inputs") from exc
        median = basket_median_vol(list(vols.values()))
        for symbol, history in histories.items():
            ready = has_full_history(history, params)
            signal, votes, _detail = is_in_trend(history, params, currently_held=False) if ready else (False, 0, {})
            multiplier = vol_target_multiplier(vols[symbol], median)
            baseline, targeted = size, size * multiplier
            if cap > 0:
                baseline, targeted = min(baseline, cap), min(targeted, cap)
            if (not all(math.isfinite(value) and value > 0 for value in (baseline, targeted))
                    or not 0.33 <= multiplier <= 1.0 or targeted > baseline):
                raise ValueError("invalid bounded runtime sizing result")
            rows.append({"decision_ms": manifest["start_ms"] + (index + 1) * day,
                "symbol": symbol, "closed_bars_available": index + 1,
                "history_bars_used": len(history), "full_signal_history": ready,
                "entry_signal_if_unheld": signal, "votes": votes,
                "realized_vol": vols[symbol], "basket_median_vol": median,
                "multiplier": multiplier,
                "baseline_requested_quote": baseline if signal else None,
                "voltarget_requested_quote": targeted if signal else None,
                "reason": "conditional_entry_quote" if signal else "no_entry_signal" if ready else "warmup"})
    result = {"schema_version": 1, "kind": "trend_requested_quote_sizing_diagnostic",
        "variants": ["vol_target_off", "vol_target_on"],
        "inputs_sha256": hashlib.sha256(encoded_inputs).hexdigest(), "inputs": inputs,
        "universe": list(series), "decision_time": "UTC_daily_bar_close",
        "quote_condition": "unheld, entry signal true, all other runtime entry gates allow",
        "historical_universe_verified": False, "historical_configuration_verified": False,
        "portfolio_parity": False, "pnl_simulated": False, "promotion_eligible": False,
        "rows": rows}
    if trials:
        result["trial_registration"] = trials
    result["source_sha256"] = sources
    result["experiment_identity_sha256"] = hashlib.sha256(json.dumps(
        {"inputs_sha256": result["inputs_sha256"], "source_sha256": sources},
        sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    from tools.native_ohlcv_snapshot import verify_snapshot
    if verify_snapshot(directory) != manifest:
        raise ValueError("native sizing snapshot changed during calculation")
    if _sizing_source_hashes() != sources:
        raise ValueError("sizing source changed during calculation")
    if len(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()) > MAX_SIZING_REPORT_BYTES:
        raise ValueError("sizing report byte budget exceeded")
    return result


def _sizing_diagnostic_main(arguments):
    import argparse
    import hashlib
    from bot_utils.atomic_publish import atomic_create_bytes
    from tools.native_ohlcv_snapshot import _plain_path

    parser = argparse.ArgumentParser(description="Offline TREND conditional quote sizing; no portfolio/PnL simulation")
    parser.add_argument("--sizing-snapshot", required=True, type=Path)
    parser.add_argument("--sizing-params", required=True, help="POSITION_SIZE,POSITION_SIZE_MAX,LOOKBACK")
    parser.add_argument("--output", required=True, type=Path, help="fresh immutable JSON report")
    from tools.simulation_workspace import add_trial_registry_arguments, validate_trial_registry_options
    add_trial_registry_arguments(parser)
    try:
        remaining, params = _extract_runtime_options(arguments)
        args = parser.parse_args(remaining)
        validate_trial_registry_options(args.registry_root, args.trial_id)
        if params is None:
            raise ValueError("sizing requires six explicit effective runtime signal parameters")
        if len(args.sizing_params) > 128:
            raise ValueError("oversized sizing parameters")
        values = args.sizing_params.split(",")
        if len(values) != 3 or not values[2].isascii() or not values[2].isdigit():
            raise ValueError("sizing params require size,cap,integer-lookback")
        output = _plain_path(args.output)
        if output.exists():
            raise ValueError("sizing output must be fresh")
        report = _sizing_diagnostic_report(args.sizing_snapshot, params,
            size=float(values[0]), cap=float(values[1]), lookback=int(values[2]),
            registry_root=args.registry_root, trial_id=args.trial_id)
        raw = json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        if len(raw) > MAX_SIZING_REPORT_BYTES:
            raise ValueError("sizing report byte budget exceeded")
        if not atomic_create_bytes(output, raw):
            raise ValueError("sizing output already exists")
        print(json.dumps({"report": str(output), "sha256": hashlib.sha256(raw).hexdigest(),
                          "decision_rows": len(report["rows"]), "promotion_eligible": False}, sort_keys=True))
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except (OSError, ValueError, TypeError, OverflowError, UnicodeError, RuntimeError) as exc:
        print(f"Invalid sizing diagnostic: {_safe_exc(exc)}")
        return 2


def _event_trace_input(path):
    """Bounded, stable, link-free input; duplicate keys never select a winner."""
    from tools.native_ohlcv_snapshot import _plain_path
    from tools.backtester import TREND_EVENT_MAX_BYTES

    path = _plain_path(path)
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        raw = stream.read(TREND_EVENT_MAX_BYTES + 1)
        after = os.fstat(stream.fileno())
    if (not raw or len(raw) > TREND_EVENT_MAX_BYTES or len(raw) != before.st_size
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
        raise ValueError("event trace changed during read or exceeds byte budget")
    _plain_path(path)

    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate event trace key")
            result[key] = value
        return result

    def invalid_constant(_value):
        raise ValueError("nonfinite event trace number")

    try:
        trace = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=unique,
                           parse_constant=invalid_constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("invalid event trace JSON") from exc
    return raw, trace


def _event_source_hashes():
    from tools.backtester import _trend_event_code_hashes
    return {**_sizing_source_hashes(), **_trend_event_code_hashes()}


def _event_replay_report(path, *, compare_vol_target=False, registry_root=None, trial_id=None):
    """Explicit paper accounting, with no historical scheduler/account grant."""
    import hashlib
    from copy import deepcopy
    from tools.backtester import _trend_event_encode, simulate_trend_events
    from tools.simulation_workspace import register_planned_trial, validate_trial_registry_options

    validate_trial_registry_options(registry_root, trial_id)
    if type(compare_vol_target) is not bool:
        raise ValueError("event comparison flag must be boolean")
    suffixes = ("off", "on") if compare_vol_target else ("explicit",)
    if registry_root is not None:
        for suffix in suffixes:
            validate_trial_registry_options(registry_root, trial_id + ":" + suffix)
    sources = _event_source_hashes()
    raw, trace = _event_trace_input(path)
    if (type(trace) is not dict or type(trace.get("strategy_params")) is not dict
            or type(trace["strategy_params"].get("vol_target")) is not bool
            or type(trace.get("signal_params")) is not dict
            or type(trace.get("execution_model")) is not dict):
        raise ValueError("event trace requires explicit strategy, signal and execution parameters")
    canonical = _trend_event_encode(trace)
    file_sha = hashlib.sha256(raw).hexdigest()
    comparable = deepcopy(trace)
    if compare_vol_target:
        comparable["strategy_params"].pop("vol_target")
    dataset = {"input_file_sha256": file_sha,
               "canonical_trace_sha256": hashlib.sha256(canonical).hexdigest(),
               "comparison_inputs_sha256": hashlib.sha256(_trend_event_encode(comparable)).hexdigest()}
    variants = []
    for suffix in suffixes:
        variant = deepcopy(trace)
        if compare_vol_target:
            variant["strategy_params"]["vol_target"] = suffix == "on"
        variants.append((suffix, variant))
    baseline = {"policy": "fixed_off" if compare_vol_target else "explicit_trace_no_comparison",
                "strategy_params": variants[0][1]["strategy_params"],
                "no_benchmark_performance_claim": True}
    trials = []
    if registry_root is not None:
        for suffix, variant in variants:
            trials.append(register_planned_trial(registry_root, trial_id + ":" + suffix,
                experiment_name="trend_explicit_event_replay", sources=sources, dataset=dataset,
                controls={name: variant[name] for name in ("strategy_params", "signal_params", "execution_model")},
                baseline=baseline,
                split_policy={"method": "explicit_ordered_trace_exploratory_only",
                              "historical_trace_provenance_verified": False,
                              "independent_holdout_verified": False}))
    results = [{"variant": suffix, "result": simulate_trend_events(variant)}
               for suffix, variant in variants]
    if _trend_event_encode(trace) != canonical or _event_trace_input(path)[0] != raw:
        raise ValueError("event trace changed during calculation")
    if _event_source_hashes() != sources:
        raise ValueError("event replay source changed during calculation")
    identity = {"dataset": dataset, "sources": sources,
                "variants": [{"name": suffix, "strategy_params": variant["strategy_params"]}
                             for suffix, variant in variants]}
    report = {"schema_version": 1, "kind": "trend_explicit_event_replay",
              "verdict": "NO_GO", "promotion_eligible": False,
              "historical_configuration_verified": False,
              "historical_trace_provenance_verified": False,
              "independent_holdout_verified": False,
              "comparison_policy": baseline["policy"], "dataset": dataset,
              "source_sha256": sources,
              "experiment_identity_sha256": hashlib.sha256(_trend_event_encode(identity)).hexdigest(),
              "variants": results}
    if trials:
        report["trial_registration"] = trials
    _trend_event_encode(report)
    return report


def _event_replay_main(arguments):
    import argparse
    import hashlib
    import sqlite3
    from bot_utils.atomic_publish import atomic_create_bytes
    from tools.native_ohlcv_snapshot import _plain_path
    from tools.backtester import _trend_event_encode
    from tools.simulation_workspace import add_trial_registry_arguments, validate_trial_registry_options

    parser = argparse.ArgumentParser(description="Offline TREND explicit event replay; research only")
    parser.add_argument("--event-trace", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="fresh immutable JSON report")
    parser.add_argument("--compare-vol-target", action="store_true")
    add_trial_registry_arguments(parser)
    try:
        args = parser.parse_args(arguments)
        validate_trial_registry_options(args.registry_root, args.trial_id)
        output = _plain_path(args.output)
        if output.exists():
            raise ValueError("event replay output must be fresh")
        report = _event_replay_report(args.event_trace, compare_vol_target=args.compare_vol_target,
                                     registry_root=args.registry_root, trial_id=args.trial_id)
        raw = _trend_event_encode(report)
        if not atomic_create_bytes(output, raw):
            raise ValueError("event replay output already exists")
        print(json.dumps({"report": str(output), "sha256": hashlib.sha256(raw).hexdigest(),
                          "variants": len(report["variants"]), "promotion_eligible": False}, sort_keys=True))
        return 0
    except SystemExit as exc:
        return int(exc.code)
    except (OSError, ValueError, TypeError, OverflowError, RecursionError, RuntimeError, sqlite3.Error) as exc:
        print(f"Invalid event replay: {_safe_exc(exc)}")
        return 2


def main():
    if any(arg in {"--event-trace", "--compare-vol-target"} for arg in sys.argv[1:]):
        return _event_replay_main(sys.argv[1:])
    if any(arg in {"--sizing-snapshot", "--sizing-params"} for arg in sys.argv[1:]):
        return _sizing_diagnostic_main(sys.argv[1:])
    try:
        args, runtime_params = _extract_runtime_options(sys.argv[1:])
    except (OSError, ValueError, TypeError):
        print("  Invalid runtime signal input: supply six effective values or a sanitized snapshot")
        return 2
    unknown_options = [arg for arg in args if arg.startswith("--") and arg != "--sweep"]
    if unknown_options:
        print(f"  Unknown option: {unknown_options[0]}")
        return 2
    do_sweep = "--sweep" in args
    if do_sweep and runtime_params is not None:
        print("  Runtime signal mode and research sweep must run separately")
        return 2
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
    if runtime_params is not None:
        from trading.trend_signal import bars_required

        if days < bars_required(runtime_params):
            print(f"  Invalid days argument: runtime history requires {bars_required(runtime_params)} closed bars")
            return 2
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
    configs = CONFIGS
    if runtime_params is not None:
        def runtime_signal(closes, _highs, _lows):
            return np.asarray([decision[0] for decision in _runtime_signal_trace(closes, runtime_params)], dtype=bool)

        configs = [("Runtime TREND", runtime_signal)]
        values = ",".join(str(getattr(runtime_params, field)) for _key, field in _RUNTIME_FIELDS)
        print("  TREND RUNTIME SIGNAL COMPARISON (explicit parameter snapshot)")
        print(f"  effective fast,slow,cross_fast,cross_slow,vote_min,exit_vote={values}")
        print(f"  history guard: {bars_required(runtime_params)} closed daily bars; initial warmup stays flat")
        print("  Signal parity only: daily-close returns and switch costs are a proxy.")
        print("  No execution parity: voltarget/sizing, cooldown/reentry, 20s safety,")
        print("  killswitch/recovery, fees/fills/claims and portfolio effects are not simulated.")
        print("  This supplied snapshot is not proof of historical runtime configuration.")
    else:
        print("  TREND RESEARCH VARIANTS (long/flat daily proxies, vs buy-and-hold)")
        print("  Generic rules/sweeps do not reproduce the configured runtime strategy.")
    print(
        f"  {days}d daily | cost {COST * 100:.2f}%/switch | "
        f"rules: {', '.join(n for n, _ in configs)}"
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
        if runtime_params is not None:
            market = markets[sym]
            if (
                not isinstance(market, dict) or market.get("spot") is not True
                or market.get("type") not in (None, "spot")
                or any(market.get(flag) is not None and market.get(flag) is not False
                       for flag in ("swap", "future", "contract"))
                or market.get("symbol") not in (None, sym)
            ):
                print(f"  {coin}: no confirmed spot market  skipped")
                continue
            ohlc = _fetch_ohlc(ex, sym, days, runtime_signal=True)
        else:
            ohlc = _fetch_ohlc(ex, sym, days)
        if len(ohlc) < days:
            print(f"  {coin}: only {len(ohlc)} daily bars  skipped")
            continue
        if runtime_params is not None:
            # Runtime signal truth uses closes only; do not invent high/low
            # values or impose an unrelated OHLC strategy's validation rules.
            data[coin] = (np.asarray([float(row[4]) for row in ohlc]), None, None)
        else:
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
    for name, fn in configs:
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

    print("\n  READ-OUT: historical signal-proxy comparisons; no profitability or execution claim.")

    if do_sweep:
        print("\n" + "=" * 90)
        print("  RESEARCH PARAMETER SWEEP (not runtime parity)")
        print("=" * 90)
        _print_sweep(data)

    print("\n  NOTE: 'SAFER' = kept most upside with much smaller drawdown  the")
    print("  classic trend-following benefit. 'WORSE' = whipsawed vs just holding.")
    print(f"  Generated: {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    print("=" * 90)
    return 0


if __name__ == "__main__":
    sys.exit(main())
