"""Cross-sectional (relative-strength) momentum test.

Hypothese: long die strksten K Coins / short die schwchsten K (marktneutral),
periodisch rebalancen. Marktneutral  Edge unabhngig von der Marktrichtung
(funktioniert auch im aktuellen Angst-Markt). Diversifiziert  kein Einzelcoin-
Tail. Misst NETTO nach konservativen Kosten (Round-Trip 0.40%, one-way 0.20%
pro Namens-Rotation).

Run: PYTHONIOENCODING=utf-8 python -m tools.xsec_momentum [days]
"""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

import argparse
import json
import math
import os
import statistics
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from numbers import Real
from pathlib import Path

_TOOL_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TOOL_PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _TOOL_PROJECT_ROOT)

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

import pandas as pd

from tools.backtester import connect_exchange, get_top_volume_coins, fetch_history
from tools.simulation_workspace import load_history_dataset
from tools.trend_check import _safe_exc
from trading.xsec_signal import (
    XSecParams,
    advance_crash_history,
    compute_target_book,
)

DEFAULT_DAYS = 180
MAX_DAYS = 3650
MAX_XSEC_REPORT_BYTES = 64 * 1024 * 1024
MIN_ONLINE_ROWS = 4 * (24 + 24 + 1)
MIN_ONLINE_COINS = 2 * 8 + 2


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: Path, *, label: str) -> Path:
    requested = path.expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} path must not contain links")
    return requested


def _sync_directory(path: Path) -> None:
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(str(path), flags)
    except AttributeError:
        return
    except OSError as exc:
        if os.name == "nt":
            if isinstance(exc, PermissionError):
                return
            if (
                isinstance(exc, FileNotFoundError)
                and path == Path(path.anchor)
                and path.is_dir()
            ):
                return
        raise
    primary_error: BaseException | None = None
    try:
        os.fsync(directory_fd)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            os.close(directory_fd)
        except BaseException as close_error:
            if primary_error is None:
                raise
            try:
                primary_error.add_note(
                    "CROSS replay directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _sync_parent_chain(path: Path) -> None:
    parent = path.parent
    while True:
        _sync_directory(parent)
        if parent == parent.parent:
            break
        parent = parent.parent


def _write_immutable_report(path: Path, encoded: bytes) -> Path:
    if not isinstance(encoded, bytes) or len(encoded) > MAX_XSEC_REPORT_BYTES:
        raise ValueError("CROSS replay output is oversized")
    path = _absolute_without_links(path, label="CROSS replay output")
    path.parent.mkdir(parents=True, exist_ok=True)
    path = _absolute_without_links(path, label="CROSS replay output")
    _sync_parent_chain(path.parent)

    def existing_matches() -> bool:
        if _is_linklike(path) or not path.is_file():
            return False
        try:
            if path.stat().st_size != len(encoded):
                return False
            with path.open("rb") as handle:
                return handle.read(len(encoded) + 1) == encoded
        except OSError:
            return False

    if path.exists() or _is_linklike(path):
        if existing_matches():
            _sync_directory(path.parent)
            return path
        raise FileExistsError("immutable CROSS replay output conflict")

    temporary: Path | None = None
    temporary_owned = False
    primary_error: BaseException | None = None
    result: Path | None = None
    handle = None
    try:
        for attempt in range(3):
            candidate = path.with_name(
                f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            )
            candidate = _absolute_without_links(
                candidate, label="temporary CROSS replay output"
            )
            try:
                handle = candidate.open("xb")
            except FileExistsError:
                if attempt == 2:
                    raise
                continue
            temporary = candidate
            temporary_owned = True
            break
        if handle is None or temporary is None:
            raise RuntimeError("CROSS replay temporary allocation failed")
        try:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if primary_error is None:
                    primary_error = close_error
                    raise
                try:
                    primary_error.add_note(
                        "CROSS replay temporary close failed: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        path = _absolute_without_links(path, label="CROSS replay output")
        _absolute_without_links(temporary, label="temporary CROSS replay output")
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            if existing_matches():
                result = path
            else:
                raise FileExistsError(
                    "immutable CROSS replay output conflict"
                ) from exc
        else:
            result = path
    except BaseException as exc:
        if primary_error is None:
            primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        if temporary_owned and temporary is not None:
            for _attempt in range(2):
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    temporary_owned = False
                    break
                except OSError as exc:
                    cleanup_error = exc
                except BaseException as exc:
                    cleanup_error = exc
                    break
                else:
                    temporary_owned = False
                    break
            if not temporary_owned:
                cleanup_error = None
        sync_error: BaseException | None = None
        if temporary is not None:
            try:
                _sync_directory(path.parent)
            except BaseException as exc:
                sync_error = exc
        if primary_error is not None:
            for label, secondary_error in (
                ("temporary cleanup", cleanup_error),
                ("directory sync", sync_error),
            ):
                if secondary_error is None:
                    continue
                try:
                    primary_error.add_note(
                        f"CROSS replay {label} failed: "
                        f"{type(secondary_error).__name__}: {secondary_error}"
                    )
                except BaseException:
                    pass
        elif sync_error is not None:
            if cleanup_error is not None:
                try:
                    sync_error.add_note(
                        "CROSS replay temporary cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
            raise sync_error
        elif cleanup_error is not None:
            raise cleanup_error
    if result is None:
        raise RuntimeError("CROSS replay publish did not complete")
    return result


def _parse_xsec_days(args=None) -> int:
    values = sys.argv[1:] if args is None else list(args)
    if not values:
        return DEFAULT_DAYS
    try:
        days = int(values[0])
    except (TypeError, ValueError, OverflowError):
        return DEFAULT_DAYS
    return days if 1 <= days <= MAX_DAYS else DEFAULT_DAYS


def _parse_online_days(args: list[str]) -> int | None:
    if len(args) > 1:
        return None
    if not args:
        return DEFAULT_DAYS
    try:
        days = int(args[0])
    except (TypeError, ValueError, OverflowError):
        return None
    return days if 1 <= days <= MAX_DAYS else None


def _valid_online_universe(coins: object) -> bool:
    return (
        isinstance(coins, list)
        and 1 <= len(coins) <= 120
        and all(
            isinstance(symbol, str)
            and symbol.isascii()
            and symbol == symbol.upper()
            and symbol.count("/") == 1
            and symbol.endswith("/USDT")
            and 1 <= len(symbol.split("/", 1)[0]) <= 20
            and symbol.split("/", 1)[0].isalnum()
            for symbol in coins
        )
        and len(set(coins)) == len(coins)
    )


def _valid_online_hourly_grid(index: object) -> bool:
    return (
        isinstance(index, pd.DatetimeIndex)
        and index.tz is not None
        and str(index.tz).upper() == "UTC"
        and not index.hasnans
        and index.is_unique
        and index.is_monotonic_increasing
        and len(index) >= 2
        and bool(
            ((index[1:] - index[:-1]) == pd.Timedelta(hours=1)).all()
        )
    )


def _valid_online_freshness(index: pd.DatetimeIndex) -> bool:
    now = pd.Timestamp.now(tz="UTC")
    latest = index[-1]
    return (
        now - pd.Timedelta(hours=3)
        <= latest
        <= now + pd.Timedelta(minutes=5)
    )


def _valid_online_price_data(prices: pd.DataFrame) -> bool:
    if any(
        not pd.api.types.is_numeric_dtype(dtype)
        or pd.api.types.is_bool_dtype(dtype)
        for dtype in prices.dtypes
    ):
        return False
    return all(
        math.isfinite(float(value)) and float(value) > 0.0
        for value in prices.to_numpy().flat
    )


def _valid_online_panel_symbols(prices: pd.DataFrame, coins: list[str]) -> bool:
    columns = list(prices.columns)
    return (
        prices.columns.is_unique
        and all(isinstance(column, str) for column in columns)
        and set(columns).issubset(coins)
    )


def _valid_online_returns(returns: object) -> bool:
    return (
        isinstance(returns, list)
        and bool(returns)
        and all(
            isinstance(value, Real)
            and not isinstance(value, bool)
            and math.isfinite(float(value))
            and float(value) > -1.0
            for value in returns
        )
    )


def _validated_online_stats(returns: list) -> tuple[float, float, float, int] | None:
    try:
        result = equity_stats(returns)
    except Exception as exc:
        print(f"Simulation failed: {_safe_exc(exc)}")
        return None
    if not isinstance(result, tuple) or len(result) != 4:
        print("Invalid simulation statistics; cannot produce a reliable report")
        return None
    total, drawdown, sharpe, periods = result
    numeric = (total, drawdown, sharpe)
    if (
        any(
            not isinstance(value, Real)
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            for value in numeric
        )
        or float(total) < -100.0
        or not 0.0 <= float(drawdown) <= 100.0
        or isinstance(periods, bool)
        or not isinstance(periods, int)
        or periods != len(returns)
    ):
        print("Invalid simulation statistics; cannot produce a reliable report")
        return None
    return float(total), float(drawdown), float(sharpe), periods


FEE_ONE_WAY = 0.0006  # futures taker 0.01% + slippage 0.05% per side


def _canonical_exchange(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("dataset exchange provenance is missing")
    normalized = "".join(char for char in value.lower() if char.isalnum())
    return {"mexcglobal": "mexc"}.get(normalized, normalized)


def _validate_xsec_dataset_manifest(manifest: dict, expected_exchange: str) -> dict:
    if not isinstance(manifest, dict):
        raise ValueError("dataset manifest is invalid")
    payload = manifest.get("fingerprint_payload")
    provenance = manifest.get("provenance")
    if not isinstance(payload, dict) or payload.get("kind") != "optimizer_ohlcv_1h":
        raise ValueError("CROSS replay requires an immutable 1h optimizer dataset")
    if not isinstance(provenance, dict):
        raise ValueError("dataset provenance is missing")
    recorded = provenance.get("exchange_id") or provenance.get("exchange")
    actual = _canonical_exchange(recorded)
    expected = _canonical_exchange(expected_exchange)
    if actual != expected:
        raise ValueError(
            f"dataset exchange mismatch: expected {expected}, found {actual}",
        )
    return {
        "exchange": actual,
        "survivorship_bias": provenance.get("survivorship_bias") is True,
    }


def _panel_from_history(history: dict) -> pd.DataFrame:
    series = {}
    for symbol in sorted(history):
        frame = history[symbol]
        validated = _validated_close_series(frame)
        if validated is None:
            raise ValueError(f"invalid replay series: {symbol}")
        series[symbol] = validated
    panel = pd.DataFrame(series).sort_index()
    if panel.empty or not panel.index.is_monotonic_increasing or not panel.index.is_unique:
        raise ValueError("replay panel has an invalid UTC timeline")
    return panel


def _target_weights(book, k: int) -> dict[str, float]:
    raw_exposure = book.exposure_mult
    if isinstance(raw_exposure, bool):
        raise ValueError("replay exposure must be finite and within [0, 1]")
    try:
        exposure = float(raw_exposure)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "replay exposure must be finite and within [0, 1]"
        ) from exc
    if not math.isfinite(exposure) or not 0.0 <= exposure <= 1.0:
        raise ValueError("replay exposure must be finite and within [0, 1]")
    if exposure <= 0.0:
        return {}
    if len(book.longs) != len(book.shorts) or len(book.longs) != k:
        raise ValueError("replay target book is not fully dollar-neutral")
    if (
        any(not isinstance(symbol, str) or not symbol for symbol in book.longs)
        or any(not isinstance(symbol, str) or not symbol for symbol in book.shorts)
        or len(set(book.longs)) != k
        or len(set(book.shorts)) != k
        or not set(book.longs).isdisjoint(book.shorts)
    ):
        raise ValueError("replay target symbols must be unique and disjoint")
    leg_weight = 0.5 * exposure / k
    weights = {symbol: leg_weight for symbol in book.longs}
    weights.update({symbol: -leg_weight for symbol in book.shorts})
    return weights


def _weight_turnover(previous: dict[str, float], target: dict[str, float]) -> float:
    value = math.fsum(
        abs(target.get(symbol, 0.0) - previous.get(symbol, 0.0))
        for symbol in set(previous) | set(target)
    )
    if not math.isfinite(value) or value < 0.0:
        raise ValueError("non-finite portfolio turnover")
    return value


def run_stateful_replay(
    prices: pd.DataFrame,
    lookback: int,
    rebalance: int,
    k: int,
    *,
    fee_one_way: float = FEE_ONE_WAY,
    funding_per_day: float = 0.0006,
    crash_filter: bool = True,
    crash_window: int = 4,
    initial_recent_returns: list[float] | None = None,
    liquidate_at_end: bool = True,
) -> dict:
    """Replay the live CROSS signal as an anchored, stateful portfolio."""
    integers = (lookback, rebalance, k, crash_window)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in integers):
        raise ValueError("CROSS replay periods and K must be integers")
    if lookback <= 0 or rebalance <= 0 or k <= 0 or crash_window <= 0:
        raise ValueError("CROSS replay periods and K must be positive")
    for name, value in (
        ("fee_one_way", fee_one_way),
        ("funding_per_day", funding_per_day),
    ):
        if isinstance(value, bool):
            raise ValueError(f"{name} must be finite and non-negative")
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be finite and non-negative") from exc
        if not math.isfinite(numeric) or numeric < 0.0:
            raise ValueError(f"{name} must be finite and non-negative")
    fee_one_way = float(fee_one_way)
    funding_per_day = float(funding_per_day)
    if not isinstance(crash_filter, bool) or not isinstance(liquidate_at_end, bool):
        raise ValueError("CROSS replay flags must be boolean")
    if not isinstance(prices, pd.DataFrame) or prices.empty:
        raise ValueError("prices must be a non-empty DataFrame")
    if not prices.index.is_monotonic_increasing or not prices.index.is_unique:
        raise ValueError("prices must have a unique ascending timeline")
    if (
        not isinstance(prices.index, pd.DatetimeIndex)
        or prices.index.tz is None
        or str(prices.index.tz).upper() != "UTC"
        or prices.index.isna().any()
    ):
        raise ValueError("prices must have a timezone-aware UTC timeline")
    if any(
        current - previous != pd.Timedelta(hours=1)
        for previous, current in zip(prices.index, prices.index[1:])
    ):
        raise ValueError("prices must have a contiguous hourly timeline")

    recent = advance_crash_history(
        list(initial_recent_returns or []),
        None,
        was_crash_flat=False,
        slot_advanced=False,
    )
    params = XSecParams(
        lookback_hours=lookback,
        k_per_side=k,
        crash_filter=crash_filter,
        crash_window=crash_window,
    )
    previous_weights: dict[str, float] = {}
    periods = []
    index = lookback
    while index + rebalance < len(prices):
        histories = {
            symbol: prices[symbol].iloc[: index + 1].tolist()
            for symbol in prices.columns
        }
        book = compute_target_book(histories, recent, params)
        target = _target_weights(book, k)
        entry = prices.iloc[index]
        exit_row = prices.iloc[index + rebalance]
        gross = 0.0
        for symbol, weight in target.items():
            p0 = float(entry[symbol])
            p1 = float(exit_row[symbol])
            if not all(math.isfinite(value) and value > 0.0 for value in (p0, p1)):
                raise ValueError(f"selected replay leg has a missing price: {symbol}")
            gross += weight * (p1 / p0 - 1.0)
        turnover = _weight_turnover(previous_weights, target)
        fees = turnover * fee_one_way
        funding = (
            funding_per_day * (rebalance / 24.0)
            if target
            else 0.0
        )
        net = gross - fees - funding
        if not all(math.isfinite(value) for value in (gross, fees, funding, net)):
            raise ValueError("non-finite CROSS replay accounting")
        net = _period_return(net)
        periods.append(
            {
                "entry_utc": prices.index[index].isoformat(),
                "exit_utc": prices.index[index + rebalance].isoformat(),
                "exposure": float(book.exposure_mult),
                "longs": list(book.longs),
                "shorts": list(book.shorts),
                "turnover": turnover,
                "gross": gross,
                "fees": fees,
                "funding": funding,
                "net": net,
            },
        )
        recent = advance_crash_history(
            recent,
            gross if target else None,
            was_crash_flat=not target,
            slot_advanced=True,
        )
        previous_weights = target
        index += rebalance

    if not periods:
        raise ValueError("dataset is too short for the requested CROSS replay")
    terminal_turnover = 0.0
    if liquidate_at_end and previous_weights:
        terminal_turnover = _weight_turnover(previous_weights, {})
        terminal_fee = terminal_turnover * fee_one_way
        periods[-1]["turnover"] += terminal_turnover
        periods[-1]["fees"] += terminal_fee
        periods[-1]["net"] = _period_return(
            periods[-1]["net"] - terminal_fee
        )

    return {
        "periods": periods,
        "period_count": len(periods),
        "terminal_turnover": terminal_turnover,
        "recent_returns": recent,
    }


def _period_return(value) -> float:
    if isinstance(value, bool):
        raise ValueError("period return must be finite and at least -100%")
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("period return must be finite and at least -100%") from exc
    if not math.isfinite(numeric) or numeric < -1.0:
        raise ValueError("period return must be finite and at least -100%")
    return numeric


def _replay_metrics(periods: list[dict]) -> dict:
    if not periods:
        return {
            "periods": 0,
            "gross": 0.0,
            "fees": 0.0,
            "funding": 0.0,
            "net": 0.0,
            "max_drawdown": 0.0,
            "positive_periods": 0,
        }
    normalized = []
    for period in periods:
        if not isinstance(period, dict):
            raise ValueError("period return evidence is invalid")
        values = {}
        for field in ("gross", "fees", "funding", "net"):
            value = period.get(field)
            if isinstance(value, bool):
                raise ValueError("period return evidence is invalid")
            try:
                numeric = float(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("period return evidence is invalid") from exc
            if not math.isfinite(numeric):
                raise ValueError("period return must be finite and at least -100%")
            values[field] = numeric
        values["net"] = _period_return(values["net"])
        normalized.append(values)
    equity = peak = 1.0
    max_drawdown = 0.0
    for period in normalized:
        equity *= 1.0 + period["net"]
        if not math.isfinite(equity):
            raise ValueError("period return compounding is non-finite")
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, (peak - equity) / peak)
    return {
        "periods": len(periods),
        "gross": math.fsum(period["gross"] for period in normalized),
        "fees": math.fsum(period["fees"] for period in normalized),
        "funding": math.fsum(period["funding"] for period in normalized),
        "net": math.fsum(period["net"] for period in normalized),
        "compounded_return": equity - 1.0,
        "max_drawdown": max_drawdown,
        "positive_periods": sum(period["net"] > 0.0 for period in normalized),
    }


def _offline_replay(argv: list[str]) -> dict:
    parser = argparse.ArgumentParser(description="Immutable MEXC CROSS replay")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--exchange", default="mexc")
    parser.add_argument("--lookback", type=int, default=24)
    parser.add_argument("--rebalance", type=int, default=48)
    parser.add_argument("--k", type=int, default=2)
    parser.add_argument("--fee-one-way", type=float, default=0.0006)
    parser.add_argument("--funding-per-day", type=float, default=0.0006)
    parser.add_argument("--crash-window", type=int, default=4)
    parser.add_argument("--no-crash-filter", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args(argv)

    history, manifest = load_history_dataset(args.dataset)
    provenance = _validate_xsec_dataset_manifest(manifest, args.exchange)
    replay = run_stateful_replay(
        _panel_from_history(history),
        args.lookback,
        args.rebalance,
        args.k,
        fee_one_way=args.fee_one_way,
        funding_per_day=args.funding_per_day,
        crash_filter=not args.no_crash_filter,
        crash_window=args.crash_window,
    )
    periods = replay["periods"]
    train_end = max(1, int(len(periods) * 0.60))
    validation_end = max(train_end + 1, int(len(periods) * 0.80))
    validation_end = min(validation_end, len(periods))
    report = {
        "schema_version": 1,
        "classification": "exploratory_only",
        "dataset_fingerprint": manifest["dataset_fingerprint"],
        "dataset": provenance,
        "parameters": {
            "lookback_hours": args.lookback,
            "rebalance_hours": args.rebalance,
            "k_per_side": args.k,
            "fee_one_way": args.fee_one_way,
            "funding_per_day": args.funding_per_day,
            "crash_filter": not args.no_crash_filter,
            "crash_window": args.crash_window,
            "liquidate_at_end": True,
        },
        "metrics": {
            "all": _replay_metrics(periods),
            "train": _replay_metrics(periods[:train_end]),
            "validation": _replay_metrics(periods[train_end:validation_end]),
            "holdout": _replay_metrics(periods[validation_end:]),
        },
        "terminal_turnover": replay["terminal_turnover"],
        "periods": periods,
        "limitations": [
            "survivorship_biased_universe"
            if provenance["survivorship_bias"]
            else "point_in_time_universe_not_proven",
            "fixed_conservative_funding_drag",
            "close_only_execution_without_orderbook_depth",
        ],
    }
    raw = json.dumps(report, allow_nan=False, separators=(",", ":"), sort_keys=True)
    if args.output:
        _write_immutable_report(Path(args.output), raw.encode("utf-8"))
    return report


def _validated_close_series(df):
    if not isinstance(df, pd.DataFrame) or not {"dt", "close"}.issubset(df.columns):
        return None
    frame = df.loc[:, ["dt", "close"]].copy()
    if frame.empty or frame["close"].map(lambda value: isinstance(value, bool)).any():
        return None
    frame["dt"] = pd.to_datetime(frame["dt"], errors="coerce", utc=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    valid = (
        frame["dt"].notna()
        & frame["dt"].eq(frame["dt"].dt.floor("h"))
        & frame["close"].map(
            lambda value: math.isfinite(float(value)) and float(value) > 0.0
        )
    )
    if not valid.all():
        return None
    duplicate_rows = frame[frame.duplicated(subset="dt", keep=False)]
    if any(
        group["close"].nunique(dropna=False) > 1
        for _, group in duplicate_rows.groupby("dt")
    ):
        return None
    frame = frame.drop_duplicates(subset="dt", keep="first").sort_values("dt")
    return frame.set_index("dt")["close"].astype(float)


def load_panel(ex, coins, days=DEFAULT_DAYS):
    if not _valid_online_universe(coins):
        raise ValueError("history universe is invalid")
    if (
        isinstance(days, bool)
        or not isinstance(days, int)
        or not 1 <= days <= MAX_DAYS
    ):
        raise ValueError(f"history days must be an integer in [1, {MAX_DAYS}]")
    hist = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(fetch_history, ex, c, days): c for c in coins}
        for f in as_completed(futs):
            try:
                df = f.result()
                series = _validated_close_series(df)
                if series is not None:
                    hist[futs[f]] = series
            except Exception:
                pass
    if not hist:
        return pd.DataFrame()
    panel = pd.DataFrame({symbol: hist[symbol] for symbol in sorted(hist)}).sort_index()
    panel = panel.resample("1h").last().ffill(limit=3)
    return panel


def run(prices, L, reb, K, fee=FEE_ONE_WAY, fund_day=0.0):
    """L=lookback (h), reb=rebalance (h), K=Korbgre je Seite,
    fee=one-way Kosten, fund_day=Funding-Drag pro Tag auf dem Buch."""
    if not isinstance(prices, pd.DataFrame) or prices.empty:
        raise ValueError("prices must be a non-empty DataFrame")
    columns = list(prices.columns)
    if (
        not prices.columns.is_unique
        or any(not isinstance(column, str) or not column for column in columns)
    ):
        raise ValueError("price symbols must be unique non-empty strings")
    if not _valid_online_price_data(prices):
        raise ValueError("price data must be numeric, finite, and positive")
    controls = (L, reb, K)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in controls):
        raise ValueError("lookback, rebalance, and K must be integers")
    if L <= 0 or reb <= 0 or K <= 0:
        raise ValueError("lookback, rebalance, and K must be positive")
    for name, value in (("fee", fee), ("fund_day", fund_day)):
        if isinstance(value, bool):
            raise ValueError(f"{name} must be finite and non-negative")
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be finite and non-negative") from exc
        if not math.isfinite(numeric) or numeric < 0.0:
            raise ValueError(f"{name} must be finite and non-negative")
    fee = float(fee)
    fund_day = float(fund_day)
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
        ranked = sorted(
            past.index,
            key=lambda symbol: (-float(past[symbol]), symbol),
        )
        longs = ranked[:K]
        shorts = ranked[-K:]
        long_ret = float(fwd_row[longs].mean())
        short_ret = float(fwd_row[shorts].mean())

        ch_l = len(set(longs) ^ prev_long)
        ch_s = len(set(shorts) ^ prev_short)
        cost = 0.5 * (ch_l / K) * fee + 0.5 * (ch_s / K) * fee
        prev_long, prev_short = set(longs), set(shorts)

        ls_rets.append(0.5 * (long_ret - short_ret) - cost - funding)
        lo_rets.append(long_ret - (ch_l / K) * fee)
        i += reb
    if ls_rets:
        terminal_ls_cost = (
            0.5 * (len(prev_long) / K) * fee
            + 0.5 * (len(prev_short) / K) * fee
        )
        terminal_lo_cost = (len(prev_long) / K) * fee
        ls_rets[-1] -= terminal_ls_cost
        lo_rets[-1] -= terminal_lo_cost
    if any(
        not math.isfinite(value) or value <= -1.0
        for value in (*ls_rets, *lo_rets)
    ):
        raise ValueError("simulation return must be finite and greater than -100%")
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
    rets = [_period_return(value) for value in rets]
    eq = 1.0
    for r in rets:
        eq *= 1 + r
        if not math.isfinite(eq):
            raise ValueError("period return compounding is non-finite")
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
    rets = [_period_return(value) for value in rets]
    eq = peak = 1.0
    mdd = 0.0
    for r in rets:
        eq *= 1 + r
        if not math.isfinite(eq):
            raise ValueError("period return compounding is non-finite")
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak)
    sd = statistics.stdev(rets) if len(rets) > 1 else 0
    sh = (statistics.mean(rets) / sd * math.sqrt(len(rets))) if sd > 0 else 0.0
    return ((eq - 1) * 100, mdd * 100, sh, len(rets))


def apply_filter(rets, kind, W=4, target=0.04):
    """Crash-Filter als Exposure-Overlay  nutzt NUR vergangene Returns (kein
    Look-Ahead). voltarget: Exposure ~ target/recent_vol (cap 1.0). ownmom:
    flach wenn letzte W Rebalances im Schnitt negativ. combo: beide."""
    if kind not in ("none", "voltarget", "ownmom", "combo"):
        raise ValueError("filter kind is invalid")
    if isinstance(W, bool) or not isinstance(W, int) or W < 2:
        raise ValueError("filter window must be an integer of at least two")
    if isinstance(target, bool):
        raise ValueError("filter target must be finite and non-negative")
    try:
        target = float(target)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("filter target must be finite and non-negative") from exc
    if not math.isfinite(target) or target < 0.0:
        raise ValueError("filter target must be finite and non-negative")
    if not _valid_online_returns(rets):
        raise ValueError("filter returns must be a non-empty finite return list")
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


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    if "--dataset" in args:
        report = _offline_replay(args)
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True))
        return 0
    days = _parse_online_days(args)
    if days is None:
        print(f"Usage: python -m tools.xsec_momentum [days: 1..{MAX_DAYS}]")
        return 2
    try:
        ex = connect_exchange()
        coins = get_top_volume_coins(ex, n=120)  # breiteres Universum
    except Exception as exc:
        print(f"Exchange setup failed: {_safe_exc(exc)}")
        return 1
    if not _valid_online_universe(coins):
        print("Invalid or empty universe; cannot load reliable history")
        return 1
    print(f"Coins angefragt: {len(coins)} | loading {days}d ...")
    try:
        prices = load_panel(ex, coins, days)
    except Exception as exc:
        print(f"History load failed: {_safe_exc(exc)}")
        return 1
    if not isinstance(prices, pd.DataFrame) or prices.empty:
        print("Insufficient history for a reliable XSEC online report")
        return 1
    if not _valid_online_panel_symbols(prices, coins):
        print("Invalid panel symbols; history provenance is not reliable")
        return 1

    #  De-Bias: nur Coins mit VOLLER Historie (am Anfang UND Ende vorhanden)
    head = prices.iloc[:48].notna().all()
    tail = prices.iloc[-48:].notna().all()
    stable = prices.loc[:, head & tail]
    if len(stable) < MIN_ONLINE_ROWS or stable.shape[1] < MIN_ONLINE_COINS:
        print("Insufficient history for a reliable XSEC online report")
        return 1
    if not _valid_online_hourly_grid(stable.index):
        print("Invalid hourly grid; cannot run a reliable XSEC online report")
        return 1
    if not _valid_online_freshness(stable.index):
        print("Stale or future history; cannot run a reliable XSEC online report")
        return 1
    if not _valid_online_price_data(stable):
        print("Invalid price data; cannot run a reliable XSEC online report")
        return 1
    if len(stable) < days * 24:
        print("History is shorter than requested; cannot run a reliable XSEC online report")
        return 1
    print(
        f"Voll-Historie-Universum: {stable.shape[1]} von {prices.shape[1]} Coins "
        f"(frisch gelistete Pumper entfernt)"
    )
    if "BTC/USDT" in stable.columns:
        b = stable["BTC/USDT"]
        print(f"Benchmark BTC B&H: {(b.iloc[-1] / b.iloc[0] - 1) * 100:+.1f}%")

    REAL_FEE, REAL_FUND = 0.0006, 0.0006
    L, reb, K = 24, 72, 8

    #  B) CRASH-FILTER: Vergleich auf dem GANZEN Fenster
    try:
        base_result = run(stable, L, reb, K, REAL_FEE, REAL_FUND)
    except Exception as exc:
        print(f"Simulation failed: {_safe_exc(exc)}")
        return 1
    expected_periods = (len(stable) - L - 1) // reb
    if (
        not isinstance(base_result, tuple)
        or len(base_result) != 2
        or not _valid_online_returns(base_result[0])
        or len(base_result[0]) != expected_periods
    ):
        print("Invalid baseline returns; cannot produce a reliable report")
        return 1
    base = base_result[0]
    print(
        f"\n#### B) CRASH-FILTER  (Config L={L} reb={reb} K={K}, "
        f"fee {REAL_FEE * 100:.2f}% + funding {REAL_FUND * 100:.2f}%/d) ####"
    )
    print(f"{'Filter':>12} | {'total%':>8} {'maxDD%':>7} {'Sharpe':>7}")
    print("-" * 42)
    for kind in ("none", "voltarget", "ownmom", "combo"):
        try:
            filtered = apply_filter(base, kind)
        except Exception as exc:
            print(f"Simulation failed: {_safe_exc(exc)}")
            return 1
        if not _valid_online_returns(filtered) or len(filtered) != len(base):
            print("Invalid filtered returns; cannot produce a reliable report")
            return 1
        result = _validated_online_stats(filtered)
        if result is None:
            return 1
        t, dd, sh, _ = result
        print(f"{kind:>12} | {t:>+7.1f}% {dd:>6.1f}% {sh:>+6.2f}")

    #  Walk-Forward mit dem besten Filter (combo) vs ohne
    n = stable.shape[0]
    nwin = max(4, n // (45 * 24))  # ~45d je Fenster
    seg = n // nwin
    print(
        f"\n#### WALK-FORWARD  (~{seg // 24}d/Fenster, baseline vs combo-Filter) ####"
    )
    all_b, all_f = [], []
    for w in range(1, nwin):
        tr = stable.iloc[(w - 1) * seg : w * seg]
        te = stable.iloc[w * seg : (w + 1) * seg]
        try:
            cfg = best_config(tr, REAL_FEE, REAL_FUND)
            if cfg is None:
                continue
            if not isinstance(cfg, tuple) or cfg not in GRID:
                print(
                    "Invalid walk-forward configuration; "
                    "cannot produce a reliable report"
                )
                return 1
            raw_result = run(te, *cfg, REAL_FEE, REAL_FUND)
        except Exception as exc:
            print(f"Walk-forward failed: {_safe_exc(exc)}")
            return 1
        expected_periods = (len(te) - cfg[0] - 1) // cfg[1]
        if (
            not isinstance(raw_result, tuple)
            or len(raw_result) != 2
            or not _valid_online_returns(raw_result[0])
            or len(raw_result[0]) != expected_periods
        ):
            print("Invalid walk-forward returns; cannot produce a reliable report")
            return 1
        raw = raw_result[0]
        try:
            filt = apply_filter(raw, "combo")
        except Exception as exc:
            print(f"Walk-forward failed: {_safe_exc(exc)}")
            return 1
        if not _valid_online_returns(filt) or len(filt) != len(raw):
            print(
                "Invalid walk-forward filtered returns; "
                "cannot produce a reliable report"
            )
            return 1
        sb = _validated_online_stats(raw)
        if sb is None:
            return 1
        sf = _validated_online_stats(filt)
        if sf is None:
            return 1
        all_b += raw
        all_f += filt
        print(
            f"  Fenster {w} (cfg L={cfg[0]} reb={cfg[1]} K={cfg[2]}):  "
            f"baseline {sb[0]:>+6.1f}% (DD {sb[1]:.0f}%)   "
            f"combo-Filter {sf[0]:>+6.1f}% (DD {sf[1]:.0f}%)"
        )
    if not all_b or not all_f:
        print("No valid walk-forward windows; cannot produce a reliable report")
        return 1
    tb = _validated_online_stats(all_b)
    if tb is None:
        return 1
    tf = _validated_online_stats(all_f)
    if tf is None:
        return 1
    print(
        f"  kombiniert OOS:  baseline {tb[0]:+.1f}% (maxDD {tb[1]:.0f}%)  "
        f"combo-Filter {tf[0]:+.1f}% (maxDD {tf[1]:.0f}%)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
