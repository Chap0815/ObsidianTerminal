"""Run checkpointed cost and leakage validation on a MEXC capture dataset."""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import secrets
import stat
import sys
from collections import Counter
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

from tools.backtester import calc_round_trip, simulate_fast
from tools.futures_capture_replay import _json_safe
from tools.simulation_workspace import ReproducibleRun, canonical_evidence_sha256
from core.constants import SIM_CAPTURE_CONTRACT_SCHEMA
from trading.capture_replay_validation import (
    analyze_independent_positions,
    build_temporal_replay_splits,
    calibrated_cost_profiles,
    collect_sim_cost_evidence,
    load_cost_evidence,
    validate_cost_evidence,
)
from trading.futures_capture_replay import (
    build_capture_replay_index,
    load_replay_dataset,
    verify_replay_dataset,
)
from trading.promotion_producers import build_validation_promotion_fragment


_WORKER_INDEXED = None
_WORKER_FUNDING = None
_WORKER_CONFIG = None
_RESERVED_CONFIG = {
    "capture_replay",
    "futures_screener_parity",
    "historical_funding_timeline",
    "round_trip_cost_bps",
}
_RUNTIME_TO_REPLAY_KEYS = {
    "MIN_PUMP": "min_pump",
    "ACTIVATION_PROFIT": "activation_profit",
    "TRAILING_DISTANCE": "trailing_distance",
    "POST_PARTIAL_TRAILING_DISTANCE": "post_partial_trailing_distance",
    "INITIAL_STOP_LOSS": "stop_loss",
    "PARTIAL_SELL_PCT": "partial_pct",
    "RSI_MAX": "rsi_max",
    "POSITION_SIZE": "position_size",
    "POSITION_SIZE_MAX": "position_size_max",
    "MAX_OPEN_TRADES": "max_open_trades",
    "LEVERAGE": "leverage",
    "LIQ_SAFETY_PCT": "liq_safety_pct",
    "BREAKEVEN_TRIGGER": "breakeven_trigger",
    "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED": (
        "pre_activation_giveback_stop_enabled"
    ),
    "PRE_ACTIVATION_MIN_MFE_PCT": "pre_activation_min_mfe_pct",
    "PRE_ACTIVATION_GIVEBACK_PCT": "pre_activation_giveback_pct",
    "MFE_FALLBACK_STOP_ENABLED": "mfe_fallback_stop_enabled",
    "MFE_FALLBACK_MIN_AGE_MINUTES": "mfe_fallback_min_age_minutes",
    "MFE_FALLBACK_MIN_MFE_PCT": "mfe_fallback_min_mfe_pct",
    "MFE_FALLBACK_EXIT_MOVE_PCT": "mfe_fallback_exit_move_pct",
    "COOLDOWN_AFTER_SL": "cooldown_after_stop_minutes",
    "OWN_MOMENTUM_FILTER": "own_momentum_filter",
    "OWN_MOMENTUM_WINDOW": "own_momentum_window",
    "OWN_MOMENTUM_MIN_LOSS_PCT": "own_momentum_min_loss_pct",
    "MAX_DAILY_LOSS": "max_daily_loss",
    "MAX_DAILY_LOSS_HARD_MULT": "max_daily_loss_hard_mult",
    "ENTRY_QUALITY_FILTER_ENABLED": "entry_quality_filter_enabled",
    "ENTRY_QUALITY_MIN_SCORE": "entry_quality_min_score",
}
_PHASE2_CODE_FILES = (
    "tools/futures_capture_phase2.py",
    "tools/futures_capture_replay.py",
    "tools/backtester.py",
    "tools/simulation_workspace.py",
    "bots/main_bot_futures.py",
    "bot_utils/config.py",
    "trading/capture_replay_validation.py",
    "trading/cooldown_utils.py",
    "trading/futures_capture_replay.py",
    "trading/historical_futures_evidence.py",
    "trading/promotion_assembly.py",
    "trading/promotion_gate.py",
    "trading/promotion_producers.py",
    "trading/entry_quality.py",
    "trading/futures_peak_trail.py",
    "trading/futures_mfe_fallback.py",
    "bot_utils/indicators.py",
    "bot_utils/futures_funding.py",
    "bot_utils/futures_math.py",
    "core/constants.py",
)
_GIB = 1024**3
_MIN_WORKER_RSS_BYTES = 512 * 1024**2
_REPLAY_CONFIG_MAX_BYTES = 1024 * 1024
_RUNTIME_CONFIG_MAX_BYTES = 2 * 1024 * 1024
_MAX_DURABILITY_PARENT_DEPTH = 64
_RUNTIME_ENV_REPLAY_FIELDS = {
    "FUT_EXT_FILTER": ("futures_extension_filter", bool),
    "FUT_MAX_EXT_PCT": ("futures_max_extension_pct", float),
    "FUT_VOL_FILTER": ("futures_volume_filter", bool),
    "FUT_MIN_VOL_SURGE": ("futures_min_volume_surge", float),
    "FUT_RS_FILTER": ("futures_relative_strength_filter", bool),
    "FUT_MIN_RS_PCT": ("futures_min_relative_strength_pct", float),
}


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: Path, *, label: str) -> Path:
    requested = path.expanduser().absolute()
    components = (requested, *requested.parents)
    if any(_is_linklike(component) for component in components):
        raise ValueError(f"{label} must be a real path without links")
    return requested


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_json_constant(value: str):
    raise ValueError(f"non-standard JSON constant: {value}")


def _require_finite_json(value, *, label: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{label} must contain finite JSON values")
    if isinstance(value, dict):
        for item in value.values():
            _require_finite_json(item, label=label)
    elif isinstance(value, list):
        for item in value:
            _require_finite_json(item, label=label)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least one")
    return parsed


def _memory_safe_worker_limit(
    *,
    process_rss_bytes: int,
    available_bytes: int,
    total_bytes: int,
) -> int:
    """Estimate safe spawned workers while reserving OS/headroom memory."""
    values = (process_rss_bytes, available_bytes, total_bytes)
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values):
        raise ValueError("worker memory evidence must be positive integer bytes")
    if available_bytes > total_bytes or process_rss_bytes > total_bytes:
        raise ValueError("worker memory evidence is inconsistent")
    reserve = max(4 * _GIB, total_bytes // 5)
    per_worker = max(process_rss_bytes, _MIN_WORKER_RSS_BYTES)
    # Spawned Windows workers load and index the immutable dataset separately.
    # Keep 10% overhead above the observed parent footprint.
    per_worker = (per_worker * 11 + 9) // 10
    budget = max(0, available_bytes - reserve)
    return budget // per_worker


def _enforce_worker_memory(workers: int) -> None:
    if workers == 1:
        return
    try:
        import psutil

        memory = psutil.virtual_memory()
        process_rss = psutil.Process().memory_info().rss
        safe_limit = _memory_safe_worker_limit(
            process_rss_bytes=int(process_rss),
            available_bytes=int(memory.available),
            total_bytes=int(memory.total),
        )
    except (AttributeError, ImportError, OSError, TypeError, ValueError) as exc:
        raise RuntimeError("cannot prove safe phase-2 worker memory") from exc
    if workers > safe_limit:
        recommendation = max(1, safe_limit)
        raise MemoryError(
            f"--workers {workers} exceeds the memory-safe limit {recommendation}; "
            f"rerun with --workers {recommendation} or lower"
        )


def _validated_config(config: dict) -> dict:
    if not isinstance(config, dict):
        raise ValueError("phase-2 config must be an object")
    conflicts = sorted(_RESERVED_CONFIG.intersection(config))
    if conflicts:
        raise ValueError(
            "phase-2 config contains reserved keys: " + ", ".join(conflicts)
        )
    # Round-trip through strict JSON so run identity cannot depend on aliases,
    # custom objects or non-finite Python values.
    try:
        encoded = json.dumps(config, sort_keys=True, allow_nan=False)
        normalized = json.loads(encoded)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("phase-2 config must contain finite JSON values") from exc
    return normalized


def _runtime_replay_config(root_config: dict) -> tuple[dict, list[str]]:
    """Resolve FUTURES exactly like runtime and expose remaining parity gaps."""
    from bot_utils.config import merge_runtime_config
    from bots.main_bot_futures import FuturesExchangeBot

    resolved = merge_runtime_config(
        "FUTURES", FuturesExchangeBot.DEFAULTS, root_config
    )
    replay = {
        replay_key: resolved[runtime_key]
        for runtime_key, replay_key in _RUNTIME_TO_REPLAY_KEYS.items()
    }
    gaps = []
    runtime_env = root_config.get("_RUNTIME_ENV")
    timezone_name = runtime_env.get("BOT_TIMEZONE") if isinstance(runtime_env, dict) else None
    if not isinstance(timezone_name, str) or not timezone_name.strip():
        gaps.append("daily_loss_timezone_not_bound")
    else:
        try:
            ZoneInfo(timezone_name)
        except (ValueError, ZoneInfoNotFoundError):
            gaps.append("daily_loss_timezone_invalid")
        else:
            replay["daily_loss_timezone"] = timezone_name
    for env_key, (replay_key, value_type) in _RUNTIME_ENV_REPLAY_FIELDS.items():
        if not isinstance(runtime_env, dict) or env_key not in runtime_env:
            gaps.append(f"runtime_env_{env_key.lower()}_not_bound")
            continue
        raw = runtime_env[env_key]
        if value_type is bool:
            if not isinstance(raw, bool):
                gaps.append(f"runtime_env_{env_key.lower()}_invalid")
                continue
            replay[replay_key] = raw
            continue
        if isinstance(raw, bool):
            gaps.append(f"runtime_env_{env_key.lower()}_invalid")
            continue
        try:
            parsed = float(raw)
        except (TypeError, ValueError, OverflowError):
            parsed = math.nan
        if not math.isfinite(parsed):
            gaps.append(f"runtime_env_{env_key.lower()}_invalid")
            continue
        replay[replay_key] = parsed
    return _validated_config(replay), gaps


def _load_json_config_snapshot(
    path: Path,
    *,
    maximum_bytes: int,
    label: str,
) -> tuple[dict, str]:
    try:
        requested = _absolute_without_links(path, label=label)
    except ValueError as exc:
        raise ValueError(f"{label} must be a real file without links") from exc
    if not requested.is_file() or _is_linklike(requested):
        raise ValueError(f"{label} must be a real file without links")
    try:
        with requested.open("rb") as handle:
            raw = handle.read(maximum_bytes + 1)
    except OSError as exc:
        raise ValueError(f"{label} must be a readable real file") from exc
    if len(raw) > maximum_bytes:
        raise ValueError(f"{label} is oversized")
    try:
        root_config = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
        _require_finite_json(root_config, label=label)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{label} is invalid JSON") from exc
    if not isinstance(root_config, dict):
        raise ValueError(f"{label} must be a JSON object")
    return root_config, hashlib.sha256(raw).hexdigest()


def _load_runtime_replay_config(path: Path) -> tuple[dict, dict]:
    root_config, source_sha256 = _load_json_config_snapshot(
        path,
        maximum_bytes=_RUNTIME_CONFIG_MAX_BYTES,
        label="runtime config",
    )
    replay, gaps = _runtime_replay_config(root_config)
    return replay, {
        "source": "futures_runtime_config",
        "source_sha256": source_sha256,
        "runtime_parity_ready": not gaps,
        "parity_gaps": gaps,
    }


def _load_research_replay_config(path: Path) -> tuple[dict, dict]:
    config, source_sha256 = _load_json_config_snapshot(
        path,
        maximum_bytes=_REPLAY_CONFIG_MAX_BYTES,
        label="replay config",
    )
    return config, {
        "source": "research_config",
        "source_sha256": source_sha256,
        "runtime_parity_ready": False,
        "parity_gaps": ["runtime_config_not_bound"],
    }


def _validated_config_evidence(value: dict) -> dict:
    if not isinstance(value, dict):
        raise ValueError("config evidence must be an object")
    try:
        normalized = json.loads(json.dumps(value, sort_keys=True, allow_nan=False))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("config evidence must contain finite JSON values") from exc
    source = normalized.get("source")
    ready = normalized.get("runtime_parity_ready")
    gaps = normalized.get("parity_gaps")
    if not isinstance(source, str) or not source.strip():
        raise ValueError("config evidence source must be a non-empty string")
    if not isinstance(ready, bool):
        raise ValueError("runtime_parity_ready must be boolean")
    if (
        not isinstance(gaps, list)
        or any(not isinstance(gap, str) or not gap.strip() for gap in gaps)
        or len(set(gaps)) != len(gaps)
    ):
        raise ValueError("config evidence parity_gaps must be unique strings")
    if ready and gaps:
        raise ValueError("runtime parity cannot be ready while gaps remain")
    return normalized


def _phase2_code_files() -> list[Path]:
    """Return every source that can change deterministic capture replay output."""
    root = Path(_PROJECT_ROOT)
    return [root / Path(relative) for relative in _PHASE2_CODE_FILES]


def _phase2_dependency_lock_file() -> Path:
    return Path(_PROJECT_ROOT) / "requirements.lock.txt"


def _params(config: dict, funding, *, cost_bps: float | None = None) -> dict:
    values = dict(config)
    values.update({
        "capture_replay": True,
        "futures_screener_parity": True,
        "historical_funding_timeline": funding,
    })
    if cost_bps is not None:
        values["round_trip_cost_bps"] = cost_bps
    return values


def _period_index(indexed: dict, times: tuple) -> dict:
    selected = set(times)
    return {
        symbol: {time: tick for time, tick in rows.items() if time in selected}
        for symbol, rows in indexed.items()
    }


def _run_task(indexed: dict, funding, config: dict, task: dict) -> dict:
    times = tuple(task["times"])
    stats = _normalized_phase2_stats(simulate_fast(
        _period_index(indexed, times),
        list(times),
        "FUTURES",
        False,
        _params(config, funding, cost_bps=task["round_trip_cost_bps"]),
    ))
    positions = analyze_independent_positions(
        stats,
        indexed=indexed,
        allowed_times=times,
    )
    return _json_safe({
        "task": {key: value for key, value in task.items() if key != "times"},
        "period": {
            "start": times[0],
            "end": times[-1],
            "timestamps": len(times),
        },
        "stats": stats,
        "positions": positions,
    })


def _worker_initialize(dataset_path: str, config: dict) -> None:
    global _WORKER_CONFIG, _WORKER_FUNDING, _WORKER_INDEXED
    dataset = load_replay_dataset(dataset_path)
    indexed, _times, _evidence = build_capture_replay_index(
        dataset,
        min_pump=float(config.get("min_pump", 1.0)),
    )
    _WORKER_INDEXED = indexed
    _WORKER_FUNDING = dataset.funding
    _WORKER_CONFIG = config


def _worker_run(task: dict) -> tuple[int, dict]:
    if _WORKER_INDEXED is None or _WORKER_CONFIG is None:
        raise RuntimeError("phase-2 worker was not initialized")
    return task["candidate_index"], _run_task(
        _WORKER_INDEXED,
        _WORKER_FUNDING,
        _WORKER_CONFIG,
        task,
    )


def _task_params(task: dict, config: dict) -> dict:
    return {
        "strategy_config": config,
        "scope": task["scope"],
        "fold_index": task["fold_index"],
        "cost_profile": task["cost_profile"],
        "round_trip_cost_bps": task["round_trip_cost_bps"],
        "period_start": task["times"][0],
        "period_end": task["times"][-1],
        "period_timestamps": len(task["times"]),
    }


def _values_match(actual: float, expected: float) -> bool:
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


def _normalized_phase2_stats(stats: dict) -> dict:
    """Turn the backtester's no-trade ranking sentinel into zero PnL evidence."""
    if not isinstance(stats, dict):
        raise ValueError("phase-2 backtest stats must be an object")
    trades = stats.get("closed_trades")
    if trades == [] and "invalid_reason" not in stats:
        normalized = dict(stats)
        normalized.update({
            "trades": 0,
            "full_trades": 0,
            "trade_count": 0,
            "gross": 0.0,
            "costs": 0.0,
            "net": 0.0,
            "max_dd": 0.0,
            "total_fees": 0.0,
            "total_funding": 0.0,
        })
        return normalized
    return stats


def _validated_phase2_stats(stats: dict, *, label: str) -> dict:
    """Prove per-fill and aggregate accounting from primary trade rows."""
    if not isinstance(stats, dict) or stats.get("invalid_reason") is not None:
        raise ValueError(f"{label} backtest evidence is invalid")
    trades = stats.get("closed_trades")
    if not isinstance(trades, list):
        raise ValueError(f"{label} closed-trade evidence is missing")
    totals = {
        "net": [],
        "gross": [],
        "costs": [],
        "total_fees": [],
        "total_funding": [],
    }

    def number(value, name: str, *, minimum: float | None = None) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{label} {name} must be finite")
        result = float(value)
        if not math.isfinite(result) or (minimum is not None and result < minimum):
            raise ValueError(f"{label} {name} must be finite")
        return result

    for trade in trades:
        if not isinstance(trade, dict):
            raise ValueError(f"{label} closed trade must be an object")
        gross = number(trade.get("gross"), "closed-trade gross")
        fees = number(trade.get("fees"), "closed-trade fees", minimum=0.0)
        funding = number(trade.get("funding"), "closed-trade funding")
        cost = number(trade.get("cost"), "closed-trade cost")
        net = number(trade.get("net"), "closed-trade net")
        if not _values_match(cost, fees + funding):
            raise ValueError(f"{label} closed-trade cost accounting is inconsistent")
        if not _values_match(net, gross - cost):
            raise ValueError(f"{label} closed-trade net accounting is inconsistent")
        totals["gross"].append(gross)
        totals["total_fees"].append(fees)
        totals["total_funding"].append(funding)
        totals["costs"].append(cost)
        totals["net"].append(net)
    try:
        reconstructed = {
            name: math.fsum(values) for name, values in totals.items()
        }
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{label} trade accounting is nonfinite") from exc
    for name, expected in reconstructed.items():
        actual = number(stats.get(name), f"aggregate {name}")
        if not _values_match(actual, expected):
            raise ValueError(f"{label} aggregate {name} accounting is inconsistent")
    reported_counts = {
        name: stats.get(name) for name in ("trades", "full_trades", "trade_count")
    }
    for name, actual in reported_counts.items():
        if (
            isinstance(actual, bool)
            or not isinstance(actual, int)
            or actual < 0
        ):
            raise ValueError(f"{label} aggregate {name} count is inconsistent")
    if (
        reported_counts["trades"] != len(trades)
        or reported_counts["full_trades"] != reported_counts["trade_count"]
    ):
        raise ValueError(f"{label} aggregate trade counts are inconsistent")
    return stats


def _validated_baseline_result(result: dict, indexed: dict, times: tuple) -> dict:
    stats = _validated_phase2_stats(
        _json_safe(result), label="phase-2 baseline"
    )
    canonical_times = tuple(_json_safe(value) for value in times)
    canonical_indexed = {
        symbol: {
            _json_safe(timestamp): tick for timestamp, tick in rows.items()
        }
        for symbol, rows in indexed.items()
    }
    positions = analyze_independent_positions(
        stats,
        indexed=canonical_indexed,
        allowed_times=canonical_times,
    )
    if positions.get("evidence_valid") is not True:
        raise ValueError("phase-2 baseline position evidence is invalid")
    return stats


def _phase2_record(record: dict, task: dict, indexed: dict) -> dict:
    """Validate fresh and resumed task evidence before it can reach gates."""
    if not isinstance(record, dict):
        raise ValueError("phase-2 task record must be an object")
    expected_task = _json_safe({
        key: value for key, value in task.items() if key != "times"
    })
    expected_period = _json_safe({
        "start": task["times"][0],
        "end": task["times"][-1],
        "timestamps": len(task["times"]),
    })
    if record.get("task") != expected_task or record.get("period") != expected_period:
        raise ValueError("phase-2 task record identity is inconsistent")
    stats = record.get("stats")
    positions = record.get("positions")
    if not isinstance(positions, dict):
        raise ValueError("phase-2 task evidence is incomplete")
    stats = _validated_phase2_stats(stats, label="phase-2 task")
    evidence_valid = positions.get("evidence_valid")
    if not isinstance(evidence_valid, bool):
        raise ValueError("phase-2 position validity must be boolean")
    if evidence_valid is False:
        if (
            set(positions) != {"evidence_valid", "reason"}
            or not isinstance(positions.get("reason"), str)
            or not positions["reason"].strip()
        ):
            raise ValueError("invalid phase-2 position evidence is malformed")
    else:
        count = positions.get("independent_positions")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("phase-2 independent position count is invalid")
        if stats.get("full_trades") != count:
            raise ValueError("phase-2 position count conflicts with stats")
        if positions.get("position_ids_unique_within_period") is not True:
            raise ValueError("phase-2 position identity evidence is invalid")

        def counts(name: str) -> dict:
            values = positions.get(name)
            if not isinstance(values, dict):
                raise ValueError(f"phase-2 {name} is invalid")
            for key, value in values.items():
                if (
                    not isinstance(key, str)
                    or not key
                    or isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 0
                ):
                    raise ValueError(f"phase-2 {name} is invalid")
            if sum(values.values()) != count:
                raise ValueError(f"phase-2 {name} does not cover all positions")
            return values

        symbol_counts = counts("symbol_counts")
        regime_counts = counts("regime_counts")

        def positive_nets(name: str, available: dict) -> None:
            values = positions.get(name)
            if not isinstance(values, dict) or not set(values).issubset(available):
                raise ValueError(f"phase-2 {name} is invalid")
            for key, value in values.items():
                if isinstance(value, bool):
                    raise ValueError(f"phase-2 {name} is invalid")
                try:
                    number = float(value)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ValueError(f"phase-2 {name} is invalid") from exc
                if not math.isfinite(number) or number <= 0.0:
                    raise ValueError(f"phase-2 {name} is invalid")

        positive_nets("positive_net_by_symbol", symbol_counts)
        positive_nets("positive_net_by_regime", regime_counts)
    canonical_allowed_times = tuple(_json_safe(value) for value in task["times"])
    period_index = _period_index(indexed, tuple(task["times"]))
    canonical_indexed = {
        symbol: {
            _json_safe(timestamp): tick for timestamp, tick in rows.items()
        }
        for symbol, rows in period_index.items()
    }
    reconstructed_positions = analyze_independent_positions(
        stats,
        indexed=canonical_indexed,
        allowed_times=canonical_allowed_times,
    )
    if positions != _json_safe(reconstructed_positions):
        raise ValueError(
            "phase-2 position evidence conflicts with closed trades"
        )
    return record


def _tasks(splits, profiles: dict) -> list[dict]:
    periods = [("initial_train", None, splits.initial_train)]
    periods.extend(
        ("walk_forward", index, values)
        for index, values in enumerate(splits.walk_forward_tests)
    )
    periods.append(("final_holdout", None, splits.final_holdout))
    result = []
    for profile_name, cost_key in (
        ("normal", "normal_round_trip_bps"),
        ("stressed", "stressed_round_trip_bps"),
    ):
        for scope, fold_index, times in periods:
            result.append({
                "candidate_index": len(result),
                "scope": scope,
                "fold_index": fold_index,
                "cost_profile": profile_name,
                "round_trip_cost_bps": profiles[cost_key],
                "times": tuple(times),
            })
    return result


def _aggregate_concentration(records: list[dict]) -> dict:
    symbol_profit = {}
    regime_profit = {}
    symbol_counts = Counter()
    regime_counts = Counter()
    positions = 0
    valid = True
    for record in records:
        evidence = record.get("positions") or {}
        valid = valid and evidence.get("evidence_valid") is True
        positions += int(evidence.get("independent_positions", 0) or 0)
        symbol_counts.update(evidence.get("symbol_counts") or {})
        regime_counts.update(evidence.get("regime_counts") or {})
        for key, value in (evidence.get("positive_net_by_symbol") or {}).items():
            symbol_profit[key] = symbol_profit.get(key, 0.0) + float(value)
        for key, value in (evidence.get("positive_net_by_regime") or {}).items():
            regime_profit[key] = regime_profit.get(key, 0.0) + float(value)
    total_positive = math.fsum(symbol_profit.values())

    def share(values: dict) -> float | None:
        return max(values.values()) / total_positive if total_positive > 0.0 and values else None

    symbol_share = share(symbol_profit)
    regime_share = share(regime_profit)
    regimes = sorted(name for name in regime_counts if name != "UNKNOWN")
    return {
        "evidence_valid": valid,
        "independent_positions": positions,
        "symbol_counts": dict(sorted(symbol_counts.items())),
        "regime_counts": dict(sorted(regime_counts.items())),
        "maximum_symbol_profit_share": symbol_share,
        "maximum_regime_profit_share": regime_share,
        "symbol_concentration_pass": bool(
            symbol_share is not None and symbol_share <= 0.25
        ),
        "regime_concentration_pass": bool(
            regime_share is not None and regime_share <= 0.80 and len(regimes) >= 2
        ),
        "regimes_observed": regimes,
    }


def _summary(
    records: list[dict],
    profiles: dict,
    *,
    runtime_parity_ready: bool = False,
) -> dict:
    by_profile = {}
    for profile in ("normal", "stressed"):
        selected = [row for row in records if row["task"]["cost_profile"] == profile]
        folds = [row for row in selected if row["task"]["scope"] == "walk_forward"]
        holdout = next(
            row for row in selected if row["task"]["scope"] == "final_holdout"
        )
        fold_nets = [float(row["stats"]["net"]) for row in folds]
        valid_folds = [
            row for row in folds if row["positions"].get("evidence_valid") is True
        ]
        positive = sum(value > 0.0 for value in fold_nets)
        by_profile[profile] = {
            "round_trip_cost_bps": profiles[f"{profile}_round_trip_bps"],
            "walk_forward_nets": fold_nets,
            "positive_walk_forward_folds": positive,
            "walk_forward_fold_count": len(folds),
            "walk_forward_majority_positive": bool(
                len(valid_folds) == len(folds) and positive > len(folds) / 2.0
            ),
            "final_holdout_net": holdout["stats"]["net"],
            "final_holdout_positive": bool(holdout["stats"]["net"] > 0.0),
        }
    stressed_oos = [
        row for row in records
        if row["task"]["cost_profile"] == "stressed"
        and row["task"]["scope"] in {"walk_forward", "final_holdout"}
    ]
    concentration = _aggregate_concentration(stressed_oos)
    all_positions_valid = all(
        row["positions"].get("evidence_valid") is True for row in records
    )
    gates = {
        "cost_calibration_ready": profiles["simulation_ready"] is True,
        "markout_coverage_ready": profiles["promotion_evidence_ready"] is True,
        "runtime_parity_ready": bool(runtime_parity_ready),
        "position_grouping_valid": all_positions_valid,
        "minimum_300_independent_positions": concentration["independent_positions"] >= 300,
        "walk_forward_majority_positive_normal": by_profile["normal"]["walk_forward_majority_positive"],
        "walk_forward_majority_positive_stressed": by_profile["stressed"]["walk_forward_majority_positive"],
        "stressed_final_holdout_positive": by_profile["stressed"]["final_holdout_positive"],
        "symbol_concentration_pass": concentration["symbol_concentration_pass"],
        "regime_concentration_pass": concentration["regime_concentration_pass"],
    }
    return {
        "profiles": by_profile,
        "stressed_oos_concentration": concentration,
        "gates": gates,
        "phase2_evidence_passed": all(gates.values()),
        # Capture parity gaps and the later Monte-Carlo/DSR/PBO/forward gates
        # intentionally keep this report from ever promoting a strategy alone.
        "promotion_eligible": False,
        "classification": (
            "phase2_passed_research_only"
            if all(gates.values())
            else "exploratory_only"
        ),
    }


def build_validation_fragment_inputs(
    records: list[dict], cost_evidence: dict
) -> dict:
    """Reconstruct validation producer inputs from raw Phase-2 evidence."""
    evidence_body = validate_cost_evidence(cost_evidence)
    costs = evidence_body.get("costs")
    markouts = evidence_body.get("markouts")
    if not isinstance(costs, dict) or not isinstance(markouts, dict):
        raise ValueError("cost evidence counters are unavailable")
    contract_costs = costs.get("capture_contract")
    contract_markouts = markouts.get("capture_contract")
    if not isinstance(contract_costs, dict) or not isinstance(
        contract_markouts, dict
    ):
        raise ValueError("cost evidence capture cohort is unavailable")
    if (
        contract_costs.get("schema_version") != SIM_CAPTURE_CONTRACT_SCHEMA
        or contract_markouts.get("schema_version")
        != SIM_CAPTURE_CONTRACT_SCHEMA
    ):
        raise ValueError("cost evidence capture cohort schema is unsupported")
    paired = contract_costs.get("valid_samples")
    horizons = contract_markouts.get("by_horizon")
    if (
        isinstance(paired, bool)
        or not isinstance(paired, int)
        or paired < 1
        or not isinstance(horizons, dict)
        or not horizons
    ):
        raise ValueError("cost evidence coverage scope is unavailable")
    complete = []
    noncausal = 0
    for horizon in horizons.values():
        if not isinstance(horizon, dict):
            raise ValueError("markout horizon evidence is invalid")
        count = horizon.get("complete_entries")
        rejected = horizon.get("rejected")
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or count > paired
            or not isinstance(rejected, dict)
        ):
            raise ValueError("markout horizon counters are invalid")
        complete.append(count)
        value = rejected.get("noncausal_complete_markout", 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("markout causality counter is invalid")
        noncausal += value
    rejected_costs = contract_costs.get("rejected")
    if not isinstance(rejected_costs, dict):
        raise ValueError("cost rejection counters are invalid")
    for key in ("noncausal_pair", "noncausal_candidate_anchor"):
        value = rejected_costs.get(key, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("cost causality counter is invalid")
        noncausal += value
    periods = []
    for record in records:
        task = record.get("task")
        period = record.get("period")
        stats = record.get("stats")
        positions = record.get("positions")
        if not all(isinstance(value, dict) for value in (task, period, stats, positions)):
            raise ValueError("phase-2 validation record is incomplete")
        raw_positions = positions.get("positions")
        if positions.get("evidence_valid") is not True or not isinstance(raw_positions, list):
            raise ValueError("phase-2 independent positions are unavailable")
        initial_capital = stats.get("initial_capital")
        if (
            isinstance(initial_capital, bool)
            or not isinstance(initial_capital, (int, float))
            or not math.isfinite(float(initial_capital))
            or float(initial_capital) <= 0.0
        ):
            raise ValueError("phase-2 position summaries conflict with raw evidence")
        equity = peak = float(initial_capital)
        reconstructed_drawdown = 0.0
        nets_by_exit: dict[float, list[float]] = {}
        reconstructed_liquidations = 0
        for row in raw_positions:
            if not isinstance(row, dict):
                raise ValueError("phase-2 raw position evidence is invalid")
            exit_time = row.get("exit_time")
            net = row.get("net")
            liquidated = row.get("liquidated")
            if (
                isinstance(exit_time, bool)
                or not isinstance(exit_time, (int, float))
                or not math.isfinite(float(exit_time))
                or isinstance(net, bool)
                or not isinstance(net, (int, float))
                or not math.isfinite(float(net))
                or not isinstance(liquidated, bool)
            ):
                raise ValueError("phase-2 raw position evidence is invalid")
            nets_by_exit.setdefault(float(exit_time), []).append(float(net))
            reconstructed_liquidations += int(liquidated)
        for exit_time in sorted(nets_by_exit):
            equity += math.fsum(nets_by_exit[exit_time])
            peak = max(peak, equity)
            if peak > 0.0:
                reconstructed_drawdown = max(
                    reconstructed_drawdown,
                    (peak - equity) / peak * 100.0,
                )
        if (
            not _values_match(stats.get("max_dd"), reconstructed_drawdown)
            or stats.get("liquidation_count") != reconstructed_liquidations
        ):
            raise ValueError("phase-2 position summaries conflict with raw evidence")
        periods.append({
            "profile": task.get("cost_profile"),
            "scope": task.get("scope"),
            "fold_index": task.get("fold_index"),
            "period_start": period.get("start"),
            "period_end": period.get("end"),
            "initial_capital": initial_capital,
            "positions": [
                {
                    "position_id": row.get("position_id"),
                    "symbol": row.get("symbol"),
                    "regime": row.get("regime"),
                    "exit_time": row.get("exit_time"),
                    "net": row.get("net"),
                    "liquidated": row.get("liquidated"),
                }
                for row in raw_positions
            ],
            "max_drawdown_pct": reconstructed_drawdown,
            "liquidation_count": reconstructed_liquidations,
        })
    return {
        "eligible_observation_count": paired,
        "covered_observation_count": min(complete),
        "noncausal_observation_count": noncausal,
        "periods": periods,
    }
def run_phase2(
    dataset_path: Path,
    config: dict,
    cost_evidence: dict,
    workspace: Path,
    *,
    seed: int,
    workers: int,
    resume: bool,
    walk_forward_steps: int = 4,
    embargo_bars: int = 24,
    config_evidence: dict | None = None,
) -> dict:
    normalized_config = _validated_config(config)
    if config_evidence is None:
        config_evidence = {
            "source": "research_config",
            "runtime_parity_ready": False,
            "parity_gaps": ["runtime_config_not_bound"],
        }
    config_evidence = _validated_config_evidence(config_evidence)
    dataset = load_replay_dataset(dataset_path)
    indexed, all_times, dataset_report = build_capture_replay_index(
        dataset,
        min_pump=float(normalized_config.get("min_pump", 1.0)),
    )
    _enforce_worker_memory(workers)
    splits = build_temporal_replay_splits(
        all_times,
        walk_forward_steps=walk_forward_steps,
        embargo_bars=embargo_bars,
    )
    default_cost = calc_round_trip(False, "FUTURES") * 10_000.0
    profiles = calibrated_cost_profiles(
        cost_evidence,
        default_round_trip_bps=default_cost,
    )
    run = ReproducibleRun(
        workspace,
        dataset_path,
        run_config={
            "method": "mexc_futures_capture_phase2_v1",
            "strategy_config": normalized_config,
            "config_evidence": config_evidence,
            "cost_profiles": profiles,
            "cost_evidence_sha256": cost_evidence["evidence_sha256"],
        },
        splits=splits.specification,
        seed=seed,
        workers=workers,
        code_files=_phase2_code_files(),
        dependency_lock_file=_phase2_dependency_lock_file(),
        resume=resume,
        dataset_verifier=verify_replay_dataset,
    )
    baseline = run.baseline(normalized_config)
    if baseline is None:
        baseline_stats = _normalized_phase2_stats(simulate_fast(
            indexed,
            all_times,
            "FUTURES",
            False,
            _params(normalized_config, dataset.funding),
        ))
        baseline_stats = _validated_baseline_result(
            baseline_stats, indexed, tuple(all_times)
        )
        baseline = run.record_baseline(
            normalized_config,
            baseline_stats,
        )
    else:
        _validated_baseline_result(
            baseline["result"], indexed, tuple(all_times)
        )

    tasks = _tasks(splits, profiles)
    records: dict[int, dict] = {}
    pending = []
    for task in tasks:
        index = task["candidate_index"]
        params = _task_params(task, normalized_config)
        checkpoint = run.checkpoint(index, params)
        if checkpoint is None:
            pending.append(task)
        else:
            records[index] = _phase2_record(checkpoint, task, indexed)
    if workers == 1:
        completed = (
            (task["candidate_index"], _run_task(
                indexed, dataset.funding, normalized_config, task
            ))
            for task in pending
        )
        for index, record in completed:
            task = tasks[index]
            record = _phase2_record(record, task, indexed)
            run.record_checkpoint(index, _task_params(task, normalized_config), record)
            records[index] = record
    elif pending:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers,
            initializer=_worker_initialize,
            initargs=(str(dataset_path.expanduser().absolute()), normalized_config),
        ) as executor:
            futures = {executor.submit(_worker_run, task): task for task in pending}
            for future in concurrent.futures.as_completed(futures):
                index, record = future.result()
                task = tasks[index]
                record = _phase2_record(record, task, indexed)
                run.record_checkpoint(index, _task_params(task, normalized_config), record)
                records[index] = record
    ordered = [records[index] for index in range(len(tasks))]
    candidate_fingerprint = canonical_evidence_sha256(normalized_config)
    validation_fragment_inputs = None
    validation_fragment = None
    validation_fragment_error = None
    try:
        validation_fragment_inputs = build_validation_fragment_inputs(
            ordered, cost_evidence
        )
        validation_fragment = build_validation_promotion_fragment(
            strategy="FUTURES",
            run_id=run.run_id,
            dataset_fingerprint=dataset_report["dataset_fingerprint"],
            candidate_fingerprint=candidate_fingerprint,
            **validation_fragment_inputs,
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        validation_fragment_error = f"{type(exc).__name__}: {str(exc)[:160]}"
    return _json_safe({
        "method": "mexc_futures_capture_phase2_v1",
        "strategy": "FUTURES",
        "run_id": run.run_id,
        "candidate_params": normalized_config,
        "candidate_fingerprint": candidate_fingerprint,
        "run_root": run.root,
        "config_evidence": config_evidence,
        "dataset": dataset_report,
        "splits": splits.specification,
        "cost_profiles": profiles,
        "baseline": baseline,
        "evaluations": ordered,
        "validation_cost_evidence": cost_evidence,
        "validation_promotion_fragment": validation_fragment,
        "validation_promotion_fragment_error": validation_fragment_error,
        "summary": _summary(
            ordered,
            profiles,
            runtime_parity_ready=bool(
                config_evidence.get("runtime_parity_ready", False)
            ),
        ),
        "research_only": True,
        "changes_runtime": False,
    })


def _sync_phase2_output_directory(path: Path) -> None:
    directory = path.resolve(strict=True)
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        create_file.restype = wintypes.HANDLE
        flush_file_buffers = kernel32.FlushFileBuffers
        flush_file_buffers.argtypes = [wintypes.HANDLE]
        flush_file_buffers.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL

        handle = create_file(
            str(directory),
            0x40000000,  # GENERIC_WRITE
            0x00000007,  # FILE_SHARE_READ | WRITE | DELETE
            None,
            3,  # OPEN_EXISTING
            0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
            None,
        )
        invalid_handle = ctypes.c_void_p(-1).value
        if not handle or int(handle) == invalid_handle:
            raise ctypes.WinError(ctypes.get_last_error())
        primary_error: BaseException | None = None
        try:
            if not flush_file_buffers(handle):
                raise ctypes.WinError(ctypes.get_last_error())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            close_error: BaseException | None = None
            try:
                if not close_handle(handle):
                    close_error = ctypes.WinError(ctypes.get_last_error())
            except BaseException as exc:
                close_error = exc
            if close_error is not None:
                if primary_error is None:
                    raise close_error
                try:
                    primary_error.add_note(
                        "close phase-2 output directory after sync failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(str(directory), flags)
    primary_error = None
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
                    "close phase-2 output directory after sync failure: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _sync_phase2_publication_chain(path: Path) -> None:
    """Sync the output parent and every ancestor through its filesystem root."""
    directory = path.resolve(strict=True)
    for _depth in range(_MAX_DURABILITY_PARENT_DEPTH):
        _sync_phase2_output_directory(directory)
        parent = directory.parent
        if parent == directory:
            return
        directory = parent
    raise ValueError("phase-2 output directory depth exceeds durability limit")


def _atomic_json(path: Path, payload: dict) -> None:
    encoded = (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    path = _absolute_without_links(path, label="immutable JSON output path")
    path.parent.mkdir(parents=True, exist_ok=True)
    path = _absolute_without_links(path, label="immutable JSON output path")

    def existing_matches() -> bool:
        if not path.is_file() or _is_linklike(path):
            return False
        try:
            if path.stat().st_size != len(encoded):
                return False
            with path.open("rb") as handle:
                return handle.read(len(encoded) + 1) == encoded
        except OSError:
            return False

    def sync_publication() -> None:
        _sync_phase2_publication_chain(path.parent)

    if path.exists():
        if existing_matches():
            sync_publication()
            return
        raise FileExistsError("immutable JSON output conflict")
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        handle = temporary.open("xb")
        temporary_owned = True
        write_primary: BaseException | None = None
        try:
            temporary_stat = os.fstat(handle.fileno())
            temporary_identity = (temporary_stat.st_dev, temporary_stat.st_ino)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            write_primary = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if write_primary is None:
                    raise
                try:
                    write_primary.add_note(
                        "close phase-2 output temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            if existing_matches():
                sync_publication()
                return
            raise FileExistsError("immutable JSON output conflict") from exc
        sync_publication()
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        same_generation = False
        if temporary_owned and temporary_identity is not None:
            try:
                current = temporary.stat(follow_symlinks=False)
                same_generation = (
                    stat.S_ISREG(current.st_mode)
                    and not _is_linklike(temporary)
                    and (current.st_dev, current.st_ino) == temporary_identity
                )
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if same_generation:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            try:
                primary_error.add_note(
                    "phase-2 owned temporary cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            except BaseException:
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    evidence = sub.add_parser(
        "cost-evidence", help="read-only aggregate SIM-TCA and markouts"
    )
    evidence.add_argument("--runtime-root", type=Path, required=True)
    evidence.add_argument("--json-output", type=Path, required=True)

    run_parser = sub.add_parser(
        "run", help="run or resume immutable phase-2 replay validation"
    )
    run_parser.add_argument("--dataset", type=Path, required=True)
    config_group = run_parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument("--config", type=Path)
    config_group.add_argument("--runtime-config", type=Path)
    run_parser.add_argument("--cost-evidence", type=Path, required=True)
    run_parser.add_argument("--workspace", type=Path, required=True)
    run_parser.add_argument("--json-output", type=Path, required=True)
    run_parser.add_argument("--seed", type=int, default=20260813)
    run_parser.add_argument("--workers", type=_positive_int, default=4)
    run_parser.add_argument("--walk-forward-steps", type=_positive_int, default=4)
    run_parser.add_argument("--embargo-bars", type=_positive_int, default=24)
    run_parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "cost-evidence":
        payload = collect_sim_cost_evidence(args.runtime_root)
        output = args.json_output.expanduser().absolute()
        _atomic_json(output, payload)
        print(json.dumps({
            "classification": payload["readiness"]["classification"],
            "evidence_sha256": payload["evidence_sha256"],
            "output": str(output),
        }, sort_keys=True))
        return 0
    if not 1 <= args.workers <= 32:
        parser.error("--workers must be between 1 and 32")
    if args.walk_forward_steps < 2 or args.walk_forward_steps > 12:
        parser.error("--walk-forward-steps must be between 2 and 12")
    if args.runtime_config is not None:
        strategy_config, config_evidence = _load_runtime_replay_config(
            args.runtime_config
        )
    else:
        strategy_config, config_evidence = _load_research_replay_config(
            args.config
        )
    report = run_phase2(
        args.dataset,
        strategy_config,
        load_cost_evidence(args.cost_evidence),
        args.workspace,
        seed=args.seed,
        workers=args.workers,
        resume=args.resume,
        walk_forward_steps=args.walk_forward_steps,
        embargo_bars=args.embargo_bars,
        config_evidence=config_evidence,
    )
    output = args.json_output.expanduser().absolute()
    _atomic_json(output, report)
    print(json.dumps({
        "classification": report["summary"]["classification"],
        "run_id": report["run_id"],
        "output": str(output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
