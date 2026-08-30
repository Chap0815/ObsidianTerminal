"""Deterministic raw-row producers for validation and forward promotion fragments."""

from __future__ import annotations

import math
import json
import os
import statistics
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trading.promotion_assembly import seal_promotion_fragment


_PERIOD_KEYS = {
    "profile",
    "scope",
    "fold_index",
    "period_start",
    "period_end",
    "initial_capital",
    "positions",
    "max_drawdown_pct",
    "liquidation_count",
}
_POSITION_KEYS = {
    "position_id",
    "symbol",
    "regime",
    "exit_time",
    "net",
    "liquidated",
}
_SHADOW_POSITION_KEYS = {
    "position_id",
    "closed_at",
    "net_after_cost",
    "stressed_net_after_cost",
    "predicted_probability",
    "outcome",
}
_CAPACITY_KEYS = {"sample_id", "measured_at", "participation_rate", "capacity_allowed"}
_FORWARD_REPORT_KEYS = {
    "strategy", "run_id", "dataset_fingerprint", "candidate_fingerprint",
    "period_start", "period_end", "positions", "capacity_samples",
}
_MAX_FORWARD_ARTIFACT_BYTES = 8 * 1024 * 1024
_FORWARD_CLOCK_FUTURE_TOLERANCE_SECONDS = 5.0

# One-sided 95% Student-t critical values for 1..30 degrees of freedom.
# For larger samples df=30 remains deliberately conservative.
_ONE_SIDED_T95 = (
    6.3138, 2.9200, 2.3534, 2.1318, 2.0150, 1.9432, 1.8946, 1.8595,
    1.8331, 1.8125, 1.7959, 1.7823, 1.7709, 1.7613, 1.7531, 1.7459,
    1.7396, 1.7341, 1.7291, 1.7247, 1.7207, 1.7171, 1.7139, 1.7109,
    1.7081, 1.7056, 1.7033, 1.7011, 1.6991, 1.6973,
)


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


def _count(value: Any, name: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < (1 if positive else 0):
        raise ValueError(f"{name} is outside its valid range")
    return value


def _timestamp_number(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a timezone-aware timestamp")
    if isinstance(value, (int, float)):
        return _number(value, name)
    try:
        parsed = (
            datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if isinstance(value, str)
            else value
        )
        if not isinstance(parsed, datetime) or parsed.tzinfo is None:
            raise ValueError
        number = parsed.astimezone(timezone.utc).timestamp()
    except (AttributeError, OSError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a timezone-aware timestamp") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a timezone-aware timestamp")
    return number


def _period_identity(row: dict) -> tuple:
    return (
        row["scope"],
        row["fold_index"],
        row["period_start"],
        row["period_end"],
    )


def _one_sided_t95(degrees_of_freedom: int) -> float:
    if degrees_of_freedom < 1:
        raise ValueError("clustered confidence requires positive degrees of freedom")
    return _ONE_SIDED_T95[min(degrees_of_freedom, len(_ONE_SIDED_T95)) - 1]


def _reconstructed_drawdown_pct(
    positions: list[dict], initial_capital: float
) -> float:
    """Rebuild closed-position equity without inventing same-timestamp order."""
    by_exit: dict[float, list[float]] = {}
    for position in positions:
        by_exit.setdefault(position["exit_time"], []).append(position["net"])
    equity = initial_capital
    peak = initial_capital
    maximum = 0.0
    for exit_time in sorted(by_exit):
        equity += math.fsum(by_exit[exit_time])
        peak = max(peak, equity)
        if peak > 0.0:
            maximum = max(maximum, (peak - equity) / peak * 100.0)
    return maximum


def _normalize_validation_periods(periods: Any) -> dict[str, list[dict]]:
    if not isinstance(periods, list) or not periods:
        raise ValueError("validation periods must be a non-empty list")
    by_profile = {"normal": [], "stressed": []}
    position_ids_by_profile = {"normal": set(), "stressed": set()}
    future_ceiling = time.time() + _FORWARD_CLOCK_FUTURE_TOLERANCE_SECONDS
    for row in periods:
        if not isinstance(row, dict) or set(row) != _PERIOD_KEYS:
            raise ValueError("validation period structure is invalid")
        profile = row["profile"]
        scope = row["scope"]
        if profile not in by_profile or scope not in {"walk_forward", "final_holdout"}:
            raise ValueError("validation period profile or scope is invalid")
        fold_index = row["fold_index"]
        if scope == "walk_forward":
            _count(fold_index, "walk-forward fold index")
        elif fold_index is not None:
            raise ValueError("final holdout must not have a fold index")
        start = _timestamp_number(row["period_start"], "validation period start")
        end = _timestamp_number(row["period_end"], "validation period end")
        if end <= start:
            raise ValueError("validation period must be chronological")
        if end > future_ceiling:
            raise ValueError("validation period end is in the future")
        initial_capital = _number(
            row["initial_capital"], "validation initial capital"
        )
        if initial_capital <= 0.0:
            raise ValueError("validation initial capital must be positive")
        drawdown = _number(row["max_drawdown_pct"], "validation drawdown")
        liquidations = _count(row["liquidation_count"], "liquidation count")
        if drawdown < 0.0:
            raise ValueError("validation drawdown must be nonnegative")
        positions = row["positions"]
        if not isinstance(positions, list):
            raise ValueError("validation positions must be a list")
        normalized_positions = []
        seen = set()
        for position in positions:
            if not isinstance(position, dict) or set(position) != _POSITION_KEYS:
                raise ValueError("validation position structure is invalid")
            position_id = position["position_id"]
            if (
                isinstance(position_id, bool)
                or not isinstance(position_id, (int, str))
                or not str(position_id).strip()
                or position_id in seen
                or not isinstance(position["symbol"], str)
                or not position["symbol"].strip()
                or not isinstance(position["regime"], str)
                or not position["regime"].strip()
            ):
                raise ValueError("validation position identity is invalid")
            exit_time = _timestamp_number(position["exit_time"], "position exit time")
            if not start <= exit_time <= end:
                raise ValueError("validation position exit is outside its period")
            if not isinstance(position["liquidated"], bool):
                raise ValueError("validation position liquidation flag is invalid")
            seen.add(position_id)
            if position_id in position_ids_by_profile[profile]:
                raise ValueError(
                    "position identity repeats across validation periods"
                )
            position_ids_by_profile[profile].add(position_id)
            normalized_positions.append({
                **position,
                "exit_time": exit_time,
                "net": _number(position["net"], "position net"),
            })
        reconstructed_drawdown = _reconstructed_drawdown_pct(
            normalized_positions, initial_capital
        )
        reconstructed_liquidations = sum(
            position["liquidated"] for position in normalized_positions
        )
        if not math.isclose(
            drawdown,
            reconstructed_drawdown,
            rel_tol=1e-9,
            abs_tol=1e-9,
        ):
            raise ValueError("validation drawdown conflicts with raw positions")
        if liquidations != reconstructed_liquidations:
            raise ValueError("validation liquidation count conflicts with raw positions")
        by_profile[profile].append(
            {
                **row,
                "period_start": start,
                "period_end": end,
                "initial_capital": initial_capital,
                "max_drawdown_pct": reconstructed_drawdown,
                "liquidation_count": reconstructed_liquidations,
                "positions": normalized_positions,
            }
        )
    for profile, rows in by_profile.items():
        identities = [_period_identity(row) for row in rows]
        if len(rows) < 2 or len(identities) != len(set(identities)):
            raise ValueError(f"{profile} validation periods are incomplete")
        ordered = sorted(rows, key=lambda row: (row["period_start"], row["period_end"]))
        if any(current["period_start"] <= previous["period_end"] for previous, current in zip(ordered, ordered[1:])):
            raise ValueError(f"{profile} validation periods overlap")
        if sum(row["scope"] == "final_holdout" for row in rows) != 1:
            raise ValueError(f"{profile} final holdout scope is invalid")
        if ordered[-1]["scope"] != "final_holdout":
            raise ValueError(
                f"{profile} final holdout must follow all walk-forward periods"
            )
        fold_indices = sorted(
            row["fold_index"] for row in rows if row["scope"] == "walk_forward"
        )
        if fold_indices != list(range(len(fold_indices))):
            raise ValueError(f"{profile} walk-forward fold indices are invalid")
    if {_period_identity(row) for row in by_profile["normal"]} != {
        _period_identity(row) for row in by_profile["stressed"]
    }:
        raise ValueError("normal and stressed validation periods do not match")
    stressed_by_period = {
        _period_identity(row): row for row in by_profile["stressed"]
    }
    for normal in by_profile["normal"]:
        stressed = stressed_by_period[_period_identity(normal)]
        if not math.isclose(
            normal["initial_capital"],
            stressed["initial_capital"],
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("normal and stressed initial capital does not match")
        normal_population = {
            position["position_id"]: (
                position["symbol"],
                position["regime"],
                position["exit_time"],
                position["liquidated"],
            )
            for position in normal["positions"]
        }
        stressed_population = {
            position["position_id"]: (
                position["symbol"],
                position["regime"],
                position["exit_time"],
                position["liquidated"],
            )
            for position in stressed["positions"]
        }
        if normal_population != stressed_population:
            raise ValueError(
                "normal and stressed position populations do not match"
            )
        normal_nets = {
            position["position_id"]: position["net"]
            for position in normal["positions"]
        }
        for position in stressed["positions"]:
            stressed_net = position["net"]
            normal_net = normal_nets[position["position_id"]]
            if stressed_net > normal_net and not math.isclose(
                stressed_net,
                normal_net,
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError(
                    "stressed validation net exceeds matching normal net"
                )
    return by_profile


def _positive_profit_share(positions: list[dict], key: str) -> float:
    totals = {}
    for row in positions:
        if row["net"] > 0.0:
            totals[row[key]] = totals.get(row[key], 0.0) + row["net"]
    total = math.fsum(totals.values())
    if total <= 0.0 or not totals:
        raise ValueError("positive validation profit distribution is unavailable")
    return max(totals.values()) / total


def build_validation_promotion_fragment(
    *,
    strategy: str,
    run_id: str,
    dataset_fingerprint: str,
    candidate_fingerprint: str,
    eligible_observation_count: int,
    covered_observation_count: int,
    noncausal_observation_count: int,
    periods: list[dict],
) -> dict:
    """Reconstruct a validation fragment from disjoint raw OOS positions."""
    eligible = _count(
        eligible_observation_count, "eligible observation count", positive=True
    )
    covered = _count(covered_observation_count, "covered observation count")
    noncausal = _count(noncausal_observation_count, "noncausal observation count")
    if covered > eligible or noncausal > eligible:
        raise ValueError("validation observation counts are inconsistent")
    coverage_value = covered / eligible
    profiles = _normalize_validation_periods(periods)
    normal_rows = profiles["normal"]
    stressed_rows = profiles["stressed"]
    normal_positions = [position for row in normal_rows for position in row["positions"]]
    if len(normal_positions) < 2:
        raise ValueError("validation position sample is insufficient")
    nets = [row["net"] for row in normal_positions]
    gross_win = math.fsum(max(net, 0.0) for net in nets)
    gross_loss = abs(math.fsum(min(net, 0.0) for net in nets))
    if gross_loss <= 0.0:
        raise ValueError("validation profit factor requires realized losses")
    mean = statistics.mean(nets)
    # Positions inside one disjoint OOS period share regime, liquidity and
    # market shocks and are not independent draws.  Use the intercept-only
    # cluster-robust standard error across periods instead of shrinking the
    # uncertainty as if every position were independent.
    populated_periods = [row for row in normal_rows if row["positions"]]
    if len(populated_periods) < 2:
        raise ValueError(
            "clustered validation confidence requires two populated OOS periods"
        )
    cluster_scores = [
        math.fsum(position["net"] - mean for position in row["positions"])
        for row in populated_periods
    ]
    cluster_count = len(cluster_scores)
    clustered_variance = (
        cluster_count
        / (cluster_count - 1)
        * math.fsum(score * score for score in cluster_scores)
        / (len(nets) * len(nets))
    )
    clustered_standard_error = math.sqrt(max(0.0, clustered_variance))
    critical = _one_sided_t95(cluster_count - 1)
    lower = mean - (critical * clustered_standard_error)
    normal_folds = [row for row in normal_rows if row["scope"] == "walk_forward"]
    stressed_folds = [row for row in stressed_rows if row["scope"] == "walk_forward"]
    normal_holdout = next(row for row in normal_rows if row["scope"] == "final_holdout")
    stressed_holdout = next(row for row in stressed_rows if row["scope"] == "final_holdout")
    def period_net(row: dict) -> float:
        return math.fsum(position["net"] for position in row["positions"])

    normal_positive = sum(period_net(row) > 0.0 for row in normal_folds)
    stressed_positive = sum(period_net(row) > 0.0 for row in stressed_folds)
    observed_regimes = {row["regime"] for row in normal_positions if row["regime"] != "UNKNOWN"}
    regime_labels_complete = all(
        row["regime"] != "UNKNOWN" for row in normal_positions
    )
    symbol_share = _positive_profit_share(normal_positions, "symbol")
    regime_share = _positive_profit_share(normal_positions, "regime")
    stressed_net = math.fsum(period_net(row) for row in stressed_rows)
    fields = {
        "causality_passed": noncausal == 0,
        "coverage": coverage_value,
        "oos_net": math.fsum(nets),
        "profit_factor": gross_win / gross_loss,
        "confidence_lower_bound": lower,
        "max_symbol_profit_share": symbol_share,
        "cost_stress_passed": stressed_net > 0.0,
        "sample_count": len(nets),
        "regime_stability_passed": (
            regime_labels_complete
            and len(observed_regimes) >= 2
            and regime_share <= 0.80
        ),
        "walk_forward_passed": normal_positive * 2 > len(normal_folds) and stressed_positive * 2 > len(stressed_folds),
        "walk_forward_positive_folds": normal_positive,
        "stressed_walk_forward_positive_folds": stressed_positive,
        "walk_forward_total_folds": len(normal_folds),
        "final_holdout_net_after_cost": period_net(normal_holdout),
        "stressed_final_holdout_net_after_cost": period_net(stressed_holdout),
        "oos_max_drawdown_pct": max(row["max_drawdown_pct"] for row in normal_rows),
        "stressed_oos_max_drawdown_pct": max(row["max_drawdown_pct"] for row in stressed_rows),
        "independent_position_count": len(nets),
        "observed_regime_count": len(observed_regimes),
        "max_regime_profit_share": regime_share,
        "oos_liquidation_count": sum(row["liquidation_count"] for row in normal_rows),
        "stressed_oos_liquidation_count": sum(row["liquidation_count"] for row in stressed_rows),
    }
    return seal_promotion_fragment(
        source_type="validation", strategy=strategy,
        candidate_fingerprint=candidate_fingerprint, source_run_id=run_id,
        dataset_fingerprint=dataset_fingerprint, fields=fields,
    )


def build_forward_shadow_promotion_fragment(
    *,
    strategy: str,
    run_id: str,
    dataset_fingerprint: str,
    candidate_fingerprint: str,
    period_start: float,
    period_end: float,
    positions: list[dict],
    capacity_samples: list[dict],
) -> dict:
    """Reconstruct forward outcomes, calibration and capacity from one period."""
    start = _number(period_start, "forward period start")
    end = _number(period_end, "forward period end")
    if end <= start:
        raise ValueError("forward period must be chronological")
    if not isinstance(positions, list) or not positions:
        raise ValueError("forward positions must be a non-empty list")
    normalized_positions = []
    position_ids = set()
    observed_utc_days = set()
    for row in positions:
        if not isinstance(row, dict) or set(row) != _SHADOW_POSITION_KEYS:
            raise ValueError("forward position structure is invalid")
        identity = row["position_id"]
        closed_at = _number(row["closed_at"], "forward close time")
        probability = _number(row["predicted_probability"], "predicted probability")
        outcome = row["outcome"]
        normal_net = _number(row["net_after_cost"], "forward net")
        stressed_net = _number(
            row["stressed_net_after_cost"], "stressed forward net"
        )
        if (
            identity in position_ids or not isinstance(identity, (str, int))
            or isinstance(identity, bool) or not str(identity).strip()
            or not start <= closed_at <= end or not 0.0 <= probability <= 1.0
            or type(outcome) is not int or outcome not in {0, 1}
        ):
            raise ValueError("forward position identity, time or outcome is invalid")
        if outcome != int(normal_net > 0.0):
            raise ValueError(
                "forward outcome does not match realized net"
            )
        if stressed_net > normal_net and not math.isclose(
            stressed_net, normal_net, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise ValueError(
                "stressed forward net exceeds normal net"
            )
        position_ids.add(identity)
        observed_utc_days.add(math.floor(closed_at / 86_400.0))
        normalized_positions.append({
            **row,
            "net_after_cost": normal_net,
            "stressed_net_after_cost": stressed_net,
            "predicted_probability": probability,
        })
    probabilities = [row["predicted_probability"] for row in normalized_positions]
    outcomes = [row["outcome"] for row in normalized_positions]
    if len(probabilities) < 2 or statistics.pvariance(probabilities) <= 0.0:
        raise ValueError("forward calibration sample has no probability variation")
    brier = statistics.mean((p - y) ** 2 for p, y in zip(probabilities, outcomes))
    mean_p = statistics.mean(probabilities)
    mean_y = statistics.mean(outcomes)
    slope = math.fsum((p - mean_p) * (y - mean_y) for p, y in zip(probabilities, outcomes)) / math.fsum((p - mean_p) ** 2 for p in probabilities)
    bins = [[] for _ in range(10)]
    for probability, outcome in zip(probabilities, outcomes):
        bins[min(9, int(probability * 10))].append((probability, outcome))
    ece = math.fsum(
        len(bucket) / len(probabilities) * abs(statistics.mean(p for p, _ in bucket) - statistics.mean(y for _, y in bucket))
        for bucket in bins if bucket
    )
    if not isinstance(capacity_samples, list) or not capacity_samples:
        raise ValueError("capacity samples must be a non-empty list")
    sample_ids = set()
    participations = []
    capacity_passed = True
    for row in capacity_samples:
        if not isinstance(row, dict) or set(row) != _CAPACITY_KEYS:
            raise ValueError("capacity sample structure is invalid")
        identity = row["sample_id"]
        measured_at = _number(row["measured_at"], "capacity sample time")
        participation = _number(row["participation_rate"], "participation rate")
        if (
            identity in sample_ids or not isinstance(identity, (str, int))
            or isinstance(identity, bool) or not str(identity).strip()
            or not start <= measured_at <= end or not 0.0 < participation <= 1.0
            or not isinstance(row["capacity_allowed"], bool)
        ):
            raise ValueError("capacity sample identity, time or value is invalid")
        sample_ids.add(identity)
        observed_utc_days.add(math.floor(measured_at / 86_400.0))
        participations.append(participation)
        capacity_passed = capacity_passed and row["capacity_allowed"]
    if end > time.time() + _FORWARD_CLOCK_FUTURE_TOLERANCE_SECONDS:
        raise ValueError("forward period end is in the future")
    fields = {
        "forward_shadow_days": min(
            len(observed_utc_days),
            math.floor((end - start) / 86_400.0),
        ),
        "calibration_brier": brier,
        "calibration_ece": ece,
        "calibration_slope": slope,
        "capacity_passed": capacity_passed,
        "forward_shadow_net_after_cost": math.fsum(row["net_after_cost"] for row in normalized_positions),
        "forward_shadow_position_count": len(normalized_positions),
        "forward_shadow_cost_stress_passed": math.fsum(row["stressed_net_after_cost"] for row in normalized_positions) > 0.0,
        "capacity_sample_count": len(capacity_samples),
        "maximum_capacity_participation_rate": max(participations),
    }
    return seal_promotion_fragment(
        source_type="forward_shadow", strategy=strategy,
        candidate_fingerprint=candidate_fingerprint, source_run_id=run_id,
        dataset_fingerprint=dataset_fingerprint, fields=fields,
    )


def _forward_fragment_from_report(report: Any) -> dict:
    if not isinstance(report, dict) or set(report) != _FORWARD_REPORT_KEYS:
        raise ValueError("forward shadow raw report structure is invalid")
    return build_forward_shadow_promotion_fragment(**report)


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: Path, *, label: str) -> Path:
    requested = path.expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} path must not contain links")
    return requested


def _read_existing_forward_artifact(path: Path, expected: bytes) -> bool:
    if _is_linklike(path) or not path.is_file():
        return False
    try:
        if path.stat().st_size != len(expected):
            return False
        with path.open("rb") as handle:
            return handle.read(len(expected) + 1) == expected
    except OSError:
        return False


def write_forward_shadow_promotion_artifact(
    report: dict, destination: str | Path
) -> dict:
    """Atomically persist raw forward rows together with their rebuilt fragment."""
    fragment = _forward_fragment_from_report(report)
    artifact = {
        "artifact_schema": 1,
        "raw_report": report,
        "fragment": fragment,
    }
    encoded = (
        json.dumps(
            artifact, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    if len(encoded) > _MAX_FORWARD_ARTIFACT_BYTES:
        raise ValueError("forward shadow promotion artifact is oversized")
    path = _absolute_without_links(
        Path(destination), label="forward shadow artifact destination"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path = _absolute_without_links(
        path, label="forward shadow artifact destination"
    )
    if path.exists():
        if _read_existing_forward_artifact(path, encoded):
            return artifact
        raise ValueError("forward shadow artifact destination conflict")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            if _read_existing_forward_artifact(path, encoded):
                return artifact
            raise ValueError(
                "forward shadow artifact destination conflict"
            ) from exc
        try:
            directory_fd = os.open(str(path.parent), os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                os.fsync(directory_fd)
            except OSError:
                pass
            finally:
                os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return artifact


def load_forward_shadow_promotion_artifact(source: str | Path) -> dict:
    """Load and completely rebuild one bounded forward promotion artifact."""
    try:
        path = _absolute_without_links(
            Path(source), label="forward shadow promotion artifact"
        )
    except ValueError as exc:
        raise ValueError(
            "forward shadow promotion artifact must be a real file"
        ) from exc
    if not path.is_file():
        raise ValueError("forward shadow promotion artifact must be a real file")
    with path.open("rb") as handle:
        raw = handle.read(_MAX_FORWARD_ARTIFACT_BYTES + 1)
    if len(raw) > _MAX_FORWARD_ARTIFACT_BYTES:
        raise ValueError("forward shadow promotion artifact is oversized")

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("forward shadow promotion artifact has duplicate keys")
            value[key] = item
        return value

    try:
        artifact = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("forward shadow promotion artifact is invalid JSON") from exc
    if (
        not isinstance(artifact, dict)
        or set(artifact) != {"artifact_schema", "raw_report", "fragment"}
        or type(artifact.get("artifact_schema")) is not int
        or artifact.get("artifact_schema") != 1
    ):
        raise ValueError("forward shadow promotion artifact structure is invalid")
    rebuilt = _forward_fragment_from_report(artifact.get("raw_report"))
    artifact_fragment = json.dumps(
        artifact.get("fragment"), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    rebuilt_fragment = json.dumps(
        rebuilt, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    if artifact_fragment != rebuilt_fragment:
        raise ValueError("forward shadow promotion artifact does not match raw rows")
    return artifact
