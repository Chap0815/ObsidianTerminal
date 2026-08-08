"""Operator CLI for fail-closed profit research; never places orders."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trading.profit_research_runner import (  # noqa: E402
    build_carry_preview,
    build_research_status,
    check_promotion,
    register_experiment_trial,
    train_expectancy_candidate,
)
from trading.research_experiment_suite import (  # noqa: E402
    EXPERIMENT_CATALOG,
    run_research_experiments,
    write_immutable_experiment_report,
)

RESEARCH_JSON_MAX_BYTES = 4 * 1024 * 1024


def _reject_json_constant(value: str):
    raise ValueError(f"non-finite JSON constant is not allowed: {value}")


def _json_file(path: str) -> dict:
    with Path(path).open("rb") as handle:
        raw = handle.read(RESEARCH_JSON_MAX_BYTES + 1)
    if len(raw) > RESEARCH_JSON_MAX_BYTES:
        raise ValueError(
            f"JSON input exceeds size limit ({RESEARCH_JSON_MAX_BYTES} bytes)"
        )
    payload = json.loads(
        raw.decode("utf-8-sig"), parse_constant=_reject_json_constant
    )
    if not isinstance(payload, dict):
        raise ValueError("JSON input must be an object")
    return payload


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def _finite_float(value: str, *, positive: bool) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise argparse.ArgumentTypeError("must be a finite number") from exc
    if not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("must be a finite number")
    if positive and parsed <= 0.0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    if not positive and parsed < 0.0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def _positive_float(value: str) -> float:
    return _finite_float(value, positive=True)


def _nonnegative_float(value: str) -> float:
    return _finite_float(value, positive=False)


def _print(payload: dict) -> None:
    print(json.dumps(payload, default=str, sort_keys=True, indent=2, allow_nan=False))


def _write_report(root: Path, payload: dict) -> Path:
    from datetime import datetime, timezone

    folder = root / "data" / "research" / "reports"
    folder.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = folder / f"profit_research_status_{stamp}_{uuid.uuid4().hex[:8]}.json"
    temp = path.with_suffix(".tmp")
    encoded = json.dumps(
        payload, default=str, sort_keys=True, indent=2, allow_nan=False
    ).encode("utf-8")
    with temp.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)
    return path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect and run research-only profit components. No command places "
            "orders or automatically deploys a model."
        )
    )
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="read-only readiness and data report")
    status.add_argument("--minimum-cost-samples", type=_positive_int, default=50)
    status.add_argument("--minimum-expectancy-rows", type=_positive_int, default=1200)
    status.add_argument("--write-report", action="store_true")

    train = sub.add_parser(
        "train-expectancy", help="write a candidate model outside data/models"
    )
    train.add_argument("--bot", required=True)
    train.add_argument("--mode", required=True, choices=("LIVE", "SIM", "live", "sim"))
    train.add_argument("--min-train", type=_positive_int, default=1000)
    train.add_argument("--test-size", type=_positive_int, default=200)
    train.add_argument("--purge-days", type=_nonnegative_int, default=8)
    train.add_argument("--schema-version", type=_positive_int)

    carry = sub.add_parser("carry-preview", help="SIM-only carry candidate preview")
    carry.add_argument("--notional", type=_positive_float, default=100.0)
    carry.add_argument("--funding-periods", type=_positive_int, default=3)
    carry.add_argument("--taker-fee", type=_nonnegative_float, default=0.001)
    carry.add_argument("--maker-fee", type=_nonnegative_float, default=0.0002)
    carry.add_argument("--entry-slippage-bps", type=_nonnegative_float, default=2.0)
    carry.add_argument("--exit-slippage-bps", type=_nonnegative_float, default=2.0)

    promotion = sub.add_parser(
        "promotion-check", help="evaluate evidence without deploying anything"
    )
    promotion.add_argument("--evidence", required=True)
    promotion.add_argument("--minimum-samples", type=_positive_int, required=True)
    promotion.add_argument("--manual-live-approval", action="store_true")

    trial = sub.add_parser(
        "register-trial", help="explicitly append one immutable experiment trial"
    )
    trial.add_argument("--trial-id", required=True)
    trial.add_argument("--name", required=True)
    trial.add_argument("--params", required=True)
    trial.add_argument(
        "--status", required=True,
        choices=("PLANNED", "RUNNING", "REJECTED", "COMPLETE"),
    )

    sub.add_parser(
        "experiment-catalog",
        help="show all research-only experiment definitions",
    )
    experiments = sub.add_parser(
        "run-experiments",
        help="run every measurable experiment without changing trading",
    )
    experiments.add_argument("--bot", required=True)
    experiments.add_argument(
        "--mode", required=True, choices=("LIVE", "SIM", "live", "sim")
    )
    experiments.add_argument("--minimum-expectancy-rows", type=_positive_int, default=1200)
    experiments.add_argument("--minimum-entry-rows", type=_positive_int, default=200)
    experiments.add_argument("--minimum-cost-samples", type=_positive_int, default=50)
    experiments.add_argument("--minimum-momentum-windows", type=_positive_int, default=30)
    experiments.add_argument("--write-report", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.root.resolve()
    try:
        if args.command == "status":
            payload = build_research_status(
                root,
                minimum_cost_samples=args.minimum_cost_samples,
                minimum_expectancy_rows=args.minimum_expectancy_rows,
            )
            if args.write_report:
                payload["report"] = str(_write_report(root, payload))
            _print(payload)
            return 0
        if args.command == "train-expectancy":
            _print(
                train_expectancy_candidate(
                    root,
                    bot=args.bot,
                    mode=args.mode,
                    min_train=args.min_train,
                    test_size=args.test_size,
                    purge_days=args.purge_days,
                    schema_version=args.schema_version,
                )
            )
            return 0
        if args.command == "carry-preview":
            _print(
                build_carry_preview(
                    root,
                    notional_usdt=args.notional,
                    expected_funding_periods=args.funding_periods,
                    taker_fee_rate=args.taker_fee,
                    maker_fee_rate=args.maker_fee,
                    entry_slippage_bps_per_leg=args.entry_slippage_bps,
                    exit_slippage_bps_per_leg=args.exit_slippage_bps,
                )
            )
            return 0
        if args.command == "promotion-check":
            payload = check_promotion(
                _json_file(args.evidence),
                minimum_samples=args.minimum_samples,
                manual_live_approval=args.manual_live_approval,
            )
            _print(payload)
            return 0 if payload["research_passed"] else 3
        if args.command == "register-trial":
            _print(
                register_experiment_trial(
                    root,
                    trial_id=args.trial_id,
                    experiment_name=args.name,
                    params=_json_file(args.params),
                    status=args.status,
                )
            )
            return 0
        if args.command == "experiment-catalog":
            from dataclasses import asdict

            _print(
                {
                    "experiments": [asdict(item) for item in EXPERIMENT_CATALOG],
                    "changes_orders": False,
                    "automatic_promotion": False,
                }
            )
            return 0
        if args.command == "run-experiments":
            payload = run_research_experiments(
                root,
                bot=args.bot,
                mode=args.mode,
                minimum_expectancy_rows=args.minimum_expectancy_rows,
                minimum_entry_rows=args.minimum_entry_rows,
                minimum_cost_samples=args.minimum_cost_samples,
                minimum_momentum_windows=args.minimum_momentum_windows,
            )
            if args.write_report:
                payload["report"] = str(
                    write_immutable_experiment_report(root, payload)
                )
            _print(payload)
            return 0
    except Exception as exc:
        _print({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
