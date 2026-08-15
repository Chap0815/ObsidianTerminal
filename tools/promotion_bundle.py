"""Strict extraction and atomic persistence of complete promotion bundles."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

from tools.simulation_workspace import canonical_evidence_sha256
from trading.promotion_assembly import (
    assemble_promotion_envelope,
    verify_promotion_envelope,
    verify_promotion_fragment,
)
from trading.promotion_producers import load_forward_shadow_promotion_artifact

MAX_PROMOTION_SOURCE_BYTES = 16 * 1024 * 1024
MAX_PROMOTION_BUNDLE_BYTES = 16 * 1024 * 1024
MAX_PROMOTION_APPLY_ARTIFACT_BYTES = 40 * 1024 * 1024
OPTIMIZER_PROMOTION_ARTIFACT_SCHEMA = 1
PROMOTION_APPLY_ARTIFACT_SCHEMA = 1
PROMOTION_APPLY_ARTIFACT_KIND = "optimizer_promotion_apply"


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(source: str | Path, *, label: str) -> Path:
    requested = Path(source).expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} path must not contain links")
    return requested


def _load_unique_json_object(
    source: str | Path, *, maximum_bytes: int, label: str
) -> dict:
    try:
        path = _absolute_without_links(source, label=label)
    except ValueError as exc:
        raise ValueError(f"{label} must be a real file") from exc
    if not path.is_file():
        raise ValueError(f"{label} must be a real file")
    with path.open("rb") as handle:
        raw = handle.read(maximum_bytes + 1)
    if len(raw) > maximum_bytes:
        raise ValueError(f"{label} is oversized")

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate keys")
            result[key] = value
        return result

    def reject_constant(_value):
        raise ValueError(f"{label} contains a non-finite JSON constant")

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise TypeError(f"{label} must contain an object")
    return value


def extract_optimizer_fragment(payload: Any) -> dict:
    if type(payload) is not dict:
        raise ValueError("optimizer artifact must be an object")
    fragment = verify_promotion_fragment(payload.get("optimizer_promotion_fragment"))
    params = payload.get("optimizer_candidate_params")
    raw_inputs = payload.get("optimizer_promotion_inputs")
    if (
        type(payload.get("optimizer_artifact_schema")) is not int
        or payload.get("optimizer_artifact_schema")
        != OPTIMIZER_PROMOTION_ARTIFACT_SCHEMA
        or payload.get("research_only") is not True
        or payload.get("promotion_eligible") is not False
        or payload.get("changes_runtime") is not False
        or payload.get("optimizer_promotion_fragment_error") is not None
        or type(params) is not dict
        or type(raw_inputs) is not dict
        or set(raw_inputs)
        != {"dsr", "pbo", "outlier", "monte_carlo", "sensitivity"}
        or canonical_evidence_sha256(params)
        != payload.get("optimizer_candidate_fingerprint")
        or fragment["source_type"] != "optimizer"
        or fragment["strategy"] != payload.get("strategy")
        or fragment["source_run_id"] != payload.get("reproducible_run_id")
        or fragment["dataset_fingerprint"] != payload.get("dataset_fingerprint")
        or fragment["candidate_fingerprint"]
        != payload.get("optimizer_candidate_fingerprint")
    ):
        raise ValueError("optimizer fragment provenance does not match its artifact")
    from tools.optimizer import build_optimizer_promotion_fragment

    try:
        rebuilt = build_optimizer_promotion_fragment(
            strategy=payload.get("strategy"),
            run_id=payload.get("reproducible_run_id"),
            dataset_fingerprint=payload.get("dataset_fingerprint"),
            candidate_fingerprint=payload.get("optimizer_candidate_fingerprint"),
            **raw_inputs,
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("optimizer fragment raw inputs are invalid") from exc
    if _canonical_bytes(fragment) != _canonical_bytes(rebuilt):
        raise ValueError("optimizer fragment does not match its raw inputs")
    return fragment


def load_optimizer_fragment(source: str | Path) -> dict:
    payload = load_optimizer_promotion_artifact(source)
    return extract_optimizer_fragment(payload)


def load_optimizer_promotion_artifact(source: str | Path) -> dict:
    payload = _load_unique_json_object(
        source,
        maximum_bytes=MAX_PROMOTION_SOURCE_BYTES,
        label="optimizer promotion artifact",
    )
    extract_optimizer_fragment(payload)
    return payload


def extract_validation_fragment(report: Any) -> dict:
    if type(report) is not dict:
        raise ValueError("validation artifact must be an object")
    fragment = verify_promotion_fragment(report.get("validation_promotion_fragment"))
    dataset = report.get("dataset")
    params = report.get("candidate_params")
    evaluations = report.get("evaluations")
    cost_evidence = report.get("validation_cost_evidence")
    cost_profiles = report.get("cost_profiles")
    if (
        report.get("method") != "mexc_futures_capture_phase2_v1"
        or report.get("strategy") != "FUTURES"
        or report.get("research_only") is not True
        or report.get("changes_runtime") is not False
        or report.get("validation_promotion_fragment_error") is not None
        or type(dataset) is not dict
        or type(params) is not dict
        or type(evaluations) is not list
        or type(cost_evidence) is not dict
        or type(cost_profiles) is not dict
        or canonical_evidence_sha256(params)
        != report.get("candidate_fingerprint")
        or fragment["source_type"] != "validation"
        or fragment["strategy"] != report.get("strategy")
        or fragment["source_run_id"] != report.get("run_id")
        or fragment["dataset_fingerprint"] != dataset.get("dataset_fingerprint")
        or fragment["candidate_fingerprint"] != report.get("candidate_fingerprint")
    ):
        raise ValueError("validation fragment provenance does not match its artifact")
    expected_cost_hash = cost_evidence.get("evidence_sha256")
    cost_body = {
        key: value
        for key, value in cost_evidence.items()
        if key != "evidence_sha256"
    }
    from trading.capture_replay_validation import (
        COST_EVIDENCE_SCHEMA,
        calibrated_cost_profiles,
    )

    if (
        cost_body.get("schema_version") != COST_EVIDENCE_SCHEMA
        or cost_body.get("kind") != "mexc_sim_execution_cost_calibration"
        or not isinstance(expected_cost_hash, str)
        or expected_cost_hash != canonical_evidence_sha256(cost_body)
    ):
        raise ValueError("validation cost evidence integrity check failed")
    try:
        rebuilt_profiles = calibrated_cost_profiles(
            cost_evidence,
            default_round_trip_bps=cost_profiles.get("default_round_trip_bps"),
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("validation cost profiles are invalid") from exc
    if _canonical_bytes(cost_profiles) != _canonical_bytes(rebuilt_profiles):
        raise ValueError("validation cost profiles do not match their evidence")
    from tools.futures_capture_phase2 import build_validation_fragment_inputs
    from trading.promotion_producers import build_validation_promotion_fragment

    try:
        raw_inputs = build_validation_fragment_inputs(evaluations, cost_evidence)
        rebuilt = build_validation_promotion_fragment(
            strategy=report.get("strategy"),
            run_id=report.get("run_id"),
            dataset_fingerprint=dataset.get("dataset_fingerprint"),
            candidate_fingerprint=report.get("candidate_fingerprint"),
            **raw_inputs,
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("validation fragment raw evidence is invalid") from exc
    if _canonical_bytes(fragment) != _canonical_bytes(rebuilt):
        raise ValueError("validation fragment does not match its raw evidence")
    return fragment


def load_validation_fragment(source: str | Path) -> dict:
    report = _load_unique_json_object(
        source,
        maximum_bytes=MAX_PROMOTION_SOURCE_BYTES,
        label="validation promotion artifact",
    )
    return extract_validation_fragment(report)


def extract_forward_fragment(path: str | Path) -> dict:
    artifact = load_forward_shadow_promotion_artifact(path)
    fragment = verify_promotion_fragment(artifact["fragment"])
    if fragment["source_type"] != "forward_shadow":
        raise ValueError("forward artifact contains the wrong fragment source")
    return fragment


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _read_existing_bytes(path: Path, *, maximum_bytes: int, label: str) -> bytes:
    if _is_linklike(path) or not path.is_file():
        raise ValueError(f"{label} destination conflicts with a non-file")
    with path.open("rb") as handle:
        existing = handle.read(maximum_bytes + 1)
    if len(existing) > maximum_bytes:
        raise ValueError(f"{label} destination is oversized")
    return existing


def _write_immutable_json(
    value: dict,
    destination: str | Path,
    *,
    maximum_bytes: int,
    label: str,
) -> None:
    encoded = _canonical_bytes(value) + b"\n"
    if len(encoded) > maximum_bytes:
        raise ValueError(f"{label} is oversized")
    path = _absolute_without_links(destination, label=f"{label} destination")
    path.parent.mkdir(parents=True, exist_ok=True)
    path = _absolute_without_links(path, label=f"{label} destination")
    if path.exists():
        if _read_existing_bytes(
            path, maximum_bytes=maximum_bytes, label=label
        ) != encoded:
            raise ValueError(f"{label} destination conflicts with existing evidence")
        return

    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if _read_existing_bytes(
                path, maximum_bytes=maximum_bytes, label=label
            ) != encoded:
                raise ValueError(
                    f"{label} destination conflicts with concurrently written evidence"
                )
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


def write_optimizer_promotion_artifact(
    payload: dict, destination: str | Path
) -> dict:
    """Verify and immutably persist one reproducible optimizer source artifact."""
    normalized = json.loads(_canonical_bytes(payload))
    extract_optimizer_fragment(normalized)
    _write_immutable_json(
        normalized,
        destination,
        maximum_bytes=MAX_PROMOTION_SOURCE_BYTES,
        label="optimizer promotion artifact",
    )
    return normalized


def _write_promotion_bundle(
    fragments: list[dict], destination: str | Path, *,
    minimum_samples: int, manual_live_approval: bool,
) -> dict:
    envelope = assemble_promotion_envelope(
        fragments, minimum_samples=minimum_samples,
        manual_live_approval=manual_live_approval,
    )
    artifact = {"artifact_schema": 1, "promotion_envelope": envelope}
    _write_immutable_json(
        artifact,
        destination,
        maximum_bytes=MAX_PROMOTION_BUNDLE_BYTES,
        label="promotion bundle artifact",
    )
    return artifact


def write_promotion_bundle(
    optimizer_source: str | Path,
    validation_source: str | Path,
    forward_source: str | Path,
    destination: str | Path,
    *,
    minimum_samples: int,
    manual_live_approval: bool,
) -> dict:
    """Verify all three source artifacts and atomically persist their envelope."""
    fragments = [
        load_optimizer_fragment(optimizer_source),
        load_validation_fragment(validation_source),
        extract_forward_fragment(forward_source),
    ]
    return _write_promotion_bundle(
        fragments,
        destination,
        minimum_samples=minimum_samples,
        manual_live_approval=manual_live_approval,
    )


def load_promotion_bundle(source: str | Path) -> dict:
    artifact = _load_unique_json_object(
        source,
        maximum_bytes=MAX_PROMOTION_BUNDLE_BYTES,
        label="promotion bundle artifact",
    )
    return _verify_promotion_bundle_artifact(artifact)


def _verify_promotion_bundle_artifact(artifact: Any) -> dict:
    if (
        not isinstance(artifact, dict)
        or set(artifact) != {"artifact_schema", "promotion_envelope"}
        or type(artifact.get("artifact_schema")) is not int
        or artifact["artifact_schema"] != 1
    ):
        raise ValueError("promotion bundle structure is invalid")
    rebuilt = verify_promotion_envelope(artifact.get("promotion_envelope"))
    expected = {"artifact_schema": 1, "promotion_envelope": rebuilt}
    if _canonical_bytes(artifact) != _canonical_bytes(expected):
        raise ValueError("promotion bundle does not match its sealed sources")
    return expected


def _build_promotion_apply_artifact(
    optimizer_source: dict, promotion_bundle: dict
) -> dict:
    normalized_optimizer = json.loads(_canonical_bytes(optimizer_source))
    optimizer_fragment = extract_optimizer_fragment(normalized_optimizer)
    normalized_bundle = _verify_promotion_bundle_artifact(
        json.loads(_canonical_bytes(promotion_bundle))
    )
    envelope = normalized_bundle["promotion_envelope"]
    bundle_optimizer = next(
        fragment
        for fragment in envelope["source_fragments"]
        if fragment["source_type"] == "optimizer"
    )
    if _canonical_bytes(optimizer_fragment) != _canonical_bytes(bundle_optimizer):
        raise ValueError("promotion bundle is bound to a different optimizer source")

    from trading.profit_research_runner import check_promotion

    decision = check_promotion(
        envelope["evidence"],
        minimum_samples=envelope["minimum_samples"],
        manual_live_approval=envelope["manual_live_approval"],
    )
    if (
        decision.get("research_passed") is not True
        or decision.get("live_allowed") is not True
        or decision.get("deployment_performed") is not False
        or decision.get("decision_input_schema")
        != envelope.get("decision_input_schema")
        or decision.get("decision_input_sha256")
        != envelope.get("decision_input_sha256")
    ):
        raise ValueError("promotion bundle is not approved for manual apply")

    apply_payload = dict(normalized_optimizer)
    apply_payload.pop("optimizer_promotion_artifact_path", None)
    apply_payload.pop("optimizer_promotion_artifact_error", None)
    apply_payload.update({
        "research_only": False,
        "promotion_eligible": True,
        "changes_runtime": False,
        "promotion_envelope": envelope,
    })
    artifact = {
        "artifact_schema": PROMOTION_APPLY_ARTIFACT_SCHEMA,
        "artifact_kind": PROMOTION_APPLY_ARTIFACT_KIND,
        "optimizer_source_sha256": canonical_evidence_sha256(
            normalized_optimizer
        ),
        "promotion_bundle_sha256": canonical_evidence_sha256(
            normalized_bundle
        ),
        "optimizer_source": normalized_optimizer,
        "promotion_bundle": normalized_bundle,
    }
    apply_payload["promotion_apply_provenance"] = {
        "artifact_schema": PROMOTION_APPLY_ARTIFACT_SCHEMA,
        "artifact_kind": PROMOTION_APPLY_ARTIFACT_KIND,
        "promotion_apply_artifact_sha256": canonical_evidence_sha256(artifact),
        "optimizer_source_sha256": artifact["optimizer_source_sha256"],
        "promotion_bundle_sha256": artifact["promotion_bundle_sha256"],
    }
    return {"artifact": artifact, "apply_payload": apply_payload}


def write_promotion_apply_artifact(
    optimizer_source: str | Path,
    promotion_bundle: str | Path,
    destination: str | Path,
) -> dict:
    """Bind one verified optimizer source to one positive promotion bundle."""
    result = _build_promotion_apply_artifact(
        load_optimizer_promotion_artifact(optimizer_source),
        load_promotion_bundle(promotion_bundle),
    )
    _write_immutable_json(
        result["artifact"],
        destination,
        maximum_bytes=MAX_PROMOTION_APPLY_ARTIFACT_BYTES,
        label="promotion apply artifact",
    )
    return result


def load_promotion_apply_artifact(source: str | Path) -> dict:
    artifact = _load_unique_json_object(
        source,
        maximum_bytes=MAX_PROMOTION_APPLY_ARTIFACT_BYTES,
        label="promotion apply artifact",
    )
    if (
        set(artifact)
        != {
            "artifact_schema",
            "artifact_kind",
            "optimizer_source_sha256",
            "promotion_bundle_sha256",
            "optimizer_source",
            "promotion_bundle",
        }
        or type(artifact.get("artifact_schema")) is not int
        or artifact.get("artifact_schema") != PROMOTION_APPLY_ARTIFACT_SCHEMA
        or artifact.get("artifact_kind") != PROMOTION_APPLY_ARTIFACT_KIND
        or type(artifact.get("optimizer_source")) is not dict
        or type(artifact.get("promotion_bundle")) is not dict
        or artifact.get("optimizer_source_sha256")
        != canonical_evidence_sha256(artifact.get("optimizer_source"))
        or artifact.get("promotion_bundle_sha256")
        != canonical_evidence_sha256(artifact.get("promotion_bundle"))
    ):
        raise ValueError("promotion apply artifact structure is invalid")
    rebuilt = _build_promotion_apply_artifact(
        artifact["optimizer_source"], artifact["promotion_bundle"]
    )
    if _canonical_bytes(artifact) != _canonical_bytes(rebuilt["artifact"]):
        raise ValueError("promotion apply artifact does not match its sources")
    return rebuilt


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _cli_summary(operation: str, artifact: dict) -> dict:
    envelope = artifact["promotion_envelope"]
    from trading.profit_research_runner import check_promotion

    decision = check_promotion(
        envelope["evidence"],
        minimum_samples=envelope["minimum_samples"],
        manual_live_approval=envelope["manual_live_approval"],
    )
    return {
        "operation": operation,
        "artifact_schema": artifact["artifact_schema"],
        "strategy": envelope["evidence"]["optimizer_strategy"],
        "candidate_fingerprint": envelope["evidence"][
            "optimizer_candidate_fingerprint"
        ],
        "source_bundle_sha256": envelope["source_bundle_sha256"],
        "decision_input_sha256": envelope["decision_input_sha256"],
        "research_passed": decision["research_passed"],
        "live_allowed": decision["live_allowed"],
        "deployment_performed": decision["deployment_performed"],
    }


def _apply_cli_summary(operation: str, result: dict) -> dict:
    summary = _cli_summary(operation, result["artifact"]["promotion_bundle"])
    summary.update({
        "artifact_schema": result["artifact"]["artifact_schema"],
        "artifact_kind": result["artifact"]["artifact_kind"],
        "optimizer_source_sha256": result["artifact"][
            "optimizer_source_sha256"
        ],
        "promotion_bundle_sha256": result["artifact"][
            "promotion_bundle_sha256"
        ],
    })
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    assemble = commands.add_parser(
        "assemble", help="verify three source artifacts and create one bundle"
    )
    assemble.add_argument("--optimizer", type=Path, required=True)
    assemble.add_argument("--validation", type=Path, required=True)
    assemble.add_argument("--forward", type=Path, required=True)
    assemble.add_argument("--output", type=Path, required=True)
    assemble.add_argument(
        "--minimum-samples", type=_positive_integer, required=True
    )
    assemble.add_argument("--manual-live-approval", action="store_true")
    verify = commands.add_parser(
        "verify", help="rebuild and verify an existing promotion bundle"
    )
    verify.add_argument("--bundle", type=Path, required=True)
    finalize = commands.add_parser(
        "finalize", help="bind one positive bundle to its optimizer source"
    )
    finalize.add_argument("--optimizer", type=Path, required=True)
    finalize.add_argument("--bundle", type=Path, required=True)
    finalize.add_argument("--output", type=Path, required=True)
    verify_apply = commands.add_parser(
        "verify-apply", help="rebuild and verify a promotion apply artifact"
    )
    verify_apply.add_argument("--artifact", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "assemble":
            artifact = write_promotion_bundle(
                args.optimizer,
                args.validation,
                args.forward,
                args.output,
                minimum_samples=args.minimum_samples,
                manual_live_approval=args.manual_live_approval,
            )
            summary = _cli_summary(args.command, artifact)
        elif args.command == "verify":
            artifact = load_promotion_bundle(args.bundle)
            summary = _cli_summary(args.command, artifact)
        elif args.command == "finalize":
            result = write_promotion_apply_artifact(
                args.optimizer, args.bundle, args.output
            )
            summary = _apply_cli_summary(args.command, result)
        else:
            result = load_promotion_apply_artifact(args.artifact)
            summary = _apply_cli_summary(args.command, result)
    except (OSError, TypeError, ValueError, OverflowError) as exc:
        print(
            f"ERROR: {type(exc).__name__}: {str(exc)[:240]}",
            file=sys.stderr,
        )
        return 2
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
