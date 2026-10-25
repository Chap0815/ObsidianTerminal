"""Explicit administration and offline training of the funding forward corpus.

Use ``python -m tools.funding_forward_corpus``. This command never promotes a
model, initializes a runtime database, or repairs missing historical evidence.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from importlib import import_module
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

from sqlite_safety import harden_sqlite_connection


MAX_CANDIDATES = 10_000
WRITE_DEADLINE_SECONDS = 30.0
MAX_TRAINING_SOURCE_BYTES = 2 * 1024 * 1024
_TRAINING_SOURCE_FILES = (
    "tools/funding_forward_corpus.py", "trading/expectancy_training.py",
    "trading/expectancy_runtime.py", "trading/expectancy_telemetry.py", "requirements.lock.txt",
    "tools/simulation_workspace.py", "trading/experiment_registry_contract.py",
    "trading/profit_research_runner.py", "sqlite_safety.py",
)
_REQUIRED_TABLES = frozenset({
    "schema_versions", "expectancy_candidates", "trades",
    "funding_forward_source_sets", "funding_forward_source_chunks",
    "funding_forward_generations", "candidate_feature_availability",
    "sim_feature_entry_bindings", "sim_trade_fragment_availability",
})


def _generation_api():
    return import_module("trading.funding_forward_generation")


def _plain(path, label):
    from trading.expectancy_runtime import _absolute_without_links
    return _absolute_without_links(path, label=label)


def _aware(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise argparse.ArgumentTypeError("as-of must be an explicit timezone-aware ISO timestamp") from exc


def _bounded_int(text, minimum, maximum):
    if not text.isascii() or not text.isdecimal():
        raise argparse.ArgumentTypeError("argument must be a bounded positive integer")
    result = int(text)
    if not minimum <= result <= maximum:
        raise argparse.ArgumentTypeError(f"argument must be between {minimum} and {maximum}")
    return result


def _sha_argument(text):
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise argparse.ArgumentTypeError("source cursor must be a lowercase SHA-256 identity")
    return text


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    sub = parser.add_subparsers(dest="command", required=True)
    register = sub.add_parser("register", help="archive 1..16 native funding snapshots now")
    register.add_argument("--snapshot", type=Path, nargs="+", required=True)
    sub.add_parser("seal", help="consider all bounded closed FUTURES SIM entries chronologically")
    recover = sub.add_parser("recover", help="retry bounded existing pending generation witnesses")
    recover.add_argument("--limit", type=lambda x: _bounded_int(x, 1, 25), default=25)
    recover.add_argument("--source-after", type=_sha_argument)
    status = sub.add_parser("status", help="read-only corpus counts; no clock renewal")
    status.add_argument("--as-of", type=_aware)
    train = sub.add_parser("train", help="offline research model into a new output directory")
    train.add_argument("--as-of", type=_aware, required=True)
    train.add_argument("--output", type=Path, required=True)
    train.add_argument("--min-train", type=lambda x: _bounded_int(x, 30, MAX_CANDIDATES), default=1000)
    train.add_argument("--test-size", type=lambda x: _bounded_int(x, 1, MAX_CANDIDATES), default=200)
    train.add_argument("--purge-days", type=lambda x: _bounded_int(x, 0, 36500), default=8)
    train.add_argument("--include-consumed-regime-context", action="store_true",
                       help="include original verified context snapshots; legacy contexts remain unknown")
    train.add_argument("--publish-ui-status", action="store_true",
                       help="record this terminal offline attempt for the local UI; never activate a model")
    from tools.simulation_workspace import add_trial_registry_arguments
    add_trial_registry_arguments(train)
    return parser


def _known_database(root):
    project = _plain(root, "project root")
    database = _plain(project / "data" / "trading_bot.db", "forward corpus database")
    if not project.is_dir() or not database.is_file():
        raise ValueError("an existing project/data/trading_bot.db is required")
    return project, database


def _connect(database, *, writable=False):
    database = _plain(database, "forward corpus database")
    if not database.is_file():
        raise ValueError("forward corpus database must already exist")
    connection = harden_sqlite_connection(sqlite3.connect(
        database.as_uri() + ("?mode=rw" if writable else "?mode=ro"),
        uri=True, timeout=1.0,
    ))
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=1000")
        if writable:
            connection.execute("PRAGMA foreign_keys=ON")
            if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
                raise sqlite3.OperationalError("foreign key protection unavailable")
    except BaseException as primary:
        try:
            connection.close()
        except BaseException as cleanup:
            primary.add_note(f"forward corpus setup close failed: {type(cleanup).__name__}")
        raise
    return connection


def _schema(connection):
    for name in _REQUIRED_TABLES:
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is None:
            raise ValueError("supported schema 14 migration is required before this command")
    row = connection.execute("SELECT MAX(version) FROM schema_versions").fetchone()
    if type(row[0]) is not int or row[0] < 14:
        raise ValueError("supported schema 14 migration is required before this command")


def _read(database, action):
    from trading.capture_replay_validation import (
        _start_evidence_read_budget, _finish_bounded_read_transaction,
    )
    connection = _connect(database)
    primary = None
    try:
        budget = _start_evidence_read_budget(connection)
        connection.execute("BEGIN")
        _schema(connection)
        result = action(connection)
        _check_budget(budget)
        return result
    except BaseException as exc:
        primary = exc
        raise
    finally:
        _finish_bounded_read_transaction(connection, primary)


def _write(database, action):
    from trading.capture_replay_validation import (
        _start_evidence_read_budget, _finish_bounded_read_transaction,
    )
    connection = _connect(database, writable=True)
    primary = None
    try:
        budget = _start_evidence_read_budget(connection)
        _schema(connection)
        result = action(connection)
        _check_budget(budget)
        return result
    except BaseException as exc:
        primary = exc
        raise
    finally:
        _finish_bounded_read_transaction(connection, primary)


def _check_budget(budget):
    from trading.capture_replay_validation import MAX_CALIBRATION_READ_ROWS, MAX_CALIBRATION_READ_BYTES
    budget.check()
    # A nested optional helper cannot turn a caught row() failure into success.
    if budget.rows > MAX_CALIBRATION_READ_ROWS or budget.bytes > MAX_CALIBRATION_READ_BYTES:
        raise RuntimeError("calibration read budget exceeded: rows/bytes")


def _snapshot_inputs(paths):
    from tools import native_funding_snapshot as native
    from trading.funding_forward_corpus import MAX_ARCHIVE_BYTES, MAX_SOURCES
    from trading.settled_funding_model import provider_from_native_snapshots
    if not 1 <= len(paths) <= MAX_SOURCES:
        raise ValueError("register requires 1..16 snapshots")
    checked, values, total = [], [], 0
    for path in paths:
        path = _plain(path, "native snapshot")
        if not path.is_file():
            raise ValueError("native snapshot must be an existing file")
        total += path.stat().st_size
        if total > MAX_ARCHIVE_BYTES:
            raise ValueError("native snapshots exceed the 64MiB archive budget")
        values.append(native.read_snapshot(path))
        checked.append(path)
    provider_from_native_snapshots(values)
    return checked


def _closed_entries(connection, policy_id):
    rows = connection.execute("""SELECT CASE WHEN length(CAST(candidate.entry_id AS BLOB))
            BETWEEN 1 AND 200 THEN candidate.entry_id ELSE NULL END,
          EXISTS (SELECT 1 FROM trades final WHERE final.entry_id=candidate.entry_id
            AND final.bot_name='FUTURES (SIM)' AND final.is_sim=1 AND final.is_futures=1
            AND final.is_partial=0),
          EXISTS (SELECT 1 FROM funding_forward_generations generation
            WHERE generation.entry_id=candidate.entry_id AND generation.policy_id=?)
        FROM expectancy_candidates candidate
        WHERE candidate.bot_name='FUTURES' AND candidate.mode='SIM'
          AND EXISTS (SELECT 1 FROM trades trade WHERE trade.entry_id=candidate.entry_id
            AND trade.bot_name='FUTURES (SIM)' AND trade.is_sim=1 AND trade.is_futures=1
            AND (trade.is_partial=0 OR trade.is_partial IS NULL))
        ORDER BY candidate.candidate_time,candidate.entry_id LIMIT ?""",
        (policy_id, MAX_CANDIDATES + 1,)).fetchall()
    if len(rows) > MAX_CANDIDATES:
        raise ValueError("closed candidate scan exceeds the complete 10000-entry budget")
    if any(type(row[0]) is not str or row[0] != row[0].strip() for row in rows):
        raise ValueError("closed candidate scan contains an invalid entry identity")
    # NULL is historical uncertainty, not proof of a terminal campaign. Keep
    # it in the bounded inventory without sending it to the sealer as closed.
    return {"entries": [row[0] for row in rows if row[1] == 1 and row[2] == 0],
            "closed_entries_considered": sum(row[1] == 1 for row in rows),
            "skipped_existing_generation": sum(row[1] == 1 and row[2] == 1 for row in rows),
            "unsupported_legacy_null_partial_entries": sum(row[1] == 0 for row in rows)}


def _public_clock_probe():
    import ccxt
    from config.exchange_config import _publish_clock_offset
    from core.clock import availability_upper_bound_ms
    exchange = ccxt.mexc({
        "apiKey": "", "secret": "", "password": "", "enableRateLimit": True,
        "timeout": 10_000, "options": {"defaultType": "swap", "maxRetriesOnFailure": 0},
    })
    primary = None
    try:
        if any(getattr(exchange, field, None) != "" for field in ("apiKey", "secret", "password")):
            raise ValueError("anonymous public clock client must have empty credentials")
        _publish_clock_offset(exchange)
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            exchange.close()
        except BaseException as cleanup:
            if primary is None:
                raise
            try:
                primary.add_note(f"close public clock client failed: {type(cleanup).__name__}")
            except BaseException:
                pass
    return {"clock_probe_requested": True,
            "measured_clock_available": availability_upper_bound_ms() is not None}


def _state_counts(connection):
    return {
        table: {row[0]: row[1] for row in connection.execute(
            f"SELECT state,COUNT(*) FROM {table} GROUP BY state ORDER BY state")}
        for table in ("funding_forward_source_sets", "funding_forward_generations")
    }


def _native(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _native(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_native(item) for item in value]
    return value


def _policy(api):
    policy = api.current_funding_model_policy()
    if type(policy) is not dict:
        raise ValueError("invalid current funding model policy")
    identity = policy.get("policy_id")
    if (type(identity) is not str or len(identity) != 64
            or any(char not in "0123456789abcdef" for char in identity)):
        raise ValueError("invalid current funding model policy identity")
    return json.loads(json.dumps(policy, allow_nan=False))


def _require_policy(api, expected):
    if _policy(api) != expected:
        raise ValueError("funding model policy changed during CLI operation")


def _write_with_policy(api, expected, database, action):
    def guarded(connection):
        _require_policy(api, expected)
        result = action(connection)
        _require_policy(api, expected)
        return result
    return _write(database, guarded)


def _seal(api, database, inventory, policy):
    entries = inventory["entries"]
    rejected, states = Counter(), Counter()
    deadline = time.monotonic() + WRITE_DEADLINE_SECONDS

    def action(connection):
        for identity in entries:
            _require_policy(api, policy)
            if time.monotonic() > deadline:
                raise TimeoutError("forward seal operation exceeded its 30s deadline")
            try:
                result = api.seal_funding_model_generation(connection, entry_id=identity)
            except api.FundingCorpusUnavailable as exc:
                rejected[str(exc)] += 1
            else:
                if result.get("policy_id", policy["policy_id"]) != policy["policy_id"]:
                    raise ValueError("sealed generation belongs to a different funding model policy")
                states[result["state"]] += 1
            _require_policy(api, policy)
            if time.monotonic() > deadline:
                raise TimeoutError("forward seal operation exceeded its 30s deadline")
        return {"closed_entries_considered": inventory["closed_entries_considered"],
                "skipped_existing_generation": inventory["skipped_existing_generation"],
                "new_entries_considered": len(entries), "generation_states": dict(states),
                "rejections": dict(sorted(rejected.items()))}
    return _write_with_policy(api, policy, database, action)


def _training_source_identity(source_root=None, *, include_ui_status=False):
    root = Path(__file__).resolve().parents[1] if source_root is None else Path(source_root)
    sources = {}
    names = _TRAINING_SOURCE_FILES + (("trading/training_status.py", "bot_utils/atomic_publish.py") if include_ui_status else ())
    for name in names:
        path = _plain(root / name, "forward training source")
        with path.open("rb") as stream:
            raw = stream.read(MAX_TRAINING_SOURCE_BYTES + 1)
        if not raw or len(raw) > MAX_TRAINING_SOURCE_BYTES:
            raise ValueError("forward training source byte budget exceeded")
        sources[name] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    return sources


def _digest(value):
    raw = json.dumps(_native(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _train(api, database, policy, args):
    from trading.expectancy_telemetry import EXPECTANCY_FEATURE_SCHEMAS
    from trading.expectancy_training import (
        _validate_rows, _label_ready_time, _decision_cutoff_time, expanding_walk_forward_fit,
        GROUPED_UTC_DAY_POLICY,
    )
    from trading.expectancy_runtime import save_expectancy_model, save_expectancy_training_report
    features = EXPECTANCY_FEATURE_SCHEMAS["FUTURES"][1]
    source_options = {"include_ui_status": True} if args.publish_ui_status else {}
    sources = _training_source_identity(**source_options)
    _require_policy(api, policy)
    context_options = {"include_consumed_regime": True} if args.include_consumed_regime_context else {}
    labels, metadata = _read(database, lambda connection: api.read_funding_forward_labels(
        connection, policy_id=policy["policy_id"], as_of=args.as_of, **context_options))
    json.dumps(_native(metadata), allow_nan=False)
    _require_policy(api, policy)
    if any(row.label_available_time is None or row.decision_lower_time is None for row in labels):
        raise ValueError("forward labels require explicit availability and pre-entry lower evidence")
    controls = {"min_train": args.min_train, "test_size": args.test_size,
                "purge_days": args.purge_days, "purge_seconds": args.purge_days * 86400,
                "ridge": 1.0, "calibration_fraction": 0.20, "min_calibration": 30,
                "skip_unready_folds": True, "cold_start_policy": "skip_unready_calibration_v1",
                "grouping_policy": GROUPED_UTC_DAY_POLICY,
                "feature_order": list(features), "final_as_of": args.as_of.isoformat()}
    if args.include_consumed_regime_context:
        controls["include_consumed_regime_context"] = True
    if args.publish_ui_status:
        controls["publish_ui_status"] = True
    from tools.simulation_workspace import register_planned_trial
    trial = register_planned_trial(args.registry_root, args.trial_id,
        experiment_name="funding_forward_grouped_training", sources=sources,
        dataset={"qualified_dataset_sha256": metadata.get("dataset_sha256"),
                 "corpus_metadata_sha256": _digest(metadata)},
        controls=controls,
        baseline={"basis": "fixed_estimator_parameter_reference", "controls": controls,
                  "prior_baseline_performance_available": False},
        split_policy={"method": "expanding_utc_day_grouped", "grouping_policy": GROUPED_UTC_DAY_POLICY,
                      "outer_purge_seconds": controls["purge_seconds"],
                      "inner_purge": "whole_cluster_max_ready_before_min_calibration_decision",
                      "independent_holdout_verified": False})
    labels = _validate_rows(labels, features, final_as_of=args.as_of)
    result = expanding_walk_forward_fit(
        labels, feature_order=features, min_train=args.min_train, test_size=args.test_size,
        purge=timedelta(days=args.purge_days), final_as_of=args.as_of,
        ridge=controls["ridge"], calibration_fraction=controls["calibration_fraction"],
        min_calibration=controls["min_calibration"], skip_unready_folds=controls["skip_unready_folds"],
        grouping_policy=GROUPED_UTC_DAY_POLICY,
    )
    _require_policy(api, policy)
    if _training_source_identity(**source_options) != sources:
        raise ValueError("forward training source changed during CLI operation")
    experiment = {
        "schema_version": 1, "sources": sources,
        "source_identity_basis": "file_bytes_observed_before_and_after_training",
        "python_version": sys.version, "python_implementation": sys.implementation.name,
        "dependency_lock_is_environment_verification": False,
        "dataset": {"dataset_sha256": metadata.get("dataset_sha256"),
                    "corpus_metadata_sha256": _digest(metadata)},
        "funding_model_policy_id": policy["policy_id"], "as_of": args.as_of.isoformat(),
        "effective_controls": controls,
    }
    experiment = {**experiment, "experiment_sha256": _digest(experiment)}
    output = _plain(args.output, "new research output")
    output.mkdir(exist_ok=False)
    model = output / "funding_forward.candidate.json"
    report_path = output / "funding_forward.report.json"
    model_identity = save_expectancy_model(model, result.final_model)
    report = {
        "kind": "funding_forward_research_training", "bot": "FUTURES", "mode": "SIM",
        "schema_version": 1, "as_of": args.as_of, "policy": policy, "corpus": metadata,
        "research_only": True, "funding_is_model_estimate": True, "account_pnl": False,
        "promotion_eligible": False, "runtime_model_changed": False,
        "feature_order": features, "labels": len(labels), "closed_labels_by_schema": {"1": len(labels)},
        "training_controls": controls, "experiment_identity": experiment,
        "trial_registration": trial, "grouping_policy": result.grouping_policy,
        "grouping_audit": result.grouping_audit, "final_grouping": result.final_grouping,
        "availability_provenance": {
            "label_ready": "max(actual_close,generation_available_upper)",
            "decision_cutoff": "min(original_candidate,pre_entry_decision_lower)",
            "outer_cutoff": "minimum_all_test_decisions_minus_purge",
            "inner_cutoff": "minimum_all_calibration_decisions",
            "max_label_ready": max(_label_ready_time(row) for row in labels),
            "min_decision_cutoff": min(_decision_cutoff_time(row) for row in labels),
            "fold_time_fields_are_actual_event_times": True,
        },
        "folds": [asdict(fold) for fold in result.folds],
        "predictions": len(result.predictions), "calibration": asdict(result.calibration),
        "cold_start_policy": result.cold_start_policy,
        "skipped_unready_folds": [asdict(fold) for fold in result.skipped_unready_folds],
        "candidate_model": str(model), "candidate_model_sha256": model_identity["sha256"],
        "candidate_model_bytes": model_identity["bytes"],
    }
    save_expectancy_training_report(report_path, _native(report))
    return {"labels": len(labels), "folds": len(result.folds),
            "predictions": len(result.predictions),
            "skipped_unready_folds": len(result.skipped_unready_folds),
            "cold_start_policy": result.cold_start_policy,
            "candidate_model": str(model), "candidate_model_sha256": model_identity["sha256"],
            "candidate_model_bytes": model_identity["bytes"], "report": str(report_path)}


def _publish_ui_attempt(project, args, state, payload=None):
    """Optional observations cannot replace a training error or activate a model."""
    try:
        from trading.training_status import publish_training_ui_status
        counts = ({"qualified_labels": payload["labels"], "fold_count": payload["folds"],
                   "skipped_count": payload["skipped_unready_folds"]} if state == "completed" else {})
        publish_training_ui_status(project, state=state,
            dataset_as_of_utc=args.as_of.isoformat().replace("+00:00", "Z"), **counts)
        return True
    except BaseException as exc:
        # A terminal-record cleanup must preserve an existing training error.
        # A new owner interruption on the successful path still propagates.
        if state == "completed" and not isinstance(exc, Exception):
            raise
        return False


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(argv)
    _project = None
    publish_status = args.command == "train" and args.publish_ui_status
    try:
        _project, database = _known_database(args.root)
        if args.command == "train":
            from tools.simulation_workspace import validate_trial_registry_options
            validate_trial_registry_options(args.registry_root, args.trial_id)
            output = _plain(args.output, "new research output")
            if output.exists() or not output.parent.is_dir():
                raise ValueError("training output must be a new directory with an existing parent")
        snapshots = _snapshot_inputs(args.snapshot) if args.command == "register" else None
        api = _generation_api()
        policy = _policy(api)
        entries = _read(database, (lambda conn: _closed_entries(conn, policy["policy_id"]))
                        if args.command == "seal" else lambda _conn: None)
        _require_policy(api, policy)
        if args.command == "train":
            payload = _train(api, database, policy, args)
        elif args.command == "status":
            def status(connection):
                payload = {"counts": _state_counts(connection)}
                if args.as_of is not None:
                    labels, metadata = api.read_funding_forward_labels(
                        connection, policy_id=policy["policy_id"], as_of=args.as_of)
                    payload.update(labels=len(labels), corpus=metadata, as_of=args.as_of)
                return payload
            payload = _read(database, status)
        else:
            probe = _public_clock_probe()
            _require_policy(api, policy)
            if args.command == "register":
                from trading.funding_forward_corpus import register_funding_source_set
                payload = _write_with_policy(api, policy, database,
                    lambda conn: register_funding_source_set(conn, snapshots))
            elif args.command == "seal":
                payload = {**_seal(api, database, entries, policy),
                           "unsupported_legacy_null_partial_entries":
                               entries["unsupported_legacy_null_partial_entries"]}
            else:
                from trading.funding_forward_corpus import recover_funding_source_sets

                def recover(connection):
                    sources = recover_funding_source_sets(
                        connection, limit=args.limit, after_source_set_id=args.source_after)
                    generations = api.recover_funding_forward_generations(connection, limit=args.limit)
                    return {**sources, "recovered_generations": generations}
                payload = _write_with_policy(api, policy, database, recover)
            payload = {**payload, **probe}
        _require_policy(api, policy)
        payload = {"command": args.command, "policy_id": policy["policy_id"],
                   "promotion_eligible": False, "account_pnl": False,
                   "runtime_model_changed": False, **payload}
        if publish_status:
            payload["ui_status_published"] = _publish_ui_attempt(_project, args, "completed", payload)
        print(json.dumps(_native(payload), sort_keys=True, allow_nan=False))
        return 0
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        if publish_status and _project is not None:
            _publish_ui_attempt(_project, args, "blocked" if isinstance(exc, ValueError) else "failed")
        print(json.dumps({"error_type": type(exc).__name__, "error": str(exc),
                          "promotion_eligible": False}, sort_keys=True))
        return 1
    except BaseException:
        if publish_status and _project is not None:
            _publish_ui_attempt(_project, args, "failed")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
