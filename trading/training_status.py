"""Bounded offline-training observations for UI display; never model authority."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat

MAX_STATUS_BYTES = 8192
MAX_STATUS_COUNT = 1_000_000
_PRODUCER = "funding_forward_corpus_v1"
_KEYS = frozenset({"schema_version", "producer", "state", "reason", "recorded_at_utc",
    "dataset_as_of_utc", "qualified_labels", "fold_count", "skipped_count", "report_sha256",
    "research_only", "promotion_eligible", "runtime_model_changed"})
_REASONS = {"completed": "training_completed", "blocked": "training_blocked", "failed": "training_failed"}


def _utc(value):
    if type(value) is not str or len(value) > 40 or not value.isascii():
        raise ValueError("invalid status time")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("status time must be explicit UTC")
    # Accept only the writer's canonical ISO representation, not permissive
    # fromisoformat alternatives such as a date/space or missing UTC offset.
    if parsed.isoformat().replace("+00:00", "Z") != value:
        raise ValueError("noncanonical status time")
    return value


def _validate(value):
    if type(value) is not dict or set(value) != _KEYS:
        raise ValueError("invalid status fields")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("unsupported status schema")
    if value["producer"] != _PRODUCER or type(value["state"]) is not str or value["state"] not in _REASONS:
        raise ValueError("invalid status producer or state")
    if value["reason"] != _REASONS[value["state"]]:
        raise ValueError("invalid status reason")
    for key, expected in (("research_only", True), ("promotion_eligible", False), ("runtime_model_changed", False)):
        if type(value[key]) is not bool or value[key] is not expected:
            raise ValueError("invalid status scope")
    _utc(value["recorded_at_utc"])
    if value["dataset_as_of_utc"] is not None:
        _utc(value["dataset_as_of_utc"])
    for key in ("qualified_labels", "fold_count", "skipped_count"):
        count = value[key]
        if count is not None and (type(count) is not int or not 0 <= count <= MAX_STATUS_COUNT):
            raise ValueError("invalid status count")
    digest = value["report_sha256"]
    if digest is not None and (type(digest) is not str or not re.fullmatch(r"[0-9a-f]{64}", digest)):
        raise ValueError("invalid report identity")
    if value["state"] == "completed":
        if (value["dataset_as_of_utc"] is None or value["qualified_labels"] in (None, 0)
                or value["fold_count"] is None or value["skipped_count"] is None):
            raise ValueError("completed status requires exported result metadata")
    elif any(value[key] is not None for key in ("qualified_labels", "fold_count", "skipped_count", "report_sha256")):
        raise ValueError("incomplete status cannot claim completed results")
    return value


def _unlinked(info):
    return not (stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _identity(info):
    # Windows lstat and handle-fstat can expose different ctime semantics.
    # Compare ctime only within the same API, retaining inode/size/mtime across
    # both APIs rather than rejecting every atomically published Windows file.
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _generation(info):
    return (*_identity(info), info.st_ctime_ns)


def _status_path(project_root):
    raw = Path(project_root)
    if ".." in raw.parts:
        raise ValueError("status path traversal")
    root = Path(os.path.abspath(raw))
    path = root / "data" / "training_ui_status.json"
    parents = list(path.parents)
    if len(parents) > 256:
        raise ValueError("status path depth")
    chain = []
    for parent in reversed(parents):
        info = parent.lstat()
        if not _unlinked(info) or not stat.S_ISDIR(info.st_mode):
            raise ValueError("status parent must be an unlinked directory")
        chain.append((parent, info.st_dev, info.st_ino))
    return path, chain


def _check_chain(chain):
    for parent, device, inode in chain:
        info = parent.lstat()
        if not _unlinked(info) or not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != (device, inode):
            raise ValueError("status parent changed")


def _file_info(path):
    info = path.lstat()
    if not _unlinked(info) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("status must be an unlinked regular file")
    return info


def _read_status(project_root):
    path, chain = _status_path(project_root)
    before = _file_info(path)
    if not 0 < before.st_size <= MAX_STATUS_BYTES:
        raise ValueError("status byte budget")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if _identity(opened) != _identity(before):
            raise ValueError("status changed before read")
        raw = stream.read(MAX_STATUS_BYTES + 1)
        if _generation(os.fstat(stream.fileno())) != _generation(opened):
            raise ValueError("status changed during read")
    _check_chain(chain)
    if len(raw) != before.st_size or _generation(_file_info(path)) != _generation(before):
        raise ValueError("status changed after read")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate status field")
            result[key] = value
        return result

    def invalid(_value):
        raise ValueError("nonfinite status field")
    return _validate(json.loads(raw, object_pairs_hook=unique, parse_constant=invalid))


def _unavailable(reason):
    return {"schema_version": 1, "unavailable": True, "reason": reason,
        "data_status": "Not assessed", "data_detail": "No verified training report is available.",
        "last_run_utc": None, "result_status": "Not run",
        "result_detail": "Run offline training after the data checks pass.",
        "approval_status": "No approval recorded",
        "approval_detail": "Training completion does not confirm live activation.",
        "dataset_as_of_utc": None, "qualified_labels": None, "research_only": True}


def read_training_ui_status(project_root) -> dict:
    """Read a small observational summary without SQLite, config, SDK or I/O writes."""
    try:
        value = _read_status(project_root)
    except FileNotFoundError:
        return _unavailable("status_missing")
    except OSError:
        return _unavailable("status_unreadable")
    except (ValueError, TypeError, OverflowError, RecursionError):
        return _unavailable("status_invalid")
    result = _unavailable(value["reason"])
    result.update(unavailable=False, last_run_utc=value["recorded_at_utc"],
                  dataset_as_of_utc=value["dataset_as_of_utc"], qualified_labels=value["qualified_labels"])
    if value["state"] == "completed":
        result.update(data_status="Assessed for training",
            data_detail=f"Reported {value['qualified_labels']} qualified offline labels at the recorded cutoff.",
            result_status="Completed", result_detail="Offline model and report exported; research only.")
    else:
        result.update(data_detail="No completed offline training result is recorded for this attempt.",
            result_status="Blocked" if value["state"] == "blocked" else "Failed",
            result_detail="Offline training did not complete. Review the offline command result.")
    return result


def publish_training_ui_status(project_root, *, state, dataset_as_of_utc=None,
                               qualified_labels=None, fold_count=None, skipped_count=None, report_sha256=None):
    """Record one explicit terminal offline attempt, without activating any model."""
    value = _validate({"schema_version": 1, "producer": _PRODUCER, "state": state,
        "reason": _REASONS.get(state),
        "recorded_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset_as_of_utc": dataset_as_of_utc, "qualified_labels": qualified_labels,
        "fold_count": fold_count, "skipped_count": skipped_count, "report_sha256": report_sha256,
        "research_only": True, "promotion_eligible": False, "runtime_model_changed": False})
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(data) > MAX_STATUS_BYTES:
        raise ValueError("status byte budget")
    path, chain = _status_path(project_root)
    if path.exists() or path.is_symlink():
        _file_info(path)
    _check_chain(chain)
    from bot_utils.atomic_publish import atomic_write_bytes
    atomic_write_bytes(path, data)
    _check_chain(chain)
