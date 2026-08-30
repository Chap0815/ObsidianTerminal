"""Run-bound launcher-to-bot shutdown requests.

Console control events are not a reliable IPC channel for hidden Windows
processes.  This module provides a tiny atomic control record which the bot's
main thread consumes.  Requests are bound to both the launcher-assigned run ID
and the child PID, so a stale file can never stop a later bot generation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import uuid


SHUTDOWN_CONTROL_ENV = "OBSIDIAN_SHUTDOWN_CONTROL_PATH"
CLOSE_POSITIONS = "close_positions"
PRESERVE_POSITIONS = "preserve_positions"
_VALID_MODES = {CLOSE_POSITIONS, PRESERVE_POSITIONS}
_CONTROL_DIR = Path("logs") / ".shutdown-control"
_MAX_REQUEST_BYTES = 4096
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_BOT_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,31}$")


def _project_root(project_root: str | os.PathLike[str] | None) -> Path:
    root = (
        Path(project_root)
        if project_root is not None
        else Path(__file__).resolve().parent.parent
    )
    return root.absolute()


def _validate_run_id(run_id: str) -> str:
    value = str(run_id or "").strip().lower()
    if not _RUN_ID_RE.fullmatch(value):
        raise ValueError("shutdown control run ID is invalid")
    return value


def _validate_target(
    path: str | os.PathLike[str],
    *,
    project_root: str | os.PathLike[str] | None,
) -> Path:
    root = _project_root(project_root)
    control_root = (root / _CONTROL_DIR).absolute()
    target = Path(path).absolute()
    if target.parent != control_root or target.suffix != ".json":
        raise ValueError("shutdown control path is outside its runtime scope")
    if root.is_symlink() or control_root.is_symlink() or target.is_symlink():
        raise ValueError("shutdown control path must not use links")
    try:
        if root.resolve() / _CONTROL_DIR != control_root.resolve():
            raise ValueError("shutdown control path escaped through a link")
    except OSError as exc:
        raise ValueError("shutdown control path cannot be resolved") from exc
    return target


def prepare_shutdown_control(
    project_root: str | os.PathLike[str],
    bot_name: str,
    run_id: str,
) -> str:
    """Create the private control directory and return one unique run path."""
    root = _project_root(project_root)
    name = str(bot_name or "BOT").strip().upper()
    if not _BOT_NAME_RE.fullmatch(name):
        raise ValueError("shutdown control bot name is invalid")
    run = _validate_run_id(run_id)
    control_root = root / _CONTROL_DIR
    control_root.mkdir(parents=True, exist_ok=True)
    target = _validate_target(
        control_root / f"{name}-{run}.json",
        project_root=root,
    )
    target.unlink(missing_ok=True)
    return str(target)


def publish_shutdown_request(
    path: str | os.PathLike[str],
    *,
    run_id: str,
    pid: int,
    mode: str,
    project_root: str | os.PathLike[str] | None = None,
) -> None:
    """Atomically publish one exact request for the current child generation."""
    target = _validate_target(path, project_root=project_root)
    run = _validate_run_id(run_id)
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError("shutdown control PID is invalid")
    if mode not in _VALID_MODES:
        raise ValueError("shutdown control mode is invalid")
    payload = {
        "mode": mode,
        "pid": pid,
        "run_id": run,
        "schema_version": 1,
    }
    encoded = json.dumps(
        payload,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > _MAX_REQUEST_BYTES:
        raise ValueError("shutdown control request exceeds size limit")
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        with temporary.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def consume_shutdown_request(
    *,
    path: str | os.PathLike[str] | None = None,
    run_id: str | None = None,
    pid: int | None = None,
    project_root: str | os.PathLike[str] | None = None,
) -> str | None:
    """Consume and return a valid request mode, otherwise return ``None``."""
    raw_path = path if path is not None else os.getenv(SHUTDOWN_CONTROL_ENV, "")
    if not raw_path:
        return None
    target = _validate_target(raw_path, project_root=project_root)
    try:
        with target.open("rb") as stream:
            raw = stream.read(_MAX_REQUEST_BYTES + 1)
    except FileNotFoundError:
        return None
    if len(raw) > _MAX_REQUEST_BYTES:
        raise ValueError("shutdown control request exceeds size limit")

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate shutdown control key: {key}")
            result[key] = value
        return result

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-standard JSON constant: {value}")
            ),
        )
        if not isinstance(payload, dict) or set(payload) != {
            "mode", "pid", "run_id", "schema_version"
        }:
            raise ValueError("shutdown control request shape is invalid")
        expected_run = _validate_run_id(
            run_id if run_id is not None else os.getenv("BOT_RUN_ID", "")
        )
        expected_pid = os.getpid() if pid is None else pid
        if (
            payload.get("schema_version") != 1
            or payload.get("run_id") != expected_run
            or payload.get("pid") != expected_pid
            or payload.get("mode") not in _VALID_MODES
        ):
            raise ValueError("shutdown control request identity is invalid")
    except Exception:
        # The exact run-scoped file has been consumed as invalid evidence. Do
        # not parse and log the same corrupt request every coordinator tick.
        target.unlink(missing_ok=True)
        raise
    target.unlink(missing_ok=True)
    return str(payload["mode"])


def cleanup_shutdown_control(
    path: str | os.PathLike[str] | None,
    *,
    project_root: str | os.PathLike[str] | None = None,
) -> None:
    if not path:
        return
    target = _validate_target(path, project_root=project_root)
    target.unlink(missing_ok=True)
