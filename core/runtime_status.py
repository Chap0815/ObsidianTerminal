"""Runtime readiness and build metadata helpers."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from core.paths import PROJECT_ROOT


_STATUS_REPLACE_RETRIES = 8
_STATUS_REPLACE_SLEEP_SEC = 0.05


def _log_status_write_failure(context: str, exc: Exception) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(context, exc)
    except Exception:
        pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _read_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_build_info() -> dict:
    """Return deploy metadata without raising.

    Prefer DEPLOY_MANIFEST.json, then SYNC_MANIFEST.json. The latter is created
    by manual remote syncs and only has file hashes, so we derive a build id.
    """
    deploy = _read_json(PROJECT_ROOT / "DEPLOY_MANIFEST.json")
    if deploy:
        return {
            "build_id": str(deploy.get("build_id") or "unknown"),
            "created_at": str(deploy.get("created_at")
                              or deploy.get("created_at_utc") or ""),
            "source": "DEPLOY_MANIFEST.json",
        }

    sync = _read_json(PROJECT_ROOT / "SYNC_MANIFEST.json")
    files = sync.get("files") if isinstance(sync.get("files"), list) else sync
    if isinstance(files, list):
        h = hashlib.sha256()
        for row in sorted(files, key=lambda r: str(r.get("path", ""))):
            h.update(str(row.get("path", "")).encode("utf-8"))
            h.update(str(row.get("sha256", "")).encode("ascii", errors="ignore"))
        return {
            "build_id": h.hexdigest()[:16],
            "created_at": str(sync.get("created_at") or ""),
            "source": "SYNC_MANIFEST.json",
        }

    return {"build_id": "unknown", "created_at": "", "source": "fallback"}


def runtime_status_path(log_dir: str | os.PathLike[str]) -> Path:
    return PROJECT_ROOT / str(log_dir) / "runtime_status.json"


def write_runtime_status(log_dir: str | os.PathLike[str],
                         bot_name: str,
                         status: str,
                         simulation: bool,
                         *,
                         threads: Mapping[str, Any] | None = None,
                         extra: Mapping[str, Any] | None = None) -> None:
    try:
        path = runtime_status_path(log_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        build = get_build_info()
        payload = {
            "bot": bot_name,
            "status": status,
            "simulation": bool(simulation),
            "pid": os.getpid(),
            "run_id": os.getenv("BOT_RUN_ID", ""),
            "updated_at": _utc_now(),
            "monotonic_ts": time.monotonic(),
            "build_id": build.get("build_id", "unknown"),
            "build_source": build.get("source", "fallback"),
            "build_created_at": build.get("created_at", ""),
            "threads": dict(threads or {}),
        }
        if extra:
            payload.update(dict(extra))

        fd, tmp_name = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except Exception:
                    pass
            last_err = None
            for _ in range(_STATUS_REPLACE_RETRIES):
                try:
                    os.replace(tmp_name, path)
                    last_err = None
                    break
                except PermissionError as exc:
                    last_err = exc
                    time.sleep(_STATUS_REPLACE_SLEEP_SEC)
            if last_err is not None:
                _log_status_write_failure(f"write_runtime_status({path})",
                                          last_err)
        finally:
            try:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)
            except OSError:
                pass
    except Exception as exc:
        _log_status_write_failure(f"write_runtime_status({log_dir})", exc)


def read_runtime_status(log_dir: str | os.PathLike[str]) -> dict:
    return _read_json(runtime_status_path(log_dir))
