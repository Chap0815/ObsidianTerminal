"""Runtime readiness and build metadata helpers."""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any, Mapping

from core.paths import PROJECT_ROOT


_STATUS_REPLACE_RETRIES = 20
_STATUS_REPLACE_SLEEP_SEC = 0.10
_STATUS_MAX_FUTURE_SKEW_SEC = 300.0
_STATUS_JSON_MAX_BYTES = 2 * 1024 * 1024
_STATUS_MAX_STRING_CHARS = 4096
_STATUS_MAX_CONTAINER_ITEMS = 256
_STATUS_MAX_KEY_CHARS = 256
_STATUS_LOCK_TIMEOUT_SEC = 1.0
_STATUS_LOCK_CHECK_SEC = 0.01


def _log_status_write_failure(context: str, exc: Exception) -> None:
    try:
        from bot_utils.silent_log import silent_log
        silent_log(context, exc)
    except Exception:
        pass


@contextmanager
def _runtime_status_path_lock(path: Path):
    """Serialize a primary+fallback snapshot with its atomic publisher."""
    lock = None
    try:
        import portalocker

        lock_path = path.with_name(f"{path.name}.publish.lock")
        lock = portalocker.Lock(
            str(lock_path),
            mode="a+b",
            timeout=_STATUS_LOCK_TIMEOUT_SEC,
            check_interval=_STATUS_LOCK_CHECK_SEC,
            flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
        )
        lock.acquire()
    except Exception:
        if lock is not None:
            try:
                lock.release()
            except Exception:
                pass
        yield False
        return
    try:
        yield True
    finally:
        try:
            lock.release()
        except Exception:
            pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _clock_health() -> dict[str, Any]:
    """Return bounded, non-throwing provenance for the process wall clock."""
    try:
        from core.clock import (
            get_offset_age_seconds,
            get_offset_ms,
            have_offset,
        )

        anchored = have_offset()
        offset_ms = get_offset_ms()
        offset_age_seconds = get_offset_age_seconds()
        if (
            not isinstance(anchored, bool)
            or isinstance(offset_ms, bool)
            or not isinstance(offset_ms, (int, float))
            or not math.isfinite(float(offset_ms))
            or (
                anchored
                and (
                    isinstance(offset_age_seconds, bool)
                    or not isinstance(offset_age_seconds, (int, float))
                    or not math.isfinite(float(offset_age_seconds))
                    or float(offset_age_seconds) < 0.0
                )
            )
        ):
            raise ValueError("invalid exchange clock state")
        return {
            "component": "exchange_clock",
            "exchange_anchored": anchored,
            "offset_ms": float(offset_ms) if anchored else 0.0,
            "offset_age_seconds": (
                round(float(offset_age_seconds), 3) if anchored else None
            ),
            "source": "exchange_offset" if anchored else "local_fallback",
        }
    except Exception:
        return {
            "component": "exchange_clock",
            "exchange_anchored": False,
            "offset_ms": None,
            "offset_age_seconds": None,
            "source": "clock_state_unavailable",
        }


def _read_json(path: Path) -> dict:
    try:
        with path.open("rb") as fh:
            raw = fh.read(_STATUS_JSON_MAX_BYTES + 1)
        if len(raw) > _STATUS_JSON_MAX_BYTES:
            return {}
        data = json.loads(raw.decode("utf-8-sig"))
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
        if not files or any(
            not isinstance(row, Mapping)
            or not str(row.get("path") or "").strip()
            or not str(row.get("sha256") or "").strip()
            for row in files
        ):
            return {"build_id": "unknown", "created_at": "", "source": "fallback"}
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


def runtime_status_fallback_path(log_dir: str | os.PathLike[str]) -> Path:
    return runtime_status_path(log_dir).with_name("runtime_status.fallback.json")


def cleanup_runtime_status_temps(logs_root: str | os.PathLike[str] | None = None,
                                 *,
                                 min_age_sec: float = 3600.0) -> int:
    """Remove stale runtime_status temp files left by crashes/file locks."""
    root = Path(logs_root) if logs_root is not None else PROJECT_ROOT / "logs"
    cutoff = time.time() - max(0.0, float(min_age_sec))
    removed = 0
    try:
        candidates = list(root.glob("*/runtime_status*.tmp"))
    except Exception:
        return 0
    for path in candidates:
        try:
            if path.stat().st_mtime > cutoff:
                continue
            path.unlink()
            removed += 1
        except Exception as exc:
            _log_status_write_failure(f"cleanup_runtime_status_temps({path})", exc)
    return removed


def _status_freshness(data: Mapping[str, Any]) -> float:
    rejected_wall_timestamp = False
    for key in ("wall_ts", "epoch_ts"):
        candidate = _positive_finite_timestamp(data.get(key))
        value = _plausible_wall_timestamp(candidate)
        if value is not None:
            return value
        if candidate is not None:
            rejected_wall_timestamp = True
    try:
        raw = str(data.get("updated_at") or "").strip()
        if raw:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc)
            candidate = _positive_finite_timestamp(dt.timestamp())
            value = _plausible_wall_timestamp(candidate)
            if value is not None:
                return value
            if candidate is not None:
                rejected_wall_timestamp = True
    except (TypeError, ValueError, OverflowError, OSError):
        pass
    if rejected_wall_timestamp:
        return 0.0
    return _positive_finite_timestamp(data.get("monotonic_ts")) or 0.0


def _positive_finite_timestamp(raw: Any) -> float | None:
    if isinstance(raw, bool):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(value) or value <= 0.0:
        return None
    return value


def _plausible_wall_timestamp(raw: Any) -> float | None:
    value = _positive_finite_timestamp(raw)
    if value is None:
        return None
    try:
        upper_bound = time.time() + _STATUS_MAX_FUTURE_SKEW_SEC
    except Exception:
        return value
    if not math.isfinite(upper_bound) or value > upper_bound:
        return None
    return value


def _strict_json_value(value: Any, *, depth: int = 0) -> Any:
    """Normalize runtime telemetry to interoperable JSON primitives."""
    if depth > 20:
        return None
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value[:_STATUS_MAX_STRING_CHARS]
    if isinstance(value, int):
        # Avoid Python's integer-string conversion limit turning one corrupt
        # telemetry counter into a missing heartbeat.
        return value if value.bit_length() <= 4096 else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        try:
            bounded = {}
            for key, item in islice(
                value.items(), _STATUS_MAX_CONTAINER_ITEMS
            ):
                if not isinstance(key, str) or len(key) > _STATUS_MAX_KEY_CHARS:
                    continue
                bounded[key] = _strict_json_value(item, depth=depth + 1)
            return bounded
        except Exception:
            return None
    if isinstance(value, (list, tuple)):
        try:
            return [
                _strict_json_value(item, depth=depth + 1)
                for item in value[:_STATUS_MAX_CONTAINER_ITEMS]
            ]
        except Exception:
            return None
    return None


def _write_fallback_status(path: Path, payload: Mapping[str, Any]) -> None:
    fallback = path.with_name("runtime_status.fallback.json")
    tmp_name = ""
    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=fallback.name + ".", suffix=".tmp", dir=str(fallback.parent))
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(dict(payload), fh, indent=2, sort_keys=True, allow_nan=False)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception as exc:
                _log_status_write_failure(
                    f"write_runtime_status fallback fsync({path})", exc)
        try:
            os.replace(tmp_name, fallback)
            tmp_name = ""
        except OSError as replace_exc:
            # Never truncate a last-good fallback in place. A hard link makes
            # the already-flushed temp visible atomically when no fallback
            # exists; otherwise retain the older valid status for the reader.
            try:
                os.link(tmp_name, fallback)
            except FileExistsError:
                pass
            except (AttributeError, OSError) as link_exc:
                _log_status_write_failure(
                    f"write_runtime_status fallback publish({path})",
                    link_exc or replace_exc,
                )
    except Exception as exc:
        _log_status_write_failure(f"write_runtime_status fallback({path})", exc)
    finally:
        if tmp_name:
            try:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)
            except OSError:
                pass


def write_runtime_status(log_dir: str | os.PathLike[str],
                         bot_name: str,
                         status: str,
                         simulation: bool,
                         *,
                         threads: Mapping[str, Any] | None = None,
                         extra: Mapping[str, Any] | None = None,
                         build_info: Mapping[str, Any] | None = None,
                         process_pid: int | None = None,
                         process_run_id: str | None = None) -> None:
    try:
        path = runtime_status_path(log_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Long-running bots pass the snapshot captured during startup so an
        # on-disk sync cannot make already-loaded code advertise a newer build.
        build = dict(build_info) if isinstance(build_info, Mapping) else get_build_info()
        if process_pid is None:
            status_pid = os.getpid()
        elif isinstance(process_pid, bool):
            status_pid = 0
        else:
            try:
                status_pid = max(0, int(process_pid))
            except (TypeError, ValueError, OverflowError):
                status_pid = 0
        status_run_id = (
            os.getenv("BOT_RUN_ID", "")
            if process_run_id is None
            else str(process_run_id or "")[:_STATUS_MAX_STRING_CHARS]
        )
        payload = {
            "bot": bot_name,
            "status": status,
            "simulation": bool(simulation),
            "pid": status_pid,
            "run_id": status_run_id,
            "updated_at": _utc_now(),
            "wall_ts": time.time(),
            "monotonic_ts": time.monotonic(),
            "build_id": build.get("build_id", "unknown"),
            "build_source": build.get("source", "fallback"),
            "build_created_at": build.get("created_at", ""),
            "threads": _strict_json_value(threads or {}),
            "clock_health": _clock_health(),
        }
        if extra:
            normalized_extra = _strict_json_value(extra)
            for key, value in (
                normalized_extra.items()
                if isinstance(normalized_extra, Mapping)
                else ()
            ):
                if isinstance(key, str) and key not in payload:
                    payload[key] = value

        with _runtime_status_path_lock(path) as acquired:
            if not acquired:
                _log_status_write_failure(
                    f"write_runtime_status lock({path})",
                    TimeoutError("runtime status publish lock unavailable"),
                )
                return
            fd, tmp_name = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, indent=2, sort_keys=True, allow_nan=False)
                    fh.flush()
                    try:
                        os.fsync(fh.fileno())
                    except Exception as exc:
                        _log_status_write_failure(
                            f"write_runtime_status fsync({path})", exc
                        )
                last_err = None
                for attempt in range(_STATUS_REPLACE_RETRIES):
                    try:
                        os.replace(tmp_name, path)
                        last_err = None
                        try:
                            path.with_name("runtime_status.fallback.json").unlink()
                        except FileNotFoundError:
                            pass
                        except Exception as exc:
                            _log_status_write_failure(
                                f"write_runtime_status cleanup_fallback({path})",
                                exc,
                            )
                        break
                    except OSError as exc:
                        last_err = exc
                        time.sleep(
                            _STATUS_REPLACE_SLEEP_SEC
                            * (1.0 + (attempt % 3) * 0.25)
                        )
                if last_err is not None:
                    _write_fallback_status(path, payload)
                    _log_status_write_failure(
                        f"write_runtime_status({path})", last_err
                    )
            finally:
                try:
                    if os.path.exists(tmp_name):
                        os.remove(tmp_name)
                except OSError:
                    pass
    except Exception as exc:
        _log_status_write_failure(f"write_runtime_status({log_dir})", exc)


def read_runtime_status(log_dir: str | os.PathLike[str]) -> dict:
    status, _path = read_runtime_status_with_path(log_dir)
    return status


def read_runtime_status_with_path(
    log_dir: str | os.PathLike[str],
) -> tuple[dict, Path | None]:
    primary_path = runtime_status_path(log_dir)
    fallback_path = runtime_status_fallback_path(log_dir)
    with _runtime_status_path_lock(primary_path) as acquired:
        if not acquired:
            return {}, None
        primary = _read_json(primary_path)
        fallback = _read_json(fallback_path)
        if not fallback:
            return primary, primary_path if primary else None
        if not primary:
            return fallback, fallback_path
        if _status_freshness(fallback) > _status_freshness(primary):
            return fallback, fallback_path
        return primary, primary_path
