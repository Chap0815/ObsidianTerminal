"""Runtime readiness and build metadata helpers."""
from __future__ import annotations

import hashlib
import json
import math
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any, Mapping

from bot_utils.atomic_publish import atomic_create_bytes, atomic_write_bytes
from bot_utils.atomic_publish import _sync_directory as _sync_status_directory
from core.paths import PROJECT_ROOT


_STATUS_REPLACE_RETRIES = 20
_STATUS_REPLACE_SLEEP_SEC = 0.10
_STATUS_MAX_FUTURE_SKEW_SEC = 300.0
_STATUS_JSON_MAX_BYTES = 2 * 1024 * 1024
_STATUS_MAX_STRING_CHARS = 4096
_STATUS_MAX_CONTAINER_ITEMS = 256
_STATUS_MAX_KEY_CHARS = 256
_STATUS_NORMALIZATION_MAX_NODES = 4096
_STATUS_LOCK_TIMEOUT_SEC = 1.0
_STATUS_LOCK_CHECK_SEC = 0.01
_STATUS_TEMP_CLEANUP_MAX_FILES = 512
_STATUS_TEMP_CLEANUP_MAX_SCAN_SEC = 0.5
_STATUS_TEMP_CLEANUP_MAX_ERRORS = 16
_STATUS_PUBLISH_SEQUENCE_MAX = (1 << 63) - 1
_STATUS_PUBLISH_COUNTER_MAX_BYTES = 32
_CLOCK_OFFSET_MAX_AGE_SECONDS = 6.0 * 60.0 * 60.0
_BUILD_ID_CHARS = 16
_BUILD_CREATED_AT_MAX_CHARS = 128
_BUILD_SOURCE_MAX_CHARS = 64
_MANIFEST_SHA256_CHARS = 64
_SYNC_MANIFEST_MAX_ROWS = 4096
_SYNC_BUILD_HASH_DOMAIN = b"TradingBot SYNC_MANIFEST build v2\0"


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
        handle = lock.acquire()
    except Exception as exc:
        if lock is not None:
            try:
                lock.release()
            except Exception:
                pass
        _log_status_write_failure(
            f"runtime status lock acquire({path})", exc
        )
        yield False
        return
    try:
        yield handle
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
        clock_ok = bool(
            anchored
            and float(offset_age_seconds) <= _CLOCK_OFFSET_MAX_AGE_SECONDS
        )
        return {
            "component": "exchange_clock",
            "exchange_anchored": anchored,
            "offset_ms": float(offset_ms) if anchored else 0.0,
            "offset_age_seconds": (
                round(float(offset_age_seconds), 3) if anchored else None
            ),
            "maximum_offset_age_seconds": _CLOCK_OFFSET_MAX_AGE_SECONDS,
            "ok": clock_ok,
            "reason": (
                ""
                if clock_ok
                else "exchange_offset_stale"
                if anchored
                else "exchange_offset_unavailable"
            ),
            "source": "exchange_offset" if anchored else "local_fallback",
        }
    except Exception:
        return {
            "component": "exchange_clock",
            "exchange_anchored": False,
            "offset_ms": None,
            "offset_age_seconds": None,
            "maximum_offset_age_seconds": _CLOCK_OFFSET_MAX_AGE_SECONDS,
            "ok": False,
            "reason": "clock_state_unavailable",
            "source": "clock_state_unavailable",
        }


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate runtime-status JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str):
    raise ValueError(f"non-standard runtime-status JSON constant: {value}")


def _read_json(path: Path) -> dict:
    try:
        with path.open("rb") as fh:
            raw = fh.read(_STATUS_JSON_MAX_BYTES + 1)
        if len(raw) > _STATUS_JSON_MAX_BYTES:
            return {}
        data = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _validated_build_id(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) != _BUILD_ID_CHARS:
        return None
    if any(char not in "0123456789abcdef" for char in value):
        return None
    return value


def _bounded_build_created_at(value: Any) -> str:
    if not isinstance(value, str) or len(value) > _BUILD_CREATED_AT_MAX_CHARS:
        return ""
    return value


def _bounded_build_source(value: Any) -> str:
    if not isinstance(value, str) or len(value) > _BUILD_SOURCE_MAX_CHARS:
        return "fallback"
    return value


def _validated_sync_manifest_rows(value: Any) -> list[Mapping[str, Any]] | None:
    if (
        not isinstance(value, list)
        or not value
        or len(value) > _SYNC_MANIFEST_MAX_ROWS
    ):
        return None
    rows: list[Mapping[str, Any]] = []
    seen_paths: set[str] = set()
    for row in value:
        if not isinstance(row, Mapping):
            return None
        path = row.get("path")
        sha256 = row.get("sha256")
        if (
            not isinstance(path, str)
            or path != path.strip()
            or not path
            or len(path) > _STATUS_MAX_STRING_CHARS
            or not isinstance(sha256, str)
            or len(sha256) != _MANIFEST_SHA256_CHARS
            or any(char not in "0123456789abcdefABCDEF" for char in sha256)
        ):
            return None
        canonical_path = path.replace("\\", "/")
        parts = canonical_path.split("/")
        if (
            canonical_path.startswith("/")
            or any(not part or part in {".", ".."} for part in parts)
            or ":" in parts[0]
        ):
            return None
        identity = canonical_path.casefold()
        if identity in seen_paths:
            return None
        seen_paths.add(identity)
        rows.append({"path": canonical_path, "sha256": sha256.lower()})
    return rows


def _sync_manifest_build_id(files: list[Mapping[str, Any]]) -> str:
    """Hash canonical rows with explicit framing between variable fields."""
    h = hashlib.sha256()
    h.update(_SYNC_BUILD_HASH_DOMAIN)
    h.update(len(files).to_bytes(4, "big"))
    for row in sorted(files, key=lambda item: item["path"]):
        path_bytes = row["path"].encode("utf-8")
        h.update(len(path_bytes).to_bytes(4, "big"))
        h.update(path_bytes)
        h.update(bytes.fromhex(row["sha256"]))
    return h.hexdigest()[:16]


def get_build_info() -> dict:
    """Return deploy metadata without raising.

    Prefer DEPLOY_MANIFEST.json, then SYNC_MANIFEST.json. The latter is created
    by manual remote syncs and only has file hashes, so we derive a build id.
    """
    deploy = _read_json(PROJECT_ROOT / "DEPLOY_MANIFEST.json")
    deploy_build_id = _validated_build_id(deploy.get("build_id"))
    if deploy_build_id is not None:
        return {
            "build_id": deploy_build_id,
            "created_at": _bounded_build_created_at(
                deploy.get("created_at") or deploy.get("created_at_utc") or ""
            ),
            "source": "DEPLOY_MANIFEST.json",
        }

    sync = _read_json(PROJECT_ROOT / "SYNC_MANIFEST.json")
    files = _validated_sync_manifest_rows(sync.get("files"))
    if files is not None:
        return {
            "build_id": _sync_manifest_build_id(files),
            "created_at": _bounded_build_created_at(sync.get("created_at") or ""),
            "source": "SYNC_MANIFEST.json",
        }

    return {"build_id": "unknown", "created_at": "", "source": "fallback"}


def _contained_runtime_status_directory(
    log_dir: str | os.PathLike[str],
) -> Path:
    root = Path(PROJECT_ROOT).resolve(strict=False)
    candidate = Path(log_dir)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("runtime status directory escapes project root") from exc
    return resolved


def runtime_status_path(log_dir: str | os.PathLike[str]) -> Path:
    return _contained_runtime_status_directory(log_dir) / "runtime_status.json"


def runtime_status_fallback_path(log_dir: str | os.PathLike[str]) -> Path:
    return runtime_status_path(log_dir).with_name("runtime_status.fallback.json")


def cleanup_runtime_status_temps(logs_root: str | os.PathLike[str] | None = None,
                                 *,
                                 min_age_sec: float = 3600.0) -> int:
    """Remove stale runtime_status temp files left by crashes/file locks."""
    root = (
        Path(logs_root)
        if logs_root is not None
        else _contained_runtime_status_directory("logs")
    )
    removed = 0
    if isinstance(min_age_sec, bool):
        return removed
    try:
        minimum_age = float(min_age_sec)
        now = float(time.time())
    except (TypeError, ValueError, OverflowError):
        return removed
    if not math.isfinite(minimum_age) or not math.isfinite(now):
        return removed
    cutoff = now - max(0.0, minimum_age)
    logged_errors = 0
    try:
        try:
            root_boundary = root.resolve(strict=False)
        except AttributeError:
            # Lightweight bounded-iteration test doubles have no filesystem
            # identity. Real cleanup roots are always concrete ``Path`` values.
            root_boundary = None
        deadline = time.monotonic() + _STATUS_TEMP_CLEANUP_MAX_SCAN_SEC
        candidates = root.glob("*/runtime_status*.tmp")
        for path in candidates:
            if time.monotonic() >= deadline:
                break
            try:
                target = path
                if root_boundary is not None:
                    target = path.resolve(strict=True)
                    try:
                        target.relative_to(root_boundary)
                    except ValueError:
                        continue
                if target.stat().st_mtime > cutoff:
                    continue
                target.unlink()
                removed += 1
                if removed >= _STATUS_TEMP_CLEANUP_MAX_FILES:
                    break
            except Exception as exc:
                if logged_errors < _STATUS_TEMP_CLEANUP_MAX_ERRORS:
                    _log_status_write_failure(
                        f"cleanup_runtime_status_temps({path})", exc
                    )
                    logged_errors += 1
    except Exception as exc:
        if logged_errors < _STATUS_TEMP_CLEANUP_MAX_ERRORS:
            _log_status_write_failure("cleanup_runtime_status_temps scan", exc)
        return removed
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
        elif key in data:
            rejected_wall_timestamp = True
    raw_updated_at = data.get("updated_at")
    if "updated_at" in data:
        if not isinstance(raw_updated_at, str) or not raw_updated_at.strip():
            rejected_wall_timestamp = True
        else:
            raw = raw_updated_at.strip()
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc)
                candidate = _positive_finite_timestamp(dt.timestamp())
                value = _plausible_wall_timestamp(candidate)
                if value is not None:
                    return value
                rejected_wall_timestamp = True
            except (TypeError, ValueError, OverflowError, OSError):
                rejected_wall_timestamp = True
    if rejected_wall_timestamp:
        return 0.0
    return _positive_finite_timestamp(data.get("monotonic_ts")) or 0.0


def _status_publish_sequence(data: Mapping[str, Any]) -> int:
    raw = data.get("publish_seq")
    if (
        isinstance(raw, bool)
        or not isinstance(raw, int)
        or raw <= 0
        or raw > _STATUS_PUBLISH_SEQUENCE_MAX
    ):
        return 0
    return raw


def _next_status_publish_sequence(path: Path, lock_handle: Any) -> int:
    """Reserve one durable generation while the path publish lock is held."""
    fallback = path.with_name("runtime_status.fallback.json")
    highest = max(
        _status_publish_sequence(_read_json(path)),
        _status_publish_sequence(_read_json(fallback)),
    )
    file_methods = ("seek", "read", "truncate", "write", "flush", "fileno")
    if all(callable(getattr(lock_handle, name, None)) for name in file_methods):
        lock_handle.seek(0)
        raw = lock_handle.read(_STATUS_PUBLISH_COUNTER_MAX_BYTES + 1)
        if isinstance(raw, str):
            raw = raw.encode("ascii", errors="ignore")
        if len(raw) <= _STATUS_PUBLISH_COUNTER_MAX_BYTES:
            try:
                text = raw.decode("ascii")
                if text and text.isdigit():
                    counter = int(text)
                    if 0 <= counter <= _STATUS_PUBLISH_SEQUENCE_MAX:
                        highest = max(highest, counter)
            except (AttributeError, UnicodeError, ValueError, OverflowError):
                pass
    if highest >= _STATUS_PUBLISH_SEQUENCE_MAX:
        raise OverflowError("runtime status publish sequence exhausted")
    sequence = highest + 1
    if all(callable(getattr(lock_handle, name, None)) for name in file_methods):
        encoded = str(sequence).encode("ascii")
        lock_handle.seek(0)
        lock_handle.truncate(0)
        lock_handle.seek(0)
        lock_handle.write(encoded)
        lock_handle.flush()
        os.fsync(lock_handle.fileno())
    return sequence


def _positive_finite_timestamp(raw: Any) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
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
        return None
    if not math.isfinite(upper_bound) or value > upper_bound:
        return None
    return value


def _strict_json_value(
    value: Any,
    *,
    depth: int = 0,
    _budget: list[int] | None = None,
) -> Any:
    """Normalize runtime telemetry to interoperable JSON primitives."""
    if depth > 20:
        return None
    if _budget is None:
        _budget = [_STATUS_NORMALIZATION_MAX_NODES]
    if _budget[0] <= 0:
        return None
    _budget[0] -= 1
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
                bounded[key] = _strict_json_value(
                    item,
                    depth=depth + 1,
                    _budget=_budget,
                )
            return bounded
        except Exception:
            return None
    if isinstance(value, (list, tuple)):
        try:
            return [
                _strict_json_value(
                    item,
                    depth=depth + 1,
                    _budget=_budget,
                )
                for item in value[:_STATUS_MAX_CONTAINER_ITEMS]
            ]
        except Exception:
            return None
    return None


def _write_fallback_status(path: Path, payload: Mapping[str, Any]) -> bool:
    fallback = path.with_name("runtime_status.fallback.json")
    try:
        encoded = json.dumps(
            dict(payload),
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        try:
            atomic_write_bytes(fallback, encoded)
            return True
        except OSError:
            # Preserve the no-overwrite emergency path whenever atomic
            # replacement is unavailable (for example a Windows sharing
            # violation or a filesystem-level replace failure).
            return atomic_create_bytes(fallback, encoded)
    except Exception as exc:
        _log_status_write_failure(f"write_runtime_status fallback({path})", exc)
        return False


def write_runtime_status(log_dir: str | os.PathLike[str],
                         bot_name: str,
                         status: str,
                         simulation: bool,
                         *,
                         threads: Mapping[str, Any] | None = None,
                         extra: Mapping[str, Any] | None = None,
                         build_info: Mapping[str, Any] | None = None,
                         process_pid: int | None = None,
                         process_run_id: str | None = None) -> bool:
    try:
        if not isinstance(simulation, bool):
            raise ValueError("runtime simulation must be boolean")
        if not isinstance(bot_name, str) or not bot_name.strip():
            raise ValueError("runtime bot name must be non-empty text")
        if not isinstance(status, str) or not status.strip():
            raise ValueError("runtime status must be non-empty text")
        bot_name = bot_name.strip()
        status = status.strip()
        if threads is not None and not isinstance(threads, Mapping):
            raise ValueError("runtime threads must be a mapping")
        if extra is not None and not isinstance(extra, Mapping):
            raise ValueError("runtime extra must be a mapping")
        if build_info is not None and not isinstance(build_info, Mapping):
            raise ValueError("runtime build info must be a mapping")
        if process_run_id is not None and not isinstance(process_run_id, str):
            raise ValueError("runtime run id must be text")
        path = runtime_status_path(log_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Long-running bots pass the snapshot captured during startup so an
        # on-disk sync cannot make already-loaded code advertise a newer build.
        build = dict(build_info) if build_info is not None else get_build_info()
        status_build_id = _validated_build_id(build.get("build_id")) or "unknown"
        status_build_created_at = _bounded_build_created_at(
            build.get("created_at") or ""
        )
        status_build_source = _bounded_build_source(
            build.get("source") or "fallback"
        )
        if process_pid is None:
            status_pid = os.getpid()
        elif (
            isinstance(process_pid, bool)
            or not isinstance(process_pid, int)
            or process_pid < 0
            or process_pid > 0xFFFFFFFF
        ):
            status_pid = 0
        else:
            status_pid = process_pid
        if process_run_id is None:
            raw_run_id = os.getenv("BOT_RUN_ID", "")
        else:
            raw_run_id = process_run_id
        status_run_id = raw_run_id[:_STATUS_MAX_STRING_CHARS]
        status_bot_name = bot_name[:_STATUS_MAX_STRING_CHARS]
        status_name = status[:_STATUS_MAX_STRING_CHARS]
        payload = {
            "bot": status_bot_name,
            "status": status_name,
            "simulation": simulation,
            "pid": status_pid,
            "run_id": status_run_id,
            # Reserved here so ``extra`` cannot override freshness provenance.
            # The real values are sampled only after the publish lock is held.
            "updated_at": "",
            "wall_ts": 0.0,
            "monotonic_ts": 0.0,
            "publish_seq": 0,
            "build_id": status_build_id,
            "build_source": status_build_source,
            "build_created_at": status_build_created_at,
            "threads": _strict_json_value(
                threads if threads is not None else {}
            ),
            "clock_health": {},
        }
        if extra is not None:
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
                return False
            payload["publish_seq"] = _next_status_publish_sequence(
                path,
                acquired,
            )
            # Freshness must follow the serialized publication order. Sampling
            # before this lock lets a delayed older writer overwrite a newer
            # heartbeat with a regressed timestamp.
            payload["updated_at"] = _utc_now()
            payload["wall_ts"] = time.time()
            payload["monotonic_ts"] = time.monotonic()
            payload["clock_health"] = _clock_health()
            encoded = json.dumps(
                payload,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
            if len(encoded) > _STATUS_JSON_MAX_BYTES:
                raise ValueError(
                    "runtime status exceeds reader size limit "
                    f"({_STATUS_JSON_MAX_BYTES} bytes)"
                )
            published = False
            last_err = None
            for attempt in range(_STATUS_REPLACE_RETRIES):
                try:
                    # Each retry owns a fresh exclusive UUID generation and
                    # succeeds only after both file and directory durability.
                    atomic_write_bytes(path, encoded)
                    last_err = None
                    try:
                        path.with_name("runtime_status.fallback.json").unlink()
                        _sync_status_directory(path.parent)
                    except FileNotFoundError:
                        pass
                    except Exception as exc:
                        _log_status_write_failure(
                            f"write_runtime_status cleanup_fallback({path})",
                            exc,
                        )
                    published = True
                    break
                except OSError as exc:
                    last_err = exc
                    if attempt + 1 < _STATUS_REPLACE_RETRIES:
                        time.sleep(
                            _STATUS_REPLACE_SLEEP_SEC
                            * (1.0 + (attempt % 3) * 0.25)
                        )
            if last_err is not None:
                published = _write_fallback_status(path, payload)
                _log_status_write_failure(
                    f"write_runtime_status({path})", last_err
                )
            return published
    except Exception as exc:
        _log_status_write_failure(f"write_runtime_status({log_dir})", exc)
        return False


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
        primary_sequence = _status_publish_sequence(primary)
        fallback_sequence = _status_publish_sequence(fallback)
        if fallback_sequence > primary_sequence:
            return fallback, fallback_path
        if primary_sequence > fallback_sequence:
            return primary, primary_path
        if _status_freshness(fallback) > _status_freshness(primary):
            return fallback, fallback_path
        return primary, primary_path
