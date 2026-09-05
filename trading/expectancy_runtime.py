"""Versioned expectancy-model loading for live/shadow entry decisions."""
from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import tempfile
from dataclasses import asdict
from pathlib import Path

from shared_limits import normalize_gate_mode
from trading.profit_experiments import (
    ExpectancyDecision,
    LinearExpectancyModel,
    decide_net_expectancy,
)


_EXPECTANCY_MODEL_JSON_MAX_BYTES = 1024 * 1024
_EXPECTANCY_REPORT_JSON_MAX_BYTES = 16 * 1024 * 1024


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: str | os.PathLike, *, label: str) -> Path:
    requested = Path(path).expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} must be a real link-free path")
    return requested


def _sync_directory(path: Path) -> None:
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(str(path), flags)
    except AttributeError:
        return
    except OSError as exc:
        if os.name == "nt":
            if isinstance(exc, PermissionError):
                return
            if (
                isinstance(exc, FileNotFoundError)
                and path == Path(path.anchor)
                and path.is_dir()
            ):
                return
        raise
    primary_error: BaseException | None = None
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
                    "expectancy directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _sync_parent_chain(path: Path) -> None:
    parent = path.parent
    while True:
        _sync_directory(parent)
        if parent == parent.parent:
            break
        parent = parent.parent


def _same_file_generation(path: Path, identity: tuple[int, int]) -> bool:
    try:
        current = path.stat(follow_symlinks=False)
    except (OSError, ValueError):
        return False
    return (
        stat.S_ISREG(current.st_mode)
        and (current.st_dev, current.st_ino) == identity
    )


def _note_cleanup_error(primary: BaseException, error: BaseException) -> None:
    try:
        primary.add_note(
            "expectancy temporary cleanup failed: "
            f"{type(error).__name__}: {error}"
        )
    except BaseException:
        pass


def _publish_expectancy_json(path: str | Path, encoded: bytes, *, label: str) -> Path:
    target = _absolute_without_links(path, label=label)
    target.parent.mkdir(parents=True, exist_ok=True)
    target = _absolute_without_links(target, label=label)
    _sync_parent_chain(target.parent)
    if not target.parent.is_dir():
        raise ValueError(f"{label} parent must be a real directory")
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    temp = Path(temp_name)
    fd_owned = True
    temp_owned = True
    temp_identity: tuple[int, int] | None = None
    published = False
    primary_error: BaseException | None = None
    try:
        temp_stat = os.fstat(fd)
        temp_identity = (temp_stat.st_dev, temp_stat.st_ino)
        handle = None
        handle_error: BaseException | None = None
        try:
            handle = os.fdopen(fd, "wb")
            fd_owned = False
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            handle_error = exc
            raise
        finally:
            if handle is not None:
                try:
                    handle.close()
                except BaseException as close_error:
                    if handle_error is None:
                        raise
                    _note_cleanup_error(handle_error, close_error)
        target = _absolute_without_links(target, label=label)
        _absolute_without_links(temp, label=f"{label} temporary path")
        os.replace(temp, target)
        temp_owned = False
        published = True
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        if fd_owned:
            try:
                os.close(fd)
            except BaseException as exc:
                cleanup_error = exc
        same_generation = False
        if temp_owned and temp_identity is not None:
            try:
                same_generation = _same_file_generation(temp, temp_identity)
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
                else:
                    _note_cleanup_error(cleanup_error, exc)
        if same_generation:
            try:
                temp.unlink()
            except FileNotFoundError:
                pass
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
                else:
                    _note_cleanup_error(cleanup_error, exc)
        try:
            if published:
                _sync_directory(target.parent)
        except BaseException as exc:
            if cleanup_error is None:
                cleanup_error = exc
            else:
                _note_cleanup_error(cleanup_error, exc)
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            _note_cleanup_error(primary_error, cleanup_error)
    return target


def _require_json_native(value, *, seen: set[int] | None = None) -> None:
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("expectancy report must contain finite JSON values")
        return
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("expectancy report must contain JSON-native string keys")
        seen = set() if seen is None else seen
        identity = id(value)
        if identity in seen:
            raise ValueError("expectancy report must not contain cycles")
        seen.add(identity)
        try:
            for item in value.values():
                _require_json_native(item, seen=seen)
        finally:
            seen.remove(identity)
        return
    if isinstance(value, list):
        seen = set() if seen is None else seen
        identity = id(value)
        if identity in seen:
            raise ValueError("expectancy report must not contain cycles")
        seen.add(identity)
        try:
            for item in value:
                _require_json_native(item, seen=seen)
        finally:
            seen.remove(identity)
        return
    raise TypeError(
        "expectancy report must contain JSON-native values, "
        f"got {type(value).__name__}"
    )


def save_expectancy_model(
    path: str | Path, model: LinearExpectancyModel
) -> dict[str, str | int]:
    payload = asdict(model)
    encoded = json.dumps(
        payload, sort_keys=True, allow_nan=False
    ).encode("utf-8")
    if len(encoded) > _EXPECTANCY_MODEL_JSON_MAX_BYTES:
        raise ValueError("expectancy model JSON exceeds size limit")
    _publish_expectancy_json(path, encoded, label="expectancy model path")
    return {
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "bytes": len(encoded),
    }


def save_expectancy_training_report(path: str | Path, payload: dict) -> None:
    if not isinstance(payload, dict):
        raise TypeError("expectancy report must be a JSON-native object")
    _require_json_native(payload)
    encoded = json.dumps(
        payload, sort_keys=True, indent=2, allow_nan=False
    ).encode("utf-8")
    if len(encoded) > _EXPECTANCY_REPORT_JSON_MAX_BYTES:
        raise ValueError("expectancy report exceeds size limit")
    _publish_expectancy_json(path, encoded, label="expectancy report path")


def load_expectancy_model(path: str | Path) -> LinearExpectancyModel | None:
    try:
        target = _absolute_without_links(path, label="expectancy model path")
        if not target.is_file():
            return None
        with open(target, "rb") as handle:
            opened_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened_stat.st_mode):
                return None
            opened_identity = (opened_stat.st_dev, opened_stat.st_ino)
            raw = handle.read(_EXPECTANCY_MODEL_JSON_MAX_BYTES + 1)
        _absolute_without_links(target, label="expectancy model path")
        if not _same_file_generation(target, opened_identity):
            return None
        if len(raw) > _EXPECTANCY_MODEL_JSON_MAX_BYTES:
            raise ValueError("expectancy model JSON exceeds size limit")
        payload = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-standard JSON constant: {value}")
            ),
            object_pairs_hook=_unique_json_object,
        )
        if not isinstance(payload, dict):
            return None
        for key in (
            "feature_order",
            "coefficients",
            "feature_means",
            "feature_scales",
        ):
            if key in payload:
                if not isinstance(payload[key], list):
                    return None
                payload[key] = tuple(payload[key])
        return LinearExpectancyModel(**payload)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def evaluate_runtime_expectancy(
    *,
    bot_name: str,
    features: dict,
    mode: str,
    model_path: str | Path | None = None,
) -> ExpectancyDecision:
    normalized_mode = normalize_gate_mode(mode)
    if model_path is None:
        from core.paths import DATA_DIR

        model_path = DATA_DIR / "models" / f"{bot_name.lower()}_expectancy.json"
    model = load_expectancy_model(model_path)
    if model is None:
        enforce = normalized_mode == "enforce"
        return ExpectancyDecision(
            allowed=not enforce,
            shadow_allowed=False,
            expected_net_bps=None,
            probability_positive=None,
            model_version="missing",
            reason="validated expectancy model unavailable",
        )
    return decide_net_expectancy(model, features, mode=normalized_mode)
