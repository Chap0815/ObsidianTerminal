"""Run a causal 150-second FUTURES replay on an immutable MEXC dataset."""

# ruff: noqa: E402  # update barrier must run before project/runtime imports

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from launcher.tool_processes import guard_tool_entrypoint

guard_tool_entrypoint(__file__, __name__)

from tools.backtester import simulate_fast
from trading.futures_capture_replay import (
    build_capture_replay_index,
    load_replay_dataset,
)


MAX_CONFIG_BYTES = 1024 * 1024
MAX_REPLAY_REPORT_BYTES = 64 * 1024 * 1024


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: Path, *, label: str) -> Path:
    requested = path.expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} path must not contain links")
    return requested


def _require_finite_json(value) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("replay JSON contains a non-finite number")
    if isinstance(value, dict):
        for item in value.values():
            _require_finite_json(item)
    elif isinstance(value, list):
        for item in value:
            _require_finite_json(item)


def _read_config(path: Path | None) -> dict:
    if path is None:
        return {}
    try:
        path = _absolute_without_links(path, label="replay config")
    except ValueError as exc:
        raise ValueError("replay config must be a real file") from exc
    if not path.is_file():
        raise ValueError("replay config must be a real file")
    with path.open("rb") as handle:
        raw = handle.read(MAX_CONFIG_BYTES + 1)
    if len(raw) > MAX_CONFIG_BYTES:
        raise ValueError("replay config is oversized")
    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("replay config contains duplicate keys")
            result[key] = item
        return result

    def reject_constant(_value):
        raise ValueError("replay config contains a non-finite constant")

    try:
        value = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("replay config is invalid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("replay config must be a JSON object")
    _require_finite_json(value)
    return value


def _fsync_directory(path: Path) -> None:
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
                    "replay output directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _mkdir_with_parent_fsync(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    parent = path.parent
    while True:
        _fsync_directory(parent)
        if parent == parent.parent:
            break
        parent = parent.parent


def _write_immutable_output(path: Path, encoded: bytes) -> Path:
    if len(encoded) > MAX_REPLAY_REPORT_BYTES:
        raise ValueError("replay output is oversized")
    path = _absolute_without_links(path, label="replay output")
    _mkdir_with_parent_fsync(path.parent)
    path = _absolute_without_links(path, label="replay output")

    def existing_matches() -> bool:
        if _is_linklike(path) or not path.is_file():
            return False
        try:
            if path.stat().st_size != len(encoded):
                return False
            with path.open("rb") as handle:
                return handle.read(len(encoded) + 1) == encoded
        except OSError:
            return False

    if path.exists() or _is_linklike(path):
        if existing_matches():
            _fsync_directory(path.parent)
            return path
        raise FileExistsError("immutable replay output conflict")
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    primary_error: BaseException | None = None
    temporary_owned = False
    handle = None
    try:
        handle = temporary.open("xb")
        temporary_owned = True
        try:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if primary_error is None:
                    primary_error = close_error
                    raise
                try:
                    primary_error.add_note(
                        "replay output close failed: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            if existing_matches():
                _fsync_directory(path.parent)
                return path
            raise FileExistsError("immutable replay output conflict") from exc
        _fsync_directory(path.parent)
    except BaseException as exc:
        if primary_error is None:
            primary_error = exc
        raise
    finally:
        if temporary_owned:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            except BaseException as cleanup_error:
                if primary_error is None:
                    raise
                try:
                    primary_error.add_note(
                        "replay output cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass
    return path


def _json_safe(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("replay result contains a non-finite number")
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def run_replay(dataset_path: Path, config: dict) -> dict:
    dataset = load_replay_dataset(dataset_path)
    min_pump = config.get("min_pump", 1.0)
    indexed, all_times, evidence = build_capture_replay_index(
        dataset,
        min_pump=float(min_pump),
    )
    params = dict(config)
    params.update({
        "capture_replay": True,
        "futures_screener_parity": True,
        "historical_funding_timeline": dataset.funding,
    })
    result = simulate_fast(indexed, all_times, "FUTURES", False, params)
    visible_config = {
        key: value for key, value in params.items()
        if key != "historical_funding_timeline"
    }
    return _json_safe({
        "method": "mexc_futures_capture_replay_v1",
        "dataset": evidence,
        "config": visible_config,
        "result": result,
        "research_only": True,
        "promotion_eligible": False,
    })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--json-output", type=Path, required=True)
    args = parser.parse_args(argv)
    config = _read_config(args.config)
    report = run_replay(args.dataset, config)
    encoded = json.dumps(
        report,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ) + "\n"
    output_path = _write_immutable_output(
        args.json_output, encoded.encode("utf-8")
    )
    print(json.dumps({
        "dataset_fingerprint": report["dataset"]["dataset_fingerprint"],
        "trades": report["result"]["trades"],
        "net": report["result"]["net"],
        "output": str(output_path),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
