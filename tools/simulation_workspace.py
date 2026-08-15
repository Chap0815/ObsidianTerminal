"""Immutable optimizer datasets and restart-safe reproducible run state.

This module is deliberately independent from the live runtime.  It freezes the
exact, already-loaded OHLCV frames used by the optimizer, verifies every byte
before reuse and binds baseline/checkpoint evidence to the dataset, code,
configuration, split boundaries, seed and worker count.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import itertools
import json
import math
import os
import platform
import shutil
import stat
import sys
import threading
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DATASET_SCHEMA_VERSION = 1
RUN_SCHEMA_VERSION = 4
MANIFEST_MAX_BYTES = 16 * 1024 * 1024
RUN_FINGERPRINT_MAX_FILES = 256
RUN_FINGERPRINT_FILE_MAX_BYTES = 32 * 1024 * 1024
RUN_FINGERPRINT_TOTAL_MAX_BYTES = 256 * 1024 * 1024
_HOUR_MS = 3_600_000
_HISTORY_COLUMNS = (
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "rsi",
    "macd_h",
)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("reproducible evidence must contain finite numbers")
        return value
    if isinstance(value, dict):
        normalized = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("reproducible evidence keys must be strings")
            normalized[key] = _jsonable(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    item = getattr(value, "item", None)
    if callable(item):
        return _jsonable(item())
    raise ValueError(f"unsupported reproducible evidence type: {type(value).__name__}")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def canonical_evidence_sha256(value: Any) -> str:
    """Hash one finite JSON-compatible evidence value canonically."""
    return _sha256_bytes(_canonical_bytes(value))


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: str | os.PathLike, *, label: str) -> Path:
    requested = Path(path).expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} must not contain links")
    return requested


def _sha256_file(path: Path, *, maximum_bytes: int | None = None) -> str:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            total += len(chunk)
            if maximum_bytes is not None and total > maximum_bytes:
                raise ValueError(f"fingerprint file exceeds {maximum_bytes} bytes")
            digest.update(chunk)
    return digest.hexdigest()


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        int(value.st_mode),
        int(value.st_size),
        int(value.st_mtime_ns),
        int(value.st_ctime_ns),
        int(value.st_dev),
        int(value.st_ino),
    )


def _same_file_object(left: tuple[int, ...], right: tuple[int, ...]) -> bool:
    # Windows may expose a slightly different ctime through fstat() and stat()
    # for the same open file. Size, mtime and native file identity remain stable.
    return all(left[index] == right[index] for index in (0, 1, 2, 4, 5))


def _stable_file_fingerprint(
    path: Path,
    *,
    public_path: str,
    label: str,
) -> tuple[dict, dict]:
    """Hash one regular, link-free file and reject concurrent replacement/write."""
    try:
        verified_path = _absolute_without_links(path, label=label)
        with verified_path.open("rb") as identity_handle:
            before = os.fstat(identity_handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError(f"{label} must be a regular file")
            if before.st_size > RUN_FINGERPRINT_FILE_MAX_BYTES:
                raise ValueError(
                    f"{label} exceeds {RUN_FINGERPRINT_FILE_MAX_BYTES} bytes"
                )
            digest = _sha256_file(
                verified_path, maximum_bytes=RUN_FINGERPRINT_FILE_MAX_BYTES
            )
            identity_handle.seek(0)
            confirmed_digest = hashlib.sha256()
            confirmed_bytes = 0
            for chunk in iter(lambda: identity_handle.read(1024 * 1024), b""):
                confirmed_bytes += len(chunk)
                if confirmed_bytes > RUN_FINGERPRINT_FILE_MAX_BYTES:
                    raise ValueError(
                        f"{label} exceeds {RUN_FINGERPRINT_FILE_MAX_BYTES} bytes"
                    )
                confirmed_digest.update(chunk)
            after = os.fstat(identity_handle.fileno())
        current_path = _absolute_without_links(verified_path, label=label)
        current = current_path.stat()
    except (OSError, ValueError) as exc:
        if isinstance(exc, ValueError) and "exceeds" in str(exc):
            raise
        raise ValueError(f"{label} changed during fingerprint") from exc
    before_identity = _stat_identity(before)
    after_identity = _stat_identity(after)
    current_identity = _stat_identity(current)
    if (
        before_identity != after_identity
        or not _same_file_object(after_identity, current_identity)
        or not stat.S_ISREG(current.st_mode)
        or confirmed_bytes != before.st_size
        or confirmed_digest.hexdigest() != digest
    ):
        raise ValueError(f"{label} changed during fingerprint")
    public = {
        "path": public_path,
        "bytes": int(before.st_size),
        "sha256": digest,
    }
    observation = {
        "path": verified_path,
        "identity": current_identity,
        "expected": public,
        "label": label,
    }
    return public, observation


def _environment_fingerprint() -> dict:
    packages = {}
    for name in ("numpy", "pandas"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "python_build": list(platform.python_build()),
        "byteorder": sys.byteorder,
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "packages": packages,
    }


def _read_json(path: Path) -> dict:
    try:
        path = _absolute_without_links(path, label="JSON manifest")
    except ValueError as exc:
        raise ValueError("JSON manifest must be a real file") from exc
    if not path.is_file():
        raise ValueError("JSON manifest must be a real file")
    with path.open("rb") as handle:
        raw = handle.read(MANIFEST_MAX_BYTES + 1)
    if len(raw) > MANIFEST_MAX_BYTES:
        raise ValueError(f"manifest exceeds {MANIFEST_MAX_BYTES} bytes")

    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("reproducible JSON contains duplicate keys")
            result[key] = item
        return result

    def reject_constant(_value):
        raise ValueError("reproducible JSON contains a non-finite constant")

    try:
        value = json.loads(
            raw.decode("utf-8-sig"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON manifest: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"manifest must be a JSON object: {path}")
    try:
        _jsonable(value)
    except ValueError as exc:
        raise ValueError("reproducible JSON contains a non-finite number") from exc
    return value


def _fsync_directory(path: Path) -> None:
    try:
        directory_fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        try:
            os.close(directory_fd)
        except OSError:
            pass


def _atomic_write(path: Path, raw: bytes) -> None:
    if len(raw) > MANIFEST_MAX_BYTES:
        raise ValueError(f"manifest exceeds {MANIFEST_MAX_BYTES} bytes")
    path = _absolute_without_links(path, label="immutable evidence path")
    path.parent.mkdir(parents=True, exist_ok=True)
    path = _absolute_without_links(path, label="immutable evidence path")
    if _is_linklike(path):
        raise ValueError("immutable evidence conflict")

    def existing_matches() -> bool:
        if path.is_symlink() or not path.is_file():
            return False
        try:
            if path.stat().st_size != len(raw):
                return False
            with path.open("rb") as handle:
                return handle.read(len(raw) + 1) == raw
        except OSError:
            return False

    if path.exists():
        if existing_matches():
            return
        raise ValueError("immutable evidence conflict")
    temp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp, path)
        except FileExistsError as exc:
            if existing_matches():
                return
            raise ValueError("immutable evidence conflict") from exc
        _fsync_directory(path.parent)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _utc_cutoff(value: str | datetime) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("cutoff_utc must be an ISO-8601 timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise ValueError("cutoff_utc must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("cutoff_utc must include a UTC offset")
    parsed = parsed.astimezone(timezone.utc)
    if parsed.microsecond:
        raise ValueError("cutoff_utc must use whole seconds")
    return parsed


def _validated_history_rows(raw_rows, cutoff_ms: int) -> list[list[float | int]]:
    rows = []
    previous = None
    for raw in raw_rows:
        if not isinstance(raw, (list, tuple)) or len(raw) != len(_HISTORY_COLUMNS):
            raise ValueError("OHLCV rows must match the dataset column contract")
        timestamp = raw[0]
        if isinstance(timestamp, bool):
            raise ValueError("OHLCV timestamp must be an integer")
        try:
            timestamp_number = float(timestamp)
            values = [float(item) for item in raw[1:]]
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("OHLCV evidence must be numeric") from exc
        if (
            not math.isfinite(timestamp_number)
            or not timestamp_number.is_integer()
            or timestamp_number <= 0
            or timestamp_number >= cutoff_ms
            or any(not math.isfinite(item) for item in values)
        ):
            raise ValueError("OHLCV evidence must be finite and precede cutoff_utc")
        timestamp_int = int(timestamp_number)
        if previous is not None and timestamp_int - previous != _HOUR_MS:
            raise ValueError("OHLCV timestamps must be contiguous 1h candles")
        open_, high, low, close, volume = values[:5]
        if (
            min(open_, high, low, close) <= 0.0
            or volume < 0.0
            or low > min(open_, close)
            or high < max(open_, close)
        ):
            raise ValueError("OHLCV candle geometry is invalid")
        rows.append([timestamp_int, *values])
        previous = timestamp_int
    if not rows:
        raise ValueError("history frame must not be empty")
    return rows


def _history_rows(frame, cutoff_ms: int) -> list[list[float | int]]:
    missing = [column for column in _HISTORY_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"history frame is missing columns: {', '.join(missing)}")
    return _validated_history_rows(
        frame.loc[:, _HISTORY_COLUMNS].itertuples(index=False, name=None),
        cutoff_ms,
    )


def _series_filename(symbol: str) -> str:
    digest = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
    return f"series/{digest}.json"


def _market_identity_provenance(provenance: dict | None) -> dict:
    if not isinstance(provenance, dict):
        return {}
    raw_exchange = provenance.get("exchange_id") or provenance.get("exchange")
    identity = {}
    if isinstance(raw_exchange, str) and raw_exchange.strip():
        identity["exchange"] = "".join(
            char for char in raw_exchange.strip().lower() if char.isalnum()
        )
    market_type = provenance.get("market_type")
    if isinstance(market_type, str) and market_type.strip():
        identity["market_type"] = market_type.strip().lower()
    return identity


def freeze_history_dataset(
    history: dict,
    workspace_root: str | os.PathLike,
    *,
    cutoff_utc: str | datetime,
    provenance: dict | None = None,
) -> Path:
    """Freeze exact optimizer frames below ``workspace_root/datasets``.

    The returned directory name is the SHA-256 fingerprint of all normalized
    series plus the cutoff and universe.  Existing content is never overwritten.
    """
    if not isinstance(history, dict) or not history:
        raise ValueError("history must be a non-empty symbol mapping")
    cutoff = _utc_cutoff(cutoff_utc)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    try:
        workspace = _absolute_without_links(
            workspace_root, label="dataset workspace"
        )
    except ValueError as exc:
        raise ValueError("dataset workspace must be a real path") from exc
    datasets = workspace / "datasets"
    datasets.mkdir(parents=True, exist_ok=True)
    datasets = _absolute_without_links(datasets, label="dataset workspace")
    staging = datasets / f".staging-{os.getpid()}-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        series = []
        for symbol in sorted(history):
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("history symbols must be non-empty strings")
            rows = _history_rows(history[symbol], cutoff_ms)
            relative = _series_filename(symbol)
            raw = _canonical_bytes(rows)
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            series.append(
                {
                    "symbol": symbol,
                    "timeframe": "1h",
                    "path": relative,
                    "sha256": _sha256_bytes(raw),
                    "bytes": len(raw),
                    "rows": len(rows),
                    "first_ts": rows[0][0],
                    "last_ts": rows[-1][0],
                }
            )
        payload = {
            "schema_version": DATASET_SCHEMA_VERSION,
            "kind": "optimizer_ohlcv_1h",
            "cutoff_utc": cutoff.isoformat(),
            "market_identity": _market_identity_provenance(provenance),
            "universe": [item["symbol"] for item in series],
            "series": series,
        }
        fingerprint = _sha256_bytes(_canonical_bytes(payload))
        manifest = {
            "dataset_fingerprint": fingerprint,
            "fingerprint_payload": payload,
        }
        _atomic_write(staging / "dataset_manifest.json", _canonical_bytes(manifest))
        final = datasets / fingerprint
        if final.exists():
            verify_history_dataset(final)
            return final
        try:
            os.rename(staging, final)
        except OSError:
            # Another local worker may have published the identical immutable
            # dataset between the existence check and the atomic rename.
            if final.is_dir():
                verify_history_dataset(final)
                return final
            raise
        _fsync_directory(datasets)
        for path in final.rglob("*"):
            if path.is_file():
                try:
                    path.chmod(stat.S_IREAD)
                except OSError:
                    pass
        return final
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def verify_history_dataset(dataset_root: str | os.PathLike) -> dict:
    try:
        root = _absolute_without_links(dataset_root, label="dataset root")
    except ValueError as exc:
        raise ValueError("dataset root must be a real directory") from exc
    if not root.is_dir():
        raise ValueError("dataset root must be a real directory")
    manifest = _read_json(root / "dataset_manifest.json")
    payload = manifest.get("fingerprint_payload")
    fingerprint = manifest.get("dataset_fingerprint")
    if not isinstance(payload, dict) or payload.get("schema_version") != DATASET_SCHEMA_VERSION:
        raise ValueError("unsupported dataset manifest schema")
    expected_fingerprint = _sha256_bytes(_canonical_bytes(payload))
    if fingerprint != expected_fingerprint or root.name != fingerprint:
        raise ValueError("dataset fingerprint does not match manifest or directory")
    series = payload.get("series")
    if not isinstance(series, list) or not series:
        raise ValueError("dataset series manifest must not be empty")
    cutoff = _utc_cutoff(payload.get("cutoff_utc"))
    cutoff_ms = int(cutoff.timestamp() * 1000)
    expected = {"dataset_manifest.json"}
    for item in series:
        if not isinstance(item, dict):
            raise ValueError("dataset series entry must be an object")
        relative = item.get("path")
        if not isinstance(relative, str):
            raise ValueError("dataset series path is invalid")
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError("dataset series path escapes root")
        try:
            path = _absolute_without_links(
                root / relative_path, label="dataset series"
            )
        except ValueError as exc:
            raise ValueError(
                f"dataset series is missing or linked: {relative}"
            ) from exc
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError("dataset series path escapes root") from exc
        if not path.is_file():
            raise ValueError(f"dataset series is missing or linked: {relative}")
        if path.stat().st_size != item.get("bytes") or _sha256_file(path) != item.get("sha256"):
            raise ValueError(f"dataset series fingerprint mismatch: {relative}")
        rows = _read_series(path)
        normalized = _validated_history_rows(rows, cutoff_ms)
        if (
            item.get("timeframe") != "1h"
            or item.get("rows") != len(normalized)
            or item.get("first_ts") != normalized[0][0]
            or item.get("last_ts") != normalized[-1][0]
        ):
            raise ValueError(f"dataset series metadata mismatch: {relative}")
        expected.add(relative.replace("\\", "/"))
    discovered = list(root.rglob("*"))
    if any(_is_linklike(path) for path in discovered):
        raise ValueError("dataset contains linked paths")
    actual = {
        path.relative_to(root).as_posix()
        for path in discovered
        if path.is_file()
    }
    if actual != expected:
        raise ValueError("dataset contains missing or unmanifested files")
    return manifest


def load_history_dataset(dataset_root: str | os.PathLike) -> tuple[dict, dict]:
    manifest = verify_history_dataset(dataset_root)
    root = _absolute_without_links(dataset_root, label="dataset root")
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required to load optimizer datasets") from exc
    history = {}
    for item in manifest["fingerprint_payload"]["series"]:
        raw = _read_series(root / item["path"])
        frame = pd.DataFrame(raw, columns=_HISTORY_COLUMNS)
        # Epoch milliseconds are UTC by contract; keep the timezone explicit so
        # every persisted train/validation/holdout boundary is unambiguous.
        frame["dt"] = pd.to_datetime(frame["ts"], unit="ms", utc=True)
        history[item["symbol"]] = frame
    return history, manifest


def _read_series(path: Path) -> list:
    with path.open("rb") as handle:
        raw = handle.read()
    try:
        rows = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid dataset series: {path.name}") from exc
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"empty dataset series: {path.name}")
    return rows


def split_boundaries(
    train_validation_times: Iterable,
    validation_folds: Iterable[Iterable],
    holdout_times: Iterable,
) -> dict:
    tune = list(train_validation_times)
    holdout = list(holdout_times)

    def bounds(values: list) -> dict | None:
        if not values:
            return None
        return {
            "start": _jsonable(values[0]),
            "end": _jsonable(values[-1]),
            "timestamps": len(values),
        }

    return {
        "train_validation": bounds(tune),
        "validation_folds": [bounds(list(fold)) for fold in validation_folds],
        "final_holdout": bounds(holdout),
    }


class ReproducibleRun:
    """Run manifest plus atomically persisted baseline and grid checkpoints."""

    def __init__(
        self,
        workspace_root: str | os.PathLike,
        dataset_root: str | os.PathLike,
        *,
        run_config: dict,
        splits: dict,
        seed: int,
        workers: int,
        code_files: Iterable[str | os.PathLike],
        dependency_lock_file: str | os.PathLike | None = None,
        resume: bool = False,
        dataset_verifier=None,
    ) -> None:
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise ValueError("seed must be an integer")
        if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 256:
            raise ValueError("workers must be an integer between 1 and 256")
        verifier = dataset_verifier or verify_history_dataset
        if not callable(verifier):
            raise ValueError("dataset_verifier must be callable")
        try:
            verified_dataset_root = _absolute_without_links(
                dataset_root, label="dataset root"
            )
        except ValueError as exc:
            raise ValueError("dataset root must be a real directory") from exc
        if not verified_dataset_root.is_dir():
            raise ValueError("dataset root must be a real directory")
        dataset = verifier(verified_dataset_root)
        if (
            not isinstance(dataset, dict)
            or not isinstance(dataset.get("dataset_fingerprint"), str)
            or not dataset["dataset_fingerprint"]
        ):
            raise ValueError("dataset verifier returned an invalid manifest")
        if isinstance(code_files, (str, bytes, os.PathLike)):
            raise ValueError("code fingerprint files must be an iterable of paths")
        try:
            raw_code_files = list(itertools.islice(
                iter(code_files), RUN_FINGERPRINT_MAX_FILES + 1
            ))
        except TypeError as exc:
            raise ValueError(
                "code fingerprint files must be an iterable of paths"
            ) from exc
        if len(raw_code_files) > RUN_FINGERPRINT_MAX_FILES:
            raise ValueError("run fingerprint file limit exceeded")
        resolved_code_files = []
        for raw_path in raw_code_files:
            try:
                path = _absolute_without_links(
                    raw_path, label="code fingerprint file"
                )
            except ValueError as exc:
                raise ValueError(
                    f"code fingerprint file is missing: {raw_path}"
                ) from exc
            if not path.is_file():
                raise ValueError(f"code fingerprint file is missing: {path}")
            resolved_code_files.append(path)
        if not resolved_code_files:
            raise ValueError("code fingerprint files must not be empty")
        if len(set(resolved_code_files)) != len(resolved_code_files):
            raise ValueError("code fingerprint files must be unique")
        resolved_dependency_lock = None
        if dependency_lock_file is not None:
            try:
                resolved_dependency_lock = _absolute_without_links(
                    dependency_lock_file, label="dependency lock file"
                )
            except ValueError as exc:
                raise ValueError("dependency lock file is missing") from exc
            if not resolved_dependency_lock.is_file():
                raise ValueError(
                    f"dependency lock file is missing: {resolved_dependency_lock}"
                )
            if resolved_dependency_lock in resolved_code_files:
                raise ValueError("dependency lock file must be separate from code files")
            if len(resolved_code_files) + 1 > RUN_FINGERPRINT_MAX_FILES:
                raise ValueError("run fingerprint file limit exceeded")
        try:
            code_root = Path(os.path.commonpath(
                [str(path.parent) for path in resolved_code_files]
            ))
        except ValueError as exc:
            raise ValueError(
                "code fingerprint files must share a stable root"
            ) from exc
        code = []
        source_observations = []
        total_fingerprint_bytes = 0
        for path in resolved_code_files:
            relative = path.relative_to(code_root).as_posix()
            public, observation = _stable_file_fingerprint(
                path,
                public_path=relative,
                label="code fingerprint file",
            )
            code.append(public)
            observation["kind"] = "code"
            source_observations.append(observation)
            total_fingerprint_bytes += int(public["bytes"])
            if total_fingerprint_bytes > RUN_FINGERPRINT_TOTAL_MAX_BYTES:
                raise ValueError(
                    "run fingerprint files exceed "
                    f"{RUN_FINGERPRINT_TOTAL_MAX_BYTES} bytes"
                )
        if len({item["path"] for item in code}) != len(code):
            raise ValueError("code fingerprint identities must be unique")
        code.sort(key=lambda item: (item["path"], item["sha256"]))
        dependency_lock = None
        if resolved_dependency_lock is not None:
            dependency_lock, observation = _stable_file_fingerprint(
                resolved_dependency_lock,
                public_path=resolved_dependency_lock.name,
                label="dependency lock file",
            )
            observation["kind"] = "dependency lock"
            source_observations.append(observation)
            total_fingerprint_bytes += int(dependency_lock["bytes"])
            if total_fingerprint_bytes > RUN_FINGERPRINT_TOTAL_MAX_BYTES:
                raise ValueError(
                    "run fingerprint files exceed "
                    f"{RUN_FINGERPRINT_TOTAL_MAX_BYTES} bytes"
                )
        spec = {
            "schema_version": RUN_SCHEMA_VERSION,
            "dataset_fingerprint": dataset["dataset_fingerprint"],
            "code": code,
            "dependency_lock": dependency_lock,
            "environment": _environment_fingerprint(),
            "config": _jsonable(run_config),
            "splits": _jsonable(splits),
            "seeds": {"optimizer": seed},
            "workers": workers,
        }
        run_id = _sha256_bytes(_canonical_bytes(spec))
        try:
            workspace = _absolute_without_links(
                workspace_root, label="reproducible workspace"
            )
        except ValueError as exc:
            raise ValueError(
                "reproducible workspace must be a real path"
            ) from exc
        runs_root = workspace / "runs"
        self.root = runs_root / run_id
        self.run_id = run_id
        self.spec = spec
        self._lock = threading.Lock()
        self._source_lock = threading.Lock()
        self._source_observations = source_observations
        self._revalidate_sources()
        manifest_path = self.root / "run_manifest.json"
        manifest = {
            "run_id": run_id,
            "run_spec": spec,
        }
        if self.root.exists():
            existing = _read_json(manifest_path)
            if (
                existing.get("run_id") != run_id
                or _canonical_bytes(existing.get("run_spec"))
                != _canonical_bytes(spec)
            ):
                raise ValueError("existing run manifest conflicts with requested run")
            if not resume:
                raise FileExistsError("run already exists; pass resume=True to continue")
        else:
            if resume:
                raise FileNotFoundError("resume requested but reproducible run does not exist")
            runs_root.mkdir(parents=True, exist_ok=True)
            staging = runs_root / f".staging-{os.getpid()}-{uuid.uuid4().hex}"
            try:
                staging.mkdir()
                _atomic_write(
                    staging / "run_manifest.json", _canonical_bytes(manifest)
                )
                (staging / "checkpoints").mkdir()
                try:
                    os.rename(staging, self.root)
                except OSError as exc:
                    if self.root.is_dir():
                        existing = _read_json(manifest_path)
                        if (
                            existing.get("run_id") != run_id
                            or _canonical_bytes(existing.get("run_spec"))
                            != _canonical_bytes(spec)
                        ):
                            raise ValueError(
                                "existing run manifest conflicts with requested run"
                            ) from exc
                        raise FileExistsError(
                            "run already exists; pass resume=True to continue"
                        ) from exc
                    raise
                _fsync_directory(runs_root)
            finally:
                if staging.exists():
                    shutil.rmtree(staging, ignore_errors=True)
        (self.root / "checkpoints").mkdir(exist_ok=True)

    @staticmethod
    def _params_hash(params: dict) -> str:
        return canonical_evidence_sha256(params)

    def _revalidate_sources(self) -> None:
        with self._source_lock:
            for observation in self._source_observations:
                kind = observation["kind"]
                path = observation["path"]
                try:
                    verified_path = _absolute_without_links(
                        path, label=observation["label"]
                    )
                    current = verified_path.stat()
                    if (
                        stat.S_ISREG(current.st_mode)
                        and _stat_identity(current) == observation["identity"]
                    ):
                        continue
                    public, refreshed = _stable_file_fingerprint(
                        verified_path,
                        public_path=observation["expected"]["path"],
                        label=observation["label"],
                    )
                except (OSError, ValueError) as exc:
                    raise ValueError(f"{kind} fingerprint changed") from exc
                if public != observation["expected"]:
                    raise ValueError(f"{kind} fingerprint changed")
                observation["identity"] = refreshed["identity"]

    @staticmethod
    def _seal_evidence(value: dict) -> dict:
        sealed = dict(value)
        sealed["evidence_sha256"] = _sha256_bytes(_canonical_bytes(value))
        return sealed

    @staticmethod
    def _verify_evidence(value: dict, *, label: str) -> dict:
        expected = value.get("evidence_sha256")
        body = {key: item for key, item in value.items() if key != "evidence_sha256"}
        actual = _sha256_bytes(_canonical_bytes(body))
        if not isinstance(expected, str) or expected != actual:
            raise ValueError(f"{label} evidence integrity check failed")
        return body

    def baseline(self, expected_params: dict | None = None) -> dict | None:
        self._revalidate_sources()
        path = self.root / "baseline.json"
        if not path.exists():
            return None
        value = self._verify_evidence(_read_json(path), label="baseline")
        if value.get("run_id") != self.run_id:
            raise ValueError("baseline is bound to a different run")
        stored_params = value.get("params")
        result = value.get("result")
        if (
            not isinstance(stored_params, dict)
            or not isinstance(result, dict)
            or value.get("params_hash") != self._params_hash(stored_params)
        ):
            raise ValueError("baseline evidence contract is invalid")
        if expected_params is not None:
            if not isinstance(expected_params, dict):
                raise ValueError("expected baseline params must be an object")
            if value["params_hash"] != self._params_hash(expected_params):
                raise ValueError("baseline conflicts with expected parameters")
        return self._seal_evidence(value)

    def record_baseline(self, params: dict, result: dict) -> dict:
        if not isinstance(params, dict) or not isinstance(result, dict):
            raise ValueError("baseline params and result must be objects")
        value = self._seal_evidence({
            "run_id": self.run_id,
            "params_hash": self._params_hash(params),
            "params": _jsonable(params),
            "result": _jsonable(result),
        })
        path = self.root / "baseline.json"
        with self._lock:
            self._revalidate_sources()
            if path.exists():
                existing = _read_json(path)
                if _canonical_bytes(existing) != _canonical_bytes(value):
                    raise ValueError("baseline evidence conflicts with existing run")
                return existing
            _atomic_write(path, _canonical_bytes(value))
        return value

    def checkpoint(self, index: int, params: dict) -> dict | None:
        if not isinstance(params, dict):
            raise ValueError("checkpoint params must be an object")
        self._revalidate_sources()
        path = self._checkpoint_path(index)
        if not path.exists():
            return None
        value = self._verify_evidence(
            _read_json(path), label=f"checkpoint {index}"
        )
        if (
            value.get("run_id") != self.run_id
            or value.get("candidate_index") != index
            or value.get("params_hash") != self._params_hash(params)
        ):
            raise ValueError(f"checkpoint {index} conflicts with deterministic grid")
        record = value.get("record")
        if not isinstance(record, dict):
            raise ValueError(f"checkpoint {index} record contract is invalid")
        return record

    def record_checkpoint(self, index: int, params: dict, record: dict) -> None:
        if not isinstance(params, dict) or not isinstance(record, dict):
            raise ValueError("checkpoint params and record must be objects")
        if self.baseline() is None:
            raise RuntimeError("production baseline must complete before optimization")
        value = self._seal_evidence({
            "run_id": self.run_id,
            "candidate_index": index,
            "params_hash": self._params_hash(params),
            "record": _jsonable(record),
        })
        path = self._checkpoint_path(index)
        with self._lock:
            self._revalidate_sources()
            if path.exists():
                existing = _read_json(path)
                if _canonical_bytes(existing) != _canonical_bytes(value):
                    raise ValueError(f"checkpoint {index} already contains other evidence")
                return
            _atomic_write(path, _canonical_bytes(value))

    def _checkpoint_path(self, index: int) -> Path:
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ValueError("candidate index must be a non-negative integer")
        return self.root / "checkpoints" / f"{index:08d}.json"
