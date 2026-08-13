"""Immutable optimizer datasets and restart-safe reproducible run state.

This module is deliberately independent from the live runtime.  It freezes the
exact, already-loaded OHLCV frames used by the optimizer, verifies every byte
before reuse and binds baseline/checkpoint evidence to the dataset, code,
configuration, split boundaries, seed and worker count.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import platform
import shutil
import stat
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


DATASET_SCHEMA_VERSION = 1
RUN_SCHEMA_VERSION = 2
MANIFEST_MAX_BYTES = 16 * 1024 * 1024
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    with path.open("rb") as handle:
        raw = handle.read(MANIFEST_MAX_BYTES + 1)
    if len(raw) > MANIFEST_MAX_BYTES:
        raise ValueError(f"manifest exceeds {MANIFEST_MAX_BYTES} bytes")
    try:
        value = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON manifest: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"manifest must be a JSON object: {path}")
    return value


def _atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
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
    workspace = Path(workspace_root).expanduser().resolve()
    datasets = workspace / "datasets"
    datasets.mkdir(parents=True, exist_ok=True)
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
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "provenance": _jsonable(provenance or {}),
        }
        _atomic_write(staging / "dataset_manifest.json", _canonical_bytes(manifest))
        final = datasets / fingerprint
        if final.exists():
            verify_history_dataset(final)
            return final
        try:
            os.replace(staging, final)
        except OSError:
            # Another local worker may have published the identical immutable
            # dataset between the existence check and the atomic rename.
            if final.is_dir():
                verify_history_dataset(final)
                return final
            raise
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
    root = Path(dataset_root).expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
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
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError("dataset series path escapes root") from exc
        if path.is_symlink() or not path.is_file():
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
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    if actual != expected:
        raise ValueError("dataset contains missing or unmanifested files")
    return manifest


def load_history_dataset(dataset_root: str | os.PathLike) -> tuple[dict, dict]:
    manifest = verify_history_dataset(dataset_root)
    root = Path(dataset_root).expanduser().resolve()
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
        resume: bool = False,
    ) -> None:
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise ValueError("seed must be an integer")
        if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 256:
            raise ValueError("workers must be an integer between 1 and 256")
        dataset = verify_history_dataset(dataset_root)
        code = []
        for raw_path in code_files:
            path = Path(raw_path).expanduser().resolve()
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"code fingerprint file is missing: {path}")
            code.append(
                {
                    "name": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256_file(path),
                }
            )
        code.sort(key=lambda item: (item["name"], item["sha256"]))
        spec = {
            "schema_version": RUN_SCHEMA_VERSION,
            "dataset_fingerprint": dataset["dataset_fingerprint"],
            "code": code,
            "environment": _environment_fingerprint(),
            "config": _jsonable(run_config),
            "splits": _jsonable(splits),
            "seeds": {"optimizer": seed},
            "workers": workers,
        }
        run_id = _sha256_bytes(_canonical_bytes(spec))
        runs_root = Path(workspace_root).expanduser().resolve() / "runs"
        self.root = runs_root / run_id
        self.run_id = run_id
        self.spec = spec
        self._lock = threading.Lock()
        manifest_path = self.root / "run_manifest.json"
        manifest = {
            "run_id": run_id,
            "run_spec": spec,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        if self.root.exists():
            existing = _read_json(manifest_path)
            if existing.get("run_id") != run_id or existing.get("run_spec") != spec:
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
                os.replace(staging, self.root)
            finally:
                if staging.exists():
                    shutil.rmtree(staging, ignore_errors=True)
        (self.root / "checkpoints").mkdir(exist_ok=True)

    @staticmethod
    def _params_hash(params: dict) -> str:
        return _sha256_bytes(_canonical_bytes(params))

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

    def baseline(self) -> dict | None:
        path = self.root / "baseline.json"
        if not path.exists():
            return None
        value = self._verify_evidence(_read_json(path), label="baseline")
        if value.get("run_id") != self.run_id:
            raise ValueError("baseline is bound to a different run")
        return self._seal_evidence(value)

    def record_baseline(self, params: dict, result: dict) -> dict:
        value = self._seal_evidence({
            "run_id": self.run_id,
            "params_hash": self._params_hash(params),
            "params": _jsonable(params),
            "result": _jsonable(result),
        })
        path = self.root / "baseline.json"
        with self._lock:
            if path.exists():
                existing = _read_json(path)
                if existing != value:
                    raise ValueError("baseline evidence conflicts with existing run")
                return existing
            _atomic_write(path, _canonical_bytes(value))
        return value

    def checkpoint(self, index: int, params: dict) -> dict | None:
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
        return value.get("record")

    def record_checkpoint(self, index: int, params: dict, record: dict) -> None:
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
            if path.exists():
                existing = _read_json(path)
                if existing != value:
                    raise ValueError(f"checkpoint {index} already contains other evidence")
                return
            _atomic_write(path, _canonical_bytes(value))

    def _checkpoint_path(self, index: int) -> Path:
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ValueError("candidate index must be a non-negative integer")
        return self.root / "checkpoints" / f"{index:08d}.json"
