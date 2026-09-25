"""Restart-safe immutable venue event partitions."""
from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import sqlite3
import stat
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import portalocker

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.order_utils import (
    explicit_trade_symbol_matches,
    order_id_text_or_none,
)
from bot_utils.runtime_threads import thread_definitely_never_started
from core.constants import NONCRYPTO_BASES
from trading.capture_trade_identity import (
    TradeIdentityConflict,
    canonical_ws_writer_input_hash,
    read_trade_identity,
    validate_trade_identity_schema,
    validate_ws_trade_payload,
)


MAX_PARTITION_CLOCK_AGE_MS = 86_400_000
MAX_EXCHANGE_FUTURE_SKEW_MS = 30_000
_CAPTURE_CONTROL_JSON_MAX_BYTES = 64 * 1024
_CAPTURE_EVENT_PAYLOAD_MAX_BYTES = 16 * 1024 * 1024
_CAPTURE_CONTROL_TEMP_ATTEMPTS = 3
_CAPACITY_OPERATIONAL_RESERVE_RATIO = 1.05
_CAPTURE_PARTITION_STREAMS = ("overview", "depth", "trades", "l2_stream")
_CAPTURE_STORAGE_MAX_PARENTS = 256
_CAPTURE_STORAGE_MAX_ARTIFACTS = 10_000
_CAPTURE_RETENTION_MAX_DAYS = 3_650
_CAPTURE_STORAGE_MIN_GIB = 0.1
_CAPTURE_STORAGE_MAX_GIB = 1_000.0
_STREAM_HEALTH_COUNTER_MAX = (1 << 63) - 1
_STREAM_HEALTH_FIELDS = (
    "connection_epoch",
    "reconnect_attempts",
    "connection_error_type",
    "l2_missing_or_stale",
    "trade_missing_or_stale",
    "trade_duplicates_suppressed",
    "transport_errors_total",
    "transport_errors_consecutive",
    "last_transport_error",
    "last_interruption_wall_ts",
)
_STORAGE_HEALTH_FIELDS = (
    "total_bytes",
    "measurement_complete",
    "measurement_errors",
    "max_storage_bytes",
    "closed_days_observed",
    "peak_closed_day_bytes",
    "projected_required_bytes",
    "required_capacity_bytes",
    "filesystem_free_bytes",
    "filesystem_capacity_bytes",
    "filesystem_probe_error",
    "capacity_ok",
    "capacity_state",
    "operational_reserve_ratio",
    "headroom_ratio",
)
_STORAGE_CAPACITY_STATES = frozenset({
    "collecting",
    "healthy",
    "insufficient",
    "low_headroom",
    "unavailable",
})
_STORAGE_INTEGER_FIELDS = (
    "total_bytes",
    "max_storage_bytes",
    "closed_days_observed",
)
_STORAGE_OPTIONAL_INTEGER_FIELDS = (
    "peak_closed_day_bytes",
    "projected_required_bytes",
    "required_capacity_bytes",
    "filesystem_free_bytes",
    "filesystem_capacity_bytes",
)
_STORAGE_RATIO_FIELDS = (
    "operational_reserve_ratio",
    "headroom_ratio",
)
_STORAGE_CAPACITY_PROOF_FIELDS = (
    "total_bytes",
    "max_storage_bytes",
    "projected_required_bytes",
    "required_capacity_bytes",
    "filesystem_free_bytes",
    "filesystem_capacity_bytes",
)
_CAPTURE_EVENT_SCHEMA = (
    ("event_id", "TEXT", 1, 1),
    ("market_id", "TEXT", 1, 0),
    ("exchange_time", "TEXT", 1, 0),
    ("received_time", "TEXT", 1, 0),
    ("schema_version", "INTEGER", 1, 0),
    ("quality_flags_json", "TEXT", 1, 0),
    ("payload_json", "TEXT", 1, 0),
)
_CAPTURE_EVENT_TIME_INDEX_XINFO = (
    ("exchange_time", 0, "BINARY", 1),
    ("market_id", 0, "BINARY", 1),
    ("event_id", 0, "BINARY", 0),
)
_CAPTURE_SCHEMA_OBJECTS = (
    ("index", "idx_venue_events_time", "venue_events"),
    ("table", "venue_events", "venue_events"),
)
_PARTITION_ROOT_GUARDS: dict[Path, threading.RLock] = {}
_PARTITION_ROOT_GUARDS_LOCK = threading.Lock()
_PARTITION_ROOT_DEPTH = threading.local()


def _strict_capture_payload(raw: str) -> dict:
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate capture payload key")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError(f"invalid capture payload constant: {value}")

    payload = json.loads(raw, object_pairs_hook=unique_pairs,
                         parse_constant=reject_constant)
    if not isinstance(payload, dict):
        raise ValueError("capture payload is not an object")
    return payload
_CAPTURE_CONTROL_FIELDS = frozenset(("schema_version", "connection_epoch"))
_CAPTURE_CONTROL_DURABLE_FIELDS = frozenset(
    (*_CAPTURE_CONTROL_FIELDS, "updated_at")
)


def _unique_capture_control_object(pairs) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate capture control key: {key}")
        result[key] = value
    return result


def _reject_capture_control_constant(value: str):
    raise ValueError(f"invalid capture control constant: {value}")


def _plain_health_text(value, *, max_chars: int, allow_none: bool = False) -> bool:
    if value is None:
        return allow_none
    return bool(
        type(value) is str
        and len(value) <= max_chars
        and all(
            codepoint >= 32
            and codepoint != 127
            and not 0xD800 <= codepoint <= 0xDFFF
            for codepoint in map(ord, value)
        )
    )


def _writer_acknowledged(result) -> bool:
    if result is None:
        return True
    if type(result) is bool:
        return result
    return isinstance(result, Path)


def _capture_now_ms() -> int:
    """Exchange-anchored absolute time for immutable capture provenance."""
    from core.clock import now_ms

    return int(now_ms())


def _capture_now_utc() -> datetime:
    from core.clock import now_utc

    return now_utc()


def _safe_exception_summary(
    exc: BaseException,
    *,
    max_chars: int = 160,
) -> tuple[str, str]:
    """Return a bounded, redacted single-line exception identity."""
    try:
        error_type = type(exc).__name__[:40] or "BaseException"
    except BaseException:
        error_type = "BaseException"
    try:
        raw_detail = str(exc)
    except BaseException:
        raw_detail = "[UNRENDERABLE]"
    try:
        from core.logger import clean_user_text, redact

        detail = clean_user_text(
            redact(raw_detail),
            max_chars=max_chars,
        )
    except BaseException:
        # An error status must never fall back to publishing an unredacted
        # exception if the sanitizer itself is unavailable.
        detail = "[REDACTION_FAILED]"
    try:
        detail = " ".join(detail.split()).strip()[:max_chars]
    except BaseException:
        detail = "[UNRENDERABLE]"
    return error_type, detail


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
                    "capture directory close failed: "
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


@dataclass(frozen=True)
class VenueEvent:
    event_id: str
    kind: str
    market_id: str
    exchange_time: str
    received_time: str
    payload: dict
    schema_version: int = 1
    quality_flags: tuple[str, ...] = ()


class SealedCapturePartitionError(RuntimeError):
    """Raised when a write targets an intentionally immutable UTC day."""


class SQLitePartitionWriter:
    """Deduplicated daily SQLite-WAL chunks; one file per stream and UTC day."""

    def __init__(
        self,
        root: str | Path,
        *,
        retention_days: int = 30,
        max_storage_gib: float = 150.0,
    ) -> None:
        if type(retention_days) is not int or retention_days < 1:
            raise ValueError("retention_days must be a positive integer")
        if retention_days > _CAPTURE_RETENTION_MAX_DAYS:
            raise ValueError("retention_days exceeds supported limit")
        if type(max_storage_gib) not in (int, float):
            raise ValueError(
                "max_storage_gib must be a finite nonnegative number"
            )
        if max_storage_gib > _CAPTURE_STORAGE_MAX_GIB:
            raise ValueError("max_storage_gib is outside supported range")
        if (
            not math.isfinite(max_storage_gib)
            or max_storage_gib < 0
        ):
            raise ValueError(
                "max_storage_gib must be a finite nonnegative number"
            )
        if 0 < max_storage_gib < _CAPTURE_STORAGE_MIN_GIB:
            raise ValueError("max_storage_gib is outside supported range")
        self.root = Path(root)
        self.retention_days = retention_days
        self.max_storage_bytes = int(float(max_storage_gib) * 1024**3)
        self._connections: dict[Path, sqlite3.Connection] = {}
        self._setup_connections: list[sqlite3.Connection] = []
        self._setup_connection_candidate: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        self._write_terminal = False
        # Epoch allocation only protects capture_state.json.  It may wait up
        # to 15 seconds on the cross-process lock after a reconnect, so it
        # must not occupy the data-plane lock used by every REST/L2 write.
        self._control_lock = threading.Lock()
        self._partition_locks: dict[str, threading.RLock] = {}
        self._control_path = self.root / "capture_state.json"

    @contextmanager
    def partition_guard(self, day) -> object:
        """Serialize a UTC day's final seal with every in-process write."""
        day_text = day.isoformat() if hasattr(day, "isoformat") else str(day)
        with self._root_partition_guard():
            with self._lock:
                guard = self._partition_locks.setdefault(
                    day_text, threading.RLock()
                )
            with guard:
                yield

    @contextmanager
    def _root_partition_guard(self):
        """One scoped lock for writes, sealing, and retention across writers."""
        key = self.root.resolve()
        with _PARTITION_ROOT_GUARDS_LOCK:
            guard = _PARTITION_ROOT_GUARDS.setdefault(key, threading.RLock())
        with guard:
            depths = getattr(_PARTITION_ROOT_DEPTH, "values", None)
            if depths is None:
                depths = {}
                _PARTITION_ROOT_DEPTH.values = depths
            if depths.get(key, 0):
                depths[key] += 1
                try:
                    yield
                finally:
                    depths[key] -= 1
                return
            lock_path = self.root / ".capture_partition.lock"
            self._assert_scoped_path(lock_path)
            _mkdir_with_parent_fsync(self.root)
            self._assert_scoped_path(lock_path)
            with portalocker.Lock(
                str(lock_path), mode="a", timeout=15.0,
                check_interval=0.05, fail_when_locked=False,
            ):
                depths[key] = 1
                try:
                    yield
                finally:
                    depths.pop(key, None)

    @contextmanager
    def seal_partition_guard(self, day) -> object:
        """Checkpoint a closed UTC day and exclude every later write."""
        day_text = day.isoformat() if hasattr(day, "isoformat") else str(day)
        with self.partition_guard(day_text):
            seal = self.root / "integrity" / f"{day_text}.json"
            self._assert_scoped_path(seal)
            if seal.exists():
                # Published reports are hash-bound to their original files.
                # Verification owns this branch; even closing a WAL handle can
                # checkpoint bytes, so a sealed day must remain untouched.
                yield
                return
            with self._lock:
                for path in list(self._connections):
                    if path.stem == day_text:
                        self._close_path(path)
                # Failed connection setup/rollback can leave an unindexed
                # SQLite handle quarantined for close retry. Its path is no
                # longer knowable here, so every such handle must close before
                # any day can be immutably sealed.
                for connection in list(
                    getattr(self, "_setup_connections", ())
                ):
                    connection.close()
                    self._discard_setup_connection(connection)
                candidate = getattr(
                    self, "_setup_connection_candidate", None
                )
                if candidate is not None:
                    candidate.close()
                    if self._setup_connection_candidate is candidate:
                        self._setup_connection_candidate = None
            # A prior process can leave committed WAL frames without any
            # handle in this writer's registry. Recover those frames only for
            # an unsealed closed day.
            for stream in _CAPTURE_PARTITION_STREAMS:
                self._checkpoint_restart_wal(
                    self.root / stream / f"{day_text}.sqlite3"
                )
            yield

    def _checkpoint_restart_wal(self, path: Path) -> None:
        self._assert_scoped_path(path)
        wal = Path(f"{path}-wal")
        shm = Path(f"{path}-shm")
        self._assert_scoped_path(wal)
        self._assert_scoped_path(shm)
        try:
            path_stat = path.lstat()
        except FileNotFoundError:
            return
        if self._stat_is_linklike(path_stat) or not stat.S_ISREG(path_stat.st_mode):
            raise RuntimeError("capture partition is not a regular file")
        sidecar_data = False
        for sidecar, label in ((wal, "WAL"), (shm, "SHM")):
            try:
                sidecar_stat = sidecar.lstat()
            except FileNotFoundError:
                continue
            if self._stat_is_linklike(sidecar_stat) or not stat.S_ISREG(
                sidecar_stat.st_mode
            ):
                raise RuntimeError(
                    f"capture {label} is not a regular file"
                )
            sidecar_data = sidecar_data or sidecar_stat.st_size > 0
        if not sidecar_data:
            return

        connection = None
        primary_error = None
        try:
            connection = sqlite3.connect(
                f"{path.resolve().as_uri()}?mode=rw",
                uri=True,
                timeout=15.0,
            )
            with self._lock:
                self._setup_connection_candidate = connection
                try:
                    self._setup_connections.append(connection)
                except BaseException as primary_exc:
                    try:
                        connection.close()
                    except BaseException as close_error:
                        try:
                            primary_exc.add_note(
                                "SQLite checkpoint candidate cleanup failed: "
                                f"{type(close_error).__name__}: {close_error}"
                            )
                        except BaseException:
                            pass
                    else:
                        self._discard_setup_connection(connection)
                    raise
                self._setup_connection_candidate = None
            journal_mode = connection.execute(
                "PRAGMA journal_mode"
            ).fetchone()
            if (
                journal_mode is None
                or str(journal_mode[0]).strip().lower() != "wal"
            ):
                raise RuntimeError(
                    "capture restart WAL mode is unavailable"
                )
            connection.execute("PRAGMA busy_timeout=15000")
            busy_timeout = connection.execute(
                "PRAGMA busy_timeout"
            ).fetchone()
            if busy_timeout is None or int(busy_timeout[0]) != 15_000:
                raise RuntimeError(
                    "capture restart WAL busy timeout is unavailable"
                )
            connection.execute("PRAGMA synchronous=FULL")
            synchronous = connection.execute(
                "PRAGMA synchronous"
            ).fetchone()
            if synchronous is None or int(synchronous[0]) != 2:
                raise RuntimeError(
                    "capture restart FULL synchronous mode is unavailable"
                )
            result = connection.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            if (
                not isinstance(result, tuple)
                or len(result) != 3
                or any(type(value) is not int for value in result)
                or any(value < 0 for value in result)
            ):
                raise RuntimeError("capture WAL checkpoint result is invalid")
            if result[0] != 0:
                raise RuntimeError("capture WAL checkpoint remained busy")
            if result[1:] != (0, 0):
                raise RuntimeError("capture WAL checkpoint result is invalid")
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            if connection is not None:
                try:
                    connection.close()
                except BaseException as close_error:
                    if primary_error is None:
                        raise
                    try:
                        primary_error.add_note(
                            "restart WAL connection close failed: "
                            f"{type(close_error).__name__}: {close_error}"
                        )
                    except BaseException:
                        pass
                else:
                    with self._lock:
                        self._discard_setup_connection(connection)
        for sidecar, label in ((wal, "WAL"), (shm, "SHM")):
            try:
                sidecar_stat = sidecar.lstat()
            except FileNotFoundError:
                continue
            if self._stat_is_linklike(sidecar_stat) or not stat.S_ISREG(
                sidecar_stat.st_mode
            ):
                raise RuntimeError(
                    f"capture {label} is not a regular file"
                )
            if sidecar_stat.st_size:
                raise RuntimeError(
                    f"capture {label} checkpoint did not quiesce"
                )

    @contextmanager
    def _partition_guards(self, days) -> object:
        """Acquire multiple day guards in deterministic deadlock-safe order."""
        with ExitStack() as stack:
            for day in sorted(set(days)):
                stack.enter_context(self.partition_guard(day))
            yield

    def _storage_artifacts(self):
        """Yield a bounded one-level scan without traversing linked parents."""
        parent_count = 0
        artifact_count = 0
        try:
            parents = self.root.iterdir()
            for parent in parents:
                parent_count += 1
                if parent_count > _CAPTURE_STORAGE_MAX_PARENTS:
                    raise RuntimeError(
                        "venue capture storage parent limit exceeded"
                    )
                try:
                    parent_stat = parent.lstat()
                except OSError as exc:
                    raise RuntimeError(
                        "venue capture storage parent unavailable"
                    ) from exc
                if self._stat_is_linklike(parent_stat):
                    raise RuntimeError(
                        "venue capture storage parent is linked"
                    )
                if not stat.S_ISDIR(parent_stat.st_mode):
                    continue
                self._assert_scoped_path(parent)
                try:
                    for item in parent.iterdir():
                        artifact_count += 1
                        if artifact_count > _CAPTURE_STORAGE_MAX_ARTIFACTS:
                            raise RuntimeError(
                                "venue capture storage artifact limit exceeded"
                            )
                        # ``iterdir`` produced this direct child of an already
                        # scoped, non-link parent. Its authoritative lstat
                        # belongs to the caller.
                        yield item
                except OSError as exc:
                    raise RuntimeError(
                        "venue capture storage directory unavailable"
                    ) from exc
        except FileNotFoundError:
            return
        except RuntimeError:
            raise
        except OSError as exc:
            raise RuntimeError(
                "venue capture storage tree unavailable"
            ) from exc

    def _retention_guard_days(self, *, current: datetime) -> set:
        """Snapshot old UTC days retention is allowed to mutate this pass."""
        with self._lock:
            paths = list(self._connections)
        paths.extend(
            item
            for item in self._storage_artifacts()
            if (
                item.parent.name in _CAPTURE_PARTITION_STREAMS
                and ".sqlite3" in item.name
            )
            or (
                item.parent.name == "integrity"
                and item.suffix == ".json"
            )
        )
        days = set()
        for path in paths:
            name = (
                path.name.split(".sqlite3", 1)[0]
                if ".sqlite3" in path.name
                else path.stem
            )
            try:
                day = datetime.strptime(name, "%Y-%m-%d").date()
            except ValueError:
                continue
            if day.isoformat() != name:
                continue
            if day < current.date():
                days.add(day)
        return days

    def _assert_partition_writable(self, path: Path) -> None:
        seal = self.root / "integrity" / f"{path.stem}.json"
        self._assert_scoped_path(seal)
        if seal.exists():
            raise SealedCapturePartitionError(
                f"sealed capture partition is immutable: {path.stem}"
            )

    @staticmethod
    def _stat_is_linklike(value) -> bool:
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        return bool(
            stat.S_ISLNK(value.st_mode)
            or (
                reparse_flag
                and getattr(value, "st_file_attributes", 0) & reparse_flag
            )
        )

    @classmethod
    def _linklike(cls, path: Path) -> bool:
        try:
            value = path.lstat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise RuntimeError("capture path cannot be inspected") from exc
        return cls._stat_is_linklike(value)

    def _assert_scoped_path(self, path: Path) -> None:
        root_absolute = self.root.absolute()
        path_absolute = path.absolute()
        try:
            relative = path_absolute.relative_to(root_absolute)
        except ValueError as exc:
            raise RuntimeError("capture path escaped root") from exc
        if self._linklike(self.root):
            raise RuntimeError("capture root is linked")
        if self._linklike(path.parent) or self._linklike(path):
            raise RuntimeError("capture path is linked")
        try:
            if path.lstat().st_nlink != 1:
                raise RuntimeError("capture path is hard-linked")
        except FileNotFoundError:
            pass
        except RuntimeError:
            raise
        except OSError as exc:
            raise RuntimeError("capture path cannot be inspected") from exc
        try:
            resolved_root = self.root.resolve()
            resolved_path = path.resolve(strict=False)
        except OSError as exc:
            raise RuntimeError("capture path cannot be resolved") from exc
        if resolved_path != resolved_root / relative:
            raise RuntimeError("capture path escaped root through a link")

    def _load_control_state_locked(self) -> dict:
        try:
            with self._control_path.open("rb") as handle:
                raw = handle.read(_CAPTURE_CONTROL_JSON_MAX_BYTES + 1)
        except FileNotFoundError:
            return {"schema_version": 1, "connection_epoch": 0}
        if len(raw) > _CAPTURE_CONTROL_JSON_MAX_BYTES:
            raise RuntimeError("capture control state exceeds size limit")
        try:
            value = json.loads(
                raw.decode("utf-8-sig"),
                object_pairs_hook=_unique_capture_control_object,
                parse_constant=_reject_capture_control_constant,
            )
        except (ValueError, UnicodeError) as exc:
            raise RuntimeError("capture control state is invalid") from exc
        if (
            not isinstance(value, dict)
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != 1
            or frozenset(value) not in (
                _CAPTURE_CONTROL_FIELDS,
                _CAPTURE_CONTROL_DURABLE_FIELDS,
            )
        ):
            raise RuntimeError("capture control state schema is invalid")
        epoch = value.get("connection_epoch")
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
            raise RuntimeError("capture connection epoch is invalid")
        return dict(value)

    def _write_control_state_locked(self, state: dict) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._assert_scoped_path(self._control_path)
        encoded = json.dumps(
            state,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        temporary: Path | None = None
        primary_error: BaseException | None = None
        temporary_owned = False
        temporary_identity: tuple[int, int] | None = None
        handle = None
        try:
            for attempt in range(_CAPTURE_CONTROL_TEMP_ATTEMPTS):
                candidate = self.root / (
                    f".{self._control_path.name}.{os.getpid()}."
                    f"{threading.get_ident()}.{uuid.uuid4().hex}.tmp"
                )
                self._assert_scoped_path(candidate)
                try:
                    handle = candidate.open("xb")
                except FileExistsError:
                    if attempt + 1 == _CAPTURE_CONTROL_TEMP_ATTEMPTS:
                        raise
                    continue
                temporary = candidate
                temporary_owned = True
                break
            if handle is None or temporary is None:
                raise RuntimeError("capture control temporary allocation failed")
            try:
                temporary_stat = os.fstat(handle.fileno())
                temporary_identity = (
                    temporary_stat.st_dev,
                    temporary_stat.st_ino,
                )
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
                            "capture control temporary close failed: "
                            f"{type(close_error).__name__}: {close_error}"
                        )
                    except BaseException:
                        pass
            os.replace(temporary, self._control_path)
            temporary_owned = False
            _fsync_directory(self.root)
        except BaseException as exc:
            if primary_error is None:
                primary_error = exc
            raise
        finally:
            cleanup_error: BaseException | None = None
            same_generation = False
            if temporary_owned and temporary_identity is not None:
                assert temporary is not None
                try:
                    current = temporary.stat(follow_symlinks=False)
                    same_generation = (
                        stat.S_ISREG(current.st_mode)
                        and not self._stat_is_linklike(current)
                        and (current.st_dev, current.st_ino)
                        == temporary_identity
                    )
                except FileNotFoundError:
                    pass
                except BaseException as identity_error:
                    cleanup_error = identity_error
            if same_generation:
                assert temporary is not None
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
                except BaseException as unlink_error:
                    cleanup_error = unlink_error
            if cleanup_error is not None:
                if primary_error is None:
                    raise cleanup_error
                try:
                    primary_error.add_note(
                        "capture control temporary cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
                except BaseException:
                    pass

    def next_connection_epoch(self) -> int:
        """Return a process-independent monotonic transport epoch."""
        _mkdir_with_parent_fsync(self.root)
        lock_path = self.root / ".capture_state.lock"
        self._assert_scoped_path(self._control_path)
        self._assert_scoped_path(lock_path)
        with self._control_lock:
            with portalocker.Lock(
                str(lock_path),
                mode="a",
                timeout=15.0,
                check_interval=0.05,
                fail_when_locked=False,
            ):
                state = self._load_control_state_locked()
                epoch = int(state["connection_epoch"]) + 1
                state["connection_epoch"] = epoch
                state["updated_at"] = (
                    _capture_now_utc().isoformat().replace("+00:00", "Z")
                )
                self._write_control_state_locked(state)
                return epoch

    @staticmethod
    def _parse_event_time(value) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            text = value.strip()
            if not text:
                return None
            parsed = datetime.fromisoformat(
                text[:-1] + "+00:00" if text.endswith("Z") else text
            )
            if parsed.tzinfo is None:
                return None
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _canonical_utc(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")

    @classmethod
    def _storage_exchange_time(cls, event: VenueEvent) -> tuple[str, bool]:
        exchange_time = cls._parse_event_time(event.exchange_time)
        received_time = cls._parse_event_time(event.received_time)
        needs_fallback = exchange_time is None or (
            received_time is not None
            and exchange_time - received_time > timedelta(seconds=30)
        )
        if needs_fallback and received_time is not None:
            return cls._canonical_utc(received_time), True
        if exchange_time is not None:
            return cls._canonical_utc(exchange_time), needs_fallback
        return str.__str__(event.exchange_time), False

    @classmethod
    def _day(cls, event: VenueEvent) -> str:
        storage_time, _fallback = cls._storage_exchange_time(event)
        parsed = cls._parse_event_time(storage_time)
        return parsed.strftime("%Y-%m-%d") if parsed is not None else "unknown-date"

    def _path(self, event: VenueEvent) -> Path:
        return self.root / event.kind / f"{self._day(event)}.sqlite3"

    @staticmethod
    def _valid_text(value, *, max_chars: int) -> bool:
        if not isinstance(value, str):
            return False
        try:
            str.encode(value, "utf-8")
            return bool(
                value
                and value == value.strip()
                and len(value) <= max_chars
                and all(ord(char) >= 32 and ord(char) != 127 for char in value)
            )
        except Exception:
            return False

    @classmethod
    def _validate_event(cls, event: VenueEvent) -> None:
        """Reject malformed capture evidence before selecting a path."""
        if not isinstance(event, VenueEvent):
            raise ValueError("venue event type is invalid")
        if not cls._valid_text(event.event_id, max_chars=2048):
            raise ValueError("venue event id is invalid")
        try:
            valid_kind = (
                cls._valid_text(event.kind, max_chars=32)
                and event.kind[0] in "abcdefghijklmnopqrstuvwxyz"
                and all(
                    char in "abcdefghijklmnopqrstuvwxyz0123456789_"
                    for char in event.kind
                )
                and event.kind in _CAPTURE_PARTITION_STREAMS
            )
        except Exception:
            valid_kind = False
        if not valid_kind:
            raise ValueError("venue event kind is invalid")
        if not cls._valid_text(event.market_id, max_chars=256):
            raise ValueError("venue event market id is invalid")
        # Unusable clocks are retained in the explicit unknown-date quarantine
        # (or exchange_time falls back to a usable receiving clock). They still
        # have to be bounded text so serialization and paths remain safe.
        if not cls._valid_text(event.exchange_time, max_chars=64):
            raise ValueError("venue event exchange_time is invalid")
        if not cls._valid_text(event.received_time, max_chars=64):
            raise ValueError("venue event received_time is invalid")
        if type(event.schema_version) is not int or event.schema_version != 1:
            raise ValueError("venue event schema version is invalid")
        if not isinstance(event.payload, dict):
            raise ValueError("venue event payload is invalid")
        if not isinstance(event.quality_flags, tuple):
            raise ValueError("venue event quality flags are invalid")
        try:
            valid_quality_flags = (
                len(event.quality_flags) <= 64
                and all(
                    cls._valid_text(flag, max_chars=128)
                    for flag in event.quality_flags
                )
                and len(set(event.quality_flags)) == len(event.quality_flags)
            )
        except Exception:
            valid_quality_flags = False
        if not valid_quality_flags:
            raise ValueError("venue event quality flags are invalid")

    def _connection(self, path: Path) -> sqlite3.Connection:
        self._assert_scoped_path(path)
        connection = self._connections.get(path)
        if connection is not None:
            return connection
        path.parent.mkdir(parents=True, exist_ok=True)
        self._assert_scoped_path(path)
        # Writes are serialized by ``_lock`` but the recorder's REST and L2
        # workers legitimately share this connection across two threads.
        connection = sqlite3.connect(path, timeout=15.0, check_same_thread=False)
        self._setup_connection_candidate = connection
        try:
            self._setup_connections.append(connection)
        except BaseException as primary_exc:
            try:
                connection.close()
            except BaseException as cleanup_exc:
                try:
                    primary_exc.add_note(
                        "SQLite candidate cleanup failed: "
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                    )
                except BaseException:
                    pass
            else:
                self._discard_setup_connection(connection)
            raise
        self._setup_connection_candidate = None
        try:
            journal_mode = connection.execute(
                "PRAGMA journal_mode=WAL"
            ).fetchone()
            if (
                journal_mode is None
                or str(journal_mode[0]).strip().lower() != "wal"
            ):
                raise RuntimeError("capture WAL mode is unavailable")
            # Capture evidence favours durability over peak insert throughput.
            # In WAL mode FULL syncs the WAL before a commit is acknowledged,
            # so abrupt host/power loss cannot silently discard an already
            # reported successful sample.
            connection.execute("PRAGMA synchronous=FULL")
            synchronous = connection.execute(
                "PRAGMA synchronous"
            ).fetchone()
            if synchronous is None or int(synchronous[0]) != 2:
                raise RuntimeError(
                    "capture FULL synchronous mode is unavailable"
                )
            connection.execute("PRAGMA busy_timeout=15000")
            busy_timeout = connection.execute(
                "PRAGMA busy_timeout"
            ).fetchone()
            if busy_timeout is None or int(busy_timeout[0]) != 15_000:
                raise RuntimeError("capture busy timeout is unavailable")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS venue_events (
                       event_id TEXT PRIMARY KEY,
                       market_id TEXT NOT NULL,
                       exchange_time TEXT NOT NULL,
                       received_time TEXT NOT NULL,
                       schema_version INTEGER NOT NULL,
                       quality_flags_json TEXT NOT NULL,
                       payload_json TEXT NOT NULL
                   ) WITHOUT ROWID"""
            )
            table_info = connection.execute(
                "PRAGMA table_xinfo(venue_events)"
            ).fetchall()
            schema = tuple(
                (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
                for row in table_info
            )
            if schema != _CAPTURE_EVENT_SCHEMA:
                raise RuntimeError("capture partition schema is invalid")
            trigger = connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='trigger' AND tbl_name='venue_events' LIMIT 1"
            ).fetchone()
            if trigger is not None:
                raise RuntimeError("capture partition trigger is invalid")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_venue_events_time "
                "ON venue_events(exchange_time, market_id)"
            )
            index_rows = tuple(
                row
                for row in connection.execute(
                    "PRAGMA index_list(venue_events)"
                ).fetchall()
                if str(row[1]) == "idx_venue_events_time"
            )
            index_xinfo = tuple(
                (str(row[2]), int(row[3]), str(row[4]), int(row[5]))
                for row in connection.execute(
                    "PRAGMA index_xinfo(idx_venue_events_time)"
                ).fetchall()
            )
            if (
                len(index_rows) != 1
                or int(index_rows[0][2]) != 0
                or str(index_rows[0][3]) != "c"
                or int(index_rows[0][4]) != 0
                or index_xinfo != _CAPTURE_EVENT_TIME_INDEX_XINFO
            ):
                raise RuntimeError("capture partition index is invalid")
            schema_objects = tuple(
                (str(row[0]), str(row[1]), str(row[2]))
                for row in connection.execute(
                    "SELECT type,name,tbl_name FROM sqlite_master "
                    "ORDER BY type,name"
                ).fetchall()
            )
            if path.parent.name == "trades":
                version = validate_trade_identity_schema(connection)
            else:
                user_version = connection.execute("PRAGMA user_version").fetchone()
                if user_version != (0,):
                    raise RuntimeError("capture partition version is invalid")
                version = 0
            expected_objects = _CAPTURE_SCHEMA_OBJECTS + (
                (("table", "websocket_trade_identities", "websocket_trade_identities"),)
                if version == 2 else ()
            )
            if schema_objects != tuple(sorted(expected_objects)):
                raise RuntimeError(
                    "capture partition schema objects are invalid"
                )
            connection.commit()
            if path.parent.name == "trades" and version == 0:
                self._migrate_trade_identities(connection, path)
            elif path.parent.name == "trades":
                self._verify_trade_identity_partition(connection, path)
            self._connections[path] = connection
        except BaseException as primary_exc:
            if self._connections.get(path) is connection:
                self._connections.pop(path, None)
            try:
                connection.close()
            except BaseException as cleanup_exc:
                try:
                    primary_exc.add_note(
                        "SQLite candidate cleanup failed: "
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                    )
                except BaseException:
                    pass
            else:
                self._discard_setup_connection(connection)
            raise
        self._discard_setup_connection(connection)
        return connection

    def _migrate_trade_identities(self, connection: sqlite3.Connection, path: Path) -> None:
        """Upgrade an unsealed legacy day, rejecting any existing duplicates."""
        from datetime import date

        self._assert_partition_writable(path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            if validate_trade_identity_schema(connection) != 0:
                raise RuntimeError("capture trade identity version changed")
            connection.execute(
                """CREATE TABLE websocket_trade_identities (
                       market_id TEXT NOT NULL,
                       trade_id TEXT NOT NULL,
                       trade_time INTEGER NOT NULL,
                       trade_price REAL NOT NULL,
                       trade_amount REAL NOT NULL,
                       trade_side TEXT NOT NULL,
                       event_id TEXT NOT NULL,
                       PRIMARY KEY (market_id, trade_id)
                   ) WITHOUT ROWID"""
            )
            cursor = connection.execute(
                "SELECT event_id,market_id,exchange_time,received_time,"
                "length(CAST(payload_json AS BLOB)),"
                "CASE WHEN length(CAST(payload_json AS BLOB))<=? "
                "THEN payload_json ELSE NULL END "
                "FROM venue_events ORDER BY event_id",
                (_CAPTURE_EVENT_PAYLOAD_MAX_BYTES,),
            )
            day = date.fromisoformat(path.stem)
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                event_id, market_id, exchange_time, received_time, size, raw = row
                if size > _CAPTURE_EVENT_PAYLOAD_MAX_BYTES:
                    raise RuntimeError("legacy trade payload exceeds size limit")
                payload = _strict_capture_payload(raw)
                if payload.get("stream_source") != "ccxt_pro":
                    continue
                if "writer_input_sha256" in payload or "writer_input_exchange_time" in payload:
                    raise RuntimeError("legacy trade payload has unexpected writer markers")
                trades = validate_ws_trade_payload(
                    payload, exchange_time=exchange_time,
                    received_time=received_time, day=day,
                )
                for trade_id, trade_time, price, amount, side in trades:
                    connection.execute(
                        "INSERT INTO main.websocket_trade_identities VALUES (?,?,?,?,?,?,?)",
                        (market_id, trade_id, trade_time, price, amount, side, event_id),
                    )
            connection.execute("PRAGMA user_version=2")
            if validate_trade_identity_schema(connection) != 2:
                raise RuntimeError("trade identity migration failed")
            self._assert_partition_writable(path)
            connection.commit()
        except Exception as primary_exc:
            try:
                connection.rollback()
            except BaseException as rollback_exc:
                try:
                    primary_exc.add_note(
                        "trade identity migration rollback failed: "
                        f"{type(rollback_exc).__name__}: {rollback_exc}"
                    )
                except BaseException:
                    pass
            raise

    def _verify_trade_identity_partition(self, connection, path: Path) -> None:
        """Refuse incomplete or orphaned V2 ledgers on each connection open."""
        from datetime import date

        day = date.fromisoformat(path.stem)
        count = 0
        cursor = connection.execute(
            "SELECT event_id,market_id,exchange_time,received_time,"
            "length(CAST(payload_json AS BLOB)),"
            "CASE WHEN length(CAST(payload_json AS BLOB))<=? "
            "THEN payload_json ELSE NULL END "
            "FROM venue_events ORDER BY event_id",
            (_CAPTURE_EVENT_PAYLOAD_MAX_BYTES,),
        )
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            event_id, market_id, exchange_time, received_time, size, raw = row
            if size > _CAPTURE_EVENT_PAYLOAD_MAX_BYTES or raw is None:
                raise RuntimeError("trade partition event payload is oversized")
            try:
                payload = _strict_capture_payload(raw)
            except (ValueError, TypeError) as exc:
                raise RuntimeError("trade partition event payload is invalid") from exc
            if not isinstance(payload, dict):
                raise RuntimeError("trade partition event payload is invalid")
            if payload.get("stream_source") != "ccxt_pro":
                continue
            try:
                identities = validate_ws_trade_payload(
                    payload, exchange_time=exchange_time,
                    received_time=received_time, day=day,
                )
            except ValueError as exc:
                raise RuntimeError("trade partition websocket evidence is invalid") from exc
            for trade_id, trade_time, price, amount, side in identities:
                if read_trade_identity(connection, market_id, trade_id) != (
                    trade_time, price, amount, side, event_id
                ):
                    raise RuntimeError("trade partition identity ledger mismatch")
                count += 1
        ledger_count = connection.execute(
            "SELECT count(*) FROM main.websocket_trade_identities"
        ).fetchone()
        if ledger_count != (count,):
            raise RuntimeError("trade partition identity ledger count mismatch")

    def _discard_setup_connection(self, connection) -> None:
        pending = getattr(self, "_setup_connections", None)
        if pending is not None:
            for index in range(len(pending) - 1, -1, -1):
                if pending[index] is connection:
                    pending.pop(index)
        if getattr(self, "_setup_connection_candidate", None) is connection:
            self._setup_connection_candidate = None

    @classmethod
    def _persisted_values_match(cls, existing: tuple, values: tuple) -> bool:
        if existing == values:
            return True
        existing_time = cls._parse_event_time(existing[2])
        requested_time = cls._parse_event_time(values[2])
        return bool(
            existing_time is not None
            and requested_time is not None
            and existing_time == requested_time
            and existing[:2] == values[:2]
            and existing[3:] == values[3:]
        )

    def write(self, event: VenueEvent) -> Path:
        self._validate_event(event)
        try:
            payload = json.dumps(
                event.payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except Exception as exc:
            raise ValueError("venue event payload is invalid") from exc
        if len(payload.encode("utf-8")) > _CAPTURE_EVENT_PAYLOAD_MAX_BYTES:
            raise ValueError("venue event payload exceeds size limit")
        event = replace(event, payload=json.loads(payload))
        self._validate_event(event)
        ws_trade = event.kind == "trades" and event.payload.get("stream_source") == "ccxt_pro"
        if (
            "writer_input_sha256" in event.payload
            or "writer_input_exchange_time" in event.payload
        ):
            raise TradeIdentityConflict("writer input markers are reserved")
        path = self._path(event)
        self._assert_scoped_path(path)
        exchange_time, clock_fallback = self._storage_exchange_time(event)
        quality_flags = tuple(event.quality_flags)
        if (
            clock_fallback
            and "storage_exchange_time_fallback" not in quality_flags
        ):
            quality_flags = (*quality_flags, "storage_exchange_time_fallback")
        if len(quality_flags) > 64:
            raise ValueError("venue event quality flags are invalid")
        flags = json.dumps(quality_flags, separators=(",", ":"))
        values = (
            event.event_id,
            event.market_id,
            exchange_time,
            event.received_time,
            int(event.schema_version),
            flags,
            payload,
        )
        with self.partition_guard(path.stem):
            with self._lock:
                if self._write_terminal:
                    raise RuntimeError("partition writer is closed")
                self._assert_partition_writable(path)
                connection = self._connection(path)
                try:
                    if ws_trade:
                        connection.execute("BEGIN IMMEDIATE")
                        values, new_trades = self._prepare_ws_trade_write(
                            connection, event, values, path
                        )
                        if values is None:
                            self._assert_partition_writable(path)
                            connection.commit()
                            return path
                    cursor = connection.execute(
                        """INSERT OR IGNORE INTO venue_events
                           (event_id, market_id, exchange_time, received_time,
                            schema_version, quality_flags_json, payload_json)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        values,
                    )
                    if cursor.rowcount == 0:
                        existing = connection.execute(
                            """SELECT event_id, market_id, exchange_time,
                                      received_time, schema_version,
                                      quality_flags_json, payload_json
                                 FROM venue_events WHERE event_id=?""",
                            (event.event_id,),
                        ).fetchone()
                        if existing is None or not self._persisted_values_match(
                            tuple(existing), values
                        ):
                            raise ValueError(
                                "venue event id conflicts with persisted evidence"
                            )
                    elif ws_trade:
                        for trade in new_trades:
                            trade_id, trade_time, price, amount, side = trade
                            connection.execute(
                                "INSERT INTO main.websocket_trade_identities VALUES (?,?,?,?,?,?,?)",
                                (event.market_id, trade_id, trade_time, price,
                                 amount, side, event.event_id),
                            )
                    self._assert_partition_writable(path)
                    connection.commit()
                except Exception as primary_exc:
                    try:
                        connection.rollback()
                    except BaseException as rollback_exc:
                        # A failed rollback proves that this handle is not a
                        # safe transaction boundary anymore. Never mask the
                        # original write error and never reuse the handle on
                        # the next capture event.
                        try:
                            primary_exc.add_note(
                                "SQLite rollback failed: "
                                f"{type(rollback_exc).__name__}: {rollback_exc}"
                            )
                        except BaseException:
                            pass
                        self._setup_connection_candidate = connection
                        try:
                            if not any(
                                candidate is connection
                                for candidate in self._setup_connections
                            ):
                                self._setup_connections.append(connection)
                        except BaseException as quarantine_exc:
                            try:
                                primary_exc.add_note(
                                    "SQLite quarantine publication failed: "
                                    f"{type(quarantine_exc).__name__}: "
                                    f"{quarantine_exc}"
                                )
                            except BaseException:
                                pass
                        else:
                            self._setup_connection_candidate = None
                        if self._connections.get(path) is connection:
                            self._connections.pop(path, None)
                        try:
                            connection.close()
                        except BaseException as cleanup_exc:
                            try:
                                primary_exc.add_note(
                                    "SQLite write cleanup failed: "
                                    f"{type(cleanup_exc).__name__}: "
                                    f"{cleanup_exc}"
                                )
                            except BaseException:
                                pass
                        else:
                            self._discard_setup_connection(connection)
                    raise
            return path

    def _prepare_ws_trade_write(self, connection, event, values, path):
        from datetime import date

        try:
            identities = validate_ws_trade_payload(
                event.payload, exchange_time=values[2],
                received_time=event.received_time,
                day=date.fromisoformat(path.stem),
            )
        except ValueError as exc:
            raise TradeIdentityConflict("websocket trade input is invalid") from exc
        digest = canonical_ws_writer_input_hash(event)
        previous = connection.execute(
            "SELECT event_id,market_id,exchange_time,received_time,schema_version,"
            "quality_flags_json,payload_json FROM venue_events WHERE event_id=?",
            (event.event_id,),
        ).fetchone()
        if previous is not None:
            try:
                stored_payload = _strict_capture_payload(previous[6])
            except ValueError as exc:
                raise RuntimeError("websocket stored event payload is invalid") from exc
            if "writer_input_sha256" in stored_payload:
                if (
                    stored_payload.get("writer_input_sha256") != digest
                ):
                    raise TradeIdentityConflict("websocket trade event id conflicts with persisted input")
                if (
                    self._parse_event_time(stored_payload.get("writer_input_exchange_time"))
                    != self._parse_event_time(event.exchange_time)
                    or previous[1] != event.market_id
                    or previous[3] != values[3]
                    or previous[4] != values[4]
                    or previous[5] != values[5]
                ):
                    raise RuntimeError("websocket stored event provenance changed")
                stored_base = {
                    key: value for key, value in stored_payload.items()
                    if key not in ("trades", "writer_input_sha256", "writer_input_exchange_time")
                }
                input_base = {
                    key: value for key, value in event.payload.items()
                    if key != "trades"
                }
                def canonical(value):
                    return json.dumps(
                        value, sort_keys=True, separators=(",", ":"),
                        allow_nan=False,
                    )
                if canonical(stored_base) != canonical(input_base):
                    raise RuntimeError("websocket stored event context changed")
            elif not self._persisted_values_match(tuple(previous), values):
                raise TradeIdentityConflict("legacy websocket event id conflicts with persisted input")
            try:
                stored_identities = validate_ws_trade_payload(
                    stored_payload, exchange_time=previous[2],
                    received_time=previous[3], day=date.fromisoformat(path.stem),
                )
            except ValueError as exc:
                raise RuntimeError("websocket stored event trades are invalid") from exc
            input_trades = {row["id"]: row for row in event.payload["trades"]}
            if "writer_input_sha256" in stored_payload:
                for stored_trade in stored_payload["trades"]:
                    original = input_trades.get(stored_trade["id"])
                    if original is None or canonical(stored_trade) != canonical(original):
                        raise RuntimeError("websocket stored trade subset changed")
            for stored in stored_identities:
                if read_trade_identity(connection, event.market_id, stored[0]) != (
                    *stored[1:], event.event_id
                ):
                    raise RuntimeError("websocket event ledger linkage is invalid")
            if "writer_input_sha256" in stored_payload:
                original_owners = {}
                for trade_id, trade_time, price, amount, side in identities:
                    evidence = (trade_id, trade_time, price, amount, side)
                    prior = read_trade_identity(connection, event.market_id, trade_id)
                    if prior is None or prior[:4] != evidence[1:]:
                        raise RuntimeError("websocket original input identity is missing")
                    original_owners.setdefault(prior[4], []).append(evidence)
                self._verify_ws_trade_owners(
                    connection, event.market_id, original_owners, path
                )
            return None, ()
        new = []
        new_rows = []
        duplicate_owners = {}
        for trade, evidence in zip(event.payload["trades"], identities):
            trade_id = evidence[0]
            prior = read_trade_identity(connection, event.market_id, trade_id)
            if prior is None:
                new.append(evidence)
                new_rows.append(trade)
            elif prior[:4] != evidence[1:]:
                raise TradeIdentityConflict("websocket trade identity conflicts with persisted evidence")
            else:
                duplicate_owners.setdefault(prior[4], []).append(evidence)
        self._verify_ws_trade_owners(
            connection, event.market_id, duplicate_owners, path
        )
        if not new:
            if event.quality_flags:
                raise TradeIdentityConflict("websocket duplicate update would lose quality evidence")
            return None, ()
        out_payload = dict(event.payload)
        out_payload["trades"] = new_rows
        out_payload["writer_input_sha256"] = digest
        out_payload["writer_input_exchange_time"] = self._canonical_utc(
            self._parse_event_time(event.exchange_time)
        )
        latest_ms = new[-1][1]
        storage_time = self._canonical_utc(
            datetime.fromtimestamp(latest_ms / 1000, timezone.utc)
        )
        output_payload = json.dumps(out_payload, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False)
        if len(output_payload.encode("utf-8")) > _CAPTURE_EVENT_PAYLOAD_MAX_BYTES:
            raise TradeIdentityConflict("websocket trade output exceeds size limit")
        output = (
            values[0], values[1], storage_time, values[3], values[4], values[5],
            output_payload,
        )
        return output, tuple(new)

    def _verify_ws_trade_owners(self, connection, market_id, owners, path):
        from datetime import date

        for owner, claimed in owners.items():
            owner_row = connection.execute(
                "SELECT market_id,exchange_time,received_time,"
                "CASE WHEN length(CAST(payload_json AS BLOB))<=? "
                "THEN payload_json ELSE NULL END "
                "FROM venue_events WHERE event_id=?",
                (_CAPTURE_EVENT_PAYLOAD_MAX_BYTES, owner),
            ).fetchone()
            if owner_row is None or owner_row[3] is None:
                raise RuntimeError("websocket trade identity owner is missing")
            try:
                owner_trades = validate_ws_trade_payload(
                    _strict_capture_payload(owner_row[3]), exchange_time=owner_row[1],
                    received_time=owner_row[2], day=date.fromisoformat(path.stem),
                )
            except (ValueError, TypeError) as exc:
                raise RuntimeError("websocket trade identity owner is invalid") from exc
            if owner_row[0] != market_id or not set(claimed).issubset(owner_trades):
                raise RuntimeError("websocket trade identity owner evidence is invalid")

    def _close_path(self, path: Path) -> None:
        connection = self._connections.get(path)
        if connection is not None:
            connection.close()
            if self._connections.get(path) is connection:
                self._connections.pop(path, None)

    @staticmethod
    def _partition_date(path: Path) -> datetime | None:
        try:
            return datetime.strptime(path.stem, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return None

    def _delete_partition(self, path: Path) -> None:
        # Never unlink a partition whose live handle could not be closed.
        # Retention will retry the registered handle on its next pass.
        self._close_path(path)
        first_error: OSError | sqlite3.Error | None = None
        for candidate in (
            path,
            Path(f"{path}-wal"),
            Path(f"{path}-shm"),
        ):
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def _retention_staging_dirs(self) -> dict[object, Path]:
        """Discover canonical crash-recovery staging directories."""
        prefix = ".retention_trash-"
        try:
            entries = tuple(self.root.iterdir())
        except FileNotFoundError:
            return {}
        except OSError as exc:
            raise RuntimeError(
                "capture retention staging discovery failed"
            ) from exc
        staging_by_day = {}
        for path in entries:
            if not path.name.startswith(prefix):
                continue
            self._assert_scoped_path(path)
            try:
                value = path.lstat()
            except OSError as exc:
                raise RuntimeError(
                    "capture retention staging cannot be inspected"
                ) from exc
            if self._stat_is_linklike(value) or not stat.S_ISDIR(value.st_mode):
                raise RuntimeError(
                    "capture retention staging is not a plain directory"
                )
            day_text = path.name[len(prefix):]
            try:
                day = datetime.strptime(day_text, "%Y-%m-%d").date()
            except ValueError as exc:
                raise RuntimeError(
                    "capture retention staging day is invalid"
                ) from exc
            if day.isoformat() != day_text:
                raise RuntimeError(
                    "capture retention staging day is invalid"
                )
            if day in staging_by_day:
                raise RuntimeError(
                    "duplicate capture retention staging day"
                )
            staging_by_day[day] = path
        return staging_by_day

    def _cleanup_retention_staging(self, day, staging: Path) -> None:
        """Finish deletion of one previously staged expired UTC day."""
        self._assert_scoped_path(staging)
        allowed = {f"integrity__{day.isoformat()}.json"}
        for stream in ("overview", "depth", "trades", "l2_stream"):
            base = f"{stream}__{day.isoformat()}.sqlite3"
            allowed.update((base, f"{base}-wal", f"{base}-shm"))
        try:
            items = tuple(sorted(staging.iterdir()))
        except OSError as exc:
            raise RuntimeError(
                "capture retention staging cannot be enumerated"
            ) from exc
        # Validate the complete generation before deleting any part of it.
        for item in items:
            self._assert_scoped_path(item)
            try:
                item_stat = item.lstat()
            except OSError as exc:
                raise RuntimeError(
                    "capture retention staged artifact unavailable"
                ) from exc
            if (
                item.name not in allowed
                or self._stat_is_linklike(item_stat)
                or not stat.S_ISREG(item_stat.st_mode)
            ):
                raise RuntimeError(
                    "capture retention staging contains an unexpected artifact"
                )
        first_error = None
        for item in items:
            try:
                item.unlink()
            except OSError as exc:
                if first_error is None:
                    first_error = exc
        try:
            staging.rmdir()
        except OSError as exc:
            if first_error is None:
                first_error = exc
        if first_error is not None:
            raise first_error

    def _stage_delete_day(self, day, paths: list[Path]) -> None:
        """Remove one expired UTC dataset without leaving active fragments."""
        staging = self.root / f".retention_trash-{day.isoformat()}"
        self._assert_scoped_path(staging)
        for path in paths:
            self._assert_scoped_path(path)
        if staging.exists():
            self._cleanup_retention_staging(day, staging)
        staging.mkdir(parents=False, exist_ok=False)
        moved: list[tuple[Path, Path]] = []
        try:
            for path in paths:
                for source in (
                    path,
                    Path(f"{path}-wal"),
                    Path(f"{path}-shm"),
                ):
                    self._assert_scoped_path(source)
                    if not source.exists():
                        continue
                    target = staging / f"{path.parent.name}__{source.name}"
                    source.replace(target)
                    moved.append((source, target))
        except (OSError, RuntimeError):
            rollback_error = None
            for source, target in reversed(moved):
                try:
                    target.replace(source)
                except OSError as exc:
                    if rollback_error is None:
                        rollback_error = exc
            try:
                staging.rmdir()
            except OSError as exc:
                if rollback_error is None:
                    rollback_error = exc
            if rollback_error is not None:
                raise RuntimeError(
                    "expired capture day staging rollback failed"
                ) from rollback_error
            raise

        first_error = None
        for _source, target in moved:
            try:
                target.unlink()
            except OSError as exc:
                if first_error is None:
                    first_error = exc
        try:
            staging.rmdir()
        except OSError as exc:
            if first_error is None:
                first_error = exc
        if first_error is not None:
            # The active capture tree is still coherent: every stream of the
            # expired day has moved out together. A later retention pass cleans
            # the private staging directory before doing more work.
            raise first_error

    def enforce_retention(self, *, now: datetime | None = None) -> None:
        with self._root_partition_guard():
            self._enforce_retention_locked(now=now)

    def _enforce_retention_locked(self, *, now: datetime | None = None) -> None:
        current = (now or _capture_now_utc()).astimezone(timezone.utc)
        cutoff = (current - timedelta(days=self.retention_days)).date()
        self._assert_scoped_path(self._control_path)
        staging_by_day = self._retention_staging_dirs()
        guard_days = (
            self._retention_guard_days(current=current)
            | set(staging_by_day)
        )
        with self._partition_guards(guard_days), self._lock:
            first_cleanup_error: OSError | sqlite3.Error | RuntimeError | None = None
            failed_cleanup_paths: set[Path] = set()
            failed_staging_days = set()

            # A crash or locked-file failure can leave a fully moved day only
            # in private staging.  Resume that generation even when no active
            # partition remains from which the normal scan could rediscover it.
            for day, staging in sorted(staging_by_day.items()):
                if day >= cutoff:
                    failed_staging_days.add(day)
                    if first_cleanup_error is None:
                        first_cleanup_error = RuntimeError(
                            "capture retention staging is inside the required "
                            "retention window"
                        )
                    continue
                try:
                    self._cleanup_retention_staging(day, staging)
                except (OSError, RuntimeError) as exc:
                    failed_staging_days.add(day)
                    if first_cleanup_error is None:
                        first_cleanup_error = exc

            # A daily partition is immutable after its UTC day in the normal
            # recorder flow. Close past-day handles even while the partition
            # remains inside retention, so SQLite can checkpoint its WAL and
            # the process does not accumulate one live connection per stream
            # per retained day. A genuinely late event can reopen the file;
            # the next hourly retention pass closes it again.
            for path in list(self._connections):
                partition_date = self._partition_date(path)
                if (
                    partition_date is None
                    or partition_date.date() >= current.date()
                    or partition_date.date() not in guard_days
                ):
                    continue
                try:
                    self._close_path(path)
                except (OSError, sqlite3.Error) as exc:
                    failed_cleanup_paths.add(path)
                    if first_cleanup_error is None:
                        first_cleanup_error = exc

            # A previous Windows cleanup may have removed the main database
            # while antivirus or another reader still held its WAL/SHM file.
            # Such sidecars are no longer reachable through the *.sqlite3
            # partition scan, so retry them explicitly when their base DB is
            # absent.
            orphan_sidecars = sorted({
                sidecar
                for stream in _CAPTURE_PARTITION_STREAMS
                for pattern in ("*.sqlite3-wal", "*.sqlite3-shm")
                for sidecar in (self.root / stream).glob(pattern)
            })
            for sidecar in orphan_sidecars:
                self._assert_scoped_path(sidecar)
                base_path = Path(str(sidecar)[:-4])
                if base_path.exists():
                    continue
                sidecar_date = self._partition_date(base_path)
                if (
                    sidecar_date is not None
                    and sidecar_date.date() < current.date()
                    and sidecar_date.date() not in guard_days
                ):
                    continue
                try:
                    sidecar.unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    if first_cleanup_error is None:
                        first_cleanup_error = exc

            partitions = sorted(
                partition
                for stream in _CAPTURE_PARTITION_STREAMS
                for partition in (self.root / stream).glob("*.sqlite3")
            )
            sealed_reports = sorted((self.root / "integrity").glob("*.json"))
            expired_by_day: dict[object, list[Path]] = {}
            for path in (*partitions, *sealed_reports):
                partition_date = self._partition_date(path)
                if (
                    partition_date is not None
                    and partition_date.date() < cutoff
                    and partition_date.date() in guard_days
                ):
                    expired_by_day.setdefault(partition_date.date(), []).append(path)
            # Retention is a dataset operation, not a file operation.  Remove
            # every available stream of an expired UTC day together; never
            # trim one stream from a still-required day merely to meet quota.
            for day in sorted(expired_by_day):
                paths = sorted(expired_by_day[day])
                if (
                    day in failed_staging_days
                    or any(path in failed_cleanup_paths for path in paths)
                ):
                    continue
                try:
                    self._stage_delete_day(day, paths)
                except (OSError, sqlite3.Error, RuntimeError) as exc:
                    failed_cleanup_paths.update(paths)
                    if first_cleanup_error is None:
                        first_cleanup_error = exc
            if self.max_storage_bytes <= 0:
                if first_cleanup_error is not None:
                    raise first_cleanup_error
                return
            def _storage_bytes() -> int:
                total = 0
                for item in self._storage_artifacts():
                    try:
                        item_stat = item.lstat()
                    except OSError as exc:
                        raise RuntimeError(
                            "venue capture storage size unavailable"
                        ) from exc
                    if self._stat_is_linklike(item_stat):
                        raise RuntimeError(
                            "venue capture storage artifact is linked"
                        )
                    if item_stat.st_nlink != 1:
                        raise RuntimeError(
                            "venue capture storage artifact is hard-linked"
                        )
                    if stat.S_ISREG(item_stat.st_mode):
                        total += item_stat.st_size
                return total
            total = _storage_bytes()
            if total > self.max_storage_bytes and first_cleanup_error is None:
                raise RuntimeError(
                    "venue capture storage cap cannot be met without deleting "
                    "the required retention window"
                )
            if first_cleanup_error is not None:
                raise first_cleanup_error

    def storage_health(self, *, now: datetime | None = None) -> dict:
        # Serialize the advisory snapshot with commits and retention so a
        # borderline capacity result cannot combine pre- and post-write file
        # sizes into a false-green projection.
        with self._lock:
            return self._storage_health_locked(now=now)

    def _storage_health_locked(self, *, now: datetime | None = None) -> dict:
        current = (now or _capture_now_utc()).astimezone(timezone.utc)
        daily_bytes: dict[str, int] = {}
        total = 0
        measurement_errors = 0
        try:
            for item in self._storage_artifacts():
                try:
                    item_stat = item.lstat()
                except OSError:
                    measurement_errors += 1
                    continue
                if (
                    self._stat_is_linklike(item_stat)
                    or item_stat.st_nlink != 1
                ):
                    measurement_errors += 1
                    continue
                if not stat.S_ISREG(item_stat.st_mode):
                    continue
                size = item_stat.st_size
                total += size
                name = item.name.split(".sqlite3", 1)[0]
                try:
                    day = datetime.strptime(name, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if day.isoformat() != name:
                    continue
                daily_bytes[name] = daily_bytes.get(name, 0) + size
        except RuntimeError:
            measurement_errors += 1
        closed = [
            size
            for day, size in daily_bytes.items()
            if day < current.date().isoformat()
        ]
        peak = max(closed, default=None)
        projected = (
            int(peak * (self.retention_days + 1) * 1.2)
            if peak is not None and len(closed) >= 3
            else None
        )
        filesystem_probe_error = ""
        try:
            filesystem_free = max(0, int(shutil.disk_usage(self.root).free))
        except (OSError, TypeError, ValueError, OverflowError) as exc:
            filesystem_free = None
            error_type, detail = _safe_exception_summary(exc, max_chars=120)
            filesystem_probe_error = f"{error_type}: {detail}"
        filesystem_capacity = (
            total + filesystem_free
            if filesystem_free is not None
            else None
        )
        effective_capacity = (
            min(self.max_storage_bytes, filesystem_capacity)
            if self.max_storage_bytes > 0 and filesystem_capacity is not None
            else None
        )
        required_capacity = (
            math.ceil(projected * _CAPACITY_OPERATIONAL_RESERVE_RATIO)
            if projected is not None
            else None
        )
        capacity_ok = (
            False
            if measurement_errors
            else None
            if projected is None or self.max_storage_bytes <= 0
            else (
                total <= self.max_storage_bytes
                and required_capacity is not None
                and self.max_storage_bytes >= required_capacity
                and filesystem_capacity is not None
                and filesystem_capacity >= required_capacity
            )
        )
        headroom_ratio = (
            None
            if (
                measurement_errors
                or not projected
                or effective_capacity is None
            )
            else effective_capacity / projected
        )
        if measurement_errors or filesystem_capacity is None:
            capacity_state = "unavailable"
        elif projected is None or self.max_storage_bytes <= 0:
            capacity_state = "collecting"
        elif capacity_ok:
            capacity_state = "healthy"
        elif headroom_ratio is not None and headroom_ratio >= 1.0:
            capacity_state = "low_headroom"
        else:
            capacity_state = "insufficient"
        return {
            "total_bytes": total,
            "measurement_complete": measurement_errors == 0,
            "measurement_errors": measurement_errors,
            "max_storage_bytes": self.max_storage_bytes,
            "closed_days_observed": len(closed),
            "peak_closed_day_bytes": peak,
            "projected_required_bytes": projected,
            "required_capacity_bytes": required_capacity,
            "filesystem_free_bytes": filesystem_free,
            "filesystem_capacity_bytes": filesystem_capacity,
            "filesystem_probe_error": filesystem_probe_error,
            "capacity_ok": capacity_ok,
            "capacity_state": capacity_state,
            "operational_reserve_ratio": (
                _CAPACITY_OPERATIONAL_RESERVE_RATIO
            ),
            "headroom_ratio": headroom_ratio,
        }

    def close(self) -> bool:
        with self._lock:
            if (
                not self._connections
                and not self._setup_connections
                and self._setup_connection_candidate is None
            ):
                self._write_terminal = True
                return True
        with self._root_partition_guard():
            return self._close_locked()

    def _close_locked(self) -> bool:
        with self._lock:
            # Close is a terminal write-admission boundary even when an OS
            # handle needs a later close retry. Otherwise a delayed producer
            # can reopen a partition between teardown attempts.
            self._write_terminal = True
            for attempt in range(2):
                failed = False
                for path in list(self._connections):
                    try:
                        self._close_path(path)
                    except Exception:
                        failed = True
                for connection in list(
                    getattr(self, "_setup_connections", ())
                ):
                    try:
                        connection.close()
                    except BaseException:
                        failed = True
                    else:
                        self._discard_setup_connection(connection)
                candidate = getattr(
                    self, "_setup_connection_candidate", None
                )
                if candidate is not None and not any(
                    pending is candidate
                    for pending in getattr(self, "_setup_connections", ())
                ):
                    try:
                        candidate.close()
                    except BaseException:
                        failed = True
                    else:
                        if self._setup_connection_candidate is candidate:
                            self._setup_connection_candidate = None
                if not failed:
                    return True
                if attempt == 0:
                    time.sleep(0.01)
            return False


class VenueRecorder:
    """Exchange-neutral overview, REST microstructure, and shadow L2 capture."""

    CAPTURE_FAILURE_THRESHOLD = 3
    _INTEGRITY_STOP_TIMEOUT_SEC = 15.0
    _INTEGRITY_RETRY_SECONDS = 300.0
    _INTEGRITY_FRESHNESS_SECONDS = 7_200.0
    GAP_WARNING_INTERVAL_SEC = 60.0
    GAP_WARNING_KEYS_MAX = 64

    def __init__(
        self,
        exchange,
        root: str | Path,
        *,
        max_symbols: int = 8,
        depth_levels: int = 20,
        micro_interval_seconds: float = 6.0,
        overview_interval_seconds: float = 60.0,
        retention_days: int = 30,
        max_storage_gib: float = 150.0,
        log_event=None,
        writer=None,
        l2_mode: str = "disabled",
        l2_sample_interval_seconds: float = 1.0,
        l2_stale_after_ms: int = 5_000,
        l2_collector_factory=None,
        priority_loader=None,
        health_callback=None,
    ) -> None:
        if type(max_symbols) is not int or not 1 <= max_symbols <= 50:
            raise ValueError(
                "max_symbols must be an integer between 1 and 50"
            )
        if type(depth_levels) is not int or not 5 <= depth_levels <= 100:
            raise ValueError(
                "depth_levels must be an integer between 5 and 100"
            )
        if (
            type(micro_interval_seconds) not in (int, float)
            or not math.isfinite(micro_interval_seconds)
            or not 1 <= micro_interval_seconds <= 600
        ):
            raise ValueError(
                "micro_interval_seconds must be a finite number between "
                "1 and 600"
            )
        if (
            type(overview_interval_seconds) not in (int, float)
            or not math.isfinite(overview_interval_seconds)
            or not micro_interval_seconds
            <= overview_interval_seconds
            <= 3_600
        ):
            raise ValueError(
                "overview_interval_seconds must be a finite number between "
                "micro_interval_seconds and 3600"
            )
        if (
            type(l2_sample_interval_seconds) not in (int, float)
            or not math.isfinite(l2_sample_interval_seconds)
            or not 0.25 <= l2_sample_interval_seconds <= 60
        ):
            raise ValueError(
                "l2_sample_interval_seconds must be a finite number between "
                "0.25 and 60"
            )
        if (
            type(l2_stale_after_ms) is not int
            or not 250 <= l2_stale_after_ms <= 60_000
        ):
            raise ValueError(
                "l2_stale_after_ms must be an integer between 250 and 60000"
            )
        if type(l2_mode) is not str or l2_mode not in {"disabled", "shadow"}:
            raise ValueError("l2_mode must be 'disabled' or 'shadow'")
        if l2_collector_factory is not None and not callable(
            l2_collector_factory
        ):
            raise ValueError("l2_collector_factory must be callable")
        if priority_loader is not None and not callable(priority_loader):
            raise ValueError("priority_loader must be callable")
        if health_callback is not None and not callable(health_callback):
            raise ValueError("health_callback must be callable")
        if log_event is not None and not callable(log_event):
            raise ValueError("log_event must be callable")
        self.exchange = exchange
        self.writer = (
            writer
            if writer is not None
            else SQLitePartitionWriter(
                root,
                retention_days=retention_days,
                max_storage_gib=max_storage_gib,
            )
        )
        self.max_symbols = max_symbols
        self.depth_levels = depth_levels
        self.micro_interval = float(micro_interval_seconds)
        self.overview_interval = float(overview_interval_seconds)
        self.log_event = log_event
        self._gap_warning_seen_at: dict[str, float] = {}
        self._health_callback = health_callback
        self._priority_loader = priority_loader
        self._last_priority_symbols: list[str] = []
        self._universe: list[str] = []
        self._cursor = 0
        self._next_retention_check = 0.0
        self._rest_health_lock = threading.Lock()
        self._rest_health: dict[tuple[str, str], dict] = {}
        self._integrity_lock = threading.Lock()
        self._integrity_thread: threading.Thread | None = None
        self._integrity_worker_state: dict | None = None
        self._integrity_shutdown_event = threading.Event()
        self._integrity_retry_at = 0.0
        self._integrity_last_success_monotonic: float | None = None
        self._resource_shutdown_lock = threading.Lock()
        self._run_state_lock = threading.Lock()
        self._run_state: dict | None = None
        self._run_admission_closed = False
        self._l2_stopped = False
        self._integrity_stopped = False
        self._writer_closed = False
        self._writer_close_state: dict | None = None
        try:
            writer_root = getattr(self.writer, "root", None)
        except Exception:
            # Optional/extension writers may expose a temporarily unavailable
            # root.  Construction must stay available, while integrity remains
            # fail-closed until a real scan can complete.
            writer_root = object()
        self._integrity_verified_once = writer_root is None
        self._integrity_health = {
            "ok": self._integrity_verified_once,
            "verified_once": self._integrity_verified_once,
            "sealed_days": 0,
            "valid_days": 0,
            "usable_days": 0,
            "degraded_days": [],
            "invalid_days": [],
            "latest_day": None,
            "continuity": {
                "window_days": 30,
                "observed_days": 0,
                "ready": False,
                "ok": None,
                "reason": "collecting_closed_days",
            },
        }
        self._integrity_errors_total = 0
        self._last_integrity_error = ""
        self._integrity_incident_key = None
        # Process-local proof cache: unchanged manifest artifacts are not
        # re-read in full on every hourly integrity pass. Any stat identity
        # change forces a fresh SHA-256 verification.
        self._integrity_verification_cache: dict[str, tuple] = {}
        self.l2_mode = l2_mode
        self._l2_collector = None
        if self.l2_mode == "shadow":
            if l2_collector_factory is None:
                from trading.l2_stream import L2ShadowCollector

                l2_collector_factory = L2ShadowCollector
            self._l2_collector = l2_collector_factory(
                exchange,
                root,
                writer=self.writer,
                max_symbols=self.max_symbols,
                depth_levels=self.depth_levels,
                sample_interval_seconds=l2_sample_interval_seconds,
                stale_after_ms=l2_stale_after_ms,
                log_event=log_event,
            )
        self._l2_stopped = self._l2_collector is None

    @staticmethod
    def _iso_now() -> str:
        return _capture_now_utc().isoformat().replace("+00:00", "Z")

    @staticmethod
    def _market_mapping(exchange) -> dict:
        try:
            markets = getattr(exchange, "markets", None)
        except Exception:
            return {}
        if not isinstance(markets, dict):
            return {}
        if type(markets) is dict:
            return markets
        try:
            return dict.copy(markets)
        except Exception:
            return {}

    @staticmethod
    def _market_row(markets: dict, symbol: str) -> dict:
        try:
            market = markets.get(symbol)
        except Exception:
            return {}
        if not isinstance(market, dict):
            return {}
        if type(market) is dict:
            return market
        try:
            return dict.copy(market)
        except Exception:
            return {}

    @staticmethod
    def _market_id(exchange, symbol: str) -> str:
        try:
            markets = VenueRecorder._market_mapping(exchange)
            market = VenueRecorder._market_row(markets, symbol)
            raw_market_id = market.get("id")
            market_id = raw_market_id if type(raw_market_id) is str else symbol
            if not SQLitePartitionWriter._valid_text(market_id, max_chars=256):
                return symbol
            return market_id
        except Exception:
            return symbol

    @staticmethod
    def _finite_number_or_none(value) -> float | None:
        if value is None or type(value) not in (int, float, str):
            return None
        try:
            parsed = float(value)
        except Exception:
            return None
        return parsed if math.isfinite(parsed) else None

    def _is_noncrypto_swap(self, symbol: str) -> bool:
        if not SQLitePartitionWriter._valid_text(symbol, max_chars=256):
            return True
        symbol_base = symbol.split("/", 1)[0]
        try:
            markets = self._market_mapping(self.exchange)
            market = self._market_row(markets, symbol)
            raw_base = market.get("base")
        except Exception:
            raw_base = None
        base = (
            raw_base
            if type(raw_base) is str
            and SQLitePartitionWriter._valid_text(raw_base, max_chars=249)
            else symbol_base
        )
        return base.upper() in NONCRYPTO_BASES

    def _priority_symbols(self, *, commit: bool = True) -> list[str]:
        try:
            if self._priority_loader is None:
                from core.database import list_venue_capture_priorities

                requested = list_venue_capture_priorities(self.max_symbols)
            else:
                requested = self._priority_loader(self.max_symbols)
        except Exception as exc:
            self._log_gap("priority load gap", exc)
            return list(self._last_priority_symbols)
        try:
            if isinstance(requested, list):
                requested_count = list.__len__(requested)
                requested = list.__getitem__(requested, slice(0, self.max_symbols + 1))
            elif isinstance(requested, tuple):
                requested_count = tuple.__len__(requested)
                requested = tuple.__getitem__(requested, slice(0, self.max_symbols + 1))
            else:
                return list(self._last_priority_symbols)
        except Exception:
            return list(self._last_priority_symbols)
        if requested_count > self.max_symbols:
            return list(self._last_priority_symbols)
        markets = self._market_mapping(self.exchange)
        resolved = []
        for raw in requested:
            if not SQLitePartitionWriter._valid_text(raw, max_chars=256):
                continue
            candidate = raw
            symbol = candidate if candidate in markets else None
            if symbol is None:
                base = candidate.split("/", 1)[0].split("_", 1)[0].upper()
                for market_symbol in markets:
                    if not SQLitePartitionWriter._valid_text(
                        market_symbol, max_chars=256
                    ):
                        continue
                    market = self._market_row(markets, market_symbol)
                    try:
                        market_base = (market or {}).get("base")
                        matches = (
                            type(market_base) is str
                            and SQLitePartitionWriter._valid_text(
                                market_base,
                                max_chars=249,
                            )
                            and market_base.upper() == base
                            and (market or {}).get("swap") is True
                            and type((market or {}).get("quote")) is str
                            and (market or {}).get("quote") == "USDT"
                        )
                    except Exception:
                        continue
                    if matches:
                        symbol = market_symbol
                        break
            market = (
                self._market_row(markets, symbol)
                if symbol is not None
                else None
            )
            market_base = market.get("base") if isinstance(market, dict) else None
            market_swap = market.get("swap") if isinstance(market, dict) else None
            market_quote = market.get("quote") if isinstance(market, dict) else None
            market_active = market.get("active") if isinstance(market, dict) else None
            if (
                symbol is None
                or not SQLitePartitionWriter._valid_text(symbol, max_chars=256)
                or not isinstance(market, dict)
                or (
                    market_base is not None
                    and (
                        type(market_base) is not str
                        or not SQLitePartitionWriter._valid_text(
                            market_base,
                            max_chars=249,
                        )
                    )
                )
                or market_swap is not True
                or type(market_quote) is not str
                or market_quote != "USDT"
                or (
                    market_active is not None
                    and type(market_active) is not bool
                )
                or market_active is False
                or self._is_noncrypto_swap(symbol)
                or symbol in resolved
            ):
                continue
            resolved.append(symbol)
            if len(resolved) >= self.max_symbols:
                break
        if commit:
            self._last_priority_symbols = list(resolved)
        return resolved

    @staticmethod
    def _exception_summary(
        exc: BaseException,
        *,
        max_chars: int = 160,
    ) -> tuple[str, str]:
        return _safe_exception_summary(exc, max_chars=max_chars)

    def _log_gap(self, context: str, exc: BaseException) -> None:
        try:
            context_key = str(context or "capture gap")[:120]
        except Exception:
            context_key = "capture gap"
        if self.log_event is None:
            self._silent_gap_fallback(context_key, exc)
            return
        try:
            current = time.monotonic()
            error_key = type(exc).__name__[:40]
            key = f"{context_key}|{error_key}"
            seen = getattr(self, "_gap_warning_seen_at", None)
            if not isinstance(seen, dict):
                seen = {}
                self._gap_warning_seen_at = seen
            previous = seen.get(key)
            if (
                isinstance(previous, (int, float))
                and current - float(previous) < self.GAP_WARNING_INTERVAL_SEC
            ):
                return
            seen[key] = current
            if len(seen) > self.GAP_WARNING_KEYS_MAX:
                newest = sorted(
                    seen.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:self.GAP_WARNING_KEYS_MAX]
                self._gap_warning_seen_at = dict(newest)
        except Exception:
            # Throttling is display hygiene only. If its own bookkeeping is
            # unavailable, retain the original immediate warning behavior.
            pass
        error_type, detail = self._exception_summary(exc, max_chars=240)
        try:
            self.log_event(
                f"Venue recorder {context_key}: {error_type}"
                + (f": {detail}" if detail else ""),
                "WARN",
            )
        except Exception:
            self._silent_gap_fallback(context_key, exc)

    @staticmethod
    def _silent_gap_fallback(context: str, exc: BaseException) -> None:
        """Persist the root failure when the visible logger is unavailable."""
        try:
            from bot_utils.silent_log import silent_log

            silent_log(f"Venue recorder {context}", exc)
        except Exception:
            pass

    def _report_health(self, payload: dict) -> None:
        if self._health_callback is None:
            return
        try:
            self._health_callback(dict(payload))
        except Exception as exc:
            self._log_gap("health callback gap", exc)

    @staticmethod
    def _require_api_budget(endpoint: str):
        reservation = try_consume_api_call(
            endpoint,
            return_reservation=True,
        )
        if not reservation:
            raise RuntimeError(f"API budget denied: {endpoint}")
        return reservation

    def _record_api_failure(self, endpoint: str, reservation) -> None:
        if not isinstance(reservation, ApiCallReservation):
            return
        try:
            record_api_error(endpoint, reservation)
        except Exception as exc:
            self._log_gap("API error-ledger gap", exc)

    def _write_event(
        self,
        kind: str,
        symbol: str,
        payload: dict,
        *,
        exchange_ms=None,
        started_ms: int,
        ended_ms: int,
        flags: tuple[str, ...] = (),
        market_id: str | None = None,
        health_symbol: str | None = None,
        health_stream: str | None = None,
        event_universe: list[str] | None = None,
    ) -> Path:
        market_id = market_id or self._market_id(self.exchange, symbol)
        if ended_ms < started_ms and "wallclock_non_monotonic" not in flags:
            flags = (*flags, "wallclock_non_monotonic")
        raw_event_clock = exchange_ms if exchange_ms is not None else ended_ms
        invalid_exchange_clock = False
        stale_exchange_clock = False
        future_exchange_clock = False
        received_time = datetime.fromtimestamp(
            int(ended_ms) / 1000, tz=timezone.utc
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        try:
            event_clock_value = self._finite_number_or_none(raw_event_clock)
            if (
                event_clock_value is None
                or event_clock_value <= 0
                or not event_clock_value.is_integer()
                or event_clock_value
                > int(ended_ms) + MAX_PARTITION_CLOCK_AGE_MS
            ):
                raise ValueError("event clock outside accepted range")
            if (
                exchange_ms is not None
                and event_clock_value
                > int(ended_ms) + MAX_EXCHANGE_FUTURE_SKEW_MS
            ):
                future_exchange_clock = True
                raise ValueError("event clock is in the future")
            if (
                exchange_ms is not None
                and event_clock_value
                < int(ended_ms) - MAX_PARTITION_CLOCK_AGE_MS
            ):
                stale_exchange_clock = True
                raise ValueError("event clock is stale")
            event_clock = int(event_clock_value)
            exchange_time = datetime.fromtimestamp(
                event_clock / 1000, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except (TypeError, ValueError, OverflowError, OSError):
            event_clock = int(ended_ms)
            exchange_time = received_time
            invalid_exchange_clock = bool(
                exchange_ms is not None
                and not stale_exchange_clock
                and not future_exchange_clock
            )
        if (
            stale_exchange_clock
            and "stale_exchange_timestamp" not in flags
        ):
            flags = (*flags, "stale_exchange_timestamp")
        elif (
            future_exchange_clock
            and "future_exchange_timestamp" not in flags
        ):
            flags = (*flags, "future_exchange_timestamp")
        elif invalid_exchange_clock and "invalid_exchange_timestamp" not in flags:
            flags = (*flags, "invalid_exchange_timestamp")
        stored_payload = {
            **payload,
            "request_started_ms": started_ms,
            "request_ended_ms": ended_ms,
            "latency_ms": max(0, ended_ms - started_ms),
            "universe": list(
                self._universe
                if event_universe is None
                else event_universe
            ),
        }
        digest_input = json.dumps(
            {
                "payload": stored_payload,
                "quality_flags": flags,
            },
            sort_keys=True,
            default=str,
        )
        digest = hashlib.blake2s(digest_input.encode("utf-8"), digest_size=8).hexdigest()
        path = self.writer.write(
            VenueEvent(
                event_id=(
                    f"{kind}:{market_id}:{event_clock}:"
                    f"{started_ms}:{ended_ms}:{digest}"
                ),
                kind=kind,
                market_id=market_id,
                exchange_time=exchange_time,
                received_time=received_time,
                payload=stored_payload,
                quality_flags=flags,
            )
        )
        if not _writer_acknowledged(path):
            raise RuntimeError(f"{kind} capture writer rejected event")
        if health_stream is not None:
            self._record_rest_observation(
                health_symbol or market_id,
                health_stream,
                flags,
            )
        return path

    def capture_overview(self) -> int:
        endpoint = "venue_recorder_fetch_tickers"
        reservation = self._require_api_budget(endpoint)
        started = _capture_now_ms()
        try:
            tickers = self.exchange.fetch_tickers()
        except Exception:
            self._record_api_failure(endpoint, reservation)
            raise
        ended = _capture_now_ms()
        candidates = []
        invalid_numeric_payload = False
        invalid_ticker_identity = False
        invalid_tickers_payload = not isinstance(tickers, dict)
        if isinstance(tickers, dict):
            try:
                ticker_rows = tuple(dict.items(tickers))
            except Exception:
                invalid_tickers_payload = True
                ticker_rows = ()
        else:
            ticker_rows = ()

        def _number(value):
            nonlocal invalid_numeric_payload
            parsed = self._finite_number_or_none(value)
            if value is not None and parsed is None:
                invalid_numeric_payload = True
            return parsed

        def _positive_price(value):
            nonlocal invalid_numeric_payload
            parsed = _number(value)
            if parsed is not None and parsed <= 0:
                invalid_numeric_payload = True
                return None
            return parsed

        def _nonnegative_number(value):
            nonlocal invalid_numeric_payload
            parsed = _number(value)
            if parsed is not None and parsed < 0:
                invalid_numeric_payload = True
                return None
            return parsed

        def _funding_rate(value):
            nonlocal invalid_numeric_payload
            parsed = _number(value)
            if parsed is not None and abs(parsed) > 1:
                invalid_numeric_payload = True
                return None
            return parsed

        def _settlement_time(value):
            nonlocal invalid_numeric_payload
            parsed = _number(value)
            if parsed is not None and (parsed <= 0 or not parsed.is_integer()):
                invalid_numeric_payload = True
                return None
            return None if parsed is None else int(parsed)

        quote_volumes: dict[str, float | None] = {}
        quote_volume_sources: dict[str, str] = {}
        markets = self._market_mapping(self.exchange)

        for symbol, ticker in ticker_rows:
            if not SQLitePartitionWriter._valid_text(symbol, max_chars=256):
                invalid_ticker_identity = True
                continue
            if isinstance(ticker, dict) and type(ticker) is not dict:
                try:
                    ticker = dict.copy(ticker)
                except Exception:
                    invalid_numeric_payload = True
                    continue
            if isinstance(ticker, dict):
                raw_ticker_symbol = dict.get(ticker, "symbol")
                if (
                    raw_ticker_symbol is not None
                    and (
                        type(raw_ticker_symbol) is not str
                        or raw_ticker_symbol != str.strip(raw_ticker_symbol)
                    )
                ) or not explicit_trade_symbol_matches(ticker, symbol):
                    invalid_ticker_identity = True
                    continue
            try:
                raw_market = markets.get(symbol)
            except Exception:
                raw_market = None
            if not isinstance(raw_market, dict):
                invalid_ticker_identity = True
                continue
            market = self._market_row(markets, symbol)
            swap = market.get("swap")
            quote = market.get("quote")
            if swap is not True or type(quote) is not str or quote != "USDT":
                if (swap is not None and type(swap) is not bool) or (
                    quote is not None and type(quote) is not str
                ):
                    invalid_ticker_identity = True
                continue
            active = market.get("active")
            if active is not None and type(active) is not bool:
                invalid_ticker_identity = True
                continue
            if not isinstance(ticker, dict):
                invalid_numeric_payload = True
                continue
            raw_quote_volume = ticker.get("quoteVolume")
            if raw_quote_volume is not None:
                quote_volume = _number(raw_quote_volume)
                quote_volume_source = "ticker_quote"
                if quote_volume is None or quote_volume < 0.0:
                    invalid_numeric_payload = True
                    quote_volume = None
                    quote_volume_source = "invalid"
            else:
                base_volume = _number(ticker.get("baseVolume"))
                last_price = _number(ticker.get("last"))
                if base_volume is None or last_price is None:
                    quote_volume = None
                    quote_volume_source = "unavailable"
                elif base_volume < 0.0 or last_price <= 0.0:
                    invalid_numeric_payload = True
                    quote_volume = None
                    quote_volume_source = "invalid"
                else:
                    quote_volume = base_volume * last_price
                    if not math.isfinite(quote_volume):
                        invalid_numeric_payload = True
                        quote_volume = None
                        quote_volume_source = "invalid"
                    else:
                        quote_volume_source = "base_times_last"
            quote_volumes[symbol] = quote_volume
            quote_volume_sources[symbol] = quote_volume_source
            candidates.append((quote_volume or 0.0, symbol, ticker))
        candidates.sort(reverse=True)
        volume_universe = [
            symbol
            for _volume, symbol, _ticker in candidates
            if not self._is_noncrypto_swap(symbol)
            and self._market_row(markets, symbol).get("active") is not False
        ]
        priority_universe = self._priority_symbols(commit=False)
        next_universe = list(
            dict.fromkeys((*priority_universe, *volume_universe))
        )[: self.max_symbols]
        markets_payload = {}
        for _volume, symbol, ticker in candidates:
            info = ticker.get("info") if isinstance(ticker, dict) else None
            if info is None:
                info = {}
            elif not isinstance(info, dict):
                invalid_numeric_payload = True
                info = {}
            if type(info) is not dict:
                try:
                    info = dict.copy(info)
                except Exception:
                    invalid_numeric_payload = True
                    info = {}
            market = self._market_row(markets, symbol)
            raw_base = market.get("base")
            if raw_base is None:
                base = symbol.split("/", 1)[0]
            elif SQLitePartitionWriter._valid_text(raw_base, max_chars=249):
                base = raw_base
            else:
                invalid_ticker_identity = True
                base = symbol.split("/", 1)[0]
            spot_symbol = f"{base}/USDT" if base else ""
            if spot_symbol and not SQLitePartitionWriter._valid_text(
                spot_symbol, max_chars=256
            ):
                invalid_ticker_identity = True
                spot_symbol = ""
            spot_market = (
                self._market_row(markets, spot_symbol)
                if spot_symbol
                else None
            )
            spot_available = False
            if isinstance(spot_market, dict):
                spot = spot_market.get("spot")
                spot_active = spot_market.get("active")
                if (spot is not None and type(spot) is not bool) or (
                    spot_active is not None and type(spot_active) is not bool
                ):
                    invalid_ticker_identity = True
                else:
                    spot_available = spot is True and spot_active is not False
            market_id = self._market_id(self.exchange, symbol)
            if market_id in markets_payload:
                invalid_ticker_identity = True
                if (
                    not SQLitePartitionWriter._valid_text(symbol, max_chars=256)
                    or symbol in markets_payload
                ):
                    continue
                market_id = symbol
            last_price = _positive_price(ticker.get("last"))
            bid_price = _positive_price(ticker.get("bid"))
            ask_price = _positive_price(ticker.get("ask"))
            if (
                bid_price is not None
                and ask_price is not None
                and bid_price >= ask_price
            ):
                invalid_numeric_payload = True
                bid_price = None
                ask_price = None
            index_raw = info.get("indexPrice")
            if index_raw is None:
                index_raw = ticker.get("index")
            fair_raw = info.get("fairPrice")
            if fair_raw is None:
                fair_raw = ticker.get("mark")
            markets_payload[market_id] = {
                "symbol": symbol,
                "spot_symbol": spot_symbol or None,
                "spot_available": spot_available,
                "last": last_price,
                "bid": bid_price,
                "ask": ask_price,
                "quote_volume": quote_volumes[symbol],
                "quote_volume_source": quote_volume_sources[symbol],
                "hold_volume": _nonnegative_number(info.get("holdVol")),
                "index_price": _positive_price(index_raw),
                "fair_price": _positive_price(fair_raw),
                "funding_rate": _funding_rate(info.get("fundingRate")),
                "next_settle_time": _settlement_time(info.get("nextSettleTime")),
                "universe_member": symbol in next_universe,
                "capture_priority": symbol in priority_universe,
            }
        overview_flags = []
        if invalid_tickers_payload:
            overview_flags.append("invalid_tickers_payload")
        if invalid_ticker_identity:
            overview_flags.append("invalid_ticker_identity")
        if invalid_numeric_payload:
            overview_flags.append("invalid_numeric_payload")
        latest_timestamp, invalid_exchange_timestamp = self._latest_timestamp(
            candidates, ended
        )
        if invalid_exchange_timestamp:
            overview_flags.append("invalid_exchange_timestamp")
        if latest_timestamp > ended + MAX_EXCHANGE_FUTURE_SKEW_MS:
            overview_flags.append("future_exchange_timestamp")
        elif latest_timestamp < ended - MAX_PARTITION_CLOCK_AGE_MS:
            overview_flags.append("stale_exchange_timestamp")
        if overview_flags:
            self._record_api_failure(endpoint, reservation)
        self._write_event(
            "overview",
            "",
            {"markets": markets_payload},
            exchange_ms=latest_timestamp,
            started_ms=started,
            ended_ms=ended,
            flags=tuple(overview_flags),
            market_id="ALL_USDT_SWAPS",
            health_symbol="ALL_USDT_SWAPS",
            health_stream="overview",
            event_universe=next_universe,
        )
        self._last_priority_symbols = list(priority_universe)
        self._universe = next_universe
        return len(candidates)

    @classmethod
    def _latest_timestamp(cls, candidates, fallback: int) -> tuple[int, bool]:
        timestamps = []
        invalid_timestamp = False
        for _volume, _symbol, ticker in candidates:
            value = ticker.get("timestamp") if isinstance(ticker, dict) else None
            if value is None:
                continue
            timestamp_value = cls._finite_number_or_none(value)
            if (
                timestamp_value is None
                or timestamp_value <= 0
                or not timestamp_value.is_integer()
            ):
                invalid_timestamp = True
                continue
            timestamps.append(int(timestamp_value))
        return max(timestamps, default=int(fallback)), invalid_timestamp

    def capture_microstructure(self, symbol: str) -> tuple[Path, Path]:
        book_endpoint = "venue_recorder_fetch_order_book"
        book_reservation = self._require_api_budget(book_endpoint)
        started_book = _capture_now_ms()
        try:
            book = self.exchange.fetch_order_book(
                symbol,
                limit=self.depth_levels,
            )
        except Exception:
            self._record_api_failure(book_endpoint, book_reservation)
            raise
        ended_book = _capture_now_ms()
        if isinstance(book, dict) and type(book) is not dict:
            try:
                book = dict.copy(book)
            except Exception:
                book = {}
        flags = []
        try:
            from trading.l2_stream import normalize_order_book
            raw_book_symbol = dict.get(book, "symbol")
            if (
                raw_book_symbol is not None
                and (
                    type(raw_book_symbol) is not str
                    or raw_book_symbol != str.strip(raw_book_symbol)
                )
            ) or not explicit_trade_symbol_matches(book, symbol):
                raise ValueError("venue order book changed requested symbol")
            normalized_book = normalize_order_book(
                book,
                depth_levels=self.depth_levels,
            )
            book_timestamp = normalized_book["timestamp"]
            if (
                book_timestamp is not None
                and book_timestamp
                > ended_book + MAX_EXCHANGE_FUTURE_SKEW_MS
            ):
                flags.append("future_exchange_timestamp")
                self._record_api_failure(book_endpoint, book_reservation)
            elif (
                book_timestamp is not None
                and book_timestamp
                < ended_book - MAX_PARTITION_CLOCK_AGE_MS
            ):
                flags.append("stale_exchange_timestamp")
                self._record_api_failure(book_endpoint, book_reservation)
        except Exception as exc:
            self._record_api_failure(book_endpoint, book_reservation)
            flags.append("invalid_book_payload")
            if "empty" in str(exc):
                flags.append("incomplete_book")
            if "crossed or locked" in str(exc):
                flags.append("crossed_book")
            raw_timestamp = (
                book.get("timestamp") if isinstance(book, dict) else None
            )
            timestamp = self._finite_number_or_none(raw_timestamp)
            invalid_depth_timestamp = raw_timestamp is not None and (
                timestamp is None
                or timestamp <= 0
                or not timestamp.is_integer()
                or timestamp > ended_book + 86_400_000
            )
            if invalid_depth_timestamp:
                flags.append("invalid_exchange_timestamp")
                timestamp = None
            normalized_book = {
                "bids": [],
                "asks": [],
                "nonce": None,
                "timestamp": int(timestamp) if timestamp is not None else None,
            }
        book_path = self._write_event(
            "depth",
            symbol,
            {
                "bids": normalized_book["bids"],
                "asks": normalized_book["asks"],
                "nonce": normalized_book["nonce"],
            },
            exchange_ms=normalized_book["timestamp"],
            started_ms=started_book,
            ended_ms=ended_book,
            flags=tuple(flags),
            health_symbol=symbol,
            health_stream="depth",
        )
        trades_endpoint = "venue_recorder_fetch_trades"
        trades_reservation = self._require_api_budget(trades_endpoint)
        started_trades = _capture_now_ms()
        try:
            trades = self.exchange.fetch_trades(symbol, limit=100)
        except Exception:
            self._record_api_failure(trades_endpoint, trades_reservation)
            raise
        ended_trades = _capture_now_ms()
        normalized = []
        seen: dict[tuple, tuple] = {}
        out_of_order = False
        invalid_trade_payload = not isinstance(trades, (list, tuple))
        truncated_trade_payload = False
        conflicting_trade_id = False
        last_ts = -1
        trade_count = 0
        if isinstance(trades, (list, tuple)):
            try:
                if isinstance(trades, list):
                    trade_count = list.__len__(trades)
                    trade_rows = list.__getitem__(trades, slice(0, 100))
                else:
                    trade_count = tuple.__len__(trades)
                    trade_rows = tuple.__getitem__(trades, slice(0, 100))
            except Exception:
                invalid_trade_payload = True
                trade_rows = ()
            if trade_count > 100:
                invalid_trade_payload = True
                truncated_trade_payload = True
        else:
            trade_rows = ()
        for trade in trade_rows:
            if isinstance(trade, dict) and type(trade) is not dict:
                try:
                    trade = dict.copy(trade)
                except Exception:
                    invalid_trade_payload = True
                    continue
            if not isinstance(trade, dict):
                invalid_trade_payload = True
                continue
            raw_trade_symbol = dict.get(trade, "symbol")
            if (
                raw_trade_symbol is not None
                and (
                    type(raw_trade_symbol) is not str
                    or raw_trade_symbol != str.strip(raw_trade_symbol)
                )
            ) or not explicit_trade_symbol_matches(trade, symbol):
                invalid_trade_payload = True
                continue
            raw_trade_id = trade.get("id")
            trade_id = order_id_text_or_none(raw_trade_id)
            unsafe_text_id = isinstance(raw_trade_id, str) and (
                type(raw_trade_id) is not str
                or raw_trade_id != str.strip(raw_trade_id)
            )
            unsafe_numeric_id = (
                isinstance(raw_trade_id, int)
                and (
                    type(raw_trade_id) is not int
                    or raw_trade_id < 0
                )
            )
            if raw_trade_id is not None and (
                trade_id is None or unsafe_text_id or unsafe_numeric_id
            ):
                invalid_trade_payload = True
                continue
            if trade_id is not None and not SQLitePartitionWriter._valid_text(
                trade_id,
                max_chars=256,
            ):
                invalid_trade_payload = True
                continue

            timestamp_value = self._finite_number_or_none(
                trade.get("timestamp")
            )
            price = self._finite_number_or_none(trade.get("price"))
            amount = self._finite_number_or_none(trade.get("amount"))
            if (
                timestamp_value is None
                or timestamp_value <= 0
                or not timestamp_value.is_integer()
                or timestamp_value
                > ended_trades + MAX_EXCHANGE_FUTURE_SKEW_MS
                or timestamp_value
                < ended_trades - MAX_PARTITION_CLOCK_AGE_MS
                or price is None
                or price <= 0
                or amount is None
                or amount <= 0
            ):
                invalid_trade_payload = True
                continue
            timestamp = int(timestamp_value)
            raw_side = trade.get("side")
            if (
                type(raw_side) is str
                and raw_side == str.strip(raw_side)
            ):
                side = str.lower(raw_side)
            else:
                side = None
            if side not in {"buy", "sell"}:
                invalid_trade_payload = True
                continue
            evidence = (timestamp, price, amount, side)
            dedup_key = (
                ("id", trade_id)
                if trade_id is not None
                else ("fallback", *evidence)
            )
            previous = seen.get(dedup_key)
            if previous is not None:
                if trade_id is not None and previous != evidence:
                    invalid_trade_payload = True
                    conflicting_trade_id = True
                continue
            seen[dedup_key] = evidence
            if timestamp < last_ts:
                out_of_order = True
            last_ts = max(last_ts, timestamp)
            normalized.append(
                {
                    "id": trade_id or None,
                    "timestamp": timestamp,
                    "price": price,
                    "amount": amount,
                    "side": side,
                }
            )
        trade_flags = []
        if out_of_order:
            trade_flags.append("out_of_order")
        if invalid_trade_payload:
            self._record_api_failure(trades_endpoint, trades_reservation)
            trade_flags.append("invalid_trade_payload")
        if conflicting_trade_id:
            trade_flags.append("conflicting_trade_id")
        if truncated_trade_payload:
            trade_flags.append("truncated_trade_payload")
        # The MEXC swap endpoint is a latest-N window.  A full page cannot
        # prove that no older trade was displaced between polls, even though
        # CCXT correctly returns no more than the requested limit.
        if trade_count >= 100:
            trade_flags.append("saturated_trade_payload")
        # REST latest-N is one receipt snapshot; individual exchange times
        # remain in trades without reopening a potentially sealed prior day.
        trades_path = self._write_event(
            "trades",
            symbol,
            {
                "trades": normalized,
                "rest_snapshot_version": 1,
                "partition_basis": "request_received",
            },
            exchange_ms=ended_trades,
            started_ms=started_trades,
            ended_ms=ended_trades,
            flags=tuple(trade_flags),
            health_symbol=symbol,
            health_stream="trades",
        )
        return book_path, trades_path

    def _record_rest_observation(
        self,
        symbol: str,
        stream: str,
        flags,
    ) -> None:
        now = time.monotonic()
        normalized_flags = tuple(sorted({str(flag) for flag in flags if flag}))
        with self._rest_health_lock:
            self._rest_health[(str(symbol), str(stream))] = {
                "last_attempt_monotonic": now,
                "last_valid_monotonic": None if normalized_flags else now,
                "flags": normalized_flags,
            }

    def _rest_data_health(self) -> dict:
        desired = tuple(self._universe)
        now = time.monotonic()
        stale_after = max(
            30.0,
            self.micro_interval * max(1, self.max_symbols) * 2.5,
        )
        overview_stale_after = max(30.0, self.overview_interval * 2.5)
        markets = {}
        with self._rest_health_lock:
            overview_item = self._rest_health.get(
                ("ALL_USDT_SWAPS", "overview")
            )
            overview_valid_at = (
                overview_item.get("last_valid_monotonic")
                if overview_item
                else None
            )
            overview_age = (
                None
                if overview_valid_at is None
                else max(0.0, now - overview_valid_at)
            )
            overview = {
                "valid": (
                    overview_age is not None
                    and overview_age <= overview_stale_after
                ),
                "age_seconds": overview_age,
                "flags": list((overview_item or {}).get("flags") or ()),
            }
            for symbol in desired:
                streams = {}
                for stream in ("depth", "trades"):
                    item = self._rest_health.get((symbol, stream))
                    valid_at = item.get("last_valid_monotonic") if item else None
                    age = None if valid_at is None else max(0.0, now - valid_at)
                    streams[stream] = {
                        "valid": age is not None and age <= stale_after,
                        "age_seconds": age,
                        "flags": list((item or {}).get("flags") or ()),
                    }
                markets[symbol] = streams
        # REST order-book freshness is part of the core snapshot contract.
        # REST trades are an independent reconciliation/audit window now that
        # the lossless-path candidate is the public WebSocket trade stream.
        missing = sorted(
            f"{symbol}:depth"
            for symbol, streams in markets.items()
            if streams["depth"]["valid"] is not True
        )
        if overview["valid"] is not True:
            missing.append("ALL_USDT_SWAPS:overview")
            missing.sort()
        trade_audit_warnings = sorted(
            f"{symbol}:trades"
            for symbol, streams in markets.items()
            if streams["trades"]["valid"] is not True
        )
        return {
            # Missing evidence is not healthy evidence. In particular, the
            # first status publication must stay degraded until overview and
            # every required depth stream produced a valid observation.
            "ok": not missing,
            "stale_after_seconds": stale_after,
            "overview_stale_after_seconds": overview_stale_after,
            "missing_or_invalid": missing,
            "trade_audit_warnings": trade_audit_warnings,
            "overview": overview,
            "markets": markets,
        }

    def _record_integrity_failure(self, exc: BaseException) -> None:
        error_type, detail = self._exception_summary(exc)
        incident_key = (
            "exception",
            error_type,
            detail,
        )
        with self._integrity_lock:
            if incident_key != self._integrity_incident_key:
                self._integrity_errors_total += 1
                self._integrity_incident_key = incident_key
            self._last_integrity_error = f"{error_type}: {detail}"
            self._integrity_health = {
                **self._integrity_health,
                "ok": False,
            }
            self._integrity_retry_at = (
                time.monotonic() + self._INTEGRITY_RETRY_SECONDS
            )
        self._log_gap("capture integrity gap", exc)

    def _run_integrity_check(self, writer_root: Path) -> None:
        try:
            from trading.capture_integrity import seal_closed_capture_days

            partition_guard = getattr(
                self.writer, "seal_partition_guard", None
            )
            if not callable(partition_guard):
                partition_guard = getattr(
                    self.writer, "partition_guard", None
                )

            health = seal_closed_capture_days(
                writer_root,
                max_symbols=self.max_symbols,
                micro_interval_seconds=self.micro_interval,
                l2_sample_interval_seconds=(
                    self._l2_collector.sample_interval
                    if self._l2_collector is not None
                    else 1.0
                ),
                partition_guard=partition_guard,
                verification_cache=self._integrity_verification_cache,
            )
        except Exception as exc:
            self._record_integrity_failure(exc)
            return
        with self._integrity_lock:
            self._integrity_verified_once = True
            self._integrity_retry_at = 0.0
            self._integrity_last_success_monotonic = time.monotonic()
            self._integrity_health = {
                **dict(health),
                "verified_once": True,
            }
            if health.get("ok") is True:
                self._last_integrity_error = ""
                self._integrity_incident_key = None
            else:
                invalid_days = tuple(
                    str(day) for day in (health.get("invalid_days") or [])
                )
                incident_key = (
                    "invalid_days",
                    invalid_days,
                    str(health.get("latest_day") or ""),
                )
                if incident_key != self._integrity_incident_key:
                    self._integrity_errors_total += 1
                    self._integrity_incident_key = incident_key
                self._last_integrity_error = (
                    "invalid sealed days: "
                    + ",".join(invalid_days)
                )[:160]

    def _schedule_integrity_check(self, writer_root) -> None:
        with self._integrity_lock:
            shutdown_event = getattr(
                self,
                "_integrity_shutdown_event",
                None,
            )
            if shutdown_event is None:
                shutdown_event = threading.Event()
                self._integrity_shutdown_event = shutdown_event
            if shutdown_event.is_set():
                return
            state = getattr(self, "_integrity_worker_state", None)
            if state is not None and not state["done"].is_set():
                return
            if self._integrity_thread and self._integrity_thread.is_alive():
                return
            state = {
                "done": threading.Event(),
                "thread": None,
                "started_monotonic": time.monotonic(),
            }

            def run_owned() -> None:
                try:
                    self._run_integrity_check(Path(writer_root))
                except BaseException as exc:
                    # Fatal target exits must not disappear behind the daemon
                    # thread boundary with the previous health still green.
                    self._record_integrity_failure(exc)
                finally:
                    state["done"].set()

            thread = threading.Thread(
                target=run_owned,
                daemon=True,
                name="VenueCaptureIntegrity",
            )
            state["thread"] = thread
            self._integrity_worker_state = state
            self._integrity_thread = thread
            try:
                thread.start()
            except BaseException as exc:
                if (
                    isinstance(exc, Exception)
                    and thread_definitely_never_started(thread)
                ):
                    # Exact stdlib pre-launch failure: no worker can ever own
                    # this generation, so a later integrity pass may retry.
                    state["done"].set()
                # Launch acceptance is uncertain after start() was invoked.
                # Keep the exact state authoritative until the target confirms
                # completion; shutdown must not close the shared writer early.
                raise

    def _schedule_integrity_retry_if_due(
        self,
        writer_root,
        *,
        now: float,
    ) -> bool:
        with self._integrity_lock:
            retry_at = getattr(self, "_integrity_retry_at", 0.0)
            if retry_at <= 0.0 or now < retry_at:
                return False
        try:
            self._schedule_integrity_check(writer_root)
        except BaseException:
            with self._integrity_lock:
                if self._integrity_retry_at <= now:
                    self._integrity_retry_at = (
                        now + self._INTEGRITY_RETRY_SECONDS
                    )
            raise
        with self._integrity_lock:
            # A quickly failing replacement may already have installed a newer
            # future deadline. Only consume the due generation we observed.
            if self._integrity_retry_at <= now:
                self._integrity_retry_at = 0.0
        return True

    def _integrity_status(self) -> tuple[dict, int, str]:
        with self._integrity_lock:
            health = dict(self._integrity_health)
            verified_once = getattr(self, "_integrity_verified_once", True)
            health["verified_once"] = verified_once
            if not verified_once:
                health["ok"] = False
                health["initial_pending"] = True
            thread = self._integrity_thread
            state = getattr(self, "_integrity_worker_state", None)
            worker_running = (
                (state is not None and not state["done"].is_set())
                or (thread is not None and thread.is_alive())
            )
            now = time.monotonic()
            worker_age = None
            if state is not None:
                started = state.get("started_monotonic")
                if (
                    isinstance(started, (int, float))
                    and not isinstance(started, bool)
                    and math.isfinite(float(started))
                ):
                    worker_age = max(0.0, now - float(started))
                    health["worker_age_seconds"] = round(worker_age, 3)
            last_success = getattr(
                self,
                "_integrity_last_success_monotonic",
                None,
            )
            verified_age = None
            if (
                isinstance(last_success, (int, float))
                and not isinstance(last_success, bool)
                and math.isfinite(float(last_success))
            ):
                verified_age = max(0.0, now - float(last_success))
                health["last_verified_age_seconds"] = round(
                    verified_age,
                    3,
                )
            last_error = self._last_integrity_error
            if worker_running:
                health["worker_running"] = True
                if (
                    worker_age is not None
                    and worker_age > self._INTEGRITY_FRESHNESS_SECONDS
                ):
                    health["ok"] = False
                    health["worker_stalled"] = True
                    last_error = (
                        "integrity worker stalled for "
                        f"{worker_age:.3f} seconds"
                    )
            elif (
                health.get("ok") is True
                and verified_age is not None
                and verified_age > self._INTEGRITY_FRESHNESS_SECONDS
            ):
                health["ok"] = False
                health["stale"] = True
                last_error = (
                    "integrity verification stale for "
                    f"{verified_age:.3f} seconds"
                )
            return (
                health,
                self._integrity_errors_total,
                last_error,
            )

    def _wait_for_integrity_worker(self, *, timeout: float) -> bool:
        """Return only after the seal worker is quiescent or timeout expires."""
        deadline = time.monotonic() + max(0.0, float(timeout))
        lock_timeout = min(
            max(0.0, deadline - time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        if not self._integrity_lock.acquire(timeout=lock_timeout):
            return False
        try:
            state = getattr(self, "_integrity_worker_state", None)
            thread = (
                state.get("thread")
                if state is not None
                else self._integrity_thread
            )
        finally:
            self._integrity_lock.release()
        if thread is None:
            return True
        try:
            if thread.is_alive():
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
            if state is not None and not state["done"].is_set():
                return False
            return not thread.is_alive()
        except Exception:
            return False

    def _wait_for_writer_close(self, *, deadline: float) -> bool:
        """Own exactly one close attempt and observe it within the deadline."""
        state = getattr(self, "_writer_close_state", None)
        if state is None:
            close = getattr(self.writer, "close", None)
            if not callable(close):
                self._writer_closed = True
                return True
            state = {
                "done": threading.Event(),
                "thread": None,
                "outcome": False,
                "error": None,
            }

            def close_writer() -> None:
                try:
                    closed = close()
                    if closed is True:
                        state["outcome"] = True
                    elif closed is False:
                        state["error"] = RuntimeError(
                            "partition writer connections remain open"
                        )
                    else:
                        state["error"] = RuntimeError(
                            "partition writer close was not confirmed"
                        )
                except BaseException as exc:
                    state["error"] = (
                        exc
                        if isinstance(exc, Exception)
                        else RuntimeError(
                            f"partition writer close aborted: "
                            f"{type(exc).__name__}"
                        )
                    )
                finally:
                    state["done"].set()

            try:
                thread = threading.Thread(
                    target=close_writer,
                    name="venue-writer-close",
                    daemon=True,
                )
            except BaseException as exc:
                error = (
                    exc
                    if isinstance(exc, Exception)
                    else RuntimeError(
                        f"writer close worker construction aborted: "
                        f"{type(exc).__name__}"
                    )
                )
                self._log_gap("writer close gap", error)
                return False
            state["thread"] = thread
            self._writer_close_state = state
            try:
                thread.start()
            except BaseException as exc:
                # Once start() was invoked, launch acceptance is uncertain.
                # Keep this exact state authoritative until its target proves
                # completion; a successor could otherwise close concurrently.
                state["launch_error"] = (
                    exc
                    if isinstance(exc, Exception)
                    else RuntimeError(
                        f"writer close worker launch aborted: "
                        f"{type(exc).__name__}"
                    )
                )
                if (
                    isinstance(exc, Exception)
                    and thread_definitely_never_started(thread)
                ):
                    # No close callback owns the writer. Mark this generation
                    # terminally failed so the next shutdown call can retry.
                    state["error"] = state["launch_error"]
                    state["done"].set()

        if not state["done"].is_set():
            state["done"].wait(
                timeout=max(0.0, deadline - time.monotonic())
            )
        if not state["done"].is_set():
            return False
        if state.get("outcome") is True:
            self._writer_closed = True
            return True
        error = state.get("error") or state.get("launch_error")
        if error is None:
            error = RuntimeError("partition writer close outcome is unknown")
        self._log_gap("writer close gap", error)
        if getattr(self, "_writer_close_state", None) is state:
            self._writer_close_state = None
        return False

    def shutdown_resources(self, *, timeout: float = 0.0) -> bool:
        """Retry child-producer and writer teardown after the run loop exits."""
        if type(timeout) not in (int, float):
            return False
        try:
            timeout = float(timeout)
        except Exception:
            return False
        if not math.isfinite(timeout):
            return False
        timeout = min(max(0.0, timeout), threading.TIMEOUT_MAX)
        deadline = time.monotonic() + timeout
        shutdown_lock = getattr(self, "_resource_shutdown_lock", None)
        if shutdown_lock is None:
            shutdown_lock = threading.Lock()
            self._resource_shutdown_lock = shutdown_lock
        lock_timeout = min(
            max(0.0, deadline - time.monotonic()),
            threading.TIMEOUT_MAX,
        )
        if not shutdown_lock.acquire(timeout=lock_timeout):
            return False
        try:
            integrity_shutdown = getattr(
                self,
                "_integrity_shutdown_event",
                None,
            )
            if integrity_shutdown is None:
                integrity_shutdown = threading.Event()
                self._integrity_shutdown_event = integrity_shutdown
            integrity_shutdown.set()
            run_state_lock = getattr(self, "_run_state_lock", None)
            if run_state_lock is None:
                run_state_lock = threading.Lock()
                self._run_state_lock = run_state_lock
            run_lock_timeout = min(
                max(0.0, deadline - time.monotonic()),
                threading.TIMEOUT_MAX,
            )
            if not run_state_lock.acquire(timeout=run_lock_timeout):
                return False
            try:
                self._run_admission_closed = True
                run_state = getattr(self, "_run_state", None)
            finally:
                run_state_lock.release()
            if (
                run_state is not None
                and not run_state["done"].is_set()
            ):
                if run_state.get("thread") is threading.current_thread():
                    return False
                run_state["done"].wait(
                    timeout=max(0.0, deadline - time.monotonic())
                )
                if not run_state["done"].is_set():
                    return False
            producers_quiescent = True
            l2_stopped = getattr(
                self, "_l2_stopped", self._l2_collector is None
            )
            if not l2_stopped and self._l2_collector is not None:
                try:
                    stopped = self._l2_collector.stop(
                        timeout=max(0.0, deadline - time.monotonic())
                    )
                    producers_quiescent = stopped is True
                    if producers_quiescent:
                        missing = object()
                        alive = getattr(
                            self._l2_collector, "is_alive", missing
                        )
                        if alive is not missing:
                            if callable(alive):
                                alive = alive()
                            producers_quiescent = alive is False
                    if producers_quiescent:
                        self._l2_stopped = True
                    if not producers_quiescent:
                        raise RuntimeError(
                            "capture producer remains alive after shutdown timeout"
                        )
                except Exception as exc:
                    producers_quiescent = False
                    self._log_gap("L2 stop gap", exc)
            integrity_quiescent = getattr(self, "_integrity_stopped", False)
            if not integrity_quiescent:
                integrity_quiescent = self._wait_for_integrity_worker(
                    timeout=max(0.0, deadline - time.monotonic()),
                )
                if integrity_quiescent:
                    self._integrity_stopped = True
            if not integrity_quiescent:
                producers_quiescent = False
                self._log_gap(
                    "capture integrity stop gap",
                    RuntimeError(
                        "integrity worker remains alive after shutdown timeout"
                    ),
                )
            if not producers_quiescent:
                return False
            if getattr(self, "_writer_closed", False):
                return True
            return self._wait_for_writer_close(deadline=deadline)
        finally:
            shutdown_lock.release()

    def run(self, shutdown_event: threading.Event) -> None:
        try:
            is_set = getattr(shutdown_event, "is_set", None)
            wait = getattr(shutdown_event, "wait", None)
        except Exception:
            is_set = None
            wait = None
        if not callable(is_set) or not callable(wait):
            raise ValueError("shutdown_event must provide is_set and wait")
        try:
            initially_set = is_set()
        except Exception as exc:
            raise ValueError("shutdown_event is_set must return bool") from exc
        if type(initially_set) is not bool:
            raise ValueError("shutdown_event is_set must return bool")
        def shutdown_requested() -> bool:
            try:
                requested = is_set()
            except Exception as exc:
                raise RuntimeError("shutdown_event is_set failed") from exc
            if type(requested) is not bool:
                raise RuntimeError("shutdown_event is_set failed")
            return requested

        def shutdown_wait(timeout: float) -> bool:
            try:
                requested = wait(timeout)
            except Exception as exc:
                raise RuntimeError("shutdown_event wait failed") from exc
            if type(requested) is not bool:
                raise RuntimeError("shutdown_event wait failed")
            return requested

        run_state = {
            "done": threading.Event(),
            "thread": threading.current_thread(),
        }
        run_state_lock = getattr(self, "_run_state_lock", None)
        if run_state_lock is None:
            run_state_lock = threading.Lock()
            self._run_state_lock = run_state_lock
        with run_state_lock:
            if getattr(self, "_run_admission_closed", False):
                return
            current = getattr(self, "_run_state", None)
            if current is not None and not current["done"].is_set():
                raise RuntimeError("venue recorder run owner already active")
            self._run_state = run_state
        next_overview = 0.0
        l2_started = False
        next_l2_start_attempt = 0.0
        capture_errors_consecutive = 0
        capture_errors_total = 0
        captures_total = 0
        overview_captures_total = 0
        overview_errors_total = 0
        microstructure_captures_total = 0
        microstructure_errors_total = 0
        last_capture_success_wall_ts = None
        last_capture_error = ""
        last_overview_error = ""
        last_microstructure_error = ""
        retention_ok = True
        retention_errors_total = 0
        last_retention_error = ""
        storage_health = {
            "capacity_ok": None,
            "closed_days_observed": 0,
        }
        l2_errors_consecutive = 0
        l2_errors_total = 0
        last_l2_error = ""
        l2_probe_errors_consecutive = 0
        l2_probe_errors_total = 0
        last_l2_probe_error = ""

        def note_l2_failure(exc: Exception) -> None:
            nonlocal l2_errors_consecutive, l2_errors_total, last_l2_error
            l2_errors_consecutive += 1
            l2_errors_total += 1
            error_type, detail = self._exception_summary(exc)
            last_l2_error = f"{error_type}: {detail}"

        def note_l2_success() -> None:
            nonlocal l2_errors_consecutive, last_l2_error
            l2_errors_consecutive = 0
            last_l2_error = ""

        def note_l2_probe_failure(exc: Exception) -> None:
            nonlocal l2_probe_errors_consecutive
            nonlocal l2_probe_errors_total
            nonlocal last_l2_probe_error
            l2_probe_errors_consecutive += 1
            l2_probe_errors_total += 1
            error_type, detail = self._exception_summary(exc)
            last_l2_probe_error = f"{error_type}: {detail}"

        def note_l2_probe_success() -> None:
            nonlocal l2_probe_errors_consecutive, last_l2_probe_error
            l2_probe_errors_consecutive = 0
            last_l2_probe_error = ""

        def read_l2_health_bool(attribute: str, *, default: bool) -> bool:
            marker_missing = object()
            marker = getattr(
                self._l2_collector,
                attribute,
                marker_missing,
            )
            if marker is marker_missing:
                return default
            value = marker() if callable(marker) else marker
            if type(value) is not bool:
                raise TypeError(f"collector {attribute} must return bool")
            return value

        def start_l2_confirmed() -> bool:
            missing = object()
            start_error = None
            try:
                result = self._l2_collector.start(shutdown_event)
            except Exception as exc:
                result = missing
                start_error = exc
            try:
                alive = getattr(self._l2_collector, "is_alive", missing)
                if callable(alive):
                    alive = alive()
            except Exception:
                if start_error is not None:
                    raise start_error
                raise
            if alive is missing:
                if start_error is not None:
                    raise start_error
                return result is None or result is True
            if type(alive) is not bool:
                if start_error is not None:
                    raise start_error
                return False
            if alive:
                return True
            if start_error is not None:
                raise start_error
            return False

        try:
            if self._l2_collector is not None:
                try:
                    if not start_l2_confirmed():
                        raise RuntimeError("collector start was not confirmed")
                    l2_started = True
                    note_l2_success()
                except Exception as exc:
                    note_l2_failure(exc)
                    next_l2_start_attempt = time.monotonic() + 30.0
                    self._log_gap("L2 start gap", exc)
            while not shutdown_requested():
                now = time.monotonic()
                if self._l2_collector is not None:
                    alive_missing = object()
                    alive_error = None
                    try:
                        alive_marker = getattr(
                            self._l2_collector,
                            "is_alive",
                            alive_missing,
                        )
                    except Exception as exc:
                        alive_marker = alive_missing
                        alive_error = exc
                    if l2_started and (
                        alive_marker is not alive_missing
                        or alive_error is not None
                    ):
                        try:
                            if alive_error is not None:
                                raise alive_error
                            alive_value = (
                                alive_marker()
                                if callable(alive_marker)
                                else alive_marker
                            )
                            if type(alive_value) is not bool:
                                raise TypeError(
                                    "collector is_alive must return bool"
                                )
                            collector_alive = alive_value
                        except Exception as exc:
                            collector_alive = False
                            alive_error = exc
                        if not collector_alive:
                            l2_started = False
                            stopped_error = (
                                alive_error
                                or RuntimeError(
                                    "collector thread is not alive"
                                )
                            )
                            note_l2_failure(stopped_error)
                            self._log_gap("L2 collector stopped", stopped_error)
                    if (
                        not l2_started
                        and now >= next_l2_start_attempt
                        and not shutdown_requested()
                    ):
                        try:
                            if not start_l2_confirmed():
                                raise RuntimeError(
                                    "collector restart was not confirmed"
                                )
                        except Exception as exc:
                            l2_started = False
                            note_l2_failure(exc)
                            next_l2_start_attempt = now + 30.0
                            self._log_gap("L2 restart gap", exc)
                        else:
                            l2_started = True
                            note_l2_success()
                            # If this replacement dies immediately, do not
                            # create one daemon thread per recorder cycle.
                            next_l2_start_attempt = now + 30.0
                            if self._universe:
                                try:
                                    updated = self._l2_collector.update_symbols(
                                        self._universe
                                    )
                                    if updated is not None and updated is not True:
                                        raise RuntimeError(
                                            "collector symbol update was not confirmed"
                                        )
                                except Exception as exc:
                                    note_l2_failure(exc)
                                    self._log_gap(
                                        "L2 symbol update gap", exc
                                    )
                                else:
                                    note_l2_success()
                if now >= self._next_retention_check:
                    retry_seconds = 3600.0
                    try:
                        enforce = getattr(self.writer, "enforce_retention", None)
                        if callable(enforce):
                            enforce()
                        storage_marker = getattr(
                            self.writer, "storage_health", None
                        )
                        if callable(storage_marker):
                            raw_storage_health = storage_marker()
                            if not isinstance(raw_storage_health, dict):
                                raise TypeError(
                                    "writer storage_health must return dict"
                                )
                            candidate_storage_health = {
                                key: dict.get(raw_storage_health, key)
                                for key in _STORAGE_HEALTH_FIELDS
                                if dict.__contains__(raw_storage_health, key)
                            }
                            for integer_field in _STORAGE_INTEGER_FIELDS:
                                if integer_field not in candidate_storage_health:
                                    continue
                                integer_value = candidate_storage_health.get(
                                    integer_field
                                )
                                if (
                                    type(integer_value) is not int
                                    or not 0
                                    <= integer_value
                                    <= _STREAM_HEALTH_COUNTER_MAX
                                ):
                                    raise ValueError(
                                        f"invalid storage {integer_field} marker"
                                    )
                            for integer_field in _STORAGE_OPTIONAL_INTEGER_FIELDS:
                                if integer_field not in candidate_storage_health:
                                    continue
                                integer_value = candidate_storage_health.get(
                                    integer_field
                                )
                                if integer_value is not None and (
                                    type(integer_value) is not int
                                    or not 0
                                    <= integer_value
                                    <= _STREAM_HEALTH_COUNTER_MAX
                                ):
                                    raise ValueError(
                                        f"invalid storage {integer_field} marker"
                                    )
                            for ratio_field in _STORAGE_RATIO_FIELDS:
                                if ratio_field not in candidate_storage_health:
                                    continue
                                ratio_value = candidate_storage_health.get(
                                    ratio_field
                                )
                                if ratio_value is None:
                                    continue
                                try:
                                    ratio_valid = bool(
                                        type(ratio_value) in (int, float)
                                        and math.isfinite(ratio_value)
                                        and ratio_value >= 0
                                    )
                                except (OverflowError, TypeError, ValueError):
                                    ratio_valid = False
                                if not ratio_valid:
                                    raise ValueError(
                                        f"invalid storage {ratio_field} marker"
                                    )
                            capacity_marker = candidate_storage_health.get(
                                "capacity_ok"
                            )
                            if (
                                capacity_marker is not None
                                and type(capacity_marker) is not bool
                            ):
                                raise ValueError(
                                    "invalid storage capacity health marker"
                                )
                            measurement_complete = candidate_storage_health.get(
                                "measurement_complete"
                            )
                            if (
                                "measurement_complete"
                                in candidate_storage_health
                                and type(measurement_complete) is not bool
                            ):
                                raise ValueError(
                                    "invalid storage measurement_complete marker"
                                )
                            measurement_errors = candidate_storage_health.get(
                                "measurement_errors"
                            )
                            if (
                                "measurement_errors" in candidate_storage_health
                                and (
                                    type(measurement_errors) is not int
                                    or not 0
                                    <= measurement_errors
                                    <= _STREAM_HEALTH_COUNTER_MAX
                                )
                            ):
                                raise ValueError(
                                    "invalid storage measurement_errors marker"
                                )
                            if (
                                measurement_complete is not None
                                and measurement_errors is not None
                                and measurement_complete
                                != (measurement_errors == 0)
                            ):
                                raise ValueError(
                                    "inconsistent storage measurement markers"
                                )
                            if capacity_marker is True and not (
                                measurement_complete is True
                                and measurement_errors == 0
                            ):
                                raise ValueError(
                                    "storage capacity lacks complete measurement"
                                )
                            capacity_state = candidate_storage_health.get(
                                "capacity_state"
                            )
                            if (
                                "capacity_state" in candidate_storage_health
                                and (
                                    type(capacity_state) is not str
                                    or capacity_state
                                    not in _STORAGE_CAPACITY_STATES
                                )
                            ):
                                raise ValueError(
                                    "invalid storage capacity_state marker"
                                )
                            if capacity_state is not None:
                                expected_capacity_marker = (
                                    None
                                    if capacity_state == "collecting"
                                    else True
                                    if capacity_state == "healthy"
                                    else False
                                )
                                if capacity_marker is not expected_capacity_marker:
                                    raise ValueError(
                                        "storage capacity state contradicts marker"
                                    )
                            filesystem_probe_error = (
                                candidate_storage_health.get(
                                    "filesystem_probe_error"
                                )
                            )
                            if (
                                "filesystem_probe_error"
                                in candidate_storage_health
                                and not _plain_health_text(
                                    filesystem_probe_error,
                                    max_chars=164,
                                )
                            ):
                                raise ValueError(
                                    "invalid storage filesystem_probe_error marker"
                                )
                            if filesystem_probe_error and not (
                                capacity_marker is False
                                and capacity_state == "unavailable"
                            ):
                                raise ValueError(
                                    "storage filesystem probe state contradicts marker"
                                )
                            total_bytes = candidate_storage_health.get(
                                "total_bytes"
                            )
                            filesystem_free_bytes = candidate_storage_health.get(
                                "filesystem_free_bytes"
                            )
                            filesystem_capacity_bytes = (
                                candidate_storage_health.get(
                                    "filesystem_capacity_bytes"
                                )
                            )
                            if (
                                total_bytes is not None
                                and filesystem_free_bytes is not None
                                and filesystem_capacity_bytes is not None
                                and filesystem_capacity_bytes
                                != total_bytes + filesystem_free_bytes
                            ):
                                raise ValueError(
                                    "inconsistent storage filesystem capacity"
                                )
                            if capacity_marker is True:
                                if any(
                                    candidate_storage_health.get(field) is None
                                    for field in _STORAGE_CAPACITY_PROOF_FIELDS
                                ):
                                    raise ValueError(
                                        "storage capacity proof is incomplete"
                                    )
                                max_storage_bytes = candidate_storage_health[
                                    "max_storage_bytes"
                                ]
                                projected_bytes = candidate_storage_health[
                                    "projected_required_bytes"
                                ]
                                required_bytes = candidate_storage_health[
                                    "required_capacity_bytes"
                                ]
                                if not (
                                    total_bytes <= max_storage_bytes
                                    and projected_bytes <= required_bytes
                                    and required_bytes <= max_storage_bytes
                                    and required_bytes
                                    <= filesystem_capacity_bytes
                                ):
                                    raise ValueError(
                                        "storage capacity proof is inconsistent"
                                    )
                            storage_health = candidate_storage_health
                        retention_ok = True
                        last_retention_error = ""
                        writer_root = getattr(self.writer, "root", None)
                        if writer_root is not None:
                            self._schedule_integrity_check(writer_root)
                    except Exception as exc:
                        retry_seconds = 300.0
                        retention_ok = False
                        retention_errors_total += 1
                        error_type, detail = self._exception_summary(exc)
                        last_retention_error = f"{error_type}: {detail}"
                        self._log_gap("retention gap", exc)
                    finally:
                        self._next_retention_check = now + retry_seconds
                try:
                    retry_root = getattr(self.writer, "root", None)
                    if retry_root is not None:
                        self._schedule_integrity_retry_if_due(
                            retry_root,
                            now=now,
                        )
                except Exception as exc:
                    self._log_gap("capture integrity retry gap", exc)
                try:
                    overview_error = None
                    if now >= next_overview or not self._universe:
                        try:
                            self.capture_overview()
                        except Exception as exc:
                            overview_error = exc
                            overview_errors_total += 1
                            error_type, detail = self._exception_summary(exc)
                            last_overview_error = f"{error_type}: {detail}"
                            if self._universe:
                                # Keep sampling the last validated universe,
                                # but do not hammer the overview endpoint once
                                # per micro cycle throughout a network outage.
                                retry_delay = max(
                                    self.micro_interval,
                                    min(self.overview_interval, 30.0),
                                )
                                next_overview = (
                                    time.monotonic() + retry_delay
                                )
                        else:
                            overview_captures_total += 1
                            last_overview_error = ""
                            next_overview = now + self.overview_interval
                            if l2_started:
                                try:
                                    updated = self._l2_collector.update_symbols(
                                        self._universe
                                    )
                                    if updated is not None and updated is not True:
                                        raise RuntimeError(
                                            "collector symbol update was not confirmed"
                                        )
                                except Exception as exc:
                                    note_l2_failure(exc)
                                    self._log_gap("L2 symbol update gap", exc)
                                else:
                                    note_l2_success()
                    micro_error = None
                    if self._universe:
                        symbol = self._universe[self._cursor % len(self._universe)]
                        self._cursor += 1
                        try:
                            self.capture_microstructure(symbol)
                        except Exception as exc:
                            micro_error = exc
                            microstructure_errors_total += 1
                            error_type, detail = self._exception_summary(exc)
                            last_microstructure_error = f"{error_type}: {detail}"
                        else:
                            microstructure_captures_total += 1
                            last_microstructure_error = ""
                    if overview_error is not None:
                        if micro_error is not None:
                            self._log_gap(
                                "microstructure gap after overview failure",
                                micro_error,
                            )
                        raise overview_error
                    if micro_error is not None:
                        raise micro_error
                    if not self._universe:
                        raise RuntimeError(
                            "venue recorder capture universe is empty"
                        )
                except Exception as exc:
                    capture_errors_consecutive += 1
                    capture_errors_total += 1
                    error_type, detail = self._exception_summary(exc)
                    last_capture_error = f"{error_type}: {detail}"
                    self._log_gap("capture gap", exc)
                else:
                    capture_errors_consecutive = 0
                    captures_total += 1
                    last_capture_success_wall_ts = time.time()
                    last_capture_error = ""
                try:
                    rest_data_health = self._rest_data_health()
                except Exception as exc:
                    self._log_gap("REST health synthesis gap", exc)
                    rest_data_health = {
                        "ok": False,
                        "stale_after_seconds": None,
                        "overview_stale_after_seconds": None,
                        "missing_or_invalid": ["REST_HEALTH:unavailable"],
                        "trade_audit_warnings": [],
                        "overview": {
                            "valid": False,
                            "age_seconds": None,
                            "flags": ["health_snapshot_unavailable"],
                        },
                        "markets": {},
                    }
                (
                    integrity_health,
                    integrity_errors_total,
                    last_integrity_error,
                ) = self._integrity_status()
                rest_ok = (
                    capture_errors_consecutive
                    < self.CAPTURE_FAILURE_THRESHOLD
                    and rest_data_health["ok"] is True
                )
                l2_enabled = self._l2_collector is not None
                l2_data_healthy = not l2_enabled
                trade_stream_healthy = not l2_enabled
                stream_health = {}
                collector_l2_errors_total = 0
                collector_l2_errors_consecutive = 0
                collector_last_l2_error = ""
                l2_probe_failed = False
                if l2_enabled:
                    try:
                        l2_data_healthy = read_l2_health_bool(
                            "is_healthy",
                            default=l2_started,
                        )
                    except Exception as exc:
                        l2_data_healthy = False
                        l2_probe_failed = True
                        note_l2_probe_failure(exc)
                        self._log_gap("L2 health read gap", exc)
                    try:
                        trade_stream_healthy = read_l2_health_bool(
                            "trades_healthy",
                            default=False,
                        )
                    except Exception as exc:
                        trade_stream_healthy = False
                        l2_probe_failed = True
                        note_l2_probe_failure(exc)
                        self._log_gap("trade stream health read gap", exc)
                    snapshot_missing = object()
                    try:
                        snapshot_marker = getattr(
                            self._l2_collector,
                            "health_snapshot",
                            snapshot_missing,
                        )
                    except Exception as exc:
                        snapshot_marker = snapshot_missing
                        l2_probe_failed = True
                        note_l2_probe_failure(exc)
                        self._log_gap("stream health snapshot gap", exc)
                    if snapshot_marker is not snapshot_missing:
                        try:
                            if not callable(snapshot_marker):
                                raise TypeError(
                                    "collector health_snapshot must be callable"
                                )
                            raw_stream_health = snapshot_marker()
                            if not isinstance(raw_stream_health, dict):
                                raise TypeError(
                                    "collector health_snapshot must return dict"
                                )
                            stream_health = {
                                key: dict.get(raw_stream_health, key)
                                for key in _STREAM_HEALTH_FIELDS
                                if dict.__contains__(raw_stream_health, key)
                            }
                            for connection_key in (
                                "connection_epoch",
                                "reconnect_attempts",
                            ):
                                if connection_key not in stream_health:
                                    continue
                                connection_value = stream_health.get(
                                    connection_key
                                )
                                if (
                                    type(connection_value) is not int
                                    or connection_value < 0
                                    or connection_value > _STREAM_HEALTH_COUNTER_MAX
                                ):
                                    raise ValueError(
                                        f"invalid {connection_key} health marker"
                                    )
                            if "connection_error_type" in stream_health:
                                connection_error = stream_health.get(
                                    "connection_error_type"
                                )
                                if not _plain_health_text(
                                    connection_error,
                                    max_chars=100,
                                    allow_none=True,
                                ):
                                    raise ValueError(
                                        "invalid connection_error_type health marker"
                                    )
                            if "trade_duplicates_suppressed" in stream_health:
                                duplicates_marker = stream_health.get(
                                    "trade_duplicates_suppressed"
                                )
                                if (
                                    type(duplicates_marker) is not int
                                    or duplicates_marker < 0
                                    or duplicates_marker
                                    > _STREAM_HEALTH_COUNTER_MAX
                                ):
                                    raise ValueError(
                                        "invalid trade_duplicates_suppressed "
                                        "health marker"
                                    )
                            if "last_transport_error" in stream_health:
                                last_transport_marker = stream_health.get(
                                    "last_transport_error"
                                )
                                if not _plain_health_text(
                                    last_transport_marker,
                                    max_chars=256,
                                    allow_none=True,
                                ):
                                    raise ValueError(
                                        "invalid last_transport_error health marker"
                                    )
                                if type(last_transport_marker) is str:
                                    stream_health["last_transport_error"] = (
                                        last_transport_marker
                                    )
                            if "last_interruption_wall_ts" in stream_health:
                                interruption_marker = stream_health.get(
                                    "last_interruption_wall_ts"
                                )
                                latest_interruption_wall_ts = (
                                    time.time()
                                    + MAX_EXCHANGE_FUTURE_SKEW_MS / 1000.0
                                )
                                valid_interruption = (
                                    interruption_marker is None
                                    or (
                                        type(interruption_marker) is int
                                        and 0
                                        <= interruption_marker
                                        <= latest_interruption_wall_ts
                                    )
                                    or (
                                        type(interruption_marker) is float
                                        and math.isfinite(interruption_marker)
                                        and 0.0
                                        <= interruption_marker
                                        <= latest_interruption_wall_ts
                                    )
                                )
                                if not valid_interruption:
                                    raise ValueError(
                                        "invalid last_interruption_wall_ts "
                                        "health marker"
                                    )
                            total_marker = stream_health.get(
                                "transport_errors_total", 0
                            )
                            consecutive_marker = stream_health.get(
                                "transport_errors_consecutive", 0
                            )
                            for counter_key, counter_value in (
                                ("transport_errors_total", total_marker),
                                (
                                    "transport_errors_consecutive",
                                    consecutive_marker,
                                ),
                            ):
                                if (
                                    type(counter_value) is not int
                                    or counter_value < 0
                                    or counter_value > _STREAM_HEALTH_COUNTER_MAX
                                ):
                                    raise ValueError(
                                        f"invalid {counter_key} health marker"
                                    )
                            if consecutive_marker > total_marker:
                                raise ValueError(
                                    "invalid transport_errors_consecutive "
                                    "health marker"
                                )
                            collector_l2_errors_total = total_marker
                            collector_l2_errors_consecutive = (
                                consecutive_marker
                            )
                            last_marker = stream_health.get(
                                "last_transport_error", ""
                            )
                            if isinstance(last_marker, str):
                                collector_last_l2_error = last_marker[:256]
                            for gap_key, stream_name in (
                                ("l2_missing_or_stale", "l2"),
                                ("trade_missing_or_stale", "trade"),
                            ):
                                if gap_key not in stream_health:
                                    continue
                                gap_marker = stream_health.get(gap_key)
                                gap_slice = slice(0, self.max_symbols + 1)
                                if isinstance(gap_marker, list):
                                    normalized_gap = list.__getitem__(
                                        gap_marker,
                                        gap_slice,
                                    )
                                elif isinstance(gap_marker, tuple):
                                    normalized_gap = list(
                                        tuple.__getitem__(gap_marker, gap_slice)
                                    )
                                else:
                                    normalized_gap = []
                                gap_valid = bool(
                                    isinstance(gap_marker, (list, tuple))
                                    and len(normalized_gap) <= self.max_symbols
                                    and all(
                                        _plain_health_text(
                                            item,
                                            max_chars=100,
                                        )
                                        and bool(item)
                                        and item == item.strip()
                                        for item in normalized_gap
                                    )
                                    and len(normalized_gap)
                                    == len(set(normalized_gap))
                                )
                                if not gap_valid:
                                    stream_health[gap_key] = []
                                    gap_error = ValueError(
                                        f"invalid {gap_key} health marker"
                                    )
                                    l2_probe_failed = True
                                    note_l2_probe_failure(gap_error)
                                    self._log_gap(
                                        "stream health snapshot gap",
                                        gap_error,
                                    )
                                    gap_active = True
                                else:
                                    stream_health[gap_key] = normalized_gap
                                    gap_active = bool(normalized_gap)
                                if stream_name == "l2" and gap_active:
                                    l2_data_healthy = False
                                elif stream_name == "trade" and gap_active:
                                    trade_stream_healthy = False
                        except Exception as exc:
                            stream_health = {}
                            l2_probe_failed = True
                            note_l2_probe_failure(exc)
                            self._log_gap("stream health snapshot gap", exc)
                    if not l2_probe_failed:
                        note_l2_probe_success()
                effective_l2_errors_consecutive = max(
                    l2_errors_consecutive,
                    l2_probe_errors_consecutive,
                    collector_l2_errors_consecutive,
                )
                effective_l2_errors_total = (
                    l2_errors_total
                    + l2_probe_errors_total
                    + collector_l2_errors_total
                )
                effective_last_l2_error = (
                    last_l2_error
                    or last_l2_probe_error
                    or collector_last_l2_error
                )
                l2_ok = not l2_enabled or (
                    l2_started
                    and effective_l2_errors_consecutive == 0
                    and l2_data_healthy
                )
                reason = (
                    "capture_error"
                    if capture_errors_consecutive >= self.CAPTURE_FAILURE_THRESHOLD
                    else "rest_market_gap"
                    if not rest_ok
                    else "l2_unavailable"
                    if not l2_ok
                    else "trade_stream_unavailable"
                    if not trade_stream_healthy
                    else "retention_error"
                    if not retention_ok
                    else "capture_integrity_error"
                    if integrity_health.get("ok") is not True
                    else "capture_capacity_insufficient"
                    if storage_health.get("capacity_ok") is False
                    else ""
                )
                self._report_health({
                    "ok": (
                        rest_ok
                        and l2_ok
                        and trade_stream_healthy
                        and retention_ok
                        and integrity_health.get("ok") is True
                        and storage_health.get("capacity_ok") is not False
                    ),
                    "reason": reason,
                    "rest_ok": rest_ok,
                    "rest_data_health": rest_data_health,
                    "l2_enabled": l2_enabled,
                    "l2_ok": l2_ok,
                    "l2_data_healthy": l2_data_healthy,
                    "trade_stream_healthy": trade_stream_healthy,
                    "stream_health": stream_health,
                    "consecutive_capture_errors": capture_errors_consecutive,
                    "capture_errors_total": capture_errors_total,
                    "captures_total": captures_total,
                    "overview_captures_total": overview_captures_total,
                    "overview_errors_total": overview_errors_total,
                    "microstructure_captures_total": (
                        microstructure_captures_total
                    ),
                    "microstructure_errors_total": (
                        microstructure_errors_total
                    ),
                    "last_capture_success_wall_ts": (
                        last_capture_success_wall_ts
                    ),
                    "last_capture_error": last_capture_error,
                    "last_overview_error": last_overview_error,
                    "last_microstructure_error": last_microstructure_error,
                    "retention_ok": retention_ok,
                    "retention_errors_total": retention_errors_total,
                    "last_retention_error": last_retention_error,
                    "integrity_health": dict(integrity_health),
                    "integrity_errors_total": integrity_errors_total,
                    "last_integrity_error": last_integrity_error,
                    "storage_health": dict(storage_health),
                    "l2_consecutive_errors": effective_l2_errors_consecutive,
                    "l2_errors_total": effective_l2_errors_total,
                    "last_l2_error": effective_last_l2_error,
                    "last_poll_monotonic": time.monotonic(),
                    "last_poll_wall_ts": time.time(),
                })
                shutdown_wait(self.micro_interval)
        finally:
            # No producer work occurs after this publication. External runtime
            # finalization may now close the shared writer without a reopen.
            with run_state_lock:
                self._run_admission_closed = True
                run_state["done"].set()
            self.shutdown_resources(
                timeout=max(15.0, self._INTEGRITY_STOP_TIMEOUT_SEC)
            )


# Compatibility for older imports and third-party extensions.
MexcVenueRecorder = VenueRecorder
