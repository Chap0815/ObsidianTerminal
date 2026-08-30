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
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import portalocker

from bot_utils.api_budget import (
    ApiCallReservation,
    record_api_error,
    try_consume_api_call,
)
from bot_utils.order_utils import order_id_text_or_none
from core.constants import NONCRYPTO_BASES


MAX_PARTITION_CLOCK_AGE_MS = 86_400_000
_CAPTURE_CONTROL_JSON_MAX_BYTES = 64 * 1024
_CAPACITY_OPERATIONAL_RESERVE_RATIO = 1.05


def _capture_now_ms() -> int:
    """Exchange-anchored absolute time for immutable capture provenance."""
    from core.clock import now_ms

    return int(now_ms())


def _capture_now_utc() -> datetime:
    from core.clock import now_utc

    return now_utc()


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


class SQLitePartitionWriter:
    """Deduplicated daily SQLite-WAL chunks; one file per stream and UTC day."""

    def __init__(
        self,
        root: str | Path,
        *,
        retention_days: int = 30,
        max_storage_gib: float = 150.0,
    ) -> None:
        self.root = Path(root)
        self.retention_days = max(1, int(retention_days))
        self.max_storage_bytes = max(0, int(float(max_storage_gib) * 1024**3))
        self._connections: dict[Path, sqlite3.Connection] = {}
        self._lock = threading.Lock()
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
        with self._lock:
            guard = self._partition_locks.setdefault(
                day_text, threading.RLock()
            )
        with guard:
            yield

    def _assert_partition_writable(self, path: Path) -> None:
        seal = self.root / "integrity" / f"{path.stem}.json"
        self._assert_scoped_path(seal)
        if seal.exists():
            raise RuntimeError(
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
            value = json.loads(raw.decode("utf-8-sig"))
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise RuntimeError("capture control state is invalid") from exc
        if not isinstance(value, dict) or value.get("schema_version") != 1:
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
        temporary = self.root / (
            f".{self._control_path.name}.{os.getpid()}."
            f"{threading.get_ident()}.tmp"
        )
        self._assert_scoped_path(temporary)
        try:
            with temporary.open("xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._control_path)
        finally:
            temporary.unlink(missing_ok=True)
        try:
            directory_fd = os.open(self.root, os.O_RDONLY)
        except (AttributeError, OSError):
            return
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def next_connection_epoch(self) -> int:
        """Return a process-independent monotonic transport epoch."""
        self.root.mkdir(parents=True, exist_ok=True)
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
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(
                text[:-1] + "+00:00" if text.endswith("Z") else text
            )
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)

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
            and exchange_time > received_time + timedelta(seconds=30)
        )
        if needs_fallback and received_time is not None:
            return cls._canonical_utc(received_time), True
        if exchange_time is not None:
            return cls._canonical_utc(exchange_time), needs_fallback
        return str(event.exchange_time), needs_fallback

    @classmethod
    def _day(cls, event: VenueEvent) -> str:
        storage_time, _fallback = cls._storage_exchange_time(event)
        parsed = cls._parse_event_time(storage_time)
        return parsed.strftime("%Y-%m-%d") if parsed is not None else "unknown-date"

    def _path(self, event: VenueEvent) -> Path:
        return self.root / event.kind / f"{self._day(event)}.sqlite3"

    @staticmethod
    def _valid_text(value, *, max_chars: int) -> bool:
        return bool(
            isinstance(value, str)
            and value
            and value == value.strip()
            and len(value) <= max_chars
            and all(ord(char) >= 32 and ord(char) != 127 for char in value)
        )

    @classmethod
    def _validate_event(cls, event: VenueEvent) -> None:
        """Reject malformed capture evidence before selecting a path."""
        if not isinstance(event, VenueEvent):
            raise ValueError("venue event type is invalid")
        if not cls._valid_text(event.event_id, max_chars=2048):
            raise ValueError("venue event id is invalid")
        if (
            not cls._valid_text(event.kind, max_chars=32)
            or event.kind[0] not in "abcdefghijklmnopqrstuvwxyz"
            or any(
                char not in "abcdefghijklmnopqrstuvwxyz0123456789_"
                for char in event.kind
            )
        ):
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
        if len(event.quality_flags) > 64:
            raise ValueError("venue event quality flags are invalid")
        if len(set(event.quality_flags)) != len(event.quality_flags):
            raise ValueError("venue event quality flags are invalid")
        if any(
            not cls._valid_text(flag, max_chars=128)
            for flag in event.quality_flags
        ):
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
        connection.execute("PRAGMA journal_mode=WAL")
        # Capture evidence favours durability over peak insert throughput.  In
        # WAL mode FULL syncs the WAL before a commit is acknowledged, so an
        # abrupt host/power loss cannot silently discard an already reported
        # successful sample.
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=15000")
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
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_venue_events_time "
            "ON venue_events(exchange_time, market_id)"
        )
        connection.commit()
        self._connections[path] = connection
        return connection

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
        path = self._path(event)
        self._assert_scoped_path(path)
        exchange_time, clock_fallback = self._storage_exchange_time(event)
        payload = json.dumps(
            event.payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        quality_flags = tuple(event.quality_flags)
        if (
            clock_fallback
            and "storage_exchange_time_fallback" not in quality_flags
        ):
            quality_flags = (*quality_flags, "storage_exchange_time_fallback")
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
                self._assert_partition_writable(path)
                connection = self._connection(path)
                try:
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
                    self._assert_partition_writable(path)
                    connection.commit()
                except Exception:
                    try:
                        connection.rollback()
                    except Exception:
                        # A failed rollback proves that this handle is not a
                        # safe transaction boundary anymore. Never mask the
                        # original write error and never reuse the handle on
                        # the next capture event.
                        try:
                            connection.close()
                        except Exception:
                            pass
                        finally:
                            if self._connections.get(path) is connection:
                                self._connections.pop(path, None)
                    raise
        return path

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

    def _stage_delete_day(self, day, paths: list[Path]) -> None:
        """Remove one expired UTC dataset without leaving active fragments."""
        staging = self.root / f".retention_trash-{day.isoformat()}"
        self._assert_scoped_path(staging)
        for path in paths:
            self._assert_scoped_path(path)
        if staging.exists():
            first_error = None
            for item in sorted(staging.iterdir()):
                try:
                    item.unlink()
                except OSError as exc:
                    if first_error is None:
                        first_error = exc
            if first_error is not None:
                raise first_error
            staging.rmdir()
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
        except OSError:
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
        current = (now or _capture_now_utc()).astimezone(timezone.utc)
        cutoff = (current - timedelta(days=self.retention_days)).date()
        self._assert_scoped_path(self._control_path)
        with self._lock:
            first_cleanup_error: OSError | sqlite3.Error | None = None
            failed_cleanup_paths: set[Path] = set()

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
            orphan_sidecars = sorted(
                set(self.root.glob("*/*.sqlite3-wal"))
                | set(self.root.glob("*/*.sqlite3-shm"))
            )
            for sidecar in orphan_sidecars:
                self._assert_scoped_path(sidecar)
                base_path = Path(str(sidecar)[:-4])
                if base_path.exists():
                    continue
                try:
                    sidecar.unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    if first_cleanup_error is None:
                        first_cleanup_error = exc

            partitions = sorted(self.root.glob("*/*.sqlite3"))
            sealed_reports = sorted((self.root / "integrity").glob("*.json"))
            expired_by_day: dict[object, list[Path]] = {}
            for path in (*partitions, *sealed_reports):
                partition_date = self._partition_date(path)
                if partition_date is not None and partition_date.date() < cutoff:
                    expired_by_day.setdefault(partition_date.date(), []).append(path)
            # Retention is a dataset operation, not a file operation.  Remove
            # every available stream of an expired UTC day together; never
            # trim one stream from a still-required day merely to meet quota.
            for day in sorted(expired_by_day):
                paths = sorted(expired_by_day[day])
                if any(path in failed_cleanup_paths for path in paths):
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
                for item in self.root.glob("*/*"):
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
        current = (now or _capture_now_utc()).astimezone(timezone.utc)
        daily_bytes: dict[str, int] = {}
        total = 0
        measurement_errors = 0
        for item in self.root.glob("*/*"):
            try:
                item_stat = item.lstat()
            except OSError:
                measurement_errors += 1
                continue
            if self._stat_is_linklike(item_stat):
                measurement_errors += 1
                continue
            if not stat.S_ISREG(item_stat.st_mode):
                continue
            size = item_stat.st_size
            total += size
            name = item.name.split(".sqlite3", 1)[0]
            try:
                datetime.strptime(name, "%Y-%m-%d")
            except ValueError:
                continue
            daily_bytes[name] = daily_bytes.get(name, 0) + size
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
        try:
            filesystem_free = max(0, int(shutil.disk_usage(self.root).free))
        except (OSError, TypeError, ValueError, OverflowError):
            filesystem_free = None
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
            "capacity_ok": capacity_ok,
            "capacity_state": capacity_state,
            "operational_reserve_ratio": (
                _CAPACITY_OPERATIONAL_RESERVE_RATIO
            ),
            "headroom_ratio": headroom_ratio,
        }

    def close(self) -> bool:
        with self._lock:
            for attempt in range(2):
                failed = False
                for path in list(self._connections):
                    try:
                        self._close_path(path)
                    except Exception:
                        failed = True
                if not failed:
                    return True
                if attempt == 0:
                    time.sleep(0.01)
            return False


class VenueRecorder:
    """Exchange-neutral overview, REST microstructure, and shadow L2 capture."""

    CAPTURE_FAILURE_THRESHOLD = 3
    _INTEGRITY_STOP_TIMEOUT_SEC = 15.0
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
        self.exchange = exchange
        self.writer = writer or SQLitePartitionWriter(
            root,
            retention_days=retention_days,
            max_storage_gib=max_storage_gib,
        )
        self.max_symbols = max(1, int(max_symbols))
        self.depth_levels = max(5, min(100, int(depth_levels)))
        self.micro_interval = max(1.0, float(micro_interval_seconds))
        self.overview_interval = max(self.micro_interval, float(overview_interval_seconds))
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
        self._integrity_health = {
            "ok": True,
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
        self.l2_mode = str(l2_mode).strip().lower()
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

    @staticmethod
    def _iso_now() -> str:
        return _capture_now_utc().isoformat().replace("+00:00", "Z")

    @staticmethod
    def _market_id(exchange, symbol: str) -> str:
        market = (getattr(exchange, "markets", None) or {}).get(symbol) or {}
        return str(market.get("id") or symbol)

    @staticmethod
    def _finite_number_or_none(value) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) else None

    def _is_noncrypto_swap(self, symbol: str) -> bool:
        market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
        base = str(market.get("base") or str(symbol).split("/", 1)[0])
        return base.strip().upper() in NONCRYPTO_BASES

    def _priority_symbols(self) -> list[str]:
        try:
            if self._priority_loader is None:
                from core.database import list_venue_capture_priorities

                requested = list_venue_capture_priorities(self.max_symbols)
            else:
                requested = self._priority_loader(self.max_symbols)
        except Exception as exc:
            self._log_gap("priority load gap", exc)
            return list(self._last_priority_symbols)
        markets = getattr(self.exchange, "markets", None) or {}
        resolved = []
        for raw in requested or ():
            candidate = str(raw or "").strip()
            symbol = candidate if candidate in markets else None
            if symbol is None:
                base = candidate.split("/", 1)[0].split("_", 1)[0].upper()
                symbol = next(
                    (
                        market_symbol
                        for market_symbol, market in markets.items()
                        if str((market or {}).get("base") or "").strip().upper() == base
                        and (market or {}).get("swap")
                        and (market or {}).get("quote") == "USDT"
                    ),
                    None,
                )
            market = markets.get(symbol) if symbol is not None else None
            if (
                symbol is None
                or not isinstance(market, dict)
                or not market.get("swap")
                or market.get("quote") != "USDT"
                or market.get("active") is False
                or self._is_noncrypto_swap(symbol)
                or symbol in resolved
            ):
                continue
            resolved.append(symbol)
            if len(resolved) >= self.max_symbols:
                break
        self._last_priority_symbols = list(resolved)
        return resolved

    def _log_gap(self, context: str, exc: Exception) -> None:
        if not self.log_event:
            return
        try:
            current = time.monotonic()
            context_key = str(context or "capture gap")[:120]
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
        detail = ""
        try:
            from core.logger import clean_user_text, redact

            detail = clean_user_text(redact(str(exc)), max_chars=240)
            detail = " ".join(detail.splitlines()).strip()
        except Exception:
            detail = ""
        try:
            self.log_event(
                f"Venue recorder {context}: {type(exc).__name__}"
                + (f": {detail}" if detail else ""),
                "WARN",
            )
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
    ) -> Path:
        market_id = market_id or self._market_id(self.exchange, symbol)
        if ended_ms < started_ms and "wallclock_non_monotonic" not in flags:
            flags = (*flags, "wallclock_non_monotonic")
        raw_event_clock = exchange_ms if exchange_ms is not None else ended_ms
        invalid_exchange_clock = False
        stale_exchange_clock = False
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
            exchange_time = datetime.fromtimestamp(
                event_clock / 1000, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            invalid_exchange_clock = exchange_ms is not None
        if (
            stale_exchange_clock
            and "stale_exchange_timestamp" not in flags
        ):
            flags = (*flags, "stale_exchange_timestamp")
        elif invalid_exchange_clock and "invalid_exchange_timestamp" not in flags:
            flags = (*flags, "invalid_exchange_timestamp")
        digest_input = json.dumps(payload, sort_keys=True, default=str)
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
                received_time=self._iso_now(),
                payload={
                    **payload,
                    "request_started_ms": started_ms,
                    "request_ended_ms": ended_ms,
                    "latency_ms": max(0, ended_ms - started_ms),
                    "universe": list(self._universe),
                },
                quality_flags=flags,
            )
        )
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
        invalid_tickers_payload = not isinstance(tickers, dict)
        ticker_rows = tickers if isinstance(tickers, dict) else {}

        def _number(value):
            nonlocal invalid_numeric_payload
            parsed = self._finite_number_or_none(value)
            if value is not None and parsed is None:
                invalid_numeric_payload = True
            return parsed

        quote_volumes: dict[str, float | None] = {}
        quote_volume_sources: dict[str, str] = {}

        for symbol, ticker in ticker_rows.items():
            market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
            if not market.get("swap") or market.get("quote") != "USDT":
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
            and (
                (getattr(self.exchange, "markets", None) or {})
                .get(symbol, {})
                .get("active")
                is not False
            )
        ]
        priority_universe = self._priority_symbols()
        self._universe = list(
            dict.fromkeys((*priority_universe, *volume_universe))
        )[: self.max_symbols]
        markets_payload = {}
        for _volume, symbol, ticker in candidates:
            info = ticker.get("info") if isinstance(ticker, dict) else {}
            info = info if isinstance(info, dict) else {}
            market = (getattr(self.exchange, "markets", None) or {}).get(symbol) or {}
            base = str(market.get("base") or "").strip()
            spot_symbol = f"{base}/USDT" if base else ""
            spot_market = (
                (getattr(self.exchange, "markets", None) or {}).get(spot_symbol)
                if spot_symbol
                else None
            )
            spot_available = bool(
                isinstance(spot_market, dict)
                and spot_market.get("spot")
                and spot_market.get("active") is not False
            )
            market_id = self._market_id(self.exchange, symbol)
            markets_payload[market_id] = {
                "symbol": symbol,
                "spot_symbol": spot_symbol or None,
                "spot_available": spot_available,
                "last": _number(ticker.get("last")),
                "bid": _number(ticker.get("bid")),
                "ask": _number(ticker.get("ask")),
                "quote_volume": quote_volumes[symbol],
                "quote_volume_source": quote_volume_sources[symbol],
                "hold_volume": _number(info.get("holdVol")),
                "index_price": _number(
                    info.get("indexPrice") or ticker.get("index")
                ),
                "fair_price": _number(
                    info.get("fairPrice") or ticker.get("mark")
                ),
                "funding_rate": _number(info.get("fundingRate")),
                "next_settle_time": _number(info.get("nextSettleTime")),
                "universe_member": symbol in self._universe,
                "capture_priority": symbol in priority_universe,
            }
        overview_flags = []
        if invalid_tickers_payload:
            overview_flags.append("invalid_tickers_payload")
        if invalid_numeric_payload:
            overview_flags.append("invalid_numeric_payload")
        latest_timestamp, invalid_exchange_timestamp = self._latest_timestamp(
            candidates, ended
        )
        if invalid_exchange_timestamp:
            overview_flags.append("invalid_exchange_timestamp")
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
        )
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
        flags = []
        try:
            from trading.l2_stream import normalize_order_book
            normalized_book = normalize_order_book(
                book,
                depth_levels=self.depth_levels,
            )
        except Exception as exc:
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
        if isinstance(trades, (list, tuple)):
            trade_rows = trades[:100]
            if len(trades) > 100:
                invalid_trade_payload = True
                truncated_trade_payload = True
        else:
            trade_rows = ()
        for trade in trade_rows:
            if not isinstance(trade, dict):
                invalid_trade_payload = True
                continue
            raw_trade_id = trade.get("id")
            trade_id = order_id_text_or_none(raw_trade_id)
            if raw_trade_id is not None and trade_id is None:
                invalid_trade_payload = True

            timestamp_value = self._finite_number_or_none(
                trade.get("timestamp")
            )
            price = self._finite_number_or_none(trade.get("price"))
            amount = self._finite_number_or_none(trade.get("amount"))
            if (
                timestamp_value is None
                or timestamp_value <= 0
                or not timestamp_value.is_integer()
                or timestamp_value > ended_trades + 86_400_000
                or price is None
                or price <= 0
                or amount is None
                or amount <= 0
            ):
                invalid_trade_payload = True
                continue
            timestamp = int(timestamp_value)
            side = (
                trade.get("side").strip().lower()
                if isinstance(trade.get("side"), str)
                else None
            )
            if side not in {"buy", "sell"}:
                side = None
                invalid_trade_payload = True
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
            trade_flags.append("invalid_trade_payload")
        if conflicting_trade_id:
            trade_flags.append("conflicting_trade_id")
        if truncated_trade_payload:
            trade_flags.append("truncated_trade_payload")
        # The MEXC swap endpoint is a latest-N window.  A full page cannot
        # prove that no older trade was displaced between polls, even though
        # CCXT correctly returns no more than the requested limit.
        if isinstance(trades, (list, tuple)) and len(trades) >= 100:
            trade_flags.append("saturated_trade_payload")
        trades_path = self._write_event(
            "trades",
            symbol,
            {"trades": normalized},
            exchange_ms=max((row["timestamp"] for row in normalized), default=None),
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

    def _run_integrity_check(self, writer_root: Path) -> None:
        try:
            from trading.capture_integrity import seal_closed_capture_days

            health = seal_closed_capture_days(
                writer_root,
                max_symbols=self.max_symbols,
                micro_interval_seconds=self.micro_interval,
                l2_sample_interval_seconds=(
                    self._l2_collector.sample_interval
                    if self._l2_collector is not None
                    else 1.0
                ),
                partition_guard=getattr(
                    self.writer, "partition_guard", None
                ),
                verification_cache=self._integrity_verification_cache,
            )
        except Exception as exc:
            incident_key = (
                "exception",
                type(exc).__name__,
                str(exc)[:160],
            )
            with self._integrity_lock:
                if incident_key != self._integrity_incident_key:
                    self._integrity_errors_total += 1
                    self._integrity_incident_key = incident_key
                self._last_integrity_error = (
                    f"{type(exc).__name__}: {str(exc)[:160]}"
                )
                self._integrity_health = {
                    **self._integrity_health,
                    "ok": False,
                }
            self._log_gap("capture integrity gap", exc)
            return
        with self._integrity_lock:
            self._integrity_health = dict(health)
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
            if self._integrity_thread and self._integrity_thread.is_alive():
                return
            thread = threading.Thread(
                target=self._run_integrity_check,
                args=(Path(writer_root),),
                daemon=True,
                name="VenueCaptureIntegrity",
            )
            self._integrity_thread = thread
        thread.start()

    def _integrity_status(self) -> tuple[dict, int, str]:
        with self._integrity_lock:
            health = dict(self._integrity_health)
            thread = self._integrity_thread
            if thread is not None and thread.is_alive():
                health["worker_running"] = True
            return (
                health,
                self._integrity_errors_total,
                self._last_integrity_error,
            )

    def _wait_for_integrity_worker(self, *, timeout: float) -> bool:
        """Return only after the seal worker is quiescent or timeout expires."""
        with self._integrity_lock:
            thread = self._integrity_thread
        if thread is None:
            return True
        try:
            if thread.is_alive():
                thread.join(timeout=max(0.0, float(timeout)))
            return not thread.is_alive()
        except Exception:
            return False

    def run(self, shutdown_event: threading.Event) -> None:
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
            last_l2_error = f"{type(exc).__name__}: {str(exc)[:160]}"

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
            last_l2_probe_error = (
                f"{type(exc).__name__}: {str(exc)[:160]}"
            )

        def note_l2_probe_success() -> None:
            nonlocal l2_probe_errors_consecutive, last_l2_probe_error
            l2_probe_errors_consecutive = 0
            last_l2_probe_error = ""

        try:
            if self._l2_collector is not None:
                try:
                    self._l2_collector.start(shutdown_event)
                    l2_started = True
                    note_l2_success()
                except Exception as exc:
                    note_l2_failure(exc)
                    next_l2_start_attempt = time.monotonic() + 30.0
                    self._log_gap("L2 start gap", exc)
            while not shutdown_event.is_set():
                now = time.monotonic()
                if self._l2_collector is not None:
                    alive_marker = getattr(
                        self._l2_collector, "is_alive", None
                    )
                    if l2_started and alive_marker is not None:
                        alive_error = None
                        try:
                            collector_alive = bool(
                                alive_marker()
                                if callable(alive_marker)
                                else alive_marker
                            )
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
                        and not shutdown_event.is_set()
                    ):
                        try:
                            self._l2_collector.start(shutdown_event)
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
                                    self._l2_collector.update_symbols(
                                        self._universe
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
                            storage_health = dict(storage_marker())
                        retention_ok = True
                        last_retention_error = ""
                        writer_root = getattr(self.writer, "root", None)
                        if writer_root is not None:
                            self._schedule_integrity_check(writer_root)
                    except Exception as exc:
                        retry_seconds = 300.0
                        retention_ok = False
                        retention_errors_total += 1
                        last_retention_error = (
                            f"{type(exc).__name__}: {str(exc)[:160]}"
                        )
                        self._log_gap("retention gap", exc)
                    finally:
                        self._next_retention_check = now + retry_seconds
                try:
                    overview_error = None
                    if now >= next_overview or not self._universe:
                        try:
                            self.capture_overview()
                        except Exception as exc:
                            overview_error = exc
                            overview_errors_total += 1
                            last_overview_error = (
                                f"{type(exc).__name__}: {str(exc)[:160]}"
                            )
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
                                    self._l2_collector.update_symbols(self._universe)
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
                            last_microstructure_error = (
                                f"{type(exc).__name__}: {str(exc)[:160]}"
                            )
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
                    last_capture_error = (
                        f"{type(exc).__name__}: {str(exc)[:160]}"
                    )
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
                    healthy_marker = getattr(
                        self._l2_collector, "is_healthy", None
                    )
                    if healthy_marker is None:
                        l2_data_healthy = l2_started
                    else:
                        try:
                            l2_data_healthy = bool(
                                healthy_marker()
                                if callable(healthy_marker)
                                else healthy_marker
                            )
                        except Exception as exc:
                            l2_data_healthy = False
                            l2_probe_failed = True
                            note_l2_probe_failure(exc)
                            self._log_gap("L2 health read gap", exc)
                    trade_marker = getattr(
                        self._l2_collector, "trades_healthy", False
                    )
                    try:
                        trade_stream_healthy = bool(
                            trade_marker()
                            if callable(trade_marker)
                            else trade_marker
                        )
                    except Exception as exc:
                        trade_stream_healthy = False
                        l2_probe_failed = True
                        note_l2_probe_failure(exc)
                        self._log_gap("trade stream health read gap", exc)
                    snapshot_marker = getattr(
                        self._l2_collector, "health_snapshot", None
                    )
                    if callable(snapshot_marker):
                        try:
                            stream_health = dict(snapshot_marker())
                            total_marker = stream_health.get(
                                "transport_errors_total", 0
                            )
                            consecutive_marker = stream_health.get(
                                "transport_errors_consecutive", 0
                            )
                            if (
                                isinstance(total_marker, int)
                                and not isinstance(total_marker, bool)
                                and total_marker >= 0
                            ):
                                collector_l2_errors_total = total_marker
                            if (
                                isinstance(consecutive_marker, int)
                                and not isinstance(consecutive_marker, bool)
                                and consecutive_marker >= 0
                            ):
                                collector_l2_errors_consecutive = (
                                    consecutive_marker
                                )
                            last_marker = stream_health.get(
                                "last_transport_error", ""
                            )
                            if isinstance(last_marker, str):
                                collector_last_l2_error = last_marker[:256]
                        except Exception as exc:
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
                shutdown_event.wait(self.micro_interval)
        finally:
            producers_quiescent = True
            if self._l2_collector is not None:
                try:
                    stopped = self._l2_collector.stop(timeout=15.0)
                    producers_quiescent = stopped is not False
                    if not producers_quiescent:
                        raise RuntimeError(
                            "capture producer remains alive after shutdown timeout"
                        )
                except Exception as exc:
                    producers_quiescent = False
                    self._log_gap("L2 stop gap", exc)
            integrity_quiescent = self._wait_for_integrity_worker(
                timeout=self._INTEGRITY_STOP_TIMEOUT_SEC,
            )
            if not integrity_quiescent:
                producers_quiescent = False
                self._log_gap(
                    "capture integrity stop gap",
                    RuntimeError(
                        "integrity worker remains alive after shutdown timeout"
                    ),
                )
            close = getattr(self.writer, "close", None)
            if producers_quiescent and callable(close):
                try:
                    closed = close()
                    if closed is False:
                        raise RuntimeError(
                            "partition writer connections remain open"
                        )
                except Exception as exc:
                    self._log_gap("writer close gap", exc)


# Compatibility for older imports and third-party extensions.
MexcVenueRecorder = VenueRecorder
