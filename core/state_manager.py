"""
state_manager.py  SQLite-backed open-position state.

Disk I/O runs OUTSIDE the lock (snapshot under lock, write after) so a read
never blocks on a write. JSON is written by a single dedicated writer thread
per path; reconciliation is robust against schema drift.
"""
from __future__ import annotations

import json
import math
import os
import re
import shutil
import sqlite3
import threading
import time
from typing import Dict, Optional

from core.models import Position


# Robust coin-key regex: only A-Z0-9, 2-15 chars
_COIN_RE = re.compile(r"^[A-Z0-9]{2,15}$")
_STATE_JSON_MAX_BYTES = 4 * 1024 * 1024

# Assets that are NEVER bot positions  the quote currency (USDT cash),
# stablecoins, and exchange tokens (MEXC's MX) held in the account. Without
# this filter, reconcile spams them as phantom "orphan positions on exchange"
# (e.g. USDT/USDT = your cash). Extend via RECONCILE_IGNORE_ASSETS env.
_RECONCILE_IGNORE = {"USDT", "USDC", "USD", "DAI", "TUSD", "FDUSD", "BUSD", "MX"}
try:
    _RECONCILE_IGNORE |= {a.strip().upper() for a in
                          os.getenv("RECONCILE_IGNORE_ASSETS", "").split(",") if a.strip()}
except Exception:
    pass


def _canonical_json_path(path: str) -> str:
    return os.path.realpath(os.path.abspath(os.path.normpath(os.fspath(path))))


# 
# Shared-connection wrapper
# 

class _SharedConnWrapper:
    """Wraps a long-lived shared sqlite3.Connection so legacy callers
    that do ``conn.close()`` don't actually close it. Everything else
    passes through to the real connection.

    The wrapper is intentionally thin  we only override ``close``.
    Using ``__getattr__`` to forward all other access keeps it safe
    against future API additions on sqlite3.Connection.
    """
    __slots__ = ("_real",)

    def __init__(self, real_conn: sqlite3.Connection):
        object.__setattr__(self, "_real", real_conn)

    def close(self) -> None:
        # No-op: the shared connection lives for the thread's lifetime.
        return None

    def __getattr__(self, name: str):
        return getattr(self._real, name)

    # Pass-through context-manager protocol so ``with conn:`` blocks
    # still commit/rollback as expected. sqlite3.Connection's __enter__
    # returns the connection itself; we want to return THIS wrapper so
    # the user can keep doing ``with conn: conn.close()``.
    def __enter__(self):
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._real.__exit__(exc_type, exc, tb)


# 
# Single-writer JSON
# 

class _SingleWriterJSON:
    """One dedicated writer thread per JSON path. submit() replaces pending
    payload, so under load disk never has more than one in-flight write."""

    _instances: Dict[str, "_SingleWriterJSON"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def for_path(cls, path: str) -> "_SingleWriterJSON":
        canonical_path = _canonical_json_path(path)
        path_key = os.path.normcase(canonical_path)
        with cls._instances_lock:
            inst = cls._instances.get(path_key)
            if inst is None:
                inst = cls(canonical_path)
                cls._instances[path_key] = inst
            return inst

    def __init__(self, path: str):
        self.path = path
        self._latest: Optional[tuple[int, dict]] = None
        self._cv = threading.Condition()
        self._write_lock = threading.Lock()
        self._seq = 0
        self._floor_rev = 0
        self._stop = False
        self._thread = threading.Thread(
            target=self._run, daemon=True,
            name=f"state-json-{os.path.basename(path)}",
        )
        self._thread.start()

    def _next_revision_locked(self) -> int:
        highest = max(self._seq, self._floor_rev)
        if self._latest is not None:
            highest = max(highest, self._latest[0])
        self._seq = highest + 1
        return self._seq

    def submit(self, payload: dict, rev: int | None = None) -> None:
        with self._cv:
            if rev is None:
                rev = self._next_revision_locked()
            else:
                self._seq = max(self._seq, rev)
            if rev < self._floor_rev:
                return
            self._latest = (rev, payload)
            self._cv.notify()

    def write_now(self, payload: dict, rev: int | None = None) -> bool:
        with self._cv:
            if rev is None:
                rev = self._next_revision_locked()
            else:
                self._seq = max(self._seq, rev)
            self._floor_rev = max(self._floor_rev, rev)
            # Drop queued older snapshots so they cannot overwrite this
            # critical synchronous write after a close/remove.
            self._latest = None
        try:
            with self._write_lock:
                self._write_atomic(payload)
            return True
        except Exception as e:
            try:
                from core.logger import log_event
                log_event(f"[StateManager] JSON write failed for "
                          f"{self.path}: {e}", "WARN")
            except Exception:
                pass
            return False

    def _run(self) -> None:
        while not self._stop:
            with self._cv:
                while self._latest is None and not self._stop:
                    self._cv.wait(timeout=2.0)
                item = self._latest
                self._latest = None
            if item is None:
                continue
            rev, payload = item
            retry_delay = 0.1
            last_warning_at = 0.0
            while not self._stop:
                with self._cv:
                    if rev < self._floor_rev:
                        break
                    pending = self._latest
                    # A same/newer complete snapshot supersedes this failed
                    # attempt. The outer loop will persist that generation.
                    if pending is not None and pending[0] >= rev:
                        break
                try:
                    with self._write_lock:
                        with self._cv:
                            if rev < self._floor_rev:
                                break
                        self._write_atomic(payload)
                    # Once a revision is durable, a late older submit must not
                    # be able to overwrite it.
                    with self._cv:
                        self._floor_rev = max(self._floor_rev, rev)
                    break
                except Exception as e:
                    now = time.monotonic()
                    if last_warning_at == 0.0 or now - last_warning_at >= 60.0:
                        last_warning_at = now
                        try:
                            from core.logger import log_event
                            log_event(
                                f"[StateManager] JSON write failed for "
                                f"{self.path}: {e}; retrying",
                                "WARN",
                            )
                        except Exception:
                            pass
                    # Keep the latest full snapshot alive across transient
                    # Windows reader locks/disk contention. Condition.wait()
                    # wakes immediately for a newer submit or shutdown.
                    with self._cv:
                        if self._stop or rev < self._floor_rev:
                            break
                        pending = self._latest
                        if pending is not None and pending[0] >= rev:
                            break
                        self._cv.wait(timeout=retry_delay)
                    retry_delay = min(2.0, retry_delay * 2.0)

    def _write_atomic(self, payload: dict) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = (
            f"{self.path}.tmp.{os.getpid()}."
            f"{threading.get_ident()}.{time.monotonic_ns()}"
        )
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(
                    payload,
                    fh,
                    indent=2,
                    ensure_ascii=False,
                    allow_nan=False,
                )
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except (AttributeError, OSError):
                    pass
            for _ in range(8):
                try:
                    os.replace(tmp, self.path)
                    return
                except PermissionError:
                    time.sleep(0.05)
            # All retries exhausted: surface the error to the single writer,
            # which keeps the latest complete snapshot queued for bounded,
            # rate-limited retry until contention clears or a newer revision
            # supersedes it.
            raise OSError(
                f"os.replace failed after 8 retries for {self.path}")
        finally:
            # cleanup leftover tmp on any exit path
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass


# 
# StateManager
# 

class StateManager:
    def __init__(self, bot_name: str, log_dir: str, json_path: str,
                 db_path: str = None, write_json: bool = True):
        """
        db_path defaults to None  resolved to core.paths.DB_PATH_STR (an
        absolute path, so the DB is never duplicated depending on cwd).

        Schema init is unconditional: init_db() is called once here regardless
        of how this StateManager was constructed. init_db() is idempotent
        (single-process guard + advisory DB lock), which avoids a
        "no such table: bot_open_positions" race against the bot's own init_db()
        on the first start.
        """
        if db_path is None:
            try:
                from core.paths import DB_PATH_STR
                db_path = DB_PATH_STR
            except ImportError:
                # Compute an absolute path from this file's own location so a
                # chdir() during early boot can't relocate the DB.
                _here = os.path.dirname(os.path.abspath(__file__))
                _root = os.path.dirname(_here)
                db_path = os.path.join(_root, "data", "trading_bot.db")
        self.bot_name   = bot_name
        self.log_dir    = log_dir
        self.json_path  = _canonical_json_path(json_path)
        self.db_path    = db_path
        self.write_json = write_json
        self.lock       = threading.Lock()
        self._persist_lock = threading.Lock()
        self._persist_rev = 0
        self._positions: Dict[str, Position] = {}
        self._sqlite_load_available = True
        # Ensure schema BEFORE any load() / save() can be called
        try:
            from core.database import init_db
            init_db()
        except Exception as _e:
            # Best-effort; the defensive retry in _load_from_sqlite catches
            # the "no such table" case too.
            try:
                self._warn(f"StateManager.__init__: init_db failed: {_e}")
            except Exception:
                pass
        if write_json:
            self._json_writer = _SingleWriterJSON.for_path(self.json_path)
        else:
            self._json_writer = None

    def load(self) -> Dict[str, Position]:
        db_pos   = self._load_from_sqlite()
        if not self._sqlite_load_available:
            raise RuntimeError(
                f"[{self.bot_name}] SQLite state unavailable; refusing "
                "JSON migration"
            )
        json_pos = self._load_from_json()

        merged: Dict[str, Position] = {}
        merged.update(db_pos)

        migrated, only_in_db = [], []
        for sym, raw in json_pos.items():
            if sym in merged:
                continue
            try:
                pos = Position.from_dict(
                    {**raw, "bot_name": self.bot_name, "symbol": sym}
                )
                merged[sym] = pos
                migrated.append(sym)
            except Exception as e:
                self._warn(f"Skip malformed JSON position {sym}: {e}")

        for sym in db_pos:
            if sym not in json_pos:
                only_in_db.append(sym)

        if only_in_db:
            self._warn(
                f"Crash-recovered from SQLite (not in JSON): {only_in_db}")
        if migrated:
            self._warn(f"One-time migration JSONSQLite: {migrated}")

        if migrated and not self._write_sqlite_bulk(merged):
            migrated_set = set(migrated)
            for sym in migrated_set:
                merged.pop(sym, None)
            self._warn(
                f"JSONSQLite migration failed; ignored unclaimed JSON-only "
                f"position(s): {sorted(migrated_set)}")

        with self.lock:
            self._positions = merged
            self._persist_rev += 1

        if self.write_json and self._json_writer:
            self._json_writer.submit(
                {s: p.to_dict() for s, p in merged.items()},
            )

        return dict(self._positions)

    def add(self, position: Position) -> None:
        # snapshot under lock, I/O outside
        with self.lock:
            previous = self._positions.get(position.symbol)
            self._positions[position.symbol] = position
            snapshot = {s: p.to_dict() for s, p in self._positions.items()}
            rev = self._persist_rev = self._persist_rev + 1
        # Disk I/O outside lock
        persisted = True
        superseded = False
        with self._persist_lock:
            if self._is_current_revision(rev):
                persisted = self._write_sqlite_single(position)
                if persisted and self.write_json and self._json_writer:
                    # Writer revisions are path-global. A StateManager is
                    # recreated on an in-process bot restart and its local
                    # persistence revision starts over, so passing ``rev``
                    # here could make the surviving per-path writer reject a
                    # valid new-session snapshot as stale.
                    self._json_writer.submit(snapshot)
            else:
                superseded = True
        if superseded:
            return
        if not persisted:
            with self.lock:
                if self._persist_rev != rev:
                    # A newer mutation owns the next durable write. Never let
                    # this older failed generation roll it back in memory.
                    return
                current = self._positions.get(position.symbol)
                if current is position:
                    if previous is None:
                        self._positions.pop(position.symbol, None)
                    else:
                        self._positions[position.symbol] = previous
            return
        self._emit("POSITION_OPENED", position)

    def update(self, symbol: str, **kwargs) -> Optional[Position]:
        """Capture snapshot under lock, write outside."""
        _alias = {"buy": "buy_price", "highest": "highest_price"}
        live_pos = None
        persisted_pos = None
        snapshot = None
        old_pos = None
        with self.lock:
            pos = self._positions.get(symbol)
            if pos is None:
                return None
            old_pos = Position.from_dict(pos.to_dict())
            for k, v in kwargs.items():
                attr = _alias.get(k, k)
                if hasattr(pos, attr):
                    setattr(pos, attr, v)
            live_pos = pos
            # Freeze this exact revision. A concurrent later update mutates the
            # live Position object before it can acquire _persist_lock; passing
            # that shared object into SQLite would let the newer generation
            # bleed into this older durable write.
            persisted_pos = Position.from_dict(pos.to_dict())
            snapshot     = {s: p.to_dict() for s, p in self._positions.items()}
            rev = self._persist_rev = self._persist_rev + 1
        # Disk I/O OUTSIDE lock
        persisted = True
        with self._persist_lock:
            if self._is_current_revision(rev):
                persisted = self._write_sqlite_single(persisted_pos)
                if persisted and self.write_json and self._json_writer:
                    self._json_writer.submit(snapshot)
        if not persisted:
            with self.lock:
                if (
                    self._persist_rev == rev
                    and self._positions.get(symbol) is live_pos
                ):
                    self._positions[symbol] = old_pos
                    self._persist_rev += 1
            return None
        return live_pos

    def remove(self, symbol: str) -> Optional[Position]:
        with self.lock:
            pos = self._positions.pop(symbol, None)
            snapshot = {s: p.to_dict() for s, p in self._positions.items()}
            rev = self._persist_rev = self._persist_rev + 1
        persisted = True
        sqlite_deleted = False
        with self._persist_lock:
            if self._is_current_revision(rev):
                deleted = self._delete_sqlite(symbol)
                # Backwards-compatible for tests/legacy monkeypatches from
                # the old void-return helper; real _delete_sqlite now returns
                # False only on confirmed persistence failure.
                sqlite_deleted = True if deleted is None else bool(deleted)
                persisted = sqlite_deleted
                if persisted and self.write_json and self._json_writer:
                    if hasattr(self._json_writer, "write_now"):
                        persisted = self._json_writer.write_now(snapshot)
                    else:
                        self._json_writer.submit(snapshot)
                if not persisted and pos is not None:
                    restored = False
                    with self.lock:
                        if self._persist_rev == rev:
                            self._positions[symbol] = pos
                            self._persist_rev += 1
                            restored = True
                    # A JSON failure follows a committed SQLite delete. Restore
                    # that row while still holding the persistence lock, so a
                    # newer generation can only write durable state afterwards.
                    if restored and sqlite_deleted:
                        self._write_sqlite_single(pos)
        if not persisted:
            # If a newer generation superseded this remove, it owns both the
            # current in-memory value and the next serialized durable write.
            if pos is not None:
                return None
            return None
        if pos:
            self._emit("POSITION_CLOSED", pos)
        return pos

    def get(self, symbol: str) -> Optional[Position]:
        with self.lock:
            return self._positions.get(symbol)

    def all(self) -> Dict[str, Position]:
        with self.lock:
            return dict(self._positions)

    def count(self) -> int:
        with self.lock:
            return len(self._positions)

    def save_snapshot(self) -> None:
        with self.lock:
            snapshot = dict(self._positions)
            rev = self._persist_rev
        with self._persist_lock:
            if self._is_current_revision(rev):
                persisted = self._write_sqlite_bulk(snapshot)
                if persisted and self.write_json and self._json_writer:
                    self._json_writer.submit(
                        {s: p.to_dict() for s, p in snapshot.items()},
                    )

    def _is_current_revision(self, rev: int) -> bool:
        with self.lock:
            return rev == self._persist_rev

    # 
    # SQLite
    # 

    def _get_conn(self) -> sqlite3.Connection:
        """Route through ``core.database.get_connection``, which keeps a
        thread-local cached connection with WAL + busy_timeout configured
        (reused across calls, no per-call fd churn).

        Callers must NOT close this connection  it's shared. The wrapper
        returns a connection whose .close() is a no-op so legacy callers that
        do ``conn.close()`` don't break.
        """
        try:
            from core.database import get_connection
            real = get_connection()
            return _SharedConnWrapper(real)
        except Exception:
            # Fallback: own dedicated connection, if core.database isn't
            # importable.
            conn = sqlite3.connect(self.db_path, timeout=20.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=20000")
            return conn

    def _ensure_schema(self) -> None:
        """Trigger init_db() so the schema exists before we read  covers the
        case where bot_open_positions doesn't exist yet (first start, or the DB
        was deleted) and load() runs before any other init_db() call.
        """
        try:
            from core.database import init_db
            init_db()
        except Exception as e:
            self._warn(f"_ensure_schema: init_db failed: {e}")

    def _load_from_sqlite(self) -> Dict[str, Position]:
        result = {}
        self._sqlite_load_available = False
        try:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM bot_open_positions WHERE bot_name=?",
                    (self.bot_name,)
                ).fetchall()
            except sqlite3.OperationalError as oe:
                # Most likely "no such table: bot_open_positions"  schema
                # hasn't been created yet. Initialize via core.database.init_db
                # and retry once. After that, real errors propagate.
                if "no such table" in str(oe).lower():
                    conn.close()
                    self._ensure_schema()
                    conn = self._get_conn()
                    rows = conn.execute(
                        "SELECT * FROM bot_open_positions WHERE bot_name=?",
                        (self.bot_name,)
                    ).fetchall()
                else:
                    raise
            conn.close()
            from core.database import _strict_claim_extra_object

            for row in rows:
                d   = dict(row)
                sym = d.get("symbol", "")
                if not sym:
                    continue
                extra = _strict_claim_extra_object(d.pop("extra_json", None))
                if extra is None:
                    raise ValueError(
                        f"malformed SQLite recovery metadata for {sym}"
                    )
                protected = self._BASE_COLS | {
                    "buy",
                    "highest",
                    "opened_at",
                }
                d.update(
                    key_value
                    for key_value in extra.items()
                    if key_value[0] not in protected
                )
                try:
                    pos = Position.from_dict({**d, "bot_name": self.bot_name})
                    money_error = self._normalize_required_money(pos)
                    if money_error is not None:
                        self._warn(
                            f"Skip malformed SQLite position {sym}: "
                            f"{money_error}"
                        )
                        continue
                    result[sym] = pos
                except Exception as e:
                    self._warn(f"Skip malformed SQLite position {sym}: {e}")
            self._sqlite_load_available = True
        except Exception as e:
            self._warn(f"SQLite load error: {e}")
        return result

    _BASE_COLS = frozenset({
        "bot_name", "symbol", "buy_price", "buy_time", "amount",
        "invested_usdt", "position_type", "leverage", "state",
        "rsi_15m", "rsi_1h", "rsi_4h", "change_pct",
        "btc_trend", "fear_greed",
    })

    @staticmethod
    def _claim_base(symbol: str) -> str:
        text = str(symbol or "").upper()
        return text.split("/")[0].split(":")[0].strip()

    @staticmethod
    def _is_futures_type(position_type: str) -> bool:
        return str(position_type or "").upper() != "SPOT"

    def _blocked_by_other_owner(self, conn, pos: Position) -> bool:
        base = self._claim_base(pos.symbol)
        if not base:
            return False
        is_futures = self._is_futures_type(pos.position_type.value)
        rows = conn.execute(
            "SELECT bot_name, symbol, position_type FROM bot_open_positions "
            "WHERE bot_name != ?",
            (pos.bot_name,),
        ).fetchall()
        for row in rows:
            other = dict(row)
            if self._claim_base(other.get("symbol")) != base:
                continue
            if self._is_futures_type(other.get("position_type")) == is_futures:
                self._warn(
                    f"SQLite write blocked: {pos.symbol} already owned by "
                    f"{other.get('bot_name')}"
                )
                return True
        return False

    @staticmethod
    def _normalize_required_money(pos: Position) -> str | None:
        parsed = {}
        for field_name in ("buy_price", "amount", "invested_usdt", "leverage"):
            raw = getattr(pos, field_name)
            if isinstance(raw, bool):
                return f"{field_name} is boolean"
            try:
                value = float(raw)
            except (TypeError, ValueError, OverflowError):
                return f"{field_name} is not numeric"
            if not math.isfinite(value):
                return f"{field_name} is not finite"
            parsed[field_name] = value
        if parsed["buy_price"] <= 0.0:
            return "buy_price must be positive"
        if parsed["amount"] <= 0.0:
            return "amount must be positive"
        if parsed["invested_usdt"] < 0.0:
            return "invested_usdt must be non-negative"
        if parsed["leverage"] <= 0.0:
            return "leverage must be positive"
        for field_name, value in parsed.items():
            setattr(pos, field_name, value)
        return None

    def _upsert_row(self, conn, pos: Position) -> bool:
        if pos.bot_name != self.bot_name:
            self._warn(
                f"SQLite write blocked: position owner {pos.bot_name!r} "
                f"does not match manager owner {self.bot_name!r}"
            )
            return False
        money_error = self._normalize_required_money(pos)
        if money_error is not None:
            self._warn(
                f"SQLite write blocked for {pos.symbol}: {money_error}"
            )
            return False
        if self._blocked_by_other_owner(conn, pos):
            return False
        d     = pos.to_dict()
        # Sanitize inf/nan in extra before serialization
        extra = {}
        for k, v in d.items():
            if k in self._BASE_COLS or k in ("buy", "highest", "opened_at"):
                continue
            if isinstance(v, float):
                import math as _math
                if _math.isnan(v) or _math.isinf(v):
                    v = 0.0
            extra[k] = v

        conn.execute("""
        INSERT INTO bot_open_positions
            (bot_name, symbol, buy_price, buy_time, amount, invested_usdt,
             position_type, leverage, state,
             rsi_15m, rsi_1h, rsi_4h, change_pct,
             btc_trend, fear_greed, extra_json, opened_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(bot_name, symbol) DO UPDATE SET
            buy_price     = excluded.buy_price,
            -- Keep the original buy_time, matching opened_at. Reconcile and
            -- partial-update calls must not reset the entry timestamp because
            -- PnL analytics use it for "time open" calculations.
            -- buy_time      = excluded.buy_time,
            amount        = excluded.amount,
            invested_usdt = excluded.invested_usdt,
            position_type = excluded.position_type,
            leverage      = excluded.leverage,
            state         = excluded.state,
            rsi_15m       = excluded.rsi_15m,
            rsi_1h        = excluded.rsi_1h,
            rsi_4h        = excluded.rsi_4h,
            change_pct    = excluded.change_pct,
            btc_trend     = excluded.btc_trend,
            fear_greed    = excluded.fear_greed,
            extra_json    = excluded.extra_json
            -- Keep opened_at stable, otherwise PnL analytics are wrong.
        """, (
            pos.bot_name, pos.symbol, pos.buy_price, pos.buy_time,
            pos.amount, pos.invested_usdt,
            pos.position_type.value, pos.leverage, pos.state.value,
            pos.rsi_15m, pos.rsi_1h, pos.rsi_4h,
            pos.change_pct, pos.btc_trend, pos.fear_greed,
            json.dumps(extra, allow_nan=False, default=str), pos.opened_at,
        ))
        return True

    def _write_sqlite_single(self, pos: Position) -> bool:
        conn = None
        try:
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            persisted = self._upsert_row(conn, pos)
            conn.execute("COMMIT")
            return persisted
        except Exception as e:
            self._warn(f"SQLite write error ({pos.symbol}): {e}")
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _write_sqlite_bulk(self, positions: Dict[str, Position]) -> bool:
        conn = None
        try:
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            existing = {
                r["symbol"] for r in conn.execute(
                    "SELECT symbol FROM bot_open_positions WHERE bot_name=?",
                    (self.bot_name,),
                ).fetchall()
            }
            new_syms = set(positions.keys())
            for sym in existing - new_syms:
                conn.execute(
                    "DELETE FROM bot_open_positions WHERE bot_name=? AND symbol=?",
                    (self.bot_name, sym),
                )
            for pos in positions.values():
                if not self._upsert_row(conn, pos):
                    conn.execute("ROLLBACK")
                    return False
            conn.execute("COMMIT")
            return True
        except Exception as e:
            self._warn(f"SQLite bulk write error: {e}")
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _delete_sqlite(self, symbol: str) -> bool:
        conn = None
        try:
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM bot_open_positions WHERE bot_name=? AND symbol=?",
                (self.bot_name, symbol),
            )
            conn.execute("COMMIT")
            return True
        except Exception as e:
            self._warn(f"SQLite delete error ({symbol}): {e}")
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _load_from_json(self) -> dict:
        if not os.path.exists(self.json_path):
            return {}
        try:
            with open(self.json_path, "rb") as fh:
                raw = fh.read(_STATE_JSON_MAX_BYTES + 1)
            if len(raw) > _STATE_JSON_MAX_BYTES:
                raise ValueError("state JSON exceeds size limit")
            data = json.loads(raw.decode("utf-8-sig"))
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError as e:
            backup = self.json_path + ".corrupted"
            try:
                shutil.copy2(self.json_path, backup)
            except Exception:
                pass
            self._warn(
                f"JSON corrupted ({e})  backup at {backup!r}. Using SQLite only.")
            return {}
        except Exception as e:
            self._warn(f"JSON load error: {e}")
            return {}

    def _warn(self, msg: str) -> None:
        try:
            from core.logger import log_event
            log_event(f"[StateManager:{self.bot_name}] {msg}", "WARN")
        except Exception:
            print(f"[StateManager:{self.bot_name}] WARN: {msg}")

    def _emit(self, event_type: str, pos: Position) -> None:
        try:
            from core.event_bus import get_bus
            get_bus().emit(event_type, {
                "bot_name":      pos.bot_name,
                "symbol":        pos.symbol,
                "position_type": pos.position_type.value,
                "buy_price":     pos.buy_price,
                "amount":        pos.amount,
                "invested_usdt": pos.invested_usdt,
                "state":         pos.state.value,
            }, emitted_by=self.bot_name)
        except Exception:
            pass

    def __repr__(self) -> str:
        return (
            f"StateManager(bot={self.bot_name!r}, "
            f"open={len(self._positions)})"
        )
