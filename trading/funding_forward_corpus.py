"""Forward-only research evidence in the existing journal database.

Registration attests availability now. Native collection times and filesystem
metadata never qualify old folds. This module grants no account-PnL authority.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3


SCHEMA = 1
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
CHUNK_BYTES = 64 * 1024
MAX_SOURCES = 16
MAX_MANIFEST_BYTES = 32 * 1024
CLOCK_KIND = "exchange_rtt_upper_v1"
CHUNKS_PER_TRANSACTION = 8


class ForwardCorpusConnectionError(RuntimeError):
    """The caller-owned connection must not be reused after failed cleanup."""


class SourceArchiveIncomplete(ValueError):
    """An interrupted PENDING archive needs an identical registration retry."""


def _rollback_or_close(connection, primary=None):
    try:
        if connection.in_transaction:
            connection.rollback()
    except BaseException as rollback_error:
        cause = primary if primary is not None else rollback_error
        if primary is not None:
            cause.add_note(f"forward corpus rollback failed: {rollback_error}")
        try:
            connection.close()
        except BaseException as close_error:
            cause.add_note(f"forward corpus connection close failed: {close_error}")
        raise ForwardCorpusConnectionError(
            "forward corpus connection cleanup failed; do not reuse this connection"
        ) from cause


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate_forward_corpus_key")
            result[key] = value
        return result

    def invalid(_value):
        raise ValueError("nonfinite_forward_corpus_value")

    return json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)


def _identity(value):
    if (type(value) is not str or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)):
        raise ValueError("invalid_forward_corpus_identity")
    return value


def _one(connection, sql, parameters=()):
    cursor = connection.execute(sql, parameters)
    row = cursor.fetchone()
    return None if row is None else dict(zip((item[0] for item in cursor.description), row))


def initialize_forward_corpus_schema(connection):
    """Called by a supported migration; never commit the caller's transaction."""
    connection.execute("""CREATE TABLE IF NOT EXISTS funding_forward_source_sets (
        source_set_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL,
        manifest_json TEXT NOT NULL, facts_sha256 TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'PENDING', available_by_ms REAL,
        witness_kind TEXT, clock_bound_kind TEXT)""")
    connection.execute("""CREATE TABLE IF NOT EXISTS funding_forward_source_chunks (
        source_set_id TEXT NOT NULL, source_index INTEGER NOT NULL,
        chunk_index INTEGER NOT NULL, data BLOB NOT NULL,
        PRIMARY KEY(source_set_id,source_index,chunk_index),
        FOREIGN KEY(source_set_id) REFERENCES funding_forward_source_sets(source_set_id)
            ON DELETE CASCADE)""")
    from trading.funding_forward_generation import initialize_funding_generation_schema
    initialize_funding_generation_schema(connection)


def _read_source_set(connection, source_set_id):
    from tools import native_funding_snapshot as native
    from trading.settled_funding_model import provider_from_native_snapshots

    _identity(source_set_id)
    row = _one(connection, """SELECT schema_version,manifest_json,facts_sha256,state,
        available_by_ms,witness_kind,clock_bound_kind FROM funding_forward_source_sets
        WHERE source_set_id=? AND length(CAST(manifest_json AS BLOB))<=?""",
        (source_set_id, MAX_MANIFEST_BYTES))
    if row is None or type(row["schema_version"]) is not int or row["schema_version"] != SCHEMA:
        raise ValueError("missing_forward_source_set")
    if row["state"] == "PENDING" and any(row[name] is not None for name in
            ("available_by_ms", "witness_kind", "clock_bound_kind")):
        raise ValueError("conflicting_forward_source_pending_proof")
    manifest = _decode(row["manifest_json"])
    if (type(manifest) is not dict
            or set(manifest) != {"schema_version", "kind", "sources", "provider_sha256"}
            or type(manifest["schema_version"]) is not int or manifest["schema_version"] != SCHEMA
            or manifest["kind"] != "registered_native_settled_funding_sources"
            or type(manifest["sources"]) is not list or not 1 <= len(manifest["sources"]) <= MAX_SOURCES
            or _sha(_canonical(manifest)) != source_set_id or row["facts_sha256"] != source_set_id):
        raise ValueError("invalid_forward_source_manifest")
    snapshots, total, expected_keys, incomplete = [], 0, set(), False
    for index, item in enumerate(manifest["sources"]):
        if (type(item) is not dict or set(item) != {"symbol", "sha256", "bytes", "source_sha256", "start_ms", "end_ms"}
                or type(item["bytes"]) is not int or not 0 < item["bytes"] <= native.MAX_BYTES):
            raise ValueError("invalid_forward_source_manifest")
        _identity(item["sha256"])
        _identity(item["source_sha256"])
        total += item["bytes"]
        if total > MAX_ARCHIVE_BYTES:
            raise ValueError("forward_source_byte_budget_exceeded")
        expected_count = (item["bytes"] + CHUNK_BYTES - 1) // CHUNK_BYTES
        # Test SQL length before materializing even a malicious oversized BLOB.
        cursor = connection.execute("""SELECT chunk_index,
            CASE WHEN typeof(data)='blob' AND length(data)<=? THEN data ELSE NULL END
            FROM funding_forward_source_chunks WHERE source_set_id=? AND source_index=?
            ORDER BY chunk_index LIMIT ?""", (CHUNK_BYTES, source_set_id, index, expected_count + 1))
        parts = []
        for position, chunk in enumerate(cursor):
            if (position >= expected_count or type(chunk[0]) is not int or chunk[0] != position
                    or type(chunk[1]) is not bytes or not chunk[1]
                    or len(chunk[1]) != min(CHUNK_BYTES, item["bytes"] - position * CHUNK_BYTES)):
                raise ValueError("invalid_forward_source_chunks")
            parts.append(chunk[1])
            expected_keys.add((index, position))
        raw = b"".join(parts)
        if len(parts) < expected_count and row["state"] == "PENDING":
            incomplete = True
            continue
        if len(raw) != item["bytes"] or _sha(raw) != item["sha256"]:
            raise ValueError("forward_source_bytes_changed")
        snapshot = _decode(raw)
        native._validate(snapshot)
        if (snapshot["symbol"] != item["symbol"] or snapshot["source_sha256"] != item["source_sha256"]
                or type(item["start_ms"]) is not int or type(item["end_ms"]) is not int
                or snapshot["start_ms"] != item["start_ms"] or snapshot["end_ms"] != item["end_ms"]):
            raise ValueError("forward_source_binding_changed")
        snapshots.append(snapshot)
    # Reject undeclared indices/chunks without reading their data.
    keys = connection.execute("""SELECT source_index,chunk_index FROM funding_forward_source_chunks
        WHERE source_set_id=? LIMIT ?""", (source_set_id, MAX_ARCHIVE_BYTES // CHUNK_BYTES + MAX_SOURCES + 1))
    actual_keys = set()
    for key in keys:
        if any(type(value) is not int for value in key):
            raise ValueError("invalid_forward_source_chunks")
        actual_keys.add(tuple(key))
    if actual_keys != expected_keys:
        raise ValueError("undeclared_forward_source_chunks")
    if incomplete:
        raise SourceArchiveIncomplete("incomplete_forward_source_archive")
    provider = provider_from_native_snapshots(snapshots)
    if provider.fingerprint != manifest["provider_sha256"]:
        raise ValueError("forward_source_provider_changed")
    return row, provider, manifest


def _source_set_status(connection, source_set_id, *, cutoff_ms=None):
    row, provider, manifest = _read_source_set(connection, source_set_id)
    if (row["state"] != "ACKED" or row["clock_bound_kind"] != CLOCK_KIND
            or row["witness_kind"] not in {"commit_ack", "recovery_read"}
            or type(row["available_by_ms"]) not in (int, float)
            or not math.isfinite(row["available_by_ms"]) or row["available_by_ms"] <= 0):
        raise ValueError("unproven_forward_source_availability")
    if cutoff_ms is not None:
        if type(cutoff_ms) not in (int, float) or not math.isfinite(cutoff_ms):
            raise ValueError("invalid_forward_corpus_cutoff")
        if row["available_by_ms"] > cutoff_ms:
            raise ValueError("late_forward_source_availability")
    return row, provider, manifest


def read_registered_funding_sources(connection, source_set_id, *, cutoff_ms=None):
    """Use a single read snapshot; no clock, file reread, or availability renewal."""
    if not connection.in_transaction:
        raise ValueError("forward_sources_require_read_snapshot")
    return _source_set_status(connection, source_set_id, cutoff_ms=cutoff_ms)


def _verified_source_snapshot(connection, source_set_id):
    if connection.in_transaction:
        raise ValueError("forward_source_verification_requires_idle_connection")
    connection.execute("BEGIN")
    try:
        result = _read_source_set(connection, source_set_id)
    except BaseException as primary:
        _rollback_or_close(connection, primary)
        raise
    _rollback_or_close(connection)
    return result


def _ack_source_set(connection, source_set_id, *, recovery):
    from core.clock import availability_upper_bound_ms

    if connection.in_transaction:
        raise ValueError("forward_source_ack_requires_original_commit")
    # Full bounded validation uses a read snapshot, never the writer lock.
    # Content-addressing binds these exact bytes even across the following CAS.
    row, _provider, _manifest = _verified_source_snapshot(connection, source_set_id)
    if row["state"] == "ACKED":
        connection.execute("BEGIN")
        try:
            _source_set_status(connection, source_set_id)
        except BaseException as primary:
            _rollback_or_close(connection, primary)
            raise
        _rollback_or_close(connection)
        return True
    if (row["state"] != "PENDING" or row["available_by_ms"] is not None
            or row["witness_kind"] is not None or row["clock_bound_kind"] is not None):
        return False
    stamp = availability_upper_bound_ms()
    if type(stamp) is not int or stamp <= 0:
        return False
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute("""UPDATE funding_forward_source_sets
            SET state='ACKED',available_by_ms=?,witness_kind=?,clock_bound_kind=?
            WHERE source_set_id=? AND schema_version=? AND manifest_json=? AND facts_sha256=?
            AND state='PENDING' AND available_by_ms IS NULL
            AND witness_kind IS NULL AND clock_bound_kind IS NULL""",
            (stamp, "recovery_read" if recovery else "commit_ack", CLOCK_KIND, source_set_id,
             SCHEMA, row["manifest_json"], source_set_id))
        connection.commit()
        return changed.rowcount == 1
    except BaseException as primary:
        _rollback_or_close(connection, primary)
        raise


def register_funding_source_set(connection, paths):
    """Explicit admin write; exact validated bytes plus postcommit UTC witness.

    An interrupted archive leaves PENDING inputs. Identical retries may add
    missing chunks, never replace conflicting bytes or repair an ACKED set.
    First availability follows complete verification, not header creation.
    """
    from tools import native_funding_snapshot as native
    from trading.settled_funding_model import provider_from_native_snapshots

    if connection.in_transaction:
        raise ValueError("forward_source_registration_requires_idle_connection")
    if type(paths) not in (list, tuple) or not 1 <= len(paths) <= MAX_SOURCES:
        raise ValueError("forward_sources_require_1_to_16_inputs")
    sources, total = [], 0
    for path in paths:
        with native._plain_path(path).open("rb") as stream:
            raw = stream.read(native.MAX_BYTES + 1)
        total += len(raw)
        if len(raw) > native.MAX_BYTES or total > MAX_ARCHIVE_BYTES:
            raise ValueError("forward_source_byte_budget_exceeded")
        snapshot = _decode(raw)
        native._validate(snapshot)
        sources.append((snapshot, raw))
    sources.sort(key=lambda pair: pair[0]["symbol"])
    provider = provider_from_native_snapshots([pair[0] for pair in sources])
    manifest = {"schema_version": SCHEMA, "kind": "registered_native_settled_funding_sources",
                "provider_sha256": provider.fingerprint,
                "sources": [{"symbol": value["symbol"], "sha256": _sha(raw), "bytes": len(raw),
                             "source_sha256": value["source_sha256"],
                             "start_ms": value["start_ms"], "end_ms": value["end_ms"]}
                            for value, raw in sources]}
    encoded = _canonical(manifest)
    if len(encoded) > MAX_MANIFEST_BYTES:
        raise ValueError("forward_manifest_byte_budget_exceeded")
    identity = _sha(encoded)
    connection.execute("BEGIN IMMEDIATE")
    try:
        existing = _one(connection, "SELECT manifest_json,state FROM funding_forward_source_sets WHERE source_set_id=?", (identity,))
        if existing is None:
            connection.execute("""INSERT INTO funding_forward_source_sets
                (source_set_id,schema_version,manifest_json,facts_sha256) VALUES (?,?,?,?)""",
                (identity, SCHEMA, encoded.decode(), identity))
        elif existing["manifest_json"] != encoded.decode() or existing["state"] not in {"PENDING", "ACKED"}:
            raise ValueError("conflicting_forward_source_header")
        connection.commit()
    except BaseException as primary:
        _rollback_or_close(connection, primary)
        raise
    if existing is None or existing["state"] == "PENDING":
        pending = []
        for index, (_snapshot, raw) in enumerate(sources):
            pending.extend((index, number, raw[start:start + CHUNK_BYTES])
                           for number, start in enumerate(range(0, len(raw), CHUNK_BYTES)))
        for start in range(0, len(pending), CHUNKS_PER_TRANSACTION):
            connection.execute("BEGIN IMMEDIATE")
            try:
                header = _one(connection, """SELECT manifest_json,state,facts_sha256,schema_version
                    FROM funding_forward_source_sets WHERE source_set_id=?""", (identity,))
                if (header is None or header["manifest_json"] != encoded.decode()
                        or header["facts_sha256"] != identity or header["schema_version"] != SCHEMA):
                    raise ValueError("conflicting_forward_source_header")
                if header["state"] == "ACKED":
                    _rollback_or_close(connection)
                    break
                if header["state"] != "PENDING":
                    raise ValueError("conflicting_forward_source_header")
                for index, number, part in pending[start:start + CHUNKS_PER_TRANSACTION]:
                    previous = _one(connection, """SELECT CASE WHEN typeof(data)='blob'
                        AND length(data)<=? THEN data ELSE NULL END AS data
                        FROM funding_forward_source_chunks WHERE source_set_id=?
                        AND source_index=? AND chunk_index=?""", (CHUNK_BYTES, identity, index, number))
                    if previous is None:
                        connection.execute("INSERT INTO funding_forward_source_chunks VALUES (?,?,?,?)",
                                           (identity, index, number, part))
                    elif previous["data"] != part:
                        raise ValueError("conflicting_forward_source_chunk")
                connection.commit()
            except BaseException as primary:
                _rollback_or_close(connection, primary)
                raise
    # These reads/JSON checks intentionally happen after releasing writer locks.
    _verified_source_snapshot(connection, identity)
    # A failed optional ACK cannot erase the already committed input archive.
    try:
        _ack_source_set(connection, identity, recovery=existing is not None)
    except sqlite3.Error as exc:
        if (getattr(exc, "sqlite_errorcode", 0) & 0xff) in {
                sqlite3.SQLITE_INTERRUPT, sqlite3.SQLITE_TOOBIG,
                sqlite3.SQLITE_NOMEM, sqlite3.SQLITE_FULL}:
            raise
    except ValueError:
        pass
    return {"source_set_id": identity, "sources": len(sources), "bytes": total,
            "training_eligible": False, "historical_publication_verified": False,
            "promotion_eligible": False}


def recover_funding_source_sets(connection, *, limit=25, after_source_set_id=None):
    """Attest complete pending archives now; never fabricate missing chunks."""
    if connection.in_transaction or type(limit) is not int or not 1 <= limit <= 25:
        raise ValueError("invalid_forward_source_recovery_request")
    if after_source_set_id is not None:
        _identity(after_source_set_id)
    identities = connection.execute("""SELECT source_set_id
        FROM funding_forward_source_sets WHERE state='PENDING' AND source_set_id>?
        ORDER BY source_set_id LIMIT ?""", (after_source_set_id or "", limit + 1)).fetchall()
    recovered = 0
    batch = identities[:limit]
    for (identity,) in batch:
        try:
            recovered += bool(_ack_source_set(connection, identity, recovery=True))
        except SourceArchiveIncomplete:
            continue
    return {"recovered_source_sets": recovered, "scanned_source_sets": len(batch),
            "last_source_set_id": batch[-1][0] if batch else None,
            "has_more_source_sets": len(identities) > limit}
