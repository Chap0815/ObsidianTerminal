"""Shared, strict on-disk identity contract for websocket trades."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from datetime import date, datetime, timezone


TRADE_IDENTITY_TABLE = "websocket_trade_identities"


class TradeIdentityConflict(ValueError):
    """A durable trade identity contradicts incoming evidence."""
_COLUMNS = (
    ("market_id", "TEXT", 1, 1),
    ("trade_id", "TEXT", 1, 2),
    ("trade_time", "INTEGER", 1, 0),
    ("trade_price", "REAL", 1, 0),
    ("trade_amount", "REAL", 1, 0),
    ("trade_side", "TEXT", 1, 0),
    ("event_id", "TEXT", 1, 0),
)
_FULL_COLUMNS = tuple((*column, None, 0) for column in _COLUMNS)


def canonical_ws_writer_input_hash(event) -> str:
    exchange_time = _parse_time(event.exchange_time).isoformat(timespec="microseconds")
    exchange_time = exchange_time.replace("+00:00", "Z")
    raw = json.dumps(
        {
            "event_id": event.event_id,
            "kind": event.kind,
            "market_id": event.market_id,
            "exchange_time": exchange_time,
            "received_time": event.received_time,
            "schema_version": event.schema_version,
            "quality_flags": event.quality_flags,
            "payload": event.payload,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def validate_trade_identity_schema(connection: sqlite3.Connection) -> int:
    version = connection.execute("PRAGMA user_version").fetchone()
    if version is None or type(version[0]) is not int or version[0] not in (0, 2):
        raise RuntimeError("capture trade identity version is invalid")
    version = version[0]
    objects = connection.execute(
        "SELECT type,name,tbl_name FROM sqlite_master "
        "WHERE name=? OR tbl_name=? ORDER BY type,name",
        (TRADE_IDENTITY_TABLE, TRADE_IDENTITY_TABLE),
    ).fetchall()
    if version == 0:
        if objects:
            raise RuntimeError("legacy capture has unexpected trade identity objects")
        return 0
    if tuple(map(tuple, objects)) != (("table", TRADE_IDENTITY_TABLE, TRADE_IDENTITY_TABLE),):
        raise RuntimeError("capture trade identity schema objects are invalid")
    columns = tuple(
        (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]),
         row[4], int(row[6]))
        for row in connection.execute(f"PRAGMA main.table_xinfo({TRADE_IDENTITY_TABLE})")
    )
    if columns != _FULL_COLUMNS:
        raise RuntimeError("capture trade identity columns are invalid")
    table_list = tuple(connection.execute("PRAGMA main.table_list"))
    identity_tables = tuple(row for row in table_list if row[1] == TRADE_IDENTITY_TABLE)
    if (
        len(identity_tables) != 1
        or identity_tables[0][0] != "main"
        or identity_tables[0][2:] != ("table", len(_COLUMNS), 1, 0)
    ):
        raise RuntimeError("capture trade identity table storage is invalid")
    indexes = tuple(connection.execute(f"PRAGMA main.index_list({TRADE_IDENTITY_TABLE})"))
    if len(indexes) != 1 or indexes[0][2:] != (1, "pk", 0):
        raise RuntimeError("capture trade identity index is invalid")
    xinfo = tuple(
        (row[2], row[3], row[4], row[5])
        for row in connection.execute(f"PRAGMA main.index_xinfo({indexes[0][1]})")
    )
    expected_xinfo = tuple((column[0], 0, "BINARY", 1 if index < 2 else 0)
                           for index, column in enumerate(_COLUMNS))
    if xinfo != expected_xinfo:
        raise RuntimeError("capture trade identity key is invalid")
    if tuple(connection.execute(f"PRAGMA main.foreign_key_list({TRADE_IDENTITY_TABLE})")):
        raise RuntimeError("capture trade identity foreign keys are invalid")
    return 2


def _parse_time(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("websocket trade event time is invalid")
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise ValueError("websocket trade event time is invalid") from exc


def _positive_number(value, label: str) -> float:
    if type(value) not in (int, float):
        raise ValueError(f"{label} is invalid")
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{label} is invalid")
    return result


def validate_ws_trade_payload(
    payload, *, exchange_time: str, received_time: str, day: date
) -> tuple[tuple[str, int, float, float, str], ...]:
    if not isinstance(payload, dict) or (
        payload.get("stream_source") != "ccxt_pro"
        or payload.get("continuity_status") != "websocket_observed_id_deduplicated"
    ):
        raise ValueError("trades websocket payload is malformed")
    universe = payload.get("universe")
    if (
        not isinstance(universe, list) or not universe
        or any(type(symbol) is not str or not symbol or symbol != symbol.strip()
               or any(ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF for char in symbol)
               for symbol in universe)
        or len(universe) != len(set(universe))
        or type(payload.get("connection_epoch")) is not int
        or payload["connection_epoch"] < 1
    ):
        raise ValueError("trades websocket universe or epoch is invalid")
    trades = payload.get("trades")
    if not isinstance(trades, list) or not 1 <= len(trades) <= 20_000:
        raise ValueError("trades websocket payload is empty or oversized")
    receipt_ms = round(_parse_time(received_time).timestamp() * 1000)
    actual_exchange = _parse_time(exchange_time)
    seen = set()
    previous_order = None
    result = []
    for trade in trades:
        if not isinstance(trade, dict):
            raise ValueError("trades websocket payload is malformed")
        trade_id, trade_time = trade.get("id"), trade.get("timestamp")
        if (
            type(trade_id) is not str
            or not trade_id
            or trade_id != trade_id.strip()
            or len(trade_id) > 256
            or any(ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF for char in trade_id)
            or trade_id in seen
            or type(trade_time) is not int
            or trade_time <= 0
        ):
            raise ValueError("trades websocket identity is invalid")
        seen.add(trade_id)
        try:
            if datetime.fromtimestamp(trade_time / 1000, timezone.utc).date() != day:
                raise ValueError("trade escaped UTC partition")
        except (OSError, OverflowError) as exc:
            raise ValueError("trades websocket timestamp is invalid") from exc
        if trade_time > receipt_ms + 30_000 or receipt_ms - trade_time > 86_400_000:
            raise ValueError("trades websocket receipt provenance mismatch")
        price = _positive_number(trade.get("price"), "trade price")
        amount = _positive_number(trade.get("amount"), "trade amount")
        side = trade.get("side")
        if side not in ("buy", "sell") or type(side) is not str:
            raise ValueError("trade side is invalid")
        order = (trade_time, trade_id)
        if previous_order is not None and order < previous_order:
            raise ValueError("trades websocket chronology is invalid")
        previous_order = order
        result.append((trade_id, trade_time, price, amount, side))
    if actual_exchange != datetime.fromtimestamp(previous_order[0] / 1000, timezone.utc):
        raise ValueError("trades websocket exchange_time mismatch")
    return tuple(result)


def read_trade_identity(connection, market_id: str, trade_id: str):
    row = connection.execute(
        "SELECT trade_time,trade_price,trade_amount,trade_side,event_id "
        "FROM main.websocket_trade_identities WHERE market_id=? AND trade_id=?",
        (market_id, trade_id),
    ).fetchone()
    return tuple(row) if row is not None else None


def verify_trade_identity(connection, market_id: str, trade_id: str, evidence, event_id: str) -> None:
    row = read_trade_identity(connection, market_id, trade_id)
    if row != (*evidence, event_id):
        raise TradeIdentityConflict("websocket trade identity ledger mismatch")
