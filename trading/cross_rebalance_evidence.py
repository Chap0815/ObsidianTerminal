"""Bounded, read-only evidence for CROSS's durable crash-filter cursor.

This journal is bot accounting evidence, not an exchange-flatness proof and not
a supported way to seed or migrate an old risk history.
"""
from __future__ import annotations

import math
import re
import hashlib
import json
import stat
from dataclasses import dataclass


MAX_CLOSED_TRADES = 1000
_EARLY_CLOSE_REASONS = frozenset({
    "Cross disaster-stop", "Cross daily-loss killswitch", "Cross neutrality-guard",
})


def risk_scope(*, simulation: bool, exchange_id: str,
               api_key_identifier: str | None = None) -> str:
    if not isinstance(simulation, bool):
        raise ValueError("invalid risk mode")
    if not isinstance(exchange_id, str) or re.fullmatch(r"[a-z0-9_-]{1,32}", exchange_id) is None:
        raise ValueError("invalid risk exchange")
    if simulation:
        return "SIM:" + exchange_id
    if (not isinstance(api_key_identifier, str)
            or not 1 <= len(api_key_identifier) <= 512
            or not api_key_identifier.strip()):
        raise ValueError("LIVE risk scope requires a credential identifier")
    # Credential binding deliberately invalidates on key rotation. It is not
    # advertised as a stable account identity. No secret is hashed or stored.
    material = (b"cross-risk-credential-scope-v1\0"
                + exchange_id.encode("ascii") + b"\0"
                + api_key_identifier.encode("utf-8"))
    return "LIVE:" + hashlib.sha256(material).hexdigest()[:32]


def validate_risk_evidence(value: object) -> dict:
    """Validate schema2 evidence without inventing a seed or migration proof."""
    if not isinstance(value, dict) or set(value) != {
        "scope", "cursor", "reentry_slot", "pending",
    }:
        raise ValueError("invalid risk evidence shape")
    scope = value["scope"]
    if not isinstance(scope, str) or re.fullmatch(r"(?:SIM|LIVE):[a-z0-9_-]{1,32}", scope) is None:
        raise ValueError("invalid risk scope")
    _nonnegative_int(value["cursor"], "trade cursor")
    if value["reentry_slot"] is not None:
        _nonnegative_int(value["reentry_slot"], "reentry slot")
    pending = value["pending"]
    if pending is not None:
        if not isinstance(pending, dict) or set(pending) != {
            "id", "kind", "slot", "through", "target_sha256",
        }:
            raise ValueError("invalid pending risk intent shape")
        if not isinstance(pending["id"], str) or re.fullmatch(r"[0-9a-f]{32}", pending["id"]) is None:
            raise ValueError("invalid pending operation id")
        if pending["kind"] not in ("rebalance", "topup"):
            raise ValueError("invalid pending operation kind")
        _nonnegative_int(pending["slot"], "pending slot")
        through = _nonnegative_int(pending["through"], "pending cursor")
        if through < value["cursor"]:
            raise ValueError("pending cursor precedes committed evidence")
        if not isinstance(pending["target_sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", pending["target_sha256"]) is None:
            raise ValueError("invalid pending target digest")
    # Return a copy so mutable caller dictionaries cannot change authority.
    return {**value, "pending": None if pending is None else dict(pending)}


def read_empty_cross_state_evidence() -> dict[str, str | None]:
    """Read both canonical mode files without TradeState's registry writes."""
    from core.paths import PROJECT_ROOT, LOG_DIR_CROSS

    root = PROJECT_ROOT.resolve()
    evidence = {}
    for name in ("trades.json", "trades.sim.json"):
        path = LOG_DIR_CROSS / name
        path.absolute().relative_to(root)
        for component in (path, *path.parents):
            if component.exists() or component.is_symlink():
                attrs = component.lstat()
                if component.is_symlink() or getattr(attrs, "st_file_attributes", 0) & 0x400:
                    raise ValueError("CROSS state path is a reparse point")
            if component == root:
                break
        if not path.exists():
            evidence[name] = None
            continue
        before = path.stat()
        if not stat.S_ISREG(before.st_mode) or before.st_size > 65_536:
            raise ValueError("CROSS state is not bounded regular data")
        with path.open("rb") as handle:
            raw = handle.read(65_537)
        after = path.stat()
        if (len(raw) > 65_536 or before.st_size != len(raw)
                or (before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns)):
            raise ValueError("CROSS state changed while observed")
        parsed = json.loads(raw.decode("utf-8-sig"))
        if type(parsed) is not dict or parsed:
            raise ValueError("CROSS state or recovery accounting is not empty")
        evidence[name] = hashlib.sha256(raw).hexdigest()
    return evidence


def cross_epoch_local_blockers(conn, *, simulation: bool) -> tuple[str, ...]:
    """Strict local DB gates, also reused inside the reset write transaction."""
    if type(simulation) is not bool:
        raise ValueError("invalid epoch mode")
    names = ("CROSS", "CROSS (SIM)") if simulation else (
        "CROSS", "CROSS (SIM)", "FUTURES", "FUTURES (SIM)", "FUTREND", "FUTREND (SIM)",
    )
    marks = ",".join("?" for _ in names)
    checks = (
        ("claims", f"SELECT 1 FROM bot_open_positions WHERE UPPER(TRIM(bot_name)) IN ({marks}) "
         "AND UPPER(TRIM(COALESCE(state,''))) NOT IN ('CLOSED','FLAT') LIMIT 1"),
        ("futures_state", f"SELECT 1 FROM futures_state WHERE UPPER(TRIM(bot_name)) IN ({marks}) LIMIT 1"),
        ("entry_recovery", f"SELECT 1 FROM order_intents WHERE UPPER(TRIM(bot_name)) IN ({marks}) "
         "AND COALESCE(status,'')!='FINALIZED' LIMIT 1"),
        ("reservations", f"SELECT 1 FROM portfolio_reservations WHERE UPPER(TRIM(bot_name)) IN ({marks}) "
         "AND COALESCE(status,'') NOT IN ('CONSUMED','RELEASED','EXPIRED') LIMIT 1"),
    )
    return tuple(name for name, query in checks if conn.execute(query, names).fetchone() is not None)


@dataclass(frozen=True)
class ClosedTradeEvidence:
    through_id: int
    moves: tuple[tuple[float, float], ...]


def _nonnegative_int(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= (1 << 63) - 1:
        raise ValueError(f"invalid {name}")
    return value


def _positive_number(value, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid {name}")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"invalid {name}")
    return result


def read_closed_trade_evidence(cursor: int, *, simulation: bool) -> ClosedTradeEvidence:
    """Read one SQLite snapshot, detecting overflow rather than truncating it.

    Cursor advancement covers every committed row through the snapshot's HWM,
    but only the original full early-close reasons contribute a GROSS signal.
    Unknown mode rows are ambiguous evidence and fail closed. Rows committed
    after this snapshot stay beyond its cursor for the next decision.
    """
    from core.database import get_connection

    _nonnegative_int(cursor, "trade cursor")
    if not isinstance(simulation, bool):
        raise ValueError("simulation must be boolean")
    conn = get_connection()
    if conn.in_transaction:
        raise ValueError("risk evidence requires an independent read snapshot")
    conn.execute("BEGIN")
    try:
        row = conn.execute("SELECT COALESCE(MAX(id),0) AS h FROM trades").fetchone()
        if row is None:
            raise ValueError("missing trade high-water mark")
        through_id = _nonnegative_int(row["h"], "trade high-water mark")
        if through_id < cursor:
            raise ValueError("trade journal precedes durable cursor")
        count_row = conn.execute(
            """SELECT COUNT(*) AS n FROM (
               SELECT id FROM trades
               WHERE bot_name IN ('CROSS','CROSS (SIM)') AND id>? AND id<=?
               LIMIT ?)""",
            (cursor, through_id, MAX_CLOSED_TRADES + 1),
        ).fetchone()
        if count_row is None:
            raise ValueError("missing trade evidence count")
        count = _nonnegative_int(count_row["n"], "trade evidence count")
        if count > MAX_CLOSED_TRADES:
            raise ValueError("unconsumed CROSS trade evidence exceeds bound")
        rows = conn.execute(
            """SELECT id,buy_price,sell_price,invested_usdt,position_type,
                      leverage,is_sim,is_futures,is_partial,reason,bot_name
               FROM trades WHERE bot_name IN ('CROSS','CROSS (SIM)') AND id>? AND id<=?
               ORDER BY id LIMIT ?""",
            (cursor, through_id, MAX_CLOSED_TRADES + 1),
        ).fetchall()
        if len(rows) != count:
            raise ValueError("incomplete trade evidence snapshot")
        moves = []
        previous_id = cursor
        for item in rows:
            trade_id = _nonnegative_int(item["id"], "trade id")
            if not previous_id < trade_id <= through_id:
                raise ValueError("invalid trade snapshot ordering")
            previous_id = trade_id
            mode = item["is_sim"]
            if type(mode) is not int or mode not in (0, 1):
                raise ValueError("ambiguous CROSS trade mode")
            if mode != int(simulation):
                continue
            if item["bot_name"] != ("CROSS (SIM)" if simulation else "CROSS"):
                raise ValueError("ambiguous CROSS trade namespace")
            if type(item["is_futures"]) is not int or item["is_futures"] != 1:
                raise ValueError("ambiguous CROSS market class")
            if type(item["is_partial"]) is not int or item["is_partial"] not in (0, 1):
                raise ValueError("ambiguous CROSS partial-close flag")
            if item["is_partial"] or item["reason"] not in _EARLY_CLOSE_REASONS:
                continue
            entry = _positive_number(item["buy_price"], "entry price")
            closed = _positive_number(item["sell_price"], "close price")
            margin = _positive_number(item["invested_usdt"], "margin")
            leverage = _positive_number(item["leverage"], "leverage")
            side = item["position_type"]
            if side not in ("LONG", "SHORT"):
                raise ValueError("invalid CROSS close side")
            move = (closed - entry) / entry
            if side == "SHORT":
                move = -move
            weight = margin * leverage
            if not math.isfinite(move) or not math.isfinite(weight) or weight <= 0:
                raise ValueError("invalid CROSS weighted close")
            moves.append((move, weight))
        return ClosedTradeEvidence(through_id, tuple(moves))
    finally:
        # Read-only transaction: never commit a caller's writes or leave a lock.
        conn.rollback()
