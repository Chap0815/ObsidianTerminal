"""Dry-run claim/state repair report.

Default mode is read-only. ``--apply`` only removes rows that match strict
safe transient criteria: market-shaped bot names are reported, but deleted
only when they are also old empty CLAIMING/ADOPTING placeholders. It never
deletes OPEN claims.

Usage:
    py -3.12 -m tools.repair_claim_state
    py -3.12 -m tools.repair_claim_state --apply
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from bot_utils.state_persist import atomic_save_json
from core.paths import BOT_CONFIG, DB_PATH, PROJECT_ROOT
from launcher.config.settings import BOT_META


SIM_TAG = " (SIM)"


@dataclass(frozen=True)
class Issue:
    severity: str
    code: str
    bot: str
    symbol: str
    detail: str


def _base_symbol(symbol: Any) -> str:
    return str(symbol or "").split("/")[0].split(":")[0].strip().upper()


def _is_sim(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _raw_bot(metric_bot: str) -> tuple[str, bool]:
    text = str(metric_bot or "")
    if text.endswith(SIM_TAG):
        return text[:-len(SIM_TAG)], True
    return text, False


def _utcnow() -> datetime:
    try:
        from core.clock import now_utc
        return now_utc().replace(tzinfo=None)
    except Exception:
        return datetime.now(timezone.utc).replace(tzinfo=None)


def _read_config_modes(config_path: Path) -> dict[str, bool]:
    modes = {bot: True for bot in BOT_META}
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return modes
    if not isinstance(raw, dict):
        return modes
    for bot in modes:
        section = raw.get(bot)
        if not isinstance(section, dict):
            continue
        if "SIMULATION" in section:
            modes[bot] = _is_sim(section.get("SIMULATION"))
        elif "SIMULATION_MODE" in section:
            modes[bot] = _is_sim(section.get("SIMULATION_MODE"))
    return modes


def _state_path(project_root: Path, bot: str, is_sim: bool) -> Path:
    log_dir = str(BOT_META[bot]["log_dir"]).replace("/", "\\")
    filename = "trades.sim.json" if is_sim else "trades.json"
    return project_root / log_dir / filename


def _load_state(path: Path) -> tuple[dict[str, dict], str | None]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return {}, None
    except (OSError, json.JSONDecodeError) as exc:
        return {}, f"{type(exc).__name__}: {exc}"
    if not isinstance(raw, dict):
        return {}, "top-level JSON is not an object"
    out: dict[str, dict] = {}
    for symbol, row in raw.items():
        if isinstance(row, dict):
            out[str(symbol)] = row
    return out, None


def _connect(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=20.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


def _position_contracts(position: dict) -> float:
    try:
        return float(position.get("contracts") or position.get("size") or 0)
    except (TypeError, ValueError):
        return 0.0


def fetch_exchange_futures_bases() -> tuple[set[str], str | None]:
    """Fetch live futures bases from the exchange.

    Read-only. Returns (bases, error). Used only for report annotation; never
    authorizes deleting OPEN claims.
    """
    try:
        from config.exchange_config import get_futures_exchange_connection
        ex = get_futures_exchange_connection()
        ex.timeout = 20000
        try:
            ex.load_markets()
        except Exception:
            pass
        bases = set()
        for p in ex.fetch_positions():
            if abs(_position_contracts(p)) < 1e-12:
                continue
            bases.add(_base_symbol(p.get("symbol")))
        return bases, None
    except Exception as exc:
        return set(), f"{type(exc).__name__}: {exc}"


def _table_rows(conn: sqlite3.Connection, table: str) -> tuple[list[dict], str | None]:
    try:
        rows = conn.execute(f"SELECT rowid, * FROM {table}").fetchall()
        return [dict(r) for r in rows], None
    except sqlite3.Error as exc:
        return [], f"{type(exc).__name__}: {exc}"


def _safe_delete_candidates(
    claims: list[dict],
    *,
    ttl_minutes: int,
    now: datetime | None = None,
) -> list[dict]:
    cutoff = (now or _utcnow()) - timedelta(minutes=ttl_minutes)
    out = []
    for row in claims:
        state = str(row.get("state") or "").upper()
        if state not in {"CLAIMING", "ADOPTING"}:
            continue
        try:
            amount = float(row.get("amount") or 0)
            invested = float(row.get("invested_usdt") or 0)
        except (TypeError, ValueError):
            continue
        opened_at = str(row.get("opened_at") or "")
        try:
            opened_dt = datetime.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if amount <= 0 and invested <= 0 and opened_dt < cutoff:
            out.append(row)
    return out


def analyze(
    *,
    project_root: Path = PROJECT_ROOT,
    db_path: Path = DB_PATH,
    config_path: Path = BOT_CONFIG,
    ttl_minutes: int = 5,
    exchange_futures_bases: set[str] | None = None,
    exchange_error: str | None = None,
) -> tuple[list[Issue], list[dict], dict[str, Any]]:
    project_root = Path(project_root)
    db_path = Path(db_path)
    config_path = Path(config_path)
    modes = _read_config_modes(config_path)
    issues: list[Issue] = []
    meta: dict[str, Any] = {
        "project_root": str(project_root),
        "db_path": str(db_path),
        "ttl_minutes": ttl_minutes,
    }
    if exchange_futures_bases is not None:
        meta["exchange_futures_bases"] = sorted(exchange_futures_bases)
    if exchange_error:
        meta["exchange_verify_error"] = exchange_error

    states: dict[tuple[str, bool], dict[str, dict]] = {}
    state_errors: dict[tuple[str, bool], str] = {}
    for bot in BOT_META:
        for is_sim in (False, True):
            path = _state_path(project_root, bot, is_sim)
            rows, err = _load_state(path)
            states[(bot, is_sim)] = rows
            if err:
                state_errors[(bot, is_sim)] = err
                issues.append(Issue(
                    "ERROR", "state_json_unreadable", bot, "-",
                    f"{path}: {err}",
                ))
            for symbol, row in rows.items():
                base = _base_symbol(symbol)
                if row.get("accounting_pending"):
                    issues.append(Issue(
                        "WARN", "accounting_pending", bot, base,
                        f"{'SIM' if is_sim else 'LIVE'} state has pending accounting",
                    ))
                if row.get("claim_release_pending"):
                    issues.append(Issue(
                        "WARN", "claim_release_pending", bot, base,
                        f"{'SIM' if is_sim else 'LIVE'} state has pending claim release",
                    ))

    conn = _connect(db_path)
    try:
        claims, claim_err = _table_rows(conn, "bot_open_positions")
        fstate, fstate_err = _table_rows(conn, "futures_state")
    finally:
        conn.close()
    if claim_err:
        issues.append(Issue("ERROR", "claims_read_failed", "-", "-", claim_err))
        claims = []
    if fstate_err:
        issues.append(Issue("ERROR", "futures_state_read_failed", "-", "-", fstate_err))
        fstate = []

    safe_deletes = _safe_delete_candidates(claims, ttl_minutes=ttl_minutes)
    safe_delete_keys = {(r.get("bot_name"), r.get("symbol")) for r in safe_deletes}

    live_claim_bases: dict[str, set[str]] = {bot: set() for bot in BOT_META}
    for row in claims:
        bot = str(row.get("bot_name") or "")
        symbol = str(row.get("symbol") or "")
        base = _base_symbol(symbol)
        if "/" in bot or ":" in bot:
            issues.append(Issue(
                "WARN", "junk_market_shaped_bot_name", bot, base,
                "market-shaped bot_name; apply only deletes it if it is an old empty transient",
            ))
            continue
        if bot not in BOT_META:
            issues.append(Issue("WARN", "unknown_claim_bot", bot, base, "unknown bot_name"))
            continue
        live_claim_bases.setdefault(bot, set()).add(base)
        state = str(row.get("state") or "").upper()
        if modes.get(bot, True):
            issues.append(Issue(
                "ERROR", "sim_bot_has_live_claim", bot, base,
                "bot_config is SIM but bot_open_positions contains a LIVE claim",
            ))
        live_state = states.get((bot, False), {})
        live_bases = {_base_symbol(s) for s in live_state}
        if (bot, symbol) in safe_delete_keys:
            issues.append(Issue(
                "INFO", "safe_transient_claim_delete_candidate", bot, base,
                f"{state} older than {ttl_minutes}min with zero amount/invested",
            ))
            continue
        if base not in live_bases:
            severity = "WARN" if state in {"CLAIMING", "ADOPTING"} else "ERROR"
            exchange_note = ""
            if exchange_futures_bases is not None and BOT_META.get(bot, {}).get("is_futures"):
                exchange_note = (
                    f"; exchange_flat={base not in exchange_futures_bases}"
                    if not exchange_error else
                    f"; exchange_verify_error={exchange_error}"
                )
            issues.append(Issue(
                severity, "claim_without_live_json_state", bot, base,
                f"state={state or '-'} amount={row.get('amount')} "
                f"invested={row.get('invested_usdt')}{exchange_note}",
            ))
        try:
            amount = float(row.get("amount") or 0)
            invested = float(row.get("invested_usdt") or 0)
        except (TypeError, ValueError):
            amount = invested = 0.0
        if state == "OPEN" and amount <= 0 and invested <= 0:
            issues.append(Issue(
                "WARN", "empty_open_claim", bot, base,
                "OPEN claim has zero amount and zero invested_usdt",
            ))

    for bot in BOT_META:
        if modes.get(bot, True):
            continue
        if (bot, False) in state_errors:
            continue
        for symbol in states.get((bot, False), {}):
            base = _base_symbol(symbol)
            if base not in live_claim_bases.get(bot, set()):
                issues.append(Issue(
                    "ERROR", "live_json_state_without_claim", bot, base,
                    "LIVE trades.json has a position but bot_open_positions has no claim",
                ))

    for row in fstate:
        metric_bot = str(row.get("bot_name") or "")
        bot, is_sim = _raw_bot(metric_bot)
        base = _base_symbol(row.get("symbol"))
        if bot not in BOT_META:
            issues.append(Issue("WARN", "unknown_futures_state_bot", metric_bot, base, "unknown bot_name"))
            continue
        if (bot, is_sim) in state_errors:
            continue
        mode_state_bases = {_base_symbol(s) for s in states.get((bot, is_sim), {})}
        if base not in mode_state_bases:
            issues.append(Issue(
                "WARN", "futures_state_without_json_state", bot, base,
                f"{'SIM' if is_sim else 'LIVE'} futures_state row has no matching JSON state",
            ))

    futures_bots = {bot for bot, m in BOT_META.items() if m.get("is_futures")}
    fstate_bases = {
        (_raw_bot(str(row.get("bot_name") or ""))[0],
         _raw_bot(str(row.get("bot_name") or ""))[1],
         _base_symbol(row.get("symbol")))
        for row in fstate
    }
    for bot in futures_bots:
        for is_sim in (False, True):
            if (bot, is_sim) in state_errors:
                continue
            for symbol in states.get((bot, is_sim), {}):
                base = _base_symbol(symbol)
                if (bot, is_sim, base) not in fstate_bases:
                    issues.append(Issue(
                        "WARN", "json_state_without_futures_state", bot, base,
                        f"{'SIM' if is_sim else 'LIVE'} JSON state has no dashboard futures_state row",
                    ))

    return issues, safe_deletes, meta


def apply_safe_deletes(
    *,
    db_path: Path = DB_PATH,
    project_root: Path = PROJECT_ROOT,
    ttl_minutes: int = 5,
) -> tuple[int, Path | None]:
    issues, candidates, _meta = analyze(
        project_root=project_root,
        db_path=db_path,
        config_path=Path(project_root) / "bot_config.json",
        ttl_minutes=ttl_minutes,
    )
    _ = issues
    if not candidates:
        return 0, None
    stamp = _utcnow().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(project_root) / "logs" / f"repair_claim_state_{stamp}.json"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at_utc": _utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "db_path": str(db_path),
        "ttl_minutes": ttl_minutes,
        "deleted_rows": candidates,
    }
    if not atomic_save_json(str(backup_path), payload):
        raise RuntimeError(f"backup write failed: {backup_path}")

    conn = _connect(db_path)
    try:
        deleted = 0
        conn.execute("BEGIN IMMEDIATE")
        cutoff_s = (_utcnow() - timedelta(minutes=ttl_minutes)).strftime(
            "%Y-%m-%d %H:%M:%S")
        for row in candidates:
            cur = conn.execute(
                "DELETE FROM bot_open_positions "
                "WHERE rowid=? "
                "AND state IN ('CLAIMING','ADOPTING') "
                "AND COALESCE(amount, 0) <= 0 "
                "AND COALESCE(invested_usdt, 0) <= 0 "
                "AND opened_at < ?",
                (row.get("rowid"), cutoff_s),
            )
            deleted += cur.rowcount
        conn.commit()
        return deleted, backup_path
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _print_report(issues: list[Issue], candidates: list[dict], meta: dict[str, Any]) -> None:
    print("CLAIM/STATE REPAIR REPORT")
    print("=" * 78)
    print(f"project_root: {meta.get('project_root')}")
    print(f"db_path     : {meta.get('db_path')}")
    print(f"ttl_minutes : {meta.get('ttl_minutes')}")
    print(f"safe apply candidates: {len(candidates)}")
    print()
    if not issues:
        print("No claim/state issues found.")
        return
    print(f"{'SEV':<6} {'CODE':<38} {'BOT':<14} {'SYMBOL':<10} DETAIL")
    print("-" * 78)
    order = {"ERROR": 0, "WARN": 1, "INFO": 2}
    for issue in sorted(issues, key=lambda i: (order.get(i.severity, 9), i.bot, i.symbol, i.code)):
        print(
            f"{issue.severity:<6} {issue.code:<38} "
            f"{issue.bot:<14} {issue.symbol:<10} {issue.detail}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="delete only safe transient/junk claim rows")
    parser.add_argument("--ttl-minutes", type=int, default=5,
                        help="minimum age for empty CLAIMING/ADOPTING cleanup")
    parser.add_argument("--json", action="store_true",
                        help="print machine-readable JSON report")
    parser.add_argument("--verify-exchange-flat", action="store_true",
                        help="read-only futures exchange check; annotate OPEN claim issues")
    args = parser.parse_args(argv)
    if args.ttl_minutes < 5:
        parser.error("--ttl-minutes must be >= 5")

    exchange_bases = None
    exchange_error = None
    if args.verify_exchange_flat:
        exchange_bases, exchange_error = fetch_exchange_futures_bases()
    issues, candidates, meta = analyze(
        ttl_minutes=args.ttl_minutes,
        exchange_futures_bases=exchange_bases,
        exchange_error=exchange_error,
    )
    deleted = 0
    backup = None
    if args.apply:
        deleted, backup = apply_safe_deletes(ttl_minutes=args.ttl_minutes)

    if args.json:
        print(json.dumps({
            "meta": meta,
            "issues": [issue.__dict__ for issue in issues],
            "safe_apply_candidates": candidates,
            "applied": bool(args.apply),
            "deleted": deleted,
            "backup_path": str(backup) if backup else None,
        }, ensure_ascii=False, indent=2, default=str))
    else:
        _print_report(issues, candidates, meta)
        if args.apply:
            print()
            print(f"APPLY: deleted={deleted} backup={backup or '-'}")

    if any(i.severity == "ERROR" for i in issues):
        return 2
    if issues:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
