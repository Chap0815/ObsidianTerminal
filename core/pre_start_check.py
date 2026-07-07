"""Pre-start safety checks shared by launcher and CLI tools."""
from __future__ import annotations

import importlib.util
import json
import math
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from core.paths import BOT_CONFIG, DB_PATH, PROJECT_ROOT
from core.runtime_status import get_build_info, read_runtime_status


@dataclass(frozen=True)
class CheckIssue:
    severity: str
    code: str
    message: str


def _issue(severity: str, code: str, message: str) -> CheckIssue:
    return CheckIssue(severity=severity, code=code, message=message)


def _read_config() -> tuple[dict, list[CheckIssue]]:
    try:
        with open(BOT_CONFIG, "r", encoding="utf-8-sig") as fh:
            cfg = json.load(fh)
        if not isinstance(cfg, dict):
            return {}, [_issue("error", "config_type",
                               "bot_config.json root is not an object")]
        return cfg, []
    except Exception as e:
        return {}, [_issue("error", "config_read",
                           f"bot_config.json unreadable: {e}")]


def _to_bool(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"true", "1", "yes", "on", "y", "t"}:
            return True
        if s in {"false", "0", "no", "off", "n", "f"}:
            return False
    return None


def _pid_cmdline(pid: int) -> str:
    try:
        import psutil  # type: ignore
        p = psutil.Process(pid)
        return " ".join(p.cmdline())
    except Exception:
        return ""


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil  # type: ignore
        return psutil.pid_exists(pid)
    except Exception:
        pass
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
    except Exception:
        return False


def _state_path(log_dir: str, simulation: bool) -> Path:
    from bot_utils.sim_flag import sim_state_path
    return PROJECT_ROOT / sim_state_path(f"{log_dir}/trades.json", simulation)


def _state_path_for_mode(log_dir: str, simulation: bool) -> Path:
    return _state_path(log_dir, simulation)


def _load_state(path: Path) -> tuple[dict, str | None]:
    if not path.exists():
        return {}, None
    try:
        with path.open("r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
        if data is None:
            return {}, None
        if not isinstance(data, dict):
            return {}, "state file root is not an object"
        return data, None
    except Exception as e:
        return {}, str(e)


def _validate_state(bot_name: str, raw: dict, is_futures: bool) -> list[CheckIssue]:
    if not raw:
        return []
    try:
        from bot_utils.state_persist import (
            validate_futures_state,
            validate_spot_state,
        )
        validator = validate_futures_state if is_futures else validate_spot_state
        _clean, rejected = validator(raw)
    except Exception as e:
        return [_issue("error", "state_validate",
                       f"{bot_name}: state validation crashed: {e}")]
    if rejected:
        return [_issue("error", "state_invalid",
                       f"{bot_name}: invalid state rows: {', '.join(rejected[:10])}")]
    return []


def _db_rows(table: str, where: str = "", params: Iterable = ()) -> tuple[list[dict], str | None]:
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        rows = [dict(r) for r in con.execute(sql, tuple(params)).fetchall()]
        con.close()
        return rows, None
    except Exception as e:
        return [], str(e)


def _check_manifest() -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    manifest = PROJECT_ROOT / "DEPLOY_MANIFEST.json"
    if not manifest.exists():
        return [_issue("error", "manifest_missing",
                       "DEPLOY_MANIFEST.json missing; build cannot be identified")]
    build = get_build_info()
    if build.get("build_id") in ("", "unknown", None):
        issues.append(_issue("error", "manifest_unreadable",
                             "DEPLOY_MANIFEST.json exists but build_id is unknown"))
    for folder in ("bots", "core", "launcher", "config", "data", "logs"):
        if not (PROJECT_ROOT / folder).exists():
            issues.append(_issue("error", "root_incomplete",
                                 f"required folder missing: {folder}"))
    return issues


def _check_runtime(bot_name: str, meta: dict) -> list[CheckIssue]:
    status = read_runtime_status(meta.get("log_dir", ""))
    if not status:
        return []
    pid = int(status.get("pid") or 0)
    state = str(status.get("status") or "")
    cmdline = _pid_cmdline(pid) if pid > 0 else ""
    expected_module = str(meta.get("module") or "")
    if pid > 0 and state in {"starting", "ready"} and _pid_alive(pid):
        if expected_module and expected_module in cmdline:
            return [_issue("error", "bot_already_running",
                           f"{bot_name}: runtime_status reports live pid {pid}")]
        return [_issue("warn", "runtime_pid_reused",
                       f"{bot_name}: runtime_status pid {pid} is alive but cmdline did not match bot")]
    if state in {"starting", "ready"}:
        try:
            path = PROJECT_ROOT / str(meta.get("log_dir", "")) / "runtime_status.json"
            age_sec = time.time() - path.stat().st_mtime
        except Exception:
            age_sec = 0.0
        if age_sec >= 300:
            return [_issue(
                "warn", "runtime_status_stale",
                f"{bot_name}: stale runtime_status says {state} but pid {pid} is not alive"
            )]
    return []


def _check_config(bot_name: str | None,
                  cfg: dict,
                  bot_meta: dict) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    targets = [bot_name] if bot_name else list(bot_meta)
    visible = set((cfg.get("UI") or {}).get("VISIBLE_BOTS") or [])

    for name in targets:
        section = cfg.get(name)
        if not isinstance(section, dict):
            issues.append(_issue("error", "config_section_missing",
                                 f"{name}: config section missing"))
            continue
        sim = _to_bool(section.get("SIMULATION"))
        if sim is None:
            issues.append(_issue("error", "simulation_invalid",
                                 f"{name}: SIMULATION is not a boolean"))
            continue
        if not sim and name not in visible:
            issues.append(_issue("error", "hidden_live_bot",
                                 f"{name}: LIVE but hidden in UI.VISIBLE_BOTS"))
        for key in ("POSITION_SIZE", "MAX_OPEN_TRADES", "MAX_DAILY_LOSS"):
            try:
                val = float(section.get(key))
                if not math.isfinite(val):
                    raise ValueError("not finite")
            except Exception:
                issues.append(_issue("error", "config_numeric",
                                     f"{name}: {key} is not numeric/finite"))
        if "TRAILING_DISTANCE" in section and "ACTIVATION_PROFIT" in section:
            try:
                td = float(section.get("TRAILING_DISTANCE"))
                ap = float(section.get("ACTIVATION_PROFIT"))
                if td >= ap:
                    issues.append(_issue("error", "trailing_invalid",
                                         f"{name}: TRAILING_DISTANCE >= ACTIVATION_PROFIT"))
                if "POST_PARTIAL_TRAILING_DISTANCE" in section:
                    ptd = float(section.get("POST_PARTIAL_TRAILING_DISTANCE"))
                    if ptd <= 0:
                        issues.append(_issue("error", "post_partial_trailing_invalid",
                                             f"{name}: POST_PARTIAL_TRAILING_DISTANCE <= 0"))
                    elif ptd >= ap:
                        issues.append(_issue("error", "post_partial_trailing_invalid",
                                             f"{name}: POST_PARTIAL_TRAILING_DISTANCE >= ACTIVATION_PROFIT"))
            except Exception:
                issues.append(_issue("error", "trailing_numeric",
                                     f"{name}: trailing/activation not numeric"))
    return issues


def _check_module(bot_name: str, meta: dict) -> list[CheckIssue]:
    module = str(meta.get("module") or "")
    if not module:
        return [_issue("error", "module_missing", f"{bot_name}: module missing")]
    if importlib.util.find_spec(module) is None:
        return [_issue("error", "module_not_found",
                       f"{bot_name}: module not importable: {module}")]
    return []


def _check_state_and_claims(bot_name: str | None,
                            cfg: dict,
                            bot_meta: dict) -> list[CheckIssue]:
    issues: list[CheckIssue] = []
    targets = [bot_name] if bot_name else list(bot_meta)
    all_claims, err = _db_rows("bot_open_positions")
    if err:
        issues.append(_issue("error", "db_claims_read",
                             f"bot_open_positions unreadable: {err}"))
        all_claims = []
    all_fstate, err = _db_rows("futures_state")
    if err:
        issues.append(_issue("error", "db_futures_state_read",
                             f"futures_state unreadable: {err}"))
        all_fstate = []

    for name in targets:
        meta = bot_meta[name]
        section = cfg.get(name) if isinstance(cfg.get(name), dict) else {}
        sim = _to_bool(section.get("SIMULATION"))
        if sim is None:
            sim = True
        state_path = _state_path(str(meta.get("log_dir")), sim)
        raw, state_err = _load_state(state_path)
        if state_err:
            issues.append(_issue("error", "state_read",
                                 f"{name}: {state_path} unreadable: {state_err}"))
            continue
        issues.extend(_validate_state(name, raw, bool(meta.get("is_futures"))))

        other_path = _state_path_for_mode(str(meta.get("log_dir")), not sim)
        other_raw, other_err = _load_state(other_path)
        if other_err:
            issues.append(_issue("warn", "inactive_state_read",
                                 f"{name}: inactive mode state unreadable: {other_path}: {other_err}"))
        elif other_raw:
            mode = "SIM" if not sim else "LIVE"
            issues.append(_issue("error", "inactive_state_present",
                                 f"{name}: stale {mode} state exists while configured for {'SIM' if sim else 'LIVE'}: {other_path}"))

        bases = {str(s).split("/")[0].split(":")[0].upper()
                 for s in raw.keys()}
        claims = [r for r in all_claims if r.get("bot_name") == name]
        claim_bases = {str(r.get("symbol") or "").split("/")[0].split(":")[0].upper()
                       for r in claims}
        if sim and claims:
            issues.append(_issue("error", "sim_bot_has_live_claims",
                                 f"{name}: SIM but has live claim rows: {sorted(claim_bases)}"))
        elif not sim and claim_bases - bases:
            missing = []
            recoverable = []
            for r in claims:
                base = str(r.get("symbol") or "").split("/")[0].split(":")[0].upper()
                if base in bases:
                    continue
                state = str(r.get("state") or "").upper()
                try:
                    amount = float(r.get("amount") or 0)
                    invested = float(r.get("invested_usdt") or 0)
                except (TypeError, ValueError):
                    amount = invested = 0.0
                if state in {"CLAIMING", "ADOPTING"} and amount <= 0 and invested <= 0:
                    recoverable.append(base)
                else:
                    missing.append(base)
            if recoverable:
                issues.append(_issue(
                    "warn", "pending_claims_without_state",
                    f"{name}: pending claim(s) without JSON state: "
                    f"{sorted(set(recoverable))}; startup reconciliation will resync/adopt"))
            if missing:
                issues.append(_issue("error", "stale_claims",
                                     f"{name}: claims without state rows: {sorted(set(missing))}"))

        if meta.get("is_futures"):
            f_rows = [r for r in all_fstate if r.get("bot_name") == name]
            f_bases = {str(r.get("symbol") or "").split("/")[0].split(":")[0].upper()
                       for r in f_rows}
            if f_bases - bases:
                issues.append(_issue("warn", "stale_futures_state",
                                     f"{name}: futures_state rows not in JSON state: {sorted(f_bases - bases)}"))
    return issues


def run_pre_start_checks(bot_name: str | None = None) -> list[CheckIssue]:
    from launcher.config.settings import BOT_META

    issues: list[CheckIssue] = []
    cfg, cfg_issues = _read_config()
    issues.extend(cfg_issues)
    issues.extend(_check_manifest())
    if bot_name and bot_name not in BOT_META:
        issues.append(_issue("error", "unknown_bot", f"unknown bot: {bot_name}"))
        return issues
    if cfg:
        issues.extend(_check_config(bot_name, cfg, BOT_META))
        issues.extend(_check_state_and_claims(bot_name, cfg, BOT_META))
    for name in ([bot_name] if bot_name else list(BOT_META)):
        if name:
            meta = BOT_META[name]
            issues.extend(_check_module(name, meta))
            issues.extend(_check_runtime(name, meta))
    return issues


def has_errors(issues: Iterable[CheckIssue]) -> bool:
    return any(i.severity == "error" for i in issues)


def format_issues(issues: Iterable[CheckIssue]) -> list[str]:
    return [f"{i.severity.upper()} {i.code}: {i.message}" for i in issues]


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    bot_name = argv[0].upper() if argv else None
    issues = run_pre_start_checks(bot_name)
    build = get_build_info()
    print(f"Build: {build.get('build_id', 'unknown')} ({build.get('source', 'fallback')})")
    if not issues:
        print("Pre-start check: OK")
        return 0
    for line in format_issues(issues):
        print(line)
    return 1 if has_errors(issues) else 0


if __name__ == "__main__":
    raise SystemExit(main())
