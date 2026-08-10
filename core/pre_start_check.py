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

from bot_utils.config import parse_explicit_bool
from core.paths import BOT_CONFIG, DB_PATH, PROJECT_ROOT
from core.runtime_status import (
    get_build_info,
    read_runtime_status_with_path,
    write_runtime_status,
)


def _reject_json_constant(value: str):
    raise ValueError(f"non-standard JSON constant rejected: {value}")


POSITION_LIMIT_BY_BOT = {
    "SPOT": 500.0,
    "FUTURES": 500.0,
    "CROSS": 500.0,
    "TREND": 2500.0,
    "FUTREND": 2500.0,
}
LEVERAGE_LIMIT_BY_BOT = {
    "FUTURES": (1.0, 10.0),
    "FUTREND": (1.0, 6.0),
    "CROSS": (1.0, 3.0),
}
MAX_DAILY_LOSS_LIMIT = 100_000.0
MAX_DAILY_LOSS_MIN_ABS = 0.01
_PRE_START_CONFIG_JSON_MAX_BYTES = 2 * 1024 * 1024
_PRE_START_STATE_JSON_MAX_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class CheckIssue:
    severity: str
    code: str
    message: str


def _issue(severity: str, code: str, message: str) -> CheckIssue:
    return CheckIssue(severity=severity, code=code, message=message)


def _read_json_bounded(
    path,
    max_bytes: int,
    label: str,
    *,
    reject_constants: bool = False,
):
    with open(path, "rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError(f"{label} JSON exceeds size limit")
    kwargs = {"parse_constant": _reject_json_constant} if reject_constants else {}
    return json.loads(raw.decode("utf-8-sig"), **kwargs)


def _read_config() -> tuple[dict, list[CheckIssue]]:
    try:
        cfg = _read_json_bounded(
            BOT_CONFIG,
            _PRE_START_CONFIG_JSON_MAX_BYTES,
            "bot_config",
            reject_constants=True,
        )
        if not isinstance(cfg, dict):
            return {}, [_issue("error", "config_type",
                               "bot_config.json root is not an object")]
        return cfg, []
    except Exception as e:
        return {}, [_issue("error", "config_read",
                           f"bot_config.json unreadable: {e}")]


def _to_bool(value) -> bool | None:
    return parse_explicit_bool(value)


def _finite_float(value) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("not finite")
    return parsed


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
        data = _read_json_bounded(
            path,
            _PRE_START_STATE_JSON_MAX_BYTES,
            "state file",
        )
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
    con = None
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        rows = [dict(r) for r in con.execute(sql, tuple(params)).fetchall()]
        return rows, None
    except Exception as e:
        return [], str(e)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


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


def _check_runtime(bot_name: str, meta: dict, *, cleanup: bool = False) -> list[CheckIssue]:
    status, status_path = read_runtime_status_with_path(meta.get("log_dir", ""))
    if not status:
        return []
    pid = int(status.get("pid") or 0)
    state = str(status.get("status") or "").lower()
    cmdline = _pid_cmdline(pid) if pid > 0 else ""
    expected_module = str(meta.get("module") or "")
    active_states = {"starting", "started", "ready", "running", "degraded"}
    if pid > 0 and state in active_states and _pid_alive(pid):
        try:
            path = status_path or PROJECT_ROOT / str(meta.get("log_dir", "")) / "runtime_status.json"
            age_sec = time.time() - path.stat().st_mtime
        except Exception:
            age_sec = 0.0
        try:
            from core.process_identity import cmdline_matches_bot
            matches_expected = cmdline_matches_bot(bot_name, cmdline)
        except Exception:
            matches_expected = bool(expected_module and expected_module in cmdline)
        if matches_expected:
            return [_issue("error", "bot_already_running",
                           f"{bot_name}: runtime_status reports live pid {pid}")]
        if age_sec < 300:
            return [_issue(
                "error", "bot_runtime_pid_alive",
                f"{bot_name}: runtime_status is fresh/live with pid {pid} "
                f"but cmdline did not match expected module"
            )]
        return [_issue(
            "warn", "runtime_pid_reused",
            f"{bot_name}: runtime_status pid {pid} is alive but stale/cmdline did not match bot"
        )]
    if state in active_states:
        try:
            path = status_path or PROJECT_ROOT / str(meta.get("log_dir", "")) / "runtime_status.json"
            age_sec = time.time() - path.stat().st_mtime
        except Exception:
            age_sec = 0.0
        if age_sec >= 300:
            if cleanup:
                try:
                    write_runtime_status(
                        meta.get("log_dir", ""),
                        bot_name,
                        "stopped",
                        bool(status.get("simulation", True)),
                        threads={"monitor": False, "scan": False, "reconcile": False},
                        extra={
                            "previous_status": state,
                            "stopped_by": "pre_start_stale_cleanup",
                            "stale_pid": pid,
                            "stale_run_id": str(status.get("run_id") or ""),
                        },
                    )
                except Exception:
                    pass
                msg = "marked stopped"
            else:
                msg = "left unchanged"
            return [_issue(
                "warn", "runtime_status_stale",
                f"{bot_name}: stale runtime_status said {state} but pid {pid} is not alive; {msg}"
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
                val = _finite_float(section.get(key))
                if key == "MAX_OPEN_TRADES" and not (1.0 <= val <= 50.0):
                    issues.append(_issue(
                        "error", "max_open_trades_invalid",
                        f"{name}: MAX_OPEN_TRADES={val} outside 1-50"))
                if key == "MAX_DAILY_LOSS" and not (
                    -MAX_DAILY_LOSS_LIMIT <= val <= -MAX_DAILY_LOSS_MIN_ABS
                ):
                    issues.append(_issue(
                        "error", "max_daily_loss_invalid",
                        f"{name}: MAX_DAILY_LOSS={val} outside "
                        f"-{MAX_DAILY_LOSS_LIMIT:g}--{MAX_DAILY_LOSS_MIN_ABS:g}"))
            except Exception:
                issues.append(_issue("error", "config_numeric",
                                     f"{name}: {key} is not numeric/finite"))
        try:
            pos = _finite_float(section.get("POSITION_SIZE"))
            pos_max_raw = section.get("POSITION_SIZE_MAX", pos)
            pos_max = _finite_float(pos_max_raw)
            limit = POSITION_LIMIT_BY_BOT.get(name, 150.0)
            if pos <= 0 or pos > limit:
                issues.append(_issue(
                    "error", "position_size_invalid",
                    f"{name}: POSITION_SIZE={pos} outside 0-{limit}"))
            if pos_max <= 0 or pos_max > limit:
                issues.append(_issue(
                    "error", "position_size_max_invalid",
                    f"{name}: POSITION_SIZE_MAX={pos_max} outside 0-{limit}"))
            if pos > pos_max:
                issues.append(_issue(
                    "error", "position_size_gt_cap",
                    f"{name}: POSITION_SIZE={pos} exceeds POSITION_SIZE_MAX={pos_max}"))
        except Exception:
            issues.append(_issue("error", "position_size_numeric",
                                 f"{name}: POSITION_SIZE/POSITION_SIZE_MAX invalid"))
        lev_limits = LEVERAGE_LIMIT_BY_BOT.get(name)
        if lev_limits and "LEVERAGE" in section:
            try:
                lev = _finite_float(section.get("LEVERAGE"))
                lo, hi = lev_limits
                if not (lo <= lev <= hi):
                    issues.append(_issue(
                        "error", "leverage_invalid",
                        f"{name}: LEVERAGE={lev} outside {lo}-{hi}"))
                if name == "FUTURES" and abs(lev - round(lev)) > 1e-9:
                    issues.append(_issue(
                        "error", "leverage_integer_required",
                        f"{name}: LEVERAGE={lev} must be a whole number"))
            except Exception:
                issues.append(_issue("error", "leverage_numeric",
                                     f"{name}: LEVERAGE is not numeric"))
        try:
            initial_sl = _finite_float(section.get("INITIAL_STOP_LOSS"))
            if not (-100.0 < initial_sl < 0.0):
                issues.append(_issue(
                    "error", "initial_stop_loss_invalid",
                    f"{name}: INITIAL_STOP_LOSS={initial_sl} must be negative and > -100"))
            lev_raw = section.get("LEVERAGE", 1)
            lev = 1.0 if lev_raw is None else _finite_float(lev_raw)
            if lev > 1.0 and initial_sl <= -((100.0 / lev) * 0.9):
                issues.append(_issue(
                    "error", "initial_stop_loss_beyond_liq",
                    f"{name}: INITIAL_STOP_LOSS={initial_sl} sits at/beyond liquidation at {lev:g}x"))
        except Exception:
            issues.append(_issue("error", "initial_stop_loss_numeric",
                                     f"{name}: INITIAL_STOP_LOSS is not numeric"))
        if "PER_LEG_DISASTER_STOP" in section:
            try:
                pds = _finite_float(section.get("PER_LEG_DISASTER_STOP"))
                if pds >= 0.0:
                    issues.append(_issue(
                        "error", "per_leg_disaster_stop_invalid",
                        f"{name}: PER_LEG_DISASTER_STOP={pds} must be negative"))
            except Exception:
                issues.append(_issue("error", "per_leg_disaster_stop_numeric",
                                     f"{name}: PER_LEG_DISASTER_STOP is not numeric"))
        if "TRAILING_DISTANCE" in section and "ACTIVATION_PROFIT" in section:
            try:
                td = _finite_float(section.get("TRAILING_DISTANCE"))
                ap = _finite_float(section.get("ACTIVATION_PROFIT"))
                if td >= ap:
                    issues.append(_issue("error", "trailing_invalid",
                                         f"{name}: TRAILING_DISTANCE >= ACTIVATION_PROFIT"))
                if "POST_PARTIAL_TRAILING_DISTANCE" in section:
                    ptd = _finite_float(section.get("POST_PARTIAL_TRAILING_DISTANCE"))
                    if ptd <= 0:
                        issues.append(_issue("error", "post_partial_trailing_invalid",
                                             f"{name}: POST_PARTIAL_TRAILING_DISTANCE <= 0"))
                    elif ptd >= ap:
                        issues.append(_issue("error", "post_partial_trailing_invalid",
                                             f"{name}: POST_PARTIAL_TRAILING_DISTANCE >= ACTIVATION_PROFIT"))
            except Exception:
                issues.append(_issue("error", "trailing_numeric",
                                     f"{name}: trailing/activation not numeric"))
        if name == "FUTURES" and "PRE_ACTIVATION_GIVEBACK_STOP_ENABLED" in section:
            from trading.futures_peak_trail import validate_peak_trail_config

            _peak_config, peak_error = validate_peak_trail_config(
                enabled=section.get("PRE_ACTIVATION_GIVEBACK_STOP_ENABLED"),
                activation_mfe_pct=section.get("PRE_ACTIVATION_MIN_MFE_PCT"),
                giveback_pct=section.get("PRE_ACTIVATION_GIVEBACK_PCT"),
            )
            if peak_error:
                issues.append(_issue(
                    "error", "pre_activation_peak_trail_invalid",
                    f"FUTURES: invalid peak trail config: {peak_error}"))
        if name == "FUTURES" and "MFE_FALLBACK_STOP_ENABLED" in section:
            from trading.futures_mfe_fallback import (
                validate_mfe_fallback_config,
            )

            _fallback_config, fallback_error = validate_mfe_fallback_config(
                enabled=section.get("MFE_FALLBACK_STOP_ENABLED"),
                min_age_minutes=section.get("MFE_FALLBACK_MIN_AGE_MINUTES"),
                min_mfe_pct=section.get("MFE_FALLBACK_MIN_MFE_PCT"),
                exit_move_pct=section.get("MFE_FALLBACK_EXIT_MOVE_PCT"),
                initial_stop_loss_pct=section.get("INITIAL_STOP_LOSS"),
            )
            if fallback_error:
                issues.append(_issue(
                    "error", "mfe_fallback_invalid",
                    f"FUTURES: invalid MFE fallback config: {fallback_error}"))
        bounded_numeric = (
            ("TREND_VOTE_MIN", 1.0, 3.0),
            ("TREND_EXIT_VOTE", 1.0, 3.0),
            ("TREND_SMA_FAST", 1.0, 5000.0),
            ("TREND_SMA_SLOW", 1.0, 5000.0),
            ("TREND_CROSS_FAST", 1.0, 5000.0),
            ("TREND_CROSS_SLOW", 1.0, 5000.0),
            ("TREND_VOL_TARGET_LOOKBACK", 2.0, 500.0),
            ("TREND_EXIT_STALE_LIMIT", 1.0, 50.0),
            ("MAX_NEW_TRADES_PER_TICK", 0.0, 50.0),
            ("ENTRY_QUALITY_FILTER_ENABLED", 0.0, 1.0),
            ("ENTRY_QUALITY_MIN_SCORE", 0.0, 100.0),
            ("ENTRY_QUALITY_SHADOW_ENABLED", 0.0, 1.0),
            ("ENTRY_QUALITY_SHADOW_MIN_SCORE", 0.0, 100.0),
            ("SPOT_EXIT_SHADOW_ENABLED", 0.0, 1.0),
            ("XSEC_K", 1.0, 15.0),
            ("XSEC_LOOKBACK_HOURS", 6.0, 336.0),
            ("XSEC_REBALANCE_HOURS", 6.0, 336.0),
            ("XSEC_UNIVERSE_SIZE", 10.0, 100.0),
            ("CRASH_WINDOW", 1.0, 50.0),
            ("XSEC_MAX_SPREAD_PCT", 0.01, 10.0),
            ("PORTFOLIO_MAX_GROSS_PCT", 0.0, 1000.0),
            ("PORTFOLIO_MAX_NET_PCT", 0.0, 1000.0),
            ("PORTFOLIO_MIN_FREE_PCT", 0.0, 100.0),
            ("PORTFOLIO_MAX_CLUSTER_PCT", 0.0, 1000.0),
            ("PORTFOLIO_MAX_BETA_PCT", 0.0, 1000.0),
        )
        for key, lo, hi in bounded_numeric:
            if key not in section:
                continue
            try:
                if key in {
                    "ENTRY_QUALITY_FILTER_ENABLED",
                    "ENTRY_QUALITY_SHADOW_ENABLED",
                    "SPOT_EXIT_SHADOW_ENABLED",
                }:
                    bool_val = _to_bool(section.get(key))
                    if bool_val is None:
                        issues.append(_issue(
                            "error", "config_range_invalid",
                            f"{name}: {key}={section.get(key)} outside {lo:g}-{hi:g}"))
                        continue
                    val = 1.0 if bool_val else 0.0
                else:
                    val = _finite_float(section.get(key))
                if not (lo <= val <= hi):
                    issues.append(_issue(
                        "error", "config_range_invalid",
                        f"{name}: {key}={section.get(key)} outside {lo:g}-{hi:g}"))
            except Exception:
                issues.append(_issue(
                    "error", "config_numeric",
                    f"{name}: {key} is not numeric/finite"))
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
            pending_release = []
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
                try:
                    extra = json.loads(r.get("extra_json") or "{}")
                except (TypeError, ValueError):
                    extra = None
                if (
                    isinstance(extra, dict)
                    and extra.get("claim_release_pending") is True
                ):
                    pending_release.append(base)
                elif (
                    state in {"CLAIMING", "ADOPTING"}
                    and amount <= 0
                    and invested <= 0
                ):
                    recoverable.append(base)
                else:
                    missing.append(base)
            if pending_release:
                issues.append(_issue(
                    "warn", "pending_claim_release_without_state",
                    f"{name}: explicitly pending claim release(s) without "
                    f"JSON state: {sorted(set(pending_release))}; startup "
                    f"recovery will compare-and-delete exact marked rows"))
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


def run_pre_start_checks(bot_name: str | None = None,
                         *,
                         cleanup: bool = False) -> list[CheckIssue]:
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
            issues.extend(_check_runtime(name, meta, cleanup=cleanup))
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
