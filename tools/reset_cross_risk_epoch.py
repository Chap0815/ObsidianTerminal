"""Diagnose or explicitly start a NEW CROSS risk epoch after verified recovery.

No order placement/cancellation, automatic migration or historical-return
reconstruction. LIVE coverage includes every Unix-era trigger-order window;
retention, pagination, authentication or budget uncertainty vetoes the reset.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from trading.cross_rebalance_evidence import (  # noqa: E402
    cross_epoch_local_blockers, read_empty_cross_state_evidence, risk_scope,
)

MAX_WINDOWS = 512
MAX_PRIVATE_CALLS = MAX_WINDOWS + 16
MAX_WINDOW_MS = 90 * 24 * 3600 * 1000
TOTAL_TIMEOUT_SECONDS = 120.0
PRIVATE_CALL_SPACING_SECONDS = 0.125  # <=16 calls/2s, below venue's20/2s


class ResetVeto(ValueError):
    """Constant diagnostic reason; never echoes private API response text."""


def _windows(start: int, end: int):
    if type(start) is not int or type(end) is not int or not 0 <= start <= end:
        raise ResetVeto("invalid_epoch_clock")
    count = 0
    while True:
        count += 1
        if count > MAX_WINDOWS:
            raise ResetVeto("plan_window_bound")
        stop = min(end, start + MAX_WINDOW_MS)
        yield start, stop
        if stop == end:
            return
        # Shared millisecond boundary avoids assuming undocumented exclusive
        # endpoint semantics; no history interval can fall between windows.
        start = stop


def _require_empty_native(response):
    if (type(response) is not dict or response.get("success") is not True
            or type(response.get("code")) not in (int, str)
            or response["code"] not in (0, "0")):
        raise ResetVeto("native_response_unverified")
    if set(response) - {"success", "code", "data", "message", "msg"}:
        raise ResetVeto("native_pagination_unverified")
    data = response.get("data")
    if type(data) is dict:
        if set(data) - {"resultList", "totalCount", "totalPage", "currentPage", "pageSize"}:
            raise ResetVeto("native_pagination_unverified")
        if not {"resultList", "totalCount", "totalPage", "currentPage"} <= set(data):
            raise ResetVeto("native_pagination_unverified")
        if (type(data["totalCount"]) is not int or data["totalCount"] != 0
                or type(data["totalPage"]) is not int or data["totalPage"] not in (0, 1)
                or type(data["currentPage"]) is not int or data["currentPage"] != 1):
            raise ResetVeto("native_orders_or_pagination_present")
        data = data["resultList"]
    if type(data) is not list:
        raise ResetVeto("native_rows_unverified")
    # This administrative reset accepts only a genuinely empty result. A
    # nonempty first page is already a veto; no truncation is treated as flat.
    if data:
        raise ResetVeto("native_positions_or_orders_present")


class _NativeProbe:
    def __init__(self, exchange, *, monotonic=time.monotonic, sleep=time.sleep,
                 budget=None):
        if getattr(exchange, "id", None) != "mexc":
            raise ResetVeto("native_venue_unsupported")
        self.exchange = exchange
        self.scope = risk_scope(simulation=False, exchange_id=exchange.id,
                                api_key_identifier=getattr(exchange, "apiKey", None))
        self.monotonic = monotonic
        self.sleep = sleep
        self.budget = budget
        self.started = self._now()
        self.last_clock = self.started
        self.deadline = self.started + TOTAL_TIMEOUT_SECONDS
        self.last_call = None
        self.calls = 0

    def _now(self):
        value = self.monotonic()
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ResetVeto("invalid_monotonic_clock")
        return value

    def _check(self):
        value = self._now()
        if value < self.last_clock or value >= self.deadline:
            raise ResetVeto("native_scan_deadline_or_clock")
        self.last_clock = value
        if risk_scope(simulation=False, exchange_id=self.exchange.id,
                      api_key_identifier=getattr(self.exchange, "apiKey", None)) != self.scope:
            raise ResetVeto("credential_scope_changed")
        return value

    def empty(self, method, params):
        now = self._check()
        if self.calls >= MAX_PRIVATE_CALLS:
            raise ResetVeto("native_call_bound")
        if self.last_call is not None:
            delay = PRIVATE_CALL_SPACING_SECONDS - (now - self.last_call)
            if delay > 0:
                self.sleep(delay)
                if self._check() - self.last_call < PRIVATE_CALL_SPACING_SECONDS - 1e-9:
                    raise ResetVeto("native_pacing_clock_unverified")
        if self.budget is None:
            from bot_utils.api_budget import try_consume_api_call
            allowed = try_consume_api_call("cross_epoch_native_read", return_reservation=True)
        else:
            allowed = self.budget()
        if not allowed:
            raise ResetVeto("native_api_budget_denied")
        self._check()
        function = getattr(self.exchange, method, None)
        if not callable(function):
            raise ResetVeto("native_endpoint_unavailable")
        self.calls += 1
        self.last_call = self._check()
        try:
            response = function(params)
        except Exception as exc:
            # API retention/range errors are never empty historical evidence.
            raise ResetVeto("native_endpoint_or_retention_unverified") from exc
        self._check()
        _require_empty_native(response)


def _live_flat_evidence(exchange, *, epoch_ms=None, probe=None):
    from core.clock import now_ms

    epoch_ms = now_ms if epoch_ms is None else epoch_ms
    probe = _NativeProbe(exchange) if probe is None else probe

    def current_epoch():
        value = epoch_ms()
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ResetVeto("invalid_epoch_clock")
        return int(value)

    def positions():
        probe.empty("contractPrivateGetPositionOpenPositions", {})

    def current_orders():
        probe.empty("contractPrivateGetOrderListOpenOrders", {"page_num": 1, "page_size": 100})
        probe.empty("contractPrivateGetStoporderOpenOrders", {})
        probe.empty("contractPrivateGetTrackorderListOrders",
                    {"states": [0, 1], "pageIndex": 1, "pageSize": 100})

    def plans(start, end):
        probe.empty("contractPrivateGetPlanorderListOrders", {
            "states": "1", "start_time": start, "end_time": end,
            "page_num": 1, "page_size": 100,
        })

    positions()
    current_orders()
    end = current_epoch()
    last_start = 0
    for start, stop in _windows(0, end):
        plans(start, stop)
        last_start = start
    latest = current_epoch()
    if latest < end:
        raise ResetVeto("epoch_clock_rollback")
    for start, stop in _windows(last_start, latest):
        plans(start, stop)
    current_orders()
    positions()
    return {"scope": probe.scope, "observed_monotonic": probe._check()}


def _require_processes_stopped():
    from tools.update_from_git import (
        _running_bots, _running_launchers, _running_tool_processes,
        _running_dashboard_processes,
    )
    if any(check() for check in (
        _running_bots, _running_launchers, _running_tool_processes,
        _running_dashboard_processes,
    )):
        raise ResetVeto("project_processes_running")


def _local_snapshot(*, simulation):
    from core.database import get_connection

    state = read_empty_cross_state_evidence()
    conn = get_connection()
    if conn.in_transaction:
        raise ResetVeto("local_transaction_busy")
    conn.execute("BEGIN")
    try:
        if cross_epoch_local_blockers(conn, simulation=simulation):
            raise ResetVeto("local_positions_or_recovery_present")
        row = conn.execute(
            """SELECT LENGTH(param_value) AS n, SUBSTR(param_value,1,65537) AS value
               FROM bot_params WHERE bot_name='CROSS' AND param_name='REBALANCE_STATE_V2'""",
        ).fetchone()
        if row is not None and (type(row["n"]) is not int or row["n"] > 65_536
                                or type(row["value"]) is not str):
            raise ResetVeto("old_payload_unbounded")
        old = "" if row is None else row["value"]
        hwm = conn.execute("SELECT COALESCE(MAX(id),0) FROM trades").fetchone()[0]
        if type(hwm) is not int or not 0 <= hwm <= (1 << 63) - 1:
            raise ResetVeto("trade_cursor_unverified")
        return {"old_sha256": hashlib.sha256(old.encode("utf-8")).hexdigest(),
                "hwm": hwm, "state": state}
    finally:
        conn.rollback()


def run(*, simulation, reset=False, expected_old_sha256=None, reason=""):
    from bot_utils.sim_flag import read_simulation_flag
    from config.exchange_config import get_active_exchange_name, get_futures_exchange_connection
    from core import database as db
    from core.clock import now_ms
    from core.cross_bot import CrossBot, _encode_rebalance_state
    from core.paths import PROJECT_ROOT
    from update_barrier import update_lifecycle_lock

    if type(simulation) is not bool or read_simulation_flag("CROSS") is not simulation:
        raise ResetVeto("configured_mode_mismatch")
    if not Path(db.DB_PATH).is_file():
        raise ResetVeto("database_not_initialized")
    if reset and (type(reason) is not str or not reason.strip()
                  or len(reason) > 400 or expected_old_sha256 is None):
        raise ResetVeto("explicit_epoch_reason_and_expected_hash_required")
    with update_lifecycle_lock(PROJECT_ROOT, fail_when_locked=True, timeout=0.0):
        _require_processes_stopped()
        initial = _local_snapshot(simulation=simulation)
        if reset and expected_old_sha256 != initial["old_sha256"]:
            raise ResetVeto("expected_old_payload_mismatch")
        native = None
        exchange = None
        outcome = None
        try:
            if simulation:
                scope = risk_scope(simulation=True, exchange_id=get_active_exchange_name())
            else:
                exchange = get_futures_exchange_connection()
                # Only this CLI-owned client is changed; never exchange settings.
                timeout = getattr(exchange, "timeout", 5000)
                if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                    raise ResetVeto("invalid_native_timeout")
                exchange.timeout = min(timeout, 5000)
                native = _live_flat_evidence(exchange)
                scope = native["scope"]
            _require_processes_stopped()
            final = _local_snapshot(simulation=simulation)
            if final != initial:
                raise ResetVeto("local_evidence_changed")
            if not reset:
                outcome = {"status": "eligible_new_epoch", "mode": "SIM" if simulation else "LIVE",
                           "old_sha256": final["old_sha256"], "historical_returns_verified": False}
                return outcome
            if expected_old_sha256 != final["old_sha256"]:
                raise ResetVeto("expected_old_payload_mismatch")
            from bot_utils.config import get_live_value
            host = SimpleNamespace(C=lambda key, default=None: get_live_value("CROSS", key, default))
            current = now_ms()
            if type(current) not in (int, float) or not math.isfinite(current) or current < 0:
                raise ResetVeto("invalid_epoch_clock")
            slot = int(current // (1000 * CrossBot._rebalance_interval_sec(host)))
            payload = _encode_rebalance_state(slot, [], False, risk_evidence={
                "scope": scope, "cursor": final["hwm"], "reentry_slot": None, "pending": None,
            })
            db.reset_cross_risk_epoch(
                payload, mode_is_sim=simulation,
                expected_old_sha256=final["old_sha256"], expected_trade_hwm=final["hwm"],
                expected_state=final["state"], reason=reason, native_evidence=native,
            )
            outcome = {"status": "new_epoch_committed", "mode": "SIM" if simulation else "LIVE",
                       "historical_returns_verified": False}
            return outcome
        finally:
            close = getattr(exchange, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    # An acknowledged database commit must never be reported
                    # as an uncommitted reset merely because HTTP close fails.
                    if outcome is not None:
                        outcome["client_close_ok"] = False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("SIM", "LIVE"))
    parser.add_argument("--reset-epoch", action="store_true")
    parser.add_argument("--expected-old-sha256")
    parser.add_argument("--reason", default="")
    args = parser.parse_args(argv)
    try:
        result = run(simulation=args.mode == "SIM", reset=args.reset_epoch,
                     expected_old_sha256=args.expected_old_sha256, reason=args.reason)
    except KeyboardInterrupt:
        print(json.dumps({"status": "veto", "reason": "operator_cancelled"}))
        return 2
    except Exception as exc:
        reason = str(exc) if isinstance(exc, ResetVeto) else "evidence_or_persistence_unavailable"
        print(json.dumps({"status": "veto", "reason": reason}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
