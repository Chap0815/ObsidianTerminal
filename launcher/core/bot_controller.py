"""
Bot-lifecycle commands wired up as plain functions that take the running
:class:`ObsidianApp` as their first argument.

These used to be ``_start_bot``, ``_stop_bot``, ``_restart_bot`` etc.
methods on the app. Pulling them out keeps ``app.py`` from becoming a
2-thousand-line god class while still letting the UI buttons call into
the same logic.

UI / dialog presentation lives in :mod:`launcher.ui.dialogs.shutdown`
this module only owns the start / stop / restart **decisions** and
the async workers they delegate to.
"""

from __future__ import annotations

import sys
import threading
from contextlib import ExitStack

from launcher.config.settings import (
    BOT_META,
    audit_event,
    effective_default_config,
    save_config_merge,
)
from launcher.core.positions import (
    direct_close_remaining_futures,
    direct_close_remaining_spot,
    get_open_futures_positions,
    get_open_spot_positions,
    mode_switch_blockers,
)
from launcher.core.runtime_status_values import positive_int_or_zero
from launcher.state.poller import _runtime_or_config_sim
from launcher.ui.logging_panel import log_to_card
from core.pre_start_check import (
    format_issues,
    has_errors,
    run_pre_start_checks,
)


_RESTART_UI_HANDOFF_TIMEOUT_SECONDS = 10.0


def _post_ui(app, callback, *, delay_ms: int = 0) -> bool:
    post = getattr(app, "post_ui", None)
    if callable(post):
        return post(callback, delay_ms=delay_ms) is not False
    app.after(delay_ms, callback)
    return True


def _start_critical_worker(app, target, *, name: str):
    registry = getattr(app, "critical_workers", None)
    start = getattr(registry, "start", None)
    if callable(start):
        return start(target, name=name, daemon=True)
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def _bounded_exception_summary(exc: BaseException, limit: int = 240) -> str:
    """Return a one-line, redacted UI diagnostic for lifecycle failures."""
    kind = type(exc).__name__
    try:
        detail = str(exc).replace("\r", " ").replace("\n", " ")
    except Exception:
        detail = ""
    try:
        from core.logger import redact

        detail = redact(detail)
    except Exception:
        # Error details can contain command lines or environment-derived
        # values. If the redactor itself is unavailable, retain only the
        # exception type rather than risking a credential in the UI.
        detail = ""
    detail = detail[:max(0, int(limit))].strip()
    return f"{kind}: {detail}" if detail else kind


def _fmt_num(value, default=0, precision=1) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = float(default)
    if number.is_integer():
        return str(int(number))
    return f"{number:.{precision}f}".rstrip("0").rstrip(".")


def _fmt_int(value, default=0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _release_dead_process_close_locks(
    process_identity: tuple[int, str] | None,
) -> None:
    if process_identity is None:
        return
    try:
        pid, run_id = process_identity
        from core.database import release_advisory_locks_for_dead_process
        release_advisory_locks_for_dead_process(
            int(pid), str(run_id), "close:"
        )
    except Exception:
        pass


def _stop_bot_process_verified(
    bot,
    *,
    graceful_close: bool,
) -> tuple[int, str] | None:
    """Stop one bot and return its proven-dead process incarnation."""
    prior_pid = None
    prior_run_id = ""
    try:
        prior_proc = getattr(bot, "proc", None)
        prior_pid = getattr(prior_proc, "pid", None)
        prior_run_id = str(getattr(bot, "run_id", "") or "")
    except Exception:
        prior_pid = None
        prior_run_id = ""
    stop_error: Exception | None = None
    try:
        stopped_pid = bot.stop(graceful_close=graceful_close)
    except Exception as exc:
        stopped_pid = None
        stop_error = exc
    try:
        still_running = bool(bot.is_running())
    except Exception as exc:
        raise RuntimeError("bot stop outcome could not be verified") from exc
    if still_running:
        raise RuntimeError("bot is still running after stop escalation") from stop_error
    if stop_error is not None:
        stderr = sys.stderr
        if stderr is not None:
            try:
                stderr.write(
                    f"[Stop] process exited despite stop error: {stop_error}\n"
                )
            except Exception:
                pass
    resolved_pid = stopped_pid if stopped_pid is not None else prior_pid
    if (
        isinstance(resolved_pid, bool)
        or not isinstance(resolved_pid, int)
        or not 1 <= resolved_pid <= 2_147_483_647
    ):
        return None
    if (
        prior_pid is not None
        and (
            isinstance(prior_pid, bool)
            or not isinstance(prior_pid, int)
            or prior_pid != resolved_pid
        )
    ):
        raise RuntimeError("bot stop returned a different process identity")
    normalized_run_id = prior_run_id.lower()
    if (
        len(normalized_run_id) != 32
        or not normalized_run_id.isascii()
        or any(
            char not in "0123456789abcdef"
            for char in normalized_run_id
        )
    ):
        return None
    return resolved_pid, normalized_run_id


def format_start_params(name: str, snapshot: dict) -> str:
    pos = _fmt_num(snapshot.get("POSITION_SIZE"), 0)
    max_open = _fmt_int(snapshot.get("MAX_OPEN_TRADES", 5), 5)

    if name == "TREND":
        return (
            f"Trend check {snapshot.get('TREND_CHECK_HOURS', '?')}h | "
            f"SMA {snapshot.get('TREND_SMA_FAST', '?')}/"
            f"{snapshot.get('TREND_SMA_SLOW', '?')} | "
            f"Cross {snapshot.get('TREND_CROSS_FAST', '?')}/"
            f"{snapshot.get('TREND_CROSS_SLOW', '?')} | "
            f"Vote {snapshot.get('TREND_VOTE_MIN', '?')}/"
            f"{snapshot.get('TREND_EXIT_VOTE', '?')} | "
            f"Pos {pos}USDT | Max {max_open} trades"
        )

    if name == "CROSS":
        return (
            f"Cross K {snapshot.get('XSEC_K', '?')}/side | "
            f"Lookback {snapshot.get('XSEC_LOOKBACK_HOURS', '?')}h | "
            f"Rebalance {snapshot.get('XSEC_REBALANCE_HOURS', '?')}h | "
            f"Universe {snapshot.get('XSEC_UNIVERSE_SIZE', '?')} | "
            f"Capital {_fmt_num(snapshot.get('BASE_CAPITAL_USDT'), 0)}USDT | "
            f"MaxGross {_fmt_num(snapshot.get('MAX_GROSS_EXPOSURE_PCT'), 0)}% | "
            f"Leverage {_fmt_num(snapshot.get('LEVERAGE'), 1)}x"
        )

    if name == "FUTREND":
        return (
            f"TF {snapshot.get('TREND_TIMEFRAME', '?')} | "
            f"SMA {snapshot.get('TREND_SMA_FAST', '?')}/"
            f"{snapshot.get('TREND_SMA_SLOW', '?')} | "
            f"Cross {snapshot.get('TREND_CROSS_FAST', '?')}/"
            f"{snapshot.get('TREND_CROSS_SLOW', '?')} | "
            f"Vote {snapshot.get('TREND_VOTE_MIN', '?')}/"
            f"{snapshot.get('TREND_EXIT_VOTE', '?')} | "
            f"TP+{_fmt_num(snapshot.get('ACTIVATION_PROFIT'), 0)}% | "
            f"Trail {_fmt_num(snapshot.get('TRAILING_DISTANCE'), 0)}% | "
            f"Stop {_fmt_num(snapshot.get('INITIAL_STOP_LOSS'), 0)}% | "
            f"Pos {pos}USDT | Max {max_open} trades | "
            f"Leverage {_fmt_num(snapshot.get('LEVERAGE'), 1)}x | "
            f"LiqBuffer {_fmt_num(snapshot.get('LIQ_SAFETY_PCT'), 0)}%"
        )

    param_line = (
        f"Pump>={_fmt_num(snapshot.get('MIN_PUMP'), 0)}% | "
        f"TP+{_fmt_num(snapshot.get('ACTIVATION_PROFIT'), 0)}% | "
        f"Trail {_fmt_num(snapshot.get('TRAILING_DISTANCE'), 0)}% | "
        f"Stop {_fmt_num(snapshot.get('INITIAL_STOP_LOSS'), 0)}% | "
        f"Pos {pos}USDT | Max {max_open} trades"
    )
    if BOT_META[name]["is_futures"]:
        param_line += (
            f" | Leverage {_fmt_num(snapshot.get('LEVERAGE'), 3)}x"
            f" | LiqBuffer {_fmt_num(snapshot.get('LIQ_SAFETY_PCT'), 15)}%"
        )
    return param_line


def _wait_for_bot_ready(name: str, bot, timeout_sec: float = 180.0) -> tuple[bool, str]:
    import time as _time
    try:
        from core.runtime_status import read_runtime_status
    except Exception as e:
        return False, f"readiness unavailable: {e}"

    log_dir = BOT_META[name]["log_dir"]
    run_id = getattr(bot, "run_id", None) or ""
    deadline = _time.monotonic() + timeout_sec
    last = {}
    while _time.monotonic() < deadline:
        if not bot.is_running():
            return False, "process exited before readiness"
        if str(getattr(bot, "run_id", None) or "") != str(run_id):
            return False, "superseded by newer run"
        last = read_runtime_status(log_dir)
        threads = last.get("threads") if isinstance(last.get("threads"), dict) else {}
        if (last.get("status") == "ready"
                and str(last.get("run_id") or "") == str(run_id)
                and positive_int_or_zero(last.get("pid")) > 0
                and all(bool(threads.get(k))
                        for k in ("monitor", "scan", "reconcile"))):
            return True, str(last.get("build_id") or "unknown")
        _time.sleep(0.25)
    status = last.get("status") or "missing"
    seen_run = last.get("run_id") or ""
    return False, f"timeout waiting for ready (status={status}, run_id={seen_run})"


def _has_unsaved_params(app, name: str) -> bool:
    try:
        card = app.cards.get(name) or {}
        lbl = card.get("unsaved_lbl")
        return bool(str(lbl.cget("text") or "").strip())
    except Exception:
        return False


def _save_current_config(app, name: str) -> None:
    if not _has_unsaved_params(app, name):
        # Persist missing DEFAULT_CONFIG keys before starting. load_config()
        # fills them only in memory; without a write, a bot with older own
        # hardcoded defaults can still start with stale values.
        defaults = effective_default_config().get(name, {})
        section = app.config.get(name) if isinstance(app.config, dict) else {}
        section = section if isinstance(section, dict) else {}
        missing = [
            key for key in defaults
            if key not in section
        ] if isinstance(defaults, dict) else []
        if missing:
            app.config = save_config_merge({name: {}})
        return
    dirty = getattr(app, "_dirty_param_keys", {}).get(name) or set()
    if dirty:
        rows = getattr(app, "param_rows", {}).get(name, {})
        updates = {k: rows[k].value for k in dirty if k in rows}
        app.config = save_config_merge({name: updates})
    else:
        app.config = save_config_merge(section_replacements={
            name: dict(app.config.get(name, {})),
        })


#  Start 

def start_bot(app, name: str) -> None:
    """Start ``name``'s subprocess if it isn't already running."""
    card = app.cards[name]
    bot = app.bots[name]
    if bot.is_running():
        log_to_card(card, "system", f"{name} already running")
        return
    try:
        _save_current_config(app, name)
    except Exception as e:
        log_to_card(card, "error", f"Config save failed - start aborted: {e}")
        return
    app._mark_dirty(name, False)
    snapshot = dict(app.config.get(name, {}))

    # A real launcher start is the safe ownership boundary for retiring a
    # crashed predecessor's stale heartbeat. Standalone diagnostics remain
    # read-only because run_pre_start_checks() still defaults cleanup=False.
    try:
        issues = run_pre_start_checks(name, cleanup=True)
    except Exception as exc:
        log_to_card(
            card,
            "error",
            "Pre-start check failed - start aborted: "
            f"{_bounded_exception_summary(exc)}",
        )
        return
    if issues:
        for line in format_issues(issues):
            severity = "error" if line.startswith("ERROR ") else "warn"
            log_to_card(card, severity, f"Pre-start: {line}")
        if has_errors(issues):
            log_to_card(card, "error", "Start aborted by pre-start check")
            return

    # Close the previous run's display-only aggregation and start the new run
    # with fresh warning/state fingerprints.  This never touches durable logs.
    display_filter = card.get("log_filter")
    try:
        flush = getattr(display_filter, "flush", None)
        if callable(flush):
            for pending_line in flush():
                log_to_card(card, "info", pending_line)
    except Exception:
        # A cosmetic display helper must never prevent a validated bot start.
        pass
    try:
        reset = getattr(display_filter, "reset", None)
        if callable(reset):
            reset()
    except Exception:
        # Reset failure is display-only and must not stop the bot either.
        pass
    log_to_card(card, "system", "Loading: " + format_start_params(name, snapshot))
    try:
        bot.start(current_config_snapshot=snapshot)
    except Exception as exc:
        # A Windows CreateProcess/guard/thread-start failure belongs to this
        # button action. Do not let it escape through Tk's callback dispatcher
        # and leave the operator with only a console traceback.
        log_to_card(
            card,
            "error",
            f"Start failed: {_bounded_exception_summary(exc)}",
        )
        return

    def _ready_worker():
        ready, detail = _wait_for_bot_ready(name, bot)
        if detail == "superseded by newer run":
            return
        if ready:
            _post_ui(app, lambda: log_to_card(
                card, "system", f"Started - ready ({detail})"))
        elif bot.is_running():
            _post_ui(app, lambda: log_to_card(
                card, "warn", f"Started but not ready: {detail}"))
        else:
            _post_ui(app, lambda: log_to_card(
                card, "error", f"Start failed: {detail}"))

    try:
        threading.Thread(
            target=_ready_worker,
            name=f"ready-{name}",
            daemon=True,
        ).start()
    except Exception as exc:
        # The subprocess has already started successfully. A local diagnostic
        # thread failure must not escape the Tk callback or imply that the bot
        # itself failed to start.
        log_to_card(
            card,
            "warn",
            "Started; readiness monitor unavailable: "
            f"{_bounded_exception_summary(exc)}",
        )


#  Stop 

def _position_modes_for_stop(name: str) -> list[bool]:
    try:
        primary = bool(_runtime_or_config_sim(name))
    except Exception:
        primary = True
    return [primary, not primary]


def open_positions_for_stop(name: str) -> list:
    """Read both SIM/LIVE state buckets for Stop/Quit fail-closed checks.

    Runtime mode is probed first, but stale config must never make an open LIVE
    position invisible and route Stop into the instant hard-terminate path.
    """
    out: list = []
    seen: set[tuple] = set()
    read_errors: list[Exception] = []
    for is_sim in _position_modes_for_stop(name):
        try:
            if BOT_META[name].get("is_futures"):
                rows = get_open_futures_positions(
                    name, mode_is_sim=is_sim, strict=True)
            else:
                rows = get_open_spot_positions(
                    name, mode_is_sim=is_sim, strict=True)
        except Exception as exc:
            read_errors.append(exc)
            rows = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or row.get("base") or "")
            key = (symbol, bool(is_sim))
            if key in seen:
                continue
            seen.add(key)
            item = dict(row)
            item.setdefault("mode", "SIM" if is_sim else "LIVE")
            out.append(item)
    if read_errors:
        raise RuntimeError(
            f"{name}: position state read failed; stop/quit cannot prove flat"
        ) from read_errors[0]
    return out


def close_modes_for_stop(name: str) -> list[bool]:
    """Return SIM/LIVE buckets that currently contain positions for ``name``.

    Falls back to the runtime/config mode only when no state rows are visible.
    This keeps the direct close fallback aligned with the fail-closed position
    search and avoids stale config hiding a LIVE book.
    """
    modes: list[bool] = []
    rows = open_positions_for_stop(name)
    for row in rows or []:
        mode = str(row.get("mode") or "").upper()
        if mode == "LIVE":
            val = False
        elif mode == "SIM":
            val = True
        else:
            continue
        if val not in modes:
            modes.append(val)
    if modes:
        return modes
    return _position_modes_for_stop(name)[:1]


def stop_bot(app, name: str) -> None:
    """Stop ``name``'s subprocess.

      No open positions  stop IMMEDIATELY without any modal dialog.
        Uses terminate() not graceful  there's nothing to clean up.
      Open positions  show a position-confirmation dialog; only
        after the user confirms do we call graceful shutdown.

    The instant-stop path runs off the UI thread (no grab_set busy-modal)
    so the other bot cards stay responsive.
    """
    from launcher.ui.dialogs.shutdown import (
        show_state_read_error_dialog,
        show_futures_stop_dialog,
        show_spot_stop_dialog,
    )
    card = app.cards[name]
    bot = app.bots[name]
    if not bot.is_running():
        log_to_card(card, "system", f"{name} not running")
        return

    #  Check for open positions 
    # CROSS is a futures-type bot (is_futures=True)  it stores positions in
    # futures_state, NOT spot trades.json. Branch on is_futures (not == FUTURES)
    # so the CROSS stop/close goes through the FUTURES path that books realized
    # PnL and clears futures_state, rather than trying to spot-sell perps.
    if BOT_META[name].get("is_futures"):
        try:
            open_positions = open_positions_for_stop(name)
        except Exception as exc:
            log_to_card(card, "error", f"Stop aborted: {exc}")
            show_state_read_error_dialog(app, f"Stop {name}", str(exc))
            return
        if open_positions:
            # Has positions  confirm dialog blocks until user decides
            show_futures_stop_dialog(app, name, open_positions)
            return
    else:
        try:
            open_spot = open_positions_for_stop(name)
        except Exception as exc:
            log_to_card(card, "error", f"Stop aborted: {exc}")
            show_state_read_error_dialog(app, f"Stop {name}", str(exc))
            return
        if open_spot:
            show_spot_stop_dialog(app, name, open_spot)
            return

    #  No open positions  instant stop 
    # We run terminate() in a daemon thread so the UI never blocks even
    # for the brief ~1s that subprocess.terminate() takes on Windows.
    log_to_card(card, "system", "Stopping bot")

    def _instant_stop_worker():
        try:
            with bot.exclusive_stop_operation():
                # graceful_close=False uses terminate() without invoking the
                # bot's emergency-close handler. Keep restart blocked until
                # the old OS process is proven dead.
                stopped_pid = _stop_bot_process_verified(
                    bot,
                    graceful_close=False,
                )
                _release_dead_process_close_locks(stopped_pid)
            _post_ui(app, lambda: log_to_card(card, "system", "Stopped"))
        except Exception as e:
            _post_ui(app, lambda err=e: log_to_card(
                card, "warn", f"Stop failed: {err}"))

    _start_critical_worker(
        app,
        _instant_stop_worker,
        name=f"stop-{name}",
    )


#  Restart 

def restart_bot(app, name: str) -> None:
    """Restart with the current parameters.

      Restart always reloads the current config (save before restart).
      Restart NEVER asks about open positions  it's just a process
        cycle, the bot itself picks the positions back up from state.
      Stop quickly via terminate() (no graceful close  positions stay
        in trades.json and are reconciled on startup).

    Sequence:
      1. save_config()  persist the new params to bot_config.json
      2. terminate the old  fast, ~1s on Windows via TerminateProcess
      3. wait for poll==None (typically <500ms)
      4. start_bot()  reads fresh config and restarts
    """
    card = app.cards[name]
    bot = app.bots[name]
    in_flight = getattr(app, "_restart_in_progress", set())
    if name in in_flight:
        log_to_card(card, "warn", f"{name} restart already in progress")
        return
    app._restart_in_progress = in_flight

    # Persist current params first  the new subprocess will read them
    try:
        _save_current_config(app, name)
    except Exception as e:
        log_to_card(card, "error", f"Config save failed - restart aborted: {e}")
        return
    app._mark_dirty(name, False)

    # If the bot isn't running, "restart" degenerates to "start"
    if not bot.is_running():
        start_bot(app, name)
        return

    audit_event("restart_apply", bot=name,
                restart_required=config_changed_since_start(app, name))
    log_to_card(card, "system", "Restarting (config reloaded)")
    in_flight.add(name)

    def _restart_worker():
        try:
            # Step 1: terminate the running process (fast path, no emergency
            # close). The state file already has open positions, the next
            # start_bot will reconcile them.
            try:
                with bot.exclusive_stop_operation():
                    stopped_pid = _stop_bot_process_verified(
                        bot,
                        graceful_close=False,
                    )
                    _release_dead_process_close_locks(stopped_pid)
            except Exception as e:
                _post_ui(app, lambda err=e: log_to_card(
                    card, "warn", f"Restart aborted: {err}"))
                return

            # Step 2: start with the new config (on the UI thread, since
            # start_bot touches widgets). Keep the critical worker and the
            # per-bot restart claim alive until that handoff has actually run.
            # Otherwise a clean launcher close between queueing and dispatch
            # can strand the already-stopped bot with no remaining owner.
            handoff_complete = threading.Event()
            handoff_lock = threading.Lock()
            handoff_state = {"cancelled": False, "started": False}

            def _start_on_ui() -> None:
                with handoff_lock:
                    if handoff_state["cancelled"]:
                        return
                    handoff_state["started"] = True
                try:
                    start_bot(app, name)
                finally:
                    handoff_complete.set()

            accepted = _post_ui(app, _start_on_ui)
            if accepted:
                try:
                    handoff_timeout = max(
                        0.01,
                        float(_RESTART_UI_HANDOFF_TIMEOUT_SECONDS),
                    )
                except (TypeError, ValueError, OverflowError):
                    handoff_timeout = 10.0
                if not handoff_complete.wait(timeout=handoff_timeout):
                    cancelled_before_start = False
                    with handoff_lock:
                        if not handoff_state["started"]:
                            handoff_state["cancelled"] = True
                            cancelled_before_start = True
                    if cancelled_before_start:
                        message = (
                            "Restart aborted: UI start handoff timed out; "
                            "bot remains stopped"
                        )
                        stderr = sys.stderr
                        if stderr is not None:
                            try:
                                stderr.write(f"[Restart] {name}: {message}\n")
                            except Exception:
                                pass
                        _post_ui(
                            app,
                            lambda msg=message: log_to_card(card, "error", msg),
                        )
                    else:
                        # The UI callback acquired ownership immediately before
                        # the deadline. Retain the claim until start_bot() has
                        # actually returned; cancelling mid-start could permit
                        # a concurrent stop/restart against a partial launch.
                        handoff_complete.wait()
            else:
                stderr = sys.stderr
                if stderr is not None:
                    try:
                        stderr.write(
                            f"[Restart] {name}: UI start handoff rejected; "
                            "bot remains stopped\n"
                        )
                    except Exception:
                        pass
        finally:
            in_flight.discard(name)

    _start_critical_worker(
        app,
        _restart_worker,
        name=f"restart-{name}",
    )


#  Simulation toggle helper 

def apply_simulation(app, bot_name: str, sim: bool, card: dict) -> None:
    """Persist the SIM/LIVE flag and re-style the pill button accordingly."""

    bot = app.bots.get(bot_name)
    if bot is not None and bot.is_running():
        start_cfg = bot.start_config or {}
        actual_sim = bool(start_cfg.get(
            "SIMULATION", app.config[bot_name].get("SIMULATION", True)))
        log_to_card(card, "warn",
                    "Stop or restart the bot before switching SIM/LIVE mode")
        app.config[bot_name]["SIMULATION"] = actual_sim
        btn = card["sim_btn"]
        if actual_sim:
            btn.configure(text="  SIM  ", fg_color="#0c2a3a",
                          hover_color="#0e3850", text_color="#22d3ee")
        else:
            btn.configure(text="  LIVE  ", fg_color="#2d0a0a",
                          hover_color="#3d1010", text_color="#f87171")
        return

    blockers = mode_switch_blockers(bot_name)
    if blockers:
        log_to_card(
            card, "error",
            "SIM/LIVE switch blocked: close or repair open state first "
            f"({', '.join(blockers)})",
        )
        btn = card["sim_btn"]
        current_sim = bool(app.config[bot_name].get("SIMULATION", True))
        if current_sim:
            btn.configure(text="  SIM  ", fg_color="#0c2a3a",
                          hover_color="#0e3850", text_color="#22d3ee")
        else:
            btn.configure(text="  LIVE  ", fg_color="#2d0a0a",
                          hover_color="#3d1010", text_color="#f87171")
        return

    app.config[bot_name]["SIMULATION"] = sim
    try:
        app.config = save_config_merge({bot_name: {"SIMULATION": sim}})
    except Exception as e:
        log_to_card(card, "error", f"Config save failed - mode unchanged: {e}")
        app.config[bot_name]["SIMULATION"] = not sim
        return
    btn = card["sim_btn"]
    if sim:
        btn.configure(text="  SIM  ", fg_color="#0c2a3a", hover_color="#0e3850",
                        text_color="#22d3ee")
    else:
        btn.configure(text="  LIVE  ", fg_color="#2d0a0a", hover_color="#3d1010",
                        text_color="#f87171")
    mode = "SIMULATION" if sim else "LIVE"
    log_to_card(card, "warn" if not sim else "system",
                f"Mode switched to {mode}  restart bot to apply")


#  Parameter-change detection 

def config_changed_since_start(app, name: str) -> bool:
    """Return ``True`` if the user changed any parameter since the bot
    was started  used to show the "restart required" hint."""
    return bool(config_restart_required_reason(app, name))


def config_restart_required_reason(app, name: str) -> str:
    """Human-readable restart reason for params that differ from boot config."""
    bot = app.bots[name]
    if not bot.is_running() or bot.start_config is None:
        return ""
    current = app.config.get(name, {})
    changed = []
    for key, val in current.items():
        try:
            if abs(bot.start_config.get(key, val) - val) > 1e-6:
                changed.append(key)
        except TypeError:
            if bot.start_config.get(key, val) != val:
                changed.append(key)
    if not changed:
        return ""
    if "LEVERAGE" in changed:
        return "Leverage changed - restart required"
    if "SIMULATION" in changed:
        return "Mode changed - restart required"
    return "Parameters changed - restart required"


#  Async workers (run on background threads) 

def async_simple_stop(app, name: str, card: dict, update) -> None:
    """Keep restart blocked until the verified hard stop is complete."""
    with app.bots[name].exclusive_stop_operation():
        _async_simple_stop_owned(app, name, card, update)


def _async_simple_stop_owned(app, name: str, card: dict, update) -> None:
    """Simple async stop WITHOUT closing positions.

    graceful_close MUST be False here: "Stop without closing" means keep
    positions open on the exchange. graceful_close=True would send
    SIGTERM / CTRL_BREAK_EVENT and trigger the bot's emergency_close_all_*
    handler, which SELLS EVERY OPEN POSITION. With graceful_close=False the
    process is terminated hard (no signal handler runs); trades stay in
    trades.json and the bot reconciles them on next start.
    """
    update("Hard-stopping bot  positions stay OPEN on exchange")
    try:
        # HARD STOP. No graceful close, no bot signal handler.
        stopped_pid = _stop_bot_process_verified(
            app.bots[name], graceful_close=False
        )
        _release_dead_process_close_locks(stopped_pid)
    except Exception as e:
        stderr = sys.stderr
        if stderr is not None:
            try:
                stderr.write(f"[Stop] hard stop {name} failed: {e}\n")
            except Exception:
                pass
        update("Stop failed - bot is still running; positions unchanged")
        _post_ui(app, lambda: log_to_card(
            card,
            "error",
            "Stop failed: bot is still running; no success was reported",
        ))
        return
    update("Done. Positions left open  reconciled on next start.")
    _post_ui(app, lambda: log_to_card(
        card, "system", "Stopped (positions kept open)"
    ))


def async_close_and_stop_spot(app, name: str, update) -> None:
    """Keep restart blocked across stop, fallback close, and accounting."""
    with app.bots[name].exclusive_stop_operation():
        _async_close_and_stop_spot_owned(app, name, update)


def _async_close_and_stop_spot_owned(app, name: str, update) -> None:
    """Graceful shutdown of a spot bot (its own handler closes positions),
    then verify + cleanup."""
    card = app.cards[name]
    close_modes = close_modes_for_stop(name)
    def _log(severity, msg):
        _post_ui(app, lambda: log_to_card(card, severity, msg))

    update("Sending shutdown signal to bot")
    try:
        stopped_pid = _stop_bot_process_verified(
            app.bots[name], graceful_close=True
        )
        _release_dead_process_close_locks(stopped_pid)
    except Exception as e:
        sys.stderr.write(f"[Stop] graceful stop failed: {e}" + "\n")
        _log("error", f"Stop failed; launcher fallback aborted: {e}")
        update("Stop failed - bot is still running; fallback close aborted")
        return
    update("Verifying positions closed")

    # Fallback: if the bot's handler couldn't close everything, do it here in
    # the same SIM/LIVE buckets the pre-stop position scan found.
    failed: list[str] = []
    for sim_only in close_modes:
        result = direct_close_remaining_spot(
            name, _log, sim_only, reason="Manual Close & Stop")
        failed.extend(str(sym) for sym in result.get("failed", []) if sym)
    if failed:
        msg = "Fallback close failed for: " + ", ".join(sorted(set(failed)))
        _log("error", msg)
        update("Close failed - manual review needed")
        return
    update("Done.")


def async_close_and_stop_futures(app, name: str, card: dict, update,
                                  reason: str = "Manual Close & Stop") -> None:
    """Keep restart blocked across stop, fallback close, and accounting."""
    with app.bots[name].exclusive_stop_operation():
        _async_close_and_stop_futures_owned(app, name, card, update, reason)


def _async_close_and_stop_futures_owned(
    app,
    name: str,
    card: dict,
    update,
    reason: str,
) -> None:
    """Graceful shutdown + fallback close for FUTURES."""
    close_modes = close_modes_for_stop(name)
    def _log(severity, msg):
        _post_ui(app, lambda: log_to_card(card, severity, msg))

    update("Sending shutdown signal to bot")
    try:
        stopped_pid = _stop_bot_process_verified(
            app.bots[name], graceful_close=True
        )
        _release_dead_process_close_locks(stopped_pid)
    except Exception as e:
        sys.stderr.write(f"[Stop] graceful stop failed: {e}" + "\n")
        _log("error", f"Stop failed; launcher fallback aborted: {e}")
        update("Stop failed - bot is still running; fallback close aborted")
        return
    update("Verifying all positions closed")

    # Pass bot_name=name so a CROSS "Close & Stop" closes the CROSS book
    # (it defaults to "FUTURES" otherwise).
    failed: list[str] = []
    for sim_only in close_modes:
        result = direct_close_remaining_futures(
            _log, sim_only, reason=reason, bot_name=name)
        failed.extend(str(sym) for sym in result.get("failed", []) if sym)
    if failed:
        msg = "Fallback close failed for: " + ", ".join(sorted(set(failed)))
        _log("error", msg)
        update("Close failed - manual review needed")
        return
    update("Done.")


def async_emergency_close(app, card: dict, bot_was_running: bool, update) -> None:
    """Hold every futures restart barrier through the emergency fallback."""
    futures_bots = [
        name for name, meta in BOT_META.items() if meta.get("is_futures")
    ]
    with ExitStack() as stack:
        for bot_name in futures_bots:
            bot = app.bots.get(bot_name)
            if bot is not None:
                stack.enter_context(bot.exclusive_stop_operation())
        _async_emergency_close_owned(
            app,
            card,
            bot_was_running,
            update,
            futures_bots,
        )


def _async_emergency_close_owned(
    app,
    card: dict,
    bot_was_running: bool,
    update,
    futures_bots: list[str],
) -> None:
    """Emergency Close  all futures-type bots.

      NO confirmation dialog  the dialog already confirmed.
      Close positions DIRECTLY via the exchange, do NOT wait for the
        bot's own emergency-close handler (which can take 30-60s).
      Terminate the bot subprocess immediately afterwards.

    Sequence:
      1. terminate() the bot (fast, ~1s)  stops it from interfering
      2. direct_close_remaining_futures()  fires reduceOnly orders
         against the exchange in parallel with terminate cleanup
      3. log final status

    This trades graceful-close behaviour (each position closed by the
    bot with full DB attribution) for SPEED (~3-5s total). The
    direct-close path still records each trade to the DB  just via
    the launcher's own path, not the bot's.
    """
    close_modes_by_bot: dict[str, list[bool]] = {}
    mode_errors: dict[str, str] = {}

    for bot_name in futures_bots:
        try:
            close_modes_by_bot[bot_name] = close_modes_for_stop(bot_name)
        except Exception as exc:
            # Emergency close is explicitly a last-resort action. If we cannot
            # prove the active namespace, try both buckets after killing the bot
            # and make the degraded path visible.
            close_modes_by_bot[bot_name] = [False, True]
            mode_errors[bot_name] = str(exc)

    def _log(severity, msg):
        _post_ui(app, lambda: log_to_card(card, severity, msg))

    # Step 1: kill futures subprocesses immediately. We don't want them to
    # race with the launcher by trying to close the same positions.
    update("Terminating futures bots")
    stop_failures: list[str] = []
    for bot_name in futures_bots:
        try:
            bot = app.bots.get(bot_name)
            if bot is not None and bot.is_running():
                stopped_pid = _stop_bot_process_verified(
                    bot, graceful_close=False
                )
                _release_dead_process_close_locks(stopped_pid)
        except Exception as e:
            sys.stderr.write(f"[Emergency] terminate {bot_name} failed: {e}\n")
            stop_failures.append(bot_name)
            _log("error", f"{bot_name}: stop failed; emergency fallback aborted: {e}")

    if stop_failures:
        update("Emergency close aborted - bot is still running")
        return

    # Step 2: direct close. The bot is now dead, the launcher owns
    # the exchange state.
    update("Closing all open positions")

    for bot_name in futures_bots:
        if bot_name in mode_errors:
            _log("warn",
                 f"{bot_name}: mode detection failed before emergency close; "
                 f"trying LIVE and SIM buckets ({mode_errors[bot_name]})")
        failed: list[str] = []
        for sim_only in close_modes_by_bot.get(bot_name, [False, True]):
            try:
                result = direct_close_remaining_futures(
                    _log, sim_only, reason="Emergency Close", bot_name=bot_name)
                failed.extend(str(sym) for sym in result.get("failed", []) if sym)
            except Exception as exc:
                failed.append(f"{bot_name}:{type(exc).__name__}")
                _log("error", f"{bot_name}: emergency close fallback failed: {exc}")
        if failed:
            _log("error",
                 f"{bot_name}: emergency close incomplete for "
                 + ", ".join(sorted(set(failed))))
            update("Emergency close incomplete - manual review needed")
            return
    update("Done.")
