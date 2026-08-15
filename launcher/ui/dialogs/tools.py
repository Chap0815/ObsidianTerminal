"""
Sidebar tool dialogs: run a backtest, run the optimizer, show the
win/loss heatmap.

Backtest + optimizer share the same generic "run a tool subprocess,
stream its stdout into the dialog" engine implemented in
:func:`run_tool_dialog`. The heatmap is a one-shot DB read with a
custom-painted grid.
"""

from __future__ import annotations

import math
import os
import re
import subprocess
import threading
from collections import deque as _deque

import customtkinter as ctk

from bot_utils.config import _read_config_json
from launcher.config.settings import (
    BOT_META,
    BOT_ORDER,
    COLORS,
    FONT_BODY,
    PROJECT_ROOT,
    ConfigMergeConflict,
    _get_python_exe,
    capture_config_merge_expectations,
    load_config,
    save_config_merge,
    subprocess_no_window_kwargs,
)
from launcher.tool_processes import (
    start_registered_tool_process,
    stop_tool_processes,
    tool_root_xoption,
)
from launcher.ui.components.widgets import safe_geometry
from launcher.ui.logging_panel import classify_severity
from launcher.ui.theme import force_dark_titlebar

# Realistic 8h funding magnitude for leveraged-perp backtests so leveraged EV
# isn't overstated by modelling zero carry. ~0.01%/8h  0.03%/day. Overridable
# via the FUTURES config key FUNDING_RATE_8H.
DEFAULT_FUTURES_FUNDING_8H = 0.0001

_BACKTEST_CONFIG_FLAGS = {
    "MIN_PUMP": ("--pump", None),
    "ACTIVATION_PROFIT": ("--activation", None),
    "TRAILING_DISTANCE": ("--trailing", None),
    "INITIAL_STOP_LOSS": ("--stop", abs),
    "PARTIAL_SELL_PCT": ("--partial", None),
    "RSI_MAX": ("--rsimax", None),
    "LEVERAGE": ("--leverage", None),
}


OPTIMIZER_CONFIG_MAPPING = {
    "min_pump":          "MIN_PUMP",
    "activation_profit": "ACTIVATION_PROFIT",
    "trailing_distance": "TRAILING_DISTANCE",
    "stop_loss":         "INITIAL_STOP_LOSS",
    "partial_pct":       "PARTIAL_SELL_PCT",
    "rsi_max":           "RSI_MAX",
}
OPTIMIZER_CONFIG_BOUNDS = {
    "min_pump": (0.5, 20.0),
    "activation_profit": (1.0, 20.0),
    "trailing_distance": (0.25, 10.0),
    "stop_loss": (-15.0, -0.5),
    "partial_pct": (0.05, 1.0),
    "rsi_max": (40.0, 90.0),
}

OPTIMIZER_MIN_HOLDOUT_TRADES = 30
TOOL_OUTPUT_MAX_PENDING_LINES = 1_000
TOOL_OUTPUT_MAX_RAW_LINE_BYTES = 16 * 1024
TOOL_OUTPUT_DRAIN_BATCH_LINES = 100
OPTIMIZER_BEST_CONFIG_START = "<<<BEST_CONFIG>>>"
OPTIMIZER_BEST_CONFIG_END = "<<<END_BEST_CONFIG>>>"
PROMOTION_APPLY_PROVENANCE_FIELDS = {
    "artifact_schema",
    "artifact_kind",
    "promotion_apply_artifact_sha256",
    "optimizer_source_sha256",
    "promotion_bundle_sha256",
}


class _BoundedToolOutputBuffer:
    """Thread-safe drop-oldest queue with one aggregated UI warning."""

    def __init__(self, *, max_lines: int = TOOL_OUTPUT_MAX_PENDING_LINES):
        if not isinstance(max_lines, int) or isinstance(max_lines, bool) or max_lines <= 0:
            raise ValueError("max_lines must be a positive integer")
        self._max_lines = max_lines
        self._lines = _deque()
        self._dropped_since_drain = 0
        self._lock = threading.Lock()

    def append(self, line) -> None:
        with self._lock:
            if len(self._lines) >= self._max_lines:
                self._lines.popleft()
                self._dropped_since_drain += 1
            self._lines.append(line)

    def drain(self, limit: int) -> list:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            return []
        batch = []
        with self._lock:
            remaining = limit
            if self._dropped_since_drain:
                batch.append(
                    f"[WARN] {self._dropped_since_drain} older "
                    f"tool-output lines dropped (buffer limit "
                    f"{self._max_lines})"
                )
                self._dropped_since_drain = 0
                remaining -= 1
            while self._lines and remaining > 0:
                batch.append(self._lines.popleft())
                remaining -= 1
        return batch

    def __bool__(self) -> bool:
        with self._lock:
            return bool(self._lines or self._dropped_since_drain)

    def __len__(self) -> int:
        with self._lock:
            return len(self._lines)


class _DelimitedByteFramer:
    """Frame bounded binary pipe output on CR/LF without partial decoding."""

    def __init__(self, *, max_bytes: int = TOOL_OUTPUT_MAX_RAW_LINE_BYTES):
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        self._max_bytes = max_bytes
        self._pending = bytearray()
        self._discard_until_delimiter = False
        self._skip_lf_after_cr = False

    @property
    def pending_bytes(self) -> int:
        return len(self._pending)

    def feed(self, chunk) -> list[bytes]:
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError("tool output chunk must be bytes-like")
        framed = []
        for byte in bytes(chunk):
            if self._skip_lf_after_cr:
                self._skip_lf_after_cr = False
                if byte == 0x0A:
                    continue
            if self._discard_until_delimiter:
                if byte in (0x0A, 0x0D):
                    self._discard_until_delimiter = False
                    self._skip_lf_after_cr = byte == 0x0D
                continue
            if byte in (0x0A, 0x0D):
                if self._pending:
                    framed.append(bytes(self._pending))
                    self._pending.clear()
                self._skip_lf_after_cr = byte == 0x0D
                continue
            if len(self._pending) >= self._max_bytes:
                self._pending.clear()
                self._discard_until_delimiter = True
                framed.append(
                    f"[WARN] overlong tool-output line discarded "
                    f"(limit {self._max_bytes} bytes)".encode("ascii")
                )
                continue
            self._pending.append(byte)
        return framed

    def finish(self) -> list[bytes]:
        if self._discard_until_delimiter:
            self._pending.clear()
            self._discard_until_delimiter = False
            self._skip_lf_after_cr = False
            return []
        framed = [bytes(self._pending)] if self._pending else []
        self._pending.clear()
        self._skip_lf_after_cr = False
        return framed


def _drain_tool_output_batch(
    buffer,
    append_line,
    *,
    done: bool,
    limit: int = TOOL_OUTPUT_DRAIN_BATCH_LINES,
) -> bool:
    """Render one bounded batch and report whether another UI tick is needed."""
    drain = getattr(buffer, "drain", None)
    if callable(drain):
        for line in drain(limit):
            append_line(line)
        return not done or bool(buffer)
    count = 0
    while buffer and count < limit:
        append_line(buffer.popleft())
        count += 1
    return not done or bool(buffer)


def _finite_optimizer_number(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _read_tool_bot_config(bot: str, *, path=None) -> dict:
    """Read one bot section through the shared bounded config reader."""
    if path is None:
        from core.paths import BOT_CONFIG

        path = BOT_CONFIG
    config = _read_config_json(str(path))
    if not isinstance(config, dict):
        return {}
    section = config.get(bot)
    return section if isinstance(section, dict) else {}


def _backtest_config_cli_args(bot_cfg: dict) -> list[str]:
    """Build backtester overrides from finite numeric config values only."""
    if not isinstance(bot_cfg, dict):
        return []
    args: list[str] = []
    for cfg_key, (flag, transform) in _BACKTEST_CONFIG_FLAGS.items():
        value = _finite_optimizer_number(bot_cfg.get(cfg_key))
        if value is None:
            continue
        if transform:
            value = transform(value)
        args.extend((flag, str(round(value, 4))))
    return args


def optimizer_promotion_reasons(cfg: dict) -> list[str]:
    """Return fail-closed reasons why an optimizer payload is not promotable."""
    if not isinstance(cfg, dict):
        return ["payload is not an object"]
    reasons = []
    for key, expected in (
        ("research_only", False),
        ("promotion_eligible", True),
        ("changes_runtime", False),
    ):
        if cfg.get(key) is not expected:
            reasons.append(f"{key} is not explicitly {str(expected).lower()}")
    run_id = cfg.get("reproducible_run_id")
    dataset_fingerprint = cfg.get("dataset_fingerprint")
    candidate_fingerprint = cfg.get("optimizer_candidate_fingerprint")
    if not isinstance(run_id, str) or re.fullmatch(r"[0-9a-f]{64}", run_id) is None:
        reasons.append("reproducible optimizer run id is invalid")
    if (
        not isinstance(dataset_fingerprint, str)
        or re.fullmatch(r"[0-9a-f]{64}", dataset_fingerprint) is None
    ):
        reasons.append("optimizer dataset fingerprint is invalid")
    if (
        not isinstance(candidate_fingerprint, str)
        or re.fullmatch(r"[0-9a-f]{64}", candidate_fingerprint) is None
    ):
        reasons.append("optimizer candidate fingerprint is invalid")
    candidate_params = cfg.get("optimizer_candidate_params")
    if not isinstance(candidate_params, dict):
        reasons.append("optimizer candidate parameters are invalid")
    else:
        try:
            from tools.simulation_workspace import canonical_evidence_sha256

            actual_candidate_fingerprint = canonical_evidence_sha256(candidate_params)
        except (TypeError, ValueError, OverflowError):
            reasons.append("optimizer candidate parameters are invalid")
        else:
            if actual_candidate_fingerprint != candidate_fingerprint:
                reasons.append("optimizer candidate fingerprint does not match")
        for key in OPTIMIZER_CONFIG_MAPPING:
            if key not in cfg:
                continue
            emitted = _finite_optimizer_number(cfg.get(key))
            bound = _finite_optimizer_number(candidate_params.get(key))
            if (
                emitted is None
                or bound is None
                or not math.isclose(emitted, bound, rel_tol=1e-12, abs_tol=1e-12)
            ):
                reasons.append(f"{key} does not match optimizer candidate")
    envelope = cfg.get("promotion_envelope")
    if not isinstance(envelope, dict):
        reasons.append("shared promotion envelope is missing")
    else:
        evidence = envelope.get("evidence")
        if not isinstance(evidence, dict):
            reasons.append("shared promotion evidence is invalid")
        elif (
            evidence.get("optimizer_strategy") != cfg.get("strategy")
            or evidence.get("optimizer_candidate_fingerprint")
            != candidate_fingerprint
            or evidence.get("optimizer_run_id") != run_id
            or evidence.get("dataset_fingerprint") != dataset_fingerprint
        ):
            reasons.append("shared promotion provenance does not match optimizer run")
        else:
            minimum_samples = envelope.get("minimum_samples")
            manual_approval = envelope.get("manual_live_approval")
            if (
                not isinstance(minimum_samples, int)
                or isinstance(minimum_samples, bool)
                or minimum_samples < 1
                or not isinstance(manual_approval, bool)
            ):
                reasons.append("shared promotion controls are invalid")
            else:
                try:
                    from trading.profit_research_runner import check_promotion
                    from trading.promotion_assembly import verify_promotion_envelope

                    verified_envelope = verify_promotion_envelope(envelope)
                    verified = check_promotion(
                        verified_envelope["evidence"],
                        minimum_samples=minimum_samples,
                        manual_live_approval=manual_approval,
                    )
                except (TypeError, ValueError, OverflowError):
                    reasons.append("shared promotion envelope is invalid")
                else:
                    if verified.get("research_passed") is not True:
                        reasons.append("shared research promotion gate did not pass")
                    if verified.get("live_allowed") is not True:
                        reasons.append("shared live promotion gate did not pass")
                    if verified.get("deployment_performed") is not False:
                        reasons.append("shared promotion boundary is invalid")
                    if (
                        envelope.get("decision_input_schema")
                        != verified.get("decision_input_schema")
                        or envelope.get("decision_input_sha256")
                        != verified.get("decision_input_sha256")
                    ):
                        reasons.append("shared promotion fingerprint does not match")
    for key in (
        "robust",
        "deep_validation_complete",
        "deployment_validated",
        "deployment_trustworthy",
        "final_holdout_pass",
        "holdout_net_consistent",
        "holdout_sample_consistent",
        "holdout_outcomes_consistent",
        "cost_stress_pass",
    ):
        if cfg.get(key) is not True:
            reasons.append(f"{key} is not explicitly true")
    holdout_net = _finite_optimizer_number(cfg.get("holdout_net"))
    if holdout_net is None or holdout_net <= 0.0:
        reasons.append("holdout_net must be finite and positive")

    holdout_trades = _finite_optimizer_number(cfg.get("holdout_trades"))
    if holdout_trades is None or not holdout_trades.is_integer():
        reasons.append("holdout trade count must be a finite integer")
    elif holdout_trades < 0.0:
        reasons.append("holdout trade count must be nonnegative")

    holdout_full_trades = _finite_optimizer_number(cfg.get("holdout_full_trades"))
    if holdout_full_trades is None or not holdout_full_trades.is_integer():
        reasons.append("holdout full-trade count must be a finite integer")
    elif holdout_full_trades < OPTIMIZER_MIN_HOLDOUT_TRADES:
        reasons.append("holdout full-trade count is below minimum")
    elif holdout_trades is not None and holdout_full_trades > holdout_trades:
        reasons.append("holdout full-trade count exceeds trade rows")

    dsr = _finite_optimizer_number(cfg.get("dsr"))
    if dsr is None or not 0.95 <= dsr <= 1.0:
        reasons.append("DSR must be finite and between 0.95 and 1.0")

    pbo = _finite_optimizer_number(cfg.get("pbo"))
    if pbo is None or not 0.0 <= pbo <= 0.25:
        reasons.append("PBO must be finite and between 0.0 and 0.25")

    for key, (minimum, maximum) in OPTIMIZER_CONFIG_BOUNDS.items():
        if key not in cfg:
            continue
        value = _finite_optimizer_number(cfg.get(key))
        if value is None or not minimum <= value <= maximum:
            reasons.append(
                f"{key} must be finite and between {minimum:g} and {maximum:g}"
            )
    return reasons


def optimizer_config_from_complete_marker(
    line: str, *, expected_strategy: str | None = None
) -> dict | None:
    """Keep the legacy stdout marker informational; file evidence is mandatory."""
    if not isinstance(line, str) or OPTIMIZER_BEST_CONFIG_START not in line:
        return None
    payload_with_tail = line.split(OPTIMIZER_BEST_CONFIG_START, 1)[1]
    if OPTIMIZER_BEST_CONFIG_END not in payload_with_tail:
        return None
    _ = expected_strategy
    return None


def optimizer_config_from_promotion_artifact(
    source, *, expected_strategy: str | None = None
) -> dict:
    """Load one finalized artifact and revalidate it for manual Launcher apply."""
    from tools.promotion_bundle import load_promotion_apply_artifact

    result = load_promotion_apply_artifact(source)
    payload = result.get("apply_payload") if isinstance(result, dict) else None
    if not isinstance(payload, dict):
        raise TypeError("promotion apply artifact payload is invalid")
    if expected_strategy is not None and payload.get("strategy") != expected_strategy:
        raise ValueError("promotion apply artifact strategy mismatch")
    reasons = optimizer_promotion_reasons(payload)
    if reasons:
        raise ValueError(
            "promotion apply artifact is not promotable: " + "; ".join(reasons)
        )
    optimizer_apply_provenance(payload)
    return payload


def optimizer_apply_provenance(cfg: dict) -> dict:
    """Return exact apply-artifact identities or fail closed."""
    provenance = cfg.get("promotion_apply_provenance") if isinstance(cfg, dict) else None
    if (
        type(provenance) is not dict
        or set(provenance) != PROMOTION_APPLY_PROVENANCE_FIELDS
        or type(provenance.get("artifact_schema")) is not int
        or provenance.get("artifact_schema") != 1
        or provenance.get("artifact_kind") != "optimizer_promotion_apply"
    ):
        raise ValueError("optimizer promotion apply provenance is invalid")
    for key in (
        "promotion_apply_artifact_sha256",
        "optimizer_source_sha256",
        "promotion_bundle_sha256",
    ):
        value = provenance.get(key)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("optimizer promotion apply provenance is invalid")
    return dict(provenance)


def optimizer_best_config_updates(cfg: dict) -> tuple[dict, list[str]]:
    """Return only config keys explicitly emitted by the optimizer."""
    if not isinstance(cfg, dict):
        return {}, []
    updates: dict = {}
    applied: list[str] = []
    for opt_key, cfg_key in OPTIMIZER_CONFIG_MAPPING.items():
        if opt_key not in cfg:
            continue
        val = _finite_optimizer_number(cfg[opt_key])
        if val is None:
            continue
        updates[cfg_key] = val
        applied.append(f"{cfg_key}={val}")
    return updates, applied


def apply_optimizer_best_config_to_app(
    app,
    strategy: str,
    cfg: dict,
    *,
    expected_section_values: dict | None = None,
) -> list[str]:
    """Apply optimizer-emitted keys to UI memory and persist only those keys."""
    if not isinstance(cfg, dict) or cfg.get("strategy") != strategy:
        raise ValueError("optimizer strategy mismatch")
    updates, applied = optimizer_best_config_updates(cfg)
    reasons = optimizer_promotion_reasons(cfg)
    if reasons:
        raise ValueError("optimizer result is not promotable: " + "; ".join(reasons))
    provenance = optimizer_apply_provenance(cfg)
    if not updates:
        return applied
    if type(expected_section_values) is not dict:
        raise ValueError("optimizer config staging expectation is missing")
    from tools.simulation_workspace import canonical_evidence_sha256

    audit_context = {
        **provenance,
        "strategy": strategy,
        "config_update_sha256": canonical_evidence_sha256({
            "strategy": strategy,
            "updates": updates,
        }),
        "updated_keys": sorted(updates),
    }
    persisted_config = save_config_merge(
        {strategy: dict(updates)},
        expected_section_values=expected_section_values,
        audit_source="optimizer_promotion_apply",
        audit_context=audit_context,
    )
    app.config = persisted_config
    for cfg_key, val in updates.items():
        try:
            row = app.param_rows.get(strategy, {}).get(cfg_key)
            if row:
                row.set_value(val)
        except Exception:
            pass
    return applied


def reset_optimizer_apply_run_state(parse_state: dict, apply_btn_ref: dict) -> bool:
    """Invalidate any prior Apply action before a new tool run starts."""
    button = apply_btn_ref.get("btn")
    if button is not None:
        inactive = False
        try:
            inactive = not bool(button.winfo_exists())
        except Exception:
            pass
        if not inactive:
            try:
                button.configure(state="disabled", command=lambda: None)
                inactive = True
            except Exception:
                pass
        try:
            button.destroy()
            inactive = True
        except Exception:
            pass
        if not inactive:
            return False
    try:
        generation = int(parse_state.get("run_generation", 0)) + 1
    except (TypeError, ValueError, OverflowError):
        generation = 1
    apply_btn_ref["btn"] = None
    parse_state.clear()
    parse_state.update({
        "in_marker": False,
        "marker_buf": "",
        "best_config": None,
        "trophy_lines_seen": 0,
        "run_generation": generation,
        "output_complete": False,
        "expected_strategy": None,
        "apply_expectations": None,
    })
    return True


def finalize_optimizer_apply_run(
    parse_state: dict,
    apply_btn_ref: dict,
    exit_code,
    publish,
) -> bool:
    """Publish a staged optimizer result only after a proven clean exit."""
    clean_exit = type(exit_code) is int and exit_code == 0
    cfg = parse_state.get("best_config")
    expected_strategy = parse_state.get("expected_strategy")
    if (
        not clean_exit
        or parse_state.get("output_complete") is not True
        or not isinstance(parse_state.get("apply_expectations"), dict)
        or not isinstance(cfg, dict)
        or (
            expected_strategy is not None
            and cfg.get("strategy") != expected_strategy
        )
        or optimizer_promotion_reasons(cfg)
    ):
        if clean_exit:
            parse_state["best_config"] = None
        else:
            reset_optimizer_apply_run_state(parse_state, apply_btn_ref)
        return False
    generation = parse_state.get("run_generation")
    try:
        publish(generation)
    except Exception:
        reset_optimizer_apply_run_state(parse_state, apply_btn_ref)
        return False
    return True


def _futures_funding_8h(bot_cfg: dict) -> float:
    """8h funding rate to model for a FUTURES run: live config override else default."""
    try:
        value = _finite_optimizer_number(bot_cfg.get("FUNDING_RATE_8H"))
        if value is not None:
            return value
    except (AttributeError, TypeError):
        pass
    return DEFAULT_FUTURES_FUNDING_8H


#  Public entry points 

def open_backtest_dialog(app) -> None:
    run_tool_dialog(
        app,
        title="Run Backtest",
        tool_name="backtest",
        description=(
            "Run a historical backtest using your current parameters.\n"
            "Useful after editing the AI prompt to see how the new\n"
            "decision logic would have performed on past data."
        ),
    )


def open_optimizer_dialog(app) -> None:
    run_tool_dialog(
        app,
        title="Optimize Parameters",
        tool_name="optimizer",
        description=(
            "Run the parameter optimizer with K-Fold cross-validation.\n"
            "Tests many combinations and finds the most robust set.\n"
            "Quick mode: ~5-15min. Full mode: 30-90min."
        ),
    )


def open_selftest_dialog(app) -> None:
    run_tool_dialog(
        app,
        title="Run Self-Test",
        tool_name="selftest",
        description=(
            "Runs the full invariant suite when tests are installed.\n"
            "Packaged releases without DEV tests run an integrity and\n"
            "source-compile smoke check instead.\n"
            "Run it after editing code, before going live."
        ),
    )


def open_heatmap_dialog(app) -> None:
    """Win/loss heatmap by hour-of-day  day-of-week."""
    try:
        from core.database import get_winloss_heatmap  # type: ignore
    except Exception as e:
        from tkinter import messagebox
        messagebox.showerror("Heatmap", f"Database module error: {e}")
        return

    dlg = ctk.CTkToplevel(app)
    dlg.title("Win/Loss Heatmap")
    dlg.configure(fg_color=COLORS["panel"])
    dlg.grab_set()
    dlg.transient(app)
    force_dark_titlebar(dlg)
    safe_geometry(dlg, 960, 620, parent=app)

    # Header
    ctk.CTkLabel(
        dlg, text="  Win Rate by Hour  Day-of-Week",
        font=ctk.CTkFont(FONT_BODY, 16, "bold"),
        text_color=COLORS["text"]
    ).pack(pady=(20, 4), padx=24, anchor="w")

    ctk.CTkLabel(
        dlg,
        text="Last 30 days  Each cell shows win-rate% (sample size).\n"
             "Greener = higher win rate  Red = avoid these times.\n"
             "Empty cells = no trades in that slot.",
        font=ctk.CTkFont(FONT_BODY, 10),
        text_color=COLORS["text_dim"], justify="left"
    ).pack(padx=24, anchor="w", pady=(0, 12))

    # Bot selector
    sel_frame = ctk.CTkFrame(dlg, fg_color="transparent")
    sel_frame.pack(fill="x", padx=24, pady=(0, 8))

    ctk.CTkLabel(sel_frame, text="Bot:",
                  font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                  text_color=COLORS["text_dim"]
                  ).pack(side="left", padx=(0, 8))

    bot_var = ctk.StringVar(value="ALL")
    for label in ("ALL", "TREND", "SPOT", "FUTURES"):
        color = COLORS.get(label.lower(), COLORS["text"]) if label != "ALL" else COLORS["text"]
        # Per-bot color when a specific bot is selected, else the neutral
        # "balanced" emerald as the default checkbox accent. (COLORS has no
        # "accent" key  use .get with a fallback.)
        _radio_accent = COLORS.get(label.lower(), COLORS["balanced"])
        ctk.CTkRadioButton(
            sel_frame, text=label, variable=bot_var, value=label,
            font=ctk.CTkFont(FONT_BODY, 10),
            text_color=color,
            fg_color=_radio_accent, hover_color=COLORS["panel_hover"]
        ).pack(side="left", padx=4)

    # Grid container
    grid_outer = ctk.CTkFrame(dlg, fg_color=COLORS["bg"], corner_radius=8,
                                 border_width=1, border_color=COLORS["border"])
    grid_outer.pack(fill="both", expand=True, padx=24, pady=8)

    # Stats label
    stats_var = ctk.StringVar(value="")
    ctk.CTkLabel(
        dlg, textvariable=stats_var,
        font=ctk.CTkFont(app.mono_font, 10),
        text_color=COLORS["text_subtle"]
    ).pack(pady=(0, 8), padx=24, anchor="w")

    def _color_for(wr: float, n: int) -> str:
        """Heat colour based on win-rate. Less saturated when sample size small."""
        if n == 0:
            return COLORS["bg"]
        if wr >= 0.55:
            t = min(1.0, (wr - 0.55) / 0.30)
            if t > 0.6:
                return "#0d9b6c"
            if t > 0.3:
                return "#0d7050"
            return "#143d2e"
        elif wr <= 0.40:
            t = min(1.0, (0.40 - wr) / 0.30)
            if t > 0.6:
                return "#dc2626"
            if t > 0.3:
                return "#7c1d1d"
            return "#3a1414"
        else:
            return "#1f2937"

    DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    def _redraw():
        for w in grid_outer.winfo_children():
            w.destroy()

        bot_filter = None if bot_var.get() == "ALL" else bot_var.get()
        try:
            data = get_winloss_heatmap(bot_name=bot_filter, days=30)
        except Exception as e:
            ctk.CTkLabel(grid_outer, text=f"Error loading data: {e}",
                          text_color=COLORS["danger"]).pack(pady=40)
            return

        hd = data.get("by_hour_dow", {})
        if not hd:
            ctk.CTkLabel(
                grid_outer,
                text="  No trade data yet.\n"
                     "Run some bots and check back after a few trades.",
                font=ctk.CTkFont(FONT_BODY, 13, "bold"),
                text_color=COLORS["text_dim"], justify="center"
            ).pack(pady=80)
            stats_var.set("")
            return

        inner = ctk.CTkFrame(grid_outer, fg_color="transparent")
        inner.pack(padx=14, pady=14)

        # Header row: hour labels
        ctk.CTkLabel(inner, text="", width=42).grid(row=0, column=0)
        for h in range(24):
            ctk.CTkLabel(
                inner, text=f"{h:02d}",
                font=ctk.CTkFont(app.mono_font, 9, "bold"),
                text_color=COLORS["text_dim"], width=32
            ).grid(row=0, column=h+1, padx=1, pady=1)

        # Data rows
        total_n = 0
        total_w = 0.0
        for dow in range(7):
            ctk.CTkLabel(
                inner, text=DOW_NAMES[dow],
                font=ctk.CTkFont(app.mono_font, 9, "bold"),
                text_color=COLORS["text_dim"], width=42, anchor="w"
            ).grid(row=dow+1, column=0, padx=(0, 4))

            for h in range(24):
                cell = hd.get((dow, h))
                if cell:
                    wr = cell["wr"]
                    n  = cell["n"]
                    bg = _color_for(wr, n)
                    text = f"{int(wr*100)}\n{n}"
                    total_n += n
                    total_w += wr * n
                else:
                    bg = COLORS["bg"]
                    text = ""

                f = ctk.CTkFrame(
                    inner, fg_color=bg, width=32, height=28,
                    corner_radius=3
                )
                f.grid(row=dow+1, column=h+1, padx=1, pady=1)
                f.grid_propagate(False)
                if text:
                    ctk.CTkLabel(
                        f, text=text,
                        font=ctk.CTkFont(app.mono_font, 8, "bold"),
                        text_color="#ffffff"
                    ).place(relx=0.5, rely=0.5, anchor="center")

        # Legend
        legend = ctk.CTkFrame(grid_outer, fg_color="transparent")
        legend.pack(pady=(4, 14))
        ctk.CTkLabel(legend, text="Win Rate: ",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_dim"]).pack(side="left")
        for label, color in [("<40%", "#dc2626"), ("40-55%", "#1f2937"),
                              ("55-70%", "#0d7050"), (">70%", "#0d9b6c")]:
            box = ctk.CTkFrame(legend, fg_color=color, width=14, height=14)
            box.pack(side="left", padx=(6, 2))
            ctk.CTkLabel(legend, text=label,
                          font=ctk.CTkFont(FONT_BODY, 9),
                          text_color=COLORS["text_dim"]).pack(side="left")

        # Best/worst slots
        sorted_slots = sorted(
            hd.items(),
            key=lambda kv: (kv[1]["wr"] if kv[1]["n"] >= 3 else -1, kv[1]["n"]),
            reverse=True
        )
        best = [s for s in sorted_slots if s[1]["n"] >= 3][:3]
        worst = sorted(
            [s for s in sorted_slots if s[1]["n"] >= 3],
            key=lambda kv: kv[1]["wr"]
        )[:3]

        avg_wr = (total_w / total_n * 100) if total_n else 0.0
        stats_text = f"Overall: {total_n} trades  {avg_wr:.1f}% avg win rate"
        if best:
            top = best[0]
            d, h = top[0]
            stats_text += (f"  Best slot: {DOW_NAMES[d]} {h:02d}:00 "
                            f"({top[1]['wr']*100:.0f}% in {top[1]['n']} trades)")
        if worst:
            bad = worst[0]
            d, h = bad[0]
            stats_text += (f"  Worst slot: {DOW_NAMES[d]} {h:02d}:00 "
                            f"({bad[1]['wr']*100:.0f}% in {bad[1]['n']} trades)")
        stats_var.set(stats_text)

    bot_var.trace_add("write", lambda *_: _redraw())
    _redraw()

    # Close button
    btns = ctk.CTkFrame(dlg, fg_color="transparent")
    btns.pack(side="bottom", fill="x", padx=20, pady=12)
    ctk.CTkButton(btns, text="Close",
                   height=34, corner_radius=8, width=110,
                   font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                   fg_color="transparent", hover_color=COLORS["panel_hover"],
                   text_color=COLORS["text_dim"],
                   border_width=1, border_color=COLORS["border"],
                   command=dlg.destroy
                   ).pack(side="right")


#  Generic tool runner (backtest + optimizer) 

def run_tool_dialog(app, title: str, tool_name: str, description: str) -> None:
    """Two-phase tool runner:

    1. Config phase: user picks strategy, days, mode
    2. Run phase: subprocess starts and its stdout/stderr is streamed
       live into the dialog (a dedicated reader thread pulls the pipe).
    """
    dlg = ctk.CTkToplevel(app)
    dlg.title(title)
    dlg.configure(fg_color=COLORS["panel"])
    dlg.grab_set()
    dlg.transient(app)
    force_dark_titlebar(dlg)
    # minsize clamped so a short screen can't force it taller than usable area.
    safe_geometry(dlg, 620, 580, parent=app)
    try:
        sh = dlg.winfo_screenheight()
        dlg.minsize(min(600, dlg.winfo_screenwidth() - 80),
                    min(560, sh - 80))
    except Exception:
        pass

    # Subprocess state
    proc_state: dict = {
        "proc": None, "reader_thread": None,
        "buffer": _BoundedToolOutputBuffer(),
        "done": False, "exit_code": None, "stop_reading": False,
    }

    #  Header 
    ctk.CTkLabel(dlg, text=title,
                  font=ctk.CTkFont(FONT_BODY, 16, "bold"),
                  text_color=COLORS["text"]
                  ).pack(pady=(20, 4), padx=24, anchor="w")
    ctk.CTkLabel(dlg, text=description,
                  font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                  text_color=COLORS["text_muted"], justify="left", wraplength=460
                  ).pack(pady=(0, 16), padx=24, anchor="w")

    #  PHASE 1: config frame 
    config_frame = ctk.CTkFrame(dlg, fg_color="transparent")
    config_frame.pack(fill="both", expand=True, padx=24)

    # Always defined (used by _start_run); the pickers are only shown for
    # tools that actually need a strategy/timeframe. The self-test runs the
    # whole pytest suite and needs neither.
    bot_var = ctk.StringVar(value="TREND")
    days_var = ctk.StringVar(value="30")

    if tool_name != "selftest":
        # Bot selector
        ctk.CTkLabel(config_frame, text="Strategy",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(pady=(0, 4), anchor="w")

        bot_row = ctk.CTkFrame(config_frame, fg_color="transparent")
        bot_row.pack(fill="x")
        for bot in BOT_ORDER:
            meta = BOT_META[bot]
            ctk.CTkRadioButton(
                bot_row, text=bot, variable=bot_var, value=bot,
                font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                fg_color=meta["accent"], hover_color=meta["accent_dim"],
                text_color=COLORS["text_dim"]
            ).pack(side="left", padx=(0, 12))

        # Days
        ctk.CTkLabel(config_frame, text="Historical days",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(pady=(16, 4), anchor="w")

        days_row = ctk.CTkFrame(config_frame, fg_color="transparent")
        days_row.pack(fill="x")

        # Window presets depend on the selected strategy's underlying tool:
        # FUTURES/SPOT run the hourly backtester (short windows are meaningful);
        # TREND/FUTREND/CROSS run daily validators (TREND's SMA200 needs ~200
        # daily bars), so 7-90d would just return "No data". Rebuild the buttons
        # whenever the strategy changes and snap days_var to a valid value.
        _DAILY_TOOLS = {"TREND", "FUTREND", "CROSS"}
        _DAY_PRESETS = {"hourly": (["7", "14", "30", "60", "90"], "30"),
                        "daily":  (["365", "730", "1095"], "365")}

        def _rebuild_days(*_):
            for _w in days_row.winfo_children():
                _w.destroy()
            choices, default = _DAY_PRESETS[
                "daily" if bot_var.get() in _DAILY_TOOLS else "hourly"]
            if days_var.get() not in choices:
                days_var.set(default)
            for d in choices:
                ctk.CTkRadioButton(
                    days_row, text=f"{d}d", variable=days_var, value=d,
                    font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                    fg_color=COLORS["purple"], hover_color=COLORS["purple_dim"],
                    text_color=COLORS["text_dim"]
                ).pack(side="left", padx=(0, 12))

        _rebuild_days()
        bot_var.trace_add("write", _rebuild_days)

    # Optimizer: quick mode
    quick_var = ctk.BooleanVar(value=True)
    if tool_name == "optimizer":
        ctk.CTkLabel(config_frame, text="Mode",
                      font=ctk.CTkFont(FONT_BODY, 10, "bold"),
                      text_color=COLORS["text_muted"]
                      ).pack(pady=(16, 4), anchor="w")
        ctk.CTkSwitch(
            config_frame, text="Quick mode (faster, smaller search space)",
            variable=quick_var,
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            text_color=COLORS["text_dim"],
            progress_color=COLORS["aggressive"]
        ).pack(pady=(0, 8), anchor="w")

    #  PHASE 2: output frame (initially hidden) 
    output_frame = ctk.CTkFrame(dlg, fg_color="transparent")
    # not packed yet

    output_header = ctk.CTkFrame(output_frame, fg_color="transparent")
    output_header.pack(fill="x", padx=24, pady=(0, 8))

    run_label_var = ctk.StringVar(value="")
    ctk.CTkLabel(output_header, textvariable=run_label_var,
                  font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                  text_color=COLORS["text_dim"], anchor="w"
                  ).pack(side="left")

    # Live activity spinner
    spinner_var = ctk.StringVar(value="")
    spinner_lbl = ctk.CTkLabel(output_header, textvariable=spinner_var,
                                  font=ctk.CTkFont(FONT_BODY, 14, "bold"),
                                  text_color=COLORS["balanced"])
    spinner_lbl.pack(side="right", padx=(0, 4))

    # Scrollable text output
    log_outer = ctk.CTkFrame(output_frame, fg_color=COLORS["bg"],
                                corner_radius=8,
                                border_width=1, border_color=COLORS["border"])
    log_outer.pack(fill="both", expand=True, padx=24, pady=(0, 8))

    log_text = ctk.CTkTextbox(
        log_outer, fg_color=COLORS["bg"],
        text_color=COLORS["text"],
        font=ctk.CTkFont(app.mono_font, 10),
        wrap="none", corner_radius=8,
        border_width=0,
    )
    log_text.pack(fill="both", expand=True, padx=2, pady=2)
    log_text.configure(state="disabled")

    #  Optimizer progress bar (hidden until first PROGRESS marker) 
    accent = COLORS["balanced"] if tool_name == "backtest" else COLORS["aggressive"]

    progress_frame = ctk.CTkFrame(dlg, fg_color="transparent", height=44)
    progress_frame.pack(fill="x", padx=24, pady=(2, 0))
    progress_frame.pack_propagate(False)

    progress_label = ctk.CTkLabel(
        progress_frame, text="",
        font=ctk.CTkFont(FONT_BODY, 10, "bold"),
        text_color=COLORS["text_dim"],
        anchor="w"
    )
    progress_label.pack(fill="x", pady=(2, 2))

    progress_bar = ctk.CTkProgressBar(
        progress_frame,
        height=10, corner_radius=4,
        fg_color=COLORS["bg"],
        progress_color=accent,
        border_width=0
    )
    progress_bar.set(0)
    # Don't pack initially  appears only when a progress marker arrives
    progress_state = {"shown": False}

    def _update_progress(payload: dict) -> None:
        """Called when a <<<PROGRESS>>>...<<<END>>> marker is parsed."""
        try:
            pct      = float(payload.get("pct", 0))
            done     = int(payload.get("done", 0))
            total    = int(payload.get("total", 1))
            eta_s    = int(payload.get("eta_s", 0))
            best     = payload.get("best")
            elapsed  = int(payload.get("elapsed_s", 0))
            label    = payload.get("label", "Sim")
        except (TypeError, ValueError):
            return

        if not progress_state["shown"]:
            progress_bar.pack(fill="x", pady=(2, 4))
            progress_state["shown"] = True

        progress_bar.set(max(0.0, min(1.0, pct)))

        if eta_s >= 60:
            eta_str = f"{eta_s // 60}m {eta_s % 60:02d}s"
        else:
            eta_str = f"{eta_s}s"

        best_str = ""
        if best is not None:
            try:
                bv = float(best)
                best_str = f"  Best: {bv:+.2f}"
            except (TypeError, ValueError):
                pass

        progress_label.configure(
            text=f"{label}: {done}/{total} ({pct*100:.0f}%)  "
                 f"Elapsed {elapsed}s  ETA {eta_str}{best_str}"
        )

    #  Status 
    status_var = ctk.StringVar(value="")
    status_lbl = ctk.CTkLabel(dlg, textvariable=status_var,
                                font=ctk.CTkFont(FONT_BODY, 11, "bold"),
                                text_color=COLORS["warning"])
    status_lbl.pack(pady=(0, 4), padx=24, anchor="w")

    #  Bottom buttons (state-dependent) 
    btns = ctk.CTkFrame(dlg, fg_color="transparent")
    btns.pack(side="bottom", fill="x", padx=20, pady=16)

    run_btn = ctk.CTkButton(btns, text=f" {title}",
                               height=36, corner_radius=8, width=180,
                               font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                               fg_color=accent, hover_color=COLORS["panel_hover"],
                               text_color="#ffffff")
    cancel_btn = ctk.CTkButton(btns, text="Cancel",
                                   height=36, corner_radius=8, width=100,
                                   font=ctk.CTkFont(FONT_BODY, 12, "bold"),
                                   fg_color="transparent", hover_color=COLORS["panel_hover"],
                                   text_color=COLORS["text_dim"],
                                   border_width=1, border_color=COLORS["border"],
                                   command=dlg.destroy)

    run_btn.pack(side="right")
    cancel_btn.pack(side="right", padx=(0, 8))

    #  Output formatting / classification 
    log_text._textbox.tag_configure("info",  foreground=COLORS["text"])
    log_text._textbox.tag_configure("good",  foreground=COLORS["success"])
    log_text._textbox.tag_configure("bad",   foreground=COLORS["danger"])
    log_text._textbox.tag_configure("warn",  foreground=COLORS["warning"])
    log_text._textbox.tag_configure("dim",   foreground=COLORS["text_dim"])
    log_text._textbox.tag_configure("hdr",   foreground=COLORS["balanced"])
    # Trophy tags are foreground-colour only (no background/spacing) so
    # they read as text highlights rather than full-width coloured bars.
    log_text._textbox.tag_configure(
        "trophy",
        foreground="#fbbf24",                   # gold
        font=(app.mono_font, 11, "bold"),
    )
    log_text._textbox.tag_configure(
        "trophy_val",
        foreground=COLORS["success"],
        font=(app.mono_font, 10, "bold"),
    )

    def _classify_line(line: str) -> str:
        up = line.upper()
        if "" in line or "BESTE KONFIGURATION" in up:
            return "trophy"
        if any(t in line for t in ("Avg Netto:", "Konsistenz:", "Folds:",
                                      "Vollperiode:")) and "BESTE" not in up:
            return "trophy_val"
        severity = classify_severity(line)
        if severity == "error":
            return "bad"
        if severity == "warn":
            return "warn"
        if any(t in up for t in ("", "ROBUST", "WIN", "PROFIT")):
            return "good"
        if any(t in line for t in ("", "", "===")):
            return "hdr"
        return "info"

    # Parse-marker buffer + extracted best config
    parse_state: dict = {
        "in_marker": False,
        "marker_buf": "",
        "best_config": None,
        "trophy_lines_seen": 0,
        "run_generation": 0,
        "output_complete": False,
        "expected_strategy": None,
        "apply_expectations": None,
    }

    def _append_line(line: str) -> None:
        """Append a line to the log. Filters out machine markers."""
        # Progress marker  update bar, don't render line
        if "<<<PROGRESS>>>" in line and "<<<END>>>" in line:
            try:
                payload = line.split("<<<PROGRESS>>>", 1)[1]
                payload = payload.split("<<<END>>>", 1)[0]
                import json as _json
                data = _json.loads(payload)
                dlg.after(0, lambda d=data: _update_progress(d))
            except Exception:
                pass
            return

        # Best-config marker  parse and stash; don't render
        if OPTIMIZER_BEST_CONFIG_START in line:
            parse_state["best_config"] = None
            cfg = optimizer_config_from_complete_marker(
                line,
                expected_strategy=parse_state.get("expected_strategy"),
            )
            if cfg is not None:
                parse_state["best_config"] = cfg
            return

        tag = _classify_line(line)
        log_text.configure(state="normal")
        log_text._textbox.insert("end", line + "\n", tag)
        # Cap optimizer log at 2200 lines so a 30k-combination FULL_SPACE
        # run doesn't choke the UI thread.
        try:
            line_count = int(log_text._textbox.index("end-1c").split(".")[0])
            if line_count > 2200:
                log_text._textbox.delete("1.0", f"{line_count - 2000}.0")
        except Exception:
            pass
        log_text.configure(state="disabled")
        log_text.see("end")

    #  Apply-best-config button (appears when optimizer finishes) 
    apply_btn_ref: dict = {"btn": None}

    def _show_apply_button(run_generation: int):
        if run_generation != parse_state.get("run_generation"):
            return
        if apply_btn_ref["btn"] is not None:
            return
        cfg = parse_state.get("best_config")
        if not cfg:
            return
        if optimizer_promotion_reasons(cfg):
            return
        try:
            optimizer_apply_provenance(cfg)
        except ValueError:
            return
        if not isinstance(parse_state.get("apply_expectations"), dict):
            return

        apply_btn = ctk.CTkButton(
            btns,
            text=f" Apply best config to {cfg.get('strategy', '?')}",
            height=36, corner_radius=8, width=260,
            font=ctk.CTkFont(FONT_BODY, 12, "bold"),
            fg_color=COLORS["success"], hover_color="#0d9b6c",
            text_color="#ffffff",
            command=lambda: _apply_best_config(cfg)
        )
        apply_btn.pack(side="left", padx=(0, 8))
        apply_btn_ref["btn"] = apply_btn

    def _apply_best_config(cfg: dict):
        """Promote the optimizer's best config into ``app.config``."""
        strategy = cfg.get("strategy")
        if not strategy or strategy not in app.config:
            status_var.set(f" Unknown strategy: {strategy}")
            status_lbl.configure(text_color=COLORS["danger"])
            return

        # The momentum keys below (MIN_PUMP/RSI_MAX/) are NOT read by the live
        # TREND bot  it trades the SMA-ensemble (TREND_SMA_*/TREND_VOTE_*).
        # Refuse to write them so a stray marker can't corrupt the TREND config.
        if strategy == "TREND":
            status_var.set(" TREND tunes via tools.trend_check (SMA-ensemble); "
                           "momentum params don't apply.")
            status_lbl.configure(text_color=COLORS["danger"])
            return

        try:
            applied = apply_optimizer_best_config_to_app(
                app,
                strategy,
                cfg,
                expected_section_values=parse_state.get("apply_expectations"),
            )
        except Exception as exc:
            stale_conflict = isinstance(exc, ConfigMergeConflict)
            try:
                app.config = load_config()
                rows = app.param_rows.get(strategy, {})
                disk_section = app.config.get(strategy, {})
                for cfg_key in OPTIMIZER_CONFIG_MAPPING.values():
                    row = rows.get(cfg_key)
                    if row and cfg_key in disk_section:
                        row.set_value(disk_section[cfg_key])
                app._mark_dirty(strategy, False)
            except Exception:
                pass
            if stale_conflict:
                reset_optimizer_apply_run_state(parse_state, apply_btn_ref)
            status_var.set(f" Config rejected for {strategy}: {exc}")
            status_lbl.configure(text_color=COLORS["danger"])
            _append_line("")
            _append_line(f" Configuration rejected for {strategy}: {exc}")
            return
        if not applied:
            status_var.set(f" No optimizer parameters to apply for {strategy}.")
            status_lbl.configure(text_color=COLORS["danger"])
            _append_line("")
            _append_line(f" No optimizer parameters to apply for {strategy}.")
            return
        app._mark_dirty(strategy, True)  # show the restart hint

        status_var.set(f" Applied {len(applied)} parameter(s) to {strategy}. "
                        f"Restart bot to activate.")
        status_lbl.configure(text_color=COLORS["success"])

        _append_line("")
        _append_line(f" Configuration applied to {strategy} bot:")
        for a in applied:
            _append_line(f"    {a}")
        _append_line(f"  Restart the {strategy} bot to use the new parameters.")

        try:
            apply_btn_ref["btn"].configure(state="disabled",
                                              text=" Applied")
        except Exception:
            pass

    def _import_promotion_artifact() -> None:
        """Stage a verified file; config still changes only on the Apply click."""
        if tool_name != "optimizer":
            return
        if _process_is_alive(proc_state.get("proc")):
            status_var.set(" Stop the running optimizer before importing evidence")
            status_lbl.configure(text_color=COLORS["danger"])
            return
        from tkinter import filedialog

        selected = filedialog.askopenfilename(
            parent=dlg,
            title="Load promotion artifact",
            filetypes=(("Promotion JSON", "*.json"), ("All files", "*.*")),
        )
        if not selected:
            return
        if not reset_optimizer_apply_run_state(parse_state, apply_btn_ref):
            status_var.set(" Cannot invalidate the previous optimizer result")
            status_lbl.configure(text_color=COLORS["danger"])
            return
        expected_strategy = bot_var.get()
        try:
            cfg = optimizer_config_from_promotion_artifact(
                selected, expected_strategy=expected_strategy
            )
            updates, _applied = optimizer_best_config_updates(cfg)
            if not updates:
                raise ValueError("promotion artifact contains no applicable parameters")
            expectations = capture_config_merge_expectations({
                cfg["strategy"]: updates
            })
        except (OSError, TypeError, ValueError, OverflowError) as exc:
            status_var.set(f" Promotion artifact rejected: {exc}")
            status_lbl.configure(text_color=COLORS["danger"])
            return
        parse_state["best_config"] = cfg
        parse_state["expected_strategy"] = cfg["strategy"]
        parse_state["output_complete"] = True
        parse_state["apply_expectations"] = expectations
        _show_apply_button(parse_state["run_generation"])
        status_var.set(
            f" Promotion verified for {cfg['strategy']}; review and Apply manually"
        )
        status_lbl.configure(text_color=COLORS["success"])

    if tool_name == "optimizer":
        import_promotion_btn = ctk.CTkButton(
            btns,
            text="Load promotion artifact",
            height=36,
            corner_radius=8,
            width=190,
            font=ctk.CTkFont(FONT_BODY, 11, "bold"),
            fg_color="transparent",
            hover_color=COLORS["panel_hover"],
            text_color=COLORS["text"],
            border_width=1,
            border_color=COLORS["border"],
            command=_import_promotion_artifact,
        )
        import_promotion_btn.pack(side="left", padx=(0, 8))

    #  Subprocess output reader thread 
    def _read_stdout(proc, run_generation):
        # Pattern to detect the terminal-style progress bar emitted by the
        # optimizer on stderr (which is merged into stdout via stderr=STDOUT).
        # Example: "  Optimize [] 45/648 (7%) ETA 3s Best: +-1.99"
        # The structured <<<PROGRESS>>> marker already drives the in-UI bar,
        # so the raw stderr bar is just noise in the log.
        _PROGRESS_BAR_RE = re.compile(
            r"^\s*\w+\s*\[[\u2588\u2591#=\->\s]+\]\s+\d+/\d+\s+\(\d+%\)"
        )
        framer = _DelimitedByteFramer()
        normal_eof = False

        def _append_raw_output(raw: bytes) -> None:
            if not raw:
                return
            try:
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            except Exception:
                line = str(raw)
            line = re.sub(r'\x1b\[[0-9;]*m', '', line)
            if not line.strip() or _PROGRESS_BAR_RE.match(line):
                return
            proc_state["buffer"].append(line)

        try:
            # When the optimizer uses '\r' to repaint the same line, readline
            # will block until the next '\n'. Split on \r too so we can
            # discard the in-progress bar frames quickly.
            while True:
                chunk = proc.stdout.read(256)
                if not chunk:
                    normal_eof = True
                    break
                if proc_state["stop_reading"]:
                    break
                for raw in framer.feed(chunk):
                    _append_raw_output(raw)
        except Exception as e:
            proc_state["buffer"].append(f"[Reader-Error] {e}")
        finally:
            for raw in framer.finish():
                _append_raw_output(raw)
            # Set exit_code BEFORE done  otherwise the UI tick might see
            # done=True with exit_code=None and treat the run as successful.
            try:
                proc_state["exit_code"] = proc.wait()
            except Exception:
                proc_state["exit_code"] = -1
            if parse_state.get("run_generation") == run_generation:
                parse_state["output_complete"] = bool(
                    normal_eof and not proc_state["stop_reading"]
                )
            if not _process_is_alive(proc):
                try:
                    registry = getattr(app, "tool_processes", None)
                    if registry is not None:
                        registry.unregister(proc)
                except Exception:
                    pass
            proc_state["done"] = True

    #  UI tick: poll the buffer every 120ms 
    spinner_chars = ["  ", "  ", "  ", "  "]
    spinner_idx = [0]

    def _process_is_alive(proc) -> bool:
        if proc is None:
            return False
        try:
            return proc.poll() is None
        except Exception:
            return True

    def _stop_owned_process(proc) -> bool:
        registry = getattr(app, "tool_processes", None)
        try:
            if registry is not None:
                registry.stop(proc)
            else:
                stop_tool_processes([proc])
        except Exception:
            try:
                stop_tool_processes([proc])
            except Exception:
                pass
        return not _process_is_alive(proc)

    def _mark_process_survivor() -> None:
        status_var.set(" Process could not be stopped; launcher still owns it")
        status_lbl.configure(text_color=COLORS["danger"])
        run_btn.configure(
            text=" Process still running",
            state="disabled",
            command=lambda: None,
        )
        cancel_btn.configure(
            text="Close",
            fg_color="transparent",
            text_color=COLORS["text_dim"],
            command=_on_close,
        )

    def _mark_process_survivor_if_open(proc) -> None:
        try:
            if (
                dlg.winfo_exists()
                and proc_state.get("proc") is proc
                and _process_is_alive(proc)
            ):
                _mark_process_survivor()
        except Exception:
            pass

    def _ui_tick():
        # Cap at 100 lines per tick so the Tk main thread isn't blocked
        # for seconds when the optimizer dumps 10,000+ lines at once.
        buf = proc_state["buffer"]
        keep_polling = _drain_tool_output_batch(
            buf,
            _append_line,
            done=bool(proc_state["done"]),
        )

        if keep_polling:
            if not proc_state["done"]:
                spinner_idx[0] = (spinner_idx[0] + 1) % len(spinner_chars)
                spinner_var.set(spinner_chars[spinner_idx[0]])
            dlg.after(0 if proc_state["done"] else 120, _ui_tick)
        else:
            if _process_is_alive(proc_state.get("proc")):
                _mark_process_survivor()
                return
            ec = proc_state["exit_code"]
            published = finalize_optimizer_apply_run(
                parse_state,
                apply_btn_ref,
                ec,
                _show_apply_button,
            )
            if ec == 0:
                spinner_var.set("")
                spinner_lbl.configure(text_color=COLORS["success"])
                if published:
                    status_var.set(" Completed  best config available below")
                else:
                    status_var.set(" Completed successfully")
                status_lbl.configure(text_color=COLORS["success"])
            else:
                spinner_var.set("")
                spinner_lbl.configure(text_color=COLORS["danger"])
                status_var.set(f" Exited with code {ec} (see log above)")
                status_lbl.configure(text_color=COLORS["danger"])

            try:
                cancel_btn.configure(text="Close",
                                      fg_color="transparent",
                                      text_color=COLORS["text_dim"],
                                      command=_on_close)
            except Exception:
                pass

            try:
                run_btn.configure(text=" Run Again",
                                   state="normal",
                                   command=_start_run)
            except Exception:
                pass

            try:
                withdrawn = str(dlg.state()).strip().lower() == "withdrawn"
            except Exception:
                withdrawn = False
            if withdrawn:
                dlg.destroy()

    def _hide_dialog() -> None:
        try:
            dlg.grab_release()
        except Exception as exc:
            status_var.set(f" Cannot hide dialog: {exc}")
            status_lbl.configure(text_color=COLORS["danger"])
            return
        try:
            dlg.withdraw()
        except Exception as exc:
            try:
                dlg.grab_set()
            except Exception:
                pass
            status_var.set(f" Cannot hide dialog: {exc}")
            status_lbl.configure(text_color=COLORS["danger"])

    #  Stop button: hard-kill the subprocess 
    def _stop_subprocess():
        """Stop the running subprocess without blocking the UI.

        Terminate, bounded-wait, kill if needed, and reap in a background
        thread. Streams are closed only after process exit so a reader holding
        the buffered-pipe lock cannot make shutdown itself unbounded.
        """
        import threading as _threading

        proc = proc_state.get("proc")
        proc_state["stop_reading"] = True
        if _process_is_alive(proc):
            def _reap():
                if not _stop_owned_process(proc):
                    try:
                        dlg.after(
                            0,
                            lambda stopped_proc=proc: (
                                _mark_process_survivor_if_open(stopped_proc)
                            ),
                        )
                    except Exception:
                        pass

            try:
                _threading.Thread(target=_reap, daemon=True,
                                   name="proc-reap").start()
            except Exception:
                if not _stop_owned_process(proc):
                    _mark_process_survivor()
                    return False

            status_var.set("Stopping process...")
        else:
            status_var.set("Stopped by user")
        return True

    #  Start function: fired on Run click 
    def _start_run():
        if _process_is_alive(proc_state.get("proc")):
            _mark_process_survivor()
            return

        bot = bot_var.get()
        days = days_var.get()

        if not reset_optimizer_apply_run_state(parse_state, apply_btn_ref):
            status_var.set(" Cannot invalidate the previous optimizer result")
            status_lbl.configure(text_color=COLORS["danger"])
            return
        parse_state["expected_strategy"] = (
            bot if tool_name == "optimizer" else None
        )
        run_generation = parse_state["run_generation"]

        # Phase switch: hide config, show output
        config_frame.pack_forget()
        output_frame.pack(fill="both", expand=True, padx=0, pady=(0, 0))

        # Enlarge window so the log has room (clamped to usable screen)
        safe_geometry(dlg, 780, 680, parent=app)

        mode_str = " (Quick)" if tool_name == "optimizer" and quick_var.get() else ""
        if tool_name == "selftest":
            run_label_var.set("Self-Test  invariant suite or package smoke")
        else:
            run_label_var.set(f"{tool_name.title()}  {bot}  {days}d{mode_str}")

        # Clear any previous log
        log_text.configure(state="normal")
        log_text._textbox.delete("1.0", "end")
        log_text.configure(state="disabled")
        if tool_name == "selftest":
            _append_line("Running self-test...")
            _append_line(
                "Full invariant suite when tests are installed; "
                "otherwise package integrity smoke. Live output below:"
            )
        else:
            _append_line(f"Starting {tool_name} for {bot} on {days} days of data...")
            _append_line("This may take a few minutes. Live output below:")
        _append_line("" * 60)

        # State reset
        proc_state["proc"] = None
        proc_state["buffer"] = _BoundedToolOutputBuffer()
        proc_state["done"] = False
        proc_state["exit_code"] = None
        proc_state["stop_reading"] = False

        # Run button  Stop button
        run_btn.configure(text=" Stop", fg_color="transparent",
                            text_color=COLORS["danger"],
                            border_width=1, border_color=COLORS["danger"],
                            command=_stop_subprocess)
        cancel_btn.configure(text="Hide (keeps running)",
                              command=_hide_dialog)

        proc = None
        try:
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"]       = "1"
            env["PYTHONUNBUFFERED"] = "1"  # required for live output

            # TREND, CROSS and FUTREND are NOT the generic momentum strategy the
            # backtester/optimizer model  route each to the validator that runs
            # its ACTUAL live signal. TREND  trend_check (SMA-ensemble is_in_trend,
            # closed candles only); CROSS  xsec_momentum; FUTREND  leverage curve.
            # These validators take only [days]; the optimizer path adds --sweep so
            # TREND shows its robustness sweep instead of the momentum grid.
            _alt_tool = {"TREND":   "tools.trend_check",
                         "FUTREND": "tools.trend_leverage_check",
                         "CROSS":   "tools.xsec_momentum"}.get(bot)

            base_cmd = [
                _get_python_exe(),
                "-u",
                "-X",
                tool_root_xoption(PROJECT_ROOT),
                "-m",
            ]
            if tool_name == "selftest":
                cmd = [*base_cmd, "tools.selftest"]
            elif _alt_tool:
                cmd = [*base_cmd, _alt_tool, days]
                if tool_name == "optimizer" and bot == "TREND":
                    cmd.append("--sweep")
            elif tool_name == "backtest":
                cmd = [*base_cmd, "tools.backtester", bot, days]
                # Read current parameters from bot_config.json and pass
                # as CLI args so the backtest reflects what the user saved.
                # Without this, backtester falls back to STRATEGY_DEFAULTS
                # (hardcoded values) regardless of what's set in the UI.
                try:
                    _b = _read_tool_bot_config(bot)
                    cmd += _backtest_config_cli_args(_b)
                    if bot == "FUTURES":
                        cmd += ["--funding", str(_futures_funding_8h(_b))]
                except Exception as _e:
                    _append_line(f"[WARN] bot_config.json nicht gelesen: {_e}  nutze Defaults")
            else:
                cmd = [*base_cmd, "tools.optimizer", bot, days]
                if quick_var.get():
                    cmd.append("--quick")
                if bot == "FUTURES":
                    try:
                        _b = _read_tool_bot_config(bot)
                    except Exception:
                        _b = {}
                    cmd += ["--funding", str(_futures_funding_8h(_b))]

            kw = subprocess_no_window_kwargs()

            registry = getattr(app, "tool_processes", None)
            proc = start_registered_tool_process(
                registry,
                cmd,
                root=PROJECT_ROOT,
                cwd=PROJECT_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # merged
                # bufsize=-1: Python 3.13 raises RuntimeWarning for bufsize=1
                # on binary pipes. The subprocess uses -u (unbuffered) so
                # output is flushed immediately regardless of bufsize.
                bufsize=-1,
                **kw,
            )
            proc_state["proc"] = proc

            reader = threading.Thread(
                target=_read_stdout,
                args=(proc, run_generation),
                daemon=True,
            )
            reader.start()
            proc_state["reader_thread"] = reader

            dlg.after(100, _ui_tick)

            status_var.set("")
        except Exception as e:
            proc = proc or proc_state.get("proc")
            stopped = not _process_is_alive(proc)
            if not stopped:
                stopped = _stop_owned_process(proc)
            _append_line(f"[ERROR] Failed to start subprocess: {e}")
            status_var.set(f" Failed: {e}")
            status_lbl.configure(text_color=COLORS["danger"])
            proc_state["exit_code"] = -1
            if stopped:
                proc_state["done"] = True
                dlg.after(0, _ui_tick)
            else:
                proc_state["done"] = False
                _mark_process_survivor()

    run_btn.configure(command=_start_run)

    # Cleanup on dialog close: kill the subprocess
    def _on_close():
        try:
            _stop_subprocess()
        finally:
            dlg.destroy()
    dlg.protocol("WM_DELETE_WINDOW", _on_close)
