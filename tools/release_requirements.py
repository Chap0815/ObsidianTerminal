"""Shared required release files for smoke checks and updater validation."""
from __future__ import annotations

from pathlib import Path


REQUIRED_RELEASE_ITEMS = [
    "launcher.pyw",
    "update_barrier.py",
    "env_setup_files.py",
    "setup_wizard.pyw",
    "OBSIDIAN.vbs",
    "start_launcher.bat",
    "requirements.lock.txt",
    "bot_config.default.json",
    "DEPLOY_MANIFEST.json",
    "bots",
    "core",
    "launcher",
    "bot_utils",
    "config",
    "config/github_known_hosts",
    "config/update_config.example.json",
    "news",
    "trading",
    "trading/entry_quality.py",
    "trading/entry_score_shadow.py",
    "trading/entry_lifecycle.py",
    "trading/runtime_observability.py",
    "trading/spot_exit_shadow.py",
    "trading/carry_sim.py",
    "trading/causality.py",
    "trading/entry_admission.py",
    "trading/entry_executor.py",
    "trading/execution_quality.py",
    "trading/execution_cost_model.py",
    "trading/exit_evidence.py",
    "trading/expectancy_runtime.py",
    "trading/expectancy_telemetry.py",
    "trading/expectancy_training.py",
    "trading/orderflow_experiment.py",
    "trading/portfolio_guard.py",
    "trading/portfolio_risk.py",
    "trading/profit_experiments.py",
    "trading/profit_research_runner.py",
    "trading/research_experiment_suite.py",
    "trading/promotion_gate.py",
    "trading/venue_recorder.py",
    "tools",
    "core/runtime_status.py",
    "launcher/config/settings.py",
    "launcher/ui/app.py",
    "launcher/tool_processes.py",
    "tools/dashboard.py",
    "tools/release_requirements.py",
    "tools/ensure_git.py",
    "tools/update_check.py",
    "tools/update_from_git.py",
    "tools/update_launcher.py",
    "tools/release_check.py",
    "tools/profit_research.py",
    "tools/simulation_workspace.py",
    "tools/selftest.py",
    "bots/main_bot_aggressive.py",
    "bots/main_bot_balanced.py",
    "bots/main_bot_cross.py",
    "bots/main_bot_futures.py",
    "bots/main_bot_trendfut.py",
]

REQUIRED_RELEASE_DIRS = {
    "bot_utils",
    "bots",
    "config",
    "core",
    "launcher",
    "news",
    "tools",
    "trading",
}


UPDATE_SMOKE_FILES = [
    "launcher.pyw",
    "update_barrier.py",
    "env_setup_files.py",
    "setup_wizard.pyw",
    "OBSIDIAN.vbs",
    "start_launcher.bat",
    "requirements.lock.txt",
    "bot_config.default.json",
    "config/github_known_hosts",
    "config/update_config.example.json",
    "bots/main_bot_aggressive.py",
    "bots/main_bot_balanced.py",
    "bots/main_bot_cross.py",
    "bots/main_bot_futures.py",
    "bots/main_bot_trendfut.py",
    "launcher/config/settings.py",
    "launcher/ui/app.py",
    "launcher/tool_processes.py",
    "tools/dashboard.py",
    "tools/ensure_git.py",
    "tools/update_check.py",
    "tools/update_from_git.py",
    "tools/update_launcher.py",
    "tools/release_check.py",
    "tools/simulation_workspace.py",
    "tools/selftest.py",
]


RELEASE_TOOL_FILES = {
    "tools/__init__.py",
    "tools/backtester.py",
    "tools/check_connection.py",
    "tools/dashboard.py",
    "tools/ensure_git.py",
    "tools/futures_capture_replay.py",
    "tools/futures_capture_phase2.py",
    "tools/ohlcv_cache.py",
    "tools/optimizer.py",
    "tools/promotion_bundle.py",
    "tools/profit_research.py",
    "tools/release_check.py",
    "tools/release_requirements.py",
    "tools/simulation_workspace.py",
    "tools/selftest.py",
    "tools/trend_check.py",
    "tools/trend_leverage_check.py",
    "tools/update_check.py",
    "tools/update_deploy_manifest.py",
    "tools/update_from_git.py",
    "tools/update_launcher.py",
    "tools/xsec_momentum.py",
}


def inno_tool_exclude_patterns(tool_files: list[str] | None = None) -> list[str]:
    """Return Inno exclude patterns for every non-release file under tools/."""
    root = Path(__file__).resolve().parents[1]
    tools_dir = root / "tools"
    if tool_files is None:
        if tools_dir.exists():
            tool_files = [
                path.relative_to(root).as_posix()
                for path in tools_dir.iterdir()
                if path.is_file()
            ]
        else:
            tool_files = []
    patterns: list[str] = []
    for rel in sorted(set(tool_files)):
        rel_norm = str(rel).replace("\\", "/")
        if not rel_norm.startswith("tools/") or rel_norm in RELEASE_TOOL_FILES:
            continue
        patterns.append("\\" + rel_norm.replace("/", "\\"))
    return patterns


REQUIRED_MANIFEST_FILES = [
    rel for rel in REQUIRED_RELEASE_ITEMS
    if rel != "DEPLOY_MANIFEST.json" and rel not in REQUIRED_RELEASE_DIRS
]
