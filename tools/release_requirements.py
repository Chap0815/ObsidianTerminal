"""Shared required release files for smoke checks and updater validation."""
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from pathlib import Path, PurePosixPath

RELEASE_ROOT_FILES = frozenset({
    "CHANGELOG.md", "CODE_OF_CONDUCT.md", "CONTRIBUTING.md", "ISSUES.md",
    "LICENSE", "OBSIDIAN.vbs", "README.md", "RELEASE.md", "SECURITY.md",
    "SUPPORT.md", "UPDATE_SETUP.md", "bot_config.default.json", "env_parameter.txt",
    "env_setup_files.py", "install.bat", "install.py", "launcher.pyw",
    "requirements.lock.txt", "requirements.txt", "setup_wizard.pyw", "shared_limits.py",
    "start_launcher.bat", "update.bat", "update_barrier.py",
})
RELEASE_ROOT_METADATA_FILES = frozenset({"DEPLOY_MANIFEST.json"})


def is_release_root_file(name: str) -> bool:
    """Root payloads require explicit public approval; metadata is separate."""
    return name in RELEASE_ROOT_FILES or name in RELEASE_ROOT_METADATA_FILES


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
    "config/dependency_advisory_policy.json",
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
    "trading/capture_trade_identity.py",
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
    "tools/install_locked_dependencies.py",
    "tools/update_check.py",
    "tools/update_from_git.py",
    "tools/update_launcher.py",
    "tools/release_check.py",
    "tools/profit_research.py",
    "tools/strategy_microstructure_research.py",
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
    "config/dependency_advisory_policy.json",
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
    "tools/install_locked_dependencies.py",
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
    "tools/install_locked_dependencies.py",
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
    "tools/strategy_microstructure_research.py",
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


def _inno_payload_path(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("installer manifest path must be a nonempty string")
    parts = value.split("/")
    if any(character in value for character in '\\:{}"*?') or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ) or any(part in {"", ".", ".."} or part.rstrip(" .") != part for part in parts):
        raise ValueError("installer manifest path is not a safe canonical relative path")
    if PurePosixPath(value).is_absolute():
        raise ValueError("installer manifest path must be relative")
    if len(parts) == 1 and value not in RELEASE_ROOT_FILES:
        raise ValueError("installer manifest contains an unapproved root payload")
    return value


def _unique_manifest_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("installer manifest has duplicate JSON keys")
        result[key] = value
    return result


def render_inno_payload_include(root: Path) -> str:
    """Project a freshly hash-verified public manifest into explicit Inno files."""
    # Import lazily: the manifest producer imports the policy constants above.
    from tools.update_deploy_manifest import (
        _manifest_build_id,
        _manifest_root,
        _verify_manifest_snapshot,
    )

    root = _manifest_root(root)
    target = root / "DEPLOY_MANIFEST.json"

    def read_manifest() -> bytes:
        with target.open("rb") as stream:
            data = stream.read(4 * 1024 * 1024 + 1)
        if len(data) > 4 * 1024 * 1024:
            raise ValueError("installer manifest exceeds the bounded read limit")
        return data

    original = read_manifest()
    manifest = json.loads(original.decode("utf-8-sig"), object_pairs_hook=_unique_manifest_object)
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
        raise TypeError("installer manifest must contain a file list")
    files = manifest["files"]
    if type(manifest.get("file_count")) is not int or manifest["file_count"] != len(files):
        raise ValueError("installer manifest has an invalid file count")
    seen = set()
    for item in files:
        if not isinstance(item, dict) or set(item) != {"path", "sha256", "bytes"}:
            raise ValueError("installer manifest has an invalid file record")
        relative = _inno_payload_path(item["path"])
        if relative.casefold() in seen:
            raise ValueError("installer manifest contains duplicate or case-alias paths")
        seen.add(relative.casefold())
        if type(item["bytes"]) is not int or item["bytes"] < 0 or not isinstance(
            item["sha256"], str
        ) or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None:
            raise ValueError("installer manifest has invalid file hashes or sizes")
    build_id = manifest.get("build_id")
    if not isinstance(build_id, str) or re.fullmatch(r"[0-9a-f]{16}", build_id) is None:
        raise ValueError("installer manifest has an invalid build identity")
    if build_id != _manifest_build_id(files):
        raise ValueError("installer manifest build identity does not match its records")
    _verify_manifest_snapshot(root, manifest)
    # Recheck both the link boundary and metadata after the potentially long scan.
    _manifest_root(root)
    if read_manifest() != original:
        raise RuntimeError("installer manifest changed during payload verification")
    lines = [
        "; Generated from the freshly verified public release manifest.",
        f'#if AppBuild != "{build_id}"',
        '  #error Installer AppBuild differs from the verified payload manifest',
        "#endif",
    ]
    for relative in [item["path"] for item in files] + ["DEPLOY_MANIFEST.json"]:
        path = PurePosixPath(relative)
        source = relative.replace("/", "\\")
        parent = path.parent.as_posix()
        destination = "{app}" if parent == "." else "{app}\\" + parent.replace("/", "\\")
        lines.append(f'Source: "{{#ProjectRoot}}\\{source}"; DestDir: "{destination}"; Flags: ignoreversion')
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a verified explicit Inno payload include")
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    text = render_inno_payload_include(args.root)
    from tools.update_deploy_manifest import _absolute_without_links

    staging = Path(__file__).absolute().parents[1] / "installer" / "staging"
    _absolute_without_links(staging, label="installer include staging")
    staging.mkdir(parents=True, exist_ok=True)
    _absolute_without_links(staging, label="installer include staging")
    descriptor, filename = tempfile.mkstemp(prefix="payload_", suffix=".iss", dir=staging)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    print(filename)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
