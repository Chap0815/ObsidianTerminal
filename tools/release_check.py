from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.release_requirements import (
    RELEASE_TOOL_FILES,
    REQUIRED_MANIFEST_FILES,
    REQUIRED_RELEASE_ITEMS,
)
from tools.update_deploy_manifest import build_manifest


FORBIDDEN_DIRS = {
    ".git",
    ".pytest_cache",
    ".pytest_tmp_review",
    "__pycache__",
    "backups",
    "data",
    "logs",
    "optimizer_results",
    "research_kitraining",
    "research_run2",
    "staging",
    "Output",
    "installer",
    "tests",
    "docs",
    "BOT ADDITIONAL",
}
FORBIDDEN_SUFFIXES = {
    ".db",
    ".db-shm",
    ".db-wal",
    ".db3",
    ".db3-shm",
    ".db3-wal",
    ".exe",
    ".key",
    ".log",
    ".jsonl",
    ".pem",
    ".pyc",
    ".pyo",
    ".sqlite",
    ".sqlite-shm",
    ".sqlite-wal",
    ".sqlite3",
    ".sqlite3-shm",
    ".sqlite3-wal",
}
FORBIDDEN_NAMES = {
    ".env",
    ".gitignore",
    "TODO.md",
    "README_GITHUB.md",
    "pytest.ini",
    "structured.jsonl",
}
FORBIDDEN_REL_PATHS = {
    "bot_config.json",
    "bot_config.json.lock",
    "config/update_config.json",
    "prompts/spot.txt",
    "prompts/futures.txt",
}
REQUIRED = REQUIRED_RELEASE_ITEMS


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _build_id_from_manifest_files(files: list[dict]) -> str:
    build = hashlib.sha256()
    for item in files:
        build.update(str(item.get("path", "")).encode("utf-8"))
        build.update(str(item.get("sha256", "")).encode("ascii"))
    return build.hexdigest()[:16]


def _has_mojibake(text: str) -> bool:
    markers = (chr(0x00C3), chr(0x00C2), chr(0x00E2) + chr(0x20AC), chr(0xFFFD))
    return any(marker in text for marker in markers)


_TELEGRAM_TOKEN_RE = re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{30,}\b")
_GENERIC_SECRET_ASSIGN_RE = re.compile(
    r"(?i)\b(api[_-]?key|api[_-]?secret|secret|token|password|passphrase)"
    r"\b['\"]?\s*[:=]\s*['\"]?([A-Za-z0-9._~+/=\-]{12,})"
)
_BEARER_RE = re.compile(
    r"(?i)\b(?:authorization|proxy-authorization)\s*[:=]\s*bearer\s+"
    r"[A-Za-z0-9._~+/=\-]{16,}"
)
_PROVIDER_TOKEN_RE = re.compile(
    r"\b(?:ghp|github_pat|glpat|sk|xoxb|xoxp)-[A-Za-z0-9._~+/=\-]{16,}\b"
)
_PRIVATE_KEY_MARKERS = (
    "BEGIN " + "OPENSSH " + "PRIVATE KEY",
    "BEGIN " + "PRIVATE KEY",
)
_PLACEHOLDER_VALUES = {
    "changeme",
    "example",
    "exampletoken",
    "placeholder",
    "your_api_key",
    "your_token",
    "your_secret",
}


def _embedded_secret_reason(text: str) -> str | None:
    upper = text.upper()
    if any(marker in upper for marker in _PRIVATE_KEY_MARKERS):
        return "embedded private key"
    if _TELEGRAM_TOKEN_RE.search(text):
        return "embedded Telegram bot token"
    if _BEARER_RE.search(text):
        return "embedded bearer token"
    if _PROVIDER_TOKEN_RE.search(text):
        return "embedded provider token"
    for match in _GENERIC_SECRET_ASSIGN_RE.finditer(text):
        value = (match.group(2) or "").strip().strip("'\"")
        lower = value.lower()
        if lower in _PLACEHOLDER_VALUES:
            continue
        if lower.startswith(("example", "your_", "dummy", "test_")):
            continue
        if lower.startswith(("self.", "exchange.", "getattr", "os.getenv", "config.")):
            continue
        if "***" in value:
            continue
        return f"embedded {match.group(1)}"
    return None


def _tracked_files(root: Path) -> list[Path] | None:
    """Return git-tracked files, or None when source is not a git worktree."""
    if not (root / ".git").exists():
        return None
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files"],
            text=True,
            capture_output=True,
            timeout=20,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    files = []
    for line in (result.stdout or "").splitlines():
        rel = line.strip()
        path = root / rel if rel else None
        if path is not None and path.exists():
            files.append(path)
    return files


def _is_backup_temp_artifact(path: Path) -> bool:
    name_lower = path.name.lower()
    return (
        name_lower.endswith(".bak")
        or ".bak_" in name_lower
        or name_lower.endswith((".tmp", ".old", "~"))
    )


def _is_secret_artifact(path: Path) -> bool:
    name_lower = path.name.lower()
    stem_lower = path.stem.lower()
    if name_lower.startswith(("id_rsa", "id_dsa", "id_ecdsa", "id_ed25519")):
        return True
    return any(marker in stem_lower for marker in ("secret", "token", "private"))


def _is_forbidden_release_artifact(path: Path, root: Path) -> str | None:
    rel = path.relative_to(root)
    parts = set(rel.parts)
    parts_lower = {part.lower() for part in rel.parts}
    forbidden_dirs_lower = {part.lower() for part in FORBIDDEN_DIRS}
    forbidden_rel_lower = {part.lower() for part in FORBIDDEN_REL_PATHS}
    forbidden_names_lower = {part.lower() for part in FORBIDDEN_NAMES}
    if ".git" in parts_lower:
        return None
    rel_posix = rel.as_posix()
    rel_posix_lower = rel_posix.lower()
    if rel.parts and rel.parts[0].lower() == "tools" and rel_posix not in RELEASE_TOOL_FILES:
        return f"forbidden non-release tool in release: {rel}"
    if parts_lower & forbidden_dirs_lower:
        return f"forbidden file under runtime/test directory: {rel}"
    if rel_posix_lower in forbidden_rel_lower:
        return f"forbidden user/update config in release: {rel}"
    if path.name.lower() in forbidden_names_lower or path.suffix.lower() in FORBIDDEN_SUFFIXES:
        return f"forbidden runtime/secret artifact in release: {rel}"
    if _is_secret_artifact(path):
        return f"forbidden secret artifact in release: {rel}"
    if _is_backup_temp_artifact(path):
        return f"forbidden backup/temp artifact in release: {rel}"
    return None


def check_release(source: Path, strict_release_name: bool = False) -> tuple[list[str], list[str]]:
    root = source.resolve()
    errors: list[str] = []
    warnings: list[str] = []

    if strict_release_name and not root.name.endswith("_Release"):
        errors.append(f"release source must be *_Release, got {root.name}")

    for rel in REQUIRED:
        if not (root / rel).exists():
            errors.append(f"missing required release item: {rel}")

    manifest_path = root / "DEPLOY_MANIFEST.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
            if not manifest.get("build_id"):
                errors.append("DEPLOY_MANIFEST.json has no build_id")
            files = manifest.get("files")
            if not files:
                errors.append("DEPLOY_MANIFEST.json has no file list")
            elif not isinstance(files, list):
                errors.append("DEPLOY_MANIFEST.json files must be a list")
            else:
                by_path = {}
                for item in files:
                    rel = item.get("path") if isinstance(item, dict) else None
                    if not rel:
                        errors.append("DEPLOY_MANIFEST.json has file without path")
                        continue
                    by_path[str(rel).replace("\\", "/")] = item
                for rel in REQUIRED_MANIFEST_FILES:
                    rel_norm = rel.replace("\\", "/")
                    if rel_norm not in by_path:
                        errors.append(
                            f"DEPLOY_MANIFEST.json missing required file: {rel}"
                        )
                expected_manifest = build_manifest(root)
                expected_by_path = {
                    str(item["path"]).replace("\\", "/"): item
                    for item in expected_manifest.get("files", [])
                }
                expected_paths = set(expected_by_path)
                manifest_paths = set(by_path)
                for rel_norm in sorted(expected_paths - manifest_paths):
                    errors.append(
                        f"DEPLOY_MANIFEST.json missing file: {rel_norm}"
                    )
                for rel_norm in sorted(manifest_paths - expected_paths):
                    errors.append(
                        f"DEPLOY_MANIFEST.json unexpected file: {rel_norm}"
                    )
                expected_build = _build_id_from_manifest_files(files)
                if manifest.get("build_id") != expected_build:
                    errors.append(
                        "DEPLOY_MANIFEST.json build_id does not match file list"
                    )
                if manifest.get("build_id") != expected_manifest.get("build_id"):
                    errors.append(
                        "DEPLOY_MANIFEST.json build_id does not match current release files"
                    )
                for rel_norm, item in by_path.items():
                    path = root / rel_norm
                    if not path.is_file():
                        errors.append(
                            f"DEPLOY_MANIFEST.json references missing file: {rel_norm}"
                        )
                        continue
                    try:
                        size = path.stat().st_size
                        digest = _sha256(path)
                    except OSError as exc:
                        errors.append(
                            f"DEPLOY_MANIFEST.json cannot hash {rel_norm}: {exc}"
                        )
                        continue
                    if int(item.get("bytes", -1)) != size:
                        errors.append(
                            f"DEPLOY_MANIFEST.json byte mismatch: {rel_norm}"
                        )
                    if item.get("sha256") != digest:
                        errors.append(
                            f"DEPLOY_MANIFEST.json hash mismatch: {rel_norm}"
                        )
        except Exception as exc:
            errors.append(f"DEPLOY_MANIFEST.json unreadable: {exc}")

    tracked = _tracked_files(root)
    paths = [
        p for p in root.rglob("*")
        if ".git" not in p.relative_to(root).parts
    ]
    if tracked is not None:
        warnings.append("checking all release files, including untracked files")
        for path in root.rglob("*"):
            if path.is_file():
                artifact_error = _is_forbidden_release_artifact(path, root)
                if artifact_error:
                    errors.append(artifact_error)

    for path in paths:
        rel = path.relative_to(root)
        parts = set(rel.parts)
        parts_lower = {part.lower() for part in rel.parts}
        forbidden_dirs_lower = {part.lower() for part in FORBIDDEN_DIRS}
        forbidden_rel_lower = {part.lower() for part in FORBIDDEN_REL_PATHS}
        forbidden_names_lower = {part.lower() for part in FORBIDDEN_NAMES}
        if path.is_dir():
            if path.name.lower() in forbidden_dirs_lower:
                errors.append(f"forbidden runtime/test directory in release: {rel}")
            continue
        artifact_error = _is_forbidden_release_artifact(path, root)
        if artifact_error:
            errors.append(artifact_error)
            continue
        if parts_lower & forbidden_dirs_lower:
            errors.append(f"forbidden file under runtime/test directory: {rel}")
            continue
        rel_posix = rel.as_posix()
        if rel_posix.lower() in forbidden_rel_lower:
            errors.append(f"forbidden user/update config in release: {rel}")
            continue
        if path.name.lower() in forbidden_names_lower or path.suffix.lower() in FORBIDDEN_SUFFIXES:
            errors.append(f"forbidden runtime/secret artifact in release: {rel}")
            continue
        if _is_secret_artifact(path):
            errors.append(f"forbidden secret artifact in release: {rel}")
            continue
        if _is_backup_temp_artifact(path):
            errors.append(f"forbidden backup/temp artifact in release: {rel}")
            continue
        if path.suffix.lower() in {".py", ".bat", ".iss", ".md", ".txt", ".json"}:
            try:
                text = path.read_text(encoding="utf-8-sig", errors="replace")
            except OSError:
                continue
            if _has_mojibake(text):
                errors.append(f"mojibake marker found in release text file: {rel}")
            secret_reason = _embedded_secret_reason(text)
            if secret_reason:
                errors.append(f"{secret_reason} found in release text file: {rel}")

    return errors, warnings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=".")
    parser.add_argument("--strict-release-name", action="store_true")
    parser.add_argument("--simulate-copy", action="store_true")
    args = parser.parse_args(argv)
    source = Path(args.source).resolve()
    errors, warnings = check_release(source, strict_release_name=args.strict_release_name)

    if args.simulate_copy and not errors:
        with tempfile.TemporaryDirectory(prefix="obsidian_release_check_") as tmp:
            dst = Path(tmp) / source.name
            shutil.copytree(
                source,
                dst,
                ignore=shutil.ignore_patterns(
                    ".git", ".pytest_cache", "__pycache__", "data", "logs",
                    "optimizer_results", "research_kitraining", "research_run2",
                    "staging", "Output", "tests", "backups",
                ),
            )
            copied_errors, copied_warnings = check_release(dst, strict_release_name=False)
            errors.extend(f"copy: {e}" for e in copied_errors)
            warnings.extend(f"copy: {w}" for w in copied_warnings)

    for warning in warnings:
        print(f"WARN: {warning}")
    for error in errors:
        print(f"ERROR: {error}")
    if errors:
        return 1
    print(f"Release check OK: {source}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
