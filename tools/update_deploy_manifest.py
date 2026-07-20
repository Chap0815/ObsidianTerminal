from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.dont_write_bytecode = True

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.release_requirements import (  # noqa: E402 - project-root bootstrap
    RELEASE_TOOL_FILES,
    REQUIRED_RELEASE_ITEMS,
)


EXCLUDED_DIRS = {
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
    "docs",
    "BOT ADDITIONAL",
    "tests",
}
EXCLUDED_SUFFIXES = {
    ".db",
    ".db-shm",
    ".db-wal",
    ".db3",
    ".db3-shm",
    ".db3-wal",
    ".key",
    ".log",
    ".jsonl",
    ".pem",
    ".pyc",
    ".pyo",
    ".exe",
    ".sqlite",
    ".sqlite-shm",
    ".sqlite-wal",
    ".sqlite3",
    ".sqlite3-shm",
    ".sqlite3-wal",
}
EXCLUDED_NAMES = {
    ".env",
    ".gitignore",
    "DEPLOY_MANIFEST.json",
    "TODO.md",
    "README_GITHUB.md",
    "pytest.ini",
    "structured.jsonl",
}
EXCLUDED_REL_PATHS = {
    "bot_config.json",
    "bot_config.json.lock",
    "config/update_config.json",
    "prompts/spot.txt",
    "prompts/futures.txt",
}


def _secret_name(path: Path) -> bool:
    name_lower = path.name.lower()
    stem_lower = path.stem.lower()
    if name_lower.startswith(("id_rsa", "id_dsa", "id_ecdsa", "id_ed25519")):
        return True
    return any(marker in stem_lower for marker in ("secret", "token", "private"))


def _skip(path: Path, root: Path) -> bool:
    rel = path.relative_to(root)
    rel_posix = rel.as_posix()
    rel_posix_lower = rel_posix.lower()
    excluded_dirs_lower = {part.lower() for part in EXCLUDED_DIRS}
    excluded_rel_lower = {item.lower() for item in EXCLUDED_REL_PATHS}
    excluded_names_lower = {item.lower() for item in EXCLUDED_NAMES}
    if (
        rel.parts
        and rel.parts[0].lower() == "tools"
        and rel_posix not in RELEASE_TOOL_FILES
    ):
        return True
    return (
        rel_posix_lower in excluded_rel_lower
        or any(part.lower() in excluded_dirs_lower for part in rel.parts)
        or path.name.lower().endswith(".bak")
        or ".bak_" in path.name.lower()
        or path.name.lower().endswith((".tmp", ".old", "~"))
        or path.name.lower() in excluded_names_lower
        or _secret_name(path)
        or path.suffix.lower() in EXCLUDED_SUFFIXES
    )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_tracked_files(root: Path) -> list[Path] | None:
    if not (root / ".git").exists():
        return None
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    files: list[Path] = []
    for line in result.stdout.splitlines():
        rel = line.strip()
        if not rel or rel.replace("/", "\\") == "DEPLOY_MANIFEST.json":
            continue
        path = root / rel
        if path.is_file():
            files.append(path.resolve())
    return sorted(files)


def build_manifest(root: Path) -> dict:
    root = root.resolve()
    files = []
    if root.name.endswith("_Release"):
        source_files = sorted(
            path.resolve() for path in root.rglob("*") if path.is_file()
        )
    else:
        source_files = _git_tracked_files(root)
    if source_files is None:
        source_files = sorted(
            path.resolve() for path in root.rglob("*") if path.is_file()
        )
    else:
        required_files = {
            (root / rel).resolve()
            for rel in REQUIRED_RELEASE_ITEMS
            if (root / rel).is_file()
        }
        source_files = sorted(set(source_files) | required_files)

    for path in source_files:
        if path.is_file() and not _skip(path, root):
            files.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "sha256": _sha256(path),
                    "bytes": path.stat().st_size,
                }
            )

    build = hashlib.sha256()
    for item in files:
        build.update(item["path"].encode("utf-8"))
        build.update(item["sha256"].encode("ascii"))
    return {
        "build_id": build.hexdigest()[:16],
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "update_deploy_manifest.py",
        "file_count": len(files),
        "files": files,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    manifest = build_manifest(root)
    out = root / "DEPLOY_MANIFEST.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8", newline="\n")
    print(f"Wrote {out}")
    print(f"Build: {manifest['build_id']} ({manifest['file_count']} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
