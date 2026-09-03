from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
import uuid
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
    ".ruff_cache",
    ".SynologyWorkingDirectory",
    ".venv",
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
    "venv",
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
    ".update_synced.json",
    ".env",
    ".gitignore",
    "AGENTS.md",
    "DEPLOY_MANIFEST.json",
    "PROJECT_MEMORY.md",
    "TODO.md",
    "README_GITHUB.md",
    "cooldown.json",
    "cooldown.json.lock",
    "desktop.ini",
    "pytest.ini",
    "structured.jsonl",
    "structured.jsonl.rotation.lock",
}
EXCLUDED_REL_PATHS = {
    "bot_config.json",
    "bot_config.json.lock",
    "config/update_config.json",
    "prompts/spot.txt",
    "prompts/futures.txt",
}
RUNTIME_REFERENCE_ROOTS = frozenset(
    {"bot_utils", "bots", "core", "launcher", "trading"}
)


def _is_linklike(path: Path) -> bool:
    return path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    )


def _absolute_without_links(path: str | os.PathLike, *, label: str) -> Path:
    requested = Path(path).expanduser().absolute()
    if any(_is_linklike(component) for component in (requested, *requested.parents)):
        raise ValueError(f"{label} must be a real path without links")
    return requested


def _manifest_root(path: str | os.PathLike) -> Path:
    root = _absolute_without_links(path, label="manifest root")
    if not root.is_dir():
        raise ValueError("manifest root must be a real directory without links")
    target = root / "DEPLOY_MANIFEST.json"
    _absolute_without_links(target, label="manifest target")
    return root


def _sync_directory(path: Path) -> None:
    directory = path.resolve(strict=True)
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        create_file.restype = wintypes.HANDLE
        flush_file_buffers = kernel32.FlushFileBuffers
        flush_file_buffers.argtypes = [wintypes.HANDLE]
        flush_file_buffers.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL

        handle = create_file(
            str(directory),
            0x40000000,  # GENERIC_WRITE
            0x00000007,  # FILE_SHARE_READ | WRITE | DELETE
            None,
            3,  # OPEN_EXISTING
            0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
            None,
        )
        invalid_handle = ctypes.c_void_p(-1).value
        if not handle or int(handle) == invalid_handle:
            raise ctypes.WinError(ctypes.get_last_error())
        primary_error: BaseException | None = None
        try:
            if not flush_file_buffers(handle):
                raise ctypes.WinError(ctypes.get_last_error())
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            close_error: BaseException | None = None
            try:
                if not close_handle(handle):
                    close_error = ctypes.WinError(ctypes.get_last_error())
            except BaseException as exc:
                close_error = exc
            if close_error is not None:
                if primary_error is None:
                    raise close_error
                try:
                    primary_error.add_note(
                        "close manifest directory after sync failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(str(directory), flags)
    primary_error: BaseException | None = None
    try:
        os.fsync(directory_fd)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            os.close(directory_fd)
        except BaseException as close_error:
            if primary_error is None:
                raise
            try:
                primary_error.add_note(
                    "manifest directory close failed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            except BaseException:
                pass


def _publish_manifest(path: Path, encoded: bytes) -> None:
    path = _absolute_without_links(path, label="manifest target")
    if not path.parent.is_dir():
        raise ValueError("manifest target parent must be a real directory")
    if path.exists() and not path.is_file():
        raise ValueError("manifest target must be a regular file")
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        _absolute_without_links(temporary, label="manifest target")
        handle = temporary.open("xb")
        temporary_owned = True
        write_primary: BaseException | None = None
        try:
            temporary_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(temporary_stat.st_mode):
                raise ValueError("manifest temporary must be a regular file")
            temporary_identity = (
                temporary_stat.st_dev,
                temporary_stat.st_ino,
            )
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            write_primary = exc
            raise
        finally:
            try:
                handle.close()
            except BaseException as close_error:
                if write_primary is None:
                    raise
                try:
                    write_primary.add_note(
                        "close manifest temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        path = _absolute_without_links(path, label="manifest target")
        if path.exists() and not path.is_file():
            raise ValueError("manifest target must be a regular file")
        os.replace(temporary, path)
        temporary_owned = False
        _sync_directory(path.parent)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        same_generation = False
        if temporary_owned and temporary_identity is not None:
            try:
                current = temporary.stat(follow_symlinks=False)
                same_generation = (
                    stat.S_ISREG(current.st_mode)
                    and not _is_linklike(temporary)
                    and (current.st_dev, current.st_ino) == temporary_identity
                )
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if same_generation:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            try:
                primary_error.add_note(
                    "manifest owned temporary cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            except BaseException:
                pass


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


def _stat_signature(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_source_once(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                byte_count += len(chunk)
                digest.update(chunk)
            after = os.fstat(handle.fileno())
    except OSError as exc:
        raise RuntimeError(
            f"manifest source changed during manifest scan: {path}"
        ) from exc
    signature = _stat_signature(before)
    if (
        signature != _stat_signature(after)
        or byte_count != before.st_size
    ):
        raise RuntimeError(
            f"manifest source changed during manifest scan: {path}"
        )
    return digest.hexdigest(), byte_count


def _source_record(path: Path, root: Path) -> dict:
    path = _absolute_without_links(path, label="manifest source")
    first = _read_source_once(path)
    path = _absolute_without_links(path, label="manifest source")
    second = _read_source_once(path)
    if first != second:
        raise RuntimeError(
            f"manifest source changed during manifest scan: {path}"
        )
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": second[0],
        "bytes": second[1],
    }


def _filesystem_source_files(root: Path) -> list[Path]:
    excluded_dirs = {name.lower() for name in EXCLUDED_DIRS}
    pending = [root]
    files = []
    while pending:
        directory = _absolute_without_links(
            pending.pop(), label="manifest source"
        )
        with os.scandir(directory) as iterator:
            entries = sorted(iterator, key=lambda entry: entry.name.lower())
        child_dirs = []
        for entry in entries:
            path = Path(entry.path).absolute()
            if entry.is_dir(follow_symlinks=False):
                if entry.name.lower() in excluded_dirs:
                    continue
                path = _absolute_without_links(path, label="manifest source")
                child_dirs.append(path)
                continue
            if _skip(path, root):
                continue
            path = _absolute_without_links(path, label="manifest source")
            if entry.is_file(follow_symlinks=False):
                files.append(path)
        pending.extend(reversed(child_dirs))
    return sorted(files)


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
            files.append(_absolute_without_links(path, label="manifest source"))
    return sorted(files)


def _manifest_source_files(root: Path) -> list[Path]:
    if root.name.endswith("_Release"):
        source_files = _filesystem_source_files(root)
    else:
        source_files = _git_tracked_files(root)
    if source_files is None:
        source_files = _filesystem_source_files(root)
    else:
        required_files = {
            _absolute_without_links(root / rel, label="manifest source")
            for rel in REQUIRED_RELEASE_ITEMS
            if (root / rel).is_file()
        }
        source_files = sorted(set(source_files) | required_files)
    return source_files


def _manifest_build_id(files: list[dict]) -> str:
    build = hashlib.sha256()
    for item in files:
        build.update(item["path"].encode("utf-8"))
        build.update(item["sha256"].encode("ascii"))
    return build.hexdigest()[:16]


def _collect_source_records(root: Path) -> list[dict]:
    files = []
    for path in _manifest_source_files(root):
        path = _absolute_without_links(path, label="manifest source")
        if path.is_file() and not _skip(path, root):
            files.append(_source_record(path, root))
    return files


def _verify_manifest_snapshot(root: Path, manifest: dict) -> None:
    current_files = _collect_source_records(root)
    if (
        manifest.get("files") != current_files
        or manifest.get("file_count") != len(current_files)
        or manifest.get("build_id") != _manifest_build_id(current_files)
    ):
        raise RuntimeError("release tree changed before manifest publication")


def _reference_projection(manifest: dict, reference_root: Path) -> dict[str, dict]:
    reference_records = {
        str(item["path"]): item
        for item in _collect_source_records(reference_root)
    }
    release_records = {
        str(item["path"]): item for item in manifest.get("files", [])
    }
    runtime_reference_paths = {
        path
        for path in reference_records
        if path.lower().endswith(".py")
        and path.split("/", 1)[0].lower() in RUNTIME_REFERENCE_ROOTS
    }
    missing_runtime = runtime_reference_paths - set(release_records)
    if missing_runtime:
        raise RuntimeError("release payload does not match reference source")
    projection: dict[str, dict] = {}
    for path, release_item in release_records.items():
        reference_item = reference_records.get(path)
        if reference_item != release_item:
            raise RuntimeError("release payload does not match reference source")
        projection[path] = reference_item
    return projection


def build_manifest(root: Path) -> dict:
    root = _manifest_root(root)
    files = _collect_source_records(root)

    return {
        "build_id": _manifest_build_id(files),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "update_deploy_manifest.py",
        "file_count": len(files),
        "files": files,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--reference-source")
    args = parser.parse_args(argv)
    root = _manifest_root(args.root)
    if not root.name.lower().endswith("_release"):
        raise ValueError(
            "manifest publication requires a release root ending in _Release"
        )
    manifest = build_manifest(root)
    reference_root = None
    reference_projection = None
    if args.reference_source:
        reference_root = _manifest_root(args.reference_source)
        reference_projection = _reference_projection(manifest, reference_root)
    _verify_manifest_snapshot(root, manifest)
    if (
        reference_root is not None
        and _reference_projection(manifest, reference_root)
        != reference_projection
    ):
        raise RuntimeError("reference source changed before manifest publication")
    out = root / "DEPLOY_MANIFEST.json"
    encoded = json.dumps(manifest, indent=2).encode("utf-8")
    _publish_manifest(out, encoded)
    print(f"Wrote {out}")
    print(f"Build: {manifest['build_id']} ({manifest['file_count']} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
