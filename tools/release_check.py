from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import request

sys.dont_write_bytecode = True

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.release_requirements import (  # noqa: E402, I001
    RELEASE_TOOL_FILES,
    REQUIRED_MANIFEST_FILES,
    REQUIRED_RELEASE_DIRS,
    REQUIRED_RELEASE_ITEMS,
)
from tools.update_deploy_manifest import (  # noqa: E402
    _is_test_temp_name,
    _sync_directory as _sync_policy_directory,
    build_manifest,
)


FORBIDDEN_DIRS = {
    ".git",
    ".pytest_cache",
    ".pytest_tmp_review",
    ".ruff_cache",
    ".test-tmp",
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
    "tests",
    "venv",
    "docs",
    "BOT ADDITIONAL",
}
IGNORED_WORKTREE_METADATA_DIRS = {".synologyworkingdirectory"}
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
    ".update_synced.json",
    ".env",
    ".gitignore",
    "AGENTS.md",
    "PROJECT_MEMORY.md",
    "TODO.md",
    "README_GITHUB.md",
    "cooldown.json",
    "cooldown.json.lock",
    "pytest.ini",
    "structured.jsonl",
    "structured.jsonl.rotation.lock",
}
FORBIDDEN_REL_PATHS = {
    "bot_config.json",
    "bot_config.json.lock",
    "config/update_config.json",
    "prompts/spot.txt",
    "prompts/futures.txt",
}
REQUIRED = REQUIRED_RELEASE_ITEMS
_SHA256_REQUIREMENT_HASH_RE = re.compile(r"--hash=sha256:[0-9a-fA-F]{64}(?:\s|$)")
_EXACT_REQUIREMENT_RE = re.compile(
    r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)(?:\s|$)"
)
DEPENDENCY_ADVISORY_POLICY_REL = Path("config/dependency_advisory_policy.json")
DEPENDENCY_ADVISORY_POLICY_MAX_BYTES = 1024 * 1024
DEPENDENCY_ADVISORY_MAX_AGE = timedelta(days=30)
DEPENDENCY_ADVISORY_FUTURE_TOLERANCE = timedelta(minutes=5)
OSV_QUERYBATCH_URL = "https://api.osv.dev/v1/querybatch"
RELEASE_METADATA_MAX_BYTES = 4 * 1024 * 1024
RELEASE_TEXT_MAX_BYTES = 4 * 1024 * 1024
RELEASE_TEXT_SUFFIXES = {
    ".bat",
    ".iss",
    ".json",
    ".md",
    ".py",
    ".pyw",
    ".txt",
    ".vbs",
}
RELEASE_TEXT_REL_PATHS = {"config/github_known_hosts"}
RUNTIME_MODULE_ROOTS = ("bot_utils", "bots", "core", "trading", "launcher")
REFERENCE_OPTIONAL_RELEASE_PATHS = {"UPDATE_SETUP.md", "update.bat"}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_release_text(
    path: Path,
    *,
    max_bytes: int,
    label: str,
    errors: str = "strict",
) -> str:
    with path.open("rb") as fh:
        raw = fh.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError(f"{label} exceeds size limit ({max_bytes} bytes)")
    return raw.decode("utf-8-sig", errors=errors)


def _canonical_manifest_path(value: object) -> str | None:
    if not isinstance(value, str) or not value or value != value.strip():
        return None
    if "\\" in value or "\x00" in value or ":" in value or value.startswith("/"):
        return None
    if any(part in {"", ".", ".."} for part in value.split("/")):
        return None
    return value


def _resolved_within_root(path: Path, root: Path) -> Path | None:
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        return None
    return resolved


def _is_ignored_worktree_metadata(path: Path, root: Path) -> bool:
    try:
        rel = path.relative_to(root)
    except ValueError:
        return False
    return any(
        part.lower() in IGNORED_WORKTREE_METADATA_DIRS for part in rel.parts
    )


def _requirements_hash_errors(path: Path) -> list[str]:
    """Return lock-file errors that would weaken pip ``--require-hashes``."""
    errors: list[str] = []
    try:
        lines = _read_release_text(
            path,
            max_bytes=RELEASE_METADATA_MAX_BYTES,
            label="requirements.lock.txt",
        ).splitlines()
    except (OSError, UnicodeError, ValueError) as exc:
        return [f"requirements.lock.txt unreadable: {exc}"]
    pins = 0
    seen_dependencies: set[str] = set()
    for number, raw in enumerate(lines, 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "==" not in line:
            errors.append(
                f"requirements.lock.txt line {number} is not exactly pinned"
            )
            continue
        pins += 1
        dependency = re.sub(r"[-_.]+", "-", line.split("==", 1)[0]).lower()
        if dependency in seen_dependencies:
            errors.append(
                f"requirements.lock.txt line {number} duplicates dependency "
                f"{dependency}"
            )
        else:
            seen_dependencies.add(dependency)
        if not _SHA256_REQUIREMENT_HASH_RE.search(line):
            errors.append(
                f"requirements.lock.txt line {number} has no sha256 artifact hash"
            )
    if pins == 0:
        errors.append("requirements.lock.txt contains no pinned dependencies")
    return errors


def _normalized_package_name(value: object) -> str:
    return re.sub(r"[-_.]+", "-", str(value or "").strip()).lower()


def _locked_requirements(path: Path) -> dict[str, tuple[str, str]]:
    """Return normalized package -> (display name, exact version)."""
    locked: dict[str, tuple[str, str]] = {}
    lines = _read_release_text(
        path,
        max_bytes=RELEASE_METADATA_MAX_BYTES,
        label="requirements.lock.txt",
    ).splitlines()
    for number, raw in enumerate(lines, 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _EXACT_REQUIREMENT_RE.match(line)
        if match is None:
            raise ValueError(
                f"requirements.lock.txt line {number} is not an exact pin"
            )
        display_name, version = match.groups()
        normalized = _normalized_package_name(display_name)
        if normalized in locked:
            raise ValueError(
                f"requirements.lock.txt line {number} duplicates {normalized}"
            )
        locked[normalized] = (display_name, version)
    if not locked:
        raise ValueError("requirements.lock.txt contains no pinned dependencies")
    return locked


def _numeric_version_tuple(value: object) -> tuple[int, ...] | None:
    text = str(value or "").strip()
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", text) is None:
        return None
    return tuple(int(part) for part in text.split("."))


def _dependency_advisory_errors(root: Path) -> list[str]:
    """Validate the offline OSV snapshot bound to this exact lock file."""
    lock_path = root / "requirements.lock.txt"
    policy_path = root / DEPENDENCY_ADVISORY_POLICY_REL
    try:
        locked = _locked_requirements(lock_path)
        policy = json.loads(
            _read_release_text(
                policy_path,
                max_bytes=DEPENDENCY_ADVISORY_POLICY_MAX_BYTES,
                label=str(DEPENDENCY_ADVISORY_POLICY_REL),
            )
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        return [f"dependency advisory policy unreadable: {exc}"]
    if not isinstance(policy, dict) or policy.get("schema_version") != 1:
        return ["dependency advisory policy schema is unsupported"]

    errors: list[str] = []
    try:
        retrieved_text = str(policy.get("retrieved_at_utc") or "").strip()
        retrieved_at = datetime.fromisoformat(
            retrieved_text.replace("Z", "+00:00")
        )
        if retrieved_at.tzinfo is None:
            raise ValueError("timezone is required")
        retrieved_at = retrieved_at.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        errors.append("dependency advisory policy has invalid retrieval time")
    else:
        now = datetime.now(timezone.utc)
        if retrieved_at > now + DEPENDENCY_ADVISORY_FUTURE_TOLERANCE:
            errors.append("dependency advisory policy retrieval time is in the future")
        elif now - retrieved_at > DEPENDENCY_ADVISORY_MAX_AGE:
            errors.append(
                "dependency advisory policy is older than 30 days; run the "
                "explicit OSV policy refresh and review every result"
            )
    expected_lock_hash = str(policy.get("requirements_lock_sha256") or "").lower()
    actual_lock_hash = _sha256(lock_path)
    if not re.fullmatch(r"[0-9a-f]{64}", expected_lock_hash):
        errors.append("dependency advisory policy has invalid lock hash")
    elif expected_lock_hash != actual_lock_hash:
        errors.append(
            "dependency advisory policy is stale for requirements.lock.txt; "
            "run the explicit OSV policy refresh and review every result"
        )

    floors = policy.get("safety_floors")
    if not isinstance(floors, dict) or not floors:
        errors.append("dependency advisory policy has no safety floors")
    else:
        for raw_name, raw_floor in sorted(floors.items()):
            name = _normalized_package_name(raw_name)
            locked_item = locked.get(name)
            floor = _numeric_version_tuple(raw_floor)
            if locked_item is None:
                errors.append(f"dependency safety floor references missing pin: {name}")
                continue
            version = _numeric_version_tuple(locked_item[1])
            if floor is None or version is None:
                errors.append(f"dependency safety floor is not numeric for {name}")
            elif version < floor:
                errors.append(
                    f"dependency {name}=={locked_item[1]} is below safety floor "
                    f"{raw_floor}"
                )

    advisories = policy.get("advisories")
    if not isinstance(advisories, list):
        errors.append("dependency advisory policy advisories must be a list")
        return errors
    seen_ids: set[str] = set()
    for index, item in enumerate(advisories, 1):
        if not isinstance(item, dict):
            errors.append(f"dependency advisory entry {index} is invalid")
            continue
        name = _normalized_package_name(item.get("package"))
        version = str(item.get("version") or "").strip()
        advisory_id = str(item.get("id") or "").strip().upper()
        decision = str(item.get("decision") or "").strip().lower()
        rationale = str(item.get("rationale") or "").strip()
        locked_item = locked.get(name)
        if locked_item is None or locked_item[1] != version:
            errors.append(
                f"dependency advisory {advisory_id or index} is not bound to "
                "the exact locked package version"
            )
        if not advisory_id or advisory_id in seen_ids:
            errors.append(f"dependency advisory entry {index} has invalid/duplicate id")
        else:
            seen_ids.add(advisory_id)
        if decision == "blocked":
            errors.append(
                f"dependency advisory blocks release: {name}=={version} {advisory_id}"
            )
        elif decision != "waived" or len(rationale) < 20:
            errors.append(
                f"dependency advisory requires explicit review: "
                f"{name}=={version} {advisory_id}"
            )
    return errors


def _refresh_dependency_advisory_policy(source: Path, output: Path) -> None:
    """Explicitly query OSV and write an unapproved, review-required snapshot."""
    lock_path = source / "requirements.lock.txt"
    locked = _locked_requirements(lock_path)
    queries = [
        {
            "package": {"ecosystem": "PyPI", "name": display_name},
            "version": version,
        }
        for _name, (display_name, version) in sorted(locked.items())
    ]
    body = json.dumps({"queries": queries}, separators=(",", ":")).encode("utf-8")
    http_request = request.Request(
        OSV_QUERYBATCH_URL,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "obsidian-release-check/1"},
        method="POST",
    )
    with request.urlopen(http_request, timeout=30) as response:
        raw = response.read(DEPENDENCY_ADVISORY_POLICY_MAX_BYTES + 1)
    if len(raw) > DEPENDENCY_ADVISORY_POLICY_MAX_BYTES:
        raise RuntimeError("OSV response exceeds policy size limit")
    payload = json.loads(raw.decode("utf-8"))
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or len(results) != len(queries):
        raise RuntimeError("OSV returned an incomplete query batch")
    advisories: list[dict[str, str]] = []
    ordered_locked = [item for item in sorted(locked.items())]
    for (name, (_display_name, version)), result in zip(
        ordered_locked, results, strict=True
    ):
        vulns = result.get("vulns", []) if isinstance(result, dict) else []
        if not isinstance(vulns, list):
            raise TypeError(f"OSV returned malformed advisories for {name}")
        for vuln in vulns:
            if not isinstance(vuln, dict):
                raise TypeError(f"OSV returned a malformed advisory for {name}")
            advisory_id = str(vuln.get("id") or "").strip().upper()
            if not advisory_id:
                raise RuntimeError(f"OSV returned an advisory without id for {name}")
            advisories.append(
                {
                    "package": name,
                    "version": version,
                    "id": advisory_id,
                    "decision": "review_required",
                    "rationale": "",
                }
            )
    existing_floors: dict[str, str] = {}
    current_policy = source / DEPENDENCY_ADVISORY_POLICY_REL
    try:
        current = json.loads(
            _read_release_text(
                current_policy,
                max_bytes=DEPENDENCY_ADVISORY_POLICY_MAX_BYTES,
                label=str(DEPENDENCY_ADVISORY_POLICY_REL),
            )
        )
        if isinstance(current, dict) and isinstance(current.get("safety_floors"), dict):
            existing_floors = {
                str(key): str(value)
                for key, value in current["safety_floors"].items()
            }
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
        pass
    snapshot = {
        "schema_version": 1,
        "source": OSV_QUERYBATCH_URL,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "requirements_lock_sha256": _sha256(lock_path),
        "safety_floors": existing_floors,
        "advisories": sorted(
            advisories, key=lambda item: (item["package"], item["id"])
        ),
    }
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(
        f".{output.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    encoded = (json.dumps(snapshot, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary_owned = False
    temporary_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        handle = temporary.open("xb")
        temporary_owned = True
        write_primary: BaseException | None = None
        try:
            temporary_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(temporary_stat.st_mode):
                raise ValueError("dependency policy temporary must be regular")
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
                        "close dependency-policy temporary after write failure: "
                        f"{type(close_error).__name__}: {close_error}"
                    )
                except BaseException:
                    pass
        os.replace(temporary, output)
        temporary_owned = False
        _sync_policy_directory(output.parent)
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
                    and not temporary.is_symlink()
                    and (current.st_dev, current.st_ino)
                    == temporary_identity
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
                    "dependency-policy owned temporary cleanup failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            except BaseException:
                pass


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
    if parts_lower & forbidden_dirs_lower or any(_is_test_temp_name(part) for part in rel.parts):
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


def _runtime_python_inventory(root: Path) -> set[str]:
    return set(_runtime_python_files(root))


def _runtime_python_files(root: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for directory in RUNTIME_MODULE_ROOTS:
        module_root = root / directory
        if not module_root.is_dir():
            continue
        for path in module_root.rglob("*.py"):
            try:
                resolved = path.resolve(strict=True)
                resolved.relative_to(root)
            except (OSError, ValueError):
                continue
            files[path.relative_to(root).as_posix()] = resolved
    return files


def _runtime_inventory_errors(root: Path, reference: Path) -> list[str]:
    release_files = _runtime_python_files(root)
    reference_files = _runtime_python_files(reference)
    release_modules = set(release_files)
    reference_modules = set(reference_files)
    errors = [
        f"release missing reference runtime module: {rel}"
        for rel in sorted(reference_modules - release_modules)
    ]
    errors.extend(
        f"release has stale runtime module absent from reference: {rel}"
        for rel in sorted(release_modules - reference_modules)
    )
    for rel in sorted(release_modules & reference_modules):
        try:
            release_hash = _sha256(release_files[rel])
            reference_hash = _sha256(reference_files[rel])
        except OSError as exc:
            errors.append(f"cannot hash reference runtime module {rel}: {exc}")
            continue
        if release_hash != reference_hash:
            errors.append(f"release runtime module differs from reference: {rel}")
    return errors


def _release_payload_reference_errors(root: Path, reference: Path) -> list[str]:
    errors: list[str] = []
    runtime_roots = {part.lower() for part in RUNTIME_MODULE_ROOTS}
    for item in build_manifest(root).get("files", []):
        rel = str(item["path"]).replace("\\", "/")
        parts = rel.split("/")
        if rel in REFERENCE_OPTIONAL_RELEASE_PATHS:
            continue
        if (
            len(parts) > 1
            and parts[0].lower() in runtime_roots
            and rel.lower().endswith(".py")
        ):
            continue
        reference_path = reference / rel
        resolved = _resolved_within_root(reference_path, reference)
        if resolved is None or not resolved.is_file():
            errors.append(f"release payload absent from reference: {rel}")
            continue
        try:
            reference_hash = _sha256(resolved)
        except OSError as exc:
            errors.append(f"cannot hash reference payload {rel}: {exc}")
            continue
        if item.get("sha256") != reference_hash:
            errors.append(f"release payload differs from reference: {rel}")
    return errors


def check_release(
    source: Path,
    strict_release_name: bool = False,
    reference_source: Path | None = None,
) -> tuple[list[str], list[str]]:
    root = source.resolve()
    errors: list[str] = []
    warnings: list[str] = []

    if strict_release_name and not root.name.endswith("_Release"):
        errors.append(f"release source must be *_Release, got {root.name}")

    if reference_source is not None:
        reference = reference_source.resolve()
        if not reference.is_dir():
            errors.append(f"reference source is not a directory: {reference}")
        elif reference == root:
            errors.append("reference source must differ from release source")
        elif reference.name.lower().endswith("_release"):
            errors.append(
                f"reference source must be a DEV tree, got {reference.name}"
            )
        else:
            errors.extend(_runtime_inventory_errors(root, reference))
            errors.extend(_release_payload_reference_errors(root, reference))

    for rel in REQUIRED:
        required_path = root / rel
        if rel in REQUIRED_RELEASE_DIRS:
            if not required_path.is_dir():
                errors.append(f"missing required release directory: {rel}")
        elif not required_path.is_file():
            errors.append(f"missing required release file: {rel}")

    requirements_path = root / "requirements.lock.txt"
    if requirements_path.exists():
        errors.extend(_requirements_hash_errors(requirements_path))
        errors.extend(_dependency_advisory_errors(root))

    manifest_path = root / "DEPLOY_MANIFEST.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(_read_release_text(
                manifest_path,
                max_bytes=RELEASE_METADATA_MAX_BYTES,
                label="DEPLOY_MANIFEST.json",
            ))
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
                    raw_rel = item.get("path") if isinstance(item, dict) else None
                    rel = _canonical_manifest_path(raw_rel)
                    if raw_rel is None:
                        errors.append("DEPLOY_MANIFEST.json has file without path")
                        continue
                    if rel is None:
                        errors.append(
                            f"DEPLOY_MANIFEST.json unsafe manifest path: {raw_rel}"
                        )
                        continue
                    if rel in by_path:
                        errors.append(
                            f"DEPLOY_MANIFEST.json duplicate manifest path: {rel}"
                        )
                        continue
                    by_path[rel] = item
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
                    resolved_path = _resolved_within_root(path, root)
                    if resolved_path is None:
                        errors.append(
                            "DEPLOY_MANIFEST.json path escapes release root: "
                            f"{rel_norm}"
                        )
                        continue
                    try:
                        size = resolved_path.stat().st_size
                        digest = _sha256(resolved_path)
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
        and not _is_ignored_worktree_metadata(p, root)
    ]
    if tracked is not None:
        warnings.append("checking all release files, including untracked files")
        for path in paths:
            if path.is_file():
                artifact_error = _is_forbidden_release_artifact(path, root)
                if artifact_error:
                    errors.append(artifact_error)

    for path in paths:
        rel = path.relative_to(root)
        if _resolved_within_root(path, root) is None:
            errors.append(f"release path escapes release root: {rel}")
            continue
        parts_lower = {part.lower() for part in rel.parts}
        forbidden_dirs_lower = {part.lower() for part in FORBIDDEN_DIRS}
        forbidden_rel_lower = {part.lower() for part in FORBIDDEN_REL_PATHS}
        forbidden_names_lower = {part.lower() for part in FORBIDDEN_NAMES}
        if path.is_dir():
            if path.name.lower() in forbidden_dirs_lower or _is_test_temp_name(path.name):
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
        if (
            path.suffix.lower() in RELEASE_TEXT_SUFFIXES
            or rel_posix.lower() in RELEASE_TEXT_REL_PATHS
        ):
            try:
                text = _read_release_text(
                    path,
                    max_bytes=RELEASE_TEXT_MAX_BYTES,
                    label=str(rel),
                    errors="replace",
                )
            except ValueError as exc:
                errors.append(str(exc))
                continue
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
    parser.add_argument("--reference-source")
    parser.add_argument(
        "--refresh-advisory-policy",
        metavar="OUTPUT",
        help=(
            "explicitly query OSV and write a review-required offline policy; "
            "normal release checks never use the network"
        ),
    )
    args = parser.parse_args(argv)
    source = Path(args.source).resolve()
    if args.refresh_advisory_policy:
        try:
            _refresh_dependency_advisory_policy(
                source,
                Path(args.refresh_advisory_policy),
            )
        except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
            print(f"ERROR: dependency advisory refresh failed: {exc}")
            return 1
        print(f"Advisory policy candidate written: {args.refresh_advisory_policy}")
        print("Review every entry and replace review_required before release.")
        return 0
    reference = (
        Path(args.reference_source).resolve() if args.reference_source else None
    )
    errors, warnings = check_release(
        source,
        strict_release_name=args.strict_release_name,
        reference_source=reference,
    )

    if args.simulate_copy and not errors:
        with tempfile.TemporaryDirectory(prefix="obsidian_release_check_") as tmp:
            dst = Path(tmp) / source.name
            shutil.copytree(
                source,
                dst,
                ignore=shutil.ignore_patterns(
                    ".git", ".SynologyWorkingDirectory", "desktop.ini",
                    ".pytest_cache", ".ruff_cache", ".venv",
                    "venv", "__pycache__",
                    "data", "logs",
                    "optimizer_results", "research_kitraining", "research_run2",
                    "staging", "Output", "tests", "backups",
                ),
            )
            copied_errors, copied_warnings = check_release(
                dst,
                strict_release_name=False,
                reference_source=reference,
            )
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
