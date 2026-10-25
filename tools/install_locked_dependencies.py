"""Install the project lock without an unpinned source-build bootstrap."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from urllib import request
from pathlib import Path

_LOCK_MAX_BYTES = 1024 * 1024
_PIP_VERSION = "26.2.2"
_PIP_WHEEL_NAME = "pip-26.2.2-py3-none-any.whl"
_PIP_WHEEL_SHA256 = "fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"
_PIP_WHEEL_URL = (
    "https://files.pythonhosted.org/packages/3f/06/"
    "3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/"
    + _PIP_WHEEL_NAME
)
_PIP_WHEEL_MAX_BYTES = 4 * 1024 * 1024
_PIP_CERTIFI_MIN = (2026, 10, 19)
_PIP_CA_SHA256 = "50f79f98dac192b20dbf73f35c56f0b0c557ab267ff5199d1117cc1180b70785"
INSTALLER_CAPABILITIES = {"schema_version": 1, "installer_api_version": 2,
                        "offline_pip_bootstrap": True, "require_hashes": True,
                        "no_build_isolation": True, "force_reinstall": True}
_PIN = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._-]*)==[^\s]+"
    r"(?:\s+--hash=sha256:[0-9a-fA-F]{64})+"
)


def _numeric_version(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
        return ()
    return tuple(map(int, value.split(".")))


_PIP_TRUST_PROBE = r'''
import hashlib, itertools, json, sys, sysconfig
from importlib import metadata
from pathlib import Path
try:
    base = Path(sysconfig.get_path("purelib")).resolve(strict=True)
    distributions = list(itertools.islice(metadata.Distribution.discover(name="pip", path=[str(base)]), 2))
    if len(distributions) != 1:
        raise ValueError("ambiguous pip metadata")
    distribution = distributions[0]
    import pip
    from pip._vendor import certifi
    if (Path(pip.__file__).resolve(strict=True) != base / "pip" / "__init__.py"
            or Path(certifi.__file__).resolve(strict=True) != base / "pip" / "_vendor" / "certifi" / "__init__.py"
            or Path(distribution.locate_file("")).resolve(strict=True) != base
            or pip.__version__ != distribution.version):
        raise ValueError("pip import and metadata do not match isolated purelib")
    bundle = Path(certifi.where()).resolve(strict=True)
    if bundle != base / "pip" / "_vendor" / "certifi" / "cacert.pem":
        raise ValueError("unexpected pip CA location")
    with bundle.open("rb") as stream:
        ca = stream.read(1024 * 1024 + 1)
    if not ca or len(ca) > 1024 * 1024:
        raise ValueError("pip CA byte budget")
    print(json.dumps({"pip_version": pip.__version__, "ca_version": certifi.__version__,
                      "ca_sha256": hashlib.sha256(ca).hexdigest()}, separators=(",", ":")))
except Exception:
    raise SystemExit(1)
'''


def _pip_has_current_trust() -> bool:
    """Inspect the actual isolated child's imports without contacting an index."""
    try:
        result = subprocess.run([sys.executable, "-I", "-B", "-c", _PIP_TRUST_PROBE],
                                capture_output=True, timeout=15)
        if result.returncode or len(result.stdout) > 4096:
            return False
        facts = json.loads(result.stdout)
        if type(facts) is not dict or set(facts) != {"pip_version", "ca_version", "ca_sha256"}:
            return False
        if _numeric_version(facts["pip_version"]) < _numeric_version(_PIP_VERSION):
            return False
        ca_version = _numeric_version(facts["ca_version"])
        if (ca_version < _PIP_CERTIFI_MIN or not isinstance(facts["ca_sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", facts["ca_sha256"])):
            return False
        # Verify the exact approved bundle for this release. A newer certifi
        # remains supported; never downgrade a newer security trust bundle.
        return ca_version > _PIP_CERTIFI_MIN or facts["ca_sha256"] == _PIP_CA_SHA256
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        return False


class _NoBootstrapRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # urllib has no finally around redirect_request: close without reading
        # an untrusted redirect body before rejecting any intermediate request.
        fp.close()
        raise ValueError("pip bootstrap redirects are not allowed")


def _default_pip_runner(*args):
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("PIP_") or key == "PIP_CACHE_DIR"}
    for key in ("SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
        environment.pop(key, None)
    environment.update(PIP_CONFIG_FILE=os.devnull, PIP_INDEX_URL="https://pypi.org/simple",
                       PIP_NO_INPUT="1", PIP_DISABLE_PIP_VERSION_CHECK="1")
    # pip otherwise adds Windows/system roots to certifi. Select only the
    # verified, refreshed vendored bundle for this child, with TLS checks on.
    return subprocess.run([sys.executable, "-I", "-B", "-m", "pip",
                           "--use-deprecated=legacy-certs", *args], env=environment)


def _bootstrap_pip(temporary: Path, pip_runner, *, wheel_path=None) -> int:
    """Use verified local bytes offline before the first network pip command."""
    raw = None
    if wheel_path is not None:
        with Path(wheel_path).open("rb") as stream:
            raw = stream.read(_PIP_WHEEL_MAX_BYTES + 1)
        if len(raw) > _PIP_WHEEL_MAX_BYTES or hashlib.sha256(raw).hexdigest() != _PIP_WHEEL_SHA256:
            raise ValueError("pip bootstrap wheel failed its pinned SHA256 check")
    if _pip_has_current_trust():
        return 0
    if raw is None:
        # urllib uses Python's platform TLS trust, independently of pip's old
        # vendored certifi. The downloaded wheel must still match the public pin.
        opener = request.build_opener(_NoBootstrapRedirect())
        with opener.open(_PIP_WHEEL_URL, timeout=30) as response:
            if not response.geturl().startswith("https://files.pythonhosted.org/"):
                raise ValueError("unexpected pip bootstrap download origin")
            raw = response.read(_PIP_WHEEL_MAX_BYTES + 1)
        if len(raw) > _PIP_WHEEL_MAX_BYTES or hashlib.sha256(raw).hexdigest() != _PIP_WHEEL_SHA256:
            raise ValueError("pip bootstrap wheel failed its pinned SHA256 check")
    wheel = temporary / _PIP_WHEEL_NAME
    wheel.write_bytes(raw)
    pinned = temporary / "pip.bootstrap.requirements.txt"
    pinned.write_text(f"pip=={_PIP_VERSION} --hash=sha256:{_PIP_WHEEL_SHA256}\n", encoding="utf-8")
    result = pip_runner("install", "--no-index", "--no-deps", "--require-hashes",
                        "--find-links", str(temporary), "-r", str(pinned))
    if result.returncode:
        return result.returncode
    if not _pip_has_current_trust():
        raise ValueError("pip bootstrap did not establish the required TLS trust bundle")
    return 0


def install_locked_requirements(lock_path: Path, *, pip_runner=None, pip_bootstrap_wheel=None,
                                force_reinstall=False) -> int:
    """Bootstrap TLS trust and the pinned builder before hash-locked installs.

    An injected runner owns its execution environment; the default runner
    verifies the current interpreter's pip before any network pip operation.
    """
    if type(force_reinstall) is not bool:
        raise ValueError("force_reinstall must be boolean")
    with lock_path.open("rb") as handle:
        original = handle.read(_LOCK_MAX_BYTES + 1)
    if len(original) > _LOCK_MAX_BYTES:
        raise ValueError("dependency lock exceeds size limit")
    builder = None
    seen = set()
    for raw in original.decode("utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _PIN.fullmatch(line)
        if match is None:
            raise ValueError("dependency lock contains an unpinned or unhashed entry")
        name = re.sub(r"[-_.]+", "-", match.group(1)).lower()
        if name in seen:
            raise ValueError("dependency lock contains duplicate entries")
        seen.add(name)
        if name == "setuptools":
            builder = line
    if builder is None:
        raise ValueError("dependency lock is missing its pinned source-build helper")

    owned_runner = pip_runner is None
    if owned_runner:
        pip_runner = _default_pip_runner

    with tempfile.TemporaryDirectory(prefix="tradingbot_locked_build_") as temporary:
        if owned_runner or pip_bootstrap_wheel is not None:
            result = _bootstrap_pip(Path(temporary), pip_runner, wheel_path=pip_bootstrap_wheel)
            if result:
                return result
            with lock_path.open("rb") as handle:
                current = handle.read(_LOCK_MAX_BYTES + 1)
            if current != original:
                raise ValueError("dependency lock changed during pip bootstrap")
        bootstrap = Path(temporary) / "bootstrap.requirements.txt"
        bootstrap.write_text(builder + "\n", encoding="utf-8")
        result = pip_runner(
            "install", "--require-hashes", "--no-deps", "--only-binary=:all:",
            "-r", str(bootstrap),
        )
        if result.returncode != 0:
            return result.returncode
        with lock_path.open("rb") as handle:
            current = handle.read(_LOCK_MAX_BYTES + 1)
        if current != original:
            raise ValueError("dependency lock changed during build bootstrap")
        arguments = ["install", "--require-hashes", "--no-build-isolation"]
        if force_reinstall:
            arguments.append("--force-reinstall")
        result = pip_runner(*arguments, "-r", str(lock_path))
        return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--capabilities", action="store_true",
                        help="report the installer protocol without reading a lock or installing packages")
    parser.add_argument("--force-reinstall", action="store_true")
    parser.add_argument("--pip-bootstrap-wheel", type=Path,
                        help="optional local copy of the publicly SHA256-pinned pip bootstrap wheel")
    args = parser.parse_args()
    if args.capabilities:
        print(json.dumps(INSTALLER_CAPABILITIES, sort_keys=True))
        return 0
    try:
        return install_locked_requirements(args.lock, pip_bootstrap_wheel=args.pip_bootstrap_wheel,
                                           force_reinstall=args.force_reinstall)
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"ERROR: locked dependency installation rejected: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
