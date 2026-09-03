"""
System-level statistics (CPU, RAM, GPU, VRAM) used to feed the sidebar
:class:`MiniBar` widgets.

Pure functions  no Tk imports, no globals beyond the small nvidia-smi
result cache attached to :func:`_query_nvidia_smi`.
"""

from __future__ import annotations

import math
import os
import subprocess
import threading
import time

from bot_utils.runtime_threads import thread_definitely_never_started

try:
    import psutil  # type: ignore
    HAS_PSUTIL = True
except ImportError:
    psutil = None  # type: ignore
    HAS_PSUTIL = False

from launcher.config.settings import subprocess_no_window_kwargs

_NVIDIA_SUCCESS_CACHE_SEC = 3.0
_NVIDIA_FAILURE_CACHE_SEC = 30.0
_NVIDIA_PROBE_LOCK = threading.Lock()


#  GPU via nvidia-smi (3 s cache)

def _probe_nvidia_smi() -> dict | None:
    """Perform one bounded sensor subprocess call and validate its output."""
    try:
        kw = subprocess_no_window_kwargs()
        result = subprocess.run(
            ["nvidia-smi",
             "--query-gpu=name,utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=2, **kw
        )
        if result.returncode != 0:
            data = None
        else:
            first_row = next(
                (row.strip() for row in result.stdout.splitlines()
                 if row.strip()),
                "",
            )
            parts = [part.strip() for part in first_row.split(",")]
            if len(parts) != 4 or not parts[0]:
                data = None
            else:
                load = float(parts[1])
                vram_used = float(parts[2])
                vram_total = float(parts[3])
                if (
                    not all(math.isfinite(value) for value in (
                        load, vram_used, vram_total
                    ))
                    or not 0.0 <= load <= 100.0
                    or not 0.0 <= vram_used <= vram_total
                    or vram_total <= 0.0
                ):
                    data = None
                else:
                    data = {
                        "name": parts[0],
                        "load": load,
                        "vram_used_mb": vram_used,
                        "vram_total_mb": vram_total,
                    }
    except Exception:
        data = None
    return data


def _nvidia_probe_worker(owner_state: dict) -> None:
    data = _probe_nvidia_smi()
    cache_seconds = (
        _NVIDIA_SUCCESS_CACHE_SEC
        if data is not None
        else _NVIDIA_FAILURE_CACHE_SEC
    )
    with _NVIDIA_PROBE_LOCK:
        owner_state["done"].set()
        if _query_nvidia_smi._probe_state is owner_state:
            _query_nvidia_smi._last_result = data
            _query_nvidia_smi._cached_until = time.monotonic() + cache_seconds
            _query_nvidia_smi._probe_state = None
            _query_nvidia_smi._probe_active = False


def _query_nvidia_smi():
    """Return cached GPU sensors and refresh them without blocking the poller."""
    now = time.monotonic()
    with _NVIDIA_PROBE_LOCK:
        cached = _query_nvidia_smi._last_result
        if _query_nvidia_smi._cached_until > now:
            return cached
        if _query_nvidia_smi._probe_active:
            return cached
        _query_nvidia_smi._probe_active = True
        owner_state = {"done": threading.Event(), "thread": None}
        _query_nvidia_smi._probe_state = owner_state
    try:
        worker = threading.Thread(
            target=lambda: _nvidia_probe_worker(owner_state),
            daemon=True,
            name="launcher-nvidia-probe",
        )
    except BaseException as exc:
        with _NVIDIA_PROBE_LOCK:
            if _query_nvidia_smi._probe_state is owner_state:
                owner_state["done"].set()
                _query_nvidia_smi._probe_state = None
                _query_nvidia_smi._probe_active = False
                _query_nvidia_smi._cached_until = (
                    now + _NVIDIA_FAILURE_CACHE_SEC
                )
        if not isinstance(exc, Exception):
            raise
        return cached
    with _NVIDIA_PROBE_LOCK:
        if _query_nvidia_smi._probe_state is owner_state:
            owner_state["thread"] = worker
    try:
        worker.start()
    except BaseException as exc:
        with _NVIDIA_PROBE_LOCK:
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(worker)
                and _query_nvidia_smi._probe_state is owner_state
            ):
                owner_state["done"].set()
                _query_nvidia_smi._probe_state = None
                _query_nvidia_smi._probe_active = False
                _query_nvidia_smi._cached_until = (
                    now + _NVIDIA_FAILURE_CACHE_SEC
                )
        if not isinstance(exc, Exception):
            raise
    return cached


_query_nvidia_smi._last_result = None      # type: ignore[attr-defined]
_query_nvidia_smi._cached_until = 0        # type: ignore[attr-defined]
_query_nvidia_smi._probe_active = False    # type: ignore[attr-defined]
_query_nvidia_smi._probe_state = None      # type: ignore[attr-defined]


def _query_windows_commit_memory() -> dict | None:
    """Return Windows commit usage, which physical-RAM stats do not expose."""
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", wintypes.DWORD),
                ("dwMemoryLoad", wintypes.DWORD),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(status)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        query = kernel32.GlobalMemoryStatusEx
        query.argtypes = [ctypes.POINTER(MemoryStatusEx)]
        query.restype = wintypes.BOOL
        if not query(ctypes.byref(status)):
            return None
        limit = int(status.ullTotalPageFile)
        available = int(status.ullAvailPageFile)
        if limit <= 0 or available < 0 or available > limit:
            return None
        used = limit - available
        return {
            "percent": used / limit * 100.0,
            "used_bytes": used,
            "limit_bytes": limit,
        }
    except Exception:
        return None


#  Aggregate system stats 

def get_system_stats() -> dict:
    """Combined CPU / RAM / GPU snapshot. All fields are ``None`` when their
    source is unavailable so the UI can render "N/A" instead of zeros."""
    stats = {"cpu": None, "ram": None, "ram_used_gb": None, "ram_total_gb": None,
             "commit": None, "commit_used_gb": None, "commit_limit_gb": None,
             "gpu": None, "gpu_name": None,
             "vram_pct": None, "vram_used_mb": None, "vram_total_mb": None}
    if HAS_PSUTIL:
        try:
            cpu = float(psutil.cpu_percent(interval=None))
            if math.isfinite(cpu) and 0.0 <= cpu <= 100.0:
                stats["cpu"] = cpu
            mem = psutil.virtual_memory()
            ram = float(mem.percent)
            used = float(mem.used)
            total = float(mem.total)
            if (
                all(math.isfinite(value) for value in (ram, used, total))
                and 0.0 <= ram <= 100.0
                and 0.0 <= used <= total
                and total > 0.0
            ):
                stats["ram"] = ram
                stats["ram_used_gb"] = used / (1024 ** 3)
                stats["ram_total_gb"] = total / (1024 ** 3)
        except Exception:
            pass
    commit = _query_windows_commit_memory()
    if commit:
        stats["commit"] = commit["percent"]
        stats["commit_used_gb"] = commit["used_bytes"] / (1024 ** 3)
        stats["commit_limit_gb"] = commit["limit_bytes"] / (1024 ** 3)
    nv = _query_nvidia_smi()
    if nv:
        stats["gpu"] = nv["load"]
        stats["gpu_name"] = nv["name"]
        stats["vram_used_mb"] = nv["vram_used_mb"]
        stats["vram_total_mb"] = nv["vram_total_mb"]
        if nv["vram_total_mb"] > 0:
            stats["vram_pct"] = (nv["vram_used_mb"] / nv["vram_total_mb"]) * 100
    return stats
