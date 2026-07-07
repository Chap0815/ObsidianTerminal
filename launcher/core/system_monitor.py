"""
System-level statistics (CPU, RAM, GPU, VRAM) used to feed the sidebar
:class:`MiniBar` widgets.

Pure functions  no Tk imports, no globals beyond the small nvidia-smi
result cache attached to :func:`_query_nvidia_smi`.
"""

from __future__ import annotations

import subprocess
import sys
import time

try:
    import psutil  # type: ignore
    HAS_PSUTIL = True
except ImportError:
    psutil = None  # type: ignore
    HAS_PSUTIL = False

from launcher.config.settings import subprocess_no_window_kwargs


#  GPU via nvidia-smi (3 s cache) 

def _query_nvidia_smi():
    """Query nvidia-smi for GPU load + VRAM usage.

    Cached for 3 s  the launcher's refresh loop ticks every 500 ms and we
    don't want to fork a subprocess that often. Returns ``None`` when
    nvidia-smi is missing or fails.
    """
    if _query_nvidia_smi._cached_until > time.time():
        return _query_nvidia_smi._last_result
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
            parts = [p.strip() for p in result.stdout.strip().split(",")]
            if len(parts) >= 4:
                data = {"name": parts[0], "load": float(parts[1]),
                        "vram_used_mb": float(parts[2]),
                        "vram_total_mb": float(parts[3])}
            else:
                data = None
    except Exception:
        data = None
    _query_nvidia_smi._last_result = data
    _query_nvidia_smi._cached_until = time.time() + 3.0
    return data


_query_nvidia_smi._last_result = None      # type: ignore[attr-defined]
_query_nvidia_smi._cached_until = 0        # type: ignore[attr-defined]


#  Aggregate system stats 

def get_system_stats() -> dict:
    """Combined CPU / RAM / GPU snapshot. All fields are ``None`` when their
    source is unavailable so the UI can render "N/A" instead of zeros."""
    stats = {"cpu": None, "ram": None, "ram_used_gb": None, "ram_total_gb": None,
             "gpu": None, "gpu_name": None,
             "vram_pct": None, "vram_used_mb": None, "vram_total_mb": None}
    if HAS_PSUTIL:
        try:
            stats["cpu"] = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            stats["ram"] = mem.percent
            stats["ram_used_gb"] = mem.used / (1024 ** 3)
            stats["ram_total_gb"] = mem.total / (1024 ** 3)
        except Exception:
            pass
    nv = _query_nvidia_smi()
    if nv:
        stats["gpu"] = nv["load"]
        stats["gpu_name"] = nv["name"]
        stats["vram_used_mb"] = nv["vram_used_mb"]
        stats["vram_total_mb"] = nv["vram_total_mb"]
        if nv["vram_total_mb"] > 0:
            stats["vram_pct"] = (nv["vram_used_mb"] / nv["vram_total_mb"]) * 100
    return stats
