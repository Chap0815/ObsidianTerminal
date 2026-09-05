"""
llm_utils.py  LLM availability and keyword fallback.
"""
from __future__ import annotations

import math
import time
import os
import socket
import threading
import portalocker
from bot_utils.runtime_threads import thread_definitely_never_started
import requests
import ollama
from bot_utils.config import _read_config_json
from core.logger import log_event
from news.http_limits import read_bounded_json_response
from news.news_keywords import BEARISH_KEYWORDS, POSITIVE_KEYWORDS as _POSITIVE
from core.constants import (
    OLLAMA_URL as _ollama_url_fn,
    LLM_MODEL_DEFAULT,
    LLM_INFERENCE_TIMEOUT,
    LLM_MAX_CONCURRENT,
    LLM_SLOT_WAIT_SEC,
)


NEGATIVE_KEYWORDS = list(BEARISH_KEYWORDS)
POSITIVE_KEYWORDS = list(_POSITIVE)


OLLAMA_URL = _ollama_url_fn()
_LLM_SLOT_FILE_MAX_BYTES = 1024
_LLM_SLOT_MAX_WAIT_SEC = 300.0


def _read_llm_slot_payload(path: str) -> str:
    with open(path, "rb") as stream:
        raw = stream.read(_LLM_SLOT_FILE_MAX_BYTES + 1)
    if len(raw) > _LLM_SLOT_FILE_MAX_BYTES:
        raise ValueError("LLM slot file exceeds size limit")
    return raw.decode("utf-8").strip()


def _apply_no_think_if_needed(model: str, prompt: str) -> str:
    """Append /no_think to the prompt for Qwen3-family models.

    Why: Qwen3 enables 'thinking mode' by default, which produces a long
    chain-of-thought before the actual answer. For a trading bot this is
    pure latency overhead (we want a fast JSON decision, not reasoning
    traces). On qwen3:4b without /no_think the bot would be SLOWER than
    qwen2.5:7b  defeating the whole point of the model switch.

    Why /no_think and not the think=False parameter:
    Ollama's generate() API has a confirmed bug where think=False is
    silently ignored (issue #14793). Only the chat() API honors it. We
    use generate(), so the in-prompt switch is the only reliable path.

    For non-Qwen3 models (Qwen2.5, Llama, etc.) the /no_think token is
    just unknown text and gets safely ignored  no harm, no change in
    behavior. Detection is conservative: only model names that clearly
    start with 'qwen3' get the suffix.
    """
    if not isinstance(model, str):
        return prompt
    m = model.lower().lstrip()
    if m.startswith("qwen3"):
        # Append at end so it's the last instruction the model sees.
        # Newline first to avoid gluing it onto a content token.
        if "/no_think" not in prompt:
            return prompt.rstrip() + "\n/no_think"
    return prompt


def _config_path() -> str:
    """Absolute path to bot_config.json (falls back to a relative name)."""
    try:
        from core.paths import BOT_CONFIG as _BOT_CONFIG_PATH
        return str(_BOT_CONFIG_PATH)
    except Exception:
        return "bot_config.json"


def _load_model_name(config_path: str) -> str:
    cfg = _read_config_json(config_path)
    if not isinstance(cfg, dict):
        return LLM_MODEL_DEFAULT
    model = cfg.get("LLM_MODEL")
    if not isinstance(model, str):
        return LLM_MODEL_DEFAULT
    model = model.strip()
    if (
        not 1 <= len(model) <= 200
        or any(not ch.isprintable() or ch.isspace() for ch in model)
    ):
        return LLM_MODEL_DEFAULT
    return model


def _get_model_name() -> str:
    """Read the configured LLM model name from bot_config.json.

    Returns the value of ``LLM_MODEL`` or LLM_MODEL_DEFAULT as fallback.
    """
    try:
        return _load_model_name(_config_path())
    except Exception:
        pass
    return LLM_MODEL_DEFAULT


#  Hot-reload of LLM_MODEL 
# ``get_model_name()`` re-reads bot_config.json on each call, cached by the
# file's stat signature: the first call after an external edit hits disk,
# subsequent calls for the same file generation use the cache. ``MODEL_NAME`` below is a
# seeded string snapshot for callers that ``from .llm_utils import MODEL_NAME``
# at startup; on-the-fly model changes are picked up via get_model_name().

_MODEL_CACHE = {"value": None, "signature": None}


def get_model_name() -> str:
    """Public hot-reloading getter. Re-reads bot_config.json when the file
    has changed. Safe to call from anywhere on the hot path.
    """
    config_path = _config_path()
    try:
        stat_result = os.stat(config_path)
        signature = (
            stat_result.st_dev,
            stat_result.st_ino,
            stat_result.st_size,
            stat_result.st_mtime_ns,
            stat_result.st_ctime_ns,
        )
    except OSError:
        # Config gone  keep using whatever we last had, or default
        return _MODEL_CACHE["value"] or LLM_MODEL_DEFAULT
    if (_MODEL_CACHE["value"] is None
            or signature != _MODEL_CACHE.get("signature")):
        try:
            value = _load_model_name(config_path)
        except Exception:
            # Preserve the last validated value and retry this generation on
            # the next call after a transient read/replace race.
            return _MODEL_CACHE["value"] or LLM_MODEL_DEFAULT
        _MODEL_CACHE["value"] = value
        _MODEL_CACHE["signature"] = signature
    return _MODEL_CACHE["value"]


# Seed the cache so MODEL_NAME below has a value at import time
MODEL_NAME = get_model_name()
LLM_INFERENCE_TIMEOUT_SEC = float(LLM_INFERENCE_TIMEOUT)
try:
    from core.paths import LLM_SLOTS_DIR
    LLM_LOCK_DIR = str(LLM_SLOTS_DIR)
except Exception:
    # Compute project root from this file's location so the slots dir is
    # invariant to cwd (a relative "llm_slots" path would resolve against
    # whatever the process chdir'd to).
    _here = os.path.dirname(os.path.abspath(__file__))
    _root = os.path.dirname(_here)  # news/  project root
    LLM_LOCK_DIR = os.path.join(_root, "llm_slots")


#  Boot fingerprint 
def _boot_fingerprint() -> str:
    try:
        if os.name == "posix":
            try:
                with open("/proc/sys/kernel/random/boot_id", "r") as fh:
                    return fh.read().strip()
            except OSError:
                pass
        return socket.gethostname()
    except Exception:
        return "unknown-boot"


_BOOT_FP = _boot_fingerprint()


def _pid_alive(pid: int, payload_boot_fp: str = "") -> bool:
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or not 0 < pid <= 0xFFFFFFFF
    ):
        return False
    if payload_boot_fp and payload_boot_fp != _BOOT_FP:
        return False
    try:
        from core.process_identity import pid_alive

        return pid_alive(pid)
    except Exception:
        # Fail closed: an indeterminate holder remains alive until the bounded
        # stale-lock age expires. Never steal a possibly live slot.
        return True


class _LLMSlotLease:
    """One OS-locked slot handle; process exit releases it automatically."""

    def __init__(self, path: str, lock, handle, payload: bytes) -> None:
        self.path = path
        self.lock = lock
        self.handle = handle
        self.payload = payload
        self._released = False
        self._release_lock = threading.Lock()

    def release(self) -> None:
        with self._release_lock:
            if self._released:
                return
            self._released = True
            try:
                self.lock.release()
            finally:
                try:
                    if not self.handle.closed:
                        self.handle.close()
                except Exception:
                    pass


def _acquire_llm_slot():
    """Claim one cross-process OS lock. Returns a lease or ``None``."""
    if isinstance(LLM_SLOT_WAIT_SEC, bool):
        return None
    try:
        wait_budget = float(LLM_SLOT_WAIT_SEC)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        not math.isfinite(wait_budget)
        or wait_budget < 0.0
        or wait_budget > _LLM_SLOT_MAX_WAIT_SEC
    ):
        return None
    if (
        not isinstance(LLM_MAX_CONCURRENT, int)
        or isinstance(LLM_MAX_CONCURRENT, bool)
        or not 1 <= LLM_MAX_CONCURRENT <= 64
    ):
        return None
    slot_count = LLM_MAX_CONCURRENT

    try:
        os.makedirs(LLM_LOCK_DIR, exist_ok=True)
    except Exception as e:
        log_event(
            f"[llm] cannot create slot dir {LLM_LOCK_DIR}: {e}  "
            f"forcing keyword fallback for this scan", "WARN",
        )
        return None

    deadline = time.monotonic() + wait_budget
    my_payload = f"{os.getpid()}:{_BOOT_FP}:{time.time():.3f}".encode()

    attempt_immediately = True
    while attempt_immediately or time.monotonic() < deadline:
        attempt_immediately = False
        for slot in range(slot_count):
            lock_path = os.path.join(LLM_LOCK_DIR, f"slot_{slot}.lock")
            lock = None
            try:
                lock = portalocker.Lock(
                    lock_path,
                    mode="a+b",
                    timeout=0.0,
                    check_interval=0.0,
                    flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
                )
                handle = lock.acquire()
                try:
                    handle.seek(0)
                    handle.truncate(0)
                    handle.write(my_payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                except Exception:
                    raise
                return _LLMSlotLease(
                    lock_path,
                    lock,
                    handle,
                    my_payload,
                )
            except Exception:
                if lock is not None:
                    try:
                        lock.release()
                    except Exception:
                        pass
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            break
        time.sleep(min(0.3, remaining))

    return None


def _release_llm_slot(lease):
    if not isinstance(lease, _LLMSlotLease):
        return
    try:
        lease.release()
    except Exception:
        pass


_LAST_USED_MODEL: list  = [None]   # [0] = last model name generate() used

_CLIENT_CACHE: dict = {}           # (host, timeout) -> ollama.Client
_CLIENT_CACHE_LOCK = threading.Lock()
_CLIENT_CLOSE_GENERATION = None
_LLM_SHUTDOWN_EVENT = threading.Event()


def _get_ollama_client(host: str, timeout: float):
    """Return a pooled ollama.Client for (host, timeout), creating once."""
    key = (host, timeout)
    with _CLIENT_CACHE_LOCK:
        if _LLM_SHUTDOWN_EVENT.is_set():
            raise RuntimeError("LLM resources are shutting down")
        client = _CLIENT_CACHE.get(key)
        if client is None:
            client = ollama.Client(host=host, timeout=timeout)
            _CLIENT_CACHE[key] = client
        return client


def _close_ollama_client(client) -> bool:
    """Close one sync Ollama/httpx client without losing retry evidence."""
    closer = getattr(client, "close", None)
    if callable(closer):
        try:
            if closer() is not False:
                return True
        except Exception:
            pass
    transport = getattr(client, "_client", None)
    closer = getattr(transport, "close", None)
    if not callable(closer):
        return False
    try:
        return closer() is not False
    except Exception:
        return False


def generate_with_timeout(model: str, prompt: str,
                           use_json_format: bool = True) -> dict:
    """Call Ollama via ollama.Client (connection pooling, no TCP overhead).

    Options used:
    keep_alive=-1  model stays in VRAM (no idle eviction between calls).
    num_predict  120 for JSON decisions, 60 for plain challenge calls.
    temperature=0.0  deterministic greedy decoding for scoring.
    format="json"  opt-in via OLLAMA_FORCE_JSON (see below).

    ollama.Client keeps an internal HTTP connection pool, avoiding the
    per-call TCP-handshake overhead of bare requests.post (~1s  calls/scan).
    """
    valid_model = (
        isinstance(model, str)
        and 1 <= len(model) <= 200
        and all(ch.isprintable() and not ch.isspace() for ch in model)
    )
    valid_prompt = (
        isinstance(prompt, str)
        and bool(prompt.strip())
        and len(prompt) <= 1_000_000
        and "\x00" not in prompt
    )
    if not valid_model or not valid_prompt or not isinstance(use_json_format, bool):
        raise ValueError("LLM generation request is invalid")

    slot = _acquire_llm_slot()
    if slot is None:
        raise TimeoutError(
            f"LLM slot queue full after {LLM_SLOT_WAIT_SEC}s  "
            f"keyword fallback will be used for this scan cycle"
        )
    _LLM_SLOW_WARN_SEC = 10.0
    _LLM_SLOW_CRIT_SEC = 30.0
    t0 = time.perf_counter()
    try:
        # Detect model change  first call after a switch needs a longer
        # timeout because the new model has to cold-load into VRAM.
        # Old model stays until VRAM pressure forces Ollama to evict it.
        prev_model = _LAST_USED_MODEL[0]
        model_needs_cold_load = prev_model != model
        if prev_model is not None and model_needs_cold_load:
            log_event(
                f"LLM model changed: {prev_model}  {model} "
                f" first call will be slower (cold-load)", "INFO"
            )
        # Use a longer timeout for the first call after a model switch
        # (cold-load can take 15-60s depending on model size and disk speed).
        effective_timeout = (
            max(LLM_INFERENCE_TIMEOUT_SEC, 180.0)
            if model_needs_cold_load
            else LLM_INFERENCE_TIMEOUT_SEC
        )
        client = _get_ollama_client(OLLAMA_URL, effective_timeout)

        # If using a Qwen3-family model, append /no_think to the prompt to
        # disable its (slow) chain-of-thought mode. No-op for other models.
        # See _apply_no_think_if_needed() docstring for the full rationale.
        prompt = _apply_no_think_if_needed(model, prompt)

        kwargs = dict(
            model=model,
            prompt=prompt,
            keep_alive=-1,
            stream=False,
            options={
                # Speed tuning for qwen2.5:14b on RTX 5080
                "num_predict":    120 if use_json_format else 60,
                "num_ctx":        2048,
                "temperature":    0.0,
                "repeat_penalty": 1.0,
            },
        )
        # format="json" uses grammar-constrained sampling in llama.cpp:
        # at every token step it computes the valid JSON token mask, which
        # adds ~30-50% overhead even at temperature=0.0 (greedy decoding).
        # With temp=0 + explicit "Do NOT output text outside the JSON"
        # instruction, qwen2.5 reliably produces clean JSON without the
        # constraint. We only enable it as a safety net for non-JSON calls
        # where the model might ramble.
        # Set OLLAMA_FORCE_JSON=1 in .env to re-enable if you see parsing
        # issues after switching to a different model.
        import os as _os
        if use_json_format and _os.getenv("OLLAMA_FORCE_JSON", "0") == "1":
            kwargs["format"] = "json"
        result  = client.generate(**kwargs)
        _LAST_USED_MODEL[0] = model
        elapsed = time.perf_counter() - t0
        level   = "INFO"
        suffix  = ""
        if elapsed >= _LLM_SLOW_CRIT_SEC:
            level  = "WARN"
            suffix = "  SLOW (model still loading?)"
        elif elapsed >= _LLM_SLOW_WARN_SEC:
            level  = "WARN"
            suffix = "  slower than expected"
        log_event(f"LLM inference: {elapsed:.1f}s [{model}]{suffix}", level)
        return result
    except Exception:
        elapsed = time.perf_counter() - t0
        log_event(f"LLM inference FAILED after {elapsed:.1f}s [{model}]", "WARN")
        raise
    finally:
        _release_llm_slot(slot)


"""LLM status (background-pinged, 3-tier check)

Three layers are now distinguished, matching what ``ollama ps`` and
``ollama list`` expose:

    DAEMON_DOWN  Ollama process not reachable at OLLAMA_URL.

    MODEL_MISSING  Daemon is up, but the configured model is NOT in
                    ``ollama list`` (/api/tags)  inference would fail.

    MODEL_COLD  Daemon up, model installed, NOT currently loaded in
                    RAM (/api/ps empty for this model).  Inference will
                    work but triggers a ~10-30s cold-start load.
                    This is NORMAL after Ollama's idle-timeout evicts
                    the model.  Status shown as "Ready (cold)".

    MODEL_HOT  Daemon up, model installed AND loaded in VRAM/RAM.
                    Inference is fast.  Status shown as "Ready".

``llm_available()`` returns True for MODEL_COLD and MODEL_HOT  both
are usable.  Only DAEMON_DOWN and MODEL_MISSING return False and
trigger the keyword-fallback path.

The ping loop checks every PING_INTERVAL_SEC.  On every tick:
  1. GET /api/tags  confirm daemon + get installed-model list
  2. Check configured model is in that list
  3. GET /api/ps  get loaded-model list for hot/cold
"""

# LLM status constants
LLM_STATUS_UNKNOWN       = "unknown"
LLM_STATUS_DAEMON_DOWN   = "daemon_down"
LLM_STATUS_MODEL_MISSING = "model_missing"
LLM_STATUS_MODEL_COLD    = "model_cold"
LLM_STATUS_MODEL_HOT     = "model_hot"

PING_INTERVAL_SEC = 60.0
PING_TIMEOUT_SEC  = 5.0
PING_MAX_FAILURES = 2

_STATE = {
    "available": None,       # bool (backward-compat)
    "status":    LLM_STATUS_UNKNOWN,   # fine-grained status string
    "last_logged": None,
}
_STATE_LOCK   = threading.Lock()
_PING_STARTED = False
_PING_LOCK    = threading.Lock()
_PING_THREAD: threading.Thread | None = None
_PING_START_UNCERTAIN = False


def _normalise_model_name(name: str) -> str:
    """Normalise a model name for fuzzy matching.

    Preserve the tag because different tags may be different model sizes.
    Ollama's implicit tag is ``latest``.
    """
    if not isinstance(name, str):
        return ""
    normalized = name.strip().lower()
    if not normalized or any(ch.isspace() for ch in normalized):
        return ""
    leaf = normalized.rsplit("/", 1)[-1]
    if ":" not in leaf:
        normalized += ":latest"
    return normalized


def _model_name_matches(configured: str, candidate: str) -> bool:
    cfg = _normalise_model_name(configured)
    found = _normalise_model_name(candidate)
    if not cfg or not found:
        return False
    return (
        cfg == found
        or found.endswith("/" + cfg)
        or cfg.endswith("/" + found)
    )


def _model_names_from_payload(payload) -> list[str]:
    if not isinstance(payload, dict):
        return []
    models = payload.get("models")
    if not isinstance(models, list):
        return []
    names = []
    for model in models[:256]:
        if not isinstance(model, dict):
            continue
        name = model.get("name")
        if isinstance(name, str):
            name = name.strip()
            if name and len(name) <= 256:
                names.append(name)
    return names


_OLLAMA_PROBE_MAX_BYTES = 2 * 1024 * 1024


def _bounded_ollama_probe_payload(response):
    reader_closes = False
    try:
        if getattr(response, "status_code", None) != 200:
            return None
        reader_closes = callable(getattr(response, "iter_content", None))
        return read_bounded_json_response(
            response,
            max_bytes=_OLLAMA_PROBE_MAX_BYTES,
        )
    finally:
        closer = getattr(response, "close", None)
        if callable(closer) and not reader_closes:
            try:
                closer()
            except Exception:
                pass


def _do_one_ping() -> str:
    """Return one of the LLM_STATUS_* constants.

    Steps:
      1. GET /api/tags  daemon check + installed-model list
      2. Match configured model name
      3. GET /api/ps  hot/cold check
    """
    base_url = OLLAMA_URL.rstrip("/")
    model_cfg = get_model_name()

    #  Step 1: daemon + installed models 
    for attempt in range(PING_MAX_FAILURES):
        try:
            r = requests.get(
                f"{base_url}/api/tags",
                timeout=PING_TIMEOUT_SEC,
                stream=True,
            )
            payload = _bounded_ollama_probe_payload(r)
            if payload is not None:
                installed = _model_names_from_payload(payload)
                break
        except Exception:
            installed = None
        if attempt < PING_MAX_FAILURES - 1:
            if _LLM_SHUTDOWN_EVENT.wait(1.0):
                return LLM_STATUS_DAEMON_DOWN
    else:
        return LLM_STATUS_DAEMON_DOWN

    if installed is None:
        return LLM_STATUS_DAEMON_DOWN

    #  Step 2: check configured model is installed 
    model_installed = any(
        _model_name_matches(model_cfg, name) for name in installed
    )
    if not model_installed:
        return LLM_STATUS_MODEL_MISSING

    #  Step 3: check if model is loaded in memory (hot vs cold) 
    try:
        r2 = requests.get(
            f"{base_url}/api/ps",
            timeout=PING_TIMEOUT_SEC,
            stream=True,
        )
        payload = _bounded_ollama_probe_payload(r2)
        if payload is not None:
            loaded = _model_names_from_payload(payload)
            is_hot = any(
                _model_name_matches(model_cfg, name) for name in loaded
            )
            return LLM_STATUS_MODEL_HOT if is_hot else LLM_STATUS_MODEL_COLD
    except Exception:
        pass

    # /api/ps failed but model IS installed  treat as cold
    return LLM_STATUS_MODEL_COLD


def _ping_loop():
    first = True
    while not _LLM_SHUTDOWN_EVENT.is_set():
        if not first and _LLM_SHUTDOWN_EVENT.wait(PING_INTERVAL_SEC):
            break
        first = False
        try:
            new_status = _do_one_ping()
            if _LLM_SHUTDOWN_EVENT.is_set():
                break
            new_avail  = new_status in (LLM_STATUS_MODEL_COLD,
                                         LLM_STATUS_MODEL_HOT)
            log_msg = None
            log_lvl = "INFO"
            with _STATE_LOCK:
                prev_avail  = _STATE["available"]
                prev_status = _STATE["status"]
                _STATE["available"] = new_avail
                _STATE["status"]    = new_status

            if new_status != prev_status:
                # Transitions worth logging:
                if new_status == LLM_STATUS_DAEMON_DOWN:
                    log_msg = ("LLM daemon unreachable "
                               " switching to keyword fallback")
                    log_lvl = "WARN"
                elif new_status == LLM_STATUS_MODEL_MISSING:
                    log_msg = (
                        f"LLM model '{get_model_name()}' not installed "
                        f" run 'ollama pull {get_model_name()}' to fix "
                        f" keyword fallback active")
                    log_lvl = "WARN"
                elif new_status == LLM_STATUS_MODEL_COLD and prev_avail is not True:
                    log_msg = ("LLM ready  model cold (will load "
                               "on first request)")
                elif new_status == LLM_STATUS_MODEL_HOT and prev_avail is not True:
                    log_msg = (f"LLM online ({get_model_name()}) "
                               f" sentiment analysis active")
                elif (new_status == LLM_STATUS_MODEL_COLD
                      and prev_status == LLM_STATUS_MODEL_HOT):
                    # Normal idle eviction  info only, NOT a failure
                    log_msg = ("LLM model evicted from memory (idle timeout) "
                               " will auto-reload on next request")
                    log_lvl = "INFO"

            if log_msg:
                log_event(log_msg, log_lvl)
        except Exception:
            pass


def _ensure_ping_thread_started():
    global _PING_STARTED, _PING_START_UNCERTAIN, _PING_THREAD
    if _LLM_SHUTDOWN_EVENT.is_set():
        return False
    with _PING_LOCK:
        if _LLM_SHUTDOWN_EVENT.is_set():
            return False
        if _PING_STARTED:
            if _PING_START_UNCERTAIN:
                try:
                    start_observed = (
                        _PING_THREAD is not None
                        and (
                            _PING_THREAD.is_alive()
                            or _PING_THREAD.ident is not None
                        )
                    )
                except BaseException:
                    return False
                if not start_observed:
                    return False
                _PING_START_UNCERTAIN = False
            return True
        # Non-blocking: do NOT ping inline (that blocks the caller up to
        # ~11s when Ollama is down). Leave state at its UNKNOWN default
        # (available=None  treated as unavailable) and let the background
        # ping thread run the first check and emit the startup log.
        t = threading.Thread(target=_ping_loop, daemon=True, name="LLMPing")
        _PING_THREAD = t
        _PING_STARTED = True
        _PING_START_UNCERTAIN = False
        try:
            t.start()
        except BaseException as exc:
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(t)
                and _PING_THREAD is t
            ):
                _PING_THREAD = None
                _PING_STARTED = False
                _PING_START_UNCERTAIN = False
            else:
                # Once start() has been invoked, an exception is not proof
                # that a custom or ambiguously launched worker was never
                # queued. Keep this exact generation authoritative.
                _PING_START_UNCERTAIN = True
            raise
        return True


def _client_close_generation_unresolved(generation) -> bool:
    if generation is None:
        return False
    if not generation["done"].is_set():
        return True
    worker = generation.get("thread")
    if worker is None:
        return False
    try:
        return bool(worker.is_alive())
    except BaseException:
        return True


def _run_client_close_generation(generation, cached) -> None:
    global _CLIENT_CACHE
    failed = list(cached)
    try:
        for item in cached:
            if _close_ollama_client(item[1]):
                failed = [entry for entry in failed if entry is not item]
    finally:
        with _CLIENT_CACHE_LOCK:
            for key, client in failed:
                _CLIENT_CACHE.setdefault(key, client)
            generation["result"] = not failed
            generation["done"].set()


def _recover_client_prelaunch_locked() -> None:
    global _CLIENT_CLOSE_GENERATION
    generation = _CLIENT_CLOSE_GENERATION
    if generation is None or not generation.get("prelaunch_restore_pending"):
        return
    for key, client in generation.get("cached", ()):
        _CLIENT_CACHE.setdefault(key, client)
    generation["prelaunch_restore_pending"] = False
    generation["done"].set()
    if _CLIENT_CLOSE_GENERATION is generation:
        _CLIENT_CLOSE_GENERATION = None


def shutdown_llm_resources(timeout: float = 0.0) -> bool:
    """Terminally stop the ping worker and close cached Ollama clients.

    A client whose close fails remains registered for a later finalizer retry.
    No client is closed while the ping worker can still be running.
    """
    global _CLIENT_CLOSE_GENERATION, _PING_START_UNCERTAIN
    if isinstance(timeout, bool):
        return False
    try:
        requested_timeout = float(timeout)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(requested_timeout):
        return False
    budget = min(max(0.0, requested_timeout), threading.TIMEOUT_MAX)
    deadline = time.monotonic() + budget
    _LLM_SHUTDOWN_EVENT.set()
    if not _PING_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        ping_thread = _PING_THREAD
        start_uncertain = _PING_START_UNCERTAIN
    finally:
        _PING_LOCK.release()
    if ping_thread is not None:
        try:
            ping_alive = ping_thread.is_alive()
        except BaseException:
            return False
        if ping_alive:
            try:
                ping_thread.join(
                    timeout=max(0.0, deadline - time.monotonic())
                )
            except BaseException:
                return False
            try:
                if ping_thread.is_alive():
                    return False
            except BaseException:
                return False
        if start_uncertain:
            try:
                start_observed = ping_thread.ident is not None
            except BaseException:
                return False
            if not start_observed:
                return False
            if not _PING_LOCK.acquire(
                timeout=max(0.0, deadline - time.monotonic())
            ):
                return False
            try:
                if _PING_THREAD is ping_thread:
                    _PING_START_UNCERTAIN = False
            finally:
                _PING_LOCK.release()

    if not _CLIENT_CACHE_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    start_worker = None
    cached = []
    generation = None
    try:
        _recover_client_prelaunch_locked()
        existing = _CLIENT_CLOSE_GENERATION
        if _client_close_generation_unresolved(existing):
            generation = existing
        else:
            if existing is not None:
                if existing.get("result") is True and not _CLIENT_CACHE:
                    return True
                _CLIENT_CLOSE_GENERATION = None
            if not _CLIENT_CACHE:
                return True
            cached = list(_CLIENT_CACHE.items())
            generation = {
                "done": threading.Event(),
                "result": False,
                "thread": None,
                "cached": cached,
                "prelaunch_restore_pending": False,
            }
            try:
                start_worker = threading.Thread(
                    target=_run_client_close_generation,
                    args=(generation, cached),
                    name="llm-client-close",
                    daemon=True,
                )
            except BaseException:
                return False
            generation["thread"] = start_worker
            _CLIENT_CACHE.clear()
            _CLIENT_CLOSE_GENERATION = generation
    finally:
        _CLIENT_CACHE_LOCK.release()

    if start_worker is not None:
        try:
            start_worker.start()
        except BaseException as exc:
            if (
                isinstance(exc, Exception)
                and thread_definitely_never_started(start_worker)
            ):
                generation["prelaunch_restore_pending"] = True
                if _CLIENT_CACHE_LOCK.acquire(
                    timeout=max(0.0, deadline - time.monotonic())
                ):
                    try:
                        _recover_client_prelaunch_locked()
                    finally:
                        _CLIENT_CACHE_LOCK.release()
            if not isinstance(exc, Exception):
                raise
            return False

    worker = generation.get("thread")
    if worker is not None and worker is not threading.current_thread():
        try:
            worker.join(timeout=max(0.0, deadline - time.monotonic()))
        except BaseException:
            return False
    if not _CLIENT_CACHE_LOCK.acquire(
        timeout=max(0.0, deadline - time.monotonic())
    ):
        return False
    try:
        return (
            _CLIENT_CLOSE_GENERATION is generation
            and generation.get("result") is True
            and not _client_close_generation_unresolved(generation)
            and not _CLIENT_CACHE
        )
    finally:
        _CLIENT_CACHE_LOCK.release()


def llm_available() -> bool:
    """True if the configured model is usable (installed + daemon up).

    MODEL_COLD counts as available  Ollama will load it on demand.
    MODEL_MISSING and DAEMON_DOWN return False  keyword fallback.
    """
    if not _ensure_ping_thread_started():
        return False
    with _STATE_LOCK:
        return bool(_STATE["available"])


def get_llm_status() -> str:
    """Return one of the LLM_STATUS_* constants (fine-grained)."""
    with _STATE_LOCK:
        return _STATE["status"]


def get_llm_status_text() -> str:
    """Human-readable status for the launcher UI."""
    status = get_llm_status()
    model  = get_model_name()
    if status == LLM_STATUS_MODEL_HOT:
        return f"Ready  {model} (loaded)"
    if status == LLM_STATUS_MODEL_COLD:
        return f"Ready  {model} (cold, loads on request)"
    if status == LLM_STATUS_MODEL_MISSING:
        return f"Model not installed: {model}"
    if status == LLM_STATUS_DAEMON_DOWN:
        return "Daemon offline (keyword fallback)"
    return "Checking"


#  Bull/Bear adversarial mode 
_BULL_BEAR_ENABLED     = os.getenv("BULL_BEAR_MODE", "true").lower() != "false"
_BULL_BEAR_FAIL_CLOSED = os.getenv("BULL_BEAR_FAIL_CLOSED", "false").lower() == "true"
# When the LLM brain is offline, do not open new entries from a bare keyword
# scan unless the operator explicitly opts into fail-open keyword mode.
_LLM_FALLBACK_FAILCLOSED = os.getenv("LLM_FALLBACK_FAILCLOSED", "true").lower() == "true"

_CHALLENGE_TEMPLATE = (
    "A trader wants to {action} {symbol}.\n"
    "Context: {brief}\n\n"
    "Their analysis summary:\n{analysis}\n\n"
    "You are a skeptical risk manager. Give the 2-3 strongest reasons "
    "this trade could FAIL. Be concrete and specific (not generic).\n\n"
    "End with exactly one line:\n"
    "CHALLENGE: STRONG  risks clearly outweigh reward, skip this trade\n"
    "CHALLENGE: WEAK  bull/bear case holds despite the risks"
)


def _challenge_text(value, *, fallback: str, limit: int) -> str:
    if not isinstance(value, str):
        return fallback
    clean = value.replace("\U0001f4ad", "").strip()
    if "</think>" in clean:
        clean = clean.split("</think>", 1)[-1].strip()
    clean = "".join(ch if ch >= " " else " " for ch in clean)
    clean = " ".join(clean.split())
    if not clean:
        return fallback
    if len(clean) > limit:
        clean = "..." + clean[-(limit - 3):]
    return clean


def _interpret_challenge(resp, label: str) -> str:
    """Map a challenge response to OVERRIDE_WAIT / PROCEED.

    ``resp`` is the SDK mapping model (or dict/str) from
    generate_with_timeout. A trailing 'CHALLENGE: STRONG' line means skip the
    trade; an empty answer is treated like the error path (honours
    _BULL_BEAR_FAIL_CLOSED)."""
    text = ""
    if isinstance(resp, str):
        text = resp
    else:
        getter = getattr(resp, "get", None)
        if callable(getter):
            try:
                candidate = getter("response")
            except Exception:
                candidate = None
            if isinstance(candidate, str):
                text = candidate
    verdict = text.upper()
    if not verdict.strip():
        return "OVERRIDE_WAIT" if _BULL_BEAR_FAIL_CLOSED else "PROCEED"
    lines = [ln for ln in verdict.splitlines() if ln.strip()]
    tail_tokens = (lines[-1] if lines else verdict).split()
    challenge = (
        tail_tokens[1]
        if len(tail_tokens) >= 2 and tail_tokens[0] == "CHALLENGE:"
        else ""
    )
    if challenge == "STRONG":
        log_event(f"[Bull/Bear] {label}: STRONG challenge  OVERRIDE_WAIT", "INFO")
        return "OVERRIDE_WAIT"
    if challenge == "WEAK":
        return "PROCEED"
    return "OVERRIDE_WAIT" if _BULL_BEAR_FAIL_CLOSED else "PROCEED"


def bull_bear_challenge(symbol, bull_analysis, context_brief="", confidence=""):
    # ``confidence`` lets the SPOT brains skip the 2nd LLM call on LOW-confidence
    # setups (not worth the latency; usually already filtered by the scan's
    # confidence gate). MEDIUM/HIGH BUYs are challenged.
    if not _BULL_BEAR_ENABLED or not llm_available():
        return "PROCEED"
    if str(confidence).upper() == "LOW":
        return "PROCEED"
    clean = _challenge_text(
        bull_analysis, fallback="No valid analysis supplied.", limit=500
    )
    brief = _challenge_text(
        context_brief,
        fallback="momentum coin, positive news scan passed",
        limit=300,
    )
    prompt = _CHALLENGE_TEMPLATE.format(
        action="BUY", symbol=symbol,
        brief=brief,
        analysis=clean,
    )
    try:
        resp = generate_with_timeout(get_model_name(), prompt,
                                       use_json_format=False)
    except Exception as exc:
        log_event(
            f"[Bull/Bear] challenge failed ({type(exc).__name__})  "
            f"{'forcing WAIT (fail-closed)' if _BULL_BEAR_FAIL_CLOSED else 'keeping BUY (fail-open)'}",
            "WARN")
        return "OVERRIDE_WAIT" if _BULL_BEAR_FAIL_CLOSED else "PROCEED"

    return _interpret_challenge(resp, symbol)


def futures_bull_bear_challenge(symbol, direction, analysis, context_brief=""):
    if not _BULL_BEAR_ENABLED or not llm_available():
        return "PROCEED"
    if direction not in ("LONG", "SHORT"):
        return "PROCEED"
    clean = _challenge_text(
        analysis, fallback="No valid analysis supplied.", limit=500
    )
    if direction == "LONG":
        action = "go LONG on"
        extra  = "Consider: squeeze risk, funding rate pressure, late entry, RSI overextension."
    else:
        action = "go SHORT on"
        extra  = "Consider: short squeeze risk, bullish catalyst, low float, cover rally."
    prompt = _CHALLENGE_TEMPLATE.format(
        action=action, symbol=symbol,
        brief=(
            _challenge_text(context_brief, fallback="", limit=300) + " " + extra
        ).strip(),
        analysis=clean,
    )
    try:
        resp = generate_with_timeout(get_model_name(), prompt,
                                       use_json_format=False)
    except Exception as exc:
        log_event(
            f"[Bull/Bear] futures challenge failed ({type(exc).__name__})  "
            f"{'forcing WAIT (fail-closed)' if _BULL_BEAR_FAIL_CLOSED else 'keeping ' + direction}",
            "WARN")
        return "OVERRIDE_WAIT" if _BULL_BEAR_FAIL_CLOSED else "PROCEED"

    return _interpret_challenge(resp, f"{symbol} {direction}")


def keyword_fallback(symbol: str, news: str, strategy: str = "TREND") -> str:
    """Simple keyword analysis when LLM is offline."""
    news_lower = news.lower() if isinstance(news, str) else ""
    found_negative = [kw for kw in NEGATIVE_KEYWORDS if kw in news_lower]
    if found_negative:
        reason = ", ".join(found_negative[:3])
        return (
            f"[Keyword Fallback] Negative news detected: {reason}.\n"
            f"Technical signals overridden by critical news flags.\n"
            f"CONFIDENCE: HIGH\n"
            f"RESULT: WAIT"
        )
    found_positive = [kw for kw in POSITIVE_KEYWORDS if kw in news_lower]
    pos_note = f"Positive context: {', '.join(found_positive[:2])}. " if found_positive else ""
    if _LLM_FALLBACK_FAILCLOSED:
        # Opt-in (LLM_FALLBACK_FAILCLOSED=true): refuse NEW entries while the
        # LLM is offline rather than buying on a bare keyword scan.
        return (
            f"[Keyword Fallback - FAIL-CLOSED] {pos_note}"
            f"No negative news, but the analysis brain is unavailable - "
            f"refusing a new entry on keyword scan alone.\n"
            f"CONFIDENCE: LOW\n"
            f"RESULT: WAIT"
        )
    return (
        f"[Keyword Fallback] {pos_note}"
        f"No negative news found. Technical filters already passed.\n"
        f"CONFIDENCE: MEDIUM\n"
        f"RESULT: BUY"
    )
