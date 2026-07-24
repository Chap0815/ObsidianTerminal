"""
llm_utils.py  LLM availability and keyword fallback.
"""
from __future__ import annotations

import time
import json
import os
import socket
import threading
import requests
import ollama
from core.logger import log_event
from news.news_keywords import BEARISH_KEYWORDS, POSITIVE_KEYWORDS as _POSITIVE
from core.constants import (
    OLLAMA_URL as _ollama_url_fn,
    LLM_MODEL_DEFAULT,
    LLM_INFERENCE_TIMEOUT,
    LLM_MAX_CONCURRENT,
    LLM_SLOT_WAIT_SEC,
    LLM_STALE_LOCK_SEC,
)


NEGATIVE_KEYWORDS = list(BEARISH_KEYWORDS)
POSITIVE_KEYWORDS = list(_POSITIVE)


OLLAMA_URL = _ollama_url_fn()


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


def _get_model_name() -> str:
    """Read the configured LLM model name from bot_config.json.

    Returns the value of ``LLM_MODEL`` or LLM_MODEL_DEFAULT as fallback.
    """
    config_path = _config_path()
    try:
        if os.path.exists(config_path):
            with open(config_path, encoding="utf-8") as f:
                cfg = json.load(f)
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
    except Exception:
        pass
    return LLM_MODEL_DEFAULT


#  Hot-reload of LLM_MODEL 
# ``get_model_name()`` re-reads bot_config.json on each call, cached by the
# file's mtime: the first call after an external edit hits disk, subsequent
# calls within the same mtime window use the cache. ``MODEL_NAME`` below is a
# seeded string snapshot for callers that ``from .llm_utils import MODEL_NAME``
# at startup; on-the-fly model changes are picked up via get_model_name().

_MODEL_CACHE = {"value": None, "mtime": 0.0}


def get_model_name() -> str:
    """Public hot-reloading getter. Re-reads bot_config.json when the file
    has changed (mtime check). Safe to call from anywhere on the hot path.
    """
    config_path = _config_path()
    try:
        mt = os.path.getmtime(config_path)
    except OSError:
        # Config gone  keep using whatever we last had, or default
        return _MODEL_CACHE["value"] or LLM_MODEL_DEFAULT
    if _MODEL_CACHE["value"] is None or mt != _MODEL_CACHE["mtime"]:
        _MODEL_CACHE["value"] = _get_model_name()
        _MODEL_CACHE["mtime"] = mt
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
    if pid <= 0:
        return False
    if payload_boot_fp and payload_boot_fp != _BOOT_FP:
        return False
    if os.name == "nt":
        # CPython's Windows os.kill() is not a POSIX liveness probe: signal 0
        # is passed to TerminateProcess and can kill the slot holder. psutil
        # uses a read-only process query and is already a runtime dependency.
        try:
            import psutil
            return bool(psutil.pid_exists(pid))
        except Exception:
            # Fail closed: an indeterminate holder remains alive until the
            # bounded stale-lock age expires. Never steal a possibly live slot.
            return True
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _acquire_llm_slot():
    """Claim one of LLM_MAX_CONCURRENT slots. Returns path or None."""
    try:
        os.makedirs(LLM_LOCK_DIR, exist_ok=True)
    except Exception as e:
        log_event(
            f"[llm] cannot create slot dir {LLM_LOCK_DIR}: {e}  "
            f"forcing keyword fallback for this scan", "WARN",
        )
        return None

    deadline = time.monotonic() + LLM_SLOT_WAIT_SEC
    my_payload = f"{os.getpid()}:{_BOOT_FP}:{time.time():.3f}".encode()

    while time.monotonic() < deadline:
        for slot in range(LLM_MAX_CONCURRENT):
            lock_path = os.path.join(LLM_LOCK_DIR, f"slot_{slot}.lock")
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                try:
                    os.write(fd, my_payload)
                finally:
                    os.close(fd)
                return (lock_path, my_payload)
            except FileExistsError:
                try:
                    with open(lock_path, "r") as lf:
                        parts = lf.read().strip().split(":")
                    holder_pid = int(parts[0]) if parts and parts[0].isdigit() else 0
                    holder_fp = parts[1] if len(parts) > 1 else ""
                    try:
                        created_at = float(parts[-1])
                    except (ValueError, IndexError):
                        created_at = 0.0

                    age = time.time() - created_at if created_at else 0.0
                    if age > LLM_STALE_LOCK_SEC or not _pid_alive(holder_pid, holder_fp):
                        stale = f"{lock_path}.stale.{os.getpid()}.{int(time.time())}"
                        try:
                            os.rename(lock_path, stale)
                            try:
                                os.remove(stale)
                            except OSError:
                                pass
                        except OSError:
                            pass
                except Exception:
                    pass
            except Exception:
                pass
        time.sleep(0.3)

    return None


def _release_llm_slot(lease):
    if not isinstance(lease, tuple) or len(lease) != 2:
        return
    lock_path, owned_payload = lease
    if not isinstance(lock_path, str) or not isinstance(owned_payload, bytes):
        return
    try:
        with open(lock_path, "rb") as lock_file:
            current_payload = lock_file.read(4096)
        if current_payload != owned_payload:
            return
        os.remove(lock_path)
    except Exception:
        pass


_LAST_USED_MODEL: list  = [None]   # [0] = last model name generate() used

_CLIENT_CACHE: dict = {}           # (host, timeout) -> ollama.Client
_CLIENT_CACHE_LOCK = threading.Lock()


def _get_ollama_client(host: str, timeout: float):
    """Return a pooled ollama.Client for (host, timeout), creating once."""
    key = (host, timeout)
    client = _CLIENT_CACHE.get(key)
    if client is not None:
        return client
    with _CLIENT_CACHE_LOCK:
        client = _CLIENT_CACHE.get(key)
        if client is None:
            client = ollama.Client(host=host, timeout=timeout)
            _CLIENT_CACHE[key] = client
        return client


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
        model_just_changed = (prev_model is not None and prev_model != model)
        if model_just_changed:
            log_event(
                f"LLM model changed: {prev_model}  {model} "
                f" first call will be slower (cold-load)", "INFO"
            )
        _LAST_USED_MODEL[0] = model

        # Use a longer timeout for the first call after a model switch
        # (cold-load can take 15-60s depending on model size and disk speed).
        effective_timeout = (
            max(LLM_INFERENCE_TIMEOUT_SEC, 180.0)
            if model_just_changed
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
    for model in models:
        if not isinstance(model, dict):
            continue
        name = model.get("name")
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return names


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
            r = requests.get(f"{base_url}/api/tags", timeout=PING_TIMEOUT_SEC)
            if r.status_code == 200:
                installed = _model_names_from_payload(r.json())
                break
        except Exception:
            installed = None
        if attempt < PING_MAX_FAILURES - 1:
            time.sleep(1.0)
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
        r2 = requests.get(f"{base_url}/api/ps", timeout=PING_TIMEOUT_SEC)
        if r2.status_code == 200:
            loaded = _model_names_from_payload(r2.json())
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
    while True:
        if not first:
            time.sleep(PING_INTERVAL_SEC)
        first = False
        try:
            new_status = _do_one_ping()
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
    global _PING_STARTED
    if _PING_STARTED:
        return
    with _PING_LOCK:
        if _PING_STARTED:
            return
        # Non-blocking: do NOT ping inline (that blocks the caller up to
        # ~11s when Ollama is down). Leave state at its UNKNOWN default
        # (available=None  treated as unavailable) and let the background
        # ping thread run the first check and emit the startup log.
        t = threading.Thread(target=_ping_loop, daemon=True, name="LLMPing")
        t.start()
        _PING_STARTED = True


def llm_available() -> bool:
    """True if the configured model is usable (installed + daemon up).

    MODEL_COLD counts as available  Ollama will load it on demand.
    MODEL_MISSING and DAEMON_DOWN return False  keyword fallback.
    """
    _ensure_ping_thread_started()
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

    ``resp`` is the dict (or str) from generate_with_timeout. A trailing
    'CHALLENGE: STRONG' line means skip the trade; an empty answer is treated
    like the error path (honours _BULL_BEAR_FAIL_CLOSED)."""
    text = ""
    if isinstance(resp, dict):
        candidate = resp.get("response")
        if isinstance(candidate, str):
            text = candidate
    elif isinstance(resp, str):
        text = resp
    verdict = text.upper()
    if not verdict.strip():
        return "OVERRIDE_WAIT" if _BULL_BEAR_FAIL_CLOSED else "PROCEED"
    lines = [ln for ln in verdict.splitlines() if ln.strip()]
    tail = lines[-1] if lines else verdict
    if "STRONG" in tail:
        log_event(f"[Bull/Bear] {label}: STRONG challenge  OVERRIDE_WAIT", "INFO")
        return "OVERRIDE_WAIT"
    return "PROCEED"


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
