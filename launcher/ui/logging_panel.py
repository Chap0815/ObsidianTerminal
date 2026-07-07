"""
Logging-panel helpers extracted from :class:`ObsidianApp`.

Two responsibilities:

* Classifying raw bot stdout lines into severity buckets so they get the
  right colour / badge in the UI (:func:`classify_severity`)
* Rendering one log line into a per-card ``tk.Text`` widget with the
  appropriate timestamp + badge + tag (:func:`write_log_to_box` and the
  small helpers around it)
* Tracking which AI mode (LLM vs keyword fallback) each bot is currently in
  (:func:`detect_ai_mode_from_log`, :func:`update_ai_badge`)

All functions are pure-ish: they read from / write to widgets the caller
hands in, but keep no global state.
"""

from __future__ import annotations

from datetime import datetime
import re

import tkinter as tk

from launcher.config.settings import COLORS


#  Severity 

_EMBEDDED_LEVEL_RE = re.compile(
    r"^(\[\d{2}:\d{2}:\d{2}\]\s+)"
    r"(INFO|WARN|WARNING|ERROR|START|SCAN|WAIT|BUY|SELL|WIN|LOSS)\s+",
    re.IGNORECASE,
)


def normalize_log_message_for_display(msg: str) -> str:
    """Remove redundant bot-side level labels before rendering in the card."""
    text = str(msg or "")
    return _EMBEDDED_LEVEL_RE.sub(r"\1", text, count=1)


def is_benign_info_line(line: str) -> bool:
    """True for expected fallback/degraded lines that should not alarm users."""
    lower = line.lower()
    if ("set_margin_mode(" in lower and "keyerror" in lower
            and "order params will carry" in lower):
        return True
    if ("margin-mode precheck unavailable" in lower
            and "keyerror" in lower
            and "per-order marginmode/leverage params" in lower):
        return True
    if ("insufficient" in lower and "closed bars" in lower
            and "symbol skipped" in lower):
        return True
    if "llm failed" in lower and "keyword fallback" in lower:
        return True
    if "llm unavailable" in lower and "keyword fallback" in lower:
        return True
    if "news module loaded:" in lower:
        return True
    return False


def is_known_warn_line(line: str) -> bool:
    """True for degraded-but-expected WARN lines that may contain 'failed'."""
    upper = line.upper()
    lower = line.lower()
    if not any(k in upper for k in ("WARN", "WARNING")):
        return False
    if ("telegram send failure" in lower
            or "telegram api error" in lower
            or "telegram unavailable" in lower):
        return True
    if any(k in upper for k in ("ERROR", "EXCEPTION", "TRACEBACK")):
        return False
    if "llm inference failed" in lower:
        return True
    if "ws feed init failed" in lower and "rest only" in lower:
        return True
    return False


def classify_severity(line: str) -> str:
    """Map a raw bot stdout line to one of the severity tags used by the log
    box. Detection order matters  see comments below."""
    upper = line.upper()
    lower = line.lower()

    if is_benign_info_line(line):
        return "info"
    if "margin-mode precheck unavailable" in lower and "WARN" in upper:
        return "warn"

    # 0. Known benign API errors  WARN instead of ERROR
    #    These patterns are expected and not actionable:
    #  "Indicator error XYZ 15m/1h/4h"  symbol has no candle data
    #  "Ticker fetch failed"  brief Bitget API hiccup
    #  "BTC trend fetch failed"  BTC data briefly unavailable
    #  "Market phase analysis failed"  regime briefly unavailable
    #  "[cache] ... fetch failed, serving stale"  cache kicks in, fine
    #  "load_markets attempt X/3 failed"  connection retry (not final)
    #  "Symbol XYZ has no ... candle"  hard-cached symbol
    #  "Rate-limit on"  API budget temporarily out
    #  "Skipping for"  follow-up after a data gap
    _benign = (
        "indicator error ",
        "ticker fetch failed",
        "btc trend fetch failed",
        "btc data unavailable",
        "market phase analysis failed",
        "serving stale value",
        "[cache]",
        "load_markets attempt ",  # retries (1/3, 2/3)  not final
        "has no 15m candle", "has no 1h candle",
        "has no 4h candle",  "has no candle",
        "rate-limit on ",
        "skipping for 1h", "skipping for 5min",
        "skipping 1h",     "skipping 5min",
    )
    if any(k in lower for k in _benign):
        return "warn"

    if is_known_warn_line(line):
        return "warn"

    # 1. Real errors (highest priority)
    if any(k in upper for k in ("ERROR", "EXCEPTION", "FAILED", "FEHLER",
                                "FEHLGESCHLAGEN", "TRACEBACK")):
        return "error"

    # 2. Warnings
    # NOTE: "LIQUIDATION" intentionally omitted  it appears in normal AI
    # analysis responses ("liquidation levels at X%") and would cause every
    # futures analysis to be badged as WARN.
    if any(k in upper for k in ("WARN", "WARNUNG", "TIMEOUT", "PAUSIERT",
                                "PAUSED", "BLACKLIST")):
        return "warn"

    # 3. AI activity  MUST come before monitor/info, otherwise lines like
    #    "X: KI sagt WAIT" / "X: AI says WAIT" get classified as info
    ai_markers = (
        "ki:", "ai:",
        "ki sagt", "ki says", "ai says", "ai sagt",
        "result: buy", "result: wait", "result: long", "result: short",
        "result: hold", "result: sell",
        "deepseek", "ollama",
        "analysiere ", "analyzing ",
        "thinking", "denken",
        "llm ",
        "sentiment-analyse", "sentiment analysis",
    )
    if any(k in lower for k in ai_markers):
        return "ki"

    # 4. Real trade events (open / close)
    trade_markers = (
        "kauf", "bought ", "long opened", "long eroeffnet",
        "verkauf", "sold ", "short opened", "short eroeffnet",
        "take-profit", "stop-loss triggered",
        "position closed", "trade closed", "position geschlossen",
        "emergency close ", "break-even hit", "break-even erreicht",
    )
    if any(k in lower for k in trade_markers):
        return "win"

    # 5. Monitor  only real position-monitoring ticks, not heartbeats
    monitor_markers = (
        "monitoring ", "monitor idle",
        "monitor-tick", "tick #",
    )
    if any(k in lower for k in monitor_markers):
        return "monitor"

    # 6. Standard lifecycle (heartbeat, startups, status)  info
    return "info"


def _severity_to_badge(severity: str, msg: str = "") -> tuple[str, str, str]:
    """Map ``(severity, msg)`` to ``(badge_text, badge_tag, msg_tag)``.

    Detection order:
      1. Explicit severity (error/warn/win/buy/sell)  clear badges
      2. Content-based fallback for system/info:
         - KI / AI / LLM / Decision / Brain /  KI badge
         - WIN / Profit / SELL  / Bought  TRADE badge
         - default  INFO badge
    """
    sev = severity.lower()
    m = msg.lower()

    # Explicit severities
    if sev == "error":
        return ("ERROR", "badge_error", "error")
    if sev == "warn":
        return ("WARN", "badge_warn", "warn")
    if sev == "monitor":
        return ("MONITOR", "badge_monitor", "monitor")
    if sev == "ki":
        return ("KI", "badge_ki", "system")
    if sev == "win":
        return ("$ TRADE", "badge_trade", "win")
    if sev in ("buy", "sell"):
        return ("$ TRADE", "badge_trade", sev)

    if is_benign_info_line(msg):
        return ("INFO", "badge_info", "info")

    # Content-based (for system/info)
    ai_kws = ("ki:", "ai:", "llm", "deepseek", "ollama", "thinking",
              "decision", "prompt", "result:",
              "steelman:", "rationale:")  # Steelman  always KI badge
    if any(kw in m for kw in ai_kws):
        return ("KI", "badge_ki", "system")

    trade_kws = ("partial", "stop-loss", "trailing", "profit",
                 "bought", "sold", "long opened", "short opened",
                 "closed", "break-even", "win")
    if any(kw in m for kw in trade_kws):
        return ("$ TRADE", "badge_trade", "win")

    # Default  INFO
    return ("INFO", "badge_info", "info")


#  Log writer 

def write_log_to_box(box: tk.Text, auto_var, severity: str, msg: str) -> None:
    """Append ``msg`` to a per-card log box with timestamp + badge.

    The log is capped at ~2200 lines; once exceeded, the oldest 200 are
    trimmed to keep the Tk text widget from blowing up to hundreds of MB
    during long bot runs.
    """
    ts = datetime.now().strftime("%H:%M:%S")
    msg = normalize_log_message_for_display(msg)
    badge_text, badge_tag, msg_tag = _severity_to_badge(severity, msg)

    box.config(state="normal")
    # Cap log size to prevent the Tk text widget from growing unbounded.
    try:
        lines = int(box.index("end-1c").split(".")[0])
        if lines > 2200:
            box.delete("1.0", f"{lines - 2000}.0")
    except (ValueError, tk.TclError):
        pass
    box.insert("end", f"{ts}  ", "time")
    # Badge with surrounding padding spaces (Tk has no "real" padding for
    # tags, so we simulate it with spaces).
    box.insert("end", f" {badge_text} ", badge_tag)
    box.insert("end", f"  {msg}\n", msg_tag)
    if auto_var.get():
        box.see("end")
    box.config(state="disabled")


def log_to_card(card: dict, severity: str, msg: str) -> None:
    """Append a message to a bot card's log. Thin convenience wrapper that
    pulls the log box + auto-scroll var out of the card dict."""
    write_log_to_box(card["log_box"], card["auto_var"], severity, msg)


#  AI mode detection / badge 

def detect_ai_mode_from_log(line: str) -> str | None:
    """Look at one log line and try to determine whether the last AI
    decision was made via the LLM or the keyword fallback.

    Returns ``'llm'``, ``'keyword'``, or ``None`` if the line carries no
    such signal.
    """
    lower = line.lower()
    if "keyword fallback" in lower or "[keyword fallback" in lower:
        return "keyword"
    if "llm offline" in lower or "ollama offline" in lower:
        return "keyword"
    if "analysiere " in lower or "analyzing " in lower:
        # LLM is about to be called  no fallback detected yet
        return None
    # A ``RESULT:`` line WITHOUT a "fallback" marker means the LLM answered
    if "result: buy" in lower or "result: wait" in lower or \
       "result: long" in lower or "result: short" in lower:
        if "fallback" not in lower:
            return "llm"
    return None


def update_ai_badge(card: dict, running: bool, global_llm_online: bool,
                    now_ts: float, uses_llm: bool = True) -> None:
    """Update one bot card's AI-mode pill (LLM / Keyword).

    Logic:
      * Bot not running  ``"AI: "`` (neutral)
      * Bot running and made a decision recently (< 5 min)  use the
        detected mode from the log
      * Bot running but no decision seen yet  reflect the global LLM status
    """
    var = card["ai_badge_var"]
    lbl = card["ai_badge"]
    if not running:
        var.set("AI: -")
        lbl.configure(text_color=COLORS["text_subtle"])
        return

    if not uses_llm:
        var.set("Signal")
        lbl.configure(text_color=COLORS["text_muted"])
        return

    mode = card.get("ai_mode", "unknown")
    ts   = card.get("ai_mode_ts", 0)
    recent = (now_ts - ts) < 300  # 5 minutes

    if mode == "llm" and recent:
        var.set("AI: LLM")
        lbl.configure(text_color=COLORS["success"])
    elif mode == "keyword" and recent:
        var.set("AI: Keyword")
        lbl.configure(text_color=COLORS["warning"])
    else:
        # No recent decision  fall back to the global Ollama status
        if global_llm_online:
            var.set("AI: LLM*")  # * = not yet confirmed
            lbl.configure(text_color=COLORS["balanced"])
        else:
            var.set("AI: Keyword")
            lbl.configure(text_color=COLORS["warning"])
