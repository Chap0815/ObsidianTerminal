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

from collections import Counter
from datetime import datetime
import re
import time

import tkinter as tk

from launcher.config.settings import COLORS


#  Severity 

_LEVEL_NAMES = (
    r"INFO|OK|WARN|WARNING|ERROR|CRITICAL|FATAL|START|SCAN|WAIT|"
    r"BUY|SELL|WIN|LOSS"
)
_EMBEDDED_LEVEL_RE = re.compile(
    rf"^\[(?P<after_time>\d{{2}}:\d{{2}}:\d{{2}})\]\s+"
    rf"(?P<after_level>{_LEVEL_NAMES})\s+",
    re.IGNORECASE,
)
_LEADING_LEVEL_RE = re.compile(
    rf"^(?P<before_level>{_LEVEL_NAMES})\s+"
    rf"\[(?P<before_time>\d{{2}}:\d{{2}}:\d{{2}})\]\s+",
    re.IGNORECASE,
)
_BARE_LEVEL_RE = re.compile(
    rf"^(?P<bare_level>{_LEVEL_NAMES})\s+",
)

_EXPLICIT_LEVEL_RE = re.compile(
    r"^\s*(?:"
    r"\[\d{2}:\d{2}:\d{2}\]\s+"
    rf"(?P<after>{_LEVEL_NAMES})"
    r"|"
    rf"(?P<before>{_LEVEL_NAMES})"
    r"\s+\[\d{2}:\d{2}:\d{2}\]"
    r"|"
    rf"(?P<bare>{_LEVEL_NAMES})"
    r")\b",
    re.IGNORECASE,
)


_DISPLAY_SEPARATOR_RE = re.compile(r"^\s*[-=_*#]{8,}\s*$")
_DISPLAY_HEARTBEAT_RE = re.compile(
    r"(?:\bheartbeat\b.*\bopen:\s*\d+|"
    r"\bopen trades?:\s*.*\b(?:balance|next scan):)",
    re.IGNORECASE,
)
_DISPLAY_MONITOR_RE = re.compile(
    r"\b(?:monitor(?:ing)?(?:[- ]?tick| idle)?|tick\s*#\d+)\b",
    re.IGNORECASE,
)
_DISPLAY_ANALYSIS_RE = re.compile(
    r"\b(?:analyz(?:e|ing|is|ed)|analys(?:e|ing|is|iere|iert)|"
    r"signal check|"
    r"(?:ai|ki|llm|keyword fallback)\s+(?:says?|sagt)\s+(?:wait|hold)|"
    r"result:\s*(?:wait|hold))\b",
    re.IGNORECASE,
)
_DISPLAY_SKIP_RE = re.compile(
    r"\b(?:skip(?:ped|ping)?|blacklist(?:ed)?|cooldown(?: active)?|"
    r"insufficient[^|]{0,80}(?:history|data|bars?|candles?)|"
    r"has no (?:(?:15m|1h|4h)\s+)?candle|"
    r"(?:held|claimed) by another bot|"
    r"(?:funding filter|quality gate) excluded|"
    r"\b(?:long|short|buy|entry)?\s*blocked\b|"
    r"\bblocked (?:entry|by)\b)\b",
    re.IGNORECASE,
)
_DISPLAY_IMPORTANT_STATE_RE = re.compile(
    r"\b(?:safe[_ -]?mode|kill[- ]?switch|circuit breaker|"
    r"pause(?:d)?|pausiert|risk gate unavailable|daily[- ]loss|"
    r"max(?:imum|\.)? trades?|bad hour|market[- ]filter|markt-filter|"
    r"api budget exhausted|shutdown|stopp(?:ed|ing)|started|running|ready|"
    r"recovered|connection established)\b",
    re.IGNORECASE,
)


class LiveLogDisplayFilter:
    """Condense routine bot stdout without changing durable audit logs.

    This filter is deliberately display-only. Errors, critical messages, trade
    events and state transitions always pass through. High-volume analysis,
    skip and monitor chatter is counted and rendered as a periodic summary.
    The caller can bypass the policy with ``detailed=True`` at any time.
    """

    _CATEGORY_LABELS = {
        "analysis": "analysis",
        "skipped": "skipped",
        "monitor": "monitor",
        "scan": "scan",
        "wait": "wait",
        "heartbeat": "status",
        "formatting": "formatting",
        "repeated_warning": "repeated warnings",
        "repeated_state": "unchanged state",
    }
    _NEVER_FILTER = frozenset({
        "error", "critical", "buy", "sell", "win", "loss", "ok",
    })

    def __init__(
        self,
        *,
        summary_interval_seconds: float = 20.0,
        warning_repeat_window_seconds: float = 120.0,
        state_repeat_window_seconds: float = 300.0,
        max_warning_fingerprints: int = 512,
    ) -> None:
        self.summary_interval_seconds = max(
            1.0, float(summary_interval_seconds)
        )
        self.warning_repeat_window_seconds = max(
            1.0, float(warning_repeat_window_seconds)
        )
        self.state_repeat_window_seconds = max(
            1.0, float(state_repeat_window_seconds)
        )
        self.max_warning_fingerprints = max(
            16, int(max_warning_fingerprints)
        )
        self._pending: Counter[str] = Counter()
        self._pending_first_at: float | None = None
        self._pending_last_at: float | None = None
        self._last_summary_at: float | None = None
        self._warning_seen_at: dict[str, float] = {}
        self._state_seen_at: dict[str, float] = {}

    @staticmethod
    def _fingerprint(line: str) -> str:
        text = str(line or "")
        text = _EMBEDDED_LEVEL_RE.sub("", text, count=1)
        text = _LEADING_LEVEL_RE.sub("", text, count=1)
        text = _BARE_LEVEL_RE.sub("", text, count=1)
        return re.sub(r"\s+", " ", text).strip().casefold()

    @staticmethod
    def _routine_category(line: str, severity: str) -> str | None:
        text = str(line or "")
        explicit = _explicit_level(text)
        if _DISPLAY_SEPARATOR_RE.fullmatch(text):
            return "formatting"
        if _DISPLAY_HEARTBEAT_RE.search(text):
            return "heartbeat"
        if _DISPLAY_IMPORTANT_STATE_RE.search(text):
            return None
        if severity == "monitor" or _DISPLAY_MONITOR_RE.search(text):
            return "monitor"
        if _DISPLAY_ANALYSIS_RE.search(text):
            return "analysis"
        if _DISPLAY_SKIP_RE.search(text):
            return "skipped"
        if explicit == "SCAN":
            if re.search(r"\b(?:complete|completed|opened|failed)\b", text,
                         re.IGNORECASE):
                return None
            return "scan"
        if explicit == "WAIT":
            return "wait"
        return None

    def _summary(self) -> str | None:
        total = sum(self._pending.values())
        if total <= 0:
            return None
        ordered = sorted(
            self._pending.items(), key=lambda item: (-item[1], item[0])
        )
        details = ", ".join(
            f"{self._CATEGORY_LABELS.get(category, category)}: {count}"
            for category, count in ordered
        )
        first_at = self._pending_first_at
        last_at = self._pending_last_at
        self._pending.clear()
        self._pending_first_at = None
        self._pending_last_at = None
        noun = "message" if total == 1 else "messages"
        duration = 0.0
        if first_at is not None and last_at is not None:
            duration = max(0.0, last_at - first_at)
        return (
            f"INFO [Activity] {total} routine {noun} condensed "
            f"over {duration:.0f}s ({details}). "
            "Enable Details for raw output."
        )

    def _summary_if_due(self, current: float) -> tuple[str, ...]:
        if not self._pending:
            return ()
        if self._last_summary_at is None:
            self._last_summary_at = current
            return ()
        if current - self._last_summary_at < self.summary_interval_seconds:
            return ()
        summary = self._summary()
        self._last_summary_at = current
        return (summary,) if summary else ()

    def poll_due(self, *, now: float | None = None) -> tuple[str, ...]:
        """Emit a due summary even when no further bot line arrives."""
        current = time.monotonic() if now is None else float(now)
        return self._summary_if_due(current)

    def _remember_warning(self, fingerprint: str, current: float) -> bool:
        previous = self._warning_seen_at.get(fingerprint)
        cutoff = current - self.warning_repeat_window_seconds
        if previous is not None and current - previous < (
            self.warning_repeat_window_seconds
        ):
            return True
        self._warning_seen_at[fingerprint] = current
        if len(self._warning_seen_at) > self.max_warning_fingerprints:
            self._warning_seen_at = {
                key: seen
                for key, seen in self._warning_seen_at.items()
                if seen >= cutoff
            }
            if len(self._warning_seen_at) > self.max_warning_fingerprints:
                newest = sorted(
                    self._warning_seen_at.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:self.max_warning_fingerprints]
                self._warning_seen_at = dict(newest)
        return False

    def _remember_state(self, fingerprint: str, current: float) -> bool:
        previous = self._state_seen_at.get(fingerprint)
        if previous is not None and current - previous < (
            self.state_repeat_window_seconds
        ):
            return True
        self._state_seen_at[fingerprint] = current
        if len(self._state_seen_at) > self.max_warning_fingerprints:
            cutoff = current - self.state_repeat_window_seconds
            self._state_seen_at = {
                key: seen
                for key, seen in self._state_seen_at.items()
                if seen >= cutoff
            }
            if len(self._state_seen_at) > self.max_warning_fingerprints:
                newest = sorted(
                    self._state_seen_at.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:self.max_warning_fingerprints]
                self._state_seen_at = dict(newest)
        return False

    def _suppress(self, category: str, current: float) -> tuple[str, ...]:
        self._pending[category] += 1
        if self._pending_first_at is None:
            self._pending_first_at = current
        self._pending_last_at = current
        return self._summary_if_due(current)

    def push(
        self,
        line: str,
        severity: str,
        *,
        detailed: bool = False,
        now: float | None = None,
    ) -> tuple[str, ...]:
        """Return zero or more user-visible lines for one raw stdout line."""
        current = time.monotonic() if now is None else float(now)
        text = str(line or "")
        if detailed:
            summary = self._summary()
            self._last_summary_at = current
            return ((summary,) if summary else ()) + (text,)

        normalized_severity = str(severity or "info").lower()
        if (
            normalized_severity not in self._NEVER_FILTER | {"warn"}
            and _DISPLAY_IMPORTANT_STATE_RE.search(text)
        ):
            fingerprint = self._fingerprint(text)
            if fingerprint:
                state_seen_before = fingerprint in self._state_seen_at
                if self._remember_state(fingerprint, current):
                    return self._suppress("repeated_state", current)
                if state_seen_before:
                    output = list(self.flush())
                    self._last_summary_at = current
                    output.append(text)
                    return tuple(output)
        category = None
        if normalized_severity not in self._NEVER_FILTER | {"warn"}:
            category = self._routine_category(text, normalized_severity)
        if category is not None:
            return self._suppress(category, current)

        if normalized_severity == "warn":
            fingerprint = self._fingerprint(text)
            if fingerprint:
                warning_seen_before = fingerprint in self._warning_seen_at
                if self._remember_warning(fingerprint, current):
                    return self._suppress("repeated_warning", current)
                if warning_seen_before:
                    output = list(self.flush())
                    self._last_summary_at = current
                    output.append(text)
                    return tuple(output)

        output = list(self._summary_if_due(current))
        output.append(text)
        return tuple(output)

    def flush(self) -> tuple[str, ...]:
        """Render the currently pending condensation summary, if any."""
        summary = self._summary()
        return (summary,) if summary else ()

    def reset(self) -> None:
        self._pending.clear()
        self._pending_first_at = None
        self._pending_last_at = None
        self._warning_seen_at.clear()
        self._state_seen_at.clear()
        self._last_summary_at = None


def _explicit_level(line: str) -> str | None:
    """Return a producer-supplied level instead of guessing from wording."""
    match = _EXPLICIT_LEVEL_RE.match(str(line or ""))
    if match is None:
        return None
    return str(
        match.group("after") or match.group("before") or match.group("bare")
    ).upper()


def normalize_log_message_for_display(msg: str) -> str:
    """Strip redundant producer prefixes and keep one compact UI line."""
    text = str(msg or "")
    if _EMBEDDED_LEVEL_RE.match(text):
        text = _EMBEDDED_LEVEL_RE.sub("", text, count=1)
    elif _LEADING_LEVEL_RE.match(text):
        text = _LEADING_LEVEL_RE.sub("", text, count=1)
    else:
        text = _BARE_LEVEL_RE.sub("", text, count=1)
    return re.sub(r"\s*[\r\n]+\s*", " | ", text).strip()


def _source_timestamp(msg: str) -> str | None:
    text = str(msg or "")
    match = _EMBEDDED_LEVEL_RE.match(text) or _LEADING_LEVEL_RE.match(text)
    if match is None:
        return None
    return match.groupdict().get("after_time") or match.groupdict().get("before_time")


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

    # A level explicitly supplied by the bot is authoritative. In particular,
    # wording such as "retry failed" must not turn an intentional WARN into a
    # red ERROR badge, while an OK recovery containing "Timeout" stays green.
    explicit = _explicit_level(line)
    if explicit == "OK":
        return "ok"
    if explicit in ("WARN", "WARNING"):
        return "warn"
    if explicit in ("ERROR", "CRITICAL", "FATAL"):
        if explicit in ("CRITICAL", "FATAL"):
            return "critical"
        return "error"
    if explicit in ("BUY", "SELL", "WIN", "LOSS"):
        return explicit.lower()

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
    if sev == "critical":
        return ("CRITICAL", "badge_error", "error")
    if sev == "ok":
        return ("OK", "badge_ok", "win")
    if sev == "warn":
        return ("WARN", "badge_warn", "warn")
    if sev == "monitor":
        return ("MONITOR", "badge_monitor", "monitor")
    if sev == "ki":
        return ("KI", "badge_ki", "system")
    if sev == "win":
        return ("$ TRADE", "badge_trade", "win")
    if sev == "loss":
        return ("LOSS", "badge_loss", "error")
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
    ts = _source_timestamp(msg) or datetime.now().strftime("%H:%M:%S")
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
