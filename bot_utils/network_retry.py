"""
bot_utils/network_retry.py  Generic network-error retry with exponential backoff.

Wraps an operation so a transient network glitch (VPN drop, brief API outage,
DNS timeout) during e.g. an emergency close is retried instead of giving up
after one attempt. Permanent errors (precision, insufficient balance, symbol
not listed) are detected and abort immediately  patterns are word-bounded and
phrase-anchored so transient errors ("invalid timestamp", "leverage update in
progress") aren't mistaken for permanent ones.
"""
from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable, Optional


# Word-bounded, phrase-anchored patterns  each tied to a real permanent
# condition. Bare words ("invalid", "leverage", "below") would also match
# transient errors.
_PERMANENT_PATTERN_STRINGS = (
    r"insufficient\s+(?:balance|funds|margin)",
    r"not\s+enough\s+(?:balance|funds|margin)",
    # MEXC "Oversold" / code 30005 = selling more than held  retrying never
    # helps; treat as permanent so reconciliation can clean up the orphan
    # instead of hammering the API.
    r"oversold",
    r"\b30005\b",
    r"quantity\s+(?:too\s+)?(?:large|exceeds)",
    r"balance\s+(?:not\s+enough|insufficient)",
    r"invalid\s+symbol",
    r"symbol\s+not\s+(?:found|exist|listed)",
    r"market\s+not\s+(?:found|exist)",
    r"order\s+does\s+not\s+exist",
    r"position\s+(?:does\s+not\s+exist|not\s+exist|not\s+found)",
    # MEXC code 2009 "Position is nonexistent or closed" (one word "nonexistent",
    # not matched above). When the user closes a position manually, treat as
    # permanent so reconciliation removes it from local state instead of
    # retrying the close forever.
    r"position\s+is\s+nonexistent",
    r"nonexistent\s+or\s+closed",
    r"\b2009\b",
    r"no\s+(?:open\s+)?position",
    r"reduce[-_\s]?only",
    # Narrow to genuine API-credential/permission failures (permanent); a bare
    # ``forbidden`` would also match transient Cloudflare/WAF 403s.
    r"invalid\s+api[\s_-]?key",
    r"api[\s_-]?key[^\n]{0,40}(?:permission|forbidden|disabled|expired)",
    r"permission\s+denied",
    r"trading\s+suspended",
    r"trading\s+is\s+halted",
    r"too\s+many\s+decimals",
    r"amount\s+precision",
    r"lot\s+size",
    r"min\s+notional",
    r"below\s+min(?:imum)?\s+(?:notional|amount|size)",
    r"margin\s+mode\s+(?:already|not\s+allowed)",
    # MEXC code 6026  account-level block (KYC/region/risk-control) that
    # retrying cannot resolve; treat as permanent to stop the 3x retry spam.
    r"\b6026\b",
    r"risk\s+control\s+verification",
    r"disabled_contract_open_position",
    # MEXC code 8823  pair being DELISTED, "new positions cannot be opened":
    # an exchange-side block retrying can't fix; treat as permanent  skip fast.
    r"\b8823\b",
    r"will\s+be\s+delisted",
    r"new\s+positions\s+cannot\s+be\s+opened",
)
_PERMANENT_PATTERNS = re.compile(
    "|".join(f"(?:{p})" for p in _PERMANENT_PATTERN_STRINGS),
    re.IGNORECASE
)


def is_permanent_error(exc: BaseException) -> bool:
    """Return True if the exception text matches a permanent failure pattern.

    Permanent failures (precision, insufficient balance, symbol not listed)
    will not be fixed by retrying  caller should abort immediately.
    """
    return bool(_PERMANENT_PATTERNS.search(str(exc)))


# Rate-limit detection for a dedicated longer backoff. A genuine 429 / DDoS
# response should NOT burn the 3 normal attempts in 3.5s (0.512), especially
# during an emergency close  rate-limit errors get 4s8s16s (capped 30s).
try:
    import ccxt as _ccxt
    _RATE_LIMIT_EXC = (_ccxt.RateLimitExceeded, _ccxt.DDoSProtection)
except Exception:
    _RATE_LIMIT_EXC = ()


def is_rate_limited(exc: BaseException) -> bool:
    """True if the exception is an exchange rate-limit / DDoS response.

    Patterns matched:
    HTTP 429 / "Too Many Requests" (standard)
    "rate limit" / "ratelimit" (generic)
    MEXC: code 510 / "Requests are too frequent" / "too frequent"
      (MEXC-specific  different from the standard 429 wording, so without
      this branch the retry wrapper treats it as a hard error and gives up)
    """
    if _RATE_LIMIT_EXC and isinstance(exc, _RATE_LIMIT_EXC):
        return True
    s = str(exc).lower()
    return ("429" in s or "too many requests" in s
            or "rate limit" in s or "ratelimit" in s
            or "too frequent" in s
            or '"code":510' in s or "code 510" in s)


def is_server_error(exc: BaseException) -> bool:
    """True for exchange-side 5xx / gateway errors (502/503/504).

    A sustained 5xx storm means the exchange is overloaded; the normal
    0.512s backoff would burn all 3 attempts in 3.5s and hammer a struggling
    endpoint, so these get the same longer, capped backoff as a 429.
    """
    s = str(exc).lower()
    return ("502" in s or "bad gateway" in s
            or "503" in s or "service unavailable" in s
            or "504" in s or "gateway time" in s)


def is_transient_network(exc: BaseException) -> bool:
    """True for transient connectivity blips: DNS failure, SSL EOF, read/
    connect timeout, connection reset. These are NOT code bugs  they happen
    when the internet/VPN/exchange briefly drops (common in restricted
    networks). Callers should log these compactly (one line) rather than
    dumping a full traceback for every hiccup, and simply retry next cycle.
    """
    s = str(exc).lower()
    needles = (
        "getaddrinfo failed", "failed to resolve", "name resolution",
        "nameresolutionerror", "temporary failure in name resolution",
        "ssl", "tls/ssl connection has been closed", "eof occurred",
        "read timed out", "read timeout", "connection timed out",
        "connect timeout", "max retries exceeded", "connection reset",
        "connection aborted", "connection refused", "remote end closed",
        "requesttimeout", "networkerror", "ddosprotection",
    )
    return any(n in s for n in needles)


def with_network_retry(operation: Callable[[], Any],
                         action_label: str = "operation",
                         max_attempts: int = 3,
                         base_delay: float = 0.5,
                         shutdown_event: Optional[threading.Event] = None,
                         log_event: Optional[Callable] = None) -> Any:
    """Execute ``operation()`` with exponential-backoff retry on transient errors.

    Returns whatever ``operation`` returns on the first success. Raises the
    last exception if all attempts fail OR if a permanent error is detected
    on any attempt.

    Backoff schedule: 0.5s  1.0s  2.0s (configurable via ``base_delay``).
    """
    last_err: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as e:
            last_err = e
            if is_permanent_error(e):
                if log_event:
                    try:
                        log_event(
                            f"{action_label}  permanent error, no retry: {e}",
                            "WARN"
                        )
                    except Exception:
                        pass
                raise
            if attempt >= max_attempts:
                if log_event:
                    try:
                        log_event(
                            f"{action_label}  failed after {attempt}/{max_attempts} "
                            f"attempts: {e}",
                            "WARN"
                        )
                    except Exception:
                        pass
                break
            # Rate-limit / 5xx errors get a longer backoff (4s8s16s, capped
            # 30s) instead of 0.512s  burning 3 attempts in 3.5s on a 429
            # risks an IP ban and can abort an emergency close prematurely.
            if is_rate_limited(e) or is_server_error(e):
                # 429/DDoS AND 5xx gateway errors both indicate the endpoint
                # is overloaded  back off longer (4s8s16s, capped 30s)
                # instead of hammering it 3 in 3.5s.
                wait_s = min(30.0, base_delay * (2 ** (attempt - 1)) * 8)
            else:
                wait_s = base_delay * (2 ** (attempt - 1))
            if log_event:
                try:
                    log_event(
                        f"{action_label}  attempt {attempt}/{max_attempts} "
                        f"failed ({type(e).__name__}), retry in {wait_s:.1f}s: "
                        f"{str(e)[:120]}",
                        "INFO"
                    )
                except Exception:
                    pass
            if shutdown_event is not None:
                if shutdown_event.wait(timeout=wait_s):
                    if log_event:
                        try:
                            log_event(
                                f"{action_label}  shutdown during retry, abort",
                                "WARN"
                            )
                        except Exception:
                            pass
                    raise
            else:
                time.sleep(wait_s)

    if last_err is not None:
        raise last_err
    raise RuntimeError(f"{action_label}: retry loop exited without success or error")
