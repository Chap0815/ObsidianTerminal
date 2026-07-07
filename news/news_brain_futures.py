"""
news_brain_futures.py  News-Sentiment analysis for the FUTURES bot.

Three possible outputs: LONG, SHORT, WAIT. Shares the news/parse infrastructure
with news_brain_core; BEARISH_KEYWORDS come from news_keywords so all bots react
to the same vocabulary.
"""
from __future__ import annotations

from trading.market_filters import get_fear_greed
from news.llm_utils      import (llm_available, get_model_name,
                            futures_bull_bear_challenge, generate_with_timeout)
from news.news_brain_core import (
    load_prompt_template, render_prompt,
    get_latest_news, parse_confidence as _parse_conf,
    parse_last_result, strip_thinking, is_valid_symbol,
)
from news.news_keywords import BEARISH_KEYWORDS
from core.logger import log_event

_BOT_NAME = "FUTURES"

# Prompts live in PROJECT_ROOT/prompts/.
from core.paths import (
    PROMPT_FUTURES         as _PROMPT_FILE,
    PROMPT_FUTURES_DEFAULT as _DEFAULT_FILE,
)
_PROMPT_FILE  = str(_PROMPT_FILE)
_DEFAULT_FILE = str(_DEFAULT_FILE)

_FALLBACK_PROMPT = (
    "Futures trader at {leverage}x leverage. Symbol: {symbol}, "
    "price {price:.6f}, 24h {change:+.2f}%.\n"
    "RSI: {rsi_15m:.1f}/{rsi_1h:.1f}/{rsi_4h:.1f}. "
    "Funding: {funding_rate:+.4f}%. News: {news}.\n"
    "End with: RESULT: LONG, RESULT: SHORT, or RESULT: WAIT."
)


def _futures_keyword_fallback(symbol, news, change, rsi_1h):
    """Keyword fallback for futures  picks LONG / SHORT / WAIT.

    Uses the shared BEARISH_KEYWORDS vocabulary so a hack event triggers the
    same response across all bots."""
    news_lower = (news or "").lower()
    found_bearish = [kw for kw in BEARISH_KEYWORDS if kw in news_lower]

    if found_bearish:
        return (
            f"[Keyword Fallback] Bearish news detected: "
            f"{', '.join(found_bearish[:3])}.\n"
            f"RESULT: SHORT"
        )

    if change > 20 and rsi_1h > 75:
        return ("[Keyword Fallback] Large pump + overbought RSI  "
                "exhaustion risk.\nRESULT: WAIT")
    if change > 5 and rsi_1h < 65:
        return ("[Keyword Fallback] Healthy momentum + room in RSI.\n"
                "RESULT: LONG")
    return ("[Keyword Fallback] Mixed signals.\nRESULT: WAIT")


def analyze_sentiment(symbol, change, rsi_15m, rsi_1h, rsi_4h, news,
                      *,
                      price: float = 0.0,
                      funding_rate: float = 0.0,
                      leverage: int = 3,
                      market_regime: dict = None,
                      open_interest_usdt=None,
                      oi_change=None,
                      screener_direction: str = "",
                      **_ignored_kwargs):
    """Six positionals (symbol, change, rsi_15m, rsi_1h, rsi_4h, news) matching
    news_brain_spot.analyze_sentiment; everything else is keyword-only.

    ``screener_direction`` (LONG/SHORT from the technical scan: MACD + price
    momentum) is prepended to the prompt as a context line so the LLM treats
    the scan's directional bias as an additional data point when scoring both
    sides.
    """
    if not is_valid_symbol(symbol):
        return _futures_keyword_fallback(symbol, news, change, rsi_1h)
    if not llm_available():
        return _futures_keyword_fallback(symbol, news, change, rsi_1h)

    fg = get_fear_greed()
    if market_regime is None:
        market_regime = {"regime": "NEUTRAL", "btc_24h": 0.0, "btc_7d": 0.0}

    template = load_prompt_template(_PROMPT_FILE, _DEFAULT_FILE,
                                    fallback=_FALLBACK_PROMPT)

    try:
        from trading.risk_manager import get_reflection_context
        reflection = get_reflection_context(_BOT_NAME)
    except Exception:
        reflection = ""

    try:
        oi_change_pct = float(oi_change or 0)
    except (TypeError, ValueError):
        oi_change_pct = 0.0

    if screener_direction in ("LONG", "SHORT"):
        opposite = "SHORT" if screener_direction == "LONG" else "LONG"
        _hint = ("MACD > 0, bullish momentum" if screener_direction == "LONG"
                 else "MACD < 0, bearish momentum")
        screener_context = (
            f"The technical screener found {symbol} as a {screener_direction} "
            f"candidate ({_hint}, volume surge confirmed). Weight the "
            f"{screener_direction} side, but go {opposite} if data is stronger."
        )
    else:
        screener_context = "No directional bias from screener."

    fill_data = {
        "symbol":           symbol,
        "price":            float(price or 0),
        "change":           f"{float(change or 0):+.2f}",
        "rsi_15m":          f"{float(rsi_15m or 0):.1f}",
        "rsi_1h":           f"{float(rsi_1h or 0):.1f}",
        "rsi_4h":           f"{float(rsi_4h or 0):.1f}",
        "funding_rate":     float(funding_rate or 0),
        "oi_change":        oi_change_pct,
        "news":             news or "No news available.",
        "regime":           market_regime.get("regime", "NEUTRAL"),
        "btc_24h":          float(market_regime.get("btc_24h", 0.0) or 0),
        "screener_context": screener_context,
        # Legacy keys for old prompts that still have these placeholders
        "leverage":         int(leverage or 1),
        "btc_7d":           float(market_regime.get("btc_7d", 0.0) or 0),
        "fg":               fg,
        "fg_label":         str(market_regime.get("fg_label", "Neutral")),
        "open_interest_usdt": float(open_interest_usdt or 0),
        "liq_long_pct":     0.0,
        "liq_short_pct":    0.0,
    }

    prompt = render_prompt(template, fill_data)
    if reflection:
        prompt = reflection + "\n" + prompt

    try:
        response  = generate_with_timeout(get_model_name(), prompt,
                                           use_json_format=True)
        full_text = response.get("response") or ""

        # New prompt returns clean JSON  parse directly.
        # The old prompt returned free-text with "RESULT: LONG" at end.
        # We handle both so user-edited prompts using the old format still work.
        import json as _json
        _jtext = full_text.strip()
        if _jtext.startswith("```"):
            _jtext = _jtext.split("```")[1].strip()
            if _jtext.startswith("json"):
                _jtext = _jtext[4:].lstrip()
        try:
            parsed    = _json.loads(_jtext)
            direction = str(parsed.get("direction", "WAIT")).upper()
            if direction not in ("LONG", "SHORT", "WAIT"):
                direction = "WAIT"
            confidence = str(parsed.get("confidence", "LOW")).upper()
            rationale  = parsed.get("rationale", "")
            steelman   = parsed.get("steelman", "")
            if steelman:
                log_event(f"[{symbol}] Steelman: {steelman}", "INFO")

            #  Bull/Bear challenge (adversarial risk check) 
            # Runs only when a real direction is present; a STRONG verdict from
            # the skeptical risk-manager downgrades the trade to WAIT. Costs a
            # 2nd LLM call per candidate.
            if direction in ("LONG", "SHORT"):
                try:
                    verdict = futures_bull_bear_challenge(
                        symbol, direction,
                        steelman or rationale or full_text,
                        context_brief=f"conf={confidence}, 24h={change}%",
                    )
                except Exception as _bb_exc:
                    # Challenge must never crash the main flow  fail-open.
                    # Log type + message so the cause is diagnosable.
                    log_event(
                        f"[{symbol}] Bull/Bear skipped "
                        f"({type(_bb_exc).__name__}: {_bb_exc})", "WARN")
                    verdict = "PROCEED"
                if verdict == "OVERRIDE_WAIT":
                    parsed["direction"] = "WAIT"
                    parsed.setdefault("rationale", "")
                    parsed["rationale"] = (
                        (parsed["rationale"] + " | ") if parsed["rationale"] else ""
                    ) + "Bull/Bear override: risks outweigh setup"
                    log_event(f"[{symbol}] LLM decision: WAIT "
                              f"(Bull/Bear override)", "INFO")
                    try:
                        from core.logger import log_struct
                        log_struct("llm_veto", symbol=symbol, original=direction,
                                   confidence=confidence, decision="WAIT",
                                   reason="bull_bear_override")
                    except Exception:
                        pass
                    return _json.dumps(parsed)

            # Return a JSON string that parse_direction_and_confidence can read
            log_event(
                f"[{symbol}] LLM decision: {direction} (conf={confidence})  "
                f"{str(rationale or '')[:140]}", "INFO")
            return full_text
        except (_json.JSONDecodeError, ValueError):
            # Old free-text prompt format  fall through to old parser
            thinking, answer = strip_thinking(full_text)
            return answer if answer else full_text

    except Exception as exc:
        # Log the exact reason so the user can diagnose keyword-fallback
        # incidents. Common causes:
        #  LLM timeout (model cold-loading, takes 15-30s)
        #  Ollama daemon down or model not loaded
        #  Connection error / Ollama crash
        try:
            log_event(
                f"[{symbol}] LLM unavailable; keyword fallback used "
                f"({type(exc).__name__}: {str(exc)[:120]})",
                "INFO",
            )
        except Exception:
            pass
        return _futures_keyword_fallback(symbol, news, float(change or 0),
                                          float(rsi_1h or 0))


def parse_direction(llm_response: str) -> str:
    direction, _ = parse_direction_and_confidence(llm_response)
    return direction


def parse_direction_and_confidence(llm_response: str):
    """Returns (direction, confidence). direction  {LONG, SHORT, WAIT}.

    Handles two prompt formats:
    1. New simple JSON: {"direction": "LONG", "confidence": "HIGH", ...}
    2. Old free-text:   ends with "RESULT: LONG"
    """
    if not llm_response:
        return ("WAIT", "LOW")

    # Try new JSON format first
    import json as _json
    _text = llm_response.strip()
    # Strip markdown code fences if present
    if _text.startswith("```"):
        _text = _text.split("```")[1]
        if _text.startswith("json"):
            _text = _text[4:]
        _text = _text.strip()
    try:
        parsed = _json.loads(_text)
        direction  = str(parsed.get("direction", "WAIT")).upper()
        confidence = str(parsed.get("confidence", "LOW")).upper()
        if direction not in ("LONG", "SHORT", "WAIT"):
            direction = "WAIT"
        if confidence not in ("HIGH", "MEDIUM", "LOW"):
            confidence = "LOW"
        return (direction, confidence)
    except (_json.JSONDecodeError, ValueError, AttributeError):
        pass

    # Fall back to old free-text format
    direction  = parse_last_result(llm_response)
    if direction not in ("LONG", "SHORT", "WAIT"):
        direction = "WAIT"
    confidence = _parse_conf(llm_response)
    return (direction, confidence)
