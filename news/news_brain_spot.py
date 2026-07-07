"""
news_brain_spot.py  News-Sentiment analysis for the SPOT bot.

Thin adapter around news_brain_core + llm_utils  shares all infrastructure
with news_brain_futures.py.
"""
from __future__ import annotations

from datetime import datetime, timezone

from news.llm_utils import (llm_available, keyword_fallback, get_model_name,
                        bull_bear_challenge, generate_with_timeout)
from news.news_brain_core import (
    load_prompt_template, render_prompt,
    get_latest_news, parse_last_result, parse_confidence as _parse_conf,
    strip_thinking, is_valid_symbol,
)

_BOT_NAME = "SPOT"

# Prompts live in PROJECT_ROOT/prompts/.
from core.paths import (
    PROMPT_SPOT         as _PROMPT_FILE,
    PROMPT_SPOT_DEFAULT as _DEFAULT_FILE,
)
from core.logger import log_event
_PROMPT_FILE  = str(_PROMPT_FILE)
_DEFAULT_FILE = str(_DEFAULT_FILE)

_FALLBACK_PROMPT = (
    "Aggressive momentum trader. Symbol: {symbol}, 24h: {change}%, "
    "RSI: {rsi_15m:.1f}/{rsi_1h:.1f}/{rsi_4h:.1f}.\n"
    "News: {news}. Market: {regime}.\n"
    "Default to BUY unless clear danger. End with: RESULT: BUY or RESULT: WAIT."
)


def analyze_sentiment(symbol, change, rsi_15m, rsi_1h, rsi_4h, news,
                      market_regime: dict = None):
    if not is_valid_symbol(symbol):
        return keyword_fallback(symbol, news, strategy="SPOT")
    if not llm_available():
        return keyword_fallback(symbol, news, strategy="SPOT")

    if market_regime is None:
        market_regime = {"regime": "NEUTRAL", "btc_24h": 0.0, "btc_7d": 0.0}

    template = load_prompt_template(_PROMPT_FILE, _DEFAULT_FILE,
                                    fallback=_FALLBACK_PROMPT)

    try:
        from trading.risk_manager import get_reflection_context
        reflection = get_reflection_context(_BOT_NAME)
    except Exception:
        reflection = ""

    fill_data = {
        "symbol":           symbol,
        "change":           change,
        "rsi_15m":          float(rsi_15m or 0),
        "rsi_1h":           float(rsi_1h or 0),
        "rsi_4h":           float(rsi_4h or 0),
        "news":             news,
        "regime":           market_regime.get("regime", "NEUTRAL"),
        "btc_24h":          float(market_regime.get("btc_24h", 0.0) or 0),
        "btc_7d":           float(market_regime.get("btc_7d", 0.0) or 0),
        # Fields required by user's detailed spot.txt prompt
        "as_of":            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "vol_24h_usdt":     0.0,
        "vol_ratio":        1.0,
        "current_position": "None",
    }

    prompt = render_prompt(template, fill_data)
    if reflection:
        prompt = reflection + "\n" + prompt

    try:
        response = generate_with_timeout(get_model_name(), prompt,
                                          use_json_format=True)
        full_text  = response.get("response") or ""
        if not full_text:
            return keyword_fallback(symbol, news, strategy="SPOT")

        import json as _json
        _text = full_text.strip()
        if _text.startswith("```"):
            _text = _text.split("```")[1].strip()
            if _text.startswith("json"):
                _text = _text[4:].lstrip()
        try:
            parsed   = _json.loads(_text)
            steelman = parsed.get("steelman", "")
            if steelman:
                log_event(f"[{symbol}] Steelman: {steelman}", "INFO")

            #  Bull/Bear challenge (adversarial risk check) 
            _dir = str(parsed.get("direction", "WAIT")).upper()
            if _dir == "BUY":
                _conf = str(parsed.get("confidence", "LOW")).upper()
                try:
                    verdict = bull_bear_challenge(
                        symbol,
                        steelman or parsed.get("rationale", "") or full_text,
                        context_brief=f"conf={_conf}, 24h={change}%",
                        confidence=_conf,   # PERF: skip if LOW
                    )
                except Exception as _bb_exc:
                    log_event(
                        f"[{symbol}] Bull/Bear skipped "
                        f"({type(_bb_exc).__name__})", "WARN")
                    verdict = "PROCEED"
                if verdict == "OVERRIDE_WAIT":
                    parsed["direction"] = "WAIT"
                    _r = parsed.get("rationale", "")
                    parsed["rationale"] = (
                        (_r + " | ") if _r else ""
                    ) + "Bull/Bear override: risks outweigh setup"
                    log_event(f"[{symbol}] LLM decision: WAIT "
                              f"(Bull/Bear override)", "INFO")
                    return _json.dumps(parsed)

            log_event(
                f"[{symbol}] LLM decision: {_dir} "
                f"(conf={str(parsed.get('confidence', 'LOW')).upper()})  "
                f"{str(parsed.get('rationale', '') or '')[:140]}", "INFO")
            return full_text
        except (_json.JSONDecodeError, ValueError):
            thinking, answer = strip_thinking(full_text)
            return answer if answer else full_text

    except Exception as exc:
        try:
            log_event(
                f"[{symbol}] LLM unavailable; keyword fallback used "
                f"({type(exc).__name__}: {str(exc)[:120]})",
                "INFO",
            )
        except Exception:
            pass
        return keyword_fallback(symbol, news, strategy="SPOT")


def parse_confidence(llm_response: str) -> str:
    """Parse confidence from new JSON or old free-text format."""
    if not llm_response:
        return "LOW"
    import json as _json
    try:
        parsed = _json.loads(llm_response.strip())
        c = str(parsed.get("confidence", "LOW")).upper()
        return c if c in ("HIGH", "MEDIUM", "LOW") else "LOW"
    except Exception:
        return _parse_conf(llm_response)


def parse_direction_and_confidence(llm_response: str):
    """Returns (direction, confidence). Handles JSON and old free-text.
    Spot bots return BUY/WAIT.
    """
    if not llm_response:
        return ("WAIT", "LOW")
    import json as _json
    _text = llm_response.strip()
    if _text.startswith("```"):
        _text = _text.split("```")[1].strip()
        if _text.startswith("json"):
            _text = _text[4:].lstrip()
    try:
        parsed = _json.loads(_text)
        raw_dir = str(parsed.get("direction", "WAIT")).upper()
        direction = raw_dir if raw_dir in ("BUY", "WAIT") else "WAIT"
        conf = str(parsed.get("confidence", "LOW")).upper()
        confidence = conf if conf in ("HIGH", "MEDIUM", "LOW") else "LOW"
        return (direction, confidence)
    except Exception:
        direction = parse_last_result(llm_response)
        if direction not in ("BUY", "WAIT"):
            direction = "WAIT"
        return (direction, _parse_conf(llm_response))
