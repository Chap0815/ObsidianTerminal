"""
check_connection.py  Connection smoke test for spot AND futures.

Verifies:
  Spot connection
  Futures connection (if configured)
  fetch_balance works
  fetch_ticker works (BTC/USDT)
  fetch_ohlcv works (the screener's main call)
  Optional: fetch_positions for futures
  Ollama LLM availability
  Telegram bot token (if configured)
  Local SQLite DB initialization
"""
from __future__ import annotations

import os
import math
import sys
import time

from bot_utils.order_utils import explicit_trade_symbol_matches


def _hdr(text: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}")


def _redact_text(text: str) -> str:
    try:
        from core.logger import redact
        return redact(text)
    except Exception:
        return "<redaction unavailable>"


def _safe_exc(exc: Exception) -> str:
    return f"{type(exc).__name__}: {_redact_text(str(exc))}"


def _mask_chat_ids(raw: str) -> str:
    ids = str(raw or "").replace(";", " ").replace(",", " ").split()
    masked = []
    for cid in ids:
        text = str(cid)
        masked.append("***" + text[-4:] if len(text) > 4 else "***")
    return ", ".join(masked)


def _finite_number(value, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(parsed) or (parsed <= 0.0 if positive else parsed < 0.0):
        qualifier = "finite and positive" if positive else "finite and non-negative"
        raise ValueError(f"{field} must be {qualifier}")
    return parsed


def _finite_signed_number(value, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be a finite number")
    return parsed


def _fear_greed_index(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Fear & Greed must be an integer from 0 to 100")
    if not 0 <= value <= 100:
        raise ValueError("Fear & Greed must be an integer from 0 to 100")
    return value


def _valid_ohlcv_response(bars, *, limit: int) -> bool:
    if not isinstance(bars, (list, tuple)) or not 1 <= len(bars) <= limit:
        return False
    for row in bars:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            return False
        if any(isinstance(value, bool) for value in row[:6]):
            return False
        try:
            timestamp = float(row[0])
            open_price, high, low, close, volume = map(float, row[1:6])
        except (TypeError, ValueError, OverflowError):
            return False
        if not (
            math.isfinite(timestamp)
            and timestamp >= 0.0
            and timestamp.is_integer()
            and all(
                math.isfinite(value)
                for value in (open_price, high, low, close, volume)
            )
            and min(open_price, high, low, close) > 0.0
            and low <= open_price <= high
            and low <= close <= high
            and volume >= 0.0
        ):
            return False
    return True


def _check_spot() -> int:
    _hdr("1/6  SPOT exchange connection")
    try:
        from config.exchange_config import get_exchange_connection
        ex = get_exchange_connection()
        ex.timeout = 15000
        t0 = time.time()
        markets = ex.load_markets()
        if not isinstance(markets, dict) or not markets:
            raise ValueError("spot market catalog is empty or invalid")
        markets_ms = int((time.time() - t0) * 1000)
        print(f"  load_markets: {len(markets)} symbols in {markets_ms} ms")

        t0 = time.time()
        bal = ex.fetch_balance()
        bal_ms = int((time.time() - t0) * 1000)
        usdt_free = _finite_number(
            (bal.get("USDT") or {}).get("free", 0) or 0,
            "spot USDT free balance",
        )
        print(f"  fetch_balance: {bal_ms} ms  USDT free: {usdt_free}")

        t0 = time.time()
        t = ex.fetch_ticker("BTC/USDT")
        if not explicit_trade_symbol_matches(t, "BTC/USDT"):
            raise ValueError("spot connection ticker changed requested symbol")
        last = _finite_number(
            t.get("last"),
            "spot BTC/USDT last price",
            positive=True,
        )
        print(f"  fetch_ticker(BTC/USDT): {int((time.time()-t0)*1000)} ms  last: {last}")

        t0 = time.time()
        bars = ex.fetch_ohlcv("BTC/USDT", timeframe="1h", limit=10)
        if not _valid_ohlcv_response(bars, limit=10):
            raise ValueError("spot BTC/USDT OHLCV response is invalid")
        print(f"  fetch_ohlcv: {len(bars)} bars in {int((time.time()-t0)*1000)} ms")
        return 0
    except Exception as e:
        print(f"  Spot connection FAILED: {_safe_exc(e)}")
        return 1


def _check_futures() -> int:
    _hdr("2/6  FUTURES exchange connection (optional)")
    try:
        from config.exchange_config import (
            get_futures_exchange_connection,
            supports_futures, get_active_exchange_name,
        )
        if not supports_futures():
            print(f"  Futures not supported on {get_active_exchange_name()}")
            return 0
        ex = get_futures_exchange_connection()
        ex.timeout = 15000
        t0 = time.time()
        markets = ex.load_markets()
        if not isinstance(markets, dict) or not markets:
            raise ValueError("futures market catalog is empty or invalid")
        print(f"  load_markets: {len(markets)} swaps in {int((time.time()-t0)*1000)} ms")

        bal = ex.fetch_balance()
        usdt_free = _finite_number(
            (bal.get("USDT") or {}).get("free", 0) or 0,
            "futures USDT free balance",
        )
        print(f"  fetch_balance  USDT free: {usdt_free}")

        try:
            pos = ex.fetch_positions(["BTC/USDT:USDT"])
            print(f"  fetch_positions: {len(pos)} entries")
        except Exception as e:
            print(f"  fetch_positions: {_safe_exc(e)} (non-fatal)")
        return 0
    except Exception as e:
        print(f"  Futures connection FAILED: {_safe_exc(e)}")
        return 1


def _check_market_data() -> int:
    _hdr("3/6  Market filter data")
    try:
        from config.exchange_config import get_exchange_connection
        from trading.market_filters  import get_market_regime, get_fear_greed
        ex = get_exchange_connection()
        ex.timeout = 15000
        markets = ex.load_markets()
        if not isinstance(markets, dict) or not markets:
            raise ValueError("market-data catalog is empty or invalid")
        regime = get_market_regime(ex)
        regime_label = regime.get("regime")
        if regime_label not in {"BULL", "BEAR", "NEUTRAL", "UNKNOWN"}:
            raise ValueError("market regime label is invalid")
        if regime_label == "UNKNOWN":
            raise RuntimeError("market regime data is unavailable")
        fg = _fear_greed_index(get_fear_greed())
        btc_24h = _finite_signed_number(
            regime.get("btc_24h"), "BTC 24h change"
        )
        btc_7d = _finite_signed_number(
            regime.get("btc_7d"), "BTC 7d change"
        )
        print(f"  Market regime: {regime_label}")
        print(f"  BTC 24h:  {btc_24h:+.2f}%")
        print(f"  BTC 7d:  {btc_7d:+.2f}%")
        print(f"  Fear & Greed:  {fg}")
        return 0
    except Exception as e:
        print(f"  Market data FAILED: {_safe_exc(e)}")
        return 1


def _check_database() -> int:
    _hdr("4/6  SQLite database")
    try:
        from core.database import init_db, DB_PATH
        init_db()
        if not os.path.isfile(DB_PATH):
            raise RuntimeError("database artifact was not created")
        size = os.path.getsize(DB_PATH)
        if size <= 0:
            raise RuntimeError("database artifact is empty")
        print(f"  Database initialised: {DB_PATH} ({size/1024:.1f} KB)")
        return 0
    except Exception as e:
        print(f"  Database init FAILED: {_safe_exc(e)}")
        return 1


def _check_llm() -> int:
    _hdr("5/6  Ollama LLM")
    try:
        from news.llm_utils import (
            OLLAMA_URL,
            get_llm_status_text,
            get_model_name,
            llm_available,
        )
        ok = llm_available()
        if ok:
            print(f"  Ollama online at {OLLAMA_URL}")
            print(f"  Model: {get_model_name()}")
            return 0
        print(
            f"  LLM not ready at {OLLAMA_URL}: "
            f"{get_llm_status_text()} (keyword fallback)"
        )
        return 0   # non-fatal
    except Exception as e:
        print(f"  LLM check error: {_safe_exc(e)} (non-fatal)")
        return 0


def _check_telegram() -> int:
    _hdr("6/6  Telegram (optional)")
    try:
        from config.telegram_config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            print("  Telegram not configured (.env missing TELEGRAM_TOKEN / TELEGRAM_CHAT_ID)")
            return 0
        # Just verify format  don't actually send to avoid spam
        if not TELEGRAM_TOKEN.count(":") == 1:
            print("  TELEGRAM_TOKEN format unusual  expected `<id>:<secret>`")
        else:
            print(f"  TELEGRAM_TOKEN configured (id={TELEGRAM_TOKEN.split(':')[0]})")
        print(f"  TELEGRAM_CHAT_ID: {_mask_chat_ids(TELEGRAM_CHAT_ID)}")
        return 0
    except Exception as e:
        print(f"  Telegram check error: {_safe_exc(e)}")
        return 0


def main() -> int:
    print("\n")
    print("  OBSIDIAN TRADING TERMINAL  Connection Test  ")
    print("")

    failures = 0
    failures += _check_spot()
    failures += _check_futures()
    failures += _check_market_data()
    failures += _check_database()
    failures += _check_llm()
    failures += _check_telegram()

    print(f"\n{'=' * 60}")
    if failures == 0:
        print("  ALL CRITICAL CHECKS PASSED")
    else:
        print(f"  {failures} CHECK(S) FAILED")
        print("\n  Common causes:")
        print("  USE_PROXY=true but no proxy running")
        print("  API_KEY / API_SECRET wrong or expired")
        print("  API_PASSPHRASE missing (Bitget/OKX/KuCoin)")
        print("  Network connectivity issues")
    print(f"{'=' * 60}\n")
    return failures


if __name__ == "__main__":
    sys.exit(main())
