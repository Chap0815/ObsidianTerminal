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
import sys
import time
import traceback


def _hdr(text: str) -> None:
    print(f"\n{'' * 60}")
    print(f"  {text}")
    print(f"{'' * 60}")


def _check_spot() -> int:
    _hdr("1/6  SPOT exchange connection")
    try:
        from config.exchange_config import get_exchange_connection
        ex = get_exchange_connection()
        ex.timeout = 15000
        t0 = time.time()
        ex.load_markets()
        markets_ms = int((time.time() - t0) * 1000)
        print(f"  load_markets: {len(ex.markets)} symbols in {markets_ms} ms")

        t0 = time.time()
        bal = ex.fetch_balance()
        bal_ms = int((time.time() - t0) * 1000)
        usdt_free = (bal.get("USDT") or {}).get("free", 0) or 0
        print(f"  fetch_balance: {bal_ms} ms  USDT free: {usdt_free}")

        t0 = time.time()
        t = ex.fetch_ticker("BTC/USDT")
        last = t.get("last")
        print(f"  fetch_ticker(BTC/USDT): {int((time.time()-t0)*1000)} ms  last: {last}")

        t0 = time.time()
        bars = ex.fetch_ohlcv("BTC/USDT", timeframe="1h", limit=10)
        print(f"  fetch_ohlcv: {len(bars)} bars in {int((time.time()-t0)*1000)} ms")
        return 0
    except Exception as e:
        print(f"  Spot connection FAILED: {type(e).__name__}: {e}")
        traceback.print_exc(limit=2)
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
        ex.load_markets()
        print(f"  load_markets: {len(ex.markets)} swaps in {int((time.time()-t0)*1000)} ms")

        bal = ex.fetch_balance()
        usdt_free = (bal.get("USDT") or {}).get("free", 0) or 0
        print(f"  fetch_balance  USDT free: {usdt_free}")

        try:
            pos = ex.fetch_positions(["BTC/USDT:USDT"])
            print(f"  fetch_positions: {len(pos)} entries")
        except Exception as e:
            print(f"  fetch_positions: {type(e).__name__} (non-fatal)")
        return 0
    except Exception as e:
        print(f"  Futures connection FAILED: {type(e).__name__}: {e}")
        return 1


def _check_market_data() -> int:
    _hdr("3/6  Market filter data")
    try:
        from config.exchange_config import get_exchange_connection
        from trading.market_filters  import get_market_regime, get_fear_greed
        ex = get_exchange_connection()
        ex.load_markets()
        regime = get_market_regime(ex)
        fg = get_fear_greed()
        print(f"  Market regime: {regime.get('regime')}")
        print(f"  BTC 24h:  {regime.get('btc_24h', 0):+.2f}%")
        print(f"  BTC 7d:  {regime.get('btc_7d',  0):+.2f}%")
        print(f"  Fear & Greed:  {fg}")
        return 0
    except Exception as e:
        print(f"  Market data FAILED: {type(e).__name__}: {e}")
        return 1


def _check_database() -> int:
    _hdr("4/6  SQLite database")
    try:
        from core.database import init_db, DB_PATH
        init_db()
        size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        print(f"  Database initialised: {DB_PATH} ({size/1024:.1f} KB)")
        return 0
    except Exception as e:
        print(f"  Database init FAILED: {type(e).__name__}: {e}")
        traceback.print_exc(limit=2)
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
        print(f"  LLM check error: {type(e).__name__}: {e} (non-fatal)")
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
        print(f"  TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID}")
        return 0
    except Exception as e:
        print(f"  Telegram check error: {type(e).__name__}: {e}")
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

    print(f"\n{'' * 60}")
    if failures == 0:
        print("  ALL CRITICAL CHECKS PASSED")
    else:
        print(f"  {failures} CHECK(S) FAILED")
        print("\n  Common causes:")
        print("  USE_PROXY=true but no proxy running")
        print("  API_KEY / API_SECRET wrong or expired")
        print("  API_PASSPHRASE missing (Bitget/OKX/KuCoin)")
        print("  Network connectivity issues")
    print(f"{'' * 60}\n")
    return failures


if __name__ == "__main__":
    sys.exit(main())
