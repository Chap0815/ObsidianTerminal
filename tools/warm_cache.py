"""Sequentially pre-warm the OHLCV disk cache for the backtest universe.

The optimizers fetch with 8 parallel workers, which bursts past MEXC's 429 rate
limit. Warming the cache SEQUENTIALLY first (one symbol at a time, with backoff)
fills data/ohlcv_cache/ reliably; the optimizers then read from cache with zero
API calls. Full history (asof-agnostic) so the same cache serves IS + OOS.

Run: PYTHONIOENCODING=utf-8 python -m tools.warm_cache [universe_n]
"""
import sys
import io
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from tools.backtester import connect_exchange, get_top_volume_coins
from tools.ohlcv_cache import get_series

N = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 40
DAYS = 688

# 12-coin TREND live universe (daily)  warmed so a 12-coin TREND re-run is API-free too.
MAJORS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "SOL/USDT", "ADA/USDT",
          "AVAX/USDT", "LINK/USDT", "DOT/USDT", "LTC/USDT", "DOGE/USDT", "TRX/USDT"]

ex = connect_exchange()
now = ex.milliseconds()
since_1h = now - (DAYS * 24 + 50) * 3_600_000
since_1d = now - (DAYS + 5) * 86_400_000

coins = get_top_volume_coins(ex, N, days=DAYS)
# union with majors so both the alt universe and the TREND set are covered
uni = list(dict.fromkeys(list(coins) + MAJORS))
print(f"warming {len(uni)} symbols (1h) + {len(MAJORS)} majors (1d)")

t0 = time.time()
ok = 0
for i, c in enumerate(uni, 1):
    try:
        s = get_series(ex, c, "1h", since_1h)
        n = len(s)
        if n > 0:
            ok += 1
        print(f"  [{i:>2}/{len(uni)}] 1h {c:16s} bars={n}", flush=True)
    except Exception as e:
        print(f"  [{i:>2}/{len(uni)}] 1h {c:16s} FAIL {type(e).__name__}", flush=True)
print(f"1h warm: {ok}/{len(uni)} ok in {time.time()-t0:.0f}s")

okd = 0
for c in MAJORS:
    try:
        s = get_series(ex, c, "1d", since_1d)
        if len(s) > 0:
            okd += 1
        print(f"  1d {c:16s} bars={len(s)}", flush=True)
    except Exception as e:
        print(f"  1d {c:16s} FAIL {type(e).__name__}", flush=True)
print(f"1d warm: {okd}/{len(MAJORS)} ok")
print("WARM DONE")
