"""
bot_utils/indicators.py  Native, dependency-free technical indicators.

WARUM nativ statt pandas_ta:
  pandas-ta 0.3.14b0 wurde von PyPI ENTFERNT. Die einzige verbleibende
  Version (0.4.71b0) zieht numpy 2.x + pandas 3.0 + numba nach  pandas 3.0
  bringt Breaking Changes, gegen die der restliche Bot nicht getestet ist.
  Eine externe Lib, die jederzeit von PyPI verschwinden kann, ist fuer einen
  Trading-Bot eine echte Schwachstelle.

Diese vier Funktionen decken ALLES ab, was Screener + Backtester brauchen,
in reinem pandas (vektorisiert, schnell, numpy-1.x UND 2.x, pandas-2 UND 3).

FIDELITAET:
  rsi  rekursive Wilder-RMA mit expliziter Zero-loss-Behandlung
  atr  rekursive Wilder-RMA
  ema  Standard-EWM (adjust=False)
  Live-Screener und Research verwenden dieselbe Implementierung. Eine
  Bitidentitaet zu einer bestimmten pandas_ta-Version wird nicht behauptet,
  weil deren Initial-Seeding versionsabhaengig sein kann.
  macd_signal  gibt die SIGNALLINIE (MACDs) zurueck. Das ist BEWUSST:
    der Altcode nahm ``df.ta.macd(...).iloc[:, -1]``  und die letzte Spalte
    von pandas_ta ist ``MACDs`` (Signallinie), NICHT das Histogramm. Die
    Variable hiess zwar "macd_hist", gefiltert wurde aber stets auf der
    Signallinie. Wir replizieren das 1:1, um das (validierte) Live-Verhalten
    NICHT zu aendern. ``macd_hist()`` steht separat bereit, falls spaeter auf
    das echte Histogramm umgestellt werden soll.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def ema(close: pd.Series, length: int) -> pd.Series:
    """Exponential moving average (adjust=False, wie pandas_ta)."""
    return close.ewm(span=length, adjust=False).mean()


def rsi(close: pd.Series, length: int = 14) -> pd.Series:
    """Wilder-RSI mit rekursiver RMA und expliziter Zero-loss-Behandlung.

    Zero-loss handling (pandas_ta-Konvention)  ein naives
    ``rs = gain / loss`` ergbe bei verlustfreiem Wilder-Fenster RSI=NaN, und
    der Screener wrde die strksten Momentum-Coins still verwerfen:
      loss==0  gain>0  RSI = 100  (nur Gewinne)
      gain==0  loss==0  RSI = 50  (kein Move)
    """
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1.0 / length, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1.0 / length, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    # No losses in the window  RSI 100 (matches pandas_ta), aber nur wo
    # tatschlich Gewinne anliegen; flat (gain==0  loss==0)  neutral 50.
    # Wo gain/loss selbst NaN sind (erste Bar, delta=NaN) bleibt out NaN 
    # die where()-Maske darf das nicht zu 100/50 verflschen.
    valid = gain.notna() & loss.notna()
    out = out.where(~(valid & (loss == 0)), 100.0)
    out = out.where(~(valid & (gain == 0) & (loss == 0)), 50.0)
    return out


def _macd_lines(close: pd.Series, fast: int = 12, slow: int = 26,
                signal: int = 9):
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


def macd_signal(close: pd.Series, fast: int = 12, slow: int = 26,
                signal: int = 9) -> pd.Series:
    """Signallinie (MACDs)  repliziert ``df.ta.macd(...).iloc[:, -1]``.

    Bewahrt das bisherige Screener/Backtester-Verhalten (Sign-basierter
    Momentum-Filter auf der Signallinie). Siehe Modul-Docstring.
    """
    _, sig = _macd_lines(close, fast, slow, signal)
    return sig


def macd_hist(close: pd.Series, fast: int = 12, slow: int = 26,
              signal: int = 9) -> pd.Series:
    """True MACD histogram (MACD - signal).

    Currently not used by the screener; kept for a future migration away from
    the legacy signal-line behavior.
    """
    macd_line, sig = _macd_lines(close, fast, slow, signal)
    return macd_line - sig


def atr(high: pd.Series, low: pd.Series, close: pd.Series,
        length: int = 14) -> pd.Series:
    """Average True Range (Wilder-RMA)  identisch zu pandas_ta.atr(length)."""
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / length, adjust=False).mean()
