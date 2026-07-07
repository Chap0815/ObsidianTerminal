"""
core  Trading-bot base classes.

Public API:
  SpotBot  abstract dual-thread spot trading bot
                   (subclassed by BalancedBot, AggressiveBot)
  FuturesBot  abstract dual-thread futures trading bot
                   (subclassed by FuturesExchangeBot)
"""
from core.spot_bot import SpotBot
from core.futures_bot import FuturesBot

__all__ = ["SpotBot", "FuturesBot"]
