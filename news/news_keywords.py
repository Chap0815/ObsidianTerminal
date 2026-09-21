"""
news_keywords.py  Shared news-keyword vocabulary.

Alle Bots verwenden dieselbe Liste, damit sich Bullish/Bearish-Signale
nicht auseinander entwickeln.
"""
from __future__ import annotations

import re


# Keywords die eine Long-Bias-Setup so gut wie immer invalidieren.
# Ein Spot-Bot der eines davon sieht, returnt WAIT.
# Ein Futures-Bot kann darauf SHORT erwgen.
BEARISH_KEYWORDS: tuple = (
    "hack", "hacked", "exploit", "breach", "breached", "stolen", "theft",
    "scam", "fraud", "rug pull", "rugpull", "exit scam",
    "delist", "delisting", "removed",
    "bankrupt", "insolvent", "insolvency", "collapse",
    "ban", "banned", "illegal", "lawsuit", "arrested", "sec charges",
    "cftc charges", "fined", "indicted", "indictment",
    "shutdown", "suspended", "halt", "halted",
    "investigation", "subpoena", "ponzi", "depeg", "depegged",
    "governance attack", "insider sell", "insider selling",
    "deauthorized", "unauthorized withdrawal",
)


# Keywords die einen Long-Bias-Setup strken.
POSITIVE_KEYWORDS: tuple = (
    "partnership", "listing", "mainnet", "upgrade", "launch",
    "integration", "adoption", "etf", "approval", "approved",
    "funding round", "grant", "milestone",
    "burn", "buyback", "treasury",
)


def find_keyword_matches(
    text: str, keywords: tuple[str, ...] | list[str]
) -> list[str]:
    """Return vocabulary terms found at lexical boundaries in normalized text."""
    if type(text) is not str:
        return []
    return [
        keyword
        for keyword in keywords
        if re.search(rf"(?<!\w){re.escape(keyword)}(?!\w)", text)
    ]
