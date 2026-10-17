"""Bounded, bytes-only RSS parsing without the native XML parser."""
from __future__ import annotations

import base64
import codecs
import re

from feedparser.api import LooseFeedParser
from feedparser.util import FeedParserDict

from news.http_limits import MAX_RSS_RESPONSE_BYTES

_MAX_DECLARATION = 1024
_MAX_TOKEN = 8192
_MAX_TEXT = 65536
_MAX_TOKENS = 20000
_MAX_DEPTH = 64
_MAX_ATTRIBUTES = 64
_MAX_ATTRIBUTE_VALUE = 2048
_MAX_ENTRIES = 256
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.:-]*")
_ENCODING = re.compile(r"\bencoding\s*=\s*(['\"])([A-Za-z0-9_.-]{1,64})\1", re.I)
_FORBIDDEN_DECLARATION = re.compile(r"<!\s*(?:DOCTYPE|ENTITY)\b", re.I)
_HTML_VOID = frozenset((
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link",
    "meta", "param", "source", "track", "wbr",
))


def _declared_encoding(prefix: str) -> str | None:
    if not prefix.startswith("<?xml") or prefix[5:6] not in (" ", "\t", "\r", "\n", "?"):
        return None
    end = prefix.find("?>", 5, _MAX_DECLARATION + 1)
    if end < 0:
        raise ValueError("RSS XML declaration exceeds limit or is incomplete")
    matches = list(_ENCODING.finditer(prefix[:end]))
    if len(matches) > 1:
        raise ValueError("duplicate RSS encoding declaration")
    return matches[0][2] if matches else None


def _xml_codec(name: str) -> str:
    try:
        codec = codecs.lookup(name).name
    except LookupError as exc:
        raise ValueError("unsupported RSS encoding") from exc
    # Only XML text codecs; escape/transform codecs could introduce markup.
    if codec not in {
        "ascii", "utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be",
        "utf-32", "utf-32-le", "utf-32-be", "big5", "big5hkscs", "gb18030",
        "gb2312", "gbk", "shift_jis", "euc_jp", "euc_kr", "koi8-r", "koi8-u",
    } and not re.fullmatch(r"(?:iso8859-(?:[1-9]|1[0-6])|cp125[0-8])", codec):
        raise ValueError("unsupported RSS encoding")
    return {"gb2312": "gb18030", "utf-8-sig": "utf-8"}.get(codec, codec)


def _decode_xml(payload: bytes) -> tuple[str, str]:
    # Locate declarations with bounded work, before any third-party regex.
    signatures = (
        (codecs.BOM_UTF32_BE, "utf-32-be"),
        (codecs.BOM_UTF32_LE, "utf-32-le"),
        (codecs.BOM_UTF16_BE, "utf-16-be"),
        (codecs.BOM_UTF16_LE, "utf-16-le"),
        (codecs.BOM_UTF8, "utf-8"),
    )
    detected = None
    for signature, codec in signatures:
        if payload.startswith(signature):
            detected = codec
            payload = payload[len(signature):]
            break
    if detected is None:
        detected = {
            b"\x00\x00\x00<": "utf-32-be", b"<\x00\x00\x00": "utf-32-le",
            b"\x00<\x00?": "utf-16-be", b"<\x00?\x00": "utf-16-le",
        }.get(payload[:4])
    if detected:
        text = payload.decode(detected)
        declared = _declared_encoding(text[:_MAX_DECLARATION + 1])
        if declared:
            canonical = _xml_codec(declared)
            family = detected.rsplit("-", 1)[0]
            if canonical not in (detected, family):
                raise ValueError("conflicting RSS encoding declaration")
        return text, detected
    declared = _declared_encoding(payload[:_MAX_DECLARATION + 1].decode("ascii", "replace"))
    codec = _xml_codec(declared) if declared else "utf-8"
    try:
        return payload.decode(codec), codec
    except UnicodeDecodeError:
        if declared:
            raise
        for fallback in ("cp1252", "iso8859-2"):
            try:
                return payload.decode(fallback), fallback
            except UnicodeDecodeError:
                pass
        raise ValueError("invalid RSS encoding") from None


def _check_text(text: str) -> None:
    if len(text) > _MAX_TEXT:
        raise ValueError("RSS text exceeds limit")
    # Numeric references are converted to Python integers by the loose parser.
    pos = 0
    while True:
        pos = text.find("&#", pos)
        if pos < 0:
            return
        end = pos + 2
        while end < len(text) and text[end] in "0123456789abcdefABCDEFxX":
            end += 1
        if end - pos > 32:
            raise ValueError("RSS character reference exceeds limit")
        pos = end


def _check_tag(token: str) -> tuple[str, bool]:
    match = _NAME.match(token, 1)
    if match is None or len(match[0]) > 128:
        raise ValueError("invalid RSS tag")
    name = match[0].lower()
    pos, count = match.end(), 0
    limit = len(token) - 1
    while pos < limit:
        while pos < limit and token[pos].isspace():
            pos += 1
        if pos == limit or token[pos:limit].strip() == "/":
            break
        attr = _NAME.match(token, pos)
        if attr is None or len(attr[0]) > 128:
            raise ValueError("invalid RSS attribute")
        count += 1
        if count > _MAX_ATTRIBUTES:
            raise ValueError("RSS attribute count exceeds limit")
        pos = attr.end()
        while pos < limit and token[pos].isspace():
            pos += 1
        if pos < limit and token[pos] == "=":
            pos += 1
            while pos < limit and token[pos].isspace():
                pos += 1
            start = pos
            if pos < limit and token[pos] in "\"'":
                quote = token[pos]
                pos = token.find(quote, pos + 1, limit)
                if pos < 0:
                    raise ValueError("incomplete RSS attribute")
                pos += 1
            else:
                while pos < limit and not token[pos].isspace():
                    pos += 1
            if pos - start > _MAX_ATTRIBUTE_VALUE:
                raise ValueError("RSS attribute value exceeds limit")
            if "<" in token[start:pos]:
                raise ValueError("invalid RSS attribute markup")
    return name, token[:-1].rstrip().endswith("/")


def _check_markup(text: str, *, html: bool = False) -> None:
    """Linear lexical bounds before SGML regexes/attribute allocation run."""
    if _FORBIDDEN_DECLARATION.search(text):
        raise ValueError("RSS DTD and entity declarations are prohibited")
    stack: list[str] = []
    pos = count = 0
    while pos < len(text):
        start = text.find("<", pos)
        if start < 0:
            _check_text(text[pos:])
            break
        _check_text(text[pos:start])
        if html and not text.startswith(("<!", "<?", "</"), start) and _NAME.match(text, start + 1) is None:
            # A comparison such as "BTC < $100000" is ordinary text.
            pos = start + 1
            continue
        count += 1
        if count > _MAX_TOKENS:
            raise ValueError("RSS token count exceeds limit")
        if text.startswith("<!--", start):
            end = text.find("-->", start + 4, start + _MAX_TOKEN + 1)
            end = end + 3 if end >= 0 else -1
        elif text.startswith("<![CDATA[", start):
            if html:
                # The HTML sanitizer escapes this opener and parses its body.
                pos = start + 9
                continue
            end = text.find("]]>", start + 9, start + _MAX_TEXT + 1)
            end = end + 3 if end >= 0 else -1
        elif text.startswith("<?", start):
            end = text.find("?>", start + 2, start + _MAX_DECLARATION + 1)
            end = end + 2 if end >= 0 else -1
        else:
            if text.startswith("<!", start):
                raise ValueError("unsupported RSS declaration")
            end = start + 1
            quote = None
            while end < len(text) and end - start < _MAX_TOKEN:
                char = text[end]
                if quote:
                    if char == quote:
                        quote = None
                elif char in "\"'":
                    quote = char
                elif char == ">":
                    break
                elif char == "<":
                    break
                end += 1
            if end >= len(text) or text[end] != ">" or quote:
                raise ValueError("RSS tag exceeds limit or is incomplete")
            end += 1
            token = text[start:end]
            if token.startswith("</"):
                name = token[2:-1].strip().lower()
                if not _NAME.fullmatch(name):
                    raise ValueError("invalid RSS end tag")
                if html:
                    if name in stack:
                        del stack[len(stack) - 1 - stack[::-1].index(name):]
                elif not stack or stack.pop() != name:
                    raise ValueError("unbalanced RSS tags")
            else:
                name, closed = _check_tag(token)
                if not closed and not (html and name in _HTML_VOID):
                    stack.append(name)
                    if len(stack) > _MAX_DEPTH:
                        raise ValueError("RSS nesting exceeds limit")
        if end < 0:
            raise ValueError("RSS markup exceeds limit or is incomplete")
        pos = end


def _check_parser_input(text: str, *, html: bool = False) -> None:
    _check_markup(text, html=html)
    # _BaseHTMLProcessor.feed rewrites these before SGML tokenization. Quotes
    # can move a token boundary, so callbacks alone cannot enforce this bound.
    rewritten = text.replace("&#39;", "'").replace("&#34;", '"')
    if rewritten != text:
        _check_markup(rewritten, html=html)


class _BoundedLooseFeedParser(LooseFeedParser):
    def unknown_starttag(self, tag, attrs):
        # Feedparser rewrites numeric quote references before SGML tokenization.
        if len(attrs) > _MAX_ATTRIBUTES or any(len(value) > _MAX_ATTRIBUTE_VALUE for _, value in attrs):
            raise ValueError("RSS attribute limits exceeded")
        return super().unknown_starttag(tag, attrs)

    def _start_item(self, attrs_d):
        if len(self.entries) >= _MAX_ENTRIES:
            raise ValueError("RSS entry count exceeds limit")
        return super()._start_item(attrs_d)

    _start_entry = _start_item

    def decode_entities(self, element, data):
        data = super().decode_entities(element, data)
        # Embedded HTML is parsed again by Feedparser's sanitizer/URI resolver.
        _check_parser_input(data, html=True)
        return data

    def pop(self, element, strip_whitespace=1):
        if self.elementstack and self.elementstack[-1][0] == element:
            pieces = self.elementstack[-1][2]
            if sum(len(piece) for piece in pieces) > _MAX_TEXT:
                raise ValueError("RSS field exceeds limit")
            if self.contentparams.get("base64", 0):
                raw = "".join(piece.decode("utf-8") if isinstance(piece, bytes) else piece for piece in pieces)
                try:
                    decoded = base64.decodebytes(raw.encode("utf-8")).decode("utf-8")
                except (ValueError, UnicodeError):
                    decoded = raw
                _check_parser_input(decoded, html=True)
        return super().pop(element, strip_whitespace)


def parse_rss_bytes(payload: bytes) -> FeedParserDict:
    """Parse bounded in-memory bytes; never open a filename, URL, or SAX parser."""
    if type(payload) is not bytes:
        raise TypeError("RSS input must be bytes")
    if len(payload) > MAX_RSS_RESPONSE_BYTES:
        raise ValueError("RSS response exceeds byte limit")
    result = FeedParserDict(bozo=False, entries=[], feed=FeedParserDict(), headers={})
    if not payload:
        return result
    text, encoding = _decode_xml(payload)
    if len(text.encode("utf-8")) > MAX_RSS_RESPONSE_BYTES:
        raise ValueError("normalized RSS exceeds byte limit")
    if any(ord(char) < 32 and char not in "\t\r\n" for char in text):
        raise ValueError("invalid RSS control character")
    _check_parser_input(text)
    parser = _BoundedLooseFeedParser("", None, "utf-8", {})
    parser.resolve_relative_uris = True
    parser.sanitize_html = True
    parser.feed(text)
    result.update(
        feed=parser.feeddata, entries=parser.entries, version=parser.version,
        namespaces=parser.namespaces_in_use, encoding=encoding,
    )
    return result
