"""Bounded response readers for untrusted external news payloads."""
from __future__ import annotations

import json
from collections.abc import Mapping

MAX_RSS_RESPONSE_BYTES = 2 * 1024 * 1024
_STREAM_CHUNK_BYTES = 64 * 1024


def _close_response_quietly(response) -> None:
    try:
        closer = getattr(response, "close", None)
        if callable(closer):
            closer()
    except Exception:
        pass


def require_success(response) -> None:
    """Raise for an HTTP failure and always release a streamed response."""
    checker = getattr(response, "raise_for_status", None)
    if not callable(checker):
        _close_response_quietly(response)
        raise ValueError("response does not expose status validation")
    try:
        checker()
    except BaseException:
        _close_response_quietly(response)
        raise


def read_bounded_response(
    response,
    *,
    max_bytes: int = MAX_RSS_RESPONSE_BYTES,
) -> bytes:
    """Read a streamed response with a hard decompressed-size ceiling."""
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ValueError("response byte limit must be a positive integer")
    closer = getattr(response, "close", None)
    try:
        headers = getattr(response, "headers", None)
        if isinstance(headers, Mapping):
            raw_length = headers.get("Content-Length")
            if raw_length is not None:
                try:
                    declared = int(raw_length)
                except (TypeError, ValueError, OverflowError):
                    declared = None
                if declared is not None and declared > max_bytes:
                    raise ValueError("response exceeds byte limit")

        iterator = getattr(response, "iter_content", None)
        if callable(iterator):
            chunks: list[bytes] = []
            total = 0
            for chunk in iterator(chunk_size=_STREAM_CHUNK_BYTES):
                if not chunk:
                    continue
                if not isinstance(chunk, (bytes, bytearray)):
                    raise ValueError("response chunk must be bytes")
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError("response exceeds byte limit")
                chunks.append(bytes(chunk))
            return b"".join(chunks)

        content = getattr(response, "content", None)
        if not isinstance(content, (bytes, bytearray)):
            raise ValueError("response content must be bytes")
        if len(content) > max_bytes:
            raise ValueError("response exceeds byte limit")
        return bytes(content)
    finally:
        if callable(closer):
            try:
                closer()
            except Exception:
                pass


def read_bounded_json_response(
    response,
    *,
    max_bytes: int = MAX_RSS_RESPONSE_BYTES,
):
    """Decode bounded JSON; tiny test doubles may expose only ``json()``."""
    if callable(getattr(response, "iter_content", None)) or isinstance(
        getattr(response, "content", None), (bytes, bytearray)
    ):
        payload = read_bounded_response(response, max_bytes=max_bytes)
        return json.loads(payload)
    try:
        decoder = getattr(response, "json", None)
        if not callable(decoder):
            raise ValueError("response does not expose JSON content")
        return decoder()
    finally:
        _close_response_quietly(response)
