"""Bound an already-authorized download stream without choosing a host or path."""

from __future__ import annotations

from collections.abc import AsyncIterator

from .policy import BrowserError


async def collect_download(chunks: AsyncIterator[bytes], maximum: int) -> bytes:
    """Caller holds the E/B response context and an outer operation deadline.

    I supplies a fixed-origin stream after checking status and disabling
    redirects and decompression. No authorization headers, cookies, response
    filenames or URLs leave this function. Closing the response belongs to its
    enclosing context, including on overflow or cancellation.
    """
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not 1 <= maximum <= 8388608:
        raise BrowserError("browser_artifact_limit")
    data = bytearray()
    async for chunk in chunks:
        if not isinstance(chunk, bytes) or len(chunk) > maximum - len(data):
            raise BrowserError("browser_artifact_limit")
        data.extend(chunk)
    return bytes(data)
