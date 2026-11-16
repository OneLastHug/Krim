"""Bounded download inspection through an existing viewer-bound preview lease."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from krim_agent.contracts import ServiceError
from krim_agent.dev_browser.policy import relative_target
from krim_agent.dev_preview.boundary import request_headers
from krim_agent.dev_preview.http_transport import ConnectedHTTPTransport
from krim_agent.development.preview_ports import PreviewRoutes, RouteLease

MAX_DOWNLOAD_BYTES = 1024 * 1024
_MEDIA = re.compile(r"[A-Za-z0-9!#$&^_.+\-]+/[A-Za-z0-9!#$&^_.+\-]+\Z")


async def inspect_download(
    routes: PreviewRoutes,
    lease: RouteLease,
    authorize: Callable[[], Awaitable[None]],
    *,
    host: str,
    origin: str,
    path: str,
    cookie_mode: str,
    descriptor: dict[str, Any],
) -> dict[str, Any]:
    """The descriptor is private worker IPC, not model-selected HTTP headers.

    The original viewer/preview/epoch remains owned by the caller's lease. Only
    application cookies can be forwarded; preview_only forwards no app cookies.
    No response body, cookies, filename or Location enters the returned metadata.
    """
    target = relative_target(path)
    if (
        cookie_mode not in {"context", "preview_only"}
        or set(descriptor) != {"path", "cookie_mode", "cookie_header"}
        or descriptor["path"] != target
        or descriptor["cookie_mode"] != cookie_mode
    ):
        raise ServiceError("browser_protocol", 503)
    cookie = descriptor["cookie_header"]
    if (
        not isinstance(cookie, str)
        or len(cookie) > 8192
        or any(not 32 <= ord(char) < 127 for char in cookie)
        or cookie_mode == "preview_only"
        and cookie
    ):
        raise ServiceError("browser_protocol", 503)
    incoming = [(b"accept-encoding", b"identity")]
    if cookie:
        incoming.append((b"cookie", cookie.encode("ascii")))
    # Reuse E's cookie translation: it strips infrastructure credentials even
    # if a compromised worker includes one in its otherwise valid descriptor.
    headers = request_headers(incoming, host, origin)
    await authorize()
    async with (
        routes.connect(lease) as sock,
        httpx.AsyncClient(
            trust_env=False,
            follow_redirects=False,
            transport=ConnectedHTTPTransport(sock, host),
            timeout=httpx.Timeout(3, read=3, write=3, pool=3),
        ) as client,
    ):
        url = httpx.URL("http://" + host + target)
        async with client.stream("GET", url, headers=headers) as response:
            await authorize()
            encodings = response.headers.get_list("content-encoding")
            lengths = response.headers.get_list("content-length")
            media = response.headers.get_list("content-type")
            if (
                300 <= response.status_code < 400
                or encodings
                and [value.strip().lower() for value in encodings] != ["identity"]
                or len(lengths) > 1
                or len(media) > 1
            ):
                raise ServiceError("browser_artifact_invalid", 422)
            if lengths and (
                not re.fullmatch(r"[0-9]{1,10}", lengths[0]) or int(lengths[0]) > MAX_DOWNLOAD_BYTES
            ):
                raise ServiceError("browser_output_limit", 422)
            media_type = media[0].partition(";")[0].strip() if media else "application/octet-stream"
            if len(media_type) > 128 or not _MEDIA.fullmatch(media_type):
                raise ServiceError("browser_artifact_invalid", 422)
            digest, size = hashlib.sha256(), 0
            # Raw chunks disable decompression; the stream and connected socket
            # are closed on overflow, cancellation, malformed framing or revoke.
            async for chunk in response.aiter_raw(chunk_size=65536):
                await authorize()
                if len(chunk) > MAX_DOWNLOAD_BYTES - size:
                    raise ServiceError("browser_output_limit", 422)
                digest.update(chunk)
                size += len(chunk)
            if lengths and size != int(lengths[0]):
                raise ServiceError("browser_artifact_invalid", 422)
            await authorize()
            return {
                "status": response.status_code,
                "media_type": media_type,
                "size": size,
                "sha256": digest.hexdigest(),
            }
