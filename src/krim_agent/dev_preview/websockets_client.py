"""Optional websockets 15 client; I owns dependency installation/locking."""

from __future__ import annotations

import importlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlsplit

from krim_agent.development.preview_ports import PreviewRoutes, RouteLease

from .repository import UpstreamSocket
from .service import PreviewError

_LOG = logging.Logger("krim.preview.websocket.private")
_LOG.addHandler(logging.NullHandler())
_LOG.propagate = False
_LOG.disabled = True


class _Socket:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    @property
    def subprotocol(self) -> str | None:
        value = self.connection.subprotocol
        return str(value) if value is not None else None

    async def messages(self) -> AsyncIterator[str | bytes]:
        async for message in self.connection:
            if not isinstance(message, (str, bytes)):
                raise PreviewError("preview_protocol_denied", 502)
            yield message

    async def send(self, message: str | bytes) -> None:
        await self.connection.send(message)


class WebsocketsConnector:
    """Use B's socket; public URI controls handshake Host only, never dialing.

    websockets 15 rejects redirects with a preexisting socket. No DNS/default
    network/proxy/retry path is provided by E. Socketpair compatibility must be
    verified with the installed version before deployment.
    """

    def __init__(self, routes: PreviewRoutes) -> None:
        self.routes = routes

    @asynccontextmanager
    async def connect(
        self,
        lease: RouteLease,
        target: bytes,
        *,
        origin: str,
        headers: list[tuple[str, str]],
        subprotocols: list[str],
    ) -> AsyncIterator[UpstreamSocket]:
        try:
            client = importlib.import_module("websockets.asyncio.client")
        except ImportError:
            raise PreviewError("preview_websocket_unavailable", 503) from None
        public = urlsplit(origin)
        if (
            public.scheme != "https"
            or not public.hostname
            or public.path
            or public.username
            or public.password
            or public.query
            or public.fragment
        ):
            raise PreviewError("preview_route_invalid", 503)
        url = "ws://" + public.netloc + target.decode("ascii")
        async with self.routes.connect(lease) as sock:
            try:
                async with client.connect(
                    url,
                    sock=sock,
                    origin=origin,
                    additional_headers=headers,
                    subprotocols=subprotocols,
                    proxy=None,
                    logger=_LOG,
                    open_timeout=10,
                    close_timeout=2,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_048_576,
                    max_queue=16,
                    compression=None,
                ) as connection:
                    yield _Socket(connection)
            finally:
                # The protocol context closes its asyncio transport first. On
                # constructor/handshake failure the handed-off endpoint must also
                # close before B reaps its peer/relay in the outer context.
                sock.close()
