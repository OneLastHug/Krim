"""Separate-origin ASGI proxy built on httpx and Starlette WebSocket APIs."""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from urllib.parse import parse_qs

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse
from starlette.types import Message, Receive, Scope, Send
from starlette.websockets import WebSocket, WebSocketDisconnect

from krim_agent.contracts import ServiceError
from krim_agent.development.preview_ports import PreviewRouteError, RouteLease
from krim_agent.security import PrincipalContext

from .boundary import (
    BOOTSTRAP_PATH,
    SESSION_COOKIE,
    PreviewOrigins,
    parse_cookies,
    request_headers,
    request_target,
    response_headers,
)
from .http_transport import ConnectedHTTPTransport
from .repository import BrowserGrant, PreviewCredentials, PreviewRecord, SocketConnector
from .service import PreviewError, PreviewService

_TOKEN = re.compile(r"[A-Za-z0-9_-]{40,128}\Z")
_METHODS = {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}


def digest(token: str) -> str:
    if not _TOKEN.fullmatch(token):
        raise PreviewError("preview_unauthorized", 401)
    return hashlib.sha256(token.encode()).hexdigest()


@dataclass(frozen=True)
class Bootstrap:
    """Trusted API response; never model/tool output or URL/query parameter."""

    action: str
    ticket: str = field(repr=False)
    expires_at: float
    allow_embedding: bool = False


@dataclass
class _Checks:
    """Coalesce stream-driven full rechecks with the watch cadence.

    Every chunk/message still verifies the in-memory browser grant. The full
    DB/authority/route recheck runs at most once per poll interval from the
    data path; the watch loop still runs one unconditionally each interval.
    """

    last_full: float = field(default_factory=time.monotonic)


async def race(*awaitables: Awaitable[None]) -> None:
    tasks = [asyncio.ensure_future(item) for item in awaitables]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        # Inspect all completed tasks so a simultaneous authorization failure is
        # not lost behind a normal socket close.
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class PreviewApp:
    """Only mount on the dedicated preview listener, never on the control app.

    TLS edge must preserve exact Host, reject direct untrusted traffic to its
    private backend, disable response buffering, and set a trusted https scheme.
    This app ignores forwarded headers. It has no anonymous proxy fallback.
    """

    def __init__(
        self,
        service: PreviewService,
        credentials: PreviewCredentials,
        origins: PreviewOrigins,
        sockets: SocketConnector,
        *,
        poll_seconds: float = 0.5,
        session_seconds: int = 3600,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not 0.01 <= poll_seconds <= 2 or not 60 <= session_seconds <= 3600:
            raise ValueError("invalid preview lifetime bounds")
        self.service, self.credentials, self.origins, self.sockets = (
            service,
            credentials,
            origins,
            sockets,
        )
        self.poll_seconds, self.session_seconds, self.clock = poll_seconds, session_seconds, clock

    async def issue_bootstrap(self, preview_id: str, principal: PrincipalContext) -> Bootstrap:
        """I calls only after existing API authentication and CSRF validation."""
        await self.service.check(preview_id, principal, action="open")
        ticket = secrets.token_urlsafe(32)
        expires = self.clock() + 30
        await self.credentials.put_ticket(
            digest(ticket), BrowserGrant(preview_id, principal, expires)
        )
        return Bootstrap(
            self.origins.origin(preview_id) + BOOTSTRAP_PATH, ticket, expires,
            allow_embedding=self.origins.allow_same_site_subdomains,
        )

    async def _resolve_host(self, scope: Scope) -> PreviewRecord:
        headers = scope["headers"]
        hosts = [v.decode("latin1") for k, v in headers if k.lower() == b"host"]
        if len(hosts) != 1 or scope["scheme"] not in {"https", "wss"}:
            raise PreviewError("preview_host_denied", 421)
        host = hosts[0]
        suffix = "." + self.origins.preview_domain
        prefix = self.origins.hostname_prefix + "-"
        if not host.startswith(prefix) or not host.endswith(suffix):
            raise PreviewError("preview_host_denied", 421)
        key = host[len(prefix) : -len(suffix)]
        if not re.fullmatch("[0-9a-f]{40}", key):
            raise PreviewError("preview_host_denied", 421)
        record = await self.service.repository.get_by_host_key(key)
        if host != self.origins.host(record.handle.preview_id):
            raise PreviewError("preview_host_denied", 421)
        return record

    def _origin(self, scope: Scope, expected: str, *, required: bool) -> None:
        origins = [v.decode("latin1") for k, v in scope["headers"] if k.lower() == b"origin"]
        if origins != [expected] and (required or origins):
            raise PreviewError("preview_origin_denied", 403)
        sites = [v for k, v in scope["headers"] if k.lower() == b"sec-fetch-site"]
        if expected != self.origins.control_origin and sites and sites != [b"same-origin"]:
            # Only a clean root navigation may follow the control-site ticket
            # exchange. Sibling previews cannot trigger arbitrary GET actions.
            modes = [v for k, v in scope["headers"] if k.lower() == b"sec-fetch-mode"]
            if not (
                scope.get("method") == "GET"
                and modes == [b"navigate"]
                and scope["path"] == "/"
                and not scope.get("query_string")
            ):
                raise PreviewError("preview_origin_denied", 403)

    async def _grant(self, scope: Scope, preview_id: str) -> BrowserGrant:
        token = parse_cookies(scope["headers"]).get(SESSION_COOKIE, "")
        grant = await self.credentials.get_session(digest(token), preview_id, self.clock())
        self._valid_grant(grant, preview_id)
        return grant

    def _valid_grant(self, grant: BrowserGrant, preview_id: str) -> None:
        if grant.preview_id != preview_id or grant.expires_at <= self.clock():
            raise PreviewError("preview_unauthorized", 401)

    async def _recheck(
        self,
        record: PreviewRecord,
        grant: BrowserGrant,
        lease: RouteLease,
        checks: _Checks,
        *,
        force: bool = False,
    ) -> None:
        # Exceptions never update last_full: a failed recheck is retried rather
        # than cached, matching the service's fail-closed behavior.
        if not force and time.monotonic() - checks.last_full < self.poll_seconds:
            return
        await self.service.recheck(record, grant.principal, lease)
        checks.last_full = time.monotonic()

    async def _bootstrap(self, scope: Scope, receive: Receive, send: Send, preview_id: str) -> None:
        if scope["method"] != "POST" or scope.get("query_string"):
            raise PreviewError("preview_bootstrap_denied", 405)
        self._origin(scope, self.origins.control_origin, required=True)
        request = Request(scope, receive)
        if request.headers.get("content-type", "").split(";", 1)[0] != (
            "application/x-www-form-urlencoded"
        ):
            raise PreviewError("preview_bootstrap_denied", 415)
        body = bytearray()
        async with asyncio.timeout(5):
            async for part in request.stream():
                body.extend(part)
                if len(body) > 512:
                    raise PreviewError("preview_bootstrap_denied", 413)
        try:
            fields = parse_qs(body.decode("ascii"), strict_parsing=True, max_num_fields=1)
        except (ValueError, UnicodeError):
            raise PreviewError("preview_bootstrap_denied", 400) from None
        if set(fields) != {"ticket"} or len(fields["ticket"]) != 1:
            raise PreviewError("preview_bootstrap_denied", 400)
        grant = await self.credentials.consume_ticket(
            digest(fields["ticket"][0]), preview_id, self.clock()
        )
        self._valid_grant(grant, preview_id)
        await self.service.check(preview_id, grant.principal, action="open")
        token = secrets.token_urlsafe(32)
        session = BrowserGrant(preview_id, grant.principal, self.clock() + self.session_seconds)
        await self.credentials.put_session(digest(token), session)
        response = RedirectResponse(
            "/",
            status_code=303,
            headers={
                "cache-control": "no-store",
                "referrer-policy": "no-referrer",
                "content-security-policy": (
                    "default-src 'none'; frame-ancestors "
                    + (self.origins.control_origin
                       if self.origins.allow_same_site_subdomains else "'none'")
                ),
            },
        )
        response.set_cookie(
            SESSION_COOKIE,
            token,
            max_age=self.session_seconds,
            httponly=True,
            secure=True,
            samesite="lax",
            path="/",
        )
        await response(scope, receive, send)

    async def _watch(
        self, record: PreviewRecord, grant: BrowserGrant, lease: RouteLease, checks: _Checks,
    ) -> None:
        while True:
            self._valid_grant(grant, record.handle.preview_id)
            # Loss of the control plane must close idle streams too.
            async with asyncio.timeout(5):
                await self._recheck(record, grant, lease, checks, force=True)
            await asyncio.sleep(self.poll_seconds)

    async def _http(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        record: PreviewRecord,
        grant: BrowserGrant,
    ) -> None:
        if scope["method"] not in _METHODS:
            raise PreviewError("preview_method_denied", 405)
        if any(k.lower() == b"service-worker" for k, _ in scope["headers"]):
            # A root-scoped worker could intercept the next ticket POST body.
            raise PreviewError("preview_worker_denied", 403)
        preview_id = record.handle.preview_id
        origin = self.origins.origin(preview_id)
        self._origin(scope, origin, required=scope["method"] not in {"GET", "HEAD"})
        target = request_target(
            scope.get("raw_path", scope["path"].encode()), scope.get("query_string", b"")
        )
        headers = request_headers(scope["headers"], self.origins.host(preview_id), origin)
        async with self.service.route(preview_id, grant.principal) as (current, lease):
            checks = _Checks()

            async def forward() -> None:
                # One client per downstream request: no shared auth defaults,
                # cookie jar, environment proxies or redirect following.
                host = self.origins.host(preview_id)
                async with (
                    self.service.routes.connect(lease) as sock,
                    httpx.AsyncClient(
                        trust_env=False,
                        follow_redirects=False,
                        transport=ConnectedHTTPTransport(sock, host),
                        timeout=httpx.Timeout(10, read=None, write=30, pool=5),
                    ) as client,
                ):
                    # URI supplies Host/target only; the transport consumes the
                    # already-connected B socket and has no DNS/dial fallback.
                    url = httpx.URL("http://" + host).copy_with(raw_path=target)
                    uploaded = asyncio.Event()
                    incoming = Request(scope, receive).stream()
                    first = await anext(incoming)
                    if not first:
                        uploaded.set()

                    async def body() -> AsyncIterator[bytes]:
                        yield first
                        async for part in incoming:
                            yield part
                        uploaded.set()

                    async with client.stream(
                        scope["method"],
                        url,
                        headers=headers,
                        content=body() if first else None,
                    ) as upstream:
                        await self._recheck(current, grant, lease, checks, force=True)
                        out_headers = response_headers(
                            upstream.headers.raw,
                            origin=origin,
                            upstream="http://" + host,
                            control_origin=self.origins.control_origin,
                            allow_embedding=self.origins.allow_same_site_subdomains,
                        )
                        await send(
                            {
                                "type": "http.response.start",
                                "status": upstream.status_code,
                                "headers": out_headers,
                            }
                        )

                        async def drain() -> None:
                            async for chunk in upstream.aiter_raw():
                                self._valid_grant(grant, preview_id)
                                await self._recheck(current, grant, lease, checks)
                                await send(
                                    {"type": "http.response.body", "body": chunk, "more_body": True}
                                )
                            await send({"type": "http.response.body", "body": b""})

                        async def disconnect() -> None:
                            # Ownership of receive transfers only after upload
                            # EOF; never race a body reader for request chunks.
                            await uploaded.wait()
                            while (await receive())["type"] != "http.disconnect":
                                pass

                        await race(drain(), disconnect())

            await race(forward(), self._watch(current, grant, lease, checks))

    async def _websocket(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        record: PreviewRecord,
        grant: BrowserGrant,
    ) -> None:
        preview_id = record.handle.preview_id
        origin = self.origins.origin(preview_id)
        self._origin(scope, origin, required=True)
        target = request_target(
            scope.get("raw_path", scope["path"].encode()), scope.get("query_string", b"")
        )
        headers = [
            (k.decode("ascii"), v.decode("latin1"))
            for k, v in request_headers(
                scope["headers"],
                self.origins.host(preview_id),
                origin,
            )
            if k not in {b"host", b"origin", b"content-length", b"content-type"}
        ]
        subprotocols = scope.get("subprotocols", [])
        if len(subprotocols) > 16 or any(
            len(p) > 128 or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9a-zA-Z-]+", p)
            for p in subprotocols
        ):
            raise PreviewError("preview_protocol_denied", 400)
        websocket = WebSocket(scope, receive, send)
        async with self.service.route(preview_id, grant.principal) as (current, lease):
            checks = _Checks()

            async def forward() -> None:
                async with self.sockets.connect(
                    lease,
                    target,
                    origin=origin,
                    headers=headers,
                    subprotocols=subprotocols,
                ) as upstream:
                    if (
                        upstream.subprotocol is not None
                        and upstream.subprotocol not in subprotocols
                    ):
                        raise PreviewError("preview_protocol_denied", 502)
                    await self._recheck(current, grant, lease, checks, force=True)
                    await websocket.accept(subprotocol=upstream.subprotocol)

                    async def inbound() -> None:
                        while True:
                            message = await websocket.receive()
                            if message["type"] == "websocket.disconnect":
                                return
                            value = message.get("bytes")
                            if value is None:
                                value = message.get("text", "")
                            size = (
                                len(value.encode("utf-8")) if isinstance(value, str) else len(value)
                            )
                            if size > 1_048_576:
                                raise PreviewError("preview_message_too_large", 413)
                            self._valid_grant(grant, preview_id)
                            await self._recheck(current, grant, lease, checks)
                            await upstream.send(value)

                    async def outbound() -> None:
                        async for value in upstream.messages():
                            size = (
                                len(value.encode("utf-8")) if isinstance(value, str) else len(value)
                            )
                            if size > 1_048_576:
                                raise PreviewError("preview_message_too_large", 413)
                            self._valid_grant(grant, preview_id)
                            await self._recheck(current, grant, lease, checks)
                            if isinstance(value, bytes):
                                await websocket.send_bytes(value)
                            else:
                                await websocket.send_text(value)

                    await race(inbound(), outbound())
                    await websocket.close(code=1000)

            await race(forward(), self._watch(current, grant, lease, checks))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                else:
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] not in {"http", "websocket"}:
            return
        started = False
        closed = False

        async def tracked(message: Message) -> None:
            nonlocal started, closed
            if message["type"] in {"http.response.start", "websocket.accept"}:
                started = True
            if message["type"] == "websocket.close" or (
                message["type"] == "http.response.body" and not message.get("more_body", False)
            ):
                closed = True
            await send(message)

        try:
            record = await self._resolve_host(scope)
            if scope["path"].startswith("/.krim-preview/"):
                if scope["type"] != "http" or scope["path"] != BOOTSTRAP_PATH:
                    raise PreviewError("preview_not_found", 404)
                await self._bootstrap(scope, receive, tracked, record.handle.preview_id)
                return
            grant = await self._grant(scope, record.handle.preview_id)
            if scope["type"] == "http":
                await self._http(scope, receive, tracked, record, grant)
            else:
                await self._websocket(scope, receive, tracked, record, grant)
        except WebSocketDisconnect:
            return
        except Exception as exc:
            # Do not log repr(exc): it may contain internal URLs or headers.
            status = exc.status if isinstance(exc, (PreviewError, ServiceError)) else 502
            code = exc.code if isinstance(exc, PreviewError) else "preview_unavailable"
            if isinstance(exc, PreviewRouteError):
                status = {
                    "preview_route_unavailable": 503,
                    "preview_route_invalidated": 410,
                    "preview_route_capacity": 503,
                    "preview_route_denied": 403,
                }.get(exc.code, 503)
                code = exc.code
            if closed:
                return
            if scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1008 if status < 500 else 1011})
            elif not started:
                await JSONResponse(
                    {"error": code},
                    status_code=status,
                    headers={
                        "cache-control": "no-store",
                        "referrer-policy": "no-referrer",
                    },
                )(scope, receive, send)
            else:
                # Propagate a sanitized exception so ASGI aborts the incomplete
                # stream instead of marking a truncated download successful.
                raise PreviewError("preview_stream_closed", status) from None
