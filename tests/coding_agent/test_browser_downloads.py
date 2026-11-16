"""Real httpcore/httpcore over socketpairs; no Docker, browser, DNS or model calls."""

import asyncio
import hashlib
import socket
from contextlib import asynccontextmanager

import httpcore
import pytest

from krim_agent.contracts import ServiceError
from krim_agent.dev_preview.boundary import SESSION_COOKIE
from krim_agent.development.browser_downloads import MAX_DOWNLOAD_BYTES, inspect_download


class Routes:
    def __init__(self, reply):
        self.reply = reply
        self.request = b""
        self.closed = False
        self.started = asyncio.Event()

    @asynccontextmanager
    async def connect(self, lease):
        owner = asyncio.current_task()
        client, peer = socket.socketpair()
        client.setblocking(False)
        peer.setblocking(False)
        loop = asyncio.get_running_loop()

        async def serve():
            while b"\r\n\r\n" not in self.request:
                part = await loop.sock_recv(peer, 4096)
                if not part:
                    return
                self.request += part
            self.started.set()
            await loop.sock_sendall(peer, self.reply)
            peer.shutdown(socket.SHUT_WR)

        server = asyncio.create_task(serve())
        try:
            yield client
        finally:
            assert asyncio.current_task() is owner
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)
            client.close()
            peer.close()
            self.closed = True


def response(body=b"image-content", *, headers=b"", status=b"200 OK"):
    return b"HTTP/1.1 " + status + b"\r\nConnection: close\r\n" + headers + b"\r\n" + body


async def inspect(routes, *, mode="context", cookie="app=private", authorize=None):
    async def check():
        pass

    return await inspect_download(
        routes,
        object(),
        authorize or check,
        host="p.preview.example",
        origin="https://p.preview.example",
        path="/images/a.png",
        cookie_mode=mode,
        descriptor={"path": "/images/a.png", "cookie_mode": mode, "cookie_header": cookie},
    )


async def test_real_stream_hash_and_cookie_filtering_without_dns(monkeypatch):
    loop = asyncio.get_running_loop()

    async def forbidden(*args, **kwargs):
        pytest.fail("download transport must never dial or resolve")

    monkeypatch.setattr(loop, "getaddrinfo", forbidden)
    monkeypatch.setattr(loop, "sock_connect", forbidden)
    body = b"x" * 200000
    routes = Routes(response(body, headers=b"Content-Type: image/png\r\n"))
    result = await inspect(routes, cookie=f"{SESSION_COOKIE}=infra; app=private")
    assert result == {
        "status": 200,
        "media_type": "image/png",
        "size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }
    assert b"cookie: app=private\r\n" in routes.request
    assert SESSION_COOKIE.encode() not in routes.request
    assert b"accept-encoding: identity\r\n" in routes.request
    assert routes.closed and "private" not in str(result)


async def test_preview_only_download_is_anonymous_to_application():
    routes = Routes(
        response(b"public", headers=b"Content-Length: 6\r\nContent-Type: image/png\r\n")
    )
    result = await inspect(routes, mode="preview_only", cookie="")
    assert result["status"] == 200 and result["size"] == 6
    assert b"cookie:" not in routes.request.lower()
    assert routes.closed


async def test_application_denial_status_is_preserved_without_body_exposure():
    routes = Routes(response(b"login required", status=b"401 Unauthorized"))
    result = await inspect(routes, mode="preview_only", cookie="")
    assert result["status"] == 401 and result["size"] == 14
    assert set(result) == {"status", "media_type", "size", "sha256"}


@pytest.mark.parametrize(
    "headers,status",
    [
        (b"Content-Encoding: gzip\r\n", b"200 OK"),
        (b"Content-Encoding: identity, gzip\r\n", b"200 OK"),
        (b"Content-Length: 1048577\r\n", b"200 OK"),
        (b"Content-Type: bad-type\r\n", b"200 OK"),
        (b"Location: https://external.invalid/\r\n", b"302 Found"),
    ],
)
async def test_response_limits_and_redirects_fail_closed(headers, status):
    routes = Routes(response(headers=headers, status=status))
    with pytest.raises(ServiceError):
        await inspect(routes)
    assert routes.closed


async def test_chunked_overflow_closes_response_before_collecting_whole_body():
    body = b"x" * (MAX_DOWNLOAD_BYTES + 65536)
    wire = hex(len(body))[2:].encode() + b"\r\n" + body + b"\r\n0\r\n\r\n"
    routes = Routes(response(wire, headers=b"Transfer-Encoding: chunked\r\n"))
    with pytest.raises(ServiceError, match="browser_output_limit"):
        await inspect(routes)
    assert routes.closed


async def test_partial_body_does_not_return_success_metadata():
    routes = Routes(response(b"short", headers=b"Content-Length: 99\r\n"))
    with pytest.raises(httpcore.RemoteProtocolError):
        await inspect(routes)
    assert routes.closed


async def test_revoke_during_stream_prevents_success_and_closes_socket():
    routes = Routes(response(b"x" * 90000))
    checks = 0

    async def authorize():
        nonlocal checks
        checks += 1
        if checks == 3:
            raise ServiceError("permission_denied")

    with pytest.raises(ServiceError, match="permission_denied"):
        await inspect(routes, authorize=authorize)
    assert routes.closed


@pytest.mark.parametrize(
    "mode,cookie",
    [
        ("preview_only", "app=private"),
        ("context", "app=x\r\nX-Injected: y"),
        ("context", "a" * 8193),
        ("invalid", ""),
    ],
)
async def test_bad_private_descriptor_never_connects(mode, cookie):
    routes = Routes(response())
    with pytest.raises(ServiceError, match="browser_protocol"):
        await inspect(routes, mode=mode, cookie=cookie)
    assert not routes.request and not routes.closed
