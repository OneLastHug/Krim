"""ASGI/message fixtures; no Docker, DNS, TLS edge or real WebSocket transport."""

from __future__ import annotations

import asyncio
import hashlib
import socket
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import h11
import httpx
import pytest

from krim_agent.contracts import ServiceError
from krim_agent.dev_preview.app import PreviewApp
from krim_agent.dev_preview.boundary import (
    BOOTSTRAP_PATH,
    SESSION_COOKIE,
    PreviewOrigins,
    request_target,
    response_headers,
)
from krim_agent.dev_preview.service import PreviewError, PreviewService
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord, ProcessRecord, command_digest
from krim_agent.development.preview_ports import PreviewRouteError, RouteLease, ServiceInstance
from krim_agent.security import PrincipalContext


class Repository:
    def __init__(self):
        self.rows = {}

    async def create(self, row):
        assert row.handle.preview_id not in self.rows
        self.rows[row.handle.preview_id] = row
        return row

    async def get(self, key):
        if key not in self.rows:
            raise ServiceError("not_found", 404)
        return self.rows[key]

    async def get_by_host_key(self, key):
        for pid in self.rows:
            if hashlib.sha256(pid.encode()).hexdigest()[:40] == key:
                return self.rows[pid]
        raise ServiceError("not_found", 404)

    async def save(self, old, new):
        if self.rows[old.handle.preview_id] != old:
            raise ServiceError("revision_conflict")
        assert new.revision == old.revision + 1
        assert (
            replace(old, handle=new.handle, revision=new.revision, route_epoch=new.route_epoch)
            == new
        )
        invalidating = new.handle.state in {"stopped", "revoked"}
        assert new.route_epoch == old.route_epoch + int(invalidating)
        assert old.handle.model_copy(update={"state": new.handle.state}) == new.handle
        self.rows[old.handle.preview_id] = new
        return new


class Credentials:
    def __init__(self):
        self.tickets, self.sessions = {}, {}

    async def put_ticket(self, key, grant):
        self.tickets[key] = grant

    async def consume_ticket(self, key, pid, now):
        grant = self.tickets.get(key)
        if not grant or grant.preview_id != pid or grant.expires_at <= now:
            raise ServiceError("unauthorized", 401)
        return self.tickets.pop(key)

    async def put_session(self, key, grant):
        self.sessions[key] = grant

    async def get_session(self, key, pid, now):
        grant = self.sessions.get(key)
        if not grant or grant.preview_id != pid or grant.expires_at <= now:
            raise ServiceError("unauthorized", 401)
        return grant


class Authority:
    revoked = False

    async def check(self, record, principal, action):
        if self.revoked or (
            principal is not None
            and (principal.principal_id != "owner" or principal.generation != 1)
        ):
            raise ServiceError("permission_denied", 403)


class Routes:
    """Fake control plane, real nonblocking socketpair + h11 HTTP fixture peer."""

    def __init__(self):
        self.active, self.is_ready, self.closed, self.acquired = True, True, 0, 0
        self.started_at = "2026-11-15T00:00:00.000000001Z"
        self.transport = httpx.MockTransport(lambda request: httpx.Response(204))
        self.connected = 0
        self.socket_closed = 0
        self.principals = []
        self.wire_headers = []

    async def observe(self, process, port, principal):
        return ServiceInstance(execution_id="a" * 64, started_at=self.started_at)

    @asynccontextmanager
    async def acquire(self, binding, principal):
        self.acquired += 1
        self.principals.append(principal)
        lease = RouteLease(binding, "lease")
        await self.check(lease)
        try:
            yield lease
        finally:
            self.closed += 1

    async def check(self, lease):
        if not self.active or lease.binding.started_at != self.started_at:
            raise PreviewError("preview_inactive", 410)

    async def ready(self, lease):
        return self.is_ready

    @asynccontextmanager
    async def connect(self, lease):
        await self.check(lease)
        owner = asyncio.current_task()
        client, peer = socket.socketpair()
        client.setblocking(False)
        peer.setblocking(False)
        self.connected += 1
        task = asyncio.create_task(self.serve(peer))
        try:
            yield client
        finally:
            assert asyncio.current_task() is owner
            assert client.fileno() == -1, "transport must close before B context exits"
            self.socket_closed += 1
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            peer.close()

    async def serve(self, peer):
        connection = h11.Connection(h11.SERVER)
        loop = asyncio.get_running_loop()
        request_event = None
        chunks = []
        response = None
        try:
            while True:
                event = connection.next_event()
                if event is h11.NEED_DATA:
                    data = await loop.sock_recv(peer, 65536)
                    connection.receive_data(data)
                elif isinstance(event, h11.Request):
                    request_event = event
                elif isinstance(event, h11.Data):
                    chunks.append(event.data)
                elif isinstance(event, h11.EndOfMessage):
                    break
                elif isinstance(event, h11.ConnectionClosed):
                    return
            assert request_event is not None
            headers = httpx.Headers(request_event.headers)
            self.wire_headers.append(headers)
            url = "http://" + headers["host"] + request_event.target.decode()

            async def body():
                for chunk in chunks:
                    yield chunk

            request = httpx.Request(
                request_event.method.decode(), url, headers=headers, content=body()
            )
            response = await self.transport.handle_async_request(request)
            await loop.sock_sendall(
                peer,
                connection.send(
                    h11.Response(status_code=response.status_code, headers=response.headers.raw)
                ),
            )
            if not response.is_stream_consumed:
                async for data in response.aiter_raw():
                    if data:
                        await loop.sock_sendall(peer, connection.send(h11.Data(data=data)))
            elif response.content:
                await loop.sock_sendall(peer, connection.send(h11.Data(data=response.content)))
            await loop.sock_sendall(peer, connection.send(h11.EndOfMessage()))
        finally:
            if response is not None:
                await response.aclose()
            peer.close()


class Process:
    def __init__(self, row):
        self.row, self.cancelled = row, False

    async def get(self, process_id):
        if process_id != self.row.handle.process_id:
            raise ServiceError("not_found", 404)
        return self.row

    async def inspect(self, handle):
        return self.row.handle

    async def cancel(self, handle):
        self.cancelled = True
        self.row = replace(
            self.row, handle=self.row.handle.model_copy(update={"state": "cancelled"})
        )
        return self.row.handle


class Environments:
    def __init__(self, environment):
        self.environment = environment

    async def resolve(self, handle):
        return EnvironmentRecord(self.environment, "sha256:image", Path("/project"), 1, 2)


class Sockets:
    def __init__(self):
        self.received = []
        self.queue = asyncio.Queue()
        self.closed = False
        self.subprotocol = "vite-hmr"
        self.connect_args = None

    @asynccontextmanager
    async def connect(self, lease, target, **kwargs):
        self.connect_args = (lease, target, kwargs)
        try:
            yield self
        finally:
            self.closed = True

    async def messages(self):
        while True:
            yield await self.queue.get()

    async def send(self, value):
        self.received.append(value)
        await self.queue.put(value)


@pytest.fixture
async def rig():
    env = EnvironmentHandle(
        owner_id="owner", project_id="project", environment_id="env", generation=1, state="ready"
    )
    request = CommandRequest(
        operation_id="op",
        environment=env,
        ownership=RunOwnership(owner_id="owner", session_id="session", run_id="run"),
        authorization=AuthorizationRef(
            actor_id="owner", actor_generation=1, project_id="project", arguments_digest="0" * 64
        ),
        command="python fixture.py",
        lifecycle="project_service",
        timeout_seconds=None,
    )
    handle = ProcessHandle(
        process_id="process",
        operation_id="op",
        environment=env,
        ownership=request.ownership,
        lifecycle="project_service",
        state="running",
        log_ref="log",
    )
    process = Process(
        ProcessRecord(request, handle, command_digest(request), execution_id="a" * 64)
    )
    repo, authority, routes, credentials, sockets = (
        Repository(),
        Authority(),
        Routes(),
        Credentials(),
        Sockets(),
    )
    environments = Environments(env)
    service = PreviewService(repo, process, environments, process, authority, routes)
    principal = PrincipalContext("owner", 1, ())
    origins = PreviewOrigins("https://krim.example.com", "example.com", "preview.example.net")
    clock = [100.0]
    app = PreviewApp(
        service, credentials, origins, sockets, poll_seconds=0.01, clock=lambda: clock[0]
    )
    await service.register("preview", handle, 3000, principal)
    return SimpleNamespace(**locals())


async def login(rig, client):
    bootstrap = await rig.app.issue_bootstrap("preview", rig.principal)
    response = await client.post(
        BOOTSTRAP_PATH,
        data={"ticket": bootstrap.ticket},
        headers={"origin": rig.origins.control_origin},
    )
    assert response.status_code == 303
    return response


def client_for(rig):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=rig.app), base_url=rig.origins.origin("preview")
    )


async def test_ticket_single_use_host_bound_and_cookie_scope(rig):
    async with client_for(rig) as client:
        bootstrap = await rig.app.issue_bootstrap("preview", rig.principal)
        wrong = await client.post(
            BOOTSTRAP_PATH,
            data={"ticket": bootstrap.ticket},
            headers={"origin": "https://evil.example.net"},
        )
        assert wrong.status_code == 403
        response = await client.post(
            BOOTSTRAP_PATH,
            data={"ticket": bootstrap.ticket},
            headers={"origin": rig.origins.control_origin},
        )
        assert response.status_code == 303 and response.headers["location"] == "/"
        cookie = response.headers["set-cookie"]
        assert "Secure" in cookie and "HttpOnly" in cookie and "Domain=" not in cookie
        assert bootstrap.ticket not in cookie
        replay = await client.post(
            BOOTSTRAP_PATH,
            data={"ticket": bootstrap.ticket},
            headers={"origin": rig.origins.control_origin},
        )
        assert replay.status_code == 401
        assert bootstrap.ticket not in repr(bootstrap)


async def test_expired_ticket_and_revoked_creator(rig):
    async with client_for(rig) as client:
        bootstrap = await rig.app.issue_bootstrap("preview", rig.principal)
        rig.clock[0] += 31
        response = await client.post(
            BOOTSTRAP_PATH,
            data={"ticket": bootstrap.ticket},
            headers={"origin": rig.origins.control_origin},
        )
        assert response.status_code == 401
        rig.authority.revoked = True
        with pytest.raises(ServiceError):
            await rig.app.issue_bootstrap("preview", rig.principal)


async def test_wrong_host_and_no_auth_never_acquire_route(rig):
    async with client_for(rig) as client:
        assert (await client.get("/")).status_code == 401
        assert (await client.get("/", headers={"host": "krim.example.com"})).status_code == 421
        assert rig.routes.acquired == 0


class EmptyStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b""


async def test_empty_get_does_not_invent_transfer_encoding(rig):
    seen = []

    async def upstream(request):
        seen.append(request)
        assert await request.aread() == b""
        return httpx.Response(200, content=b"bodyless-get")

    rig.routes.transport = httpx.MockTransport(upstream)
    async with client_for(rig) as client:
        await login(rig, client)
        response = await client.get("/")
    assert response.status_code == 200 and response.content == b"bodyless-get"
    assert len(seen) == 1 and len(rig.routes.wire_headers) == 1
    assert "transfer-encoding" not in rig.routes.wire_headers[0]
    assert "content-length" not in rig.routes.wire_headers[0]


async def test_http_headers_upload_cookies_and_redirects(rig):
    seen = []

    async def upstream(request):
        seen.append((request, await request.aread()))
        return httpx.Response(
            302,
            stream=EmptyStream(),
            headers=[
                ("location", "http://" + rig.origins.host("preview") + "/done"),
                ("set-cookie", "app=yes; Domain=.example.net; Path=/"),
                ("set-cookie", f"{SESSION_COOKIE}=bad; Path=/"),
                ("connection", "x-private"),
                ("x-private", "secret"),
            ],
        )

    rig.routes.transport = httpx.MockTransport(upstream)
    async with client_for(rig) as client:
        await login(rig, client)
        client.cookies.set("app", "own")
        response = await client.post(
            "/upload?q=1",
            content=b"raw bytes",
            headers={
                "origin": rig.origins.origin("preview"),
                "authorization": "Bearer control-secret",
                "x-forwarded-host": "169.254.169.254",
                "x-krim-key": "secret",
                "connection": "x-secret",
                "x-secret": "value",
                "content-type": "image/png",
            },
        )
        assert response.status_code == 302
        request, body = seen[0]
        assert body == b"raw bytes" and request.url.raw_path == b"/upload?q=1"
        assert request.headers["host"] == rig.origins.host("preview")
        assert request.headers["cookie"] == "app=own"
        for header in ("authorization", "x-forwarded-host", "x-krim-key", "x-secret"):
            assert header not in request.headers
        assert response.headers["location"] == rig.origins.origin("preview") + "/done"
        assert "x-private" not in response.headers
        assert response.headers.get_list("set-cookie") == ["app=yes; Path=/; Secure"]


@pytest.mark.parametrize(
    "change", ["revoked", "generation", "exited", "stopping", "execution", "accepted"]
)
async def test_lifecycle_invalidates_existing_session(rig, change):
    async with client_for(rig) as client:
        await login(rig, client)
        if change == "revoked":
            rig.authority.revoked = True
        elif change == "generation":
            rig.environments.environment = rig.env.model_copy(update={"generation": 2})
        elif change == "exited":
            rig.process.row = replace(
                rig.process.row,
                handle=rig.handle.model_copy(update={"state": "exited", "exit_code": 0}),
            )
        elif change == "stopping":
            rig.environments.environment = rig.env.model_copy(update={"state": "stopping"})
        elif change == "accepted":
            rig.process.row = replace(
                rig.process.row, handle=rig.handle.model_copy(update={"state": "accepted"})
            )
        else:
            rig.process.row = replace(rig.process.row, execution_id="other")
        assert (await client.get("/")).status_code in {403, 409, 410}
        assert rig.routes.acquired == 0


async def test_readiness_timeout_and_stop_fences_first(rig):
    rig.routes.is_ready = False
    with pytest.raises(PreviewError, match="preview_not_ready"):
        await rig.service.wait_ready("preview", wait_seconds=0.02)
    rig.routes.is_ready = True
    assert (await rig.service.wait_ready("preview")).state == "ready"
    assert (await rig.service.stop("preview", rig.principal)).state == "stopped"
    assert rig.process.cancelled
    with pytest.raises(PreviewError, match="preview_inactive"):
        await rig.service.check("preview", rig.principal)


@pytest.mark.parametrize("path", [b"//evil/a", b"/%2Fevil/a", b"/%5cevil", b"/\x00"])
def test_target_cannot_change_authority(path):
    with pytest.raises(PreviewError):
        request_target(path, b"")


@pytest.mark.parametrize("location", ["http://169.254.169.254/latest", "//evil/a", "/%5c/evil"])
def test_redirect_cannot_escape(location):
    with pytest.raises(PreviewError):
        response_headers(
            [(b"location", location.encode())],
            origin="https://p.example.net",
            upstream="http://127.0.0.1:45678",
            control_origin="https://krim.example.com",
        )


def test_reject_control_cookie_domain_overlap():
    with pytest.raises(ValueError):
        PreviewOrigins("https://krim.example.com", "example.com", "preview.example.com")


async def socket_scope(rig):
    async with client_for(rig) as client:
        await login(rig, client)
        cookie = "; ".join(f"{k}={v}" for k, v in client.cookies.items())
    return {
        "type": "websocket",
        "scheme": "wss",
        "path": "/hmr",
        "raw_path": b"/hmr",
        "query_string": b"v=1",
        "subprotocols": ["vite-hmr"],
        "headers": [
            (b"host", rig.origins.host("preview").encode()),
            (b"origin", rig.origins.origin("preview").encode()),
            (b"cookie", cookie.encode()),
            (b"authorization", b"Bearer secret"),
        ],
    }


async def test_websocket_text_binary_hmr_and_revoke(rig):
    scope = await socket_scope(rig)
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    await incoming.put({"type": "websocket.connect"})
    task = asyncio.create_task(rig.app(scope, incoming.get, outgoing.put))
    async with asyncio.timeout(2):
        assert await outgoing.get() == {
            "type": "websocket.accept",
            "subprotocol": "vite-hmr",
            "headers": [],
        }
        for value in ("update", b"binary"):
            kind = "text" if isinstance(value, str) else "bytes"
            await incoming.put({"type": "websocket.receive", kind: value})
            assert await outgoing.get() == {"type": "websocket.send", kind: value}
        rig.authority.revoked = True
        assert (await outgoing.get())["type"] == "websocket.close"
        await task
    assert rig.sockets.closed and rig.routes.closed
    assert rig.sockets.received == ["update", b"binary"]
    _, target, kwargs = rig.sockets.connect_args
    assert target == b"/hmr?v=1"
    assert not any(k in {"cookie", "authorization"} for k, _ in kwargs["headers"])


async def test_websocket_rejects_sibling_origin(rig):
    scope = await socket_scope(rig)
    scope["headers"] = [
        (k, b"https://sibling.preview.example.net" if k == b"origin" else v)
        for k, v in scope["headers"]
    ]
    output = []

    async def receive():
        return {"type": "websocket.connect"}

    async def send(message):
        output.append(message)

    await rig.app(scope, receive, send)
    assert output == [{"type": "websocket.close", "code": 1008}]
    assert rig.sockets.connect_args is None


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self):
        self.next_chunk = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        yield b"first"
        await self.next_chunk.wait()
        yield b"second"

    async def aclose(self):
        self.closed = True


async def test_http_stream_delivers_before_completion_and_revokes(rig):
    stream = ChunkStream()
    rig.routes.transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
    ws = await socket_scope(rig)
    scope = {**ws, "type": "http", "scheme": "https", "method": "GET", "http_version": "1.1"}
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    await incoming.put({"type": "http.request", "body": b"", "more_body": False})
    task = asyncio.create_task(rig.app(scope, incoming.get, outgoing.put))
    async with asyncio.timeout(2):
        assert (await outgoing.get())["type"] == "http.response.start"
        assert await outgoing.get() == {
            "type": "http.response.body",
            "body": b"first",
            "more_body": True,
        }
        assert not task.done() and not stream.next_chunk.is_set()
        rig.authority.revoked = True
        with pytest.raises(PreviewError, match="preview_stream_closed"):
            await task
    assert stream.closed and outgoing.empty()


async def test_http_disconnect_closes_upstream(rig):
    stream = ChunkStream()
    rig.routes.transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
    scope = {
        **(await socket_scope(rig)),
        "type": "http",
        "scheme": "https",
        "method": "GET",
        "http_version": "1.1",
    }
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    await incoming.put({"type": "http.request", "body": b"", "more_body": False})
    task = asyncio.create_task(rig.app(scope, incoming.get, outgoing.put))
    async with asyncio.timeout(2):
        await outgoing.get()
        await outgoing.get()
        await incoming.put({"type": "http.disconnect"})
        await task
    assert stream.closed


async def test_worker_registration_and_cross_site_action_get_are_rejected(rig):
    async with client_for(rig) as client:
        await login(rig, client)
        worker = await client.get("/worker.js", headers={"service-worker": "script"})
        action = await client.get(
            "/delete?id=1",
            headers={
                "sec-fetch-site": "same-site",
                "sec-fetch-mode": "navigate",
            },
        )
        assert worker.status_code == action.status_code == 403
        assert rig.routes.acquired == 0


async def test_oserror_is_sanitized_before_headers(rig):
    async def fail(binding, principal):
        raise OSError("private-host-and-credential")
        yield

    rig.routes.acquire = asynccontextmanager(fail)
    async with client_for(rig) as client:
        await login(rig, client)
        response = await client.get("/")
        assert response.status_code == 502
        assert "private-host" not in response.text


async def test_readiness_cannot_resurrect_revoked_record(rig):
    check = rig.routes.check
    checks = 0

    async def revoke_during_check(lease):
        nonlocal checks
        checks += 1
        await check(lease)
        if checks == 2:
            await rig.service.revoke("preview")

    rig.routes.check = revoke_during_check
    with pytest.raises(PreviewError, match="preview_binding_drift"):
        await rig.service.wait_ready("preview")
    assert (await rig.repo.get("preview")).handle.state == "revoked"
    assert (await rig.repo.get("preview")).route_epoch == 2


async def test_multiple_upload_chunks_preserved(rig):
    received = []

    class UploadTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            async for chunk in request.stream:
                received.append(chunk)
                await asyncio.sleep(0)
            return httpx.Response(204, stream=EmptyStream())

    rig.routes.transport = UploadTransport()
    scope = {
        **(await socket_scope(rig)),
        "type": "http",
        "scheme": "https",
        "method": "POST",
        "http_version": "1.1",
    }
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    task = asyncio.create_task(rig.app(scope, incoming.get, outgoing.put))
    async with asyncio.timeout(2):
        for value in (b"one", b"two", b"three"):
            await incoming.put({"type": "http.request", "body": value, "more_body": True})
            await asyncio.sleep(0)
        await incoming.put({"type": "http.request", "body": b"end", "more_body": False})
        await task
    assert b"".join(received) == b"onetwothreeend"
    assert (await outgoing.get())["status"] == 204


async def test_cross_preview_ticket_and_cookie_rejected(rig):
    await rig.service.register("other", rig.handle, 3001, rig.principal)
    async with client_for(rig) as client:
        bootstrap = await rig.app.issue_bootstrap("preview", rig.principal)
        wrong = await client.post(
            rig.origins.origin("other") + BOOTSTRAP_PATH,
            data={"ticket": bootstrap.ticket},
            headers={"origin": rig.origins.control_origin},
        )
        assert wrong.status_code == 401
        await login(rig, client)
        token = client.cookies.get(SESSION_COOKIE)
        wrong_session = await client.get(
            rig.origins.origin("other") + "/", headers={"cookie": f"{SESSION_COOKIE}={token}"}
        )
        assert wrong_session.status_code == 401
        assert rig.routes.acquired == 0


def test_upstream_cannot_delegate_transport_or_hop_nominated_cookies():
    headers = response_headers(
        [
            (b"alt-svc", b'h3="internal.example:443"'),
            (b"connection", b"set-cookie, location"),
            (b"set-cookie", b"app=bad"),
            (b"location", b"/bad"),
            (b"service-worker-allowed", b"/"),
        ],
        origin="https://p.preview.example.net",
        upstream="http://127.0.0.1:45678",
        control_origin="https://krim.example.com",
    )
    assert not any(
        k in {b"alt-svc", b"set-cookie", b"location", b"service-worker-allowed"} for k, _ in headers
    )


async def test_process_route_and_owner_forgery_rejected(rig):
    for field, value in (
        ("owner_id", "other"),
        ("project_id", "other"),
        ("environment_id", "other"),
        ("generation", 9),
    ):
        handle = rig.handle.model_copy(
            update={
                "environment": rig.env.model_copy(update={field: value}),
            }
        )
        with pytest.raises(PreviewError, match="preview_binding_drift"):
            await rig.service.register("forged", handle, 3000, rig.principal)
    original = rig.routes.acquire

    @asynccontextmanager
    async def wrong_route(binding, principal):
        async with original(binding, principal) as lease:
            yield replace(lease, binding=binding.model_copy(update={"port": 4000}))

    rig.routes.acquire = wrong_route
    with pytest.raises(PreviewError, match="preview_binding_drift"):
        async with rig.service.route("preview", rig.principal):
            pytest.fail("wrong route must not be acquired")


async def test_stop_fences_access_when_cancel_fails(rig):
    async def fail(handle):
        assert (await rig.repo.get("preview")).handle.state == "stopped"
        assert (await rig.repo.get("preview")).route_epoch == 2
        raise OSError("runtime unavailable")

    rig.process.cancel = fail
    with pytest.raises(OSError):
        await rig.service.stop("preview", rig.principal)
    with pytest.raises(PreviewError, match="preview_inactive"):
        await rig.service.check("preview", rig.principal)


async def test_restarted_same_execution_cannot_rebind_preview(rig):
    record = await rig.repo.get("preview")
    assert record.instance.started_at == rig.routes.started_at
    rig.routes.started_at = "2026-11-15T01:00:00.000000001Z"
    async with client_for(rig) as client:
        await login(rig, client)
        response = await client.get("/")
        assert response.status_code == 410
        assert rig.routes.connected == 0
    assert (await rig.repo.get("preview")).instance.started_at == record.instance.started_at


async def test_route_epoch_change_cancels_stream_and_keeps_original_viewer(rig):
    stream = ChunkStream()
    rig.routes.transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
    scope = {
        **(await socket_scope(rig)),
        "type": "http",
        "scheme": "https",
        "method": "GET",
        "http_version": "1.1",
    }
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    await incoming.put({"type": "http.request", "body": b"", "more_body": False})
    task = asyncio.create_task(rig.app(scope, incoming.get, outgoing.put))
    async with asyncio.timeout(2):
        await outgoing.get()
        assert (await outgoing.get())["body"] == b"first"
        record = await rig.repo.get("preview")
        rig.repo.rows["preview"] = replace(record, route_epoch=record.route_epoch + 1)
        with pytest.raises(PreviewError, match="preview_stream_closed"):
            await task
    assert rig.routes.principals == [rig.principal]
    assert rig.routes.socket_closed == 1 and stream.closed


async def test_registration_persists_shared_instance_only(rig):
    from dataclasses import fields

    record = await rig.repo.get("preview")
    assert type(record.instance) is ServiceInstance
    assert record.instance.execution_id == rig.process.row.execution_id
    assert not {"execution_id", "started_at"} & {f.name for f in fields(record)}
    await rig.service.wait_ready("preview")
    ready = await rig.repo.get("preview")
    assert ready.instance == record.instance
    assert ready.route_epoch == record.route_epoch == 1
    assert ready.revision == record.revision + 1


async def test_route_source_denial_prevents_registration_persistence(rig):
    async def denied(process, port, principal):
        raise PreviewRouteError("preview_route_denied")

    rig.routes.observe = denied
    with pytest.raises(PreviewRouteError, match="preview_route_denied"):
        await rig.service.register("denied", rig.handle, 3001, rig.principal)
    assert "denied" not in rig.repo.rows


async def test_observed_different_execution_rejected_before_persist(rig):
    async def different(process, port, principal):
        return ServiceInstance(execution_id="b" * 64, started_at=rig.routes.started_at)

    rig.routes.observe = different
    with pytest.raises(PreviewError, match="preview_binding_drift"):
        await rig.service.register("different", rig.handle, 3001, rig.principal)
    assert "different" not in rig.repo.rows


@pytest.mark.parametrize(
    "code,status",
    [
        ("preview_route_unavailable", 503),
        ("preview_route_invalidated", 410),
        ("preview_route_capacity", 503),
        ("preview_route_denied", 403),
    ],
)
async def test_shared_route_errors_retain_safe_http_status(rig, code, status):
    @asynccontextmanager
    async def failed(binding, principal):
        raise PreviewRouteError(code)
        yield

    rig.routes.acquire = failed
    async with client_for(rig) as client:
        await login(rig, client)
        response = await client.get("/")
    assert response.status_code == status and response.json() == {"error": code}
    assert rig.routes.connected == 0


@pytest.mark.parametrize(
    "code,close_code",
    [
        ("preview_route_denied", 1008),
        ("preview_route_unavailable", 1011),
    ],
)
async def test_shared_route_errors_close_websocket(rig, code, close_code):
    scope = await socket_scope(rig)

    @asynccontextmanager
    async def failed(binding, principal):
        raise PreviewRouteError(code)
        yield

    rig.routes.acquire = failed
    output = []

    async def receive():
        return {"type": "websocket.connect"}

    async def send(message):
        output.append(message)

    await rig.app(scope, receive, send)
    assert output == [{"type": "websocket.close", "code": close_code}]
    assert rig.sockets.connect_args is None
