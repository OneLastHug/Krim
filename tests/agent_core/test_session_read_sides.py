"""真实连接与TCP SSE资源门禁；Probe沿用AC-SRC-01的有限观测模式。"""

import asyncio
import hashlib
import json
import socket
from contextlib import nullcontext
from contextvars import ContextVar

import anyio
import httpx
import pytest
import uvicorn
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as sql
from krim_agent.storage.sqlite import SQLiteStore


class Connections:
    def __init__(self, store):
        self.events = []
        self.pending = set()
        self.unsafe = 0

        def checkout(conn, record, proxy):
            self.pending.add(id(record))
            self.record("checkout", connection=id(record))

        def checkin(conn, record):
            self.pending.discard(id(record))
            self.record("checkin", connection=id(record))

        def reset(conn, record, state):
            self.unsafe += int(not state.asyncio_safe)
            self.record("reset", connection=id(record), asyncio_safe=state.asyncio_safe)

        def close(conn, record):
            self.record("close", connection=id(record))

        for name, callback in [
            ("checkout", checkout),
            ("checkin", checkin),
            ("reset", reset),
            ("close", close),
        ]:
            event.listen(store.engine.sync_engine, name, callback)

    def record(self, kind, **values):
        task = asyncio.current_task()
        self.events.append(
            {
                "kind": kind,
                "task": id(task),
                "task_name": task.get_name() if task else None,
                **values,
            }
        )


@pytest.mark.parametrize("stage", ["enter", "query", "exit"])
@pytest.mark.parametrize("cancellation", ["once", "repeated", "anyio"])
async def test_session_reader_cancellation_settles_real_connection(
    tmp_path, monkeypatch, stage, cancellation
):
    database = tmp_path / "readonly.sqlite"
    async with await open_application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="read"))
    before = database.read_bytes()
    store = await SQLiteStore.open(database, readonly=True)
    probe = Connections(store)
    entered = asyncio.Event()
    release = asyncio.Event()
    scope = None
    start, execute, exit_ = (
        AsyncTransaction.start,
        AsyncConnection.execute,
        AsyncTransaction.__aexit__,
    )

    async def barrier():
        probe.record("barrier", stage=stage)
        entered.set()
        await release.wait()
        probe.record("released")

    async def entering(transaction, *args, **kwargs):
        result = await start(transaction, *args, **kwargs)
        if transaction.connection.engine is store.engine and stage == "enter":
            await barrier()
        return result

    async def querying(conn, *args, **kwargs):
        result = await execute(conn, *args, **kwargs)
        if conn.engine is store.engine and stage == "query" and not entered.is_set():
            await barrier()
        return result

    async def exiting(transaction, *args, **kwargs):
        if transaction.connection.engine is store.engine and stage == "exit":
            await barrier()
        return await exit_(transaction, *args, **kwargs)

    monkeypatch.setattr(AsyncTransaction, "start", entering)
    monkeypatch.setattr(AsyncConnection, "execute", querying)
    monkeypatch.setattr(AsyncTransaction, "__aexit__", exiting)

    async def caller():
        nonlocal scope
        with anyio.CancelScope() as scope:
            try:
                await sql.get_session(store, session.id)
            except asyncio.CancelledError:
                probe.record("cancel-delivered", lock_held=store.lock.locked())
                raise
        probe.record("caller-exited")

    task = asyncio.create_task(caller(), name="session-reader")
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert probe.pending
        if cancellation == "anyio":
            scope.cancel()
        else:
            task.cancel()
        await asyncio.sleep(0.02)
        if cancellation == "repeated":
            task.cancel()
            await asyncio.sleep(0.02)
        assert not task.done() and not any(v["kind"] == "cancel-delivered" for v in probe.events)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await store.close()
        (tmp_path / "connections.json").write_text(json.dumps(probe.events, indent=2))
    kinds = [v["kind"] for v in probe.events]
    assert kinds.index("checkin") < kinds.index("cancel-delivered")
    assert not probe.pending and probe.unsafe == 0 and not store.lock.locked()
    assert all(not v["lock_held"] for v in probe.events if "lock_held" in v)
    ids = {v["task"] for v in probe.events if v["kind"] == "barrier"}
    assert ids != {id(task)} and not ids.intersection(
        id(t) for t in asyncio.all_tasks() if not t.done()
    )
    assert database.read_bytes() == before


async def test_tcp_reader_cancel_joins_snapshot_without_cancelling_run(tmp_path, wire, monkeypatch):
    class Server(uvicorn.Server):
        def capture_signals(self):
            return nullcontext()

    wire.hold_at.add(1)
    database = tmp_path / "sse.sqlite"
    async with wire.application(database) as app:
        probe = Connections(app.store)
        reading = ContextVar("session_sse_read", default=False)
        entered = asyncio.Event()
        release = asyncio.Event()
        exited = asyncio.Event()
        sse_task = None
        original_stream, original_snapshot = app.service.stream, app.store._snapshot

        async def stream(*args, **kwargs):
            nonlocal sse_task
            sse_task = asyncio.current_task()
            token = reading.set(True)
            source = original_stream(*args, **kwargs)
            try:
                async for value in source:
                    yield value
            finally:
                await source.aclose()
                reading.reset(token)
                probe.record("sse-exited", lock_held=app.store.lock.locked())
                exited.set()

        async def snapshot(conn, run_id):
            value = await original_snapshot(conn, run_id)
            if reading.get() and not entered.is_set():
                probe.record("sse-query", connections=list(probe.pending))
                entered.set()
                await release.wait()
                probe.record("sse-released")
            return value

        monkeypatch.setattr(app.service, "stream", stream)
        monkeypatch.setattr(app.store, "_snapshot", snapshot)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        listener.setblocking(False)
        port = listener.getsockname()[1]
        server = Server(
            uvicorn.Config(create_app(application=app), log_level="warning", access_log=False)
        )
        owner = asyncio.create_task(server.serve(sockets=[listener]), name="session-api-server")
        reader = None
        disconnect = asyncio.Event()
        try:
            async with asyncio.timeout(3):
                while not server.started:
                    if owner.done():
                        await owner
                    await asyncio.sleep(0.005)
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=3
            ) as client:
                created = (
                    await client.post("/v2/workspaces/local/sessions", json={"request_key": "sse"})
                ).json()
                base = f"/v2/workspaces/local/sessions/{created['id']}"
                turn = (
                    await client.post(
                        base + "/turns",
                        json={"request_key": "turn", "text": "/add 2 3", "expected_revision": 0},
                    )
                ).json()
                await wire.wait_requests(1)

                async def consume():
                    async with client.stream("GET", base + f"/turns/{turn['id']}/stream"):
                        await disconnect.wait()

                reader = asyncio.create_task(consume())
                await asyncio.wait_for(entered.wait(), 2)
                assert probe.pending and sse_task is not None
                held = next(v["connections"] for v in probe.events if v["kind"] == "sse-query")
                disconnect.set()
                await reader
                await asyncio.sleep(0.02)
                assert sse_task.cancelling() and not sse_task.done() and not exited.is_set()
                assert probe.pending and app.store.lock.locked()
                assert (
                    len(wire.requests) == 1 and wire.executions == [] and app.provider.active == 1
                )
                release.set()
                await asyncio.wait_for(exited.wait(), 2)
                index = next(i for i, v in enumerate(probe.events) if v["kind"] == "sse-exited")
                assert all(
                    any(
                        v["kind"] == "checkin" and v["connection"] == identifier
                        for v in probe.events[:index]
                    )
                    for identifier in held
                )
                current = (await client.get(base + f"/turns/{turn['id']}")).json()
                assert current["status"] == "running"
                stopped = await client.post(
                    base + f"/turns/{turn['id']}/cancel",
                    json={"request_key": "stop", "expected_revision": 1},
                )
                assert (
                    stopped.status_code == 200 and stopped.json()["result"]["status"] == "cancelled"
                )
                assert app.provider.active == 0 and app.provider.streams_closed == 1
                assert len(wire.requests) == 1 and wire.executions == []
                with pytest.raises(ServiceError, match="database_busy"):
                    await SQLiteStore.open(database)
        finally:
            release.set()
            disconnect.set()
            wire.release.set()
            if reader is not None:
                await asyncio.gather(reader, return_exceptions=True)
            server.should_exit = True
            await asyncio.wait_for(owner, 3)
            listener.close()
            await app.close()
            await app.close()
            (tmp_path / "connections.json").write_text(json.dumps(probe.events, indent=2))
        assert probe.unsafe == 0 and not probe.pending
        assert sum(v["kind"] == "checkout" for v in probe.events) == sum(
            v["kind"] == "checkin" for v in probe.events
        )
        second = await SQLiteStore.open(database)
        await second.close()
        sock = socket.socket()
        sock.settimeout(0.2)
        try:
            assert sock.connect_ex(("127.0.0.1", port)) != 0
        finally:
            sock.close()


async def test_scope_and_secret_redaction_in_trace_errors_and_health(tmp_path, wire, caplog):
    from pydantic import SecretStr

    from krim_agent.security import AccessPolicy, Credential, Membership

    secret = "SYNTHETIC_SESSION_KEY_012345"
    policy = AccessPolicy(
        credentials=(
            Credential(
                principal_id="local",
                token=SecretStr(secret),
                memberships=(Membership(workspace_id="local", role="admin"),),
                legacy=True,
            ),
        )
    )
    async with wire.application(tmp_path / "privacy.sqlite", access_policy=policy) as app:
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + secret},
            ) as client,
        ):
            session = (
                await client.post("/v2/workspaces/local/sessions", json={"request_key": "privacy"})
            ).json()
            turn = (
                await client.post(
                    f"/v2/workspaces/local/sessions/{session['id']}/turns",
                    json={"request_key": "one", "text": "/add 2 3", "expected_revision": 0},
                )
            ).json()
            complete = await app.sessions.wait(LOCAL, "local", session["id"], turn["id"])
            trace = (
                await client.get(
                    f"/v2/workspaces/local/sessions/{session['id']}/turns/{complete.id}/trace"
                )
            ).text
            error = (
                await client.post(
                    "/v2/workspaces/local/sessions", json={"request_key": "bad", "secret": secret}
                )
            ).text
            public = (
                json.dumps(session)
                + trace
                + error
                + (await client.get("/healthz")).text
                + (await client.get("/v2/me")).text
                + caplog.text
            )
            assert (
                secret not in public and hashlib.sha256(secret.encode()).hexdigest() not in public
            )
            assert f"127.0.0.1:{wire.port}" not in public and "access_policy_file" not in public
