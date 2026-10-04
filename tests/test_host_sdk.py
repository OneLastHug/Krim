import asyncio

import httpx

from krim_agent.api import create_app
from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def test_python_host_session_request_reply_timeout_duplicate_and_shutdown(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'host.sqlite'}")
    runtime = ChatRuntime(
        KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
    )
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client,
    ):
        created = await client.post("/v1/host/sessions")
        assert created.status_code == 201
        session_id = created.json()["id"]
        assert (
            await client.post(
                f"/v1/host/sessions/{session_id}/commands",
                json={
                    "envelope": {
                        "protocol_version": 2,
                        "kind": "call",
                        "id": "not-ready",
                        "channel": "host/get-state",
                        "data": None,
                    },
                },
            )
        ).status_code == 409
        assert (await client.post(f"/v1/host/sessions/{session_id}/ready")).status_code == 200
        request_task = asyncio.create_task(
            client.post(
                f"/v1/host/sessions/{session_id}/commands",
                json={
                    "envelope": {
                        "protocol_version": 2,
                        "kind": "call",
                        "id": "one",
                        "channel": "host/get-state",
                        "data": None,
                    },
                    "timeout_seconds": 2,
                },
            )
        )
        invalid_cursor = await client.get(
            f"/v1/host/sessions/{session_id}/events", params={"from_seq": 999}
        )
        assert invalid_cursor.status_code == 416
        await asyncio.sleep(0.02)
        response = await client.post(
            f"/v1/host/sessions/{session_id}/reply",
            json={
                "protocol_version": 2,
                "kind": "reply",
                "id": "one",
                "channel": "host/get-state",
                "data": {"ok": True, "data": {"locale": "zh-CN"}, "error": None},
            },
        )
        assert response.status_code == 200
        command = await request_task
        assert command.status_code == 200 and command.json()["data"] == {"locale": "zh-CN"}
        duplicate = await client.post(
            f"/v1/host/sessions/{session_id}/commands",
            json={
                "envelope": {
                    "protocol_version": 2,
                    "kind": "notify",
                    "id": "one",
                    "channel": "host/sidebar",
                    "data": {"open": True},
                }
            },
        )
        assert duplicate.status_code == 409
        closed = await client.delete(f"/v1/host/sessions/{session_id}")
        assert closed.status_code == 200
        assert (await client.get(f"/v1/host/sessions/{session_id}")).status_code == 410
    await runtime.close()
    await database.close()


async def test_host_session_rejects_wrong_channel_reply_and_explicit_timeout(api):
    session = (await api.post("/v1/host/sessions")).json()
    sid = session["id"]
    await api.post(f"/v1/host/sessions/{sid}/ready")
    task = asyncio.create_task(
        api.post(
            f"/v1/host/sessions/{sid}/commands",
            json={
                "envelope": {
                    "protocol_version": 2,
                    "kind": "call",
                    "id": "timeout",
                    "channel": "host/get-state",
                    "data": None,
                },
                "timeout_seconds": 0.01,
            },
        )
    )
    await asyncio.sleep(0.03)
    assert (await task).status_code == 504
    bad = await api.post(
        f"/v1/host/sessions/{sid}/reply",
        json={
            "protocol_version": 2,
            "kind": "reply",
            "id": "timeout",
            "channel": "host/get-state",
            "data": {"ok": True, "data": None, "error": None},
        },
    )
    assert bad.status_code == 409
    await api.delete(f"/v1/host/sessions/{sid}")


async def test_host_reply_binding_duplicate_and_close_pending(api):
    sid = (await api.post("/v1/host/sessions")).json()["id"]
    await api.post(f"/v1/host/sessions/{sid}/ready")
    command = {
        "envelope": {
            "protocol_version": 2,
            "kind": "call",
            "id": "bound",
            "channel": "host/get-state",
            "data": None,
        },
        "timeout_seconds": 2,
    }
    task = asyncio.create_task(api.post(f"/v1/host/sessions/{sid}/commands", json=command))
    await asyncio.sleep(0.02)
    wrong = await api.post(
        f"/v1/host/sessions/{sid}/reply",
        json={
            "protocol_version": 2,
            "kind": "reply",
            "id": "bound",
            "channel": "host/theme",
            "data": {"ok": True, "data": {"theme": "dark"}, "error": None},
        },
    )
    assert wrong.status_code == 409 and not task.done()
    invalid = await api.post(
        f"/v1/host/sessions/{sid}/reply",
        json={
            "protocol_version": 2,
            "kind": "reply",
            "id": "bound",
            "channel": "host/get-state",
            "data": {"ok": False, "data": "forged", "error": "bad"},
        },
    )
    assert invalid.status_code == 422 and not task.done()
    await api.delete(f"/v1/host/sessions/{sid}")
    assert (await task).status_code == 410


async def test_host_api_auth_origin_protocol_and_unknown_channel():
    from pydantic import SecretStr

    app = create_app(
        Settings(database_url="sqlite+aiosqlite:///:memory:", api_key=SecretStr("host-test-key"))
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client,
    ):
        assert (await client.post("/v1/host/sessions")).status_code == 401
        client.headers["Authorization"] = "Bearer host-test-key"
        assert (
            await client.post("/v1/host/sessions", headers={"Origin": "http://evil.invalid"})
        ).status_code == 403
        sid = (await client.post("/v1/host/sessions")).json()["id"]
        await client.post(f"/v1/host/sessions/{sid}/ready")
        for envelope in [
            {
                "protocol_version": 3,
                "kind": "call",
                "id": "one",
                "channel": "host/get-state",
                "data": None,
            },
            {
                "protocol_version": 2,
                "kind": "reply",
                "id": "one",
                "channel": "host/get-state",
                "data": None,
            },
            {
                "protocol_version": 2,
                "kind": "call",
                "id": "one",
                "channel": "danger/execute",
                "data": None,
            },
        ]:
            result = await client.post(
                f"/v1/host/sessions/{sid}/commands", json={"envelope": envelope}
            )
            assert result.status_code == 422


async def test_host_event_replay_marks_completed_command_cancelled_instead_of_reexecuting():
    from krim_agent.application.host import HostSessions
    from krim_agent.host import HostCommand
    from krim_agent.protocol.models import ProtocolEnvelope

    sessions = HostSessions(max_sessions=1, max_pending=1)
    sid = (await sessions.create()).id
    await sessions.mark_ready(sid)
    iterator = sessions.events(sid, "app", 0)
    task = asyncio.create_task(
        sessions.command(
            sid,
            HostCommand(
                envelope=ProtocolEnvelope(
                    kind="call", id="once", channel="host/prompt", data={"value": "one"}
                )
            ),
        )
    )
    frame = await asyncio.wait_for(anext(iterator), 2)
    assert frame is not None and frame.data["kind"] == "call"
    await sessions.reply(
        sid,
        ProtocolEnvelope(
            kind="reply",
            id="once",
            channel="host/prompt",
            data={"ok": True, "data": {"value": "one"}, "error": None},
        ),
    )
    assert (await task).ok
    await iterator.aclose()
    replay = sessions.events(sid, "app", 0)
    retired = await asyncio.wait_for(anext(replay), 2)
    assert retired is not None and retired.data["kind"] == "cancel"
    await replay.aclose()
    await sessions.shutdown()


async def test_remote_host_handle_uses_same_http_session_and_closes_pending(repository):
    runtime = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
    app = create_app(runtime=runtime)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as client:
            session = (await client.post("/v1/host/sessions")).json()
            sid = session["id"]
            await client.post(f"/v1/host/sessions/{sid}/ready")
            async with RemoteHostHandle("http://127.0.0.1", sid, transport=transport) as host:
                task = asyncio.create_task(host.emit("host/get-state", timeout=2))
                await asyncio.sleep(0.02)
                response = await client.post(
                    f"/v1/host/sessions/{sid}/reply",
                    json={
                        "protocol_version": 2,
                        "kind": "reply",
                        "id": next(iter(app.state.host_sessions.session(sid).pending)),
                        "channel": "host/get-state",
                        "data": {"ok": True, "data": {"locale": "zh-CN"}, "error": None},
                    },
                )
                assert response.status_code == 200
                assert (await task).data == {"locale": "zh-CN"}
                assert (await host.status()).connected
            assert (await client.get(f"/v1/host/sessions/{sid}")).status_code == 200
            await client.delete(f"/v1/host/sessions/{sid}")
        await runtime.close()
