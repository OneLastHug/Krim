import asyncio

import httpx
import pytest

from krim_agent.app_sdk import RemoteHostHandle


async def test_app_to_python_request_uses_real_callback_reply_and_side_binding(server_url):
    async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
        sid = (await api.post("/v1/host/sessions")).json()["id"]
        await api.post(f"/v1/host/sessions/{sid}/ready")
        async with RemoteHostHandle(server_url, sid) as host:
            host.on_request("host/callback", lambda data: {"done": True, "data": data})
            command = {
                "envelope": {
                    "protocol_version": 2,
                    "kind": "call",
                    "id": "app-call",
                    "channel": "host/callback",
                    "data": {"item_id": "change", "action": "undo"},
                },
                "timeout_seconds": 3,
            }
            pending = asyncio.create_task(
                api.post(f"/v1/host/sessions/{sid}/requests", json=command)
            )
            event = None
            async for value in host.events():
                if value.kind == "call":
                    event = value
                    break
            assert event is not None and event.id == "app-call"
            response = await pending
            assert response.status_code == 200 and response.json()["data"] == {
                "done": True,
                "data": {"item_id": "change", "action": "undo"},
            }


async def test_app_to_python_request_rejects_unknown_channel_and_cancel_cleans_callback(api):
    sid = (await api.post("/v1/host/sessions")).json()["id"]
    await api.post(f"/v1/host/sessions/{sid}/ready")
    unknown = await api.post(
        f"/v1/host/sessions/{sid}/requests",
        json={
            "envelope": {
                "protocol_version": 2,
                "kind": "call",
                "id": "unknown",
                "channel": "danger/execute",
                "data": None,
            }
        },
    )
    assert unknown.status_code == 422
    command = {
        "envelope": {
            "protocol_version": 2,
            "kind": "call",
            "id": "cancel-me",
            "channel": "host/callback",
            "data": {},
        },
        "timeout_seconds": 5,
    }
    task = asyncio.create_task(api.post(f"/v1/host/sessions/{sid}/requests", json=command))
    await asyncio.sleep(0.02)
    assert (await api.post(f"/v1/host/sessions/{sid}/cancel/cancel-me")).status_code == 200
    assert (await task).status_code == 409
    await api.delete(f"/v1/host/sessions/{sid}")


@pytest.mark.parametrize("side", ["app", "host"])
async def test_reply_direction_matches_actual_request_target(api, side):
    sid = (await api.post("/v1/host/sessions")).json()["id"]
    await api.post(f"/v1/host/sessions/{sid}/ready")
    target = "requests" if side == "host" else "commands"
    channel = "host/callback" if side == "host" else "host/get-state"
    body = {
        "envelope": {
            "protocol_version": 2,
            "kind": "call",
            "id": "side",
            "channel": channel,
            "data": None,
        },
        "timeout_seconds": 3,
    }
    pending = asyncio.create_task(api.post(f"/v1/host/sessions/{sid}/{target}", json=body))
    await asyncio.sleep(0.02)
    envelope = {
        "protocol_version": 2,
        "kind": "reply",
        "id": "side",
        "channel": channel,
        "data": {"ok": True, "data": {"actual": side}, "error": None},
    }
    wrong = "app" if side == "host" else "host"
    assert (
        await api.post(f"/v1/host/sessions/{sid}/reply", params={"side": wrong}, json=envelope)
    ).status_code == 409
    assert not pending.done()
    assert (
        await api.post(f"/v1/host/sessions/{sid}/reply", params={"side": side}, json=envelope)
    ).status_code == 200
    assert (await pending).json()["data"] == {"actual": side}
    assert (await api.post(f"/v1/host/sessions/{sid}/{target}", json=body)).status_code == 409


async def test_live_python_callback_is_cancelled_and_completed_call_replay_is_retired(server_url):
    started, stopped = asyncio.Event(), asyncio.Event()
    calls = []
    async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
        sid = (await api.post("/v1/host/sessions")).json()["id"]
        await api.post(f"/v1/host/sessions/{sid}/ready")
        async with RemoteHostHandle(server_url, sid) as host:

            async def handler(data):
                calls.append(data)
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    stopped.set()

            host.on_request("host/callback", handler)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            request = asyncio.create_task(
                api.post(
                    f"/v1/host/sessions/{sid}/requests",
                    json={
                        "envelope": {
                            "protocol_version": 2,
                            "kind": "call",
                            "id": "live",
                            "channel": "host/callback",
                            "data": {"marker": "one"},
                        },
                        "timeout_seconds": 3,
                    },
                )
            )
            try:
                await asyncio.wait_for(started.wait(), 3)
                assert (await api.post(f"/v1/host/sessions/{sid}/cancel/live")).status_code == 200
                assert (await request).status_code == 409
                await asyncio.wait_for(stopped.wait(), 3)
                assert len(calls) == 1
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)


@pytest.mark.parametrize("mode", ["missing", "raises", "invalid_output"])
async def test_unavailable_or_failing_callback_returns_failure_without_leaking_error(
    server_url, mode
):
    async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
        sid = (await api.post("/v1/host/sessions")).json()["id"]
        await api.post(f"/v1/host/sessions/{sid}/ready")
        async with RemoteHostHandle(server_url, sid) as host:

            def handler(_data):
                if mode == "raises":
                    raise RuntimeError("private executor detail")
                return object()

            if mode != "missing":
                host.on_request("host/callback", handler)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            try:
                reply = await api.post(
                    f"/v1/host/sessions/{sid}/requests",
                    json={
                        "envelope": {
                            "protocol_version": 2,
                            "kind": "call",
                            "id": "error",
                            "channel": "host/callback",
                            "data": None,
                        },
                        "timeout_seconds": 3,
                    },
                )
                assert reply.status_code == 200 and not reply.json()["ok"]
                assert "private executor detail" not in reply.text
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)
