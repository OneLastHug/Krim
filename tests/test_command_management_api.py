"""真实HTTP命令管理：接受、历史、读取、等待、取消与同键并发。"""

import asyncio
import json
import shutil
import sys

import httpx
import pytest

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.sdk import AgentCommandRequest
from krim_agent.tools import ShortCircuit

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux HTTP命令服务",
)

NAMES = [
    "workspace_read",
    "workspace_change",
    "workspace_reconcile",
    "workspace_finalize",
    "command_execute",
    "command_get",
    "command_read",
    "command_cancel",
    "command_history",
    "command_inspect",
    "command_changes",
    "command_propose",
]


async def setup_app(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "input.txt").write_text("input")
    app = create_app(
        Settings(
            database_url=f"sqlite+aiosqlite:///{tmp_path / 'api.sqlite'}", interactive_tools=True
        ),
        workspace_root=root,
        workspace_writable=True,
        execution_runner=LinuxSandboxRunner(),
        execution_owned=True,
    )
    context = app.router.lifespan_context(app)
    await context.__aenter__()
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1")
    return app, context, client


async def configure(client):
    response = await client.put(
        "/v1/agents/profiles/commands",
        json={
            "expected_revision": 0,
            "config": {
                "name": "commands",
                "description": "HTTP命令",
                "workspace_key": "local",
                "execution_key": "local",
                "tool_names": NAMES,
                "persistent": True,
            },
        },
    )
    assert response.status_code == 200, response.text
    spawned = await client.post("/v1/agents", json={"name": "commands", "persistent": True})
    assert spawned.status_code == 201, spawned.text
    return spawned.json()["agent_id"]


async def test_http_start_history_read_wait_by_key_and_inspect(tmp_path):
    app, context, client = await setup_app(tmp_path)
    try:
        agent_id = await configure(client)
        payload = {
            "request_key": "http-one",
            "command": {"argv": ["/usr/bin/python3", "-I", "-c", "print('http')"]},
        }
        first, second = await asyncio.gather(
            client.post(
                f"/v1/agents/{agent_id}/commands", params={"confirmed": "true"}, json=payload
            ),
            client.post(
                f"/v1/agents/{agent_id}/commands", params={"confirmed": "true"}, json=payload
            ),
        )
        assert first.status_code == second.status_code == 202
        command_id = first.json()["command_id"]
        assert command_id == second.json()["command_id"]
        history = await client.get(f"/v1/agents/{agent_id}/commands")
        assert (
            history.status_code == 200 and history.json()["entries"][0]["command_id"] == command_id
        )
        by_key = await client.get(
            f"/v1/agents/{agent_id}/commands/by-key", params={"request_key": "http-one"}
        )
        assert by_key.status_code == 200 and by_key.json()["command_id"] == command_id
        inspected = await client.get(f"/v1/agents/{agent_id}/commands/{command_id}/inspect")
        assert (
            inspected.status_code == 200
            and inspected.json()["request"]["request_key"] == "http-one"
        )
        waited = await client.post(
            f"/v1/agents/{agent_id}/commands/{command_id}/wait", params={"timeout_seconds": 30}
        )
        assert waited.status_code == 200 and waited.json()["outcome"] == "completed"
        output = await client.get(
            f"/v1/agents/{agent_id}/commands/{command_id}/read", params={"source": "stdout"}
        )
        assert output.status_code == 200 and output.json()["text"] == "http\n"
    finally:
        await client.aclose()
        await context.__aexit__(None, None, None)


async def test_http_input_validation_permission_failure_and_cancel(tmp_path):
    app, context, client = await setup_app(tmp_path)
    try:
        agent_id = await configure(client)
        invalid = await client.post(
            f"/v1/agents/{agent_id}/commands", json={"request_key": "bad", "command": {"argv": []}}
        )
        assert invalid.status_code == 422
        denied = await client.post(
            f"/v1/agents/{agent_id}/commands",
            json={"request_key": "unconfirmed", "command": {"argv": ["/usr/bin/true"]}},
        )
        assert denied.status_code == 409 and denied.json()["error"]["code"] == "approval_required"
        assert (
            await client.get(
                f"/v1/agents/{agent_id}/commands/by-key", params={"request_key": "unconfirmed"}
            )
        ).json() is None
        long = {
            "request_key": "cancel",
            "command": {"argv": ["/usr/bin/python3", "-I", "-c", "import time;time.sleep(30)"]},
        }
        accepted = await client.post(
            f"/v1/agents/{agent_id}/commands", params={"confirmed": "true"}, json=long
        )
        assert accepted.status_code == 202
        command_id = accepted.json()["command_id"]
        cancelled = await client.post(
            f"/v1/agents/{agent_id}/commands/{command_id}/cancel", params={"confirmed": "true"}
        )
        assert cancelled.status_code == 200
        result = await client.post(
            f"/v1/agents/{agent_id}/commands/{command_id}/wait", params={"timeout_seconds": 30}
        )
        assert result.status_code == 200
        assert result.json()["outcome"] == "cancelled", result.json()
        assert result.json()["processes_reaped"]
        missing = await client.get(
            f"/v1/agents/{agent_id}/commands/by-key", params={"request_key": "missing"}
        )
        assert missing.status_code == 200 and missing.json() is None
    finally:
        await client.aclose()
        await context.__aexit__(None, None, None)


async def test_cancelled_start_waiter_joins_original_and_rejects_changed_request(
    tmp_path, monkeypatch
):
    app, context, client = await setup_app(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    try:
        identifier = await configure(client)
        service = app.state.agents
        registry = service._handle(identifier).runtime.tools
        runner = service.resources.execution("local").runner
        calls = 0
        original = runner.start

        async def counted(*args, **kwargs):
            nonlocal calls
            calls += 1
            return await original(*args, **kwargs)

        async def hold(definition, _context):
            if definition.name == "command_execute":
                entered.set()
                await release.wait()

        monkeypatch.setattr(runner, "start", counted)
        registry.add_global_pre(hold)
        url = f"/v1/agents/{identifier}/commands"
        payload = {
            "request_key": "same",
            "command": {"argv": ["/usr/bin/python3", "-c", "print('once')"]},
        }
        first = asyncio.create_task(client.post(url, params={"confirmed": True}, json=payload))
        await asyncio.wait_for(entered.wait(), 5)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(client.post(url, params={"confirmed": True}, json=payload))
        changed = await client.post(
            url,
            params={"confirmed": True},
            json={**payload, "command": {"argv": ["/usr/bin/false"]}},
        )
        assert changed.status_code == 409 and calls == 0
        release.set()
        accepted = await asyncio.wait_for(second, 10)
        assert accepted.status_code == 202
        cmd = accepted.json()["command_id"]
        result = await client.post(url + f"/{cmd}/wait")
        assert result.json()["stdout_text"] == "once\n" and calls == 1
        again = await client.post(url, params={"confirmed": True}, json=payload)
        assert again.json()["command_id"] == cmd and calls == 1
    finally:
        release.set()
        await client.aclose()
        await context.__aexit__(None, None, None)


async def test_wait_timeout_and_waiter_disconnect_do_not_stop_production(tmp_path):
    app, context, client = await setup_app(tmp_path)
    try:
        identifier = await configure(client)
        base = f"/v1/agents/{identifier}/commands"
        accepted = await client.post(
            base,
            params={"confirmed": True},
            json={
                "request_key": "long",
                "command": {
                    "argv": [
                        "/usr/bin/python3",
                        "-c",
                        "import time;print('ready',flush=True);time.sleep(30)",
                    ]
                },
            },
        )
        path = base + "/" + accepted.json()["command_id"]
        timeout = await client.post(path + "/wait", params={"timeout_seconds": 0})
        assert timeout.status_code == 200 and timeout.json()["phase"] not in {
            "finished",
            "interrupted",
        }
        waiting = asyncio.create_task(client.post(path + "/wait", params={"timeout_seconds": 30}))
        await asyncio.sleep(0.05)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert (await client.get(path)).json()["phase"] not in {"finished", "interrupted"}
        cancelled = await client.post(path + "/cancel", params={"confirmed": True})
        assert cancelled.status_code == 200 and cancelled.json()["processes_reaped"]
    finally:
        await client.aclose()
        await context.__aexit__(None, None, None)


@pytest.mark.parametrize("fault", ["short", "callback", "mutate"])
async def test_unverified_admission_and_modified_background_intent_are_rejected(tmp_path, fault):
    app, context, client = await setup_app(tmp_path)
    try:
        identifier = await configure(client)
        registry = app.state.agents._handle(identifier).runtime.tools

        async def hook(definition, ctx):
            if definition.name != "command_execute":
                return None
            if fault == "mutate":
                ctx.arguments["request_key"] = "changed"
                return None
            if fault == "callback":
                ctx.command_admitted("fake-command")
            return ShortCircuit({})

        registry.add_global_pre(hook)
        response = await client.post(
            f"/v1/agents/{identifier}/commands",
            params={"confirmed": True},
            json={"request_key": "original", "command": {"argv": ["/usr/bin/true"]}},
        )
        assert response.status_code == 409 or response.status_code == 403, response.text
        assert response.json()["error"]["code"] in {
            "execution_receipt_unverified",
            "permission_denied",
        }
        assert (await client.get(f"/v1/agents/{identifier}/commands")).json()["entries"] == []
    finally:
        await client.aclose()
        await context.__aexit__(None, None, None)


async def test_history_inspect_and_file_routes_keep_scope_and_validation(tmp_path):
    app, context, client = await setup_app(tmp_path)
    try:
        identifier = await configure(client)
        base = f"/v1/agents/{identifier}/commands"
        ids = []
        for key in ["one", "two", "three"]:
            accepted = await client.post(
                base,
                params={"confirmed": True},
                json={
                    "request_key": key,
                    "command": {
                        "argv": ["/usr/bin/python3", "-c", "open('out.txt','w').write('content')"]
                    },
                },
            )
            cmd = accepted.json()["command_id"]
            ids.append(cmd)
            assert (await client.post(base + "/" + cmd + "/wait")).json()["outcome"] == "completed"
        first = (await client.get(base, params={"limit": 1})).json()
        second = (await client.get(base, params={"limit": 1, "after": first["next_after"]})).json()
        assert (
            first["entries"][0]["command_id"] == ids[0]
            and second["entries"][0]["command_id"] == ids[1]
        )
        assert "stdout" not in first["entries"][0] and "artifacts" not in first["entries"][0]
        assert "token" not in json.dumps(first)
        assert (await client.get(base + "/" + ids[0] + "/inspect")).json()["request"][
            "request_key"
        ] == "one"
        assert (
            await client.get(base + "/" + ids[0] + "/read", params={"source": "artifact"})
        ).status_code == 422
        assert (
            await client.get(
                base + "/" + ids[0] + "/read", params={"source": "artifact", "path": "../escape"}
            )
        ).status_code == 422
        assert (
            await client.post(base + "/" + ids[0] + "/wait", params={"timeout_seconds": "nan"})
        ).status_code == 422
        assert (await client.get(base, params={"limit": 33})).status_code == 422
        other = (await client.post("/v1/agents", json={"name": "commands"})).json()["agent_id"]
        assert (await client.get(f"/v1/agents/{other}/commands/{ids[0]}")).status_code == 404
        changes = await client.get(base + "/" + ids[0] + "/changes")
        assert changes.status_code == 200 and changes.json()["entries"][0]["path"] == "out.txt"
        proposal = await client.post(
            base + "/" + ids[0] + "/proposals",
            json={"command_id": ids[0], "path": "out.txt", "request_key": "proposal"},
        )
        assert proposal.status_code == 200
        view = proposal.json()["proposal"]
        applied = await client.post(
            f"/v1/agents/{identifier}/workspace/changes",
            params={"confirmed": True},
            json={
                "proposal_id": view["proposal_id"],
                "expected_revision": view["revision"],
                "action": "apply",
                "request_key": "apply",
            },
        )
        assert (
            applied.json()["status"] == "done"
            and (tmp_path / "files/out.txt").read_text() == "content"
        )
    finally:
        await client.aclose()
        await context.__aexit__(None, None, None)


async def test_background_policy_cannot_wait_or_close_its_own_launch(tmp_path):
    app, context, client = await setup_app(tmp_path)
    try:
        identifier = await configure(client)
        service = app.state.agents

        async def hook(definition, _ctx):
            if definition.name == "command_execute":
                with pytest.raises(Exception) as reentry:
                    await service.close()
                assert reentry.value.code == "execution_start_reentrant"
                with pytest.raises(Exception) as waiting:
                    await service.command_start(
                        identifier,
                        AgentCommandRequest.model_validate(
                            {"request_key": "nested", "command": {"argv": ["/usr/bin/true"]}}
                        ),
                        confirmed=True,
                    )
                assert waiting.value.code == "execution_start_reentrant"

        service._handle(identifier).runtime.tools.add_global_pre(hook)
        start = await client.post(
            f"/v1/agents/{identifier}/commands",
            params={"confirmed": True},
            json={"request_key": "one", "command": {"argv": ["/usr/bin/true"]}},
        )
        assert start.status_code == 202 and not service._closed
    finally:
        await client.aclose()
        await context.__aexit__(None, None, None)
