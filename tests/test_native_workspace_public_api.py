"""原生修改清单的 REST、同步客户端和真实 CLI 消费路径。"""

import asyncio
import json
import os
import socket
import subprocess
import sys
from contextlib import asynccontextmanager

import httpx
import pytest
import uvicorn

from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.workspace_change import (
    FileChangeSpec,
    WorkspaceChangeAction,
    WorkspacePreviewRequest,
)
from krim_agent.workspace_list import NativeBatchRequest, NativeBatchTarget

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="需要 Linux 条件文件事务"
)


async def allow_or_ask(definition, _context):
    return "allow" if definition.risk == "read" else "ask"


@asynccontextmanager
async def native_api(runtime, tmp_path):
    runtime.tool_policy = allow_or_ask
    app = create_app(runtime=runtime, workspace_root=tmp_path, workspace_writable=True)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        agent = (await api.post("/v1/agents", json={"name": "default"})).json()
        yield app, api, agent["agent_id"]


async def apply(api, agent_id, path):
    base = f"/v1/agents/{agent_id}/workspace"
    proposal = await api.post(
        f"{base}/changes/preview",
        json={
            "spec": {
                "path": path,
                "operation": "write",
                "expected_sha256": None,
                "content": f"content-{path}\n",
            },
            "request_key": f"preview-{path}",
        },
    )
    assert proposal.status_code == 200, proposal.text
    value = proposal.json()
    applied = await api.post(
        f"{base}/changes",
        params={"confirmed": "true"},
        json={
            "proposal_id": value["proposal_id"],
            "expected_revision": value["revision"],
            "action": "apply",
            "request_key": f"apply-{path}",
        },
    )
    assert applied.status_code == 200 and applied.json()["status"] == "done", applied.text
    return value


def batch_request(page, action="keep", key="page-key"):
    return {
        "action": action,
        "snapshot_revision": page["snapshot_revision"],
        "targets": [
            {"proposal_id": item["proposal_id"], "expected_revision": item["revision"]}
            for item in page["items"]
        ],
        "request_key": key,
    }


def assert_public(value):
    private = {
        "token",
        "fence",
        "admission_open",
        "file_key",
        "root",
        "owner_id",
        "lease_expires_at",
    }
    if isinstance(value, dict):
        assert not private.intersection(value)
        for child in value.values():
            assert_public(child)
    elif isinstance(value, list):
        for child in value:
            assert_public(child)


async def test_rest_native_page_batch_lookup_and_stale_errors(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (_app, api, agent_id):
        await apply(api, agent_id, "first.txt")
        await apply(api, agent_id, "second.txt")
        base = f"/v1/agents/{agent_id}/workspace"
        response = await api.get(f"{base}/native-changes", params={"limit": 1})
        assert response.status_code == 200
        page = response.json()
        assert page["total"] == 2 and len(page["items"]) == 1 and page["next_cursor"]
        next_page = (
            await api.get(f"{base}/native-changes", params={"cursor": page["next_cursor"]})
        ).json()
        assert len(next_page["items"]) == 1 and next_page["next_cursor"] is None
        assert next_page["items"][0]["proposal_id"] != page["items"][0]["proposal_id"]
        request = batch_request(page, key="审阅/../页?confirmed=true#%")
        accepted = await api.post(
            f"{base}/native-batches", params={"confirmed": "true"}, json=request
        )
        assert accepted.status_code == 202, accepted.text
        batch_id = accepted.json()["batch_id"]
        completed = await api.post(f"{base}/native-batches/{batch_id}/wait")
        assert completed.status_code == 200 and completed.json()["status"] == "done"
        value = completed.json()
        assert value["items"][0]["operation"]["action"] == "keep"
        assert value["items"][0]["operation"]["receipt"]["changed"] is False
        for path, params in (
            (f"{base}/native-batches/{batch_id}", {}),
            (f"{base}/native-batches/by-key", {"request_key": request["request_key"]}),
        ):
            observed = await api.get(path, params=params)
            assert observed.status_code == 200 and observed.json() == value
            assert_public(observed.json())
        assert_public(page)
        repeated = await api.post(
            f"{base}/native-batches", params={"confirmed": "true"}, json=request
        )
        assert repeated.status_code == 202 and repeated.json()["batch_id"] == batch_id
        assert (tmp_path / "first.txt").read_text() == "content-first.txt\n"
        assert (tmp_path / "second.txt").read_text() == "content-second.txt\n"
        stale = await api.get(f"{base}/native-changes", params={"cursor": page["next_cursor"]})
        assert stale.status_code == 409 and stale.json()["error"]["code"] == "workspace_list_stale"
        rejected = await api.post(
            f"{base}/native-batches",
            json={**request, "request_key": "old-page"},
            params={"confirmed": "true"},
        )
        assert rejected.status_code == 409
        assert rejected.json()["error"]["code"] == "workspace_list_revision_conflict"
        conflict = await api.post(f"{base}/native-batches", json={**request, "action": "undo"})
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "workspace_batch_idempotency_conflict"
        assert (
            await api.get(f"{base}/native-batches/by-key", params={"request_key": "missing"})
        ).json() is None
        assert (await api.get(f"{base}/native-batches/missing")).status_code == 404
        other = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        foreign = f"/v1/agents/{other}/workspace/native-batches"
        assert (await api.get(f"{foreign}/{batch_id}")).status_code == 404
        assert (
            await api.get(f"{foreign}/by-key", params={"request_key": request["request_key"]})
        ).json() is None


async def test_rest_cancel_stops_unstarted_children_and_wait_does_not_cancel(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (app, api, agent_id):
        await apply(api, agent_id, "first.txt")
        base = f"/v1/agents/{agent_id}/workspace"
        page = (await api.get(f"{base}/native-changes")).json()
        entered, release = asyncio.Event(), asyncio.Event()
        registry = app.state.agents._handle(agent_id).runtime.tools

        async def blocked(definition, _context):
            if definition.name == "workspace_change":
                entered.set()
                await release.wait()

        registry.add_global_pre(blocked)
        try:
            accepted = await api.post(
                f"{base}/native-batches",
                params={"confirmed": "true"},
                json=batch_request(page, "undo"),
            )
            assert accepted.status_code == 202
            batch_id = accepted.json()["batch_id"]
            await asyncio.wait_for(entered.wait(), 5)
            waiting = await api.post(
                f"{base}/native-batches/{batch_id}/wait", params={"timeout_seconds": 0}
            )
            assert waiting.status_code == 200 and waiting.json()["status"] == "running"
            cancelled = await api.post(f"{base}/native-batches/{batch_id}/cancel")
            assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
            assert cancelled.json()["items"][0]["operation"] is None
            assert (tmp_path / "first.txt").read_text() == "content-first.txt\n"
            assert_public(cancelled.json())
            session = (await api.get(f"/v1/agents/{agent_id}/session")).json()
            closed = await api.post(
                f"/v1/agents/{agent_id}/session/close",
                json={"expected_revision": session["revision"]},
            )
            assert closed.status_code == 200
            for suffix in ("", "/wait", "/cancel"):
                method = api.get if not suffix else api.post
                assert (
                    await method(f"{base}/native-batches/{batch_id}{suffix}")
                ).status_code == 200
        finally:
            release.set()
            registry._global_pre.clear()


async def test_rest_native_inputs_are_bounded_and_internal_fields_are_rejected(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (_app, api, agent_id):
        await apply(api, agent_id, "first.txt")
        base = f"/v1/agents/{agent_id}/workspace"
        page = (await api.get(f"{base}/native-changes")).json()
        for params in ({"limit": 0}, {"limit": 33}, {"filter": "wrong"}, {"cursor": "x" * 1025}):
            assert (await api.get(f"{base}/native-changes", params=params)).status_code == 422
        for key in ("", "x" * 129, "line\nbreak", "trailing\n", "\x00key"):
            assert (
                await api.get(f"{base}/native-batches/by-key", params={"request_key": key})
            ).status_code == 422
        for field in ("token", "fence", "owner_id"):
            response = await api.post(
                f"{base}/native-batches",
                json={**batch_request(page), field: "PRIVATE-INPUT-CANARY"},
            )
            assert response.status_code == 422
            assert "PRIVATE-INPUT-CANARY" not in response.text
        assert (await api.post(f"{base}/native-batches", content=b"x" * 65537)).status_code == 413
        for seconds in (-1, 301, "nan", "inf"):
            assert (
                await api.post(
                    f"{base}/native-batches/missing/wait", params={"timeout_seconds": seconds}
                )
            ).status_code == 422
        blocked = await api.post(
            f"{base}/native-batches",
            headers={"Origin": "https://untrusted.example"},
            params={"confirmed": "true"},
            json=batch_request(page),
        )
        assert blocked.status_code == 403 and blocked.json()["error"]["code"] == "origin_denied"


async def run_cli(url, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "workspace",
        *map(str, args),
        "--url",
        url,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(child.communicate(), 15)
    return child.returncode, stdout.decode(), stderr.decode()


async def test_tcp_client_and_cli_native_batch_lifecycle(runtime, tmp_path):
    runtime.tool_policy = allow_or_ask
    app = create_app(runtime=runtime, workspace_root=tmp_path, workspace_writable=True)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)

        def client_flow():
            with KrimClient(url) as client:
                agent_id = client.agent_spawn("default").agent_id
                proposal = client.agent_workspace_preview(
                    agent_id,
                    WorkspacePreviewRequest(
                        spec=FileChangeSpec(
                            path="client.txt",
                            operation="write",
                            expected_sha256=None,
                            content="client\n",
                        ),
                        request_key="client-preview",
                    ),
                )
                applied = client.agent_workspace_change(
                    agent_id,
                    WorkspaceChangeAction(
                        proposal_id=proposal.proposal_id,
                        expected_revision=proposal.revision,
                        action="apply",
                        request_key="client-apply",
                    ),
                    confirmed=True,
                )
                assert applied.status == "done"
                page = client.agent_workspace_native_changes(agent_id, limit=1)
                request = NativeBatchRequest(
                    action="keep",
                    snapshot_revision=page.snapshot_revision,
                    targets=[
                        NativeBatchTarget(
                            proposal_id=item.proposal_id, expected_revision=item.revision
                        )
                        for item in page.items
                    ],
                    request_key="客户端/../?confirmed=false#%",
                )
                started = client.agent_workspace_start_native_batch(
                    agent_id, request, confirmed=True
                )
                result = client.agent_workspace_wait_native_batch(agent_id, started.batch_id)
                assert result.status == "done"
                assert client.agent_workspace_native_batch(agent_id, started.batch_id) == result
                assert (
                    client.agent_workspace_native_batch_by_key(agent_id, request.request_key)
                    == result
                )
                assert client.agent_workspace_native_batch_by_key(agent_id, "missing") is None
                assert (
                    client.agent_workspace_cancel_native_batch(agent_id, started.batch_id) == result
                )
                with pytest.raises(ClientError) as conflict:
                    client.agent_workspace_start_native_batch(
                        agent_id, request.model_copy(update={"request_key": "stale-key"})
                    )
                assert (
                    conflict.value.status == 409
                    and conflict.value.code == "workspace_list_revision_conflict"
                )
                return agent_id

        agent_id = await asyncio.to_thread(client_flow)
        assert (tmp_path / "client.txt").read_text() == "client\n"
        code, stdout, stderr = await run_cli(
            url, "native-changes", agent_id, "--filter", "active", "--limit", 1
        )
        assert code == 0, stderr
        page = json.loads(stdout)
        assert page["items"][0]["status"] == "kept"
        source = tmp_path / "batch.json"
        request = batch_request(page, "undo", "cli-ask")
        source.write_text(json.dumps(request))
        code, stdout, stderr = await run_cli(url, "start-batch", agent_id, "--request", source)
        assert code == 0, stderr  # 返回接受回执，后台权限判定通过 wait 查询。
        denied_id = json.loads(stdout)["batch_id"]
        code, stdout, stderr = await run_cli(url, "wait-batch", agent_id, denied_id)
        assert code == 1 and json.loads(stdout)["items"][0]["error_code"] == "approval_required", (
            stderr
        )
        assert (tmp_path / "client.txt").exists()
        request["request_key"] = "cli/批准?页#1"
        source.write_text(json.dumps(request))
        code, stdout, stderr = await run_cli(
            url, "start-batch", agent_id, "--request", source, "--confirm"
        )
        assert code == 0, stderr
        batch_id = json.loads(stdout)["batch_id"]
        code, stdout, stderr = await run_cli(
            url, "wait-batch", agent_id, batch_id, "--timeout-seconds", 30
        )
        assert code == 0 and json.loads(stdout)["status"] == "done", stderr
        for command in (
            ["native-batch", agent_id, "--batch-id", batch_id],
            ["batch-by-key", agent_id, "--request-key", request["request_key"]],
            ["cancel-batch", agent_id, batch_id],
        ):
            code, stdout, stderr = await run_cli(url, *command)
            assert code == 0 and json.loads(stdout)["batch_id"] == batch_id, stderr
            assert_public(json.loads(stdout))
        code, stdout, stderr = await run_cli(
            url, "batch-by-key", agent_id, "--request-key", "missing"
        )
        assert code == 0 and json.loads(stdout) is None, stderr
        code, stdout, stderr = await run_cli(url, "native-changes", agent_id, "--filter", "history")
        assert code == 0 and json.loads(stdout)["total"] == 1, stderr
        assert not (tmp_path / "client.txt").exists()
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        listener.close()


@pytest.mark.parametrize("case", ["missing", "invalid", "oversized", "internal-token"])
def test_cli_batch_request_validation_does_not_echo_input(tmp_path, case):
    source = tmp_path / "request.json"
    if case == "invalid":
        source.write_text('{"private":"PRIVATE-INPUT-CANARY"')
    elif case == "oversized":
        source.write_text("PRIVATE-INPUT-CANARY" + "x" * 65537)
    elif case == "internal-token":
        source.write_text(
            json.dumps(
                {
                    "action": "undo",
                    "snapshot_revision": "a" * 64,
                    "targets": [{"proposal_id": "proposal", "expected_revision": 1}],
                    "request_key": "key",
                    "token": "PRIVATE-INPUT-CANARY",
                }
            )
        )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "krim_agent",
            "workspace",
            "start-batch",
            "agent",
            "--request",
            str(source),
            "--url",
            "http://127.0.0.1:1",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 2
    assert "PRIVATE-INPUT-CANARY" not in result.stdout + result.stderr


def test_client_does_not_retry_lost_native_batch_response():
    calls = []

    def dropped(request):
        calls.append(request)
        raise httpx.ReadTimeout("accepted response lost")

    with KrimClient(transport=httpx.MockTransport(dropped)) as client:
        with pytest.raises(httpx.ReadTimeout):
            client.agent_workspace_start_native_batch(
                "agent",
                NativeBatchRequest(
                    action="undo",
                    snapshot_revision="a" * 64,
                    targets=[NativeBatchTarget(proposal_id="proposal", expected_revision=1)],
                    request_key="fixed-key",
                ),
                confirmed=True,
            )
    assert len(calls) == 1


@pytest.mark.parametrize("key", ["", "x" * 129, "line\nbreak"])
def test_client_validates_lookup_keys_before_request(key):
    calls = []

    def capture(request):
        calls.append(request)
        return httpx.Response(200, json=None)

    with KrimClient(transport=httpx.MockTransport(capture)) as client:
        with pytest.raises(ClientError) as invalid:
            client.agent_workspace_native_batch_by_key("agent", key)
        assert invalid.value.status == 422
    assert calls == []


def test_client_native_wait_extends_http_timeout_and_does_not_swallow_agent_error():
    captured = []

    def capture(request):
        captured.append(request)
        return httpx.Response(
            404, json={"error": {"code": "agent_not_found", "message": "Agent不存在"}}
        )

    with KrimClient(transport=httpx.MockTransport(capture)) as client:
        for call in (
            lambda: client.agent_workspace_wait_native_batch(
                "missing", "batch", timeout_seconds=300
            ),
            lambda: client.agent_workspace_native_batch_by_key("missing", "key"),
        ):
            with pytest.raises(ClientError) as missing:
                call()
            assert missing.value.code == "agent_not_found"
    assert captured[0].extensions["timeout"]["read"] == 305
