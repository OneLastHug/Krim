"""原生检查点公开管理的真实REST、TCP客户端、CLI和失败边界。"""

import asyncio
import json
import socket
import subprocess
import sys
from contextlib import asynccontextmanager

import httpx
import pytest
import uvicorn
from test_native_workspace_public_api import allow_or_ask, assert_public, native_api, run_cli

from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.sdk import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    WorkspaceCheckpointCreate,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def capture(api, base, paths=("file.txt",), key="capture"):
    result = await api.post(
        f"{base}/native-checkpoints", json={"paths": list(paths), "request_key": key}
    )
    assert result.status_code == 202, result.text
    result = await api.post(f"{base}/native-checkpoints/{result.json()['checkpoint_id']}/wait")
    assert result.status_code == 200 and result.json()["status"] == "available", result.text
    return result.json()


async def plan(api, base, checkpoint, key="preview"):
    result = await api.post(
        f"{base}/native-restores/preview",
        json={
            "checkpoint_id": checkpoint["checkpoint_id"],
            "request_key": key,
        },
    )
    assert result.status_code == 202, result.text
    result = await api.post(f"{base}/native-restores/{result.json()['plan_id']}/wait")
    assert result.status_code == 200 and result.json()["phase"] == "ready", result.text
    return result.json()


def execute_request(value, key="execute"):
    return {"plan_id": value["plan_id"], "expected_revision": value["revision"], "request_key": key}


async def test_rest_capture_restore_history_isolation_idempotency_and_original_file_receipts(
    runtime, tmp_path
):
    file = tmp_path / "file.txt"
    file.write_text("A\n")
    async with native_api(runtime, tmp_path) as (_app, api, agent_id):
        base = f"/v1/agents/{agent_id}/workspace"
        key = "原始/../?confirmed=true#%&"
        checkpoint = await capture(api, base, ("file.txt", "missing.txt"), key)
        file.write_text("B\n")
        (tmp_path / "missing.txt").write_text("added")
        restore = await plan(api, base, checkpoint, key)
        assert restore["baseline_checkpoint_id"] != checkpoint["checkpoint_id"]
        assert "-B" in restore["items"][0]["proposal"]["preview"]["diff"]
        request = execute_request(restore)
        mismatch = await api.post(
            f"{base}/native-restores/wrong/execute", json=request, params={"confirmed": True}
        )
        assert (
            mismatch.status_code == 422
            and mismatch.json()["error"]["code"] == "workspace_restore_mismatch"
        )
        started = await api.post(
            f"{base}/native-restores/{restore['plan_id']}/execute",
            json=request,
            params={"confirmed": True},
        )
        assert started.status_code == 202, started.text
        result = await api.post(f"{base}/native-restores/{restore['plan_id']}/wait")
        done = result.json()
        assert done["status"] == "done" and file.read_text() == "A\n", done
        assert not (tmp_path / "missing.txt").exists()
        assert_public(done)
        assert str(tmp_path) not in json.dumps(done)
        for kind, identifier, expected in (
            ("native-checkpoints", checkpoint["checkpoint_id"], checkpoint),
            ("native-restores", restore["plan_id"], done),
        ):
            assert (await api.get(f"{base}/{kind}/{identifier}")).json() == expected
            assert (
                await api.get(f"{base}/{kind}/by-key", params={"request_key": key})
            ).json() == expected
            assert (
                await api.get(f"{base}/{kind}/by-key", params={"request_key": "absent"})
            ).json() is None
            history = await api.get(f"{base}/{kind}", params={"offset": 0, "limit": 1})
            assert history.status_code == 200 and len(history.json()) == 1
            assert_public(history.json())
            assert (await api.post(f"{base}/{kind}/{identifier}/cancel")).json() == expected
        repeated = await api.post(
            f"{base}/native-restores/{restore['plan_id']}/execute", json=request
        )
        assert repeated.json() == done
        changed_key = await api.post(
            f"{base}/native-restores/{restore['plan_id']}/execute",
            json={**request, "request_key": "new-key"},
        )
        assert changed_key.status_code == 409
        operation = done["items"][0]["operation"]
        assert (await api.get(f"{base}/operations/{operation['operation_id']}")).json() == operation
        other = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        for kind, identifier in (
            ("native-checkpoints", checkpoint["checkpoint_id"]),
            ("native-restores", restore["plan_id"]),
        ):
            foreign = f"/v1/agents/{other}/workspace/{kind}"
            assert (await api.get(f"{foreign}/{identifier}")).status_code == 404
            assert (await api.get(f"{foreign}/by-key", params={"request_key": key})).json() is None
        state = (await api.get(f"/v1/agents/{agent_id}/session")).json()
        assert (
            await api.post(
                f"/v1/agents/{agent_id}/session/close",
                json={"expected_revision": state["revision"]},
            )
        ).status_code == 200
        assert (await api.get(f"{base}/native-restores/{restore['plan_id']}")).json() == done
        assert (await api.get(f"{base}/native-checkpoints")).status_code == 200
        assert (
            await api.post(
                f"{base}/native-restores/preview",
                json={"checkpoint_id": checkpoint["checkpoint_id"], "request_key": "closed"},
            )
        ).status_code == 410


@pytest.mark.parametrize("phase", ["capture", "prepare", "execute"])
async def test_rest_wait_timeout_and_cancel_stop_the_original_work(runtime, tmp_path, phase):
    file = tmp_path / "file.txt"
    file.write_text("A")
    async with native_api(runtime, tmp_path) as (app, api, agent_id):
        base = f"/v1/agents/{agent_id}/workspace"
        checkpoint = await capture(api, base) if phase != "capture" else None
        file.write_text("B")
        restore = await plan(api, base, checkpoint) if phase == "execute" else None
        entered, release = asyncio.Event(), asyncio.Event()

        async def pause(_definition, context):
            if context.checkpoint_capture or context.checkpoint_restore:
                entered.set()
                await release.wait()

        registry = app.state.agents._handle(agent_id).runtime.tools
        registry.add_global_pre(pause)
        try:
            if phase == "capture":
                started = await api.post(
                    f"{base}/native-checkpoints",
                    json={"paths": ["file.txt"], "request_key": "blocked"},
                )
                resource = f"{base}/native-checkpoints/{started.json()['checkpoint_id']}"
            elif phase == "prepare":
                started = await api.post(
                    f"{base}/native-restores/preview",
                    json={"checkpoint_id": checkpoint["checkpoint_id"], "request_key": "blocked"},
                )
                resource = f"{base}/native-restores/{started.json()['plan_id']}"
            else:
                started = await api.post(
                    f"{base}/native-restores/{restore['plan_id']}/execute",
                    json=execute_request(restore),
                    params={"confirmed": True},
                )
                resource = f"{base}/native-restores/{restore['plan_id']}"
            assert started.status_code == 202, started.text
            await asyncio.wait_for(entered.wait(), 5)
            waiting = await api.post(resource + "/wait", params={"timeout_seconds": 0})
            assert waiting.json()["status"] in {"capturing", "preparing", "running"}
            cancelled = await api.post(resource + "/cancel")
            assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled", (
                cancelled.text
            )
            assert file.read_text() == "B"
            if phase == "prepare":
                backup = cancelled.json()["baseline_checkpoint_id"]
                assert (await api.get(f"{base}/native-checkpoints/{backup}")).json()[
                    "status"
                ] == "cancelled"
            assert_public(cancelled.json())
        finally:
            release.set()
            registry._global_pre.clear()


async def test_rest_denied_unconfirmed_restore_requires_a_new_plan(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("A")
    async with native_api(runtime, tmp_path) as (_app, api, agent_id):
        base = f"/v1/agents/{agent_id}/workspace"
        checkpoint = await capture(api, base)
        file.write_text("B")
        restore = await plan(api, base, checkpoint)
        request = execute_request(restore)
        await api.post(f"{base}/native-restores/{restore['plan_id']}/execute", json=request)
        denied = (await api.post(f"{base}/native-restores/{restore['plan_id']}/wait")).json()
        assert (
            denied["status"] == "failed" and denied["items"][0]["error_code"] == "approval_required"
        )
        repeated = await api.post(
            f"{base}/native-restores/{restore['plan_id']}/execute",
            json=request,
            params={"confirmed": True},
        )
        assert repeated.json() == denied and file.read_text() == "B"
        fresh = await plan(api, base, checkpoint, key="fresh")
        await api.post(
            f"{base}/native-restores/{fresh['plan_id']}/execute",
            json=execute_request(fresh, "fresh-execute"),
            params={"confirmed": True},
        )
        assert (await api.post(f"{base}/native-restores/{fresh['plan_id']}/wait")).json()[
            "status"
        ] == "done"


async def test_rest_checkpoint_boundaries_strict_models_pagination_and_local_origin(
    runtime, tmp_path
):
    async with native_api(runtime, tmp_path) as (_app, api, agent_id):
        base = f"/v1/agents/{agent_id}/workspace"
        for kind in ("native-checkpoints", "native-restores"):
            for params in (
                {"limit": 0},
                {"limit": 33},
                {"offset": -1},
                {"offset": 9007199254740992},
            ):
                assert (await api.get(f"{base}/{kind}", params=params)).status_code == 422
            for key in ("", "x" * 129, "line\nbreak", "trailing\n", "\x00key"):
                assert (
                    await api.get(f"{base}/{kind}/by-key", params={"request_key": key})
                ).status_code == 422
            for seconds in (-1, 301, "nan", "inf"):
                assert (
                    await api.post(
                        f"{base}/{kind}/missing/wait", params={"timeout_seconds": seconds}
                    )
                ).status_code == 422
            assert (await api.get(f"{base}/{kind}/bad%3Fquery")).status_code == 422
        for suffix, payload in (
            ("native-checkpoints", {"paths": ["file.txt"], "request_key": "key"}),
            ("native-restores/preview", {"checkpoint_id": "checkpoint", "request_key": "key"}),
            (
                "native-restores/plan/execute",
                {"plan_id": "plan", "expected_revision": 1, "request_key": "key"},
            ),
        ):
            for field in ("root", "token", "owner_id", "after_content", "receipt"):
                response = await api.post(
                    f"{base}/{suffix}", json={**payload, field: "PRIVATE-INPUT-CANARY"}
                )
                assert response.status_code == 422 and "PRIVATE-INPUT-CANARY" not in response.text
            assert (await api.post(f"{base}/{suffix}", content=b"x" * 65537)).status_code == 413
            blocked = await api.post(
                f"{base}/{suffix}", json=payload, headers={"Origin": "https://untrusted.example"}
            )
            assert blocked.status_code == 403 and blocked.json()["error"]["code"] == "origin_denied"
        for identifier in ("../path", "bad?query", "trailing\n", "x" * 37):
            response = await api.post(
                f"{base}/native-restores/preview",
                json={"checkpoint_id": identifier, "request_key": "key"},
            )
            assert response.status_code == 422
        for ttl in (0, 86401, "nan"):
            response = await api.post(
                f"{base}/native-restores/preview",
                json={"checkpoint_id": "id", "request_key": "key", "ttl_seconds": ttl},
            )
            assert response.status_code == 422


@asynccontextmanager
async def tcp_api(runtime, root):
    runtime.tool_policy = allow_or_ask
    app = create_app(runtime=runtime, workspace_root=root, workspace_writable=True)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(15):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield app, f"http://127.0.0.1:{listener.getsockname()[1]}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 15)
        listener.close()


async def test_tcp_client_and_cli_restore_same_checkpoint(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("A")
    async with tcp_api(runtime, tmp_path) as (_app, url):

        def client_flow():
            with KrimClient(url) as client:
                agent = client.agent_spawn("default").agent_id
                checkpoint = client.agent_workspace_create_checkpoint(
                    agent,
                    WorkspaceCheckpointCreate(paths=["file.txt"], request_key="client/原始?key#"),
                )
                checkpoint = client.agent_workspace_wait_checkpoint(agent, checkpoint.checkpoint_id)
                assert checkpoint.status == "available"
                assert (
                    client.agent_workspace_checkpoint(agent, checkpoint.checkpoint_id) == checkpoint
                )
                assert (
                    client.agent_workspace_checkpoint_by_key(agent, checkpoint.request_key)
                    == checkpoint
                )
                assert client.agent_workspace_checkpoint_by_key(agent, "absent") is None
                assert client.agent_workspace_checkpoints(agent) == [checkpoint]
                assert (
                    client.agent_workspace_cancel_checkpoint(agent, checkpoint.checkpoint_id)
                    == checkpoint
                )
                file.write_text("B")
                restore = client.agent_workspace_preview_restore(
                    agent,
                    CheckpointRestorePreview(
                        checkpoint_id=checkpoint.checkpoint_id, request_key="client/恢复?key#"
                    ),
                )
                restore = client.agent_workspace_wait_restore(agent, restore.plan_id)
                request = CheckpointRestoreExecute(
                    plan_id=restore.plan_id,
                    expected_revision=restore.revision,
                    request_key="client-execute",
                )
                client.agent_workspace_execute_restore(agent, request, confirmed=True)
                done = client.agent_workspace_wait_restore(agent, restore.plan_id)
                assert done.status == "done" and file.read_text() == "A", done
                assert client.agent_workspace_restore(agent, restore.plan_id) == done
                assert client.agent_workspace_restore_by_key(agent, restore.request_key) == done
                assert client.agent_workspace_restore_by_key(agent, "absent") is None
                assert client.agent_workspace_restores(agent) == [done]
                assert client.agent_workspace_cancel_restore(agent, restore.plan_id) == done
                assert client.agent_workspace_execute_restore(agent, request) == done
                return agent, checkpoint.checkpoint_id

        agent, checkpoint_id = await asyncio.to_thread(client_flow)
        source = tmp_path / "request.json"
        source.write_text(json.dumps({"paths": ["file.txt"], "request_key": "cli-capture"}))
        code, output, error = await run_cli(url, "create-checkpoint", agent, "--request", source)
        assert code == 0, error
        cli_checkpoint = json.loads(output)["checkpoint_id"]
        code, output, error = await run_cli(url, "wait-checkpoint", agent, cli_checkpoint)
        assert code == 0 and json.loads(output)["status"] == "available", error
        file.write_text("C")
        source.write_text(
            json.dumps({"checkpoint_id": checkpoint_id, "request_key": "cli-preview"})
        )
        code, output, error = await run_cli(url, "preview-restore", agent, "--request", source)
        assert code == 0, error
        plan_id = json.loads(output)["plan_id"]
        code, output, error = await run_cli(url, "wait-restore", agent, plan_id)
        ready = json.loads(output)
        assert code == 0 and ready["phase"] == "ready", (error, ready)
        code, output, error = await run_cli(
            url,
            "execute-restore",
            agent,
            plan_id,
            "--revision",
            ready["revision"],
            "--request-key",
            "cli-execute",
            "--confirm",
        )
        assert code == 0, error
        code, output, error = await run_cli(url, "wait-restore", agent, plan_id)
        assert code == 0 and json.loads(output)["status"] == "done" and file.read_text() == "A", (
            error
        )
        for command in (
            ("checkpoints", agent),
            ("checkpoint", agent, cli_checkpoint),
            ("checkpoint-by-key", agent, "--request-key", "cli-capture"),
            ("cancel-checkpoint", agent, cli_checkpoint),
            ("restores", agent),
            ("restore", agent, plan_id),
            ("restore-by-key", agent, "--request-key", "cli-preview"),
            ("cancel-restore", agent, plan_id),
        ):
            code, output, error = await run_cli(url, *command)
            assert code == 0 and json.loads(output), error
            assert_public(json.loads(output))
        code, output, error = await run_cli(url, "restore-by-key", agent, "--request-key", "absent")
        assert code == 0 and json.loads(output) is None, error
        file.write_text("D")
        source.write_text(json.dumps({"checkpoint_id": checkpoint_id, "request_key": "cli-denied"}))
        code, output, error = await run_cli(url, "preview-restore", agent, "--request", source)
        assert code == 0, error
        denied_id = json.loads(output)["plan_id"]
        code, output, error = await run_cli(url, "wait-restore", agent, denied_id)
        denied_ready = json.loads(output)
        assert code == 0 and denied_ready["phase"] == "ready", (error, denied_ready)
        code, output, error = await run_cli(
            url,
            "execute-restore",
            agent,
            denied_id,
            "--revision",
            denied_ready["revision"],
            "--request-key",
            "cli-denied-execute",
        )
        assert code == 0, error
        code, output, error = await run_cli(url, "wait-restore", agent, denied_id)
        assert code == 1 and json.loads(output)["items"][0]["error_code"] == "approval_required", (
            output,
            error,
        )
        code, output, error = await run_cli(url, "restore", agent, denied_id)
        assert code == 0 and json.loads(output)["status"] == "failed" and file.read_text() == "D", (
            error
        )


async def test_real_tcp_response_lost_after_acceptance_is_found_without_reexecution(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("A")
    async with tcp_api(runtime, tmp_path) as (app, url):
        async with httpx.AsyncClient(base_url=url) as api:
            agent = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
            base = f"/v1/agents/{agent}/workspace"
            checkpoint = await capture(api, base)
            file.write_text("B")
            restore = await plan(api, base, checkpoint)
            original = app.state.agents.workspace_restores.start
            accepted, release = asyncio.Event(), asyncio.Event()

            async def delayed(*args, **kwargs):
                result = await original(*args, **kwargs)
                accepted.set()
                await release.wait()
                return result

            monkeypatch.setattr(app.state.agents.workspace_restores, "start", delayed)
            try:
                pending = asyncio.create_task(
                    api.post(
                        f"{base}/native-restores/{restore['plan_id']}/execute",
                        json=execute_request(restore),
                        params={"confirmed": True},
                        timeout=15,
                    )
                )
                await asyncio.wait_for(accepted.wait(), 10)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
                done = (await api.post(f"{base}/native-restores/{restore['plan_id']}/wait")).json()
                assert done["status"] == "done" and file.read_text() == "A", done
                found = await api.get(
                    f"{base}/native-restores/by-key", params={"request_key": restore["request_key"]}
                )
                assert found.json() == done
                release.set()
                repeat = await api.post(
                    f"{base}/native-restores/{restore['plan_id']}/execute",
                    json=execute_request(restore),
                )
                assert repeat.json() == done
                history = (
                    await api.get(
                        f"{base}/changes/{done['items'][0]['proposal']['proposal_id']}/history"
                    )
                ).json()
                assert len(history) == 1
            finally:
                release.set()


@pytest.mark.parametrize("action", ["create-checkpoint", "preview-restore"])
@pytest.mark.parametrize("fault", ["missing", "invalid", "oversized", "private-field"])
def test_cli_checkpoint_request_errors_are_bounded_and_do_not_echo_input(tmp_path, action, fault):
    source = tmp_path / "request.json"
    if fault == "invalid":
        source.write_text('{"private":"PRIVATE-INPUT-CANARY"')
    elif fault == "oversized":
        source.write_text("PRIVATE-INPUT-CANARY" + "x" * 65537)
    elif fault == "private-field":
        payload = (
            {"paths": ["file.txt"]}
            if action == "create-checkpoint"
            else {"checkpoint_id": "checkpoint"}
        )
        source.write_text(
            json.dumps({**payload, "request_key": "key", "token": "PRIVATE-INPUT-CANARY"})
        )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "krim_agent",
            "workspace",
            action,
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
    assert result.returncode == 2 and "PRIVATE-INPUT-CANARY" not in result.stdout + result.stderr


@pytest.mark.parametrize("kind", ["capture", "restore"])
def test_client_rejects_invalid_keys_ids_waits_and_extends_transport_timeout(kind):
    calls = []

    def observe(request):
        calls.append(request)
        return httpx.Response(
            404, json={"error": {"code": "agent_not_found", "message": "Agent不存在"}}
        )

    with KrimClient(transport=httpx.MockTransport(observe)) as client:
        lookup = (
            client.agent_workspace_checkpoint_by_key
            if kind == "capture"
            else client.agent_workspace_restore_by_key
        )
        get = (
            client.agent_workspace_checkpoint
            if kind == "capture"
            else client.agent_workspace_restore
        )
        wait = (
            client.agent_workspace_wait_checkpoint
            if kind == "capture"
            else client.agent_workspace_wait_restore
        )
        for key in ("", "x" * 129, "line\nbreak"):
            with pytest.raises(ClientError):
                lookup("agent", key)
        for identifier in ("../outside", "bad?query", "x" * 37):
            with pytest.raises(ClientError):
                get("agent", identifier)
        for seconds in (-1, 301, float("nan"), float("inf")):
            with pytest.raises(ClientError):
                wait("agent", "id", timeout_seconds=seconds)
        assert not calls
        with pytest.raises(ClientError) as missing:
            wait("agent", "id", timeout_seconds=300)
        assert (
            missing.value.code == "agent_not_found"
            and calls[0].extensions["timeout"]["read"] == 305
        )


@pytest.mark.parametrize("operation", ["capture", "preview", "execute"])
def test_client_does_not_retry_checkpoint_posts_after_lost_response(operation):
    calls = []

    def lost(request):
        calls.append(request)
        raise httpx.ReadTimeout("accepted response lost")

    with KrimClient(transport=httpx.MockTransport(lost)) as client:
        with pytest.raises(httpx.ReadTimeout):
            if operation == "capture":
                client.agent_workspace_create_checkpoint(
                    "agent", WorkspaceCheckpointCreate(paths=["file.txt"], request_key="key")
                )
            elif operation == "preview":
                client.agent_workspace_preview_restore(
                    "agent", CheckpointRestorePreview(checkpoint_id="checkpoint", request_key="key")
                )
            else:
                client.agent_workspace_execute_restore(
                    "agent",
                    CheckpointRestoreExecute(
                        plan_id="plan", expected_revision=1, request_key="key"
                    ),
                    confirmed=True,
                )
    assert len(calls) == 1
