"""SDK/严格REST/真实TCP Client/CLI同原捕获；显式serve和模型拒绝。"""

import asyncio
import json
import os
import socket
import sys

import httpx
import pytest
from test_directory_checkpoint_capture import capture, counts, make_tree, setup
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import ClientError, KrimClient
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import DirectoryCheckpointCreate
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def cli(url, agent_id, action, value=None, *options):
    arguments = [
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "directory-checkpoints",
        action,
        agent_id,
    ]
    if value is not None:
        arguments.append(value)
    process = await asyncio.create_subprocess_exec(
        *arguments,
        "--url",
        url,
        *options,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, errors = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_sdk_rest_tcp_client_cli_original_fact_and_private_input_rejection(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent, _binding = await setup(runtime, tmp_path)
    try:
        key = "原始/../?confirmed=true#%&"
        request, final = await capture(agent, key)
        assert final.status == "available", final
        value = final.model_dump(mode="json")

        def no_read(*_args, **_kwargs):
            pytest.fail("公开消费者重读原任务不得再次访问目录")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", no_read)
        async with api_for(runtime, service, monkeypatch) as (app, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/directory-checkpoints"
            assert (await api.get(base + "/by-key", params={"request_key": key})).json() == value
            for private in (
                {"token": "private"},
                {"material_id": "fake"},
                {"snapshot": {}},
                {"host_path": "/outside"},
            ):
                rejected = await api.post(base, json={**request.model_dump(mode="json"), **private})
                assert rejected.status_code == 422, rejected.text
            for path, params in (
                (base, {"host_path": "/outside"}),
                (base + "/by-key", [("request_key", key), ("request_key", "other")]),
                (base, {"offset": -1}),
                (base, {"limit": 33}),
            ):
                rejected = await api.get(path, params=params)
                assert rejected.status_code == 422, rejected.text
            assert (
                await api.post(
                    base + f"/{final.checkpoint_id}/wait", params={"timeout_seconds": "nan"}
                )
            ).status_code == 422
            other = await service.spawn("directory-capture")
            assert (
                await api.get(
                    f"/v1/agents/{other.agent_id}/workspace/directory-checkpoints/{final.checkpoint_id}"
                )
            ).status_code == 404
            async with tcp_for(app) as url:

                def client_calls():
                    with KrimClient(url) as client:
                        assert (
                            client.agent_workspace_create_directory_checkpoint(
                                agent.agent_id, request
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_directory_checkpoint(
                                agent.agent_id, final.checkpoint_id
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_directory_checkpoint_by_key(agent.agent_id, key)
                            == final
                        )
                        assert client.agent_workspace_directory_checkpoints(agent.agent_id) == [
                            final
                        ]
                        assert (
                            client.agent_workspace_wait_directory_checkpoint(
                                agent.agent_id, final.checkpoint_id, timeout_seconds=0
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_cancel_directory_checkpoint(
                                agent.agent_id, final.checkpoint_id
                            )
                            == final
                        )

                await asyncio.to_thread(client_calls)
                assert await cli(url, agent.agent_id, "get", final.checkpoint_id) == value
                assert await cli(url, agent.agent_id, "by-key", key) == value
                assert await cli(url, agent.agent_id, "list") == [value]
                assert (
                    await cli(
                        url, agent.agent_id, "wait", final.checkpoint_id, "--timeout-seconds", "0"
                    )
                    == value
                )
                request_file = tmp_path / "request.json"
                request_file.write_text(request.model_dump_json(), encoding="utf-8")
                assert (
                    await cli(url, agent.agent_id, "create", None, "--request", str(request_file))
                    == value
                )
        assert await counts(service) == (1, 1)
    finally:
        await service.close()


async def test_client_rejects_wrong_scope_id_request_and_duplicate_page(runtime, tmp_path):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        request, final = await capture(agent)
        for fault in ("scope", "id", "request", "page"):
            data = final.model_dump(mode="json")
            if fault == "scope":
                data["scope_id"] = "session:other"
            elif fault == "id":
                data["checkpoint_id"] = "other"
            elif fault == "request":
                data["root_path"] = "other"
            http = httpx.Client(
                transport=httpx.MockTransport(
                    lambda _request, data=data, fault=fault: httpx.Response(
                        200, json=[data, data] if fault == "page" else data
                    )
                ),
                base_url="http://127.0.0.1",
            )
            with KrimClient("http://127.0.0.1") as client:
                client._http.close()
                client._http = http
                with pytest.raises(ClientError) as rejected:
                    if fault == "page":
                        client.agent_workspace_directory_checkpoints(agent.agent_id)
                    elif fault == "request":
                        client.agent_workspace_create_directory_checkpoint(agent.agent_id, request)
                    else:
                        client.agent_workspace_directory_checkpoint(
                            agent.agent_id, final.checkpoint_id
                        )
                assert rejected.value.code == "workspace_directory_capture_response"
    finally:
        await service.close()


async def test_real_model_run_cannot_forge_host_directory_fence(runtime, tmp_path, monkeypatch):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    calls = 0

    class Provider:
        name = "offline-directory-capture-boundary"

        async def stream(self, _messages, _tools, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                yield ModelToolCall(
                    id="original-directory-call",
                    name="workspace_directory_checkpoint_capture",
                    arguments={"root_path": "tree"},
                )
            else:
                yield ModelText(text="保留原私有资格拒绝")

    def no_read(*_args, **_kwargs):
        pytest.fail("原模型调用未绑定目录生产者，不能访问Reader")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", no_read)
    service._handle(agent.agent_id).runtime.provider = Provider()
    try:
        result = await agent.execute(
            "不得伪造原目录Fence", idempotency_key="model-directory-refusal"
        )
        assert result.status == "completed" and calls == 2, result
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 1 and audits[0].call_id == "original-directory-call"
        assert audits[0].run_id == result.run_id and not audits[0].ok
        assert audits[0].code == "workspace_directory_capture_context"
        assert await counts(service) == (0, 0)
    finally:
        await service.close()


async def test_actual_serve_explicit_directory_flag_and_same_key_no_new_task(tmp_path):
    files = tmp_path / "files"
    files.mkdir()
    make_tree(files)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--workspace",
        str(files),
        "--workspace-write",
        "--directory-checkpoints",
        cwd=str(tmp_path),
        env={
            **os.environ,
            "KRIM_DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path / 'serve.sqlite'}",
            "KRIM_API_KEY": "synthetic-directory-key",
        },
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": "Bearer synthetic-directory-key"},
        ) as api:
            async with asyncio.timeout(10):
                while True:
                    if process.returncode is not None:
                        _, errors = await process.communicate()
                        raise AssertionError(errors.decode())
                    try:
                        if (await api.get("/healthz")).is_success:
                            break
                    except httpx.TransportError:
                        pass
                    await asyncio.sleep(0.05)
            created = await api.post("/v1/agents", json={"name": "default"})
            assert created.status_code == 201, created.text
            agent_id = created.json()["agent_id"]
            base = f"/v1/agents/{agent_id}/workspace/directory-checkpoints"
            request = DirectoryCheckpointCreate(
                root_path="tree", request_key="serve-directory"
            ).model_dump(mode="json")
            started = await api.post(base, json=request)
            assert started.status_code == 202, started.text
            final = await api.post(base + f"/{started.json()['checkpoint_id']}/wait")
            assert final.status_code == 200 and final.json()["status"] == "available", final.text
            assert final.json()["snapshot"]["entry_count"] == 6
            assert (await api.post(base, json=request)).json() == final.json()
            assert len((await api.get(base)).json()) == 1
            assert (files / "tree/nested/nul").read_bytes() == b"\0\xff"
    finally:
        if process.returncode is None:
            process.terminate()
        await asyncio.wait_for(process.communicate(), 15)
