"""bytes公共捕获同SDK/REST/TCP/CLI事实与严格错回程，恢复基线另单元。"""

import asyncio
import copy
import json
import socket
import sys

import httpx
import pytest
from test_native_checkpoint_byte_capture import DATA, capture, setup
from test_tree_move_public_consumers import tcp_for

import krim_agent.api.server as server_module
from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.usage import TokenUsage


async def test_actual_serve_cli_explicit_bytes_register_captures_and_same_key_reads(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "nul").write_bytes(b"\0\xff")
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
        str(root),
        "--workspace-write",
        "--checkpoint-bytes",
        cwd=str(tmp_path),
        env={
            **__import__("os").environ,
            "KRIM_DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path / 'serve.sqlite'}",
            "KRIM_API_KEY": "byte-test-key",
        },
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", headers={"Authorization": "Bearer byte-test-key"}
        ) as api:
            for _ in range(100):
                if process.returncode is not None:
                    _, error = await process.communicate()
                    raise AssertionError(error.decode())
                try:
                    if (await api.get("/healthz")).is_success:
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.05)
            created = await api.post("/v1/agents", json={"name": "default"})
            assert created.status_code == 201, created.text
            agent_id = created.json()["agent_id"]
            base = f"/v1/agents/{agent_id}/workspace/native-checkpoints"
            request = {
                "paths": ["nul", "missing"],
                "request_key": "serve-bytes",
                "capture_format": "bytes",
            }
            capture = await api.post(base, json=request)
            assert capture.status_code == 202, capture.text
            waited = await api.post(base + f"/{capture.json()['checkpoint_id']}/wait")
            assert waited.status_code == 200, waited.text
            final = waited.json()
            assert (
                final["status"] == "available" and final["files"][0]["content_kind"] == "binary"
            ), final
            assert (await api.post(base, json=request)).json() == final
            assert (root / "nul").read_bytes() == b"\0\xff"
    finally:
        if process.returncode is None:
            process.terminate()
            await asyncio.wait_for(process.communicate(), 15)


async def test_sdk_http_tcp_and_real_cli_same_bytes_key_metadata_scope_and_no_second_capture(
    runtime, tmp_path, monkeypatch
):
    for path, data in DATA.items():
        if data is not None:
            (tmp_path / path).write_bytes(data)
    service, agent, _ = await setup(runtime, tmp_path)
    monkeypatch.setattr(server_module, "AgentService", lambda *_args, **_kwargs: service)
    app = create_app(runtime=runtime)
    base = f"/v1/agents/{agent.agent_id}/workspace/native-checkpoints"
    try:
        request, final = await capture(agent, list(DATA))
        assert final.status == "available", final
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(base + "/by-key", params={"request_key": request.request_key})
            assert response.status_code == 200 and response.json() == final.model_dump(mode="json")
            for private in ({"token": "private"}, {"content": "raw"}, {"capture_format": "binary"}):
                rejected = await api.post(base, json={**request.model_dump(mode="json"), **private})
                assert rejected.status_code == 422
        async with tcp_for(app) as url:
            with KrimClient(url) as client:
                assert (
                    await asyncio.to_thread(
                        client.agent_workspace_create_checkpoint, agent.agent_id, request
                    )
                    == final
                )
                assert (
                    await asyncio.to_thread(
                        client.agent_workspace_checkpoint_by_key,
                        agent.agent_id,
                        request.request_key,
                    )
                    == final
                )
            path = tmp_path / "capture.json"
            path.write_text(request.model_dump_json())
            for args in (
                ["create-checkpoint", "--request", str(path)],
                ["checkpoint-by-key", "--request-key", request.request_key],
            ):
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    "-m",
                    "krim_agent",
                    "workspace",
                    args[0],
                    agent.agent_id,
                    *args[1:],
                    "--url",
                    url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, error = await asyncio.wait_for(process.communicate(), 15)
                assert process.returncode == 0, error.decode()
                assert json.loads(out) == final.model_dump(mode="json")
        assert len(service._handle(agent.agent_id).runtime.tools.audit) == len(DATA)
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["scope", "paths", "key", "title", "format", "id", "by-key"])
async def test_client_rejects_schema_valid_wrong_capture_facts(runtime, tmp_path, fault):
    (tmp_path / "nul").write_bytes(b"\0\xff")
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        request, final = await capture(agent, ["nul"])
        value = copy.deepcopy(final.model_dump(mode="json"))
        if fault == "scope":
            value["scope_id"] = "session:another"
        elif fault == "paths":
            value["files"][0]["path"] = "other"
        elif fault in {"key", "by-key"}:
            value["request_key"] = "other"
        elif fault == "title":
            value["title"] = "other"
        elif fault == "format":
            value.pop("capture_format")
            value["files"][0].pop("content_kind")
        elif fault == "id":
            value["checkpoint_id"] = "other"
        transport = httpx.MockTransport(lambda _request: httpx.Response(200, json=value))
        with KrimClient("http://127.0.0.1", transport=transport) as client:
            with pytest.raises(ClientError) as wrong:
                if fault == "id":
                    client.agent_workspace_checkpoint(agent.agent_id, final.checkpoint_id)
                elif fault == "by-key":
                    client.agent_workspace_checkpoint_by_key(agent.agent_id, request.request_key)
                else:
                    client.agent_workspace_create_checkpoint(agent.agent_id, request)
            assert wrong.value.code == "workspace_checkpoint_response" and wrong.value.status == 502
    finally:
        await service.close()


@pytest.mark.parametrize("enabled", [False, True])
async def test_factory_requires_explicit_byte_registration_and_new_profile_freeze(
    runtime, tmp_path, enabled
):
    app = create_app(
        runtime=runtime,
        workspace_root=tmp_path,
        workspace_writable=True,
        checkpoint_bytes_enabled=enabled,
    )
    service = app.state.agents
    try:
        view = await service.spawn("default")
        actual = {item.name for item in service._handle(view.agent_id).runtime.tools.definitions}
        assert ("workspace_checkpoint_capture" in actual) == enabled
    finally:
        await service.close()


async def test_real_offline_model_cannot_forge_private_capture_fence_and_keeps_original_run_audit(
    runtime, tmp_path
):
    (tmp_path / "nul").write_bytes(b"\0\xff")
    service, agent, _ = await setup(runtime, tmp_path)
    calls = 0

    class Provider:
        name = "offline-byte-capture-boundary"

        async def stream(self, messages, tools):
            nonlocal calls
            calls += 1
            assert any(item.name == "workspace_checkpoint_capture" for item in tools)
            if calls == 1:
                yield ModelToolCall(
                    id="model-private-capture",
                    name="workspace_checkpoint_capture",
                    arguments={"path": "nul"},
                )
            else:
                yield ModelText(text="没有原生私有Fence，保持拒绝")
            yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))

    handle = service._handle(agent.agent_id)
    handle.runtime.provider = Provider()
    try:
        result = await agent.execute("不得伪造字节捕获上下文", idempotency_key="model-boundary")
        assert result.status == "completed" and calls == 2
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 1 and audits[0].call_id == "model-private-capture"
        assert (
            audits[0].run_id == result.run_id
            and audits[0].code == "workspace_checkpoint_context"
            and not audits[0].ok
        )
        assert await agent.file_checkpoints() == []
        assert (tmp_path / "nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()
