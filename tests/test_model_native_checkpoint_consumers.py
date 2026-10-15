"""模型生产的原检查点由SDK/真实TCP/Client/CLI与serve同事实消费。"""

import asyncio
import json
import os
import socket
import sys

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from test_model_native_checkpoints import NAMES, OneShotProvider, setup
from test_tree_move_public_consumers import tcp_for

import krim_agent.api.server as server_module
from krim_agent.client import KrimClient
from krim_agent.persistence import Database
from krim_agent.persistence.database import NativeCheckpointModelSourceRow
from krim_agent.workspace_writer import WorkspaceWriter


async def test_real_model_result_tcp_client_cli_and_sdk_read_same_original_without_capture(
    runtime, tmp_path, monkeypatch
):
    (tmp_path / "plain.txt").write_text("same-original")
    provider = OneShotProvider()
    service, agent = await setup(runtime, tmp_path, provider)
    monkeypatch.setattr(server_module, "AgentService", lambda *_args, **_kwargs: service)
    app = server_module.create_app(runtime=runtime)
    try:
        result = await agent.execute("模型创建后跨入口只读", idempotency_key="tcp-original")
        original = await agent.file_checkpoint_by_key("one-root")
        assert result.status == "completed" and original.status == "available"

        def forbidden(*_args, **_kwargs):
            raise AssertionError("只读不能重新捕获或重新调用模型")

        monkeypatch.setattr(WorkspaceWriter, "capture_file", forbidden)
        monkeypatch.setattr(provider, "stream", forbidden)
        async with tcp_for(app) as url:
            with KrimClient(url) as client:
                assert (
                    await asyncio.to_thread(
                        client.agent_workspace_checkpoint, agent.agent_id, original.checkpoint_id
                    )
                    == original
                )
                assert (
                    await asyncio.to_thread(
                        client.agent_workspace_checkpoint_by_key, agent.agent_id, "one-root"
                    )
                    == original
                )
            for command, options in [
                ("checkpoint", [original.checkpoint_id]),
                ("checkpoint-by-key", ["--request-key", "one-root"]),
            ]:
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    "-m",
                    "krim_agent",
                    "workspace",
                    command,
                    agent.agent_id,
                    *options,
                    "--url",
                    url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                output, error = await asyncio.wait_for(process.communicate(), 15)
                assert process.returncode == 0, error.decode()
                assert json.loads(output) == original.model_dump(mode="json")
    finally:
        await service.close()


async def test_real_serve_explicit_controls_local_compatible_provider_and_public_approval(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "binary.bin").write_bytes(b"\0\xff")
    stub = FastAPI()
    model_calls = []

    @stub.post("/v1/chat/completions")
    async def completion(request: Request):
        payload = await request.json()
        model_calls.append(payload)
        if payload["messages"][-1]["role"] == "tool":
            original = json.loads(payload["messages"][-1]["content"])
            assert original["status"] == "available"
            delta, finish = {"content": "离线兼容端点已核实原检查点"}, "stop"
        else:
            delta = {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "serve-model-create",
                        "type": "function",
                        "function": {
                            "name": "workspace_checkpoint_create",
                            "arguments": json.dumps(
                                {
                                    "paths": ["binary.bin"],
                                    "request_key": "serve-model",
                                    "capture_format": "bytes",
                                }
                            ),
                        },
                    }
                ]
            }
            finish = "tool_calls"
        frames = [
            {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": finish}]},
        ]
        data = (
            "".join("data: " + json.dumps(frame) + "\n\n" for frame in frames) + "data: [DONE]\n\n"
        )
        return StreamingResponse(iter([data]), media_type="text/event-stream")

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'serve.sqlite'}")
    process = running = None
    try:
        async with tcp_for(stub) as provider_url:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                "-m",
                "krim_agent",
                "serve",
                "--port",
                str(port),
                "--workspace",
                str(root),
                "--workspace-write",
                "--checkpoint-bytes",
                "--checkpoint-controls",
                cwd=str(tmp_path),
                env={
                    **os.environ,
                    "KRIM_AGENT_DATABASE_URL": str(database.engine.url),
                    "KRIM_AGENT_API_KEY": "fictional-model-serve-key",
                    "KRIM_AGENT_PROVIDER": "openai-compatible",
                    "KRIM_AGENT_PROVIDER_BASE_URL": provider_url + "/v1",
                    "KRIM_AGENT_PROVIDER_MODEL": "offline-synthetic",
                    "KRIM_AGENT_PROVIDER_API_KEY": "fictional-local-only",
                    "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
                },
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}",
                timeout=40,
                headers={"Authorization": "Bearer fictional-model-serve-key"},
            ) as api:
                async with asyncio.timeout(15):
                    while True:
                        if process.returncode is not None:
                            _out, error = await process.communicate()
                            raise AssertionError(error.decode())
                        try:
                            if (await api.get("/healthz")).is_success:
                                break
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.05)
                profile = await api.put(
                    "/v1/agents/profiles/serve-model",
                    json={
                        "expected_revision": 0,
                        "config": {
                            "name": "serve-model",
                            "description": "本机离线Model",
                            "workspace_key": "local",
                            "tool_names": NAMES,
                        },
                    },
                )
                assert profile.is_success, profile.text
                spawned = await api.post("/v1/agents", json={"name": "serve-model"})
                assert spawned.status_code == 201, spawned.text
                identifier = spawned.json()["agent_id"]
                running = asyncio.create_task(
                    api.post(
                        f"/v1/agents/{identifier}/execute",
                        json={
                            "prompt": "请创建原生检查点",
                            "idempotency_key": "serve-original",
                        },
                    )
                )
                async with asyncio.timeout(15):
                    pending = []
                    while not pending:
                        current = (await api.get(f"/v1/agents/{identifier}")).json()
                        run_id = current.get("current_run_id")
                        if run_id:
                            pending = (
                                await api.get(
                                    f"/v1/runs/{run_id}/interactions",
                                    params={"pending_only": "true"},
                                )
                            ).json()
                        if not pending:
                            await asyncio.sleep(0.05)
                approved = await api.post(
                    f"/v1/runs/{run_id}/interactions/{pending[0]['id']}/respond",
                    json={"action": "approve"},
                )
                assert approved.status_code == 200, approved.text
                response = await asyncio.wait_for(running, 30)
                assert response.status_code == 200, response.text
                assert response.json()["result"]["status"] == "completed", response.text
                base = f"/v1/agents/{identifier}/workspace/native-checkpoints"
                original = (
                    await api.get(base + "/by-key", params={"request_key": "serve-model"})
                ).json()
                assert (
                    original["status"] == "available"
                    and original["files"][0]["content_kind"] == "binary"
                )
                assert len(model_calls) == 2
                assert (await api.get(base + "/" + original["checkpoint_id"])).json() == original
                assert len(model_calls) == 2
                async with database.sessions() as sql:
                    source = await sql.scalar(select(NativeCheckpointModelSourceRow))
                    assert source.run_id == run_id and source.call_id == "serve-model-create"
    finally:
        if running is not None and not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        if process is not None and process.returncode is None:
            process.terminate()
            await asyncio.wait_for(process.communicate(), 15)
        await database.close()
