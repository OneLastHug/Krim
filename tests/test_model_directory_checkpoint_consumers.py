"""原模型目录事实的SDK/REST/TCP/CLI和真实serve本机兼容端点消费。"""

import asyncio
import json
import os
import socket
import sys

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from test_directory_checkpoint_capture import make_tree
from test_directory_checkpoint_capture_consumers import cli
from test_model_directory_checkpoints import (
    CREATE,
    NAMES,
    QUERY,
    OneShotProvider,
    setup,
    source_count,
)
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import KrimClient
from krim_agent.persistence import Database
from krim_agent.persistence.database import WorkspaceDirectoryCaptureModelSourceRow
from krim_agent.sdk import AgentSession
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def test_real_model_capture_sdk_http_tcp_client_cli_share_cold_fact_without_producer_replay(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    provider = OneShotProvider()
    service, agent = await setup(runtime, tmp_path, provider)
    try:
        result = await agent.execute("模型目录后跨入口冷读", idempotency_key="consumers")
        original = await agent.directory_checkpoint_by_key("one-directory")
        assert result.status == "completed" and original.status == "available", (result, original)
        calls = provider.calls

        def forbidden(*_args, **_kwargs):
            raise AssertionError("原模型目录冷读不得重跑Reader/Provider")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "verify", forbidden)
        monkeypatch.setattr(provider, "stream", forbidden)
        async with api_for(runtime, service, monkeypatch) as (app, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/directory-checkpoints"
            assert (
                await api.get(base + "/by-key", params={"request_key": "one-directory"})
            ).json() == original.model_dump(mode="json")
            async with tcp_for(app) as url:

                def read():
                    with KrimClient(url) as client:
                        assert (
                            client.agent_workspace_directory_checkpoint(
                                agent.agent_id, original.checkpoint_id
                            )
                            == original
                        )
                        assert (
                            client.agent_workspace_directory_checkpoint_by_key(
                                agent.agent_id, "one-directory"
                            )
                            == original
                        )
                        assert (
                            client.agent_workspace_wait_directory_checkpoint(
                                agent.agent_id, original.checkpoint_id, timeout_seconds=0
                            )
                            == original
                        )
                        assert client.agent_workspace_directory_checkpoints(agent.agent_id) == [
                            original
                        ]

                await asyncio.to_thread(read)
                assert await cli(
                    url, agent.agent_id, "get", original.checkpoint_id
                ) == original.model_dump(mode="json")
                assert await cli(
                    url, agent.agent_id, "by-key", "one-directory"
                ) == original.model_dump(mode="json")
        assert provider.calls == calls == 2 and await source_count(runtime) == 1
    finally:
        await service.close()


async def test_new_model_run_queries_historical_directory_after_original_root_closed(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    try:
        created = await agent.execute("创建原目录", idempotency_key="create")
        original = await agent.directory_checkpoint_by_key("one-directory")
        assert original.status == "available" and created.status == "completed"
        provider = OneShotProvider(QUERY, {"kind": "key", "request_key": "one-directory"})
        handle = service._handle(agent.agent_id)
        handle.runtime.provider = provider
        handle.definition.workspace.workspace.close()
        monkeypatch.setattr(
            WorkspaceTreeSnapshotReader,
            "capture",
            lambda *_args, **_kwargs: pytest.fail("历史query不能读取原目录"),
        )
        result = await agent.execute("原目录已关闭，只查询历史", idempotency_key="query")
        assert result.status == "completed" and provider.calls == 2, result
        assert json.loads(provider.reply)["checkpoint"] == original.model_dump(mode="json")
        assert await source_count(runtime) == 1
    finally:
        await service.close()


async def test_current_other_session_cannot_query_original_model_directory(runtime, tmp_path):
    make_tree(tmp_path)
    service, first = await setup(runtime, tmp_path, OneShotProvider())
    try:
        await first.execute("原Scope创建", idempotency_key="scope-first")
        original = await first.directory_checkpoint_by_key("one-directory")
        other = await service.spawn("model-directory")
        provider = OneShotProvider(QUERY, {"kind": "id", "checkpoint_id": original.checkpoint_id})
        service._handle(other.agent_id).runtime.provider = provider
        result = await AgentSession(service, other.agent_id).execute(
            "跨Scope拒绝", idempotency_key="scope-other"
        )
        audits = await runtime.repository.audits(result.run_id)
        assert any(
            audit.code == "workspace_directory_capture_not_found" and not audit.ok
            for audit in audits
        )
        assert await source_count(runtime) == 1
    finally:
        await service.close()


async def test_actual_serve_directory_model_controls_local_provider_and_public_approval(tmp_path):
    files = tmp_path / "files"
    files.mkdir()
    make_tree(files)
    stub = FastAPI()
    model_calls = []

    @stub.post("/v1/chat/completions")
    async def completion(request: Request):
        data = await request.json()
        model_calls.append(data)
        assert set(NAMES) <= {tool["function"]["name"] for tool in data["tools"]}
        if data["messages"][-1]["role"] == "tool":
            original = json.loads(data["messages"][-1]["content"])
            assert original["status"] == "available" and original["snapshot"]["entry_count"] == 6
            delta, reason = {"content": "本机端点已核实目录捕获"}, "stop"
        else:
            delta = {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "serve-directory-original",
                        "type": "function",
                        "function": {
                            "name": CREATE,
                            "arguments": json.dumps(
                                {"root_path": "tree", "request_key": "serve-model-directory"}
                            ),
                        },
                    }
                ]
            }
            reason = "tool_calls"
        frames = [
            {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": reason}]},
        ]
        return StreamingResponse(
            iter(
                [
                    "".join("data: " + json.dumps(frame) + "\n\n" for frame in frames)
                    + "data: [DONE]\n\n"
                ]
            ),
            media_type="text/event-stream",
        )

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
                str(files),
                "--workspace-write",
                "--directory-checkpoints",
                "--directory-checkpoint-controls",
                cwd=str(tmp_path),
                env={
                    **os.environ,
                    "KRIM_AGENT_DATABASE_URL": str(database.engine.url),
                    "KRIM_AGENT_API_KEY": "fictional-directory-serve",
                    "KRIM_AGENT_PROVIDER": "openai-compatible",
                    "KRIM_AGENT_PROVIDER_BASE_URL": provider_url + "/v1",
                    "KRIM_AGENT_PROVIDER_MODEL": "offline-directory",
                    "KRIM_AGENT_PROVIDER_API_KEY": "fictional-local-only",
                    "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
                },
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}",
                timeout=40,
                headers={"Authorization": "Bearer fictional-directory-serve"},
            ) as api:
                async with asyncio.timeout(15):
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
                profile = await api.put(
                    "/v1/agents/profiles/serve-directory",
                    json={
                        "expected_revision": 0,
                        "config": {
                            "name": "serve-directory",
                            "description": "本机离线目录模型",
                            "workspace_key": "local",
                            "tool_names": NAMES,
                        },
                    },
                )
                assert profile.is_success, profile.text
                spawned = await api.post("/v1/agents", json={"name": "serve-directory"})
                assert spawned.status_code == 201, spawned.text
                actor = spawned.json()["agent_id"]
                payload = {"prompt": "创建目录检查点", "idempotency_key": "serve-model-original"}
                running = asyncio.create_task(api.post(f"/v1/agents/{actor}/execute", json=payload))
                async with asyncio.timeout(15):
                    pending = []
                    while not pending:
                        state = (await api.get(f"/v1/agents/{actor}")).json()
                        run_id = state.get("current_run_id")
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
                assert (
                    response.status_code == 200
                    and response.json()["result"]["status"] == "completed"
                ), response.text
                base = f"/v1/agents/{actor}/workspace/directory-checkpoints"
                original = (
                    await api.get(base + "/by-key", params={"request_key": "serve-model-directory"})
                ).json()
                assert (
                    original["status"] == "available" and original["snapshot"]["entry_count"] == 6
                )
                assert len(model_calls) == 2
                assert (await api.get(base + "/" + original["checkpoint_id"])).json() == original
                repeated = await api.post(f"/v1/agents/{actor}/execute", json=payload)
                assert (
                    repeated.status_code == 200 and repeated.json()["result"]["run_id"] == run_id
                ), repeated.text
                assert len(model_calls) == 2
                async with database.sessions() as sql:
                    source = await sql.scalar(select(WorkspaceDirectoryCaptureModelSourceRow))
                    assert source.run_id == run_id and source.call_id == "serve-directory-original"
    finally:
        if running is not None and not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        if process is not None and process.returncode is None:
            process.terminate()
            await asyncio.wait_for(process.communicate(), 15)
        await database.close()
