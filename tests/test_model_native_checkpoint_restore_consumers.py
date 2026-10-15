"""模型恢复的TCP/同步Client/CLI与实际serve离线兼容端点消费同一账本。"""

import asyncio
import json
import os
import socket
import sys

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from test_model_native_checkpoint_restores import NAMES, RestoreProvider, setup
from test_tree_move_public_consumers import tcp_for

import krim_agent.api.server as server_module
from krim_agent.client import KrimClient
from krim_agent.persistence import Database
from krim_agent.persistence.database import WorkspaceRestoreModelSourceRow
from krim_agent.workspace_writer import WorkspaceWriter


async def test_model_restore_original_tcp_client_cli_and_sdk_facts_without_replay(
    runtime, tmp_path, monkeypatch
):
    (tmp_path / "plain").write_text("target\n")
    (tmp_path / "binary").write_bytes(b"\x00\xff")
    provider = RestoreProvider(tmp_path, "bytes")
    service, agent = await setup(runtime, tmp_path, provider)
    monkeypatch.setattr(server_module, "AgentService", lambda *_a, **_kw: service)
    app = server_module.create_app(runtime=runtime)
    try:
        result = await agent.execute("模型恢复与跨入口原账本", idempotency_key="tcp")
        plan = await agent.file_checkpoint_restore_by_key("preview")
        assert result.status == "completed" and plan.status == "done", (result, plan)

        def forbidden(*_args, **_kwargs):
            pytest = __import__("pytest")
            pytest.fail("只读消费者不能重放模型、捕获或文件动作")

        monkeypatch.setattr(provider, "stream", forbidden)
        monkeypatch.setattr(WorkspaceWriter, "capture_file", forbidden)
        monkeypatch.setattr(WorkspaceWriter, "apply", forbidden)
        async with tcp_for(app) as url:
            with KrimClient(url) as client:
                assert (
                    await asyncio.to_thread(
                        client.agent_workspace_restore, agent.agent_id, plan.plan_id
                    )
                    == plan
                )
                assert (
                    await asyncio.to_thread(
                        client.agent_workspace_restore_by_key, agent.agent_id, "preview"
                    )
                    == plan
                )
            for command, args in [
                ("restore", [plan.plan_id]),
                ("restore-by-key", ["--request-key", "preview"]),
            ]:
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    "-m",
                    "krim_agent",
                    "workspace",
                    command,
                    agent.agent_id,
                    *args,
                    "--url",
                    url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                output, error = await asyncio.wait_for(process.communicate(), 15)
                assert process.returncode == 0, error.decode()
                assert json.loads(output) == plan.model_dump(mode="json")
        assert provider.calls == 5
    finally:
        await service.close()


async def test_actual_serve_model_restore_switch_public_approval_and_original_sql_sources(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    path = root / "file"
    path.write_bytes(b"\x00\xff")
    stub = FastAPI()
    calls = []
    produced = {}

    @stub.post("/v1/chat/completions")
    async def completion(request: Request):
        payload = await request.json()
        calls.append(payload)
        count = len(calls)
        if count == 1:
            name, identifier, arguments = (
                "workspace_checkpoint_create",
                "serve-model-target",
                {
                    "paths": ["file"],
                    "request_key": "target",
                    "capture_format": "bytes",
                },
            )
        elif count == 2:
            produced["target"] = json.loads(payload["messages"][-1]["content"])
            assert produced["target"]["status"] == "available"
            path.write_bytes(b"before\x00\xff")
            name, identifier, arguments = (
                "workspace_checkpoint_restore_preview",
                "serve-model-preview",
                {
                    "checkpoint_id": produced["target"]["checkpoint_id"],
                    "request_key": "preview",
                },
            )
        elif count == 3:
            produced["preview"] = json.loads(payload["messages"][-1]["content"])
            assert produced["preview"]["phase"] == "ready"
            name, identifier, arguments = (
                "workspace_checkpoint_restore_start",
                "serve-model-start",
                {
                    "plan_id": produced["preview"]["plan_id"],
                    "expected_revision": produced["preview"]["revision"],
                    "request_key": "start",
                },
            )
        elif count == 4:
            produced["done"] = json.loads(payload["messages"][-1]["content"])
            assert produced["done"]["status"] == "done"
            name, identifier, arguments = (
                "workspace_checkpoint_restore_query",
                "serve-model-query",
                {
                    "kind": "id",
                    "plan_id": produced["done"]["plan_id"],
                },
            )
        else:
            assert (
                count == 5
                and json.loads(payload["messages"][-1]["content"])["plan"] == produced["done"]
            )
            name = None
        if name is None:
            delta, finish = {"content": "原模型恢复已核实"}, "stop"
        else:
            delta, finish = (
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": identifier,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": json.dumps(arguments),
                            },
                        }
                    ]
                },
                "tool_calls",
            )
        frames = [
            {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": finish}]},
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
                str(root),
                "--workspace-write",
                "--checkpoint-bytes",
                "--checkpoint-controls",
                "--checkpoint-restore-controls",
                cwd=str(tmp_path),
                env={
                    **os.environ,
                    "KRIM_AGENT_DATABASE_URL": str(database.engine.url),
                    "KRIM_AGENT_API_KEY": "synthetic-restore-serve-key",
                    "KRIM_AGENT_PROVIDER": "openai-compatible",
                    "KRIM_AGENT_PROVIDER_BASE_URL": provider_url + "/v1",
                    "KRIM_AGENT_PROVIDER_MODEL": "offline-synthetic",
                    "KRIM_AGENT_PROVIDER_API_KEY": "synthetic-loopback-only",
                    "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
                },
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}",
                timeout=50,
                headers={"Authorization": "Bearer synthetic-restore-serve-key"},
            ) as api:
                async with asyncio.timeout(15):
                    while True:
                        if process.returncode is not None:
                            _, error = await process.communicate()
                            raise AssertionError(error.decode())
                        try:
                            if (await api.get("/healthz")).is_success:
                                break
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.05)
                profile = await api.put(
                    "/v1/agents/profiles/serve-model-restore",
                    json={
                        "expected_revision": 0,
                        "config": {
                            "name": "serve-model-restore",
                            "description": "原模型恢复本机端点",
                            "workspace_key": "local",
                            "tool_names": NAMES,
                            "tool_timeout_seconds": 30,
                        },
                    },
                )
                assert profile.is_success, profile.text
                response = await api.post("/v1/agents", json={"name": "serve-model-restore"})
                assert response.status_code == 201, response.text
                identifier = response.json()["agent_id"]
                running = asyncio.create_task(
                    api.post(
                        f"/v1/agents/{identifier}/execute",
                        json={
                            "prompt": "模型创建和原恢复",
                            "idempotency_key": "original",
                        },
                    )
                )
                approvals = []
                async with asyncio.timeout(40):
                    while not running.done():
                        state = (await api.get(f"/v1/agents/{identifier}")).json()
                        run_id = state.get("current_run_id")
                        if run_id:
                            pending = (
                                await api.get(f"/v1/runs/{run_id}/interactions?pending_only=true")
                            ).json()
                            for item in pending:
                                approved = await api.post(
                                    f"/v1/runs/{run_id}/interactions/{item['id']}/respond",
                                    json={"action": "approve"},
                                )
                                assert approved.is_success, approved.text
                                approvals.append(item["id"])
                        await asyncio.sleep(0.03)
                executed = await running
                assert executed.is_success, executed.text
                result = executed.json()["result"]
                assert result["status"] == "completed" and approvals
                original = await api.get(
                    f"/v1/agents/{identifier}/workspace/native-restores/{produced['done']['plan_id']}"
                )
                assert original.status_code == 200 and original.json() == produced["done"]
                assert path.read_bytes() == b"\x00\xff" and len(calls) == 5
                async with database.sessions() as sql:
                    sources = list(await sql.scalars(select(WorkspaceRestoreModelSourceRow)))
                assert {(item.phase, item.run_id, item.call_id) for item in sources} == {
                    ("preview", result["run_id"], "serve-model-preview"),
                    ("execute", result["run_id"], "serve-model-start"),
                }
    finally:
        if running is not None and not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        if process is not None:
            if process.returncode is None:
                process.terminate()
            _, error = await asyncio.wait_for(process.communicate(), 10)
            (tmp_path / "serve-stderr.log").write_bytes(error)
        await database.close()
