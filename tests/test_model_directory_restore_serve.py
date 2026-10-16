"""实际serve经本机兼容模型SSE、公开审批、持久重启共享原目录恢复事实。"""

import asyncio
import json
import os
import socket
import sys

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from test_directory_checkpoint_capture import make_tree
from test_model_directory_restores import NAMES, PREVIEW, QUERY, START
from test_tree_move_public_consumers import tcp_for

from krim_agent.persistence import Database
from krim_agent.persistence.database import WorkspaceDirectoryRestoreModelSourceRow


async def test_actual_serve_model_directory_restore_sse_approvals_restart_and_zero_replay(tmp_path):
    files = tmp_path / "files"
    files.mkdir()
    make_tree(files)
    stub = FastAPI()
    model_calls, target = [], []

    @stub.post("/v1/chat/completions")
    async def completion(request: Request):
        data = await request.json()
        model_calls.append(data)
        assert set(NAMES) <= {tool["function"]["name"] for tool in data["tools"]}
        count = len(model_calls)
        if count == 1:
            name, payload = (
                PREVIEW,
                {"target": {"identifier": target[0]}, "request_key": "serve-preview"},
            )
        elif count == 2:
            plan = json.loads(data["messages"][-1]["content"])
            assert plan["phase"] == "ready", plan
            name, payload = (
                START,
                {
                    "plan_id": plan["plan_id"],
                    "expected_revision": plan["revision"],
                    "request_key": "serve-execute",
                },
            )
        elif count == 3:
            plan = json.loads(data["messages"][-1]["content"])
            assert plan["phase"] == "finished", plan
            name, payload = QUERY, {"kind": "operation", "operation_id": plan["operation_id"]}
        else:
            assert count == 4
            result = json.loads(data["messages"][-1]["content"])
            assert result["operation"]["status"] == "done"
        if count < 4:
            delta = {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": f"serve-directory-restore-{count}",
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(payload)},
                    }
                ]
            }
            reason = "tool_calls"
        else:
            delta, reason = {"content": "目录恢复已沿原模型调用完成"}, "stop"
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
            command = [
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
                "--directory-restores",
                "--directory-restore-controls",
            ]
            environment = {
                **os.environ,
                "KRIM_AGENT_DATABASE_URL": str(database.engine.url),
                "KRIM_AGENT_API_KEY": "fictional-directory-model-restore",
                "KRIM_AGENT_PROVIDER": "openai-compatible",
                "KRIM_AGENT_PROVIDER_BASE_URL": provider_url + "/v1",
                "KRIM_AGENT_PROVIDER_MODEL": "offline-directory-restore",
                "KRIM_AGENT_PROVIDER_API_KEY": "fictional-loopback-only",
                "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
            }

            async def launch():
                return await asyncio.create_subprocess_exec(
                    *command,
                    cwd=str(tmp_path),
                    env=environment,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )

            async def ready(api):
                async with asyncio.timeout(15):
                    while True:
                        if process.returncode is not None:
                            _, errors = await process.communicate()
                            raise AssertionError(errors.decode())
                        try:
                            if (await api.get("/healthz")).is_success:
                                return
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.05)

            process = await launch()
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}",
                timeout=40,
                headers={"Authorization": "Bearer fictional-directory-model-restore"},
            ) as api:
                await ready(api)
                profile = await api.put(
                    "/v1/agents/profiles/serve-directory-model-restore",
                    json={
                        "expected_revision": 0,
                        "config": {
                            "name": "serve-directory-model-restore",
                            "description": "本机离线模型目录恢复",
                            "workspace_key": "local",
                            "persistent": True,
                            "tool_names": NAMES,
                        },
                    },
                )
                assert profile.status_code == 200, profile.text
                spawned = await api.post(
                    "/v1/agents", json={"name": "serve-directory-model-restore"}
                )
                assert spawned.status_code == 201, spawned.text
                actor = spawned.json()["agent_id"]
                captures = f"/v1/agents/{actor}/workspace/directory-checkpoints"
                captured = await api.post(
                    captures, json={"root_path": "tree", "request_key": "target"}
                )
                assert captured.status_code == 202, captured.text
                captured = await api.post(
                    captures + f"/{captured.json()['checkpoint_id']}/wait",
                    params={"timeout_seconds": 10},
                )
                assert captured.status_code == 200 and captured.json()["status"] == "available", (
                    captured.text
                )
                target.append(captured.json()["checkpoint_id"])
                (files / "tree/nested/nul").write_bytes(b"current\x80\0")
                payload = {
                    "prompt": "完整预览、批准恢复并核对原操作",
                    "idempotency_key": "serve-model-restore",
                }
                running = asyncio.create_task(api.post(f"/v1/agents/{actor}/execute", json=payload))
                approvals = set()
                async with asyncio.timeout(30):
                    while not running.done():
                        state = (await api.get(f"/v1/agents/{actor}")).json()
                        run_id = state.get("current_run_id")
                        if run_id:
                            pending = (
                                await api.get(
                                    f"/v1/runs/{run_id}/interactions",
                                    params={"pending_only": "true"},
                                )
                            ).json()
                            for interaction in pending:
                                if interaction["id"] not in approvals:
                                    response = await api.post(
                                        f"/v1/runs/{run_id}/interactions/{interaction['id']}/respond",
                                        json={"action": "approve"},
                                    )
                                    assert response.status_code == 200, response.text
                                    approvals.add(interaction["id"])
                        await asyncio.sleep(0.05)
                response = await running
                assert (
                    response.status_code == 200
                    and response.json()["result"]["status"] == "completed"
                ), response.text
                run_id = response.json()["result"]["run_id"]
                assert len(approvals) == 2 and len(model_calls) == 4
                base = f"/v1/agents/{actor}/workspace/directory-restores"
                original = (
                    await api.get(base + "/by-key", params={"request_key": "serve-preview"})
                ).json()
                assert (
                    original["status"] == "done"
                    and (files / "tree/nested/nul").read_bytes() == b"\0\xff"
                )
                inode = (files / "tree").stat().st_ino
                async with database.sessions() as sql:
                    origins = list(
                        await sql.scalars(select(WorkspaceDirectoryRestoreModelSourceRow))
                    )
                    assert {(row.phase, row.run_id, row.call_id) for row in origins} == {
                        ("preview", run_id, "serve-directory-restore-1"),
                        ("execute", run_id, "serve-directory-restore-2"),
                    }
                process.terminate()
                await asyncio.wait_for(process.communicate(), 15)
                process = await launch()
                await ready(api)
                assert (await api.get(base + "/" + original["plan_id"])).json() == original
                assert (
                    await api.get(
                        base + "/operations/by-key", params={"request_key": "serve-execute"}
                    )
                ).json()["operation_id"] == original["operation_id"]
                repeated = await api.post(f"/v1/agents/{actor}/execute", json=payload)
                assert (
                    repeated.status_code == 200 and repeated.json()["result"]["run_id"] == run_id
                ), repeated.text
                assert len(model_calls) == 4 and (files / "tree").stat().st_ino == inode
    finally:
        if running is not None and not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        if process is not None and process.returncode is None:
            process.terminate()
        if process is not None:
            await asyncio.wait_for(process.communicate(), 15)
        await database.close()
