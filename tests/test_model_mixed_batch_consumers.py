"""模型创建的混合批次由SDK/REST/同步Client/CLI读取同一持久账本。"""

import asyncio
import json
import os
import socket
import sys

import httpx
import uvicorn
from test_model_mixed_batch_execute import setup

from krim_agent.api import create_app
from krim_agent.client import KrimClient


async def cli(url: str, *args: str) -> tuple[int, str, str]:
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "workspace",
        *args,
        "--url",
        url,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    return process.returncode, stdout.decode(), stderr.decode()


async def test_model_batch_is_read_by_sdk_rest_client_cli_and_history(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    app = create_app(runtime=service.runtime)
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        result = await agent.execute("执行后由公开入口读取", idempotency_key="consumer-turn")
        assert result.status == "completed"
        model_batch = await agent.mixed_change_batch_by_key("model")
        assert model_batch is not None and model_batch.status == "done"

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            base = f"/v1/agents/{agent.agent_id}/workspace/mixed-batches"
            detail = await api.get(f"{base}/{model_batch.batch_id}")
            by_key = await api.get(f"{base}/by-key", params={"request_key": "model"})
            history = await api.get(f"{base}/history", params={"limit": 1})
            assert detail.status_code == by_key.status_code == history.status_code == 200
            assert detail.json() == by_key.json() == model_batch.model_dump(mode="json")
            assert history.json()["items"][0]["batch_id"] == model_batch.batch_id
            assert "parent-mixed-call" not in detail.text

        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)

        def read_client():
            with KrimClient(url) as client:
                return (
                    client.agent_workspace_mixed_batch(agent.agent_id, model_batch.batch_id),
                    client.agent_workspace_mixed_batch_history(agent.agent_id, limit=1),
                )

        client_detail, client_history = await asyncio.to_thread(read_client)
        assert client_detail == model_batch
        assert client_history.items[0].batch_id == model_batch.batch_id
        code, output, errors = await cli(
            url, "mixed-batch-by-key", agent.agent_id, "--request-key", "model"
        )
        assert code == 0, errors
        assert json.loads(output) == model_batch.model_dump(mode="json")
        code, output, errors = await cli(url, "mixed-batch-history", agent.agent_id, "--limit", "1")
        assert code == 0, errors
        assert json.loads(output)["items"][0]["batch_id"] == model_batch.batch_id
        assert provider.calls == 2
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 15)
        listener.close()
        await lifespan.__aexit__(None, None, None)
        await service.close()


async def test_public_sdk_cancel_routes_to_model_producer_before_child_wait(runtime, tmp_path):
    service, agent, _provider = await setup(runtime, tmp_path)
    entered = asyncio.Event()
    try:

        async def pause(definition, context):
            if definition.name == "workspace_change":
                entered.set()
                async with context.human_wait():
                    await asyncio.Event().wait()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
        execution = asyncio.create_task(
            agent.execute("公开取消模型批次", idempotency_key="cancel-public")
        )
        await asyncio.wait_for(entered.wait(), 10)
        batch = await agent.mixed_change_batch_by_key("model")
        assert batch is not None and batch.producer_status == "running"
        cancelled = await agent.cancel_mixed_change_batch(batch.batch_id)
        assert cancelled.producer_status == "cancelled"
        assert not service.manager.mixed_batch_writer._tasks
        assert not service.manager.mixed_batch_writer._batches
        assert all(item.operation is None for item in cancelled.items)
        result = await asyncio.wait_for(execution, 10)
        assert result.status in {"aborted", "failed"}
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        await service.close()


async def test_same_call_read_retry_cannot_detach_original_cancel_producer(
    runtime, tmp_path, monkeypatch
):
    service, agent, provider = await setup(runtime, tmp_path)
    entered = asyncio.Event()
    child_closed = asyncio.Event()
    parent_context = None
    execution = None
    writer = service.manager.mixed_batch_writer
    try:

        async def pause_heartbeat(_state, _worker):
            # 固定取消路径的观测窗口，不能用稍后的heartbeat掩盖公开取消断口。
            await asyncio.Event().wait()

        monkeypatch.setattr(writer, "_renew", pause_heartbeat)

        async def pause(definition, context):
            nonlocal parent_context
            if definition.name == "workspace_mixed_start":
                parent_context = context
            if definition.name == "workspace_change":
                entered.set()
                try:
                    async with context.human_wait():
                        await asyncio.Event().wait()
                finally:
                    child_closed.set()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
        execution = asyncio.create_task(agent.execute("原调用重读", idempotency_key="retry-read"))
        await asyncio.wait_for(entered.wait(), 10)
        assert parent_context is not None
        repeated = await writer.tool_start(provider.request.model_dump(), parent_context)
        assert repeated["producer_status"] == "running" and not child_closed.is_set()
        cancelled = await agent.cancel_mixed_change_batch(repeated["batch_id"])
        assert cancelled.producer_status == "cancelled"
        assert child_closed.is_set(), "同键只读重试不能解除原模型任务的公开取消关联"
        assert not writer._tasks
        assert all(item.operation is None for item in cancelled.items)
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
        assert (await asyncio.wait_for(execution, 10)).status in {"failed", "aborted"}
    finally:
        await service.close()
        if execution is not None:
            await asyncio.gather(execution, return_exceptions=True)
