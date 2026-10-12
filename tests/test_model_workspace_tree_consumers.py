"""实际Provider创建的同一树由HTTP/同步Client/CLI冷读与公共取消收束。"""

import asyncio
import json
import os
import socket
import sys
from contextlib import asynccontextmanager

import httpx
import pytest
import uvicorn
from test_model_workspace_tree_tools import counts, setup
from test_model_workspace_tree_tools import pytestmark as pytestmark

from krim_agent.api import create_app
from krim_agent.client import KrimClient


@asynccontextmanager
async def server(service, monkeypatch):
    monkeypatch.setattr("krim_agent.api.server.AgentService", lambda *_args, **_kwargs: service)
    app = create_app(runtime=service.runtime)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    runner = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
    task = asyncio.create_task(runner.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(10):
            while not runner.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        async with httpx.AsyncClient(base_url=url) as api:
            yield url, api
    finally:
        runner.should_exit = True
        await asyncio.wait_for(task, 15)
        listener.close()


async def cli(url, *args):
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")
    }
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
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
    try:
        output, errors = await asyncio.wait_for(child.communicate(), 20)
        assert child.returncode == 0, errors.decode()
        return json.loads(output)
    finally:
        if child.returncode is None:
            child.kill()
            await child.communicate()


async def test_model_tree_sdk_rest_client_cli_and_scope_read_same_receipts(
    runtime, tmp_path, monkeypatch
):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        result = await agent.execute("跨消费端读取原模型树", idempotency_key="tree-consumers")
        assert result.status == "completed" and provider.calls == 4
        plan = provider.plan
        page = await agent.directory_tree_steps(plan.plan_id)
        assert all(item.status == "done" and item.operation is not None for item in page.items)
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 9 and {item.depth for item in audits} == {1, 2}
        assert await counts(runtime.repository.database) == [1, 1, 2, 2, 1, 1]
        async with server(service, monkeypatch) as (url, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/tree-plans"
            for response in (
                await api.get(f"{base}/{plan.plan_id}"),
                await api.get(f"{base}/by-key", params={"request_key": plan.request_key}),
                await api.post(f"{base}/{plan.plan_id}/start", params={"confirmed": "true"}),
                await api.post(f"{base}/{plan.plan_id}/wait"),
            ):
                assert response.is_success and response.json() == plan.model_dump(mode="json")
                assert "model-start-call" not in response.text

            def read_client():
                with KrimClient(url) as client:
                    return (
                        client.agent_workspace_tree(agent.agent_id, plan.plan_id),
                        client.agent_workspace_tree_by_key(agent.agent_id, plan.request_key),
                        client.agent_workspace_tree_steps(agent.agent_id, plan.plan_id),
                    )

            detail, by_key, client_page = await asyncio.to_thread(read_client)
            assert detail == by_key == plan and client_page == page
            assert await cli(url, "tree", agent.agent_id, plan.plan_id) == plan.model_dump(
                mode="json"
            )
            assert await cli(
                url, "tree-by-key", agent.agent_id, "--request-key", plan.request_key
            ) == plan.model_dump(mode="json")
            first = await cli(url, "tree-steps", agent.agent_id, plan.plan_id, "--limit", "1")
            assert first["items"][0] == page.items[0].model_dump(mode="json")
            assert first["next_after"] == 0
            second = await cli(
                url, "tree-steps", agent.agent_id, plan.plan_id, "--after", "0", "--limit", "1"
            )
            assert second["items"][0] == page.items[1].model_dump(mode="json")
            other = (await service.spawn("tree-model")).agent_id
            assert (
                await api.get(f"/v1/agents/{other}/workspace/tree-plans/{plan.plan_id}")
            ).status_code == 404
        assert provider.calls == 4 and await counts(runtime.repository.database) == [
            1,
            1,
            2,
            2,
            1,
            1,
        ]
        assert not service.manager.tree_models._tasks and not service.workspace_trees._tasks
    finally:
        await service.close()


@pytest.mark.parametrize("consumer", ["rest", "client", "cli"])
async def test_public_cancel_settles_original_model_run_and_waiting_child(
    runtime, tmp_path, monkeypatch, consumer
):
    service, agent, provider = await setup(runtime, tmp_path)
    entered, child_closed = asyncio.Event(), asyncio.Event()
    executing = None
    try:

        async def pause(definition, child):
            if definition.name == "directory_change" and child.tree_fence is not None:
                entered.set()
                try:
                    async with child.human_wait():
                        await asyncio.Event().wait()
                finally:
                    child_closed.set()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
        async with server(service, monkeypatch) as (url, api):
            executing = asyncio.create_task(
                agent.execute("公开收束原模型树", idempotency_key="cancel-" + consumer)
            )
            await asyncio.wait_for(entered.wait(), 10)
            plan = await agent.directory_tree_by_key(provider.request.request_key)
            assert plan.producer_status == "running" and provider.calls == 2
            if consumer == "rest":
                response = await api.post(
                    f"/v1/agents/{agent.agent_id}/workspace/tree-plans/{plan.plan_id}/cancel"
                )
                assert response.status_code == 200
                value = response.json()
            elif consumer == "client":

                def cancel():
                    with KrimClient(url) as client:
                        return client.agent_workspace_cancel_tree(agent.agent_id, plan.plan_id)

                value = (await asyncio.to_thread(cancel)).model_dump(mode="json")
            else:
                value = await cli(url, "cancel-tree", agent.agent_id, plan.plan_id)
            assert value["producer_status"] == "cancelled" and child_closed.is_set()
            result = await asyncio.wait_for(executing, 10)
            assert result.status in {"failed", "aborted"}
            assert not service.manager.tree_models._tasks and not service.manager.tree_models._plans
            assert (await agent.directory_tree(plan.plan_id)).model_dump(mode="json") == value
            assert await counts(runtime.repository.database) == [1, 1, 1, 0, 0, 0]
            assert list(tmp_path.iterdir()) == [] and provider.calls == 2
    finally:
        if executing is not None and not executing.done():
            executing.cancel()
            await asyncio.gather(executing, return_exceptions=True)
        await service.close()
