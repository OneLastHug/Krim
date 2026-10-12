"""公共树准备沿原Registry；SDK/REST/TCP/CLI消费同一持久原事实。"""

import asyncio
import json
import shutil
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import uvicorn
from sqlalchemy import func, select
from test_command_directory_dependency_source import counts
from test_command_publication import setup
from test_mixed_workspace_batch_public import cli

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.directory_publication import CommandDirectoryChangesRequest
from krim_agent.persistence.database import WorkspaceTreePlanRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentCommandRequest, AgentSession, TreePlanRequest
from krim_agent.tools import ShortCircuit, ToolContext
from krim_agent.workspace_directory import WorkspaceDirectoryWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux目录来源与原宿主发布",
)

NAMES = [
    "workspace_tree_prepare",
    "command_execute",
    "command_get",
    "directory_source_query",
    "command_propose",
    "directory_source_preview",
    "directory_change",
    "workspace_change",
    "workspace_reconcile",
    "directory_reconcile",
]
CODE = (
    "import os;os.makedirs('tree/sub');os.chmod('tree',0o750);"
    "os.chmod('tree/sub',0o700);open('tree/sub/data.bin','wb').write(bytes([0,255]))"
)


async def source(service, agent, *, failed=False):
    command = await agent.execute_command(
        AgentCommandRequest(
            request_key="command",
            command=CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    CODE + (";raise SystemExit(3)" if failed else ""),
                ]
            ),
        ),
        confirmed=True,
    )
    catalog = await agent.command_directory_changes(
        CommandDirectoryChangesRequest(command_id=command.command_id)
    )
    request = TreePlanRequest(
        command_id=command.command_id,
        source_hash=catalog.source_hash,
        root_path="tree",
        request_key="树/../?confirmed=false#%",
    )
    return request


async def prepared_count(service):
    async with service.workspace_tree_store.database.sessions() as sql:
        return await sql.scalar(select(func.count()).select_from(WorkspaceTreePlanRow))


async def test_sdk_prepare_cold_key_conflict_cancel_and_no_original_attempts(
    runtime, tmp_path, monkeypatch
):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        request = await source(service, agent)
        plan = await agent.prepare_directory_tree(request)
        assert plan.status == "planned" and plan.step_count == 3 and plan.done_count == 0
        assert list(tmp_path.iterdir()) == [] and await counts(
            service.workspace_tree_store.database
        ) == [0, 0, 0, 0]

        def forbidden(*_args, **_kwargs):
            raise AssertionError("原key冷读不能再次准备根")

        monkeypatch.setattr(WorkspaceDirectoryWriter, "prepare_artifact", forbidden)
        assert await agent.prepare_directory_tree(request) == plan
        assert await agent.directory_tree_by_key(request.request_key) == plan
        assert (await agent.directory_tree_steps(plan.plan_id, limit=1)).next_after == 0
        with pytest.raises(ServiceError) as conflict:
            await agent.prepare_directory_tree(request.model_copy(update={"accept_failed": True}))
        assert (
            conflict.value.code == "workspace_tree_idempotency_conflict"
            and conflict.value.status == 409
        )
        cancelled = await agent.cancel_directory_tree(plan.plan_id)
        assert await agent.prepare_directory_tree(request) == cancelled
        assert await agent.start_directory_tree(plan.plan_id, confirmed=True) == cancelled
        assert await prepared_count(service) == 1 and await counts(
            service.workspace_tree_store.database
        ) == [0, 0, 0, 0]
    finally:
        await service.close()


async def test_sdk_prepare_then_original_worker_publishes_same_tree_once(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        request = await source(service, agent)
        plan = await agent.prepare_directory_tree(request)
        await agent.start_directory_tree(plan.plan_id, confirmed=True)
        done = await agent.wait_directory_tree(plan.plan_id, timeout_seconds=30)
        assert done.status == "done" and done.done_count == 3
        assert await agent.prepare_directory_tree(request) == done
        assert await agent.start_directory_tree(plan.plan_id, confirmed=True) == done
        assert await counts(service.workspace_tree_store.database) == [2, 2, 1, 1]
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert all(
            item.operation is not None and item.status == "done"
            for item in (await agent.directory_tree_steps(plan.plan_id)).items
        )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "field", ["request_key", "root_path", "session_id", "origin", "workspace_trees"]
)
async def test_prepare_hook_cannot_redirect_original_request_or_context(runtime, tmp_path, field):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        request = await source(service, agent)

        async def redirect(definition, context):
            if definition.name != "workspace_tree_prepare":
                return None
            if field in {"request_key", "root_path"}:
                context.arguments[field] = "foreign"
            else:
                setattr(
                    context,
                    field,
                    {"session_id": "foreign", "origin": "model", "workspace_trees": None}[field],
                )

        service._handle(agent.agent_id).runtime.tools.add_global_pre(redirect)
        with pytest.raises(ServiceError) as denied:
            await agent.prepare_directory_tree(request)
        assert denied.value.code == "workspace_tree_fence_mismatch" and denied.value.status == 403
        assert await prepared_count(service) == 0 and await counts(
            service.workspace_tree_store.database
        ) == [0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_prepare_short_circuit_or_post_hook_cannot_forge_persistent_result(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        request = await source(service, agent)
        first = await agent.prepare_directory_tree(request)
        registry = service._handle(agent.agent_id).runtime.tools

        async def shortcut(definition, _context):
            if definition.name == "workspace_tree_prepare":
                return ShortCircuit(first.model_dump(mode="json"))
            return None

        registry.add_global_pre(shortcut)
        with pytest.raises(ServiceError) as denied:
            await agent.prepare_directory_tree(request.model_copy(update={"request_key": "other"}))
        assert denied.value.code == "workspace_tree_prepare_unverified"
        assert await prepared_count(service) == 1
        registry._global_pre.clear()

        async def forge(definition, result, _context):
            if definition.name == "workspace_tree_prepare":
                return {**result, "status": "done", "done_count": 3}
            return result

        registry.add_global_post(forge)
        with pytest.raises(ServiceError) as denied:
            await agent.prepare_directory_tree(request.model_copy(update={"request_key": "other"}))
        assert denied.value.code == "workspace_tree_prepare_unverified"
        cold = await agent.directory_tree_by_key("other")
        assert cold.status == "planned" and cold.done_count == 0
        assert await prepared_count(service) == 2 and list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_prepare_model_source_bad_hash_and_failed_acceptance_are_closed(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        request = await source(service, agent, failed=True)
        with pytest.raises(ServiceError) as failed:
            await agent.prepare_directory_tree(request)
        assert failed.value.code == "execution_failed_artifacts"
        with pytest.raises(ServiceError) as bad_hash:
            await agent.prepare_directory_tree(request.model_copy(update={"source_hash": "0" * 64}))
        assert bad_hash.value.code == "workspace_tree_source_conflict"
        registry = service._handle(agent.agent_id).runtime.tools
        value = await registry.invoke(
            "workspace_tree_prepare",
            request.model_dump(),
            ToolContext(registry, origin="model", run_id="model-run"),
        )
        assert value.is_error and value.code == "workspace_tree_model_unbound"
        assert await prepared_count(service) == 0
        plan = await agent.prepare_directory_tree(
            request.model_copy(update={"accept_failed": True})
        )
        assert plan.status == "planned" and await counts(service.workspace_tree_store.database) == [
            0,
            0,
            0,
            0,
        ]
    finally:
        await service.close()


@asynccontextmanager
async def tree_api(runtime, root):
    async def policy(definition, _context):
        return "allow" if definition.risk == "read" else "ask"

    runtime.tool_policy = policy
    app = create_app(
        runtime=runtime,
        workspace_root=root,
        workspace_writable=True,
        execution_runner=LinuxSandboxRunner(),
        execution_owned=True,
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        service = app.state.agents
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="tree-public",
                    description="公共目录树",
                    workspace_key="local",
                    execution_key="local",
                    tool_names=NAMES,
                    tool_timeout_seconds=30,
                ),
            )
        )
        agent = AgentSession(service, (await service.spawn("tree-public")).agent_id)
        request = await source(service, agent)
        yield app, api, agent, request


async def test_rest_prepare_start_original_scope_pages_and_no_public_credentials(runtime, tmp_path):
    async with tree_api(runtime, tmp_path) as (app, api, agent, request):
        base = f"/v1/agents/{agent.agent_id}/workspace/tree-plans"
        prepared = await api.post(base, json=request.model_dump(mode="json"))
        assert prepared.status_code == 201, prepared.text
        value = prepared.json()
        assert (
            value["status"] == "planned"
            and value["done_count"] == 0
            and list(tmp_path.iterdir()) == []
        )
        plan_id = value["plan_id"]
        assert (
            await api.get(f"{base}/by-key", params={"request_key": request.request_key})
        ).json() == value
        first = (await api.get(f"{base}/{plan_id}/steps", params={"limit": 1})).json()
        assert first["next_after"] == 0 and first["items"][0]["path"] == "tree"
        assert (
            len((await api.get(f"{base}/{plan_id}/steps", params={"after": 0})).json()["items"])
            == 2
        )
        started = await api.post(f"{base}/{plan_id}/start", params={"confirmed": "true"})
        assert started.status_code == 202, started.text
        done = await api.post(f"{base}/{plan_id}/wait")
        assert done.status_code == 200 and done.json()["status"] == "done", done.text
        assert (await api.post(base, json=request.model_dump(mode="json"))).json() == done.json()
        assert (
            await api.post(f"{base}/{plan_id}/start", params={"confirmed": "true"})
        ).json() == done.json()
        assert await counts(app.state.agents.workspace_tree_store.database) == [2, 2, 1, 1]
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        other = (await app.state.agents.spawn("tree-public")).agent_id
        assert (
            await api.get(f"/v1/agents/{other}/workspace/tree-plans/{plan_id}")
        ).status_code == 404
        assert (
            await api.get(
                f"/v1/agents/{other}/workspace/tree-plans/by-key",
                params={"request_key": request.request_key},
            )
        ).json() is None
        schemas = app.openapi()["components"]["schemas"]
        assert "TreePlanFence" not in schemas
        assert not {
            "owner_id",
            "token",
            "lease_expires_at",
            "frozen",
            "root_prepared",
            "manifest",
        }.intersection(schemas["TreePlanRequest"]["properties"])


async def test_rest_internal_extra_and_duplicate_parameters_refuse_before_freeze(runtime, tmp_path):
    async with tree_api(runtime, tmp_path) as (app, api, agent, request):
        base = f"/v1/agents/{agent.agent_id}/workspace/tree-plans"
        for extra in (
            "token",
            "owner_id",
            "lease_expires_at",
            "root",
            "mode",
            "manifest",
            "fence",
            "run_id",
        ):
            assert (
                await api.post(base, json={**request.model_dump(mode="json"), extra: "forged"})
            ).status_code == 422
        for query in ([("confirmed", "true"), ("confirmed", "false")], {"token": "forged"}):
            assert (
                await api.post(base, params=query, json=request.model_dump(mode="json"))
            ).status_code == 422
        for query in (
            [("request_key", "one"), ("request_key", "two")],
            {"request_key": "ok", "owner_id": "evil"},
        ):
            assert (await api.get(f"{base}/by-key", params=query)).status_code == 422
        for seconds in ("nan", "inf", "-1", "301"):
            assert (
                await api.post(f"{base}/missing/wait", params={"timeout_seconds": seconds})
            ).status_code == 422
        assert (
            await api.get(f"{base}/missing/steps", params=[("limit", 1), ("limit", 2)])
        ).status_code == 422
        assert (
            await api.post(f"{base}/missing/cancel", params={"fence": "forged"})
        ).status_code == 422
        assert await prepared_count(app.state.agents) == 0 and await counts(
            app.state.agents.workspace_tree_store.database
        ) == [0, 0, 0, 0]


async def test_tcp_client_and_cli_prepare_start_cold_read_same_key(runtime, tmp_path):
    async with tree_api(runtime, tmp_path) as (app, _api, agent, request):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        path = (
            Path(".cache/project-completion/p04-workspace-tree-public") / f"{agent.agent_id}.json"
        )
        path.write_text(request.model_dump_json())
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
            code, output, errors = await cli(url, "prepare-tree", agent.agent_id, "--request", path)
            assert code == 0 and json.loads(output)["status"] == "planned", errors
            plan_id = json.loads(output)["plan_id"]
            code, output, errors = await cli(
                url, "start-tree", agent.agent_id, plan_id, "--confirm"
            )
            assert code == 0, errors
            code, output, errors = await cli(url, "wait-tree", agent.agent_id, plan_id)
            assert code == 0 and json.loads(output)["status"] == "done", errors

            def read():
                with KrimClient(url) as client:
                    done = client.agent_workspace_tree(agent.agent_id, plan_id)
                    assert (
                        client.agent_workspace_tree_by_key(agent.agent_id, request.request_key)
                        == done
                    )
                    assert client.agent_workspace_prepare_tree(agent.agent_id, request) == done
                    assert (
                        client.agent_workspace_start_tree(agent.agent_id, plan_id, confirmed=True)
                        == done
                    )
                    assert client.agent_workspace_cancel_tree(agent.agent_id, plan_id) == done
                    assert (
                        len(client.agent_workspace_tree_steps(agent.agent_id, plan_id).items) == 3
                    )
                    return done

            done = await asyncio.to_thread(read)
            code, output, errors = await cli(
                url, "tree-by-key", agent.agent_id, "--request-key", request.request_key
            )
            assert code == 0 and json.loads(output)["plan_id"] == done.plan_id, errors
            code, output, errors = await cli(
                url, "tree-steps", agent.agent_id, plan_id, "--limit", 1
            )
            assert code == 0 and json.loads(output)["next_after"] == 0, errors
            assert await counts(app.state.agents.workspace_tree_store.database) == [2, 2, 1, 1]
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 15)
            listener.close()


async def test_public_cancel_running_tree_closes_original_admission_and_retries_only_read(
    runtime, tmp_path
):
    async with tree_api(runtime, tmp_path) as (app, api, agent, request):
        entered = asyncio.Event()

        async def pause(definition, _context):
            if definition.name == "directory_change":
                entered.set()
                await asyncio.Event().wait()

        app.state.agents._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
        base = f"/v1/agents/{agent.agent_id}/workspace/tree-plans"
        plan = (await api.post(base, json=request.model_dump(mode="json"))).json()
        identifier = plan["plan_id"]
        assert (
            await api.post(f"{base}/{identifier}/start", params={"confirmed": "true"})
        ).status_code == 202
        await asyncio.wait_for(entered.wait(), 10)
        cancelled = await asyncio.wait_for(api.post(f"{base}/{identifier}/cancel"), 15)
        assert cancelled.status_code == 200 and cancelled.json()["producer_status"] == "cancelled"
        assert (
            await api.post(f"{base}/{identifier}/start", params={"confirmed": "true"})
        ).json() == cancelled.json()
        assert (
            await api.post(base, json=request.model_dump(mode="json"))
        ).json() == cancelled.json()
        assert all(
            item["status"] == "skipped" and item["operation"] is None
            for item in (await api.get(f"{base}/{identifier}/steps")).json()["items"]
        )
        assert await counts(app.state.agents.workspace_tree_store.database) == [1, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []


async def test_prepare_policy_or_disabled_execution_never_freezes(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        request = await source(service, agent)

        async def deny_prepare(definition, _context):
            return "deny" if definition.name == "workspace_tree_prepare" else "allow"

        handle = service._handle(agent.agent_id)
        original = handle.runtime.tool_policy
        handle.runtime.tool_policy = deny_prepare
        with pytest.raises(ServiceError) as denied:
            await agent.prepare_directory_tree(request)
        assert denied.value.code == "permission_denied"
        handle.runtime.tool_policy = original
        service.disable_execution("sandbox")
        with pytest.raises(ServiceError):
            await agent.prepare_directory_tree(request)
        assert await prepared_count(service) == 0 and await counts(
            service.workspace_tree_store.database
        ) == [0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


def test_client_rejects_invalid_wait_or_page_without_http():
    calls = []
    transport = httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(500))
    with KrimClient("http://127.0.0.1", transport=transport) as client:
        for seconds in (float("nan"), float("inf"), -1, 301, True):
            with pytest.raises(ClientError) as denied:
                client.agent_workspace_wait_tree("agent", "plan", timeout_seconds=seconds)
            assert denied.value.code == "workspace_tree_timeout"
        for options in ({"limit": 0}, {"limit": True}, {"after": -1}, {"after": True}):
            with pytest.raises(ClientError) as denied:
                client.agent_workspace_tree_steps("agent", "plan", **options)
            assert denied.value.code == "workspace_tree_page"
        assert not calls
