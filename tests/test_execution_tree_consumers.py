"""原命令整树结果持久消费、真实Model/SDK/HTTP/TCP/CLI与只读零重放。"""

import asyncio
import copy
import json
import sys

import httpx
import pytest
from test_agent_commands import request
from test_execution_tree_quotas import (
    limits,
)
from test_execution_tree_quotas import (
    tree_root as tree_root,
)
from test_tree_move_public_consumers import tcp_for

import krim_agent.api.server as server_module
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.client import ClientError, KrimClient
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.execution.cgroups import CgroupLease
from krim_agent.execution.management import CommandResourcesView
from krim_agent.persistence.command_models import CommandRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession, CommandLookup
from krim_agent.tools import ShortCircuit
from krim_agent.usage import TokenUsage

NAMES = [
    "command_execute",
    "command_get",
    "command_inspect",
    "command_resources",
    "command_history",
    "command_cancel",
]


async def setup(runtime, root, *, provider=None, policy=None, persistent=False, mode="agent"):
    async def allow(*_args):
        return "allow"

    runtime.tool_policy = policy or allow
    if provider is not None:
        runtime.provider = provider
    service = AgentService(runtime, runtime.settings)
    runner = LinuxSandboxRunner(limits=limits(), cgroup_root=root)
    service.register_execution("tree", runner, version="1", owned=True)
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="tree-resource",
                description="原命令进程树资源",
                execution_key="tree",
                tool_names=NAMES,
                persistent=persistent,
                mode=mode,
            ),
        )
    )
    view = await service.spawn("tree-resource")
    return service, AgentSession(service, view.agent_id), runner


async def test_resources_same_sdk_http_tcp_cli_and_closed_root_has_zero_kernel_replay(
    runtime, tmp_path, monkeypatch, tree_root
):
    service, agent, runner = await setup(runtime, tree_root)
    monkeypatch.setattr(server_module, "AgentService", lambda *_args, **_kwargs: service)
    app = server_module.create_app(runtime=runtime)
    try:
        done = await agent.execute_command(request("print('group')"))
        assert done.outcome == "completed" and done.processes_reaped, done
        resource = await agent.command_resources(CommandLookup(command_id=done.command_id))
        assert resource.usage.membership_verified and resource.usage.populated is False
        assert resource.usage.leader_reaped and resource.limits.tree == runner.limits.tree
        tree_root.close()

        def no_kernel(*_args, **_kwargs):
            pytest.fail("冷查询不得访问cgroup或再次启动")

        monkeypatch.setattr(CgroupLease, "observe", no_kernel)
        monkeypatch.setattr(runner, "start", no_kernel)
        assert await agent.command_resources(CommandLookup(request_key="one")) == resource
        async with tcp_for(app) as url:
            with KrimClient(url) as client:
                actual = await asyncio.to_thread(
                    client.agent_command_resources, agent.agent_id, done.command_id
                )
                assert actual == resource
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                "-m",
                "krim_agent",
                "commands",
                "resources",
                agent.agent_id,
                done.command_id,
                "--url",
                url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, error = await asyncio.wait_for(child.communicate(), 30)
            assert child.returncode == 0, error.decode()
            assert json.loads(out) == resource.model_dump(mode="json")
            async with httpx.AsyncClient(base_url=url) as api:
                response = await api.get(
                    f"/v1/agents/{agent.agent_id}/commands/{done.command_id}/resources"
                )
                assert response.status_code == 200 and response.json() == resource.model_dump(
                    mode="json"
                )
        assert (
            "cgroup_root" not in resource.model_dump_json()
            and str(tree_root.path) not in resource.model_dump_json()
        )
    finally:
        await service.close()


async def test_actual_offline_model_executes_and_reads_same_original_tree_resource_fact(
    runtime, tree_root
):
    class Provider:
        name = "offline-process-tree-model"
        calls = 0
        command_id = None
        resource = None

        async def stream(self, messages, tools):
            self.calls += 1
            if self.calls == 1:
                yield ModelToolCall(
                    id="model-tree-execute",
                    name="command_execute",
                    arguments=request("print('model-tree')", key="model-tree").model_dump(),
                )
            elif self.calls == 2:
                self.command_id = json.loads(messages[-1].content)["command_id"]
                yield ModelToolCall(
                    id="model-tree-resource",
                    name="command_resources",
                    arguments={"command_id": self.command_id},
                )
            else:
                self.resource = CommandResourcesView.model_validate_json(messages[-1].content)
                yield ModelText(text="原进程树资源已确认")
                yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=1, total_tokens=6))

    provider = Provider()
    service, agent, _ = await setup(runtime, tree_root, provider=provider)
    try:
        report = await agent.execute("执行合成命令并查询原资源", idempotency_key="model-tree-turn")
        assert report.status == "completed", report
        value = await agent.command_resources(CommandLookup(command_id=provider.command_id))
        assert value == provider.resource and value.run_id == report.run_id
        assert value.call_id == "model-tree-execute" and value.processes_reaped
        assert value.usage.membership_verified and provider.calls == 3
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["argument", "scope", "budget", "short", "post", "marker"])
async def test_resource_query_hooks_cannot_replace_original_scope_budget_or_kernel_facts(
    runtime, tree_root, fault
):
    service, agent, _ = await setup(runtime, tree_root)
    registry = service._handle(agent.agent_id).runtime.tools
    try:
        done = await agent.execute_command(request())
        original = await agent.command_resources(CommandLookup(command_id=done.command_id))

        async def pre(definition, context):
            if definition.name != "command_resources":
                return
            if fault == "argument":
                context.arguments["command_id"] = "another"
            elif fault == "scope":
                context.session_id = "another"
            elif fault == "budget":
                context.timeout = 120
            elif fault == "short":
                return ShortCircuit(original.model_dump(mode="json"))

        async def post(definition, value, context):
            if definition.name == "command_resources" and fault in {"post", "marker"}:
                value = copy.deepcopy(value)
                value["usage"]["cpu_microseconds"] = 0
                if fault == "marker":
                    context._command_response = ("resources", json.dumps(value, sort_keys=True))
            return value

        registry.add_global_pre(pre)
        registry.add_global_post(post)
        with pytest.raises(ServiceError) as rejected:
            await agent.command_resources(CommandLookup(command_id=done.command_id))
        assert rejected.value.code in {"execution_resource_context", "execution_receipt_unverified"}
        assert (
            await service.manager.commands.store.resources(original.scope_id, done.command_id)
        ) == original
    finally:
        await service.close()


async def test_tree_stats_wrong_checksum_or_original_limits_fail_closed_without_reexecution(
    runtime, tree_root
):
    service, agent, _ = await setup(runtime, tree_root)
    try:
        done = await agent.execute_command(request())
        database = runtime.repository.database
        async with database.write_lock, database.sessions.begin() as sql:
            row = await sql.get(CommandRow, done.command_id)
            changed = copy.deepcopy(row.result)
            changed["tree_usage"]["cpu_microseconds"] += 1
            row.result = changed
        with pytest.raises(ServiceError) as rejected:
            await agent.command_resources(CommandLookup(command_id=done.command_id))
        assert rejected.value.code == "execution_record_corrupt"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["scope", "id"])
def test_client_rejects_valid_schema_resource_response_for_another_command_or_scope(fault):
    from pathlib import Path

    old = json.loads(
        (Path(__file__).parent / "fixtures/execution_tree_legacy_vectors.json").read_text()
    )
    view = old["view"]
    value = {
        name: view[name]
        for name in (
            "command_id",
            "scope_id",
            "topic_id",
            "revision",
            "resource",
            "run_id",
            "call_id",
            "phase",
            "outcome",
            "dispatched",
            "started",
            "processes_reaped",
            "error_code",
        )
    }
    value.update(request_key="legacy-command", limits=old["snapshot"]["limits"], usage=None)
    value["scope_id" if fault == "scope" else "command_id"] = "another"
    with KrimClient(
        "http://127.0.0.1",
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=value)),
    ) as client:
        with pytest.raises(ClientError) as rejected:
            client.agent_command_resources("old-agent", "old-command")
        assert rejected.value.code == "execution_resource_response" and rejected.value.status == 502


@pytest.mark.parametrize(
    "mode,decision,confirmed,error",
    [
        ("agent", "deny", True, "permission_denied"),
        ("agent", "ask", False, "approval_required"),
        ("plan", "allow", True, "permission_denied"),
        ("agent", "ask", True, None),
    ],
)
async def test_tree_launch_keeps_original_permission_and_plan_gate_before_any_root_child(
    runtime, tree_root, monkeypatch, mode, decision, confirmed, error
):
    async def policy(definition, _context):
        return decision if definition.name == "command_execute" else "allow"

    service, agent, _ = await setup(runtime, tree_root, policy=policy, mode=mode)
    try:
        if error is not None:

            def forbidden(*_args, **_kwargs):
                pytest.fail("未授权命令不得新建内核叶组")

            monkeypatch.setattr(tree_root, "allocate", forbidden)
            with pytest.raises(ServiceError) as rejected:
                await agent.execute_command(request(), confirmed=confirmed)
            assert rejected.value.code == error
            assert not await service.manager.commands.store.list_commands(
                f"session:{agent.session_id}"
            )
        else:
            done = await agent.execute_command(request(), confirmed=confirmed)
            assert done.outcome == "completed" and done.processes_reaped, done
    finally:
        await service.close()


async def test_tree_registration_does_not_expand_a_previously_frozen_legacy_agent(
    runtime, tree_root
):
    service = AgentService(runtime, runtime.settings)
    legacy = LinuxSandboxRunner()
    service.register_execution("legacy", legacy, version="1", owned=True)
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="legacy-frozen",
                description="保持原冻结能力",
                execution_key="legacy",
                tool_names=["command_get"],
            ),
        )
    )
    agent = AgentSession(service, (await service.spawn("legacy-frozen")).agent_id)
    frozen = service._handle(agent.agent_id).runtime.tools
    before = [item.schema().model_dump(mode="json") for item in frozen.definitions]
    try:
        tree = LinuxSandboxRunner(limits=limits(), cgroup_root=tree_root)
        service.register_execution("tree", tree, version="1", owned=True)
        assert "command_resources" in {item.name for item in runtime.tools.definitions}
        assert [item.schema().model_dump(mode="json") for item in frozen.definitions] == before
        with pytest.raises(ServiceError) as rejected:
            await agent.command_resources(CommandLookup(command_id="foreign"))
        assert rejected.value.code == "tool_not_found"
        assert not list(tree_root.path.glob("krim-exec-*"))
    finally:
        await service.close()


async def test_real_foreign_agent_resources_denied_and_closed_session_history_has_zero_replay(
    runtime, monkeypatch, tree_root
):
    service, agent, runner = await setup(runtime, tree_root)
    monkeypatch.setattr(server_module, "AgentService", lambda *_args, **_kwargs: service)
    app = server_module.create_app(runtime=runtime)
    try:
        done = await agent.execute_command(request("print('closed-history')"))
        assert done.outcome == "completed" and done.processes_reaped
        original = await agent.command_resources(CommandLookup(command_id=done.command_id))
        other = AgentSession(service, (await service.spawn("tree-resource")).agent_id)
        with pytest.raises(ServiceError) as rejected:
            await other.command_resources(CommandLookup(command_id=done.command_id))
        assert rejected.value.code == "execution_not_found" and rejected.value.status == 404
        await agent.close()

        def no_replay(*_args, **_kwargs):
            pytest.fail("关闭会话的历史查询不得观察内核或重启命令")

        monkeypatch.setattr(CgroupLease, "observe", no_replay)
        monkeypatch.setattr(runner, "start", no_replay)
        assert await agent.command_resources(CommandLookup(request_key="one")) == original
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            foreign = await api.get(
                f"/v1/agents/{other.agent_id}/commands/{done.command_id}/resources"
            )
            assert foreign.status_code == 404
            own = await api.get(f"/v1/agents/{agent.agent_id}/commands/{done.command_id}/resources")
            assert own.status_code == 200 and own.json() == original.model_dump(mode="json")
    finally:
        await service.close()
