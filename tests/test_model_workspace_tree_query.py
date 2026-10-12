"""原模型Run/call/预算只读目录树，原参数/回程拒绝与零新副作用。"""

import asyncio
import json
import shutil
import sys

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from test_workspace_tree_public import NAMES, source

from krim_agent.agents import AgentError, AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.application.agents import AgentService
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.context_engine import ContextRunConfig
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence.command_models import CommandRow
from krim_agent.persistence.database import (
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceTreePlanRow,
)
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession, TreePlanQuery, TreePlanQueryResult
from krim_agent.tools import ShortCircuit, ToolContext, ToolError, ToolRegistry
from krim_agent.tools.workspace_trees import tree_query_tool
from krim_agent.usage import TokenUsage

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="目录树模型实测需要真实Linux隔离来源",
)


async def setup(runtime, root, *, enabled=True, usage_budget=None):
    async def allow(_definition, _context):
        return "allow"

    runtime.tool_policy = allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, version="v1", writable=True)
    service.register_execution("sandbox", LinuxSandboxRunner(), version="v1", owned=True)
    if enabled:
        service.register_tree_queries()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="tree-reader",
                description="原目录树模型消费",
                workspace_key="files",
                execution_key="sandbox",
                tool_names=[*NAMES, *(["workspace_tree_query"] if enabled else [])],
                tool_timeout_seconds=30,
                context_config=ContextRunConfig(
                    tokenizer="cl100k_base", reserved_output_tokens=128
                ),
                usage_budget=usage_budget,
            ),
        )
    )
    agent = AgentSession(service, (await service.spawn("tree-reader")).agent_id)
    request = await source(service, agent)
    plan = await agent.prepare_directory_tree(request)
    return service, agent, plan


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                CommandRow,
                WorkspaceTreePlanRow,
                WorkspaceDirectoryProposalRow,
                WorkspaceDirectoryAttemptRow,
                WorkspaceEditProposalRow,
                WorkspaceEditAttemptRow,
            )
        ]


class Reader:
    name = "offline-original-tree-reader"
    calls = 0
    plan = None

    async def stream(self, messages, tools):
        self.calls += 1
        assert "workspace_tree_query" in {item.name for item in tools}
        if self.calls == 1:
            query = {"kind": "plan", "plan_id": self.plan.plan_id}
        else:
            value = TreePlanQueryResult.model_validate_json(messages[-1].content)
            if self.calls == 2:
                assert value.plan == self.plan
                query = {"kind": "by_key", "request_key": self.plan.request_key}
            elif self.calls == 3:
                assert value.plan == self.plan
                query = {"kind": "steps", "plan_id": self.plan.plan_id, "limit": 1}
            elif self.calls == 4:
                assert value.steps.items[0].position == 0 and value.steps.next_after == 0
                query = {
                    "kind": "steps",
                    "plan_id": self.plan.plan_id,
                    "after": value.steps.next_after,
                    "limit": 1,
                }
            elif self.calls == 5:
                assert value.steps.items[0].position == 1 and value.steps.next_after == 1
                query = {"kind": "by_key", "request_key": "missing-original"}
            else:
                assert value.kind == "by_key" and value.plan is value.steps is None
                yield ModelText(text="已读取原目录树与步骤")
                yield ModelUsage(
                    usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11)
                )
                return
        yield ModelToolCall(
            id=f"tree-query-{self.calls}", name="workspace_tree_query", arguments=query
        )
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def test_real_model_loop_same_sdk_rest_tree_steps_run_calls_budget_zero_replay(
    runtime, tmp_path, monkeypatch
):
    provider = Reader()
    runtime.provider = provider
    service, agent, plan = await setup(
        runtime, tmp_path, usage_budget=UsageBudgetConfig(max_total_tokens=100_000)
    )
    try:
        await agent.start_directory_tree(plan.plan_id, confirmed=True)
        provider.plan = await agent.wait_directory_tree(plan.plan_id)
        assert provider.plan.status == "done"
        before = await counts(runtime.repository.database)
        result = await agent.execute("读取原目录树", idempotency_key="model-query")
        assert result.status == "completed" and result.text == "已读取原目录树与步骤", result
        assert result.token_usage.total_tokens == 66 and provider.calls == 6
        assert await counts(runtime.repository.database) == before == [1, 1, 2, 2, 1, 1]
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert await agent.directory_tree(plan.plan_id) == provider.plan
        messages = await runtime.repository.run_messages(result.run_id)
        tool_messages = [message for message in messages if message.role == "tool"]
        assert [message.details["call_id"] for message in tool_messages] == [
            f"tree-query-{index}" for index in range(1, 6)
        ]
        assert all(not message.details["is_error"] for message in tool_messages)
        accounts = await service._handle(agent.agent_id).runtime.budget_store.accounts()
        assert (
            len(accounts) == 1 and accounts[0].spent_tokens == 66 and accounts[0].held_tokens == 0
        )
        original_page = await agent.directory_tree_steps(plan.plan_id, limit=1)
        # ASGI装配使用同一真实Service/owner，不向产品工厂增加测试专用参数。
        monkeypatch.setattr("krim_agent.api.server.AgentService", lambda *_args, **_kwargs: service)
        app = create_app(runtime=runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/tree-plans/{plan.plan_id}/steps?limit=1"
            )
        assert response.status_code == 200, response.text
        assert response.json() == original_page.model_dump(mode="json")
        assert (
            TreePlanQueryResult.model_validate_json(tool_messages[2].content).steps == original_page
        )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "plan"},
        {"kind": "plan", "plan_id": "id", "request_key": "key"},
        {"kind": "plan", "plan_id": "id", "after": 0},
        {"kind": "plan", "plan_id": "id", "limit": 1},
        {"kind": "by_key"},
        {"kind": "by_key", "request_key": "x\x7f"},
        {"kind": "steps"},
        {"kind": "steps", "plan_id": "id", "request_key": "key"},
        {"kind": "steps", "plan_id": "id", "after": True},
        {"kind": "steps", "plan_id": "id", "after": 16384},
        {"kind": "steps", "plan_id": "id", "limit": 33},
        {"kind": "steps", "plan_id": "id", "limit": True},
        {"kind": "plan", "plan_id": "id", "scope_id": "session:other"},
        {"kind": "plan", "plan_id": "id", "run_id": "local"},
        {"kind": "plan", "plan_id": "id", "token": "forged"},
        {"kind": "plan", "plan_id": "id", "root_path": "/data"},
        {"kind": "start", "plan_id": "id"},
    ],
)
def test_strict_selectors_private_fields_and_fake_write_queries_are_rejected(payload):
    with pytest.raises(ValidationError):
        TreePlanQuery.model_validate(payload)


async def test_hook_cannot_redirect_any_original_query_identity_and_audit(runtime, tmp_path):
    service, agent, plan = await setup(runtime, tmp_path)
    try:
        registry = service._handle(agent.agent_id).runtime.tools
        selected = [None]

        async def pre(definition, context):
            if definition.name == "workspace_tree_query":
                field = selected[0]
                if field == "arguments":
                    context.arguments["request_key"] = "missing"
                else:
                    value = {
                        "run_id": "missing",
                        "topic_id": "other",
                        "session_id": "other",
                        "agent_id": "other",
                        "origin": "model",
                        "call_id": "foreign-call",
                        "source_call_id": "foreign-call",
                        "depth": 7,
                        "timeout": 120,
                        "budget_account_ids": ("foreign",),
                        "execution_timeouts": (),
                        "workspace": None,
                        "execution": None,
                        "workspace_tree_queries": object(),
                        "registry": ToolRegistry(),
                    }[field]
                    setattr(context, field, value)

        registry.add_global_pre(pre)
        before = await counts(runtime.repository.database)
        for field in (
            "arguments",
            "run_id",
            "topic_id",
            "session_id",
            "agent_id",
            "origin",
            "call_id",
            "source_call_id",
            "depth",
            "timeout",
            "budget_account_ids",
            "execution_timeouts",
            "workspace",
            "execution",
            "workspace_tree_queries",
            "registry",
        ):
            selected[0] = field
            with pytest.raises(ServiceError) as redirected:
                await service._workspace_call(
                    agent.agent_id,
                    "workspace_tree_query",
                    {"kind": "by_key", "request_key": plan.request_key},
                )
            assert redirected.value.code == "workspace_tree_query_mismatch", field
            audit = registry.audit[-1]
            assert audit.run_id == "local" and audit.call_id != "foreign-call" and audit.depth == 1
        assert await counts(runtime.repository.database) == before == [1, 1, 0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_hook_short_post_result_context_and_private_marker_cannot_forge(runtime, tmp_path):
    service, agent, plan = await setup(runtime, tmp_path)
    try:
        registry = service._handle(agent.agent_id).runtime.tools
        selected = [None]

        async def pre(definition, context):
            if definition.name == "workspace_tree_query" and selected[0] in {
                "short",
                "short-marker",
            }:
                value = TreePlanQueryResult(kind="by_key").model_dump(mode="json")
                if selected[0] == "short-marker":
                    context._tree_query_response = json.dumps(value, sort_keys=True)
                return ShortCircuit(value)
            return None

        async def post(definition, value, context):
            if definition.name == "workspace_tree_query":
                if selected[0] in {"post-value", "post-marker"}:
                    value["plan"]["request_key"] = "forged"
                    if selected[0] == "post-marker":
                        context._tree_query_response = json.dumps(value, sort_keys=True)
                elif selected[0] == "post-context":
                    context.origin = "model"
            return value

        registry.add_global_pre(pre)
        registry.add_global_post(post)
        for fault in ("short", "short-marker", "post-value", "post-marker", "post-context"):
            selected[0] = fault
            with pytest.raises(ServiceError) as forged:
                await service._workspace_call(
                    agent.agent_id,
                    "workspace_tree_query",
                    {"kind": "by_key", "request_key": plan.request_key},
                )
            assert forged.value.code == (
                "workspace_tree_query_mismatch"
                if fault == "post-context"
                else "workspace_tree_query_unverified"
            )
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_explicit_enable_frozen_profiles_cross_scope_and_parent_intersection(
    runtime, tmp_path
):
    service, old, plan = await setup(runtime, tmp_path, enabled=False)
    try:
        handle = service._handle(old.agent_id)
        before = {item.name for item in handle.runtime.tools.definitions}
        service.register_tree_queries()
        service.register_tree_queries()
        assert {item.name for item in handle.runtime.tools.definitions} == before
        with pytest.raises(ServiceError) as absent:
            await service._workspace_call(
                old.agent_id, "workspace_tree_query", {"kind": "plan", "plan_id": plan.plan_id}
            )
        assert absent.value.code == "tool_not_found"
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="readonly-reader",
                    description="只读消费",
                    workspace_key="files",
                    tool_names=["workspace_tree_query"],
                    mode="plan",
                ),
            )
        )
        current = AgentSession(service, (await service.spawn("readonly-reader")).agent_id)
        with pytest.raises(ServiceError) as foreign:
            await service._workspace_call(
                current.agent_id, "workspace_tree_query", {"kind": "plan", "plan_id": plan.plan_id}
            )
        assert foreign.value.code == "workspace_tree_not_found"
        child_tools = ToolRegistry()
        child_tools.register(tree_query_tool())
        with pytest.raises(AgentError) as expansion:
            service.manager._restrict_workspace(
                handle.definition.workspace,
                handle.definition.workspace,
                child_tools,
                handle.runtime.tools,
            )
        assert expansion.value.code == "agent_workspace_forbidden"
        assert (
            await service._workspace_call(
                current.agent_id,
                "workspace_tree_query",
                {"kind": "by_key", "request_key": plan.request_key},
            )
        )["plan"] is None
    finally:
        await service.close()


async def test_backend_missing_wrong_or_closed_run_disabled_workspace_and_policy(runtime, tmp_path):
    definition = tree_query_tool()
    with pytest.raises(ToolError) as missing:
        await definition.executor(
            TreePlanQuery(kind="by_key", request_key="missing"),
            ToolContext(runtime.tools, origin="host"),
        )
    assert missing.value.code == "workspace_tree_backend_missing"
    service, agent, plan = await setup(runtime, tmp_path)
    try:
        handle = service._handle(agent.agent_id)
        run, _ = await runtime.repository.begin_run(
            handle.topic_id,
            ChatRequest(text="run", idempotency_key="guard"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=service.manager.owner_id,
        )
        await runtime.repository.finish(run.id, "completed", None, [])
        for identifier, code in (
            ("local", "workspace_run_mismatch"),
            ("missing", "workspace_run_mismatch"),
            (run.id, "workspace_run_closed"),
        ):
            context = ToolContext(
                handle.runtime.tools,
                run_id=identifier,
                topic_id=handle.topic_id,
                session_id=agent.session_id,
                agent_id=agent.agent_id,
                workspace=handle.definition.workspace,
                workspace_tree_queries=service.manager.tree_queries,
                origin="model",
            )
            with pytest.raises(ToolError) as denied:
                await service.manager.tree_queries.tool_query(
                    {"kind": "plan", "plan_id": plan.plan_id}, context
                )
            assert denied.value.code == code

        async def deny(_definition, _context):
            return "deny"

        handle.runtime.tool_policy = deny
        with pytest.raises(ServiceError) as forbidden:
            await service._workspace_call(
                agent.agent_id, "workspace_tree_query", {"kind": "plan", "plan_id": plan.plan_id}
            )
        assert forbidden.value.code == "permission_denied"
        handle.runtime.tool_policy = None
        service.disable_workspace("files")
        with pytest.raises(ServiceError) as revoked:
            await service._workspace_call(
                agent.agent_id, "workspace_tree_query", {"kind": "plan", "plan_id": plan.plan_id}
            )
        assert revoked.value.code == "workspace_unavailable"
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_model_budget_rejection_never_calls_provider_or_reads_tree(runtime, tmp_path):
    provider = Reader()
    runtime.provider = provider
    service, agent, plan = await setup(
        runtime, tmp_path, usage_budget=UsageBudgetConfig(max_total_tokens=1)
    )
    try:
        provider.plan = plan
        result = await agent.execute("读原目录树", idempotency_key="budget-reject")
        assert result.code == "budget_tokens_exceeded" and provider.calls == 0
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_actual_plan_model_readonly_workspace_and_ask_denial(runtime, tmp_path):
    class MissingReader:
        name = "offline-plan-readonly-tree"
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            assert "workspace_tree_query" in {item.name for item in tools}
            if self.calls == 1:
                yield ModelToolCall(
                    id="plan-read",
                    name="workspace_tree_query",
                    arguments={"kind": "by_key", "request_key": "missing"},
                )
            else:
                assert TreePlanQueryResult.model_validate_json(messages[-1].content).plan is None
                yield ModelText(text="原键缺失")

    provider = MissingReader()
    runtime.provider = provider
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("readonly", tmp_path, version="v1", writable=False)
    service.register_tree_queries()
    try:
        for mode in ("plan", "ask"):
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name=f"readonly-{mode}",
                        description="只读模式",
                        workspace_key="readonly",
                        tool_names=["workspace_tree_query"],
                        mode=mode,
                    ),
                )
            )
            agent = AgentSession(service, (await service.spawn(f"readonly-{mode}")).agent_id)
            if mode == "plan":
                result = await agent.execute("只读原键", idempotency_key="plan-read")
                assert (
                    result.status == "completed"
                    and result.text == "原键缺失"
                    and provider.calls == 2
                )
            else:
                with pytest.raises(ServiceError) as denied:
                    await service._workspace_call(
                        agent.agent_id,
                        "workspace_tree_query",
                        {"kind": "by_key", "request_key": "missing"},
                    )
                assert denied.value.code == "permission_denied"
        assert (
            await counts(runtime.repository.database) == [0, 0, 0, 0, 0, 0]
            and list(tmp_path.iterdir()) == []
        )
    finally:
        await service.close()


async def test_model_query_stop_cancels_original_read_and_tool_timeout_depth_hold(
    runtime, tmp_path, monkeypatch
):
    provider = Reader()
    runtime.provider = provider
    service, agent, plan = await setup(runtime, tmp_path)
    try:
        provider.plan = plan
        entered = asyncio.Event()
        released = asyncio.Event()
        original = service.manager.tree_queries.store.get

        async def slow(*args, **kwargs):
            entered.set()
            await released.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(service.manager.tree_queries.store, "get", slow)
        task = asyncio.create_task(agent.execute("读取并取消", idempotency_key="cancel-query"))
        await asyncio.wait_for(entered.wait(), 10)
        await agent.stop()
        result = await asyncio.wait_for(task, 10)
        assert result.status == "aborted" and provider.calls == 1
        messages = await runtime.repository.run_messages(result.run_id)
        tool = next(message for message in messages if message.role == "tool")
        assert tool.details["call_id"] == "tree-query-1" and tool.details["code"] == "cancelled"
        handle = service._handle(agent.agent_id)
        context = ToolContext(
            handle.runtime.tools,
            topic_id=handle.topic_id,
            agent_id=agent.agent_id,
            session_id=agent.session_id,
            workspace=handle.definition.workspace,
            workspace_tree_queries=service.manager.tree_queries,
            origin="host",
            timeout=0.02,
        )
        value = await handle.runtime.tools.invoke(
            "workspace_tree_query", {"kind": "plan", "plan_id": plan.plan_id}, context
        )
        assert value.is_error and value.code == "timeout"
        context.depth = 8
        value = await handle.runtime.tools.invoke(
            "workspace_tree_query", {"kind": "plan", "plan_id": plan.plan_id}, context
        )
        assert value.is_error and value.code == "max_depth"
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
    finally:
        await service.close()
