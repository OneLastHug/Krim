"""公开模型准备/start同调用原树执行，原预算/权限/取消/持久消费者。"""

import asyncio
import json
import shutil
import sys
from dataclasses import replace

import httpx
import pytest
from pydantic import ValidationError
from test_model_workspace_tree_query import counts
from test_workspace_tree_public import NAMES, source

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.application.agents import AgentService
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.context_engine import ContextRunConfig
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence.database import WorkspaceTreeModelSourceRow
from krim_agent.protocol.domain import ChatRequest
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession, TreePlanStartRequest
from krim_agent.tools import ShortCircuit, ToolContext
from krim_agent.usage import TokenUsage
from krim_agent.workspace_tree import TreePlanView

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实隔离目录/原模型工具循环",
)


class Provider:
    name = "offline-public-tree-model"

    def __init__(self):
        self.calls = 0
        self.request = None
        self.plan = None
        self.error = None
        self.expected_status = "done"

    async def stream(self, messages, tools):
        self.calls += 1
        assert {"workspace_tree_model_prepare", "workspace_tree_start"} <= {
            item.name for item in tools
        }
        if self.calls == 1:
            yield ModelToolCall(
                id="model-prepare-call",
                name="workspace_tree_model_prepare",
                arguments=self.request.model_dump(mode="json"),
            )
        elif self.calls == 2:
            self.plan = TreePlanView.model_validate_json(messages[-1].content)
            assert self.plan.status == "planned"
            yield ModelToolCall(
                id="model-start-call",
                name="workspace_tree_start",
                arguments={"plan_id": self.plan.plan_id},
            )
        elif self.calls == 3:
            if self.error:
                assert messages[-1].details["code"] == self.error, messages[-1].content
                yield ModelText(text="原权限拒绝")
            else:
                self.plan = TreePlanView.model_validate_json(messages[-1].content)
                assert self.plan.status == self.expected_status, self.plan.model_dump_json()
                yield ModelToolCall(
                    id="model-query-call",
                    name="workspace_tree_query",
                    arguments={"kind": "plan", "plan_id": self.plan.plan_id},
                )
        else:
            assert json.loads(messages[-1].content)["plan"]["plan_id"] == self.plan.plan_id
            yield ModelText(text="原模型目录树完成")
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def setup(runtime, root, *, policy=None, enabled=True):
    async def allow(_definition, _context):
        return "allow"

    provider = Provider()
    runtime.provider, runtime.tool_policy = provider, policy or allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, version="v1", writable=True)
    service.register_execution("sandbox", LinuxSandboxRunner(), version="v1", owned=True)
    service.register_tree_queries()
    if enabled:
        service.register_tree_models()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="tree-model",
                description="完整原模型目录树",
                workspace_key="files",
                execution_key="sandbox",
                tool_names=[
                    *NAMES,
                    "workspace_tree_query",
                    *(["workspace_tree_model_prepare", "workspace_tree_start"] if enabled else []),
                ],
                tool_timeout_seconds=30,
                timeout_seconds=120,
                usage_budget=UsageBudgetConfig(max_total_tokens=100_000),
                context_config=ContextRunConfig(
                    tokenizer="cl100k_base", reserved_output_tokens=128
                ),
            ),
        )
    )
    agent = AgentSession(service, (await service.spawn("tree-model")).agent_id)
    provider.request = await source(service, agent)
    return service, agent, provider


async def context(service, agent, key="manual"):
    handle = service._handle(agent.agent_id)
    run, _ = await service.runtime.repository.begin_run(
        handle.topic_id,
        ChatRequest(text="manual source", idempotency_key=key),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=service.manager.owner_id,
    )
    return ToolContext(
        handle.runtime.tools,
        run_id=run.id,
        topic_id=handle.topic_id,
        agent_id=agent.agent_id,
        session_id=agent.session_id,
        origin="model",
        call_id=key,
        workspace=handle.definition.workspace,
        execution=handle.definition.execution,
        workspace_tree_models=service.manager.tree_models,
        workspace_changes=service.manager.workspace_changes,
        directory_changes=service.manager.directory_changes,
        commands=service.manager.commands,
        policy=handle.runtime.tool_policy,
        timeout=30,
    )


async def test_real_provider_prepare_start_query_run_call_budget_and_public_cold_consumers(
    runtime, tmp_path, monkeypatch
):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        seen = []

        async def pre(definition, child):
            if child.tree_fence is not None:
                seen.append(
                    (
                        definition.name,
                        child.origin,
                        child.run_id,
                        child.source_call_id,
                        child.depth,
                        child.budget_account_ids,
                        len(child.execution_timeouts),
                    )
                )

        service._handle(agent.agent_id).runtime.tools.add_global_pre(pre)
        result = await agent.execute("发布原目录成果", idempotency_key="tree-model-loop")
        assert result.status == "completed" and result.text == "原模型目录树完成", result
        assert provider.calls == 4 and result.token_usage.total_tokens == 44
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert len(seen) == 6 and all(
            item[1] == "model"
            and item[2] == result.run_id
            and item[3] == "model-start-call"
            and item[4] == 2
            and item[5]
            and item[6] == 2
            for item in seen
        )
        assert await counts(runtime.repository.database) == [1, 1, 2, 2, 1, 1]
        async with runtime.repository.database.sessions() as sql:
            for kind, caller in (("prepare", "model-prepare-call"), ("start", "model-start-call")):
                row = await sql.get(WorkspaceTreeModelSourceRow, (provider.plan.plan_id, kind))
                assert (row.run_id, row.call_id) == (result.run_id, caller)
        assert await agent.directory_tree(provider.plan.plan_id) == provider.plan
        assert await agent.directory_tree_by_key(provider.request.request_key) == provider.plan
        accounts = await service._handle(agent.agent_id).runtime.budget_store.accounts()
        assert accounts[0].spent_tokens == 44 and accounts[0].held_tokens == 0
        monkeypatch.setattr("krim_agent.api.server.AgentService", lambda *_args, **_kwargs: service)
        app = create_app(runtime=runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/tree-plans/{provider.plan.plan_id}"
            )
            assert response.json() == provider.plan.model_dump(mode="json")
        assert provider.calls == 4 and await counts(runtime.repository.database) == [
            1,
            1,
            2,
            2,
            1,
            1,
        ]
    finally:
        await service.close()


async def test_explicit_enable_keeps_old_frozen_tools_and_host_prepare_does_not_promote(
    runtime, tmp_path
):
    service, agent, provider = await setup(runtime, tmp_path, enabled=False)
    try:
        before = {item.name for item in service._handle(agent.agent_id).runtime.tools.definitions}
        service.register_tree_models()
        service.register_tree_models()
        assert {
            item.name for item in service._handle(agent.agent_id).runtime.tools.definitions
        } == before
        ctx = await context(service, agent)
        result = await ctx.registry.invoke(
            "workspace_tree_model_prepare", provider.request.model_dump(mode="json"), ctx
        )
        assert result.is_error and result.code == "tool_not_found"
        result = await ctx.registry.invoke(
            "workspace_tree_prepare", provider.request.model_dump(mode="json"), ctx
        )
        assert result.is_error and result.code == "workspace_tree_model_unbound"
        assert await counts(runtime.repository.database) == [1, 0, 0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"plan_id": "id", "run_id": "local"},
        {"plan_id": "id", "owner": "fake"},
        {"plan_id": "id", "request_key": "fake"},
        {"plan_id": "../root"},
    ],
)
def test_start_strict_plan_only_no_private_or_new_key(payload):
    with pytest.raises(ValidationError):
        TreePlanStartRequest.model_validate(payload)


@pytest.mark.parametrize(
    "fault",
    ["pre-key", "pre-origin", "pre-budget", "pre-backend", "short", "post-value", "post-marker"],
)
async def test_prepare_hooks_cannot_redirect_or_forge_original_plan(runtime, tmp_path, fault):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        ctx = await context(service, agent, "hook-source")

        async def pre(definition, child):
            if definition.name != "workspace_tree_model_prepare":
                return None
            if fault == "pre-key":
                child.arguments["request_key"] = "other"
            elif fault == "pre-origin":
                child.origin = "host"
            elif fault == "pre-budget":
                child.budget_account_ids = ("fake",)
            elif fault == "pre-backend":
                child.workspace_tree_models = object()
            elif fault == "short":
                return ShortCircuit({"status": "planned"})
            return None

        async def post(definition, value, child):
            if definition.name == "workspace_tree_model_prepare" and fault in {
                "post-value",
                "post-marker",
            }:
                value["request_key"] = "forged"
                if fault == "post-marker":
                    child._tree_model_response = ("prepare", json.dumps(value, sort_keys=True))
            return value

        ctx.registry.add_global_pre(pre)
        ctx.registry.add_global_post(post)
        result = await ctx.registry.invoke(
            "workspace_tree_model_prepare", provider.request.model_dump(mode="json"), ctx
        )
        assert result.is_error and result.code in {
            "workspace_tree_fence_mismatch",
            "workspace_tree_model_unverified",
        }
        assert await counts(runtime.repository.database) == (
            [1, 1, 0, 0, 0, 0] if fault.startswith("post") else [1, 0, 0, 0, 0, 0]
        )
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_model_same_call_start_retry_keeps_original_cancel_index_and_public_cancel(
    runtime, tmp_path
):
    service, agent, provider = await setup(runtime, tmp_path)
    entered, released = asyncio.Event(), asyncio.Event()
    task = None
    try:
        ctx = await context(service, agent, "prepare")
        prepared = await ctx.registry.invoke(
            "workspace_tree_model_prepare", provider.request.model_dump(mode="json"), ctx
        )
        assert not prepared.is_error
        plan = TreePlanView.model_validate_json(json.dumps(prepared.data))
        start = replace(ctx, call_id="start")

        async def pause(definition, child):
            if definition.name == "directory_change" and child.tree_fence is not None:
                entered.set()
                async with child.human_wait():
                    await released.wait()

        start.registry.add_global_pre(pause)
        task = asyncio.create_task(
            start.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, start)
        )
        await asyncio.wait_for(entered.wait(), 10)
        original = service.manager.tree_models._plans[plan.plan_id][1]
        retry = await start.registry.invoke(
            "workspace_tree_start", {"plan_id": plan.plan_id}, start
        )
        assert not retry.is_error and retry.data["producer_status"] == "running"
        assert service.manager.tree_models._plans[plan.plan_id][1] is original
        cancelled = await agent.cancel_directory_tree(plan.plan_id)
        assert cancelled.status == "cancelled"
        with pytest.raises(asyncio.CancelledError):
            await task
        assert plan.plan_id not in service.manager.tree_models._plans
        assert (
            await counts(runtime.repository.database) == [1, 1, 1, 0, 0, 0]
            and list(tmp_path.iterdir()) == []
        )
        again = await start.registry.invoke(
            "workspace_tree_start", {"plan_id": plan.plan_id}, start
        )
        assert not again.is_error and again.data["producer_status"] == "cancelled"
    finally:
        released.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_source_specific_child_deny_stops_after_real_directory_receipts(runtime, tmp_path):
    async def policy(definition, _context):
        return "deny" if definition.name == "workspace_change" else "allow"

    service, agent, provider = await setup(runtime, tmp_path, policy=policy)
    try:
        ctx = await context(service, agent, "prepare")
        prepared = await ctx.registry.invoke(
            "workspace_tree_model_prepare", provider.request.model_dump(mode="json"), ctx
        )
        plan = TreePlanView.model_validate_json(json.dumps(prepared.data))
        start = replace(ctx, call_id="start")
        result = await start.registry.invoke(
            "workspace_tree_start", {"plan_id": plan.plan_id}, start
        )
        assert (
            not result.is_error
            and result.data["status"] == "partial"
            and result.data["done_count"] == 2
        )
        assert not (tmp_path / "tree/sub/data.bin").exists()
        assert await counts(runtime.repository.database) == [1, 1, 2, 2, 1, 0]
    finally:
        await service.close()
