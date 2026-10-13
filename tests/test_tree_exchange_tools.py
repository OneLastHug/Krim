"""实际原模型Run/call完整树交换、来源封存/审批预算/Hook及冷事实。"""

import json
import sys

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from test_workspace_tree_exchange import seed
from test_workspace_tree_exchange_ledger import action

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.context_engine import ContextRunConfig
from krim_agent.persistence.database import (
    WorkspaceTreeExchangeAttemptRow,
    WorkspaceTreeExchangeLockRow,
    WorkspaceTreeExchangeModelSourceRow,
    WorkspaceTreeExchangeOriginRow,
    WorkspaceTreeExchangeProposalRow,
    WorkspaceTreeSealRow,
)
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit, ToolContext, ToolError
from krim_agent.tools.core import ToolDefinition
from krim_agent.tools.tree_exchanges import tree_exchange_tools
from krim_agent.usage import TokenUsage
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeAction,
    TreeExchangeInspectRequest,
    TreeExchangeOperationRequest,
    TreeExchangePreviewRequest,
    TreeExchangePreviewView,
    TreeExchangeQueryRequest,
    TreeExchangeSealRequest,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="实际Linux交换/树锁")
NAMES = [item.name for item in tree_exchange_tools()]


async def allow(_definition, _context):
    return "allow"


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceTreeExchangeProposalRow,
                WorkspaceTreeExchangeAttemptRow,
                WorkspaceTreeExchangeLockRow,
                WorkspaceTreeExchangeModelSourceRow,
                WorkspaceTreeExchangeOriginRow,
                WorkspaceTreeSealRow,
            )
        ]


class Provider:
    name = "offline-original-tree-exchange"

    def __init__(self):
        self.calls = 0
        self.plan_id = None
        self.left_path, self.right_path = "holder/live", "new-parent/prepared"
        self.left = self.right = self.proposal = None
        self.actions = []
        self.last_result = None

    async def stream(self, messages, tools):
        assert set(NAMES) <= {item.name for item in tools}
        stages = (
            ["inspect_right", "seal", "inspect_left"]
            if self.plan_id is not None
            else ["inspect_left", "inspect_right"]
        ) + ["preview", "apply", "query", "keep", "query", "undo", "query", "done"]
        previous = json.loads(messages[-1].content) if self.calls else None
        self.last_result = previous
        previous_stage = stages[self.calls - 1] if self.calls else None
        if previous_stage == "inspect_left":
            self.left = previous["fingerprint"]
        elif previous_stage == "inspect_right":
            self.right = previous["fingerprint"]
        elif previous_stage == "seal":
            assert (
                previous["plan_id"] == self.plan_id and previous["tree_fingerprint"] == self.right
            )
        elif previous_stage in {"preview", "query"}:
            self.proposal = TreeExchangePreviewView.model_validate_json(
                json.dumps(previous["proposal"] if previous_stage == "query" else previous)
            )
        elif previous_stage in {"apply", "keep", "undo"}:
            assert previous["status"] == "done", previous
            self.actions.append(previous)
        stage = stages[self.calls]
        self.calls += 1
        if stage == "done":
            assert self.proposal.status == "undone"
            yield ModelText(text="原双完整树交换、保留和条件撤销完成")
        else:
            if stage.startswith("inspect_"):
                name = "tree_exchange_inspect"
                arguments = {"path": self.left_path if stage.endswith("left") else self.right_path}
            elif stage == "seal":
                name = "tree_exchange_seal"
                arguments = {"plan_id": self.plan_id, "expected_fingerprint": self.right}
            elif stage == "preview":
                name = "tree_exchange_preview"
                arguments = {
                    "left_path": self.left_path,
                    "right_path": self.right_path,
                    "expected_left": self.left,
                    "expected_right": self.right,
                    "request_key": "model-preview",
                }
                if self.plan_id is not None:
                    arguments["source_plan_id"] = self.plan_id
            elif stage == "query":
                name = "tree_exchange_query"
                arguments = {"kind": "proposal", "identifier": self.proposal.proposal_id}
            else:
                name = "tree_exchange_change"
                arguments = action(self.proposal, stage, "model-" + stage).model_dump()
            yield ModelToolCall(id=f"exchange-call-{self.calls}", name=name, arguments=arguments)
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def setup(
    runtime, root, *, policy=allow, mode="agent", writable=True, provider=None, enabled=True
):
    seed(root)
    runtime.tool_policy = policy
    if provider is not None:
        runtime.provider = provider
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace("files", root, version="v1", writable=writable)
    if enabled:
        service.register_tree_exchanges()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="exchange",
                description="原双完整树",
                workspace_key="files",
                tool_names=(NAMES if writable else ["tree_exchange_inspect", "tree_exchange_query"])
                if enabled
                else [],
                mode=mode,
                max_steps=16,
                tool_timeout_seconds=30,
                timeout_seconds=180,
                usage_budget=UsageBudgetConfig(max_total_tokens=100_000),
                context_config=ContextRunConfig(
                    tokenizer="cl100k_base", reserved_output_tokens=128
                ),
            ),
        )
    )
    return service, AgentSession(service, (await service.spawn("exchange")).agent_id), binding


async def context(service, agent, *, model=False, call_id="original"):
    handle = service._handle(agent.agent_id)
    run_id = "local"
    if model:
        run, _ = await service.runtime.repository.begin_run(
            handle.topic_id,
            ChatRequest(text="原模型准入", idempotency_key=call_id),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=service.manager.owner_id,
        )
        run_id = run.id
    return ToolContext(
        handle.runtime.tools,
        run_id=run_id,
        topic_id=handle.topic_id,
        agent_id=agent.agent_id,
        session_id=agent.session_id,
        call_id=call_id,
        origin="model" if model else "host",
        workspace=handle.definition.workspace,
        tree_exchanges=service.manager.tree_exchanges,
        policy=handle.runtime.tool_policy,
        timeout=30,
        model_tools={item.name: item for item in handle.runtime.tools.definitions},
    )


async def call(service, agent, name, payload, *, confirmed=False):
    request_types = {
        "tree_exchange_inspect": TreeExchangeInspectRequest,
        "tree_exchange_seal": TreeExchangeSealRequest,
        "tree_exchange_preview": TreeExchangePreviewRequest,
        "tree_exchange_change": TreeExchangeAction,
        "tree_exchange_query": TreeExchangeQueryRequest,
        "tree_exchange_reconcile": TreeExchangeOperationRequest,
    }
    value = await getattr(service, name)(
        agent.agent_id, request_types[name].model_validate(payload), confirmed=confirmed
    )
    return value.model_dump(mode="json")


async def preview(service, agent, key="host-preview"):
    left = await call(service, agent, "tree_exchange_inspect", {"path": "holder/live"})
    right = await call(service, agent, "tree_exchange_inspect", {"path": "new-parent/prepared"})
    request = TreeExchangePreviewRequest(
        left_path=left["path"],
        right_path=right["path"],
        expected_left=left["fingerprint"],
        expected_right=right["fingerprint"],
        request_key=key,
    )
    value = await call(service, agent, "tree_exchange_preview", request.model_dump())
    return TreeExchangePreviewView.model_validate_json(json.dumps(value)), request


async def test_real_provider_same_run_call_usage_audit_full_binary_keep_undo_and_cold_zero_replay(
    runtime, tmp_path, monkeypatch
):
    provider = Provider()
    service, agent, binding = await setup(runtime, tmp_path, provider=provider)
    inodes = [(tmp_path / path).stat().st_ino for path in (provider.left_path, provider.right_path)]
    try:
        result = await agent.execute("原完整交换/保留/撤销", idempotency_key="original-loop")
        assert result.status == "completed" and provider.calls == 10
        assert [item["receipt"]["changed"] for item in provider.actions] == [True, False, True]
        assert [
            (tmp_path / path).stat().st_ino for path in (provider.left_path, provider.right_path)
        ] == inodes
        assert (tmp_path / provider.left_path / "sub/data.bin").read_bytes() == b"\0\xff\x80"
        handle = service._handle(agent.agent_id)
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 9 and all(
            item.ok and item.run_id == result.run_id and item.depth == 1 for item in audits
        )
        assert result.token_usage.total_tokens == 110
        accounts = await handle.runtime.budget_store.accounts()
        assert accounts[0].spent_tokens == 110 and accounts[0].held_tokens == 0
        store = service.manager.tree_exchanges.store
        async with store.database.sessions() as sql:
            rows = (await sql.scalars(select(WorkspaceTreeExchangeModelSourceRow))).all()
            assert len(rows) == 4 and {row.run_id for row in rows} == {result.run_id}
            assert {row.call_id for row in rows} == {
                "exchange-call-3",
                "exchange-call-4",
                "exchange-call-6",
                "exchange-call-8",
            }

        def forbidden(*_args, **_kwargs):
            pytest.fail("已完成原键冷读取不能重新prepare/apply/Provider")

        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "prepare_undo", forbidden)
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", forbidden)
        monkeypatch.setattr(provider, "stream", forbidden)
        query = await call(
            service,
            agent,
            "tree_exchange_query",
            {"kind": "by_key", "request_key": "model-preview"},
        )
        assert query["proposal"]["status"] == "undone"
        assert (await counts(store.database)) == [1, 3, 0, 4, 0, 0]
    finally:
        await service.close()


async def test_actual_generated_plan_model_seal_source_apply_keep_undo_original_operations(
    runtime, tmp_path
):
    from test_completed_tree_origins import complete

    provider = Provider()
    runtime.provider = provider
    runtime.tools.register_many(*tree_exchange_tools())
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path, extra_names=NAMES
    )
    provider.plan_id = plan.plan_id
    provider.left_path, provider.right_path = "live", "tree"
    (tmp_path / "live").mkdir()
    (tmp_path / "live/old.bin").write_bytes(b"old\xff\0")
    try:
        service.register_tree_exchanges()
        result = await agent.execute(
            "原生成树封存及已有树完整更新", idempotency_key="generated-loop"
        )
        assert result.status == "completed" and provider.calls == 11, json.dumps(
            {
                "result": result.model_dump(mode="json"),
                "calls": provider.calls,
                "last": provider.last_result,
            },
            ensure_ascii=False,
        )
        assert (
            provider.proposal.origin == "completed_tree"
            and provider.proposal.source_plan_id == plan.plan_id
        )
        assert (tmp_path / "live/old.bin").read_bytes() == b"old\xff\0"
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        store = service.manager.tree_exchanges.store
        sealed = await store.seals.get(scope, binding, plan.plan_id)
        assert sealed.origin.snapshot == snapshot
        assert sealed.origin.operation_ids == tuple(item["operation_id"] for item in outputs)
        assert (
            sealed.model_source.run_id == result.run_id
            and sealed.model_source.call_id == "exchange-call-2"
        )
        assert await counts(store.database) == [1, 3, 0, 4, 1, 1]
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["pre", "post"])
@pytest.mark.parametrize(
    "fault",
    [
        "arguments",
        "call",
        "run",
        "session",
        "origin",
        "backend",
        "tools",
        "depth",
        "deadline",
        "response",
    ],
)
async def test_hook_cannot_redirect_parameters_identity_backend_budget_or_response(
    runtime, tmp_path, phase, fault
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        ctx = await context(service, agent)
        registry = ctx.registry

        def corrupt(child, value):
            if fault == "arguments":
                child.arguments["path"] = "new-parent/prepared"
            elif fault == "call":
                child.call_id = "forged"
            elif fault == "run":
                child.run_id = "forged"
            elif fault == "session":
                child.session_id = "forged"
            elif fault == "origin":
                child.origin = "model"
            elif fault == "backend":
                child.tree_exchanges = None
            elif fault == "tools":
                child.model_tools.clear()
            elif fault == "depth":
                child.registry.max_depth += 1
            elif fault == "deadline":
                child.execution_timeouts[-1].deadline.reschedule(None)
            elif phase == "pre":
                return ShortCircuit({"path": "holder/live", "found": False})
            else:
                child._tree_exchange_response = ("inspect", json.dumps(value, sort_keys=True) + " ")
            return None

        async def before(_definition, child):
            return corrupt(child, None)

        async def after(_definition, value, child):
            corrupt(child, value)
            return value

        if phase == "pre":
            registry.add_global_pre(before)
        else:
            registry.add_global_post(after)
        with pytest.raises(ToolError) as refused:
            await registry.call_tool("tree_exchange_inspect", {"path": "holder/live"}, ctx)
        assert refused.value.code == (
            "workspace_tree_exchange_unverified"
            if fault == "response"
            else "workspace_tree_exchange_context"
        )
        assert registry.audit[-1].call_id == ctx.call_id and registry.audit[-1].run_id == ctx.run_id
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("policy_kind", ["deny", "ask", "plan"])
async def test_original_write_policy_and_confirmation_cannot_expand_permission(
    runtime, tmp_path, policy_kind
):
    async def policy(definition, _context):
        return "allow" if definition.risk == "read" or policy_kind == "plan" else policy_kind

    service, agent, _ = await setup(
        runtime, tmp_path, policy=policy, mode="plan" if policy_kind == "plan" else "agent"
    )
    try:
        proposal, _ = await preview(service, agent)
        request = action(proposal)
        with pytest.raises(ServiceError) as refused:
            await service.tree_exchange_change(
                agent.agent_id, request, confirmed=policy_kind != "ask"
            )
        assert refused.value.code == (
            "approval_required" if policy_kind == "ask" else "permission_denied"
        )
        assert await counts(service.workspace_edits.database) == [1, 0, 0, 0, 0, 0]
        if policy_kind == "ask":
            assert (
                await service.tree_exchange_change(agent.agent_id, request, confirmed=True)
            ).status == "done"
    finally:
        await service.close()


async def test_explicit_registration_conflict_and_old_frozen_agents_are_not_expanded(
    runtime, tmp_path
):
    service, agent, _ = await setup(runtime, tmp_path, enabled=False)
    try:
        original = tuple(service._handle(agent.agent_id).runtime.tools.definitions)
        service.register_tree_exchanges()
        service.register_tree_exchanges()
        assert tuple(service._handle(agent.agent_id).runtime.tools.definitions) == original
        with pytest.raises(ServiceError) as missing:
            await service.tree_exchange_inspect(
                agent.agent_id, TreeExchangeInspectRequest(path="holder/live")
            )
        assert missing.value.code == "tool_not_found"
    finally:
        await service.close()
    registry = runtime.tools.fork()
    # 冲突检查使用独立Registry，不替换已冻结Agent的能力。
    registry._definitions["tree_exchange_query"] = ToolDefinition(
        "tree_exchange_query", "占用", TreeExchangeQueryRequest, lambda *_args: None, "read"
    )
    definitions = tuple(registry.definitions)
    other = AgentService(runtime, runtime.settings)
    try:
        other.runtime.tools = registry
        with pytest.raises(ValueError):
            other.register_tree_exchanges()
        assert tuple(registry.definitions) == definitions
    finally:
        await other.close()


@pytest.mark.parametrize(
    "field", ["scope_id", "owner_id", "run_id", "call_id", "token", "snapshot", "payload", "proof"]
)
def test_public_preview_seal_requests_forbid_private_material_and_authority(field):
    with pytest.raises(ValidationError):
        TreeExchangePreviewRequest.model_validate(
            {
                "left_path": "left",
                "right_path": "right",
                "expected_left": "a" * 64,
                "expected_right": "b" * 64,
                "request_key": "key",
                field: "private",
            }
        )
    with pytest.raises(ValidationError):
        TreeExchangeSealRequest.model_validate(
            {"plan_id": "plan", "expected_fingerprint": "a" * 64, field: "private"}
        )
