"""真实生成来源、原Service/Registry、合成Model循环与根专用事实/拒绝。"""

import copy
import json
import shutil
import sys

import pytest
from sqlalchemy import select
from test_completed_tree_origins import complete
from test_generated_tree_undo_ledger import counts
from test_tree_move_tools import context

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.generated_tree_undo import (
    GeneratedTreeUndoAction,
    GeneratedTreeUndoCancelRequest,
    GeneratedTreeUndoPreviewRequest,
    GeneratedTreeUndoPreviewView,
)
from krim_agent.persistence.database import WorkspaceTreeMoveModelSourceRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.tools import ShortCircuit, ToolContext, ToolError
from krim_agent.tools.core import ToolDefinition
from krim_agent.tools.generated_tree_undo import generated_tree_undo_tools
from krim_agent.usage import TokenUsage
from krim_agent.workspace_tree_move import TreeMovePreviewRequest, TreeMoveQueryRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="真实隔离生成/原子根移动",
)
NAMES = [item.name for item in generated_tree_undo_tools()]


async def setup(runtime, root, *, persistent=False, usage_budget=None):
    runtime.tools.register_many(*generated_tree_undo_tools())
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime,
        root,
        persistent=persistent,
        extra_names=NAMES,
        usage_budget=usage_budget,
    )
    service.register_generated_tree_undos()
    await service.manager.tree_moves.store.seals.seal(
        scope, binding, plan.plan_id, snapshot.fingerprint, snapshot
    )
    request = GeneratedTreeUndoPreviewRequest(
        plan_id=plan.plan_id,
        preserve_path="retained",
        expected_fingerprint=snapshot.fingerprint,
        request_key="root-preview",
    )
    return service, agent, scope, trees, plan, binding, snapshot, request


def action(view, kind="remove_root", key="root-remove"):
    return GeneratedTreeUndoAction(
        proposal_id=view.move.proposal_id,
        action=kind,
        expected_revision=view.move.revision,
        request_key=key,
    )


async def query(service, agent, kind, **kwargs):
    return await service.generated_tree_undo_query(
        agent.agent_id, TreeMoveQueryRequest(kind=kind, **kwargs)
    )


class Provider:
    name = "offline-original-generated-root"

    def __init__(self, request=None):
        self.request = request
        self.calls = 0
        self.proposal = None
        self.last = None

    async def stream(self, messages, tools):
        self.calls += 1
        assert set(NAMES) <= {item.name for item in tools}
        previous = json.loads(messages[-1].content) if self.calls > 1 else None
        self.last = previous
        if self.calls == 1:
            name, arguments = "generated_tree_undo_preview", self.request.model_dump()
        elif self.calls == 2:
            self.proposal = GeneratedTreeUndoPreviewView.model_validate_json(json.dumps(previous))
            name, arguments = "generated_tree_undo_change", action(self.proposal).model_dump()
        elif self.calls in {3, 5, 7}:
            assert previous["operation"]["status"] == "done", previous
            name, arguments = (
                "generated_tree_undo_query",
                {"kind": "proposal", "identifier": self.proposal.move.proposal_id},
            )
        elif self.calls in {4, 6}:
            self.proposal = GeneratedTreeUndoPreviewView.model_validate_json(
                json.dumps(previous["proposal"])
            )
            kind = "keep" if self.calls == 4 else "restore_root"
            name, arguments = (
                "generated_tree_undo_change",
                action(self.proposal, kind, "root-" + kind).model_dump(),
            )
        else:
            self.proposal = GeneratedTreeUndoPreviewView.model_validate_json(
                json.dumps(previous["proposal"])
            )
            assert (
                self.proposal.root_state == "restored" and self.proposal.generation_status == "done"
            )
            yield ModelText(text="原生成树根撤销、保留与条件还原完成")
            yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))
            return
        yield ModelToolCall(id=f"generated-root-call-{self.calls}", name=name, arguments=arguments)
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def test_actual_model_root_source_run_call_budget_audit_keep_restore_and_cold_history(
    runtime, tmp_path, monkeypatch
):
    provider = Provider()
    runtime.provider = provider
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(
        runtime, tmp_path, usage_budget=UsageBudgetConfig(max_total_tokens=100_000)
    )
    provider.request = request
    seen = []

    async def observe(definition, child):
        if definition.name in NAMES:
            seen.append(
                (
                    child.origin,
                    child.run_id,
                    child.call_id,
                    child.depth,
                    child.budget_account_ids,
                    len(child.execution_timeouts),
                )
            )

    service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
    try:
        result = await agent.execute("原生成根撤销与还原", idempotency_key="root-model-loop")
        assert result.status == "completed", (result.model_dump(), provider.last)
        assert provider.calls == 8 and provider.proposal.root_state == "restored"
        assert len(seen) == 7 and all(
            origin == "model" and run_id == result.run_id and depth == 1 and accounts and timeouts
            for origin, run_id, call_id, depth, accounts, timeouts in seen
        )
        store = service.manager.tree_moves.store
        async with store.database.sessions() as sql:
            rows = list(await sql.scalars(select(WorkspaceTreeMoveModelSourceRow)))
            assert {(row.run_id, row.call_id, row.kind) for row in rows} == {
                (result.run_id, "generated-root-call-1", "preview"),
                (result.run_id, "generated-root-call-2", "apply"),
                (result.run_id, "generated-root-call-4", "keep"),
                (result.run_id, "generated-root-call-6", "undo"),
            }
        accounts = await service._handle(agent.agent_id).runtime.budget_store.accounts()
        assert accounts[0].spent_tokens == 88 and accounts[0].held_tokens == 0
        assert [
            (item.call_id, item.run_id, item.depth, item.ok)
            for item in await runtime.repository.audits(result.run_id)
        ] == [
            (f"generated-root-call-{position}", result.run_id, 1, True) for position in range(1, 8)
        ]
        assert await trees.get(scope, plan.plan_id) == plan
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert (tmp_path / "tree/sub/data.bin").stat().st_ino == snapshot.entries[-1].file.inode
        assert not (tmp_path / "retained").exists()
        assert await counts(store.database) == [1, 1, 3, 0, 4]
        history = await query(
            service, agent, "history", identifier=provider.proposal.move.proposal_id, limit=2
        )
        rest = await query(
            service,
            agent,
            "history",
            identifier=provider.proposal.move.proposal_id,
            after=history.history.next_after,
            limit=2,
        )
        combined = [*history.history.items, *rest.history.items]
        assert len(combined) == 3 and {item.action for item in combined} == {
            "remove_root",
            "keep",
            "restore_root",
        }
        identifiers = [item.operation.operation_id for item in combined]
        assert identifiers == sorted(identifiers) and len(set(identifiers)) == 3
        assert rest.history.next_after is None
        before_calls = provider.calls

        def forbidden(*_args, **_kwargs):
            pytest.fail("根原键/历史只读不得重跑生成、Provider或Writer/观察根")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        cold = await service.generated_tree_undo_preview(agent.agent_id, request)
        assert cold == provider.proposal
        assert (
            await query(service, agent, "by_key", request_key=request.request_key)
        ).proposal == cold
        assert (
            await query(service, agent, "operation_by_key", request_key="root-remove")
        ).operation.source_plan_id == plan.plan_id
        assert provider.calls == before_calls
    finally:
        await service.close()


async def test_host_root_original_source_labels_real_preserve_restore_repeat_and_pagination(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        proposal = await service.generated_tree_undo_preview(agent.agent_id, request)
        assert (
            proposal.root_state == "awaiting_approval" and proposal.source_plan_id == plan.plan_id
        )
        assert (
            proposal.source_command_id == plan.command_id
            and proposal.sealed_fingerprint == snapshot.fingerprint
        )
        removed = await service.generated_tree_undo_change(agent.agent_id, action(proposal))
        assert removed.operation.status == "done" and removed.action == "remove_root"
        assert (
            not (tmp_path / "tree").exists()
            and (tmp_path / "retained/sub/data.bin").read_bytes() == b"\0\xff"
        )
        current = (
            await query(service, agent, "proposal", identifier=proposal.move.proposal_id)
        ).proposal
        restored = await service.generated_tree_undo_change(
            agent.agent_id, action(current, "restore_root", "host-restore")
        )
        assert restored.operation.status == "done"
        actual = WorkspaceTreeSnapshotReader(binding.require()).capture("tree")
        repeat = request.model_copy(
            update={
                "expected_fingerprint": actual.fingerprint,
                "previous_restore_operation_id": restored.operation.operation_id,
                "request_key": "host-repeat",
            }
        )
        again = await service.generated_tree_undo_preview(agent.agent_id, repeat)
        assert (
            again.previous_restore_operation_id == restored.operation.operation_id
            and again.sealed_fingerprint == snapshot.fingerprint
        )
        page = await query(service, agent, "proposals", limit=1)
        next_page = await query(
            service, agent, "proposals", after=page.proposals.next_after, limit=1
        )
        assert {
            page.proposals.items[0].move.proposal_id,
            next_page.proposals.items[0].move.proposal_id,
        } == {proposal.move.proposal_id, again.move.proposal_id}
        cancelled = await service.generated_tree_undo_cancel(
            agent.agent_id, GeneratedTreeUndoCancelRequest(proposal_id=again.move.proposal_id)
        )
        assert cancelled.root_state == "cancelled" and cancelled.generation_status == "done"
        with pytest.raises(ServiceError):
            await service.generated_tree_undo_change(
                agent.agent_id, action(again, key="cancelled-remove")
            )
        assert await trees.get(scope, plan.plan_id) == plan
    finally:
        await service.close()


async def test_generic_move_cannot_masquerade_as_generated_root_and_orphan_marker_not_hidden(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    store = service.manager.tree_moves.store
    try:
        writer = WorkspaceTreeWriter(binding.require())
        plain_request = TreeMovePreviewRequest(
            source_path="tree",
            target_path="plain",
            expected_fingerprint=snapshot.fingerprint,
            request_key="plain",
        )
        plain, _ = await store.preview(
            scope,
            binding,
            plain_request,
            writer.prepare("tree", "plain", expected_fingerprint=snapshot.fingerprint),
        )
        for call in (
            query(service, agent, "proposal", identifier=plain.view.proposal_id),
            query(service, agent, "by_key", request_key="plain"),
            service.generated_tree_undo_change(
                agent.agent_id,
                GeneratedTreeUndoAction(
                    proposal_id=plain.view.proposal_id,
                    action="remove_root",
                    expected_revision=0,
                    request_key="fake-root",
                ),
            ),
            service.generated_tree_undo_cancel(
                agent.agent_id, GeneratedTreeUndoCancelRequest(proposal_id=plain.view.proposal_id)
            ),
        ):
            with pytest.raises(ServiceError) as error:
                await call
            assert error.value.code == "workspace_generated_undo_required"
        assert (await query(service, agent, "proposals")).proposals.items == []
        root = await service.generated_tree_undo_preview(agent.agent_id, request)
        from krim_agent.persistence.database import WorkspaceTreeMoveProposalRow

        async with store.database.sessions.begin() as sql:
            (
                await sql.get(WorkspaceTreeMoveProposalRow, root.move.proposal_id)
            ).generated_plan_id = None
        with pytest.raises(ServiceError) as error:
            await query(service, agent, "proposals")
        assert error.value.code == "workspace_tree_move_corrupt"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["arguments", "source", "budget", "short", "response", "marker"])
async def test_root_registry_hooks_cannot_rewrite_scope_source_budget_or_canonical_response(
    runtime, tmp_path, fault
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools
    try:

        async def pre(_definition, child):
            if fault == "arguments":
                child.arguments["kind"] = "by_key"
                child.arguments["request_key"] = "other"
            elif fault == "source":
                child.call_id = "other"
            elif fault == "budget":
                child.execution_timeouts[-1].deadline.reschedule(None)
            elif fault == "short":
                return ShortCircuit(
                    {"kind": "proposals", "proposals": {"items": [], "next_after": None}}
                )

        async def post(_definition, value, child):
            if fault == "response":
                value["kind"] = "history"
            elif fault == "marker":
                child._tree_move_response = (
                    "generated_undo_query",
                    json.dumps(value, sort_keys=True) + " ",
                )
            return value

        registry.add_global_pre(pre)
        registry.add_global_post(post)
        ctx = await context(service, agent)
        with pytest.raises(ToolError) as error:
            await registry.call_tool("generated_tree_undo_query", {"kind": "proposals"}, ctx)
        assert error.value.code in {"workspace_tree_move_context", "workspace_tree_move_unverified"}
        assert await counts(service.manager.tree_moves.store.database) == [0, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_last_namespace_budget_check_runs_after_original_store_qualification(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    try:
        proposal = await service.generated_tree_undo_preview(agent.agent_id, request)
        registry = service._handle(agent.agent_id).runtime.tools
        contexts = []

        async def observe(_definition, child):
            contexts.append(child)

        registry.add_global_pre(observe)
        original = service.manager.tree_moves.store.assert_execution

        async def expire(*args):
            await original(*args)
            contexts[-1].execution_timeouts[-1].active = False

        monkeypatch.setattr(service.manager.tree_moves.store, "assert_execution", expire)
        with pytest.raises(ServiceError) as error:
            await service.generated_tree_undo_change(agent.agent_id, action(proposal))
        assert error.value.code == "workspace_tree_move_context"
        denied = await service.manager.tree_moves.store.operation_by_key(scope, "root-remove")
        assert denied.view.status == "rejected" and not denied.view.receipt.changed
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff" and not (
            tmp_path / "retained"
        ).exists()
    finally:
        await service.close()


async def test_original_root_permissions_confirmation_and_foreign_scope_fail_closed(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, request = await setup(runtime, tmp_path)
    handle = service._handle(agent.agent_id)
    try:

        async def ask(_definition, _context):
            return "ask"

        handle.runtime.tool_policy = ask
        with pytest.raises(ServiceError) as error:
            await service.generated_tree_undo_preview(agent.agent_id, request)
        assert error.value.code == "approval_required"
        proposal = await service.generated_tree_undo_preview(
            agent.agent_id, request, confirmed=True
        )
        with pytest.raises(ServiceError) as error:
            await service.generated_tree_undo_change(agent.agent_id, action(proposal))
        assert error.value.code == "approval_required"

        async def deny(_definition, _context):
            return "deny"

        handle.runtime.tool_policy = deny
        with pytest.raises(ServiceError) as error:
            await service.generated_tree_undo_change(
                agent.agent_id, action(proposal), confirmed=True
            )
        assert error.value.code == "permission_denied"
        ctx = await context(service, agent)
        ctx.policy = None
        ctx.topic_id = "foreign-topic"
        with pytest.raises(ToolError):
            await ctx.registry.call_tool(
                "generated_tree_undo_query",
                {"kind": "proposal", "identifier": proposal.move.proposal_id},
                ctx,
            )
        assert (await counts(service.manager.tree_moves.store.database))[:3] == [1, 1, 0]
    finally:
        await service.close()


async def test_explicit_root_registration_does_not_expand_old_agent_or_partially_register_conflict(
    runtime, tmp_path
):
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", tmp_path, version="1", writable=True)
    agent = await service.spawn("default")
    before = tuple(service._handle(agent.agent_id).runtime.tools.definitions)
    try:
        service.register_generated_tree_undos()
        service.register_generated_tree_undos()
        assert tuple(service._handle(agent.agent_id).runtime.tools.definitions) == before
        assert set(NAMES) <= {item.name for item in runtime.tools.definitions}
        from krim_agent.tools.builtins import builtin_registry

        other_runtime = copy.copy(runtime)
        other_runtime.tools = builtin_registry()
        conflict = generated_tree_undo_tools()[2]
        other_runtime.tools.register(
            ToolDefinition(
                conflict.name, "已有冲突", conflict.parameters, lambda *_args: None, "write"
            )
        )
        other = AgentService(other_runtime, runtime.settings)
        try:
            with pytest.raises(ValueError):
                other.register_generated_tree_undos()
            assert {item.name for item in other_runtime.tools.definitions} & set(NAMES) == {
                conflict.name
            }
        finally:
            await other.close()
    finally:
        await service.close()


@pytest.mark.parametrize(
    "mode,writable,expected",
    [("plan", True, "permission_denied"), ("agent", False, "workspace_read_only")],
)
async def test_root_plan_policy_and_readonly_binding_reject_write_before_source_or_namespace(
    runtime, tmp_path, mode, writable, expected
):
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace("files", tmp_path, version="1", writable=writable)
    service.register_generated_tree_undos()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="root-mode",
                description="原根计划模式与只读权限拒绝",
                workspace_key="files",
                tool_names=NAMES if writable else ["generated_tree_undo_query"],
                mode=mode,
            ),
        )
    )
    agent = await service.spawn("root-mode")
    try:
        if writable:
            with pytest.raises(ServiceError) as error:
                await service.generated_tree_undo_change(
                    agent.agent_id,
                    GeneratedTreeUndoAction(
                        proposal_id="missing",
                        action="remove_root",
                        expected_revision=0,
                        request_key="plan-write",
                    ),
                    confirmed=True,
                )
        else:
            ctx = ToolContext(
                runtime.tools,
                topic_id=agent.topic_id,
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                workspace=binding,
                tree_moves=service.manager.tree_moves,
                origin="host",
            )
            with pytest.raises(ToolError) as error:
                await runtime.tools.call_tool(
                    "generated_tree_undo_preview",
                    {
                        "plan_id": "missing",
                        "preserve_path": "retained",
                        "expected_fingerprint": "0" * 64,
                        "request_key": "readonly-preview",
                    },
                    ctx,
                )
        assert error.value.code == expected
        assert await counts(service.manager.tree_moves.store.database) == [0, 0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()
