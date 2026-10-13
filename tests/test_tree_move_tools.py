"""合成Provider与原Run/call实际完整树工具循环、冷键和拒绝边界。"""

import json
import sys
from dataclasses import replace

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from test_workspace_tree_move_ledger import action, counts
from test_workspace_tree_snapshot import seed

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.context_engine import ContextRunConfig
from krim_agent.persistence.database import WorkspaceTreeMoveModelSourceRow
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit, ToolContext, ToolError
from krim_agent.tools.core import ToolDefinition
from krim_agent.tools.tree_moves import tree_move_tools
from krim_agent.usage import TokenUsage
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveInspectRequest,
    TreeMoveOperationRequest,
    TreeMovePreviewRequest,
    TreeMovePreviewView,
    TreeMoveQueryRequest,
)
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="原Linux树锁与rename")
NAMES = [item.name for item in tree_move_tools()]


async def allow(_definition, _context):
    return "allow"


class Provider:
    name = "offline-original-tree-move"

    def __init__(self):
        self.calls = 0
        self.proposal = None
        self.actions = []

    async def stream(self, messages, tools):
        self.calls += 1
        assert set(NAMES) == {item.name for item in tools}
        previous = json.loads(messages[-1].content) if self.calls > 1 else None
        if self.calls == 1:
            name, arguments = "tree_move_inspect", {"path": "tree"}
        elif self.calls == 2:
            assert previous["found"] and previous["entry_count"] == 6
            name, arguments = (
                "tree_move_preview",
                {
                    "source_path": "tree",
                    "target_path": "dest/tree",
                    "expected_fingerprint": previous["fingerprint"],
                    "request_key": "model-preview",
                },
            )
        elif self.calls == 3:
            self.proposal = TreeMovePreviewView.model_validate_json(json.dumps(previous))
            name, arguments = (
                "tree_move_change",
                action(self.proposal, key="model-apply").model_dump(),
            )
        elif self.calls in {4, 6, 8}:
            assert previous["status"] == "done", previous
            self.actions.append(previous)
            name, arguments = (
                "tree_move_query",
                {"kind": "proposal", "identifier": self.proposal.proposal_id},
            )
        elif self.calls in {5, 7}:
            self.proposal = TreeMovePreviewView.model_validate_json(
                json.dumps(previous["proposal"])
            )
            kind = "keep" if self.calls == 5 else "undo"
            name, arguments = (
                "tree_move_change",
                action(self.proposal, kind, "model-" + kind).model_dump(),
            )
        else:
            self.proposal = TreeMovePreviewView.model_validate_json(
                json.dumps(previous["proposal"])
            )
            assert self.proposal.status == "undone"
            yield ModelText(text="原非空树移动、保留和撤销完成")
            yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))
            return
        yield ModelToolCall(id=f"tree-move-call-{self.calls}", name=name, arguments=arguments)
        yield ModelUsage(usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11))


async def setup(
    runtime, root, *, policy=allow, mode="agent", writable=True, provider=None, enabled=True
):
    seed(root)
    (root / "dest").mkdir()
    runtime.tool_policy = policy
    if provider is not None:
        runtime.provider = provider
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace("files", root, version="v1", writable=writable)
    if enabled:
        service.register_tree_moves()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="move",
                description="原非空树",
                workspace_key="files",
                tool_names=(NAMES if writable else ["tree_move_inspect", "tree_move_query"])
                if enabled
                else [],
                mode=mode,
                max_steps=12,
                tool_timeout_seconds=30,
                timeout_seconds=120,
                usage_budget=UsageBudgetConfig(max_total_tokens=100_000),
                context_config=ContextRunConfig(
                    tokenizer="cl100k_base", reserved_output_tokens=128
                ),
            ),
        )
    )
    agent = AgentSession(service, (await service.spawn("move")).agent_id)
    return service, agent, binding


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
        tree_moves=service.manager.tree_moves,
        policy=handle.runtime.tool_policy,
        timeout=30,
    )


async def call(service, agent, name, payload, *, confirmed=False):
    request_types = {
        "tree_move_inspect": TreeMoveInspectRequest,
        "tree_move_preview": TreeMovePreviewRequest,
        "tree_move_change": TreeMoveAction,
        "tree_move_query": TreeMoveQueryRequest,
        "tree_move_reconcile": TreeMoveOperationRequest,
    }
    result = await getattr(service, name)(
        agent.agent_id, request_types[name].model_validate(payload), confirmed=confirmed
    )
    return result.model_dump(mode="json")


async def preview(service, agent, key="host-preview"):
    inspected = await call(service, agent, "tree_move_inspect", {"path": "tree"})
    request = TreeMovePreviewRequest(
        source_path="tree",
        target_path="dest/tree",
        expected_fingerprint=inspected["fingerprint"],
        request_key=key,
    )
    value = await call(service, agent, "tree_move_preview", request.model_dump())
    return TreeMovePreviewView.model_validate_json(json.dumps(value)), request


async def test_provider_original_run_call_budget_audit_binary_inode_and_keep_undo(
    runtime, tmp_path
):
    provider = Provider()
    service, agent, binding = await setup(runtime, tmp_path, provider=provider)
    inode = (tmp_path / "tree/sub/data.bin").stat().st_ino
    try:
        handle = service._handle(agent.agent_id)
        seen = []

        async def observe(_definition, child):
            seen.append(
                (
                    child.origin,
                    child.run_id,
                    child.call_id,
                    child.depth,
                    child.budget_account_ids,
                    len(child.execution_timeouts),
                    child.execution,
                )
            )

        handle.runtime.tools.add_global_pre(observe)
        result = await agent.execute("移动原树后保留再撤销", idempotency_key="original-loop")
        assert result.status == "completed", result
        assert result.text == "原非空树移动、保留和撤销完成"
        assert provider.calls == 9 and result.token_usage.total_tokens == 99
        assert len(seen) == 8 and all(
            item[0] == "model"
            and item[1] == result.run_id
            and item[3] == 1
            and item[4]
            and item[5] == 1
            and item[6] is None
            for item in seen
        )
        assert [entry["receipt"]["changed"] for entry in provider.actions] == [True, False, True]
        assert (tmp_path / "tree/sub/data.bin").stat().st_ino == inode
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        assert not (tmp_path / "dest/tree").exists()
        store = service.manager.tree_moves.store
        assert await counts(store.database) == [1, 3, 0, 4]
        async with store.database.sessions() as sql:
            rows = list(await sql.scalars(select(WorkspaceTreeMoveModelSourceRow)))
            assert {(row.run_id, row.call_id, row.kind) for row in rows} == {
                (result.run_id, "tree-move-call-2", "preview"),
                (result.run_id, "tree-move-call-3", "apply"),
                (result.run_id, "tree-move-call-5", "keep"),
                (result.run_id, "tree-move-call-7", "undo"),
            }
        accounts = await handle.runtime.budget_store.accounts()
        assert accounts[0].spent_tokens == 99 and accounts[0].held_tokens == 0
        assert [
            (item.call_id, item.run_id, item.depth, item.ok)
            for item in await runtime.repository.audits(result.run_id)
        ] == [(f"tree-move-call-{index}", result.run_id, 1, True) for index in range(1, 9)]
        public = await call(
            service,
            agent,
            "tree_move_query",
            {"kind": "proposal", "identifier": provider.proposal.proposal_id},
        )
        assert public["proposal"] == provider.proposal.model_dump(mode="json")
        assert provider.calls == 9 and await counts(store.database) == [1, 3, 0, 4]
        assert not any(
            secret in json.dumps(public)
            for secret in ("payload", "operation_token", "owner_id", "inode", "run_id", "call_id")
        )
    finally:
        await service.close()


async def test_original_keys_cold_without_second_scan_undo_prepare_or_namespace(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, request = await preview(service, agent)
        apply_request = action(proposal).model_dump()
        done = await call(service, agent, "tree_move_change", apply_request)
        current = (
            await service.manager.tree_moves.store.proposal(
                "session:" + agent.session_id, proposal.proposal_id
            )
        ).view
        undo_request = action(current, "undo", "host-undo").model_dump()
        undone = await call(service, agent, "tree_move_change", undo_request)

        def forbidden(*_args, **_kwargs):
            pytest.fail("原键冷读再次扫描/准备/执行")

        for method in ("prepare", "prepare_undo", "apply", "reconcile", "verify_keep"):
            monkeypatch.setattr(WorkspaceTreeWriter, method, forbidden)
        assert (await call(service, agent, "tree_move_preview", request.model_dump()))[
            "status"
        ] == "undone"
        assert await call(service, agent, "tree_move_change", apply_request) == done
        assert await call(service, agent, "tree_move_change", undo_request) == undone
        query = await call(
            service,
            agent,
            "tree_move_query",
            {"kind": "operation_by_key", "request_key": "host-undo"},
        )
        assert query["operation"] == undone
        page = await call(
            service,
            agent,
            "tree_move_query",
            {"kind": "history", "identifier": proposal.proposal_id, "limit": 1},
        )
        assert len(page["history"]["items"]) == 1 and page["history"]["next_after"]
        next_page = await call(
            service,
            agent,
            "tree_move_query",
            {
                "kind": "history",
                "identifier": proposal.proposal_id,
                "limit": 1,
                "after": page["history"]["next_after"],
            },
        )
        assert (
            len(next_page["history"]["items"]) == 1 and next_page["history"]["next_after"] is None
        )
        assert await counts(service.manager.tree_moves.store.database) == [1, 2, 0, 0]
        with pytest.raises(ServiceError, match="参数"):
            await call(
                service,
                agent,
                "tree_move_preview",
                request.model_copy(update={"ttl_seconds": 20}).model_dump(),
            )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "mutation",
    [
        "path",
        "origin",
        "run_id",
        "call_id",
        "depth",
        "policy",
        "backend",
        "short",
        "post",
        "marker",
        "budget",
        "loaded_tools",
        "registry_depth",
        "registry_tools",
    ],
)
async def test_hooks_cannot_redirect_context_short_circuit_or_forge_fact(
    runtime, tmp_path, mutation
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        registry = service._handle(agent.agent_id).runtime.tools

        async def pre(_definition, child):
            if mutation == "path":
                child.arguments["path"] = "dest"
            elif mutation == "backend":
                child.tree_moves = None
            elif mutation == "short":
                return ShortCircuit({})
            elif mutation == "budget":
                child.budget_account_ids = ("other-budget",)
            elif mutation == "loaded_tools":
                child.model_tools.clear()
            elif mutation == "registry_depth":
                child.registry.max_depth += 1
            elif mutation == "registry_tools":
                child.registry.register(
                    ToolDefinition(
                        "extra", "越界", TreeMoveQueryRequest, lambda *_args: None, "read"
                    )
                )
            elif mutation not in {"post", "marker"}:
                setattr(
                    child,
                    mutation,
                    {
                        "origin": "model",
                        "run_id": "elsewhere",
                        "call_id": "other",
                        "depth": 2,
                        "policy": None,
                    }[mutation],
                )

        async def post(_definition, value, child):
            if mutation == "post":
                value["file_count"] = 0
            if mutation == "marker":
                child._tree_move_response = ("inspect", json.dumps(value, sort_keys=True) + " ")
            return value

        registry.add_global_pre(pre)
        registry.add_global_post(post)
        ctx = await context(service, agent)
        ctx.model_tools = {item.name: item for item in registry.definitions}
        with pytest.raises(ToolError) as error:
            await registry.call_tool("tree_move_inspect", {"path": "tree"}, ctx)
        assert error.value.code == (
            "workspace_tree_move_unverified"
            if mutation in {"short", "post", "marker"}
            else "workspace_tree_move_context"
        )
        audit = registry.audit[-1]
        assert (audit.call_id, audit.run_id, audit.depth, audit.ok) == (
            ctx.call_id,
            "local",
            1,
            False,
        )
        assert await counts(service.manager.tree_moves.store.database) == [0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "proposal"},
        {"kind": "by_key"},
        {"kind": "operation", "identifier": "x", "after": "y"},
        {"kind": "proposal", "identifier": "x", "limit": 1},
        {"kind": "proposals", "request_key": "x"},
        {"kind": "history", "identifier": "x", "limit": True},
        {"kind": "history", "identifier": "x", "limit": 33},
        {"kind": "history", "identifier": "x", "scope_id": "elsewhere"},
    ],
)
def test_strict_query_selection_and_private_fields(payload):
    with pytest.raises(ValidationError):
        TreeMoveQueryRequest.model_validate(payload)


@pytest.mark.parametrize("field", ["source_path", "target_path"])
def test_keep_rejects_receipt_path_substitution_and_keeps_original_inode(tmp_path, field):
    from krim_agent.workspace import Workspace

    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        before = writer.reader.capture("tree")
        change = writer.prepare("tree", "dest/tree", expected_fingerprint=before.fingerprint)
        effect = writer.apply(change)
        forged = replace(effect, receipt=replace(effect.receipt, **{field: "other"}))
        refused = writer.verify_keep(change, forged)
        assert (
            refused.receipt.status == "rejected"
            and refused.receipt.error_code == "workspace_tree_move_receipt"
        )
        assert not refused.receipt.changed
        assert writer.verify_keep(change, effect).receipt.status == "done"


async def test_registration_conflict_is_atomic_and_old_agent_frozen(runtime, tmp_path):
    service, agent, _ = await setup(runtime, tmp_path, enabled=False)
    try:
        service.runtime.tools.register(
            ToolDefinition(
                "tree_move_query", "占用", TreeMoveQueryRequest, lambda *_args: None, "read"
            )
        )
        before = tuple(service.runtime.tools.definitions)
        with pytest.raises(ValueError, match="占用"):
            service.register_tree_moves()
        assert tuple(service.runtime.tools.definitions) == before
        assert not set(NAMES) & {
            item.name for item in service._handle(agent.agent_id).runtime.tools.definitions
        }
    finally:
        await service.close()


@pytest.mark.parametrize(
    "mode,writable,expected",
    [("plan", True, "permission_denied"), ("agent", False, "workspace_read_only")],
)
async def test_plan_read_only_and_write_authority_do_not_admit_namespace(
    runtime, tmp_path, mode, writable, expected
):
    service, agent, _ = await setup(runtime, tmp_path, mode=mode, writable=writable)
    try:
        inspected = await call(service, agent, "tree_move_inspect", {"path": "tree"})
        assert inspected["found"]
        if writable:
            proposal, _ = await preview(service, agent)
            name, payload = "tree_move_change", action(proposal).model_dump()
        else:
            name, payload = (
                "tree_move_preview",
                TreeMovePreviewRequest(
                    source_path="tree",
                    target_path="dest/tree",
                    expected_fingerprint=inspected["fingerprint"],
                    request_key="readonly",
                ).model_dump(),
            )
        with pytest.raises((ServiceError, ToolError)) as error:
            if writable:
                await call(service, agent, name, payload, confirmed=True)
            else:
                ctx = await context(service, agent)
                await service.manager.tree_moves.tool_operation("preview", payload, ctx)
        assert error.value.code == expected
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        assert (await counts(service.manager.tree_moves.store.database))[1:] == [0, 0, 0]
    finally:
        await service.close()
