"""模型原Run/call真正完成目录捕获，不把拒绝或Host回调当模型闭环。"""

import asyncio
import json

import pytest
from sqlalchemy import func, select
from test_directory_checkpoint_capture import make_tree

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import (
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceDirectoryCaptureRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, DirectoryCheckpointCreate, DirectoryCheckpointView
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

CREATE = "workspace_directory_checkpoint_create"
LEAF = "workspace_directory_checkpoint_capture"
QUERY = "workspace_directory_checkpoint_query"
CANCEL = "workspace_directory_checkpoint_cancel"
NAMES = [CREATE, LEAF, QUERY, CANCEL]


class Provider:
    name = "offline-directory-checkpoint"

    def __init__(self):
        self.calls = 0
        self.created = None
        self.queried = None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            assert {CREATE, LEAF, QUERY, CANCEL} <= {tool.name for tool in tools}
            yield ModelToolCall(
                id="directory-original-create",
                name=CREATE,
                arguments={"root_path": "tree", "request_key": "model-directory"},
            )
        elif self.calls == 2:
            self.created = DirectoryCheckpointView.model_validate_json(messages[-1].content)
            yield ModelToolCall(
                id="directory-original-query",
                name=QUERY,
                arguments={"kind": "key", "request_key": "model-directory"},
            )
        else:
            self.queried = json.loads(messages[-1].content)["checkpoint"]
            yield ModelText(text="目录原模型捕获已完成")


class OneShotProvider:
    name = "offline-directory-control-boundary"

    def __init__(self, name=CREATE, arguments=None):
        self.tool = name
        self.arguments = (
            arguments
            if arguments is not None
            else {"root_path": "tree", "request_key": "one-directory"}
        )
        self.calls, self.reply = 0, None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(id="directory-one-call", name=self.tool, arguments=self.arguments)
        else:
            self.reply = messages[-1].content
            yield ModelText(text="目录控制回程已读取")


async def setup(
    runtime,
    root,
    provider,
    *,
    policy=None,
    mode="agent",
    writable=True,
    names=None,
    persistent=False,
    tool_timeout=10,
    round_timeout=60,
):
    async def allow(_definition, _context):
        return "allow"

    runtime.provider, runtime.tool_policy = provider, policy or allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, version="1", writable=writable, default=True)
    service.register_directory_checkpoints()
    service.register_directory_checkpoint_controls()
    service.register_directory_checkpoint_controls()
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="model-directory",
                    description="原模型目录检查点",
                    workspace_key="files",
                    tool_names=NAMES if names is None else names,
                    mode=mode,
                    persistent=persistent,
                    tool_timeout_seconds=tool_timeout,
                    timeout_seconds=round_timeout,
                ),
            )
        )
        return service, AgentSession(service, (await service.spawn("model-directory")).agent_id)
    except BaseException:
        await service.close()
        raise


async def source_count(runtime):
    async with runtime.repository.database.sessions() as sql:
        return await sql.scalar(
            select(func.count()).select_from(WorkspaceDirectoryCaptureModelSourceRow)
        )


async def test_model_create_query_original_run_parent_call_complete_material_and_sdk_fact(
    runtime, tmp_path
):
    make_tree(tmp_path)
    provider = Provider()
    service, agent = await setup(runtime, tmp_path, provider)
    internal = []

    async def observe(definition, context):
        if definition.name == LEAF:
            internal.append(
                (
                    context.run_id,
                    context.call_id,
                    context.source_call_id,
                    context.origin,
                    context.depth,
                    callable(context.directory_checkpoint_qualify),
                )
            )

    service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
    try:
        result = await agent.execute("完整捕获tree并查询原任务", idempotency_key="model-directory")
        assert result.status == "completed" and provider.calls == 3, result
        final = await agent.directory_checkpoint_by_key("model-directory")
        assert final.status == "available" and final == provider.created
        assert final.model_dump(mode="json") == provider.queried
        assert len(internal) == 1 and internal[0][0] == result.run_id
        assert internal[0][1] != "directory-original-create"
        assert internal[0][2:] == ("directory-original-create", "model", 2, True)
        assert await source_count(runtime) == 1
        source = await service.directory_checkpoint_store.source(
            final.scope_id, final.checkpoint_id
        )
        assert source == CheckpointModelSource(result.run_id, "directory-original-create")
        async with runtime.repository.database.sessions() as sql:
            row = await sql.get(WorkspaceDirectoryCaptureRow, final.checkpoint_id)
            assert row.model_bound and row.owner_id is None
            material = await service.directory_checkpoint_store._material(sql, row)
            files = {
                entry.path: entry.file
                for entry in material.snapshot.entries
                if entry.file is not None
            }
            assert (
                files["tree/nested/nul"].data == b"\0\xff"
                and files["tree/nested/nul"].mode == 0o640
            )
        assert final.snapshot.entry_count == 6 and final.snapshot.byte_size > 0
        assert (
            str(tmp_path) not in final.model_dump_json()
            and "private synthetic" not in final.model_dump_json()
        )
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 3 and all(audit.ok for audit in audits), audits
        with pytest.raises(ServiceError) as host_claim:
            await agent.create_directory_checkpoint(
                DirectoryCheckpointCreate(root_path="tree", request_key="model-directory")
            )
        assert host_claim.value.code == "workspace_directory_capture_model_conflict"
    finally:
        await service.close()


@pytest.mark.parametrize(
    "mode,decision,approve,created",
    [
        ("agent", "deny", True, False),
        ("agent", "ask", False, False),
        ("plan", "allow", True, False),
        ("agent", "ask", True, True),
    ],
)
async def test_model_root_permission_and_confirmation_precede_source_and_io(
    runtime, tmp_path, monkeypatch, mode, decision, approve, created
):
    make_tree(tmp_path)

    async def policy(definition, _context):
        return decision if definition.name == CREATE else "allow"

    service, agent = await setup(runtime, tmp_path, OneShotProvider(), mode=mode, policy=policy)

    async def approval(_definition, _context):
        return approve

    service._handle(agent.agent_id).runtime.approval_handler = approval
    if not created:
        monkeypatch.setattr(
            WorkspaceTreeSnapshotReader,
            "capture",
            lambda *_args, **_kwargs: pytest.fail("未授权根调用不得访问目录"),
        )
    try:
        result = await agent.execute("根权限", idempotency_key="root-permission")
        assert result.status == "completed", result
        assert await source_count(runtime) == int(created)
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert (final is not None) == created
        if created:
            assert final.status == "available", final
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["readonly", "missing-leaf", "old-frozen"])
async def test_readonly_missing_leaf_and_old_frozen_agent_have_zero_sources(
    runtime, tmp_path, monkeypatch, fault
):
    make_tree(tmp_path)
    names = (
        [CREATE, QUERY, CANCEL]
        if fault == "missing-leaf"
        else [LEAF]
        if fault == "old-frozen"
        else None
    )
    if fault == "readonly":
        with pytest.raises(ServiceError) as denied:
            await setup(runtime, tmp_path, OneShotProvider(), names=names, writable=False)
        assert denied.value.code == "workspace_read_only"
        assert await source_count(runtime) == 0
        return
    service, agent = await setup(
        runtime, tmp_path, OneShotProvider(), names=names, writable=fault != "readonly"
    )
    monkeypatch.setattr(
        WorkspaceTreeSnapshotReader,
        "capture",
        lambda *_args, **_kwargs: pytest.fail("未授资格不得访问目录"),
    )
    try:
        result = await agent.execute("冻结与只读边界", idempotency_key="frozen")
        assert result.status == "completed", result
        assert await source_count(runtime) == 0
        assert await agent.directory_checkpoints() == []
    finally:
        await service.close()


async def test_nested_leaf_real_ask_pause_keeps_original_budget_and_producer(runtime, tmp_path):
    make_tree(tmp_path)

    async def policy(definition, _context):
        return "ask" if definition.name == LEAF else "allow"

    service, agent = await setup(runtime, tmp_path, OneShotProvider(), policy=policy)
    paused = []

    async def approve(definition, context):
        assert definition.name == LEAF and context.depth == 2
        paused.append(context.execution_timeouts[0])
        assert paused[-1].deadline.when() is None
        await asyncio.sleep(0.25)
        return True

    service._handle(agent.agent_id).runtime.approval_handler = approve
    try:
        result = await agent.execute("目录内部读取审批", idempotency_key="leaf-ask")
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert result.status == "completed" and final.status == "available", (result, final)
        assert len(paused) == 1 and paused[0].pause_extension >= 0.25
        assert await source_count(runtime) == 1
    finally:
        await service.close()
