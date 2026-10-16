"""冻结能力、主体、审批及关闭根后的安全历史不提升工作区权限。"""

import asyncio

import pytest
from test_model_directory_restores import (
    CANCEL,
    NAMES,
    QUERY,
    OneShotProvider,
    phase_setup,
)

from krim_agent.sdk import AgentSession, DirectoryRestoreQueryResult
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def test_missing_frozen_leaf_refuses_admission_and_registration_never_promotes_old_agent(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, provider, _ = await phase_setup(
        runtime,
        tmp_path,
        names=[name for name in NAMES if name != DIRECTORY_RESTORE_STEP_TOOLS["prepare"]],
    )
    registry = service._handle(agent.agent_id).runtime.tools
    before = [definition.schema().model_dump(mode="json") for definition in registry.definitions]

    def forbidden(*_args, **_kwargs):
        pytest.fail("未冻结叶工具不得读完整目录")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
    try:
        service.register_directory_restores()
        service.register_directory_checkpoint_restore_controls()
        assert [
            definition.schema().model_dump(mode="json") for definition in registry.definitions
        ] == before
        result = await agent.execute("缺失原冻结叶", idempotency_key="missing-leaf")
        assert (
            result.status == "completed"
            and await agent.directory_checkpoint_restore_by_key("boundary-preview") is None
        )
        audits = await runtime.repository.audits(result.run_id)
        assert any(
            row.code == "workspace_directory_restore_unavailable" and not row.ok for row in audits
        ), provider.reply
    finally:
        await service.close()


async def test_original_model_depth_limit_precedes_directory_reader(runtime, tmp_path, monkeypatch):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path)
    service._handle(agent.agent_id).runtime.tools.max_depth = 1
    monkeypatch.setattr(
        WorkspaceTreeSnapshotReader,
        "capture",
        lambda *_a, **_k: pytest.fail("深度不足不能读取完整目录"),
    )
    try:
        result = await agent.execute("原深度限制", idempotency_key="depth")
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "completed" and final.phase == "failed", (
            result,
            final,
            provider.reply,
        )
        assert final.error_code == "max_depth" and final.backup is None
    finally:
        await service.close()


async def test_original_leaf_ask_pauses_real_parent_budget_and_retains_original_source(
    runtime, tmp_path
):
    leaf = DIRECTORY_RESTORE_STEP_TOOLS["execute"]

    async def policy(definition, _context):
        return "ask" if definition.name == leaf else "allow"

    service, agent, binding, provider, _ = await phase_setup(
        runtime, tmp_path, "start", policy=policy
    )
    paused = []

    async def approve(definition, context):
        assert definition.name == leaf and context.origin == "model" and context.depth == 2
        assert context.source_call_id == provider.call_id
        paused.append(context.execution_timeouts[0])
        assert paused[-1].deadline.when() is None
        await asyncio.sleep(0.25)
        return True

    service._handle(agent.agent_id).runtime.approval_handler = approve
    try:
        result = await agent.execute("原叶审批预算暂停", idempotency_key="ask")
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "completed" and final.phase == "finished", (
            result,
            final,
            provider.reply,
        )
        assert len(paused) == 1 and paused[0].pause_extension >= 0.25
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()


@pytest.mark.parametrize("operation", [QUERY, CANCEL])
async def test_other_session_cannot_read_or_cancel_original_plan(runtime, tmp_path, operation):
    service, agent, _, _, planned = await phase_setup(runtime, tmp_path, "start")
    try:
        other = AgentSession(service, (await service.spawn("model-directory-restore")).agent_id)
        provider = OneShotProvider(
            operation,
            {"plan_id": planned.plan_id, **({"kind": "id"} if operation == QUERY else {})},
        )
        service._handle(other.agent_id).runtime.provider = provider
        result = await other.execute("跨Scope拒绝", idempotency_key="other-scope")
        assert result.status == "completed"
        audits = await runtime.repository.audits(result.run_id)
        assert any(
            row.code == "workspace_directory_restore_not_found" and not row.ok for row in audits
        ), provider.reply
        assert await agent.directory_checkpoint_restore(planned.plan_id) == planned
    finally:
        await service.close()


async def test_all_model_queries_are_cold_after_original_workspace_closed(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, provider, planned = await phase_setup(runtime, tmp_path, "start")
    try:
        result = await agent.execute("原模型发布", idempotency_key="publish")
        original = await agent.directory_checkpoint_restore(planned.plan_id)
        assert result.status == "completed" and original.phase == "finished"
        service.disable_workspace("files")
        binding.workspace.close()

        def forbidden(*_args, **_kwargs):
            pytest.fail("关闭资源后的历史查询不得重新捕获或生产")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "verify", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
        queries = [
            {"kind": "id", "plan_id": original.plan_id},
            {"kind": "key", "request_key": "boundary-preview"},
            {"kind": "history", "limit": 1},
            {"kind": "diff", "plan_id": original.plan_id, "limit": 128},
            {"kind": "operation", "operation_id": original.operation_id},
            {"kind": "operation_by_key", "request_key": "boundary-execute"},
        ]
        for index, query in enumerate(queries):
            provider.calls, provider.name_value, provider.arguments = 0, QUERY, query
            read = await agent.execute("关闭根后读取原账本", idempotency_key=f"cold-{index}")
            value = DirectoryRestoreQueryResult.model_validate_json(provider.reply)
            assert read.status == "completed" and value.kind == query["kind"], (
                read,
                provider.reply,
            )
            if value.plan is not None:
                assert value.plan == original
            if value.operation is not None:
                assert (
                    value.operation.operation_id == original.operation_id
                    and value.operation.status == "done"
                )
            if value.diff is not None:
                assert (
                    value.diff.plan_id == original.plan_id
                    and value.diff.entries[0].path == "tree/nested/nul"
                )
            if value.entries:
                assert value.entries == [original]
        assert not service.directory_restores._tasks
    finally:
        await service.close()
