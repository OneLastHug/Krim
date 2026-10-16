"""跨主体、原深度、Ask暂停和关闭工作区后的六种冷模型读取。"""

import asyncio

import pytest
from test_model_missing_directory_restores import CANCEL, QUERY, OneShotProvider, phase_setup

from krim_agent.sdk import AgentSession
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_restore_model import MissingDirectoryRestoreQueryResult
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter


@pytest.mark.parametrize("name", [QUERY, CANCEL])
async def test_other_missing_session_cannot_read_or_cancel_original_plan(runtime, tmp_path, name):
    service, agent, _, _, planned = await phase_setup(runtime, tmp_path, "start")
    try:
        other = AgentSession(
            service, (await service.spawn("model-missing-directory-restore")).agent_id
        )
        provider = OneShotProvider(
            name, {"plan_id": planned.plan_id, **({"kind": "id"} if name == QUERY else {})}
        )
        service._handle(other.agent_id).runtime.provider = provider
        result = await other.execute("跨原Scope拒绝", idempotency_key="other-scope")
        assert result.status == "completed", (result, provider.reply)
        audits = await runtime.repository.audits(result.run_id)
        assert any(
            not row.ok and row.code == "workspace_missing_directory_restore_not_found"
            for row in audits
        )
        assert (
            await service.workspace_missing_directory_restore(agent.agent_id, planned.plan_id)
            == planned
        )
        assert not (tmp_path / "tree").exists()
    finally:
        await service.close()


async def test_missing_original_depth_limit_precedes_actual_absence_observation(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools
    registry.max_depth = 1

    def forbidden(*_args, **_kwargs):
        pytest.fail("原叶深度不足不得捕获缺失观察")

    monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
    try:
        frozen = tuple(registry.definitions)
        service.register_missing_directory_checkpoint_restore_controls()
        assert tuple(registry.definitions) == frozen
        result = await agent.execute("原叶深度限制", idempotency_key="depth")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and final.phase == "failed", (result, provider.reply)
        assert final.error_code == "max_depth" and final.baseline is None
        assert not (tmp_path / "tree").exists()
    finally:
        await service.close()


async def test_missing_leaf_real_ask_pauses_original_budget_without_new_source(runtime, tmp_path):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, "start")
    leaf = MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]
    paused = []

    async def policy(definition, _context):
        return "ask" if definition.name == leaf else "allow"

    async def approve(definition, context):
        assert definition.name == leaf and context.origin == "model" and context.depth == 2
        assert context.source_call_id == provider.call_id
        paused.append(context.execution_timeouts[0])
        assert paused[-1].deadline.when() is None
        await asyncio.sleep(0.2)
        return True

    handle = service._handle(agent.agent_id)
    handle.runtime.tool_policy, handle.runtime.approval_handler = policy, approve
    try:
        result = await agent.execute("原缺失叶真实审批", idempotency_key="leaf-ask")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and final.phase == "finished", (result, provider.reply)
        assert len(paused) == 1 and paused[0].pause_extension >= 0.2
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()


async def test_all_six_missing_model_queries_stay_cold_after_workspace_closed(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, provider, _ = await phase_setup(runtime, tmp_path, "start")
    try:
        result = await agent.execute("原缺失发布", idempotency_key="publish")
        original = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and original.phase == "finished"
        service.disable_workspace("files")
        binding.workspace.close()

        def forbidden(*_args, **_kwargs):
            pytest.fail("原根关闭后冷读取不能捕获或发布")

        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
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
            read = await agent.execute("原缺失历史冷读", idempotency_key=f"cold-{index}")
            value = MissingDirectoryRestoreQueryResult.model_validate_json(provider.reply)
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
                    value.diff.total == original.target_snapshot.entry_count
                    and value.diff.next_offset is None
                )
                assert all(
                    item.operation == "create" and item.before is None
                    for item in value.diff.entries
                )
            if value.kind == "history":
                assert value.entries == [original] and value.offset == 0 and value.limit == 1
                assert value.next_offset is None
        assert not service.missing_directory_restores._tasks
    finally:
        await service.close()
