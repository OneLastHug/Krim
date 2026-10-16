"""冻结权限、原Ask/Plan、当前Owner/父会话和最后撤权均约束Host回收。"""

import pytest
from sqlalchemy import func, select, update
from test_container_retention_host import completed, drain, request, setup
from test_container_retention_host import controller as controller
from test_missing_directory_restore_control import checkpoint

from krim_agent.persistence.database import AgentSessionRow
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_container_retention_control import CONTAINER_RETENTION_STEP_TOOLS
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)


@pytest.mark.parametrize("decision", ["deny", "ask", "confirmed"])
async def test_original_read_write_risks_and_explicit_ask_confirmation(controller, decision):
    service, agent, binding, original, preview = controller
    seen = []

    async def policy(definition, _context):
        seen.append((definition.name, definition.risk))
        return "allow" if definition.risk == "read" else "deny" if decision == "deny" else "ask"

    service._handle(agent.agent_id).runtime.tool_policy = policy
    observed = await service.workspace_preview_container_retention(agent.agent_id, original.plan_id)
    assert observed == preview
    started = await service.workspace_start_container_retention(
        agent.agent_id, request(preview), confirmed=decision == "confirmed"
    )
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert seen == [
        (CONTAINER_RETENTION_STEP_TOOLS["inspect"], "read"),
        (CONTAINER_RETENTION_STEP_TOOLS["remove"], "write"),
    ]
    assert final.status == ("done" if decision == "confirmed" else "interrupted"), final
    if decision != "confirmed":
        assert final.error_code == (
            "permission_denied" if decision == "deny" else "approval_required"
        )
        assert not final.intent_recorded and list(binding.workspace.root.glob("._krim-tx-*"))


async def test_late_registration_does_not_grant_frozen_agent_new_retention_leaves(
    runtime, tmp_path
):
    names = [
        "workspace_directory_checkpoint_capture",
        *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
    ]
    service, agent, binding = await setup(runtime, tmp_path, names=names)
    try:
        original = await completed(service, agent, binding)
        before = await service.workspace_info(agent.agent_id)
        frozen = tuple(service._handle(agent.agent_id).runtime.tools.definitions)
        service.register_container_retentions()
        with pytest.raises(ServiceError) as denied:
            await service.workspace_preview_container_retention(agent.agent_id, original.plan_id)
        assert denied.value.code == "workspace_container_retention_unavailable"
        assert await service.workspace_info(agent.agent_id) == before
        assert tuple(service._handle(agent.agent_id).runtime.tools.definitions) == frozen
        assert set(before.tool_names) == set(names)
        assert not service.container_retentions._jobs
        async with runtime.repository.database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(Row)) == 0
    finally:
        await service.close()


@pytest.mark.parametrize("mode", ["plan", "ask"])
async def test_original_frozen_modes_cannot_elevate_to_write_even_with_confirmation(
    runtime, tmp_path, mode
):
    from test_container_retention_sources import terminal

    from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
    from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

    service, agent, binding = await setup(runtime, tmp_path, mode=mode)
    try:
        # 可信存储层构造真实已完成来源；不把Plan/Ask冒称执行过Host恢复叶。
        if mode == "ask":
            with pytest.raises(ServiceError) as denied:
                await service.workspace_preview_container_retention(agent.agent_id, "unavailable")
            assert denied.value.code == "permission_denied"
            return
        target = await checkpoint(agent)
        snapshot = WorkspaceTreeSnapshotReader(binding.workspace).capture("tree")
        (tmp_path / "tree").rename(tmp_path / "retained-original")
        with WorkspaceDirectoryAbsenceReader(binding.workspace) as reader:
            context = (
                service.missing_directory_restore_store,
                "session:" + agent.session_id,
                binding,
                snapshot,
                target.checkpoint_id,
                reader,
            )
            _, _, original = await terminal(context)
        preview = await service.workspace_preview_container_retention(
            agent.agent_id, original.plan_id
        )
        assert preview.status == "empty"
        started = await service.workspace_start_container_retention(
            agent.agent_id, request(preview), confirmed=True
        )
        await drain(service)
        final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
        assert final.status == "interrupted" and final.error_code == "permission_denied"
        assert not final.intent_recorded and list(tmp_path.glob("._krim-tx-*"))
    finally:
        await service.close()


async def test_post_hook_revocation_rolls_back_receipt_retains_unknown_and_old_history(controller):
    service, agent, binding, original, preview = controller

    async def revoked(definition, value, _context):
        if definition.name == CONTAINER_RETENTION_STEP_TOOLS["remove"]:
            binding.revoked.set()
        return value

    service._handle(agent.agent_id).runtime.tools.add_global_post(revoked)
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == "unknown" and final.error_code == "workspace_unavailable"
    assert not list(binding.workspace.root.glob("._krim-tx-*"))
    assert (
        await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        == original
    )
    assert (
        await service.workspace_start_container_retention(agent.agent_id, request(preview)) == final
    )


@pytest.mark.parametrize("fault", ["owner", "parent"])
async def test_current_owner_or_parent_close_rejects_new_retention_before_record(controller, fault):
    service, agent, _, original, preview = controller
    database = service.container_retention_store.database
    owner = None
    async with database.sessions() as sql, sql.begin():
        row = await sql.get(AgentSessionRow, agent.session_id)
        if fault == "owner":
            owner = row.owner_id
            row.owner_id = "different-owner"
    if fault == "parent":
        # 明确合成父关联，然后真实关闭父SQL事实；不冒称委派产品流程。
        other = await service.spawn("container-retention")
        async with database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentSessionRow)
                .where(AgentSessionRow.session_id == agent.session_id)
                .values(parent_session_id=other.agent_id)
            )
            await sql.execute(
                update(AgentSessionRow)
                .where(AgentSessionRow.session_id == other.agent_id)
                .values(status="closed")
            )
    try:
        with pytest.raises(ServiceError):
            await service.workspace_start_container_retention(agent.agent_id, request(preview))
        async with database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(Row)) == 0
        assert (
            await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
            == original
        )
    finally:
        if fault == "owner":
            # 合成失主验证后归还原测试Owner，服务才能按真实所有权关闭。
            async with database.sessions() as sql, sql.begin():
                await sql.execute(
                    update(AgentSessionRow)
                    .where(AgentSessionRow.session_id == agent.session_id)
                    .values(owner_id=owner)
                )
