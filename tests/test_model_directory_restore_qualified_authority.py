"""同事务原主体撤销、真实短租约Ask续租均保留原Run/预算与最后资格。"""

import asyncio

import pytest
from test_model_directory_restores import phase_setup

from krim_agent.persistence.database import RunRow
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_original_run_revoked_inside_authority_transaction_cannot_produce_directory(
    runtime, tmp_path, monkeypatch, action
):
    service, agent, binding, provider, _ = await phase_setup(runtime, tmp_path, action)
    original = service.manager.workspace_changes._guard
    revoked = []

    async def revoke(context, **kwargs):
        value = await original(context, **kwargs)
        sql = kwargs.get("sql_session")
        if sql is not None and context.directory_restore is not None and not revoked:
            row = await sql.get(RunRow, context.run_id)
            assert row is not None
            row.stop_requested = True
            await sql.flush()
            revoked.append(row.id)
        return value

    def forbidden(*_args, **_kwargs):
        pytest.fail("同一事务内原Run被撤销后不能进入物理生产")

    monkeypatch.setattr(service.manager.workspace_changes, "_guard", revoke)
    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
    try:
        result = await agent.execute("事务内撤销原Run", idempotency_key="authority-closed")
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "completed" and revoked == [result.run_id], (result, provider.reply)
        assert final.phase == "failed" and final.error_code == "workspace_run_closed", final
        assert not final.cleanup_pending and not service.directory_restores._tasks
        async with runtime.repository.database.sessions() as sql:
            row = await sql.get(RunRow, result.run_id)
            assert row.status == "completed" and not row.stop_requested
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    finally:
        await service.close()


async def test_short_original_lease_renews_during_real_leaf_ask_without_budget_reset(
    runtime, tmp_path, monkeypatch
):
    async def policy(definition, _context):
        return "ask" if definition.name == DIRECTORY_RESTORE_STEP_TOOLS["prepare"] else "allow"

    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, policy=policy)
    store = service.directory_restore_store
    store.lease_seconds = 1
    original, renewals, paused = store.renew, [], []

    async def renew(*args, **kwargs):
        await original(*args, **kwargs)
        renewals.append(True)

    async def approve(_definition, context):
        paused.append(context.execution_timeouts[0])
        assert paused[-1].deadline.when() is None
        await asyncio.sleep(0.8)
        assert len(renewals) >= 2
        return True

    monkeypatch.setattr(store, "renew", renew)
    service._handle(agent.agent_id).runtime.approval_handler = approve
    try:
        result = await agent.execute("原短租约跨Ask续租", idempotency_key="short-lease-ask")
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "completed" and final.phase == "ready", (
            result,
            final,
            provider.reply,
        )
        assert len(paused) == 1 and paused[0].pause_extension >= 0.8
        assert not service.directory_restores._tasks
    finally:
        await service.close()
