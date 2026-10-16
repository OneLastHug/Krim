"""原缺失模型根/私有叶的策略、Plan、预算和同事务Run撤权。"""

import asyncio

import pytest
from sqlalchemy import func, select
from test_model_missing_directory_restores import PREVIEW, START, phase_setup

from krim_agent.agents.permissions import profile_policy
from krim_agent.persistence.database import (
    AgentSessionRow,
    RunRow,
    WorkspaceMissingDirectoryRestoreModelSourceRow,
)
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter


@pytest.mark.parametrize(
    "action,mode,decision,approve,allowed",
    [
        ("preview", "agent", "deny", True, False),
        ("preview", "agent", "ask", False, False),
        ("preview", "agent", "ask", True, True),
        ("preview", "plan", "allow", True, True),
        ("start", "plan", "allow", True, False),
        ("start", "agent", "deny", True, False),
        ("start", "agent", "ask", False, False),
        ("start", "agent", "ask", True, True),
    ],
)
async def test_missing_root_policy_and_plan_precede_original_admission(
    runtime, tmp_path, monkeypatch, action, mode, decision, approve, allowed
):
    service, agent, _, provider, planned = await phase_setup(runtime, tmp_path, action, mode=mode)
    selected = PREVIEW if action == "preview" else START

    async def policy(definition, _context):
        return decision if definition.name == selected else "allow"

    async def approval(_definition, _context):
        return approve

    handle = service._handle(agent.agent_id)
    handle.runtime.tool_policy = profile_policy(mode, policy)
    handle.runtime.approval_handler = approval

    def forbidden(*_args, **_kwargs):
        pytest.fail("原根未授权不能取得缺失观察或准备/发布")

    if not allowed:
        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
    try:
        result = await agent.execute("原缺失根权限", idempotency_key="root-policy")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed", (result, provider.reply)
        if allowed:
            assert final.phase == ("ready" if action == "preview" else "finished"), final
        else:
            assert final == planned
        assert (tmp_path / "tree").exists() is (allowed and action == "start")
        async with runtime.repository.database.sessions() as sql:
            assert await sql.scalar(
                select(func.count()).select_from(WorkspaceMissingDirectoryRestoreModelSourceRow)
            ) == int(allowed)
        audits = await runtime.repository.audits(result.run_id)
        assert any(row.name == selected and row.ok is allowed for row in audits), audits
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_missing_leaf_deny_keeps_exact_source_without_namespace(runtime, tmp_path, action):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, action)
    selected = MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]

    async def policy(definition, _context):
        return "deny" if definition.name == selected else "allow"

    service._handle(agent.agent_id).runtime.tool_policy = policy
    try:
        result = await agent.execute("原缺失叶拒绝", idempotency_key="leaf-deny")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed", (result, provider.reply)
        assert final.phase == "failed" and final.error_code == "permission_denied", final
        assert not final.cleanup_pending and not (tmp_path / "tree").exists()
        async with runtime.repository.database.sessions() as sql:
            sources = list(
                await sql.scalars(select(WorkspaceMissingDirectoryRestoreModelSourceRow))
            )
        assert [(row.run_id, row.call_id) for row in sources] == [(result.run_id, provider.call_id)]
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
@pytest.mark.parametrize("level", ["root", "leaf"])
async def test_missing_policy_cannot_extend_original_deadline(runtime, tmp_path, action, level):
    service, agent, _, provider, planned = await phase_setup(runtime, tmp_path, action)
    selected = (
        (PREVIEW if action == "preview" else START)
        if level == "root"
        else MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]
    )
    changed = []

    async def policy(definition, context):
        if definition.name == selected:
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 60
            )
            changed.append(True)
        return "allow"

    service._handle(agent.agent_id).runtime.tool_policy = policy
    try:
        result = await agent.execute("策略不能延长原期限", idempotency_key="policy-deadline")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and changed == [True], (result, provider.reply)
        expected = (
            "workspace_checkpoint_model_budget"
            if level == "root"
            else "workspace_missing_directory_restore_context"
        )
        if level == "root":
            assert final == planned
        else:
            assert final.phase == "failed" and final.error_code == expected, final
        assert not (tmp_path / "tree").exists()
        audits = await runtime.repository.audits(result.run_id)
        assert any(row.name == selected and not row.ok and row.code == expected for row in audits)
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
@pytest.mark.parametrize(
    "fault,code",
    [
        ("run", "workspace_run_closed"),
        ("actor", "agent_session_owner"),
        ("resource", "workspace_binding_changed"),
    ],
)
async def test_missing_original_run_revoked_inside_authority_transaction_precedes_io(
    runtime, tmp_path, monkeypatch, action, fault, code
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, action)
    original = service.manager.workspace_changes._guard
    revoked = []

    async def revoke(context, **kwargs):
        value = await original(context, **kwargs)
        sql = kwargs.get("sql_session")
        if sql is not None and context.directory_restore is not None and not revoked:
            if fault == "run":
                run = await sql.get(RunRow, context.run_id)
                run.stop_requested = True
            else:
                session = await sql.get(AgentSessionRow, context.session_id)
                if fault == "actor":
                    session.owner_id = "other-owner"
                else:
                    session.permissions = {
                        **session.permissions,
                        "workspace": {**session.permissions["workspace"], "fingerprint": "0" * 64},
                    }
            await sql.flush()
            revoked.append(context.run_id)
        return value

    def forbidden(*_args, **_kwargs):
        pytest.fail("同一事务中原Run已撤权不能进入物理IO")

    monkeypatch.setattr(service.manager.workspace_changes, "_guard", revoke)
    monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
    try:
        result = await agent.execute("同事务撤销原Run", idempotency_key="sql-revoke")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and revoked == [result.run_id], (result, provider.reply)
        assert final.phase == "failed" and final.error_code == code, final
        assert not final.cleanup_pending and not (tmp_path / "tree").exists()
        assert not service.missing_directory_restores._tasks
        assert runtime.repository.database.engine.sync_engine.pool.checkedout() == 0
    finally:
        await service.close()
