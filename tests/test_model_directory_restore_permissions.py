"""原模型根/叶权限和Plan限制先于实际目录写入，Host确认不能冒领模型。"""

import json

import pytest
from sqlalchemy import func, select
from test_model_directory_restores import (
    PREVIEW,
    START,
    OneShotProvider,
    setup,
    target,
)

from krim_agent.persistence.database import WorkspaceDirectoryRestoreModelSourceRow
from krim_agent.sdk import DirectoryRestoreExecute, DirectoryRestorePreview, DirectoryRestoreTarget
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


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
async def test_root_permission_and_plan_mode_precede_model_admission_and_io(
    runtime, tmp_path, monkeypatch, action, mode, decision, approve, allowed
):
    name = PREVIEW if action == "preview" else START

    async def policy(definition, _context):
        return decision if definition.name == name else "allow"

    provider = OneShotProvider(name)
    service, agent, binding = await setup(runtime, tmp_path, provider, policy=policy, mode=mode)
    try:
        captured = await target(agent)
        (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")
        request = DirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=captured.checkpoint_id),
            request_key="original-model-preview",
        )
        if action == "start":
            planned = await agent.preview_directory_checkpoint_restore(request)
            planned = await agent.wait_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            assert planned.phase == "ready", planned
            provider.arguments = DirectoryRestoreExecute(
                plan_id=planned.plan_id,
                expected_revision=planned.revision,
                request_key="model-execute",
            ).model_dump()
        else:
            provider.arguments = request.model_dump()

        async def approval(_definition, _context):
            return approve

        service._handle(agent.agent_id).runtime.approval_handler = approval
        if not allowed:

            def forbidden(*_args, **_kwargs):
                pytest.fail("根未获准不得进入实际目录IO")

            monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
            monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
            monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
        result = await agent.execute("原根权限与模式", idempotency_key="root-permission")
        assert result.status == "completed" and provider.calls == 2, result
        async with runtime.repository.database.sessions() as sql:
            assert await sql.scalar(
                select(func.count()).select_from(WorkspaceDirectoryRestoreModelSourceRow)
            ) == int(allowed)
        view = await agent.directory_checkpoint_restore_by_key(request.request_key)
        if action == "preview":
            assert (view is not None) is allowed
            if allowed:
                assert view.phase == "ready" and not view.cleanup_pending
        else:
            assert view.phase == ("finished" if allowed else "ready"), view
            assert (binding.workspace.root / "tree/nested/nul").read_bytes() == (
                b"\0\xff" if allowed else b"current\x80\0"
            )
        audits = await runtime.repository.audits(result.run_id)
        assert any(item.call_id == provider.call_id and item.ok is allowed for item in audits)
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_leaf_deny_keeps_original_source_but_zero_filesystem_effect(
    runtime, tmp_path, monkeypatch, action
):
    leaf = DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]
    deny = []

    async def policy(definition, _context):
        return "deny" if deny and definition.name == leaf else "allow"

    provider = OneShotProvider(PREVIEW if action == "preview" else START)
    service, agent, binding = await setup(runtime, tmp_path, provider, policy=policy)
    try:
        captured = await target(agent)
        request = DirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=captured.checkpoint_id),
            request_key="leaf-model-preview",
        )
        if action == "start":
            planned = await agent.preview_directory_checkpoint_restore(request)
            planned = await agent.wait_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            assert planned.phase == "ready", planned
            provider.arguments = DirectoryRestoreExecute(
                plan_id=planned.plan_id,
                expected_revision=planned.revision,
                request_key="leaf-execute",
            ).model_dump()
        else:
            provider.arguments = request.model_dump()
        deny.append(True)

        def forbidden(*_args, **_kwargs):
            pytest.fail("叶拒绝后不得生产完整树")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
        result = await agent.execute("原叶拒绝", idempotency_key="leaf-deny")
        assert result.status == "completed", result
        view = await agent.directory_checkpoint_restore_by_key(request.request_key)
        assert (
            view.phase == "failed"
            and view.error_code == "permission_denied"
            and not view.cleanup_pending
        ), view
        assert json.loads(provider.reply)["plan_id"] == view.plan_id
        async with runtime.repository.database.sessions() as sql:
            assert (
                await sql.scalar(
                    select(func.count()).select_from(WorkspaceDirectoryRestoreModelSourceRow)
                )
                == 1
            )
        assert not list(binding.workspace.root.glob("._krim-tx-*"))
    finally:
        await service.close()
