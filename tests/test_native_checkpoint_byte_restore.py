"""公开bytes捕获到原恢复/备份回退的真实闭环与格式资格。"""

import asyncio
import json
from pathlib import Path

import pytest
from sqlalchemy import func, select
from test_native_checkpoint_byte_capture import capture, setup
from test_workspace_changes import action
from test_workspace_changes import setup as old_setup
from test_workspace_checkpoint_restore import restore_execute as old_restore_execute
from test_workspace_checkpoint_restore import restore_preview as old_restore_preview

from krim_agent.persistence.database import (
    NativeWorkspaceCheckpointFileRow,
    WorkspaceEditAttemptRow,
    WorkspaceRestorePlanRow,
)
from krim_agent.persistence.workspace_edits import digest
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import (
    CheckpointRestorePreview,
    CheckpointRestoreView,
    WorkspaceCheckpointCreate,
)
from krim_agent.workspace_writer import WorkspaceWriter

TOOLS = [
    "workspace_read",
    "workspace_checkpoint_capture",
    "workspace_change",
    "workspace_reconcile",
]


async def mixed_setup(runtime, root, **kwargs):
    return await setup(runtime, root, names=TOOLS, **kwargs)


async def restore_preview(agent, checkpoint, **kwargs):
    plan = await old_restore_preview(agent, checkpoint, **kwargs)
    if plan.phase == "preparing":
        plan = await agent.wait_file_checkpoint_restore(plan.plan_id, timeout_seconds=30)
    return plan


async def restore_execute(agent, plan, **kwargs):
    result = await old_restore_execute(agent, plan, **kwargs)
    if result.phase == "executing":
        result = await agent.wait_file_checkpoint_restore(result.plan_id, timeout_seconds=30)
    return result


def write_files(root, values):
    for name, value in values.items():
        path = root / name
        if value is None:
            path.unlink(missing_ok=True)
        else:
            path.write_bytes(value)


def read_files(root, names):
    return {name: (root / name).read_bytes() if (root / name).exists() else None for name in names}


def test_actual_published_restore_vectors_keep_old_default_json_and_hash():
    original = json.loads(
        (Path(__file__).parent / "fixtures/native_restore_legacy_vectors.json").read_text()
    )
    request = CheckpointRestorePreview.model_validate(original["request"])
    assert request.model_dump(mode="json") == original["request"]
    assert (
        CheckpointRestoreView.model_validate_json(json.dumps(original["view"])).model_dump(
            mode="json"
        )
        == (original["view"])
    )
    assert (
        digest(
            {
                "request": request.model_dump(mode="json"),
                "key": "files",
                "resource": "a" * 64,
                "workspace": "b" * 64,
            }
        )
        == original["request_hash"]
    )


async def test_real_mixed_restore_and_complete_baseline_rollback_preserve_all_bytes_modes(
    runtime, tmp_path
):
    target = {"nul": b"\0\xff", "utf": b"hello\n", "invalid": b"\xff", "empty": b"", "absent": None}
    current = {
        "nul": b"current\n",
        "utf": b"\0current",
        "invalid": None,
        "empty": b"\xff\0",
        "absent": b"new\xff",
    }
    write_files(tmp_path, target)
    (tmp_path / "nul").chmod(0o640)
    service, agent, _ = await mixed_setup(runtime, tmp_path)
    try:
        _, checkpoint = await capture(agent, list(target))
        write_files(tmp_path, current)
        (tmp_path / "nul").chmod(0o600)
        plan = await restore_preview(agent, checkpoint)
        assert plan.phase == "ready" and plan.baseline_capture_format == "bytes", plan
        backup = await agent.file_checkpoint(plan.baseline_checkpoint_id)
        assert backup.capture_format == "bytes"
        snapshots = await service.workspace_checkpoint_store.captures(
            plan.scope_id, backup.checkpoint_id
        )
        assert [item.snapshot.data if item.snapshot else None for item in snapshots] == list(
            current.values()
        )
        assert "binary" in plan.items[0].proposal.preview.diff
        assert "sha256=" in plan.items[0].proposal.preview.diff
        done = await restore_execute(agent, plan)
        assert done.status == "done" and read_files(tmp_path, target) == target, done
        assert (tmp_path / "nul").stat().st_mode & 0o777 == 0o640
        rollback = await restore_preview(agent, backup, key="backup-preview")
        restored = await restore_execute(agent, rollback, key="backup-execute")
        assert restored.status == "done" and read_files(tmp_path, target) == current, restored
        assert (tmp_path / "nul").stat().st_mode & 0o777 == 0o600
        assert await agent.file_checkpoint_restore_by_key(plan.request_key) == done
        assert not any(value in done.model_dump_json() for value in (str(tmp_path), "current\\n"))
    finally:
        await service.close()


async def test_text_target_current_binary_requires_explicit_bytes_and_backup_can_restore(
    runtime, tmp_path
):
    file = tmp_path / "file"
    file.write_text("original\n")
    service, agent, _ = await mixed_setup(runtime, tmp_path)
    try:
        accepted = await agent.create_file_checkpoint(
            WorkspaceCheckpointCreate(paths=["file"], request_key="text")
        )
        checkpoint = await agent.wait_file_checkpoint(accepted.checkpoint_id)
        file.write_bytes(b"\0\xffcurrent")
        denied = await restore_preview(agent, checkpoint, key="default-text")
        assert denied.phase == "failed" and denied.baseline_capture_format == "text", denied
        assert file.read_bytes() == b"\0\xffcurrent"
        plan = await restore_preview(
            agent, checkpoint, key="explicit-bytes", baseline_capture_format="bytes"
        )
        assert plan.phase == "ready" and plan.baseline_capture_format == "bytes", plan
        done = await restore_execute(agent, plan)
        assert done.status == "done" and file.read_bytes() == b"original\n", done
        backup = await agent.file_checkpoint(plan.baseline_checkpoint_id)
        rollback = await restore_preview(agent, backup, key="rollback")
        assert (await restore_execute(agent, rollback, key="rollback-execute")).status == "done"
        assert file.read_bytes() == b"\0\xffcurrent"
        with pytest.raises(ServiceError) as conflict:
            await restore_preview(agent, checkpoint, key="explicit-bytes")
        assert conflict.value.code == "workspace_restore_key_conflict"
    finally:
        await service.close()


@pytest.mark.parametrize("target_format", ["text", "bytes"])
async def test_missing_new_frozen_tool_blocks_required_byte_baseline_before_plan_row(
    runtime, tmp_path, monkeypatch, target_format
):
    file = tmp_path / "file"
    file.write_bytes(b"target")
    service, agent, binding = await old_setup(runtime, tmp_path)
    store = service.workspace_checkpoint_store
    try:
        view, _ = await store.reserve(
            f"session:{agent.session_id}",
            WorkspaceCheckpointCreate(
                paths=["file"], request_key="trusted", capture_format=target_format
            ),
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
        )
        fence = (await store.execution(view.scope_id, view.checkpoint_id))[0]
        await store.record(
            view.scope_id,
            fence,
            WorkspaceWriter(binding.workspace).capture_file("file", allow_binary=True),
        )
        await store.finish(view.scope_id, fence)

        def no_io(*_args, **_kwargs):
            pytest.fail("缺少原新冻结工具不得读取文件")

        monkeypatch.setattr(WorkspaceWriter, "capture_file", no_io)
        with pytest.raises(ServiceError) as denied:
            await agent.preview_file_checkpoint_restore(
                CheckpointRestorePreview(
                    checkpoint_id=view.checkpoint_id,
                    request_key="missing-tool",
                    baseline_capture_format="bytes" if target_format == "text" else "text",
                )
            )
        assert denied.value.code == "workspace_checkpoint_bytes_unavailable"
        async with runtime.repository.database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(WorkspaceRestorePlanRow)) == 0
    finally:
        await service.close()


async def reserved_backup(service, agent, binding, checkpoint, *, backup_format="bytes"):
    store = service.workspace_restore_store
    plan, _ = await store.reserve(
        checkpoint.scope_id,
        CheckpointRestorePreview(checkpoint_id=checkpoint.checkpoint_id, request_key="reserved"),
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
    )
    preparation = await store.preparation(plan.scope_id, plan.plan_id)
    accepted = await agent.create_file_checkpoint(
        WorkspaceCheckpointCreate(
            paths=["file"], request_key=preparation.baseline_key, capture_format=backup_format
        )
    )
    backup = await agent.wait_file_checkpoint(accepted.checkpoint_id)
    return preparation, backup


async def test_baseline_same_key_wrong_format_is_never_attached(runtime, tmp_path):
    (tmp_path / "file").write_bytes(b"\0target")
    service, agent, binding = await mixed_setup(runtime, tmp_path)
    try:
        _, checkpoint = await capture(agent, ["file"])
        (tmp_path / "file").write_text("current")
        prep, backup = await reserved_backup(
            service, agent, binding, checkpoint, backup_format="text"
        )
        with pytest.raises(ServiceError) as rejected:
            await service.workspace_restore_store.attach_baseline(
                checkpoint.scope_id, prep.view.plan_id, prep.token, backup.checkpoint_id
            )
        assert rejected.value.code == "workspace_restore_baseline"
        assert (
            await agent.file_checkpoint_restore(prep.view.plan_id)
        ).baseline_checkpoint_id is None
    finally:
        await service.close()


async def test_ready_rejects_valid_digest_without_original_byte_kind(runtime, tmp_path):
    (tmp_path / "file").write_bytes(b"\0target")
    service, agent, binding = await mixed_setup(runtime, tmp_path)
    store = service.workspace_restore_store
    try:
        _, checkpoint = await capture(agent, ["file"])
        (tmp_path / "file").write_bytes(b"current\xff")
        prep, backup = await reserved_backup(service, agent, binding, checkpoint)
        await store.attach_baseline(
            checkpoint.scope_id, prep.view.plan_id, prep.token, backup.checkpoint_id
        )
        old = (
            await service.workspace_checkpoint_store.captures(
                checkpoint.scope_id, backup.checkpoint_id
            )
        )[0]
        target = (
            await service.workspace_checkpoint_store.captures(
                checkpoint.scope_id, checkpoint.checkpoint_id
            )
        )[0]
        prepared = WorkspaceWriter(binding.workspace).prepare_checkpoint_restore(old, target)
        async with store.database.write_lock, store.database.sessions.begin() as sql:
            row = await sql.get(NativeWorkspaceCheckpointFileRow, (checkpoint.checkpoint_id, 0))
            metadata = dict(row.capture_metadata)
            metadata.pop("content_kind")
            metadata["snapshot_hash"] = digest(
                {key: value for key, value in metadata.items() if key != "snapshot_hash"}
            )
            row.capture_metadata = metadata
        with pytest.raises(ServiceError) as rejected:
            await store.ready(checkpoint.scope_id, prep.view.plan_id, prep.token, (prepared,))
        assert rejected.value.code == "workspace_checkpoint_corrupt"
        assert (tmp_path / "file").read_bytes() == b"current\xff"
        assert not await agent.file_changes()
    finally:
        await service.close()


async def test_byte_restore_external_change_and_unknown_use_original_operation(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file"
    file.write_bytes(b"\0target")
    service, agent, _ = await mixed_setup(runtime, tmp_path)
    try:
        _, checkpoint = await capture(agent, ["file"])
        file.write_bytes(b"before\xff")
        stale = await restore_preview(agent, checkpoint)
        file.write_bytes(b"outside\0")
        rejected = await restore_execute(agent, stale)
        assert rejected.status == "failed" and file.read_bytes() == b"outside\0", rejected
        plan = await restore_preview(agent, checkpoint, key="unknown-preview")
        settle = service.workspace_edits.settle

        async def unavailable(*_args, **_kwargs):
            raise RuntimeError("synthetic receipt persistence failure")

        monkeypatch.setattr(service.workspace_edits, "settle", unavailable)
        unknown = await restore_execute(agent, plan, key="unknown-execute")
        assert unknown.status == "uncertain" and file.read_bytes() == b"\0target", unknown
        operation = unknown.items[0].operation
        monkeypatch.setattr(service.workspace_edits, "settle", settle)
        assert (await restore_execute(agent, plan, key="unknown-execute")).items[
            0
        ].operation == operation
        with pytest.raises(ServiceError) as active:
            await agent.reconcile_file_change(operation.operation_id)
        assert active.value.code == "workspace_operation_active"
        # 持续落盘故障保留running原租约；仅在测试里受控到期，不能接管活跃owner。
        database = runtime.repository.database
        async with database.write_lock, database.sessions.begin() as sql:
            row = await sql.get(WorkspaceEditAttemptRow, operation.operation_id)
            row.lease_expires_at = 0
        reconciled = await agent.reconcile_file_change(operation.operation_id)
        assert reconciled.status == "done"
        refreshed = await agent.file_checkpoint_restore(plan.plan_id)
        assert (
            refreshed.status == "done"
            and refreshed.items[0].operation.operation_id == operation.operation_id
        )
        undone = await agent.change_file(action(refreshed.items[0].proposal, "undo", "undo-bytes"))
        assert undone.status == "done" and file.read_bytes() == b"outside\0", undone
    finally:
        await service.close()


async def test_cancel_byte_restore_during_backup_closes_original_capture(runtime, tmp_path):
    file = tmp_path / "file"
    file.write_bytes(b"\0target")
    service, agent, _ = await mixed_setup(runtime, tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    try:
        _, checkpoint = await capture(agent, ["file"])
        file.write_bytes(b"current\xff")

        async def hold(definition, context):
            if definition.name == "workspace_checkpoint_capture" and context.checkpoint_capture:
                entered.set()
                await release.wait()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
        started = await agent.preview_file_checkpoint_restore(
            CheckpointRestorePreview(checkpoint_id=checkpoint.checkpoint_id, request_key="cancel")
        )
        await asyncio.wait_for(entered.wait(), 5)
        cancelled = await agent.cancel_file_checkpoint_restore(started.plan_id)
        assert cancelled.phase == "cancelled" and cancelled.baseline_capture_format == "bytes"
        backup = await agent.file_checkpoint(cancelled.baseline_checkpoint_id)
        assert backup.status == "cancelled" and not backup.files[0].captured
        assert not await agent.file_changes() and file.read_bytes() == b"current\xff"
    finally:
        release.set()
        await service.close()


@pytest.mark.parametrize(
    "mode,decision,confirmed,error",
    [
        ("agent", "deny", True, "permission_denied"),
        ("agent", "ask", False, "approval_required"),
        ("agent", "ask", True, None),
        ("plan", "allow", True, "permission_denied"),
    ],
)
async def test_byte_restore_keeps_original_write_approval_and_plan_boundary(
    runtime, tmp_path, mode, decision, confirmed, error
):
    file = tmp_path / "file"
    file.write_bytes(b"\0target")

    async def policy(definition, _context):
        return decision if definition.name == "workspace_change" else "allow"

    service, agent, _ = await mixed_setup(runtime, tmp_path, policy=policy, mode=mode)
    try:
        _, checkpoint = await capture(agent, ["file"])
        file.write_bytes(b"before\xff")
        plan = await restore_preview(agent, checkpoint)
        assert plan.phase == "ready" and plan.baseline_capture_format == "bytes", plan
        done = await restore_execute(agent, plan, confirmed=confirmed)
        assert done.items[0].error_code == error, done
        assert file.read_bytes() == (b"\0target" if error is None else b"before\xff")
    finally:
        await service.close()
