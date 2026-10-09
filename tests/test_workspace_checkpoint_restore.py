"""原生检查点恢复的真实文件闭环、权限链和取消/核实边界。"""

import asyncio
import sys
import time
from threading import Event

import pytest
from sqlalchemy import update
from test_workspace_changes import action, setup, wait_event
from test_workspace_checkpoint_capture import captured, file_identity
from test_workspace_effect_chain import apply_content

from krim_agent.persistence.database import WorkspaceRestorePlanRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import CheckpointRestoreExecute, CheckpointRestorePreview
from krim_agent.tools import ToolError
from krim_agent.tools.core import ShortCircuit
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def restore_preview(session, checkpoint, key="restore-preview", **kwargs):
    result = await session.preview_file_checkpoint_restore(
        CheckpointRestorePreview(checkpoint_id=checkpoint.checkpoint_id, request_key=key, **kwargs)
    )
    return await session.wait_file_checkpoint_restore(result.plan_id, timeout_seconds=10)


async def restore_execute(session, plan, key="restore-execute", *, confirmed=False):
    result = await session.execute_file_checkpoint_restore(
        CheckpointRestoreExecute(
            plan_id=plan.plan_id, expected_revision=plan.revision, request_key=key
        ),
        confirmed=confirmed,
    )
    return await session.wait_file_checkpoint_restore(result.plan_id, timeout_seconds=10)


async def test_sdk_checkpoint_restore_twice_keeps_backups_and_can_undo(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("A\n")
    file.chmod(0o640)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        await apply_content(session, file, "B\n", "to-b")
        first = await restore_preview(session, checkpoint)
        assert first.phase == "ready" and first.revision == 1, first
        assert (
            first.items[0].proposal.preview.diff
            == "--- a/file.txt\n+++ b/file.txt\n@@ -1,1 +1,1 @@\n-B\n+A\n"
        )
        backup = await service.workspace_checkpoint_store.captures(
            first.scope_id, first.baseline_checkpoint_id
        )
        assert backup[0].snapshot.data == b"B\n"
        done = await restore_execute(session, first)
        assert done.status == "done" and file.read_text() == "A\n", done
        assert (await restore_execute(session, first)) == done
        with pytest.raises(ServiceError) as twice:
            await restore_execute(session, first, key="different")
        assert twice.value.code == "workspace_restore_execute_conflict"
        assert (
            await session.file_checkpoint_restore_by_key(first.request_key)
        ).plan_id == first.plan_id
        assert (await session.file_checkpoint_restores())[0].plan_id == first.plan_id
        original = done.items[0].proposal
        kept = await session.change_file(action(original, "keep", "keep-restore"))
        assert kept.status == "done"
        undone = await session.change_file(
            action(await session.file_change(original.proposal_id), "undo", "undo-restore")
        )
        assert undone.status == "done" and file.read_text() == "B\n"
        await apply_content(session, file, "C\n", "to-c")
        second = await restore_preview(session, checkpoint, key="second-preview")
        done_again = await restore_execute(session, second, key="second-execute")
        assert done_again.status == "done" and file.read_text() == "A\n", done_again
        assert (
            second.plan_id != first.plan_id
            and second.baseline_checkpoint_id != first.baseline_checkpoint_id
        )
        assert (await restore_preview(session, checkpoint)).plan_id == first.plan_id
        # 原计划回执保留应用成功事实，后续撤销不会把它改写成另一次恢复。
        assert (await session.file_checkpoint_restore(first.plan_id)).items[
            0
        ].operation == done.items[0].operation
    finally:
        await service.close()


@pytest.mark.parametrize(
    "target,current", [(None, None), (None, ""), ("", None), ("", "B"), ("A", None), ("A", "A")]
)
async def test_restore_existence_empty_and_noop(runtime, tmp_path, target, current):
    file = tmp_path / "file.txt"
    if target is not None:
        file.write_text(target)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        if current is None:
            file.unlink(missing_ok=True)
        else:
            file.write_text(current)
        before = file_identity(file) if file.exists() else None
        plan = await restore_preview(session, checkpoint)
        done = await restore_execute(session, plan)
        assert done.status == "done", done
        assert file.exists() == (target is not None)
        if target is not None:
            assert file.read_text() == target
        if target == current:
            assert not done.items[0].operation.receipt.changed
            assert (file_identity(file) if file.exists() else None) == before
        undone = await session.change_file(action(done.items[0].proposal, "undo", "undo"))
        assert undone.status == "done"
        assert (file.read_text() if file.exists() else None) == current
    finally:
        await service.close()


async def test_large_restore_uses_private_snapshot_and_preserves_modes(runtime, tmp_path):
    file = tmp_path / "large.txt"
    content = "完整内容\n" * 20000
    file.write_text(content)
    file.chmod(0o640)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("short")
        file.chmod(0o600)
        plan = await restore_preview(session, checkpoint)
        assert plan.items[0].current_mode == 0o600 and plan.items[0].target_mode == 0o640
        assert plan.items[0].proposal.preview.truncated
        done = await restore_execute(session, plan)
        assert done.status == "done" and file.read_text() == content, done
        assert file.stat().st_mode & 0o777 == 0o640
        operation = await service.workspace_edits.operation(
            done.scope_id, done.items[0].operation.operation_id
        )
        assert len(operation.prepared.spec_json) < 512
        assert len(done.items[0].operation.model_dump_json().encode()) < 32768
        assert str(tmp_path) not in done.model_dump_json() and content not in done.model_dump_json()
        undone = await session.change_file(action(done.items[0].proposal, "undo", "undo"))
        assert undone.status == "done" and file.read_text() == "short"
        assert file.stat().st_mode & 0o777 == 0o600
    finally:
        await service.close()


async def test_same_content_mode_restore_is_not_a_noop_or_false_reconcile(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("same")
    file.chmod(0o640)
    service, session, binding = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        file.chmod(0o600)
        plan = await restore_preview(session, checkpoint)
        prepared = (
            await service.workspace_edits.get(plan.scope_id, plan.items[0].proposal.proposal_id)
        ).prepared
        # 尚未发生属性恢复，不能仅凭内容相等核实为成功。
        assert (
            WorkspaceWriter(binding.workspace).verify(prepared, None).receipt.status == "rejected"
        )
        assert (
            WorkspaceWriter(binding.workspace).verify(prepared, None, keeping=True).receipt.status
            == "rejected"
        )
        done = await restore_execute(session, plan)
        assert done.status == "done" and done.items[0].operation.receipt.changed, done
        assert file.stat().st_mode & 0o777 == 0o640
        undone = await session.change_file(action(done.items[0].proposal, "undo", "undo"))
        assert undone.status == "done" and file.stat().st_mode & 0o777 == 0o600
    finally:
        await service.close()


@pytest.mark.parametrize("when", ["target", "current"])
async def test_restore_rejects_special_modes(runtime, tmp_path, when):
    file = tmp_path / "file.txt"
    file.write_text("content")
    file.chmod(0o2640 if when == "target" else 0o640)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        file.chmod(0o2640 if when == "current" else 0o640)
        plan = await restore_preview(session, checkpoint)
        assert plan.phase == "failed" and plan.error_code == "workspace_checkpoint_mode", plan
        assert not await session.file_changes()
    finally:
        await service.close()


@pytest.mark.parametrize("conflict", ["external", "noop", "parent"])
async def test_preview_frozen_current_refuses_later_changes(runtime, tmp_path, conflict):
    root = tmp_path / "sub"
    root.mkdir()
    file = root / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, ["sub/file.txt"])
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        if conflict == "external":
            file.write_text("outside")
        elif conflict == "parent":
            root.rename(tmp_path / "old")
            root.mkdir()
            file.write_text("B")
        else:
            from hashlib import sha256

            from krim_agent.sdk import FileChangeSpec

            proposal = await session.preview_file_change(
                FileChangeSpec(
                    path="sub/file.txt",
                    operation="write",
                    expected_sha256=sha256(b"B").hexdigest(),
                    content="B",
                ),
                request_key="noop-preview",
            )
            result = await session.change_file(action(proposal))
            assert result.status == "done" and not result.receipt.changed
        done = await restore_execute(session, plan)
        assert done.status == "failed", done
        assert done.items[0].status == "rejected"
        if conflict == "noop":
            assert done.items[0].error_code == "workspace_restore_version_conflict"
        assert file.read_text() == ("outside" if conflict == "external" else "B")
    finally:
        await service.close()


@pytest.mark.parametrize(
    "decision,confirmed,expected",
    [("deny", True, "permission_denied"), ("ask", False, "approval_required"), ("ask", True, None)],
)
async def test_restore_uses_original_write_policy(runtime, tmp_path, decision, confirmed, expected):
    file = tmp_path / "file.txt"
    file.write_text("A")

    async def policy(definition, _context):
        return decision if definition.name == "workspace_change" else "allow"

    service, session, _ = await setup(runtime, tmp_path, policy=policy)
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        done = await restore_execute(session, plan, confirmed=confirmed)
        assert done.items[0].error_code == expected, done
        assert file.read_text() == ("A" if expected is None else "B")
        assert any(
            item.name == "workspace_change"
            and item.decision == ("allow" if expected is None else decision)
            for item in service._handle(session.agent_id).runtime.tools.audit
        )
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["shortcircuit", "key", "proposal", "action", "nested"])
async def test_hook_cannot_forge_or_redirect_restore(runtime, tmp_path, fault):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        with pytest.raises(ServiceError) as bypass:
            await session.change_file(action(plan.items[0].proposal))
        assert bypass.value.code == "workspace_restore_required"

        async def hook(_definition, context):
            if context.checkpoint_restore:
                if fault == "shortcircuit":
                    return ShortCircuit({"forged": True})
                if fault == "nested":
                    try:
                        await context.call_tool("workspace_finalize", {"operation_id": "fake"})
                    except ToolError as error:
                        assert error.code == "workspace_restore_action"
                        raise
                field, value = {
                    "key": ("request_key", "other"),
                    "proposal": ("proposal_id", "other"),
                    "action": ("action", "undo"),
                }.get(fault, ("request_key", "other"))
                context.arguments = {**context.arguments, field: value}

        service._handle(session.agent_id).runtime.tools.add_global_pre(hook)
        done = await restore_execute(session, plan)
        assert done.status == "failed" and done.items[0].operation is None, done
        assert file.read_text() == "B"
    finally:
        await service.close()


async def test_multifile_partial_and_same_key_does_not_replay(runtime, tmp_path):
    paths = [tmp_path / "a.txt", tmp_path / "b.txt"]
    for file in paths:
        file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [item.name for item in paths])
        for file in paths:
            file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        paths[1].write_text("outside")
        done = await restore_execute(session, plan)
        assert done.status == "partial" and [item.status for item in done.items] == [
            "done",
            "rejected",
        ], done
        paths[0].write_text("later")
        repeated = await restore_execute(session, plan)
        assert repeated == done and paths[0].read_text() == "later"
    finally:
        await service.close()


@pytest.mark.parametrize("stop", ["cancel", "expire"])
async def test_restore_fences_writer_waiting_before_file_lock(runtime, tmp_path, monkeypatch, stop):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.apply

    def pause(self, *args, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(self, *args, **kwargs)

    cancellation = None
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        monkeypatch.setattr(WorkspaceWriter, "apply", pause)
        await session.execute_file_checkpoint_restore(
            CheckpointRestoreExecute(
                plan_id=plan.plan_id, expected_revision=plan.revision, request_key="execute"
            )
        )
        await wait_event(entered)
        if stop == "cancel":
            await service.workspace_restore_store.cancel(plan.scope_id, plan.plan_id)
            cancellation = asyncio.create_task(session.cancel_file_checkpoint_restore(plan.plan_id))
        else:
            async with (
                service.workspace_edits.database.write_lock,
                service.workspace_edits.database.sessions.begin() as sql,
            ):
                await sql.execute(
                    update(WorkspaceRestorePlanRow)
                    .where(WorkspaceRestorePlanRow.plan_id == plan.plan_id)
                    .values(expires_at=time.time() - 1)
                )
            assert (await session.file_checkpoint_restore(plan.plan_id)).phase == "expired"
        release.set()
        if cancellation:
            await cancellation
        await asyncio.gather(*(value[1] for value in service.workspace_restores._tasks.values()))
        done = await session.file_checkpoint_restore(plan.plan_id)
        assert done.phase == ("cancelled" if stop == "cancel" else "expired"), done
        assert file.read_text() == "B" and done.items[0].operation.status == "rejected"
    finally:
        release.set()
        if cancellation:
            await asyncio.gather(cancellation, return_exceptions=True)
        await service.close()


async def test_cancel_preparation_stops_baseline_and_hook_self_cancel_does_not_deadlock(
    runtime, tmp_path
):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        entered, release = asyncio.Event(), asyncio.Event()

        async def hook(_definition, context):
            if context.checkpoint_capture:
                entered.set()
                await release.wait()
            if context.checkpoint_restore:
                await session.cancel_file_checkpoint_restore(context.checkpoint_restore.plan_id)

        service._handle(session.agent_id).runtime.tools.add_global_pre(hook)
        plan = await session.preview_file_checkpoint_restore(
            CheckpointRestorePreview(
                checkpoint_id=checkpoint.checkpoint_id, request_key="cancel-prepare"
            )
        )
        await asyncio.wait_for(entered.wait(), 5)
        cancelled = await asyncio.wait_for(session.cancel_file_checkpoint_restore(plan.plan_id), 5)
        assert cancelled.phase == "cancelled"
        assert (
            await session.file_checkpoint(cancelled.baseline_checkpoint_id)
        ).status == "cancelled"
        release.set()
        second = await restore_preview(session, checkpoint, key="execute-cancel")
        done = await asyncio.wait_for(restore_execute(session, second), 5)
        assert done.phase == "cancelled" and done.items[0].operation is None
        assert file.read_text() == "A"
    finally:
        await service.close()


async def test_receipt_save_failure_keeps_real_operation_for_reconcile(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        original = service.workspace_edits.settle
        calls = 0

        async def fail_first(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("simulated receipt store unavailable")
            return await original(*args, **kwargs)

        monkeypatch.setattr(service.workspace_edits, "settle", fail_first)
        done = await restore_execute(session, plan)
        assert done.status == "uncertain" and file.read_text() == "A", done
        operation = done.items[0].operation
        assert operation.receipt.error_code == "workspace_receipt_pending"
        assert (await restore_execute(session, plan)).items[0].operation == operation
        reconciled = await session.reconcile_file_change(operation.operation_id)
        assert reconciled.status == "done"
        refreshed = await session.file_checkpoint_restore(plan.plan_id)
        assert (
            refreshed.status == "done"
            and refreshed.items[0].operation.operation_id == operation.operation_id
        )
    finally:
        await service.close()


async def test_cancel_after_publication_preserves_real_receipt_and_close_guard(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("A")
    service, session, _ = await setup(runtime, tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    cancellation = None
    original = service.workspace_edits.settle

    async def pause_save(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    async def close_hook(_definition, context):
        if context.checkpoint_restore:
            with pytest.raises(ServiceError) as error:
                await service.close()
            assert error.value.code == "workspace_batch_close_reentrant"
            assert not service._closed

    try:
        checkpoint = await captured(session, [file.name])
        file.write_text("B")
        plan = await restore_preview(session, checkpoint)
        monkeypatch.setattr(service.workspace_edits, "settle", pause_save)
        service._handle(session.agent_id).runtime.tools.add_global_pre(close_hook)
        await session.execute_file_checkpoint_restore(
            CheckpointRestoreExecute(
                plan_id=plan.plan_id, expected_revision=plan.revision, request_key="execute"
            )
        )
        await asyncio.wait_for(entered.wait(), 5)
        assert file.read_text() == "A"
        await service.workspace_restore_store.cancel(plan.scope_id, plan.plan_id)
        cancellation = asyncio.create_task(session.cancel_file_checkpoint_restore(plan.plan_id))
        await asyncio.sleep(0)
        assert not cancellation.done()
        release.set()
        done = await asyncio.wait_for(cancellation, 5)
        assert done.phase == "cancelled" and done.status == "done", done
        assert done.items[0].operation.receipt.changed and file.read_text() == "A"
    finally:
        release.set()
        if cancellation:
            await asyncio.gather(cancellation, return_exceptions=True)
        await service.close()
