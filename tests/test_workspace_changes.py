"""真实AgentSession的文件提案、权限、取消、故障回执与恢复。"""

import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import replace
from pathlib import Path
from threading import Event

import pytest
from sqlalchemy import update

from krim_agent.agents import AgentError, AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.interactions import InteractionReply
from krim_agent.persistence.database import WorkspaceEditAttemptRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, FileChangeSpec, WorkspaceChangeAction
from krim_agent.tools import ToolContext
from krim_agent.workspace import WORKSPACE_CHANGE_TOOL_NAMES
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not os.sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def allow(_definition, _context):
    return "allow"


async def ask_writes(definition, _context):
    return "allow" if definition.risk == "read" else "ask"


async def setup(runtime, root, *, policy=allow, persistent=False):
    runtime.tool_policy = policy
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace("files", root, version="v1", writable=True, default=True)
    view = await service.spawn("default", persistent=persistent)
    return service, AgentSession(service, view.agent_id), binding


def action(proposal, kind="apply", key=None):
    return WorkspaceChangeAction(
        proposal_id=proposal.proposal_id,
        action=kind,
        expected_revision=proposal.revision,
        request_key=key or kind,
    )


async def preview(session, file, content="new\n", key="preview", operation="write"):
    expected = hashlib.sha256(file.read_bytes()).hexdigest() if file.exists() else None
    return await session.preview_file_change(
        FileChangeSpec(
            path=file.name,
            operation=operation,
            expected_sha256=expected,
            content=content if operation == "write" else None,
        ),
        request_key=key,
    )


async def wait_event(event):
    assert await asyncio.to_thread(event.wait, 5), "文件线程没有进入预期阶段"


@pytest.mark.parametrize(
    "old,operation", [(None, "write"), ("old\n", "write"), ("old\n", "delete")]
)
async def test_sdk_apply_keep_undo_and_explicit_cleanup(runtime, tmp_path, old, operation):
    file = tmp_path / "file.txt"
    if old is not None:
        file.write_text(old)
        file.chmod(0o600)
    service, session, binding = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file, operation=operation)
        assert proposal.status == "proposed" and proposal.revision == 0
        assert file.exists() == (old is not None)
        applied = await session.change_file(action(proposal))
        assert applied.status == "done" and applied.run_id is None
        assert file.exists() == (operation != "delete")
        if operation != "delete":
            assert file.read_text() == "new\n"
        updated = await session.file_change(proposal.proposal_id)
        assert updated.status == "applied" and updated.revision == 2
        kept = await session.change_file(action(updated, "keep"))
        assert kept.status == "done" and not kept.receipt.changed
        updated = await session.file_change(proposal.proposal_id)
        assert updated.status == "kept" and updated.revision == 4
        undone = await session.change_file(action(updated, "undo"))
        assert undone.status == "done"
        assert file.exists() == (old is not None)
        if old is not None:
            assert file.read_text() == old
            assert file.stat().st_mode & 0o777 == 0o600
        for item in await session.file_change_history(proposal.proposal_id):
            result = await session.finalize_file_change(item.operation_id)
            assert not result.cleanup_pending
        assert not list(tmp_path.glob("._krim-tx-*"))
        assert (await session.file_changes())[0].status == "undone"
        assert binding.workspace.writable
    finally:
        await service.close()


async def test_large_private_snapshot_can_be_restored_without_expanding_tool_parameters(
    runtime, tmp_path
):
    file = tmp_path / "large.txt"
    old = "完整旧版本\n" * 12000
    file.write_text(old)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file)
        assert proposal.preview.before_bytes > 32768
        await session.change_file(action(proposal))
        undone = await session.change_file(
            action(await session.file_change(proposal.proposal_id), "undo")
        )
        assert undone.status == "done" and file.read_text() == old
        private = await service.workspace_edits.operation(
            f"session:{session.session_id}", undone.operation_id
        )
        assert private.prepared.spec.operation == "restore"
        assert len(private.prepared.spec_json) < 512
        assert len(undone.model_dump_json().encode()) < 32768
    finally:
        await service.close()


async def test_replays_do_not_reread_or_reexecute_changed_files(runtime, tmp_path, monkeypatch):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    calls = 0
    original = WorkspaceWriter.apply

    def counted(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, "apply", counted)
    try:
        spec = FileChangeSpec(
            path="file.txt",
            operation="write",
            expected_sha256=hashlib.sha256(b"old\n").hexdigest(),
            content="new\n",
        )
        proposal = await session.preview_file_change(spec, request_key="preview")
        request = action(proposal)
        first = await session.change_file(request)
        file.write_text("external\n")
        assert (
            await session.preview_file_change(spec, request_key="preview")
        ).proposal_id == proposal.proposal_id
        assert (await session.change_file(request)).operation_id == first.operation_id
        assert calls == 1 and file.read_text() == "external\n"
        with pytest.raises(ServiceError) as conflict:
            await session.change_file(request.model_copy(update={"request_key": "fresh"}))
        assert conflict.value.code in {"workspace_change_state", "workspace_revision_conflict"}
    finally:
        await service.close()


async def test_same_action_key_concurrent_calls_publish_once(runtime, tmp_path, monkeypatch):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    original = WorkspaceWriter.apply
    calls = 0

    def counted(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, "apply", counted)
    try:
        proposal = await preview(session, file)
        results = await asyncio.gather(*(session.change_file(action(proposal)) for _ in range(8)))
        assert len({item.operation_id for item in results}) == 1
        assert calls == 1 and file.read_text() == "new\n"
        assert (await session.file_change_operation(results[0].operation_id)).status == "done"
    finally:
        await service.close()


async def test_keep_and_undo_refuse_external_edits(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file)
        await session.change_file(action(proposal))
        file.write_text("external\n")
        state = await session.file_change(proposal.proposal_id)
        kept = await session.change_file(action(state, "keep"))
        assert kept.status == "rejected" and kept.receipt.error_code == "workspace_conflict"
        with pytest.raises(ServiceError) as conflict:
            await session.change_file(
                action(await session.file_change(proposal.proposal_id), "undo")
            )
        assert conflict.value.code == "workspace_conflict"
        assert file.read_text() == "external\n"
    finally:
        await service.close()


async def test_host_confirmation_respects_deny_and_hook_changes(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path, policy=ask_writes)
    try:
        proposal = await preview(session, file)
        request = action(proposal)
        with pytest.raises(ServiceError) as missing:
            await session.change_file(request)
        assert missing.value.code == "approval_required"
        handle = service._handle(session.agent_id)

        async def modify(definition, context):
            if definition.name == "workspace_change":
                context.arguments["request_key"] = "hook-replaced"

        handle.runtime.tools.add_global_pre(modify)
        with pytest.raises(ServiceError) as changed:
            await session.change_file(request, confirmed=True)
        assert changed.value.code == "permission_denied"
        assert file.read_text() == "old\n" and not await session.file_change_history(
            proposal.proposal_id
        )
        handle.runtime.tools._global_pre.clear()
        result = await session.change_file(request, confirmed=True)
        assert result.status == "done"
        assert handle.runtime.tools.audit[-1].run_id == "local"
    finally:
        await service.close()


@pytest.mark.parametrize("mode", ["ask", "plan"])
async def test_session_modes_do_not_expand_write_permission(runtime, tmp_path, mode):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="restricted",
                    description="受限会话",
                    tool_names=["workspace_read", "workspace_preview", "workspace_change"],
                    workspace_key="files",
                    mode=mode,
                ),
            )
        )
        restricted = AgentSession(service, (await service.spawn("restricted")).agent_id)
        if mode == "ask":
            with pytest.raises(ServiceError) as error:
                await preview(restricted, file)
            assert error.value.code == "permission_denied"
        else:
            proposal = await preview(restricted, file)
            with pytest.raises(ServiceError) as error:
                await restricted.change_file(action(proposal), confirmed=True)
            assert error.value.code == "permission_denied"
        assert file.read_text() == "old\n"
    finally:
        await service.close()


async def test_session_scope_tool_subset_and_parent_boundaries(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file)
        other = AgentSession(service, (await service.spawn("default")).agent_id)
        with pytest.raises(ServiceError) as foreign:
            await other.change_file(action(proposal))
        assert foreign.value.code == "workspace_proposal_not_found"
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="reader",
                    description="只读子集",
                    tool_names=["workspace_read"],
                    workspace_key="files",
                ),
            )
        )
        reader = AgentSession(service, (await service.spawn("reader")).agent_id)
        with pytest.raises(ServiceError) as unavailable:
            await preview(reader, file)
        assert unavailable.value.code == "tool_not_found"
        child_definition = replace(
            await service.definition("default"), parent_agent_id=reader.agent_id
        )
        with pytest.raises(AgentError) as expanded:
            await service.manager.spawn(child_definition)
        assert expanded.value.code == "agent_workspace_forbidden"
    finally:
        await service.close()


@pytest.mark.parametrize("published", [False, True])
async def test_cancellation_waits_for_thread_and_persists_actual_effect(
    runtime, tmp_path, monkeypatch, published
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.apply

    def blocked(self, *args, **kwargs):
        if published:
            result = original(self, *args, **kwargs)
        entered.set()
        assert release.wait(5)
        return result if published else original(self, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, "apply", blocked)
    try:
        proposal = await preview(session, file)
        task = asyncio.create_task(session.change_file(action(proposal)))
        await wait_event(entered)
        task.cancel()
        await asyncio.sleep(0.03)
        task.cancel()
        await asyncio.sleep(0.03)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        attempt = (await session.file_change_history(proposal.proposal_id))[0]
        assert attempt.status == ("done" if published else "rejected")
        assert file.read_text() == ("new\n" if published else "old\n")
        assert not service.manager.workspace_changes._operations
    finally:
        release.set()
        await service.close()


async def test_service_close_waits_before_closing_workspace(runtime, tmp_path, monkeypatch):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, binding = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.apply

    def blocked(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(WorkspaceWriter, "apply", blocked)
    try:
        proposal = await preview(session, file)
        call = asyncio.create_task(session.change_file(action(proposal)))
        await wait_event(entered)
        closing = asyncio.create_task(service.close())
        await asyncio.sleep(0.03)
        assert not closing.done() and not binding.workspace.closed
        release.set()
        await asyncio.gather(call, closing, return_exceptions=True)
        attempt = (await service.workspace_history(session.agent_id, proposal.proposal_id))[0]
        assert attempt.status == "done" and binding.workspace.closed
        assert file.read_text() == "new\n"
    finally:
        release.set()
        await service.close()


@pytest.mark.parametrize("after_commit", [False, True])
async def test_receipt_save_failure_reads_durable_result_or_marks_uncertain(
    runtime, tmp_path, monkeypatch, after_commit
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    original = service.workspace_edits.settle
    failed = False

    async def fault(*args, **kwargs):
        nonlocal failed
        if not failed:
            failed = True
            if after_commit:
                await original(*args, **kwargs)
            raise OSError("injected receipt save failure")
        return await original(*args, **kwargs)

    monkeypatch.setattr(service.workspace_edits, "settle", fault)
    try:
        proposal = await preview(session, file)
        request = action(proposal)
        result = await session.change_file(request)
        assert result.status == ("done" if after_commit else "uncertain")
        assert result.receipt.before_sha256 == proposal.preview.before_sha256
        assert file.read_text() == "new\n"
        assert list(tmp_path.glob("._krim-tx-*"))
        assert (await session.change_file(request)).operation_id == result.operation_id
        resolved = await session.reconcile_file_change(result.operation_id)
        assert resolved.status == "done" and resolved.cleanup_pending
        await session.finalize_file_change(resolved.operation_id)
        assert not list(tmp_path.glob("._krim-tx-*"))
    finally:
        await service.close()


async def test_total_receipt_failure_keeps_running_and_material_until_expired_reconciliation(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    original = service.workspace_edits.settle

    async def broken(*_args, **_kwargs):
        raise OSError("storage unavailable")

    monkeypatch.setattr(service.workspace_edits, "settle", broken)
    try:
        proposal = await preview(session, file)
        with pytest.raises(ServiceError) as error:
            await session.change_file(action(proposal))
        assert error.value.code == "workspace_receipt_pending"
        attempt = (await session.file_change_history(proposal.proposal_id))[0]
        assert attempt.status == "running" and attempt.receipt is None
        assert file.read_text() == "new\n" and list(tmp_path.glob("._krim-tx-*"))
        with pytest.raises(ServiceError) as active:
            await session.reconcile_file_change(attempt.operation_id)
        assert active.value.code == "workspace_operation_active"
        monkeypatch.setattr(service.workspace_edits, "settle", original)
        async with runtime.repository.database.sessions.begin() as db:
            await db.execute(
                update(WorkspaceEditAttemptRow)
                .where(WorkspaceEditAttemptRow.operation_id == attempt.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        assert (await session.reconcile_file_change(attempt.operation_id)).status == "done"
    finally:
        await service.close()


async def test_rejected_expired_attempt_is_confirmed_without_file_action(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, binding = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file)
        scope = f"session:{session.session_id}"
        prepared = (await service.workspace_edits.get(scope, proposal.proposal_id)).prepared
        attempt, _ = await service.workspace_edits.reserve(
            scope, action(proposal), binding.fingerprint, prepared
        )
        async with runtime.repository.database.sessions.begin() as db:
            await db.execute(
                update(WorkspaceEditAttemptRow)
                .where(WorkspaceEditAttemptRow.operation_id == attempt.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        result = await session.reconcile_file_change(attempt.view.operation_id)
        assert result.status == "rejected" and not result.cleanup_pending
        assert file.read_text() == "old\n"
    finally:
        await service.close()


async def test_cleanup_mark_failure_can_be_retried_after_material_was_removed(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    original = service.workspace_edits.mark_cleaned
    failed = False

    async def fault(*args, **kwargs):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("cleanup marker save failed")
        return await original(*args, **kwargs)

    monkeypatch.setattr(service.workspace_edits, "mark_cleaned", fault)
    try:
        proposal = await preview(session, file)
        result = await session.change_file(action(proposal))
        with pytest.raises(ServiceError):
            await session.finalize_file_change(result.operation_id)
        assert not list(tmp_path.glob("._krim-tx-*"))
        assert (await session.file_change_operation(result.operation_id)).cleanup_pending
        assert not (await session.finalize_file_change(result.operation_id)).cleanup_pending
        assert file.read_text() == "new\n"
    finally:
        await service.close()


async def test_closed_session_history_survives_and_new_actions_are_blocked(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file)
        result = await session.change_file(action(proposal))
        await session.close()
        assert (await session.file_change_operation(result.operation_id)).status == "done"
        assert (await session.file_changes())[0].status == "applied"
        with pytest.raises(ServiceError) as closed:
            await session.finalize_file_change(result.operation_id)
        assert closed.value.code == "agent_session_closed"
        await session.reopen()
        assert not (await session.finalize_file_change(result.operation_id)).cleanup_pending
    finally:
        await service.close()


class FileAgentProvider:
    name = "offline-file-agent"

    async def stream(self, messages, tools):
        assert "workspace_change" in {item.name for item in tools}
        results = [message for message in messages if message.role == "tool"]
        if not results:
            yield ModelToolCall(
                id="proposal-call",
                name="workspace_preview",
                arguments={
                    "spec": {
                        "path": "model.txt",
                        "operation": "write",
                        "expected_sha256": None,
                        "content": "模型修改\n",
                    },
                    "request_key": "model-preview",
                },
            )
        elif results[-1].details.get("name") == "workspace_preview":
            proposal = json.loads(results[-1].content)
            yield ModelToolCall(
                id="apply-call",
                name="workspace_change",
                arguments={
                    "proposal_id": proposal["proposal_id"],
                    "action": "apply",
                    "expected_revision": proposal["revision"],
                    "request_key": "model-apply",
                },
            )
        else:
            yield ModelText(text="已读取真实工具回执")


@pytest.mark.parametrize("approved", [False, True])
async def test_actual_model_loop_approval_is_bound_to_original_run_and_call(
    runtime, tmp_path, approved
):
    runtime.provider = FileAgentProvider()
    service, session, _ = await setup(runtime, tmp_path, policy=ask_writes)
    try:
        await session.start("完成文件任务", idempotency_key="model-loop")
        run_id = await service.wait_started(session.agent_id, "model-loop")
        async with asyncio.timeout(5):
            while True:
                interactions = await runtime.repository.list_interactions(run_id)
                if interactions:
                    break
                await asyncio.sleep(0.01)
        pending = interactions[0]
        assert pending.status == "pending" and pending.call_id == "apply-call"
        assert pending.tool_name == "workspace_change" and not (tmp_path / "model.txt").exists()
        await service._handle(session.agent_id).runtime.interactions.reply(
            run_id, pending.id, InteractionReply(action="approve" if approved else "deny")
        )
        assert (await session.wait()).status == "completed"
        proposal = (await session.file_changes())[0]
        history = await session.file_change_history(proposal.proposal_id)
        assert len(history) == int(approved)
        assert (tmp_path / "model.txt").exists() == approved
        if approved:
            assert history[0].run_id == run_id and history[0].call_id == "apply-call"
            assert (tmp_path / "model.txt").read_text() == "模型修改\n"
        assert all(item.run_id == run_id for item in await runtime.repository.audits(run_id))
    finally:
        await service.close()


async def test_operation_lease_renews_and_active_execution_cannot_be_reconciled(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    service.workspace_edits.lease_seconds = 1
    renewed = asyncio.Event()
    renewals = 0
    original_renew = service.workspace_edits.renew

    async def observe_renew(operation_id):
        nonlocal renewals
        await original_renew(operation_id)
        renewals += 1
        if renewals >= 6:
            renewed.set()

    monkeypatch.setattr(service.workspace_edits, "renew", observe_renew)
    entered, release = Event(), Event()
    original = WorkspaceWriter.apply

    def blocked(self, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, "apply", blocked)
    try:
        proposal = await preview(session, file)
        task = asyncio.create_task(session.change_file(action(proposal)))
        await wait_event(entered)
        await asyncio.wait_for(renewed.wait(), 5)
        view = (await session.file_change_history(proposal.proposal_id))[0]
        stored = await service.workspace_edits.operation(
            f"session:{session.session_id}", view.operation_id
        )
        assert stored.lease_expires_at > time.time()
        assert await service.workspace_edits.recover_interrupted() == 0
        with pytest.raises(ServiceError) as active:
            await session.reconcile_file_change(view.operation_id)
        assert active.value.code == "workspace_operation_active"
        release.set()
        assert (await task).status == "done"
    finally:
        release.set()
        await service.close()


@pytest.mark.parametrize("loss", ["session", "resource", "operation_owner"])
async def test_authority_loss_before_publication_stops_file_action(
    runtime, tmp_path, monkeypatch, loss
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.apply

    def blocked(self, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, "apply", blocked)
    try:
        proposal = await preview(session, file)
        task = asyncio.create_task(session.change_file(action(proposal)))
        await wait_event(entered)
        view = (await session.file_change_history(proposal.proposal_id))[0]
        if loss == "session":
            await session.close()
        elif loss == "resource":
            service.disable_workspace("files")
        else:
            async with runtime.repository.database.sessions.begin() as db:
                await db.execute(
                    update(WorkspaceEditAttemptRow)
                    .where(WorkspaceEditAttemptRow.operation_id == view.operation_id)
                    .values(owner_id="foreign")
                )
        await asyncio.sleep(0.3)
        release.set()
        result = await asyncio.gather(task, return_exceptions=True)
        assert file.read_text() == "old\n"
        stored = await session.file_change_operation(view.operation_id)
        if loss == "operation_owner":
            assert (
                isinstance(result[0], ServiceError)
                and result[0].code == "workspace_receipt_pending"
            )
            assert stored.status == "running"
        else:
            assert result[0].status == "rejected" and stored.status == "rejected"
    finally:
        release.set()
        await service.close()


@pytest.mark.parametrize("mutation", ["content", "mode", "stage"])
async def test_reconciliation_preserves_uncertain_material_for_unproven_state(
    runtime, tmp_path, monkeypatch, mutation
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    original = service.workspace_edits.settle
    failed = False

    async def fault(*args, **kwargs):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("receipt unavailable")
        return await original(*args, **kwargs)

    monkeypatch.setattr(service.workspace_edits, "settle", fault)
    try:
        proposal = await preview(session, file)
        attempt = await session.change_file(action(proposal))
        assert attempt.status == "uncertain"
        stage = next(tmp_path.glob("._krim-tx-*"))
        if mutation == "content":
            file.write_text("external\n")
        elif mutation == "mode":
            file.chmod(0o400)
        else:
            stage.write_text("unrelated material")
        assert (await session.reconcile_file_change(attempt.operation_id)).status == "uncertain"
        with pytest.raises(ServiceError) as cleanup:
            await session.finalize_file_change(attempt.operation_id)
        assert cleanup.value.code == "workspace_cleanup_state"
        assert stage.exists()
    finally:
        await service.close()


async def test_model_context_and_private_restore_cannot_be_forged_as_public_input(
    runtime, tmp_path
):
    from pydantic import ValidationError

    from krim_agent.tools import ToolError
    from krim_agent.workspace_change import WorkspacePreviewRequest

    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, binding = await setup(runtime, tmp_path)
    try:
        with pytest.raises(ValidationError):
            WorkspacePreviewRequest.model_validate(
                {
                    "spec": {"path": "file.txt", "operation": "restore", "expected_sha256": None},
                    "request_key": "forged",
                }
            )
        with pytest.raises(ToolError) as missing:
            await service.manager.workspace_changes.tool_operation(
                "preview",
                {
                    "spec": {
                        "path": "file.txt",
                        "operation": "write",
                        "expected_sha256": None,
                        "content": "no",
                    },
                    "request_key": "forged",
                },
                ToolContext(
                    service.runtime.tools,
                    workspace=binding,
                    session_id=session.session_id,
                    agent_id=session.agent_id,
                    topic_id=(await session.view()).topic_id,
                ),
            )
        assert missing.value.code == "workspace_run_mismatch"
        assert file.read_text() == "old\n"
        assert WORKSPACE_CHANGE_TOOL_NAMES <= {
            item.name for item in service._handle(session.agent_id).runtime.tools.definitions
        }
    finally:
        await service.close()


@pytest.mark.parametrize("mode", ["create", "crash-before", "crash-after"])
async def test_real_process_crash_restart_reconciles_and_never_replays_file_action(tmp_path, mode):
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("old\n")
    database = tmp_path / "process.sqlite"
    metadata = tmp_path / "metadata.json"
    script = str(Path(__file__).parent / "fixtures" / "workspace_change_worker.py")

    async def process(phase):
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            script,
            str(database),
            str(root),
            phase,
            str(metadata),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(child.communicate(), 20)
        assert child.returncode == (23 if phase.startswith("crash") else 0), stderr.decode()
        return json.loads(stdout.decode().splitlines()[-1]) if child.returncode == 0 else None

    first = await process(mode)
    if mode.startswith("crash"):
        # 真实os._exit不执行任何close；只加速原租约到期，恢复不取得活跃owner。
        with sqlite3.connect(database) as connection:
            connection.execute("UPDATE agent_handles SET lease_expires_at = ?", (time.time() - 1,))
            connection.execute(
                "UPDATE workspace_edit_attempts SET lease_expires_at = ?", (time.time() - 1,)
            )
    second = await process("recover" if mode.startswith("crash") else "resume")
    assert second["calls"] == 0 and second["migration"] == "0031_command_artifact_proposals"
    assert second["status"] == ("rejected" if mode == "crash-before" else "done")
    if first:
        assert second["operation_id"] == first["operation_id"]
    assert (root / "file.txt").read_text() == "old\n"


async def test_interrupted_creation_with_two_internal_links_can_be_verified_and_cleaned(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "created.txt"
    service, session, _ = await setup(runtime, tmp_path)
    original_unlink = os.unlink
    failed = False

    def fault(path, *args, **kwargs):
        nonlocal failed
        if str(path).startswith("._krim-tx-") and not failed:
            failed = True
            raise OSError("injected creation link cleanup failure")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", fault)
    try:
        proposal = await preview(session, file)
        result = await session.change_file(action(proposal))
        assert result.status == "uncertain" and file.stat().st_nlink == 2
        with pytest.raises(ServiceError) as linked:
            await session.read_file(file.name)
        assert linked.value.code == "workspace_link"
        verified = await session.reconcile_file_change(result.operation_id)
        assert verified.status == "done" and verified.cleanup_pending
        assert not (await session.finalize_file_change(result.operation_id)).cleanup_pending
        assert file.stat().st_nlink == 1 and (await session.read_file(file.name)).content == "new\n"
    finally:
        await service.close()


async def test_unpublished_staged_content_is_rejected_and_cleanup_checks_original_identity(
    runtime, tmp_path
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, binding = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file)
        scope = f"session:{session.session_id}"
        prepared = (await service.workspace_edits.get(scope, proposal.proposal_id)).prepared
        attempt, _ = await service.workspace_edits.reserve(
            scope, action(proposal), binding.fingerprint, prepared
        )
        stage = tmp_path / attempt.recovery.stage_name
        stage.write_bytes(prepared.after)
        async with runtime.repository.database.sessions.begin() as db:
            await db.execute(
                update(WorkspaceEditAttemptRow)
                .where(WorkspaceEditAttemptRow.operation_id == attempt.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        result = await session.reconcile_file_change(attempt.view.operation_id)
        assert result.status == "rejected" and result.cleanup_pending
        assert not (await session.finalize_file_change(result.operation_id)).cleanup_pending
        assert not stage.exists() and file.read_text() == "old\n"
        assert not (await session.finalize_file_change(result.operation_id)).cleanup_pending
    finally:
        await service.close()


async def test_post_hook_invalid_output_does_not_hide_durable_file_result(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)

    async def invalid(definition, result, _context):
        return {"invalid": True} if definition.name == "workspace_change" else result

    try:
        proposal = await preview(session, file)
        registry = service._handle(session.agent_id).runtime.tools
        registry.add_global_post(invalid)
        with pytest.raises(ServiceError) as output:
            await session.change_file(action(proposal))
        assert output.value.code == "invalid_output_schema"
        assert file.read_text() == "new\n"
        attempt = (await session.file_change_history(proposal.proposal_id))[0]
        assert attempt.status == "done" and attempt.cleanup_pending
        registry._global_post.clear()
        replay = await session.change_file(action(proposal))
        assert replay.operation_id == attempt.operation_id
    finally:
        await service.close()
