"""连续文件效果在真实SDK、文件系统及故障核实中保持同一事实。"""

import asyncio
import hashlib
import sys
from threading import Event

import pytest
from sqlalchemy import func, select
from test_workspace_changes import action, preview, setup, wait_event

from krim_agent.application.workspace_changes import WorkspaceChangeService
from krim_agent.persistence.database import WorkspaceFileEffectRow
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession
from krim_agent.workspace_writer import FileEffect, WorkspaceWriter, observed_state

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def apply_content(session, file, content, key):
    proposal = await preview(
        session,
        file,
        content=content,
        key=f"preview-{key}",
        operation="delete" if content is None else "write",
    )
    result = await session.change_file(action(proposal, key=f"apply-{key}"))
    assert result.status == "done"
    return await session.file_change(proposal.proposal_id), result


@pytest.mark.parametrize(
    "old,middle,last",
    [
        ("A\n", "B\n", "C\n"),
        (None, "", "C\n"),
        ("A\n", None, "C\n"),
        ("A\n", "B\n", None),
        (None, "B\n", None),
        ("A\n", "A\n", "C\n"),
    ],
)
async def test_keep_chain_without_writes_then_reverse_undo(runtime, tmp_path, old, middle, last):
    file = tmp_path / "chain.txt"
    if old is not None:
        file.write_text(old)
        file.chmod(0o640)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        first, first_apply = await apply_content(session, file, middle, "first")
        second, second_apply = await apply_content(session, file, last, "second")
        stamp = file.stat() if file.exists() else None
        kept = await session.change_file(action(first, "keep", "keep-first"))
        assert kept.status == "done" and not kept.receipt.changed
        assert kept.receipt.before_sha256 == first.preview.before_sha256
        assert kept.receipt.after_sha256 == first.preview.after_sha256
        proof = await session.file_change_proof(kept.operation_id)
        assert proof.kind == "successor_chain" and proof.successor_count == 1
        assert proof.tip_proposal_id == second.proposal_id
        assert proof.tip_operation_id == second_apply.operation_id
        assert proof.tip_after_sha256 == second.preview.after_sha256
        assert (file.stat() if file.exists() else None) == stamp
        assert (await session.file_change_proof(first_apply.operation_id)).kind == "not_applicable"
        await session.change_file(action(second, "keep", "keep-second"))
        async with runtime.repository.database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(WorkspaceFileEffectRow)) == 2
        for item, expected in ((second, middle), (first, old)):
            current = await session.file_change(item.proposal_id)
            assert current.status == "kept"
            undone = await session.change_file(action(current, "undo", f"undo-{item.proposal_id}"))
            assert undone.status == "done"
            assert (file.read_text() if file.exists() else None) == expected
            for operation in await session.file_change_history(item.proposal_id):
                await session.finalize_file_change(operation.operation_id)
        # 原证明不会因后续撤销而重新选择链尾，且没有私有正文/路径泄露。
        assert await session.file_change_proof(kept.operation_id) == proof
        assert str(tmp_path) not in proof.model_dump_json()
        assert not list(tmp_path.glob("._krim-tx-*"))
    finally:
        await service.close()


async def test_external_attribute_or_content_change_rejects_frozen_tip(runtime, tmp_path):
    file = tmp_path / "chain.txt"
    file.write_text("A\n")
    file.chmod(0o640)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        first, _ = await apply_content(session, file, "B\n", "first")
        await apply_content(session, file, "C\n", "second")
        file.chmod(0o600)
        rejected = await session.change_file(action(first, "keep", "changed-mode"))
        assert rejected.status == "rejected" and not rejected.receipt.changed
        assert (await session.file_change(first.proposal_id)).status == "applied"
        file.chmod(0o640)
        file.write_text("external\n")
        rejected = await session.change_file(
            action(await session.file_change(first.proposal_id), "keep", "changed-content")
        )
        assert rejected.status == "rejected"
        assert file.read_text() == "external\n"
    finally:
        await service.close()


async def test_noop_and_create_capture_actual_attributes(runtime, tmp_path):
    file = tmp_path / "chain.txt"
    file.write_text("A\n")
    file.chmod(0o4755)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        first, noop = await apply_content(session, file, "A\n", "noop")
        assert not noop.receipt.changed and file.stat().st_mode & 0o7777 == 0o4755
        second, _ = await apply_content(session, file, "B\n", "changed")
        assert file.stat().st_mode & 0o7777 == 0o755
        kept = await session.change_file(action(first, "keep", "keep-noop"))
        assert kept.status == "done"
        assert (await session.file_change_proof(kept.operation_id)).kind == "successor_chain"
        async with runtime.repository.database.sessions() as sql:
            row = await sql.scalar(
                select(WorkspaceFileEffectRow).where(
                    WorkspaceFileEffectRow.operation_id == noop.operation_id
                )
            )
            assert row.observed_after["mode"] == 0o4755
        undone = await session.change_file(action(second, "undo", "undo-second"))
        assert undone.status == "done" and file.stat().st_mode & 0o7777 == 0o755
        created = tmp_path / "created.txt"
        tmp_path.chmod(0o2770)
        _proposal, applied = await apply_content(session, created, "", "create")
        async with runtime.repository.database.sessions() as sql:
            row = await sql.scalar(
                select(WorkspaceFileEffectRow).where(
                    WorkspaceFileEffectRow.operation_id == applied.operation_id
                )
            )
            assert row.observed_after == {
                "sha256": hashlib.sha256(b"").hexdigest(),
                "mode": created.stat().st_mode & 0o7777,
                "uid": created.stat().st_uid,
                "gid": created.stat().st_gid,
            }
    finally:
        await service.close()


async def test_foreign_scope_breaks_chain_without_leaking_its_proposals(runtime, tmp_path):
    file = tmp_path / "chain.txt"
    file.write_text("A\n")
    service, first_session, _ = await setup(runtime, tmp_path)
    second_session = AgentSession(service, (await service.spawn("default")).agent_id)
    try:
        first, _ = await apply_content(first_session, file, "B\n", "first")
        foreign, _ = await apply_content(second_session, file, "C\n", "foreign")
        await apply_content(second_session, file, "B\n", "foreign-return")
        await apply_content(first_session, file, "D\n", "last")
        rejected = await first_session.change_file(action(first, "keep", "keep-first"))
        assert rejected.status == "rejected"
        proof = await first_session.file_change_proof(rejected.operation_id)
        assert proof.kind == "exact_after" and proof.successor_count == 0
        assert foreign.proposal_id not in proof.model_dump_json()
        with pytest.raises(ServiceError) as missing:
            await second_session.file_change_proof(rejected.operation_id)
        assert missing.value.code == "workspace_operation_not_found"
        assert file.read_text() == "D\n"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["verify", "save"])
async def test_uncertain_keep_recovers_original_frozen_chain(runtime, tmp_path, monkeypatch, fault):
    file = tmp_path / "chain.txt"
    file.write_text("A\n")
    service, session, _ = await setup(runtime, tmp_path)
    verify = WorkspaceWriter.verify
    try:
        first, _ = await apply_content(session, file, "B\n", "first")
        second, _ = await apply_content(session, file, "C\n", "second")

        def inconclusive(self, change, reference, **kwargs):
            result = verify(self, change, reference, **kwargs)
            assert result.receipt.status == "done"
            return FileEffect(
                result.receipt.model_copy(
                    update={
                        "status": "uncertain",
                        "error_code": "workspace_reconcile_inconclusive",
                        "recovery_required": True,
                    }
                )
            )

        if fault == "verify":
            monkeypatch.setattr(WorkspaceWriter, "verify", inconclusive)
        else:
            settle = service.workspace_edits.settle

            async def failed_save(*args, **kwargs):
                if args[2].status == "done":
                    raise RuntimeError("保留回执提交前存储中断")
                return await settle(*args, **kwargs)

            monkeypatch.setattr(service.workspace_edits, "settle", failed_save)
        pending = await session.change_file(action(first, "keep", "keep-first"))
        assert pending.status == "uncertain" and not pending.receipt.changed
        assert not pending.cleanup_pending
        assert (await session.file_change(first.proposal_id)).status == "uncertain"
        blocked = await preview(session, file, content="D\n", key="blocked")
        with pytest.raises(ServiceError) as busy:
            await session.change_file(action(blocked, key="blocked-apply"))
        assert busy.value.code == "workspace_file_busy"
        # 重建编排服务和仓储对象，不能依赖原请求内存中的tip。
        previous = service.manager.workspace_changes
        await previous.close()
        service.manager.workspace_changes = WorkspaceChangeService(
            WorkspaceEditStore(service.workspace_edits.database, owner_id=service.manager.owner_id)
        )
        monkeypatch.setattr(WorkspaceWriter, "verify", verify)
        result = await session.reconcile_file_change(pending.operation_id)
        assert result.status == "done" and result.operation_id == pending.operation_id
        assert result.receipt.after_sha256 == first.preview.after_sha256
        proof = await session.file_change_proof(result.operation_id)
        assert proof.tip_proposal_id == second.proposal_id
        assert file.read_text() == "C\n"
    finally:
        await service.close()


async def test_keep_check_inside_file_lock_rejects_lost_resource(runtime, tmp_path, monkeypatch):
    file = tmp_path / "chain.txt"
    file.write_text("A\n")
    service, session, binding = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.verify
    snapshot = WorkspaceWriter._snapshot
    reads = 0

    def delayed(self, *args, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(self, *args, **kwargs)

    def observed_read(self, *args, **kwargs):
        nonlocal reads
        reads += 1
        return snapshot(self, *args, **kwargs)

    try:
        first, _ = await apply_content(session, file, "B\n", "first")
        await apply_content(session, file, "C\n", "second")
        monkeypatch.setattr(WorkspaceWriter, "verify", delayed)
        monkeypatch.setattr(WorkspaceWriter, "_snapshot", observed_read)
        task = asyncio.create_task(session.change_file(action(first, "keep", "keep-first")))
        await wait_event(entered)
        binding.revoked.set()
        release.set()
        result = await task
        assert result.status == "uncertain"
        assert reads == 0  # 锁内失权先拒绝读取，不能仅靠核实结束后的guard通过测试。
        assert (await session.file_change(first.proposal_id)).status != "kept"
        assert file.read_text() == "C\n"
    finally:
        release.set()
        await service.close()


def test_strict_keep_state_has_no_noop_attribute_fallback(tmp_path):
    from krim_agent.workspace import Workspace
    from krim_agent.workspace_change import FileChangeSpec

    file = tmp_path / "note.txt"
    file.write_text("same")
    file.chmod(0o640)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        prepared = writer.prepare(
            FileChangeSpec(
                path="note.txt",
                operation="write",
                expected_sha256=hashlib.sha256(b"same").hexdigest(),
                content="same",
            )
        )
        expected = observed_state(prepared.before).model_copy(update={"mode": 0o600})
        result = writer.verify(prepared, None, keeping=True, expected_after=expected)
        assert result.receipt.status == "rejected"
