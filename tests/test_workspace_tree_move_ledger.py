"""原非空树Store/共享锁/来源/CAS，真实Writer效果与原操作核实。"""

import asyncio
import copy
import time
from dataclasses import replace

import pytest
from sqlalchemy import event, func, select, update
from sqlalchemy.exc import IntegrityError
from test_workspace_changes import setup
from test_workspace_tree_snapshot import seed

import krim_agent.workspace_tree_writer as move_module
from krim_agent.persistence.database import (
    Database,
    RunRow,
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveLockRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreeMoveProposalRow,
)
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveModelSource,
    TreeMovePreviewRequest,
)
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter
from krim_agent.workspace_writer import WorkspaceWriter


async def prepared(runtime, root):
    seed(root)
    (root / "dest").mkdir()
    service, agent, binding = await setup(runtime, root)
    writer = WorkspaceTreeWriter(binding.workspace)
    snapshot = writer.reader.capture("tree")
    request = TreeMovePreviewRequest(
        source_path="tree",
        target_path="dest/tree",
        expected_fingerprint=snapshot.fingerprint,
        request_key="original-preview",
    )
    change = writer.prepare("tree", "dest/tree", expected_fingerprint=snapshot.fingerprint)
    store = WorkspaceTreeMoveStore(service.workspace_edits)
    return service, agent, binding, writer, store, request, change


def action(view, kind="apply", key="original-apply"):
    return TreeMoveAction(
        proposal_id=view.proposal_id, action=kind, expected_revision=view.revision, request_key=key
    )


async def publish(store, writer, scope, attempt):
    loop = asyncio.get_running_loop()

    def authorize():
        asyncio.run_coroutine_threadsafe(
            store.assert_execution(scope, attempt.view.operation_id), loop
        ).result(timeout=5)

    def intent(change):
        asyncio.run_coroutine_threadsafe(
            store.record_intent(scope, attempt.view.operation_id, change), loop
        ).result(timeout=5)

    return await asyncio.to_thread(
        writer.apply, attempt.prepared, authorize=authorize, record_intent=intent
    )


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceTreeMoveProposalRow,
                WorkspaceTreeMoveAttemptRow,
                WorkspaceTreeMoveLockRow,
                WorkspaceTreeMoveModelSourceRow,
            )
        ]


async def test_original_real_move_keep_undo_cold_key_and_public_receipt_has_no_private_material(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, created = await store.preview(scope, binding, request, change)
        assert (
            created
            and proposal.view.status == "proposed"
            and await counts(store.database) == [1, 0, 0, 0]
        )
        assert (await store.preview(scope, binding, request, None)) == (proposal, False)
        attempt, created = await store.reserve(
            scope, action(proposal.view), binding.fingerprint, change
        )
        assert created and attempt.view.status == "running"
        await store.renew(scope, attempt.view.operation_id)
        effect = await publish(store, writer, scope, attempt)
        assert effect.receipt.status == "done"
        done = await store.settle(scope, attempt.view.operation_id, effect)
        assert done.view.receipt.changed and done.intent_recorded
        assert (
            "operation_token" not in done.view.model_dump_json()
            and change.token not in done.view.model_dump_json()
        )
        assert (tmp_path / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        assert (await store.reserve(scope, action(proposal.view), binding.fingerprint, change)) == (
            done,
            False,
        )
        assert (await store.proposal_by_key(scope, request.request_key)).view.status == "applied"
        current = await store.proposal(scope, proposal.view.proposal_id)
        kept, _ = await store.reserve(
            scope, action(current.view, "keep", "keep"), binding.fingerprint, change
        )
        keep_effect = replace(effect, receipt=replace(effect.receipt, changed=False))
        kept = await store.settle(scope, kept.view.operation_id, keep_effect)
        assert kept.view.status == "done" and not kept.view.receipt.changed
        current = await store.proposal(scope, proposal.view.proposal_id)
        reverse = writer.prepare_undo(change, effect)
        undo, _ = await store.reserve(
            scope, action(current.view, "undo", "undo"), binding.fingerprint, reverse
        )
        undo_effect = await publish(store, writer, scope, undo)
        undone = await store.settle(scope, undo.view.operation_id, undo_effect)
        assert (
            undone.view.status == "done"
            and (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        )
        assert (await store.proposal(scope, proposal.view.proposal_id)).view.status == "undone"
        assert [item.action for item in await store.history(scope, proposal.view.proposal_id)] == [
            "apply",
            "keep",
            "undo",
        ]
        assert (await store.list(scope)).items[0].proposal_id == proposal.view.proposal_id
        assert await counts(store.database) == [1, 3, 0, 0]
    finally:
        await service.close()


async def test_model_prepare_accept_sources_same_transaction_stopped_run_fences_and_cold_read(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="original model source", idempotency_key="source"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=store.owner_id,
        )
        preview_source, apply_source = (
            TreeMoveModelSource(run.id, "preview"),
            TreeMoveModelSource(run.id, "apply"),
        )
        proposal, _ = await store.preview(
            scope, binding, request, change, model_source=preview_source
        )
        with pytest.raises(ServiceError) as conflict:
            await store.preview(
                scope,
                binding,
                request.model_copy(update={"request_key": "other"}),
                change,
                model_source=preview_source,
            )
        assert conflict.value.code == "workspace_tree_move_call_conflict"
        with pytest.raises(ServiceError) as conflict:
            await store.preview(scope, binding, request, None, model_source=apply_source)
        assert conflict.value.code == "workspace_tree_move_source_conflict"
        attempt, _ = await store.reserve(
            scope, action(proposal.view), binding.fingerprint, change, model_source=apply_source
        )
        assert attempt.model_source == apply_source and await counts(store.database) == [
            1,
            1,
            7,
            2,
        ]
        async with store.database.sessions.begin() as sql:
            original = await sql.get(RunRow, run.id)
            original.stop_requested = True
        with pytest.raises(ServiceError):
            await store.assert_execution(scope, attempt.view.operation_id)
        effect = await publish(store, writer, scope, attempt)
        assert effect.receipt.status == "rejected" and not effect.receipt.changed
        rejected = await store.settle(scope, attempt.view.operation_id, effect)
        await runtime.repository.finish(run.id, "aborted", "stopped", [])
        assert (await store.operation(scope, attempt.view.operation_id)) == rejected
        assert (await store.preview(scope, binding, request, None, model_source=preview_source))[
            1
        ] is False
        assert (tmp_path / "tree").is_dir() and not (tmp_path / "dest/tree").exists()
    finally:
        await service.close()


async def test_two_connections_only_one_cas_attempt_namespace_and_original_source(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    other = Database(str(store.database.engine.url))
    await other.initialize()
    second = WorkspaceTreeMoveStore(WorkspaceEditStore(other, owner_id=store.owner_id))
    scope = "session:" + agent.session_id
    try:
        proposals = await asyncio.gather(
            store.preview(scope, binding, request, change),
            second.preview(scope, binding, request, change),
        )
        assert sum(item[1] for item in proposals) == 1
        proposal = proposals[0][0]
        attempts = await asyncio.gather(
            store.reserve(scope, action(proposal.view), binding.fingerprint, change),
            second.reserve(scope, action(proposal.view), binding.fingerprint, change),
        )
        assert (
            sum(item[1] for item in attempts) == 1
            and attempts[0][0].view.operation_id == attempts[1][0].view.operation_id
        )
        attempt = attempts[0][0]
        effect = await publish(store, writer, scope, attempt)
        await store.settle(scope, attempt.view.operation_id, effect)
        assert (await second.operation(scope, attempt.view.operation_id)).view.status == "done"
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        await other.close()
        await service.close()


@pytest.mark.parametrize("first", ["file", "directory", "tree-file", "tree-directory"])
async def test_original_file_directory_and_nonempty_move_both_acceptance_orders_are_exclusive(
    runtime, tmp_path, first
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        files = service.workspace_edits
        directories = service.manager.directory_changes.store
        file_writer = WorkspaceWriter(binding.workspace)
        file_change = file_writer.prepare(
            FileChangeSpec(
                path="tree/sub/new.txt", operation="write", content="new", expected_sha256=None
            )
        )
        file_view, _ = await files.preview(
            scope, binding.key, binding.fingerprint, "file", file_change
        )
        file_action = WorkspaceChangeAction(
            proposal_id=file_view.view.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="file",
        )
        directory_writer = WorkspaceDirectoryWriter(binding.workspace)
        directory_change = directory_writer.prepare(
            DirectoryChangeSpec(path="tree/sub/newdir", operation="create", mode=0o750)
        )
        directory_view, _ = await directories.preview(
            scope, binding.key, binding.fingerprint, "directory", directory_change
        )
        directory_action = DirectoryChangeAction(
            proposal_id=directory_view.view.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="directory",
        )
        if first == "file":
            await files.reserve(scope, file_action, binding.fingerprint, file_change)
            with pytest.raises(ServiceError) as blocked:
                await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
            assert blocked.value.code == "workspace_file_busy"
        elif first == "directory":
            await directories.reserve(
                scope, directory_action, binding.fingerprint, directory_change
            )
            with pytest.raises(ServiceError) as blocked:
                await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
            assert blocked.value.code == "workspace_directory_busy"
        else:
            attempt, _ = await store.reserve(
                scope, action(proposal.view), binding.fingerprint, change
            )
            with pytest.raises(ServiceError) as blocked:
                if first == "tree-file":
                    await files.reserve(scope, file_action, binding.fingerprint, file_change)
                else:
                    await directories.reserve(
                        scope, directory_action, binding.fingerprint, directory_change
                    )
            assert blocked.value.code == "workspace_tree_move_busy"
            await store.assert_execution(scope, attempt.view.operation_id)
        assert (tmp_path / "tree").is_dir() and not (tmp_path / "dest/tree").exists()
    finally:
        await service.close()


async def test_expired_owner_keeps_locks_and_reconciles_original_operation_without_replay(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    calls = 0
    original = move_module.rename_at
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)

        def lost(*args, **kwargs):
            nonlocal calls
            calls += 1
            original(*args, **kwargs)
            raise OSError("original tree return lost")

        monkeypatch.setattr(move_module, "rename_at", lost)
        effect = await publish(store, writer, scope, attempt)
        assert effect.receipt.status == "uncertain" and calls == 1
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.operation_id == attempt.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        interrupted = await store.operation(scope, attempt.view.operation_id)
        assert interrupted.view.status == "uncertain" and interrupted.effect is None
        assert (await counts(store.database))[2] > 0
        with pytest.raises(ServiceError):
            await store.renew(scope, attempt.view.operation_id)
        with pytest.raises(ServiceError):
            await store.settle(scope, attempt.view.operation_id, effect)
        resolved = writer.reconcile(interrupted.prepared)
        result = await store.settle(scope, attempt.view.operation_id, resolved, resolving=True)
        assert (
            result.view.status == "done"
            and result.view.operation_id == attempt.view.operation_id
            and calls == 1
        )
        assert await counts(store.database) == [1, 1, 0, 0]
        assert (await store.reserve(scope, action(proposal.view), binding.fingerprint, change)) == (
            result,
            False,
        )
    finally:
        await service.close()


async def test_cancel_preserves_original_admission_and_rejected_cannot_reopen_apply(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        await store.cancel(scope, proposal.view.proposal_id)
        with pytest.raises(ServiceError) as stopped:
            await store.assert_execution(scope, attempt.view.operation_id)
        assert stopped.value.code == "workspace_tree_move_fenced"
        result = await publish(store, writer, scope, attempt)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        done = await store.settle(scope, attempt.view.operation_id, result)
        assert done.view.cancelled and done.view.status == "rejected"
        closed = await store.proposal(scope, proposal.view.proposal_id)
        assert closed.view.status == "cancelled"
        with pytest.raises(ServiceError):
            await store.reserve(
                scope, action(closed.view, key="new-apply"), binding.fingerprint, change
            )
        assert (tmp_path / "tree").is_dir() and not (tmp_path / "dest/tree").exists()
    finally:
        await service.close()


async def test_source_bind_failure_rolls_back_proposal_and_attempt_same_transaction(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="source", idempotency_key="run"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=store.owner_id,
        )
        source = TreeMoveModelSource(run.id, "preview")

        def fail(_connection, _cursor, statement, _params, _context, _many):
            if statement.startswith("INSERT INTO workspace_tree_move_model_sources"):
                raise RuntimeError("source bind failure")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="source bind failure"):
                await store.preview(scope, binding, request, change, model_source=source)
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await counts(store.database) == [0, 0, 0, 0]
        proposal, _ = await store.preview(scope, binding, request, change, model_source=source)
        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="source bind failure"):
                await store.reserve(
                    scope,
                    action(proposal.view),
                    binding.fingerprint,
                    change,
                    model_source=TreeMoveModelSource(run.id, "apply"),
                )
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await counts(store.database) == [1, 0, 0, 1]
        assert (await store.proposal(scope, proposal.view.proposal_id)).view.status == "proposed"
    finally:
        await service.close()


async def test_private_payload_or_request_mutation_refuses_cold_fact(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        async with store.database.sessions.begin() as sql:
            row = await sql.get(WorkspaceTreeMoveProposalRow, proposal.view.proposal_id)
            row.payload = b"corrupt"
        with pytest.raises(ServiceError) as damaged:
            await store.proposal(scope, proposal.view.proposal_id)
        assert damaged.value.code == "workspace_tree_move_corrupt"
        async with store.database.sessions.begin() as sql:
            row = await sql.get(WorkspaceTreeMoveProposalRow, proposal.view.proposal_id)
            row.payload = (
                change.before.entries[1].file.data + b"\0\xff\x80" + "目录树原字节\n".encode()
            )
            row.request = {**copy.deepcopy(row.request), "target_path": "other"}
        with pytest.raises(ServiceError):
            await store.proposal(scope, proposal.view.proposal_id)
    finally:
        await service.close()


async def test_model_source_and_sql_null_fk_constraints_cannot_bind_other_parent_or_stage(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="source constraints", idempotency_key="run"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=store.owner_id,
        )
        proposal, _ = await store.preview(
            scope, binding, request, change, model_source=TreeMoveModelSource(run.id, "preview")
        )
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        other, _ = await store.preview(
            scope, binding, request.model_copy(update={"request_key": "other"}), change
        )
        for changes in (
            {"kind": None},
            {"kind": "unknown"},
            {"kind": "preview"},
            {"run_id": None},
            {"run_id": "missing"},
            {"call_id": None},
            {"call_id": ""},
            {"call_id": "x" * 129},
            {"call_id": "preview"},
            {"operation_id": None},
            {"operation_id": "missing"},
            {"proposal_id": other.view.proposal_id},
        ):
            with pytest.raises(IntegrityError):
                async with store.database.sessions.begin() as sql:
                    sql.add(
                        WorkspaceTreeMoveModelSourceRow(
                            **{
                                "run_id": run.id,
                                "call_id": "apply",
                                "kind": "apply",
                                "proposal_id": proposal.view.proposal_id,
                                "operation_id": attempt.view.operation_id,
                                **changes,
                            }
                        )
                    )
        assert (await counts(store.database))[3] == 1
        for changes in (
            {"owner_id": None},
            {"lease_expires_at": None},
            {"intent_recorded": None},
            {"run_id": run.id},
            {"call_id": "unbound"},
            {"after_payload": b"x"},
            {"status": "done", "owner_id": None, "lease_expires_at": None},
        ):
            with pytest.raises(IntegrityError):
                async with store.database.sessions.begin() as sql:
                    await sql.execute(
                        update(WorkspaceTreeMoveAttemptRow)
                        .where(
                            WorkspaceTreeMoveAttemptRow.operation_id == attempt.view.operation_id
                        )
                        .values(**changes)
                    )
        await store.assert_execution(scope, attempt.view.operation_id)
    finally:
        await service.close()


async def test_receipt_forgery_and_late_owner_cannot_replace_original_fact(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        effect = await publish(store, writer, scope, attempt)
        fake = replace(effect, receipt=replace(effect.receipt, operation_token="0" * 32))
        with pytest.raises(ServiceError) as forged:
            await store.settle(scope, attempt.view.operation_id, fake)
        assert forged.value.code == "workspace_tree_move_corrupt"
        assert (await store.operation(scope, attempt.view.operation_id)).view.status == "running"
        done = await store.settle(scope, attempt.view.operation_id, effect)
        with pytest.raises(ServiceError) as replaced:
            await store.settle(
                scope,
                attempt.view.operation_id,
                replace(effect, receipt=replace(effect.receipt, status="rejected", changed=False)),
            )
        assert replaced.value.code == "workspace_tree_move_resolved"
        assert await store.operation(scope, attempt.view.operation_id) == done
    finally:
        await service.close()


async def test_namespace_lock_loss_or_foreign_scope_is_fail_closed(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        other = await service.spawn("default")
        with pytest.raises(ServiceError) as foreign:
            await store.proposal("session:" + other.session_id, proposal.view.proposal_id)
        assert foreign.value.code == "workspace_tree_move_not_found"
        async with store.database.sessions.begin() as sql:
            locks = list(
                await sql.scalars(
                    select(WorkspaceTreeMoveLockRow).where(
                        WorkspaceTreeMoveLockRow.operation_id == attempt.view.operation_id
                    )
                )
            )
            await sql.delete(locks[0])
        with pytest.raises(ServiceError) as lost:
            await store.assert_execution(scope, attempt.view.operation_id)
        assert lost.value.code == "workspace_tree_move_corrupt"
        result = await publish(store, writer, scope, attempt)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert (tmp_path / "tree").is_dir()
    finally:
        await service.close()


async def test_history_snapshot_quota_rolls_back_without_lock_or_effect(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        small = WorkspaceTreeMoveStore(service.workspace_edits, max_snapshot_bytes=1024)
        with pytest.raises(ServiceError) as full:
            await small.preview(scope, binding, request, change)
        assert full.value.code == "workspace_tree_move_snapshot_limit"
        assert await counts(store.database) == [0, 0, 0, 0]
        limited = WorkspaceTreeMoveStore(service.workspace_edits, max_proposals=1)
        proposal, _ = await limited.preview(scope, binding, request, change)
        with pytest.raises(ServiceError) as full:
            await limited.preview(
                scope, binding, request.model_copy(update={"request_key": "new"}), change
            )
        assert full.value.code == "workspace_tree_move_limit"
        assert (await limited.preview(scope, binding, request, None)) == (proposal, False)
        assert await counts(store.database) == [1, 0, 0, 0]
    finally:
        await service.close()


async def test_proposal_cold_read_expires_original_producer_without_releasing_unknown_locks(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.operation_id == attempt.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        cold = await store.proposal_by_key(scope, request.request_key)
        assert (
            cold.view.status == "uncertain"
            and cold.view.active_operation_id == attempt.view.operation_id
        )
        assert (await store.operation(scope, attempt.view.operation_id)).view.status == "uncertain"
        assert (await counts(store.database))[2] == 7
        with pytest.raises(ServiceError):
            await store.assert_execution(scope, attempt.view.operation_id)
        assert (tmp_path / "tree").is_dir() and not (tmp_path / "dest/tree").exists()
    finally:
        await service.close()
