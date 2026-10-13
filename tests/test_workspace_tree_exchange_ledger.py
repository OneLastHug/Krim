"""真实0044双树账本/CAS/调用来源/双向锁/unknown与原Writer资格。"""

import asyncio
import copy
import time
from dataclasses import replace

import pytest
from sqlalchemy import delete, event, func, select, update
from sqlalchemy.exc import IntegrityError
from test_workspace_changes import setup

import krim_agent.workspace_tree_exchange as physical
from krim_agent.persistence.database import (
    Database,
    RunRow,
    WorkspaceTreeExchangeAttemptRow,
    WorkspaceTreeExchangeLockRow,
    WorkspaceTreeExchangeModelSourceRow,
    WorkspaceTreeExchangeProposalRow,
)
from krim_agent.persistence.tree_exchange_codec import encode_exchange
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_exchanges import WorkspaceTreeExchangeStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeAction,
    TreeExchangeModelSource,
    TreeExchangePreviewRequest,
)
from krim_agent.workspace_tree_move import TreeMoveAction, TreeMovePreviewRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter
from krim_agent.workspace_writer import WorkspaceWriter


async def prepared(runtime, root):
    (root / "left/sub/empty").mkdir(parents=True)
    (root / "right/sub/empty").mkdir(parents=True)
    (root / "left/sub/data.bin").write_bytes(b"old\xff\0")
    (root / "right/sub/data.bin").write_bytes(b"new\0\xff")
    service, agent, binding = await setup(runtime, root)
    writer = WorkspaceTreeExchangeWriter(binding.workspace)
    left = writer.trees.reader.capture("left")
    right = writer.trees.reader.capture("right")
    request = TreeExchangePreviewRequest(
        left_path="left",
        right_path="right",
        expected_left=left.fingerprint,
        expected_right=right.fingerprint,
        request_key="original-preview",
    )
    change = writer.prepare(
        "left", "right", expected_left=left.fingerprint, expected_right=right.fingerprint
    )
    store = WorkspaceTreeExchangeStore(service.workspace_edits)
    return service, agent, binding, writer, store, request, change


def action(view, kind="apply", key="original-apply"):
    return TreeExchangeAction(
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
                WorkspaceTreeExchangeProposalRow,
                WorkspaceTreeExchangeAttemptRow,
                WorkspaceTreeExchangeLockRow,
                WorkspaceTreeExchangeModelSourceRow,
            )
        ]


async def start_run(runtime, service, agent, store):
    run, _ = await runtime.repository.begin_run(
        service._handle(agent.agent_id).topic_id,
        ChatRequest(text="合成原调用", idempotency_key="source"),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=store.owner_id,
    )
    return run


async def test_real_exchange_keep_original_evidence_undo_and_cold_same_key_zero_new_effect(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, created = await store.preview(scope, binding, request, change)
        assert created and proposal.view.origin == "existing_trees"
        assert await store.preview(scope, binding, request, None) == (proposal, False)
        attempt, created = await store.reserve(
            scope, action(proposal.view), binding.fingerprint, change
        )
        assert created and await counts(store.database) == [1, 1, 9, 0]
        await store.renew(scope, attempt.view.operation_id)
        effect = await publish(store, writer, scope, attempt)
        assert effect.receipt.status == "done"
        done = await store.settle(scope, attempt.view.operation_id, effect)
        assert done.effect == effect and done.intent_recorded
        assert change.token not in done.view.model_dump_json()
        assert "operation_token" not in done.view.model_dump_json()
        assert (tmp_path / "left/sub/data.bin").read_bytes() == b"new\0\xff"
        assert await store.reserve(scope, action(proposal.view), binding.fingerprint, change) == (
            done,
            False,
        )
        current = await store.proposal_by_key(scope, request.request_key)
        kept, _ = await store.reserve(
            scope, action(current.view, "keep", "keep"), binding.fingerprint, change
        )
        with pytest.raises(ServiceError) as logical:
            await store.record_intent(scope, kept.view.operation_id, change)
        assert logical.value.code == "workspace_tree_exchange_state"
        kept = await store.settle(scope, kept.view.operation_id, effect)
        assert not kept.view.receipt.changed and kept.effect == effect and not kept.intent_recorded
        assert (await store.proposal(scope, current.view.proposal_id)).view.status == "kept"
        reverse = writer.prepare_undo(change, effect)
        current = await store.proposal(scope, current.view.proposal_id)
        undo, _ = await store.reserve(
            scope, action(current.view, "undo", "undo"), binding.fingerprint, reverse
        )
        undone = await store.settle(
            scope, undo.view.operation_id, await publish(store, writer, scope, undo)
        )
        assert undone.view.status == "done" and undone.view.receipt.changed
        assert (tmp_path / "left/sub/data.bin").read_bytes() == b"old\xff\0"
        assert (tmp_path / "right/sub/data.bin").read_bytes() == b"new\0\xff"
        assert [item.action for item in await store.history(scope, current.view.proposal_id)] == [
            "apply",
            "keep",
            "undo",
        ]
        assert (await store.list(scope)).items[0].status == "undone"
        assert await counts(store.database) == [1, 3, 0, 0]
    finally:
        await service.close()


async def test_model_sources_atomic_unique_missing_preview_denied_stopped_run_and_cold_history(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        run = await start_run(runtime, service, agent, store)
        preview_source = TreeExchangeModelSource(run.id, "preview")
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
        assert conflict.value.code == "workspace_tree_exchange_call_conflict"
        attempt, _ = await store.reserve(
            scope,
            action(proposal.view),
            binding.fingerprint,
            change,
            model_source=TreeExchangeModelSource(run.id, "apply"),
        )
        async with store.database.sessions.begin() as sql:
            (await sql.get(RunRow, run.id)).stop_requested = True
        with pytest.raises(ServiceError):
            await store.assert_execution(scope, attempt.view.operation_id)
        rejected = await store.settle(
            scope, attempt.view.operation_id, await publish(store, writer, scope, attempt)
        )
        assert rejected.view.status == "rejected" and not rejected.view.receipt.changed
        await runtime.repository.finish(run.id, "aborted", "原Run停止", [])
        assert await store.operation(scope, attempt.view.operation_id) == rejected
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                delete(WorkspaceTreeExchangeModelSourceRow).where(
                    WorkspaceTreeExchangeModelSourceRow.call_id == "preview"
                )
            )
        with pytest.raises(ServiceError) as missing:
            await store.proposal_by_key(scope, request.request_key)
        assert missing.value.code == "workspace_tree_exchange_corrupt"
        with pytest.raises(ServiceError):
            await store.preview(scope, binding, request, None)
        assert (tmp_path / "left/sub/data.bin").read_bytes() == b"old\xff\0"
    finally:
        await service.close()


async def test_two_database_connections_accept_once_and_same_operation_cas(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    second_db = Database(str(store.database.engine.url))
    await second_db.initialize()
    second = WorkspaceTreeExchangeStore(WorkspaceEditStore(second_db, owner_id=store.owner_id))
    scope = "session:" + agent.session_id
    try:
        proposals = await asyncio.gather(
            store.preview(scope, binding, request, change),
            second.preview(scope, binding, request, change),
        )
        assert sum(item[1] for item in proposals) == 1
        proposal = proposals[0][0]
        accepted = await asyncio.gather(
            store.reserve(scope, action(proposal.view), binding.fingerprint, change),
            second.reserve(scope, action(proposal.view), binding.fingerprint, change),
        )
        assert sum(item[1] for item in accepted) == 1
        assert accepted[0][0].view.operation_id == accepted[1][0].view.operation_id
        result = await store.settle(
            scope,
            accepted[0][0].view.operation_id,
            await publish(store, writer, scope, accepted[0][0]),
        )
        assert await second.operation(scope, result.view.operation_id) == result
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        await second_db.close()
        await service.close()


@pytest.mark.parametrize("side", ["left", "right"])
@pytest.mark.parametrize("neighbor", ["file", "directory", "move"])
@pytest.mark.parametrize("first", ["neighbor", "exchange"])
async def test_file_directory_move_exchange_both_trees_and_both_acceptance_orders(
    runtime, tmp_path, side, neighbor, first
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        if neighbor == "file":
            adjacent = service.workspace_edits
            item = WorkspaceWriter(binding.workspace).prepare(
                FileChangeSpec(
                    path=side + "/sub/new.txt",
                    operation="write",
                    content="new",
                    expected_sha256=None,
                )
            )
            view, _ = await adjacent.preview(
                scope, binding.key, binding.fingerprint, "adjacent", item
            )
            spec = WorkspaceChangeAction(
                proposal_id=view.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="adjacent",
            )
        elif neighbor == "directory":
            adjacent = service.manager.directory_changes.store
            item = WorkspaceDirectoryWriter(binding.workspace).prepare(
                DirectoryChangeSpec(path=side + "/sub/newdir", operation="create", mode=0o750)
            )
            view, _ = await adjacent.preview(
                scope, binding.key, binding.fingerprint, "adjacent", item
            )
            spec = DirectoryChangeAction(
                proposal_id=view.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="adjacent",
            )
        else:
            adjacent = WorkspaceTreeMoveStore(service.workspace_edits)
            move_writer = WorkspaceTreeWriter(binding.workspace)
            snapshot = move_writer.reader.capture(side + "/sub")
            item = move_writer.prepare(
                snapshot.root_path, "out-" + side, expected_fingerprint=snapshot.fingerprint
            )
            spec_preview = TreeMovePreviewRequest(
                source_path=snapshot.root_path,
                target_path="out-" + side,
                expected_fingerprint=snapshot.fingerprint,
                request_key="adjacent",
            )
            view, _ = await adjacent.preview(scope, binding, spec_preview, item)
            spec = TreeMoveAction(
                proposal_id=view.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="adjacent",
            )
        if first == "neighbor":
            await adjacent.reserve(scope, spec, binding.fingerprint, item)
            with pytest.raises(ServiceError) as blocked:
                await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
            assert (
                blocked.value.code
                == {
                    "file": "workspace_file_busy",
                    "directory": "workspace_directory_busy",
                    "move": "workspace_tree_move_busy",
                }[neighbor]
            )
        else:
            accepted, _ = await store.reserve(
                scope, action(proposal.view), binding.fingerprint, change
            )
            with pytest.raises(ServiceError) as blocked:
                await adjacent.reserve(scope, spec, binding.fingerprint, item)
            assert blocked.value.code == "workspace_tree_exchange_busy"
            await store.assert_execution(scope, accepted.view.operation_id)
        assert (tmp_path / "left/sub/data.bin").read_bytes() == b"old\xff\0"
        assert (tmp_path / "right/sub/data.bin").read_bytes() == b"new\0\xff"
    finally:
        await service.close()


async def test_expired_owner_unknown_same_operation_keeps_locks_and_reconcile_no_replay(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    calls = 0
    actual = physical.rename_at
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)

        def lost(*args, **kwargs):
            nonlocal calls
            calls += 1
            actual(*args, **kwargs)
            raise OSError("原物理回程丢失")

        monkeypatch.setattr(physical, "rename_at", lost)
        unknown = await publish(store, writer, scope, attempt)
        assert unknown.receipt.status == "uncertain" and calls == 1
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(WorkspaceTreeExchangeAttemptRow)
                .where(WorkspaceTreeExchangeAttemptRow.operation_id == attempt.view.operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
        cold = await store.proposal_by_key(scope, request.request_key)
        assert (
            cold.view.status == "uncertain"
            and cold.view.active_operation_id == attempt.view.operation_id
        )
        assert await counts(store.database) == [1, 1, 9, 0]
        with pytest.raises(ServiceError):
            await store.renew(scope, attempt.view.operation_id)
        with pytest.raises(ServiceError):
            await store.settle(scope, attempt.view.operation_id, unknown)
        result = await store.settle(
            scope, attempt.view.operation_id, writer.reconcile(change), resolving=True
        )
        assert (
            result.view.operation_id == attempt.view.operation_id
            and result.view.status == "done"
            and calls == 1
        )
        assert await counts(store.database) == [1, 1, 0, 0]
        assert await store.reserve(scope, action(proposal.view), binding.fingerprint, change) == (
            result,
            False,
        )
    finally:
        await service.close()


@pytest.mark.parametrize("when", ["before", "after"])
async def test_cancel_before_and_after_physical_effect_preserves_actual_fact(
    runtime, tmp_path, monkeypatch, when
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        if when == "before":
            await store.cancel(scope, proposal.view.proposal_id)
        else:
            loop = asyncio.get_running_loop()
            actual = physical.rename_at

            def cancel_after(*args, **kwargs):
                actual(*args, **kwargs)
                asyncio.run_coroutine_threadsafe(
                    store.cancel(scope, proposal.view.proposal_id), loop
                ).result(timeout=5)

            monkeypatch.setattr(physical, "rename_at", cancel_after)
        effect = await publish(store, writer, scope, attempt)
        result = await store.settle(scope, attempt.view.operation_id, effect)
        assert result.view.cancelled
        assert result.view.status == ("rejected" if when == "before" else "done")
        assert result.view.receipt.changed == (when == "after")
        assert (await store.proposal(scope, proposal.view.proposal_id)).view.status == (
            "cancelled" if when == "before" else "applied"
        )
    finally:
        await service.close()


async def test_source_insert_failure_rolls_back_entire_admission_and_locks(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        run = await start_run(runtime, service, agent, store)
        source = TreeExchangeModelSource(run.id, "preview")

        def fail(_conn, _cursor, statement, _params, _context, _many):
            if statement.startswith("INSERT INTO workspace_tree_exchange_model_sources"):
                raise RuntimeError("原来源绑定失败")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError):
                await store.preview(scope, binding, request, change, model_source=source)
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await counts(store.database) == [0, 0, 0, 0]
        proposal, _ = await store.preview(scope, binding, request, change, model_source=source)
        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError):
                await store.reserve(
                    scope,
                    action(proposal.view),
                    binding.fingerprint,
                    change,
                    model_source=TreeExchangeModelSource(run.id, "apply"),
                )
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await counts(store.database) == [1, 0, 0, 1]
        assert (await store.proposal(scope, proposal.view.proposal_id)).view.revision == 0
    finally:
        await service.close()


async def test_corrupt_request_payload_half_after_and_terminal_overwrite_are_denied(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        async with store.database.sessions.begin() as sql:
            (
                await sql.get(WorkspaceTreeExchangeProposalRow, proposal.view.proposal_id)
            ).payload = b"damaged"
        with pytest.raises(ServiceError):
            await store.proposal(scope, proposal.view.proposal_id)
        async with store.database.sessions.begin() as sql:
            (
                await sql.get(WorkspaceTreeExchangeProposalRow, proposal.view.proposal_id)
            ).payload = encode_exchange(change)[1]
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        effect = await publish(store, writer, scope, attempt)
        with pytest.raises(ServiceError):
            await store.settle(
                scope, attempt.view.operation_id, replace(effect, observed_right=None)
            )
        assert await counts(store.database) == [1, 1, 9, 0]
        done = await store.settle(scope, attempt.view.operation_id, effect)
        with pytest.raises(ServiceError) as closed:
            await store.settle(
                scope,
                attempt.view.operation_id,
                replace(effect, receipt=replace(effect.receipt, changed=False)),
            )
        assert closed.value.code == "workspace_tree_exchange_resolved"
        assert await store.operation(scope, attempt.view.operation_id) == done
        async with store.database.sessions.begin() as sql:
            row = await sql.get(WorkspaceTreeExchangeProposalRow, proposal.view.proposal_id)
            row.request = {**copy.deepcopy(row.request), "expected_right": "0" * 64}
        with pytest.raises(ServiceError):
            await store.proposal(scope, proposal.view.proposal_id)
    finally:
        await service.close()


async def test_quota_foreign_scope_and_lock_loss_fail_closed(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        with pytest.raises(ServiceError) as quota:
            await WorkspaceTreeExchangeStore(
                service.workspace_edits, max_snapshot_bytes=1024
            ).preview(scope, binding, request, change)
        assert quota.value.code == "workspace_tree_exchange_snapshot_limit" and await counts(
            store.database
        ) == [0, 0, 0, 0]
        proposal, _ = await store.preview(scope, binding, request, change)
        other = await service.spawn("default")
        with pytest.raises(ServiceError) as foreign:
            await store.proposal("session:" + other.session_id, proposal.view.proposal_id)
        assert foreign.value.code == "workspace_tree_exchange_not_found"
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        async with store.database.sessions.begin() as sql:
            locks = list(await sql.scalars(select(WorkspaceTreeExchangeLockRow)))
            await sql.delete(locks[0])
        with pytest.raises(ServiceError) as lost:
            await store.assert_execution(scope, attempt.view.operation_id)
        assert lost.value.code == "workspace_tree_exchange_corrupt"
        effect = await publish(store, writer, scope, attempt)
        assert effect.receipt.status == "rejected" and not effect.receipt.changed
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["null-source", "bad-parent", "half-source", "half-effect"])
async def test_sql_fk_null_and_pair_constraints_reject_invalid_origin_or_effect(
    runtime, tmp_path, fault
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        run = await start_run(runtime, service, agent, store)
        proposal, _ = await store.preview(
            scope, binding, request, change, model_source=TreeExchangeModelSource(run.id, "preview")
        )
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        other, _ = await store.preview(
            scope, binding, request.model_copy(update={"request_key": "other"}), change
        )
        with pytest.raises(IntegrityError):
            async with store.database.sessions.begin() as sql:
                if fault == "null-source":
                    await sql.execute(update(WorkspaceTreeExchangeModelSourceRow).values(kind=None))
                elif fault == "bad-parent":
                    sql.add(
                        WorkspaceTreeExchangeModelSourceRow(
                            run_id=run.id,
                            call_id="action",
                            proposal_id=other.view.proposal_id,
                            kind="apply",
                            operation_id=attempt.view.operation_id,
                        )
                    )
                elif fault == "half-source":
                    await sql.execute(
                        update(WorkspaceTreeExchangeProposalRow)
                        .where(
                            WorkspaceTreeExchangeProposalRow.proposal_id
                            == proposal.view.proposal_id
                        )
                        .values(call_id=None)
                    )
                else:
                    await sql.execute(
                        update(WorkspaceTreeExchangeAttemptRow).values(after_payload=b"half")
                    )
        assert await store.proposal(scope, proposal.view.proposal_id)
    finally:
        await service.close()


@pytest.mark.parametrize("neighbor", ["file", "directory", "move"])
async def test_old_writer_last_authority_rechecks_exchange_locks_after_admission(
    runtime, tmp_path, neighbor
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        accepted, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        # 合成锁丢失/恢复，模拟准入后新围栏出现；两原Writer最后资格仍须拒绝。
        async with store.database.sessions.begin() as sql:
            locks = [
                (row.kind, row.lock_key)
                for row in await sql.scalars(select(WorkspaceTreeExchangeLockRow))
            ]
            await sql.execute(delete(WorkspaceTreeExchangeLockRow))
        if neighbor == "file":
            adjacent = service.workspace_edits
            adjacent_writer = WorkspaceWriter(binding.workspace)
            item = adjacent_writer.prepare(
                FileChangeSpec(
                    path="left/sub/new.txt", operation="write", content="new", expected_sha256=None
                )
            )
            view, _ = await adjacent.preview(scope, binding.key, binding.fingerprint, "late", item)
            spec = WorkspaceChangeAction(
                proposal_id=view.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="late",
            )
        elif neighbor == "directory":
            adjacent = service.manager.directory_changes.store
            adjacent_writer = WorkspaceDirectoryWriter(binding.workspace)
            item = adjacent_writer.prepare(
                DirectoryChangeSpec(path="right/sub/newdir", operation="create", mode=0o750)
            )
            view, _ = await adjacent.preview(scope, binding.key, binding.fingerprint, "late", item)
            spec = DirectoryChangeAction(
                proposal_id=view.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="late",
            )
        else:
            adjacent = WorkspaceTreeMoveStore(service.workspace_edits)
            adjacent_writer = WorkspaceTreeWriter(binding.workspace)
            snapshot = adjacent_writer.reader.capture("right/sub")
            item = adjacent_writer.prepare(
                snapshot.root_path, "out", expected_fingerprint=snapshot.fingerprint
            )
            view, _ = await adjacent.preview(
                scope,
                binding,
                TreeMovePreviewRequest(
                    source_path=snapshot.root_path,
                    target_path="out",
                    expected_fingerprint=snapshot.fingerprint,
                    request_key="late",
                ),
                item,
            )
            spec = TreeMoveAction(
                proposal_id=view.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="late",
            )
        admitted, _ = await adjacent.reserve(scope, spec, binding.fingerprint, item)
        async with store.database.sessions.begin() as sql:
            for kind, key in locks:
                sql.add(
                    WorkspaceTreeExchangeLockRow(
                        operation_id=accepted.view.operation_id, kind=kind, lock_key=key
                    )
                )
        loop = asyncio.get_running_loop()

        def authorize():
            asyncio.run_coroutine_threadsafe(
                adjacent.assert_execution(scope, admitted.view.operation_id), loop
            ).result(timeout=5)

        blocked = await asyncio.to_thread(adjacent_writer.apply, item, authorize=authorize)
        assert blocked.receipt.status == "rejected" and not blocked.receipt.changed
        assert blocked.receipt.error_code == "workspace_tree_exchange_busy"
        exchange_blocked = await publish(store, writer, scope, accepted)
        assert (
            exchange_blocked.receipt.status == "rejected" and not exchange_blocked.receipt.changed
        )
        assert (tmp_path / "left/sub/data.bin").read_bytes() == b"old\xff\0"
        assert (tmp_path / "right/sub/data.bin").read_bytes() == b"new\0\xff"
        assert not (tmp_path / "left/sub/new.txt").exists()
        assert not (tmp_path / "right/sub/newdir").exists()
        assert not (tmp_path / "out").exists()
    finally:
        await service.close()


async def test_cold_undo_material_must_still_match_original_actual_root_versions(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        apply, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        effect = await publish(store, writer, scope, apply)
        await store.settle(scope, apply.view.operation_id, effect)
        current = await store.proposal(scope, proposal.view.proposal_id)
        reverse = writer.prepare_undo(change, effect)
        undo, _ = await store.reserve(
            scope, action(current.view, "undo", "undo"), binding.fingerprint, reverse
        )
        root = reverse.left.entries[0]
        changed_root = replace(
            root, directory=replace(root.directory, ctime_ns=root.directory.ctime_ns + 1)
        )
        altered = replace(
            reverse, left=replace(reverse.left, entries=(changed_root, *reverse.left.entries[1:]))
        )
        metadata, blob = encode_exchange(altered)
        async with store.database.sessions.begin() as sql:
            row = await sql.get(WorkspaceTreeExchangeAttemptRow, undo.view.operation_id)
            row.prepared, row.payload = metadata, blob
        with pytest.raises(ServiceError) as substitution:
            await store.operation(scope, undo.view.operation_id)
        assert substitution.value.code == "workspace_tree_exchange_corrupt"
        assert (tmp_path / "left/sub/data.bin").read_bytes() == b"new\0\xff"
    finally:
        await service.close()


async def test_settlement_quota_failure_preserves_unknown_locks_and_original_operation(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        apply, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        effect = await publish(store, writer, scope, apply)
        original_limit = store.max_snapshot_bytes
        store.max_snapshot_bytes = 1024
        with pytest.raises(ServiceError) as exhausted:
            await store.settle(scope, apply.view.operation_id, effect)
        assert exhausted.value.code == "workspace_tree_exchange_snapshot_limit"
        unknown = await store.mark_uncertain(scope, apply.view.operation_id)
        assert (
            unknown.view.status == "uncertain"
            and unknown.intent_recorded
            and unknown.effect is None
        )
        assert await counts(store.database) == [1, 1, 9, 0]
        store.max_snapshot_bytes = original_limit
        result = await store.settle(
            scope, apply.view.operation_id, writer.reconcile(change), resolving=True
        )
        assert result.view.status == "done" and result.view.operation_id == apply.view.operation_id
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        await service.close()


async def test_changed_key_stale_cas_ttl_and_second_exchange_never_open_second_attempt(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        with pytest.raises(ServiceError) as changed:
            await store.preview(
                scope, binding, request.model_copy(update={"expected_right": "0" * 64}), None
            )
        assert changed.value.code == "workspace_tree_exchange_idempotency_conflict"
        with pytest.raises(ServiceError) as stale:
            await store.reserve(
                scope,
                action(proposal.view).model_copy(update={"expected_revision": 1}),
                binding.fingerprint,
                change,
            )
        assert stale.value.code == "workspace_revision_conflict"
        other, _ = await store.preview(
            scope, binding, request.model_copy(update={"request_key": "second"}), change
        )
        accepted, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        with pytest.raises(ServiceError) as busy:
            await store.reserve(
                scope, action(other.view, key="second"), binding.fingerprint, change
            )
        assert busy.value.code == "workspace_tree_exchange_busy"
        result = await store.settle(
            scope, accepted.view.operation_id, await publish(store, writer, scope, accepted)
        )
        assert result.view.status == "done"
        async with store.database.sessions.begin() as sql:
            (await sql.get(WorkspaceTreeExchangeProposalRow, other.view.proposal_id)).expires_at = (
                time.time() - 1
            )
        with pytest.raises(ServiceError) as expired:
            await store.reserve(
                scope, action(other.view, key="expired"), binding.fingerprint, change
            )
        assert expired.value.code == "workspace_proposal_expired"
        assert await counts(store.database) == [2, 1, 0, 0]
    finally:
        await service.close()
