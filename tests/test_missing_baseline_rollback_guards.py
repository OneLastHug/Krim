"""独立来源的未完成拒绝、SQL原子性、分页、模型绑定和最后租约。"""

import asyncio
import time
from dataclasses import replace

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_baseline_rollback_store import preview
from test_missing_baseline_rollback_store import rollback_origin as rollback_origin
from test_missing_directory_restore_model_sources import model_missing as model_missing
from test_missing_directory_restore_store import apply, prepared, ready
from test_missing_directory_restore_store import missing as missing
from test_model_directory_checkpoint_store import context as context
from test_workspace_tree_move_ledger import action, publish

from krim_agent.persistence.database import (
    Database,
    WorkspaceMissingDirectoryRollbackRow,
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveLockRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreeMoveProposalRow,
)
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_missing_directory_rollback import MissingDirectoryRollbackSource
from krim_agent.workspace_tree_move import TreeMoveModelSource, TreeMovePreviewRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


@pytest.fixture
async def model_rollback_origin(model_missing):
    restores, scope, binding, *_ = model_missing
    planned, _ = await ready(model_missing)
    _, fence, writer, change, authorize = await prepared(model_missing, planned)
    effect = await apply(model_missing, fence, writer, change, authorize)
    assert effect.receipt.status == "done"
    finished = await restores.settle(scope, fence, binding, effect, qualify=lambda: None)
    return WorkspaceTreeMoveStore(restores.edits), scope, binding, finished, effect


async def counts(store):
    async with store.database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceTreeMoveProposalRow,
                WorkspaceMissingDirectoryRollbackRow,
                WorkspaceTreeMoveAttemptRow,
                WorkspaceTreeMoveLockRow,
                WorkspaceTreeMoveModelSourceRow,
            )
        ]


@pytest.mark.parametrize("phase", ["ready", "running", "uncertain"])
async def test_unfinished_or_unknown_missing_origin_cannot_authorize_rollback(missing, phase):
    restores, scope, binding, snapshot, *_ = missing
    planned, _ = await ready(missing)
    if phase != "ready":
        _, fence, _, change, _ = await prepared(missing, planned)
        if phase == "uncertain":
            await restores.record_intent(scope, fence, binding, change, qualify=lambda: None)
            await restores.fail(scope, fence, "synthetic_unknown")
    moves = WorkspaceTreeMoveStore(restores.edits)
    request = TreeMovePreviewRequest(
        source_path="tree",
        target_path="retained",
        expected_fingerprint=snapshot.fingerprint,
        request_key="incomplete-origin",
    )
    with pytest.raises(ServiceError):
        await moves.preview(
            scope,
            binding,
            request,
            None,
            missing_origin=MissingDirectoryRollbackSource(planned.plan_id),
        )
    assert await counts(moves) == [0, 0, 0, 0, 0]
    assert not (binding.workspace.root / "retained").exists()


async def test_source_insert_failure_rolls_back_all_original_rows(rollback_origin):
    store, *_ = rollback_origin

    def fail(_conn, _cursor, statement, _params, _context, _many):
        if statement.startswith("INSERT INTO workspace_missing_directory_rollbacks"):
            raise OSError("synthetic rollback source insert failure")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(OSError, match="source insert failure"):
            await preview(rollback_origin)
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
    assert await counts(store) == [0, 0, 0, 0, 0]
    assert store.database.engine.sync_engine.pool.checkedout() == 0


async def test_committed_preview_reply_loss_reads_original_key_without_capture(
    rollback_origin, monkeypatch
):
    store, scope, binding, original, _ = rollback_origin

    def lost(_sql):
        raise RuntimeError("synthetic committed preview reply loss")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(RuntimeError, match="committed preview reply loss"):
            await preview(rollback_origin)
    finally:
        event.remove(Session, "after_commit", lost)
    assert store.database.engine.sync_engine.pool.checkedout() == 0, "提交丢回程也必须归还连接"
    stored = await store.proposal_by_key(scope, "rollback-preview")
    request = TreeMovePreviewRequest(
        source_path=stored.view.source_path,
        target_path=stored.view.target_path,
        expected_fingerprint=stored.view.before_fingerprint,
        request_key="rollback-preview",
    )

    def forbidden(*_args, **_kwargs):
        pytest.fail("原键禁止重新捕获或发布")

    monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
    assert await store.preview(
        scope,
        binding,
        request,
        None,
        missing_origin=MissingDirectoryRollbackSource(original.plan_id),
    ) == (stored, False)
    assert await counts(store) == [1, 1, 0, 0, 0]


@pytest.mark.parametrize("phase", ["reserve", "intent", "settle"])
async def test_each_committed_action_reply_loss_closes_connection_without_replay(
    rollback_origin, monkeypatch, phase
):
    store, scope, binding, *_ = rollback_origin
    writer, _, change, proposal = await preview(rollback_origin)
    request = action(proposal.view)
    attempt = effect = None
    if phase != "reserve":
        attempt, _ = await store.reserve(scope, request, binding.fingerprint, change)
        if phase == "settle":
            effect = await publish(store, writer, scope, attempt)
            assert effect.receipt.status == "done"

    def lost(_sql):
        raise RuntimeError("synthetic committed action reply loss")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(RuntimeError, match="committed action reply loss"):
            if phase == "reserve":
                await store.reserve(scope, request, binding.fingerprint, change)
            elif phase == "intent":
                await store.record_intent(scope, attempt.view.operation_id, change)
            else:
                await store.settle(scope, attempt.view.operation_id, effect)
    finally:
        event.remove(Session, "after_commit", lost)
    assert store.database.engine.sync_engine.pool.checkedout() == 0

    def forbidden(*_args, **_kwargs):
        pytest.fail("提交已成功，必须读取原事实而不能重发副作用")

    monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
    actual = await store.operation_by_key(scope, request.request_key)
    assert actual.view.status == ("done" if phase == "settle" else "running")
    assert actual.intent_recorded == (phase != "reserve")
    assert await store.reserve(scope, request, binding.fingerprint, change) == (actual, False)
    assert store.database.engine.sync_engine.pool.checkedout() == 0


async def test_two_real_connections_share_one_preview_cas_and_missing_link(rollback_origin):
    store, scope, binding, original, _ = rollback_origin
    writer = WorkspaceTreeWriter(binding.workspace)
    current = writer.reader.capture("tree")
    change = writer.prepare("tree", "retained", expected_fingerprint=current.fingerprint)
    request = TreeMovePreviewRequest(
        source_path="tree",
        target_path="retained",
        expected_fingerprint=current.fingerprint,
        request_key="concurrent-preview",
    )
    database = Database(str(store.database.engine.url))
    second = WorkspaceTreeMoveStore(WorkspaceEditStore(database, owner_id=store.owner_id))
    try:
        proposals = await asyncio.gather(
            *(
                selected.preview(
                    scope,
                    binding,
                    request,
                    change,
                    missing_origin=MissingDirectoryRollbackSource(original.plan_id),
                )
                for selected in (store, second)
            )
        )
        assert sum(created for _, created in proposals) == 1
        assert proposals[0][0] == proposals[1][0]
        proposal = proposals[0][0]
        attempts = await asyncio.gather(
            *(
                selected.reserve(scope, action(proposal.view), binding.fingerprint, change)
                for selected in (store, second)
            )
        )
        assert sum(created for _, created in attempts) == 1
        assert attempts[0][0] == attempts[1][0]
        assert (await counts(store))[:3] == [1, 1, 1]
        effect = await publish(store, writer, scope, attempts[0][0])
        assert effect.receipt.status == "done"
        await store.settle(scope, attempts[0][0].view.operation_id, effect)
        assert (
            await store.missing_rollback_proposal(scope, proposal.view.proposal_id)
        ).rollback_state == "preserved"
    finally:
        await database.close()


@pytest.mark.parametrize("fault", ["row", "row-and-markers"])
async def test_model_preview_source_cannot_be_dropped_to_host(
    model_rollback_origin, context, fault
):
    store, scope, *_ = model_rollback_origin
    source = TreeMoveModelSource(context[4].run_id, "rollback-preview-模型")
    _, _, _, proposal = await preview(model_rollback_origin, model_source=source)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceTreeMoveModelSourceRow, (source.run_id, source.call_id))
        await sql.delete(row)
        if fault == "row-and-markers":
            original = await sql.get(WorkspaceTreeMoveProposalRow, proposal.view.proposal_id)
            original.missing_restore_plan_id = original.missing_restore_hash = None
            await sql.delete(
                await sql.get(WorkspaceMissingDirectoryRollbackRow, proposal.view.proposal_id)
            )
    with pytest.raises(ServiceError):
        await store.proposal(scope, proposal.view.proposal_id)
    with pytest.raises(ServiceError):
        await store.missing_rollback_list(scope)


async def test_model_same_run_call_conflict_and_closed_run_cold_history(
    model_rollback_origin, context, monkeypatch
):
    from krim_agent.persistence.database import RunRow

    store, scope, binding, original, _ = model_rollback_origin
    source = TreeMoveModelSource(context[4].run_id, "preview-source")
    _, request, change, proposal = await preview(model_rollback_origin, model_source=source)
    with pytest.raises(ServiceError) as denied:
        await preview(model_rollback_origin, key="another", model_source=source)
    assert denied.value.code == "workspace_tree_move_call_conflict"
    async with store.database.sessions() as sql, sql.begin():
        run = await sql.get(RunRow, source.run_id)
        run.status, run.stop_requested = "aborted", True
    assert (await store.proposal(scope, proposal.view.proposal_id)) == proposal
    assert await store.preview(
        scope,
        binding,
        request,
        None,
        model_source=source,
        missing_origin=MissingDirectoryRollbackSource(original.plan_id),
    ) == (proposal, False)
    with pytest.raises(ServiceError):
        await store.reserve(
            scope,
            action(proposal.view),
            binding.fingerprint,
            change,
            model_source=replace(source, call_id="apply-source"),
        )


@pytest.mark.parametrize("drop_markers", [False, True])
async def test_model_action_source_cannot_be_erased_to_borrow_host_qualification(
    model_rollback_origin, context, drop_markers
):
    store, scope, binding, *_ = model_rollback_origin
    _, _, change, proposal = await preview(model_rollback_origin)
    source = TreeMoveModelSource(context[4].run_id, "original-rollback-execute")
    attempt, _ = await store.reserve(
        scope, action(proposal.view), binding.fingerprint, change, model_source=source
    )
    async with store.database.sessions() as sql, sql.begin():
        await sql.delete(
            await sql.get(WorkspaceTreeMoveModelSourceRow, (source.run_id, source.call_id))
        )
        if drop_markers:
            row = await sql.get(WorkspaceTreeMoveAttemptRow, attempt.view.operation_id)
            row.run_id = row.call_id = None
    with pytest.raises(ServiceError):
        await store.assert_execution(scope, attempt.view.operation_id)
    with pytest.raises(ServiceError):
        await store.operation(scope, attempt.view.operation_id)


async def test_host_cold_key_keeps_original_model_action_without_live_run_or_replay(
    model_rollback_origin, context, monkeypatch
):
    from krim_agent.persistence.database import RunRow

    store, scope, binding, *_ = model_rollback_origin
    _, _, change, proposal = await preview(model_rollback_origin)
    source = TreeMoveModelSource(context[4].run_id, "original-model-execution")
    request = action(proposal.view)
    attempt, _ = await store.reserve(
        scope, request, binding.fingerprint, change, model_source=source
    )
    async with store.database.sessions() as sql, sql.begin():
        run = await sql.get(RunRow, source.run_id)
        run.status, run.stop_requested = "aborted", True

    def forbidden(*_args, **_kwargs):
        pytest.fail("冷键不能重发Writer")

    monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
    monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
    cold = type(store)(store.files)
    assert await cold.find_action(scope, request, binding.fingerprint) == attempt
    assert await cold.reserve(scope, request, binding.fingerprint, change) == (attempt, False)
    assert (await cold.operation_by_key(scope, request.request_key)).model_source == source
    with pytest.raises(ServiceError):
        await cold.assert_execution(scope, attempt.view.operation_id)


@pytest.mark.parametrize("phase", ["authorize", "intent", "renew", "settle"])
async def test_lease_expiring_during_last_lock_query_cannot_grant_execution(
    rollback_origin, monkeypatch, phase
):
    store, scope, binding, *_ = rollback_origin
    writer, _, change, proposal = await preview(rollback_origin)
    attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
    effect = await publish(store, writer, scope, attempt) if phase == "settle" else None
    identifier = attempt.view.operation_id
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceTreeMoveAttemptRow, identifier)
        deadline = time.time() + 0.4
        row.lease_expires_at = deadline
    original = store._locks

    async def delayed(*args, **kwargs):
        await original(*args, **kwargs)
        await asyncio.sleep(max(0, deadline - time.time()) + 0.05)

    monkeypatch.setattr(store, "_locks", delayed)
    operation = {
        "authorize": lambda: store.assert_execution(scope, identifier),
        "intent": lambda: store.record_intent(scope, identifier, change),
        "renew": lambda: store.renew(scope, identifier),
        "settle": lambda: store.settle(scope, identifier, effect),
    }[phase]
    with pytest.raises(ServiceError) as denied:
        await operation()
    assert denied.value.code == "workspace_tree_move_fenced"
    async with store.database.sessions() as sql:
        row = await sql.get(WorkspaceTreeMoveAttemptRow, identifier)
        assert row.lease_expires_at == deadline
        assert row.status == "running"
        assert row.intent_recorded == (phase == "settle")
