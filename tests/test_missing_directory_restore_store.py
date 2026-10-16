"""真实缺失基线、单次CAS发布、原Owner、unknown冷拒绝与SQL原子事实。"""

import asyncio
import time

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import bridge
from test_directory_restore_plans import target

import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreAttemptRow as Attempt,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreLockRow as Lock,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestorePlanRow as Plan,
)
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_directory_restore import DirectoryRestoreTarget
from krim_agent.workspace_missing_directory_restore import (
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestorePreview,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


def preview(identifier, key="missing-preview", *, kind="checkpoint"):
    return MissingDirectoryRestorePreview(
        target=DirectoryRestoreTarget(kind=kind, identifier=identifier), request_key=key
    )


def execute(plan, key="missing-execute"):
    return MissingDirectoryRestoreExecute(
        plan_id=plan.plan_id, expected_revision=plan.revision, request_key=key
    )


@pytest.fixture
async def missing(capture_context):
    captures, scope, binding, snapshot, _ = capture_context
    identifier = await target(capture_context)
    # 合成原树完整保留；之后实际观察的目标确实缺失，绝非伪造零节点快照。
    (binding.workspace.root / "tree").rename(binding.workspace.root / "retained-original")
    store = WorkspaceMissingDirectoryRestoreStore(captures)
    with WorkspaceDirectoryAbsenceReader(binding.workspace) as reader:
        yield store, scope, binding, snapshot, identifier, reader


async def ready(context, *, source=None, key="missing-preview"):
    store, scope, binding, _, identifier, reader = context
    planned, created = await store.reserve(
        scope, binding, preview(identifier, key), model_source=source, qualify=lambda: None
    )
    assert created and planned.baseline is None and planned.revision == 0
    fence = await store.preparation(scope, planned.plan_id)
    before = reader.capture(planned.root_path)
    await store.record_absence(
        scope, fence, binding, before, observation_reader=reader, qualify=lambda: None
    )
    planned = await store.ready(scope, fence, binding, qualify=lambda: None)
    assert planned.baseline.kind == "absent" and planned.baseline.fingerprint == before.fingerprint
    return planned, before


async def prepared(context, planned, *, source=None):
    store, scope, binding, _, _, reader = context
    operation, created = await store.start(
        scope, binding, execute(planned), model_source=source, qualify=lambda: None
    )
    assert created and not operation.intent_recorded
    fence = await store.execution(scope, operation.operation_id)
    wanted, before, token = await store.inputs(scope, fence, binding, qualify=lambda: None)
    writer = WorkspaceMissingDirectoryRestoreWriter(binding.workspace, reader)
    loop = asyncio.get_running_loop()
    authorize = bridge(
        loop, lambda: store.assert_execution(scope, fence, binding, qualify=lambda: None)
    )
    record = bridge(
        loop, lambda value: store.record_stage(scope, fence, binding, value, qualify=lambda: None)
    )
    change = await asyncio.to_thread(
        writer.prepare, wanted, before, token=token, record_stage=record, authorize=authorize
    )
    await store.record_prepared(scope, fence, binding, change, qualify=lambda: None)
    return operation, fence, writer, change, authorize


async def apply(context, fence, writer, change, authorize):
    store, scope, binding, *_ = context
    record = bridge(
        asyncio.get_running_loop(),
        lambda value: store.record_intent(scope, fence, binding, value, qualify=lambda: None),
    )
    return await asyncio.to_thread(writer.apply, change, record_intent=record, authorize=authorize)


async def counts(store):
    async with store.database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(row)) for row in (Plan, Attempt, Lock)
        ]


def forbidden(*_args, **_kwargs):
    pytest.fail("历史/原键/核实不得再次捕获、准备或发布")


async def test_real_missing_publication_and_cold_queries_keep_one_original_fact(
    missing, monkeypatch
):
    store, scope, binding, snapshot, identifier, _ = missing
    planned, before = await ready(missing)
    assert planned.phase == "ready" and planned.revision == 1
    assert not (binding.workspace.root / "tree").exists()
    diff = await store.diff(scope, planned.plan_id)
    assert len(diff) == len(snapshot.entries)
    assert all(item.operation == "create" and item.before is None for item in diff)
    assert all(
        value not in planned.model_dump_json() for value in ("observation_id", "inode", "token")
    )
    operation, fence, writer, change, authorize = await prepared(missing, planned)
    assert (await counts(store))[:2] == [1, 1]
    effect = await apply(missing, fence, writer, change, authorize)
    assert effect.receipt.status == "done", effect
    finished = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert (
        finished.phase == "finished"
        and finished.receipt.before_absence_fingerprint == before.fingerprint
    )
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    assert (await counts(store))[2] == 0
    async with store.database.sessions() as sql:
        row = await sql.get(Plan, planned.plan_id)
        assert row.staging_reserved_bytes == 0 and row.staging_reserved_entries == 1
        assert row.cleanup_pending
    original_op = await store.operation(scope, operation.operation_id)
    for model, method in (
        (WorkspaceDirectoryAbsenceReader, "capture"),
        (WorkspaceTreeSnapshotReader, "capture"),
        (WorkspaceMissingDirectoryRestoreWriter, "prepare"),
        (WorkspaceMissingDirectoryRestoreWriter, "apply"),
    ):
        monkeypatch.setattr(model, method, forbidden)
    cold = WorkspaceMissingDirectoryRestoreStore(store.captures)
    binding.revoked.set()
    assert await cold.get(scope, planned.plan_id) == finished
    assert await cold.by_key(scope, planned.request_key) == finished
    assert await cold.list(scope) == [finished]
    assert await cold.operation_by_key(scope, "missing-execute") == original_op
    assert await cold.reserve(scope, binding, preview(identifier), qualify=forbidden) == (
        finished,
        False,
    )
    assert await cold.start(scope, binding, execute(planned), qualify=forbidden) == (
        original_op,
        False,
    )
    assert await cold.cancel(scope, planned.plan_id) == finished
    with pytest.raises(ServiceError):
        await cold.execution(scope, operation.operation_id)


async def test_ready_loses_original_observer_on_new_instance_without_recapture(
    missing, monkeypatch
):
    store, scope, binding, _, identifier, _ = missing
    planned, _ = await ready(missing)
    monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
    monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
    cold = WorkspaceMissingDirectoryRestoreStore(store.captures)
    result = await cold.get(scope, planned.plan_id)
    assert result.phase == "interrupted" and result.baseline == planned.baseline
    assert await cold.reserve(scope, binding, preview(identifier), qualify=forbidden) == (
        result,
        False,
    )
    with pytest.raises(ServiceError):
        await cold.start(scope, binding, execute(planned), qualify=lambda: None)
    assert (await counts(store)) == [1, 0, 0]


@pytest.mark.parametrize("cold", [False, True])
async def test_unknown_same_operation_verify_never_republishes_and_cold_stays_unknown(
    missing, monkeypatch, cold
):
    store, scope, binding, *_ = missing
    planned, _ = await ready(missing)
    operation, fence, writer, change, authorize = await prepared(missing, planned)
    rename = physical.rename_at
    calls = []

    def lost(*args, **kwargs):
        rename(*args, **kwargs)
        calls.append(True)
        raise OSError("原rename后丢回程")

    monkeypatch.setattr(physical, "rename_at", lost)
    effect = await apply(missing, fence, writer, change, authorize)
    assert effect.receipt.status == "uncertain" and calls == [True]
    unknown = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert unknown.phase == "uncertain" and (await counts(store))[2] > 0
    selected = WorkspaceMissingDirectoryRestoreStore(store.captures) if cold else store
    _, verify = await selected.claim_verify(
        scope, operation.operation_id, binding, qualify=lambda: None
    )
    assert verify.kind == "verify" and verify.operation_id == operation.operation_id
    for refused in (fence, verify):
        with pytest.raises(ServiceError):
            await selected.record_intent(scope, refused, binding, change, qualify=lambda: None)
    actual = await selected.verification_input(scope, verify, binding, qualify=lambda: None)
    selected_writer = (
        WorkspaceMissingDirectoryRestoreWriter(binding.workspace, missing[-1]) if cold else writer
    )
    monkeypatch.setattr(physical, "rename_at", forbidden)
    monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
    result = await asyncio.to_thread(selected_writer.reconcile, actual)
    settled = await selected.settle(scope, verify, binding, result, qualify=lambda: None)
    assert settled.phase == ("uncertain" if cold else "finished")
    assert settled.operation_id == operation.operation_id and calls == [True]
    assert ((await counts(store))[2] > 0) == cold


@pytest.mark.parametrize("stage", ["reserve", "start", "intent", "settle"])
async def test_committed_sql_reply_loss_reads_same_original_key_without_replay(missing, stage):
    store, scope, binding, _, identifier, _ = missing
    planned = operation = fence = writer = change = authorize = effect = None
    if stage != "reserve":
        planned, _ = await ready(missing)
    if stage in {"intent", "settle"}:
        operation, fence, writer, change, authorize = await prepared(missing, planned)
    if stage == "settle":
        effect = await apply(missing, fence, writer, change, authorize)

    def lost(_sql):
        raise RuntimeError("原SQL已提交，仅丢回程")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(RuntimeError, match="原SQL已提交"):
            if stage == "reserve":
                await store.reserve(scope, binding, preview(identifier), qualify=lambda: None)
            elif stage == "start":
                await store.start(scope, binding, execute(planned), qualify=lambda: None)
            elif stage == "intent":
                await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
            else:
                await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    finally:
        event.remove(Session, "after_commit", lost)
    assert store.database.engine.sync_engine.pool.checkedout() == 0
    result = await store.by_key(scope, "missing-preview")
    assert (
        result.phase
        == {
            "reserve": "preparing",
            "start": "executing",
            "intent": "executing",
            "settle": "finished",
        }[stage]
    )
    if stage == "intent":
        assert (await store.operation(scope, operation.operation_id)).intent_recorded
        with pytest.raises(ServiceError):
            await store.execution(scope, operation.operation_id)
    assert (await counts(store))[:2] == [1, int(stage != "reserve")]


async def test_two_connections_start_cas_once_and_foreign_owner_never_gets_execute_fence(missing):
    store, scope, binding, *_ = missing
    planned, _ = await ready(missing)
    other_database = Database(str(store.database.engine.url))
    other = WorkspaceMissingDirectoryRestoreStore(
        WorkspaceDirectoryCaptureStore(
            WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(other_database))
        )
    )
    try:
        # 两个独立Database真实竞争：异Owner只能冷读已准入事实，不能借活观察准入。
        results = await asyncio.gather(
            store.start(scope, binding, execute(planned), qualify=lambda: None),
            other.start(scope, binding, execute(planned), qualify=lambda: None),
            return_exceptions=True,
        )
        assert not isinstance(results[0], BaseException) and results[0][1] is True
        if isinstance(results[1], ServiceError):
            assert results[1].code == "workspace_missing_directory_restore_observation_lost"
        else:
            assert results[1] == (results[0][0], False)
        assert await store.start(scope, binding, execute(planned), qualify=lambda: None) == (
            results[0][0],
            False,
        )
        with pytest.raises(ServiceError):
            await other.execution(scope, results[0][0].operation_id)
        assert (await counts(store))[:2] == [1, 1]
    finally:
        await other_database.close()


@pytest.mark.parametrize("after_intent", [False, True])
async def test_cancel_closes_original_admission_and_keeps_intent_unknown(missing, after_intent):
    store, scope, binding, *_ = missing
    planned, _ = await ready(missing)
    operation, fence, _, change, _ = await prepared(missing, planned)
    if after_intent:
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    cancelled = await store.cancel(scope, planned.plan_id)
    assert cancelled.phase == ("uncertain" if after_intent else "cancelled")
    assert cancelled.cancel_requested and cancelled.cleanup_pending
    assert (await store.operation(scope, operation.operation_id)).intent_recorded == after_intent
    with pytest.raises(ServiceError):
        await store.record_prepared(scope, fence, binding, change, qualify=lambda: None)
    assert await store.cancel(scope, planned.plan_id) == cancelled


@pytest.mark.parametrize(
    "fault", ["request", "absence", "owner", "execute", "stage", "payload", "effect"]
)
async def test_strict_original_hash_material_links_never_become_success_on_corruption(
    missing, fault
):
    store, scope, binding, *_ = missing
    planned, _ = await ready(missing)
    operation, fence, writer, change, authorize = await prepared(missing, planned)
    if fault == "effect":
        effect = await apply(missing, fence, writer, change, authorize)
        await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(Plan, planned.plan_id)
        if fault == "request":
            row.request_hash = "f" * 64
        elif fault == "absence":
            row.absence_fingerprint = "f" * 64
        elif fault == "owner":
            row.observation_owner_id = "changed-owner"
        elif fault == "execute":
            row.execute_hash = "f" * 64
        elif fault == "stage":
            row.stage_metadata = dict(row.stage_metadata, token="f" * 32)
        elif fault == "payload":
            row.prepared_payload += b"bad"
        else:
            attempt = await sql.get(Attempt, operation.operation_id)
            attempt.effect_metadata = dict(attempt.effect_metadata, private_token="bad")
    with pytest.raises(ServiceError) as denied:
        await store.get(scope, planned.plan_id)
    assert denied.value.code == "workspace_missing_directory_restore_corrupt"


@pytest.mark.parametrize(
    "kind,fault", [(k, f) for k in ("prepare", "execute") for f in ("lease", "qualification")]
)
async def test_projection_rechecks_original_lease_and_qualification(
    missing, monkeypatch, kind, fault
):
    store, scope, binding, _, identifier, _ = missing
    if kind == "prepare":
        planned, _ = await store.reserve(scope, binding, preview(identifier), qualify=lambda: None)
        fence = await store.preparation(scope, planned.plan_id)
        table, identifier = Plan, planned.plan_id
    else:
        planned, _ = await ready(missing)
        operation, fence, *_ = await prepared(missing, planned)
        table, identifier = Attempt, operation.operation_id
    deadline = time.time() + 0.3
    if fault == "lease":
        async with store.database.sessions() as sql, sql.begin():
            row = await sql.get(table, identifier)
            row.lease_expires_at = deadline
    view, projected = store._view, []

    async def delayed(*args, **kwargs):
        result = await view(*args, **kwargs)
        projected.append(True)
        if fault == "lease":
            await asyncio.sleep(max(0, deadline - time.time()) + 0.01)
        return result

    def qualify():
        if projected and fault == "qualification":
            raise ServiceError("original_parent_revoked", "原父资格已撤销", 403)

    monkeypatch.setattr(store, "_view", delayed)
    read = store.preparation_view if kind == "prepare" else store.execution_view
    with pytest.raises(ServiceError) as denied:
        await read(scope, fence, binding, qualify=qualify)
    assert projected == [True]
    assert denied.value.code == (
        "workspace_missing_directory_restore_fenced"
        if fault == "lease"
        else "original_parent_revoked"
    )


@pytest.mark.parametrize("phase", ["ready", "start"])
@pytest.mark.parametrize("loss", ["close", "release"])
async def test_lost_live_observation_never_leaves_ready_or_new_execute_qualification(
    missing, phase, loss
):
    store, scope, binding, _, identifier, reader = missing
    planned, _ = await store.reserve(scope, binding, preview(identifier), qualify=lambda: None)
    fence = await store.preparation(scope, planned.plan_id)
    before = reader.capture("tree")
    await store.record_absence(
        scope, fence, binding, before, observation_reader=reader, qualify=lambda: None
    )
    if phase == "start":
        planned = await store.ready(scope, fence, binding, qualify=lambda: None)
    if loss == "close":
        reader.close()
    else:
        assert reader.release(before)
    with pytest.raises(ServiceError) as denied:
        if phase == "ready":
            await store.ready(scope, fence, binding, qualify=lambda: None)
        else:
            await store.start(scope, binding, execute(planned), qualify=lambda: None)
    assert denied.value.code == "workspace_missing_directory_restore_observation_lost"
    assert (await counts(store))[:2] == [1, 0]
