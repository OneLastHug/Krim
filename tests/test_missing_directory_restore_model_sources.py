"""缺失恢复原Run/call来源、全部旧Checkpoint生产者互斥和新Run只读核实。"""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import delete, select
from test_directory_restore_guards import model_target
from test_directory_restore_plans import request as full_preview
from test_missing_directory_restore_store import execute, prepared, preview, ready
from test_model_directory_checkpoint_store import context as context

from krim_agent.persistence.database import (
    RunRow,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreAttemptRow as Attempt,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreModelSourceRow as Source,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestorePlanRow as Plan,
)
from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource, WorkspaceCheckpointCreate
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_restore import CheckpointRestorePreview
from krim_agent.workspace_writer import WorkspaceWriter


@pytest.fixture
async def model_missing(context):
    _, _, binding, scope, _, _ = context
    complete, identifier = await model_target(context)
    (binding.workspace.root / "tree").rename(binding.workspace.root / "retained-original")
    store = WorkspaceMissingDirectoryRestoreStore(complete.captures)
    with WorkspaceDirectoryAbsenceReader(binding.workspace) as reader:
        yield store, scope, binding, None, identifier, reader


async def file_checkpoint(service, scope, binding, source=None):
    files = service.workspace_checkpoint_store
    view, _ = await files.reserve(
        scope,
        WorkspaceCheckpointCreate(
            paths=["tree/a.txt"], request_key="file-target", capture_format="bytes"
        ),
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
        model_source=source,
    )
    if source is None:
        fence = (await files.execution(scope, view.checkpoint_id))[0]
        await files.record(
            scope, fence, WorkspaceWriter(binding.workspace).capture_file("tree/a.txt")
        )
        await files.finish(scope, fence)
    return view.checkpoint_id


@pytest.mark.parametrize("producer", ["directory", "file", "restore", "full"])
@pytest.mark.parametrize("first", ["missing", "old"])
async def test_every_old_checkpoint_producer_and_missing_call_are_mutually_exclusive(
    context, producer, first
):
    service, _, binding, scope, source, _ = context
    complete, identifier = await model_target(context)
    store = WorkspaceMissingDirectoryRestoreStore(complete.captures)
    file_id = await file_checkpoint(service, scope, binding) if producer == "restore" else None

    async def old():
        if producer == "directory":
            return await store.captures.reserve(
                scope,
                binding,
                DirectoryCheckpointMaterialCreate(root_path="tree", request_key="old-directory"),
                model_source=source,
            )
        if producer == "file":
            return await file_checkpoint(service, scope, binding, source)
        if producer == "restore":
            return await service.workspace_restore_store.reserve(
                scope,
                CheckpointRestorePreview(checkpoint_id=file_id, request_key="old-file-restore"),
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                model_source=source,
            )
        return await complete.reserve(
            scope, binding, full_preview(identifier), model_source=source, qualify=lambda: None
        )

    async def missing():
        return await store.reserve(
            scope, binding, preview(identifier), model_source=source, qualify=lambda: None
        )

    first_action, second_action = (missing, old) if first == "missing" else (old, missing)
    await first_action()
    with pytest.raises(ServiceError) as denied:
        await second_action()
    assert denied.value.code.endswith("_model_call_conflict")
    async with store.database.sessions() as sql:
        assert len(list(await sql.scalars(select(Source)))) == int(first == "missing")


@pytest.mark.parametrize("phase", ["preview", "execute", "verify"])
@pytest.mark.parametrize("fault", ["source", "source_and_marker"])
async def test_missing_original_phase_source_cannot_be_dropped_to_host(
    model_missing, context, phase, fault
):
    store, scope, binding, *_ = model_missing
    source = context[4]
    planned, _ = await ready(model_missing, source=source)
    operation = None
    if phase in {"execute", "verify"}:
        operation, fence, _, change, _ = await prepared(
            model_missing, planned, source=replace(source, call_id="original-execute")
        )
        if phase == "verify":
            await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
            await store.fail(scope, fence, "original_unknown_dispatch")
            await store.claim_verify(
                scope,
                operation.operation_id,
                binding,
                model_source=replace(source, call_id="original-verify"),
                qualify=lambda: None,
            )
    async with store.database.sessions() as sql, sql.begin():
        await sql.execute(delete(Source).where(Source.phase == phase))
        if fault == "source_and_marker":
            row = await sql.get(Plan, planned.plan_id)
            if phase == "verify":
                attempt = await sql.get(Attempt, operation.operation_id)
                attempt.verify_model_bound = False
                attempt.verification_run_id = attempt.verification_call_id = None
            else:
                setattr(row, phase + "_model_bound", False)
    with pytest.raises(ServiceError) as denied:
        await store.get(scope, planned.plan_id)
    assert denied.value.code == "workspace_missing_directory_restore_corrupt"


async def test_closed_original_run_new_verify_keeps_same_operation_and_all_sources(
    model_missing, context, runtime, monkeypatch
):
    store, scope, binding, *_ = model_missing
    service, agent, _, _, source, _ = context
    planned, _ = await ready(model_missing, source=source)
    operation, fence, writer, change, _ = await prepared(
        model_missing, planned, source=replace(source, call_id="original-execute")
    )
    await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    await store.fail(scope, fence, "original_unknown_dispatch")
    async with store.database.sessions() as sql, sql.begin():
        run = await sql.get(RunRow, source.run_id)
        run.status, run.stop_requested = "aborted", True
    run, _ = await runtime.repository.begin_run(
        service._handle(agent.agent_id).topic_id,
        ChatRequest(text="新Run只核实原操作", idempotency_key="new-missing-verify"),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=service.manager.owner_id,
    )
    verify_source = CheckpointModelSource(run.id, "new-verify")
    _, verify = await store.claim_verify(
        scope, operation.operation_id, binding, model_source=verify_source, qualify=lambda: None
    )
    with pytest.raises(ServiceError):
        await store.record_intent(scope, verify, binding, change, qualify=lambda: None)
    actual = await store.verification_input(scope, verify, binding, qualify=lambda: None)

    def forbidden(*_args, **_kwargs):
        pytest.fail("新核实不能生产或重新观察旧根")

    import krim_agent.workspace_missing_directory_writer as physical

    monkeypatch.setattr(physical, "rename_at", forbidden)
    monkeypatch.setattr(type(writer), "prepare", forbidden)
    monkeypatch.setattr(type(writer), "apply", forbidden)
    result = await asyncio.to_thread(writer.reconcile, actual)
    # 原Writer一直持有活观察且未发起rename，因此可明确拒绝；没有冷资格冒认成功。
    assert result.receipt.status == "rejected"
    finished = await store.settle(scope, verify, binding, result, qualify=lambda: None)
    assert finished.operation_id == operation.operation_id and finished.phase == "failed"
    assert await store.source(scope, planned.plan_id, "preview") == source
    assert await store.source(scope, planned.plan_id, "execute") == replace(
        source, call_id="original-execute"
    )
    assert await store.source(scope, planned.plan_id, "verify") == verify_source
    async with store.database.sessions() as sql:
        rows = list(await sql.scalars(select(Source)))
        assert {(row.phase, row.run_id, row.call_id) for row in rows} == {
            ("preview", source.run_id, source.call_id),
            ("execute", source.run_id, "original-execute"),
            ("verify", run.id, "new-verify"),
        }


async def test_model_admission_cancel_matches_only_original_phase_and_complete_request(
    model_missing, context
):
    store, scope, binding, _, identifier, _ = model_missing
    source = context[4]
    planned, _ = await ready(model_missing, source=source)
    execute_source = replace(source, call_id="execute")
    operation, _ = await store.start(
        scope, binding, execute(planned), model_source=execute_source, qualify=lambda: None
    )
    # preview回程的迟到收尾不能取消已进入execute的同计划。
    result = await store.cancel_model_admission(scope, source, "preview", preview(identifier))
    assert result.phase == "executing" and not result.cancel_requested
    assert (
        await store.cancel_model_admission(
            scope, execute_source, "execute", execute(planned, "changed-key")
        )
        is None
    )
    cancelled = await store.cancel_model_admission(
        scope, execute_source, "execute", execute(planned)
    )
    assert cancelled.phase == "cancelled" and cancelled.operation_id == operation.operation_id


async def test_original_run_revoked_inside_same_sql_authorization_cannot_return_prepare_view(
    model_missing, context
):
    store, scope, binding, _, identifier, _ = model_missing
    source = context[4]
    planned, _ = await store.reserve(
        scope, binding, preview(identifier), model_source=source, qualify=lambda: None
    )
    fence = await store.preparation(scope, planned.plan_id)

    async def revoked(sql):
        run = await sql.get(RunRow, source.run_id)
        run.stop_requested = True
        await sql.flush()

    with pytest.raises(ServiceError):
        await store.preparation_view(scope, fence, binding, qualify=lambda: None, authorize=revoked)
    async with store.database.sessions() as sql:
        run = await sql.get(RunRow, source.run_id)
        assert not run.stop_requested
        row = await sql.get(Plan, planned.plan_id)
        assert row.phase == "preparing" and row.absence_metadata is None
