"""实际旧文件入口双向围栏、跨Scope/资源、原事务提交丢回程与会话释放。"""

from dataclasses import replace

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session
from test_container_retention_sources import source, terminal
from test_container_retention_store import remove, start
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_directory_restore_store import missing as missing

from krim_agent.persistence import KrimRepository
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.workspace_container_retentions import WorkspaceContainerRetentionStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_container_retention import ContainerRetentionRequest
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention
from krim_agent.workspace_writer import WorkspaceWriter


async def proposal(context):
    restores, scope, binding, *_ = context
    prepared = WorkspaceWriter(binding.workspace).prepare(
        FileChangeSpec(operation="write", path="unrelated.txt", content="new", expected_sha256=None)
    )
    stored, _ = await restores.edits.preview(
        scope, binding.key, binding.fingerprint, "original-file-preview", prepared
    )
    action = WorkspaceChangeAction(
        proposal_id=stored.view.proposal_id,
        action="apply",
        expected_revision=stored.view.revision,
        request_key="original-file-apply",
    )
    return prepared, action


async def test_retention_blocks_actual_old_file_reservation_on_same_physical_parent(missing):
    context = await start(missing)
    _store, scope, binding, *_ = context
    prepared, action = await proposal(missing)
    with pytest.raises(ServiceError) as error:
        await missing[0].edits.reserve(scope, action, binding.fingerprint, prepared)
    assert error.value.code == "workspace_container_retention_busy"
    assert not (binding.workspace.root / "unrelated.txt").exists()


async def test_actual_old_file_reservation_blocks_new_retention_without_partial_ledger(missing):
    restores, scope, binding, *_ = missing
    plan, _operation, _old = await terminal(missing)
    origin = await source(missing, plan.plan_id)
    core = WorkspaceEmptyContainerRetention(binding.workspace)
    observed = core.inspect(origin.stage, authorize=lambda: None)
    prepared, action = await proposal(missing)
    await restores.edits.reserve(scope, action, binding.fingerprint, prepared)
    store = WorkspaceContainerRetentionStore(restores)
    request = ContainerRetentionRequest(
        source_plan_id=plan.plan_id,
        expected_source_hash=origin.view.source_hash,
        expected_container_fingerprint=observed.fingerprint,
        request_key="blocked-by-original-file",
    )
    with pytest.raises(ServiceError) as error:
        await store.reserve(scope, binding, request, qualify=lambda: None)
    assert error.value.code == "workspace_file_busy"
    async with store.database.sessions() as sql:
        assert await sql.scalar(select(func.count()).select_from(Row)) == 0
    assert core.inspect(origin.stage, authorize=lambda: None).status == "empty"


@pytest.mark.parametrize("fault", ["scope", "resource"])
async def test_new_ledger_cannot_rebind_original_done_source(missing, fault):
    restores, scope, binding, *_ = missing
    plan, _operation, _old = await terminal(missing)
    origin = await source(missing, plan.plan_id)
    observed = WorkspaceEmptyContainerRetention(binding.workspace).inspect(
        origin.stage, authorize=lambda: None
    )
    selected_scope, selected_binding = scope, binding
    if fault == "scope":
        topic = await KrimRepository(restores.database).create_topic("其他Scope")
        selected_scope = "topic:" + topic.id
    else:
        selected_binding = replace(binding, fingerprint="f" * 64)
    store = WorkspaceContainerRetentionStore(restores)
    request = ContainerRetentionRequest(
        source_plan_id=plan.plan_id,
        expected_source_hash=origin.view.source_hash,
        expected_container_fingerprint=observed.fingerprint,
        request_key="wrong-original-source",
    )
    with pytest.raises(ServiceError):
        await store.reserve(selected_scope, selected_binding, request, qualify=lambda: None)
    async with store.database.sessions() as sql:
        assert await sql.scalar(select(func.count()).select_from(Row)) == 0


@pytest.mark.parametrize("point", ["reserve", "settle"])
async def test_actual_commit_lost_reply_cold_key_returns_once_and_pool_is_closed(missing, point):
    restores, scope, binding, *_ = missing
    if point == "settle":
        context = await start(missing)
        store, _scope, _binding, _origin, _old, _core, request, view, fence = context
        effect = await remove(context)
        assert effect.status == "removed"
    else:
        plan, _operation, _old = await terminal(missing)
        origin = await source(missing, plan.plan_id)
        observed = WorkspaceEmptyContainerRetention(binding.workspace).inspect(
            origin.stage, authorize=lambda: None
        )
        store = WorkspaceContainerRetentionStore(restores)
        request = ContainerRetentionRequest(
            source_plan_id=plan.plan_id,
            expected_source_hash=origin.view.source_hash,
            expected_container_fingerprint=observed.fingerprint,
            request_key="lost-original-commit",
        )
    fired = []

    def before_flush(session, *_args):
        candidates = [*session.new, *session.dirty]
        if any(
            isinstance(row, Row)
            and row.request_key == request.request_key
            and row.status == ("running" if point == "reserve" else "done")
            for row in candidates
        ):
            session.info["krim_retention_lost_reply"] = True

    def after_commit(session):
        if session.info.pop("krim_retention_lost_reply", False) and not fired:
            fired.append(True)
            raise OSError("synthetic after-commit lost reply")

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "after_commit", after_commit)
    try:
        with pytest.raises(OSError, match="after-commit"):
            if point == "reserve":
                await store.reserve(scope, binding, request, qualify=lambda: None)
            else:
                await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "after_commit", after_commit)
    assert fired
    pool = store.database.engine.pool
    if hasattr(pool, "checkedout"):
        assert pool.checkedout() == 0
    cold = WorkspaceContainerRetentionStore(restores)
    actual = await cold.by_key(scope, request.request_key)
    assert actual.status == ("running" if point == "reserve" else "done")
    assert await cold.reserve(
        scope, binding, request, qualify=lambda: pytest.fail("原键冷读不重新准入")
    ) == (actual, False)
    async with store.database.sessions() as sql:
        assert await sql.scalar(select(func.count()).select_from(Row)) == 1
