"""独立SQL回收与真实rmdir同一来源；旧finished回执/Source/完整材料保持。"""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import func, select
from test_container_retention_sources import source, terminal
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import bridge
from test_missing_directory_restore_store import missing as missing

import krim_agent.workspace_empty_container_retention as physical
from krim_agent.persistence.database import WorkspaceContainerRetentionLockRow as Lock
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.database import WorkspaceMissingDirectoryRestorePlanRow as SourcePlan
from krim_agent.persistence.workspace_container_retentions import WorkspaceContainerRetentionStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_container_retention import ContainerRetentionRequest
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention


async def start(context, *, key="original-retention"):
    restores, scope, binding, *_ = context
    plan, _operation, original = await terminal(context)
    origin = await source(context, plan.plan_id)
    core = WorkspaceEmptyContainerRetention(binding.workspace)
    observed = await asyncio.to_thread(core.inspect, origin.stage, authorize=lambda: None)
    assert observed.status == "empty"
    store = WorkspaceContainerRetentionStore(restores)
    request = ContainerRetentionRequest(
        source_plan_id=plan.plan_id,
        expected_source_hash=origin.view.source_hash,
        expected_container_fingerprint=observed.fingerprint,
        request_key=key,
    )
    view, created = await store.reserve(scope, binding, request, qualify=lambda: None)
    assert created and not view.intent_recorded and view.status == "running"
    fence = await store.execution(scope, view.operation_id)
    assert await store.inputs(scope, fence, binding, qualify=lambda: None) == origin
    return store, scope, binding, origin, original, core, request, view, fence


async def remove(context):
    store, scope, binding, origin, _original, core, request, _view, fence = context
    loop = asyncio.get_running_loop()
    authorize = bridge(
        loop, lambda: store.assert_execution(scope, fence, binding, qualify=lambda: None)
    )
    record = bridge(
        loop,
        lambda observed: store.record_intent(scope, fence, binding, observed, qualify=lambda: None),
    )
    return await asyncio.to_thread(
        core.remove,
        origin.stage,
        expected_fingerprint=request.expected_container_fingerprint,
        confirmed=True,
        authorize=authorize,
        record_intent=record,
    )


async def test_real_sql_retention_same_original_key_releases_slot_keeps_old_finished(
    missing, monkeypatch
):
    context = await start(missing)
    store, scope, binding, origin, original, _core, request, view, fence = context
    tree = binding.workspace.root / "tree"
    inode = tree.stat().st_ino
    (tree / "user-added").write_bytes(b"user\0\xff")
    effect = await remove(context)
    assert effect.status == "removed" and effect.changed
    final = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert final.status == "done" and final.intent_recorded and final.receipt.outcome == "removed"
    assert final.revision == 2
    assert await store.restores.get(scope, origin.view.source_plan_id) == original
    assert tree.stat().st_ino == inode and (tree / "nested/nul").read_bytes() == b"\0\xff"
    assert (tree / "user-added").read_bytes() == b"user\0\xff"
    async with store.database.sessions() as sql:
        old = await sql.get(SourcePlan, origin.view.source_plan_id)
        assert (
            old.cleanup_pending
            and old.staging_reserved_bytes == 0
            and old.staging_reserved_entries == 0
        )
        assert await sql.scalar(select(func.count()).select_from(Lock)) == 0
        assert await sql.scalar(select(func.count()).select_from(Row)) == 1
    assert await source(missing, origin.view.source_plan_id) == origin
    cold = WorkspaceContainerRetentionStore(store.restores)
    monkeypatch.setattr(physical.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("原键只能读"))
    assert await cold.by_key(scope, request.request_key) == final
    assert await store.reserve(
        scope, binding, request, qualify=lambda: pytest.fail("原键不得再准入")
    ) == (final, False)
    with pytest.raises(ServiceError) as error:
        await store.reserve(
            scope,
            binding,
            request.model_copy(update={"request_key": "different-key"}),
            qualify=lambda: None,
        )
    assert error.value.code == "workspace_container_retention_source_busy"


async def test_already_absent_commits_observation_without_invented_namespace_intent(missing):
    restores, scope, binding, *_ = missing
    plan, _operation, original = await terminal(missing)
    origin = await source(missing, plan.plan_id)
    core = WorkspaceEmptyContainerRetention(binding.workspace)
    first = core.inspect(origin.stage, authorize=lambda: None)
    assert (
        core.remove(
            origin.stage,
            expected_fingerprint=first.fingerprint,
            confirmed=True,
            authorize=lambda: None,
            record_intent=lambda _value: None,
        ).status
        == "removed"
    )
    observed = core.inspect(origin.stage, authorize=lambda: None)
    assert observed.status == "absent"
    store = WorkspaceContainerRetentionStore(restores)
    request = ContainerRetentionRequest(
        source_plan_id=plan.plan_id,
        expected_source_hash=origin.view.source_hash,
        expected_container_fingerprint=observed.fingerprint,
        request_key="already-absent",
    )
    view, _ = await store.reserve(scope, binding, request, qualify=lambda: None)
    fence = await store.execution(scope, view.operation_id)
    effect = core.remove(
        origin.stage,
        expected_fingerprint=observed.fingerprint,
        confirmed=True,
        authorize=lambda: None,
        record_intent=lambda _value: pytest.fail("不存在的目录不派发删除"),
    )
    final = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert final.status == "done" and not final.intent_recorded and not final.receipt.changed
    assert final.receipt.outcome == "already_absent"
    assert await restores.get(scope, plan.plan_id) == original


async def test_post_rmdir_unknown_keeps_namespace_and_quota_until_explicit_read(
    missing, monkeypatch
):
    context = await start(missing)
    store, scope, binding, origin, original, _core, request, view, fence = context
    rmdir = physical.os.rmdir

    def lost(name, *, dir_fd):
        rmdir(name, dir_fd=dir_fd)
        raise OSError("lost namespace reply")

    monkeypatch.setattr(physical.os, "rmdir", lost)
    effect = await remove(context)
    assert effect.status == "unknown"
    final = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert final.status == "unknown" and final.intent_recorded and final.receipt.recovery_required
    async with store.database.sessions() as sql:
        assert await sql.scalar(select(func.count()).select_from(Lock)) == 3
        old = await sql.get(SourcePlan, origin.view.source_plan_id)
        assert old.staging_reserved_entries == 1
    assert await store.restores.get(scope, origin.view.source_plan_id) == original
    assert await store.by_key(scope, request.request_key) == final
    assert await store.get(scope, view.operation_id) == final
    assert replace(effect, stage=origin.stage) == effect
