"""原生批次冻结、持久准入、原操作投影、跨连接资格及迁移保护。"""

import asyncio
import hashlib
import stat
import time
from dataclasses import dataclass

import pytest
from alembic import command
from sqlalchemy import delete, inspect, text, update

from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    NativeWorkspaceBatchChildRow,
    NativeWorkspaceBatchRow,
    WorkspaceFileEffectRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_batches import (
    NativeBatchStore,
    assert_batch_execution,
    assert_batch_request,
)
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_list import NativeChangeListStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeReceipt, FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_list import NativeBatchFence, NativeBatchRequest, NativeBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter


@dataclass
class Ledger:
    database: Database
    edits: WorkspaceEditStore
    store: NativeBatchStore
    workspace: Workspace
    scope: str
    count: int = 0

    async def apply(self, content="B", path="file.txt", *, scope=None):
        self.count += 1
        scope = scope or self.scope
        existing = self.workspace.root / path
        before = hashlib.sha256(existing.read_bytes()).hexdigest() if existing.exists() else None
        prepared = WorkspaceWriter(self.workspace).prepare(
            FileChangeSpec(path=path, operation="write", expected_sha256=before, content=content)
        )
        change, _ = await self.edits.preview(
            scope, "files", self.workspace.fingerprint, f"p-{self.count}", prepared
        )
        attempt, _ = await self.edits.reserve(
            scope,
            WorkspaceChangeAction(
                action="apply",
                proposal_id=change.view.proposal_id,
                expected_revision=0,
                request_key=f"a-{self.count}",
            ),
            self.workspace.fingerprint,
            prepared,
        )
        writer = WorkspaceWriter(self.workspace)
        result = writer.apply(prepared)
        info = existing.stat()
        from krim_agent.workspace_change import WorkspaceFileState

        await self.edits.settle(
            scope,
            attempt.view.operation_id,
            result.receipt,
            result.recovery,
            observed_after=WorkspaceFileState(
                sha256=prepared.after_sha256,
                mode=stat.S_IMODE(info.st_mode),
                uid=info.st_uid,
                gid=info.st_gid,
            ),
        )
        if result.recovery is not None:
            writer.finalize(prepared, result.recovery)
        await self.edits.mark_cleaned(scope, attempt.view.operation_id)
        return await self.edits.get(scope, change.view.proposal_id)

    async def request(self, proposals, *, action="keep", key="batch"):
        async with self.database.sessions() as session:
            revision, _ = await NativeChangeListStore(self.edits).snapshot(session, self.scope)
        return NativeBatchRequest(
            action=action,
            request_key=key,
            snapshot_revision=revision,
            targets=[
                NativeBatchTarget(
                    proposal_id=item.view.proposal_id, expected_revision=item.view.revision
                )
                for item in proposals
            ],
        )

    async def reserve(self, request, *, store=None):
        return await (store or self.store).reserve(
            self.scope, request, self.workspace.fingerprint, "files"
        )

    async def expire(self, batch_id):
        async with self.database.sessions.begin() as session:
            await session.execute(
                update(NativeWorkspaceBatchRow)
                .where(NativeWorkspaceBatchRow.batch_id == batch_id)
                .values(lease_expires_at=time.time() - 1)
            )

    async def check(self, dispatch, *, fence=None, request=None, operation=None, owner=None):
        async with self.database.sessions.begin() as session:
            await self.edits._writer(session)
            return await assert_batch_request(
                session,
                owner or self.edits.owner_id,
                self.scope,
                request or dispatch.request,
                self.workspace.fingerprint,
                fence or dispatch.fence,
                operation,
            )


@pytest.fixture
async def ledger(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'batches.sqlite'}")
    await database.initialize()
    topic = await KrimRepository(database).create_topic("批次测试")
    root = tmp_path / "files"
    root.mkdir()
    workspace = Workspace(root, writable=True)
    edits = WorkspaceEditStore(database)
    try:
        yield Ledger(database, edits, NativeBatchStore(edits), workspace, "topic:" + topic.id)
    finally:
        workspace.close()
        await database.close()


async def test_freezes_explicit_targets_in_reverse_effect_order_and_replays(ledger):
    first = await ledger.apply("A")
    second = await ledger.apply("B")
    await ledger.apply("C", "other.txt")
    request = await ledger.request([first, second])
    view, created = await ledger.reserve(request)
    assert created and view.status == "running" and len(view.items) == 2
    assert [item.proposal_id for item in view.items] == [
        second.view.proposal_id,
        first.view.proposal_id,
    ]
    assert [item.apply_sequence for item in view.items] == [2, 1]
    assert all(item.operation is None for item in view.items)
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    assert all(child.request.request_key.startswith("krim-batch:") for child in execution.children)
    assert "token" not in view.model_dump_json() and "file_key" not in view.model_dump_json()
    duplicate, created = await ledger.reserve(request)
    assert not created and duplicate == view
    assert await ledger.store.by_key(ledger.scope, request.request_key) == view
    assert await ledger.store.by_key(ledger.scope, "missing") is None
    with pytest.raises(ServiceError, match="批次键"):
        await ledger.reserve(request.model_copy(update={"action": "undo"}))
    with pytest.raises(ServiceError) as busy:
        await ledger.reserve(request.model_copy(update={"request_key": "another"}))
    assert busy.value.code == "workspace_batch_busy"
    await ledger.store.cancel(ledger.scope, view.batch_id)
    replay, created = await ledger.reserve(request)
    assert not created and replay.status == "cancelled"


async def test_snapshot_target_revision_resource_and_action_are_checked(ledger):
    first = await ledger.apply()
    request = await ledger.request([first])
    await ledger.apply("C", "other.txt")
    with pytest.raises(ServiceError) as stale:
        await ledger.reserve(request)
    assert stale.value.code == "workspace_list_revision_conflict"
    request = await ledger.request([first])
    wrong_target = NativeBatchTarget(proposal_id=first.view.proposal_id, expected_revision=0)
    with pytest.raises(ServiceError) as wrong_revision:
        await ledger.reserve(request.model_copy(update={"targets": [wrong_target]}))
    assert wrong_revision.value.code == "workspace_revision_conflict"
    with pytest.raises(ServiceError) as resource:
        await ledger.store.reserve(ledger.scope, request, "a" * 64, "files")
    assert resource.value.code == "workspace_binding_changed"


async def test_two_connections_reserve_only_one_active_batch(ledger):
    first = await ledger.apply()
    request = await ledger.request([first])
    database = Database(str(ledger.database.engine.url))
    await database.initialize()
    other = NativeBatchStore(WorkspaceEditStore(database))
    try:
        results = await asyncio.gather(
            ledger.reserve(request),
            ledger.reserve(request.model_copy(update={"request_key": "racer"}), store=other),
            return_exceptions=True,
        )
        assert sum(isinstance(result, tuple) for result in results) == 1
        assert [item.code for item in results if isinstance(item, ServiceError)] == [
            "workspace_batch_busy"
        ]
    finally:
        await database.close()


async def test_same_key_concurrent_reserve_reuses_frozen_batch(ledger):
    first = await ledger.apply()
    request = await ledger.request([first])
    results = await asyncio.gather(ledger.reserve(request), ledger.reserve(request))
    assert sorted(created for _, created in results) == [False, True]
    assert results[0][0].batch_id == results[1][0].batch_id


@pytest.mark.parametrize(
    "field,value",
    [
        ("proposal_id", "not-the-proposal"),
        ("expected_revision", 99),
        ("action", "undo"),
        ("request_key", "outsider"),
    ],
)
async def test_fence_rejects_hook_mutated_request(ledger, field, value):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    with pytest.raises(ServiceError) as mismatch:
        await ledger.check(dispatch, request=dispatch.request.model_copy(update={field: value}))
    assert mismatch.value.code == "workspace_batch_fence_mismatch"
    assert (await ledger.check(dispatch)).operation_id is None


async def test_fence_checks_token_scope_and_owner(ledger):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    with pytest.raises(ServiceError) as mismatch:
        await ledger.check(dispatch, fence=NativeBatchFence(view.batch_id, 0, "wrong"))
    assert mismatch.value.code == "workspace_batch_fence_mismatch"
    with pytest.raises(ServiceError) as foreign:
        await ledger.check(dispatch, owner="other-owner")
    assert foreign.value.code == "workspace_batch_fenced"
    topic = await KrimRepository(ledger.database).create_topic("其他")
    with pytest.raises(ServiceError) as wrong_scope:
        await ledger.store.get("topic:" + topic.id, view.batch_id)
    assert wrong_scope.value.code == "workspace_batch_not_found"
    with pytest.raises(ServiceError):
        await ledger.store.finish_child(
            ledger.scope, NativeBatchFence(view.batch_id, 0, "wrong"), error_code="fake"
        )
    await ledger.store.renew(view.batch_id, dispatch.fence.token)


async def test_internal_keys_cannot_be_claimed_by_external_calls(ledger):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    async with ledger.database.sessions() as session:
        with pytest.raises(ServiceError) as protected:
            await assert_batch_request(
                session,
                ledger.edits.owner_id,
                ledger.scope,
                dispatch.request,
                ledger.workspace.fingerprint,
                None,
            )
        assert protected.value.code == "workspace_batch_request_reserved"
        # 旧版本可能曾接受同前缀；已有动作的只读重放仍可通过。
        assert (
            await assert_batch_request(
                session,
                ledger.edits.owner_id,
                ledger.scope,
                dispatch.request,
                ledger.workspace.fingerprint,
                None,
                "legacy-existing",
            )
            is None
        )
    with pytest.raises(ServiceError) as occupied:
        await ledger.check(dispatch, operation="not-linked")
    assert occupied.value.code == "workspace_batch_operation_conflict"


@pytest.mark.parametrize("skipped", [False, True])
async def test_finishing_unlinked_child_closes_late_hook_admission(ledger, skipped):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    dispatch = execution.children[0]
    item = await ledger.store.finish_child(ledger.scope, dispatch.fence, skipped=skipped)
    assert item.status == ("skipped" if skipped else "rejected") and item.operation is None
    with pytest.raises(ServiceError) as late:
        await ledger.check(dispatch)
    assert late.value.code == "workspace_batch_child_closed"
    again = await ledger.store.finish_child(ledger.scope, dispatch.fence, error_code="overwrite")
    assert again == item
    await ledger.store.finish(ledger.scope, view.batch_id, execution.token)
    assert (await ledger.store.get(ledger.scope, view.batch_id)).status == "failed"


@pytest.mark.parametrize("method", ["get", "by_key", "recover_interrupted", "finish"])
async def test_expiry_persistently_closes_all_pending_admission(ledger, method):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    await ledger.expire(view.batch_id)
    if method == "recover_interrupted":
        assert await ledger.store.recover_interrupted() == 1
        assert await ledger.store.recover_interrupted() == 0
    elif method == "finish":
        await ledger.store.finish(ledger.scope, view.batch_id, execution.token)
    elif method == "by_key":
        await ledger.store.by_key(ledger.scope, view.request_key)
    else:
        await ledger.store.get(ledger.scope, view.batch_id)
    stored = await ledger.store.get(ledger.scope, view.batch_id)
    assert stored.status == stored.producer_status == "interrupted"
    assert stored.stop_reason == "workspace_batch_lease_expired"
    assert stored.items[0].status == "skipped"
    async with ledger.database.sessions() as session:
        child = await session.get(NativeWorkspaceBatchChildRow, (view.batch_id, 0))
        row = await session.get(NativeWorkspaceBatchRow, view.batch_id)
        assert not child.admission_open and row.owner_id is None and row.lease_expires_at is None
    with pytest.raises(ServiceError):
        await ledger.check(execution.children[0])
    with pytest.raises(ServiceError):
        await ledger.store.renew(view.batch_id, execution.token)


async def test_cancel_by_foreign_owner_closes_fence_without_taking_ownership(ledger):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    other = NativeBatchStore(WorkspaceEditStore(ledger.database))
    with pytest.raises(ServiceError):
        await other.execution(ledger.scope, view.batch_id)
    cancelled = await other.cancel(ledger.scope, view.batch_id)
    assert cancelled.status == "cancelled" and cancelled.items[0].status == "skipped"
    with pytest.raises(ServiceError):
        await ledger.check(execution.children[0])
    await ledger.store.finish(ledger.scope, view.batch_id, execution.token)
    assert (await other.cancel(ledger.scope, view.batch_id)).status == "cancelled"


async def test_native_reserve_atomically_associates_original_attempt_and_projects_receipt(ledger):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    dispatch = execution.children[0]
    attempt, created = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        first.prepared,
        batch_fence=dispatch.fence,
    )
    assert created
    async with ledger.database.sessions() as session:
        child = await session.get(NativeWorkspaceBatchChildRow, (view.batch_id, 0))
        assert child.operation_id == attempt.view.operation_id and not child.admission_open
        await assert_batch_execution(session, ledger.edits.owner_id, attempt.view.operation_id)
    receipt = FileChangeReceipt(
        path="file.txt",
        status="done",
        changed=False,
        before_sha256=first.prepared.before_sha256,
        after_sha256=first.prepared.after_sha256,
    )
    settled = await ledger.edits.settle(ledger.scope, attempt.view.operation_id, receipt)
    # Registry若在工具成功后抛出或返回伪造拒绝，汇总仍必须引用唯一原操作。
    item = await ledger.store.finish_child(ledger.scope, dispatch.fence, error_code="outer-error")
    assert item.status == "done" and item.operation == settled.view and item.error_code is None
    assert (await ledger.store.get(ledger.scope, view.batch_id)).status == "running"
    await ledger.store.finish(ledger.scope, view.batch_id, execution.token)
    assert (await ledger.store.get(ledger.scope, view.batch_id)).status == "done"
    replay, created = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        first.prepared,
        batch_fence=dispatch.fence,
    )
    assert not created and replay.view == settled.view
    assert (
        await ledger.check(dispatch, operation=attempt.view.operation_id)
    ).operation_id == attempt.view.operation_id


async def test_lost_summary_recovers_from_original_attempt_without_file_replay(ledger):
    first = await ledger.apply()
    request = await ledger.request([first])
    view, _ = await ledger.reserve(request)
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    attempt, _ = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        first.prepared,
        batch_fence=dispatch.fence,
    )
    receipt = FileChangeReceipt(
        path="file.txt",
        status="done",
        changed=False,
        before_sha256=first.prepared.before_sha256,
        after_sha256=first.prepared.after_sha256,
    )
    await ledger.edits.settle(ledger.scope, attempt.view.operation_id, receipt)
    # 不调用finish_child或finish，模拟生产者在原动作保存后崩溃。
    await ledger.expire(view.batch_id)
    reopened = NativeBatchStore(WorkspaceEditStore(ledger.database))
    recovered = await reopened.get(ledger.scope, view.batch_id)
    assert recovered.status == "done" and recovered.producer_status == "interrupted"
    assert recovered.stop_reason == "workspace_batch_lease_expired"
    assert recovered.items[0].operation.operation_id == attempt.view.operation_id
    assert not (await ledger.reserve(request, store=reopened))[1]
    assert (ledger.workspace.root / "file.txt").read_text() == "B"


async def test_cancelled_running_attempt_remains_uncertain_until_original_receipt(ledger):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    dispatch = execution.children[0]
    attempt, _ = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        first.prepared,
        batch_fence=dispatch.fence,
    )
    cancelled = await ledger.store.cancel(ledger.scope, view.batch_id)
    assert cancelled.status == "uncertain" and cancelled.producer_status == "cancelled"
    assert cancelled.items[0].status == "running" and cancelled.items[0].operation == attempt.view
    async with ledger.database.sessions() as session:
        with pytest.raises(ServiceError) as fenced:
            await assert_batch_execution(session, ledger.edits.owner_id, attempt.view.operation_id)
        assert fenced.value.code == "workspace_batch_fenced"
    with pytest.raises(ServiceError) as publication:
        await ledger.edits.assert_execution(ledger.scope, attempt.view.operation_id)
    assert publication.value.code == "workspace_batch_fenced"
    receipt = FileChangeReceipt(
        path="file.txt",
        status="rejected",
        changed=False,
        before_sha256=first.prepared.before_sha256,
        after_sha256=first.prepared.after_sha256,
        error_code="workspace_batch_fenced",
    )
    await ledger.edits.settle(ledger.scope, attempt.view.operation_id, receipt)
    result = await ledger.store.finish_child(ledger.scope, dispatch.fence, error_code="outer")
    assert result.status == "rejected" and result.error_code == "workspace_batch_fenced"
    assert (await ledger.store.get(ledger.scope, view.batch_id)).status == "cancelled"


async def test_partial_result_uses_operations_and_preserves_unstarted_errors(ledger):
    first = await ledger.apply("B")
    second = await ledger.apply("C", "other.txt")
    view, _ = await ledger.reserve(await ledger.request([first, second]))
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    dispatch = execution.children[0]
    attempt, _ = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        second.prepared,
        batch_fence=dispatch.fence,
    )
    await ledger.edits.settle(
        ledger.scope,
        attempt.view.operation_id,
        FileChangeReceipt(
            path="other.txt",
            status="done",
            changed=False,
            before_sha256=second.prepared.before_sha256,
            after_sha256=second.prepared.after_sha256,
        ),
    )
    await ledger.store.finish_child(
        ledger.scope, execution.children[1].fence, error_code="tool_denied"
    )
    await ledger.store.finish(ledger.scope, view.batch_id, execution.token)
    result = await ledger.store.get(ledger.scope, view.batch_id)
    assert result.status == "partial"
    assert [item.status for item in result.items] == ["done", "rejected"]
    assert result.items[1].error_code == "tool_denied"


async def test_cancel_does_not_discard_already_published_undo_effect(ledger):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first], action="undo"))
    execution = await ledger.store.execution(ledger.scope, view.batch_id)
    dispatch = execution.children[0]
    writer = WorkspaceWriter(ledger.workspace)
    prepared = writer.prepare_restore(first.prepared)
    attempt, _ = await ledger.edits.reserve(
        ledger.scope,
        dispatch.request,
        ledger.workspace.fingerprint,
        prepared,
        batch_fence=dispatch.fence,
    )
    await ledger.edits.assert_execution(ledger.scope, attempt.view.operation_id)
    effect = writer.apply(prepared)
    assert effect.receipt.status == "done" and not (ledger.workspace.root / "file.txt").exists()
    assert (await ledger.store.cancel(ledger.scope, view.batch_id)).status == "uncertain"
    settled = await ledger.edits.settle(
        ledger.scope, attempt.view.operation_id, effect.receipt, effect.recovery
    )
    if effect.recovery is not None:
        writer.finalize(prepared, effect.recovery)
        await ledger.edits.mark_cleaned(ledger.scope, attempt.view.operation_id)
    assert settled.view.status == "done"
    result = await ledger.store.get(ledger.scope, view.batch_id)
    assert result.status == "done" and result.producer_status == "cancelled"
    assert result.items[0].operation.operation_id == attempt.view.operation_id
    assert (await ledger.edits.get(ledger.scope, first.view.proposal_id)).view.status == "undone"


async def test_closed_session_permits_read_cancel_and_same_key_without_new_writes(
    runtime, tmp_path
):
    from krim_agent.agents import AgentProfile, AgentProfileUpdate
    from krim_agent.application.agents import AgentService

    root = tmp_path / "session-files"
    root.mkdir()
    service = AgentService(runtime, runtime.settings)
    try:
        binding = service.register_workspace("files", root, version="v1", writable=True)
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="writer",
                    description="批次关闭测试",
                    workspace_key="files",
                    tool_names=["workspace_read"],
                ),
            )
        )
        agent = await service.spawn("writer")
        scope = "session:" + agent.session_id
        edits = WorkspaceEditStore(runtime.repository.database, owner_id=service.manager.owner_id)
        writer = WorkspaceWriter(binding.workspace)
        prepared = writer.prepare(
            FileChangeSpec(path="file.txt", operation="write", content="new", expected_sha256=None)
        )
        proposal, _ = await edits.preview(scope, "files", binding.fingerprint, "p", prepared)
        attempt, _ = await edits.reserve(
            scope,
            WorkspaceChangeAction(
                action="apply",
                proposal_id=proposal.view.proposal_id,
                expected_revision=0,
                request_key="a",
            ),
            binding.fingerprint,
            prepared,
        )
        effect = writer.apply(prepared)
        await edits.settle(scope, attempt.view.operation_id, effect.receipt, effect.recovery)
        if effect.recovery is not None:
            writer.finalize(prepared, effect.recovery)
            await edits.mark_cleaned(scope, attempt.view.operation_id)
        async with edits.database.sessions() as session:
            revision, items = await NativeChangeListStore(edits).snapshot(session, scope)
        request = NativeBatchRequest(
            action="keep",
            request_key="batch",
            snapshot_revision=revision,
            targets=[
                NativeBatchTarget(
                    proposal_id=items[0].proposal_id, expected_revision=items[0].revision
                )
            ],
        )
        batches = NativeBatchStore(edits)
        batch, _ = await batches.reserve(scope, request, binding.fingerprint, "files")
        await service.close_session(agent.agent_id, 0)
        assert (await batches.get(scope, batch.batch_id)).status == "running"
        assert (await batches.cancel(scope, batch.batch_id)).status == "cancelled"
        replay, created = await batches.reserve(scope, request, binding.fingerprint, "files")
        assert not created and replay.status == "cancelled"
        with pytest.raises(ServiceError) as closed:
            await batches.reserve(
                scope,
                request.model_copy(update={"request_key": "new"}),
                binding.fingerprint,
                "files",
            )
        assert closed.value.code == "agent_session_closed"
    finally:
        await service.close()


async def test_undo_fence_checks_unselected_and_later_cross_scope_successors(ledger):
    first = await ledger.apply("B")
    await ledger.apply("A")
    third = await ledger.apply("B")
    view, _ = await ledger.reserve(await ledger.request([first], action="undo"))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    with pytest.raises(ServiceError) as dependency:
        await ledger.check(dispatch)
    assert dependency.value.code == "workspace_batch_dependency"
    await ledger.store.cancel(ledger.scope, view.batch_id)
    view, _ = await ledger.reserve(await ledger.request([third], action="undo", key="new"))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    assert await ledger.check(dispatch)
    topic = await KrimRepository(ledger.database).create_topic("其他作用域")
    await ledger.apply("B", scope="topic:" + topic.id)
    with pytest.raises(ServiceError) as cross_scope:
        await ledger.check(dispatch)
    assert cross_scope.value.code == "workspace_batch_dependency"


async def test_unknown_order_refuses_multiple_targets_and_competing_single_undo(ledger):
    first = await ledger.apply("B")
    second = await ledger.apply("C")
    async with ledger.database.sessions.begin() as session:
        await session.execute(
            delete(WorkspaceFileEffectRow).where(
                WorkspaceFileEffectRow.proposal_id == first.view.proposal_id
            )
        )
    with pytest.raises(ServiceError) as unknown:
        await ledger.reserve(await ledger.request([first, second], action="undo"))
    assert unknown.value.code == "workspace_batch_order_unknown"
    view, _ = await ledger.reserve(await ledger.request([first], action="undo"))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    with pytest.raises(ServiceError) as dependency:
        await ledger.check(dispatch)
    assert dependency.value.code == "workspace_batch_dependency"


async def test_unknown_single_history_without_same_file_competitor_is_allowed(ledger):
    first = await ledger.apply("B")
    async with ledger.database.sessions.begin() as session:
        await session.execute(delete(WorkspaceFileEffectRow))
    await ledger.apply("C", "other.txt")
    view, _ = await ledger.reserve(await ledger.request([first], action="undo"))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    assert await ledger.check(dispatch)


async def test_known_undo_can_precede_stable_legacy_but_not_uncertain_legacy(ledger):
    first = await ledger.apply("B")
    async with ledger.database.sessions.begin() as session:
        await session.execute(delete(WorkspaceFileEffectRow))
    second = await ledger.apply("C")
    view, _ = await ledger.reserve(await ledger.request([second], action="undo"))
    dispatch = (await ledger.store.execution(ledger.scope, view.batch_id)).children[0]
    assert await ledger.check(dispatch)
    from krim_agent.persistence.database import WorkspaceEditProposalRow

    async with ledger.database.sessions.begin() as session:
        await session.execute(
            update(WorkspaceEditProposalRow)
            .where(WorkspaceEditProposalRow.proposal_id == first.view.proposal_id)
            .values(status="uncertain")
        )
    with pytest.raises(ServiceError) as unknown:
        await ledger.check(dispatch)
    assert unknown.value.code == "workspace_batch_dependency"


async def test_capacity_rejects_new_but_preserves_idempotent_history(ledger):
    first = await ledger.apply()
    limited = NativeBatchStore(ledger.edits, max_batches=1)
    request = await ledger.request([first])
    view, _ = await ledger.reserve(request, store=limited)
    await limited.cancel(ledger.scope, view.batch_id)
    with pytest.raises(ServiceError) as capacity:
        await ledger.reserve(request.model_copy(update={"request_key": "overflow"}), store=limited)
    assert capacity.value.code == "workspace_batch_limit"
    assert not (await ledger.reserve(request, store=limited))[1]


@pytest.mark.parametrize("value", [0, 5001])
def test_batch_capacity_bounds(value):
    with pytest.raises(ValueError):
        NativeBatchStore(None, max_batches=value)


async def test_migration_empty_roundtrip_preserves_workspace_effects(ledger):
    first = await ledger.apply()
    async with ledger.database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: command.downgrade(migration_config(sync), "0026_workspace_effect_chain")
        )
        assert "native_workspace_batches" not in await connection.run_sync(
            lambda sync: inspect(sync).get_table_names()
        )
    await ledger.database.initialize()
    assert (await ledger.edits.get(ledger.scope, first.view.proposal_id)).view.status == "applied"
    async with ledger.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT count(*) FROM workspace_file_effects"))
        ).scalar_one() == 1
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()


async def test_migration_native_tables_match_declared_metadata(ledger):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    from krim_agent.persistence.database import Base

    def compare(connection):
        def include_object(obj, _name, type_, _reflected, _compare_to):
            table = obj if type_ == "table" else getattr(obj, "table", None)
            return table is not None and table.name.startswith("native_workspace_")

        context = MigrationContext.configure(connection, opts={"include_object": include_object})
        return compare_metadata(context, Base.metadata)

    async with ledger.database.engine.connect() as connection:
        assert await connection.run_sync(compare) == []


async def test_migration_refuses_nonempty_batch_loss(ledger):
    first = await ledger.apply()
    view, _ = await ledger.reserve(await ledger.request([first]))
    with pytest.raises(RuntimeError, match="原生修改批次账本"):
        async with ledger.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0026_workspace_effect_chain"
                )
            )
    assert (await ledger.store.get(ledger.scope, view.batch_id)).status == "running"
    async with ledger.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0036_workspace_directory_ledger"
