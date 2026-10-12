"""混合批次的真实原事务关联、资格失效、冷回程与零重放。"""

import asyncio
import json
import sys
import time
from dataclasses import asdict, replace
from pathlib import Path

import pytest
from sqlalchemy import event, func, select, update
from test_mixed_workspace_batch_store import setup
from test_workspace_changes import setup as agent_setup
from test_workspace_changes import wait_event
from test_workspace_effect_chain import apply_content

from krim_agent.persistence.database import (
    Database,
    MixedWorkspaceBatchChildRow,
    MixedWorkspaceBatchRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools import ShortCircuit, ToolContext, ToolError
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux条件事务")


def source_store(store, source):
    return store.files if source == "file" else store.directories


def mutation_metadata(path):
    # 原keep核实允许读取并推进atime，仍严格检查实际写入与身份/权限变化。
    value = path.stat()
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


async def reserve_child(store, scope, workspace, dispatch, *, request=None, fence=None):
    original_store = source_store(store, dispatch.fence.source)
    request = request or dispatch.request
    original = await original_store.get(scope, request.proposal_id)
    prepared = original.prepared
    if request.action == "undo":
        if dispatch.fence.source == "file":
            prepared = WorkspaceWriter(workspace).prepare_restore(prepared)
        else:
            applied = next(
                item
                for item in reversed(await original_store.history(scope, request.proposal_id))
                if item.view.action == "apply" and item.view.status == "done"
            )
            prepared = WorkspaceDirectoryWriter(workspace).prepare_undo(
                prepared, await original_store.effect(scope, applied.view.operation_id)
            )
    return await original_store.reserve(
        scope,
        request,
        workspace.fingerprint,
        prepared,
        mixed_fence=fence or dispatch.fence,
    )


async def apply_child(store, scope, workspace, dispatch):
    attempt, created = await reserve_child(store, scope, workspace, dispatch)
    assert created
    original_store = source_store(store, dispatch.fence.source)
    await original_store.assert_execution(scope, attempt.view.operation_id)
    if dispatch.fence.source == "file":
        effect = WorkspaceWriter(workspace).apply(attempt.prepared)
        done = await original_store.settle(
            scope,
            attempt.view.operation_id,
            effect.receipt,
            effect.recovery,
            observed_after=effect.observed_after,
        )
    else:
        references = []
        effect = WorkspaceDirectoryWriter(workspace).apply(
            attempt.prepared, record_intent=references.append
        )
        for reference in references:
            await original_store.record_intent(scope, attempt.view.operation_id, reference)
        done = await original_store.settle(scope, attempt.view.operation_id, effect)
    assert done.view.status == "done"
    item = await store.finish_child(scope, dispatch.fence, error_code="hook_false_error")
    assert item.operation == done.view and item.error_code is None
    return done.view


async def attempt_counts(database):
    async with database.sessions() as sql:
        return tuple(
            [
                await sql.scalar(select(func.count()).select_from(table))
                for table in (WorkspaceEditAttemptRow, WorkspaceDirectoryAttemptRow)
            ]
        )


async def test_real_file_directory_apply_atomic_associations_and_cold_reopen(repository, tmp_path):
    store, scope, workspace, request = await setup(repository, tmp_path)
    cold_db = Database(str(repository.database.engine.url))
    cold = MixedBatchStore(
        WorkspaceEditStore(cold_db, owner_id="cold-owner"),
        WorkspaceDirectoryStore(cold_db, owner_id="cold-owner"),
    )
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        execution = await store.execution(scope, view.batch_id)
        assert len(execution.children) == 2
        for dispatch in execution.children:
            await apply_child(store, scope, workspace, dispatch)
        assert not (await store.execution(scope, view.batch_id)).children
        done = await store.finish(scope, view.batch_id, execution.token)
        assert done.status == "done"
        assert (tmp_path / "file.txt").read_text() == "data"
        assert (tmp_path / "empty").is_dir()
        assert await cold.get(scope, view.batch_id) == done
        assert await cold.by_key(scope, "mixed") == done
        assert await attempt_counts(store.database) == (1, 1)
        async with store.database.sessions() as sql:
            for position, dispatch in enumerate(execution.children):
                child = await sql.get(MixedWorkspaceBatchChildRow, (view.batch_id, position))
                identifier = child.file_operation_id or child.directory_operation_id
                assert identifier == done.items[position].operation.operation_id
                assert not child.admission_open
                original = source_store(store, dispatch.fence.source)
                read = await original.find_action(scope, dispatch.request, workspace.fingerprint)
                assert read.view == done.items[position].operation
        assert await attempt_counts(store.database) == (1, 1)
    finally:
        await cold_db.close()
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize(
    "case",
    [
        "token",
        "source",
        "position",
        "batch",
        "action",
        "revision",
        "key",
        "proposal",
        "scope",
        "resource",
    ],
)
async def test_frozen_credentials_reject_mismatch_without_original_attempts(
    repository, tmp_path, source, case
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        execution = await store.execution(scope, view.batch_id)
        dispatch = execution.children[0 if source == "file" else 1]
        fence, action, wanted_scope, resource = (
            dispatch.fence,
            dispatch.request,
            scope,
            workspace.fingerprint,
        )
        if case in {"token", "source", "position", "batch"}:
            changes = {
                "token": "0" * 64,
                "source": "directory" if source == "file" else "file",
                "position": 31,
                "batch_id": "missing",
            }
            fence = replace(
                fence,
                **{
                    ("batch_id" if case == "batch" else case): changes[
                        "batch_id" if case == "batch" else case
                    ]
                },
            )
        elif case == "scope":
            wanted_scope = "topic:" + (await repository.create_topic("foreign")).id
        elif case == "resource":
            resource = "0" * 64
        else:
            changes = {
                "action": "undo",
                "expected_revision": 1,
                "request_key": "foreign",
                "proposal_id": execution.children[1 if source == "file" else 0].request.proposal_id,
            }
            field = {
                "revision": "expected_revision",
                "key": "request_key",
                "proposal": "proposal_id",
            }.get(case, case)
            action = action.model_copy(update={field: changes[field]})
        original = source_store(store, source)
        prepared = (await original.get(scope, dispatch.request.proposal_id)).prepared
        for method in ("find", "reserve"):
            with pytest.raises(ServiceError):
                if method == "find":
                    await original.find_action(wanted_scope, action, resource, mixed_fence=fence)
                else:
                    await original.reserve(
                        wanted_scope, action, resource, prepared, mixed_fence=fence
                    )
        assert await attempt_counts(store.database) == (0, 0)
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_public_internal_key_is_denied_but_existing_original_key_is_read_only(
    repository, tmp_path, source
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        dispatch = (await store.execution(scope, view.batch_id)).children[
            0 if source == "file" else 1
        ]
        original = source_store(store, source)
        plan = (await original.get(scope, dispatch.request.proposal_id)).prepared
        with pytest.raises(ServiceError) as rejected:
            await original.reserve(scope, dispatch.request, workspace.fingerprint, plan)
        assert rejected.value.code == "workspace_mixed_batch_request_reserved"
        done = await apply_child(store, scope, workspace, dispatch)
        await store.cancel(scope, view.batch_id)
        repeated, created = await original.reserve(
            scope, dispatch.request, workspace.fingerprint, plan
        )
        assert not created and repeated.view == done
        forged = replace(dispatch.fence, token="0" * 64)
        with pytest.raises(ServiceError):
            await original.reserve(
                scope, dispatch.request, workspace.fingerprint, plan, mixed_fence=forged
            )
        assert await attempt_counts(store.database) == ((1, 0) if source == "file" else (0, 1))
    finally:
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize("closed", ["cancel", "expiry", "owner"])
@pytest.mark.parametrize("admitted", [False, True])
async def test_closed_parent_blocks_admission_execution_renew_and_finish(
    repository, tmp_path, source, closed, admitted
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        execution = await store.execution(scope, view.batch_id)
        dispatch = execution.children[0 if source == "file" else 1]
        attempt = (await reserve_child(store, scope, workspace, dispatch))[0] if admitted else None
        if closed == "cancel":
            await store.cancel(scope, view.batch_id)
        else:
            async with store.database.sessions.begin() as sql:
                await sql.execute(
                    update(MixedWorkspaceBatchRow)
                    .where(MixedWorkspaceBatchRow.batch_id == view.batch_id)
                    .values(
                        **(
                            {"lease_expires_at": time.time() - 1}
                            if closed == "expiry"
                            else {"owner_id": "other"}
                        )
                    )
                )
        for method in ("execution", "renew", "finish_child", "finish"):
            with pytest.raises(ServiceError) as refused:
                if method == "execution":
                    await store.execution(scope, view.batch_id)
                elif method == "renew":
                    await store.renew(view.batch_id, execution.token)
                elif method == "finish_child":
                    await store.finish_child(scope, dispatch.fence)
                else:
                    await store.finish(scope, view.batch_id, execution.token)
            assert refused.value.code == "workspace_mixed_batch_fenced"
        if admitted:
            with pytest.raises(ServiceError) as refused:
                await source_store(store, source).assert_execution(scope, attempt.view.operation_id)
            assert refused.value.code == "workspace_mixed_batch_fenced"
        else:
            with pytest.raises(ServiceError):
                await reserve_child(store, scope, workspace, dispatch)
        if closed != "owner":
            cold = await store.get(scope, view.batch_id)
            if admitted:
                assert (
                    cold.status == "uncertain"
                    and cold.items[dispatch.fence.position].operation == attempt.view
                )
            else:
                assert all(
                    item.operation is None and item.status == "skipped" for item in cold.items
                )
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_attempt_and_child_association_roll_back_together(repository, tmp_path, source):
    store, scope, workspace, request = await setup(repository, tmp_path)
    view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
    dispatch = (await store.execution(scope, view.batch_id)).children[0 if source == "file" else 1]

    def reject_association(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("UPDATE mixed_workspace_batch_children"):
            raise RuntimeError("test child association write failed")

    engine = store.database.engine.sync_engine
    event.listen(engine, "before_cursor_execute", reject_association)
    try:
        with pytest.raises(RuntimeError, match="association"):
            await reserve_child(store, scope, workspace, dispatch)
    finally:
        event.remove(engine, "before_cursor_execute", reject_association)
    try:
        assert await attempt_counts(store.database) == (0, 0)
        pending = await store.get(scope, view.batch_id)
        assert all(item.operation is None and item.status == "pending" for item in pending.items)
        attempt, created = await reserve_child(store, scope, workspace, dispatch)
        assert created and attempt.view.status == "running"
    finally:
        workspace.close()


async def test_hook_short_circuit_without_attempt_is_rejected_and_cannot_reopen(
    repository, tmp_path
):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        execution = await store.execution(scope, view.batch_id)
        first = await store.finish_child(scope, execution.children[0].fence)
        assert first.status == "rejected" and first.operation is None
        with pytest.raises(ServiceError) as refused:
            await reserve_child(store, scope, workspace, execution.children[0])
        assert refused.value.code == "workspace_mixed_batch_child_closed"
        await apply_child(store, scope, workspace, execution.children[1])
        done = await store.finish(scope, view.batch_id, execution.token)
        assert (
            done.status == "partial"
            and done.items[0].error_code == "workspace_mixed_batch_not_executed"
        )
        assert not (tmp_path / "file.txt").exists() and (tmp_path / "empty").is_dir()
    finally:
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_two_connections_reserve_exactly_one_original_attempt(repository, tmp_path, source):
    store, scope, workspace, request = await setup(repository, tmp_path)
    other_db = Database(str(store.database.engine.url))
    other = MixedBatchStore(
        WorkspaceEditStore(other_db, owner_id=store.owner_id),
        WorkspaceDirectoryStore(other_db, owner_id=store.owner_id),
    )
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        dispatch = (await store.execution(scope, view.batch_id)).children[
            0 if source == "file" else 1
        ]
        results = await asyncio.gather(
            *(reserve_child(item, scope, workspace, dispatch) for item in (store, other))
        )
        assert sum(created for _attempt, created in results) == 1
        assert len({attempt.view.operation_id for attempt, _created in results}) == 1
        assert await attempt_counts(store.database) == ((1, 0) if source == "file" else (0, 1))
        assert await other.get(scope, view.batch_id) == await store.get(scope, view.batch_id)
    finally:
        await other_db.close()
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_cancel_after_effect_preserves_actual_receipt(repository, tmp_path, source):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        dispatch = (await store.execution(scope, view.batch_id)).children[
            0 if source == "file" else 1
        ]
        attempt, _ = await reserve_child(store, scope, workspace, dispatch)
        await source_store(store, source).assert_execution(scope, attempt.view.operation_id)
        if source == "file":
            effect = WorkspaceWriter(workspace).apply(attempt.prepared)
        else:
            effect = WorkspaceDirectoryWriter(workspace).apply(attempt.prepared)
        await store.cancel(scope, view.batch_id)
        original = source_store(store, source)
        if source == "file":
            await original.settle(
                scope,
                attempt.view.operation_id,
                effect.receipt,
                effect.recovery,
                observed_after=effect.observed_after,
            )
        else:
            await original.settle(scope, attempt.view.operation_id, effect)
        read = await store.get(scope, view.batch_id)
        assert read.producer_status == "cancelled" and read.status == "cancelled"
        assert read.items[dispatch.fence.position].status == "done"
        assert read.items[dispatch.fence.position].operation.receipt.changed
        assert read.items[1 - dispatch.fence.position].status == "skipped"
    finally:
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_two_processes_admit_exactly_one_original_attempt(repository, tmp_path, source):
    store, scope, workspace, request = await setup(repository, tmp_path)
    processes = []
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        dispatch = (await store.execution(scope, view.batch_id)).children[
            0 if source == "file" else 1
        ]
        payload = json.dumps(
            {
                "database": str(store.database.engine.url),
                "scope": scope,
                "resource": workspace.fingerprint,
                "owner": store.owner_id,
                "source": source,
                "request": dispatch.request.model_dump(),
                "fence": asdict(dispatch.fence),
            }
        ).encode()
        script = Path(__file__).parent / "fixtures" / "mixed_batch_fence_worker.py"
        for _ in range(2):
            processes.append(
                await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(script),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            )
        results = await asyncio.wait_for(
            asyncio.gather(*(process.communicate(payload) for process in processes)), 30
        )
        assert all(process.returncode == 0 for process in processes), [
            stderr.decode() for _stdout, stderr in results
        ]
        values = [json.loads(stdout) for stdout, _stderr in results]
        assert sum(value["created"] for value in values) == 1
        assert len({value["operation_id"] for value in values}) == 1
        assert await attempt_counts(store.database) == ((1, 0) if source == "file" else (0, 1))
        cold = await store.get(scope, view.batch_id)
        assert (
            cold.items[dispatch.fence.position].operation.operation_id == values[0]["operation_id"]
        )
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
        await asyncio.gather(*(process.wait() for process in processes))
        workspace.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_cold_projection_rejects_corrupt_original_association(repository, tmp_path, source):
    store, scope, workspace, request = await setup(repository, tmp_path)
    try:
        view, _ = await store.reserve(scope, "files", workspace.fingerprint, request)
        dispatch = (await store.execution(scope, view.batch_id)).children[
            0 if source == "file" else 1
        ]
        attempt, _ = await reserve_child(store, scope, workspace, dispatch)
        async with store.database.sessions.begin() as sql:
            await sql.execute(
                update(MixedWorkspaceBatchChildRow)
                .where(
                    MixedWorkspaceBatchChildRow.batch_id == view.batch_id,
                    MixedWorkspaceBatchChildRow.position == dispatch.fence.position,
                )
                .values(expected_revision=9)
            )
        with pytest.raises(ServiceError) as refused:
            await store.get(scope, view.batch_id)
        assert refused.value.code == "workspace_mixed_batch_corrupt"
        with pytest.raises(ServiceError) as refused:
            await source_store(store, source).assert_execution(scope, attempt.view.operation_id)
        assert refused.value.code == "workspace_mixed_batch_corrupt"
    finally:
        workspace.close()


async def agent_mixed_setup(runtime, tmp_path):
    service, agent, binding = await agent_setup(runtime, tmp_path)
    file = await agent.preview_file_change(
        FileChangeSpec(path="file.txt", operation="write", content="data", expected_sha256=None),
        request_key="file-preview",
    )
    directory = await agent.preview_directory_change(
        DirectoryChangeSpec(path="empty", operation="create", mode=0o750),
        request_key="directory-preview",
    )
    store = MixedBatchStore(service.workspace_edits, service.manager.directory_changes.store)
    request = MixedBatchRequest(
        action="apply",
        request_key="mixed",
        targets=[
            MixedBatchTarget(source="file", proposal_id=file.proposal_id, expected_revision=0),
            MixedBatchTarget(
                source="directory", proposal_id=directory.proposal_id, expected_revision=0
            ),
        ],
    )
    scope = "session:" + agent.session_id
    view, _ = await store.reserve(scope, binding.key, binding.fingerprint, request)
    return service, agent, binding, store, scope, await store.execution(scope, view.batch_id)


async def invoke_child(service, agent, dispatch):
    return await service._workspace_call(
        agent.agent_id,
        "workspace_change" if dispatch.fence.source == "file" else "directory_change",
        dispatch.request.model_dump(),
        confirmed=True,
        mixed_fence=dispatch.fence,
    )


async def test_registry_services_apply_keep_undo_share_original_ledger(runtime, tmp_path):
    service, agent, binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    try:
        for kind in ("apply", "keep", "undo"):
            stamp = (
                (mutation_metadata(tmp_path / "file.txt"), mutation_metadata(tmp_path / "empty"))
                if kind == "keep"
                else None
            )
            if kind != "apply":
                current = [
                    (await source_store(store, item.source).get(scope, item.proposal_id)).view
                    for item in execution.view.items
                ]
                request = MixedBatchRequest(
                    action=kind,
                    request_key=kind,
                    targets=[
                        MixedBatchTarget(
                            source=item.source,
                            proposal_id=item.proposal_id,
                            expected_revision=view.revision,
                        )
                        for item, view in zip(execution.view.items, current, strict=True)
                    ],
                )
                view, _ = await store.reserve(scope, binding.key, binding.fingerprint, request)
                execution = await store.execution(scope, view.batch_id)
            for dispatch in execution.children:
                raw = await invoke_child(service, agent, dispatch)
                assert raw["status"] == "done"
                item = await store.finish_child(scope, dispatch.fence)
                assert item.operation.operation_id == raw["operation_id"]
            done = await store.finish(scope, execution.view.batch_id, execution.token)
            assert done.status == "done"
            if kind == "keep":
                assert all(not item.operation.receipt.changed for item in done.items)
                assert stamp == (
                    mutation_metadata(tmp_path / "file.txt"),
                    mutation_metadata(tmp_path / "empty"),
                )
                assert (tmp_path / "file.txt").read_bytes() == b"data"
                assert list((tmp_path / "empty").iterdir()) == []
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
        assert await attempt_counts(store.database) == (3, 3)
    finally:
        await service.close()


@pytest.mark.parametrize("case", ["noop", "aba", "foreign"])
async def test_mixed_undo_refuses_unselected_successor_even_with_matching_bytes(
    runtime, tmp_path, case
):
    service, agent, binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    try:
        dispatch = execution.children[0]
        await invoke_child(service, agent, dispatch)
        await store.finish(scope, execution.view.batch_id, execution.token)
        file = tmp_path / "file.txt"
        owner = agent
        if case == "foreign":
            from krim_agent.sdk import AgentSession

            owner = AgentSession(service, (await service.spawn("default")).agent_id)
        await apply_content(owner, file, "data" if case == "noop" else "changed", "successor")
        if case != "noop":
            await apply_content(owner, file, "data", "aba")
        proposal = (await store.files.get(scope, dispatch.request.proposal_id)).view
        request = MixedBatchRequest(
            action="undo",
            request_key="undo",
            targets=[
                MixedBatchTarget(
                    source="file",
                    proposal_id=proposal.proposal_id,
                    expected_revision=proposal.revision,
                )
            ],
        )
        view, _ = await store.reserve(scope, binding.key, binding.fingerprint, request)
        undo = (await store.execution(scope, view.batch_id)).children[0]
        with pytest.raises(ServiceError) as refused:
            await invoke_child(service, agent, undo)
        assert refused.value.code == "workspace_batch_dependency"
        item = await store.finish_child(scope, undo.fence, error_code=refused.value.code)
        assert item.operation is None and item.status == "rejected"
        assert len(await agent.file_change_history(proposal.proposal_id)) == 1
        assert file.read_text() == "data"
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_real_registry_short_circuit_is_not_an_original_operation(runtime, tmp_path, source):
    service, agent, _binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    try:
        dispatch = execution.children[0 if source == "file" else 1]
        registry = service._handle(agent.agent_id).runtime.tools

        async def shortcut(definition, _context):
            if definition.name == ("workspace_change" if source == "file" else "directory_change"):
                return ShortCircuit({"status": "done", "operation_id": "hook-invented"})
            return None

        registry.add_global_pre(shortcut)
        try:
            await invoke_child(service, agent, dispatch)
        except ServiceError:
            pass
        item = await store.finish_child(scope, dispatch.fence)
        assert item.status == "rejected" and item.operation is None
        assert await attempt_counts(store.database) == (0, 0)
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_mixed_context_cannot_invoke_other_source_finalize_or_arbitrary_tool(
    runtime, tmp_path, source
):
    service, agent, binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    try:
        dispatch = execution.children[0 if source == "file" else 1]
        registry = service._handle(agent.agent_id).runtime.tools
        context = ToolContext(
            registry, workspace=binding, mixed_batch=dispatch.fence, origin="host"
        )
        for name in (
            "workspace_preview",
            "directory_preview",
            "workspace_finalize",
            "directory_finalize",
            "echo",
        ):
            result = await registry.invoke(name, {}, context)
            assert result.is_error and result.code == "workspace_mixed_batch_action"
        backend = (
            service.manager.workspace_changes
            if source == "file"
            else service.manager.directory_changes
        )
        with pytest.raises(ToolError) as refused:
            await backend.tool_operation("finalize", {"operation_id": "fake"}, context)
        assert refused.value.code == "workspace_mixed_batch_action"
        assert await attempt_counts(store.database) == (0, 0)
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize("closed", ["cancel", "expiry"])
async def test_cancel_or_expiry_after_admission_blocks_late_writer(
    runtime, tmp_path, monkeypatch, source, closed
):
    from threading import Event

    service, agent, _binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    entered, release = Event(), Event()
    writer_type = WorkspaceWriter if source == "file" else WorkspaceDirectoryWriter
    original = writer_type.apply

    def waiting(self, prepared, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(self, prepared, **kwargs)

    monkeypatch.setattr(writer_type, "apply", waiting)
    dispatch = execution.children[0 if source == "file" else 1]
    task = asyncio.create_task(invoke_child(service, agent, dispatch))
    try:
        await wait_event(entered)
        linked = await store.get(scope, execution.view.batch_id)
        assert linked.items[dispatch.fence.position].operation is not None
        if closed == "cancel":
            await store.cancel(scope, execution.view.batch_id)
        else:
            async with store.database.sessions.begin() as sql:
                await sql.execute(
                    update(MixedWorkspaceBatchRow)
                    .where(MixedWorkspaceBatchRow.batch_id == execution.view.batch_id)
                    .values(lease_expires_at=time.time() - 1)
                )
        release.set()
        raw = await asyncio.wait_for(task, 20)
        assert raw["status"] in {"rejected", "uncertain"}
        read = await store.get(scope, execution.view.batch_id)
        assert read.items[dispatch.fence.position].operation.operation_id == raw["operation_id"]
        assert read.items[dispatch.fence.position].status == raw["status"]
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
        repeated = await invoke_child(service, agent, dispatch)
        assert repeated == raw
        assert await attempt_counts(store.database) == ((1, 0) if source == "file" else (0, 1))
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize("field", ["action", "request_key", "expected_revision", "mixed_batch"])
async def test_hook_cannot_replace_frozen_parameters_or_clear_fence(
    runtime, tmp_path, source, field
):
    service, agent, _binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    try:
        dispatch = execution.children[0 if source == "file" else 1]
        registry = service._handle(agent.agent_id).runtime.tools

        async def redirect(_definition, context):
            if field == "mixed_batch":
                context.mixed_batch = None
            else:
                context.arguments[field] = {
                    "action": "undo",
                    "request_key": "hook-key",
                    "expected_revision": 8,
                }[field]

        registry.add_global_pre(redirect)
        with pytest.raises(ServiceError) as refused:
            await invoke_child(service, agent, dispatch)
        assert refused.value.code == "workspace_mixed_batch_fence_mismatch"
        assert await attempt_counts(store.database) == (0, 0)
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_unknown_reconcile_updates_original_fact_without_replaying_system_call(
    runtime, tmp_path, monkeypatch, source
):
    import krim_agent.workspace_directory as directory_module
    import krim_agent.workspace_writer as file_module

    service, agent, _binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    module = file_module if source == "file" else directory_module
    # 新建文件以link发布，目录以rename发布；在实际副作用边界模拟丢回执。
    original = module.os.link if source == "file" else module.rename_at
    calls = 0

    def ambiguous(*args, **kwargs):
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        raise OSError("test lost rename response")

    if source == "file":
        monkeypatch.setattr(module.os, "link", ambiguous)
    else:
        monkeypatch.setattr(module, "rename_at", ambiguous)
    try:
        dispatch = execution.children[0 if source == "file" else 1]
        raw = await invoke_child(service, agent, dispatch)
        assert raw["status"] == "uncertain" and calls == 1
        await store.finish_child(scope, dispatch.fence)
        closed = await store.finish(scope, execution.view.batch_id, execution.token)
        assert closed.status == "uncertain"
        assert await invoke_child(service, agent, dispatch) == raw
        assert calls == 1
        reconciled = await (
            agent.reconcile_file_change(raw["operation_id"])
            if source == "file"
            else agent.reconcile_directory_change(raw["operation_id"])
        )
        assert reconciled.status == "done" and calls == 1
        read = await store.get(scope, execution.view.batch_id)
        assert (
            read.status == "partial" and read.items[dispatch.fence.position].operation == reconciled
        )
        repeated = await invoke_child(service, agent, dispatch)
        assert repeated["status"] == "done" and repeated["operation_id"] == raw["operation_id"]
        assert calls == 1
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_cancel_between_keep_verification_and_receipt_cannot_report_kept(
    runtime, tmp_path, monkeypatch, source
):
    service, agent, binding, store, scope, execution = await agent_mixed_setup(runtime, tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    task = None
    try:
        for dispatch in execution.children:
            await invoke_child(service, agent, dispatch)
        await store.finish(scope, execution.view.batch_id, execution.token)
        target = execution.view.items[0 if source == "file" else 1]
        original_store = source_store(store, source)
        proposal = (await original_store.get(scope, target.proposal_id)).view
        request = MixedBatchRequest(
            action="keep",
            request_key="keep",
            targets=[
                MixedBatchTarget(
                    source=source,
                    proposal_id=proposal.proposal_id,
                    expected_revision=proposal.revision,
                )
            ],
        )
        accepted, _ = await store.reserve(scope, binding.key, binding.fingerprint, request)
        keep_execution = await store.execution(scope, accepted.batch_id)
        original_settle = original_store.settle

        async def pause_receipt(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                await release.wait()
            return await original_settle(*args, **kwargs)

        monkeypatch.setattr(original_store, "settle", pause_receipt)
        stamp = (mutation_metadata(tmp_path / "file.txt"), mutation_metadata(tmp_path / "empty"))
        task = asyncio.create_task(invoke_child(service, agent, keep_execution.children[0]))
        await asyncio.wait_for(entered.wait(), 10)
        await store.cancel(scope, accepted.batch_id)
        release.set()
        raw = await asyncio.wait_for(task, 10)
        assert raw["status"] == "uncertain" and raw["action"] == "keep"
        cold = await store.get(scope, accepted.batch_id)
        assert cold.producer_status == "cancelled" and cold.status == "uncertain"
        assert cold.items[0].operation.operation_id == raw["operation_id"]
        assert stamp == (
            mutation_metadata(tmp_path / "file.txt"),
            mutation_metadata(tmp_path / "empty"),
        )
        assert (tmp_path / "file.txt").read_bytes() == b"data"
        assert list((tmp_path / "empty").iterdir()) == []
        assert (await original_store.get(scope, target.proposal_id)).view.status != "kept"
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await service.close()
