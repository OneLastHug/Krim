"""混合后台批次通过真实AgentSession/Registry执行，等待和冷读不重放。"""

import asyncio
import sys
import time
from threading import Event

import pytest
from sqlalchemy import update
from test_mixed_workspace_batch_fences import attempt_counts
from test_workspace_changes import setup as setup_agent
from test_workspace_changes import wait_event

from krim_agent.persistence.database import MixedWorkspaceBatchRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit, ToolError
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux条件事务")


async def setup(runtime, tmp_path, *, source_first="file", persistent=False):
    service, agent, binding = await setup_agent(runtime, tmp_path, persistent=persistent)
    request = await previews(agent, source_first=source_first)
    return service, agent, binding, request


async def previews(agent, *, source_first="file"):
    file = await agent.preview_file_change(
        FileChangeSpec(path="file.txt", operation="write", content="data", expected_sha256=None),
        request_key="file-preview",
    )
    directory = await agent.preview_directory_change(
        DirectoryChangeSpec(path="empty", operation="create", mode=0o750),
        request_key="directory-preview",
    )
    targets = [
        MixedBatchTarget(source="file", proposal_id=file.proposal_id, expected_revision=0),
        MixedBatchTarget(
            source="directory", proposal_id=directory.proposal_id, expected_revision=0
        ),
    ]
    if source_first == "directory":
        targets.reverse()
    return MixedBatchRequest(action="apply", request_key="mixed", targets=targets)


async def run(agent, request, *, confirmed=False):
    accepted = await agent.start_mixed_change_batch(request, confirmed=confirmed)
    done = await agent.wait_mixed_change_batch(accepted.batch_id, timeout_seconds=30)
    assert done.producer_status != "running"
    return done


async def test_sdk_mixed_worker_apply_keep_undo_and_same_key_never_reruns(runtime, tmp_path):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    try:
        for action in ("apply", "keep", "undo"):
            if action != "apply":
                targets = []
                for target in request.targets:
                    store = (
                        service.workspace_edits
                        if target.source == "file"
                        else service.manager.directory_changes.store
                    )
                    proposal = (
                        await store.get("session:" + agent.session_id, target.proposal_id)
                    ).view
                    targets.append(
                        MixedBatchTarget(
                            source=target.source,
                            proposal_id=target.proposal_id,
                            expected_revision=proposal.revision,
                        )
                    )
                request = MixedBatchRequest(action=action, request_key=action, targets=targets)
            stamp = (
                ((tmp_path / "file.txt").stat(), (tmp_path / "empty").stat())
                if action == "keep"
                else None
            )
            done = await run(agent, request)
            assert done.status == "done" and all(
                item.operation.status == "done" for item in done.items
            )
            assert await agent.mixed_change_batch(done.batch_id) == done
            assert await agent.mixed_change_batch_by_key(request.request_key) == done
            assert await agent.start_mixed_change_batch(request) == done
            if action == "keep":
                assert all(not item.operation.receipt.changed for item in done.items)
                assert stamp == ((tmp_path / "file.txt").stat(), (tmp_path / "empty").stat())
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
        assert await attempt_counts(service.runtime.repository.database) == (3, 3)
    finally:
        await service.close()


@pytest.mark.parametrize("decision,confirmed", [("deny", True), ("ask", False), ("ask", True)])
async def test_worker_permissions_and_confirmation_are_checked_per_original_tool(
    runtime, tmp_path, decision, confirmed
):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    try:

        async def policy(definition, _context):
            return decision if definition.name == "workspace_change" else "allow"

        service._handle(agent.agent_id).runtime.tool_policy = policy
        done = await run(agent, request, confirmed=confirmed)
        if decision == "ask" and confirmed:
            assert done.status == "done" and (tmp_path / "file.txt").read_text() == "data"
        else:
            assert done.status == "partial" and done.items[0].operation is None
            assert done.items[0].error_code in {"permission_denied", "approval_required"}
            assert not (tmp_path / "file.txt").exists()
        assert done.items[1].status == "done" and (tmp_path / "empty").is_dir()
    finally:
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
async def test_short_circuit_is_partial_but_post_hook_error_retains_actual_done(
    runtime, tmp_path, source
):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    try:
        registry = service._handle(agent.agent_id).runtime.tools
        name = "workspace_change" if source == "file" else "directory_change"

        async def shortcut(definition, _context):
            if definition.name == name:
                return ShortCircuit({"status": "done", "operation_id": "invented"})
            return None

        registry.add_global_pre(shortcut)
        short = await run(agent, request)
        assert short.status == "partial"
        item = next(item for item in short.items if item.source == source)
        assert item.operation is None and item.status == "rejected"
        registry._global_pre.clear()

        async def lose_result(definition, _result, _context):
            if definition.name == name:
                raise ToolError("test_hook_failed", "模拟效果保存后的Hook失败")
            return _result

        registry.add_global_post(lose_result)
        retry = MixedBatchRequest(
            action="apply",
            request_key="new-key",
            targets=[target for target in request.targets if target.source == source],
        )
        done = await run(agent, retry)
        assert done.status == "done" and done.items[0].operation.receipt.changed
    finally:
        await service.close()


async def test_background_late_hook_cannot_admit_closed_child_of_live_parent(runtime, tmp_path):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    second_entered, release_second, late_release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    late_task = None
    try:

        async def hook(definition, context):
            nonlocal late_task
            if definition.name == "workspace_change":
                payload = dict(context.arguments)

                async def late():
                    await late_release.wait()
                    return await service.manager.workspace_changes.tool_operation(
                        "change", payload, context
                    )

                late_task = asyncio.create_task(late())
                return ShortCircuit({"operation_id": "invented", "status": "done"})
            if definition.name == "directory_change":
                second_entered.set()
                await release_second.wait()
            return None

        service._handle(agent.agent_id).runtime.tools.add_global_pre(hook)
        accepted = await agent.start_mixed_change_batch(request)
        await asyncio.wait_for(second_entered.wait(), 10)
        assert (await agent.mixed_change_batch(accepted.batch_id)).producer_status == "running"
        late_release.set()
        with pytest.raises(ToolError) as denied:
            await asyncio.wait_for(late_task, 10)
        assert denied.value.code == "workspace_mixed_batch_child_closed"
        release_second.set()
        done = await agent.wait_mixed_change_batch(accepted.batch_id)
        assert done.status == "partial" and await attempt_counts(runtime.repository.database) == (
            0,
            1,
        )
    finally:
        release_second.set()
        late_release.set()
        if late_task is not None:
            await asyncio.gather(late_task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize("phase", ["hook", "writer"])
async def test_worker_cancel_closes_before_join_and_stops_late_effects(
    runtime, tmp_path, monkeypatch, source, phase
):
    service, agent, _binding, request = await setup(runtime, tmp_path, source_first=source)
    entered, release = Event(), Event()
    cancel_task = None
    try:
        if phase == "hook":

            async def wait_before(definition, _context):
                if definition.name == (
                    "workspace_change" if source == "file" else "directory_change"
                ):
                    entered.set()
                    await asyncio.Event().wait()

            service._handle(agent.agent_id).runtime.tools.add_global_pre(wait_before)
        else:
            writer = WorkspaceWriter if source == "file" else WorkspaceDirectoryWriter
            original = writer.apply

            def slow(self, prepared, **kwargs):
                entered.set()
                assert release.wait(10)
                return original(self, prepared, **kwargs)

            monkeypatch.setattr(writer, "apply", slow)
        accepted = await agent.start_mixed_change_batch(request)
        await wait_event(entered)
        stopped = asyncio.Event()
        original_cancel = service.workspace_mixed_batch_store.cancel

        async def signal_cancel(*args, **kwargs):
            value = await original_cancel(*args, **kwargs)
            stopped.set()
            return value

        monkeypatch.setattr(service.workspace_mixed_batch_store, "cancel", signal_cancel)
        cancel_task = asyncio.create_task(agent.cancel_mixed_change_batch(accepted.batch_id))
        async with asyncio.timeout(10):
            await stopped.wait()
        assert (await agent.mixed_change_batch(accepted.batch_id)).producer_status == "cancelled"
        release.set()
        cancelled = await asyncio.wait_for(cancel_task, 10)
        assert cancelled.producer_status == "cancelled"
        assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
        assert cancelled.items[1].status == "skipped" and cancelled.items[1].operation is None
        assert await agent.start_mixed_change_batch(request) == cancelled
        assert await agent.cancel_mixed_change_batch(accepted.batch_id) == cancelled
    finally:
        release.set()
        if cancel_task is not None:
            await asyncio.gather(cancel_task, return_exceptions=True)
        await service.close()


async def test_wait_timeout_and_waiter_cancel_do_not_stop_worker_or_duplicate_acceptance(
    runtime, tmp_path
):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    waiter = None
    try:

        async def wait_before(definition, _context):
            if definition.name == "workspace_change":
                entered.set()
                await release.wait()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(wait_before)
        accepted = await agent.start_mixed_change_batch(request)
        await entered.wait()
        pending = await agent.wait_mixed_change_batch(accepted.batch_id, timeout_seconds=0)
        assert pending.status == "running"
        waiter = asyncio.create_task(agent.wait_mixed_change_batch(accepted.batch_id))
        await asyncio.sleep(0.02)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        again = await agent.start_mixed_change_batch(request)
        assert again.batch_id == accepted.batch_id
        assert await attempt_counts(runtime.repository.database) == (0, 0)
        release.set()
        done = await agent.wait_mixed_change_batch(accepted.batch_id)
        assert done.status == "done" and await attempt_counts(runtime.repository.database) == (1, 1)
    finally:
        release.set()
        await service.close()


async def test_lease_expiry_cancels_worker_and_same_key_cannot_restart(runtime, tmp_path):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    entered = asyncio.Event()
    try:

        async def wait_before(definition, _context):
            if definition.name == "workspace_change":
                entered.set()
                await asyncio.Event().wait()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(wait_before)
        accepted = await agent.start_mixed_change_batch(request)
        await entered.wait()
        async with runtime.repository.database.sessions.begin() as sql:
            await sql.execute(
                update(MixedWorkspaceBatchRow)
                .where(MixedWorkspaceBatchRow.batch_id == accepted.batch_id)
                .values(lease_expires_at=time.time() - 1)
            )
        interrupted = await agent.wait_mixed_change_batch(accepted.batch_id)
        assert interrupted.status == "interrupted" and all(
            item.operation is None for item in interrupted.items
        )
        assert await agent.start_mixed_change_batch(request) == interrupted
        assert await attempt_counts(runtime.repository.database) == (0, 0)
    finally:
        await service.close()


async def test_hook_can_cancel_own_worker_and_close_rejects_reentrancy(runtime, tmp_path):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    checked = False
    try:

        async def hook(definition, context):
            nonlocal checked
            if definition.name == "workspace_change":
                with pytest.raises(ServiceError):
                    await service.close()
                assert not service._closed
                checked = True
                await agent.cancel_mixed_change_batch(context.mixed_batch.batch_id)

        service._handle(agent.agent_id).runtime.tools.add_global_pre(hook)
        done = await asyncio.wait_for(run(agent, request), 10)
        assert (
            checked
            and done.status == "cancelled"
            and all(item.operation is None for item in done.items)
        )
        assert await attempt_counts(runtime.repository.database) == (0, 0)
    finally:
        await service.close()


async def test_two_scope_hooks_cancel_each_other_without_join_cycle(runtime, tmp_path):
    service, first, _binding, first_request = await setup(runtime, tmp_path)
    second = AgentSession(service, (await service.spawn("default")).agent_id)
    second_request = await previews(second)
    barrier = asyncio.Barrier(2)
    identifiers = [None, None]
    try:

        def hook_for(index):
            async def hook(definition, _context):
                if definition.name == "workspace_change":
                    await barrier.wait()
                    other = second if index == 0 else first
                    await other.cancel_mixed_change_batch(identifiers[1 - index])

            return hook

        for index, agent in enumerate((first, second)):
            service._handle(agent.agent_id).runtime.tools.add_global_pre(hook_for(index))
        accepted = await asyncio.gather(
            first.start_mixed_change_batch(first_request),
            second.start_mixed_change_batch(second_request),
        )
        identifiers[:] = [view.batch_id for view in accepted]
        done = await asyncio.wait_for(
            asyncio.gather(
                first.wait_mixed_change_batch(identifiers[0]),
                second.wait_mixed_change_batch(identifiers[1]),
            ),
            10,
        )
        assert all(item.producer_status == "cancelled" for item in done)
        assert await attempt_counts(runtime.repository.database) == (0, 0)
    finally:
        await service.close()


async def test_scope_busy_cross_scope_reads_and_service_shutdown_preserve_no_replay(
    runtime, tmp_path
):
    service, agent, _binding, request = await setup(runtime, tmp_path)
    entered = asyncio.Event()
    try:

        async def wait_before(definition, _context):
            if definition.name == "workspace_change":
                entered.set()
                await asyncio.Event().wait()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(wait_before)
        accepted = await agent.start_mixed_change_batch(request)
        await entered.wait()
        other = AgentSession(service, (await service.spawn("default")).agent_id)
        with pytest.raises(ServiceError) as denied:
            await other.mixed_change_batch(accepted.batch_id)
        assert denied.value.code == "workspace_mixed_batch_not_found"
        assert await other.mixed_change_batch_by_key("mixed") is None
        with pytest.raises(ServiceError) as denied:
            await agent.start_mixed_change_batch(
                request.model_copy(update={"request_key": "another"})
            )
        assert denied.value.code == "workspace_mixed_batch_busy"
        await service.close()
        cold = await service.workspace_mixed_batch_store.get(
            "session:" + agent.session_id, accepted.batch_id
        )
        assert cold.status == "cancelled" and await attempt_counts(runtime.repository.database) == (
            0,
            0,
        )
        assert not service.workspace_mixed_batches._tasks
    finally:
        await service.close()
