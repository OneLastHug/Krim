"""可信host树Worker真实原Registry、原账本、控制与幂等闭环。"""

import asyncio
import shutil
import sys

import pytest
from test_command_directory_dependency_source import counts
from test_workspace_tree_fences import tree

from krim_agent.application.workspace_trees import WorkspaceTreeService
from krim_agent.persistence.database import AgentSessionRow, WorkspaceTreePlanRow
from krim_agent.persistence.workspace_directories import decode
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_tree import TreePlanRequest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux目录来源与原Registry发布",
)


async def test_host_worker_publishes_original_tree_and_repeated_start_only_reads(runtime, tmp_path):
    service, agent, scope, _store, plan = await tree(runtime, tmp_path, claim=False)
    calls = []
    original = service.workspace_trees.execute

    async def recorded(identifier, dispatch, confirmed):
        calls.append((dispatch.fence.position, dispatch.fence.phase))
        await original(identifier, dispatch, confirmed)
        return {"status": "invented", "operation_id": "ignored"}

    service.workspace_trees.execute = recorded
    try:
        values = await asyncio.gather(
            *(
                service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
                for _ in range(3)
            )
        )
        assert all(value.plan_id == plan.plan_id for value in values)
        done = await service.workspace_wait_tree(agent.agent_id, plan.plan_id, timeout_seconds=20)
        assert done.status == "done" and done.done_count == 3
        assert calls == [
            (position, phase) for position in range(3) for phase in ("preview", "change")
        ]
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\x00\xff"
        assert (tmp_path / "tree").stat().st_mode & 0o777 == 0o750
        assert (tmp_path / "tree/sub").stat().st_mode & 0o777 == 0o700
        assert (
            await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True) == done
        )
        assert await counts(service.workspace_tree_store.database) == [2, 2, 1, 1]
        page = await service.workspace_tree_steps(agent.agent_id, plan.plan_id)
        assert all(item.status == "done" and item.operation is not None for item in page.items)
        assert await service.workspace_tree(agent.agent_id, plan.plan_id) == done
        assert len(calls) == 6
    finally:
        await service.close()


async def test_worker_callback_success_without_original_proposal_is_rejected(runtime, tmp_path):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    calls = 0

    async def fake(*_args):
        nonlocal calls
        calls += 1
        return {"status": "done", "operation_id": "invented"}

    service.workspace_trees.execute = fake
    try:
        await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        value = await service.workspace_wait_tree(agent.agent_id, plan.plan_id, timeout_seconds=20)
        assert value.status == "failed" and value.done_count == 0
        page = await store.steps(scope, plan.plan_id)
        assert page.items[0].status == "rejected" and page.items[0].operation is None
        assert page.items[0].error_code == "workspace_tree_original_proposal_missing"
        assert all(item.status == "skipped" and item.operation is None for item in page.items[1:])
        assert calls == 1 and await counts(store.database) == [0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_worker_original_write_policy_denial_stops_unstarted_descendants(runtime, tmp_path):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    handle = service._handle(agent.agent_id)

    async def deny_change(definition, _context):
        return "deny" if definition.name == "directory_change" else "allow"

    handle.runtime.tool_policy = deny_change
    try:
        await service.workspace_start_tree(agent.agent_id, plan.plan_id)
        failed = await service.workspace_wait_tree(agent.agent_id, plan.plan_id, timeout_seconds=20)
        assert failed.status == "failed" and failed.done_count == 0
        page = await store.steps(scope, plan.plan_id)
        assert (
            page.items[0].status == "rejected" and page.items[0].error_code == "permission_denied"
        )
        assert all(item.status == "skipped" for item in page.items[1:])
        assert await counts(store.database) == [1, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_return_failure_after_original_done_preserves_effect_and_stops_remaining(
    runtime, tmp_path
):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    original = service.workspace_trees.execute

    async def lose_response(identifier, dispatch, confirmed):
        await original(identifier, dispatch, confirmed)
        if dispatch.fence.phase == "change":
            raise ServiceError("test_return_lost", "测试真实回程丢失", 502)

    service.workspace_trees.execute = lose_response
    try:
        await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        partial = await service.workspace_wait_tree(
            agent.agent_id, plan.plan_id, timeout_seconds=20
        )
        assert partial.status == "partial" and partial.done_count == 1
        page = await store.steps(scope, plan.plan_id)
        assert page.items[0].operation.status == "done"
        assert all(item.status == "skipped" and item.operation is None for item in page.items[1:])
        assert (tmp_path / "tree").exists() and not (tmp_path / "tree/sub").exists()
        assert await counts(store.database) == [1, 1, 0, 0]
        assert (
            await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
            == partial
        )
        assert await counts(store.database) == [1, 1, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["cancel", "close"])
async def test_worker_cancel_or_close_before_original_attempt_closes_admission(
    runtime, tmp_path, action
):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    entered = asyncio.Event()

    async def pause(definition, _context):
        if definition.name == "directory_change":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
    try:
        await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        await asyncio.wait_for(entered.wait(), 10)
        if action == "cancel":
            cancelled = await asyncio.wait_for(
                service.workspace_cancel_tree(agent.agent_id, plan.plan_id), 15
            )
        else:
            await asyncio.wait_for(service.close(), 15)
            cancelled = await store.get(scope, plan.plan_id)
        assert cancelled.producer_status == "cancelled" and cancelled.done_count == 0
        page = await store.steps(scope, plan.plan_id)
        assert all(item.status == "skipped" and item.operation is None for item in page.items)
        assert await counts(store.database) == [1, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_two_host_services_start_only_one_original_producer(runtime, tmp_path):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    other = WorkspaceTreeService(
        service.workspace_tree_store,
        service._workspace_batch_authorize,
        service._workspace_tree_child,
    )
    try:
        values = await asyncio.gather(
            service.workspace_trees.start(agent.agent_id, plan.plan_id, confirmed=True),
            other.start(agent.agent_id, plan.plan_id, confirmed=True),
        )
        assert all(value.plan_id == plan.plan_id for value in values)
        assert len(service.workspace_trees._tasks) + len(other._tasks) == 1
        done = await service.workspace_trees.wait(scope, plan.plan_id, timeout_seconds=20)
        assert done.status == "done" and await counts(store.database) == [2, 2, 1, 1]
    finally:
        await other.close()
        await service.close()


async def test_worker_callback_close_is_rejected_without_waiting_itself(runtime, tmp_path):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    observed = []
    original = service.workspace_trees.execute

    async def inspect_close(identifier, dispatch, confirmed):
        with pytest.raises(ServiceError) as denied:
            await service.close()
        observed.append(denied.value.code)
        await original(identifier, dispatch, confirmed)

    service.workspace_trees.execute = inspect_close
    try:
        await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        done = await service.workspace_wait_tree(agent.agent_id, plan.plan_id, timeout_seconds=20)
        assert done.status == "done" and observed == ["workspace_tree_close_reentrant"] * 6
        assert await counts(store.database) == [2, 2, 1, 1]
    finally:
        await service.close()


@pytest.mark.parametrize("seconds", [float("nan"), float("inf"), -1, 301, True])
async def test_worker_wait_rejects_invalid_time_without_claim(runtime, tmp_path, seconds):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    try:
        with pytest.raises(ServiceError) as denied:
            await service.workspace_wait_tree(agent.agent_id, plan.plan_id, timeout_seconds=seconds)
        assert denied.value.code == "workspace_tree_timeout"
        assert (await store.get(scope, plan.plan_id)).status == "planned"
        assert await counts(store.database) == [0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_capacity_preserves_planned_tree_and_allows_original_running_reads(runtime, tmp_path):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    entered, release = asyncio.Event(), asyncio.Event()
    service.workspace_trees.max_active = 1
    try:
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreePlanRow, plan.plan_id)
            request = TreePlanRequest.model_validate(row.frozen["request"]).model_copy(
                update={"request_key": "second"}
            )
            prepared = decode(row.frozen["root_prepared"])
        binding = service._handle(agent.agent_id).definition.workspace
        second, _ = await store.reserve(scope, binding, request, prepared)

        async def pause(definition, _context):
            if definition.name == "directory_change":
                entered.set()
                await release.wait()

        service._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
        await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        await asyncio.wait_for(entered.wait(), 10)
        with pytest.raises(ServiceError) as busy:
            await service.workspace_start_tree(agent.agent_id, second.plan_id, confirmed=True)
        assert busy.value.code == "workspace_tree_busy" and busy.value.status == 429
        assert (await store.get(scope, second.plan_id)).status == "planned"
        assert (
            await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        ).producer_status == "running"
        await service.workspace_cancel_tree(agent.agent_id, plan.plan_id)
        release.set()
        await service.workspace_start_tree(agent.agent_id, second.plan_id, confirmed=True)
        done = await service.workspace_wait_tree(agent.agent_id, second.plan_id, timeout_seconds=20)
        assert done.status == "done" and done.done_count == 3
        assert (await store.get(scope, plan.plan_id)).producer_status == "cancelled"
        assert await counts(store.database) == [3, 2, 1, 1]
    finally:
        release.set()
        await service.close()


async def test_heartbeat_revoked_resource_closes_parent_before_cancelled_registry(
    runtime, tmp_path
):
    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    entered = asyncio.Event()
    service.workspace_tree_store.lease_seconds = 0.3

    async def pause(definition, _context):
        if definition.name == "directory_change":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(pause)
    try:
        await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        await asyncio.wait_for(entered.wait(), 10)
        async with store.database.sessions.begin() as sql:
            session = await sql.get(AgentSessionRow, agent.session_id)
            session.permissions = {
                **session.permissions,
                "execution": {**session.permissions["execution"], "fingerprint": "0" * 64},
            }
        closed = await service.workspace_trees.wait(scope, plan.plan_id, timeout_seconds=20)
        assert closed.producer_status in {"cancelled", "interrupted"}
        page = await store.steps(scope, plan.plan_id)
        assert all(item.status == "skipped" and item.operation is None for item in page.items)
        assert await counts(store.database) == [1, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_worker_unknown_stops_children_and_reconcile_never_restarts_producer(
    runtime, tmp_path, monkeypatch
):
    import krim_agent.workspace_directory as directory_module

    service, agent, scope, store, plan = await tree(runtime, tmp_path, claim=False)
    original = directory_module.rename_at
    calls = 0

    def ambiguous(*args, **kwargs):
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        raise OSError("test worker original receipt lost")

    monkeypatch.setattr(directory_module, "rename_at", ambiguous)
    try:
        await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        unknown = await service.workspace_wait_tree(
            agent.agent_id, plan.plan_id, timeout_seconds=20
        )
        page = await store.steps(scope, plan.plan_id)
        assert unknown.status == "uncertain" and calls == 1, (
            unknown.model_dump(),
            page.model_dump(),
            calls,
        )
        operation_id = page.items[0].operation.operation_id
        assert all(item.status == "skipped" and item.operation is None for item in page.items[1:])
        resolved = await agent.reconcile_directory_change(operation_id)
        assert resolved.status == "done" and calls == 1
        current = await service.workspace_start_tree(agent.agent_id, plan.plan_id, confirmed=True)
        assert current.status == "partial" and current.done_count == 1 and calls == 1
        assert await counts(store.database) == [1, 1, 0, 0]
        assert not (tmp_path / "tree/sub").exists()
    finally:
        await service.close()
