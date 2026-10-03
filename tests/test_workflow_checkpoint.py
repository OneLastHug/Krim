import asyncio

import pytest
from alembic import command

from krim_agent.persistence import Database, WorkflowCheckpointStore
from krim_agent.workflow import (
    WorkflowEdge,
    WorkflowEngine,
    WorkflowError,
    WorkflowGraph,
    WorkflowNode,
)
from krim_agent.workflow.runners import RunnerBinding
from krim_agent.workflow.types import NodeOutput


async def paused(handle, cursor=0):
    async for event in handle.stream_events(from_seq=cursor, heartbeat=0.01):
        if event is not None and event.type == "paused":
            return event
    raise AssertionError("没有暂停事件")


def chain():
    return WorkflowGraph(
        nodes=[WorkflowNode(id="one", kind="tool"), WorkflowNode(id="two", kind="tool")],
        edges=[WorkflowEdge(source="one", target="two")],
        output_node="two",
    )


async def test_completed_checkpoint_result_and_events_survive_reopen(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'workflow.sqlite'}"
    database = Database(url)
    await database.initialize()
    try:
        store = WorkflowCheckpointStore(database)
        graph = WorkflowGraph(
            nodes=[WorkflowNode(id="value", kind="constant", config={"value": 5})]
        )
        handle = WorkflowEngine().execute(graph, None, checkpoint=store)
        original = await handle.wait()
        run_id = handle.run_id
        assert original.status == "completed" and original.output == 5
        saved = await store.load(run_id)
        assert saved.state.result == original
        assert [event.seq for event in saved.state.events] == list(range(len(saved.state.events)))
    finally:
        await database.close()
    reopened = Database(url)
    await reopened.initialize()
    try:
        restored = await WorkflowEngine().restore(WorkflowCheckpointStore(reopened), run_id)
        assert await restored.wait() == original
        assert await restored.events() == saved.state.events
    finally:
        await reopened.close()


@pytest.mark.parametrize("boundary", ["before", "after"])
async def test_pause_boundary_and_override_survive_suspend_and_reopen(tmp_path, boundary):
    url = f"sqlite+aiosqlite:///{tmp_path / 'pause.sqlite'}"
    calls = []

    async def runner(node, inputs, context):
        calls.append((node.id, inputs["in"], context.execution_id))
        return inputs["in"]

    database = Database(url)
    await database.initialize()
    try:
        store = WorkflowCheckpointStore(database)
        options = {"pause_before": {"two"}} if boundary == "before" else {"pause_after": {"one"}}
        handle = WorkflowEngine().execute(
            chain(), "original", RunnerBinding(runner, retry_safe=True), checkpoint=store, **options
        )
        event = await asyncio.wait_for(paused(handle), 3)
        assert event.node_id == ("two" if boundary == "before" else "one")
        run_id = handle.run_id
        saved = await handle.suspend()
        assert saved.status == "paused" and len(calls) == 1
    finally:
        await database.close()
    reopened = Database(url)
    await reopened.initialize()
    try:
        handle = await WorkflowEngine().restore(
            WorkflowCheckpointStore(reopened), run_id, RunnerBinding(runner, retry_safe=True)
        )
        await asyncio.wait_for(paused(handle, len(saved.events)), 3)
        if boundary == "after":
            with pytest.raises(WorkflowError):
                await handle.resume("invalid override")
            await handle.resume()
        else:
            await handle.resume("replacement")
        result = await handle.wait()
        assert result.output == ("replacement" if boundary == "before" else "original")
        assert [item[0] for item in calls] == ["one", "two"]
        assert result.run_id == run_id
    finally:
        await reopened.close()


async def test_inflight_side_effect_requires_confirmation_or_completed_output(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'effect.sqlite'}")
    await database.initialize()
    entered = asyncio.Event()
    calls = []

    async def effect(node, inputs, context):
        calls.append(context.execution_id)
        entered.set()
        await asyncio.Event().wait()

    try:
        store = WorkflowCheckpointStore(database)
        graph = WorkflowGraph(nodes=[WorkflowNode(id="effect", kind="tool")])
        handle = WorkflowEngine().execute(graph, "input", RunnerBinding(effect), checkpoint=store)
        await asyncio.wait_for(entered.wait(), 3)
        run_id = handle.run_id
        await handle.suspend()
        with pytest.raises(WorkflowError) as error:
            await WorkflowEngine().restore(store, run_id, RunnerBinding(effect))
        assert error.value.code == "recovery_requires_confirmation" and len(calls) == 1
        restored = await WorkflowEngine().restore(
            store,
            run_id,
            RunnerBinding(effect),
            completed_outputs={"effect": NodeOutput(ports={"out": "already committed externally"})},
        )
        assert (await restored.wait()).output == "already committed externally"
        assert len(calls) == 1
        assert any(event.source == "recovery" for event in await restored.events())
    finally:
        await database.close()


async def test_safe_inflight_recovery_preserves_execution_id(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'safe.sqlite'}")
    await database.initialize()
    entered = asyncio.Event()
    ids = []
    first = True

    async def runner(node, inputs, context):
        nonlocal first
        ids.append(context.execution_id)
        if first:
            first = False
            entered.set()
            await asyncio.Event().wait()
        return inputs["in"]

    try:
        store = WorkflowCheckpointStore(database)
        graph = WorkflowGraph(nodes=[WorkflowNode(id="work", kind="tool")])
        handle = WorkflowEngine().execute(
            graph, 7, RunnerBinding(runner, retry_safe=True), checkpoint=store
        )
        await asyncio.wait_for(entered.wait(), 3)
        run_id = handle.run_id
        await handle.suspend()
        restored = await WorkflowEngine().restore(
            store, run_id, RunnerBinding(runner, retry_safe=True)
        )
        assert (await restored.wait()).output == 7
        assert len(ids) == 2 and ids[0] == ids[1]
    finally:
        await database.close()


async def test_live_lease_blocks_another_producer_and_downgrade_keeps_data(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'lease.sqlite'}")
    await database.initialize()
    try:
        store = WorkflowCheckpointStore(database, lease_seconds=0.5)
        graph = WorkflowGraph(
            nodes=[WorkflowNode(id="value", kind="constant", config={"value": 1})]
        )
        handle = WorkflowEngine().execute(graph, None, checkpoint=store, pause_before={"value"})
        await asyncio.wait_for(paused(handle), 3)
        with pytest.raises(WorkflowError) as error:
            await store.claim(handle.run_id, "other-owner")
        assert error.value.code == "checkpoint_busy"
        with pytest.raises(RuntimeError, match="Workflow 检查点"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(store_config(sync), "0005_oauth_tokens")
                )
        await handle.resume()
        assert (await handle.wait()).output == 1
    finally:
        await database.close()


def store_config(connection):
    from krim_agent.persistence.migration import migration_config

    return migration_config(connection)


@pytest.mark.parametrize("kind", ["sub_workflow", "loop", "fanout"])
async def test_nested_progress_survives_suspend_without_replaying_completed_child(tmp_path, kind):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'nested.sqlite'}")
    await database.initialize()
    entered = asyncio.Event()
    calls = []
    blocked = True

    async def runner(node, inputs, context):
        nonlocal blocked
        value = inputs["in"]
        calls.append((value, context.execution_id))
        if blocked and value == 2:
            blocked = False
            entered.set()
            await asyncio.Event().wait()
        return value + 1

    inner = (
        chain()
        if kind == "sub_workflow"
        else WorkflowGraph(nodes=[WorkflowNode(id="work", kind="tool")])
    )
    configuration = {"graph": inner.model_dump(mode="json")}
    if kind == "loop":
        configuration["max_iterations"] = 3
    graph = WorkflowGraph(nodes=[WorkflowNode(id="nested", kind=kind, config=configuration)])
    value = [1, 2, 3] if kind == "fanout" else 1
    try:
        store = WorkflowCheckpointStore(database)
        handle = WorkflowEngine(max_concurrency=2).execute(
            graph, value, RunnerBinding(runner, retry_safe=True), checkpoint=store
        )
        await asyncio.wait_for(entered.wait(), 3)
        if kind == "fanout":
            # 等待数据库确认子运行 0 完成；runner 返回不等于结果已提交。
            async with asyncio.timeout(3):
                while True:
                    snapshot = (await store.load(handle.run_id)).state
                    if any(
                        child.input_value == 1 and child.status == "completed"
                        for child in snapshot.children.values()
                    ):
                        break
                    await asyncio.sleep(0.01)
        run_id = handle.run_id
        saved = await handle.suspend()
        assert saved.children
        restored = await WorkflowEngine(max_concurrency=2).restore(
            store, run_id, RunnerBinding(runner, retry_safe=True)
        )
        result = await restored.wait()
        assert result.status == "completed"
        assert result.output == ([2, 3, 4] if kind == "fanout" else 4 if kind == "loop" else 3)
        assert sum(value == 1 for value, _identifier in calls) == 1
        resumed_ids = [identifier for value, identifier in calls if value == 2]
        assert len(resumed_ids) == 2 and resumed_ids[0] == resumed_ids[1]
    finally:
        await database.close()


async def test_checkpoint_lease_heartbeat_revision_and_history_are_fenced(tmp_path):
    from sqlalchemy import text

    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'fenced.sqlite'}")
    await database.initialize()
    try:
        store = WorkflowCheckpointStore(database, lease_seconds=2)
        graph = WorkflowGraph(
            nodes=[WorkflowNode(id="value", kind="constant", config={"value": 1})]
        )
        handle = WorkflowEngine().execute(graph, None, checkpoint=store, pause_before={"value"})
        await asyncio.wait_for(paused(handle), 3)
        await asyncio.sleep(2.4)
        with pytest.raises(WorkflowError) as error:
            await store.claim(handle.run_id, "competitor")
        assert error.value.code == "checkpoint_busy"
        await handle.suspend()
        first = await store.claim(handle.run_id, "owner-one")
        async with database.engine.begin() as connection:
            await connection.execute(
                text("UPDATE workflow_checkpoints SET lease_expires_at=0 WHERE run_id=:run"),
                {"run": handle.run_id},
            )
        second = await store.claim(handle.run_id, "owner-two")
        with pytest.raises(WorkflowError) as error:
            await store.save(first.state, "owner-one", first.revision)
        assert error.value.code == "checkpoint_owner_lost"
        modified = second.state.model_copy(deep=True)
        modified.events[0].data = "overwrite-history"
        with pytest.raises(WorkflowError) as error:
            await store.save(modified, "owner-two", second.revision)
        assert error.value.code == "checkpoint_conflict"
        unchanged = await store.load(handle.run_id)
        assert (
            unchanged.revision == second.revision and unchanged.state.events == second.state.events
        )
        await store.release(handle.run_id, "owner-two")
    finally:
        await database.close()


async def test_branch_selection_and_rerun_epoch_survive_restore(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'branch.sqlite'}")
    await database.initialize()
    graph = WorkflowGraph(
        nodes=[
            WorkflowNode(id="choose", kind="branch", config={"operator": "truthy"}),
            WorkflowNode(id="yes", kind="output"),
            WorkflowNode(id="no", kind="output"),
        ],
        edges=[
            WorkflowEdge(source="choose", target="yes", when="true"),
            WorkflowEdge(source="choose", target="no", when="false"),
        ],
    )
    try:
        store = WorkflowCheckpointStore(database)
        handle = WorkflowEngine().execute(graph, True, checkpoint=store, pause_after={"choose"})
        await asyncio.wait_for(paused(handle), 3)
        run_id = handle.run_id
        await handle.suspend()
        recovered = await WorkflowEngine().restore(store, run_id)
        await asyncio.wait_for(paused(recovered, len(handle.events_log)), 3)
        await recovered.resume()
        first = await recovered.wait()
        assert first.output is True and first.node_statuses["no"] == "skipped"
        events = await recovered.events()
        again = await WorkflowEngine().restore(store, run_id)
        assert await again.wait() == first
        second = await again.rerun("choose", False)
        assert second.epoch == 1 and second.output is False
        assert second.node_statuses["yes"] == "skipped"
        assert (await store.load(run_id)).state.events[: len(events)] == events
    finally:
        await database.close()


async def test_restore_keeps_deadline_and_rejects_changed_options_or_corrupt_graph(tmp_path):
    from sqlalchemy import text

    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'budget.sqlite'}")
    await database.initialize()
    try:
        store = WorkflowCheckpointStore(database)
        graph = WorkflowGraph(
            nodes=[WorkflowNode(id="value", kind="constant", config={"value": 1})]
        )
        handle = WorkflowEngine(timeout_seconds=0.3).execute(
            graph, None, checkpoint=store, pause_before={"value"}
        )
        await asyncio.wait_for(paused(handle), 3)
        run_id = handle.run_id
        await handle.suspend()
        with pytest.raises(WorkflowError) as error:
            await WorkflowEngine().restore(store, run_id)
        assert error.value.code == "checkpoint_options_mismatch"
        await asyncio.sleep(0.35)
        restored = await WorkflowEngine(timeout_seconds=0.3).restore(store, run_id)
        result = await restored.wait()
        assert result.status == "failed" and result.code == "timeout"
        assert result.node_results == {}
        async with database.engine.begin() as connection:
            await connection.execute(
                text("UPDATE workflow_checkpoints SET graph_hash=:hash WHERE run_id=:run"),
                {"hash": "0" * 64, "run": run_id},
            )
        with pytest.raises(WorkflowError) as error:
            await store.load(run_id)
        assert error.value.code == "checkpoint_corrupt"
    finally:
        await database.close()


async def test_stop_waits_for_started_checkpoint_write_and_never_runs_cancelled_node(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'cancel-write.sqlite'}")
    await database.initialize()
    calls = []

    class DelayedStore(WorkflowCheckpointStore):
        async def save(self, state, owner_id, expected_revision):
            if state.events and state.events[-1].type == "node_start":
                entered.set()
                await release.wait()
            return await super().save(state, owner_id, expected_revision)

    async def runner(node, inputs, context):
        calls.append(context.execution_id)
        return inputs["in"]

    try:
        store = DelayedStore(database)
        handle = WorkflowEngine().execute(
            WorkflowGraph(nodes=[WorkflowNode(id="work", kind="tool")]),
            "value",
            RunnerBinding(runner, retry_safe=True),
            checkpoint=store,
        )
        await asyncio.wait_for(entered.wait(), 3)
        stopping = asyncio.create_task(handle.stop())
        await asyncio.sleep(0.02)
        assert not stopping.done()
        release.set()
        result = await stopping
        assert result.status == "aborted" and not calls
        snapshot = (await store.load(handle.run_id)).state
        assert snapshot.result == result
        assert [event.type for event in snapshot.events] == ["start", "node_start", "end"]
    finally:
        release.set()
        await database.close()


async def test_checkpoint_failure_never_publishes_uncommitted_node_or_executes_runner(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'failed-write.sqlite'}")
    await database.initialize()
    calls = []

    class FailedStore(WorkflowCheckpointStore):
        async def save(self, state, owner_id, expected_revision):
            if state.events and state.events[-1].type == "node_start":
                raise WorkflowError("checkpoint_store_failed", "controlled storage failure")
            return await super().save(state, owner_id, expected_revision)

    async def runner(node, inputs, context):
        calls.append(node.id)
        return inputs["in"]

    try:
        store = FailedStore(database)
        handle = WorkflowEngine().execute(
            WorkflowGraph(nodes=[WorkflowNode(id="work", kind="tool")]),
            None,
            RunnerBinding(runner, retry_safe=True),
            checkpoint=store,
        )
        with pytest.raises(WorkflowError) as error:
            await handle.wait()
        assert error.value.code == "checkpoint_store_failed" and not calls
        assert [event.type for event in handle.events_log] == ["start"]
        assert [event.type for event in (await store.load(handle.run_id)).state.events] == ["start"]
    finally:
        await database.close()
