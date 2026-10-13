"""原已生成完整树首次持久封存、条件/冷读/事务/当前调用门禁。"""

import asyncio
import copy
import json
import sys
from pathlib import Path
from threading import Event

import pytest
from sqlalchemy import event, update
from sqlalchemy.exc import IntegrityError
from test_completed_tree_origins import complete

from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.database import Database, RunRow, WorkspaceTreeSealRow
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace_tree import TreePlanModelSource
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def test_original_plan_seals_once_and_cold_reads_never_replace_same_bytes_new_inode(
    runtime, tmp_path, monkeypatch
):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    store = CompletedTreeSealStore(trees)
    try:
        sealed, created = await store.seal(
            scope, binding, plan.plan_id, snapshot.fingerprint, snapshot
        )
        assert created and sealed.origin.snapshot == snapshot
        assert sealed.origin.operation_ids == tuple(item["operation_id"] for item in outputs)
        old = tmp_path / "tree/sub/data.bin"
        old.rename(tmp_path / "retained.bin")
        old.write_bytes(snapshot.entries[-1].file.data)
        old.chmod(snapshot.entries[-1].file.mode)
        current = WorkspaceTreeSnapshotReader(binding.require()).capture("tree")
        assert current.fingerprint != snapshot.fingerprint

        def forbidden(*_args, **_kwargs):
            pytest.fail("重复原plan/冷读不能重新观察或换inode")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "verify", forbidden)
        assert await store.seal(scope, binding, plan.plan_id, snapshot.fingerprint, None) == (
            sealed,
            False,
        )
        assert await store.get(scope, binding, plan.plan_id) == sealed
        with pytest.raises(ServiceError) as change:
            await store.seal(scope, binding, plan.plan_id, current.fingerprint, current)
        assert change.value.code == "workspace_tree_seal_conflict"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["inode", "cancel", "quota", "authority"])
async def test_current_condition_or_cancellation_refuses_entire_first_seal(
    runtime, tmp_path, fault
):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    store = CompletedTreeSealStore(
        trees, max_snapshot_bytes=1024 if fault == "quota" else 536_870_912
    )
    abort = Event()
    try:
        if fault == "inode":
            file = tmp_path / "tree/sub/data.bin"
            file.rename(tmp_path / "retained.bin")
            file.write_bytes(snapshot.entries[-1].file.data)
            file.chmod(snapshot.entries[-1].file.mode)
        if fault == "cancel":
            abort.set()

        def authorize():
            if fault == "authority":
                raise ServiceError("original_authority_lost", "原当前资格失效", 409)

        with pytest.raises(ServiceError):
            await store.seal(
                scope,
                binding,
                plan.plan_id,
                snapshot.fingerprint,
                snapshot,
                cancelled=abort.is_set,
                authorize=authorize,
            )
        async with store.database.sessions() as sql:
            assert await sql.get(WorkspaceTreeSealRow, plan.plan_id) is None
    finally:
        await service.close()


async def test_model_run_source_stopped_run_and_cold_read_preserve_original_call(runtime, tmp_path):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    store = CompletedTreeSealStore(trees)
    try:
        run, _ = await runtime.repository.begin_run(
            service._handle(agent.agent_id).topic_id,
            ChatRequest(text="原封存调用", idempotency_key="seal"),
            agent_session_id=agent.session_id,
            agent_id=agent.agent_id,
            agent_owner_id=trees.owner_id,
        )
        source = TreePlanModelSource(run.id, "seal")
        async with store.database.sessions.begin() as sql:
            (await sql.get(RunRow, run.id)).stop_requested = True
        with pytest.raises(ServiceError):
            await store.seal(
                scope, binding, plan.plan_id, snapshot.fingerprint, snapshot, model_source=source
            )
        async with store.database.sessions.begin() as sql:
            (await sql.get(RunRow, run.id)).stop_requested = False
        value, created = await store.seal(
            scope, binding, plan.plan_id, snapshot.fingerprint, snapshot, model_source=source
        )
        assert created and value.model_source == source
        await runtime.repository.finish(run.id, "completed", None, [])
        assert await store.get(scope, binding, plan.plan_id) == value
        with pytest.raises(ServiceError):
            await store.seal(
                scope,
                binding,
                plan.plan_id,
                snapshot.fingerprint,
                None,
                model_source=TreePlanModelSource(run.id, "different"),
            )
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["blob", "proof", "scope", "operations"])
async def test_corrupt_or_foreign_sealed_fact_is_not_returned(runtime, tmp_path, fault):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    store = CompletedTreeSealStore(trees)
    try:
        await store.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot)
        if fault == "scope":
            other = await service.spawn("default")
            scope = "session:" + other.session_id
        else:
            async with store.database.sessions.begin() as sql:
                row = await sql.get(WorkspaceTreeSealRow, plan.plan_id)
                if fault == "blob":
                    row.payload = b"damaged"
                elif fault == "proof":
                    row.proof = {
                        **copy.deepcopy(row.proof),
                        "view": {**row.proof["view"], "tree_fingerprint": "0" * 64},
                    }
                else:
                    row.proof = {**copy.deepcopy(row.proof), "operation_ids": []}
        with pytest.raises(ServiceError):
            await store.get(scope, binding, plan.plan_id)
    finally:
        await service.close()


async def test_two_connections_seal_original_plan_once_without_second_observation(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    database = Database(str(trees.database.engine.url))
    await database.initialize()
    files = WorkspaceEditStore(database, owner_id=trees.owner_id)
    other = CompletedTreeSealStore(
        WorkspaceTreePlanStore(files, WorkspaceDirectoryStore(database, owner_id=trees.owner_id))
    )
    store = CompletedTreeSealStore(trees)
    try:
        values = await asyncio.gather(
            store.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot),
            other.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot),
        )
        assert sum(value[1] for value in values) == 1 and values[0][0] == values[1][0]
    finally:
        await database.close()
        await service.close()


async def test_source_seal_insert_failure_rolls_back_and_sql_pair_constraints_hold(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    store = CompletedTreeSealStore(trees)
    try:

        def fail(_conn, _cursor, statement, _params, _context, _many):
            if statement.startswith("INSERT INTO workspace_tree_completed_seals"):
                raise RuntimeError("封存写入失败")

        event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="封存写入失败"):
                await store.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot)
        finally:
            event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        assert await store.get(scope, binding, plan.plan_id) is None
        sealed, _ = await store.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot)
        for changes in (
            {"payload": None},
            {"metadata_value": None},
            {"command_id": "missing"},
            {"call_id": "half-source"},
        ):
            with pytest.raises(IntegrityError):
                async with store.database.sessions.begin() as sql:
                    await sql.execute(
                        update(WorkspaceTreeSealRow)
                        .where(WorkspaceTreeSealRow.plan_id == plan.plan_id)
                        .values(**changes)
                    )
        assert await store.get(scope, binding, plan.plan_id) == sealed
    finally:
        await service.close()


async def test_other_process_cold_reads_original_full_material_after_current_inode_changes(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    store = CompletedTreeSealStore(trees)
    process = None
    try:
        sealed, _ = await store.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot)
        file = tmp_path / "tree/sub/data.bin"
        file.rename(tmp_path / "retained.bin")
        file.write_bytes(snapshot.entries[-1].file.data)
        file.chmod(snapshot.entries[-1].file.mode)
        resolved = await asyncio.to_thread(Path(store.database.engine.url.database).resolve)
        url = str(store.database.engine.url.set(database=str(resolved)))
        script = Path(__file__).parent / "fixtures/completed_tree_seal_cold.py"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            url,
            str(tmp_path),
            scope,
            plan.plan_id,
            binding.fingerprint,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, err.decode()
        proof = json.loads(out)
        assert proof["plan_id"] == plan.plan_id and proof["fingerprint"] == snapshot.fingerprint
        assert (
            proof["inode"] == snapshot.entries[-1].file.inode
            and proof["inode"] != file.stat().st_ino
        )
        assert (
            proof["bytes_hex"] == snapshot.entries[-1].file.data.hex()
            and proof["replay_calls"] == 0
        )
        assert proof["operation_ids"] == list(sealed.origin.operation_ids)
    finally:
        if process is not None and process.returncode is None:
            process.kill()
            await process.communicate()
        await service.close()
