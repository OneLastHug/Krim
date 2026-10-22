"""D1_DATA：生产 SQLite 的版本、工作关联与未知回程事实。"""

from contextlib import closing
from pathlib import Path

import pytest

from krim_agent.storage.sqlite import SQLiteStore


@pytest.mark.parametrize(
    "case",
    [
        "new",
        "legacy",
        "readonly1",
        "readonly2",
        "readonly3",
        "readonly4",
        "ddl-work_sessions",
        "ddl-work_turns",
        "ddl-compactions",
        "ddl-tool_effects",
        "ddl-artifact_revisions",
        "commit",
        "unknown5",
    ],
)
async def test_schema4_upgrade_atomic_and_legacy_rows_readonly(tmp_path, case, legacy_schema3):
    import asyncio
    import hashlib
    import json
    import os
    import sqlite3

    from fixtures.workspace_crash_worker import watch_store
    from sqlalchemy import event
    from sqlalchemy.engine import Engine

    import krim_agent.storage.sqlite as consumer
    from krim_agent.contracts import ServiceError, fingerprint
    from krim_agent.observability import project_trace
    from krim_agent.runtime.facts import project_facts

    resolved = await asyncio.to_thread(Path(consumer.__file__).resolve)
    assert resolved.is_relative_to(Path.cwd() / "src")
    database = tmp_path / "data.sqlite"
    parent, seed = legacy_schema3
    if case != "new":
        with closing(sqlite3.connect(f"file:{parent}?mode=ro", uri=True)) as source, source:
            with closing(sqlite3.connect(database)) as dest, dest:
                source.backup(dest)

    def rows():
        with closing(sqlite3.connect(f"file:{database}?mode=ro", uri=True)) as conn, conn:
            names = [
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            ]
            return {n: conn.execute('SELECT * FROM "' + n + '"').fetchall() for n in names}

    original = rows() if case != "new" else {}
    if case == "unknown5":
        with closing(sqlite3.connect(database)) as conn, conn:
            conn.execute("PRAGMA user_version=5")
        before = database.read_bytes()
        with pytest.raises(ServiceError, match="database_invalid"):
            await SQLiteStore.open(database)
        assert database.read_bytes() == before
        return
    if case.startswith("readonly"):
        version = int(case[-1])
        if version < 3:
            with closing(sqlite3.connect(database)) as conn, conn:
                for table in ["session_controls", "turns", "sessions"] + (
                    ["eval_items", "evaluations", "dataset_revisions"] if version == 1 else []
                ):
                    conn.execute('DROP TABLE "' + table + '"')
                conn.execute(f"PRAGMA user_version={version}")
        if version == 4:
            writer = await SQLiteStore.open(database)
            await writer.close()
        before = database.read_bytes()
        reader = await SQLiteStore.open(database, readonly=True)
        try:
            assert reader.schema_version == version
            assert (await reader.snapshot(seed["run_id"])).run.status == "completed"
            if version == 1:
                with pytest.raises(ServiceError, match="schema_upgrade_required"):
                    await reader.dataset("legacy", "1")
            else:
                assert (await reader.dataset("legacy", "1")).name == "legacy"
        finally:
            await reader.close()
        assert database.read_bytes() == before
        return
    if case.startswith("ddl-") or case == "commit":
        before = database.read_bytes()
        hits = []

        def fail_sql(conn, cursor, statement, parameters, context, executemany):
            if case.startswith("ddl-") and ("CREATE TABLE " + case[4:]) in statement:
                hits.append(statement)
                raise OSError("injected exact DDL")

        def fail_commit(conn):
            if case == "commit":
                hits.append("commit")
                raise OSError("injected actual commit")

        event.listen(Engine, "before_cursor_execute", fail_sql)
        event.listen(Engine, "commit", fail_commit)
        try:
            with pytest.raises(ServiceError, match="database_invalid"):
                await SQLiteStore.open(database)
        finally:
            event.remove(Engine, "before_cursor_execute", fail_sql)
            event.remove(Engine, "commit", fail_commit)
        assert hits
        assert database.read_bytes() == before
        assert rows() == original
    store = await SQLiteStore.open(database)
    events = watch_store(store)
    fd = store.fd
    try:
        assert store.schema_version == 4
        if original:
            assert {k: v for k, v in rows().items() if k in original} == original
            assert (
                project_facts(await store.snapshot(seed["run_id"])).model_dump(mode="json")
                == seed["facts"]
            )
            assert (
                project_trace(await store.snapshot(seed["run_id"])).model_dump(mode="json")
                == seed["trace"]
            )
            report = (await store.eval_snapshot(seed["eval_id"]))["report"]
            assert fingerprint(report) == fingerprint(seed["report"])
            assert (await store.by_key("active")).id == seed["active_id"]
            assert (await store.snapshot(seed["active_id"])).run.status == "queued"
    finally:
        await store.close()
    assert events.count("checkout") == events.count("checkin")
    with pytest.raises(OSError):
        os.fstat(fd)
    second = await SQLiteStore.open(database)
    await second.close()
    (tmp_path / "resources.json").write_text(
        json.dumps(
            {
                "node": case,
                "pool": events,
                "writer_closed": store.closed,
                "second_writer_closed": second.closed,
                "db_hash": hashlib.sha256(database.read_bytes()).hexdigest(),
            }
        )
    )


@pytest.mark.parametrize(
    "phase",
    [
        "link",
        "before_commit",
        "after_commit",
        "ready",
        "payload_output",
        "export_chunk",
        "after_cas",
    ],
)
async def test_work_link_commit_unknown_and_restart_zero_replay(tmp_path, phase):
    if phase in {"before_commit", "after_commit"}:
        await association_commit(tmp_path, phase)
        return
    if phase != "link":
        from fixtures.workspace_crash_worker import verify_crash

        await verify_crash(tmp_path, phase)
        return
    from fixtures.workspace_crash_worker import (
        create_work,
        effect_intent,
        execute,
        start_turn,
        target_for,
        watch_store,
    )

    from krim_agent.config import Settings
    from krim_agent.contracts import ServiceError
    from krim_agent.security import LOCAL
    from krim_agent.storage import sessions as sql
    from krim_agent.workspace.artifacts import ArtifactStore
    from krim_agent.workspace.types import (
        Completion,
        ExecutionAccess,
        InputSelector,
        WorkspaceTurnRequest,
    )

    store = await SQLiteStore.open(tmp_path / "work.sqlite")
    events = watch_store(store)
    artifacts = ArtifactStore(store, tmp_path / "objects")
    try:
        session = await create_work(store)
        access = ExecutionAccess(LOCAL, "local", session.id)
        r0 = await artifacts.head(access)
        assert r0.revision == 0
        turn, run, body, created = await start_turn(store, access)
        assert created
        snap = await store.snapshot(run.id)
        assert snap.work.original_request == body
        assert snap.work.input_revision.revision_id == r0.revision_id
        second = WorkspaceTurnRequest(
            request_key="next", text="next", expected_revision=1, completion=Completion()
        )
        queued = await sql.submit(store, session.id, second, Settings(), access=access)
        from sqlalchemy import select

        from krim_agent.storage.models import work_turns

        async with store.engine.begin() as conn:
            pending = (
                (await conn.execute(select(work_turns).where(work_turns.c.turn_id == queued.id)))
                .mappings()
                .one()
            )
        assert pending["requested_input"] == {"mode": "head_at_dispatch", "revision_id": None}
        assert pending["resolved_input_revision"] is None and pending["run_id"] is None
        cid = await effect_intent(store, access, run)
        receipt = await execute("link-" + tmp_path.name)
        r1 = await artifacts.publish(
            access, key="effect", expected=0, run_id=run.id, call_id=cid, receipt=receipt
        )
        assert r1.revision == 1 and r1.producer_run_id == run.id
        await store.finish(run.id, "failed", "completion_unverified")
        request, _ = await store.turn_request(queued.id)
        run2, _ = await store.begin_turn_run(queued.id, request, target_for(), Settings())
        snap2 = await store.snapshot(run2.id)
        assert snap2.work.input_revision.revision_id == r1.revision_id
        again = await sql.submit(store, session.id, body, Settings(), access=access)
        assert again.id == turn.id
        same, is_new = await store.begin_turn_run(
            turn.id, (await store.turn_request(turn.id))[0], target_for(), Settings()
        )
        assert same.id == run.id and is_new is False
        await store.finish(run2.id, "failed", "completion_unverified")
        stale = WorkspaceTurnRequest(
            request_key="stale",
            text="old revision",
            expected_revision=2,
            input=InputSelector(mode="revision", revision_id=r0.revision_id),
            completion=Completion(),
        )
        old = await sql.submit(store, session.id, stale, Settings(), access=access)
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await store.begin_turn_run(
                old.id, (await store.turn_request(old.id))[0], target_for(), Settings()
            )
        from sqlalchemy import update

        from krim_agent.contracts import RunRequest

        legacy, _ = await store.begin(
            RunRequest(text="legacy", request_key="ac-turn.legitimate"), {}
        )
        assert (await store.snapshot(legacy.id)).session_context is None
        async with store.engine.begin() as conn:
            await conn.execute(
                update(work_turns).where(work_turns.c.turn_id == turn.id).values(run_id=None)
            )
        try:
            with pytest.raises(ServiceError, match="facts_corrupt"):
                await store.snapshot(run.id)
        finally:
            async with store.engine.begin() as conn:
                await conn.execute(
                    update(work_turns).where(work_turns.c.turn_id == turn.id).values(run_id=run.id)
                )
        with pytest.raises(ServiceError, match="workspace_contract_required"):
            from krim_agent.sessions.types import TurnRequest

            await sql.submit(
                store,
                session.id,
                TurnRequest(
                    request_key="v2-bypass", text="missing conditions", expected_revision=3
                ),
                Settings(),
                access=access,
            )
        assert snap2.work.source_refs[0]["terminal_seq"] >= 0
        await store.recover()
        assert (await artifacts.get(access, r0.revision_id)).manifest == ()
        assert (
            await artifacts.by_key(access, "effect", kind="effect")
        ).revision_id == r1.revision_id
    finally:
        await artifacts.close()
        await store.close()
    assert events.count("checkout") == events.count("checkin")


async def association_commit(tmp_path, phase):
    from fixtures.workspace_crash_worker import create_work, target_for
    from sqlalchemy import event, func, select

    from krim_agent.config import Settings
    from krim_agent.contracts import ServiceError
    from krim_agent.security import LOCAL
    from krim_agent.storage import sessions as sql
    from krim_agent.storage.models import turns, work_turns
    from krim_agent.workspace.types import Completion, ExecutionAccess, WorkspaceTurnRequest

    store = await SQLiteStore.open(tmp_path / "commit.sqlite")
    try:
        session = await create_work(store)
        access = ExecutionAccess(LOCAL, "local", session.id)
        body = WorkspaceTurnRequest(
            request_key="commit",
            text="keep full conditions",
            expected_revision=0,
            completion=Completion(required_paths=("output.json",)),
        )
        lost = []
        if phase == "before_commit":

            def fail_commit(conn):
                if not lost:
                    lost.append("actual before_commit")
                    raise OSError("before commit")

            event.listen(store.engine.sync_engine, "commit", fail_commit)
            try:
                with pytest.raises(ServiceError, match="storage_unavailable"):
                    await sql.submit(store, session.id, body, Settings(), access=access)
            finally:
                event.remove(store.engine.sync_engine, "commit", fail_commit)
            assert lost
            async with store.engine.begin() as conn:
                assert await conn.scalar(select(func.count()).select_from(turns)) == 0
                assert await conn.scalar(select(func.count()).select_from(work_turns)) == 0
            assert (await sql.get_session(store, session.id)).revision == 0
            # 关闭失败writer再显式重开；没有恢复或自动重执行。
            await store.close()
            store = await SQLiteStore.open(tmp_path / "commit.sqlite")
        original = store._write

        async def lose(operation):
            result = await original(operation)
            if (
                phase == "after_commit"
                and not lost
                and operation.__qualname__.startswith("submit.")
            ):
                lost.append("commit ack lost")
                raise OSError("after actual commit")
            return result

        store._write = lose
        turn = await sql.submit(store, session.id, body, Settings(), access=access)
        assert lost
        assert (await sql.submit(store, session.id, body, Settings(), access=access)).id == turn.id
        request, _ = await store.turn_request(turn.id)
        run, created = await store.begin_turn_run(turn.id, request, target_for(), Settings())
        assert created
        frame = (await store.snapshot(run.id)).work
        assert frame.original_request == body
        assert frame.original_request.completion.required_paths == ("output.json",)
        async with store.engine.begin() as conn:
            assert await conn.scalar(select(func.count()).select_from(turns)) == 1
            assert await conn.scalar(select(func.count()).select_from(work_turns)) == 1
    finally:
        await store.close()


async def test_workspace_request_snapshot_before_wait(tmp_path, monkeypatch, request):
    import asyncio
    import json

    from fixtures.workspace_crash_worker import (
        create_work,
        repair_observe,
        target_for,
    )
    from sqlalchemy import select

    from krim_agent.config import Settings
    from krim_agent.contracts import ServiceError, fingerprint
    from krim_agent.security import LOCAL
    from krim_agent.storage import sessions as sql
    from krim_agent.storage.models import turns, work_turns
    from krim_agent.workspace.types import ExecutionAccess, WorkspaceTurnRequest

    async with repair_observe(tmp_path, request.node.nodeid, monkeypatch) as (watch, notes):
        database = tmp_path / "snapshot.sqlite"
        store = watch(await SQLiteStore.open(database), database)
        task = None
        locked = False
        try:
            session = await create_work(store)
            access = ExecutionAccess(LOCAL, "local", session.id)
            body = WorkspaceTurnRequest.model_validate_json(
                json.dumps(
                    {
                        "request_key": "nested",
                        "text": "freeze nested conditions",
                        "expected_revision": 0,
                        "completion": {
                            "conditions": [
                                {
                                    "id": "value",
                                    "kind": "json_equal",
                                    "path": "result.json",
                                    "expected": {"nested": [{"value": "original"}, False, None]},
                                }
                            ]
                        },
                    }
                )
            )
            original_json = body.model_dump_json()
            original = json.loads(original_json)
            entered = asyncio.Event()
            actual_write = store._write

            async def writing(operation):
                entered.set()
                return await actual_write(operation)

            monkeypatch.setattr(store, "_write", writing)
            await store.lock.acquire()
            locked = True
            task = asyncio.create_task(
                sql.submit(store, session.id, body, Settings(), access=access)
            )
            async with asyncio.timeout(5):
                await entered.wait()
            assert not task.done()
            body.completion.conditions[0].expected["nested"][0]["value"] = "changed while waiting"
            body.completion.conditions[0].expected["nested"].append({"later": [1, 2]})
            store.lock.release()
            locked = False
            turn = await task
            async with store.engine.begin() as conn:
                old = (
                    (await conn.execute(select(turns).where(turns.c.id == turn.id)))
                    .mappings()
                    .one()
                )
                work = (
                    (await conn.execute(select(work_turns).where(work_turns.c.turn_id == turn.id)))
                    .mappings()
                    .one()
                )
            notes.update(
                {
                    "database": str(database),
                    "turn_id": turn.id,
                    "turn_hash": old["request_hash"],
                    "work_hash": work["request_hash"],
                    "stored_request": work["request_json"],
                    "original": original,
                }
            )
            assert work["request_json"] == original
            assert old["request_hash"] == work["request_hash"] == fingerprint(original)
            assert work["requested_input"] == original["input"]
            accepted = WorkspaceTurnRequest.model_validate_json(original_json)
            same = await sql.submit(store, session.id, accepted, Settings(), access=access)
            assert same.id == turn.id
            with pytest.raises(ServiceError, match="key_conflict"):
                await sql.submit(store, session.id, body, Settings(), access=access)
            run, fresh = await store.begin_turn_run(
                turn.id, (await store.turn_request(turn.id))[0], target_for(), Settings()
            )
            assert fresh and (await store.snapshot(run.id)).work.original_request == accepted
            assert (await sql.get_turn(store, turn.id)).run_id == run.id
            await store.finish(run.id, "failed", "completion_unverified")
            notes.update({"run_id": run.id, "exec_count": 0, "original_key_same_turn": True})
        finally:
            if locked:
                store.lock.release()
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await store.close()
