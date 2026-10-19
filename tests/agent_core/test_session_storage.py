"""真实SQLite版本、提交和来源接缝。"""

import asyncio
import json
import sqlite3

import pytest
from sqlalchemy import func, select

from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.models import runs, turns
from krim_agent.storage.sqlite import SQLiteStore


async def test_eval_consumers_use_actual_validated_schema(tmp_path):
    store = await SQLiteStore.open(tmp_path / "schema.sqlite")
    try:
        assert store.schema_version == 3
        with pytest.raises(ServiceError, match="not_found"):
            await store.dataset("not-created", "1")
    finally:
        await store.close()


def downgrade(path, version):
    conn = sqlite3.connect(path)
    try:
        for name in ["session_controls", "turns", "sessions"]:
            conn.execute('DROP TABLE "' + name + '"')
        if version == 1:
            for name in ["eval_items", "evaluations", "dataset_revisions"]:
                conn.execute('DROP TABLE "' + name + '"')
        conn.execute(f"PRAGMA user_version={version}")
        conn.commit()
    finally:
        conn.close()


def table_rows(path, names):
    conn = sqlite3.connect(path)
    try:
        return {name: conn.execute('SELECT * FROM "' + name + '"').fetchall() for name in names}
    finally:
        conn.close()


@pytest.mark.parametrize("version", [1, 2, 3])
async def test_v3_migration_atomic_and_old_rows_unchanged(tmp_path, version):
    from krim_agent.composition import open_application
    from krim_agent.contracts import RunRequest

    database = tmp_path / f"v{version}.sqlite"
    async with await open_application(database) as app:
        run = await app.service.start(RunRequest(text="/echo keep", request_key="legacy"))
        await app.service.wait(run.id)
        expected = (await app.service.result(run.id)).model_dump(mode="json")
    if version < 3:
        downgrade(database, version)
    names = ["runs", "run_events"] + (
        [] if version == 1 else ["dataset_revisions", "evaluations", "eval_items"]
    )
    rows = table_rows(database, names)
    before = database.read_bytes()
    store = await SQLiteStore.open(database, readonly=True)
    try:
        assert store.schema_version == version
        assert (await store.snapshot(run.id)).session_context is None
        assert (await store.snapshot(run.id)).request.text == "/echo keep"
        with pytest.raises(
            ServiceError, match="schema_upgrade_required" if version == 1 else "not_found"
        ):
            await store.dataset("absent", "1")
        with pytest.raises(
            ServiceError, match="not_found" if version == 3 else "schema_upgrade_required"
        ):
            await session_sql.get_session(store, "absent")
    finally:
        await store.close()
    assert database.read_bytes() == before
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == 3
        from krim_agent.runtime.facts import project_facts

        assert project_facts(await writer.snapshot(run.id)).model_dump(mode="json") == expected
        assert table_rows(database, names) == rows
    finally:
        await writer.close()


@pytest.mark.parametrize("failure", ["exception", "cancel"])
async def test_v3_migration_failure_rolls_back_before_unlock(tmp_path, monkeypatch, failure):
    database = tmp_path / "migration.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    downgrade(database, 2)
    before = database.read_bytes()
    original = SQLiteStore._upgrade

    async def injected(conn):
        await original(conn)
        if failure == "cancel":
            raise asyncio.CancelledError
        raise RuntimeError("injected migration failure")

    monkeypatch.setattr(SQLiteStore, "_upgrade", staticmethod(injected))
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(database)
    assert database.read_bytes() == before
    monkeypatch.setattr(SQLiteStore, "_upgrade", staticmethod(original))
    probe = await SQLiteStore.open(database)
    await probe.close()


@pytest.mark.parametrize("bad", ["version", "column", "unique", "table"])
async def test_unknown_or_mismatched_schema_is_rejected(tmp_path, bad):
    database = tmp_path / "invalid.sqlite"
    store = await SQLiteStore.open(database)
    await store.close()
    conn = sqlite3.connect(database)
    try:
        if bad == "version":
            conn.execute("PRAGMA user_version=99")
        elif bad == "column":
            conn.execute("ALTER TABLE runs ADD COLUMN surprise TEXT")
        elif bad == "unique":
            conn.execute("CREATE UNIQUE INDEX surprise_unique ON runs(trace_id)")
        else:
            conn.execute("CREATE TABLE surprise(value TEXT)")
        conn.commit()
    finally:
        conn.close()
    before = database.read_bytes()
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(database)
    assert database.read_bytes() == before


@pytest.mark.parametrize("phase", ["create_session", "submit", "begin_run", "finish"])
async def test_turn_keys_and_link_commit_loss_never_reexecute(tmp_path, wire, monkeypatch, phase):
    database = tmp_path / "lost.sqlite"
    async with wire.application(database) as app:
        original = app.store._write
        injected = []

        async def lost(operation):
            value = await original(operation)
            if phase in operation.__qualname__ and not injected:
                injected.append(operation.__qualname__)
                raise RuntimeError("committed return lost")
            return value

        monkeypatch.setattr(app.store, "_write", lost)
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key="original-session")
        )
        body = TurnRequest(request_key="original-turn", text="/add 2 3", expected_revision=0)
        turn = await app.sessions.submit(LOCAL, "local", session.id, body)
        turn = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert turn.status == "completed" and injected
        assert (await app.sessions.submit(LOCAL, "local", session.id, body)).id == turn.id
        assert (
            await app.sessions.create(LOCAL, "local", SessionCreate(request_key="original-session"))
        ).id == session.id
        assert len(wire.requests) == 2 and len(wire.executions) == 1
        async with app.store.lock, app.store.engine.begin() as conn:
            assert await conn.scalar(select(func.count()).select_from(runs)) == 1
            assert await conn.scalar(select(func.count()).select_from(turns)) == 1


async def test_failed_link_commit_never_dispatches_and_restart_pauses(tmp_path, wire, monkeypatch):
    database = tmp_path / "link-failure.sqlite"
    async with wire.application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        original = app.store._write
        failed = asyncio.Event()

        async def failure(operation):
            if "begin_run" in operation.__qualname__:

                async def fail_inside(conn):
                    await operation(conn)
                    raise RuntimeError("rollback link")

                try:
                    return await original(fail_inside)
                finally:
                    failed.set()
            return await original(operation)

        monkeypatch.setattr(app.store, "_write", failure)
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(request_key="pending", text="/add 2 3", expected_revision=0),
        )
        await asyncio.wait_for(failed.wait(), 5)
        assert not app.store.healthy
        assert not wire.requests and not wire.executions
        found = await app.sessions.turn_by_key(LOCAL, "local", session.id, "pending")
        assert found.id == turn.id and found.run_id is None
        async with app.store.lock, app.store.engine.begin() as conn:
            assert await conn.scalar(select(func.count()).select_from(runs)) == 0
    from krim_agent.composition import open_application

    async with await open_application(database) as restarted:
        assert (await restarted.sessions.get(LOCAL, "local", session.id)).dispatch_paused
        assert (
            await restarted.sessions.turn_by_key(LOCAL, "local", session.id, "pending")
        ).run_id is None
        assert restarted.provider.calls == 0


async def test_corrupted_session_source_never_falls_back_to_legacy(tmp_path, wire):
    from sqlalchemy import update

    database = tmp_path / "corrupt.sqlite"
    async with wire.application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="source"))
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(request_key="one", text="/add 2 3", expected_revision=0),
        )
        turn = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        async with app.store.lock, app.store.engine.begin() as conn:
            await conn.execute(
                update(turns).where(turns.c.id == turn.id).values(context_fingerprint="0" * 64)
            )
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await app.evaluations.candidate(turn.run_id)


@pytest.mark.parametrize(
    "phase,requests,executions", [("link", 0, 0), ("provider", 1, 0), ("tool", 1, 1)]
)
async def test_restart_interrupts_and_pauses_queue_without_replay(
    tmp_path, wire, phase, requests, executions
):
    import os
    import sys
    from pathlib import Path

    from krim_agent.composition import open_application

    if phase == "provider":
        wire.hold_at.add(1)
    database = tmp_path / "crash.sqlite"
    marker = tmp_path / "marker.json"
    effects = tmp_path / "effects.jsonl"
    worker = Path(__file__).parent / "fixtures/session_crash_worker.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-B",
        str(worker),
        "--database",
        str(database),
        "--marker",
        str(marker),
        "--effects",
        str(effects),
        "--base-url",
        f"http://127.0.0.1:{wire.port}/v1",
        "--phase",
        phase,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(8):
            while not marker.exists():
                if child.returncode is not None:
                    raise AssertionError("worker exited before barrier")
                await asyncio.sleep(0.005)
        info = json.loads(marker.read_text())
        if phase == "provider":
            await wire.wait_requests(1)
        assert len(wire.requests) == requests
        assert len(effects.read_text().splitlines()) == executions
        child.kill()
        stdout, stderr = await asyncio.wait_for(child.communicate(), 5)
        assert child.returncode == -9, (stdout, stderr)
        (tmp_path / "ownership.json").write_text(
            json.dumps(
                {
                    "creator_pid": os.getpid(),
                    "owned_child_namespace_pid": child.pid,
                    "exit_code": child.returncode,
                    "phase": phase,
                    "http": len(wire.requests),
                    "tool_entries": executions,
                    "stderr": stderr.decode(),
                },
                indent=2,
            )
        )
    finally:
        if child.returncode is None:
            child.kill()
            await child.communicate()
    wire.release.set()
    before = database.read_bytes()
    readonly = await SQLiteStore.open(database, readonly=True)
    try:
        original = await session_sql.get_turn(readonly, info["first"])
        assert original.status == "running" and original.run_id is not None
        assert (await session_sql.get_turn(readonly, info["queued"])).run_id is None
    finally:
        await readonly.close()
    assert database.read_bytes() == before
    probe = await SQLiteStore.open(database)
    await probe.close()
    async with await open_application(database) as app:
        assert (await app.sessions.get(LOCAL, "local", info["session_id"])).dispatch_paused
        first = await app.sessions.turn_by_key(LOCAL, "local", info["session_id"], "active")
        assert first.status == "interrupted"
        snapshot = await app.store.snapshot(first.run_id)
        assert snapshot.run.status == "interrupted"
        assert len([e for e in snapshot.events if e.kind == "terminal"]) == 1
        assert len([e for e in snapshot.events if e.kind == "model_end"]) == (
            1 if phase == "tool" else 0
        )
        if phase == "tool":
            facts = await app.service.result(first.run_id)
            assert facts.tools[0].status == "unknown" and facts.tools[0].result_available is None
        assert app.provider.calls == 0 and len(wire.requests) == requests
        assert len(effects.read_text().splitlines()) == executions
        assert (
            await app.sessions.turn_by_key(LOCAL, "local", info["session_id"], "queued")
        ).run_id is None
