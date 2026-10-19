"""合成库的M2迁移、关联与提交边界。"""

import asyncio
import hashlib
import json
import sqlite3

import pytest

from krim_agent.storage.sqlite import SQLiteStore


def v1(path):
    from sqlalchemy.dialects.sqlite import dialect
    from sqlalchemy.schema import CreateTable

    from krim_agent.storage.models import run_events, runs
    from krim_agent.storage.sqlite import APPLICATION_ID

    with sqlite3.connect(path) as conn:
        conn.execute(str(CreateTable(runs).compile(dialect=dialect())))
        conn.execute(str(CreateTable(run_events).compile(dialect=dialect())))
        conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
        conn.execute("PRAGMA user_version=1")


@pytest.mark.asyncio
async def test_v1_upgrade_atomic_and_readonly_pure(tmp_path):
    from krim_agent.contracts import ServiceError

    path = tmp_path / "v1.sqlite"
    v1(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    reader = await SQLiteStore.open(path, readonly=True)
    try:
        assert reader.schema_version == 1
        with pytest.raises(ServiceError, match="schema_upgrade_required"):
            await reader.eval_by_key("absent")
    finally:
        await reader.close()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    store = await SQLiteStore.open(path)
    try:
        async with store.engine.begin() as conn:
            version = (await conn.exec_driver_sql("PRAGMA user_version")).scalar_one()
        assert version == 3
        async with store.engine.begin() as conn:
            tables = (
                (await conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'"))
                .scalars()
                .all()
            )
        assert set(tables) == {
            "runs",
            "run_events",
            "dataset_revisions",
            "evaluations",
            "eval_items",
            "sessions",
            "turns",
            "session_controls",
        }
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_migration_failure_keeps_v1(tmp_path, monkeypatch):
    from krim_agent.contracts import ServiceError
    from krim_agent.storage.models import dataset_revisions

    path = tmp_path / "rollback.sqlite"
    v1(path)

    async def fail(conn):
        await conn.run_sync(dataset_revisions.create)
        raise OSError("SYNTHETIC_DDL_FAILURE")

    monkeypatch.setattr(SQLiteStore, "_upgrade", staticmethod(fail))
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(path)
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        assert {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        } == {"runs", "run_events"}


@pytest.mark.asyncio
@pytest.mark.parametrize("point", ["begin_eval", "commit_item"])
async def test_eval_key_and_admission_commit_loss(tmp_path, point):
    from test_evaluation import one_request

    from krim_agent.composition import open_application
    from krim_agent.contracts import ServiceError

    async with await open_application(tmp_path / (point + ".sqlite")) as app:
        request = await one_request(app)
        original = app.store._write
        lost = False

        async def lose(operation):
            nonlocal lost
            value = await original(operation)
            if point + ".<locals>.operation" in operation.__qualname__ and not lost:
                lost = True
                raise OSError("COMMIT_RETURN_LOST")
            return value

        app.store._write = lose
        view = await app.evaluations.start(request)
        await app.evaluations.wait(view.id)
        assert lost and (await app.evaluations.start(request)).id == view.id
        with pytest.raises(ServiceError, match="key_conflict"):
            await app.evaluations.start(request.model_copy(update={"target_key": "target-v1"}))
        assert app.service.bindings["target-v2"].loop.provider.calls == 2
        assert (await app.evaluations.report(view.id))["complete"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["lost", "rollback", "read_failure", "cancel"])
async def test_child_link_failure_or_lost_return_never_restarts(tmp_path, mode):
    from test_evaluation import one_request

    from krim_agent.composition import open_application
    from krim_agent.contracts import ServiceError

    async with await open_application(tmp_path / (mode + ".sqlite")) as app:
        request = await one_request(app)
        entered = asyncio.Event()
        release = asyncio.Event()
        original = app.store._write
        once = False

        async def fault(operation):
            nonlocal once
            match = "attach_child.<locals>.operation" in operation.__qualname__ and not once
            if not match:
                return await original(operation)
            once = True
            await app.service.bindings["target-v2"].loop.provider.entered.wait()
            entered.set()
            await release.wait()
            if mode in {"rollback", "read_failure"}:

                async def rollback(conn):
                    await operation(conn)
                    raise OSError("REAL_LINK_ROLLBACK")

                return await original(rollback)
            value = await original(operation)
            if mode == "lost":
                raise OSError("REAL_LINK_RETURN_LOST")
            return value

        app.store._write = fault
        stopped = asyncio.Event()
        stop_original = app.store.stop_eval

        async def stop(*args):
            await stop_original(*args)
            stopped.set()

        app.store.stop_eval = stop
        view = await app.evaluations.start(request)
        await asyncio.wait_for(entered.wait(), 3)
        if mode == "read_failure":

            async def unavailable(*args):
                raise ServiceError("storage_unavailable", 503)

            app.store.snapshot = unavailable
        waiting = asyncio.create_task(
            app.evaluations.cancel(view.id) if mode == "cancel" else app.evaluations.wait(view.id)
        )
        if mode == "cancel":
            await asyncio.wait_for(stopped.wait(), 3)
            assert not waiting.done()
        release.set()
        result = await asyncio.wait_for(waiting, 5)
        assert result.status == (
            "completed" if mode == "lost" else "aborted" if mode == "cancel" else "failed"
        )
        assert app.service.bindings["target-v2"].loop.provider.calls <= 2
        assert all(task.done() for task in app.service.tasks.values())
        assert app.service.bindings["target-v2"].loop.provider.active == 0
        snapshot = await app.store.eval_snapshot(view.id)
        assert len(snapshot["items"]) == 1
        before = app.service.bindings["target-v2"].loop.provider.calls
        await app.evaluations.report(view.id)
        assert app.service.bindings["target-v2"].loop.provider.calls == before
        if mode in {"rollback", "read_failure"}:
            assert not app.store.healthy
            with pytest.raises(ServiceError, match="storage_unavailable"):
                await app.evaluations.start(request.model_copy(update={"request_key": "new"}))


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", [True, False])
async def test_final_report_transaction_and_lost_return(tmp_path, lost):
    from test_evaluation import one_request

    from krim_agent.composition import open_application

    async with await open_application(tmp_path / ("final-" + str(lost) + ".sqlite")) as app:
        request = await one_request(app)
        original = app.store._write
        once = False

        async def fault(operation):
            nonlocal once
            if "commit_item.<locals>.operation" not in operation.__qualname__ or once:
                return await original(operation)
            once = True
            if lost:
                await original(operation)
                raise OSError("REPORT_RETURN_LOST")

            async def rollback(conn):
                await operation(conn)
                raise OSError("REPORT_TRANSACTION_ROLLBACK")

            return await original(rollback)

        app.store._write = fault
        view = await app.evaluations.start(request)
        result = await app.evaluations.wait(view.id)
        report = await app.evaluations.report(view.id)
        assert result.status == ("completed" if lost else "failed")
        assert report["complete"] is lost
        assert (report["items"][0]["scores"] is not None) is lost
        assert app.service.bindings["target-v2"].loop.provider.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["association_before", "association_after", "scorer", "final"])
async def test_restart_interrupts_without_replay(tmp_path, mode):
    import sys
    from pathlib import Path

    from krim_agent.composition import open_application

    root = tmp_path / mode
    root.mkdir()
    database = root / "crash.sqlite"
    worker = Path(__file__).parent / "fixtures/eval_crash_worker.py"
    argv = [
        sys.executable,
        "-I",
        "-B",
        str(worker),
        "--source-test",
        "--database",
        str(database),
        "--evidence",
        str(root),
        "--mode",
        mode,
    ]
    with (root / "worker.stdout").open("w") as out, (root / "worker.stderr").open("w") as err:
        process = await asyncio.create_subprocess_exec(*argv, cwd=root, stdout=out, stderr=err)
        try:
            async with asyncio.timeout(8):
                while not (root / "ready.json").exists():
                    assert process.returncode is None, (root / "worker.stderr").read_text()
                    await asyncio.sleep(0.01)
            process.kill()
            code = await asyncio.wait_for(process.wait(), 5)
            assert code == -9
        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), 5)
    before = (root / "counts.jsonl").read_bytes()
    async with await open_application(database) as app:
        view = await app.evaluations.by_key("crash-eval")
        assert view.status == "interrupted"
        report = await app.evaluations.report(view.id)
        assert report["complete"] is False and report["items"][0]["scores"] is None
        assert report["items"][0]["run_id"] is not None
        assert app.evaluations.tasks == {} and app.service.tasks == {}
        for binding in app.service.bindings.values():
            assert binding.loop.provider.calls == 0
        await app.evaluations.get(view.id)
        await app.evaluations.report(view.id)
        assert (root / "counts.jsonl").read_bytes() == before
    frozen = hashlib.sha256(database.read_bytes()).hexdigest()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        snapshot = await reader.eval_by_key("crash-eval")
        assert snapshot["status"] == "interrupted"
    finally:
        await reader.close()
    assert hashlib.sha256(database.read_bytes()).hexdigest() == frozen
    assert (root / "counts.jsonl").read_bytes() == before
    (root / "crash-proof.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "exit": code,
                "mode": mode,
                "provider_scorer_counts": before.decode().splitlines(),
                "readonly_zero_writes": True,
                "zero_replay": True,
            },
            indent=2,
        )
    )
