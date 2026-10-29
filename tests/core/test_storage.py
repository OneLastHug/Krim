import hashlib
import os
import sqlite3

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import RunEvent, RunRequest, ServiceError
from krim_agent.storage.sqlite import APPLICATION_ID, SQLiteStore


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["old", "version", "link", "fifo", "parent-link", "missing-readonly"]
)
async def test_store_refuses_foreign_version_nonregular_links_and_missing_readonly(tmp_path, kind):
    path = tmp_path / "target.db"
    if kind in {"old", "version"}:
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE old_data(x)")
            if kind == "version":
                conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
                conn.execute("PRAGMA user_version=99")
    elif kind == "link":
        target = tmp_path / "real.db"
        target.touch()
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "parent-link":
        (tmp_path / "real").mkdir()
        (tmp_path / "alias").symlink_to(tmp_path / "real")
        path = tmp_path / "alias" / "x.db"
    before = path.read_bytes() if kind in {"old", "version"} else None
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(path, readonly=kind == "missing-readonly")
    if before is not None:
        assert path.read_bytes() == before
    if kind == "missing-readonly":
        assert not path.exists()


@pytest.mark.asyncio
async def test_second_writer_refused_before_recovery_and_readonly_never_writes(tmp_path):
    path = tmp_path / "owned.db"
    store = await SQLiteStore.open(path)
    try:
        run, _ = await store.begin(RunRequest(text="x", request_key="live"), {})
        with pytest.raises(ServiceError, match="database_busy"):
            await SQLiteStore.open(path)
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        reader = await SQLiteStore.open(path, readonly=True)
        try:
            assert (await reader.snapshot(run.id)).run.status == "queued"
            with pytest.raises(ServiceError, match="readonly"):
                await reader.recover()
        finally:
            await reader.close()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == before
        assert (await store.snapshot(run.id)).run.status == "queued"
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("point", ["tool_intent", "tool_end", "terminal"])
async def test_transaction_failure_rolls_back_and_disables_admission(tmp_path, point, registry_for):
    calls = []

    async def execute(args):
        calls.append(1)
        return "real"

    app = await open_application(tmp_path / (point + ".db"), registry=registry_for(execute))
    original = app.store._event

    async def fail(conn, run_id, kind, *args, **kwargs):
        result = await original(conn, run_id, kind, *args, **kwargs)
        if kind == point:
            raise OSError("SYNTHETIC_SQL_SECRET")
        return result

    app.store._event = fail
    try:
        run = await app.service.start(RunRequest(text="/echo x", request_key="fault"))
        try:
            await app.service.wait(run.id)
        except ServiceError:
            pass
        snap = await app.store.snapshot(run.id)
        assert all(e.kind != point for e in snap.events)
        assert len(calls) == (0 if point == "tool_intent" else 1)
        assert not app.store.healthy
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await app.service.start(RunRequest(text="new", request_key="new"))
        if point == "terminal":
            assert snap.run.status not in {"completed", "failed", "aborted", "interrupted"}
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_lost_tool_result_commit_return_reads_original_event_without_reexecution(
    tmp_path, registry_for
):
    calls = []

    async def execute(args):
        calls.append(1)
        return "real"

    async with await open_application(
        tmp_path / "return.db", registry=registry_for(execute)
    ) as app:
        original = app.store._write
        lost = False

        async def lose(operation):
            nonlocal lost
            result = await original(operation)
            if isinstance(result, RunEvent) and result.kind == "tool_end" and not lost:
                lost = True
                raise OSError("LOST_RETURN_SECRET")
            return result

        app.store._write = lose
        run = await app.service.start(RunRequest(text="/echo x", request_key="return"))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        assert facts.run.status == "completed" and lost
        assert calls == [1] and len([e for e in facts.events if e.kind == "tool_end"]) == 1
        assert app.store.healthy
        with pytest.raises(ServiceError, match="run_closed"):
            await app.store.append(run.id, "text", {"text": "late"})


@pytest.mark.asyncio
@pytest.mark.parametrize("point", ["begin", "terminal"])
async def test_run_commit_return_loss_is_confirmed_without_duplicate_run_or_model(tmp_path, point):
    from krim_agent.contracts import RunView

    async with await open_application(tmp_path / (point + "-return.db")) as app:
        original = app.store._write
        lost = False

        async def lose(operation):
            nonlocal lost
            result = await original(operation)
            match = (
                point == "begin"
                and isinstance(result, tuple)
                and isinstance(result[0], RunView)
                and result[1] is True
            ) or (
                point == "terminal" and isinstance(result, RunView) and result.status == "completed"
            )
            if match and not lost:
                lost = True
                raise OSError("LOST_RUN_RETURN_SECRET")
            return result

        app.store._write = lose
        request = RunRequest(text="/add 2 3", request_key="same")
        run = await app.service.start(request)
        await app.service.wait(run.id)
        again = await app.service.start(request)
        assert lost and again.id == run.id
        facts = await app.service.result(run.id)
        assert facts.run.status == "completed" and facts.text == "5"
        assert app.provider.calls == 2
        assert sum(e.kind == "terminal" for e in facts.events) == 1
