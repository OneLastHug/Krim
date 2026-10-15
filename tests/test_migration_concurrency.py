"""产品初始化在多个库/loop并发、取消/失败时保留完整事务与代理。"""

import asyncio
import json

import pytest
from sqlalchemy import event, inspect, text
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.util.concurrency import await_only

from krim_agent.persistence import Database
from krim_agent.persistence.migration import migration_guard


async def schema(database):
    async with database.engine.connect() as sql:

        def reflect(connection):
            def normalized(value):
                if isinstance(value, TextClause):
                    return str(value)
                if isinstance(value, dict):
                    return {key: normalized(item) for key, item in value.items()}
                if isinstance(value, (list, tuple)):
                    return [normalized(item) for item in value]
                return value

            inspector = inspect(connection)
            tables = {}
            for name in sorted(inspector.get_table_names()):
                columns = [
                    {**column, "type": str(column["type"])}
                    for column in inspector.get_columns(name)
                ]
                values = {"columns": columns, "primary_key": inspector.get_pk_constraint(name)}
                for kind, read in (
                    ("foreign_keys", inspector.get_foreign_keys),
                    ("unique", inspector.get_unique_constraints),
                    ("checks", inspector.get_check_constraints),
                    ("indexes", inspector.get_indexes),
                ):
                    values[kind] = sorted(
                        normalized(read(name)), key=lambda value: json.dumps(value, sort_keys=True)
                    )
                tables[name] = values
            others = connection.execute(
                text(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type != 'table' "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
                )
            ).all()
            return {"tables": tables, "other_objects": [tuple(row) for row in others]}

        result = await sql.run_sync(reflect)
        head = (await sql.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
        foreign_keys = (await sql.execute(text("PRAGMA foreign_key_check"))).all()
    assert head == "0055_directory_restore_plans" and foreign_keys == []
    return result


async def test_different_databases_initialize_concurrently_keep_exact_complete_schema(tmp_path):
    base = Database(f"sqlite+aiosqlite:///{tmp_path / 'baseline.sqlite'}")
    databases = [
        Database(f"sqlite+aiosqlite:///{tmp_path / f'parallel-{index}.sqlite'}")
        for index in range(3)
    ]
    results = []
    try:
        await base.initialize()
        expected = await schema(base)
        results = await asyncio.gather(
            *(database.initialize() for database in databases), return_exceptions=True
        )
        assert not [item for item in results if isinstance(item, BaseException)], repr(results)
        assert all([await schema(database) == expected for database in databases])
    finally:
        (tmp_path / "outcomes.json").write_text(json.dumps([repr(item) for item in results]) + "\n")
        await asyncio.gather(base.close(), *(database.close() for database in databases))


async def test_same_file_multiple_database_instances_keep_schema_and_existing_history(tmp_path):
    path = tmp_path / "shared.sqlite"
    databases = [Database(f"sqlite+aiosqlite:///{path}") for _ in range(3)]
    try:
        await databases[0].initialize()
        expected = await schema(databases[0])
        async with databases[0].engine.begin() as sql:
            await sql.execute(text("CREATE TABLE application_marker(value TEXT)"))
            await sql.execute(text("INSERT INTO application_marker VALUES ('original')"))
        await asyncio.gather(*(database.initialize() for database in databases))
        for database in databases:
            actual = await schema(database)
            actual["tables"].pop("application_marker")
            assert actual == expected
            async with database.engine.connect() as sql:
                assert (await sql.execute(text("SELECT value FROM application_marker"))).all() == [
                    ("original",)
                ]
    finally:
        await asyncio.gather(*(database.close() for database in databases))


async def test_migration_guard_covers_distinct_threads_and_event_loops(tmp_path):
    def isolated(index):
        async def run():
            database = Database(f"sqlite+aiosqlite:///{tmp_path / f'thread-{index}.sqlite'}")
            try:
                await database.initialize()
                return await schema(database)
            finally:
                await database.close()

        return asyncio.run(run())

    values = await asyncio.gather(*(asyncio.to_thread(isolated, index) for index in range(3)))
    assert all(value == values[0] for value in values[1:])


async def test_cancel_waiting_initializer_never_enters_proxy_and_does_not_hold_gate(tmp_path):
    first = Database(f"sqlite+aiosqlite:///{tmp_path / 'first.sqlite'}")
    second = Database(f"sqlite+aiosqlite:///{tmp_path / 'second.sqlite'}")
    entered, release, second_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()

    def hold(_conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().startswith("CREATE TABLE topics"):
            entered.set()
            await_only(release.wait())

    def observe(*_args):
        second_entered.set()

    event.listen(first.engine.sync_engine, "before_cursor_execute", hold)
    event.listen(second.engine.sync_engine, "begin", observe)
    task = asyncio.create_task(first.initialize())
    waiting = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        waiting = asyncio.create_task(second.initialize())
        await asyncio.sleep(0.02)
        assert not waiting.done() and not second_entered.is_set()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert not second_entered.is_set()
        release.set()
        await task
        await second.initialize()
        assert await schema(first) == await schema(second)
    finally:
        release.set()
        for candidate in (task, waiting):
            if candidate is not None and not candidate.done():
                candidate.cancel()
        await asyncio.gather(
            *(candidate for candidate in (task, waiting) if candidate is not None),
            return_exceptions=True,
        )
        event.remove(first.engine.sync_engine, "before_cursor_execute", hold)
        event.remove(second.engine.sync_engine, "begin", observe)
        await asyncio.gather(first.close(), second.close())


async def test_repeat_cancel_waits_for_real_rollback_before_next_proxy(tmp_path):
    first = Database(f"sqlite+aiosqlite:///{tmp_path / 'cancelled.sqlite'}")
    second = Database(f"sqlite+aiosqlite:///{tmp_path / 'after.sqlite'}")
    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    observed = {}

    def pause(conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().startswith("CREATE TABLE topics") and not entered.is_set():
            conn.exec_driver_sql("CREATE TABLE cancelled_probe(value TEXT)")
            observed["task"] = asyncio.current_task()
            entered.set()
            try:
                await_only(asyncio.Event().wait())
            finally:
                cleaning.set()
                await_only(release.wait())

    event.listen(first.engine.sync_engine, "before_cursor_execute", pause)
    task = asyncio.create_task(first.initialize())
    after = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        await asyncio.wait_for(cleaning.wait(), 10)
        after = asyncio.create_task(second.initialize())
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done() and not after.done()
        assert observed["task"].cancelling() == 1
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await after
        await schema(second)
        async with first.engine.connect() as sql:
            assert (
                await sql.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            ).all() == []
    finally:
        release.set()
        for candidate in (task, after):
            if candidate is not None and not candidate.done():
                candidate.cancel()
        await asyncio.gather(
            *(candidate for candidate in (task, after) if candidate is not None),
            return_exceptions=True,
        )
        event.remove(first.engine.sync_engine, "before_cursor_execute", pause)
        await asyncio.gather(first.close(), second.close())


async def test_initializer_failure_rolls_back_and_releases_proxy_for_other_database(tmp_path):
    first = Database(f"sqlite+aiosqlite:///{tmp_path / 'failed.sqlite'}")
    second = Database(f"sqlite+aiosqlite:///{tmp_path / 'next.sqlite'}")

    def fail(_conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().startswith("CREATE TABLE topics"):
            raise OSError("synthetic migration failure")

    event.listen(first.engine.sync_engine, "before_cursor_execute", fail)
    try:
        values = await asyncio.gather(
            first.initialize(), second.initialize(), return_exceptions=True
        )
        assert isinstance(values[0], OSError) and values[1] is None
        event.remove(first.engine.sync_engine, "before_cursor_execute", fail)
        await first.initialize()
        assert await schema(first) == await schema(second)
    finally:
        if event.contains(first.engine.sync_engine, "before_cursor_execute", fail):
            event.remove(first.engine.sync_engine, "before_cursor_execute", fail)
        await asyncio.gather(first.close(), second.close())


async def test_reentrant_initializer_refuses_and_leaves_next_normal_initialization_available(
    tmp_path,
):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'reentrant.sqlite'}")
    try:
        async with migration_guard():
            with pytest.raises(RuntimeError, match="不能重入"):
                await database.initialize()
        await database.initialize()
        await schema(database)
    finally:
        await database.close()
