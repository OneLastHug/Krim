import hashlib

import pytest
from alembic import command
from sqlalchemy import inspect, text

from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, EditRequest
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def test_legacy_m1_database_is_upgraded_without_changing_history(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'legacy.sqlite'}")
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.upgrade(migration_config(sync), "0001_m1")
            )
            await connection.execute(
                text("INSERT INTO topics VALUES ('topic', '历史', '2026-10-02', '2026-10-02')")
            )
            await connection.execute(
                text(
                    "INSERT INTO runs VALUES ("
                    "'run','topic','user','same',:hash,'completed',0,NULL,"
                    "'2026-10-02','2026-10-02','2026-10-02')"
                ),
                {"hash": hashlib.sha256(b"hello").hexdigest()},
            )
            await connection.execute(
                text(
                    "INSERT INTO messages VALUES ("
                    "'user','topic','run',1,'user','hello','{}','2026-10-02')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO messages VALUES ("
                    "'reply','topic','run',2,'assistant','retained','{}','2026-10-02')"
                )
            )
            await connection.execute(
                text("INSERT INTO tool_audits VALUES ('call','run','{}','2026-10-02')")
            )
            # 真实 M1 没有版本表：必须验证无版本号接管，不能只测 Alembic 旧版本。
            await connection.execute(text("DROP TABLE alembic_version"))
        await database.initialize()
        await database.initialize()
        repository = KrimRepository(database)
        messages = await repository.list_messages("topic")
        assert [message.id for message in messages] == ["user", "reply"]
        assert messages[0].parent_id is None and messages[1].parent_id == "user"
        assert messages[-1].content == "retained"
        run, created = await repository.begin_run(
            "topic", ChatRequest(text="hello", idempotency_key="same")
        )
        assert not created and run.id == "run" and run.action == "send"
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0002_message_tree"
            assert (
                await connection.execute(
                    text("SELECT payload FROM tool_audits WHERE call_id='call'")
                )
            ).scalar() == "{}"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        await database.close()


async def test_unknown_schema_is_rejected_without_overwriting_data(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'unknown.sqlite'}")
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text("CREATE TABLE topics (id TEXT PRIMARY KEY, valuable TEXT)")
            )
            await connection.execute(text("INSERT INTO topics VALUES ('original','do not delete')"))
        with pytest.raises(RuntimeError, match="备份"):
            await database.initialize()
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT valuable FROM topics"))
            ).scalar() == "do not delete"
            assert await connection.run_sync(lambda sync: inspect(sync).get_table_names()) == [
                "topics"
            ]
    finally:
        await database.close()


async def test_downgrade_refuses_to_erase_branches(runtime, repository):
    topic = await repository.create_topic("回退保护")
    run = await runtime.start(topic.id, ChatRequest(text="before", idempotency_key="original"))
    state = await runtime.stream_state(run.id, 0)
    await state.task
    edited = await runtime.edit(
        topic.id, run.input_message_id, EditRequest(text="after", idempotency_key="edit")
    )
    await (await runtime.stream_state(edited.id, 0)).task
    async with repository.database.engine.begin() as connection:
        with pytest.raises(RuntimeError, match="分支"):
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0001_m1")
            )
    assert len((await repository.message_tree(topic.id)).messages) == 4


async def test_linear_history_can_downgrade_and_upgrade_again(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'roundtrip.sqlite'}")
    await database.initialize()
    repository = KrimRepository(database)
    runtime = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
    try:
        topic = await repository.create_topic("线性回退")
        run = await runtime.start(topic.id, ChatRequest(text="hello", idempotency_key="one"))
        await (await runtime.stream_state(run.id, 0)).task
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0001_m1")
            )
        await database.initialize()
        assert (await repository.list_messages(topic.id))[-1].content == "[fake] 收到：hello"
    finally:
        await runtime.close()
        await database.close()
