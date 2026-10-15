"""0053仅追加捕获任务表；0052材料和旧协议数据保持，非空降级拒绝。"""

import pytest
from alembic import command
from sqlalchemy import inspect, text
from test_directory_checkpoint_capture_store import capture_context as capture_context

from krim_agent.persistence import Database
from krim_agent.persistence.database import Base
from krim_agent.persistence.migration import migration_config


async def test_real_0052_upgrade_and_empty_roundtrip_keep_all_old_ddl_and_material(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'old.sqlite'}")
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.upgrade(migration_config(sync), "0052_directory_checkpoints")
            )
            await connection.execute(
                text(
                    "INSERT INTO topics(id,title,locale,created_at,updated_at) "
                    "VALUES ('old','旧材料','zh-CN','2026-10-15','2026-10-15')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO workspace_directory_checkpoints("
                    "checkpoint_id,scope_id,topic_id,request_key,request_hash,title,root_path,"
                    "workspace_key,resource_fingerprint,workspace_fingerprint,snapshot_fingerprint,"
                    "metadata,payload,created_at) VALUES ("
                    "'old-material','topic:old','old','old-key',"
                    ":hash,'原材料','tree','files',:hash,:hash,:hash,'{}',:data,'2026-10-15')"
                ),
                {"hash": "a" * 64, "data": b"\0\xfflegacy"},
            )
            before = dict(
                (
                    await connection.execute(
                        text(
                            "SELECT name,sql FROM sqlite_master WHERE type='table' "
                            "AND name NOT IN ('alembic_version','sqlite_sequence')"
                        )
                    )
                ).all()
            )
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.upgrade(migration_config(sync), "0053_directory_captures")
            )
        async with database.engine.begin() as connection:
            for phase in ("upgraded", "roundtrip"):
                assert (
                    await connection.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0053_directory_captures"
                after = dict(
                    (
                        await connection.execute(
                            text(
                                "SELECT name,sql FROM sqlite_master WHERE type='table' "
                                "AND name NOT IN ('alembic_version','sqlite_sequence')"
                            )
                        )
                    ).all()
                )
                assert set(after) == set(before) | {"workspace_directory_captures"}
                assert {name: after[name] for name in before} == before
                assert (
                    await connection.execute(
                        text("SELECT request_key,payload FROM workspace_directory_checkpoints")
                    )
                ).one() == ("old-key", b"\0\xfflegacy")
                assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
                if phase == "upgraded":
                    await connection.run_sync(
                        lambda sync: command.downgrade(
                            migration_config(sync), "0052_directory_checkpoints"
                        )
                    )
                    assert (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar_one() == "0052_directory_checkpoints"
                    await connection.run_sync(
                        lambda sync: command.upgrade(
                            migration_config(sync), "0053_directory_captures"
                        )
                    )
    finally:
        await database.close()


async def test_nonempty_0053_downgrade_refuses_loss_and_original_job_still_reads(capture_context):
    store, scope, binding, _snapshot, request = capture_context
    original, _ = await store.reserve(scope, binding, request)
    with pytest.raises(RuntimeError, match="原生目录捕获任务非空"):
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0052_directory_checkpoints")
            )
    assert await store.get(scope, original.checkpoint_id) == original
    async with store.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0055_directory_restore_plans"
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()


async def test_0053_actual_columns_checks_foreign_keys_and_uniques_match_orm(repository):
    def compare(connection):
        inspector = inspect(connection)
        name = "workspace_directory_captures"
        table = Base.metadata.tables[name]
        columns = {c["name"]: c for c in inspector.get_columns(name)}
        assert set(columns) == set(table.columns.keys())
        assert all(columns[c.name]["nullable"] == c.nullable for c in table.columns)
        assert {tuple(item["column_names"]) for item in inspector.get_unique_constraints(name)} == {
            ("scope_id", "request_key"),
            ("material_id",),
        }
        assert {item["name"] for item in inspector.get_check_constraints(name)} == {
            c.name for c in table.constraints if c.__class__.__name__ == "CheckConstraint"
        }
        assert {
            (
                tuple(item["constrained_columns"]),
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for item in inspector.get_foreign_keys(name)
        } == {
            (("topic_id",), "topics", ("id",)),
            (("material_id",), "workspace_directory_checkpoints", ("checkpoint_id",)),
        }

    async with repository.database.engine.connect() as connection:
        await connection.run_sync(compare)
