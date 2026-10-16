"""0052只新增完整目录材料，旧结构/数据与非空降级保护保持。"""

import pytest
from alembic import command
from sqlalchemy import inspect, text
from test_directory_checkpoint_materials import material_context as material_context

from krim_agent.persistence import Database
from krim_agent.persistence.database import Base
from krim_agent.persistence.migration import migration_config


async def test_real_0051_upgrade_empty_roundtrip_preserves_all_old_table_ddl_and_data(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'old.sqlite'}")
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.upgrade(
                    migration_config(sync), "0051_checkpoint_restore_models"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO topics(id,title,locale,created_at,updated_at) "
                    "VALUES ('old','保留原记录','zh-CN','2026-10-15','2026-10-15')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO native_workspace_checkpoints("
                    "checkpoint_id,scope_id,topic_id,workspace_key,resource_fingerprint,"
                    "workspace_fingerprint,request_key,request_hash,title,capture_format,"
                    "status,token,created_at,updated_at) VALUES ("
                    "'old-checkpoint','topic:old','old','files',:hash,:hash,'original',:hash,"
                    "'旧bytes','bytes','available',:hash,'2026-10-15','2026-10-15')"
                ),
                {"hash": "a" * 64},
            )
            await connection.execute(
                text(
                    "INSERT INTO native_workspace_checkpoint_files("
                    "checkpoint_id,position,path,captured,capture_metadata,content) "
                    "VALUES ('old-checkpoint',0,'old.bin',1,'{}',:data)"
                ),
                {"data": b"\0\xfflegacy"},
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
                lambda sync: command.upgrade(migration_config(sync), "0052_directory_checkpoints")
            )
        async with database.engine.begin() as connection:
            for phase in ("upgraded", "roundtrip"):
                assert (
                    await connection.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0052_directory_checkpoints"
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
                assert set(after) == set(before) | {"workspace_directory_checkpoints"}
                assert {name: after[name] for name in before} == before
                assert (
                    await connection.execute(
                        text("SELECT content FROM native_workspace_checkpoint_files")
                    )
                ).scalar_one() == b"\0\xfflegacy"
                assert (
                    await connection.execute(
                        text("SELECT request_key FROM native_workspace_checkpoints")
                    )
                ).scalar_one() == "original"
                assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
                if phase == "upgraded":
                    await connection.run_sync(
                        lambda sync: command.downgrade(
                            migration_config(sync), "0051_checkpoint_restore_models"
                        )
                    )
                    assert (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar_one() == "0051_checkpoint_restore_models"
                    await connection.run_sync(
                        lambda sync: command.upgrade(
                            migration_config(sync), "0052_directory_checkpoints"
                        )
                    )
    finally:
        await database.close()


async def test_nonempty_0052_downgrade_refuses_loss_and_keeps_original_material(material_context):
    store, scope, binding, snapshot, request = material_context
    original, _ = await store.save_captured(scope, binding, request, snapshot)
    with pytest.raises(RuntimeError, match="原生目录检查点材料非空"):
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0051_checkpoint_restore_models"
                )
            )
    assert (
        await store.get_material(
            scope,
            original.view.checkpoint_id,
            workspace_key=binding.key,
            resource_fingerprint=binding.fingerprint,
        )
        == original
    )
    async with store.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0057_missing_directory_rollbacks"
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()


async def test_0052_actual_columns_foreign_keys_and_checks_match_orm(repository):
    def compare(connection):
        inspector = inspect(connection)
        name = "workspace_directory_checkpoints"
        table = Base.metadata.tables[name]
        columns = {c["name"]: c for c in inspector.get_columns(name)}
        assert set(columns) == set(table.columns.keys())
        assert all(columns[c.name]["nullable"] == c.nullable for c in table.columns)
        assert {tuple(item["column_names"]) for item in inspector.get_unique_constraints(name)} == {
            ("scope_id", "request_key")
        }
        assert {c["name"] for c in inspector.get_check_constraints(name)} == {
            c.name for c in table.constraints if c.__class__.__name__ == "CheckConstraint"
        }
        assert [
            (f["constrained_columns"], f["referred_table"], f["referred_columns"])
            for f in inspector.get_foreign_keys(name)
        ] == [(["topic_id"], "topics", ["id"])]

    async with repository.database.engine.connect() as connection:
        await connection.run_sync(compare)
