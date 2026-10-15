"""0055只追加专用计划表，真实升级/旧DDL-BLOB/安全降级和ORM一致。"""

import time
from uuid import uuid4

from alembic import command
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint, inspect, text
from test_directory_checkpoint_capture_store import capture_context as capture_context

from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    Base,
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.persistence.migration import migration_config

TABLES = {
    "workspace_directory_restore_plans",
    "workspace_directory_restore_attempts",
    "workspace_directory_restore_locks",
    "workspace_directory_restore_model_sources",
}


async def schema(sql):
    return {
        row[0]: (row[1], row[2])
        for row in (
            await sql.execute(
                text(
                    "SELECT name,rootpage,sql FROM sqlite_master WHERE type='table' AND name "
                    "NOT IN ('alembic_version','sqlite_sequence')"
                )
            )
        ).all()
    }


async def test_0054_upgrade_and_empty_roundtrip_leave_old_tables_and_opaque_blob_unchanged(
    tmp_path,
):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'upgrade.sqlite'}")
    try:
        async with database.engine.begin() as sql:
            await sql.run_sync(
                lambda connection: command.upgrade(
                    migration_config(connection), "0054_directory_capture_models"
                )
            )
            await sql.execute(
                text(
                    "INSERT INTO topics(id,title,locale,created_at,updated_at) VALUES "
                    "('old','旧目录','zh-CN','2026-10-15','2026-10-15')"
                )
            )
            await sql.execute(
                text(
                    "INSERT INTO "
                    "workspace_directory_checkpoints(checkpoint_id,scope_id,topic_id,request_"
                    "key,request_hash,title,root_path,workspace_key,resource_fingerprint,work"
                    "space_fingerprint,snapshot_fingerprint,metadata,payload,created_at) "
                    "VALUES "
                    "('opaque','topic:old','old','old',:digest,'旧','tree','files',:digest,:di"
                    "gest,:digest,'{}',:blob,'2026-10-15')"
                ),
                {"digest": "a" * 64, "blob": b"\0\xff0054-opaque"},
            )
            before = await schema(sql)
            for phase in ("first", "roundtrip"):
                await sql.run_sync(
                    lambda connection: command.upgrade(
                        migration_config(connection), "0055_directory_restore_plans"
                    )
                )
                after = await schema(sql)
                assert set(after) == set(before) | TABLES
                assert all(after[name] == original for name, original in before.items())
                assert (
                    await sql.execute(
                        text(
                            "SELECT payload FROM workspace_directory_checkpoints WHERE "
                            "checkpoint_id='opaque'"
                        )
                    )
                ).scalar_one() == b"\0\xff0054-opaque"
                assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
                if phase == "first":
                    await sql.run_sync(
                        lambda connection: command.downgrade(
                            migration_config(connection), "0054_directory_capture_models"
                        )
                    )
                    assert await schema(sql) == before
    finally:
        await database.close()


async def test_0055_new_columns_constraints_indexes_and_foreign_keys_match_orm(repository):
    def compare(connection):
        inspector = inspect(connection)
        for name in TABLES:
            table = Base.metadata.tables[name]
            columns = {column["name"]: column for column in inspector.get_columns(name)}
            assert set(columns) == set(table.columns.keys())
            assert all(
                columns[column.name]["nullable"] == column.nullable for column in table.columns
            )
            assert {item["name"] for item in inspector.get_check_constraints(name)} == {
                constraint.name
                for constraint in table.constraints
                if isinstance(constraint, CheckConstraint)
            }
            assert {
                tuple(item["column_names"]) for item in inspector.get_unique_constraints(name)
            } == {
                tuple(column.name for column in constraint.columns)
                for constraint in table.constraints
                if isinstance(constraint, UniqueConstraint)
            }
            expected = {
                (
                    tuple(item.parent.name for item in constraint.elements),
                    constraint.elements[0].column.table.name,
                    tuple(item.column.name for item in constraint.elements),
                )
                for constraint in table.constraints
                if isinstance(constraint, ForeignKeyConstraint)
            }
            actual = {
                (
                    tuple(item["constrained_columns"]),
                    item["referred_table"],
                    tuple(item["referred_columns"]),
                )
                for item in inspector.get_foreign_keys(name)
            }
            assert actual == expected
            assert {
                item["name"]: (tuple(item["column_names"]), bool(item["unique"]))
                for item in inspector.get_indexes(name)
            } == {
                str(index.name): (tuple(column.name for column in index.columns), index.unique)
                for index in table.indexes
            }
        assert inspector.get_pk_constraint("workspace_directory_restore_model_sources")[
            "constrained_columns"
        ] == ["run_id", "call_id"]

    async with repository.database.engine.connect() as connection:
        await connection.run_sync(compare)
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0055_directory_restore_plans"


async def test_nonempty_preparing_plan_refuses_downgrade_without_losing_original_capture(
    capture_context,
):
    store, scope, binding, snapshot, request = capture_context
    checkpoint, _ = await store.reserve(scope, binding, request)
    fence = await store.execution(scope, checkpoint.checkpoint_id)
    value = await store.record(scope, fence, binding, snapshot, qualify=lambda: None)
    await store.finish(scope, fence, binding, value, qualify=lambda: None)
    original = await store.get(scope, checkpoint.checkpoint_id)
    identifier = str(uuid4())
    async with store.database.sessions() as sql, sql.begin():
        capture = await sql.get(WorkspaceDirectoryCaptureRow, checkpoint.checkpoint_id)
        sql.add(
            WorkspaceDirectoryRestorePlanRow(
                plan_id=identifier,
                scope_id=scope,
                topic_id=capture.topic_id,
                request_key="partial-preparation",
                request_hash="b" * 64,
                ttl_seconds=3600,
                expires_at=time.time() + 3600,
                target_kind="checkpoint",
                target_checkpoint_id=checkpoint.checkpoint_id,
                target_material_id=capture.material_id,
                target_fingerprint=snapshot.fingerprint,
                target_summary=original.snapshot.model_dump(mode="json"),
                root_path="tree",
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
                owner_id=store.owner_id,
                lease_expires_at=time.time() + 60,
                token="c" * 64,
                stage_token="d" * 32,
            )
        )
    import pytest

    with pytest.raises(RuntimeError, match="目录恢复计划非空"):
        async with store.database.engine.begin() as sql:
            await sql.run_sync(
                lambda connection: command.downgrade(
                    migration_config(connection), "0054_directory_capture_models"
                )
            )
    assert await store.get(scope, checkpoint.checkpoint_id) == original
    async with store.database.sessions() as sql:
        assert await sql.get(WorkspaceDirectoryRestorePlanRow, identifier) is not None
        assert (
            await sql.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0055_directory_restore_plans"
