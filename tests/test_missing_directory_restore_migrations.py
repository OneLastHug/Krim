"""0056仅追加缺失账本；真实旧版本升级、无损回退、逐表非空拒绝和ORM一致。"""

import time
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint, inspect, text
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_migrations import schema
from test_directory_restore_plans import target

from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    Base,
    WorkspaceDirectoryCaptureRow,
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestoreLockRow,
    WorkspaceMissingDirectoryRestoreModelSourceRow,
    WorkspaceMissingDirectoryRestorePlanRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest

TABLES = {
    "workspace_missing_directory_restore_plans",
    "workspace_missing_directory_restore_attempts",
    "workspace_missing_directory_restore_locks",
    "workspace_missing_directory_restore_model_sources",
}


async def test_0055_upgrade_empty_roundtrip_keeps_old_ddl_json_and_binary_bytes(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / '0055-upgrade.sqlite'}")
    try:
        async with database.engine.begin() as sql:
            await sql.run_sync(
                lambda connection: command.upgrade(
                    migration_config(connection), "0055_directory_restore_plans"
                )
            )
            await sql.execute(
                text(
                    "INSERT INTO topics(id,title,locale,created_at,updated_at) VALUES "
                    "('old','旧目录','zh-CN','2026-10-16','2026-10-16')"
                )
            )
            opaque_json = '{ "opaque" : [true, null, "原样"] }'
            opaque_blob = b"\0\xff0055-opaque\x80"
            await sql.execute(
                text(
                    "INSERT INTO workspace_directory_checkpoints(checkpoint_id,scope_id,"
                    "topic_id,request_key,request_hash,title,root_path,workspace_key,resource_"
                    "fingerprint,workspace_fingerprint,snapshot_fingerprint,metadata,payload,"
                    "created_at) VALUES ('opaque','topic:old','old','old',:digest,'旧','tree',"
                    "'files',:digest,:digest,:digest,:metadata,:payload,'2026-10-16')"
                ),
                {"digest": "a" * 64, "metadata": opaque_json, "payload": opaque_blob},
            )
            before = await schema(sql)
            for phase in ("first", "roundtrip"):
                await sql.run_sync(
                    lambda connection: command.upgrade(
                        migration_config(connection), "0056_missing_directory_restores"
                    )
                )
                after = await schema(sql)
                assert set(after) == set(before) | TABLES
                assert all(after[name] == original for name, original in before.items())
                assert (
                    await sql.execute(
                        text(
                            "SELECT metadata,payload FROM workspace_directory_checkpoints "
                            "WHERE checkpoint_id='opaque'"
                        )
                    )
                ).one() == (opaque_json, opaque_blob)
                assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
                if phase == "first":
                    await sql.run_sync(
                        lambda connection: command.downgrade(
                            migration_config(connection), "0055_directory_restore_plans"
                        )
                    )
                    assert await schema(sql) == before
    finally:
        await database.close()


async def test_0056_columns_checks_keys_indexes_and_foreign_keys_match_orm(repository):
    def compare(connection):
        inspector = inspect(connection)
        for name in TABLES:
            table = Base.metadata.tables[name]
            columns = {item["name"]: item for item in inspector.get_columns(name)}
            assert set(columns) == set(table.columns.keys())
            assert all(columns[column.name]["nullable"] == column.nullable for column in table.c)
            assert {item["name"] for item in inspector.get_check_constraints(name)} == {
                item.name for item in table.constraints if isinstance(item, CheckConstraint)
            }
            assert {
                tuple(item["column_names"]) for item in inspector.get_unique_constraints(name)
            } == {
                tuple(column.name for column in item.columns)
                for item in table.constraints
                if isinstance(item, UniqueConstraint)
            }
            assert {
                (
                    tuple(item["constrained_columns"]),
                    item["referred_table"],
                    tuple(item["referred_columns"]),
                )
                for item in inspector.get_foreign_keys(name)
            } == {
                (
                    tuple(element.parent.name for element in item.elements),
                    item.elements[0].column.table.name,
                    tuple(element.column.name for element in item.elements),
                )
                for item in table.constraints
                if isinstance(item, ForeignKeyConstraint)
            }
            assert {
                item["name"]: (tuple(item["column_names"]), bool(item["unique"]))
                for item in inspector.get_indexes(name)
            } == {
                str(item.name): (tuple(column.name for column in item.columns), item.unique)
                for item in table.indexes
            }
            assert inspector.get_pk_constraint(name)["constrained_columns"] == [
                column.name for column in table.primary_key
            ]
        index = next(
            item
            for item in inspector.get_indexes("workspace_missing_directory_restore_model_sources")
            if item["name"] == "uq_missing_restore_original_source"
        )
        assert str(index["dialect_options"]["sqlite_where"]) == "phase IN ('preview','execute')"

    async with repository.database.engine.connect() as sql:
        await sql.run_sync(compare)
        assert (
            await sql.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0059_container_retention_models"


@pytest.mark.parametrize("last_row", ["plan", "attempt", "lock", "source"])
async def test_each_nonempty_family_refuses_downgrade_before_any_drop(
    capture_context, repository, last_row
):
    captures, scope, binding, snapshot, _ = capture_context
    checkpoint_id = await target(capture_context)
    original = await captures.get(scope, checkpoint_id)
    plan_id, operation_id = str(uuid4()), str(uuid4())
    async with captures.database.sessions() as sql, sql.begin():
        capture = await sql.get(WorkspaceDirectoryCaptureRow, checkpoint_id)
        topic_id = capture.topic_id
        sql.add(
            WorkspaceMissingDirectoryRestorePlanRow(
                plan_id=plan_id,
                scope_id=scope,
                topic_id=topic_id,
                request_key="partial-missing-preparation",
                request_hash="b" * 64,
                ttl_seconds=3600,
                expires_at=time.time() + 3600,
                target_kind="checkpoint",
                target_checkpoint_id=checkpoint_id,
                target_material_id=capture.material_id,
                target_fingerprint=snapshot.fingerprint,
                target_summary=original.snapshot.model_dump(mode="json"),
                root_path="tree",
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
                owner_id=captures.owner_id,
                lease_expires_at=time.time() + 60,
                token="c" * 64,
                stage_token="d" * 32,
            )
        )
        await sql.flush()
        if last_row in {"attempt", "lock"}:
            sql.add(
                WorkspaceMissingDirectoryRestoreAttemptRow(
                    operation_id=operation_id,
                    plan_id=plan_id,
                    scope_id=scope,
                    request_key="original-execute",
                    request_hash="e" * 64,
                    expected_revision=1,
                    owner_id=captures.owner_id,
                    lease_expires_at=time.time() + 60,
                    token="f" * 64,
                )
            )
            await sql.flush()
        if last_row == "lock":
            sql.add(
                WorkspaceMissingDirectoryRestoreLockRow(
                    plan_id=plan_id, operation_id=operation_id, kind="entry", lock_key="a" * 64
                )
            )
    if last_row == "source":
        run, _ = await repository.begin_run(
            topic_id, ChatRequest(text="原调用", idempotency_key="migration-source")
        )
        async with captures.database.sessions() as sql, sql.begin():
            sql.add(
                WorkspaceMissingDirectoryRestoreModelSourceRow(
                    run_id=run.id, call_id="original-preview", plan_id=plan_id, phase="preview"
                )
            )
    async with captures.database.engine.connect() as sql:
        before = await schema(sql)
        rows = {name: (await sql.execute(text("SELECT * FROM " + name))).all() for name in TABLES}
    with pytest.raises(RuntimeError, match="缺失目录恢复账本非空，禁止有损降级"):
        async with captures.database.engine.begin() as sql:
            await sql.run_sync(
                lambda connection: command.downgrade(
                    migration_config(connection), "0055_directory_restore_plans"
                )
            )
    async with captures.database.engine.connect() as sql:
        assert await schema(sql) == before
        assert {
            name: (await sql.execute(text("SELECT * FROM " + name))).all() for name in TABLES
        } == rows
        assert (
            await sql.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0059_container_retention_models"
        assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
    assert (await captures.get(scope, checkpoint_id)).status == "available"
