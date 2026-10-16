"""0058只加独立表，旧DDL/BLOB不改；来源槽位、FK、Owner和降级实际拒绝。"""

from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import inspect, text, update
from sqlalchemy.exc import IntegrityError
from test_container_retention_store import start
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_directory_restore_store import missing as missing

from krim_agent.persistence import Database
from krim_agent.persistence.database import WorkspaceContainerRetentionLockRow as Lock
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.migration import migration_config


async def test_0057_upgrade_empty_roundtrip_preserves_original_tables_and_opaque_bytes(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'legacy.sqlite'}")
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda c: command.upgrade(migration_config(c), "0057_missing_directory_rollbacks")
            )
            await connection.execute(
                text(
                    "INSERT INTO topics(id,title,locale,created_at,updated_at) "
                    "VALUES ('old','保留旧记录','zh-CN','2026-10-16','2026-10-16')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO native_workspace_checkpoints(checkpoint_id,scope_id,topic_id,"
                    "workspace_key,resource_fingerprint,workspace_fingerprint,request_key,"
                    "request_hash,title,capture_format,status,token,created_at,updated_at) "
                    "VALUES ('old-checkpoint','topic:old','old','files',:hash,:hash,'original',"
                    ":hash,'旧bytes','bytes','available',:hash,'2026-10-16','2026-10-16')"
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
            for phase in ["first", "roundtrip"]:
                await connection.run_sync(
                    lambda c: command.upgrade(migration_config(c), "0058_container_retention")
                )
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
                assert set(after) == set(before) | {Row.__tablename__, Lock.__tablename__}
                assert {name: after[name] for name in before} == before
                assert (
                    await connection.execute(
                        text("SELECT content FROM native_workspace_checkpoint_files")
                    )
                ).scalar_one() == b"\0\xfflegacy"
                assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
                if phase == "first":
                    await connection.run_sync(
                        lambda c: command.downgrade(
                            migration_config(c), "0057_missing_directory_rollbacks"
                        )
                    )
                    assert (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar_one() == "0057_missing_directory_rollbacks"
    finally:
        await database.close()


async def test_new_tables_match_orm_columns_constraints_and_normal_head(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'new.sqlite'}")
    try:
        await database.initialize()
        async with database.engine.connect() as connection:
            for model in (Row, Lock):
                columns = await connection.run_sync(
                    lambda c, model=model: inspect(c).get_columns(model.__tablename__)
                )
                assert {item["name"] for item in columns} == {
                    column.name for column in model.__table__.columns
                }
                checks = await connection.run_sync(
                    lambda c, model=model: inspect(c).get_check_constraints(model.__tablename__)
                )
                wanted = {
                    item.name
                    for item in model.__table__.constraints
                    if item.__class__.__name__ == "CheckConstraint"
                }
                assert {item["name"] for item in checks} == wanted
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0059_container_retention_models"
    finally:
        await database.close()


@pytest.mark.parametrize(
    "fault",
    [
        "source-slot",
        "owner",
        "token",
        "unknown-no-intent",
        "done-no-receipt",
        "duplicate-source",
        "foreign-source",
    ],
)
async def test_sql_rejects_invalid_or_duplicate_retention_even_without_store_checks(missing, fault):
    context = await start(missing)
    store, scope, _binding, _origin, _old, _core, request, view, _fence = context
    async with store.database.sessions() as sql:
        original = await sql.get(Row, view.operation_id)
        values = {column.name: getattr(original, column.key) for column in Row.__table__.columns}
    with pytest.raises(IntegrityError):
        async with store.database.sessions() as sql, sql.begin():
            if fault == "duplicate-source":
                values.update(operation_id=str(uuid4()), request_key="another-explicit-key")
                await sql.execute(Row.__table__.insert().values(**values))
            else:
                changes = {
                    "source-slot": {"active_source_id": None},
                    "owner": {"owner_id": None},
                    "token": {"token": "short"},
                    "unknown-no-intent": {
                        "status": "unknown",
                        "owner_id": None,
                        "lease_expires_at": None,
                    },
                    "done-no-receipt": {
                        "status": "done",
                        "owner_id": None,
                        "lease_expires_at": None,
                    },
                    "foreign-source": {"source_operation_id": str(uuid4())},
                }[fault]
                await sql.execute(
                    update(Row).where(Row.operation_id == view.operation_id).values(**changes)
                )
    assert await store.by_key(scope, request.request_key) == view


async def test_nonempty_retention_rejects_lossy_downgrade_and_preserves_original_record(missing):
    context = await start(missing)
    store, scope, _binding, _origin, _old, _core, request, view, _fence = context
    with pytest.raises(RuntimeError, match="禁止有损降级"):
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda c: command.downgrade(migration_config(c), "0057_missing_directory_rollbacks")
            )
    assert await store.by_key(scope, request.request_key) == view
    async with store.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0059_container_retention_models"
