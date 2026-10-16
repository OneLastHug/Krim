"""0059真实旧Host材料往返、来源/标记降级保护与数据库原生约束。"""

import pytest
from alembic import command
from sqlalchemy import delete, inspect, select, text, update
from sqlalchemy.exc import IntegrityError
from test_container_retention_current_model_sources import actor as actor
from test_container_retention_current_model_sources import reserved
from test_container_retention_host import controller as controller
from test_container_retention_host import request

from krim_agent.persistence import Database
from krim_agent.persistence.database import WorkspaceContainerRetentionModelSourceRow as Source
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.migration import migration_config

HEAD = "0059_container_retention_models"
OLD = "0058_container_retention"


async def test_new_model_columns_table_checks_unique_and_foreign_keys_match_orm(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'current.sqlite'}")
    try:
        await database.initialize()
        async with database.engine.connect() as sql:
            for model in (Row, Source):
                columns = await sql.run_sync(
                    lambda c, model=model: inspect(c).get_columns(model.__tablename__)
                )
                assert {c["name"] for c in columns} == {c.name for c in model.__table__.columns}
                checks = await sql.run_sync(
                    lambda c, model=model: inspect(c).get_check_constraints(model.__tablename__)
                )
                assert {c["name"] for c in checks} == {
                    c.name
                    for c in model.__table__.constraints
                    if type(c).__name__ == "CheckConstraint"
                }
                actual = await sql.run_sync(
                    lambda c, model=model: inspect(c).get_unique_constraints(model.__tablename__)
                )
                assert {c["name"] for c in actual} == {
                    c.name
                    for c in model.__table__.constraints
                    if type(c).__name__ == "UniqueConstraint"
                }
            assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
            assert (
                await sql.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == HEAD
    finally:
        await database.close()


async def test_real_0058_host_row_json_blob_and_rootpages_survive_in_place_roundtrip(actor):
    service, _, binding, original, preview, scope, _ = actor
    store = service.container_retention_store
    view, _ = await store.reserve(scope, binding, request(preview), qualify=lambda: None)
    old_columns = [c.name for c in Row.__table__.columns if c.name != "model_sources_metadata"]
    query = text("SELECT " + ",".join(old_columns) + " FROM workspace_container_retentions")
    async with store.database.engine.begin() as sql:
        await sql.run_sync(lambda c: command.downgrade(migration_config(c), OLD))
        legacy = (await sql.execute(query)).one()
        ddl = dict(
            (
                await sql.execute(
                    text(
                        "SELECT name,rootpage FROM sqlite_master WHERE type='table' "
                        "AND name NOT IN ('alembic_version','sqlite_sequence')"
                    )
                )
            ).all()
        )
        # 这是实际0058数据库，并非当前ORM create_all仿造旧列。
        assert (
            await sql.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == OLD
        columns = await sql.run_sync(lambda c: inspect(c).get_columns(Row.__tablename__))
        assert {c["name"] for c in columns} == set(old_columns)
    for phase in ["first", "roundtrip"]:
        async with store.database.engine.begin() as sql:
            await sql.run_sync(lambda c: command.upgrade(migration_config(c), HEAD))
            assert (await sql.execute(query)).one() == legacy
            pages = dict(
                (
                    await sql.execute(
                        text(
                            "SELECT name,rootpage FROM sqlite_master WHERE type='table' "
                            "AND name NOT IN ('alembic_version','sqlite_sequence')"
                        )
                    )
                ).all()
            )
            assert {name: pages[name] for name in ddl} == ddl
            assert (
                await sql.execute(
                    text("SELECT model_sources_metadata FROM workspace_container_retentions")
                )
            ).scalar_one() is None
            assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
        assert await store.get(scope, view.operation_id) == view
        assert await store.restores.get(scope, original.plan_id) == original
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
        if phase == "first":
            async with store.database.engine.begin() as sql:
                await sql.run_sync(lambda c: command.downgrade(migration_config(c), OLD))


@pytest.mark.parametrize("material", ["both", "marker", "index"])
async def test_any_model_index_or_marker_blocks_downgrade_before_any_drop(actor, material):
    store, _, _, _, _, _, _, view, _ = await reserved(actor)
    if material != "both":
        async with store.database.sessions() as sql, sql.begin():
            if material == "marker":
                await sql.execute(delete(Source).where(Source.operation_id == view.operation_id))
            else:
                await sql.execute(
                    update(Row)
                    .where(Row.operation_id == view.operation_id)
                    .values(model_sources_metadata=None)
                )
    async with store.database.engine.connect() as sql:
        before = (
            await sql.execute(text("SELECT name,sql,rootpage FROM sqlite_master ORDER BY name"))
        ).all()
    with pytest.raises(RuntimeError, match="原Run/call来源或标记"):
        async with store.database.engine.begin() as sql:
            await sql.run_sync(lambda c: command.downgrade(migration_config(c), OLD))
    async with store.database.engine.connect() as sql:
        assert (
            await sql.execute(text("SELECT name,sql,rootpage FROM sqlite_master ORDER BY name"))
        ).all() == before
        assert (
            await sql.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == HEAD


@pytest.mark.parametrize(
    "fault",
    [
        "phase",
        "epoch",
        "empty-call",
        "missing-run",
        "duplicate-epoch",
        "missing-operation",
        "verify-epoch0",
        "verify-epoch129",
    ],
)
async def test_model_source_sql_constraints_reject_impossible_origin_without_application(
    actor, fault
):
    store, scope, _, _, _, _, _, view, _ = await reserved(actor)
    async with store.database.sessions() as sql:
        item = await sql.scalar(select(Source).where(Source.operation_id == view.operation_id))
        values = {c.name: getattr(item, c.key) for c in Source.__table__.columns}
    with pytest.raises(IntegrityError):
        async with store.database.sessions() as sql, sql.begin():
            if fault == "duplicate-epoch":
                values["call_id"] = "another-call"
                await sql.execute(Source.__table__.insert().values(**values))
            else:
                changes = {
                    "phase": {"phase": "capture"},
                    "epoch": {"epoch": 1},
                    "empty-call": {"call_id": ""},
                    "missing-run": {"run_id": "missing-run"},
                    "missing-operation": {"operation_id": "missing-operation"},
                    "verify-epoch0": {"phase": "verify", "epoch": 0},
                    "verify-epoch129": {"phase": "verify", "epoch": 129},
                }[fault]
                await sql.execute(
                    update(Source).where(Source.operation_id == view.operation_id).values(**changes)
                )
    assert await store.get(scope, view.operation_id) == view
