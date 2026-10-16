"""0057原生追加标记/来源，旧JSON/hash/BLOB/表页保持、可空回退与非空拒绝。"""

import pytest
from alembic import command
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, inspect, text
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_migrations import schema
from test_missing_baseline_rollback_store import preview
from test_missing_baseline_rollback_store import rollback_origin as rollback_origin
from test_missing_directory_restore_store import missing as missing

from krim_agent.persistence import Database
from krim_agent.persistence.database import Base
from krim_agent.persistence.migration import migration_config


async def test_real_0056_upgrade_and_empty_roundtrip_keep_legacy_tree_move_opaque_bytes(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'old0056.sqlite'}")
    try:
        async with db.engine.begin() as sql:
            await sql.run_sync(
                lambda c: command.upgrade(migration_config(c), "0056_missing_directory_restores")
            )
            await sql.execute(
                text(
                    "INSERT INTO topics(id,title,locale,created_at,updated_at) VALUES "
                    "('old','旧','zh-CN','2026-10-16','2026-10-16')"
                )
            )
            await sql.execute(
                text(
                    "INSERT INTO workspace_tree_move_proposals(proposal_id,scope_"
                    "id,topic_id,request_key,request_hash,request,workspace_key,r"
                    "esource_fingerprint,source_path,target_path,prepared,payload"
                    ",status,revision,expires_at,created_at,updated_at) VALUES ('"
                    "old-move','topic:old','old','old',:hash,:json,'files',:hash,"
                    "'tree','retained',:json,:blob,'proposed',0,9999999999,'2026-"
                    "10-16','2026-10-16')"
                ),
                {
                    "hash": "a" * 64,
                    "json": '{ "opaque" : [true,null,"原样"] }',
                    "blob": b"\0\xfflegacy0056",
                },
            )
            before = await schema(sql)
            values = (await sql.execute(text("SELECT * FROM workspace_tree_move_proposals"))).one()
            for phase in ("first", "roundtrip"):
                await sql.run_sync(
                    lambda c: command.upgrade(
                        migration_config(c), "0057_missing_directory_rollbacks"
                    )
                )
                after = await schema(sql)
                assert set(after) == set(before) | {"workspace_missing_directory_rollbacks"}
                assert all(
                    after[name] == value
                    for name, value in before.items()
                    if name != "workspace_tree_move_proposals"
                )
                assert (
                    after["workspace_tree_move_proposals"][0]
                    == before["workspace_tree_move_proposals"][0]
                )
                updated = (
                    await sql.execute(text("SELECT * FROM workspace_tree_move_proposals"))
                ).one()
                assert tuple(updated[:-2]) == tuple(values) and updated[-2:] == (None, None)
                assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
                if phase == "first":
                    await sql.run_sync(
                        lambda c: command.downgrade(
                            migration_config(c), "0056_missing_directory_restores"
                        )
                    )
                    assert await schema(sql) == before
                    assert (
                        await sql.execute(text("SELECT * FROM workspace_tree_move_proposals"))
                    ).one() == values
    finally:
        await db.close()


async def test_new_source_and_parent_marker_orm_columns_checks_and_foreign_keys(repository):
    def compare(connection):
        inspector = inspect(connection)
        for name in ("workspace_tree_move_proposals", "workspace_missing_directory_rollbacks"):
            table = Base.metadata.tables[name]
            actual = {x["name"]: x for x in inspector.get_columns(name)}
            assert set(actual) == set(table.c.keys())
            assert all(actual[c.name]["nullable"] == c.nullable for c in table.c)
            assert {x["name"] for x in inspector.get_check_constraints(name)} == {
                x.name for x in table.constraints if isinstance(x, CheckConstraint)
            }
            assert {
                (tuple(x["constrained_columns"]), x["referred_table"], tuple(x["referred_columns"]))
                for x in inspector.get_foreign_keys(name)
            } == {
                (
                    tuple(e.parent.name for e in x.elements),
                    x.elements[0].column.table.name,
                    tuple(e.column.name for e in x.elements),
                )
                for x in table.constraints
                if isinstance(x, ForeignKeyConstraint)
            }

    async with repository.database.engine.connect() as sql:
        await sql.run_sync(compare)


@pytest.mark.parametrize("keep", ["both", "marker", "source"])
async def test_nonempty_source_or_marker_refuses_loss_before_any_drop(rollback_origin, keep):
    store, scope, _, _, _ = rollback_origin
    _, _, _, proposal = await preview(rollback_origin)
    async with store.database.sessions() as sql, sql.begin():
        if keep == "marker":
            await sql.execute(text("DELETE FROM workspace_missing_directory_rollbacks"))
        if keep == "source":
            await sql.execute(
                text(
                    "UPDATE workspace_tree_move_proposals SET missing_restore_plan_id=NULL,"
                    "missing_restore_hash=NULL"
                )
            )
    async with store.database.engine.connect() as sql:
        before = await schema(sql)
        rows = (
            await sql.execute(text("SELECT * FROM workspace_missing_directory_rollbacks"))
        ).all()
    with pytest.raises(RuntimeError, match="缺失基线回退来源/标记非空"):
        async with store.database.engine.begin() as sql:
            await sql.run_sync(
                lambda c: command.downgrade(migration_config(c), "0056_missing_directory_restores")
            )
    async with store.database.engine.connect() as sql:
        assert await schema(sql) == before
        assert (
            await sql.execute(text("SELECT * FROM workspace_missing_directory_rollbacks"))
        ).all() == rows
