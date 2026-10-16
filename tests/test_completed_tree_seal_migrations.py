"""0045封存实际结构/空回退/旧账本保持/非空损失拒绝。"""

import pytest
from alembic import command
from sqlalchemy import inspect, text
from test_completed_tree_origins import complete

from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.database import Base
from krim_agent.persistence.migration import migration_config


async def test_empty_0045_roundtrip_retains_original_plan_sources_and_attempts(runtime, tmp_path):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    try:
        async with trees.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0044_tree_exchange_ledger")
            )
            assert (
                await connection.execute(text("SELECT count(*) FROM workspace_tree_plans"))
            ).scalar_one() == 1
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0058_container_retention"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        assert await trees.get(scope, plan.plan_id) == plan
    finally:
        await service.close()


async def test_nonempty_0045_downgrade_refuses_loss_and_preserves_complete_identity(
    runtime, tmp_path
):
    service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
        runtime, tmp_path
    )
    store = CompletedTreeSealStore(trees)
    try:
        sealed, _ = await store.seal(scope, binding, plan.plan_id, snapshot.fingerprint, snapshot)
        with pytest.raises(RuntimeError, match="完整树来源封存非空"):
            async with trees.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0044_tree_exchange_ledger"
                    )
                )
        assert await store.get(scope, binding, plan.plan_id) == sealed
    finally:
        await service.close()


async def test_0045_actual_columns_nullability_foreign_keys_and_checks_match_base(runtime):
    def compare(connection):
        inspector = inspect(connection)
        name = "workspace_tree_completed_seals"
        table = Base.metadata.tables[name]
        columns = {c["name"]: c for c in inspector.get_columns(name)}
        assert set(columns) == set(table.columns.keys())
        assert all(columns[c.name]["nullable"] == c.nullable for c in table.columns)
        expected = {
            (
                tuple(e.parent.name for e in fk.elements),
                tuple(e.target_fullname for e in fk.elements),
            )
            for fk in table.foreign_key_constraints
        }
        actual = {
            (
                tuple(fk["constrained_columns"]),
                tuple(fk["referred_table"] + "." + c for c in fk["referred_columns"]),
            )
            for fk in inspector.get_foreign_keys(name)
        }
        assert actual == expected
        assert {c["name"] for c in inspector.get_check_constraints(name)} == {
            c.name for c in table.constraints if c.__class__.__name__ == "CheckConstraint"
        }

    async with runtime.repository.database.engine.connect() as connection:
        await connection.run_sync(compare)
