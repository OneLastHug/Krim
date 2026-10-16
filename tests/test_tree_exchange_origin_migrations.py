"""0046原来源关联迁移、NULL/FK保护与旧hash/封存保全。"""

import pytest
from alembic import command
from sqlalchemy import inspect, text, update
from sqlalchemy.exc import IntegrityError
from test_tree_exchange_origins import prepared

from krim_agent.persistence.database import Base, WorkspaceTreeExchangeOriginRow
from krim_agent.persistence.migration import migration_config


async def test_empty_0046_roundtrip_preserves_original_0045_material_and_existing_0044_request(
    runtime, tmp_path
):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    plain = request.model_copy(update={"source_plan_id": None})
    try:
        proposal, _ = await store.preview(scope, binding, plain, change)
        sealed = await seals.get(scope, binding, plan.plan_id)
        async with store.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0045_completed_tree_seals")
            )
            assert (
                await connection.execute(
                    text("SELECT count(*) FROM workspace_tree_completed_seals")
                )
            ).scalar_one() == 1
            assert (
                await connection.execute(
                    text("SELECT count(*) FROM workspace_tree_exchange_proposals")
                )
            ).scalar_one() == 1
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0058_container_retention"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        assert await seals.get(scope, binding, plan.plan_id) == sealed
        assert await store.proposal(scope, proposal.view.proposal_id) == proposal
    finally:
        await service.close()


async def test_nonempty_0046_refuses_loss_of_original_bound_source(runtime, tmp_path):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        with pytest.raises(RuntimeError, match="交换生成树来源关联非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0045_completed_tree_seals"
                    )
                )
        assert await store.proposal(scope, proposal.view.proposal_id) == proposal
    finally:
        await service.close()


async def test_null_fk_and_foreign_plan_links_cannot_replace_original_origin(runtime, tmp_path):
    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        for bad in (
            {"source_plan_id": None},
            {"source_plan_id": "missing"},
            {"proof_hash": None},
            {"proposal_id": "missing"},
            {"tree_fingerprint": None},
        ):
            with pytest.raises(IntegrityError):
                async with store.database.sessions.begin() as sql:
                    await sql.execute(
                        update(WorkspaceTreeExchangeOriginRow)
                        .where(
                            WorkspaceTreeExchangeOriginRow.proposal_id == proposal.view.proposal_id
                        )
                        .values(**bad)
                    )
        assert await store.proposal(scope, proposal.view.proposal_id) == proposal
    finally:
        await service.close()


async def test_0046_actual_structure_and_foreign_keys_match_base(runtime):
    def compare(connection):
        inspector = inspect(connection)
        name = "workspace_tree_exchange_origins"
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
                tuple(fk["referred_table"] + "." + col for col in fk["referred_columns"]),
            )
            for fk in inspector.get_foreign_keys(name)
        }
        assert expected == actual

    async with runtime.repository.database.engine.connect() as connection:
        await connection.run_sync(compare)
