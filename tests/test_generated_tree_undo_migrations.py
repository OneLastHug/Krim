"""0047真实SQLite ALTER往返、旧active循环外键/私有材料保全与来源拒降级。"""

import pytest
from alembic import command
from sqlalchemy import delete, inspect, text, update
from sqlalchemy.exc import IntegrityError
from test_generated_tree_undo_ledger import prepared
from test_workspace_tree_move_ledger import action

from krim_agent.persistence.database import (
    Base,
    WorkspaceGeneratedTreeUndoRow,
    WorkspaceTreeMoveProposalRow,
)
from krim_agent.persistence.migration import migration_config


async def test_empty_0047_roundtrip_preserves_original_active_move_and_seal_byte_material(
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
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, req, change)
        op, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        sealed = await seals.get(scope, binding, plan.plan_id)
        queries = [
            "SELECT proposal_id, request_hash, request, prepared, payload, active_operation_id "
            "FROM workspace_tree_move_proposals",
            "SELECT operation_id, prepared, payload, status, owner_id, lease_expires_at "
            "FROM workspace_tree_move_attempts",
            "SELECT * FROM workspace_tree_move_locks",
            "SELECT * FROM workspace_tree_completed_seals",
        ]
        async with store.database.engine.begin() as connection:
            before = [(await connection.execute(text(query))).all() for query in queries]
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0046_tree_exchange_origins")
            )
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0046_tree_exchange_origins"
            assert before == [(await connection.execute(text(query))).all() for query in queries]
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0053_directory_captures"
            assert before == [(await connection.execute(text(query))).all() for query in queries]
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        assert await seals.get(scope, binding, plan.plan_id) == sealed
        assert await store.operation(scope, op.view.operation_id) == op
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreeMoveProposalRow, proposal.view.proposal_id)
            assert row.generated_plan_id is None
    finally:
        await service.close()


@pytest.mark.parametrize("material", ["both", "marker-only", "source-only"])
async def test_0047_nonempty_marker_or_source_refuses_loss_with_original_rows_unchanged(
    runtime, tmp_path, material
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
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        async with store.database.sessions.begin() as sql:
            if material == "marker-only":
                await sql.execute(delete(WorkspaceGeneratedTreeUndoRow))
            elif material == "source-only":
                await sql.execute(
                    update(WorkspaceTreeMoveProposalRow).values(generated_plan_id=None)
                )
        async with store.database.engine.connect() as connection:
            markers = (
                await connection.execute(
                    text("SELECT generated_plan_id FROM workspace_tree_move_proposals")
                )
            ).all()
            links = (
                await connection.execute(text("SELECT * FROM workspace_generated_tree_undos"))
            ).all()
        with pytest.raises(RuntimeError, match="拒绝有损降级"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0046_tree_exchange_origins"
                    )
                )
        async with store.database.engine.connect() as connection:
            assert (
                await connection.execute(
                    text("SELECT generated_plan_id FROM workspace_tree_move_proposals")
                )
            ).all() == markers
            assert (
                await connection.execute(text("SELECT * FROM workspace_generated_tree_undos"))
            ).all() == links
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0053_directory_captures"
        if material == "both":
            assert await store.proposal(scope, proposal.view.proposal_id) == proposal
    finally:
        await service.close()


async def test_sql_null_foreign_key_and_hash_checks_preserve_generated_source(runtime, tmp_path):
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
        req,
        change,
        source,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, req, change, generated_origin=source)
        for bad in (
            {"source_plan_id": None},
            {"source_plan_id": "missing"},
            {"proposal_id": "missing"},
            {"scope_id": None},
            {"proof_hash": None},
            {"proof_hash": "short"},
            {"approved_fingerprint": None},
            {"tree_fingerprint": "short"},
            {"previous_restore_operation_id": "missing"},
        ):
            with pytest.raises(IntegrityError):
                async with store.database.sessions.begin() as sql:
                    await sql.execute(update(WorkspaceGeneratedTreeUndoRow).values(**bad))
        with pytest.raises(IntegrityError):
            async with store.database.sessions.begin() as sql:
                await sql.execute(
                    update(WorkspaceTreeMoveProposalRow).values(generated_plan_id="missing")
                )
        assert await store.proposal(scope, proposal.view.proposal_id) == proposal
    finally:
        await service.close()


async def test_0047_actual_column_nullability_foreign_keys_and_checks_match_orm(runtime):
    def compare(connection):
        inspector = inspect(connection)
        for name in ("workspace_generated_tree_undos", "workspace_tree_move_proposals"):
            table = Base.metadata.tables[name]
            columns = {col["name"]: col for col in inspector.get_columns(name)}
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
            assert actual == expected
        assert {
            c["name"] for c in inspector.get_check_constraints("workspace_generated_tree_undos")
        } == {
            "ck_generated_undo_tree_hash",
            "ck_generated_undo_approved_hash",
            "ck_generated_undo_proof_hash",
        }

    async with runtime.repository.database.engine.connect() as connection:
        await connection.run_sync(compare)
