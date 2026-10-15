"""0044真实空升级回退、旧账本保持、非空降级拒绝及ORM一致。"""

import pytest
from alembic import command
from sqlalchemy import inspect, text
from test_workspace_tree_exchange_ledger import prepared

from krim_agent.persistence.database import Base
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter
from krim_agent.workspace_tree_move import TreeMovePreviewRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter
from krim_agent.workspace_writer import WorkspaceWriter

TABLES = (
    "workspace_tree_exchange_proposals",
    "workspace_tree_exchange_attempts",
    "workspace_tree_exchange_locks",
    "workspace_tree_exchange_model_sources",
)


async def test_empty_0044_roundtrip_preserves_original_file_directory_and_move_material(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        directory = WorkspaceDirectoryWriter(binding.workspace).prepare(
            DirectoryChangeSpec(path="legacy-empty", operation="create", mode=0o750)
        )
        old_directory, _ = await service.manager.directory_changes.store.preview(
            scope, binding.key, binding.fingerprint, "legacy", directory
        )
        file = WorkspaceWriter(binding.workspace).prepare(
            FileChangeSpec(
                path="legacy.txt", operation="write", content="legacy", expected_sha256=None
            )
        )
        old_file, _ = await service.workspace_edits.preview(
            scope, binding.key, binding.fingerprint, "legacy", file
        )
        moves = WorkspaceTreeMoveStore(service.workspace_edits)
        move_writer = WorkspaceTreeWriter(binding.workspace)
        snapshot = move_writer.reader.capture("left")
        move = move_writer.prepare("left", "out", expected_fingerprint=snapshot.fingerprint)
        old_move, _ = await moves.preview(
            scope,
            binding,
            TreeMovePreviewRequest(
                source_path="left",
                target_path="out",
                expected_fingerprint=snapshot.fingerprint,
                request_key="legacy",
            ),
            move,
        )
        async with store.database.engine.begin() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0054_directory_capture_models"
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0043_tree_move_ledger")
            )
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0043_tree_move_ledger"
            for name in (
                "workspace_edit_proposals",
                "workspace_directory_proposals",
                "workspace_tree_move_proposals",
            ):
                assert (
                    await connection.execute(text(f"SELECT count(*) FROM {name}"))
                ).scalar_one() == 1
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0054_directory_capture_models"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        assert (
            await service.workspace_edits.get(scope, old_file.view.proposal_id)
        ).view == old_file.view
        assert (
            await service.manager.directory_changes.store.get(scope, old_directory.view.proposal_id)
        ).view == old_directory.view
        assert await moves.proposal(scope, old_move.view.proposal_id) == old_move
    finally:
        await service.close()


async def test_nonempty_0044_refuses_loss_and_keeps_original_dual_material(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        with pytest.raises(RuntimeError, match="双完整树交换账本非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0043_tree_move_ledger")
                )
        assert await store.proposal(scope, proposal.view.proposal_id) == proposal
        async with store.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0054_directory_capture_models"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        await service.close()


async def test_actual_0044_schema_matches_base_and_retains_cyclic_fk_for_supported_backend(runtime):
    def compare(connection):
        inspector = inspect(connection)
        for name in TABLES:
            table = Base.metadata.tables[name]
            actual = {column["name"]: column for column in inspector.get_columns(name)}
            assert set(actual) == set(table.columns.keys())
            for column in table.columns:
                assert actual[column.name]["nullable"] == column.nullable
            expected_fk = {
                (
                    tuple(element.parent.name for element in constraint.elements),
                    tuple(element.target_fullname for element in constraint.elements),
                )
                for constraint in table.foreign_key_constraints
            }
            actual_fk = {
                (
                    tuple(item["constrained_columns"]),
                    tuple(
                        item["referred_table"] + "." + column for column in item["referred_columns"]
                    ),
                )
                for item in inspector.get_foreign_keys(name)
            }
            assert expected_fk == actual_fk
            assert {item["name"] for item in inspector.get_check_constraints(name)} == {
                constraint.name
                for constraint in table.constraints
                if constraint.__class__.__name__ == "CheckConstraint"
            }
        active = [
            fk
            for fk in Base.metadata.tables[TABLES[0]].foreign_key_constraints
            if fk.name == "fk_tree_exchange_active_operation"
        ]
        assert len(active) == 1 and active[0].use_alter

    async with runtime.repository.database.engine.connect() as connection:
        await connection.run_sync(compare)
