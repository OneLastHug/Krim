"""0043真实空迁移/旧历史保持、非空降级和namespace结构门禁。"""

import pytest
from alembic import command
from sqlalchemy import text
from test_workspace_tree_move_ledger import prepared

from krim_agent.persistence.migration import migration_config
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter


async def test_empty_0043_downgrade_upgrade_keeps_original_directory_history(runtime, tmp_path):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        original = WorkspaceDirectoryWriter(binding.workspace).prepare(
            DirectoryChangeSpec(path="legacy-empty", operation="create", mode=0o750)
        )
        previous, _ = await service.manager.directory_changes.store.preview(
            scope, binding.key, binding.fingerprint, "legacy", original
        )
        async with store.database.engine.begin() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0051_checkpoint_restore_models"
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0042_workspace_tree_model_sources"
                )
            )
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0042_workspace_tree_model_sources"
            assert (
                await connection.execute(text("SELECT count(*) FROM workspace_directory_proposals"))
            ).scalar_one() == 1
            await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0051_checkpoint_restore_models"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        assert (
            await service.manager.directory_changes.store.get(scope, previous.view.proposal_id)
        ).view == previous.view
    finally:
        await service.close()


async def test_nonempty_0043_downgrade_refuses_loss_and_keeps_current_root_material(
    runtime, tmp_path
):
    service, agent, binding, writer, store, request, change = await prepared(runtime, tmp_path)
    scope = "session:" + agent.session_id
    try:
        original, _ = await store.preview(scope, binding, request, change)
        with pytest.raises(RuntimeError, match="非空树移动账本非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0042_workspace_tree_model_sources"
                    )
                )
        assert await store.proposal(scope, original.view.proposal_id) == original
        async with store.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0051_checkpoint_restore_models"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        await service.close()
