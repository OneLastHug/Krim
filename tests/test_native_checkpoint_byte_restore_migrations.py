"""0049真实非空往返、旧请求材料保全与bytes降级拒绝。"""

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from test_native_checkpoint_byte_restore import mixed_setup, restore_preview

from krim_agent.persistence.database import Base
from krim_agent.persistence.migration import migration_config
from krim_agent.sdk import WorkspaceCheckpointCreate


async def test_actual_0048_restore_rows_roundtrip_preserves_old_json_hash_blob_and_fk(
    runtime, tmp_path
):
    file = tmp_path / "file"
    file.write_bytes(b"original\n")
    service, agent, _ = await mixed_setup(runtime, tmp_path)
    database = runtime.repository.database
    try:
        accepted = await agent.create_file_checkpoint(
            WorkspaceCheckpointCreate(paths=["file"], request_key="old-text")
        )
        checkpoint = await agent.wait_file_checkpoint(accepted.checkpoint_id)
        file.write_bytes(b"current\n")
        plan = await restore_preview(agent, checkpoint)
        assert plan.phase == "ready" and "baseline_capture_format" not in plan.model_dump()
        before = plan.model_dump(mode="json")
        async with database.engine.connect() as connection:
            original_hash = (
                await connection.execute(
                    text("SELECT request_hash FROM workspace_restore_plans WHERE plan_id=:id"),
                    {"id": plan.plan_id},
                )
            ).scalar_one()
            materials = (
                await connection.execute(
                    text(
                        "SELECT capture_metadata,content FROM native_workspace_checkpoint_files "
                        "ORDER BY checkpoint_id,position"
                    )
                )
            ).all()
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0048_native_checkpoint_bytes"
                )
            )
            names = await connection.run_sync(
                lambda sync: {
                    column["name"]
                    for column in inspect(sync).get_columns("workspace_restore_plans")
                }
            )
            assert "baseline_capture_format" not in names
        await database.initialize()
        assert (await agent.file_checkpoint_restore(plan.plan_id)).model_dump(mode="json") == before
        async with database.engine.connect() as connection:
            assert (await connection.execute(text("PRAGMA foreign_key_check"))).all() == []
            assert (
                await connection.execute(
                    text("SELECT request_hash FROM workspace_restore_plans WHERE plan_id=:id"),
                    {"id": plan.plan_id},
                )
            ).scalar_one() == original_hash
            assert (
                await connection.execute(
                    text(
                        "SELECT capture_metadata,content FROM "
                        "native_workspace_checkpoint_files "
                        "ORDER BY checkpoint_id,position"
                    )
                )
            ).all() == materials

            def compare(sync):
                def included(obj, _name, kind, _reflected, _compare_to):
                    table = obj if kind == "table" else getattr(obj, "table", None)
                    return table is not None and table.name.startswith("workspace_restore_")

                return compare_metadata(
                    MigrationContext.configure(
                        sync,
                        opts={
                            "include_object": included,
                        },
                    ),
                    Base.metadata,
                )

            assert await connection.run_sync(compare) == []
        byte_plan = await restore_preview(
            agent, checkpoint, key="new-bytes", baseline_capture_format="bytes"
        )
        assert byte_plan.phase == "ready" and byte_plan.baseline_capture_format == "bytes"
        with pytest.raises(RuntimeError, match="字节恢复前基线"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0048_native_checkpoint_bytes"
                    )
                )
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0057_missing_directory_rollbacks"
        with pytest.raises(IntegrityError):
            async with database.engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE workspace_restore_plans SET "
                        "baseline_capture_format='binary' WHERE plan_id=:id"
                    ),
                    {"id": byte_plan.plan_id},
                )
        assert (
            await agent.file_checkpoint_restore(byte_plan.plan_id)
        ).baseline_capture_format == "bytes"
    finally:
        await service.close()
