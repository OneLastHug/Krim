"""0054来源迁移保护模型证据，非空Host原行/旧摘要/材料可安全往返。"""

import json
import time
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy import delete, inspect, text
from test_model_directory_checkpoint_store import context as context

from krim_agent.persistence import Database
from krim_agent.persistence.database import Base, WorkspaceDirectoryCaptureModelSourceRow
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore


async def test_actual_0053_host_upgrade_roundtrip_preserves_original_hash_rows_and_opaque_blob(
    tmp_path,
):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'legacy.sqlite'}")
    vector = json.loads(
        (
            Path(__file__).parent / "fixtures/directory_checkpoint_host_legacy_vectors.json"
        ).read_text()
    )
    try:
        async with database.engine.begin() as sql:
            await sql.run_sync(
                lambda sync: command.upgrade(migration_config(sync), "0053_directory_captures")
            )
            await sql.execute(
                text(
                    "INSERT INTO topics(id,title,locale,created_at,updated_at) "
                    "VALUES ('old','原Host','zh-CN','2026-10-15','2026-10-15')"
                )
            )
            await sql.execute(
                text(
                    "INSERT INTO workspace_directory_checkpoints("
                    "checkpoint_id,scope_id,topic_id,request_key,request_hash,title,root_path,"
                    "workspace_key,resource_fingerprint,workspace_fingerprint,sna"
                    "pshot_fingerprint,metadata,payload,created_at) "
                    "VALUES ('old-opaque-material','topic:old','old','opaque',:ha"
                    "sh,'旧材料','tree','files',:hash,:hash,:hash,'{}',:blob,'2026-1"
                    "0-15')"
                ),
                {"hash": "a" * 64, "blob": b"\0\xffopaque-0053"},
            )
            await sql.execute(
                text(
                    "INSERT INTO workspace_directory_captures("
                    "checkpoint_id,scope_id,topic_id,request_key,request_hash,tit"
                    "le,root_path,workspace_key,"
                    "resource_fingerprint,workspace_fingerprint,status,owner_id,l"
                    "ease_expires_at,token,created_at,updated_at) "
                    "VALUES ('old-host','topic:old','old',:key,:hash,:title,:root"
                    ",:workspace,:resource,:fingerprint,"
                    "'capturing','old-owner',:lease,:token,'2026-10-15','2026-10-15')"
                ),
                {
                    "key": vector["request"]["request_key"],
                    "hash": vector["request_hash"],
                    "title": vector["request"]["title"],
                    "root": vector["request"]["root_path"],
                    "workspace": vector["workspace_key"],
                    "resource": vector["resource_fingerprint"],
                    "fingerprint": vector["workspace_fingerprint"],
                    "lease": time.time() + 60,
                    "token": "c" * 64,
                },
            )
            columns = [
                row[1]
                for row in (
                    await sql.execute(text("PRAGMA table_info(workspace_directory_captures)"))
                ).all()
            ]
            before_row = (
                await sql.execute(text("SELECT * FROM workspace_directory_captures"))
            ).one()
            before = {
                row[0]: (row[1], row[2])
                for row in (
                    await sql.execute(
                        text(
                            "SELECT name,rootpage,sql FROM sqlite_master WHERE "
                            "type='table' AND name NOT IN "
                            "('alembic_version','sqlite_sequence')"
                        )
                    )
                ).all()
            }
        # 此用例审计0053→0054的单阶段历史DDL，不能随正常latest-head移动。
        async with database.engine.begin() as sql:
            await sql.run_sync(
                lambda sync: command.upgrade(
                    migration_config(sync), "0054_directory_capture_models"
                )
            )
        store = WorkspaceDirectoryCaptureStore(
            WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(database))
        )
        original = (await store.get("topic:old", "old-host")).model_dump(mode="json")
        assert original["status"] == "capturing" and original["snapshot"] is None
        assert await store.source("topic:old", "old-host") is None
        async with database.engine.begin() as sql:
            for phase in ("upgraded", "roundtrip"):
                assert (
                    await sql.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0054_directory_capture_models"
                after = {
                    row[0]: (row[1], row[2])
                    for row in (
                        await sql.execute(
                            text(
                                "SELECT name,rootpage,sql FROM sqlite_master WHERE "
                                "type='table' AND name NOT IN "
                                "('alembic_version','sqlite_sequence')"
                            )
                        )
                    ).all()
                }
                assert set(after) == set(before) | {"workspace_directory_capture_model_sources"}
                for name in before:
                    if name != "workspace_directory_captures":
                        assert after[name][1] == before[name][1]
                if phase == "upgraded":
                    assert (
                        after["workspace_directory_captures"][0]
                        == before["workspace_directory_captures"][0]
                    )
                assert (
                    await sql.execute(
                        text("SELECT " + ",".join(columns) + " FROM workspace_directory_captures")
                    )
                ).one() == before_row
                assert (
                    await sql.execute(text("SELECT model_bound FROM workspace_directory_captures"))
                ).scalar_one() == 0
                assert (
                    await sql.execute(text("SELECT payload FROM workspace_directory_checkpoints"))
                ).scalar_one() == b"\0\xffopaque-0053"
                assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
                if phase == "upgraded":
                    await sql.run_sync(
                        lambda sync: command.downgrade(
                            migration_config(sync), "0053_directory_captures"
                        )
                    )
                    assert (
                        await sql.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar_one() == "0053_directory_captures"
                    assert (
                        await sql.execute(text("SELECT * FROM workspace_directory_captures"))
                    ).one() == before_row
                    await sql.run_sync(
                        lambda sync: command.upgrade(
                            migration_config(sync), "0054_directory_capture_models"
                        )
                    )
        assert (await store.get("topic:old", "old-host")).model_dump(mode="json") == original
    finally:
        await database.close()


@pytest.mark.parametrize("marker_only", [False, True])
async def test_0054_model_source_or_marker_only_cannot_be_lossily_downgraded(context, marker_only):
    service, _agent, binding, scope, source, request = context
    store = service.directory_checkpoint_store
    original, _ = await store.reserve(scope, binding, request, model_source=source)
    if marker_only:
        async with store.database.sessions() as sql, sql.begin():
            await sql.execute(
                delete(WorkspaceDirectoryCaptureModelSourceRow).where(
                    WorkspaceDirectoryCaptureModelSourceRow.checkpoint_id == original.checkpoint_id
                )
            )
    with pytest.raises(RuntimeError, match="模型目录检查点不能降级"):
        async with store.database.engine.begin() as sql:
            await sql.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0053_directory_captures")
            )
    async with store.database.engine.connect() as sql:
        assert (
            await sql.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0057_missing_directory_rollbacks"
        assert (
            await sql.execute(text("SELECT request_hash FROM workspace_directory_captures"))
        ).scalar_one()
        assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
    if not marker_only:
        assert await store.get(scope, original.checkpoint_id) == original
        assert await store.source(scope, original.checkpoint_id) == source


async def test_0054_source_columns_foreign_keys_unique_and_flag_checks_match_orm(repository):
    def compare(sync):
        schema = inspect(sync)
        name = "workspace_directory_capture_model_sources"
        table = Base.metadata.tables[name]
        columns = {column["name"]: column for column in schema.get_columns(name)}
        assert set(columns) == set(table.columns.keys())
        assert all(columns[column.name]["nullable"] == column.nullable for column in table.columns)
        assert {tuple(item["column_names"]) for item in schema.get_unique_constraints(name)} == {
            ("run_id", "call_id")
        }
        assert {
            (
                tuple(item["constrained_columns"]),
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for item in schema.get_foreign_keys(name)
        } == {
            (("checkpoint_id",), "workspace_directory_captures", ("checkpoint_id",)),
            (("run_id",), "runs", ("id",)),
        }
        assert "ck_directory_capture_model_bound" in {
            item["name"] for item in schema.get_check_constraints("workspace_directory_captures")
        }
        assert {item["name"] for item in schema.get_check_constraints(name)} == {
            "ck_directory_capture_call"
        }

    async with repository.database.engine.connect() as sql:
        await sql.run_sync(compare)
