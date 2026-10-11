"""项目外安装包核对0039混合冻结Store：迁移、冷读、取消和零文件副作用。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import text

import krim_agent
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-mixed-store-trial-", dir="/data/tmp/root") as folder:
        root = Path(folder)
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        workspace_root = root / "workspace"
        workspace_root.mkdir()
        with Workspace(workspace_root, writable=True) as workspace:
            files = WorkspaceEditStore(database)
            directories = WorkspaceDirectoryStore(database, owner_id=files.owner_id)
            store = MixedBatchStore(files, directories)
            topic = await KrimRepository(database).create_topic("wheel mixed freeze")
            scope = "topic:" + topic.id
            file, _ = await files.preview(
                scope,
                "files",
                workspace.fingerprint,
                "file",
                WorkspaceWriter(workspace).prepare(
                    FileChangeSpec(
                        path="file.txt", operation="write", expected_sha256=None, content="new"
                    )
                ),
            )
            directory, _ = await directories.preview(
                scope,
                "files",
                workspace.fingerprint,
                "directory",
                WorkspaceDirectoryWriter(workspace).prepare(
                    DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
                ),
            )
            request = MixedBatchRequest(
                action="apply",
                request_key="mixed",
                targets=[
                    MixedBatchTarget(
                        source="file", proposal_id=file.view.proposal_id, expected_revision=0
                    ),
                    MixedBatchTarget(
                        source="directory",
                        proposal_id=directory.view.proposal_id,
                        expected_revision=0,
                    ),
                ],
            )
            accepted, created = await store.reserve(scope, "files", workspace.fingerprint, request)
            assert created and all(
                x.status == "pending" and x.operation is None for x in accepted.items
            )
            assert not list(workspace_root.iterdir())
            again, created = await store.reserve(scope, "files", workspace.fingerprint, request)
            assert not created and again == accepted
            cancelled = await store.cancel(scope, accepted.batch_id)
            assert cancelled.status == "cancelled"
            assert all(x.status == "skipped" for x in cancelled.items)
            assert await store.by_key(scope, "mixed") == cancelled
            async with database.engine.connect() as sql:
                assert (
                    await sql.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0041_workspace_tree_plans"
                assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
        await database.close()
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "schema": "0041_workspace_tree_plans",
                    "mixed_pending": True,
                    "same_key_zero_replay": True,
                    "cancel_closes_pending": True,
                    "zero_file_effects": True,
                }
            )
        )


asyncio.run(main())
