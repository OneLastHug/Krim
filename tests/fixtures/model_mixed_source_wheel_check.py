"""独立wheel核对0040附表/原Run来源、停止后围栏和零新attempt。"""

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
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter
from krim_agent.workspace_mixed_batch import (
    MixedBatchModelSource,
    MixedBatchRequest,
    MixedBatchTarget,
)
from krim_agent.workspace_writer import WorkspaceWriter


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(
        prefix="krim-model-mixed-source-wheel-", dir="/data/tmp/root"
    ) as folder:
        root = Path(folder)
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        try:
            repo = KrimRepository(database)
            topic = await repo.create_topic("source wheel")
            run, _ = await repo.begin_run(
                topic.id, ChatRequest(text="source", idempotency_key="run")
            )
            scope = "topic:" + topic.id
            (root / "files").mkdir()
            with Workspace(root / "files", writable=True) as workspace:
                files = WorkspaceEditStore(database)
                directories = WorkspaceDirectoryStore(database, owner_id=files.owner_id)
                store = MixedBatchStore(files, directories)
                file, _ = await files.preview(
                    scope,
                    "files",
                    workspace.fingerprint,
                    "file",
                    WorkspaceWriter(workspace).prepare(
                        FileChangeSpec(
                            path="file.txt", operation="write", content="data", expected_sha256=None
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
                    request_key="model",
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
                origin = MixedBatchModelSource(run.id, "source-call")
                accepted, created = await store.reserve(
                    scope, "files", workspace.fingerprint, request, model_source=origin
                )
                assert created
                execution = await store.execution(scope, accepted.batch_id)
                assert execution.model_source == origin
                child = execution.children[0]
                operation, created = await files.reserve(
                    scope,
                    child.request,
                    workspace.fingerprint,
                    file.prepared,
                    mixed_fence=child.fence,
                    run_id=run.id,
                    call_id="nested-file",
                )
                assert created
                await files.assert_execution(scope, operation.view.operation_id)
                await repo.finish(run.id, "completed", None, [])
                for action in (
                    lambda: files.assert_execution(scope, operation.view.operation_id),
                    lambda: store.renew(accepted.batch_id, execution.token),
                    lambda: store.execution(scope, accepted.batch_id),
                ):
                    try:
                        await action()
                    except ServiceError as error:
                        assert error.code in {
                            "workspace_run_closed",
                            "workspace_mixed_batch_run_closed",
                        }
                    else:
                        raise AssertionError("closed Run unexpectedly executed")
                repeated, created = await store.reserve(
                    scope, "files", workspace.fingerprint, request, model_source=origin
                )
                assert not created and repeated.batch_id == accepted.batch_id
                cancelled = await store.cancel(scope, accepted.batch_id)
                assert (
                    cancelled.status == "uncertain"
                    and cancelled.items[0].operation.operation_id == operation.view.operation_id
                )
                assert (
                    not (root / "files" / "file.txt").exists()
                    and not (root / "files" / "empty").exists()
                )
                async with database.engine.connect() as sql:
                    assert (
                        await sql.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar_one() == "0041_workspace_tree_plans"
                    assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "schema": "0041_workspace_tree_plans",
                        "original_run_call": True,
                        "closed_run_fenced": True,
                        "same_key_zero_replay": True,
                        "zero_file_effects": True,
                    }
                )
            )
        finally:
            await database.close()


asyncio.run(main())
