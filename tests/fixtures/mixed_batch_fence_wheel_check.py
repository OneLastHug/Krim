"""项目外wheel通过可信宿主围栏执行真实混合apply/keep/undo并冷读取。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import func, select, text

import krim_agent
from krim_agent.application.agents import AgentService
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import WorkspaceDirectoryAttemptRow, WorkspaceEditAttemptRow
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import AgentSession
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_directory import DirectoryChangeSpec
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-mixed-fence-wheel-", dir="/data/tmp/root") as folder:
        root = Path(folder)
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )

        async def allow(_definition, _context):
            return "allow"

        runtime.tool_policy = allow
        service = AgentService(runtime, runtime.settings)
        workspace = root / "workspace"
        workspace.mkdir()
        binding = service.register_workspace(
            "files", workspace, version="1", writable=True, default=True
        )
        agent = AgentSession(service, (await service.spawn("default")).agent_id)
        store = MixedBatchStore(service.workspace_edits, service.manager.directory_changes.store)
        scope = "session:" + agent.session_id
        try:
            file = await agent.preview_file_change(
                FileChangeSpec(
                    path="file.txt", operation="write", content="data", expected_sha256=None
                ),
                request_key="file-preview",
            )
            directory = await agent.preview_directory_change(
                DirectoryChangeSpec(path="empty", operation="create", mode=0o750),
                request_key="directory-preview",
            )
            targets = [("file", file.proposal_id), ("directory", directory.proposal_id)]
            results = []
            for action in ("apply", "keep", "undo"):
                revisions = [
                    (
                        await (store.files if source == "file" else store.directories).get(
                            scope, proposal
                        )
                    ).view.revision
                    for source, proposal in targets
                ]
                request = MixedBatchRequest(
                    action=action,
                    request_key=action,
                    targets=[
                        MixedBatchTarget(
                            source=source, proposal_id=proposal, expected_revision=revision
                        )
                        for (source, proposal), revision in zip(targets, revisions, strict=True)
                    ],
                )
                accepted, created = await store.reserve(
                    scope, binding.key, binding.fingerprint, request
                )
                assert created
                execution = await store.execution(scope, accepted.batch_id)
                stamp = (
                    [(workspace / path).stat() for path in ("file.txt", "empty")]
                    if action == "keep"
                    else None
                )
                for child in execution.children:
                    value = await service._workspace_call(
                        agent.agent_id,
                        "workspace_change" if child.fence.source == "file" else "directory_change",
                        child.request.model_dump(),
                        confirmed=True,
                        mixed_fence=child.fence,
                    )
                    assert value["status"] == "done"
                    item = await store.finish_child(scope, child.fence)
                    assert item.operation.operation_id == value["operation_id"]
                finished = await store.finish(scope, accepted.batch_id, execution.token)
                assert finished.status == "done"
                if stamp:
                    assert stamp == [(workspace / path).stat() for path in ("file.txt", "empty")]
                repeated, created = await store.reserve(
                    scope, binding.key, binding.fingerprint, request
                )
                assert not created and repeated == finished
                results.append(finished)
            assert not (workspace / "file.txt").exists() and not (workspace / "empty").exists()
            cold_db = Database(str(database.engine.url))
            try:
                cold = MixedBatchStore(
                    WorkspaceEditStore(cold_db, owner_id="cold"),
                    WorkspaceDirectoryStore(cold_db, owner_id="cold"),
                )
                for result in results:
                    assert await cold.get(scope, result.batch_id) == result
                    assert await cold.by_key(scope, result.request_key) == result
                async with cold_db.sessions() as sql:
                    assert (
                        await sql.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow))
                        == 3
                    )
                    assert (
                        await sql.scalar(
                            select(func.count()).select_from(WorkspaceDirectoryAttemptRow)
                        )
                        == 3
                    )
                async with cold_db.engine.connect() as sql:
                    assert (
                        await sql.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar_one() == "0040_mixed_batch_model_sources"
                    assert not (await sql.execute(text("PRAGMA foreign_key_check"))).all()
            finally:
                await cold_db.close()
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "schema": "0040_mixed_batch_model_sources",
                        "real_apply_keep_undo": True,
                        "keep_zero_write": True,
                        "cold_original_fact": True,
                        "same_key_zero_replay": True,
                        "file_attempts": 3,
                        "directory_attempts": 3,
                    }
                )
            )
        finally:
            await service.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
