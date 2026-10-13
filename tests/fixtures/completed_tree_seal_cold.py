"""另一进程只读原封存，禁止重新观察/生成/Writer或改写材料。"""

import asyncio
import json
import sys

import krim_agent
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.database import Database
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_directory import WorkspaceDirectoryWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter


async def main():
    db_url, root, scope, plan, resource_fingerprint = sys.argv[1:6]
    database = Database(db_url)
    calls = 0

    def forbidden(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("原封存冷读不可重观察或发布")

    WorkspaceTreeSnapshotReader.capture = forbidden
    WorkspaceTreeSnapshotReader.verify = forbidden
    WorkspaceWriter.apply = forbidden
    WorkspaceDirectoryWriter.apply = forbidden
    try:
        await database.initialize()
        files = WorkspaceEditStore(database)
        directories = WorkspaceDirectoryStore(database, owner_id=files.owner_id)
        store = CompletedTreeSealStore(WorkspaceTreePlanStore(files, directories))
        with Workspace(root, writable=True) as workspace:
            binding = WorkspaceBinding("files", "1", workspace, resource_fingerprint)
            sealed = await store.get(scope, binding, plan)
            assert sealed is not None and calls == 0
            print(
                json.dumps(
                    {
                        "package": str(krim_agent.__file__),
                        "plan_id": sealed.origin.view.plan_id,
                        "fingerprint": sealed.origin.snapshot.fingerprint,
                        "proof_hash": sealed.origin.view.proof_hash,
                        "operation_ids": sealed.origin.operation_ids,
                        "inode": sealed.origin.snapshot.entries[-1].file.inode,
                        "bytes_hex": sealed.origin.snapshot.entries[-1].file.data.hex(),
                        "replay_calls": calls,
                    }
                ),
                flush=True,
            )
    finally:
        await database.close()


asyncio.run(main())
