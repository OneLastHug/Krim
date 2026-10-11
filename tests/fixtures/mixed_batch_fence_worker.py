"""独立进程只竞争原事务准入；凭据经stdin传入，不写命令行或日志。"""

import asyncio
import json
import sys

from krim_agent.persistence.database import Database
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_directory import DirectoryChangeAction
from krim_agent.workspace_mixed_batch import MixedBatchFence


async def main():
    value = json.loads(sys.stdin.read())
    database = Database(value["database"])
    try:
        source = value["source"]
        store = (
            WorkspaceEditStore(database, owner_id=value["owner"])
            if source == "file"
            else WorkspaceDirectoryStore(database, owner_id=value["owner"])
        )
        request = (
            WorkspaceChangeAction if source == "file" else DirectoryChangeAction
        ).model_validate(value["request"])
        fence = MixedBatchFence(**value["fence"])
        proposal = await store.get(value["scope"], request.proposal_id)
        attempt, created = await store.reserve(
            value["scope"], request, value["resource"], proposal.prepared, mixed_fence=fence
        )
        print(json.dumps({"operation_id": attempt.view.operation_id, "created": created}))
    finally:
        await database.close()


asyncio.run(main())
