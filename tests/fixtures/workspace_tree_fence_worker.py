"""独立进程竞争树原事务准入；凭据仅由标准输入传递。"""

import asyncio
import json
import sys

from krim_agent.persistence.database import Database
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_directory import DirectoryChangeAction
from krim_agent.workspace_tree import TreePlanFence


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
        proposal = await store.get(value["scope"], request.proposal_id)
        attempt, created = await store.reserve(
            value["scope"],
            request,
            value["resource"],
            proposal.prepared,
            tree_fence=TreePlanFence(**value["fence"]),
        )
        print(
            json.dumps({"operation_id": attempt.view.operation_id, "created": created}), flush=True
        )
        if value.get("hold"):
            await asyncio.Event().wait()
    finally:
        await database.close()


asyncio.run(main())
