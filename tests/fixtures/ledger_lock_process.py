"""独立进程真实申请账本写锁/原命令资格；只输出同步标记和非敏感结果。"""

import asyncio
import json
import sys

from sqlalchemy import text

from krim_agent.persistence import Database
from krim_agent.persistence.commands import CommandStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError


async def main() -> None:
    payload = json.loads(sys.stdin.readline())
    database = Database(payload["url"])
    try:
        print("WAITING", flush=True)
        if payload["operation"] == "authorize":
            store = CommandStore(database, owner_id=payload["owner"])
            try:
                await store.authorize(payload["scope"], payload["identifier"], payload["token"])
            except ServiceError as error:
                print(json.dumps({"error": error.code}), flush=True)
            else:
                print(json.dumps({"error": None}), flush=True)
        else:
            async with database.write_lock, database.sessions.begin() as session:
                if payload["kind"] == "command":
                    await CommandStore._writer(session)
                else:
                    await WorkspaceEditStore(database)._writer(session)
                changed = await session.scalar(text("SELECT total_changes()"))
                print(json.dumps({"locked": True, "changes": changed}), flush=True)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
