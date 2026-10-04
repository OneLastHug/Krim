"""在真实文件写入后强制退出；用于验证不确定结果绝不自动重放。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from krim_agent.change_list import ChangeInvoke, ChangeItemInput, ChangeSync
from krim_agent.persistence import ChangeListRepository, Database, KrimRepository


async def main():
    database = Database(f"sqlite+aiosqlite:///{sys.argv[1]}")
    await database.initialize()
    topic = await KrimRepository(database).create_topic("崩溃恢复")
    changes = ChangeListRepository(database)
    await changes.sync(
        topic.id, ChangeSync(revision=0, items=[ChangeItemInput(id="one", title="实际资源")])
    )
    operation, created = await changes.prepare(
        topic.id, ChangeInvoke(revision=1, action="undo", item_id="one", idempotency_key="once")
    )
    assert created
    await asyncio.to_thread(Path(sys.argv[2]).write_text, "旧值")
    print(
        json.dumps({"topic_id": topic.id, "operation_id": operation.execution.operation_id}),
        flush=True,
    )
    os._exit(0)


asyncio.run(main())
