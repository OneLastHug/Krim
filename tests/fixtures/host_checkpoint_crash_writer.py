"""实际备份写入后强制退出，验证宿主 Checkpoint 不自动重放。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from krim_agent.checkpoint import CheckpointCreate
from krim_agent.persistence import CheckpointRepository, Database, KrimRepository


async def main() -> None:
    database = Database(f"sqlite+aiosqlite:///{sys.argv[1]}")
    await database.initialize()
    topic = await KrimRepository(database).create_topic("宿主快照崩溃")
    operation, created = await CheckpointRepository(database).prepare_create(
        topic.id, CheckpointCreate(revision=0, idempotency_key="crashed")
    )
    assert created
    await asyncio.to_thread(Path(sys.argv[2]).write_text, "实际备份原值", encoding="utf-8")
    print(
        json.dumps({"topic_id": topic.id, "operation_id": operation.execution.operation_id}),
        flush=True,
    )
    os._exit(0)


asyncio.run(main())
