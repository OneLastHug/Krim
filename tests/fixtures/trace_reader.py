"""独立 Python 进程只读已保存 trace，不启动 Runtime 或 exporter。"""

import asyncio
import json
import sys

from krim_agent.persistence import Database
from krim_agent.persistence.traces import TraceStore


async def main():
    database = Database(sys.argv[1])
    await database.initialize()
    try:
        result = await TraceStore(database).trace(sys.argv[2])
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
    finally:
        await database.close()


asyncio.run(main())
