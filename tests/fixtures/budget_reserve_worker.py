"""两个真实 Python 进程竞争同一预算；显式 barrier 后才开始预留。"""

import asyncio
import json
import sys

from krim_agent.persistence import Database
from krim_agent.persistence.budgets import UsageBudgetStore
from krim_agent.protocol.domain import ServiceError


async def main():
    database = Database(f"sqlite+aiosqlite:///{sys.argv[1]}")
    await database.initialize()
    store = UsageBudgetStore(database)
    print("READY", flush=True)
    await asyncio.to_thread(sys.stdin.readline)
    try:
        request = await store.reserve(sys.argv[2], 0, (sys.argv[3],), 5, 5, sys.argv[2])
        print(json.dumps({"state": request.state, "request_id": request.request_id}), flush=True)
    except ServiceError as error:
        print(json.dumps({"code": error.code}), flush=True)
    finally:
        await database.close()


asyncio.run(main())
