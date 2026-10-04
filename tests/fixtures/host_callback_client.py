"""真实 HTTP/SSE/iframe callback 回程；资源变化发生在 Python 字典中。"""

import asyncio
import json
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle


async def main() -> None:
    resource = {"value": "after"}
    async with RemoteHostHandle(sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]) as host:

        async def apply(data):
            assert data["action"] == "undo"
            resource["value"] = "before"
            return {"status": "done", "value": resource["value"], "metadata": data["metadata"]}

        host.on_request("changeList/invoke", apply)

        async def listen():
            async for _event in host.events():
                pass

        task = asyncio.create_task(listen())
        try:
            value = await host.emit(
                "host/request",
                {
                    "channel": "changeList/invoke",
                    "data": {"action": "undo", "metadata": {"source": "test"}},
                },
            )
            assert value.ok and resource["value"] == "before"
            assert value.data["metadata"] == {"source": "test"}
            print(json.dumps(value.data, ensure_ascii=False))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


asyncio.run(main())
