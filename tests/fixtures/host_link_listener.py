"""真实浏览器 Link 验收：Python Host 等待一次经过 iframe/HTTP/SSE 的点击通知。"""

import asyncio
import json
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle


async def main() -> None:
    received = []
    async with RemoteHostHandle(sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]) as host:
        host.on_link(lambda value: received.append(value))
        print("ready", flush=True)
        async with asyncio.timeout(15):
            async for _event in host.events():
                if received:
                    break
        assert len(received) == 1
        print(json.dumps(received[0].model_dump(mode="json"), ensure_ascii=False))


asyncio.run(main())
