"""浏览器 Locale 验收的真实 Python Host 调用；仅使用本机测试 Key。"""

import asyncio
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle


async def main() -> None:
    async with RemoteHostHandle(sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]) as host:
        result = await host.set_locale(sys.argv[3])
        state = await host.state()
        assert state.locale == result.effective
        print(result.model_dump_json())


asyncio.run(main())
