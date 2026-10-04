"""Playwright 启动的真实 Python Host SDK 客户端；只使用本机测试 Key。"""

import asyncio
import json
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle


async def main() -> None:
    async with RemoteHostHandle(
        sys.argv[1], sys.argv[2], os.environ.get("KRIM_AGENT_API_KEY", "")
    ) as host:
        state = await host.state()
        assert state.connected and state.iframe
        theme = await host.set_theme("dark")
        assert theme.theme == "dark"
        width = await host.resize(840)
        assert width.width == 840
        prompt = await host.prompt("由 Python Host SDK 插入")
        assert prompt["value"] == "由 Python Host SDK 插入"
        closed = await host.sidebar(False)
        assert not closed.open
        reopened = await host.sidebar(True)
        assert reopened.open
        await host.notice("Python 回程完成")
        print(
            json.dumps(
                {"connected": state.connected, "width": width.width, "theme": theme.theme},
                ensure_ascii=False,
            )
        )


asyncio.run(main())
