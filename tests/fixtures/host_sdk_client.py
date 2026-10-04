"""Playwright 启动的真实 Python Host SDK 客户端；只使用本机测试 Key。"""

import asyncio
import json
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.prompt_context import ContextTagInput, MentionInput


async def main() -> None:
    async with RemoteHostHandle(
        sys.argv[1], sys.argv[2], os.environ.get("KRIM_AGENT_API_KEY", "")
    ) as host:
        state = await host.state()
        assert state.connected and state.iframe
        if len(sys.argv) > 3 and sys.argv[3] == "prompt-context":
            assert state.topic_id is not None
            reference = await host.add_mention(
                state.topic_id, MentionInput(label="宿主引用", content="Python 提供的引用")
            )
            context = await host.add_context_tag(
                state.topic_id, ContextTagInput(name="宿主背景", content="Python 提供的背景")
            )
            assert any(mention.id == reference.id for mention in context.mentions)
            assert (await host.prompt_context(state.topic_id)).context_tags
            print(
                json.dumps(
                    {"topic_id": state.topic_id, "mention_id": reference.id}, ensure_ascii=False
                )
            )
            return
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
