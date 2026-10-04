"""经真实 HTTP/SSE/iframe 的 Python Host Drag 预览与 drop 回程。"""

import asyncio
import json
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.drag import DragPreview
from krim_agent.prompt_context import MentionInput


async def main() -> None:
    async with RemoteHostHandle(sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]) as host:
        state = await host.state()
        assert state.topic_id is not None
        entered = await host.drag_enter(state.topic_id, DragPreview(kind="text", label="Host 引用"))
        assert entered.active and not (await host.prompt_context(state.topic_id)).mentions
        assert not (await host.drag_leave(state.topic_id)).active
        reference = await host.drag_drop(
            state.topic_id, MentionInput(label="Host 引用", content="Python 明确提供的拖放正文")
        )
        context = await host.prompt_context(state.topic_id)
        assert reference.id in {value.id for value in context.mentions}
        print(
            json.dumps(
                {
                    "topic_id": state.topic_id,
                    "mention_id": reference.id,
                    "content": reference.content,
                },
                ensure_ascii=False,
            )
        )


asyncio.run(main())
