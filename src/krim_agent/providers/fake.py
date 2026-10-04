"""确定性离线演示；明确不是 LLM，也不执行网络请求。"""

import asyncio
import re
from collections.abc import AsyncIterator, Sequence

from krim_agent.protocol.domain import MessageView
from krim_agent.tools.core import ToolDefinition

from .types import ModelText, ModelToolCall


class FakeProvider:
    name = "fake-offline"

    def __init__(self, delay: float = 0.01) -> None:
        self.delay = delay

    async def stream(
        self, messages: Sequence[MessageView], tools: Sequence[ToolDefinition]
    ) -> AsyncIterator[ModelText | ModelToolCall]:
        last_user = next(
            (index for index in range(len(messages) - 1, -1, -1) if messages[index].role == "user"),
            None,
        )
        if last_user is None:
            raise ValueError("缺少用户消息")
        current = messages[last_user]
        results = [message for message in messages[last_user + 1 :] if message.role == "tool"]
        if results:
            reply = "[fake] 工具结果：" + results[-1].content
        else:
            original = current.details.get("_prompt_context_raw_text", current.content)
            text = (original if isinstance(original, str) else current.content).strip()
            number = r"(-?\d+(?:\.\d+)?)"
            match = re.fullmatch(rf"/add\s+{number}\s+{number}", text)
            if match is None:
                match = re.fullmatch(rf"(?:计算\s*)?{number}\s*\+\s*{number}", text)
            if match is not None:
                yield ModelToolCall(
                    name="add", arguments={"left": float(match[1]), "right": float(match[2])}
                )
                return
            if text.startswith("/echo "):
                yield ModelToolCall(name="echo", arguments={"text": text[6:]})
                return
            if text.startswith("/question "):
                yield ModelToolCall(name="question", arguments={"question": text[10:]})
                return
            if text.startswith("/skill "):
                parts = text[7:].split(maxsplit=1)
                yield ModelToolCall(
                    name="read_skill",
                    arguments={"name": parts[0], "resource": parts[1] if len(parts) > 1 else None},
                )
                return
            reply = "[fake] 收到：" + current.content.strip()
        reply = reply[:8192]
        for position in range(0, len(reply), 16):
            await asyncio.sleep(self.delay)
            yield ModelText(text=reply[position : position + 16])

    async def close(self) -> None:
        """离线 provider 不持有外部资源。"""
