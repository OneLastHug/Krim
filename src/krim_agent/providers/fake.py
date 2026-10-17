"""确定性离线脚本；不代表商业模型质量，不发送网络请求。"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Sequence

from pydantic import JsonValue

from krim_agent.contracts import Message, parse_json

from .types import ModelEvent, ModelText, ModelToolCall


class FakeProvider:
    def __init__(self, delay: float = 0.02) -> None:
        self.delay = delay
        self.calls = 0
        self.entered = asyncio.Event()
        self.active = 0
        self.streams_closed = 0
        self.closed = False

    async def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        self.calls += 1
        self.active += 1
        self.entered.set()
        try:
            if messages[-1].role == "tool":
                result = parse_json(messages[-1].content)
                if isinstance(result, dict) and isinstance(result.get("sum"), (int, float)):
                    text = format(result["sum"], "g")
                else:
                    text = result if isinstance(result, str) else messages[-1].content
                await asyncio.sleep(self.delay)
                yield ModelText(text=text)
                return
            text = messages[-1].content
            if text == "/slow":
                await asyncio.sleep(60)
            await asyncio.sleep(self.delay)
            words = text.split()
            call_id = uuid.uuid4().hex
            if len(words) == 3 and words[0] == "/add":
                try:
                    left, right = float(words[1]), float(words[2])
                except ValueError:
                    yield ModelText(text="参数需要两个有限数字")
                    return
                yield ModelToolCall(
                    id=call_id, name="add", arguments={"left": left, "right": right}
                )
            elif words and words[0] == "/note":
                yield ModelToolCall(
                    id=call_id, name="lookup_note", arguments={"key": text[6:].strip()}
                )
            elif text.startswith("/echo "):
                yield ModelToolCall(id=call_id, name="echo", arguments={"text": text[6:]})
            else:
                yield ModelText(text="离线脚本回显：" + text)
        finally:
            self.active -= 1
            self.streams_closed += 1

    async def close(self) -> None:
        self.closed = True
