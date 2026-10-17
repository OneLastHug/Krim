"""宿主控制的真实解析版本；无参考答案输入、无网络目标。"""

import math
from collections.abc import AsyncIterator, Sequence
from typing import Literal

from pydantic import JsonValue

from krim_agent.contracts import Message

from .fake import FakeProvider
from .types import ModelEvent


class EvalScriptProvider(FakeProvider):
    def __init__(self, mode: Literal["integer", "decimal"]) -> None:
        super().__init__()
        self.mode = mode

    async def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        selected = list(messages)
        if self.mode == "integer" and selected[-1].role == "user":
            words = selected[-1].content.split()
            if len(words) == 3 and words[0] == "/add":
                try:
                    left, right = float(words[1]), float(words[2])
                    if math.isfinite(left) and math.isfinite(right):
                        selected[-1] = selected[-1].model_copy(
                            update={"content": f"/add {int(left)} {int(right)}"}
                        )
                except ValueError:
                    pass
        stream = super().stream(selected, tools)
        try:
            async for item in stream:
                yield item
        finally:
            close = getattr(stream, "aclose", None)
            if close is not None:
                await close()
