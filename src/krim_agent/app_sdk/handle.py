"""运行时无关的语义化宿主 API，只依赖内部协议。"""

import json
from collections.abc import AsyncIterator, Callable

from pydantic import JsonValue, TypeAdapter

from krim_agent.protocol import ChannelResult, InProcessBridge, StreamFrame
from krim_agent.protocol.bridge import Handler
from krim_agent.protocol.domain import ChatRequest, RunView, ServiceError, TopicView


class AppHandle:
    def __init__(self, bridge: InProcessBridge) -> None:
        self._bridge = bridge

    @property
    def ready(self) -> bool:
        return self._bridge.ready

    def on(self, channel: str, handler: Handler) -> Callable[[], None]:
        return self._bridge.on(channel, handler)

    async def emit(self, channel: str, data: JsonValue = None) -> ChannelResult:
        return await self._bridge.emit(channel, data)

    async def _call(self, channel: str, data: JsonValue) -> JsonValue:
        result = await self.emit(channel, data)
        if not result.ok:
            raise ServiceError("channel_error", result.error or "宿主调用失败")
        return result.data

    async def create_topic(self, title: str = "新话题") -> TopicView:
        return TopicView.model_validate(await self._call("topic/create", {"title": title}))

    async def send(self, topic_id: str, request: ChatRequest) -> RunView:
        data: JsonValue = TypeAdapter(JsonValue).validate_python(
            {"topic_id": topic_id, **request.model_dump(mode="json")}
        )
        return RunView.model_validate(await self._call("chat/send", data))

    async def stop(self, run_id: str) -> RunView:
        return RunView.model_validate(await self._call("chat/stop", {"run_id": run_id}))

    async def events(self, run_id: str) -> AsyncIterator[StreamFrame]:
        async for frame in self._bridge.stream("chat/stream", {"run_id": run_id}):
            if frame.kind == "error":
                raise ServiceError("stream_error", "宿主流处理失败")
            if frame.kind == "chunk":
                yield StreamFrame.model_validate_json(json.dumps(frame.data, ensure_ascii=False))

    async def destroy(self) -> None:
        await self._bridge.close()
