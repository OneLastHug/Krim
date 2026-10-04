"""运行时无关的语义化宿主 API，只依赖内部协议。"""

import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable

from pydantic import JsonValue, TypeAdapter

from krim_agent.context_engine import ContextCompactRequest, ContextCompactResponse
from krim_agent.interactions import InteractionReply, InteractionView
from krim_agent.link import LinkNavigate
from krim_agent.locale import LocaleChange, LocaleView
from krim_agent.prompt_context import ContextTagInput, MentionInput, MentionRef, PromptContext
from krim_agent.protocol import ChannelResult, InProcessBridge, StreamFrame
from krim_agent.protocol.bridge import Handler
from krim_agent.protocol.domain import (
    ChatRequest,
    EditRequest,
    MessageTreeView,
    MessageView,
    RegenerateRequest,
    RunView,
    ServiceError,
    TopicView,
)
from krim_agent.skills import SkillBatch, SkillCatalogView, SkillReadInput, SkillReadResult


class AppHandle:
    def __init__(self, bridge: InProcessBridge) -> None:
        self._bridge = bridge

    @property
    def ready(self) -> bool:
        return self._bridge.ready

    def on(self, channel: str, handler: Handler) -> Callable[[], None]:
        return self._bridge.on(channel, handler)

    def on_link(
        self, handler: Callable[[LinkNavigate], object | Awaitable[object]]
    ) -> Callable[[], None]:
        async def consume(data: JsonValue) -> JsonValue:
            value = handler(LinkNavigate.model_validate(data))
            if inspect.isawaitable(value):
                await value
            return None

        return self.on("link/navigate", consume)

    async def emit(self, channel: str, data: JsonValue = None) -> ChannelResult:
        return await self._bridge.emit(channel, data)

    async def _call(self, channel: str, data: JsonValue) -> JsonValue:
        result = await self.emit(channel, data)
        if not result.ok:
            raise ServiceError("channel_error", result.error or "宿主调用失败")
        return result.data

    async def create_topic(self, title: str = "新话题", *, locale: str = "zh-CN") -> TopicView:
        return TopicView.model_validate(
            await self._call("topic/create", {"title": title, "locale": locale})
        )

    async def set_topic_locale(self, topic_id: str, locale: str) -> LocaleView:
        value = await self._call(
            "locale/change",
            {"topic_id": topic_id, **LocaleChange(locale=locale).model_dump(mode="json")},
        )
        return LocaleView.model_validate(value)

    async def prompt_context(self, topic_id: str) -> PromptContext:
        return PromptContext.model_validate(await self._call("mention/get", {"topic_id": topic_id}))

    async def skills(self, topic_id: str) -> SkillCatalogView:
        return SkillCatalogView.model_validate(
            await self._call("skill/list", {"topic_id": topic_id})
        )

    async def inject_skills(self, topic_id: str, request: SkillBatch) -> SkillCatalogView:
        return SkillCatalogView.model_validate(
            await self._call(
                "skill/inject", {"topic_id": topic_id, **request.model_dump(mode="json")}
            )
        )

    async def remove_skill(self, topic_id: str, name: str | None = None) -> SkillCatalogView:
        return SkillCatalogView.model_validate(
            await self._call("skill/remove", {"topic_id": topic_id, "name": name})
        )

    async def read_skill(
        self, topic_id: str, name: str, resource: str | None = None
    ) -> SkillReadResult:
        request = SkillReadInput(name=name, resource=resource)
        return SkillReadResult.model_validate(
            await self._call(
                "skill/read", {"topic_id": topic_id, **request.model_dump(mode="json")}
            )
        )

    async def add_mention(self, topic_id: str, request: MentionInput) -> MentionRef:
        return MentionRef.model_validate(
            await self._call(
                "mention/add", {"topic_id": topic_id, **request.model_dump(mode="json")}
            )
        )

    async def remove_mention(self, topic_id: str, mention_id: str) -> PromptContext:
        return PromptContext.model_validate(
            await self._call("mention/remove", {"topic_id": topic_id, "mention_id": mention_id})
        )

    async def clear_mentions(self, topic_id: str) -> PromptContext:
        return PromptContext.model_validate(
            await self._call("mention/clear", {"topic_id": topic_id})
        )

    async def add_context_tag(self, topic_id: str, request: ContextTagInput) -> PromptContext:
        return PromptContext.model_validate(
            await self._call(
                "context/addTag", {"topic_id": topic_id, **request.model_dump(mode="json")}
            )
        )

    async def remove_context_tag(self, topic_id: str, name: str) -> PromptContext:
        return PromptContext.model_validate(
            await self._call("context/removeTag", {"topic_id": topic_id, "name": name})
        )

    async def clear_context_tags(self, topic_id: str) -> PromptContext:
        return PromptContext.model_validate(
            await self._call("context/clearTags", {"topic_id": topic_id})
        )

    async def send(self, topic_id: str, request: ChatRequest) -> RunView:
        data: JsonValue = TypeAdapter(JsonValue).validate_python(
            {"topic_id": topic_id, **request.model_dump(mode="json")}
        )
        return RunView.model_validate(await self._call("chat/send", data))

    async def stop(self, run_id: str) -> RunView:
        return RunView.model_validate(await self._call("chat/stop", {"run_id": run_id}))

    async def interactions(
        self, run_id: str, *, pending_only: bool = False
    ) -> list[InteractionView]:
        return TypeAdapter(list[InteractionView]).validate_python(
            await self._call("interaction/list", {"run_id": run_id, "pending_only": pending_only})
        )

    async def respond_interaction(
        self, run_id: str, interaction_id: str, request: InteractionReply
    ) -> InteractionView:
        return InteractionView.model_validate(
            await self._call(
                "interaction/respond",
                {
                    "run_id": run_id,
                    "interaction_id": interaction_id,
                    **request.model_dump(mode="json"),
                },
            )
        )

    async def tree(self, topic_id: str) -> MessageTreeView:
        return MessageTreeView.model_validate(await self._call("chat/tree", {"topic_id": topic_id}))

    async def compact(
        self, topic_id: str, request: ContextCompactRequest | None = None
    ) -> ContextCompactResponse:
        payload = request or ContextCompactRequest()
        return ContextCompactResponse.model_validate(
            await self._call(
                "chat/compact", {"topic_id": topic_id, **payload.model_dump(mode="json")}
            )
        )

    async def messages(self, topic_id: str) -> list[MessageView]:
        return TypeAdapter(list[MessageView]).validate_python(
            await self._call("chat/messages", {"topic_id": topic_id})
        )

    async def select_branch(self, topic_id: str, message_id: str) -> TopicView:
        return TopicView.model_validate(
            await self._call("chat/branch", {"topic_id": topic_id, "message_id": message_id})
        )

    async def edit(self, topic_id: str, message_id: str, request: EditRequest) -> RunView:
        return RunView.model_validate(
            await self._call(
                "chat/edit",
                {
                    "topic_id": topic_id,
                    "message_id": message_id,
                    **request.model_dump(mode="json"),
                },
            )
        )

    async def regenerate(
        self, topic_id: str, message_id: str, request: RegenerateRequest
    ) -> RunView:
        return RunView.model_validate(
            await self._call(
                "chat/regenerate",
                {
                    "topic_id": topic_id,
                    "message_id": message_id,
                    **request.model_dump(mode="json"),
                },
            )
        )

    async def events(self, run_id: str, from_seq: int = 0) -> AsyncIterator[StreamFrame]:
        async for frame in self._bridge.stream(
            "chat/stream", {"run_id": run_id, "from_seq": from_seq}
        ):
            if frame.kind == "error":
                raise ServiceError("stream_error", "宿主流处理失败")
            if frame.kind == "chunk":
                yield StreamFrame.model_validate_json(json.dumps(frame.data, ensure_ascii=False))

    async def destroy(self) -> None:
        await self._bridge.close()
