"""本地宿主桥组合根；借用运行时，关闭连接不销毁持久化话题。"""

from collections.abc import AsyncGenerator

from pydantic import Field, JsonValue, TypeAdapter

from .app_sdk import AppHandle
from .context_engine import ContextCompactRequest
from .interactions import InteractionReply
from .locale import LocaleChange
from .prompt_context import (
    ContextTagAddRequest,
    ContextTagInput,
    ContextTagRemoveRequest,
    MentionAddRequest,
    MentionInput,
    MentionRemoveRequest,
    PromptContextRequest,
)
from .protocol import InProcessBridge
from .protocol.domain import ChatRequest, EditRequest, RegenerateRequest, TopicCreate
from .protocol.models import StrictModel
from .runtime import ChatRuntime


class SendRequest(ChatRequest):
    topic_id: str


class CompactTopicRequest(ContextCompactRequest):
    topic_id: str


class RunRequest(StrictModel):
    run_id: str


class InteractionListRequest(RunRequest):
    pending_only: bool = False


class InteractionReplyRequest(InteractionReply):
    run_id: str
    interaction_id: str


class StreamRequest(RunRequest):
    from_seq: int = Field(default=0, ge=0)


class TopicRequest(StrictModel):
    topic_id: str


class LocaleTopicRequest(LocaleChange):
    topic_id: str


class BranchSelection(TopicRequest):
    message_id: str


class EditMessageRequest(EditRequest):
    topic_id: str
    message_id: str


class RegenerateMessageRequest(RegenerateRequest):
    topic_id: str
    message_id: str


class LocalApp:
    def __init__(self, runtime: ChatRuntime) -> None:
        host, self._app = InProcessBridge.pair()
        self.handle = AppHandle(host)
        self.runtime = runtime

        async def create_topic(data: JsonValue) -> JsonValue:
            payload = TopicCreate.model_validate(data)
            topic = await runtime.repository.create_topic(payload.title, payload.locale)
            return TypeAdapter(JsonValue).validate_json(topic.model_dump_json())

        async def locale_change(data: JsonValue) -> JsonValue:
            payload = LocaleTopicRequest.model_validate(data)
            value = await runtime.repository.set_topic_locale(payload.topic_id, payload.locale)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def send(data: JsonValue) -> JsonValue:
            payload = SendRequest.model_validate(data)
            run = await runtime.start(
                payload.topic_id,
                ChatRequest.model_validate(payload.model_dump(exclude={"topic_id"})),
            )
            return TypeAdapter(JsonValue).validate_json(run.model_dump_json())

        async def tree(data: JsonValue) -> JsonValue:
            value = await runtime.repository.message_tree(
                TopicRequest.model_validate(data).topic_id
            )
            return TypeAdapter(JsonValue).validate_json(value.model_dump_json())

        async def compact(data: JsonValue) -> JsonValue:
            payload = CompactTopicRequest.model_validate(data)
            value = await runtime.compact(
                payload.topic_id,
                ContextCompactRequest.model_validate(payload.model_dump(exclude={"topic_id"})),
            )
            return TypeAdapter(JsonValue).validate_json(value.model_dump_json())

        async def messages(data: JsonValue) -> JsonValue:
            history = await runtime.repository.list_messages(
                TopicRequest.model_validate(data).topic_id
            )
            return [
                TypeAdapter(JsonValue).validate_json(message.model_dump_json())
                for message in history
            ]

        async def branch(data: JsonValue) -> JsonValue:
            payload = BranchSelection.model_validate(data)
            value = await runtime.repository.select_branch(payload.topic_id, payload.message_id)
            return TypeAdapter(JsonValue).validate_json(value.model_dump_json())

        async def edit(data: JsonValue) -> JsonValue:
            payload = EditMessageRequest.model_validate(data)
            value = await runtime.edit(
                payload.topic_id,
                payload.message_id,
                EditRequest.model_validate(payload.model_dump(exclude={"topic_id", "message_id"})),
            )
            return TypeAdapter(JsonValue).validate_json(value.model_dump_json())

        async def regenerate(data: JsonValue) -> JsonValue:
            payload = RegenerateMessageRequest.model_validate(data)
            value = await runtime.regenerate(
                payload.topic_id,
                payload.message_id,
                RegenerateRequest(idempotency_key=payload.idempotency_key),
            )
            return TypeAdapter(JsonValue).validate_json(value.model_dump_json())

        async def stop(data: JsonValue) -> JsonValue:
            run = await runtime.stop(RunRequest.model_validate(data).run_id)
            return TypeAdapter(JsonValue).validate_json(run.model_dump_json())

        async def interactions(data: JsonValue) -> JsonValue:
            request = InteractionListRequest.model_validate(data)
            values = await runtime.repository.list_interactions(
                request.run_id, pending_only=request.pending_only
            )
            return TypeAdapter(JsonValue).validate_python(
                [value.model_dump(mode="json") for value in values]
            )

        async def respond(data: JsonValue) -> JsonValue:
            request = InteractionReplyRequest.model_validate(data)
            value = await runtime.interactions.reply(
                request.run_id,
                request.interaction_id,
                InteractionReply.model_validate(
                    request.model_dump(exclude={"run_id", "interaction_id"})
                ),
            )
            return TypeAdapter(JsonValue).validate_json(value.model_dump_json())

        async def events(data: JsonValue) -> AsyncGenerator[JsonValue, None]:
            request = StreamRequest.model_validate(data)
            state = await runtime.stream_state(request.run_id, request.from_seq)
            async for frame in runtime.subscribe(state, request.from_seq):
                if frame is not None:
                    yield TypeAdapter(JsonValue).validate_json(frame.model_dump_json())

        async def get_context(data: JsonValue) -> JsonValue:
            payload = PromptContextRequest.model_validate(data)
            value = await runtime.repository.get_prompt_context(payload.topic_id)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def add_mention(data: JsonValue) -> JsonValue:
            payload = MentionAddRequest.model_validate(data)
            value = await runtime.repository.add_mention(
                payload.topic_id,
                MentionInput.model_validate(payload.model_dump(exclude={"topic_id"})),
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def remove_mention(data: JsonValue) -> JsonValue:
            payload = MentionRemoveRequest.model_validate(data)
            value = await runtime.repository.remove_mention(payload.topic_id, payload.mention_id)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def clear_mentions(data: JsonValue) -> JsonValue:
            value = await runtime.repository.remove_mention(
                PromptContextRequest.model_validate(data).topic_id
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def add_tag(data: JsonValue) -> JsonValue:
            payload = ContextTagAddRequest.model_validate(data)
            value = await runtime.repository.add_context_tag(
                payload.topic_id,
                ContextTagInput.model_validate(payload.model_dump(exclude={"topic_id"})),
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def remove_tag(data: JsonValue) -> JsonValue:
            payload = ContextTagRemoveRequest.model_validate(data)
            value = await runtime.repository.remove_context_tag(payload.topic_id, payload.name)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def clear_tags(data: JsonValue) -> JsonValue:
            value = await runtime.repository.remove_context_tag(
                PromptContextRequest.model_validate(data).topic_id
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        self._app.on("topic/create", create_topic)
        self._app.on("locale/change", locale_change)
        self._app.on("chat/send", send)
        self._app.on("chat/stop", stop)
        self._app.on("interaction/list", interactions)
        self._app.on("interaction/respond", respond)
        self._app.on("chat/tree", tree)
        self._app.on("chat/compact", compact)
        self._app.on("chat/messages", messages)
        self._app.on("chat/branch", branch)
        self._app.on("chat/edit", edit)
        self._app.on("chat/regenerate", regenerate)
        self._app.on_stream("chat/stream", events)
        self._app.on("mention/get", get_context)
        self._app.on("mention/add", add_mention)
        self._app.on("mention/remove", remove_mention)
        self._app.on("mention/clear", clear_mentions)
        self._app.on("context/getTags", get_context)
        self._app.on("context/addTag", add_tag)
        self._app.on("context/removeTag", remove_tag)
        self._app.on("context/clearTags", clear_tags)

    async def close(self) -> None:
        await self.handle.destroy()
        await self._app.close()
