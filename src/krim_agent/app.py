"""本地宿主桥组合根；借用运行时，关闭连接不销毁持久化话题。"""

from collections.abc import AsyncGenerator

from pydantic import JsonValue, TypeAdapter

from .app_sdk import AppHandle
from .protocol import InProcessBridge
from .protocol.domain import ChatRequest, EditRequest, RegenerateRequest, TopicCreate
from .protocol.models import StrictModel
from .runtime import ChatRuntime


class SendRequest(ChatRequest):
    topic_id: str


class RunRequest(StrictModel):
    run_id: str


class TopicRequest(StrictModel):
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
            topic = await runtime.repository.create_topic(payload.title)
            return TypeAdapter(JsonValue).validate_json(topic.model_dump_json())

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

        async def events(data: JsonValue) -> AsyncGenerator[JsonValue, None]:
            run_id = RunRequest.model_validate(data).run_id
            state = await runtime.stream_state(run_id, 0)
            async for frame in runtime.subscribe(state):
                if frame is not None:
                    yield TypeAdapter(JsonValue).validate_json(frame.model_dump_json())

        self._app.on("topic/create", create_topic)
        self._app.on("chat/send", send)
        self._app.on("chat/stop", stop)
        self._app.on("chat/tree", tree)
        self._app.on("chat/messages", messages)
        self._app.on("chat/branch", branch)
        self._app.on("chat/edit", edit)
        self._app.on("chat/regenerate", regenerate)
        self._app.on_stream("chat/stream", events)

    async def close(self) -> None:
        await self.handle.destroy()
        await self._app.close()
