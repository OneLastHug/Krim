"""运行时无关的语义化宿主 API，只依赖内部协议。"""

import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable

from pydantic import JsonValue, TypeAdapter

from krim_agent.attachment import (
    AttachmentAccess,
    AttachmentContent,
    AttachmentUpload,
    AttachmentView,
)
from krim_agent.change_list import (
    ChangeExecution,
    ChangeInvoke,
    ChangeItemClick,
    ChangeListView,
    ChangeOperation,
    ChangeReceipt,
    ChangeSync,
)
from krim_agent.checkpoint import (
    CheckpointCanRollback,
    CheckpointCatalog,
    CheckpointCheck,
    CheckpointCheckResult,
    CheckpointCreate,
    CheckpointExecution,
    CheckpointOperation,
    CheckpointReceipt,
    CheckpointRollback,
)
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextCompactResponse,
    ContextProposalDecision,
    ContextProposalView,
)
from krim_agent.drag import DragPreview, DragState
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
from krim_agent.streams import ActiveRunView, MultiplexRequest, RunStreamFrame


class AppHandle:
    def __init__(
        self, bridge: InProcessBridge, *, attachments: AttachmentAccess | None = None
    ) -> None:
        self._bridge = bridge
        self._attachments = attachments

    def _attachment_access(self) -> AttachmentAccess:
        if not self.ready or self._attachments is None:
            raise ServiceError("attachment_unavailable", "宿主没有就绪的附件数据访问", 409)
        return self._attachments

    async def attachments(self, topic_id: str) -> list[AttachmentView]:
        return await self._attachment_access().list(topic_id)

    async def upload_attachment(self, topic_id: str, request: AttachmentUpload) -> AttachmentView:
        return await self._attachment_access().upload(topic_id, request)

    async def attachment_content(self, topic_id: str, attachment_id: str) -> AttachmentContent:
        return await self._attachment_access().content(topic_id, attachment_id)

    async def delete_attachment(self, topic_id: str, attachment_id: str) -> None:
        await self._attachment_access().delete(topic_id, attachment_id)

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

    def on_change_invoke(
        self, handler: Callable[[ChangeExecution], Awaitable[ChangeReceipt]]
    ) -> Callable[[], None]:
        async def consume(data: JsonValue) -> JsonValue:
            receipt = await handler(ChangeExecution.model_validate(data))
            return TypeAdapter(JsonValue).validate_python(receipt.model_dump(mode="json"))

        return self.on("changeList/invoke", consume)

    def on_change_click(
        self, handler: Callable[[ChangeItemClick], object | Awaitable[object]]
    ) -> Callable[[], None]:
        async def consume(data: JsonValue) -> JsonValue:
            value = handler(ChangeItemClick.model_validate(data))
            if inspect.isawaitable(value):
                await value
            return None

        return self.on("changeList/item/click", consume)

    def on_checkpoint_create(
        self, handler: Callable[[CheckpointExecution], Awaitable[CheckpointReceipt]]
    ) -> Callable[[], None]:
        return self._checkpoint_handler("checkpoint/create", handler)

    def on_checkpoint_rollback(
        self, handler: Callable[[CheckpointExecution], Awaitable[CheckpointReceipt]]
    ) -> Callable[[], None]:
        return self._checkpoint_handler("checkpoint/rollback", handler)

    def _checkpoint_handler(
        self, channel: str, handler: Callable[[CheckpointExecution], Awaitable[CheckpointReceipt]]
    ) -> Callable[[], None]:
        async def consume(data: JsonValue) -> JsonValue:
            receipt = await handler(CheckpointExecution.model_validate(data))
            return TypeAdapter(JsonValue).validate_python(receipt.model_dump(mode="json"))

        return self.on(channel, consume)

    def on_checkpoint_can_rollback(
        self, handler: Callable[[CheckpointCheck], Awaitable[CheckpointCheckResult]]
    ) -> Callable[[], None]:
        async def consume(data: JsonValue) -> JsonValue:
            result = await handler(CheckpointCheck.model_validate(data))
            return TypeAdapter(JsonValue).validate_python(result.model_dump(mode="json"))

        return self.on("checkpoint/canRollback", consume)

    async def create_checkpoint(
        self, topic_id: str, request: CheckpointCreate
    ) -> CheckpointOperation:
        value = await self._checkpoint_call(
            "checkpoint/create",
            {"topic_id": topic_id, **request.model_dump(mode="json")},
            request.timeout_seconds,
        )
        return CheckpointOperation.model_validate_json(json.dumps(value, ensure_ascii=False))

    async def checkpoints(self, topic_id: str) -> CheckpointCatalog:
        value = await self._call("checkpoint/list", {"topic_id": topic_id})
        return CheckpointCatalog.model_validate_json(json.dumps(value, ensure_ascii=False))

    async def rollback_checkpoint(
        self, topic_id: str, request: CheckpointRollback
    ) -> CheckpointOperation:
        value = await self._checkpoint_call(
            "checkpoint/rollback",
            {"topic_id": topic_id, **request.model_dump(mode="json")},
            request.timeout_seconds,
        )
        return CheckpointOperation.model_validate_json(json.dumps(value, ensure_ascii=False))

    async def can_rollback_checkpoint(
        self, topic_id: str, request: CheckpointCanRollback
    ) -> CheckpointCheckResult:
        value = await self._checkpoint_call(
            "checkpoint/canRollback",
            {"topic_id": topic_id, **request.model_dump(mode="json")},
            request.timeout_seconds,
        )
        return CheckpointCheckResult.model_validate_json(json.dumps(value, ensure_ascii=False))

    async def _checkpoint_call(
        self, channel: str, data: JsonValue, timeout_seconds: float
    ) -> JsonValue:
        result = await self._bridge.emit(channel, data, timeout=timeout_seconds + 2)
        if not result.ok:
            raise ServiceError("channel_error", result.error or "Checkpoint 宿主调用失败")
        return result.data

    async def change_list(self, topic_id: str) -> ChangeListView:
        return ChangeListView.model_validate(
            await self._call("changeList/get", {"topic_id": topic_id})
        )

    async def sync_changes(self, topic_id: str, request: ChangeSync) -> ChangeListView:
        return ChangeListView.model_validate(
            await self._call(
                "changeList/sync", {"topic_id": topic_id, **request.model_dump(mode="json")}
            )
        )

    async def clear_changes(self, topic_id: str, revision: int) -> ChangeListView:
        return ChangeListView.model_validate(
            await self._call("changeList/clear", {"topic_id": topic_id, "revision": revision})
        )

    async def invoke_change(self, topic_id: str, request: ChangeInvoke) -> ChangeOperation:
        result = await self._bridge.emit(
            "changeList/apply",
            {"topic_id": topic_id, **request.model_dump(mode="json")},
            timeout=request.timeout_seconds + 2,
        )
        if not result.ok:
            raise ServiceError("channel_error", result.error or "变更调用失败")
        return ChangeOperation.model_validate_json(json.dumps(result.data, ensure_ascii=False))

    async def click_change(self, topic_id: str, revision: int, item_id: str) -> ChangeItemClick:
        return ChangeItemClick.model_validate(
            await self._call(
                "changeList/click", {"topic_id": topic_id, "revision": revision, "item_id": item_id}
            )
        )

    async def emit(self, channel: str, data: JsonValue = None) -> ChannelResult:
        return await self._bridge.emit(channel, data)

    async def change_operation(self, topic_id: str, operation_id: str) -> ChangeOperation:
        value = await self._call(
            "changeList/operation", {"topic_id": topic_id, "operation_id": operation_id}
        )
        return ChangeOperation.model_validate_json(json.dumps(value, ensure_ascii=False))

    async def cancel_change(self, topic_id: str, operation_id: str) -> ChangeOperation:
        value = await self._call(
            "changeList/cancel", {"topic_id": topic_id, "operation_id": operation_id}
        )
        return ChangeOperation.model_validate_json(json.dumps(value, ensure_ascii=False))

    async def resolve_change(self, receipt: ChangeReceipt) -> ChangeOperation:
        value = await self._call("changeList/resolve", receipt.model_dump(mode="json"))
        return ChangeOperation.model_validate_json(json.dumps(value, ensure_ascii=False))

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

    async def drag_enter(self, topic_id: str, preview: DragPreview) -> DragState:
        return DragState.model_validate(
            await self._call(
                "drag/enter", {"topic_id": topic_id, **preview.model_dump(mode="json")}
            )
        )

    async def drag_leave(self, topic_id: str) -> DragState:
        return DragState.model_validate(await self._call("drag/leave", {"topic_id": topic_id}))

    async def drag_drop(self, topic_id: str, request: MentionInput) -> MentionRef:
        return MentionRef.model_validate(
            await self._call("drag/drop", {"topic_id": topic_id, **request.model_dump(mode="json")})
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

    async def context_proposals(
        self, topic_id: str, *, pending_only: bool = False
    ) -> list[ContextProposalView]:
        value = await self._call(
            "context/proposals", {"topic_id": topic_id, "pending_only": pending_only}
        )
        return TypeAdapter(list[ContextProposalView]).validate_json(
            json.dumps(value, ensure_ascii=False)
        )

    async def context_proposal(self, topic_id: str, proposal_id: str) -> ContextProposalView:
        value = await self._call(
            "context/proposal", {"topic_id": topic_id, "proposal_id": proposal_id}
        )
        return ContextProposalView.model_validate_json(json.dumps(value, ensure_ascii=False))

    async def decide_context_proposal(
        self, topic_id: str, proposal_id: str, request: ContextProposalDecision
    ) -> ContextProposalView:
        value = await self._call(
            "context/decision",
            {"topic_id": topic_id, "proposal_id": proposal_id, **request.model_dump(mode="json")},
        )
        return ContextProposalView.model_validate_json(json.dumps(value, ensure_ascii=False))

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

    async def active_runs(self) -> list[ActiveRunView]:
        value = await self._call("chat/active-runs", None)
        return TypeAdapter(list[ActiveRunView]).validate_json(json.dumps(value, ensure_ascii=False))

    async def multiplex_events(self, request: MultiplexRequest) -> AsyncIterator[RunStreamFrame]:
        async for frame in self._bridge.stream("chat/multiplex", request.model_dump(mode="json")):
            if frame.kind == "error":
                raise ServiceError("stream_error", "宿主多路流读取失败")
            if frame.kind == "chunk":
                yield RunStreamFrame.model_validate_json(json.dumps(frame.data, ensure_ascii=False))
