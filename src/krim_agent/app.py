"""本地宿主桥组合根；借用运行时，关闭连接不销毁持久化话题。"""

from collections.abc import AsyncGenerator

from pydantic import Field, JsonValue, TypeAdapter

from .app_sdk import AppHandle
from .application.change_lists import ChangeListService
from .application.checkpoints import CheckpointService
from .application.streams import MultiplexReader
from .change_list import (
    ChangeClearRequest,
    ChangeClickRequest,
    ChangeExecution,
    ChangeInvoke,
    ChangeInvokeRequest,
    ChangeItemClick,
    ChangeOperationRequest,
    ChangeReceipt,
    ChangeSync,
    ChangeSyncRequest,
    ChangeTopic,
)
from .checkpoint import (
    CheckpointCanRollback,
    CheckpointCanRollbackRequest,
    CheckpointCheck,
    CheckpointCheckResult,
    CheckpointCreate,
    CheckpointCreateRequest,
    CheckpointExecution,
    CheckpointReceipt,
    CheckpointRollback,
    CheckpointRollbackRequest,
)
from .context_engine import ContextCompactRequest, ContextProposalDecision
from .drag import DragDropRequest, DragEnterRequest, DragPreview, DragState
from .interactions import InteractionReply
from .link import LinkNavigate
from .locale import LocaleChange
from .persistence.attachments import AttachmentRepository
from .persistence.change_lists import ChangeListRepository
from .persistence.checkpoints import CheckpointRepository
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
from .protocol.domain import ChatRequest, EditRequest, RegenerateRequest, ServiceError, TopicCreate
from .protocol.models import StrictModel
from .runtime import ChatRuntime
from .skills import (
    SkillBatch,
    SkillInjectRequest,
    SkillReadRequest,
    SkillRemoveRequest,
    SkillTopicRequest,
)
from .streams import ActiveRunView, MultiplexRequest


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


class ProposalListRequest(TopicRequest):
    pending_only: bool = False


class ProposalRequest(TopicRequest):
    proposal_id: str


class ProposalDecisionRequest(ContextProposalDecision):
    topic_id: str
    proposal_id: str


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
        self.attachment_repository = AttachmentRepository(runtime.repository.database)
        self.handle = AppHandle(host, attachments=self.attachment_repository)
        self.runtime = runtime
        self.change_repository = ChangeListRepository(runtime.repository.database)
        self.changes = ChangeListService(self.change_repository)
        self.checkpoint_repository = CheckpointRepository(runtime.repository.database)
        self.checkpoints = CheckpointService(self.checkpoint_repository)

        async def get_changes(data: JsonValue) -> JsonValue:
            value = await self.change_repository.get(ChangeTopic.model_validate(data).topic_id)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def sync_changes(data: JsonValue) -> JsonValue:
            request = ChangeSyncRequest.model_validate(data)
            value = await self.change_repository.sync(
                request.topic_id,
                ChangeSync.model_validate(request.model_dump(exclude={"topic_id"})),
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def clear_changes(data: JsonValue) -> JsonValue:
            request = ChangeClearRequest.model_validate(data)
            value = await self.change_repository.sync(
                request.topic_id, ChangeSync(revision=request.revision)
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def apply_changes(data: JsonValue) -> JsonValue:
            payload = ChangeInvokeRequest.model_validate(data)
            request = ChangeInvoke.model_validate(payload.model_dump(exclude={"topic_id"}))

            async def execute(execution: ChangeExecution) -> ChangeReceipt:
                result = await self._app.emit(
                    "changeList/invoke",
                    TypeAdapter(JsonValue).validate_python(execution.model_dump(mode="json")),
                    timeout=request.timeout_seconds,
                )
                if not result.ok:
                    raise ServiceError("executor_failed", "宿主执行未返回有效业务回执", 502)
                return ChangeReceipt.model_validate(result.data)

            value = await self.changes.invoke(payload.topic_id, request, execute)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def click_change(data: JsonValue) -> JsonValue:
            request = ChangeClickRequest.model_validate(data)
            current = await self.change_repository.get(request.topic_id)
            if current.revision != request.revision:
                raise ServiceError("change_revision_conflict", "变更清单版本已更新", 409)
            item = next((item for item in current.items if item.id == request.item_id), None)
            if item is None:
                raise ServiceError("change_item_not_found", "变更条目不在该话题中", 404)
            click = ChangeItemClick(topic_id=request.topic_id, revision=current.revision, item=item)
            value: JsonValue = TypeAdapter(JsonValue).validate_python(click.model_dump(mode="json"))
            self._app.notify("changeList/item/click", value)
            return value

        async def create_topic(data: JsonValue) -> JsonValue:
            payload = TopicCreate.model_validate(data)
            topic = await runtime.repository.create_topic(payload.title, payload.locale)
            return TypeAdapter(JsonValue).validate_json(topic.model_dump_json())

        async def get_change_operation(data: JsonValue) -> JsonValue:
            request = ChangeOperationRequest.model_validate(data)
            operation = await self.change_repository.operation(
                request.topic_id, request.operation_id
            )
            return TypeAdapter(JsonValue).validate_python(operation.model_dump(mode="json"))

        async def cancel_change(data: JsonValue) -> JsonValue:
            request = ChangeOperationRequest.model_validate(data)
            operation = await self.changes.cancel(request.topic_id, request.operation_id)
            return TypeAdapter(JsonValue).validate_python(operation.model_dump(mode="json"))

        async def resolve_change(data: JsonValue) -> JsonValue:
            receipt = ChangeReceipt.model_validate(data)
            operation = await self.change_repository.settle(receipt, resolve=True)
            return TypeAdapter(JsonValue).validate_python(operation.model_dump(mode="json"))

        async def create_checkpoint(data: JsonValue) -> JsonValue:
            request = CheckpointCreateRequest.model_validate(data)

            async def execute(execution: CheckpointExecution) -> CheckpointReceipt:
                result = await self._app.emit(
                    "checkpoint/create",
                    TypeAdapter(JsonValue).validate_python(execution.model_dump(mode="json")),
                    timeout=request.timeout_seconds,
                )
                if not result.ok:
                    raise ServiceError("executor_failed", "宿主未返回 Checkpoint 创建回执", 502)
                return CheckpointReceipt.model_validate(result.data)

            operation = await self.checkpoints.create(
                request.topic_id,
                CheckpointCreate.model_validate(request.model_dump(exclude={"topic_id"})),
                execute,
            )
            return TypeAdapter(JsonValue).validate_python(operation.model_dump(mode="json"))

        async def list_checkpoints(data: JsonValue) -> JsonValue:
            request = TopicRequest.model_validate(data)
            catalog = await self.checkpoint_repository.catalog(request.topic_id)
            return TypeAdapter(JsonValue).validate_python(catalog.model_dump(mode="json"))

        async def rollback_checkpoint(data: JsonValue) -> JsonValue:
            request = CheckpointRollbackRequest.model_validate(data)

            async def execute(execution: CheckpointExecution) -> CheckpointReceipt:
                result = await self._app.emit(
                    "checkpoint/rollback",
                    TypeAdapter(JsonValue).validate_python(execution.model_dump(mode="json")),
                    timeout=request.timeout_seconds,
                )
                if not result.ok:
                    raise ServiceError("executor_failed", "宿主未返回 Checkpoint 回滚回执", 502)
                return CheckpointReceipt.model_validate(result.data)

            operation = await self.checkpoints.rollback(
                request.topic_id,
                CheckpointRollback.model_validate(request.model_dump(exclude={"topic_id"})),
                execute,
            )
            return TypeAdapter(JsonValue).validate_python(operation.model_dump(mode="json"))

        async def can_rollback_checkpoint(data: JsonValue) -> JsonValue:
            request = CheckpointCanRollbackRequest.model_validate(data)

            async def execute(check: CheckpointCheck) -> CheckpointCheckResult:
                result = await self._app.emit(
                    "checkpoint/canRollback",
                    TypeAdapter(JsonValue).validate_python(check.model_dump(mode="json")),
                    timeout=request.timeout_seconds,
                )
                if not result.ok:
                    raise ServiceError("executor_failed", "宿主未返回 Checkpoint 校验回执", 502)
                return CheckpointCheckResult.model_validate(result.data)

            value = await self.checkpoints.can_rollback(
                request.topic_id,
                CheckpointCanRollback.model_validate(request.model_dump(exclude={"topic_id"})),
                execute,
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

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

        async def list_proposals(data: JsonValue) -> JsonValue:
            request = ProposalListRequest.model_validate(data)
            values = await runtime.context_proposals(
                request.topic_id, pending_only=request.pending_only
            )
            return TypeAdapter(JsonValue).validate_python(
                [value.model_dump(mode="json") for value in values]
            )

        async def get_proposal(data: JsonValue) -> JsonValue:
            request = ProposalRequest.model_validate(data)
            value = await runtime.context_proposal(request.topic_id, request.proposal_id)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def decide_proposal(data: JsonValue) -> JsonValue:
            request = ProposalDecisionRequest.model_validate(data)
            value = await runtime.decide_context_proposal(
                request.topic_id,
                request.proposal_id,
                ContextProposalDecision(action=request.action, revision=request.revision),
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

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

        async def active_runs(_data: JsonValue) -> JsonValue:
            values = [
                ActiveRunView(run=run, events=await runtime.repository.event_summary(run.id))
                for run in await runtime.repository.active_runs()
            ]
            return TypeAdapter(JsonValue).validate_python(
                [value.model_dump(mode="json") for value in values]
            )

        async def multiplex(data: JsonValue) -> AsyncGenerator[JsonValue, None]:
            reader = MultiplexReader(runtime)
            prepared = await reader.prepare(MultiplexRequest.model_validate(data))
            async for item in reader.subscribe(prepared):
                if item is not None:
                    yield TypeAdapter(JsonValue).validate_python(item.model_dump(mode="json"))

        async def get_context(data: JsonValue) -> JsonValue:
            payload = PromptContextRequest.model_validate(data)
            value = await runtime.repository.get_prompt_context(payload.topic_id)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def list_skills(data: JsonValue) -> JsonValue:
            payload = SkillTopicRequest.model_validate(data)
            value = await runtime.repository.skill_catalog(payload.topic_id)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def inject_skills(data: JsonValue) -> JsonValue:
            payload = SkillInjectRequest.model_validate(data)
            value = await runtime.repository.inject_skills(
                payload.topic_id, SkillBatch(skills=payload.skills)
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def remove_skill(data: JsonValue) -> JsonValue:
            payload = SkillRemoveRequest.model_validate(data)
            value = await runtime.repository.remove_skill(payload.topic_id, payload.name)
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def read_skill(data: JsonValue) -> JsonValue:
            payload = SkillReadRequest.model_validate(data)
            value = await runtime.repository.read_skill(
                payload.topic_id, payload.name, payload.resource
            )
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

        async def drag_enter(data: JsonValue) -> JsonValue:
            payload = DragEnterRequest.model_validate(data)
            await runtime.repository.get_topic(payload.topic_id)
            value = DragState(
                active=True,
                preview=DragPreview.model_validate(payload.model_dump(exclude={"topic_id"})),
            )
            return TypeAdapter(JsonValue).validate_python(value.model_dump(mode="json"))

        async def drag_leave(data: JsonValue) -> JsonValue:
            payload = TopicRequest.model_validate(data)
            await runtime.repository.get_topic(payload.topic_id)
            return TypeAdapter(JsonValue).validate_python(
                DragState(active=False).model_dump(mode="json")
            )

        async def drag_drop(data: JsonValue) -> JsonValue:
            payload = DragDropRequest.model_validate(data)
            value = await runtime.repository.add_mention(
                payload.topic_id,
                MentionInput.model_validate(payload.model_dump(exclude={"topic_id"})),
            )
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
        self._app.on("changeList/get", get_changes)
        self._app.on("changeList/sync", sync_changes)
        self._app.on("changeList/clear", clear_changes)
        self._app.on("changeList/apply", apply_changes)
        self._app.on("changeList/click", click_change)
        self._app.on("changeList/operation", get_change_operation)
        self._app.on("changeList/cancel", cancel_change)
        self._app.on("changeList/resolve", resolve_change)
        self._app.on("checkpoint/create", create_checkpoint)
        self._app.on("checkpoint/list", list_checkpoints)
        self._app.on("checkpoint/rollback", rollback_checkpoint)
        self._app.on("checkpoint/canRollback", can_rollback_checkpoint)
        self._app.on("skill/list", list_skills)
        self._app.on("skill/inject", inject_skills)
        self._app.on("skill/remove", remove_skill)
        self._app.on("skill/read", read_skill)
        self._app.on("locale/change", locale_change)
        self._app.on("chat/send", send)
        self._app.on("chat/stop", stop)
        self._app.on("interaction/list", interactions)
        self._app.on("interaction/respond", respond)
        self._app.on("chat/tree", tree)
        self._app.on("chat/compact", compact)
        self._app.on("context/proposals", list_proposals)
        self._app.on("context/proposal", get_proposal)
        self._app.on("context/decision", decide_proposal)
        self._app.on("chat/messages", messages)
        self._app.on("chat/branch", branch)
        self._app.on("chat/edit", edit)
        self._app.on("chat/regenerate", regenerate)
        self._app.on_stream("chat/stream", events)
        self._app.on("chat/active-runs", active_runs)
        self._app.on_stream("chat/multiplex", multiplex)
        self._app.on("mention/get", get_context)
        self._app.on("mention/add", add_mention)
        self._app.on("drag/enter", drag_enter)
        self._app.on("drag/leave", drag_leave)
        self._app.on("drag/drop", drag_drop)
        self._app.on("mention/remove", remove_mention)
        self._app.on("mention/clear", clear_mentions)
        self._app.on("context/getTags", get_context)
        self._app.on("context/addTag", add_tag)
        self._app.on("context/removeTag", remove_tag)
        self._app.on("context/clearTags", clear_tags)

    def navigate_link(self, navigation: LinkNavigate) -> None:
        checked = LinkNavigate.model_validate(navigation.model_dump(mode="json"))
        self._app.notify(
            "link/navigate", TypeAdapter(JsonValue).validate_python(checked.model_dump(mode="json"))
        )

    async def close(self) -> None:
        await self.changes.close()
        await self.checkpoints.close()
        await self.handle.destroy()
        await self._app.close()
