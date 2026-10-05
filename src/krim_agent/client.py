"""SRC-OSS-012：Python 最小 HTTP/SSE 客户端；只通过公开 API。"""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType
from uuid import uuid4

import httpx
from pydantic import TypeAdapter

from .agents import AgentDescriptor
from .application.agents import AgentPromptRequest, AgentView
from .attachment import AttachmentContent, AttachmentUpload, AttachmentView
from .change_list import (
    ChangeClick,
    ChangeInvoke,
    ChangeItemClick,
    ChangeListView,
    ChangeOperation,
    ChangeReceipt,
    ChangeRevision,
    ChangeSync,
)
from .checkpoint import (
    CheckpointCanRollback,
    CheckpointCatalog,
    CheckpointCheckResult,
    CheckpointCreate,
    CheckpointOperation,
    CheckpointReceipt,
    CheckpointRollback,
)
from .context_engine import ContextCompactRequest, ContextCompactResponse, ContextOptions
from .drag import DragPreview, DragState
from .interactions import InteractionReply, InteractionView
from .locale import LocaleChange, LocaleView
from .prompt_context import (
    ContextTagInput,
    MentionInput,
    MentionOccurrence,
    MentionRef,
    PromptContext,
)
from .prompt_nodes import PromptNode
from .protocol.domain import (
    ChatRequest,
    EditRequest,
    MessageTreeView,
    MessageView,
    RegenerateRequest,
    RunView,
    StreamEventView,
    TopicCreate,
    TopicView,
)
from .protocol.models import StreamFrame
from .skills import SkillBatch, SkillCatalogView, SkillReadInput, SkillReadResult
from .usage import RunUsageView
from .workflow import WorkflowEvent, WorkflowGraph, WorkflowRunView


class ClientError(Exception):
    pass


class KrimClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        api_key: str = "",
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._http = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=60,
            transport=transport,
            trust_env=False,
            follow_redirects=False,
        )

    def __enter__(self) -> KrimClient:
        return self

    def __exit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    @staticmethod
    def _check(response: httpx.Response) -> None:
        if response.is_success:
            return
        raise ClientError(f"HTTP 请求失败: {response.status_code}；请检查服务、权限或运行状态")

    def create_topic(self, title: str = "新话题", *, locale: str = "zh-CN") -> TopicView:
        request = TopicCreate(title=title, locale=locale)
        response = self._http.post("/v1/topics", json=request.model_dump(mode="json"))
        self._check(response)
        return TopicView.model_validate_json(response.content)

    def set_topic_locale(self, topic_id: str, locale: str) -> LocaleView:
        response = self._http.patch(
            f"/v1/topics/{topic_id}/locale",
            json=LocaleChange(locale=locale).model_dump(mode="json"),
        )
        self._check(response)
        return LocaleView.model_validate_json(response.content)

    def topics(self) -> list[TopicView]:
        response = self._http.get("/v1/topics")
        self._check(response)
        return TypeAdapter(list[TopicView]).validate_python(response.json()["topics"])

    def skills(self, topic_id: str) -> SkillCatalogView:
        response = self._http.get(f"/v1/topics/{topic_id}/skills")
        self._check(response)
        return SkillCatalogView.model_validate_json(response.content)

    def attachments(self, topic_id: str) -> list[AttachmentView]:
        response = self._http.get(f"/v1/topics/{topic_id}/attachments")
        self._check(response)
        return TypeAdapter(list[AttachmentView]).validate_json(response.content)

    def upload_attachment(self, topic_id: str, request: AttachmentUpload) -> AttachmentView:
        response = self._http.post(
            f"/v1/topics/{topic_id}/attachments", json=request.model_dump(mode="json")
        )
        self._check(response)
        return AttachmentView.model_validate_json(response.content)

    def attachment_content(self, topic_id: str, attachment_id: str) -> AttachmentContent:
        response = self._http.get(f"/v1/topics/{topic_id}/attachments/{attachment_id}/content")
        self._check(response)
        return AttachmentContent.model_validate_json(response.content)

    def delete_attachment(self, topic_id: str, attachment_id: str) -> None:
        response = self._http.delete(f"/v1/topics/{topic_id}/attachments/{attachment_id}")
        self._check(response)

    def change_list(self, topic_id: str) -> ChangeListView:
        response = self._http.get(f"/v1/topics/{topic_id}/change-list")
        self._check(response)
        return ChangeListView.model_validate_json(response.content)

    def checkpoints(self, topic_id: str) -> CheckpointCatalog:
        response = self._http.get(f"/v1/topics/{topic_id}/checkpoints")
        self._check(response)
        return CheckpointCatalog.model_validate_json(response.content)

    def create_checkpoint(self, topic_id: str, request: CheckpointCreate) -> CheckpointOperation:
        response = self._http.post(
            f"/v1/topics/{topic_id}/checkpoints",
            json=request.model_dump(mode="json"),
            timeout=request.timeout_seconds + 2,
        )
        self._check(response)
        return CheckpointOperation.model_validate_json(response.content)

    def rollback_checkpoint(
        self, topic_id: str, request: CheckpointRollback
    ) -> CheckpointOperation:
        response = self._http.post(
            f"/v1/topics/{topic_id}/checkpoints/{request.checkpoint_id}/rollback",
            json=request.model_dump(mode="json"),
            timeout=request.timeout_seconds + 2,
        )
        self._check(response)
        return CheckpointOperation.model_validate_json(response.content)

    def can_rollback_checkpoint(
        self, topic_id: str, request: CheckpointCanRollback
    ) -> CheckpointCheckResult:
        response = self._http.post(
            f"/v1/topics/{topic_id}/checkpoints/can-rollback",
            json=request.model_dump(mode="json"),
            timeout=request.timeout_seconds + 2,
        )
        self._check(response)
        return CheckpointCheckResult.model_validate_json(response.content)

    def checkpoint_operation(self, topic_id: str, operation_id: str) -> CheckpointOperation:
        response = self._http.get(f"/v1/topics/{topic_id}/checkpoints/operations/{operation_id}")
        self._check(response)
        return CheckpointOperation.model_validate_json(response.content)

    def cancel_checkpoint(self, topic_id: str, operation_id: str) -> CheckpointOperation:
        response = self._http.post(
            f"/v1/topics/{topic_id}/checkpoints/operations/{operation_id}/cancel"
        )
        self._check(response)
        return CheckpointOperation.model_validate_json(response.content)

    def resolve_checkpoint(self, receipt: CheckpointReceipt) -> CheckpointOperation:
        response = self._http.post(
            f"/v1/topics/{receipt.topic_id}/checkpoints/operations/{receipt.operation_id}/resolve",
            json=receipt.model_dump(mode="json"),
        )
        self._check(response)
        return CheckpointOperation.model_validate_json(response.content)

    def sync_changes(self, topic_id: str, request: ChangeSync) -> ChangeListView:
        response = self._http.put(
            f"/v1/topics/{topic_id}/change-list", json=request.model_dump(mode="json")
        )
        self._check(response)
        return ChangeListView.model_validate_json(response.content)

    def clear_changes(self, topic_id: str, revision: int) -> ChangeListView:
        response = self._http.post(
            f"/v1/topics/{topic_id}/change-list/clear",
            json=ChangeRevision(revision=revision).model_dump(mode="json"),
        )
        self._check(response)
        return ChangeListView.model_validate_json(response.content)

    def invoke_change(self, topic_id: str, request: ChangeInvoke) -> ChangeOperation:
        response = self._http.post(
            f"/v1/topics/{topic_id}/change-list/invoke",
            json=request.model_dump(mode="json"),
            timeout=request.timeout_seconds + 2,
        )
        self._check(response)
        return ChangeOperation.model_validate_json(response.content)

    def change_operation(self, topic_id: str, operation_id: str) -> ChangeOperation:
        response = self._http.get(f"/v1/topics/{topic_id}/change-list/operations/{operation_id}")
        self._check(response)
        return ChangeOperation.model_validate_json(response.content)

    def resolve_change(self, receipt: ChangeReceipt) -> ChangeOperation:
        response = self._http.post(
            f"/v1/topics/{receipt.topic_id}/change-list/operations/{receipt.operation_id}/resolve",
            json=receipt.model_dump(mode="json"),
        )
        self._check(response)
        return ChangeOperation.model_validate_json(response.content)

    def cancel_change(self, topic_id: str, operation_id: str) -> ChangeOperation:
        response = self._http.post(
            f"/v1/topics/{topic_id}/change-list/operations/{operation_id}/cancel"
        )
        self._check(response)
        return ChangeOperation.model_validate_json(response.content)

    def click_change(self, topic_id: str, request: ChangeClick) -> ChangeItemClick:
        response = self._http.post(
            f"/v1/topics/{topic_id}/change-list/click", json=request.model_dump(mode="json")
        )
        self._check(response)
        return ChangeItemClick.model_validate_json(response.content)

    def inject_skills(self, topic_id: str, request: SkillBatch) -> SkillCatalogView:
        response = self._http.post(
            f"/v1/topics/{topic_id}/skills", json=request.model_dump(mode="json")
        )
        self._check(response)
        return SkillCatalogView.model_validate_json(response.content)

    def remove_skill(self, topic_id: str, name: str | None = None) -> SkillCatalogView:
        path = f"/v1/topics/{topic_id}/skills" + (f"/{name}" if name else "")
        response = self._http.delete(path)
        self._check(response)
        return SkillCatalogView.model_validate_json(response.content)

    def read_skill(self, topic_id: str, name: str, resource: str | None = None) -> SkillReadResult:
        request = SkillReadInput(name=name, resource=resource)
        response = self._http.get(
            f"/v1/topics/{topic_id}/skills/{request.name}",
            params={"resource": request.resource} if request.resource else {},
        )
        self._check(response)
        return SkillReadResult.model_validate_json(response.content)

    def drag_enter(self, topic_id: str, preview: DragPreview) -> DragState:
        response = self._http.post(
            f"/v1/topics/{topic_id}/drag/enter", json=preview.model_dump(mode="json")
        )
        self._check(response)
        return DragState.model_validate_json(response.content)

    def drag_leave(self, topic_id: str) -> DragState:
        response = self._http.post(f"/v1/topics/{topic_id}/drag/leave")
        self._check(response)
        return DragState.model_validate_json(response.content)

    def drag_drop(self, topic_id: str, request: MentionInput) -> MentionRef:
        response = self._http.post(
            f"/v1/topics/{topic_id}/drag/drop", json=request.model_dump(mode="json")
        )
        self._check(response)
        return MentionRef.model_validate_json(response.content)

    def messages(self, topic_id: str) -> list[MessageView]:
        response = self._http.get(f"/v1/topics/{topic_id}/messages")
        self._check(response)
        return TypeAdapter(list[MessageView]).validate_python(response.json()["messages"])

    def tree(self, topic_id: str) -> MessageTreeView:
        response = self._http.get(f"/v1/topics/{topic_id}/tree")
        self._check(response)
        return MessageTreeView.model_validate_json(response.content)

    def prompt_context(self, topic_id: str) -> PromptContext:
        response = self._http.get(f"/v1/topics/{topic_id}/prompt-context")
        self._check(response)
        return PromptContext.model_validate_json(response.content)

    def add_mention(self, topic_id: str, request: MentionInput) -> MentionRef:
        response = self._http.post(
            f"/v1/topics/{topic_id}/mentions", json=request.model_dump(mode="json")
        )
        self._check(response)
        return MentionRef.model_validate_json(response.content)

    def remove_mention(self, topic_id: str, mention_id: str | None = None) -> PromptContext:
        path = f"/v1/topics/{topic_id}/mentions" + (f"/{mention_id}" if mention_id else "")
        response = self._http.delete(path)
        self._check(response)
        return PromptContext.model_validate_json(response.content)

    def add_context_tag(self, topic_id: str, request: ContextTagInput) -> PromptContext:
        response = self._http.post(
            f"/v1/topics/{topic_id}/context-tags", json=request.model_dump(mode="json")
        )
        self._check(response)
        return PromptContext.model_validate_json(response.content)

    def remove_context_tag(self, topic_id: str, name: str | None = None) -> PromptContext:
        response = self._http.delete(
            f"/v1/topics/{topic_id}/context-tags", params={"name": name} if name is not None else {}
        )
        self._check(response)
        return PromptContext.model_validate_json(response.content)

    def interactions(self, run_id: str, *, pending_only: bool = False) -> list[InteractionView]:
        response = self._http.get(
            f"/v1/runs/{run_id}/interactions", params={"pending_only": str(pending_only).lower()}
        )
        self._check(response)
        return TypeAdapter(list[InteractionView]).validate_json(response.content)

    def respond_interaction(
        self, run_id: str, interaction_id: str, request: InteractionReply
    ) -> InteractionView:
        response = self._http.post(
            f"/v1/runs/{run_id}/interactions/{interaction_id}/respond",
            json=request.model_dump(mode="json", exclude_none=True),
        )
        self._check(response)
        return InteractionView.model_validate_json(response.content)

    def agents_catalog(self) -> list[AgentDescriptor]:
        response = self._http.get("/v1/agents/catalog")
        self._check(response)
        return TypeAdapter(list[AgentDescriptor]).validate_json(response.content)

    def agents(self) -> list[AgentView]:
        response = self._http.get("/v1/agents")
        self._check(response)
        return TypeAdapter(list[AgentView]).validate_json(response.content)

    def agent_spawn(self, name: str, *, persistent: bool = False) -> AgentView:
        response = self._http.post("/v1/agents", json={"name": name, "persistent": persistent})
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent(self, agent_id: str) -> AgentView:
        response = self._http.get(f"/v1/agents/{agent_id}")
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_execute(
        self, agent_id: str, prompt: str, idempotency_key: str | None = None
    ) -> AgentView:
        request = AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        response = self._http.post(
            f"/v1/agents/{agent_id}/execute",
            json=request.model_dump(exclude_none=True),
            timeout=httpx.Timeout(30, read=330),
        )
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_resume(
        self, agent_id: str, prompt: str, idempotency_key: str | None = None
    ) -> AgentView:
        request = AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        response = self._http.post(
            f"/v1/agents/{agent_id}/resume",
            json=request.model_dump(exclude_none=True),
            timeout=httpx.Timeout(30, read=330),
        )
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_stop(self, agent_id: str) -> AgentView:
        response = self._http.post(f"/v1/agents/{agent_id}/stop")
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_destroy(self, agent_id: str) -> AgentView:
        response = self._http.delete(f"/v1/agents/{agent_id}")
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_events(self, agent_id: str, from_seq: int = 0) -> Iterator[StreamFrame]:
        with self._http.stream(
            "GET",
            f"/v1/agents/{agent_id}/events",
            params={"from_seq": from_seq},
            timeout=httpx.Timeout(30, read=45),
        ) as response:
            self._check(response)
            run_id = response.headers.get("x-krim-agent-run-id")
            if not run_id:
                raise ClientError("Agent SSE 缺少运行关联")
            data: list[str] = []
            event_id = ""
            event_kind = ""
            expected = from_seq
            event_bytes = 0
            for line in response.iter_lines():
                if len(line.encode("utf-8")) > 131072:
                    raise ClientError("Agent SSE 行超过大小限制")
                if line.startswith("data:"):
                    event_bytes += len(line.encode("utf-8"))
                    if event_bytes > 131072:
                        raise ClientError("Agent SSE 事件超过大小限制")
                    data.append(line[5:].lstrip(" "))
                elif line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("event:"):
                    event_kind = line[6:].strip()
                elif not line and data:
                    frame = StreamFrame.model_validate_json("\n".join(data))
                    if (
                        frame.stream_id != run_id
                        or frame.seq != expected
                        or event_id != str(frame.seq)
                        or event_kind != frame.kind
                    ):
                        raise ClientError("Agent SSE 帧顺序或关联不正确")
                    yield frame
                    expected += 1
                    data, event_id, event_kind = [], "", ""
                    event_bytes = 0
                    if frame.kind in {"end", "error"}:
                        return
            if expected == from_seq and from_seq > 0:
                summary = self.event_summary(run_id)
                if summary.terminal and from_seq == summary.last_seq + 1:
                    return
            raise ClientError("Agent SSE 未收到终止帧")

    def usage(self, run_id: str) -> RunUsageView:
        response = self._http.get(f"/v1/runs/{run_id}/usage")
        self._check(response)
        return RunUsageView.model_validate_json(response.content)

    def compact(
        self,
        topic_id: str,
        options: ContextOptions | None = None,
        *,
        confirm: bool = False,
        expected_branch_key: str | None = None,
    ) -> ContextCompactResponse:
        values = options.model_dump() if options is not None else {}
        request = ContextCompactRequest(
            **values, confirm=confirm, expected_branch_key=expected_branch_key
        )
        response = self._http.post(
            f"/v1/topics/{topic_id}/compact", json=request.model_dump(mode="json")
        )
        self._check(response)
        return ContextCompactResponse.model_validate_json(response.content)

    def select_branch(self, topic_id: str, message_id: str) -> TopicView:
        response = self._http.put(f"/v1/topics/{topic_id}/branch", json={"message_id": message_id})
        self._check(response)
        return TopicView.model_validate_json(response.content)

    def edit(
        self,
        topic_id: str,
        message_id: str,
        text: str,
        idempotency_key: str | None = None,
        *,
        mentions: list[MentionOccurrence] | None = None,
        skill_names: list[str] | None = None,
        nodes: list[PromptNode] | None = None,
    ) -> RunView:
        request = EditRequest(
            text=text,
            idempotency_key=idempotency_key or str(uuid4()),
            mentions=mentions or [],
            skill_names=skill_names or [],
            nodes=nodes or [],
        )
        response = self._http.post(
            f"/v1/topics/{topic_id}/messages/{message_id}/edit", json=request.model_dump()
        )
        self._check(response)
        return RunView.model_validate_json(response.content)

    def regenerate(
        self, topic_id: str, message_id: str, idempotency_key: str | None = None
    ) -> RunView:
        request = RegenerateRequest(idempotency_key=idempotency_key or str(uuid4()))
        response = self._http.post(
            f"/v1/topics/{topic_id}/messages/{message_id}/regenerate", json=request.model_dump()
        )
        self._check(response)
        return RunView.model_validate_json(response.content)

    def send(
        self,
        topic_id: str,
        text: str,
        idempotency_key: str | None = None,
        *,
        parent_message_id: str | None = None,
        mentions: list[MentionOccurrence] | None = None,
        skill_names: list[str] | None = None,
        nodes: list[PromptNode] | None = None,
    ) -> RunView:
        request = ChatRequest(
            text=text,
            idempotency_key=idempotency_key or str(uuid4()),
            parent_message_id=parent_message_id,
            mentions=mentions or [],
            skill_names=skill_names or [],
            nodes=nodes or [],
        )
        response = self._http.post(f"/v1/topics/{topic_id}/messages", json=request.model_dump())
        self._check(response)
        return RunView.model_validate_json(response.content)

    def run(self, run_id: str) -> RunView:
        response = self._http.get(f"/v1/runs/{run_id}")
        self._check(response)
        return RunView.model_validate_json(response.content)

    def run_messages(self, run_id: str) -> list[MessageView]:
        response = self._http.get(f"/v1/runs/{run_id}/messages")
        self._check(response)
        return TypeAdapter(list[MessageView]).validate_python(response.json()["messages"])

    def stop(self, run_id: str) -> RunView:
        response = self._http.post(f"/v1/runs/{run_id}/stop")
        self._check(response)
        return RunView.model_validate_json(response.content)

    def events(self, run_id: str, from_seq: int = 0) -> Iterator[StreamFrame]:
        with self._http.stream(
            "GET",
            f"/v1/runs/{run_id}/events",
            params={"from_seq": from_seq},
            timeout=httpx.Timeout(30, read=45),
        ) as response:
            self._check(response)
            data: list[str] = []
            event_id = ""
            event_kind = ""
            expected = from_seq
            event_bytes = 0
            for line in response.iter_lines():
                if len(line) > 131072:
                    raise ClientError("SSE 行超过大小限制")
                if line.startswith("data:"):
                    event_bytes += len(line.encode("utf-8"))
                    if event_bytes > 131072:
                        raise ClientError("SSE 事件超过大小限制")
                    data.append(line[5:].lstrip(" "))
                elif line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("event:"):
                    event_kind = line[6:].strip()
                elif not line and data:
                    frame = StreamFrame.model_validate_json("\n".join(data))
                    if (
                        frame.stream_id != run_id
                        or frame.seq != expected
                        or event_id != str(frame.seq)
                        or event_kind != frame.kind
                    ):
                        raise ClientError("SSE 帧关联或顺序不正确")
                    yield frame
                    expected += 1
                    data, event_id, event_kind = [], "", ""
                    event_bytes = 0
                    if frame.kind in {"end", "error"}:
                        return
            if expected == from_seq and from_seq > 0:
                summary = self.event_summary(run_id)
                if summary.terminal and from_seq == summary.last_seq + 1:
                    return
            raise ClientError("SSE 未收到终止帧；运行可能仍在后台执行，请读取运行状态")

    def event_summary(self, run_id: str) -> StreamEventView:
        response = self._http.get(f"/v1/runs/{run_id}/events/summary")
        self._check(response)
        return StreamEventView.model_validate_json(response.content)

    def workflow_run(
        self,
        graph: WorkflowGraph,
        input_value: object = None,
        *,
        production: bool = True,
        pause_before: list[str] | None = None,
        pause_after: list[str] | None = None,
        idempotency_key: str | None = None,
    ) -> WorkflowRunView:
        response = self._http.post(
            "/v1/workflows/runs",
            json={
                "idempotency_key": idempotency_key or str(uuid4()),
                "graph": graph.model_dump(mode="json"),
                "input_value": input_value,
                "production": production,
                "pause_before": pause_before or [],
                "pause_after": pause_after or [],
            },
        )
        self._check(response)
        return WorkflowRunView.model_validate_json(response.content)

    def workflow(self, run_id: str) -> WorkflowRunView:
        response = self._http.get(f"/v1/workflows/runs/{run_id}")
        self._check(response)
        return WorkflowRunView.model_validate_json(response.content)

    def workflow_graph(self, run_id: str) -> WorkflowGraph:
        response = self._http.get(f"/v1/workflows/runs/{run_id}/graph")
        self._check(response)
        return WorkflowGraph.model_validate_json(response.content)

    def workflows(self) -> list[WorkflowRunView]:
        response = self._http.get("/v1/workflows/runs")
        self._check(response)
        return TypeAdapter(list[WorkflowRunView]).validate_json(response.content)

    def workflow_restore(
        self,
        run_id: str,
        *,
        replay_side_effects: bool = False,
        completed_outputs: dict[str, object] | None = None,
    ) -> WorkflowRunView:
        response = self._http.post(
            f"/v1/workflows/runs/{run_id}/restore",
            json={
                "replay_side_effects": replay_side_effects,
                "completed_outputs": completed_outputs or {},
            },
        )
        self._check(response)
        return WorkflowRunView.model_validate_json(response.content)

    def workflow_events(self, run_id: str, from_seq: int = 0) -> Iterator[WorkflowEvent]:
        with self._http.stream(
            "GET",
            f"/v1/workflows/runs/{run_id}/events",
            params={"from_seq": from_seq},
            timeout=httpx.Timeout(30, read=45),
        ) as response:
            self._check(response)
            data: list[str] = []
            event_id = ""
            event_kind = ""
            expected = from_seq
            event_bytes = 0
            for line in response.iter_lines():
                if len(line.encode("utf-8")) > 131072:
                    raise ClientError("Workflow SSE 行超过大小限制")
                if line.startswith("data:"):
                    event_bytes += len(line.encode("utf-8"))
                    if event_bytes > 131072:
                        raise ClientError("Workflow SSE 事件超过大小限制")
                    data.append(line[5:].lstrip(" "))
                elif line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("event:"):
                    event_kind = line[6:].strip()
                elif not line and data:
                    event = WorkflowEvent.model_validate_json("\n".join(data))
                    if (
                        event.run_id != run_id
                        or event.seq != expected
                        or event_id != str(event.seq)
                        or event_kind != event.type
                    ):
                        raise ClientError("Workflow SSE 帧关联或顺序不正确")
                    yield event
                    expected += 1
                    data, event_id, event_kind = [], "", ""
                    event_bytes = 0
                    if event.type == "end":
                        return
            current = self.workflow(run_id)
            if current.status in {"queued", "running"} or current.event_count != expected:
                raise ClientError("Workflow SSE 不完整；请检查运行状态与游标")

    def workflow_pause(self, run_id: str) -> WorkflowRunView:
        response = self._http.post(f"/v1/workflows/runs/{run_id}/pause")
        self._check(response)
        return WorkflowRunView.model_validate_json(response.content)

    def workflow_resume(
        self, run_id: str, input_override: object = None, *, override: bool = False
    ) -> WorkflowRunView:
        response = self._http.post(
            f"/v1/workflows/runs/{run_id}/resume",
            json={"has_input_override": override, "input_override": input_override},
        )
        self._check(response)
        return WorkflowRunView.model_validate_json(response.content)

    def workflow_stop(self, run_id: str) -> WorkflowRunView:
        response = self._http.post(f"/v1/workflows/runs/{run_id}/stop")
        self._check(response)
        return WorkflowRunView.model_validate_json(response.content)

    def workflow_rerun(
        self,
        run_id: str,
        node_id: str,
        input_override: object = None,
        *,
        override: bool = False,
        allow_side_effects: bool = False,
    ) -> WorkflowRunView:
        response = self._http.post(
            f"/v1/workflows/runs/{run_id}/rerun",
            json={
                "node_id": node_id,
                "has_input_override": override,
                "input_override": input_override,
                "allow_side_effects": allow_side_effects,
            },
        )
        self._check(response)
        return WorkflowRunView.model_validate_json(response.content)
