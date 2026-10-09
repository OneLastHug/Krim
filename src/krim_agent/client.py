"""SRC-OSS-012：Python 最小 HTTP/SSE 客户端；只通过公开 API。"""

from __future__ import annotations

import math
import re
from collections.abc import Iterator
from types import TracebackType
from uuid import uuid4

import httpx
from pydantic import TypeAdapter

from krim_agent.agents import AgentProfileUpdate, AgentProfileVersion, AgentResourceView
from krim_agent.agents.sessions import AgentSessionControl, AgentSessionView, AgentTurnView

from .agents import AgentDescriptor
from .application.agents import AgentInventoryView, AgentPromptRequest, AgentView
from .attachment import AttachmentContent, AttachmentUpload, AttachmentView
from .budgets import BudgetAccountView, BudgetRequestView, BudgetResolveRequest
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
from .context_engine import (
    ContextCompactRequest,
    ContextCompactResponse,
    ContextOptions,
    ContextProposalDecision,
    ContextProposalView,
)
from .drag import DragPreview, DragState
from .evaluation.remote import SyncEvalClient
from .execution.agent_models import (
    AgentCommandRequest,
    CommandRead,
    CommandSlice,
    CommandSummary,
)
from .execution.management import CommandAccepted, CommandHistoryPage, CommandHistoryRequest
from .execution.publication import (
    CommandChangesPage,
    CommandChangesRequest,
    CommandProposalRequest,
    CommandProposalView,
)
from .execution.records import CommandView
from .interactions import InteractionReply, InteractionView
from .locale import LocaleChange, LocaleView
from .observability import ObservationStatus, SpanRecord, TraceView
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
from .streams import ActiveRunView, MultiplexRequest, RunStreamFrame
from .task_state import TaskKind, TaskOperationView, TaskSnapshot, TaskWriteRequest
from .tools.workspace import ListFilesInput, ReadFileInput, SearchFilesInput
from .usage import RunUsageView
from .workflow import WorkflowEvent, WorkflowGraph, WorkflowRunView
from .workspace import WorkspaceFile, WorkspaceListing, WorkspaceSearch, WorkspaceView
from .workspace_change import (
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
    WorkspaceOperationProofView,
    WorkspaceOperationRequest,
    WorkspacePreviewRequest,
)
from .workspace_checkpoint import WorkspaceCheckpointCreate, WorkspaceCheckpointView
from .workspace_list import (
    NativeBatchHistoryPage,
    NativeBatchRequest,
    NativeBatchView,
    NativeChangePage,
    NativeListFilter,
)
from .workspace_restore import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)


class ClientError(Exception):
    def __init__(self, message: str, *, code: str | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.code, self.status = code, status


def _workspace_identifier(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,35}", value) is None:
        raise ClientError(
            "工作区Agent/提案/操作标识无效", code="workspace_identifier_invalid", status=422
        )
    return value


def _checkpoint_key(value: str) -> str:
    if not 1 <= len(value) <= 128 or any(ord(char) < 32 for char in value):
        raise ClientError("检查点请求键无效", code="workspace_request_key_invalid", status=422)
    return value


def _checkpoint_wait(seconds: float, code: str) -> float:
    if not math.isfinite(seconds) or not 0 <= seconds <= 300:
        raise ClientError("检查点等待时间须为0至300秒", code=code, status=422)
    return max(60, seconds + 5)


def _command_path(agent_id: str, command_id: str | None = None) -> str:
    for value in (agent_id, command_id):
        if value is not None and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,35}", value) is None:
            raise ClientError(
                "命令或Agent标识无效", code="execution_identifier_invalid", status=422
            )
    return f"/v1/agents/{agent_id}/commands" + (f"/{command_id}" if command_id is not None else "")


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

    def agent_command_start(
        self, agent_id: str, request: AgentCommandRequest, *, confirmed: bool = False
    ) -> CommandAccepted:
        request = AgentCommandRequest.model_validate(request.model_dump())
        response = self._http.post(
            _command_path(agent_id),
            json=request.model_dump(mode="json"),
            params={"confirmed": str(confirmed).lower()},
        )
        self._check(response)
        return CommandAccepted.model_validate_json(response.content)

    def agent_command_history(
        self, agent_id: str, *, after: str | None = None, limit: int = 32, confirmed: bool = False
    ) -> CommandHistoryPage:
        request = CommandHistoryRequest(after=after, limit=limit)
        response = self._http.get(
            _command_path(agent_id),
            params={**request.model_dump(exclude_none=True), "confirmed": str(confirmed).lower()},
        )
        self._check(response)
        return CommandHistoryPage.model_validate_json(response.content)

    def agent_command(
        self, agent_id: str, command_id: str, *, confirmed: bool = False
    ) -> CommandSummary:
        response = self._http.get(
            _command_path(agent_id, command_id), params={"confirmed": str(confirmed).lower()}
        )
        self._check(response)
        return CommandSummary.model_validate_json(response.content)

    def agent_command_by_key(
        self, agent_id: str, request_key: str, *, confirmed: bool = False
    ) -> CommandSummary | None:
        if (
            not 1 <= len(request_key) <= 128
            or re.fullmatch(r"[A-Za-z0-9_.:-]+", request_key) is None
        ):
            raise ClientError("命令请求键无效", code="execution_request_key_invalid", status=422)
        response = self._http.get(
            _command_path(agent_id) + "/by-key",
            params={"request_key": request_key, "confirmed": str(confirmed).lower()},
        )
        self._check(response)
        return TypeAdapter(CommandSummary | None).validate_json(response.content)

    def agent_command_inspect(
        self, agent_id: str, command_id: str, *, confirmed: bool = False
    ) -> CommandView:
        response = self._http.get(
            _command_path(agent_id, command_id) + "/inspect",
            params={"confirmed": str(confirmed).lower()},
        )
        self._check(response)
        return CommandView.model_validate_json(response.content)

    def agent_command_wait(
        self,
        agent_id: str,
        command_id: str,
        *,
        timeout_seconds: float = 30,
        confirmed: bool = False,
    ) -> CommandSummary:
        if not math.isfinite(timeout_seconds) or not 0 <= timeout_seconds <= 300:
            raise ClientError(
                "命令等待时间须为有限的0–300秒", code="execution_wait_timeout", status=422
            )
        response = self._http.post(
            _command_path(agent_id, command_id) + "/wait",
            params={"timeout_seconds": timeout_seconds, "confirmed": str(confirmed).lower()},
            timeout=max(60, timeout_seconds + 5),
        )
        self._check(response)
        return CommandSummary.model_validate_json(response.content)

    def agent_command_cancel(
        self, agent_id: str, command_id: str, *, confirmed: bool = False
    ) -> CommandSummary:
        response = self._http.post(
            _command_path(agent_id, command_id) + "/cancel",
            params={"confirmed": str(confirmed).lower()},
        )
        self._check(response)
        return CommandSummary.model_validate_json(response.content)

    def agent_command_read(
        self,
        agent_id: str,
        command_id: str,
        *,
        source: str = "stdout",
        path: str | None = None,
        offset: int = 0,
        limit: int = 2048,
        confirmed: bool = False,
    ) -> CommandSlice:
        request = CommandRead.model_validate(
            {
                "command_id": command_id,
                "source": source,
                "path": path,
                "offset": offset,
                "limit": limit,
            }
        )
        response = self._http.get(
            _command_path(agent_id, command_id) + "/read",
            params={
                **request.model_dump(exclude={"command_id", "request_key"}, exclude_none=True),
                "confirmed": str(confirmed).lower(),
            },
        )
        self._check(response)
        return CommandSlice.model_validate_json(response.content)

    def agent_command_changes(
        self,
        agent_id: str,
        command_id: str,
        *,
        after: str | None = None,
        limit: int = 32,
        include_unchanged: bool = False,
        confirmed: bool = False,
    ) -> CommandChangesPage:
        request = CommandChangesRequest(
            command_id=command_id, after=after, limit=limit, include_unchanged=include_unchanged
        )
        response = self._http.get(
            _command_path(agent_id, command_id) + "/changes",
            params={
                **request.model_dump(exclude={"command_id", "request_key"}, exclude_none=True),
                "confirmed": str(confirmed).lower(),
            },
        )
        self._check(response)
        return CommandChangesPage.model_validate_json(response.content)

    def agent_command_propose(
        self, agent_id: str, request: CommandProposalRequest, *, confirmed: bool = False
    ) -> CommandProposalView:
        request = CommandProposalRequest.model_validate(request.model_dump())
        response = self._http.post(
            _command_path(agent_id, request.command_id) + "/proposals",
            json=request.model_dump(mode="json"),
            params={"confirmed": str(confirmed).lower()},
        )
        self._check(response)
        return CommandProposalView.model_validate_json(response.content)

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
        code: str | None = None
        try:
            payload = response.json()
            if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
                value = payload["error"].get("code")
                if isinstance(value, str) and len(value) <= 128 and value.isascii():
                    code = value
        except ValueError:
            pass
        raise ClientError(
            f"HTTP 请求失败: {response.status_code}；{code or '请检查服务、权限或运行状态'}",
            code=code,
            status=response.status_code,
        )

    @property
    def evaluations(self) -> SyncEvalClient:
        return SyncEvalClient(http=self._http)

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

    def agent_inventory(self) -> list[AgentInventoryView]:
        response = self._http.get("/v1/agents/inventory")
        self._check(response)
        return TypeAdapter(list[AgentInventoryView]).validate_json(response.content)

    def agent_resources(self) -> list[AgentResourceView]:
        response = self._http.get("/v1/agents/resources")
        self._check(response)
        return TypeAdapter(list[AgentResourceView]).validate_json(response.content)

    def agent_workspace(self, agent_id: str) -> WorkspaceView:
        response = self._http.get(f"/v1/agents/{agent_id}/workspace")
        self._check(response)
        return WorkspaceView.model_validate_json(response.content)

    def agent_read_file(
        self, agent_id: str, path: str, *, start_line: int = 1, max_lines: int = 200
    ) -> WorkspaceFile:
        request = ReadFileInput(path=path, start_line=start_line, max_lines=max_lines)
        response = self._http.get(
            f"/v1/agents/{agent_id}/workspace/read", params=request.model_dump()
        )
        self._check(response)
        return WorkspaceFile.model_validate_json(response.content)

    def agent_list_files(
        self, agent_id: str, path: str = ".", *, limit: int = 200
    ) -> WorkspaceListing:
        request = ListFilesInput(path=path, limit=limit)
        response = self._http.get(
            f"/v1/agents/{agent_id}/workspace/list", params=request.model_dump()
        )
        self._check(response)
        return WorkspaceListing.model_validate_json(response.content)

    def agent_search_files(
        self, agent_id: str, query: str, *, path: str = ".", limit: int = 100
    ) -> WorkspaceSearch:
        request = SearchFilesInput(query=query, path=path, limit=limit)
        response = self._http.post(
            f"/v1/agents/{agent_id}/workspace/search", json=request.model_dump(mode="json")
        )
        self._check(response)
        return WorkspaceSearch.model_validate_json(response.content)

    def agent_workspace_preview(
        self, agent_id: str, request: WorkspacePreviewRequest, *, confirmed: bool = False
    ) -> WorkspaceChangeView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/changes/preview",
            params={"confirmed": str(confirmed).lower()},
            json=request.model_dump(mode="json"),
        )
        self._check(response)
        return WorkspaceChangeView.model_validate_json(response.content)

    def agent_workspace_change(
        self, agent_id: str, request: WorkspaceChangeAction, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/changes",
            params={"confirmed": str(confirmed).lower()},
            json=request.model_dump(mode="json"),
        )
        self._check(response)
        return WorkspaceAttemptView.model_validate_json(response.content)

    def agent_workspace_changes(
        self, agent_id: str, *, offset: int = 0, limit: int = 50
    ) -> list[WorkspaceChangeView]:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/changes",
            params={"offset": offset, "limit": limit},
        )
        self._check(response)
        return TypeAdapter(list[WorkspaceChangeView]).validate_json(response.content)

    def agent_workspace_proposal(self, agent_id: str, proposal_id: str) -> WorkspaceChangeView:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/changes/"
            f"{_workspace_identifier(proposal_id)}"
        )
        self._check(response)
        return WorkspaceChangeView.model_validate_json(response.content)

    def agent_workspace_history(
        self, agent_id: str, proposal_id: str
    ) -> list[WorkspaceAttemptView]:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/changes/"
            f"{_workspace_identifier(proposal_id)}/history"
        )
        self._check(response)
        return TypeAdapter(list[WorkspaceAttemptView]).validate_json(response.content)

    def agent_workspace_operation(self, agent_id: str, operation_id: str) -> WorkspaceAttemptView:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/operations/"
            f"{_workspace_identifier(operation_id)}"
        )
        self._check(response)
        return WorkspaceAttemptView.model_validate_json(response.content)

    def agent_workspace_operation_proof(
        self, agent_id: str, operation_id: str
    ) -> WorkspaceOperationProofView:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/operations/"
            f"{_workspace_identifier(operation_id)}/proof"
        )
        self._check(response)
        return WorkspaceOperationProofView.model_validate_json(response.content)

    def agent_workspace_reconcile(
        self, agent_id: str, operation_id: str, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/operations/"
            f"{_workspace_identifier(operation_id)}/reconcile",
            params={"confirmed": str(confirmed).lower()},
            json=WorkspaceOperationRequest(operation_id=operation_id).model_dump(mode="json"),
        )
        self._check(response)
        return WorkspaceAttemptView.model_validate_json(response.content)

    def agent_workspace_finalize(
        self, agent_id: str, operation_id: str, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/operations/"
            f"{_workspace_identifier(operation_id)}/finalize",
            params={"confirmed": str(confirmed).lower()},
            json=WorkspaceOperationRequest(operation_id=operation_id).model_dump(mode="json"),
        )
        self._check(response)
        return WorkspaceAttemptView.model_validate_json(response.content)

    def agent_workspace_native_changes(
        self,
        agent_id: str,
        *,
        filter: NativeListFilter = "active",
        cursor: str | None = None,
        limit: int = 32,
    ) -> NativeChangePage:
        """读取原生修改清单的一页；游标绑定快照，不能与旧 changes offset 混用。"""
        params: dict[str, str | int] = {"filter": filter, "limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-changes",
            params=params,
        )
        self._check(response)
        return NativeChangePage.model_validate_json(response.content)

    def agent_workspace_start_native_batch(
        self, agent_id: str, request: NativeBatchRequest, *, confirmed: bool = False
    ) -> NativeBatchView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-batches",
            params={"confirmed": str(confirmed).lower()},
            json=request.model_dump(mode="json"),
        )
        self._check(response)
        return NativeBatchView.model_validate_json(response.content)

    def agent_workspace_native_batch(self, agent_id: str, batch_id: str) -> NativeBatchView:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-batches/"
            f"{_workspace_identifier(batch_id)}"
        )
        self._check(response)
        return NativeBatchView.model_validate_json(response.content)

    def agent_workspace_native_batch_history(
        self, agent_id: str, *, cursor: str | None = None, limit: int = 32
    ) -> NativeBatchHistoryPage:
        params: dict[str, str | int] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-batches/history",
            params=params,
        )
        self._check(response)
        return NativeBatchHistoryPage.model_validate_json(response.content)

    def agent_workspace_native_batch_by_key(
        self, agent_id: str, request_key: str
    ) -> NativeBatchView | None:
        if not 1 <= len(request_key) <= 128 or any(ord(char) < 32 for char in request_key):
            raise ClientError(
                "原生批次请求键无效", code="workspace_request_key_invalid", status=422
            )
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-batches/by-key",
            params={"request_key": request_key},
        )
        self._check(response)
        return TypeAdapter(NativeBatchView | None).validate_json(response.content)

    def agent_workspace_wait_native_batch(
        self, agent_id: str, batch_id: str, *, timeout_seconds: float = 30
    ) -> NativeBatchView:
        if not math.isfinite(timeout_seconds) or not 0 <= timeout_seconds <= 300:
            raise ClientError(
                "批次等待时间须为0至300秒", code="workspace_batch_timeout", status=422
            )
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-batches/"
            f"{_workspace_identifier(batch_id)}/wait",
            params={"timeout_seconds": timeout_seconds},
            timeout=max(60, timeout_seconds + 5),
        )
        self._check(response)
        return NativeBatchView.model_validate_json(response.content)

    def agent_workspace_cancel_native_batch(self, agent_id: str, batch_id: str) -> NativeBatchView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-batches/"
            f"{_workspace_identifier(batch_id)}/cancel"
        )
        self._check(response)
        return NativeBatchView.model_validate_json(response.content)

    def agent_workspace_create_checkpoint(
        self, agent_id: str, request: WorkspaceCheckpointCreate, *, confirmed: bool = False
    ) -> WorkspaceCheckpointView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-checkpoints",
            params={"confirmed": str(confirmed).lower()},
            json=request.model_dump(mode="json"),
        )
        self._check(response)
        return WorkspaceCheckpointView.model_validate_json(response.content)

    def agent_workspace_checkpoints(
        self, agent_id: str, *, offset: int = 0, limit: int = 32
    ) -> list[WorkspaceCheckpointView]:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-checkpoints",
            params={"offset": offset, "limit": limit},
        )
        self._check(response)
        return TypeAdapter(list[WorkspaceCheckpointView]).validate_json(response.content)

    def agent_workspace_checkpoint(
        self, agent_id: str, checkpoint_id: str
    ) -> WorkspaceCheckpointView:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-checkpoints/{_workspace_identifier(checkpoint_id)}"
        )
        self._check(response)
        return WorkspaceCheckpointView.model_validate_json(response.content)

    def agent_workspace_checkpoint_by_key(
        self, agent_id: str, request_key: str
    ) -> WorkspaceCheckpointView | None:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-checkpoints/by-key",
            params={"request_key": _checkpoint_key(request_key)},
        )
        self._check(response)
        return TypeAdapter(WorkspaceCheckpointView | None).validate_json(response.content)

    def agent_workspace_wait_checkpoint(
        self, agent_id: str, checkpoint_id: str, *, timeout_seconds: float = 30
    ) -> WorkspaceCheckpointView:
        timeout = _checkpoint_wait(timeout_seconds, "workspace_checkpoint_timeout")
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-checkpoints/"
            f"{_workspace_identifier(checkpoint_id)}/wait",
            params={"timeout_seconds": timeout_seconds},
            timeout=timeout,
        )
        self._check(response)
        return WorkspaceCheckpointView.model_validate_json(response.content)

    def agent_workspace_cancel_checkpoint(
        self, agent_id: str, checkpoint_id: str
    ) -> WorkspaceCheckpointView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-checkpoints/"
            f"{_workspace_identifier(checkpoint_id)}/cancel"
        )
        self._check(response)
        return WorkspaceCheckpointView.model_validate_json(response.content)

    def agent_workspace_preview_restore(
        self, agent_id: str, request: CheckpointRestorePreview, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-restores/preview",
            params={"confirmed": str(confirmed).lower()},
            json=request.model_dump(mode="json"),
        )
        self._check(response)
        return CheckpointRestoreView.model_validate_json(response.content)

    def agent_workspace_execute_restore(
        self, agent_id: str, request: CheckpointRestoreExecute, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-restores/"
            f"{_workspace_identifier(request.plan_id)}/execute",
            params={"confirmed": str(confirmed).lower()},
            json=request.model_dump(mode="json"),
        )
        self._check(response)
        return CheckpointRestoreView.model_validate_json(response.content)

    def agent_workspace_restores(
        self, agent_id: str, *, offset: int = 0, limit: int = 32
    ) -> list[CheckpointRestoreView]:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-restores",
            params={"offset": offset, "limit": limit},
        )
        self._check(response)
        return TypeAdapter(list[CheckpointRestoreView]).validate_json(response.content)

    def agent_workspace_restore(self, agent_id: str, plan_id: str) -> CheckpointRestoreView:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-restores/{_workspace_identifier(plan_id)}"
        )
        self._check(response)
        return CheckpointRestoreView.model_validate_json(response.content)

    def agent_workspace_restore_by_key(
        self, agent_id: str, request_key: str
    ) -> CheckpointRestoreView | None:
        response = self._http.get(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-restores/by-key",
            params={"request_key": _checkpoint_key(request_key)},
        )
        self._check(response)
        return TypeAdapter(CheckpointRestoreView | None).validate_json(response.content)

    def agent_workspace_wait_restore(
        self, agent_id: str, plan_id: str, *, timeout_seconds: float = 30
    ) -> CheckpointRestoreView:
        timeout = _checkpoint_wait(timeout_seconds, "workspace_restore_timeout")
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-restores/{_workspace_identifier(plan_id)}/wait",
            params={"timeout_seconds": timeout_seconds},
            timeout=timeout,
        )
        self._check(response)
        return CheckpointRestoreView.model_validate_json(response.content)

    def agent_workspace_cancel_restore(self, agent_id: str, plan_id: str) -> CheckpointRestoreView:
        response = self._http.post(
            f"/v1/agents/{_workspace_identifier(agent_id)}/workspace/native-restores/{_workspace_identifier(plan_id)}/cancel"
        )
        self._check(response)
        return CheckpointRestoreView.model_validate_json(response.content)

    def agent_tasks(self, agent_id: str, kind: TaskKind) -> TaskSnapshot:
        response = self._http.get(f"/v1/agents/{agent_id}/tasks/{kind}")
        self._check(response)
        return TaskSnapshot.model_validate_json(response.content)

    def agent_tasks_write(
        self, agent_id: str, kind: TaskKind, request: TaskWriteRequest
    ) -> TaskSnapshot:
        response = self._http.post(
            f"/v1/agents/{agent_id}/tasks/{kind}", json=request.model_dump(mode="json")
        )
        self._check(response)
        return TaskSnapshot.model_validate_json(response.content)

    def agent_tasks_history(self, agent_id: str, kind: TaskKind) -> list[TaskOperationView]:
        response = self._http.get(f"/v1/agents/{agent_id}/tasks/{kind}/history")
        self._check(response)
        return TypeAdapter(list[TaskOperationView]).validate_json(response.content)

    def agent_restore(self, agent_id: str) -> AgentView:
        response = self._http.post(f"/v1/agents/{agent_id}/restore")
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_session(self, agent_id: str) -> AgentSessionView:
        response = self._http.get(f"/v1/agents/{agent_id}/session")
        self._check(response)
        return AgentSessionView.model_validate_json(response.content)

    def agent_turns(self, agent_id: str) -> list[AgentTurnView]:
        response = self._http.get(f"/v1/agents/{agent_id}/turns")
        self._check(response)
        return TypeAdapter(list[AgentTurnView]).validate_json(response.content)

    def agent_session_close(self, agent_id: str, expected_revision: int) -> AgentSessionView:
        request = AgentSessionControl(expected_revision=expected_revision)
        response = self._http.post(
            f"/v1/agents/{agent_id}/session/close", json=request.model_dump(mode="json")
        )
        self._check(response)
        return AgentSessionView.model_validate_json(response.content)

    def agent_session_reopen(self, agent_id: str, expected_revision: int) -> AgentSessionView:
        request = AgentSessionControl(expected_revision=expected_revision)
        response = self._http.post(
            f"/v1/agents/{agent_id}/session/reopen", json=request.model_dump(mode="json")
        )
        self._check(response)
        return AgentSessionView.model_validate_json(response.content)

    def agent_profiles(self) -> list[AgentProfileVersion]:
        response = self._http.get("/v1/agents/profiles")
        self._check(response)
        return TypeAdapter(list[AgentProfileVersion]).validate_json(response.content)

    def agent_profile(self, name: str, revision: int | None = None) -> AgentProfileVersion:
        response = self._http.get(
            f"/v1/agents/profiles/{name}", params={"revision": revision} if revision else {}
        )
        self._check(response)
        return AgentProfileVersion.model_validate_json(response.content)

    def agent_profile_put(self, request: AgentProfileUpdate) -> AgentProfileVersion:
        response = self._http.put(
            f"/v1/agents/profiles/{request.config.name}", json=request.model_dump(mode="json")
        )
        self._check(response)
        return AgentProfileVersion.model_validate_json(response.content)

    def agent_spawn(
        self, name: str, *, persistent: bool = False, profile_revision: int | None = None
    ) -> AgentView:
        response = self._http.post(
            "/v1/agents",
            json={"name": name, "persistent": persistent, "profile_revision": profile_revision},
        )
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

    def agent_start(
        self, agent_id: str, prompt: str, idempotency_key: str | None = None
    ) -> AgentView:
        request = AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        response = self._http.post(
            f"/v1/agents/{agent_id}/start", json=request.model_dump(exclude_none=True)
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

    def agent_wait(self, agent_id: str) -> AgentView:
        response = self._http.post(
            f"/v1/agents/{agent_id}/wait", timeout=httpx.Timeout(30, read=330)
        )
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_destroy(self, agent_id: str) -> AgentView:
        response = self._http.delete(f"/v1/agents/{agent_id}")
        self._check(response)
        return AgentView.model_validate_json(response.content)

    def agent_events(
        self, agent_id: str, from_seq: int = 0, *, run_id: str | None = None
    ) -> Iterator[StreamFrame]:
        params: dict[str, str | int] = {"from_seq": from_seq}
        if run_id is not None:
            params["run_id"] = run_id
        with self._http.stream(
            "GET",
            f"/v1/agents/{agent_id}/events",
            params=params,
            timeout=httpx.Timeout(30, read=45),
        ) as response:
            self._check(response)
            bound_run_id = response.headers.get("x-krim-agent-run-id")
            if not bound_run_id or (run_id is not None and run_id != bound_run_id):
                raise ClientError("Agent SSE 缺少运行关联")
            run_id = bound_run_id
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

    def run_trace(self, run_id: str) -> list[SpanRecord]:
        response = self._http.get(f"/v1/runs/{run_id}/trace")
        self._check(response)
        return TypeAdapter(list[SpanRecord]).validate_json(response.content)

    def workflow_trace(self, run_id: str) -> list[SpanRecord]:
        response = self._http.get(f"/v1/workflows/runs/{run_id}/trace")
        self._check(response)
        return TypeAdapter(list[SpanRecord]).validate_json(response.content)

    def trace(self, trace_id: str) -> TraceView:
        response = self._http.get(f"/v1/traces/{trace_id}")
        self._check(response)
        return TraceView.model_validate_json(response.content)

    def observability_status(self) -> ObservationStatus:
        response = self._http.get("/v1/observability/status")
        self._check(response)
        return ObservationStatus.model_validate_json(response.content)

    def budget_accounts(self) -> list[BudgetAccountView]:
        response = self._http.get("/v1/budgets/accounts")
        self._check(response)
        return TypeAdapter(list[BudgetAccountView]).validate_json(response.content)

    def budget_account(self, account_id: str) -> BudgetAccountView:
        response = self._http.get(f"/v1/budgets/accounts/{account_id}")
        self._check(response)
        return BudgetAccountView.model_validate_json(response.content)

    def run_budget(self, run_id: str) -> list[BudgetRequestView]:
        response = self._http.get(f"/v1/runs/{run_id}/budget")
        self._check(response)
        return TypeAdapter(list[BudgetRequestView]).validate_json(response.content)

    def budget_request(self, request_id: str) -> BudgetRequestView:
        response = self._http.get(f"/v1/budgets/requests/{request_id}")
        self._check(response)
        return BudgetRequestView.model_validate_json(response.content)

    def budget_resolve(self, request_id: str, payload: BudgetResolveRequest) -> BudgetRequestView:
        response = self._http.post(
            f"/v1/budgets/requests/{request_id}/resolve", json=payload.model_dump(mode="json")
        )
        self._check(response)
        return BudgetRequestView.model_validate_json(response.content)

    def compact(
        self,
        topic_id: str,
        options: ContextOptions | None = None,
        *,
        confirm: bool = False,
        expected_branch_key: str | None = None,
        proposal_id: str | None = None,
        proposal_revision: int | None = None,
    ) -> ContextCompactResponse:
        values = options.model_dump() if options is not None else {}
        request = ContextCompactRequest(
            **values,
            confirm=confirm,
            expected_branch_key=expected_branch_key,
            proposal_id=proposal_id,
            proposal_revision=proposal_revision,
        )
        response = self._http.post(
            f"/v1/topics/{topic_id}/compact", json=request.model_dump(mode="json")
        )
        self._check(response)
        return ContextCompactResponse.model_validate_json(response.content)

    def context_proposals(
        self, topic_id: str, *, pending_only: bool = False
    ) -> list[ContextProposalView]:
        response = self._http.get(
            f"/v1/topics/{topic_id}/compact-proposals", params={"pending_only": pending_only}
        )
        self._check(response)
        return TypeAdapter(list[ContextProposalView]).validate_json(response.content)

    def context_proposal(self, topic_id: str, proposal_id: str) -> ContextProposalView:
        response = self._http.get(f"/v1/topics/{topic_id}/compact-proposals/{proposal_id}")
        self._check(response)
        return ContextProposalView.model_validate_json(response.content)

    def decide_context_proposal(
        self, topic_id: str, proposal_id: str, request: ContextProposalDecision
    ) -> ContextProposalView:
        response = self._http.post(
            f"/v1/topics/{topic_id}/compact-proposals/{proposal_id}/decision",
            json=request.model_dump(mode="json"),
        )
        self._check(response)
        return ContextProposalView.model_validate_json(response.content)

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

    def active_runs(self) -> list[ActiveRunView]:
        response = self._http.get("/v1/runs/active")
        self._check(response)
        return TypeAdapter(list[ActiveRunView]).validate_json(response.content)

    def multiplex_events(self, request: MultiplexRequest) -> Iterator[RunStreamFrame | None]:
        with self._http.stream(
            "POST",
            "/v1/runs/events",
            json=request.model_dump(mode="json"),
            timeout=httpx.Timeout(30, read=45),
        ) as response:
            self._check(response)
            expected = {source.run_id: source.next_seq for source in request.sources}
            topics = {source.run_id: source.topic_id for source in request.sources}
            data: list[str] = []
            event_id = ""
            event_kind = ""
            for line in response.iter_lines():
                if len(line.encode("utf-8")) > 262144:
                    raise ClientError("多路 SSE 行超过大小限制")
                if line.startswith("data:"):
                    data.append(line[5:].lstrip(" "))
                elif line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("event:"):
                    event_kind = line[6:].strip()
                elif not line and data:
                    if len("\n".join(data).encode("utf-8")) > 262144:
                        raise ClientError("多路 SSE 事件超过大小限制")
                    value = RunStreamFrame.model_validate_json("\n".join(data))
                    expected_seq = expected.get(value.frame.stream_id)
                    if expected_seq is None or value.frame.seq != expected_seq:
                        raise ClientError("多路 SSE 来源或序号不正确")
                    if topics[value.frame.stream_id] != value.topic_id:
                        raise ClientError("多路 SSE 话题关联不正确")
                    if (
                        event_kind != "run-frame"
                        or event_id != f"{value.frame.stream_id}:{value.frame.seq}"
                    ):
                        raise ClientError("多路 SSE 事件关联不正确")
                    expected[value.frame.stream_id] += 1
                    yield value
                    data, event_id, event_kind = [], "", ""
                    if value.frame.kind in {"end", "error"}:
                        expected.pop(value.frame.stream_id, None)
                    if not expected:
                        return
                elif not line and not data:
                    yield None
            for run_id, cursor in expected.items():
                summary = self.event_summary(run_id)
                if not summary.terminal or summary.count != cursor:
                    raise ClientError("多路 SSE 提前断开；可使用各来源 next_seq 续传")

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
