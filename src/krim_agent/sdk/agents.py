"""Python AgentSession SDK；与 REST/CLI 使用同一会话与持久事实。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING
from uuid import uuid4

from krim_agent.agents import AgentSessionView, AgentTurnView, ExecResult
from krim_agent.application.agents import AgentPromptRequest, AgentService, AgentView
from krim_agent.execution.agent_models import (
    AgentCommandRequest,
    CommandLookup,
    CommandRead,
    CommandSlice,
    CommandSummary,
)
from krim_agent.execution.directory_publication import (
    CommandDirectoryChangesPage,
    CommandDirectoryChangesRequest,
    CommandDirectoryProposalRequest,
    CommandDirectoryProposalView,
)
from krim_agent.execution.management import (
    CommandAccepted,
    CommandHistoryPage,
    CommandHistoryRequest,
)
from krim_agent.execution.publication import (
    CommandChangesPage,
    CommandChangesRequest,
    CommandProposalRequest,
    CommandProposalView,
)
from krim_agent.execution.records import CommandView
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StreamFrame
from krim_agent.task_state import TaskKind, TaskMutation, TaskOperationView, TaskSnapshot
from krim_agent.tools.workspace import ListFilesInput, ReadFileInput, SearchFilesInput
from krim_agent.workspace import WorkspaceFile, WorkspaceListing, WorkspaceSearch, WorkspaceView
from krim_agent.workspace_change import (
    FileChangeSpec,
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
    WorkspaceOperationProofView,
    WorkspaceOperationRequest,
    WorkspacePreviewRequest,
)
from krim_agent.workspace_checkpoint import WorkspaceCheckpointCreate, WorkspaceCheckpointView
from krim_agent.workspace_directory import (
    DirectoryAttemptView,
    DirectoryChangeAction,
    DirectoryChangeSpec,
    DirectoryInspectRequest,
    DirectoryInspectView,
    DirectoryOperationRequest,
    DirectoryPreviewRequest,
    DirectoryPreviewView,
    DirectoryQueryRequest,
    DirectoryQueryResult,
)
from krim_agent.workspace_list import (
    NativeBatchHistoryPage,
    NativeBatchRequest,
    NativeBatchView,
    NativeChangePage,
    NativeListFilter,
)
from krim_agent.workspace_mixed_batch import (
    MixedBatchHistoryPage,
    MixedBatchRequest,
    MixedBatchView,
)
from krim_agent.workspace_restore import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)
from krim_agent.workspace_tree import TreePlanRequest, TreePlanStepPage, TreePlanView

if TYPE_CHECKING:
    from .text import StreamTextResult
    from .types import GenerateResult


class AgentSession:
    """一个实例对应一个稳定 session；重开保留上下文、轮次和预算。"""

    def __init__(self, service: AgentService, agent_id: str) -> None:
        self._service = service
        self.agent_id = agent_id
        self.session_id = agent_id

    @property
    def service(self) -> AgentService:
        if self._service._closed:
            raise ServiceError("sdk_closed", "Agent SDK 已关闭", 503)
        return self._service

    async def state(self) -> AgentSessionView:
        return await self.service.session(self.agent_id)

    async def execute_command(
        self, request: AgentCommandRequest, *, confirmed: bool = False
    ) -> CommandSummary:
        return await self.service.command_execute(self.agent_id, request, confirmed=confirmed)

    async def start_command(
        self, request: AgentCommandRequest, *, confirmed: bool = False
    ) -> CommandAccepted:
        return await self.service.command_start(self.agent_id, request, confirmed=confirmed)

    async def wait_command(
        self, command_id: str, *, timeout_seconds: float = 30, confirmed: bool = False
    ) -> CommandSummary:
        return await self.service.command_wait(
            self.agent_id, command_id, timeout_seconds=timeout_seconds, confirmed=confirmed
        )

    async def command_history(
        self, request: CommandHistoryRequest | None = None, *, confirmed: bool = False
    ) -> CommandHistoryPage:
        return await self.service.command_history(
            self.agent_id, request or CommandHistoryRequest(), confirmed=confirmed
        )

    async def inspect_command(
        self, request: CommandLookup, *, confirmed: bool = False
    ) -> CommandView:
        return await self.service.command_inspect(self.agent_id, request, confirmed=confirmed)

    async def command(self, request: CommandLookup, *, confirmed: bool = False) -> CommandSummary:
        return await self.service.command_get(self.agent_id, request, confirmed=confirmed)

    async def read_command(self, request: CommandRead, *, confirmed: bool = False) -> CommandSlice:
        return await self.service.command_read(self.agent_id, request, confirmed=confirmed)

    async def cancel_command(
        self, request: CommandLookup, *, confirmed: bool = False
    ) -> CommandSummary:
        return await self.service.command_cancel(self.agent_id, request, confirmed=confirmed)

    async def command_changes(
        self, request: CommandChangesRequest, *, confirmed: bool = False
    ) -> CommandChangesPage:
        return await self.service.command_changes(self.agent_id, request, confirmed=confirmed)

    async def propose_command_change(
        self, request: CommandProposalRequest, *, confirmed: bool = False
    ) -> CommandProposalView:
        return await self.service.command_propose(self.agent_id, request, confirmed=confirmed)

    async def propose_command_directory(
        self,
        request: CommandDirectoryProposalRequest,
        *,
        confirmed: bool = False,
    ) -> CommandDirectoryProposalView:
        return await self.service.command_directory_propose(
            self.agent_id, request, confirmed=confirmed
        )

    async def command_directory_changes(
        self,
        request: CommandDirectoryChangesRequest,
        *,
        confirmed: bool = False,
    ) -> CommandDirectoryChangesPage:
        return await self.service.command_directory_changes(
            self.agent_id, request, confirmed=confirmed
        )

    async def view(self) -> AgentView:
        return await self.service.get(self.agent_id)

    async def inspect_directory(
        self, path: str, *, confirmed: bool = False
    ) -> DirectoryInspectView:
        return await self.service.directory_inspect(
            self.agent_id, DirectoryInspectRequest(path=path), confirmed=confirmed
        )

    async def preview_directory_change(
        self,
        spec: DirectoryChangeSpec,
        *,
        request_key: str | None = None,
        ttl_seconds: float = 3600,
        confirmed: bool = False,
    ) -> DirectoryPreviewView:
        return await self.service.directory_preview(
            self.agent_id,
            DirectoryPreviewRequest(
                spec=spec, request_key=request_key or str(uuid4()), ttl_seconds=ttl_seconds
            ),
            confirmed=confirmed,
        )

    async def change_directory(
        self, request: DirectoryChangeAction, *, confirmed: bool = False
    ) -> DirectoryAttemptView:
        return await self.service.directory_change(self.agent_id, request, confirmed=confirmed)

    async def reconcile_directory_change(
        self, operation_id: str, *, confirmed: bool = False
    ) -> DirectoryAttemptView:
        return await self.service.directory_reconcile(
            self.agent_id, DirectoryOperationRequest(operation_id=operation_id), confirmed=confirmed
        )

    async def finalize_directory_change(
        self, operation_id: str, *, confirmed: bool = False
    ) -> DirectoryAttemptView:
        return await self.service.directory_finalize(
            self.agent_id, DirectoryOperationRequest(operation_id=operation_id), confirmed=confirmed
        )

    async def query_directory(
        self, request: DirectoryQueryRequest, *, confirmed: bool = False
    ) -> DirectoryQueryResult:
        return await self.service.directory_query(self.agent_id, request, confirmed=confirmed)

    async def execute(self, prompt: str, *, idempotency_key: str | None = None) -> ExecResult:
        result = await self.service.execute(
            self.agent_id, AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        )
        assert result.result is not None
        return result.result

    async def start(self, prompt: str, *, idempotency_key: str | None = None) -> AgentView:
        return await self.service.start(
            self.agent_id, AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        )

    async def wait(self) -> ExecResult:
        result = await self.service.wait(self.agent_id)
        assert result.result is not None
        return result.result

    async def stream_text(
        self, prompt: str, *, idempotency_key: str | None = None
    ) -> StreamTextResult:
        key = idempotency_key or str(uuid4())
        await self.start(prompt, idempotency_key=key)
        run_id = await self.service.wait_started(self.agent_id, key)
        return await self.stream(run_id=run_id)

    async def generate_text(
        self, prompt: str, *, idempotency_key: str | None = None
    ) -> GenerateResult:
        stream = await self.stream_text(prompt, idempotency_key=idempotency_key)
        result = await stream.collect()
        await self.service.wait_run(self.agent_id, result.run.id)
        return result

    async def resume(self, prompt: str, *, idempotency_key: str | None = None) -> ExecResult:
        result = await self.service.resume(
            self.agent_id, AgentPromptRequest(prompt=prompt, idempotency_key=idempotency_key)
        )
        assert result.result is not None
        return result.result

    async def turns(self) -> list[AgentTurnView]:
        return await self.service.session_turns(self.agent_id)

    async def tasks(self, kind: TaskKind = "todo") -> TaskSnapshot:
        return await self.service.task_snapshot(self.agent_id, kind)

    async def update_tasks(self, kind: TaskKind, mutation: TaskMutation) -> TaskSnapshot:
        if mutation.expected_revision is None:
            raise ValueError("SDK任务写需要expected_revision")
        return await self.service.task_mutate(self.agent_id, kind, mutation)

    async def task_history(self, kind: TaskKind = "todo") -> list[TaskOperationView]:
        return await self.service.task_history(self.agent_id, kind)

    async def workspace(self) -> WorkspaceView:
        return await self.service.workspace_info(self.agent_id)

    async def read_file(
        self, path: str, *, start_line: int = 1, max_lines: int = 200
    ) -> WorkspaceFile:
        return await self.service.workspace_read(
            self.agent_id, ReadFileInput(path=path, start_line=start_line, max_lines=max_lines)
        )

    async def list_files(self, path: str = ".", *, limit: int = 200) -> WorkspaceListing:
        return await self.service.workspace_list(
            self.agent_id, ListFilesInput(path=path, limit=limit)
        )

    async def search_files(
        self, query: str, *, path: str = ".", limit: int = 100
    ) -> WorkspaceSearch:
        return await self.service.workspace_search(
            self.agent_id, SearchFilesInput(query=query, path=path, limit=limit)
        )

    async def preview_file_change(
        self,
        spec: FileChangeSpec,
        *,
        request_key: str,
        ttl_seconds: float = 3600,
        confirmed: bool = False,
    ) -> WorkspaceChangeView:
        return await self.service.workspace_preview(
            self.agent_id,
            WorkspacePreviewRequest(spec=spec, request_key=request_key, ttl_seconds=ttl_seconds),
            confirmed=confirmed,
        )

    async def change_file(
        self, request: WorkspaceChangeAction, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        """apply/undo/keep共享幂等/CAS；confirmed只满足宿主ask，不覆盖deny。"""
        return await self.service.workspace_change(self.agent_id, request, confirmed=confirmed)

    async def reconcile_file_change(
        self, operation_id: str, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        return await self.service.workspace_reconcile(
            self.agent_id, WorkspaceOperationRequest(operation_id=operation_id), confirmed=confirmed
        )

    async def finalize_file_change(
        self, operation_id: str, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        return await self.service.workspace_finalize(
            self.agent_id, WorkspaceOperationRequest(operation_id=operation_id), confirmed=confirmed
        )

    async def file_changes(self, *, offset: int = 0, limit: int = 50) -> list[WorkspaceChangeView]:
        return await self.service.workspace_proposals(self.agent_id, offset=offset, limit=limit)

    async def file_change(self, proposal_id: str) -> WorkspaceChangeView:
        return await self.service.workspace_proposal(self.agent_id, proposal_id)

    async def file_change_operation(self, operation_id: str) -> WorkspaceAttemptView:
        return await self.service.workspace_operation(self.agent_id, operation_id)

    async def file_change_proof(self, operation_id: str) -> WorkspaceOperationProofView:
        """读取保留时冻结的版本证明；当前文件内容仍通过read_file读取。"""
        return await self.service.workspace_operation_proof(self.agent_id, operation_id)

    async def file_change_history(self, proposal_id: str) -> list[WorkspaceAttemptView]:
        return await self.service.workspace_history(self.agent_id, proposal_id)

    async def file_change_list(
        self,
        *,
        filter: NativeListFilter = "active",
        cursor: str | None = None,
        limit: int = 32,
    ) -> NativeChangePage:
        return await self.service.workspace_change_list(
            self.agent_id, filter=filter, cursor=cursor, limit=limit
        )

    async def create_file_checkpoint(
        self, request: WorkspaceCheckpointCreate, *, confirmed: bool = False
    ) -> WorkspaceCheckpointView:
        return await self.service.workspace_create_checkpoint(
            self.agent_id, request, confirmed=confirmed
        )

    async def file_checkpoint(self, checkpoint_id: str) -> WorkspaceCheckpointView:
        return await self.service.workspace_checkpoint(self.agent_id, checkpoint_id)

    async def file_checkpoint_by_key(self, request_key: str) -> WorkspaceCheckpointView | None:
        return await self.service.workspace_checkpoint_by_key(self.agent_id, request_key)

    async def file_checkpoints(
        self, *, offset: int = 0, limit: int = 32
    ) -> list[WorkspaceCheckpointView]:
        return await self.service.workspace_checkpoint_list(
            self.agent_id, offset=offset, limit=limit
        )

    async def wait_file_checkpoint(
        self, checkpoint_id: str, *, timeout_seconds: float = 30
    ) -> WorkspaceCheckpointView:
        return await self.service.workspace_checkpoint_wait(
            self.agent_id, checkpoint_id, timeout_seconds=timeout_seconds
        )

    async def cancel_file_checkpoint(self, checkpoint_id: str) -> WorkspaceCheckpointView:
        return await self.service.workspace_checkpoint_cancel(self.agent_id, checkpoint_id)

    async def preview_file_checkpoint_restore(
        self, request: CheckpointRestorePreview, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        return await self.service.workspace_preview_restore(
            self.agent_id, request, confirmed=confirmed
        )

    async def execute_file_checkpoint_restore(
        self, request: CheckpointRestoreExecute, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        return await self.service.workspace_execute_restore(
            self.agent_id, request, confirmed=confirmed
        )

    async def file_checkpoint_restore(self, plan_id: str) -> CheckpointRestoreView:
        return await self.service.workspace_restore(self.agent_id, plan_id)

    async def file_checkpoint_restore_by_key(
        self, request_key: str
    ) -> CheckpointRestoreView | None:
        return await self.service.workspace_restore_by_key(self.agent_id, request_key)

    async def file_checkpoint_restores(
        self, *, offset: int = 0, limit: int = 32
    ) -> list[CheckpointRestoreView]:
        return await self.service.workspace_restore_list(self.agent_id, offset=offset, limit=limit)

    async def wait_file_checkpoint_restore(
        self, plan_id: str, *, timeout_seconds: float = 30
    ) -> CheckpointRestoreView:
        return await self.service.workspace_restore_wait(
            self.agent_id, plan_id, timeout_seconds=timeout_seconds
        )

    async def cancel_file_checkpoint_restore(self, plan_id: str) -> CheckpointRestoreView:
        return await self.service.workspace_restore_cancel(self.agent_id, plan_id)

    async def start_file_change_batch(
        self, request: NativeBatchRequest, *, confirmed: bool = False
    ) -> NativeBatchView:
        return await self.service.workspace_start_batch(self.agent_id, request, confirmed=confirmed)

    async def start_mixed_change_batch(
        self, request: MixedBatchRequest, *, confirmed: bool = False
    ) -> MixedBatchView:
        return await self.service.workspace_start_mixed_batch(
            self.agent_id, request, confirmed=confirmed
        )

    async def prepare_directory_tree(
        self, request: TreePlanRequest, *, confirmed: bool = False
    ) -> TreePlanView:
        return await self.service.workspace_prepare_tree(
            self.agent_id, request, confirmed=confirmed
        )

    async def start_directory_tree(self, plan_id: str, *, confirmed: bool = False) -> TreePlanView:
        return await self.service.workspace_start_tree(self.agent_id, plan_id, confirmed=confirmed)

    async def directory_tree(self, plan_id: str) -> TreePlanView:
        return await self.service.workspace_tree(self.agent_id, plan_id)

    async def directory_tree_by_key(self, request_key: str) -> TreePlanView | None:
        return await self.service.workspace_tree_by_key(self.agent_id, request_key)

    async def directory_tree_steps(
        self, plan_id: str, *, after: int | None = None, limit: int = 32
    ) -> TreePlanStepPage:
        return await self.service.workspace_tree_steps(
            self.agent_id, plan_id, after=after, limit=limit
        )

    async def wait_directory_tree(
        self, plan_id: str, *, timeout_seconds: float = 30
    ) -> TreePlanView:
        return await self.service.workspace_wait_tree(
            self.agent_id, plan_id, timeout_seconds=timeout_seconds
        )

    async def cancel_directory_tree(self, plan_id: str) -> TreePlanView:
        return await self.service.workspace_cancel_tree(self.agent_id, plan_id)

    async def mixed_change_batch(self, batch_id: str) -> MixedBatchView:
        return await self.service.workspace_mixed_batch(self.agent_id, batch_id)

    async def mixed_change_batch_by_key(self, request_key: str) -> MixedBatchView | None:
        return await self.service.workspace_mixed_batch_by_key(self.agent_id, request_key)

    async def mixed_change_batch_history(
        self, *, cursor: str | None = None, limit: int = 32
    ) -> MixedBatchHistoryPage:
        return await self.service.workspace_mixed_batch_history(
            self.agent_id, cursor=cursor, limit=limit
        )

    async def wait_mixed_change_batch(
        self, batch_id: str, *, timeout_seconds: float = 30
    ) -> MixedBatchView:
        return await self.service.workspace_wait_mixed_batch(
            self.agent_id, batch_id, timeout_seconds=timeout_seconds
        )

    async def cancel_mixed_change_batch(self, batch_id: str) -> MixedBatchView:
        return await self.service.workspace_cancel_mixed_batch(self.agent_id, batch_id)

    async def file_change_batch(self, batch_id: str) -> NativeBatchView:
        return await self.service.workspace_batch(self.agent_id, batch_id)

    async def file_change_batch_history(
        self, *, cursor: str | None = None, limit: int = 32
    ) -> NativeBatchHistoryPage:
        """发现原批次；历史翻页不会执行或接管文件操作。"""
        return await self.service.workspace_batch_history(self.agent_id, cursor=cursor, limit=limit)

    async def file_change_batch_by_key(self, request_key: str) -> NativeBatchView | None:
        return await self.service.workspace_batch_by_key(self.agent_id, request_key)

    async def cancel_file_change_batch(self, batch_id: str) -> NativeBatchView:
        return await self.service.workspace_cancel_batch(self.agent_id, batch_id)

    async def wait_file_change_batch(
        self, batch_id: str, *, timeout_seconds: float = 30
    ) -> NativeBatchView:
        return await self.service.workspace_wait_batch(
            self.agent_id, batch_id, timeout_seconds=timeout_seconds
        )

    async def stream(self, *, run_id: str | None = None) -> StreamTextResult:
        from .text import StreamTextResult

        runtime, state = await self.service.stream_state(self.agent_id, 0, run_id)
        return StreamTextResult(runtime, state.run)

    async def events(
        self, from_seq: int = 0, *, run_id: str | None = None
    ) -> AsyncIterator[StreamFrame]:
        runtime, state = await self.service.stream_state(self.agent_id, from_seq, run_id)
        subscription = runtime.subscribe(state, from_seq)
        try:
            async for frame in subscription:
                if frame is not None:
                    yield frame
        finally:
            await subscription.aclose()

    async def stop(self) -> AgentView:
        return await self.service.stop(self.agent_id)

    async def close(self, *, expected_revision: int | None = None) -> AgentSessionView:
        revision = (
            expected_revision if expected_revision is not None else (await self.state()).revision
        )
        return await self.service.close_session(self.agent_id, revision)

    async def reopen(self, *, expected_revision: int | None = None) -> AgentSessionView:
        revision = (
            expected_revision if expected_revision is not None else (await self.state()).revision
        )
        return await self.service.reopen_session(self.agent_id, revision)

    async def restore(self) -> AgentView:
        return await self.service.restore(self.agent_id)

    async def destroy(self) -> AgentView:
        return await self.service.destroy(self.agent_id)
