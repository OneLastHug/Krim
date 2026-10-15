"""Agent 目录与当前进程管理 API；不把 callable/provider 序列化到 HTTP。"""

from __future__ import annotations

import asyncio
import copy
import json
import math
import time
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from dataclasses import replace
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import Field, field_validator

from krim_agent.agents import (
    AgentCatalog,
    AgentDefinition,
    AgentDescriptor,
    AgentError,
    AgentHandle,
    AgentManager,
    AgentProfile,
    AgentProfileUpdate,
    AgentProfileVersion,
    AgentResourceRegistry,
    AgentResourceView,
    ExecResult,
)
from krim_agent.agents.permissions import profile_policy
from krim_agent.agents.sessions import AgentSessionView, AgentTurnView
from krim_agent.application.workspace_batches import WorkspaceBatchService
from krim_agent.application.workspace_checkpoints import WorkspaceCheckpointService
from krim_agent.application.workspace_mixed_batches import MixedBatchService
from krim_agent.application.workspace_model_checkpoints import ModelCheckpointService
from krim_agent.application.workspace_model_restores import ModelRestoreService
from krim_agent.application.workspace_restores import WorkspaceRestoreService
from krim_agent.application.workspace_trees import WorkspaceTreeService
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.execution.agent_models import (
    AgentCommandRequest,
    CommandLookup,
    CommandRead,
    CommandSlice,
    CommandSummary,
)
from krim_agent.execution.binding import COMMAND_TOOL_NAMES, ExecutionBinding
from krim_agent.execution.directory_publication import (
    CommandDirectoryChangesPage,
    CommandDirectoryChangesRequest,
    CommandDirectoryProposalRequest,
    CommandDirectoryProposalView,
)
from krim_agent.execution.linux import LinuxSandboxRunner
from krim_agent.execution.management import (
    CommandAccepted,
    CommandHistoryPage,
    CommandHistoryRequest,
    CommandResourcesView,
)
from krim_agent.execution.publication import (
    CommandChangesPage,
    CommandChangesRequest,
    CommandProposalRequest,
    CommandProposalView,
)
from krim_agent.execution.records import CommandView
from krim_agent.generated_tree_undo import (
    GeneratedTreeUndoAction,
    GeneratedTreeUndoAttemptView,
    GeneratedTreeUndoCancelRequest,
    GeneratedTreeUndoPreviewRequest,
    GeneratedTreeUndoPreviewView,
    GeneratedTreeUndoQueryResult,
)
from krim_agent.persistence.agent_profiles import AgentProfileStore
from krim_agent.persistence.agent_sessions import assert_session_open, guard_agent_topic
from krim_agent.persistence.completed_tree_origins import (
    CompletedTreeOriginLookupView,
    CompletedTreeOriginView,
)
from krim_agent.persistence.workspace_batches import NativeBatchStore
from krim_agent.persistence.workspace_checkpoints import WorkspaceCheckpointStore
from krim_agent.persistence.workspace_list import NativeChangeListStore
from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.persistence.workspace_restores import WorkspaceRestoreStore
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.runtime.chat import RunState
from krim_agent.task_state import TaskKind, TaskMutation, TaskOperationView, TaskSnapshot
from krim_agent.tools import ToolContext, ToolError
from krim_agent.tools.checkpoint_capture import checkpoint_capture_tool
from krim_agent.tools.core import ToolDefinition, ToolResult
from krim_agent.tools.directory_changes import directory_change_tools
from krim_agent.tools.generated_tree_undo import generated_tree_undo_tools
from krim_agent.tools.tree_exchanges import tree_exchange_tools
from krim_agent.tools.tree_moves import tree_move_tools
from krim_agent.tools.workspace import ListFilesInput, ReadFileInput, SearchFilesInput, workspace_io
from krim_agent.tools.workspace_changes import workspace_change_tools
from krim_agent.tools.workspace_mixed_batches import mixed_batch_start_tool
from krim_agent.tools.workspace_mixed_queries import mixed_batch_query_tool
from krim_agent.tools.workspace_model_trees import model_tree_tools
from krim_agent.tools.workspace_trees import tree_prepare_tool, tree_query_tool
from krim_agent.workspace import (
    WORKSPACE_TOOL_NAMES,
    WORKSPACE_WRITE_TOOL_NAMES,
    Workspace,
    WorkspaceBinding,
    WorkspaceFile,
    WorkspaceListing,
    WorkspaceSearch,
    WorkspaceView,
)
from krim_agent.workspace_change import (
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
    WorkspaceOperationProofView,
    WorkspaceOperationRequest,
    WorkspacePreviewRequest,
)
from krim_agent.workspace_checkpoint import (
    CheckpointCaptureFence,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointView,
)
from krim_agent.workspace_directory import (
    ArtifactDirectorySpec,
    DirectoryAttemptView,
    DirectoryChangeAction,
    DirectoryInspectRequest,
    DirectoryInspectView,
    DirectoryOperationRequest,
    DirectoryPreviewRequest,
    DirectoryPreviewView,
    DirectoryQueryRequest,
    DirectoryQueryResult,
    WorkspaceDirectoryWriter,
)
from krim_agent.workspace_list import (
    NativeBatchFence,
    NativeBatchHistoryPage,
    NativeBatchRequest,
    NativeBatchView,
    NativeChangePage,
    NativeListFilter,
)
from krim_agent.workspace_mixed_batch import (
    MixedBatchDispatch,
    MixedBatchFence,
    MixedBatchHistoryPage,
    MixedBatchRequest,
    MixedBatchView,
)
from krim_agent.workspace_restore import (
    CheckpointRestoreExecute,
    CheckpointRestoreFence,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)
from krim_agent.workspace_tree import (
    TreePlanDispatch,
    TreePlanFence,
    TreePlanModelSource,
    TreePlanRequest,
    TreePlanStepPage,
    TreePlanView,
)
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeAction,
    TreeExchangeAttemptView,
    TreeExchangeInspectRequest,
    TreeExchangeInspectView,
    TreeExchangeOperationRequest,
    TreeExchangePreviewRequest,
    TreeExchangePreviewView,
    TreeExchangeQueryRequest,
    TreeExchangeQueryResult,
    TreeExchangeSealLookupRequest,
    TreeExchangeSealRequest,
)
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveAttemptView,
    TreeMoveInspectRequest,
    TreeMoveInspectView,
    TreeMoveOperationRequest,
    TreeMovePreviewRequest,
    TreeMovePreviewView,
    TreeMoveQueryRequest,
    TreeMoveQueryResult,
)

_command_launching: ContextVar[asyncio.Task[object] | None] = ContextVar(
    "krim_command_launching", default=None
)

AgentViewStatus = Literal[
    "created", "queued", "running", "completed", "failed", "aborted", "interrupted", "destroyed"
]


class AgentSpawnRequest(StrictModel):
    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    persistent: bool = False
    profile_revision: int | None = Field(default=None, ge=1)


class AgentPromptRequest(StrictModel):
    prompt: str = Field(min_length=1, max_length=8192)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("prompt")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Agent prompt 不能只有空白")
        return value


class AgentView(StrictModel):
    agent_id: str
    session_id: str
    name: str
    topic_id: str
    status: AgentViewStatus
    current_run_id: str | None = None
    turn: int
    persistent: bool
    parent_agent_id: str | None = None
    parent_tool_call_id: str | None = None
    result: ExecResult | None = None
    profile_revision: int | None = None
    session: AgentSessionView


class AgentInventoryView(StrictModel):
    agent_id: str
    name: str
    topic_id: str
    profile_revision: int | None = None
    status: str
    available: bool
    error_code: str | None = None
    current_run_id: str | None = None
    turn: int
    session_id: str | None = None
    session_status: str | None = None
    lifecycle_status: str | None = None


class AgentService:
    """当前进程句柄门面；查询读持久 Run，不修改执行句柄的 current_run 指针。"""

    def __init__(
        self,
        runtime: ChatRuntime,
        settings: Settings,
        *,
        providers: Mapping[str, ModelProvider] | None = None,
    ) -> None:
        self.runtime = runtime
        if (
            providers is not None
            and providers.get("runtime", runtime.provider) is not runtime.provider
        ):
            raise ValueError("runtime资源只能由宿主Runtime管理")
        self.providers = {"runtime": runtime.provider, **dict(providers or {})}
        self.manager = AgentManager(
            runtime.repository,
            max_agents=settings.agent_max_agents,
            max_concurrent=settings.agent_max_concurrent,
            max_recursion_depth=settings.agent_max_recursion_depth,
            tracer=runtime.tracer,
        )
        self.catalog = AgentCatalog(self.manager)
        self.profiles = AgentProfileStore(runtime.repository.database)
        self.sessions = self.manager.sessions
        self.resources = AgentResourceRegistry(self.providers, runtime.tools)
        self.workspace_edits = self.manager.workspace_changes.store
        self.workspace_list_store = NativeChangeListStore(self.workspace_edits)
        self.workspace_batch_store = NativeBatchStore(self.workspace_edits)
        self.workspace_batches = WorkspaceBatchService(
            self.workspace_batch_store,
            self._workspace_batch_authorize,
            self._workspace_batch_child,
        )
        self.workspace_mixed_batch_store = MixedBatchStore(
            self.workspace_edits, self.manager.directory_changes.store
        )
        self.workspace_mixed_batches = MixedBatchService(
            self.workspace_mixed_batch_store,
            self._workspace_batch_authorize,
            self._mixed_batch_child,
        )
        self.workspace_tree_store = WorkspaceTreePlanStore(
            self.workspace_edits, self.manager.directory_changes.store
        )
        self.workspace_trees = WorkspaceTreeService(
            self.workspace_tree_store,
            self._workspace_batch_authorize,
            self._workspace_tree_child,
            prepare=self._prepare_tree,
        )
        self.manager.tree_models.prepare = self._prepare_tree
        self.workspace_checkpoint_store = WorkspaceCheckpointStore(self.workspace_edits)
        self.workspace_checkpoints = WorkspaceCheckpointService(
            self.workspace_checkpoint_store,
            self.manager.workspace_changes,
            self._workspace_checkpoint_authorize,
            self._workspace_checkpoint_read,
            require_bytes=self._workspace_checkpoint_bytes_authorize,
        )
        self.workspace_restore_store = WorkspaceRestoreStore(self.workspace_edits)
        self.model_checkpoints = ModelCheckpointService(self.workspace_checkpoints)
        self.workspace_restores = WorkspaceRestoreService(
            self.workspace_restore_store,
            self.workspace_checkpoints,
            self._workspace_checkpoint_authorize,
            self._workspace_restore_child,
        )
        self.model_checkpoints.restores = ModelRestoreService(
            self.model_checkpoints, self.workspace_restores
        )
        self._results: dict[str, tuple[str | None, int, ExecResult]] = {}
        self.restore_errors: dict[str, str] = {}
        self._tasks: dict[str, tuple[AgentPromptRequest, asyncio.Task[AgentView], str | None]] = {}
        self._workspace_operations: set[asyncio.Task[ToolResult]] = set()
        self._command_launch_lock = asyncio.Lock()
        self._command_launches: dict[
            tuple[str, str], tuple[str, asyncio.Future[str], asyncio.Task[object]]
        ] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False
        self._workspace_recovered = False
        self.catalog.register(
            "default",
            AgentDefinition(
                name="default",
                provider=runtime.provider,
                tools=runtime.tools,
                max_turns=settings.agent_max_turns,
                max_steps=settings.max_steps,
                timeout_seconds=settings.run_timeout_seconds,
                tool_timeout_seconds=settings.tool_timeout_seconds,
                policy=runtime.tool_policy,
                context_config=ContextRunConfig.from_settings(settings),
                usage_budget=settings.agent_usage_budget,
            ),
            description="复用当前本地 Runtime provider 和工具权限的 Agent",
            expose_as_tool=False,
        )

    def _handle(self, agent_id: str) -> AgentHandle:
        handle = self.manager._handles.get(agent_id)
        if handle is None:
            raise ServiceError("agent_not_found", "Agent 句柄不存在或已跨进程失效", 404)
        return handle

    async def view(self, handle: AgentHandle, *, destroyed: bool = False) -> AgentView:
        current = handle.current_run
        turn = handle.turn
        status: AgentViewStatus = (
            "destroyed"
            if destroyed or handle.destroyed
            else "queued"
            if handle.pending_start
            else "created"
        )
        result: ExecResult | None = None
        if current is not None:
            run = await self.runtime.repository.get_run(current.id)
            if not destroyed and not handle.destroyed:
                status = run.status
            if run.status in {"completed", "failed", "aborted", "interrupted"}:
                messages = await self.runtime.repository.run_messages(run.id)
                usage = await self.runtime.repository.usage_view(run.id)
                text = next(
                    (
                        message.content
                        for message in reversed(messages)
                        if message.role == "assistant"
                    ),
                    "",
                )
                result = ExecResult(
                    agent_id=handle.agent_id,
                    run_id=run.id,
                    status=run.status,
                    text=text,
                    error=run.error,
                    code=run.error_code,
                    parent_tool_call_id=handle.parent_tool_call_id,
                    turn=turn,
                    token_usage=usage.token_usage,
                    usage_complete=usage.usage_complete,
                )
        recent = self._results.get(handle.agent_id)
        if (
            recent is not None
            and not handle.pending_start
            and recent[:2] == (current.id if current else None, turn)
            and (result is not None or current is None)
        ):
            result = recent[2].model_copy(deep=True)
            if not destroyed and not handle.destroyed:
                status = result.status
        if handle.pending_start and not destroyed and not handle.destroyed:
            status = "queued"
        background = self._tasks.get(handle.agent_id)
        if (
            background is not None
            and not background[1].done()
            and background[2] == (current.id if current else None)
            and asyncio.current_task() is not background[1]
            and not destroyed
            and not handle.destroyed
        ):
            status = "queued"
        return AgentView(
            agent_id=handle.agent_id,
            session_id=handle.agent_id,
            name=handle.definition.name,
            topic_id=handle.topic_id,
            status=status,
            current_run_id=current.id if current is not None else None,
            turn=turn,
            persistent=handle.definition.persistent,
            parent_agent_id=handle.definition.parent_agent_id,
            parent_tool_call_id=handle.definition.parent_tool_call_id,
            result=result,
            profile_revision=handle.definition.profile_revision,
            session=await handle.session(),
        )

    async def _profile_definition(
        self, name: str, revision: int | None = None, *, restoring: bool = False
    ) -> AgentDefinition:
        if not restoring and not (await self.profiles.get(name)).enabled:
            raise ServiceError("agent_profile_disabled", "Agent配置已停用", 409)
        profile = await self.profiles.get(name, revision)
        return self._definition_from_profile(profile)

    def _definition_from_profile(self, profile: AgentProfileVersion) -> AgentDefinition:
        if not profile.enabled:
            raise ServiceError("agent_profile_disabled", "Agent配置已停用", 409)
        if profile.config.provider_key not in self.resources.providers:
            raise ServiceError("agent_profile_provider", "Agent Provider 未登记", 422)
        definitions = {definition.name: definition for definition in self.runtime.tools.definitions}
        unknown = set(profile.config.tool_names) - set(definitions)
        if unknown:
            raise ServiceError("agent_profile_tool", "Agent配置包含未登记工具", 422)
        tools = self.runtime.tools.select(profile.config.tool_names)
        workspace = self._profile_workspace(profile.config)
        execution = self._profile_execution(profile.config)
        try:
            provider = self.resources.provider(profile.config.provider_key)
        except KeyError:
            raise ServiceError("agent_profile_provider", "Agent Provider已停用", 422) from None
        resource_version = self.resources.fingerprint(
            profile.config.provider_key,
            profile.config.tool_names,
            workspace_key=profile.config.workspace_key,
            execution_key=profile.config.execution_key,
        )
        base = self.catalog.get("default").definition
        host_context = ContextRunConfig.from_settings(self.runtime.settings)
        try:
            selected_context = (
                host_context
                if profile.config.context_config == ContextRunConfig()
                else profile.config.context_config.restricted_by(host_context)
            )
        except ValueError:
            raise ServiceError(
                "agent_profile_context", "Agent与宿主上下文预算不兼容", 422
            ) from None
        return replace(
            base,
            name=profile.config.name,
            provider=provider,
            tools=tools,
            max_turns=profile.config.max_turns,
            max_steps=profile.config.max_steps,
            persistent=profile.config.persistent,
            timeout_seconds=profile.config.timeout_seconds,
            tool_timeout_seconds=profile.config.tool_timeout_seconds,
            context_config=selected_context,
            usage_budget=profile.config.usage_budget,
            profile_revision=profile.revision,
            instructions=profile.config.instructions,
            policy=profile_policy(profile.config.mode, self.runtime.tool_policy),
            resource_version=resource_version,
            resource_manifest=self.resources.manifest(
                profile.config.provider_key,
                profile.config.tool_names,
                workspace_key=profile.config.workspace_key,
                execution_key=profile.config.execution_key,
            ),
            workspace=workspace,
            execution=execution,
            mode=profile.config.mode,
            session_ttl_seconds=profile.config.session_ttl_seconds,
        )

    async def spawn(
        self, name: str, *, persistent: bool = False, profile_revision: int | None = None
    ) -> AgentView:
        try:
            try:
                definition = self._catalog_definition(name)
            except ToolError:
                try:
                    definition = await self._profile_definition(name, profile_revision)
                except ServiceError as error:
                    if error.code == "agent_profile_not_found" and profile_revision is None:
                        raise ServiceError("agent_not_found", "未登记Agent", 404) from None
                    raise
            parent_accounts = await self.runtime.budget_store.bind(
                "runtime",
                self.runtime.settings.usage_budget_scope,
                self.runtime.settings.usage_budget,
            )
            handle = await self.manager.spawn(
                replace(
                    definition,
                    agent_id="",
                    persistent=persistent or definition.persistent,
                ),
                budget_account_ids=parent_accounts,
            )
        except ToolError as exc:
            raise ServiceError(exc.code, str(exc), 404) from exc
        except AgentError as exc:
            raise ServiceError(
                exc.code, str(exc), 429 if exc.code == "agent_limit" else 409
            ) from exc
        return await self.view(handle)

    async def profile_list(self) -> list[AgentProfileVersion]:
        return await self.profiles.list()

    async def profile_get(self, name: str, revision: int | None = None) -> AgentProfileVersion:
        return await self.profiles.get(name, revision)

    async def profile_put(self, request: AgentProfileUpdate) -> AgentProfileVersion:
        if request.config.name in {entry.name for entry in self.catalog.descriptors()}:
            raise ServiceError("agent_profile_reserved", "宿主静态Agent名称不能被配置覆盖", 409)
        if request.config.provider_key not in self.resources.providers:
            raise ServiceError("agent_profile_provider", "Agent Provider未登记", 422)
        try:
            self.resources.provider(request.config.provider_key)
        except KeyError:
            raise ServiceError("agent_profile_provider", "Agent Provider已停用", 422) from None
        available = {definition.name for definition in self.runtime.tools.definitions}
        if set(request.config.tool_names) - available:
            raise ServiceError("agent_profile_tool", "工具未登记", 422)
        self._profile_workspace(request.config)
        self._profile_execution(request.config)
        if request.config.context_config != ContextRunConfig():
            try:
                request.config.context_config.restricted_by(
                    ContextRunConfig.from_settings(self.runtime.settings)
                )
            except ValueError:
                raise ServiceError(
                    "agent_profile_context", "Agent与宿主上下文预算不兼容", 422
                ) from None
        return await self.profiles.put(request)

    def register_provider(
        self,
        key: str,
        provider: ModelProvider,
        *,
        version: str,
        owned: bool = False,
    ) -> None:
        """可信宿主注册或替换 Provider；HTTP/配置不能调用此入口。"""
        if key == "runtime" and provider is not self.runtime.provider:
            raise ValueError("runtime资源须保持宿主Runtime的Provider对象")
        if owned and provider is self.runtime.provider:
            raise ValueError("Runtime自身Provider不能转交所有权")
        self.resources.register_provider(key, provider, version=version, owned=owned)
        self.providers[key] = provider

    def disable_provider(self, key: str) -> None:
        self.resources.disable_provider(key)

    def register_tool_version(self, name: str, version: str) -> None:
        self.resources.register_tool_version(name, version)

    def _profile_workspace(self, config: AgentProfile) -> WorkspaceBinding | None:
        if config.workspace_key is None:
            if set(config.tool_names) & (WORKSPACE_TOOL_NAMES | {"command_propose"}):
                raise ServiceError("agent_profile_workspace", "文件工具需要已登记工作区", 422)
            return None
        try:
            binding = self.resources.workspace(config.workspace_key)
        except KeyError:
            raise ServiceError("agent_profile_workspace", "工作区未登记或已停用", 422) from None
        if (
            set(config.tool_names) & (WORKSPACE_WRITE_TOOL_NAMES | {"command_propose"})
            and not binding.workspace.writable
        ):
            raise ServiceError("workspace_read_only", "只读工作区不可登记修改工具", 422)
        return binding

    def register_workspace(
        self,
        key: str,
        root: str | Path | Workspace,
        *,
        version: str,
        owned: bool = False,
        default: bool = False,
        writable: bool = False,
    ) -> WorkspaceBinding:
        writing = writable or isinstance(root, Workspace) and root.writable
        selected = (
            (*workspace_change_tools(), *directory_change_tools(), tree_prepare_tool())
            if writing
            else tuple(
                item for item in directory_change_tools() if item.name == "directory_inspect"
            )
        )
        existing = {item.name: item for item in self.runtime.tools.definitions}
        for definition in selected:
            if definition.name in existing and existing[definition.name] is not definition:
                raise ValueError("工作区目录工具名称已由其它定义占用")
        binding = self.resources.register_workspace(
            key, root, version=version, owned=owned, writable=writable
        )
        for definition in selected:
            if definition.name not in existing:
                self.runtime.tools.register(definition)
        if default:
            self.runtime.workspace = binding
        return binding

    def register_mixed_batch_queries(self) -> None:
        """可信宿主显式启用新只读工具；不改变既有冻结Agent的工具集合。"""
        definition = mixed_batch_query_tool()
        existing = next(
            (item for item in self.runtime.tools.definitions if item.name == definition.name), None
        )
        if existing is not None and existing is not definition:
            raise ValueError("混合查询工具名称已由其它定义占用")
        if existing is None:
            self.runtime.tools.register(definition)

    def register_checkpoint_bytes(self) -> None:
        """显式登记字节元数据工具；旧冻结Agent不自动提升能力。"""
        definition = checkpoint_capture_tool()
        existing = next(
            (item for item in self.runtime.tools.definitions if item.name == definition.name), None
        )
        if existing is not None and existing is not definition:
            raise ValueError("字节捕获工具名称已由其它定义占用")
        if existing is None:
            self.runtime.tools.register(definition)

    def register_mixed_batch_writes(self) -> None:
        """可信宿主显式启用模型混合批次写工具；权限仍由原ToolRegistry裁决。"""
        definition = mixed_batch_start_tool()
        existing = next(
            (item for item in self.runtime.tools.definitions if item.name == definition.name), None
        )
        if existing is not None and existing is not definition:
            raise ValueError("混合批次写工具名称已由其它定义占用")
        if existing is None:
            self.runtime.tools.register(definition)

    def register_tree_queries(self) -> None:
        """显式启用原目录树只读工具；既有Agent冻结定义不静默扩大。"""
        definition = tree_query_tool()
        existing = next(
            (item for item in self.runtime.tools.definitions if item.name == definition.name), None
        )
        if existing is not None and existing is not definition:
            raise ValueError("目录树查询工具名称已由其它定义占用")
        if existing is None:
            self.runtime.tools.register(definition)

    def disable_workspace(self, key: str) -> None:
        self.resources.disable_workspace(key)

    def register_tree_models(self) -> None:
        """显式新增模型准备/启动工具；保留既有冻结工具与host准备语义。"""
        definitions = model_tree_tools()
        existing = {item.name: item for item in self.runtime.tools.definitions}
        if any(item.name in existing and existing[item.name] is not item for item in definitions):
            raise ValueError("模型目录树工具名称已被其它定义占用")
        for definition in definitions:
            if definition.name not in existing:
                self.runtime.tools.register(definition)

    def register_checkpoint_controls(self) -> None:
        from krim_agent.tools.workspace_checkpoints import checkpoint_control_tools

        definitions = checkpoint_control_tools()
        existing = {item.name: item for item in self.runtime.tools.definitions}
        if any(item.name in existing and existing[item.name] is not item for item in definitions):
            raise ValueError("模型检查点控制工具名称已被其它定义占用")
        for definition in definitions:
            if definition.name not in existing:
                self.runtime.tools.register(definition)
        self.manager.workspace_checkpoints = self.workspace_checkpoints
        self.manager.workspace_checkpoint_controls = self.model_checkpoints

    def register_checkpoint_restore_controls(self) -> None:
        from krim_agent.tools.workspace_checkpoints import checkpoint_restore_control_tools

        definitions = checkpoint_restore_control_tools()
        existing = {item.name: item for item in self.runtime.tools.definitions}
        if any(item.name in existing and existing[item.name] is not item for item in definitions):
            raise ValueError("模型检查点恢复工具名称已被其它定义占用")
        for definition in definitions:
            if definition.name not in existing:
                self.runtime.tools.register(definition)
        self.manager.workspace_checkpoints = self.workspace_checkpoints
        self.manager.workspace_checkpoint_controls = self.model_checkpoints

    def register_tree_moves(self) -> None:
        """显式启用非空树工具；全量冲突检查，不扩大既有Agent冻结集合。"""
        definitions = tree_move_tools()
        existing = {item.name: item for item in self.runtime.tools.definitions}
        if any(item.name in existing and existing[item.name] is not item for item in definitions):
            raise ValueError("非空树移动工具名称已被其它定义占用")
        for definition in definitions:
            if definition.name not in existing:
                self.runtime.tools.register(definition)

    def register_tree_exchanges(self) -> None:
        """显式启用双完整树工具；全量冲突检查，不扩大既有Agent冻结集合。"""
        definitions = tree_exchange_tools()
        existing = {item.name: item for item in self.runtime.tools.definitions}
        if any(item.name in existing and existing[item.name] is not item for item in definitions):
            raise ValueError("双完整树交换工具名称已被其它定义占用")
        for definition in definitions:
            if definition.name not in existing:
                self.runtime.tools.register(definition)

    def register_generated_tree_undos(self) -> None:
        """显式登记原根撤销；整组冲突先拒绝，不扩大原冻结Agent工具。"""
        definitions = generated_tree_undo_tools()
        existing = {item.name: item for item in self.runtime.tools.definitions}
        if any(item.name in existing and existing[item.name] is not item for item in definitions):
            raise ValueError("原生成根撤销工具名称已被其它定义占用")
        for definition in definitions:
            if definition.name not in existing:
                self.runtime.tools.register(definition)

    def register_execution(
        self, key: str, runner: LinuxSandboxRunner, *, version: str, owned: bool = False
    ) -> ExecutionBinding:
        return self.resources.register_execution(key, runner, version=version, owned=owned)

    def disable_execution(self, key: str) -> None:
        self.resources.disable_execution(key)

    def _profile_execution(self, config: AgentProfile) -> ExecutionBinding | None:
        if config.execution_key is None:
            if set(config.tool_names) & COMMAND_TOOL_NAMES:
                raise ServiceError("agent_profile_execution", "命令工具需要显式执行资源授权", 422)
            return None
        try:
            return self.resources.execution(config.execution_key)
        except KeyError:
            raise ServiceError("agent_profile_execution", "执行资源未登记或已停用", 422) from None

    def resource_inventory(self) -> list[AgentResourceView]:
        return self.resources.views()

    async def definition(self, name: str, revision: int | None = None) -> AgentDefinition:
        if revision is None:
            try:
                return self._catalog_definition(name)
            except ToolError:
                pass
        return await self._profile_definition(name, revision)

    def _catalog_definition(self, name: str) -> AgentDefinition:
        definition = self.catalog.get(name).definition
        if name != "default":
            return definition
        workspace = self.runtime.workspace
        names = [
            tool.name
            for tool in definition.tools.definitions
            if tool.name not in WORKSPACE_TOOL_NAMES or workspace is not None
            if tool.name not in COMMAND_TOOL_NAMES or definition.execution is not None
            if tool.name not in WORKSPACE_WRITE_TOOL_NAMES
            or (workspace is not None and workspace.workspace.writable)
        ]
        try:
            manifest = self.resources.manifest(
                "runtime", names, workspace_key=workspace.key if workspace else None
            )
        except KeyError:
            raise ServiceError(
                "agent_profile_workspace" if workspace else "agent_profile_provider",
                "Runtime资源已停用",
                422,
            ) from None
        version = (
            self.resources.fingerprint(
                "runtime", names, workspace_key=workspace.key if workspace else None
            )
            if "runtime" in self.resources.provider_versions
            or any(tool in self.resources.tool_versions for tool in names)
            or workspace is not None
            else definition.resource_version
        )
        return replace(
            definition,
            tools=definition.tools.select(names),
            workspace=workspace,
            resource_version=version,
            resource_manifest=manifest,
        )

    async def workspace_info(self, agent_id: str) -> WorkspaceView:
        handle = self._handle(agent_id)
        binding = handle.definition.workspace
        if binding is None:
            raise ServiceError("workspace_unbound", "Agent没有工作区授权", 409)
        names = sorted(
            item.name
            for item in handle.runtime.tools.definitions
            if item.name in WORKSPACE_TOOL_NAMES
        )
        view = binding.view(names)
        session = await handle.session()
        return view.model_copy(update={"available": view.available and session.status == "open"})

    async def recover_workspace_edits(self) -> int:
        """启动恢复只处理过期租约，不接管其它进程的活跃文件动作。"""
        if self._workspace_recovered:
            return 0
        recovered = await self.workspace_edits.recover_interrupted()
        recovered += await self.workspace_batch_store.recover_interrupted()
        recovered += await self.workspace_mixed_batch_store.recover_interrupted()
        recovered += await self.workspace_restore_store.recover_interrupted()
        recovered += await self.manager.tree_moves.store.recover_interrupted()
        recovered += await self.manager.tree_exchanges.store.recover_interrupted()
        self._workspace_recovered = True
        return recovered

    async def _workspace_call(
        self,
        agent_id: str,
        name: str,
        arguments: dict[str, object],
        *,
        confirmed: bool = False,
        batch_fence: NativeBatchFence | None = None,
        mixed_fence: MixedBatchFence | None = None,
        tree_fence: TreePlanFence | None = None,
        checkpoint_capture: CheckpointCaptureFence | None = None,
        checkpoint_restore: CheckpointRestoreFence | None = None,
        command_admitted: Callable[[str], None] | None = None,
    ) -> object:
        if self._closed:
            raise ServiceError("agent_service_closed", "Agent服务已关闭", 503)
        handle = self._handle(agent_id)
        async with self.runtime.repository.database.write_lock:
            async with self.runtime.repository.database.sessions() as session:
                session_row = await guard_agent_topic(
                    session,
                    handle.topic_id,
                    handle.session_id,
                    handle.agent_id,
                    self.manager.owner_id,
                )
                if session_row is None:
                    raise ServiceError("agent_session_topic", "Agent工作区缺少会话绑定", 403)
                if name not in {
                    "command_get",
                    "command_read",
                    "command_changes",
                    "command_history",
                    "command_inspect",
                    "command_resources",
                    "directory_query",
                    "directory_source_query",
                    "tree_move_query",
                    "tree_exchange_query",
                    "tree_exchange_seal_query",
                    "generated_tree_undo_query",
                }:
                    await assert_session_open(session, session_row)
        if self._closed:
            raise ServiceError("agent_service_closed", "Agent服务已关闭", 503)
        if len(self._workspace_operations) >= 64:
            raise ServiceError("workspace_busy", "工作区任务已达上限，请稍后重试", 429)
        call_id = str(uuid4())
        frozen_arguments = copy.deepcopy(arguments)
        registry = handle.runtime.tools

        async def confirm(definition: ToolDefinition, context: ToolContext) -> bool:
            command_paths = frozen_arguments.get("input_paths")
            if (
                confirmed
                and name == "command_execute"
                and definition.name == "workspace_read"
                and context.origin == "host"
                and context.command_capture is not None
                and context.command_capture.parent_call_id == call_id
                and isinstance(command_paths, list)
                and context.command_capture.path in command_paths
                and context.arguments
                == ReadFileInput(path=context.command_capture.path).model_dump()
            ):
                return True
            return (
                confirmed
                and context.origin == "host"
                and definition.name == name
                and context.call_id == call_id
                and context.arguments == frozen_arguments
            )

        operation = asyncio.create_task(
            registry.invoke(
                name,
                arguments,
                ToolContext(
                    registry,
                    topic_id=handle.topic_id,
                    agent_id=handle.agent_id,
                    session_id=handle.session_id,
                    workspace=handle.definition.workspace,
                    execution=handle.definition.execution,
                    commands=self.manager.commands,
                    command_admitted=command_admitted,
                    _command_expected=json.dumps(
                        AgentCommandRequest.model_validate(arguments).model_dump(mode="json"),
                        sort_keys=True,
                    )
                    if command_admitted is not None
                    else None,
                    workspace_changes=self.manager.workspace_changes,
                    directory_changes=self.manager.directory_changes,
                    workspace_mixed_queries=self.manager.mixed_queries,
                    workspace_tree_queries=self.manager.tree_queries,
                    workspace_tree_models=self.manager.tree_models,
                    tree_moves=self.manager.tree_moves,
                    tree_exchanges=self.manager.tree_exchanges,
                    workspace_mixed_batches=self.manager.mixed_batch_writer,
                    workspace_batch=batch_fence,
                    mixed_batch=mixed_fence,
                    tree_fence=tree_fence,
                    workspace_trees=self.workspace_trees,
                    checkpoint_capture=checkpoint_capture,
                    checkpoint_restore=checkpoint_restore,
                    workspace_checkpoints=self.workspace_checkpoints,
                    call_id=call_id,
                    approval_handler=confirm if confirmed else None,
                    policy=handle.runtime.tool_policy,
                    timeout=handle.definition.tool_timeout_seconds,
                    origin="host",
                    tracer=handle.runtime.tracer,
                    trace_owner_kind="host",
                    trace_owner_id=handle.agent_id,
                ),
            )
        )
        self._workspace_operations.add(operation)
        try:
            result = await operation
            if result.is_error:
                raise ToolError(result.code or "execution_error", result.content[0].text)
            return result.data
        except ToolError as error:
            status = {
                "tool_not_found": 403,
                "permission_denied": 403,
                "approval_required": 409,
                "workspace_unbound": 409,
                "workspace_tree_exchange_context": 403,
                "workspace_tree_exchange_model_context": 403,
                "workspace_tree_exchange_source_conflict": 403,
                "workspace_tree_exchange_closed": 503,
                "workspace_tree_exchange_backend_missing": 503,
                "workspace_tree_exchange_busy": 429,
                "workspace_tree_exchange_limit": 429,
                "workspace_tree_exchange_snapshot_limit": 413,
                "workspace_tree_exchange_not_found": 404,
                "workspace_tree_exchange_operation_not_found": 404,
                "workspace_tree_exchange_unverified": 409,
                "workspace_tree_exchange_self_close": 409,
                "workspace_tree_exchange_page": 422,
                "workspace_tree_seal_limit": 413,
                "workspace_tree_seal_not_found": 404,
                "workspace_tree_origin_incomplete": 409,
                "workspace_tree_exchange_origin_required": 409,
                "workspace_tree_exchange_origin_conflict": 409,
                "workspace_tree_exchange_conflict": 409,
                "workspace_tree_exchange_idempotency_conflict": 409,
                "workspace_tree_exchange_state": 409,
                "workspace_tree_exchange_active": 409,
                "workspace_tree_move_context": 403,
                "workspace_generated_undo_backend_missing": 503,
                "workspace_generated_undo_context": 403,
                "workspace_generated_undo_source": 403,
                "workspace_generated_undo_deadline": 409,
                "workspace_generated_undo_required": 409,
                "workspace_generated_undo_seal_required": 409,
                "workspace_generated_undo_source_conflict": 409,
                "workspace_generated_undo_version_conflict": 409,
                "workspace_generated_undo_restore_conflict": 409,
                "workspace_generated_undo_operation": 422,
                "workspace_tree_move_model_context": 403,
                "workspace_tree_move_source_conflict": 403,
                "workspace_tree_move_closed": 503,
                "workspace_tree_move_backend_missing": 503,
                "workspace_tree_move_busy": 429,
                "workspace_tree_move_limit": 429,
                "workspace_tree_move_operation_limit": 429,
                "workspace_tree_move_bytes": 429,
                "workspace_tree_move_not_found": 404,
                "workspace_tree_move_operation_not_found": 404,
                "workspace_tree_move_unverified": 409,
                "workspace_tree_move_self_close": 409,
                "workspace_tree_snapshot_device": 403,
                "workspace_tree_snapshot_depth": 413,
                "workspace_tree_snapshot_limit": 413,
                "workspace_tree_snapshot_bytes": 413,
                "workspace_tree_move_page": 422,
                "workspace_tree_move_path": 422,
                "workspace_tree_move_conflict": 409,
                "workspace_tree_move_idempotency_conflict": 409,
                "workspace_tree_move_state": 409,
                "workspace_tree_move_active": 409,
                "workspace_unavailable": 403,
                "invalid_input": 422,
                "workspace_path": 422,
                "workspace_sensitive": 403,
                "workspace_file_type": 422,
                "workspace_not_found": 404,
                "workspace_access": 403,
                "workspace_link": 403,
                "workspace_file_limit": 413,
                "workspace_binary": 422,
                "workspace_encoding": 422,
                "workspace_changed": 409,
                "workspace_closed": 503,
                "timeout": 504,
                "workspace_read_only": 403,
                "workspace_conflict": 409,
                "workspace_replace_conflict": 409,
                "workspace_revision_conflict": 409,
                "workspace_idempotency_conflict": 409,
                "workspace_file_busy": 409,
                "workspace_operation_active": 409,
                "workspace_cleanup_state": 409,
                "workspace_change_state": 409,
                "workspace_effect_corrupt": 409,
                "workspace_keep_witness_corrupt": 409,
                "workspace_snapshot_corrupt": 409,
                "workspace_batch_action": 403,
                "workspace_batch_fence_mismatch": 403,
                "workspace_batch_request_reserved": 403,
                "workspace_batch_fenced": 409,
                "workspace_batch_child_closed": 409,
                "workspace_batch_operation_conflict": 409,
                "workspace_batch_dependency": 409,
                "workspace_mixed_batch_action": 403,
                "workspace_mixed_batch_fence_mismatch": 403,
                "workspace_mixed_batch_request_reserved": 403,
                "workspace_mixed_batch_fenced": 409,
                "workspace_mixed_batch_child_closed": 409,
                "workspace_mixed_batch_operation_conflict": 409,
                "workspace_mixed_batch_corrupt": 409,
                "workspace_tree_action": 403,
                "workspace_tree_model_unbound": 403,
                "workspace_tree_model_source_conflict": 403,
                "workspace_tree_model_run_mismatch": 403,
                "workspace_tree_model_run_closed": 409,
                "workspace_tree_model_call_conflict": 409,
                "workspace_tree_model_context": 403,
                "workspace_tree_model_closed": 503,
                "workspace_tree_model_unverified": 409,
                "workspace_tree_model_reentrant": 409,
                "workspace_tree_fence_mismatch": 403,
                "workspace_tree_request_reserved": 403,
                "workspace_tree_resource_conflict": 403,
                "workspace_tree_source_conflict": 409,
                "workspace_tree_fenced": 409,
                "workspace_tree_step_closed": 409,
                "workspace_tree_parent_unresolved": 409,
                "workspace_tree_parent_mismatch": 409,
                "workspace_tree_proposal_conflict": 409,
                "workspace_tree_operation_conflict": 409,
                "workspace_tree_corrupt": 409,
                "workspace_tree_idempotency_conflict": 409,
                "workspace_tree_backend_missing": 503,
                "workspace_tree_prepare_unverified": 409,
                "workspace_tree_query_unverified": 409,
                "workspace_tree_query_mismatch": 403,
                "workspace_tree_root_not_found": 404,
                "workspace_tree_existing_input": 409,
                "workspace_tree_step_limit": 409,
                "workspace_tree_metadata_limit": 409,
                "workspace_tree_limit": 429,
                "workspace_tree_service_closed": 503,
                "workspace_checkpoint_fenced": 409,
                "workspace_checkpoint_target": 403,
                "workspace_checkpoint_action": 403,
                "workspace_checkpoint_not_captured": 409,
                "workspace_checkpoint_changed": 409,
                "workspace_checkpoint_content": 422,
                "workspace_checkpoint_unavailable": 503,
                "workspace_checkpoint_closed": 503,
                "workspace_checkpoint_bytes": 429,
                "workspace_proposal_expired": 410,
                "workspace_receipt_pending": 503,
                "workspace_busy": 429,
                "workspace_run_closed": 409,
                "workspace_binding_changed": 403,
                "workspace_proposal_not_found": 404,
                "workspace_operation_not_found": 404,
                "workspace_stage_changed": 409,
                "workspace_parent_changed": 409,
                "workspace_write_platform": 503,
                "workspace_proposal_limit": 429,
                "workspace_operation_limit": 429,
                "workspace_snapshot_limit": 429,
                "agent_session_owner": 409,
                "agent_lease_lost": 409,
                "agent_session_closed": 410,
                "agent_session_expired": 410,
                "agent_session_destroyed": 410,
                "agent_parent_session_closed": 409,
                "execution_unbound": 409,
                "execution_unavailable": 403,
                "execution_binding_changed": 403,
                "execution_key_conflict": 409,
                "execution_input_target": 403,
                "execution_input_context": 403,
                "execution_input_not_captured": 403,
                "execution_input_tool": 403,
                "execution_input_file": 422,
                "execution_input_limit": 413,
                "execution_input_unavailable": 503,
                "execution_run_mismatch": 403,
                "execution_run_closed": 409,
                "execution_not_found": 404,
                "execution_file_not_found": 404,
                "execution_receipt_unverified": 409,
                "execution_storage_limit": 429,
                "execution_busy": 429,
                "execution_command_reentrant": 409,
                "execution_service_reentrant": 409,
                "execution_artifacts_incomplete": 409,
                "execution_source_unbound": 409,
                "execution_source_corrupt": 409,
                "execution_failed_artifacts": 409,
                "execution_destination_exists": 409,
                "execution_artifact_baseline": 409,
                "execution_artifact_mode": 422,
                "execution_directories_unrecorded": 409,
                "execution_directory_not_found": 404,
                "execution_proposal_source": 409,
                "execution_proposal_unavailable": 503,
                "workspace_directory_not_found": 404,
                "workspace_directory_operation_not_found": 404,
                "workspace_directory_closed": 503,
                "workspace_directory_backend_missing": 503,
                "workspace_directory_busy": 409,
                "workspace_directory_conflict": 409,
                "workspace_directory_proposal": 409,
                "workspace_directory_state": 409,
                "workspace_directory_lease_lost": 409,
                "workspace_directory_receipt_unverified": 409,
                "workspace_directory_active": 409,
                "workspace_directory_page": 422,
                "workspace_directory_type": 422,
                "workspace_directory_mode": 422,
                "workspace_directory_nonempty": 409,
                "workspace_directory_limit": 429,
                "workspace_directory_authority": 409,
                "workspace_directory_intent": 503,
                "workspace_directory_operation_mismatch": 422,
                "workspace_directory_operation": 422,
                "workspace_directory_corrupt": 409,
                "workspace_directory_self_close": 409,
                "workspace_directory_changed": 409,
                "workspace_directory_access": 403,
                "workspace_directory_exists": 409,
            }.get(error.code, 502)
            raise ServiceError(error.code, str(error), status) from error
        finally:
            self._workspace_operations.discard(operation)

    async def workspace_read(self, agent_id: str, request: ReadFileInput) -> WorkspaceFile:
        return WorkspaceFile.model_validate(
            await self._workspace_call(agent_id, "workspace_read", request.model_dump())
        )

    async def tree_move_inspect(
        self, agent_id: str, request: TreeMoveInspectRequest, *, confirmed: bool = False
    ) -> TreeMoveInspectView:
        return TreeMoveInspectView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_move_inspect",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_move_preview(
        self, agent_id: str, request: TreeMovePreviewRequest, *, confirmed: bool = False
    ) -> TreeMovePreviewView:
        return TreeMovePreviewView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_move_preview",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_move_change(
        self, agent_id: str, request: TreeMoveAction, *, confirmed: bool = False
    ) -> TreeMoveAttemptView:
        return TreeMoveAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_move_change",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_move_query(
        self, agent_id: str, request: TreeMoveQueryRequest, *, confirmed: bool = False
    ) -> TreeMoveQueryResult:
        return TreeMoveQueryResult.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_move_query",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_move_reconcile(
        self, agent_id: str, request: TreeMoveOperationRequest, *, confirmed: bool = False
    ) -> TreeMoveAttemptView:
        return TreeMoveAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_move_reconcile",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_move_cancel(self, agent_id: str, proposal_id: str) -> TreeMovePreviewView:
        session = await self.session(agent_id)
        return await self.manager.tree_moves.cancel(f"session:{session.session_id}", proposal_id)

    async def generated_tree_undo_preview(
        self, agent_id: str, request: GeneratedTreeUndoPreviewRequest, *, confirmed: bool = False
    ) -> GeneratedTreeUndoPreviewView:
        return GeneratedTreeUndoPreviewView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "generated_tree_undo_preview",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def generated_tree_undo_change(
        self, agent_id: str, request: GeneratedTreeUndoAction, *, confirmed: bool = False
    ) -> GeneratedTreeUndoAttemptView:
        return GeneratedTreeUndoAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "generated_tree_undo_change",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def generated_tree_undo_query(
        self, agent_id: str, request: TreeMoveQueryRequest, *, confirmed: bool = False
    ) -> GeneratedTreeUndoQueryResult:
        return GeneratedTreeUndoQueryResult.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "generated_tree_undo_query", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def generated_tree_undo_reconcile(
        self, agent_id: str, request: TreeMoveOperationRequest, *, confirmed: bool = False
    ) -> GeneratedTreeUndoAttemptView:
        return GeneratedTreeUndoAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "generated_tree_undo_reconcile",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def generated_tree_undo_cancel(
        self, agent_id: str, request: GeneratedTreeUndoCancelRequest, *, confirmed: bool = False
    ) -> GeneratedTreeUndoPreviewView:
        return GeneratedTreeUndoPreviewView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "generated_tree_undo_cancel",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_exchange_inspect(
        self, agent_id: str, request: TreeExchangeInspectRequest, *, confirmed: bool = False
    ) -> TreeExchangeInspectView:
        return TreeExchangeInspectView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_exchange_inspect",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_exchange_preview(
        self, agent_id: str, request: TreeExchangePreviewRequest, *, confirmed: bool = False
    ) -> TreeExchangePreviewView:
        return TreeExchangePreviewView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_exchange_preview",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_exchange_change(
        self, agent_id: str, request: TreeExchangeAction, *, confirmed: bool = False
    ) -> TreeExchangeAttemptView:
        return TreeExchangeAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_exchange_change",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_exchange_query(
        self, agent_id: str, request: TreeExchangeQueryRequest, *, confirmed: bool = False
    ) -> TreeExchangeQueryResult:
        return TreeExchangeQueryResult.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_exchange_query",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_exchange_reconcile(
        self, agent_id: str, request: TreeExchangeOperationRequest, *, confirmed: bool = False
    ) -> TreeExchangeAttemptView:
        return TreeExchangeAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_exchange_reconcile",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_exchange_cancel(
        self, agent_id: str, proposal_id: str
    ) -> TreeExchangePreviewView:
        session = await self.session(agent_id)
        return await self.manager.tree_exchanges.cancel(
            f"session:{session.session_id}", proposal_id
        )

    async def tree_exchange_seal(
        self, agent_id: str, request: TreeExchangeSealRequest, *, confirmed: bool = False
    ) -> CompletedTreeOriginView:
        return CompletedTreeOriginView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "tree_exchange_seal",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def tree_exchange_seal_query(
        self, agent_id: str, request: TreeExchangeSealLookupRequest, *, confirmed: bool = False
    ) -> CompletedTreeOriginLookupView:
        return CompletedTreeOriginLookupView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "tree_exchange_seal_query", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def directory_inspect(
        self, agent_id: str, request: DirectoryInspectRequest, *, confirmed: bool = False
    ) -> DirectoryInspectView:
        return DirectoryInspectView.model_validate(
            await self._workspace_call(
                agent_id, "directory_inspect", request.model_dump(), confirmed=confirmed
            )
        )

    async def directory_preview(
        self, agent_id: str, request: DirectoryPreviewRequest, *, confirmed: bool = False
    ) -> DirectoryPreviewView:
        return DirectoryPreviewView.model_validate(
            await self._workspace_call(
                agent_id, "directory_preview", request.model_dump(), confirmed=confirmed
            )
        )

    async def directory_change(
        self, agent_id: str, request: DirectoryChangeAction, *, confirmed: bool = False
    ) -> DirectoryAttemptView:
        return DirectoryAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "directory_change", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def directory_reconcile(
        self, agent_id: str, request: DirectoryOperationRequest, *, confirmed: bool = False
    ) -> DirectoryAttemptView:
        return DirectoryAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "directory_reconcile", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def directory_finalize(
        self, agent_id: str, request: DirectoryOperationRequest, *, confirmed: bool = False
    ) -> DirectoryAttemptView:
        return DirectoryAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "directory_finalize", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def directory_query(
        self, agent_id: str, request: DirectoryQueryRequest, *, confirmed: bool = False
    ) -> DirectoryQueryResult:
        return DirectoryQueryResult.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "directory_query", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def command_execute(
        self, agent_id: str, request: AgentCommandRequest, *, confirmed: bool = False
    ) -> CommandSummary:
        return CommandSummary.model_validate(
            await self._workspace_call(
                agent_id, "command_execute", request.model_dump(), confirmed=confirmed
            )
        )

    async def command_get(
        self, agent_id: str, request: CommandLookup, *, confirmed: bool = False
    ) -> CommandSummary:
        return CommandSummary.model_validate(
            await self._workspace_call(
                agent_id, "command_get", request.model_dump(), confirmed=confirmed
            )
        )

    async def command_start(
        self, agent_id: str, request: AgentCommandRequest, *, confirmed: bool = False
    ) -> CommandAccepted:
        self.assert_external_command_control()
        request = AgentCommandRequest.model_validate(request.model_dump())
        key = (agent_id, request.request_key)
        signature = json.dumps(
            {"request": request.model_dump(mode="json"), "confirmed": confirmed}, sort_keys=True
        )
        async with self._command_launch_lock:
            if self._closed:
                raise ServiceError("agent_service_closed", "Agent服务已关闭", 503)
            entry = self._command_launches.get(key)
            if entry is not None and entry[2].done() and not entry[1].done():
                self._command_launches.pop(key, None)
                entry = None
            if entry is not None and entry[0] != signature:
                raise ServiceError(
                    "execution_request_conflict", "同一请求键正在处理不同参数或确认", 409
                )
            if entry is None:
                if len(self._command_launches) >= 64:
                    raise ServiceError("execution_busy", "后台命令准入已达上限", 429)
                admitted: asyncio.Future[str] = asyncio.get_running_loop().create_future()

                def accepted(identifier: str) -> None:
                    if not admitted.done():
                        admitted.set_result(identifier)

                async def launch() -> object:
                    marker = _command_launching.set(asyncio.current_task())
                    try:
                        return await self._workspace_call(
                            agent_id,
                            "command_execute",
                            request.model_dump(),
                            confirmed=confirmed,
                            command_admitted=accepted,
                        )
                    finally:
                        _command_launching.reset(marker)

                task = asyncio.create_task(launch())
                entry = (signature, admitted, task)
                self._command_launches[key] = entry

                def finished(result: asyncio.Task[object]) -> None:
                    current = self._command_launches.get(key)
                    if current is not None and current[2] is result:
                        self._command_launches.pop(key, None)
                    if not result.cancelled():
                        result.exception()

                task.add_done_callback(finished)
        _, admitted, task = entry
        # asyncio.wait不把请求等待者取消传给后台任务或原admitted通知。
        await asyncio.wait((admitted, task), return_when=asyncio.FIRST_COMPLETED)
        if admitted.done():
            identifier = admitted.result()
            handle = self._handle(agent_id)
            try:
                view = await self.manager.commands.store.get(
                    f"session:{handle.session_id}", identifier
                )
            except ServiceError as error:
                if error.code == "execution_not_found":
                    raise ServiceError(
                        "execution_receipt_unverified", "后台通知没有对应持久命令", 409
                    ) from None
                raise
            origin = await self.manager.commands.store.origin(
                f"session:{handle.session_id}", identifier
            )
            if (
                view.request.request_key != request.request_key
                or origin is None
                or origin.get("request") != request.model_dump(mode="json")
            ):
                raise ServiceError("execution_receipt_unverified", "后台通知未对应原持久请求", 409)
            return CommandAccepted(command_id=identifier, request_key=request.request_key)
        if task.cancelled():
            raise ServiceError("execution_service_closed", "命令接受前已停止", 503)
        task.result()
        raise ServiceError("execution_receipt_unverified", "命令未报告真实持久接受", 409)

    async def command_by_key(
        self, agent_id: str, key: str, *, confirmed: bool = False
    ) -> CommandSummary | None:
        try:
            return await self.command_get(
                agent_id, CommandLookup(request_key=key), confirmed=confirmed
            )
        except ServiceError as error:
            if error.code == "execution_not_found":
                return None
            raise

    async def command_history(
        self, agent_id: str, request: CommandHistoryRequest, *, confirmed: bool = False
    ) -> CommandHistoryPage:
        return CommandHistoryPage.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "command_history", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def command_inspect(
        self, agent_id: str, request: CommandLookup, *, confirmed: bool = False
    ) -> CommandView:
        return CommandView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "command_inspect", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def command_resources(
        self, agent_id: str, request: CommandLookup, *, confirmed: bool = False
    ) -> CommandResourcesView:
        return CommandResourcesView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "command_resources", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def command_wait(
        self,
        agent_id: str,
        identifier: str,
        *,
        timeout_seconds: float = 30,
        confirmed: bool = False,
    ) -> CommandSummary:
        self.assert_external_command_control()
        if not math.isfinite(timeout_seconds) or not 0 <= timeout_seconds <= 300:
            raise ServiceError("execution_wait_timeout", "等待时间须为有限的0–300秒", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            result = await self.command_get(
                agent_id, CommandLookup(command_id=identifier), confirmed=confirmed
            )
            remaining = deadline - asyncio.get_running_loop().time()
            if result.phase in {"finished", "interrupted"} or remaining <= 0:
                return result
            await asyncio.sleep(min(0.05, remaining))

    @staticmethod
    def assert_external_command_control() -> None:
        task = _command_launching.get()
        if task is not None and not task.done():
            raise ServiceError(
                "execution_start_reentrant", "后台命令回调不能等待启动、等待结束或关闭服务", 409
            )

    async def command_read(
        self, agent_id: str, request: CommandRead, *, confirmed: bool = False
    ) -> CommandSlice:
        return CommandSlice.model_validate(
            await self._workspace_call(
                agent_id, "command_read", request.model_dump(), confirmed=confirmed
            )
        )

    async def command_cancel(
        self, agent_id: str, request: CommandLookup, *, confirmed: bool = False
    ) -> CommandSummary:
        return CommandSummary.model_validate(
            await self._workspace_call(
                agent_id, "command_cancel", request.model_dump(), confirmed=confirmed
            )
        )

    async def command_changes(
        self, agent_id: str, request: CommandChangesRequest, *, confirmed: bool = False
    ) -> CommandChangesPage:
        return CommandChangesPage.model_validate(
            await self._workspace_call(
                agent_id, "command_changes", request.model_dump(), confirmed=confirmed
            )
        )

    async def command_propose(
        self, agent_id: str, request: CommandProposalRequest, *, confirmed: bool = False
    ) -> CommandProposalView:
        return CommandProposalView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "command_propose", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def command_directory_propose(
        self,
        agent_id: str,
        request: CommandDirectoryProposalRequest,
        *,
        confirmed: bool = False,
    ) -> CommandDirectoryProposalView:
        return CommandDirectoryProposalView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "directory_source_preview",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def command_directory_changes(
        self,
        agent_id: str,
        request: CommandDirectoryChangesRequest,
        *,
        confirmed: bool = False,
    ) -> CommandDirectoryChangesPage:
        return CommandDirectoryChangesPage.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id,
                    "directory_source_query",
                    request.model_dump(),
                    confirmed=confirmed,
                )
            )
        )

    async def workspace_list(self, agent_id: str, request: ListFilesInput) -> WorkspaceListing:
        return WorkspaceListing.model_validate(
            await self._workspace_call(agent_id, "workspace_list", request.model_dump())
        )

    async def workspace_search(self, agent_id: str, request: SearchFilesInput) -> WorkspaceSearch:
        return WorkspaceSearch.model_validate(
            await self._workspace_call(agent_id, "workspace_search", request.model_dump())
        )

    async def workspace_preview(
        self, agent_id: str, request: WorkspacePreviewRequest, *, confirmed: bool = False
    ) -> WorkspaceChangeView:
        return WorkspaceChangeView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "workspace_preview", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def workspace_change(
        self, agent_id: str, request: WorkspaceChangeAction, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        return WorkspaceAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "workspace_change", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def workspace_reconcile(
        self, agent_id: str, request: WorkspaceOperationRequest, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        return WorkspaceAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "workspace_reconcile", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def workspace_finalize(
        self, agent_id: str, request: WorkspaceOperationRequest, *, confirmed: bool = False
    ) -> WorkspaceAttemptView:
        return WorkspaceAttemptView.model_validate_json(
            json.dumps(
                await self._workspace_call(
                    agent_id, "workspace_finalize", request.model_dump(), confirmed=confirmed
                )
            )
        )

    async def workspace_proposals(
        self, agent_id: str, *, offset: int = 0, limit: int = 50
    ) -> list[WorkspaceChangeView]:
        session = await self.session(agent_id)
        return await self.workspace_edits.list(
            f"session:{session.session_id}", offset=offset, limit=limit
        )

    async def workspace_history(
        self, agent_id: str, proposal_id: str
    ) -> list[WorkspaceAttemptView]:
        session = await self.session(agent_id)
        return await self.workspace_edits.history(f"session:{session.session_id}", proposal_id)

    async def workspace_proposal(self, agent_id: str, proposal_id: str) -> WorkspaceChangeView:
        session = await self.session(agent_id)
        return (await self.workspace_edits.get(f"session:{session.session_id}", proposal_id)).view

    async def workspace_operation(self, agent_id: str, operation_id: str) -> WorkspaceAttemptView:
        session = await self.session(agent_id)
        return (
            await self.workspace_edits.operation(f"session:{session.session_id}", operation_id)
        ).view

    async def workspace_operation_proof(
        self, agent_id: str, operation_id: str
    ) -> WorkspaceOperationProofView:
        session = await self.session(agent_id)
        return await self.workspace_edits.operation_proof(
            f"session:{session.session_id}", operation_id
        )

    async def _workspace_batch_authorize(self, agent_id: str) -> tuple[str, str, str]:
        scope, binding = await self._workspace_checkpoint_authorize(agent_id)
        return scope, binding.key, binding.fingerprint

    async def _workspace_checkpoint_authorize(self, agent_id: str) -> tuple[str, WorkspaceBinding]:
        if self._closed:
            raise ServiceError("agent_service_closed", "Agent服务已关闭", 503)
        handle = self._handle(agent_id)
        scope, binding = await self.manager.workspace_changes._guard(
            ToolContext(
                handle.runtime.tools,
                topic_id=handle.topic_id,
                agent_id=agent_id,
                session_id=handle.session_id,
                workspace=handle.definition.workspace,
                origin="host",
            )
        )
        return scope, binding

    async def _workspace_checkpoint_read(
        self, agent_id: str, path: str, fence: CheckpointCaptureFence, confirmed: bool
    ) -> object:
        return await self._workspace_call(
            agent_id,
            "workspace_checkpoint_capture" if fence.capture_format == "bytes" else "workspace_read",
            {"path": path}
            if fence.capture_format == "bytes"
            else ReadFileInput(path=path).model_dump(),
            confirmed=confirmed,
            checkpoint_capture=fence,
        )

    async def _workspace_checkpoint_bytes_authorize(self, agent_id: str) -> None:
        handle = self._handle(agent_id)
        if "workspace_checkpoint_capture" not in {
            item.name for item in handle.runtime.tools.definitions
        }:
            raise ServiceError(
                "workspace_checkpoint_bytes_unavailable", "原冻结Agent未授字节捕获工具", 403
            )

    async def workspace_create_checkpoint(
        self, agent_id: str, request: WorkspaceCheckpointCreate, *, confirmed: bool = False
    ) -> WorkspaceCheckpointView:
        return await self.workspace_checkpoints.create(agent_id, request, confirmed=confirmed)

    async def workspace_checkpoint(
        self, agent_id: str, checkpoint_id: str
    ) -> WorkspaceCheckpointView:
        session = await self.session(agent_id)
        return await self.workspace_checkpoint_store.get(
            f"session:{session.session_id}", checkpoint_id
        )

    async def workspace_checkpoint_by_key(
        self, agent_id: str, request_key: str
    ) -> WorkspaceCheckpointView | None:
        session = await self.session(agent_id)
        return await self.workspace_checkpoint_store.by_key(
            f"session:{session.session_id}", request_key
        )

    async def workspace_checkpoint_list(
        self, agent_id: str, *, offset: int = 0, limit: int = 32
    ) -> list[WorkspaceCheckpointView]:
        session = await self.session(agent_id)
        return await self.workspace_checkpoint_store.list(
            f"session:{session.session_id}", offset=offset, limit=limit
        )

    async def workspace_checkpoint_wait(
        self, agent_id: str, checkpoint_id: str, *, timeout_seconds: float = 30
    ) -> WorkspaceCheckpointView:
        session = await self.session(agent_id)
        return await self.workspace_checkpoints.wait(
            f"session:{session.session_id}", checkpoint_id, timeout_seconds=timeout_seconds
        )

    async def workspace_checkpoint_cancel(
        self, agent_id: str, checkpoint_id: str
    ) -> WorkspaceCheckpointView:
        session = await self.session(agent_id)
        return await self.workspace_checkpoints.cancel(
            f"session:{session.session_id}", checkpoint_id
        )

    async def _workspace_restore_child(
        self,
        agent_id: str,
        request: WorkspaceChangeAction,
        fence: CheckpointRestoreFence,
        confirmed: bool,
    ) -> object:
        return await self._workspace_call(
            agent_id,
            "workspace_change",
            request.model_dump(),
            confirmed=confirmed,
            checkpoint_restore=fence,
        )

    async def workspace_preview_restore(
        self, agent_id: str, request: CheckpointRestorePreview, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        return await self.workspace_restores.preview(agent_id, request, confirmed=confirmed)

    async def workspace_execute_restore(
        self, agent_id: str, request: CheckpointRestoreExecute, *, confirmed: bool = False
    ) -> CheckpointRestoreView:
        return await self.workspace_restores.start(agent_id, request, confirmed=confirmed)

    async def workspace_restore(self, agent_id: str, plan_id: str) -> CheckpointRestoreView:
        session = await self.session(agent_id)
        return await self.workspace_restore_store.get(f"session:{session.session_id}", plan_id)

    async def workspace_restore_by_key(
        self, agent_id: str, request_key: str
    ) -> CheckpointRestoreView | None:
        session = await self.session(agent_id)
        return await self.workspace_restore_store.by_key(
            f"session:{session.session_id}", request_key
        )

    async def workspace_restore_list(
        self, agent_id: str, *, offset: int = 0, limit: int = 32
    ) -> list[CheckpointRestoreView]:
        session = await self.session(agent_id)
        return await self.workspace_restore_store.history(
            f"session:{session.session_id}", offset=offset, limit=limit
        )

    async def workspace_restore_wait(
        self, agent_id: str, plan_id: str, *, timeout_seconds: float = 30
    ) -> CheckpointRestoreView:
        session = await self.session(agent_id)
        return await self.workspace_restores.wait(
            f"session:{session.session_id}", plan_id, timeout_seconds=timeout_seconds
        )

    async def workspace_restore_cancel(self, agent_id: str, plan_id: str) -> CheckpointRestoreView:
        session = await self.session(agent_id)
        return await self.workspace_restores.cancel(f"session:{session.session_id}", plan_id)

    async def _workspace_batch_child(
        self,
        agent_id: str,
        request: WorkspaceChangeAction,
        fence: NativeBatchFence,
        confirmed: bool,
    ) -> object:
        return await self._workspace_call(
            agent_id,
            "workspace_change",
            request.model_dump(),
            confirmed=confirmed,
            batch_fence=fence,
        )

    async def workspace_change_list(
        self,
        agent_id: str,
        *,
        filter: NativeListFilter = "active",
        cursor: str | None = None,
        limit: int = 32,
    ) -> NativeChangePage:
        session = await self.session(agent_id)
        return await self.workspace_list_store.page(
            f"session:{session.session_id}", filter=filter, cursor=cursor, limit=limit
        )

    async def workspace_start_batch(
        self, agent_id: str, request: NativeBatchRequest, *, confirmed: bool = False
    ) -> NativeBatchView:
        return await self.workspace_batches.start(agent_id, request, confirmed=confirmed)

    async def _mixed_batch_child(
        self, agent_id: str, dispatch: MixedBatchDispatch, confirmed: bool
    ) -> object:
        return await self._workspace_call(
            agent_id,
            "workspace_change" if dispatch.fence.source == "file" else "directory_change",
            dispatch.request.model_dump(),
            confirmed=confirmed,
            mixed_fence=dispatch.fence,
        )

    async def workspace_start_mixed_batch(
        self, agent_id: str, request: MixedBatchRequest, *, confirmed: bool = False
    ) -> MixedBatchView:
        return await self.workspace_mixed_batches.start(agent_id, request, confirmed=confirmed)

    async def _workspace_tree_child(
        self, agent_id: str, dispatch: TreePlanDispatch, confirmed: bool
    ) -> object:
        return await self._workspace_call(
            agent_id,
            dispatch.name,
            dispatch.request.model_dump(),
            confirmed=confirmed,
            tree_fence=dispatch.fence,
        )

    async def workspace_start_tree(
        self, agent_id: str, plan_id: str, *, confirmed: bool = False
    ) -> TreePlanView:
        return await self.workspace_trees.start(agent_id, plan_id, confirmed=confirmed)

    async def _prepare_tree(self, request: TreePlanRequest, context: ToolContext) -> TreePlanView:
        scope, binding, execution = await self.manager.directory_changes._source_guard(context)
        model_source = (
            TreePlanModelSource(context.run_id, context.call_id)
            if context.origin == "model"
            else None
        )
        if await self.workspace_tree_store.by_key(scope, request.request_key) is not None:
            return (
                await self.workspace_tree_store.reserve(
                    scope, binding, request, None, model_source=model_source
                )
            )[0]
        source = await self.manager.directory_changes.command_sources.source(
            scope, request.command_id
        )
        if source.source_hash != request.source_hash:
            raise ServiceError("workspace_tree_source_conflict", "树准备的原命令摘要不匹配", 409)
        if source.workspace != binding.manifest() or source.snapshot.resource != execution.resource:
            raise ServiceError(
                "workspace_tree_resource_conflict", "树准备不属于原工作区/执行资源", 403
            )
        if source.outcome != "completed" and not request.accept_failed:
            raise ServiceError("execution_failed_artifacts", "失败目录树成果须显式接受", 409)
        root = source.directories.get(request.root_path) if source.directories is not None else None
        if root is None:
            raise ServiceError("workspace_tree_root_not_found", "原完整来源没有所选目录根", 404)
        prepared = await workspace_io(
            WorkspaceDirectoryWriter(binding.require()).prepare_artifact,
            ArtifactDirectorySpec(
                path=request.root_path,
                operation="create",
                mode=root.mode,
                command_id=request.command_id,
                source_hash=request.source_hash,
            ),
        )
        await self.manager.directory_changes._source_guard(context)
        return (
            await self.workspace_tree_store.reserve(
                scope, binding, request, prepared, model_source=model_source
            )
        )[0]

    async def workspace_prepare_tree(
        self, agent_id: str, request: TreePlanRequest, *, confirmed: bool = False
    ) -> TreePlanView:
        value = await self._workspace_call(
            agent_id, "workspace_tree_prepare", request.model_dump(), confirmed=confirmed
        )
        return TreePlanView.model_validate_json(json.dumps(value))

    async def workspace_tree_by_key(self, agent_id: str, request_key: str) -> TreePlanView | None:
        session = await self.session(agent_id)
        return await self.workspace_tree_store.by_key(f"session:{session.session_id}", request_key)

    async def workspace_tree(self, agent_id: str, plan_id: str) -> TreePlanView:
        session = await self.session(agent_id)
        return await self.workspace_tree_store.get(f"session:{session.session_id}", plan_id)

    async def workspace_tree_steps(
        self, agent_id: str, plan_id: str, *, after: int | None = None, limit: int = 32
    ) -> TreePlanStepPage:
        session = await self.session(agent_id)
        return await self.workspace_tree_store.steps(
            f"session:{session.session_id}", plan_id, after=after, limit=limit
        )

    async def workspace_wait_tree(
        self, agent_id: str, plan_id: str, *, timeout_seconds: float = 30
    ) -> TreePlanView:
        session = await self.session(agent_id)
        return await self.workspace_trees.wait(
            f"session:{session.session_id}", plan_id, timeout_seconds=timeout_seconds
        )

    async def workspace_cancel_tree(self, agent_id: str, plan_id: str) -> TreePlanView:
        session = await self.session(agent_id)
        scope = f"session:{session.session_id}"
        await self.manager.tree_models.cancel_if_running(scope, plan_id)
        return await self.workspace_trees.cancel(scope, plan_id)

    async def workspace_mixed_batch(self, agent_id: str, batch_id: str) -> MixedBatchView:
        session = await self.session(agent_id)
        return await self.workspace_mixed_batch_store.get(f"session:{session.session_id}", batch_id)

    async def workspace_mixed_batch_by_key(
        self, agent_id: str, request_key: str
    ) -> MixedBatchView | None:
        session = await self.session(agent_id)
        return await self.workspace_mixed_batch_store.by_key(
            f"session:{session.session_id}", request_key
        )

    async def workspace_mixed_batch_history(
        self, agent_id: str, *, cursor: str | None = None, limit: int = 32
    ) -> MixedBatchHistoryPage:
        session = await self.session(agent_id)
        return await self.workspace_mixed_batch_store.history(
            f"session:{session.session_id}", cursor=cursor, limit=limit
        )

    async def workspace_wait_mixed_batch(
        self, agent_id: str, batch_id: str, *, timeout_seconds: float = 30
    ) -> MixedBatchView:
        session = await self.session(agent_id)
        return await self.workspace_mixed_batches.wait(
            f"session:{session.session_id}", batch_id, timeout_seconds=timeout_seconds
        )

    async def workspace_cancel_mixed_batch(self, agent_id: str, batch_id: str) -> MixedBatchView:
        session = await self.session(agent_id)
        scope = f"session:{session.session_id}"
        await self.manager.mixed_batch_writer.cancel_if_running(scope, batch_id)
        return await self.workspace_mixed_batches.cancel(scope, batch_id)

    async def workspace_batch(self, agent_id: str, batch_id: str) -> NativeBatchView:
        session = await self.session(agent_id)
        return await self.workspace_batch_store.get(f"session:{session.session_id}", batch_id)

    async def workspace_batch_history(
        self, agent_id: str, *, cursor: str | None = None, limit: int = 32
    ) -> NativeBatchHistoryPage:
        session = await self.session(agent_id)
        return await self.workspace_batch_store.history(
            f"session:{session.session_id}", cursor=cursor, limit=limit
        )

    async def workspace_batch_by_key(
        self, agent_id: str, request_key: str
    ) -> NativeBatchView | None:
        session = await self.session(agent_id)
        return await self.workspace_batch_store.by_key(f"session:{session.session_id}", request_key)

    async def workspace_cancel_batch(self, agent_id: str, batch_id: str) -> NativeBatchView:
        session = await self.session(agent_id)
        return await self.workspace_batches.cancel(f"session:{session.session_id}", batch_id)

    async def workspace_wait_batch(
        self, agent_id: str, batch_id: str, *, timeout_seconds: float = 30
    ) -> NativeBatchView:
        session = await self.session(agent_id)
        return await self.workspace_batches.wait(
            f"session:{session.session_id}", batch_id, timeout_seconds=timeout_seconds
        )

    async def as_tool(self, name: str, revision: int | None = None) -> ToolDefinition:
        """可信组合层显式取得固定版本工具；不由HTTP配置自动安装工具。"""
        definition = replace(await self.definition(name, revision), persistent=False, agent_id="")
        directory = AgentCatalog(self.manager)
        directory.register(
            name, definition, description=f"委派给 {name} 的固定版本 Agent", risk="write"
        )
        return directory.as_tool(name)

    async def restore_persistent(self) -> int:
        await self.recover_workspace_edits()
        restored = 0
        pending = {
            item.agent_id: item
            for item in await self.manager.store.list()
            if item.persistent and item.agent_id not in self.manager._handles
        }
        while pending:
            progress = False
            for identifier, item in list(pending.items()):
                if item.parent_agent_id in pending:
                    continue
                progress = True
                pending.pop(identifier)
                try:
                    definition = (
                        await self._profile_definition(
                            item.name, item.profile_revision, restoring=True
                        )
                        if item.profile_revision is not None
                        else self._catalog_definition(item.name)
                    )
                    handle = await self.manager.restore(definition, item)
                    if handle.current_run is not None:
                        handle.current_run = await self.runtime.repository.get_run(
                            handle.current_run.id
                        )
                    restored += 1
                    self.restore_errors.pop(identifier, None)
                except (AgentError, ServiceError, ToolError) as error:
                    self.restore_errors[identifier] = error.code
            if not progress:
                for identifier in pending:
                    self.restore_errors[identifier] = "parent_agent_missing"
                break
        return restored

    async def inventory(self) -> list[AgentInventoryView]:
        values = []
        for item in await self.manager.store.list():
            session = await self.sessions.for_agent(item.agent_id)
            current = session.current_run_id or item.current_run_id
            run = await self.runtime.repository.get_run(current) if current is not None else None
            handle = self.manager._handles.get(item.agent_id)
            available = handle is not None and not handle._lease_lost and not handle.destroyed
            values.append(
                AgentInventoryView(
                    agent_id=item.agent_id,
                    name=item.name,
                    topic_id=item.topic_id,
                    profile_revision=item.profile_revision,
                    status=run.status if run is not None else "created",
                    available=available,
                    error_code="agent_lease_lost"
                    if handle is not None and handle._lease_lost
                    else self.restore_errors.get(item.agent_id),
                    current_run_id=current,
                    turn=max(session.turn, item.turn),
                    session_id=session.session_id,
                    session_status=session.status,
                    lifecycle_status=item.status,
                )
            )
        return values

    async def restore(self, agent_id: str) -> AgentView:
        if agent_id in self.manager._handles:
            handle = self.manager._handles[agent_id]
            if not handle._lease_lost:
                return await self.get(agent_id)
            await handle.detach()
            self._results.pop(agent_id, None)
        stored = next(
            (item for item in await self.manager.store.list() if item.agent_id == agent_id), None
        )
        if stored is None:
            raise ServiceError("agent_not_found", "持久Agent不存在", 404)
        try:
            definition = (
                await self._profile_definition(stored.name, stored.profile_revision, restoring=True)
                if stored.profile_revision is not None
                else self._catalog_definition(stored.name)
            )
            handle = await self.manager.restore(definition, stored)
        except (AgentError, ServiceError, ToolError) as error:
            self.restore_errors[agent_id] = error.code
            if isinstance(error, ServiceError):
                raise
            raise ServiceError(error.code, str(error), 409) from error
        self.restore_errors.pop(agent_id, None)
        return await self.view(handle)

    async def assert_inventory_available(self) -> None:
        """启动恢复前先拒绝有效的其它 owner，不能中断仍在生产的进程。"""
        now = time.time()
        for item in await self.manager.store.list():
            if (
                item.owner_id is not None
                and item.owner_id != self.manager.owner_id
                and item.lease_expires_at is not None
                and item.lease_expires_at > now
            ):
                raise ServiceError(
                    "agent_inventory_busy", "持久 Agent 仍由另一进程拥有，禁止启动恢复", 409
                )

    def catalog_views(self) -> list[AgentDescriptor]:
        return self.catalog.descriptors()

    async def catalog_all(self) -> list[AgentDescriptor]:
        values = []
        for entry in self.catalog_views():
            try:
                self._catalog_definition(entry.name)
                values.append(entry)
            except ServiceError as error:
                values.append(
                    entry.model_copy(update={"available": False, "error_code": error.code})
                )
        for profile in await self.profiles.list():
            if not profile.enabled:
                continue
            error_code = None
            try:
                self._definition_from_profile(profile)
            except ServiceError as error:
                error_code = error.code
            values.append(
                AgentDescriptor(
                    name=profile.name,
                    description=profile.config.description,
                    tool_name=self.catalog.tool_name(profile.name),
                    risk="write",
                    max_turns=profile.config.max_turns,
                    max_steps=profile.config.max_steps,
                    persistent=profile.config.persistent,
                    exposed_as_tool=False,
                    profile_revision=profile.revision,
                    available=error_code is None,
                    error_code=error_code,
                )
            )
        return values

    async def views(self) -> list[AgentView]:
        handles = tuple(self.manager._handles.values())
        return [await self.view(handle) for handle in handles]

    async def get(self, agent_id: str) -> AgentView:
        return await self.view(self._handle(agent_id))

    async def session(self, agent_id: str) -> AgentSessionView:
        """持久事实只读；不要求当前进程已恢复实例。"""
        return await self.sessions.for_agent(agent_id)

    async def session_turns(self, agent_id: str) -> list[AgentTurnView]:
        return await self.sessions.turns((await self.session(agent_id)).session_id)

    async def task_snapshot(self, agent_id: str, kind: TaskKind) -> TaskSnapshot:
        value = await self.session(agent_id)
        return await self.manager.tasks.snapshot(f"session:{value.session_id}", kind)

    async def task_mutate(
        self, agent_id: str, kind: TaskKind, request: TaskMutation
    ) -> TaskSnapshot:
        self._handle(agent_id)
        value = await self.session(agent_id)
        return await self.manager.tasks.mutate(f"session:{value.session_id}", kind, request)

    async def task_history(
        self, agent_id: str, kind: TaskKind, *, offset: int = 0, limit: int = 100
    ) -> list[TaskOperationView]:
        value = await self.session(agent_id)
        return await self.manager.tasks.history(
            f"session:{value.session_id}", kind, offset=offset, limit=limit
        )

    async def close_session(self, agent_id: str, expected_revision: int) -> AgentSessionView:
        self.model_checkpoints.assert_external_close()
        try:
            return await self._handle(agent_id).close_session(expected_revision)
        except AgentError as error:
            raise ServiceError(error.code, str(error), 409) from error

    async def reopen_session(self, agent_id: str, expected_revision: int) -> AgentSessionView:
        try:
            return await self._handle(agent_id).reopen_session(expected_revision)
        except AgentError as error:
            raise ServiceError(error.code, str(error), 409) from error

    def _remember(self, handle: AgentHandle, result: ExecResult) -> None:
        if result.run_id is not None and (
            handle.current_run is None
            or result.run_id != handle.current_run.id
            or result.turn != handle.turn
        ):
            return
        if handle.agent_id in self.manager._handles:
            self._results[handle.agent_id] = (
                handle.current_run.id if handle.current_run else None,
                handle.turn,
                result,
            )

    async def execute(self, agent_id: str, request: AgentPromptRequest) -> AgentView:
        handle = self._handle(agent_id)
        try:
            result = await handle.execute(request.prompt, request.idempotency_key)
        except asyncio.CancelledError:
            caller = asyncio.current_task()
            if caller is not None and caller.cancelling():
                raise
            result = ExecResult(
                agent_id=handle.agent_id,
                run_id=handle.current_run.id if handle.current_run else None,
                status="aborted",
                error="Agent 执行已停止",
                code="cancelled",
                turn=handle.turn,
                parent_tool_call_id=handle.parent_tool_call_id,
            )
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        self._remember(handle, result)
        value = await self.view(handle)
        if result.run_id is not None and result.run_id != value.current_run_id:
            return value.model_copy(update={"result": result})
        return value

    async def start(self, agent_id: str, request: AgentPromptRequest) -> AgentView:
        async with self._start_lock:
            return await self._start_background(agent_id, request)

    async def _start_background(self, agent_id: str, request: AgentPromptRequest) -> AgentView:
        if self._closed:
            raise ServiceError("agent_service_closed", "Agent 服务已关闭", 503)
        handle = self._handle(agent_id)
        background = self._tasks.get(agent_id)
        if background is not None and not background[1].done():
            if request != background[0]:
                accepted = await self.sessions.find_turn(
                    handle.session_id, background[0].idempotency_key or ""
                )
                if accepted is None or accepted[0].status in {"queued", "running"}:
                    raise ServiceError(
                        "agent_busy", "Agent 已有后台执行，重试须使用相同输入和幂等键", 409
                    )
                # 终帧已保存但 consumer 正在收尾；先释放同轮资源，再接受下一轮。
                await asyncio.shield(background[1])
            else:
                return await self.view(handle)
        if handle._execute_lock.locked() or handle.pending_start:
            raise ServiceError("agent_busy", "Agent 已有活跃执行", 409)
        session = await handle.session()
        if session.status != "open":
            raise ServiceError(f"agent_session_{session.status}", "Agent 会话不可执行", 410)
        if request.idempotency_key is not None:
            historical = await self.sessions.find_turn(handle.session_id, request.idempotency_key)
            if historical is not None:
                # 验证同键输入在接受后台任务前完成；只读原 Run，不启动生产。
                from krim_agent.protocol.domain import ChatRequest

                await handle.runtime.start(
                    handle.topic_id,
                    ChatRequest(text=request.prompt, idempotency_key=request.idempotency_key),
                )
        if handle.current_run is not None:
            current = await self.runtime.repository.get_run(handle.current_run.id)
            if current.status in {"queued", "running"}:
                raise ServiceError("agent_busy", "Agent 当前生产尚未结束", 409)
        payload = request.model_copy(
            update={"idempotency_key": request.idempotency_key or str(uuid4())}
        )
        handle._accepted.clear()
        task = asyncio.create_task(self.execute(agent_id, payload))
        self._tasks[agent_id] = (
            payload,
            task,
            handle.current_run.id if handle.current_run else None,
        )

        def finished(completed: asyncio.Task[AgentView]) -> None:
            # 观察任务异常，避免断开的请求留下未消费异常；结果仍由 Run/session 查询。
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(finished)
        return await self.view(handle)

    async def wait(self, agent_id: str) -> AgentView:
        background = self._tasks.get(agent_id)
        if background is not None:
            return await asyncio.shield(background[1])
        handle = self._handle(agent_id)
        try:
            result = await handle.wait()
        except AgentError as error:
            raise ServiceError(error.code, str(error), 409) from error
        self._remember(handle, result)
        return await self.view(handle)

    async def wait_started(self, agent_id: str, key: str) -> str:
        """等待后台输入取得自己的 Run；队列拒绝不能伪装成此前的 Run。"""
        handle = self._handle(agent_id)
        background = self._tasks.get(agent_id)
        if background is None or background[0].idempotency_key != key:
            raise ServiceError("agent_start_mismatch", "没有该幂等键的后台输入", 409)
        found = await self.sessions.find_turn(handle.session_id, key)
        if found is not None:
            return found[0].id
        accepted = asyncio.create_task(handle._accepted.wait())
        try:
            await asyncio.wait((accepted, background[1]), return_when=asyncio.FIRST_COMPLETED)
        finally:
            accepted.cancel()
            await asyncio.gather(accepted, return_exceptions=True)
        found = await self.sessions.find_turn(handle.session_id, key)
        if found is not None:
            return found[0].id
        finished = await asyncio.shield(background[1])
        raise ServiceError(
            finished.result.code
            if finished.result and finished.result.code
            else "agent_not_started",
            "后台 Agent 输入未创建新的 Run",
            409,
        )

    async def wait_run(self, agent_id: str, run_id: str) -> None:
        background = self._tasks.get(agent_id)
        if background is not None:
            accepted = await self.sessions.find_turn(agent_id, background[0].idempotency_key or "")
            if accepted is not None and accepted[0].id == run_id:
                await asyncio.shield(background[1])

    async def resume(self, agent_id: str, request: AgentPromptRequest) -> AgentView:
        handle = self._handle(agent_id)
        try:
            result = await handle.resume(request.prompt, request.idempotency_key)
        except asyncio.CancelledError:
            caller = asyncio.current_task()
            if caller is not None and caller.cancelling():
                raise
            result = ExecResult(
                agent_id=handle.agent_id,
                run_id=handle.current_run.id if handle.current_run else None,
                status="aborted",
                error="Agent 执行已停止",
                code="cancelled",
                turn=handle.turn,
                parent_tool_call_id=handle.parent_tool_call_id,
            )
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        self._remember(handle, result)
        value = await self.view(handle)
        if result.run_id is not None and result.run_id != value.current_run_id:
            return value.model_copy(update={"result": result})
        return value

    async def stop(self, agent_id: str) -> AgentView:
        self.model_checkpoints.assert_external_close()
        handle = self._handle(agent_id)
        background = self._tasks.get(agent_id)
        if (
            background is not None
            and not background[1].done()
            and not handle._execute_lock.locked()
        ):
            background[1].cancel()
            await asyncio.gather(background[1], return_exceptions=True)
        try:
            result = await handle.stop()
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        self._remember(handle, result)
        return await self.view(handle)

    async def destroy(self, agent_id: str) -> AgentView:
        self.model_checkpoints.assert_external_close()
        handle = self._handle(agent_id)
        try:
            await handle.destroy()
        except AgentError as exc:
            raise ServiceError(exc.code, str(exc), 409) from exc
        value = await self.view(handle, destroyed=True)
        background = self._tasks.pop(agent_id, None)
        if background is not None:
            await asyncio.gather(background[1], return_exceptions=True)
        self._results.pop(handle.agent_id, None)
        return value

    async def stream_state(
        self, agent_id: str, from_seq: int, run_id: str | None = None
    ) -> tuple[ChatRuntime, RunState]:
        session = await self.session(agent_id)
        selected = run_id or session.current_run_id
        if selected is None:
            raise ServiceError("agent_not_started", "Agent 尚未启动运行", 409)
        await self.sessions.assert_run(session.session_id, selected)
        handle = self.manager._handles.get(agent_id)
        reader = handle.runtime if handle is not None else self.runtime
        state = await reader.stream_state(selected, from_seq)
        return reader, state

    async def close(self) -> None:
        self.model_checkpoints.assert_external_close()
        self.manager.tree_exchanges.assert_external_close()
        self.manager.tree_moves.assert_external_close()
        self.assert_external_command_control()
        self.workspace_trees.assert_external_close()
        self.manager.tree_models.assert_external_close()
        self.manager.commands.assert_external_close()
        self.manager.directory_changes.assert_external_close()
        self.workspace_batches.assert_external_close()
        self.workspace_mixed_batches.assert_external_close()
        async with self._start_lock:
            self._closed = True
        try:
            launches = list(self._command_launches.values())
            for _signature, _admitted, task in launches:
                if not task.cancelling():
                    task.cancel()
            await asyncio.gather(*(entry[2] for entry in launches), return_exceptions=True)
            await self.workspace_restores.close()
            await self.model_checkpoints.close()
            await self.workspace_checkpoints.close()
            await self.workspace_batches.close()
            await self.workspace_mixed_batches.close()
            await self.workspace_trees.close()
            operations = list(self._workspace_operations)
            for operation in operations:
                if not operation.cancelling():
                    operation.cancel()
            await asyncio.gather(*operations, return_exceptions=True)
            await self.manager.close()
            await asyncio.gather(
                *(task for _, task, _ in self._tasks.values()), return_exceptions=True
            )
        finally:
            self._tasks.clear()
            self._workspace_operations.clear()
            self._results.clear()
            await self.resources.close()
