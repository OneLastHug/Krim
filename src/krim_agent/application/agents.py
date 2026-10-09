"""Agent 目录与当前进程管理 API；不把 callable/provider 序列化到 HTTP。"""

from __future__ import annotations

import asyncio
import copy
import json
import time
from collections.abc import Mapping
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
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.persistence.agent_profiles import AgentProfileStore
from krim_agent.persistence.agent_sessions import assert_session_open, guard_agent_topic
from krim_agent.persistence.workspace_batches import NativeBatchStore
from krim_agent.persistence.workspace_checkpoints import WorkspaceCheckpointStore
from krim_agent.persistence.workspace_list import NativeChangeListStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.runtime.chat import RunState
from krim_agent.task_state import TaskKind, TaskMutation, TaskOperationView, TaskSnapshot
from krim_agent.tools import ToolContext, ToolError
from krim_agent.tools.core import ToolDefinition, ToolResult
from krim_agent.tools.workspace import ListFilesInput, ReadFileInput, SearchFilesInput
from krim_agent.tools.workspace_changes import workspace_change_tools
from krim_agent.workspace import (
    WORKSPACE_CHANGE_TOOL_NAMES,
    WORKSPACE_TOOL_NAMES,
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
from krim_agent.workspace_list import (
    NativeBatchFence,
    NativeBatchHistoryPage,
    NativeBatchRequest,
    NativeBatchView,
    NativeChangePage,
    NativeListFilter,
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
        self.workspace_checkpoint_store = WorkspaceCheckpointStore(self.workspace_edits)
        self.workspace_checkpoints = WorkspaceCheckpointService(
            self.workspace_checkpoint_store,
            self.manager.workspace_changes,
            self._workspace_checkpoint_authorize,
            self._workspace_checkpoint_read,
        )
        self._results: dict[str, tuple[str | None, int, ExecResult]] = {}
        self.restore_errors: dict[str, str] = {}
        self._tasks: dict[str, tuple[AgentPromptRequest, asyncio.Task[AgentView], str | None]] = {}
        self._workspace_operations: set[asyncio.Task[ToolResult]] = set()
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
        try:
            provider = self.resources.provider(profile.config.provider_key)
        except KeyError:
            raise ServiceError("agent_profile_provider", "Agent Provider已停用", 422) from None
        resource_version = self.resources.fingerprint(
            profile.config.provider_key,
            profile.config.tool_names,
            workspace_key=profile.config.workspace_key,
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
            ),
            workspace=workspace,
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
            if set(config.tool_names) & WORKSPACE_TOOL_NAMES:
                raise ServiceError("agent_profile_workspace", "文件工具需要已登记工作区", 422)
            return None
        try:
            binding = self.resources.workspace(config.workspace_key)
        except KeyError:
            raise ServiceError("agent_profile_workspace", "工作区未登记或已停用", 422) from None
        if set(config.tool_names) & WORKSPACE_CHANGE_TOOL_NAMES and not binding.workspace.writable:
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
        if writable or isinstance(root, Workspace) and root.writable:
            existing = {item.name: item for item in self.runtime.tools.definitions}
            for definition in workspace_change_tools():
                if definition.name in existing and existing[definition.name] is not definition:
                    raise ValueError("工作区修改工具名称已由其它定义占用")
        binding = self.resources.register_workspace(
            key, root, version=version, owned=owned, writable=writable
        )
        if binding.workspace.writable:
            registered_names = {item.name for item in self.runtime.tools.definitions}
            for definition in workspace_change_tools():
                if definition.name not in registered_names:
                    self.runtime.tools.register(definition)
        if default:
            self.runtime.workspace = binding
        return binding

    def disable_workspace(self, key: str) -> None:
        self.resources.disable_workspace(key)

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
            if tool.name not in WORKSPACE_CHANGE_TOOL_NAMES
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
        checkpoint_capture: CheckpointCaptureFence | None = None,
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
                await assert_session_open(session, session_row)
        if self._closed:
            raise ServiceError("agent_service_closed", "Agent服务已关闭", 503)
        if len(self._workspace_operations) >= 64:
            raise ServiceError("workspace_busy", "工作区任务已达上限，请稍后重试", 429)
        call_id = str(uuid4())
        frozen_arguments = copy.deepcopy(arguments)
        registry = handle.runtime.tools

        async def confirm(definition: ToolDefinition, context: ToolContext) -> bool:
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
                    workspace_changes=self.manager.workspace_changes,
                    workspace_batch=batch_fence,
                    checkpoint_capture=checkpoint_capture,
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
            }.get(error.code, 502)
            raise ServiceError(error.code, str(error), status) from error
        finally:
            self._workspace_operations.discard(operation)

    async def workspace_read(self, agent_id: str, request: ReadFileInput) -> WorkspaceFile:
        return WorkspaceFile.model_validate(
            await self._workspace_call(agent_id, "workspace_read", request.model_dump())
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
            "workspace_read",
            ReadFileInput(path=path).model_dump(),
            confirmed=confirmed,
            checkpoint_capture=fence,
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
                await self._profile_definition(profile.name, profile.revision)
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
        self.workspace_batches.assert_external_close()
        async with self._start_lock:
            self._closed = True
        try:
            await self.workspace_checkpoints.close()
            await self.workspace_batches.close()
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
