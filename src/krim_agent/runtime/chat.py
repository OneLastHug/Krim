"""独立于订阅者的生产任务；取消后等待消息、审计与终态持久化。"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Literal

from pydantic import JsonValue, ValidationError

from krim_agent.application.context import PersistentContext
from krim_agent.application.interactions import InteractionService
from krim_agent.application.workspace_changes import WorkspaceChangeService
from krim_agent.budgets import BudgetOwner, BudgetRequestView
from krim_agent.config import Settings
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextCompactResponse,
    ContextEngine,
    ContextError,
    ContextOptions,
    ContextProposalDecision,
    ContextProposalNotice,
    ContextProposalView,
    ContextResult,
    ContextRunConfig,
)
from krim_agent.execution.binding import COMMAND_TOOL_NAMES, ExecutionBinding
from krim_agent.observability import JsonlSink, SpanSink, TraceContext, Tracer
from krim_agent.otel import OtlpJsonSink
from krim_agent.persistence import KrimRepository
from krim_agent.persistence.budgets import UsageBudgetStore
from krim_agent.persistence.budgets import digest as budget_digest
from krim_agent.persistence.context_proposals import fingerprint
from krim_agent.persistence.traces import TraceStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import (
    TERMINAL_STATUSES,
    ChatRequest,
    EditRequest,
    MessageView,
    RegenerateRequest,
    RunStatus,
    RunView,
    ServiceError,
)
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import (
    ModelProvider,
    ModelText,
    ModelToolCall,
    ModelUsage,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.runtime.skills import definition as skill_tool_definition
from krim_agent.skills import SkillReadResult, SkillSnapshot
from krim_agent.tokenizer import TokenizerError, TokenizerSpec
from krim_agent.tools import ToolContext, ToolRegistry
from krim_agent.tools.core import (
    ApprovalHandler,
    CheckpointControlBackend,
    CommandBackend,
    Decision,
    DirectoryChangeBackend,
    DirectoryCheckpointBackend,
    DirectoryRestoreBackend,
    InteractionPublisher,
    MixedBatchQueryBackend,
    MixedBatchWriteBackend,
    Policy,
    QuestionHandler,
    TaskBackend,
    ToolDefinition,
    ToolExecutionBudget,
    TreeExchangeBackend,
    TreeMoveBackend,
    TreePlanModelBackend,
    TreePlanQueryBackend,
    WorkspaceCheckpointBackend,
)
from krim_agent.tools.discovery import ToolDiscovery
from krim_agent.usage import TokenUsage
from krim_agent.workspace import WORKSPACE_TOOL_NAMES, WORKSPACE_WRITE_TOOL_NAMES, WorkspaceBinding

logger = logging.getLogger(__name__)


@dataclass
class RunState:
    run: RunView
    frames: list[StreamFrame] = field(default_factory=list)
    event_writer: Callable[[StreamFrame], Awaitable[StreamFrame]] | None = None
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    started: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    finalizing: bool = False
    terminal: bool = False
    cancelled: bool = False
    draft: str = ""
    draft_calls: list[ModelToolCall] = field(default_factory=list)
    pending_calls: list[ModelToolCall] = field(default_factory=list)
    seen_calls: set[str] = field(default_factory=set)
    publish_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    step: int = 0
    discovery: ToolDiscovery | None = None
    skill_snapshot: SkillSnapshot | None = None
    context_config: ContextRunConfig | None = None
    context_engine: ContextEngine | None = None
    budget_account_ids: tuple[str, ...] = ()
    context_noticed: bool = False
    trace_context: TraceContext | None = None

    async def publish(
        self,
        kind: Literal["start", "chunk", "end", "error"],
        data: JsonValue = None,
        *,
        writer: Callable[[StreamFrame], Awaitable[StreamFrame]] | None = None,
        on_persisted: Callable[[], None] | None = None,
    ) -> None:
        async with self.publish_lock:
            frame = StreamFrame(stream_id=self.run.id, seq=len(self.frames), kind=kind, data=data)

            async def commit() -> None:
                write_frame = writer if writer is not None else self.event_writer
                stored = await write_frame(frame) if write_frame is not None else frame
                if on_persisted is not None:
                    on_persisted()
                async with self.condition:
                    self.frames.append(stored)
                    self.condition.notify_all()

            write = asyncio.create_task(commit())
            cancelled = False
            while True:
                try:
                    await asyncio.shield(write)
                    break
                except asyncio.CancelledError:
                    if write.cancelled():
                        raise
                    # 后续取消也不能传入原写任务，否则已提交帧可能未推进内存序号。
                    cancelled = True
            if cancelled:
                raise asyncio.CancelledError


class ChatRuntime:
    def __init__(
        self,
        repository: KrimRepository,
        provider: ModelProvider,
        tools: ToolRegistry,
        settings: Settings,
        *,
        tool_policy: Policy | None = None,
        agent_id: str | None = None,
        system_prompt: str = "",
        agent_definition_hash: str | None = None,
        agent_session_id: str | None = None,
        agent_owner_id: str | None = None,
        allow_skill_tool: bool = True,
        task_store: TaskBackend | None = None,
        workspace_changes: WorkspaceChangeService | None = None,
        directory_changes: DirectoryChangeBackend | None = None,
        workspace_mixed_queries: MixedBatchQueryBackend | None = None,
        workspace_tree_queries: TreePlanQueryBackend | None = None,
        workspace_tree_models: TreePlanModelBackend | None = None,
        tree_moves: TreeMoveBackend | None = None,
        tree_exchanges: TreeExchangeBackend | None = None,
        workspace_checkpoints: WorkspaceCheckpointBackend | None = None,
        workspace_checkpoint_controls: CheckpointControlBackend | None = None,
        workspace_directory_checkpoints: DirectoryCheckpointBackend | None = None,
        workspace_directory_restores: DirectoryRestoreBackend | None = None,
        workspace_missing_directory_restores: DirectoryRestoreBackend | None = None,
        workspace_mixed_batches: MixedBatchWriteBackend | None = None,
        workspace: WorkspaceBinding | None = None,
        execution: ExecutionBinding | None = None,
        commands: CommandBackend | None = None,
        approval_handler: ApprovalHandler | None = None,
        question_handler: QuestionHandler | None = None,
        interaction_publisher: InteractionPublisher | None = None,
        execution_timeouts: tuple[ToolExecutionBudget, ...] = (),
        budget_account_ids: tuple[str, ...] = (),
        budget_owner_kind: BudgetOwner = "runtime",
        budget_owner_id: str | None = None,
        tracer: Tracer | None = None,
    ) -> None:
        self.repository = repository
        self.provider = provider
        self.tools = tools
        self.settings = settings
        sink: SpanSink | None = None
        if tracer is None and settings.observability_enabled:
            if settings.observability_jsonl_path:
                sink = JsonlSink(settings.observability_jsonl_path)
            elif settings.observability_otlp_endpoint:
                sink = OtlpJsonSink(settings.observability_otlp_endpoint)
        self.tracer = tracer or Tracer(
            TraceStore(repository.database),
            enabled=settings.observability_enabled,
            sample_rate=settings.observability_sample_rate,
            queue_size=settings.observability_queue_size,
            timeout_seconds=settings.observability_timeout_seconds,
            sink=sink,
        )
        self._owns_tracer = tracer is None
        self.tool_policy = tool_policy
        if self.tool_policy is None and settings.interactive_tools:
            self.tool_policy = self._confirm_writes
        self.agent_id = agent_id
        self.system_prompt = system_prompt
        self.agent_definition_hash = agent_definition_hash
        self.agent_session_id = agent_session_id
        self.agent_owner_id = agent_owner_id
        if task_store is None:
            from krim_agent.persistence.tasks import AgentTaskStore

            task_store = AgentTaskStore(repository.database, owner_id=agent_owner_id)
        self.task_store = task_store
        self._owns_workspace_changes = workspace_changes is None
        self.workspace_changes = workspace_changes or WorkspaceChangeService(
            WorkspaceEditStore(repository.database, owner_id=agent_owner_id)
        )
        self.directory_changes = directory_changes
        self.workspace_mixed_queries = workspace_mixed_queries
        self.workspace_tree_queries = workspace_tree_queries
        self.workspace_tree_models = workspace_tree_models
        self.tree_moves = tree_moves
        self.tree_exchanges = tree_exchanges
        self.workspace_checkpoints = workspace_checkpoints
        self.workspace_checkpoint_controls = workspace_checkpoint_controls
        self.workspace_directory_checkpoints = workspace_directory_checkpoints
        self.workspace_directory_restores = workspace_directory_restores
        self.workspace_missing_directory_restores = workspace_missing_directory_restores
        self.workspace_mixed_batches = workspace_mixed_batches
        self.workspace = workspace
        self.execution, self.commands = execution, commands
        self.interactions = InteractionService(
            repository, seconds=settings.interaction_timeout_seconds
        )
        self.approval_handler = approval_handler or self.interactions.approve
        self.question_handler = question_handler or self.interactions.question
        self.interaction_publisher = interaction_publisher
        self.execution_timeouts = execution_timeouts
        self.budget_store = UsageBudgetStore(repository.database)
        self._parent_budget_accounts = budget_account_ids
        self._budget_owner_kind = budget_owner_kind
        self._budget_owner_id = budget_owner_id or settings.usage_budget_scope
        self._own_budget_accounts: tuple[str, ...] | None = None
        self._lock = asyncio.Lock()
        self._runs: OrderedDict[str, RunState] = OrderedDict()
        self._closed = False
        self.context_engine = self._engine_for_context(ContextRunConfig.from_settings(settings))
        self.persistent_context = PersistentContext(repository, self.context_engine)
        existing_skill_tool = next(
            (
                definition
                for definition in self.tools.definitions
                if definition.name == "read_skill"
            ),
            None,
        )
        if existing_skill_tool is not None and existing_skill_tool is not skill_tool_definition():
            raise ValueError("read_skill 是运行时保留工具名称")
        if existing_skill_tool is None and allow_skill_tool:
            self.tools.register(skill_tool_definition())

    def _engine_for_context(self, config: ContextRunConfig) -> ContextEngine:
        try:
            return ContextEngine(
                tokenizer=TokenizerSpec(
                    config.tokenizer,
                    message_projector=self.provider.project_messages
                    if isinstance(self.provider, OpenAICompatibleProvider)
                    else None,
                )
            )
        except TokenizerError as error:
            raise ServiceError(error.code, "本地词元编码器不可用", 503) from error

    @staticmethod
    async def _budget_commit[Result](operation: Awaitable[Result]) -> Result:
        task = asyncio.ensure_future(operation)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def ensure_usage_budget_binding(self) -> tuple[str, ...]:
        if self._own_budget_accounts is None:
            self._own_budget_accounts = await self.budget_store.bind(
                self._budget_owner_kind,
                self._budget_owner_id,
                self.settings.usage_budget,
                self._parent_budget_accounts,
            )
        return self._own_budget_accounts

    async def start(
        self, topic_id: str, request: ChatRequest, *, budget_account_ids: tuple[str, ...] = ()
    ) -> RunView:
        return await self._start(topic_id, request, budget_account_ids=budget_account_ids)

    @staticmethod
    async def _confirm_writes(definition: ToolDefinition, _context: ToolContext) -> Decision:
        return "allow" if definition.risk == "read" else "ask"

    async def edit(self, topic_id: str, message_id: str, request: EditRequest) -> RunView:
        return await self._start(topic_id, request, action="edit", source_message_id=message_id)

    async def regenerate(
        self, topic_id: str, message_id: str, request: RegenerateRequest
    ) -> RunView:
        return await self._start(
            topic_id, request, action="regenerate", source_message_id=message_id
        )

    async def compact(
        self, topic_id: str, request: ContextCompactRequest
    ) -> ContextCompactResponse:
        if self._closed:
            raise ServiceError("runtime_closed", "运行时已经关闭", 503)
        config = ContextRunConfig.from_settings(self.settings)
        budgets = (request.max_messages, request.max_chars, request.max_tokens)
        if all(value is None for value in budgets):
            budgets = (
                self.settings.context_max_messages,
                self.settings.context_max_chars,
                self.settings.context_max_tokens,
            )
            if all(value is None for value in budgets):
                budgets = (20, None, None)
        outcome = await self.persistent_context.compact(
            topic_id,
            ContextOptions(
                strategy=request.strategy,
                policy=request.policy,
                max_messages=budgets[0],
                max_chars=budgets[1],
                max_tokens=budgets[2],
                reserved_output_tokens=max(
                    request.reserved_output_tokens, config.reserved_output_tokens
                ),
            ),
            confirm=request.confirm,
            expected_branch_key=request.expected_branch_key,
            timeout_seconds=self.settings.run_timeout_seconds,
            engine=self._engine_for_context(config),
            proposal_id=request.proposal_id,
            proposal_revision=request.proposal_revision,
            config_hash=fingerprint(config.model_dump(mode="json")),
            ttl_seconds=config.proposal_ttl_seconds,
        )
        sources = outcome.result.source_message_ids
        return ContextCompactResponse(
            topic_id=topic_id,
            status=outcome.status,
            branch_key=outcome.branch_key,
            source_branch_key=outcome.source_branch_key,
            source_until_message_id=sources[-1] if sources else None,
            reason=outcome.reason,
            persisted=outcome.persisted,
            options=outcome.options,
            proposal_id=outcome.proposal_id,
            proposal_revision=outcome.proposal_revision,
            proposal_expires_at=outcome.proposal_expires_at,
            **outcome.result.model_dump(),
        )

    async def context_proposals(
        self, topic_id: str, *, pending_only: bool = False
    ) -> list[ContextProposalView]:
        return await self.persistent_context.proposals.list(
            topic_id,
            pending_only=pending_only,
            manual_config_hash=fingerprint(
                ContextRunConfig.from_settings(self.settings).model_dump(mode="json")
            ),
        )

    async def context_proposal(self, topic_id: str, proposal_id: str) -> ContextProposalView:
        return await self.persistent_context.proposals.get(
            topic_id,
            proposal_id,
            manual_config_hash=fingerprint(
                ContextRunConfig.from_settings(self.settings).model_dump(mode="json")
            ),
        )

    async def decide_context_proposal(
        self, topic_id: str, proposal_id: str, request: ContextProposalDecision
    ) -> ContextProposalView:
        if self._closed:
            raise ServiceError("runtime_closed", "运行时已经关闭", 503)
        proposal = await self.context_proposal(topic_id, proposal_id)
        config = ContextRunConfig.from_settings(self.settings)
        if proposal.run_id is not None:
            frames = await self.repository.list_events(proposal.run_id)
            if not frames or frames[0].kind != "start" or not isinstance(frames[0].data, dict):
                raise ServiceError("proposal_config_missing", "提案运行缺少公开配置快照", 409)
            config = ContextRunConfig.model_validate(frames[0].data.get("context_config"))
        return await self.persistent_context.decide(
            topic_id,
            proposal_id,
            request,
            config_hash=fingerprint(config.model_dump(mode="json")),
            engine=self._engine_for_context(config),
        )

    async def _wait_context_proposal(
        self, state: RunState, proposal: ContextProposalView
    ) -> ContextResult:
        # 子 Agent 可能借用父工具的执行 deadline；人工压缩确认与审批一样暂停它，
        # 当前 Run/Agent 的总预算仍由外层 timeout 管理，不释放生产名额。
        context = ToolContext(self.tools, execution_timeouts=self.execution_timeouts)
        async with context.human_wait():
            current = await self._await_context_proposal(state, proposal)
        assert state.context_engine is not None
        if not state.context_engine._fits(current.result.messages, current.options):
            raise ContextError("确认摘要不满足本次模型输入预算", code="model_input_budget")
        return await self.persistent_context._observe(
            state.run.topic_id,
            "automatic",
            current.result.model_copy(deep=True),
            True,
        )

    async def _await_context_proposal(
        self, state: RunState, proposal: ContextProposalView
    ) -> ContextProposalView:
        def notice(current: ContextProposalView) -> dict[str, JsonValue]:
            value = ContextProposalNotice(
                id=current.id,
                topic_id=current.topic_id,
                run_id=current.run_id,
                step=current.step,
                status=current.status,
                revision=current.revision,
                expires_at=current.expires_at,
                branch_key=current.branch_key,
                source_branch_key=current.source_branch_key,
                source_count=len(current.result.source_message_ids),
                summary_hash=fingerprint(current.result.summary),
                estimated_tokens=current.result.estimated_tokens,
                reserved_output_tokens=current.result.reserved_output_tokens,
                tokenizer=current.result.tokenizer,
            )
            return value.model_dump(mode="json")

        await state.publish(
            "chunk",
            {
                "type": "context_compact_proposal",
                "phase": "pending",
                "proposal": notice(proposal),
            },
        )
        while True:
            current = await self.context_proposal(state.run.topic_id, proposal.id)
            if current.status != "proposed":
                await state.publish(
                    "chunk",
                    {
                        "type": "context_compact_proposal",
                        "phase": "resolved",
                        "proposal": notice(current),
                    },
                )
                if current.status != "completed":
                    if (await self.repository.get_run(state.run.id)).stop_requested:
                        raise asyncio.CancelledError
                    raise ServiceError(
                        "proposal_" + current.status, "压缩提案未获确认，模型派发已停止", 409
                    )
                return current
            await asyncio.sleep(0.05)

    async def _start(
        self,
        topic_id: str,
        request: ChatRequest | EditRequest | RegenerateRequest,
        *,
        action: Literal["send", "edit", "regenerate"] = "send",
        source_message_id: str | None = None,
        budget_account_ids: tuple[str, ...] = (),
    ) -> RunView:
        async with self._lock:
            if self._closed:
                raise ServiceError("runtime_closed", "运行时已经关闭", 503)
            if self.persistent_context.is_busy(topic_id):
                raise ServiceError("context_busy", "话题正在生成手动压缩结果", 409)
            context_config = ContextRunConfig.from_settings(self.settings)
            context_engine = self._engine_for_context(context_config)
            await self.ensure_usage_budget_binding()
            assert self._own_budget_accounts is not None
            accounts = tuple(dict.fromkeys((*self._own_budget_accounts, *budget_account_ids)))
            if accounts and (
                context_config.tokenizer == "approx-v1"
                or context_config.reserved_output_tokens <= 0
            ):
                raise ServiceError(
                    "budget_configuration_invalid", "累计预算需要 BPE 与正数输出预留", 422
                )
            active = sum(
                state.task is not None and not state.terminal for state in self._runs.values()
            )
            run, created = await self.repository.begin_run(
                topic_id,
                request,
                allow_new=active < self.settings.max_active_runs,
                action=action,
                source_message_id=source_message_id,
                agent_session_id=self.agent_session_id,
                agent_id=self.agent_id,
                agent_owner_id=self.agent_owner_id,
            )
            if not created:
                return run
            state = RunState(
                run,
                event_writer=self.repository.append_event,
                context_config=context_config,
                context_engine=context_engine,
                budget_account_ids=accounts,
            )
            self._runs[run.id] = state
            state.task = asyncio.create_task(self._drive(state), name=f"krim-run-{run.id}")
            await state.started.wait()
            self._evict()
            return await self.repository.get_run(run.id)

    def _evict(self) -> None:
        for run_id, state in list(self._runs.items()):
            if len(self._runs) <= 128:
                break
            if state.terminal:
                del self._runs[run_id]

    async def _drive(self, state: RunState) -> None:
        with self.tracer.scope("run", "run", state.run.id) as span:
            state.trace_context = span.context if span.context and span.context.sampled else None
            await self._drive_observed(state)
            if state.frames and isinstance(state.frames[-1].data, dict):
                status = state.frames[-1].data.get("status")
                span.status = (
                    "ok"
                    if status == "completed"
                    else "cancelled"
                    if status == "aborted"
                    else "error"
                )

    async def _drive_observed(self, state: RunState) -> None:
        status: RunStatus = "completed"
        error: str | None = None
        error_code: str | None = None
        state.started.set()
        try:
            await self.repository.set_running(state.run.id)
            assert state.context_config is not None
            start_data: dict[str, JsonValue] = {
                "provider": self.provider.name,
                "model_usage_trace": 1,
                "context_config": state.context_config.model_dump(mode="json"),
                "budget_account_ids": list(state.budget_account_ids),
            }
            if state.trace_context is not None:
                start_data["trace_context"] = state.trace_context.model_dump(mode="json")
            if self.agent_session_id is not None:
                start_data["agent_id"] = self.agent_id
                start_data["session_id"] = self.agent_session_id
                start_data["agent_definition_hash"] = self.agent_definition_hash
            await state.publish("start", start_data)
            snapshot = asyncio.create_task(self.repository.run_skill_snapshot(state.run.id))
            try:
                state.skill_snapshot = await asyncio.shield(snapshot)
            except asyncio.CancelledError:
                await snapshot
                raise
            async with asyncio.timeout(self.settings.run_timeout_seconds):
                await self._loop(state)
        except asyncio.CancelledError:
            status, error = "aborted", "运行已停止"
            error_code = "cancelled"
        except TimeoutError:
            status, error = "failed", "运行超过时间预算"
            error_code = "run_timeout"
        except ServiceError as exc:
            status, error = "failed", str(exc)
            error_code = exc.code
        except ProviderError as exc:
            status, error = "failed", str(exc)
            error_code = exc.code
        except ContextError as exc:
            status, error = "failed", str(exc)
            error_code = exc.code
        except Exception:
            logger.error("运行失败；异常正文和堆栈不进入日志: %s", state.run.id)
            status, error = "failed", "模型或运行时执行失败"
            error_code = "execution_error"
        finally:
            state.finalizing = True
            try:
                if state.draft_calls:
                    await self.repository.add_message(
                        state.run,
                        "assistant",
                        state.draft,
                        {
                            "partial": True,
                            "step": state.step,
                            "tool_calls": [
                                call.model_dump(mode="json") for call in state.draft_calls
                            ],
                        },
                    )
                    state.draft = ""
                    state.pending_calls.extend(state.draft_calls)
                    state.draft_calls.clear()
                for pending_call in state.pending_calls:
                    await self.repository.add_message(
                        state.run,
                        "tool",
                        "工具未完成",
                        {
                            "call_id": pending_call.id,
                            "name": pending_call.name,
                            "is_error": True,
                            "code": "cancelled" if status == "aborted" else "incomplete",
                        },
                    )
                state.pending_calls.clear()
                if state.draft:
                    await self.repository.add_message(
                        state.run,
                        "assistant",
                        state.draft,
                        {"partial": status != "completed", "step": state.step},
                    )
                    state.draft = ""
                audits = self.tools.audits_for_run(state.run.id)
                await self.repository.finish(
                    state.run.id, status, error, audits, error_code=error_code
                )
                self.tools.clear_run_audits(state.run.id)
                await state.publish("end", {"status": status, "error": error, "code": error_code})
            finally:
                state.terminal = True
                async with state.condition:
                    state.condition.notify_all()

    async def _publish_message(
        self,
        state: RunState,
        role: str,
        content: str,
        details: dict[str, JsonValue],
        data: dict[str, JsonValue],
        on_persisted: Callable[[], None],
    ) -> None:
        async def write(frame: StreamFrame) -> StreamFrame:
            try:
                return await self.repository.append_message_event(
                    state.run, role, content, details, frame
                )
            except Exception:
                # COMMIT之后的回程可能丢失；只核实原消息/seq，不重发模型或工具。
                original = await self.repository.read_message_event(
                    state.run, role, content, details, frame
                )
                if original is not None:
                    return original
                raise

        await state.publish("chunk", data, writer=write, on_persisted=on_persisted)

    async def _loop(self, state: RunState) -> None:
        assert state.skill_snapshot is not None
        assert state.context_config is not None and state.context_engine is not None
        config = state.context_config
        tokenizer = state.context_engine.tokenizer
        assert tokenizer is not None
        try:
            definitions = [
                definition
                for definition in self.tools.definitions
                if definition.name != "read_skill" or state.skill_snapshot.skills
                if definition.name not in WORKSPACE_TOOL_NAMES or self.workspace is not None
                if definition.name not in COMMAND_TOOL_NAMES or self.execution is not None
                if definition.name not in WORKSPACE_WRITE_TOOL_NAMES
                or (self.workspace is not None and self.workspace.workspace.writable)
            ]
            state.discovery = ToolDiscovery(definitions)
        except ValueError as error:
            raise ServiceError(
                "tool_catalog_conflict", "工具目录与按需搜索名称冲突", 409
            ) from error
        for _step in range(self.settings.max_steps):
            state.step = _step
            history = await self.repository.list_messages(state.run.topic_id)
            system = (
                MessageView(
                    id=f"agent-config:{self.agent_id}",
                    topic_id=state.run.topic_id,
                    run_id=state.run.id,
                    sequence=-1,
                    role="system",
                    content=self.system_prompt,
                    details={
                        "source": "agent_config",
                        "definition_hash": self.agent_definition_hash,
                    },
                    created_at=state.run.created_at,
                )
                if self.system_prompt
                else None
            )
            assert state.discovery is not None
            tool_step = state.discovery.prepare()
            tool_tokens = (
                tokenizer.tool_tokens(list(tool_step.definitions.values()))
                if config.model_context_window_tokens is not None or state.budget_account_ids
                else 0
            )
            token_limit = config.max_tokens
            hard_limit_applied = False
            if config.model_context_window_tokens is not None:
                hard_limit = (
                    config.model_context_window_tokens
                    - tool_tokens
                    - config.model_framing_reserve_tokens
                )
                if hard_limit <= config.reserved_output_tokens:
                    raise ContextError(
                        "工具 Schema/输出/封装预留超过模型窗口", code="model_input_budget"
                    )
                if token_limit is None or hard_limit < token_limit:
                    token_limit = hard_limit
                    hard_limit_applied = True
                if config.trigger_ratio is not None:
                    soft_limit = config.reserved_output_tokens + int(
                        (hard_limit - config.reserved_output_tokens) * config.trigger_ratio
                    )
                    if soft_limit < token_limit:
                        token_limit = soft_limit
                        hard_limit_applied = False
            if (
                config.notice_messages is not None
                and len(history) >= config.notice_messages
                and not state.context_noticed
            ):
                state.context_noticed = True
                await state.publish(
                    "chunk",
                    {
                        "type": "context_notice",
                        "step": _step,
                        "message_count": len(history),
                        "threshold": config.notice_messages,
                    },
                )
            if system is not None and token_limit is not None:
                token_limit -= tokenizer.count([system])
                if token_limit <= config.reserved_output_tokens:
                    raise ContextError(
                        "Agent系统指令超过上下文容量", code="agent_instruction_budget"
                    )
            try:
                options = ContextOptions(
                    strategy=config.strategy,
                    policy=config.policy,
                    max_messages=config.max_messages,
                    max_chars=config.max_chars,
                    max_tokens=token_limit,
                    reserved_output_tokens=config.reserved_output_tokens,
                )
                if config.compact_mode == "confirmation":
                    prepared = await self.persistent_context.prepare_confirmation(
                        state.run.topic_id,
                        history,
                        options,
                        engine=state.context_engine,
                        run_id=state.run.id,
                        step=_step,
                        config_hash=fingerprint(config.model_dump(mode="json")),
                        ttl_seconds=config.proposal_ttl_seconds,
                    )
                    context = (
                        await self._wait_context_proposal(state, prepared)
                        if isinstance(prepared, ContextProposalView)
                        else prepared
                    )
                else:
                    context = await self.persistent_context.prepare(
                        state.run.topic_id,
                        history,
                        options,
                        engine=state.context_engine,
                    )
            except ContextError as error:
                if hard_limit_applied and error.code == "context_budget":
                    raise ContextError(
                        "最新完整上下文与输出预留超过模型窗口", code="model_input_budget"
                    ) from error
                raise
            messages = [system, *context.messages] if system is not None else context.messages
            if context.compacted:
                await state.publish(
                    "chunk",
                    {
                        "type": "context_compact",
                        "original_count": context.original_count,
                        "kept_count": len(context.messages),
                        "omitted_count": context.omitted_count,
                        "strategy": context.strategy,
                        "summary": context.summary,
                        "tokenizer": context.tokenizer,
                        "estimated_tokens": context.estimated_tokens,
                        "reserved_output_tokens": context.reserved_output_tokens,
                        "status": "completed",
                        "persisted": (
                            context.summary is not None
                            and context.strategy in {"summarize_then_truncate", "annotated_compact"}
                            and context.policy != "never"
                        ),
                        "source_count": len(context.source_message_ids),
                        "source_until_message_id": (
                            context.source_message_ids[-1] if context.source_message_ids else None
                        ),
                        "source_branch_key": (
                            self.repository.context_branch_key(
                                history[: len(context.source_message_ids)]
                            )
                            if context.source_message_ids
                            else None
                        ),
                    },
                )
            calls: list[ModelToolCall] = []
            if config.model_context_window_tokens is not None:
                assert tokenizer is not None
                input_tokens = tokenizer.count(messages)
                required = (
                    input_tokens
                    + tool_tokens
                    + config.reserved_output_tokens
                    + config.model_framing_reserve_tokens
                )
                await state.publish(
                    "chunk",
                    {
                        "type": "context_budget",
                        "step": _step,
                        "encoding": config.tokenizer,
                        "input_tokens": input_tokens,
                        "tool_tokens": tool_tokens,
                        "reserved_output_tokens": config.reserved_output_tokens,
                        "framing_reserve_tokens": config.model_framing_reserve_tokens,
                        "window_tokens": config.model_context_window_tokens,
                        "required_tokens": required,
                        "within_limit": required <= config.model_context_window_tokens,
                    },
                )
                if required > config.model_context_window_tokens:
                    raise ContextError(
                        "完整消息/工具 Schema/输出预留超过模型窗口", code="model_input_budget"
                    )
                if isinstance(self.provider, OpenAICompatibleProvider) and (
                    self.provider.config.max_output_tokens is None
                    or self.provider.config.max_output_tokens > config.reserved_output_tokens
                ):
                    raise ContextError(
                        "Provider 输出上限未受当前预留约束", code="model_output_budget"
                    )
            provider_stream: AsyncIterator[ModelText | ModelToolCall | ModelUsage] | None = None
            provider_usage: TokenUsage | None = None
            usage_final = False
            provider_completed = False
            reservation: BudgetRequestView | None = None
            invoked = False
            if state.budget_account_ids:
                if isinstance(self.provider, OpenAICompatibleProvider) and (
                    self.provider.config.max_output_tokens is None
                    or self.provider.config.max_output_tokens > config.reserved_output_tokens
                ):
                    raise ServiceError(
                        "budget_output_cap_invalid", "Provider 输出 cap 未受预留约束", 409
                    )
                pending_reservation = asyncio.create_task(
                    self.budget_store.reserve(
                        state.run.id,
                        _step,
                        state.budget_account_ids,
                        tokenizer.count(messages)
                        + tool_tokens
                        + config.model_framing_reserve_tokens,
                        config.reserved_output_tokens,
                        budget_digest(
                            [
                                [
                                    {
                                        "role": message.role,
                                        "content": message.content,
                                        "details": message.details,
                                    }
                                    for message in messages
                                ],
                                [
                                    definition.schema().model_dump(mode="json")
                                    for definition in tool_step.definitions.values()
                                ],
                            ]
                        ),
                    )
                )
                try:
                    reservation = await asyncio.shield(pending_reservation)
                except asyncio.CancelledError:
                    reservation = await pending_reservation
                    if reservation.state == "reserved":
                        await self._budget_commit(
                            self.budget_store.transition(reservation.request_id, "released")
                        )
                    raise
                if reservation.state != "reserved":
                    raise ServiceError(
                        "budget_request_replay_denied", "已派发预算 request 禁止再次执行模型", 409
                    )
            try:
                if reservation is not None:
                    await state.publish(
                        "chunk",
                        {"type": "usage_budget", "request": reservation.model_dump(mode="json")},
                    )
                await state.publish(
                    "chunk",
                    {
                        "type": "model_step",
                        "step": _step,
                        "phase": "start",
                        "provider": self.provider.name,
                    },
                )
                if reservation is not None:
                    reservation = await self._budget_commit(
                        self.budget_store.transition(reservation.request_id, "dispatched")
                    )
            except BaseException:
                if reservation is not None:
                    await self._budget_commit(
                        self.budget_store.transition(reservation.request_id, "released")
                    )
                raise
            with self.tracer.scope("model", "run", state.run.id, {"step": _step}):
                try:
                    invoked = True
                    provider_stream = self.provider.stream(
                        messages, list(tool_step.definitions.values())
                    )
                    async for delta in provider_stream:
                        if usage_final and not isinstance(delta, ModelUsage):
                            raise ProviderError("provider_usage_conflict", "最终用量后仍有模型输出")
                        if isinstance(delta, ModelText):
                            if len(state.draft) + len(delta.text) > 8192:
                                raise ServiceError("output_limit", "单轮输出超过限制")
                            state.draft += delta.text
                            await state.publish("chunk", {"type": "text_delta", "text": delta.text})
                        elif isinstance(delta, ModelUsage):
                            try:
                                delta = ModelUsage.model_validate(delta.model_dump(mode="json"))
                            except ValidationError as error:
                                raise ProviderError(
                                    "provider_usage", "Provider 用量计数无效"
                                ) from error
                            if provider_usage is not None and not delta.usage.follows(
                                provider_usage
                            ):
                                raise ProviderError(
                                    "provider_usage_conflict", "Provider 用量发生回退"
                                )
                            if usage_final and (delta.usage != provider_usage or not delta.final):
                                raise ProviderError("provider_usage_conflict", "最终用量不能被改写")
                            if delta.usage != provider_usage or delta.final != usage_final:
                                await state.publish(
                                    "chunk",
                                    {
                                        "type": "token_usage",
                                        "step": _step,
                                        "usage": delta.usage.model_dump(mode="json"),
                                        "final": delta.final,
                                    },
                                )
                            provider_usage, usage_final = delta.usage, delta.final
                        elif isinstance(delta, ModelToolCall):
                            if delta.id in state.seen_calls:
                                raise ServiceError("duplicate_tool_call", "工具调用 ID 重复")
                            state.seen_calls.add(delta.id)
                            if len(calls) >= 8:
                                raise ServiceError("tool_limit", "单轮工具调用超过限制")
                            calls.append(delta)
                            state.draft_calls.append(delta)
                            await state.publish(
                                "chunk", {"type": "tool_call", **delta.model_dump(mode="json")}
                            )
                        else:
                            raise ProviderError("provider_response", "Provider 返回未知模型事件")
                        if len(state.frames) >= 2048:
                            raise ServiceError("stream_limit", "流事件超过限制")
                    provider_completed = True
                finally:
                    try:
                        close_stream = getattr(provider_stream, "aclose", None)
                        if close_stream is not None:
                            await close_stream()
                    finally:
                        if reservation is not None:
                            if not invoked:
                                reservation = await self._budget_commit(
                                    self.budget_store.transition(reservation.request_id, "released")
                                )
                            elif (
                                provider_completed
                                and usage_final
                                and provider_usage is not None
                                and all(
                                    getattr(provider_usage, key) is not None
                                    for key in ("input_tokens", "output_tokens", "total_tokens")
                                )
                            ):
                                try:
                                    reservation = await self._budget_commit(
                                        self.budget_store.settle(
                                            reservation.request_id, provider_usage
                                        )
                                    )
                                except Exception:
                                    await self._budget_commit(
                                        self.budget_store.transition(
                                            reservation.request_id, "uncertain"
                                        )
                                    )
                                    raise
                            else:
                                reservation = await self._budget_commit(
                                    self.budget_store.transition(
                                        reservation.request_id, "uncertain"
                                    )
                                )
                        await state.publish(
                            "chunk",
                            {
                                "type": "model_step",
                                "step": _step,
                                "phase": "end",
                                "provider": self.provider.name,
                                "completed": provider_completed,
                            },
                        )
                        if reservation is not None:
                            await state.publish(
                                "chunk",
                                {
                                    "type": "usage_budget",
                                    "request": reservation.model_dump(mode="json"),
                                },
                            )
            if reservation is not None:
                if reservation.state == "uncertain":
                    raise ServiceError(
                        "budget_usage_unknown", "模型用量未获完整确认，预算保留待核实", 409
                    )
                if reservation.overdrawn:
                    raise ServiceError(
                        "budget_report_overrun", "已报告用量超过预留/累计预算，已照实记录", 409
                    )
            if not calls:
                if not state.draft:
                    raise ServiceError("no_output", "模型没有输出文本或工具请求")
                await self._publish_message(
                    state,
                    "assistant",
                    state.draft,
                    {"step": _step, "partial": False},
                    {"type": "step_finish", "step": _step, "finish_reason": "stop"},
                    lambda: setattr(state, "draft", ""),
                )
                return
            await self.repository.add_message(
                state.run,
                "assistant",
                state.draft,
                {"tool_calls": [call.model_dump(mode="json") for call in calls], "step": _step},
            )
            state.draft = ""
            state.draft_calls.clear()
            state.pending_calls = list(calls)

            async def publish_interaction(data: dict[str, JsonValue]) -> None:
                await state.publish("chunk", data)
                if self.interaction_publisher is not None:
                    await self.interaction_publisher(data)

            for call in calls:
                result = await self.tools.invoke(
                    call.name,
                    call.arguments,
                    ToolContext(
                        self.tools,
                        run_id=state.run.id,
                        topic_id=state.run.topic_id,
                        timeout=self.settings.tool_timeout_seconds,
                        call_id=call.id,
                        policy=self.tool_policy,
                        agent_id=self.agent_id,
                        session_id=self.agent_session_id,
                        task_store=self.task_store,
                        workspace=self.workspace,
                        execution=self.execution,
                        commands=self.commands,
                        workspace_changes=self.workspace_changes,
                        directory_changes=self.directory_changes,
                        workspace_mixed_queries=self.workspace_mixed_queries,
                        workspace_tree_queries=self.workspace_tree_queries,
                        workspace_tree_models=self.workspace_tree_models,
                        tree_moves=self.tree_moves,
                        tree_exchanges=self.tree_exchanges,
                        workspace_checkpoints=self.workspace_checkpoints,
                        workspace_checkpoint_controls=self.workspace_checkpoint_controls,
                        workspace_directory_checkpoints=self.workspace_directory_checkpoints,
                        workspace_directory_restores=self.workspace_directory_restores,
                        workspace_missing_directory_restores=self.workspace_missing_directory_restores,
                        workspace_mixed_batches=self.workspace_mixed_batches,
                        approval_handler=self.approval_handler,
                        question_handler=self.question_handler,
                        interaction_publisher=publish_interaction,
                        execution_timeouts=self.execution_timeouts,
                        model_tools=tool_step.bindings(),
                        skill_snapshot=state.skill_snapshot.model_copy(deep=True)
                        if state.skill_snapshot
                        else None,
                        context_config=config.model_dump(mode="json"),
                        budget_account_ids=state.budget_account_ids,
                        tracer=self.tracer,
                    ),
                )
                added = state.discovery.consume(tool_step, call.id, call.name, result)
                await self._publish_message(
                    state,
                    "tool",
                    result.content[0].text,
                    {"call_id": call.id, "name": call.name, **result.model_dump(mode="json")},
                    {
                        "type": "tool_result",
                        "call_id": call.id,
                        "name": call.name,
                        **result.model_dump(mode="json"),
                    },
                    partial(state.pending_calls.remove, call),
                )
                if added:
                    await state.publish(
                        "chunk",
                        {
                            "type": "tool_discovery",
                            "tools": list(added),
                            "step": _step,
                            "call_id": call.id,
                        },
                    )
                if (
                    call.name == "read_skill"
                    and not result.is_error
                    and isinstance(result.data, dict)
                ):
                    loaded = SkillReadResult.model_validate(result.data)
                    try:
                        expected = state.skill_snapshot.read(loaded.name, loaded.resource)
                    except KeyError:
                        expected = None
                    if expected == loaded:
                        await state.publish(
                            "chunk",
                            {
                                "type": "skill_loaded",
                                "name": loaded.name,
                                "resource": loaded.resource,
                                "step": _step,
                                "call_id": call.id,
                            },
                        )
            await state.publish(
                "chunk", {"type": "step_finish", "step": _step, "finish_reason": "tool-calls"}
            )
        raise ServiceError("step_limit", "模型步骤超过预算")

    async def stop(self, run_id: str) -> RunView:
        async with self._lock:
            run = await self.repository.get_run(run_id)
            state = self._runs.get(run_id)
            if run.status in TERMINAL_STATUSES:
                if state is None or state.task is None:
                    return run
                task = state.task
            else:
                if state is None or state.task is None:
                    raise ServiceError("run_unavailable", "运行不在当前进程中", 409)
                if not state.cancelled and not state.finalizing:
                    await self.repository.request_stop(run_id)
                    state.cancelled = True
                    if not state.finalizing:
                        state.task.cancel()
                task = state.task
        await asyncio.shield(task)
        return await self.repository.get_run(run_id)

    def production_task(self, run_id: str) -> asyncio.Task[None] | None:
        """可信组合层只读借用生产任务，用于资源名额的真实生命周期绑定。"""
        state = self._runs.get(run_id)
        return state.task if state is not None else None

    async def stream_state(self, run_id: str, from_seq: int) -> RunState:
        run = await self.repository.get_run(run_id)
        state = self._runs.get(run_id)
        if state is None or state.task is None:
            frames = await self.repository.ensure_terminal_event(run_id)
            if not frames and run.status in TERMINAL_STATUSES:
                raise ServiceError("stream_gone", "该运行没有可恢复的持久事件", 410)
            state = RunState(
                run,
                frames=frames,
                event_writer=self.repository.append_event,
                terminal=bool(frames and frames[-1].kind in {"end", "error"}),
            )
            state.started.set()
            self._runs[run_id] = state
            self._evict()
        summary = await self.repository.event_summary(run_id)
        if from_seq < 0 or from_seq > max(len(state.frames), summary.count):
            raise ServiceError("invalid_cursor", "流游标超出已有事件", 416)
        return state

    async def subscribe(
        self, state: RunState, from_seq: int = 0, heartbeat: float = 15
    ) -> AsyncGenerator[StreamFrame | None, None]:
        cursor = from_seq
        heartbeat_at = asyncio.get_running_loop().time()
        while True:
            if state.task is None and not state.terminal:
                state.frames = await self.repository.ensure_terminal_event(state.run.id)
                current_run = await self.repository.get_run(state.run.id)
                state.terminal = bool(state.frames and state.frames[-1].kind in {"end", "error"})
                if not state.frames and current_run.status in TERMINAL_STATUSES:
                    raise ServiceError("stream_gone", "该运行没有可恢复的持久事件", 410)
            async with state.condition:
                if cursor >= len(state.frames) and not state.terminal:
                    try:
                        await asyncio.wait_for(
                            state.condition.wait(),
                            timeout=heartbeat if state.task else min(heartbeat, 0.1),
                        )
                    except TimeoutError:
                        pass
                frames = state.frames[cursor:]
                finished = state.terminal
            for frame in frames:
                cursor = frame.seq + 1
                heartbeat_at = asyncio.get_running_loop().time()
                yield frame
            if finished:
                return
            if not frames and asyncio.get_running_loop().time() - heartbeat_at >= heartbeat:
                heartbeat_at = asyncio.get_running_loop().time()
                yield None

    async def close(self) -> None:
        if self.workspace_checkpoint_controls is not None:
            self.workspace_checkpoint_controls.assert_external_close()
        if self.tree_exchanges is not None:
            self.tree_exchanges.assert_external_close()
        if self.tree_moves is not None:
            self.tree_moves.assert_external_close()
        if self.commands is not None:
            self.commands.assert_external_close()
        self._closed = True
        for state in list(self._runs.values()):
            if state.task is not None and not state.terminal:
                await self.stop(state.run.id)
        if self._owns_workspace_changes:
            await self.workspace_changes.close()
        await self.tools.close()
        if self._owns_tracer:
            await self.tracer.close()
