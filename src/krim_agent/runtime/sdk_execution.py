"""Shared preparation, dispatch, and completion boundary for SDK-backed runs."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

import ai
from pydantic import JsonValue

from krim_agent.agent_sdk.adapter import (
    ProbeResult,
    ProbeStopped,
    SDKAdapter,
    StepBoundary,
    StepCommit,
)
from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider, agent_tools, sdk_messages
from krim_agent.config import DEFAULT_TOOL_PARALLELISM, Settings
from krim_agent.context.types import ContextPolicy, HistorySource
from krim_agent.contracts import Message, RunEvent, ServiceError, fingerprint
from krim_agent.development.contracts import (
    EnvironmentHandle,
    OperationReceipt,
    OperationStore,
    RunOwnership,
)
from krim_agent.providers.types import ModelProvider
from krim_agent.runtime.contracts import ExecutionBudget, RunStore
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import Approve, ToolContext, ToolExecutor

from .model_step import ModelStep
from .recovery import SDKResume

ApprovalRequest = Callable[
    [Invocation, float | None], Awaitable[dict[str, JsonValue]]
]
EventEmitter = Callable[[str, dict[str, JsonValue]], Awaitable[RunEvent]]
HistoryBuilder = Callable[[ModelStep, int | None], Awaitable["SDKHistory"]]
CompletionHook = Callable[[], Awaitable[None]]


class Parked(Exception):
    """The SDK saved an approval checkpoint and the run must await a reply."""


@dataclass(frozen=True)
class SDKExecutionPolicy:
    """One resolved set of limits shared by host-specific SDK run adapters."""

    max_steps: int | None
    max_output_bytes: int | None
    max_tools_per_step: int | None
    max_tool_calls: int | None
    parallelism: int = DEFAULT_TOOL_PARALLELISM
    model_timeout: float | None = None
    tool_timeout: float | None = None
    approval_timeout: float | None = None
    batch_approvals: bool = True
    fail_on_tool_error: bool = True

    def __post_init__(self) -> None:
        if type(self.parallelism) is not int or self.parallelism < 1:
            raise ValueError("invalid SDK execution parallelism")
        if any(value is not None and (type(value) is not int or value < 1)
               for value in (self.max_steps, self.max_tools_per_step, self.max_tool_calls)):
            raise ValueError("invalid SDK execution count limit")
        if self.max_output_bytes is not None and (
            type(self.max_output_bytes) is not int or self.max_output_bytes < 1
        ):
            raise ValueError("invalid SDK output limit")
        for value in (self.model_timeout, self.tool_timeout, self.approval_timeout):
            if value is not None and (type(value) not in (int, float) or value <= 0):
                raise ValueError("invalid SDK execution timeout")
        if type(self.batch_approvals) is not bool or type(self.fail_on_tool_error) is not bool:
            raise ValueError("invalid SDK execution behavior")

    @classmethod
    def from_settings(cls, settings: Settings) -> SDKExecutionPolicy:
        return cls(
            max_steps=settings.max_steps,
            max_output_bytes=settings.max_output_bytes,
            max_tools_per_step=settings.max_tools_per_step,
            max_tool_calls=settings.max_tool_calls,
            parallelism=settings.read_parallelism,
            model_timeout=settings.model_timeout,
            tool_timeout=settings.tool_timeout,
            approval_timeout=settings.approval_timeout,
        )


@dataclass(frozen=True)
class SDKHistory:
    history: CompactorHistory | None = None
    context_policy: ContextPolicy | None = None
    session_sources: Sequence[HistorySource] | None = None


@dataclass(frozen=True)
class SDKExecutionPlan:
    store: RunStore
    provider: ModelProvider
    executor: ToolExecutor
    settings: Settings
    run_id: str
    model_id: str
    initial_history: Sequence[Message]
    policy: SDKExecutionPolicy
    execution_budget: ExecutionBudget
    tool_context: ToolContext
    emit: EventEmitter
    check: Callable[[], None]
    approval_request: ApprovalRequest | Approve
    read_result: Callable[[str], Awaitable[JsonValue]]
    read_receipt: Callable[[OperationReceipt], Awaitable[JsonValue]]
    build_history: HistoryBuilder
    operations: OperationStore | None = None
    environment: EnvironmentHandle | None = None
    ownership: RunOwnership | None = None
    run_tools: Any | None = None
    host_media: Any | None = None
    startup_gate: Callable[[], asyncio.Lock | None] | None = None
    authorize_media: Callable[[Any], Awaitable[None]] | None = None
    resume: SDKResume | None = None
    workspace: bool = False
    on_completed: CompletionHook | None = None
    subagents: Any | None = None


async def execute_sdk(plan: SDKExecutionPlan) -> ProbeResult:
    """Prepare the common SDK core, run it, then invoke one environment finalizer."""
    if plan.resume is not None and not isinstance(plan.resume, SDKResume):
        raise ServiceError("facts_corrupt")
    if (isinstance(plan.resume, SDKResume)
            and fingerprint(plan.resume.checkpoint) != plan.resume.fingerprint):
        raise ServiceError("facts_corrupt")

    policy = plan.policy
    stepper = ModelStep(
        plan.provider,
        plan.emit,
        plan.check,
        max_steps=policy.max_steps,
        max_output=policy.max_output_bytes,
        model_timeout=policy.model_timeout,
        deadline=plan.execution_budget.deadline,
        workspace=plan.workspace,
        execution_budget=plan.execution_budget,
        request_slot=(
            (lambda index: plan.subagents.request_slot(plan.run_id, index, plan.check))
            if plan.subagents is not None else None
        ),
    )
    request_limit = plan.settings.request_capacity(plan.provider)
    history = await plan.build_history(stepper, request_limit)
    if history.history is not None and history.history.compactor.step is not stepper:
        raise ValueError("compaction must share the host ModelStep")

    model_provider = HostModelProvider(
        stepper,
        plan.executor.registry,
        max_group=policy.max_tools_per_step,
        max_calls=policy.max_tool_calls,
        request_bytes=request_limit,
        context_policy=history.context_policy,
        history=history.history,
        session_sources=history.session_sources,
        run_tools=plan.run_tools,
        host_media=plan.host_media,
    )
    host = HostExecution(
        plan.tool_context,
        plan.execution_budget,
        plan.read_receipt,
        model_provider.step_index,
        model_provider.checkpoint,
        model_provider.restore,
    )
    async def input_boundary(boundary: StepBoundary) -> Any:
        return await plan.subagents.prepare_input(
            plan.run_id, list(boundary.messages), adapter.checkpoint, initial=False,
        )

    async def step_committed(commit: StepCommit) -> None:
        await plan.subagents.commit_step(
            plan.run_id, [*commit.messages, commit.response], adapter.checkpoint,
        )

    async def sdk_event(event: Any) -> None:
        await plan.subagents.on_event(plan.run_id, event)

    adapter = SDKAdapter(
        store=plan.store,
        operations=plan.operations,
        executor=plan.executor,
        tools=agent_tools(plan.executor.registry),
        environment=plan.environment,
        ownership=plan.ownership,
        run_id=plan.run_id,
        read_parallelism=policy.parallelism,
        batch_approvals=policy.batch_approvals,
        fail_on_tool_error=policy.fail_on_tool_error,
        read_result=plan.read_result,
        approval_request=cast(ApprovalRequest, plan.approval_request),
        host=host,
        authorize_media=cast(Any, plan.authorize_media),
        startup_gate=plan.startup_gate,
        step_boundary=input_boundary if plan.subagents is not None else None,
        on_step_commit=step_committed if plan.subagents is not None else None,
        on_event=sdk_event if plan.subagents is not None else None,
        recover_tool=plan.subagents.recover_tool if plan.subagents is not None else None,
        max_events=(plan.subagents.configuration.max_cached_events
                    if plan.subagents is not None else None),
    )

    try:
        initial = sdk_messages(plan.initial_history)
        resuming = plan.resume is not None
        replies = plan.resume.replies if plan.resume is not None else None
        if plan.subagents is not None and not resuming:
            initial.extend(await plan.subagents.prepare_input(
                plan.run_id, initial, adapter.checkpoint, initial=True,
            ))
        while True:
            result = await adapter.run(
                ai.Model(id=plan.model_id, provider=model_provider),
                initial,
                resume=resuming,
                replies=replies,
            )
            if result.status != "completed" or plan.subagents is None:
                break
            # A final response is not root-task completion while accepted
            # descendants or inputs remain. Waiting here owns no model permit.
            extra = await plan.subagents.finish_or_continue(
                plan.run_id, list(result.messages), adapter.checkpoint, plan.check,
            )
            if not extra:
                break
            initial = [*result.messages, *extra]
            resuming, replies = False, None
    except ProbeStopped:
        raise ServiceError("cancelled") from None
    except BaseExceptionGroup as group:
        def first(error: BaseException) -> BaseException:
            if isinstance(error, BaseExceptionGroup):
                return first(error.exceptions[0])
            return error

        cause = first(group)
        if isinstance(cause, (ServiceError, asyncio.CancelledError, TimeoutError)):
            raise cause from None
        raise ServiceError("execution_error") from None

    if result.status == "awaiting":
        raise Parked()
    if result.status != "completed":
        raise ServiceError("execution_error")
    plan.check()
    if plan.on_completed is not None:
        await plan.on_completed()
    plan.check()
    return result
