"""有界模型→工具→模型循环；不装配服务、数据库或旧管理体系。"""

from __future__ import annotations

import asyncio
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable
from functools import partial
from typing import cast

from pydantic import JsonValue

from krim_agent.config import Settings
from krim_agent.context import builder, compaction
from krim_agent.context.types import SessionContext
from krim_agent.contracts import Message, RunEvent, ServiceError, ToolObservation, encoded
from krim_agent.providers.types import ModelProvider, ModelToolCall
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import Approve, ToolContext, ToolExecutor
from krim_agent.workspace.operations import WorkspaceOperations
from krim_agent.workspace.verification import Verifier, stage_result

from . import model_step
from .contracts import ExecutionBudget, RunStore, WorkRuntime, settled


class AgentLoop:
    def __init__(
        self, store: RunStore, provider: ModelProvider, executor: ToolExecutor, settings: Settings,
        *, work: WorkRuntime | None = None,
    ) -> None:
        self.store, self.provider, self.executor, self.settings = (
            store,
            provider,
            executor,
            settings,
        )
        self.work = cast(WorkspaceOperations | None, work)
        self._verifier = Verifier(self.store, self.work.artifacts) if self.work else None
        self._d4_seal = self._verifier.assert_current if self._verifier else None
        self._d3_seal = (compaction.binding_token(), model_step.binding_token(),
            builder.binding_token())

    async def run(
        self,
        run_id: str,
        approve: Approve,
        assert_target_current: Callable[[], None] = lambda: None,
        execution_budget: ExecutionBudget | None = None,
    ) -> None:
        def check() -> None:
            assert_target_current()
            if self._d3_seal != (compaction.binding_token(), model_step.binding_token(),
                                 builder.binding_token()):
                raise ServiceError("target_binding_changed")
            if self._d4_seal is not None:
                self._d4_seal()
        check()
        snapshot = await self.store.snapshot(run_id)
        check()
        messages = [
            Message(role="system", content=self.settings.system_prompt),
            Message(role="user", content=snapshot.request.text),
        ]
        operations = self.work
        frame = None
        runtime: dict[str, JsonValue] = {}
        scope = None
        if operations is not None:
            frame = await operations.prepare(run_id)
            check()
            runtime = cast(dict[str, JsonValue], frame.conditions["runtime"])
            messages = [Message(role="user", content=snapshot.request.text)]
        elif snapshot.session_context is not None:
            if not isinstance(snapshot.session_context, SessionContext):
                raise ServiceError("workspace_contract_required")
            messages = list(snapshot.session_context.context.messages)
        if execution_budget is None:
            timeout = (
                cast(float, runtime["run_timeout_seconds"]) if frame else self.settings.run_timeout
            )
            deadline = asyncio.get_running_loop().time() + max(
                0, snapshot.run.created_at + timeout - time.time())
            execution_budget = ExecutionBudget(deadline)
        if frame is not None and operations is not None:
            scope = operations.scope(frame, check, execution_budget.deadline, execution_budget)
        max_steps = cast(int, runtime["model_steps"]) if frame else self.settings.max_steps
        max_group = cast(int,
            runtime["max_tools_per_step"]) if frame else self.settings.max_tools_per_step
        max_calls = cast(int, runtime["tool_calls"]) if frame else self.settings.max_tool_calls
        tool_timeout = cast(float,
            runtime["tool_timeout_seconds"]) if frame else self.settings.tool_timeout
        approval_timeout = cast(float, runtime["approval_timeout_seconds"]
            ) if frame else self.settings.approval_timeout
        tool_count = 0
        append_event = partial(self.store.append, run_id)
        compressor: compaction.Compactor | None = None

        async def emit(kind: str, data: dict[str, JsonValue]) -> RunEvent:
            event = await append_event(kind, data)
            # ModelStep的model_start本身是await；开实际stream前再核实准入锚。
            if kind == "model_start" and compressor is not None:
                await compressor.store.work_read(compressor.verify)
            check()
            return event
        stepper = model_step.ModelStep(self.provider, emit, check, max_steps=max_steps,
            max_output=self.settings.max_output_bytes, deadline=scope.deadline if scope else None,
            workspace=frame is not None,
            execution_budget=execution_budget if frame is not None else None)
        provider_limit = getattr(getattr(self.provider, "config", None),
                                 "max_request_bytes", self.settings.max_request_bytes)
        request_limit = min(self.settings.max_request_bytes, provider_limit)
        if frame is not None and scope is not None:
            compressor = compaction.Compactor(cast(SQLiteStore, self.store), frame, scope.access,
                stepper, check, self.settings.system_prompt,
                request_limit,
                cast(dict[str, JsonValue], snapshot.target["provider"]),
                source_anchor=cast(str, getattr(
                    snapshot.session_context, "source_fingerprint", None)))
        conversation = list(messages)
        while stepper.index < max_steps:
            check()
            self.executor.registry.assert_current()
            schemas = self.executor.registry.schemas()
            remaining_tools = max_calls-tool_count
            if compressor is not None:
                messages = await compressor.prepare(
                    run_id,
                    conversation,
                    schemas,
                    max_group=max_group,
                    remaining_tools=remaining_tools,
                )
                check()
            request_budget = stepper.request_budget(max_group, remaining_tools)
            request_messages = stepper.with_budget_prompt(messages, request_budget)
            request_tools = schemas if request_budget.max_tool_calls_this_step > 0 else []
            if compressor is None:
                if len(encoded({
                    "messages": [m.model_dump(mode="json") for m in request_messages],
                    "tools": request_tools,
                }).encode()) > request_limit:
                    raise ServiceError("input_limit")
                if isinstance(snapshot.session_context, SessionContext):
                    builder.check_budget(
                        request_messages,
                        request_tools,
                        snapshot.session_context.context.policy,
                        request_limit,
                    )
            response = await stepper.call(
                request_messages,
                request_tools,
                max_group=max_group,
                remaining_tools=remaining_tools,
                request_budget=request_budget,
                request_prepared=True,
            )
            check()
            text, calls, step = response.text, list(response.calls), response.index
            if not calls:
                if not text:
                    raise ServiceError("no_output")
                if frame is not None:
                    assert self._verifier is not None and scope is not None
                    verdict = await self._verifier.verify(run_id, frame, scope.access,
                        check=scope.check, deadline=scope.deadline)
                    scope.check()
                    producer = asyncio.current_task()
                    assert producer is not None
                    stage_result(verdict, issuer=self._verifier, store=self.store,
                        producer=producer, access=scope.access,
                        check=scope.check, run_deadline=scope.deadline)
                    scope.check()
                    if verdict.state != "passed":
                        raise ServiceError("completion_unverified")
                return
            conversation.append(
                Message(
                    role="assistant",
                    content=text,
                    tool_calls=[call.model_dump(mode="json") for call in calls],
                )
            )
            async def execute(call: ModelToolCall, step: int = step) -> ToolObservation:
                check()
                try:
                    spec = self.executor.registry.get(call.name)
                    digest = spec.fingerprint
                except ServiceError as error:
                    if error.code != "unknown_tool":
                        raise
                    digest = ""
                invocation = Invocation(run_id,
                    str(uuid.uuid4()) if frame else secrets.token_hex(16),
                    secrets.token_hex(8), step, call.name, encoded(call.arguments), digest,
                    provider_call_id=call.id)
                context = ToolContext(cast(Callable[[str, dict[str, JsonValue]],
                    Awaitable[RunEvent]], emit),
                                      approve, tool_timeout, check, scope, approval_timeout)
                return await self.executor.execute(invocation, context)

            index = 0
            while index < len(calls):
                group = [calls[index]]
                if frame is not None:
                    parallel = cast(int, runtime["read_parallelism"])
                    while len(group) < parallel and index+len(group) < len(calls):
                        try:
                            risks = [
                                self.executor.registry.get(
                                call.name).risk for call in group+[calls[index+len(group)]]]
                        except ServiceError:
                            break
                        if any(risk != "read" for risk in risks):
                            break
                        group.append(calls[index+len(group)])
                tool_count += len(group)
                results: list[ToolObservation | BaseException]
                if len(group) == 1:
                    # legacy与串行写沿原producer Task；只有真实并行读创建有界子Task。
                    results = [await execute(group[0])]
                else:
                    tasks = [asyncio.create_task(execute(call)) for call in group]
                    try:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                    finally:
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await settled(asyncio.gather(*tasks, return_exceptions=True))
                assert_target_current()
                for call, result in zip(group, results, strict=True):
                    if isinstance(result, BaseException):
                        raise result
                    if result.status != "ok":
                        raise ServiceError(result.code or "tool_error")
                    conversation.append(Message(role="tool", content=encoded(result.value),
                        call_id=call.id))
                index += len(group)
            if compressor is None:
                messages = conversation
        raise ServiceError("step_limit")
