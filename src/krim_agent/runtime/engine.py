"""有界模型→工具→模型循环；不装配服务、数据库或旧管理体系。"""

from __future__ import annotations

import asyncio
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import replace
from functools import partial
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.config import Settings
from krim_agent.context import builder, compaction
from krim_agent.context.types import SessionContext
from krim_agent.contracts import (
    Message,
    RunEvent,
    ServiceError,
    ToolObservation,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.providers.types import ModelProvider, ModelToolCall
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import Admission, Approve, ToolContext, ToolExecutor
from krim_agent.workspace.operations import WorkspaceOperations
from krim_agent.workspace.verification import Verifier, stage_result

from . import model_step
from .contracts import ExecutionBudget, RunStore, WorkRuntime, settled
from .recovery import PendingAsk, RecoveryState, build_checkpoint

type ApprovalRequest = Callable[[Invocation, float | None], Awaitable[dict[str, JsonValue]]]
# 停靠回调：引擎用它构造 approval_request 事件内容（服务侧负责溯源、
# WorkLimits 校验与期限计算；第二个参数是执行预算剩余，ADR-0015 §4）。


async def _no_inprocess_wait(invocation: Invocation, timeout_seconds: float) -> bool:
    """引擎路径不再有进程内审批等待：ask 一律停靠，由恢复执行。"""
    raise ServiceError("approval_unavailable")


class Parked(Exception):
    """本步存在待人工审批的调用：恢复点已写入，运行停靠为 awaiting。"""


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
        approval_request: ApprovalRequest | Approve,
        assert_target_current: Callable[[], None] = lambda: None,
        execution_budget: ExecutionBudget | None = None,
        resume: RecoveryState | None = None,
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
        if resume is not None:
            # 停靠恢复（ADR-0015）：对话/计数/步号从恢复点读回；先收尾停靠批。
            conversation = list(resume.conversation)
            messages = conversation
            stepper.index = resume.step
            stepper.output_bytes = resume.output_bytes
            stepper.seen = set(resume.seen)
            tool_count = resume.tool_count
            await self._finish_pending(
                resume.pending,
                conversation,
                ToolContext(cast(Callable[[str, dict[str, JsonValue]],
                    Awaitable[RunEvent]], emit),
                    _no_inprocess_wait, tool_timeout, check, scope, approval_timeout),
                check,
                emit,
            )
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
            # 入场判定与集中受理（ADR-0015）：全批按声明序先判定、后发射 tool_start；
            # 非 ask 调用按相邻连读分组执行（read 并发、read/write 不并发），
            # workspace 与非 workspace 线统一适用。
            context = ToolContext(cast(Callable[[str, dict[str, JsonValue]],
                Awaitable[RunEvent]], emit),
                                  _no_inprocess_wait, tool_timeout, check, scope,
                                  approval_timeout)

            def build(call: ModelToolCall, step: int = step) -> Invocation:
                check()
                try:
                    spec = self.executor.registry.get(call.name)
                    digest = spec.fingerprint
                except ServiceError as error:
                    if error.code != "unknown_tool":
                        raise
                    digest = ""
                return Invocation(run_id,
                    str(uuid.uuid4()) if frame else secrets.token_hex(16),
                    secrets.token_hex(8), step, call.name, encoded(call.arguments), digest,
                    provider_call_id=call.id)

            def observation(invocation: Invocation, *, status: str,
                            code: str | None) -> dict[str, JsonValue]:
                return ToolObservation(
                    call_id=invocation.call_id,
                    provider_call_id=invocation.provider_call_id,
                    arguments=cast(dict[str, JsonValue], parse_json(
                        invocation.arguments_json)),
                    span_id=invocation.span_id, step_index=invocation.step_index,
                    name=invocation.name, status=cast(Any, status),
                    executor_started=False, result_available=False, code=code,
                    decision="deny").model_dump(mode="json")

            invocations = [build(call) for call in calls]
            admissions: list[Admission] = []
            for invocation in invocations:  # 声明序：先受理、再判定，全部通过后统一执行
                await emit("tool_start", observation(invocation, status="unknown", code=None))
                try:
                    admission = await self.executor.admit(
                        invocation, context, timeout_seconds=tool_timeout)
                except ServiceError as error:
                    # 入场硬失败：如实补失败观察（保持既有事实形状），整次运行失败。
                    await emit("tool_end", observation(
                        invocation, status="failed", code=error.code))
                    raise
                admissions.append(replace(admission, start_emitted=True))
            pending_asks: list[tuple[Invocation, Admission]] = []
            denied: list[tuple[Invocation, Admission]] = []
            for invocation, admission in zip(invocations, admissions, strict=True):
                if admission.decision == "deny":
                    # 软拒绝（ADR-0015 §7）：记录观察并以固定模板回传模型，运行继续
                    code = admission.code or "permission_denied"
                    await emit("tool_end", observation(
                        invocation, status="denied", code=code))
                    denied.append((invocation, admission))
                if admission.decision == "ask":
                    pending_asks.append((invocation, admission))
            tool_count += len(admissions)  # 声明即计入总量预算（含待批）
            executable = [
                (invocation, admission)
                for invocation, admission in zip(invocations, admissions, strict=True)
                if admission.decision == "allow"
            ]
            index = 0
            while index < len(executable):
                group: list[tuple[Invocation, Admission]] = [executable[index]]
                parallel = cast(int, runtime["read_parallelism"]) if frame \
                    else self.settings.read_parallelism
                while len(group) < parallel and index+len(group) < len(executable):
                    candidate = executable[index+len(group)][1]
                    if candidate.spec.risk != "read" or group[-1][1].spec.risk != "read":
                        break
                    group.append(executable[index+len(group)])
                results: list[ToolObservation | BaseException]
                if len(group) == 1:
                    # 串行写与单调用沿原producer Task；只有真实并行读创建有界子Task。
                    results = [await self.executor.execute(
                        group[0][0], context, group[0][1])]
                else:
                    tasks = [asyncio.create_task(
                        self.executor.execute(invocation, context, admission))
                        for invocation, admission in group]
                    try:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                    finally:
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await settled(asyncio.gather(*tasks, return_exceptions=True))
                assert_target_current()
                for (invocation, _), result in zip(group, results, strict=True):
                    if isinstance(result, BaseException):
                        raise result
                    if result.status != "ok":
                        raise ServiceError(result.code or "tool_error")
                    conversation.append(Message(role="tool", content=encoded(result.value),
                        call_id=invocation.provider_call_id))
                index += len(group)
            for invocation, admission in denied:  # 拒绝按声明序回传（ADR-0015 §7）
                conversation.append(Message(
                    role="tool",
                    content=encoded({"call_id": invocation.call_id,
                        "name": invocation.name, "status": "denied",
                        "code": admission.code or "permission_denied"}),
                    call_id=invocation.provider_call_id))
            if pending_asks:
                # 剩余 ask：落审批请求（同批同刻到期）、写恢复点、停靠（ADR-0015）。
                budget_remaining = (
                    execution_budget.remaining if execution_budget is not None else None)
                batch_expires = time.time() + approval_timeout
                for invocation, _ in pending_asks:
                    data = await approval_request(invocation, budget_remaining)
                    if not isinstance(data, dict) or "expires_at" not in data:
                        raise ServiceError("execution_error")  # 停靠回调契约破裂
                    data["expires_at"] = min(
                        cast(float, data["expires_at"]), batch_expires
                    )
                    await emit("approval_request", data)
                checkpoint = build_checkpoint(
                    conversation=conversation,
                    step=stepper.index,
                    output_bytes=stepper.output_bytes,
                    tool_count=tool_count,
                    seen=set(stepper.seen),
                    budget_remaining=budget_remaining,
                    pending=[invocation for invocation, _ in pending_asks],
                )
                await self.store.write_checkpoint(
                    run_id, checkpoint, fingerprint(checkpoint)
                )
                raise Parked()
            if compressor is None:
                messages = conversation
        raise ServiceError("step_limit")

    async def _finish_pending(
        self,
        pending: tuple[PendingAsk, ...],
        conversation: list[Message],
        context: ToolContext,
        check: Callable[[], None],
        emit: Callable[[str, dict[str, JsonValue]], Awaitable[RunEvent]],
    ) -> None:
        """恢复时收尾停靠批：指纹漂移拒绝续跑；批准的执行、拒绝的记观察回传。"""
        from pydantic import ValidationError

        for item in pending:
            check()
            self.executor.registry.assert_current()
            spec = self.executor.registry.get(item.invocation.name)
            if spec.fingerprint != item.invocation.tool_hash:
                raise ServiceError("tool_binding_changed")  # 停靠期间工具被换
            if not item.allow:
                observation = ToolObservation(
                    call_id=item.invocation.call_id,
                    provider_call_id=item.invocation.provider_call_id,
                    arguments=cast(dict[str, JsonValue],
                        parse_json(item.invocation.arguments_json)),
                    span_id=item.invocation.span_id,
                    step_index=item.invocation.step_index,
                    name=item.invocation.name, status="denied",
                    executor_started=False, result_available=False,
                    code="approval_denied", decision="ask")
                await emit("tool_end", observation.model_dump(mode="json"))
                conversation.append(Message(
                    role="tool",
                    content=encoded({"call_id": observation.call_id,
                        "name": observation.name, "status": "denied",
                        "code": "approval_denied"}),
                    call_id=item.invocation.provider_call_id))
                continue
            try:
                parsed = spec.parameters.model_validate(
                    parse_json(item.invocation.arguments_json), strict=True)
            except (ValidationError, ValueError):
                raise ServiceError("invalid_input") from None
            admission = replace(
                Admission(spec, parsed, "ask"), start_emitted=True, approved=True)
            result = await self.executor.execute(item.invocation, context, admission)
            if result.status != "ok":
                raise ServiceError(result.code or "tool_error")
            conversation.append(Message(role="tool", content=encoded(result.value),
                call_id=item.invocation.provider_call_id))
