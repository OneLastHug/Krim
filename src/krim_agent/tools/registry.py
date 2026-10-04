"""统一执行链；SRC-DOC-001 的工具语义、SRC-OSS-003 的输入验证。"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
from collections import deque
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any

from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError

from krim_agent.protocol.models import check_json

from .core import (
    Decision,
    ShortCircuit,
    TextContent,
    ToolAudit,
    ToolContext,
    ToolDefinition,
    ToolError,
    ToolExecutionBudget,
    ToolInfo,
    ToolObservation,
    ToolResult,
)

Observer = Callable[[ToolAudit], object]
ResultObserver = Callable[[ToolObservation], object]
GlobalPre = Callable[[ToolDefinition, ToolContext], Awaitable[ShortCircuit | None]]
GlobalPost = Callable[[ToolDefinition, Any, ToolContext], Awaitable[Any]]


def _unique_callbacks[Callback](values: list[Callback]) -> list[Callback]:
    result: list[Callback] = []
    for value in values:
        if not any(value is previous for previous in result):
            result.append(value)
    return result


class ToolRegistry:
    def __init__(self, *, max_depth: int = 8, default_timeout: float = 10) -> None:
        self.max_depth = max_depth
        self.default_timeout = default_timeout
        self._definitions: dict[str, ToolDefinition] = {}
        self._global_pre: list[GlobalPre] = []
        self._global_post: list[GlobalPost] = []
        self._observers: list[Observer] = []
        self._result_observers: list[ResultObserver] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self.audit: deque[ToolAudit] = deque(maxlen=4096)
        self._run_audits: dict[str, list[ToolAudit]] = {}

    def audits_for_run(self, run_id: str) -> list[ToolAudit]:
        return list(self._run_audits.get(run_id, []))

    def clear_run_audits(self, run_id: str) -> None:
        self._run_audits.pop(run_id, None)

    def fork(self, *, controls: ToolRegistry | None = None) -> ToolRegistry:
        """保留可信定义与全局执行链，隔离审计缓存和观察任务生命周期。"""
        target = ToolRegistry(
            max_depth=min(self.max_depth, controls.max_depth) if controls else self.max_depth,
            default_timeout=min(self.default_timeout, controls.default_timeout)
            if controls
            else self.default_timeout,
        )
        target.register_many(*self.definitions)
        target._global_pre = list(self._global_pre)
        target._global_post = list(self._global_post)
        target._observers = list(self._observers)
        target._result_observers = list(self._result_observers)
        if controls is not None:
            target._global_pre = _unique_callbacks([*controls._global_pre, *self._global_pre])
            target._global_post = _unique_callbacks([*self._global_post, *controls._global_post])
            target._observers = _unique_callbacks([*controls._observers, *self._observers])
            target._result_observers = _unique_callbacks(
                [*controls._result_observers, *self._result_observers]
            )
        return target

    def register(self, definition: ToolDefinition) -> ToolDefinition:
        if definition.name in self._definitions:
            raise ValueError(f"重复工具: {definition.name}")
        self._definitions[definition.name] = definition
        return definition

    def register_many(self, *definitions: ToolDefinition) -> None:
        for definition in definitions:
            self.register(definition)

    def get(self, name: str) -> ToolDefinition:
        if name not in self._definitions:
            raise ToolError("tool_not_found", f"未注册工具: {name}")
        return self._definitions[name]

    @property
    def definitions(self) -> list[ToolDefinition]:
        return list(self._definitions.values())

    def list_tools(self) -> list[ToolInfo]:
        return [definition.schema() for definition in self._definitions.values()]

    def add_global_pre(self, hook: GlobalPre) -> None:
        self._global_pre.append(hook)

    def add_global_post(self, hook: GlobalPost) -> None:
        self._global_post.append(hook)

    def add_observer(self, observer: Observer) -> None:
        self._observers.append(observer)

    def add_result_observer(self, observer: ResultObserver) -> None:
        self._result_observers.append(observer)

    async def call_tool(
        self, name: str, arguments: dict[str, Any], context: ToolContext | None = None
    ) -> Any:
        return await self._execute(name, arguments, context, lambda _definition, value: value)

    async def _execute[Result](
        self,
        name: str,
        arguments: dict[str, Any],
        context: ToolContext | None,
        consume: Callable[[ToolDefinition, Any], Result],
    ) -> Result:
        parent = context or ToolContext(self, timeout=self.default_timeout)
        try:
            definition = self.get(name)
        except ToolError:
            self._observe(
                ToolAudit(
                    call_id=parent.call_id,
                    run_id=parent.run_id,
                    name=name,
                    risk="write",
                    decision="deny",
                    depth=parent.depth + 1,
                    ok=False,
                    code="tool_not_found",
                )
            )
            raise
        if parent.depth >= self.max_depth:
            raise ToolError("max_depth", f"工具嵌套超过 {self.max_depth} 层")
        child = parent.child(dict(arguments))
        decision: Decision = "deny"
        code: str | None = None
        budget: ToolExecutionBudget | None = None
        try:
            async with asyncio.timeout(child.timeout) as execution_timeout:
                budget = ToolExecutionBudget(execution_timeout)
                child.execution_timeouts = (*child.execution_timeouts, budget)
                parsed = definition.parameters.model_validate(child.arguments, strict=True)
                child.arguments = parsed.model_dump(by_alias=True)
                original_arguments = copy.deepcopy(child.arguments)
                decision = (
                    await child.policy(definition, child)
                    if child.policy is not None
                    else "allow"
                    if definition.risk == "read"
                    else "deny"
                )
                if decision == "ask" and child.approval_handler is not None:
                    async with child.human_wait():
                        allowed = await child.approval_handler(definition, child)
                    decision = "allow" if allowed else "deny"
                if decision not in {"allow", "audit"}:
                    code = "approval_required" if decision == "ask" else "permission_denied"
                    raise ToolError(code, f"工具没有执行许可: {name}")
                short: ShortCircuit | None = None
                for hook in tuple(self._global_pre):
                    short = await hook(definition, child)
                    if short is not None:
                        break
                if short is None and definition.pre_execute is not None:
                    short = await definition.pre_execute(child)
                parsed = definition.parameters.model_validate(child.arguments, strict=True)
                if child.arguments != original_arguments and parent.policy is not None:
                    decision = await parent.policy(definition, child)
                    if decision == "ask" and child.approval_handler is not None:
                        async with child.human_wait():
                            allowed = await child.approval_handler(definition, child)
                        decision = "allow" if allowed else "deny"
                    if decision not in {"allow", "audit"}:
                        code = "approval_required" if decision == "ask" else "permission_denied"
                        raise ToolError(code, "中间件修改后的参数没有执行许可")
                result = (
                    short.value if short is not None else await definition.executor(parsed, child)
                )
                if definition.post_execute is not None:
                    result = await definition.post_execute(result, child)
                for post in tuple(self._global_post):
                    result = await post(definition, result, child)
                return consume(definition, result)
        except ValidationError as exc:
            code = "invalid_input"
            raise ToolError(code, "工具参数校验失败") from exc
        except TimeoutError as exc:
            code = "timeout"
            raise ToolError(code, f"工具执行超时: {name}") from exc
        except asyncio.CancelledError:
            code = "cancelled"
            raise
        except ToolError as exc:
            code = exc.code
            raise
        except Exception as exc:
            code = "execution_error"
            raise ToolError(code, f"工具执行失败: {name}") from exc
        finally:
            if budget is not None:
                budget.active = False
            self._observe(
                ToolAudit(
                    call_id=child.call_id,
                    run_id=child.run_id,
                    name=name,
                    risk=definition.risk,
                    decision=decision,
                    depth=child.depth,
                    ok=code is None,
                    code=code,
                )
            )

    async def invoke(
        self, name: str, arguments: dict[str, Any], context: ToolContext
    ) -> ToolResult:
        try:
            result = await self._execute(name, arguments, context, self._normalize)
        except ToolError as error:
            result = self._error_result(error)
        for observer in tuple(self._result_observers):
            snapshot = ToolObservation(
                call_id=context.call_id,
                run_id=context.run_id,
                name=name,
                result=result.model_copy(deep=True),
            )
            self._schedule(partial(observer, snapshot))
        return result

    @staticmethod
    def _normalize(definition: ToolDefinition, raw: Any) -> ToolResult:
        try:
            value = (
                raw.model_dump(mode="json", by_alias=True) if isinstance(raw, BaseModel) else raw
            )
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
            data: JsonValue = TypeAdapter(JsonValue).validate_json(encoded)
            check_json(data)
        except (TypeError, ValueError, RecursionError) as error:
            raise ToolError("invalid_output", "工具结果不是有效的有限 JSON") from error
        try:
            definition.validate_output(data)
        except ValueError as error:
            raise ToolError("invalid_output_schema", "工具结果不符合声明的输出 Schema") from error
        text = raw if isinstance(raw, str) else encoded
        return ToolResult(content=[TextContent(text=text)], data=data)

    @staticmethod
    def _error_result(error: ToolError) -> ToolResult:
        return ToolResult(content=[TextContent(text=str(error))], is_error=True, code=error.code)

    def _observe(self, audit: ToolAudit) -> None:
        self.audit.append(audit)
        if audit.run_id != "local":
            self._run_audits.setdefault(audit.run_id, []).append(audit)
        for observer in tuple(self._observers):
            self._schedule(partial(observer, audit))

    def _schedule(self, callback: Callable[[], object]) -> None:
        if len(self._tasks) >= 128:
            return

        async def deliver() -> None:
            try:
                async with asyncio.timeout(1):
                    value: object
                    if inspect.iscoroutinefunction(callback):
                        value = callback()
                    else:
                        value = await asyncio.to_thread(callback)
                    if inspect.isawaitable(value):
                        await value
            except Exception:
                pass

        task = asyncio.create_task(deliver())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
