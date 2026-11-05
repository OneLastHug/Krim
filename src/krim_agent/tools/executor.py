"""唯一工具责任链：校验、否决、许可、审批、意图、执行、输出、审计。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, replace
from typing import cast

from pydantic import BaseModel, JsonValue, ValidationError

from krim_agent.contracts import (
    RunEvent,
    ServiceError,
    ToolObservation,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.runtime.contracts import ExecutionBudget, settled

from .core import Decision, Invocation, Policy, PostHook, PreHook, ToolSpec
from .mcp_schema import MCPContract
from .output import OutputContract
from .pipeline import CURRENT, Middleware, Scope, Wrap, _calling
from .registry import ToolRegistry

type Emit = Callable[[str, dict[str, JsonValue]], Awaitable[RunEvent]]
type Approve = Callable[[Invocation, float], Awaitable[bool]]


@dataclass(frozen=True)
class ToolContext:
    emit: Emit
    approve: Approve
    timeout: float
    assert_target_current: Callable[[], None] = lambda: None
    scope: Scope | None = None
    approval_timeout: float = 300


@dataclass(frozen=True)
class Admission:
    """入场判定结果（ADR-0015）：工具档案、已校验参数、策略判定。

    deny 时 code 给出稳定错误码；ask 表示需要人工批准（当前版本仍在
    执行核内等待，停靠化后由引擎接管）。start_emitted 表示引擎已按
    声明序集中发射过 tool_start，执行段不再重复发射。
    """

    spec: ToolSpec
    parsed: BaseModel
    decision: Decision
    code: str | None = None
    start_emitted: bool = False
    approved: bool = False  # ask 已经人工批准（停靠恢复执行时置位，不再等待）


class ToolExecutor:
    def __init__(
        self,
        registry: ToolRegistry,
        policy: Policy | None,
        pre: PreHook | None = None,
        post: PostHook | None = None,
        *,
        middleware: tuple[Middleware, ...] = (),
        wrap: Wrap | None = None,
    ) -> None:
        self.registry, self.policy, self.pre, self.post = registry, policy, pre, post
        self.middleware, self.wrap = tuple(middleware), wrap

    async def _decision(self, invocation: Invocation, spec: ToolSpec) -> Decision:
        if self.policy is None:
            return "deny"
        decisions = [await self.policy(invocation)]
        if spec.binding_target is not None:
            target = spec.binding_target
            decisions.append(await self.policy(replace(
                invocation, name=target.name, tool_hash=target.fingerprint)))
        if any(value not in {"allow", "ask"} for value in decisions):
            return "deny"
        return "ask" if "ask" in decisions else "allow"

    async def admit(
        self, invocation: Invocation, context: ToolContext, *, timeout_seconds: float
    ) -> Admission:
        """入场判定（ADR-0015）：指纹核对、严格入参校验、pre 钩子、middleware、策略判定。

        workspace 策略依赖 pipeline 的 CURRENT 上下文（无 scope 即拒绝），
        因此这里同样设置 CURRENT。硬失败抛 ServiceError；策略 deny 是软
        结论，返回带 code 的判定（由调用方决定终止或回传）。
        """
        with _calling(invocation):
            token = CURRENT.set(replace(context.scope,
                invocation=invocation) if context.scope is not None else None)
            try:
                async with asyncio.timeout(timeout_seconds):
                    self.registry.assert_current()
                    context.assert_target_current()
                    if context.scope is not None:
                        context.scope.check()
                    spec = self.registry.get(invocation.name)
                    if spec.fingerprint != invocation.tool_hash:
                        raise ServiceError("tool_binding_changed")
                    try:
                        parsed = spec.parameters.model_validate(
                            parse_json(invocation.arguments_json), strict=True)
                    except (ValidationError, ValueError):
                        raise ServiceError("invalid_input") from None

                    async def inspected(hook: PreHook, *, middleware: bool = False) -> str | None:
                        snapshot = replace(invocation)
                        before = asdict(snapshot)
                        try:
                            verdict = await hook(snapshot)
                            if asdict(snapshot) != before or (verdict is not None
                                and not isinstance(verdict, str)):
                                raise ValueError("Hook不得修改参数或伪造结果")
                        except Exception:
                            raise ServiceError(
                                "middleware_error" if middleware else "pre_hook_error") from None
                        self.registry.assert_current()
                        context.assert_target_current()
                        if context.scope is not None:
                            context.scope.check()
                        return verdict

                    if self.pre is not None and await inspected(self.pre) is not None:
                        raise ServiceError("hook_denied")
                    for middleware in self.middleware:
                        if await inspected(middleware, middleware=True) is not None:
                            raise ServiceError("middleware_error")
                    decision = await self._decision(invocation, spec)
                    if decision not in {"allow", "deny", "ask"}:
                        decision = "deny"
                    return Admission(
                        spec, parsed, decision,
                        "permission_denied" if decision == "deny" else None)
            finally:
                CURRENT.reset(token)

    async def execute(
        self, invocation: Invocation, context: ToolContext, admission: Admission | None = None
    ) -> ToolObservation:
        with _calling(invocation):
            fields = {
                key: value
                for key, value in asdict(invocation).items()
                if key in ToolObservation.model_fields
            }
            fields["arguments"] = parse_json(invocation.arguments_json)
            outcome = ToolObservation.model_validate(fields)
            clock = asyncio.get_running_loop().time
            deadline = clock() + context.timeout if context.scope is not None else None
            tool_budget: ExecutionBudget | None = None
            decision = "deny"
            entering = returned = available = cancelled = False
            value: JsonValue = None
            code: str | None = None
            first: asyncio.Task[JsonValue] | None = None
            repeated = False
            token = CURRENT.set(replace(context.scope,
                invocation=invocation) if context.scope is not None else None)

            def check() -> None:
                self.registry.assert_current()
                context.assert_target_current()
                if context.scope is not None:
                    context.scope.check()
                if first is not None and repeated:
                    raise ServiceError("wrap_reentry")

            try:
                if admission is None:
                    # 直接调用路径：入场判定内联完成（引擎路径先 admit 再 execute）。
                    admission = await self.admit(
                        invocation, context, timeout_seconds=context.timeout)
                spec = admission.spec
                parsed = admission.parsed
                decision = admission.decision
                if not admission.start_emitted and context.scope is None:
                    # legacy的tool_start仍在原工具timeout之外。
                    await context.emit("tool_start", outcome.model_dump(mode="json"))
                if context.scope is None:
                    deadline = clock() + context.timeout
                assert deadline is not None
                tool_budget = ExecutionBudget(deadline)
                tool_timeout = asyncio.timeout_at(deadline)
                tool_budget.bind(tool_timeout)
                async with tool_timeout:
                    if not admission.start_emitted and context.scope is not None:
                        await context.emit("tool_start", outcome.model_dump(mode="json"))
                    check()

                    async def security() -> JsonValue:
                        nonlocal entering, returned, available, value
                        check()
                        if decision == "deny":
                            raise ServiceError(admission.code or "permission_denied")
                        if decision == "ask" and not admission.approved:
                            assert tool_budget is not None
                            with tool_budget.pause():
                                approved = await context.approve(
                                    invocation, context.approval_timeout)
                            check()
                            if not approved:
                                raise ServiceError("approval_denied")
                        check()
                        await context.emit("tool_intent", {
                            "call_id": invocation.call_id, "span_id": invocation.span_id,
                            "step_index": invocation.step_index, "decision": decision,
                        })
                        check()
                        entering = True
                        if spec.external_executor is not None:
                            raw = await spec.external_executor(invocation, parsed)
                        elif spec.scoped_executor is None:
                            raw = await spec.executor(parsed)
                        elif context.scope is None:
                            entering = False
                            raise ServiceError("workspace_contract_required")
                        else:
                            raw = await spec.scoped_executor(invocation, parsed, context)
                        returned = True
                        try:
                            value = parse_json(encoded(raw))
                            contract = (MCPContract(spec.output_schema)
                                if spec.source is not None and spec.source.kind == "mcp"
                                else OutputContract(spec.output_schema))
                            contract.validate(value)
                            available = True
                        except (TypeError, ValueError, RecursionError):
                            raise ServiceError("invalid_output") from None
                        # 已发生结果先固定；fence失败不能抹除写入和已知结果。
                        check()
                        return parse_json(encoded(value))

                    def next_once() -> Awaitable[JsonValue]:
                        nonlocal first, repeated
                        if first is not None:
                            repeated = True
                            raise ServiceError("wrap_reentry")
                        # 同步置位先于首次await；所有者始终join这一个真实内核Task。
                        first = asyncio.create_task(security())
                        return first

                    try:
                        if self.wrap is None:
                            # 无wrap时保留原调用Task身份（包括legacy self_wait边界）。
                            await security()
                        else:
                            snapshot = replace(invocation)
                            before = asdict(snapshot)
                            try:
                                response = await self.wrap(snapshot, next_once)
                            except ServiceError:
                                raise
                            except Exception:
                                raise ServiceError("middleware_error") from None
                            check()
                            if first is None or not first.done() or asdict(snapshot) != before:
                                raise ServiceError("middleware_error")
                            actual = first.result()
                            if encoded(response) != encoded(actual):
                                raise ServiceError("middleware_error")
                    finally:
                        if first is not None:
                            if not first.done():
                                first.cancel()
                            await settled(asyncio.gather(first, return_exceptions=True))
                    check()
                    if self.post is not None:
                        original = outcome.model_copy(update={"status": "ok",
                            "executor_started": True,
                            "result_available": True, "value": value, "decision": decision})
                        snapshot_result = original.model_copy(deep=True)
                        snapshot_invocation = replace(invocation)
                        before_result = fingerprint(snapshot_result.model_dump(mode="json"))
                        try:
                            post_response = await cast(Callable[[Invocation, ToolObservation],
                                Awaitable[object]], self.post)(snapshot_invocation, snapshot_result)
                            if post_response is not None or fingerprint(
                                snapshot_result.model_dump(
                                mode="json")) != before_result or asdict(
                                snapshot_invocation) != asdict(invocation):
                                raise ValueError("Hook不得修改结果")
                        except Exception:
                            raise ServiceError("post_hook_error") from None
                        check()
            except asyncio.CancelledError:
                cancelled, code = True, "cancelled"
            except TimeoutError:
                code = "tool_timeout"
            except ServiceError as error:
                code = error.code
            except Exception:
                code = "tool_error"
            try:
                started: bool | None = True if returned else None if entering else False
                result_available: bool | None = (
                    True if available else None if entering and not returned else False
                )
                if context.scope is not None and entering and not returned:
                    observed = await settled(context.scope.operations.observe(invocation))
                    if observed is not None:
                        started = cast(bool | None, observed["executor_started"])
                        result_available = True if observed["output_complete"] is True else None
                        value = observed
                        available = True
                outcome = ToolObservation(call_id=invocation.call_id,
                    provider_call_id=invocation.provider_call_id,
                    arguments=cast(dict[str, JsonValue], parse_json(invocation.arguments_json)),
                    span_id=invocation.span_id, step_index=invocation.step_index,
                    name=invocation.name,
                    status="ok" if code is None else "cancelled" if cancelled else "denied"
                    if code in {"permission_denied", "approval_denied",
                    "approval_expired"} else "failed",
                    executor_started=started, result_available=result_available,
                    value=value if available else None, code=code, decision=cast("Decision",
                    decision))
                await settled(context.emit("tool_end", outcome.model_dump(mode="json")))
                if cancelled:
                    raise asyncio.CancelledError
                return outcome
            finally:
                CURRENT.reset(token)
