"""唯一工具责任链：校验、否决、许可、审批、意图、执行、输出、审计。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, replace
from typing import cast

from pydantic import JsonValue, ValidationError

from krim_agent.contracts import (
    RunEvent,
    ServiceError,
    ToolObservation,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.runtime.contracts import settled

from .core import Decision, Invocation, Policy, PostHook, PreHook
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

    async def execute(self, invocation: Invocation, context: ToolContext) -> ToolObservation:
        with _calling(invocation):
            fields = {
                key: value
                for key, value in asdict(invocation).items()
                if key in ToolObservation.model_fields
            }
            fields["arguments"] = parse_json(invocation.arguments_json)
            outcome = ToolObservation.model_validate(fields)
            clock = asyncio.get_running_loop().time
            deadline = min(clock() + context.timeout,
                context.scope.deadline) if context.scope else None
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
                check()
                return verdict

            try:
                if context.scope is None:
                    # legacy的tool_start仍在原工具timeout之外；workspace从入口开始计时。
                    await context.emit("tool_start", outcome.model_dump(mode="json"))
                    deadline = clock() + context.timeout
                async with asyncio.timeout_at(deadline):
                    if context.scope is not None:
                        await context.emit("tool_start", outcome.model_dump(mode="json"))
                    check()
                    spec = self.registry.get(invocation.name)
                    if spec.fingerprint != invocation.tool_hash:
                        raise ServiceError("tool_binding_changed")
                    try:
                        parsed = spec.parameters.model_validate(
                            parse_json(invocation.arguments_json), strict=True)
                    except (ValidationError, ValueError):
                        raise ServiceError("invalid_input") from None
                    if self.pre is not None and await inspected(self.pre) is not None:
                        raise ServiceError("hook_denied")
                    for middleware in self.middleware:
                        if await inspected(middleware, middleware=True) is not None:
                            raise ServiceError("middleware_error")

                    async def security() -> JsonValue:
                        nonlocal decision, entering, returned, available, value
                        check()
                        decision = await self.policy(
                            invocation) if self.policy is not None else "deny"
                        check()
                        if decision not in {"allow", "deny", "ask"}:
                            decision = "deny"
                        if decision == "ask":
                            remaining = max(0, cast(float,
                                deadline) - clock(
                                )) if context.scope is not None else context.timeout
                            approved = await context.approve(invocation, remaining)
                            check()
                            if not approved:
                                raise ServiceError("approval_denied")
                        elif decision == "deny":
                            raise ServiceError("permission_denied")
                        check()
                        await context.emit("tool_intent", {
                            "call_id": invocation.call_id, "span_id": invocation.span_id,
                            "step_index": invocation.step_index, "decision": decision,
                        })
                        check()
                        entering = True
                        if spec.scoped_executor is None:
                            raw = await spec.executor(parsed)
                        elif context.scope is None:
                            entering = False
                            raise ServiceError("workspace_contract_required")
                        else:
                            raw = await spec.scoped_executor(invocation, parsed, context)
                        returned = True
                        try:
                            value = parse_json(encoded(raw))
                            OutputContract(spec.output_schema).validate(value)
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
