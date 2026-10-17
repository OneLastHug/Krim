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

from .core import Decision, Invocation, Policy, PostHook, PreHook
from .output import OutputContract
from .registry import ToolRegistry

type Emit = Callable[[str, dict[str, JsonValue]], Awaitable[RunEvent]]
type Approve = Callable[[Invocation, float], Awaitable[bool]]


@dataclass(frozen=True)
class ToolContext:
    emit: Emit
    approve: Approve
    timeout: float
    assert_target_current: Callable[[], None] = lambda: None


class ToolExecutor:
    def __init__(
        self,
        registry: ToolRegistry,
        policy: Policy | None,
        pre: PreHook | None = None,
        post: PostHook | None = None,
    ) -> None:
        self.registry, self.policy, self.pre, self.post = registry, policy, pre, post

    async def execute(self, invocation: Invocation, context: ToolContext) -> ToolObservation:
        fields = {
            key: value
            for key, value in asdict(invocation).items()
            if key in ToolObservation.model_fields
        }
        fields["arguments"] = parse_json(invocation.arguments_json)
        outcome = ToolObservation.model_validate(fields)
        await context.emit("tool_start", outcome.model_dump(mode="json"))
        decision = "deny"
        entering = False
        returned = False
        available = False
        value: JsonValue = None
        code: str | None = None
        cancelled = False
        try:
            async with asyncio.timeout(context.timeout):
                context.assert_target_current()
                spec = self.registry.get(invocation.name)
                if spec.fingerprint != invocation.tool_hash:
                    raise ServiceError("tool_binding_changed")
                try:
                    parsed = spec.parameters.model_validate(
                        parse_json(invocation.arguments_json), strict=True
                    )
                except (ValidationError, ValueError):
                    raise ServiceError("invalid_input") from None
                if self.pre is not None:
                    snapshot = replace(invocation)
                    before_invocation = asdict(snapshot)
                    try:
                        verdict = await self.pre(snapshot)
                        if asdict(snapshot) != before_invocation or (
                            verdict is not None and not isinstance(verdict, str)
                        ):
                            raise ValueError("Hook 不得修改或伪造结果")
                    except Exception:
                        raise ServiceError("pre_hook_error") from None
                    if verdict is not None:
                        raise ServiceError("hook_denied")
                decision = await self.policy(invocation) if self.policy is not None else "deny"
                if decision not in {"allow", "deny", "ask"}:
                    decision = "deny"
                if decision == "ask":
                    if not await context.approve(invocation, context.timeout):
                        raise ServiceError("approval_denied")
                elif decision == "deny":
                    raise ServiceError("permission_denied")
                self.registry.assert_current()
                context.assert_target_current()
                await context.emit(
                    "tool_intent",
                    {
                        "call_id": invocation.call_id,
                        "span_id": invocation.span_id,
                        "step_index": invocation.step_index,
                        "decision": decision,
                    },
                )
                self.registry.assert_current()
                context.assert_target_current()
                entering = True
                raw = await spec.executor(parsed)
                returned = True
                try:
                    value = parse_json(encoded(raw))
                    OutputContract(spec.output_schema).validate(value)
                    available = True
                except (TypeError, ValueError, RecursionError):
                    raise ServiceError("invalid_output") from None
                if self.post is not None:
                    original = outcome.model_copy(
                        update={
                            "status": "ok",
                            "executor_started": True,
                            "result_available": True,
                            "value": value,
                            "decision": decision,
                        }
                    )
                    snapshot_result = original.model_copy(deep=True)
                    snapshot_invocation = replace(invocation)
                    before_result = fingerprint(snapshot_result.model_dump(mode="json"))
                    try:
                        response = await cast(
                            Callable[[Invocation, ToolObservation], Awaitable[object]], self.post
                        )(snapshot_invocation, snapshot_result)
                        if (
                            response is not None
                            or fingerprint(snapshot_result.model_dump(mode="json")) != before_result
                            or asdict(snapshot_invocation) != asdict(invocation)
                        ):
                            raise ValueError("Hook 不得修改结果")
                    except Exception:
                        raise ServiceError("post_hook_error") from None
        except asyncio.CancelledError:
            cancelled, code = True, "cancelled"
        except TimeoutError:
            code = "tool_timeout"
        except ServiceError as error:
            code = error.code
        except Exception:
            code = "tool_error"
        outcome = ToolObservation(
            call_id=invocation.call_id,
            provider_call_id=invocation.provider_call_id,
            arguments=cast(dict[str, JsonValue], parse_json(invocation.arguments_json)),
            span_id=invocation.span_id,
            step_index=invocation.step_index,
            name=invocation.name,
            status="ok"
            if code is None
            else "cancelled"
            if cancelled
            else "denied"
            if code in {"permission_denied", "approval_denied", "approval_expired"}
            else "failed",
            executor_started=True if returned else None if entering else False,
            result_available=True if available else None if entering and not returned else False,
            value=value if available else None,
            code=code,
            decision=cast("Decision", decision),
        )
        await context.emit("tool_end", outcome.model_dump(mode="json"))
        if cancelled:
            raise asyncio.CancelledError
        return outcome
