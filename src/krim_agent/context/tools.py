"""The ``compact_context`` session tool: enqueue only, never compact inline.

Compaction runs at the model boundary (CompactorHistory with ``force=True`` on
a safe prepare), never inside a tool execution. Calling this tool therefore
only records a durable request via
:meth:`krim_agent.context.service.ContextService.schedule_compaction`; the
run's boundary adapter surfaces it on the next safe prepare and marks it
fulfilled after the pass succeeds. The request key is derived from the
invocation identity, so a retried tool call is idempotent while distinct calls
create distinct requests; the model cannot replay or collide keys by choosing
arguments.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, Field, JsonValue

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

if TYPE_CHECKING:
    from .service import ContextService


class CompactContextInput(StrictModel):
    """Optional free-form reason recorded with the durable request."""

    reason: str | None = Field(default=None, max_length=2048)


async def unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


def compact_context_bundle(service: ContextService, resolve: SessionScopeResolver) -> ToolBundle:
    async def scope_for(invocation: Invocation) -> SessionToolScope:
        scope = await resolve(invocation)
        scope.check()
        if scope.run_id != invocation.run_id:
            raise ServiceError("tool_binding_changed")
        return scope

    async def policy(invocation: Invocation) -> Decision:
        if invocation.name != "compact_context":
            return "deny"
        try:
            scope = await scope_for(invocation)
            await service.schedule_auth(
                scope.actor, scope.workspace_id, scope.session_id, check=scope.check
            )
        except ServiceError:
            return "deny"
        return "allow"

    async def execute(invocation: Invocation, parsed: BaseModel, context: object) -> JsonValue:
        scope = await scope_for(invocation)
        arguments = cast(CompactContextInput, parsed)
        request_key = f"compact-context.{invocation.run_id}.{invocation.call_id}"
        result = await service.schedule_compaction(
            scope.actor,
            scope.workspace_id,
            scope.session_id,
            request_key,
            reason=arguments.reason,
            check=scope.check,
        )
        scope.check()
        return cast(JsonValue, result)

    digest = hashlib.sha256()
    root = Path(__file__).parent
    for path in (
        root / "service.py",
        Path(__file__),
        root.parent / "storage" / "context_models.py",
    ):
        digest.update(path.read_bytes())
    implementation = digest.hexdigest()
    return ToolBundle(
        specs=(
            ToolSpec(
                name="compact_context",
                parameters=CompactContextInput,
                executor=unavailable,
                output_schema={"type": "object"},
                version="context/1",
                implementation_digest=implementation,
                scoped_executor=execute,
                capability="session_state",
                risk="write",
                description=(
                    "请求在安全的模型边界处压缩当前会话历史。只登记一次持久请求；"
                    "实际压缩由下一次运行的宿主历史边界执行，本调用不会立即改变上下文。"
                    "同一次调用的重试幂等；reason 仅供记录。"
                ),
            ),
        ),
        policy=policy,
    )


# Short alias matching the planning/interaction bundle entry points.
bundle = compact_context_bundle
