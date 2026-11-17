"""view_image registration for the existing session-scoped tool executor."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from pydantic import BaseModel, Field, JsonValue

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionScopeResolver, ToolBundle

from .references import MediaSource
from .service import MediaService


class ViewImageInput(MediaSource):
    frame: int | None = Field(default=None, ge=0, description="Optional zero-based animation frame")


async def unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


def bundle(service: MediaService, resolve: SessionScopeResolver) -> ToolBundle:
    async def policy(invocation: Invocation) -> Decision:
        if invocation.name != "view_image":
            return "deny"
        scope = await resolve(invocation)
        scope.check()
        scope.actor.member(scope.workspace_id)
        return "allow"

    async def execute(invocation: Invocation, parameters: BaseModel, _context: object) -> JsonValue:
        if invocation.name != "view_image" or not isinstance(parameters, ViewImageInput):
            raise ServiceError("invalid_input", 422)
        scope = await resolve(invocation)
        scope.check()
        return cast(JsonValue, await service.view(
            scope, MediaSource(path=parameters.path, reference=parameters.reference),
            frame=parameters.frame,
        ))

    digest = fingerprint({
        name: Path(__file__).with_name(name).read_text(encoding="utf-8")
        for name in ("tools.py", "references.py", "service.py", "formats.py", "delivery.py")
    })
    return ToolBundle(specs=(ToolSpec(
        name="view_image", parameters=ViewImageInput, executor=unavailable,
        output_schema={"type": "object"}, version="session/1", implementation_digest=digest,
        risk="read", scoped_executor=execute, capability="session_read",
        description=("View an authorized image from a project-relative path or an opaque artifact "
                     "reference (exactly one). Never provide an absolute host path or URL. "
                     "An optional frame selects a zero-based animation frame. "
                     "The original reference remains available after context compaction. "
                     "Only a subsequent host-attached image part makes the image visible; "
                     "unavailable or pending metadata is not visual evidence."),
    ),), policy=policy)
