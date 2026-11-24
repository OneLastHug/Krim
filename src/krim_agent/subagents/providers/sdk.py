"""Continuable SDK sessions use pre-registered production RunService bindings."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue

from krim_agent.contracts import RunView, ServiceError, encoded, fingerprint, parse_json
from krim_agent.subagents.types import StartTask

from .base import ProviderCapabilities

if TYPE_CHECKING:
    from krim_agent.runtime.service import RunService


class SdkSessionProvider:
    """A route to an existing SDK loop, not another loop or model client.

    Composition creates one immutable binding per configured worker profile and
    shares the configured provider client. This object is retained by accepted
    activations so removing a registry entry does not change their backend.
    """

    def __init__(
        self,
        runtime: RunService | None,
        descriptor: dict[str, JsonValue],
        *,
        images: bool = False,
        structured_output: bool = False,
        workspace_isolation: bool = False,
    ) -> None:
        if not isinstance(descriptor.get("key"), str) or "context" in descriptor:
            raise ValueError("SDK provider requires a base target binding")
        self.runtime = runtime
        self._descriptor_json = encoded(descriptor)
        self.version = fingerprint(descriptor)
        self.capabilities = ProviderCapabilities(
            continuable=True,
            fork=True,
            images=images,
            structured_output=structured_output,
            workspace_isolation=workspace_isolation,
        )

    @property
    def descriptor(self) -> dict[str, JsonValue]:
        # Do not publish a mutable reference to the provider's frozen binding.
        return cast(dict[str, JsonValue], parse_json(self._descriptor_json))

    async def start(self, specification: StartTask) -> RunView:
        if encoded(specification.target) != self._descriptor_json:
            raise ServiceError("target_binding_changed")
        if self.runtime is None:
            raise ServiceError("closed", 503)
        return await self.runtime.start_delegate(specification)

    async def interrupt(self, run_id: str) -> Any:
        # The domain service verifies the tree, target task and activation before
        # using this host-only primitive. It must not call external cancel() from
        # inside a tool (that path intentionally rejects self-waiting callers).
        if self.runtime is None:
            raise ServiceError("closed", 503)
        return await self.runtime.cancel_delegate(run_id)

    async def dispose(self, activation_id: str) -> None:
        # SDK activations hold no separate client or process. Runs are cancelled
        # and drained by SubagentService before this method is reached; the
        # shared HTTP client's lifetime belongs to Application.close().
        if not activation_id:
            raise ValueError("missing activation identity")
