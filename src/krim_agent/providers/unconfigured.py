"""Non-executable model boundary for management-only application startup."""

from collections.abc import AsyncIterator, Sequence

from pydantic import JsonValue

from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.types import ModelEvent


class UnconfiguredProvider:
    """Never produces model output; no run binding may reference this boundary."""

    def stream(
        self, messages: Sequence[Message], tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        raise ServiceError("provider_not_configured", 503)

    async def close(self) -> None:
        pass
