"""Host-only preview routing shared by B/E and assembled by I.

The public C0 PreviewHandle stays unchanged. These ports never accept a network
address or executable from a model/browser. Valid values alone grant no access:
B calls I's source on registration, acquisition, every connection and renewal.
"""

from __future__ import annotations

import socket
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Literal, Protocol, Self

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel
from krim_agent.development.contracts import Digest, Identifier, ProcessHandle
from krim_agent.development.ports import ProcessRecord
from krim_agent.security import PrincipalContext

PREVIEW_ROUTE_VERSION = "preview-route-v1"


class PreviewRouteError(Exception):
    """Fixed transport failure; never include daemon stderr or private paths."""

    def __init__(
        self,
        code: Literal[
            "preview_route_unavailable", "preview_route_invalidated",
            "preview_route_capacity", "preview_route_denied",
        ],
    ) -> None:
        self.code = code
        super().__init__(code)


class ServiceInstance(StrictModel):
    """B's observation of an already-running execution; no start/replay permission.

    started_at is the exact Docker State.StartedAt token, not a rounded datetime.
    I/E persist it at first preview registration and never silently refresh it.
    """

    execution_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    started_at: str = Field(
        min_length=20,
        max_length=40,
        pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$",
    )


class RouteBinding(ServiceInstance):
    """Immutable registered target; normal observed-state revisions aren't epochs."""

    preview_id: Identifier
    owner_id: Identifier
    project_id: Identifier
    environment_id: Identifier
    generation: int = Field(ge=1)
    service_id: Identifier
    process_id: Identifier
    operation_id: Identifier
    request_digest: Digest
    port: int = Field(ge=1, le=65535)
    route_epoch: int = Field(ge=1)

    @model_validator(mode="after")
    def same_service(self) -> Self:
        if self.service_id != self.process_id:
            raise ValueError("preview service_id must be the persistent process_id")
        return self


@dataclass(frozen=True)
class RouteLease:
    """Opaque B-owned resource, tied to the acquiring viewer and exact binding.

    A caller cannot mint or move a lease by constructing this value. B checks its
    own live lease table and I's persisted binding. Expiry uses a monotonic clock
    independently of blocked control calls, socket writes or E's polling.
    """

    binding: RouteBinding
    lease_id: str


class PreviewRouteSource(Protocol):
    """I re-reads durable records and authenticates the original viewer.

    Both methods return the current canonical ProcessRecord, never caller data.
    They use repositories and authorization only, never recursively call B or
    PreviewRoutes, and hold no database transaction during B's runtime/network IO.
    B independently checks its command digest, project_service/running state,
    ready environment and actual container configuration/instance.
    """

    async def registration(
        self, process: ProcessHandle, port: int, principal: PrincipalContext
    ) -> ProcessRecord:
        """Authorize registration for this exact service/port before observing it."""
        ...

    async def resolve(
        self, binding: RouteBinding, principal: PrincipalContext | None
    ) -> ProcessRecord:
        """Require the stored binding/epoch, active preview and current access.

        principal=None only permits a scoped host readiness check; it never
        grants browser access or renews a lease created for another viewer.
        The creator's current authority must also remain valid. Completion of
        the originating run alone does not revoke a project_service.
        """
        ...


class PreviewRoutes(Protocol):
    async def observe(
        self, process: ProcessHandle, port: int, principal: PrincipalContext
    ) -> ServiceInstance:
        """Authorize and inspect an existing service without connecting/starting.

        Registration captures the instance here; later acquisition must reject
        a restart between this observation and the persistence of its binding.
        """
        ...

    def acquire(
        self, binding: RouteBinding, principal: PrincipalContext | None
    ) -> AbstractAsyncContextManager[RouteLease]:
        """Revalidate and reserve a bounded lease; release it and all streams on exit.

        Retain the original viewer, including actor generation, inside B. Never
        hold a project lock across the caller's context body. No business bytes
        or health GET are sent during acquisition. Missing routes fail closed.
        """
        ...

    def connect(self, lease: RouteLease) -> AbstractAsyncContextManager[socket.socket]:
        """Yield one connected nonblocking SOCK_STREAM endpoint, not an address.

        B revalidates both before relay startup and before yielding. In between,
        only a TCP connect to the registered container's IPv4 loopback may occur;
        E cannot send business bytes until the second check completes. No TTY,
        arbitrary argv, network publication, transparent retry or reconnection.

        The socket can be an AF_UNIX socketpair endpoint. E may transfer socket
        ownership to its HTTP/WS transport, which must close before this context
        exits. B owns the peer and relay, shuts the peer on expiry/revocation and
        reaps relay resources on exit even if E already closed its endpoint.
        Half-close is preserved. Closing a preview never stops the service.
        """
        ...

    async def check(self, lease: RouteLease) -> None:
        """Revalidate the original viewer and binding; renew only a still-live lease.

        Expired/revoked/closed leases never revive. State.StartedAt, execution,
        generation, port or route_epoch drift invalidates all of its streams.
        Independent expiry still closes streams if this check blocks or fails.
        """
        ...

    async def ready(self, lease: RouteLease) -> bool:
        """Bounded connect/close only; false for not-listening, never a health GET.

        Authorization/binding/control-plane errors propagate, not become ready.
        Every probe releases its relay and connection capacity.
        """
        ...

    async def aclose(self) -> None:
        """Close this adapter's leases/relays without stopping project services."""
        ...
