"""E-owned persistence/auth ports; shared routing comes only from I's C0 module.

No implementation here creates a database, launches a process, publishes a port,
or resolves a caller-supplied URL. Repository methods must reject binding drift.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Literal, Protocol

from krim_agent.development.contracts import AuthorizationRef, PreviewHandle, ProcessHandle
from krim_agent.development.preview_ports import RouteLease, ServiceInstance
from krim_agent.security import PrincipalContext


@dataclass(frozen=True)
class PreviewRecord:
    handle: PreviewHandle
    process: ProcessHandle
    authorization: AuthorizationRef
    instance: ServiceInstance
    request_digest: str
    route_epoch: int = 1
    revision: int = 1


class PreviewRepository(Protocol):
    """I-owned rows; immutable target/StartedAt, monotonic CAS revision and epoch.

    create rejects reused preview_id and duplicate live service/port bindings.
    save requires updated.revision == expected.revision + 1. Normal readiness
    observations keep route_epoch; stop/revoke increment it. Retarget/restart
    requires a new preview ID; never refresh StartedAt on an old record. get and
    constructed values are not authorization. All adapter reads are authoritative.
    """

    async def create(self, record: PreviewRecord) -> PreviewRecord: ...
    async def get(self, preview_id: str) -> PreviewRecord: ...
    async def get_by_host_key(self, key: str) -> PreviewRecord:
        """Lookup SHA256(preview_id)[:40]; store unique key with the preview row."""
        ...

    async def save(self, expected: PreviewRecord, updated: PreviewRecord) -> PreviewRecord: ...


class PreviewAuthorizer(Protocol):
    async def check(
        self,
        record: PreviewRecord,
        principal: PrincipalContext | None,
        action: Literal["register", "open", "serve", "stop"],
    ) -> None:
        """Re-read creator authorization and current actor generation/grant.

        principal=None is the scoped host readiness check, never an anonymous
        browser grant. It still checks creation authorization/revocation. Errors
        use ServiceError; a previously issued ticket/session is not permission.
        B additionally consults I's nonrecursive PreviewRouteSource on each use.
        """
        ...


@dataclass(frozen=True)
class BrowserGrant:
    preview_id: str
    principal: PrincipalContext
    expires_at: float


class PreviewCredentials(Protocol):
    """I persists hashes only, with expiration and atomic one-use ticket consume.

    All lookups bind preview_id before returning a grant. Retired IDs are never
    recycled. A grant still requires current PreviewAuthorizer validation.
    """

    async def put_ticket(self, digest: str, grant: BrowserGrant) -> None: ...
    async def consume_ticket(self, digest: str, preview_id: str, now: float) -> BrowserGrant: ...
    async def put_session(self, digest: str, grant: BrowserGrant) -> None: ...
    async def get_session(self, digest: str, preview_id: str, now: float) -> BrowserGrant: ...


class UpstreamSocket(Protocol):
    @property
    def subprotocol(self) -> str | None: ...
    def messages(self) -> AsyncIterator[str | bytes]: ...
    async def send(self, message: str | bytes) -> None: ...


class SocketConnector(Protocol):
    def connect(
        self,
        lease: RouteLease,
        target: bytes,
        *,
        origin: str,
        headers: list[tuple[str, str]],
        subprotocols: list[str],
    ) -> AbstractAsyncContextManager[UpstreamSocket]: ...
