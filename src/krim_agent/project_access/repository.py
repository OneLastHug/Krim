"""D-owned persistence port; I supplies the durable, cross-process adapter."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Protocol

from krim_agent.development.contracts import AuthorizationRef
from krim_agent.project_access.git import GitGrant
from krim_agent.project_access.types import (
    AccessIntent,
    ApprovalBinding,
    NetworkGrant,
    ProjectGrant,
    SecretRecord,
)
from krim_agent.security import PrincipalContext


@dataclass(frozen=True, slots=True)
class AccessSnapshot:
    actor: PrincipalContext | None
    grant: ProjectGrant | None
    approval: ApprovalBinding | None
    secrets: tuple[SecretRecord, ...] = field(default=(), repr=False)
    network: NetworkGrant | None = None
    git: GitGrant | None = None


class AccessRepository(Protocol):
    """I owns the persistent adapter and cross-process ordering.

    guard loads CURRENT records under one consistent transaction/fence and
    serializes mutations affecting actor generation, memberships, project grant,
    approval, secret revision/revocation and egress/Git grants until context exit.
    It must fetch by full owner/project/actor scope, not bare secret/policy ids.
    A missing await alone does not provide this cross-process guarantee.

    Delivery is bounded synchronous acceptance into a trusted local queue/pipe;
    no uncontrolled network, blocking pipe wait or long-lived process is allowed
    inside guard. A full sink must fail promptly. Revocation committed before
    acceptance wins; after acceptance it affects subsequent deliveries and must
    trigger I's resource cleanup. Accepted queue entries carry their binding;
    the trusted consumer must recheck before any later credential release.

    The ref is NOT proof of caller identity. I binds it to the authenticated
    caller before invoking D. A snapshot/receipt is never a bearer capability.
    This port does not admit operations, consume approvals or authorize cleanup;
    those remain I's existing RunStore/OperationStore and management authority.
    """

    def guard(
        self,
        ref: AuthorizationRef,
        intent: AccessIntent,
    ) -> AbstractAsyncContextManager[AccessSnapshot]: ...
