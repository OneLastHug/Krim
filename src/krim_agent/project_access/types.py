"""D-owned authorization values. No persistence or shared DTO definitions."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator

from krim_agent.contracts import StrictModel, fingerprint, parse_json
from krim_agent.development.contracts import AuthorizationRef, Digest, Identifier

Action = Literal[
    "shell.execute",
    "secret.deliver",
    "network.connect",
    "git.clone",
    "git.diff",
    "git.commit",
    "git.push",
]


class AccessIntent(StrictModel):
    """Built by a trusted adapter from the actual operation, never a supplied hash."""

    owner_id: Identifier
    workspace_id: Identifier
    project_id: Identifier
    operation_id: Identifier
    action: Action
    arguments_json: str = Field(min_length=2, max_length=65536, repr=False, exclude=True)

    @field_validator("arguments_json")
    @classmethod
    def object_arguments(cls, value: str) -> str:
        if not isinstance(parse_json(value), dict):
            raise ValueError("operation arguments must be an object")
        return value

    def digest(self, ref: AuthorizationRef) -> str:
        return fingerprint(
            {
                "domain": "krim.project-access.v1",
                "owner_id": self.owner_id,
                "workspace_id": self.workspace_id,
                "project_id": self.project_id,
                "operation_id": self.operation_id,
                "action": self.action,
                "arguments": parse_json(self.arguments_json),
                "actor_id": ref.actor_id,
                "actor_generation": ref.actor_generation,
                "approval_id": ref.approval_id,
                "secret_refs": list(ref.secret_refs),
                "network_policy_ref": ref.network_policy_ref,
            }
        )


class ProjectGrant(StrictModel):
    owner_id: Identifier
    workspace_id: Identifier
    project_id: Identifier
    actor_id: Identifier
    actor_generation: int = Field(ge=1)
    revision: int = Field(ge=1)
    actions: tuple[Action, ...]
    requires_approval: bool = True
    expires_at: float
    revoked: bool = False


class ApprovalBinding(StrictModel):
    """Projection of the host approval, not an independent approval ledger."""

    approval_id: Identifier
    actor_id: Identifier
    actor_generation: int = Field(ge=1)
    owner_id: Identifier
    workspace_id: Identifier
    project_id: Identifier
    operation_id: Identifier
    action: Action
    arguments_digest: Digest
    expires_at: float
    approved: bool


class SecretRecord(StrictModel):
    owner_id: Identifier
    project_id: Identifier
    secret_id: Identifier
    revision: int = Field(ge=1, le=2**31 - 1)
    purpose: Literal["project.delivery", "git.credential"]
    ciphertext: str = Field(min_length=1, max_length=16384, repr=False, exclude=True)
    expires_at: float
    revoked: bool = False


class NetworkGrant(StrictModel):
    owner_id: Identifier
    project_id: Identifier
    policy_id: Identifier
    revision: int = Field(ge=1)
    hosts: tuple[str, ...] = Field(min_length=1, max_length=256)
    ports: tuple[int, ...] = Field(default=(443,), min_length=1, max_length=64)
    expires_at: float
    revoked: bool = False


class ResolvedAccess(StrictModel):
    """Informational receipt only; cannot be used to bypass the next resolve."""

    actor_id: Identifier
    actor_generation: int
    project_id: Identifier
    operation_id: Identifier
    arguments_digest: Digest
    grant_revision: int
    secret_revisions: tuple[tuple[str, int], ...]
    network_revision: int | None = None
