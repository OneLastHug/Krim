"""Project access resolution and revocation-aware, trusted delivery only."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import NoReturn

from krim_agent.account_mcp.contracts import SecretVault
from krim_agent.contracts import ServiceError, parse_json
from krim_agent.development.contracts import AuthorizationRef, CommandRequest
from krim_agent.network.egress_policy import EgressViolation
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.git import (
    GitIdentity,
    GitOperation,
    check_git_operation,
    check_helper_request,
    credential_response,
)
from krim_agent.project_access.repository import AccessRepository, AccessSnapshot
from krim_agent.project_access.secrets import (
    DeliveryBinding,
    SecretSink,
    handoff,
    project_secret_context,
)
from krim_agent.project_access.types import AccessIntent, ResolvedAccess, SecretRecord


def _deny() -> NoReturn:
    raise ServiceError("permission_denied", 403)


def command_intent(request: CommandRequest, *, workspace_id: str) -> AccessIntent:
    """Host adapter computes the digest from the exact C0 command it submits."""
    from krim_agent.contracts import encoded

    payload = request.model_dump(mode="json", exclude={"authorization"})
    # Environment state is an observation, not stable operation identity.
    payload["environment"].pop("state")
    return AccessIntent(
        owner_id=request.environment.owner_id,
        workspace_id=workspace_id,
        project_id=request.environment.project_id,
        operation_id=request.operation_id,
        action="shell.execute",
        arguments_json=encoded(payload),
    )


class ProjectAccessService:
    def __init__(
        self,
        repository: AccessRepository,
        vault: SecretVault,
        *,
        secret_sink: SecretSink,
        git_sink: SecretSink,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._repository = repository
        self._vault = vault
        self._secret_sink = secret_sink
        self._git_sink = git_sink
        self._clock = clock

    def _validate(
        self,
        ref: AuthorizationRef,
        intent: AccessIntent,
        snapshot: AccessSnapshot,
    ) -> ResolvedAccess:
        now = self._clock()
        actor, grant, approval = snapshot.actor, snapshot.grant, snapshot.approval
        if (
            actor is None
            or grant is None
            or grant.revoked
            or grant.expires_at <= now
            or (actor.principal_id, actor.generation) != (ref.actor_id, ref.actor_generation)
            or (grant.actor_id, grant.actor_generation) != (ref.actor_id, ref.actor_generation)
            or (grant.owner_id, grant.workspace_id, grant.project_id)
            != (intent.owner_id, intent.workspace_id, intent.project_id)
            or ref.project_id != intent.project_id
            or intent.action not in grant.actions
            or ref.arguments_digest != intent.digest(ref)
            or len(set(ref.secret_refs)) != len(ref.secret_refs)
        ):
            _deny()
        assert actor is not None and grant is not None
        actor.owns(intent.workspace_id, intent.owner_id, write=True)
        if grant.requires_approval and ref.approval_id is None:
            _deny()
        if ref.approval_id is not None:
            if (
                approval is None
                or not approval.approved
                or approval.expires_at <= now
                or (
                    approval.approval_id,
                    approval.actor_id,
                    approval.actor_generation,
                    approval.owner_id,
                    approval.workspace_id,
                    approval.project_id,
                    approval.operation_id,
                    approval.action,
                    approval.arguments_digest,
                )
                != (
                    ref.approval_id,
                    ref.actor_id,
                    ref.actor_generation,
                    intent.owner_id,
                    intent.workspace_id,
                    intent.project_id,
                    intent.operation_id,
                    intent.action,
                    ref.arguments_digest,
                )
            ):
                _deny()
        records = snapshot.secrets
        if {r.secret_id for r in records} != set(ref.secret_refs) or len(records) != len(
            ref.secret_refs
        ):
            _deny()
        for record in records:
            if (
                record.revoked
                or record.expires_at <= now
                or (record.owner_id, record.project_id) != (intent.owner_id, intent.project_id)
            ):
                _deny()
        network = snapshot.network
        if ref.network_policy_ref is not None:
            if (
                network is None
                or network.revoked
                or network.expires_at <= now
                or (network.owner_id, network.project_id, network.policy_id)
                != (intent.owner_id, intent.project_id, ref.network_policy_ref)
            ):
                _deny()
        elif network is not None:
            _deny()
        if intent.action.startswith("git."):
            self._git_operation(intent, snapshot)
        return ResolvedAccess(
            actor_id=ref.actor_id,
            actor_generation=ref.actor_generation,
            project_id=intent.project_id,
            operation_id=intent.operation_id,
            arguments_digest=ref.arguments_digest,
            grant_revision=grant.revision,
            secret_revisions=tuple((r.secret_id, r.revision) for r in records),
            network_revision=network.revision if network else None,
        )

    async def resolve(self, ref: AuthorizationRef, intent: AccessIntent) -> ResolvedAccess:
        async with self._repository.guard(ref, intent) as snapshot:
            return self._validate(ref, intent, snapshot)

    async def deliver_secret(self, ref: AuthorizationRef, intent: AccessIntent) -> ResolvedAccess:
        if intent.action != "secret.deliver" or len(ref.secret_refs) != 1:
            _deny()
        async with self._repository.guard(ref, intent) as snapshot:
            receipt = self._validate(ref, intent, snapshot)
            record = snapshot.secrets[0]
            if record.purpose != "project.delivery":
                _deny()
            plaintext = self._decrypt(record)
            # Decryption can consume wall-clock time; expiry must still hold at
            # the acceptance boundary. No await occurs between recheck and sink.
            try:
                self._validate(ref, intent, snapshot)
                handoff(
                    self._secret_sink,
                    DeliveryBinding(intent, ref, record.secret_id, record.revision),
                    plaintext,
                )
            finally:
                del plaintext
            return receipt

    async def network_policy(
        self,
        ref: AuthorizationRef,
        intent: AccessIntent,
    ) -> ProjectEgressPolicy:
        async with self._repository.guard(ref, intent) as snapshot:
            self._validate(ref, intent, snapshot)
            if snapshot.network is None:
                _deny()
            assert snapshot.network is not None
            return ProjectEgressPolicy(snapshot.network)

    async def deliver_git_credential(
        self,
        ref: AuthorizationRef,
        intent: AccessIntent,
        helper_request: bytes,
        *,
        helper_action: str,
    ) -> ResolvedAccess:
        if (
            helper_action != "get"
            or intent.action not in ("git.clone", "git.push")
            or len(ref.secret_refs) != 1
        ):
            _deny()
        async with self._repository.guard(ref, intent) as snapshot:
            receipt = self._validate(ref, intent, snapshot)
            operation = self._git_operation(intent, snapshot)
            grant = snapshot.git
            assert grant is not None
            if grant.credential_secret_id != ref.secret_refs[0]:
                _deny()
            record = snapshot.secrets[0]
            if record.purpose != "git.credential" or snapshot.network is None:
                _deny()
            check_helper_request(
                helper_request, operation.repository, username=grant.credential_username
            )
            # This validates policy membership only. B owns DNS/socket pinning.
            try:
                ProjectEgressPolicy(snapshot.network).validate_target(operation.repository)
            except EgressViolation:
                _deny()
            plaintext = self._decrypt(record)
            try:
                response = credential_response(grant.credential_username, plaintext)
            finally:
                del plaintext
            try:
                self._validate(ref, intent, snapshot)
                self._git_operation(intent, snapshot)
                handoff(
                    self._git_sink,
                    DeliveryBinding(intent, ref, record.secret_id, record.revision),
                    response,
                )
            finally:
                del response
            return receipt

    async def git_identity(self, ref: AuthorizationRef, intent: AccessIntent) -> GitIdentity:
        async with self._repository.guard(ref, intent) as snapshot:
            self._validate(ref, intent, snapshot)
            self._git_operation(intent, snapshot)
            assert snapshot.git is not None
            return snapshot.git.identity

    def _git_operation(self, intent: AccessIntent, snapshot: AccessSnapshot) -> GitOperation:
        grant = snapshot.git
        if (
            grant is None
            or grant.revoked
            or grant.expires_at <= self._clock()
            or (grant.owner_id, grant.project_id) != (intent.owner_id, intent.project_id)
        ):
            _deny()
        assert grant is not None
        try:
            arguments = parse_json(intent.arguments_json)
            assert isinstance(arguments, dict)
            operation = GitOperation.model_validate(arguments["git"])
        except (KeyError, ValueError, TypeError):
            raise ServiceError("invalid_input", 422) from None
        if operation.action != intent.action:
            _deny()
        check_git_operation(operation, grant)
        return operation

    def _decrypt(self, record: SecretRecord) -> bytes:
        # Vault is trusted but still sanitize exceptions from injected adapters.
        error: tuple[str, int] | None = None
        try:
            return self._vault.decrypt(project_secret_context(record), record.ciphertext)
        except ServiceError as exc:
            error = (exc.code, exc.status)
        except Exception:
            error = ("execution_error", 500)
        raise ServiceError(*error)
