"""Project access resolution and revocation-aware, trusted delivery only."""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from typing import NoReturn

from krim_agent.account_mcp.contracts import SecretVault
from krim_agent.contracts import ServiceError, parse_json
from krim_agent.development.contracts import AuthorizationRef, CommandRequest
from krim_agent.development.ports import command_digest
from krim_agent.network.egress_policy import EgressViolation
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.git import (
    GitIdentity,
    GitOperation,
    check_git_operation,
    check_helper_request,
    credential_response,
)
from krim_agent.project_access.process_secrets import (
    MAX_COMMAND_BYTES,
    MAX_SECRET_BYTES,
    CommandSecretBinding,
    CommandSecretSink,
)
from krim_agent.project_access.repository import AccessRepository, AccessSnapshot
from krim_agent.project_access.secrets import (
    DeliveryBinding,
    SecretDelivery,
    SecretMaterial,
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
        delivery_sink: SecretSink | None = None,
        command_sink: CommandSecretSink | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._repository = repository
        self._vault = vault
        self._secret_sink = secret_sink
        self._git_sink = git_sink
        self._delivery_sink = delivery_sink
        self._command_sink = command_sink
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

    async def release_secret(
        self, binding: DeliveryBinding, pending: SecretDelivery
    ) -> ResolvedAccess:
        """Host-only delayed release, fenced with current scope and revision.

        The constructor-wired final sink must accept synchronously and without
        blocking; neither the queue nor receipt is a bearer capability.
        """
        sink = self._delivery_sink
        if sink is None:
            _deny()
        assert sink is not None
        final_sink: SecretSink = sink
        async with self._repository.guard(binding.authorization, binding.intent) as snapshot:
            receipt = self._validate(binding.authorization, binding.intent, snapshot)
            self._delivery_record(binding, snapshot)
            service = self

            class FinalSink:
                def deliver(self, actual: DeliveryBinding, material: SecretMaterial) -> None:
                    if actual != binding:
                        _deny()
                    # Queue decryption can advance wall time even while no
                    # other writer can change records under this guard.
                    service._validate(binding.authorization, binding.intent, snapshot)
                    service._delivery_record(binding, snapshot)
                    final_sink.deliver(actual, material)

            pending.release(binding, FinalSink())
            return receipt

    def _command_records(
        self, binding: CommandSecretBinding, snapshot: AccessSnapshot, *, starting: bool,
    ) -> tuple[SecretRecord, ...]:
        request = binding.request
        proof = snapshot.command
        if (proof is None or snapshot.grant is None
                or "secret.deliver" not in snapshot.grant.actions):
            _deny()
        assert proof is not None
        process = proof.process
        operation = proof.operation
        try:
            process.handle.require_binding(request)
        except ValueError:
            _deny()
        expected_environment = request.environment.model_dump(exclude={"state"})
        operation_live = operation.state == "accepted" if starting else (
            operation.state in {"accepted", "running"}
            or (operation.state == "completed"
                and operation.result_ref == operation.identity.call_id)
        )
        if (
            proof.run_status != "running" or proof.stop_requested
            or proof.ownership != request.ownership
            or proof.environment.state != "ready"
            or proof.environment.model_dump(exclude={"state"}) != expected_environment
            or process.request != request
            or proof.command_digest != command_digest(request)
            or process.request_digest != command_digest(request)
            or process.handle.process_id != binding.process_id
            or process.execution_id != binding.execution_id
            or process.handle.state not in ({"accepted"} if starting else {"accepted", "running"})
            or not operation_live
            or operation.identity.operation_id != request.operation_id
            or operation.identity.ownership != request.ownership
            or operation.identity.environment.model_dump(exclude={"state"}) != expected_environment
        ):
            _deny()
        records = {r.secret_id: r for r in snapshot.secrets}
        if any(
            key not in records or records[key].revision != revision
            or records[key].purpose != "project.delivery"
            for key, revision in binding.revisions
        ):
            _deny()
        assert snapshot.grant is not None
        expiries = [self._clock(), snapshot.grant.expires_at,
                    *(r.expires_at for r in snapshot.secrets)]
        if snapshot.approval is not None:
            expiries.append(snapshot.approval.expires_at)
        if snapshot.network is not None:
            expiries.append(snapshot.network.expires_at)
        if any(not math.isfinite(value) for value in expiries):
            _deny()
        return tuple(records[key] for key in request.authorization.secret_refs)

    async def validate_command_secrets(
        self, binding: CommandSecretBinding, *, starting: bool = False,
    ) -> ResolvedAccess:
        """Host watcher checks current run/operation/process and secret revisions."""
        request = binding.request
        intent = command_intent(request, workspace_id=binding.workspace_id)
        async with self._repository.guard(request.authorization, intent) as snapshot:
            receipt = self._validate(request.authorization, intent, snapshot)
            self._command_records(binding, snapshot, starting=starting)
            return receipt

    async def deliver_command_secrets(self, binding: CommandSecretBinding) -> ResolvedAccess:
        """All-or-nothing package accepted under CURRENT authorization fence.

        The sole constructor-wired sink retains ciphertext only. No callback,
        destination path or environment variable comes from model input.
        """
        sink = self._command_sink
        if sink is None:
            _deny()
        assert sink is not None
        request = binding.request
        intent = command_intent(request, workspace_id=binding.workspace_id)
        materials: list[SecretMaterial] = []
        failed = False
        async with self._repository.guard(request.authorization, intent) as snapshot:
            receipt = self._validate(request.authorization, intent, snapshot)
            records = self._command_records(binding, snapshot, starting=True)
            try:
                size = 0
                for record in records:
                    plaintext = self._decrypt(record)
                    try:
                        size += len(plaintext)
                        if not 1 <= len(plaintext) <= MAX_SECRET_BYTES or size > MAX_COMMAND_BYTES:
                            raise ServiceError("input_limit", 422)
                        materials.append(SecretMaterial(plaintext))
                    finally:
                        del plaintext
                # Decryption/encryption can advance wall time. Recheck at the
                # actual sink acceptance while mutation writers remain fenced.
                self._validate(request.authorization, intent, snapshot)
                self._command_records(binding, snapshot, starting=True)
                assert snapshot.grant is not None
                expiries = [snapshot.grant.expires_at, *(r.expires_at for r in records)]
                if snapshot.approval is not None:
                    expiries.append(snapshot.approval.expires_at)
                if snapshot.network is not None:
                    expiries.append(snapshot.network.expires_at)
                assert request.timeout_seconds is not None
                expiries.append(self._clock() + request.timeout_seconds)
                sink.deliver_command(binding, tuple(materials), expires_at=min(expiries))
                self._validate(request.authorization, intent, snapshot)
            except Exception:
                failed = True
            finally:
                for material in materials:
                    material.erase()
        if failed:
            raise ServiceError("execution_error", 500)
        return receipt

    async def release_command_secrets(self, binding: CommandSecretBinding) -> ResolvedAccess:
        """Revalidate again at one-shot transport spawn, not merely queue time."""
        sink = self._command_sink
        if sink is None:
            _deny()
        assert sink is not None
        request = binding.request
        intent = command_intent(request, workspace_id=binding.workspace_id)
        failed = False
        async with self._repository.guard(request.authorization, intent) as snapshot:
            receipt = self._validate(request.authorization, intent, snapshot)
            self._command_records(binding, snapshot, starting=True)
            def check() -> None:
                self._validate(request.authorization, intent, snapshot)
                self._command_records(binding, snapshot, starting=True)

            try:
                sink.release_command(binding, check)
            except Exception:
                failed = True
        if failed:
            raise ServiceError("execution_error", 500)
        return receipt

    def _delivery_record(self, binding: DeliveryBinding, snapshot: AccessSnapshot) -> None:
        records = snapshot.secrets
        if len(records) != 1 or (records[0].secret_id, records[0].revision) != (
            binding.secret_id,
            binding.revision,
        ):
            _deny()
        record = records[0]
        if binding.intent.action == "secret.deliver":
            if record.purpose != "project.delivery":
                _deny()
        elif binding.intent.action in ("git.clone", "git.push"):
            if (
                record.purpose != "git.credential"
                or snapshot.network is None
                or snapshot.git is None
                or snapshot.git.credential_secret_id != binding.secret_id
            ):
                _deny()
            operation = self._git_operation(binding.intent, snapshot)
            try:
                ProjectEgressPolicy(snapshot.network).validate_target(operation.repository)
            except EgressViolation:
                _deny()
        else:
            _deny()

    async def accept_git(
        self,
        ref: AuthorizationRef,
        intent: AccessIntent,
        accept: Callable[[GitIdentity], None],
        *,
        credential_binding: DeliveryBinding | None = None,
    ) -> ResolvedAccess:
        """Accept one exact native Git spawn under the authorization fence.

        The constructor-wired host executor derives argv from intent. This is
        not a model tool or a permission to execute arbitrary callbacks. The
        callback only spawns/registers a bounded process; waiting is outside.
        I must terminate accepted processes on later committed revocation.
        """
        if not intent.action.startswith("git."):
            _deny()
        async with self._repository.guard(ref, intent) as snapshot:
            receipt = self._validate(ref, intent, snapshot)
            assert snapshot.git is not None
            if (
                intent.action in ("git.clone", "git.push")
                and snapshot.git.credential_secret_id is not None
                and credential_binding is None
            ):
                _deny()
            if credential_binding is not None:
                if credential_binding.intent != intent or credential_binding.authorization != ref:
                    _deny()
                self._delivery_record(credential_binding, snapshot)
            assert snapshot.git is not None
            failed = False
            try:
                accept(snapshot.git.identity)
            except Exception:
                failed = True
            if failed:
                raise ServiceError("execution_error", 500)
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
            allowed = {
                ("invalid_input", 422),
                ("input_limit", 422),
                ("mcp_unavailable", 503),
                ("storage_unavailable", 503),
            }
            error = (
                (exc.code, exc.status)
                if (exc.code, exc.status) in allowed
                else ("execution_error", 500)
            )
        except Exception:
            error = ("execution_error", 500)
        raise ServiceError(*error)
