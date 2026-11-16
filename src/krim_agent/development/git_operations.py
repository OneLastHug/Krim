"""Host-only Git facade over the existing SDK operation ledger and D adapters.

The SDK owns admission and final tool receipts. This facade consumes a fresh
accepted row once, resolves canonical run/session/project/environment ownership,
and never exposes host paths, credentials, argv or transport selection to tools.
I wires GitCredentialRouter into the fixed ProjectAccessService git_sink and
calls management cleanup at committed revocation on every owning worker.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from pydantic import Field, field_validator
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError, StrictModel, encoded, fingerprint, parse_json
from krim_agent.development.contracts import (
    AuthorizationRef,
    EnvironmentHandle,
    OperationIdentity,
    OperationReceipt,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord
from krim_agent.project_access.git import (
    GitGrant,
    GitIdentity,
    GitOperation,
    canonical_repo,
    exact_ref,
)
from krim_agent.project_access.git_credentials import NativeGitCredentialCache, helper_request
from krim_agent.project_access.git_execution import (
    GitExecutionRequest,
    GitExecutionResult,
    TrustedGitRunner,
)
from krim_agent.project_access.git_transport import (
    DockerGitTransport,
    GitNetworkAuthorization,
    GitTransportBinding,
)
from krim_agent.project_access.git_workspace import GitWorkspaceBridge, WorkspaceBinding
from krim_agent.project_access.secrets import DeliveryBinding, SecretMaterial, SecretSink
from krim_agent.project_access.service import ProjectAccessService
from krim_agent.project_access.types import AccessIntent, ResolvedAccess
from krim_agent.sandbox.bindings import read_binding
from krim_agent.sandbox.service import SandboxService
from krim_agent.security import PrincipalContext
from krim_agent.storage.development import DevelopmentStore, environment_binding
from krim_agent.storage.development_models import development_operations, project_access_records
from krim_agent.storage.sqlite import SQLiteStore


class GitTreeRequest(StrictModel):
    """Snapshot the current trusted workspace; no path or executable input."""

    repository: str
    ref: str
    _repo = field_validator("repository")(canonical_repo)
    _ref = field_validator("ref")(exact_ref)


class GitTreeResult(StrictModel):
    tree_id: str = Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
    file_count: int = Field(ge=0)
    byte_count: int = Field(ge=0)


GitRequest = GitExecutionRequest | GitTreeRequest


def _payload(request: GitRequest) -> dict[str, Any]:
    if type(request) not in (GitExecutionRequest, GitTreeRequest):
        raise ServiceError("invalid_input", 422)
    value = request.model_dump(mode="json")
    # Revalidate even host model_copy inputs; the normal tool parse is not a
    # license to bypass request-shape validators at a later boundary.
    try:
        type(request).model_validate(value)
    except ValueError:
        raise ServiceError("invalid_input", 422) from None
    return value


def _operation(request: GitRequest) -> GitOperation:
    if isinstance(request, GitTreeRequest):
        return GitOperation(repository=request.repository, ref=request.ref, action="git.commit")
    return request.git


def _tool_name(request: GitRequest) -> str:
    return (
        "dev_git_tree"
        if isinstance(request, GitTreeRequest)
        else "dev_" + request.git.action.replace(".", "_")
    )


@dataclass(frozen=True)
class GitScope:
    actor: PrincipalContext
    workspace_id: str
    environment: EnvironmentHandle
    ownership: RunOwnership
    record: EnvironmentRecord = field(repr=False)
    secret_refs: tuple[str, ...]
    network_policy_ref: str | None
    request_fingerprint: str
    grant_fingerprint: str
    session_binding_revision: int


@dataclass(frozen=True)
class DerivedGitIntent:
    intent: AccessIntent
    authorization: AuthorizationRef
    approval_projection: dict[str, Any]


def derive_intent(
    request: GitRequest,
    scope: GitScope,
    operation_identity: OperationIdentity,
    approval_id: str | None = None,
) -> DerivedGitIntent:
    """Pure host approval projection; this value never authenticates a caller."""
    values = _payload(request)
    if (
        environment_binding(scope.environment)
        != environment_binding(operation_identity.environment)
        or scope.ownership != operation_identity.ownership
        or fingerprint(values) != scope.request_fingerprint
    ):
        raise ServiceError("permission_denied", 403)
    operation = _operation(request)
    arguments = (
        values
        if isinstance(request, GitExecutionRequest)
        else {"git": operation.model_dump(mode="json"), "workspace_tree": values}
    )
    intent = AccessIntent(
        owner_id=scope.ownership.owner_id,
        workspace_id=scope.workspace_id,
        project_id=scope.environment.project_id,
        operation_id=operation_identity.operation_id,
        action=operation.action,
        arguments_json=encoded(arguments),
    )
    ref = AuthorizationRef(
        actor_id=scope.actor.principal_id,
        actor_generation=scope.actor.generation,
        project_id=scope.environment.project_id,
        approval_id=approval_id,
        arguments_digest="0" * 64,
        secret_refs=scope.secret_refs,
        network_policy_ref=scope.network_policy_ref,
    )
    ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
    projection = {
        "actor_id": ref.actor_id,
        "actor_generation": ref.actor_generation,
        "owner_id": intent.owner_id,
        "workspace_id": intent.workspace_id,
        "project_id": intent.project_id,
        "operation_id": intent.operation_id,
        "action": intent.action,
        "arguments_digest": ref.arguments_digest,
    }
    return DerivedGitIntent(intent, ref, projection)


class GitCredentialRouter:
    """Constructor-wired sink; a task-local exact binding selects its host cache."""

    def __init__(self) -> None:
        self._target: ContextVar[tuple[DeliveryBinding, SecretSink] | None] = ContextVar(
            "git_operation_credential_target", default=None
        )

    @contextmanager
    def route(self, binding: DeliveryBinding, sink: SecretSink) -> Iterator[None]:
        if self._target.get() is not None:
            raise ServiceError("permission_denied", 403)
        token = self._target.set((binding, sink))
        try:
            yield
        finally:
            self._target.reset(token)

    def deliver(self, binding: DeliveryBinding, material: SecretMaterial) -> None:
        target = self._target.get()
        if target is None or binding != target[0]:
            raise ServiceError("permission_denied", 403)
        target[1].deliver(binding, material)


@dataclass
class _Active:
    scope: GitScope
    identity: OperationIdentity
    request: GitRequest
    derived: DerivedGitIntent
    task: asyncio.Task[Any]
    executing: bool = True
    authorization_receipt: ResolvedAccess | None = None
    lease: DockerGitTransport | None = None
    cache: NativeGitCredentialCache | None = None


class _ScopedAccess:
    def __init__(self, facade: GitOperations, active: _Active) -> None:
        self.facade, self.active = facade, active

    async def accept_git(
        self,
        ref: AuthorizationRef,
        intent: AccessIntent,
        accept: Callable[[GitIdentity], None],
        *,
        credential_binding: DeliveryBinding | None = None,
    ) -> ResolvedAccess:
        active = self.active
        if ref != active.derived.authorization or intent != active.derived.intent:
            raise ServiceError("permission_denied", 403)
        await self.facade._check(active)

        def bounded(identity: GitIdentity) -> None:
            self.facade._not_closed(active.scope.ownership)
            self.facade.identities.validate(active.scope.actor)
            accept(identity)

        return await self.facade.access.accept_git(
            ref, intent, bounded, credential_binding=credential_binding
        )


class GitOperations:
    """Concrete facade. Roots and dependencies are authenticated host assembly only.

    resolve_network is a constructor-fixed host resolver for a current
    repository/operation policy mapping. It is called only for clone/push,
    after canonical scope and SDK request binding checks, on every recheck.
    A returned policy id is still resolved by ProjectAccessService; it is not
    a capability. EnvironmentRecord network configuration is not consulted.

    Scope/derive_intent may run before SDK admission to project an approval.
    Execution requires both the persisted SDK checkpoint and an accepted row;
    the existing row is atomically consumed as running. A lost outcome never
    starts a second process. SDK writes final receipt/tool_end after return.
    """

    def __init__(
        self,
        store: SQLiteStore,
        repositories: DevelopmentStore,
        identities: IdentityService,
        sandbox: SandboxService,
        access: ProjectAccessService,
        *,
        credential_sink: GitCredentialRouter,
        repository_root: Path,
        stage_root: Path,
        cache_root: Path,
        image_id: str,
        resolve_network: Callable[
            [PrincipalContext, OperationIdentity, GitExecutionRequest], Awaitable[str | None]
        ]
        | None = None,
    ) -> None:
        if repositories.store is not store or sandbox.store is not store:
            raise ServiceError("invalid_input", 422)
        if not image_id.startswith("sha256:") or len(image_id) != 71:
            raise ServiceError("invalid_input", 422)
        for path in (repository_root, stage_root, cache_root):
            if not path.is_absolute() or path.resolve() != path:
                raise ServiceError("permission_denied", 403)
            TrustedGitRunner._secure_dir(path, private=True)
        if len({repository_root, stage_root, cache_root}) != 3:
            raise ServiceError("invalid_input", 422)
        self.store, self.repositories, self.identities, self.sandbox = (
            store,
            repositories,
            identities,
            sandbox,
        )
        self.access, self.credential_sink = access, credential_sink
        self.repository_root, self.stage_root, self.cache_root = (
            repository_root,
            stage_root,
            cache_root,
        )
        self.image_id = image_id
        self._resolve_network = resolve_network
        self.runner = TrustedGitRunner(repository_root)
        self.workspace = GitWorkspaceBridge()
        self._active: dict[str, _Active] = {}
        self._closed = False
        self._revoked_owners: set[str] = set()
        self._closed_runs: set[tuple[str, str, str]] = set()

    @staticmethod
    def _run_key(ownership: RunOwnership) -> tuple[str, str, str]:
        return ownership.owner_id, ownership.session_id, ownership.run_id

    def _not_closed(self, ownership: RunOwnership) -> None:
        if (
            self._closed
            or ownership.owner_id in self._revoked_owners
            or self._run_key(ownership) in self._closed_runs
        ):
            raise ServiceError("cancelled", 409)

    async def _invocation(self, identity: OperationIdentity, request: GitRequest) -> None:
        saved = await self.store.read_checkpoint(identity.ownership.run_id)
        if saved is None:
            raise ServiceError("facts_corrupt", 409)
        checkpoint, stored_fingerprint = saved
        if stored_fingerprint != fingerprint(checkpoint):
            raise ServiceError("facts_corrupt", 409)
        entries = checkpoint.get("entries")
        if not isinstance(entries, dict):
            raise ServiceError("facts_corrupt", 409)
        found = [
            item
            for item in entries.values()
            if isinstance(item, dict)
            and isinstance(item.get("invocation"), dict)
            and item["invocation"].get("call_id") == identity.call_id
        ]
        if len(found) != 1:
            raise ServiceError("permission_denied", 403)
        item = found[0]
        invocation = item["invocation"]
        try:
            raw = parse_json(invocation["arguments_json"])
            canonical = type(request).model_validate(raw)
            persisted = OperationIdentity.model_validate(item["identity"])
        except (KeyError, TypeError, ValueError):
            raise ServiceError("facts_corrupt", 409) from None
        if (
            persisted != identity
            or canonical != request
            or invocation.get("run_id") != identity.ownership.run_id
            or invocation.get("name") != _tool_name(request)
            or not isinstance(invocation.get("tool_hash"), str)
            or item.get("decision") not in {"allow", "ask"}
            or identity.request_digest
            != fingerprint(
                {"name": invocation["name"], "args": raw, "tool_hash": invocation["tool_hash"]}
            )
        ):
            raise ServiceError("permission_denied", 403)

    async def scope(
        self, actor: PrincipalContext, identity: OperationIdentity, request: GitRequest
    ) -> GitScope:
        values = _payload(request)
        self._not_closed(identity.ownership)
        self.identities.validate(actor)
        if actor.principal_id != identity.ownership.owner_id:
            raise ServiceError("permission_denied", 403)
        snapshot = await self.store.snapshot(identity.ownership.run_id)
        context = snapshot.session_context
        if (
            context is None
            or context.session_id != identity.ownership.session_id
            or snapshot.run.status != "running"
            or snapshot.run.stop_requested
        ):
            raise ServiceError("cancelled", 409)
        binding = await read_binding(self.sandbox, actor, context.session_id)
        if (
            binding["project_id"] != identity.environment.project_id
            or binding["workspace_id"] != context.workspace_id
        ):
            raise ServiceError("permission_denied", 403)
        project = await self.sandbox.project(actor, identity.environment.project_id, write=True)
        if (
            project["principal_id"] != identity.ownership.owner_id
            or project["workspace_id"] != context.workspace_id
            or project["status"] != "ready"
        ):
            raise ServiceError("permission_denied", 403)
        actor.owns(context.workspace_id, identity.ownership.owner_id, write=True)
        operation = _operation(request)
        async with self.store.lock, self.store.engine.begin() as conn:
            await self.repositories.ownership(conn, identity.environment, identity.ownership)
            record = await self.repositories.current_environment(conn, identity.environment)
            row = (
                (
                    await conn.execute(
                        select(
                            project_access_records.c.document, project_access_records.c.revision
                        ).where(
                            project_access_records.c.owner_id == actor.principal_id,
                            project_access_records.c.project_id == identity.environment.project_id,
                            project_access_records.c.kind == "git",
                            project_access_records.c.resource_id
                            == fingerprint(operation.repository),
                        )
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            raise ServiceError("permission_denied", 403)
        try:
            grant = GitGrant.model_validate_json(encoded(row["document"]))
        except ValueError:
            raise ServiceError("facts_corrupt", 409) from None
        if (
            grant.owner_id != actor.principal_id
            or grant.project_id != identity.environment.project_id
            or grant.repository != operation.repository
            or grant.revoked
            or grant.expires_at <= time.time()
            or operation.action not in grant.actions
            or operation.ref not in grant.refs
            or record.environment.state != "ready"
            or record.image_id != self.image_id
        ):
            raise ServiceError("permission_denied", 403)
        remote = operation.action in ("git.clone", "git.push")
        await self._invocation(identity, request)
        network_policy_ref = None
        if remote:
            resolver = self._resolve_network
            if resolver is None or not isinstance(request, GitExecutionRequest):
                raise ServiceError("permission_denied", 403)
            failed = False
            try:
                network_policy_ref = await asyncio.wait_for(resolver(actor, identity, request), 5)
            except Exception:
                failed = True
            if failed or type(network_policy_ref) is not str or not network_policy_ref:
                raise ServiceError("permission_denied", 403)
            self._not_closed(identity.ownership)
            self.identities.validate(actor)
        return GitScope(
            actor,
            context.workspace_id,
            record.environment,
            identity.ownership,
            record,
            (grant.credential_secret_id,) if remote and grant.credential_secret_id else (),
            network_policy_ref,
            fingerprint(values),
            fingerprint({"revision": row["revision"], "grant": grant.model_dump(mode="json")}),
            binding["revision"],
        )

    async def _check(self, active: _Active) -> EnvironmentRecord:
        current = await self.scope(active.scope.actor, active.identity, active.request)
        if current != active.scope:
            raise ServiceError("permission_denied", 403)
        receipt = await self.repositories.operations.lookup(active.identity)
        if receipt is None or receipt.state != "running":
            raise ServiceError("sandbox_operation_unknown", 409)
        if active.authorization_receipt is not None:
            current_access = await self.access.resolve(
                active.derived.authorization, active.derived.intent
            )
            if current_access != active.authorization_receipt:
                raise ServiceError("permission_denied", 403)
        return current.record

    async def _claim(self, identity: OperationIdentity) -> None:
        running = OperationReceipt(identity=identity, state="running")

        async def write(conn: AsyncConnection) -> bool:
            await self.repositories.ownership(conn, identity.environment, identity.ownership)
            raw = (
                await conn.execute(
                    select(development_operations.c.receipt).where(
                        development_operations.c.operation_id == identity.operation_id
                    )
                )
            ).scalar_one_or_none()
            if raw is None:
                raise ServiceError("sandbox_operation_unknown", 409)
            previous = OperationReceipt.model_validate(raw)
            if previous.identity != identity:
                raise ServiceError("key_conflict", 409)
            if previous.state != "accepted":
                raise ServiceError("sandbox_operation_unknown", 409)
            # Existing ledger only. _eval_write/SQLite write transaction orders
            # concurrent claimers; SQL state condition also prevents stale claim.
            result = await conn.execute(
                update(development_operations)
                .where(
                    development_operations.c.operation_id == identity.operation_id,
                    development_operations.c.receipt["state"].as_string() == "accepted",
                )
                .values(receipt=running.model_dump(mode="json"), updated_at=time.time())
            )
            return result.rowcount == 1

        async def uncertain() -> bool | None:
            # Even a matching running row after an uncertain COMMIT is not a
            # fresh execution permit. Leave it for SDK unknown receipt recovery.
            return False

        if not await self.store._eval_write(write, uncertain):
            raise ServiceError("sandbox_operation_unknown", 409)

    def _repository(self, scope: GitScope, request: GitRequest) -> Path:
        owner = TrustedGitRunner._safe_component(scope.ownership.owner_id)
        project = TrustedGitRunner._safe_component(scope.environment.project_id)
        name = hashlib.sha256(_operation(request).repository.encode("ascii")).hexdigest() + ".git"
        return self.repository_root / owner / project / name

    async def clone(
        self,
        actor: PrincipalContext,
        identity: OperationIdentity,
        request: GitExecutionRequest,
        *,
        approval_id: str | None = None,
    ) -> GitExecutionResult:
        return cast(
            GitExecutionResult,
            await self._execute("git.clone", actor, identity, request, approval_id),
        )

    async def diff(
        self,
        actor: PrincipalContext,
        identity: OperationIdentity,
        request: GitExecutionRequest,
        *,
        approval_id: str | None = None,
    ) -> GitExecutionResult:
        return cast(
            GitExecutionResult,
            await self._execute("git.diff", actor, identity, request, approval_id),
        )

    async def commit(
        self,
        actor: PrincipalContext,
        identity: OperationIdentity,
        request: GitExecutionRequest,
        *,
        approval_id: str | None = None,
    ) -> GitExecutionResult:
        return cast(
            GitExecutionResult,
            await self._execute("git.commit", actor, identity, request, approval_id),
        )

    async def push(
        self,
        actor: PrincipalContext,
        identity: OperationIdentity,
        request: GitExecutionRequest,
        *,
        approval_id: str | None = None,
    ) -> GitExecutionResult:
        return cast(
            GitExecutionResult,
            await self._execute("git.push", actor, identity, request, approval_id),
        )

    async def tree(
        self,
        actor: PrincipalContext,
        identity: OperationIdentity,
        request: GitTreeRequest,
        *,
        approval_id: str | None = None,
    ) -> GitTreeResult:
        return cast(
            GitTreeResult, await self._execute("tree", actor, identity, request, approval_id)
        )

    async def _execute(
        self,
        action: str,
        actor: PrincipalContext,
        identity: OperationIdentity,
        request: GitRequest,
        approval_id: str | None,
    ) -> GitExecutionResult | GitTreeResult:
        if (action == "tree") != isinstance(request, GitTreeRequest):
            raise ServiceError("invalid_input", 422)
        if action != "tree" and _operation(request).action != action:
            raise ServiceError("invalid_input", 422)
        scope = await self.scope(actor, identity, request)
        derived = derive_intent(request, scope, identity, approval_id)
        self._not_closed(scope.ownership)
        if identity.operation_id in self._active:
            raise ServiceError("sandbox_operation_unknown", 409)
        task = asyncio.current_task()
        assert task is not None
        active = _Active(scope, identity, request, derived, task)
        self._active[identity.operation_id] = active
        failed = False
        try:
            await self._claim(identity)
            async with asyncio.timeout(125):
                await self._check(active)
                receipt = await self.access.resolve(derived.authorization, derived.intent)
                active.authorization_receipt = receipt
                if action == "tree":
                    return await self._tree(active)
                assert isinstance(request, GitExecutionRequest)
                if action in ("git.clone", "git.push"):
                    await self._remote(active, receipt)
                await self._check(active)
                result = await self.runner.execute(
                    derived.intent,
                    derived.authorization,
                    request,
                    cast(ProjectAccessService, _ScopedAccess(self, active)),
                    credential_helper=(active.cache.helper,) if active.cache else (),
                    credential_binding=active.cache.binding if active.cache else None,
                    transport=active.lease,
                )
                await self._check(active)
                return result
        except asyncio.CancelledError:
            raise
        except ServiceError:
            raise
        except Exception:
            failed = True
        finally:
            active.executing = False
            await self._settle_release(active)
        if failed:
            raise ServiceError("execution_error", 500)
        raise ServiceError("execution_error", 500)

    async def _tree(self, active: _Active) -> GitTreeResult:
        source = WorkspaceBinding(
            active.scope.environment,
            active.scope.record.content_path,
            active.scope.record.content_device,
            active.scope.record.content_inode,
        )
        destination = self._repository(active.scope, active.request)

        async def verify(current: WorkspaceBinding, target: Path) -> None:
            if current != source or target != destination:
                raise ServiceError("permission_denied", 403)
            await self._check(active)
            await self.access.resolve(active.derived.authorization, active.derived.intent)

        result = await self.workspace.import_tree(source, destination, verify=verify)
        await self._check(active)
        return GitTreeResult(
            tree_id=result.tree_id, file_count=result.file_count, byte_count=result.byte_count
        )

    async def _remote(self, active: _Active, receipt: ResolvedAccess) -> None:
        derived = active.derived
        if active.scope.secret_refs:
            if len(receipt.secret_revisions) != 1:
                raise ServiceError("permission_denied", 403)
            secret_id, revision = receipt.secret_revisions[0]
            binding = DeliveryBinding(derived.intent, derived.authorization, secret_id, revision)
            active.cache = NativeGitCredentialCache(self.cache_root, binding)
            with self.credential_sink.route(binding, active.cache):
                await self.access.deliver_git_credential(
                    derived.authorization,
                    derived.intent,
                    helper_request(_operation(active.request)),
                    helper_action="get",
                )
            await active.cache.ready()

        async def resolve() -> GitNetworkAuthorization:
            record = await self._check(active)
            current = await self.access.resolve(derived.authorization, derived.intent)
            policy = await self.access.network_policy(derived.authorization, derived.intent)
            return GitNetworkAuthorization(current, policy, record.environment, record.image_id)

        active.lease = DockerGitTransport(
            GitTransportBinding(
                active.scope.environment, derived.authorization, derived.intent, self.image_id
            ),
            stage_root=self.stage_root,
            repository_root=self.repository_root,
            resolve=resolve,
            cache=active.cache,
        )
        # Host paths are derived from the validated scope, never request fields.
        repository = self.runner._repository_path(derived.intent, _operation(active.request))
        await active.lease.prepare(repository)

    async def _release(self, active: _Active) -> None:
        error = False
        if active.lease is not None:
            try:
                await active.lease.close()
            except Exception:
                error = True
        if active.cache is not None:
            try:
                await asyncio.to_thread(active.cache.close)
            except Exception:
                error = True
        if error:
            raise ServiceError("execution_error", 500)
        if self._active.get(active.identity.operation_id) is active:
            del self._active[active.identity.operation_id]

    async def _settle_release(self, active: _Active) -> None:
        cleanup = asyncio.create_task(self._release(active))
        cancelled = False
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                cancelled = True
        cleanup.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _stop(self, selected: list[_Active]) -> None:
        current = asyncio.current_task()
        for active in selected:
            if active.executing and active.task is current:
                raise ServiceError("invalid_input", 422)
            if active.executing and not active.task.done():
                active.task.cancel()
        tasks = [active.task for active in selected if active.executing and not active.task.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        failed = False
        for active in selected:
            try:
                await self._release(active)
            except Exception:
                failed = True
        if failed:
            raise ServiceError("execution_error", 500)

    async def invalidate_project(self, owner_id: str, project_id: str) -> None:
        selected = [a for a in self._active.values()
                    if a.scope.ownership.owner_id == owner_id
                    and a.scope.environment.project_id == project_id]
        self.runner.invalidate(owner_id, project_id)
        await self._stop(selected)

    async def revoke_owner(self, owner_id: str) -> None:
        self._revoked_owners.add(owner_id)
        selected = [a for a in self._active.values() if a.scope.ownership.owner_id == owner_id]
        for active in selected:
            self.runner.invalidate(owner_id, active.scope.environment.project_id)
        await self._stop(selected)

    async def close_run(self, ownership: RunOwnership) -> None:
        self._closed_runs.add(self._run_key(ownership))
        await self._stop([a for a in self._active.values() if a.scope.ownership == ownership])

    async def close(self) -> None:
        self._closed = True
        await self._stop(list(self._active.values()))
