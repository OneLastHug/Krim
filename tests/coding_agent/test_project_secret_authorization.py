"""Command secret authorization against the existing SQLite operation ledger."""

import asyncio
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import pytest
from sqlalchemy import select, update

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import (
    CommandRequest,
    OperationIdentity,
    OperationReceipt,
    RunOwnership,
)
from krim_agent.development.ports import ProcessRecord, command_digest
from krim_agent.project_access.process_secrets import CommandSecretBinding
from krim_agent.project_access.repository import AccessSnapshot, CommandAccessState
from krim_agent.project_access.secrets import SecretMaterial, encrypt_secret
from krim_agent.project_access.service import ProjectAccessService, command_intent
from krim_agent.project_access.types import ProjectGrant, SecretRecord
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.service import SandboxService
from krim_agent.storage.account_models import accounts
from krim_agent.storage.development import _environment_record, _process_record
from krim_agent.storage.development_models import (
    development_environments,
    development_operations,
    development_processes,
)
from krim_agent.storage.models import runs, sessions, turns
from krim_agent.storage.project_access import SQLiteAccessRepository
from krim_agent.storage.sandbox_models import sandbox_projects
from tests.coding_agent.test_project_access_storage import access as access_fixture

EXECUTION_ID = "e" * 64


class CommandSink:
    """Bounded test sink that retains only synthetic fixture materials."""

    def __init__(self) -> None:
        self.accepted: list[tuple[bytes, ...]] = []
        self.materials: list[SecretMaterial] = []
        self.expiries: list[float] = []
        self.released: list[CommandSecretBinding] = []
        self.release_checks = 0
        self.reject_delivery = False
        self.on_accept = None

    def deliver_command(
        self,
        binding: CommandSecretBinding,
        materials: tuple[SecretMaterial, ...],
        *,
        expires_at: float,
    ) -> None:
        if self.on_accept is not None:
            self.on_accept()
        self.materials.extend(materials)
        self.expiries.append(expires_at)
        if self.reject_delivery:
            raise RuntimeError("fixture sink full")
        self.accepted.append(tuple(bytes(item.borrow()) for item in materials))

    def release_command(self, binding: CommandSecretBinding, check) -> None:
        check()
        self.release_checks += 1
        self.released.append(binding)


class CommandAccessRepository:
    """Test adapter that projects command proof inside the SQLite guard txn.

    SQLiteAccessRepository currently owns the base actor/grant/secret readers;
    this wrapper uses those readers and extends their immediate transaction with
    the existing operation, process, environment, run and session rows.
    """

    def __init__(self, inner: SQLiteAccessRepository) -> None:
        self.inner = inner
        self.store = inner.store

    async def _command_state(self, conn, intent) -> CommandAccessState | None:
        operation_row = (
            await conn.execute(
                select(development_operations).where(
                    development_operations.c.operation_id == intent.operation_id,
                    development_operations.c.owner_id == intent.owner_id,
                    development_operations.c.project_id == intent.project_id,
                )
            )
        ).mappings().first()
        process_row = (
            await conn.execute(
                select(development_processes).where(
                    development_processes.c.operation_id == intent.operation_id,
                    development_processes.c.owner_id == intent.owner_id,
                    development_processes.c.project_id == intent.project_id,
                )
            )
        ).mappings().first()
        if operation_row is None or process_row is None:
            return None

        identity = OperationIdentity.model_validate(operation_row["identity"])
        operation = OperationReceipt.model_validate(operation_row["receipt"])
        process: ProcessRecord = _process_record(process_row)
        environment_row = (
            await conn.execute(
                select(development_environments).where(
                    development_environments.c.environment_id
                    == process.request.environment.environment_id,
                )
            )
        ).mappings().first()
        if environment_row is None:
            return None
        environment_record = _environment_record(environment_row["record"])

        joined = (
            await conn.execute(
                select(
                    turns.c.session_id.label("turn_session_id"),
                    turns.c.owner_id.label("turn_owner_id"),
                    turns.c.workspace_id.label("turn_workspace_id"),
                    turns.c.run_id.label("turn_run_id"),
                    sessions.c.id.label("joined_session_id"),
                    sessions.c.owner_id.label("session_owner_id"),
                    sessions.c.workspace_id.label("session_workspace_id"),
                )
                .select_from(turns.join(sessions, turns.c.session_id == sessions.c.id))
                .where(turns.c.run_id == identity.ownership.run_id)
            )
        ).mappings().first()
        run = (
            await conn.execute(
                select(runs).where(runs.c.id == identity.ownership.run_id)
            )
        ).mappings().first()
        if joined is None or run is None:
            return None
        turn_session_id = joined["turn_session_id"]
        ownership = RunOwnership(
            owner_id=joined["turn_owner_id"],
            session_id=turn_session_id,
            run_id=joined["turn_run_id"],
        )
        indexed_environment = environment_record.environment
        if (
            operation.identity != identity
            or identity.operation_id != intent.operation_id
            or operation_row["environment_id"] != indexed_environment.environment_id
            or operation_row["owner_id"] != ownership.owner_id
            or operation_row["project_id"] != indexed_environment.project_id
            or operation_row["session_id"] != ownership.session_id
            or operation_row["run_id"] != ownership.run_id
            or process.request.operation_id != identity.operation_id
            or process.request.ownership != ownership
            or process.request.environment.model_dump(exclude={"state"})
            != indexed_environment.model_dump(exclude={"state"})
            or process_row["environment_id"] != indexed_environment.environment_id
            or process_row["session_id"] != ownership.session_id
            or process_row["run_id"] != ownership.run_id
            or process_row["owner_id"] != ownership.owner_id
            or process_row["project_id"] != indexed_environment.project_id
            or environment_row["owner_id"] != indexed_environment.owner_id
            or environment_row["project_id"] != indexed_environment.project_id
            or environment_row["generation"] != indexed_environment.generation
            or turn_session_id != identity.ownership.session_id
            or joined["turn_run_id"] != identity.ownership.run_id
            or joined["turn_owner_id"] != identity.ownership.owner_id
            or joined["turn_workspace_id"] != intent.workspace_id
            or joined["session_owner_id"] != identity.ownership.owner_id
            or joined["session_workspace_id"] != intent.workspace_id
            or joined["joined_session_id"] != identity.ownership.session_id
            or environment_record.environment.owner_id != intent.owner_id
            or environment_record.environment.project_id != intent.project_id
            or operation_row["command_digest"] is None
        ):
            return None
        return CommandAccessState(
            operation=operation,
            process=process,
            environment=indexed_environment,
            ownership=ownership,
            run_status=run["status"],
            stop_requested=run["stop_requested"],
            command_digest=operation_row["command_digest"],
        )

    @asynccontextmanager
    async def guard(self, ref, intent):
        self.inner.require()
        if ref.project_id != intent.project_id:
            raise ServiceError("permission_denied", 403)
        async with self.store.lock, self.store.engine.connect() as conn:
            await conn.execution_options(krim_immediate=True)
            async with conn.begin():
                project = (
                    await conn.execute(
                        select(sandbox_projects).where(
                            sandbox_projects.c.project_id == intent.project_id,
                        )
                    )
                ).mappings().first()
                if (
                    project is None
                    or project["principal_id"] != intent.owner_id
                    or project["workspace_id"] != intent.workspace_id
                ):
                    raise ServiceError("permission_denied", 403)
                actor = await self.inner._actor(conn, ref.actor_id)
                grant = await self.inner._record(
                    conn, intent.owner_id, intent.project_id, "grant", ref.actor_id
                )
                secrets = []
                for secret_id in ref.secret_refs:
                    record = await self.inner._record(
                        conn, intent.owner_id, intent.project_id, "secret", secret_id
                    )
                    if isinstance(record, SecretRecord):
                        secrets.append(record)
                network = (
                    await self.inner._record(
                        conn,
                        intent.owner_id,
                        intent.project_id,
                        "network",
                        ref.network_policy_ref,
                    )
                    if ref.network_policy_ref
                    else None
                )
                command = await self._command_state(conn, intent)
                yield AccessSnapshot(
                    actor=actor,
                    grant=grant if isinstance(grant, ProjectGrant) else None,
                    approval=await self.inner._approval(conn, ref, intent),
                    secrets=tuple(secrets),
                    network=network,
                    command=command,
                )


@dataclass
class PreparedCommand:
    store: object
    development: object
    access_repository: SQLiteAccessRepository
    repository: CommandAccessRepository
    service: ProjectAccessService
    sink: CommandSink
    request: CommandRequest
    intent: object
    binding: CommandSecretBinding
    secrets: dict[str, SecretRecord]
    grant: ProjectGrant
    actor_id: str
    actor_generation: int
    workspace_id: str
    project_id: str
    identity: OperationIdentity
    database: Path


async def _authorize(
    request: CommandRequest, workspace_id: str,
) -> tuple[CommandRequest, object]:
    intent = command_intent(request, workspace_id=workspace_id)
    authorization = request.authorization.model_copy(
        update={"arguments_digest": intent.digest(request.authorization)}
    )
    request = request.model_copy(update={"authorization": authorization})
    return request, intent


async def prepare_command(
    path: Path,
    *,
    secret_ids: tuple[str, ...] = ("image-api",),
    include_command_proof: bool = True,
    command: str | None = None,
) -> PreparedCommand:
    (
        store,
        development,
        access_repository,
        _old_service,
        _old_sink,
        original_ref,
        _old_intent,
        original_secret,
        original_grant,
        vault,
        original_request,
    ) = await access_fixture(path)
    project_id = original_request.environment.project_id
    owner_id = original_request.environment.owner_id
    actor_generation = original_ref.actor_generation
    async with store.engine.connect() as connection:
        workspace_id = await connection.scalar(
            select(sandbox_projects.c.workspace_id).where(
                sandbox_projects.c.project_id == project_id,
            )
        )

    grant = original_grant.model_copy(
        update={"revision": original_grant.revision + 1,
                "actions": ("shell.execute", "secret.deliver")}
    )
    await access_repository.put(grant, expected_revision=original_grant.revision)
    secrets = {original_secret.secret_id: original_secret}
    for secret_id in secret_ids:
        if secret_id == original_secret.secret_id:
            continue
        record = encrypt_secret(
            vault,
            SecretRecord(
                owner_id=owner_id,
                project_id=project_id,
                secret_id=secret_id,
                revision=1,
                purpose="project.delivery",
                ciphertext="fixture-ciphertext",
                expires_at=time.time() + 600,
            ),
            ("fixture-secret:" + secret_id).encode(),
        )
        await access_repository.put(record, expected_revision=0)
        secrets[secret_id] = record

    authorization = original_ref.model_copy(
        update={"secret_refs": secret_ids, "arguments_digest": "0" * 64}
    )
    request = original_request.model_copy(update={"authorization": authorization})
    if command is not None:
        request = request.model_copy(update={"command": command})
    request, intent = await _authorize(request, workspace_id)
    identity = OperationIdentity(
        operation_id=request.operation_id,
        call_id="host-shell-call",
        environment=request.environment,
        ownership=request.ownership,
        request_digest=request.authorization.arguments_digest,
    )
    admitted = await development.operations.admit(identity)
    assert admitted.execute
    await development.operations.bind_command(identity, request)
    reservation = await development.processes.reserve(request, command_digest(request))
    assert reservation.execute
    process = reservation.record
    process = await development.processes.save(
        process,
        ProcessRecord(
            request=process.request,
            handle=process.handle,
            request_digest=process.request_digest,
            revision=process.revision + 1,
            execution_id=EXECUTION_ID,
        ),
    )
    binding = CommandSecretBinding(
        request=request,
        workspace_id=workspace_id,
        process_id=process.handle.process_id,
        execution_id=EXECUTION_ID,
        revisions=tuple((secret_id, secrets[secret_id].revision) for secret_id in secret_ids),
    )
    repository = CommandAccessRepository(access_repository)
    sink = CommandSink()
    class MissingCommandProof:
        @asynccontextmanager
        async def guard(self, ref, intent):
            async with access_repository.guard(ref, intent) as snapshot:
                yield replace(snapshot, command=None)

    active_repository = repository if include_command_proof else MissingCommandProof()
    service = ProjectAccessService(
        active_repository,
        vault,
        secret_sink=_old_sink,
        git_sink=_old_sink,
        command_sink=sink,
    )
    return PreparedCommand(
        store=store,
        development=development,
        access_repository=access_repository,
        repository=repository,
        service=service,
        sink=sink,
        request=request,
        intent=intent,
        binding=binding,
        secrets=secrets,
        grant=grant,
        actor_id=original_ref.actor_id,
        actor_generation=actor_generation,
        workspace_id=workspace_id,
        project_id=project_id,
        identity=identity,
        database=path,
    )


def _binding_for(fixture: PreparedCommand, request: CommandRequest) -> CommandSecretBinding:
    return CommandSecretBinding(
        request=request,
        workspace_id=fixture.workspace_id,
        process_id=fixture.binding.process_id,
        execution_id=fixture.binding.execution_id,
        revisions=fixture.binding.revisions,
    )


def _assert_erased(sink: CommandSink) -> None:
    for material in sink.materials:
        view = material.borrow()
        assert bytes(view) == b"\x00" * len(view)


def _try_generation_change(database: Path, actor_id: str) -> str:
    try:
        with sqlite3.connect(database, timeout=0.05) as connection:
            connection.execute(
                "UPDATE accounts SET authorization_generation="
                "authorization_generation+1 WHERE principal_id=?",
                (actor_id,),
            )
            connection.commit()
    except sqlite3.OperationalError as error:
        assert "locked" in str(error)
        return "blocked"
    return "committed"


def _attempt_generation_change_during_guard(database: Path, actor_id: str) -> str:
    results: list[str] = []
    worker = threading.Thread(
        target=lambda: results.append(_try_generation_change(database, actor_id))
    )
    worker.start()
    worker.join(timeout=2)
    assert not worker.is_alive()
    return results[0]


async def _attempt_generation_change(database: Path, actor_id: str) -> str:
    return await asyncio.to_thread(_try_generation_change, database, actor_id)


async def test_shell_permission_does_not_implicitly_grant_secret_delivery(tmp_path: Path) -> None:
    fixture = await prepare_command(tmp_path / "shell-only.sqlite")
    try:
        restricted = fixture.grant.model_copy(
            update={"revision": fixture.grant.revision + 1, "actions": ("shell.execute",)}
        )
        await fixture.access_repository.put(restricted, expected_revision=fixture.grant.revision)
        with pytest.raises(ServiceError, match="permission_denied"):
            await fixture.service.deliver_command_secrets(fixture.binding)
        assert fixture.sink.accepted == []
    finally:
        await fixture.store.close()


async def test_valid_delivery_erases_material_and_holds_final_guard_through_release(
    tmp_path: Path,
) -> None:
    fixture = await prepare_command(tmp_path / "command.sqlite")
    try:
        lock_results: list[str] = []
        fixture.sink.on_accept = lambda: lock_results.append(
            _attempt_generation_change_during_guard(fixture.database, fixture.actor_id)
        )
        receipt = await fixture.service.deliver_command_secrets(fixture.binding)
        assert receipt.secret_revisions == (("image-api", 1),)
        assert fixture.sink.accepted == [(b"fixture-secret",)]
        assert fixture.sink.expiries[0] > time.time()
        assert fixture.sink.expiries[0] <= time.time() + fixture.request.timeout_seconds
        assert lock_results == ["blocked"]
        _assert_erased(fixture.sink)

        await fixture.service.release_command_secrets(fixture.binding)
        assert fixture.sink.release_checks == 1
        assert fixture.sink.released == [fixture.binding]
        assert await _attempt_generation_change(fixture.database, fixture.actor_id) == "committed"
        assert b"fixture-secret" not in fixture.database.read_bytes()
    finally:
        await fixture.store.close()


@pytest.mark.parametrize("mutation", ["actor_generation", "grant_expired", "secret_expired"])
async def test_current_actor_and_expiry_are_rechecked_before_delivery(
    tmp_path: Path, mutation: str,
) -> None:
    fixture = await prepare_command(tmp_path / f"{mutation}.sqlite")
    try:
        if mutation == "actor_generation":
            async with fixture.store.engine.begin() as connection:
                await connection.execute(
                    update(accounts)
                    .where(accounts.c.principal_id == fixture.actor_id)
                    .values(authorization_generation=fixture.actor_generation + 1)
                )
        elif mutation == "grant_expired":
            expired = fixture.grant.model_copy(
                update={"revision": fixture.grant.revision + 1, "expires_at": 0}
            )
            await fixture.access_repository.put(expired, expected_revision=fixture.grant.revision)
        else:
            secret = fixture.secrets["image-api"]
            expired = secret.model_copy(update={"revision": secret.revision + 1, "expires_at": 0})
            await fixture.access_repository.put(expired, expected_revision=secret.revision)

        with pytest.raises(ServiceError, match="permission_denied"):
            await fixture.service.deliver_command_secrets(fixture.binding)
        assert fixture.sink.accepted == []
        _assert_erased(fixture.sink)
    finally:
        await fixture.store.close()


async def test_command_proof_cannot_cross_project_or_run_ownership(
    tmp_path: Path,
) -> None:
    fixture = await prepare_command(tmp_path / "scope.sqlite")
    identities = await IdentityService.open(fixture.store)
    actor = identities.resolve(fixture.actor_id)
    sandbox = SandboxService(fixture.store, identities, SandboxConfiguration())
    try:
        other_project = await sandbox.create_project(
            actor,
            {"workspace_id": fixture.workspace_id, "title": "second project"},
            request_key="second-project",
        )
        other_environment = fixture.request.environment.model_copy(
            update={"project_id": other_project["project_id"]}
        )
        other_authorization = fixture.request.authorization.model_copy(
            update={"project_id": other_project["project_id"]}
        )
        cross_project = fixture.request.model_copy(
            update={"environment": other_environment, "authorization": other_authorization}
        )
        cross_project, _ = await _authorize(cross_project, fixture.workspace_id)
        with pytest.raises(ServiceError, match="permission_denied"):
            await fixture.service.deliver_command_secrets(
                _binding_for(fixture, cross_project)
            )

        other_ownership = fixture.request.ownership.model_copy(
            update={"session_id": "unrelated-session"}
        )
        wrong_owner = fixture.request.model_copy(update={"ownership": other_ownership})
        wrong_owner, _ = await _authorize(wrong_owner, fixture.workspace_id)
        with pytest.raises(ServiceError, match="permission_denied"):
            await fixture.service.deliver_command_secrets(_binding_for(fixture, wrong_owner))
        assert fixture.sink.accepted == []
    finally:
        await sandbox.close()
        await identities.close()
        await fixture.store.close()


@pytest.mark.parametrize("mutation", ["run_stopped", "operation_unknown", "execution_mismatch"])
async def test_stopped_run_unknown_operation_and_wrong_execution_are_denied(
    tmp_path: Path, mutation: str,
) -> None:
    fixture = await prepare_command(tmp_path / f"{mutation}.sqlite")
    binding = fixture.binding
    try:
        if mutation == "run_stopped":
            await fixture.store.request_stop(fixture.request.ownership.run_id)
        elif mutation == "operation_unknown":
            receipt = await fixture.development.operations.lookup(fixture.identity)
            assert receipt is not None
            await fixture.development.operations.save_receipt(
                receipt.model_copy(update={"state": "unknown"})
            )
        else:
            binding = CommandSecretBinding(
                request=fixture.request,
                workspace_id=fixture.workspace_id,
                process_id=fixture.binding.process_id,
                execution_id="f" * 64,
                revisions=fixture.binding.revisions,
            )

        with pytest.raises(ServiceError, match="permission_denied"):
            await fixture.service.deliver_command_secrets(binding)
        assert fixture.sink.accepted == []
    finally:
        await fixture.store.close()


async def test_rotated_secret_rejects_binding_to_old_revision(tmp_path: Path) -> None:
    fixture = await prepare_command(tmp_path / "rotation.sqlite")
    try:
        secret = fixture.secrets["image-api"]
        rotated = encrypt_secret(
            AuthenticatedSecretVault(MasterKeyring.create(b"x" * 32)),
            secret.model_copy(update={"revision": 2}),
            b"rotated-fixture-secret",
        )
        await fixture.access_repository.put(rotated, expected_revision=secret.revision)
        with pytest.raises(ServiceError, match="permission_denied"):
            await fixture.service.deliver_command_secrets(fixture.binding)
        assert fixture.sink.accepted == []
    finally:
        await fixture.store.close()


async def test_revoked_member_rejects_entire_multi_secret_package(tmp_path: Path) -> None:
    fixture = await prepare_command(
        tmp_path / "multi-secret.sqlite", secret_ids=("image-api", "second-secret")
    )
    try:
        secret = fixture.secrets["second-secret"]
        revoked = secret.model_copy(
            update={"revision": secret.revision + 1, "revoked": True}
        )
        await fixture.access_repository.put(revoked, expected_revision=secret.revision)
        with pytest.raises(ServiceError, match="permission_denied"):
            await fixture.service.deliver_command_secrets(fixture.binding)
        assert fixture.sink.accepted == []
        _assert_erased(fixture.sink)
    finally:
        await fixture.store.close()


@pytest.mark.parametrize("case", ["missing_command_proof", "sink_rejects_package"])
async def test_missing_proof_or_rejecting_sink_never_accepts_delivery(
    tmp_path: Path, case: str,
) -> None:
    fixture = await prepare_command(
        tmp_path / f"{case}.sqlite", include_command_proof=case != "missing_command_proof"
    )
    try:
        if case == "sink_rejects_package":
            fixture.sink.reject_delivery = True
            with pytest.raises(ServiceError, match="execution_error"):
                await fixture.service.deliver_command_secrets(fixture.binding)
            _assert_erased(fixture.sink)
        else:
            with pytest.raises(ServiceError, match="permission_denied"):
                await fixture.service.deliver_command_secrets(fixture.binding)
        assert fixture.sink.accepted == []
    finally:
        await fixture.store.close()
