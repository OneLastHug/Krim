"""D domain service with real identity rows, AESGCM and SQLite delivery fencing."""

import asyncio
import sqlite3
import time
from pathlib import Path

import pytest

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.development.contracts import AuthorizationRef, OperationIdentity
from krim_agent.project_access.secrets import encrypt_secret
from krim_agent.project_access.service import ProjectAccessService
from krim_agent.project_access.types import AccessIntent, ProjectGrant, SecretRecord
from krim_agent.storage.project_access import SQLiteAccessRepository
from krim_agent.storage.sqlite import SQLiteStore
from tests.coding_agent.test_development_storage import prepare


class Sink:
    def __init__(self) -> None:
        self.values: list[bytes] = []
        self.bindings = []

    def deliver(self, binding, material) -> None:
        self.values.append(bytes(material.borrow()))
        self.bindings.append(binding)


async def access(path: Path, *, approval: bool = False):
    store, development, _, request = await prepare(path)
    actor = request.authorization
    repo = SQLiteAccessRepository(store)
    async with store.engine.connect() as conn:
        from sqlalchemy import select

        from krim_agent.storage.sandbox_models import sandbox_projects

        workspace = await conn.scalar(
            select(sandbox_projects.c.workspace_id).where(
                sandbox_projects.c.project_id == request.environment.project_id,
            )
        )
    intent = AccessIntent(
        owner_id=request.environment.owner_id,
        workspace_id=workspace,
        project_id=request.environment.project_id,
        operation_id=request.operation_id,
        action="secret.deliver",
        arguments_json=encoded({"purpose": "fixture"}),
    )
    ref = AuthorizationRef(
        actor_id=actor.actor_id,
        actor_generation=actor.actor_generation,
        project_id=intent.project_id,
        approval_id="host-request" if approval else None,
        secret_refs=("image-api",),
        arguments_digest="0" * 64,
    )
    ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
    grant = ProjectGrant(
        owner_id=intent.owner_id,
        workspace_id=intent.workspace_id,
        project_id=intent.project_id,
        actor_id=actor.actor_id,
        actor_generation=actor.actor_generation,
        revision=1,
        actions=("secret.deliver",),
        requires_approval=approval,
        expires_at=time.time() + 300,
    )
    await repo.put(grant, expected_revision=0)
    vault = AuthenticatedSecretVault(MasterKeyring.create(b"x" * 32))
    secret = encrypt_secret(
        vault,
        SecretRecord(
            owner_id=intent.owner_id,
            project_id=intent.project_id,
            secret_id="image-api",
            revision=1,
            purpose="project.delivery",
            ciphertext="pending",
            expires_at=time.time() + 300,
        ),
        b"fixture-secret",
    )
    await repo.put(secret, expected_revision=0)
    sink = Sink()
    service = ProjectAccessService(repo, vault, secret_sink=sink, git_sink=sink)
    return store, development, repo, service, sink, ref, intent, secret, grant, vault, request


async def test_secret_ciphertext_survives_store_reopen_and_rotation_is_current(
    tmp_path: Path,
) -> None:
    values = await access(tmp_path / "access.sqlite")
    store, _, repo, service, sink, ref, intent, secret, _, vault, _ = values
    try:
        assert "ciphertext" not in secret.model_dump()
        result = await service.deliver_secret(ref, intent)
        assert sink.values == [b"fixture-secret"]
        assert "fixture-secret" not in repr(result)
        await store.close()
        store = await SQLiteStore.open(tmp_path / "access.sqlite")
        repo = SQLiteAccessRepository(store)
        service = ProjectAccessService(repo, vault, secret_sink=sink, git_sink=sink)
        await service.deliver_secret(ref, intent)
        assert sink.values[-1] == b"fixture-secret"
        rotated = encrypt_secret(
            vault, secret.model_copy(update={"revision": 2}), b"rotated-fixture"
        )
        await repo.put(rotated, expected_revision=1)
        with pytest.raises(ServiceError, match="revision_conflict"):
            await repo.put(secret, expected_revision=0)
        await service.deliver_secret(ref, intent)
        assert sink.values[-1] == b"rotated-fixture"
        assert sink.bindings[-1].revision == 2
        assert sink.bindings[-1].authorization == ref
        assert b"fixture-secret" not in (tmp_path / "access.sqlite").read_bytes()
    finally:
        await store.close()


async def test_committed_secret_revocation_prevents_delivery(tmp_path: Path) -> None:
    store, _, repo, service, sink, ref, intent, secret, *_ = await access(
        tmp_path / "access.sqlite"
    )
    try:
        revoked = secret.model_copy(update={"revision": 2, "revoked": True})
        await repo.put(revoked, expected_revision=1)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.deliver_secret(ref, intent)
        assert sink.values == []
    finally:
        await store.close()


async def test_guard_fences_other_sqlite_writer_until_acceptance(tmp_path: Path) -> None:
    database = tmp_path / "access.sqlite"
    store, _, repo, service, sink, ref, intent, *_ = await access(database)

    def revoke() -> str:
        with sqlite3.connect(database, timeout=0.05) as connection:
            try:
                connection.execute(
                    "UPDATE accounts SET authorization_generation="
                    "authorization_generation+1 WHERE principal_id=?",
                    (ref.actor_id,),
                )
                connection.commit()
            except sqlite3.OperationalError as error:
                assert "locked" in str(error)
                return "blocked"
        return "committed"

    try:
        async with repo.guard(ref, intent):
            assert await asyncio.to_thread(revoke) == "blocked"
        assert await asyncio.to_thread(revoke) == "committed"
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.deliver_secret(ref, intent)
        assert sink.values == []
    finally:
        await store.close()


@pytest.mark.parametrize("scoped,stop", [(False, False), (True, False), (True, True)])
async def test_existing_approval_requires_project_scope_projection(
    tmp_path: Path, scoped: bool, stop: bool
) -> None:
    values = await access(tmp_path / "access.sqlite", approval=True)
    store, development, _, service, sink, ref, intent, _, _, _, command = values
    try:
        identity = OperationIdentity(
            operation_id=intent.operation_id,
            call_id="host-call",
            environment=command.environment,
            ownership=command.ownership,
            request_digest="b" * 64,
        )
        await development.operations.admit(identity)
        request = {
            "request_id": ref.approval_id,
            "run_id": identity.ownership.run_id,
            "call_id": identity.call_id,
            "arguments_hash": fingerprint({"purpose": "fixture"}),
            "tool_hash": "c" * 64,
            "expires_at": time.time() + 60,
        }
        if scoped:
            request["project_access"] = {
                "actor_id": ref.actor_id,
                "actor_generation": ref.actor_generation,
                "owner_id": intent.owner_id,
                "workspace_id": intent.workspace_id,
                "project_id": intent.project_id,
                "operation_id": intent.operation_id,
                "action": intent.action,
                "arguments_digest": ref.arguments_digest,
            }
        await store.append(identity.ownership.run_id, "approval_request", request)
        await store.append(
            identity.ownership.run_id,
            "approval_reply",
            {
                "request_id": ref.approval_id,
                "reply": {
                    "call_id": identity.call_id,
                    "arguments_hash": request["arguments_hash"],
                    "tool_hash": request["tool_hash"],
                    "allow": True,
                },
            },
        )
        if stop:
            await store.request_stop(identity.ownership.run_id)
        if scoped and not stop:
            await service.deliver_secret(ref, intent)
            assert sink.values == [b"fixture-secret"]
        else:
            with pytest.raises(ServiceError, match="permission_denied"):
                await service.deliver_secret(ref, intent)
            assert sink.values == []
    finally:
        await store.close()
