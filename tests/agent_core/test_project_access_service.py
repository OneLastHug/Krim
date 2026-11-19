from __future__ import annotations

import asyncio
import dataclasses
import json
import pickle
import traceback
from contextlib import asynccontextmanager

import pytest

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.contracts import ServiceError, encoded
from krim_agent.development.contracts import AuthorizationRef, CommandRequest
from krim_agent.project_access.git import GitGrant, GitIdentity
from krim_agent.project_access.repository import AccessSnapshot
from krim_agent.project_access.secrets import SecretMaterial, encrypt_secret, project_secret_context
from krim_agent.project_access.service import ProjectAccessService, command_intent
from krim_agent.project_access.types import (
    AccessIntent,
    ApprovalBinding,
    NetworkGrant,
    ProjectGrant,
    SecretRecord,
)
from krim_agent.security import Membership, PrincipalContext

TOKEN = b"fixture-only-D-secret-3e8cf"


class MemoryRepository:
    """Test-only serial guard; production I adapter must prove cross-process CAS."""

    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.lock = asyncio.Lock()

    @asynccontextmanager
    async def guard(self, ref, intent):
        async with self.lock:
            yield self.snapshot

    async def change(self, **values):
        async with self.lock:
            self.snapshot = dataclasses.replace(self.snapshot, **values)


class Sink:
    def __init__(self):
        self.calls = []
        self.views = []
        self.failure = False

    def deliver(self, binding, material):
        self.calls.append((binding, bytes(material.borrow())))
        self.views.append(material.borrow())
        if self.failure:
            raise RuntimeError(TOKEN.decode())


@pytest.fixture
def setup_access():
    vault = AuthenticatedSecretVault(MasterKeyring.create(b"k" * 32))
    clock = [1000.0]
    actor = PrincipalContext("owner", 3, (Membership(workspace_id="workspace"),))
    intent = AccessIntent(
        owner_id="owner",
        workspace_id="workspace",
        project_id="project-a",
        operation_id="op-1",
        action="secret.deliver",
        arguments_json=encoded({"destination": "trusted-service"}),
    )
    ref = AuthorizationRef(
        actor_id="owner",
        actor_generation=3,
        project_id="project-a",
        approval_id="approval",
        arguments_digest="0" * 64,
        secret_refs=("token",),
        network_policy_ref="network",
    )
    ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
    grant = ProjectGrant(
        owner_id="owner",
        workspace_id="workspace",
        project_id="project-a",
        actor_id="owner",
        actor_generation=3,
        revision=1,
        actions=(
            "secret.deliver",
            "git.push",
            "git.clone",
            "git.diff",
            "git.commit",
            "shell.execute",
            "network.connect",
        ),
        expires_at=2000.0,
    )
    approval = ApprovalBinding(
        approval_id="approval",
        actor_id="owner",
        actor_generation=3,
        owner_id="owner",
        workspace_id="workspace",
        project_id="project-a",
        operation_id="op-1",
        action=intent.action,
        arguments_digest=ref.arguments_digest,
        expires_at=2000.0,
        approved=True,
    )
    record = SecretRecord(
        owner_id="owner",
        project_id="project-a",
        secret_id="token",
        revision=1,
        purpose="project.delivery",
        ciphertext="pending",
        expires_at=2000.0,
    )
    record = encrypt_secret(vault, record, TOKEN)
    network = NetworkGrant(
        owner_id="owner",
        project_id="project-a",
        policy_id="network",
        revision=1,
        hosts=("git.example", "registry.npmjs.org", "cdn.example"),
        expires_at=2000.0,
    )
    snapshot = AccessSnapshot(
        actor=actor, grant=grant, approval=approval, secrets=(record,), network=network
    )
    repo = MemoryRepository(snapshot)
    sink = Sink()
    git_sink = Sink()
    service = ProjectAccessService(
        repo, vault, secret_sink=sink, git_sink=git_sink, clock=lambda: clock[0]
    )
    return service, repo, intent, ref, vault, sink, git_sink, clock


@pytest.mark.asyncio
async def test_resolve_and_delivery_only_return_references(setup_access):
    service, repo, intent, ref, _, sink, _, _ = setup_access
    receipt = await service.resolve(ref, intent)
    assert not sink.calls
    assert receipt.secret_revisions == (("token", 1),)
    delivered = await service.deliver_secret(ref, intent)
    assert delivered == receipt
    assert sink.calls[0][1] == TOKEN
    binding = sink.calls[0][0]
    assert binding.intent == intent
    assert binding.authorization == ref
    assert binding.revision == 1
    assert binding.secret_id == "token"
    assert bytes(sink.views[0]) == b"\0" * len(TOKEN)
    assert TOKEN.decode() not in repr(repo.snapshot)
    assert TOKEN.decode() not in delivered.model_dump_json()
    assert "ciphertext" not in repo.snapshot.secrets[0].model_dump()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "update",
    [
        {"owner_id": "other"},
        {"project_id": "project-b"},
        {"workspace_id": "other"},
        {"operation_id": "op-2"},
        {"action": "git.push"},
        {"arguments_json": '{"destination":"attacker"}'},
    ],
)
async def test_actual_intent_drift_is_denied(setup_access, update):
    service, _, intent, ref, _, sink, _, _ = setup_access
    with pytest.raises(ServiceError):
        await service.deliver_secret(ref, intent.model_copy(update=update))
    assert not sink.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,update",
    [
        ("grant", {"revoked": True}),
        ("grant", {"expires_at": 1000.0}),
        ("grant", {"actor_generation": 4}),
        ("grant", {"actions": ("network.connect",)}),
        ("grant", {"project_id": "project-b"}),
        ("approval", {"approved": False}),
        ("approval", {"expires_at": 999.0}),
        ("approval", {"actor_generation": 4}),
        ("approval", {"operation_id": "op-other"}),
        ("approval", {"approval_id": "other"}),
        ("approval", {"action": "git.push"}),
        ("approval", {"arguments_digest": "f" * 64}),
        ("network", {"revoked": True}),
        ("network", {"expires_at": 1000.0}),
        ("network", {"project_id": "project-b"}),
    ],
)
async def test_changed_authority_after_precheck_is_denied(setup_access, field, update):
    service, repo, intent, ref, _, sink, _, _ = setup_access
    await service.resolve(ref, intent)
    value = getattr(repo.snapshot, field).model_copy(update=update)
    await repo.change(**{field: value})
    with pytest.raises(ServiceError):
        await service.deliver_secret(ref, intent)
    assert not sink.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "update",
    [
        {"revoked": True},
        {"expires_at": 1000.0},
        {"project_id": "project-b"},
        {"purpose": "git.credential"},
    ],
)
async def test_secret_authority_denied(setup_access, update):
    service, repo, intent, ref, _, sink, _, _ = setup_access
    record = repo.snapshot.secrets[0].model_copy(update=update)
    await repo.change(secrets=(record,))
    with pytest.raises(ServiceError):
        await service.deliver_secret(ref, intent)
    assert not sink.calls


@pytest.mark.asyncio
async def test_actor_generation_membership_and_removed_principal(setup_access):
    service, repo, intent, ref, _, sink, _, _ = setup_access
    original = repo.snapshot.actor
    for actor in (
        None,
        dataclasses.replace(original, generation=4),
        dataclasses.replace(original, memberships=()),
    ):
        await repo.change(actor=actor)
        with pytest.raises(ServiceError):
            await service.deliver_secret(ref, intent)
    assert not sink.calls


@pytest.mark.asyncio
async def test_rotation_delivers_current_version_and_old_aad_cannot_replay(setup_access):
    service, repo, intent, ref, vault, sink, _, _ = setup_access
    old = repo.snapshot.secrets[0]
    new = encrypt_secret(vault, old.model_copy(update={"revision": 2}), b"new-fixture-token")
    await repo.change(secrets=(new,))
    receipt = await service.deliver_secret(ref, intent)
    assert receipt.secret_revisions == (("token", 2),)
    assert sink.calls[-1][1] == b"new-fixture-token"
    await repo.change(secrets=(new.model_copy(update={"ciphertext": old.ciphertext}),))
    with pytest.raises(ServiceError, match="invalid_input"):
        await service.deliver_secret(ref, intent)
    assert len(sink.calls) == 1


@pytest.mark.parametrize(
    "update",
    [
        {"owner_id": "other"},
        {"project_id": "project-b"},
        {"secret_id": "other"},
        {"revision": 2},
        {"purpose": "git.credential"},
    ],
)
def test_vault_authentication_covers_every_project_scope(setup_access, update):
    _, repo, _, _, vault, *_ = setup_access
    record = repo.snapshot.secrets[0]
    changed = record.model_copy(update=update)
    with pytest.raises(ServiceError, match="invalid_input"):
        vault.decrypt(project_secret_context(changed), record.ciphertext)


@pytest.mark.asyncio
async def test_expiry_during_decryption_rechecked_before_sink(setup_access):
    service, _, intent, ref, vault, sink, _, clock = setup_access

    class SlowVault:
        def decrypt(self, context, ciphertext):
            result = vault.decrypt(context, ciphertext)
            clock[0] = 2000.0
            return result

    service._vault = SlowVault()
    with pytest.raises(ServiceError):
        await service.deliver_secret(ref, intent)
    assert not sink.calls


@pytest.mark.asyncio
async def test_revoke_wins_when_delivery_waits_at_guard(setup_access):
    service, repo, intent, ref, _, sink, _, _ = setup_access
    await repo.lock.acquire()
    task = asyncio.create_task(service.deliver_secret(ref, intent))
    await asyncio.sleep(0)
    repo.snapshot = dataclasses.replace(
        repo.snapshot, grant=repo.snapshot.grant.model_copy(update={"revoked": True})
    )
    repo.lock.release()
    with pytest.raises(ServiceError):
        await task
    assert not sink.calls


@pytest.mark.asyncio
async def test_sink_and_vault_exception_do_not_expose_secret(setup_access, caplog):
    service, _, intent, ref, _, sink, _, _ = setup_access
    sink.failure = True
    with pytest.raises(ServiceError) as failure:
        await service.deliver_secret(ref, intent)
    assert failure.value.__context__ is None
    assert TOKEN.decode() not in "".join(traceback.format_exception(failure.value))
    assert TOKEN.decode() not in caplog.text
    assert bytes(sink.views[0]) == b"\0" * len(TOKEN)

    class BadVault:
        def decrypt(self, context, ciphertext):
            raise RuntimeError(TOKEN.decode())

    service._vault = BadVault()
    with pytest.raises(ServiceError) as failure:
        await service.deliver_secret(ref, intent)
    assert failure.value.__context__ is None
    assert TOKEN.decode() not in "".join(traceback.format_exception(failure.value))


def test_secret_material_redaction_and_serialization():
    material = SecretMaterial(TOKEN)
    assert TOKEN.decode() not in repr(material)
    with pytest.raises(TypeError):
        pickle.dumps(material)
    with pytest.raises(TypeError):
        json.dumps(material)
    material.erase()


def git_setup(setup_access, *, action="git.push"):
    service, repo, intent, ref, vault, sink, git_sink, clock = setup_access
    operation = {
        "repository": "https://git.example/team/repo.git",
        "ref": "refs/heads/main",
        "action": action,
        "object_id": "a" * 40,
    }
    intent = intent.model_copy(
        update={"action": action, "arguments_json": encoded({"git": operation})}
    )
    ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
    approval = repo.snapshot.approval.model_copy(
        update={"action": action, "arguments_digest": ref.arguments_digest}
    )
    secret = encrypt_secret(
        vault, repo.snapshot.secrets[0].model_copy(update={"purpose": "git.credential"}), TOKEN
    )
    git = GitGrant(
        owner_id="owner",
        project_id="project-a",
        repository=operation["repository"],
        refs=("refs/heads/main",),
        actions=("git.push", "git.clone", "git.commit", "git.diff"),
        identity=GitIdentity(name="Fixture Author", email="fixture@example.test"),
        credential_secret_id="token",
        expires_at=2000.0,
    )
    repo.snapshot = dataclasses.replace(
        repo.snapshot, approval=approval, secrets=(secret,), git=git
    )
    return service, repo, intent, ref, git_sink


@pytest.mark.asyncio
async def test_git_identity_and_credential_channel(setup_access):
    service, _, intent, ref, sink = git_setup(setup_access)
    identity = await service.git_identity(ref, intent)
    assert identity.name == "Fixture Author"
    receipt = await service.deliver_git_credential(
        ref,
        intent,
        b"protocol=https\nhost=git.example\npath=team/repo.git\n\n",
        helper_action="get",
    )
    assert sink.calls[0][1] == b"username=oauth2\npassword=" + TOKEN + b"\n\n"
    assert TOKEN.decode() not in receipt.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("ref", "refs/heads/other"),
        ("repository", "https://git.example/team/other.git"),
        ("action", "git.clone"),
        ("object_id", None),
    ],
)
async def test_git_binding_even_with_matching_approval(setup_access, field, value):
    service, repo, intent, ref, sink = git_setup(setup_access)
    arguments = json.loads(intent.arguments_json)
    arguments["git"][field] = value
    intent = intent.model_copy(update={"arguments_json": encoded(arguments)})
    ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
    repo.snapshot = dataclasses.replace(
        repo.snapshot,
        approval=repo.snapshot.approval.model_copy(
            update={"arguments_digest": ref.arguments_digest}
        ),
    )
    with pytest.raises(ServiceError):
        await service.deliver_git_credential(
            ref,
            intent,
            b"protocol=https\nhost=git.example\npath=team/repo.git\n\n",
            helper_action="get",
        )
    assert not sink.calls


@pytest.mark.asyncio
async def test_general_shell_cannot_get_git_credential(setup_access):
    service, _, intent, ref, sink = git_setup(setup_access)
    with pytest.raises(ServiceError):
        await service.deliver_git_credential(
            ref, intent.model_copy(update={"action": "shell.execute"}), b"", helper_action="get"
        )
    assert not sink.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "update",
    [
        {"revoked": True},
        {"expires_at": 1000.0},
        {"credential_secret_id": "other"},
        {"project_id": "project-b"},
        {"actions": ("git.diff",)},
    ],
)
async def test_git_grant_rechecked_for_every_delivery(setup_access, update):
    service, repo, intent, ref, sink = git_setup(setup_access)
    await service.resolve(ref, intent)
    await repo.change(git=repo.snapshot.git.model_copy(update=update))
    with pytest.raises(ServiceError):
        await service.deliver_git_credential(
            ref,
            intent,
            b"protocol=https\nhost=git.example\npath=team/repo.git\n\n",
            helper_action="get",
        )
    assert not sink.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("helper_action", ["store", "erase", "", "GET"])
async def test_only_helper_get_can_deliver(setup_access, helper_action):
    service, _, intent, ref, sink = git_setup(setup_access)
    with pytest.raises(ServiceError):
        await service.deliver_git_credential(
            ref,
            intent,
            b"protocol=https\nhost=git.example\npath=team/repo.git\n\n",
            helper_action=helper_action,
        )
    assert not sink.calls


@pytest.mark.asyncio
async def test_missing_and_duplicate_secret_references_fail_closed(setup_access):
    service, repo, intent, ref, _, sink, _, _ = setup_access
    for records in ((), (repo.snapshot.secrets[0], repo.snapshot.secrets[0])):
        await repo.change(secrets=records)
        with pytest.raises(ServiceError):
            await service.deliver_secret(ref, intent)
    assert not sink.calls


def test_command_adapter_hash_binds_actual_command(setup_access):
    _, _, _, ref, *_ = setup_access
    request = CommandRequest(
        operation_id="command-op",
        environment={
            "owner_id": "owner",
            "project_id": "project-a",
            "environment_id": "env",
            "generation": 1,
            "state": "ready",
        },
        ownership={"owner_id": "owner", "session_id": "session", "run_id": "run"},
        authorization=ref,
        command="git diff",
        cwd="repo",
    )
    intent = command_intent(request, workspace_id="workspace")
    for field, value in (
        ("command", "git push"),
        ("cwd", "other"),
        ("pty", True),
        ("timeout_seconds", 120.0),
        ("lifecycle", "project_service"),
    ):
        changed = command_intent(
            request.model_copy(update={field: value}), workspace_id="workspace"
        )
        assert changed.digest(ref) != intent.digest(ref)
