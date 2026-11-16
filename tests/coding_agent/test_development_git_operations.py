"""Concrete facade over real SQLite/Git with fixture-only transport and vault."""

from __future__ import annotations

import asyncio
import subprocess
import time
from pathlib import Path

import pytest
from sqlalchemy import insert, update

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.development.contracts import OperationIdentity, OperationReceipt
from krim_agent.development.git_operations import (
    GitCredentialRouter,
    GitOperations,
    GitTreeRequest,
    derive_intent,
)
from krim_agent.project_access.git import GitGrant, GitIdentity, GitOperation
from krim_agent.project_access.git_execution import GitExecutionRequest
from krim_agent.project_access.service import ProjectAccessService
from krim_agent.project_access.types import NetworkGrant, ProjectGrant
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.service import SandboxService
from krim_agent.storage.project_access import SQLiteAccessRepository
from krim_agent.storage.sandbox_models import sandbox_projects, sandbox_session_bindings
from tests.coding_agent.test_development_storage import prepare

REPO = "https://git.example/team/repository.git"
REF = "refs/heads/main"


class RejectSink:
    def deliver(self, binding, material):
        raise ServiceError("permission_denied", 403)


def native(cwd: Path, *args: str, data: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["/usr/bin/git", *args],
        cwd=cwd,
        input=data,
        capture_output=True,
        timeout=10,
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(cwd),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_AUTHOR_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        },
        check=True,
    )
    return result.stdout.strip()


class Fixture:
    async def open(self, root, *, cache_root=None):
        self.root = root
        self.store, self.repositories, record, command = await prepare(root / "db.sqlite")
        self.identities = await IdentityService.open(self.store)
        self.actor = self.identities.resolve(command.ownership.owner_id)
        self.sandbox = SandboxService(self.store, self.identities, SandboxConfiguration())
        self.record = record
        assert record.network_id is None and record.network_policy_ref is None
        self.network_policy_ref = "git-network"
        self.network_resolutions = []
        self.ownership = command.ownership
        self.workspace = self.actor.memberships[0].workspace_id
        now = time.time()
        async with self.store.lock, self.store.engine.begin() as conn:
            await conn.execute(
                update(sandbox_projects)
                .where(sandbox_projects.c.project_id == record.environment.project_id)
                .values(status="ready")
            )
            await conn.execute(
                insert(sandbox_session_bindings).values(
                    session_id=self.ownership.session_id,
                    principal_id=self.actor.principal_id,
                    project_id=record.environment.project_id,
                    revision=1,
                    created_at=now,
                    updated_at=now,
                )
            )
        self.access_repository = SQLiteAccessRepository(self.store)
        self.grant = ProjectGrant(
            owner_id=self.actor.principal_id,
            workspace_id=self.workspace,
            project_id=record.environment.project_id,
            actor_id=self.actor.principal_id,
            actor_generation=self.actor.generation,
            revision=1,
            actions=("git.clone", "git.diff", "git.commit", "git.push"),
            requires_approval=False,
            expires_at=now + 300,
        )
        await self.access_repository.put(self.grant, expected_revision=0)
        self.git_grant = GitGrant(
            owner_id=self.actor.principal_id,
            project_id=record.environment.project_id,
            repository=REPO,
            refs=(REF,),
            actions=self.grant.actions,
            identity=GitIdentity(name="Stored Author", email="stored@example.invalid"),
            expires_at=now + 300,
        )
        await self.access_repository.put(self.git_grant, expected_revision=0)
        await self.access_repository.put(
            NetworkGrant(
                owner_id=self.actor.principal_id,
                project_id=record.environment.project_id,
                policy_id="git-network",
                revision=1,
                hosts=("git.example",),
                expires_at=now + 300,
            ),
            expected_revision=0,
        )
        self.router = GitCredentialRouter()
        self.vault = AuthenticatedSecretVault(MasterKeyring.create(b"f" * 32))
        self.access = ProjectAccessService(
            self.access_repository, self.vault, secret_sink=RejectSink(), git_sink=self.router
        )
        self.repo_root, self.stage_root = root / "private", root / "stages"
        self.cache_root = cache_root if cache_root is not None else root / "cache"
        for path in (self.repo_root, self.stage_root, self.cache_root):
            path.mkdir(mode=0o700, exist_ok=path == cache_root)
        self.facade = self.make_facade()
        self.count = 0
        self.entries = {}
        return self

    async def resolve_network(self, actor, identity, request):
        assert actor == self.actor and identity.ownership == self.ownership
        assert identity.environment == self.record.environment and request.git.repository == REPO
        self.network_resolutions.append((actor, identity, request))
        return self.network_policy_ref

    def make_facade(self, *, network=True):
        return GitOperations(
            self.store,
            self.repositories,
            self.identities,
            self.sandbox,
            self.access,
            credential_sink=self.router,
            repository_root=self.repo_root,
            stage_root=self.stage_root,
            cache_root=self.cache_root,
            image_id=self.record.image_id,
            resolve_network=self.resolve_network if network else None,
        )

    async def operation(self, request, *, state="accepted", admit=True):
        self.count += 1
        name = (
            "git_tree"
            if isinstance(request, GitTreeRequest)
            else request.git.action.replace(".", "_")
        )
        args = request.model_dump(mode="json")
        tool_hash = "b" * 64
        identity = OperationIdentity(
            operation_id=f"op-{self.count}",
            call_id=f"call-{self.count}",
            environment=self.record.environment,
            ownership=self.ownership,
            request_digest=fingerprint({"name": name, "args": args, "tool_hash": tool_hash}),
        )
        self.entries[identity.call_id] = {
            "identity": identity.model_dump(mode="json"),
            "invocation": {
                "call_id": identity.call_id,
                "run_id": self.ownership.run_id,
                "name": name,
                "arguments_json": encoded(args),
                "tool_hash": tool_hash,
            },
            "decision": "allow",
        }
        checkpoint = {"entries": self.entries}
        await self.store.write_checkpoint(
            self.ownership.run_id, checkpoint, fingerprint(checkpoint)
        )
        if admit:
            admission = await self.repositories.operations.admit(identity)
            assert admission.execute
            if state != "accepted":
                await self.repositories.operations.save_receipt(
                    OperationReceipt(identity=identity, state=state)
                )
        return identity

    async def close(self):
        await self.facade.close()
        await self.sandbox.close()
        await self.identities.close()
        await self.store.close()


@pytest.fixture
async def fixture(tmp_path, credential_cache_root):
    fixture = await Fixture().open(tmp_path, cache_root=credential_cache_root)
    try:
        yield fixture
    finally:
        await fixture.close()


def request(action, *, object_id=None, expected_old_id=None, tree_id=None, message=None):
    return GitExecutionRequest(
        git=GitOperation(
            repository=REPO,
            ref=REF,
            action=action,
            object_id=object_id,
            expected_old_id=expected_old_id,
        ),
        tree_id=tree_id,
        message=message,
    )


class LocalTransport:
    remote = None
    instances = []

    def __init__(self, binding, *, stage_root, repository_root, resolve, cache=None):
        self.binding, self.resolve, self.cache = binding, resolve, cache
        self.closed = False
        self.instances.append(self)

    async def prepare(self, repository):
        current = await self.resolve()
        assert current.access.operation_id == self.binding.intent.operation_id
        assert current.environment == self.binding.environment

    def spawn(self, operation, argv, *, cwd, env, stdin, stdout, stderr):
        assert operation.repository == REPO and argv.count(REPO) == 1
        mapped = tuple(
            str(self.remote)
            if part == REPO
            else "protocol.file.allow=always"
            if part == "protocol.file.allow=never"
            else part
            for part in argv
        )
        return subprocess.Popen(
            mapped,
            cwd=cwd,
            env={**env, "GIT_ALLOW_PROTOCOL": "file"},
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )

    async def close(self):
        self.closed = True


async def test_real_git_five_entrypoints_and_stored_identity(fixture, monkeypatch):
    f = fixture
    remote = f.root / "remote.git"
    native(f.root, "init", "--bare", str(remote))
    tree = native(remote, "mktree", data=b"").decode()
    initial = native(remote, "commit-tree", tree, data=b"base\n").decode()
    native(remote, "update-ref", REF, initial)
    LocalTransport.remote = remote
    LocalTransport.instances = []
    monkeypatch.setattr("krim_agent.development.git_operations.DockerGitTransport", LocalTransport)
    clone = request("git.clone", object_id=initial)
    identity = await f.operation(clone)
    result = await f.facade.clone(f.actor, identity, clone)
    assert result.object_id == initial
    assert (await f.repositories.operations.lookup(identity)).state == "running"
    with pytest.raises(ServiceError, match="sandbox_operation_unknown"):
        await f.facade.clone(f.actor, identity, clone)
    (f.record.content_path / "note.txt").write_text("workspace fixture\n")
    treq = GitTreeRequest(repository=REPO, ref=REF)
    tree_identity = await f.operation(treq)
    imported = await f.facade.tree(f.actor, tree_identity, treq)
    assert imported.file_count == 1 and imported.byte_count == 18
    assert set(imported.model_dump()) == {"tree_id", "file_count", "byte_count"}
    commit = request(
        "git.commit", tree_id=imported.tree_id, expected_old_id=initial, message="fixture commit"
    )
    commit_identity = await f.operation(commit)
    committed = await f.facade.commit(f.actor, commit_identity, commit)
    private = f.facade._repository(await f.facade.scope(f.actor, commit_identity, commit), commit)
    assert (
        native(private, "show", "-s", "--format=%an|%ae", committed.object_id)
        == b"Stored Author|stored@example.invalid"
    )
    diff = request("git.diff", object_id=committed.object_id, expected_old_id=initial)
    assert (await f.facade.diff(f.actor, await f.operation(diff), diff)).changed
    push = request("git.push", object_id=committed.object_id, expected_old_id=initial)
    assert (await f.facade.push(f.actor, await f.operation(push), push)).changed
    assert native(remote, "show", f"{REF}:note.txt") == b"workspace fixture"
    assert all(lease.closed for lease in LocalTransport.instances)
    assert not f.facade._active


@pytest.mark.parametrize("state", ["running", "unknown", "completed", "failed", "cancelled"])
async def test_existing_outcome_never_redispatches(fixture, state, monkeypatch):
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await fixture.operation(req, state=state)

    async def forbidden(*args, **kwargs):
        raise AssertionError("must not dispatch")

    monkeypatch.setattr(fixture.facade.runner, "execute", forbidden)
    with pytest.raises(ServiceError, match="sandbox_operation_unknown"):
        await fixture.facade.diff(fixture.actor, identity, req)


async def test_not_admitted_and_tampered_request_are_denied(fixture, monkeypatch):
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await fixture.operation(req, admit=False)
    with pytest.raises(ServiceError, match="sandbox_operation_unknown"):
        await fixture.facade.diff(fixture.actor, identity, req)
    identity = await fixture.operation(req)
    changed = request("git.diff", object_id="c" * 40, expected_old_id="b" * 40)
    with pytest.raises(ServiceError, match="permission_denied"):
        await fixture.facade.diff(fixture.actor, identity, changed)
    assert (await fixture.repositories.operations.lookup(identity)).state == "accepted"


async def test_approval_projection_binds_full_message_tree_and_principal(fixture):
    req = request("git.commit", tree_id="a" * 40, expected_old_id="b" * 40, message="approved")
    identity = await fixture.operation(req)
    scope = await fixture.facade.scope(fixture.actor, identity, req)
    derived = derive_intent(req, scope, identity, "approval-1")
    assert derived.intent.arguments_json == encoded(req.model_dump(mode="json"))
    assert derived.authorization.arguments_digest == derived.intent.digest(derived.authorization)
    assert derived.approval_projection["actor_id"] == fixture.actor.principal_id
    assert derived.approval_projection["action"] == "git.commit"
    changed = request("git.commit", tree_id="a" * 40, expected_old_id="b" * 40, message="changed")
    with pytest.raises(ServiceError):
        derive_intent(changed, scope, identity, "approval-1")


async def test_two_facades_can_claim_only_once(fixture):
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await fixture.operation(req)
    other = fixture.make_facade()
    try:
        values = await asyncio.gather(
            fixture.facade._claim(identity), other._claim(identity), return_exceptions=True
        )
        assert sum(value is None for value in values) == 1
        assert sum(isinstance(value, ServiceError) for value in values) == 1
    finally:
        await other.close()


async def test_uncertain_claim_is_not_dispatched(fixture, monkeypatch):
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await fixture.operation(req)
    original = fixture.store._eval_write

    async def uncertain(write, confirm):
        await original(write, confirm)
        return await confirm()

    monkeypatch.setattr(fixture.store, "_eval_write", uncertain)
    with pytest.raises(ServiceError, match="sandbox_operation_unknown"):
        await fixture.facade.diff(fixture.actor, identity, req)
    assert (await fixture.repositories.operations.lookup(identity)).state == "running"
