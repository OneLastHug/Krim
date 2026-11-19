"""Facade fixed-vault cache assembly with fixture secrets; no remote provider."""

import asyncio
import subprocess
import time

import pytest

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.project_access.git_credentials import helper_request
from krim_agent.project_access.secrets import encrypt_secret
from krim_agent.project_access.types import SecretRecord
from tests.coding_agent.test_development_git_operations import (
    REF,
    LocalTransport,
    native,
    request,
)
from tests.coding_agent.test_development_git_operations import (
    fixture as fixture,
)


async def test_private_clone_uses_fixed_router_and_cleans_cache(fixture, monkeypatch):
    f = fixture
    secret = encrypt_secret(
        f.vault,
        SecretRecord(
            owner_id=f.actor.principal_id,
            project_id=f.record.environment.project_id,
            secret_id="git-credential",
            revision=1,
            purpose="git.credential",
            ciphertext="pending",
            expires_at=time.time() + 300,
        ),
        b"local-fixture-token",
    )
    await f.access_repository.put(secret, expected_revision=0)
    await f.access_repository.put(
        f.git_grant.model_copy(update={"credential_secret_id": secret.secret_id}),
        expected_revision=1,
    )
    remote = f.root / "remote.git"
    native(f.root, "init", "--bare", str(remote))
    tree = native(remote, "mktree", data=b"").decode()
    oid = native(remote, "commit-tree", tree, data=b"fixture\n").decode()
    native(remote, "update-ref", REF, oid)

    class CheckCredential(LocalTransport):
        async def prepare(self, repository):
            await super().prepare(repository)
            assert self.cache is not None
            operation = request("git.clone", object_id=oid).git
            result = await asyncio.to_thread(
                subprocess.run,
                [
                    "/usr/bin/git",
                    "-c",
                    "credential.helper=",
                    "-c",
                    f"credential.helper={self.cache.helper}",
                    "-c",
                    "credential.useHttpPath=true",
                    "credential",
                    "fill",
                ],
                input=helper_request(operation),
                cwd=f.root,
                capture_output=True,
                timeout=5,
                env={
                    "PATH": "/usr/bin:/bin",
                    "HOME": str(f.root),
                    "GIT_CONFIG_NOSYSTEM": "1",
                    "GIT_CONFIG_GLOBAL": "/dev/null",
                    "GIT_TERMINAL_PROMPT": "0",
                },
            )
            assert result.returncode == 0 and b"local-fixture-token" in result.stdout
            assert b"local-fixture-token" not in result.stderr

    CheckCredential.remote = remote
    CheckCredential.instances = []
    monkeypatch.setattr("krim_agent.development.git_operations.DockerGitTransport", CheckCredential)
    req = request("git.clone", object_id=oid)
    result = await f.facade.clone(f.actor, await f.operation(req), req)
    assert result.object_id == oid and "local-fixture-token" not in result.model_dump_json()
    assert not list(f.cache_root.iterdir()) and not f.facade._active
    with pytest.raises(ServiceError):
        _ = CheckCredential.instances[-1].cache.binding


async def test_project_grant_rotation_during_work_rejects_result(fixture, monkeypatch):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)
    from krim_agent.project_access.git_execution import GitExecutionResult

    async def changed(*args, **kwargs):
        await f.access_repository.put(
            f.grant.model_copy(update={"revision": 2}), expected_revision=1
        )
        return GitExecutionResult(
            operation_id=identity.operation_id, action="git.diff", changed=False
        )

    monkeypatch.setattr(f.facade.runner, "execute", changed)
    with pytest.raises(ServiceError, match="permission_denied"):
        await f.facade.diff(f.actor, identity, req)


async def test_denied_or_corrupted_sdk_checkpoint_is_rejected(fixture):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)
    saved = await f.store.read_checkpoint(f.ownership.run_id)
    checkpoint, _ = saved
    await f.store.write_checkpoint(f.ownership.run_id, checkpoint, "a" * 64)
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await f.facade.diff(f.actor, identity, req)
    checkpoint["entries"][identity.call_id]["decision"] = "deny"
    await f.store.write_checkpoint(f.ownership.run_id, checkpoint, fingerprint(checkpoint))
    with pytest.raises(ServiceError, match="permission_denied"):
        await f.facade.diff(f.actor, identity, req)
    assert (await f.repositories.operations.lookup(identity)).state == "accepted"
