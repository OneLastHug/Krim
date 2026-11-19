"""Real vault/SQLite delayed release; native Git cache uses fixture tokens only."""

import asyncio
import os
import sqlite3
import subprocess
import time
from pathlib import Path

import pytest

from krim_agent.contracts import ServiceError, encoded
from krim_agent.project_access.git import GitGrant, GitIdentity, GitOperation
from krim_agent.project_access.git_credentials import NativeGitCredentialCache, helper_request
from krim_agent.project_access.secrets import DeliveryBinding, encrypt_secret
from krim_agent.project_access.service import ProjectAccessService
from krim_agent.project_access.types import NetworkGrant
from tests.coding_agent.test_project_access_storage import Sink, access


async def test_sealed_release_rechecks_real_sqlite_rotation_and_revocation(tmp_path):
    from krim_agent.project_access.delivery import SealedDeliveryQueue

    values = await access(tmp_path / "db.sqlite")
    store, _, repo, _, sink, ref, intent, secret, _, vault, _ = values
    queue = SealedDeliveryQueue(vault)
    service = ProjectAccessService(
        repo, vault, secret_sink=queue, git_sink=queue, delivery_sink=sink
    )
    binding = DeliveryBinding(intent, ref, secret.secret_id, 1)
    try:
        await service.deliver_secret(ref, intent)
        rotated = encrypt_secret(vault, secret.model_copy(update={"revision": 2}), b"new-fixture")
        await repo.put(rotated, expected_revision=1)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.release_secret(binding, queue)
        assert sink.values == []
        queue.revoke(intent.owner_id, intent.project_id)
        await service.deliver_secret(ref, intent)
        current = DeliveryBinding(intent, ref, secret.secret_id, 2)
        await service.release_secret(current, queue)
        assert sink.values == [b"new-fixture"]
        with pytest.raises(ServiceError):
            await service.release_secret(current, queue)
        await service.deliver_secret(ref, intent)
        await repo.put(
            rotated.model_copy(update={"revision": 3, "revoked": True}), expected_revision=2
        )
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.release_secret(current, queue)
        assert sink.values == [b"new-fixture"]
    finally:
        queue.close()
        await store.close()


async def test_release_expiry_is_checked_after_queue_decryption(tmp_path):
    from krim_agent.project_access.delivery import SealedDeliveryQueue

    values = await access(tmp_path / "db.sqlite")
    store, _, repo, _, sink, ref, intent, secret, grant, vault, _ = values
    now = [time.time()]

    class SlowOpen:
        def encrypt(self, context, plaintext):
            return vault.encrypt(context, plaintext)

        def decrypt(self, context, ciphertext):
            now[0] = grant.expires_at + 1
            return vault.decrypt(context, ciphertext)

    queue = SealedDeliveryQueue(SlowOpen())
    service = ProjectAccessService(
        repo, vault, secret_sink=queue, git_sink=queue, delivery_sink=sink, clock=lambda: now[0]
    )
    try:
        await service.deliver_secret(ref, intent)
        with pytest.raises(ServiceError):
            await service.release_secret(DeliveryBinding(intent, ref, secret.secret_id, 1), queue)
        assert sink.values == []
    finally:
        queue.close()
        await store.close()


async def test_release_holds_database_fence_at_final_sink(tmp_path):
    from krim_agent.project_access.delivery import SealedDeliveryQueue

    database = tmp_path / "db.sqlite"
    values = await access(database)
    store, _, repo, _, _, ref, intent, secret, _, vault, _ = values
    queue = SealedDeliveryQueue(vault)

    class CheckSink(Sink):
        def deliver(self, binding, material):
            with sqlite3.connect(database, timeout=0.01) as conn:
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    conn.execute(
                        "UPDATE accounts SET authorization_generation=authorization_generation+1"
                    )
            super().deliver(binding, material)

    sink = CheckSink()
    service = ProjectAccessService(
        repo, vault, secret_sink=queue, git_sink=queue, delivery_sink=sink
    )
    try:
        await service.deliver_secret(ref, intent)
        await service.release_secret(DeliveryBinding(intent, ref, secret.secret_id, 1), queue)
        assert sink.values == [b"fixture-secret"]
    finally:
        queue.close()
        await store.close()


async def git_access(path: Path):
    values = await access(path)
    store, _, repo, _, sink, ref, intent, secret, grant, vault, _ = values
    operation = GitOperation(
        repository="https://github.com/fixture/repository",
        ref="refs/heads/main",
        action="git.push",
        object_id="a" * 40,
        expected_old_id="b" * 40,
    )
    intent = intent.model_copy(
        update={
            "action": "git.push",
            "arguments_json": encoded({"git": operation.model_dump(mode="json")}),
        }
    )
    ref = ref.model_copy(update={"network_policy_ref": "git-net", "arguments_digest": "0" * 64})
    ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
    await repo.put(
        grant.model_copy(update={"revision": 2, "actions": ("git.push",)}), expected_revision=1
    )
    secret = encrypt_secret(
        vault,
        secret.model_copy(update={"revision": 2, "purpose": "git.credential"}),
        b"fixture-only-token",
    )
    await repo.put(secret, expected_revision=1)
    await repo.put(
        NetworkGrant(
            owner_id=intent.owner_id,
            project_id=intent.project_id,
            policy_id="git-net",
            revision=1,
            hosts=("github.com",),
            expires_at=time.time() + 300,
        ),
        expected_revision=0,
    )
    await repo.put(
        GitGrant(
            owner_id=intent.owner_id,
            project_id=intent.project_id,
            repository=operation.repository,
            refs=(operation.ref,),
            actions=("git.push",),
            identity=GitIdentity(name="Fixture", email="fixture@example.invalid"),
            credential_secret_id=secret.secret_id,
            expires_at=time.time() + 300,
        ),
        expected_revision=0,
    )
    service = ProjectAccessService(repo, vault, secret_sink=sink, git_sink=sink)
    return store, repo, service, ref, intent, secret, vault, operation


async def test_git_actual_acceptance_rechecks_digest_and_cached_revision(tmp_path):
    store, repo, service, ref, intent, secret, vault, operation = await git_access(
        tmp_path / "db.sqlite"
    )
    binding = DeliveryBinding(intent, ref, secret.secret_id, secret.revision)
    accepted = []
    try:
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.accept_git(ref, intent, accepted.append)
        assert accepted == []
        await service.accept_git(ref, intent, accepted.append, credential_binding=binding)
        assert len(accepted) == 1
        changed = intent.model_copy(
            update={
                "arguments_json": encoded(
                    {
                        "git": operation.model_copy(update={"object_id": "c" * 40}).model_dump(
                            mode="json"
                        )
                    }
                )
            }
        )
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.accept_git(ref, changed, accepted.append, credential_binding=binding)
        rotated = encrypt_secret(
            vault, secret.model_copy(update={"revision": 3}), b"rotated-fixture"
        )
        await repo.put(rotated, expected_revision=2)
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.accept_git(ref, intent, accepted.append, credential_binding=binding)
        assert len(accepted) == 1
    finally:
        await store.close()


async def test_git_spawn_failure_does_not_retain_injected_exception(tmp_path):
    store, _, service, ref, intent, secret, *_ = await git_access(tmp_path / "db.sqlite")
    binding = DeliveryBinding(intent, ref, secret.secret_id, secret.revision)

    def failed_spawn(identity):
        raise RuntimeError("fixture-sensitive-error")

    try:
        with pytest.raises(ServiceError, match="execution_error") as raised:
            await service.accept_git(ref, intent, failed_spawn, credential_binding=binding)
        assert raised.value.__context__ is None
        assert raised.value.__cause__ is None
        assert "fixture-sensitive-error" not in str(raised.value)
    finally:
        await store.close()


@pytest.mark.skipif(os.name != "posix", reason="native Git Unix socket helper")
async def test_native_git_helper_private_channel_and_cleanup(tmp_path, credential_cache_root):
    store, repo, _, ref, intent, secret, vault, operation = await git_access(tmp_path / "db.sqlite")
    root = credential_cache_root
    binding = DeliveryBinding(intent, ref, secret.secret_id, secret.revision)
    cache = NativeGitCredentialCache(root, binding)
    sink = Sink()
    service = ProjectAccessService(repo, vault, secret_sink=sink, git_sink=cache)
    try:
        await service.deliver_git_credential(
            ref, intent, helper_request(operation), helper_action="get"
        )
        await cache.ready()
        from krim_agent.project_access.git_execution import (
            GitExecutionRequest,
            TrustedGitRunner,
        )

        TrustedGitRunner._validate_credentials(
            intent, ref, GitExecutionRequest(git=operation), (cache.helper,), cache.binding
        )
        argv = [
            "/usr/bin/git",
            "-c",
            "credential.helper=",
            "-c",
            f"credential.helper={cache.helper}",
            "-c",
            "credential.useHttpPath=true",
            "credential",
            "fill",
        ]
        result = await asyncio.to_thread(
            subprocess.run,
            argv,
            input=helper_request(operation),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=tmp_path,
            env={
                "PATH": "/usr/bin:/bin",
                "HOME": str(tmp_path),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": "/dev/null",
                "GIT_TERMINAL_PROMPT": "0",
            },
            timeout=5,
        )
        assert result.returncode == 0
        assert b"password=fixture-only-token" in result.stdout
        assert b"fixture-only-token" not in result.stderr
        other = await asyncio.to_thread(
            subprocess.run,
            argv,
            input=b"protocol=https\nhost=github.com\npath=other/repository\n\n",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=tmp_path,
            env={
                "PATH": "/usr/bin:/bin",
                "HOME": str(tmp_path),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": "/dev/null",
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_ASKPASS": "/bin/false",
            },
            timeout=5,
        )
        assert other.returncode != 0
        assert b"fixture-only-token" not in other.stdout + other.stderr
        assert "fixture-only-token" not in repr(cache)
        assert all(
            b"fixture-only-token" not in f.read_bytes() for f in root.rglob("*") if f.is_file()
        )
        await service.accept_git(
            ref, intent, lambda identity: None, credential_binding=cache.binding
        )
    finally:
        cache.close()
        await store.close()
    assert list(root.iterdir()) == []
    with pytest.raises(ServiceError):
        _ = cache.binding
