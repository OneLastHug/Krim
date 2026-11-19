"""Actual native local Git commands with real SQLite authorization at each spawn.

The transport rewrites exactly one approved logical HTTPS repository to a local
fixture bare remote. This does not exercise an HTTPS provider or network policy.
"""

from __future__ import annotations

import sqlite3
import subprocess
import time
from pathlib import Path

import pytest

from krim_agent.contracts import ServiceError, encoded
from krim_agent.project_access.git import GitGrant, GitIdentity, GitOperation
from tests.coding_agent.test_project_credentials_delivery import git_access


def git(cwd: Path, *args: str, data: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["/usr/bin/git", *args],
        cwd=cwd,
        input=data,
        capture_output=True,
        check=True,
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(cwd),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
        },
    )
    return result.stdout.strip()


async def test_native_git_four_actions_under_real_sqlite_fence(tmp_path):
    from krim_agent.project_access.git_execution import GitExecutionRequest, TrustedGitRunner

    database = tmp_path / "db.sqlite"
    store, repo, service, ref, intent, secret, _, operation = await git_access(database)
    logical = operation.repository
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "--bare", str(remote))
    tree = git(remote, "mktree", data=b"").decode()
    base = git(
        remote,
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit-tree",
        tree,
        data=b"base\n",
    ).decode()
    git(remote, "update-ref", "refs/heads/main", base)
    git(remote, "symbolic-ref", "HEAD", "refs/heads/main")
    async with repo.guard(ref, intent) as snapshot:
        assert snapshot.grant is not None
        grant = snapshot.grant.model_copy(
            update={"revision": 3, "actions": ("git.clone", "git.diff", "git.commit", "git.push")}
        )
    await repo.put(grant, expected_revision=2)
    await repo.put(
        GitGrant(
            owner_id=intent.owner_id,
            project_id=intent.project_id,
            repository=logical,
            refs=("refs/heads/main",),
            actions=("git.clone", "git.diff", "git.commit", "git.push"),
            identity=GitIdentity(name="Fixture", email="fixture@example.invalid"),
            credential_secret_id=None,
            expires_at=time.time() + 300,
        ),
        expected_revision=1,
    )
    accepted = []

    class LocalFixture:
        def spawn(self, actual, argv, *, cwd, env, stdin, stdout, stderr):
            assert actual.repository == logical
            assert logical in argv
            # The actual Popen acceptance is still inside BEGIN IMMEDIATE.
            with sqlite3.connect(database, timeout=0.01) as conn:
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    conn.execute(
                        "UPDATE accounts SET authorization_generation=authorization_generation+1"
                    )
            accepted.append(actual)
            mapped = tuple(str(remote) if part == logical else part for part in argv)
            mapped = tuple(
                "protocol.file.allow=always" if part == "protocol.file.allow=never" else part
                for part in mapped
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

    runner = TrustedGitRunner(tmp_path / "private-git")

    async def execute(action, **fields):
        op = GitOperation(
            repository=logical,
            ref="refs/heads/main",
            action=action,
            object_id=fields.pop("object_id", None),
            expected_old_id=fields.pop("expected_old_id", None),
        )
        request = GitExecutionRequest(git=op, **fields)
        actual_intent = intent.model_copy(
            update={
                "operation_id": action.replace(".", "-"),
                "action": action,
                "arguments_json": encoded(request.model_dump(mode="json")),
            }
        )
        actual_ref = ref.model_copy(update={"arguments_digest": actual_intent.digest(ref)})
        return await runner.execute(
            actual_intent,
            actual_ref,
            request,
            service,
            transport=LocalFixture() if action in ("git.clone", "git.push") else None,
        )

    try:
        await execute("git.clone", object_id=base)
        committed = await execute(
            "git.commit", tree_id=tree, expected_old_id=base, message="fixture next"
        )
        assert committed.object_id != base
        await execute("git.diff", object_id=committed.object_id, expected_old_id=base)
        await execute("git.push", object_id=committed.object_id, expected_old_id=base)
        assert git(remote, "rev-parse", "refs/heads/main").decode() == committed.object_id
        assert [v.action for v in accepted] == ["git.clone", "git.push", "git.push"]
        with pytest.raises(ServiceError):
            await execute("git.push", object_id=base, expected_old_id=base)
        assert git(remote, "rev-parse", "refs/heads/main").decode() == committed.object_id
        # Committed current-actor revocation prevents even a transport spawn.
        with sqlite3.connect(database) as conn:
            conn.execute("UPDATE accounts SET authorization_generation=authorization_generation+1")
            conn.commit()
        before = len(accepted)
        with pytest.raises(ServiceError, match="permission_denied"):
            await execute(
                "git.push", object_id=committed.object_id, expected_old_id=committed.object_id
            )
        assert len(accepted) == before
    finally:
        await store.close()
