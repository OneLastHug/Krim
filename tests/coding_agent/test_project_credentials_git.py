"""Native host-only Git execution against isolated local fixtures."""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest

from krim_agent.contracts import ServiceError, encoded
from krim_agent.development.contracts import AuthorizationRef
from krim_agent.project_access.git import GitIdentity, GitOperation
from krim_agent.project_access.git_execution import (
    GitExecutionRequest,
    TrustedGitRunner,
)
from krim_agent.project_access.secrets import DeliveryBinding
from krim_agent.project_access.types import AccessIntent

GIT = Path("/usr/bin/git")
REPOSITORY = "https://git.example/team/repo.git"
REF = "refs/heads/main"


class RecordingAccess:
    def __init__(self) -> None:
        self.calls: list[tuple[AccessIntent, DeliveryBinding | None]] = []
        self.identity = GitIdentity(name="Fixture Author", email="fixture@example.test")

    async def accept_git(self, authorization, intent, accept, *, credential_binding=None):
        self.calls.append((intent, credential_binding))
        accept(self.identity)


class LocalFixtureTransport:
    """Map only the approved logical HTTPS URL to one fixed local fixture."""

    def __init__(self, remote: Path) -> None:
        self.remote = remote
        self.calls: list[tuple[GitOperation, tuple[str, ...]]] = []

    def spawn(
        self,
        operation: GitOperation,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        env: dict[str, str],
        stdin: int,
        stdout: int,
        stderr: int,
    ) -> subprocess.Popen[bytes]:
        assert operation.repository == REPOSITORY
        assert argv.count(REPOSITORY) == 1
        self.calls.append((operation, argv))
        mapped = tuple(str(self.remote) if value == REPOSITORY else value for value in argv)
        env = {**env, "GIT_ALLOW_PROTOCOL": "file"}
        mapped = tuple(
            "protocol.file.allow=always" if value == "protocol.file.allow=never" else value
            for value in mapped
        )
        return subprocess.Popen(
            mapped,
            cwd=str(cwd),
            env=env,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            shell=False,
            close_fds=True,
            start_new_session=True,
        )


def git(*args: str, input: bytes | None = None) -> bytes:
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/tmp",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_AUTHOR_NAME": "Fixture Author",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "Fixture Author",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
        "LC_ALL": "C",
    }
    result = subprocess.run(
        (str(GIT), *args),
        input=input,
        capture_output=True,
        env=environment,
        check=True,
    )
    return result.stdout


def fixture_remote(root: Path) -> tuple[Path, str]:
    remote = root / "fixture-remote.git"
    work = root / "fixture-work"
    git("init", "--bare", str(remote))
    git("init", str(work))
    git("-C", str(work), "config", "user.name", "Fixture Author")
    git("-C", str(work), "config", "user.email", "fixture@example.test")
    (work / "file.txt").write_text("initial\n", encoding="utf-8")
    git("-C", str(work), "add", "file.txt")
    git("-C", str(work), "commit", "-m", "initial")
    git("-C", str(work), "branch", "-M", "main")
    git("-C", str(work), "push", str(remote), "HEAD:refs/heads/main")
    oid = git("--git-dir", str(remote), "rev-parse", REF).decode().strip()
    return remote, oid


def intent_and_request(
    action: str,
    operation: GitOperation,
    *,
    operation_id: str,
    tree_id: str | None = None,
    message: str | None = None,
) -> tuple[AccessIntent, AuthorizationRef, GitExecutionRequest]:
    payload: dict[str, object] = {
        "git": operation.model_dump(mode="json", exclude_none=True),
    }
    if tree_id is not None:
        payload["tree_id"] = tree_id
    if message is not None:
        payload["message"] = message
    intent = AccessIntent(
        owner_id="owner",
        workspace_id="workspace",
        project_id="project",
        operation_id=operation_id,
        action=action,
        arguments_json=encoded(payload),
    )
    authorization = AuthorizationRef(
        actor_id="actor",
        actor_generation=1,
        project_id="project",
        arguments_digest="0" * 64,
    )
    authorization = authorization.model_copy(
        update={"arguments_digest": intent.digest(authorization)}
    )
    return intent, authorization, GitExecutionRequest.model_validate(payload)


@pytest.mark.asyncio
async def test_clone_commit_diff_push_and_stale_lease(tmp_path: Path) -> None:
    os.chmod(tmp_path, 0o700)
    remote, initial = fixture_remote(tmp_path)
    transport = LocalFixtureTransport(remote)
    access = RecordingAccess()
    runner = TrustedGitRunner(tmp_path / "private-root", git_executable=GIT)

    clone_operation = GitOperation(
        repository=REPOSITORY,
        ref=REF,
        action="git.clone",
        object_id=initial,
    )
    intent, authorization, request = intent_and_request(
        "git.clone", clone_operation, operation_id="clone"
    )
    cloned = await runner.execute(
        intent,
        authorization,
        request,
        access,
        transport=transport,
    )
    assert cloned.object_id == initial
    assert cloned.changed is True
    assert transport.calls[0][1].count(REPOSITORY) == 1
    repository = runner._repository_path(intent, clone_operation)
    assert repository.is_dir()
    assert len(access.calls) >= 4  # clone, origin admission, config checks and ref check

    blob = (
        git(
            "--git-dir",
            str(repository),
            "hash-object",
            "-w",
            "--stdin",
            input=b"updated\n",
        )
        .decode()
        .strip()
    )
    tree = (
        git(
            "--git-dir",
            str(repository),
            "mktree",
            input=f"100644 blob {blob}\tfile.txt\n".encode(),
        )
        .decode()
        .strip()
    )

    commit_operation = GitOperation(
        repository=REPOSITORY,
        ref=REF,
        action="git.commit",
        expected_old_id=initial,
    )
    intent, authorization, request = intent_and_request(
        "git.commit",
        commit_operation,
        operation_id="commit",
        tree_id=tree,
        message="host staged commit\n",
    )
    committed = await runner.execute(intent, authorization, request, access)
    assert committed.object_id is not None
    assert committed.object_id != initial
    assert (
        git("--git-dir", str(repository), "rev-parse", REF).decode().strip() == committed.object_id
    )

    diff_operation = GitOperation(
        repository=REPOSITORY,
        ref=REF,
        action="git.diff",
        object_id=committed.object_id,
        expected_old_id=initial,
    )
    intent, authorization, request = intent_and_request(
        "git.diff", diff_operation, operation_id="diff"
    )
    diff = await runner.execute(intent, authorization, request, access)
    assert diff.changed is True

    push_operation = GitOperation(
        repository=REPOSITORY,
        ref=REF,
        action="git.push",
        object_id=committed.object_id,
        expected_old_id=initial,
    )
    intent, authorization, request = intent_and_request(
        "git.push", push_operation, operation_id="push"
    )
    pushed = await runner.execute(
        intent,
        authorization,
        request,
        access,
        transport=transport,
    )
    assert pushed.changed is True
    assert git("--git-dir", str(remote), "rev-parse", REF).decode().strip() == committed.object_id

    stale_intent, stale_authorization, stale_request = intent_and_request(
        "git.push", push_operation, operation_id="stale-push"
    )
    with pytest.raises(ServiceError) as failure:
        await runner.execute(
            stale_intent,
            stale_authorization,
            stale_request,
            access,
            transport=transport,
        )
    assert failure.value.code == "input_revision_conflict"


@pytest.mark.asyncio
async def test_request_is_exactly_intent_bound_and_helpers_need_binding(tmp_path: Path) -> None:
    runner = TrustedGitRunner(tmp_path / "private-root", git_executable=GIT)
    access = RecordingAccess()
    operation = GitOperation(
        repository=REPOSITORY,
        ref=REF,
        action="git.diff",
        object_id="a" * 40,
        expected_old_id="b" * 40,
    )
    intent, authorization, request = intent_and_request("git.diff", operation, operation_id="bound")
    tampered = request.model_copy(update={"message": None})
    # The model itself rejects a non-commit message; mutate the intent instead.
    tampered_intent = intent.model_copy(
        update={"arguments_json": encoded({"git": operation.model_dump(mode="json"), "extra": 1})}
    )
    with pytest.raises(ServiceError) as extra:
        await runner.execute(tampered_intent, authorization, request, access)
    assert extra.value.code == "invalid_input"

    remote_operation = operation.model_copy(
        update={
            "action": "git.clone",
            "object_id": "a" * 40,
            "expected_old_id": None,
        }
    )
    remote_intent, remote_authorization, remote_request = intent_and_request(
        "git.clone", remote_operation, operation_id="credential"
    )
    binding = DeliveryBinding(
        intent=remote_intent,
        authorization=remote_authorization,
        secret_id="git-secret",
        revision=1,
    )
    with pytest.raises(ServiceError) as missing:
        await runner.execute(
            remote_intent,
            remote_authorization,
            remote_request,
            access,
            credential_helper=("cache --socket=/private/cache",),
            transport=LocalFixtureTransport(tmp_path),
        )
    assert missing.value.code == "permission_denied"
    assert tampered is not None
    assert binding.secret_id == "git-secret"


@pytest.mark.asyncio
async def test_malicious_repo_metadata_is_rejected_and_invalidate_kills_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    os.chmod(tmp_path, 0o700)
    remote, initial = fixture_remote(tmp_path)
    transport = LocalFixtureTransport(remote)
    access = RecordingAccess()
    runner = TrustedGitRunner(tmp_path / "private-root", git_executable=GIT)
    operation = GitOperation(
        repository=REPOSITORY,
        ref=REF,
        action="git.clone",
        object_id=initial,
    )
    intent, authorization, request = intent_and_request(
        "git.clone", operation, operation_id="metadata-clone"
    )
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "hostile-global"))
    (tmp_path / "hostile-global").write_text(
        "[alias]\n\tconfig = !touch " + str(tmp_path / "global-hit") + "\n",
        encoding="utf-8",
    )
    await runner.execute(intent, authorization, request, access, transport=transport)
    repository = runner._repository_path(intent, operation)
    with (repository / "config").open("a", encoding="utf-8") as config:
        config.write("\n[include]\n\tpath = /tmp/hostile\n")
    diff_operation = operation.model_copy(
        update={
            "action": "git.diff",
            "object_id": initial,
            "expected_old_id": initial,
        }
    )
    diff_intent, diff_authorization, diff_request = intent_and_request(
        "git.diff", diff_operation, operation_id="metadata-diff"
    )
    with pytest.raises(ServiceError):
        await runner.execute(diff_intent, diff_authorization, diff_request, access)

    marker = tmp_path / "hook-hit"
    hook = repository / "hooks" / "update"
    hook.parent.mkdir(exist_ok=True)
    hook.write_text(f"#!/bin/sh\ntouch {marker}\nexit 0\n", encoding="utf-8")
    hook.chmod(0o700)
    process = await asyncio.to_thread(
        subprocess.Popen,
        ("/usr/bin/sleep", "30"),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    runner._register(intent, process)
    runner.invalidate("owner", "project")
    await asyncio.to_thread(process.wait, 2)
    assert process.returncode is not None
    assert not marker.exists()
