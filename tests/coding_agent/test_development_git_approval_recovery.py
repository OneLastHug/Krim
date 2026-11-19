"""Real SQLite, RunService, SDK, and native Git approval recovery regressions."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from sqlalchemy import update

from krim_agent.contracts import ApprovalReply, parse_json
from krim_agent.development.configuration import (
    DevelopmentConfiguration,
    DevelopmentGitConfiguration,
)
from krim_agent.development.contracts import (
    EnvironmentHandle,
    OperationIdentity,
    OperationReceipt,
)
from krim_agent.development.ports import EnvironmentRecord
from krim_agent.development.service import DevelopmentService
from krim_agent.project_access.git import GitGrant, GitIdentity, GitOperation
from krim_agent.project_access.git_execution import GitExecutionRequest
from krim_agent.project_access.types import ProjectGrant
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.runtime.sdk_loop import DevelopmentSDKLoop
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.sessions.types import TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.development import DevelopmentStore
from krim_agent.storage.models import sessions as session_rows
from krim_agent.storage.project_access import SQLiteAccessRepository
from krim_agent.tools.executor import ToolExecutor
from tests.coding_agent.test_development_context_v3 import (
    development_session as development_session,
)

REPOSITORY = "https://git.example/team/recovery.git"
REF = "refs/heads/main"
IMAGE_ID = "sha256:" + "a" * 64


def native(cwd: Path, *args: str, data: bytes | None = None) -> bytes:
    """Use the same fixed POSIX Git path as TrustedGitRunner; no network."""
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
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_AUTHOR_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
            "LC_ALL": "C",
        },
        check=True,
    )
    return result.stdout.strip()


class GitModel:
    """Deterministic provider: commit tool call, then a final text response."""

    def __init__(self, request: GitExecutionRequest) -> None:
        self.request = request
        self.task_calls = 0

    async def stream(self, messages: Any, tools: Any):
        names = {item["name"] for item in tools}
        if "git_commit" not in names:
            # The real development-context compactor may ask for a no-tool
            # summary first. Keep this local and deterministic too.
            yield ModelText(text="No prior history.")
            return
        self.task_calls += 1
        if self.task_calls == 1:
            yield ModelToolCall(
                id="fixture-commit",
                name="git_commit",
                arguments=self.request.model_dump(mode="json"),
            )
        else:
            yield ModelText(text="Commit task finished.")

    async def close(self) -> None:
        return None


async def build_case(
    fixture: Any, tmp_path: Path, credential_cache_root: Path,
) -> SimpleNamespace:
    """Real development service and SDK loop; only provider input is scripted."""
    store, identities, sandbox, actor, project, session, settings, _policy, _target = fixture
    now = time.time()
    access = SQLiteAccessRepository(store)
    await access.put(
        ProjectGrant(
            owner_id=actor.principal_id,
            workspace_id=session.workspace_id,
            project_id=project["project_id"],
            actor_id=actor.principal_id,
            actor_generation=actor.generation,
            revision=2,
            actions=("shell.execute", "git.commit"),
            requires_approval=True,
            expires_at=now + 600,
        ),
        expected_revision=1,
    )
    git_grant = GitGrant(
        owner_id=actor.principal_id,
        project_id=project["project_id"],
        repository=REPOSITORY,
        refs=(REF,),
        actions=("git.commit",),
        identity=GitIdentity(name="Stored Author", email="stored@example.invalid"),
        expires_at=now + 600,
    )
    await access.put(git_grant, expected_revision=0)

    environment = EnvironmentHandle(
        owner_id=actor.principal_id,
        project_id=project["project_id"],
        environment_id=project["environment_id"],
        generation=1,
        state="ready",
    )
    content = tmp_path / "project-content"
    content.mkdir()
    info = content.stat()
    repositories = DevelopmentStore(store)
    await repositories.environments.register(
        EnvironmentRecord(environment, IMAGE_ID, content, info.st_dev, info.st_ino)
    )

    # This root stays short for the production Unix credential-cache invariant.
    cache_root = credential_cache_root
    configuration = DevelopmentConfiguration(
        image_id=IMAGE_ID,
        broker_environments_root=tmp_path / "broker-environments",
        private_state_root=tmp_path / "private-state",
        git=DevelopmentGitConfiguration(
            repository_root=tmp_path / "git-repositories",
            stage_root=tmp_path / "git-stages",
            cache_root=cache_root,
        ),
    )
    DevelopmentService.prepare_private(configuration)
    development = DevelopmentService(store, identities, sandbox, configuration)

    # Seed only a local bare fixture at the same canonical path the real Git
    # facade derives. The commit under test still runs through TrustedGitRunner.
    source = tmp_path / "seed.git"
    native(tmp_path, "init", "--bare", str(source))
    blob = native(source, "hash-object", "-w", "--stdin", data=b"seed\n").decode()
    tree = native(
        source,
        "mktree",
        data=f"100644 blob {blob}\tseed.txt\n".encode(),
    ).decode()
    base = native(source, "commit-tree", tree, data=b"base\n").decode()
    native(source, "update-ref", REF, base)
    native(source, "symbolic-ref", "HEAD", REF)

    assert development.git is not None
    owner_root = configuration.git.repository_root / actor.principal_id
    project_root = owner_root / project["project_id"]
    owner_root.mkdir(mode=0o700)
    project_root.mkdir(mode=0o700)
    private_repo = project_root / (hashlib.sha256(REPOSITORY.encode("ascii")).hexdigest() + ".git")
    native(
        tmp_path,
        "-c",
        "protocol.file.allow=always",
        "clone",
        "--bare",
        str(source),
        str(private_repo),
    )
    # The checked-in Git boundary validates the configured logical URL; the
    # local clone above supplies fixture objects only and makes no network call.
    native(private_repo, "config", "remote.origin.url", REPOSITORY)

    request = GitExecutionRequest(
        git=GitOperation(
            repository=REPOSITORY,
            ref=REF,
            action="git.commit",
            expected_old_id=base,
        ),
        tree_id=tree,
        message="approved fixture commit",
    )
    provider = GitModel(request)
    run_settings = settings.model_copy(update={
        "run_timeout": 30.0, "tool_timeout": 10.0, "max_request_bytes": 262144,
    })

    async def no_process_cleanup(_run_id: str) -> None:
        # This test starts no project process or container.
        return None

    loop = DevelopmentSDKLoop(
        store,
        provider,
        ToolExecutor(development.registry(), development.policy),
        run_settings,
        resolve_scope=development.scope,
        repositories=development.repositories,
        cleanup_run=no_process_cleanup,
        descriptor={"version": "git-recovery/1"},
        approval_projection=development.approval_projection,
    )
    binding = RunBinding.freeze("target-development", loop, run_settings)
    target = parse_json(binding.descriptor_json)
    async with store.lock, store.engine.begin() as connection:
        await connection.execute(
            update(session_rows)
            .where(session_rows.c.id == session.id)
            .values(
                target=target,
                context_policy=_policy.model_copy(update={"max_bytes": 262144}).model_dump(),
            )
        )
    service = RunService(store, loop, run_settings, target, {"target-development": binding})
    current_session = await session_sql.get_session(store, session.id)
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(
            text="Create the approved Git commit.",
            request_key="git-commit-turn",
            expected_revision=current_session.revision,
        ),
        run_settings,
    )
    run = await service.start_turn(turn.id)
    parked = await service.wait(run.id)
    assert parked.status == "awaiting", parked.code
    return SimpleNamespace(
        store=store,
        access=access,
        development=development,
        provider=provider,
        service=service,
        loop=loop,
        binding=binding,
        target=target,
        run=run,
        environment=environment,
        private_repo=private_repo,
        base=base,
        tree=tree,
        cache_root=cache_root,
        git_grant=git_grant,
    )


def approval_reply(pending: dict[str, Any]) -> ApprovalReply:
    return ApprovalReply(
        request_key="git-approval-reply",
        call_id=pending["call_id"],
        tool_hash=pending["tool_hash"],
        arguments_hash=pending["arguments_hash"],
        allow=True,
    )


async def pending_identity(case: SimpleNamespace) -> tuple[dict[str, Any], OperationIdentity]:
    pending = (await case.service.approvals(case.run.id))[0]
    checkpoint, _stored = await case.store.read_checkpoint(case.run.id)
    matches = [
        OperationIdentity.model_validate(entry["identity"])
        for entry in checkpoint["entries"].values()
        if entry["invocation"]["call_id"] == pending["call_id"]
    ]
    assert len(matches) == 1
    return pending, matches[0]


async def close_case(case: SimpleNamespace, *services: RunService) -> None:
    for service in (case.service, *services):
        await service.close()
    await case.development.close()


async def test_real_git_approval_recovery_commits_once(
    development_session, tmp_path, credential_cache_root,
):
    case = await build_case(development_session, tmp_path, credential_cache_root)
    resumed: RunService | None = None
    try:
        pending, identity = await pending_identity(case)
        assert case.provider.task_calls == 1
        assert native(case.private_repo, "rev-parse", REF).decode() == case.base
        assert native(case.private_repo, "rev-list", "--count", REF) == b"1"
        # SDK approval parks before the operation ledger admits or dispatches it.
        assert await case.development.repositories.operations.lookup(identity) is None

        reply = approval_reply(pending)
        # Fault injection only drops the resume trigger after durable approval.
        # Replaying that same approval through a fresh RunService must recover it.
        case.service._trigger_resume = lambda _run_id, _snapshot: False
        first = await case.service.reply_approval(case.run.id, pending["request_id"], reply)
        assert first["replayed"] is False
        assert (await case.service.get(case.run.id)).status == "awaiting"
        await case.service.close()

        resumed = RunService(
            case.store,
            case.loop,
            case.binding.settings,
            case.target,
            {"target-development": case.binding},
        )
        replies = await asyncio.gather(
            resumed.reply_approval(case.run.id, pending["request_id"], reply),
            resumed.reply_approval(case.run.id, pending["request_id"], reply),
        )
        assert all(item["replayed"] is True for item in replies)
        result = await resumed.wait(case.run.id)
        assert result.status == "completed", result.code
        assert case.provider.task_calls == 2
        receipt = await case.development.repositories.operations.lookup(identity)
        assert receipt is not None and receipt.state == "completed"
        head = native(case.private_repo, "rev-parse", REF).decode()
        assert head != case.base
        assert native(case.private_repo, "rev-list", "--count", REF) == b"2"
        assert native(case.private_repo, "show", "-s", "--format=%s", head) == (
            b"approved fixture commit"
        )

        # A duplicate completed reply is idempotent and cannot make another commit.
        replay = await resumed.reply_approval(case.run.id, pending["request_id"], reply)
        assert replay["replayed"] is True
        assert native(case.private_repo, "rev-parse", REF).decode() == head
        assert native(case.private_repo, "rev-list", "--count", REF) == b"2"
    finally:
        await close_case(case, *(tuple([resumed]) if resumed is not None else ()))


async def test_real_git_approval_revocation_before_resume_has_no_git_dispatch(
    development_session, tmp_path, credential_cache_root, monkeypatch
):
    case = await build_case(development_session, tmp_path, credential_cache_root)
    try:
        pending, identity = await pending_identity(case)
        dispatches = 0
        original = case.development.git.runner.execute

        async def count_dispatch(*args: Any, **kwargs: Any):
            nonlocal dispatches
            dispatches += 1
            return await original(*args, **kwargs)

        monkeypatch.setattr(case.development.git.runner, "execute", count_dispatch)
        await case.access.put(
            case.git_grant.model_copy(update={"revoked": True}),
            expected_revision=1,
        )

        await case.service.reply_approval(
            case.run.id, pending["request_id"], approval_reply(pending)
        )
        result = await case.service.wait(case.run.id)
        assert result.status == "completed", result.code
        assert case.provider.task_calls == 2
        assert dispatches == 0
        receipt = await case.development.repositories.operations.lookup(identity)
        assert receipt is not None and receipt.state == "unknown"
        snapshot = await case.store.snapshot(case.run.id)
        denied = next(
            event for event in reversed(snapshot.events)
            if event.kind == "tool_end" and event.data.get("call_id") == identity.call_id
        )
        assert denied.data["status"] == "denied"
        assert denied.data["code"] == "permission_denied"
        assert native(case.private_repo, "rev-parse", REF).decode() == case.base
        assert native(case.private_repo, "rev-list", "--count", REF) == b"1"
    finally:
        await close_case(case)


async def test_unknown_sqlite_receipt_on_sdk_restore_never_replays_git(
    development_session, tmp_path, credential_cache_root, monkeypatch
):
    case = await build_case(development_session, tmp_path, credential_cache_root)
    try:
        pending, identity = await pending_identity(case)
        dispatches = 0
        original = case.development.git.runner.execute

        async def count_dispatch(*args: Any, **kwargs: Any):
            nonlocal dispatches
            dispatches += 1
            return await original(*args, **kwargs)

        monkeypatch.setattr(case.development.git.runner, "execute", count_dispatch)
        original_set_running = case.store.set_running

        async def inject_unknown_after_real_resume_transition(run_id: str):
            result = await original_set_running(run_id)
            if run_id == case.run.id:
                # Model a prior admitted-but-indeterminate operation in real
                # SQLite after RunService restores the run to running.
                admitted = await case.development.repositories.operations.admit(identity)
                assert admitted.execute and admitted.receipt.state == "accepted"
                await case.development.repositories.operations.save_receipt(
                    OperationReceipt(identity=identity, state="unknown")
                )
            return result

        monkeypatch.setattr(case.store, "set_running", inject_unknown_after_real_resume_transition)
        await case.service.reply_approval(
            case.run.id, pending["request_id"], approval_reply(pending)
        )
        result = await case.service.wait(case.run.id)
        receipt = await case.development.repositories.operations.lookup(identity)
        assert result.status == "completed", result.code
        assert receipt is not None and receipt.state == "unknown"
        assert dispatches == 0
        assert case.provider.task_calls == 2
        assert native(case.private_repo, "rev-parse", REF).decode() == case.base
        assert native(case.private_repo, "rev-list", "--count", REF) == b"1"
    finally:
        await close_case(case)
