"""Scope, helper and actual process lifecycle tests for the native Git runner."""

import asyncio
import os
import subprocess

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.project_access.git import GitOperation
from krim_agent.project_access.git_execution import TrustedGitRunner
from krim_agent.project_access.secrets import DeliveryBinding
from tests.coding_agent.test_project_credentials_git import (
    REF,
    REPOSITORY,
    RecordingAccess,
    intent_and_request,
)


@pytest.mark.parametrize("scope", [".", ".."])
async def test_project_scope_cannot_escape_trusted_root(tmp_path, scope):
    runner = TrustedGitRunner(tmp_path / "private")
    op = GitOperation(
        repository=REPOSITORY,
        ref=REF,
        action="git.diff",
        object_id="a" * 40,
        expected_old_id="b" * 40,
    )
    intent, ref, request = intent_and_request("git.diff", op, operation_id="scope")
    intent = intent.model_copy(update={"project_id": scope})
    with pytest.raises(ServiceError):
        await runner.execute(intent, ref, request, RecordingAccess())


async def test_private_root_cannot_follow_ancestor_symlink(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    with pytest.raises(ServiceError):
        TrustedGitRunner(alias / "private")


async def test_arbitrary_git_helper_program_is_rejected(tmp_path):
    runner = TrustedGitRunner(tmp_path / "private")
    op = GitOperation(repository=REPOSITORY, ref=REF, action="git.clone", object_id="a" * 40)
    intent, ref, request = intent_and_request("git.clone", op, operation_id="helper")
    binding = DeliveryBinding(intent, ref, "secret", 1)
    with pytest.raises(ServiceError):
        await runner.execute(
            intent,
            ref,
            request,
            RecordingAccess(),
            credential_helper=("!touch /fixture-helper",),
            credential_binding=binding,
        )


class SleepingTransport:
    def __init__(self):
        self.process = None
        self.started = asyncio.Event()

    def spawn(self, operation, argv, *, cwd, env, stdin, stdout, stderr):
        self.process = subprocess.Popen(
            ("/usr/bin/sleep", "30"),
            cwd=cwd,
            env=env,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        self.started.set()
        return self.process


@pytest.mark.parametrize("stop", ["cancel", "invalidate", "timeout"])
async def test_actual_accepted_clone_is_reaped_on_stop(tmp_path, stop):
    runner = TrustedGitRunner(tmp_path / "private", timeout=0.1 if stop == "timeout" else 5)
    op = GitOperation(repository=REPOSITORY, ref=REF, action="git.clone", object_id="a" * 40)
    intent, ref, request = intent_and_request("git.clone", op, operation_id="lifecycle")
    transport = SleepingTransport()
    task = asyncio.create_task(
        runner.execute(intent, ref, request, RecordingAccess(), transport=transport)
    )
    await asyncio.wait_for(transport.started.wait(), 2)
    if stop == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        if stop == "invalidate":
            runner.invalidate("other-owner", "project")
            assert transport.process.poll() is None
            runner.invalidate(intent.owner_id, intent.project_id)
        with pytest.raises(ServiceError):
            await task
    assert transport.process.returncode is not None
    with pytest.raises(ProcessLookupError):
        os.kill(transport.process.pid, 0)
    assert runner._active == {}
    assert not list((tmp_path / "private").rglob("*.git"))
