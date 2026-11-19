"""Transport authority, request, and resource cleanup fixtures."""

import asyncio
import base64
import json
import subprocess
from dataclasses import replace

import httpx
import pytest

from krim_agent.contracts import ServiceError, encoded
from krim_agent.development.contracts import AuthorizationRef
from krim_agent.project_access.git import GitOperation
from krim_agent.project_access.git_execution import GitExecutionRequest, TrustedGitRunner
from krim_agent.project_access.git_transport import (
    DockerGitTransport,
    GitHttpBroker,
    GitTransportBinding,
)
from krim_agent.project_access.types import AccessIntent
from tests.coding_agent.test_project_credentials_transport import LOGICAL, REF, authority


def setup_binding(action="git.clone"):
    op = GitOperation(
        repository=LOGICAL,
        ref=REF,
        action=action,
        object_id="a" * 40,
        expected_old_id="b" * 40 if action == "git.push" else None,
    )
    request = GitExecutionRequest(git=op)
    intent = AccessIntent(
        owner_id="owner",
        workspace_id="workspace",
        project_id="project",
        operation_id="operation",
        action=action,
        arguments_json=encoded(request.model_dump(mode="json")),
    )
    ref = AuthorizationRef(
        actor_id="actor",
        actor_generation=1,
        project_id="project",
        network_policy_ref="network",
        arguments_digest="a" * 64,
    )
    ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
    base = authority()
    current = replace(
        base, access=base.access.model_copy(update={"arguments_digest": ref.arguments_digest})
    )
    return GitTransportBinding(current.environment, ref, intent, current.image_id), current, request


async def test_changed_authority_clears_credential_and_never_reaches_upstream():
    binding, current, request = setup_binding()
    original = current
    hits = []

    async def resolve():
        return current

    async def handler(req):
        hits.append(req)
        return httpx.Response(200)

    broker = GitHttpBroker(
        request.git,
        original,
        resolve,
        authorization="Basic fixture",
        transport=httpx.MockTransport(handler),
    )
    current = replace(current, access=current.access.model_copy(update={"actor_generation": 2}))
    try:
        with pytest.raises(ServiceError):
            await broker.request(
                {
                    "id": 1,
                    "method": "GET",
                    "path": "/info/refs?service=git-upload-pack",
                    "headers": {},
                    "body": "",
                }
            )
        assert hits == [] and broker.closed and broker.authorization is None
    finally:
        await broker.close()


async def test_redirect_cannot_receive_or_forward_host_credential():
    _, current, request = setup_binding()
    seen = []

    async def resolve():
        return current

    async def handler(req):
        seen.append(str(req.url))
        assert req.headers["Authorization"] == "Basic fixture"
        return httpx.Response(302, headers={"Location": "https://other.example/elsewhere"})

    broker = GitHttpBroker(
        request.git,
        current,
        resolve,
        authorization="Basic fixture",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(ServiceError):
            await broker.request(
                {
                    "id": 1,
                    "method": "GET",
                    "path": "/info/refs?service=git-upload-pack",
                    "headers": {},
                    "body": "",
                }
            )
        assert len(seen) == 1 and seen[0].startswith(LOGICAL)
    finally:
        await broker.close()


async def test_upload_chunks_recheck_revocation():
    _, current, request = setup_binding("git.push")
    changed = False

    async def resolve():
        if changed:
            raise ServiceError("permission_denied", 403)
        return current

    class InspectTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, req):
            nonlocal changed
            count = 0
            async for _chunk in req.stream:
                count += 1
                changed = True
            raise AssertionError("revocation must stop next upload chunk")

    broker = GitHttpBroker(request.git, current, resolve, transport=InspectTransport())
    try:
        with pytest.raises(ServiceError):
            await broker.request(
                {
                    "id": 1,
                    "method": "POST",
                    "path": "/git-receive-pack",
                    "headers": {"content-type": "application/x-git-receive-pack-request"},
                    "body": base64.b64encode(b"x" * 131072).decode(),
                }
            )
        assert changed
    finally:
        await broker.close()


async def test_spawn_requires_exact_argv_and_does_not_copy_helper(tmp_path, monkeypatch):
    binding, current, request = setup_binding()

    async def resolve():
        return current

    root = tmp_path / "stage"
    root.mkdir(mode=0o700)
    runner = TrustedGitRunner(tmp_path / "private")
    lease = DockerGitTransport(
        binding, stage_root=root, repository_root=runner.root, resolve=resolve
    )
    repo = runner._repository_path(binding.intent, request.git)
    repo.mkdir(mode=0o700)
    lease.directory = root
    lease.container_id = "c" * 64
    lease.port = 12345
    lease._repository = repo
    lease._prepared = True
    expected = (
        "/usr/bin/git",
        "clone",
        "--bare",
        "--single-branch",
        "--no-tags",
        "--no-recurse-submodules",
        "--branch",
        "main",
        LOGICAL,
        str(repo),
    )
    calls = []

    class Process:
        def poll(self):
            return 0

    def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return Process()

    monkeypatch.setattr(subprocess, "Popen", spawn)
    with pytest.raises(ServiceError):
        lease.spawn(
            request.git,
            expected[:-1] + ("/other",),
            cwd=repo.parent,
            env={},
            stdin=-1,
            stdout=-1,
            stderr=-1,
        )
    assert calls == []
    lease.spawn(
        request.git,
        expected,
        cwd=repo.parent,
        env={"TOKEN": "fixture"},
        stdin=-1,
        stdout=-1,
        stderr=-1,
    )
    config = json.loads((root / "worker-0.json").read_text())
    assert "fixture" not in json.dumps(config)
    assert "credential.helper=" in config["argv"]
    assert config["promote"]["object_id"] == request.git.object_id
    with pytest.raises(ServiceError):
        lease.spawn(request.git, expected, cwd=repo.parent, env={}, stdin=-1, stdout=-1, stderr=-1)


async def test_close_during_pending_authority_cannot_create_resource(tmp_path):
    binding, current, request = setup_binding()
    started = asyncio.Event()
    proceed = asyncio.Event()

    async def resolve():
        started.set()
        await proceed.wait()
        return current

    root = tmp_path / "stage"
    root.mkdir(mode=0o700)
    runner = TrustedGitRunner(tmp_path / "private")
    lease = DockerGitTransport(
        binding, stage_root=root, repository_root=runner.root, resolve=resolve
    )
    repo = runner._repository_path(binding.intent, request.git)
    preparation = asyncio.create_task(lease.prepare(repo))
    await started.wait()
    closing = asyncio.create_task(lease.close())
    await asyncio.sleep(0)
    proceed.set()
    with pytest.raises(ServiceError):
        await preparation
    await closing
    assert lease._cleanup_complete and lease.container_id is None and list(root.iterdir()) == []
