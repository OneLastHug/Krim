"""Git transport: real native smart-HTTP fixture, no external network or secrets."""

import asyncio
import base64
import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import EnvironmentHandle
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.git import GitOperation
from krim_agent.project_access.git_transport import GitHttpBroker, GitNetworkAuthorization
from krim_agent.project_access.types import NetworkGrant, ResolvedAccess

LOGICAL = "https://git.example/team/fixture.git"
REF = "refs/heads/main"
IMAGE = "sha256:" + "a" * 64


def authority():
    return GitNetworkAuthorization(
        ResolvedAccess(
            actor_id="actor",
            actor_generation=1,
            project_id="project",
            operation_id="operation",
            arguments_digest="a" * 64,
            grant_revision=1,
            secret_revisions=(),
            network_revision=1,
        ),
        ProjectEgressPolicy(
            NetworkGrant(
                owner_id="owner",
                project_id="project",
                policy_id="network",
                revision=1,
                hosts=("git.example",),
                expires_at=2**40,
            )
        ),
        EnvironmentHandle(
            owner_id="owner",
            project_id="project",
            environment_id="environment",
            generation=1,
            state="ready",
        ),
        IMAGE,
    )


def git(cwd, *args, data=None):
    result = subprocess.run(
        ["/usr/bin/git", *args],
        cwd=cwd,
        input=data,
        capture_output=True,
        check=True,
        timeout=15,
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
    )
    return result.stdout.strip()


class SmartHttpFixture:
    def __init__(self, root):
        self.root = root
        self.requests = []

    async def __call__(self, request):
        assert str(request.url).startswith(LOGICAL + "/")
        self.requests.append((request.method, str(request.url), dict(request.headers)))
        env = {
            "PATH": "/usr/bin:/bin",
            "HOME": str(self.root),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_PROJECT_ROOT": str(self.root),
            "GIT_HTTP_EXPORT_ALL": "1",
            "PATH_INFO": "/fixture.git" + request.url.path[len("/team/fixture.git") :],
            "QUERY_STRING": request.url.query.decode(),
            "REQUEST_METHOD": request.method,
            "REMOTE_USER": "fixture",
            "CONTENT_TYPE": request.headers.get("content-type", ""),
            "CONTENT_LENGTH": str(len(request.content)),
        }
        result = await asyncio.to_thread(
            subprocess.run,
            ["/usr/bin/git", "http-backend"],
            input=request.content,
            cwd=self.root,
            env=env,
            capture_output=True,
            check=True,
            timeout=15,
        )
        head, body = result.stdout.split(b"\r\n\r\n", 1)
        headers = {}
        status = 200
        for line in head.split(b"\r\n"):
            key, value = line.decode().split(":", 1)
            if key.lower() == "status":
                status = int(value.split()[0])
            else:
                headers[key] = value.strip()
        return httpx.Response(status, headers=headers, content=body)


async def test_git_broker_native_clone_and_push_smart_http(tmp_path):
    remote = tmp_path / "fixture.git"
    git(tmp_path, "init", "--bare", str(remote))
    git(remote, "config", "http.receivepack", "true")
    tree = git(remote, "mktree", data=b"").decode()
    initial = git(remote, "commit-tree", tree, data=b"base\n").decode()
    git(remote, "update-ref", REF, initial)
    git(remote, "symbolic-ref", "HEAD", REF)
    fixture = SmartHttpFixture(tmp_path)
    current = authority()

    async def resolve():
        return current

    op = GitOperation(repository=LOGICAL, ref=REF, action="git.clone", object_id=initial)
    broker = GitHttpBroker(op, current, resolve, transport=httpx.MockTransport(fixture))
    source = Path(__file__).parents[2] / "src/krim_agent/project_access/git_transport_shim.py"
    shim = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-u",
        str(source),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        limit=34 * 1024 * 1024,
    )
    shim.stdin.write(b'{"max_bytes":16777216}\n')
    await shim.stdin.drain()
    port = json.loads(await shim.stdout.readline())["ready"]

    async def serve():
        while raw := await shim.stdout.readline():
            try:
                response = await broker.request(json.loads(raw))
            except Exception as exc:
                print(type(exc).__name__, str(exc), flush=True)
                raise
            shim.stdin.write(json.dumps(response).encode() + b"\n")
            await shim.stdin.drain()

    server = asyncio.create_task(serve())
    checkout = tmp_path / "checkout.git"
    try:
        await asyncio.to_thread(
            git,
            tmp_path,
            "-c",
            "protocol.version=0",
            "clone",
            "--bare",
            "--single-branch",
            "--branch",
            "main",
            "--no-tags",
            f"http://127.0.0.1:{port}/repo.git",
            str(checkout),
        )
        assert git(checkout, "rev-parse", REF).decode() == initial
        assert any(
            method == "POST" and url.endswith("/git-upload-pack")
            for method, url, _ in fixture.requests
        )
        new = git(checkout, "commit-tree", tree, "-p", initial, data=b"next\n").decode()
        await broker.close()
        broker = GitHttpBroker(
            GitOperation(
                repository=LOGICAL,
                ref=REF,
                action="git.push",
                object_id=new,
                expected_old_id=initial,
            ),
            current,
            resolve,
            transport=httpx.MockTransport(fixture),
        )
        await asyncio.to_thread(
            git,
            checkout,
            "-c",
            "protocol.version=0",
            "push",
            f"--force-with-lease={REF}:{initial}",
            f"http://127.0.0.1:{port}/repo.git",
            f"{new}:{REF}",
        )
        assert git(remote, "rev-parse", REF).decode() == new
        assert any(
            method == "POST" and url.endswith("/git-receive-pack")
            for method, url, _ in fixture.requests
        )
    finally:
        server.cancel()
        await asyncio.gather(server, return_exceptions=True)
        shim.kill()
        await shim.wait()
        await broker.close()


@pytest.mark.parametrize(
    "method,path,headers",
    [
        ("CONNECT", "git.example:443", {}),
        ("GET", "/other/info/refs?service=git-upload-pack", {}),
        ("POST", "/git-receive-pack", {}),
        ("GET", "/info/refs?service=git-upload-pack", {"authorization": "fixture"}),
    ],
)
async def test_broker_rejects_wrong_route_or_credentials(method, path, headers):
    current = authority()

    async def resolve():
        return current

    async def unexpected(request):
        raise AssertionError("upstream must not be reached")

    broker = GitHttpBroker(
        GitOperation(repository=LOGICAL, ref=REF, action="git.clone", object_id="a" * 40),
        current,
        resolve,
        transport=httpx.MockTransport(unexpected),
    )
    try:
        with pytest.raises(ServiceError):
            await broker.request(
                {
                    "id": 1,
                    "method": method,
                    "path": path,
                    "headers": headers,
                    "body": base64.b64encode(b"").decode(),
                }
            )
    finally:
        await broker.close()
