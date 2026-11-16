"""Existing-image network-none Git acceptance; local HTTP backend and fixture key only."""

import asyncio
import base64
import json
import shutil
import tempfile
from pathlib import Path

import httpx

from krim_agent.contracts import ServiceError, encoded
from krim_agent.development.contracts import AuthorizationRef, EnvironmentHandle
from krim_agent.project_access.git import GitOperation, credential_response
from krim_agent.project_access.git_credentials import NativeGitCredentialCache
from krim_agent.project_access.git_execution import GitExecutionRequest, TrustedGitRunner
from krim_agent.project_access.git_transport import (
    DockerGitTransport,
    GitNetworkAuthorization,
    GitTransportBinding,
)
from krim_agent.project_access.git_workspace import GitWorkspaceBridge, WorkspaceBinding
from krim_agent.project_access.secrets import DeliveryBinding, handoff
from krim_agent.project_access.types import AccessIntent
from tests.coding_agent.test_project_credentials_git import RecordingAccess
from tests.coding_agent.test_project_credentials_transport import (
    LOGICAL,
    REF,
    SmartHttpFixture,
    authority,
    git,
)

IMAGE = "sha256:82098beca2bd1c8887f82deb8d2fba13f87da04938463881a5e95b3d346834dc"
ROOT = Path(__file__).resolve().parents[2]
TOKEN = b"local-git-credential-fixture-only"


async def main():
    parent = ROOT / "docs/coding-agent/evidence/D2-increment"
    await asyncio.to_thread(parent.mkdir, parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="d2-git-live-", dir=Path("/home/krim")))
    await asyncio.to_thread(root.chmod, 0o700)
    evidence = {"containers": [], "removed": [], "checks": []}
    try:
        remote = root / "fixture.git"
        git(root, "init", "--bare", str(remote))
        git(remote, "config", "http.receivepack", "true")
        tree = git(remote, "mktree", data=b"").decode()
        initial = git(remote, "commit-tree", tree, data=b"base\n").decode()
        git(remote, "update-ref", REF, initial)
        git(remote, "symbolic-ref", "HEAD", REF)
        fixture = SmartHttpFixture(root)
        expected_header = "Basic " + base64.b64encode(b"oauth2:" + TOKEN).decode()

        async def protected(request):
            assert request.headers.get("Authorization") == expected_header
            return await fixture(request)

        runner = TrustedGitRunner(root / "private")
        stage = root / "stage"
        stage.mkdir(mode=0o700)
        cache_root = root / "credentials"
        cache_root.mkdir(mode=0o700)
        environment = EnvironmentHandle(
            owner_id="owner",
            project_id="project",
            environment_id="environment",
            generation=1,
            state="ready",
        )
        access = RecordingAccess()

        async def execute(action, object_id, expected_old_id=None, revoke=False):
            operation = GitOperation(
                repository=LOGICAL,
                ref=REF,
                action=action,
                object_id=object_id,
                expected_old_id=expected_old_id,
            )
            request = GitExecutionRequest(git=operation)
            intent = AccessIntent(
                owner_id="owner",
                workspace_id="workspace",
                project_id="project",
                operation_id=action.replace(".", "-") + ("-revoke" if revoke else ""),
                action=action,
                arguments_json=encoded(request.model_dump(mode="json")),
            )
            ref = AuthorizationRef(
                actor_id="actor",
                actor_generation=1,
                project_id="project",
                network_policy_ref="network",
                secret_refs=("git-fixture",),
                arguments_digest="a" * 64,
            )
            ref = ref.model_copy(update={"arguments_digest": intent.digest(ref)})
            initial_authority = authority()
            current = GitNetworkAuthorization(
                initial_authority.access.model_copy(
                    update={
                        "operation_id": intent.operation_id,
                        "arguments_digest": ref.arguments_digest,
                        "secret_revisions": (("git-fixture", 1),),
                    }
                ),
                initial_authority.policy,
                environment,
                IMAGE,
            )

            async def resolve():
                return current

            binding = DeliveryBinding(intent, ref, "git-fixture", 1)
            cache = NativeGitCredentialCache(cache_root, binding)
            handoff(cache, binding, credential_response("oauth2", TOKEN))
            await cache.ready()
            lease = DockerGitTransport(
                GitTransportBinding(environment, ref, intent, IMAGE),
                stage_root=stage,
                repository_root=runner.root,
                resolve=resolve,
                cache=cache,
                http_transport=httpx.MockTransport(protected),
            )
            repository = runner._repository_path(intent, operation)
            identifier = None
            try:
                await lease.prepare(repository)
                identifier = lease.container_id
                evidence["containers"].append(identifier)
                if action == "git.clone":
                    probe = (
                        "import socket,json; s=socket.socket(); s.settimeout(.5); "
                        "r=s.connect_ex(('1.1.1.1',443)); "
                        "print(json.dumps({'direct_tcp_denied':r!=0})); s.close()"
                    )
                    out = await lease.runner.run(
                        ("exec", identifier, "/usr/bin/python3", "-I", "-c", probe)
                    )
                    assert out.code == 0 and json.loads(out.stdout)["direct_tcp_denied"]
                    evidence["checks"].append({"direct_tcp_denied": True})
                if revoke:
                    current = GitNetworkAuthorization(
                        current.access.model_copy(update={"actor_generation": 2}),
                        current.policy,
                        environment,
                        IMAGE,
                    )
                    for _ in range(100):
                        if lease._cleanup_complete:
                            break
                        await asyncio.sleep(0.05)
                    assert lease._cleanup_complete
                    with_closed = False
                    try:
                        _ = cache.binding
                    except ServiceError:
                        with_closed = True
                    assert with_closed
                    evidence["checks"].append({"revocation_closed_container_cache": True})
                    return repository
                result = await runner.execute(
                    intent,
                    ref,
                    request,
                    access,
                    transport=lease,
                    credential_helper=(cache.helper,),
                    credential_binding=cache.binding,
                )
                evidence["checks"].append(
                    {"action": action, "result": result.model_dump(mode="json")}
                )
                assert TOKEN.decode() not in json.dumps(result.model_dump(mode="json"))
                for file in lease.directory.rglob("*"):
                    if file.is_file():
                        assert TOKEN not in file.read_bytes()
                evidence["checks"].append({"no_credential_files": True})
                return repository
            except Exception as exc:
                evidence["failure"] = type(exc).__name__
                evidence["worker_status"] = (
                    [json.loads(p.read_text()) for p in lease.directory.glob("*.status")]
                    if lease.directory and lease.directory.exists()
                    else []
                )
                raise
            finally:
                await lease.close()
                assert lease.container_id is None
                if identifier:
                    evidence["removed"].append(identifier)

        repository = await execute("git.clone", initial)
        workspace = root / "workspace"
        workspace.mkdir()
        (workspace / "README.txt").write_text("workspace bridge fixture\n")
        info = workspace.stat()
        source = WorkspaceBinding(environment, workspace, info.st_dev, info.st_ino)

        async def verify_source(current, destination):
            assert current == source and destination == repository

        imported = await GitWorkspaceBridge().import_tree(source, repository, verify=verify_source)
        new = git(
            repository, "commit-tree", imported.tree_id, "-p", initial, data=b"workspace change\n"
        ).decode()
        await execute("git.push", new, initial)
        assert git(remote, "rev-parse", REF).decode() == new
        assert git(remote, "show", f"{new}:README.txt") == b"workspace bridge fixture"
        evidence["checks"].append({"workspace_tree_pushed": True})
        await execute("git.push", new, new, revoke=True)
        evidence["fixture_requests"] = [(method, url) for method, url, _ in fixture.requests]
        assert not list(stage.iterdir()) and not list(cache_root.iterdir())
        evidence["stage_cache_clean"] = True
    finally:
        shutil.rmtree(root)
        (parent / "live-final.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence))


if __name__ == "__main__":
    asyncio.run(main())
