"""Increment-specific root, object and workspace boundary regression tests."""

import os

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.project_access.git_execution import TrustedGitRunner
from krim_agent.project_access.git_transport import DockerGitTransport
from krim_agent.project_access.git_transport_worker import promote
from krim_agent.project_access.git_workspace import GitWorkspaceBridge
from tests.coding_agent.test_project_credentials_transport import REF, git
from tests.coding_agent.test_project_credentials_transport_safety import setup_binding
from tests.coding_agent.test_project_credentials_workspace import allow, binding, repository


async def test_same_names_under_other_store_root_are_denied(tmp_path):
    bound, current, request = setup_binding()
    root = tmp_path / "stage"
    root.mkdir(mode=0o700)
    runner = TrustedGitRunner(tmp_path / "private")
    other = TrustedGitRunner(tmp_path / "other")

    async def resolve():
        return current

    lease = DockerGitTransport(bound, stage_root=root, repository_root=runner.root, resolve=resolve)
    wrong = other._repository_path(bound.intent, request.git)
    with pytest.raises(ServiceError):
        await lease.prepare(wrong)
    assert lease.container_id is None and not list(root.iterdir())


async def test_workspace_authority_checks_destination_and_hardlinks(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "file"
    source.write_text("fixture")
    first = repository(tmp_path / "first.git")
    other = repository(tmp_path / "other.git")

    async def verify(current, destination):
        if destination != first:
            raise ServiceError("permission_denied", 403)

    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(binding(workspace), other, verify=verify)
    os.link(source, workspace / "link")
    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(binding(workspace), first, verify=allow)


def test_clone_quarantine_not_promoted_for_wrong_oid_or_extra_ref(tmp_path):
    source = tmp_path / "source.git"
    target = tmp_path / "target.git"
    target.mkdir(mode=0o700)
    info = target.stat()
    git(tmp_path, "init", "--bare", str(source))
    tree = git(source, "mktree", data=b"").decode()
    oid = git(source, "commit-tree", tree, data=b"fixture\n").decode()
    git(source, "update-ref", REF, oid)
    with pytest.raises(ValueError):
        promote(source, target, info.st_dev, info.st_ino, REF, "a" * 40)
    assert not list(target.iterdir())
    git(source, "update-ref", "refs/heads/other", oid)
    with pytest.raises(ValueError):
        promote(source, target, info.st_dev, info.st_ino, REF, oid)
    assert not list(target.iterdir())
