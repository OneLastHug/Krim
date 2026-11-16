"""Security and native-Git tests for the host workspace import bridge."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import EnvironmentHandle
from krim_agent.project_access.git_workspace import (
    GitWorkspaceBridge,
    WorkspaceBinding,
)

GIT = Path("/usr/bin/git")
ORIGIN = "https://git.example/team/repo.git"


def git(*args: str, input_bytes: bytes | None = None) -> bytes:
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/tmp",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "LC_ALL": "C",
        "LANG": "C",
    }
    result = subprocess.run(
        (str(GIT), *args),
        cwd="/",
        env=environment,
        input=input_bytes,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise AssertionError(result.stderr.decode(errors="replace"))
    return result.stdout


def repository(path: Path) -> Path:
    git("init", "--bare", str(path))
    git("--git-dir", str(path), "config", "remote.origin.url", ORIGIN)
    return path


def binding(
    root: Path, *, project_id: str = "project", environment_id: str = "env"
) -> WorkspaceBinding:
    info = root.stat()
    environment = EnvironmentHandle(
        owner_id="owner",
        project_id=project_id,
        environment_id=environment_id,
        generation=1,
        state="ready",
    )
    return WorkspaceBinding(environment, root, info.st_dev, info.st_ino)


async def allow(_binding: WorkspaceBinding, _repository: Path) -> None:
    return None


def tree_entries(repo: Path, tree_id: str) -> list[str]:
    return git("--git-dir", str(repo), "ls-tree", "-r", tree_id).decode().splitlines()


@pytest.mark.asyncio
async def test_imports_contents_modes_and_excludes_git_metadata(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "README.md").write_text("hello\n", encoding="utf-8")
    executable = root / "bin" / "run.sh"
    executable.parent.mkdir()
    executable.write_text("#!/bin/sh\necho safe\n", encoding="utf-8")
    executable.chmod(0o755)
    (root / ".gitattributes").write_text("*.txt filter=host-filter\n", encoding="utf-8")
    git_dir = root / ".git"
    (git_dir / "hooks").mkdir(parents=True)
    (git_dir / "hooks" / "pre-commit").write_text("touch SHOULD_NOT_RUN\n", encoding="utf-8")
    (git_dir / "config").write_text("not an imported file\n", encoding="utf-8")
    repo = repository(tmp_path / "private.git")

    result = await GitWorkspaceBridge().import_tree(binding(root), repo, verify=allow)

    assert result.file_count == 3
    assert result.byte_count == sum(
        len(path.read_bytes()) for path in (root / "README.md", executable, root / ".gitattributes")
    )
    entries = tree_entries(repo, result.tree_id)
    assert any("README.md" in entry and entry.startswith("100644") for entry in entries)
    assert any("bin/run.sh" in entry and entry.startswith("100755") for entry in entries)
    assert any(".gitattributes" in entry for entry in entries)
    assert not any(entry.partition(chr(9))[2].split("/")[0] == ".git" for entry in entries)
    assert git("--git-dir", str(repo), "show", f"{result.tree_id}:README.md") == b"hello\n"
    assert not (tmp_path / "SHOULD_NOT_RUN").exists()


@pytest.mark.asyncio
async def test_repository_filter_config_is_rejected_before_writes(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "file.txt").write_text("content\n", encoding="utf-8")
    repo = repository(tmp_path / "private.git")
    git("--git-dir", str(repo), "config", "filter.host-filter.clean", "touch BAD")
    before = set(repo.rglob("*"))

    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(binding(root), repo, verify=allow)

    assert set(repo.rglob("*")) == before


@pytest.mark.asyncio
async def test_symlink_special_file_and_symlink_ancestor_are_rejected(tmp_path: Path) -> None:
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "escape").symlink_to(outside)
    repo = repository(tmp_path / "private.git")
    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(binding(root), repo, verify=allow)

    (root / "escape").unlink()
    if hasattr(os, "mkfifo"):
        os.mkfifo(root / "pipe")
        with pytest.raises(ServiceError):
            await GitWorkspaceBridge().import_tree(binding(root), repo, verify=allow)
        (root / "pipe").unlink()

    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    aliased_parent = tmp_path / "aliased-parent"
    aliased_parent.symlink_to(real_parent, target_is_directory=True)
    aliased_root = aliased_parent / "workspace"
    aliased_root.mkdir()
    aliased_binding = binding(aliased_root)
    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(aliased_binding, repo, verify=allow)


@pytest.mark.asyncio
async def test_verifier_rejects_cross_project_or_environment_binding(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "file").write_text("data", encoding="utf-8")
    repo = repository(tmp_path / "private.git")
    calls: list[WorkspaceBinding] = []

    async def verify(current: WorkspaceBinding, _repository: Path) -> None:
        calls.append(current)
        if (
            current.environment.project_id != "project"
            or current.environment.environment_id != "env"
        ):
            raise ServiceError("permission_denied", 403)

    wrong = binding(root, project_id="other", environment_id="other-env")
    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(wrong, repo, verify=verify)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "files"),
    [
        ({"max_files": 1}, ("one", "two")),
        ({"max_bytes": 1}, ("one", "two")),
        ({"max_file_bytes": 2}, ("large",)),
    ],
)
async def test_import_limits_are_enforced(
    tmp_path: Path, kwargs: dict[str, int], files: tuple[str, ...]
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    for name in files:
        (root / name).write_text("1234" if name == "large" else "1", encoding="utf-8")
    repo = repository(tmp_path / "private.git")

    with pytest.raises(ServiceError):
        await GitWorkspaceBridge(**kwargs).import_tree(binding(root), repo, verify=allow)


@pytest.mark.asyncio
async def test_depth_and_path_limits_are_enforced(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    (root / "a" / "b").mkdir(parents=True)
    (root / "a" / "b" / "file").write_text("data", encoding="utf-8")
    repo = repository(tmp_path / "private.git")

    with pytest.raises(ServiceError):
        await GitWorkspaceBridge(max_depth=1).import_tree(binding(root), repo, verify=allow)

    long_name = "x" * 240
    current = root
    for _ in range(34):
        current /= long_name[:100]
        current.mkdir()
    (current / "file").write_text("data", encoding="utf-8")
    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(binding(root), repo, verify=allow)


@pytest.mark.asyncio
async def test_source_mutation_after_scan_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    source = root / "file"
    source.write_text("before", encoding="utf-8")
    repo = repository(tmp_path / "private.git")
    calls = 0

    async def mutate_after_scan(_binding: WorkspaceBinding, _repository: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 4:
            source.write_text("after", encoding="utf-8")

    with pytest.raises(ServiceError):
        await GitWorkspaceBridge().import_tree(binding(root), repo, verify=mutate_after_scan)
    assert calls >= 4


@pytest.mark.asyncio
async def test_temporary_index_is_removed_on_success_and_failure(
    tmp_path: Path,
) -> None:
    temp_root = tmp_path / "trusted-temp"
    temp_root.mkdir(mode=0o700)
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "file").write_text("data", encoding="utf-8")
    repo = repository(temp_root / "private.git")
    bridge = GitWorkspaceBridge()

    before = set(temp_root.iterdir())
    await bridge.import_tree(binding(root), repo, verify=allow)
    assert set(temp_root.iterdir()) == before

    calls = 0

    async def deny_during_import(_binding: WorkspaceBinding, _repository: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 7:
            raise ServiceError("permission_denied", 403)

    with pytest.raises(ServiceError):
        await bridge.import_tree(binding(root), repo, verify=deny_during_import)
    assert set(temp_root.iterdir()) == before
