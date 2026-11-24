"""Workspace isolation domain tests (plan §9.2 / 阶段6 workspace.py).

All coverage runs against an in-memory port: no host subprocess, no Docker,
no sandbox deployment. The real authorized surface is ExecutorSandboxPort
(subagents.providers.process); these tests fix the domain contracts — bounded
manifests that include uncommitted changes, patch delivery without touching
the source, explicit integration that re-checks the baseline first, and
conflict/failure paths that keep both sides.
"""

from __future__ import annotations

import base64
import hashlib

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.subagents.workspace import (
    DEFAULT_LIMITS,
    IntegrationOutcome,
    WorkspaceAssignment,
    WorkspaceBaseline,
    WorkspaceIsolation,
    WorkspaceLimits,
    capture_baseline,
    collect_patch,
    discard_workspace,
    integrate_patch,
    validate_assignment,
    workspace_directory,
)


class FakePort:
    """In-memory stand-in for the authorized sandbox file surface.

    Relative paths only, mutations recorded for untouched-source assertions.
    """

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files = dict(files or {})
        self.dirs: set[str] = set()
        self.mutations: list[tuple[str, str]] = []
        self.head: str | None = None
        self.project_id = PROJECT_ID

    async def read(self, project_id: str, path: str, *, offset: int = 0, limit: int = 65536):
        data = self.files.get(path)
        if data is None:
            raise ServiceError("not_found", 404)
        chunk = data[offset:offset + limit]
        return {
            "path": path,
            "offset": offset,
            "size": len(data),
            "eof": offset + len(chunk) >= len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "data": base64.b64encode(chunk).decode(),
        }

    async def entries(self, project_id: str, path: str, *, offset: int = 0):
        prefix = "" if path == "." else path.rstrip("/") + "/"
        names: dict[str, str] = {}
        for candidate in sorted(self.files):
            if candidate.startswith(prefix):
                rest = candidate[len(prefix):]
                if "/" in rest:
                    names.setdefault(rest.split("/", 1)[0], "directory")
                else:
                    names[rest] = "file"
        for directory in sorted(self.dirs):
            if directory.startswith(prefix):
                rest = directory[len(prefix):].rstrip("/")
                if rest and "/" not in rest:
                    names.setdefault(rest, "directory")
        ordered = [
            {"name": name, "type": kind, "size": 0} for name, kind in sorted(names.items())
        ]
        following = offset + 100
        return {
            "path": path,
            "entries": ordered[offset:following],
            "offset": offset,
            "next_offset": following if following < len(ordered) else None,
            "total": len(ordered),
        }

    async def make_directory(self, project_id: str, path: str):
        self.dirs.add(path)
        self.mutations.append(("mkdir", path))
        return {"path": path, "generation": len(self.mutations)}

    async def write(
        self, project_id: str, path: str, content: str, *, expected_sha256: str | None = None
    ):
        if expected_sha256 is not None:
            current = self.files.get(path)
            if current is None or hashlib.sha256(current).hexdigest() != expected_sha256:
                raise ServiceError("input_revision_conflict")
        self.files[path] = content.encode("utf-8")
        self.mutations.append(("write", path))
        return {"path": path, "generation": len(self.mutations)}

    async def remove(self, project_id: str, path: str, *, expected_sha256: str):
        data = self.files.get(path)
        if data is None:
            raise ServiceError("not_found", 404)
        if hashlib.sha256(data).hexdigest() != expected_sha256:
            raise ServiceError("input_revision_conflict")
        del self.files[path]
        self.mutations.append(("remove", path))
        return {"path": path, "generation": len(self.mutations)}

    async def git_head(self, project_id: str):
        return self.head

    def source_mutations(self) -> list[tuple[str, str]]:
        return [item for item in self.mutations if not item[1].startswith(".krim/")]


def seeded_port() -> FakePort:
    return FakePort({
        "src/app.py": b"print('v1')\n",
        "src/lib/util.py": b"def helper():\n    return 1\n",
        "README.md": b"# demo\n",
        ".git/HEAD": b"ref: refs/heads/main\n",
        ".git/refs/heads/main": b"0" * 40 + b"\n",
        "node_modules/pkg/index.js": b"module.exports = 1;\n",
        ".krim/subagent-workspaces/other/delivered.py": b"kept\n",
    })


PROJECT_ID = "11111111-1111-4111-8111-111111111111"


async def test_baseline_covers_uncommitted_files_and_skips_managed_namespaces():
    port = seeded_port()
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )

    paths = {entry.path for entry in baseline.manifest}
    assert {"src/app.py", "src/lib/util.py", "README.md"} <= paths
    assert not any(path.startswith(".git") for path in paths)
    assert not any(path.startswith("node_modules") for path in paths)
    assert not any(path.startswith(".krim/") for path in paths)
    assert baseline.truncated is False
    assert baseline.head is None  # the sandbox file surface blocks .git
    again = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assert again.fingerprint == baseline.fingerprint


async def test_baseline_fingerprint_covers_head_input_version_and_content():
    port = seeded_port()
    port.head = "a" * 40
    first = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    same = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assert same.fingerprint == first.fingerprint

    port.files["README.md"] = b"# changed\n"
    content_changed = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assert content_changed.fingerprint != first.fingerprint

    port.head = "b" * 40
    head_changed = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assert head_changed.head == "b" * 40
    assert head_changed.fingerprint != content_changed.fingerprint

    version_changed = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=2
    )
    assert version_changed.fingerprint != head_changed.fingerprint


async def test_manifest_bounds_mark_truncation_instead_of_lying():
    files = {f"dir{index % 3}/file{index}.txt": b"x" for index in range(10)}
    port = FakePort(files)
    limits = WorkspaceLimits(max_files=5)
    baseline = await capture_baseline(
        port, port.project_id, mode="shared_read_only", input_version=1, limits=limits
    )
    assert baseline.truncated is True
    assert len(baseline.manifest) == 5

    full = await capture_baseline(
        port, port.project_id, mode="shared_read_only", input_version=1
    )
    assert full.truncated is False
    assert len(full.manifest) == 10
    assert full.fingerprint != baseline.fingerprint


def test_workspace_directory_rejects_unsafe_agent_ids():
    assert workspace_directory("agent-1").startswith(".krim/subagent-workspaces/")
    for bad in ("", "../x", "a/b", "x" * 65):
        with pytest.raises(ValueError):
            workspace_directory(bad)


def test_assignment_validation_enforces_mode_contract():
    shared = WorkspaceAssignment(
        mode="shared_read_only",
        agent_id="a1",
        source_project_id=PROJECT_ID,
        workspace_project_id=PROJECT_ID,
        baseline_fingerprint="0" * 64,
        input_version=1,
    )
    isolated = WorkspaceAssignment(
        mode="isolated_write",
        agent_id="a1",
        source_project_id=PROJECT_ID,
        workspace_project_id=PROJECT_ID,
        directory=".krim/subagent-workspaces/a1",
        baseline_fingerprint="0" * 64,
        input_version=1,
    )
    assert validate_assignment(shared, expected_mode="shared_read_only") is shared
    assert validate_assignment(isolated, expected_mode="isolated_write") is isolated
    with pytest.raises(ServiceError):
        validate_assignment(shared, expected_mode="isolated_write")
    with pytest.raises(ServiceError):
        validate_assignment(isolated, expected_mode="shared_read_only")
    missing_directory = isolated.model_copy(update={"directory": None})
    with pytest.raises(ServiceError):
        validate_assignment(missing_directory, expected_mode="isolated_write")
    with pytest.raises(ServiceError):
        validate_assignment(None, expected_mode="isolated_write")


async def test_assign_is_read_only_and_records_isolated_directory():
    port = seeded_port()
    isolation = WorkspaceIsolation(port)
    isolated_assignment, baseline = await isolation.assign(
        "isolated_write", source_project_id=port.project_id, agent_id="worker-1",
        input_version=1,
    )
    assert isolated_assignment.directory == ".krim/subagent-workspaces/worker-1"
    assert isolated_assignment.baseline_fingerprint == baseline.fingerprint
    # assign 只读：目录准备属于运行内的审批路径（ProcessAgentLoop）
    assert port.mutations == []

    shared_assignment, shared_baseline = await isolation.assign(
        "shared_read_only", source_project_id=port.project_id, agent_id="reader-1",
        input_version=1,
    )
    assert shared_assignment.directory is None
    assert shared_assignment.workspace_project_id == port.project_id
    assert shared_baseline.truncated is False
    assert port.mutations == []


def _assignment_for(baseline: WorkspaceBaseline, agent_id: str = "worker-1"):
    return WorkspaceAssignment(
        mode="isolated_write",
        agent_id=agent_id,
        source_project_id=baseline.project_id,
        workspace_project_id=baseline.project_id,
        directory=workspace_directory(agent_id),
        baseline_fingerprint=baseline.fingerprint,
        input_version=baseline.input_version,
    )


def _copy_into_workspace(port: FakePort, directory: str, baseline: WorkspaceBaseline) -> None:
    """The worker's sandbox copy starts as a full copy of the baseline."""
    for entry in baseline.manifest:
        port.files[f"{directory}/{entry.path}"] = port.files[entry.path]


async def test_collect_patch_reports_added_modified_removed_without_touching_source():
    port = seeded_port()
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assignment = _assignment_for(baseline)
    directory = assignment.directory
    assert directory is not None
    _copy_into_workspace(port, directory, baseline)
    # worker edits its sandbox copy only: app.py modified, new.txt added,
    # README.md never copied over (delivered as removed)
    port.files[f"{directory}/src/app.py"] = b"print('v2')\n"
    port.files[f"{directory}/new.txt"] = b"hello\n"
    del port.files[f"{directory}/README.md"]

    patch = await collect_patch(port, assignment, baseline)

    kinds = {change.path: change.kind for change in patch.changes}
    assert kinds == {"src/app.py": "modified", "new.txt": "added", "README.md": "removed"}
    modified = next(change for change in patch.changes if change.path == "src/app.py")
    assert modified.content == "print('v2')\n"
    assert modified.base_sha256 == hashlib.sha256(b"print('v1')\n").hexdigest()
    assert modified.result_sha256 == hashlib.sha256(b"print('v2')\n").hexdigest()
    assert modified.diff is not None and "-print('v1')" in modified.diff
    assert "+print('v2')" in modified.diff
    assert patch.truncated is False
    # 源项目在此期间零改动
    assert port.source_mutations() == []
    assert port.files["src/app.py"] == b"print('v1')\n"


async def test_collect_patch_flags_binary_and_diff_budget():
    files = {"app.py": b"print('v1')\n", "blob.bin": b"\x00\x01\x02"}
    port = FakePort(files)
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assignment = _assignment_for(baseline)
    directory = assignment.directory
    assert directory is not None
    port.files[f"{directory}/app.py"] = b"print('v2')\n"
    port.files[f"{directory}/blob.bin"] = b"\x00\x01\x03"

    patch = await collect_patch(
        port, assignment, baseline, limits=WorkspaceLimits(max_patch_bytes=10)
    )
    by_path = {change.path: change for change in patch.changes}
    assert by_path["blob.bin"].binary is True
    assert by_path["blob.bin"].content is None
    assert by_path["app.py"].diff_truncated is True
    assert by_path["app.py"].content == "print('v2')\n"


async def test_explicit_integration_applies_cas_writes_and_removals():
    port = seeded_port()
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assignment = _assignment_for(baseline)
    directory = assignment.directory
    assert directory is not None
    _copy_into_workspace(port, directory, baseline)
    port.files[f"{directory}/src/app.py"] = b"print('v2')\n"
    del port.files[f"{directory}/README.md"]
    patch = await collect_patch(port, assignment, baseline)

    outcome = await integrate_patch(port, patch, baseline)

    assert isinstance(outcome, IntegrationOutcome)
    assert outcome.state == "integrated"
    assert outcome.skipped == ()
    assert set(outcome.applied) == {"src/app.py", "README.md"}
    assert outcome.post_fingerprint is not None
    assert port.files["src/app.py"] == b"print('v2')\n"
    assert "README.md" not in port.files
    assert port.files["src/lib/util.py"] == b"def helper():\n    return 1\n"
    post = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=2
    )
    assert post.fingerprint == outcome.post_fingerprint
    # 新基线不再包含旧 README，且工作区交付物没有混进源清单
    assert "README.md" not in {entry.path for entry in post.manifest}


async def test_integration_refuses_when_source_drifted_from_baseline():
    port = seeded_port()
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assignment = _assignment_for(baseline)
    directory = assignment.directory
    assert directory is not None
    port.files[f"{directory}/src/app.py"] = b"print('v2')\n"
    patch = await collect_patch(port, assignment, baseline)

    # 用户在整合前改了源文件（未提交改动）
    port.files["src/app.py"] = b"print('user edit')\n"
    outcome = await integrate_patch(port, patch, baseline)

    assert outcome.state == "baseline_changed"
    assert outcome.applied == ()
    assert outcome.code == "baseline_changed"
    assert port.files["src/app.py"] == b"print('user edit')\n"
    assert not any(action == "write" for action, _ in port.source_mutations())


async def test_integration_refuses_when_artifact_drifted_from_patch():
    port = seeded_port()
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assignment = _assignment_for(baseline)
    directory = assignment.directory
    assert directory is not None
    port.files[f"{directory}/src/app.py"] = b"print('v2')\n"
    patch = await collect_patch(port, assignment, baseline)

    port.files[f"{directory}/src/app.py"] = b"print('v3')\n"
    outcome = await integrate_patch(port, patch, baseline)

    assert outcome.state == "artifact_changed"
    assert outcome.applied == ()
    assert outcome.code == "patch_artifact_changed"
    assert port.files["src/app.py"] == b"print('v1')\n"


async def test_failed_integration_reports_applied_and_pending_sets():
    port = FakePort({"a.txt": b"one\n", "b.txt": b"two\n"})
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assignment = _assignment_for(baseline)
    directory = assignment.directory
    assert directory is not None
    port.files[f"{directory}/a.txt"] = b"one-v2\n"
    port.files[f"{directory}/b.txt"] = b"two-v2\n"
    patch = await collect_patch(port, assignment, baseline)

    original_write = port.write

    async def failing_write(project_id, path, content, *, expected_sha256=None):
        if path == "b.txt":
            raise ServiceError("capacity", 429)
        return await original_write(project_id, path, content, expected_sha256=expected_sha256)

    port.write = failing_write  # type: ignore[method-assign]
    outcome = await integrate_patch(port, patch, baseline)

    assert outcome.state == "failed"
    assert outcome.code == "capacity"
    assert outcome.applied == ("a.txt",)
    assert outcome.pending == ("b.txt",)
    assert port.files["a.txt"] == b"one-v2\n"
    assert port.files["b.txt"] == b"two\n"
    # 补丁工件保持可用：修复后按原补丁重放
    port.write = original_write  # type: ignore[method-assign]
    retry = await integrate_patch(port, patch, baseline)
    assert retry.state == "baseline_changed"  # 源已被部分更新，不再等于基线


async def test_discard_removes_delivered_files_only():
    port = seeded_port()
    baseline = await capture_baseline(
        port, port.project_id, mode="isolated_write", input_version=1
    )
    assignment = _assignment_for(baseline, agent_id="worker-9")
    directory = assignment.directory
    assert directory is not None
    port.files[f"{directory}/generated.py"] = b"kept\n"
    removed = await discard_workspace(port, assignment)
    assert removed == 1
    assert f"{directory}/generated.py" not in port.files
    assert port.files["src/app.py"] == b"print('v1')\n"
    with pytest.raises(ValueError):
        await discard_workspace(
            port, assignment.model_copy(update={"mode": "shared_read_only", "directory": None})
        )


async def test_isolation_facade_end_to_end_assignment_collection_integration():
    port = seeded_port()
    isolation = WorkspaceIsolation(port, limits=DEFAULT_LIMITS)
    assignment, baseline = await isolation.assign(
        "isolated_write", source_project_id=port.project_id, agent_id="facade",
        input_version=3,
    )
    assert assignment.input_version == 3
    directory = assignment.directory
    assert directory is not None
    # 目录准备由运行内的审批路径完成；这里模拟其完成后的状态
    await port.make_directory(port.project_id, directory)
    _copy_into_workspace(port, directory, baseline)
    port.files[f"{directory}/README.md"] = b"# rewritten\n"
    patch = await isolation.collect(assignment, baseline)
    outcome = await isolation.integrate(patch, baseline)
    assert outcome.state == "integrated"
    assert port.files["README.md"] == b"# rewritten\n"
    discarded = await isolation.discard(assignment)
    assert discarded == len(baseline.manifest)
