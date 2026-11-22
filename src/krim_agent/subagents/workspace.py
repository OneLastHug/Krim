"""Isolated write workspaces over the existing project sandbox (plan §9.2).

Two modes: ``shared_read_only`` exposes the source project through side-effect
free reads only; ``isolated_write`` allocates a controlled directory inside the
same sandbox project. Both Git and non-Git sources use a sandbox copy plus a
delivered patch artifact. The file surface deliberately blocks ``.git``, so a
controlled in-sandbox ``git worktree`` fast path needs the approved command
surface and is left to integration; worktrees and copies are therefore not
treated as a security boundary — file permissions, command execution and
network limits keep coming from the sandbox.

Baselines record the source working-tree manifest (which covers the user's
uncommitted changes), the git HEAD when the surface can read it, and the
delegation input version. Never only HEAD. Deliveries never touch the source
project: a patch is produced against the baseline, and only the root's
explicit integration applies it after re-checking that the baseline manifest
is unchanged, with per-file CAS writes. Baseline drift, artifact drift and
mid-application failures keep both sides — the patch artifact stays intact
and the applied/skipped/pending sets are reported separately.
"""

from __future__ import annotations

import base64
import binascii
import difflib
import hashlib
import re
import time
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

from pydantic import Field

from krim_agent.contracts import ServiceError, StrictModel, fingerprint

WorkspaceMode = Literal["shared_read_only", "isolated_write"]

# 项目根下保留给宿主簿记的命名空间；基线与补丁清单一律跳过，避免把任务
# 输入或他人工作区当作用户文件。敏感目录沿用沙箱面已有的过滤语义。
MANAGEMENT_ROOT = ".krim"
SKIP_DIRECTORIES = frozenset({".git", ".krim", "node_modules", ".venv"})
AGENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


@dataclass(frozen=True)
class WorkspaceLimits:
    """Bounded manifests, bounded patch text and bounded workspace copies;
    exceeding a bound marks the result truncated instead of silently
    pretending the snapshot is complete."""

    max_files: int = 2000
    max_depth: int = 12
    max_entries_per_directory: int = 400
    max_patch_file_bytes: int = 65536
    max_patch_bytes: int = 262_144
    max_read_page: int = 65536
    # 副本初始化（沙箱内 bootstrap 命令）的边界；超出即 truncated 并上报。
    max_copy_files: int = 400
    max_copy_file_bytes: int = 1_048_576

    def copy_bounds(self) -> dict[str, int]:
        return {
            "max_files": self.max_copy_files,
            "max_file_bytes": self.max_copy_file_bytes,
        }


DEFAULT_LIMITS = WorkspaceLimits()


class FileEntry(StrictModel):
    path: str = Field(min_length=1, max_length=512)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)


class WorkspaceBaseline(StrictModel):
    """Source snapshot an isolated workspace was cut from.

    ``manifest`` covers tracked and uncommitted files alike; ``head`` is the
    git HEAD commit when readable and ``None`` otherwise (never a reason to
    skip the manifest). ``truncated`` marks bounds exceeded during capture.
    """

    mode: WorkspaceMode
    project_id: str = Field(min_length=1, max_length=36)
    input_version: int = Field(ge=1)
    manifest: tuple[FileEntry, ...]
    head: str | None = None
    truncated: bool = False
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class WorkspaceAssignment(StrictModel):
    """What a spawn hands to its worker and what the root retains.

    ``shared_read_only`` carries no directory; ``isolated_write`` must carry
    the controlled directory and the baseline fingerprint it was cut from.
    """

    mode: WorkspaceMode
    agent_id: str = Field(min_length=1, max_length=64)
    source_project_id: str = Field(min_length=1, max_length=36)
    workspace_project_id: str = Field(min_length=1, max_length=36)
    directory: str | None = Field(default=None, max_length=512)
    baseline_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_version: int = Field(ge=1)


class FileChange(StrictModel):
    """One delivered path. ``content`` carries the full new text for
    applicable changes; ``diff`` is best-effort review material only."""

    path: str = Field(min_length=1, max_length=512)
    kind: Literal["modified", "added", "removed"]
    base_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    result_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    size: int = Field(default=0, ge=0)
    binary: bool = False
    diff: str | None = None
    diff_truncated: bool = False
    content: str | None = None


class WorkspacePatch(StrictModel):
    """Delivered diff artifact plus the exact baseline it applies to."""

    mode: WorkspaceMode = "isolated_write"
    source_project_id: str = Field(min_length=1, max_length=36)
    workspace_project_id: str = Field(min_length=1, max_length=36)
    directory: str = Field(min_length=1, max_length=512)
    input_version: int = Field(ge=1)
    baseline_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    changes: tuple[FileChange, ...]
    truncated: bool = False
    created_at: float = Field(ge=0)
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class IntegrationOutcome(StrictModel):
    """Explicit integration result; failures never overwrite the patch."""

    state: Literal["integrated", "baseline_changed", "artifact_changed", "failed"]
    patch_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    applied: tuple[str, ...] = ()
    skipped: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    code: str | None = None
    post_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class WorkspacePort(Protocol):
    """Sandbox file surface the isolation domain is expressed against.

    Implementations are the host's authorized surface (see
    ``subagents.providers.process.ExecutorSandboxPort``): reads are
    side-effect free sandbox calls, mutations go through the run's own
    approval pipeline. No implementation may expose host paths or run host
    commands. Method failures surface as ``ServiceError``.
    """

    async def read(
        self, project_id: str, path: str, *, offset: int = 0, limit: int = 65536
    ) -> dict[str, Any]: ...

    async def entries(self, project_id: str, path: str, *, offset: int = 0) -> dict[str, Any]: ...

    async def make_directory(self, project_id: str, path: str) -> dict[str, Any]: ...

    async def write(
        self, project_id: str, path: str, content: str, *, expected_sha256: str | None = None
    ) -> dict[str, Any]: ...

    async def remove(
        self, project_id: str, path: str, *, expected_sha256: str
    ) -> dict[str, Any]: ...

    async def git_head(self, project_id: str) -> str | None: ...


def workspace_directory(agent_id: str) -> str:
    """The controlled namespace inside the sandbox project; never a host path."""
    if AGENT_ID_PATTERN.fullmatch(agent_id) is None:
        raise ValueError("invalid agent identity for a workspace directory")
    return f"{MANAGEMENT_ROOT}/subagent-workspaces/{agent_id}"


class SandboxWorkspaceReader:
    """Read-only ``WorkspacePort`` over ``SandboxService`` for spawn-time baselines.

    ``WorkspaceIsolation.assign`` runs before any run exists, so it must not
    mutate: this adapter maps the side-effect free service reads and refuses
    every mutation. Delivery, integration and cleanup run inside the root's
    own run through ``subagents.providers.process.ExecutorSandboxPort``, whose
    writes go through the approval pipeline.
    """

    def __init__(self, service: Any, actor: Any) -> None:
        self._service = service
        self._actor = actor

    async def read(
        self, project_id: str, path: str, *, offset: int = 0, limit: int = 65536
    ) -> dict[str, Any]:
        return cast(dict[str, Any], await self._service.read_file(
            self._actor, project_id, path=path, offset=offset, limit=limit,
        ))

    async def entries(self, project_id: str, path: str, *, offset: int = 0) -> dict[str, Any]:
        return cast(dict[str, Any], await self._service.directory(
            self._actor, project_id, path=path, limit=100, offset=offset,
        ))

    async def make_directory(self, project_id: str, path: str) -> dict[str, Any]:
        raise ServiceError("workspace_contract_required")

    async def write(
        self, project_id: str, path: str, content: str, *, expected_sha256: str | None = None
    ) -> dict[str, Any]:
        raise ServiceError("workspace_contract_required")

    async def remove(
        self, project_id: str, path: str, *, expected_sha256: str
    ) -> dict[str, Any]:
        raise ServiceError("workspace_contract_required")

    async def git_head(self, project_id: str) -> str | None:
        # The sandbox file surface blocks ``.git``; the effective baseline is
        # the working-tree manifest, and the in-sandbox bootstrap reports the
        # realized HEAD of the copy it prepares.
        try:
            head = await self.read(project_id, ".git/HEAD", limit=256)
        except ServiceError:
            return None
        try:
            raw = base64.b64decode(str(head.get("data", "")), validate=True).decode("utf-8")
        except (binascii.Error, ValueError, UnicodeDecodeError):
            return None
        reference = raw.strip()
        if reference.startswith("ref:"):
            reference = reference[4:].strip()
        if (
            len(reference) == 40
            and all(character in "0123456789abcdef" for character in reference)
        ):
            return reference
        return None


def validate_assignment(
    assignment: WorkspaceAssignment | None, *, expected_mode: WorkspaceMode
) -> WorkspaceAssignment | None:
    """Check a task document's workspace claim against the configured mode."""
    if expected_mode == "shared_read_only":
        if assignment is not None and assignment.mode != "shared_read_only":
            raise ServiceError("invalid_input", 422)
        return assignment
    if (
        assignment is None
        or assignment.mode != "isolated_write"
        or not assignment.directory
        or not assignment.baseline_fingerprint
    ):
        raise ServiceError("invalid_input", 422)
    return assignment


def _manifest_fingerprint(
    *,
    mode: str,
    project_id: str,
    input_version: int,
    entries: list[FileEntry],
    head: str | None,
    truncated: bool,
) -> str:
    return fingerprint({
        "mode": mode,
        "project_id": project_id,
        "input_version": input_version,
        "manifest": [entry.model_dump(mode="json") for entry in entries],
        "head": head,
        "truncated": truncated,
    })


async def _walk(
    port: WorkspacePort,
    project_id: str,
    *,
    root: str = "",
    limits: WorkspaceLimits = DEFAULT_LIMITS,
) -> tuple[dict[str, FileEntry], bool]:
    """Bounded recursive manifest; the sandbox filters sensitive names itself.

    Managed/sensitive namespaces (``SKIP_DIRECTORIES``) are excluded by
    definition and do not count as incompleteness; bounds that would hide
    real files set ``truncated`` so an incomplete manifest never looks
    complete.
    """
    manifest: dict[str, FileEntry] = {}
    truncated = False
    pending = [root]
    while pending and not truncated:
        prefix = pending.pop(0)
        offset = 0
        seen = 0
        while True:
            page = await port.entries(project_id, prefix or ".", offset=offset)
            child_prefix = f"{prefix}/" if prefix else ""
            for entry in page.get("entries", []):
                seen += 1
                if seen > limits.max_entries_per_directory or entry.get("type") == "blocked":
                    truncated = True
                    continue
                name = str(entry.get("name", ""))
                path = f"{child_prefix}{name}"
                if entry.get("type") == "directory":
                    if name in SKIP_DIRECTORIES:
                        continue
                    if path.count("/") >= limits.max_depth:
                        truncated = True
                    else:
                        pending.append(path)
                    continue
                if entry.get("type") != "file":
                    continue
                if len(manifest) >= limits.max_files:
                    truncated = True
                    continue
                page_info = await port.read(project_id, path, offset=0, limit=1)
                manifest[path] = FileEntry(
                    path=path, sha256=str(page_info["sha256"]), size=int(page_info["size"])
                )
            next_offset = page.get("next_offset")
            if truncated or next_offset is None:
                break
            offset = int(next_offset)
    return manifest, truncated


async def capture_baseline(
    port: WorkspacePort,
    project_id: str,
    *,
    mode: WorkspaceMode,
    input_version: int,
    limits: WorkspaceLimits = DEFAULT_LIMITS,
) -> WorkspaceBaseline:
    if type(input_version) is not int or input_version < 1:
        raise ValueError("input version must be a positive integer")
    manifest, truncated = await _walk(port, project_id, limits=limits)
    head = await port.git_head(project_id)
    entries = [manifest[path] for path in sorted(manifest)]
    return WorkspaceBaseline(
        mode=mode,
        project_id=project_id,
        input_version=input_version,
        manifest=tuple(entries),
        head=head,
        truncated=truncated,
        fingerprint=_manifest_fingerprint(
            mode=mode,
            project_id=project_id,
            input_version=input_version,
            entries=entries,
            head=head,
            truncated=truncated,
        ),
    )


def baseline_matches(current: WorkspaceBaseline, expected_fingerprint: str) -> bool:
    """Baseline compatibility check for explicit integration."""
    return current.fingerprint == expected_fingerprint


async def _read_bytes(
    port: WorkspacePort, project_id: str, path: str, *, max_bytes: int, limits: WorkspaceLimits
) -> bytes | None:
    """Whole-file read with per-page consistency checks; None when missing."""
    chunks = bytearray()
    expected: str | None = None
    offset = 0
    while True:
        try:
            page = await port.read(
                project_id,
                path,
                offset=offset,
                limit=min(limits.max_read_page, max(1, max_bytes - len(chunks) + 1)),
            )
        except ServiceError as error:
            if error.code == "not_found" and offset == 0:
                return None
            raise
        if expected is None:
            expected = str(page["sha256"])
        elif str(page["sha256"]) != expected:
            raise ServiceError("artifact_conflict")
        try:
            data = base64.b64decode(page["data"], validate=True)
        except (binascii.Error, ValueError):
            raise ServiceError("artifact_conflict") from None
        chunks.extend(data)
        offset += len(data)
        if page.get("eof"):
            if len(chunks) != int(page["size"]) or hashlib.sha256(chunks).hexdigest() != expected:
                raise ServiceError("artifact_conflict")
            return bytes(chunks)
        if not data or len(chunks) > max_bytes:
            raise ServiceError("artifact_limit")


def _unified_diff(path: str, base: bytes | None, result: bytes | None, budget: int) -> str | None:
    if base is None or result is None or budget <= 0:
        return None
    try:
        base_text = base.decode("utf-8")
        result_text = result.decode("utf-8")
    except UnicodeDecodeError:
        return None
    diff = "".join(difflib.unified_diff(
        base_text.splitlines(keepends=True),
        result_text.splitlines(keepends=True),
        fromfile=f"a/{path}",
        tofile=f"b/{path}",
    ))
    if len(diff.encode("utf-8")) > budget:
        return None
    return diff


async def collect_patch(
    port: WorkspacePort,
    assignment: WorkspaceAssignment,
    baseline: WorkspaceBaseline,
    *,
    limits: WorkspaceLimits = DEFAULT_LIMITS,
    now: float | None = None,
) -> WorkspacePatch:
    """Produce the delivered patch artifact; the source project is untouched."""
    if assignment.mode != "isolated_write" or not assignment.directory:
        raise ValueError("patches only come from isolated write workspaces")
    if assignment.baseline_fingerprint != baseline.fingerprint:
        raise ServiceError("input_revision_conflict")
    directory: str = assignment.directory
    source = assignment.source_project_id
    workspace_project = assignment.workspace_project_id
    isolated, truncated = await _walk(port, workspace_project, root=directory, limits=limits)
    base_map = {entry.path: entry for entry in baseline.manifest}
    prefix = f"{directory}/"
    changes: list[FileChange] = []
    diff_budget = limits.max_patch_bytes
    for path in sorted(set(base_map) | {
        relative[len(prefix):] for relative in isolated if relative.startswith(prefix)
    }):
        base = base_map.get(path)
        live = isolated.get(prefix + path)
        if base is not None and live is not None and base.sha256 == live.sha256:
            continue
        kind: Literal["modified", "added", "removed"] = (
            "removed" if live is None else "modified" if base is not None else "added"
        )
        result_sha = live.sha256 if live is not None else None
        size = live.size if live is not None else 0
        binary = False
        diff_truncated = False
        content: str | None = None
        raw: bytes | None = None
        if kind != "removed":
            raw = await _read_bytes(
                port,
                workspace_project,
                prefix + path,
                max_bytes=limits.max_patch_file_bytes,
                limits=limits,
            )
            if raw is None:
                raise ServiceError("facts_corrupt")
            if b"\x00" in raw:
                # NUL 字节按 git 的二进制判定处理：不做文本 diff/应用，
                # 结果显式跳过而不是近似改写。
                binary = True
            else:
                try:
                    content = raw.decode("utf-8")
                except UnicodeDecodeError:
                    binary = True
        base_bytes: bytes | None = None
        if base is not None:
            base_bytes = await _read_bytes(
                port, source, path, max_bytes=limits.max_patch_file_bytes, limits=limits
            )
            if base_bytes is not None and hashlib.sha256(base_bytes).hexdigest() != base.sha256:
                # The source already drifted from the baseline: no honest base
                # text for a review diff, but the change itself is still real.
                base_bytes = None
        diff = _unified_diff(path, base_bytes, b"" if kind == "removed" else raw, diff_budget)
        if diff is None and (kind != "added" or base_bytes is not None) and not binary:
            # No review diff exists although both sides were readable text:
            # the only reasons are base drift or the diff budget.
            diff_truncated = True
        diff_budget -= len((diff or "").encode("utf-8"))
        changes.append(FileChange(
            path=path,
            kind=kind,
            base_sha256=base.sha256 if base is not None else None,
            result_sha256=result_sha,
            size=size,
            binary=binary,
            diff=diff,
            diff_truncated=diff_truncated,
            content=content,
        ))
    created = time.time() if now is None else now
    # 工件身份只覆盖内容与基线：created_at 是展示信息，不参与指纹，
    # 否则整合时的重新收集永远不等于交付时的补丁。
    digest = fingerprint({
        "baseline": baseline.fingerprint,
        "changes": [change.model_dump(mode="json") for change in changes],
        "truncated": truncated,
    })
    return cast(WorkspacePatch, WorkspacePatch(
        mode="isolated_write",
        source_project_id=source,
        workspace_project_id=workspace_project,
        directory=directory,
        input_version=assignment.input_version,
        baseline_fingerprint=baseline.fingerprint,
        changes=tuple(changes),
        truncated=truncated,
        created_at=created,
        fingerprint=digest,
    ))


def _patch_assignment(patch: WorkspacePatch) -> WorkspaceAssignment:
    return WorkspaceAssignment(
        mode="isolated_write",
        agent_id="integration",
        source_project_id=patch.source_project_id,
        workspace_project_id=patch.workspace_project_id,
        directory=patch.directory,
        baseline_fingerprint=patch.baseline_fingerprint,
        input_version=patch.input_version,
    )


async def integrate_patch(
    port: WorkspacePort,
    patch: WorkspacePatch,
    baseline: WorkspaceBaseline,
    *,
    limits: WorkspaceLimits = DEFAULT_LIMITS,
    expected_baseline_fingerprint: str | None = None,
) -> IntegrationOutcome:
    """Root-driven integration: re-check both sides, then apply with CAS.

    The source must still match the delivered baseline fingerprint and the
    workspace must still match the delivered patch fingerprint. Any drift
    keeps both sides untouched apart from the reported state. Text changes
    are applied with the baseline hash as the per-file precondition; binary
    changes are skipped and reported, never approximated.
    """
    if expected_baseline_fingerprint is not None:
        if patch.baseline_fingerprint != expected_baseline_fingerprint:
            raise ServiceError("input_revision_conflict")
    current = await capture_baseline(
        port, patch.source_project_id, mode=patch.mode,
        input_version=patch.input_version, limits=limits,
    )
    if current.fingerprint != patch.baseline_fingerprint:
        return IntegrationOutcome(
            state="baseline_changed",
            patch_fingerprint=patch.fingerprint,
            code="baseline_changed",
        )
    fresh = await collect_patch(port, _patch_assignment(patch), baseline, limits=limits)
    if fresh.fingerprint != patch.fingerprint:
        return IntegrationOutcome(
            state="artifact_changed",
            patch_fingerprint=patch.fingerprint,
            code="patch_artifact_changed",
        )
    applied: list[str] = []
    skipped: list[str] = []
    source = patch.source_project_id
    position = 0
    for change in fresh.changes:
        position += 1
        if change.kind != "removed" and change.content is None:
            skipped.append(change.path)
            continue
        try:
            if change.kind == "removed":
                assert change.base_sha256 is not None
                await port.remove(source, change.path, expected_sha256=change.base_sha256)
            elif change.kind == "added":
                existing = await _read_bytes(port, source, change.path, max_bytes=1, limits=limits)
                if existing is not None:
                    raise ServiceError("input_revision_conflict")
                assert change.content is not None
                await port.write(source, change.path, change.content)
            else:
                assert change.base_sha256 is not None and change.content is not None
                await port.write(
                    source, change.path, change.content, expected_sha256=change.base_sha256
                )
        except ServiceError as error:
            return IntegrationOutcome(
                state="failed",
                patch_fingerprint=patch.fingerprint,
                applied=tuple(applied),
                skipped=tuple(skipped),
                # pending 含失败项本身：未完成的工作集合，补丁工件仍可整批保留
                pending=tuple(item.path for item in fresh.changes[position - 1:]),
                code=error.code,
            )
        applied.append(change.path)
    post: WorkspaceBaseline | None = None
    try:
        post = await capture_baseline(
            port, source, mode=patch.mode,
            input_version=patch.input_version + 1, limits=limits,
        )
    except ServiceError:
        post = None
    return IntegrationOutcome(
        state="integrated",
        patch_fingerprint=patch.fingerprint,
        applied=tuple(applied),
        skipped=tuple(skipped),
        post_fingerprint=post.fingerprint if post is not None else None,
    )


async def discard_workspace(
    port: WorkspacePort,
    assignment: WorkspaceAssignment,
    *,
    limits: WorkspaceLimits = DEFAULT_LIMITS,
) -> int:
    """Remove the delivered files of an isolated workspace; the directory
    shell stays because the sandbox file surface has no directory removal."""
    if assignment.mode != "isolated_write" or not assignment.directory:
        raise ValueError("only isolated write workspaces can be discarded")
    isolated, _truncated = await _walk(
        port, assignment.workspace_project_id, root=assignment.directory, limits=limits
    )
    removed = 0
    for path in sorted(isolated, reverse=True):
        await port.remove(
            assignment.workspace_project_id, path, expected_sha256=isolated[path].sha256
        )
        removed += 1
    return removed


class WorkspaceIsolation:
    """One port plus bounds; allocation, delivery, integration and cleanup.

    ``assign`` is read-only: it captures the baseline (safe to call from the
    spawn-acceptance path, no approval needed) and returns the assignment
    record. Mutations are explicit and belong to a run's authorized surface:
    the isolated directory is created by ``ProcessAgentLoop`` through the
    port (approved like any sandbox write), and ``integrate`` runs inside the
    root's own run context. Delivery and integration are separate explicit
    steps so a crash between them keeps the patch artifact and the baseline
    consistent.
    """

    def __init__(self, port: WorkspacePort, *, limits: WorkspaceLimits | None = None) -> None:
        self._port = port
        self._limits = limits or DEFAULT_LIMITS

    async def assign(
        self,
        mode: WorkspaceMode,
        *,
        source_project_id: str,
        agent_id: str,
        input_version: int,
    ) -> tuple[WorkspaceAssignment, WorkspaceBaseline]:
        if mode == "isolated_write":
            workspace_directory(agent_id)  # validate before any sandbox call
        baseline = await capture_baseline(
            self._port, source_project_id, mode=mode, input_version=input_version,
            limits=self._limits,
        )
        directory = workspace_directory(agent_id) if mode == "isolated_write" else None
        assignment = WorkspaceAssignment(
            mode=mode,
            agent_id=agent_id,
            source_project_id=source_project_id,
            workspace_project_id=source_project_id,
            directory=directory,
            baseline_fingerprint=baseline.fingerprint,
            input_version=input_version,
        )
        return assignment, baseline

    async def collect(
        self, assignment: WorkspaceAssignment, baseline: WorkspaceBaseline
    ) -> WorkspacePatch:
        return await collect_patch(self._port, assignment, baseline, limits=self._limits)

    async def integrate(
        self,
        patch: WorkspacePatch,
        baseline: WorkspaceBaseline,
        *,
        expected_baseline_fingerprint: str | None = None,
    ) -> IntegrationOutcome:
        return await integrate_patch(
            self._port, patch, baseline, limits=self._limits,
            expected_baseline_fingerprint=expected_baseline_fingerprint,
        )

    async def discard(self, assignment: WorkspaceAssignment) -> int:
        return await discard_workspace(self._port, assignment, limits=self._limits)
