"""单空目录移动的双父级条件锁、原inode撤销和unknown核实。"""

import errno
import os
import sys

import pytest

import krim_agent.workspace_directory as module
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux目录rename")


def move(path: str, target: str, fingerprint: str) -> DirectoryChangeSpec:
    return DirectoryChangeSpec(
        path=path, operation="move", target_path=target, expected_fingerprint=fingerprint
    )


def test_move_preserves_inode_mode_and_undo_restores_original_path(tmp_path):
    source_parent, target_parent = tmp_path / "a", tmp_path / "b"
    source_parent.mkdir()
    target_parent.mkdir()
    source = source_parent / "empty"
    source.mkdir(mode=0o750)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("a/empty")
        plan = writer.prepare(move("a/empty", "b/moved", before.fingerprint))
        result = writer.apply(plan)
        assert result.receipt.status == "done" and not source.exists()
        moved = writer.capture("b/moved")
        assert moved.identity == before.identity and moved.mode == before.mode
        undo = writer.prepare_undo(plan, result)
        undone = writer.apply(undo)
        assert (
            undone.receipt.status == "done"
            and source.is_dir()
            and not (target_parent / "moved").exists()
        )
        assert writer.capture("a/empty").identity == before.identity


@pytest.mark.parametrize("case", ["target", "nonempty", "source_changed", "target_parent"])
def test_move_rechecks_both_parents_and_target_conditions(tmp_path, case):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    source = tmp_path / "a" / "empty"
    source.mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("a/empty")
        plan = writer.prepare(move("a/empty", "b/moved", before.fingerprint))
        if case == "target":
            (tmp_path / "b" / "moved").mkdir()
        elif case == "nonempty":
            (source / "child").write_text("x")
        elif case == "source_changed":
            source.chmod(0o700)
        else:
            (tmp_path / "b").rename(tmp_path / "old")
            (tmp_path / "b").mkdir()
        result = writer.apply(plan)
        assert result.receipt.status == "rejected"
        assert source.exists()


def test_move_unknown_reconciles_without_replaying_rename(tmp_path, monkeypatch):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    source = tmp_path / "a" / "empty"
    source.mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("a/empty")
        plan = writer.prepare(move("a/empty", "b/moved", before.fingerprint))
        original = os.rename
        called = False

        def unknown(source_name, target_name, *args, **kwargs):
            nonlocal called
            called = True
            original(source_name, target_name, *args, **kwargs)
            raise OSError(errno.EIO, "lost rename result")

        monkeypatch.setattr(module.os, "rename", unknown)
        uncertain = writer.apply(plan)
        assert called and uncertain.receipt.status == "uncertain" and uncertain.recovery is not None
        assert not source.exists() and (tmp_path / "b" / "moved").is_dir()
        verified = writer.verify(plan, uncertain.recovery)
        assert verified.receipt.status == "done"
        writer.finalize(plan, uncertain.recovery)


def test_move_readonly_and_cross_root_shapes_fail_closed(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "a" / "empty").mkdir()
    with Workspace(tmp_path) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("a/empty")
        plan = writer.prepare(move("a/empty", "b/moved", before.fingerprint))
        result = writer.apply(plan)
        assert (
            result.receipt.status == "rejected"
            and result.receipt.error_code == "workspace_read_only"
        )
    with pytest.raises(ValueError):
        DirectoryChangeSpec(
            path="a/empty", operation="move", target_path="../escape", expected_fingerprint="0" * 64
        )


async def test_move_target_entry_lock_blocks_competing_directory_create(repository, tmp_path):
    topic = await repository.create_topic("move lock")
    scope = "topic:" + topic.id
    workspace = Workspace(tmp_path, writable=True)
    try:
        (tmp_path / "source").mkdir()
        (tmp_path / "target").mkdir()
        (tmp_path / "source" / "empty").mkdir()
        store = WorkspaceDirectoryStore(repository.database)
        writer = WorkspaceDirectoryWriter(workspace)
        source_snapshot = writer.capture("source/empty")
        moved = writer.prepare(move("source/empty", "target/moved", source_snapshot.fingerprint))
        source_proposal, _ = await store.preview(
            scope, "files", workspace.fingerprint, "move", moved
        )
        source_attempt, created = await store.reserve(
            scope,
            DirectoryChangeAction(
                proposal_id=source_proposal.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="move-action",
            ),
            workspace.fingerprint,
            moved,
        )
        assert created
        target_plan = writer.prepare(
            DirectoryChangeSpec(path="target/moved", operation="create", mode=0o750)
        )
        target_proposal, _ = await store.preview(
            scope, "files", workspace.fingerprint, "target", target_plan
        )
        with pytest.raises(ServiceError) as busy:
            await store.reserve(
                scope,
                DirectoryChangeAction(
                    proposal_id=target_proposal.view.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="target-action",
                ),
                workspace.fingerprint,
                target_plan,
            )
        assert busy.value.code == "workspace_directory_busy"
        assert source_attempt.view.status == "running"
    finally:
        workspace.close()
