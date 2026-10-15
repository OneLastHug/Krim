"""完整Checkpoint材料到真实整树单次发布；物理核心不冒充公开恢复产品。"""

import os
import sys
import time
from dataclasses import replace
from threading import Event
from uuid import uuid4

import pytest

import krim_agent.workspace_directory_restore_writer as module
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="原Linux renameat2")


def versions(root, writer, *, owner=False):
    tree = root / "holder/live"
    (tree / "sub/empty").mkdir(parents=True)
    (tree / "old-only.txt").write_bytes(b"original")
    (tree / "sub/raw.bin").write_bytes(b"\0\xff")
    (tree / "empty.bin").write_bytes(b"")
    (tree / "sub/raw.bin").chmod(0o640)
    tree.chmod(0o750)
    if owner:
        os.chown(tree / "sub/raw.bin", 65534, 65534)
        os.chown(tree, 65534, 65534)
    target = writer.reader.capture("holder/live")
    (tree / "old-only.txt").unlink()
    (tree / "new-only.txt").write_bytes(b"new before")
    (tree / "sub/raw.bin").write_bytes(b"\x80current")
    (tree / "sub/raw.bin").chmod(0o600)
    tree.chmod(0o700)
    before = writer.reader.capture("holder/live")
    return target, before


def prepared(root, writer):
    target, before = versions(root, writer)
    change = writer.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)
    return target, before, change


def test_real_full_material_single_namespace_all_bytes_modes_old_fd_and_zero_second_publish(
    tmp_path, monkeypatch
):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        target, before = versions(tmp_path, writer)
        stages = []
        change = writer.prepare(
            target,
            expected_before=before.fingerprint,
            token=uuid4().hex,
            record_stage=stages.append,
        )
        assert writer.reader.capture(target.root_path) == before
        assert stages == [change.stage]
        private = tmp_path / "holder" / ("._krim-tx-" + change.stage.token)
        assert private.stat().st_mode & 0o777 == 0o700
        with pytest.raises(ServiceError, match="敏感"):
            workspace.read("holder/" + private.name + "/payload/old-only.txt")
        assert "original" not in repr(change) and "\\x80current" not in repr(change)
        old = os.open(tmp_path / "holder/live", os.O_RDONLY | os.O_DIRECTORY)
        calls, intents = [], []
        rename = module.rename_at

        def observe(*args, **kwargs):
            assert (tmp_path / "holder/live/sub/raw.bin").read_bytes() == b"\x80current"
            calls.append((args, kwargs))
            rename(*args, **kwargs)

        monkeypatch.setattr(module, "rename_at", observe)
        try:
            result = writer.apply(change, record_intent=intents.append)
            assert result.receipt.status == "done", result
            assert result.receipt.changed and intents == [change]
            assert len(calls) == 1 and calls[0][1]["exchange"] is True
            after, retained = result.observed_after, result.retained_before
            assert after.entries[0].directory.inode == change.staged.entries[0].directory.inode
            assert retained.entries[0].directory.inode == before.entries[0].directory.inode
            assert result.receipt.after_fingerprint == after.fingerprint != target.fingerprint
            assert result.receipt.retained_fingerprint == retained.fingerprint
            assert (tmp_path / "holder/live/sub/raw.bin").read_bytes() == b"\0\xff"
            assert (tmp_path / "holder/live/sub/raw.bin").stat().st_mode & 0o777 == 0o640
            assert (tmp_path / "holder/live").stat().st_mode & 0o777 == 0o750
            assert (tmp_path / "holder/live/sub/empty").is_dir()
            assert (tmp_path / "holder/live/empty.bin").read_bytes() == b""
            assert (tmp_path / "holder/live/old-only.txt").read_bytes() == b"original"
            assert not (tmp_path / "holder/live/new-only.txt").exists()
            file = os.open("sub/raw.bin", os.O_RDONLY, dir_fd=old)
            try:
                assert os.read(file, 100) == b"\x80current"
            finally:
                os.close(file)
            assert (private / "payload/new-only.txt").read_bytes() == b"new before"
            assert writer.apply(change, record_intent=intents.append).receipt.status == "rejected"
            assert len(calls) == 1 and len(intents) == 1
        finally:
            os.close(old)


@pytest.mark.parametrize(
    "fault",
    ["current_bytes", "current_mode", "added", "identity", "parent", "stage_bytes", "container"],
)
def test_original_current_stage_or_parent_change_refuses_without_namespace(
    tmp_path, monkeypatch, fault
):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        _, _, change = prepared(tmp_path, writer)
        live = tmp_path / "holder/live"
        private = tmp_path / "holder" / ("._krim-tx-" + change.stage.token)
        if fault == "current_bytes":
            (live / "sub/raw.bin").write_bytes(b"raced")
        elif fault == "current_mode":
            live.chmod(0o755)
        elif fault == "added":
            (live / "added.txt").write_text("raced")
        elif fault == "identity":
            (live / "sub/raw.bin").rename(live / "retained.bin")
            (live / "sub/raw.bin").write_bytes(b"\x80current")
        elif fault == "parent":
            (tmp_path / "holder").rename(tmp_path / "old-holder")
            (tmp_path / "holder").mkdir()
        elif fault == "stage_bytes":
            (private / "payload/sub/raw.bin").write_bytes(b"forged")
        else:
            private.chmod(0o755)

        def forbidden(*_args, **_kwargs):
            pytest.fail("原条件变化后不得发布")

        monkeypatch.setattr(module, "rename_at", forbidden)
        effect = writer.apply(change, record_intent=forbidden)
        assert effect.receipt.status == "rejected" and not effect.receipt.changed


@pytest.mark.parametrize(
    "fault",
    [
        "cancel",
        "authority",
        "last_authority",
        "last_authority_mutation",
        "intent_error",
        "intent_mutation",
        "container_swap",
        "timeout",
        "platform",
    ],
)
def test_last_intent_qualification_cancellation_and_container_identity_are_original(
    tmp_path, monkeypatch, fault
):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        _, _, change = prepared(tmp_path, writer)
        abort, calls = Event(), []
        checks = 0

        def authorize():
            nonlocal checks
            checks += 1
            if fault == "authority" and checks == 2:
                raise ServiceError("original_owner_lost", "原owner失权", 409)
            if fault == "last_authority" and checks == 3:
                raise ServiceError("original_owner_lost", "原最后owner失权", 409)
            if fault == "last_authority_mutation" and checks == 3:
                (tmp_path / "holder/live/sub/raw.bin").write_bytes(b"late raced")

        def intent(value):
            calls.append(value)
            if fault == "cancel":
                abort.set()
            elif fault == "intent_error":
                raise OSError("原SQL提交回程未知")
            elif fault == "intent_mutation":
                (tmp_path / "holder/live/sub/raw.bin").write_bytes(b"raced")
            elif fault == "container_swap":
                path = tmp_path / "holder" / ("._krim-tx-" + change.stage.token)
                path.rename(path.with_name("retained-private"))
                path.mkdir(mode=0o700)
            elif fault == "timeout":
                time.sleep(0.07)

        def forbidden(*_args, **_kwargs):
            pytest.fail("最后资格/容器/原意图变化后不得发布")

        monkeypatch.setattr(module, "rename_at", forbidden)
        if fault == "timeout":
            writer.lock_timeout = 0.05
        if fault == "platform":

            def unavailable():
                raise ServiceError("workspace_write_platform", "合成缺原语", 503)

            monkeypatch.setattr(module, "_rename_function", unavailable)
        result = writer.apply(
            change, record_intent=intent, authorize=authorize, cancelled=abort.is_set
        )
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert checks >= 1


@pytest.mark.parametrize("fault", ["lost_response", "fsync", "postcheck"])
def test_namespace_unknown_only_reconciles_original_dual_versions_zero_second_io(
    tmp_path, monkeypatch, fault
):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        _, _, change = prepared(tmp_path, writer)
        rename, calls = module.rename_at, []

        def unknown(*args, **kwargs):
            rename(*args, **kwargs)
            calls.append(1)
            if fault == "lost_response":
                raise OSError("真实namespace后丢回程")
            if fault == "postcheck":
                (tmp_path / "holder/live/sub/raw.bin").write_bytes(b"other mutation")

        monkeypatch.setattr(module, "rename_at", unknown)
        if fault == "fsync":

            def failed(_fd):
                raise OSError("真实发布后fsync故障")

            monkeypatch.setattr(module.os, "fsync", failed)
        effect = writer.apply(change, record_intent=lambda _value: None)
        assert effect.receipt.status == "uncertain" and effect.receipt.changed
        assert effect.receipt.recovery_required and len(calls) == 1

        def forbidden(*_args, **_kwargs):
            pytest.fail("核实不得重新写入或发布")

        monkeypatch.setattr(module, "rename_at", forbidden)
        monkeypatch.setattr(writer, "prepare", forbidden)
        monkeypatch.setattr(writer, "_write_tree", forbidden)
        result = WorkspaceDirectoryRestoreWriter(workspace).reconcile(change)
        assert result.receipt.status == ("uncertain" if fault == "postcheck" else "done")
        assert (tmp_path / "holder/live/sub/raw.bin").read_bytes() == (
            b"other mutation" if fault == "postcheck" else b"\0\xff"
        )


def test_reconcile_original_unpublished_preparation_is_read_only_and_not_success(
    tmp_path, monkeypatch
):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        _, before, change = prepared(tmp_path, writer)

        def forbidden(*_args, **_kwargs):
            pytest.fail("原未发布准备的核实不能产生副作用")

        monkeypatch.setattr(module, "rename_at", forbidden)
        monkeypatch.setattr(writer, "_write_tree", forbidden)
        effect = writer.reconcile(change)
        assert effect.receipt.status == "rejected" and not effect.receipt.changed
        assert writer.reader.capture(change.stage.root_path) == before


@pytest.mark.parametrize(
    "fault",
    [
        "falsey",
        "root",
        "sensitive",
        "workspace",
        "mode",
        "bytes_type",
        "uid",
        "duplicate_identity",
        "limit",
    ],
)
def test_bad_private_snapshot_refuses_before_any_staging(tmp_path, fault):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        target, before = versions(tmp_path, writer)
        if fault == "falsey":
            target = False
        elif fault in {"root", "sensitive"}:
            object.__setattr__(target, "root_path", "." if fault == "root" else ".git/tree")
        elif fault == "workspace":
            target = replace(target, workspace_fingerprint="a" * 64)
        elif fault in {"mode", "bytes_type", "uid", "limit"}:
            index = next(i for i, entry in enumerate(target.entries) if entry.file is not None)
            entry = target.entries[index]
            attributes = (
                {"mode": 0o4755}
                if fault == "mode"
                else {"data": bytearray(b"bad")}
                if fault == "bytes_type"
                else {"uid": True}
                if fault == "uid"
                else {"data": b"a" * 8_388_609}
            )
            target = replace(
                target,
                entries=target.entries[:index]
                + (replace(entry, file=replace(entry.file, **attributes)),)
                + target.entries[index + 1 :],
            )
        else:
            files = [entry for entry in target.entries if entry.file is not None]
            changed = replace(
                files[1],
                file=replace(files[1].file, inode=files[0].file.inode, device=files[0].file.device),
            )
            target = replace(
                target,
                entries=tuple(
                    changed if entry.path == changed.path else entry for entry in target.entries
                ),
            )
        with pytest.raises(ServiceError):
            writer.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)
        assert not list((tmp_path / "holder").glob("._krim-tx-*"))


def test_partial_stage_failure_preserves_original_and_original_stage_identity(
    tmp_path, monkeypatch
):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        target, before = versions(tmp_path, writer)
        stages = []

        def fail(*_args):
            raise OSError("合成部分暂存失败")

        monkeypatch.setattr(writer, "_write_tree", fail)
        with pytest.raises(ServiceError):
            writer.prepare(
                target,
                expected_before=before.fingerprint,
                token=uuid4().hex,
                record_stage=stages.append,
            )
        assert len(stages) == 1
        assert writer.reader.capture(target.root_path) == before
        path = tmp_path / "holder" / ("._krim-tx-" + stages[0].token)
        assert path.stat().st_ino == stages[0].container_identity[1]
        assert path.stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize(
    "fault",
    [
        "readonly",
        "link",
        "hardlink",
        "special",
        "directory_mode",
        "cancel",
        "authority",
        "old_version",
        "collision",
        "prepare_timeout",
        "stage_callback_swap",
    ],
)
def test_prepare_rejects_without_partial_original_target(tmp_path, monkeypatch, fault):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        target, before = versions(tmp_path, writer)
        token = uuid4().hex
        abort = Event()
        if fault == "readonly":
            workspace._writable = False
        elif fault == "link":
            (tmp_path / "holder/live/link").symlink_to(tmp_path / "outside")
        elif fault == "hardlink":
            os.link(tmp_path / "holder/live/sub/raw.bin", tmp_path / "holder/live/alias")
        elif fault == "special":
            os.mkfifo(tmp_path / "holder/live/fifo")
        elif fault == "directory_mode":
            (tmp_path / "holder/live").chmod(0o2750)
        elif fault == "cancel":
            abort.set()
        elif fault == "old_version":
            (tmp_path / "holder/live/sub/raw.bin").write_bytes(b"changed")
        elif fault == "collision":
            path = tmp_path / "holder" / ("._krim-tx-" + token)
            path.mkdir(mode=0o700)
            (path / "foreign.txt").write_bytes(b"foreign")
        elif fault == "prepare_timeout":
            writer.lock_timeout = 0.05

        def authorize():
            if fault == "authority":
                raise ServiceError("owner_revoked", "原资格撤销", 409)

        def record(stage):
            if fault == "prepare_timeout":
                time.sleep(0.07)
            if fault == "stage_callback_swap":
                path = tmp_path / "holder" / ("._krim-tx-" + stage.token)
                path.rename(path.with_name("original-private"))
                path.mkdir(mode=0o700)

        with pytest.raises(ServiceError):
            writer.prepare(
                target,
                expected_before=before.fingerprint,
                token=token,
                record_stage=record,
                authorize=authorize,
                cancelled=abort.is_set,
            )
        expected = b"changed" if fault == "old_version" else b"\x80current"
        assert (tmp_path / "holder/live/sub/raw.bin").read_bytes() == expected
        assert (tmp_path / "holder/live/new-only.txt").read_bytes() == b"new before"
        if fault == "collision":
            assert (path / "foreign.txt").read_bytes() == b"foreign"


def test_two_original_preparations_one_publishes_other_never_replays(tmp_path, monkeypatch):
    with Workspace(tmp_path, writable=True) as workspace:
        first = WorkspaceDirectoryRestoreWriter(workspace)
        target, before = versions(tmp_path, first)
        a = first.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)
        with Workspace(tmp_path, writable=True) as other:
            second = WorkspaceDirectoryRestoreWriter(other)
            b = second.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)
            assert first.apply(a, record_intent=lambda _value: None).receipt.status == "done"

            def forbidden(*_args, **_kwargs):
                pytest.fail("旧准备在其他发布后不得再namespace")

            monkeypatch.setattr(module, "rename_at", forbidden)
            effect = second.apply(b, record_intent=forbidden)
            assert effect.receipt.status == "rejected" and not effect.receipt.changed
            assert second.reconcile(b).receipt.status == "uncertain"


def test_empty_original_checkpoint_replaces_nonempty_current_as_one_tree(tmp_path):
    (tmp_path / "holder/live").mkdir(parents=True)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        target = writer.reader.capture("holder/live")
        (tmp_path / "holder/live/sub").mkdir()
        (tmp_path / "holder/live/sub/raw.bin").write_bytes(b"current\0bytes")
        before = writer.reader.capture("holder/live")
        change = writer.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)
        effect = writer.apply(change, record_intent=lambda _value: None)
        assert effect.receipt.status == "done"
        assert list((tmp_path / "holder/live").iterdir()) == []
        assert len(effect.observed_after.entries) == 1
        assert effect.observed_after.entries[0].directory.empty
        assert effect.retained_before.entries[-1].file.data == b"current\0bytes"


@pytest.mark.skipif(getattr(os, "geteuid", lambda: -1)() != 0, reason="仅本机root已有chown权限")
def test_real_target_uid_gid_restored_under_existing_os_permissions(tmp_path):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        target, before = versions(tmp_path, writer, owner=True)
        change = writer.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)
        effect = writer.apply(change, record_intent=lambda _value: None)
        assert effect.receipt.status == "done"
        for path in ("holder/live", "holder/live/sub/raw.bin"):
            info = (tmp_path / path).stat()
            assert info.st_uid == info.st_gid == 65534
        assert (tmp_path / "holder/live/sub/raw.bin").read_bytes() == b"\0\xff"
