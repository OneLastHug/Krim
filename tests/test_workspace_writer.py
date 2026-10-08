"""条件修改的实际文件、竞争、错误阶段和原材料保留；不使用模型或外部服务。"""

import asyncio
import errno
import hashlib
import os
import stat
import subprocess
import sys
from dataclasses import replace
from threading import Event

import pytest
from pydantic import ValidationError

import krim_agent.workspace_writer as writer_module
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec, TextReplacement
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux renameat2原语")


def sha(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def write_spec(text="new\n", expected="old\n", path="file.txt"):
    return FileChangeSpec(path=path, operation="write", expected_sha256=sha(expected), content=text)


def before_file(tmp_path, text="old\n", mode=0o644):
    file = tmp_path / "file.txt"
    file.write_text(text, encoding="utf-8")
    file.chmod(mode)
    return file


def test_real_create_write_replace_delete_and_retained_snapshots(tmp_path):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        created = writer.prepare(
            FileChangeSpec(
                path="file.txt", operation="write", expected_sha256=None, content="old\n"
            )
        )
        assert created.preview().before_sha256 is None
        assert not (tmp_path / "file.txt").exists()
        assert writer.apply(created).receipt.status == "done"
        assert (tmp_path / "file.txt").read_text() == "old\n"
        (tmp_path / "file.txt").chmod(0o755)
        changed = writer.prepare(write_spec())
        effect = writer.apply(changed)
        assert effect.receipt.status == "done" and effect.receipt.changed
        assert stat.S_IMODE((tmp_path / "file.txt").stat().st_mode) == 0o755
        current, old = writer.inspect_recovery(effect.recovery)
        assert current.data == b"new\n" and old.data == b"old\n"
        assert [entry.path for entry in workspace.list().entries] == ["file.txt"]
        writer.finalize(changed, effect.recovery)
        assert not list(tmp_path.glob("._krim-tx-*"))
        literal = writer.prepare(
            FileChangeSpec(
                path="file.txt",
                operation="replace",
                expected_sha256=sha("new\n"),
                replacements=[TextReplacement(old_text="new", new_text="literal")],
            )
        )
        literal_effect = writer.apply(literal)
        assert (tmp_path / "file.txt").read_text() == "literal\n"
        writer.finalize(literal, literal_effect.recovery)
        deleted = writer.prepare(
            FileChangeSpec(path="file.txt", operation="delete", expected_sha256=sha("literal\n"))
        )
        deletion = writer.apply(deleted)
        assert deletion.receipt.status == "done" and not (tmp_path / "file.txt").exists()
        current, old = writer.inspect_recovery(deletion.recovery)
        assert current is None and old.data == b"literal\n"
        writer.finalize(deleted, deletion.recovery)
        assert not list(tmp_path.iterdir())


def test_read_only_capability_and_fingerprint_and_frozen_intent(tmp_path):
    file = before_file(tmp_path)
    with Workspace(tmp_path) as readonly, Workspace(tmp_path, writable=True) as writable:
        assert readonly.fingerprint != writable.fingerprint
        plan = WorkspaceWriter(readonly).prepare(write_spec())
        with pytest.raises(ServiceError) as error:
            WorkspaceWriter(readonly).apply(plan)
        assert error.value.code == "workspace_read_only"
        writer = WorkspaceWriter(writable)
        with pytest.raises(ServiceError) as changed:
            writer.apply(plan)
        assert changed.value.code == "workspace_binding_changed"
        proper = writer.prepare(write_spec())
        proper.spec.path = "another.txt"
        assert proper.spec.path == "file.txt"
        assert writer.apply(proper).receipt.status == "done"
        assert file.read_text() == "new\n"


@pytest.mark.parametrize(
    "path",
    [
        "../file.txt",
        "/tmp/file.txt",
        ".env",
        ".Env.local",
        ".git/config",
        ".ssh/key",
        "x/../file.txt",
        "x//file.txt",
        "C:/secret",
        ".",
        "._krim-tx-0123",
    ],
)
def test_change_paths_keep_existing_security_boundaries(tmp_path, path):
    with Workspace(tmp_path, writable=True) as workspace:
        with pytest.raises(ServiceError) as error:
            WorkspaceWriter(workspace).prepare(
                FileChangeSpec(path=path, operation="write", expected_sha256=None, content="text")
            )
        assert error.value.code in {"workspace_path", "workspace_sensitive"}
        assert not list(tmp_path.iterdir())


def test_file_type_link_encoding_and_capacity_boundaries(tmp_path):
    (tmp_path / "invalid.txt").write_bytes(b"\xff")
    (tmp_path / "binary.txt").write_bytes(b"\0")
    file = before_file(tmp_path)
    (tmp_path / "symlink.txt").symlink_to(file)
    (tmp_path / "directory").mkdir()
    os.mkfifo(tmp_path / "fifo")
    with Workspace(tmp_path, writable=True, max_file_bytes=64) as workspace:
        writer = WorkspaceWriter(workspace)
        for path, code in [
            ("invalid.txt", "workspace_encoding"),
            ("binary.txt", "workspace_binary"),
            ("symlink.txt", "workspace_access"),
            ("directory", "workspace_file_type"),
            ("fifo", "workspace_file_type"),
        ]:
            with pytest.raises(ServiceError) as rejected:
                writer.prepare(
                    FileChangeSpec(
                        path=path, operation="write", expected_sha256=None, content="new"
                    )
                )
            assert rejected.value.code == code
        os.link(file, tmp_path / "hard.txt")
        with pytest.raises(ServiceError) as hard:
            writer.prepare(write_spec())
        assert hard.value.code == "workspace_link"
        (tmp_path / "hard.txt").unlink()
        with pytest.raises(ServiceError) as capacity:
            writer.prepare(write_spec("x" * 65))
        assert capacity.value.code == "workspace_file_limit"
        with pytest.raises(ServiceError) as binary:
            writer.prepare(write_spec("\0"))
        assert binary.value.code == "workspace_binary"
        assert file.read_text() == "old\n"


@pytest.mark.parametrize("change", ["contents", "mode", "identity", "symlink"])
def test_commit_rechecks_original_file_and_never_overwrites_changed_file(tmp_path, change):
    file = before_file(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec())
        if change == "contents":
            file.write_text("external", encoding="utf-8")
        elif change == "mode":
            file.chmod(0o600)
        elif change == "identity":
            file.rename(tmp_path / "original.txt")
            file.write_text("old\n", encoding="utf-8")
            assert file.stat().st_ino != (tmp_path / "original.txt").stat().st_ino
        else:
            file.unlink()
            (tmp_path / "outside.txt").write_text("external", encoding="utf-8")
            file.symlink_to(tmp_path / "outside.txt")
        if change == "symlink":
            with pytest.raises(ServiceError):
                writer.apply(plan)
        else:
            result = writer.apply(plan)
            assert result.receipt.status == "rejected" and not result.receipt.changed
        assert file.read_text() != "new\n"
        assert not list(tmp_path.glob("._krim-tx-*"))


def test_parent_identity_switch_and_missing_parent_are_rejected(tmp_path):
    (tmp_path / "src").mkdir()
    file = tmp_path / "src" / "file.txt"
    file.write_text("old\n", encoding="utf-8")
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec(path="src/file.txt"))
        (tmp_path / "src").rename(tmp_path / "saved")
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "file.txt").write_text("old\n", encoding="utf-8")
        with pytest.raises(ServiceError) as parent:
            writer.apply(plan)
        assert parent.value.code == "workspace_parent_changed"
        with pytest.raises(ServiceError) as missing:
            writer.prepare(
                FileChangeSpec(
                    path="missing/new.txt", operation="write", expected_sha256=None, content="new"
                )
            )
        assert missing.value.code == "workspace_not_found"


def test_literal_occurrences_conflict_and_preview_hash_and_diff_are_real(tmp_path):
    file = before_file(tmp_path, "line\nold old\nend")
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        with pytest.raises(ServiceError) as count:
            writer.prepare(
                FileChangeSpec(
                    path="file.txt",
                    operation="replace",
                    expected_sha256=sha(file.read_text()),
                    replacements=[TextReplacement(old_text="old", new_text="new")],
                )
            )
        assert count.value.code == "workspace_replace_conflict"
        plan = writer.prepare(
            FileChangeSpec(
                path="file.txt",
                operation="replace",
                expected_sha256=sha(file.read_text()),
                replacements=[TextReplacement(old_text="old", new_text="new", occurrences=2)],
            )
        )
        view = plan.preview()
        assert view.after_sha256 == sha("line\nnew new\nend") and not view.truncated
        assert "-old old" in view.diff and "+new new" in view.diff
        assert "No newline at end of file" in view.diff
        result = subprocess.run(
            ["git", "apply", "--check", "-"],
            cwd=tmp_path,
            input=view.diff,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stderr
        assert file.read_text() == "line\nold old\nend"


@pytest.mark.parametrize(
    "old,new",
    [
        ("", "new\n"),
        ("old\n", ""),
        ("a\nb\nc\n", "a\nB\nc\n"),
        ("carriage\rdata", "changed\rdata"),
        ("你好\n最后", "你好\n变更"),
    ],
)
def test_untruncated_preview_is_valid_unified_diff_for_external_git(tmp_path, old, new):
    file = before_file(tmp_path, old)
    with Workspace(tmp_path, writable=True) as workspace:
        plan = WorkspaceWriter(workspace).prepare(write_spec(new, old))
        preview = plan.preview()
        applied = subprocess.run(
            ["git", "apply", "--whitespace=nowarn", "-"],
            cwd=tmp_path,
            input=preview.diff,
            text=True,
            capture_output=True,
        )
        assert applied.returncode == 0, applied.stderr
        assert file.read_bytes() == new.encode()


def test_preview_large_and_escape_heavy_content_is_bounded(tmp_path):
    before_file(tmp_path, "x\n" * 100000)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        large = writer.prepare(write_spec("changed\n" * 2000, "x\n" * 100000)).preview()
        assert large.truncated and len(large.model_dump_json().encode()) <= 24000
        file = tmp_path / "file.txt"
        file.write_text("\x01" * 4000, encoding="utf-8")
        escapes = writer.prepare(write_spec("\x02" * 4000, "\x01" * 4000)).preview()
        assert escapes.truncated and len(escapes.model_dump_json().encode()) <= 24000


def test_same_content_is_explicit_noop_and_bad_spec_is_rejected(tmp_path):
    file = before_file(tmp_path)
    identity = file.stat().st_ino
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec("old\n"))
        assert plan.preview().diff == ""
        result = writer.apply(plan)
        assert result.receipt.status == "done" and not result.receipt.changed
        assert file.stat().st_ino == identity
        with pytest.raises(ValidationError):
            FileChangeSpec(path="file.txt", operation="delete", expected_sha256=None)
        with pytest.raises(ValidationError):
            FileChangeSpec(
                path="file.txt", operation="write", expected_sha256="not-sha", content="text"
            )
        with pytest.raises(ServiceError) as token:
            writer.apply(replace(plan, token="../bad"))
        assert token.value.code == "workspace_change_invalid"


@pytest.mark.parametrize("deletion", [False, True])
def test_change_between_precheck_and_rename_restores_actual_external_file(
    tmp_path, monkeypatch, deletion
):
    file = before_file(tmp_path)
    original_rename = writer_module.rename_at
    first = True

    def external_then_rename(fd, source, target, **kwargs):
        nonlocal first
        if first:
            first = False
            file.write_text("external update", encoding="utf-8")
        original_rename(fd, source, target, **kwargs)

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        spec = (
            FileChangeSpec(path="file.txt", operation="delete", expected_sha256=sha("old\n"))
            if deletion
            else write_spec()
        )
        plan = writer.prepare(spec)
        monkeypatch.setattr(writer_module, "rename_at", external_then_rename)
        result = writer.apply(plan)
        assert (
            result.receipt.status == "rejected"
            and result.receipt.error_code == "workspace_conflict"
        )
        assert file.read_text() == "external update"
        assert not list(tmp_path.glob("._krim-tx-*"))


def test_second_external_change_during_conflict_restore_is_uncertain_and_keeps_both(
    tmp_path, monkeypatch
):
    file = before_file(tmp_path)
    original_rename = writer_module.rename_at
    count = 0

    def racing_rename(fd, source, target, **kwargs):
        nonlocal count
        count += 1
        if count == 1:
            file.write_text("external one", encoding="utf-8")
        elif count == 2:
            file.write_text("external two", encoding="utf-8")
        original_rename(fd, source, target, **kwargs)

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec())
        monkeypatch.setattr(writer_module, "rename_at", racing_rename)
        result = writer.apply(plan)
        assert result.receipt.status == "uncertain" and result.receipt.recovery_required
        current, recovery = writer.inspect_recovery(result.recovery)
        assert {current.data, recovery.data} == {b"external one", b"external two"}


def test_delete_conflict_followed_by_new_external_file_preserves_recovery(tmp_path, monkeypatch):
    file = before_file(tmp_path)
    original_rename = writer_module.rename_at
    first = True

    def racing_delete(fd, source, target, **kwargs):
        nonlocal first
        if first:
            first = False
            file.write_text("external before delete", encoding="utf-8")
            original_rename(fd, source, target, **kwargs)
            file.write_text("external new file", encoding="utf-8")
        else:
            original_rename(fd, source, target, **kwargs)

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(
            FileChangeSpec(path="file.txt", operation="delete", expected_sha256=sha("old\n"))
        )
        monkeypatch.setattr(writer_module, "rename_at", racing_delete)
        result = writer.apply(plan)
        assert result.receipt.status == "uncertain"
        current, old = writer.inspect_recovery(result.recovery)
        assert current.data == b"external new file" and old.data == b"external before delete"


@pytest.mark.parametrize("phase", ["before", "after", "cleanup"])
def test_fsync_failure_never_reports_success_or_erases_unconfirmed_old_data(
    tmp_path, monkeypatch, phase
):
    file = before_file(tmp_path)
    original = os.fsync
    count = 0

    def fsync(fd):
        nonlocal count
        count += 1
        fail_at = {"before": 1, "after": 3, "cleanup": 4}[phase]
        if count == fail_at:
            raise OSError(errno.EIO, "injected sync failure")
        return original(fd)

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec())
        monkeypatch.setattr(os, "fsync", fsync)
        result = writer.apply(plan)
        assert result.receipt.status == ("rejected" if phase == "before" else "uncertain")
        if phase == "before":
            assert file.read_text() == "old\n" and not list(tmp_path.glob("._krim-tx-*"))
        else:
            current, old = writer.inspect_recovery(result.recovery)
            assert current.data == b"new\n" and old.data == b"old\n"


def test_stage_collision_and_creation_race_never_overwrite_other_files(tmp_path, monkeypatch):
    file = before_file(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec())
        stage = tmp_path / f"._krim-tx-{plan.token}"
        stage.write_text("already there", encoding="utf-8")
        assert writer.apply(plan).receipt.status == "rejected"
        assert stage.read_text() == "already there" and file.read_text() == "old\n"
        stage.unlink()
        fresh = writer.prepare(
            FileChangeSpec(path="new.txt", operation="write", expected_sha256=None, content="ours")
        )
        original_link = os.link

        def raced_link(*args, **kwargs):
            (tmp_path / "new.txt").write_text("external", encoding="utf-8")
            return original_link(*args, **kwargs)

        monkeypatch.setattr(os, "link", raced_link)
        assert writer.apply(fresh).receipt.status == "rejected"
        assert (tmp_path / "new.txt").read_text() == "external"


def test_cancel_before_publication_leaves_file_and_cleanup_consistent(tmp_path, monkeypatch):
    file = before_file(tmp_path)
    cancelled = Event()
    original_fsync = os.fsync

    def cancel_during_staging(fd):
        original_fsync(fd)
        cancelled.set()

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec())
        monkeypatch.setattr(os, "fsync", cancel_during_staging)
        result = writer.apply(plan, abort=cancelled.is_set)
        assert (
            result.receipt.status == "rejected"
            and result.receipt.error_code == "workspace_write_cancelled"
        )
        assert file.read_text() == "old\n" and not list(tmp_path.glob("._krim-tx-*"))


async def test_two_independent_workspace_objects_cannot_both_commit_same_version(tmp_path):
    file = before_file(tmp_path)
    with Workspace(tmp_path, writable=True) as one, Workspace(tmp_path, writable=True) as two:
        writers = [WorkspaceWriter(one), WorkspaceWriter(two)]
        plans = [writers[0].prepare(write_spec("one\n")), writers[1].prepare(write_spec("two\n"))]
        results = await asyncio.gather(
            *(
                asyncio.to_thread(writer.apply, plan)
                for writer, plan in zip(writers, plans, strict=True)
            )
        )
        assert sorted(result.receipt.status for result in results) == ["done", "rejected"]
        assert file.read_text() in {"one\n", "two\n"}


def test_explicit_finalize_refuses_changed_recovery_material(tmp_path):
    before_file(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        plan = writer.prepare(write_spec())
        effect = writer.apply(plan)
        stage = tmp_path / effect.recovery.stage_name
        stage.write_text("external material", encoding="utf-8")
        with pytest.raises(ServiceError) as rejected:
            writer.finalize(plan, effect.recovery)
        assert rejected.value.code == "workspace_stage_changed"
        assert stage.read_text() == "external material"
        assert (tmp_path / "file.txt").read_text() == "new\n"


def test_short_and_zero_length_writes_are_handled_without_partial_public_file(
    tmp_path, monkeypatch
):
    file = before_file(tmp_path)
    original = os.write

    def short(fd, value):
        return original(fd, value[:2])

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        monkeypatch.setattr(os, "write", short)
        plan = writer.prepare(write_spec("many partial writes\n"))
        result = writer.apply(plan)
        assert result.receipt.status == "done" and file.read_text() == "many partial writes\n"
        writer.finalize(plan, result.recovery)
        monkeypatch.setattr(os, "write", lambda _fd, _value: 0)
        rejected = writer.apply(writer.prepare(write_spec("next", "many partial writes\n")))
        assert rejected.receipt.status == "rejected" and file.read_text() == "many partial writes\n"
        assert not list(tmp_path.glob("._krim-tx-*"))


@pytest.mark.skipif(os.geteuid() != 0, reason="需要本机root核验非当前uid/gid的文件所有权保留")
def test_owner_group_and_ordinary_mode_are_preserved_without_setuid_bits(tmp_path):
    file = before_file(tmp_path)
    os.chown(file, 1001, 1002)
    file.chmod(0o4755)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace)
        result = writer.apply(writer.prepare(write_spec()))
        assert result.receipt.status == "done"
        current = file.stat()
        assert (current.st_uid, current.st_gid) == (1001, 1002)
        assert stat.S_IMODE(current.st_mode) == 0o755


def test_kernel_lock_wait_has_a_real_deadline(tmp_path):
    import fcntl

    before_file(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace, lock_timeout=0.05)
        plan = writer.prepare(write_spec())
        holder = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(holder, fcntl.LOCK_EX)
            with pytest.raises(ServiceError) as blocked:
                writer.apply(plan)
            assert blocked.value.code == "workspace_write_busy"
        finally:
            os.close(holder)
        assert (tmp_path / "file.txt").read_text() == "old\n"


async def test_instance_lock_deadline_and_inflight_file_operation(tmp_path, monkeypatch):
    before_file(tmp_path)
    entered, release = Event(), Event()
    original = os.fsync
    first = True

    def slow_fsync(fd):
        nonlocal first
        if first:
            first = False
            entered.set()
            assert release.wait(3)
        original(fd)

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceWriter(workspace, lock_timeout=0.05)
        one, two = writer.prepare(write_spec()), writer.prepare(write_spec("second"))
        monkeypatch.setattr(os, "fsync", slow_fsync)
        operation = asyncio.create_task(asyncio.to_thread(writer.apply, one))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            with pytest.raises(ServiceError) as blocked:
                await asyncio.to_thread(writer.apply, two)
            assert blocked.value.code == "workspace_write_busy"
            release.set()
            assert (await operation).receipt.status == "done"
        finally:
            release.set()
            await operation


async def test_two_actual_python_processes_serialize_commit_on_same_directory(tmp_path):
    file = before_file(tmp_path)
    code = """
import json, sys
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec
from krim_agent.workspace_writer import WorkspaceWriter
with Workspace(sys.argv[1], writable=True) as workspace:
    writer = WorkspaceWriter(workspace)
    spec = FileChangeSpec(path='file.txt', operation='write',
                          expected_sha256=workspace.read('file.txt').sha256, content=sys.argv[2])
    plan = writer.prepare(spec)
    print('ready', flush=True)
    assert sys.stdin.readline().strip() == 'go'
    result = writer.apply(plan)
    print(result.receipt.model_dump_json(), flush=True)
"""
    processes = []
    try:
        for value in ("process one", "process two"):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                code,
                str(tmp_path),
                value,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            processes.append(process)
            assert (await asyncio.wait_for(process.stdout.readline(), 10)).strip() == b"ready"
        for process in processes:
            process.stdin.write(b"go\n")
            await process.stdin.drain()
            process.stdin.close()
        results = []
        for process in processes:
            output, error = await asyncio.wait_for(process.communicate(), 10)
            assert process.returncode == 0, error.decode()
            import json

            results.append(json.loads(output)["status"])
        assert sorted(results) == ["done", "rejected"]
        assert file.read_text() in {"process one", "process two"}
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()


async def test_host_write_capability_registration_does_not_add_model_write_tools(runtime, tmp_path):
    from krim_agent.application.agents import AgentService

    service = AgentService(runtime, runtime.settings)
    try:
        binding = service.register_workspace(
            "files", tmp_path, version="v1", writable=True, default=True
        )
        assert binding.workspace.writable
        view = await service.spawn("default")
        assert (await service.workspace_info(view.agent_id)).writable
        assert {
            tool.name for tool in runtime.tools.definitions if tool.name.startswith("workspace_")
        } == {"workspace_read", "workspace_list", "workspace_search"}
    finally:
        await service.close()
