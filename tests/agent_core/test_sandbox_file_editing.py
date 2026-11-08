"""A3补丁与rename模块测试；全部使用内存fake端口，不构成对真实rename的验收。"""

from __future__ import annotations

import hashlib
import inspect
import sys
from collections.abc import Callable
from typing import Any

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.edit_models import MAX_PATCH_FILE_BYTES, MAX_REPLACEMENT_BYTES
from krim_agent.sandbox.file_editing import OperationJournal, ProjectFileEditor
from krim_agent.sandbox.ports import ProjectRef

PROJECT = ProjectRef(principal_id="p1", environment_id="e1", project_id="pr1", generation=7)
GENERATION = 7


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FakeMutation:
    """内存端口：按真实端口语义执行前置校验；可选在写/rename落地前后注入异常。"""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files: dict[str, bytes] = {k: bytes(v) for k, v in (files or {}).items()}
        self.reads: list[str] = []
        self.writes: list[tuple[str, str, bytes, str | None]] = []
        self.renames: list[tuple[str, str, str, str, str | None]] = []
        self.write_error: BaseException | None = None
        self.apply_before_error = False

    def read(self, project: ProjectRef, path: str) -> bytes | None:
        self.reads.append(path)
        return self.files.get(path)

    def write(
        self, project: ProjectRef, operation_id: str, path: str, content: bytes,
        expected_sha256: str | None,
    ) -> dict[str, Any]:
        self.writes.append((operation_id, path, bytes(content), expected_sha256))
        current = self.files.get(path)
        if expected_sha256 is not None and (current is None or digest(current) != expected_sha256):
            raise ServiceError("source_changed")
        if self.write_error is not None:
            if self.apply_before_error:
                self.files[path] = bytes(content)
            raise self.write_error
        self.files[path] = bytes(content)
        return {"operation_id": operation_id, "path": path}

    def rename(
        self, project: ProjectRef, operation_id: str, source: str, destination: str,
        expected_sha256: str, expected_destination_sha256: str | None,
    ) -> dict[str, Any]:
        self.renames.append((operation_id, source, destination, expected_sha256,
                             expected_destination_sha256))
        if source not in self.files or digest(self.files[source]) != expected_sha256:
            raise ServiceError("source_changed")
        if destination in self.files:
            raise ServiceError("key_conflict")
        moved = self.files.pop(source)
        if self.write_error is not None:
            if self.apply_before_error:
                self.files[destination] = moved  # 已落地但响应丢失
            else:
                self.files[source] = moved  # 未落地，回滚
            raise self.write_error
        self.files[destination] = moved
        return {"operation_id": operation_id, "source": source, "destination": destination}


class FakeJournal:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def record(self, operation_id: str, status: str, detail: str = "") -> None:
        self.entries.append((operation_id, status))

    def status(self, operation_id: str) -> str | None:
        for entry_operation, entry_status in reversed(self.entries):
            if entry_operation == operation_id:
                return entry_status
        return None


class CommitThenRecheckMutation(FakeMutation):
    """模拟AcceptedProjectMutation真实语义：先完成落盘动作，再复核租约/generation。

    复核失败抛ServiceError时变更可能已落盘（真实端口即如此）；on_recheck注入
    复核窗口内的并发改写，read_error注入读回通道故障。
    """

    def __init__(
        self,
        files: dict[str, bytes] | None = None,
        recheck_error: BaseException | None = None,
        on_recheck: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(files)
        self.recheck_error = recheck_error
        self.on_recheck = on_recheck
        self.read_error: BaseException | None = None

    def read(self, project: ProjectRef, path: str) -> bytes | None:
        if self.read_error is not None:
            raise self.read_error
        return super().read(project, path)

    def _recheck(self) -> None:
        if self.on_recheck is not None:
            self.on_recheck()
        if self.recheck_error is not None:
            raise self.recheck_error

    def write(
        self, project: ProjectRef, operation_id: str, path: str, content: bytes,
        expected_sha256: str | None,
    ) -> dict[str, Any]:
        self.writes.append((operation_id, path, bytes(content), expected_sha256))
        current = self.files.get(path)
        if expected_sha256 is not None and (current is None or digest(current) != expected_sha256):
            raise ServiceError("source_changed")
        self.files[path] = bytes(content)  # 已落盘
        self._recheck()  # 落盘后围栏复核失败
        return {"operation_id": operation_id, "path": path}

    def rename(
        self, project: ProjectRef, operation_id: str, source: str, destination: str,
        expected_sha256: str, expected_destination_sha256: str | None,
    ) -> dict[str, Any]:
        self.renames.append((operation_id, source, destination, expected_sha256,
                             expected_destination_sha256))
        if source not in self.files or digest(self.files[source]) != expected_sha256:
            raise ServiceError("source_changed")
        if destination in self.files:
            raise ServiceError("key_conflict")
        self.files[destination] = self.files.pop(source)  # 已原子移动
        self._recheck()
        return {"operation_id": operation_id, "source": source, "destination": destination}


def editor(
    files: dict[str, bytes] | None = None, journal: OperationJournal | None = None
) -> tuple[ProjectFileEditor, FakeMutation]:
    port = FakeMutation(files)
    return ProjectFileEditor(port, journal), port


def patch_kwargs(
    content: bytes, offset: int = 0, delete_length: int = 0, replacement: str = ""
) -> dict[str, Any]:
    return {
        "expected_generation": GENERATION,
        "path": "notes.md",
        "expected_sha256": digest(content),
        "offset": offset,
        "delete_length": delete_length,
        "replacement": replacement,
    }


def rename_plan(
    service: ProjectFileEditor, source: str, destination: str, content: bytes
):
    return service.plan_rename(
        PROJECT, expected_generation=GENERATION, source=source, destination=destination,
        expected_source_sha256=digest(content),
    )


def test_patch_plan_and_apply_updates_content_with_single_port_call() -> None:
    content = "héllo 世界".encode()  # h0 é1-2 l3 l4 o5 空格6 世7-9 界10-12
    service, port = editor({"notes.md": content})
    plan = service.plan_patch(PROJECT, **patch_kwargs(content, 10, 3, "好"))
    assert plan.result_sha256 == digest("héllo 世好".encode())
    result = service.apply_patch(plan)
    assert result["status"] == "applied"
    assert port.files["notes.md"] == "héllo 世好".encode()
    assert len(port.writes) == 1
    operation_id, path, written, expected = port.writes[0]
    assert operation_id == plan.plan_id
    assert path == "notes.md"
    assert written == "héllo 世好".encode()
    assert expected == digest(content)
    assert port.renames == []


def test_patch_plan_is_deterministic_and_read_only() -> None:
    content = "计划不改文件".encode()
    service, port = editor({"notes.md": content})
    first = service.plan_patch(PROJECT, **patch_kwargs(content, 0, 0, "x"))
    second = service.plan_patch(PROJECT, **patch_kwargs(content, 0, 0, "x"))
    assert first.plan_id == second.plan_id
    assert first.result_sha256 == second.result_sha256
    assert first.source_sha256 == digest(content)
    assert port.writes == [] and port.renames == []  # 计划只读，不产生变动
    assert port.files["notes.md"] == content


def test_patch_hash_conflict_rejected_without_write() -> None:
    service, port = editor({"notes.md": b"actual"})
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs(b"stale"))
    assert raised.value.code == "source_changed"
    assert port.writes == []


def test_patch_generation_conflict_rejected() -> None:
    service, port = editor({"notes.md": b"content"})
    kwargs = patch_kwargs(b"content")
    kwargs["expected_generation"] = GENERATION + 1
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **kwargs)
    assert raised.value.code == "revision_conflict"
    assert port.writes == []


def test_patch_misaligned_utf8_boundary_rejected() -> None:
    content = "中文ab".encode()  # 中0-2 文3-5 a6 b7
    service, port = editor({"notes.md": content})
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs(content, 1, 2, "x"))  # 起点落在中内部
    assert raised.value.code == "invalid_input"
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs(content, 0, 4, "x"))  # 终点落在文内部
    assert raised.value.code == "invalid_input"
    assert port.writes == []


def test_patch_range_overflow_and_missing_file_rejected() -> None:
    service, port = editor({"notes.md": b"abc"})
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs(b"abc", 2, 2, "x"))
    assert raised.value.code == "invalid_input"
    missing = patch_kwargs(b"abc")
    missing["path"] = "other.md"
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **missing)
    assert raised.value.code == "not_found"
    assert port.writes == []


def test_patch_file_size_and_replacement_limits() -> None:
    big = b"a" * (MAX_PATCH_FILE_BYTES + 1)
    service, port = editor({"notes.md": big})
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs(big, 0, 0, "x"))
    assert raised.value.code == "sandbox_limit"
    full = b"a" * MAX_PATCH_FILE_BYTES
    service, port = editor({"notes.md": full})
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs(full, 0, 0, "x" * (MAX_REPLACEMENT_BYTES + 1)))
    assert raised.value.code == "invalid_input"
    plan = service.plan_patch(
        PROJECT, **patch_kwargs(full, 0, MAX_PATCH_FILE_BYTES, "x" * MAX_REPLACEMENT_BYTES)
    )
    service.apply_patch(plan)
    assert port.files["notes.md"] == b"x" * MAX_REPLACEMENT_BYTES  # 16KiB整字节替换成功


def test_patch_rejects_binary_and_surrogate_replacement() -> None:
    service, port = editor({"notes.md": b"\xff\xfe\x00binary"})
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs(b"\xff\xfe\x00binary", 0, 0, "x"))
    assert raised.value.code == "invalid_input"
    service, port = editor({"notes.md": "文本".encode()})
    with pytest.raises(ServiceError) as raised:
        service.plan_patch(PROJECT, **patch_kwargs("文本".encode(), 0, 0, "\udce9"))
    assert raised.value.code == "invalid_input"
    assert port.writes == []


def test_patch_append_at_end_and_inplace_delete() -> None:
    tail = "尾".encode()
    service, port = editor({"notes.md": tail})
    plan = service.plan_patch(PROJECT, **patch_kwargs(tail, len(tail), 0, "!"))
    service.apply_patch(plan)
    assert port.files["notes.md"] == "尾!".encode()
    service, port = editor({"notes.md": b"keep this"})
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"keep this", 4, 5, ""))
    service.apply_patch(plan)
    assert port.files["notes.md"] == b"keep"


def test_patch_stale_apply_rejected_and_external_application_short_circuits() -> None:
    service, port = editor({"notes.md": b"one"})
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    port.files["notes.md"] = b"changed-by-others"
    with pytest.raises(ServiceError) as raised:
        service.apply_patch(plan)
    assert raised.value.code == "source_changed"
    assert port.writes == []
    port.files["notes.md"] = b"two"  # 外部已落地同样内容
    result = service.apply_patch(plan)
    assert result["already_applied"] is True
    assert port.writes == []


def test_rename_plan_and_apply_moves_once_without_copy_delete() -> None:
    content = "搬走的文件".encode()
    service, port = editor({"old.txt": content})
    plan = rename_plan(service, "old.txt", "new/deep.txt", content)
    assert plan.mode == "atomic_rename"
    assert plan.destination_preexists is False
    result = service.apply_rename(plan)
    assert result["status"] == "applied"
    assert "old.txt" not in port.files and port.files["new/deep.txt"] == content
    assert len(port.renames) == 1 and port.writes == []  # 无copy+delete伪装
    operation_id, source, destination, expected_sha, expected_dst = port.renames[0]
    assert (operation_id, source, destination) == (plan.plan_id, "old.txt", "new/deep.txt")
    assert expected_sha == digest(content) and expected_dst is None


def test_rename_destination_conflict_rejected_before_port_call() -> None:
    service, port = editor({"old.txt": b"body", "new.txt": b"occupied"})
    with pytest.raises(ServiceError) as raised:
        service.apply_rename(rename_plan(service, "old.txt", "new.txt", b"body"))
    assert raised.value.code == "key_conflict"
    assert port.renames == []


def test_rename_path_escape_and_protected_paths_rejected() -> None:
    service, _ = editor({"old.txt": b"body"})
    cases = [
        ("../old.txt", "new.txt", "invalid_input"),
        ("old.txt", "/etc/new.txt", "invalid_input"),
        ("old.txt", "a\\b.txt", "invalid_input"),
        (".git/config", "new.txt", "permission_denied"),
        ("old.txt", "credentials", "permission_denied"),
        ("old.txt", ".env.local", "permission_denied"),
    ]
    for bad_source, bad_destination, expected in cases:
        with pytest.raises(ServiceError) as raised:
            rename_plan(service, bad_source, bad_destination, b"body")
        assert raised.value.code == expected


def test_rename_non_nfc_and_generation_conflict_rejected() -> None:
    service, _ = editor({"old.txt": b"body"})
    nfd_destination = "e" + "́" + ".txt"  # 组合符形式，非NFC
    with pytest.raises(ServiceError) as raised:
        rename_plan(service, "old.txt", nfd_destination, b"body")
    assert raised.value.code == "invalid_input"
    with pytest.raises(ServiceError) as raised:
        service.plan_rename(
            PROJECT, expected_generation=GENERATION + 1, source="old.txt", destination="new.txt",
            expected_source_sha256=digest(b"body"),
        )
    assert raised.value.code == "revision_conflict"


def test_rename_missing_or_changed_source_rejected() -> None:
    service, port = editor({"old.txt": b"one"})
    with pytest.raises(ServiceError) as raised:
        rename_plan(service, "missing.txt", "new.txt", b"one")
    assert raised.value.code == "not_found"
    # 目录经read端口不可读（返回None），无法取得源hash，递归目录移动因此被排除。
    with pytest.raises(ServiceError) as raised:
        service.plan_rename(
            PROJECT, expected_generation=GENERATION, source="some-directory", destination="d2",
            expected_source_sha256=digest(b""),
        )
    assert raised.value.code == "not_found"
    port.files["old.txt"] = b"mutated"
    with pytest.raises(ServiceError) as raised:
        rename_plan(service, "old.txt", "new.txt", b"one")
    assert raised.value.code == "source_changed"
    assert port.renames == []


def test_rename_stale_apply_rejected() -> None:
    service, port = editor({"old.txt": b"one"})
    plan = rename_plan(service, "old.txt", "new.txt", b"one")
    port.files["old.txt"] = b"two"
    with pytest.raises(ServiceError) as raised:
        service.apply_rename(plan)
    assert raised.value.code == "source_changed"
    assert port.renames == []


def test_duplicate_apply_calls_mutation_port_once() -> None:
    service, port = editor({"notes.md": b"one"})
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, 0, "two"))
    assert service.apply_patch(plan)["status"] == "applied"
    assert service.apply_patch(plan)["already_applied"] is True
    assert len(port.writes) == 1  # 同operation只调一次写端口
    service, port = editor({"old.txt": b"one"})
    plan = rename_plan(service, "old.txt", "new.txt", b"one")
    service.apply_rename(plan)
    service.apply_rename(plan)
    assert len(port.renames) == 1


def test_unknown_outcome_is_not_replayed_until_resolved() -> None:
    service, port = editor({"notes.md": b"one"})
    port.write_error = RuntimeError("transport lost")
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError) as raised:
        service.apply_patch(plan)
    assert raised.value.code == "sandbox_operation_unknown"
    assert len(port.writes) == 1
    with pytest.raises(ServiceError) as raised:  # 未查询前重试被拒绝且不再调用端口
        service.apply_patch(plan)
    assert raised.value.code == "sandbox_operation_unknown"
    assert len(port.writes) == 1
    assert service.resolve_patch_outcome(plan) == "unapplied"
    port.write_error = None
    result = service.apply_patch(plan)  # 显式确认未应用后允许一次新的写调用
    assert result["status"] == "applied"
    assert len(port.writes) == 2 and port.files["notes.md"] == b"two"


def test_unknown_outcome_resolved_as_applied_via_journal() -> None:
    journal = FakeJournal()
    service, port = editor({"notes.md": b"one"}, journal)
    port.write_error = RuntimeError("timeout after commit")
    port.apply_before_error = True  # 端口异常但内容已落地
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError):
        service.apply_patch(plan)
    assert journal.entries[-1] == (plan.plan_id, "unknown")
    assert service.resolve_patch_outcome(plan) == "applied"
    assert journal.entries[-1] == (plan.plan_id, "applied")
    assert service.operation_status(plan.plan_id)["status"] == "applied"
    fresh, fresh_port = editor({"notes.md": b"two"}, journal)  # 新实例共享journal后同样短路
    assert fresh.apply_patch(plan)["already_applied"] is True
    assert fresh_port.writes == []


def test_rename_unknown_outcome_resolution() -> None:
    service, port = editor({"old.txt": b"one"})
    port.write_error = RuntimeError("lost")
    plan = rename_plan(service, "old.txt", "new.txt", b"one")
    with pytest.raises(ServiceError) as raised:
        service.apply_rename(plan)
    assert raised.value.code == "sandbox_operation_unknown"
    assert len(port.renames) == 1 and "new.txt" not in port.files
    with pytest.raises(ServiceError):
        service.apply_rename(plan)
    assert len(port.renames) == 1
    assert service.resolve_rename_outcome(plan) == "unapplied"
    port.write_error = None
    assert service.apply_rename(plan)["status"] == "applied"
    assert port.files == {"new.txt": b"one"}
    assert len(port.renames) == 2


def test_port_rejection_is_safe_to_retry() -> None:
    journal = FakeJournal()
    service, port = editor({"notes.md": b"one"}, journal)
    port.write_error = ServiceError("sandbox_limit")  # 端口确定性拒绝
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError) as raised:
        service.apply_patch(plan)
    assert raised.value.code == "sandbox_limit"
    assert journal.entries[-1] == (plan.plan_id, "rejected")
    port.write_error = None
    result = service.apply_patch(plan)
    assert result["status"] == "applied"
    assert len(port.writes) == 2 and port.files["notes.md"] == b"two"  # 首次被拒+重试成功


def test_apply_passes_port_operation_id_through() -> None:
    service, port = editor({"notes.md": b"one"})
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, 0, "two"))
    assert service.apply_patch(plan, port_operation_id="lease-1")["status"] == "applied"
    assert port.writes[0][0] == "lease-1"  # 端口侧operation_id为宿主受理lease_id
    assert service.apply_patch(plan)["already_applied"] is True  # 去重仍按plan_id
    assert len(port.writes) == 1
    service, port = editor({"old.txt": b"one"})
    plan = rename_plan(service, "old.txt", "new.txt", b"one")
    assert service.apply_rename(plan, port_operation_id="lease-2")["status"] == "applied"
    assert port.renames[0][0] == "lease-2"
    next_plan = rename_plan(service, "new.txt", "final.txt", b"one")
    with pytest.raises(ServiceError) as raised:
        service.apply_rename(next_plan, port_operation_id=3)  # type: ignore[arg-type]
    assert raised.value.code == "invalid_input"
    assert port.renames == [(
        "lease-2", "old.txt", "new.txt", digest(b"one"), None
    )]


def test_post_commit_service_error_classified_by_readback() -> None:
    """端口落盘后才因租约/generation复核抛ServiceError：读回核实，不得记为未写。"""
    journal = FakeJournal()
    port = CommitThenRecheckMutation(
        {"notes.md": b"one"}, recheck_error=ServiceError("permission_denied", 403)
    )
    service = ProjectFileEditor(port, journal)
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError) as raised:
        service.apply_patch(plan)
    assert raised.value.code == "permission_denied"  # 原端口错误原样上抛
    assert journal.entries[-1] == (plan.plan_id, "applied")  # 读回证实已落地
    assert port.files["notes.md"] == b"two"
    assert service.apply_patch(plan)["already_applied"] is True  # 重试不重复写
    assert len(port.writes) == 1


def test_post_commit_rename_service_error_resolves_applied_not_not_found() -> None:
    """rename落盘后围栏抛错：记applied，重试短路而非not_found死路。"""
    journal = FakeJournal()
    port = CommitThenRecheckMutation(
        {"old.txt": b"one"}, recheck_error=ServiceError("input_revision_conflict")
    )
    service = ProjectFileEditor(port, journal)
    plan = rename_plan(service, "old.txt", "new.txt", b"one")
    with pytest.raises(ServiceError):
        service.apply_rename(plan)
    assert journal.entries[-1] == (plan.plan_id, "applied")
    assert port.files == {"new.txt": b"one"}
    assert service.apply_rename(plan)["already_applied"] is True
    assert len(port.renames) == 1


def test_port_declared_unknown_records_unknown_until_resolved() -> None:
    """端口声明sandbox_operation_unknown即记unknown，不读回降级为rejected。"""
    journal = FakeJournal()
    port = CommitThenRecheckMutation(
        {"notes.md": b"one"}, recheck_error=ServiceError("sandbox_operation_unknown")
    )
    service = ProjectFileEditor(port, journal)
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError) as raised:
        service.apply_patch(plan)
    assert raised.value.code == "sandbox_operation_unknown"
    assert journal.entries[-1] == (plan.plan_id, "unknown")  # 已落盘也不自动记applied
    assert service.resolve_patch_outcome(plan) == "applied"  # 显式只读核实
    assert journal.entries[-1] == (plan.plan_id, "applied")
    journal = FakeJournal()
    service, port = editor({"notes.md": b"one"}, journal)
    port.write_error = ServiceError("sandbox_operation_unknown")  # 未落盘的端口未知
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError):
        service.apply_patch(plan)
    assert journal.entries[-1] == (plan.plan_id, "unknown")  # 不得按现值降级为rejected
    assert service.resolve_patch_outcome(plan) == "unapplied"
    port.write_error = None
    assert service.apply_patch(plan)["status"] == "applied"


def test_service_error_with_unprovable_outcome_blocks_until_diverged() -> None:
    """报错时点无法证实落地状态（并发改写）：记unknown并封锁，diverged不解除。"""
    journal = FakeJournal()
    port = CommitThenRecheckMutation(
        {"notes.md": b"one"},
        recheck_error=ServiceError("permission_denied", 403),
        on_recheck=lambda: port.files.__setitem__("notes.md", b"tampered"),
    )
    service = ProjectFileEditor(port, journal)
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError):
        service.apply_patch(plan)
    assert journal.entries[-1] == (plan.plan_id, "unknown")  # 既非源也非目标
    with pytest.raises(ServiceError) as raised:
        service.apply_patch(plan)
    assert raised.value.code == "sandbox_operation_unknown"
    assert service.resolve_patch_outcome(plan) == "diverged"  # 不记录、不重写
    assert journal.entries[-1] == (plan.plan_id, "unknown")


def test_service_error_readback_failure_records_unknown() -> None:
    """读回核实通道本身故障（如generation复核拦截读端口）：一律按unknown。"""
    journal = FakeJournal()
    port = CommitThenRecheckMutation(
        {"notes.md": b"one"}, recheck_error=ServiceError("input_revision_conflict")
    )

    def lose_reads() -> None:
        port.read_error = ServiceError("input_revision_conflict")

    port.on_recheck = lose_reads  # 模拟真实端口读回时再次复核租约/generation失败
    service = ProjectFileEditor(port, journal)
    plan = service.plan_patch(PROJECT, **patch_kwargs(b"one", 0, len(b"one"), "two"))
    with pytest.raises(ServiceError):
        service.apply_patch(plan)
    assert journal.entries[-1] == (plan.plan_id, "unknown")
    with pytest.raises(ServiceError):
        service.apply_patch(plan)
    assert len(port.writes) == 1  # 封锁期间不再触端口


def test_rename_plan_rejects_occupied_destination_at_plan_time() -> None:
    service, port = editor({"old.txt": b"body", "new.txt": b"occupied"})
    with pytest.raises(ServiceError) as raised:
        rename_plan(service, "old.txt", "new.txt", b"body")
    assert raised.value.code == "key_conflict"
    with pytest.raises(ServiceError) as raised:  # 自移动同样在计划期拒绝
        rename_plan(service, "old.txt", "old.txt", b"body")
    assert raised.value.code == "key_conflict"
    assert port.renames == [] and port.writes == []


def test_rename_destination_created_between_plan_and_apply_rejected() -> None:
    service, port = editor({"old.txt": b"body"})
    plan = rename_plan(service, "old.txt", "new.txt", b"body")
    port.files["new.txt"] = b"appeared"  # 计划后目标被占用
    with pytest.raises(ServiceError) as raised:
        service.apply_rename(plan)
    assert raised.value.code == "key_conflict"
    assert port.renames == []  # apply复核仍拒绝，不触端口


def test_patch_plan_rejects_result_over_file_limit() -> None:
    full = b"a" * MAX_PATCH_FILE_BYTES
    service, port = editor({"notes.md": full})
    with pytest.raises(ServiceError) as raised:  # 末尾追加16KiB，结果超过4MiB
        service.plan_patch(
            PROJECT, **patch_kwargs(full, MAX_PATCH_FILE_BYTES, 0, "x" * MAX_REPLACEMENT_BYTES)
        )
    assert raised.value.code == "sandbox_limit"
    assert port.writes == []


def test_resolve_rename_outcome_rewritten_destination_is_diverged() -> None:
    """目标存在还须内容与计划源hash一致；被改写或独立创建均判diverged。"""
    service, port = editor({"old.txt": b"one"})
    plan = rename_plan(service, "old.txt", "new.txt", b"one")
    port.files = {"new.txt": b"one"}  # rename已落地且目标未被改动
    assert service.resolve_rename_outcome(plan) == "applied"
    port.files = {"new.txt": b"rewritten"}  # 目标被后续改写：无法证明原rename落地
    assert service.resolve_rename_outcome(plan) == "diverged"
    port.files = {"new.txt": b"other", "old.txt": b"one"}  # 目标为独立创建且源仍在
    assert service.resolve_rename_outcome(plan) == "diverged"


def test_module_has_no_http_surface() -> None:
    module = sys.modules["krim_agent.sandbox.file_editing"]
    source = inspect.getsource(module)
    assert "fastapi" not in source and "APIRouter" not in source
    assert inspect.isclass(ProjectFileEditor)
