"""条件文件事务：身份/哈希检查、Linux原子交换与可核实回执；ADR-0055。"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import stat
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cache
from typing import Literal, cast
from uuid import uuid4

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import (
    FileChangePreview,
    FileChangeReceipt,
    FileChangeSpec,
    RestoreFileSpec,
    WorkspaceFileState,
)


@dataclass(frozen=True)
class FileSnapshot:
    data: bytes
    mode: int
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int
    uid: int
    gid: int

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()

    @property
    def identity(self) -> tuple[int, int]:
        return self.device, self.inode


@dataclass(frozen=True)
class CapturedFile:
    path: str
    workspace_fingerprint: str
    parent_identity: tuple[int, int]
    snapshot: FileSnapshot | None


@dataclass(frozen=True)
class PreparedFileChange:
    spec_json: str
    workspace_fingerprint: str
    parent_identity: tuple[int, int]
    before: FileSnapshot | None
    after: bytes | None
    token: str

    @property
    def spec(self) -> FileChangeSpec | RestoreFileSpec:
        # 外部拿到的模型副本不能改写已冻结的预览意图。
        if json.loads(self.spec_json).get("operation") == "restore":
            return RestoreFileSpec.model_validate_json(self.spec_json)
        return FileChangeSpec.model_validate_json(self.spec_json)

    @property
    def before_sha256(self) -> str | None:
        return self.before.sha256 if self.before is not None else None

    @property
    def after_sha256(self) -> str | None:
        return hashlib.sha256(self.after).hexdigest() if self.after is not None else None

    def preview(self) -> FileChangePreview:
        before = self.before.data.decode("utf-8") if self.before is not None else ""
        after = self.after.decode("utf-8") if self.after is not None else ""
        diff, truncated = bounded_diff(self.spec.path, before, after)
        result = FileChangePreview(
            path=self.spec.path,
            operation=("delete" if self.after is None else "write")
            if self.spec.operation == "restore"
            else self.spec.operation,
            before_sha256=self.before_sha256,
            after_sha256=self.after_sha256,
            before_bytes=len(self.before.data) if self.before is not None else 0,
            after_bytes=len(self.after) if self.after is not None else 0,
            diff=diff,
            truncated=truncated,
        )
        while len(result.model_dump_json().encode("utf-8")) > 24000:
            result = result.model_copy(
                update={
                    "diff": result.diff[: len(result.diff) // 2],
                    "truncated": True,
                }
            )
        return result


@dataclass(frozen=True)
class FileRecovery:
    workspace_fingerprint: str
    path: str
    stage_name: str
    parent_identity: tuple[int, int]


@dataclass(frozen=True)
class FileEffect:
    receipt: FileChangeReceipt
    recovery: FileRecovery | None = None
    observed_after: WorkspaceFileState | None = None


def observed_state(snapshot: FileSnapshot | None) -> WorkspaceFileState:
    """显式空状态表示确认不存在；None留给没有实际观察的旧回执。"""
    return WorkspaceFileState(
        sha256=snapshot.sha256 if snapshot is not None else None,
        mode=snapshot.mode if snapshot is not None else None,
        uid=snapshot.uid if snapshot is not None else None,
        gid=snapshot.gid if snapshot is not None else None,
    )


def bounded_diff(path: str, before: str, after: str) -> tuple[str, bool]:
    """单个真实统一差异区间；线性首尾比较，避免对抗输入触发二次方对齐。"""

    def to_lines(value: str) -> list[str]:
        pieces = value.split("\n")
        return [piece + "\n" for piece in pieces[:-1]] + ([pieces[-1]] if pieces[-1] else [])

    old, new = to_lines(before), to_lines(after)
    left, right = 0, 0
    while left < min(len(old), len(new)) and old[left] == new[left]:
        left += 1
    while right < min(len(old), len(new)) - left and old[-right - 1] == new[-right - 1]:
        right += 1
    if left == len(old) == len(new):
        return "", False
    start = max(0, left - 3)
    old_end = min(len(old), len(old) - right + 3)
    new_end = min(len(new), len(new) - right + 3)
    parts = [f"--- a/{path}\n+++ b/{path}\n"]
    parts.append(
        f"@@ -{start + 1 if old_end > start else start},{old_end - start} "
        f"+{start + 1 if new_end > start else start},{new_end - start} @@\n"
    )
    size = len("".join(parts).encode("utf-8"))
    truncated = False
    for marker, lines in (("-", old[start:old_end]), ("+", new[start:new_end])):
        for line in lines:
            value = marker + line
            if not line.endswith("\n"):
                value += "\n\\ No newline at end of file\n"
            raw = value.encode("utf-8")
            if size + len(raw) > 12000:
                parts.append(raw[: max(0, 12000 - size)].decode("utf-8", errors="ignore"))
                truncated = True
                break
            parts.append(value)
            size += len(raw)
        if truncated:
            break
    return "".join(parts), truncated


@cache
def _rename_function() -> Callable[[int, bytes, int, bytes, int], int]:
    if not sys.platform.startswith("linux"):
        raise ServiceError("workspace_write_platform", "当前平台尚未实现条件文件写入", 503)
    library = ctypes.CDLL(None, use_errno=True)
    function = getattr(library, "renameat2", None)
    if function is None:
        raise ServiceError("workspace_write_platform", "当前系统缺少renameat2文件原语", 503)
    function.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    function.restype = ctypes.c_int
    return cast(Callable[[int, bytes, int, bytes, int], int], function)


def rename_at(parent_fd: int, source: str, target: str, *, exchange: bool = False) -> None:
    # Linux RENAME_EXCHANGE=2，RENAME_NOREPLACE=1；不退化为可覆盖路径的普通rename。
    result = _rename_function()(
        parent_fd, os.fsencode(source), parent_fd, os.fsencode(target), 2 if exchange else 1
    )
    if result != 0:
        code = ctypes.get_errno()
        raise OSError(code, "条件文件重命名失败")


def _identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


class WorkspaceWriter:
    def __init__(self, workspace: Workspace, *, lock_timeout: float = 5) -> None:
        if not 0 < lock_timeout <= 30:
            raise ValueError("文件协作锁超时须在0–30秒之间")
        self.workspace = workspace
        self.lock_timeout = lock_timeout

    def _parent(self, path: str) -> tuple[int, str, str]:
        parts = self.workspace._parts(path)
        if not parts:
            raise ServiceError("workspace_path", "文件修改须指定相对文件路径", 422)
        parent = "/".join(parts[:-1]) or "."
        return self.workspace._open(parent, directory=True), parent, parts[-1]

    def _snapshot(
        self, parent_fd: int, name: str, *, missing: bool = False, own_links: int = 1
    ) -> FileSnapshot | None:
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
        except FileNotFoundError:
            if missing:
                return None
            raise ServiceError("workspace_not_found", "文件条目不存在", 404) from None
        except OSError:
            raise ServiceError("workspace_access", "文件不能打开或包含链接", 403) from None
        try:
            initial = os.fstat(fd)
            if not stat.S_ISREG(initial.st_mode):
                raise ServiceError("workspace_file_type", "修改只支持普通文件", 422)
            if initial.st_nlink != own_links:
                raise ServiceError("workspace_link", "修改拒绝多硬链接文件", 403)
            if initial.st_size > self.workspace.max_file_bytes:
                raise ServiceError("workspace_file_limit", "文件超过工作区容量限制", 413)
            parts, size = [], 0
            while size <= self.workspace.max_file_bytes:
                value = os.read(fd, min(65536, self.workspace.max_file_bytes + 1 - size))
                if not value:
                    break
                parts.append(value)
                size += len(value)
            raw = b"".join(parts)
            final = os.fstat(fd)
            if size > self.workspace.max_file_bytes:
                raise ServiceError("workspace_file_limit", "文件超过工作区容量限制", 413)
            if (
                initial.st_size,
                initial.st_mtime_ns,
                initial.st_ctime_ns,
                initial.st_nlink,
                initial.st_uid,
                initial.st_gid,
                initial.st_mode,
            ) != (
                final.st_size,
                final.st_mtime_ns,
                final.st_ctime_ns,
                final.st_nlink,
                final.st_uid,
                final.st_gid,
                final.st_mode,
            ) or initial.st_size != size:
                raise ServiceError("workspace_changed", "文件读取期间变化", 409)
            self._text(raw)
            return FileSnapshot(
                raw,
                stat.S_IMODE(final.st_mode),
                final.st_dev,
                final.st_ino,
                final.st_mtime_ns,
                final.st_ctime_ns,
                final.st_uid,
                final.st_gid,
            )
        except OSError:
            raise ServiceError("workspace_access", "文件快照读取失败", 403) from None
        finally:
            os.close(fd)

    def _text(self, raw: bytes) -> str:
        if len(raw) > self.workspace.max_file_bytes:
            raise ServiceError("workspace_file_limit", "修改结果超过工作区容量限制", 413)
        if b"\0" in raw:
            raise ServiceError("workspace_binary", "修改不支持二进制内容", 422)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            raise ServiceError("workspace_encoding", "修改内容须为UTF-8", 422) from None

    def capture_file(
        self, path: str, *, authorize: Callable[[], None] | None = None
    ) -> CapturedFile:
        """捕获完整有界文件或明确缺失；不发布文件修改。"""
        with self._locked_parent(path, lambda: False) as (fd, parent, name):
            if authorize is not None:
                authorize()
            identity = _identity(os.fstat(fd))
            snapshot = self._snapshot(fd, name, missing=True)
            self._check_parent(parent, identity)
            return CapturedFile(path, self.workspace.fingerprint, identity, snapshot)

    def verify_captured(
        self,
        captures: tuple[CapturedFile, ...],
        *,
        authorize: Callable[[], None] | None = None,
    ) -> None:
        """全部捕获后在同一协作锁内检查未变化，不把零散读取冒充可用快照。"""
        if not 1 <= len(captures) <= 32:
            raise ServiceError("workspace_checkpoint_files", "检查点文件数量无效", 422)
        with self._locked_parent(captures[0].path, lambda: False):
            if authorize is not None:
                authorize()
            for capture in captures:
                if capture.workspace_fingerprint != self.workspace.fingerprint:
                    raise ServiceError("workspace_binding_changed", "检查点不属于当前工作区", 409)
                fd, parent, name = self._parent(capture.path)
                try:
                    self._check_parent(parent, capture.parent_identity)
                    actual = self._snapshot(fd, name, missing=True)
                    if not self._same_before(actual, capture.snapshot):
                        raise ServiceError(
                            "workspace_checkpoint_changed", "文件在检查点捕获期间变化", 409
                        )
                    self._check_parent(parent, capture.parent_identity)
                finally:
                    os.close(fd)
            if authorize is not None:
                authorize()

    def prepare(self, spec: FileChangeSpec) -> PreparedFileChange:
        request = FileChangeSpec.model_validate(spec.model_dump())
        fd, _parent, name = self._parent(request.path)
        try:
            before = self._snapshot(fd, name, missing=True)
            actual = before.sha256 if before is not None else None
            if actual != request.expected_sha256:
                raise ServiceError("workspace_conflict", "文件原版本与预期SHA-256不符", 409)
            after: bytes | None = None
            if request.operation == "write":
                assert request.content is not None
                try:
                    after = request.content.encode("utf-8")
                except UnicodeEncodeError:
                    raise ServiceError("workspace_encoding", "修改内容须为UTF-8", 422) from None
            elif request.operation == "replace":
                assert before is not None
                content = self._text(before.data)
                for replacement in request.replacements:
                    if content.count(replacement.old_text) != replacement.occurrences:
                        raise ServiceError(
                            "workspace_replace_conflict", "替换原文出现次数不符", 409
                        )
                    content = content.replace(replacement.old_text, replacement.new_text)
                after = content.encode("utf-8")
            if after is not None:
                self._text(after)
            return PreparedFileChange(
                request.model_dump_json(),
                self.workspace.fingerprint,
                _identity(os.fstat(fd)),
                before,
                after,
                uuid4().hex,
            )
        finally:
            os.close(fd)

    def prepare_restore(self, original: PreparedFileChange) -> PreparedFileChange:
        """从冻结快照恢复完整旧内容，仍核验当前应用后版本与原父目录。"""
        if original.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "撤销不属于当前工作区", 409)
        fd, parent, name = self._parent(original.spec.path)
        try:
            self._check_parent(parent, original.parent_identity)
            before = self._snapshot(fd, name, missing=True)
            if (before.data if before is not None else None) != original.after:
                raise ServiceError("workspace_conflict", "撤销目标已偏离原应用后版本", 409)
            after = original.before.data if original.before is not None else None
            if after is not None:
                self._text(after)
            spec = RestoreFileSpec(
                path=original.spec.path,
                expected_sha256=before.sha256 if before is not None else None,
                mode=original.before.mode if original.before is not None else None,
                uid=original.before.uid if original.before is not None else None,
                gid=original.before.gid if original.before is not None else None,
            )
            return PreparedFileChange(
                spec.model_dump_json(),
                self.workspace.fingerprint,
                original.parent_identity,
                before,
                after,
                uuid4().hex,
            )
        finally:
            os.close(fd)

    def prepare_checkpoint_restore(
        self, current: CapturedFile, target: CapturedFile
    ) -> PreparedFileChange:
        """从两份可信完整快照冻结恢复意图；此方法不读取或修改文件。"""
        if (
            current.path != target.path
            or current.workspace_fingerprint != self.workspace.fingerprint
            or target.workspace_fingerprint != self.workspace.fingerprint
            or current.parent_identity != target.parent_identity
        ):
            raise ServiceError(
                "workspace_checkpoint_binding", "恢复目标路径、目录或工作区已改变", 409
            )
        if any(
            item.snapshot is not None and item.snapshot.mode & ~0o777 for item in (current, target)
        ):
            raise ServiceError("workspace_checkpoint_mode", "当前恢复不支持设置特殊权限位", 409)
        old, desired = current.snapshot, target.snapshot
        spec = RestoreFileSpec(
            path=current.path,
            expected_sha256=old.sha256 if old is not None else None,
            mode=desired.mode if desired is not None else None,
            uid=desired.uid if desired is not None else None,
            gid=desired.gid if desired is not None else None,
        )
        return PreparedFileChange(
            spec.model_dump_json(),
            self.workspace.fingerprint,
            current.parent_identity,
            old,
            desired.data if desired is not None else None,
            uuid4().hex,
        )

    @staticmethod
    def _same_before(
        actual: FileSnapshot | None, wanted: FileSnapshot | None, *, displaced: bool = False
    ) -> bool:
        if wanted is None or actual is None:
            return wanted is actual
        return (
            actual.identity == wanted.identity
            and actual.sha256 == wanted.sha256
            and actual.mode == wanted.mode
            and actual.uid == wanted.uid
            and actual.gid == wanted.gid
            and actual.mtime_ns == wanted.mtime_ns
            and (displaced or actual.ctime_ns == wanted.ctime_ns)
        )

    def _check_parent(self, parent: str, wanted: tuple[int, int]) -> None:
        fd = self.workspace._open(parent, directory=True)
        try:
            if _identity(os.fstat(fd)) != wanted:
                raise ServiceError("workspace_parent_changed", "文件父目录身份已变化", 409)
        finally:
            os.close(fd)

    @contextmanager
    def _locked_parent(
        self, path: str, abort: Callable[[], bool]
    ) -> Iterator[tuple[int, str, str]]:
        import fcntl

        deadline = time.monotonic() + self.lock_timeout
        while not self.workspace._write_lock.acquire(timeout=0.01):
            if abort():
                raise ServiceError("workspace_write_cancelled", "文件修改在发布前取消", 409)
            if time.monotonic() >= deadline:
                raise ServiceError("workspace_write_busy", "文件协作锁等待超时", 429)
        try:
            if not self.workspace.writable:
                raise ServiceError("workspace_read_only", "宿主未授权工作区写能力", 403)
            anchor = self.workspace._open(".", directory=True)
            try:
                mutex = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=anchor)
            finally:
                os.close(anchor)
            try:
                while True:
                    if abort():
                        raise ServiceError("workspace_write_cancelled", "文件修改在发布前取消", 409)
                    try:
                        fcntl.flock(mutex, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise ServiceError(
                                "workspace_write_busy", "文件协作锁等待超时", 429
                            ) from None
                        time.sleep(0.01)
                fd, parent, name = self._parent(path)
                try:
                    yield fd, parent, name
                finally:
                    os.close(fd)
            finally:
                os.close(mutex)
        finally:
            self.workspace._write_lock.release()

    @staticmethod
    def _receipt(
        change: PreparedFileChange,
        status: Literal["done", "rejected", "uncertain"],
        code: str | None = None,
        *,
        recovery: FileRecovery | None = None,
        changed: bool = False,
        observed_after: WorkspaceFileState | None = None,
    ) -> FileEffect:
        return FileEffect(
            FileChangeReceipt(
                path=change.spec.path,
                status=status,
                before_sha256=change.before_sha256,
                after_sha256=change.after_sha256,
                changed=changed,
                error_code=code,
                recovery_required=status == "uncertain",
            ),
            recovery,
            observed_after,
        )

    def apply(
        self,
        change: PreparedFileChange,
        *,
        abort: Callable[[], bool] | None = None,
        authorize: Callable[[], None] | None = None,
        retain_before: bool = True,
    ) -> FileEffect:
        cancelled = abort or (lambda: False)
        if change.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "修改提案不属于当前工作区", 409)
        if len(change.token) != 32 or any(c not in "0123456789abcdef" for c in change.token):
            raise ServiceError("workspace_change_invalid", "修改提案内部标识无效", 422)
        _rename_function()
        if change.after is not None:
            self._text(change.after)
        with self._locked_parent(change.spec.path, cancelled) as (fd, parent, name):
            # 校验在文件锁内，核实者不能在校验与发布之间抢先认定“没有修改”。
            if authorize is not None:
                try:
                    authorize()
                except ServiceError as error:
                    return self._receipt(change, "rejected", error.code)
            self._check_parent(parent, change.parent_identity)
            current = self._snapshot(fd, name, missing=True)
            if not self._same_before(current, change.before):
                return self._receipt(change, "rejected", "workspace_conflict")
            if self._no_op(change, current):
                return self._receipt(change, "done", observed_after=observed_state(current))
            stage = f"._krim-tx-{change.token}"
            recovery = FileRecovery(
                self.workspace.fingerprint, change.spec.path, stage, change.parent_identity
            )
            stage_owned: tuple[int, int] | None = None
            attempted = False
            published = False
            try:
                if change.after is not None:
                    stage_fd = os.open(
                        stage,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                        0o600,
                        dir_fd=fd,
                    )
                    try:
                        stage_owned = _identity(os.fstat(stage_fd))
                        remaining = memoryview(change.after)
                        while remaining:
                            count = os.write(stage_fd, remaining)
                            if count <= 0:
                                raise OSError(errno.EIO, "临时文件写入未完成")
                            remaining = remaining[count:]
                        mode, uid, gid = self._attributes(change)
                        if uid is not None and gid is not None:
                            info = os.fstat(stage_fd)
                            if (info.st_uid, info.st_gid) != (uid, gid):
                                os.fchown(stage_fd, uid, gid)
                        os.fchmod(stage_fd, mode)
                        os.fsync(stage_fd)
                    finally:
                        os.close(stage_fd)
                    os.fsync(fd)
                self._check_parent(parent, change.parent_identity)
                if authorize is not None:
                    authorize()
                if cancelled():
                    if stage_owned is not None:
                        self._remove_owned(fd, stage, stage_owned)
                        os.fsync(fd)
                    return self._receipt(change, "rejected", "workspace_write_cancelled")
                # 原子操作前再次核验；交换后仍检查实际移走的旧条目。
                if not self._same_before(self._snapshot(fd, name, missing=True), change.before):
                    if stage_owned is not None:
                        self._remove_owned(fd, stage, stage_owned)
                    return self._receipt(change, "rejected", "workspace_conflict")
                attempted = True
                if change.before is None:
                    os.link(stage, name, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
                elif change.after is None:
                    rename_at(fd, name, stage)
                else:
                    rename_at(fd, stage, name, exchange=True)
                published = True
                if change.before is not None:
                    displaced = self._snapshot(fd, stage)
                    if not self._same_before(displaced, change.before, displaced=True):
                        return self._restore_conflict(
                            change, fd, parent, name, stage, displaced, stage_owned, recovery
                        )
                desired = self._snapshot(
                    fd, name, missing=True, own_links=2 if change.before is None else 1
                )
                if not self._desired(change, desired, stage_owned):
                    return self._receipt(
                        change,
                        "uncertain",
                        "workspace_postcheck_failed",
                        recovery=recovery,
                        changed=True,
                    )
                self._check_parent(parent, change.parent_identity)
                os.fsync(fd)
                # 清理前确认暂存条目身份；它可能是真实旧文件，不能按名字盲删。
                if change.before is None:
                    assert stage_owned is not None
                    self._remove_owned(fd, stage, stage_owned)
                elif not retain_before:
                    expected_stage = change.before.identity
                    if not self._same_before(
                        self._snapshot(fd, stage), change.before, displaced=True
                    ):
                        return self._receipt(
                            change,
                            "uncertain",
                            "workspace_stage_changed",
                            recovery=recovery,
                            changed=True,
                        )
                    self._remove_owned(fd, stage, expected_stage)
                os.fsync(fd)
                final = self._snapshot(fd, name, missing=True)
                if not self._desired(change, final, stage_owned):
                    return self._receipt(
                        change,
                        "uncertain",
                        "workspace_postcheck_failed",
                        recovery=recovery,
                        changed=True,
                    )
                return self._receipt(
                    change,
                    "done",
                    changed=True,
                    recovery=recovery if change.before is not None and retain_before else None,
                    observed_after=observed_state(final),
                )
            except FileExistsError:
                if published:
                    return self._receipt(
                        change,
                        "uncertain",
                        "workspace_write_uncertain",
                        recovery=recovery,
                        changed=True,
                    )
                if attempted and change.before is not None:
                    try:
                        still_before = self._same_before(
                            self._snapshot(fd, name, missing=True), change.before
                        )
                    except (OSError, ServiceError):
                        still_before = False
                    if not still_before:
                        return self._receipt(
                            change,
                            "uncertain",
                            "workspace_write_uncertain",
                            recovery=recovery,
                            changed=True,
                        )
                # 新建/不覆盖删除的EEXIST确认本次发布未覆盖原条目。
                if stage_owned is not None:
                    try:
                        self._remove_owned(fd, stage, stage_owned)
                    except (OSError, ServiceError):
                        return self._receipt(
                            change, "uncertain", "workspace_stage_cleanup_failed", recovery=recovery
                        )
                return self._receipt(change, "rejected", "workspace_conflict")
            except (OSError, ServiceError) as error:
                code = error.code if isinstance(error, ServiceError) else "workspace_write_io"
                if attempted:
                    return self._receipt(change, "uncertain", code, recovery=recovery, changed=True)
                if stage_owned is not None:
                    try:
                        self._remove_owned(fd, stage, stage_owned)
                    except (OSError, ServiceError):
                        return self._receipt(
                            change, "uncertain", "workspace_stage_cleanup_failed", recovery=recovery
                        )
                return self._receipt(change, "rejected", code)

    @staticmethod
    def _remove_owned(fd: int, stage: str, identity: tuple[int, int]) -> None:
        info = os.stat(stage, dir_fd=fd, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or _identity(info) != identity:
            raise ServiceError("workspace_stage_changed", "内部恢复条目身份已变化", 409)
        os.unlink(stage, dir_fd=fd)

    @staticmethod
    def _attributes(change: PreparedFileChange) -> tuple[int, int | None, int | None]:
        spec = change.spec
        if isinstance(spec, RestoreFileSpec) and spec.mode is not None:
            return spec.mode & 0o777, spec.uid, spec.gid
        if change.before is not None:
            return change.before.mode & 0o777, change.before.uid, change.before.gid
        return 0o644, None, None

    @staticmethod
    def _desired(
        change: PreparedFileChange, actual: FileSnapshot | None, identity: tuple[int, int] | None
    ) -> bool:
        if change.after is None:
            return actual is None
        mode, uid, gid = WorkspaceWriter._attributes(change)
        return (
            actual is not None
            and actual.data == change.after
            and actual.identity == identity
            and actual.mode == mode
            and (uid is None or (actual.uid, actual.gid) == (uid, gid))
        )

    def _restore_conflict(
        self,
        change: PreparedFileChange,
        fd: int,
        parent: str,
        name: str,
        stage: str,
        displaced: FileSnapshot | None,
        new_identity: tuple[int, int] | None,
        recovery: FileRecovery,
    ) -> FileEffect:
        if displaced is None:
            return self._receipt(
                change, "uncertain", "workspace_write_uncertain", recovery=recovery, changed=True
            )
        actual = self._snapshot(fd, name, missing=True)
        if change.after is None:
            if actual is not None:
                return self._receipt(
                    change,
                    "uncertain",
                    "workspace_write_uncertain",
                    recovery=recovery,
                    changed=True,
                )
            rename_at(fd, stage, name)
        else:
            if not self._desired(change, actual, new_identity):
                return self._receipt(
                    change,
                    "uncertain",
                    "workspace_write_uncertain",
                    recovery=recovery,
                    changed=True,
                )
            rename_at(fd, stage, name, exchange=True)
            moved_new = self._snapshot(fd, stage)
            if not self._desired(change, moved_new, new_identity):
                return self._receipt(
                    change,
                    "uncertain",
                    "workspace_write_uncertain",
                    recovery=recovery,
                    changed=True,
                )
        restored = self._snapshot(fd, name)
        if not self._same_before(restored, displaced, displaced=True):
            return self._receipt(
                change, "uncertain", "workspace_write_uncertain", recovery=recovery, changed=True
            )
        self._check_parent(parent, change.parent_identity)
        os.fsync(fd)
        if change.after is not None:
            assert new_identity is not None
            self._remove_owned(fd, stage, new_identity)
            os.fsync(fd)
        return self._receipt(change, "rejected", "workspace_conflict")

    def inspect_recovery(
        self, reference: FileRecovery
    ) -> tuple[FileSnapshot | None, FileSnapshot | None]:
        """可信宿主只读取原目标/暂存材料；不在核实中自动重新执行修改。"""
        if reference.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "恢复材料不属于当前工作区", 409)
        if (
            len(reference.stage_name) != len("._krim-tx-") + 32
            or not reference.stage_name.startswith("._krim-tx-")
            or any(c not in "0123456789abcdef" for c in reference.stage_name[len("._krim-tx-") :])
        ):
            raise ServiceError("workspace_change_invalid", "恢复引用无效", 422)
        fd, parent, name = self._parent(reference.path)
        try:
            self._check_parent(parent, reference.parent_identity)
            return self._snapshot(fd, name, missing=True), self._snapshot(
                fd, reference.stage_name, missing=True
            )
        finally:
            os.close(fd)

    @staticmethod
    def _content_matches(snapshot: FileSnapshot | None, data: bytes | None) -> bool:
        return snapshot is None if data is None else snapshot is not None and snapshot.data == data

    @classmethod
    def _no_op(cls, change: PreparedFileChange, current: FileSnapshot | None) -> bool:
        return cls._content_matches(current, change.after) and (
            not isinstance(change.spec, RestoreFileSpec)
            or change.after is None
            or current is not None
            and (current.mode, current.uid, current.gid) == cls._attributes(change)
        )

    def verify(
        self,
        change: PreparedFileChange,
        reference: FileRecovery | None,
        *,
        keeping: bool = False,
        expected_after: WorkspaceFileState | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> FileEffect:
        """只读取目标/暂存并生成回执，不发布第二次文件动作。"""
        if change.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "核实不属于当前工作区", 409)
        if reference is not None:
            self._check_reference(change, reference)
        try:
            with self._locked_parent(change.spec.path, lambda: False) as (fd, parent, name):
                if authorize is not None:
                    authorize()
                self._check_parent(parent, change.parent_identity)
                if reference is None:
                    target = self._snapshot(fd, name, missing=True)
                    staged, linked = None, False
                else:
                    target, staged, linked = self._recovery_snapshots(change, fd, name, reference)
                desired = self._desired(change, target, target.identity if target else None)
                before = self._same_before(target, change.before, displaced=True)
                if keeping:
                    desired = desired or (before and self._no_op(change, target))
                    if expected_after is not None:
                        # 连续链采用实际冻结属性，不能被旧的无变化内容分支绕过。
                        desired = desired and observed_state(target) == expected_after
                    return self._receipt(
                        change,
                        "done" if desired else "rejected",
                        None if desired else "workspace_conflict",
                        observed_after=observed_state(target) if desired else None,
                    )
                if before and staged is None and self._no_op(change, target):
                    return self._receipt(change, "done", observed_after=observed_state(target))
                old_stage = change.before is not None and self._same_before(
                    staged, change.before, displaced=True
                )
                # 删除须保留原旧条目证明；新建只接受自身两条链接或已完成的新版本。
                published = desired and (
                    old_stage
                    or (change.before is None and (staged is None or linked))
                    or (change.after is not None and staged is None)
                )
                if published:
                    os.fsync(fd)
                    return self._receipt(
                        change,
                        "done",
                        changed=True,
                        recovery=reference if staged else None,
                        observed_after=observed_state(target),
                    )
                new_stage = staged is not None and self._desired(change, staged, staged.identity)
                if before and (staged is None or new_stage):
                    return self._receipt(
                        change,
                        "rejected",
                        "confirmed_no_effect",
                        recovery=reference if staged else None,
                    )
        except (ServiceError, OSError):
            pass
        return self._receipt(
            change,
            "uncertain",
            "workspace_reconcile_inconclusive",
            recovery=reference,
            changed=True,
        )

    @staticmethod
    def _check_reference(change: PreparedFileChange, reference: FileRecovery) -> None:
        if (
            reference.workspace_fingerprint != change.workspace_fingerprint
            or reference.path != change.spec.path
            or reference.stage_name != f"._krim-tx-{change.token}"
            or reference.parent_identity != change.parent_identity
        ):
            raise ServiceError("workspace_change_invalid", "修改与暂存引用不匹配", 422)

    def _recovery_snapshots(
        self, change: PreparedFileChange, fd: int, name: str, reference: FileRecovery
    ) -> tuple[FileSnapshot | None, FileSnapshot | None, bool]:
        def info(entry: str) -> os.stat_result | None:
            try:
                return os.stat(entry, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                return None

        target_info, stage_info = info(name), info(reference.stage_name)
        linked = (
            change.before is None
            and change.after is not None
            and target_info is not None
            and stage_info is not None
            and target_info.st_nlink == stage_info.st_nlink == 2
            and _identity(target_info) == _identity(stage_info)
        )
        target = self._snapshot(fd, name, missing=True, own_links=2 if linked else 1)
        staged = self._snapshot(
            fd, reference.stage_name, missing=True, own_links=2 if linked else 1
        )
        if linked and (
            target is None
            or staged is None
            or target.identity != staged.identity
            or not self._desired(change, target, target.identity)
            or not self._desired(change, staged, staged.identity)
        ):
            raise ServiceError("workspace_stage_changed", "新建恢复条目身份或内容不匹配", 409)
        return target, staged, linked

    def finalize(
        self, change: PreparedFileChange, reference: FileRecovery, *, rejected: bool = False
    ) -> None:
        """调用方保存旧快照/结果后显式清理；检查全部旧内容与身份，不重放修改。"""
        if reference.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "清理不属于当前工作区", 409)
        self._check_reference(change, reference)
        with self._locked_parent(reference.path, lambda: False) as (fd, parent, name):
            self._check_parent(parent, reference.parent_identity)
            if change.before is None:
                target, actual, linked = self._recovery_snapshots(change, fd, name, reference)
                if actual is not None and not rejected and not linked:
                    raise ServiceError(
                        "workspace_stage_changed", "新建恢复条目没有原双链接证明", 409
                    )
            else:
                target = None
                actual = self._snapshot(fd, reference.stage_name, missing=True)
            if actual is None:
                # unlink成功而清理标记提交失败的重试；仍同步原目录。
                os.fsync(fd)
                return
            if rejected:
                target = self._snapshot(fd, name, missing=True)
                if not self._same_before(
                    target, change.before, displaced=True
                ) or not self._desired(change, actual, actual.identity):
                    raise ServiceError(
                        "workspace_stage_changed", "新文件恢复材料已变化，保留暂存", 409
                    )
            elif change.before is not None:
                if not self._same_before(actual, change.before, displaced=True):
                    raise ServiceError(
                        "workspace_stage_changed", "旧文件恢复材料已变化，保留暂存", 409
                    )
            self._remove_owned(fd, reference.stage_name, actual.identity)
            os.fsync(fd)
