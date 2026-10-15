"""完整目录的可信暂存/单次条件发布；持久来源和控制由上层原Fence仲裁。"""

from __future__ import annotations

import os
import re
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Literal

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import DirectorySnapshot, WorkspaceDirectoryWriter
from krim_agent.workspace_tree_snapshot import (
    TreeSnapshotEntry,
    WorkspaceTreeSnapshot,
    WorkspaceTreeSnapshotReader,
    _path,
    _same_info,
)
from krim_agent.workspace_writer import FileSnapshot, _rename_function, rename_at


@dataclass(frozen=True)
class DirectoryRestoreStage:
    root_path: str
    token: str = field(repr=False)
    parent_identity: tuple[int, int]
    container_identity: tuple[int, int]
    container_owner: tuple[int, int]


@dataclass(frozen=True)
class PreparedDirectoryRestore:
    stage: DirectoryRestoreStage
    target_fingerprint: str
    before: WorkspaceTreeSnapshot = field(repr=False)
    staged: WorkspaceTreeSnapshot = field(repr=False)


@dataclass(frozen=True)
class DirectoryRestoreReceipt:
    root_path: str
    status: Literal["done", "rejected", "uncertain"]
    before_fingerprint: str
    target_fingerprint: str
    after_fingerprint: str | None
    retained_fingerprint: str | None
    changed: bool
    error_code: str | None
    recovery_required: bool


@dataclass(frozen=True)
class DirectoryRestoreEffect:
    receipt: DirectoryRestoreReceipt
    observed_after: WorkspaceTreeSnapshot | None = field(default=None, repr=False)
    retained_before: WorkspaceTreeSnapshot | None = field(default=None, repr=False)


class WorkspaceDirectoryRestoreWriter:
    """仅可信完整材料；普通调用者不能上传私有准备材料或选择暂存路径。"""

    def __init__(self, workspace: Workspace, *, lock_timeout: float = 5) -> None:
        self.workspace = workspace
        self.reader = WorkspaceTreeSnapshotReader(workspace, lock_timeout=lock_timeout)
        self.directories = self.reader.directories
        self.lock_timeout = self.reader.lock_timeout

    @staticmethod
    def _error(code: str = "material") -> ServiceError:
        return ServiceError(
            "workspace_directory_restore_" + code, "原目录恢复材料或条件不匹配", 409
        )

    @staticmethod
    def _token(token: str) -> None:
        if type(token) is not str or re.fullmatch(r"[a-f0-9]{32}", token) is None:
            raise WorkspaceDirectoryRestoreWriter._error()

    def _validate_snapshot(self, value: WorkspaceTreeSnapshot) -> None:
        try:
            if type(value) is not WorkspaceTreeSnapshot:
                raise self._error()
            _path(value.root_path)
            if (
                value.workspace_fingerprint != self.workspace.fingerprint
                or type(value.parent_identity) is not tuple
                or len(value.parent_identity) != 2
                or any(type(item) is not int or item < 0 for item in value.parent_identity)
                or type(value.entries) is not tuple
                or not 1 <= len(value.entries) <= 1024
            ):
                raise self._error()
            # 重建验证父闭包/唯一性，防止可信层传入被object.__setattr__破坏的对象。
            WorkspaceTreeSnapshot(
                value.root_path, value.workspace_fingerprint, value.parent_identity, value.entries
            )
            total = 0
            identities = {value.parent_identity}
            for entry in value.entries:
                if type(entry) is not TreeSnapshotEntry:
                    raise self._error()
                TreeSnapshotEntry(entry.path, directory=entry.directory, file=entry.file)
                _path(entry.path)
                if len(entry.path.split("/")) - len(value.root_path.split("/")) + 1 > 64:
                    raise self._error("limit")
                item = entry.file if entry.file is not None else entry.directory
                if type(item) not in {FileSnapshot, DirectorySnapshot} or item is None:
                    raise self._error()
                if (
                    type(item.mode) is not int
                    or not 0 <= item.mode <= 0o777
                    or any(
                        type(number) is not int or not 0 <= number < 2**32 - 1
                        for number in (item.uid, item.gid)
                    )
                    or type(item.device) is not int
                    or item.device != value.parent_identity[0]
                    or type(item.inode) is not int
                    or item.inode <= 0
                    or type(item.mtime_ns) is not int
                    or type(item.ctime_ns) is not int
                ):
                    raise self._error()
                identity = item.device, item.inode
                if identity in identities:
                    raise self._error()
                identities.add(identity)
                if entry.file is not None:
                    if type(entry.file.data) is not bytes:
                        raise self._error()
                    total += len(entry.file.data)
                    if len(entry.file.data) > min(self.workspace.max_file_bytes, 8_388_608):
                        raise self._error("limit")
                elif entry.directory is not None:
                    child = any(
                        other.path.rpartition("/")[0] == entry.path for other in value.entries
                    )
                    if (
                        type(entry.directory.empty) is not bool
                        or entry.directory.empty == child
                        or type(entry.directory.nlink) is not int
                        or entry.directory.nlink < 1
                    ):
                        raise self._error()
            if total > 33_554_432:
                raise self._error("limit")
        except (AttributeError, ValueError, TypeError):
            raise self._error() from None

    def _check(self, deadline: float, abort: Callable[[], bool]) -> None:
        if abort():
            raise self._error("cancelled")
        if time.monotonic() >= deadline:
            raise self._error("timeout")

    def _capture(
        self, parent: int, name: str, root: str, check: Callable[[], None]
    ) -> WorkspaceTreeSnapshot:
        check()
        first = self.reader._observe(parent, name, root, check)
        second = self.reader._observe(parent, name, root, check)
        if first != second:
            raise self._error("changed")
        info = os.fstat(parent)
        return WorkspaceTreeSnapshot(
            root, self.workspace.fingerprint, (info.st_dev, info.st_ino), first
        )

    @staticmethod
    def _same_content(wanted: WorkspaceTreeSnapshot, actual: WorkspaceTreeSnapshot) -> bool:
        entries = {entry.path: entry for entry in actual.entries}
        if set(entries) != {entry.path for entry in wanted.entries}:
            return False
        for entry in wanted.entries:
            other = entries[entry.path]
            old, new = entry.file or entry.directory, other.file or other.directory
            if (
                old is None
                or new is None
                or type(old) is not type(new)
                or (old.mode, old.uid, old.gid) != (new.mode, new.uid, new.gid)
                or entry.file is not None
                and (other.file is None or entry.file.data != other.file.data)
                or entry.directory is not None
                and (other.directory is None or entry.directory.empty != other.directory.empty)
            ):
                return False
        return True

    @contextmanager
    def _container(self, parent: int, stage: DirectoryRestoreStage) -> Iterator[int]:
        name = "._krim-tx-" + stage.token
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            self._check_container(parent, stage, fd)
            yield fd
        finally:
            os.close(fd)

    def _check_container(self, parent: int, stage: DirectoryRestoreStage, fd: int) -> None:
        info = os.fstat(fd)
        named = os.stat("._krim-tx-" + stage.token, dir_fd=parent, follow_symlinks=False)
        if (
            (info.st_dev, info.st_ino) != stage.container_identity
            or (named.st_dev, named.st_ino) != stage.container_identity
            or not stat.S_ISDIR(named.st_mode)
            or (info.st_uid, info.st_gid) != stage.container_owner
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise self._error("container")

    def _write_tree(
        self, container: int, target: WorkspaceTreeSnapshot, check: Callable[[], None]
    ) -> None:
        children: dict[str, list[TreeSnapshotEntry]] = {}
        for entry in target.entries[1:]:
            children.setdefault(entry.path.rpartition("/")[0], []).append(entry)

        def owner(fd: int, item: FileSnapshot | DirectorySnapshot) -> None:
            check()
            info = os.fstat(fd)
            if (info.st_uid, info.st_gid) != (item.uid, item.gid):
                os.fchown(fd, item.uid, item.gid)
            os.fchmod(fd, item.mode)
            os.fsync(fd)
            check()

        def write(parent: int, name: str, entry: TreeSnapshotEntry) -> None:
            check()
            if entry.file is not None:
                fd = os.open(
                    name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=parent
                )
                try:
                    data, offset = entry.file.data, 0
                    while offset < len(data):
                        check()
                        count = os.write(fd, data[offset : offset + 65536])
                        if count <= 0:
                            raise self._error("io")
                        offset += count
                    owner(fd, entry.file)
                finally:
                    os.close(fd)
            else:
                assert entry.directory is not None
                os.mkdir(name, 0o700, dir_fd=parent)
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                try:
                    for child in sorted(children.get(entry.path, []), key=lambda item: item.path):
                        write(fd, child.path.rsplit("/", 1)[-1], child)
                    owner(fd, entry.directory)
                finally:
                    os.close(fd)

        write(container, "payload", target.entries[0])
        os.fsync(container)
        check()

    def _verify_stats(
        self, parent: int, name: str, snapshot: WorkspaceTreeSnapshot, check: Callable[[], None]
    ) -> None:
        """最后资格回调后核实全部固定版本；不在核实后再调用可等待的资格回调。"""
        children: dict[str, list[TreeSnapshotEntry]] = {}
        for entry in snapshot.entries[1:]:
            children.setdefault(entry.path.rpartition("/")[0], []).append(entry)

        def visit(fd: int, leaf: str, entry: TreeSnapshotEntry) -> None:
            check()
            wanted = entry.file or entry.directory
            assert wanted is not None
            if not _same_info(wanted, os.stat(leaf, dir_fd=fd, follow_symlinks=False)):
                raise self._error("conflict")
            if entry.directory is not None:
                child = os.open(leaf, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    if not _same_info(wanted, os.fstat(child)):
                        raise self._error("conflict")
                    for value in children.get(entry.path, []):
                        visit(child, value.path.rsplit("/", 1)[-1], value)
                    if not _same_info(wanted, os.stat(leaf, dir_fd=fd, follow_symlinks=False)):
                        raise self._error("conflict")
                finally:
                    os.close(child)
            check()

        visit(parent, name, snapshot.entries[0])

    def prepare(
        self,
        target: WorkspaceTreeSnapshot,
        *,
        expected_before: str,
        token: str,
        record_stage: Callable[[DirectoryRestoreStage], None] | None = None,
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> PreparedDirectoryRestore:
        deadline, abort = time.monotonic() + self.lock_timeout, cancelled or (lambda: False)

        def check() -> None:
            self._check(deadline, abort)
            if authorize is not None:
                authorize()

        self._token(token)
        self._validate_snapshot(target)
        if (
            type(expected_before) is not str
            or re.fullmatch(r"[a-f0-9]{64}", expected_before) is None
        ):
            raise self._error()
        _rename_function()
        try:
            check()
            with self.directories._locked_parent(target.root_path, abort) as (parent, path, name):
                before = self._capture(parent, name, target.root_path, check)
                if before.fingerprint != expected_before:
                    raise self._error("conflict")
                self.directories._files._check_parent(path, before.parent_identity)
                check()
                stage_name = "._krim-tx-" + token
                os.mkdir(stage_name, 0o700, dir_fd=parent)
                info = os.stat(stage_name, dir_fd=parent, follow_symlinks=False)
                stage = DirectoryRestoreStage(
                    target.root_path,
                    token,
                    before.parent_identity,
                    (info.st_dev, info.st_ino),
                    (info.st_uid, info.st_gid),
                )
                with self._container(parent, stage) as container:
                    os.fsync(parent)
                    if record_stage is not None:
                        record_stage(stage)
                    check()
                    self._check_container(parent, stage, container)
                    self._write_tree(container, target, check)
                    staged = self._capture(container, "payload", target.root_path, check)
                    if not self._same_content(target, staged):
                        raise self._error("stage")
                    if self._capture(parent, name, target.root_path, check) != before:
                        raise self._error("conflict")
                    self.directories._files._check_parent(path, before.parent_identity)
                    check()
                    self._check_container(parent, stage, container)
                    return PreparedDirectoryRestore(stage, target.fingerprint, before, staged)
        except OSError:
            # 失败保留同token私有容器；上层原operation可审计，禁止盲目删除/重新生产。
            raise self._error("io") from None

    def _prepared(self, change: PreparedDirectoryRestore) -> None:
        try:
            if (
                type(change) is not PreparedDirectoryRestore
                or type(change.stage) is not DirectoryRestoreStage
            ):
                raise self._error()
            stage = change.stage
            self._token(stage.token)
            self._validate_snapshot(change.before)
            self._validate_snapshot(change.staged)
            if (
                stage.root_path != change.before.root_path
                or stage.root_path != change.staged.root_path
                or stage.parent_identity != change.before.parent_identity
                or stage.container_identity != change.staged.parent_identity
                or stage.parent_identity[0] != stage.container_identity[0]
                or re.fullmatch(r"[a-f0-9]{64}", change.target_fingerprint) is None
                or any(
                    type(identity) is not tuple
                    or len(identity) != 2
                    or any(type(item) is not int or item < 0 for item in identity)
                    for identity in (
                        stage.parent_identity,
                        stage.container_identity,
                        stage.container_owner,
                    )
                )
            ):
                raise self._error()
        except (AttributeError, TypeError, ValueError):
            raise self._error() from None

    @staticmethod
    def _moved(
        before: WorkspaceTreeSnapshot, actual: WorkspaceTreeSnapshot, parent: tuple[int, int]
    ) -> bool:
        if (
            before.root_path != actual.root_path
            or before.workspace_fingerprint != actual.workspace_fingerprint
            or actual.parent_identity != parent
            or len(before.entries) != len(actual.entries)
        ):
            return False
        for index, (old, new) in enumerate(zip(before.entries, actual.entries, strict=True)):
            if old.path != new.path:
                return False
            if old.file is not None:
                if old.file != new.file or new.directory is not None:
                    return False
            elif new.file is not None or not WorkspaceDirectoryWriter._same(
                new.directory, old.directory, displaced=index == 0
            ):
                return False
        return True

    @staticmethod
    def _effect(
        change: PreparedDirectoryRestore,
        status: Literal["done", "rejected", "uncertain"],
        code: str | None = None,
        *,
        changed: bool = False,
        after: WorkspaceTreeSnapshot | None = None,
        retained: WorkspaceTreeSnapshot | None = None,
    ) -> DirectoryRestoreEffect:
        return DirectoryRestoreEffect(
            DirectoryRestoreReceipt(
                change.stage.root_path,
                status,
                change.before.fingerprint,
                change.target_fingerprint,
                after.fingerprint if after else None,
                retained.fingerprint if retained else None,
                changed,
                code,
                status == "uncertain",
            ),
            after,
            retained,
        )

    def apply(
        self,
        change: PreparedDirectoryRestore,
        *,
        record_intent: Callable[[PreparedDirectoryRestore], None],
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> DirectoryRestoreEffect:
        deadline, abort = time.monotonic() + self.lock_timeout, cancelled or (lambda: False)
        self._prepared(change)
        attempted = False

        def check() -> None:
            self._check(deadline, abort)

        try:
            with self.directories._locked_parent(change.stage.root_path, abort) as (
                parent,
                path,
                name,
            ):
                with self._container(parent, change.stage) as container:

                    def original() -> None:
                        check()
                        if authorize is not None:
                            authorize()
                        self.directories._files._check_parent(path, change.stage.parent_identity)
                        self._check_container(parent, change.stage, container)
                        if (
                            self._capture(parent, name, change.stage.root_path, check)
                            != change.before
                            or self._capture(container, "payload", change.stage.root_path, check)
                            != change.staged
                        ):
                            raise self._error("conflict")
                        check()

                    original()
                    _rename_function()
                    record_intent(change)
                    original()
                    if authorize is not None:
                        authorize()
                    self._check_container(parent, change.stage, container)
                    self._verify_stats(parent, name, change.before, check)
                    self._verify_stats(container, "payload", change.staged, check)
                    self.directories._files._check_parent(path, change.stage.parent_identity)
                    check()
                    attempted = True
                    rename_at(container, "payload", name, target_fd=parent, exchange=True)
                    os.fsync(container)
                    os.fsync(parent)

                    def observed() -> None:
                        self._check(deadline, lambda: False)

                    after = self._capture(parent, name, change.stage.root_path, observed)
                    retained = self._capture(container, "payload", change.stage.root_path, observed)
                    self.directories._files._check_parent(path, change.stage.parent_identity)
                    self._check_container(parent, change.stage, container)
                    if not self._moved(
                        change.staged, after, change.stage.parent_identity
                    ) or not self._moved(change.before, retained, change.stage.container_identity):
                        raise self._error("postcheck")
                    return self._effect(
                        change, "done", changed=True, after=after, retained=retained
                    )
        except (OSError, ServiceError) as error:
            return self._effect(
                change,
                "uncertain" if attempted else "rejected",
                error.code if isinstance(error, ServiceError) else "workspace_directory_restore_io",
                changed=attempted,
            )

    def reconcile(self, change: PreparedDirectoryRestore) -> DirectoryRestoreEffect:
        deadline = time.monotonic() + self.lock_timeout
        self._prepared(change)

        def check() -> None:
            self._check(deadline, lambda: False)

        try:
            with self.directories._locked_parent(
                change.stage.root_path, lambda: False, require_writable=False
            ) as (parent, path, name):
                self.directories._files._check_parent(path, change.stage.parent_identity)
                with self._container(parent, change.stage) as container:
                    after = self._capture(parent, name, change.stage.root_path, check)
                    retained = self._capture(container, "payload", change.stage.root_path, check)
                    self.directories._files._check_parent(path, change.stage.parent_identity)
                    self._check_container(parent, change.stage, container)
                    if self._moved(
                        change.staged, after, change.stage.parent_identity
                    ) and self._moved(change.before, retained, change.stage.container_identity):
                        return self._effect(
                            change, "done", changed=True, after=after, retained=retained
                        )
                    if after == change.before and retained == change.staged:
                        return self._effect(
                            change, "rejected", "workspace_directory_restore_not_observed"
                        )
                    return self._effect(
                        change, "uncertain", "workspace_directory_restore_ambiguous", changed=True
                    )
        except (OSError, ServiceError) as error:
            return self._effect(
                change,
                "uncertain",
                error.code if isinstance(error, ServiceError) else "workspace_directory_restore_io",
                changed=True,
            )
