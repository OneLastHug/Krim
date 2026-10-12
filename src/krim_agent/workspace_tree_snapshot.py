"""已有目录树的完整私有快照；同一原协作锁内核实全部节点与字节。"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import DirectorySnapshot, WorkspaceDirectoryWriter
from krim_agent.workspace_writer import FileSnapshot


def _path(value: str) -> list[str]:
    parts = Workspace._parts(value)
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ServiceError("workspace_path", "目录树路径不是有效UTF-8", 422) from None
    if not parts or "/".join(parts) != value:
        raise ServiceError("workspace_path", "目录树需要规范相对路径", 422)
    return parts


def _same_info(snapshot: DirectorySnapshot | FileSnapshot, info: os.stat_result) -> bool:
    return (
        snapshot.device == info.st_dev
        and snapshot.inode == info.st_ino
        and snapshot.mode == stat.S_IMODE(info.st_mode)
        and snapshot.uid == info.st_uid
        and snapshot.gid == info.st_gid
        and snapshot.mtime_ns == info.st_mtime_ns
        and snapshot.ctime_ns == info.st_ctime_ns
        and (
            snapshot.nlink == info.st_nlink
            if isinstance(snapshot, DirectorySnapshot)
            else info.st_nlink == 1 and len(snapshot.data) == info.st_size
        )
    )


@dataclass(frozen=True)
class TreeSnapshotEntry:
    path: str
    directory: DirectorySnapshot | None = None
    file: FileSnapshot | None = None

    def __post_init__(self) -> None:
        _path(self.path)
        if (self.directory is None) == (self.file is None):
            raise ValueError("树节点必须只保存目录或文件快照")

    def metadata(self) -> dict[str, object]:
        if self.directory is not None:
            return {"path": self.path, "kind": "directory", **asdict(self.directory)}
        assert self.file is not None
        file = self.file
        return {
            "path": self.path,
            "kind": "file",
            "mode": file.mode,
            "device": file.device,
            "inode": file.inode,
            "mtime_ns": file.mtime_ns,
            "ctime_ns": file.ctime_ns,
            "uid": file.uid,
            "gid": file.gid,
            "nlink": 1,
            "sha256": file.sha256,
            "byte_size": len(file.data),
        }


@dataclass(frozen=True)
class WorkspaceTreeSnapshot:
    root_path: str
    workspace_fingerprint: str
    parent_identity: tuple[int, int]
    entries: tuple[TreeSnapshotEntry, ...]

    def __post_init__(self) -> None:
        _path(self.root_path)
        if not self.entries or self.entries[0].path != self.root_path:
            raise ValueError("树快照须包含原目录根")
        if self.entries[0].directory is None:
            raise ValueError("树快照根必须是目录")
        parents = set()
        seen = set()
        for entry in self.entries:
            if entry.path in seen or (
                entry.path != self.root_path
                and (
                    not entry.path.startswith(self.root_path + "/")
                    or entry.path.rpartition("/")[0] not in parents
                )
            ):
                raise ValueError("树快照路径须唯一且保留父闭包")
            seen.add(entry.path)
            if entry.directory is not None:
                parents.add(entry.path)

    @property
    def total_bytes(self) -> int:
        return sum(len(entry.file.data) for entry in self.entries if entry.file is not None)

    @property
    def fingerprint(self) -> str:
        value = {
            "root_path": self.root_path,
            "workspace_fingerprint": self.workspace_fingerprint,
            "parent_identity": self.parent_identity,
            "entries": [entry.metadata() for entry in self.entries],
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
        ).hexdigest()


class WorkspaceTreeSnapshotReader:
    def __init__(
        self,
        workspace: Workspace,
        *,
        max_entries: int = 1024,
        max_total_bytes: int = 33_554_432,
        max_depth: int = 64,
        lock_timeout: float = 5,
    ) -> None:
        if type(max_entries) is not int or not 1 <= max_entries <= 16384:
            raise ValueError("完整树节点上限无效")
        if type(max_total_bytes) is not int or not 0 <= max_total_bytes <= 134_217_728:
            raise ValueError("完整树字节上限无效")
        if type(max_depth) is not int or not 1 <= max_depth <= 256:
            raise ValueError("完整树深度上限无效")
        if type(lock_timeout) not in {int, float} or not 0 < lock_timeout <= 5:
            raise ValueError("完整树物理工作预算须在0–5秒之间")
        self.workspace = workspace
        self.directories = WorkspaceDirectoryWriter(workspace, lock_timeout=lock_timeout)
        self.max_entries, self.max_total_bytes, self.max_depth = (
            max_entries,
            max_total_bytes,
            max_depth,
        )
        self.lock_timeout = self.directories.lock_timeout

    def capture(
        self,
        root_path: str,
        *,
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> WorkspaceTreeSnapshot:
        parts = _path(root_path)
        abort = cancelled or (lambda: False)
        deadline = time.monotonic() + self.lock_timeout

        def check() -> None:
            if abort():
                raise ServiceError("workspace_tree_snapshot_cancelled", "完整树读取已取消", 409)
            if time.monotonic() >= deadline:
                raise ServiceError("workspace_tree_snapshot_timeout", "完整树读取超过物理预算", 409)
            if authorize is not None:
                authorize()

        try:
            check()
            with self.directories._locked_parent(root_path, abort, require_writable=False) as (
                fd,
                parent,
                name,
            ):
                check()
                parent_identity = os.fstat(fd).st_dev, os.fstat(fd).st_ino
                first = self._observe(fd, name, root_path, check)
                check()
                second = self._observe(fd, name, root_path, check)
                if first != second:
                    raise ServiceError("workspace_tree_snapshot_changed", "完整树读取期间变化", 409)
                self.directories._files._check_parent(parent, parent_identity)
                check()
                return WorkspaceTreeSnapshot(
                    "/".join(parts), self.workspace.fingerprint, parent_identity, first
                )
        except OSError:
            raise ServiceError("workspace_access", "完整树读取失败", 403) from None

    def _observe(
        self,
        parent_fd: int,
        root_name: str,
        root_path: str,
        check: Callable[[], None],
    ) -> tuple[TreeSnapshotEntry, ...]:
        entries: list[TreeSnapshotEntry] = []
        total = 0
        device = os.fstat(parent_fd).st_dev

        def visit(fd: int, name: str, path: str, depth: int) -> None:
            nonlocal total
            check()
            _path(path)
            if depth > self.max_depth:
                raise ServiceError("workspace_tree_snapshot_depth", "完整树超过深度上限", 413)
            if len(entries) >= self.max_entries:
                raise ServiceError("workspace_tree_snapshot_limit", "完整树超过节点上限", 413)
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if info.st_dev != device:
                raise ServiceError("workspace_tree_snapshot_device", "目录树包含其他设备", 403)
            if stat.S_ISLNK(info.st_mode):
                raise ServiceError("workspace_link", "完整树不能包含符号链接", 403)
            if stat.S_ISREG(info.st_mode):
                if depth == 1:
                    raise ServiceError("workspace_directory_type", "树根必须是普通目录", 422)
                if info.st_nlink != 1:
                    raise ServiceError("workspace_link", "完整树不能包含多硬链接文件", 403)
                if info.st_size > min(self.workspace.max_file_bytes, self.max_total_bytes - total):
                    raise ServiceError("workspace_tree_snapshot_bytes", "完整树超过字节上限", 413)
                snapshot = self.directories._files._snapshot(
                    fd, name, byte_budget=self.max_total_bytes - total
                )
                assert snapshot is not None
                if not _same_info(snapshot, info) or not _same_info(
                    snapshot, os.stat(name, dir_fd=fd, follow_symlinks=False)
                ):
                    raise ServiceError("workspace_tree_snapshot_changed", "目录树文件身份变化", 409)
                total += len(snapshot.data)
                if total > self.max_total_bytes:
                    raise ServiceError("workspace_tree_snapshot_bytes", "完整树超过字节上限", 413)
                if snapshot.mode & ~0o777:
                    raise ServiceError("workspace_file_mode", "树文件模式不受支持", 422)
                entries.append(TreeSnapshotEntry(path, file=snapshot))
                check()
                return
            if not stat.S_ISDIR(info.st_mode):
                raise ServiceError("workspace_file_type", "完整树只允许普通目录和文件", 422)
            before = self.directories._snapshot(fd, name)
            assert before is not None
            if not _same_info(before, info):
                raise ServiceError("workspace_tree_snapshot_changed", "目录树身份变化", 409)
            entries.append(TreeSnapshotEntry(path, directory=before))
            child_fd = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd
            )
            try:
                opened = os.fstat(child_fd)
                if (opened.st_dev, opened.st_ino) != before.identity:
                    raise ServiceError("workspace_tree_snapshot_changed", "目录树身份变化", 409)
                names: list[str] = []
                with os.scandir(child_fd) as children:
                    for child in children:
                        check()
                        if len(names) + len(entries) >= self.max_entries:
                            raise ServiceError(
                                "workspace_tree_snapshot_limit", "完整树超过节点上限", 413
                            )
                        names.append(child.name)
                for child_name in sorted(names):
                    visit(child_fd, child_name, path + "/" + child_name, depth + 1)
                if self.directories._snapshot(fd, name, missing=True) != before:
                    raise ServiceError("workspace_tree_snapshot_changed", "目录树版本变化", 409)
                check()
            finally:
                os.close(child_fd)

        visit(parent_fd, root_name, root_path, 1)
        return tuple(entries)

    def verify(
        self,
        snapshot: WorkspaceTreeSnapshot,
        *,
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        if snapshot.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "树快照不属于原工作区版本", 403)
        current = self.capture(snapshot.root_path, authorize=authorize, cancelled=cancelled)
        if current != snapshot:
            raise ServiceError("workspace_tree_snapshot_changed", "原完整树已变化", 409)
