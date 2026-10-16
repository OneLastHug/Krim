"""缺失目录的可信只读事实；保留父版本，不将缺失冒充空树或发布资格。"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import stat
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from threading import RLock
from types import TracebackType
from typing import Self
from uuid import uuid4

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader, _path


def _error(code: str) -> ServiceError:
    return ServiceError("workspace_directory_absence_" + code, "缺失目录原状态核实失败", 409)


@dataclass(frozen=True)
class DirectoryAbsenceParent:
    device: int
    inode: int
    mode: int
    uid: int
    gid: int
    mtime_ns: int
    ctime_ns: int
    nlink: int

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        values = (
            self.device,
            self.inode,
            self.mode,
            self.uid,
            self.gid,
            self.mtime_ns,
            self.ctime_ns,
            self.nlink,
        )
        if (
            any(type(value) is not int for value in values)
            or self.device < 0
            or self.inode <= 0
            or not 0 <= self.mode <= 0o7777
            or not 0 <= self.uid < 2**32 - 1
            or not 0 <= self.gid < 2**32 - 1
            or self.nlink < 1
        ):
            raise ValueError("缺失目录父版本必须保留真实整数属性")

    @property
    def identity(self) -> tuple[int, int]:
        self.validate()
        return self.device, self.inode

    def metadata(self) -> dict[str, int]:
        self.validate()
        return {
            "device": self.device,
            "inode": self.inode,
            "mode": self.mode,
            "uid": self.uid,
            "gid": self.gid,
            "mtime_ns": self.mtime_ns,
            "ctime_ns": self.ctime_ns,
            "nlink": self.nlink,
        }


@dataclass(frozen=True)
class WorkspaceDirectoryAbsence:
    root_path: str
    workspace_fingerprint: str
    parent: DirectoryAbsenceParent
    observation_id: str = field(repr=False)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if (
            type(self.root_path) is not str
            or type(self.workspace_fingerprint) is not str
            or re.fullmatch(r"[a-f0-9]{64}", self.workspace_fingerprint) is None
            or type(self.parent) is not DirectoryAbsenceParent
            or type(self.observation_id) is not str
            or re.fullmatch(r"[a-f0-9]{32}", self.observation_id) is None
        ):
            raise ValueError("缺失目录状态需要原相对路径、工作区和父版本")
        _path(self.root_path)
        self.parent.validate()

    @property
    def fingerprint(self) -> str:
        self.validate()
        value = {
            "schema_version": 1,
            "kind": "absent_directory",
            "root_path": self.root_path,
            "workspace_fingerprint": self.workspace_fingerprint,
            "parent": self.parent.metadata(),
            "observation_id": self.observation_id,
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
        ).hexdigest()


def _filesystem_kind(library: ctypes.CDLL, parent: int) -> int:
    inspect = library.fstatfs
    inspect.argtypes, inspect.restype = [ctypes.c_int, ctypes.c_void_p], ctypes.c_int
    # Linux statfs首字段为原生long；保留足够的ABI输出空间，不解读其余私有字段。
    buffer = ctypes.create_string_buffer(256)
    if int(inspect(parent, ctypes.byref(buffer))) < 0:
        raise _error("watch_unavailable")
    return int(ctypes.c_long.from_buffer(buffer).value) & 0xFFFFFFFF


def _watch_fd(parent: int) -> int:
    if not sys.platform.startswith("linux"):
        raise _error("watch_unavailable")
    try:
        library = ctypes.CDLL(None, use_errno=True)
        # 远端/用户态文件系统可绕过本机inotify；不把注册成功当作持续观察保证。
        if _filesystem_kind(library, parent) not in {
            0xEF53,  # ext2/3/4
            0x58465342,  # XFS
            0x9123683E,  # Btrfs
            0x01021994,  # tmpfs
        }:
            raise _error("watch_filesystem")
        initialize = library.inotify_init1
        initialize.argtypes, initialize.restype = [ctypes.c_int], ctypes.c_int
        observe = library.inotify_add_watch
        observe.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        observe.restype = ctypes.c_int
    except (AttributeError, OSError):
        raise _error("watch_unavailable") from None
    descriptor = int(initialize(os.O_NONBLOCK | os.O_CLOEXEC))
    if descriptor < 0:
        raise _error("watch_unavailable")
    try:
        # Linux UAPI：ATTRIB/MOVE/CREATE/DELETE/SELF；ONLYDIR绑定原安全父FD。
        # UNMOUNT/Q_OVERFLOW/IGNORED由内核自动投递，同样使观察失效。
        if int(observe(descriptor, os.fsencode(f"/proc/self/fd/{parent}"), 0x01000FC4)) < 0:
            raise _error("watch_unavailable")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


class _ParentWatch:
    def __init__(self, parent: int) -> None:
        self.descriptor = _watch_fd(parent)
        self.invalid = False

    def changed(self) -> bool:
        if self.invalid or self.descriptor < 0:
            return True
        try:
            # 任何事件或未知/截断数据都失效，不以丢失细节为理由回退stat-only。
            os.read(self.descriptor, 65_536)
        except BlockingIOError:
            return False
        except OSError:
            pass
        self.invalid = True
        return True

    def close(self) -> None:
        descriptor, self.descriptor = self.descriptor, -1
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


@dataclass(frozen=True)
class _Observation:
    value: WorkspaceDirectoryAbsence
    watch: _ParentWatch


class WorkspaceDirectoryAbsenceReader:
    """原安全父FD和协作锁内观察缺失；不创建目录、暂存或任何发布效果。"""

    def __init__(
        self, workspace: Workspace, *, lock_timeout: float = 5, max_observations: int = 128
    ) -> None:
        self._owner_pid = os.getpid()
        self._observations: dict[str, _Observation] = {}
        self._state_lock = RLock()
        self._closed = True
        if type(max_observations) is not int or not 1 <= max_observations <= 128:
            raise ValueError("缺失目录观察数量须在1–128之间")
        # 沿原物理预算校验，不能为新的缺失分支另开更长截止时间。
        reader = WorkspaceTreeSnapshotReader(workspace, lock_timeout=lock_timeout)
        self.workspace = workspace
        self.directories = reader.directories
        self.lock_timeout = reader.lock_timeout
        self.max_observations = max_observations
        self._closed = False

    def __enter__(self) -> Self:
        self._owner()
        if self._closed:
            raise _error("closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        if os.getpid() != self._owner_pid:
            # fork后锁可能属于已经不存在的父线程；只关闭子进程自己的FD副本。
            self._closed = True
            for observation in self._observations.values():
                observation.watch.close()
            self._observations.clear()
            return
        with self._state_lock:
            self._closed = True
            for observation in self._observations.values():
                observation.watch.close()
            self._observations.clear()

    def _owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise _error("process")

    def _discard(self, path: str, observation: _Observation) -> None:
        with self._state_lock:
            if self._observations.get(path) is observation:
                del self._observations[path]
            observation.watch.close()

    def _value(self, value: WorkspaceDirectoryAbsence) -> None:
        self._owner()
        try:
            if type(value) is not WorkspaceDirectoryAbsence:
                raise _error("material")
            value.validate()
            if value.workspace_fingerprint != self.workspace.fingerprint:
                raise _error("material")
        except (AttributeError, TypeError, ValueError):
            raise _error("material") from None

    def release(self, value: WorkspaceDirectoryAbsence) -> bool:
        self._value(value)
        with self._state_lock:
            observation = self._observations.get(value.root_path)
            if observation is None or observation.value != value:
                return False
            self._discard(value.root_path, observation)
            return True

    @staticmethod
    def _parent(fd: int) -> DirectoryAbsenceParent:
        value = os.fstat(fd)
        if not stat.S_ISDIR(value.st_mode):
            raise _error("parent")
        return DirectoryAbsenceParent(
            value.st_dev,
            value.st_ino,
            stat.S_IMODE(value.st_mode),
            value.st_uid,
            value.st_gid,
            value.st_mtime_ns,
            value.st_ctime_ns,
            value.st_nlink,
        )

    @staticmethod
    def _absent(parent: int, name: str) -> None:
        try:
            os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise _error("present")

    def capture(
        self,
        root_path: str,
        *,
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> WorkspaceDirectoryAbsence:
        return self._observe(root_path, None, authorize=authorize, cancelled=cancelled)

    def _observe(
        self,
        root_path: str,
        expected: WorkspaceDirectoryAbsence | None,
        *,
        authorize: Callable[[], None] | None,
        cancelled: Callable[[], bool] | None,
    ) -> WorkspaceDirectoryAbsence:
        self._owner()
        if self._closed:
            raise _error("closed")
        if type(root_path) is not str:
            raise _error("material")
        _path(root_path)
        if (authorize is not None and not callable(authorize)) or (
            cancelled is not None and not callable(cancelled)
        ):
            raise _error("context")
        deadline = time.monotonic() + self.lock_timeout
        abort = cancelled if cancelled is not None else lambda: False

        def deadline_check() -> None:
            if time.monotonic() >= deadline:
                raise _error("timeout")

        def qualify() -> None:
            self._owner()
            if abort():
                raise _error("cancelled")
            self._owner()
            deadline_check()
            if authorize is not None:
                authorize()
            self._owner()
            if abort():
                raise _error("cancelled")
            self._owner()
            deadline_check()

        observation: _Observation | None = None
        try:
            qualify()
            with self.directories._locked_parent(root_path, abort, require_writable=False) as (
                parent,
                path,
                name,
            ):
                first = self._parent(parent)
                with self._state_lock:
                    if self._closed:
                        raise _error("closed")
                    cached = self._observations.get(root_path)
                    if expected is not None and (cached is None or cached.value != expected):
                        raise _error("unverifiable")
                    if cached is not None and (
                        cached.value.parent != first or cached.watch.changed()
                    ):
                        self._discard(root_path, cached)
                        if expected is not None:
                            raise _error("conflict")
                        cached = None
                    reused = cached is not None
                    if cached is None:
                        if len(self._observations) >= self.max_observations:
                            raise _error("limit")
                        watch = _ParentWatch(parent)
                        try:
                            value = WorkspaceDirectoryAbsence(
                                root_path, self.workspace.fingerprint, first, uuid4().hex
                            )
                        except BaseException:
                            watch.close()
                            raise
                        observation = _Observation(value, watch)
                    else:
                        observation = cached
                self._absent(parent, name)
                # 最后授权/取消回调之后重新核实，避免回调期间变化被当作原事实。
                qualify()
                self.directories._files._check_parent(path, first.identity)
                self._absent(parent, name)
                last = self._parent(parent)
                if first != last or observation.watch.changed():
                    raise _error("conflict")
                deadline_check()
                with self._state_lock:
                    if self._closed:
                        raise _error("closed")
                    if reused and self._observations.get(root_path) is not observation:
                        raise _error("unverifiable")
                    self._observations[root_path] = observation
                    # 返回独立副本，Hook不能通过字段突变改写原私有观察记录。
                    return replace(observation.value, parent=replace(observation.value.parent))
        except BaseException as error:
            if observation is not None:
                self._discard(root_path, observation)
            if isinstance(error, OSError):
                raise _error("access") from None
            raise

    def verify(
        self,
        value: WorkspaceDirectoryAbsence,
        *,
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        self._value(value)
        actual = self._observe(value.root_path, value, authorize=authorize, cancelled=cancelled)
        if actual != value:
            raise _error("conflict")
