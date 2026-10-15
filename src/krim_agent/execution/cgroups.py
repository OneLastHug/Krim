"""可信宿主的独立cgroup2根/叶组；只创建自己子树、只给启动器自己的入组FD。"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import stat
import sys
import time
from pathlib import Path
from threading import RLock
from uuid import UUID

from krim_agent.protocol.domain import ServiceError

from .tree_models import SandboxTreeLimits, SandboxTreeUsage

_CONTROLLERS = frozenset({"cpu", "memory", "pids"})
_CGROUP2_MAGIC = 0x63677270


def _error() -> ServiceError:
    return ServiceError("execution_tree_unavailable", "进程树cgroup资源或资格不可用", 503)


def _filesystem(fd: int) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    libc.fstatfs.argtypes = [ctypes.c_int, ctypes.c_void_p]
    libc.fstatfs.restype = ctypes.c_int
    buffer = ctypes.create_string_buffer(256)
    if libc.fstatfs(fd, buffer) != 0:
        raise _error()
    if ctypes.c_long.from_buffer(buffer).value != _CGROUP2_MAGIC:
        raise _error()


def _identity(fd: int) -> tuple[int, int]:
    value = os.fstat(fd)
    return value.st_dev, value.st_ino


def _directory(path: Path) -> int:
    if not sys.platform.startswith("linux") or not path.is_absolute():
        raise _error()
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            if part in {"", ".", ".."}:
                raise _error()
            next_fd = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd
            )
            os.close(fd)
            fd = next_fd
        _filesystem(fd)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _read(fd: int, name: str) -> str:
    value = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
    try:
        data = os.read(value, 16385)
        if len(data) > 16384:
            raise _error()
        return data.decode("ascii").strip()
    finally:
        os.close(value)


def _write(fd: int, name: str, value: str) -> None:
    target = os.open(name, os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
    try:
        data = (value + "\n").encode("ascii")
        if os.write(target, data) != len(data):
            raise _error()
    finally:
        os.close(target)


def _numbers(raw: str) -> dict[str, int]:
    values = {}
    for line in raw.splitlines():
        key, number = line.split()
        parsed = int(number)
        if parsed < 0:
            raise _error()
        values[key] = parsed
    return values


class CgroupRoot:
    """根须为可信宿主专门委派的空krim-*目录；不修改祖先控制器。"""

    def __init__(self, path: str | Path, *, remove_on_close: bool = False) -> None:
        self.path = Path(path)
        if not re.fullmatch(r"krim-[A-Za-z0-9_.-]{1,100}", self.path.name):
            raise _error()
        self._fd = -1
        self._closed = False
        self._lock = RLock()
        self._jobs: dict[str, CgroupLease] = {}
        self._remove = remove_on_close
        try:
            self._fd = _directory(self.path)
            self._identity = _identity(self._fd)
            self._verify()
            self.fingerprint = hashlib.sha256(
                json.dumps(
                    {
                        "version": 1,
                        "identity": self._identity,
                        "controllers": sorted(_CONTROLLERS),
                    },
                    sort_keys=True,
                ).encode()
            ).hexdigest()
        except BaseException as error:
            if self._fd >= 0:
                os.close(self._fd)
                self._fd = -1
            if isinstance(error, ServiceError):
                raise
            raise _error() from None

    @classmethod
    def prepare(cls, path: str | Path, *, remove_on_close: bool = False) -> CgroupRoot:
        """只新建专用空根；已有目录不采纳、不改祖先或移动任何已有进程。"""
        selected = Path(path)
        if not re.fullmatch(r"krim-[A-Za-z0-9_.-]{1,100}", selected.name):
            raise _error()
        parent = -1
        child = -1
        created = False
        try:
            parent = _directory(selected.parent)
            if not _CONTROLLERS <= set(_read(parent, "cgroup.subtree_control").split()):
                raise _error()
            os.mkdir(selected.name, 0o700, dir_fd=parent)
            created = True
            child = os.open(
                selected.name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
            os.fchmod(child, 0o700)
            if _read(child, "cgroup.procs"):
                raise _error()
            _write(child, "cgroup.subtree_control", "+cpu +memory +pids")
            return cls(selected, remove_on_close=remove_on_close)
        except BaseException as error:
            if created:
                try:
                    os.rmdir(selected.name, dir_fd=parent)
                except OSError:
                    pass
            if isinstance(error, ServiceError):
                raise
            raise _error() from None
        finally:
            if child >= 0:
                os.close(child)
            if parent >= 0:
                os.close(parent)

    def _verify(self, *, allow_closed: bool = False) -> None:
        if (
            self._closed
            and not allow_closed
            or self._fd < 0
            or _identity(self._fd) != self._identity
        ):
            raise _error()
        observed = _directory(self.path)
        try:
            if _identity(observed) != self._identity:
                raise _error()
        finally:
            os.close(observed)
        if _read(self._fd, "cgroup.type") != "domain" or _read(self._fd, "cgroup.procs"):
            raise _error()
        if not _CONTROLLERS <= set(_read(self._fd, "cgroup.subtree_control").split()):
            raise _error()

    def require(self) -> None:
        with self._lock:
            try:
                self._verify()
            except (OSError, ValueError):
                raise _error() from None

    @property
    def closed(self) -> bool:
        return self._closed

    @staticmethod
    def _name(identifier: str) -> str:
        try:
            if str(UUID(identifier)) != identifier:
                raise ValueError("identifier")
        except (ValueError, TypeError, AttributeError):
            raise _error() from None
        return "krim-exec-" + identifier

    def allocate(self, identifier: str, limits: SandboxTreeLimits) -> CgroupLease:
        name = self._name(identifier)
        limits = SandboxTreeLimits.model_validate(limits.model_dump())
        with self._lock:
            fd = -1
            created = False
            try:
                self._verify()
                os.mkdir(name, 0o700, dir_fd=self._fd)
                created = True
                fd = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=self._fd,
                )
                os.fchmod(fd, 0o700)
                for field, value in (
                    ("memory.max", str(limits.memory_bytes)),
                    ("memory.swap.max", "0"),
                    ("memory.oom.group", "1"),
                    ("pids.max", str(limits.process_count)),
                ):
                    _write(fd, field, value)
                    if _read(fd, field) != value:
                        raise _error()
                if "usage_usec" not in _numbers(_read(fd, "cpu.stat")):
                    raise _error()
                if "populated" not in _numbers(_read(fd, "cgroup.events")):
                    raise _error()
                lease = CgroupLease(self, identifier, name, fd, limits)
                self._jobs[identifier] = lease
                return lease
            except BaseException as error:
                if fd >= 0:
                    os.close(fd)
                if created:
                    try:
                        os.rmdir(name, dir_fd=self._fd)
                    except OSError:
                        pass
                if isinstance(error, ServiceError):
                    raise
                raise _error() from None

    def _remove_empty(self, name: str, identity: tuple[int, int]) -> None:
        with self._lock:
            observed = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            if (observed.st_dev, observed.st_ino) != identity or not stat.S_ISDIR(observed.st_mode):
                raise _error()
            os.rmdir(name, dir_fd=self._fd)

    def recover_empty(self, identifier: str) -> None:
        """仅可信宿主在确认原生产者结束后调用；不接管或杀死任何非空组。"""
        name = self._name(identifier)
        with self._lock:
            self._verify(allow_closed=True)
            if identifier in self._jobs:
                raise _error()
            fd = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._fd
            )
            try:
                if _numbers(_read(fd, "cgroup.events"))["populated"] != 0:
                    raise _error()
                self._remove_empty(name, _identity(fd))
            finally:
                os.close(fd)

    def close(self) -> None:
        with self._lock:
            if self._fd < 0:
                return
            self._closed = True
            jobs = tuple(self._jobs.values())
        for job in jobs:
            job.finish()
        with self._lock:
            if self._jobs:
                raise ServiceError("execution_tree_cleanup", "进程树尚未确认清空，保留原组", 503)
            if self._remove:
                observed = _directory(self.path)
                try:
                    if _identity(observed) != self._identity:
                        raise _error()
                finally:
                    os.close(observed)
                self.path.rmdir()
            if self._fd >= 0:
                os.close(self._fd)
                self._fd = -1

    def __enter__(self) -> CgroupRoot:
        self.require()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class CgroupLease:
    def __init__(
        self, root: CgroupRoot, identifier: str, name: str, fd: int, limits: SandboxTreeLimits
    ) -> None:
        self.root, self.identifier, self.name, self._fd = root, identifier, name, fd
        self.limits = limits
        self._identity = _identity(fd)
        self._member = False
        self._closed = False
        self._lock = RLock()
        self._last = SandboxTreeUsage(
            limits=limits,
            membership_verified=False,
            cpu_microseconds=None,
            memory_peak_bytes=None,
            tasks_peak=None,
            oom_kills=None,
            task_limit_hits=None,
            populated=None,
        )

    def open_join_fd(self) -> int:
        with self._lock:
            if self._closed:
                raise _error()
            self.root.require()
            return os.open(
                "cgroup.procs", os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._fd
            )

    def verify_membership(self, launcher_pid: int) -> None:
        with self._lock:
            if self._closed or _identity(self._fd) != self._identity:
                raise _error()
            self.root.require()
            self.verify_limits()
            if launcher_pid not in {
                int(value) for value in _read(self._fd, "cgroup.procs").split()
            }:
                raise _error()
            self._member = True

    def verify_limits(self) -> None:
        with self._lock:
            if self._closed or _identity(self._fd) != self._identity:
                raise _error()
            self.root.require()
            for name, expected in (
                ("memory.max", str(self.limits.memory_bytes)),
                ("memory.swap.max", "0"),
                ("memory.oom.group", "1"),
                ("pids.max", str(self.limits.process_count)),
            ):
                if _read(self._fd, name) != expected:
                    raise _error()

    def _optional(self, name: str) -> int | None:
        try:
            return int(_read(self._fd, name))
        except FileNotFoundError:
            return None

    def observe(self) -> SandboxTreeUsage:
        with self._lock:
            if self._closed:
                return self._last
            self._last = SandboxTreeUsage(
                limits=self.limits,
                membership_verified=self._member,
                cpu_microseconds=_numbers(_read(self._fd, "cpu.stat"))["usage_usec"],
                memory_peak_bytes=self._optional("memory.peak"),
                tasks_peak=self._optional("pids.peak"),
                oom_kills=_numbers(_read(self._fd, "memory.events"))["oom_kill"],
                task_limit_hits=_numbers(_read(self._fd, "pids.events"))["max"],
                populated=bool(_numbers(_read(self._fd, "cgroup.events"))["populated"]),
            )
            return self._last

    def violation(self) -> str | None:
        value = self.observe()
        if value.oom_kills:
            return "execution_tree_memory_limit"
        if value.task_limit_hits:
            return "execution_tree_process_limit"
        if (
            value.cpu_microseconds is not None
            and value.cpu_microseconds >= self.limits.cpu_seconds * 1000000
        ):
            return "execution_tree_cpu_limit"
        return None

    def stop(self) -> None:
        with self._lock:
            if not self._closed:
                _write(self._fd, "cgroup.kill", "1")

    def finish(self) -> SandboxTreeUsage:
        with self._lock:
            if self._closed:
                return self._last
            try:
                self.stop()
                deadline = time.monotonic() + 2
                value = self.observe()
                while value.populated and time.monotonic() < deadline:
                    time.sleep(0.01)
                    value = self.observe()
                if value.populated is not False:
                    return value
                self.root._remove_empty(self.name, self._identity)
                os.close(self._fd)
                self._fd = -1
                self._closed = True
                with self.root._lock:
                    self.root._jobs.pop(self.identifier, None)
                return value
            except (OSError, ValueError, ServiceError):
                self._last = self._last.model_copy(update={"populated": None})
                return self._last
