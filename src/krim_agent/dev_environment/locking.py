"""Cross-process project serialization on a trusted, non-project host directory."""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
import stat
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from .model import EnvironmentError, RuntimeSpec


class ProjectLocks:
    def __init__(self, root: Path, *, timeout_seconds: float = 30.0) -> None:
        self.root = root
        self.timeout_seconds = timeout_seconds
        self._holders: dict[tuple[str, str], asyncio.Task[Any] | None] = {}

    @asynccontextmanager
    async def hold(self, owner: str, project: str) -> AsyncIterator[None]:
        key = (owner, project)
        task = asyncio.current_task()
        # C's startup handshake may inspect in the same task while prepare holds
        # the launch fence. Child tasks never inherit this ownership.
        if key in self._holders and self._holders[key] is task:
            yield
            return
        # Provisioned by deployment, never inside a writable project mount.
        if self.root.resolve(strict=True) != self.root:
            raise EnvironmentError("lock_directory_untrusted")
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        fd = None
        try:
            metadata = os.fstat(directory)
            if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise EnvironmentError("lock_directory_untrusted")
            name = hashlib.sha256(f"{owner}\0{project}".encode()).hexdigest() + ".lock"
            fd = os.open(name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                         0o600, dir_fd=directory)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) & 0o077):
                raise EnvironmentError("lock_file_untrusted")
            deadline = time.monotonic() + self.timeout_seconds
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise EnvironmentError("project_busy") from None
                    await asyncio.sleep(0.02)
            self._holders[key] = task
            try:
                yield
            finally:
                self._holders.pop(key, None)
        finally:
            if fd is not None:
                os.close(fd)
            os.close(directory)


def verify_mount(record: RuntimeSpec) -> None:
    try:
        path = record.mount.path
        if path.resolve(strict=True) != path:
            raise EnvironmentError("project_mount_drift")
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
        finally:
            os.close(fd)
        if ((info.st_dev, info.st_ino) != (record.mount.device, record.mount.inode)
                or (info.st_uid, info.st_gid) != (65532, 65532)):
            raise EnvironmentError("project_mount_drift")
    except OSError:
        raise EnvironmentError("project_mount_unavailable") from None
