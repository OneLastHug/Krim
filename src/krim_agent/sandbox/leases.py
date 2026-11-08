"""持久项目写租约：flock防并发，遗留记录必须先核实旧写者已停止。"""

from __future__ import annotations

import fcntl
import json
import math
import os
import stat
import time
import uuid
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.xfs import internal_id, private_directory


class Lease:
    def __init__(
        self, owner: ProjectLeases, project_id: str, lease_id: str, deadline: float, fd: int
    ) -> None:
        self.owner, self.project_id, self.lease_id = owner, project_id, lease_id
        self.deadline, self.fd = deadline, fd
        self.released = False

    def check(self) -> None:
        self.owner.current()
        if self.released:
            raise ServiceError("closed")
        if time.time() >= self.deadline:
            raise ServiceError("sandbox_limit")

    def release(self, *, clean: bool) -> None:
        if self.released:
            return
        self.owner.current()
        self.owner._save(
            self.project_id,
            {
                "status": "idle" if clean is True else "blocked",
                "lease_id": self.lease_id,
                "deadline": self.deadline,
            },
        )
        self.released = True
        os.close(self.fd)
        self.owner.active.pop(self.project_id, None)


class ProjectLeases:
    def __init__(self, root: Path) -> None:
        self.root = private_directory(root)
        self.identity = (root.stat().st_dev, root.stat().st_ino)
        self.fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.closed = False
        self.active: dict[str, Lease] = {}

    def current(self) -> None:
        if self.closed:
            raise ServiceError("closed")
        try:
            private_directory(self.root)
            info = self.root.stat()
            if (info.st_dev, info.st_ino) != self.identity:
                raise ServiceError("artifact_invalid")
        except OSError:
            raise ServiceError("artifact_invalid") from None

    def _read(self, project_id: str) -> dict[str, Any]:
        try:
            fd = os.open(
                project_id + ".json", os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=self.fd
            )
        except FileNotFoundError:
            return {"status": "idle"}
        except OSError:
            raise ServiceError("facts_corrupt") from None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 4096:
                raise ServiceError("facts_corrupt")
            value = json.loads(os.read(fd, 4097))
            if (
                not isinstance(value, dict)
                or value.get("status") not in {"idle", "active", "blocked"}
                or set(value) != {"status", "lease_id", "deadline"}
            ):
                raise ValueError("无效租约记录")
            internal_id(value["lease_id"])
            if type(value["deadline"]) not in (int, float) or not math.isfinite(value["deadline"]):
                raise ValueError("无效期限")
            return value
        except (ValueError, TypeError, KeyError, ServiceError):
            raise ServiceError("facts_corrupt") from None
        finally:
            os.close(fd)

    def _save(self, project_id: str, record: dict[str, Any]) -> None:
        temporary = ".tmp-" + uuid.uuid4().hex
        fd = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.fd
        )
        try:
            raw = json.dumps(record, allow_nan=False).encode()
            os.write(fd, raw)
            os.fsync(fd)
            os.replace(temporary, project_id + ".json", src_dir_fd=self.fd, dst_dir_fd=self.fd)
            os.fsync(self.fd)
        finally:
            os.close(fd)
            try:
                os.unlink(temporary, dir_fd=self.fd)
            except FileNotFoundError:
                pass

    def _lock(self, project_id: str) -> int:
        try:
            fd = os.open(
                project_id + ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=self.fd
            )
        except OSError:
            raise ServiceError("facts_corrupt") from None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ServiceError("facts_corrupt")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError("workspace_busy") from None
            os.fsync(self.fd)
            return fd
        except BaseException:
            os.close(fd)
            raise

    def claim(self, project_id: str, lease_id: str, *, deadline: float) -> Lease:
        self.current()
        internal_id(project_id)
        internal_id(lease_id)
        if (
            type(deadline) not in (float, int)
            or not math.isfinite(deadline)
            or not time.time() < deadline <= time.time() + 300
        ):
            raise ServiceError("invalid_input", 422)
        fd = self._lock(project_id)
        try:
            record = self._read(project_id)
            if record["status"] != "idle":
                raise ServiceError("sandbox_cleanup_required")
            self._save(project_id, {"status": "active", "lease_id": lease_id, "deadline": deadline})
            lease = Lease(self, project_id, lease_id, deadline, fd)
            self.active[project_id] = lease
            return lease
        except BaseException:
            os.close(fd)
            raise

    def status(self, project_id: str) -> dict[str, Any]:
        self.current()
        internal_id(project_id)
        return self._read(project_id)

    def reconcile(self, project_id: str, lease_id: str, *, stopped: bool) -> None:
        self.current()
        internal_id(project_id)
        internal_id(lease_id)
        if stopped is not True:
            raise ServiceError("sandbox_cleanup_required")
        fd = self._lock(project_id)
        try:
            record = self._read(project_id)
            if record.get("lease_id") != lease_id:
                raise ServiceError("sandbox_cleanup_required")
            self._save(project_id, {**record, "status": "idle"})
        finally:
            os.close(fd)

    def close(self) -> None:
        if self.closed:
            return
        failure: BaseException | None = None
        try:
            for lease in list(self.active.values()):
                try:
                    lease.release(clean=False)
                except BaseException as error:
                    # 原active记录仍阻止下一写者；落盘失败也必须关闭flock句柄。
                    if not lease.released:
                        os.close(lease.fd)
                        lease.released = True
                    failure = error
        finally:
            self.active.clear()
            self.closed = True
            os.close(self.fd)
        if failure is not None:
            raise failure
