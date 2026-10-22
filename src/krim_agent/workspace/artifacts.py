"""私有内容对象与原连接 CAS；数据导入从不执行用户文件。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import stat
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.execution.types import Receipt
from krim_agent.runtime.contracts import settled
from krim_agent.storage import work
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.workspace.types import (
    ArtifactChunk,
    ArtifactFile,
    ArtifactRevision,
    ExecutionAccess,
    InputSnapshotRequest,
    path_parts,
)


def manifest_for(files: dict[str, bytes]) -> list[dict[str, Any]]:
    if type(files) is not dict or len(files) > 32:
        raise ServiceError("artifact_limit")
    total = 0
    result = []
    entries: set[str] = set()
    for name, value in sorted(files.items()):
        parts = path_parts(name)
        entries.update("/".join(parts[:i]) for i in range(1, len(parts) + 1))
        if len(entries) > 128:
            raise ServiceError("artifact_limit")
        if type(value) is not bytes or len(value) > 1048576:
            raise ServiceError("artifact_limit")
        total += len(value)
        result.append(
            ArtifactFile(
                path=name, size=len(value), sha256=hashlib.sha256(value).hexdigest()
            ).model_dump(mode="json")
        )
    if total > 8388608:
        raise ServiceError("artifact_limit")
    return result


def directory_fd(path: Path, *, create: bool = False) -> int:
    """从根dirfd逐段打开；任何父段替换为链接均失败关闭。"""
    if not path.is_absolute():
        raise ServiceError("artifact_invalid")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            info = os.stat(part, dir_fd=fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                raise ServiceError("artifact_invalid")
            child = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd
            )
            actual = os.fstat(child)
            if (actual.st_ino, actual.st_dev) != (info.st_ino, info.st_dev):
                os.close(child)
                raise ServiceError("artifact_invalid")
            os.close(fd)
            fd = child
        result, fd = fd, -1
        return result
    except OSError:
        raise ServiceError("artifact_invalid") from None
    finally:
        if fd != -1:
            os.close(fd)


class ArtifactStore:
    def __init__(
        self, store: SQLiteStore, root: Path, *, hook: Callable[..., Awaitable[None]] | None = None
    ) -> None:
        work.require(store)
        self.store, self.root, self.hook = store, root.absolute(), hook
        self.closed = False
        self.closing = False
        self.jobs: set[asyncio.Task[Any]] = set()
        self._close_task: asyncio.Task[None] | None = None
        self.fd = directory_fd(self.root, create=not store.readonly)
        self.identity = os.fstat(self.fd)

    def _current(self) -> None:
        if self.closed or self.closing:
            raise ServiceError("closed")
        current = self.root.lstat()
        if (current.st_dev, current.st_ino) != (self.identity.st_dev, self.identity.st_ino):
            raise ServiceError("artifact_invalid")

    async def _point(self, phase: str) -> None:
        if self.hook is not None:
            await self.hook(phase)
        self._current()

    async def _owned[T](self, operation: Coroutine[Any, Any, T]) -> T:
        task: asyncio.Task[T] = asyncio.create_task(operation)
        self.jobs.add(task)
        cancelled = False
        try:
            while True:
                try:
                    value = await asyncio.shield(task)
                    break
                except asyncio.CancelledError:
                    if task.cancelled():
                        raise
                    cancelled = True
                    task.cancel()
            if cancelled:
                raise asyncio.CancelledError
            return value
        finally:
            if not task.done():
                task.cancel()
                await settled(asyncio.gather(task, return_exceptions=True))
            if task.done():
                self.jobs.discard(task)

    def _blob(self, digest: str, size: int) -> bytes:
        info = os.stat(digest, dir_fd=self.fd, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != size:
            raise ServiceError("artifact_invalid")
        fd = os.open(
            digest, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.fd
        )
        try:
            after = os.fstat(fd)
            if (info.st_ino, info.st_dev, info.st_size, info.st_nlink) != (
                after.st_ino,
                after.st_dev,
                after.st_size,
                after.st_nlink,
            ):
                raise ServiceError("artifact_invalid")
            data = bytearray()
            while len(data) <= size:
                chunk = os.read(fd, min(65536, size + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
            if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                raise ServiceError("artifact_invalid")
            return bytes(data)
        finally:
            os.close(fd)

    def _stage(self, files: dict[str, bytes]) -> list[dict[str, Any]]:
        self._current()
        manifest = manifest_for(files)
        for item in manifest:
            name = ".stage-" + uuid.uuid4().hex
            fd = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=self.fd,
            )
            try:
                try:
                    data = memoryview(files[item["path"]])
                    while data:
                        written = os.write(fd, data)
                        if written <= 0:
                            raise ServiceError("artifact_invalid")
                        data = data[written:]
                    os.fsync(fd)
                finally:
                    os.close(fd)
                try:
                    os.link(
                        name,
                        item["sha256"],
                        src_dir_fd=self.fd,
                        dst_dir_fd=self.fd,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    self._blob(item["sha256"], item["size"])
            finally:
                os.unlink(name, dir_fd=self.fd)
            self._blob(item["sha256"], item["size"])
        os.fsync(self.fd)
        return manifest

    async def _known(
        self, access: ExecutionAccess, kind: str, key: str, digest: str | None = None
    ) -> ArtifactRevision | None:
        found = await self.store.work_read(
            lambda conn: work.by_key(conn, access.session_id, kind, key, access)
        )
        if found is not None and digest is not None and found.request_hash != digest:
            raise ServiceError("artifact_conflict")
        return found

    async def by_key(
        self, access: ExecutionAccess, key: str, *, kind: str = "input"
    ) -> ArtifactRevision:
        self._current()
        found = await self._known(access, kind, key)
        if found is None:
            raise ServiceError("not_found", 404)
        return found

    async def get(self, access: ExecutionAccess, revision_id: str) -> ArtifactRevision:
        self._current()
        return await self.store.work_read(
            lambda conn: work.get_revision(conn, access.session_id, revision_id, access)
        )

    async def head(self, access: ExecutionAccess) -> ArtifactRevision:
        self._current()

        async def read(conn: Any) -> ArtifactRevision:
            row = await work.row(conn, access.session_id, access)
            return await work.get_revision(conn, access.session_id, row["head_revision"], access)

        return await self.store.work_read(read)

    async def input(
        self, access: ExecutionAccess, request: InputSnapshotRequest
    ) -> ArtifactRevision:
        self._current()

        async def owned() -> ArtifactRevision:
            await self.store.work_read(
                lambda conn: work.row(conn, access.session_id, access, write=True)
            )
            digest = request.request_hash()
            old = await self._known(access, "input", request.request_key, digest)
            if old is not None:
                return old
            if self.store.readonly:
                raise ServiceError("readonly")
            if not self.store.healthy:
                raise ServiceError("storage_unavailable", 503)
            await self._point("before_stage")
            manifest = self._stage({f.path: f.content() for f in request.files})
            await self._point("before_cas")

            async def commit(conn: Any) -> ArtifactRevision:
                return await work.publish(
                    conn,
                    access.session_id,
                    access,
                    kind="input",
                    key=request.request_key,
                    digest=digest,
                    expected=request.expected_head_revision,
                    manifest=manifest,
                )

            result = await self.store.work_write(
                commit, lambda: self._known(access, "input", request.request_key, digest)
            )
            await self._point("after_cas")
            return result

        return await self._owned(owned())

    async def publish(
        self,
        access: ExecutionAccess,
        *,
        key: str,
        expected: int,
        run_id: str,
        call_id: str,
        receipt: Receipt,
    ) -> ArtifactRevision:
        self._current()
        if (
            receipt.output_complete is not True
            or receipt.cleanup != "clean"
            or type(receipt.exit_code) is not int
        ):
            raise ServiceError("execution_unknown")
        if type(receipt.files) is not dict:
            raise ServiceError("artifact_invalid")
        files = dict(receipt.files)
        invalid: ServiceError | None = None
        try:
            manifest = manifest_for(files)
        except ServiceError as error:
            # 产物不可发布不等于执行未知；授权及effect核实后仍保存已知退出事实。
            invalid = error
            manifest = []
        digest = fingerprint(
            {
                "key": key,
                "expected": expected,
                "run_id": run_id,
                "call_id": call_id,
                "manifest": manifest,
                "exit_code": receipt.exit_code,
            }
        )

        async def owned() -> ArtifactRevision:
            await self.store.work_read(
                lambda conn: work.row(conn, access.session_id, access, write=True)
            )
            if invalid is None:
                old = await self._known(access, "effect", key, digest)
                if old is not None:
                    return old
            actual = {
                "executor_started": True,
                "exit_code": receipt.exit_code,
                "output_complete": True,
                "cleanup": receipt.cleanup,
                "code": receipt.code,
            }

            async def record(conn: Any) -> dict[str, Any]:
                return await work.record_result(conn, access, call_id, actual, run_id=run_id)

            async def confirmed() -> dict[str, Any] | None:
                found = await self.store.work_read(lambda conn: work.effect(conn, access, call_id))
                return actual if found and found["receipt"] == actual else None

            await self.store.work_write(record, confirmed)
            if invalid is not None:
                raise invalid
            await self._point("before_stage")
            self._stage(files)
            await self._point("before_cas")

            async def commit(conn: Any) -> ArtifactRevision:
                return await work.publish(
                    conn,
                    access.session_id,
                    access,
                    kind="effect",
                    key=key,
                    digest=digest,
                    expected=expected,
                    manifest=manifest,
                    run_id=run_id,
                    call_id=call_id,
                    receipt={
                        "executor_started": True,
                        "exit_code": receipt.exit_code,
                        "output_complete": True,
                        "cleanup": receipt.cleanup,
                        "code": receipt.code,
                    },
                )

            result = await self.store.work_write(
                commit, lambda: self._known(access, "effect", key, digest)
            )
            await self._point("after_cas")
            return result

        return await self._owned(owned())

    async def chunk(
        self,
        access: ExecutionAccess,
        revision_id: str,
        path: str,
        *,
        offset: int = 0,
        length: int = 16384,
        model: bool = False,
    ) -> ArtifactChunk:
        self._current()
        path_parts(path)
        if (
            type(offset) is not int
            or offset < 0
            or type(length) is not int
            or not 1 <= length <= (2048 if model else 16384)
        ):
            raise ServiceError("invalid_input", 422)
        revision = await self.get(access, revision_id)
        self._current()
        found = next((f for f in revision.manifest if f.path == path), None)
        if found is None:
            raise ServiceError("not_found", 404)
        content = self._blob(found.sha256, found.size)
        data = content[offset : offset + length]
        return ArtifactChunk(
            revision_id=revision_id,
            path=path,
            offset=offset,
            total_size=found.size,
            length=len(data),
            sha256=found.sha256,
            data=base64.b64encode(data).decode(),
        )

    async def snapshot_directory(self, root: Path) -> dict[str, bytes]:
        """仅供可信内部stage消费；HTTP/模型不能登记host路径。"""
        self._current()
        return await self._owned(self._snapshot_directory(root))

    async def _snapshot_directory(self, root: Path) -> dict[str, bytes]:
        self._current()
        for parent in (root, *root.parents):
            if not stat.S_ISDIR(parent.lstat().st_mode):
                raise ServiceError("artifact_invalid")
        count = 0
        result: dict[str, bytes] = {}

        async def walk(fd: int, prefix: str) -> None:
            nonlocal count
            for name in sorted(os.listdir(fd)):
                count += 1
                if count > 128:
                    raise ServiceError("artifact_limit")
                path = prefix + name
                path_parts(path)
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if not stat.S_ISDIR(info.st_mode) and (
                    not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                ):
                    raise ServiceError("artifact_invalid")
                await self._point("lstat_open")
                child = os.open(
                    name,
                    os.O_RDONLY
                    | os.O_NOFOLLOW
                    | os.O_CLOEXEC
                    | os.O_NONBLOCK
                    | (os.O_DIRECTORY if stat.S_ISDIR(info.st_mode) else 0),
                    dir_fd=fd,
                )
                try:
                    actual = os.fstat(child)
                    if (info.st_ino, info.st_dev, info.st_mode, info.st_nlink) != (
                        actual.st_ino,
                        actual.st_dev,
                        actual.st_mode,
                        actual.st_nlink,
                    ):
                        raise ServiceError("artifact_invalid")
                    if stat.S_ISDIR(actual.st_mode):
                        await walk(child, path + "/")
                    else:
                        if actual.st_size > 1048576:
                            raise ServiceError("artifact_limit")
                        data = bytearray()
                        while len(data) <= 1048576:
                            chunk = os.read(child, min(65536, 1048577 - len(data)))
                            if not chunk:
                                break
                            data.extend(chunk)
                        final = os.fstat(child)
                        if (final.st_size, final.st_mtime_ns, final.st_ctime_ns) != (
                            actual.st_size,
                            actual.st_mtime_ns,
                            actual.st_ctime_ns,
                        ) or len(data) != actual.st_size:
                            raise ServiceError("artifact_invalid")
                        result[path] = bytes(data)
                        manifest_for(result)
                finally:
                    os.close(child)

        root_fd = directory_fd(root)
        try:
            await walk(root_fd, "")
            return result
        except OSError:
            raise ServiceError("artifact_invalid") from None
        finally:
            os.close(root_fd)

    async def close(self) -> None:
        if self.closed:
            return
        if self._close_task is None or self._close_task.done():
            self.closing = True

            async def owned() -> None:
                jobs = tuple(self.jobs)
                for task in jobs:
                    task.cancel()
                results = await asyncio.gather(*jobs, return_exceptions=True)
                failures = [
                    r
                    for r in results
                    if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError)
                ]
                self.jobs.difference_update(jobs)
                # 业务失败仍需收束目录FD；close本身失败则后续只重试此清理，不重发作业。
                os.close(self.fd)
                self.closed = True
                if failures:
                    raise ServiceError("artifact_invalid")

            self._close_task = asyncio.create_task(owned())
        await settled(self._close_task)
