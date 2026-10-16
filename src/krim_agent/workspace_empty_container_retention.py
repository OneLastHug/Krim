"""原恢复事务空容器的条件回收；不遍历删除payload，也不核实工作区效果。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Literal

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_restore_writer import (
    DirectoryRestoreStage,
    WorkspaceDirectoryRestoreWriter,
)
from krim_agent.workspace_tree_snapshot import _path


@dataclass(frozen=True)
class EmptyContainerObservation:
    stage: DirectoryRestoreStage = field(repr=False)
    status: Literal["empty", "not_empty", "absent", "unverifiable"]
    fingerprint: str | None
    error_code: str | None = None


@dataclass(frozen=True)
class EmptyContainerReceipt:
    stage: DirectoryRestoreStage = field(repr=False)
    expected_fingerprint: str
    status: Literal["removed", "already_absent", "rejected", "unknown"]
    changed: bool
    recovery_required: bool
    observed_fingerprint: str | None = None
    error_code: str | None = None


class WorkspaceEmptyContainerRetention:
    """可信层必须从原持久操作取Stage；本核心没有公共参数或自动清理入口。"""

    def __init__(self, workspace: Workspace, *, lock_timeout: float = 5) -> None:
        self.base = WorkspaceDirectoryRestoreWriter(workspace, lock_timeout=lock_timeout)
        self.workspace = workspace
        self.lock_timeout = self.base.lock_timeout

    @staticmethod
    def _error(code: str, *, status: int = 409) -> ServiceError:
        return ServiceError(
            "workspace_empty_container_" + code, "原空容器身份、版本或回收条件不符", status
        )

    @classmethod
    def _stage(cls, value: DirectoryRestoreStage) -> DirectoryRestoreStage:
        try:
            if type(value) is not DirectoryRestoreStage:
                raise ValueError("材料类型无效")
            _path(value.root_path)
            if type(value.root_path) is not str or len(value.root_path) > 512:
                raise ValueError("原目录路径无效")
            if type(value.token) is not str or re.fullmatch(r"[a-f0-9]{32}", value.token) is None:
                raise ValueError("原token无效")
            if (
                any(
                    type(identity) is not tuple
                    or len(identity) != 2
                    or any(type(item) is not int or item < 0 for item in identity)
                    for identity in (
                        value.parent_identity,
                        value.container_identity,
                        value.container_owner,
                    )
                )
                or value.parent_identity[0] != value.container_identity[0]
            ):
                raise ValueError("原身份无效")
            return replace(value)
        except (TypeError, ValueError, AttributeError, ServiceError):
            raise cls._error("parameters", status=422) from None

    def _fingerprint(self, stage: DirectoryRestoreStage, info: os.stat_result | None) -> str:
        values = (
            [
                info.st_dev,
                info.st_ino,
                info.st_mode,
                info.st_uid,
                info.st_gid,
                info.st_nlink,
                info.st_mtime_ns,
                info.st_ctime_ns,
            ]
            if info is not None
            else None
        )
        data = json.dumps(
            [
                "krim-empty-transaction-container-v1",
                self.workspace.fingerprint,
                stage.root_path,
                stage.token,
                stage.parent_identity,
                stage.container_identity,
                stage.container_owner,
                values,
            ],
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _empty(fd: int) -> bool:
        # 只看第一个目录项；不读取其名称/内容，不跟随任何子链接。
        fresh = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        try:
            with os.scandir(fresh) as entries:
                return next(entries, None) is None
        finally:
            os.close(fresh)

    def _check(self, deadline: float, abort: Callable[[], bool]) -> None:
        if abort():
            raise self._error("cancelled")
        if time.monotonic() >= deadline:
            raise self._error("timeout")

    def _absence(self, parent: int, path: str, stage: DirectoryRestoreStage) -> str:
        self.base.directories._files._check_parent(path, stage.parent_identity)
        try:
            os.stat("._krim-tx-" + stage.token, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return self._fingerprint(stage, None)
        raise self._error("container")

    def inspect(
        self, stage: DirectoryRestoreStage, *, authorize: Callable[[], None]
    ) -> EmptyContainerObservation:
        stage = self._stage(stage)
        if not callable(authorize):
            raise self._error("parameters", status=422)
        deadline = time.monotonic() + self.lock_timeout

        def abort() -> bool:
            self._check(deadline, lambda: False)
            return False

        try:
            authorize()
            self._check(deadline, lambda: False)
            with self.base.directories._locked_parent(
                stage.root_path, abort, require_writable=False
            ) as (parent, path, _name):
                self._check(deadline, lambda: False)
                self.base.directories._files._check_parent(path, stage.parent_identity)
                try:
                    container_context = self.base._container(parent, stage)
                    with container_context as fd:
                        empty = self._empty(fd)
                        first = os.fstat(fd)
                        authorize()
                        self._check(deadline, lambda: False)
                        self.base.directories._files._check_parent(path, stage.parent_identity)
                        self.base._check_container(parent, stage, fd)
                        if empty != self._empty(fd) or self._fingerprint(
                            stage, first
                        ) != self._fingerprint(stage, os.fstat(fd)):
                            raise self._error("changed")
                        return EmptyContainerObservation(
                            stage,
                            "empty" if empty else "not_empty",
                            self._fingerprint(stage, first),
                        )
                except FileNotFoundError:
                    authorize()
                    self._check(deadline, lambda: False)
                    return EmptyContainerObservation(
                        stage, "absent", self._absence(parent, path, stage)
                    )
        except Exception as error:
            return EmptyContainerObservation(
                stage,
                "unverifiable",
                None,
                error.code if isinstance(error, ServiceError) else "workspace_empty_container_io",
            )

    def remove(
        self,
        stage: DirectoryRestoreStage,
        *,
        expected_fingerprint: str,
        confirmed: bool,
        authorize: Callable[[], None],
        record_intent: Callable[[EmptyContainerObservation], None],
        cancelled: Callable[[], bool] | None = None,
    ) -> EmptyContainerReceipt:
        stage = self._stage(stage)
        if (
            type(expected_fingerprint) is not str
            or re.fullmatch(r"[a-f0-9]{64}", expected_fingerprint) is None
            or type(confirmed) is not bool
            or not callable(authorize)
            or not callable(record_intent)
            or cancelled is not None
            and not callable(cancelled)
        ):
            raise self._error("parameters", status=422)
        if not confirmed:
            raise self._error("confirmation")
        abort = cancelled if cancelled is not None else lambda: False
        deadline = time.monotonic() + self.lock_timeout
        attempted = False

        def check() -> None:
            self._check(deadline, abort)

        def lock_abort() -> bool:
            check()
            return False

        def guard(parent: int, path: str, fd: int) -> None:
            check()
            self.base.directories._files._check_parent(path, stage.parent_identity)
            self.base._check_container(parent, stage, fd)
            if not self._empty(fd):
                raise self._error("not_empty")
            if self._fingerprint(stage, os.fstat(fd)) != expected_fingerprint:
                raise self._error("version")

        try:
            authorize()
            check()
            with self.base.directories._locked_parent(stage.root_path, lock_abort) as (
                parent,
                path,
                _name,
            ):
                check()
                self.base.directories._files._check_parent(path, stage.parent_identity)
                try:
                    context = self.base._container(parent, stage)
                    with context as fd:
                        guard(parent, path, fd)
                        authorize()
                        guard(parent, path, fd)
                        record_intent(
                            EmptyContainerObservation(replace(stage), "empty", expected_fingerprint)
                        )
                        authorize()
                        guard(parent, path, fd)
                        check()
                        attempted = True
                        os.rmdir("._krim-tx-" + stage.token, dir_fd=parent)
                        os.fsync(parent)
                        check()
                        if os.fstat(fd).st_nlink != 0:
                            raise self._error("container")
                        absent = self._absence(parent, path, stage)
                        check()
                        return EmptyContainerReceipt(
                            stage, expected_fingerprint, "removed", True, False, absent
                        )
                except FileNotFoundError:
                    if attempted:
                        raise
                    authorize()
                    check()
                    absent = self._absence(parent, path, stage)
                    return EmptyContainerReceipt(
                        stage, expected_fingerprint, "already_absent", False, False, absent
                    )
        except Exception as error:
            return EmptyContainerReceipt(
                stage,
                expected_fingerprint,
                "unknown" if attempted else "rejected",
                attempted,
                attempted,
                error_code=error.code
                if isinstance(error, ServiceError)
                else "workspace_empty_container_io",
            )
