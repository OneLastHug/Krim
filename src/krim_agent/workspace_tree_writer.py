"""可信非空树条件移动；原全部身份/字节、双父、unknown与反向准备。"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_snapshot import (
    WorkspaceTreeSnapshot,
    WorkspaceTreeSnapshotReader,
    _path,
)
from krim_agent.workspace_writer import _rename_function, rename_at


@dataclass(frozen=True)
class PreparedTreeMove:
    before: WorkspaceTreeSnapshot
    target_path: str
    target_parent_identity: tuple[int, int]
    token: str


@dataclass(frozen=True)
class TreeMoveReceipt:
    source_path: str
    target_path: str
    status: Literal["done", "rejected", "uncertain"]
    before_fingerprint: str
    after_fingerprint: str | None
    operation_token: str
    changed: bool
    error_code: str | None
    recovery_required: bool


@dataclass(frozen=True)
class TreeMoveEffect:
    receipt: TreeMoveReceipt
    observed_after: WorkspaceTreeSnapshot | None = None


class WorkspaceTreeWriter:
    def __init__(
        self,
        workspace: Workspace,
        *,
        max_entries: int = 1024,
        max_total_bytes: int = 33_554_432,
        max_depth: int = 64,
        lock_timeout: float = 5,
    ) -> None:
        self.workspace = workspace
        self.reader = WorkspaceTreeSnapshotReader(
            workspace,
            max_entries=max_entries,
            max_total_bytes=max_total_bytes,
            max_depth=max_depth,
            lock_timeout=lock_timeout,
        )
        self.directories = self.reader.directories
        self.lock_timeout = lock_timeout

    @staticmethod
    def _paths(source: str, target: str) -> tuple[str, str, str, str]:
        left, right = _path(source), _path(target)
        if left == right or left == right[: len(left)] or right == left[: len(right)]:
            raise ServiceError("workspace_tree_move_path", "树移动不能相同或嵌套源与目标", 422)
        return "/".join(left[:-1]) or ".", left[-1], "/".join(right[:-1]) or ".", right[-1]

    @staticmethod
    def _missing(fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return True
        return False

    def _check(self, deadline: float, abort: Callable[[], bool]) -> None:
        if abort():
            raise ServiceError("workspace_write_cancelled", "树移动在发布前取消", 409)
        if time.monotonic() >= deadline:
            raise ServiceError("workspace_tree_move_timeout", "树移动超过物理工作预算", 409)

    def _capture(
        self, fd: int, name: str, path: str, check: Callable[[], None]
    ) -> WorkspaceTreeSnapshot:
        check()
        first = self.reader._observe(fd, name, path, check)
        second = self.reader._observe(fd, name, path, check)
        if first != second:
            raise ServiceError("workspace_tree_snapshot_changed", "原树读取期间变化", 409)
        identity = os.fstat(fd).st_dev, os.fstat(fd).st_ino
        self.directories._files._check_parent(path.rpartition("/")[0] or ".", identity)
        check()
        return WorkspaceTreeSnapshot(path, self.workspace.fingerprint, identity, first)

    def prepare(
        self,
        source_path: str,
        target_path: str,
        *,
        expected_fingerprint: str,
        authorize: Callable[[], None] | None = None,
    ) -> PreparedTreeMove:
        source_parent, source_name, target_parent, target_name = self._paths(
            source_path, target_path
        )
        deadline = time.monotonic() + self.lock_timeout

        def check() -> None:
            self._check(deadline, lambda: False)
            if authorize is not None:
                authorize()

        with self.directories._locked_parents(
            (source_path, target_path), lambda: False, require_writable=False
        ) as parents:
            source_fd, target_fd = parents[source_parent], parents[target_parent]
            before = self._capture(source_fd, source_name, source_path, check)
            target_identity = os.fstat(target_fd).st_dev, os.fstat(target_fd).st_ino
            if before.parent_identity[0] != target_identity[0]:
                raise ServiceError("workspace_tree_snapshot_device", "树移动不能跨设备", 403)
            if before.fingerprint != expected_fingerprint or not self._missing(
                target_fd, target_name
            ):
                raise ServiceError(
                    "workspace_tree_move_conflict", "原完整树版本或目标条件不匹配", 409
                )
            self.directories._files._check_parent(target_parent, target_identity)
            check()
            return PreparedTreeMove(before, target_path, target_identity, uuid4().hex)

    def _validate(self, change: PreparedTreeMove) -> tuple[str, str, str, str]:
        paths = self._paths(change.before.root_path, change.target_path)
        if change.before.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "原树移动不属于工作区版本", 403)
        if len(change.token) != 32 or any(char not in "0123456789abcdef" for char in change.token):
            raise ServiceError("workspace_tree_move_token", "原树移动token无效", 409)
        return paths

    def _parents(self, change: PreparedTreeMove, parents: dict[str, int]) -> None:
        source_parent, _source_name, target_parent, _target_name = self._validate(change)
        for path, identity in (
            (source_parent, change.before.parent_identity),
            (target_parent, change.target_parent_identity),
        ):
            info = os.fstat(parents[path])
            if (info.st_dev, info.st_ino) != identity:
                raise ServiceError("workspace_parent_changed", "原树双父身份变化", 409)
            self.directories._files._check_parent(path, identity)

    def _effect(
        self,
        change: PreparedTreeMove,
        status: Literal["done", "rejected", "uncertain"],
        *,
        code: str | None = None,
        after: WorkspaceTreeSnapshot | None = None,
        changed: bool = False,
    ) -> TreeMoveEffect:
        return TreeMoveEffect(
            TreeMoveReceipt(
                change.before.root_path,
                change.target_path,
                status,
                change.before.fingerprint,
                after.fingerprint if after else None,
                change.token,
                changed,
                code,
                status == "uncertain",
            ),
            after,
        )

    def _moved(self, change: PreparedTreeMove, after: WorkspaceTreeSnapshot) -> bool:
        before = change.before
        if (
            after.root_path != change.target_path
            or after.workspace_fingerprint != before.workspace_fingerprint
            or after.parent_identity != change.target_parent_identity
            or len(after.entries) != len(before.entries)
        ):
            return False
        for position, (old, current) in enumerate(zip(before.entries, after.entries, strict=True)):
            if current.path != change.target_path + old.path.removeprefix(before.root_path):
                return False
            if old.file is not None:
                if current.directory is not None or current.file != old.file:
                    return False
            elif current.file is not None or not self.directories._same(
                current.directory, old.directory, displaced=position == 0
            ):
                return False
        return True

    def apply(
        self,
        change: PreparedTreeMove,
        *,
        cancelled: Callable[[], bool] | None = None,
        authorize: Callable[[], None] | None = None,
        record_intent: Callable[[PreparedTreeMove], None] | None = None,
    ) -> TreeMoveEffect:
        source_parent, source_name, target_parent, target_name = self._validate(change)
        abort = cancelled or (lambda: False)
        deadline = time.monotonic() + self.lock_timeout
        attempted = False

        def check() -> None:
            self._check(deadline, abort)

        try:
            with self.directories._locked_parents(
                (change.before.root_path, change.target_path), abort
            ) as parents:
                source_fd, target_fd = parents[source_parent], parents[target_parent]

                def original_conditions() -> None:
                    check()
                    self._parents(change, parents)
                    if authorize is not None:
                        authorize()
                    current = self._capture(source_fd, source_name, change.before.root_path, check)
                    if current != change.before or not self._missing(target_fd, target_name):
                        raise ServiceError(
                            "workspace_tree_move_conflict", "原树移动条件已变化", 409
                        )
                    check()

                original_conditions()
                _rename_function()
                if record_intent is not None:
                    record_intent(change)
                original_conditions()
                # 完整扫描后再核实原资格，避免扫描期间Run/租约已失效仍发布。
                if authorize is not None:
                    authorize()
                self._parents(change, parents)
                check()
                attempted = True
                try:
                    rename_at(source_fd, source_name, target_name, target_fd=target_fd)
                except FileExistsError:
                    return self._effect(change, "rejected", code="workspace_tree_move_conflict")
                os.fsync(source_fd)
                if target_fd != source_fd:
                    os.fsync(target_fd)
                # 原rename已发生时忽略取消信号，核实真实效果；不自动回滚。
                after = self._capture(
                    target_fd,
                    target_name,
                    change.target_path,
                    lambda: self._check(deadline, lambda: False),
                )
                self._parents(change, parents)
                if not self._missing(source_fd, source_name) or not self._moved(change, after):
                    return self._effect(
                        change,
                        "uncertain",
                        code="workspace_tree_move_postcheck",
                        after=after,
                        changed=True,
                    )
                return self._effect(change, "done", after=after, changed=True)
        except (OSError, ServiceError) as error:
            return self._effect(
                change,
                "uncertain" if attempted else "rejected",
                code=error.code if isinstance(error, ServiceError) else "workspace_tree_move_io",
                changed=attempted,
            )

    def reconcile(self, change: PreparedTreeMove) -> TreeMoveEffect:
        source_parent, source_name, target_parent, target_name = self._validate(change)
        deadline = time.monotonic() + self.lock_timeout

        def check() -> None:
            self._check(deadline, lambda: False)

        try:
            with self.directories._locked_parents(
                (change.before.root_path, change.target_path), lambda: False, require_writable=False
            ) as parents:
                self._parents(change, parents)
                source_fd, target_fd = parents[source_parent], parents[target_parent]
                source_missing = self._missing(source_fd, source_name)
                target_missing = self._missing(target_fd, target_name)
                if source_missing and not target_missing:
                    after = self._capture(target_fd, target_name, change.target_path, check)
                    if self._moved(change, after):
                        return self._effect(change, "done", after=after, changed=True)
                elif target_missing and not source_missing:
                    before = self._capture(source_fd, source_name, change.before.root_path, check)
                    if before == change.before:
                        return self._effect(
                            change, "rejected", code="workspace_tree_move_not_observed"
                        )
                return self._effect(
                    change, "uncertain", code="workspace_tree_move_ambiguous", changed=True
                )
        except (OSError, ServiceError) as error:
            return self._effect(
                change,
                "uncertain",
                code=error.code if isinstance(error, ServiceError) else "workspace_tree_move_io",
                changed=True,
            )

    def prepare_undo(self, change: PreparedTreeMove, effect: TreeMoveEffect) -> PreparedTreeMove:
        self._validate(change)
        receipt = effect.receipt
        if (
            receipt.status != "done"
            or receipt.operation_token != change.token
            or receipt.source_path != change.before.root_path
            or receipt.target_path != change.target_path
            or receipt.before_fingerprint != change.before.fingerprint
            or effect.observed_after is None
            or receipt.after_fingerprint != effect.observed_after.fingerprint
            or not self._moved(change, effect.observed_after)
        ):
            raise ServiceError("workspace_tree_move_receipt", "树撤销须绑定原真实完成回执", 409)
        reverse = self.prepare(
            change.target_path,
            change.before.root_path,
            expected_fingerprint=effect.observed_after.fingerprint,
        )
        if (
            reverse.before.parent_identity != change.target_parent_identity
            or reverse.target_parent_identity != change.before.parent_identity
        ):
            raise ServiceError("workspace_parent_changed", "原树撤销双父身份变化", 409)
        return reverse
