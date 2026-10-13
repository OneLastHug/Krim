"""已有两完整树的可信条件原子交换；真实双after、unknown核实及反向准备。"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot
from krim_agent.workspace_tree_writer import PreparedTreeMove, WorkspaceTreeWriter
from krim_agent.workspace_writer import _rename_function, rename_at


@dataclass(frozen=True)
class PreparedTreeExchange:
    left: WorkspaceTreeSnapshot
    right: WorkspaceTreeSnapshot
    token: str


@dataclass(frozen=True)
class TreeExchangeReceipt:
    left_path: str
    right_path: str
    status: Literal["done", "rejected", "uncertain"]
    left_before_fingerprint: str
    right_before_fingerprint: str
    left_after_fingerprint: str | None
    right_after_fingerprint: str | None
    operation_token: str
    changed: bool
    error_code: str | None
    recovery_required: bool


@dataclass(frozen=True)
class TreeExchangeEffect:
    receipt: TreeExchangeReceipt
    observed_left: WorkspaceTreeSnapshot | None = None
    observed_right: WorkspaceTreeSnapshot | None = None


class WorkspaceTreeExchangeWriter:
    def __init__(
        self,
        workspace: Workspace,
        *,
        max_entries: int = 1024,
        max_total_bytes: int = 33_554_432,
        max_depth: int = 64,
        lock_timeout: float = 5,
    ) -> None:
        if type(max_entries) is not int or not 2 <= max_entries <= 16384:
            raise ValueError("原两树合计节点上限无效")
        self.workspace = workspace
        self.trees = WorkspaceTreeWriter(
            workspace,
            max_entries=max_entries - 1,
            max_total_bytes=max_total_bytes,
            max_depth=max_depth,
            lock_timeout=lock_timeout,
        )
        self.max_entries, self.max_total_bytes, self.max_depth = (
            max_entries,
            max_total_bytes,
            max_depth,
        )
        self.lock_timeout = self.trees.lock_timeout

    def _capture(
        self,
        left: str,
        right: str,
        parents: dict[str, int],
        check: Callable[[], None],
    ) -> tuple[WorkspaceTreeSnapshot, WorkspaceTreeSnapshot]:
        left_parent, left_name, right_parent, right_name = self.trees._paths(left, right)
        first = self.trees._capture(parents[left_parent], left_name, left, check)
        remaining = WorkspaceTreeWriter(
            self.workspace,
            max_entries=self.max_entries - len(first.entries),
            max_total_bytes=self.max_total_bytes - first.total_bytes,
            max_depth=self.max_depth,
            lock_timeout=self.lock_timeout,
        )
        second = remaining._capture(parents[right_parent], right_name, right, check)
        if first.parent_identity[0] != second.parent_identity[0]:
            raise ServiceError("workspace_tree_snapshot_device", "原两树不能跨设备交换", 403)
        return first, second

    @staticmethod
    def _moves(change: PreparedTreeExchange) -> tuple[PreparedTreeMove, PreparedTreeMove]:
        return (
            PreparedTreeMove(
                change.left, change.right.root_path, change.right.parent_identity, change.token
            ),
            PreparedTreeMove(
                change.right, change.left.root_path, change.left.parent_identity, change.token
            ),
        )

    def _validate(self, change: PreparedTreeExchange) -> tuple[str, str, str, str]:
        left, right = self._moves(change)
        result = self.trees._validate(left)
        self.trees._validate(right)
        if (
            len(change.left.entries) + len(change.right.entries) > self.max_entries
            or change.left.total_bytes + change.right.total_bytes > self.max_total_bytes
            or change.left.parent_identity[0] != change.right.parent_identity[0]
        ):
            raise ServiceError(
                "workspace_tree_exchange_material", "原两树材料容量或设备不匹配", 409
            )
        return result

    def _parents(self, change: PreparedTreeExchange, parents: dict[str, int]) -> None:
        for move in self._moves(change):
            self.trees._parents(move, parents)

    @staticmethod
    def _swapped(
        change: PreparedTreeExchange, left: WorkspaceTreeSnapshot, right: WorkspaceTreeSnapshot
    ) -> bool:
        left_move, right_move = WorkspaceTreeExchangeWriter._moves(change)
        return WorkspaceTreeWriter._moved(left_move, right) and WorkspaceTreeWriter._moved(
            right_move, left
        )

    @staticmethod
    def _effect(
        change: PreparedTreeExchange,
        status: Literal["done", "rejected", "uncertain"],
        *,
        code: str | None = None,
        left: WorkspaceTreeSnapshot | None = None,
        right: WorkspaceTreeSnapshot | None = None,
        changed: bool = False,
    ) -> TreeExchangeEffect:
        return TreeExchangeEffect(
            TreeExchangeReceipt(
                change.left.root_path,
                change.right.root_path,
                status,
                change.left.fingerprint,
                change.right.fingerprint,
                left.fingerprint if left else None,
                right.fingerprint if right else None,
                change.token,
                changed,
                code,
                status == "uncertain",
            ),
            left,
            right,
        )

    def prepare(
        self,
        left_path: str,
        right_path: str,
        *,
        expected_left: str,
        expected_right: str,
        cancelled: Callable[[], bool] | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> PreparedTreeExchange:
        self.trees._paths(left_path, right_path)
        abort = cancelled or (lambda: False)
        deadline = time.monotonic() + self.lock_timeout

        def check() -> None:
            self.trees._check(deadline, abort)
            if authorize is not None:
                authorize()

        with self.trees.directories._locked_parents(
            (left_path, right_path), abort, require_writable=False
        ) as parents:
            left, right = self._capture(left_path, right_path, parents, check)
            if left.fingerprint != expected_left or right.fingerprint != expected_right:
                raise ServiceError("workspace_tree_exchange_conflict", "原两树完整版本不匹配", 409)
            change = PreparedTreeExchange(left, right, uuid4().hex)
            self._parents(change, parents)
            check()
            return change

    def apply(
        self,
        change: PreparedTreeExchange,
        *,
        cancelled: Callable[[], bool] | None = None,
        authorize: Callable[[], None] | None = None,
        record_intent: Callable[[PreparedTreeExchange], None] | None = None,
    ) -> TreeExchangeEffect:
        left_parent, left_name, right_parent, right_name = self._validate(change)
        abort = cancelled or (lambda: False)
        deadline = time.monotonic() + self.lock_timeout
        attempted = False

        def check() -> None:
            self.trees._check(deadline, abort)

        try:
            with self.trees.directories._locked_parents(
                (change.left.root_path, change.right.root_path), abort
            ) as parents:

                def original() -> None:
                    check()
                    self._parents(change, parents)
                    if authorize is not None:
                        authorize()
                    if self._capture(
                        change.left.root_path, change.right.root_path, parents, check
                    ) != (change.left, change.right):
                        raise ServiceError(
                            "workspace_tree_exchange_conflict", "原任一完整树已变化", 409
                        )
                    check()

                original()
                _rename_function()
                if record_intent is not None:
                    record_intent(change)
                original()
                if authorize is not None:
                    authorize()
                self._parents(change, parents)
                check()
                attempted = True
                rename_at(
                    parents[left_parent],
                    left_name,
                    right_name,
                    target_fd=parents[right_parent],
                    exchange=True,
                )
                for fd in set(parents.values()):
                    os.fsync(fd)

                def observed() -> None:
                    self.trees._check(deadline, lambda: False)

                left, right = self._capture(
                    change.left.root_path, change.right.root_path, parents, observed
                )
                self._parents(change, parents)
                if not self._swapped(change, left, right):
                    raise ServiceError(
                        "workspace_tree_exchange_postcheck", "原两树交换后的完整事实不匹配", 409
                    )
                return self._effect(change, "done", left=left, right=right, changed=True)
        except (OSError, ServiceError) as error:
            return self._effect(
                change,
                "uncertain" if attempted else "rejected",
                changed=attempted,
                code=error.code
                if isinstance(error, ServiceError)
                else "workspace_tree_exchange_io",
            )

    def reconcile(self, change: PreparedTreeExchange) -> TreeExchangeEffect:
        self._validate(change)
        deadline = time.monotonic() + self.lock_timeout

        def check() -> None:
            self.trees._check(deadline, lambda: False)

        try:
            with self.trees.directories._locked_parents(
                (change.left.root_path, change.right.root_path),
                lambda: False,
                require_writable=False,
            ) as parents:
                self._parents(change, parents)
                left, right = self._capture(
                    change.left.root_path, change.right.root_path, parents, check
                )
                if self._swapped(change, left, right):
                    return self._effect(change, "done", left=left, right=right, changed=True)
                if (left, right) == (change.left, change.right):
                    return self._effect(
                        change, "rejected", code="workspace_tree_exchange_not_observed"
                    )
                return self._effect(
                    change, "uncertain", changed=True, code="workspace_tree_exchange_ambiguous"
                )
        except (OSError, ServiceError) as error:
            return self._effect(
                change,
                "uncertain",
                changed=True,
                code=error.code
                if isinstance(error, ServiceError)
                else "workspace_tree_exchange_io",
            )

    def prepare_undo(
        self, change: PreparedTreeExchange, effect: TreeExchangeEffect
    ) -> PreparedTreeExchange:
        self._validate(change)
        receipt = effect.receipt
        if (
            receipt.status != "done"
            or receipt.operation_token != change.token
            or receipt.left_path != change.left.root_path
            or receipt.right_path != change.right.root_path
            or receipt.left_before_fingerprint != change.left.fingerprint
            or receipt.right_before_fingerprint != change.right.fingerprint
            or effect.observed_left is None
            or effect.observed_right is None
            or receipt.left_after_fingerprint != effect.observed_left.fingerprint
            or receipt.right_after_fingerprint != effect.observed_right.fingerprint
            or not self._swapped(change, effect.observed_left, effect.observed_right)
        ):
            raise ServiceError(
                "workspace_tree_exchange_receipt", "撤销须绑定原两份真实完成after", 409
            )
        reverse = self.prepare(
            change.left.root_path,
            change.right.root_path,
            expected_left=effect.observed_left.fingerprint,
            expected_right=effect.observed_right.fingerprint,
        )
        if reverse.left != effect.observed_left or reverse.right != effect.observed_right:
            raise ServiceError(
                "workspace_tree_exchange_conflict", "原两树撤销身份或版本已变化", 409
            )
        return reverse
