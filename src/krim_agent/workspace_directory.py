"""单目录空目录条件事务；持久账本/公开消费在ADR-0087后续单元接入。"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace
from krim_agent.workspace_writer import WorkspaceWriter, _identity, _rename_function, rename_at


class DirectoryChangeSpec(StrictModel):
    """可信内部目录意图；不作为旧文本 workspace_change 公共参数。"""

    path: str = Field(min_length=1, max_length=512)
    operation: Literal["create", "delete"]
    expected_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    mode: int | None = Field(default=None, ge=0, le=0o777)

    @model_validator(mode="after")
    def shape(self) -> DirectoryChangeSpec:
        try:
            if not Workspace._parts(self.path):
                raise ValueError("目录路径不能为空")
        except ServiceError as error:
            raise ValueError(str(error)) from error
        if self.operation == "create":
            if self.expected_fingerprint is not None or self.mode is None:
                raise ValueError("目录创建须为缺失目标并指定普通mode")
            if self.mode & 0o700 != 0o700:
                raise ValueError("新目录须保留宿主owner的读取/写入/进入权限")
        elif self.expected_fingerprint is None or self.mode is not None:
            raise ValueError("目录删除须绑定原fingerprint且不指定新mode")
        return self


class DirectoryRestoreSpec(StrictModel):
    """仅由原已完成删除回执派生；不扩大公开目录创建参数。"""

    operation: Literal["restore_deleted"] = "restore_deleted"
    path: str = Field(min_length=1, max_length=512)
    mode: int = Field(ge=0, le=0o777)
    original_token: str = Field(pattern=r"^[0-9a-f]{32}$")


@dataclass(frozen=True)
class DirectorySnapshot:
    mode: int
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int
    uid: int
    gid: int
    nlink: int
    empty: bool

    @property
    def identity(self) -> tuple[int, int]:
        return self.device, self.inode

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "device": self.device,
                    "inode": self.inode,
                    "mode": self.mode,
                    "mtime_ns": self.mtime_ns,
                    "ctime_ns": self.ctime_ns,
                    "uid": self.uid,
                    "gid": self.gid,
                    "nlink": self.nlink,
                    "empty": self.empty,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()


@dataclass(frozen=True)
class PreparedDirectoryChange:
    spec_json: str
    workspace_fingerprint: str
    parent_identity: tuple[int, int]
    before: DirectorySnapshot | None
    token: str
    restore_from: DirectoryRecovery | None = None

    @property
    def spec(self) -> DirectoryChangeSpec | DirectoryRestoreSpec:
        if json.loads(self.spec_json).get("operation") == "restore_deleted":
            return DirectoryRestoreSpec.model_validate_json(self.spec_json)
        return DirectoryChangeSpec.model_validate_json(self.spec_json)


@dataclass(frozen=True)
class DirectoryChangeReceipt:
    path: str
    status: Literal["done", "rejected", "uncertain"]
    before_fingerprint: str | None
    after_fingerprint: str | None
    changed: bool
    error_code: str | None = None
    recovery_required: bool = False
    operation_token: str = ""


@dataclass(frozen=True)
class DirectoryEffect:
    receipt: DirectoryChangeReceipt
    observed_after: DirectorySnapshot | None = None
    recovery: DirectoryRecovery | None = None


@dataclass(frozen=True)
class DirectoryRecovery:
    workspace_fingerprint: str
    path: str
    stage_name: str
    parent_identity: tuple[int, int]
    expected: DirectorySnapshot | None


def _is_plain_mode(mode: int) -> bool:
    return type(mode) is int and 0 <= mode <= 0o777


class WorkspaceDirectoryWriter:
    def __init__(self, workspace: Workspace, *, lock_timeout: float = 5) -> None:
        if not 0 < lock_timeout <= 30:
            raise ValueError("目录协作锁超时须在0–30秒之间")
        self.workspace = workspace
        self.lock_timeout = lock_timeout
        self._files = WorkspaceWriter(workspace, lock_timeout=lock_timeout)

    def _parent(self, path: str) -> tuple[int, str, str]:
        parts = self.workspace._parts(path)
        if not parts:
            raise ServiceError("workspace_path", "目录动作须指定相对目录路径", 422)
        parent = "/".join(parts[:-1]) or "."
        return self.workspace._open(parent, directory=True), parent, parts[-1]

    @staticmethod
    def _empty(fd: int) -> bool:
        with os.scandir(fd) as entries:
            return next(entries, None) is None

    def _snapshot(
        self, parent_fd: int, name: str, *, missing: bool = False
    ) -> DirectorySnapshot | None:
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            if missing:
                return None
            raise ServiceError("workspace_not_found", "目录条目不存在", 404) from None
        except OSError:
            raise ServiceError("workspace_directory_type", "目标不是可信普通目录", 422) from None
        try:
            info = os.fstat(fd)
            mode = stat.S_IMODE(info.st_mode)
            if not stat.S_ISDIR(info.st_mode):
                raise ServiceError("workspace_directory_type", "目标不是目录", 422)
            if mode & ~0o777 or info.st_nlink < 1:
                raise ServiceError(
                    "workspace_directory_mode", "目录包含不支持的权限或链接状态", 422
                )
            empty = self._empty(fd)
            final = os.fstat(fd)
            if (
                info.st_dev,
                info.st_ino,
                info.st_mode,
                info.st_uid,
                info.st_gid,
                info.st_mtime_ns,
                info.st_ctime_ns,
                info.st_nlink,
            ) != (
                final.st_dev,
                final.st_ino,
                final.st_mode,
                final.st_uid,
                final.st_gid,
                final.st_mtime_ns,
                final.st_ctime_ns,
                final.st_nlink,
            ) or _identity(os.stat(name, dir_fd=parent_fd, follow_symlinks=False)) != _identity(
                final
            ):
                raise ServiceError("workspace_directory_changed", "目录读取期间变化", 409)
            return DirectorySnapshot(
                mode,
                info.st_dev,
                info.st_ino,
                info.st_mtime_ns,
                info.st_ctime_ns,
                info.st_uid,
                info.st_gid,
                info.st_nlink,
                empty,
            )
        except OSError:
            raise ServiceError("workspace_directory_access", "目录状态读取失败", 403) from None
        finally:
            os.close(fd)

    @contextmanager
    def _locked_parent(
        self, path: str, abort: Callable[[], bool], *, require_writable: bool = True
    ) -> Iterator[tuple[int, str, str]]:
        # 复用已验证的工作区级/根目录 flock；其内部只打开父目录，不触碰目标。
        with self._files._locked_parent(path, abort, require_writable=require_writable) as value:
            yield value

    def prepare(self, spec: DirectoryChangeSpec) -> PreparedDirectoryChange:
        request = DirectoryChangeSpec.model_validate(spec.model_dump())
        with self._locked_parent(request.path, lambda: False, require_writable=False) as (
            fd,
            parent,
            name,
        ):
            parent_identity = _identity(os.fstat(fd))
            before = self._snapshot(fd, name, missing=True)
            if request.operation == "create":
                if before is not None:
                    raise ServiceError("workspace_directory_exists", "待创建目录已存在", 409)
                assert request.mode is not None and _is_plain_mode(request.mode)
            else:
                if before is not None and not before.empty:
                    raise ServiceError("workspace_directory_nonempty", "只允许单项删除空目录", 409)
                if before is None or before.fingerprint != request.expected_fingerprint:
                    raise ServiceError(
                        "workspace_directory_conflict", "目录版本或空目录条件不匹配", 409
                    )
            self._files._check_parent(parent, parent_identity)
            return PreparedDirectoryChange(
                request.model_dump_json(),
                self.workspace.fingerprint,
                parent_identity,
                before,
                uuid4().hex,
            )

    def capture(self, path: str) -> DirectorySnapshot | None:
        with self._locked_parent(path, lambda: False, require_writable=False) as (fd, parent, name):
            identity = _identity(os.fstat(fd))
            value = self._snapshot(fd, name, missing=True)
            self._files._check_parent(parent, identity)
            return value

    def _receipt(
        self,
        change: PreparedDirectoryChange,
        status: Literal["done", "rejected", "uncertain"],
        code: str | None = None,
        *,
        after: DirectorySnapshot | None = None,
        changed: bool = False,
        recovery: DirectoryRecovery | None = None,
    ) -> DirectoryEffect:
        return DirectoryEffect(
            DirectoryChangeReceipt(
                change.spec.path,
                status,
                change.before.fingerprint if change.before else None,
                after.fingerprint if after else None,
                changed,
                code,
                status == "uncertain",
                change.token,
            ),
            after,
            recovery,
        )

    @staticmethod
    def _same(
        actual: DirectorySnapshot | None,
        wanted: DirectorySnapshot | None,
        *,
        displaced: bool = False,
    ) -> bool:
        if actual is None or wanted is None:
            return actual is wanted
        return (
            actual.identity == wanted.identity
            and actual.mode == wanted.mode
            and actual.uid == wanted.uid
            and actual.gid == wanted.gid
            and actual.nlink == wanted.nlink
            and actual.empty == wanted.empty
            and actual.mtime_ns == wanted.mtime_ns
            and (displaced or actual.ctime_ns == wanted.ctime_ns)
        )

    def _create_stage(self, fd: int, stage: str, mode: int) -> DirectorySnapshot:
        os.mkdir(stage, 0o700, dir_fd=fd)
        initial = os.stat(stage, dir_fd=fd, follow_symlinks=False)
        opened = os.open(stage, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        try:
            if _identity(os.fstat(opened)) != _identity(initial):
                raise ServiceError("workspace_directory_changed", "目录暂存身份变化", 409)
            os.fchmod(opened, mode)
            os.fsync(opened)
        finally:
            os.close(opened)
        value = self._snapshot(fd, stage)
        assert value is not None
        if not value.empty or value.mode != mode:
            raise ServiceError("workspace_directory_changed", "目录暂存内容或属性变化", 409)
        os.fsync(fd)
        return value

    def _remove_stage(self, fd: int, reference: DirectoryRecovery) -> None:
        actual = self._snapshot(fd, reference.stage_name, missing=True)
        if actual is None:
            return
        if reference.expected is None:
            raise ServiceError(
                "workspace_directory_stage_unknown", "目录暂存身份未确认，拒绝清理", 409
            )
        if not actual.empty or not self._same(actual, reference.expected, displaced=True):
            raise ServiceError(
                "workspace_directory_stage_changed", "目录恢复材料变化，拒绝删除", 409
            )
        os.rmdir(reference.stage_name, dir_fd=fd)
        os.fsync(fd)

    def apply(
        self,
        change: PreparedDirectoryChange,
        *,
        abort: Callable[[], bool] | None = None,
        authorize: Callable[[], None] | None = None,
        record_intent: Callable[[DirectoryRecovery], None] | None = None,
    ) -> DirectoryEffect:
        cancelled = abort or (lambda: False)
        self._validate(change)
        _rename_function()
        creating = change.spec.operation in {"create", "restore_deleted"}
        with self._locked_parent(change.spec.path, cancelled) as (fd, parent, name):
            self._files._check_parent(parent, change.parent_identity)
            if authorize is not None:
                try:
                    authorize()
                except ServiceError as error:
                    return self._receipt(change, "rejected", error.code)
            current = self._snapshot(fd, name, missing=True)
            if not self._same(current, change.before):
                return self._receipt(change, "rejected", "workspace_directory_conflict")
            if cancelled():
                return self._receipt(change, "rejected", "workspace_write_cancelled")
            attempted = False
            recovery: DirectoryRecovery | None = None
            stage_owned = False
            try:
                if creating:
                    assert change.spec.mode is not None
                    if change.restore_from is not None:
                        recovery = change.restore_from
                        self._check_reference(change, recovery, restoring=True)
                        staged = self._snapshot(fd, recovery.stage_name)
                        if (
                            not self._same(staged, recovery.expected, displaced=True)
                            or staged is None
                            or not staged.empty
                        ):
                            raise ServiceError(
                                "workspace_directory_stage_changed", "原删除恢复材料变化", 409
                            )
                    else:
                        stage = f"._krim-tx-{change.token}"
                        try:
                            staged = self._create_stage(fd, stage, change.spec.mode)
                        except (OSError, ServiceError):
                            try:
                                os.stat(stage, dir_fd=fd, follow_symlinks=False)
                            except FileNotFoundError:
                                raise
                            # 暂存建立可能已发生；没有完整identity就不能按名字盲删。
                            pending = DirectoryRecovery(
                                self.workspace.fingerprint,
                                change.spec.path,
                                stage,
                                change.parent_identity,
                                None,
                            )
                            return self._receipt(
                                change,
                                "uncertain",
                                "workspace_directory_stage_unknown",
                                recovery=pending,
                            )
                        recovery = DirectoryRecovery(
                            self.workspace.fingerprint,
                            change.spec.path,
                            stage,
                            change.parent_identity,
                            staged,
                        )
                        stage_owned = True
                else:
                    assert current is not None and current.empty
                    recovery = DirectoryRecovery(
                        self.workspace.fingerprint,
                        change.spec.path,
                        f"._krim-tx-{change.token}",
                        change.parent_identity,
                        current,
                    )
                if record_intent is not None:
                    record_intent(recovery)
                self._files._check_parent(parent, change.parent_identity)
                if authorize is not None:
                    authorize()
                self._files._check_parent(parent, change.parent_identity)
                if cancelled():
                    if stage_owned:
                        self._remove_stage(fd, recovery)
                    return self._receipt(change, "rejected", "workspace_write_cancelled")
                if not self._same(self._snapshot(fd, name, missing=True), change.before):
                    if stage_owned:
                        self._remove_stage(fd, recovery)
                    return self._receipt(change, "rejected", "workspace_directory_conflict")
                if creating:
                    actual_stage = self._snapshot(fd, recovery.stage_name)
                    if (
                        actual_stage is None
                        or not actual_stage.empty
                        or not self._same(
                            actual_stage,
                            recovery.expected,
                            displaced=change.restore_from is not None,
                        )
                    ):
                        raise ServiceError(
                            "workspace_directory_stage_changed", "发布前目录恢复材料变化", 409
                        )
                attempted = True
                if creating:
                    rename_at(fd, recovery.stage_name, name)
                else:
                    rename_at(fd, name, recovery.stage_name)
                os.fsync(fd)
                after = self._snapshot(fd, name, missing=True)
                expected_present = creating
                if (after is not None) != expected_present:
                    return self._receipt(
                        change,
                        "uncertain",
                        "workspace_directory_postcheck",
                        after=after,
                        changed=True,
                        recovery=recovery,
                    )
                if after is not None:
                    if not self._same(after, recovery.expected, displaced=True) or not after.empty:
                        return self._receipt(
                            change,
                            "uncertain",
                            "workspace_directory_postcheck",
                            after=after,
                            changed=True,
                            recovery=recovery,
                        )
                else:
                    displaced = self._snapshot(fd, recovery.stage_name)
                    if not self._same(displaced, change.before, displaced=True):
                        # 保留被移走的真实目录；恢复只能不覆盖原名，不能rmdir外部替换条目。
                        rename_at(fd, recovery.stage_name, name)
                        os.fsync(fd)
                        return self._receipt(change, "rejected", "workspace_directory_conflict")
                self._files._check_parent(parent, change.parent_identity)
                os.fsync(fd)
                return self._receipt(
                    change,
                    "done",
                    after=after,
                    changed=True,
                    recovery=recovery if not creating else None,
                )
            except (OSError, ServiceError) as error:
                code = error.code if isinstance(error, ServiceError) else "workspace_directory_io"
                if isinstance(error, FileExistsError) and attempted:
                    target = self._snapshot(fd, name, missing=True)
                    if not self._same(target, change.before):
                        return self._receipt(
                            change,
                            "uncertain",
                            "workspace_directory_io",
                            changed=True,
                            recovery=recovery,
                        )
                    if stage_owned and recovery is not None:
                        try:
                            self._remove_stage(fd, recovery)
                        except (OSError, ServiceError):
                            return self._receipt(
                                change,
                                "uncertain",
                                "workspace_directory_cleanup",
                                recovery=recovery,
                            )
                    return self._receipt(change, "rejected", "workspace_directory_conflict")
                if not attempted and stage_owned and recovery is not None:
                    try:
                        self._remove_stage(fd, recovery)
                        recovery = None
                    except (OSError, ServiceError):
                        return self._receipt(
                            change, "uncertain", "workspace_directory_cleanup", recovery=recovery
                        )
                return self._receipt(
                    change,
                    "uncertain" if attempted else "rejected",
                    code,
                    changed=attempted,
                    recovery=recovery,
                )

    def prepare_undo(
        self, original: PreparedDirectoryChange, effect: DirectoryEffect
    ) -> PreparedDirectoryChange:
        self._validate(original)
        if (
            effect.receipt.status != "done"
            or effect.receipt.operation_token != original.token
            or effect.receipt.path != original.spec.path
        ):
            raise ServiceError("workspace_directory_receipt", "撤销须绑定原已完成回执", 409)
        if effect.receipt.before_fingerprint != (
            original.before.fingerprint if original.before else None
        ) or effect.receipt.after_fingerprint != (
            effect.observed_after.fingerprint if effect.observed_after else None
        ):
            raise ServiceError("workspace_directory_receipt", "目录回执不匹配原快照", 409)
        with self._locked_parent(original.spec.path, lambda: False) as (fd, parent, name):
            self._files._check_parent(parent, original.parent_identity)
            current = self._snapshot(fd, name, missing=True)
            if original.spec.operation in {"create", "restore_deleted"}:
                if (
                    current is None
                    or not current.empty
                    or not self._same(current, effect.observed_after)
                ):
                    raise ServiceError(
                        "workspace_directory_conflict", "创建目录已变化或不为空", 409
                    )
                spec: DirectoryChangeSpec | DirectoryRestoreSpec = DirectoryChangeSpec(
                    path=original.spec.path,
                    operation="delete",
                    expected_fingerprint=current.fingerprint,
                )
            else:
                if current is not None:
                    raise ServiceError("workspace_directory_conflict", "删除目标已有外部目录", 409)
                assert original.before is not None
                if effect.recovery is None:
                    raise ServiceError(
                        "workspace_directory_stage_missing", "原目录恢复材料未保留", 409
                    )
                self._check_reference(original, effect.recovery)
                staged = self._snapshot(fd, effect.recovery.stage_name)
                if (
                    not self._same(staged, original.before, displaced=True)
                    or staged is None
                    or not staged.empty
                ):
                    raise ServiceError(
                        "workspace_directory_stage_changed", "原目录恢复材料变化", 409
                    )
                spec = DirectoryRestoreSpec(
                    path=original.spec.path,
                    mode=original.before.mode,
                    original_token=original.token,
                )
            return PreparedDirectoryChange(
                spec.model_dump_json(),
                self.workspace.fingerprint,
                original.parent_identity,
                current,
                uuid4().hex,
                effect.recovery if original.spec.operation == "delete" else None,
            )

    def _check_reference(
        self,
        change: PreparedDirectoryChange,
        reference: DirectoryRecovery,
        *,
        restoring: bool = False,
    ) -> None:
        self._validate(change)
        if (
            reference.workspace_fingerprint != change.workspace_fingerprint
            or reference.path != change.spec.path
            or reference.parent_identity != change.parent_identity
            or not reference.stage_name.startswith("._krim-tx-")
            or len(reference.stage_name) != len("._krim-tx-") + 32
            or any(c not in "0123456789abcdef" for c in reference.stage_name[len("._krim-tx-") :])
            or (not restoring and reference.stage_name != f"._krim-tx-{change.token}")
            or (
                restoring
                and (
                    not isinstance(change.spec, DirectoryRestoreSpec)
                    or reference.stage_name != f"._krim-tx-{change.spec.original_token}"
                )
            )
        ):
            raise ServiceError("workspace_directory_reference", "目录恢复引用不匹配", 409)

    def _validate(self, change: PreparedDirectoryChange) -> None:
        if change.workspace_fingerprint != self.workspace.fingerprint:
            raise ServiceError("workspace_binding_changed", "目录提案不属于当前工作区", 409)
        if len(change.token) != 32 or any(c not in "0123456789abcdef" for c in change.token):
            raise ServiceError("workspace_change_invalid", "目录提案内部标识无效", 422)
        spec = change.spec
        self.workspace._parts(spec.path)
        if isinstance(spec, DirectoryRestoreSpec):
            if (
                change.before is not None
                or change.restore_from is None
                or change.restore_from.expected is None
                or spec.mode != change.restore_from.expected.mode
            ):
                raise ServiceError("workspace_change_invalid", "目录恢复规格与原材料不符", 422)
        elif spec.operation == "create":
            if change.before is not None or change.restore_from is not None:
                raise ServiceError("workspace_change_invalid", "目录创建规格与原基线不符", 422)
        elif (
            change.before is None
            or not change.before.empty
            or spec.expected_fingerprint != change.before.fingerprint
            or change.restore_from is not None
        ):
            raise ServiceError("workspace_change_invalid", "目录删除规格与原基线不符", 422)

    def verify(
        self, change: PreparedDirectoryChange, reference: DirectoryRecovery
    ) -> DirectoryEffect:
        """核实只读原目标/暂存，不重放目录创建或删除。"""
        self._check_reference(change, reference, restoring=change.restore_from is not None)
        if reference.expected is None:
            return self._receipt(
                change, "uncertain", "workspace_directory_stage_unknown", recovery=reference
            )
        try:
            with self._locked_parent(change.spec.path, lambda: False) as (fd, parent, name):
                self._files._check_parent(parent, change.parent_identity)
                target = self._snapshot(fd, name, missing=True)
                staged = self._snapshot(fd, reference.stage_name, missing=True)
                if change.spec.operation in {"create", "restore_deleted"}:
                    if staged is None and self._same(target, reference.expected, displaced=True):
                        return self._receipt(change, "done", after=target, changed=True)
                    if target is None and self._same(staged, reference.expected, displaced=True):
                        return self._receipt(
                            change, "rejected", "confirmed_no_effect", recovery=reference
                        )
                else:
                    if target is None and self._same(staged, reference.expected, displaced=True):
                        return self._receipt(change, "done", changed=True, recovery=reference)
                    if staged is None and self._same(target, change.before):
                        return self._receipt(change, "rejected", "confirmed_no_effect")
        except (ServiceError, OSError):
            pass
        return self._receipt(
            change, "uncertain", "workspace_directory_reconcile", changed=True, recovery=reference
        )

    def finalize(
        self,
        change: PreparedDirectoryChange,
        reference: DirectoryRecovery,
        *,
        rejected: bool = False,
    ) -> None:
        self._check_reference(change, reference, restoring=change.restore_from is not None)
        with self._locked_parent(change.spec.path, lambda: False) as (fd, parent, name):
            self._files._check_parent(parent, change.parent_identity)
            target = self._snapshot(fd, name, missing=True)
            if rejected and not self._same(target, change.before):
                raise ServiceError("workspace_directory_conflict", "拒绝动作的原目标已变化", 409)
            if change.spec.operation in {"create", "restore_deleted"} and not rejected:
                if self._snapshot(fd, reference.stage_name, missing=True) is not None:
                    raise ServiceError(
                        "workspace_directory_stage_changed", "创建回执不能盲删剩余目录", 409
                    )
                return
            self._remove_stage(fd, reference)
