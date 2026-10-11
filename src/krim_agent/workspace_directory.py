"""单空目录条件事务；创建/删除/移动与原账本消费共享身份和父级门禁。"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Literal
from uuid import uuid4

from pydantic import Field, SerializerFunctionWrapHandler, model_serializer, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace
from krim_agent.workspace_writer import WorkspaceWriter, _identity, _rename_function, rename_at


class DirectoryChangeSpec(StrictModel):
    """独立目录公开意图；不作为旧文本 workspace_change 参数。"""

    path: str = Field(min_length=1, max_length=512)
    operation: Literal["create", "delete", "move"]
    expected_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    mode: int | None = Field(default=None, ge=0, le=0o777)
    target_path: str | None = Field(default=None, min_length=1, max_length=512)

    @model_validator(mode="after")
    def shape(self) -> DirectoryChangeSpec:
        try:
            if not Workspace._parts(self.path):
                raise ValueError("目录路径不能为空")
        except ServiceError as error:
            raise ValueError(str(error)) from error
        if self.operation == "create":
            if (
                self.expected_fingerprint is not None
                or self.mode is None
                or self.target_path is not None
            ):
                raise ValueError("目录创建须为缺失目标并指定普通mode")
            if self.mode & 0o700 != 0o700:
                raise ValueError("新目录须保留宿主owner的读取/写入/进入权限")
        elif self.operation == "delete":
            if (
                self.expected_fingerprint is None
                or self.mode is not None
                or self.target_path is not None
            ):
                raise ValueError("目录删除须绑定原fingerprint且不指定新mode")
        elif (
            self.expected_fingerprint is None
            or self.mode is not None
            or self.target_path is None
            or self.target_path == self.path
        ):
            raise ValueError("目录移动须绑定原fingerprint和不同目标路径")
        if self.target_path is not None:
            try:
                if not Workspace._parts(self.target_path):
                    raise ValueError("目录目标路径不能为空")
            except ServiceError as error:
                raise ValueError(str(error)) from error
        if self.operation == "delete" and self.expected_fingerprint is None:
            raise ValueError("目录删除须绑定原fingerprint且不指定新mode")
        return self


class DirectoryRestoreSpec(StrictModel):
    """仅由原已完成删除回执派生；不扩大公开目录创建参数。"""

    operation: Literal["restore_deleted"] = "restore_deleted"
    path: str = Field(min_length=1, max_length=512)
    mode: int = Field(ge=0, le=0o777)
    original_token: str = Field(pattern=r"^[0-9a-f]{32}$")


class DirectoryMoveRestoreSpec(StrictModel):
    operation: Literal["restore_move"] = "restore_move"
    source_path: str = Field(min_length=1, max_length=512)
    target_path: str = Field(min_length=1, max_length=512)
    expected_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    original_token: str = Field(pattern=r"^[0-9a-f]{32}$")

    @property
    def path(self) -> str:
        return self.source_path


class ArtifactDirectorySpec(DirectoryChangeSpec):
    """仅从原命令目录manifest派生，普通preview不接受来源字段。"""

    command_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def create_only(self) -> ArtifactDirectorySpec:
        if self.operation != "create":
            raise ValueError("命令目录来源当前只派生创建容器提案")
        return self


class DirectoryPreviewView(StrictModel):
    proposal_id: str
    scope_id: str
    workspace_key: str
    resource_fingerprint: str
    path: str
    operation: Literal["create", "delete", "move"]
    target_path: str | None = Field(default=None, min_length=1, max_length=512)
    before_fingerprint: str | None
    mode: int | None
    revision: int = Field(ge=0)
    status: Literal[
        "proposed",
        "applying",
        "applied",
        "undoing",
        "undone",
        "keeping",
        "kept",
        "rejected",
        "uncertain",
    ]
    expires_at: float

    @model_validator(mode="after")
    def conditions(self) -> DirectoryPreviewView:
        DirectoryChangeSpec(
            path=self.path,
            operation=self.operation,
            target_path=self.target_path,
            expected_fingerprint=self.before_fingerprint,
            mode=self.mode,
        )
        return self

    @model_serializer(mode="wrap")
    def omit_empty_target(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        """普通create/delete响应省略新增空字段，让旧Client继续严格解析。"""
        value: dict[str, object] = handler(self)
        if self.target_path is None:
            value.pop("target_path", None)
        return value


class DirectoryChangeAction(StrictModel):
    proposal_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    action: Literal["apply", "undo", "keep"]
    expected_revision: int = Field(ge=0)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")


class DirectoryPreviewRequest(StrictModel):
    spec: DirectoryChangeSpec
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")
    ttl_seconds: float = Field(default=3600, gt=0, le=86400, allow_inf_nan=False)

    @model_validator(mode="after")
    def ordinary_spec(self) -> DirectoryPreviewRequest:
        if type(self.spec) is not DirectoryChangeSpec:
            raise ValueError("普通目录预览不接受内部来源规格")
        return self


class DirectoryOperationRequest(StrictModel):
    operation_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class DirectoryInspectRequest(StrictModel):
    path: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def relative_path(self) -> DirectoryInspectRequest:
        try:
            if not Workspace._parts(self.path):
                raise ValueError("目录检查须指定相对目录路径")
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


class DirectoryInspectView(StrictModel):
    path: str = Field(min_length=1, max_length=512)
    found: bool
    fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    mode: int | None = Field(default=None, ge=0, le=0o777)
    empty: bool | None = None

    @model_validator(mode="after")
    def presence(self) -> DirectoryInspectView:
        present = (self.fingerprint is not None, self.mode is not None, self.empty is not None)
        if any(present) != self.found or self.found and not all(present):
            raise ValueError("目录存在状态与指纹/模式/空性不一致")
        return self


DirectoryQueryKind = Literal[
    "proposal", "operation", "history", "proposals", "proposal_by_key", "operation_by_key"
]


class DirectoryQueryRequest(StrictModel):
    kind: DirectoryQueryKind
    identifier: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )
    request_key: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$"
    )
    after: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )
    limit: int = Field(default=32, ge=1, le=32)

    @model_validator(mode="after")
    def selection(self) -> DirectoryQueryRequest:
        if self.kind in {"proposal", "operation", "history"}:
            if self.identifier is None or self.request_key is not None:
                raise ValueError("目录查询须只提供原identifier")
        elif self.kind.endswith("_by_key"):
            if self.request_key is None or self.identifier is not None or self.after is not None:
                raise ValueError("按原键查询须只提供request_key")
        elif self.identifier is not None or self.request_key is not None:
            raise ValueError("目录列表只接受游标/容量")
        if self.kind not in {"history", "proposals"} and self.after is not None:
            raise ValueError("单项目录查询不接受after")
        if self.kind not in {"history", "proposals"} and self.limit != 32:
            raise ValueError("单项目录查询不接受分页容量")
        return self


class DirectoryQueryResult(StrictModel):
    kind: DirectoryQueryKind
    proposal: DirectoryPreviewView | None = None
    operation: DirectoryAttemptView | None = None
    proposals: list[DirectoryPreviewView] = Field(default_factory=list, max_length=32)
    history: list[DirectoryAttemptView] = Field(default_factory=list, max_length=32)
    next_after: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
    )

    @model_validator(mode="after")
    def selection(self) -> DirectoryQueryResult:
        if self.kind in {"history", "proposals"}:
            if self.proposal is not None or self.operation is not None:
                raise ValueError("目录页不能附带单项查询事实")
            if self.kind == "history":
                if self.proposals:
                    raise ValueError("目录历史页不能混入提案列表")
                last = self.history[-1].operation_id if self.history else None
            else:
                if self.history:
                    raise ValueError("目录提案页不能混入操作历史")
                last = self.proposals[-1].proposal_id if self.proposals else None
            if self.next_after is not None and self.next_after != last:
                raise ValueError("目录页游标须指向本页末项")
        else:
            if self.proposals or self.history or self.next_after is not None:
                raise ValueError("目录单项查询不能附带分页事实")
            if self.kind in {"proposal", "proposal_by_key"}:
                if self.operation is not None or self.kind == "proposal" and self.proposal is None:
                    raise ValueError("目录提案查询结果无效")
            elif self.proposal is not None or self.kind == "operation" and self.operation is None:
                raise ValueError("目录操作查询结果无效")
        return self


class DirectoryAttemptView(StrictModel):
    operation_id: str
    proposal_id: str
    scope_id: str
    action: Literal["apply", "undo", "keep"]
    expected_revision: int
    status: Literal["running", "done", "rejected", "uncertain"]
    receipt: DirectoryChangeReceipt | None
    recovery_required: bool
    cleanup_pending: bool
    created_at: float
    updated_at: float


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

    def __post_init__(self) -> None:
        if any(
            type(value) is not int
            for value in (
                self.mode,
                self.device,
                self.inode,
                self.mtime_ns,
                self.ctime_ns,
                self.uid,
                self.gid,
                self.nlink,
            )
        ):
            raise ValueError("目录快照整数类型无效")
        if (
            not 0 <= self.mode <= 0o777
            or any(value < 0 for value in (self.device, self.inode, self.uid, self.gid))
            or self.nlink < 1
            or type(self.empty) is not bool
        ):
            raise ValueError("目录快照属性无效")

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
    target_parent_identity: tuple[int, int] | None = None
    target_before: DirectorySnapshot | None = None

    @property
    def spec(self) -> DirectoryChangeSpec | DirectoryRestoreSpec | DirectoryMoveRestoreSpec:
        value = json.loads(self.spec_json)
        if value.get("operation") == "restore_deleted":
            return DirectoryRestoreSpec.model_validate_json(self.spec_json)
        if value.get("operation") == "restore_move":
            return DirectoryMoveRestoreSpec.model_validate_json(self.spec_json)
        if "command_id" in value:
            return ArtifactDirectorySpec.model_validate_json(self.spec_json)
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

    def __post_init__(self) -> None:
        if (self.status == "done") != (self.error_code is None) or self.recovery_required != (
            self.status == "uncertain"
        ):
            raise ValueError("目录回执状态/错误/恢复标志不一致")
        if len(self.operation_token) != 32 or any(
            c not in "0123456789abcdef" for c in self.operation_token
        ):
            raise ValueError("目录回执必须绑定原操作token")


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
    target_path: str | None = None
    target_parent_identity: tuple[int, int] | None = None


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

    @contextmanager
    def _locked_parents(
        self, paths: tuple[str, ...], abort: Callable[[], bool], *, require_writable: bool = True
    ) -> Iterator[dict[str, int]]:
        """稳定顺序持有根写锁、根mutex和所有相关父目录fd，避免双父死锁。"""
        import fcntl

        deadline = time.monotonic() + self.lock_timeout
        if not self.workspace._write_lock.acquire(timeout=self.lock_timeout):
            raise ServiceError("workspace_write_busy", "目录协作锁等待超时", 429)
        mutex: int | None = None
        fds: dict[str, int] = {}
        try:
            if require_writable and not self.workspace.writable:
                raise ServiceError("workspace_read_only", "宿主未授权工作区写能力", 403)
            anchor = self.workspace._open(".", directory=True)
            try:
                mutex = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=anchor)
            finally:
                os.close(anchor)
            while True:
                if abort():
                    raise ServiceError("workspace_write_cancelled", "目录移动在发布前取消", 409)
                try:
                    fcntl.flock(mutex, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise ServiceError(
                            "workspace_write_busy", "目录协作锁等待超时", 429
                        ) from None
                    time.sleep(0.01)
            parents = sorted({"/".join(self.workspace._parts(path)[:-1]) or "." for path in paths})
            for parent in parents:
                fds[parent] = self.workspace._open(parent, directory=True)
            yield fds
        finally:
            for fd in fds.values():
                os.close(fd)
            if mutex is not None:
                try:
                    fcntl.flock(mutex, fcntl.LOCK_UN)
                finally:
                    os.close(mutex)
            self.workspace._write_lock.release()

    def prepare(self, spec: DirectoryChangeSpec) -> PreparedDirectoryChange:
        request = DirectoryChangeSpec.model_validate(spec.model_dump())
        if request.operation == "move":
            assert request.target_path is not None
            source_parent = "/".join(self.workspace._parts(request.path)[:-1]) or "."
            target_parent = "/".join(self.workspace._parts(request.target_path)[:-1]) or "."
            with self._locked_parents(
                (request.path, request.target_path), lambda: False, require_writable=False
            ) as parents:
                source_fd, target_fd = parents[source_parent], parents[target_parent]
                source_identity = _identity(os.fstat(source_fd))
                target_identity = _identity(os.fstat(target_fd))
                before = self._snapshot(source_fd, self.workspace._parts(request.path)[-1])
                target_before = self._snapshot(
                    target_fd, self.workspace._parts(request.target_path)[-1], missing=True
                )
                assert before is not None
                if not before.empty:
                    raise ServiceError("workspace_directory_nonempty", "只允许移动空目录", 409)
                if before.fingerprint != request.expected_fingerprint or target_before is not None:
                    raise ServiceError(
                        "workspace_directory_conflict", "目录移动版本或目标条件不匹配", 409
                    )
                self._files._check_parent(source_parent, source_identity)
                self._files._check_parent(target_parent, target_identity)
                return PreparedDirectoryChange(
                    request.model_dump_json(),
                    self.workspace.fingerprint,
                    source_identity,
                    before,
                    uuid4().hex,
                    None,
                    target_identity,
                    target_before,
                )
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

    def prepare_artifact(self, spec: ArtifactDirectorySpec) -> PreparedDirectoryChange:
        request = ArtifactDirectorySpec.model_validate(spec.model_dump())
        prepared = self.prepare(
            DirectoryChangeSpec(path=request.path, operation="create", mode=request.mode)
        )
        return replace(prepared, spec_json=request.model_dump_json())

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
                change.spec.path
                if isinstance(change.spec, (DirectoryChangeSpec, DirectoryRestoreSpec))
                else change.spec.source_path,
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
        if isinstance(change.spec, DirectoryChangeSpec) and change.spec.operation == "move":
            return self._move_apply(change, cancelled, authorize, record_intent)
        if isinstance(change.spec, DirectoryMoveRestoreSpec):
            return self._move_apply(change, cancelled, authorize, record_intent)
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

    def _move_apply(
        self,
        change: PreparedDirectoryChange,
        cancelled: Callable[[], bool],
        authorize: Callable[[], None] | None,
        record_intent: Callable[[DirectoryRecovery], None] | None,
    ) -> DirectoryEffect:
        spec = change.spec
        if isinstance(spec, DirectoryChangeSpec):
            source_path, target_path = spec.path, spec.target_path
        elif isinstance(spec, DirectoryMoveRestoreSpec):
            source_path, target_path = spec.source_path, spec.target_path
        else:
            raise ServiceError("workspace_change_invalid", "目录移动规格类型无效", 422)
        assert (
            target_path is not None
            and change.before is not None
            and change.target_parent_identity is not None
        )
        source_parent = "/".join(self.workspace._parts(source_path)[:-1]) or "."
        target_parent = "/".join(self.workspace._parts(target_path)[:-1]) or "."
        source_name, target_name = (
            self.workspace._parts(source_path)[-1],
            self.workspace._parts(target_path)[-1],
        )
        recovery = DirectoryRecovery(
            self.workspace.fingerprint,
            source_path,
            "",
            change.parent_identity,
            change.before,
            target_path,
            change.target_parent_identity,
        )
        attempted = False
        try:
            with self._locked_parents((source_path, target_path), cancelled) as parents:
                source_fd, target_fd = parents[source_parent], parents[target_parent]
                self._check_move_parents(change, parents, source_parent, target_parent)
                if authorize is not None:
                    authorize()
                source = self._snapshot(source_fd, source_name, missing=True)
                target = self._snapshot(target_fd, target_name, missing=True)
                if not self._same(source, change.before) or target is not None:
                    return self._receipt(change, "rejected", "workspace_directory_conflict")
                if cancelled():
                    return self._receipt(change, "rejected", "workspace_write_cancelled")
                if record_intent is not None:
                    record_intent(recovery)
                if authorize is not None:
                    authorize()
                self._check_move_parents(change, parents, source_parent, target_parent)
                source = self._snapshot(source_fd, source_name, missing=True)
                target = self._snapshot(target_fd, target_name, missing=True)
                if not self._same(source, change.before) or target is not None:
                    return self._receipt(change, "rejected", "workspace_directory_conflict")
                if cancelled():
                    return self._receipt(change, "rejected", "workspace_write_cancelled")
                attempted = True
                try:
                    rename_at(source_fd, source_name, target_name, target_fd=target_fd)
                except FileExistsError:
                    return self._receipt(change, "rejected", "workspace_directory_conflict")
                os.fsync(source_fd)
                if target_fd != source_fd:
                    os.fsync(target_fd)
                after = self._snapshot(target_fd, target_name, missing=True)
                source_after = self._snapshot(source_fd, source_name, missing=True)
                if source_after is not None or not self._same(after, change.before, displaced=True):
                    return self._receipt(
                        change,
                        "uncertain",
                        "workspace_directory_postcheck",
                        after=after,
                        changed=True,
                        recovery=recovery,
                    )
                self._check_move_parents(change, parents, source_parent, target_parent)
                return self._receipt(change, "done", after=after, changed=True)
        except (OSError, ServiceError) as error:
            return self._receipt(
                change,
                "uncertain" if attempted else "rejected",
                error.code if isinstance(error, ServiceError) else "workspace_directory_io",
                changed=attempted,
                recovery=recovery if attempted else None,
            )

    def _check_move_parents(
        self,
        change: PreparedDirectoryChange,
        parents: dict[str, int],
        source_parent: str,
        target_parent: str,
    ) -> None:
        assert change.target_parent_identity is not None
        for parent, wanted in (
            (source_parent, change.parent_identity),
            (target_parent, change.target_parent_identity),
        ):
            if _identity(os.fstat(parents[parent])) != wanted:
                raise ServiceError("workspace_parent_changed", "目录父身份已变化", 409)
            self._files._check_parent(parent, wanted)

    def verify_keep(
        self,
        change: PreparedDirectoryChange,
        effect: DirectoryEffect,
        *,
        abort: Callable[[], bool] | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> DirectoryEffect:
        """保留只核对原完整效果；移动同时检查双父、源缺失与目标身份。"""
        self._validate(change)
        if (
            effect.receipt.status != "done"
            or effect.receipt.path != change.spec.path
            or effect.receipt.operation_token != change.token
            or effect.receipt.before_fingerprint
            != (change.before.fingerprint if change.before else None)
            or effect.receipt.after_fingerprint
            != (effect.observed_after.fingerprint if effect.observed_after else None)
        ):
            raise ServiceError("workspace_directory_receipt", "保留须绑定原完整效果", 409)
        cancelled = abort or (lambda: False)
        spec = change.spec
        try:
            if isinstance(spec, DirectoryChangeSpec) and spec.operation == "move":
                assert spec.target_path is not None
                source_parent = "/".join(self.workspace._parts(spec.path)[:-1]) or "."
                target_parent = "/".join(self.workspace._parts(spec.target_path)[:-1]) or "."
                with self._locked_parents((spec.path, spec.target_path), cancelled) as parents:
                    self._check_move_parents(change, parents, source_parent, target_parent)
                    if authorize is not None:
                        authorize()
                        self._check_move_parents(change, parents, source_parent, target_parent)
                    source = self._snapshot(
                        parents[source_parent], self.workspace._parts(spec.path)[-1], missing=True
                    )
                    target = self._snapshot(
                        parents[target_parent],
                        self.workspace._parts(spec.target_path)[-1],
                        missing=True,
                    )
                    matches = source is None and target == effect.observed_after
            else:
                with self._locked_parent(spec.path, cancelled) as (fd, parent, name):
                    self._files._check_parent(parent, change.parent_identity)
                    if authorize is not None:
                        authorize()
                        self._files._check_parent(parent, change.parent_identity)
                    matches = self._snapshot(fd, name, missing=True) == effect.observed_after
            if cancelled():
                return self._receipt(change, "rejected", "workspace_write_cancelled")
            if not matches:
                return self._receipt(change, "rejected", "workspace_directory_conflict")
            return replace(effect, receipt=replace(effect.receipt, changed=False))
        except (OSError, ServiceError) as error:
            return self._receipt(
                change,
                "rejected",
                error.code if isinstance(error, ServiceError) else "workspace_directory_io",
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
        if isinstance(original.spec, DirectoryChangeSpec) and original.spec.operation == "move":
            if (
                effect.observed_after is None
                or original.target_parent_identity is None
                or original.spec.target_path is None
            ):
                raise ServiceError("workspace_directory_receipt", "移动回执缺少目标观察", 409)
            return PreparedDirectoryChange(
                DirectoryMoveRestoreSpec(
                    source_path=original.spec.target_path,
                    target_path=original.spec.path,
                    expected_fingerprint=effect.observed_after.fingerprint,
                    original_token=original.token,
                ).model_dump_json(),
                self.workspace.fingerprint,
                original.target_parent_identity,
                effect.observed_after,
                uuid4().hex,
                None,
                original.parent_identity,
                None,
            )
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
        if reference.target_path is not None:
            spec = change.spec
            expected_target = (
                spec.target_path
                if isinstance(spec, DirectoryMoveRestoreSpec)
                or isinstance(spec, DirectoryChangeSpec)
                and spec.operation == "move"
                else None
            )
            if (
                reference.target_parent_identity is None
                or reference.workspace_fingerprint != change.workspace_fingerprint
                or reference.path != change.spec.path
                or reference.parent_identity != change.parent_identity
                or reference.target_parent_identity != change.target_parent_identity
                or reference.target_path != expected_target
                or reference.expected != change.before
            ):
                raise ServiceError("workspace_directory_reference", "目录移动双父引用不匹配", 409)
            return
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
        if isinstance(spec, DirectoryMoveRestoreSpec):
            self.workspace._parts(spec.source_path)
            self.workspace._parts(spec.target_path)
            if (
                change.before is None
                or not change.before.empty
                or change.before.fingerprint != spec.expected_fingerprint
            ):
                raise ServiceError("workspace_change_invalid", "移动撤销规格与原基线不符", 422)
            if (
                change.target_parent_identity is None
                or change.target_before is not None
                or change.restore_from is not None
            ):
                raise ServiceError("workspace_change_invalid", "移动撤销目标基线无效", 422)
            return
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
            if reference.target_path is not None:
                source_path = change.spec.path
                source_parent = "/".join(self.workspace._parts(source_path)[:-1]) or "."
                target_parent = "/".join(self.workspace._parts(reference.target_path)[:-1]) or "."
                with self._locked_parents(
                    (source_path, reference.target_path), lambda: False
                ) as parents:
                    self._check_move_parents(change, parents, source_parent, target_parent)
                    source = self._snapshot(
                        parents[source_parent], self.workspace._parts(source_path)[-1], missing=True
                    )
                    target = self._snapshot(
                        parents[target_parent],
                        self.workspace._parts(reference.target_path)[-1],
                        missing=True,
                    )
                    if source is None and self._same(target, reference.expected, displaced=True):
                        return self._receipt(change, "done", after=target, changed=True)
                    if self._same(source, change.before) and target is None:
                        return self._receipt(change, "rejected", "confirmed_no_effect")
                return self._receipt(
                    change,
                    "uncertain",
                    "workspace_directory_reconcile",
                    changed=True,
                    recovery=reference,
                )
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
        if reference.target_path is not None:
            return
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
