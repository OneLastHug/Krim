"""可信缺失目录暂存/NOREPLACE；独立缺失回执，不冒充原完整旧树交换。"""

from __future__ import annotations

import os
import re
import struct
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from threading import RLock
from typing import Literal
from uuid import uuid4

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_absence import (
    WorkspaceDirectoryAbsence,
    WorkspaceDirectoryAbsenceReader,
    _Observation,
)
from krim_agent.workspace_directory_restore_writer import (
    DirectoryRestoreStage,
    WorkspaceDirectoryRestoreWriter,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot
from krim_agent.workspace_writer import _rename_function, rename_at


def _error(code: str) -> ServiceError:
    return ServiceError(
        "workspace_missing_directory_restore_" + code, "缺失目录原恢复条件不匹配", 409
    )


def _absence(value: WorkspaceDirectoryAbsence) -> WorkspaceDirectoryAbsence:
    value.validate()
    return replace(value, parent=replace(value.parent))


def _snapshot(value: WorkspaceTreeSnapshot) -> WorkspaceTreeSnapshot:
    return replace(
        value,
        entries=tuple(
            replace(
                entry,
                file=replace(entry.file) if entry.file is not None else None,
                directory=replace(entry.directory) if entry.directory is not None else None,
            )
            for entry in value.entries
        ),
    )


@dataclass(frozen=True)
class PreparedMissingDirectoryRestore:
    stage: DirectoryRestoreStage
    target_fingerprint: str
    before: WorkspaceDirectoryAbsence = field(repr=False)
    current_absence: WorkspaceDirectoryAbsence = field(repr=False)
    staged: WorkspaceTreeSnapshot = field(repr=False)


def _prepared_copy(value: PreparedMissingDirectoryRestore) -> PreparedMissingDirectoryRestore:
    return replace(
        value,
        stage=replace(value.stage),
        before=_absence(value.before),
        current_absence=_absence(value.current_absence),
        staged=_snapshot(value.staged),
    )


@dataclass(frozen=True)
class MissingDirectoryRestoreReceipt:
    root_path: str
    status: Literal["done", "rejected", "uncertain"]
    before_absence_fingerprint: str
    target_fingerprint: str
    after_fingerprint: str | None
    changed: bool
    error_code: str | None
    recovery_required: bool


@dataclass(frozen=True)
class MissingDirectoryRestoreEffect:
    receipt: MissingDirectoryRestoreReceipt
    observed_after: WorkspaceTreeSnapshot | None = field(default=None, repr=False)


def _effect_copy(value: MissingDirectoryRestoreEffect) -> MissingDirectoryRestoreEffect:
    return replace(
        value,
        receipt=replace(value.receipt),
        observed_after=_snapshot(value.observed_after)
        if value.observed_after is not None
        else None,
    )


class WorkspaceMissingDirectoryRestoreWriter:
    """单原操作的可信物理执行；Reader生命周期由同原宿主管理，不提供持久重放。"""

    def __init__(
        self,
        workspace: Workspace,
        absence_reader: WorkspaceDirectoryAbsenceReader,
        *,
        lock_timeout: float = 5,
    ) -> None:
        if (
            type(absence_reader) is not WorkspaceDirectoryAbsenceReader
            or absence_reader.workspace is not workspace
        ):
            raise _error("context")
        absence_reader._owner()
        base = WorkspaceDirectoryRestoreWriter(workspace, lock_timeout=lock_timeout)
        self.base = WorkspaceDirectoryRestoreWriter(
            workspace, lock_timeout=min(base.lock_timeout, absence_reader.lock_timeout)
        )
        self.workspace, self.absences = workspace, absence_reader
        self._lock = RLock()
        self._busy = False
        self._prepare_started = False
        self._prepare_key: tuple[str, str, str] | None = None
        self._ready: PreparedMissingDirectoryRestore | None = None
        self._execution_started = False
        self._attempted = False
        self._namespace_known = False
        self._result: MissingDirectoryRestoreEffect | None = None

    @contextmanager
    def _activity(self) -> Iterator[None]:
        self.absences._owner()
        with self._lock:
            if self._busy:
                raise _error("busy")
            self._busy = True
        try:
            yield
        finally:
            # 原PID守卫已经拒绝fork借用；回调中fork也不能等待父线程继承锁。
            if os.getpid() == self.absences._owner_pid:
                with self._lock:
                    self._busy = False
            else:
                self._busy = False

    def _guard(
        self,
        deadline: float,
        abort: Callable[[], bool],
        authorize: Callable[[], None] | None = None,
        *,
        require_observer: bool = True,
    ) -> None:
        self.absences._owner()
        if require_observer and self.absences._closed:
            raise _error("observation")
        self.base._check(deadline, abort)
        if authorize is not None:
            authorize()
        self.absences._owner()
        self.base._check(deadline, abort)
        if require_observer and self.absences._closed:
            raise _error("observation")

    @staticmethod
    def _callbacks(authorize: object, cancelled: object) -> None:
        if any(value is not None and not callable(value) for value in (authorize, cancelled)):
            raise _error("context")

    def _observation(self, value: WorkspaceDirectoryAbsence) -> _Observation:
        self.absences._value(value)
        with self.absences._state_lock:
            current = self.absences._observations.get(value.root_path)
            if (
                self.absences._closed
                or current is None
                or current.value != value
                or current.watch.invalid
                or current.watch.descriptor < 0
            ):
                raise _error("observation")
            return current

    def _invalidate(self, observation: _Observation) -> None:
        with self.absences._state_lock:
            current = self.absences._observations.get(observation.value.root_path)
            if current is not None and current.watch is observation.watch:
                self.absences._discard(observation.value.root_path, current)
            else:
                observation.watch.close()

    def _missing(
        self, value: WorkspaceDirectoryAbsence, parent: int, path: str, name: str
    ) -> _Observation:
        observation = self._observation(value)
        self.base.directories._files._check_parent(path, value.parent.identity)
        self.absences._absent(parent, name)
        if self.absences._parent(parent) != value.parent or observation.watch.changed():
            raise _error("conflict")
        return observation

    def _event(
        self, observation: _Observation, name: str, mask: int, *, moved: bool = False
    ) -> None:
        self.absences._owner()
        if observation.watch.invalid or observation.watch.descriptor < 0:
            raise _error("observation")
        try:
            data = os.read(observation.watch.descriptor, 65_536)
        except OSError:
            raise _error("namespace_event") from None
        # 一个原命名空间副作用只允许一个完整事件；额外事件/溢出均不能被掩盖。
        if len(data) < 16:
            raise _error("namespace_event")
        watch, actual_mask, cookie, size = struct.unpack_from("iIII", data)
        expected_name = os.fsencode(name)
        if (
            watch < 0
            or actual_mask != mask
            or (cookie != 0) != moved
            or size == 0
            or size % 4 != 0
            or 16 + size != len(data)
            or data[16:] != expected_name + b"\0" * (size - len(expected_name))
            or size <= len(expected_name)
            or observation.watch.changed()
        ):
            raise _error("namespace_event")

    def _advance(
        self,
        before: WorkspaceDirectoryAbsence,
        observation: _Observation,
        parent: int,
        path: str,
        name: str,
    ) -> WorkspaceDirectoryAbsence:
        self.base.directories._files._check_parent(path, before.parent.identity)
        self.absences._absent(parent, name)
        after = self.absences._parent(parent)
        if any(
            getattr(before.parent, key) != getattr(after, key)
            for key in ("device", "inode", "mode", "uid", "gid")
        ):
            raise _error("conflict")
        value = WorkspaceDirectoryAbsence(
            before.root_path, before.workspace_fingerprint, after, uuid4().hex
        )
        with self.absences._state_lock:
            if self._observation(before) is not observation or observation.watch.changed():
                raise _error("observation")
            self.absences._observations[before.root_path] = replace(observation, value=value)
        return _absence(value)

    def prepare(
        self,
        target: WorkspaceTreeSnapshot,
        before: WorkspaceDirectoryAbsence,
        *,
        token: str,
        record_stage: Callable[[DirectoryRestoreStage], None] | None = None,
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> PreparedMissingDirectoryRestore:
        self._callbacks(authorize, cancelled)
        if record_stage is not None and not callable(record_stage):
            raise _error("context")
        deadline = time.monotonic() + min(5, self.base.lock_timeout)
        abort = cancelled if cancelled is not None else lambda: False
        self.base._token(token)
        self.base._validate_snapshot(target)
        self.absences._value(before)
        if target.root_path != before.root_path:
            raise _error("material")
        frozen, original = _snapshot(target), _absence(before)
        key = frozen.fingerprint, original.fingerprint, token
        with self._activity():
            if self._prepare_started:
                if self._ready is not None and key == self._prepare_key:
                    return _prepared_copy(self._ready)
                raise _error("prepare_closed")
            self._prepare_started, self._prepare_key = True, key
            observation: _Observation | None = None

            def check() -> None:
                self._guard(deadline, abort, authorize)

            try:
                check()
                _rename_function()
                with self.base.directories._locked_parent(original.root_path, abort) as (
                    parent,
                    path,
                    name,
                ):
                    observation = self._missing(original, parent, path, name)
                    check()
                    observation = self._missing(original, parent, path, name)
                    stage_name = "._krim-tx-" + token
                    os.mkdir(stage_name, 0o700, dir_fd=parent)
                    info = os.stat(stage_name, dir_fd=parent, follow_symlinks=False)
                    stage = DirectoryRestoreStage(
                        original.root_path,
                        token,
                        original.parent.identity,
                        (info.st_dev, info.st_ino),
                        (info.st_uid, info.st_gid),
                    )
                    with self.base._container(parent, stage) as container:
                        os.fsync(parent)
                        if record_stage is not None:
                            record_stage(replace(stage))
                        check()
                        self.base._check_container(parent, stage, container)
                        self._event(observation, stage_name, 0x40000100)
                        current = self._advance(original, observation, parent, path, name)
                        self.base._write_tree(container, frozen, check)
                        staged = self.base._capture(container, "payload", frozen.root_path, check)
                        if not self.base._same_content(frozen, staged):
                            raise _error("material")
                        check()
                        self._missing(current, parent, path, name)
                        self.base._check_container(parent, stage, container)
                        self._ready = PreparedMissingDirectoryRestore(
                            stage, frozen.fingerprint, original, current, staged
                        )
                        return _prepared_copy(self._ready)
            except BaseException as error:
                if observation is not None:
                    self._invalidate(observation)
                if isinstance(error, OSError):
                    raise _error("io") from None
                raise

    def _validate(self, value: PreparedMissingDirectoryRestore) -> None:
        try:
            if (
                type(value) is not PreparedMissingDirectoryRestore
                or type(value.stage) is not DirectoryRestoreStage
            ):
                raise _error("material")
            self.base._token(value.stage.token)
            self.absences._value(value.before)
            self.absences._value(value.current_absence)
            self.base._validate_snapshot(value.staged)
            if (
                value.stage.root_path != value.before.root_path
                or value.stage.root_path != value.current_absence.root_path
                or value.stage.root_path != value.staged.root_path
                or value.stage.parent_identity != value.before.parent.identity
                or value.stage.parent_identity != value.current_absence.parent.identity
                or value.stage.container_identity != value.staged.parent_identity
                or value.before.observation_id == value.current_absence.observation_id
                or type(value.target_fingerprint) is not str
                or re.fullmatch(r"[a-f0-9]{64}", value.target_fingerprint) is None
            ):
                raise _error("material")
        except (AttributeError, TypeError, ValueError):
            raise _error("material") from None

    @staticmethod
    def _effect(
        value: PreparedMissingDirectoryRestore,
        status: Literal["done", "rejected", "uncertain"],
        code: str | None = None,
        *,
        after: WorkspaceTreeSnapshot | None = None,
        changed: bool = False,
    ) -> MissingDirectoryRestoreEffect:
        return MissingDirectoryRestoreEffect(
            MissingDirectoryRestoreReceipt(
                value.stage.root_path,
                status,
                value.before.fingerprint,
                value.target_fingerprint,
                after.fingerprint if after is not None else None,
                changed,
                code,
                status == "uncertain",
            ),
            after,
        )

    def apply(
        self,
        value: PreparedMissingDirectoryRestore,
        *,
        record_intent: Callable[[PreparedMissingDirectoryRestore], None],
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> MissingDirectoryRestoreEffect:
        self._callbacks(authorize, cancelled)
        if not callable(record_intent):
            raise _error("context")
        self._validate(value)
        with self._activity():
            if self._ready is None or value != self._ready:
                raise _error("material")
            if self._execution_started:
                assert self._result is not None
                return _effect_copy(self._result)
            frozen = _prepared_copy(self._ready)
            self._execution_started = True
            deadline = time.monotonic() + min(5, self.base.lock_timeout)
            abort = cancelled if cancelled is not None else lambda: False

            def check() -> None:
                self._guard(deadline, abort)

            try:
                check()
                with self.base.directories._locked_parent(frozen.stage.root_path, abort) as (
                    parent,
                    path,
                    name,
                ):
                    with self.base._container(parent, frozen.stage) as container:

                        def original() -> None:
                            self._guard(deadline, abort, authorize)
                            self.base._check_container(parent, frozen.stage, container)
                            self._missing(frozen.current_absence, parent, path, name)
                            if (
                                self.base._capture(
                                    container, "payload", frozen.stage.root_path, check
                                )
                                != frozen.staged
                            ):
                                raise _error("conflict")

                        original()
                        record_intent(_prepared_copy(frozen))
                        original()
                        self._guard(deadline, abort, authorize)
                        if (
                            self.base._capture(container, "payload", frozen.stage.root_path, check)
                            != frozen.staged
                        ):
                            raise _error("conflict")
                        self.base._verify_stats(container, "payload", frozen.staged, check)
                        self.base._check_container(parent, frozen.stage, container)
                        observation = self._missing(frozen.current_absence, parent, path, name)
                        check()
                        self._attempted = True
                        rename_at(container, "payload", name, target_fd=parent)
                        self._namespace_known = True
                        os.fsync(container)
                        os.fsync(parent)
                        self._event(observation, name, 0x40000080, moved=True)

                        def observed() -> None:
                            self._guard(deadline, lambda: False)

                        after = self._published(frozen, parent, path, name, container, observed)
                        self._result = self._effect(frozen, "done", after=after, changed=True)
            except BaseException as error:
                self._result = self._effect(
                    frozen,
                    "uncertain" if self._attempted else "rejected",
                    error.code
                    if isinstance(error, ServiceError)
                    else "workspace_missing_directory_restore_io",
                    changed=self._attempted,
                )
                if not isinstance(error, (OSError, ServiceError)):
                    raise
            assert self._result is not None
            return _effect_copy(self._result)

    def _published(
        self,
        value: PreparedMissingDirectoryRestore,
        parent: int,
        path: str,
        name: str,
        container: int,
        check: Callable[[], None],
    ) -> WorkspaceTreeSnapshot:
        after = self.base._capture(parent, name, value.stage.root_path, check)
        try:
            os.stat("payload", dir_fd=container, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise _error("postcheck")
        self.base.directories._files._check_parent(path, value.stage.parent_identity)
        self.base._check_container(parent, value.stage, container)
        if not self.base._moved(value.staged, after, value.stage.parent_identity):
            raise _error("postcheck")
        return after

    def reconcile(
        self,
        value: PreparedMissingDirectoryRestore,
        *,
        authorize: Callable[[], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> MissingDirectoryRestoreEffect:
        self._callbacks(authorize, cancelled)
        self._validate(value)
        with self._activity():
            if self._result is not None and self._result.receipt.status != "uncertain":
                if value != self._ready:
                    raise _error("material")
                return _effect_copy(self._result)
            if self._ready is None or value != self._ready:
                return self._effect(
                    value,
                    "uncertain",
                    "workspace_missing_directory_restore_unverifiable",
                    changed=True,
                )
            frozen = _prepared_copy(self._ready)
            deadline = time.monotonic() + min(5, self.base.lock_timeout)
            abort = cancelled if cancelled is not None else lambda: False

            def check() -> None:
                self._guard(deadline, abort, require_observer=not self._namespace_known)

            try:
                self._guard(deadline, abort, authorize, require_observer=not self._namespace_known)
                with self.base.directories._locked_parent(
                    frozen.stage.root_path, abort, require_writable=False
                ) as (parent, path, name):
                    with self.base._container(parent, frozen.stage) as container:
                        try:
                            os.stat(name, dir_fd=parent, follow_symlinks=False)
                        except FileNotFoundError:
                            self._missing(frozen.current_absence, parent, path, name)
                            if (
                                self._namespace_known
                                or self.base._capture(
                                    container, "payload", frozen.stage.root_path, check
                                )
                                != frozen.staged
                            ):
                                raise _error("unverifiable") from None
                            self._result = self._effect(
                                frozen,
                                "rejected",
                                "workspace_missing_directory_restore_not_observed",
                            )
                        else:
                            after = self._published(frozen, parent, path, name, container, check)
                            if not self._namespace_known:
                                observation = self._observation(frozen.current_absence)
                                self._event(observation, name, 0x40000080, moved=True)
                                self._namespace_known = True
                            self._result = self._effect(frozen, "done", after=after, changed=True)
            except (OSError, ServiceError) as error:
                self._result = self._effect(
                    frozen,
                    "uncertain",
                    error.code
                    if isinstance(error, ServiceError)
                    else "workspace_missing_directory_restore_io",
                    changed=True,
                )
            assert self._result is not None
            return _effect_copy(self._result)
