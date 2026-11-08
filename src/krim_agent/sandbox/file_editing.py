"""A3有界文本补丁与显式rename服务；所有变动仅经ProjectMutation端口单次调用。

调用方负责审批、lease与checkpoint；本模块不开放HTTP旁路、不直接访问宿主路径、
不实现copy+delete伪装rename。端口调用结果未知时不盲目重写，先经resolve_*_outcome
查询实际落地状态，确认未应用后才允许再次调用同一operation。端口ServiceError不
默认视为写前拒绝：真实端口可能已完成落盘动作才在围栏复核时抛错，须读回核实后定性。
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any, Protocol

from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.edit_models import (
    MAX_PATCH_FILE_BYTES,
    PatchPlan,
    PatchRequest,
    RenamePlan,
    RenameRequest,
)
from krim_agent.sandbox.ports import ProjectMutation, ProjectRef


class OperationJournal(Protocol):
    """可选的持久化操作流水；实现由宿主注入，本模块不建第二业务库。

    status: applied（已应用，含端口报错但读回证实已落地）/ rejected（读回证实未应用，
    可安全重试）/ unknown（结果未知：端口声明或读回无法证实）/ unapplied（查询确认
    未应用）。unknown之后apply被封锁，处理见_gate与resolve_*_outcome。
    """

    def record(self, operation_id: str, status: str, detail: str = "") -> None: ...
    def status(self, operation_id: str) -> str | None: ...


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plan_id(kind: str, *fields: str | bytes) -> str:
    encoded = [field if isinstance(field, bytes) else field.encode() for field in fields]
    material = b"\x00".join([kind.encode(), *encoded])
    return hashlib.sha256(material).hexdigest()[:32]


def _on_codepoint_boundary(data: bytes, position: int) -> bool:
    return position >= len(data) or data[position] & 0xC0 != 0x80


def _port_operation_id(port_operation_id: str | None, plan_id: str) -> str:
    if port_operation_id is None:
        return plan_id
    if type(port_operation_id) is not str or not port_operation_id:
        raise ServiceError("invalid_input", 422)
    return port_operation_id


def _validated(model_type: type[PatchRequest] | type[RenameRequest], **fields: Any) -> Any:
    try:
        return model_type(**fields)
    except ValidationError as error:
        raise ServiceError("invalid_input", 422) from error


def _checked_project(project: ProjectRef) -> ProjectRef:
    if type(project.generation) is not int or project.generation < 0:
        raise ServiceError("invalid_input", 422)
    for name in (project.principal_id, project.environment_id, project.project_id):
        if type(name) is not str or not name:
            raise ServiceError("invalid_input", 422)
    return project


class ProjectFileEditor:
    """依赖ProjectMutation端口的有界补丁与rename计划器。

    同一plan_id（operation_id）对mutation端口最多发起一次写调用。端口ServiceError
    不默认视为写前拒绝：sandbox_operation_unknown一律记unknown；其余代码读回核实，
    证实未落地记rejected（可安全重试）、证实已落地记applied、无法证实记unknown并
    拒绝后续apply，直到resolve确认状态。计划本身只读，不产生任何变动。
    """

    def __init__(self, mutation: ProjectMutation, journal: OperationJournal | None = None) -> None:
        self._mutation = mutation
        self._journal = journal
        self._attempts: dict[str, str] = {}
        self._receipts: dict[str, dict[str, Any]] = {}

    def plan_patch(
        self,
        project: ProjectRef,
        *,
        expected_generation: int,
        path: str,
        expected_sha256: str,
        offset: int,
        delete_length: int,
        replacement: str,
    ) -> PatchPlan:
        """读取当前文件生成补丁计划；不写任何数据。"""
        _checked_project(project)
        if project.generation != expected_generation:
            raise ServiceError("revision_conflict")
        request: PatchRequest = _validated(
            PatchRequest,
            path=path,
            expected_sha256=expected_sha256,
            expected_generation=expected_generation,
            offset=offset,
            delete_length=delete_length,
            replacement=replacement,
        )
        old = self._mutation.read(project, request.path)
        if old is None:
            raise ServiceError("not_found", 404)
        if len(old) > MAX_PATCH_FILE_BYTES:
            raise ServiceError("sandbox_limit")
        end = request.offset + request.delete_length
        if end > len(old):
            raise ServiceError("invalid_input", 422)
        try:
            old.decode("utf-8")
        except UnicodeDecodeError:
            raise ServiceError("invalid_input", 422) from None  # 非文本文件不支持补丁
        if not _on_codepoint_boundary(old, request.offset) or not _on_codepoint_boundary(old, end):
            raise ServiceError("invalid_input", 422)
        if _digest(old) != request.expected_sha256:
            raise ServiceError("source_changed")
        replacement_bytes = request.replacement.encode("utf-8")
        updated = old[: request.offset] + replacement_bytes + old[end:]
        if len(updated) > MAX_PATCH_FILE_BYTES:
            raise ServiceError("sandbox_limit")  # 结果内容同样受4MiB文件上限约束
        return PatchPlan(
            plan_id=_plan_id(
                "patch",
                project.principal_id,
                project.environment_id,
                project.project_id,
                str(project.generation),
                request.path,
                request.expected_sha256,
                str(request.offset),
                str(request.delete_length),
                replacement_bytes,
            ),
            principal_id=project.principal_id,
            environment_id=project.environment_id,
            project_id=project.project_id,
            generation=project.generation,
            path=request.path,
            source_sha256=request.expected_sha256,
            result_sha256=_digest(updated),
            offset=request.offset,
            delete_length=request.delete_length,
            replacement_bytes=replacement_bytes,
        )

    def apply_patch(
        self, plan: PatchPlan, *, port_operation_id: str | None = None
    ) -> dict[str, Any]:
        """执行补丁计划；同一plan_id至多一次端口写调用。

        port_operation_id：宿主受理作业传入的lease_id（AcceptedProjectMutation要求
        operation_id==lease_id）；缺省使用plan_id。去重与journal始终以plan_id为键。
        """
        project = _checked_project(self._reference(plan))
        port_id = _port_operation_id(port_operation_id, plan.plan_id)
        if self._gate(plan.plan_id) == "applied":
            return self._already_applied(plan.plan_id)
        current = self._mutation.read(project, plan.path)
        if current is None:
            raise ServiceError("not_found", 404)
        current_digest = _digest(current)
        if current_digest == plan.result_sha256:  # 计划目标已在（外部）落地
            self._record(plan.plan_id, "applied", "already")
            return self._already_applied(plan.plan_id)
        if current_digest != plan.source_sha256:
            raise ServiceError("source_changed")
        end = plan.offset + plan.delete_length
        content = current[: plan.offset] + plan.replacement_bytes + current[end:]
        if _digest(content) != plan.result_sha256:
            raise ServiceError("execution_error")  # 防御：计划与现值不一致
        def classify() -> str:
            current_now = self._mutation.read(project, plan.path)
            if current_now is not None:
                digest_now = _digest(current_now)
                if digest_now == plan.result_sha256:
                    return "applied"
                if digest_now == plan.source_sha256:
                    return "rejected"
            return "unknown"

        receipt = self._invoke(
            plan.plan_id,
            lambda: self._mutation.write(project, port_id, plan.path, content, plan.source_sha256),
            classify,
        )
        return {
            "status": "applied",
            "operation_id": plan.plan_id,
            "result_sha256": plan.result_sha256,
            "receipt": receipt,
        }

    def plan_rename(
        self, project: ProjectRef, *, expected_generation: int, source: str, destination: str,
        expected_source_sha256: str,
    ) -> RenamePlan:
        """生成原子rename计划；只读校验，不移动任何数据。"""
        _checked_project(project)
        if project.generation != expected_generation:
            raise ServiceError("revision_conflict")
        request: RenameRequest = _validated(
            RenameRequest,
            source=source,
            destination=destination,
            expected_source_sha256=expected_source_sha256,
            expected_generation=expected_generation,
        )
        current = self._mutation.read(project, request.source)
        if current is None:
            # 目录经read端口不可读、无法取得源hash，因此递归目录移动天然被排除。
            raise ServiceError("not_found", 404)
        if _digest(current) != request.expected_source_sha256:
            raise ServiceError("source_changed")
        if self._mutation.read(project, request.destination) is not None:
            raise ServiceError("key_conflict")  # 计划期即拒绝占用目标与自移动；apply仍复核
        return RenamePlan(
            plan_id=_plan_id(
                "rename",
                project.principal_id,
                project.environment_id,
                project.project_id,
                str(project.generation),
                request.source,
                request.destination,
                request.expected_source_sha256,
            ),
            principal_id=project.principal_id,
            environment_id=project.environment_id,
            project_id=project.project_id,
            generation=project.generation,
            source=request.source,
            destination=request.destination,
            source_sha256=request.expected_source_sha256,
        )

    def apply_rename(
        self, plan: RenamePlan, *, port_operation_id: str | None = None
    ) -> dict[str, Any]:
        """执行rename计划；同一plan_id至多一次端口rename调用，绝不copy+delete。

        port_operation_id语义同apply_patch；实际原子性由端口保证（同fs单文件），
        本模块只做前置校验与单次调用。
        """
        project = _checked_project(self._reference(plan))
        port_id = _port_operation_id(port_operation_id, plan.plan_id)
        if self._gate(plan.plan_id) == "applied":
            return self._already_applied(plan.plan_id)
        source = self._mutation.read(project, plan.source)
        if source is None:
            raise ServiceError("not_found", 404)
        if _digest(source) != plan.source_sha256:
            raise ServiceError("source_changed")
        if self._mutation.read(project, plan.destination) is not None:
            raise ServiceError("key_conflict")  # 目标必须不存在；不支持覆盖语义
        def classify() -> str:
            source_now = self._mutation.read(project, plan.source)
            destination_now = self._mutation.read(project, plan.destination)
            if destination_now is None:
                if source_now is not None and _digest(source_now) == plan.source_sha256:
                    return "rejected"
                return "unknown"
            if source_now is None and _digest(destination_now) == plan.source_sha256:
                return "applied"  # 源已不在且目标内容与源一致，rename落地
            return "unknown"

        receipt = self._invoke(
            plan.plan_id,
            lambda: self._mutation.rename(
                project, port_id, plan.source, plan.destination, plan.source_sha256, None
            ),
            classify,
        )
        return {"status": "applied", "operation_id": plan.plan_id, "receipt": receipt}

    def operation_status(self, operation_id: str) -> dict[str, Any]:
        """查询已记录的operation状态；untracked表示本服务与journal均无记录。"""
        return {"operation_id": operation_id, "status": self._status(operation_id) or "untracked"}

    def resolve_patch_outcome(self, plan: PatchPlan) -> str:
        """只读查询补丁是否落地；applied/unapplied记入journal，diverged保持只读。

        unknown之后：unapplied解除apply封锁（须显式走本方法确认），applied记为完成，
        diverged沿用apply时的hash复核自然拒绝。
        """
        current = self._mutation.read(self._reference(plan), plan.path)
        if current is None:
            return "diverged"
        digest = _digest(current)
        if digest == plan.result_sha256:
            self._record(plan.plan_id, "applied", "resolved")
            return "applied"
        if digest == plan.source_sha256:
            self._record(plan.plan_id, "unapplied", "resolved")
            return "unapplied"
        return "diverged"

    def resolve_rename_outcome(self, plan: RenamePlan) -> str:
        """只读查询rename是否落地；diverged不记录、不重写，由调用方处置。

        目标存在还须内容与计划源hash一致：目标被后续改写或为独立创建时无法
        证明原rename落地（与patch的hash严格语义一致），判diverged。
        """
        reference = self._reference(plan)
        source = self._mutation.read(reference, plan.source)
        destination = self._mutation.read(reference, plan.destination)
        if destination is not None:
            landed = source is None and _digest(destination) == plan.source_sha256
            outcome = "applied" if landed else "diverged"
        elif source is not None:
            outcome = "unapplied" if _digest(source) == plan.source_sha256 else "diverged"
        else:
            outcome = "diverged"  # 源与目标同时缺失，无法判定
        if outcome in {"applied", "unapplied"}:
            self._record(plan.plan_id, outcome, "resolved")
        return outcome

    def _reference(self, plan: PatchPlan | RenamePlan) -> ProjectRef:
        return ProjectRef(
            principal_id=plan.principal_id,
            environment_id=plan.environment_id,
            project_id=plan.project_id,
            generation=plan.generation,
        )

    def _status(self, operation_id: str) -> str | None:
        if operation_id in self._attempts:
            return self._attempts[operation_id]
        return self._journal.status(operation_id) if self._journal is not None else None

    def _record(self, operation_id: str, status: str, detail: str = "") -> None:
        self._attempts[operation_id] = status
        if self._journal is not None:
            self._journal.record(operation_id, status, detail)

    def _gate(self, operation_id: str) -> str | None:
        status = self._status(operation_id)
        if status == "unknown":
            raise ServiceError("sandbox_operation_unknown")  # 先查询，不盲目重写
        return status

    def _already_applied(self, operation_id: str) -> dict[str, Any]:
        return {
            "status": "applied",
            "operation_id": operation_id,
            "already_applied": True,
            "receipt": self._receipts.get(operation_id, {}),
        }

    def _invoke(
        self,
        operation_id: str,
        call: Callable[[], dict[str, Any]],
        classify: Callable[[], str],
    ) -> dict[str, Any]:
        try:
            receipt = call()
        except ServiceError as error:
            if error.code == "sandbox_operation_unknown":
                self._record(operation_id, "unknown", "port_unknown")  # 端口声明结果未知
                raise
            # 其余ServiceError不得默认视为写前拒绝：端口可能已落盘才在围栏复核时
            # 抛错，读回核实后定性；证实之前绝不记为未写。
            self._record(operation_id, self._proven(classify), f"port_error:{error.code}")
            raise
        except Exception as error:
            self._record(operation_id, "unknown")
            raise ServiceError("sandbox_operation_unknown") from error
        except BaseException:
            self._record(operation_id, "unknown")
            raise
        self._receipts[operation_id] = receipt if isinstance(receipt, dict) else {}
        self._record(operation_id, "applied")
        return receipt

    @staticmethod
    def _proven(classify: Callable[[], str]) -> str:
        """读回核实端口报错后的落地状态；核实失败或状态不符一律按unknown。"""
        try:
            outcome = classify()
        except Exception:
            return "unknown"
        return outcome if outcome in {"applied", "rejected"} else "unknown"


__all__ = ["MAX_PATCH_FILE_BYTES", "OperationJournal", "ProjectFileEditor"]
