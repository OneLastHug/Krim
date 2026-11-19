"""公共模块到受管项目的接线；只在已授权且已取得的项目租约内使用。"""

from __future__ import annotations

import base64
import hashlib
from typing import TYPE_CHECKING, Any

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.ports import ProjectRef
from krim_agent.sandbox.xfs import internal_id

if TYPE_CHECKING:
    from krim_agent.sandbox.broker import StorageBroker
    from krim_agent.sandbox.leases import Lease


class AcceptedProjectMutation:
    """不申请权限或生成检查点；只能由已有受理作业在围栏内构造。"""

    def __init__(self, broker: StorageBroker, project: ProjectRef, lease: Lease) -> None:
        self.broker = broker
        self.project = project
        self.lease = lease
        internal_id(project.environment_id)
        internal_id(project.project_id)
        if lease.project_id != project.project_id:
            raise ServiceError("permission_denied", 403)
        self._current(project)

    def _current(self, project: ProjectRef, operation_id: str | None = None) -> None:
        self.lease.check()
        if project != self.project or operation_id not in {None, self.lease.lease_id}:
            raise ServiceError("permission_denied", 403)
        row = self.broker._project(project.environment_id, project.project_id)
        if row.get("generation", 0) != project.generation:
            raise ServiceError("input_revision_conflict")

    def _after_change(self, project: ProjectRef, operation_id: str) -> None:
        try:
            self._current(project, operation_id)
        except ServiceError:
            # 文件已经可能落地，写后围栏失败不是“确定未写”的拒绝。
            raise ServiceError("sandbox_operation_unknown") from None

    def read(self, project: ProjectRef, path: str) -> bytes | None:
        """按ContentSource单页窗口分页取回完整内容；不设应用内容总量上限。

        每页沿用files.read的整文件sha256复核（页间一致才继续），每页前后重查
        租约与generation，EOF时再核对拼接长度/摘要，避免漏页或并发改写被当作完整读取。
        单页窗口（65536字节）不变；返回bytes是现有端口协议，调用方按全内容处理。
        """
        self._current(project)
        body = bytearray()
        expected = None
        size = None
        offset = 0
        while True:
            try:
                chunk = self.broker.files.read(
                    project.environment_id, project.project_id, path, offset=offset, limit=65536
                )
            except ServiceError as error:
                if error.code == "not_found" and offset == 0:
                    return None
                raise
            if expected is None:
                expected = chunk["sha256"]
                size = chunk["size"]
            if expected != chunk["sha256"] or size != chunk["size"]:
                raise ServiceError("artifact_conflict")
            data = base64.b64decode(chunk["data"], validate=True)
            body.extend(data)
            self._current(project)
            if chunk["eof"]:
                if len(body) != size or hashlib.sha256(body).hexdigest() != expected:
                    raise ServiceError("artifact_conflict")
                return bytes(body)
            if not data:
                raise ServiceError("artifact_conflict")
            offset += len(data)

    def write(
        self,
        project: ProjectRef,
        operation_id: str,
        path: str,
        content: bytes,
        expected_sha256: str | None,
    ) -> dict[str, Any]:
        if operation_id != self.lease.lease_id:
            raise ServiceError("permission_denied", 403)
        self._current(project, operation_id)
        result = self.broker.files.restore_write(
            project.environment_id, project.project_id, path, content, expected_sha256
        )
        self._after_change(project, operation_id)
        return result

    def rename(
        self,
        project: ProjectRef,
        operation_id: str,
        source: str,
        destination: str,
        expected_sha256: str,
        expected_destination_sha256: str | None,
    ) -> dict[str, Any]:
        if operation_id != self.lease.lease_id:
            raise ServiceError("permission_denied", 403)
        self._current(project, operation_id)
        result = self.broker.files.rename(
            project.environment_id,
            project.project_id,
            source,
            destination,
            expected_sha256,
            expected_destination_sha256,
        )
        self._after_change(project, operation_id)
        return result
